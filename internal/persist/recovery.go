package persist

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
	"github.com/nima-ca/sylphy/internal/store"
)

// ErrReplay marks a logged command that failed when it was replayed.
var ErrReplay = errors.New("persist: command failed during replay")

// CorruptError is a startup-fatal problem in the data directory, naming the
// file and, when known, the byte offset.
type CorruptError struct {
	// File is a file name inside the data directory (or the directory itself).
	File string
	// Offset is the byte offset of the problem, or -1 when it concerns the
	// whole file.
	Offset int64
	// Err is the cause; it wraps ErrCorrupt (or one of its refinements).
	Err error
}

// Error implements the error interface.
func (e *CorruptError) Error() string {
	if e.Offset < 0 {
		return fmt.Sprintf("persist: %s: %v", e.File, e.Err)
	}
	return fmt.Sprintf("persist: %s: at byte offset %d: %v", e.File, e.Offset, e.Err)
}

// Unwrap returns the cause.
func (e *CorruptError) Unwrap() error { return e.Err }

func fileErr(file string, err error) error {
	var de *DecodeError
	if errors.As(err, &de) {
		inner := de.Err
		if de.Msg != "" {
			inner = fmt.Errorf("%s: %w", de.Msg, de.Err)
		}
		return &CorruptError{File: file, Offset: de.Offset, Err: inner}
	}
	return fmt.Errorf("persist: reading %s: %w", file, err)
}

func missingFile(name string) error {
	return &CorruptError{File: name, Offset: -1, Err: fmt.Errorf("%w: listed in the manifest but missing", ErrCorrupt)}
}

// Target is what recovery loads into. The store implements it.
type Target interface {
	// Load inserts key with its value and optional absolute deadline in Unix ms
	// (0 means none). It runs before the server accepts clients.
	Load(key string, v store.Value, deadlineMs int64) error
	// BeginReplay freezes the store's clock at 0 until the returned function
	// is called. While frozen nothing expires and no deadline deletes a key, so
	// replaying the log reproduces the original sequence exactly instead of
	// resurrecting or deleting keys depending on how long the server was down.
	BeginReplay() (end func())
}

// Replayer executes one logged command against the store being rebuilt.
type Replayer interface {
	// Exec runs argv and returns an error if the command failed.
	Exec(argv [][]byte) error
}

// CommandReplayer replays commands through the command dispatcher with a loader
// client: raw store, no propagation, replies discarded (error replies are
// turned into errors).
type CommandReplayer struct {
	d   *command.Dispatcher
	ctx *command.Context
	buf *bytes.Buffer
	w   *protocol.Writer
}

var _ Replayer = (*CommandReplayer)(nil)

// NewCommandReplayer builds the loader client over reg and st. A nil cfg means
// config.Default.
func NewCommandReplayer(reg *command.Registry, st command.Store, cfg *config.Config) *CommandReplayer {
	if cfg == nil {
		c := config.Default()
		cfg = &c
	}
	buf := &bytes.Buffer{}
	w := protocol.NewWriter(buf)
	return &CommandReplayer{
		d:   command.NewDispatcher(reg, nil),
		ctx: &command.Context{Store: st, W: w, Config: cfg, Conn: &command.ConnState{}},
		buf: buf,
		w:   w,
	}
}

// Exec implements Replayer. It is not safe for concurrent use.
func (r *CommandReplayer) Exec(argv [][]byte) error {
	r.buf.Reset()
	r.d.Dispatch(r.ctx, argv)
	if err := r.w.Flush(); err != nil {
		return err
	}
	if b := r.buf.Bytes(); len(b) > 0 && b[0] == '-' {
		return fmt.Errorf("%w: %s", ErrReplay, strings.TrimSpace(string(b[1:])))
	}
	return nil
}

// RecoveryReport summarizes a recovery.
type RecoveryReport struct {
	// BaseKeys is the number of keys loaded from the base snapshot.
	BaseKeys int
	// Segments is the number of segments replayed.
	Segments int
	// Records is the number of log records replayed.
	Records int
	// TruncatedBytes is the size of the partial record cut off the end of the
	// last segment, if any.
	TruncatedBytes int64
	// Elapsed is how long recovery took.
	Elapsed time.Duration
}

// RecoverOptions configures Recover.
type RecoverOptions struct {
	// FS is the file system; required.
	FS FS
	// Dir is the data directory; required. It is created if missing.
	Dir string
	// Target receives the base snapshot; required.
	Target Target
	// Replay executes log records; required.
	Replay Replayer
	// LoadTruncated repairs (truncates) a partial record at the end of the last
	// segment instead of failing (aof-load-truncated).
	LoadTruncated bool
	// Logger receives recovery messages. Nil discards.
	Logger *slog.Logger
	// Now supplies the time for the report. Nil means time.Now.
	Now func() time.Time
}

// RecoverResult is what recovery learned about the directory.
type RecoverResult struct {
	// Manifest is the manifest the directory now holds.
	Manifest Manifest
	// ActiveSegment is the segment to keep appending to (the last one the
	// manifest lists, after any repair), or "" if there is none.
	ActiveSegment string
	// NextID is greater than every base and segment id ever seen in the
	// directory, including stale files recovery deleted. Use it for the next
	// file ids.
	NextID uint64
	// Report summarizes the work.
	Report RecoveryReport
}

type dirScan struct {
	maxID uint64
	data  []string // canonical base and segment files
	tmp   []string // anything ending in TmpSuffix
}

func idOf(name string) (uint64, bool) {
	name = strings.TrimSuffix(name, TmpSuffix)
	if id, ok := ParseBaseName(name); ok {
		return id, true
	}
	return ParseSegmentName(name)
}

func scanDir(names []string) dirScan {
	var s dirScan
	for _, n := range names {
		id, isData := idOf(n)
		if isData {
			s.maxID = max(s.maxID, id)
		}
		switch {
		case strings.HasSuffix(n, TmpSuffix):
			s.tmp = append(s.tmp, n)
		case isData:
			s.data = append(s.data, n)
		}
	}
	return s
}

// Recover rebuilds the dataset from the data directory:
//
//  1. read the manifest (none and an empty directory: commit an empty
//     manifest and start empty; none but data files present: refuse);
//  2. with the store clock frozen, load the base snapshot, then replay the
//     listed segments in order through the command dispatcher;
//  3. a partial record at the end of the last segment is truncated (when
//     LoadTruncated) and logged; any other damage is a *CorruptError naming
//     the file and byte offset;
//  4. delete leftover temp files and files the manifest does not list.
//
// The first start commits an empty manifest so that every later directory has
// one, which is what makes "a base or segment the manifest does not list is
// garbage from an unfinished checkpoint" a safe rule: segments are listed
// before they receive a record, bases before anything depends on them.
//
// Recover does not purge expired keys. They stay invisible (reads check
// deadlines), but removing them must be logged, so the caller does it after
// opening the AOF. It must run before the listener starts and before the
// expiry sweeper does.
func Recover(ctx context.Context, o RecoverOptions) (RecoverResult, error) {
	if o.FS == nil || o.Dir == "" || o.Target == nil || o.Replay == nil {
		return RecoverResult{}, errors.New("persist: RecoverOptions needs FS, Dir, Target and Replay")
	}
	log := o.Logger
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	now := o.Now
	if now == nil {
		now = time.Now
	}
	started := now()
	if err := ctx.Err(); err != nil {
		return RecoverResult{}, err
	}
	if err := o.FS.MkdirAll(o.Dir, 0o755); err != nil {
		return RecoverResult{}, fmt.Errorf("persist: creating data directory %s: %w", o.Dir, err)
	}
	names, err := o.FS.ReadDir(o.Dir)
	if err != nil {
		return RecoverResult{}, fmt.Errorf("persist: listing %s: %w", o.Dir, err)
	}
	scan := scanDir(names)
	m, found, err := LoadManifest(o.FS, o.Dir)
	if err != nil {
		return RecoverResult{}, err
	}
	if !found {
		if len(scan.data) > 0 {
			return RecoverResult{}, &CorruptError{File: o.Dir, Offset: -1, Err: fmt.Errorf(
				"%w: the directory holds persistence files (%s) but no %s; refusing to guess which are valid: restore the manifest or move the files away",
				ErrCorrupt, strings.Join(scan.data, ", "), ManifestName)}
		}
		removeGarbage(o.FS, o.Dir, log, scan.tmp)
		if err := CommitManifest(o.FS, o.Dir, Manifest{}); err != nil {
			return RecoverResult{}, err
		}
		log.Info("no data found: starting empty", "dir", o.Dir)
		return RecoverResult{
			NextID: scan.maxID + 1,
			Report: RecoveryReport{Elapsed: now().Sub(started)},
		}, nil
	}

	listed := map[string]bool{m.Base.Name: m.Base.Name != ""}
	maxID := scan.maxID
	if id, ok := idOf(m.Base.Name); ok {
		maxID = max(maxID, id)
	}
	for _, s := range m.Segments {
		listed[s] = true
		if id, ok := idOf(s); ok {
			maxID = max(maxID, id)
		}
	}

	res := RecoverResult{Manifest: m, NextID: maxID + 1}
	end := o.Target.BeginReplay()
	defer end()
	if m.Base.Name != "" {
		if err := loadBase(ctx, &o, m.Base, &res.Report); err != nil {
			return RecoverResult{}, err
		}
	}
	for i, seg := range m.Segments {
		if err := replaySegment(ctx, &o, log, seg, i == len(m.Segments)-1, &res.Report); err != nil {
			return RecoverResult{}, err
		}
		res.Report.Segments++
	}
	if len(m.Segments) > 0 {
		res.ActiveSegment = m.Segments[len(m.Segments)-1]
	}

	var garbage []string
	garbage = append(garbage, scan.tmp...)
	for _, n := range scan.data {
		if !listed[n] {
			garbage = append(garbage, n)
		}
	}
	removeGarbage(o.FS, o.Dir, log, garbage)

	res.Report.Elapsed = now().Sub(started)
	log.Info("recovery complete",
		"base_keys", res.Report.BaseKeys, "segments", res.Report.Segments, "records", res.Report.Records,
		"truncated_bytes", res.Report.TruncatedBytes, "elapsed", res.Report.Elapsed)
	return res, nil
}

func loadBase(ctx context.Context, o *RecoverOptions, ref BaseRef, report *RecoveryReport) error {
	f, err := o.FS.OpenFile(filepath.Join(o.Dir, ref.Name), os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return missingFile(ref.Name)
		}
		return fmt.Errorf("persist: opening %s: %w", ref.Name, err)
	}
	defer func() { _ = f.Close() }()
	size, err := f.Size()
	if err != nil {
		return fmt.Errorf("persist: sizing %s: %w", ref.Name, err)
	}
	if size != ref.Size {
		return &CorruptError{File: ref.Name, Offset: -1, Err: fmt.Errorf(
			"%w: file is %d bytes but the manifest says %d", ErrCorrupt, size, ref.Size)}
	}
	sr, err := NewSnapshotReader(f)
	if err != nil {
		return fileErr(ref.Name, err)
	}
	for n := 0; ; n++ {
		if n&0xFF == 0 {
			if err := ctx.Err(); err != nil {
				return err
			}
		}
		e, err := sr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return fileErr(ref.Name, err)
		}
		var deadline int64
		if e.HasDeadline {
			deadline = e.DeadlineMs
		}
		if err := o.Target.Load(e.Key, e.Value, deadline); err != nil {
			return fmt.Errorf("persist: loading a key from %s: %w", ref.Name, err)
		}
		report.BaseKeys++
	}
	if got, _ := sr.Result(); got.CRC != ref.CRC {
		return &CorruptError{File: ref.Name, Offset: -1, Err: fmt.Errorf(
			"%w: file checksum %08x differs from the manifest's %08x", ErrChecksum, got.CRC, ref.CRC)}
	}
	return nil
}

func cmdName(argv [][]byte) string {
	if len(argv) == 0 {
		return "<empty>"
	}
	n := argv[0]
	if len(n) > 32 {
		n = n[:32]
	}
	return strings.ToUpper(string(n))
}

func replaySegment(ctx context.Context, o *RecoverOptions, log *slog.Logger, name string, last bool, report *RecoveryReport) error {
	path := filepath.Join(o.Dir, name)
	f, err := o.FS.OpenFile(path, os.O_RDONLY, 0)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return missingFile(name)
		}
		return fmt.Errorf("persist: opening %s: %w", name, err)
	}
	size, err := f.Size()
	if err != nil {
		_ = f.Close()
		return fmt.Errorf("persist: sizing %s: %w", name, err)
	}
	rd := NewRecordReader(f, RecordLimits{})
	truncateAt := int64(-1)
	for n := 0; ; n++ {
		if n&0xFF == 0 {
			if err := ctx.Err(); err != nil {
				_ = f.Close()
				return err
			}
		}
		start := rd.Offset()
		argv, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			if last && errors.Is(err, ErrTruncated) {
				truncateAt = start
				break
			}
			_ = f.Close()
			return fileErr(name, err)
		}
		if err := o.Replay.Exec(argv); err != nil {
			_ = f.Close()
			return &CorruptError{File: name, Offset: start, Err: fmt.Errorf(
				"%w: replaying %q: %w", ErrCorrupt, cmdName(argv), err)}
		}
		report.Records++
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("persist: closing %s: %w", name, err)
	}
	if truncateAt < 0 {
		return nil
	}
	tail := size - truncateAt
	if !o.LoadTruncated {
		return &CorruptError{File: name, Offset: truncateAt, Err: fmt.Errorf(
			"%w: partial record of %d bytes at the end of the last segment (aof-load-truncated is false)", ErrTruncated, tail)}
	}
	wf, err := o.FS.OpenFile(path, os.O_WRONLY, 0)
	if err != nil {
		return fmt.Errorf("persist: reopening %s to repair it: %w", name, err)
	}
	err = wf.Truncate(truncateAt)
	if err == nil {
		err = wf.Sync()
	}
	if cerr := wf.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return fmt.Errorf("persist: truncating the partial record in %s: %w", name, err)
	}
	report.TruncatedBytes += tail
	log.Warn("truncated a partial record at the end of the last segment",
		"segment", name, "offset", truncateAt, "bytes", tail)
	return nil
}

// removeGarbage deletes files that no committed state refers to. Failures are
// logged, not fatal: the files are harmless and the next start tries again.
func removeGarbage(fsys FS, dir string, log *slog.Logger, names []string) {
	removed := 0
	for _, n := range names {
		err := fsys.Remove(filepath.Join(dir, n))
		switch {
		case err == nil:
			removed++
			log.Info("removed a file left by an unfinished checkpoint", "file", n)
		case !errors.Is(err, fs.ErrNotExist):
			log.Warn("could not remove a stale file", "file", n, "error", err)
		}
	}
	if removed > 0 {
		if err := fsys.SyncDir(dir); err != nil {
			log.Warn("could not sync the data directory after removing stale files", "error", err)
		}
	}
}
