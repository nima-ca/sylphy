package persist

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Policy says when the AOF flusher fsyncs.
type Policy uint8

// Fsync policies. The zero value is PolicyEverySec, the default.
const (
	// PolicyEverySec fsyncs in the background once per second, if anything was
	// written since the last fsync. A crash loses at most about one second of
	// acknowledged writes (a little more if the disk is slow).
	PolicyEverySec Policy = iota
	// PolicyAlways fsyncs after every flusher batch. A client is acknowledged
	// only after WaitDurable returns for its record; one fsync covers every
	// record appended while the previous one ran (group commit).
	PolicyAlways
	// PolicyNo never fsyncs on its own; the OS decides. Rotation, Close and an
	// explicit WaitDurable still fsync.
	PolicyNo
)

// ParsePolicy parses "always", "everysec" or "no".
func ParsePolicy(s string) (Policy, error) {
	switch strings.ToLower(s) {
	case "always":
		return PolicyAlways, nil
	case "everysec":
		return PolicyEverySec, nil
	case "no":
		return PolicyNo, nil
	}
	return 0, fmt.Errorf("persist: invalid appendfsync %q (want always, everysec or no)", s)
}

// String returns the policy name as ParsePolicy accepts it.
func (p Policy) String() string {
	switch p {
	case PolicyAlways:
		return "always"
	case PolicyEverySec:
		return "everysec"
	case PolicyNo:
		return "no"
	}
	return fmt.Sprintf("Policy(%d)", uint8(p))
}

// DefaultHighWater is the default AOFOptions.HighWater.
const DefaultHighWater = 32 << 20

const (
	syncInterval = time.Second
	maxSpare     = 1 << 20 // largest buffer kept for reuse
)

// Errors returned by the AOF.
var (
	// ErrClosed is returned after Close.
	ErrClosed = errors.New("persist: AOF is closed")
	// ErrLatched wraps the cause of a persistent write or fsync failure. While
	// the AOF is latched, Append and Admit fail with it.
	ErrLatched = errors.New("persist: AOF is in an error state")
	// ErrNotHealed is returned by ClearError when the rotation given does not
	// cover the failure.
	ErrNotHealed = errors.New("persist: error state not cleared: the rotation does not cover it")
)

// Ticker is the part of time.Ticker the flusher uses; tests inject a fake.
type Ticker interface {
	// C delivers the ticks.
	C() <-chan time.Time
	// Stop releases the ticker.
	Stop()
}

type realTicker struct{ t *time.Ticker }

func newRealTicker(d time.Duration) Ticker { return realTicker{time.NewTicker(d)} }

func (r realTicker) C() <-chan time.Time { return r.t.C }
func (r realTicker) Stop()               { r.t.Stop() }

// AOFOptions configures OpenAOF.
type AOFOptions struct {
	// FS is the file system; required.
	FS FS
	// Dir is the data directory; required.
	Dir string
	// ActiveSegment is the existing segment (a name from SegmentName) to append
	// to, normally the last one the manifest lists after recovery repaired its
	// tail. Empty creates a new segment from NextSegmentID.
	ActiveSegment string
	// NextSegmentID is the id for the next segment this AOF creates. It must
	// exceed every segment id already in Dir, and the active segment's. Zero
	// means 1.
	NextSegmentID uint64
	// Policy is the fsync policy; the zero value is PolicyEverySec.
	Policy Policy
	// HighWater is the number of buffered bytes at which Admit starts to block.
	// Zero means DefaultHighWater.
	HighWater int
	// NewTicker creates the everysec ticker. Nil uses a real time.Ticker.
	NewTicker func(time.Duration) Ticker
	// OnRotate is called on the flusher goroutine (and once from OpenAOF when it
	// creates the first segment) after the new segment exists durably and,
	// for a rotation, after everything before the boundary is fsynced in the
	// previous segment. It must make the manifest list segment durably
	// (CommitManifest). Returning an error latches the AOF. It is not called for
	// a healing rotation: the checkpoint's own manifest lists that segment.
	OnRotate func(segment string) error
	// Logger receives persistence errors. Nil discards.
	Logger *slog.Logger

	admitBlocked func() // test hook: Admit is about to wait
}

// Stats is a snapshot of the writer's counters.
type Stats struct {
	// Appended is the sequence number of the last appended record.
	Appended uint64
	// Written is the last sequence handed to the OS.
	Written uint64
	// Durable is the last sequence known to be fsynced.
	Durable uint64
	// PendingBytes is the buffered, not yet written, size.
	PendingBytes int
	// BytesWritten counts bytes written to segments since OpenAOF.
	BytesWritten uint64
	// Segment is the active segment name.
	Segment string
	// SegmentSize is the active segment's size in bytes.
	SegmentSize int64
	// AdmitWaits counts Admit calls that had to block.
	AdmitWaits uint64
	// Err is the latched error, if any.
	Err error
}

type chunk struct {
	data    []byte
	lastSeq uint64    // sequence of the last record at or before the end of data
	rot     *Rotation // if set, rotate to a new segment after writing data
}

// Rotation tracks a request to start a new segment.
type Rotation struct {
	done    chan struct{}
	seq     uint64 // last record that belongs to the old segment
	gen     uint64 // latch generation when requested
	latched bool   // the AOF was latched when requested
	name    string
	err     error
	heal    bool // performed as a healing rotation
}

// Seq returns the sequence number of the last record before the boundary.
func (r *Rotation) Seq() uint64 { return r.seq }

// Wait blocks until the rotation is complete and returns the new segment's name.
func (r *Rotation) Wait(ctx context.Context) (string, error) {
	select {
	case <-r.done:
		return r.name, r.err
	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func (r *Rotation) finish(name string, err error) {
	r.name, r.err = name, err
	close(r.done)
}

// AOF is the append-only-file writer. Records (complete RESP arrays, see
// AppendRecord) are copied into a memory buffer by Append, which is cheap enough
// to call while holding a shard lock; one flusher goroutine writes them to the
// active segment and fsyncs according to the policy.
//
// Ordering. Sequence numbers are assigned under a mutex, so the order in the
// file is the order of the Append calls. Callers get per-key order by calling
// Append inside the shard-lock critical section.
//
// Group commit. WaitDurable(seq) blocks until seq is fsynced. The flusher takes
// everything appended since its last cycle as one batch, so concurrent waiters
// share a single write and a single fsync.
//
// Backpressure. Admit blocks, outside any lock, while the buffered bytes are at
// or above HighWater. Append itself never blocks, so the bound is soft: it can
// be exceeded by what writers admitted before the buffer filled up.
//
// Errors. A failed write or fsync latches the AOF: Append, Admit and
// WaitDurable return an error wrapping ErrLatched until ClearError (after a
// checkpoint) or a restart. The flusher never retries a failed fsync: the
// kernel may already have dropped the dirty pages, so a later success would
// prove nothing. A failed write is cut back to the last complete record. Records
// that were accepted but never written are counted and logged, never dropped
// silently.
//
// When Append fails after the command was already applied in memory. The
// journaling store calls Append inside the critical section, after the command
// changed memory, so a failed Append means the change is in memory but will
// never be in the log. By design: that client receives an error instead of the
// command's reply; the AOF is latched, so every later write is refused;
// memory and log differ until a checkpoint taken after the latch writes a new
// base from memory (Rotate while latched, then ClearError); if the process dies
// first, the change is lost like any non-durable write. Admit narrows the
// window, but the AOF can latch between Admit and Append.
//
// Goroutines. One flusher, owned by the AOF, stopped by Close.
type AOF struct {
	opts   AOFOptions
	log    *slog.Logger
	ticker Ticker

	mu         sync.Mutex
	buf        []byte  // records appended since the last seal
	spare      []byte  // a recycled buffer
	queue      []chunk // sealed, waiting for the flusher
	pending    int     // bytes in buf and queue
	seq        uint64  // last assigned sequence
	written    uint64
	durable    uint64
	syncReq    uint64 // highest sequence somebody asked to be made durable
	bytes      uint64
	segSize    int64
	statSeg    string
	err        error
	errGen     uint64
	closed     bool
	admitWaits uint64
	notify     chan struct{} // closed and replaced on every state change

	wake chan struct{} // capacity 1: the flusher has work
	quit chan struct{} // closed by Close
	done chan struct{} // closed when the flusher exits

	// Owned by the flusher goroutine once it starts.
	f        File
	segName  string
	goodSize int64 // size after the last complete write
	nextID   uint64
	closeErr error // written before done is closed
}

// OpenAOF opens (or creates) the active segment and starts the flusher.
func OpenAOF(o AOFOptions) (*AOF, error) {
	if o.FS == nil {
		return nil, errors.New("persist: AOFOptions.FS is required")
	}
	if o.Dir == "" {
		return nil, errors.New("persist: AOFOptions.Dir is required")
	}
	if o.Policy > PolicyNo {
		return nil, fmt.Errorf("persist: invalid fsync policy %d", o.Policy)
	}
	if o.HighWater <= 0 {
		o.HighWater = DefaultHighWater
	}
	if o.NewTicker == nil {
		o.NewTicker = newRealTicker
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	a := &AOF{
		opts:   o,
		log:    o.Logger,
		notify: make(chan struct{}),
		wake:   make(chan struct{}, 1),
		quit:   make(chan struct{}),
		done:   make(chan struct{}),
		nextID: o.NextSegmentID,
	}
	if a.nextID == 0 {
		a.nextID = 1
	}
	if o.ActiveSegment != "" {
		id, ok := ParseSegmentName(o.ActiveSegment)
		if !ok {
			return nil, fmt.Errorf("persist: bad active segment name %q", o.ActiveSegment)
		}
		if a.nextID <= id {
			return nil, fmt.Errorf("persist: NextSegmentID %d must exceed the active segment id %d", a.nextID, id)
		}
		f, err := o.FS.OpenFile(filepath.Join(o.Dir, o.ActiveSegment), os.O_WRONLY|os.O_APPEND, 0o644)
		if err != nil {
			return nil, fmt.Errorf("persist: opening segment %s: %w", o.ActiveSegment, err)
		}
		size, err := f.Size()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("persist: sizing segment %s: %w", o.ActiveSegment, err)
		}
		a.f, a.segName, a.goodSize, a.segSize, a.statSeg = f, o.ActiveSegment, size, size, o.ActiveSegment
	} else {
		f, name, err := a.createSegment()
		if err != nil {
			return nil, err
		}
		if o.OnRotate != nil {
			if err := o.OnRotate(name); err != nil {
				_ = f.Close()
				return nil, fmt.Errorf("persist: listing segment %s: %w", name, err)
			}
		}
		a.f, a.segName, a.statSeg = f, name, name
	}
	if o.Policy == PolicyEverySec {
		a.ticker = o.NewTicker(syncInterval)
	}
	go a.run()
	return a, nil
}

// createSegment makes a new, empty, durable segment file: the file is created
// exclusively (never reusing a stale file of unknown content), fsynced, and its
// directory entry fsynced.
func (a *AOF) createSegment() (File, string, error) {
	name := SegmentName(a.nextID)
	a.nextID++
	f, err := a.opts.FS.OpenFile(filepath.Join(a.opts.Dir, name), os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o644)
	if err != nil {
		return nil, "", fmt.Errorf("persist: creating segment %s: %w", name, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return nil, "", fmt.Errorf("persist: syncing new segment %s: %w", name, err)
	}
	if err := a.opts.FS.SyncDir(a.opts.Dir); err != nil {
		_ = f.Close()
		return nil, "", fmt.Errorf("persist: syncing %s after creating %s: %w", a.opts.Dir, name, err)
	}
	return f, name, nil
}

func (a *AOF) signalLocked() {
	close(a.notify)
	a.notify = make(chan struct{})
}

func (a *AOF) wakeLocked() {
	select {
	case a.wake <- struct{}{}:
	default:
	}
}

// Admit blocks, outside any lock, while the buffer is at or above HighWater. It
// returns the latched error, ErrClosed or the context's error instead of
// admitting a write that would be refused. Call it before executing a write
// command, not while holding a shard lock.
func (a *AOF) Admit(ctx context.Context) error {
	counted := false
	for {
		a.mu.Lock()
		switch {
		case a.err != nil:
			err := a.err
			a.mu.Unlock()
			return err
		case a.closed:
			a.mu.Unlock()
			return ErrClosed
		case a.pending < a.opts.HighWater:
			a.mu.Unlock()
			return nil
		}
		first := !counted
		if first {
			a.admitWaits++
			counted = true
		}
		ch := a.notify
		a.mu.Unlock()
		if first && a.opts.admitBlocked != nil {
			a.opts.admitBlocked()
		}
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// Append copies complete records into the buffer, in order and contiguously,
// and returns the sequence number of the last one. It never blocks and does no
// I/O, so it is safe to call under a shard lock. It fails only if the AOF is
// latched or closed; see the AOF documentation for what that means for a
// command that was already applied in memory. With no records it returns the
// last assigned sequence.
func (a *AOF) Append(recs ...[]byte) (uint64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err != nil {
		return 0, a.err
	}
	if a.closed {
		return 0, ErrClosed
	}
	if len(recs) == 0 {
		return a.seq, nil
	}
	for _, r := range recs {
		a.buf = append(a.buf, r...)
		a.pending += len(r)
	}
	a.seq += uint64(len(recs))
	a.wakeLocked()
	return a.seq, nil
}

// WaitDurable blocks until the record with sequence seq is fsynced, whatever
// the policy (for everysec and no it asks the flusher to fsync now). It returns
// the latched error if the AOF fails first, and ctx's error if ctx ends.
func (a *AOF) WaitDurable(ctx context.Context, seq uint64) error {
	a.mu.Lock()
	if seq > a.seq {
		last := a.seq
		a.mu.Unlock()
		return fmt.Errorf("persist: WaitDurable(%d) is beyond the last appended sequence %d", seq, last)
	}
	if seq > a.syncReq {
		a.syncReq = seq
		a.wakeLocked()
	}
	for {
		if a.durable >= seq {
			a.mu.Unlock()
			return nil
		}
		if a.err != nil {
			err := a.err
			a.mu.Unlock()
			return err
		}
		select {
		case <-a.done:
			a.mu.Unlock()
			return ErrClosed
		default:
		}
		ch := a.notify
		a.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		a.mu.Lock()
	}
}

// Flush blocks until every record appended before the call has been written to
// the OS (not necessarily fsynced).
func (a *AOF) Flush(ctx context.Context) error {
	a.mu.Lock()
	target := a.seq
	a.wakeLocked()
	for a.written < target {
		if a.err != nil {
			err := a.err
			a.mu.Unlock()
			return err
		}
		ch := a.notify
		a.mu.Unlock()
		select {
		case <-ch:
		case <-ctx.Done():
			return ctx.Err()
		}
		a.mu.Lock()
	}
	a.mu.Unlock()
	return nil
}

// Err returns the latched error, or nil.
func (a *AOF) Err() error {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.err
}

// Stats returns the current counters.
func (a *AOF) Stats() Stats {
	a.mu.Lock()
	defer a.mu.Unlock()
	return Stats{
		Appended: a.seq, Written: a.written, Durable: a.durable,
		PendingBytes: a.pending, BytesWritten: a.bytes,
		Segment: a.statSeg, SegmentSize: a.segSize,
		AdmitWaits: a.admitWaits, Err: a.err,
	}
}

// sealLocked moves the active buffer into the queue, optionally with a rotation
// marker, so the flusher can split the stream exactly at the boundary.
func (a *AOF) sealLocked(r *Rotation) {
	if r == nil && len(a.buf) == 0 {
		return
	}
	a.queue = append(a.queue, chunk{data: a.buf, lastSeq: a.seq, rot: r})
	a.buf, a.spare = a.spare, nil
}

// Rotate asks the flusher to finish the current segment at exactly this point
// in the record stream and continue in a new one. It only marks the boundary in
// memory (no I/O), so it can run while every shard lock is held; Rotation.Wait
// then waits for the flusher outside the locks.
//
// Everything up to the boundary is written and fsynced into the old segment
// before the manifest lists the new one, whatever the policy. Rotate is allowed
// on a latched AOF: it then abandons the untrusted old segment and starts a
// fresh one without listing it (a healing rotation); the caller's checkpoint
// commits the manifest and then calls ClearError.
func (a *AOF) Rotate() (*Rotation, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.closed {
		return nil, ErrClosed
	}
	r := &Rotation{done: make(chan struct{}), seq: a.seq, gen: a.errGen, latched: a.err != nil}
	a.sealLocked(r)
	a.wakeLocked()
	return r, nil
}

// ClearError ends the latched state. r must be a completed healing rotation
// requested while the AOF was already latched, and no new failure may have
// happened since. Call it only after the checkpoint that r belongs to has been
// committed: the new base then covers every record up to r.Seq(), which is
// what makes the log trustworthy again. Otherwise it returns ErrNotHealed and
// the caller should run another checkpoint.
func (a *AOF) ClearError(r *Rotation) error {
	select {
	case <-r.done:
	default:
		return errors.New("persist: rotation has not completed")
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.err == nil {
		return nil
	}
	if r.err != nil || !r.heal || !r.latched || r.gen != a.errGen {
		return ErrNotHealed
	}
	a.err = nil
	a.written = max(a.written, r.seq)
	a.durable = max(a.durable, r.seq)
	a.signalLocked()
	return nil
}

// Close stops accepting records, writes everything buffered, fsyncs it (whatever
// the policy), closes the segment and stops the flusher. If the AOF is latched
// it returns the latched error and logs how many records were never persisted.
// It is idempotent. If ctx ends first Close returns its error and the flusher
// still finishes and exits on its own.
func (a *AOF) Close(ctx context.Context) error {
	a.mu.Lock()
	first := !a.closed
	a.closed = true
	a.signalLocked()
	a.mu.Unlock()
	if first {
		close(a.quit)
	}
	select {
	case <-a.done:
		return a.closeErr
	case <-ctx.Done():
		return ctx.Err()
	}
}

// run is the flusher goroutine.
func (a *AOF) run() {
	defer close(a.done)
	var tick <-chan time.Time
	if a.ticker != nil {
		defer a.ticker.Stop()
		tick = a.ticker.C()
	}
	for {
		var ticked, final bool
		select {
		case <-a.wake:
		case <-tick:
			ticked = true
		case <-a.quit:
			final = true
		}
		a.cycle(ticked, final)
		if final {
			a.finish()
			return
		}
	}
}

func (a *AOF) cycle(ticked, final bool) {
	a.mu.Lock()
	a.sealLocked(nil)
	work := a.queue
	a.queue = nil
	a.mu.Unlock()
	for _, c := range work {
		a.process(c, final)
	}
	a.maybeSync(ticked, final)
}

func (a *AOF) process(c chunk, final bool) {
	if a.Err() != nil {
		a.drop(c)
		if c.rot != nil {
			a.abandonRotation(c.rot, final)
		}
		return
	}
	if len(c.data) > 0 {
		if err := a.write(c.data); err != nil {
			a.latch(fmt.Errorf("writing %s: %w", a.segName, err))
			a.process(c, final) // latched now: drops the chunk, heals a rotation
			return
		}
		a.wrote(c)
	}
	if c.rot != nil {
		if err := a.rotate(c); err != nil {
			a.latch(err)
			c.rot.finish("", err)
		}
	}
}

// write appends data to the active segment. A failed or short write is cut
// back to the last complete write so no torn record stays in the middle of the
// log.
func (a *AOF) write(data []byte) error {
	n, err := a.f.Write(data)
	if err == nil && n < len(data) {
		err = io.ErrShortWrite
	}
	if err != nil {
		if terr := a.f.Truncate(a.goodSize); terr != nil {
			a.log.Warn("could not cut a torn write off the segment; recovery will truncate it",
				"segment", a.segName, "error", terr)
		}
		return err
	}
	a.goodSize += int64(n)
	return nil
}

func (a *AOF) wrote(c chunk) {
	a.mu.Lock()
	a.written = max(a.written, c.lastSeq)
	a.pending -= len(c.data)
	a.bytes += uint64(len(c.data))
	a.segSize = a.goodSize
	if cap(c.data) <= maxSpare {
		a.spare = c.data[:0]
	}
	a.signalLocked()
	a.mu.Unlock()
}

// drop discards a chunk that cannot be written because the AOF is latched. It
// is loud on purpose: these records were accepted and are not persisted.
func (a *AOF) drop(c chunk) {
	if len(c.data) == 0 {
		return
	}
	a.mu.Lock()
	a.pending -= len(c.data)
	a.signalLocked()
	a.mu.Unlock()
	a.log.Error("records dropped: not written because the AOF is in an error state",
		"bytes", len(c.data), "through_seq", c.lastSeq)
}

func (a *AOF) synced(upTo uint64) {
	a.mu.Lock()
	a.written = max(a.written, upTo)
	a.durable = max(a.durable, upTo)
	a.signalLocked()
	a.mu.Unlock()
}

func (a *AOF) maybeSync(ticked, final bool) {
	a.mu.Lock()
	target := a.written
	want := a.err == nil && target > a.durable &&
		(a.opts.Policy == PolicyAlways || ticked || final || a.syncReq > a.durable)
	a.mu.Unlock()
	if !want || a.f == nil {
		return
	}
	if err := a.f.Sync(); err != nil {
		a.latch(fmt.Errorf("syncing %s: %w", a.segName, err))
		return
	}
	a.synced(target)
}

// rotate performs a healthy rotation. The order is the point:
//  1. fsync the old segment, so everything up to the boundary is durable;
//  2. create the new segment durably (file and directory entry);
//  3. only now let OnRotate list the new segment in the manifest;
//  4. switch.
//
// A crash before step 3 leaves a manifest that does not know the new segment; a
// crash after it finds the old segment complete.
func (a *AOF) rotate(c chunk) error {
	if err := a.f.Sync(); err != nil {
		return fmt.Errorf("syncing %s before rotation: %w", a.segName, err)
	}
	a.synced(c.lastSeq)
	nf, name, err := a.createSegment()
	if err != nil {
		return err
	}
	if a.opts.OnRotate != nil {
		if err := a.opts.OnRotate(name); err != nil {
			_ = nf.Close()
			return fmt.Errorf("listing segment %s: %w", name, err)
		}
	}
	if err := a.f.Close(); err != nil {
		a.log.Warn("closing the old segment", "segment", a.segName, "error", err)
	}
	a.setSegment(nf, name)
	c.rot.finish(name, nil)
	return nil
}

// abandonRotation completes a rotation on a latched AOF. The old segment may
// hold a torn or unsynced tail, so its handle is dropped and a fresh segment is
// started without listing it.
func (a *AOF) abandonRotation(r *Rotation, final bool) {
	if final {
		r.finish("", a.Err())
		return
	}
	if a.f != nil {
		if err := a.f.Close(); err != nil {
			a.log.Warn("closing the failed segment", "segment", a.segName, "error", err)
		}
		a.f = nil
	}
	f, name, err := a.createSegment()
	if err != nil {
		r.finish("", err)
		return
	}
	a.setSegment(f, name)
	r.heal = true
	r.finish(name, nil)
}

func (a *AOF) setSegment(f File, name string) {
	a.f, a.segName, a.goodSize = f, name, 0
	a.mu.Lock()
	a.statSeg, a.segSize = name, 0
	a.mu.Unlock()
}

func (a *AOF) latch(cause error) {
	a.mu.Lock()
	first := a.err == nil
	if first {
		a.err = fmt.Errorf("%w: %w", ErrLatched, cause)
		a.errGen++
	}
	unpersisted := a.seq - a.durable
	a.signalLocked()
	a.mu.Unlock()
	if first {
		a.log.Error("persistence error: write commands are refused until persistence recovers",
			"error", cause, "unpersisted_records", unpersisted)
	} else {
		a.log.Warn("further persistence error while latched", "error", cause)
	}
}

// finish runs once, after the final cycle: it closes the segment and records the
// result Close reports.
func (a *AOF) finish() {
	a.mu.Lock()
	err := a.err
	unpersisted := a.seq - a.durable
	a.mu.Unlock()
	if err != nil && unpersisted > 0 {
		a.log.Error("closing with records that were never persisted", "records", unpersisted)
	}
	if a.f != nil {
		if cerr := a.f.Close(); cerr != nil && err == nil {
			err = fmt.Errorf("closing %s: %w", a.segName, cerr)
		}
		a.f = nil
	}
	a.closeErr = err
}
