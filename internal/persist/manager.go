package persist

import (
	"context"
	"errors"
	"fmt"
	"io"
	"iter"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/store"
)

// Mode selects how much of persistence is on.
type Mode uint8

const (
	// ModeSnapshot persists only at checkpoints (SAVE, BGSAVE, shutdown): there
	// is no append-only file, so a crash loses everything since the last one.
	ModeSnapshot Mode = iota + 1
	// ModeAOF logs every write to the append-only file and also checkpoints.
	ModeAOF
)

// String returns "snapshot" or "aof".
func (m Mode) String() string {
	switch m {
	case ModeSnapshot:
		return "snapshot"
	case ModeAOF:
		return "aof"
	}
	return fmt.Sprintf("Mode(%d)", uint8(m))
}

// Errors returned by the Manager.
var (
	// ErrCheckpointRunning is returned when a checkpoint is already in progress.
	ErrCheckpointRunning = errors.New("persist: a checkpoint is already in progress")
	// ErrManagerClosed is returned after Shutdown.
	ErrManagerClosed = errors.New("persist: the persistence manager is shut down")
)

// failureBackoffTicks is how many one-second ticks the automatic trigger waits
// after a failed checkpoint before trying again.
const failureBackoffTicks = 10

// ManagerOptions configures NewManager.
type ManagerOptions struct {
	// FS is the file system; required.
	FS FS
	// Dir is the data directory; required.
	Dir string
	// Store is the keyspace to persist and recover into; required.
	Store *store.Store
	// Registry executes logged commands during recovery; required.
	Registry *command.Registry
	// Config is passed to the recovery loader client. Nil means config.Default.
	Config *config.Config
	// Mode is ModeSnapshot or ModeAOF; required.
	Mode Mode
	// Policy is the AOF fsync policy (ModeAOF only).
	Policy Policy
	// HighWater is the AOF backpressure threshold in bytes (0: default).
	HighWater int
	// AutoRewritePercentage triggers a checkpoint when the log has grown by this
	// percentage over the base snapshot's size (and AutoRewriteMinSize has been
	// reached). 0 disables the automatic trigger (ModeAOF only).
	AutoRewritePercentage int
	// AutoRewriteMinSize is the smallest base+log size, in bytes, at which the
	// automatic trigger may fire.
	AutoRewriteMinSize int64
	// LoadTruncated repairs a partial record at the end of the last segment.
	LoadTruncated bool
	// SnapshotOnShutdown takes a final checkpoint in Shutdown.
	SnapshotOnShutdown bool
	// NewTicker creates tickers: the one-second automatic-trigger ticker, and
	// the AOF's everysec ticker (the AOF's is created first). Nil uses real
	// time.
	NewTicker func(time.Duration) Ticker
	// Now supplies wall-clock time for LastSave. Nil means time.Now.
	Now func() time.Time
	// Logger receives persistence messages. Nil discards.
	Logger *slog.Logger
}

// Manager owns persistence for one store: recovery at startup, the AOF in
// ModeAOF, checkpoints (SAVE, BGSAVE, BGREWRITEAOF, automatic, and at
// shutdown), expiry logging, and an orderly shutdown.
//
// Lifecycle: NewManager, Recover, Start, then serve; Shutdown at the end.
//
// Checkpoint ("unified": the same procedure answers BGSAVE and BGREWRITEAOF,
// because the AOF is built from a base snapshot plus a log segment, so writing
// a new base and starting a fresh segment is both a snapshot and a rewrite):
//
//  1. Under every shard lock: mark the point in time (BeginSnapshot) and, in
//     ModeAOF, mark the AOF rotation boundary. Nothing is written yet.
//  2. Outside the locks: the AOF finishes the old segment and starts the new
//     one (see AOF.Rotate for the fsync ordering); the shards are serialized
//     (by this goroutine, or by writers that reach a shard first) into a
//     temporary base file.
//  3. Commit: fsync the base, rename it, fsync the directory; then write the
//     manifest {new base, [new segment]} (its rename is the commit point);
//     then delete the files it retired.
//
// A crash anywhere leaves either the old or the new manifest, both consistent.
//
// Goroutines (each has an owner and a shutdown path): the AOF flusher, owned by
// the AOF and stopped by Shutdown; the automatic trigger, owned by the Manager
// and stopped by closing quit; BGSAVE workers, tracked by a WaitGroup and
// cancelled through bgCtx.
type Manager struct {
	o   ManagerOptions
	log *slog.Logger

	bgCtx    context.Context
	bgCancel context.CancelFunc
	quit     chan struct{}
	wg       sync.WaitGroup

	mu        sync.Mutex
	manifest  Manifest
	baseSize  int64
	carry     int64  // log bytes that existed before the AOF's own counters began
	mark      uint64 // AOF BytesWritten at the last checkpoint's barrier
	lastSave  time.Time
	nextBase  uint64
	running   bool
	closing   bool
	recovered bool
	started   bool
	res       RecoverResult
	aof       *AOF
	autoDone  chan struct{}

	skipTicks   atomic.Int32
	checkpoints atomic.Int64 // finished checkpoint attempts, successful or not

	shutOnce sync.Once
	shutErr  error
}

// NewManager validates o and returns a Manager. It touches nothing on disk.
func NewManager(o ManagerOptions) (*Manager, error) {
	switch {
	case o.FS == nil || o.Dir == "" || o.Store == nil || o.Registry == nil:
		return nil, errors.New("persist: ManagerOptions needs FS, Dir, Store and Registry")
	case o.Mode != ModeSnapshot && o.Mode != ModeAOF:
		return nil, fmt.Errorf("persist: invalid persistence mode %d", o.Mode)
	case o.Policy > PolicyNo:
		return nil, fmt.Errorf("persist: invalid fsync policy %d", o.Policy)
	case o.AutoRewritePercentage < 0 || o.AutoRewriteMinSize < 0 || o.HighWater < 0:
		return nil, errors.New("persist: negative size or percentage option")
	}
	if o.Config == nil {
		c := config.Default()
		o.Config = &c
	}
	if o.Logger == nil {
		o.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	if o.NewTicker == nil {
		o.NewTicker = newRealTicker
	}
	if o.Now == nil {
		o.Now = time.Now
	}
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{o: o, log: o.Logger, bgCtx: ctx, bgCancel: cancel, quit: make(chan struct{})}, nil
}

// Recover rebuilds the dataset from the data directory (see the package-level
// Recover). It must run before Start and before the listener or the expiry
// sweeper start.
func (m *Manager) Recover(ctx context.Context) (RecoveryReport, error) {
	res, err := Recover(ctx, RecoverOptions{
		FS: m.o.FS, Dir: m.o.Dir, Target: m.o.Store,
		Replay:        NewCommandReplayer(m.o.Registry, m.o.Store, m.o.Config),
		LoadTruncated: m.o.LoadTruncated, Logger: m.log, Now: m.o.Now,
	})
	if err != nil {
		return RecoveryReport{}, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.res, m.manifest, m.baseSize, m.nextBase, m.recovered = res, res.Manifest, res.Manifest.Base.Size, res.NextID, true
	return res.Report, nil
}

// Start brings persistence up after Recover. In ModeAOF it opens the AOF at the
// recovered point, installs the expiry hook, and only then purges the keys that
// expired while the server was down: every purged key is logged as DEL and
// fsynced before Start returns, so a later write to such a key can never replay
// against the stale copy still stored in the base snapshot. In ModeSnapshot the
// purge needs no log. It then starts the automatic checkpoint trigger.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	switch {
	case !m.recovered:
		m.mu.Unlock()
		return errors.New("persist: Start called before Recover")
	case m.closing:
		m.mu.Unlock()
		return ErrManagerClosed
	case m.started:
		m.mu.Unlock()
		return nil
	}
	res := m.res
	m.lastSave = m.o.Now()
	m.mu.Unlock()

	if m.o.Mode == ModeSnapshot {
		if n := m.o.Store.PurgeExpired(); n > 0 {
			m.log.Info("dropped keys that expired while the server was down", "keys", n)
		}
		m.mu.Lock()
		m.started = true
		m.mu.Unlock()
		return nil
	}

	carry := m.segmentBytes(res.Manifest.Segments)
	aof, err := OpenAOF(AOFOptions{
		FS: m.o.FS, Dir: m.o.Dir, ActiveSegment: res.ActiveSegment, NextSegmentID: res.NextID,
		Policy: m.o.Policy, HighWater: m.o.HighWater, NewTicker: m.o.NewTicker,
		OnRotate: m.listSegment, Logger: m.log,
	})
	if err != nil {
		return fmt.Errorf("persist: opening the append-only file: %w", err)
	}
	m.mu.Lock()
	m.aof, m.carry = aof, carry
	m.mu.Unlock()

	m.o.Store.SetExpireHook(expireHook(aof))
	if n := m.o.Store.PurgeExpired(); n > 0 {
		seq, err := aof.Append() // with no records: the last sequence assigned
		if err == nil {
			err = aof.WaitDurable(ctx, seq)
		}
		if err != nil {
			m.o.Store.SetExpireHook(nil)
			_ = aof.Close(context.WithoutCancel(ctx))
			m.mu.Lock()
			m.aof = nil
			m.mu.Unlock()
			return fmt.Errorf("persist: making the expiry purge durable: %w", err)
		}
		m.log.Info("purged keys that expired while the server was down", "keys", n)
	}

	tick := m.o.NewTicker(time.Second)
	done := make(chan struct{})
	m.mu.Lock()
	m.autoDone, m.started = done, true
	m.mu.Unlock()
	go m.autoLoop(tick, done)
	return nil
}

// expireHook returns the store hook that logs DEL for each expired key, inside
// the shard lock, so the DEL sits at the key's own place in the log.
func expireHook(aof *AOF) func(string) {
	return func(key string) {
		_, _ = aof.Append(command.EncodeRecord(nil, [][]byte{[]byte("DEL"), []byte(key)}))
	}
}

// segmentBytes returns the total size of the named segments.
func (m *Manager) segmentBytes(names []string) int64 {
	var total int64
	for _, n := range names {
		f, err := m.o.FS.OpenFile(filepath.Join(m.o.Dir, n), os.O_RDONLY, 0)
		if err != nil {
			continue
		}
		if sz, err := f.Size(); err == nil {
			total += sz
		}
		_ = f.Close()
	}
	return total
}

// Propagator returns the AOF for the journal, or nil in ModeSnapshot (and
// before Start), where writes are not logged.
func (m *Manager) Propagator() command.Propagator {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.aof == nil {
		return nil
	}
	return m.aof
}

// Err returns the error that put the AOF into its failed state, or nil. While
// it is non-nil writes are refused (MISCONF) until a checkpoint heals the log.
func (m *Manager) Err() error {
	m.mu.Lock()
	aof := m.aof
	m.mu.Unlock()
	if aof == nil {
		return nil
	}
	return aof.Err()
}

// LastSave returns when persistence last reached a checkpoint, or when it
// started if there has not been one.
func (m *Manager) LastSave() time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.lastSave
}

// listSegment is the AOF's OnRotate callback: it commits a manifest that lists
// the new segment, which the AOF has already created durably.
func (m *Manager) listSegment(seg string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := m.manifest
	next.Generation++
	next.Segments = append(slices.Clone(m.manifest.Segments), seg)
	if err := CommitManifest(m.o.FS, m.o.Dir, next); err != nil {
		return err
	}
	m.manifest = next
	return nil
}

// claim reserves the right to run a checkpoint.
func (m *Manager) claim() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch {
	case m.closing:
		return ErrManagerClosed
	case !m.started:
		return errors.New("persist: the persistence manager is not started")
	case m.running:
		return ErrCheckpointRunning
	}
	m.running = true
	m.wg.Add(1)
	return nil
}

func (m *Manager) release() {
	m.mu.Lock()
	m.running = false
	m.mu.Unlock()
	m.wg.Done()
}

// Save runs a checkpoint and returns when it is committed (SAVE). It returns
// ErrCheckpointRunning if one is already in progress, and is aborted by ctx or
// by Shutdown.
func (m *Manager) Save(ctx context.Context) error {
	if err := m.claim(); err != nil {
		return err
	}
	defer m.release()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	stop := context.AfterFunc(m.bgCtx, cancel)
	defer stop()
	return m.runCheckpoint(ctx)
}

// BGSave starts a checkpoint in the background and returns at once (BGSAVE and
// BGREWRITEAOF). It returns ErrCheckpointRunning if one is already in progress.
// The outcome is logged and reflected in LastSave.
func (m *Manager) BGSave() error {
	if err := m.claim(); err != nil {
		return err
	}
	go func() {
		defer m.release()
		_ = m.runCheckpoint(m.bgCtx)
	}()
	return nil
}

func (m *Manager) runCheckpoint(ctx context.Context) error {
	t0 := m.o.Now()
	err := m.checkpoint(ctx)
	if err != nil {
		m.skipTicks.Store(failureBackoffTicks)
		m.log.Error("checkpoint failed", "error", err, "elapsed", m.o.Now().Sub(t0))
	} else {
		m.skipTicks.Store(0)
		m.log.Info("checkpoint complete", "elapsed", m.o.Now().Sub(t0))
	}
	m.checkpoints.Add(1)
	return err
}

type shardCount struct{ entries, blocks int }

// ckptEncoder serializes shards for a checkpoint. counts[i] is written once,
// by the single call that encodes shard i, and read after that shard's lock
// has been released and retaken by Drain, so it needs no lock of its own.
type ckptEncoder struct{ counts []shardCount }

func (c *ckptEncoder) encode(shard int, entries iter.Seq[store.ShardEntry]) ([]byte, error) {
	var bb BlockBuffer
	for e := range entries {
		if err := bb.Add(e.Key, e.Value, e.DeadlineMs, e.HasDeadline); err != nil {
			return nil, err
		}
	}
	framed, n, blocks := bb.Finish()
	c.counts[shard] = shardCount{entries: n, blocks: blocks}
	return framed, nil
}

// checkpoint is the procedure described on Manager. The caller owns the
// "running" claim (or, at shutdown, is the only one left).
func (m *Manager) checkpoint(ctx context.Context) error {
	st := m.o.Store
	m.mu.Lock()
	baseID := m.nextBase
	m.nextBase++
	aof := m.aof
	m.mu.Unlock()

	enc := &ckptEncoder{counts: make([]shardCount, st.ShardCount())}
	var rot *Rotation
	var barrierWritten uint64
	barrier := func() error {
		if aof == nil {
			return nil
		}
		r, err := aof.Rotate()
		if err != nil {
			return err
		}
		rot = r
		barrierWritten = aof.Stats().BytesWritten
		return nil
	}
	snap, err := st.BeginSnapshot(barrier, enc.encode)
	if err != nil {
		return fmt.Errorf("persist: starting the snapshot: %w", err)
	}
	defer snap.Close()

	// The rotation is awaited before any base data is written: it needs the new
	// segment to exist, and failing here is cheap.
	var newSeg string
	if rot != nil {
		if newSeg, err = rot.Wait(ctx); err != nil {
			return fmt.Errorf("persist: rotating the log: %w", err)
		}
	}
	bw, err := CreateBase(m.o.FS, m.o.Dir, baseID)
	if err != nil {
		return err
	}
	defer bw.Abort() // a no-op once Commit has run
	err = snap.Drain(ctx, func(shard int, block []byte) error {
		c := enc.counts[shard]
		return bw.WriteFramed(block, c.entries, c.blocks)
	})
	if err != nil {
		return fmt.Errorf("persist: writing the snapshot: %w", err)
	}
	snap.Close() // every shard is done; stop gating writers now
	ref, err := bw.Commit()
	if err != nil {
		return err
	}

	m.mu.Lock()
	old := m.manifest
	next := Manifest{Generation: old.Generation + 1, Base: ref}
	if newSeg != "" {
		next.Segments = []string{newSeg}
	}
	m.mu.Unlock()
	if err := CommitManifest(m.o.FS, m.o.Dir, next); err != nil {
		return err
	}
	m.mu.Lock()
	m.manifest, m.baseSize, m.carry, m.mark = next, ref.Size, 0, barrierWritten
	m.lastSave = m.o.Now()
	m.mu.Unlock()

	// The new base now covers every record the failed log could not persist, so
	// the log is trustworthy again.
	if aof != nil && rot != nil && aof.Err() != nil {
		if err := aof.ClearError(rot); err != nil {
			m.log.Warn("the log is still in its error state after the checkpoint", "error", err)
		} else {
			m.log.Info("persistence recovered: writes are accepted again")
		}
	}

	retire := []string{old.Base.Name}
	for _, s := range old.Segments {
		if s != newSeg {
			retire = append(retire, s)
		}
	}
	if err := RetireFiles(m.o.FS, m.o.Dir, retire...); err != nil {
		m.log.Warn("could not delete files the checkpoint retired; recovery will", "error", err)
	}
	return nil
}

// shouldCheckpoint is the automatic trigger: the log must have grown by at
// least pct percent over the base snapshot and base plus log must have reached
// minSize. A zero percentage disables it. A missing base counts as one byte, so
// then only minSize matters.
func shouldCheckpoint(baseSize, incr int64, pct int, minSize int64) bool {
	if pct <= 0 || incr <= 0 || baseSize+incr < minSize {
		return false
	}
	return incr*100 >= max(baseSize, 1)*int64(pct)
}

// incrementalBytes estimates the log written since the last checkpoint.
func (m *Manager) incrementalBytes() int64 {
	m.mu.Lock()
	aof, carry, mark := m.aof, m.carry, m.mark
	m.mu.Unlock()
	if aof == nil {
		return 0
	}
	return int64(aof.Stats().BytesWritten-mark) + carry
}

// autoDue reports whether the automatic trigger should start a checkpoint now:
// when the log has outgrown the base, or when the log is in its error state,
// because a checkpoint is the only way out of it.
func (m *Manager) autoDue() bool {
	m.mu.Lock()
	aof, base := m.aof, m.baseSize
	m.mu.Unlock()
	if aof == nil {
		return false
	}
	if aof.Err() != nil {
		return true
	}
	return shouldCheckpoint(base, m.incrementalBytes(), m.o.AutoRewritePercentage, m.o.AutoRewriteMinSize)
}

func (m *Manager) autoLoop(tick Ticker, done chan struct{}) {
	defer close(done)
	defer tick.Stop()
	for {
		select {
		case <-m.quit:
			return
		case <-tick.C():
		}
		if n := m.skipTicks.Load(); n > 0 {
			m.skipTicks.Add(-1)
			continue
		}
		if !m.autoDue() {
			continue
		}
		if err := m.BGSave(); err != nil && !errors.Is(err, ErrCheckpointRunning) && !errors.Is(err, ErrManagerClosed) {
			m.log.Warn("could not start the automatic checkpoint", "error", err)
		}
	}
}

// Shutdown stops persistence in order: it stops the automatic trigger and
// aborts a checkpoint in flight, takes a final checkpoint if configured (ctx
// cancels it), then flushes and fsyncs the AOF and stops its goroutine. The AOF
// is closed even if ctx is already cancelled, so cancelling only skips the final
// checkpoint, never durability. It is idempotent and returns every error it hit.
func (m *Manager) Shutdown(ctx context.Context) error {
	m.shutOnce.Do(func() { m.shutErr = m.shutdown(ctx) })
	return m.shutErr
}

func waitFor(ctx context.Context, wg *sync.WaitGroup) error {
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (m *Manager) shutdown(ctx context.Context) error {
	m.mu.Lock()
	m.closing = true
	aof, started, autoDone := m.aof, m.started, m.autoDone
	m.mu.Unlock()
	close(m.quit)
	m.bgCancel()

	var errs []error
	if err := waitFor(ctx, &m.wg); err != nil {
		errs = append(errs, fmt.Errorf("persist: waiting for a checkpoint to stop: %w", err))
	}
	if autoDone != nil {
		select {
		case <-autoDone:
		case <-ctx.Done():
			errs = append(errs, fmt.Errorf("persist: waiting for the automatic trigger to stop: %w", ctx.Err()))
		}
	}
	if started && m.o.SnapshotOnShutdown {
		if err := ctx.Err(); err != nil {
			errs = append(errs, fmt.Errorf("persist: skipping the final checkpoint: %w", err))
		} else if err := m.runCheckpoint(ctx); err != nil {
			errs = append(errs, fmt.Errorf("persist: final checkpoint: %w", err))
		}
	}
	if aof != nil {
		m.o.Store.SetExpireHook(nil)
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
		if err := aof.Close(cctx); err != nil {
			errs = append(errs, fmt.Errorf("persist: closing the append-only file: %w", err))
		}
		cancel()
	}
	return errors.Join(errs...)
}
