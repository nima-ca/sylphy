package store

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"sync"
	"sync/atomic"
)

// Snapshots: shard-granular copy-on-write.
//
// Go has no fork, so a point-in-time copy is made lazily, one shard at a time.
//
//  1. BeginSnapshot locks every shard (ascending order), runs the caller's
//     barrier (the AOF rotation: an in-memory marker, no I/O), marks every
//     shard "pending" and unlocks. The pause is O(shards) plus the barrier.
//  2. From then on each shard has two ways to be serialized, whichever comes
//     first, always under that shard's lock and always from its state at step
//     1: Drain walks the shards in order and serializes the ones still pending;
//     a writer that reaches a pending shard serializes it itself before its
//     own change (the gate in lockW). Either way the shard is marked done.
//  3. Drain hands each shard's serialized block to the caller's sink.
//
// Because every writer passes the gate before touching a shard, and a shard is
// serialized exactly once from its start-of-snapshot state, the blocks together
// are the dataset as of step 1 even though they are produced at different
// times. A writer holding several shard locks (Atomic) gates all of them first,
// so a transaction is either wholly in the snapshot or wholly out of it.
//
// Entries are serialized as they are in memory, including keys that have
// expired but not been reaped yet: whether they matter is decided at recovery,
// after the log has been replayed (see BeginReplay).

// ErrSnapshotActive is returned by BeginSnapshot while another snapshot has not
// been closed.
var ErrSnapshotActive = errors.New("store: a snapshot is already in progress")

// ShardEntry is one key as seen by a snapshot encoder.
type ShardEntry struct {
	// Key is the key.
	Key string
	// Value is the live value; it is valid only during the encoder call.
	Value Value
	// DeadlineMs is the absolute Unix-ms deadline, valid when HasDeadline.
	DeadlineMs int64
	// HasDeadline reports that the key has a deadline.
	HasDeadline bool
}

// ShardEncoder serializes one shard. It runs under the shard's lock (the read
// lock when called from Drain, the write lock when called from a writer), so it
// must be fast, must not block, must not call back into the Store and must not
// retain the entries or their values; it has to copy what it needs into the
// returned block.
type ShardEncoder func(shard int, entries iter.Seq[ShardEntry]) ([]byte, error)

// Snapshot is a point-in-time copy in progress. Create it with BeginSnapshot,
// consume it with Drain and always Close it.
type Snapshot struct {
	s    *Store
	enc  ShardEncoder
	bufs byte2d // serialized blocks, indexed by shard

	errMu   sync.Mutex
	err     error
	drained atomic.Bool
}

// byte2d is a slice of serialized shard blocks.
type byte2d = [][]byte

// lockW takes the shard's write lock and passes it through the copy-on-write
// gate: if a snapshot still needs this shard as it is now, serialize it first.
// Every write-lock acquisition in the package must use it; a test enforces that.
func (sh *shard) lockW() {
	sh.mu.Lock()
	if sn := sh.snap.Load(); sn != nil {
		sn.serializeLocked(sh)
		sh.st.gateHits.Add(1)
	}
}

// entries iterates the shard in slot order.
func (sh *shard) entries() iter.Seq[ShardEntry] {
	return func(yield func(ShardEntry) bool) {
		for _, k := range sh.order {
			e := ShardEntry{Key: k, Value: sh.m[k]}
			if at, ok := sh.exp[k]; ok {
				e.DeadlineMs, e.HasDeadline = at, true
			}
			if !yield(e) {
				return
			}
		}
	}
}

// serializeLocked encodes the shard, stores the block and clears the pending
// mark. The caller holds the shard's lock (read or write); only one goroutine
// at a time can be here for a given shard, because the writers hold the write
// lock and Drain is single-threaded.
func (sn *Snapshot) serializeLocked(sh *shard) {
	blk, err := sn.enc(sh.idx, sh.entries())
	if err != nil {
		sn.setErr(fmt.Errorf("store: encoding shard %d: %w", sh.idx, err))
	}
	sn.bufs[sh.idx] = blk
	sh.snap.Store(nil)
}

func (sn *Snapshot) setErr(err error) {
	sn.errMu.Lock()
	defer sn.errMu.Unlock()
	if sn.err == nil {
		sn.err = err
	}
}

func (sn *Snapshot) firstErr() error {
	sn.errMu.Lock()
	defer sn.errMu.Unlock()
	return sn.err
}

// BeginSnapshot establishes the point in time. It locks every shard in
// ascending order, runs barrier (if non-nil) while they are all held, and marks
// every shard pending. barrier must follow the closure rules on Store; the AOF
// rotation marker is what it is for, so that the log splits exactly at the
// snapshot. If barrier fails nothing is started and its error is returned.
// Only one snapshot may exist at a time (ErrSnapshotActive).
func (s *Store) BeginSnapshot(barrier func() error, enc ShardEncoder) (*Snapshot, error) {
	if enc == nil {
		return nil, errors.New("store: BeginSnapshot needs an encoder")
	}
	sn := &Snapshot{s: s, enc: enc, bufs: make(byte2d, len(s.shards))}
	s.snapMu.Lock()
	if s.snapActive != nil {
		s.snapMu.Unlock()
		return nil, ErrSnapshotActive
	}
	s.snapActive = sn
	s.snapMu.Unlock()

	err := s.withAllLocked(func() error {
		if barrier != nil {
			if err := barrier(); err != nil {
				return err
			}
		}
		for i := range s.shards {
			s.shards[i].snap.Store(sn)
		}
		return nil
	})
	if err != nil {
		s.releaseSnapshot(sn)
		return nil, err
	}
	return sn, nil
}

func (s *Store) releaseSnapshot(sn *Snapshot) {
	s.snapMu.Lock()
	if s.snapActive == sn {
		s.snapActive = nil
	}
	s.snapMu.Unlock()
}

// Drain walks the shards in index order, serializing the ones no writer has
// serialized yet, and passes each shard's block to sink in order, exactly once
// per shard. The shard lock is not held while sink runs. Blocks are released as
// soon as sink returns, so memory held is only what writers serialized ahead of
// the walk. Drain may be called once. It returns the first encoder error, sink
// error or context error; after an error the caller must still Close.
func (sn *Snapshot) Drain(ctx context.Context, sink func(shard int, block []byte) error) error {
	if !sn.drained.CompareAndSwap(false, true) {
		return errors.New("store: snapshot already drained")
	}
	s := sn.s
	for i := range s.shards {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := sn.firstErr(); err != nil {
			return err
		}
		sh := &s.shards[i]
		sh.mu.RLock()
		if sh.snap.Load() == sn {
			sn.serializeLocked(sh)
		}
		blk := sn.bufs[i]
		sh.mu.RUnlock()
		if err := sn.firstErr(); err != nil {
			return err
		}
		sn.bufs[i] = nil
		if err := sink(i, blk); err != nil {
			return err
		}
	}
	return sn.firstErr()
}

// Close ends the snapshot: shards that are still pending stop being pending
// (so writers stop serializing) and buffered blocks are dropped. Call it after
// Drain has returned, on success and on error alike. It is idempotent.
func (sn *Snapshot) Close() {
	s := sn.s
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock() // not lockW: this must not serialize the shard
		if sh.snap.Load() == sn {
			sh.snap.Store(nil)
		}
		sn.bufs[i] = nil
		sh.mu.Unlock()
	}
	s.releaseSnapshot(sn)
}
