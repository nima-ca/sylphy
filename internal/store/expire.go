package store

import (
	"errors"
	"sync"
	"time"
)

// Expiry model
//
// Each shard keeps an index (exp) from key to an absolute deadline in Unix
// milliseconds, guarded by the same lock as the keys themselves. Invariant:
// every key in exp is also in the shard's value map; every code path that
// removes a key removes its deadline in the same critical section.
//
// A key is expired when now > deadline: a key whose deadline equals the
// current millisecond is still alive, matching Redis. Expired keys are removed
// two ways:
//
//   - lazily, by any access that touches the key (reaping needs the write
//     lock, so read paths upgrade only when they actually find a stale key);
//   - actively, by the sweeper goroutine, which samples the index so keys that
//     are never touched again do not leak memory.
//
// Pure scans that hold only a read lock (Keys, Len, AtomicView) skip expired
// keys without removing them, so they never block writers longer than needed.

// SweepConfig tunes the active expiry sweeper.
type SweepConfig struct {
	// TickInterval is the time between sweep cycles.
	TickInterval time.Duration
	// SampleSize is the number of expiring keys examined per shard per batch.
	SampleSize int
	// CycleBudget caps the real time one cycle may spend.
	CycleBudget time.Duration
	// StaleThreshold is the expired fraction of a batch, in [0, 1), above
	// which the same shard is sampled again.
	StaleThreshold float64
}

// DefaultSweepConfig returns the defaults: 100ms ticks, 20 keys per batch, a
// 25ms budget and a 25% stale threshold.
func DefaultSweepConfig() SweepConfig {
	return SweepConfig{
		TickInterval:   100 * time.Millisecond,
		SampleSize:     20,
		CycleBudget:    25 * time.Millisecond,
		StaleThreshold: 0.25,
	}
}

// Validate reports whether the configuration is usable.
func (c SweepConfig) Validate() error {
	switch {
	case c.TickInterval <= 0:
		return errors.New("store: sweep tick interval must be positive")
	case c.SampleSize <= 0:
		return errors.New("store: sweep sample size must be positive")
	case c.CycleBudget <= 0:
		return errors.New("store: sweep cycle budget must be positive")
	case c.StaleThreshold < 0 || c.StaleThreshold >= 1:
		return errors.New("store: sweep stale threshold must be in [0, 1)")
	}
	return nil
}

// Stats is a snapshot of the store's expiry counters.
type Stats struct {
	// ExpiredLazy counts keys removed because an access found them expired.
	ExpiredLazy int64
	// ExpiredActive counts keys removed by the sweeper.
	ExpiredActive int64
	// SweepCycles counts completed sweeper cycles.
	SweepCycles int64
}

// Stats returns the current expiry counters.
func (s *Store) Stats() Stats {
	return Stats{
		ExpiredLazy:   s.expiredLazy.Load(),
		ExpiredActive: s.expiredActive.Load(),
		SweepCycles:   s.sweepCycles.Load(),
	}
}

// ExpireAt returns the absolute deadline of key in Unix milliseconds, -2 if
// the key does not exist (or has expired) and -1 if it has no deadline: the
// same sentinels TTL/PTTL/EXPIRETIME report.
func (s *Store) ExpireAt(key string) int64 {
	res := int64(-2)
	_ = s.read(key, func(sh *shard, v Value) error {
		switch {
		case v == nil:
		case hasDeadline(sh, key):
			res = sh.exp[key]
		default:
			res = -1
		}
		return nil
	})
	return res
}

func hasDeadline(sh *shard, key string) bool {
	_, ok := sh.exp[key]
	return ok
}

// lifecycle owns the sweeper goroutine. Only Start launches it and only Close
// stops it; Close waits for the goroutine to exit.
type lifecycle struct {
	mu      sync.Mutex
	started bool
	closed  bool
	stop    chan struct{}
	wg      sync.WaitGroup
}

// Start launches the active expiry sweeper. It is idempotent, and a no-op
// after Close. A Store that is never started still expires keys lazily.
func (s *Store) Start() {
	t := time.NewTicker(s.sweep.TickInterval)
	if !s.startWith(t.C, t.Stop) {
		t.Stop()
	}
}

// Close stops the sweeper and waits for it to exit. It is idempotent and safe
// to call without Start. The store stays usable afterwards, without active
// expiry.
func (s *Store) Close() {
	s.lc.mu.Lock()
	if !s.lc.closed {
		s.lc.closed = true
		close(s.lc.stop)
	}
	s.lc.mu.Unlock()
	s.lc.wg.Wait() // outside the lock: Start never Adds after closed is set
}

// startWith launches the sweeper driven by tick; release runs when the
// goroutine exits. Tests pass their own channel to drive cycles
// deterministically.
func (s *Store) startWith(tick <-chan time.Time, release func()) bool {
	s.lc.mu.Lock()
	defer s.lc.mu.Unlock()
	if s.lc.started || s.lc.closed {
		return false
	}
	s.lc.started = true
	s.lc.wg.Add(1)
	go func() {
		defer s.lc.wg.Done()
		defer release()
		s.runSweeper(tick)
	}()
	return true
}

func (s *Store) runSweeper(tick <-chan time.Time) {
	next := 0
	for {
		select {
		case <-s.lc.stop:
			return
		case <-tick:
			next = s.sweepCycle(next).Next
			s.sweepCycles.Add(1)
		}
	}
}

// cycleResult describes one sweep cycle.
type cycleResult struct {
	Sampled         int  // keys examined
	Expired         int  // keys removed
	BudgetExhausted bool // stopped early because the time budget ran out
	Next            int  // shard to start from next cycle
}

// sweepCycle runs one active-expiry cycle starting at shard start. For each
// shard it samples up to SampleSize expiring keys, removes the expired ones,
// and samples again while more than StaleThreshold of a full batch was
// expired. The cycle stops once CycleBudget has elapsed, checked after every
// batch so each cycle makes progress. It resumes at the interrupted shard next
// time, so a busy shard cannot starve the others.
//
// Sampling takes the first SampleSize entries of a Go map range, which starts
// at a random position; like Redis this is cheap and only approximately
// uniform, which is fine for deciding when to stop.
func (s *Store) sweepCycle(start int) cycleResult {
	cfg := s.sweep
	n := len(s.shards)
	mask := int(s.mask)
	t0 := s.wall()
	res := cycleResult{Next: (start + 1) & mask}
	for i := 0; i < n; i++ {
		idx := (start + i) & mask
		sh := &s.shards[idx]
		for {
			sampled, expired := sh.sample(s.clock.NowMs(), cfg.SampleSize)
			res.Sampled += sampled
			res.Expired += expired
			s.expiredActive.Add(int64(expired))
			// A short batch means the whole index was visited; nothing more to find.
			again := sampled == cfg.SampleSize &&
				float64(expired) > cfg.StaleThreshold*float64(sampled)
			over := s.wall().Sub(t0) >= cfg.CycleBudget
			if over && (again || i < n-1) {
				res.BudgetExhausted = true
				if again {
					res.Next = idx
				} else {
					res.Next = (idx + 1) & mask
				}
				return res
			}
			if !again {
				break
			}
		}
	}
	return res
}

// sample examines up to size expiring keys and removes the expired ones. The
// write lock is held for at most size map operations.
func (sh *shard) sample(now int64, size int) (sampled, expired int) {
	sh.mu.Lock()
	defer sh.mu.Unlock()
	for k, at := range sh.exp {
		if sampled == size {
			break
		}
		sampled++
		if now > at {
			sh.remove(k) // deleting during range is safe in Go
			expired++
		}
	}
	return sampled, expired
}
