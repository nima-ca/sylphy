package store

import (
	"errors"
	"fmt"
	"runtime"
	"sync/atomic"
	"testing"
	"time"
)

// expEpochMs is the fake clock's starting time for the expiry tests.
const expEpochMs = 1_700_000_000_000

// errStop lets a closure abort an operation so that nothing is written.
var errStop = errors.New("stop")

// manualSweep returns a SweepConfig for tests that drive cycles by hand: the
// tick interval is irrelevant (tests pass their own tick channel).
func manualSweep(sample int, stale float64, budget time.Duration) SweepConfig {
	return SweepConfig{TickInterval: time.Hour, SampleSize: sample, CycleBudget: budget, StaleThreshold: stale}
}

// newExpStore returns a store driven by a FakeClock starting at expEpochMs.
// The zero SweepConfig selects the defaults. Close is registered as cleanup.
func newExpStore(t testing.TB, shards int, sweep SweepConfig) (*Store, *FakeClock) {
	t.Helper()
	clk := NewFakeClock(expEpochMs)
	s, err := NewWithOptions(Options{Shards: shards, Clock: clk, Sweep: sweep})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(s.Close)
	return s, clk
}

// putTTL stores a string under key that expires at the absolute atMs.
func putTTL(t testing.TB, s *Store, key string, atMs int64) {
	t.Helper()
	err := s.MutateEntry(key, func(e *Entry) error {
		e.Put(NewString([]byte("v")))
		e.Persist()
		e.SetExpireAt(atMs)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// putMany stores n keys named prefix0..prefix(n-1). atMs == 0 means no TTL.
func putMany(t testing.TB, s *Store, prefix string, n int, atMs int64) []string {
	t.Helper()
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("%s%d", prefix, i)
		if atMs == 0 {
			s.Set(keys[i], []byte("v"))
		} else {
			putTTL(t, s, keys[i], atMs)
		}
	}
	return keys
}

// seedExpiring stores "k" expiring at expEpochMs+1000 and a persistent "other".
func seedExpiring(t testing.TB, s *Store) {
	t.Helper()
	putTTL(t, s, "k", expEpochMs+1000)
	s.Set("other", []byte("o"))
}

// rawHas looks at the shard maps directly, bypassing lazy expiry, to tell
// "invisible because skipped" from "physically removed".
func rawHas(s *Store, key string) (inMap, inExp bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	_, inMap = sh.m[key]
	_, inExp = sh.exp[key]
	return inMap, inExp
}

func shardLen(s *Store, idx int) int {
	sh := &s.shards[idx]
	sh.mu.RLock()
	defer sh.mu.RUnlock()
	return len(sh.m)
}

// rawCount is the number of physically stored keys, expired or not.
func rawCount(s *Store) int {
	n := 0
	for i := range s.shards {
		n += shardLen(s, i)
	}
	return n
}

// checkExpiryInvariant verifies that every deadline has a value (expire.go).
func checkExpiryInvariant(t testing.TB, s *Store) {
	t.Helper()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		for k := range sh.exp {
			if _, ok := sh.m[k]; !ok {
				t.Errorf("shard %d: key %q has a deadline but no value", i, k)
			}
		}
		sh.mu.RUnlock()
	}
}

// pollUntil spins (yielding, never sleeping) until cond holds, failing after a
// generous real-time deadline. It only synchronizes with goroutines; all
// expiry logic is driven by the fake clock.
func pollUntil(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		runtime.Gosched()
	}
}

func waitForGoroutines(t testing.TB, want int) {
	t.Helper()
	pollUntil(t, "goroutines to exit", func() bool { return runtime.NumGoroutine() <= want })
	if got := runtime.NumGoroutine(); got > want {
		t.Fatalf("goroutine leak: have %d, want <= %d", got, want)
	}
}

// stepWall replaces the sweeper's real-time source with one that advances by
// step on every reading, so cycle budgets become deterministic. Call it before
// any sweeper goroutine runs.
func stepWall(s *Store, step time.Duration) {
	now := time.Unix(0, 0)
	s.wall = func() time.Time {
		now = now.Add(step)
		return now
	}
}

// --- lazy expiry ---

// accessPath is one way of reaching a key. visible reports whether the path
// sees key as present; reaps whether the path physically removes an expired
// key (write-lock paths) or merely skips it (read-lock scans).
type accessPath struct {
	name    string
	reaps   bool
	visible func(s *Store, key string) bool
}

func accessPaths() []accessPath {
	return []accessPath{
		{"Get", true, func(s *Store, k string) bool { _, ok := s.Get(k); return ok }},
		{"Exists", true, func(s *Store, k string) bool { return s.Exists(k) == 1 }},
		{"View", true, func(s *Store, k string) (seen bool) {
			_ = s.View(k, func(v Value) error { seen = v != nil; return nil })
			return seen
		}},
		{"Mutate", true, func(s *Store, k string) (seen bool) {
			_ = s.Mutate(k, func(cur Value) (Value, error) { seen = cur != nil; return cur, nil })
			return seen
		}},
		{"MutateEntry", true, func(s *Store, k string) (seen bool) {
			_ = s.MutateEntry(k, func(e *Entry) error { seen = e.Exists(); return nil })
			return seen
		}},
		{"Update", true, func(s *Store, k string) (seen bool) {
			_ = s.Update(k, func(_ []byte, exists bool) ([]byte, error) { seen = exists; return nil, errStop })
			return seen
		}},
		{"ExpireAt", true, func(s *Store, k string) bool { return s.ExpireAt(k) != -2 }},
		{"Delete", true, func(s *Store, k string) bool { return s.Delete(k) == 1 }},
		{"Atomic", true, func(s *Store, k string) (seen bool) {
			_ = s.Atomic([]string{k}, func(es []*Entry) error { seen = es[0].Exists(); return nil })
			return seen
		}},
		{"AtomicView", false, func(s *Store, k string) (seen bool) {
			_ = s.AtomicView([]string{k}, func(vs []Value) error { seen = vs[0] != nil; return nil })
			return seen
		}},
		// Len and Keys count the persistent "other" key too.
		{"Len", false, func(s *Store, _ string) bool { return s.Len() == 2 }},
		{"Keys", false, func(s *Store, _ string) bool { return len(s.Keys("*")) == 2 }},
	}
}

func TestLazyExpiryOnEveryAccessPath(t *testing.T) {
	for _, p := range accessPaths() {
		// A key is alive while now == deadline and expired from the next ms on.
		t.Run(p.name+"/at_deadline", func(t *testing.T) {
			s, clk := newExpStore(t, 4, SweepConfig{})
			seedExpiring(t, s)
			clk.Advance(time.Second)
			if !p.visible(s, "k") {
				t.Fatal("key must still be alive when now == deadline")
			}
			if got := s.Stats().ExpiredLazy; got != 0 {
				t.Fatalf("ExpiredLazy = %d at the deadline, want 0", got)
			}
		})
		t.Run(p.name+"/after_deadline", func(t *testing.T) {
			s, clk := newExpStore(t, 4, SweepConfig{})
			seedExpiring(t, s)
			clk.Advance(time.Second + time.Millisecond)
			if p.visible(s, "k") {
				t.Fatal("expired key is visible")
			}
			wantLazy := int64(0)
			if p.reaps {
				wantLazy = 1
			}
			if got := s.Stats().ExpiredLazy; got != wantLazy {
				t.Fatalf("ExpiredLazy = %d, want %d", got, wantLazy)
			}
			inMap, inExp := rawHas(s, "k")
			if p.reaps && (inMap || inExp) {
				t.Fatalf("path must reap: inMap=%v inExp=%v", inMap, inExp)
			}
			if !p.reaps && (!inMap || !inExp) {
				t.Fatalf("read-lock path must skip, not reap: inMap=%v inExp=%v", inMap, inExp)
			}
			// A second access sees the same thing and never double counts.
			if p.visible(s, "k") {
				t.Fatal("expired key visible on second access")
			}
			if got := s.Stats().ExpiredLazy; got != wantLazy {
				t.Fatalf("ExpiredLazy = %d after second access, want %d", got, wantLazy)
			}
			if s.Exists("other") != 1 {
				t.Fatal("persistent key must be untouched")
			}
			checkExpiryInvariant(t, s)
		})
	}
}

func TestWritesOverExpiredKeyDoNotInheritItsDeadline(t *testing.T) {
	writers := map[string]func(s *Store){
		"Set": func(s *Store) { s.Set("k", []byte("new")) },
		"Mutate": func(s *Store) {
			_ = s.Mutate("k", func(Value) (Value, error) { return NewString([]byte("new")), nil })
		},
		"MutateEntry": func(s *Store) {
			_ = s.MutateEntry("k", func(e *Entry) error { e.Put(NewString([]byte("new"))); return nil })
		},
		"Update": func(s *Store) {
			_ = s.Update("k", func([]byte, bool) ([]byte, error) { return []byte("new"), nil })
		},
		"Atomic": func(s *Store) {
			_ = s.Atomic([]string{"k"}, func(es []*Entry) error { es[0].Put(NewString([]byte("new"))); return nil })
		},
	}
	for name, write := range writers {
		t.Run(name, func(t *testing.T) {
			s, clk := newExpStore(t, 4, SweepConfig{})
			putTTL(t, s, "k", expEpochMs+1000)
			clk.Advance(2 * time.Second)
			write(s)
			if got := s.ExpireAt("k"); got != -1 {
				t.Fatalf("ExpireAt = %d, want -1 (no stale deadline)", got)
			}
			if v, ok := s.Get("k"); !ok || string(v) != "new" {
				t.Fatalf("Get = %q,%v", v, ok)
			}
			checkExpiryInvariant(t, s)
		})
	}
}

func TestEntryDeadlineOps(t *testing.T) {
	s, _ := newExpStore(t, 4, SweepConfig{})
	err := s.MutateEntry("missing", func(e *Entry) error {
		if e.SetExpireAt(e.Now() + 10) {
			t.Error("SetExpireAt on a missing key must report false")
		}
		if e.Persist() {
			t.Error("Persist on a missing key must report false")
		}
		if _, ok := e.ExpireAt(); ok {
			t.Error("missing key has a deadline")
		}
		if e.Delete() {
			t.Error("Delete on a missing key must report false")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}

	s.Set("k", []byte("v"))
	err = s.MutateEntry("k", func(e *Entry) error {
		now := e.Now()
		if _, ok := e.ExpireAt(); ok {
			t.Error("fresh key must have no deadline")
		}
		if !e.SetExpireAt(now + 500) {
			t.Error("SetExpireAt on an existing key must report true")
		}
		if ms, ok := e.ExpireAt(); !ok || ms != now+500 {
			t.Errorf("ExpireAt = %d,%v, want %d,true", ms, ok, now+500)
		}
		if !e.Persist() || e.Persist() {
			t.Error("Persist must report true once, then false")
		}
		// A deadline at or before Now deletes the key immediately.
		if !e.SetExpireAt(now) {
			t.Error("SetExpireAt(now) must report true")
		}
		if e.Exists() {
			t.Error("key must be gone after a deadline in the past")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if in, inExp := rawHas(s, "k"); in || inExp {
		t.Fatalf("key or deadline left behind: %v %v", in, inExp)
	}
}

func TestFlushClearsDeadlines(t *testing.T) {
	s, _ := newExpStore(t, 4, SweepConfig{})
	keys := putMany(t, s, "f", 10, expEpochMs+1000)
	s.Flush()
	if n := rawCount(s); n != 0 {
		t.Fatalf("rawCount = %d after Flush", n)
	}
	if got := s.ExpireAt(keys[0]); got != -2 {
		t.Fatalf("ExpireAt = %d, want -2", got)
	}
	checkExpiryInvariant(t, s)
}

func TestStoreWorksAfterClose(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	s.Close()
	putTTL(t, s, "k", expEpochMs+1000)
	clk.Advance(2 * time.Second)
	if _, ok := s.Get("k"); ok {
		t.Fatal("lazy expiry must keep working after Close")
	}
	if got := s.Stats().ExpiredLazy; got != 1 {
		t.Fatalf("ExpiredLazy = %d, want 1", got)
	}
}

// --- sweeper cycles (driven directly through sweepCycle) ---

// While more than StaleThreshold of a full batch is expired the same shard is
// sampled again, so one cycle can drain a shard of any size.
func TestSweepCycleRepeatsWhileBatchesAreStale(t *testing.T) {
	s, clk := newExpStore(t, 1, manualSweep(10, 0.25, time.Hour))
	putMany(t, s, "e", 100, expEpochMs+1000)
	clk.Advance(2 * time.Second)

	res := s.sweepCycle(0)
	want := cycleResult{Sampled: 100, Expired: 100, Next: 0}
	if res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	if n := rawCount(s); n != 0 {
		t.Fatalf("%d keys left", n)
	}
	st := s.Stats()
	if st.ExpiredActive != 100 || st.ExpiredLazy != 0 {
		t.Fatalf("stats = %+v", st)
	}
	if st.SweepCycles != 0 {
		t.Fatalf("SweepCycles = %d: only the goroutine counts cycles", st.SweepCycles)
	}
	checkExpiryInvariant(t, s)
}

// A batch that is mostly live ends the shard's turn: with threshold 0.5 and a
// batch of 10, at most 5 expired keys can exist, which never exceeds it.
func TestSweepCycleStopsWhenBatchIsMostlyLive(t *testing.T) {
	s, clk := newExpStore(t, 1, manualSweep(10, 0.5, time.Hour))
	putMany(t, s, "dead", 5, expEpochMs+1000)
	live := putMany(t, s, "live", 95, expEpochMs+3_600_000)
	clk.Advance(2 * time.Second)

	res := s.sweepCycle(0)
	if res.Sampled != 10 || res.Expired > 5 || res.BudgetExhausted {
		t.Fatalf("result = %+v: want exactly one batch of 10", res)
	}
	if got := rawCount(s); got != 100-res.Expired {
		t.Fatalf("rawCount = %d, want %d", got, 100-res.Expired)
	}
	for _, k := range live {
		if in, inExp := rawHas(s, k); !in || !inExp {
			t.Fatalf("live key %q was removed or lost its deadline", k)
		}
	}
	if got := s.Stats().ExpiredActive; got != int64(res.Expired) {
		t.Fatalf("ExpiredActive = %d, want %d", got, res.Expired)
	}
	checkExpiryInvariant(t, s)
}

// A batch shorter than SampleSize means the whole index was visited.
func TestSweepCycleShortBatchEndsShard(t *testing.T) {
	s, clk := newExpStore(t, 1, manualSweep(10, 0.25, time.Hour))
	putMany(t, s, "e", 3, expEpochMs+1000)
	clk.Advance(2 * time.Second)
	res := s.sweepCycle(0)
	if want := (cycleResult{Sampled: 3, Expired: 3}); res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
}

func TestSweepCycleBudgetStopsMidShardAndResumes(t *testing.T) {
	s, clk := newExpStore(t, 4, manualSweep(10, 0.25, 25*time.Millisecond))
	putMany(t, s, "e", 400, expEpochMs+1000) // ~100 per shard, far more than 30
	clk.Advance(2 * time.Second)
	before := make([]int, 4)
	for i := range before {
		before[i] = shardLen(s, i)
	}

	// Each wall reading costs 10ms: the start reading plus three batches put
	// the elapsed time at 30ms >= 25ms, so the cycle stops after 3 batches.
	stepWall(s, 10*time.Millisecond)
	res := s.sweepCycle(2)
	want := cycleResult{Sampled: 30, Expired: 30, BudgetExhausted: true, Next: 2}
	if res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
	for i := range before {
		wantLen := before[i]
		if i == 2 {
			wantLen -= 30
		}
		if got := shardLen(s, i); got != wantLen {
			t.Errorf("shard %d: %d keys, want %d", i, got, wantLen)
		}
	}

	// The next cycle resumes at the interrupted shard and, with a cheap
	// clock, drains everything.
	stepWall(s, time.Nanosecond)
	res = s.sweepCycle(res.Next)
	if want := (cycleResult{Sampled: 370, Expired: 370, Next: 3}); res != want {
		t.Fatalf("resumed result = %+v, want %+v", res, want)
	}
	if n := rawCount(s); n != 0 {
		t.Fatalf("%d keys left", n)
	}
	checkExpiryInvariant(t, s)
}

// Even with the budget already spent, a cycle finishes the batch it started,
// so every cycle makes progress.
func TestSweepCycleAlwaysCompletesOneBatch(t *testing.T) {
	s, clk := newExpStore(t, 4, manualSweep(10, 0.25, 25*time.Millisecond))
	putMany(t, s, "e", 400, expEpochMs+1000)
	clk.Advance(2 * time.Second)
	stepWall(s, time.Hour)
	res := s.sweepCycle(0)
	if want := (cycleResult{Sampled: 10, Expired: 10, BudgetExhausted: true, Next: 0}); res != want {
		t.Fatalf("result = %+v, want %+v", res, want)
	}
}

func TestSweepCycleNextShard(t *testing.T) {
	t.Run("budget spent on an idle shard moves on", func(t *testing.T) {
		s, _ := newExpStore(t, 4, manualSweep(10, 0.25, 25*time.Millisecond))
		stepWall(s, time.Hour)
		res := s.sweepCycle(1)
		if want := (cycleResult{BudgetExhausted: true, Next: 2}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
	})
	t.Run("finishing the last shard is not exhaustion", func(t *testing.T) {
		s, _ := newExpStore(t, 1, manualSweep(10, 0.25, 25*time.Millisecond))
		stepWall(s, time.Hour)
		res := s.sweepCycle(0)
		if want := (cycleResult{}); res != want {
			t.Fatalf("result = %+v, want %+v", res, want)
		}
	})
	t.Run("full cycles rotate the start shard", func(t *testing.T) {
		s, _ := newExpStore(t, 4, manualSweep(10, 0.25, time.Hour))
		for start, want := range map[int]int{0: 1, 1: 2, 2: 3, 3: 0} {
			res := s.sweepCycle(start)
			if res.Next != want || res.BudgetExhausted {
				t.Errorf("sweepCycle(%d) = %+v, want Next %d", start, res, want)
			}
		}
	})
}

// --- sweeper goroutine ---

// Keys that are never read again are reclaimed by the sweeper alone: the test
// never goes through an access path, and rawHas does not reap.
func TestSweeperReapsNeverReadKeys(t *testing.T) {
	s, clk := newExpStore(t, 4, manualSweep(20, 0.25, time.Hour))
	dead := putMany(t, s, "dead", 50, expEpochMs+1000)
	alive := putMany(t, s, "alive", 10, 0)
	clk.Advance(2 * time.Second)

	tick := make(chan time.Time)
	if !s.startWith(tick, func() {}) {
		t.Fatal("startWith refused to start")
	}
	tick <- time.Now()
	pollUntil(t, "one sweep cycle", func() bool { return s.Stats().SweepCycles >= 1 })

	for _, k := range dead {
		if in, inExp := rawHas(s, k); in || inExp {
			t.Fatalf("never-read key %q was not reclaimed", k)
		}
	}
	for _, k := range alive {
		if in, _ := rawHas(s, k); !in {
			t.Fatalf("persistent key %q was removed", k)
		}
	}
	st := s.Stats()
	if st.ExpiredActive != 50 || st.ExpiredLazy != 0 {
		t.Fatalf("stats = %+v, want 50 active and 0 lazy", st)
	}
	if got := s.Len(); got != 10 {
		t.Fatalf("Len = %d, want 10", got)
	}
	checkExpiryInvariant(t, s)
}

func TestStatsCounters(t *testing.T) {
	s, clk := newExpStore(t, 1, manualSweep(10, 0.25, time.Hour))
	keys := putMany(t, s, "s", 5, expEpochMs+1000)
	clk.Advance(2 * time.Second)

	for _, k := range keys[:3] {
		if _, ok := s.Get(k); ok {
			t.Fatalf("%q still visible", k)
		}
	}
	if got := s.Stats(); got != (Stats{ExpiredLazy: 3}) {
		t.Fatalf("after lazy reaps: %+v", got)
	}
	if res := s.sweepCycle(0); res.Expired != 2 {
		t.Fatalf("sweep expired %d, want 2", res.Expired)
	}
	if got := s.Stats(); got != (Stats{ExpiredLazy: 3, ExpiredActive: 2}) {
		t.Fatalf("after direct sweep: %+v", got)
	}

	tick := make(chan time.Time)
	s.startWith(tick, func() {})
	for i := 0; i < 3; i++ {
		tick <- time.Now()
	}
	pollUntil(t, "three sweep cycles", func() bool { return s.Stats().SweepCycles == 3 })
	if got := s.Stats(); got != (Stats{ExpiredLazy: 3, ExpiredActive: 2, SweepCycles: 3}) {
		t.Fatalf("after goroutine cycles: %+v", got)
	}
}

func TestCloseStopsSweeperGoroutine(t *testing.T) {
	before := runtime.NumGoroutine()
	s, _ := newExpStore(t, 4, manualSweep(20, 0.25, time.Hour))
	tick := make(chan time.Time)
	var released atomic.Bool
	if !s.startWith(tick, func() { released.Store(true) }) {
		t.Fatal("startWith refused to start")
	}
	if s.startWith(make(chan time.Time), func() {}) {
		t.Fatal("a second sweeper must not start")
	}
	tick <- time.Now() // proves the goroutine is running and receiving
	s.Close()
	// Close waits for the goroutine, and release runs before it signals done.
	if !released.Load() {
		t.Fatal("sweeper goroutine still running after Close")
	}
	select {
	case tick <- time.Now():
		t.Fatal("tick was received after Close")
	default:
	}
	s.Close() // idempotent
	waitForGoroutines(t, before)
}

func TestCloseWithoutStartAndStartAfterClose(t *testing.T) {
	before := runtime.NumGoroutine()
	s, _ := newExpStore(t, 4, SweepConfig{})
	s.Close()
	if s.startWith(make(chan time.Time), func() {}) {
		t.Fatal("startWith must refuse after Close")
	}
	s.Start() // documented no-op after Close
	if got := s.Stats().SweepCycles; got != 0 {
		t.Fatalf("SweepCycles = %d", got)
	}
	waitForGoroutines(t, before)
}

// Exercises the real Start path (a time.Ticker) end to end: the keys expire by
// the fake clock, the sweeper wakes on real ticks, and Close leaves nothing
// behind.
func TestStartWithRealTickerAndNoLeak(t *testing.T) {
	before := runtime.NumGoroutine()
	cfg := SweepConfig{TickInterval: time.Millisecond, SampleSize: 20, CycleBudget: time.Hour, StaleThreshold: 0.25}
	s, clk := newExpStore(t, 4, cfg)
	putMany(t, s, "e", 30, expEpochMs+1000)
	clk.Advance(2 * time.Second)

	s.Start()
	s.Start() // idempotent
	pollUntil(t, "active expiry", func() bool { return s.Stats().ExpiredActive == 30 })
	s.Close()

	cycles := s.Stats().SweepCycles
	if cycles < 1 {
		t.Fatalf("SweepCycles = %d, want >= 1", cycles)
	}
	s.Start() // no-op after Close
	if got := s.Stats().SweepCycles; got != cycles {
		t.Fatalf("SweepCycles moved from %d to %d after Close", cycles, got)
	}
	waitForGoroutines(t, before)
}
