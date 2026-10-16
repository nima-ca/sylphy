package store

import (
	"slices"
	"sync"
	"testing"
	"time"
)

type keyLog struct {
	mu   sync.Mutex
	keys []string
}

func (l *keyLog) add(k string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keys = append(l.keys, k)
}

func (l *keyLog) take() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := slices.Clone(l.keys)
	slices.Sort(out)
	l.keys = nil
	return out
}

type emptyColl struct{}

func (emptyColl) Kind() Kind { return KindList }
func (emptyColl) Len() int   { return 0 }

func TestBeginReplayFreezesTheClock(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	if got := s.NowMs(); got != expEpochMs {
		t.Fatalf("NowMs = %d", got)
	}
	end := s.BeginReplay()
	if s.NowMs() != 0 || s.clock.NowMs() != 0 {
		t.Fatal("the clock is not frozen")
	}
	inner := s.BeginReplay() // calls nest
	end()
	if s.NowMs() != 0 {
		t.Fatal("the inner replay was unfrozen by the outer end")
	}
	end() // idempotent: must not release the inner freeze
	if s.NowMs() != 0 {
		t.Fatal("a second end() released someone else's freeze")
	}
	inner()
	if got := s.NowMs(); got != expEpochMs {
		t.Fatalf("after the last end, NowMs = %d", got)
	}
	clk.Advance(time.Second)
	if got := s.NowMs(); got != expEpochMs+1000 {
		t.Fatalf("NowMs = %d after advancing", got)
	}
}

func TestFrozenClockKeepsPastDeadlinesAlive(t *testing.T) {
	s, _ := newExpStore(t, 4, SweepConfig{})
	end := s.BeginReplay()
	mustLoad := func(k string, at int64) {
		t.Helper()
		if err := s.Load(k, NewString([]byte("v")), at); err != nil {
			t.Fatal(err)
		}
	}
	mustLoad("loaded", expEpochMs-5_000) // already past by the real clock
	err := s.MutateEntry("set", func(e *Entry) error {
		e.Put(NewString([]byte("v")))
		e.SetExpireAt(expEpochMs - 1) // would delete the key on a live clock
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"loaded", "set"} {
		if _, ok := s.Get(k); !ok {
			t.Fatalf("%s vanished during replay", k)
		}
	}
	end()
	for _, k := range []string{"loaded", "set"} {
		if _, ok := s.Get(k); ok {
			t.Fatalf("%s is still visible after replay although its deadline passed", k)
		}
	}
}

func TestLoad(t *testing.T) {
	s, _ := newExpStore(t, 4, SweepConfig{})
	if err := s.Load("a", NewString([]byte("1")), 0); err != nil {
		t.Fatal(err)
	}
	if err := s.Load("b", NewString([]byte("2")), expEpochMs+1000); err != nil {
		t.Fatal(err)
	}
	if s.ExpireAt("a") != -1 || s.ExpireAt("b") != expEpochMs+1000 {
		t.Fatalf("deadlines: a=%d b=%d", s.ExpireAt("a"), s.ExpireAt("b"))
	}
	// Loading over a key replaces its value and its deadline.
	if err := s.Load("b", NewString([]byte("3")), 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := s.Get("b"); string(got) != "3" || s.ExpireAt("b") != -1 {
		t.Fatalf("b = %q ttl %d", got, s.ExpireAt("b"))
	}
	if s.Len() != 2 {
		t.Fatalf("Len = %d", s.Len())
	}
	if keys := s.Keys("*"); !slices.Equal(sortedCopy(keys), []string{"a", "b"}) {
		t.Fatalf("Keys = %v", keys)
	}
	checkIndex(t, s)
	checkExpiryInvariant(t, s)

	if err := s.Load("n", nil, 0); err == nil {
		t.Fatal("nil value accepted")
	}
	if err := s.Load("e", emptyColl{}, 0); err == nil {
		t.Fatal("empty collection accepted")
	}
	if s.Exists("n", "e") != 0 {
		t.Fatal("a rejected Load left a key behind")
	}

	// A deadline in the past is stored as given, not deleted.
	if err := s.Load("old", NewString([]byte("o")), expEpochMs-1); err != nil {
		t.Fatal(err)
	}
	if inMap, inExp := rawHas(s, "old"); !inMap || !inExp {
		t.Fatalf("old: inMap=%v inExp=%v", inMap, inExp)
	}
}

func TestExpireHook(t *testing.T) {
	s, clk := newExpStore(t, 2, SweepConfig{})
	var log keyLog
	s.SetExpireHook(func(k string) {
		if sh := s.shardFor(k); sh.mu.TryLock() {
			sh.mu.Unlock()
			t.Errorf("hook for %q ran without the shard lock", k)
		}
		log.add(k)
	})

	putTTL(t, s, "lazy", expEpochMs+10)
	putTTL(t, s, "active1", expEpochMs+10)
	putTTL(t, s, "active2", expEpochMs+10)
	putTTL(t, s, "later", expEpochMs+100_000)
	s.Set("plain", []byte("p"))
	s.Set("deleted", []byte("d"))
	clk.Advance(time.Second)

	if _, ok := s.Get("lazy"); ok {
		t.Fatal("lazy is still visible")
	}
	if got := log.take(); !slices.Equal(got, []string{"lazy"}) {
		t.Fatalf("after lazy expiry: %v", got)
	}

	if res := s.sweepCycle(0); res.Expired != 2 {
		t.Fatalf("sweep expired %d", res.Expired)
	}
	if got := log.take(); !slices.Equal(got, []string{"active1", "active2"}) {
		t.Fatalf("after the sweep: %v", got)
	}

	putTTL(t, s, "purged", clk.NowMs()+10)
	clk.Advance(time.Second)
	if n := s.PurgeExpired(); n != 1 {
		t.Fatalf("PurgeExpired = %d", n)
	}
	if got := log.take(); !slices.Equal(got, []string{"purged"}) {
		t.Fatalf("after PurgeExpired: %v", got)
	}
	if n := s.PurgeExpired(); n != 0 {
		t.Fatalf("second PurgeExpired = %d", n)
	}

	// Neither an explicit delete nor a deadline that deletes as a command
	// effect is an expiry.
	s.Delete("deleted")
	err := s.MutateEntry("gone", func(e *Entry) error {
		e.Put(NewString([]byte("g")))
		e.SetExpireAt(e.Now() - 1)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := log.take(); len(got) != 0 {
		t.Fatalf("hook ran for %v", got)
	}
	if got := s.Stats(); got.ExpiredLazy != 1 || got.ExpiredActive != 3 {
		t.Fatalf("stats %+v", got)
	}
	if s.Exists("later", "plain") != 2 {
		t.Fatal("live keys were removed")
	}
}

func TestExpireHookCanBeRemoved(t *testing.T) {
	s, clk := newExpStore(t, 2, SweepConfig{})
	var log keyLog
	s.SetExpireHook(log.add)
	s.SetExpireHook(nil)
	putTTL(t, s, "k", expEpochMs+10)
	clk.Advance(time.Second)
	if n := s.PurgeExpired(); n != 1 {
		t.Fatalf("PurgeExpired = %d", n)
	}
	if got := log.take(); len(got) != 0 {
		t.Fatalf("hook ran after removal: %v", got)
	}
}

func TestFlushWithHoldsEveryShardLock(t *testing.T) {
	s := newStore(t, 4)
	s.Set("a", []byte("1"))
	s.Set("b", []byte("2"))
	called := 0
	s.FlushWith(func() {
		called++
		for i := range s.shards {
			if s.shards[i].mu.TryLock() {
				s.shards[i].mu.Unlock()
				t.Errorf("shard %d is not locked inside the callback", i)
			}
		}
	})
	if called != 1 || s.Len() != 0 {
		t.Fatalf("called=%d Len=%d", called, s.Len())
	}
	checkIndex(t, s)
	s.Set("c", []byte("3"))
	s.Flush()
	s.FlushWith(nil)
	if s.Len() != 0 {
		t.Fatal("Flush left keys behind")
	}
	s.Set("d", []byte("4")) // the store is usable after a flush
	if got, ok := s.Get("d"); !ok || string(got) != "4" {
		t.Fatal("store unusable after Flush")
	}
}

func TestShardCount(t *testing.T) {
	if got := newStore(t, 8).ShardCount(); got != 8 {
		t.Fatalf("ShardCount = %d", got)
	}
}
