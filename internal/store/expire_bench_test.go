package store

import (
	"fmt"
	"runtime"
	"strconv"
	"sync"
	"testing"
	"time"
)

// benchKeys is the keyspace size for the expiry-overhead benchmarks. Building
// it takes a second or two, so they are skipped under -short and built once.
const benchKeys = 1_000_000

type bigKeyspace struct {
	once sync.Once
	s    *Store
	keys []string
}

var bigPlain, bigTTL bigKeyspace

// get builds the shared store on first use: 1M keys, all with a far-future TTL
// when ttl is set. Benchmarks only read from it or overwrite existing keys.
func (g *bigKeyspace) get(b *testing.B, ttl bool) (*Store, []string) {
	b.Helper()
	if testing.Short() {
		b.Skip("1M-key benchmark skipped in -short mode")
	}
	g.once.Do(func() {
		s, err := New(DefaultShards)
		if err != nil {
			b.Fatal(err)
		}
		val := []byte("value-0123456789")
		deadline := s.clock.NowMs() + int64(time.Hour/time.Millisecond)
		g.keys = make([]string, benchKeys)
		for i := range g.keys {
			k := "key:" + strconv.Itoa(i)
			g.keys[i] = k
			if !ttl {
				s.Set(k, val)
				continue
			}
			_ = s.MutateEntry(k, func(e *Entry) error {
				e.Put(NewString(val))
				e.SetExpireAt(deadline)
				return nil
			})
		}
		g.s = s
	})
	return g.s, g.keys
}

// BenchmarkGet1M compares reads in a 1M-key store with and without TTLs: the
// difference is the cost of the per-read expiry check.
func BenchmarkGet1M(b *testing.B) {
	for _, tc := range []struct {
		name string
		g    *bigKeyspace
		ttl  bool
	}{{"no-ttl", &bigPlain, false}, {"ttl", &bigTTL, true}} {
		b.Run(tc.name, func(b *testing.B) {
			s, keys := tc.g.get(b, tc.ttl)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Get(keys[(i*7919)%benchKeys])
			}
		})
	}
}

// BenchmarkOverwrite1M compares plain SET with SET plus a deadline, which also
// maintains the expiry index.
func BenchmarkOverwrite1M(b *testing.B) {
	b.Run("no-ttl", func(b *testing.B) {
		s, keys := bigPlain.get(b, false)
		val := []byte("value-0123456789")
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			s.Set(keys[(i*7919)%benchKeys], val)
		}
	})
	b.Run("with-ttl", func(b *testing.B) {
		s, keys := bigTTL.get(b, true)
		val := []byte("value-0123456789")
		deadline := s.clock.NowMs() + int64(time.Hour/time.Millisecond)
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			_ = s.MutateEntry(keys[(i*7919)%benchKeys], func(e *Entry) error {
				e.Put(NewString(val))
				e.SetExpireAt(deadline)
				return nil
			})
		}
	})
}

// BenchmarkLen1M shows what DBSIZE costs: Len walks the expiry index, so it is
// proportional to the number of keys with a TTL, not constant.
func BenchmarkLen1M(b *testing.B) {
	for _, tc := range []struct {
		name string
		g    *bigKeyspace
		ttl  bool
	}{{"no-ttl", &bigPlain, false}, {"ttl", &bigTTL, true}} {
		b.Run(tc.name, func(b *testing.B) {
			s, _ := tc.g.get(b, tc.ttl)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.Len()
			}
		})
	}
}

// sweepFixture is a store of n far-future TTL keys with a sweeper driven by a
// tick channel. The sweeper is started once and deliberately never closed: a
// store's lifecycle is one-shot, and the testing package calls a benchmark
// function several times with growing b.N, so closing it after the first call
// would leave the later calls without a sweeper. The process exits right after
// the benchmarks, which reclaims the goroutine.
type sweepFixture struct {
	once sync.Once
	s    *Store
	tick chan time.Time
}

var sweepFixtures = map[int]*sweepFixture{10_000: {}, benchKeys: {}}

func (f *sweepFixture) get(b *testing.B, n int) (*Store, chan time.Time) {
	b.Helper()
	f.once.Do(func() {
		s, err := New(DefaultShards)
		if err != nil {
			b.Fatal(err)
		}
		val := []byte("v")
		deadline := s.clock.NowMs() + int64(time.Hour/time.Millisecond)
		for i := 0; i < n; i++ {
			_ = s.MutateEntry("key:"+strconv.Itoa(i), func(e *Entry) error {
				e.Put(NewString(val))
				e.SetExpireAt(deadline)
				return nil
			})
		}
		f.tick = make(chan time.Time)
		if !s.startWith(f.tick, func() {}) {
			b.Fatal("sweeper did not start")
		}
		f.s = s
	})
	return f.s, f.tick
}

// BenchmarkSweepCycle is the cost of one sweeper cycle over a store whose TTL
// keys are none of them due. It depends on the shard count and the sample size,
// so 10k and 1M keys should cost about the same.
func BenchmarkSweepCycle(b *testing.B) {
	for _, n := range []int{10_000, benchKeys} {
		b.Run(fmt.Sprintf("keys=%d", n), func(b *testing.B) {
			if n >= benchKeys && testing.Short() {
				b.Skip("1M-key benchmark skipped in -short mode")
			}
			s, tick := sweepFixtures[n].get(b, n)
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				before := s.sweepCycles.Load()
				tick <- time.Time{}
				for s.sweepCycles.Load() == before {
					runtime.Gosched()
				}
			}
		})
	}
}

// BenchmarkSweepReclaim100k times the sweeper reclaiming 100k expired keys that
// nobody reads, and reports how many cycles it took.
func BenchmarkSweepReclaim100k(b *testing.B) {
	const n = 100_000
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		clk := NewFakeClock(expEpochMs)
		s, err := NewWithOptions(Options{Shards: DefaultShards, Clock: clk})
		if err != nil {
			b.Fatal(err)
		}
		for j := 0; j < n; j++ {
			_ = s.MutateEntry("k"+strconv.Itoa(j), func(e *Entry) error {
				e.Put(NewString([]byte("v")))
				e.SetExpireAt(expEpochMs + 1000)
				return nil
			})
		}
		clk.Advance(2 * time.Second)
		tick := make(chan time.Time)
		if !s.startWith(tick, func() {}) {
			b.Fatal("sweeper did not start")
		}
		b.StartTimer()
		cycles := 0
		for s.Stats().ExpiredActive < n {
			if cycles++; cycles > 100_000 {
				b.Fatal("sweeper is not making progress")
			}
			before := s.sweepCycles.Load()
			tick <- time.Time{}
			for s.sweepCycles.Load() == before {
				runtime.Gosched()
			}
		}
		b.StopTimer()
		s.Close()
		b.ReportMetric(float64(cycles), "cycles/op")
	}
}
