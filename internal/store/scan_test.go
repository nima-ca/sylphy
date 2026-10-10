package store

import (
	"errors"
	"fmt"
	"math"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"
)

// scanAll runs a full iteration and returns every key it reported, in order.
func scanAll(t testing.TB, s *Store, count int, keep func(string, Kind) bool) []string {
	t.Helper()
	var out []string
	cur := uint64(0)
	for i := 0; ; i++ {
		if i > 1_000_000 {
			t.Fatal("scan did not terminate")
		}
		next, keys, err := s.Scan(cur, count, keep)
		if err != nil {
			t.Fatalf("Scan(%d): %v", cur, err)
		}
		out = append(out, keys...)
		if next == 0 {
			return out
		}
		cur = next
	}
}

// checkIndex verifies that every shard's slot index mirrors its map exactly.
func checkIndex(t testing.TB, s *Store) {
	t.Helper()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		if len(sh.order) != len(sh.m) || len(sh.pos) != len(sh.m) {
			t.Errorf("shard %d: %d keys, %d slots, %d positions", i, len(sh.m), len(sh.order), len(sh.pos))
		}
		for j, k := range sh.order {
			if sh.pos[k] != j {
				t.Errorf("shard %d: pos[%q] = %d, want %d", i, k, sh.pos[k], j)
			}
			if _, ok := sh.m[k]; !ok {
				t.Errorf("shard %d: slot %d holds %q, which is not in the map", i, j, k)
			}
		}
		sh.mu.RUnlock()
	}
}

func sortedCopy(in []string) []string {
	out := slices.Clone(in)
	slices.Sort(out)
	return out
}

func TestScanEmptyStore(t *testing.T) {
	s := newStore(t, 4)
	next, keys, err := s.Scan(0, 10, nil)
	if err != nil || next != 0 || len(keys) != 0 {
		t.Fatalf("Scan on an empty store = (%d, %v, %v)", next, keys, err)
	}
}

func TestScanReturnsEveryKeyExactlyOnceWithoutWriters(t *testing.T) {
	for _, count := range []int{1, 3, 7, 100, 100000, 0, -5} {
		t.Run(fmt.Sprintf("count%d", count), func(t *testing.T) {
			s := newStore(t, 8)
			want := putMany(t, s, "key", 500, 0)
			got := scanAll(t, s, count, nil)
			if !slices.Equal(sortedCopy(got), sortedCopy(want)) {
				t.Fatalf("scan returned %d keys, want the %d stored ones exactly once", len(got), len(want))
			}
		})
	}
}

func TestScanCallCountIsBoundedByCount(t *testing.T) {
	s := newStore(t, 8)
	putMany(t, s, "key", 400, 0)
	calls := 0
	cur := uint64(0)
	for {
		next, _, err := s.Scan(cur, 10, nil)
		if err != nil {
			t.Fatal(err)
		}
		calls++
		if next == 0 {
			break
		}
		cur = next
	}
	if want := 400/10 + 2; calls > want {
		t.Fatalf("%d calls for 400 keys with COUNT 10, want at most %d", calls, want)
	}
}

func TestScanSkipsExpiredKeys(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	live := putMany(t, s, "live", 20, 0)
	putMany(t, s, "dead", 20, expEpochMs+1000)
	if got := scanAll(t, s, 3, nil); len(got) != 40 {
		t.Fatalf("before expiry: %d keys, want 40", len(got))
	}
	clk.Advance(1001 * time.Millisecond)
	got := scanAll(t, s, 3, nil)
	if !slices.Equal(sortedCopy(got), sortedCopy(live)) {
		t.Fatalf("after expiry got %v, want only the live keys", sortedCopy(got))
	}
}

func TestScanKeepFilterSeesKindAndKey(t *testing.T) {
	s := newStore(t, 4)
	want := putMany(t, s, "a", 50, 0)
	putMany(t, s, "b", 50, 0)
	got := scanAll(t, s, 6, func(key string, kind Kind) bool {
		if kind != KindString {
			t.Errorf("kind of %q = %v, want string", key, kind)
		}
		return strings.HasPrefix(key, "a")
	})
	if !slices.Equal(sortedCopy(got), sortedCopy(want)) {
		t.Fatalf("filtered scan returned %d keys, want the 50 a-keys", len(got))
	}
}

func TestScanRejectsForeignCursors(t *testing.T) {
	s := newStore(t, 4)
	for _, c := range []uint64{4 << scanSlotBits, 9<<scanSlotBits | 3, math.MaxUint64} {
		if _, _, err := s.Scan(c, 10, nil); !errors.Is(err, ErrInvalidCursor) {
			t.Errorf("Scan(%d) err = %v, want ErrInvalidCursor", c, err)
		}
	}
}

func TestScanClampsStaleCursors(t *testing.T) {
	s := newStore(t, 4)
	putMany(t, s, "key", 40, 0)
	// Valid shard, slot count far beyond what the shard holds: clamped.
	if _, _, err := s.Scan(1<<scanSlotBits|1<<39, 10, nil); err != nil {
		t.Fatalf("stale cursor rejected: %v", err)
	}
	// Last shard, fresh.
	if _, _, err := s.Scan(3<<scanSlotBits, 10, nil); err != nil {
		t.Fatalf("last-shard cursor rejected: %v", err)
	}
}

// TestScanFindsStableKeysDuringChurn is the headline guarantee: while writers
// add and delete other keys (which swap-removes stable keys from the end of a
// shard's slot list into holes), every full scan still reports every stable key.
func TestScanFindsStableKeysDuringChurn(t *testing.T) {
	s := newStore(t, 8)
	const stable, churn = 400, 400
	for i := 0; i < stable; i++ { // interleaved so holes get filled by stable keys
		s.Set(fmt.Sprintf("stable-%d", i), []byte("v"))
		s.Set(fmt.Sprintf("churn-%d", i), []byte("v"))
	}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed*7+1))
			for {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("churn-%d", r.IntN(2*churn))
				if r.IntN(2) == 0 {
					s.Set(k, []byte("v"))
				} else {
					s.Delete(k)
				}
			}
		}(uint64(w + 1))
	}
	for pass := 0; pass < 20; pass++ {
		seen := make(map[string]bool)
		for _, k := range scanAll(t, s, 5, nil) {
			seen[k] = true
		}
		for i := 0; i < stable; i++ {
			if k := fmt.Sprintf("stable-%d", i); !seen[k] {
				t.Errorf("pass %d: stable key %s was missed", pass, k)
			}
		}
	}
	close(stop)
	wg.Wait()
	checkIndex(t, s)
}

func TestSlotIndexStaysConsistent(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	r := rand.New(rand.NewPCG(3, 4))
	key := func() string { return fmt.Sprintf("k%d", r.IntN(300)) }
	for i := 0; i < 20000; i++ {
		switch r.IntN(9) {
		case 0, 1:
			s.Set(key(), []byte("v"))
		case 2:
			s.Delete(key())
		case 3:
			_ = s.Update(key(), func(old []byte, _ bool) ([]byte, error) { return append(old, 'x'), nil })
		case 4:
			_ = s.Mutate(key(), func(Value) (Value, error) { return nil, nil })
		case 5:
			putTTL(t, s, key(), clk.NowMs()+int64(r.IntN(50)))
		case 6:
			clk.Advance(time.Duration(r.IntN(20)) * time.Millisecond)
			_, _ = s.Get(key())
		case 7:
			_ = s.Atomic([]string{key(), key()}, func(es []*Entry) error {
				es[0].MoveTo(es[1])
				return nil
			})
		case 8:
			if r.IntN(50) == 0 {
				s.Flush()
			}
		}
		if i%250 == 0 {
			checkIndex(t, s)
		}
	}
	checkIndex(t, s)
	got := sortedCopy(scanAll(t, s, 7, nil))
	if want := sortedCopy(s.Keys("*")); !slices.Equal(got, want) {
		t.Fatalf("scan and Keys disagree: %d vs %d keys", len(got), len(want))
	}
}

func TestOrderSliceShrinksAfterMassDeletion(t *testing.T) {
	s := newStore(t, 1)
	keys := putMany(t, s, "key", 4000, 0)
	for _, k := range keys[10:] {
		s.Delete(k)
	}
	checkIndex(t, s)
	if c := cap(s.shards[0].order); c > 200 {
		t.Fatalf("order capacity %d after deleting nearly everything", c)
	}
}

func TestMoveTo(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	a, b := keysInDifferentShards(t, s)
	move := func(from, to string) {
		t.Helper()
		if err := s.Atomic([]string{from, to}, func(es []*Entry) error {
			es[0].MoveTo(es[1])
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}

	putTTL(t, s, a, expEpochMs+5000)
	s.Set(b, []byte("old"))
	move(a, b)
	if s.Exists(a) != 0 || s.Exists(b) != 1 {
		t.Fatal("source must vanish and destination appear")
	}
	if ms := s.ExpireAt(b); ms != expEpochMs+5000 {
		t.Fatalf("deadline = %d, want it moved with the value", ms)
	}
	if ms := s.ExpireAt(a); ms != -2 {
		t.Fatalf("source deadline = %d, want -2", ms)
	}

	// A source without a TTL clears the destination's TTL.
	s.Set(a, []byte("plain"))
	move(a, b)
	if ms := s.ExpireAt(b); ms != -1 {
		t.Fatalf("deadline = %d, want -1 (persistent)", ms)
	}

	// A deadline equal to the current millisecond survives the move.
	putTTL(t, s, a, clk.NowMs())
	move(a, b)
	if s.Exists(b) != 1 {
		t.Fatal("a key whose deadline is the current millisecond must survive MoveTo")
	}

	// Missing source and same-key moves are no-ops.
	move("missing", b)
	if s.Exists(b) != 1 {
		t.Fatal("moving a missing key must not touch the destination")
	}
	move(b, b)
	if s.Exists(b) != 1 {
		t.Fatal("moving a key onto itself must keep it")
	}
	checkIndex(t, s)
}

func TestRandomKey(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	r := rand.New(rand.NewPCG(9, 9))
	if _, ok := s.RandomKey(r.IntN); ok {
		t.Fatal("empty store returned a key")
	}
	live := putMany(t, s, "live", 5, 0)
	putTTL(t, s, "dead", expEpochMs+10)
	clk.Advance(11 * time.Millisecond)
	seen := map[string]int{}
	for i := 0; i < 500; i++ {
		k, ok := s.RandomKey(r.IntN)
		if !ok {
			t.Fatal("RandomKey found nothing in a store with live keys")
		}
		seen[k]++
	}
	if seen["dead"] != 0 {
		t.Fatal("an expired key was returned")
	}
	for _, k := range live {
		if seen[k] == 0 {
			t.Errorf("key %s was never returned in 500 draws", k)
		}
	}

	s.Flush()
	putTTL(t, s, "x", clk.NowMs()+5)
	clk.Advance(6 * time.Millisecond)
	if _, ok := s.RandomKey(r.IntN); ok {
		t.Fatal("a store holding only expired keys returned a key")
	}
}
