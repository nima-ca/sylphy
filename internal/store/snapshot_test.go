package store

import (
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"iter"
	"maps"
	"math/rand/v2"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// textEnc encodes each entry as key=value[@deadline]; and records which shards
// it was asked to encode.
type textEnc struct {
	mu    sync.Mutex
	calls map[int]int
	fail  func(shard int) error
}

func newTextEnc() *textEnc { return &textEnc{calls: map[int]int{}} }

func (e *textEnc) encode(shard int, entries iter.Seq[ShardEntry]) ([]byte, error) {
	e.mu.Lock()
	e.calls[shard]++
	fail := e.fail
	e.mu.Unlock()
	if fail != nil {
		if err := fail(shard); err != nil {
			return nil, err
		}
	}
	var b []byte
	for en := range entries {
		str, ok := en.Value.(*String)
		if !ok {
			return nil, fmt.Errorf("unexpected value type %T", en.Value)
		}
		b = fmt.Appendf(b, "%s=%s", en.Key, str.b)
		if en.HasDeadline {
			b = fmt.Appendf(b, "@%d", en.DeadlineMs)
		}
		b = append(b, ';')
	}
	return b, nil
}

func (e *textEnc) count(shard int) int {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.calls[shard]
}

// parseBlocks turns encoded blocks back into key -> "value[@deadline]".
func parseBlocks(t testing.TB, blocks [][]byte) map[string]string {
	t.Helper()
	out := make(map[string]string)
	for _, b := range blocks {
		for _, rec := range strings.Split(string(b), ";") {
			if rec == "" {
				continue
			}
			k, v, ok := strings.Cut(rec, "=")
			if !ok {
				t.Fatalf("malformed record %q", rec)
			}
			if _, dup := out[k]; dup {
				t.Fatalf("key %q appears twice in the snapshot", k)
			}
			out[k] = v
		}
	}
	return out
}

func drain(t testing.TB, sn *Snapshot) [][]byte {
	t.Helper()
	n := len(sn.s.shards)
	blocks := make([][]byte, n)
	next := 0
	err := sn.Drain(context.Background(), func(shard int, block []byte) error {
		if shard != next {
			t.Errorf("sink got shard %d, want %d", shard, next)
		}
		next++
		blocks[shard] = slices.Clone(block)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if next != n {
		t.Fatalf("sink saw %d shards, want %d", next, n)
	}
	return blocks
}

// keysPerShard returns one key for every shard.
func keysPerShard(t testing.TB, s *Store) []string {
	t.Helper()
	out := make([]string, len(s.shards))
	found := 0
	for i := 0; i < 100_000 && found < len(out); i++ {
		k := fmt.Sprintf("key%d", i)
		if idx := s.shardIndex(k); out[idx] == "" {
			out[idx] = k
			found++
		}
	}
	if found < len(out) {
		t.Fatal("could not find a key for every shard")
	}
	return out
}

func TestSnapshotCapturesTheStateAtBeginSnapshot(t *testing.T) {
	s, clk := newExpStore(t, 4, SweepConfig{})
	want := make(map[string]string)
	for i := range 40 {
		k := fmt.Sprintf("k%d", i)
		s.Set(k, []byte("old"))
		want[k] = "old"
	}
	putTTL(t, s, "ttl", expEpochMs+5_000)
	want["ttl"] = "v@" + strconv.Itoa(expEpochMs+5_000)
	putTTL(t, s, "stale", expEpochMs+10)
	want["stale"] = "v@" + strconv.Itoa(expEpochMs+10)
	clk.Advance(time.Second) // "stale" is expired but not reaped: it is part of the memory state

	enc := newTextEnc()
	ran := false
	sn, err := s.BeginSnapshot(func() error {
		ran = true
		for i := range s.shards {
			if s.shards[i].mu.TryLock() {
				s.shards[i].mu.Unlock()
				t.Errorf("shard %d is not locked inside the barrier", i)
			}
		}
		return nil
	}, enc.encode)
	if err != nil || !ran {
		t.Fatalf("BeginSnapshot: %v (barrier ran: %v)", err, ran)
	}

	// Change everything while the snapshot is pending.
	for i := range 20 {
		s.Set(fmt.Sprintf("k%d", i), []byte("new"))
	}
	s.Delete("k20", "k21")
	if err := s.Update("k22", func(old []byte, _ bool) ([]byte, error) { return append(old, "!"...), nil }); err != nil {
		t.Fatal(err)
	}
	s.Set("brand-new", []byte("x"))
	s.Flush()
	s.Set("post", []byte("y"))

	blocks := drain(t, sn)
	sn.Close()
	if got := parseBlocks(t, blocks); !maps.Equal(got, want) {
		t.Fatalf("snapshot is not the state at BeginSnapshot:\n got %v\nwant %v", got, want)
	}
	for i := range s.shards {
		if enc.count(i) != 1 {
			t.Errorf("shard %d was encoded %d times", i, enc.count(i))
		}
		if s.shards[i].snap.Load() != nil {
			t.Errorf("shard %d is still pending", i)
		}
	}
}

func TestBeginSnapshotBarrierFailure(t *testing.T) {
	s := newStore(t, 4)
	s.Set("a", []byte("1"))
	enc := newTextEnc()
	if _, err := s.BeginSnapshot(func() error { return errStop }, enc.encode); err != errStop {
		t.Fatalf("got %v", err)
	}
	for i := range s.shards {
		if s.shards[i].snap.Load() != nil {
			t.Fatalf("shard %d is pending after a failed barrier", i)
		}
	}
	s.Set("a", []byte("2"))
	if len(enc.calls) != 0 {
		t.Fatal("the encoder ran for a snapshot that never started")
	}
	sn, err := s.BeginSnapshot(nil, enc.encode) // the failed attempt released its reservation
	if err != nil {
		t.Fatal(err)
	}
	sn.Close()

	if _, err := s.BeginSnapshot(nil, nil); err == nil {
		t.Fatal("nil encoder accepted")
	}
}

func TestOnlyOneSnapshotAtATime(t *testing.T) {
	s := newStore(t, 4)
	enc := newTextEnc()
	sn, err := s.BeginSnapshot(nil, enc.encode)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.BeginSnapshot(nil, enc.encode); err != ErrSnapshotActive {
		t.Fatalf("got %v", err)
	}
	sn.Close()
	sn.Close() // idempotent
	sn2, err := s.BeginSnapshot(nil, enc.encode)
	if err != nil {
		t.Fatalf("after Close: %v", err)
	}
	sn2.Close()
}

func TestCloseWithoutDrainStopsTheGate(t *testing.T) {
	s := newStore(t, 4)
	k := keysPerShard(t, s)
	for _, key := range k {
		s.Set(key, []byte("v"))
	}
	enc := newTextEnc()
	sn, err := s.BeginSnapshot(nil, enc.encode)
	if err != nil {
		t.Fatal(err)
	}
	sn.Close()
	hits := s.gateHits.Load()
	for _, key := range k {
		s.Set(key, []byte("w"))
	}
	if len(enc.calls) != 0 || s.gateHits.Load() != hits {
		t.Fatalf("writers still serialize after Close: calls %v", enc.calls)
	}
}

func TestSnapshotDrainErrors(t *testing.T) {
	t.Run("context", func(t *testing.T) {
		s := newStore(t, 4)
		sn, err := s.BeginSnapshot(nil, newTextEnc().encode)
		if err != nil {
			t.Fatal(err)
		}
		defer sn.Close()
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if err := sn.Drain(ctx, func(int, []byte) error { return nil }); err != context.Canceled {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("sink", func(t *testing.T) {
		s := newStore(t, 4)
		sn, err := s.BeginSnapshot(nil, newTextEnc().encode)
		if err != nil {
			t.Fatal(err)
		}
		defer sn.Close()
		seen := 0
		err = sn.Drain(context.Background(), func(int, []byte) error {
			seen++
			return errStop
		})
		if err != errStop || seen != 1 {
			t.Fatalf("got %v after %d shards", err, seen)
		}
	})
	t.Run("encoder error found by Drain", func(t *testing.T) {
		s := newStore(t, 4)
		enc := newTextEnc()
		enc.fail = func(shard int) error {
			if shard == 2 {
				return errStop
			}
			return nil
		}
		sn, err := s.BeginSnapshot(nil, enc.encode)
		if err != nil {
			t.Fatal(err)
		}
		defer sn.Close()
		sunk := 0
		err = sn.Drain(context.Background(), func(int, []byte) error { sunk++; return nil })
		if err == nil || !strings.Contains(err.Error(), "shard 2") || sunk != 2 {
			t.Fatalf("got %v after %d shards", err, sunk)
		}
	})
	t.Run("encoder error found by a writer", func(t *testing.T) {
		s := newStore(t, 4)
		k := keysPerShard(t, s)
		enc := newTextEnc()
		enc.fail = func(shard int) error {
			if shard == s.shardIndex(k[1]) {
				return errStop
			}
			return nil
		}
		sn, err := s.BeginSnapshot(nil, enc.encode)
		if err != nil {
			t.Fatal(err)
		}
		defer sn.Close()
		s.Set(k[1], []byte("still works")) // a failing snapshot never fails a client command
		if got, ok := s.Get(k[1]); !ok || string(got) != "still works" {
			t.Fatal("the write was lost")
		}
		if err := sn.Drain(context.Background(), func(int, []byte) error { return nil }); err == nil {
			t.Fatal("Drain did not report the writer's encoder error")
		}
	})
	t.Run("only once", func(t *testing.T) {
		s := newStore(t, 4)
		sn, err := s.BeginSnapshot(nil, newTextEnc().encode)
		if err != nil {
			t.Fatal(err)
		}
		defer sn.Close()
		drain(t, sn)
		if err := sn.Drain(context.Background(), func(int, []byte) error { return nil }); err == nil {
			t.Fatal("second Drain accepted")
		}
	})
}

// TestEveryMutatingAPIHitsTheGate runs each way of changing the store while a
// snapshot is pending and checks that the shards it touches were serialized
// first, from their pre-change state, and the others were left alone. Read-only
// APIs must not trigger the gate.
func TestEveryMutatingAPIHitsTheGate(t *testing.T) {
	type op struct {
		name  string
		all   bool  // every shard is touched
		touch []int // otherwise: indexes into the per-shard key list; -1 means the stale key's shard
		run   func(s *Store, k []string)
	}
	ops := []op{
		{"Set", false, []int{0}, func(s *Store, k []string) { s.Set(k[0], []byte("new")) }},
		{"Delete", false, []int{0}, func(s *Store, k []string) { s.Delete(k[0]) }},
		{"Update", false, []int{0}, func(s *Store, k []string) {
			_ = s.Update(k[0], func(old []byte, _ bool) ([]byte, error) { return append(old, '!'), nil })
		}},
		{"Mutate", false, []int{0}, func(s *Store, k []string) {
			_ = s.Mutate(k[0], func(Value) (Value, error) { return NewString([]byte("m")), nil })
		}},
		{"MutateEntry", false, []int{0}, func(s *Store, k []string) {
			_ = s.MutateEntry(k[0], func(e *Entry) error { e.Put(NewString([]byte("x"))); return nil })
		}},
		{"Atomic over two shards", false, []int{0, 2}, func(s *Store, k []string) {
			_ = s.Atomic([]string{k[0], k[2]}, func(es []*Entry) error {
				es[0].Put(NewString([]byte("a")))
				es[1].Put(NewString([]byte("b")))
				return nil
			})
		}},
		{"Load", false, []int{0}, func(s *Store, k []string) { _ = s.Load(k[0], NewString([]byte("l")), 0) }},
		{"lazy expiry on a read", false, []int{-1}, func(s *Store, _ []string) { s.Get("stale") }},
		{"lazy expiry inside Mutate", false, []int{-1}, func(s *Store, _ []string) {
			_ = s.Mutate("stale", func(Value) (Value, error) { return nil, nil })
		}},
		{"Flush", true, nil, func(s *Store, _ []string) { s.Flush() }},
		{"FlushWith", true, nil, func(s *Store, _ []string) { s.FlushWith(func() {}) }},
		{"sweeper", true, nil, func(s *Store, _ []string) { s.sweepCycle(0) }},
		{"PurgeExpired", true, nil, func(s *Store, _ []string) { s.PurgeExpired() }},
	}
	for _, tc := range ops {
		t.Run(tc.name, func(t *testing.T) {
			s, clk := newExpStore(t, 4, SweepConfig{})
			k := keysPerShard(t, s)
			for i, key := range k {
				s.Set(key, []byte("old"+strconv.Itoa(i)))
			}
			putTTL(t, s, "stale", expEpochMs+10)
			clk.Advance(time.Second)
			enc := newTextEnc()
			sn, err := s.BeginSnapshot(nil, enc.encode)
			if err != nil {
				t.Fatal(err)
			}
			defer sn.Close()

			tc.run(s, k)

			touched := make(map[int]bool)
			if tc.all {
				for i := range s.shards {
					touched[i] = true
				}
			}
			for _, ix := range tc.touch {
				if ix == -1 {
					touched[s.shardIndex("stale")] = true
				} else {
					touched[ix] = true // keysPerShard puts key i in shard i
				}
			}
			for i := range s.shards {
				pending := s.shards[i].snap.Load() != nil
				switch {
				case touched[i] && (pending || enc.count(i) != 1):
					t.Errorf("shard %d was changed without being serialized first (pending=%v, encoded %d times)", i, pending, enc.count(i))
				case !touched[i] && (!pending || enc.count(i) != 0):
					t.Errorf("shard %d was serialized although nothing touched it", i)
				}
			}
			if got := s.gateHits.Load(); got != int64(len(touched)) {
				t.Errorf("gate fired %d times, want %d", got, len(touched))
			}
			for i := range s.shards {
				if !touched[i] {
					continue
				}
				if want := k[i] + "=old" + strconv.Itoa(i); !strings.Contains(string(sn.bufs[i]), want) {
					t.Errorf("shard %d block %q lacks its pre-change entry %q", i, sn.bufs[i], want)
				}
			}
			if tc.name == "lazy expiry on a read" || tc.name == "lazy expiry inside Mutate" {
				if got := string(sn.bufs[s.shardIndex("stale")]); !strings.Contains(got, "stale=v@") {
					t.Errorf("the unreaped expired key is missing from its block: %q", got)
				}
			}
		})
	}

	t.Run("read-only APIs never gate", func(t *testing.T) {
		s, _ := newExpStore(t, 4, SweepConfig{})
		k := keysPerShard(t, s)
		for _, key := range k {
			s.Set(key, []byte("v"))
		}
		enc := newTextEnc()
		sn, err := s.BeginSnapshot(nil, enc.encode)
		if err != nil {
			t.Fatal(err)
		}
		defer sn.Close()
		s.Get(k[0])
		s.Exists(k...)
		s.ExpireAt(k[1])
		s.Keys("*")
		s.Len()
		_, _, _ = s.Scan(0, 100, nil)
		s.RandomKey(func(n int) int { return 0 })
		_ = s.View(k[2], func(Value) error { return nil })
		_ = s.AtomicView(k, func([]Value) error { return nil })
		if got := s.gateHits.Load(); got != 0 || len(enc.calls) != 0 {
			t.Fatalf("a read triggered the gate: hits %d, calls %v", got, enc.calls)
		}
	})
}

// TestNoWriteLockBypassesTheGate keeps the gate honest as the code grows: the
// only places allowed to take a shard's write lock are lockW itself and
// Snapshot.Close (which must not serialize). A new write path that calls
// sh.mu.Lock directly fails here.
func TestNoWriteLockBypassesTheGate(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	allowed := map[string]bool{"*shard.lockW": true, "*Snapshot.Close": true}
	fset := token.NewFileSet()
	seen := 0
	for _, de := range entries {
		name := de.Name()
		if !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		f, err := parser.ParseFile(fset, name, nil, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}
			owner := fd.Name.Name
			if fd.Recv != nil && len(fd.Recv.List) > 0 {
				owner = types.ExprString(fd.Recv.List[0].Type) + "." + owner
			}
			ast.Inspect(fd.Body, func(n ast.Node) bool {
				call, ok := n.(*ast.CallExpr)
				if !ok {
					return true
				}
				sel, ok := call.Fun.(*ast.SelectorExpr)
				if !ok || sel.Sel.Name != "Lock" {
					return true
				}
				mu, ok := sel.X.(*ast.SelectorExpr)
				if !ok || mu.Sel.Name != "mu" {
					return true
				}
				if lc, ok := mu.X.(*ast.SelectorExpr); ok && lc.Sel.Name == "lc" {
					return true // the sweeper lifecycle lock, not a shard lock
				}
				seen++
				if !allowed[owner] {
					t.Errorf("%s: %s takes a shard write lock without the copy-on-write gate; use lockW", fset.Position(call.Pos()), owner)
				}
				return true
			})
		}
	}
	if seen != 2 {
		t.Fatalf("found %d raw shard write locks, expected exactly lockW and Snapshot.Close", seen)
	}
}

// TestSnapshotsOfConcurrentCrossShardTransactionsAreConsistent keeps the
// invariant total == sum(counters) true under concurrent writers that update a
// counter and the total in one Atomic call, across different shards, while
// snapshots run. Every snapshot must satisfy it, which is only possible if a
// transaction is either wholly in or wholly out. Run it with -race.
func TestSnapshotsOfConcurrentCrossShardTransactionsAreConsistent(t *testing.T) {
	s := newStore(t, 8)
	const counters = 8
	names := make([]string, counters)
	for i := range names {
		names[i] = "c" + strconv.Itoa(i)
		s.Set(names[i], []byte("0"))
	}
	s.Set("total", []byte("0"))

	atoi := func(v Value) int {
		n, err := strconv.Atoi(string(v.(*String).b))
		if err != nil {
			t.Error(err)
		}
		return n
	}

	var stop atomic.Bool
	var ops atomic.Int64
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r := rand.New(rand.NewPCG(uint64(w), 7))
			for !stop.Load() {
				c := names[r.IntN(counters)]
				delta := r.IntN(5) - 2
				err := s.Atomic([]string{c, "total"}, func(es []*Entry) error {
					for _, e := range es {
						e.Put(NewString([]byte(strconv.Itoa(atoi(e.Value()) + delta))))
					}
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				ops.Add(1)
			}
		}()
	}

	for round := range 30 {
		enc := newTextEnc()
		hits0 := s.gateHits.Load()
		sn, err := s.BeginSnapshot(nil, enc.encode)
		if err != nil {
			t.Fatal(err)
		}
		// Let writers run against the pending snapshot. Up to one finished-
		// but-uncounted operation per writer predates BeginSnapshot, so wait
		// for more than that.
		ops0 := ops.Load()
		for ops.Load() < ops0+8 {
			runtime.Gosched()
		}
		blocks := drain(t, sn)
		sn.Close()

		got := parseBlocks(t, blocks)
		total, err := strconv.Atoi(got["total"])
		if err != nil {
			t.Fatalf("round %d: total = %q", round, got["total"])
		}
		sum := 0
		for _, c := range names {
			n, err := strconv.Atoi(got[c])
			if err != nil {
				t.Fatalf("round %d: %s = %q", round, c, got[c])
			}
			sum += n
		}
		if total != sum {
			t.Fatalf("round %d: snapshot has total=%d but the counters add up to %d", round, total, sum)
		}
		if s.gateHits.Load() == hits0 {
			t.Fatalf("round %d: no writer reached a pending shard, so the gate was not exercised", round)
		}
	}
	stop.Store(true)
	wg.Wait()
}
