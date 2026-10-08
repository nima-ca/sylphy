package store

import (
	"fmt"
	"runtime"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"
)

// keysInDifferentShards returns two keys living in shards 0 and 1 of a
// two-shard store.
func keysInDifferentShards(t testing.TB, s *Store) (a, b string) {
	t.Helper()
	for i := 0; i < 10_000 && (a == "" || b == ""); i++ {
		k := "k" + strconv.Itoa(i)
		switch s.shardIndex(k) {
		case 0:
			if a == "" {
				a = k
			}
		case 1:
			if b == "" {
				b = k
			}
		}
	}
	if a == "" || b == "" {
		t.Fatal("could not find keys in both shards")
	}
	return a, b
}

func TestLockOrderIsSortedAndDeduplicated(t *testing.T) {
	s := newStore(t, 8)
	var keys []string
	for i := 0; i < 200; i++ {
		keys = append(keys, "key"+strconv.Itoa(i))
	}
	keys = append(keys, keys[:50]...) // repeats must not add shards
	order := s.lockOrder(keys)
	if !slices.IsSorted(order) {
		t.Fatalf("not ascending: %v", order)
	}
	if len(slices.Compact(slices.Clone(order))) != len(order) {
		t.Fatalf("duplicates: %v", order)
	}
	if len(order) > 8 {
		t.Fatalf("more shards than exist: %v", order)
	}
	if got := s.lockOrder(nil); len(got) != 0 {
		t.Fatalf("no keys must lock nothing, got %v", got)
	}
}

// Workers lock the same two shards in opposite argument order. If Atomic took
// locks in argument order this would deadlock almost immediately; with the
// ascending-index rule it must always finish.
func TestAtomicOppositeKeyOrderDoesNotDeadlock(t *testing.T) {
	s := newStore(t, 2)
	s.afterLock = runtime.Gosched // yield while holding the first lock
	ka, kb := keysInDifferentShards(t, s)
	const workers, iters = 16, 2000

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			keys := []string{ka, kb}
			if w%2 == 1 {
				keys = []string{kb, ka}
			}
			for i := 0; i < iters; i++ {
				err := s.Atomic(keys, func(es []*Entry) error {
					es[0].Put(NewString([]byte("x")))
					es[1].Put(NewString([]byte("y")))
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
				if i%4 == 0 {
					_ = s.AtomicView(keys, func([]Value) error { return nil })
				}
			}
		}(w)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("deadlock: Atomic must lock shards in ascending index order")
	}
}

func TestAtomicSharesEntryForRepeatedKeys(t *testing.T) {
	s := newStore(t, 4)
	err := s.Atomic([]string{"a", "a", "b"}, func(es []*Entry) error {
		if es[0] != es[1] || es[0] == es[2] {
			t.Error("repeated keys must share one Entry, distinct keys must not")
		}
		es[0].Put(NewString([]byte("v")))
		if !es[1].Exists() || es[2].Exists() {
			t.Error("writes through one handle must be visible through its twin")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAtomicErrorIsReturnedAndLocksReleased(t *testing.T) {
	s := newStore(t, 2)
	ka, kb := keysInDifferentShards(t, s)
	boom := fmt.Errorf("boom")
	if err := s.Atomic([]string{kb, ka}, func([]*Entry) error { return boom }); err != boom {
		t.Fatalf("err = %v", err)
	}
	// Would hang if a lock leaked.
	s.Set(ka, []byte("1"))
	s.Set(kb, []byte("2"))
	if s.Len() != 2 {
		t.Fatalf("Len = %d", s.Len())
	}
}

// Moves one unit between two keys in different shards while readers check the
// total: AtomicView must never observe a half-finished transfer.
func TestAtomicTransferIsAtomicAcrossShards(t *testing.T) {
	s := newStore(t, 2)
	ka, kb := keysInDifferentShards(t, s)
	const total = 1000
	s.Set(ka, []byte(strconv.Itoa(total)))
	s.Set(kb, []byte("0"))

	get := func(v Value) int {
		n, _ := strconv.Atoi(string(v.(*String).Bytes()))
		return n
	}
	var wg sync.WaitGroup
	for w := 0; w < 4; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			src, dst := ka, kb
			if w%2 == 1 {
				src, dst = kb, ka
			}
			for i := 0; i < 1500; i++ {
				_ = s.Atomic([]string{src, dst}, func(es []*Entry) error {
					from, to := get(es[0].Value()), get(es[1].Value())
					if from == 0 {
						return nil
					}
					es[0].Put(NewString([]byte(strconv.Itoa(from - 1))))
					es[1].Put(NewString([]byte(strconv.Itoa(to + 1))))
					return nil
				})
			}
		}(w)
	}
	for r := 0; r < 4; r++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 1500; i++ {
				_ = s.AtomicView([]string{ka, kb}, func(vs []Value) error {
					if sum := get(vs[0]) + get(vs[1]); sum != total {
						t.Errorf("torn read: sum = %d, want %d", sum, total)
					}
					return nil
				})
			}
		}()
	}
	wg.Wait()
}
