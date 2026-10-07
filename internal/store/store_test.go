package store

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"sync"
	"testing"
)

func newStore(t testing.TB, n int) *Store {
	t.Helper()
	s, err := New(n)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestNewValidatesShards(t *testing.T) {
	for _, n := range []int{0, -1, 3, 6, 100} {
		if _, err := New(n); !errors.Is(err, ErrInvalidShardCount) {
			t.Errorf("New(%d) err = %v, want ErrInvalidShardCount", n, err)
		}
	}
	for _, n := range []int{1, 2, 32, 1024} {
		if _, err := New(n); err != nil {
			t.Errorf("New(%d): %v", n, err)
		}
	}
}

func TestCopySemantics(t *testing.T) {
	s := newStore(t, 4)
	in := []byte("hello")
	s.Set("k", in)
	in[0] = 'X' // mutating the caller's slice must not affect the store
	got, ok := s.Get("k")
	if !ok || string(got) != "hello" {
		t.Fatalf("got %q,%v", got, ok)
	}
	got[0] = 'Y' // mutating the returned slice must not affect the store
	again, _ := s.Get("k")
	if string(again) != "hello" {
		t.Fatalf("store mutated through returned slice: %q", again)
	}
}

func TestBasicOps(t *testing.T) {
	s := newStore(t, 8)
	if _, ok := s.Get("missing"); ok {
		t.Fatal("unexpected hit")
	}
	s.Set("a", []byte("1"))
	s.Set("b", nil)
	if v, ok := s.Get("b"); !ok || len(v) != 0 {
		t.Fatalf("empty value: %q,%v", v, ok)
	}
	if n := s.Exists("a", "a", "zz"); n != 2 {
		t.Fatalf("Exists = %d, want 2 (duplicates counted)", n)
	}
	if n := s.Delete("a", "a", "zz"); n != 1 {
		t.Fatalf("Delete = %d, want 1", n)
	}
	if s.Len() != 1 {
		t.Fatalf("Len = %d", s.Len())
	}
	s.Flush()
	if s.Len() != 0 {
		t.Fatal("Flush left keys behind")
	}
}

func TestKeys(t *testing.T) {
	s := newStore(t, 4)
	for _, k := range []string{"user:1", "user:2", "order:1", "x"} {
		s.Set(k, []byte("v"))
	}
	got := s.Keys("user:*")
	sort.Strings(got)
	if fmt.Sprint(got) != "[user:1 user:2]" {
		t.Fatalf("got %v", got)
	}
	if n := len(s.Keys("*")); n != 4 {
		t.Fatalf("Keys(*) = %d", n)
	}
	if n := len(s.Keys("nomatch*")); n != 0 {
		t.Fatalf("Keys(nomatch*) = %d", n)
	}
}

func TestUpdateAtomicIncrements(t *testing.T) {
	s := newStore(t, 4)
	const goroutines, per = 64, 500
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				err := s.Update("ctr", func(old []byte, exists bool) ([]byte, error) {
					n := 0
					if exists {
						n, _ = strconv.Atoi(string(old))
					}
					return []byte(strconv.Itoa(n + 1)), nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	v, _ := s.Get("ctr")
	if string(v) != strconv.Itoa(goroutines*per) {
		t.Fatalf("counter = %s, want %d", v, goroutines*per)
	}
}

func TestUpdateErrorLeavesValue(t *testing.T) {
	s := newStore(t, 2)
	s.Set("k", []byte("keep"))
	boom := errors.New("boom")
	err := s.Update("k", func([]byte, bool) ([]byte, error) { return []byte("lost"), boom })
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	if v, _ := s.Get("k"); string(v) != "keep" {
		t.Fatalf("value changed to %q", v)
	}
}

func TestUpdatePassesPrivateCopy(t *testing.T) {
	s := newStore(t, 2)
	s.Set("k", []byte("abc"))
	_ = s.Update("k", func(old []byte, _ bool) ([]byte, error) {
		old[0] = 'Z' // fn owns its copy; the store must be unaffected on error
		return nil, errors.New("abort")
	})
	if v, _ := s.Get("k"); string(v) != "abc" {
		t.Fatalf("store mutated via Update arg: %q", v)
	}
}

func TestConcurrentMixed(t *testing.T) {
	s := newStore(t, 16)
	var wg sync.WaitGroup
	for g := 0; g < 32; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 500; i++ {
				k := "k" + strconv.Itoa((g*31+i)%200)
				switch i % 5 {
				case 0:
					s.Set(k, []byte("v"))
				case 1:
					s.Get(k)
				case 2:
					s.Delete(k)
				case 3:
					s.Exists(k, "other")
				default:
					s.Keys("k1*")
					s.Len()
				}
			}
		}(g)
	}
	wg.Wait()
}

// --- Benchmarks: sharded store vs. a single RWMutex-guarded map ---

type mutexMap struct {
	mu sync.RWMutex
	m  map[string][]byte
}

func (m *mutexMap) Get(k string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	v, ok := m.m[k]
	return append([]byte(nil), v...), ok
}

func (m *mutexMap) Set(k string, v []byte) {
	c := append([]byte(nil), v...)
	m.mu.Lock()
	m.m[k] = c
	m.mu.Unlock()
}

type kv interface {
	Get(string) ([]byte, bool)
	Set(string, []byte)
}

func benchMixed(b *testing.B, db kv, goroutines int) {
	keys := make([]string, 1024)
	val := []byte("value")
	for i := range keys {
		keys[i] = "key:" + strconv.Itoa(i)
		db.Set(keys[i], val)
	}
	per := max(b.N/goroutines, 1)
	b.ResetTimer()
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(seed int) {
			defer wg.Done()
			for i := 0; i < per; i++ {
				k := keys[(seed*7919+i)%len(keys)]
				if i%10 == 0 { // 90% reads / 10% writes
					db.Set(k, val)
				} else {
					db.Get(k)
				}
			}
		}(g)
	}
	wg.Wait()
}

func BenchmarkMixed(b *testing.B) {
	for _, g := range []int{1, 8, 64} {
		b.Run(fmt.Sprintf("sharded/%d", g), func(b *testing.B) {
			benchMixed(b, newStore(b, DefaultShards), g)
		})
		b.Run(fmt.Sprintf("single-mutex/%d", g), func(b *testing.B) {
			benchMixed(b, &mutexMap{m: make(map[string][]byte)}, g)
		})
	}
}
