package store

import (
	"errors"
	"hash/maphash"
	"sync"
)

// DefaultShards is the default number of shards.
const DefaultShards = 32

// ErrInvalidShardCount is returned by New when the shard count is not a
// positive power of two.
var ErrInvalidShardCount = errors.New("store: shard count must be a positive power of two")

type shard struct {
	mu sync.RWMutex
	m  map[string][]byte
}

// Store is a sharded, concurrency-safe map from string keys to binary-safe
// values. Values are copied on the way in and on the way out, so callers can
// never mutate internal state through a retained slice.
//
// Phase 2 extension point: option-carrying writes (SET NX/XX/EX...) should be
// added as a method built on the same shardFor + lock pattern as Update; Set
// stays the simple unconditional case.
type Store struct {
	shards []shard
	mask   uint64
	seed   maphash.Seed
}

// New returns a Store with the given number of shards (a power of two).
func New(shards int) (*Store, error) {
	if shards <= 0 || shards&(shards-1) != 0 {
		return nil, ErrInvalidShardCount
	}
	s := &Store{
		shards: make([]shard, shards),
		mask:   uint64(shards - 1),
		seed:   maphash.MakeSeed(), // random per store: defeats hash-flooding
	}
	for i := range s.shards {
		s.shards[i].m = make(map[string][]byte)
	}
	return s, nil
}

func (s *Store) shardFor(key string) *shard {
	return &s.shards[maphash.String(s.seed, key)&s.mask]
}

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// Get returns a copy of the value for key.
func (s *Store) Get(key string) ([]byte, bool) {
	sh := s.shardFor(key)
	sh.mu.RLock()
	v, ok := sh.m[key]
	var out []byte
	if ok {
		out = clone(v)
	}
	sh.mu.RUnlock()
	return out, ok
}

// Set stores a copy of value under key, replacing any previous value.
func (s *Store) Set(key string, value []byte) {
	v := clone(value) // copy outside the lock to keep the critical section short
	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.m[key] = v
	sh.mu.Unlock()
}

// Delete removes the given keys and returns how many existed. Each key is
// deleted atomically, but the call as a whole is not atomic across keys.
func (s *Store) Delete(keys ...string) int {
	n := 0
	for _, k := range keys {
		sh := s.shardFor(k)
		sh.mu.Lock()
		if _, ok := sh.m[k]; ok {
			delete(sh.m, k)
			n++
		}
		sh.mu.Unlock()
	}
	return n
}

// Exists returns how many of keys exist; a key repeated in the argument list
// is counted each time, matching Redis. Per-key atomic only.
func (s *Store) Exists(keys ...string) int {
	n := 0
	for _, k := range keys {
		sh := s.shardFor(k)
		sh.mu.RLock()
		if _, ok := sh.m[k]; ok {
			n++
		}
		sh.mu.RUnlock()
	}
	return n
}

// Len returns the number of keys. It is a moving snapshot, not a global
// atomic one: shards are counted one after another.
func (s *Store) Len() int {
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		sh.mu.RUnlock()
	}
	return n
}

// Flush removes every key, shard by shard (not atomic across shards).
func (s *Store) Flush() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.m = make(map[string][]byte)
		sh.mu.Unlock()
	}
}

// Keys returns all keys matching the Redis glob pattern. Shards are visited one
// at a time; only that shard's read lock is held, and only while its key names
// are snapshotted, so writers on other shards are never blocked and matching
// itself runs lock-free.
func (s *Store) Keys(pattern string) []string {
	var out, snap []string
	all := pattern == "*"
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		snap = snap[:0]
		for k := range sh.m {
			snap = append(snap, k)
		}
		sh.mu.RUnlock()
		for _, k := range snap {
			if all || Match(pattern, k) {
				out = append(out, k)
			}
		}
	}
	return out
}

// Update atomically reads, transforms and writes one key (the primitive behind
// INCR, DECR, INCRBY and APPEND).
//
// fn receives a private copy of the current value (nil when exists is false)
// and returns the new value. Ownership of the returned slice transfers to the
// store: fn must not retain or modify it afterwards. If fn returns an error,
// nothing is written and the error is returned unchanged. fn runs under the
// shard's write lock, so it must be fast and must not call back into the Store.
func (s *Store) Update(key string, fn func(old []byte, exists bool) (newValue []byte, err error)) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock() // also released if fn panics
	old, ok := sh.m[key]
	var arg []byte
	if ok {
		arg = clone(old)
	}
	nv, err := fn(arg, ok)
	if err != nil {
		return err
	}
	if nv == nil {
		nv = []byte{}
	}
	sh.m[key] = nv
	return nil
}
