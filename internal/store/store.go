package store

import (
	"errors"
	"hash/maphash"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultShards is the default number of shards.
const DefaultShards = 32

// ErrInvalidShardCount is returned by New when the shard count is not a
// positive power of two.
var ErrInvalidShardCount = errors.New("store: shard count must be a positive power of two")

// orderShrinkMinCap is the capacity below which a shard's order slice is never
// reallocated to give memory back.
const orderShrinkMinCap = 64

// shard guards one slice of the keyspace. Invariants: every key in exp is also
// in m (see expire.go); order and pos index exactly the keys of m, with
// pos[order[i]] == i (see scan.go for why SCAN needs this).
type shard struct {
	mu  sync.RWMutex
	m   map[string]Value
	exp map[string]int64 // key -> absolute deadline, Unix ms

	// order lists the keys of m in slot order; a new key takes the next slot
	// and a deleted key's slot is refilled by moving the last key into it.
	order []string
	pos   map[string]int // key -> index in order
}

// Options configures NewWithOptions. Zero values select defaults, except
// Shards, which must be a positive power of two.
type Options struct {
	// Shards is the number of shards.
	Shards int
	// Clock supplies time; nil means SystemClock.
	Clock Clock
	// Sweep tunes the active expiry sweeper; the zero value means
	// DefaultSweepConfig.
	Sweep SweepConfig
}

// Store is a sharded, concurrency-safe map from string keys to typed values
// with optional per-key expiry. Strings are copied on the way in and out;
// collection values are reachable only through closures that run under the
// shard lock (View, Mutate, MutateEntry, Atomic, AtomicView), so callers can
// never mutate internal state through a retained reference.
//
// Closure rules, for every closure-taking method: the closure runs under a
// shard lock, so it must be fast, must not block, must not call back into the
// Store (the locks are not reentrant), and must not retain the Values it is
// given.
type Store struct {
	shards []shard
	mask   uint64
	seed   maphash.Seed
	clock  Clock
	sweep  SweepConfig

	expiredLazy   atomic.Int64
	expiredActive atomic.Int64
	sweepCycles   atomic.Int64

	wall func() time.Time // real-time source for the sweep budget; tests replace it
	// afterLock, if set, runs after each shard lock Atomic/AtomicView takes.
	// Tests use it to yield between acquisitions and widen the deadlock window.
	afterLock func()
	lc        lifecycle
}

// New returns a Store with the given number of shards (a power of two), the
// system clock and default sweep settings. The sweeper is not running until
// Start is called.
func New(shards int) (*Store, error) {
	return NewWithOptions(Options{Shards: shards})
}

// NewWithOptions returns a Store configured by o.
func NewWithOptions(o Options) (*Store, error) {
	if o.Shards <= 0 || o.Shards&(o.Shards-1) != 0 {
		return nil, ErrInvalidShardCount
	}
	if o.Clock == nil {
		o.Clock = SystemClock{}
	}
	if o.Sweep == (SweepConfig{}) {
		o.Sweep = DefaultSweepConfig()
	}
	if err := o.Sweep.Validate(); err != nil {
		return nil, err
	}
	s := &Store{
		shards: make([]shard, o.Shards),
		mask:   uint64(o.Shards - 1),
		seed:   maphash.MakeSeed(), // random per store: defeats hash-flooding
		clock:  o.Clock,
		sweep:  o.Sweep,
		wall:   time.Now,
	}
	s.lc.stop = make(chan struct{})
	for i := range s.shards {
		s.shards[i].m = make(map[string]Value)
		s.shards[i].exp = make(map[string]int64)
		s.shards[i].pos = make(map[string]int)
	}
	return s, nil
}

func (s *Store) shardIndex(key string) int {
	return int(maphash.String(s.seed, key) & s.mask)
}

func (s *Store) shardFor(key string) *shard { return &s.shards[s.shardIndex(key)] }

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// --- shard helpers; callers hold the appropriate lock ---

// expired reports whether key has a deadline strictly before now.
func (sh *shard) expired(key string, now int64) bool {
	at, ok := sh.exp[key]
	return ok && now > at
}

// track records key in the slot index if it is new. The write lock must be
// held, and the caller must have stored (or be about to store) key in m.
func (sh *shard) track(key string) {
	if _, ok := sh.pos[key]; ok {
		return
	}
	sh.pos[key] = len(sh.order)
	sh.order = append(sh.order, key)
}

// untrack drops key from the slot index by moving the last key into its slot
// (swap-remove). Keys therefore only ever move to a lower slot, which is the
// property SCAN's descending walk relies on. The write lock must be held.
func (sh *shard) untrack(key string) {
	i, ok := sh.pos[key]
	if !ok {
		return
	}
	last := len(sh.order) - 1
	if i != last {
		moved := sh.order[last]
		sh.order[i] = moved
		sh.pos[moved] = i
	}
	sh.order[last] = "" // do not pin the key's bytes through the spare capacity
	sh.order = sh.order[:last]
	delete(sh.pos, key)
	// Go slices never shrink on their own; give memory back after mass deletion.
	if c := cap(sh.order); c > orderShrinkMinCap && len(sh.order) <= c/4 {
		sh.order = append(make([]string, 0, 2*len(sh.order)), sh.order...)
	}
}

// remove deletes key together with its deadline.
func (sh *shard) remove(key string) {
	delete(sh.m, key)
	delete(sh.exp, key)
	sh.untrack(key)
}

// peek returns the live value of key without mutating anything (read lock
// suffices). stale reports that the key exists but has expired.
func (sh *shard) peek(key string, now int64) (v Value, stale bool) {
	v, ok := sh.m[key]
	if !ok {
		return nil, false
	}
	if sh.expired(key, now) {
		return nil, true
	}
	return v, false
}

// put stores v under key and leaves any deadline untouched. A nil value or an
// empty Collection deletes the key and its deadline instead, so empty
// collections never become visible. Callers must pass an untyped nil, not a
// nil pointer wrapped in a Value.
func (sh *shard) put(key string, v Value) {
	if v == nil {
		sh.remove(key)
		return
	}
	if c, ok := v.(Collection); ok && c.Len() == 0 {
		sh.remove(key)
		return
	}
	sh.m[key] = v
	sh.track(key)
}

// liveLocked returns the live value of key, removing it first if it has
// expired (lazy expiry). The write lock must be held.
func (s *Store) liveLocked(sh *shard, key string, now int64) Value {
	v, ok := sh.m[key]
	if !ok {
		return nil
	}
	if sh.expired(key, now) {
		sh.remove(key)
		s.expiredLazy.Add(1)
		return nil
	}
	return v
}

// reap removes key if it is expired.
func (s *Store) reap(sh *shard, key string) {
	sh.mu.Lock()
	s.liveLocked(sh, key, s.clock.NowMs())
	sh.mu.Unlock()
}

// read runs fn under the shard's read lock with the live value of key (nil if
// missing). If the key is found expired it is first reaped under the write
// lock, then read again; at most two passes, so a misbehaving clock cannot
// make it spin.
func (s *Store) read(key string, fn func(sh *shard, v Value) error) error {
	sh := s.shardFor(key)
	if done, err := s.readOnce(sh, key, fn, false); done {
		return err
	}
	s.reap(sh, key)
	_, err := s.readOnce(sh, key, fn, true)
	return err
}

func (s *Store) readOnce(sh *shard, key string, fn func(*shard, Value) error, force bool) (bool, error) {
	sh.mu.RLock()
	defer sh.mu.RUnlock() // also released if fn panics
	v, stale := sh.peek(key, s.clock.NowMs())
	if stale && !force {
		return false, nil
	}
	return true, fn(sh, v)
}

// --- typed access ---

// View runs fn under the shard's read lock with the live value of key, or nil
// if the key is missing or expired. fn must not modify the value. Any error
// from fn is returned unchanged.
func (s *Store) View(key string, fn func(v Value) error) error {
	return s.read(key, func(_ *shard, v Value) error { return fn(v) })
}

// Mutate runs fn under the shard's write lock with the live value of key (nil
// if missing or expired) and stores what fn returns. Returning nil, or a
// Collection that is now empty, deletes the key and its TTL; otherwise the
// key's TTL is kept. fn may modify cur in place and return it.
//
// If fn returns an error the key is left as it is, so fn must validate before
// it modifies cur in place.
func (s *Store) Mutate(key string, fn func(cur Value) (Value, error)) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	nv, err := fn(s.liveLocked(sh, key, s.clock.NowMs()))
	if err != nil {
		return err
	}
	sh.put(key, nv)
	return nil
}

// Entry is a handle to one key inside MutateEntry or Atomic. It is valid only
// until the closure returns. Every mutator takes effect immediately, so a
// closure that wants to fail should return its error before calling any.
type Entry struct {
	sh  *shard
	key string
	now int64
}

// Now returns the clock reading taken when the locks were acquired, so
// relative TTLs computed from it agree with the store's own expiry checks.
func (e *Entry) Now() int64 { return e.now }

// Exists reports whether the key currently holds a value.
func (e *Entry) Exists() bool {
	_, ok := e.sh.m[e.key]
	return ok
}

// Value returns the current value, or nil if the key does not exist.
func (e *Entry) Value() Value { return e.sh.m[e.key] }

// Put stores v, leaving any TTL as it is (call Persist for SET semantics). A
// nil v or an empty Collection deletes the key and its TTL.
func (e *Entry) Put(v Value) { e.sh.put(e.key, v) }

// Delete removes the key and its TTL and reports whether it existed.
func (e *Entry) Delete() bool {
	_, ok := e.sh.m[e.key]
	e.sh.remove(e.key)
	return ok
}

// ExpireAt returns the key's absolute deadline in Unix milliseconds.
func (e *Entry) ExpireAt() (ms int64, ok bool) {
	ms, ok = e.sh.exp[e.key]
	return ms, ok
}

// SetExpireAt sets the key's absolute deadline. A deadline at or before Now
// deletes the key immediately. It returns false, doing nothing, if the key
// does not exist.
func (e *Entry) SetExpireAt(ms int64) bool {
	if !e.Exists() {
		return false
	}
	if ms <= e.now {
		e.sh.remove(e.key)
		return true
	}
	e.sh.exp[e.key] = ms
	return true
}

// Persist removes the key's TTL and reports whether it had one.
func (e *Entry) Persist() bool {
	_, ok := e.sh.exp[e.key]
	delete(e.sh.exp, e.key)
	return ok
}

// MoveTo moves e's value and deadline to dst, replacing whatever dst held
// (value of any type and its TTL), and removes e's key. It is the primitive
// behind RENAME. Unlike Put plus SetExpireAt it keeps a deadline that equals
// the current millisecond instead of deleting the key. It does nothing if e's
// key does not exist or e and dst name the same key. Both entries must come
// from the same Atomic call, so both shards are locked.
func (e *Entry) MoveTo(dst *Entry) {
	if e == dst || e.key == dst.key {
		return
	}
	v, ok := e.sh.m[e.key]
	if !ok {
		return
	}
	at, hasAt := e.sh.exp[e.key]
	e.sh.remove(e.key)
	dst.sh.put(dst.key, v) // v is a live, non-empty value, so it is stored
	if hasAt {
		dst.sh.exp[dst.key] = at
	} else {
		delete(dst.sh.exp, dst.key)
	}
}

// MutateEntry runs fn under the shard's write lock with a handle to key. It is
// the TTL-aware sibling of Mutate, used by SET, EXPIRE, GETEX, GETDEL and
// similar commands that read and change value and deadline together.
func (s *Store) MutateEntry(key string, fn func(e *Entry) error) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock()
	now := s.clock.NowMs()
	s.liveLocked(sh, key, now)
	return fn(&Entry{sh: sh, key: key, now: now})
}

// --- multi-key atomic access ---

// lockOrder returns the distinct shard indexes of keys in ascending order.
//
// Lock-ordering rule: any operation that needs more than one shard lock MUST
// acquire them in ascending shard-index order, each shard once. With every
// multi-lock path following one global order there can be no cycle of waiting
// goroutines, so no deadlock. Single-key operations hold exactly one shard
// lock and cannot take part in a cycle. The sweeper, Scan, RandomKey and Flush
// also hold one lock at a time.
func (s *Store) lockOrder(keys []string) []int {
	order := make([]int, len(keys))
	for i, k := range keys {
		order[i] = s.shardIndex(k)
	}
	slices.Sort(order)
	return slices.Compact(order)
}

// Atomic runs fn with write access to all keys at once. It locks the involved
// shards in ascending index order (see lockOrder), so concurrent Atomic and
// AtomicView calls cannot deadlock. entries[i] is the handle for keys[i];
// repeated keys share one Entry. fn must follow the closure rules on Store.
func (s *Store) Atomic(keys []string, fn func(entries []*Entry) error) error {
	order := s.lockOrder(keys)
	for _, i := range order {
		s.shards[i].mu.Lock()
		if s.afterLock != nil {
			s.afterLock()
		}
	}
	defer func() {
		for j := len(order) - 1; j >= 0; j-- {
			s.shards[order[j]].mu.Unlock()
		}
	}()
	now := s.clock.NowMs()
	entries := make([]*Entry, len(keys))
	var seen map[string]*Entry
	if len(keys) > 1 {
		seen = make(map[string]*Entry, len(keys))
	}
	for i, k := range keys {
		if e, ok := seen[k]; ok {
			entries[i] = e
			continue
		}
		sh := s.shardFor(k)
		s.liveLocked(sh, k, now)
		e := &Entry{sh: sh, key: k, now: now}
		entries[i] = e
		if seen != nil {
			seen[k] = e
		}
	}
	return fn(entries)
}

// AtomicView is the read-only form of Atomic: it read-locks the involved
// shards in ascending order and passes fn a consistent view, values[i] being
// the live value of keys[i] (nil if missing or expired). Expired keys are
// skipped, not reaped, because only read locks are held.
func (s *Store) AtomicView(keys []string, fn func(values []Value) error) error {
	order := s.lockOrder(keys)
	for _, i := range order {
		s.shards[i].mu.RLock()
		if s.afterLock != nil {
			s.afterLock()
		}
	}
	defer func() {
		for j := len(order) - 1; j >= 0; j-- {
			s.shards[order[j]].mu.RUnlock()
		}
	}()
	now := s.clock.NowMs()
	values := make([]Value, len(keys))
	for i, k := range keys {
		values[i], _ = s.shardFor(k).peek(k, now)
	}
	return fn(values)
}

// --- string convenience API (Phase 1) ---
//
// These operate on string values. Where a key holds another kind they behave
// as if it were absent (Get, Exists) or return ErrWrongType (Update), so
// type-aware command handlers should use View, Mutate or MutateEntry.

// Get returns a copy of the string value for key. ok is false if the key is
// missing, expired, or not a string.
func (s *Store) Get(key string) ([]byte, bool) {
	var out []byte
	var ok bool
	_ = s.View(key, func(v Value) error {
		if str, is := v.(*String); is {
			out, ok = clone(str.b), true
		}
		return nil
	})
	return out, ok
}

// Set stores a copy of value under key, replacing any previous value of any
// type and clearing its TTL.
func (s *Store) Set(key string, value []byte) {
	v := NewString(value) // copy outside the lock to keep the critical section short
	sh := s.shardFor(key)
	sh.mu.Lock()
	sh.m[key] = v
	sh.track(key)
	delete(sh.exp, key)
	sh.mu.Unlock()
}

// Delete removes the given keys and returns how many existed (expired keys do
// not count). Each key is deleted atomically, but the call as a whole is not
// atomic across keys.
func (s *Store) Delete(keys ...string) int {
	n := 0
	for _, k := range keys {
		sh := s.shardFor(k)
		sh.mu.Lock()
		if s.liveLocked(sh, k, s.clock.NowMs()) != nil {
			sh.remove(k)
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
		_ = s.View(k, func(v Value) error {
			if v != nil {
				n++
			}
			return nil
		})
	}
	return n
}

// Len returns the number of live keys; expired keys that the sweeper has not
// reaped yet are not counted (Redis counts them). The cost is proportional to
// the number of keys with a TTL, not O(1). It is a moving snapshot: shards are
// counted one after another.
func (s *Store) Len() int {
	now := s.clock.NowMs()
	n := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		n += len(sh.m)
		for _, at := range sh.exp {
			if now > at {
				n--
			}
		}
		sh.mu.RUnlock()
	}
	return n
}

// Flush removes every key and TTL, shard by shard (not atomic across shards).
func (s *Store) Flush() {
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.Lock()
		sh.m = make(map[string]Value)
		sh.exp = make(map[string]int64)
		sh.pos = make(map[string]int)
		sh.order = nil
		sh.mu.Unlock()
	}
}

// Keys returns all live keys matching the Redis glob pattern. Shards are
// visited one at a time; only that shard's read lock is held, and only while
// its key names are snapshotted, so writers on other shards are never blocked
// and matching itself runs lock-free. Expired keys are skipped, not reaped.
func (s *Store) Keys(pattern string) []string {
	var out, snap []string
	all := pattern == "*"
	now := s.clock.NowMs()
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		snap = snap[:0]
		for k := range sh.m {
			if !sh.expired(k, now) {
				snap = append(snap, k)
			}
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

// Update atomically reads, transforms and writes one string key (the
// primitive behind INCR, DECR, INCRBY and APPEND). The key's TTL is kept.
//
// fn receives a private copy of the current value (nil when exists is false)
// and returns the new value. Ownership of the returned slice transfers to the
// store: fn must not retain or modify it afterwards. If fn returns an error,
// nothing is written and the error is returned unchanged. If the key holds a
// non-string value, Update returns ErrWrongType without calling fn. fn runs
// under the shard's write lock, so it must be fast and must not call back into
// the Store.
func (s *Store) Update(key string, fn func(old []byte, exists bool) (newValue []byte, err error)) error {
	sh := s.shardFor(key)
	sh.mu.Lock()
	defer sh.mu.Unlock() // also released if fn panics
	cur := s.liveLocked(sh, key, s.clock.NowMs())
	var arg []byte
	if cur != nil {
		str, ok := cur.(*String)
		if !ok {
			return ErrWrongType
		}
		arg = clone(str.b)
	}
	nv, err := fn(arg, cur != nil)
	if err != nil {
		return err
	}
	if nv == nil {
		nv = []byte{}
	}
	sh.m[key] = &String{b: nv}
	sh.track(key)
	return nil
}
