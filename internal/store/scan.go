package store

import "errors"

// ErrInvalidCursor is returned by Scan for a cursor it could not have issued.
var ErrInvalidCursor = errors.New("store: invalid scan cursor")

// A scan cursor packs a position into one uint64 (Redis cursors are opaque
// unsigned 64-bit decimals):
//
//	cursor = shard<<scanSlotBits | n
//
// shard is the shard being walked. n is how many of that shard's slots are
// still unvisited, i.e. slots [0, n); n == 0 means "this shard has not been
// started", which Scan reads as "all of its current slots". Cursor 0 is
// therefore both the start (shard 0, fresh) and the end (no shard left).
const (
	scanSlotBits = 40
	scanSlotMask = 1<<scanSlotBits - 1
)

// scanItem is a key snapshotted under the shard lock; filtering runs after
// the lock is released.
type scanItem struct {
	key  string
	kind Kind
}

// Scan returns a batch of keys and the cursor for the next call; a returned
// cursor of 0 means the iteration is complete. Start with cursor 0.
//
// Guarantee: every key that exists, unexpired, from the first call until the
// call that returns cursor 0 is returned at least once. Keys may be returned
// more than once, and keys created or deleted during the iteration may or may
// not appear.
//
// Why it holds: shards are walked in ascending index order, each shard's
// slots from the highest to the lowest. A key never changes shard, and within
// a shard a key's slot never increases (new keys take the next slot, and a
// deletion moves the last key into the hole, a lower slot). So a key still
// waiting to be visited stays in the unvisited region [0, n) however the
// shard changes; a key moved from the visited top part into that region is
// merely visited twice. If the shard has shrunk below the cursor's n, n is
// clamped to its length, which is safe for the same reason.
//
// count is the number of slots examined, a hint as in Redis (values below 1
// are treated as 1); expired slots are examined but not returned. keep, if
// non-nil, filters the batch (MATCH, TYPE) after the shard lock is released,
// so the batch can be empty with a non-zero cursor.
//
// Cost per call: O(count) slots plus one read-lock acquisition per shard
// entered, so O(count + shards) worst case, independent of the keyspace size.
// A full iteration is O(keys + shards x calls). Cursors name a shard below
// len(shards) and a slot count below 2^40, otherwise ErrInvalidCursor; a
// well-formed cursor that no longer matches the data is simply clamped.
func (s *Store) Scan(cursor uint64, count int, keep func(key string, kind Kind) bool) (next uint64, keys []string, err error) {
	shardIdx := cursor >> scanSlotBits
	rem := cursor & scanSlotMask
	if shardIdx >= uint64(len(s.shards)) {
		return 0, nil, ErrInvalidCursor
	}
	budget := max(count, 1)
	now := s.clock.NowMs()
	var snap []scanItem
	i := int(shardIdx)
	for i < len(s.shards) && budget > 0 {
		sh := &s.shards[i]
		sh.mu.RLock()
		n := len(sh.order)
		c := n
		if rem != 0 && rem < uint64(n) {
			c = int(rem)
		}
		take := min(c, budget)
		snap = snap[:0]
		for j := c - 1; j >= c-take; j-- {
			k := sh.order[j]
			if sh.expired(k, now) {
				continue
			}
			if v, ok := sh.m[k]; ok {
				snap = append(snap, scanItem{key: k, kind: v.Kind()})
			}
		}
		sh.mu.RUnlock()

		for _, it := range snap {
			if keep == nil || keep(it.key, it.kind) {
				keys = append(keys, it.key)
			}
		}
		budget -= take
		c -= take
		if c > 0 { // budget ran out inside this shard
			return uint64(i)<<scanSlotBits | uint64(c), keys, nil
		}
		i++
		rem = 0
	}
	if i >= len(s.shards) {
		return 0, keys, nil
	}
	return uint64(i) << scanSlotBits, keys, nil // next shard, fresh
}

// liveFrom returns the first unexpired key of sh, looking cyclically from slot
// off. The caller holds a lock on sh.
func (sh *shard) liveFrom(off int, now int64) (string, bool) {
	n := len(sh.order)
	for j := 0; j < n; j++ {
		k := sh.order[(off+j)%n]
		if !sh.expired(k, now) {
			return k, true
		}
	}
	return "", false
}

// RandomKey returns a random live key, or false if there is none. intN(n)
// must return a value in [0, n) for n > 0 (math/rand/v2's IntN, or a seeded
// generator in tests).
//
// A shard is chosen with probability proportional to its slot count and a slot
// uniformly within it, so the choice is uniform over keys up to concurrent
// changes. Expired keys are skipped by probing onward, so an unlucky draw can
// cost O(shard size); the usual cost is O(shards): it reads each shard's length
// once, one lock at a time.
func (s *Store) RandomKey(intN func(n int) int) (string, bool) {
	lens := make([]int, len(s.shards))
	total := 0
	for i := range s.shards {
		sh := &s.shards[i]
		sh.mu.RLock()
		lens[i] = len(sh.order)
		sh.mu.RUnlock()
		total += lens[i]
	}
	if total == 0 {
		return "", false
	}
	pick := intN(total)
	first, off := 0, 0
	for i, n := range lens {
		if pick < n {
			first, off = i, pick
			break
		}
		pick -= n
	}
	now := s.clock.NowMs()
	for d := range s.shards {
		sh := &s.shards[(first+d)&int(s.mask)]
		sh.mu.RLock()
		if d > 0 && len(sh.order) > 0 {
			off = intN(len(sh.order))
		}
		k, ok := sh.liveFrom(off, now)
		sh.mu.RUnlock()
		if ok {
			return k, true
		}
	}
	return "", false
}
