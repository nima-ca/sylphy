// Package zset implements the Redis sorted set value: distinct binary-safe
// members, each with a float64 score, ordered by score and then by member
// bytes.
//
// # Representation
//
// A ZSet pairs a skip list with a map from member to score. The map gives O(1)
// score lookup and membership; the skip list gives ordered iteration and
// O(log n) rank, range and removal. Every skip-list link carries a span (how
// many level-0 steps it jumps), so the rank of a node is the sum of the spans
// crossed while searching for it, and a node can be fetched by rank in
// O(log n). This is the structure Redis uses; a balanced tree would need
// subtree sizes for the same job and an array would make inserts O(n).
//
// Heights are drawn with probability 1/4 per extra level, capped at 32. The
// source of randomness is injected (NewWithRand), so tests are deterministic
// and the package keeps no global state.
//
// # Ordering
//
// Pairs sort by score ascending, ties broken by member bytes ascending. +Inf
// and -Inf are ordinary scores. Scores must never be NaN, since NaN has no
// position in the order: callers (the command layer) reject it before
// calling Set.
//
// # Ownership
//
// Members are strings: copied in once and immutable, so results may be used
// after the shard lock is released. A ZSet is not safe for concurrent use;
// the store serializes access through the shard lock.
package zset

import (
	"maps"
	"math/rand/v2"
	"slices"

	"github.com/nima-ca/sylphy/internal/store"
)

// shrinkMinPeak is the high-water mark below which the member map is never
// rebuilt.
const shrinkMinPeak = 64

// Entry is a member together with its score.
type Entry struct {
	// Member is the member name.
	Member string
	// Score is the member's score.
	Score float64
}

// ScoreRange is a score interval with optionally exclusive ends, the shape of
// ZRANGEBYSCORE's "(1 5" arguments. Infinite bounds are allowed. A range whose
// Min exceeds Max, or whose Min equals Max with an exclusive end, is empty.
type ScoreRange struct {
	// Min and Max are the bounds.
	Min, Max float64
	// MinExclusive and MaxExclusive make the matching bound exclusive.
	MinExclusive, MaxExclusive bool
}

// aboveMin reports whether s satisfies the lower bound.
func (r ScoreRange) aboveMin(s float64) bool {
	if r.MinExclusive {
		return s > r.Min
	}
	return s >= r.Min
}

// belowMax reports whether s satisfies the upper bound.
func (r ScoreRange) belowMax(s float64) bool {
	if r.MaxExclusive {
		return s < r.Max
	}
	return s <= r.Max
}

func (r ScoreRange) empty() bool {
	return r.Min > r.Max || (r.Min == r.Max && (r.MinExclusive || r.MaxExclusive))
}

// ZSet is a sorted set. Create one with New or NewWithRand; the zero value is
// not usable.
type ZSet struct {
	sl   *skipList
	dict map[string]float64
	peak int // largest len(dict) since the map was last rebuilt
}

var _ store.Collection = (*ZSet)(nil)

// New returns an empty sorted set that draws node heights from math/rand/v2's
// goroutine-safe generator.
func New() *ZSet { return NewWithRand(rand.IntN) }

// NewWithRand returns an empty sorted set whose node heights come from intN
// (n > 0 returns a value in [0, n)). A nil intN selects math/rand/v2. Tests
// pass a seeded generator for reproducible structure.
func NewWithRand(intN func(n int) int) *ZSet {
	if intN == nil {
		intN = rand.IntN
	}
	return &ZSet{sl: newSkipList(intN), dict: make(map[string]float64)}
}

// Kind implements store.Value.
func (*ZSet) Kind() store.Kind { return store.KindZSet }

// Len implements store.Collection: the number of members.
func (z *ZSet) Len() int { return len(z.dict) }

// Score returns the score of member in O(1).
func (z *ZSet) Score(member string) (float64, bool) {
	s, ok := z.dict[member]
	return s, ok
}

// Set gives member the score, inserting it if needed, in O(log n). It returns
// the previous score and whether the member existed. score must not be NaN.
func (z *ZSet) Set(member string, score float64) (old float64, existed bool) {
	old, existed = z.dict[member]
	if existed {
		if old == score {
			return old, true
		}
		z.sl.remove(old, member)
	}
	z.sl.insert(score, member)
	z.dict[member] = score
	z.peak = max(z.peak, len(z.dict))
	return old, existed
}

// Remove deletes member and reports whether it was present, in O(log n).
func (z *ZSet) Remove(member string) bool {
	s, ok := z.dict[member]
	if !ok {
		return false
	}
	z.sl.remove(s, member)
	delete(z.dict, member)
	z.compact()
	return true
}

// Rank returns the 0-based position of member in ascending order, in
// O(log n). The reverse rank is Len()-1-rank.
func (z *ZSet) Rank(member string) (int, bool) {
	s, ok := z.dict[member]
	if !ok {
		return 0, false
	}
	return z.sl.rank(s, member) - 1, true
}

// RangeByRank returns the entries at ranks [lo, hi) clamped to the set, in
// O(log n + k). With rev the ranks count from the highest score and the
// result is in descending order. It returns nil for an empty range.
func (z *ZSet) RangeByRank(lo, hi int, rev bool) []Entry {
	n := z.Len()
	lo, hi = max(lo, 0), min(hi, n)
	if lo >= hi {
		return nil
	}
	out := make([]Entry, 0, hi-lo)
	if rev {
		x := z.sl.nodeByRank(n - lo) // reverse rank lo is ascending 1-based n-lo
		for i := lo; i < hi && x != nil; i++ {
			out = append(out, Entry{Member: x.member, Score: x.score})
			x = x.backward
		}
		return out
	}
	x := z.sl.nodeByRank(lo + 1)
	for i := lo; i < hi && x != nil; i++ {
		out = append(out, Entry{Member: x.member, Score: x.score})
		x = x.levels[0].forward
	}
	return out
}

// Count returns how many members have a score in r, in O(log n).
func (z *ZSet) Count(r ScoreRange) int {
	first, fr := z.sl.firstInRange(r)
	if first == nil {
		return 0
	}
	_, lr := z.sl.lastInRange(r)
	return lr - fr + 1
}

// RangeByScore returns the entries whose score lies in r, ascending or, with
// rev, descending. The first offset matches (in that direction) are skipped
// and at most limit are returned; a negative limit means no limit, a negative
// offset yields nil. Cost is O(log n + offset + k).
func (z *ZSet) RangeByScore(r ScoreRange, rev bool, offset, limit int) []Entry {
	if offset < 0 {
		return nil
	}
	var x *node
	if rev {
		x, _ = z.sl.lastInRange(r)
	} else {
		x, _ = z.sl.firstInRange(r)
	}
	step := func(n *node) *node {
		if rev {
			return n.backward
		}
		return n.levels[0].forward
	}
	inRange := func(n *node) bool {
		if rev {
			return r.aboveMin(n.score)
		}
		return r.belowMax(n.score)
	}
	for ; x != nil && offset > 0 && inRange(x); offset-- {
		x = step(x)
	}
	var out []Entry
	for ; x != nil && inRange(x) && (limit < 0 || len(out) < limit); x = step(x) {
		out = append(out, Entry{Member: x.member, Score: x.score})
	}
	return out
}

// forget drops removed entries from the member map and returns them.
func (z *ZSet) forget(removed []Entry) []Entry {
	for _, e := range removed {
		delete(z.dict, e.Member)
	}
	z.compact()
	return removed
}

// RemoveRangeByRank deletes the entries at ascending ranks [lo, hi), clamped
// to the set, and returns how many were removed, in O(log n + k).
func (z *ZSet) RemoveRangeByRank(lo, hi int) int {
	lo, hi = max(lo, 0), min(hi, z.Len())
	if lo >= hi {
		return 0
	}
	count := hi - lo
	removed := z.sl.removeRun(z.sl.nodeByRank(lo+1), func(_ *node, i int) bool { return i < count })
	return len(z.forget(removed))
}

// RemoveRangeByScore deletes the entries whose score lies in r and returns
// how many were removed, in O(log n + k).
func (z *ZSet) RemoveRangeByScore(r ScoreRange) int {
	first, _ := z.sl.firstInRange(r)
	removed := z.sl.removeRun(first, func(n *node, _ int) bool { return r.belowMax(n.score) })
	return len(z.forget(removed))
}

// PopMin removes and returns up to k entries with the lowest scores, lowest
// first, in O(log n + k).
func (z *ZSet) PopMin(k int) []Entry {
	k = min(max(k, 0), z.Len())
	if k == 0 {
		return nil
	}
	removed := z.sl.removeRun(z.sl.header.levels[0].forward, func(_ *node, i int) bool { return i < k })
	return z.forget(removed)
}

// PopMax removes and returns up to k entries with the highest scores, highest
// first, in O(log n + k).
func (z *ZSet) PopMax(k int) []Entry {
	n := z.Len()
	k = min(max(k, 0), n)
	if k == 0 {
		return nil
	}
	removed := z.sl.removeRun(z.sl.nodeByRank(n-k+1), func(_ *node, i int) bool { return i < k })
	out := z.forget(removed)
	slices.Reverse(out)
	return out
}

// compact gives memory back after mass removal: Go maps never shrink, so the
// member map is rebuilt once the set falls to a quarter of its high-water
// mark. Skip-list nodes are individually allocated and need no compaction.
func (z *ZSet) compact() {
	if z.peak <= shrinkMinPeak || len(z.dict) > z.peak/4 {
		return
	}
	m := make(map[string]float64, 2*len(z.dict))
	maps.Copy(m, z.dict)
	z.dict, z.peak = m, len(m)
}
