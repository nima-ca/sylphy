// Package set implements the Redis set value: an unordered collection of
// distinct, binary-safe members.
//
// # Representation
//
// A Set keeps its members in a slice plus a map from member to slice
// position. Adding appends, removing swaps the last member into the hole, so
// both are O(1), and any member can be fetched by index. That index access is
// the point: SPOP and SRANDMEMBER need a uniformly random member in O(1),
// which a bare Go map cannot give (map iteration is not uniform and costs
// O(n) to reach a random position). Iteration order is insertion order,
// disturbed only by removals.
//
// # Randomness
//
// Methods that pick random members take an intN function (n > 0 returns a
// value in [0, n)) instead of reading a global generator, so callers and tests
// control the source. *rand.Rand's IntN method fits.
//
// # Ownership
//
// Members are strings: copied in once, immutable afterwards, so results may be
// used after the shard lock is released.
package set

import (
	"slices"

	"github.com/nima-ca/sylphy/internal/store"
)

// shrinkMinCap is the member-slice capacity below which no shrinking is done.
const shrinkMinCap = 64

// Set is an indexable set of strings. The zero value is an empty set ready to
// use. It is not safe for concurrent use: the store serializes access through
// the shard lock.
//
// Invariants: len(members) == len(pos), and pos[members[i]] == i for every i.
type Set struct {
	members []string
	pos     map[string]int
}

var _ store.Collection = (*Set)(nil)

// New returns an empty set.
func New() *Set { return &Set{pos: make(map[string]int)} }

// Kind implements store.Value.
func (*Set) Kind() store.Kind { return store.KindSet }

// Len implements store.Collection: the number of members.
func (s *Set) Len() int { return len(s.members) }

// Add inserts m and reports whether it was new.
func (s *Set) Add(m string) bool {
	if _, ok := s.pos[m]; ok {
		return false
	}
	if s.pos == nil {
		s.pos = make(map[string]int)
	}
	s.pos[m] = len(s.members)
	s.members = append(s.members, m)
	return true
}

// Has reports whether m is a member.
func (s *Set) Has(m string) bool {
	_, ok := s.pos[m]
	return ok
}

// Remove deletes m and reports whether it was a member.
func (s *Set) Remove(m string) bool {
	i, ok := s.pos[m]
	if !ok {
		return false
	}
	s.removeAt(i)
	return true
}

// removeAt deletes the member at index i by moving the last member into it.
func (s *Set) removeAt(i int) {
	m := s.members[i]
	last := len(s.members) - 1
	if i != last {
		s.members[i] = s.members[last]
		s.pos[s.members[i]] = i
	}
	s.members[last] = "" // drop the string reference
	s.members = s.members[:last]
	delete(s.pos, m)
	s.compact()
}

// compact gives memory back after mass removal: Go maps never shrink, so the
// slice and the map are rebuilt once the set is a quarter full.
func (s *Set) compact() {
	if cap(s.members) <= shrinkMinCap || len(s.members) > cap(s.members)/4 {
		return
	}
	members := make([]string, len(s.members), 2*len(s.members))
	copy(members, s.members)
	pos := make(map[string]int, len(members))
	for i, m := range members {
		pos[m] = i
	}
	s.members, s.pos = members, pos
}

// At returns the member at index i in iteration order, or "" if i is out of
// range. Together with Len it lets a caller pick a member by random index.
func (s *Set) At(i int) string {
	if i < 0 || i >= len(s.members) {
		return ""
	}
	return s.members[i]
}

// Members returns a copy of the members in iteration order.
func (s *Set) Members() []string { return slices.Clone(s.members) }

// Range calls fn for each member in iteration order until fn returns false.
// fn must not modify the set.
func (s *Set) Range(fn func(m string) bool) {
	for _, m := range s.members {
		if !fn(m) {
			return
		}
	}
}

// PopRandom removes and returns a uniformly random member.
func (s *Set) PopRandom(intN func(n int) int) (string, bool) {
	n := len(s.members)
	if n == 0 {
		return "", false
	}
	i := intN(n)
	m := s.members[i]
	s.removeAt(i)
	return m, true
}

// PopN removes and returns up to k distinct random members. When k >= Len the
// whole set is handed over and emptied.
func (s *Set) PopN(k int, intN func(n int) int) []string {
	n := len(s.members)
	if k <= 0 || n == 0 {
		return nil
	}
	if k >= n {
		out := s.members // ownership moves to the caller; the set forgets it
		s.members, s.pos = nil, nil
		return out
	}
	out := s.Sample(k, intN)
	for _, m := range out {
		s.Remove(m)
	}
	return out
}

// Sample returns min(k, Len) distinct random members without modifying the
// set. For k >= Len it returns every member in iteration order. Otherwise it
// runs a partial Fisher-Yates shuffle over a sparse "swapped" map, so the cost
// is O(k) time and space no matter how large the set is.
func (s *Set) Sample(k int, intN func(n int) int) []string {
	n := len(s.members)
	if k <= 0 || n == 0 {
		return nil
	}
	if k >= n {
		return s.Members()
	}
	swapped := make(map[int]int, k)
	at := func(i int) int {
		if v, ok := swapped[i]; ok {
			return v
		}
		return i
	}
	out := make([]string, k)
	for i := 0; i < k; i++ {
		j := i + intN(n-i)
		vi, vj := at(i), at(j)
		swapped[j] = vi // slot j now holds what slot i held
		out[i] = s.members[vj]
	}
	return out
}

// SampleRepeat returns k random members chosen independently, so the same
// member can appear several times (SRANDMEMBER with a negative count). It
// returns nil for an empty set or k <= 0.
func (s *Set) SampleRepeat(k int, intN func(n int) int) []string {
	n := len(s.members)
	if k <= 0 || n == 0 {
		return nil
	}
	out := make([]string, k)
	for i := range out {
		out[i] = s.members[intN(n)]
	}
	return out
}
