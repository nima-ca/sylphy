// Package list implements the list value behind LPUSH, RPOP, LRANGE and
// friends: a double-ended sequence of binary-safe byte strings.
//
// # Representation
//
// A List is a ring buffer (circular deque): one slice whose length is always a
// power of two, a head index and an element count. Pushing or popping at
// either end is O(1) amortized, and indexing is O(1) because the physical slot
// of element i is (head+i) & (len-1).
//
// A linked list was rejected on purpose. It spends a heap node, two pointers
// and a slice header per element, hands the garbage collector one object per
// element to trace, and makes LINDEX, LSET, LRANGE and LTRIM (which all start
// from an index) walk cold cache lines. Its one advantage, O(1) insertion in
// the middle, does not help here: LINSERT must first find its pivot, which is
// O(n) anyway, and shifting pointers inside one slice is cheaper than chasing
// them. The price of the ring is that growing copies the whole buffer (so one
// push can be O(n), amortized O(1)) and that LINSERT and LREM shift up to n/2
// slots. The buffer halves when it falls to a quarter full, so a list that
// shrinks gives its memory back, and doubling and halving use different
// thresholds so a list hovering at a boundary does not thrash.
//
// # Ownership
//
// Elements are copied in (Push*, Insert, Set) and copied out (Range). Pop*
// hand over the stored slice itself: the list drops its reference, so the
// caller owns it. At returns the internal slice without copying and is valid
// only while the caller holds the shard lock, like store.String.Bytes.
//
// # Blocking commands
//
// A List knows nothing about waiters. A future BLPOP can be a handler-level
// loop around the same Mutate-based pop used by LPOP plus a wake-up registry
// notified after a push, so nothing in this package has to change.
package list

import (
	"bytes"
	"math"

	"github.com/nima-ca/sylphy/internal/store"
)

// minCap is the smallest buffer a non-empty list allocates.
const minCap = 8

// List is a ring-buffer deque of byte strings. The zero value is an empty list
// ready to use. It is not safe for concurrent use: the store serializes access
// through the shard lock.
//
// Invariants: len(buf) is 0 or a power of two >= minCap; the live elements are
// the n slots starting at head, wrapping; every other slot is nil, so popped
// elements are never kept alive; and len(buf) == minCap or n > len(buf)/4.
type List struct {
	buf  [][]byte
	head int
	n    int
}

var _ store.Collection = (*List)(nil)

// New returns an empty list.
func New() *List { return &List{} }

// Kind implements store.Value.
func (*List) Kind() store.Kind { return store.KindList }

// Len implements store.Collection: the number of elements.
func (l *List) Len() int { return l.n }

func clone(b []byte) []byte {
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// slot maps logical index i to a physical slot. It must only be called when
// the buffer is allocated.
func (l *List) slot(i int) int { return (l.head + i) & (len(l.buf) - 1) }

// resize moves the elements, in order, to a fresh buffer of newCap slots.
func (l *List) resize(newCap int) {
	nb := make([][]byte, newCap)
	if l.n > 0 {
		first := min(l.n, len(l.buf)-l.head)
		copy(nb, l.buf[l.head:l.head+first])
		copy(nb[first:], l.buf[:l.n-first])
	}
	l.buf, l.head = nb, 0
}

// reserve makes room for one more element.
func (l *List) reserve() {
	if l.n == len(l.buf) {
		l.resize(max(minCap, 2*len(l.buf)))
	}
}

// compact releases memory after removals: an empty list drops its buffer, and
// a sparse one halves it until it is more than a quarter full.
func (l *List) compact() {
	if l.n == 0 {
		l.buf, l.head = nil, 0
		return
	}
	target := len(l.buf)
	for target > minCap && l.n <= target/4 {
		target /= 2
	}
	if target != len(l.buf) {
		l.resize(target)
	}
}

// PushFront inserts a copy of b before the first element.
func (l *List) PushFront(b []byte) {
	l.reserve()
	l.head = (l.head - 1) & (len(l.buf) - 1)
	l.buf[l.head] = clone(b)
	l.n++
}

// PushBack appends a copy of b.
func (l *List) PushBack(b []byte) {
	l.reserve()
	l.buf[l.slot(l.n)] = clone(b)
	l.n++
}

// popFront and popBack remove one element; the list must be non-empty. They do
// not compact, so bulk callers compact once at the end.
func (l *List) popFront() []byte {
	v := l.buf[l.head]
	l.buf[l.head] = nil
	l.head = (l.head + 1) & (len(l.buf) - 1)
	l.n--
	return v
}

func (l *List) popBack() []byte {
	s := l.slot(l.n - 1)
	v := l.buf[s]
	l.buf[s] = nil
	l.n--
	return v
}

// PopFront removes and returns the first element. The caller owns the result.
func (l *List) PopFront() ([]byte, bool) {
	if l.n == 0 {
		return nil, false
	}
	v := l.popFront()
	l.compact()
	return v, true
}

// PopBack removes and returns the last element. The caller owns the result.
func (l *List) PopBack() ([]byte, bool) {
	if l.n == 0 {
		return nil, false
	}
	v := l.popBack()
	l.compact()
	return v, true
}

// PopFrontN removes up to k elements from the front and returns them in pop
// order (first element first). k is clamped to [0, Len]; the allocation is
// bounded by Len, never by k.
func (l *List) PopFrontN(k int) [][]byte {
	k = min(max(k, 0), l.n)
	out := make([][]byte, k)
	for i := range out {
		out[i] = l.popFront()
	}
	l.compact()
	return out
}

// PopBackN removes up to k elements from the back and returns them in pop
// order (last element first), like repeated RPOP.
func (l *List) PopBackN(k int) [][]byte {
	k = min(max(k, 0), l.n)
	out := make([][]byte, k)
	for i := range out {
		out[i] = l.popBack()
	}
	l.compact()
	return out
}

// At returns the element at index i without copying; ok is false when i is
// outside [0, Len). The slice is valid only under the shard lock and must not
// be modified or retained.
func (l *List) At(i int) (b []byte, ok bool) {
	if i < 0 || i >= l.n {
		return nil, false
	}
	return l.buf[l.slot(i)], true
}

// Set replaces the element at index i with a copy of b and reports whether i
// was in range.
func (l *List) Set(i int, b []byte) bool {
	if i < 0 || i >= l.n {
		return false
	}
	l.buf[l.slot(i)] = clone(b)
	return true
}

// Range returns copies of the elements in [lo, hi), clamped to the list. All
// copies share one backing array (a single allocation), with capacities
// limited so appending to one element cannot overwrite its neighbor. It
// returns nil for an empty range.
func (l *List) Range(lo, hi int) [][]byte {
	lo = max(lo, 0)
	hi = min(hi, l.n)
	if lo >= hi {
		return nil
	}
	total := 0
	for i := lo; i < hi; i++ {
		total += len(l.buf[l.slot(i)])
	}
	arena := make([]byte, total)
	out := make([][]byte, hi-lo)
	off := 0
	for i := range out {
		e := l.buf[l.slot(lo+i)]
		end := off + len(e)
		copy(arena[off:end], e)
		out[i] = arena[off:end:end]
		off = end
	}
	return out
}

// IndexOf returns the index of the first element equal to b, or -1.
func (l *List) IndexOf(b []byte) int {
	for i := 0; i < l.n; i++ {
		if bytes.Equal(l.buf[l.slot(i)], b) {
			return i
		}
	}
	return -1
}

// Insert places a copy of b so that it ends up at index i, shifting the
// shorter side of the list, so the cost is O(min(i, Len-i)). It reports false,
// changing nothing, unless 0 <= i <= Len.
func (l *List) Insert(i int, b []byte) bool {
	if i < 0 || i > l.n {
		return false
	}
	l.reserve()
	if i < l.n/2 {
		// Open a gap at the front: move the head back and slide the first i
		// elements one place toward it.
		l.head = (l.head - 1) & (len(l.buf) - 1)
		for j := 0; j < i; j++ {
			l.buf[l.slot(j)] = l.buf[l.slot(j+1)]
		}
	} else {
		for j := l.n; j > i; j-- {
			l.buf[l.slot(j)] = l.buf[l.slot(j-1)]
		}
	}
	l.buf[l.slot(i)] = clone(b)
	l.n++
	return true
}

// Remove deletes elements equal to target and returns how many it removed,
// with LREM's count semantics: count > 0 removes the first count matches
// scanning from the head, count < 0 the first -count matches scanning from the
// tail, and count == 0 removes every match. It makes one O(n) compacting pass
// (not one shift per match).
func (l *List) Remove(count int, target []byte) int {
	if l.n == 0 {
		return 0
	}
	limit := count
	if count < 0 {
		limit = -count
		if limit < 0 { // -MinInt overflows
			limit = math.MaxInt
		}
	}
	if count == 0 {
		limit = math.MaxInt
	}
	removed := 0
	if count >= 0 {
		w := 0
		for r := 0; r < l.n; r++ {
			e := l.buf[l.slot(r)]
			if removed < limit && bytes.Equal(e, target) {
				removed++
				continue
			}
			if w != r {
				l.buf[l.slot(w)] = e
			}
			w++
		}
		for j := w; j < l.n; j++ {
			l.buf[l.slot(j)] = nil
		}
		l.n = w
	} else {
		w := l.n - 1
		for r := l.n - 1; r >= 0; r-- {
			e := l.buf[l.slot(r)]
			if removed < limit && bytes.Equal(e, target) {
				removed++
				continue
			}
			if w != r {
				l.buf[l.slot(w)] = e
			}
			w--
		}
		// Survivors occupy logical [w+1, n); drop the stale prefix.
		drop := w + 1
		for j := 0; j < drop; j++ {
			l.buf[l.slot(j)] = nil
		}
		l.head = l.slot(drop)
		l.n -= drop
	}
	l.compact()
	return removed
}

// Trim keeps only the elements in [lo, hi), clamped to the list, and empties
// the list when that range is empty.
func (l *List) Trim(lo, hi int) {
	lo = max(lo, 0)
	hi = min(hi, l.n)
	if lo >= hi {
		l.buf, l.head, l.n = nil, 0, 0
		return
	}
	for j := hi; j < l.n; j++ {
		l.buf[l.slot(j)] = nil
	}
	for j := 0; j < lo; j++ {
		l.buf[l.slot(j)] = nil
	}
	l.head = l.slot(lo)
	l.n = hi - lo
	l.compact()
}
