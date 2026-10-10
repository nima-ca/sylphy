// Package hash implements the Redis hash value: a map from field names to
// values, both binary-safe.
//
// # Representation
//
// A Hash keeps its entries in an insertion-ordered slice plus a map from field
// to slice position. Deleting swaps the last entry into the hole, so every
// operation is O(1) and iteration order is deterministic: insertion order,
// disturbed only by deletions. Redis promises no order, but HKEYS, HVALS and
// HGETALL on an unchanged hash always agree with each other here, which a bare
// Go map (random iteration order on every range) could not offer.
//
// # Ownership
//
// Fields and values are stored as strings. A string conversion copies the
// bytes in once, and because strings are immutable the store's closure rules
// (never retain what you are handed) are satisfied by construction: results
// returned from this package may be used after the shard lock is released.
package hash

import "github.com/nima-ca/sylphy/internal/store"

// shrinkMinCap is the entry-slice capacity below which no shrinking is done.
const shrinkMinCap = 64

type entry struct{ field, value string }

// Hash is an insertion-ordered field/value table. The zero value is an empty
// hash ready to use. It is not safe for concurrent use: the store serializes
// access through the shard lock.
//
// Invariants: len(ents) == len(pos), and pos[ents[i].field] == i for every i.
type Hash struct {
	ents []entry
	pos  map[string]int
}

var _ store.Collection = (*Hash)(nil)

// New returns an empty hash.
func New() *Hash { return &Hash{pos: make(map[string]int)} }

// Kind implements store.Value.
func (*Hash) Kind() store.Kind { return store.KindHash }

// Len implements store.Collection: the number of fields.
func (h *Hash) Len() int { return len(h.ents) }

// Set stores value under field and reports whether the field is new. An
// existing field keeps its position.
func (h *Hash) Set(field, value string) bool {
	if i, ok := h.pos[field]; ok {
		h.ents[i].value = value
		return false
	}
	if h.pos == nil {
		h.pos = make(map[string]int)
	}
	h.pos[field] = len(h.ents)
	h.ents = append(h.ents, entry{field, value})
	return true
}

// SetNX stores value under field only if the field does not exist yet.
func (h *Hash) SetNX(field, value string) bool {
	if _, ok := h.pos[field]; ok {
		return false
	}
	return h.Set(field, value)
}

// Get returns the value of field.
func (h *Hash) Get(field string) (string, bool) {
	i, ok := h.pos[field]
	if !ok {
		return "", false
	}
	return h.ents[i].value, true
}

// Has reports whether field exists.
func (h *Hash) Has(field string) bool {
	_, ok := h.pos[field]
	return ok
}

// Delete removes field and reports whether it existed. The last entry moves
// into the freed position.
func (h *Hash) Delete(field string) bool {
	i, ok := h.pos[field]
	if !ok {
		return false
	}
	last := len(h.ents) - 1
	if i != last {
		h.ents[i] = h.ents[last]
		h.pos[h.ents[i].field] = i
	}
	h.ents[last] = entry{} // drop the string references
	h.ents = h.ents[:last]
	delete(h.pos, field)
	h.compact()
	return true
}

// compact gives memory back after mass deletion: Go maps never shrink, so the
// slice and the map are rebuilt once the hash is a quarter full.
func (h *Hash) compact() {
	if cap(h.ents) <= shrinkMinCap || len(h.ents) > cap(h.ents)/4 {
		return
	}
	ents := make([]entry, len(h.ents), 2*len(h.ents))
	copy(ents, h.ents)
	pos := make(map[string]int, len(ents))
	for i, e := range ents {
		pos[e.field] = i
	}
	h.ents, h.pos = ents, pos
}

// Fields returns the field names in iteration order.
func (h *Hash) Fields() []string {
	out := make([]string, len(h.ents))
	for i, e := range h.ents {
		out[i] = e.field
	}
	return out
}

// Values returns the values in the same order as Fields.
func (h *Hash) Values() []string {
	out := make([]string, len(h.ents))
	for i, e := range h.ents {
		out[i] = e.value
	}
	return out
}

// Flat returns field, value, field, value, ... in iteration order (the shape
// of an HGETALL reply).
func (h *Hash) Flat() []string {
	out := make([]string, 0, 2*len(h.ents))
	for _, e := range h.ents {
		out = append(out, e.field, e.value)
	}
	return out
}
