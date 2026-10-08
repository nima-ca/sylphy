package store

import "errors"

// Kind identifies the type of a stored value.
type Kind uint8

// Value kinds. The zero Kind is deliberately invalid.
const (
	// KindString is a binary-safe string.
	KindString Kind = iota + 1
	// KindList is a list.
	KindList
	// KindHash is a hash (field to value map).
	KindHash
	// KindSet is an unordered set of members.
	KindSet
	// KindZSet is a sorted set.
	KindZSet
)

// String returns the Redis type name reported by the TYPE command.
func (k Kind) String() string {
	switch k {
	case KindString:
		return "string"
	case KindList:
		return "list"
	case KindHash:
		return "hash"
	case KindSet:
		return "set"
	case KindZSet:
		return "zset"
	default:
		return "none"
	}
}

// ErrWrongType is returned when an operation meets a key holding a different
// kind of value than it expects. Its text is the exact Redis error line, so
// the command layer can send it as-is.
var ErrWrongType = errors.New("WRONGTYPE Operation against a key holding the wrong kind of value")

// Value is anything the store can hold under a key. The store only needs the
// kind; each type's own methods are reached by a type assertion after the
// kind has been checked.
//
// Values handed to closures (View, Mutate, ...) are the store's live objects:
// they are valid only while the closure runs under the shard lock and must
// never be retained, returned, or sent to another goroutine.
type Value interface {
	// Kind reports the Redis type of the value.
	Kind() Kind
}

// Collection is a Value that can be empty. The store deletes a key (and its
// TTL) as soon as its Collection value reports Len() == 0, inside the same
// critical section that emptied it, so empty collections are never visible.
//
// Strings do not implement Collection: an empty string is a valid value.
type Collection interface {
	Value
	// Len returns the number of elements.
	Len() int
}

// String is the Value for KindString.
type String struct{ b []byte }

// NewString returns a String holding a copy of b.
func NewString(b []byte) *String { return &String{b: clone(b)} }

// Kind implements Value.
func (*String) Kind() Kind { return KindString }

// Bytes returns the internal slice without copying. Inside a closure it is
// valid only for the closure's duration; copy it before retaining.
func (s *String) Bytes() []byte { return s.b }
