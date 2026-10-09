package command

import (
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
	"github.com/nima-ca/sylphy/internal/store"
)

// Store is the keyspace interface commands depend on. *store.Store implements
// it; tests drive a real small store with a fake clock, because store.Entry
// cannot be constructed outside its package.
//
// The closure-taking methods (View, Mutate, MutateEntry, Atomic, AtomicView,
// Update) run under shard locks. Closure rules: handlers must copy what they
// need out of the closure and write the reply afterwards, so a slow client can
// never stall a shard; a closure must not block, must not call back into the
// Store, and must not retain the Values or Entries it is given.
//
// Part 7 adds Scan and RandomKey here, once the store implements them.
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
	Delete(keys ...string) int
	Exists(keys ...string) int
	Len() int
	Flush()
	Keys(pattern string) []string
	Update(key string, fn func(old []byte, exists bool) ([]byte, error)) error

	// View gives fn the live value of key (nil if missing) under a read lock.
	// fn must not modify the value.
	View(key string, fn func(v store.Value) error) error
	// Mutate gives fn the live value of key (nil if missing) under the write
	// lock and stores what fn returns; nil or an empty Collection deletes the
	// key. The TTL is kept. fn must validate before modifying cur in place.
	Mutate(key string, fn func(cur store.Value) (store.Value, error)) error
	// MutateEntry gives fn a TTL-aware handle to key under the write lock.
	MutateEntry(key string, fn func(e *store.Entry) error) error
	// Atomic gives fn write access to several keys at once. Shards are locked
	// in ascending index order; entries[i] belongs to keys[i], and repeated
	// keys share one Entry.
	Atomic(keys []string, fn func(entries []*store.Entry) error) error
	// AtomicView is the read-only form of Atomic: values[i] is the live value
	// of keys[i], nil if missing.
	AtomicView(keys []string, fn func(values []store.Value) error) error
	// ExpireAt returns the key's deadline in Unix ms, -2 if missing, -1 if none.
	ExpireAt(key string) int64
	// NowMs returns the store clock in Unix ms.
	NowMs() int64
}

var _ Store = (*store.Store)(nil)

// ConnState is per-connection state visible to handlers. Phase 4 adds fields
// here (transaction queue, subscriptions); existing handlers keep compiling
// because they only ever see *Context.
type ConnState struct {
	// ID is a process-unique connection identifier.
	ID uint64
	// Name is the value set by CLIENT SETNAME.
	Name string
	// Closing asks the server to close the connection after flushing replies
	// (set by QUIT).
	Closing bool
}

// Context is everything a handler may touch. One Context is created per
// connection and reused for each command on it.
type Context struct {
	// Store is the shared keyspace.
	Store Store
	// W is the connection's reply writer.
	W *protocol.Writer
	// Config is the read-only server configuration.
	Config *config.Config
	// Conn is the per-connection state.
	Conn *ConnState
}
