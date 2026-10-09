package command

import (
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
	"github.com/nima-ca/sylphy/internal/store"
)

// Store is the keyspace interface commands depend on. *store.Store implements
// it; tests drive a real single-shard store with a fake clock, because
// store.Entry cannot be constructed outside its package.
//
// The closure-taking methods (View, MutateEntry, Update) run under a shard
// lock: handlers must copy what they need out of the closure and write the
// reply afterwards, so a slow client can never stall a shard.
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
	View(key string, fn func(v store.Value) error) error
	// MutateEntry gives fn a TTL-aware handle to key under the write lock.
	MutateEntry(key string, fn func(e *store.Entry) error) error
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
