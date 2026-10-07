package command

import (
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
)

// Store is the keyspace interface commands depend on. *store.Store implements
// it; tests use an in-memory fake.
type Store interface {
	Get(key string) ([]byte, bool)
	Set(key string, value []byte)
	Delete(keys ...string) int
	Exists(keys ...string) int
	Len() int
	Flush()
	Keys(pattern string) []string
	Update(key string, fn func(old []byte, exists bool) ([]byte, error)) error
}

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
