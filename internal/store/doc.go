// Package store implements Sylphy's in-memory keyspace: a sharded map of
// binary-safe string values.
//
// Concurrency model: keys are spread over N shards, each guarded by its own
// sync.RWMutex. Operations on a single key are atomic. Multi-key operations
// (Delete, Exists with several keys, and the MSET/MGET commands built on top)
// are atomic per key only, never across keys. Lock-ordering rule: no method
// ever holds more than one shard lock at a time, so deadlock between shards is
// impossible by construction.
package store
