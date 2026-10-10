// Package store implements Sylphy's in-memory keyspace: a sharded map of typed
// values with optional per-key expiry.
//
// # Values
//
// A Value is anything with a Kind: the built-in String, or a collection type
// defined in another package (list, hash, set, zset) that imports this one. A
// Collection additionally reports its length, and the store deletes a key whose
// collection becomes empty, TTL included, so empty collections are never
// visible. Collections are reachable only through closures that run under the
// shard lock (View, Mutate, MutateEntry, Atomic, AtomicView); the string
// helpers (Get, Set, Update) copy on the way in and out.
//
// # Concurrency model
//
// Keys are spread over N shards, each guarded by its own sync.RWMutex.
// Operations on a single key are atomic. Atomic and AtomicView lock every shard
// they need in ascending shard-index order, which makes multi-key operations
// atomic and deadlock-free. The convenience multi-key helpers (Delete, Exists
// with several keys) are atomic per key only. Lock-ordering rule: any code that
// needs more than one shard lock must take them in ascending index order, each
// shard once; everything else (the sweeper, Scan, RandomKey, Keys, Flush) holds
// one lock at a time. Closures must not block, must not call back into the
// Store, and must not retain the values they are given.
//
// # Expiry
//
// Each shard keeps an index of key deadlines (absolute Unix milliseconds). A
// key found past its deadline is treated as absent and removed on access (lazy
// expiry); a background sweeper started with Start samples the index and
// removes expired keys that nobody reads. Time comes from an injectable Clock,
// so tests never sleep.
//
// # Scanning
//
// Each shard also indexes its keys by slot so that Scan can resume from a
// cursor in O(count); see Scan for the guarantee it gives.
package store
