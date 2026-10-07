# Design decisions

Each entry: **Context → Decision → Consequences**.

## ADR-001: RESP2 first

**Context.** Clients must connect without custom drivers. RESP3 adds many types
and a `HELLO` handshake.
**Decision.** Implement RESP2 only. The parser handles all RESP2 types (needed by
tests and future replication); the server replies with `ERR unknown command` to
`HELLO`, which go-redis tolerates and falls back from.
**Consequences.** Every mainstream client works. RESP3 features (maps, push) are
out of scope; adding them later means a protocol version on the connection state.

## ADR-002: Sharded map instead of `sync.Map`

**Context.** The store needs concurrent reads and writes on arbitrary keys plus
atomic read-modify-write.
**Decision.** N shards (default 32, power of two), each a plain `map` behind its
own `RWMutex`.
**Consequences.** `sync.Map` is optimized for write-once/read-many and disjoint
key sets and has no atomic read-modify-write; sharding gives that via the shard
lock and keeps contention proportional to 1/N. Cost: operations that span
shards (`Len`, `Keys`, `Flush`) are not point-in-time snapshots.

## ADR-003: `hash/maphash` with a per-store seed

**Context.** Shard choice must be fast, well-distributed and resistant to
adversarial keys.
**Decision.** `maphash.String(seed, key) & (N-1)` with a random seed per store.
**Consequences.** Hardware-accelerated and allocation-free; the random seed
prevents clients from crafting keys that all land in one shard. Shard placement
is not stable across restarts, which is fine for an in-memory store.
(FNV-1a would be deterministic but trivially floodable.)

## ADR-004: Copy on read and on write

**Context.** Aliasing a retained `[]byte` between connections and the store would
let one client's buffer mutate another's data.
**Decision.** `Set` copies before locking; `Get` copies under the read lock;
`Update` hands the callback a private copy and adopts its returned slice.
**Consequences.** One extra allocation+copy per call, in exchange for a store
that cannot be corrupted through retained slices. `STRLEN` pays this copy too.

## ADR-005: Per-key atomicity for multi-key commands

**Context.** `MSET`/`MGET`/`DEL`/`EXISTS` touch several keys, possibly on
different shards.
**Decision.** Each key is atomic; the command as a whole is not.
**Consequences.** No cross-shard locking, so no lock-ordering hazards and no
global bottleneck. A concurrent reader can observe a half-applied `MSET`.
Redis is atomic here because it is single-threaded; if Sylphy ever needs it, a
sorted multi-shard lock acquisition can be added behind the same API.

## ADR-006: Goroutine per connection

**Context.** Need simple, correct handling of many concurrent clients with
blocking I/O.
**Decision.** One goroutine per connection owning a buffered reader and writer;
bounded by `MaxClients`.
**Consequences.** Straightforward code and natural backpressure; memory scales
with clients (~16 KiB of buffers each). An epoll-style event loop could be faster
at extreme connection counts but is far more complex.

## ADR-007: Flush when the read buffer is empty

**Context.** Flushing after every command defeats pipelining.
**Decision.** Replies accumulate in the `Writer`; flush when `Reader.Buffered()==0`.
**Consequences.** One write syscall per pipelined batch. A batch ending in a
partial command delays earlier replies until the rest arrives.

## ADR-008: Configuration precedence is flag > env > default

**Context.** The spec asked for "flags with environment overrides".
**Decision.** Environment variables set flag defaults; explicit flags win.
**Consequences.** Matches 12-factor/Docker usage (env baseline, flag tweak). Easy
to invert if "env always wins" is preferred.

## ADR-009: Shutdown by read deadline

**Context.** Idle connections block in `Read`; in-flight commands must finish.
**Decision.** Draining sets a read deadline in the past under a per-connection
mutex; the loop flushes pending replies and exits. Force-close after the grace
period.
**Consequences.** No per-connection "busy" tracking. A client mid-request when
shutdown starts is dropped (its command never fully arrived).
