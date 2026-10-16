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

**Status.** Still in force for `MSET`, `MGET`, `DEL` and `EXISTS`. Ordered
multi-shard locking now exists for the commands that need it; see ADR-015.

**Context.** `MSET`/`MGET`/`DEL`/`EXISTS` touch several keys, possibly on
different shards.
**Decision.** Each key is atomic; the command as a whole is not.
**Consequences.** No cross-shard locking for these commands, so no global
bottleneck. A concurrent reader can observe a half-applied `MSET`. Redis is
atomic here because it is single-threaded; moving these commands onto
`Store.Atomic` is a small change if it is ever wanted.

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

## ADR-010: Typed values and closure-based access

**Context.** Phase 2 adds lists, hashes, sets and sorted sets. Handing a
collection pointer out of the store would let a handler mutate it outside the
shard lock, and one `[]byte` per key no longer fits.
**Decision.** The store maps keys to `Value` (an interface with `Kind()`);
`Collection` adds `Len()`. Concrete collections live in their own packages that
import `store`, so the store has no dependency on them. Access is by closure:
`View` (read lock), `Mutate`, `MutateEntry` (adds TTL access), and
`Atomic`/`AtomicView` for several keys. Closures run under the shard lock and
must not block, re-enter the store or retain what they are given; handlers copy
results out and write the reply afterwards. A `nil` result or an empty
`Collection` deletes the key and its TTL, so empty collections are never
visible. A mismatched kind returns `ErrWrongType`.
**Consequences.** Locking is impossible to forget and aliasing is impossible by
construction. Cost: a closure per operation, and the discipline of the closure
rules (a nil `*T` wrapped in a `Value` is not `nil`, so handlers return an
untyped `nil`). The Phase 1 string helpers stay for the string commands.

## ADR-011: Expiry: lazy checks plus sampled active sweeping

**Context.** Keys with a TTL must never be visible after their deadline, and
keys nobody reads must still be reclaimed.
**Decision.** Two mechanisms, as in Redis. Lazy: every access checks the
deadline and reaps an expired key. Active: a sweeper goroutine samples each
shard's expiry index (a `key → deadline` map kept in step with the value map),
deletes the expired keys, and re-samples a shard while more than 25% of the
sample was expired, within a 25 ms real-time budget per 100 ms tick. A timer
heap or wheel was rejected: it needs an entry per TTL key to be updated on every
overwrite, rename and delete, costs O(log n) per TTL write, and still needs
locking against the shards.
**Consequences.** TTL writes are O(1). Reclamation is probabilistic and bounded
by sweeper throughput rather than immediate, so memory for expired-but-unread
keys is held for a short while; correctness does not depend on it, because reads
check the deadline. The sweeper is a goroutine with an owner (`Store.Start` /
`Close`).

## ADR-012: Injectable clock

**Context.** Expiry tests must be deterministic and fast; sleeping is neither.
**Decision.** The store takes a `Clock` (`NowMs() int64`): `SystemClock` in
production, `FakeClock` (with `Advance`) in tests. Each lock acquisition reads
the clock once (`Entry.Now`) so all decisions inside one critical section agree.
Handlers compute deadlines from `Store.NowMs()` and the store applies them
against its own reading; a deadline at or before "now" deletes the key. The
sweeper's per-cycle budget uses the real wall clock (it bounds CPU time, not
logical time), and tests drive cycles through an injected tick channel.
**Consequences.** Expiry tests never sleep. Two clocks exist (logical for keys,
wall for the sweeper budget), which is deliberate. Integration tests run a real
server with a `FakeClock` and wait on the real sweeper ticker with bounded
polling.

## ADR-013: Ring buffer lists

**Context.** Lists need O(1) push and pop at both ends and fast index access.
**Decision.** A power-of-two ring buffer with a head index and count; slot of
element `i` is `(head+i) & (cap-1)`. The buffer doubles when full and halves at a
quarter full (different thresholds, so a list at a boundary does not thrash).
A linked list was rejected: a node per element is heavy for the garbage collector
and cache-hostile, and its O(1) middle insertion does not help `LINSERT`, which
must find its pivot in O(n) anyway.
**Consequences.** `LINDEX`/`LSET` are O(1) (Redis is O(n)). A push that grows the
buffer copies it (amortized O(1)); `LINSERT` and `LREM` shift up to n/2 slots.
Popped slots are cleared so they do not pin memory. A blocking `BLPOP` can be
added at the handler level without changing the type.

## ADR-014: Skip list with spans for sorted sets

**Context.** Sorted sets need ordered iteration, O(log n) insert, remove and
rank, range by rank and by score, and O(1) score lookup.
**Decision.** A skip list whose links carry spans (the number of level-0 steps
jumped), plus a member-to-score map. Order is score, then member bytes.
Heights use probability 1/4 per level, capped at 32, drawn from an injected
`intN` so tests are deterministic. A balanced tree would need subtree sizes for
the same rank queries, and a sorted slice makes insertion O(n).
**Consequences.** Rank is the sum of spans along a search path and a node is
reachable by rank in O(log n). Nodes are individually allocated (more pointers
than a tree of slices, but simple removal of runs for `ZREMRANGEBY*`/`ZPOP*`).
The member map is rebuilt after mass removal because Go maps never shrink.
Correctness is guarded by an invariant checker and a seeded model-based test
against a naive sorted slice, including degenerate height sources.

## ADR-015: Ascending shard lock ordering

**Context.** `SMOVE`, `SUNIONSTORE`/`SINTERSTORE`/`SDIFFSTORE` and `RENAME` must
change several keys atomically; Phase 1 held at most one shard lock, and a
second lock invites deadlock.
**Decision.** Any operation that needs several shards locks them in ascending
shard-index order, each shard once (`Store.lockOrder`), and releases them in
reverse. `Atomic` and `AtomicView` are the only entry points; repeated keys share
one `Entry`. Everything else (single-key operations, the sweeper, `Scan`,
`RandomKey`, `Keys`, `Flush`) holds one lock at a time and so cannot take part in
a cycle. Closures may not call back into the store.
**Consequences.** With one global order there is no cycle of waiters, so no
deadlock; a hook (`afterLock`) lets tests yield between acquisitions to widen the
window and run many overlapping multi-key operations under `-race`. This replaces
the Phase 1 rule "no operation holds two shard locks". Cost: a multi-key command
blocks every involved shard for its duration.

## ADR-016: SCAN cursor over shards and key slots

**Context.** `SCAN` must resume statelessly (a server-side cursor table leaks),
cost O(`COUNT`) per call rather than O(keys), and return every key that exists
for the whole iteration at least once while writers run. Go maps have no stable
iteration position; Redis's reverse-binary cursor assumes its own hash table.
**Decision.** Each shard keeps its keys in an array (a slot per key; a new key
takes the next slot, a deleted key's slot is refilled by moving the last key
into it) next to the map. A cursor is `shard<<40 | n`, where `n` is the number
of unvisited slots in that shard (0 = not started); 0 overall means start and
end. A call walks shards in ascending order and each shard's slots from high to
low, examining `COUNT` slots, and returns the next cursor. Keys never change
shard, and within a shard a key's slot never increases, so a key still waiting
to be visited stays inside the unvisited range however the shard changes; a key
moved from the visited part into it is merely seen twice. A cursor naming a
shard that does not exist is `ERR invalid cursor`; a well-formed but stale cursor
is clamped to the shard's current length. `MATCH` and `TYPE` filter after the
shard lock is released.
**Consequences.** Cost per call is O(`COUNT` + shards), independent of keyspace
size; a full iteration is O(keys + calls × shards). Duplicates are possible, keys
created during the iteration may or may not appear, expired keys are skipped,
and a filtered batch can be empty with a non-zero cursor, all as in Redis.
Memory grows by a slot array and a position map per shard (roughly 40-50 bytes
per key). Cursors are opaque and not interchangeable with Redis's.

## ADR-017: DBSIZE accounting

**Context.** Redis's `DBSIZE` counts keys that have expired but not yet been
reclaimed. The project's rule is that expired keys are never visible.
**Decision.** `Len` (behind `DBSIZE`) counts live keys: the size of each shard's
map minus the keys in its expiry index whose deadline has passed. It takes each
shard's read lock in turn.
**Consequences.** `DBSIZE` agrees with `KEYS *`, `SCAN` and `EXISTS`. The cost is
proportional to the number of keys with a TTL (benchmarked in
[BENCHMARKS.md](BENCHMARKS.md)) instead of O(shards). It is a moving snapshot
across shards. A maintained counter would make it O(shards) but would have to
count expired keys, which was rejected.

## ADR-018: Rewrite hooks for a future append-only log

**Status.** Implemented in Phase 3: see ADR-020, ADR-022 and ADR-028.

**Context.** Phase 3 will log write commands and replay them. Replaying the
original argv is wrong when a command depends on the clock (`EXPIRE 10`), on
randomness (`SPOP`), or on state the replay may not see identically (`SET ... NX
GET`).
**Decision.** `Spec` has an optional `Rewrite func(argv, *Effects) [][][]byte`,
legal only on write commands. Handlers record what they actually did in a
per-connection `Effects` struct (applied deadline, whether a TTL was removed,
members popped); if `Context.Propagate` is set, `Dispatch` calls the hook after a
successful write and leaves the result in `Context.Rewritten`. Hooked commands:
the `EXPIRE` family and `PERSIST`, `SET`, `SETNX`, `SETEX`, `PSETEX`, `GETEX`,
`GETDEL` and `SPOP`: relative TTLs become `PEXPIREAT`/`PXAT` with the deadline
the store used, conditional and `GET` forms are reduced to their outcome, and
`SPOP` becomes `SREM` of the members actually removed. Write commands without a
hook are logged verbatim. A test requires every write command to be either hooked
or on an audited deterministic list, so a new write cannot reach the log
unreviewed. A replay test runs a mixed workload, replays only the rewritten
stream on a second server and compares state and TTLs.
**Consequences.** Nothing is allocated until a consumer sets `Propagate`. Hook
output never aliases the request buffers; verbatim entries do and must be used or
copied before the next read. Expiry-driven deletions are not write commands and
are not hooked; Phase 3 must log a `DEL` when lazy or active expiry removes a key,
and must define how log order matches store order across connections.

## ADR-019: One checkpoint procedure

**Context.** A point-in-time snapshot (`BGSAVE`) and a rewrite that shrinks the
log (`BGREWRITEAOF`) are usually separate procedures. Sylphy's log is a base
snapshot plus segments listed in a manifest, so writing a new base and starting
a fresh segment does both jobs.
**Decision.** One procedure, `Manager.checkpoint`, serves `SAVE`, `BGSAVE`,
`BGREWRITEAOF`, the automatic trigger, the shutdown checkpoint and the healing of
a failed log. Steps: barrier and log rotation under every shard lock; the base
is written outside the locks to a temporary file, fsynced, renamed and its
directory synced; the manifest is committed (its rename is the commit point);
the retired base and segments are deleted. Only one checkpoint runs at a time
(`ErrCheckpointRunning`). In `aof` mode a once-a-second trigger starts one when
the log has grown by `auto-rewrite-percentage` over the base and base plus log
has reached `auto-rewrite-min-size`, or when the log is in its error state; after
a failed checkpoint it waits ten ticks.
**Consequences.** `BGREWRITEAOF` is `BGSAVE` with different reply strings, and
`snapshot` mode refuses it. A checkpoint needs room for a complete new base next
to the old files. A crash at any step leaves a consistent directory: the old
manifest, an intermediate one that also lists the new segment, or the new one;
leftover files are unlisted and recovery deletes them. In `aof` mode the
manifest generation rises by two per checkpoint (segment listing, then commit).
A checkpoint is the only way out of the error latch (ADR-025). The log is never
shortened in place, so its growth is bounded only by the trigger.

## ADR-020: Logging under the shard lock through a journaling store

**Context.** Replay must reproduce the original order of writes to every key. If
a record were appended after the shard lock was released, another write to the
same key could be applied and logged in between.
**Decision.** In `aof` mode each connection's store is wrapped in a `journal`
that implements the same `Store` interface. Closure methods (`Mutate`,
`MutateEntry`, `Atomic`, `Update`) append the command's rewrite (or the request
verbatim) inside the closure, after the handler succeeded and recorded its
effects, so the shard lock(s) are still held. The primitives log fixed forms:
`Set` logs `SET key value`, `Delete` logs `DEL key` for each key that existed and
`Flush` logs `FLUSHALL`. Before a write handler runs, `begin` refuses it with
`MISCONF` if persistence has failed and calls `Admit` outside any lock.
`AOF.Append` assigns sequence numbers under the AOF mutex, never blocks and does
no I/O, which is what makes it legal under a shard lock (ADR-010's closure
rules).
**Consequences.** For every key, log order equals apply order. A multi-key
command holds all its shards (ADR-015) while its records are appended as one
contiguous group. Commands on different shards interleave in the file in the
order of their `Append` calls; they touch disjoint keys. `MSET` logs one `SET`
per pair and `DEL` one `DEL` per key that existed, matching their per-key
atomicity (ADR-005). The lock order becomes shard locks, then the AOF mutex. If
`Append` fails after the change was applied, the change cannot be undone; see
ADR-025.

## ADR-021: Shard-granular copy-on-write snapshots

**Context.** A checkpoint needs a consistent point-in-time copy of the whole
dataset without stopping writers for the duration of a disk write. Go has no
`fork`.
**Decision.** `Store.BeginSnapshot` locks every shard in ascending order, runs a
caller-supplied barrier, marks every shard pending and unlocks. Each shard is
then serialized exactly once, from its state at the barrier, under its own lock,
by whichever comes first: `Snapshot.Drain`, which walks the shards in order under
the read lock, or the first writer that reaches the shard (the gate in `lockW`,
which every write-lock acquisition must use; a test enforces it). A writer
locking several shards gates all of them first. The encoder runs under the lock,
must not block or retain what it is given, and copies into the block.
The barrier and the log rotation are split: the barrier only marks the
rotation boundary in the AOF's memory buffer (no I/O), because it runs under
every shard lock. The I/O (fsync the old segment, create and list the new one)
happens after unlock, when the checkpoint waits on the rotation. Everything
before the boundary is in the old segments, everything after it is in the new
segment, and the base is the state at the boundary.
**Consequences.** The pause is O(shards) plus the in-memory barrier. A writer
that reaches a pending shard pays for serializing that shard under its lock, and
blocks serialized ahead of the drain stay in memory until written, so memory can
approach the encoded size of the dataset. Only one snapshot exists at a time
(`ErrSnapshotActive`). Expired but unreaped keys are serialized as they are;
recovery decides what to do with them (ADR-022). A single entry too large for a
block (1 GiB) fails the checkpoint. `Snapshot.Close` uses a plain lock so that
ending the snapshot never serializes a shard.

## ADR-022: Frozen-clock replay and logging expiry as DEL

**Context.** ADR-018 left two problems for Phase 3: expiry removes keys without
any write command, and replaying a log after downtime must not depend on how long
the server was down.
**Decision.** Recovery runs before the listener and the sweeper start and calls
`Target.BeginReplay`, which freezes the store clock at 0: no deadline is in the
past, nothing expires, so the base and the segments replay exactly as they were
written. Segments replay through the normal dispatcher with a loader client (raw
store, no propagation); a command that replies with an error stops startup. In
`aof` mode a hook installed by `Manager.Start` logs `DEL key` inside the shard
lock for each key that expiry removes, so the `DEL` sits at the key's own place
in the log. Recovery does not purge expired keys. `Start` does, after the log is
open and the hook is installed, and in `aof` mode it fsyncs the logged `DEL`s
before it returns. In `snapshot` mode the purge is in memory.
**Consequences.** Keys that expired during downtime occupy memory until `Start`
runs. Logging the purge matters: without it, a later write to such a key would
replay on top of the stale copy still stored in the base. The sweeper starts
only after the purge, so it never appends before the hook exists. The hook
ignores `Append` errors; a `DEL` lost to a latched log is covered by the
checkpoint that heals it, whose base reflects memory.

## ADR-023: Snapshot, manifest and log formats

**Context.** The files must survive crashes and corruption, load into a store
with any shard count and be readable by existing tools where possible.
**Decision.** Three formats, all versioned. The snapshot is binary and
little-endian: a header, blocks of entries (each with its own CRC-32C) and an end
record carrying entry and block counts and a whole-file CRC. Nothing in it
depends on shard count or key order, and scores are stored as the IEEE-754 bits.
The manifest is text: magic and version, a generation, an optional base (name,
size, CRC) and the segments in replay order, ending in a CRC line; numbers are
canonical, so a decoded manifest re-encodes to the same bytes. Log segments are
plain RESP arrays of bulk strings, one command per record, with no checksum:
a stricter reader validates them by parsing (canonical decimals, no inline
commands, no null bulks, no empty arrays). Decoders never panic and never trust
a length field: memory follows the bytes actually present. A checksum is checked
before a version, so damage is `ErrChecksum` and only an intact file of another
version is `ErrVersion`.
**Consequences.** Snapshots load under any `-shards` and any hash seed (ADR-003).
Corruption inside a log bulk string that still parses goes unnoticed; only the
base and the manifest are checksummed. A zero-filled tail of the last segment is
corrupt, not truncated, so it is not repaired. Newer formats are refused and
there is no migration.

## ADR-024: Fsync policies, group commit and the flusher

**Context.** Acknowledging a write only after an fsync is safe but slow if every
write pays for its own; fsync-ing in the background is fast but loses a window.
**Decision.** One flusher goroutine owns the active segment. Appends are cheap
memory copies; the flusher takes everything appended since its last cycle as one
batch, writes it, and fsyncs per `append-fsync`: `always` after every batch,
`everysec` on a one-second tick if anything was written, `no` never on its own.
Rotation, `Close` and an explicit `WaitDurable` fsync whatever the policy. With
`always` the server holds a connection's replies until `WaitDurable` returns for
its records, so one fsync covers every record appended while the previous one
ran. A failed write is cut back to the last complete write; a failed fsync is
never retried, because the kernel may already have dropped the dirty pages and a
later success would prove nothing.
**Consequences.** `always` loses no acknowledged write (if the disk honors
fsync); `everysec` loses about a second on an OS or power crash (more on a slow
disk); `no` leaves it to the OS. The policy only affects `aof` mode. A single
flusher keeps the file order equal to the sequence order and the code simple;
it is also the one place that does log I/O, which keeps `Append` free of it.

## ADR-025: Admit backpressure and the error latch

**Context.** `Append` never blocks, so a fast writer can outrun a slow disk, and
a failed write or fsync means the log can no longer be trusted.
**Decision.** `Admit` blocks, outside any lock, while buffered bytes are at or
above the high-water mark (32 MiB, not configurable). The bound is soft:
records already admitted can exceed it. A failed write or fsync latches the AOF:
`Append`, `Admit` and `WaitDurable` fail until a healing checkpoint or a restart;
every write command gets the `MISCONF` reply, while reads and admin commands keep
working. Records accepted but never written are dropped and logged, never
silently. A checkpoint taken while latched abandons the untrusted segment, starts
a fresh unlisted one, writes a new base from memory, commits the manifest and only
then clears the latch. The automatic trigger fires whenever the log is latched.
If `Append` fails after the command was applied (the log latched between `Admit`
and `Append`), the client gets `MISCONF` instead of the reply for closure
commands, `Update` leaves the key untouched, and in-place `Mutate` and
`MutateEntry` changes stay in memory until the healing checkpoint.
**Consequences.** Memory and log can differ while latched; the healing base
makes them agree again. If the process dies first, the change is lost like any
non-durable write. The primitives (`Set`, `Delete`, `Flush`) cannot return the
error to their handler; the failure is kept for `JournalErr` and further writes
are refused. Healing needs disk space for a full base.

## ADR-026: A file-system seam with an adversarial in-memory implementation

**Context.** Crash safety depends on the order of write, fsync, rename and
directory sync, which real disks make hard to test.
**Decision.** All persistence I/O goes through an `FS` interface. `OSFS` is the
real implementation; `MemFS` models failure adversarially: file bytes are durable
only after `Sync`, and a created, renamed or removed directory entry only after
`SyncDir`. It can fail or "crash" at the n-th mutating operation, and `Crash`
returns the disk image a power loss at that moment could leave. Time is injected
the same way (a `Ticker` for the flusher and the trigger, `Now` for `LastSave`).
**Consequences.** Tests can cut a checkpoint, a rotation or a recovery at every
mutating step, deterministically. The guarantees are only as good as the model.
`OSFS.SyncDir` is a no-op on Windows, which cannot open directories for syncing,
so directory-entry durability is not guaranteed there.

## ADR-027: Persistence modes

**Context.** Not every deployment wants a log, and the in-memory default must
stay free.
**Decision.** `persistence-mode` is `off` (default), `snapshot` or `aof`. `off`
creates no manager and never touches the directory. `snapshot` writes
checkpoints on `SAVE`, `BGSAVE` and at shutdown, keeps no log, runs no
automatic trigger and never refuses writes. `aof` adds the log, the fsync
policy, the automatic trigger, `Admit` and the latch. `snapshot-on-shutdown` is
`auto` (on whenever persistence is on), `true` or `false`; it is a string so that
`Config` stays comparable. `append-fsync` and the auto-rewrite settings matter
only in `aof` mode.
**Consequences.** Switching `snapshot` to `aof` works (a segment is created and
listed at start). Switching `aof` to `snapshot` replays the existing segments at
the next start and the next checkpoint drops them, with new writes unlogged in
between. Switching to `off` leaves the directory stale, and switching back later
loads whatever was last there.

## ADR-028: Rewrite hooks consumed inside the critical section

**Context.** ADR-018 had `Dispatch` compute the rewritten form after the handler
returned. For a log, the record must be produced while the shard lock is still
held (ADR-020).
**Decision.** The journal calls `Spec.rewritten` from inside the closure of
`Mutate`, `MutateEntry`, `Atomic` and `Update`, after the handler's closure
succeeded and recorded its effects in `ctx.fx`. A hook may return several
commands; they are appended as one contiguous group. A write command without a
hook is logged verbatim. If the handler's closure returns an error, nothing is
logged. The record is encoded into a reused per-connection buffer within the
critical section, so verbatim entries are copied before the next read. The
primitives bypass the hooks and log their fixed forms (ADR-020).
**Consequences.** Hooks must keep recording effects inside the closure. The test
that requires every write command to be hooked or on the audited deterministic
list still guards new commands. Expiry deletions are logged by the hook of
ADR-022, not by a command hook.
