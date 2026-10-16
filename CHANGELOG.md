# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.3.0] - 2026-10-16

### Added

- **Persistence**, off by default, with three modes: `off`, `snapshot`
  (checkpoints on `SAVE`, `BGSAVE` and shutdown) and `aof` (every write logged,
  plus checkpoints). Options: `-persistence-mode`, `-dir`, `-append-fsync`
  (`always|everysec|no`), `-auto-rewrite-percentage`, `-auto-rewrite-min-size`,
  `-aof-load-truncated`, `-snapshot-on-shutdown`, each with a `SYLPHY_*`
  environment variable.
- **Data directory** with a `MANIFEST` commit point, one `base-NNNNNN.snap`
  snapshot and `seg-NNNNNN.aof` RESP log segments; CRC-32C on the manifest and
  snapshot blocks. Formats do not depend on the shard count.
- **Checkpoints**: one procedure for `SAVE`, `BGSAVE`, `BGREWRITEAOF`, the
  automatic trigger (aof mode, once per second) and the shutdown checkpoint:
  an O(shards) barrier, per-shard copy-on-write, atomic manifest commit.
- **Append-only log** with group commit, a single flusher, fsync policies and a
  32 MiB admission high-water mark; expiry of a key is logged as `DEL`.
- **Crash recovery** before the listener starts: frozen clock during load and
  replay, repair of a truncated last record, deletion of unlisted files, purge
  of keys that expired while down. Corrupt data stops startup with exit code 1.
- **Commands** `SAVE`, `BGSAVE`, `BGREWRITEAOF` (the same checkpoint as
  `BGSAVE`) and `LASTSAVE`.
- **`MISCONF`** reply for write commands while the log is in its error state;
  a checkpoint clears it.
- End-to-end persistence tests (`task test-e2e`), fuzz targets
  `FuzzRecordReader`, `FuzzSnapshotReader` and `FuzzDecodeManifest`, persistence
  benchmarks (`task bench-persist`), `docs/PERSISTENCE.md`.
- Docker image: `/data` data directory, running as uid 65532.

### Changed

- **`Rewrite` hooks** now record what a command actually did (`Effects`) inside
  the store closure and log a deterministic form:
    - `SET` drops `NX`/`XX`/`GET` and logs expiries as `PXAT` with the applied
      deadline, or `KEEPTTL`; `SETNX` logs as `SET`; `SETEX`/`PSETEX` as
      `SET ... PXAT`.
    - `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` log an unconditional
      `PEXPIREAT`; `GETEX` logs only its TTL effect (`PERSIST` or `PEXPIREAT`);
      `GETDEL` logs `DEL`; `SPOP` logs `SREM` of the members actually removed.
    - A deadline that was not in the future logs `DEL`, since replay runs with a
      frozen clock.
    - Hooks are allowed only on write commands; results must not alias the
      request buffers; `Context.Rewritten` is reset on every dispatch; refused,
      failed and no-effect commands log nothing.
    - Every write command must be either hooked or on an audited deterministic
      list (enforced by a test).
- Startup order: recover, start persistence, start the expiry sweeper, listen.
- `task fuzz`, `task bench` and `task bench-short` cover the persistence
  packages.

### Fixed

- Replay of writes whose deadline had already passed: they are logged as `DEL`
  instead of with an expiry that a frozen-clock replay would never apply.
- README roadmap no longer describes Phase 3 as future work; task descriptions
  for `fuzz`, `bench` and `bench-short` updated.

## [0.2.0] - 2026-10-10

### Added

- **Typed values.** The store holds `Value`s (`string`, `list`, `hash`, `set`,
  `zset`); empty collections are deleted automatically. Collections live in
  their own packages (`internal/list`, `internal/hash`, `internal/set`,
  `internal/zset`) and are reached through closure-based `View`, `Mutate`,
  `MutateEntry`, `Atomic` and `AtomicView`.
- **Key expiry.** Per-key deadlines with lazy expiry on access and an active
  sampling sweeper (`SweepConfig`, `Start`, `Close`, `Stats`). Injectable
  `Clock` (`SystemClock`, `FakeClock`).
- **Commands.** `EXPIRE`, `PEXPIRE`, `EXPIREAT`, `PEXPIREAT` (with
  `NX|XX|GT|LT`), `TTL`, `PTTL`, `PERSIST`, `EXPIRETIME`, `PEXPIRETIME`;
  `SET` with `NX|XX|GET|EX|PX|EXAT|PXAT|KEEPTTL`, `SETNX`, `SETEX`, `PSETEX`,
  `GETEX`, `GETDEL`; lists (`LPUSH`, `RPUSH`, `LPUSHX`, `RPUSHX`, `LPOP`,
  `RPOP`, `LLEN`, `LRANGE`, `LINDEX`, `LSET`, `LREM`, `LTRIM`, `LINSERT`);
  hashes (`HSET`, `HSETNX`, `HGET`, `HMGET`, `HDEL`, `HGETALL`, `HKEYS`,
  `HVALS`, `HEXISTS`, `HLEN`, `HSTRLEN`, `HINCRBY`, `HINCRBYFLOAT`); sets
  (`SADD`, `SREM`, `SMEMBERS`, `SISMEMBER`, `SMISMEMBER`, `SCARD`, `SPOP`,
  `SRANDMEMBER`, `SMOVE`, `SUNION`, `SINTER`, `SDIFF` and the `*STORE` forms);
  sorted sets (`ZADD` with `NX|XX|GT|LT|CH|INCR`, `ZREM`, `ZSCORE`, `ZMSCORE`,
  `ZINCRBY`, `ZCARD`, `ZCOUNT`, `ZRANK`, `ZREVRANK`, `ZRANGE` with
  `BYSCORE|REV|LIMIT|WITHSCORES`, `ZREVRANGE`, `ZRANGEBYSCORE`,
  `ZREVRANGEBYSCORE`, `ZREMRANGEBYRANK`, `ZREMRANGEBYSCORE`, `ZPOPMIN`,
  `ZPOPMAX`); key management (`TYPE`, `RENAME`, `RENAMENX`, `RANDOMKEY`,
  `UNLINK`, `TOUCH`, `SCAN` with `MATCH|COUNT|TYPE`).
- **Sorted set** implemented as a skip list with spans (O(log n) rank) plus a
  member map, with a model-based test against a naive reference.
- **`SCAN`** with a stateless cursor and a guarantee that every key present for
  the whole iteration is returned at least once.
- **Cross-shard atomic operations** (`Atomic`, `AtomicView`) locking shards in
  ascending index order.
- **`Rewrite` hooks** on write commands, producing a deterministic argv for a
  future append-only log (`Context.Propagate`, `Context.Rewritten`).
- Integration tests over go-redis for TTLs, `WRONGTYPE`, empty-collection
  deletion, `RENAME`, `SCAN` over 50k keys, pipelining across types and
  concurrent clients; benchmarks for sorted sets, lists and expiry overhead.
- `docs/BENCHMARKS.md`, ADR-010 to ADR-018, a `task bench-short` target.

### Changed

- `DBSIZE` and `Len` no longer count expired keys that the sweeper has not yet
  reclaimed (Redis does); the cost is proportional to the number of keys with a
  TTL.
- `SET` accepts its option set instead of replying `ERR syntax error`.
- Locking rule: operations that need several shard locks take them in ascending
  shard-index order (previously no operation held more than one).
- The integration test server now runs the expiry sweeper, as the real server
  does.

### Fixed

- Documentation referred to a Makefile; the project uses Task.

## [0.1.0] - 2026-10-07

### Added

- RESP2 `Reader` (arrays of bulk strings, inline commands, all RESP2 types) and
  `Writer`, with configurable safety limits, typed errors and a fuzz test.
- Sharded in-memory string store with copy semantics, atomic `Update`, and a
  Redis-compatible glob matcher.
- Commands: `PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `KEYS`, `DBSIZE`,
  `FLUSHALL`, `INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MSET`,
  `MGET`, `QUIT`, plus `COMMAND`, `CLIENT SETNAME|SETINFO` and `SELECT 0` shims.
- Concurrent TCP server with pipelining, idle timeout, client limit, panic
  isolation and graceful shutdown.
- Flag/environment configuration, structured logging via `log/slog`.
- CI workflow, Taskfile, Dockerfile, and documentation.

[Unreleased]: https://github.com/nima-ca/sylphy/compare/v0.3.0...HEAD
[0.3.0]: https://github.com/nima-ca/sylphy/compare/v0.2.0...v0.3.0
[0.2.0]: https://github.com/nima-ca/sylphy/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nima-ca/sylphy/releases/tag/v0.1.0
