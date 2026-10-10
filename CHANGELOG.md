# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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

[Unreleased]: https://github.com/nima-ca/sylphy/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/nima-ca/sylphy/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/nima-ca/sylphy/releases/tag/v0.1.0
