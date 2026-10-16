# Sylphy

[![CI](https://github.com/nima-ca/sylphy/actions/workflows/ci.yml/badge.svg)](https://github.com/nima-ca/sylphy/actions/workflows/ci.yml)
![Go version](https://img.shields.io/badge/go-1.24%2B-00ADD8?logo=go)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Sylphy is a Redis-compatible in-memory database written in Go using only the
standard library. It speaks RESP2, so `redis-cli` and standard Redis client
libraries connect out of the box. It is built as a learning-grade but
production-minded project: sharded storage, typed values, key expiry, correct
pipelining, graceful shutdown, fuzzed protocol parsing, and documented design
decisions.

## Features

- [x] **Phase 1** — RESP2 protocol, concurrent TCP server, sharded string store, core commands
- [x] **Phase 2** — TTL/expiry, typed values (lists, hashes, sets, sorted sets), SET options (NX/XX/EX/PX/EXAT/PXAT/KEEPTTL/GET), `SCAN`, key management
- [x] **Phase 3** — Persistence (snapshots, append-only log, crash recovery; off by default)
- [ ] **Phase 4** — Transactions and pub/sub
- [ ] **Phase 5** — Replication

## Quickstart

```bash
go run ./cmd/sylphy-server            # listens on 127.0.0.1:6380
```

```text
$ redis-cli -p 6380
127.0.0.1:6380> SET greeting hello
OK
127.0.0.1:6380> APPEND greeting " world"
(integer) 11
127.0.0.1:6380> GET greeting
"hello world"
127.0.0.1:6380> MGET greeting visits nope
1) "hello world"
2) (nil)
3) (nil)
```

### Expiry

```text
127.0.0.1:6380> SET session abc EX 30
OK
127.0.0.1:6380> TTL session
(integer) 30
127.0.0.1:6380> PEXPIRE session 1500 XX
(integer) 1
127.0.0.1:6380> PERSIST session
(integer) 1
127.0.0.1:6380> SET lock owner-1 NX PX 5000
OK
127.0.0.1:6380> SET lock owner-2 NX PX 5000
(nil)
127.0.0.1:6380> GETEX session EX 60
"abc"
```

Expired keys are never visible: reads check the deadline, and a background
sweeper reclaims keys nobody reads.

### Typed values

```text
127.0.0.1:6380> RPUSH queue a b c
(integer) 3
127.0.0.1:6380> LRANGE queue 0 -1
1) "a"
2) "b"
3) "c"
127.0.0.1:6380> HSET user:1 name Ada age 36
(integer) 2
127.0.0.1:6380> HGET user:1 name
"Ada"
127.0.0.1:6380> SADD tags go redis
(integer) 2
127.0.0.1:6380> ZADD board 100 alice 250 bob 175 carol
(integer) 3
127.0.0.1:6380> ZRANGE board 0 -1 WITHSCORES
1) "alice"
2) "100"
3) "carol"
4) "175"
5) "bob"
6) "250"
127.0.0.1:6380> ZREVRANK board alice
(integer) 2
127.0.0.1:6380> GET board
(error) WRONGTYPE Operation against a key holding the wrong kind of value
127.0.0.1:6380> TYPE board
zset
127.0.0.1:6380> RENAME board scores
OK
127.0.0.1:6380> SCAN 0 MATCH user:* COUNT 100
1) "0"
2) 1) "user:1"
```

A list, hash, set or sorted set that becomes empty is deleted, along with its
TTL, exactly as in Redis.

### Persistence

Persistence is off by default. Modes: `snapshot` (checkpoints on `SAVE`,
`BGSAVE` and shutdown) and `aof` (every write is logged, plus checkpoints).
Kill-and-recover with the log fully synced:

```bash
go run ./cmd/sylphy-server -persistence-mode aof -append-fsync always -dir ./data
```

```text
$ redis-cli -p 6380
127.0.0.1:6380> SET greeting hello
OK
127.0.0.1:6380> SET counter 1
OK
```

```bash
kill -9 <pid>                      # no graceful shutdown
go run ./cmd/sylphy-server -persistence-mode aof -append-fsync always -dir ./data
```

```text
$ redis-cli -p 6380
127.0.0.1:6380> GET greeting
"hello"
127.0.0.1:6380> SAVE
OK
127.0.0.1:6380> LASTSAVE
(integer) <unix seconds>
```

With `-append-fsync everysec` (the default) a crash can lose about a second of
acknowledged writes. Corrupt data makes the server refuse to start (exit
code 1) rather than guess. Formats, recovery rules and loss windows:
[docs/PERSISTENCE.md](docs/PERSISTENCE.md).

Docker:

```bash
task docker
docker run --rm -p 6380:6380 sylphy:$(git describe --tags --always --dirty)
# with persistence: the image's /data directory on a named volume
docker run --rm -p 6380:6380 -v sylphy-data:/data -e SYLPHY_PERSISTENCE_MODE=aof \
    sylphy:$(git describe --tags --always --dirty)
```

Build with version info: `task build && ./bin/sylphy-server -version`.

## Configuration

Precedence: flag > environment variable > default.

| Flag                     | Env                            | Default          | Description                      |
| ------------------------ | ------------------------------ | ---------------- | -------------------------------- |
| `-addr`                  | `SYLPHY_ADDR`                  | `127.0.0.1:6380` | Listen address                   |
| `-shards`                | `SYLPHY_SHARDS`                | `32`             | Store shards (power of two)      |
| `-max-clients`           | `SYLPHY_MAX_CLIENTS`           | `10000`          | Max concurrent clients           |
| `-idle-timeout`          | `SYLPHY_IDLE_TIMEOUT`          | `0` (off)        | Close idle connections           |
| `-shutdown-grace-period` | `SYLPHY_SHUTDOWN_GRACE_PERIOD` | `10s`            | Graceful shutdown deadline       |
| `-log-level`             | `SYLPHY_LOG_LEVEL`             | `info`           | `debug`, `info`, `warn`, `error` |
| `-log-format`            | `SYLPHY_LOG_FORMAT`            | `text`           | `text` or `json`                 |
| `-max-bulk-len`          | `SYLPHY_MAX_BULK_LEN`          | `536870912`      | Max bulk string bytes            |
| `-max-array-len`         | `SYLPHY_MAX_ARRAY_LEN`         | `1048576`        | Max array elements               |
| `-max-inline-len`        | `SYLPHY_MAX_INLINE_LEN`        | `65536`          | Max inline line bytes            |
| `-max-depth`             | `SYLPHY_MAX_DEPTH`             | `32`             | Max array nesting                |

Invalid configuration prints a message and exits with code 2.

Persistence options (details in [docs/PERSISTENCE.md](docs/PERSISTENCE.md)):

| Flag                       | Env                              | Default    | Description                                                                                                                  |
| -------------------------- | -------------------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------- |
| `-persistence-mode`        | `SYLPHY_PERSISTENCE_MODE`        | `off`      | `off`, `snapshot` or `aof`                                                                                                   |
| `-dir`                     | `SYLPHY_DIR`                     | `./data`   | Data directory; must be non-empty when persistence is on                                                                     |
| `-append-fsync`            | `SYLPHY_APPEND_FSYNC`            | `everysec` | `always`, `everysec` or `no`; acted on in `aof` mode only, but validated in every mode                                       |
| `-auto-rewrite-percentage` | `SYLPHY_AUTO_REWRITE_PERCENTAGE` | `100`      | Checkpoint when the log has grown this percent over the base snapshot; `0` disables. `aof` mode only                         |
| `-auto-rewrite-min-size`   | `SYLPHY_AUTO_REWRITE_MIN_SIZE`   | `64m`      | Smallest base+log size that may trigger a checkpoint; bytes, or suffix `k`, `m`, `g` (also `kb`, `kib`, ...), powers of 1024 |
| `-aof-load-truncated`      | `SYLPHY_AOF_LOAD_TRUNCATED`      | `true`     | Repair a partial last log record at startup instead of refusing to start                                                     |
| `-snapshot-on-shutdown`    | `SYLPHY_SNAPSHOT_ON_SHUTDOWN`    | `auto`     | `auto` (on whenever persistence is on), `true` or `false`; `true` with mode `off` is a config error                          |

## Supported commands

| Group       | Commands                                                                                                                                                                                                                                                               |
| ----------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Connection  | `PING` `ECHO` `QUIT` `SELECT 0` `CLIENT SETNAME\|SETINFO` `COMMAND`                                                                                                                                                                                                    |
| Strings     | `SET` (`NX XX GET EX PX EXAT PXAT KEEPTTL`) `SETNX` `SETEX` `PSETEX` `GET` `GETEX` `GETDEL` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `STRLEN` `MSET` `MGET`                                                                                                            |
| Expiry      | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` (`NX XX GT LT`) `TTL` `PTTL` `PERSIST` `EXPIRETIME` `PEXPIRETIME`                                                                                                                                                            |
| Lists       | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LREM` `LTRIM` `LINSERT`                                                                                                                                                               |
| Hashes      | `HSET` `HSETNX` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HEXISTS` `HLEN` `HSTRLEN` `HINCRBY` `HINCRBYFLOAT`                                                                                                                                                    |
| Sets        | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE`                                                                                                              |
| Sorted sets | `ZADD` (`NX XX GT LT CH INCR`) `ZREM` `ZSCORE` `ZMSCORE` `ZINCRBY` `ZCARD` `ZCOUNT` `ZRANK` `ZREVRANK` (`WITHSCORE`) `ZRANGE` (`BYSCORE REV LIMIT WITHSCORES`) `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZPOPMIN` `ZPOPMAX` |
| Persistence | `SAVE` `BGSAVE` `BGREWRITEAOF` (same checkpoint as `BGSAVE`) `LASTSAVE`                                                                                                                                                                                                |
| Keys        | `DEL` `UNLINK` `EXISTS` `TOUCH` `TYPE` `RENAME` `RENAMENX` `RANDOMKEY` `KEYS` `SCAN` `DBSIZE` `FLUSHALL`                                                                                                                                                               |

Syntax, return values, complexity and every deviation from Redis:
[docs/COMMANDS.md](docs/COMMANDS.md).

## Development

Requires [Task](https://taskfile.dev) (`go install github.com/go-task/task/v3/cmd/task@latest`).

```bash
task tools        # install golangci-lint
task test-race    # unit + integration tests with the race detector
task test-e2e     # persistence end-to-end tests (builds and kills the real binary; skipped on Windows)
task cover        # coverage
task lint         # go vet + golangci-lint
task fuzz         # 20s each: protocol reader, persistence decoders
task bench-short  # quick benchmarks (protocol, store, list, zset, persist; skips 1M-key and persistence server benchmarks)
task bench        # everything, including the 1M-key expiry and recovery benchmarks
task bench-persist # persistence benchmarks only
task build        # bin/sylphy-server with version info
task docker       # build the image
```

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Commands](docs/COMMANDS.md)
- [Persistence](docs/PERSISTENCE.md)
- [Design decisions](docs/DESIGN_DECISIONS.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Changelog](CHANGELOG.md)

## Roadmap

See the checklist above. Phases 4 and 5 are future work and nothing below is
implemented yet.

- Replication (Phase 5) is planned to reuse the command journal's `Propagator`
  interface and the `Rewrite` hooks, so a replica would receive the same
  deterministic commands (absolute `PXAT` deadlines, `SREM` instead of `SPOP`)
  that the append-only log stores.
- Transactions and pub/sub (Phase 4) have no persistence-specific work
  planned in the current docs.

## License

MIT
