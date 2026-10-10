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
- [ ] **Phase 3** — Persistence
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

Docker:

```bash
task docker
docker run --rm -p 6380:6380 sylphy:dev
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

## Supported commands

| Group           | Commands                                                                                                                                                                           |
| --------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Connection      | `PING` `ECHO` `QUIT` `SELECT 0` `CLIENT SETNAME\|SETINFO` `COMMAND`                                                                                                                |
| Strings         | `SET` (`NX XX GET EX PX EXAT PXAT KEEPTTL`) `SETNX` `SETEX` `PSETEX` `GET` `GETEX` `GETDEL` `INCR` `DECR` `INCRBY` `DECRBY` `APPEND` `STRLEN` `MSET` `MGET`                         |
| Expiry          | `EXPIRE` `PEXPIRE` `EXPIREAT` `PEXPIREAT` (`NX XX GT LT`) `TTL` `PTTL` `PERSIST` `EXPIRETIME` `PEXPIRETIME`                                                                        |
| Lists           | `LPUSH` `RPUSH` `LPUSHX` `RPUSHX` `LPOP` `RPOP` `LLEN` `LRANGE` `LINDEX` `LSET` `LREM` `LTRIM` `LINSERT`                                                                            |
| Hashes          | `HSET` `HSETNX` `HGET` `HMGET` `HDEL` `HGETALL` `HKEYS` `HVALS` `HEXISTS` `HLEN` `HSTRLEN` `HINCRBY` `HINCRBYFLOAT`                                                                |
| Sets            | `SADD` `SREM` `SMEMBERS` `SISMEMBER` `SMISMEMBER` `SCARD` `SPOP` `SRANDMEMBER` `SMOVE` `SUNION` `SINTER` `SDIFF` `SUNIONSTORE` `SINTERSTORE` `SDIFFSTORE`                          |
| Sorted sets     | `ZADD` (`NX XX GT LT CH INCR`) `ZREM` `ZSCORE` `ZMSCORE` `ZINCRBY` `ZCARD` `ZCOUNT` `ZRANK` `ZREVRANK` (`WITHSCORE`) `ZRANGE` (`BYSCORE REV LIMIT WITHSCORES`) `ZREVRANGE` `ZRANGEBYSCORE` `ZREVRANGEBYSCORE` `ZREMRANGEBYRANK` `ZREMRANGEBYSCORE` `ZPOPMIN` `ZPOPMAX` |
| Keys            | `DEL` `UNLINK` `EXISTS` `TOUCH` `TYPE` `RENAME` `RENAMENX` `RANDOMKEY` `KEYS` `SCAN` `DBSIZE` `FLUSHALL`                                                                           |

Syntax, return values, complexity and every deviation from Redis:
[docs/COMMANDS.md](docs/COMMANDS.md).

## Development

Requires [Task](https://taskfile.dev) (`go install github.com/go-task/task/v3/cmd/task@latest`).

```bash
task tools        # install golangci-lint
task test-race    # unit + integration tests with the race detector
task cover        # coverage
task lint         # go vet + golangci-lint
task fuzz         # 20s protocol fuzz run
task bench-short  # quick benchmarks (protocol, store, list, zset)
task bench        # everything, including the 1M-key expiry benchmarks
task build        # bin/sylphy-server with version info
task docker       # build the image
```

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Commands](docs/COMMANDS.md)
- [Design decisions](docs/DESIGN_DECISIONS.md)
- [Benchmarks](docs/BENCHMARKS.md)
- [Changelog](CHANGELOG.md)

## Roadmap

See the checklist above. Phase 3 (persistence) builds on two Phase 2 pieces:
the per-command `Rewrite` hooks, which turn every write into a deterministic
command for an append-only log, and the typed value model, whose collections
can be walked under the shard lock for snapshots.

## License

MIT