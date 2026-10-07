# Sylphy

[![CI](https://github.com/nima-ca/sylphy/actions/workflows/ci.yml/badge.svg)](https://github.com/nima-ca/sylphy/actions/workflows/ci.yml)
![Go version](https://img.shields.io/badge/go-1.24%2B-00ADD8?logo=go)
[![License: MIT](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)

Sylphy is a Redis-compatible in-memory database written in Go using only the
standard library. It speaks RESP2, so `redis-cli` and standard Redis client
libraries connect out of the box. It is built as a learning-grade but
production-minded project: sharded storage, correct pipelining, graceful
shutdown, fuzzed protocol parsing, and documented design decisions.

## Features

- [x] **Phase 1** — RESP2 protocol, concurrent TCP server, sharded string store, core commands
- [ ] **Phase 2** — TTL/expiry, typed values, SET options (NX/XX/EX/PX)
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
127.0.0.1:6380> INCRBY visits 5
(integer) 5
127.0.0.1:6380> MGET greeting visits nope
1) "hello world"
2) "5"
3) (nil)
```

Docker:

```bash
make docker
docker run --rm -p 6380:6380 sylphy:dev
```

Build with version info: `make build && ./bin/sylphy-server -version`.

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

`PING`, `ECHO`, `SET`, `GET`, `DEL`, `EXISTS`, `KEYS`, `DBSIZE`, `FLUSHALL`,
`INCR`, `DECR`, `INCRBY`, `DECRBY`, `APPEND`, `STRLEN`, `MSET`, `MGET`, `QUIT`,
plus compatibility shims `COMMAND`, `CLIENT SETNAME|SETINFO`, `SELECT 0`.
Details and deviations: [docs/COMMANDS.md](docs/COMMANDS.md).

## Development

Requires [Task](https://taskfile.dev) (`go install github.com/go-task/task/v3/cmd/task@latest`).

```bash
task tools       # install golangci-lint
task test-race   # unit + integration tests with the race detector
task cover       # coverage
task lint        # go vet + golangci-lint
task fuzz        # 20s protocol fuzz run
task bench       # parser and store benchmarks
task build       # bin/sylphy-server with version info
task docker      # build the image
```

## Docs

- [Architecture](docs/ARCHITECTURE.md)
- [Commands](docs/COMMANDS.md)
- [Design decisions](docs/DESIGN_DECISIONS.md)
- [Changelog](CHANGELOG.md)

## Roadmap

See the checklist above. Phase 2 will extend the store with per-key expiry and
typed values; the `Store.Update` primitive and the `command.Context` struct are
designed to absorb that without breaking existing handlers.

## License

MIT
