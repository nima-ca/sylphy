# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project
adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
- CI workflow, Makefile, Dockerfile, and documentation.

[Unreleased]: https://github.com/nima-ca/sylphy/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/nima-ca/sylphy/releases/tag/v0.1.0
