# Benchmarks

How to measure Sylphy and what each benchmark is for. The results tables are
**empty on purpose**: they are meant to be filled from a run on the machine that
will be quoted, with the date and hardware, rather than copied from somewhere
else.

## Running

```bash
task bench-short   # protocol, store, list, zset, persist; skips the 1M-key and the persistence server benchmarks
task bench         # everything (a few minutes: it builds 1M-key stores)
task bench-persist # only the persistence benchmarks (timeout 30m)
go test -run '^$' -bench 'Rank|Insert' -benchmem -count 5 ./internal/zset   # one family, repeated
```

Use `-count 5` or more and compare runs with `benchstat`. Close other programs
and pin the CPU governor if you can; the sharded-store benchmarks scale with
cores.

## Inventory

| Package             | Benchmark                                 | What it measures                                                                                          |
| ------------------- | ----------------------------------------- | --------------------------------------------------------------------------------------------------------- |
| `internal/protocol` | `ReadCommandSet`, `ReadCommandGet`        | Parsing a pipelined `SET` / `GET`                                                                         |
| `internal/store`    | `Mixed` (sharded vs single mutex)         | Phase 1 string reads/writes at 1, 8 and 64 goroutines                                                     |
| `internal/store`    | `Get1M`, `Overwrite1M`                    | A 1M-key store with and without TTLs: the cost of the expiry check on reads and of index upkeep on writes |
| `internal/store`    | `Len1M`                                   | What `DBSIZE` costs; proportional to keys with a TTL ([ADR-017](DESIGN_DECISIONS.md))                     |
| `internal/store`    | `SweepCycle1M`                            | One sweeper cycle over 1M TTL keys, none due: depends on shards and sample size, not on keyspace size     |
| `internal/store`    | `SweepReclaim100k`                        | Time and cycles (`cycles/op`) for the sweeper to reclaim 100k expired, never-read keys                    |
| `internal/zset`     | `Insert`, `UpdateScore`                   | Skip-list insert and score change at 1k and 100k members                                                  |
| `internal/zset`     | `Rank`                                    | O(log n) rank via spans                                                                                   |
| `internal/zset`     | `RangeByRank100`, `RangeByScore`, `Count` | Range reads of 100 elements; score counting                                                               |
| `internal/zset`     | `PopMinAndReinsert`                       | The priority-queue pattern                                                                                |
| `internal/list`     | `Queue`, `Stack`                          | Push/pop at steady depth                                                                                  |
| `internal/list`     | `At`, `Range100`, `GrowAndDrain`          | Index access, range copy, ring-buffer resize and shrink                                                   |
| `internal/persist`  | `BenchmarkSnapshotWrite`                  | Writing a base snapshot file                                                                              |
| `internal/persist`  | `BenchmarkSnapshotRead`                   | Reading and verifying a base snapshot file                                                                |
| `internal/persist`  | `BenchmarkRecoverSnapshotOnly`            | Startup recovery from a manifest and a base snapshot, no log                                              |
| `internal/persist`  | `BenchmarkRecoverSnapshotAndTail`         | Recovery from a base snapshot plus replay of a log segment                                                |
| `test/integration`  | `BenchmarkPersistSetThroughput`           | `SET` throughput against a real server per fsync policy; skipped by `-short`                              |
| `test/integration`  | `BenchmarkPersistLatencyDuringBGSave`     | Command latency while a `BGSAVE` checkpoint runs; skipped by `-short`                                     |

## Results

Machine: _CPU, cores, RAM, OS, Go version, date_

| Benchmark                      | ns/op | B/op | allocs/op | Notes      |
| ------------------------------ | ----- | ---- | --------- | ---------- |
| `Get1M/no-ttl`                 |       |      |           |            |
| `Get1M/ttl`                    |       |      |           |            |
| `Overwrite1M/no-ttl`           |       |      |           |            |
| `Overwrite1M/with-ttl`         |       |      |           |            |
| `Len1M/no-ttl`                 |       |      |           |            |
| `Len1M/ttl`                    |       |      |           |            |
| `SweepCycle1M`                 |       |      |           |            |
| `SweepReclaim100k`             |       |      |           | cycles/op: |
| `zset Insert`                  |       |      |           |            |
| `zset UpdateScore/n=100000`    |       |      |           |            |
| `zset Rank/n=100000`           |       |      |           |            |
| `zset RangeByRank100/n=100000` |       |      |           |            |
| `zset PopMinAndReinsert`       |       |      |           |            |
| `list Queue`                   |       |      |           |            |
| `list At/n=100000`             |       |      |           |            |

What to look for:

- `Get1M/ttl` vs `Get1M/no-ttl`: the per-read overhead of the expiry check
  should be small.
- `Overwrite1M/with-ttl` vs `no-ttl`: the price of keeping the expiry index
  current on writes.
- `Len1M/ttl` growing with the number of TTL keys while `no-ttl` stays flat
  confirms the cost model in ADR-017.
- `SweepCycle1M` should not depend on the number of keys, only on shards and
  sample size.
- `zset Rank` and `RangeByRank100` should grow only slowly from 1k to 100k
  members (logarithmic).

## Persistence

```bash
task bench-persist
go test -timeout 30m -run '^$' -bench 'Snapshot|Recover' -benchmem -count 5 ./internal/persist   # one family, repeated
```

Methodology:

- Run on the machine and disk you will quote. Fsync cost is the disk's, so
  record the device, file system and where the benchmark data directory lives
  next to the results.
- Use `-count 5` or more and compare with `benchstat`. Record dataset size
  (keys and bytes) for the snapshot and recovery benchmarks.
- For `PersistSetThroughput`, fill one row per sub-benchmark exactly as the
  output names it (one per fsync policy). For `PersistLatencyDuringBGSave`,
  copy every metric the benchmark reports.
- Compare latency during `BGSAVE` with the same benchmark's idle numbers; the
  barrier pause is O(shards) and writers touching a pending shard pay for its
  serialization ([Checkpoint](PERSISTENCE.md#checkpoint)).

Machine: _CPU, cores, RAM, disk, OS, Go version, date_

| Benchmark                          | ns/op                        | B/op                         | allocs/op                    | Notes |
| ---------------------------------- | ---------------------------- | ---------------------------- | ---------------------------- | ----- |
| `SnapshotWrite`                    | TBD (run task bench-persist) | TBD (run task bench-persist) | TBD (run task bench-persist) |       |
| `SnapshotRead`                     | TBD (run task bench-persist) | TBD (run task bench-persist) | TBD (run task bench-persist) |       |
| `RecoverSnapshotOnly`              | TBD (run task bench-persist) | TBD (run task bench-persist) | TBD (run task bench-persist) |       |
| `RecoverSnapshotAndTail`           | TBD (run task bench-persist) | TBD (run task bench-persist) | TBD (run task bench-persist) |       |
| `PersistSetThroughput/<sub-bench>` | TBD (run task bench-persist) | TBD (run task bench-persist) | TBD (run task bench-persist) |       |
| `PersistLatencyDuringBGSave`       | TBD (run task bench-persist) | TBD (run task bench-persist) | TBD (run task bench-persist) |       |

What to look for:

- Under `always`, throughput is bounded by fsync latency; concurrent clients
  share one fsync through group commit, so it should rise with client count.
- `everysec` and `no` should sit close to persistence off, apart from the
  cost of copying records into the log buffer.
- Recovery with a tail should cost more than snapshot-only by the replay time.

## End-to-end with redis-benchmark

Start the server (`task build && ./bin/sylphy-server`), then:

```bash
redis-benchmark -p 6380 -t set,get -n 200000 -c 50 -q
redis-benchmark -p 6380 -t set,get -n 200000 -c 50 -P 16 -q          # pipelined
redis-benchmark -p 6380 -n 100000 -c 50 -q \
    -r 100000 ZADD zbench __rand_int__ member:__rand_int__            # sorted-set inserts
redis-benchmark -p 6380 -n 100000 -c 50 -q -r 100000 \
    SET key:__rand_int__ v EX 60                                       # SET with TTL
redis-benchmark -p 6380 -n 100000 -c 50 -q LPUSH lbench x              # one hot list
```

Results depend on the network stack and client count as much as on the server;
record the exact command line next to any number you quote.

| Command                 | Clients | Pipeline | Requests/s | p50 (ms) |
| ----------------------- | ------- | -------- | ---------- | -------- |
| `SET`                   | 50      | 1        |            |          |
| `GET`                   | 50      | 1        |            |          |
| `SET`                   | 50      | 16       |            |          |
| `ZADD` (random members) | 50      | 1        |            |          |
| `SET ... EX 60`         | 50      | 1        |            |          |
| `LPUSH` (one hot key)   | 50      | 1        |            |          |

Persistence: repeat the `SET` run with the server started in each mode.

| Server flags                                   | Clients | Pipeline | Requests/s                   | p50 (ms)                     |
| ---------------------------------------------- | ------- | -------- | ---------------------------- | ---------------------------- |
| `-persistence-mode aof -append-fsync always`   | 50      | 1        | TBD (run task bench-persist) | TBD (run task bench-persist) |
| `-persistence-mode aof -append-fsync everysec` | 50      | 1        | TBD (run task bench-persist) | TBD (run task bench-persist) |
| `-persistence-mode aof -append-fsync no`       | 50      | 1        | TBD (run task bench-persist) | TBD (run task bench-persist) |
| `-persistence-mode snapshot`                   | 50      | 1        | TBD (run task bench-persist) | TBD (run task bench-persist) |
