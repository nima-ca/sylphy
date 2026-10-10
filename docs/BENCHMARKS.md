# Benchmarks

How to measure Sylphy and what each benchmark is for. The results tables are
**empty on purpose**: they are meant to be filled from a run on the machine that
will be quoted, with the date and hardware, rather than copied from somewhere
else.

## Running

```bash
task bench-short   # protocol, store, list, zset; skips the 1M-key expiry benchmarks
task bench         # everything (a few minutes: it builds 1M-key stores)
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
