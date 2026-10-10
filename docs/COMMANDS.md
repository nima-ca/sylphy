# Commands

Complexity: _n_ = size of the value or collection, _k_ = number of arguments
(keys, members, fields), _m_ = elements returned or affected, _N_ = keys in the
database, _S_ = shards. Command names are case-insensitive. Replies are RESP2.

## Connection and server

| Command  | Syntax                                              | Returns                | Complexity | Notes                                                                                |
| -------- | --------------------------------------------------- | ---------------------- | ---------- | ------------------------------------------------------------------------------------ |
| PING     | `PING [msg]`                                        | `+PONG`, or bulk `msg` | O(1)       |                                                                                      |
| ECHO     | `ECHO msg`                                          | bulk `msg`             | O(1)       |                                                                                      |
| QUIT     | `QUIT`                                              | `+OK`, then close      | O(1)       |                                                                                      |
| SELECT   | `SELECT 0`                                          | `+OK`                  | O(1)       | Other indexes: `ERR DB index is out of range`                                        |
| CLIENT   | `CLIENT SETNAME name` / `CLIENT SETINFO attr value` | `+OK`                  | O(1)       | Other subcommands: `ERR unknown subcommand`                                          |
| COMMAND  | `COMMAND`, `COMMAND DOCS`, `COMMAND COUNT`          | empty array            | O(1)       | Shim only. `COMMAND COUNT` returns an empty array rather than an integer             |
| DBSIZE   | `DBSIZE`                                            | integer                | O(S + T)   | _T_ = keys with a TTL. Counts live keys only: expired, unreclaimed keys are excluded |
| FLUSHALL | `FLUSHALL [ASYNC\|SYNC]`                            | `+OK`                  | O(S)       | Both modes behave identically                                                        |

## Strings

| Command         | Syntax                                                                 | Returns                                                          | Complexity | Notes                                                                                                                                                                                                            |
| --------------- | ---------------------------------------------------------------------- | ---------------------------------------------------------------- | ---------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| SET             | `SET key value [NX\|XX] [GET] [EX s\|PX ms\|EXAT s\|PXAT ms\|KEEPTTL]` | `+OK`; with `GET` the old value or nil; nil if `NX`/`XX` refused | O(n)       | Clears the TTL unless `KEEPTTL`. Replaces a value of any type. `GET` on a non-string: `WRONGTYPE`, nothing changes. A past `EXAT`/`PXAT` deletes the key. Bad expiry: `ERR invalid expire time in 'set' command` |
| SETNX           | `SETNX key value`                                                      | 1 if set, 0 if the key exists (any type)                         | O(n)       |                                                                                                                                                                                                                  |
| SETEX / PSETEX  | `SETEX key seconds value`                                              | `+OK`                                                            | O(n)       | Non-positive or overflowing TTL: `ERR invalid expire time in 'setex' command`                                                                                                                                    |
| GET             | `GET key`                                                              | bulk or nil                                                      | O(n)       | Value is copied. Other types: `WRONGTYPE`                                                                                                                                                                        |
| GETEX           | `GETEX key [EX s\|PX ms\|EXAT s\|PXAT ms\|PERSIST]`                    | bulk or nil                                                      | O(n)       | Options are validated before the key is read                                                                                                                                                                     |
| GETDEL          | `GETDEL key`                                                           | bulk or nil                                                      | O(n)       |                                                                                                                                                                                                                  |
| INCR / DECR     | `INCR key`                                                             | integer: new value                                               | O(1)       | Missing key = 0. Keeps the TTL                                                                                                                                                                                   |
| INCRBY / DECRBY | `INCRBY key n`                                                         | integer: new value                                               | O(1)       | Strict int64 parsing                                                                                                                                                                                             |
| APPEND          | `APPEND key value`                                                     | integer: new length                                              | O(n)       | Keeps the TTL                                                                                                                                                                                                    |
| STRLEN          | `STRLEN key`                                                           | integer                                                          | O(n)       | Redis is O(1); Sylphy copies the value (deviation)                                                                                                                                                               |
| MSET            | `MSET key value [key value ...]`                                       | `+OK`                                                            | O(k)       | **Atomic per key, not across keys**                                                                                                                                                                              |
| MGET            | `MGET key [key ...]`                                                   | array of bulk/nil                                                | O(k)       | **Atomic per key, not across keys**; non-strings read as nil                                                                                                                                                     |

### Integer rules (INCR family)

Values are parsed as signed 64-bit decimal: no whitespace, no `+` prefix, no
leading zeros (except `0`), no `-0`. Failures reply
`ERR value is not an integer or out of range`; overflow replies
`ERR increment or decrement would overflow`. `DECRBY key -9223372036854775808`
also replies with the overflow error.

## Expiry

Deadlines are absolute Unix milliseconds. An expired key is invisible to every
command. Writing to a collection keeps its TTL; a collection that becomes empty
is deleted with its TTL.

| Command                  | Syntax                                       | Returns                                                            | Complexity | Notes                                                                                                                                                                                                                            |
| ------------------------ | -------------------------------------------- | ------------------------------------------------------------------ | ---------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| EXPIRE / PEXPIRE         | `EXPIRE key seconds [NX\|XX\|GT\|LT]`        | 1 if the TTL was set, 0 if not (missing key, or condition not met) | O(1)       | `NX` only without a TTL, `XX` only with one, `GT`/`LT` compare with the current deadline; a key without a TTL counts as infinite. NX with XX/GT/LT, or GT with LT: error. A deadline not in the future deletes the key (reply 1) |
| EXPIREAT / PEXPIREAT     | `EXPIREAT key unix-seconds [NX\|XX\|GT\|LT]` | as above                                                           | O(1)       | Overflow: `ERR invalid expire time in 'expireat' command`                                                                                                                                                                        |
| TTL / PTTL               | `TTL key`                                    | integer; -2 missing, -1 no TTL                                     | O(1)       | `TTL` rounds to the nearest second, never reports 0 for a live key's rounding down to -1                                                                                                                                         |
| PERSIST                  | `PERSIST key`                                | 1 if a TTL was removed, else 0                                     | O(1)       |                                                                                                                                                                                                                                  |
| EXPIRETIME / PEXPIRETIME | `EXPIRETIME key`                             | deadline in Unix s / ms; -2 missing, -1 no TTL                     | O(1)       |                                                                                                                                                                                                                                  |

## Lists

| Command         | Syntax                                    | Returns                                                              | Complexity     | Notes                                                                   |
| --------------- | ----------------------------------------- | -------------------------------------------------------------------- | -------------- | ----------------------------------------------------------------------- |
| LPUSH / RPUSH   | `LPUSH key element [element ...]`         | integer: new length                                                  | O(k) amortized | Creates the list                                                        |
| LPUSHX / RPUSHX | `LPUSHX key element [element ...]`        | integer: new length, 0 if no list                                    | O(k)           | Never creates                                                           |
| LPOP / RPOP     | `LPOP key [count]`                        | bulk or nil; with `count` an array (nil array if the key is missing) | O(m)           | Count must be non-negative; deleting the last element deletes the key   |
| LLEN            | `LLEN key`                                | integer                                                              | O(1)           |                                                                         |
| LRANGE          | `LRANGE key start stop`                   | array                                                                | O(m)           | Inclusive, negative indexes count from the end; out of range is clamped |
| LINDEX          | `LINDEX key index`                        | bulk or nil                                                          | O(1)           | Redis is O(n)                                                           |
| LSET            | `LSET key index element`                  | `+OK`                                                                | O(1)           | Missing key: `ERR no such key`; bad index: `ERR index out of range`     |
| LREM            | `LREM key count element`                  | integer: removed                                                     | O(n)           | `count > 0` from the head, `< 0` from the tail, `0` all                 |
| LTRIM           | `LTRIM key start stop`                    | `+OK`                                                                | O(n)           | An empty result deletes the key                                         |
| LINSERT         | `LINSERT key BEFORE\|AFTER pivot element` | new length, -1 if no pivot, 0 if no key                              | O(n)           |                                                                         |

## Hashes

| Command       | Syntax                                   | Returns                         | Complexity | Notes                                                                                    |
| ------------- | ---------------------------------------- | ------------------------------- | ---------- | ---------------------------------------------------------------------------------------- |
| HSET          | `HSET key field value [field value ...]` | integer: fields added           | O(k)       | Also the multi-field form; `HMSET` is not provided                                       |
| HSETNX        | `HSETNX key field value`                 | 1 if set, 0 if the field exists | O(1)       |                                                                                          |
| HGET          | `HGET key field`                         | bulk or nil                     | O(1)       |                                                                                          |
| HMGET         | `HMGET key field [field ...]`            | array of bulk/nil               | O(k)       |                                                                                          |
| HDEL          | `HDEL key field [field ...]`             | integer: removed                | O(k)       | Deleting the last field deletes the key                                                  |
| HGETALL       | `HGETALL key`                            | flat array field, value, ...    | O(n)       | Order is insertion order, disturbed only by deletions (Redis: unspecified)               |
| HKEYS / HVALS | `HKEYS key`                              | array                           | O(n)       | Same order as `HGETALL`                                                                  |
| HEXISTS       | `HEXISTS key field`                      | 1 or 0                          | O(1)       |                                                                                          |
| HLEN          | `HLEN key`                               | integer                         | O(1)       |                                                                                          |
| HSTRLEN       | `HSTRLEN key field`                      | integer; 0 if missing           | O(1)       |                                                                                          |
| HINCRBY       | `HINCRBY key field increment`            | integer: new value              | O(1)       | int64 rules as `INCR`; non-integer field: `ERR hash value is not an integer`             |
| HINCRBYFLOAT  | `HINCRBYFLOAT key field increment`       | bulk: new value                 | O(1)       | Exact decimal arithmetic with Redis's output formatting; `ERR hash value is not a float` |

## Sets

| Command                                | Syntax                                  | Returns                            | Complexity   | Notes                                                                                                         |
| -------------------------------------- | --------------------------------------- | ---------------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------- |
| SADD                                   | `SADD key member [member ...]`          | integer: members added             | O(k)         |                                                                                                               |
| SREM                                   | `SREM key member [member ...]`          | integer: removed                   | O(k)         | Removing the last member deletes the key                                                                      |
| SMEMBERS                               | `SMEMBERS key`                          | array                              | O(n)         | Insertion order                                                                                               |
| SISMEMBER / SMISMEMBER                 | `SMISMEMBER key member [member ...]`    | 1/0, or an array of 1/0            | O(k)         |                                                                                                               |
| SCARD                                  | `SCARD key`                             | integer                            | O(1)         |                                                                                                               |
| SPOP                                   | `SPOP key [count]`                      | bulk or nil; with `count` an array | O(count)     | Uniformly random; `count` must be non-negative                                                                |
| SRANDMEMBER                            | `SRANDMEMBER key [count]`               | bulk or nil; with `count` an array | O(\|count\|) | Positive count: distinct members; negative: may repeat, capped at 1,048,576 results (error beyond; deviation) |
| SMOVE                                  | `SMOVE source destination member`       | 1 if moved, 0 otherwise            | O(1)         | Atomic across both keys. Missing source: 0 before the destination is type-checked                             |
| SUNION / SINTER / SDIFF                | `SUNION key [key ...]`                  | array                              | O(total)     | Consistent snapshot of all keys. `SINTER` iterates the smallest set                                           |
| SUNIONSTORE / SINTERSTORE / SDIFFSTORE | `SUNIONSTORE destination key [key ...]` | integer: size of the result        | O(total)     | Replaces the destination (any type, TTL cleared); an empty result deletes it; the destination may be a source |

## Sorted sets

Members are ordered by score, then by member bytes. Scores are 64-bit floats;
`inf`, `+inf` and `-inf` are accepted, NaN is rejected (`ERR value is not a valid
float`). Range bounds for the `BYSCORE` forms may be exclusive with a leading
`(`; a bad bound is `ERR min or max is not a float`.

| Command                          | Syntax                                                                    | Returns                                                                               | Complexity            | Notes                                                                                                                                                                                                                                                                                           |
| -------------------------------- | ------------------------------------------------------------------------- | ------------------------------------------------------------------------------------- | --------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| ZADD                             | `ZADD key [NX\|XX] [GT\|LT] [CH] [INCR] score member [score member ...]`  | integer: added (plus changed with `CH`); with `INCR` the new score, or nil if skipped | O(k log n)            | `GT`/`LT` restrain updates of existing members only. Errors: `XX and NX options at the same time are not compatible`, `GT, LT, and/or NX options at the same time are not compatible`, `INCR option supports a single increment-element pair`. All scores are validated before anything changes |
| ZREM                             | `ZREM key member [member ...]`                                            | integer: removed                                                                      | O(k log n)            | Removing the last member deletes the key                                                                                                                                                                                                                                                        |
| ZSCORE / ZMSCORE                 | `ZMSCORE key member [member ...]`                                         | bulk score or nil, or an array of them                                                | O(k)                  |                                                                                                                                                                                                                                                                                                 |
| ZINCRBY                          | `ZINCRBY key increment member`                                            | bulk: new score                                                                       | O(log n)              | `inf + -inf`: `ERR resulting score is not a number (NaN)`, nothing changes                                                                                                                                                                                                                      |
| ZCARD                            | `ZCARD key`                                                               | integer                                                                               | O(1)                  |                                                                                                                                                                                                                                                                                                 |
| ZCOUNT                           | `ZCOUNT key min max`                                                      | integer                                                                               | O(log n)              |                                                                                                                                                                                                                                                                                                 |
| ZRANK / ZREVRANK                 | `ZRANK key member [WITHSCORE]`                                            | integer or nil; with `WITHSCORE` `[rank, score]` or a null array                      | O(log n)              | 0-based                                                                                                                                                                                                                                                                                         |
| ZRANGE                           | `ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset count] [WITHSCORES]` | array; with `WITHSCORES` flat member/score pairs                                      | O(log n + m)          | `BYSCORE` takes score bounds; with `REV` the first bound is the maximum. `LIMIT` only with `BYSCORE`. **`BYLEX` is not supported** (`ERR syntax error`)                                                                                                                                         |
| ZREVRANGE                        | `ZREVRANGE key start stop [WITHSCORES]`                                   | array                                                                                 | O(log n + m)          |                                                                                                                                                                                                                                                                                                 |
| ZRANGEBYSCORE / ZREVRANGEBYSCORE | `ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]`             | array                                                                                 | O(log n + offset + m) | Negative count means all; negative offset returns empty. `ZREVRANGEBYSCORE` takes `max min`                                                                                                                                                                                                     |
| ZREMRANGEBYRANK                  | `ZREMRANGEBYRANK key start stop`                                          | integer: removed                                                                      | O(log n + m)          |                                                                                                                                                                                                                                                                                                 |
| ZREMRANGEBYSCORE                 | `ZREMRANGEBYSCORE key min max`                                            | integer: removed                                                                      | O(log n + m)          |                                                                                                                                                                                                                                                                                                 |
| ZPOPMIN / ZPOPMAX                | `ZPOPMIN key [count]`                                                     | flat member/score array                                                               | O(log n + m)          | Count must be non-negative (non-integer: `not an integer`; negative: `must be positive`); 0 or a missing key gives an empty array                                                                                                                                                               |

Scores are printed as the shortest decimal that round-trips (`1.5`, `3`,
`1e+21`, `1e-7`; Redis 7 style), `inf` and `-inf` for infinities.

## Keys

| Command   | Syntax                                                  | Returns                                                        | Complexity            | Notes                                                                                                                                  |
| --------- | ------------------------------------------------------- | -------------------------------------------------------------- | --------------------- | -------------------------------------------------------------------------------------------------------------------------------------- |
| DEL       | `DEL key [key ...]`                                     | integer: keys removed                                          | O(k)                  | Atomic per key; duplicates counted once                                                                                                |
| UNLINK    | `UNLINK key [key ...]`                                  | integer: keys removed                                          | O(k)                  | Same as `DEL` (memory is freed synchronously)                                                                                          |
| EXISTS    | `EXISTS key [key ...]`                                  | integer: keys present                                          | O(k)                  | Duplicates counted each time (as Redis)                                                                                                |
| TOUCH     | `TOUCH key [key ...]`                                   | integer: keys present                                          | O(k)                  | No idle-time tracking, so identical to `EXISTS`                                                                                        |
| TYPE      | `TYPE key`                                              | simple string: `string`, `list`, `hash`, `set`, `zset`, `none` | O(1)                  |                                                                                                                                        |
| RENAME    | `RENAME key newkey`                                     | `+OK`                                                          | O(1)                  | Moves value and TTL atomically; replaces any destination and discards its TTL; same name is a no-op; missing source: `ERR no such key` |
| RENAMENX  | `RENAMENX key newkey`                                   | 1 if renamed, 0 if `newkey` exists                             | O(1)                  | An expired destination counts as free                                                                                                  |
| RANDOMKEY | `RANDOMKEY`                                             | bulk or nil                                                    | O(S)                  | Uniform over keys up to concurrent change; skips expired keys                                                                          |
| KEYS      | `KEYS pattern`                                          | array of bulk                                                  | O(N)                  | Redis glob: `* ? [abc] [a-z] [^a] \`. Does not block other shards; not a point-in-time snapshot                                        |
| SCAN      | `SCAN cursor [MATCH pattern] [COUNT count] [TYPE type]` | `[next-cursor, [keys...]]`; cursor `"0"` ends                  | O(count + S) per call | See below                                                                                                                              |

### SCAN

Start with cursor `0`; call again with the returned cursor until it is `0`.

- Every key that exists, unexpired, for the whole iteration is returned at least
  once. Keys may be returned more than once; keys created or deleted meanwhile
  may or may not appear; expired keys are skipped.
- `COUNT` (default 10, at least 1) is the number of key slots examined per call,
  not the number returned. `MATCH` and `TYPE` filter the batch afterwards, so a
  reply can be empty with a non-zero cursor. `TYPE` matches the names `TYPE`
  prints, case-insensitively; an unknown name matches nothing.
- Cursors are opaque decimal numbers valid only for this server's shard layout.
  A cursor that names no shard is `ERR invalid cursor`, as is anything that is
  not an unsigned 64-bit decimal; a stale but well-formed cursor is accepted.
  `COUNT 0` and negative counts are `ERR syntax error`.

## Errors

- Unknown command: `ERR unknown command 'foo', with args beginning with: 'a' 'b' `.
- Wrong arity: `ERR wrong number of arguments for 'get' command`.
- Wrong kind of value: `WRONGTYPE Operation against a key holding the wrong kind of value`.
- Non-integer or float arguments: `ERR value is not an integer or out of range`,
  `ERR value is not a valid float`.

## Deviations from Redis

- `ZRANGE ... BYLEX`, `ZRANGESTORE`, `ZUNION`/`ZINTER`/`ZDIFF`, `ZLEXCOUNT`,
  `ZRANDMEMBER`, `ZSCAN`, `HSCAN`, `SSCAN`, `HMSET`, `HRANDFIELD`, `LMOVE`,
  `LPOS`, `SINTERCARD` and the blocking list commands are not implemented.
- Sorted-set scores print in shortest round-trip form; exponent notation appears
  only below `1e-6` and from `1e21`.
- `SCAN` cursors are not Redis cursors; `COUNT` counts slots, not keys.
- `DBSIZE` excludes expired keys that have not been reclaimed yet; it is not O(1).
- `SRANDMEMBER` with a negative count is capped at 1,048,576 results.
- Hash and set iteration order is insertion order (disturbed by deletions).
- `STRLEN` is O(n); `LINDEX` and `LSET` are O(1) (Redis: O(n)).
- `UNLINK` is synchronous; `TOUCH` tracks no idle time.
- `RANDOMKEY` can cost more than O(S) when many expired keys are not yet
  reclaimed.
- `COMMAND COUNT` and `COMMAND DOCS` are empty-array shims.
- Inline commands work for telnet/netcat; quoting is not supported.
- RESP3 and `HELLO` are not supported.
