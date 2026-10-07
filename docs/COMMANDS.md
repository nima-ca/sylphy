# Commands

Complexity: _n_ = bytes in the value, _k_ = number of keys given, _N_ = keys in the database.

| Command         | Syntax                                              | Returns                | Complexity | Notes                                                                                           |
| --------------- | --------------------------------------------------- | ---------------------- | ---------- | ----------------------------------------------------------------------------------------------- |
| PING            | `PING [msg]`                                        | `+PONG`, or bulk `msg` | O(1)       |                                                                                                 |
| ECHO            | `ECHO msg`                                          | bulk `msg`             | O(1)       |                                                                                                 |
| SET             | `SET key value`                                     | `+OK`                  | O(n)       | Options (`EX`, `NX`, ...) return `ERR syntax error` until Phase 2                               |
| GET             | `GET key`                                           | bulk or null bulk      | O(n)       | Value is copied                                                                                 |
| DEL             | `DEL key [key ...]`                                 | integer: keys removed  | O(k)       | Atomic per key; duplicates counted once                                                         |
| EXISTS          | `EXISTS key [key ...]`                              | integer: keys present  | O(k)       | Duplicates counted each time (as Redis)                                                         |
| KEYS            | `KEYS pattern`                                      | array of bulk          | O(N)       | Redis glob: `* ? [abc] [a-z] [^a] \`. Does not block other shards; not a point-in-time snapshot |
| DBSIZE          | `DBSIZE`                                            | integer                | O(shards)  |                                                                                                 |
| FLUSHALL        | `FLUSHALL [ASYNC\|SYNC]`                            | `+OK`                  | O(shards)  | Both modes behave identically                                                                   |
| INCR / DECR     | `INCR key`                                          | integer: new value     | O(1)       | Missing key = 0                                                                                 |
| INCRBY / DECRBY | `INCRBY key n`                                      | integer: new value     | O(1)       | Strict int64 parsing                                                                            |
| APPEND          | `APPEND key value`                                  | integer: new length    | O(n)       |                                                                                                 |
| STRLEN          | `STRLEN key`                                        | integer                | O(n)       | Redis is O(1); Sylphy copies the value (deviation)                                              |
| MSET            | `MSET key value [key value ...]`                    | `+OK`                  | O(k)       | **Atomic per key, not across keys**                                                             |
| MGET            | `MGET key [key ...]`                                | array of bulk/null     | O(k)       | **Atomic per key, not across keys**                                                             |
| QUIT            | `QUIT`                                              | `+OK`, then close      | O(1)       |                                                                                                 |
| SELECT          | `SELECT 0`                                          | `+OK`                  | O(1)       | Other indexes: `ERR DB index is out of range`                                                   |
| CLIENT          | `CLIENT SETNAME name` / `CLIENT SETINFO attr value` | `+OK`                  | O(1)       | Other subcommands: `ERR unknown subcommand`                                                     |
| COMMAND         | `COMMAND`, `COMMAND DOCS`, `COMMAND COUNT`          | empty array            | O(1)       | Shim only. `COMMAND COUNT` returns an empty array rather than an integer (deviation)            |

## Integer rules (INCR family)

Values are parsed as signed 64-bit decimal: no whitespace, no `+` prefix, no
leading zeros (except `0`), no `-0`. Failures reply
`ERR value is not an integer or out of range`; overflow replies
`ERR increment or decrement would overflow`. `DECRBY key -9223372036854775808`
also replies with the overflow error.

## Other behavior

- Command names are case-insensitive.
- Inline commands (`PING\r\n`) work for telnet/netcat; quoting is not supported.
- Unknown command: `ERR unknown command 'foo', with args beginning with: 'a' 'b' `.
- Wrong arity: `ERR wrong number of arguments for 'get' command`.
