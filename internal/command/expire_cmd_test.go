package command

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestExpireConditions(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(0), "EXPIRE", "e", "10"), // missing key
		do(rOK, "SET", "e", "v"),
		do(rInt(1), "EXPIRE", "e", "10"),
		do(rInt(10), "TTL", "e"),
		do(rInt(0), "EXPIRE", "e", "10", "NX"), // already has a TTL
		do(rInt(1), "EXPIRE", "e", "20", "XX"),
		do(rInt(20000), "PTTL", "e"),

		do(rInt(0), "EXPIRE", "e", "15", "GT"), // 15 < 20
		do(rInt(0), "EXPIRE", "e", "20", "GT"), // equal is not greater
		do(rInt(1), "EXPIRE", "e", "25", "GT"),
		do(rInt(25000), "PTTL", "e"),
		do(rInt(0), "EXPIRE", "e", "30", "LT"), // 30 > 25
		do(rInt(0), "EXPIRE", "e", "25", "LT"), // equal is not less
		do(rInt(1), "EXPIRE", "e", "5", "LT"),
		do(rInt(5000), "PTTL", "e"),

		do(rInt(1), "EXPIRE", "e", "8", "XX", "GT"), // XX combines with GT or LT
		do(rInt(8000), "PTTL", "e"),
		do(rInt(1), "EXPIRE", "e", "3", "xx", "lt"),
		do(rInt(3000), "PTTL", "e"),

		// A key with no TTL behaves as if its TTL were infinite.
		do(rInt(1), "PERSIST", "e"),
		do(rInt(0), "EXPIRE", "e", "10", "XX"),
		do(rInt(0), "EXPIRE", "e", "10", "GT"),
		do(rInt(1), "EXPIRE", "e", "10", "LT"),
		do(rInt(10000), "PTTL", "e"),
		do(rInt(1), "PERSIST", "e"),
		do(rInt(1), "EXPIRE", "e", "10", "NX"),
		do(rInt(10000), "PTTL", "e"),

		do(rInt(0), "EXPIRE", "nokey", "10", "NX"), // conditions never create anything
		do(rInt(0), "EXPIRE", "nokey", "10", "LT"),
	})
}

func TestExpireOptionErrors(t *testing.T) {
	h := newHarness(t)
	const (
		nxConflict = "ERR NX and XX, GT or LT options at the same time are not compatible"
		gtlt       = "ERR GT and LT options at the same time are not compatible"
	)
	h.run(t, []step{
		do(rOK, "SET", "e", "v"),
		do(rErr(nxConflict), "EXPIRE", "e", "10", "NX", "XX"),
		do(rErr(nxConflict), "EXPIRE", "e", "10", "NX", "GT"),
		do(rErr(nxConflict), "EXPIRE", "e", "10", "LT", "NX"),
		do(rErr(gtlt), "EXPIRE", "e", "10", "GT", "LT"),
		do(rErr("ERR Unsupported option BOGUS"), "EXPIRE", "e", "10", "BOGUS"),
		do(rErr("ERR Unsupported option bogus"), "PEXPIRE", "e", "10", "bogus"), // echoed verbatim
		// Options are parsed before the integer, as in Redis.
		do(rErr("ERR Unsupported option BOGUS"), "EXPIRE", "e", "abc", "BOGUS"),
		do(rNotInt, "EXPIRE", "e", "abc"),
		do(rNotInt, "EXPIRE", "e", "1.5"),
		do(rErr("ERR wrong number of arguments for 'expire' command"), "EXPIRE", "e"),
		do(rInt(-1), "TTL", "e"), // nothing above changed the key
	})
}

func TestExpireVariantsAndPastDeadlines(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "e", "v"),
		do(rInt(1), "PEXPIRE", "e", "1500"),
		do(rInt(1500), "PTTL", "e"),
		do(rInt(2), "TTL", "e"),
		do(rInt(1), "EXPIREAT", "e", "1700000100"),
		do(rInt(100000), "PTTL", "e"),
		do(rInt(1), "PEXPIREAT", "e", "1700000200500"),
		do(rInt(200500), "PTTL", "e"),

		// A deadline that is not in the future deletes the key and replies 1.
		do(rInt(1), "EXPIRE", "e", "0"),
		do(rInt(0), "EXISTS", "e"),
		do(rInt(0), "EXPIRE", "e", "10"), // gone for good
		do(rOK, "SET", "e", "v", "EX", "100"),
		do(rInt(1), "EXPIRE", "e", "-1"),
		do(rInt(0), "EXISTS", "e"),
		do(rOK, "SET", "e", "v"),
		do(rInt(1), "EXPIREAT", "e", "1"),
		do(rInt(0), "EXISTS", "e"),
		do(rOK, "SET", "e", "v"),
		do(rInt(1), "PEXPIREAT", "e", "1700000000000"), // exactly now: not in the future
		do(rInt(0), "EXISTS", "e"),
		do(rOK, "SET", "e", "v"),
		do(rInt(1), "PEXPIRE", "e", "-9223372036854775808"), // legal: no overflow, deletes
		do(rInt(0), "EXISTS", "e"),
	})
}

func TestExpireOverflow(t *testing.T) {
	h := newHarness(t)
	big := fmt.Sprint(int64(math.MaxInt64))
	h.run(t, []step{
		do(rOK, "SET", "e", "v"),
		do(rBadExpire("expire"), "EXPIRE", "e", big),                    // seconds -> ms overflows
		do(rBadExpire("expire"), "EXPIRE", "e", "-9223372036854775808"), // ... and underflows
		do(rBadExpire("pexpire"), "PEXPIRE", "e", big),                  // now + ttl overflows
		do(rBadExpire("expireat"), "EXPIREAT", "e", big),                // seconds -> ms overflows
		do(rBulk("v"), "GET", "e"),                                      // errors leave the key alone
		do(rInt(-1), "TTL", "e"),
		do(rInt(1), "PEXPIREAT", "e", big), // the largest absolute ms timestamp is legal
		do(rInt(math.MaxInt64), "PEXPIRETIME", "e"),
		do(rInt(9223372036854776), "EXPIRETIME", "e"), // rounding must not overflow
	})
}

func TestTTLFamilyOnMissingAndPersistentKeys(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(-2), "TTL", "nokey"),
		do(rInt(-2), "PTTL", "nokey"),
		do(rInt(-2), "EXPIRETIME", "nokey"),
		do(rInt(-2), "PEXPIRETIME", "nokey"),
		do(rInt(0), "PERSIST", "nokey"),
		do(rOK, "SET", "k", "v"),
		do(rInt(-1), "TTL", "k"),
		do(rInt(-1), "PTTL", "k"),
		do(rInt(-1), "EXPIRETIME", "k"),
		do(rInt(-1), "PEXPIRETIME", "k"),
		do(rInt(0), "PERSIST", "k"), // nothing to remove
		do(rInt(1), "EXPIRE", "k", "10"),
		do(rInt(1), "PERSIST", "k"),
		do(rInt(0), "PERSIST", "k"),
		do(rInt(-1), "TTL", "k"),
		do(rErr("ERR wrong number of arguments for 'ttl' command"), "TTL"),
	})
}

// Expired keys must disappear from every read path, and writes that keep the
// key (INCR, APPEND) must keep its TTL while MSET and SET discard it.
func TestExpiryAcrossCommands(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "c", "1", "EX", "10"),
		do(rInt(2), "INCR", "c"),
		do(rInt(10), "TTL", "c"),
		do(rInt(2), "APPEND", "c", "x"),
		do(rInt(10), "TTL", "c"),
		do(rOK, "MSET", "c", "1", "d", "2"),
		do(rInt(-1), "TTL", "c"),

		do(rOK, "SET", "t", "v", "PX", "100"),
		do(rInt(3), "DBSIZE"),
		wait(101 * time.Millisecond),
		do(rInt(2), "DBSIZE"), // expired keys are not counted
		do(rInt(0), "EXISTS", "t"),
		do(rNil, "GET", "t"),
		do(rInt(0), "STRLEN", "t"),
		do("*0\r\n", "KEYS", "t*"),
		do("*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n", "MGET", "c", "t", "d"),
		do(rInt(0), "DEL", "t"),

		// INCR on an expired key starts from zero and has no TTL.
		do(rOK, "SET", "i", "5", "PX", "10"),
		wait(11 * time.Millisecond),
		do(rInt(1), "INCR", "i"),
		do(rInt(-1), "TTL", "i"),
	})
}
