package command

import (
	"fmt"
	"math"
	"testing"
	"time"
)

func TestSetTTLOptions(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "k", "v", "EX", "10"),
		do(rInt(10), "TTL", "k"),
		do(rInt(10000), "PTTL", "k"),
		wait(4 * time.Second),
		do(rInt(6), "TTL", "k"),
		do(rInt(6000), "PTTL", "k"),
		// now == deadline: still alive with 0 left, as in Redis.
		wait(6 * time.Second),
		do(rBulk("v"), "GET", "k"),
		do(rInt(0), "PTTL", "k"),
		do(rInt(0), "TTL", "k"),
		wait(time.Millisecond),
		do(rNil, "GET", "k"),
		do(rInt(-2), "TTL", "k"),
		do(rInt(0), "EXISTS", "k"),
		do(rInt(0), "DBSIZE"),

		// TTL rounds half up to whole seconds; PTTL is exact.
		do(rOK, "SET", "p", "v", "PX", "1499"),
		do(rInt(1), "TTL", "p"),
		do(rOK, "SET", "p", "v", "PX", "1500"),
		do(rInt(2), "TTL", "p"),
		do(rInt(1500), "PTTL", "p"),
	})
}

func TestSetAbsoluteDeadlines(t *testing.T) {
	h := newHarness(t) // the fake clock starts at 1700000000 s
	h.run(t, []step{
		do(rOK, "SET", "a", "v", "EXAT", "1700000100"),
		do(rInt(100000), "PTTL", "a"),
		do(rInt(1700000100), "EXPIRETIME", "a"),
		do(rInt(1700000100000), "PEXPIRETIME", "a"),
		do(rOK, "SET", "b", "v", "pxat", "1700000050500"), // option names are case-insensitive
		do(rInt(50500), "PTTL", "b"),
		do(rInt(1700000051), "EXPIRETIME", "b"), // rounds half up
	})
}

func TestSetKeepTTLAndOverwrite(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "c", "1", "EX", "100"),
		wait(10 * time.Second),
		do(rOK, "SET", "c", "2", "KEEPTTL"),
		do(rInt(90000), "PTTL", "c"),
		do(rBulk("2"), "GET", "c"),
		do(rOK, "SET", "c", "3"), // plain SET discards the TTL
		do(rInt(-1), "TTL", "c"),
		do(rOK, "SET", "c", "4", "EX", "50"),
		do(rOK, "SET", "c", "5", "EX", "100"), // a new EX replaces the old deadline
		do(rInt(100000), "PTTL", "c"),
		do(rOK, "SET", "d", "x", "KEEPTTL"), // KEEPTTL on a new key: no TTL
		do(rInt(-1), "TTL", "d"),
	})
}

func TestSetNXXXGet(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rNil, "SET", "x", "v", "XX"),
		do(rInt(0), "EXISTS", "x"),
		do(rOK, "SET", "n", "v", "NX"),
		do(rNil, "SET", "n", "other", "NX"),
		do(rBulk("v"), "GET", "n"),
		do(rOK, "SET", "n", "v2", "xx"),
		do(rBulk("v2"), "GET", "n"),
	})
	// Real GET cases: GET returns the old value (or nil) and combines with NX/XX.
	h.run(t, []step{
		do(rNil, "SET", "g", "a", "GET"), // missing: nil, but the value is still set
		do(rBulk("a"), "GET", "g"),
		do(rBulk("a"), "SET", "g", "b", "GET"),
		do(rBulk("b"), "SET", "g", "c", "NX", "GET"), // NX fails, old value returned, nothing written
		do(rBulk("b"), "GET", "g"),
		do(rNil, "SET", "g2", "z", "XX", "GET"), // XX fails on a missing key
		do(rInt(0), "EXISTS", "g2"),
		do(rBulk("b"), "SET", "g", "d", "XX", "GET"), // XX succeeds
		do(rBulk("d"), "GET", "g"),
		do(rBulk("d"), "SET", "g", "e", "GET", "EX", "10"),
		do(rInt(10), "TTL", "g"),
	})
}

func TestSetDeadlineInThePastDeletes(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "gone", "v", "EXAT", "1"),
		do(rInt(0), "EXISTS", "gone"),
		do(rOK, "SET", "old", "v1"),
		do(rBulk("v1"), "SET", "old", "v2", "GET", "EXAT", "1"), // old value is still returned
		do(rInt(0), "EXISTS", "old"),
		do(rOK, "SET", "px", "v", "PXAT", "1"),
		do(rInt(0), "EXISTS", "px"),
	})
}

func TestSetSyntaxAndValueErrors(t *testing.T) {
	h := newHarness(t)
	big := fmt.Sprint(int64(math.MaxInt64))
	h.run(t, []step{
		do(rOK, "SET", "keep", "orig"),
		do(rSyntax, "SET", "keep", "new", "NX", "XX"),
		do(rSyntax, "SET", "keep", "new", "XX", "NX"),
		do(rSyntax, "SET", "keep", "new", "EX", "10", "PX", "100"),
		do(rSyntax, "SET", "keep", "new", "EX", "10", "EX", "20"),
		do(rSyntax, "SET", "keep", "new", "KEEPTTL", "EX", "10"),
		do(rSyntax, "SET", "keep", "new", "EX", "10", "KEEPTTL"),
		do(rSyntax, "SET", "keep", "new", "EX"), // missing value
		do(rSyntax, "SET", "keep", "new", "FOO"),
		do(rSyntax, "SET", "keep", "new", "PERSIST"),
		// Conflicts are reported before the value is validated, as in Redis.
		do(rSyntax, "SET", "keep", "new", "EX", "abc", "PX", "5"),
		do(rNotInt, "SET", "keep", "new", "EX", "abc"),
		do(rNotInt, "SET", "keep", "new", "PX", "1.5"),
		do(rBadExpire("set"), "SET", "keep", "new", "EX", "0"),
		do(rBadExpire("set"), "SET", "keep", "new", "PX", "-5"),
		do(rBadExpire("set"), "SET", "keep", "new", "EXAT", "0"),
		do(rBadExpire("set"), "SET", "keep", "new", "EX", big),   // seconds -> ms overflows
		do(rBadExpire("set"), "SET", "keep", "new", "PX", big),   // now + ttl overflows
		do(rBadExpire("set"), "SET", "keep", "new", "EXAT", big), // seconds -> ms overflows
		do(rBulk("orig"), "GET", "keep"),                         // no failed SET touched the key
		do(rInt(-1), "TTL", "keep"),

		// The largest absolute millisecond timestamp is legal and must not
		// overflow any TTL arithmetic.
		do(rOK, "SET", "far", "v", "PXAT", big),
		do(rInt(math.MaxInt64), "PEXPIRETIME", "far"),
		do(rInt(9223372036854776), "EXPIRETIME", "far"),
		do(rInt(math.MaxInt64-testEpochMs), "PTTL", "far"),
	})
}

func TestSetNX(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(1), "SETNX", "s", "1"),
		do(rInt(0), "SETNX", "s", "2"),
		do(rBulk("1"), "GET", "s"),
		do(rOK, "SET", "t", "v", "EX", "10"),
		do(rInt(0), "SETNX", "t", "w"), // an existing key keeps value and TTL
		do(rInt(10), "TTL", "t"),
		wait(11 * time.Second),
		do(rInt(1), "SETNX", "t", "w"), // expired counts as missing
		do(rBulk("w"), "GET", "t"),
		do(rInt(-1), "TTL", "t"),
	})
}

func TestSetExAndPSetEx(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SETEX", "e", "10", "v"),
		do(rBulk("v"), "GET", "e"),
		do(rInt(10), "TTL", "e"),
		do(rOK, "PSETEX", "p", "1500", "v"),
		do(rInt(1500), "PTTL", "p"),
		do(rOK, "SETEX", "e", "20", "w"), // replaces value and deadline
		do(rInt(20000), "PTTL", "e"),
		do(rBadExpire("setex"), "SETEX", "e", "0", "v"),
		do(rBadExpire("setex"), "SETEX", "e", "-1", "v"),
		do(rBadExpire("setex"), "SETEX", "e", fmt.Sprint(int64(math.MaxInt64)), "v"),
		do(rBadExpire("psetex"), "PSETEX", "p", "0", "v"),
		do(rBadExpire("psetex"), "PSETEX", "p", fmt.Sprint(int64(math.MaxInt64)), "v"),
		do(rNotInt, "SETEX", "e", "abc", "v"),
		do(rNotInt, "PSETEX", "p", "1.5", "v"),
		do(rBulk("w"), "GET", "e"), // failures changed nothing
		do(rInt(20000), "PTTL", "e"),
		do(rErr("ERR wrong number of arguments for 'setex' command"), "SETEX", "e", "10"),
	})
}

func TestGetEx(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "g", "v"),
		do(rBulk("v"), "GETEX", "g"), // no options: plain read
		do(rInt(-1), "TTL", "g"),
		do(rBulk("v"), "GETEX", "g", "EX", "10"),
		do(rInt(10), "TTL", "g"),
		do(rBulk("v"), "GETEX", "g", "px", "1500"),
		do(rInt(1500), "PTTL", "g"),
		do(rBulk("v"), "GETEX", "g", "EXAT", "1700000100"),
		do(rInt(100000), "PTTL", "g"),
		do(rBulk("v"), "GETEX", "g", "PXAT", "1700000200500"),
		do(rInt(200500), "PTTL", "g"),
		do(rBulk("v"), "GETEX", "g", "PERSIST"),
		do(rInt(-1), "TTL", "g"),

		do(rNil, "GETEX", "missing"),
		do(rNil, "GETEX", "missing", "EX", "10"),
		do(rInt(0), "EXISTS", "missing"),

		// Options are validated before the key is looked up.
		do(rBadExpire("getex"), "GETEX", "missing", "EX", "0"),
		do(rBadExpire("getex"), "GETEX", "g", "PX", "-1"),
		do(rBadExpire("getex"), "GETEX", "g", "EX", fmt.Sprint(int64(math.MaxInt64))),
		do(rNotInt, "GETEX", "g", "EX", "abc"),
		do(rSyntax, "GETEX", "g", "EX"),
		do(rSyntax, "GETEX", "g", "EX", "10", "PX", "100"),
		do(rSyntax, "GETEX", "g", "PERSIST", "EX", "10"),
		do(rSyntax, "GETEX", "g", "EX", "10", "PERSIST"),
		do(rSyntax, "GETEX", "g", "PERSIST", "PERSIST"),
		do(rSyntax, "GETEX", "g", "KEEPTTL"),

		// A deadline in the past returns the value and deletes the key.
		do(rBulk("v"), "GETEX", "g", "EXAT", "1"),
		do(rInt(0), "EXISTS", "g"),
	})
}

func TestGetDel(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "g", "v", "EX", "10"),
		do(rBulk("v"), "GETDEL", "g"),
		do(rNil, "GETDEL", "g"),
		do(rInt(0), "EXISTS", "g"),
		do(rOK, "SET", "e", "v", "PX", "5"),
		wait(6 * time.Millisecond),
		do(rNil, "GETDEL", "e"), // expired counts as missing
		do(rOK, "SET", "empty", ""),
		do(rBulk(""), "GETDEL", "empty"),
	})
}
