package command

import (
	"testing"
	"time"
)

const (
	rNotFloatRange = "-ERR min or max is not a float\r\n"
	rNaNScore      = "-ERR resulting score is not a number (NaN)\r\n"
)

func TestZSetCommandsRegistered(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"ZADD", "ZREM", "ZSCORE", "ZMSCORE", "ZINCRBY", "ZCARD", "ZCOUNT", "ZRANK",
		"ZREVRANK", "ZRANGE", "ZREVRANGE", "ZRANGEBYSCORE", "ZREVRANGEBYSCORE", "ZREMRANGEBYRANK",
		"ZREMRANGEBYSCORE", "ZPOPMIN", "ZPOPMAX"} {
		if _, ok := r.Lookup([]byte(name)); !ok {
			t.Errorf("missing command %s", name)
		}
	}
}

func TestZAddBasicsAndCH(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "ZADD", "z", "1", "a", "2", "b", "3", "c"),
		do(rInt(0), "ZADD", "z", "1", "a"),                 // unchanged
		do(rInt(0), "ZADD", "z", "5", "a"),                 // updated, not counted without CH
		do(rInt(2), "ZADD", "z", "CH", "6", "a", "4", "d"), // one changed, one added
		do(rInt(0), "ZADD", "z", "ch", "6", "a"),
		do(rInt(4), "ZCARD", "z"),
		do(rArray("b", "c", "d", "a"), "ZRANGE", "z", "0", "-1"),
		do(rArray("b", "2", "c", "3", "d", "4", "a", "6"), "ZRANGE", "z", "0", "-1", "WITHSCORES"),
		// Duplicates inside one command are applied in order.
		do(rInt(1), "ZADD", "dup", "1", "a", "2", "a"),
		do(rBulk("2"), "ZSCORE", "dup", "a"),
		do(rInt(2), "ZADD", "dup2", "CH", "1", "a", "2", "a"),
		// Equal scores order by member bytes.
		do(rInt(3), "ZADD", "ties", "1", "b", "1", "a", "1", "c"),
		do(rArray("a", "b", "c"), "ZRANGE", "ties", "0", "-1"),
		// Infinities in all spellings; members are binary-safe.
		do(rInt(3), "ZADD", "inf", "+inf", "p", "-inf", "n", "inf", "q"),
		do(rBulk("inf"), "ZSCORE", "inf", "p"),
		do(rBulk("-inf"), "ZSCORE", "inf", "n"),
		do(rArray("n", "p", "q"), "ZRANGE", "inf", "0", "-1"),
		do(rInt(1), "ZADD", "bin", "1", "a\x00b\xff"),
		do(rArray("a\x00b\xff"), "ZRANGE", "bin", "0", "-1"),
	})
}

func TestZAddConditions(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "ZADD", "g", "10", "a", "20", "b"),
		do(rInt(0), "ZADD", "g", "GT", "5", "a"), // lower: refused
		do(rBulk("10"), "ZSCORE", "g", "a"),
		do(rInt(1), "ZADD", "g", "GT", "CH", "15", "a"),
		do(rBulk("15"), "ZSCORE", "g", "a"),
		do(rInt(2), "ZADD", "g", "LT", "CH", "12", "a", "99", "new"), // LT still adds new members
		do(rBulk("12"), "ZSCORE", "g", "a"),
		do(rBulk("99"), "ZSCORE", "g", "new"),
		do(rInt(1), "ZADD", "g", "NX", "1", "a", "1", "fresh"), // a untouched, fresh added
		do(rBulk("12"), "ZSCORE", "g", "a"),
		do(rInt(0), "ZADD", "g", "XX", "7", "nothere", "8", "a"), // updates are not counted without CH
		do(rBulk("8"), "ZSCORE", "g", "a"),
		do(rNil, "ZSCORE", "g", "nothere"),
		do(rInt(1), "ZADD", "g", "XX", "CH", "9", "a"),
		do(rInt(0), "ZADD", "g", "GT", "8", "b"), // 8 <= 20
		// XX on a missing key must not create it.
		do(rInt(0), "ZADD", "gx", "XX", "1", "a"),
		do(rInt(0), "EXISTS", "gx"),
	})
}

func TestZAddIncr(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rBulk("5"), "ZADD", "i", "INCR", "5", "a"),
		do(rBulk("7.5"), "ZADD", "i", "INCR", "2.5", "a"),
		do(rNil, "ZADD", "i", "INCR", "NX", "1", "a"),
		do(rNil, "ZADD", "i", "INCR", "XX", "1", "b"),
		do(rNil, "ZADD", "i", "INCR", "GT", "-1", "a"), // 6.5 is not greater than 7.5
		do(rBulk("6.5"), "ZADD", "i", "INCR", "LT", "-1", "a"),
		do(rBulk("6.5"), "ZADD", "i", "INCR", "0", "a"), // unchanged but processed
		do(rBulk("7.5"), "ZADD", "i", "INCR", "XX", "1", "a"),
		do(rErr("ERR INCR option supports a single increment-element pair"), "ZADD", "i", "INCR", "1", "a", "2", "b"),
		do(rBulk("inf"), "ZADD", "ni", "INCR", "inf", "a"),
		do(rNaNScore, "ZADD", "ni", "INCR", "-inf", "a"),
		do(rBulk("inf"), "ZSCORE", "ni", "a"),
		do(rNil, "ZADD", "nokey", "INCR", "XX", "1", "a"),
		do(rInt(0), "EXISTS", "nokey"),
	})
}

func TestZAddErrors(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rErr("ERR XX and NX options at the same time are not compatible"), "ZADD", "z", "NX", "XX", "1", "a"),
		do(rErr("ERR GT, LT, and/or NX options at the same time are not compatible"), "ZADD", "z", "GT", "LT", "1", "a"),
		do(rErr("ERR GT, LT, and/or NX options at the same time are not compatible"), "ZADD", "z", "GT", "NX", "1", "a"),
		do(rErr("ERR GT, LT, and/or NX options at the same time are not compatible"), "ZADD", "z", "LT", "NX", "1", "a"),
		do(rErr("ERR wrong number of arguments for 'zadd' command"), "ZADD", "z", "1"),
		do(rSyntax, "ZADD", "z", "1", "a", "2"),
		do(rSyntax, "ZADD", "z", "NX", "1"),
		do(rSyntax, "ZADD", "z", "NX", "XX"), // options only, no pairs (and the pair check wins)
		do(rNotFloat, "ZADD", "z", "abc", "a"),
		do(rNotFloat, "ZADD", "z", "nan", "a"),
		do(rNotFloat, "ZADD", "z", "1e999", "a"),
		// A bad score anywhere in the command changes nothing.
		do(rNotFloat, "ZADD", "z", "1", "x", "abc", "y"),
		do(rNil, "ZSCORE", "z", "x"),
		do(rInt(0), "EXISTS", "z"),
	})
}

func TestZScoreFormats(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(6), "ZADD", "f", "1.5", "a", "3", "b", "0.1", "c", "1e21", "d", "1e-7", "e", "1234567", "g"),
		do(rBulk("1.5"), "ZSCORE", "f", "a"),
		do(rBulk("3"), "ZSCORE", "f", "b"),
		do(rBulk("0.1"), "ZSCORE", "f", "c"),
		do(rBulk("1e+21"), "ZSCORE", "f", "d"),
		do(rBulk("1e-7"), "ZSCORE", "f", "e"),
		do(rBulk("1234567"), "ZSCORE", "f", "g"),
		do(rNil, "ZSCORE", "f", "missing"),
		do(rNil, "ZSCORE", "nokey", "a"),
	})
}

func TestZMScore(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "ZADD", "z", "1", "a", "2", "b"),
		do("*3\r\n"+rBulk("1")+rNil+rBulk("2"), "ZMSCORE", "z", "a", "nope", "b"),
		do("*2\r\n"+rNil+rNil, "ZMSCORE", "nokey", "a", "b"),
	})
}

func TestZIncrBy(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rBulk("2"), "ZINCRBY", "zi", "2", "m"),
		do(rBulk("4.5"), "ZINCRBY", "zi", "2.5", "m"),
		do(rBulk("0"), "ZINCRBY", "zi", "-4.5", "m"),
		do(rBulk("1000"), "ZINCRBY", "zi", "1e3", "n"),
		do(rBulk("inf"), "ZINCRBY", "zn", "inf", "m"),
		do(rNaNScore, "ZINCRBY", "zn", "-inf", "m"),
		do(rBulk("inf"), "ZSCORE", "zn", "m"),
		do(rNotFloat, "ZINCRBY", "zi", "abc", "m"),
		do(rNotFloat, "ZINCRBY", "zi", "nan", "m"),
		do(rBulk("1.5"), "ZINCRBY", "fresh", "1.5", "x"), // creates the key
		do(rInt(1), "ZCARD", "fresh"),
	})
}

func TestZRemAndEmptyDeletion(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "ZADD", "z", "1", "a", "2", "b", "3", "c"),
		do(rInt(2), "ZREM", "z", "a", "nope", "a", "b"), // a counts once
		do(rInt(0), "ZREM", "nokey", "a"),
		do(rInt(0), "EXISTS", "nokey"),
		do(rInt(1), "EXPIRE", "z", "100"),
		do(rInt(1), "ZREM", "z", "c"),
		do(rInt(0), "EXISTS", "z"), // the empty set is gone
		do(rInt(-2), "TTL", "z"),
		do(rInt(1), "ZADD", "z", "1", "a"),
		do(rInt(-1), "TTL", "z"), // and its TTL did not come back
	})
}

func TestZCardAndZCount(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(0), "ZCARD", "nokey"),
		do(rInt(6), "ZADD", "z", "1", "a", "2", "b", "3", "c", "4", "d", "5", "e", "inf", "z"),
		do(rInt(6), "ZCARD", "z"),
		do(rInt(3), "ZCOUNT", "z", "2", "4"),
		do(rInt(1), "ZCOUNT", "z", "(2", "(4"),
		do(rInt(6), "ZCOUNT", "z", "-inf", "+inf"),
		do(rInt(5), "ZCOUNT", "z", "-inf", "(inf"),
		do(rInt(0), "ZCOUNT", "z", "4", "2"),
		do(rInt(0), "ZCOUNT", "z", "(3", "3"),
		do(rInt(0), "ZCOUNT", "nokey", "-inf", "inf"),
		do(rNotFloatRange, "ZCOUNT", "z", "abc", "1"),
		do(rNotFloatRange, "ZCOUNT", "z", "1", "("),
		do(rNotFloatRange, "ZCOUNT", "z", "1", "nan"),
		do(rNotFloatRange, "ZCOUNT", "nokey", "x", "1"), // validated before the lookup
	})
}

func TestZRank(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "ZADD", "z", "1", "a", "2", "b", "3", "c"),
		do(rInt(2), "ZRANK", "z", "c"),
		do(rInt(0), "ZREVRANK", "z", "c"),
		do(rInt(0), "ZRANK", "z", "a"),
		do(rInt(2), "ZREVRANK", "z", "a"),
		do(rNil, "ZRANK", "z", "nope"),
		do(rNil, "ZREVRANK", "z", "nope"),
		do(rNil, "ZRANK", "nokey", "a"),
		do("*2\r\n"+rInt(1)+rBulk("2"), "ZRANK", "z", "b", "WITHSCORE"),
		do("*2\r\n"+rInt(2)+rBulk("1"), "ZREVRANK", "z", "a", "withscore"),
		do(rNullArray, "ZRANK", "z", "nope", "WITHSCORE"),
		do(rNullArray, "ZREVRANK", "nokey", "a", "WITHSCORE"),
		do(rSyntax, "ZRANK", "z", "b", "BOGUS"),
		do(rSyntax, "ZRANK", "z", "b", "WITHSCORE", "x"),
	})
}

func fiveMembers(h *harness, t *testing.T, key string) {
	t.Helper()
	h.run(t, []step{do(rInt(5), "ZADD", key, "1", "a", "2", "b", "3", "c", "4", "d", "5", "e")})
}

func TestZRange(t *testing.T) {
	h := newHarness(t)
	fiveMembers(h, t, "r")
	h.run(t, []step{
		do(rArray("a", "b", "c", "d", "e"), "ZRANGE", "r", "0", "-1"),
		do(rArray("b", "c"), "ZRANGE", "r", "1", "2"),
		do(rArray("d", "e"), "ZRANGE", "r", "-2", "-1"),
		do(rArray("a", "b"), "ZRANGE", "r", "-100", "1"),
		do(rEmptyArray, "ZRANGE", "r", "3", "1"),
		do(rEmptyArray, "ZRANGE", "r", "10", "20"),
		do(rEmptyArray, "ZRANGE", "nokey", "0", "-1"),
		do(rArray("e", "d", "c", "b", "a"), "ZRANGE", "r", "0", "-1", "REV"),
		do(rArray("e", "d"), "ZRANGE", "r", "0", "1", "rev"),
		do(rArray("e", "5", "d", "4"), "ZRANGE", "r", "0", "1", "REV", "WITHSCORES"),
		do(rArray("a", "1", "b", "2"), "ZRANGE", "r", "0", "1", "WITHSCORES"),
		// BYSCORE
		do(rArray("b", "c", "d"), "ZRANGE", "r", "2", "4", "BYSCORE"),
		do(rArray("c", "d"), "ZRANGE", "r", "(2", "4", "byscore"),
		do(rArray("d", "c", "b"), "ZRANGE", "r", "4", "2", "BYSCORE", "REV"), // REV: max first
		do(rArray("b", "c"), "ZRANGE", "r", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "2"),
		do(rArray("b", "c", "d", "e"), "ZRANGE", "r", "-inf", "+inf", "BYSCORE", "LIMIT", "1", "-1"),
		do(rArray("a", "1", "b", "2"), "ZRANGE", "r", "1", "5", "BYSCORE", "WITHSCORES", "LIMIT", "0", "2"),
		do(rEmptyArray, "ZRANGE", "r", "1", "5", "BYSCORE", "LIMIT", "-1", "2"),
		do(rEmptyArray, "ZRANGE", "r", "1", "5", "BYSCORE", "LIMIT", "9", "2"),
		do(rEmptyArray, "ZRANGE", "r", "2", "4", "BYSCORE", "REV"), // inverted after the swap
		// Errors
		do(rErr("ERR wrong number of arguments for 'zrange' command"), "ZRANGE", "r", "0"),
		do(rNotInt, "ZRANGE", "r", "a", "b"),
		do(rNotFloatRange, "ZRANGE", "r", "a", "b", "BYSCORE"),
		do(rErr("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX"),
			"ZRANGE", "r", "0", "-1", "LIMIT", "0", "1"),
		do(rSyntax, "ZRANGE", "r", "0", "-1", "BYLEX"),
		do(rSyntax, "ZRANGE", "r", "0", "-1", "BOGUS"),
		do(rSyntax, "ZRANGE", "r", "0", "-1", "BYSCORE", "LIMIT", "0"),
		do(rNotInt, "ZRANGE", "r", "0", "-1", "BYSCORE", "LIMIT", "x", "1"),
		do(rNotInt, "ZRANGE", "r", "0", "-1", "BYSCORE", "LIMIT", "0", "y"),
	})
}

func TestZRevRangeAndByScoreForms(t *testing.T) {
	h := newHarness(t)
	fiveMembers(h, t, "r")
	h.run(t, []step{
		do(rArray("e", "d"), "ZREVRANGE", "r", "0", "1"),
		do(rArray("e", "5", "d", "4"), "ZREVRANGE", "r", "0", "1", "WITHSCORES"),
		do(rArray("a"), "ZREVRANGE", "r", "-1", "-1"),
		do(rEmptyArray, "ZREVRANGE", "nokey", "0", "-1"),
		do(rSyntax, "ZREVRANGE", "r", "0", "1", "LIMIT", "0", "1"),

		do(rArray("b", "c", "d"), "ZRANGEBYSCORE", "r", "2", "4"),
		do(rArray("b", "2", "c", "3"), "ZRANGEBYSCORE", "r", "2", "3", "WITHSCORES"),
		do(rArray("c"), "ZRANGEBYSCORE", "r", "2", "4", "LIMIT", "1", "1"),
		do(rArray("b"), "ZRANGEBYSCORE", "r", "(1", "(3"),
		do(rEmptyArray, "ZRANGEBYSCORE", "r", "4", "2"), // no swapping in the legacy form
		do(rSyntax, "ZRANGEBYSCORE", "r", "1", "2", "REV"),
		do(rNotFloatRange, "ZRANGEBYSCORE", "r", "a", "2"),

		do(rArray("d", "c", "b"), "ZREVRANGEBYSCORE", "r", "4", "2"),
		do(rEmptyArray, "ZREVRANGEBYSCORE", "r", "2", "4"),
		do(rArray("e", "d"), "ZREVRANGEBYSCORE", "r", "+inf", "-inf", "LIMIT", "0", "2"),
		do(rArray("e", "5"), "ZREVRANGEBYSCORE", "r", "(6", "(4", "WITHSCORES"),
		do(rEmptyArray, "ZREVRANGEBYSCORE", "nokey", "+inf", "-inf"),
	})
}

func TestZRemRangeByRank(t *testing.T) {
	h := newHarness(t)
	fiveMembers(h, t, "rr")
	h.run(t, []step{
		do(rInt(2), "ZREMRANGEBYRANK", "rr", "1", "2"),
		do(rArray("a", "d", "e"), "ZRANGE", "rr", "0", "-1"),
		do(rInt(1), "ZREMRANGEBYRANK", "rr", "-1", "-1"),
		do(rInt(0), "ZREMRANGEBYRANK", "rr", "5", "9"),
		do(rInt(0), "ZREMRANGEBYRANK", "rr", "1", "0"),
		do(rNotInt, "ZREMRANGEBYRANK", "rr", "x", "1"),
		do(rNotInt, "ZREMRANGEBYRANK", "rr", "0", "y"),
		do(rInt(0), "ZREMRANGEBYRANK", "nokey", "0", "-1"),
		do(rInt(2), "ZREMRANGEBYRANK", "rr", "0", "-1"),
		do(rInt(0), "EXISTS", "rr"),
	})
}

func TestZRemRangeByScore(t *testing.T) {
	h := newHarness(t)
	fiveMembers(h, t, "rs")
	h.run(t, []step{
		do(rInt(2), "ZREMRANGEBYSCORE", "rs", "(1", "3"), // b and c
		do(rArray("a", "d", "e"), "ZRANGE", "rs", "0", "-1"),
		do(rInt(0), "ZREMRANGEBYSCORE", "rs", "50", "60"),
		do(rNotFloatRange, "ZREMRANGEBYSCORE", "rs", "a", "1"),
		do(rInt(0), "ZREMRANGEBYSCORE", "nokey", "-inf", "inf"),
		do(rInt(3), "ZREMRANGEBYSCORE", "rs", "-inf", "+inf"),
		do(rInt(0), "EXISTS", "rs"),
	})
}

func TestZPop(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(4), "ZADD", "zp", "1", "a", "2", "b", "3", "c", "4", "d"),
		do(rArray("a", "1"), "ZPOPMIN", "zp"),
		do(rArray("d", "4"), "ZPOPMAX", "zp"),
		do(rEmptyArray, "ZPOPMIN", "zp", "0"),
		do(rInt(2), "ZCARD", "zp"),
		do(rArray("b", "2", "c", "3"), "ZPOPMIN", "zp", "5"),
		do(rInt(0), "EXISTS", "zp"),
		do(rEmptyArray, "ZPOPMIN", "nokey"),
		do(rEmptyArray, "ZPOPMAX", "nokey", "3"),
		do(rInt(2), "ZADD", "zq", "1", "a", "2", "b"),
		do(rMustBePos, "ZPOPMIN", "zq", "-1"),
		do(rNotInt, "ZPOPMAX", "zq", "x"),
		do(rSyntax, "ZPOPMIN", "zq", "1", "2"),
		do(rArray("b", "2", "a", "1"), "ZPOPMAX", "zq", "9223372036854775807"),
		do(rInt(0), "EXISTS", "zq"),
	})
}

func TestZSetTTLInteraction(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "ZADD", "t", "1", "a", "2", "b"),
		do(rInt(1), "EXPIRE", "t", "100"),
		do(rInt(1), "ZADD", "t", "3", "c"),
		do(rInt(100), "TTL", "t"), // adding keeps the TTL
		do(rArray("a", "1"), "ZPOPMIN", "t"),
		do(rInt(100), "TTL", "t"), // so does a partial pop
		do(rArray("b", "2", "c", "3"), "ZPOPMIN", "t", "2"),
		do(rInt(-2), "TTL", "t"), // emptying removes the key and its TTL
		do(rInt(1), "ZADD", "t", "1", "a"),
		do(rInt(-1), "TTL", "t"),

		// Lazy expiry: an expired set is invisible and a new ZADD starts fresh.
		do(rInt(1), "ZADD", "e", "1", "old"),
		do(rInt(1), "PEXPIRE", "e", "1000"),
		wait(time.Second + time.Millisecond),
		do(rInt(0), "ZCARD", "e"),
		do(rNil, "ZSCORE", "e", "old"),
		do(rEmptyArray, "ZRANGE", "e", "0", "-1"),
		do(rInt(1), "ZADD", "e", "2", "new"),
		do(rArray("new"), "ZRANGE", "e", "0", "-1"),
		do(rInt(-1), "TTL", "e"),
	})
}
