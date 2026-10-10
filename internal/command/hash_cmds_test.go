package command

import (
	"testing"
	"time"
)

func TestHashCommandsRegistered(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"HSET", "HSETNX", "HGET", "HMGET", "HDEL", "HGETALL", "HKEYS", "HVALS",
		"HEXISTS", "HLEN", "HSTRLEN", "HINCRBY", "HINCRBYFLOAT"} {
		if _, ok := r.Lookup([]byte(name)); !ok {
			t.Errorf("missing command %s", name)
		}
	}
}

func TestHashSetGetAndIntrospection(t *testing.T) {
	h := newHarness(t)
	wrongArgs := rErr("ERR wrong number of arguments for 'hset' command")
	h.run(t, []step{
		do(rInt(2), "HSET", "h", "f1", "v1", "f2", "v2"),
		do(rInt(1), "HSET", "h", "f1", "vv", "f3", "v3"), // f1 updated (not counted), f3 new
		do(rBulk("vv"), "HGET", "h", "f1"),
		do(rNil, "HGET", "h", "nope"),
		do(rNil, "HGET", "nokey", "f1"),
		do(rInt(3), "HLEN", "h"),
		do(rInt(0), "HLEN", "nokey"),

		// Iteration is insertion order, and the three views agree.
		do(rArray("f1", "vv", "f2", "v2", "f3", "v3"), "HGETALL", "h"),
		do(rArray("f1", "f2", "f3"), "HKEYS", "h"),
		do(rArray("vv", "v2", "v3"), "HVALS", "h"),
		do(rEmptyArray, "HGETALL", "nokey"),
		do(rEmptyArray, "HKEYS", "nokey"),
		do(rEmptyArray, "HVALS", "nokey"),

		do(rInt(1), "HEXISTS", "h", "f2"),
		do(rInt(0), "HEXISTS", "h", "zz"),
		do(rInt(0), "HEXISTS", "nokey", "f"),
		do(rInt(2), "HSTRLEN", "h", "f1"),
		do(rInt(0), "HSTRLEN", "h", "zz"),
		do(rInt(0), "HSTRLEN", "nokey", "f"),

		do("*3\r\n$2\r\nvv\r\n$-1\r\n$2\r\nv2\r\n", "HMGET", "h", "f1", "zz", "f2"),
		do("*2\r\n$-1\r\n$-1\r\n", "HMGET", "nokey", "a", "b"),

		do(wrongArgs, "HSET", "h", "f1", "v1", "f2"), // a field without a value
		do(wrongArgs, "HSET", "h", "f1"),
		do(rInt(3), "HLEN", "h"), // nothing was written
	})
}

func TestHashSetNX(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(1), "HSETNX", "h", "f", "v"),
		do(rInt(0), "HSETNX", "h", "f", "other"),
		do(rBulk("v"), "HGET", "h", "f"),
		do(rInt(1), "HSETNX", "h", "g", "w"),
		do(rInt(2), "HLEN", "h"),
	})
}

func TestHashDel(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "HSET", "h", "f1", "a", "f2", "b", "f3", "c"),
		do(rInt(1), "HDEL", "h", "f1", "zz", "f1"), // a repeated field counts once
		do(rNil, "HGET", "h", "f1"),
		do(rBulk("b"), "HGET", "h", "f2"),
		do(rBulk("c"), "HGET", "h", "f3"), // survivors stay reachable after the swap
		do(rInt(2), "HDEL", "h", "f2", "f3"),
		do(rInt(0), "EXISTS", "h"), // an emptied hash is deleted
		do(rInt(0), "HDEL", "nokey", "f"),
	})
}

func TestHashIncrBy(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(5), "HINCRBY", "h", "n", "5"), // creates key and field
		do(rInt(3), "HINCRBY", "h", "n", "-2"),
		do(rBulk("3"), "HGET", "h", "n"),
		do(rInt(7), "HINCRBY", "nokey", "f", "7"),

		do(rInt(1), "HSET", "h", "s", "abc"),
		do(rHashNotInt, "HINCRBY", "h", "s", "1"),
		do(rInt(1), "HSET", "h", "lz", "007"),
		do(rHashNotInt, "HINCRBY", "h", "lz", "1"), // strict integer syntax
		do(rNotInt, "HINCRBY", "h", "n", "x"),
		do(rNotInt, "HINCRBY", "h", "n", "1.5"),

		do(rInt(1), "HSET", "h", "big", "9223372036854775807"),
		do(rOverflow, "HINCRBY", "h", "big", "1"),
		do(rInt(1), "HSET", "h", "small", "-9223372036854775808"),
		do(rOverflow, "HINCRBY", "h", "small", "-1"),
		do(rBulk("9223372036854775807"), "HGET", "h", "big"), // failures changed nothing

		// A failed HINCRBY never creates the key.
		do(rNotInt, "HINCRBY", "fresh", "f", "x"),
		do(rInt(0), "EXISTS", "fresh"),
	})
}

func TestHashIncrByFloat(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(1), "HSET", "h", "f", "10.50"),
		do(rBulk("10.6"), "HINCRBYFLOAT", "h", "f", "0.1"),
		do(rBulk("5.6"), "HINCRBYFLOAT", "h", "f", "-5"),
		do(rBulk("5.6"), "HGET", "h", "f"), // the stored value is the reply

		do(rInt(1), "HSET", "h", "g", "5.0e3"),
		do(rBulk("5200"), "HINCRBYFLOAT", "h", "g", "2.0e2"),

		do(rBulk("0.1"), "HINCRBYFLOAT", "h", "new", "0.1"), // missing field starts at 0
		do(rBulk("0.3"), "HINCRBYFLOAT", "h", "new", "0.2"),
		do(rBulk("10.3"), "HINCRBYFLOAT", "h", "new", "1e1"),
		do(rBulk("0"), "HINCRBYFLOAT", "h", "zero", "-0"),
		do(rBulk("3"), "HINCRBYFLOAT", "fresh", "f", "3"), // missing key is created

		do(rNotFloat, "HINCRBYFLOAT", "h", "f", "abc"),
		do(rNotFloat, "HINCRBYFLOAT", "h", "f", "nan"),
		do(rNotFloat, "HINCRBYFLOAT", "h", "f", " 1"),
		do(rNotFloat, "HINCRBYFLOAT", "h", "f", "1e5000"),
		do(rInt(1), "HSET", "h", "s", "abc"),
		do(rHashNotFloat, "HINCRBYFLOAT", "h", "s", "1"),
		do(rNaNOrInf, "HINCRBYFLOAT", "h", "f", "inf"),
		do(rNaNOrInf, "HINCRBYFLOAT", "h", "f", "-inf"),
		do(rInt(1), "HSET", "h", "i", "inf"), // an infinite stored value cannot be incremented
		do(rNaNOrInf, "HINCRBYFLOAT", "h", "i", "1"),
		do(rBulk("5.6"), "HGET", "h", "f"), // none of the failures changed it

		do(rNotFloat, "HINCRBYFLOAT", "fresh2", "f", "x"),
		do(rInt(0), "EXISTS", "fresh2"),
	})
}

func TestHashTTL(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(1), "HSET", "h", "f", "v"),
		do(rInt(1), "EXPIRE", "h", "10"),
		do(rInt(1), "HSET", "h", "g", "w"),
		do(rInt(10), "TTL", "h"), // writes keep the TTL
		wait(11 * time.Second),
		do(rInt(0), "EXISTS", "h"),
		do(rNil, "HGET", "h", "f"),
		do(rInt(1), "HSET", "h", "f", "v"),
		do(rInt(-1), "TTL", "h"), // an expired hash must not leak its deadline

		do(rInt(1), "EXPIRE", "h", "100"),
		do(rInt(1), "HDEL", "h", "f"),
		do(rInt(-2), "TTL", "h"), // emptying the hash removes key and TTL together
	})
}

func TestHashBinarySafe(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "HSET", "h", "a\r\nb", "x\r\ny", "", ""),
		do(rBulk("x\r\ny"), "HGET", "h", "a\r\nb"),
		do(rBulk(""), "HGET", "h", ""),
		do(rInt(0), "HSTRLEN", "h", ""),
		do(rInt(4), "HSTRLEN", "h", "a\r\nb"),
		do(rArray("a\r\nb", "x\r\ny", "", ""), "HGETALL", "h"),
	})
}
