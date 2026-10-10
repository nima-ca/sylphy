package command

import (
	"math"
	"strconv"
	"testing"
	"time"
)

func TestListCommandsRegistered(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LLEN",
		"LRANGE", "LINDEX", "LSET", "LREM", "LTRIM", "LINSERT"} {
		if _, ok := r.Lookup([]byte(name)); !ok {
			t.Errorf("missing command %s", name)
		}
	}
}

func TestListPushPop(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "RPUSH", "l", "a", "b", "c"),
		do(rInt(5), "LPUSH", "l", "x", "y"), // pushed one by one: y ends up first
		do(rArray("y", "x", "a", "b", "c"), "LRANGE", "l", "0", "-1"),
		do(rInt(5), "LLEN", "l"),
		do(rBulk("y"), "LPOP", "l"),
		do(rBulk("c"), "RPOP", "l"),
		do(rArray("x", "a"), "LPOP", "l", "2"),
		do(rArray("b"), "RPOP", "l", "10"), // more than there is: pops what remains
		do(rInt(0), "EXISTS", "l"),         // an emptied list is deleted
		do(rNil, "LPOP", "l"),
		do(rNil, "RPOP", "l"),
		do(rNullArray, "LPOP", "l", "3"),
		do(rNullArray, "RPOP", "l", "0"),
		do(rInt(0), "LLEN", "l"),
		do(rEmptyArray, "LRANGE", "l", "0", "-1"),
	})
}

func TestListPopCount(t *testing.T) {
	h := newHarness(t)
	wrongArgs := rErr("ERR wrong number of arguments for 'lpop' command")
	h.run(t, []step{
		do(rInt(3), "RPUSH", "l", "1", "2", "3"),
		do(rEmptyArray, "LPOP", "l", "0"),
		do(rInt(3), "LLEN", "l"),
		do(rArray("3", "2"), "RPOP", "l", "2"),
		do(rArray("1"), "LPOP", "l", "9223372036854775807"), // must not allocate count slots
		do(rInt(0), "EXISTS", "l"),

		do(rInt(1), "RPUSH", "l", "a"),
		do(rMustBePos, "LPOP", "l", "-1"),
		do(rMustBePos, "LPOP", "l", "x"),
		do(rMustBePos, "LPOP", "l", "1.5"),
		do(wrongArgs, "LPOP", "l", "1", "2"),
		do(rInt(1), "LLEN", "l"), // none of the failures popped anything

		// The count is validated before the key is looked up or typed.
		do(rOK, "SET", "s", "v"),
		do(rWrongType, "LPOP", "s"),
		do(rWrongType, "LPOP", "s", "1"),
		do(rWrongType, "LPOP", "s", "0"),
		do(rMustBePos, "LPOP", "s", "x"),
	})
}

func TestListPushX(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(0), "LPUSHX", "l", "a"),
		do(rInt(0), "RPUSHX", "l", "a"),
		do(rInt(0), "EXISTS", "l"), // PUSHX never creates the key
		do(rInt(1), "RPUSH", "l", "m"),
		do(rInt(2), "LPUSHX", "l", "a"),
		do(rInt(4), "RPUSHX", "l", "b", "c"),
		do(rArray("a", "m", "b", "c"), "LRANGE", "l", "0", "-1"),
	})
}

func TestListRange(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(5), "RPUSH", "l", "a", "b", "c", "d", "e"),
		do(rArray("a"), "LRANGE", "l", "0", "0"),
		do(rArray("a", "b", "c", "d", "e"), "LRANGE", "l", "0", "-1"),
		do(rArray("d", "e"), "LRANGE", "l", "-2", "-1"),
		do(rArray("b", "c", "d", "e"), "LRANGE", "l", "1", "100"),
		do(rEmptyArray, "LRANGE", "l", "3", "1"),
		do(rArray("a", "b", "c"), "LRANGE", "l", "-100", "2"),
		do(rEmptyArray, "LRANGE", "l", "5", "10"),
		do(rEmptyArray, "LRANGE", "l", "0", "-100"),
		do(rEmptyArray, "LRANGE", "l", "2", "-4"),
		do(rArray("e"), "LRANGE", "l", "-1", "-1"),
		do(rArray("c"), "LRANGE", "l", "-3", "-3"),
		do(rArray("a", "b", "c", "d", "e"), "LRANGE", "l", "-9223372036854775808", "9223372036854775807"),
		do(rEmptyArray, "LRANGE", "nokey", "0", "-1"),
		do(rNotInt, "LRANGE", "l", "a", "1"),
		do(rNotInt, "LRANGE", "l", "0", "b"),
		do(rErr("ERR wrong number of arguments for 'lrange' command"), "LRANGE", "l", "0"),
	})
}

func TestListIndex(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(5), "RPUSH", "l", "a", "b", "c", "d", "e"),
		do(rBulk("a"), "LINDEX", "l", "0"),
		do(rBulk("e"), "LINDEX", "l", "4"),
		do(rBulk("e"), "LINDEX", "l", "-1"),
		do(rBulk("a"), "LINDEX", "l", "-5"),
		do(rNil, "LINDEX", "l", "5"),
		do(rNil, "LINDEX", "l", "-6"),
		do(rNil, "LINDEX", "l", "9223372036854775807"),
		do(rNil, "LINDEX", "l", "-9223372036854775808"),
		do(rNil, "LINDEX", "nokey", "0"),
		do(rNotInt, "LINDEX", "l", "x"),
	})
}

func TestListSet(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rNoSuchKey, "LSET", "nokey", "0", "v"),
		do(rNotInt, "LSET", "nokey", "x", "v"), // the index is parsed first
		do(rInt(0), "EXISTS", "nokey"),         // a failed LSET never creates the key
		do(rInt(3), "RPUSH", "l", "a", "b", "c"),
		do(rOK, "LSET", "l", "1", "B"),
		do(rOK, "LSET", "l", "-1", "C"),
		do(rArray("a", "B", "C"), "LRANGE", "l", "0", "-1"),
		do(rBadIndex, "LSET", "l", "3", "x"),
		do(rBadIndex, "LSET", "l", "-4", "x"),
		do(rNotInt, "LSET", "l", "x", "v"),
		do(rArray("a", "B", "C"), "LRANGE", "l", "0", "-1"),
		do(rInt(1), "EXPIRE", "l", "50"),
		do(rOK, "LSET", "l", "0", "A"),
		do(rInt(50), "TTL", "l"), // LSET keeps the TTL
	})
}

func TestListRem(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(7), "RPUSH", "l", "a", "b", "a", "c", "a", "b", "a"),
		do(rInt(2), "LREM", "l", "2", "a"),
		do(rArray("b", "c", "a", "b", "a"), "LRANGE", "l", "0", "-1"),
		do(rInt(1), "LREM", "l", "-1", "a"),
		do(rArray("b", "c", "a", "b"), "LRANGE", "l", "0", "-1"),
		do(rInt(2), "LREM", "l", "0", "b"),
		do(rArray("c", "a"), "LRANGE", "l", "0", "-1"),
		do(rInt(0), "LREM", "l", "0", "zz"),
		do(rInt(1), "LREM", "l", "5", "a"),
		do(rInt(1), "LREM", "l", "0", "c"),
		do(rInt(0), "EXISTS", "l"), // removing the last element deletes the key
		do(rInt(0), "LREM", "nokey", "0", "a"),
		do(rNotInt, "LREM", "l", "x", "a"),
		do(rInt(2), "RPUSH", "m", "x", "x"),
		do(rInt(2), "LREM", "m", "-9223372036854775808", "x"),
		do(rInt(0), "EXISTS", "m"),
	})
}

func TestListTrim(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(5), "RPUSH", "l", "a", "b", "c", "d", "e"),
		do(rOK, "LTRIM", "l", "1", "-2"),
		do(rArray("b", "c", "d"), "LRANGE", "l", "0", "-1"),
		do(rOK, "LTRIM", "l", "0", "0"),
		do(rArray("b"), "LRANGE", "l", "0", "-1"),
		do(rOK, "LTRIM", "l", "5", "10"),
		do(rInt(0), "EXISTS", "l"),
		do(rOK, "LTRIM", "nokey", "0", "1"),
		do(rNotInt, "LTRIM", "l", "a", "1"),
		do(rNotInt, "LTRIM", "l", "0", "b"),

		// The TTL survives a trim that leaves elements and dies with the key.
		do(rInt(3), "RPUSH", "t", "a", "b", "c"),
		do(rInt(1), "EXPIRE", "t", "100"),
		do(rOK, "LTRIM", "t", "0", "1"),
		do(rInt(100), "TTL", "t"),
		do(rOK, "LTRIM", "t", "9", "9"),
		do(rInt(-2), "TTL", "t"),
		do(rInt(1), "RPUSH", "t", "a"),
		do(rInt(-1), "TTL", "t"), // the recreated list has no TTL
	})
}

func TestListInsert(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "RPUSH", "l", "a", "c"),
		do(rInt(3), "LINSERT", "l", "BEFORE", "c", "b"),
		do(rInt(4), "LINSERT", "l", "AFTER", "c", "d"),
		do(rInt(5), "LINSERT", "l", "before", "a", "z"), // keyword is case-insensitive
		do(rArray("z", "a", "b", "c", "d"), "LRANGE", "l", "0", "-1"),
		do(rInt(-1), "LINSERT", "l", "AFTER", "nope", "x"),
		do(rInt(5), "LLEN", "l"),
		do(rInt(0), "LINSERT", "nokey", "BEFORE", "a", "b"),
		do(rInt(0), "EXISTS", "nokey"),
		do(rSyntax, "LINSERT", "l", "MIDDLE", "a", "b"),

		// With duplicate pivots the first one from the head wins.
		do(rInt(3), "RPUSH", "d", "a", "b", "a"),
		do(rInt(4), "LINSERT", "d", "BEFORE", "a", "X"),
		do(rInt(5), "LINSERT", "d", "AFTER", "a", "Y"),
		do(rArray("X", "a", "Y", "b", "a"), "LRANGE", "d", "0", "-1"),
	})
}

func TestListWrongType(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "s", "v"),
		do(rWrongType, "LPUSH", "s", "a"),
		do(rWrongType, "RPUSH", "s", "a"),
		do(rWrongType, "LPUSHX", "s", "a"),
		do(rWrongType, "RPUSHX", "s", "a"),
		do(rWrongType, "LPOP", "s"),
		do(rWrongType, "RPOP", "s"),
		do(rWrongType, "LPOP", "s", "2"),
		do(rWrongType, "LLEN", "s"),
		do(rWrongType, "LRANGE", "s", "0", "-1"),
		do(rWrongType, "LINDEX", "s", "0"),
		do(rWrongType, "LSET", "s", "0", "a"),
		do(rWrongType, "LREM", "s", "0", "a"),
		do(rWrongType, "LTRIM", "s", "0", "1"),
		do(rWrongType, "LINSERT", "s", "BEFORE", "a", "b"),
		do(rBulk("v"), "GET", "s"), // nothing above changed the string

		// And the other way round: string commands on a real list.
		do(rInt(1), "RPUSH", "l", "a"),
		do(rWrongType, "GET", "l"),
		do(rWrongType, "STRLEN", "l"),
		do(rWrongType, "APPEND", "l", "x"),
		do(rWrongType, "INCR", "l"),
		do(rWrongType, "SET", "l", "v", "GET"),
		do(rEmptyArray, "LRANGE", "l", "5", "9"), // still a list
		do(rInt(1), "EXPIRE", "l", "10"),         // type-agnostic commands work
		do(rInt(10), "TTL", "l"),
		do(rInt(1), "DEL", "l"),

		do(rInt(1), "RPUSH", "l", "a"),
		do(rOK, "SET", "l", "v"), // SET overwrites a list
		do(rBulk("v"), "GET", "l"),
	})

	// A Collection of kind list that is not a *list.List is also rejected.
	h.putList(t, "foreign")
	h.run(t, []step{do(rWrongType, "LPUSH", "foreign", "a")})
}

func TestListTTLInteraction(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(1), "RPUSH", "l", "a"),
		do(rInt(1), "EXPIRE", "l", "10"),
		do(rInt(2), "RPUSH", "l", "b"),
		do(rInt(10), "TTL", "l"), // pushing keeps the TTL
		wait(11 * time.Second),
		do(rInt(0), "EXISTS", "l"),
		do(rInt(0), "LLEN", "l"),
		do(rNil, "LPOP", "l"),
		do(rInt(1), "RPUSH", "l", "z"),
		do(rInt(-1), "TTL", "l"), // an expired list must not leak its deadline

		// Emptying a list removes its TTL along with the key.
		do(rInt(1), "EXPIRE", "l", "100"),
		do(rBulk("z"), "LPOP", "l"),
		do(rInt(-2), "TTL", "l"),
		do(rInt(1), "RPUSH", "l", "a"),
		do(rInt(-1), "TTL", "l"),
	})
}

func TestListBinarySafeElements(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "RPUSH", "l", "a\r\nb", "", "\x00x"),
		do(rArray("a\r\nb", "", "\x00x"), "LRANGE", "l", "0", "-1"),
		do(rBulk(""), "LINDEX", "l", "1"),
		do(rInt(1), "LREM", "l", "0", ""),
		do(rInt(2), "LLEN", "l"),
		do(rInt(3), "LINSERT", "l", "AFTER", "\x00x", ""),
		do(rBulk(""), "RPOP", "l"),
	})
}

func TestListLargeSequence(t *testing.T) {
	h := newHarness(t)
	const n = 2000
	for i := 0; i < n; i++ {
		if got := h.do("RPUSH", "big", strconv.Itoa(i)); got != rInt(int64(i+1)) {
			t.Fatalf("RPUSH %d: got %q", i, got)
		}
	}
	h.run(t, []step{
		do(rInt(n), "LLEN", "big"),
		do(rBulk("0"), "LINDEX", "big", "0"),
		do(rBulk("1000"), "LINDEX", "big", "1000"),
		do(rBulk("1999"), "LINDEX", "big", "-1"),
	})
	for i := 0; i < 1000; i++ {
		if got := h.do("LPOP", "big"); got != rBulk(strconv.Itoa(i)) {
			t.Fatalf("LPOP %d: got %q", i, got)
		}
	}
	h.run(t, []step{
		do(rInt(1000), "LLEN", "big"),
		do(rBulk("1000"), "LINDEX", "big", "0"),
		do(rOK, "LTRIM", "big", "0", "9"),
		do(rInt(10), "LLEN", "big"),
		do(rBulk("1009"), "RPOP", "big"),
	})
}

func TestResolveIndex(t *testing.T) {
	tests := []struct {
		i    int64
		n    int
		want int
		ok   bool
	}{
		{0, 5, 0, true}, {4, 5, 4, true}, {-1, 5, 4, true}, {-5, 5, 0, true},
		{5, 5, 0, false}, {-6, 5, 0, false}, {0, 0, 0, false}, {-1, 0, 0, false},
		{math.MaxInt64, 5, 0, false}, {math.MinInt64, 5, 0, false},
	}
	for _, tt := range tests {
		got, ok := resolveIndex(tt.i, tt.n)
		if ok != tt.ok || (ok && got != tt.want) {
			t.Errorf("resolveIndex(%d, %d) = %d,%v; want %d,%v", tt.i, tt.n, got, ok, tt.want, tt.ok)
		}
	}
}

func TestParseCount(t *testing.T) {
	for in, want := range map[string]int64{"0": 0, "1": 1, "9223372036854775807": math.MaxInt64} {
		if got, err := parseCount([]byte(in)); err != nil || got != want {
			t.Errorf("parseCount(%q) = %d,%v", in, got, err)
		}
	}
	for _, in := range []string{"", "-1", "x", "1.5", "01", "+1", " 1", "9223372036854775808"} {
		if _, err := parseCount([]byte(in)); err != ErrMustBePositive {
			t.Errorf("parseCount(%q) err = %v, want ErrMustBePositive", in, err)
		}
	}
}
