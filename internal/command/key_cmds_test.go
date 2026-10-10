package command

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

// scanBulk reads one RESP bulk string from the front of s.
func scanBulk(t *testing.T, s string) (val, rest string) {
	t.Helper()
	line, rest, ok := strings.Cut(s, "\r\n")
	if !ok || !strings.HasPrefix(line, "$") {
		t.Fatalf("expected a bulk string at the start of %q", s)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil || n < 0 || len(rest) < n+2 {
		t.Fatalf("bad bulk length in %q", s)
	}
	return rest[:n], rest[n+2:]
}

// scanReply splits a SCAN reply into its cursor and keys.
func scanReply(t *testing.T, reply string) (string, []string) {
	t.Helper()
	rest, ok := strings.CutPrefix(reply, "*2\r\n")
	if !ok {
		t.Fatalf("not a SCAN reply: %q", reply)
	}
	cursor, rest := scanBulk(t, rest)
	line, rest, ok := strings.Cut(rest, "\r\n")
	if !ok || !strings.HasPrefix(line, "*") {
		t.Fatalf("expected the key array in %q", reply)
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		t.Fatalf("bad key count in %q", reply)
	}
	keys := make([]string, 0, n)
	for range n {
		var k string
		k, rest = scanBulk(t, rest)
		keys = append(keys, k)
	}
	return cursor, keys
}

// scanLoop runs SCAN to completion with the given options and returns every
// key reported, in order.
func (h *harness) scanLoop(t *testing.T, opts ...string) []string {
	t.Helper()
	var out []string
	cursor := "0"
	for i := 0; ; i++ {
		if i > 100000 {
			t.Fatal("SCAN did not terminate")
		}
		args := append([]string{"SCAN", cursor}, opts...)
		next, keys := scanReply(t, h.do(args...))
		out = append(out, keys...)
		if next == "0" {
			return out
		}
		cursor = next
	}
}

func TestTypeCommand(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do("+none\r\n", "TYPE", "nokey"),
		do(rOK, "SET", "s", "v"),
		do("+string\r\n", "TYPE", "s"),
		do(rInt(1), "RPUSH", "l", "a"),
		do("+list\r\n", "TYPE", "l"),
		do(rInt(1), "HSET", "h", "f", "v"),
		do("+hash\r\n", "TYPE", "h"),
		do(rInt(1), "SADD", "st", "m"),
		do("+set\r\n", "TYPE", "st"),
		do(rInt(1), "ZADD", "z", "1", "m"),
		do("+zset\r\n", "TYPE", "z"),
		do(rInt(1), "PEXPIRE", "s", "100"),
		wait(200 * time.Millisecond),
		do("+none\r\n", "TYPE", "s"),
		do(rErr("ERR wrong number of arguments for 'type' command"), "TYPE"),
	})
}

func TestRename(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "a", "1"),
		do(rOK, "RENAME", "a", "b"),
		do(rNil, "GET", "a"),
		do(rBulk("1"), "GET", "b"),
		do(rNoSuchKey, "RENAME", "a", "c"),
		do(rNoSuchKey, "RENAME", "a", "a"), // missing, even when both names are equal
		do(rOK, "RENAME", "b", "b"),        // same name, present: no-op
		do(rBulk("1"), "GET", "b"),

		// The TTL travels with the value.
		do(rOK, "SET", "t", "v", "EX", "100"),
		do(rOK, "RENAME", "t", "t2"),
		do(rInt(100), "TTL", "t2"),
		do(rInt(-2), "TTL", "t"),

		// The destination's old TTL is discarded.
		do(rOK, "SET", "d", "x", "EX", "50"),
		do(rOK, "SET", "s", "y"),
		do(rOK, "RENAME", "s", "d"),
		do(rInt(-1), "TTL", "d"),
		do(rBulk("y"), "GET", "d"),

		// A destination of another type is overwritten.
		do(rInt(1), "RPUSH", "lst", "x"),
		do(rOK, "RENAME", "d", "lst"),
		do("+string\r\n", "TYPE", "lst"),

		// Collections keep their type, contents and TTL.
		do(rInt(2), "RPUSH", "L1", "a", "b"),
		do(rInt(1), "EXPIRE", "L1", "60"),
		do(rOK, "RENAME", "L1", "L2"),
		do(rArray("a", "b"), "LRANGE", "L2", "0", "-1"),
		do(rInt(60), "TTL", "L2"),
		do(rInt(0), "EXISTS", "L1"),
		do(rInt(2), "ZADD", "z1", "1", "a", "2", "b"),
		do(rOK, "RENAME", "z1", "z2"),
		do(rArray("a", "1", "b", "2"), "ZRANGE", "z2", "0", "-1", "WITHSCORES"),

		// An expired source is a missing source.
		do(rOK, "SET", "gone", "x", "PX", "100"),
		wait(200 * time.Millisecond),
		do(rNoSuchKey, "RENAME", "gone", "other"),
		do(rInt(0), "EXISTS", "other"),

		do(rErr("ERR wrong number of arguments for 'rename' command"), "RENAME", "a"),
	})
}

func TestRenameNX(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "a", "1"),
		do(rOK, "SET", "b", "2"),
		do(rInt(0), "RENAMENX", "a", "b"), // taken: nothing changes
		do(rBulk("1"), "GET", "a"),
		do(rBulk("2"), "GET", "b"),
		do(rInt(1), "RENAMENX", "a", "c"),
		do(rNil, "GET", "a"),
		do(rBulk("1"), "GET", "c"),
		do(rInt(0), "RENAMENX", "c", "c"),
		do(rNoSuchKey, "RENAMENX", "nokey", "x"),

		do(rOK, "SET", "t", "v", "EX", "100"),
		do(rInt(1), "RENAMENX", "t", "t2"),
		do(rInt(100), "TTL", "t2"),

		// An expired destination counts as free.
		do(rOK, "SET", "e", "x", "PX", "100"),
		do(rOK, "SET", "src", "y"),
		wait(200 * time.Millisecond),
		do(rInt(1), "RENAMENX", "src", "e"),
		do(rBulk("y"), "GET", "e"),
		do(rInt(-1), "TTL", "e"),
	})
}

func TestRandomKeyCommand(t *testing.T) {
	h := newHarness(t)
	h.ctx.Rand = rand.New(rand.NewPCG(1, 2))
	h.run(t, []step{
		do(rNil, "RANDOMKEY"),
		do(rOK, "SET", "only", "v"),
		do(rBulk("only"), "RANDOMKEY"),
		do(rOK, "SET", "k2", "v"),
		do(rOK, "SET", "k3", "v"),
	})
	seen := map[string]bool{}
	for i := 0; i < 200; i++ {
		seen[parseBulk(t, h.do("RANDOMKEY"))] = true
	}
	for _, k := range []string{"only", "k2", "k3"} {
		if !seen[k] {
			t.Errorf("RANDOMKEY never returned %s in 200 draws", k)
		}
	}
	h.run(t, []step{
		do(rInt(3), "DEL", "only", "k2", "k3"),
		do(rOK, "SET", "x", "v", "PX", "100"),
		wait(200 * time.Millisecond),
		do(rNil, "RANDOMKEY"), // only an expired key is left
		do(rErr("ERR wrong number of arguments for 'randomkey' command"), "RANDOMKEY", "x"),
	})
}

func TestUnlinkAndTouch(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "a", "1"),
		do(rOK, "SET", "b", "2"),
		do(rInt(2), "TOUCH", "a", "b", "nope"),
		do(rInt(2), "TOUCH", "a", "a"), // repeats count each time
		do(rInt(2), "UNLINK", "a", "b", "nope"),
		do(rInt(0), "EXISTS", "a", "b"),
		do(rInt(0), "UNLINK", "a"),
		do(rInt(0), "TOUCH", "a"),
		do(rErr("ERR wrong number of arguments for 'unlink' command"), "UNLINK"),
	})
}

func TestScanEmptyAndShape(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do("*2\r\n"+rBulk("0")+rEmptyArray, "SCAN", "0"),
		do(rOK, "SET", "k", "v"),
	})
	cursor, keys := scanReply(t, h.do("SCAN", "0", "COUNT", "1000"))
	if cursor != "0" || !slices.Equal(keys, []string{"k"}) {
		t.Fatalf("got cursor %q keys %q", cursor, keys)
	}
}

func TestScanFullIterationReturnsEachKeyOnce(t *testing.T) {
	h := newHarness(t)
	var want []string
	for i := 0; i < 60; i++ {
		k := "k" + strconv.Itoa(i)
		want = append(want, k)
		if got := h.do("SET", k, "v"); got != rOK {
			t.Fatalf("SET: %q", got)
		}
	}
	slices.Sort(want)
	for _, count := range []string{"1", "4", "1000"} {
		got := h.scanLoop(t, "COUNT", count)
		slices.Sort(got)
		if !slices.Equal(got, want) {
			t.Errorf("COUNT %s returned %d keys, want the 60 stored ones exactly once", count, len(got))
		}
	}
	// COUNT 1 needs at most one call per key plus one per shard boundary.
	calls := 0
	cursor := "0"
	for {
		next, _ := scanReply(t, h.do("SCAN", cursor, "COUNT", "1"))
		calls++
		if next == "0" {
			break
		}
		cursor = next
	}
	if calls > 60+4+1 {
		t.Errorf("%d calls for 60 keys with COUNT 1", calls)
	}
}

func TestScanMatchAndType(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "user:1", "a"),
		do(rOK, "SET", "user:2", "a"),
		do(rOK, "SET", "user:3", "a"),
		do(rOK, "SET", "other:1", "a"),
		do(rInt(1), "RPUSH", "user:list", "x"),
		do(rInt(1), "SADD", "set:1", "m"),
		do(rInt(1), "ZADD", "zs", "1", "m"),
		do(rInt(1), "HSET", "hs", "f", "v"),
	})
	tests := []struct {
		name string
		opts []string
		want []string
	}{
		{"match prefix", []string{"MATCH", "user:*"}, []string{"user:1", "user:2", "user:3", "user:list"}},
		{"match star", []string{"MATCH", "*", "COUNT", "5"}, []string{"hs", "other:1", "set:1", "user:1", "user:2", "user:3", "user:list", "zs"}},
		{"match class", []string{"MATCH", "user:[12]"}, []string{"user:1", "user:2"}},
		{"match none", []string{"MATCH", "nomatch*"}, nil},
		{"type list", []string{"TYPE", "list"}, []string{"user:list"}},
		{"type set", []string{"TYPE", "set"}, []string{"set:1"}},
		{"type zset", []string{"TYPE", "zset"}, []string{"zs"}},
		{"type hash", []string{"TYPE", "HASH"}, []string{"hs"}}, // case-insensitive
		{"type string", []string{"TYPE", "string"}, []string{"other:1", "user:1", "user:2", "user:3"}},
		{"type unknown", []string{"TYPE", "stream"}, nil},
		{"match and type", []string{"MATCH", "user:*", "TYPE", "string", "COUNT", "2"}, []string{"user:1", "user:2", "user:3"}},
		{"options in any order", []string{"COUNT", "3", "TYPE", "string", "MATCH", "user:*"}, []string{"user:1", "user:2", "user:3"}},
	}
	for _, tt := range tests {
		got := h.scanLoop(t, tt.opts...)
		slices.Sort(got)
		if !slices.Equal(got, tt.want) {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestScanSkipsExpiredKeys(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rOK, "SET", "live1", "v"),
		do(rOK, "SET", "live2", "v"),
		do(rOK, "SET", "dead1", "v", "PX", "500"),
		do(rOK, "SET", "dead2", "v", "PX", "500"),
	})
	if got := h.scanLoop(t); len(got) != 4 {
		t.Fatalf("before expiry: %q", got)
	}
	h.run(t, []step{wait(time.Second)})
	got := h.scanLoop(t)
	slices.Sort(got)
	if !slices.Equal(got, []string{"live1", "live2"}) {
		t.Fatalf("after expiry: %q", got)
	}
}

func TestScanErrors(t *testing.T) {
	h := newHarness(t)
	shardFour := strconv.FormatUint(uint64(4)<<40, 10) // the harness store has 4 shards
	h.run(t, []step{
		do(rErr("ERR invalid cursor"), "SCAN", "abc"),
		do(rErr("ERR invalid cursor"), "SCAN", "-1"),
		do(rErr("ERR invalid cursor"), "SCAN", "1.5"),
		do(rErr("ERR invalid cursor"), "SCAN", ""),
		do(rErr("ERR invalid cursor"), "SCAN", "18446744073709551616"), // overflows uint64
		do(rErr("ERR invalid cursor"), "SCAN", "18446744073709551615"), // shard far out of range
		do(rErr("ERR invalid cursor"), "SCAN", shardFour),
		do(rErr("ERR invalid cursor"), "SCAN", "abc", "BOGUS"), // the cursor is checked first
		do(rSyntax, "SCAN", "0", "COUNT", "0"),
		do(rSyntax, "SCAN", "0", "COUNT", "-3"),
		do(rNotInt, "SCAN", "0", "COUNT", "x"),
		do(rSyntax, "SCAN", "0", "COUNT"),
		do(rSyntax, "SCAN", "0", "MATCH"),
		do(rSyntax, "SCAN", "0", "TYPE"),
		do(rSyntax, "SCAN", "0", "BOGUS"),
		do(rErr("ERR wrong number of arguments for 'scan' command"), "SCAN"),
	})
	// A stale but well-formed cursor is accepted, not rejected.
	if got := h.do("SCAN", strconv.FormatUint(1<<39, 10)); !strings.HasPrefix(got, "*2\r\n") {
		t.Fatalf("stale cursor: %q", got)
	}
}

func TestKeyCommandsRegistered(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"TYPE", "RENAME", "RENAMENX", "RANDOMKEY", "UNLINK", "TOUCH", "SCAN"} {
		if _, ok := r.Lookup([]byte(name)); !ok {
			t.Errorf("missing command %s", name)
		}
	}
}
