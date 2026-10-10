package command

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"
	"time"
)

// members runs a command whose reply is an unordered array of bulk strings and
// returns the elements sorted.
func (h *harness) members(t *testing.T, args ...string) []string {
	t.Helper()
	got := parseBulkArray(t, h.do(args...))
	slices.Sort(got)
	return got
}

func wantMembers(t *testing.T, got []string, want ...string) {
	t.Helper()
	slices.Sort(want)
	if !slices.Equal(got, want) {
		t.Errorf("members = %v, want %v", got, want)
	}
}

// take checks that every popped or sampled member is still unclaimed in all,
// then claims it, so repeats and strangers fail.
func take(t *testing.T, all map[string]bool, got []string) {
	t.Helper()
	for _, m := range got {
		if !all[m] {
			t.Fatalf("member %q is unknown or was returned twice", m)
		}
		delete(all, m)
	}
}

func remaining(all map[string]bool) []string {
	out := make([]string, 0, len(all))
	for m := range all {
		out = append(out, m)
	}
	slices.Sort(out)
	return out
}

func TestSetCommandsRegistered(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"SADD", "SREM", "SMEMBERS", "SISMEMBER", "SMISMEMBER", "SCARD", "SPOP",
		"SRANDMEMBER", "SMOVE", "SUNION", "SINTER", "SDIFF", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE"} {
		if _, ok := r.Lookup([]byte(name)); !ok {
			t.Errorf("missing command %s", name)
		}
	}
}

func TestSetAddRemoveMembership(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "SADD", "s", "a", "b", "c"),
		do(rInt(1), "SADD", "s", "c", "d", "a"), // only d is new
		do(rInt(1), "SADD", "dup", "x", "x", "x"),
		do(rInt(4), "SCARD", "s"),
		do(rInt(0), "SCARD", "nokey"),
		do(rInt(1), "SISMEMBER", "s", "a"),
		do(rInt(0), "SISMEMBER", "s", "zz"),
		do(rInt(0), "SISMEMBER", "nokey", "a"),
		do(rIntArray(1, 0, 1), "SMISMEMBER", "s", "a", "zz", "d"),
		do(rIntArray(0, 0), "SMISMEMBER", "nokey", "a", "b"),
		do(rEmptyArray, "SMEMBERS", "nokey"),
	})
	wantMembers(t, h.members(t, "SMEMBERS", "s"), "a", "b", "c", "d")
	h.run(t, []step{
		do(rInt(1), "SREM", "s", "a", "zz", "a"), // a repeated member counts once
		do(rInt(3), "SREM", "s", "b", "c", "d"),
		do(rInt(0), "EXISTS", "s"), // an emptied set is deleted
		do(rInt(0), "SREM", "nokey", "a"),
	})
}

func TestSetBinarySafe(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "SADD", "s", "a\r\nb", "", "\x00x"),
		do(rInt(1), "SISMEMBER", "s", ""),
		do(rInt(1), "SISMEMBER", "s", "a\r\nb"),
	})
	wantMembers(t, h.members(t, "SMEMBERS", "s"), "a\r\nb", "", "\x00x")
}

func TestSetPop(t *testing.T) {
	h := newHarness(t)
	h.ctx.Rand = rand.New(rand.NewPCG(1, 2))
	all := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	h.run(t, []step{do(rInt(5), "SADD", "s", "a", "b", "c", "d", "e")})

	take(t, all, []string{parseBulk(t, h.do("SPOP", "s"))})
	h.run(t, []step{do(rInt(4), "SCARD", "s")})
	two := parseBulkArray(t, h.do("SPOP", "s", "2"))
	if len(two) != 2 {
		t.Fatalf("SPOP s 2 returned %v", two)
	}
	take(t, all, two)
	h.run(t, []step{
		do(rInt(2), "SCARD", "s"),
		do(rEmptyArray, "SPOP", "s", "0"), // existing key, count 0
		do(rInt(2), "SCARD", "s"),
	})
	rest := parseBulkArray(t, h.do("SPOP", "s", "10")) // more than there is
	slices.Sort(rest)
	if !slices.Equal(rest, remaining(all)) {
		t.Fatalf("SPOP s 10 = %v, want %v", rest, remaining(all))
	}
	h.run(t, []step{
		do(rInt(0), "EXISTS", "s"),
		do(rNil, "SPOP", "s"),
		do(rEmptyArray, "SPOP", "s", "3"), // unlike lists, a missing key gives an empty array
		do(rMustBePos, "SPOP", "s", "-1"),
		do(rMustBePos, "SPOP", "s", "x"),
		do(rSyntax, "SPOP", "s", "1", "2"),

		// The count is validated before the key's type.
		do(rOK, "SET", "str", "v"),
		do(rWrongType, "SPOP", "str"),
		do(rWrongType, "SPOP", "str", "0"),
		do(rMustBePos, "SPOP", "str", "x"),
	})
}

func TestSetRandMember(t *testing.T) {
	h := newHarness(t)
	h.ctx.Rand = rand.New(rand.NewPCG(3, 4))
	universe := []string{"a", "b", "c", "d", "e"}
	h.run(t, []step{do(rInt(5), "SADD", "s", "a", "b", "c", "d", "e")})

	if m := parseBulk(t, h.do("SRANDMEMBER", "s")); !slices.Contains(universe, m) {
		t.Fatalf("SRANDMEMBER returned %q", m)
	}
	seen := map[string]bool{}
	for i := 0; i < 1000; i++ {
		seen[parseBulk(t, h.do("SRANDMEMBER", "s"))] = true
	}
	if len(seen) != 5 {
		t.Fatalf("1000 draws from 5 members hit only %d of them", len(seen))
	}

	three := parseBulkArray(t, h.do("SRANDMEMBER", "s", "3")) // distinct
	if len(three) != 3 {
		t.Fatalf("SRANDMEMBER s 3 = %v", three)
	}
	all := map[string]bool{"a": true, "b": true, "c": true, "d": true, "e": true}
	take(t, all, three)

	everything := parseBulkArray(t, h.do("SRANDMEMBER", "s", "10")) // capped at the set size
	slices.Sort(everything)
	if !slices.Equal(everything, universe) {
		t.Fatalf("SRANDMEMBER s 10 = %v", everything)
	}

	repeated := parseBulkArray(t, h.do("SRANDMEMBER", "s", "-8")) // may repeat, never capped
	if len(repeated) != 8 {
		t.Fatalf("SRANDMEMBER s -8 returned %d members", len(repeated))
	}
	for _, m := range repeated {
		if !slices.Contains(universe, m) {
			t.Fatalf("unknown member %q", m)
		}
	}

	h.run(t, []step{
		do(rInt(5), "SCARD", "s"), // read-only
		do(rEmptyArray, "SRANDMEMBER", "s", "0"),
		do(rNil, "SRANDMEMBER", "nokey"),
		do(rEmptyArray, "SRANDMEMBER", "nokey", "3"),
		do(rEmptyArray, "SRANDMEMBER", "nokey", "-3"),
		do(rNotInt, "SRANDMEMBER", "s", "x"),
		do(rErr("ERR value is out of range, must be between -9223372036854775807 and 9223372036854775807"),
			"SRANDMEMBER", "s", "-9223372036854775808"),
		do(rErr(ErrSampleTooLarge.Msg), "SRANDMEMBER", "s", "-1048577"),
		do(rErr("ERR wrong number of arguments for 'srandmember' command"), "SRANDMEMBER", "s", "1", "2"),
	})
	if got := parseBulkArray(t, h.do("SRANDMEMBER", "s", "-1048576")); len(got) != 1<<20 {
		t.Fatalf("the largest allowed negative count returned %d members", len(got))
	}
}

func TestSetOperations(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "SADD", "a", "1", "2", "3"),
		do(rInt(3), "SADD", "b", "2", "3", "4"),
		do(rInt(2), "SADD", "c", "3", "5"),
	})
	wantMembers(t, h.members(t, "SINTER", "a", "b"), "2", "3")
	wantMembers(t, h.members(t, "SINTER", "a", "b", "c"), "3")
	wantMembers(t, h.members(t, "SINTER", "a"), "1", "2", "3")
	wantMembers(t, h.members(t, "SINTER", "a", "a"), "1", "2", "3")
	wantMembers(t, h.members(t, "SUNION", "a", "b", "c"), "1", "2", "3", "4", "5")
	wantMembers(t, h.members(t, "SUNION", "a", "nokey"), "1", "2", "3")
	wantMembers(t, h.members(t, "SDIFF", "a", "b"), "1")
	wantMembers(t, h.members(t, "SDIFF", "a", "b", "c"), "1")
	wantMembers(t, h.members(t, "SDIFF", "a", "nokey"), "1", "2", "3")
	wantMembers(t, h.members(t, "SDIFF", "a"), "1", "2", "3")
	h.run(t, []step{
		do(rEmptyArray, "SINTER", "a", "nokey"), // a missing key is an empty set
		do(rEmptyArray, "SINTER", "nokey", "a"),
		do(rEmptyArray, "SUNION", "nokey1", "nokey2"),
		do(rEmptyArray, "SDIFF", "nokey", "a"),
		do(rEmptyArray, "SINTER", "nokey"),
	})
}

func TestSetOperationWrongTypes(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "SADD", "a", "1", "2"),
		do(rOK, "SET", "s", "v"),
		do(rWrongType, "SINTER", "a", "s"),
		do(rWrongType, "SINTER", "s", "a"),
		do(rWrongType, "SUNION", "a", "s"),
		do(rWrongType, "SUNION", "nokey", "s"),
		do(rWrongType, "SDIFF", "a", "s"),
		do(rWrongType, "SDIFF", "nokey", "s"), // SDIFF checks every key
		// Like Redis, SINTER stops at the first missing key, before it looks at
		// the types of the keys after it.
		do(rEmptyArray, "SINTER", "nokey", "s"),
		do(rWrongType, "SUNIONSTORE", "d", "a", "s"),
		do(rWrongType, "SINTERSTORE", "d", "s", "a"),
		do(rWrongType, "SDIFFSTORE", "d", "a", "s"),
		do(rInt(0), "EXISTS", "d"), // a failed *STORE never creates the destination
	})
}

func TestSetStoreVariants(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(3), "SADD", "a", "1", "2", "3"),
		do(rInt(3), "SADD", "b", "2", "3", "4"),
		do(rInt(4), "SUNIONSTORE", "d", "a", "b"),
	})
	wantMembers(t, h.members(t, "SMEMBERS", "d"), "1", "2", "3", "4")
	h.run(t, []step{do(rInt(2), "SINTERSTORE", "d", "a", "b")})
	wantMembers(t, h.members(t, "SMEMBERS", "d"), "2", "3")
	h.run(t, []step{do(rInt(1), "SDIFFSTORE", "d", "a", "b")})
	wantMembers(t, h.members(t, "SMEMBERS", "d"), "1")

	// An empty result deletes the destination.
	h.run(t, []step{
		do(rInt(0), "SINTERSTORE", "d", "a", "nokey"),
		do(rInt(0), "EXISTS", "d"),
		do(rInt(0), "SUNIONSTORE", "fresh", "nokey1", "nokey2"),
		do(rInt(0), "EXISTS", "fresh"),
	})

	// The destination may be one of the sources.
	h.run(t, []step{do(rInt(4), "SUNIONSTORE", "a", "a", "b")})
	wantMembers(t, h.members(t, "SMEMBERS", "a"), "1", "2", "3", "4")
	h.run(t, []step{do(rInt(4), "SDIFFSTORE", "a", "a", "nokey")})
	wantMembers(t, h.members(t, "SMEMBERS", "a"), "1", "2", "3", "4")
}

func TestSetStoreOverwritesTypeAndTTL(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "SADD", "a", "1", "2"),
		do(rOK, "SET", "d", "x"),
		do(rInt(1), "EXPIRE", "d", "100"),
		do(rInt(2), "SUNIONSTORE", "d", "a"),
		do(rInt(-1), "TTL", "d"), // the old TTL is gone
		do(rWrongType, "GET", "d"),
		do(rInt(2), "SCARD", "d"),

		do(rInt(1), "EXPIRE", "d", "100"),
		do(rInt(0), "SINTERSTORE", "d", "a", "nokey"),
		do(rInt(0), "EXISTS", "d"),
		do(rInt(-2), "TTL", "d"), // an empty result removes key and TTL

		do(rInt(1), "RPUSH", "l", "x"), // a list destination is replaced too
		do(rInt(2), "SUNIONSTORE", "l", "a"),
		do(rInt(2), "SCARD", "l"),
	})
}

func TestSetMove(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(2), "SADD", "src", "a", "b"),
		do(rInt(1), "SADD", "dst", "c"),
		do(rInt(1), "EXPIRE", "dst", "50"),
		do(rInt(1), "SMOVE", "src", "dst", "a"),
		do(rInt(0), "SISMEMBER", "src", "a"),
		do(rInt(1), "SISMEMBER", "dst", "a"),
		do(rInt(50), "TTL", "dst"), // the destination keeps its TTL
		do(rInt(0), "SMOVE", "src", "dst", "zz"),
		do(rInt(0), "SMOVE", "nosrc", "dst", "a"),
		do(rInt(0), "EXISTS", "nosrc"),

		// The member is already at the destination: still removed from the source.
		do(rInt(1), "SADD", "src", "c"),
		do(rInt(1), "SMOVE", "src", "dst", "c"),
		do(rInt(2), "SCARD", "dst"),

		// Moving the last member deletes the source (and its TTL).
		do(rInt(1), "EXPIRE", "src", "60"),
		do(rInt(1), "SMOVE", "src", "dst", "b"),
		do(rInt(0), "EXISTS", "src"),
		do(rInt(-2), "TTL", "src"),
		do(rInt(3), "SCARD", "dst"),

		// A missing destination is created.
		do(rInt(1), "SADD", "src2", "x"),
		do(rInt(1), "SMOVE", "src2", "newdst", "x"),
		do(rInt(1), "SISMEMBER", "newdst", "x"),
		do(rInt(-1), "TTL", "newdst"),

		// Source and destination are the same key.
		do(rInt(2), "SADD", "same", "p", "q"),
		do(rInt(1), "SMOVE", "same", "same", "p"),
		do(rInt(0), "SMOVE", "same", "same", "zz"),
		do(rInt(2), "SCARD", "same"),
	})
}

func TestSetMoveWrongTypes(t *testing.T) {
	h := newHarness(t)
	h.run(t, []step{
		do(rInt(1), "SADD", "src", "a"),
		do(rOK, "SET", "s", "v"),
		do(rWrongType, "SMOVE", "s", "src", "a"),
		do(rWrongType, "SMOVE", "src", "s", "a"),
		do(rWrongType, "SMOVE", "src", "s", "zz"), // the type check comes before the membership check
		do(rInt(0), "SMOVE", "nokey", "s", "a"),   // a missing source wins, as in Redis
		do(rInt(1), "SCARD", "src"),               // nothing was moved
		do(rBulk("v"), "GET", "s"),
	})
}

func TestSetTTLInteraction(t *testing.T) {
	h := newHarness(t)
	h.ctx.Rand = rand.New(rand.NewPCG(5, 6))
	h.run(t, []step{
		do(rInt(1), "SADD", "s", "a"),
		do(rInt(1), "EXPIRE", "s", "10"),
		do(rInt(1), "SADD", "s", "b"),
		do(rInt(10), "TTL", "s"), // writes keep the TTL
		wait(11 * time.Second),
		do(rInt(0), "EXISTS", "s"),
		do(rInt(0), "SCARD", "s"),
		do(rInt(1), "SADD", "s", "z"),
		do(rInt(-1), "TTL", "s"), // an expired set must not leak its deadline

		do(rInt(1), "EXPIRE", "s", "100"),
		do(rBulk("z"), "SPOP", "s"),
		do(rInt(-2), "TTL", "s"), // emptying the set removes key and TTL together
	})
}

func TestSetLargeSequence(t *testing.T) {
	h := newHarness(t)
	h.ctx.Rand = rand.New(rand.NewPCG(7, 8))
	const n = 5000
	args := []string{"SADD", "big"}
	all := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		m := "m" + strconv.Itoa(i)
		args = append(args, m)
		all[m] = true
	}

	h.run(t, []step{do(rInt(n), args...)})
	h.run(t, []step{do(rInt(0), args...)})

	sample := parseBulkArray(t, h.do("SRANDMEMBER", "big", "100"))
	if len(sample) != 100 {
		t.Fatalf("sample has %d members", len(sample))
	}
	seen := map[string]bool{}
	for _, m := range sample {
		if !all[m] || seen[m] {
			t.Fatalf("bad sample member %q", m)
		}
		seen[m] = true
	}
	popped := parseBulkArray(t, h.do("SPOP", "big", "2500"))
	take(t, all, popped)
	h.run(t, []step{do(rInt(2500), "SCARD", "big")})
	rest := parseBulkArray(t, h.do("SPOP", "big", "5000"))
	slices.Sort(rest)
	if !slices.Equal(rest, remaining(all)) {
		t.Fatal("the second SPOP did not return exactly the remaining members")
	}
	h.run(t, []step{do(rInt(0), "EXISTS", "big")})
}
