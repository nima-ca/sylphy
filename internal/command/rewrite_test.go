package command

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
	"time"
)

// renderRewritten formats logged commands as "A b c ; D e", for comparison.
func renderRewritten(cmds [][][]byte) string {
	parts := make([]string, len(cmds))
	for i, c := range cmds {
		words := make([]string, len(c))
		for j, w := range c {
			words[j] = string(w)
		}
		parts[i] = strings.Join(words, " ")
	}
	return strings.Join(parts, " ; ")
}

// rw runs a command and returns what it would log.
func (h *harness) rw(args ...string) string {
	h.do(args...)
	return renderRewritten(h.ctx.Rewritten)
}

// dump renders every key with its type, TTL and contents, in a form that does
// not depend on set member order.
func (h *harness) dump(t *testing.T) string {
	t.Helper()
	keys := parseBulkArray(t, h.do("KEYS", "*"))
	slices.Sort(keys)
	var sb strings.Builder
	for _, k := range keys {
		typ := strings.TrimSuffix(strings.TrimPrefix(h.do("TYPE", k), "+"), "\r\n")
		fmt.Fprintf(&sb, "%s %s %q ", k, typ, h.do("PTTL", k))
		switch typ {
		case "string":
			sb.WriteString(h.do("GET", k))
		case "list":
			sb.WriteString(h.do("LRANGE", k, "0", "-1"))
		case "hash":
			sb.WriteString(h.do("HGETALL", k))
		case "set":
			ms := parseBulkArray(t, h.do("SMEMBERS", k))
			slices.Sort(ms)
			sb.WriteString(strings.Join(ms, ","))
		case "zset":
			sb.WriteString(h.do("ZRANGE", k, "0", "-1", "WITHSCORES"))
		}
		sb.WriteString("\n")
	}
	return sb.String()
}

// The fake clock stands at testEpochMs = 1_700_000_000_000 for these tests.
func TestRewriteOutputs(t *testing.T) {
	h := newHarness(t)
	h.ctx.Propagate = true
	tests := []struct {
		want string
		args []string
	}{
		// SET: options are resolved, expiries become PXAT.
		{"SET k v", []string{"SET", "k", "v"}},
		{"SET k v PXAT 1700000100000", []string{"SET", "k", "v", "EX", "100"}},
		{"SET k v PXAT 1700000001500", []string{"SET", "k", "v", "PX", "1500"}},
		{"SET k v PXAT 1700000200000", []string{"SET", "k", "v", "EXAT", "1700000200"}},
		{"SET k v PXAT 1700000300000", []string{"SET", "k", "v", "PXAT", "1700000300000"}},
		{"SET k v KEEPTTL", []string{"SET", "k", "v", "KEEPTTL"}},
		{"SET k w", []string{"SET", "k", "w", "GET"}},
		{"", []string{"SET", "k", "x", "NX"}}, // refused: nothing to log
		{"SET fresh v", []string{"SET", "fresh", "v", "NX"}},
		{"SET k y", []string{"SET", "k", "y", "XX"}},
		{"", []string{"SET", "nokey", "v", "XX"}},
		{"SET n v PXAT 1700000010000", []string{"SET", "n", "v", "NX", "EX", "10"}},
		{"", []string{"SET", "k", "v", "EX", "abc"}}, // errors log nothing
		{"", []string{"SET", "k", "v", "EX", "0"}},
		{"", []string{"SET", "k", "v", "NX", "XX"}},
		{"LPUSH l a", []string{"LPUSH", "l", "a"}}, // deterministic: verbatim
		{"", []string{"SET", "l", "v", "GET"}},     // WRONGTYPE
		// SETNX, SETEX, PSETEX.
		{"SET a 1", []string{"SETNX", "a", "1"}},
		{"", []string{"SETNX", "a", "2"}},
		{"SET s v PXAT 1700000010000", []string{"SETEX", "s", "10", "v"}},
		{"SET s v PXAT 1700000002500", []string{"PSETEX", "s", "2500", "v"}},
		{"", []string{"SETEX", "s", "0", "v"}},
		// GETEX keeps only its TTL side effect.
		{"SET g v", []string{"SET", "g", "v"}},
		{"PEXPIREAT g 1700000100000", []string{"GETEX", "g", "EX", "100"}},
		{"PEXPIREAT g 1700000050000", []string{"GETEX", "g", "PX", "50000"}},
		{"PERSIST g", []string{"GETEX", "g", "PERSIST"}},
		{"", []string{"GETEX", "g"}},
		{"", []string{"GETEX", "missing", "EX", "10"}},
		{"", []string{"GETEX", "g", "EX", "abc"}},
		{"PEXPIREAT g 1", []string{"GETEX", "g", "PXAT", "1"}}, // deleted: replays as the same deletion
		{"", []string{"GETEX", "g", "EX", "10"}},
		// GETDEL becomes DEL.
		{"SET d v", []string{"SET", "d", "v"}},
		{"DEL d", []string{"GETDEL", "d"}},
		{"", []string{"GETDEL", "d"}},
		{"", []string{"GETDEL", "l"}},
		// The EXPIRE family becomes an unconditional PEXPIREAT.
		{"SET e v", []string{"SET", "e", "v"}},
		{"PEXPIREAT e 1700000100000", []string{"EXPIRE", "e", "100"}},
		{"", []string{"EXPIRE", "e", "100", "NX"}},
		{"PEXPIREAT e 1700000001500", []string{"PEXPIRE", "e", "1500"}},
		{"PEXPIREAT e 1700000500000", []string{"EXPIREAT", "e", "1700000500"}},
		{"PEXPIREAT e 1700000600000", []string{"PEXPIREAT", "e", "1700000600000"}},
		{"", []string{"EXPIRE", "missing", "10"}},
		{"", []string{"EXPIRE", "e", "10", "GT"}}, // condition not met
		{"PEXPIREAT e 1000", []string{"EXPIREAT", "e", "1"}},
		{"", []string{"EXPIRE", "e", "10"}},
		{"", []string{"EXPIRE", "e", "x"}},
		// PERSIST.
		{"SET p v PXAT 1700000100000", []string{"SET", "p", "v", "EX", "100"}},
		{"PERSIST p", []string{"PERSIST", "p"}},
		{"", []string{"PERSIST", "p"}},
		{"", []string{"PERSIST", "nokey"}},
		// Deterministic writes replay verbatim; reads and bad requests log nothing.
		{"ZADD z 1 a", []string{"ZADD", "z", "1", "a"}},
		{"HINCRBYFLOAT h f 1.5", []string{"HINCRBYFLOAT", "h", "f", "1.5"}},
		{"RENAME l l2", []string{"RENAME", "l", "l2"}},
		{"DEL l2", []string{"DEL", "l2"}},
		{"", []string{"GET", "k"}},
		{"", []string{"TTL", "k"}},
		{"", []string{"INCR", "z"}},
		{"", []string{"SET", "k"}},
		{"", []string{"NOPE"}},
	}
	for i, tt := range tests {
		if got := h.rw(tt.args...); got != tt.want {
			t.Errorf("step %d %v: logged %q, want %q", i, tt.args, got, tt.want)
		}
	}
}

func TestRewriteSPop(t *testing.T) {
	h := newHarness(t)
	h.ctx.Propagate = true
	h.ctx.Rand = rand.New(rand.NewPCG(5, 6))

	h.do("SADD", "one", "only")
	if got := h.rw("SPOP", "one"); got != "SREM one only" {
		t.Errorf("single member: %q", got)
	}
	if h.do("EXISTS", "one") != rInt(0) {
		t.Error("the emptied set must be gone")
	}

	h.do("SADD", "many", "a", "b", "c", "d", "e")
	f := strings.Fields(h.rw("SPOP", "many"))
	if len(f) != 3 || f[0] != "SREM" || f[1] != "many" {
		t.Fatalf("SPOP logged %v", f)
	}
	if h.do("SISMEMBER", "many", f[2]) != rInt(0) || h.do("SCARD", "many") != rInt(4) {
		t.Error("the logged member must be the one removed")
	}
	f = strings.Fields(h.rw("SPOP", "many", "2"))
	if len(f) != 4 || f[0] != "SREM" {
		t.Fatalf("SPOP 2 logged %v", f)
	}
	for _, m := range f[2:] {
		if h.do("SISMEMBER", "many", m) != rInt(0) {
			t.Errorf("%s was logged but is still a member", m)
		}
	}
	f = strings.Fields(h.rw("SPOP", "many", "10")) // more than are left
	if len(f) != 4 || h.do("EXISTS", "many") != rInt(0) {
		t.Fatalf("SPOP 10 logged %v", f)
	}

	h.do("SADD", "again", "x")
	for _, args := range [][]string{
		{"SPOP", "missing"}, {"SPOP", "missing", "3"}, {"SPOP", "again", "0"}, {"SPOP", "again", "-1"},
	} {
		if got := h.rw(args...); got != "" {
			t.Errorf("%v logged %q, want nothing", args, got)
		}
	}
	h.do("SET", "str", "v")
	if got := h.rw("SPOP", "str"); got != "" {
		t.Errorf("WRONGTYPE logged %q", got)
	}
}

func TestRewrittenOnlyWhilePropagating(t *testing.T) {
	h := newHarness(t)
	h.do("SET", "k", "v")
	if h.ctx.Rewritten != nil {
		t.Fatal("Rewritten must stay empty while Propagate is off")
	}
	h.ctx.Propagate = true
	if got := h.rw("SET", "k", "v"); got != "SET k v" {
		t.Fatalf("got %q", got)
	}
	// Every Dispatch resets it, whatever the outcome.
	for _, args := range [][]string{{"GET", "k"}, {"SET", "k"}, {"NOPE"}, {"INCR", "k"}, {}} {
		h.do("SET", "k", "v")
		if got := h.rw(args...); got != "" {
			t.Errorf("%v left %q behind", args, got)
		}
	}
}

func TestRewrittenDoesNotAliasTheRequest(t *testing.T) {
	h := newHarness(t)
	h.ctx.Propagate = true
	argv := [][]byte{[]byte("SET"), []byte("key"), []byte("val"), []byte("EX"), []byte("10")}
	h.d.Dispatch(h.ctx, argv)
	for _, a := range argv { // the reader reuses its buffers after Dispatch
		for i := range a {
			a[i] = 'X'
		}
	}
	if got := renderRewritten(h.ctx.Rewritten); got != "SET key val PXAT 1700000010000" {
		t.Fatalf("rewritten command changed with the request buffers: %q", got)
	}
}

func TestRewriteHookRequiresWriteCommand(t *testing.T) {
	r := NewRegistry()
	err := r.Register(Spec{Name: "BAD", Arity: 1, Flags: FlagReadOnly, Handler: cmdPing, Rewrite: rewriteSet})
	if err == nil {
		t.Fatal("a read-only command with a Rewrite hook must be rejected")
	}
}

// TestEveryWriteCommandDecidedOnRewrite forces each write command to be either
// hooked or consciously listed as deterministic, so a new write command cannot
// reach the AOF unreviewed.
func TestEveryWriteCommandDecidedOnRewrite(t *testing.T) {
	hooked := []string{
		"SET", "SETNX", "SETEX", "PSETEX", "GETEX", "GETDEL",
		"EXPIRE", "PEXPIRE", "EXPIREAT", "PEXPIREAT", "PERSIST", "SPOP",
	}
	// Deterministic: replaying argv against the same state gives the same state.
	// HINCRBYFLOAT uses exact decimal arithmetic and ZINCRBY/ZADD INCR use IEEE
	// doubles, so neither depends on the platform; the *STORE and RENAME forms
	// copy values and TTLs without consulting the clock.
	verbatim := []string{
		"FLUSHALL", "DEL", "UNLINK", "RENAME", "RENAMENX", "MSET",
		"INCR", "DECR", "INCRBY", "DECRBY", "APPEND",
		"LPUSH", "RPUSH", "LPUSHX", "RPUSHX", "LPOP", "RPOP", "LSET", "LREM", "LTRIM", "LINSERT",
		"HSET", "HSETNX", "HDEL", "HINCRBY", "HINCRBYFLOAT",
		"SADD", "SREM", "SMOVE", "SUNIONSTORE", "SINTERSTORE", "SDIFFSTORE",
		"ZADD", "ZREM", "ZINCRBY", "ZREMRANGEBYRANK", "ZREMRANGEBYSCORE", "ZPOPMIN", "ZPOPMAX",
	}
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range hooked {
		if _, ok := r.Lookup([]byte(name)); !ok {
			t.Errorf("hooked list names unknown command %s", name)
		}
	}
	for _, name := range r.Names() {
		sp, _ := r.Lookup([]byte(name))
		switch {
		case sp.Flags&FlagWrite == 0:
			if sp.Rewrite != nil {
				t.Errorf("read command %s has a Rewrite hook", name)
			}
		case slices.Contains(hooked, name):
			if sp.Rewrite == nil {
				t.Errorf("%s must have a Rewrite hook", name)
			}
		case slices.Contains(verbatim, name):
			if sp.Rewrite != nil {
				t.Errorf("%s has a hook but is listed as deterministic", name)
			}
		default:
			t.Errorf("write command %s has no Rewrite hook and is not on the audited deterministic list", name)
		}
	}
}

// TestReplayReproducesState runs a mixed workload with Propagate on, replays
// only what was logged against a second server, and expects identical state,
// TTLs included, both immediately and after the clock moves on.
func TestReplayReproducesState(t *testing.T) {
	workload := [][]string{
		{"SET", "a", "1", "EX", "100"}, {"SET", "b", "2"}, {"SET", "b", "3", "KEEPTTL"},
		{"SETEX", "c", "50", "v"}, {"PSETEX", "d", "7500", "w"},
		{"GETEX", "a", "PERSIST"}, {"GETEX", "b", "EX", "30"}, {"SET", "a", "z", "GET"},
		{"SET", "e", "1", "NX", "PX", "900"}, {"EXPIRE", "e", "5"}, {"PEXPIRE", "b", "40000"},
		{"EXPIREAT", "c", "1700000999"}, {"PERSIST", "d"}, {"GETDEL", "a"},
		{"SADD", "s", "a", "b", "c", "d", "e", "f", "g"}, {"SPOP", "s"}, {"SPOP", "s", "3"},
		{"SADD", "s2", "x"}, {"SPOP", "s2"},
		{"RPUSH", "l", "1", "2", "3"}, {"LPOP", "l"},
		{"HSET", "h", "f", "1"}, {"HINCRBYFLOAT", "h", "f", "0.5"},
		{"ZADD", "z", "1", "a", "2", "b"}, {"ZINCRBY", "z", "2.5", "a"},
		{"RENAME", "c", "c2"}, {"SET", "tmp", "v", "EX", "1"}, {"DEL", "tmp"},
		{"SET", "gone", "v", "PX", "100"}, {"EXPIREAT", "gone", "1"},
	}
	a, b := newHarness(t), newHarness(t)
	a.ctx.Propagate = true
	a.ctx.Rand = rand.New(rand.NewPCG(11, 12))

	var logged [][]string
	for _, args := range workload {
		a.do(args...)
		for _, cmd := range a.ctx.Rewritten {
			words := make([]string, len(cmd))
			for i, w := range cmd {
				words[i] = string(w)
			}
			logged = append(logged, words)
		}
	}
	if len(logged) == 0 {
		t.Fatal("nothing was logged")
	}
	for _, args := range logged {
		if reply := b.do(args...); strings.HasPrefix(reply, "-") {
			t.Fatalf("replaying %v failed: %q", args, reply)
		}
	}
	if got, want := b.dump(t), a.dump(t); got != want {
		t.Fatalf("state diverged after replay\nreplayed:\n%s\noriginal:\n%s", got, want)
	}
	a.clk.Advance(10 * time.Second)
	b.clk.Advance(10 * time.Second)
	if got, want := b.dump(t), a.dump(t); got != want {
		t.Fatalf("state diverged after the clock moved\nreplayed:\n%s\noriginal:\n%s", got, want)
	}
}
