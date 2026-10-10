package integration

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nima-ca/sylphy/internal/store"
)

// wireEpochMs is where the fake clock starts in these tests.
const wireEpochMs = 1_700_000_000_000

// wInt runs a command and returns its integer reply.
func wInt(t *testing.T, c *redis.Client, args ...any) int64 {
	t.Helper()
	v, err := c.Do(context.Background(), args...).Int64()
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

// wText runs a command and returns its string or status reply.
func wText(t *testing.T, c *redis.Client, args ...any) string {
	t.Helper()
	v, err := c.Do(context.Background(), args...).Text()
	if err != nil {
		t.Fatalf("%v: %v", args, err)
	}
	return v
}

// wNil runs a command and requires a nil reply.
func wNil(t *testing.T, c *redis.Client, args ...any) {
	t.Helper()
	if err := c.Do(context.Background(), args...).Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("%v: got err %v, want a nil reply", args, err)
	}
}

// wErr runs a command and returns its error text, failing if it succeeds.
func wErr(t *testing.T, c *redis.Client, args ...any) string {
	t.Helper()
	err := c.Do(context.Background(), args...).Err()
	if err == nil {
		t.Fatalf("%v: expected an error reply", args)
	}
	return err.Error()
}

// eventually polls cond until it holds. It is for waiting on the real-time
// sweeper ticker only; expiry itself is driven by the fake clock.
func eventually(t *testing.T, limit time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(limit)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTTLOverTheWire(t *testing.T) {
	clk := store.NewFakeClock(wireEpochMs)
	ts := startServerWithClock(t, clk, nil)
	c := newClient(t, ts.addr)

	eq(t, "SET EX", wText(t, c, "SET", "k", "v", "EX", 100), "OK")
	eq(t, "TTL", wInt(t, c, "TTL", "k"), 100)
	eq(t, "PTTL", wInt(t, c, "PTTL", "k"), 100_000)
	eq(t, "EXPIRETIME", wInt(t, c, "EXPIRETIME", "k"), wireEpochMs/1000+100)
	eq(t, "PEXPIRETIME", wInt(t, c, "PEXPIRETIME", "k"), wireEpochMs+100_000)

	clk.Advance(30 * time.Second)
	eq(t, "TTL after 30s", wInt(t, c, "TTL", "k"), 70)
	eq(t, "EXPIRE GT lower", wInt(t, c, "EXPIRE", "k", 50, "GT"), 0)
	eq(t, "EXPIRE GT higher", wInt(t, c, "EXPIRE", "k", 200, "GT"), 1)
	eq(t, "TTL", wInt(t, c, "TTL", "k"), 200)
	eq(t, "PERSIST", wInt(t, c, "PERSIST", "k"), 1)
	eq(t, "TTL none", wInt(t, c, "TTL", "k"), -1)
	eq(t, "PERSIST again", wInt(t, c, "PERSIST", "k"), 0)
	eq(t, "TTL missing", wInt(t, c, "TTL", "nokey"), -2)

	// Millisecond precision, right at the edge.
	eq(t, "SET PX", wText(t, c, "SET", "k2", "v", "PX", 1500), "OK")
	eq(t, "PTTL", wInt(t, c, "PTTL", "k2"), 1500)
	clk.Advance(1499 * time.Millisecond)
	eq(t, "alive at 1499ms", wText(t, c, "GET", "k2"), "v")
	clk.Advance(2 * time.Millisecond)
	wNil(t, c, "GET", "k2")
	eq(t, "TTL expired", wInt(t, c, "TTL", "k2"), -2)
	eq(t, "EXISTS expired", wInt(t, c, "EXISTS", "k2"), 0)

	// KEEPTTL, GETEX, GETDEL.
	eq(t, "SET", wText(t, c, "SET", "k3", "v", "EX", 100), "OK")
	eq(t, "SET KEEPTTL", wText(t, c, "SET", "k3", "w", "KEEPTTL"), "OK")
	eq(t, "TTL kept", wInt(t, c, "TTL", "k3"), 100)
	eq(t, "plain SET", wText(t, c, "SET", "k3", "x"), "OK")
	eq(t, "TTL cleared", wInt(t, c, "TTL", "k3"), -1)
	eq(t, "SET EX", wText(t, c, "SET", "k3", "y", "EX", 10), "OK")
	eq(t, "GETEX PERSIST", wText(t, c, "GETEX", "k3", "PERSIST"), "y")
	eq(t, "TTL after GETEX PERSIST", wInt(t, c, "TTL", "k3"), -1)
	eq(t, "GETEX EX", wText(t, c, "GETEX", "k3", "EX", 20), "y")
	eq(t, "TTL after GETEX EX", wInt(t, c, "TTL", "k3"), 20)
	eq(t, "GETDEL", wText(t, c, "GETDEL", "k3"), "y")
	wNil(t, c, "GET", "k3")

	// SETEX, PSETEX, SETNX and the SET conditions.
	eq(t, "SETEX", wText(t, c, "SETEX", "k4", 30, "v"), "OK")
	eq(t, "TTL SETEX", wInt(t, c, "TTL", "k4"), 30)
	eq(t, "PSETEX", wText(t, c, "PSETEX", "k5", 2500, "v"), "OK")
	eq(t, "PTTL PSETEX", wInt(t, c, "PTTL", "k5"), 2500)
	eq(t, "SETNX", wInt(t, c, "SETNX", "k6", "v"), 1)
	eq(t, "SETNX again", wInt(t, c, "SETNX", "k6", "w"), 0)
	eq(t, "SET NX EX", wText(t, c, "SET", "k7", "v", "EX", 10, "NX"), "OK")
	wNil(t, c, "SET", "k7", "w", "NX")
	eq(t, "SET GET", wText(t, c, "SET", "k7", "new", "GET"), "v")
	if msg := wErr(t, c, "SET", "k", "v", "EX", 0); !strings.HasPrefix(msg, "ERR invalid expire time") {
		t.Fatalf("SET EX 0: %q", msg)
	}
}

func TestCollectionsExpireOverTheWire(t *testing.T) {
	clk := store.NewFakeClock(wireEpochMs)
	ts := startServerWithClock(t, clk, nil)
	c := newClient(t, ts.addr)

	creates := [][]any{
		{"RPUSH", "l", "a"}, {"HSET", "h", "f", "v"}, {"SADD", "s", "m"}, {"ZADD", "z", "1", "m"},
	}
	for _, create := range creates {
		wInt(t, c, create...)
		eq(t, "EXPIRE "+create[1].(string), wInt(t, c, "EXPIRE", create[1], 10), 1)
	}
	// Writing keeps the TTL.
	wInt(t, c, "RPUSH", "l", "b")
	eq(t, "TTL after RPUSH", wInt(t, c, "TTL", "l"), 10)

	clk.Advance(9 * time.Second)
	eq(t, "all alive", wInt(t, c, "EXISTS", "l", "h", "s", "z"), 4)
	clk.Advance(2 * time.Second)
	eq(t, "all expired", wInt(t, c, "EXISTS", "l", "h", "s", "z"), 0)
	eq(t, "TYPE expired", wText(t, c, "TYPE", "l"), "none")
	eq(t, "LLEN expired", wInt(t, c, "LLEN", "l"), 0)
	eq(t, "ZCARD expired", wInt(t, c, "ZCARD", "z"), 0)

	// An expired collection is replaced by a fresh one with no TTL.
	eq(t, "ZADD fresh", wInt(t, c, "ZADD", "z", "1", "new"), 1)
	eq(t, "no TTL", wInt(t, c, "TTL", "z"), -1)
}

func TestRenameMovesTTLOverTheWire(t *testing.T) {
	clk := store.NewFakeClock(wireEpochMs)
	ts := startServerWithClock(t, clk, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	eq(t, "SET", wText(t, c, "SET", "a", "v", "EX", 100), "OK")
	eq(t, "RENAME", wText(t, c, "RENAME", "a", "b"), "OK")
	eq(t, "old TTL", wInt(t, c, "TTL", "a"), -2)
	eq(t, "new TTL", wInt(t, c, "TTL", "b"), 100)
	eq(t, "new PTTL", wInt(t, c, "PTTL", "b"), 100_000)
	clk.Advance(99 * time.Second)
	eq(t, "TTL later", wInt(t, c, "TTL", "b"), 1)
	clk.Advance(2 * time.Second)
	wNil(t, c, "GET", "b")
	eq(t, "gone", wInt(t, c, "EXISTS", "a", "b"), 0)

	// The destination's own TTL is discarded.
	wText(t, c, "SET", "d1", "x", "EX", 50)
	wText(t, c, "SET", "s1", "y")
	eq(t, "RENAME onto TTL key", wText(t, c, "RENAME", "s1", "d1"), "OK")
	eq(t, "TTL cleared", wInt(t, c, "TTL", "d1"), -1)
	eq(t, "value", wText(t, c, "GET", "d1"), "y")

	// A collection keeps its type, elements and TTL.
	wInt(t, c, "RPUSH", "l", "x", "y")
	wInt(t, c, "PEXPIRE", "l", 5000)
	eq(t, "RENAME list", wText(t, c, "RENAME", "l", "l2"), "OK")
	eq(t, "list PTTL", wInt(t, c, "PTTL", "l2"), 5000)
	eq(t, "list TYPE", wText(t, c, "TYPE", "l2"), "list")
	if got := strings.Join(c.LRange(ctx, "l2", 0, -1).Val(), ","); got != "x,y" {
		t.Fatalf("LRANGE after RENAME: %q", got)
	}
	clk.Advance(5001 * time.Millisecond)
	eq(t, "list expired", wInt(t, c, "EXISTS", "l2"), 0)

	// Errors and edge cases.
	eq(t, "missing source", wErr(t, c, "RENAME", "nokey", "x"), "ERR no such key")
	wText(t, c, "SET", "same", "v")
	eq(t, "RENAME onto itself", wText(t, c, "RENAME", "same", "same"), "OK")
	eq(t, "RENAMENX taken", wInt(t, c, "RENAMENX", "same", "d1"), 0)
	eq(t, "RENAMENX free", wInt(t, c, "RENAMENX", "same", "free"), 1)
	eq(t, "moved", wText(t, c, "GET", "free"), "v")
}

// TestSweeperReclaimsNeverReadKeys expires keys by moving the fake clock and
// waits for the real-ticker sweeper to delete them without any client ever
// reading them: lazy expiry never fires, so every reclaim is the sweeper's.
func TestSweeperReclaimsNeverReadKeys(t *testing.T) {
	clk := store.NewFakeClock(wireEpochMs)
	ts := startServerWithClock(t, clk, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	const volatile, persistent = 2000, 100
	_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < volatile; i++ {
			p.Set(ctx, "tmp:"+itoa(i), "v", time.Second)
		}
		for i := 0; i < persistent; i++ {
			p.Set(ctx, "keep:"+itoa(i), "v", 0)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	clk.Advance(2 * time.Second)
	eventually(t, 10*time.Second, "the sweeper to reclaim every expired key", func() bool {
		return ts.st.Stats().ExpiredActive >= volatile
	})
	st := ts.st.Stats()
	eq(t, "lazy expirations", st.ExpiredLazy, 0)
	eq(t, "DBSIZE", c.DBSize(ctx).Val(), int64(persistent))
}

func itoa(i int) string { return strings.TrimSpace(strings.Join([]string{intStr(i)}, "")) }

func intStr(i int) string {
	if i == 0 {
		return "0"
	}
	var b [20]byte
	pos := len(b)
	for ; i > 0; i /= 10 {
		pos--
		b[pos] = byte('0' + i%10)
	}
	return string(b[pos:])
}
