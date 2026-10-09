package integration

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

func TestPhase1Commands(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	str := func(cmd *redis.StringCmd) string {
		t.Helper()
		v, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	num := func(cmd *redis.IntCmd) int64 {
		t.Helper()
		v, err := cmd.Result()
		if err != nil {
			t.Fatal(err)
		}
		return v
	}

	eq(t, "PING", c.Ping(ctx).Val(), "PONG")
	eq(t, "ECHO", str(c.Echo(ctx, "hello")), "hello")
	eq(t, "SET", c.Set(ctx, "k", "v", 0).Val(), "OK")
	eq(t, "GET", str(c.Get(ctx, "k")), "v")
	if err := c.Get(ctx, "missing").Err(); !errors.Is(err, redis.Nil) {
		t.Fatalf("GET missing: %v", err)
	}
	eq(t, "EXISTS", num(c.Exists(ctx, "k", "k", "missing")), 2)
	eq(t, "STRLEN", num(c.StrLen(ctx, "k")), 1)
	eq(t, "APPEND", num(c.Append(ctx, "k", "xyz")), 4)
	eq(t, "GET after APPEND", str(c.Get(ctx, "k")), "vxyz")

	eq(t, "INCR", num(c.Incr(ctx, "n")), 1)
	eq(t, "INCRBY", num(c.IncrBy(ctx, "n", 10)), 11)
	eq(t, "DECR", num(c.Decr(ctx, "n")), 10)
	eq(t, "DECRBY", num(c.DecrBy(ctx, "n", 20)), -10)

	eq(t, "MSET", c.MSet(ctx, "a", "1", "b", "2").Val(), "OK")
	vals, err := c.MGet(ctx, "a", "zz", "b").Result()
	if err != nil || fmt.Sprint(vals) != "[1 <nil> 2]" {
		t.Fatalf("MGET: %v %v", vals, err)
	}

	eq(t, "DBSIZE", num(c.DBSize(ctx)), 4) // k n a b + (none other)
	keys, err := c.Keys(ctx, "*").Result()
	if err != nil {
		t.Fatal(err)
	}
	sort.Strings(keys)
	eq(t, "KEYS", fmt.Sprint(keys), "[a b k n]")
	eq(t, "DEL", num(c.Del(ctx, "a", "b", "nope")), 2)

	eq(t, "FLUSHALL", c.FlushAll(ctx).Val(), "OK")
	eq(t, "DBSIZE after flush", num(c.DBSize(ctx)), 0)
	eq(t, "SELECT 0", c.Do(ctx, "SELECT", "0").Val(), "OK")
	eq(t, "CLIENT SETNAME", c.Do(ctx, "CLIENT", "SETNAME", "it").Val(), "OK")
}

func TestErrorReplies(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	_ = c.Set(ctx, "s", "abc", 0).Err()
	_ = c.Set(ctx, "big", "9223372036854775807", 0).Err()
	tests := []struct {
		name string
		err  error
		want string
	}{
		{"incr non-integer", c.Incr(ctx, "s").Err(), "value is not an integer or out of range"},
		{"incr overflow", c.Incr(ctx, "big").Err(), "increment or decrement would overflow"},
		{"wrong arity", c.Do(ctx, "GET").Err(), "wrong number of arguments for 'get' command"},
		{"unknown command", c.Do(ctx, "FOO", "bar").Err(), "unknown command 'FOO', with args beginning with: 'bar'"},
		{"set option", c.Do(ctx, "SET", "k", "v", "BOGUS").Err(), "syntax error"},
		{"select 1", c.Do(ctx, "SELECT", "1").Err(), "DB index is out of range"},
	}
	for _, tt := range tests {
		if tt.err == nil || !strings.Contains(tt.err.Error(), tt.want) {
			t.Errorf("%s: err = %v, want it to contain %q", tt.name, tt.err, tt.want)
		}
	}
	// The connection must still be usable after error replies.
	eq(t, "PING after errors", c.Ping(ctx).Val(), "PONG")
}

func TestPipelining(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	pipe := c.Pipeline()
	const n = 1000
	for i := 0; i < n; i++ {
		pipe.Set(ctx, fmt.Sprintf("p:%d", i), i, 0)
	}
	for i := 0; i < n; i++ {
		pipe.Get(ctx, fmt.Sprintf("p:%d", i))
	}
	cmds, err := pipe.Exec(ctx)
	if err != nil {
		t.Fatal(err)
	}
	eq(t, "pipeline length", len(cmds), 2*n)
	for i := 0; i < n; i++ {
		got, _ := cmds[n+i].(*redis.StringCmd).Result()
		eq(t, "ordered reply", got, fmt.Sprint(i))
	}
}

func TestConcurrentClients(t *testing.T) {
	ts := startServer(t, nil)
	ctx := context.Background()
	const clients, per = 32, 200

	var wg sync.WaitGroup
	for i := 0; i < clients; i++ {
		c := newClient(t, ts.addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < per; j++ {
				if err := c.Incr(ctx, "shared").Err(); err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	v, err := newClient(t, ts.addr).Get(ctx, "shared").Int()
	if err != nil || v != clients*per {
		t.Fatalf("counter = %d (err %v), want %d", v, err, clients*per)
	}
}

func TestBinarySafeValues(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	val := []byte("a\r\nb\x00\xff\r\n$3\r\n*1\r\n")
	if err := c.Set(ctx, "bin", val, 0).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "bin").Bytes()
	if err != nil || !bytes.Equal(got, val) {
		t.Fatalf("got %q err %v", got, err)
	}
}

func TestLargeValue(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	val := bytes.Repeat([]byte("0123456789abcdef"), 2<<16+3) // ~2 MiB
	if err := c.Set(ctx, "large", val, 0).Err(); err != nil {
		t.Fatal(err)
	}
	got, err := c.Get(ctx, "large").Bytes()
	if err != nil || !bytes.Equal(got, val) {
		t.Fatalf("large value mismatch (len %d, err %v)", len(got), err)
	}
	n, err := c.StrLen(ctx, "large").Result()
	if err != nil || n != int64(len(val)) {
		t.Fatalf("STRLEN = %d err %v", n, err)
	}
}
