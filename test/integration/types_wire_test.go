package integration

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func TestWrongTypeMatrixOverTheWire(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	for _, cmd := range [][]any{
		{"SET", "k_string", "v"}, {"RPUSH", "k_list", "a"}, {"HSET", "k_hash", "f", "v"},
		{"SADD", "k_set", "m"}, {"ZADD", "k_zset", "1", "m"}, {"SADD", "other", "m"},
	} {
		if err := c.Do(ctx, cmd...).Err(); err != nil {
			t.Fatalf("setup %v: %v", cmd, err)
		}
	}
	keys := map[string]string{
		"string": "k_string", "list": "k_list", "hash": "k_hash", "set": "k_set", "zset": "k_zset",
	}
	families := map[string][][]any{
		"string": {
			{"GET", "KEY"}, {"APPEND", "KEY", "x"}, {"INCR", "KEY"}, {"STRLEN", "KEY"},
			{"GETDEL", "KEY"}, {"GETEX", "KEY", "EX", 10}, {"SET", "KEY", "v", "GET"},
		},
		"list": {
			{"LPUSH", "KEY", "a"}, {"RPOP", "KEY"}, {"LLEN", "KEY"}, {"LRANGE", "KEY", 0, -1},
			{"LINDEX", "KEY", 0}, {"LREM", "KEY", 0, "a"},
		},
		"hash": {
			{"HSET", "KEY", "f", "v"}, {"HGET", "KEY", "f"}, {"HDEL", "KEY", "f"},
			{"HGETALL", "KEY"}, {"HLEN", "KEY"}, {"HINCRBY", "KEY", "f", 1},
		},
		"set": {
			{"SADD", "KEY", "m"}, {"SREM", "KEY", "m"}, {"SMEMBERS", "KEY"}, {"SCARD", "KEY"},
			{"SPOP", "KEY"}, {"SMOVE", "KEY", "other", "m"}, {"SUNION", "KEY", "other"},
		},
		"zset": {
			{"ZADD", "KEY", 1, "m"}, {"ZREM", "KEY", "m"}, {"ZSCORE", "KEY", "m"},
			{"ZRANGE", "KEY", 0, -1}, {"ZRANK", "KEY", "m"}, {"ZPOPMIN", "KEY"},
			{"ZINCRBY", "KEY", 1, "m"}, {"ZCOUNT", "KEY", "-inf", "+inf"}, {"ZREMRANGEBYRANK", "KEY", 0, -1},
		},
	}
	for family, cmds := range families {
		for typ, key := range keys {
			if typ == family {
				continue
			}
			for _, cmd := range cmds {
				argv := make([]any, len(cmd))
				for i, a := range cmd {
					argv[i] = a
					if a == "KEY" {
						argv[i] = key
					}
				}
				err := c.Do(ctx, argv...).Err()
				if err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE ") {
					t.Errorf("%s command %v against a %s key: err = %v, want WRONGTYPE", family, argv, typ, err)
				}
			}
		}
	}

	// The rejected commands changed nothing, and the type-agnostic commands
	// work on every type.
	for typ, key := range keys {
		eq(t, typ+" TYPE", c.Type(ctx, key).Val(), typ)
		eq(t, typ+" EXPIRE", c.Expire(ctx, key, 100*time.Second).Val(), true)
		if ttl := c.TTL(ctx, key).Val(); ttl <= 0 || ttl > 100*time.Second {
			t.Errorf("%s TTL = %v", typ, ttl)
		}
		eq(t, typ+" PERSIST", c.Persist(ctx, key).Val(), true)
		moved := key + "_moved"
		eq(t, typ+" RENAME", c.Rename(ctx, key, moved).Val(), "OK")
		eq(t, typ+" TYPE after RENAME", c.Type(ctx, moved).Val(), typ)
		eq(t, typ+" RENAME back", c.Rename(ctx, moved, key).Val(), "OK")
		eq(t, typ+" TOUCH", c.Touch(ctx, key).Val(), int64(1))
	}
}

func TestEmptyCollectionsAreDeleted(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	cases := []struct {
		name   string
		create [][]any
		drain  []any
	}{
		{"list LPOP count", [][]any{{"RPUSH", "k", "a", "b"}}, []any{"LPOP", "k", 2}},
		{"list RPOP", [][]any{{"RPUSH", "k", "a"}}, []any{"RPOP", "k"}},
		{"list LTRIM", [][]any{{"RPUSH", "k", "a", "b"}}, []any{"LTRIM", "k", 1, 0}},
		{"list LREM", [][]any{{"RPUSH", "k", "a", "a"}}, []any{"LREM", "k", 0, "a"}},
		{"hash HDEL", [][]any{{"HSET", "k", "f", "v", "g", "w"}}, []any{"HDEL", "k", "f", "g"}},
		{"set SPOP count", [][]any{{"SADD", "k", "a", "b", "c"}}, []any{"SPOP", "k", 3}},
		{"set SREM", [][]any{{"SADD", "k", "a"}}, []any{"SREM", "k", "a"}},
		{"set SMOVE", [][]any{{"SADD", "k", "a"}}, []any{"SMOVE", "k", "dest", "a"}},
		{"set SINTERSTORE empty", [][]any{{"SADD", "k", "a"}, {"SADD", "o", "b"}}, []any{"SINTERSTORE", "k", "k", "o"}},
		{"zset ZREM", [][]any{{"ZADD", "k", 1, "a", 2, "b"}}, []any{"ZREM", "k", "a", "b"}},
		{"zset ZPOPMIN", [][]any{{"ZADD", "k", 1, "a"}}, []any{"ZPOPMIN", "k"}},
		{"zset ZPOPMAX count", [][]any{{"ZADD", "k", 1, "a", 2, "b"}}, []any{"ZPOPMAX", "k", 5}},
		{"zset ZREMRANGEBYRANK", [][]any{{"ZADD", "k", 1, "a", 2, "b"}}, []any{"ZREMRANGEBYRANK", "k", 0, -1}},
		{"zset ZREMRANGEBYSCORE", [][]any{{"ZADD", "k", 1, "a"}}, []any{"ZREMRANGEBYSCORE", "k", "-inf", "+inf"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := c.FlushAll(ctx).Err(); err != nil {
				t.Fatal(err)
			}
			create := func() {
				for _, cmd := range tc.create {
					if err := c.Do(ctx, cmd...).Err(); err != nil {
						t.Fatalf("%v: %v", cmd, err)
					}
				}
			}
			create()
			eq(t, "EXPIRE", wInt(t, c, "EXPIRE", "k", 100), 1)
			if err := c.Do(ctx, tc.drain...).Err(); err != nil {
				t.Fatalf("%v: %v", tc.drain, err)
			}
			eq(t, "EXISTS", wInt(t, c, "EXISTS", "k"), 0)
			eq(t, "TYPE", wText(t, c, "TYPE", "k"), "none")
			eq(t, "TTL", wInt(t, c, "TTL", "k"), -2)
			// Re-creating must not resurrect the old TTL.
			create()
			eq(t, "TTL after re-create", wInt(t, c, "TTL", "k"), -1)
		})
	}
}

func TestKeyCommandsOverTheWire(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	if err := c.RandomKey(ctx).Err(); err != redis.Nil {
		t.Fatalf("RANDOMKEY on an empty keyspace: %v", err)
	}
	c.Set(ctx, "a", "1", 0)
	eq(t, "RANDOMKEY", c.RandomKey(ctx).Val(), "a")
	c.Set(ctx, "b", "2", 0)
	eq(t, "TOUCH", c.Touch(ctx, "a", "b", "zz").Val(), int64(2))
	eq(t, "UNLINK", c.Unlink(ctx, "a", "b", "zz").Val(), int64(2))
	eq(t, "TYPE gone", c.Type(ctx, "a").Val(), "none")
	eq(t, "RENAMENX", c.RenameNX(ctx, "x", "y").Err() != nil, true) // missing source
}

func TestSortedSetOverTheWire(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	eq(t, "ZADD", c.ZAdd(ctx, "z", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"}, redis.Z{Score: 3, Member: "c"}).Val(), int64(3))
	eq(t, "ZCARD", c.ZCard(ctx, "z").Val(), int64(3))
	eq(t, "ZSCORE", c.ZScore(ctx, "z", "b").Val(), 2.0)
	eq(t, "ZRANK", c.ZRank(ctx, "z", "c").Val(), int64(2))
	eq(t, "ZREVRANK", c.ZRevRank(ctx, "z", "c").Val(), int64(0))
	eq(t, "ZINCRBY", c.ZIncrBy(ctx, "z", 2.5, "a").Val(), 3.5)
	eq(t, "ZRANGE", strings.Join(c.ZRange(ctx, "z", 0, -1).Val(), ","), "b,c,a")
	eq(t, "ZREVRANGE", strings.Join(c.ZRevRange(ctx, "z", 0, 0).Val(), ","), "a")
	ws := c.ZRangeWithScores(ctx, "z", 0, -1).Val()
	eq(t, "WITHSCORES", fmt.Sprint(ws), "[{2 b} {3 c} {3.5 a}]")
	byScore := c.ZRangeByScore(ctx, "z", &redis.ZRangeBy{Min: "(2", Max: "+inf", Offset: 0, Count: 10}).Val()
	eq(t, "ZRANGEBYSCORE", strings.Join(byScore, ","), "c,a")
	eq(t, "ZCOUNT", c.ZCount(ctx, "z", "-inf", "3").Val(), int64(2))
	rev := c.ZRangeArgs(ctx, redis.ZRangeArgs{Key: "z", Start: 2, Stop: "+inf", ByScore: true, Rev: true}).Val()
	eq(t, "ZRANGE BYSCORE REV", strings.Join(rev, ","), "a,c,b")

	// Option forms and replies that go-redis has no typed helper for.
	eq(t, "ZADD GT CH", wInt(t, c, "ZADD", "z", "GT", "CH", 1, "a", 99, "d"), 1+1-1) // a refused, d added
	eq(t, "ZADD NX", wInt(t, c, "ZADD", "z", "NX", 50, "d", 60, "e"), 1)
	eq(t, "ZADD INCR", wText(t, c, "ZADD", "z", "INCR", 1.5, "e"), "61.5")
	wNil(t, c, "ZADD", "z", "XX", "INCR", 1, "nobody")
	if msg := wErr(t, c, "ZADD", "z", "NX", "XX", 1, "x"); !strings.HasPrefix(msg, "ERR XX and NX") {
		t.Fatalf("NX XX: %q", msg)
	}
	if msg := wErr(t, c, "ZADD", "z", "nan", "x"); msg != "ERR value is not a valid float" {
		t.Fatalf("nan score: %q", msg)
	}
	rs, err := c.Do(ctx, "ZRANK", "z", "b", "WITHSCORE").Slice()
	if err != nil || fmt.Sprint(rs) != "[0 2]" {
		t.Fatalf("ZRANK WITHSCORE = %v, %v", rs, err)
	}
	ms, err := c.Do(ctx, "ZMSCORE", "z", "b", "nobody").Slice()
	if err != nil || fmt.Sprint(ms) != "[2 <nil>]" {
		t.Fatalf("ZMSCORE = %v, %v", ms, err)
	}

	eq(t, "ZPOPMIN", fmt.Sprint(c.ZPopMin(ctx, "z", 1).Val()), "[{2 b}]")
	eq(t, "ZPOPMAX", fmt.Sprint(c.ZPopMax(ctx, "z", 1).Val()), "[{99 d}]")
	eq(t, "ZREM", c.ZRem(ctx, "z", "c", "nobody").Val(), int64(1))
	eq(t, "ZREMRANGEBYSCORE", c.ZRemRangeByScore(ctx, "z", "-inf", "+inf").Val(), int64(2))
	eq(t, "emptied", c.Exists(ctx, "z").Val(), int64(0))
}

func TestPipeliningAcrossTypes(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	var typS, typL, typH, typSt, typZ *redis.StatusCmd
	var lrange *redis.StringSliceCmd
	var hgetall *redis.MapStringStringCmd
	var scard, after *redis.IntCmd
	var zr *redis.ZSliceCmd
	var bad *redis.StringCmd
	_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
		p.Set(ctx, "s", "v", 0)
		p.RPush(ctx, "l", "a", "b", "c")
		p.HSet(ctx, "h", "f", "1")
		p.SAdd(ctx, "st", "x", "y")
		p.ZAdd(ctx, "z", redis.Z{Score: 1, Member: "a"}, redis.Z{Score: 2, Member: "b"})
		typS, typL, typH, typSt, typZ = p.Type(ctx, "s"), p.Type(ctx, "l"), p.Type(ctx, "h"), p.Type(ctx, "st"), p.Type(ctx, "z")
		lrange = p.LRange(ctx, "l", 0, -1)
		hgetall = p.HGetAll(ctx, "h")
		scard = p.SCard(ctx, "st")
		zr = p.ZRangeWithScores(ctx, "z", 0, -1)
		bad = p.Get(ctx, "l") // WRONGTYPE in the middle of the pipeline
		after = p.LLen(ctx, "l")
		return nil
	})
	if err == nil || !strings.HasPrefix(err.Error(), "WRONGTYPE ") {
		t.Fatalf("pipeline error = %v, want the WRONGTYPE from GET on a list", err)
	}
	eq(t, "TYPE s", typS.Val(), "string")
	eq(t, "TYPE l", typL.Val(), "list")
	eq(t, "TYPE h", typH.Val(), "hash")
	eq(t, "TYPE st", typSt.Val(), "set")
	eq(t, "TYPE z", typZ.Val(), "zset")
	eq(t, "LRANGE", strings.Join(lrange.Val(), ","), "a,b,c")
	eq(t, "HGETALL", fmt.Sprint(hgetall.Val()), "map[f:1]")
	eq(t, "SCARD", scard.Val(), int64(2))
	eq(t, "ZRANGE", fmt.Sprint(zr.Val()), "[{1 a} {2 b}]")
	if bad.Err() == nil || !strings.HasPrefix(bad.Err().Error(), "WRONGTYPE ") {
		t.Fatalf("GET on a list: %v", bad.Err())
	}
	eq(t, "command after the error", after.Val(), int64(3))

	// A long pipeline across types keeps reply order.
	const n = 1000
	incrs := make([]*redis.IntCmd, 0, n)
	_, err = c.Pipelined(ctx, func(p redis.Pipeliner) error {
		for i := 0; i < n; i++ {
			p.RPush(ctx, "big:l", i)
			p.ZAdd(ctx, "big:z", redis.Z{Score: float64(i), Member: i})
			p.HSet(ctx, "big:h", i, i)
			incrs = append(incrs, p.Incr(ctx, "big:ctr"))
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	for i, cmd := range incrs {
		if cmd.Val() != int64(i+1) {
			t.Fatalf("INCR reply %d = %d, want %d: replies out of order", i, cmd.Val(), i+1)
		}
	}
	eq(t, "LLEN", c.LLen(ctx, "big:l").Val(), int64(n))
	eq(t, "ZCARD", c.ZCard(ctx, "big:z").Val(), int64(n))
	eq(t, "HLEN", c.HLen(ctx, "big:h").Val(), int64(n))
	eq(t, "ZRANK", c.ZRank(ctx, "big:z", "999").Val(), int64(999))
}
