package integration

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// parallel runs fn(0..n-1) on n goroutines and waits for all of them.
func parallel(n int, fn func(g int)) {
	var wg sync.WaitGroup
	for g := 0; g < n; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(g)
		}()
	}
	wg.Wait()
}

// TestConcurrentClientsOnOneKey hammers a single list, hash, set and sorted set
// from many goroutines (one shard lock each) and checks that no update is lost
// or duplicated. Run it with -race.
func TestConcurrentClientsOnOneKey(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()
	const goroutines, perG = 8, 200
	const total = goroutines * perG

	// drained checks that every value pushed was popped exactly once.
	drained := func(t *testing.T, popped map[string]int) {
		t.Helper()
		if len(popped) != total {
			t.Errorf("popped %d distinct values, want %d", len(popped), total)
		}
		for v, n := range popped {
			if n != 1 {
				t.Errorf("value %s popped %d times", v, n)
			}
		}
	}

	t.Run("list", func(t *testing.T) {
		parallel(goroutines, func(g int) {
			for i := 0; i < perG; i++ {
				v := fmt.Sprintf("%d-%d", g, i)
				var err error
				if g%2 == 0 {
					err = c.RPush(ctx, "list", v).Err()
				} else {
					err = c.LPush(ctx, "list", v).Err()
				}
				if err != nil {
					t.Errorf("push: %v", err)
					return
				}
			}
		})
		eq(t, "LLEN", c.LLen(ctx, "list").Val(), int64(total))
		var mu sync.Mutex
		popped := map[string]int{}
		parallel(goroutines, func(g int) {
			for {
				var v string
				var err error
				if g%2 == 0 {
					v, err = c.LPop(ctx, "list").Result()
				} else {
					v, err = c.RPop(ctx, "list").Result()
				}
				if errors.Is(err, redis.Nil) {
					return
				}
				if err != nil {
					t.Errorf("pop: %v", err)
					return
				}
				mu.Lock()
				popped[v]++
				mu.Unlock()
			}
		})
		drained(t, popped)
		eq(t, "list deleted when drained", c.Exists(ctx, "list").Val(), int64(0))
	})

	t.Run("hash", func(t *testing.T) {
		parallel(goroutines, func(g int) {
			for i := 0; i < perG; i++ {
				if err := c.HIncrBy(ctx, "hash", "counter", 1).Err(); err != nil {
					t.Errorf("HINCRBY: %v", err)
					return
				}
				if err := c.HSet(ctx, "hash", fmt.Sprintf("f-%d-%d", g, i), "v").Err(); err != nil {
					t.Errorf("HSET: %v", err)
					return
				}
			}
		})
		eq(t, "counter", c.HGet(ctx, "hash", "counter").Val(), fmt.Sprint(total))
		eq(t, "HLEN", c.HLen(ctx, "hash").Val(), int64(total+1))
		parallel(goroutines, func(g int) {
			for i := 0; i < perG; i++ {
				if err := c.HDel(ctx, "hash", fmt.Sprintf("f-%d-%d", g, i)).Err(); err != nil {
					t.Errorf("HDEL: %v", err)
					return
				}
			}
		})
		eq(t, "only the counter left", c.HLen(ctx, "hash").Val(), int64(1))
		c.HDel(ctx, "hash", "counter")
		eq(t, "hash deleted when drained", c.Exists(ctx, "hash").Val(), int64(0))
	})

	t.Run("set", func(t *testing.T) {
		parallel(goroutines, func(g int) {
			for i := 0; i < perG; i++ {
				if err := c.SAdd(ctx, "set", fmt.Sprintf("%d-%d", g, i), fmt.Sprintf("shared-%d", i%50)).Err(); err != nil {
					t.Errorf("SADD: %v", err)
					return
				}
			}
		})
		eq(t, "SCARD", c.SCard(ctx, "set").Val(), int64(total+50))
		var mu sync.Mutex
		popped := map[string]int{}
		parallel(goroutines, func(g int) {
			for {
				vs, err := c.SPopN(ctx, "set", 7).Result()
				if err != nil {
					t.Errorf("SPOP: %v", err)
					return
				}
				if len(vs) == 0 {
					return
				}
				mu.Lock()
				for _, v := range vs {
					popped[v]++
				}
				mu.Unlock()
			}
		})
		if len(popped) != total+50 {
			t.Errorf("popped %d distinct members, want %d", len(popped), total+50)
		}
		for v, n := range popped {
			if n != 1 {
				t.Errorf("member %s popped %d times", v, n)
			}
		}
		eq(t, "set deleted when drained", c.Exists(ctx, "set").Val(), int64(0))
	})

	t.Run("zset", func(t *testing.T) {
		parallel(goroutines, func(g int) {
			for i := 0; i < perG; i++ {
				z := redis.Z{Score: float64(g*perG + i), Member: fmt.Sprintf("m-%d", g*perG+i)}
				if err := c.ZAdd(ctx, "z", z).Err(); err != nil {
					t.Errorf("ZADD: %v", err)
					return
				}
				if err := c.ZIncrBy(ctx, "zcount", 1, "shared").Err(); err != nil {
					t.Errorf("ZINCRBY: %v", err)
					return
				}
			}
		})
		eq(t, "ZCARD", c.ZCard(ctx, "z").Val(), int64(total))
		eq(t, "shared ZINCRBY", c.ZScore(ctx, "zcount", "shared").Val(), float64(total))
		for i := 0; i < total; i += 37 { // spot-check that ranks follow the unique scores
			if r := c.ZRank(ctx, "z", fmt.Sprintf("m-%d", i)).Val(); r != int64(i) {
				t.Fatalf("ZRANK m-%d = %d", i, r)
			}
		}
		var mu sync.Mutex
		popped := map[string]int{}
		parallel(goroutines, func(g int) {
			for {
				zs, err := c.ZPopMin(ctx, "z", 5).Result()
				if err != nil {
					t.Errorf("ZPOPMIN: %v", err)
					return
				}
				if len(zs) == 0 {
					return
				}
				mu.Lock()
				for _, z := range zs {
					popped[z.Member.(string)]++
				}
				mu.Unlock()
			}
		})
		drained(t, popped)
		eq(t, "zset deleted when drained", c.Exists(ctx, "z").Val(), int64(0))
	})
}
