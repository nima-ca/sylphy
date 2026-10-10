package integration

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/redis/go-redis/v9"
)

// scanAll walks SCAN to completion and returns how often each key was reported.
func scanAll(t *testing.T, c *redis.Client, match string, count int64, keyType string) (seen map[string]int, calls int) {
	t.Helper()
	ctx := context.Background()
	seen = make(map[string]int)
	var cursor uint64
	for {
		var cmd *redis.ScanCmd
		if keyType == "" {
			cmd = c.Scan(ctx, cursor, match, count)
		} else {
			cmd = c.ScanType(ctx, cursor, match, count, keyType)
		}
		keys, next, err := cmd.Result()
		if err != nil {
			t.Fatalf("SCAN %d: %v", cursor, err)
		}
		for _, k := range keys {
			seen[k]++
		}
		calls++
		if next == 0 {
			return seen, calls
		}
		cursor = next
		if calls > 1_000_000 {
			t.Fatal("SCAN did not terminate")
		}
	}
}

func TestScanOver50kKeys(t *testing.T) {
	ts := startServer(t, nil)
	c := newClient(t, ts.addr)
	ctx := context.Background()

	const strs, lists = 50_000, 100
	for start := 0; start < strs; start += 1000 {
		_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i := start; i < start+1000; i++ {
				p.Set(ctx, fmt.Sprintf("key:%d", i), "v", 0)
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < lists; i++ {
		if err := c.RPush(ctx, fmt.Sprintf("list:%d", i), "x").Err(); err != nil {
			t.Fatal(err)
		}
	}
	total := strs + lists
	eq(t, "DBSIZE", c.DBSize(ctx).Val(), int64(total))

	seen, calls := scanAll(t, c, "", 1000, "")
	if len(seen) != total {
		t.Fatalf("SCAN reported %d distinct keys, want %d", len(seen), total)
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("key %s reported %d times without concurrent writers", k, n)
		}
	}
	// COUNT 1000 should take about total/1000 calls, plus one per shard boundary.
	if limit := total/1000 + 32 + 2; calls > limit {
		t.Errorf("%d SCAN calls for %d keys, want at most %d", calls, total, limit)
	}

	byType, _ := scanAll(t, c, "", 500, "list")
	eq(t, "TYPE list", len(byType), lists)
	byMatch, _ := scanAll(t, c, "list:*", 500, "")
	eq(t, "MATCH list:*", len(byMatch), lists)
	byBoth, _ := scanAll(t, c, "key:1*", 500, "string")
	for k := range byBoth {
		if k[:5] != "key:1" {
			t.Errorf("MATCH key:1* returned %s", k)
		}
	}
	if len(byBoth) == 0 {
		t.Error("MATCH key:1* TYPE string returned nothing")
	}
	if err := c.Scan(ctx, 1<<62, "", 10).Err(); err == nil || err.Error() != "ERR invalid cursor" {
		t.Fatalf("bogus cursor: %v", err)
	}
}

// TestScanDuringConcurrentWrites checks the SCAN guarantee over the wire: while
// other clients create and delete unrelated keys, every full scan still reports
// every stable key.
func TestScanDuringConcurrentWrites(t *testing.T) {
	ts := startServer(t, nil)
	ctx := context.Background()
	c := newClient(t, ts.addr)

	const stable, churn = 2000, 2000
	for start := 0; start < stable; start += 500 {
		_, err := c.Pipelined(ctx, func(p redis.Pipeliner) error {
			for i := start; i < start+500; i++ {
				p.Set(ctx, fmt.Sprintf("stable:%d", i), "v", 0)
				p.Set(ctx, fmt.Sprintf("churn:%d", i), "v", 0) // interleaved on purpose
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := 0; w < 2; w++ {
		wc := newClient(t, ts.addr)
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for i := w; ; i += 7 {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("churn:%d", i%(2*churn))
				if i%2 == 0 {
					_ = wc.Del(ctx, k).Err()
				} else {
					_ = wc.Set(ctx, k, "v", 0).Err()
				}
			}
		}(w)
	}
	for pass := 0; pass < 5; pass++ {
		seen, _ := scanAll(t, c, "", 100, "")
		for i := 0; i < stable; i++ {
			if k := fmt.Sprintf("stable:%d", i); seen[k] == 0 {
				t.Errorf("pass %d: stable key %s was missed", pass, k)
			}
		}
	}
	close(stop)
	wg.Wait()
}
