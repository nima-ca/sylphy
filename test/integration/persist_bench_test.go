package integration

import (
	"context"
	"log/slog"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/persist"
	"github.com/nima-ca/sylphy/internal/server"
	"github.com/nima-ca/sylphy/internal/store"
)

const (
	benchClients     = 16
	benchValueLen    = 64
	benchSetKeyspace = 10_000
	latencyKeys      = 200_000
	latencyValueLen  = 128
)

func benchKey(i int) string { return "bench:" + strconv.Itoa(i) }

// startBenchServer wires an in-process server the way cmd/sylphy-server's serve
// does: store, registry, server, then (persistence on) manager, Recover, Start
// and SetPersistence, then the sweeper and the listener. Cleanup stops them in
// serve's order: listener and clients, sweeper, persistence last.
//
// The automatic checkpoint trigger and the final checkpoint are off, so the
// only checkpoints a benchmark sees are the ones it asks for. mode is off, snapshot
// or aof; fsync is the append-fsync policy.
func startBenchServer(b *testing.B, mode, fsync string) string {
	b.Helper()
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.ShutdownGracePeriod = 5 * time.Second
	cfg.PersistenceMode = mode
	cfg.Dir = b.TempDir()
	cfg.AppendFsync = fsync
	cfg.AutoRewritePercentage = 0
	cfg.SnapshotOnShutdown = "false"
	if err := cfg.Validate(); err != nil {
		b.Fatal(err)
	}

	st, err := store.NewWithOptions(store.Options{
		Shards: cfg.Shards,
		Sweep: store.SweepConfig{
			TickInterval:   cfg.ExpireTickInterval,
			SampleSize:     cfg.ExpireSampleSize,
			CycleBudget:    cfg.ExpireCycleBudget,
			StaleThreshold: cfg.ExpireStaleThreshold,
		},
	})
	if err != nil {
		b.Fatal(err)
	}
	reg, err := command.NewDefaultRegistry()
	if err != nil {
		b.Fatal(err)
	}
	logger := slog.New(slog.DiscardHandler)
	srv := server.New(cfg, st, reg, logger)

	var mgr *persist.Manager
	if cfg.PersistenceEnabled() {
		policy, err := persist.ParsePolicy(cfg.AppendFsync)
		if err != nil {
			b.Fatal(err)
		}
		pmode := persist.ModeSnapshot
		if cfg.PersistenceMode == "aof" {
			pmode = persist.ModeAOF
		}
		mgr, err = persist.NewManager(persist.ManagerOptions{
			FS:                    persist.OSFS(),
			Dir:                   cfg.Dir,
			Store:                 st,
			Registry:              reg,
			Config:                &cfg,
			Mode:                  pmode,
			Policy:                policy,
			AutoRewritePercentage: cfg.AutoRewritePercentage,
			AutoRewriteMinSize:    cfg.AutoRewriteMinSize,
			LoadTruncated:         cfg.AOFLoadTruncated,
			SnapshotOnShutdown:    cfg.SnapshotOnShutdownEnabled(),
			Logger:                logger,
		})
		if err != nil {
			b.Fatal(err)
		}
		ctx := context.Background()
		if _, err := mgr.Recover(ctx); err != nil {
			b.Fatal(err)
		}
		if err := mgr.Start(ctx); err != nil {
			b.Fatal(err)
		}
		srv.SetPersistence(persist.NewCommands(mgr), mgr.Propagator(),
			cfg.PersistenceMode == "aof" && cfg.AppendFsync == "always")
	}
	st.Start()

	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		b.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Serve(ctx, ln) }()
	b.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(15 * time.Second):
			b.Error("server did not stop in time")
		}
		st.Close()
		if mgr != nil {
			if err := mgr.Shutdown(context.Background()); err != nil {
				b.Errorf("persistence shutdown: %v", err)
			}
		}
	})
	return ln.Addr().String()
}

// newBenchClients opens n clients, one connection each, and connects them, so
// dialing is not part of the timed section.
func newBenchClients(b *testing.B, addr string, n int) []*redis.Client {
	b.Helper()
	cs := make([]*redis.Client, n)
	for i := range cs {
		cs[i] = redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, MaxRetries: -1, PoolSize: 1})
		if err := cs[i].Ping(context.Background()).Err(); err != nil {
			b.Fatal(err)
		}
	}
	b.Cleanup(func() {
		for _, c := range cs {
			_ = c.Close()
		}
	})
	return cs
}

type firstError struct {
	mu  sync.Mutex
	err error
}

func (f *firstError) set(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err == nil {
		f.err = err
	}
}

func (f *firstError) get() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.err
}

// driveClients runs op b.N times in total, spread over one goroutine per client.
func driveClients(b *testing.B, cs []*redis.Client, op func(ci int, c *redis.Client, i int) error) {
	b.Helper()
	var next atomic.Int64
	var fe firstError
	var wg sync.WaitGroup
	for ci, c := range cs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				i := int(next.Add(1)) - 1
				if i >= b.N {
					return
				}
				if err := op(ci, c, i); err != nil {
					fe.set(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	if err := fe.get(); err != nil {
		b.Fatal(err)
	}
}

// BenchmarkPersistSetThroughput measures SET throughput from several parallel
// clients with persistence off and with the AOF under each fsync policy. It is
// skipped under -short and only runs with -bench.
func BenchmarkPersistSetThroughput(b *testing.B) {
	if testing.Short() {
		b.Skip("persistence benchmarks are skipped under -short")
	}
	val := strings.Repeat("v", benchValueLen)
	for _, m := range []struct{ name, mode, fsync string }{
		{"off", "off", "everysec"},
		{"aof-no", "aof", "no"},
		{"aof-everysec", "aof", "everysec"},
		{"aof-always", "aof", "always"},
	} {
		b.Run(m.name, func(b *testing.B) {
			addr := startBenchServer(b, m.mode, m.fsync)
			cs := newBenchClients(b, addr, benchClients)
			ctx := context.Background()
			b.ResetTimer()
			driveClients(b, cs, func(_ int, c *redis.Client, i int) error {
				return c.Set(ctx, benchKey(i%benchSetKeyspace), val, 0).Err()
			})
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "ops/s")
		})
	}
}

// benchPreload stores latencyKeys keys of valLen bytes.
func benchPreload(b *testing.B, addr string, n, valLen int) {
	b.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, MaxRetries: -1, PoolSize: 1})
	defer func() { _ = c.Close() }()
	ctx := context.Background()
	val := strings.Repeat("p", valLen)
	for lo := 0; lo < n; lo += 1000 {
		pipe := c.Pipeline()
		for i := lo; i < min(lo+1000, n); i++ {
			pipe.Set(ctx, benchKey(i), val, 0)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPersistLatencyDuringBGSave reports p50 and p99 latency of a 50/50
// GET/SET mix from concurrent clients on a preloaded keyspace, idle and while
// checkpoints run back to back (a goroutine reissues BGSAVE whenever the
// previous one has finished). The difference shows what the shard-granular
// copy-on-write gate and the checkpoint cost the foreground clients. The
// "bgsaves" metric is the number of checkpoints that were started during the
// timed section. Skipped under -short.
func BenchmarkPersistLatencyDuringBGSave(b *testing.B) {
	if testing.Short() {
		b.Skip("persistence benchmarks are skipped under -short")
	}
	for _, withSave := range []bool{false, true} {
		name := "idle"
		if withSave {
			name = "during-bgsave"
		}
		b.Run(name, func(b *testing.B) {
			addr := startBenchServer(b, "aof", "everysec")
			benchPreload(b, addr, latencyKeys, latencyValueLen)
			cs := newBenchClients(b, addr, benchClients)
			ctx := context.Background()
			val := strings.Repeat("w", latencyValueLen)

			var saves atomic.Int64
			var saverErr firstError
			saverCtx, stopSaver := context.WithCancel(ctx)
			saverDone := make(chan struct{})
			go func() {
				defer close(saverDone)
				if !withSave {
					return
				}
				sc := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, MaxRetries: -1, PoolSize: 1})
				defer func() { _ = sc.Close() }()
				for saverCtx.Err() == nil {
					err := sc.Do(saverCtx, "BGSAVE").Err()
					switch {
					case err == nil:
						saves.Add(1)
					case strings.Contains(err.Error(), "already in progress"):
					case saverCtx.Err() != nil:
						return
					default:
						saverErr.set(err)
						return
					}
					select {
					case <-saverCtx.Done():
					case <-time.After(time.Millisecond):
					}
				}
			}()

			lat := make([][]time.Duration, benchClients)
			for i := range lat {
				lat[i] = make([]time.Duration, 0, b.N/benchClients+1)
			}
			b.ResetTimer()
			driveClients(b, cs, func(ci int, c *redis.Client, i int) error {
				key := benchKey((i * 7919) % latencyKeys) // spread over every shard
				start := time.Now()
				var err error
				if i%2 == 0 {
					err = c.Get(ctx, key).Err()
				} else {
					err = c.Set(ctx, key, val, 0).Err()
				}
				lat[ci] = append(lat[ci], time.Since(start))
				return err
			})
			b.StopTimer()
			stopSaver()
			<-saverDone
			if err := saverErr.get(); err != nil {
				b.Fatalf("BGSAVE: %v", err)
			}

			all := make([]time.Duration, 0, b.N)
			for _, l := range lat {
				all = append(all, l...)
			}
			if len(all) == 0 {
				return
			}
			slices.Sort(all)
			b.ReportMetric(float64(all[len(all)/2])/1e3, "p50-us")
			b.ReportMetric(float64(all[min(len(all)-1, len(all)*99/100)])/1e3, "p99-us")
			b.ReportMetric(float64(saves.Load()), "bgsaves")
		})
	}
}
