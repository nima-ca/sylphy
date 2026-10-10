// Package integration runs end-to-end tests against a real Sylphy server using
// go-redis and raw sockets.
package integration

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/server"
	"github.com/nima-ca/sylphy/internal/store"
)

type testServer struct {
	addr   string
	srv    *server.Server
	st     *store.Store
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
	err    error
}

// stop cancels the server context (graceful shutdown) and waits for Serve.
func (ts *testServer) stop() error {
	ts.once.Do(func() {
		ts.cancel()
		select {
		case ts.err = <-ts.done:
		case <-time.After(10 * time.Second):
			ts.err = errors.New("server did not stop in time")
		}
	})
	return ts.err
}

// startServer boots a server on 127.0.0.1:0 with the system clock. Its cleanup
// stops the server and the store's sweeper and verifies no goroutines leaked.
// Cleanups run LIFO, so clients created after this call are closed first.
func startServer(t *testing.T, mutate func(*config.Config)) *testServer {
	t.Helper()
	return startServerWithClock(t, nil, mutate)
}

// startServerWithClock is startServer with an injected store clock (nil means
// the system clock). With a store.FakeClock, tests move time deterministically
// while the sweeper, which is driven by a real ticker, still runs.
func startServerWithClock(t *testing.T, clk store.Clock, mutate func(*config.Config)) *testServer {
	t.Helper()
	before := runtime.NumGoroutine()

	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.ShutdownGracePeriod = 2 * time.Second
	if mutate != nil {
		mutate(&cfg)
	}
	st, err := store.NewWithOptions(store.Options{Shards: cfg.Shards, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	st.Start() // as cmd/sylphy-server does; Close below stops it
	reg, err := command.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	srv := server.New(cfg, st, reg, slog.New(slog.DiscardHandler))
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	ts := &testServer{addr: ln.Addr().String(), srv: srv, st: st, cancel: cancel, done: make(chan error, 1)}
	go func() { ts.done <- srv.Serve(ctx, ln) }()

	t.Cleanup(func() {
		if err := ts.stop(); err != nil && !errors.Is(err, server.ErrServerClosed) {
			t.Errorf("server stop: %v", err)
		}
		st.Close()
		waitGoroutines(t, before)
	})
	return ts
}

func waitGoroutines(t *testing.T, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > want {
		if time.Now().After(deadline) {
			buf := make([]byte, 1<<16)
			n := runtime.Stack(buf, true)
			t.Errorf("goroutine leak: have %d, want <= %d\n%s", runtime.NumGoroutine(), want, buf[:n])
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func newClient(t *testing.T, addr string) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: addr, Protocol: 2, MaxRetries: -1})
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func rawDial(t *testing.T, addr string) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	_ = c.SetDeadline(time.Now().Add(5 * time.Second))
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func eq[T comparable](t *testing.T, what string, got, want T) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}
