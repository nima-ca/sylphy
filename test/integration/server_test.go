package integration

import (
	"bufio"
	"context"
	"errors"
	"io"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/config"
)

func assertAlive(t *testing.T, addr string) {
	t.Helper()
	c := newClient(t, addr)
	if got := c.Ping(context.Background()).Val(); got != "PONG" {
		t.Fatalf("server not healthy: PING = %q", got)
	}
}

func TestMalformedInputGetsErrorAndServerSurvives(t *testing.T) {
	ts := startServer(t, nil)
	cases := []string{
		"*1\r\n$abc\r\n",
		"*1\r\n:5\r\n",
		"*1\r\n$-5\r\n",
		"*2000000000\r\n",
		"*1\r\n$999999999999\r\n",
	}
	for _, in := range cases {
		conn := rawDial(t, ts.addr)
		_, _ = io.WriteString(conn, in)
		br := bufio.NewReader(conn)
		line, err := br.ReadString('\n')
		if err != nil || !strings.HasPrefix(line, "-ERR Protocol error") {
			t.Errorf("%q: reply %q err %v", in, line, err)
		}
		if _, err := br.ReadByte(); err == nil {
			t.Errorf("%q: connection should be closed after a protocol error", in)
		}
	}

	// Random garbage must not hurt either.
	garbage := make([]byte, 64<<10)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range garbage {
		garbage[i] = byte(rng.UintN(256))
	}
	conn := rawDial(t, ts.addr)
	_, _ = conn.Write(garbage)
	_ = conn.Close()

	assertAlive(t, ts.addr)
}

func TestRawPipeliningKeepsOrder(t *testing.T) {
	ts := startServer(t, nil)
	conn := rawDial(t, ts.addr)
	_, _ = io.WriteString(conn, "PING\r\nPING hello\r\n*2\r\n$4\r\nECHO\r\n$1\r\nx\r\n")
	br := bufio.NewReader(conn)
	for _, want := range []string{"+PONG\r\n", "$5\r\nhello\r\n", "$1\r\nx\r\n"} {
		buf := make([]byte, len(want))
		if _, err := io.ReadFull(br, buf); err != nil || string(buf) != want {
			t.Fatalf("got %q err %v, want %q", buf, err, want)
		}
	}
}

func TestMaxClients(t *testing.T) {
	ts := startServer(t, func(c *config.Config) { c.MaxClients = 1 })
	first := rawDial(t, ts.addr)
	_, _ = io.WriteString(first, "PING\r\n")
	br := bufio.NewReader(first)
	if line, err := br.ReadString('\n'); err != nil || line != "+PONG\r\n" {
		t.Fatalf("first client: %q %v", line, err)
	}

	second := rawDial(t, ts.addr)
	line, err := bufio.NewReader(second).ReadString('\n')
	if err != nil || line != "-ERR max number of clients reached\r\n" {
		t.Fatalf("second client: %q %v", line, err)
	}
}

func TestIdleTimeout(t *testing.T) {
	ts := startServer(t, func(c *config.Config) { c.IdleTimeout = 150 * time.Millisecond })
	conn := rawDial(t, ts.addr)
	time.Sleep(400 * time.Millisecond)
	if _, err := bufio.NewReader(conn).ReadByte(); !errors.Is(err, io.EOF) {
		t.Fatalf("expected the idle connection to be closed, got %v", err)
	}
}

func TestQuitClosesConnection(t *testing.T) {
	ts := startServer(t, nil)
	conn := rawDial(t, ts.addr)
	_, _ = io.WriteString(conn, "QUIT\r\n")
	br := bufio.NewReader(conn)
	if line, err := br.ReadString('\n'); err != nil || line != "+OK\r\n" {
		t.Fatalf("QUIT reply %q %v", line, err)
	}
	if _, err := br.ReadByte(); err == nil {
		t.Fatal("connection should be closed after QUIT")
	}
}

func TestGracefulShutdownWithActiveClients(t *testing.T) {
	ts := startServer(t, func(c *config.Config) { c.ShutdownGracePeriod = 2 * time.Second })

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		c := newClient(t, ts.addr)
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c.Incr(context.Background(), "n").Err() == nil {
			}
		}()
	}
	// A client stuck halfway through a request must not block shutdown.
	stuck := rawDial(t, ts.addr)
	_, _ = io.WriteString(stuck, "*1\r\n$4\r\nPI")
	time.Sleep(150 * time.Millisecond)

	start := time.Now()
	if err := ts.stop(); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("shutdown took %v, longer than the grace period", d)
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("clients still running after shutdown")
	}
	if n := ts.srv.NumClients(); n != 0 {
		t.Fatalf("%d clients still registered", n)
	}
}
