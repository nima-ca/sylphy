package main

import (
	"bufio"
	"context"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/config"
)

const guard = 30 * time.Second // bounds waits that only a bug could make long

func quietLogger() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testConfig(t *testing.T, mode string) config.Config {
	t.Helper()
	cfg := config.Default()
	cfg.Addr = "127.0.0.1:0"
	cfg.PersistenceMode = mode
	cfg.Dir = filepath.Join(t.TempDir(), "data")
	cfg.AppendFsync = "always"
	cfg.ShutdownGracePeriod = 5 * time.Second
	return cfg
}

type runningServer struct {
	addr string
	stop func() int
}

// start runs serve and returns once it is listening.
func start(t *testing.T, cfg config.Config) *runningServer {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	ready := make(chan net.Addr, 1)
	codes := make(chan int, 1)
	go func() { codes <- serve(ctx, context.Background(), cfg, quietLogger(), func(a net.Addr) { ready <- a }) }()
	var addr net.Addr
	select {
	case addr = <-ready:
	case code := <-codes:
		cancel()
		t.Fatalf("serve exited with code %d before listening", code)
	case <-time.After(guard):
		cancel()
		t.Fatal("serve did not start listening")
	}
	stopped := false
	rs := &runningServer{addr: addr.String()}
	rs.stop = func() int {
		if stopped {
			return 0
		}
		stopped = true
		cancel()
		select {
		case code := <-codes:
			return code
		case <-time.After(guard):
			t.Error("serve did not return after cancellation")
			return -1
		}
	}
	t.Cleanup(func() { rs.stop() })
	return rs
}

type client struct {
	c  net.Conn
	br *bufio.Reader
}

func dial(t *testing.T, addr string) *client {
	t.Helper()
	c, err := net.DialTimeout("tcp", addr, guard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return &client{c: c, br: bufio.NewReader(c)}
}

func (c *client) readReply(t *testing.T) string {
	t.Helper()
	_ = c.c.SetReadDeadline(time.Now().Add(guard))
	line, err := c.br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading a reply: %v", err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	switch line[0] {
	case '$':
		n, _ := strconv.Atoi(line[1:])
		if n < 0 {
			return "(nil)"
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(c.br, buf); err != nil {
			t.Fatal(err)
		}
		return string(buf[:n])
	case '*':
		n, _ := strconv.Atoi(line[1:])
		parts := make([]string, 0, n)
		for range n {
			parts = append(parts, c.readReply(t))
		}
		return strings.Join(parts, "|")
	}
	return line
}

func (c *client) do(t *testing.T, args ...string) string {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("*" + strconv.Itoa(len(args)) + "\r\n")
	for _, a := range args {
		sb.WriteString("$" + strconv.Itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	_ = c.c.SetWriteDeadline(time.Now().Add(guard))
	if _, err := c.c.Write([]byte(sb.String())); err != nil {
		t.Fatal(err)
	}
	return c.readReply(t)
}

func TestServeAOFSurvivesARestart(t *testing.T) {
	cfg := testConfig(t, "aof")
	cfg.SnapshotOnShutdown = "false" // the restart must replay the log, not a final snapshot

	s1 := start(t, cfg)
	c := dial(t, s1.addr)
	for _, cmd := range [][]string{
		{"SET", "k", "v"}, {"RPUSH", "l", "a", "b"}, {"SET", "t", "x", "EX", "100"},
	} {
		if r := c.do(t, cmd...); strings.HasPrefix(r, "-") {
			t.Fatalf("%v: %s", cmd, r)
		}
	}
	if r := c.do(t, "SAVE"); r != "+OK" {
		t.Fatalf("SAVE = %q", r)
	}
	if r := c.do(t, "LASTSAVE"); !strings.HasPrefix(r, ":") || r == ":0" {
		t.Fatalf("LASTSAVE = %q", r)
	}
	for _, cmd := range [][]string{{"SET", "after", "save"}, {"INCR", "n"}} { // only in the log
		if r := c.do(t, cmd...); strings.HasPrefix(r, "-") {
			t.Fatalf("%v: %s", cmd, r)
		}
	}
	_ = c.c.Close()
	if code := s1.stop(); code != 0 {
		t.Fatalf("first run exited with %d", code)
	}

	s2 := start(t, cfg)
	c2 := dial(t, s2.addr)
	for _, tc := range []struct {
		args []string
		want string
	}{
		{[]string{"GET", "k"}, "v"},
		{[]string{"LRANGE", "l", "0", "-1"}, "a|b"},
		{[]string{"GET", "after"}, "save"},
		{[]string{"GET", "n"}, "1"},
	} {
		if got := c2.do(t, tc.args...); got != tc.want {
			t.Errorf("after restart %v = %q, want %q", tc.args, got, tc.want)
		}
	}
	ttl, err := strconv.Atoi(strings.TrimPrefix(c2.do(t, "TTL", "t"), ":"))
	if err != nil || ttl < 1 || ttl > 100 {
		t.Errorf("TTL after restart = %d (%v)", ttl, err)
	}
	_ = c2.c.Close()
	if code := s2.stop(); code != 0 {
		t.Fatalf("second run exited with %d", code)
	}
}

func TestServeSnapshotMode(t *testing.T) {
	t.Run("a clean shutdown saves", func(t *testing.T) {
		cfg := testConfig(t, "snapshot") // snapshot-on-shutdown is auto: on
		s1 := start(t, cfg)
		c := dial(t, s1.addr)
		c.do(t, "SET", "a", "1")
		if r := c.do(t, "BGREWRITEAOF"); !strings.HasPrefix(r, "-ERR the append-only file is not enabled") {
			t.Fatalf("BGREWRITEAOF = %q", r)
		}
		_ = c.c.Close()
		if code := s1.stop(); code != 0 {
			t.Fatalf("exit %d", code)
		}
		s2 := start(t, cfg)
		if got := dial(t, s2.addr).do(t, "GET", "a"); got != "1" {
			t.Fatalf("after restart GET a = %q", got)
		}
	})
	t.Run("without the final checkpoint only SAVE persists", func(t *testing.T) {
		cfg := testConfig(t, "snapshot")
		cfg.SnapshotOnShutdown = "false"
		s1 := start(t, cfg)
		c := dial(t, s1.addr)
		c.do(t, "SET", "saved", "1")
		if r := c.do(t, "BGSAVE"); r != "+Background saving started" {
			t.Fatalf("BGSAVE = %q", r)
		}
		// SAVE waits for any background save to finish first, then runs.
		for {
			r := c.do(t, "SAVE")
			if r == "+OK" {
				break
			}
			if !strings.Contains(r, "already in progress") {
				t.Fatalf("SAVE = %q", r)
			}
		}
		c.do(t, "SET", "unsaved", "2")
		_ = c.c.Close()
		if code := s1.stop(); code != 0 {
			t.Fatalf("exit %d", code)
		}
		c2 := dial(t, start(t, cfg).addr)
		if got := c2.do(t, "GET", "saved"); got != "1" {
			t.Fatalf("GET saved = %q", got)
		}
		if got := c2.do(t, "GET", "unsaved"); got != "(nil)" {
			t.Fatalf("GET unsaved = %q: a write after the last save survived without a log", got)
		}
	})
}

func TestServeLeavesTheDiskAloneByDefault(t *testing.T) {
	cfg := testConfig(t, "off")
	s := start(t, cfg)
	c := dial(t, s.addr)
	if r := c.do(t, "SET", "k", "v"); r != "+OK" {
		t.Fatalf("SET = %q", r)
	}
	for _, name := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF"} {
		if r := c.do(t, name); !strings.HasPrefix(r, "-ERR persistence is disabled") {
			t.Errorf("%s = %q", name, r)
		}
	}
	if r := c.do(t, "LASTSAVE"); r != ":0" {
		t.Fatalf("LASTSAVE = %q", r)
	}
	_ = c.c.Close()
	if code := s.stop(); code != 0 {
		t.Fatalf("exit %d", code)
	}
	if _, err := os.Stat(cfg.Dir); !os.IsNotExist(err) {
		t.Fatalf("persistence is off but the data directory exists: %v", err)
	}
}

func TestServeRefusesToStartOnCorruptData(t *testing.T) {
	for _, mode := range []string{"aof", "snapshot"} {
		cfg := testConfig(t, mode)
		if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
			t.Fatal(err)
		}
		manifest := filepath.Join(cfg.Dir, "MANIFEST")
		if err := os.WriteFile(manifest, []byte("garbage\n"), 0o644); err != nil {
			t.Fatal(err)
		}
		code := serve(context.Background(), context.Background(), cfg, quietLogger(), func(net.Addr) {
			t.Error("the listener opened although recovery failed")
		})
		if code != 1 {
			t.Fatalf("%s: exit code %d, want 1", mode, code)
		}
		if got, _ := os.ReadFile(manifest); string(got) != "garbage\n" {
			t.Fatalf("%s: a failed recovery modified the data directory", mode)
		}
		if ents, _ := os.ReadDir(cfg.Dir); len(ents) != 1 {
			t.Fatalf("%s: a failed recovery changed the directory: %v", mode, ents)
		}
	}
}

func TestRunRejectsInvalidPersistenceSettings(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"mode", []string{"-persistence-mode", "disk"}, nil, "persistence-mode"},
		{"fsync", []string{"-append-fsync", "sometimes"}, nil, "append-fsync"},
		{"size", []string{"-auto-rewrite-min-size", "lots"}, nil, "auto-rewrite-min-size"},
		{"snapshot on shutdown", []string{"-snapshot-on-shutdown", "maybe"}, nil, "snapshot-on-shutdown"},
		{"env bool", nil, map[string]string{"SYLPHY_AOF_LOAD_TRUNCATED": "perhaps"}, "SYLPHY_AOF_LOAD_TRUNCATED"},
		{"empty dir", []string{"-persistence-mode", "aof", "-dir", ""}, nil, "dir must not be empty"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr strings.Builder
			getenv := func(k string) string { return tt.env[k] }
			if code := run(tt.args, getenv, io.Discard, &stderr); code != 2 {
				t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("stderr %q does not mention %q", stderr.String(), tt.want)
			}
		})
	}
}

func TestHelpMentionsPersistenceFlags(t *testing.T) {
	var out strings.Builder
	if code := run([]string{"-h"}, func(string) string { return "" }, io.Discard, &out); code != 0 {
		t.Fatalf("code=%d", code)
	}
	for _, f := range []string{"-persistence-mode", "-dir", "-append-fsync"} {
		if !strings.Contains(out.String(), f) {
			t.Errorf("help does not mention %s", f)
		}
	}
}
