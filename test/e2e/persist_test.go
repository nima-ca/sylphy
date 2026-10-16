package e2e

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/nima-ca/sylphy/internal/command"
)

// These tests run the real sylphy-server binary and kill it the hard way, so
// they pin the guarantees persistence documents, not just the code paths.

// 1. kill -9 under fsync=always: every type, TTLs, expiry during downtime.
func TestE2EKillRecoversAllTypesWithTTLs(t *testing.T) {
	dir := t.TempDir()
	args := aofArgs(dir, "always")

	p := startProc(t, "", args...)
	d := writeDataset(t, e2eClient(t, p.addr), true)
	p.kill()
	// The short-TTL keys must expire while the server is down.
	waitUntil(d.shortAt.Add(150 * time.Millisecond))

	p2 := startProc(t, "", args...)
	d.verify(t, e2eClient(t, p2.addr))

	// The purge of the expired keys was logged and fsynced before the listener
	// opened, so a second crash must not bring them back or change anything.
	p2.kill()
	p3 := startProc(t, "", args...)
	d.verify(t, e2eClient(t, p3.addr))
}

// 2. Under always, a write the client saw acknowledged is never lost, wherever
// the kill lands. Unacknowledged writes may or may not survive.
func TestE2EAlwaysLosesNoAcknowledgedWrite(t *testing.T) {
	const rounds, writers = 4, 8
	seed := uint64(time.Now().UnixNano())
	t.Logf("kill-timing seed %d", seed)
	rng := rand.New(rand.NewPCG(seed, 1))

	dir := t.TempDir()
	args := aofArgs(dir, "always")
	acked := map[string]string{} // guarded by mu while writers run
	var mu sync.Mutex

	for round := 0; round <= rounds; round++ {
		p := startProc(t, "", args...)
		c := e2eClient(t, p.addr)
		// Everything acknowledged in earlier rounds survived the last kill.
		e2eVerifyAcked(t, c, acked)
		t.Logf("round %d: %d acknowledged writes verified", round, len(acked))
		if round == rounds {
			return
		}

		var count atomic.Int64
		var wg sync.WaitGroup
		for w := range writers {
			wg.Add(1)
			go func() {
				defer wg.Done()
				wc := newE2EClient(p.addr, 1)
				defer func() { _ = wc.Close() }()
				for i := 0; ; i++ {
					k := fmt.Sprintf("r%d-w%d-%d", round, w, i)
					v := k + e2eBin
					if err := wc.Set(context.Background(), k, v, 0).Err(); err != nil {
						return // the server was killed
					}
					mu.Lock()
					acked[k] = v
					mu.Unlock()
					count.Add(1)
				}
			}()
		}
		eventually(t, 20*time.Second, "some writes to be acknowledged", func() bool { return count.Load() >= 50 })
		time.Sleep(time.Duration(rng.IntN(150)) * time.Millisecond) // deliberate jitter: a different kill moment each round
		p.kill()

		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(30 * time.Second):
			t.Fatal("writers did not stop after the server was killed")
		}
	}
}

// 3. everysec: the write reaches the log immediately and is fsynced within
// about a second.
//
// Honest limit of this test: kill -9 kills the process, not the machine. Bytes
// the server already handed to the OS survive it whether or not they were
// fsynced, so this test cannot observe the documented everysec loss window
// (up to about one second of acknowledged writes lost on a power failure or OS
// crash). It checks the part a process kill can show: after the window has
// elapsed, the records are in the log and recovery restores them. It does not
// and cannot assert that data is lost.
func TestE2EEverysecWritesReachTheLogWithinTheWindow(t *testing.T) {
	const window = time.Second // the documented everysec fsync interval
	const n = 50
	dir := t.TempDir()
	args := aofArgs(dir, "everysec")

	p := startProc(t, "", args...)
	c := e2eClient(t, p.addr)
	for i := range n {
		if err := c.Set(context.Background(), fmt.Sprintf("key:%02d", i), fmt.Sprintf("val:%02d", i), 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	lastAck := time.Now()
	seg := e2eLastSegment(t, dir)
	eventually(t, window+6*time.Second, "every record in the log and the fsync window to elapse", func() bool {
		return time.Since(lastAck) > window+100*time.Millisecond && len(e2eSetKeys(t, seg)) == n
	})
	p.kill()

	p2 := startProc(t, "", args...)
	c2 := e2eClient(t, p2.addr)
	for i := range n {
		k := fmt.Sprintf("key:%02d", i)
		if v, err := c2.Get(context.Background(), k).Result(); err != nil || v != fmt.Sprintf("val:%02d", i) {
			t.Fatalf("%s: %q, %v", k, v, err)
		}
	}
}

// 4. BGSAVE with writes before, during and after it.
func TestE2EBGSaveThenMoreWrites(t *testing.T) {
	const bulk = 60_000 // enough keys that the checkpoint takes a while
	ctx := context.Background()
	dir := t.TempDir()
	args := aofArgs(dir, "always")
	val := strings.Repeat("a", 64)

	p := startProc(t, "", args...)
	c := e2eClient(t, p.addr)
	e2eBulkSet(t, c, "a:", bulk, val)
	for _, kv := range [][2]string{{"del-me", "v1"}, {"mod", "v1"}} {
		if err := c.Set(ctx, kv[0], kv[1], 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	if err := c.HSet(ctx, "h", "f", "1").Err(); err != nil {
		t.Fatal(err)
	}
	eq(t, "BGSAVE", wText(t, c, "BGSAVE"), "Background saving started")

	// Writers run while the checkpoint is in flight: whichever side of the
	// barrier a write falls on, an acknowledged one must survive.
	var mu sync.Mutex
	during := map[string]string{}
	stop := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			wc := newE2EClient(p.addr, 1)
			defer func() { _ = wc.Close() }()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				k := fmt.Sprintf("d:%d:%d", w, i)
				if err := wc.Set(ctx, k, k, 0).Err(); err != nil {
					return
				}
				mu.Lock()
				during[k] = k
				mu.Unlock()
			}
		}()
	}
	e2eWaitIdle(t, c)
	close(stop)
	wg.Wait()
	if len(during) == 0 {
		t.Fatal("no write was acknowledged while the checkpoint ran")
	}

	// After the snapshot: delete one key, modify another, add more.
	if err := c.Del(ctx, "del-me").Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.Set(ctx, "mod", "v2", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if err := c.HSet(ctx, "h", "f", "2", "g", "3").Err(); err != nil {
		t.Fatal(err)
	}
	e2eBulkSet(t, c, "b:", 1000, val)
	p.kill()

	p2 := startProc(t, "", args...)
	c2 := e2eClient(t, p2.addr)
	e2eBulkVerify(t, c2, "a:", bulk, val)
	e2eBulkVerify(t, c2, "b:", 1000, val)
	if n := c2.Exists(ctx, "del-me").Val(); n != 0 {
		t.Fatal("a key deleted after the snapshot came back")
	}
	eq(t, "mod", c2.Get(ctx, "mod").Val(), "v2")
	if h := c2.HGetAll(ctx, "h").Val(); !maps.Equal(h, map[string]string{"f": "2", "g": "3"}) {
		t.Fatalf("h = %v", h)
	}
	e2eVerifyAcked(t, c2, during)
}

// 5. Snapshot mode: only a checkpoint persists; SIGTERM takes one.
func TestE2ESnapshotMode(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	args := []string{"-persistence-mode", "snapshot", "-dir", dir}

	p := startProc(t, "", args...)
	c := e2eClient(t, p.addr)
	d := writeDataset(t, c, false)
	eq(t, "SAVE", wText(t, c, "SAVE"), "OK")
	// Everything below happens after the save and is not persisted by a crash.
	for _, err := range []error{
		c.Set(ctx, "after-save", "x", 0).Err(),
		c.Set(ctx, "plain", "changed", 0).Err(),
		c.Del(ctx, e2eBinKey).Err(),
	} {
		if err != nil {
			t.Fatal(err)
		}
	}
	p.kill()

	p2 := startProc(t, "", args...)
	c2 := e2eClient(t, p2.addr)
	d.verify(t, c2) // exactly the saved dataset: no after-save, plain unchanged, bin key back
	e2eAssertNoSegments(t, dir)

	// A graceful stop takes the final checkpoint, so nothing is lost.
	if err := c2.Set(ctx, "post-restart", "y", 0).Err(); err != nil {
		t.Fatal(err)
	}
	if code := p2.term(30 * time.Second); code != 0 {
		t.Fatalf("exit code %d after SIGTERM\n%s", code, p2.logs())
	}
	p3 := startProc(t, "", args...)
	d.extra = []string{"post-restart"}
	d.verify(t, e2eClient(t, p3.addr))
	eq(t, "post-restart", e2eClient(t, p3.addr).Get(ctx, "post-restart").Val(), "y")
}

// 6. Graceful shutdown in AOF mode, with everysec: the final checkpoint and
// flush make it lossless, and nothing but the manifest's files is left.
func TestE2EGracefulShutdownAOF(t *testing.T) {
	dir := t.TempDir()
	args := aofArgs(dir, "everysec")

	p := startProc(t, "", args...)
	d := writeDataset(t, e2eClient(t, p.addr), false)
	if code := p.term(30 * time.Second); code != 0 {
		t.Fatalf("exit code %d after SIGTERM\n%s", code, p.logs())
	}
	if !strings.Contains(p.stderr.String(), "checkpoint complete") {
		t.Errorf("no final checkpoint was logged\n%s", p.logs())
	}
	e2eAssertDirConsistent(t, dir)
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range ents {
		if strings.HasSuffix(e.Name(), ".tmp") {
			t.Errorf("temp file left behind: %s", e.Name())
		}
	}

	p2 := startProc(t, "", args...)
	d.verify(t, e2eClient(t, p2.addr))
}

const e2eTornKeys = 10

func e2eTornKey(i int) string { return fmt.Sprintf("key:%02d", i) }
func e2eTornVal(i int) string { return fmt.Sprintf("val:%02d", i) + e2eBin }

// e2ePrepareTornDir writes e2eTornKeys acknowledged SETs under fsync always,
// one after another, then kill -9s the server. The last segment then holds
// exactly one record per key, in order.
func e2ePrepareTornDir(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	p := startProc(t, "", aofArgs(dir, "always")...)
	c := e2eClient(t, p.addr)
	for i := range e2eTornKeys {
		if err := c.Set(context.Background(), e2eTornKey(i), e2eTornVal(i), 0).Err(); err != nil {
			t.Fatal(err)
		}
	}
	p.kill()
	return dir
}

// 7. Truncated and corrupt logs.
func TestE2ETruncatedAndCorruptLog(t *testing.T) {
	ctx := context.Background()

	t.Run("torn tail is repaired by default", func(t *testing.T) {
		dir := e2ePrepareTornDir(t)
		seg := e2eLastSegment(t, dir)
		fi, err := os.Stat(seg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(seg, fi.Size()-3); err != nil {
			t.Fatal(err)
		}

		args := aofArgs(dir, "always") // aof-load-truncated defaults to true
		p := startProc(t, "", args...)
		c := e2eClient(t, p.addr)
		// The dataset is the last consistent prefix: the torn record is dropped.
		for i := range e2eTornKeys - 1 {
			if v, err := c.Get(ctx, e2eTornKey(i)).Result(); err != nil || v != e2eTornVal(i) {
				t.Fatalf("%s: %q, %v", e2eTornKey(i), v, err)
			}
		}
		if err := c.Get(ctx, e2eTornKey(e2eTornKeys-1)).Err(); !errors.Is(err, redis.Nil) {
			t.Fatalf("the torn record's key should be absent, got %v", err)
		}
		if !strings.Contains(p.stderr.String(), "truncated a partial record") {
			t.Errorf("repair was not logged\n%s", p.logs())
		}
		if after, err := os.Stat(seg); err != nil || after.Size() >= fi.Size()-3 {
			t.Fatalf("segment was not cut back: %v, %v", after, err)
		}

		// Appending continues cleanly after the repaired offset.
		if err := c.Set(ctx, "after-repair", "ok", 0).Err(); err != nil {
			t.Fatal(err)
		}
		p.kill()
		p2 := startProc(t, "", args...)
		c2 := e2eClient(t, p2.addr)
		if v := c2.Get(ctx, "after-repair").Val(); v != "ok" {
			t.Fatalf("after-repair = %q", v)
		}
		for i := range e2eTornKeys - 1 {
			if v := c2.Get(ctx, e2eTornKey(i)).Val(); v != e2eTornVal(i) {
				t.Fatalf("%s = %q after the second restart", e2eTornKey(i), v)
			}
		}
	})

	t.Run("torn tail refuses to start with aof-load-truncated=false", func(t *testing.T) {
		dir := e2ePrepareTornDir(t)
		seg := e2eLastSegment(t, dir)
		fi, err := os.Stat(seg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.Truncate(seg, fi.Size()-3); err != nil {
			t.Fatal(err)
		}
		data, err := os.ReadFile(seg)
		if err != nil {
			t.Fatal(err)
		}
		_, off, scanErr := e2eScan(data)
		if scanErr == nil {
			t.Fatal("the truncated segment unexpectedly decodes cleanly")
		}

		before := dirState(t, dir)
		code, stderr := runToExit(t, "", aofArgs(dir, "always", "-aof-load-truncated=false")...)
		if code == 0 {
			t.Fatalf("server started on a torn log with aof-load-truncated=false\n%s", stderr)
		}
		if want := fmt.Sprintf("%s: at byte offset %d", filepath.Base(seg), off); !strings.Contains(stderr, want) {
			t.Fatalf("stderr does not name the file and offset (%q):\n%s", want, stderr)
		}
		if after := dirState(t, dir); !maps.Equal(before, after) {
			t.Fatalf("a refused start modified the data directory:\nbefore %v\nafter  %v", before, after)
		}
	})

	t.Run("corruption before the last record refuses to start", func(t *testing.T) {
		dir := e2ePrepareTornDir(t)
		seg := e2eLastSegment(t, dir)
		data, err := os.ReadFile(seg)
		if err != nil {
			t.Fatal(err)
		}
		starts, _, scanErr := e2eScan(data)
		if scanErr != nil || len(starts) != e2eTornKeys {
			t.Fatalf("expected %d clean records, got %d (%v)", e2eTornKeys, len(starts), scanErr)
		}
		// Break the '$' that opens the first argument of record 3 (a record that
		// is not the last one): "*3\r\n$3\r\n..." becomes "*3\r\nZ3\r\n...".
		start := starts[3]
		pos := start + int64(strings.IndexByte(string(data[start:]), '\n')) + 1
		data[pos] = 'Z'
		if err := os.WriteFile(seg, data, 0o644); err != nil {
			t.Fatal(err)
		}

		before := dirState(t, dir)
		// Even with the default aof-load-truncated=true: this is not a torn tail.
		code, stderr := runToExit(t, "", aofArgs(dir, "always")...)
		if code == 0 {
			t.Fatalf("server started on a corrupt log\n%s", stderr)
		}
		if want := fmt.Sprintf("%s: at byte offset %d", filepath.Base(seg), pos); !strings.Contains(stderr, want) {
			t.Fatalf("stderr does not name the file and offset (%q):\n%s", want, stderr)
		}
		if after := dirState(t, dir); !maps.Equal(before, after) {
			t.Fatalf("a refused start modified the data directory:\nbefore %v\nafter  %v", before, after)
		}
	})
}

// 8. With persistence off nothing touches the disk and the commands say so.
func TestE2EPersistenceOffCreatesNoFiles(t *testing.T) {
	ctx := context.Background()
	work := t.TempDir()
	cases := []struct {
		name string
		args []string
	}{
		{"default dir", nil}, // the default -dir ./data would land in work
		{"explicit dir", []string{"-dir", filepath.Join(work, "explicit-data")}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p := startProc(t, work, tc.args...)
			c := e2eClient(t, p.addr)
			if err := c.Set(ctx, "k", "v", 0).Err(); err != nil {
				t.Fatal(err)
			}
			for _, cmd := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF"} {
				if got := wErr(t, c, cmd); got != command.ErrPersistenceOff.Msg {
					t.Fatalf("%s: %q, want %q", cmd, got, command.ErrPersistenceOff.Msg)
				}
			}
			eq(t, "LASTSAVE", wInt(t, c, "LASTSAVE"), 0)

			assertEmpty := func(when string) {
				t.Helper()
				ents, err := os.ReadDir(work)
				if err != nil {
					t.Fatal(err)
				}
				if len(ents) != 0 {
					names := make([]string, len(ents))
					for i, e := range ents {
						names[i] = e.Name()
					}
					t.Fatalf("%s: persistence is off but %s holds %v", when, work, names)
				}
			}
			assertEmpty("while running")
			if code := p.term(30 * time.Second); code != 0 {
				t.Fatalf("exit code %d\n%s", code, p.logs())
			}
			assertEmpty("after shutdown")
		})
	}
}

// 9. Command replies over the wire.
func TestE2ECommandReplies(t *testing.T) {
	t.Run("aof", func(t *testing.T) {
		dir := t.TempDir()
		p := startProc(t, "", aofArgs(dir, "no")...)
		c := e2eClient(t, p.addr)
		if err := c.Set(context.Background(), "k", "v", 0).Err(); err != nil {
			t.Fatal(err)
		}

		eq(t, "SAVE", wText(t, c, "SAVE"), "OK")
		eq(t, "BGSAVE", wText(t, c, "BGSAVE"), "Background saving started")
		e2eWaitIdle(t, c)
		eq(t, "BGREWRITEAOF", wText(t, c, "BGREWRITEAOF"), "Background append only file rewriting started")
		e2eWaitIdle(t, c)

		now := time.Now().Unix()
		if l := wInt(t, c, "LASTSAVE"); l < now-10 || l > now+10 {
			t.Fatalf("LASTSAVE %d is not near now (%d)", l, now)
		}
		// LASTSAVE counts whole seconds, so keep saving until it moves.
		l0 := wInt(t, c, "LASTSAVE")
		eventually(t, 10*time.Second, "LASTSAVE to advance after SAVE", func() bool {
			time.Sleep(50 * time.Millisecond)
			eq(t, "SAVE", wText(t, c, "SAVE"), "OK")
			return wInt(t, c, "LASTSAVE") > l0
		})
	})

	t.Run("snapshot mode has no append-only file", func(t *testing.T) {
		dir := t.TempDir()
		p := startProc(t, "", "-persistence-mode", "snapshot", "-dir", dir)
		c := e2eClient(t, p.addr)
		eq(t, "SAVE", wText(t, c, "SAVE"), "OK")
		eq(t, "BGSAVE", wText(t, c, "BGSAVE"), "Background saving started")
		e2eWaitIdle(t, c)
		if got := wErr(t, c, "BGREWRITEAOF"); got != command.ErrAOFOff.Msg {
			t.Fatalf("BGREWRITEAOF: %q, want %q", got, command.ErrAOFOff.Msg)
		}
	})

	t.Run("already in progress", func(t *testing.T) {
		const (
			errSave    = "ERR Background save already in progress"
			errRewrite = "ERR Background append only file rewriting already in progress"
		)
		ctx := context.Background()
		dir := t.TempDir()
		p := startProc(t, "", aofArgs(dir, "no")...)
		c := e2eClient(t, p.addr)
		e2eBulkSet(t, c, "k:", 300_000, strings.Repeat("v", 64)) // a checkpoint that takes a while

		// One pipeline, so the later commands reach the server microseconds after
		// the first BGSAVE started a checkpoint that needs far longer. If the
		// checkpoint nevertheless finished first, try again.
		seen := false
		for attempt := 0; attempt < 5 && !seen; attempt++ {
			e2eWaitIdle(t, c)
			pipe := c.Pipeline()
			cmds := []*redis.Cmd{
				pipe.Do(ctx, "BGSAVE"),
				pipe.Do(ctx, "BGSAVE"),
				pipe.Do(ctx, "SAVE"),
				pipe.Do(ctx, "BGREWRITEAOF"),
			}
			_, _ = pipe.Exec(ctx) // each reply is inspected below
			if got, err := cmds[0].Text(); err != nil || got != "Background saving started" {
				t.Fatalf("first BGSAVE: %q, %v", got, err)
			}
			for i, want := range []string{errSave, errSave, errRewrite} {
				err := cmds[i+1].Err()
				if err == nil {
					continue // the checkpoint was already done when this command ran
				}
				if err.Error() != want {
					t.Fatalf("pipelined command %d: %q, want %q", i+1, err.Error(), want)
				}
				seen = true
			}
		}
		if !seen {
			t.Fatal("never saw an 'already in progress' reply in 5 attempts")
		}
		e2eWaitIdle(t, c)
	})
}
