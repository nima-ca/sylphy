package persist

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
	"github.com/nima-ca/sylphy/internal/store"
)

func newMemDir(t *testing.T) *MemFS {
	t.Helper()
	m := NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	return m
}

func dumpStore(t *testing.T, st *store.Store) []string {
	t.Helper()
	keys := st.Keys("*")
	slices.Sort(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		var d string
		mustDo(t, st.View(k, func(v store.Value) error {
			d = describe(v)
			return nil
		}))
		out = append(out, fmt.Sprintf("%q %s ttl=%d", k, d, st.ExpireAt(k)))
	}
	return out
}

func segRecords(t *testing.T, b []byte) string {
	t.Helper()
	rd := NewRecordReader(bytes.NewReader(b), RecordLimits{})
	var parts []string
	for {
		argv, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		mustDo(t, err)
		words := make([]string, len(argv))
		for i, a := range argv {
			words[i] = string(a)
		}
		parts = append(parts, strings.Join(words, " "))
	}
	return strings.Join(parts, " ; ")
}

// recoverImage recovers a post-crash disk image into a fresh store.
func recoverImage(t *testing.T, img *MemFS) ([]string, RecoverResult, error) {
	t.Helper()
	st, err := store.NewWithOptions(store.Options{Shards: 4, Clock: store.NewFakeClock(baseMs)})
	mustDo(t, err)
	reg, err := command.NewDefaultRegistry()
	mustDo(t, err)
	res, err := Recover(tctx(t), RecoverOptions{
		FS: img, Dir: "data", Target: st, Replay: NewCommandReplayer(reg, st, nil), LoadTruncated: true,
	})
	if err != nil {
		return nil, res, err
	}
	return dumpStore(t, st), res, nil
}

type sessOpts struct {
	mode           Mode
	policy         Policy
	pct            int
	minSize        int64
	snapOnShutdown bool
	clockMs        int64
	skipRecover    bool
	skipStart      bool
}

// session is a store, a Manager and one journaled client connection, wired the
// way main will wire them.
type session struct {
	t       *testing.T
	fs      *MemFS
	st      *store.Store
	clk     *store.FakeClock
	m       *Manager
	prop    command.Propagator
	ctx     *command.Context
	d       *command.Dispatcher
	buf     *bytes.Buffer
	wait    bool // fsync always: acknowledge only after WaitDurable, like the server will
	now     atomic.Int64
	mu      sync.Mutex
	tickers []*fakeTicker
	report  RecoveryReport
}

func newSession(t *testing.T, fsys *MemFS, o sessOpts) *session {
	t.Helper()
	if o.clockMs == 0 {
		o.clockMs = baseMs
	}
	clk := store.NewFakeClock(o.clockMs)
	st, err := store.NewWithOptions(store.Options{Shards: 4, Clock: clk})
	mustDo(t, err)
	reg, err := command.NewDefaultRegistry()
	mustDo(t, err)
	cfg := config.Default()
	s := &session{t: t, fs: fsys, st: st, clk: clk, d: command.NewDispatcher(reg, nil), buf: &bytes.Buffer{}}
	s.now.Store(1_700_000_000)
	m, err := NewManager(ManagerOptions{
		FS: fsys, Dir: "data", Store: st, Registry: reg, Config: &cfg, Mode: o.mode, Policy: o.policy,
		AutoRewritePercentage: o.pct, AutoRewriteMinSize: o.minSize, LoadTruncated: true, SnapshotOnShutdown: o.snapOnShutdown,
		NewTicker: func(time.Duration) Ticker {
			ft := newFakeTicker()
			s.mu.Lock()
			s.tickers = append(s.tickers, ft)
			s.mu.Unlock()
			return ft
		},
		Now:    func() time.Time { return time.Unix(s.now.Load(), 0) },
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	mustDo(t, err)
	s.m = m
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = m.Shutdown(ctx)
	})
	if !o.skipRecover {
		rep, err := m.Recover(tctx(t))
		mustDo(t, err)
		s.report = rep
	}
	if !o.skipStart {
		mustDo(t, m.Start(tctx(t)))
	}
	s.ctx = &command.Context{Store: st, W: protocol.NewWriter(s.buf), Config: &cfg, Conn: &command.ConnState{ID: 1}}
	if p := m.Propagator(); p != nil {
		s.ctx.EnableJournal(context.Background(), p)
		s.prop = p
	}
	s.wait = o.policy == PolicyAlways
	return s
}

// doErr runs a command and reports whether the client could be acknowledged:
// no error reply, no refused record, and (fsync always) a durable record.
func (s *session) doErr(args ...string) (string, error) {
	s.t.Helper()
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	s.buf.Reset()
	s.d.Dispatch(s.ctx, argv)
	_ = s.ctx.W.Flush()
	reply := s.buf.String()
	if strings.HasPrefix(reply, "-") {
		return reply, errors.New(strings.TrimSpace(reply))
	}
	if err := s.ctx.JournalErr(); err != nil {
		return reply, err
	}
	if s.wait && s.prop != nil {
		if err := s.prop.WaitDurable(tctx(s.t), s.ctx.LastSeq); err != nil {
			return reply, err
		}
	}
	return reply, nil
}

func (s *session) doAck(args ...string) (string, bool) {
	s.t.Helper()
	r, err := s.doErr(args...)
	return r, err == nil
}

func (s *session) do(args ...string) string {
	s.t.Helper()
	r, _ := s.doErr(args...)
	return r
}

func (s *session) mustAck(args ...string) {
	s.t.Helper()
	if _, err := s.doErr(args...); err != nil {
		s.t.Fatalf("%v: %v", args, err)
	}
}

func (s *session) dump() []string { return dumpStore(s.t, s.st) }

func (s *session) manifest() Manifest {
	s.t.Helper()
	m, found, err := LoadManifest(s.fs, "data")
	if err != nil || !found {
		s.t.Fatalf("manifest: found=%v err=%v", found, err)
	}
	return m
}

func (s *session) tick() {
	s.mu.Lock()
	ft := s.tickers[0]
	s.mu.Unlock()
	ft.Tick()
}

func (m *Manager) generation() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.manifest.Generation
}

func waitBG(t *testing.T, m *Manager) {
	t.Helper()
	mustDo(t, waitFor(tctx(t), &m.wg))
}

// waitCheckpoints waits until n checkpoint attempts have finished.
func waitCheckpoints(t *testing.T, m *Manager, n int64) {
	t.Helper()
	start := time.Now()
	for m.checkpoints.Load() < n {
		if time.Since(start) > 20*time.Second {
			t.Fatalf("only %d of %d checkpoints finished", m.checkpoints.Load(), n)
		}
		runtime.Gosched()
	}
	waitBG(t, m)
}

func eqLines(a, b []string) bool { return slices.Equal(a, b) }

func TestManagerAOFLifecycle(t *testing.T) {
	fs := newMemDir(t)
	s1 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	if s1.m.Propagator() == nil {
		t.Fatal("no propagator in AOF mode")
	}
	cmds := [][]string{
		{"SET", "a", "1"}, {"RPUSH", "l", "x", "y"}, {"HSET", "h", "f", "v"},
		{"SADD", "s", "m"}, {"ZADD", "z", "1.5", "q"}, {"SET", "t", "v", "EX", "100"},
	}
	for _, c := range cmds {
		if _, ok := s1.doAck(c...); !ok {
			t.Fatalf("%v was not acknowledged", c)
		}
	}
	want := s1.dump()
	if len(want) != 6 {
		t.Fatalf("dataset %v", want)
	}
	mustDo(t, s1.m.Shutdown(tctx(t)))
	select {
	case <-s1.m.autoDone:
	default:
		t.Fatal("the automatic trigger goroutine is still running")
	}
	select {
	case <-s1.m.aof.done:
	default:
		t.Fatal("the AOF flusher is still running")
	}
	if r := s1.do("SET", "late", "1"); !strings.HasPrefix(r, "-MISCONF") {
		t.Fatalf("a write after shutdown replied %q", r)
	}
	if got := fs.Names(); !slices.Equal(got, []string{"data/MANIFEST", "data/seg-000001.aof"}) {
		t.Fatalf("files %v", got)
	}

	s2 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	if got := s2.dump(); !eqLines(got, want) {
		t.Fatalf("after restart\n got %v\nwant %v", got, want)
	}
	if s2.report.Records != len(cmds) {
		t.Fatalf("replayed %d records, want %d", s2.report.Records, len(cmds))
	}
	mustDo(t, s2.m.Shutdown(tctx(t)))
	mustDo(t, s2.m.Shutdown(tctx(t))) // idempotent
}

func TestManagerCrashKeepsEveryAcknowledgedWrite(t *testing.T) {
	fs := newMemDir(t)
	s1 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	for i := range 50 {
		k := "key" + strconv.Itoa(i)
		s1.mustAck("SET", k, "v"+strconv.Itoa(i))
		if i%5 == 0 {
			s1.mustAck("INCR", "counter")
			s1.mustAck("RPUSH", "list", k)
			s1.mustAck("SET", k+":ttl", "x", "EX", "100")
		}
	}
	want := s1.dump()
	for _, opts := range []CrashOptions{
		{}, {DropDirOps: true},
		{DropDirOps: true, KeepUnsynced: func(_ string, n int) int { return n / 2 }},
	} {
		img := fs.Crash(opts)
		s2 := newSession(t, img, sessOpts{mode: ModeAOF, policy: PolicyAlways})
		if got := s2.dump(); !eqLines(got, want) {
			t.Fatalf("an acknowledged write was lost\n got %v\nwant %v", got, want)
		}
	}
}

func TestManagerSaveInAOFMode(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	s.mustAck("SET", "a", "1")
	s.mustAck("SET", "b", "2", "EX", "60")
	s.mustAck("RPUSH", "l", "x")
	s.now.Store(1_700_000_500)
	mustDo(t, s.m.Save(tctx(t)))
	if got := s.m.LastSave().Unix(); got != 1_700_000_500 {
		t.Fatalf("LastSave = %d", got)
	}
	man := s.manifest()
	if man.Base.Name != "base-000001.snap" || !slices.Equal(man.Segments, []string{"seg-000002.aof"}) || man.Generation != 3 {
		t.Fatalf("manifest %+v", man)
	}
	if got := fs.Names(); !slices.Equal(got, []string{"data/MANIFEST", "data/base-000001.snap", "data/seg-000002.aof"}) {
		t.Fatalf("files after the first checkpoint: %v", got)
	}

	s.mustAck("SET", "after", "save")
	s.mustAck("INCR", "n")
	want := s.dump()
	img := fs.Crash(CrashOptions{DropDirOps: true})
	got, _, err := recoverImage(t, img)
	mustDo(t, err)
	if !eqLines(got, want) {
		t.Fatalf("base plus new segment\n got %v\nwant %v", got, want)
	}

	mustDo(t, s.m.Save(tctx(t))) // a second checkpoint retires the first
	if got := fs.Names(); !slices.Equal(got, []string{"data/MANIFEST", "data/base-000002.snap", "data/seg-000003.aof"}) {
		t.Fatalf("files after the second checkpoint: %v", got)
	}
	if man := s.manifest(); man.Generation != 5 {
		t.Fatalf("generation %d", man.Generation)
	}
	got, _, err = recoverImage(t, fs.Crash(CrashOptions{DropDirOps: true}))
	mustDo(t, err)
	if !eqLines(got, want) {
		t.Fatalf("after the second checkpoint\n got %v\nwant %v", got, want)
	}
}

func TestManagerOnlyOneCheckpointAtATime(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	s.mustAck("SET", "a", "1")
	hit, release := make(chan struct{}), make(chan struct{})
	rel := sync.OnceFunc(func() { close(release) })
	t.Cleanup(rel)
	var once sync.Once
	fs.SetHook(func(op Op) error {
		if op.Kind == OpWrite && strings.HasSuffix(op.Name, ".snap.tmp") {
			once.Do(func() {
				close(hit)
				<-release
			})
		}
		return nil
	})
	mustDo(t, s.m.BGSave())
	<-hit // the checkpoint is writing its base
	if err := s.m.BGSave(); !errors.Is(err, ErrCheckpointRunning) {
		t.Fatalf("second BGSave: %v", err)
	}
	if err := s.m.Save(tctx(t)); !errors.Is(err, ErrCheckpointRunning) {
		t.Fatalf("Save during BGSave: %v", err)
	}
	rel()
	waitBG(t, s.m)
	if man := s.manifest(); man.Base.Name == "" || s.m.checkpoints.Load() != 1 {
		t.Fatalf("manifest %+v after %d checkpoints", man, s.m.checkpoints.Load())
	}
	mustDo(t, s.m.BGSave()) // free again
	waitBG(t, s.m)
}

func TestManagerShutdownAbortsARunningCheckpointCleanly(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	s.mustAck("SET", "a", "1")
	s.mustAck("RPUSH", "l", "x", "y")
	want := s.dump()
	hit, release := make(chan struct{}), make(chan struct{})
	rel := sync.OnceFunc(func() { close(release) })
	t.Cleanup(rel)
	var once sync.Once
	fs.SetHook(func(op Op) error {
		if op.Kind == OpWrite && strings.HasSuffix(op.Name, ".snap.tmp") {
			once.Do(func() {
				close(hit)
				<-release
			})
		}
		return nil
	})
	mustDo(t, s.m.BGSave())
	<-hit
	done := make(chan error, 1)
	go func() { done <- s.m.Shutdown(tctx(t)) }()
	<-s.m.bgCtx.Done() // Shutdown has cancelled the checkpoint
	rel()
	if err := <-done; err != nil {
		t.Fatalf("Shutdown: %v", err)
	}
	for _, n := range fs.Names() {
		if strings.HasSuffix(n, TmpSuffix) || strings.HasSuffix(n, ".snap") {
			t.Fatalf("the aborted checkpoint left %s behind", n)
		}
	}
	s2 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	if got := s2.dump(); !eqLines(got, want) {
		t.Fatalf("after an aborted checkpoint\n got %v\nwant %v", got, want)
	}
}

func TestManagerSnapshotMode(t *testing.T) {
	t.Run("Save persists the state at the save and nothing later", func(t *testing.T) {
		fs := newMemDir(t)
		s := newSession(t, fs, sessOpts{mode: ModeSnapshot})
		if s.m.Propagator() != nil || s.prop != nil {
			t.Fatal("snapshot mode must not journal")
		}
		s.mustAck("SET", "a", "1")
		s.mustAck("RPUSH", "l", "x")
		atSave := s.dump()
		mustDo(t, s.m.Save(tctx(t)))
		man := s.manifest()
		if man.Base.Name != "base-000001.snap" || len(man.Segments) != 0 {
			t.Fatalf("manifest %+v", man)
		}
		if got := fs.Names(); !slices.Equal(got, []string{"data/MANIFEST", "data/base-000001.snap"}) {
			t.Fatalf("files %v", got)
		}
		s.mustAck("SET", "b", "2") // after the save: not persisted until the next one
		got, _, err := recoverImage(t, fs.Crash(CrashOptions{DropDirOps: true}))
		mustDo(t, err)
		if !eqLines(got, atSave) {
			t.Fatalf("recovered\n%v\nwant the state at the save\n%v", got, atSave)
		}
		mustDo(t, s.m.BGSave())
		waitBG(t, s.m)
		if got := fs.Names(); !slices.Equal(got, []string{"data/MANIFEST", "data/base-000002.snap"}) {
			t.Fatalf("files after BGSAVE: %v", got)
		}
		got, _, err = recoverImage(t, fs.Crash(CrashOptions{DropDirOps: true}))
		mustDo(t, err)
		if !eqLines(got, s.dump()) {
			t.Fatalf("after BGSAVE: %v", got)
		}
	})
	t.Run("final checkpoint at shutdown", func(t *testing.T) {
		fs := newMemDir(t)
		s := newSession(t, fs, sessOpts{mode: ModeSnapshot, snapOnShutdown: true})
		s.mustAck("SET", "a", "1")
		s.mustAck("SADD", "s", "m")
		want := s.dump()
		mustDo(t, s.m.Shutdown(tctx(t)))
		s2 := newSession(t, fs, sessOpts{mode: ModeSnapshot})
		if got := s2.dump(); !eqLines(got, want) {
			t.Fatalf("got %v want %v", got, want)
		}
	})
	t.Run("without it a clean shutdown saves nothing", func(t *testing.T) {
		fs := newMemDir(t)
		s := newSession(t, fs, sessOpts{mode: ModeSnapshot})
		s.mustAck("SET", "a", "1")
		mustDo(t, s.m.Shutdown(tctx(t)))
		if got := newSession(t, fs, sessOpts{mode: ModeSnapshot}).dump(); len(got) != 0 {
			t.Fatalf("got %v", got)
		}
	})
	t.Run("a directory last used with the log is recovered and then replaced", func(t *testing.T) {
		fs := newMemDir(t)
		s1 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
		s1.mustAck("SET", "a", "1")
		s1.mustAck("RPUSH", "l", "x")
		want := s1.dump()
		mustDo(t, s1.m.Shutdown(tctx(t)))

		s2 := newSession(t, fs, sessOpts{mode: ModeSnapshot})
		if got := s2.dump(); !eqLines(got, want) {
			t.Fatalf("snapshot mode lost the log's data: %v", got)
		}
		mustDo(t, s2.m.Save(tctx(t)))
		man := s2.manifest()
		if man.Base.Name == "" || len(man.Segments) != 0 {
			t.Fatalf("manifest %+v", man)
		}
		if got := fs.Names(); !slices.Equal(got, []string{"data/MANIFEST", "data/" + man.Base.Name}) {
			t.Fatalf("files %v", got)
		}
		if got := newSession(t, fs, sessOpts{mode: ModeSnapshot}).dump(); !eqLines(got, want) {
			t.Fatalf("after the switch: %v", got)
		}
	})
}

// TestManagerPurgeLogsDelBeforeListening is the zombie-key scenario: a key
// whose deadline passed while the server was down is still stored (from the
// base snapshot) when recovery ends. If the purge only forgot it in memory, a
// later INCR would be logged against the stale key and replay as "2" with a
// dead deadline, losing the live key "1". Start must log DEL and make it
// durable first.
func TestManagerPurgeLogsDelBeforeListening(t *testing.T) {
	fs := newMemDir(t)
	s1 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways, snapOnShutdown: true})
	s1.mustAck("SET", "z", "1", "EX", "5")
	s1.mustAck("SET", "keep", "k")
	mustDo(t, s1.m.Shutdown(tctx(t))) // the base now holds z with its deadline

	// Policy "no": only the purge's own wait can make the DEL durable.
	s2 := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyNo, clockMs: baseMs + 10_000})
	if got := s2.dump(); len(got) != 1 || !strings.Contains(got[0], `"keep"`) {
		t.Fatalf("after the purge: %v", got)
	}
	img := fs.Crash(CrashOptions{DropDirOps: true})
	man, _, err := LoadManifest(img, "data")
	mustDo(t, err)
	seg, ok := img.Contents("data/" + man.Segments[len(man.Segments)-1])
	if !ok {
		t.Fatal("no active segment")
	}
	if got := segRecords(t, seg); got != "DEL z" {
		t.Fatalf("durable log after Start holds %q, want the purge's DEL", got)
	}

	s2.prop = s2.m.Propagator()
	s2.wait = true
	s2.mustAck("INCR", "z") // the live z is 1, no TTL
	want := s2.dump()
	if len(want) != 2 {
		t.Fatalf("live dataset %v", want)
	}
	got, res, err := recoverImage(t, fs.Crash(CrashOptions{DropDirOps: true}))
	mustDo(t, err)
	if !eqLines(got, want) {
		t.Fatalf("a write to a purged key replayed against the stale copy\n got %v\nwant %v", got, want)
	}
	if res.Report.Records != 2 {
		t.Fatalf("replayed %d records, want DEL and INCR", res.Report.Records)
	}
}

func TestShouldCheckpoint(t *testing.T) {
	tests := []struct {
		name            string
		base, incr, min int64
		pct             int
		want            bool
	}{
		{"no base: only the minimum matters", 0, 100, 0, 100, true},
		{"grown less than the percentage", 1000, 999, 0, 100, false},
		{"grown exactly the percentage", 1000, 1000, 0, 100, true},
		{"below the minimum size", 1000, 5000, 10_000, 100, false},
		{"at the minimum size", 1000, 9000, 10_000, 100, true},
		{"disabled", 1000, 1 << 30, 0, 0, false},
		{"nothing written", 1000, 0, 0, 100, false},
		{"fifty percent", 100, 50, 0, 50, true},
		{"just under fifty percent", 100, 49, 0, 50, false},
	}
	for _, tc := range tests {
		if got := shouldCheckpoint(tc.base, tc.incr, tc.pct, tc.min); got != tc.want {
			t.Errorf("%s: shouldCheckpoint(%d, %d, %d, %d) = %v", tc.name, tc.base, tc.incr, tc.pct, tc.min, got)
		}
	}
}

func TestManagerAutoCheckpointTrigger(t *testing.T) {
	t.Run("fires past the minimum size and then resets", func(t *testing.T) {
		fs := newMemDir(t)
		s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways, pct: 100, minSize: 400})
		s.mu.Lock()
		if len(s.tickers) != 1 {
			s.mu.Unlock()
			t.Fatalf("%d tickers; with fsync always only the trigger's exists", len(s.tickers))
		}
		s.mu.Unlock()
		s.mustAck("SET", "small", "1")
		gen := s.m.generation()
		s.tick()
		s.tick()
		waitBG(t, s.m)
		if s.m.checkpoints.Load() != 0 || s.m.generation() != gen {
			t.Fatal("a checkpoint ran below the minimum size")
		}
		for i := 0; s.m.incrementalBytes() < 400; i++ {
			s.mustAck("SET", "key"+strconv.Itoa(i), strings.Repeat("x", 60))
		}
		s.tick()
		waitCheckpoints(t, s.m, 1)
		if man := s.manifest(); man.Base.Name == "" || len(man.Segments) != 1 {
			t.Fatalf("manifest %+v", man)
		}
		if got := s.m.incrementalBytes(); got >= 400 {
			t.Fatalf("incremental size %d was not reset", got)
		}
		s.tick()
		s.tick()
		waitBG(t, s.m)
		if got := s.m.checkpoints.Load(); got != 1 {
			t.Fatalf("%d checkpoints; the trigger fired again right after one", got)
		}
	})
	t.Run("a zero percentage disables it", func(t *testing.T) {
		fs := newMemDir(t)
		s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
		for i := range 20 {
			s.mustAck("SET", "key"+strconv.Itoa(i), strings.Repeat("x", 100))
		}
		s.tick()
		s.tick()
		waitBG(t, s.m)
		if s.m.checkpoints.Load() != 0 {
			t.Fatal("a checkpoint ran with the trigger disabled")
		}
	})
}

func TestManagerBacksOffAfterAFailedCheckpoint(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways, pct: 100})
	s.mustAck("SET", "a", "1")
	fired := false
	fs.SetHook(func(op Op) error { // the hook runs under the MemFS lock
		if !fired && op.Kind == OpOpen && strings.HasSuffix(op.Name, ".snap.tmp") {
			fired = true
			return errBoom
		}
		return nil
	})
	s.tick()
	waitCheckpoints(t, s.m, 1)
	if got := s.m.skipTicks.Load(); got != failureBackoffTicks {
		t.Fatalf("back-off is %d ticks", got)
	}
	gen := s.m.generation()
	s.mustAck("SET", "b", "2") // still due
	for range failureBackoffTicks {
		s.tick()
	}
	waitBG(t, s.m)
	if got := s.m.checkpoints.Load(); got != 1 {
		t.Fatalf("%d attempts: the trigger did not back off", got)
	}
	s.tick() // the first tick after the back-off
	waitCheckpoints(t, s.m, 2)
	if s.m.generation() <= gen || s.manifest().Base.Name == "" || s.m.skipTicks.Load() != 0 {
		t.Fatalf("the retry did not complete: generation %d, manifest %+v", s.m.generation(), s.manifest())
	}
}

func TestManagerHealsAfterALatchedLogError(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways, pct: 100})
	s.mustAck("SET", "a", "1")
	var armed atomic.Bool
	fs.SetHook(func(op Op) error {
		if op.Kind == OpSync && armed.Load() && strings.HasSuffix(op.Name, ".aof") {
			return errBoom
		}
		return nil
	})
	armed.Store(true)
	if _, ok := s.doAck("SET", "b", "2"); ok {
		t.Fatal("a write whose fsync failed was acknowledged")
	}
	if s.m.Err() == nil {
		t.Fatal("persistence is not in its error state")
	}
	if r := s.do("SET", "c", "3"); !strings.HasPrefix(r, "-MISCONF") {
		t.Fatalf("a write while latched replied %q", r)
	}
	if r := s.do("GET", "a"); r != "$1\r\n1\r\n" {
		t.Fatalf("reads must keep working: %q", r)
	}

	armed.Store(false) // the disk recovers; the trigger notices the error state
	s.tick()
	waitCheckpoints(t, s.m, 1)
	if err := s.m.Err(); err != nil {
		t.Fatalf("still latched after the checkpoint: %v", err)
	}
	s.mustAck("SET", "c", "3")
	want := s.dump() // "b" is in memory although the log never got it
	if len(want) != 3 {
		t.Fatalf("live dataset %v", want)
	}
	got, _, err := recoverImage(t, fs.Crash(CrashOptions{DropDirOps: true}))
	mustDo(t, err)
	if !eqLines(got, want) {
		t.Fatalf("the healing checkpoint did not reconcile memory and disk\n got %v\nwant %v", got, want)
	}
}

func TestManagerShutdownFlushesEvenWhenTheFinalCheckpointIsCancelled(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyNo, snapOnShutdown: true})
	s.mustAck("SET", "a", "1") // written, never fsynced
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.m.Shutdown(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Shutdown = %v", err)
	}
	got, _, err := recoverImage(t, fs.Crash(CrashOptions{DropDirOps: true}))
	mustDo(t, err)
	if len(got) != 1 || !strings.Contains(got[0], `"a"`) {
		t.Fatalf("the log was not flushed and fsynced: %v", got)
	}
}

func TestManagerStateErrors(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, skipRecover: true, skipStart: true})
	if err := s.m.Start(tctx(t)); err == nil {
		t.Fatal("Start before Recover succeeded")
	}
	if err := s.m.BGSave(); err == nil {
		t.Fatal("BGSave before Start succeeded")
	}
	if err := s.m.Save(tctx(t)); err == nil {
		t.Fatal("Save before Start succeeded")
	}
	_, err := s.m.Recover(tctx(t))
	mustDo(t, err)
	mustDo(t, s.m.Start(tctx(t)))
	mustDo(t, s.m.Start(tctx(t))) // idempotent
	mustDo(t, s.m.Shutdown(tctx(t)))
	mustDo(t, s.m.Shutdown(tctx(t)))
	if err := s.m.BGSave(); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("BGSave after Shutdown: %v", err)
	}
	if err := s.m.Save(tctx(t)); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Save after Shutdown: %v", err)
	}
	if err := s.m.Start(tctx(t)); !errors.Is(err, ErrManagerClosed) {
		t.Fatalf("Start after Shutdown: %v", err)
	}
}

func TestNewManagerValidation(t *testing.T) {
	st, err := store.NewWithOptions(store.Options{Shards: 4})
	mustDo(t, err)
	reg, err := command.NewDefaultRegistry()
	mustDo(t, err)
	good := ManagerOptions{FS: NewMemFS(), Dir: "data", Store: st, Registry: reg, Mode: ModeAOF}
	if _, err := NewManager(good); err != nil {
		t.Fatal(err)
	}
	for name, mut := range map[string]func(*ManagerOptions){
		"no fs":         func(o *ManagerOptions) { o.FS = nil },
		"no dir":        func(o *ManagerOptions) { o.Dir = "" },
		"no store":      func(o *ManagerOptions) { o.Store = nil },
		"no registry":   func(o *ManagerOptions) { o.Registry = nil },
		"no mode":       func(o *ManagerOptions) { o.Mode = 0 },
		"bad mode":      func(o *ManagerOptions) { o.Mode = 9 },
		"bad policy":    func(o *ManagerOptions) { o.Policy = 9 },
		"negative pct":  func(o *ManagerOptions) { o.AutoRewritePercentage = -1 },
		"negative size": func(o *ManagerOptions) { o.AutoRewriteMinSize = -1 },
	} {
		o := good
		mut(&o)
		if _, err := NewManager(o); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if ModeAOF.String() != "aof" || ModeSnapshot.String() != "snapshot" || Mode(7).String() != "Mode(7)" {
		t.Fatal("Mode.String")
	}
}

// mgrScenario sets up a directory whose durable state is O = {k1, k2, k3}
// (base 1 from a checkpoint plus a log segment), then, with a crash armed at the
// k-th file-system operation, runs: write k4, a checkpoint through the Manager,
// write k5. acked counts the writes that were acknowledged as durable.
func mgrScenario(t *testing.T, k int) (fs *MemFS, acked int, err error) {
	t.Helper()
	fs = newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = s.m.Shutdown(ctx)
	}()
	s.mustAck("SET", "k1", "v1")
	s.mustAck("SET", "k2", "v2", "EX", "60")
	mustDo(t, s.m.Save(tctx(t)))
	s.mustAck("SET", "k3", "v3")

	fs.CrashAt(fs.Ops() + k)
	if _, err = s.doErr("SET", "k4", "v4"); err != nil {
		return fs, acked, err
	}
	acked = 1
	if err = s.m.Save(tctx(t)); err != nil {
		return fs, acked, err
	}
	if _, err = s.doErr("SET", "k5", "v5"); err != nil {
		return fs, acked, err
	}
	acked = 2
	return fs, acked, nil
}

// TestManagerCheckpointCrashMatrix is the Part 3 crash matrix run against the
// real orchestrator: crash a full checkpoint (log rotation, base write, manifest
// commit, retirement) at every file-system operation under four disk models and
// require a consistent dataset that includes every acknowledged write, exactly
// the files the manifest lists, and an unchanged result when recovering again.
func TestManagerCheckpointCrashMatrix(t *testing.T) {
	ref := newSession(t, newMemDir(t), sessOpts{mode: ModeAOF, policy: PolicyNo})
	ref.mustAck("SET", "k1", "v1")
	ref.mustAck("SET", "k2", "v2", "EX", "60")
	ref.mustAck("SET", "k3", "v3")
	cands := [][]string{ref.dump()}
	ref.mustAck("SET", "k4", "v4")
	cands = append(cands, ref.dump())
	ref.mustAck("SET", "k5", "v5")
	cands = append(cands, ref.dump())

	models := []struct {
		name string
		opts CrashOptions
	}{
		{"optimistic", CrashOptions{}},
		{"pessimistic", CrashOptions{DropDirOps: true}},
		{"optimistic, torn tail", CrashOptions{KeepUnsynced: func(_ string, n int) int { return n / 2 }}},
		{"pessimistic, torn tail", CrashOptions{DropDirOps: true, KeepUnsynced: func(_ string, n int) int { return n / 2 }}},
	}
	check := func(label string, fs *MemFS, acked int) {
		t.Helper()
		for _, md := range models {
			img := fs.Crash(md.opts)
			got, res, err := recoverImage(t, img)
			if err != nil {
				t.Fatalf("%s [%s]: recovery failed: %v", label, md.name, err)
			}
			level := -1
			for i, c := range cands {
				if eqLines(got, c) {
					level = i
				}
			}
			if level < 0 {
				t.Fatalf("%s [%s]: recovered a dataset that never existed: %v", label, md.name, got)
			}
			if level < acked {
				t.Fatalf("%s [%s]: recovered state %d but %d writes were acknowledged", label, md.name, level, acked)
			}
			want := []string{"data/MANIFEST"}
			if res.Manifest.Base.Name != "" {
				want = append(want, "data/"+res.Manifest.Base.Name)
			}
			for _, sg := range res.Manifest.Segments {
				want = append(want, "data/"+sg)
			}
			slices.Sort(want)
			if files := img.Names(); !slices.Equal(files, want) {
				t.Fatalf("%s [%s]: directory holds %v, want exactly %v", label, md.name, files, want)
			}
			again, res2, err := recoverImage(t, img)
			if err != nil || !eqLines(again, got) || res2.Report.TruncatedBytes != 0 || res2.Manifest.Generation != res.Manifest.Generation {
				t.Fatalf("%s [%s]: recovering again changed the outcome: %v", label, md.name, err)
			}
		}
	}

	crashPoints := 0
	for k := 1; k <= 80; k++ {
		fs, acked, err := mgrScenario(t, k)
		if err == nil {
			check("completed", fs, 2)
			break
		}
		if !errors.Is(err, ErrCrashed) {
			t.Fatalf("k=%d: %v", k, err)
		}
		crashPoints++
		check(fmt.Sprintf("crash at op %d", k), fs, acked)
	}
	if crashPoints < 25 {
		t.Fatalf("only %d crash points: the matrix is not covering the checkpoint", crashPoints)
	}
}
