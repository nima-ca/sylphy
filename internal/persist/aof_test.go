package persist

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const seg1 = "data/seg-000001.aof"

func recN(i int) []byte { return AppendRecord(nil, argv("SET", "k", strconv.Itoa(i))) }

func tctx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

// fakeTicker is a ticker the test fires by hand. Tick blocks until the flusher
// receives it, so two Ticks in a row mean the first one's cycle is complete.
type fakeTicker struct {
	c        chan time.Time
	stopped  atomic.Bool
	interval atomic.Int64
}

func newFakeTicker() *fakeTicker                 { return &fakeTicker{c: make(chan time.Time)} }
func (f *fakeTicker) C() <-chan time.Time        { return f.c }
func (f *fakeTicker) Stop()                      { f.stopped.Store(true) }
func (f *fakeTicker) Tick()                      { f.c <- time.Time{} }
func (f *fakeTicker) tickAndSettle(t *testing.T) { t.Helper(); f.Tick(); f.Tick() }

// tornFS makes a Write, when armed, store half of its bytes and fail.
type tornFS struct {
	FS
	armed atomic.Bool
}

func (t *tornFS) OpenFile(name string, flag int, perm os.FileMode) (File, error) {
	f, err := t.FS.OpenFile(name, flag, perm)
	if err != nil {
		return nil, err
	}
	return &tornFile{File: f, fs: t}, nil
}

type tornFile struct {
	File
	fs *tornFS
}

func (f *tornFile) Write(p []byte) (int, error) {
	if f.fs.armed.Load() && len(p) > 1 {
		n, _ := f.File.Write(p[:len(p)/2])
		return n, errBoom
	}
	return f.File.Write(p)
}

type harnessOpts struct {
	policy    Policy
	highWater int
	wrap      func(*MemFS) FS
	tweak     func(*AOFOptions)
}

type harness struct {
	t    *testing.T
	mem  *MemFS
	fsys FS
	tick *fakeTicker
	logs *syncBuf
	aof  *AOF

	mu         sync.Mutex
	manifest   Manifest
	beforeList func(seg string)
}

func newHarness(t *testing.T, o harnessOpts) *harness {
	t.Helper()
	h := &harness{t: t, mem: NewMemFS(), tick: newFakeTicker(), logs: &syncBuf{}}
	mustDo(t, h.mem.MkdirAll("data", 0o755))
	h.fsys = h.mem
	if o.wrap != nil {
		h.fsys = o.wrap(h.mem)
	}
	opts := AOFOptions{
		FS: h.fsys, Dir: "data", NextSegmentID: 1, Policy: o.policy, HighWater: o.highWater,
		NewTicker: func(d time.Duration) Ticker { h.tick.interval.Store(int64(d)); return h.tick },
		OnRotate:  h.onRotate,
		Logger:    slog.New(slog.NewTextHandler(h.logs, nil)),
	}
	if o.tweak != nil {
		o.tweak(&opts)
	}
	a, err := OpenAOF(opts)
	mustDo(t, err)
	h.aof = a
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_ = h.aof.Close(ctx)
	})
	return h
}

// onRotate is the manifest committer a Manager will provide: it lists the new
// segment durably.
func (h *harness) onRotate(seg string) error {
	h.mu.Lock()
	hook := h.beforeList
	h.mu.Unlock()
	if hook != nil {
		hook(seg)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	h.manifest.Generation++
	h.manifest.Segments = append(h.manifest.Segments, seg)
	return CommitManifest(h.fsys, "data", h.manifest)
}

func (h *harness) setBeforeList(f func(string)) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.beforeList = f
}

func (h *harness) listed() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return slices.Clone(h.manifest.Segments)
}

func (h *harness) contents(name string) []byte {
	h.t.Helper()
	b, ok := h.mem.Contents(name)
	if !ok {
		h.t.Fatalf("%s does not exist", name)
	}
	return b
}

// opCounter counts the writes and fsyncs of segment files.
type opCounter struct{ writes, syncs atomic.Int32 }

func (c *opCounter) hook(extra func(Op) error) func(Op) error {
	return func(op Op) error {
		if !strings.HasSuffix(op.Name, ".aof") {
			return nil
		}
		switch op.Kind {
		case OpWrite:
			c.writes.Add(1)
		case OpSync:
			c.syncs.Add(1)
		}
		if extra != nil {
			return extra(op)
		}
		return nil
	}
}

// gateFirstWrite blocks the first segment write until the returned release is
// called; hit is closed when the flusher is inside it.
func gateFirstWrite(t *testing.T, m *MemFS) (hit <-chan struct{}, release func()) {
	t.Helper()
	h, rel := make(chan struct{}), make(chan struct{})
	release = sync.OnceFunc(func() { close(rel) })
	t.Cleanup(release)
	var once sync.Once
	m.SetHook(func(op Op) error {
		if op.Kind == OpWrite && strings.HasSuffix(op.Name, ".aof") {
			once.Do(func() { close(h); <-rel })
		}
		return nil
	})
	return h, release
}

func failSyncs(m *MemFS, armed *atomic.Bool) {
	m.SetHook(func(op Op) error {
		if op.Kind == OpSync && armed.Load() && strings.HasSuffix(op.Name, ".aof") {
			return errBoom
		}
		return nil
	})
}

func TestAOFAppendAndFlush(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyNo})
	var want []byte
	for i := 1; i <= 3; i++ {
		r := recN(i)
		want = append(want, r...)
		seq, err := h.aof.Append(r)
		if err != nil || seq != uint64(i) {
			t.Fatalf("Append %d = %d, %v", i, seq, err)
		}
	}
	mustDo(t, h.aof.Flush(tctx(t)))
	if got := h.contents(seg1); !bytes.Equal(got, want) {
		t.Fatalf("segment holds %q, want %q", got, want)
	}
	st := h.aof.Stats()
	if st.Appended != 3 || st.Written != 3 || st.Durable != 0 || st.PendingBytes != 0 ||
		st.BytesWritten != uint64(len(want)) || st.Segment != "seg-000001.aof" || st.SegmentSize != int64(len(want)) {
		t.Fatalf("stats %+v", st)
	}
	if h.tick.interval.Load() != 0 {
		t.Fatal("PolicyNo must not create a ticker")
	}
	if seq, err := h.aof.Append(recN(4), recN(5)); err != nil || seq != 5 {
		t.Fatalf("group append = %d, %v", seq, err)
	}
	if seq, err := h.aof.Append(); err != nil || seq != 5 {
		t.Fatalf("empty append = %d, %v", seq, err)
	}
	mustDo(t, h.aof.Flush(tctx(t)))
	if got := h.contents(seg1); !bytes.Equal(got, slices.Concat(want, recN(4), recN(5))) {
		t.Fatal("group append not contiguous")
	}
}

func TestAOFAlwaysGroupCommit(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyAlways})
	var c opCounter
	hit, release := make(chan struct{}), make(chan struct{})
	rel := sync.OnceFunc(func() { close(release) })
	t.Cleanup(rel)
	h.mem.SetHook(c.hook(func(op Op) error {
		if op.Kind == OpSync && c.syncs.Load() == 1 {
			close(hit)
			<-release
		}
		return nil
	}))
	if _, err := h.aof.Append(recN(1)); err != nil {
		t.Fatal(err)
	}
	<-hit // the first fsync is in flight, covering only record 1
	var want []byte
	want = append(want, recN(1)...)
	for i := 2; i <= 100; i++ {
		if _, err := h.aof.Append(recN(i)); err != nil {
			t.Fatal(err)
		}
		want = append(want, recN(i)...)
	}
	rel()
	mustDo(t, h.aof.WaitDurable(tctx(t), 100))
	if w, s := c.writes.Load(), c.syncs.Load(); w != 2 || s != 2 {
		t.Fatalf("100 records took %d writes and %d fsyncs, want 2 and 2 (group commit)", w, s)
	}
	img := h.mem.Crash(CrashOptions{DropDirOps: true})
	if got, _ := img.Contents(seg1); !bytes.Equal(got, want) {
		t.Fatal("acknowledged records are not on the pessimistic post-crash disk")
	}
}

func TestAOFEverySecPolicy(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyEverySec})
	if time.Duration(h.tick.interval.Load()) != time.Second {
		t.Fatalf("ticker interval %v", time.Duration(h.tick.interval.Load()))
	}
	var c opCounter
	h.mem.SetHook(c.hook(nil))
	var want []byte
	for i := 1; i <= 3; i++ {
		want = append(want, recN(i)...)
		if _, err := h.aof.Append(recN(i)); err != nil {
			t.Fatal(err)
		}
	}
	mustDo(t, h.aof.Flush(tctx(t)))
	if st := h.aof.Stats(); st.Written != 3 || st.Durable != 0 {
		t.Fatalf("before the tick: %+v", st)
	}
	// This is the everysec loss window: written, not yet fsynced.
	if got, _ := h.mem.Crash(CrashOptions{DropDirOps: true}).Contents(seg1); len(got) != 0 {
		t.Fatalf("unsynced records survived: %q", got)
	}
	h.tick.tickAndSettle(t)
	if st := h.aof.Stats(); st.Durable != 3 || c.syncs.Load() != 1 {
		t.Fatalf("after the tick: %+v, %d syncs", st, c.syncs.Load())
	}
	if got, _ := h.mem.Crash(CrashOptions{DropDirOps: true}).Contents(seg1); !bytes.Equal(got, want) {
		t.Fatal("the tick did not make the records durable")
	}
	h.tick.tickAndSettle(t) // nothing new: no fsync
	if c.syncs.Load() != 1 {
		t.Fatalf("an idle tick fsynced (%d syncs)", c.syncs.Load())
	}
	seq, err := h.aof.Append(recN(4))
	mustDo(t, err)
	mustDo(t, h.aof.Flush(tctx(t)))
	if h.aof.Stats().Durable != 3 {
		t.Fatal("a record became durable before its tick")
	}
	h.tick.tickAndSettle(t)
	if st := h.aof.Stats(); st.Durable != seq || c.syncs.Load() != 2 {
		t.Fatalf("second batch: %+v, %d syncs", st, c.syncs.Load())
	}
}

func TestAOFNoPolicyFsyncsOnlyWhenAsked(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyNo})
	var c opCounter
	h.mem.SetHook(c.hook(nil))
	r := recN(1)
	seq, err := h.aof.Append(r)
	mustDo(t, err)
	mustDo(t, h.aof.Flush(tctx(t)))
	if h.aof.Stats().Durable != 0 || c.syncs.Load() != 0 {
		t.Fatal("PolicyNo fsynced on its own")
	}
	mustDo(t, h.aof.WaitDurable(tctx(t), seq)) // an explicit request forces it
	if h.aof.Stats().Durable != seq || c.syncs.Load() != 1 {
		t.Fatalf("explicit WaitDurable: %+v, %d syncs", h.aof.Stats(), c.syncs.Load())
	}
	if got, _ := h.mem.Crash(CrashOptions{DropDirOps: true}).Contents(seg1); !bytes.Equal(got, r) {
		t.Fatal("WaitDurable returned before the record was durable")
	}
	if err := h.aof.WaitDurable(tctx(t), 99); err == nil {
		t.Fatal("WaitDurable beyond the last sequence succeeded")
	}
}

func TestAOFAdmitBackpressure(t *testing.T) {
	blocked := make(chan struct{}, 1)
	h := newHarness(t, harnessOpts{
		policy: PolicyNo, highWater: 60,
		tweak: func(o *AOFOptions) { o.admitBlocked = func() { blocked <- struct{}{} } },
	})
	hit, release := gateFirstWrite(t, h.mem)
	ctx := tctx(t)
	mustDo(t, h.aof.Admit(ctx)) // empty buffer: admitted at once
	if _, err := h.aof.Append(recN(1)); err != nil {
		t.Fatal(err)
	}
	<-hit // record 1 is in a blocked write and still counts as buffered
	for i := 2; i <= 4; i++ {
		if _, err := h.aof.Append(recN(i)); err != nil {
			t.Fatal(err)
		}
	}
	admitted := make(chan error, 1)
	go func() { admitted <- h.aof.Admit(ctx) }()
	<-blocked
	select {
	case err := <-admitted:
		t.Fatalf("Admit returned %v above the high-water mark", err)
	default:
	}
	release()
	if err := <-admitted; err != nil {
		t.Fatalf("Admit after the buffer drained: %v", err)
	}
	if got := h.aof.Stats().AdmitWaits; got != 1 {
		t.Fatalf("AdmitWaits = %d", got)
	}
}

func TestAOFAdmitHonorsContext(t *testing.T) {
	blocked := make(chan struct{}, 1)
	h := newHarness(t, harnessOpts{
		policy: PolicyNo, highWater: 10,
		tweak: func(o *AOFOptions) { o.admitBlocked = func() { blocked <- struct{}{} } },
	})
	hit, _ := gateFirstWrite(t, h.mem)
	if _, err := h.aof.Append(recN(1)); err != nil {
		t.Fatal(err)
	}
	<-hit
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	res := make(chan error, 1)
	go func() { res <- h.aof.Admit(ctx) }()
	<-blocked
	cancel()
	if err := <-res; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestAOFWriteErrorLatchesAndCutsTornTail(t *testing.T) {
	var torn *tornFS
	h := newHarness(t, harnessOpts{policy: PolicyAlways, wrap: func(m *MemFS) FS {
		torn = &tornFS{FS: m}
		return torn
	}})
	r1 := recN(1)
	seq, err := h.aof.Append(r1)
	mustDo(t, err)
	mustDo(t, h.aof.WaitDurable(tctx(t), seq))

	torn.armed.Store(true)
	seq, err = h.aof.Append(recN(2)) // accepted: the failure is asynchronous
	mustDo(t, err)
	err = h.aof.WaitDurable(tctx(t), seq)
	if !errors.Is(err, ErrLatched) || !errors.Is(err, errBoom) {
		t.Fatalf("WaitDurable = %v", err)
	}
	if got := h.contents(seg1); !bytes.Equal(got, r1) {
		t.Fatalf("a torn tail was left in the segment: %q", got)
	}
	if _, err := h.aof.Append(recN(3)); !errors.Is(err, ErrLatched) {
		t.Fatalf("Append while latched: %v", err)
	}
	if err := h.aof.Admit(tctx(t)); !errors.Is(err, ErrLatched) {
		t.Fatalf("Admit while latched: %v", err)
	}
	if !errors.Is(h.aof.Err(), errBoom) {
		t.Fatalf("Err = %v", h.aof.Err())
	}
	if st := h.aof.Stats(); st.Written != 1 || st.Durable != 1 || st.Err == nil {
		t.Fatalf("stats %+v", st)
	}
	if err := h.aof.Close(tctx(t)); !errors.Is(err, ErrLatched) {
		t.Fatalf("Close = %v", err)
	}
	logs := h.logs.String()
	for _, want := range []string{"persistence error", "unpersisted_records=1", "records dropped"} {
		if !strings.Contains(logs, want) {
			t.Fatalf("log lacks %q:\n%s", want, logs)
		}
	}
}

func TestAOFFsyncFailureLatchesAndIsNeverRetried(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyAlways})
	var c opCounter
	var armed atomic.Bool
	h.mem.SetHook(c.hook(func(op Op) error {
		if op.Kind == OpSync && armed.Load() {
			return errBoom
		}
		return nil
	}))
	armed.Store(true)
	seq, err := h.aof.Append(recN(1))
	mustDo(t, err)
	for range 2 {
		err = h.aof.WaitDurable(tctx(t), seq)
		if !errors.Is(err, ErrLatched) || !errors.Is(err, errBoom) {
			t.Fatalf("WaitDurable = %v", err)
		}
	}
	if st := h.aof.Stats(); st.Written != 1 || st.Durable != 0 {
		t.Fatalf("stats %+v", st)
	}
	if err := h.aof.Close(tctx(t)); !errors.Is(err, ErrLatched) {
		t.Fatalf("Close = %v", err)
	}
	if got := c.syncs.Load(); got != 1 {
		t.Fatalf("fsync was attempted %d times; a failed fsync must not be retried", got)
	}
}

func TestAOFRotationBoundaryAndOrdering(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyNo})
	var oldAtList []byte
	var newExists bool
	h.setBeforeList(func(seg string) {
		img := h.mem.Crash(CrashOptions{DropDirOps: true})
		oldAtList, _ = img.Contents(seg1)
		_, newExists = img.Contents("data/" + seg)
	})
	hit, release := gateFirstWrite(t, h.mem)
	r1, r2, r3, r4 := recN(1), recN(2), recN(3), recN(4)
	if _, err := h.aof.Append(r1); err != nil {
		t.Fatal(err)
	}
	<-hit
	if _, err := h.aof.Append(r2); err != nil {
		t.Fatal(err)
	}
	rot, err := h.aof.Rotate()
	mustDo(t, err)
	if _, err := h.aof.Append(r3, r4); err != nil { // after the boundary
		t.Fatal(err)
	}
	release()
	name, err := rot.Wait(tctx(t))
	mustDo(t, err)
	if name != "seg-000002.aof" || rot.Seq() != 2 {
		t.Fatalf("rotation = %q, seq %d", name, rot.Seq())
	}
	mustDo(t, h.aof.Flush(tctx(t)))
	if got := h.contents(seg1); !bytes.Equal(got, slices.Concat(r1, r2)) {
		t.Fatalf("old segment %q", got)
	}
	if got := h.contents("data/seg-000002.aof"); !bytes.Equal(got, slices.Concat(r3, r4)) {
		t.Fatalf("new segment %q", got)
	}
	if !bytes.Equal(oldAtList, slices.Concat(r1, r2)) || !newExists {
		t.Fatal("when the manifest listed the new segment, the old one was not yet complete and durable, or the new one did not exist durably")
	}
	if !slices.Equal(h.listed(), []string{"seg-000001.aof", "seg-000002.aof"}) {
		t.Fatalf("listed %v", h.listed())
	}
	if m, found, err := LoadManifest(h.mem, "data"); err != nil || !found || !slices.Equal(m.Segments, h.listed()) {
		t.Fatalf("manifest on disk: %+v %v %v", m, found, err)
	}
	if st := h.aof.Stats(); st.Segment != "seg-000002.aof" || st.Durable < 2 {
		t.Fatalf("stats %+v", st)
	}
}

// TestAOFRotationCrashOrdering crashes the rotation at every file-system
// operation and checks the invariant that matters: whenever a post-crash
// manifest lists the new segment, the old segment holds every record before the
// boundary, under both disk models.
func TestAOFRotationCrashOrdering(t *testing.T) {
	want := slices.Concat(recN(1), recN(2))
	crashPoints := 0
	for k := 1; k <= 30; k++ {
		h := newHarness(t, harnessOpts{policy: PolicyNo})
		for i := 1; i <= 2; i++ {
			if _, err := h.aof.Append(recN(i)); err != nil {
				t.Fatal(err)
			}
		}
		mustDo(t, h.aof.Flush(tctx(t))) // written, never fsynced
		h.mem.CrashAt(h.mem.Ops() + k)
		rot, err := h.aof.Rotate()
		mustDo(t, err)
		if _, err := rot.Wait(tctx(t)); err == nil {
			break // the rotation completed: no crash points left
		} else if !errors.Is(err, ErrCrashed) {
			t.Fatalf("k=%d: %v", k, err)
		}
		crashPoints++
		for _, drop := range []bool{false, true} {
			img := h.mem.Crash(CrashOptions{DropDirOps: drop})
			m, found, err := LoadManifest(img, "data")
			if err != nil || !found {
				t.Fatalf("k=%d drop=%v: manifest unreadable: found=%v err=%v", k, drop, found, err)
			}
			if slices.Contains(m.Segments, "seg-000002.aof") {
				if got, _ := img.Contents(seg1); !bytes.Equal(got, want) {
					t.Fatalf("k=%d drop=%v: the manifest lists segment 2 but segment 1 holds %d of %d bytes",
						k, drop, len(got), len(want))
				}
			}
		}
	}
	// sync old, create new, sync new, sync dir, then the manifest commit (open, write, sync, rename, sync dir)
	if crashPoints != 9 {
		t.Fatalf("expected 9 crash points in a rotation, saw %d", crashPoints)
	}
}

func TestAOFLatchedRotationHeals(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyAlways})
	var armed atomic.Bool
	failSyncs(h.mem, &armed)
	armed.Store(true)
	seq, err := h.aof.Append(recN(1))
	mustDo(t, err)
	if err := h.aof.WaitDurable(tctx(t), seq); !errors.Is(err, ErrLatched) {
		t.Fatalf("WaitDurable = %v", err)
	}
	armed.Store(false) // the disk recovers
	listedBefore := h.listed()

	rot, err := h.aof.Rotate()
	mustDo(t, err)
	name, err := rot.Wait(tctx(t))
	mustDo(t, err)
	if name != "seg-000002.aof" {
		t.Fatalf("new segment %q", name)
	}
	if !slices.Equal(h.listed(), listedBefore) {
		t.Fatal("a healing rotation must not list the segment; the checkpoint's manifest does")
	}
	if h.aof.Err() == nil {
		t.Fatal("the latch must stay until ClearError")
	}
	if _, err := h.aof.Append(recN(9)); !errors.Is(err, ErrLatched) {
		t.Fatalf("Append before ClearError: %v", err)
	}
	mustDo(t, h.aof.ClearError(rot))
	if h.aof.Err() != nil {
		t.Fatalf("still latched: %v", h.aof.Err())
	}
	r2 := recN(2)
	seq, err = h.aof.Append(r2)
	if err != nil || seq != 2 {
		t.Fatalf("Append after recovery = %d, %v", seq, err)
	}
	mustDo(t, h.aof.WaitDurable(tctx(t), seq))
	if got, _ := h.mem.Crash(CrashOptions{DropDirOps: true}).Contents("data/seg-000002.aof"); !bytes.Equal(got, r2) {
		t.Fatalf("new segment after recovery holds %q", got)
	}
	if st := h.aof.Stats(); st.Durable != 2 || st.Err != nil {
		t.Fatalf("stats %+v", st)
	}
}

func TestAOFClearErrorRefusesRotationTakenBeforeTheFailure(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyAlways})
	var armed atomic.Bool
	failSyncs(h.mem, &armed)
	rot, err := h.aof.Rotate() // healthy rotation
	mustDo(t, err)
	_, err = rot.Wait(tctx(t))
	mustDo(t, err)
	armed.Store(true)
	seq, err := h.aof.Append(recN(1))
	mustDo(t, err)
	if err := h.aof.WaitDurable(tctx(t), seq); !errors.Is(err, ErrLatched) {
		t.Fatalf("WaitDurable = %v", err)
	}
	if err := h.aof.ClearError(rot); !errors.Is(err, ErrNotHealed) {
		t.Fatalf("ClearError = %v, want ErrNotHealed", err)
	}
	if h.aof.Err() == nil {
		t.Fatal("the latch was cleared by a rotation that predates the failure")
	}
}

func TestAOFCloseFlushesSyncsAndStops(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyEverySec})
	r := recN(1)
	seq, err := h.aof.Append(r)
	mustDo(t, err)
	mustDo(t, h.aof.Close(tctx(t)))
	if got, _ := h.mem.Crash(CrashOptions{DropDirOps: true}).Contents(seg1); !bytes.Equal(got, r) {
		t.Fatal("Close did not fsync the buffered record")
	}
	if !h.tick.stopped.Load() {
		t.Fatal("ticker not stopped")
	}
	select {
	case <-h.aof.done:
	default:
		t.Fatal("flusher still running after Close")
	}
	if _, err := h.aof.Append(r); !errors.Is(err, ErrClosed) {
		t.Fatalf("Append after Close: %v", err)
	}
	if err := h.aof.Admit(tctx(t)); !errors.Is(err, ErrClosed) {
		t.Fatalf("Admit after Close: %v", err)
	}
	if _, err := h.aof.Rotate(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Rotate after Close: %v", err)
	}
	mustDo(t, h.aof.Close(tctx(t))) // idempotent
	mustDo(t, h.aof.WaitDurable(tctx(t), seq))
}

func TestAOFConcurrentAppendsKeepPerWriterOrder(t *testing.T) {
	h := newHarness(t, harnessOpts{policy: PolicyNo})
	const writers, per = 8, 250
	seqs := make([][]uint64, writers)
	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range per {
				r := AppendRecord(nil, argv("SET", "g"+strconv.Itoa(g), strconv.Itoa(i)))
				seq, err := h.aof.Append(r)
				if err != nil {
					t.Errorf("writer %d: %v", g, err)
					return
				}
				seqs[g] = append(seqs[g], seq)
			}
		}()
	}
	wg.Wait()
	mustDo(t, h.aof.Close(tctx(t)))

	rd := NewRecordReader(bytes.NewReader(h.contents(seg1)), RecordLimits{})
	next := make([]int, writers)
	total := 0
	for {
		rec, err := rd.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		mustDo(t, err)
		g, err := strconv.Atoi(strings.TrimPrefix(string(rec[1]), "g"))
		mustDo(t, err)
		i, err := strconv.Atoi(string(rec[2]))
		mustDo(t, err)
		if i != next[g] {
			t.Fatalf("writer %d: record %d appeared where %d was expected", g, i, next[g])
		}
		next[g]++
		total++
	}
	if total != writers*per {
		t.Fatalf("%d records, want %d", total, writers*per)
	}
	seen := make(map[uint64]bool)
	for g := range writers {
		for j, s := range seqs[g] {
			if j > 0 && s <= seqs[g][j-1] {
				t.Fatalf("writer %d: sequence went %d then %d", g, seqs[g][j-1], s)
			}
			if seen[s] {
				t.Fatalf("sequence %d handed out twice", s)
			}
			seen[s] = true
		}
	}
	if len(seen) != writers*per {
		t.Fatalf("%d distinct sequences", len(seen))
	}
}

func TestOpenAOFValidationAndResume(t *testing.T) {
	m := NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	writeSynced(t, m, "data/seg-000005.aof", "OLD")
	mustDo(t, m.SyncDir("data"))
	base := AOFOptions{FS: m, Dir: "data", NextSegmentID: 1, Policy: PolicyNo}
	for name, mut := range map[string]func(*AOFOptions){
		"no fs":              func(o *AOFOptions) { o.FS = nil },
		"no dir":             func(o *AOFOptions) { o.Dir = "" },
		"bad policy":         func(o *AOFOptions) { o.Policy = 9 },
		"bad segment name":   func(o *AOFOptions) { o.ActiveSegment = "wal.log" },
		"missing segment":    func(o *AOFOptions) { o.ActiveSegment = SegmentName(3); o.NextSegmentID = 4 },
		"next id too small":  func(o *AOFOptions) { o.ActiveSegment = SegmentName(5); o.NextSegmentID = 5 },
		"first segment lost": func(o *AOFOptions) { o.OnRotate = func(string) error { return errBoom } },
	} {
		o := base
		mut(&o)
		if a, err := OpenAOF(o); err == nil {
			_ = a.Close(context.Background())
			t.Errorf("%s: accepted", name)
		}
	}

	o := base
	o.ActiveSegment, o.NextSegmentID = SegmentName(5), 6
	a, err := OpenAOF(o)
	mustDo(t, err)
	if st := a.Stats(); st.Segment != "seg-000005.aof" || st.SegmentSize != 3 {
		t.Fatalf("stats %+v", st)
	}
	r := recN(1)
	_, err = a.Append(r)
	mustDo(t, err)
	mustDo(t, a.Close(tctx(t)))
	if got, _ := m.Contents("data/seg-000005.aof"); !bytes.Equal(got, slices.Concat([]byte("OLD"), r)) {
		t.Fatalf("resumed segment holds %q", got)
	}
}

func TestParsePolicy(t *testing.T) {
	for _, p := range []Policy{PolicyAlways, PolicyEverySec, PolicyNo} {
		got, err := ParsePolicy(strings.ToUpper(p.String()))
		if err != nil || got != p {
			t.Fatalf("round trip of %v: %v %v", p, got, err)
		}
	}
	if _, err := ParsePolicy("sometimes"); err == nil {
		t.Fatal("bad policy accepted")
	}
	if PolicyEverySec != 0 {
		t.Fatal("the zero Policy must be everysec")
	}
	if got := Policy(7).String(); got != "Policy(7)" {
		t.Fatal(got)
	}
	_ = fmt.Sprint(PolicyNo)
}
