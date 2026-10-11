package persist

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/hash"
	"github.com/nima-ca/sylphy/internal/list"
	"github.com/nima-ca/sylphy/internal/set"
	"github.com/nima-ca/sylphy/internal/store"
	"github.com/nima-ca/sylphy/internal/zset"
)

const baseMs = 1_700_000_000_000 // the fake clock's start

// switchClock mirrors what Store.BeginReplay will do in Part 4: while frozen it
// reads 0, so nothing expires and no deadline deletes a key during replay.
type switchClock struct {
	inner  store.Clock
	frozen atomic.Bool
}

func (c *switchClock) NowMs() int64 {
	if c.frozen.Load() {
		return 0
	}
	return c.inner.NowMs()
}

// node is a store plus the adapter that makes it a recovery Target.
type node struct {
	st  *store.Store
	clk *store.FakeClock
	sc  *switchClock
	rep *CommandReplayer
}

func newNode(t *testing.T) *node {
	t.Helper()
	clk := store.NewFakeClock(baseMs)
	sc := &switchClock{inner: clk}
	st, err := store.NewWithOptions(store.Options{Shards: 4, Clock: sc})
	mustDo(t, err)
	reg, err := command.NewDefaultRegistry()
	mustDo(t, err)
	return &node{st: st, clk: clk, sc: sc, rep: NewCommandReplayer(reg, st, nil)}
}

func (n *node) Load(key string, v store.Value, deadlineMs int64) error {
	return n.st.MutateEntry(key, func(e *store.Entry) error {
		e.Put(v)
		if deadlineMs != 0 {
			e.SetExpireAt(deadlineMs)
		}
		return nil
	})
}

func (n *node) BeginReplay() func() {
	n.sc.frozen.Store(true)
	return func() { n.sc.frozen.Store(false) }
}

func (n *node) exec(t *testing.T, recs ...[][]byte) {
	t.Helper()
	for _, r := range recs {
		mustDo(t, n.rep.Exec(r))
	}
}

func describe(v store.Value) string {
	switch x := v.(type) {
	case *store.String:
		return fmt.Sprintf("s:%q", x.Bytes())
	case *list.List:
		items := x.Range(0, x.Len())
		strs := make([]string, len(items))
		for i, b := range items {
			strs[i] = string(b)
		}
		return fmt.Sprintf("l:%q", strs)
	case *hash.Hash:
		return fmt.Sprintf("h:%q", x.Flat())
	case *set.Set:
		m := x.Members()
		slices.Sort(m)
		return fmt.Sprintf("S:%q", m)
	case *zset.ZSet:
		var sb strings.Builder
		for _, e := range x.RangeByRank(0, x.Len(), false) {
			fmt.Fprintf(&sb, "%q@%x ", e.Member, math.Float64bits(e.Score))
		}
		return "z:" + sb.String()
	}
	return fmt.Sprintf("?%T", v)
}

// dump lists every live key with its value and deadline, sorted.
func (n *node) dump(t *testing.T) []string {
	t.Helper()
	keys := n.st.Keys("*")
	slices.Sort(keys)
	out := make([]string, 0, len(keys))
	for _, k := range keys {
		var d string
		mustDo(t, n.st.View(k, func(v store.Value) error {
			d = describe(v)
			return nil
		}))
		out = append(out, fmt.Sprintf("%q %s ttl=%d", k, d, n.st.ExpireAt(k)))
	}
	return out
}

func rec(args ...string) [][]byte { return argv(args...) }

func segBytes(recs ...[][]byte) []byte {
	var b []byte
	for _, r := range recs {
		b = AppendRecord(b, r)
	}
	return b
}

type dirFixture struct {
	t *testing.T
	m *MemFS
}

func newDir(t *testing.T) *dirFixture {
	t.Helper()
	m := NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	return &dirFixture{t: t, m: m}
}

func (d *dirFixture) put(name string, data []byte) {
	d.t.Helper()
	writeSynced(d.t, d.m, "data/"+name, string(data))
}

func (d *dirFixture) commit(m Manifest) {
	d.t.Helper()
	mustDo(d.t, CommitManifest(d.m, "data", m))
}

func (d *dirFixture) writeBase(id uint64, entries []SnapshotEntry) BaseRef {
	d.t.Helper()
	var bb BlockBuffer
	for _, e := range entries {
		mustDo(d.t, bb.Add(e.Key, e.Value, e.DeadlineMs, e.HasDeadline))
	}
	framed, n, blocks := bb.Finish()
	bw, err := CreateBase(d.m, "data", id)
	mustDo(d.t, err)
	mustDo(d.t, bw.WriteFramed(framed, n, blocks))
	ref, err := bw.Commit()
	mustDo(d.t, err)
	return ref
}

func (d *dirFixture) recoverInto(n *node, loadTruncated bool) (RecoverResult, error) {
	return Recover(context.Background(), RecoverOptions{
		FS: d.m, Dir: "data", Target: n, Replay: n.rep, LoadTruncated: loadTruncated,
	})
}

func (d *dirFixture) mustRecover(n *node) RecoverResult {
	d.t.Helper()
	res, err := d.recoverInto(n, true)
	mustDo(d.t, err)
	return res
}

func asCorrupt(t *testing.T, err error) *CorruptError {
	t.Helper()
	var ce *CorruptError
	if !errors.As(err, &ce) {
		t.Fatalf("err = %v (%T), want *CorruptError", err, err)
	}
	return ce
}

func TestRecoverFreshDirectory(t *testing.T) {
	d := newDir(t)
	n := newNode(t)
	res := d.mustRecover(n)
	if got := n.dump(t); len(got) != 0 {
		t.Fatalf("fresh start holds %v", got)
	}
	if res.NextID != 1 || res.ActiveSegment != "" || res.Manifest.Generation != 0 {
		t.Fatalf("result %+v", res)
	}
	if names := d.m.Names(); !slices.Equal(names, []string{"data/MANIFEST"}) {
		t.Fatalf("files: %v", names)
	}
	img := d.m.Crash(CrashOptions{DropDirOps: true})
	if _, found, err := LoadManifest(img, "data"); !found || err != nil {
		t.Fatalf("the empty manifest is not durable: found=%v err=%v", found, err)
	}
	if n.sc.frozen.Load() {
		t.Fatal("the clock was left frozen")
	}
	// A second start loads the manifest it committed.
	res2 := d.mustRecover(newNode(t))
	if res2.NextID != 1 || res2.Manifest.Generation != 0 {
		t.Fatalf("second start: %+v", res2)
	}
}

func TestRecoverBaseOnly(t *testing.T) { // snapshot mode: a base and no segments
	d := newDir(t)
	ref := d.writeBase(1, sampleEntries())
	d.commit(Manifest{Generation: 1, Base: ref})

	want := newNode(t)
	for _, e := range sampleEntries() {
		var dl int64
		if e.HasDeadline {
			dl = e.DeadlineMs
		}
		mustDo(t, want.Load(e.Key, e.Value, dl))
	}
	n := newNode(t)
	res := d.mustRecover(n)
	if got, exp := n.dump(t), want.dump(t); !slices.Equal(got, exp) || len(got) != len(sampleEntries()) {
		t.Fatalf("recovered\n%v\nwant\n%v", got, exp)
	}
	if res.Report.BaseKeys != len(sampleEntries()) || res.Report.Segments != 0 || res.ActiveSegment != "" || res.NextID != 2 {
		t.Fatalf("result %+v", res)
	}
	if n.sc.frozen.Load() {
		t.Fatal("the clock was left frozen")
	}
}

func TestRecoverBaseAndSegments(t *testing.T) {
	recs := [][][]byte{
		rec("SET", "str", "changed"),
		rec("RPUSH", "list", "tail"),
		rec("HSET", "hash", "f2", "v2"),
		rec("SADD", "set", "m3"),
		rec("ZADD", "zset", "5", "z5"),
		rec("PEXPIREAT", "str:ttl", strconv.Itoa(baseMs+60_000)),
		rec("DEL", "\x00\r\n\xff"),
		rec("INCR", "counter"),
		rec("SET", "n", "5"),
		rec("INCRBY", "n", "10"),
		rec("APPEND", "n", "x"),
	}
	d := newDir(t)
	ref := d.writeBase(1, sampleEntries())
	d.put("seg-000002.aof", segBytes(recs...))
	d.commit(Manifest{Generation: 2, Base: ref, Segments: []string{"seg-000002.aof"}})

	want := newNode(t)
	for _, e := range sampleEntries() {
		var dl int64
		if e.HasDeadline {
			dl = e.DeadlineMs
		}
		mustDo(t, want.Load(e.Key, e.Value, dl))
	}
	want.exec(t, recs...)

	n := newNode(t)
	res := d.mustRecover(n)
	if got, exp := n.dump(t), want.dump(t); !slices.Equal(got, exp) {
		t.Fatalf("recovered\n%v\nwant\n%v", got, exp)
	}
	if res.Report.Records != len(recs) || res.Report.Segments != 1 || res.ActiveSegment != "seg-000002.aof" || res.NextID != 3 {
		t.Fatalf("result %+v", res)
	}
}

func TestRecoverSegmentsReplayInOrder(t *testing.T) {
	d := newDir(t)
	d.put("seg-000001.aof", segBytes(rec("SET", "k", "first"), rec("RPUSH", "l", "a")))
	d.put("seg-000002.aof", segBytes(rec("SET", "k", "second"), rec("RPUSH", "l", "b")))
	d.put("seg-000005.aof", segBytes(rec("SET", "k", "third")))
	d.commit(Manifest{Generation: 3, Segments: []string{"seg-000001.aof", "seg-000002.aof", "seg-000005.aof"}})
	n := newNode(t)
	res := d.mustRecover(n)
	want := []string{
		fmt.Sprintf("%q %s ttl=-1", "k", `s:"third"`),
		fmt.Sprintf("%q %s ttl=-1", "l", `l:["a" "b"]`),
	}
	if got := n.dump(t); !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if res.ActiveSegment != "seg-000005.aof" || res.NextID != 6 || res.Report.Segments != 3 {
		t.Fatalf("result %+v", res)
	}
}

// TestRecoverExpiredWhileDown is the resurrection scenario: a key whose
// deadline passed while the server was down must stay gone even though later
// log records touch it. With a live clock the base load would delete it and the
// INCR would then create a fresh key without a TTL.
func TestRecoverExpiredWhileDown(t *testing.T) {
	d := newDir(t)
	ref := d.writeBase(1, []SnapshotEntry{
		{Key: "z", Value: mkString("1"), DeadlineMs: baseMs + 5_000, HasDeadline: true},
		{Key: "keep", Value: mkString("k")},
		{Key: "late", Value: mkString("L"), DeadlineMs: baseMs + 3_600_000, HasDeadline: true},
	})
	d.put("seg-000002.aof", segBytes(
		rec("INCR", "z"),
		rec("APPEND", "z", "x"),
		rec("SET", "w", "1", "PXAT", strconv.Itoa(baseMs+2_000)),
		rec("APPEND", "w", "y"),
		rec("DEL", "ghost"),
	))
	d.commit(Manifest{Generation: 2, Base: ref, Segments: []string{"seg-000002.aof"}})

	n := newNode(t)
	n.clk.Set(baseMs + 10_000) // ten seconds of downtime
	res := d.mustRecover(n)
	want := []string{
		fmt.Sprintf("%q %s ttl=-1", "keep", `s:"k"`),
		fmt.Sprintf("%q %s ttl=%d", "late", `s:"L"`, baseMs+3_600_000),
	}
	if got := n.dump(t); !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	if res.Report.Records != 5 || res.Report.BaseKeys != 3 {
		t.Fatalf("report %+v", res.Report)
	}
}

func TestRecoverTruncatedTailAtEveryCut(t *testing.T) {
	recs := [][][]byte{rec("SET", "a", "1"), rec("RPUSH", "l", "x", "y"), rec("INCR", "a")}
	var seg []byte
	bounds := []int{0}
	for _, r := range recs {
		seg = AppendRecord(seg, r)
		bounds = append(bounds, len(seg))
	}
	for cut := 0; cut <= len(seg); cut++ {
		d := newDir(t)
		d.put("seg-000002.aof", seg[:cut])
		d.commit(Manifest{Generation: 1, Segments: []string{"seg-000002.aof"}})

		complete := 0
		for _, b := range bounds[1:] {
			if b <= cut {
				complete++
			}
		}
		want := newNode(t)
		want.exec(t, recs[:complete]...)

		n := newNode(t)
		res, err := d.recoverInto(n, true)
		if err != nil {
			t.Fatalf("cut %d: %v", cut, err)
		}
		if got, exp := n.dump(t), want.dump(t); !slices.Equal(got, exp) {
			t.Fatalf("cut %d: got %v, want %v", cut, got, exp)
		}
		if res.Report.Records != complete || res.Report.TruncatedBytes != int64(cut-bounds[complete]) {
			t.Fatalf("cut %d: report %+v (complete %d)", cut, res.Report, complete)
		}
		if got, _ := d.m.Contents("data/seg-000002.aof"); !slices.Equal(got, seg[:bounds[complete]]) {
			t.Fatalf("cut %d: the segment was not repaired to its last complete record", cut)
		}
		// Idempotent: the repaired directory recovers again with nothing to repair.
		again, err := d.recoverInto(newNode(t), true)
		if err != nil || again.Report.TruncatedBytes != 0 || again.Report.Records != complete {
			t.Fatalf("cut %d: second recovery: %+v %v", cut, again.Report, err)
		}
	}
}

func TestRecoverHardErrors(t *testing.T) {
	r1, r2, r3 := rec("SET", "a", "1"), rec("RPUSH", "l", "x"), rec("SET", "c", "3")
	full12 := segBytes(r1, r2)
	partial := AppendRecord(nil, r3)[:5]

	tests := []struct {
		name          string
		segs          map[string][]byte
		list          []string
		loadTruncated bool
		file          string
		offset        int64
		is            error
		contains      string
	}{
		{
			name: "partial tail with aof-load-truncated off", loadTruncated: false,
			segs: map[string][]byte{"seg-000002.aof": slices.Concat(full12, partial)}, list: []string{"seg-000002.aof"},
			file: "seg-000002.aof", offset: int64(len(full12)), is: ErrTruncated, contains: "aof-load-truncated",
		},
		{
			name: "partial record in a segment that is not the last", loadTruncated: true,
			segs: map[string][]byte{
				"seg-000002.aof": slices.Concat(full12, partial),
				"seg-000003.aof": segBytes(r3),
			},
			list: []string{"seg-000002.aof", "seg-000003.aof"},
			file: "seg-000002.aof", offset: int64(len(full12)), is: ErrTruncated,
		},
		{
			name: "garbage in the middle", loadTruncated: true,
			segs: map[string][]byte{"seg-000002.aof": slices.Concat(segBytes(r1), []byte("x"), segBytes(r3))},
			list: []string{"seg-000002.aof"},
			file: "seg-000002.aof", offset: int64(len(segBytes(r1))), is: ErrCorrupt, contains: "does not start",
		},
		{
			name: "zero-filled tail", loadTruncated: true,
			segs: map[string][]byte{"seg-000002.aof": slices.Concat(segBytes(r1), make([]byte, 16))},
			list: []string{"seg-000002.aof"},
			file: "seg-000002.aof", offset: int64(len(segBytes(r1))), is: ErrCorrupt,
		},
		{
			name: "bad bulk marker", loadTruncated: true,
			segs: map[string][]byte{"seg-000002.aof": slices.Concat(segBytes(r1), []byte("*1\r\n:5\r\n"))},
			list: []string{"seg-000002.aof"},
			file: "seg-000002.aof", offset: int64(len(segBytes(r1)) + 4), is: ErrCorrupt, contains: "expected '$'",
		},
		{
			name: "command fails on replay", loadTruncated: true,
			segs: map[string][]byte{"seg-000002.aof": segBytes(rec("SET", "s", "abc"), rec("INCR", "s"))},
			list: []string{"seg-000002.aof"},
			file: "seg-000002.aof", offset: int64(len(segBytes(rec("SET", "s", "abc")))), is: ErrReplay, contains: "INCR",
		},
		{
			name: "unknown command", loadTruncated: true,
			segs: map[string][]byte{"seg-000002.aof": segBytes(rec("NOSUCHCOMMAND", "x"))},
			list: []string{"seg-000002.aof"},
			file: "seg-000002.aof", offset: 0, is: ErrCorrupt, contains: "NOSUCHCOMMAND",
		},
		{
			name: "listed segment missing", loadTruncated: true,
			segs: map[string][]byte{"seg-000002.aof": segBytes(r1)}, list: []string{"seg-000002.aof", "seg-000003.aof"},
			file: "seg-000003.aof", offset: -1, is: ErrCorrupt, contains: "missing",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			d := newDir(t)
			for name, data := range tc.segs {
				d.put(name, data)
			}
			d.put("seg-000099.aof", []byte("stale")) // garbage that must survive a failed start
			d.commit(Manifest{Generation: 1, Segments: tc.list})
			before := make(map[string][]byte)
			for _, name := range d.m.Names() {
				before[name], _ = d.m.Contents(name)
			}

			_, err := d.recoverInto(newNode(t), tc.loadTruncated)
			ce := asCorrupt(t, err)
			if ce.File != tc.file || ce.Offset != tc.offset || !errors.Is(err, tc.is) {
				t.Fatalf("got file=%q offset=%d err=%v; want file=%q offset=%d is %v", ce.File, ce.Offset, err, tc.file, tc.offset, tc.is)
			}
			if !strings.Contains(err.Error(), tc.file) || !strings.Contains(err.Error(), tc.contains) {
				t.Fatalf("message %q should name the file and mention %q", err, tc.contains)
			}
			if tc.offset >= 0 && !strings.Contains(err.Error(), strconv.FormatInt(tc.offset, 10)) {
				t.Fatalf("message %q should name offset %d", err, tc.offset)
			}
			for name, data := range before { // a failed start changes nothing
				if now, ok := d.m.Contents(name); !ok || !slices.Equal(now, data) {
					t.Fatalf("%s was modified or removed by a failed recovery", name)
				}
			}
		})
	}
}

func TestRecoverBaseErrors(t *testing.T) {
	setup := func(t *testing.T) (*dirFixture, BaseRef) {
		d := newDir(t)
		ref := d.writeBase(1, sampleEntries())
		d.commit(Manifest{Generation: 1, Base: ref})
		return d, ref
	}

	t.Run("flipped byte in a block", func(t *testing.T) {
		d, _ := setup(t)
		b, _ := d.m.Contents("data/base-000001.snap")
		b[snapHeaderLen+blockHeaderLen+3] ^= 0xFF
		d.put("base-000001.snap", b)
		_, err := d.recoverInto(newNode(t), true)
		ce := asCorrupt(t, err)
		if ce.File != "base-000001.snap" || ce.Offset != snapHeaderLen || !errors.Is(err, ErrChecksum) {
			t.Fatalf("got %+v / %v", ce, err)
		}
	})
	t.Run("truncated file", func(t *testing.T) {
		d, ref := setup(t)
		b, _ := d.m.Contents("data/base-000001.snap")
		d.put("base-000001.snap", b[:len(b)/2])
		_, err := d.recoverInto(newNode(t), true)
		if ce := asCorrupt(t, err); ce.File != "base-000001.snap" || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("got %v", err)
		}
		_ = ref
	})
	t.Run("truncated stream under a matching manifest size", func(t *testing.T) {
		d, ref := setup(t)
		b, _ := d.m.Contents("data/base-000001.snap")
		d.put("base-000001.snap", b[:len(b)/2])
		d.commit(Manifest{Generation: 2, Base: BaseRef{Name: ref.Name, Size: int64(len(b) / 2), CRC: ref.CRC}})
		_, err := d.recoverInto(newNode(t), true)
		if ce := asCorrupt(t, err); ce.File != "base-000001.snap" || !errors.Is(err, ErrTruncated) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("checksum differs from the manifest", func(t *testing.T) {
		d, ref := setup(t)
		d.commit(Manifest{Generation: 2, Base: BaseRef{Name: ref.Name, Size: ref.Size, CRC: ref.CRC ^ 1}})
		_, err := d.recoverInto(newNode(t), true)
		if ce := asCorrupt(t, err); ce.File != "base-000001.snap" || !errors.Is(err, ErrChecksum) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("missing", func(t *testing.T) {
		d, _ := setup(t)
		mustDo(t, d.m.Remove("data/base-000001.snap"))
		_, err := d.recoverInto(newNode(t), true)
		if ce := asCorrupt(t, err); ce.File != "base-000001.snap" || !strings.Contains(err.Error(), "missing") {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("unknown snapshot version", func(t *testing.T) {
		d, ref := setup(t)
		b := fileOf(rawHeader(2, 0, 0), nil, 0, 0)
		d.put("base-000001.snap", b)
		d.commit(Manifest{Generation: 2, Base: BaseRef{Name: ref.Name, Size: int64(len(b)), CRC: ref.CRC}})
		_, err := d.recoverInto(newNode(t), true)
		if !errors.Is(err, ErrVersion) {
			t.Fatalf("got %v", err)
		}
	})
}

func TestRecoverManifestErrors(t *testing.T) {
	t.Run("damaged manifest is not a fresh start", func(t *testing.T) {
		d := newDir(t)
		d.put("MANIFEST", []byte("garbage\n"))
		_, err := d.recoverInto(newNode(t), true)
		if !errors.Is(err, ErrCorrupt) || !strings.Contains(err.Error(), "MANIFEST") {
			t.Fatalf("got %v", err)
		}
		if got, _ := d.m.Contents("data/MANIFEST"); string(got) != "garbage\n" {
			t.Fatal("the damaged manifest was overwritten")
		}
	})
	t.Run("future manifest version", func(t *testing.T) {
		d := newDir(t)
		d.put("MANIFEST", withCRC("SYLPHY-MANIFEST 2\ngeneration 1\n"))
		if _, err := d.recoverInto(newNode(t), true); !errors.Is(err, ErrVersion) {
			t.Fatalf("got %v", err)
		}
	})
	t.Run("data files but no manifest", func(t *testing.T) {
		d := newDir(t)
		d.put("seg-000001.aof", segBytes(rec("SET", "a", "1")))
		d.put("base-000002.snap", []byte("x"))
		_, err := d.recoverInto(newNode(t), true)
		ce := asCorrupt(t, err)
		if ce.File != "data" || !strings.Contains(err.Error(), "seg-000001.aof") || !strings.Contains(err.Error(), "MANIFEST") {
			t.Fatalf("got %v", err)
		}
		if _, ok := d.m.Contents("data/seg-000001.aof"); !ok {
			t.Fatal("the segment was deleted")
		}
	})
	t.Run("only a leftover manifest temp file", func(t *testing.T) {
		d := newDir(t)
		d.put("MANIFEST.tmp", []byte("half"))
		res := d.mustRecover(newNode(t))
		if res.Manifest.Generation != 0 || !slices.Equal(d.m.Names(), []string{"data/MANIFEST"}) {
			t.Fatalf("result %+v, files %v", res, d.m.Names())
		}
	})
}

func TestRecoverRemovesGarbage(t *testing.T) {
	d := newDir(t)
	ref := d.writeBase(1, []SnapshotEntry{{Key: "k", Value: mkString("v")}})
	d.put("seg-000002.aof", segBytes(rec("SET", "a", "1")))
	d.put("MANIFEST.tmp", []byte("x"))
	d.put("base-000009.snap.tmp", []byte("x"))
	d.put("base-000007.snap", []byte("old base"))
	d.put("seg-000008.aof", []byte("old segment"))
	d.put("notes.txt", []byte("not ours"))
	d.commit(Manifest{Generation: 2, Base: ref, Segments: []string{"seg-000002.aof"}})
	d.put("MANIFEST.tmp", []byte("x")) // CommitManifest consumed the first one

	n := newNode(t)
	res := d.mustRecover(n)
	want := []string{"data/MANIFEST", "data/base-000001.snap", "data/notes.txt", "data/seg-000002.aof"}
	if got := d.m.Names(); !slices.Equal(got, want) {
		t.Fatalf("files after recovery: %v, want %v", got, want)
	}
	if res.NextID != 10 { // the stale ids 7, 8 and the temp 9 are never reused
		t.Fatalf("NextID = %d, want 10", res.NextID)
	}
	img := d.m.Crash(CrashOptions{DropDirOps: true})
	if got := img.Names(); !slices.Equal(got, want) {
		t.Fatalf("the cleanup is not durable: %v", got)
	}
}

func TestRecoverHonorsContextAndOptions(t *testing.T) {
	d := newDir(t)
	n := newNode(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := Recover(ctx, RecoverOptions{FS: d.m, Dir: "data", Target: n, Replay: n.rep}); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
	if _, err := Recover(context.Background(), RecoverOptions{Dir: "data"}); err == nil {
		t.Fatal("missing options accepted")
	}

	// A cancelled context also stops a long replay.
	var seg []byte
	for i := range 600 {
		seg = AppendRecord(seg, rec("SET", "k"+strconv.Itoa(i), "v"))
	}
	d.put("seg-000001.aof", seg)
	d.commit(Manifest{Generation: 1, Segments: []string{"seg-000001.aof"}})
	ctx2, cancel2 := context.WithCancel(context.Background())
	counting := &cancelAfter{inner: n.rep, after: 300, cancel: cancel2}
	_, err := Recover(ctx2, RecoverOptions{FS: d.m, Dir: "data", Target: n, Replay: counting})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("replay was not interrupted: %v", err)
	}
	if n.sc.frozen.Load() {
		t.Fatal("the clock was left frozen after an aborted recovery")
	}
}

type cancelAfter struct {
	inner  Replayer
	after  int
	n      int
	cancel context.CancelFunc
}

func (c *cancelAfter) Exec(argv [][]byte) error {
	c.n++
	if c.n == c.after {
		c.cancel()
	}
	return c.inner.Exec(argv)
}

func TestCommandReplayer(t *testing.T) {
	n := newNode(t)
	mustDo(t, n.rep.Exec(rec("SET", "k", "v")))
	if v, ok := n.st.Get("k"); !ok || string(v) != "v" {
		t.Fatalf("SET not applied: %q %v", v, ok)
	}
	err := n.rep.Exec(rec("INCR", "k"))
	if !errors.Is(err, ErrReplay) || !strings.Contains(err.Error(), "not an integer") {
		t.Fatalf("INCR on a string: %v", err)
	}
	if err := n.rep.Exec(rec("NOSUCH")); !errors.Is(err, ErrReplay) || !strings.Contains(err.Error(), "unknown command") {
		t.Fatalf("unknown command: %v", err)
	}
	if err := n.rep.Exec(rec("GET", "k")); err != nil { // replies that are not errors are discarded
		t.Fatal(err)
	}
	if err := n.rep.Exec(nil); err != nil {
		t.Fatalf("empty argv: %v", err)
	}
}

func TestBaseWriter(t *testing.T) {
	t.Run("commit", func(t *testing.T) {
		d := newDir(t)
		ref := d.writeBase(7, sampleEntries())
		if ref.Name != "base-000007.snap" {
			t.Fatalf("ref %+v", ref)
		}
		if names := d.m.Names(); !slices.Equal(names, []string{"data/base-000007.snap"}) {
			t.Fatalf("files: %v", names)
		}
		img := d.m.Crash(CrashOptions{DropDirOps: true}) // the rename must be durable before a manifest names it
		b, ok := img.Contents("data/base-000007.snap")
		if !ok || int64(len(b)) != ref.Size {
			t.Fatalf("post-crash base: %d bytes (ok=%v), want %d", len(b), ok, ref.Size)
		}
	})
	t.Run("abort", func(t *testing.T) {
		d := newDir(t)
		bw, err := CreateBase(d.m, "data", 3)
		mustDo(t, err)
		if names := d.m.Names(); !slices.Equal(names, []string{"data/base-000003.snap.tmp"}) {
			t.Fatalf("files while writing: %v", names)
		}
		bw.Abort()
		bw.Abort()
		if names := d.m.Names(); len(names) != 0 {
			t.Fatalf("files after Abort: %v", names)
		}
		if _, err := bw.Commit(); err == nil {
			t.Fatal("Commit after Abort succeeded")
		}
	})
	t.Run("every failing operation leaves no temp file", func(t *testing.T) {
		var bb BlockBuffer
		mustDo(t, bb.Add("k", mkString("v"), 0, false))
		framed, n, blocks := bb.Finish()
		for k := 1; k <= 12; k++ {
			d := newDir(t)
			d.m.FailAt(d.m.Ops()+k, errBoom)
			var err error
			bw, cerr := CreateBase(d.m, "data", 1)
			if cerr == nil {
				if err = bw.WriteFramed(framed, n, blocks); err == nil {
					_, err = bw.Commit()
				}
				if err != nil {
					bw.Abort()
				}
			} else {
				err = cerr
			}
			if err == nil {
				break // past the last operation
			}
			if !errors.Is(err, errBoom) {
				t.Fatalf("k=%d: %v", k, err)
			}
			for _, name := range d.m.Names() {
				if strings.HasSuffix(name, TmpSuffix) || strings.HasSuffix(name, ".snap") {
					t.Fatalf("k=%d: %s left behind", k, name)
				}
			}
		}
	})
	t.Run("RetireFiles", func(t *testing.T) {
		d := newDir(t)
		d.put("a", []byte("1"))
		d.put("b", []byte("2"))
		mustDo(t, RetireFiles(d.m, "data", "a", "", "missing"))
		if names := d.m.Names(); !slices.Equal(names, []string{"data/b"}) {
			t.Fatalf("files: %v", names)
		}
		if !slices.Equal(d.m.Crash(CrashOptions{DropDirOps: true}).Names(), []string{"data/b"}) {
			t.Fatal("the removal is not durable")
		}
		d.m.FailFrom(d.m.Ops()+1, errBoom)
		if err := RetireFiles(d.m, "data", "b"); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
	})
}
