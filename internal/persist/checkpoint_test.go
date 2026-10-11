package persist

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sync"
	"testing"
)

// ckptRun is a miniature of what the Part 4 orchestrator does, built from the
// real pieces (AOF rotation, BaseWriter, CommitManifest, RetireFiles) so the
// crash matrix exercises production code. Part 4 repeats the matrix against the
// orchestrator itself.
type ckptRun struct {
	m        *MemFS
	mu       sync.Mutex
	manifest Manifest
}

func (r *ckptRun) onRotate(seg string) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.manifest.Generation++
	r.manifest.Segments = append(slices.Clone(r.manifest.Segments), seg)
	return CommitManifest(r.m, "data", r.manifest)
}

func ckptBase(m *MemFS, id uint64, entries []SnapshotEntry) (BaseRef, error) {
	var bb BlockBuffer
	for _, e := range entries {
		if err := bb.Add(e.Key, e.Value, e.DeadlineMs, e.HasDeadline); err != nil {
			return BaseRef{}, err
		}
	}
	framed, n, blocks := bb.Finish()
	bw, err := CreateBase(m, "data", id)
	if err != nil {
		return BaseRef{}, err
	}
	if err := bw.WriteFramed(framed, n, blocks); err != nil {
		bw.Abort()
		return BaseRef{}, err
	}
	return bw.Commit()
}

func k1k2() []SnapshotEntry {
	return []SnapshotEntry{
		{Key: "k1", Value: mkString("v1")},
		{Key: "k2", Value: mkString("v2"), DeadlineMs: baseMs + 60_000, HasDeadline: true},
	}
}

func k1k2k3k4() []SnapshotEntry {
	return append(k1k2(),
		SnapshotEntry{Key: "k3", Value: mkString("v3")},
		SnapshotEntry{Key: "k4", Value: mkString("v4")})
}

// ckptScenario sets up a directory whose durable state is O = {k1, k2, k3}
// (base 1 plus segment 2), then runs one checkpoint with a crash armed at the
// k-th file-system operation:
//
//	a. append k4, wait until durable
//	b. rotate (the boundary: the snapshot will be O + k4)
//	c. append k5 into the new segment, wait until durable
//	d. write base 4 = O + k4
//	e. commit the manifest {base 4, [new segment]}
//	f. delete base 1 and segment 2
//
// acked is how many of k4 and k5 were acknowledged as durable before the crash.
func ckptScenario(t *testing.T, k int) (m *MemFS, acked int, err error) {
	t.Helper()
	ctx := tctx(t)
	m = NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	boot := newNode(t)
	_, rerr := Recover(ctx, RecoverOptions{FS: m, Dir: "data", Target: boot, Replay: boot.rep})
	mustDo(t, rerr)

	run := &ckptRun{m: m}
	base1, berr := ckptBase(m, 1, k1k2())
	mustDo(t, berr)
	run.manifest = Manifest{Generation: 1, Base: base1}
	mustDo(t, CommitManifest(m, "data", run.manifest))

	a, aerr := OpenAOF(AOFOptions{
		FS: m, Dir: "data", NextSegmentID: 2, Policy: PolicyAlways, OnRotate: run.onRotate,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	mustDo(t, aerr)
	defer func() { _ = a.Close(context.Background()) }()
	appendDurable := func(r [][]byte) error {
		seq, err := a.Append(AppendRecord(nil, r))
		if err != nil {
			return err
		}
		return a.WaitDurable(ctx, seq)
	}
	mustDo(t, appendDurable(rec("SET", "k3", "v3")))

	m.CrashAt(m.Ops() + k)

	if err = appendDurable(rec("SET", "k4", "v4")); err != nil { // a
		return m, acked, err
	}
	acked = 1
	rot, err := a.Rotate() // b
	if err != nil {
		return m, acked, err
	}
	newSeg, err := rot.Wait(ctx)
	if err != nil {
		return m, acked, err
	}
	if err = appendDurable(rec("SET", "k5", "v5")); err != nil { // c
		return m, acked, err
	}
	acked = 2
	base4, err := ckptBase(m, 4, k1k2k3k4()) // d
	if err != nil {
		return m, acked, err
	}
	run.mu.Lock()
	next := Manifest{Generation: run.manifest.Generation + 1, Base: base4, Segments: []string{newSeg}}
	run.mu.Unlock()
	if err = CommitManifest(m, "data", next); err != nil { // e
		return m, acked, err
	}
	if err = RetireFiles(m, "data", base1.Name, "seg-000002.aof"); err != nil { // f
		return m, acked, err
	}
	return m, acked, nil
}

// TestCheckpointCrashMatrix crashes a checkpoint at every file-system
// operation, under four disk models (pessimistic or optimistic about directory
// entries, with or without a torn unsynced tail), and requires that the
// directory recovers without error to a consistent dataset that includes every
// acknowledged write, that it settles on exactly the files its manifest lists,
// and that recovering again changes nothing.
func TestCheckpointCrashMatrix(t *testing.T) {
	ref := newNode(t)
	mustDo(t, ref.Load("k1", mkString("v1"), 0))
	mustDo(t, ref.Load("k2", mkString("v2"), baseMs+60_000))
	ref.exec(t, rec("SET", "k3", "v3"))
	cands := [][]string{ref.dump(t)} // O
	ref.exec(t, rec("SET", "k4", "v4"))
	cands = append(cands, ref.dump(t)) // O + k4
	ref.exec(t, rec("SET", "k5", "v5"))
	cands = append(cands, ref.dump(t)) // O + k4 + k5

	models := []struct {
		name string
		opts CrashOptions
	}{
		{"optimistic", CrashOptions{}},
		{"pessimistic", CrashOptions{DropDirOps: true}},
		{"optimistic, torn tail", CrashOptions{KeepUnsynced: func(_ string, n int) int { return n / 2 }}},
		{"pessimistic, torn tail", CrashOptions{DropDirOps: true, KeepUnsynced: func(_ string, n int) int { return n / 2 }}},
	}

	check := func(t *testing.T, label string, m *MemFS, acked int) (base string) {
		t.Helper()
		for _, md := range models {
			img := m.Crash(md.opts)
			n := newNode(t)
			res, err := Recover(tctx(t), RecoverOptions{FS: img, Dir: "data", Target: n, Replay: n.rep, LoadTruncated: true})
			if err != nil {
				t.Fatalf("%s [%s]: recovery failed: %v", label, md.name, err)
			}
			got := n.dump(t)
			level := -1
			for i, c := range cands {
				if slices.Equal(got, c) {
					level = i
				}
			}
			if level < 0 {
				t.Fatalf("%s [%s]: recovered a dataset that never existed: %v", label, md.name, got)
			}
			if level < acked {
				t.Fatalf("%s [%s]: recovered state %d but %d writes were acknowledged durable", label, md.name, level, acked)
			}
			if b := res.Manifest.Base.Name; b != "base-000001.snap" && b != "base-000004.snap" {
				t.Fatalf("%s [%s]: manifest names base %q", label, md.name, b)
			}
			wantFiles := []string{"data/MANIFEST", "data/" + res.Manifest.Base.Name}
			for _, s := range res.Manifest.Segments {
				wantFiles = append(wantFiles, "data/"+s)
			}
			slices.Sort(wantFiles)
			if files := img.Names(); !slices.Equal(files, wantFiles) {
				t.Fatalf("%s [%s]: directory holds %v, want exactly %v", label, md.name, files, wantFiles)
			}
			n2 := newNode(t)
			res2, err := Recover(tctx(t), RecoverOptions{FS: img, Dir: "data", Target: n2, Replay: n2.rep, LoadTruncated: true})
			if err != nil || !slices.Equal(n2.dump(t), got) || res2.Report.TruncatedBytes != 0 || res2.Manifest.Generation != res.Manifest.Generation {
				t.Fatalf("%s [%s]: recovering again changed the outcome: %v", label, md.name, err)
			}
			base = res.Manifest.Base.Name
		}
		return base
	}

	crashPoints := 0
	for k := 1; k <= 60; k++ {
		m, acked, err := ckptScenario(t, k)
		if err == nil {
			// The checkpoint ran to completion: the new base is in use and the
			// old files are gone.
			if base := check(t, "completed", m, 2); base != "base-000004.snap" {
				t.Fatalf("completed checkpoint recovered from %s", base)
			}
			break
		}
		if !errors.Is(err, ErrCrashed) {
			t.Fatalf("k=%d: %v", k, err)
		}
		crashPoints++
		check(t, "crash at op "+string(rune('0'+k/10))+string(rune('0'+k%10)), m, acked)
	}
	if crashPoints < 20 {
		t.Fatalf("only %d crash points: the matrix is not covering the checkpoint", crashPoints)
	}
}
