package persist

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"testing"
)

var errBoom = errors.New("boom")

func writeNew(t *testing.T, fsys FS, name, data string) {
	t.Helper()
	f, err := fsys.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func writeSynced(t *testing.T, fsys FS, name, data string) {
	t.Helper()
	f, err := fsys.OpenFile(name, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	if err := f.Sync(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func slurp(t *testing.T, fsys FS, name string) string {
	t.Helper()
	b, err := ReadFile(fsys, name)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func mustDo(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func fsImpls() map[string]func(*testing.T) (FS, string) {
	return map[string]func(*testing.T) (FS, string){
		"os": func(t *testing.T) (FS, string) { return OSFS(), t.TempDir() },
		"mem": func(t *testing.T) (FS, string) {
			m := NewMemFS()
			mustDo(t, m.MkdirAll("data", 0o755))
			return m, "data"
		},
	}
}

// TestFSConformance runs the same behavior checks against the real and the
// in-memory implementation, so MemFS cannot drift from the OS.
func TestFSConformance(t *testing.T) {
	for name, mk := range fsImpls() {
		t.Run(name, func(t *testing.T) {
			fsys, dir := mk(t)
			p := func(n string) string { return filepath.Join(dir, n) }

			t.Run("create and exclusive", func(t *testing.T) {
				if _, err := fsys.OpenFile(p("missing"), os.O_RDONLY, 0); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("open missing: %v", err)
				}
				f, err := fsys.OpenFile(p("a"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
				mustDo(t, err)
				if n, err := f.Write([]byte("hello")); n != 5 || err != nil {
					t.Fatalf("write: %d %v", n, err)
				}
				if sz, err := f.Size(); sz != 5 || err != nil {
					t.Fatalf("size: %d %v", sz, err)
				}
				mustDo(t, f.Sync())
				mustDo(t, f.Close())
				if _, err := f.Write([]byte("x")); !errors.Is(err, fs.ErrClosed) {
					t.Fatalf("write after close: %v", err)
				}
				if _, err := fsys.OpenFile(p("a"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644); !errors.Is(err, fs.ErrExist) {
					t.Fatalf("exclusive create of existing file: %v", err)
				}
				if got := slurp(t, fsys, p("a")); got != "hello" {
					t.Fatalf("content %q", got)
				}
			})

			t.Run("append overwrite truncate", func(t *testing.T) {
				f, err := fsys.OpenFile(p("a"), os.O_WRONLY|os.O_APPEND, 0)
				mustDo(t, err)
				_, err = f.Write([]byte(" world"))
				mustDo(t, err)
				mustDo(t, f.Close())
				if got := slurp(t, fsys, p("a")); got != "hello world" {
					t.Fatalf("after append: %q", got)
				}
				f, err = fsys.OpenFile(p("a"), os.O_WRONLY, 0)
				mustDo(t, err)
				_, err = f.Write([]byte("J"))
				mustDo(t, err)
				mustDo(t, f.Truncate(5))
				if sz, _ := f.Size(); sz != 5 {
					t.Fatalf("size after truncate: %d", sz)
				}
				mustDo(t, f.Close())
				if got := slurp(t, fsys, p("a")); got != "Jello" {
					t.Fatalf("after overwrite+truncate: %q", got)
				}
				f, err = fsys.OpenFile(p("a"), os.O_WRONLY|os.O_TRUNC, 0)
				mustDo(t, err)
				mustDo(t, f.Close())
				if got := slurp(t, fsys, p("a")); got != "" {
					t.Fatalf("after O_TRUNC: %q", got)
				}
			})

			t.Run("rename remove readdir", func(t *testing.T) {
				writeNew(t, fsys, p("c"), "C")
				writeNew(t, fsys, p("b"), "B")
				mustDo(t, fsys.Rename(p("c"), p("b"))) // replaces b
				if got := slurp(t, fsys, p("b")); got != "C" {
					t.Fatalf("rename did not replace: %q", got)
				}
				if _, err := fsys.OpenFile(p("c"), os.O_RDONLY, 0); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("renamed-away file still there: %v", err)
				}
				if err := fsys.Rename(p("nope"), p("x")); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("rename missing: %v", err)
				}
				names, err := fsys.ReadDir(dir)
				mustDo(t, err)
				if !slices.Equal(names, []string{"a", "b"}) {
					t.Fatalf("ReadDir = %v", names)
				}
				mustDo(t, fsys.Remove(p("b")))
				if err := fsys.Remove(p("b")); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("remove missing: %v", err)
				}
				if _, err := fsys.ReadDir(p("nodir")); !errors.Is(err, fs.ErrNotExist) {
					t.Fatalf("ReadDir of missing dir: %v", err)
				}
			})

			t.Run("mkdir and syncdir", func(t *testing.T) {
				mustDo(t, fsys.MkdirAll(p("x/y"), 0o755))
				writeNew(t, fsys, p("x/y/f"), "F")
				mustDo(t, fsys.SyncDir(p("x/y")))
				mustDo(t, fsys.SyncDir(dir))
			})
		})
	}
}

func TestMemFSCrashDropsUnsyncedBytes(t *testing.T) {
	m := NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	f, err := m.OpenFile("data/f", os.O_CREATE|os.O_WRONLY, 0o644)
	mustDo(t, err)
	_, err = f.Write([]byte("abc"))
	mustDo(t, err)
	mustDo(t, f.Sync())
	_, err = f.Write([]byte("def")) // never synced
	mustDo(t, err)
	mustDo(t, m.SyncDir("data"))

	for _, tc := range []struct {
		name string
		keep func(string, int) int
		want string
	}{
		{"none", nil, "abc"},
		{"torn", func(string, int) int { return 2 }, "abcde"},
		{"clamped high", func(string, int) int { return 99 }, "abcdef"},
		{"clamped low", func(string, int) int { return -5 }, "abc"},
	} {
		post := m.Crash(CrashOptions{KeepUnsynced: tc.keep})
		if got := slurp(t, post, "data/f"); got != tc.want {
			t.Errorf("%s: got %q, want %q", tc.name, got, tc.want)
		}
	}

	// An in-place overwrite is not an appended tail: only synced bytes survive.
	g, err := m.OpenFile("data/f", os.O_WRONLY, 0)
	mustDo(t, err)
	_, err = g.Write([]byte("X"))
	mustDo(t, err)
	post := m.Crash(CrashOptions{KeepUnsynced: func(string, int) int { return 99 }})
	if got := slurp(t, post, "data/f"); got != "abc" {
		t.Errorf("overwritten file kept unsynced bytes: %q", got)
	}
}

func TestMemFSCrashDirectoryOps(t *testing.T) {
	m := NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	has := func(fsys *MemFS, name string) bool {
		_, ok := fsys.Contents(name)
		return ok
	}

	writeSynced(t, m, "data/a", "A") // data synced, directory entry not
	if has(m.Crash(CrashOptions{DropDirOps: true}), "data/a") {
		t.Fatal("unsynced create survived a pessimistic crash")
	}
	if !has(m.Crash(CrashOptions{}), "data/a") {
		t.Fatal("create lost although directory changes were kept")
	}
	mustDo(t, m.SyncDir("data"))
	if !has(m.Crash(CrashOptions{DropDirOps: true}), "data/a") {
		t.Fatal("synced create lost")
	}

	mustDo(t, m.Rename("data/a", "data/b")) // not synced
	drop, keep := m.Crash(CrashOptions{DropDirOps: true}), m.Crash(CrashOptions{})
	if !has(drop, "data/a") || has(drop, "data/b") {
		t.Fatalf("pessimistic crash after rename: %v", drop.Names())
	}
	if has(keep, "data/a") || !has(keep, "data/b") {
		t.Fatalf("optimistic crash after rename: %v", keep.Names())
	}
	mustDo(t, m.SyncDir("data"))
	if drop = m.Crash(CrashOptions{DropDirOps: true}); has(drop, "data/a") || !has(drop, "data/b") {
		t.Fatalf("synced rename: %v", drop.Names())
	}

	mustDo(t, m.Remove("data/b")) // not synced
	if !has(m.Crash(CrashOptions{DropDirOps: true}), "data/b") {
		t.Fatal("unsynced remove should be undone by a pessimistic crash")
	}
	if has(m.Crash(CrashOptions{}), "data/b") {
		t.Fatal("remove lost although directory changes were kept")
	}
}

func TestMemFSFaultInjection(t *testing.T) {
	newFS := func() *MemFS {
		m := NewMemFS()
		mustDo(t, m.MkdirAll("data", 0o755))
		return m
	}

	t.Run("op counting", func(t *testing.T) {
		m := newFS()
		var kinds []OpKind
		m.SetHook(func(op Op) error {
			kinds = append(kinds, op.Kind)
			return nil
		})
		n0 := m.Ops()
		f, err := m.OpenFile("data/x", os.O_CREATE|os.O_WRONLY, 0o644)
		mustDo(t, err)
		_, err = f.Write([]byte("v"))
		mustDo(t, err)
		mustDo(t, f.Sync())
		mustDo(t, f.Close())
		_ = slurp(t, m, "data/x")
		_, err = m.ReadDir("data")
		mustDo(t, err)
		mustDo(t, m.Rename("data/x", "data/y"))
		mustDo(t, m.SyncDir("data"))
		mustDo(t, m.Remove("data/y"))
		want := []OpKind{OpOpen, OpWrite, OpSync, OpRename, OpSyncDir, OpRemove}
		if !slices.Equal(kinds, want) || m.Ops()-n0 != len(want) {
			t.Fatalf("kinds = %v (ops %d), want %v", kinds, m.Ops()-n0, want)
		}
		if OpSync.String() != "sync" || OpKind(0).String() != "unknown" {
			t.Fatal("OpKind.String")
		}
	})

	t.Run("FailAt fails once without effect", func(t *testing.T) {
		m := newFS()
		m.FailAt(m.Ops()+1, errBoom)
		if _, err := m.OpenFile("data/x", os.O_CREATE|os.O_WRONLY, 0o644); !errors.Is(err, errBoom) {
			t.Fatalf("got %v", err)
		}
		if _, ok := m.Contents("data/x"); ok {
			t.Fatal("failed open created the file")
		}
		if _, err := m.OpenFile("data/x", os.O_CREATE|os.O_WRONLY, 0o644); err != nil {
			t.Fatalf("the next operation must work: %v", err)
		}
	})

	t.Run("failed sync leaves data volatile", func(t *testing.T) {
		m := newFS()
		m.FailAt(m.Ops()+3, errBoom) // open, write, sync
		f, err := m.OpenFile("data/x", os.O_CREATE|os.O_WRONLY, 0o644)
		mustDo(t, err)
		_, err = f.Write([]byte("abc"))
		mustDo(t, err)
		if err := f.Sync(); !errors.Is(err, errBoom) {
			t.Fatalf("sync: %v", err)
		}
		post := m.Crash(CrashOptions{})
		if got := slurp(t, post, "data/x"); got != "" {
			t.Fatalf("data of a failed sync survived: %q", got)
		}
	})

	t.Run("FailFrom is persistent", func(t *testing.T) {
		m := newFS()
		m.FailFrom(m.Ops()+1, errBoom)
		for range 3 {
			if err := m.MkdirAll("data/z", 0o755); !errors.Is(err, errBoom) {
				t.Fatalf("got %v", err)
			}
		}
	})

	t.Run("failed write has no effect", func(t *testing.T) {
		m := newFS()
		f, err := m.OpenFile("data/x", os.O_CREATE|os.O_WRONLY, 0o644)
		mustDo(t, err)
		m.FailAt(m.Ops()+1, errBoom)
		if _, err := f.Write([]byte("abc")); !errors.Is(err, errBoom) {
			t.Fatalf("write: %v", err)
		}
		if got, _ := m.Contents("data/x"); len(got) != 0 {
			t.Fatalf("content %q", got)
		}
	})

	t.Run("CrashAt halts everything", func(t *testing.T) {
		m := newFS()
		m.CrashAt(m.Ops() + 2)
		f, err := m.OpenFile("data/x", os.O_CREATE|os.O_WRONLY, 0o644)
		mustDo(t, err)
		if _, err := f.Write([]byte("abc")); !errors.Is(err, ErrCrashed) {
			t.Fatalf("write at the crash point: %v", err)
		}
		if _, err := ReadFile(m, "data/x"); !errors.Is(err, ErrCrashed) {
			t.Fatalf("read after the crash: %v", err)
		}
		if err := f.Sync(); !errors.Is(err, ErrCrashed) {
			t.Fatalf("sync after the crash: %v", err)
		}
		post := m.Crash(CrashOptions{})
		if got := slurp(t, post, "data/x"); got != "" {
			t.Fatalf("post-crash content %q", got)
		}
	})

	t.Run("Halt", func(t *testing.T) {
		m := newFS()
		m.Halt()
		if err := m.MkdirAll("data/q", 0o755); !errors.Is(err, ErrCrashed) {
			t.Fatalf("got %v", err)
		}
	})
}
