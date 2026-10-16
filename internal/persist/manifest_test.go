package persist

import (
	"errors"
	"fmt"
	"hash/crc32"
	"slices"
	"testing"
)

func manifestEqual(a, b Manifest) bool {
	return a.Generation == b.Generation && a.Base == b.Base && slices.Equal(a.Segments, b.Segments)
}

func withCRC(body string) []byte {
	return []byte(body + fmt.Sprintf("crc %08x\n", crc32.Checksum([]byte(body), castagnoli)))
}

func TestManifestRoundTrip(t *testing.T) {
	tests := map[string]Manifest{
		"empty":         {Generation: 0},
		"segments only": {Generation: 1, Segments: []string{SegmentName(1), SegmentName(2)}},
		"snapshot only": {Generation: 9, Base: BaseRef{Name: BaseName(3), Size: 1 << 40, CRC: 0xdeadbeef}},
		"full": {
			Generation: 1<<64 - 1,
			Base:       BaseRef{Name: BaseName(1234567), Size: 42, CRC: 0},
			Segments:   []string{SegmentName(4), SegmentName(5), SegmentName(1000000)},
		},
	}
	for name, m := range tests {
		t.Run(name, func(t *testing.T) {
			enc := EncodeManifest(m)
			got, err := DecodeManifest(enc)
			if err != nil {
				t.Fatal(err)
			}
			if !manifestEqual(got, m) {
				t.Fatalf("got %+v, want %+v", got, m)
			}
		})
	}
}

func TestManifestNames(t *testing.T) {
	if id, ok := ParseBaseName(BaseName(7)); !ok || id != 7 {
		t.Fatal("base name round trip")
	}
	if id, ok := ParseSegmentName(SegmentName(12345678)); !ok || id != 12345678 {
		t.Fatal("segment name round trip")
	}
	for _, bad := range []string{
		"", "base-.snap", "base-1.snap", "base-0000001.snap", "base-000001.aof", "seg-000001.snap",
		"../base-000001.snap", "base-000001.snap/", "base-00000a.snap", "base-+00001.snap", "snap",
		"base-99999999999999999999.snap",
	} {
		if _, ok := ParseBaseName(bad); ok {
			t.Errorf("ParseBaseName(%q) accepted", bad)
		}
	}
	if _, ok := ParseSegmentName("seg-000001.aof "); ok {
		t.Error("trailing space accepted")
	}
}

func TestDecodeManifestErrors(t *testing.T) {
	const head = "SYLPHY-MANIFEST 1\ngeneration 1\n"
	bad := []struct {
		name string
		body string
		is   error
	}{
		{"version 2", "SYLPHY-MANIFEST 2\ngeneration 1\n", ErrVersion},
		{"bad magic", "NOPE 1\ngeneration 1\n", ErrCorrupt},
		{"empty body", "", ErrCorrupt},
		{"no generation", "SYLPHY-MANIFEST 1\nsegment seg-000001.aof\n", ErrCorrupt},
		{"unknown line", head + "wat\n", ErrCorrupt},
		{"blank line", head + "\n", ErrCorrupt},
		{"base after segment", head + "segment seg-000001.aof\nbase base-000001.snap 5 00000000\n", ErrCorrupt},
		{"two bases", head + "base base-000001.snap 5 00000000\nbase base-000002.snap 5 00000000\n", ErrCorrupt},
		{"unsorted segments", head + "segment seg-000002.aof\nsegment seg-000001.aof\n", ErrCorrupt},
		{"duplicate segment", head + "segment seg-000002.aof\nsegment seg-000002.aof\n", ErrCorrupt},
		{"path traversal", head + "base ../evil 5 00000000\n", ErrCorrupt},
		{"bad segment name", head + "segment seg-1.aof\n", ErrCorrupt},
		{"leading zero generation", "SYLPHY-MANIFEST 1\ngeneration 007\n", ErrCorrupt},
		{"upper-case crc", head + "base base-000001.snap 5 0000ABCD\n", ErrCorrupt},
		{"extra field", head + "base base-000001.snap 5 00000000 x\n", ErrCorrupt},
		{"negative size", head + "base base-000001.snap -5 00000000\n", ErrCorrupt},
	}
	for _, tc := range bad {
		t.Run(tc.name, func(t *testing.T) {
			_, err := DecodeManifest(withCRC(tc.body))
			if !errors.Is(err, tc.is) {
				t.Fatalf("err = %v, want %v", err, tc.is)
			}
		})
	}
}

func TestDecodeManifestDetectsDamage(t *testing.T) {
	enc := EncodeManifest(Manifest{
		Generation: 5,
		Base:       BaseRef{Name: BaseName(2), Size: 100, CRC: 0x12345678},
		Segments:   []string{SegmentName(3), SegmentName(4)},
	})
	if _, err := DecodeManifest(enc); err != nil {
		t.Fatal(err)
	}
	for cut := range len(enc) {
		if _, err := DecodeManifest(enc[:cut]); err == nil {
			t.Fatalf("truncation to %d bytes accepted", cut)
		}
	}
	for i := range enc {
		for _, mask := range []byte{0x01, 0x80, 0xFF} {
			bad := slices.Clone(enc)
			bad[i] ^= mask
			if _, err := DecodeManifest(bad); err == nil {
				t.Fatalf("byte %d changed by %#x accepted", i, mask)
			}
		}
	}
	if _, err := DecodeManifest(append(slices.Clone(enc), '\n')); err == nil {
		t.Fatal("trailing newline accepted")
	}
	if _, err := DecodeManifest(enc[:len(enc)-1]); !errors.Is(err, ErrTruncated) {
		t.Fatalf("missing final newline: %v", err)
	}
}

func TestLoadAndCommitManifest(t *testing.T) {
	m := NewMemFS()
	mustDo(t, m.MkdirAll("data", 0o755))
	if _, found, err := LoadManifest(m, "data"); found || err != nil {
		t.Fatalf("fresh directory: found=%v err=%v", found, err)
	}
	if _, found, err := LoadManifest(m, "nodir"); found || err != nil {
		t.Fatalf("missing directory: found=%v err=%v", found, err)
	}
	one := Manifest{Generation: 1, Segments: []string{SegmentName(1)}}
	two := Manifest{Generation: 2, Base: BaseRef{Name: BaseName(2), Size: 1, CRC: 1}}
	mustDo(t, CommitManifest(m, "data", one))
	got, found, err := LoadManifest(m, "data")
	if err != nil || !found || !manifestEqual(got, one) {
		t.Fatalf("got %+v %v %v", got, found, err)
	}
	mustDo(t, CommitManifest(m, "data", two))
	if got, _, _ = LoadManifest(m, "data"); !manifestEqual(got, two) {
		t.Fatalf("second commit: %+v", got)
	}
	if names := m.Names(); !slices.Equal(names, []string{"data/MANIFEST"}) {
		t.Fatalf("leftover files: %v", names)
	}
	if err := CommitManifest(m, "data", Manifest{Segments: []string{"bogus"}}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("invalid manifest committed: %v", err)
	}

	// A damaged manifest is an error, not "not found".
	writeSynced(t, m, "data/MANIFEST", "garbage\n")
	if _, found, err := LoadManifest(m, "data"); found || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("damaged manifest: found=%v err=%v", found, err)
	}
}

func TestCommitManifestFailureLeavesOldManifest(t *testing.T) {
	old := Manifest{Generation: 1}
	next := Manifest{Generation: 2}
	for _, failAt := range []int{1, 2, 3, 4} { // open, write, sync, rename
		m := NewMemFS()
		mustDo(t, m.MkdirAll("data", 0o755))
		mustDo(t, CommitManifest(m, "data", old))
		m.FailAt(m.Ops()+failAt, errBoom)
		if err := CommitManifest(m, "data", next); !errors.Is(err, errBoom) {
			t.Fatalf("failAt %d: %v", failAt, err)
		}
		got, found, err := LoadManifest(m, "data")
		if err != nil || !found || !manifestEqual(got, old) {
			t.Fatalf("failAt %d: manifest is %+v %v %v", failAt, got, found, err)
		}
		if names := m.Names(); !slices.Equal(names, []string{"data/MANIFEST"}) {
			t.Fatalf("failAt %d: leftovers %v", failAt, names)
		}
	}
}

// TestCommitManifestCrashMatrix crashes the commit at every step, under both
// pessimistic and optimistic disk models, and requires the directory to hold
// exactly the old or exactly the new manifest.
func TestCommitManifestCrashMatrix(t *testing.T) {
	oldM := Manifest{Generation: 1, Base: BaseRef{Name: BaseName(1), Size: 10, CRC: 0xdeadbeef}, Segments: []string{SegmentName(2)}}
	newM := Manifest{Generation: 2, Base: BaseRef{Name: BaseName(3), Size: 20, CRC: 0x01020304}, Segments: []string{SegmentName(4)}}
	crashPoints := 0
	for k := 1; k <= 20; k++ {
		m := NewMemFS()
		mustDo(t, m.MkdirAll("data", 0o755))
		mustDo(t, CommitManifest(m, "data", oldM))
		m.CrashAt(m.Ops() + k)
		err := CommitManifest(m, "data", newM)
		if err == nil {
			break // ran to completion: no crash points left
		}
		if !errors.Is(err, ErrCrashed) {
			t.Fatalf("k=%d: %v", k, err)
		}
		crashPoints++
		for _, drop := range []bool{false, true} {
			post := m.Crash(CrashOptions{DropDirOps: drop})
			got, found, lerr := LoadManifest(post, "data")
			if lerr != nil || !found {
				t.Fatalf("k=%d drop=%v: manifest unreadable: found=%v err=%v", k, drop, found, lerr)
			}
			if !manifestEqual(got, oldM) && !manifestEqual(got, newM) {
				t.Fatalf("k=%d drop=%v: got %+v, neither old nor new", k, drop, got)
			}
		}
	}
	if crashPoints != 5 {
		t.Fatalf("expected 5 crash points (open, write, sync, rename, syncdir), got %d", crashPoints)
	}
}
