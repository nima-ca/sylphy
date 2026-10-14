package persist

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/store"
)

const (
	pbSnapshotKeys = 100_000   // snapshot write/read benchmarks
	pbRecoveryKeys = 1_000_000 // recovery benchmarks (skipped under -short)
	pbTailRecords  = 100_000   // log records replayed on top of the 1M-key base
	pbValueLen     = 100
	pbChunk        = 50_000 // entries per BlockBuffer.Finish, bounding memory
)

func pbKeys(n int) []string {
	keys := make([]string, n)
	for i := range keys {
		keys[i] = fmt.Sprintf("key:%08d", i)
	}
	return keys
}

// pbWriteBase writes a committed base snapshot of keys (all string values of
// pbValueLen bytes, no deadlines) into dir through the production writer.
func pbWriteBase(b *testing.B, dir string, id uint64, keys []string) BaseRef {
	b.Helper()
	bw, err := CreateBase(OSFS(), dir, id)
	if err != nil {
		b.Fatal(err)
	}
	defer bw.Abort()
	v := store.NewString(bytes.Repeat([]byte("x"), pbValueLen)) // read-only while encoding, so shared
	var bb BlockBuffer
	flush := func() {
		framed, entries, blocks := bb.Finish()
		if err := bw.WriteFramed(framed, entries, blocks); err != nil {
			b.Fatal(err)
		}
	}
	for i, k := range keys {
		if err := bb.Add(k, v, 0, false); err != nil {
			b.Fatal(err)
		}
		if (i+1)%pbChunk == 0 {
			flush()
		}
	}
	flush()
	ref, err := bw.Commit()
	if err != nil {
		b.Fatal(err)
	}
	return ref
}

// BenchmarkSnapshotWrite measures encoding and writing a base snapshot of
// pbSnapshotKeys keys to the real file system, including fsync and rename.
func BenchmarkSnapshotWrite(b *testing.B) {
	dir := b.TempDir()
	keys := pbKeys(pbSnapshotKeys)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ref := pbWriteBase(b, dir, uint64(i+1), keys)
		b.SetBytes(ref.Size)
		b.StopTimer()
		if err := os.Remove(filepath.Join(dir, ref.Name)); err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(pbSnapshotKeys*b.N)/b.Elapsed().Seconds(), "keys/s")
}

// BenchmarkSnapshotRead measures decoding (including every CRC) a base snapshot
// of pbSnapshotKeys keys from the real file system.
func BenchmarkSnapshotRead(b *testing.B) {
	dir := b.TempDir()
	ref := pbWriteBase(b, dir, 1, pbKeys(pbSnapshotKeys))
	path := filepath.Join(dir, ref.Name)
	b.SetBytes(ref.Size)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f, err := OSFS().OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			b.Fatal(err)
		}
		sr, err := NewSnapshotReader(f)
		if err != nil {
			b.Fatal(err)
		}
		n := 0
		for {
			if _, err := sr.Next(); err != nil {
				if !errors.Is(err, io.EOF) {
					b.Fatal(err)
				}
				break
			}
			n++
		}
		_ = f.Close()
		if n != pbSnapshotKeys {
			b.Fatalf("read %d entries, want %d", n, pbSnapshotKeys)
		}
	}
	b.ReportMetric(float64(pbSnapshotKeys*b.N)/b.Elapsed().Seconds(), "keys/s")
}

// pbRecover is the body of the recovery benchmarks: recover a directory holding
// a pbRecoveryKeys-key base, plus (withTail) a log segment of pbTailRecords
// SETs that overwrite existing keys, into a fresh store each iteration.
func pbRecover(b *testing.B, withTail bool) {
	if testing.Short() {
		b.Skip("1M-key recovery is skipped under -short")
	}
	dir := b.TempDir()
	keys := pbKeys(pbRecoveryKeys)
	m := Manifest{Generation: 1, Base: pbWriteBase(b, dir, 1, keys)}
	if withTail {
		name := SegmentName(2)
		var buf []byte
		step := pbRecoveryKeys / pbTailRecords
		for i := 0; i < pbTailRecords; i++ {
			buf = AppendRecord(buf, [][]byte{[]byte("SET"), []byte(keys[i*step]), []byte("tail")})
		}
		if err := os.WriteFile(filepath.Join(dir, name), buf, 0o644); err != nil {
			b.Fatal(err)
		}
		m.Segments = []string{name}
	}
	if err := CommitManifest(OSFS(), dir, m); err != nil {
		b.Fatal(err)
	}
	reg, err := command.NewDefaultRegistry()
	if err != nil {
		b.Fatal(err)
	}

	var last *store.Store
	var report RecoveryReport
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		st, err := store.NewWithOptions(store.Options{Shards: 32})
		if err != nil {
			b.Fatal(err)
		}
		res, err := Recover(context.Background(), RecoverOptions{
			FS: OSFS(), Dir: dir, Target: st, Replay: NewCommandReplayer(reg, st, nil),
		})
		if err != nil {
			b.Fatal(err)
		}
		last, report = st, res.Report
	}
	b.StopTimer()
	wantRecords := 0
	if withTail {
		wantRecords = pbTailRecords
	}
	if report.BaseKeys != pbRecoveryKeys || report.Records != wantRecords || last.Len() != pbRecoveryKeys {
		b.Fatalf("recovered %d base keys, %d records, %d live keys; want %d, %d, %d",
			report.BaseKeys, report.Records, last.Len(), pbRecoveryKeys, wantRecords, pbRecoveryKeys)
	}
	b.ReportMetric(float64(pbRecoveryKeys*b.N)/b.Elapsed().Seconds(), "keys/s")
}

// BenchmarkRecoverSnapshotOnly measures recovery of a 1M-key base snapshot.
func BenchmarkRecoverSnapshotOnly(b *testing.B) { pbRecover(b, false) }

// BenchmarkRecoverSnapshotAndTail measures recovery of a 1M-key base snapshot
// plus a log tail of pbTailRecords commands.
func BenchmarkRecoverSnapshotAndTail(b *testing.B) { pbRecover(b, true) }
