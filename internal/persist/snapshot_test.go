package persist

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"
	"strings"
	"testing"

	"github.com/nima-ca/sylphy/internal/hash"
	"github.com/nima-ca/sylphy/internal/list"
	"github.com/nima-ca/sylphy/internal/set"
	"github.com/nima-ca/sylphy/internal/store"
	"github.com/nima-ca/sylphy/internal/zset"
)

type zm struct {
	member string
	score  float64
}

func allBytes() string {
	b := make([]byte, 256)
	for i := range b {
		b[i] = byte(i)
	}
	return string(b)
}

func mkString(s string) store.Value { return store.NewString([]byte(s)) }

func mkList(items ...string) store.Value {
	l := list.New()
	for _, it := range items {
		l.PushBack([]byte(it))
	}
	return l
}

func mkHash(kv ...string) store.Value {
	h := hash.New()
	for i := 0; i < len(kv); i += 2 {
		h.Set(kv[i], kv[i+1])
	}
	return h
}

func mkSet(ms ...string) store.Value {
	s := set.New()
	for _, m := range ms {
		s.Add(m)
	}
	return s
}

func mkZSet(ps ...zm) store.Value {
	z := zset.New()
	for _, p := range ps {
		z.Set(p.member, p.score)
	}
	return z
}

func sameValue(a, b store.Value) bool {
	switch x := a.(type) {
	case *store.String:
		y, ok := b.(*store.String)
		return ok && bytes.Equal(x.Bytes(), y.Bytes())
	case *list.List:
		y, ok := b.(*list.List)
		return ok && x.Len() == y.Len() && slices.EqualFunc(x.Range(0, x.Len()), y.Range(0, y.Len()), bytes.Equal)
	case *hash.Hash:
		y, ok := b.(*hash.Hash)
		return ok && slices.Equal(x.Flat(), y.Flat())
	case *set.Set:
		y, ok := b.(*set.Set)
		if !ok {
			return false
		}
		xm, ym := x.Members(), y.Members()
		slices.Sort(xm)
		slices.Sort(ym)
		return slices.Equal(xm, ym)
	case *zset.ZSet:
		y, ok := b.(*zset.ZSet)
		if !ok || x.Len() != y.Len() {
			return false
		}
		return slices.EqualFunc(x.RangeByRank(0, x.Len(), false), y.RangeByRank(0, y.Len(), false),
			func(p, q zset.Entry) bool {
				return p.Member == q.Member && math.Float64bits(p.Score) == math.Float64bits(q.Score)
			})
	}
	return false
}

func sameEntries(a, b []SnapshotEntry) bool {
	return slices.EqualFunc(a, b, func(x, y SnapshotEntry) bool {
		return x.Key == y.Key && x.DeadlineMs == y.DeadlineMs && x.HasDeadline == y.HasDeadline && sameValue(x.Value, y.Value)
	})
}

func sampleEntries() []SnapshotEntry {
	const ttl = 1_700_000_000_123
	sum := 0.1
	sum += 0.2 // 0.30000000000000004: a score that decimal formatting could change
	return []SnapshotEntry{
		{Key: "str", Value: mkString("hello")},
		{Key: "str:ttl", Value: mkString(allBytes()), DeadlineMs: ttl, HasDeadline: true},
		{Key: "\x00\r\n\xff", Value: mkString("")},
		{Key: "", Value: mkString("empty key")},
		{Key: "list", Value: mkList("a", "", allBytes(), "a"), DeadlineMs: ttl + 1, HasDeadline: true},
		{Key: "hash", Value: mkHash("f1", "v1", "", "", allBytes(), "x")},
		{Key: "set", Value: mkSet("m1", "m2", "", allBytes()), DeadlineMs: math.MaxInt64, HasDeadline: true},
		{Key: "zset", Value: mkZSet(
			zm{"a", 1.5}, zm{"b", math.Inf(-1)}, zm{"c", math.Inf(1)}, zm{"d", math.Copysign(0, -1)},
			zm{"e", 0}, zm{"f", math.SmallestNonzeroFloat64}, zm{"g", math.MaxFloat64}, zm{"h", sum}, zm{allBytes(), -7})},
	}
}

func encodeSnapshot(t testing.TB, entries []SnapshotEntry) ([]byte, BaseRef) {
	t.Helper()
	var bb BlockBuffer
	for _, e := range entries {
		if err := bb.Add(e.Key, e.Value, e.DeadlineMs, e.HasDeadline); err != nil {
			t.Fatal(err)
		}
	}
	framed, n, blocks := bb.Finish()
	var out bytes.Buffer
	sw, err := NewSnapshotWriter(&out)
	if err != nil {
		t.Fatal(err)
	}
	if err := sw.WriteFramed(framed, n, blocks); err != nil {
		t.Fatal(err)
	}
	ref, err := sw.Close()
	if err != nil {
		t.Fatal(err)
	}
	return out.Bytes(), ref
}

func decodeSnapshot(data []byte) ([]SnapshotEntry, BaseRef, error) {
	sr, err := NewSnapshotReader(bytes.NewReader(data))
	if err != nil {
		return nil, BaseRef{}, err
	}
	var out []SnapshotEntry
	for {
		e, err := sr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return out, BaseRef{}, err
		}
		out = append(out, e)
	}
	ref, _ := sr.Result()
	return out, ref, nil
}

func TestSnapshotRoundTrip(t *testing.T) {
	want := sampleEntries()
	data, ref := encodeSnapshot(t, want)
	got, ref2, err := decodeSnapshot(data)
	if err != nil {
		t.Fatal(err)
	}
	if !sameEntries(got, want) {
		t.Fatal("decoded entries differ from the originals")
	}
	if ref != ref2 || ref.Size != int64(len(data)) {
		t.Fatalf("writer ref %+v, reader ref %+v, file size %d", ref, ref2, len(data))
	}
	if ref.CRC != binary.LittleEndian.Uint32(data[len(data)-4:]) {
		t.Fatal("BaseRef.CRC is not the trailer checksum")
	}
	// Decoded values are fresh objects: mutating one must not touch the next decode.
	got[0].Value.(*store.String).Bytes()[0] = 'X'
	again, _, err := decodeSnapshot(data)
	if err != nil || !sameEntries(again, want) {
		t.Fatal("decoding is not independent of earlier results")
	}
}

func TestSnapshotEmpty(t *testing.T) {
	data, ref := encodeSnapshot(t, nil)
	got, _, err := decodeSnapshot(data)
	if err != nil || len(got) != 0 {
		t.Fatalf("got %v, %v", got, err)
	}
	if ref.Size != int64(snapHeaderLen+17) {
		t.Fatalf("empty snapshot is %d bytes", ref.Size)
	}
}

func TestSnapshotManyBlocks(t *testing.T) {
	var bb BlockBuffer
	payload := strings.Repeat("x", 300)
	const n = 3000
	for i := range n {
		if err := bb.Add(fmt.Sprintf("key:%d", i), mkString(payload), 0, false); err != nil {
			t.Fatal(err)
		}
	}
	framed, entries, blocks := bb.Finish()
	if entries != n || blocks < 2 {
		t.Fatalf("entries %d blocks %d", entries, blocks)
	}
	var out bytes.Buffer
	sw, err := NewSnapshotWriter(&out)
	mustDo(t, err)
	mustDo(t, sw.WriteFramed(framed, entries, blocks))
	_, err = sw.Close()
	mustDo(t, err)
	got, _, err := decodeSnapshot(out.Bytes())
	if err != nil || len(got) != n {
		t.Fatalf("got %d entries, err %v", len(got), err)
	}
	for i, e := range got {
		if e.Key != fmt.Sprintf("key:%d", i) {
			t.Fatalf("entry %d is %q: order not preserved", i, e.Key)
		}
	}
}

func TestSnapshotDetectsEverySingleByteChange(t *testing.T) {
	data, _ := encodeSnapshot(t, sampleEntries())
	for i := range data {
		for _, mask := range []byte{0x01, 0x80, 0xFF} {
			bad := slices.Clone(data)
			bad[i] ^= mask
			if _, _, err := decodeSnapshot(bad); err == nil {
				t.Fatalf("byte %d changed by %#x was accepted", i, mask)
			}
		}
	}
}

func TestSnapshotDetectsTruncationAtEveryLength(t *testing.T) {
	data, _ := encodeSnapshot(t, sampleEntries())
	for n := range len(data) {
		_, _, err := decodeSnapshot(data[:n])
		if !errors.Is(err, ErrTruncated) {
			t.Fatalf("truncated to %d of %d bytes: %v", n, len(data), err)
		}
	}
	if _, _, err := decodeSnapshot(append(slices.Clone(data), 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("trailing byte: %v", err)
	}
}

func rawHeader(ver, flags uint16, reserved uint32) []byte {
	h := append([]byte(snapMagic), 0, 0, 0, 0, 0, 0, 0, 0)
	binary.LittleEndian.PutUint16(h[8:], ver)
	binary.LittleEndian.PutUint16(h[10:], flags)
	binary.LittleEndian.PutUint32(h[12:], reserved)
	return binary.LittleEndian.AppendUint32(h, crc32.Checksum(h, castagnoli))
}

func frameBlock(payload []byte, count int) []byte {
	out := []byte{tagBlock}
	out = binary.LittleEndian.AppendUint32(out, uint32(count))
	out = binary.LittleEndian.AppendUint32(out, uint32(len(payload)))
	out = append(out, payload...)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
}

func fileOf(header, blocks []byte, entries uint64, nblocks uint32) []byte {
	out := append(slices.Clone(header), blocks...)
	out = append(out, tagEnd)
	out = binary.LittleEndian.AppendUint64(out, entries)
	out = binary.LittleEndian.AppendUint32(out, nblocks)
	return binary.LittleEndian.AppendUint32(out, crc32.Checksum(out, castagnoli))
}

func TestSnapshotHeaderChecks(t *testing.T) {
	if !bytes.Equal(rawHeader(SnapshotVersion, 0, 0), appendSnapshotHeader(nil)) {
		t.Fatal("test header builder disagrees with the real one")
	}
	if _, _, err := decodeSnapshot(fileOf(rawHeader(1, 0, 0), nil, 0, 0)); err != nil {
		t.Fatalf("well-formed empty file: %v", err)
	}
	if _, _, err := decodeSnapshot(fileOf(rawHeader(2, 0, 0), nil, 0, 0)); !errors.Is(err, ErrVersion) {
		t.Fatalf("version 2: %v", err)
	}
	if _, _, err := decodeSnapshot(fileOf(rawHeader(1, 1, 0), nil, 0, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("flags: %v", err)
	}
	if _, _, err := decodeSnapshot(fileOf(rawHeader(1, 0, 9), nil, 0, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("reserved: %v", err)
	}
	bad := rawHeader(1, 0, 0)
	bad[0] = 'X'
	if _, _, err := decodeSnapshot(fileOf(bad, nil, 0, 0)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("magic: %v", err)
	}
	if _, _, err := decodeSnapshot(nil); !errors.Is(err, ErrTruncated) {
		t.Fatalf("empty input: %v", err)
	}
	unknownTag := append(rawHeader(1, 0, 0), 0x02)
	if _, _, err := decodeSnapshot(unknownTag); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("unknown block tag: %v", err)
	}
}

func TestSnapshotEndRecordChecks(t *testing.T) {
	blk := frameBlock([]byte{typeString, 0, 1, 'k', 1, 'v'}, 1)
	if _, _, err := decodeSnapshot(fileOf(rawHeader(1, 0, 0), blk, 1, 1)); err != nil {
		t.Fatalf("valid: %v", err)
	}
	for _, c := range []struct {
		name            string
		entries, blocks uint64
	}{{"entries", 2, 1}, {"blocks", 1, 2}} {
		_, _, err := decodeSnapshot(fileOf(rawHeader(1, 0, 0), blk, c.entries, uint32(c.blocks)))
		if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrChecksum) {
			t.Fatalf("wrong %s count: %v", c.name, err)
		}
	}
	if _, _, err := decodeSnapshot(fileOf(rawHeader(1, 0, 0), frameBlock(nil, 0), 0, 1)); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("empty block: %v", err)
	}
}

func TestSnapshotRejectsInvalidEntries(t *testing.T) {
	nan := binary.LittleEndian.AppendUint64(nil, 0x7ff8000000000001)
	one := binary.LittleEndian.AppendUint64(nil, math.Float64bits(1))
	tests := []struct {
		name    string
		payload []byte
		count   int
		want    string
	}{
		{"unknown type", []byte{9, 0, 1, 'k', 1, 'v'}, 1, "unknown entry type"},
		{"unknown flags", []byte{typeString, 0x02, 1, 'k', 1, 'v'}, 1, "unknown entry flags"},
		{"zero deadline", slices.Concat([]byte{typeString, flagDeadline, 1, 'k'}, make([]byte, 8), []byte{1, 'v'}), 1, "non-positive deadline"},
		{"empty list", []byte{typeList, 0, 1, 'k', 0}, 1, "empty collection"},
		{"count beyond block", []byte{typeList, 0, 1, 'k', 50, 1, 'a'}, 1, "exceeds"},
		{"string beyond block", []byte{typeString, 0, 1, 'k', 100, 'v'}, 1, "exceeds"},
		{"duplicate hash field", []byte{typeHash, 0, 1, 'k', 2, 1, 'f', 1, 'a', 1, 'f', 1, 'b'}, 1, "duplicate hash field"},
		{"duplicate set member", []byte{typeSet, 0, 1, 'k', 2, 1, 'm', 1, 'm'}, 1, "duplicate set member"},
		{"NaN score", slices.Concat([]byte{typeZSet, 0, 1, 'k', 1}, nan, []byte{1, 'm'}), 1, "NaN"},
		{"duplicate zset member", slices.Concat([]byte{typeZSet, 0, 1, 'k', 2}, one, []byte{1, 'm'}, one, []byte{1, 'm'}), 1, "duplicate zset member"},
		{"unused bytes", []byte{typeString, 0, 1, 'k', 1, 'v', 9, 9}, 1, "unused bytes"},
		{"fewer entries than count", []byte{typeString, 0, 1, 'k', 1, 'v', 0, 0}, 2, ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			data := fileOf(rawHeader(1, 0, 0), frameBlock(tc.payload, tc.count), uint64(tc.count), 1)
			_, _, err := decodeSnapshot(data)
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrChecksum) || errors.Is(err, ErrTruncated) {
				t.Fatalf("err = %v, want plain corruption", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

type stubValue struct{}

func (stubValue) Kind() store.Kind { return store.KindList }
func (stubValue) Len() int         { return 1 }

func TestAppendEntryRejectsUnsupported(t *testing.T) {
	prefix := []byte("keep")
	for name, tc := range map[string]struct {
		v   store.Value
		dl  int64
		has bool
	}{
		"nil value":         {nil, 0, false},
		"unknown type":      {stubValue{}, 0, false},
		"empty list":        {list.New(), 0, false},
		"empty hash":        {hash.New(), 0, false},
		"empty set":         {set.New(), 0, false},
		"empty zset":        {zset.New(), 0, false},
		"zero deadline":     {mkString("v"), 0, true},
		"negative deadline": {mkString("v"), -5, true},
	} {
		t.Run(name, func(t *testing.T) {
			out, err := AppendEntry(slices.Clone(prefix), "k", tc.v, tc.dl, tc.has)
			if !errors.Is(err, ErrUnsupportedValue) {
				t.Fatalf("err = %v", err)
			}
			if !bytes.Equal(out, prefix) {
				t.Fatalf("dst changed to %q", out)
			}
		})
	}
}

func TestBlockBufferDropsFailedEntries(t *testing.T) {
	var bb BlockBuffer
	if err := bb.Add("bad", stubValue{}, 0, false); err == nil {
		t.Fatal("unsupported value accepted")
	}
	if framed, n, blocks := bb.Finish(); len(framed) != 0 || n != 0 || blocks != 0 {
		t.Fatalf("a failed first entry left %d bytes, %d entries, %d blocks", len(framed), n, blocks)
	}
	mustDo(t, bb.Add("a", mkString("1"), 0, false))
	if err := bb.Add("bad", list.New(), 0, false); err == nil {
		t.Fatal("empty list accepted")
	}
	mustDo(t, bb.Add("b", mkString("2"), 5, true))
	if bb.Entries() != 2 {
		t.Fatalf("Entries = %d", bb.Entries())
	}
	framed, n, blocks := bb.Finish()
	var out bytes.Buffer
	sw, err := NewSnapshotWriter(&out)
	mustDo(t, err)
	mustDo(t, sw.WriteFramed(framed, n, blocks))
	_, err = sw.Close()
	mustDo(t, err)
	got, _, err := decodeSnapshot(out.Bytes())
	if err != nil || len(got) != 2 || got[0].Key != "a" || got[1].Key != "b" {
		t.Fatalf("got %+v, %v", got, err)
	}
	if bb.Entries() != 0 {
		t.Fatal("Finish must reset the buffer")
	}
}

type failWriter struct{ left int }

func (w *failWriter) Write(p []byte) (int, error) {
	if len(p) > w.left {
		n := w.left
		w.left = 0
		return n, errBoom
	}
	w.left -= len(p)
	return len(p), nil
}

func TestSnapshotWriterErrorsAreSticky(t *testing.T) {
	if _, err := NewSnapshotWriter(&failWriter{left: 10}); !errors.Is(err, errBoom) {
		t.Fatalf("header write: %v", err)
	}
	sw, err := NewSnapshotWriter(&failWriter{left: snapHeaderLen + 5})
	mustDo(t, err)
	if err := sw.WriteFramed(make([]byte, 100), 1, 1); !errors.Is(err, errBoom) {
		t.Fatalf("WriteFramed: %v", err)
	}
	if err := sw.WriteFramed(nil, 0, 0); !errors.Is(err, errBoom) {
		t.Fatalf("error not sticky: %v", err)
	}
	if _, err := sw.Close(); !errors.Is(err, errBoom) {
		t.Fatalf("Close: %v", err)
	}
	if _, err := sw.Close(); err == nil {
		t.Fatal("second Close succeeded")
	}
	var out bytes.Buffer
	ok, err := NewSnapshotWriter(&out)
	mustDo(t, err)
	_, err = ok.Close()
	mustDo(t, err)
	if err := ok.WriteFramed(nil, 0, 0); err == nil {
		t.Fatal("write after Close succeeded")
	}
}

func TestSnapshotReaderResultOnlyAfterEOF(t *testing.T) {
	data, _ := encodeSnapshot(t, sampleEntries()[:2])
	sr, err := NewSnapshotReader(bytes.NewReader(data))
	mustDo(t, err)
	if _, ok := sr.Result(); ok {
		t.Fatal("Result before the end")
	}
	_, err = sr.Next()
	mustDo(t, err)
	if sr.Entries() != 2 { // counted per block as it is read
		t.Fatalf("Entries = %d", sr.Entries())
	}
	_, err = sr.Next()
	mustDo(t, err)
	if _, err = sr.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("end: %v", err)
	}
	if ref, ok := sr.Result(); !ok || ref.Size != int64(len(data)) {
		t.Fatalf("Result = %+v %v", ref, ok)
	}
}
