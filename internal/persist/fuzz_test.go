package persist

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

// Run the seeds with go test; explore with task fuzz.

func FuzzRecordReader(f *testing.F) {
	f.Add(AppendRecord(nil, argv("SET", "k", "v")))
	f.Add(append(AppendRecord(nil, argv("DEL", "a")), AppendRecord(nil, argv("PEXPIREAT", "k", "1700000000000"))...))
	f.Add([]byte("*1\r\n$3\r\nab"))
	f.Add([]byte("*1\r\n$-1\r\n"))
	f.Add([]byte("\x00\x00\x00\x00"))
	f.Add([]byte("*2\r\n$3\r\nSET\r\n$1\r\nk\r\n*"))
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewRecordReader(bytes.NewReader(data), RecordLimits{MaxArgs: 64, MaxBulkLen: 1 << 16})
		prev := int64(0)
		for {
			rec, err := r.Next()
			if errors.Is(err, io.EOF) {
				if prev != int64(len(data)) {
					t.Fatalf("clean EOF at %d of %d bytes", prev, len(data))
				}
				return
			}
			if err != nil {
				var de *DecodeError
				if !errors.As(err, &de) || !errors.Is(err, ErrCorrupt) {
					t.Fatalf("unexpected error type: %v", err)
				}
				if de.Offset < prev || de.Offset > int64(len(data)) {
					t.Fatalf("error offset %d outside [%d, %d]", de.Offset, prev, len(data))
				}
				if errors.Is(err, ErrTruncated) && (de.Offset != prev || r.Offset() != prev) {
					t.Fatalf("truncation offset %d, reader offset %d, want %d", de.Offset, r.Offset(), prev)
				}
				return
			}
			end := r.Offset()
			if end <= prev || end > int64(len(data)) {
				t.Fatalf("offset went from %d to %d (len %d)", prev, end, len(data))
			}
			if !bytes.Equal(AppendRecord(nil, rec), data[prev:end]) {
				t.Fatalf("record at %d does not re-encode to the bytes it came from", prev)
			}
			prev = end
		}
	})
}

func FuzzSnapshotReader(f *testing.F) {
	valid, _ := encodeSnapshot(f, sampleEntries())
	f.Add(valid)
	f.Add(valid[:len(valid)/2])
	f.Add(valid[:snapHeaderLen])
	f.Add([]byte{})
	f.Add(appendSnapshotHeader(nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		got, ref, err := decodeSnapshot(data)
		if err != nil {
			var de *DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("unexpected error type: %v", err)
			}
			return
		}
		if ref.Size != int64(len(data)) {
			t.Fatalf("Result size %d, input %d", ref.Size, len(data))
		}
		// Anything the decoder accepts must survive the encoder and decode again.
		again, _ := encodeSnapshot(t, got)
		back, _, err := decodeSnapshot(again)
		if err != nil || !sameEntries(back, got) {
			t.Fatalf("re-encoding an accepted snapshot changed it: %v", err)
		}
	})
}

func FuzzDecodeManifest(f *testing.F) {
	f.Add(EncodeManifest(Manifest{Generation: 1}))
	f.Add(EncodeManifest(Manifest{Generation: 7, Base: BaseRef{Name: BaseName(3), Size: 9, CRC: 0xabcdef01}, Segments: []string{SegmentName(4), SegmentName(5)}}))
	f.Add([]byte("SYLPHY-MANIFEST 1\n"))
	f.Add([]byte{})
	f.Fuzz(func(t *testing.T, data []byte) {
		m, err := DecodeManifest(data)
		if err != nil {
			return
		}
		if !bytes.Equal(EncodeManifest(m), data) {
			t.Fatalf("accepted manifest does not re-encode to its own bytes")
		}
	})
}
