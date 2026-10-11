package persist

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
	"testing/iotest"
)

func argv(ss ...string) [][]byte {
	out := make([][]byte, len(ss))
	for i, s := range ss {
		out[i] = []byte(s)
	}
	return out
}

func TestRecordRoundTrip(t *testing.T) {
	big := bytes.Repeat([]byte{0xAB}, 2<<20+17) // more than one read chunk
	all := make([]byte, 256)
	for i := range all {
		all[i] = byte(i)
	}
	cases := [][][]byte{
		argv("SET", "k", "v"),
		argv("SET", "", ""),
		{[]byte("SET"), []byte("k\r\n\x00*$"), all},
		{[]byte("RPUSH"), []byte("l"), big},
		argv("PING"),
	}
	var buf []byte
	var ends []int64
	for _, c := range cases {
		buf = AppendRecord(buf, c)
		ends = append(ends, int64(len(buf)))
	}
	r := NewRecordReader(bytes.NewReader(buf), RecordLimits{})
	for i, c := range cases {
		got, err := r.Next()
		if err != nil {
			t.Fatalf("record %d: %v", i, err)
		}
		if len(got) != len(c) {
			t.Fatalf("record %d: %d args, want %d", i, len(got), len(c))
		}
		for j := range c {
			if !bytes.Equal(got[j], c[j]) {
				t.Fatalf("record %d arg %d differs", i, j)
			}
		}
		if r.Offset() != ends[i] {
			t.Fatalf("record %d: offset %d, want %d", i, r.Offset(), ends[i])
		}
	}
	if _, err := r.Next(); !errors.Is(err, io.EOF) {
		t.Fatalf("after the last record: %v", err)
	}
}

func TestRecordTruncationAtEveryByte(t *testing.T) {
	cases := [][][]byte{argv("SET", "key", "value"), argv("DEL", "a", "b", "c"), argv("PEXPIREAT", "k", "1700000000000")}
	var buf []byte
	bounds := []int64{0}
	for _, c := range cases {
		buf = AppendRecord(buf, c)
		bounds = append(bounds, int64(len(buf)))
	}
	for cut := 0; cut <= len(buf); cut++ {
		r := NewRecordReader(bytes.NewReader(buf[:cut]), RecordLimits{})
		complete := 0
		for {
			_, err := r.Next()
			if err == nil {
				complete++
				continue
			}
			boundary := bounds[complete]
			if int64(cut) == boundary {
				if !errors.Is(err, io.EOF) {
					t.Fatalf("cut %d at a boundary: %v", cut, err)
				}
			} else {
				var de *DecodeError
				if !errors.As(err, &de) || !errors.Is(err, ErrTruncated) {
					t.Fatalf("cut %d: %v, want truncation", cut, err)
				}
				if de.Offset != boundary || r.Offset() != boundary {
					t.Fatalf("cut %d: error offset %d, reader offset %d, want %d", cut, de.Offset, r.Offset(), boundary)
				}
			}
			break
		}
		want := 0
		for _, b := range bounds[1:] {
			if b <= int64(cut) {
				want++
			}
		}
		if complete != want {
			t.Fatalf("cut %d: %d complete records, want %d", cut, complete, want)
		}
	}
}

func TestRecordFormatErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
		off  int64
		want string
	}{
		{"no star", "xyz", 0, "does not start"},
		{"zero fill", "\x00\x00\x00", 0, "does not start"},
		{"zero args", "*0\r\n", 0, "out of range"},
		{"bulk marker", "*1\r\n:5\r\n", 4, "expected '$'"},
		{"negative bulk", "*1\r\n$-1\r\n", 5, "invalid byte"},
		{"leading zero", "*01\r\n", 2, "leading zero"},
		{"bare LF", "*1\n", 2, "invalid byte"},
		{"bad terminator", "*1\r\n$3\r\nabcX\r\n", 11, "not terminated"},
		{"huge bulk", "*1\r\n$600000000\r\n", 4, "exceeds limit"},
		{"long count", "*99999999999999999999\r\n", 19, "too long"},
		{"CR without LF", "*1\rX", 3, "expected LF"},
		{"empty count", "*\r\n", 1, "empty"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			r := NewRecordReader(strings.NewReader(tc.in), RecordLimits{})
			_, err := r.Next()
			var de *DecodeError
			if !errors.As(err, &de) {
				t.Fatalf("err = %v", err)
			}
			if !errors.Is(err, ErrCorrupt) || errors.Is(err, ErrTruncated) {
				t.Fatalf("err = %v, want corrupt and not truncated", err)
			}
			if de.Offset != tc.off || !strings.Contains(de.Msg, tc.want) {
				t.Fatalf("offset %d msg %q; want offset %d containing %q", de.Offset, de.Msg, tc.off, tc.want)
			}
			if _, err2 := r.Next(); err2 != err {
				t.Fatalf("error is not sticky: %v then %v", err, err2)
			}
		})
	}
}

func TestRecordLimits(t *testing.T) {
	three := AppendRecord(nil, argv("a", "b", "c"))
	if _, err := NewRecordReader(bytes.NewReader(three), RecordLimits{MaxArgs: 2}).Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("MaxArgs: %v", err)
	}
	long := AppendRecord(nil, argv("abcde"))
	if _, err := NewRecordReader(bytes.NewReader(long), RecordLimits{MaxBulkLen: 4}).Next(); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("MaxBulkLen: %v", err)
	}
	if _, err := NewRecordReader(bytes.NewReader(three), RecordLimits{MaxArgs: 3, MaxBulkLen: 1}).Next(); err != nil {
		t.Fatalf("limits at the boundary: %v", err)
	}
}

func TestRecordReaderPassesIOErrorsThrough(t *testing.T) {
	r := NewRecordReader(iotest.ErrReader(errBoom), RecordLimits{})
	_, err := r.Next()
	if !errors.Is(err, errBoom) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("err = %v", err)
	}
	// An I/O error mid-record is not truncation either.
	rec := AppendRecord(nil, argv("SET", "k", "v"))
	r = NewRecordReader(io.MultiReader(bytes.NewReader(rec[:5]), iotest.ErrReader(errBoom)), RecordLimits{})
	if _, err := r.Next(); !errors.Is(err, errBoom) || errors.Is(err, ErrCorrupt) {
		t.Fatalf("mid-record I/O error: %v", err)
	}
}
