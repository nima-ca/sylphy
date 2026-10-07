package protocol

import (
	"bytes"
	"reflect"
	"testing"
)

func TestWriter(t *testing.T) {
	tests := []struct {
		name string
		fn   func(*Writer)
		want string
	}{
		{"simple string", func(w *Writer) { w.WriteSimpleString("OK") }, "+OK\r\n"},
		{"simple string sanitized", func(w *Writer) { w.WriteSimpleString("a\r\nb") }, "+a  b\r\n"},
		{"error sanitized", func(w *Writer) { w.WriteError("ERR x\r\n+OK") }, "-ERR x  +OK\r\n"},
		{"integer", func(w *Writer) { w.WriteInteger(-7) }, ":-7\r\n"},
		{"bulk", func(w *Writer) { w.WriteBulk([]byte("foo")) }, "$3\r\nfoo\r\n"},
		{"empty bulk", func(w *Writer) { w.WriteBulk(nil) }, "$0\r\n\r\n"},
		{"bulk string", func(w *Writer) { w.WriteBulkString("hi") }, "$2\r\nhi\r\n"},
		{"null bulk", func(w *Writer) { w.WriteNullBulk() }, "$-1\r\n"},
		{"array header", func(w *Writer) { w.WriteArrayHeader(2) }, "*2\r\n"},
		{"null array", func(w *Writer) { w.WriteNullArray() }, "*-1\r\n"},
		{"bulk array", func(w *Writer) { w.WriteBulkArray([][]byte{[]byte("a"), []byte("bc")}) }, "*2\r\n$1\r\na\r\n$2\r\nbc\r\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			w := NewWriter(&buf)
			tt.fn(w)
			if buf.Len() != 0 {
				t.Fatal("writer must buffer until Flush")
			}
			if err := w.Flush(); err != nil {
				t.Fatal(err)
			}
			if buf.String() != tt.want {
				t.Fatalf("got %q, want %q", buf.String(), tt.want)
			}
		})
	}
}

func TestRoundTrip(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.WriteArrayHeader(5)
	w.WriteSimpleString("OK")
	w.WriteInteger(42)
	w.WriteBulk([]byte("a\r\nb\x00"))
	w.WriteNullBulk()
	w.WriteError("ERR nope")
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	got, err := NewReader(&buf, Limits{}).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	want := Value{Kind: KindArray, Array: []Value{
		{Kind: KindSimpleString, Str: []byte("OK")},
		{Kind: KindInteger, Int: 42},
		{Kind: KindBulk, Str: []byte("a\r\nb\x00")},
		{Kind: KindBulk, Null: true},
		{Kind: KindError, Str: []byte("ERR nope")},
	}}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("got %#v, want %#v", got, want)
	}
}
