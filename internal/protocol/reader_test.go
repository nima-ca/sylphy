package protocol

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"
)

func bulk(s string) Value { return Value{Kind: KindBulk, Str: append([]byte{}, s...)} }

func TestReadValueOK(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want Value
	}{
		{"simple string", "+OK\r\n", Value{Kind: KindSimpleString, Str: []byte("OK")}},
		{"error", "-ERR boom\r\n", Value{Kind: KindError, Str: []byte("ERR boom")}},
		{"integer", ":1000\r\n", Value{Kind: KindInteger, Int: 1000}},
		{"negative integer", ":-42\r\n", Value{Kind: KindInteger, Int: -42}},
		{"bulk", "$5\r\nhello\r\n", bulk("hello")},
		{"empty bulk", "$0\r\n\r\n", bulk("")},
		{"null bulk", "$-1\r\n", Value{Kind: KindBulk, Null: true}},
		{"binary safe", "$6\r\na\r\nb\r\n\r\n", bulk("a\r\nb\r\n")},
		{"null array", "*-1\r\n", Value{Kind: KindArray, Null: true}},
		{"empty array", "*0\r\n", Value{Kind: KindArray, Array: []Value{}}},
		{"nested array", "*3\r\n:1\r\n$1\r\nx\r\n*1\r\n+hi\r\n", Value{Kind: KindArray, Array: []Value{
			{Kind: KindInteger, Int: 1},
			bulk("x"),
			{Kind: KindArray, Array: []Value{{Kind: KindSimpleString, Str: []byte("hi")}}},
		}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewReader(strings.NewReader(tt.in), Limits{})
			got, err := r.ReadValue()
			if err != nil {
				t.Fatalf("ReadValue: %v", err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %#v, want %#v", got, tt.want)
			}
			if _, err := r.ReadValue(); !errors.Is(err, io.EOF) {
				t.Fatalf("expected clean EOF afterwards, got %v", err)
			}
		})
	}
}

func TestReadValueErrors(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want error
	}{
		{"empty input is clean EOF", "", io.EOF},
		{"truncated simple string", "+OK", io.ErrUnexpectedEOF},
		{"missing CR", "+OK\n", ErrProtocol},
		{"invalid type byte", "?x\r\n", ErrProtocol},
		{"bad integer", ":abc\r\n", ErrProtocol},
		{"integer overflow", ":99999999999999999999\r\n", ErrProtocol},
		{"negative bulk length", "$-2\r\n", ErrProtocol},
		{"truncated bulk", "$5\r\nhel", io.ErrUnexpectedEOF},
		{"bulk missing terminator", "$3\r\nabc", io.ErrUnexpectedEOF},
		{"bulk wrong terminator", "$3\r\nabcXY", ErrProtocol},
		{"oversized bulk", "$600000000\r\n", ErrLimitExceeded},
		{"negative array length", "*-3\r\n", ErrProtocol},
		{"oversized array", "*2000000\r\n", ErrLimitExceeded},
		{"truncated array", "*1\r\n", io.ErrUnexpectedEOF},
		{"too deep", strings.Repeat("*1\r\n", 100) + ":1\r\n", ErrLimitExceeded},
		{"line too long", "+" + strings.Repeat("a", 70000) + "\r\n", ErrLimitExceeded},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.in), Limits{}).ReadValue()
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want []string
	}{
		{"resp", "*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$1\r\nv\r\n", []string{"SET", "k", "v"}},
		{"inline", "PING\r\n", []string{"PING"}},
		{"inline bare LF and extra spaces", "SET  a   b\n", []string{"SET", "a", "b"}},
		{"skips empty lines", "\r\n\r\nPING\r\n", []string{"PING"}},
		{"skips empty array", "*0\r\nPING\r\n", []string{"PING"}},
		{"binary bulk", "*2\r\n$4\r\nECHO\r\n$4\r\na\r\nb\r\n", []string{"ECHO", "a\r\nb"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			args, err := NewReader(strings.NewReader(tt.in), Limits{}).ReadCommand()
			if err != nil {
				t.Fatalf("ReadCommand: %v", err)
			}
			got := make([]string, len(args))
			for i, a := range args {
				got[i] = string(a)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestReadCommandErrors(t *testing.T) {
	small := Limits{MaxInlineLen: 8}
	tests := []struct {
		name string
		in   string
		lim  Limits
		want error
	}{
		{"clean EOF", "", Limits{}, io.EOF},
		{"unterminated inline", "PING", Limits{}, io.ErrUnexpectedEOF},
		{"non-bulk element", "*1\r\n:1\r\n", Limits{}, ErrProtocol},
		{"null bulk element", "*1\r\n$-1\r\n", Limits{}, ErrProtocol},
		{"bad bulk length", "*1\r\n$x\r\n", Limits{}, ErrProtocol},
		{"huge claimed bulk", "*1\r\n$999999999999\r\n", Limits{}, ErrLimitExceeded},
		{"huge claimed array", "*2000000000\r\n", Limits{}, ErrLimitExceeded},
		{"inline too long", strings.Repeat("a", 20) + "\r\n", small, ErrLimitExceeded},
		{"truncated bulk", "*1\r\n$5\r\nabc", Limits{}, io.ErrUnexpectedEOF},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewReader(strings.NewReader(tt.in), tt.lim).ReadCommand()
			if !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
}

func TestPipelinedCommandsAndBuffered(t *testing.T) {
	in := "PING\r\n*2\r\n$4\r\nECHO\r\n$1\r\nx\r\n"
	r := NewReader(strings.NewReader(in), Limits{})
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if r.Buffered() == 0 {
		t.Fatal("expected the second pipelined request to be buffered")
	}
	if _, err := r.ReadCommand(); err != nil {
		t.Fatal(err)
	}
	if r.Buffered() != 0 {
		t.Fatalf("Buffered = %d, want 0", r.Buffered())
	}
}

func TestLargeBulkAcrossChunks(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), 3<<20+17)
	in := "$" + "3145745" + "\r\n" + string(payload) + "\r\n"
	got, err := NewReader(strings.NewReader(in), Limits{}).ReadValue()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Str, payload) {
		t.Fatal("payload mismatch")
	}
}

func TestParseInt(t *testing.T) {
	tests := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true}, {"-1", -1, true}, {"9223372036854775807", 9223372036854775807, true},
		{"-9223372036854775808", -9223372036854775808, true},
		{"9223372036854775808", 0, false}, {"-9223372036854775809", 0, false},
		{"", 0, false}, {"-", 0, false}, {"1x", 0, false}, {"+1", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseInt([]byte(tt.in))
		if ok != tt.ok || (ok && got != tt.want) {
			t.Errorf("parseInt(%q) = %d,%v; want %d,%v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}
