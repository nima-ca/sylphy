package protocol

import (
	"bytes"
	"testing"
)

// FuzzReader asserts the parser never panics on arbitrary input and never
// returns more payload bytes than the input contained (i.e. it never trusts
// length prefixes).
func FuzzReader(f *testing.F) {
	seeds := []string{
		"", "PING\r\n", "+OK\r\n", "-ERR x\r\n", ":1\r\n", "$-1\r\n", "*-1\r\n",
		"$3\r\nabc\r\n", "*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", "*1\r\n$999999999999\r\n",
		"$5\r\nab", "*3\r\n*2\r\n:1\r\n:2\r\n+x\r\n$1\r\ny\r\n", "\x00\xff\r\n", "*0\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	lim := Limits{MaxBulkLen: 1 << 16, MaxArrayLen: 1 << 10, MaxInlineLen: 1 << 10, MaxDepth: 8}

	f.Fuzz(func(t *testing.T, data []byte) {
		// Tiny buffer to exercise ErrBufferFull paths.
		r := NewReaderSize(bytes.NewReader(data), 16, lim)
		for i := 0; i < 1000; i++ {
			if _, err := r.ReadValue(); err != nil {
				break
			}
		}
		r = NewReaderSize(bytes.NewReader(data), 16, lim)
		total := 0
		for i := 0; i < 1000; i++ {
			args, err := r.ReadCommand()
			if err != nil {
				break
			}
			for _, a := range args {
				total += len(a)
			}
			if total > len(data) {
				t.Fatalf("returned %d payload bytes from %d input bytes", total, len(data))
			}
		}
	})
}
