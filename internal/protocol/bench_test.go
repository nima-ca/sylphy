package protocol

import "testing"

// loopReader serves data endlessly so benchmarks measure parsing, not I/O.
type loopReader struct {
	data []byte
	pos  int
}

func (l *loopReader) Read(p []byte) (int, error) {
	n := copy(p, l.data[l.pos:])
	l.pos = (l.pos + n) % len(l.data)
	return n, nil
}

func benchReadCommand(b *testing.B, req string) {
	r := NewReader(&loopReader{data: []byte(req)}, Limits{})
	b.ReportAllocs()
	b.SetBytes(int64(len(req)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := r.ReadCommand(); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkReadCommandSet(b *testing.B) {
	benchReadCommand(b, "*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")
}

func BenchmarkReadCommandGet(b *testing.B) {
	benchReadCommand(b, "*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n")
}
