package protocol

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

// Writer encodes RESP2 replies into a buffer. Write methods do not return
// errors: the underlying bufio.Writer is sticky, so the first I/O error is
// reported by Flush. This keeps command handlers free of error plumbing.
//
// Writer is not safe for concurrent use.
type Writer struct {
	bw      *bufio.Writer
	scratch []byte
}

// NewWriter returns a Writer over w with DefaultBufferSize.
func NewWriter(w io.Writer) *Writer { return NewWriterSize(w, DefaultBufferSize) }

// NewWriterSize returns a Writer over w with the given buffer size.
func NewWriterSize(w io.Writer, size int) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, size), scratch: make([]byte, 0, 24)}
}

// WriteSimpleString writes "+s\r\n". CR and LF in s are replaced by spaces so a
// reply can never inject extra protocol frames.
func (w *Writer) WriteSimpleString(s string) { w.line('+', s) }

// WriteError writes "-msg\r\n" with the same sanitization as WriteSimpleString.
func (w *Writer) WriteError(msg string) { w.line('-', msg) }

// WriteInteger writes ":n\r\n".
func (w *Writer) WriteInteger(n int64) { w.header(':', n) }

// WriteBulk writes a bulk string. A nil or empty b is an empty (not null) bulk.
func (w *Writer) WriteBulk(b []byte) {
	w.header('$', int64(len(b)))
	_, _ = w.bw.Write(b)
	w.crlf()
}

// WriteBulkString is WriteBulk for a string, avoiding a conversion.
func (w *Writer) WriteBulkString(s string) {
	w.header('$', int64(len(s)))
	_, _ = w.bw.WriteString(s)
	w.crlf()
}

// WriteNullBulk writes the null bulk string "$-1\r\n".
func (w *Writer) WriteNullBulk() { _, _ = w.bw.WriteString("$-1\r\n") }

// WriteArrayHeader writes "*n\r\n"; the caller must then write n elements.
func (w *Writer) WriteArrayHeader(n int) { w.header('*', int64(n)) }

// WriteNullArray writes the null array "*-1\r\n".
func (w *Writer) WriteNullArray() { _, _ = w.bw.WriteString("*-1\r\n") }

// WriteBulkArray writes an array whose elements are all bulk strings.
func (w *Writer) WriteBulkArray(items [][]byte) {
	w.WriteArrayHeader(len(items))
	for _, it := range items {
		w.WriteBulk(it)
	}
}

// Flush writes buffered data to the underlying writer and returns the first
// error encountered since the Writer was created.
func (w *Writer) Flush() error { return w.bw.Flush() }

// Buffered returns the number of bytes waiting to be flushed.
func (w *Writer) Buffered() int { return w.bw.Buffered() }

func (w *Writer) line(prefix byte, s string) {
	_ = w.bw.WriteByte(prefix)
	if strings.ContainsAny(s, "\r\n") {
		s = strings.Map(func(r rune) rune {
			if r == '\r' || r == '\n' {
				return ' '
			}
			return r
		}, s)
	}
	_, _ = w.bw.WriteString(s)
	w.crlf()
}

func (w *Writer) header(prefix byte, n int64) {
	_ = w.bw.WriteByte(prefix)
	w.scratch = strconv.AppendInt(w.scratch[:0], n, 10)
	_, _ = w.bw.Write(w.scratch)
	w.crlf()
}

func (w *Writer) crlf() { _, _ = w.bw.WriteString("\r\n") }
