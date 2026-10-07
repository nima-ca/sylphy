package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
	"slices"
)

// DefaultBufferSize is the default bufio buffer size for Reader and Writer.
const DefaultBufferSize = 8 * 1024

// bulkChunk bounds how much memory we commit per read step for large bulk
// payloads, so a client that merely *claims* a huge length cannot make us
// allocate it up front.
const bulkChunk = 1 << 20

// Limits bounds what Reader accepts. Zero or negative fields fall back to the
// defaults from DefaultLimits.
type Limits struct {
	// MaxBulkLen is the maximum bulk string payload size in bytes.
	MaxBulkLen int64
	// MaxArrayLen is the maximum number of elements in one array.
	MaxArrayLen int64
	// MaxInlineLen is the maximum length of an inline command or RESP line.
	MaxInlineLen int
	// MaxDepth is the maximum array nesting depth.
	MaxDepth int
}

// DefaultLimits returns the default safety limits (512 MiB bulk, 1 Mi array
// elements, 64 KiB lines, depth 32).
func DefaultLimits() Limits {
	return Limits{
		MaxBulkLen:   512 << 20,
		MaxArrayLen:  1024 * 1024,
		MaxInlineLen: 64 << 10,
		MaxDepth:     32,
	}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxBulkLen <= 0 {
		l.MaxBulkLen = d.MaxBulkLen
	}
	if l.MaxArrayLen <= 0 {
		l.MaxArrayLen = d.MaxArrayLen
	}
	if l.MaxInlineLen <= 0 {
		l.MaxInlineLen = d.MaxInlineLen
	}
	if l.MaxDepth <= 0 {
		l.MaxDepth = d.MaxDepth
	}
	return l
}

// Kind identifies a RESP2 value type by its leading byte.
type Kind byte

// RESP2 value kinds.
const (
	KindSimpleString Kind = '+'
	KindError        Kind = '-'
	KindInteger      Kind = ':'
	KindBulk         Kind = '$'
	KindArray        Kind = '*'
)

// Value is a parsed RESP2 value.
type Value struct {
	// Kind is the RESP type.
	Kind Kind
	// Str holds the payload of simple strings, errors and bulk strings.
	Str []byte
	// Int holds the value of integers.
	Int int64
	// Array holds the elements of arrays.
	Array []Value
	// Null is true for the null bulk string ($-1) and null array (*-1).
	Null bool
}

// Reader parses RESP2 from a buffered stream. It is not safe for concurrent
// use; each connection owns exactly one Reader.
type Reader struct {
	br  *bufio.Reader
	lim Limits
}

// NewReader returns a Reader over r with DefaultBufferSize.
func NewReader(r io.Reader, lim Limits) *Reader {
	return NewReaderSize(r, DefaultBufferSize, lim)
}

// NewReaderSize returns a Reader over r with the given buffer size.
func NewReaderSize(r io.Reader, size int, lim Limits) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, size), lim: lim.withDefaults()}
}

// Buffered returns the number of unread bytes already in the buffer. The server
// uses it to decide when a pipelined batch is finished and replies can flush.
func (r *Reader) Buffered() int { return r.br.Buffered() }

// ReadValue reads one RESP2 value of any type. It returns io.EOF only when the
// stream ended cleanly before the first byte of a value; any truncation
// afterwards is io.ErrUnexpectedEOF.
func (r *Reader) ReadValue() (Value, error) {
	t, err := r.br.ReadByte()
	if err != nil {
		return Value{}, err
	}
	v, err := r.readTyped(t, 0)
	if err != nil {
		return Value{}, eofToUnexpected(err)
	}
	return v, nil
}

// ReadCommand reads one client request and returns its arguments (including the
// command name). It accepts RESP arrays of bulk strings and inline commands.
// Empty lines and empty arrays are skipped. Errors follow the same EOF rules as
// ReadValue. The returned slices are owned by the caller.
func (r *Reader) ReadCommand() ([][]byte, error) {
	for {
		b, err := r.br.Peek(1)
		if err != nil {
			return nil, err
		}
		if b[0] != '*' {
			args, err := r.readInline()
			if err != nil {
				return nil, eofToUnexpected(err)
			}
			if len(args) == 0 {
				continue
			}
			return args, nil
		}
		_, _ = r.br.Discard(1)
		args, err := r.readMultibulk()
		if err != nil {
			return nil, eofToUnexpected(err)
		}
		if args == nil {
			continue
		}
		return args, nil
	}
}

func eofToUnexpected(err error) error {
	if errors.Is(err, io.EOF) {
		return io.ErrUnexpectedEOF
	}
	return err
}

func (r *Reader) readTyped(t byte, depth int) (Value, error) {
	switch Kind(t) {
	case KindSimpleString, KindError:
		line, err := r.readStrictLine()
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: Kind(t), Str: bytes.Clone(line)}, nil

	case KindInteger:
		line, err := r.readStrictLine()
		if err != nil {
			return Value{}, err
		}
		n, ok := parseInt(line)
		if !ok {
			return Value{}, protoErr("invalid integer")
		}
		return Value{Kind: KindInteger, Int: n}, nil

	case KindBulk:
		n, err := r.readLength("invalid bulk length")
		if err != nil {
			return Value{}, err
		}
		switch {
		case n == -1:
			return Value{Kind: KindBulk, Null: true}, nil
		case n < 0:
			return Value{}, protoErr("invalid bulk length")
		case n > r.lim.MaxBulkLen:
			return Value{}, limitErr("bulk length exceeds limit")
		}
		payload, err := r.readBulkPayload(n)
		if err != nil {
			return Value{}, err
		}
		return Value{Kind: KindBulk, Str: payload}, nil

	case KindArray:
		n, err := r.readLength("invalid multibulk length")
		if err != nil {
			return Value{}, err
		}
		switch {
		case n == -1:
			return Value{Kind: KindArray, Null: true}, nil
		case n < 0:
			return Value{}, protoErr("invalid multibulk length")
		case n > r.lim.MaxArrayLen:
			return Value{}, limitErr("array length exceeds limit")
		case depth >= r.lim.MaxDepth:
			return Value{}, limitErr("array nesting too deep")
		}
		// Capacity is capped: memory must follow data actually received, not the
		// length prefix.
		arr := make([]Value, 0, min(n, 64))
		for i := int64(0); i < n; i++ {
			et, err := r.br.ReadByte()
			if err != nil {
				return Value{}, err
			}
			v, err := r.readTyped(et, depth+1)
			if err != nil {
				return Value{}, err
			}
			arr = append(arr, v)
		}
		return Value{Kind: KindArray, Array: arr}, nil

	default:
		return Value{}, protoErr(fmt.Sprintf("invalid type byte %q", rune(t)))
	}
}

// readMultibulk parses the body of a request array (the '*' is consumed). It
// returns (nil, nil) for empty/null arrays, which callers skip.
func (r *Reader) readMultibulk() ([][]byte, error) {
	n, err := r.readLength("invalid multibulk length")
	if err != nil {
		return nil, err
	}
	switch {
	case n == -1 || n == 0:
		return nil, nil
	case n < 0:
		return nil, protoErr("invalid multibulk length")
	case n > r.lim.MaxArrayLen:
		return nil, limitErr("array length exceeds limit")
	}
	args := make([][]byte, 0, min(n, 64))
	for i := int64(0); i < n; i++ {
		t, err := r.br.ReadByte()
		if err != nil {
			return nil, err
		}
		if t != '$' {
			return nil, protoErr(fmt.Sprintf("expected '$', got %q", rune(t)))
		}
		bl, err := r.readLength("invalid bulk length")
		if err != nil {
			return nil, err
		}
		if bl < 0 {
			return nil, protoErr("invalid bulk length")
		}
		if bl > r.lim.MaxBulkLen {
			return nil, limitErr("bulk length exceeds limit")
		}
		p, err := r.readBulkPayload(bl)
		if err != nil {
			return nil, err
		}
		args = append(args, p)
	}
	return args, nil
}

// readInline parses a space-separated command line (telnet/netcat friendly).
// Quoting is intentionally unsupported in Phase 1.
func (r *Reader) readInline() ([][]byte, error) {
	line, _, err := r.readLine(r.lim.MaxInlineLen)
	if err != nil {
		return nil, err
	}
	fields := bytes.Fields(line)
	if len(fields) == 0 {
		return nil, nil
	}
	out := make([][]byte, len(fields))
	for i, f := range fields {
		out[i] = bytes.Clone(f)
	}
	return out, nil
}

func (r *Reader) readLength(msg string) (int64, error) {
	line, err := r.readStrictLine()
	if err != nil {
		return 0, err
	}
	n, ok := parseInt(line)
	if !ok {
		return 0, protoErr(msg)
	}
	return n, nil
}

// readBulkPayload reads exactly n bytes plus the trailing CRLF. Buffers grow in
// bulkChunk steps so a lying length prefix costs only what the peer really
// sends. Payloads up to bulkChunk are allocated exactly once.
func (r *Reader) readBulkPayload(n int64) ([]byte, error) {
	buf := make([]byte, 0, min(n, bulkChunk))
	for int64(len(buf)) < n {
		step := int(min(n-int64(len(buf)), bulkChunk))
		old := len(buf)
		buf = slices.Grow(buf, step)[:old+step]
		if _, err := io.ReadFull(r.br, buf[old:]); err != nil {
			return nil, err
		}
	}
	b, err := r.br.Peek(2)
	if err != nil {
		return nil, err
	}
	if b[0] != '\r' || b[1] != '\n' {
		return nil, protoErr("bulk payload not terminated by CRLF")
	}
	_, _ = r.br.Discard(2)
	return buf, nil
}

// readStrictLine reads a line that must end in CRLF (RESP headers and simple
// types). The returned slice is only valid until the next read.
func (r *Reader) readStrictLine() ([]byte, error) {
	line, crlf, err := r.readLine(r.lim.MaxInlineLen)
	if err != nil {
		return nil, err
	}
	if !crlf {
		return nil, protoErr("line not terminated by CRLF")
	}
	return line, nil
}

// readLine reads up to and including '\n', enforcing max on the content length.
// It reports whether the terminator was CRLF (true) or a bare LF (false). The
// result is only valid until the next read. Returns io.EOF only if no byte was
// read.
func (r *Reader) readLine(max int) ([]byte, bool, error) {
	var acc []byte
	for {
		chunk, err := r.br.ReadSlice('\n')
		if len(acc)+len(chunk) > max+2 {
			return nil, false, limitErr("line too long")
		}
		if errors.Is(err, bufio.ErrBufferFull) {
			acc = append(acc, chunk...)
			continue
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(acc)+len(chunk) > 0 {
				err = io.ErrUnexpectedEOF
			}
			return nil, false, err
		}
		if acc != nil {
			acc = append(acc, chunk...)
			chunk = acc
		}
		chunk = chunk[:len(chunk)-1]
		if n := len(chunk); n > 0 && chunk[n-1] == '\r' {
			return chunk[:n-1], true, nil
		}
		return chunk, false, nil
	}
}

// parseInt parses a signed decimal int64 without allocating.
func parseInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	neg := false
	i := 0
	if b[0] == '-' {
		neg = true
		i = 1
		if len(b) == 1 {
			return 0, false
		}
	}
	var n uint64
	for ; i < len(b); i++ {
		d := b[i] - '0'
		if d > 9 {
			return 0, false
		}
		if n > (math.MaxUint64-uint64(d))/10 {
			return 0, false
		}
		n = n*10 + uint64(d)
	}
	if neg {
		if n > 1<<63 {
			return 0, false
		}
		return -int64(n), true
	}
	if n > math.MaxInt64 {
		return 0, false
	}
	return int64(n), true
}
