package persist

import (
	"bufio"
	"errors"
	"io"
	"slices"
	"strconv"
)

// Defaults for RecordLimits, matching the protocol reader's.
const (
	// DefaultMaxRecordArgs is the default maximum number of arguments per record.
	DefaultMaxRecordArgs = 1 << 20
	// DefaultMaxBulkLen is the default maximum size of one argument.
	DefaultMaxBulkLen = 512 << 20

	recordReadBuf = 64 << 10
	bulkChunk     = 1 << 20 // memory committed per read step for large arguments
)

// AppendRecord appends one AOF record, a RESP array of bulk strings, to dst.
// argv must not be empty.
func AppendRecord(dst []byte, argv [][]byte) []byte {
	dst = append(dst, '*')
	dst = strconv.AppendInt(dst, int64(len(argv)), 10)
	dst = append(dst, '\r', '\n')
	for _, a := range argv {
		dst = append(dst, '$')
		dst = strconv.AppendInt(dst, int64(len(a)), 10)
		dst = append(dst, '\r', '\n')
		dst = append(dst, a...)
		dst = append(dst, '\r', '\n')
	}
	return dst
}

// RecordLimits bounds what RecordReader accepts. Zero fields select defaults.
type RecordLimits struct {
	// MaxArgs is the maximum number of arguments in one record.
	MaxArgs int
	// MaxBulkLen is the maximum size of one argument in bytes.
	MaxBulkLen int64
}

func (l RecordLimits) withDefaults() RecordLimits {
	if l.MaxArgs <= 0 {
		l.MaxArgs = DefaultMaxRecordArgs
	}
	if l.MaxBulkLen <= 0 {
		l.MaxBulkLen = DefaultMaxBulkLen
	}
	return l
}

// RecordReader decodes AOF records. It is stricter than the network reader:
// only RESP arrays of bulk strings, canonical decimal lengths, no inline
// commands, no null bulks, no empty arrays. A record that decodes therefore
// re-encodes (AppendRecord) to exactly the bytes it was read from.
//
// Errors are *DecodeError. A stream that ends in the middle of a record is
// ErrTruncated with Offset at the start of that record (the repairable torn
// write at the end of the last segment); anything unparsable is ErrCorrupt
// with Offset at the offending byte. A zero-filled tail is therefore corrupt,
// not truncated. After an error the reader keeps returning it. Other I/O
// errors from the source are returned unchanged.
type RecordReader struct {
	br  *bufio.Reader
	lim RecordLimits
	pos int64 // bytes consumed
	off int64 // end of the last complete record
	err error // sticky
}

// NewRecordReader returns a RecordReader over r.
func NewRecordReader(r io.Reader, lim RecordLimits) *RecordReader {
	return &RecordReader{br: bufio.NewReaderSize(r, recordReadBuf), lim: lim.withDefaults()}
}

// Offset returns the stream offset just past the last complete record, which is
// where a torn tail begins.
func (r *RecordReader) Offset() int64 { return r.off }

// Next returns the next record's arguments, owned by the caller. It returns
// io.EOF only at a clean record boundary.
func (r *RecordReader) Next() ([][]byte, error) {
	if r.err != nil {
		return nil, r.err
	}
	start := r.pos
	b, err := r.br.ReadByte()
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, io.EOF
		}
		r.err = err
		return nil, err
	}
	r.pos++
	argv, err := r.parse(b, start)
	if err != nil {
		r.err = err
		return nil, err
	}
	r.off = r.pos
	return argv, nil
}

func (r *RecordReader) parse(first byte, start int64) ([][]byte, error) {
	if first != '*' {
		return nil, decodeErr(start, ErrCorrupt, "record does not start with '*' (got 0x%02x)", first)
	}
	n, err := r.readCount(start, "argument count")
	if err != nil {
		return nil, err
	}
	if n < 1 || n > int64(r.lim.MaxArgs) {
		return nil, decodeErr(start, ErrCorrupt, "argument count %d out of range", n)
	}
	argv := make([][]byte, 0, min(n, 64)) // memory follows data actually read
	for range n {
		at := r.pos
		c, err := r.readByte(start)
		if err != nil {
			return nil, err
		}
		if c != '$' {
			return nil, decodeErr(at, ErrCorrupt, "expected '$', got 0x%02x", c)
		}
		l, err := r.readCount(start, "bulk length")
		if err != nil {
			return nil, err
		}
		if l > r.lim.MaxBulkLen {
			return nil, decodeErr(at, ErrCorrupt, "bulk length %d exceeds limit", l)
		}
		p, err := r.readBulk(l, start)
		if err != nil {
			return nil, err
		}
		argv = append(argv, p)
	}
	return argv, nil
}

// eof turns end-of-input inside a record into ErrTruncated at the record start.
func (r *RecordReader) eof(err error, recStart int64) error {
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return decodeErr(recStart, ErrTruncated, "record cut short after %d bytes", r.pos-recStart)
	}
	return err
}

func (r *RecordReader) readByte(recStart int64) (byte, error) {
	b, err := r.br.ReadByte()
	if err != nil {
		return 0, r.eof(err, recStart)
	}
	r.pos++
	return b, nil
}

// readCount reads a canonical non-negative decimal followed by CRLF.
func (r *RecordReader) readCount(recStart int64, what string) (int64, error) {
	var n int64
	digits := 0
	var first byte
	for {
		c, err := r.readByte(recStart)
		if err != nil {
			return 0, err
		}
		if c == '\r' {
			break
		}
		if c < '0' || c > '9' {
			return 0, decodeErr(r.pos-1, ErrCorrupt, "invalid byte 0x%02x in %s", c, what)
		}
		if digits == 0 {
			first = c
		} else if first == '0' {
			return 0, decodeErr(r.pos-1, ErrCorrupt, "leading zero in %s", what)
		}
		digits++
		if digits > 18 {
			return 0, decodeErr(r.pos-1, ErrCorrupt, "%s too long", what)
		}
		n = n*10 + int64(c-'0')
	}
	if digits == 0 {
		return 0, decodeErr(r.pos-1, ErrCorrupt, "empty %s", what)
	}
	c, err := r.readByte(recStart)
	if err != nil {
		return 0, err
	}
	if c != '\n' {
		return 0, decodeErr(r.pos-1, ErrCorrupt, "expected LF after CR in %s", what)
	}
	return n, nil
}

// readBulk reads l payload bytes and the CRLF after them. The buffer grows in
// bulkChunk steps, so a lying length costs only what the file really holds.
func (r *RecordReader) readBulk(l, recStart int64) ([]byte, error) {
	buf := make([]byte, 0, min(l, bulkChunk))
	for int64(len(buf)) < l {
		step := int(min(l-int64(len(buf)), bulkChunk))
		old := len(buf)
		buf = slices.Grow(buf, step)[:old+step]
		n, err := io.ReadFull(r.br, buf[old:])
		r.pos += int64(n)
		if err != nil {
			return nil, r.eof(err, recStart)
		}
	}
	for _, want := range [2]byte{'\r', '\n'} {
		c, err := r.readByte(recStart)
		if err != nil {
			return nil, err
		}
		if c != want {
			return nil, decodeErr(r.pos-1, ErrCorrupt, "bulk payload not terminated by CRLF")
		}
	}
	return buf, nil
}
