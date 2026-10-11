package persist

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"slices"

	"github.com/nima-ca/sylphy/internal/hash"
	"github.com/nima-ca/sylphy/internal/list"
	"github.com/nima-ca/sylphy/internal/set"
	"github.com/nima-ca/sylphy/internal/store"
	"github.com/nima-ca/sylphy/internal/zset"
)

// Snapshot file format, version 1. All integers are little-endian; CRC is
// CRC-32C (Castagnoli). Nothing depends on the shard count or on the order of
// keys, so a snapshot loads into a store with any number of shards.
//
//	file    = header block* end
//
//	header  (20 bytes)
//	  0   8  magic     "SYLPHYSN"
//	  8   2  version   1
//	  10  2  flags     0 (must be zero)
//	  12  4  reserved  0 (must be zero)
//	  16  4  CRC of bytes 0..15
//
//	block   (13 bytes of framing + payload)
//	  0   1  tag       0x01
//	  1   4  count     entries in the payload (>= 1)
//	  5   4  length    payload bytes (<= MaxBlockPayload)
//	  9   n  payload   `count` entries back to back
//	  9+n 4  CRC of tag, count, length and payload
//
//	end     (17 bytes)
//	  0   1  tag       0xFF
//	  1   8  entries   total entries in the file
//	  9   4  blocks    total blocks in the file
//	  13  4  CRC of every byte of the file before this field
//
// The file ends right after the end record. The whole-file CRC is also what
// the manifest records as BaseRef.CRC.
//
//	entry
//	  type     1   1 string, 2 list, 3 hash, 4 set, 5 zset
//	  flags    1   bit 0: has a deadline; other bits must be zero
//	  key      uvarint length, then the bytes
//	  deadline 8   absolute Unix ms as int64 (present only if flags bit 0); > 0
//	  payload  by type:
//	    string  uvarint length, bytes
//	    list    uvarint n >= 1, then n elements, head to tail
//	    hash    uvarint n >= 1, then n (field, value) pairs in iteration order
//	    set     uvarint n >= 1, then n members
//	    zset    uvarint n >= 1, then n (score, member) pairs in ascending rank
//	    where every element, field, value and member is a uvarint length plus
//	    bytes, and a score is 8 bytes, the IEEE-754 bits of the float64
//	    (so every score, including -0 and the infinities, round-trips exactly).
//
// Empty collections, duplicate fields or members, NaN scores and non-positive
// deadlines never occur in a valid file and are rejected as corrupt.
const (
	// SnapshotVersion is the snapshot format version this build reads and writes.
	SnapshotVersion = 1
	// TargetBlockSize is the payload size at which BlockBuffer closes a block.
	TargetBlockSize = 256 << 10
	// MaxBlockPayload is the largest block payload; an entry that would push a
	// block past it cannot be snapshotted.
	MaxBlockPayload = 1 << 30

	snapMagic      = "SYLPHYSN"
	snapHeaderLen  = 20
	blockHeaderLen = 9

	tagBlock = 0x01
	tagEnd   = 0xFF

	typeString = 1
	typeList   = 2
	typeHash   = 3
	typeSet    = 4
	typeZSet   = 5

	flagDeadline = 0x01
)

// SnapshotEntry is one key with its value and optional absolute deadline.
type SnapshotEntry struct {
	// Key is the key.
	Key string
	// Value is the value; the decoder returns fresh, unshared objects.
	Value store.Value
	// DeadlineMs is the absolute Unix-ms deadline, valid when HasDeadline.
	DeadlineMs int64
	// HasDeadline reports that the key expires.
	HasDeadline bool
}

func appendSnapshotHeader(dst []byte) []byte {
	start := len(dst)
	dst = append(dst, snapMagic...)
	dst = binary.LittleEndian.AppendUint16(dst, SnapshotVersion)
	dst = binary.LittleEndian.AppendUint16(dst, 0)
	dst = binary.LittleEndian.AppendUint32(dst, 0)
	return binary.LittleEndian.AppendUint32(dst, crc32.Checksum(dst[start:], castagnoli))
}

func appendBytes(dst, b []byte) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(b)))
	return append(dst, b...)
}

func appendString(dst []byte, s string) []byte {
	dst = binary.AppendUvarint(dst, uint64(len(s)))
	return append(dst, s...)
}

// AppendEntry appends the encoding of one entry to dst. On error dst is
// returned unchanged in length. It is meant to run under the shard lock: it
// reads the value in place and copies it into dst.
func AppendEntry(dst []byte, key string, v store.Value, deadlineMs int64, hasDeadline bool) ([]byte, error) {
	mark := len(dst)
	dst, err := appendEntry(dst, key, v, deadlineMs, hasDeadline)
	if err != nil {
		return dst[:mark], err
	}
	return dst, nil
}

func appendHead(dst []byte, typ byte, key string, deadlineMs int64, has bool) []byte {
	flags := byte(0)
	if has {
		flags = flagDeadline
	}
	dst = append(dst, typ, flags)
	dst = appendString(dst, key)
	if has {
		dst = binary.LittleEndian.AppendUint64(dst, uint64(deadlineMs))
	}
	return dst
}

func appendEntry(dst []byte, key string, v store.Value, deadlineMs int64, has bool) ([]byte, error) {
	if has && deadlineMs <= 0 {
		return dst, fmt.Errorf("%w: deadline %d is not positive", ErrUnsupportedValue, deadlineMs)
	}
	switch val := v.(type) {
	case *store.String:
		dst = appendHead(dst, typeString, key, deadlineMs, has)
		return appendBytes(dst, val.Bytes()), nil
	case *list.List:
		n := val.Len()
		if n == 0 {
			return dst, fmt.Errorf("%w: empty list", ErrUnsupportedValue)
		}
		dst = appendHead(dst, typeList, key, deadlineMs, has)
		dst = binary.AppendUvarint(dst, uint64(n))
		for i := range n {
			b, _ := val.At(i) // in range; valid under the shard lock
			dst = appendBytes(dst, b)
		}
		return dst, nil
	case *hash.Hash:
		n := val.Len()
		if n == 0 {
			return dst, fmt.Errorf("%w: empty hash", ErrUnsupportedValue)
		}
		dst = appendHead(dst, typeHash, key, deadlineMs, has)
		dst = binary.AppendUvarint(dst, uint64(n))
		for _, s := range val.Flat() { // field, value, field, value, ...
			dst = appendString(dst, s)
		}
		return dst, nil
	case *set.Set:
		n := val.Len()
		if n == 0 {
			return dst, fmt.Errorf("%w: empty set", ErrUnsupportedValue)
		}
		dst = appendHead(dst, typeSet, key, deadlineMs, has)
		dst = binary.AppendUvarint(dst, uint64(n))
		val.Range(func(m string) bool {
			dst = appendString(dst, m)
			return true
		})
		return dst, nil
	case *zset.ZSet:
		n := val.Len()
		if n == 0 {
			return dst, fmt.Errorf("%w: empty sorted set", ErrUnsupportedValue)
		}
		dst = appendHead(dst, typeZSet, key, deadlineMs, has)
		dst = binary.AppendUvarint(dst, uint64(n))
		for _, e := range val.RangeByRank(0, n, false) {
			dst = binary.LittleEndian.AppendUint64(dst, math.Float64bits(e.Score))
			dst = appendString(dst, e.Member)
		}
		return dst, nil
	default:
		return dst, fmt.Errorf("%w: %T", ErrUnsupportedValue, v)
	}
}

// BlockBuffer accumulates encoded entries and frames them into checksummed
// blocks, in one buffer. The store's per-shard encoder (run under the shard
// lock) feeds it with Add and hands Finish's result to SnapshotWriter. The zero
// value is ready to use.
type BlockBuffer struct {
	out     []byte
	open    bool
	hdr     int // offset of the open block's tag byte
	count   int // entries in the open block
	entries int
	blocks  int
}

// Add encodes one entry. On error nothing is added.
func (b *BlockBuffer) Add(key string, v store.Value, deadlineMs int64, hasDeadline bool) error {
	if !b.open {
		b.hdr = len(b.out)
		b.out = append(b.out, tagBlock, 0, 0, 0, 0, 0, 0, 0, 0)
		b.open = true
		b.count = 0
	}
	before := len(b.out)
	out, err := AppendEntry(b.out, key, v, deadlineMs, hasDeadline)
	b.out = out
	if err == nil && len(b.out)-b.hdr-blockHeaderLen > MaxBlockPayload {
		b.out = b.out[:before]
		err = fmt.Errorf("%w (%d-byte key)", ErrEntryTooLarge, len(key))
	}
	if err != nil {
		if b.count == 0 { // do not leave an empty block behind
			b.out = b.out[:b.hdr]
			b.open = false
		}
		return err
	}
	b.count++
	b.entries++
	if len(b.out)-b.hdr-blockHeaderLen >= TargetBlockSize {
		b.closeBlock()
	}
	return nil
}

func (b *BlockBuffer) closeBlock() {
	payload := len(b.out) - b.hdr - blockHeaderLen
	binary.LittleEndian.PutUint32(b.out[b.hdr+1:], uint32(b.count))
	binary.LittleEndian.PutUint32(b.out[b.hdr+5:], uint32(payload))
	b.out = binary.LittleEndian.AppendUint32(b.out, crc32.Checksum(b.out[b.hdr:], castagnoli))
	b.open = false
	b.count = 0
	b.blocks++
}

// Entries returns how many entries have been added.
func (b *BlockBuffer) Entries() int { return b.entries }

// Finish closes the open block and returns the framed bytes with the entry and
// block counts. The buffer is reset and the caller owns the result.
func (b *BlockBuffer) Finish() (framed []byte, entries, blocks int) {
	if b.open {
		if b.count > 0 {
			b.closeBlock()
		} else {
			b.out = b.out[:b.hdr]
		}
	}
	framed, entries, blocks = b.out, b.entries, b.blocks
	*b = BlockBuffer{}
	return framed, entries, blocks
}

// SnapshotWriter writes a snapshot file: the header, framed blocks and the end
// record. It does not sync or close the underlying writer. Errors are sticky.
type SnapshotWriter struct {
	w       io.Writer
	crc     uint32
	n       int64
	entries uint64
	blocks  uint32
	closed  bool
	err     error
}

// NewSnapshotWriter writes the header to w and returns the writer.
func NewSnapshotWriter(w io.Writer) (*SnapshotWriter, error) {
	sw := &SnapshotWriter{w: w}
	if err := sw.write(appendSnapshotHeader(nil), true); err != nil {
		return nil, err
	}
	return sw, nil
}

func (sw *SnapshotWriter) write(p []byte, inCRC bool) error {
	if sw.err != nil {
		return sw.err
	}
	n, err := sw.w.Write(p)
	if inCRC {
		sw.crc = crc32.Update(sw.crc, castagnoli, p[:n])
	}
	sw.n += int64(n)
	if err == nil && n < len(p) {
		err = io.ErrShortWrite
	}
	sw.err = err
	return err
}

// WriteFramed appends blocks produced by BlockBuffer.Finish, with the counts it
// returned. The bytes are written as they are; the reader verifies them.
func (sw *SnapshotWriter) WriteFramed(framed []byte, entries, blocks int) error {
	if sw.closed {
		return errors.New("persist: snapshot writer is closed")
	}
	if entries < 0 || blocks < 0 {
		return errors.New("persist: negative snapshot counts")
	}
	if err := sw.write(framed, true); err != nil {
		return err
	}
	sw.entries += uint64(entries)
	sw.blocks += uint32(blocks)
	return nil
}

// Close writes the end record and returns the file's size and whole-file CRC
// (Name is left empty for the caller to fill in).
func (sw *SnapshotWriter) Close() (BaseRef, error) {
	if sw.closed {
		return BaseRef{}, errors.New("persist: snapshot writer is closed")
	}
	sw.closed = true
	var e [13]byte
	e[0] = tagEnd
	binary.LittleEndian.PutUint64(e[1:], sw.entries)
	binary.LittleEndian.PutUint32(e[9:], sw.blocks)
	if err := sw.write(e[:], true); err != nil {
		return BaseRef{}, err
	}
	fileCRC := sw.crc
	var c [4]byte
	binary.LittleEndian.PutUint32(c[:], fileCRC)
	if err := sw.write(c[:], false); err != nil { // the checksum field is not part of itself
		return BaseRef{}, err
	}
	return BaseRef{Size: sw.n, CRC: fileCRC}, nil
}

// cursor reads fields from a verified block payload.
type cursor struct {
	b    []byte
	i    int
	base int64 // file offset of b[0]
}

func (c *cursor) remaining() int { return len(c.b) - c.i }

func (c *cursor) errf(format string, a ...any) error {
	return decodeErr(c.base+int64(c.i), ErrCorrupt, format, a...)
}

func (c *cursor) u8(what string) (byte, error) {
	if c.i >= len(c.b) {
		return 0, c.errf("%s: block ends inside an entry", what)
	}
	v := c.b[c.i]
	c.i++
	return v, nil
}

func (c *cursor) u64(what string) (uint64, error) {
	if c.remaining() < 8 {
		return 0, c.errf("%s: block ends inside an entry", what)
	}
	v := binary.LittleEndian.Uint64(c.b[c.i:])
	c.i += 8
	return v, nil
}

func (c *cursor) uvarint(what string) (uint64, error) {
	v, n := binary.Uvarint(c.b[c.i:])
	if n <= 0 {
		return 0, c.errf("%s: bad varint", what)
	}
	c.i += n
	return v, nil
}

// bytes reads a length-prefixed byte string, aliasing the payload.
func (c *cursor) bytes(what string) ([]byte, error) {
	n, err := c.uvarint(what)
	if err != nil {
		return nil, err
	}
	if n > uint64(c.remaining()) {
		return nil, c.errf("%s: length %d exceeds the %d bytes left in the block", what, n, c.remaining())
	}
	p := c.b[c.i : c.i+int(n)]
	c.i += int(n)
	return p, nil
}

// count reads a collection size. Every element takes at least one byte, so a
// size beyond the remaining bytes is rejected before anything is allocated.
func (c *cursor) count(what string) (int, error) {
	n, err := c.uvarint(what)
	if err != nil {
		return 0, err
	}
	if n == 0 {
		return 0, c.errf("empty collection (%s is zero)", what)
	}
	if n > uint64(c.remaining()) {
		return 0, c.errf("%s %d exceeds the %d bytes left in the block", what, n, c.remaining())
	}
	return int(n), nil
}

func (c *cursor) entry() (SnapshotEntry, error) {
	var e SnapshotEntry
	typ, err := c.u8("entry type")
	if err != nil {
		return e, err
	}
	flags, err := c.u8("entry flags")
	if err != nil {
		return e, err
	}
	if flags&^flagDeadline != 0 {
		return e, c.errf("unknown entry flags 0x%02x", flags)
	}
	key, err := c.bytes("key")
	if err != nil {
		return e, err
	}
	e.Key = string(key)
	if flags&flagDeadline != 0 {
		u, err := c.u64("deadline")
		if err != nil {
			return e, err
		}
		e.DeadlineMs, e.HasDeadline = int64(u), true
		if e.DeadlineMs <= 0 {
			return e, c.errf("non-positive deadline %d", e.DeadlineMs)
		}
	}
	switch typ {
	case typeString:
		b, err := c.bytes("string value")
		if err != nil {
			return e, err
		}
		e.Value = store.NewString(b)
	case typeList:
		n, err := c.count("list length")
		if err != nil {
			return e, err
		}
		l := list.New()
		for range n {
			b, err := c.bytes("list element")
			if err != nil {
				return e, err
			}
			l.PushBack(b)
		}
		e.Value = l
	case typeHash:
		n, err := c.count("hash length")
		if err != nil {
			return e, err
		}
		h := hash.New()
		for range n {
			f, err := c.bytes("hash field")
			if err != nil {
				return e, err
			}
			v, err := c.bytes("hash value")
			if err != nil {
				return e, err
			}
			if !h.Set(string(f), string(v)) {
				return e, c.errf("duplicate hash field")
			}
		}
		e.Value = h
	case typeSet:
		n, err := c.count("set size")
		if err != nil {
			return e, err
		}
		s := set.New()
		for range n {
			m, err := c.bytes("set member")
			if err != nil {
				return e, err
			}
			if !s.Add(string(m)) {
				return e, c.errf("duplicate set member")
			}
		}
		e.Value = s
	case typeZSet:
		n, err := c.count("zset size")
		if err != nil {
			return e, err
		}
		z := zset.New()
		for range n {
			bits, err := c.u64("zset score")
			if err != nil {
				return e, err
			}
			score := math.Float64frombits(bits)
			if math.IsNaN(score) {
				return e, c.errf("NaN zset score")
			}
			m, err := c.bytes("zset member")
			if err != nil {
				return e, err
			}
			if _, existed := z.Set(string(m), score); existed {
				return e, c.errf("duplicate zset member")
			}
		}
		e.Value = z
	default:
		return e, c.errf("unknown entry type %d", typ)
	}
	return e, nil
}

// SnapshotReader decodes a snapshot file as a stream of entries. Every block is
// checksum-verified before any of its entries is decoded, and the end record
// (entry and block counts, whole-file CRC, nothing after it) is verified before
// Next reports io.EOF, so a nil error from the whole stream means the file is
// intact. Errors are *DecodeError and sticky.
type SnapshotReader struct {
	br      *bufio.Reader
	pos     int64
	crc     uint32 // CRC of every byte consumed so far
	buf     []byte // payload of the current block
	cur     cursor
	left    int // entries left in the current block
	entries uint64
	blocks  uint32
	done    bool
	err     error
	res     BaseRef
}

// NewSnapshotReader reads and validates the header of the snapshot in r.
func NewSnapshotReader(r io.Reader) (*SnapshotReader, error) {
	sr := &SnapshotReader{br: bufio.NewReaderSize(r, 64<<10)}
	var h [snapHeaderLen]byte
	if err := sr.fill(h[:], "header"); err != nil {
		return nil, err
	}
	if string(h[:8]) != snapMagic {
		return nil, decodeErr(0, ErrCorrupt, "bad snapshot magic")
	}
	// The header checksum comes first, so a flipped version byte is corruption
	// and only an intact header of another version is ErrVersion.
	if crc32.Checksum(h[:16], castagnoli) != binary.LittleEndian.Uint32(h[16:]) {
		return nil, decodeErr(16, ErrChecksum, "snapshot header")
	}
	if v := binary.LittleEndian.Uint16(h[8:]); v != SnapshotVersion {
		return nil, &DecodeError{Offset: 8, Err: ErrVersion, Msg: fmt.Sprintf("snapshot version %d", v)}
	}
	if binary.LittleEndian.Uint16(h[10:]) != 0 || binary.LittleEndian.Uint32(h[12:]) != 0 {
		return nil, decodeErr(10, ErrCorrupt, "nonzero reserved header fields")
	}
	return sr, nil
}

// fill reads len(p) bytes, folding them into the whole-file CRC.
func (sr *SnapshotReader) fill(p []byte, what string) error {
	n, err := io.ReadFull(sr.br, p)
	sr.crc = crc32.Update(sr.crc, castagnoli, p[:n])
	sr.pos += int64(n)
	return sr.readErr(err, what)
}

// fillNoCRC reads len(p) bytes that are not part of the whole-file CRC.
func (sr *SnapshotReader) fillNoCRC(p []byte, what string) error {
	n, err := io.ReadFull(sr.br, p)
	sr.pos += int64(n)
	return sr.readErr(err, what)
}

func (sr *SnapshotReader) readErr(err error, what string) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
		return decodeErr(sr.pos, ErrTruncated, "reading %s", what)
	}
	return err
}

func (sr *SnapshotReader) readPayload(n int) ([]byte, error) {
	buf := sr.buf[:0]
	for len(buf) < n {
		step := min(n-len(buf), bulkChunk) // memory follows data actually read
		old := len(buf)
		buf = slices.Grow(buf, step)[:old+step]
		if err := sr.fill(buf[old:], "block payload"); err != nil {
			sr.buf = buf[:0]
			return nil, err
		}
	}
	sr.buf = buf
	return buf, nil
}

// Next returns the next entry, or io.EOF after the verified end record.
func (sr *SnapshotReader) Next() (SnapshotEntry, error) {
	for {
		if sr.err != nil {
			return SnapshotEntry{}, sr.err
		}
		if sr.left > 0 {
			e, err := sr.cur.entry()
			if err == nil {
				sr.left--
				if sr.left == 0 && sr.cur.remaining() != 0 {
					err = sr.cur.errf("%d unused bytes after the last entry of the block", sr.cur.remaining())
				}
			}
			if err != nil {
				sr.err = err
				return SnapshotEntry{}, err
			}
			return e, nil
		}
		if sr.done {
			return SnapshotEntry{}, io.EOF
		}
		if err := sr.nextBlock(); err != nil {
			sr.err = err
			return SnapshotEntry{}, err
		}
	}
}

func (sr *SnapshotReader) nextBlock() error {
	start := sr.pos
	var tag [1]byte
	if err := sr.fill(tag[:], "block tag"); err != nil {
		return err
	}
	switch tag[0] {
	case tagEnd:
		return sr.readEnd(start)
	case tagBlock:
	default:
		return decodeErr(start, ErrCorrupt, "unknown block tag 0x%02x", tag[0])
	}
	var h [blockHeaderLen]byte
	h[0] = tagBlock
	if err := sr.fill(h[1:], "block header"); err != nil {
		return err
	}
	count := binary.LittleEndian.Uint32(h[1:])
	length := binary.LittleEndian.Uint32(h[5:])
	switch {
	case count == 0:
		return decodeErr(start, ErrCorrupt, "block with no entries")
	case length > MaxBlockPayload:
		return decodeErr(start, ErrCorrupt, "block length %d exceeds the maximum", length)
	case uint64(length) < uint64(count)*4: // an entry takes at least 4 bytes
		return decodeErr(start, ErrCorrupt, "block length %d too small for %d entries", length, count)
	}
	payload, err := sr.readPayload(int(length))
	if err != nil {
		return err
	}
	var c [4]byte
	if err := sr.fill(c[:], "block checksum"); err != nil {
		return err
	}
	want := crc32.Update(crc32.Checksum(h[:], castagnoli), castagnoli, payload)
	if want != binary.LittleEndian.Uint32(c[:]) {
		return decodeErr(start, ErrChecksum, "block")
	}
	sr.cur = cursor{b: payload, base: start + blockHeaderLen}
	sr.left = int(count)
	sr.blocks++
	sr.entries += uint64(count)
	return nil
}

func (sr *SnapshotReader) readEnd(start int64) error {
	var e [12]byte
	if err := sr.fill(e[:], "end record"); err != nil {
		return err
	}
	fileCRC := sr.crc
	var c [4]byte
	if err := sr.fillNoCRC(c[:], "file checksum"); err != nil {
		return err
	}
	if binary.LittleEndian.Uint32(c[:]) != fileCRC {
		return decodeErr(start, ErrChecksum, "snapshot file")
	}
	total, blocks := binary.LittleEndian.Uint64(e[:8]), binary.LittleEndian.Uint32(e[8:])
	if total != sr.entries || blocks != sr.blocks {
		return decodeErr(start, ErrCorrupt, "end record counts %d entries/%d blocks do not match the %d/%d read",
			total, blocks, sr.entries, sr.blocks)
	}
	if _, err := sr.br.ReadByte(); err == nil {
		return decodeErr(sr.pos, ErrCorrupt, "trailing data after the end record")
	} else if !errors.Is(err, io.EOF) {
		return err
	}
	sr.done = true
	sr.res = BaseRef{Size: sr.pos, CRC: fileCRC}
	return nil
}

// Entries returns how many entries have been read so far.
func (sr *SnapshotReader) Entries() uint64 { return sr.entries }

// Result returns the file's size and whole-file CRC (Name empty). ok is false
// until Next has returned io.EOF.
func (sr *SnapshotReader) Result() (ref BaseRef, ok bool) { return sr.res, sr.done }
