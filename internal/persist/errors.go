package persist

import (
	"errors"
	"fmt"
)

// Sentinel errors. Use errors.Is: decoders wrap them in a *DecodeError.
var (
	// ErrCorrupt marks persisted bytes that are structurally invalid.
	ErrCorrupt = errors.New("persist: corrupt data")
	// ErrChecksum marks a CRC mismatch. It is also an ErrCorrupt.
	ErrChecksum = fmt.Errorf("%w: checksum mismatch", ErrCorrupt)
	// ErrTruncated marks input that ends in the middle of a structure. It is
	// also an ErrCorrupt; for the last AOF segment it is the repairable case.
	ErrTruncated = fmt.Errorf("%w: unexpected end of data", ErrCorrupt)
	// ErrVersion marks a format version this build does not understand.
	ErrVersion = errors.New("persist: unsupported format version")
	// ErrUnsupportedValue is returned when a value cannot be encoded: an
	// unknown Value type, an empty collection or a non-positive deadline.
	ErrUnsupportedValue = errors.New("persist: value cannot be persisted")
	// ErrEntryTooLarge is returned when one encoded entry would not fit in a
	// snapshot block (MaxBlockPayload).
	ErrEntryTooLarge = errors.New("persist: entry too large for a snapshot block")
)

// DecodeError reports where and why persisted bytes could not be decoded.
type DecodeError struct {
	// Offset is the byte offset, within the decoded stream, of the structure
	// that failed: the start of a record or block for truncation, the
	// offending byte for a malformed field.
	Offset int64
	// Err is ErrCorrupt, ErrChecksum, ErrTruncated or ErrVersion.
	Err error
	// Msg describes the problem.
	Msg string
}

// Error implements the error interface.
func (e *DecodeError) Error() string {
	if e.Msg == "" {
		return fmt.Sprintf("at offset %d: %v", e.Offset, e.Err)
	}
	return fmt.Sprintf("at offset %d: %s: %v", e.Offset, e.Msg, e.Err)
}

// Unwrap returns the sentinel error.
func (e *DecodeError) Unwrap() error { return e.Err }

func decodeErr(off int64, base error, format string, a ...any) error {
	return &DecodeError{Offset: off, Err: base, Msg: fmt.Sprintf(format, a...)}
}
