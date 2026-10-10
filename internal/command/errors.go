package command

import "strings"

// ReplyError is an error whose Msg is sent to the client verbatim as a RESP
// error. Any other error type returned by a handler is reported as a generic
// internal error so implementation details never leak.
type ReplyError struct {
	// Msg is the full error line, including the Redis error prefix (e.g. "ERR").
	Msg string
}

// Error implements the error interface.
func (e *ReplyError) Error() string { return e.Msg }

// Redis-compatible error replies.
var (
	// ErrNotInteger is returned when a value is not a valid int64.
	ErrNotInteger = &ReplyError{Msg: "ERR value is not an integer or out of range"}
	// ErrOverflow is returned when INCR/DECR family arithmetic would overflow.
	ErrOverflow = &ReplyError{Msg: "ERR increment or decrement would overflow"}
	// ErrSyntax is returned for unsupported or malformed options.
	ErrSyntax = &ReplyError{Msg: "ERR syntax error"}
	// ErrNotFloat is returned when a value is not a valid float.
	ErrNotFloat = &ReplyError{Msg: "ERR value is not a valid float"}
	// ErrMustBePositive is returned when a count or size must be positive.
	ErrMustBePositive = &ReplyError{Msg: "ERR value is out of range, must be positive"}
	// ErrNoSuchKey is returned by commands that require an existing key (LSET,
	// RENAME).
	ErrNoSuchKey = &ReplyError{Msg: "ERR no such key"}
	// ErrIndexOutOfRange is returned when an index points outside a list.
	ErrIndexOutOfRange = &ReplyError{Msg: "ERR index out of range"}
	// ErrWrongType is sent when a command meets a key of the wrong kind. The
	// dispatcher maps store.ErrWrongType to it.
	ErrWrongType = &ReplyError{Msg: "WRONGTYPE Operation against a key holding the wrong kind of value"}
)

// WrongArgs builds the "wrong number of arguments" error for a command name.
func WrongArgs(name string) *ReplyError {
	return &ReplyError{Msg: "ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"}
}

// InvalidExpireTime builds the error for an unusable TTL or deadline. name is
// the command name in any case.
func InvalidExpireTime(name string) *ReplyError {
	return &ReplyError{Msg: "ERR invalid expire time in '" + strings.ToLower(name) + "' command"}
}
