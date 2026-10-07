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
)

// WrongArgs builds the "wrong number of arguments" error for a command name.
func WrongArgs(name string) *ReplyError {
	return &ReplyError{Msg: "ERR wrong number of arguments for '" + strings.ToLower(name) + "' command"}
}
