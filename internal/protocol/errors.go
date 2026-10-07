package protocol

import "errors"

var (
	// ErrProtocol marks malformed RESP input.
	ErrProtocol = errors.New("protocol error")
	// ErrLimitExceeded marks input that exceeds a configured Limits value.
	ErrLimitExceeded = errors.New("limit exceeded")
)

// Error is the concrete error returned by Reader for invalid input. It wraps
// either ErrProtocol or ErrLimitExceeded, so callers can use errors.Is.
type Error struct {
	// Kind is ErrProtocol or ErrLimitExceeded.
	Kind error
	// Msg is a short, client-safe description (no payload bytes).
	Msg string
}

// Error implements the error interface.
func (e *Error) Error() string { return e.Msg }

// Unwrap returns the sentinel error this Error belongs to.
func (e *Error) Unwrap() error { return e.Kind }

func protoErr(msg string) error { return &Error{Kind: ErrProtocol, Msg: msg} }
func limitErr(msg string) error { return &Error{Kind: ErrLimitExceeded, Msg: msg} }
