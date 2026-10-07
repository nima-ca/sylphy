// Package protocol implements the RESP2 wire protocol used by Redis.
//
// Reader parses client requests (RESP arrays of bulk strings, or inline
// commands for telnet/netcat) and, for tests and future replication, any RESP2
// value. Writer encodes replies. Both are designed for pipelining: Reader
// exposes how many bytes are buffered and Writer only hits the socket on Flush
// or when its buffer fills.
//
// Malformed input never panics; it yields an *Error that matches ErrProtocol or
// ErrLimitExceeded via errors.Is.
package protocol
