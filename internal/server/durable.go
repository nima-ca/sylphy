package server

import (
	"context"
	"io"

	"github.com/nima-ca/sylphy/internal/command"
)

// durableWriter sits between a connection's protocol.Writer and its socket when
// fsync is "always". Before any bytes reach the socket it waits until the newest
// log record the connection appended is durable. Putting the gate on the
// io.Writer, rather than around Flush, also covers the bufio auto-flush that
// happens in the middle of a long pipelined batch, so a reply can never leave
// before the write it acknowledges is on stable storage. The wait is per batch:
// the connection's records are covered by one WaitDurable for the highest
// sequence, and the log's group commit makes concurrent connections share fsyncs.
//
// Once a wait fails (the log latched, or a record was refused after its command
// was applied) the writer refuses every later write, so the replies of that
// batch are dropped and the connection closes without having acknowledged
// anything that may be lost. It is used from the connection goroutine only.
type durableWriter struct {
	w      io.Writer
	prop   command.Propagator
	ctx    *command.Context // source of LastSeq
	base   context.Context  // cancelled when the connection ends
	waited uint64           // highest sequence already known to be durable
	err    error
}

// Write implements io.Writer.
func (d *durableWriter) Write(p []byte) (int, error) {
	if d.err != nil {
		return 0, d.err
	}
	if seq := d.ctx.LastSeq; seq > d.waited {
		if err := d.prop.WaitDurable(d.base, seq); err != nil {
			d.err = err
			return 0, err
		}
		d.waited = seq
	}
	return d.w.Write(p)
}

// fail makes every later write fail with err.
func (d *durableWriter) fail(err error) {
	if d.err == nil {
		d.err = err
	}
}
