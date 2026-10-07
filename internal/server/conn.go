package server

import (
	"errors"
	"io"
	"net"
	"runtime/debug"
	"sync"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/protocol"
)

// conn is one client connection, owned by a single goroutine (serve).
type conn struct {
	srv    *Server
	nc     net.Conn
	rd     *protocol.Reader
	wr     *protocol.Writer
	state  *command.ConnState
	cmdCtx *command.Context

	mu       sync.Mutex // guards draining and serializes deadline updates with drain()
	draining bool
}

func (s *Server) newConn(nc net.Conn) *conn {
	state := &command.ConnState{ID: s.nextID.Add(1)}
	wr := protocol.NewWriter(nc)
	return &conn{
		srv:   s,
		nc:    nc,
		rd:    protocol.NewReader(nc, s.limits),
		wr:    wr,
		state: state,
		cmdCtx: &command.Context{
			Store:  s.store,
			W:      wr,
			Config: &s.cfg,
			Conn:   state,
		},
	}
}

// drain marks the connection as draining and wakes any blocked read. Holding mu
// makes this atomic with armDeadline, so a concurrent idle-deadline update can
// never overwrite the shutdown deadline.
func (c *conn) drain() {
	c.mu.Lock()
	c.draining = true
	_ = c.nc.SetReadDeadline(time.Now())
	c.mu.Unlock()
}

func (c *conn) isDraining() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.draining
}

// armDeadline sets the idle read deadline before a blocking read.
func (c *conn) armDeadline() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.draining {
		_ = c.nc.SetReadDeadline(time.Now())
		return
	}
	var dl time.Time // zero = no deadline
	if d := c.srv.cfg.IdleTimeout; d > 0 {
		dl = time.Now().Add(d)
	}
	_ = c.nc.SetReadDeadline(dl)
}

func (c *conn) serve() {
	s := c.srv
	id := c.state.ID
	s.log.Debug("connection opened", "id", id, "remote", c.nc.RemoteAddr().String())
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("panic in connection handler; closing connection",
				"id", id, "panic", r, "stack", string(debug.Stack()))
		}
		_ = c.nc.Close()
		s.log.Debug("connection closed", "id", id)
	}()

	for {
		// Only arm the deadline when we may actually block: pipelined requests
		// already in the buffer don't touch the socket.
		if c.rd.Buffered() == 0 {
			c.armDeadline()
		}
		args, err := c.rd.ReadCommand()
		if err != nil {
			c.finish(err)
			return
		}
		s.disp.Dispatch(c.cmdCtx, args)
		if c.state.Closing {
			_ = c.flush()
			return
		}
		// Pipelining: flush once, when no further request is buffered.
		if c.rd.Buffered() == 0 {
			if err := c.wr.Flush(); err != nil {
				s.log.Debug("write failed", "id", id, "err", err)
				return
			}
		}
	}
}

// finish handles a read error: it replies to protocol violations, then flushes
// whatever replies are pending and lets serve close the connection.
func (c *conn) finish(err error) {
	s := c.srv
	switch {
	case errors.Is(err, io.EOF), errors.Is(err, net.ErrClosed):
		// Client went away cleanly, or we force-closed it.
	case errors.Is(err, protocol.ErrProtocol), errors.Is(err, protocol.ErrLimitExceeded):
		s.log.Warn("protocol error; closing connection", "id", c.state.ID, "err", err)
		c.wr.WriteError("ERR Protocol error: " + err.Error())
	case isTimeout(err):
		if !c.isDraining() {
			s.log.Debug("idle timeout", "id", c.state.ID)
		}
	case errors.Is(err, io.ErrUnexpectedEOF):
		s.log.Debug("client disconnected mid-request", "id", c.state.ID)
	default:
		s.log.Warn("read failed", "id", c.state.ID, "err", err)
	}
	_ = c.flush()
}

// flush bounds the final write so a dead peer cannot hold the goroutine.
func (c *conn) flush() error {
	_ = c.nc.SetWriteDeadline(time.Now().Add(time.Second))
	return c.wr.Flush()
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}
