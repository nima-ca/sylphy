package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
)

// ErrServerClosed is returned by Serve and ListenAndServe after Shutdown has
// been called.
var ErrServerClosed = errors.New("server: closed")

// Server accepts TCP connections and serves RESP commands.
type Server struct {
	cfg    config.Config
	store  command.Store
	disp   *command.Dispatcher
	log    *slog.Logger
	limits protocol.Limits

	quit   chan struct{} // closed when shutdown begins
	nextID atomic.Uint64
	wg     sync.WaitGroup // one count per live connection goroutine

	mu      sync.Mutex // guards the fields below
	ln      net.Listener
	conns   map[*conn]struct{}
	closing bool
}

// New creates a Server. A nil logger discards logs.
func New(cfg config.Config, st command.Store, reg *command.Registry, logger *slog.Logger) *Server {
	if logger == nil {
		logger = slog.New(slog.DiscardHandler)
	}
	return &Server{
		cfg:    cfg,
		store:  st,
		disp:   command.NewDispatcher(reg, logger),
		log:    logger,
		limits: cfg.ProtocolLimits(),
		quit:   make(chan struct{}),
		conns:  make(map[*conn]struct{}),
	}
}

// ListenAndServe listens on the configured address and serves until ctx is
// cancelled (graceful shutdown) or a fatal error occurs.
func (s *Server) ListenAndServe(ctx context.Context) error {
	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", s.cfg.Addr)
	if err != nil {
		return err
	}
	return s.Serve(ctx, ln)
}

// Serve accepts connections on ln. When ctx is cancelled it performs a graceful
// shutdown bounded by ShutdownGracePeriod and returns its result (nil if all
// connections drained in time, context.DeadlineExceeded if some had to be
// force-closed). If Shutdown is called externally, Serve returns
// ErrServerClosed immediately while Shutdown keeps draining. Serve takes
// ownership of ln.
func (s *Server) Serve(ctx context.Context, ln net.Listener) error {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = ln.Close()
		return ErrServerClosed
	}
	if s.ln != nil {
		s.mu.Unlock()
		_ = ln.Close()
		return errors.New("server: already serving")
	}
	s.ln = ln
	s.mu.Unlock()
	s.log.Info("listening", "addr", ln.Addr().String())

	acceptDone := make(chan error, 1)
	go func() { acceptDone <- s.acceptLoop(ln) }()

	select {
	case <-ctx.Done():
		gctx, cancel := context.WithTimeout(context.Background(), s.cfg.ShutdownGracePeriod)
		defer cancel()
		err := s.Shutdown(gctx)
		<-acceptDone
		return err
	case err := <-acceptDone:
		if errors.Is(err, ErrServerClosed) {
			return err
		}
		// Fatal accept error: stop everything immediately.
		cctx, cancel := context.WithCancel(context.Background())
		cancel()
		_ = s.Shutdown(cctx)
		return err
	}
}

// Shutdown stops accepting, lets in-flight commands finish, closes idle
// connections, and force-closes whatever remains when ctx expires. It returns
// nil if everything drained in time and ctx.Err() otherwise. It is safe to call
// more than once.
func (s *Server) Shutdown(ctx context.Context) error {
	s.mu.Lock()
	if !s.closing {
		s.closing = true
		close(s.quit)
		if s.ln != nil {
			_ = s.ln.Close()
		}
	}
	conns := make([]*conn, 0, len(s.conns))
	for c := range s.conns {
		conns = append(conns, c)
	}
	s.mu.Unlock()

	// A past read deadline wakes connections blocked waiting for a request;
	// connections that are mid-command finish it and then hit the deadline.
	for _, c := range conns {
		c.drain()
	}

	done := make(chan struct{})
	go func() { s.wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		s.forceCloseAll()
		<-done
		return ctx.Err()
	}
}

// Addr returns the listening address, or nil if the server is not serving.
func (s *Server) Addr() net.Addr {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ln == nil {
		return nil
	}
	return s.ln.Addr()
}

// NumClients returns the number of currently connected clients.
func (s *Server) NumClients() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.conns)
}

func (s *Server) isClosing() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closing
}

func (s *Server) acceptLoop(ln net.Listener) error {
	var delay time.Duration
	for {
		nc, err := ln.Accept()
		if err != nil {
			if s.isClosing() {
				return ErrServerClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			// Probably transient (e.g. out of file descriptors): back off.
			if delay == 0 {
				delay = 5 * time.Millisecond
			} else {
				delay = min(delay*2, time.Second)
			}
			s.log.Warn("accept failed; retrying", "err", err, "delay", delay)
			t := time.NewTimer(delay)
			select {
			case <-t.C:
			case <-s.quit:
				t.Stop()
				return ErrServerClosed
			}
			continue
		}
		delay = 0
		s.admit(nc)
	}
}

// admit registers a new connection (or rejects it) and starts its goroutine.
// wg.Add happens under mu *before* Shutdown can observe closing=true, so Add can
// never race with Wait.
func (s *Server) admit(nc net.Conn) {
	s.mu.Lock()
	if s.closing {
		s.mu.Unlock()
		_ = nc.Close()
		return
	}
	if len(s.conns) >= s.cfg.MaxClients {
		s.mu.Unlock()
		_ = nc.SetWriteDeadline(time.Now().Add(time.Second))
		_, _ = nc.Write([]byte("-ERR max number of clients reached\r\n"))
		_ = nc.Close()
		s.log.Warn("rejected connection: max clients reached", "max", s.cfg.MaxClients)
		return
	}
	if tc, ok := nc.(*net.TCPConn); ok {
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}
	c := s.newConn(nc)
	s.conns[c] = struct{}{}
	s.wg.Add(1)
	s.mu.Unlock()

	go func() {
		defer s.wg.Done()
		defer s.removeConn(c)
		c.serve()
	}()
}

func (s *Server) removeConn(c *conn) {
	s.mu.Lock()
	delete(s.conns, c)
	s.mu.Unlock()
}

func (s *Server) forceCloseAll() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.nc.Close()
	}
}
