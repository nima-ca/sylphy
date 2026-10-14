package server

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
	"github.com/nima-ca/sylphy/internal/store"
)

// guard bounds waits that only a bug could make long; it is not a delay.
const guard = 10 * time.Second

var errDurBoom = errors.New("boom")

type durProp struct {
	mu          sync.Mutex
	seq         uint64
	err         error
	appendErr   error
	waitErr     error
	waits       []uint64
	gate        chan struct{} // WaitDurable blocks until it is closed; nil does not block
	entered     chan struct{} // closed on the first WaitDurable
	enteredOnce sync.Once
	admitting   chan struct{} // if set, Admit announces itself and blocks until its context ends
}

var _ command.Propagator = (*durProp)(nil)

func (p *durProp) Admit(ctx context.Context) error {
	if p.admitting != nil {
		p.admitting <- struct{}{}
		<-ctx.Done()
		return ctx.Err()
	}
	return nil
}

func (p *durProp) Append(recs ...[]byte) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.appendErr != nil {
		return 0, p.appendErr
	}
	p.seq += uint64(len(recs))
	return p.seq, nil
}

func (p *durProp) WaitDurable(ctx context.Context, seq uint64) error {
	p.mu.Lock()
	p.waits = append(p.waits, seq)
	gate, werr := p.gate, p.waitErr
	p.mu.Unlock()
	p.enteredOnce.Do(func() {
		if p.entered != nil {
			close(p.entered)
		}
	})
	if gate != nil {
		select {
		case <-gate:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return werr
}

func (p *durProp) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *durProp) waited() []uint64 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]uint64(nil), p.waits...)
}

type durPersistence struct{}

func (durPersistence) Save(context.Context) error { return nil }
func (durPersistence) BGSave() error              { return nil }
func (durPersistence) LastSave() time.Time        { return time.Unix(1_700_000_000, 0) }
func (durPersistence) AOFEnabled() bool           { return true }

func newDurServer(t *testing.T, prop command.Propagator, wait bool) *Server {
	t.Helper()
	st, err := store.New(4)
	if err != nil {
		t.Fatal(err)
	}
	reg, err := command.NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	srv := New(config.Default(), st, reg, nil)
	srv.SetPersistence(durPersistence{}, prop, wait)
	return srv
}

type durConn struct {
	client net.Conn
	br     *bufio.Reader
	c      *conn
	done   chan struct{}
}

func startDurConn(t *testing.T, srv *Server) *durConn {
	t.Helper()
	client, serverSide := net.Pipe()
	c := srv.newConn(serverSide)
	d := &durConn{client: client, br: bufio.NewReader(client), c: c, done: make(chan struct{})}
	go func() {
		defer close(d.done)
		c.serve()
	}()
	t.Cleanup(func() {
		_ = client.Close()
		c.cancel()
		select {
		case <-d.done:
		case <-time.After(guard):
			t.Error("the connection goroutine did not exit")
		}
	})
	return d
}

func (d *durConn) send(t *testing.T, args ...string) {
	t.Helper()
	var sb strings.Builder
	sb.WriteString("*" + itoa(len(args)) + "\r\n")
	for _, a := range args {
		sb.WriteString("$" + itoa(len(a)) + "\r\n" + a + "\r\n")
	}
	_ = d.client.SetWriteDeadline(time.Now().Add(guard))
	if _, err := d.client.Write([]byte(sb.String())); err != nil {
		t.Fatal(err)
	}
}

func itoa(n int) string {
	var b []byte
	if n == 0 {
		return "0"
	}
	for n > 0 {
		b = append([]byte{byte('0' + n%10)}, b...)
		n /= 10
	}
	return string(b)
}

func (d *durConn) line(t *testing.T) string {
	t.Helper()
	_ = d.client.SetReadDeadline(time.Now().Add(guard))
	s, err := d.br.ReadString('\n')
	if err != nil {
		t.Fatalf("reading a reply: %v (got %q)", err, s)
	}
	return s
}

// TestAlwaysWaitsForDurabilityBeforeReplying: with fsync always the reply to a
// write is sent only after WaitDurable returns. net.Pipe writes block until the
// peer reads and this client reads nothing until released, so a server that wrote
// before waiting would stall in the write and never reach WaitDurable.
func TestAlwaysWaitsForDurabilityBeforeReplying(t *testing.T) {
	prop := &durProp{gate: make(chan struct{}), entered: make(chan struct{})}
	d := startDurConn(t, newDurServer(t, prop, true))
	d.send(t, "SET", "k", "v")
	select {
	case <-prop.entered:
	case <-time.After(guard):
		t.Fatal("the server replied (or stalled) without waiting for the write to be durable")
	}
	if got := prop.waited(); len(got) != 1 || got[0] != 1 {
		t.Fatalf("waited for %v, want sequence 1", got)
	}
	close(prop.gate)
	if got := d.line(t); got != "+OK\r\n" {
		t.Fatalf("reply %q", got)
	}
}

func TestEverysecDoesNotWait(t *testing.T) {
	prop := &durProp{gate: make(chan struct{})} // would block forever if it were waited on
	d := startDurConn(t, newDurServer(t, prop, false))
	d.send(t, "SET", "k", "v")
	if got := d.line(t); got != "+OK\r\n" {
		t.Fatalf("reply %q", got)
	}
	if got := prop.waited(); len(got) != 0 {
		t.Fatalf("waited for %v although fsync is not always", got)
	}
}

// A refused record after the command was applied: under fsync always the client
// must not be acknowledged, so the connection closes with no reply at all.
func TestAlwaysDropsRepliesWhenARecordWasRefused(t *testing.T) {
	prop := &durProp{appendErr: errDurBoom}
	d := startDurConn(t, newDurServer(t, prop, true))
	d.send(t, "SET", "k", "v") // a primitive write: the handler cannot return the error
	_ = d.client.SetReadDeadline(time.Now().Add(guard))
	n, err := d.br.Read(make([]byte, 64))
	if n != 0 || err == nil {
		t.Fatalf("got %d bytes (%v): an unacknowledged write must not get a reply", n, err)
	}
	select {
	case <-d.done:
	case <-time.After(guard):
		t.Fatal("the connection stayed open")
	}
}

// Without fsync always, replies were never promised durable, so the reply goes
// out (the failure is logged and the log refuses further writes).
func TestEverysecRepliesEvenWhenARecordWasRefused(t *testing.T) {
	prop := &durProp{appendErr: errDurBoom}
	d := startDurConn(t, newDurServer(t, prop, false))
	d.send(t, "SET", "k", "v")
	if got := d.line(t); got != "+OK\r\n" {
		t.Fatalf("reply %q", got)
	}
}

func TestMisconfAndReadsWhileThePersistenceIsFailed(t *testing.T) {
	for _, wait := range []bool{false, true} {
		prop := &durProp{err: errDurBoom}
		d := startDurConn(t, newDurServer(t, prop, wait))
		d.send(t, "SET", "k", "v")
		if got := d.line(t); !strings.HasPrefix(got, "-MISCONF ") {
			t.Fatalf("wait=%v: SET replied %q", wait, got)
		}
		d.send(t, "GET", "k")
		if got := d.line(t); got != "$-1\r\n" {
			t.Fatalf("wait=%v: GET replied %q", wait, got)
		}
		d.send(t, "PING")
		if got := d.line(t); got != "+PONG\r\n" {
			t.Fatalf("wait=%v: PING replied %q", wait, got)
		}
	}
}

func TestPersistenceCommandsAreWiredIntoConnections(t *testing.T) {
	d := startDurConn(t, newDurServer(t, &durProp{}, false))
	d.send(t, "LASTSAVE")
	if got := d.line(t); got != ":1700000000\r\n" {
		t.Fatalf("LASTSAVE replied %q", got)
	}
	d.send(t, "BGSAVE")
	if got := d.line(t); got != "+Background saving started\r\n" {
		t.Fatalf("BGSAVE replied %q", got)
	}

	// Without SetPersistence the commands report that persistence is off.
	st, _ := store.New(4)
	reg, _ := command.NewDefaultRegistry()
	off := startDurConn(t, New(config.Default(), st, reg, nil))
	off.send(t, "SAVE")
	if got := off.line(t); !strings.HasPrefix(got, "-ERR persistence is disabled") {
		t.Fatalf("SAVE replied %q", got)
	}
	off.send(t, "SET", "k", "v") // and writes are not journaled or gated
	if got := off.line(t); got != "+OK\r\n" {
		t.Fatalf("SET replied %q", got)
	}
}

// TestForceCloseUnblocksAConnectionWaitingForLogRoom: Admit has no deadline of
// its own, so the connection's context is what keeps a stuck disk from holding
// a goroutine through shutdown.
func TestForceCloseUnblocksAConnectionWaitingForLogRoom(t *testing.T) {
	prop := &durProp{admitting: make(chan struct{}, 1)}
	srv := newDurServer(t, prop, true)
	d := startDurConn(t, srv)
	srv.mu.Lock()
	srv.conns[d.c] = struct{}{}
	srv.mu.Unlock()
	d.send(t, "SET", "k", "v")
	select {
	case <-prop.admitting:
	case <-time.After(guard):
		t.Fatal("the command never reached Admit")
	}
	srv.forceCloseAll()
	select {
	case <-d.done:
	case <-time.After(guard):
		t.Fatal("the connection is still stuck in Admit after a force close")
	}
}

func TestDurableWriterGatesEveryWrite(t *testing.T) {
	prop := &durProp{}
	cmdCtx := &command.Context{}
	var out bytes.Buffer
	d := &durableWriter{w: &out, prop: prop, ctx: cmdCtx, base: context.Background()}

	if _, err := d.Write([]byte("a")); err != nil || len(prop.waited()) != 0 {
		t.Fatalf("nothing appended yet: err %v, waits %v", err, prop.waited())
	}
	cmdCtx.LastSeq = 7
	for range 3 {
		if _, err := d.Write([]byte("b")); err != nil {
			t.Fatal(err)
		}
	}
	if got := prop.waited(); len(got) != 1 || got[0] != 7 {
		t.Fatalf("waits %v: one wait must cover repeated writes of the same batch", got)
	}
	cmdCtx.LastSeq = 9
	_, _ = d.Write([]byte("c"))
	if got := prop.waited(); len(got) != 2 || got[1] != 9 {
		t.Fatalf("waits %v", got)
	}
	if out.String() != "abbbc" {
		t.Fatalf("output %q", out.String())
	}

	// A failed wait: nothing reaches the socket, now or later.
	prop.waitErr = errDurBoom
	cmdCtx.LastSeq = 12
	if _, err := d.Write([]byte("x")); !errors.Is(err, errDurBoom) {
		t.Fatalf("got %v", err)
	}
	prop.waitErr = nil
	if _, err := d.Write([]byte("y")); !errors.Is(err, errDurBoom) {
		t.Fatalf("the failure is not sticky: %v", err)
	}
	if out.String() != "abbbc" {
		t.Fatalf("bytes leaked past a failed wait: %q", out.String())
	}

	// fail() has the same effect.
	d2 := &durableWriter{w: &out, prop: prop, ctx: cmdCtx, base: context.Background()}
	d2.fail(errDurBoom)
	d2.fail(errors.New("second failure is ignored"))
	if _, err := d2.Write([]byte("z")); !errors.Is(err, errDurBoom) {
		t.Fatalf("got %v", err)
	}
}

// TestDurableWriterCoversTheWriterAutoFlush: a long pipelined batch makes the
// protocol writer flush on its own long before the server's explicit Flush. That
// flush must be gated too.
func TestDurableWriterCoversTheWriterAutoFlush(t *testing.T) {
	prop := &durProp{waitErr: errDurBoom}
	cmdCtx := &command.Context{LastSeq: 5}
	var out bytes.Buffer
	d := &durableWriter{w: &out, prop: prop, ctx: cmdCtx, base: context.Background()}
	w := protocol.NewWriter(d)
	for range 20_000 { // far more than one buffer of replies
		w.WriteSimpleString("OK")
	}
	if err := w.Flush(); !errors.Is(err, errDurBoom) {
		t.Fatalf("Flush = %v", err)
	}
	if out.Len() != 0 {
		t.Fatalf("%d bytes were sent before the write they acknowledge was durable", out.Len())
	}
	if got := prop.waited(); len(got) == 0 || got[0] != 5 {
		t.Fatalf("waits %v", got)
	}
	_ = io.Discard
}
