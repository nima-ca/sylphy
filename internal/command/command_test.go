package command

import (
	"bytes"
	"testing"

	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/protocol"
	"github.com/nima-ca/sylphy/internal/store"
)

// fakeStore is the Store used by handler tests: a real store
// driven by a FakeClock. A hand-written map fake cannot implement MutateEntry,
// because store.Entry cannot be constructed outside its package.
type fakeStore struct{ *store.Store }

// testEpochMs is the fake clock's starting time: a round, plausible Unix ms.
const testEpochMs = 1_700_000_000_000

type harness struct {
	d   *Dispatcher
	ctx *Context
	buf *bytes.Buffer
	w   *protocol.Writer
	clk *store.FakeClock
}

func newHarness(t *testing.T) *harness {
	t.Helper()
	reg, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	buf := &bytes.Buffer{}
	w := protocol.NewWriter(buf)
	cfg := config.Default()
	clk := store.NewFakeClock(testEpochMs)
	st, err := store.NewWithOptions(store.Options{Shards: 4, Clock: clk})
	if err != nil {
		t.Fatal(err)
	}
	return &harness{
		d:   NewDispatcher(reg, nil),
		ctx: &Context{Store: &fakeStore{st}, W: w, Config: &cfg, Conn: &ConnState{ID: 1}},
		buf: buf,
		w:   w,
		clk: clk,
	}
}

func (h *harness) do(args ...string) string {
	argv := make([][]byte, len(args))
	for i, a := range args {
		argv[i] = []byte(a)
	}
	h.buf.Reset()
	h.d.Dispatch(h.ctx, argv)
	_ = h.w.Flush()
	return h.buf.String()
}

func TestCommandsSequence(t *testing.T) {
	h := newHarness(t)
	const (
		notInt   = "-ERR value is not an integer or out of range\r\n"
		overflow = "-ERR increment or decrement would overflow\r\n"
		syntax   = "-ERR syntax error\r\n"
	)
	steps := []struct {
		args []string
		want string
	}{
		{[]string{"PING"}, "+PONG\r\n"},
		{[]string{"ping", "hi"}, "$2\r\nhi\r\n"},
		{[]string{"PING", "a", "b"}, "-ERR wrong number of arguments for 'ping' command\r\n"},
		{[]string{"ECHO", "x"}, "$1\r\nx\r\n"},
		{[]string{"ECHO"}, "-ERR wrong number of arguments for 'echo' command\r\n"},
		{[]string{"SET", "k", "v"}, "+OK\r\n"},
		{[]string{"SET", "k"}, "-ERR wrong number of arguments for 'set' command\r\n"},
		{[]string{"SET", "k", "v", "BOGUS"}, syntax},
		{[]string{"get", "k"}, "$1\r\nv\r\n"},
		{[]string{"GET", "nope"}, "$-1\r\n"},
		{[]string{"GET"}, "-ERR wrong number of arguments for 'get' command\r\n"},
		{[]string{"EXISTS", "k", "k", "z"}, ":2\r\n"},
		{[]string{"STRLEN", "k"}, ":1\r\n"},
		{[]string{"STRLEN", "nope"}, ":0\r\n"},
		{[]string{"APPEND", "k", "xyz"}, ":4\r\n"},
		{[]string{"GET", "k"}, "$4\r\nvxyz\r\n"},
		{[]string{"APPEND", "new", "ab"}, ":2\r\n"},
		{[]string{"INCR", "n"}, ":1\r\n"},
		{[]string{"INCRBY", "n", "10"}, ":11\r\n"},
		{[]string{"DECR", "n"}, ":10\r\n"},
		{[]string{"DECRBY", "n", "20"}, ":-10\r\n"},
		{[]string{"SET", "s", "abc"}, "+OK\r\n"},
		{[]string{"INCR", "s"}, notInt},
		{[]string{"SET", "big", "9223372036854775807"}, "+OK\r\n"},
		{[]string{"INCR", "big"}, overflow},
		{[]string{"SET", "small", "-9223372036854775808"}, "+OK\r\n"},
		{[]string{"DECR", "small"}, overflow},
		{[]string{"SET", "lz", "007"}, "+OK\r\n"},
		{[]string{"INCR", "lz"}, notInt},
		{[]string{"INCRBY", "n", "1.5"}, notInt},
		{[]string{"INCRBY", "n", "+1"}, notInt},
		{[]string{"DECRBY", "n", "-9223372036854775808"}, overflow},
		{[]string{"MSET", "a", "1", "b"}, "-ERR wrong number of arguments for 'mset' command\r\n"},
		{[]string{"MSET", "a", "1", "b", "2"}, "+OK\r\n"},
		{[]string{"MGET", "a", "zz", "b"}, "*3\r\n$1\r\n1\r\n$-1\r\n$1\r\n2\r\n"},
		{[]string{"DEL", "a", "zz", "a"}, ":1\r\n"},
		{[]string{"DBSIZE"}, ":8\r\n"}, // k new n s big small lz b
		{[]string{"FLUSHALL", "bogus"}, syntax},
		{[]string{"FLUSHALL", "ASYNC"}, "+OK\r\n"},
		{[]string{"DBSIZE"}, ":0\r\n"},
		{[]string{"SET", "one", "1"}, "+OK\r\n"},
		{[]string{"KEYS", "*"}, "*1\r\n$3\r\none\r\n"},
		{[]string{"FLUSHALL"}, "+OK\r\n"},
		{[]string{"SELECT", "0"}, "+OK\r\n"},
		{[]string{"SELECT", "1"}, "-ERR DB index is out of range\r\n"},
		{[]string{"SELECT", "x"}, "-ERR invalid DB index\r\n"},
		{[]string{"CLIENT", "SETNAME", "bob"}, "+OK\r\n"},
		{[]string{"CLIENT", "SETNAME", "a b"}, "-ERR Client names cannot contain spaces, newlines or special characters.\r\n"},
		{[]string{"CLIENT", "SETINFO", "LIB-NAME", "x"}, "+OK\r\n"},
		{[]string{"CLIENT", "BOGUS"}, "-ERR unknown subcommand 'BOGUS'. Try CLIENT HELP.\r\n"},
		{[]string{"COMMAND"}, "*0\r\n"},
		{[]string{"COMMAND", "DOCS"}, "*0\r\n"},
		{[]string{"COMMAND", "COUNT"}, "*0\r\n"},
		{[]string{"COMMAND", "NOPE"}, "-ERR unknown subcommand 'NOPE'. Try COMMAND HELP.\r\n"},
		{[]string{"NOPE", "a", "b"}, "-ERR unknown command 'NOPE', with args beginning with: 'a' 'b' \r\n"},
	}
	for _, s := range steps {
		if got := h.do(s.args...); got != s.want {
			t.Errorf("%v\n got: %q\nwant: %q", s.args, got, s.want)
		}
	}
	if h.ctx.Conn.Name != "bob" {
		t.Errorf("client name = %q", h.ctx.Conn.Name)
	}
}

func TestQuitSetsClosing(t *testing.T) {
	h := newHarness(t)
	if got := h.do("QUIT"); got != "+OK\r\n" || !h.ctx.Conn.Closing {
		t.Fatalf("got %q closing=%v", got, h.ctx.Conn.Closing)
	}
}

func TestEmptyArgvIsIgnored(t *testing.T) {
	h := newHarness(t)
	h.d.Dispatch(h.ctx, nil)
	if h.w.Buffered() != 0 {
		t.Fatal("wrote a reply for an empty request")
	}
}

func TestParseInt64Strict(t *testing.T) {
	tests := []struct {
		in   string
		want int64
		ok   bool
	}{
		{"0", 0, true}, {"-1", -1, true}, {"42", 42, true},
		{"9223372036854775807", 9223372036854775807, true},
		{"-9223372036854775808", -9223372036854775808, true},
		{"9223372036854775808", 0, false}, {"-9223372036854775809", 0, false},
		{"", 0, false}, {"-", 0, false}, {"-0", 0, false}, {"01", 0, false},
		{"+1", 0, false}, {" 1", 0, false}, {"1 ", 0, false}, {"1a", 0, false},
		{"123456789012345678901", 0, false},
	}
	for _, tt := range tests {
		got, ok := parseInt64Strict([]byte(tt.in))
		if ok != tt.ok || (ok && got != tt.want) {
			t.Errorf("parseInt64Strict(%q) = %d,%v; want %d,%v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

type errStore struct{ fakeStore }

func (errStore) Update(string, func([]byte, bool) ([]byte, error)) error {
	return bytes.ErrTooLarge // a non-ReplyError must not leak to the client
}

func TestInternalErrorsAreOpaque(t *testing.T) {
	h := newHarness(t)
	h.ctx.Store = &errStore{}
	if got := h.do("INCR", "k"); got != "-ERR internal error\r\n" {
		t.Fatalf("got %q", got)
	}
}
