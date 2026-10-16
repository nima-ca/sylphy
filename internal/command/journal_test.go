package command

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/protocol"
)

// fakeProp is a Propagator that records what it is given.
type fakeProp struct {
	mu        sync.Mutex
	raw       [][]byte
	seq       uint64
	err       error
	admitErr  error
	appendErr error
	admits    int
}

var _ Propagator = (*fakeProp)(nil)

func (p *fakeProp) Admit(context.Context) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.admits++
	return p.admitErr
}

func (p *fakeProp) Append(recs ...[]byte) (uint64, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.appendErr != nil {
		return 0, p.appendErr
	}
	for _, r := range recs {
		p.raw = append(p.raw, bytes.Clone(r))
		p.seq++
	}
	return p.seq, nil
}

func (p *fakeProp) WaitDurable(context.Context, uint64) error { return nil }

func (p *fakeProp) Err() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.err
}

func (p *fakeProp) set(f func(*fakeProp)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	f(p)
}

func (p *fakeProp) records() [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.raw...)
}

// parseRecord decodes one RESP-array record into its words.
func parseRecord(t testing.TB, b []byte) []string {
	t.Helper()
	rest := string(b)
	line := func() string {
		i := strings.Index(rest, "\r\n")
		if i < 0 {
			t.Fatalf("malformed record %q", b)
		}
		l := rest[:i]
		rest = rest[i+2:]
		return l
	}
	hdr := line()
	if !strings.HasPrefix(hdr, "*") {
		t.Fatalf("malformed record %q", b)
	}
	n, err := strconv.Atoi(hdr[1:])
	if err != nil {
		t.Fatalf("malformed record %q", b)
	}
	out := make([]string, 0, n)
	for range n {
		h := line()
		l, err := strconv.Atoi(strings.TrimPrefix(h, "$"))
		if err != nil || l > len(rest) {
			t.Fatalf("malformed record %q", b)
		}
		out = append(out, rest[:l])
		rest = rest[l+2:]
	}
	return out
}

func renderRaw(t testing.TB, raws [][]byte) string {
	t.Helper()
	parts := make([]string, len(raws))
	for i, r := range raws {
		parts[i] = strings.Join(parseRecord(t, r), " ")
	}
	return strings.Join(parts, " ; ")
}

func toArgv(args []string) [][]byte {
	out := make([][]byte, len(args))
	for i, a := range args {
		out[i] = []byte(a)
	}
	return out
}

type auditCase struct {
	cmd   string
	setup []string // commands, each a space-separated line, run first
	args  string
	want  string // the records the command must log; "" is a documented no-op
}

func ac(cmd, args, want string, setup ...string) auditCase {
	return auditCase{cmd: cmd, setup: setup, args: args, want: want}
}

// auditCases has at least one case for every write command in the registry.
// Where want is "", the command is a documented no-op for that input: hooked
// commands that decided nothing changed. Commands without a hook log verbatim
// even when they change nothing; those cases say so.
func auditCases() []auditCase {
	return []auditCase{
		ac("FLUSHALL", "FLUSHALL", "FLUSHALL", "SET a 1"),

		ac("SET", "SET k v EX 10", "SET k v PXAT 1700000010000"),
		ac("SET", "SET k v NX", "", "SET k old"), // refused
		ac("SET", "SET k v PXAT 1", "DEL k"),     // deadline already past: no key
		ac("SETNX", "SETNX n 1", "SET n 1"),
		ac("SETNX", "SETNX n 2", "", "SET n 1"),
		ac("SETEX", "SETEX s 10 v", "SET s v PXAT 1700000010000"),
		ac("PSETEX", "PSETEX s 2500 v", "SET s v PXAT 1700000002500"),
		ac("GETEX", "GETEX g EX 100", "PEXPIREAT g 1700000100000", "SET g v"),
		ac("GETEX", "GETEX g", "", "SET g v"),
		ac("GETEX", "GETEX g PXAT 1", "DEL g", "SET g v"),
		ac("GETDEL", "GETDEL d", "DEL d", "SET d v"),
		ac("GETDEL", "GETDEL missing", ""),
		ac("INCR", "INCR n", "INCR n"),
		ac("DECR", "DECR n", "DECR n"),
		ac("INCRBY", "INCRBY n 5", "INCRBY n 5"),
		ac("DECRBY", "DECRBY n 5", "DECRBY n 5"),
		ac("APPEND", "APPEND k x", "APPEND k x", "SET k v"),
		ac("MSET", "MSET a 1 b 2", "SET a 1 ; SET b 2"), // one record per pair

		ac("DEL", "DEL a b missing", "DEL a ; DEL b", "SET a 1", "SET b 2"), // one per key that existed
		ac("DEL", "DEL missing", ""),
		ac("UNLINK", "UNLINK a", "DEL a", "SET a 1"),

		ac("EXPIRE", "EXPIRE e 100", "PEXPIREAT e 1700000100000", "SET e v"),
		ac("EXPIRE", "EXPIRE e 100 NX", "", "SET e v EX 5"),
		ac("PEXPIRE", "PEXPIRE e 1500", "PEXPIREAT e 1700000001500", "SET e v"),
		ac("EXPIREAT", "EXPIREAT e 1700000500", "PEXPIREAT e 1700000500000", "SET e v"),
		ac("EXPIREAT", "EXPIREAT e 1", "DEL e", "SET e v"),
		ac("PEXPIREAT", "PEXPIREAT e 1700000600000", "PEXPIREAT e 1700000600000", "SET e v"),
		ac("PERSIST", "PERSIST p", "PERSIST p", "SET p v EX 100"),
		ac("PERSIST", "PERSIST p", "", "SET p v"),

		ac("LPUSH", "LPUSH l a", "LPUSH l a"),
		ac("RPUSH", "RPUSH l a", "RPUSH l a"),
		ac("LPUSHX", "LPUSHX l b", "LPUSHX l b", "RPUSH l a"),
		ac("RPUSHX", "RPUSHX l b", "RPUSHX l b", "RPUSH l a"),
		ac("LPOP", "LPOP l", "LPOP l", "RPUSH l a b"),
		ac("LPOP", "LPOP missing", "LPOP missing"), // unhooked: logged verbatim although it changes nothing
		ac("RPOP", "RPOP l", "RPOP l", "RPUSH l a b"),
		ac("LSET", "LSET l 0 z", "LSET l 0 z", "RPUSH l a"),
		ac("LREM", "LREM l 0 a", "LREM l 0 a", "RPUSH l a b a"),
		ac("LTRIM", "LTRIM l 0 0", "LTRIM l 0 0", "RPUSH l a b"),
		ac("LINSERT", "LINSERT l BEFORE a x", "LINSERT l BEFORE a x", "RPUSH l a b"),

		ac("HSET", "HSET h f v", "HSET h f v"),
		ac("HSETNX", "HSETNX h f v", "HSETNX h f v"),
		ac("HDEL", "HDEL h f", "HDEL h f", "HSET h f v"),
		ac("HINCRBY", "HINCRBY h n 1", "HINCRBY h n 1"),
		ac("HINCRBYFLOAT", "HINCRBYFLOAT h f 1.5", "HINCRBYFLOAT h f 1.5"),

		ac("SADD", "SADD s a", "SADD s a"),
		ac("SREM", "SREM s a", "SREM s a", "SADD s a b"),
		ac("SPOP", "SPOP s", "SREM s only", "SADD s only"),
		ac("SPOP", "SPOP missing", ""),
		ac("SMOVE", "SMOVE s t a", "SMOVE s t a", "SADD s a"),
		ac("SUNIONSTORE", "SUNIONSTORE d a b", "SUNIONSTORE d a b", "SADD a x", "SADD b y"),
		ac("SINTERSTORE", "SINTERSTORE d a b", "SINTERSTORE d a b", "SADD a x", "SADD b x"),
		ac("SDIFFSTORE", "SDIFFSTORE d a b", "SDIFFSTORE d a b", "SADD a x y", "SADD b y"),

		ac("ZADD", "ZADD z 1 a", "ZADD z 1 a"),
		ac("ZREM", "ZREM z a", "ZREM z a", "ZADD z 1 a"),
		ac("ZINCRBY", "ZINCRBY z 2.5 a", "ZINCRBY z 2.5 a"),
		ac("ZREMRANGEBYRANK", "ZREMRANGEBYRANK z 0 0", "ZREMRANGEBYRANK z 0 0", "ZADD z 1 a"),
		ac("ZREMRANGEBYSCORE", "ZREMRANGEBYSCORE z 0 5", "ZREMRANGEBYSCORE z 0 5", "ZADD z 1 a"),
		ac("ZPOPMIN", "ZPOPMIN z", "ZPOPMIN z", "ZADD z 1 a 2 b"),
		ac("ZPOPMAX", "ZPOPMAX z", "ZPOPMAX z", "ZADD z 1 a 2 b"),

		ac("RENAME", "RENAME a b", "RENAME a b", "SET a 1"),
		ac("RENAMENX", "RENAMENX a b", "RENAMENX a b", "SET a 1"),
		ac("RENAMENX", "RENAMENX a b", "RENAMENX a b", "SET a 1", "SET b 2"), // destination exists: verbatim no-op
	}
}

// TestJournalCoversEveryWriteCommand runs every write command in the registry
// through the journal and checks that it logs exactly what it should (or is a
// documented no-op), that replaying only the log on a second server reproduces
// the state after each command, and that, where the journal and the older
// post-hoc Rewritten agree in form, they agree.
func TestJournalCoversEveryWriteCommand(t *testing.T) {
	a, b := newHarness(t), newHarness(t)
	a.ctx.Rand = rand.New(rand.NewPCG(1, 2))
	a.ctx.Propagate = true
	prop := &fakeProp{}
	a.ctx.EnableJournal(context.Background(), prop)

	covered := map[string]bool{}
	// MSET, DEL and UNLINK are logged per key by the journal and verbatim by the
	// older Rewritten path, so the two legitimately differ for them.
	differs := map[string]bool{"MSET": true, "DEL": true, "UNLINK": true}
	for _, tc := range auditCases() {
		covered[tc.cmd] = true
		t.Run(tc.cmd+"/"+tc.args, func(t *testing.T) {
			start := len(prop.records())
			a.do("FLUSHALL")
			for _, s := range tc.setup {
				if r := a.do(strings.Fields(s)...); strings.HasPrefix(r, "-") {
					t.Fatalf("setup %q failed: %q", s, r)
				}
			}
			mark := len(prop.records())
			args := strings.Fields(tc.args)
			if r := a.do(args...); strings.HasPrefix(r, "-") {
				t.Fatalf("%q failed: %q", tc.args, r)
			}
			if got := renderRaw(t, prop.records()[mark:]); got != tc.want {
				t.Fatalf("logged %q, want %q", got, tc.want)
			}
			if !differs[tc.cmd] {
				if got := renderRewritten(a.ctx.Rewritten); got != tc.want {
					t.Fatalf("the post-hoc Rewritten path says %q but the journal logged %q", got, tc.want)
				}
			}
			for _, rec := range prop.records()[start:] {
				if r := b.do(parseRecord(t, rec)...); strings.HasPrefix(r, "-") {
					t.Fatalf("replaying %q failed: %q", renderRaw(t, [][]byte{rec}), r)
				}
			}
			if got, want := b.dump(t), a.dump(t); got != want {
				t.Fatalf("state diverged after replaying the log\nreplayed:\n%s\noriginal:\n%s", got, want)
			}
		})
	}

	reg, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range reg.Names() {
		if sp, _ := reg.Lookup([]byte(name)); sp.Flags&FlagWrite != 0 && !covered[name] {
			t.Errorf("write command %s has no journal audit case", name)
		}
	}
}

func TestJournalLastSeqAndOrder(t *testing.T) {
	h := newHarness(t)
	prop := &fakeProp{}
	h.ctx.EnableJournal(context.Background(), prop)
	h.ctx.EnableJournal(context.Background(), prop) // a second call must not wrap twice

	h.do("MSET", "a", "1", "b", "2", "c", "3")
	if h.ctx.LastSeq != 3 {
		t.Fatalf("LastSeq = %d after three records", h.ctx.LastSeq)
	}
	h.do("GET", "a")
	if h.ctx.LastSeq != 3 {
		t.Fatal("a read moved LastSeq")
	}
	h.do("SET", "k", "v", "EX", "10")
	if h.ctx.LastSeq != 4 {
		t.Fatalf("LastSeq = %d", h.ctx.LastSeq)
	}
	h.do("SET", "k", "v", "NX") // refused: nothing appended
	if h.ctx.LastSeq != 4 {
		t.Fatalf("LastSeq = %d after a no-op", h.ctx.LastSeq)
	}
	want := "SET a 1 ; SET b 2 ; SET c 3 ; SET k v PXAT 1700000010000"
	if got := renderRaw(t, prop.records()); got != want {
		t.Fatalf("log is %q, want %q", got, want)
	}
}

func TestJournalDeleteLogsOnlyWhatExisted(t *testing.T) {
	h := newHarness(t)
	prop := &fakeProp{}
	h.ctx.EnableJournal(context.Background(), prop)
	h.do("SET", "gone", "v", "PX", "10")
	h.clk.Advance(time.Second)
	prop.set(func(p *fakeProp) { p.raw, p.seq = nil, 0 })

	// An expired key is not a deletion by DEL: the store's expiry hook logs
	// those, so DEL must not log it a second time.
	if got := h.do("DEL", "gone", "never"); got != rInt(0) {
		t.Fatalf("DEL = %q", got)
	}
	if n := len(prop.records()); n != 0 {
		t.Fatalf("DEL of keys that did not exist logged %d records", n)
	}
}

func TestJournalRefusesWritesWhilePersistenceIsBroken(t *testing.T) {
	h := newHarness(t)
	prop := &fakeProp{}
	h.ctx.EnableJournal(context.Background(), prop)
	h.do("SET", "k", "v")

	prop.set(func(p *fakeProp) { p.err = errors.New("disk full") })
	for _, args := range [][]string{
		{"SET", "a", "1"}, {"MSET", "a", "1"}, {"INCR", "n"}, {"LPUSH", "l", "x"},
		{"DEL", "k"}, {"FLUSHALL"}, {"EXPIRE", "k", "10"}, {"SET", "a", "1", "EX", "5"},
	} {
		if r := h.do(args...); !strings.HasPrefix(r, "-MISCONF ") {
			t.Fatalf("%v replied %q, want MISCONF", args, r)
		}
	}
	// Nothing was applied, and reads keep working.
	if h.do("GET", "k") != "$1\r\nv\r\n" || h.do("EXISTS", "a", "n", "l") != rInt(0) {
		t.Fatal("a refused write changed the dataset")
	}
	if h.do("PING") != "+PONG\r\n" || h.do("DBSIZE") != rInt(1) {
		t.Fatal("reads stopped working")
	}
	if prop.admits != 1 {
		t.Fatalf("Admit ran %d times; a refused command must not wait for log room", prop.admits)
	}

	prop.set(func(p *fakeProp) { p.err = nil })
	if r := h.do("SET", "a", "1"); r != "+OK\r\n" {
		t.Fatalf("after recovery: %q", r)
	}
}

func TestJournalAdmitIsOncePerWriteAndRefusalIsMisconf(t *testing.T) {
	h := newHarness(t)
	prop := &fakeProp{}
	h.ctx.EnableJournal(context.Background(), prop)
	h.do("SET", "a", "1")
	h.do("GET", "a")
	h.do("MSET", "x", "1", "y", "2")
	h.do("PING")
	if prop.admits != 2 {
		t.Fatalf("Admit ran %d times for two write commands and two reads", prop.admits)
	}
	prop.set(func(p *fakeProp) { p.admitErr = errors.New("closed") })
	if r := h.do("SET", "b", "1"); !strings.HasPrefix(r, "-MISCONF ") {
		t.Fatalf("got %q", r)
	}
	if h.do("EXISTS", "b") != rInt(0) {
		t.Fatal("a command refused by Admit was applied")
	}
}

// TestJournalAppendFailureAfterApply pins down what a client sees when the log
// refuses a record after the command was applied (it latched between Admit and
// Append): see the journal's documentation.
func TestJournalAppendFailureAfterApply(t *testing.T) {
	h := newHarness(t)
	prop := &fakeProp{}
	h.ctx.EnableJournal(context.Background(), prop)
	prop.set(func(p *fakeProp) { p.appendErr = errors.New("latched") })

	// Closure commands: the client gets MISCONF instead of the reply.
	if r := h.do("LPUSH", "l", "x"); !strings.HasPrefix(r, "-MISCONF ") {
		t.Fatalf("LPUSH replied %q", r)
	}
	if h.do("EXISTS", "l") != rInt(0) {
		t.Fatal("a Mutate that created a key must leave nothing behind when its record is refused")
	}
	if r := h.do("INCR", "n"); !strings.HasPrefix(r, "-MISCONF ") || h.do("EXISTS", "n") != rInt(0) {
		t.Fatalf("INCR replied %q", r)
	}
	if err := h.ctx.JournalErr(); err == nil {
		t.Fatal("JournalErr is empty after refused records")
	}
	if err := h.ctx.JournalErr(); err != nil {
		t.Fatal("JournalErr must be cleared once taken")
	}
	// MutateEntry changes are immediate, so the change stays in memory (documented).
	if r := h.do("SET", "b", "1", "EX", "10"); !strings.HasPrefix(r, "-MISCONF ") {
		t.Fatalf("SET EX replied %q", r)
	}
	if h.do("GET", "b") != "$1\r\n1\r\n" {
		t.Fatal("the in-memory change was expected to remain")
	}
	// Primitives cannot return an error to their handler: the reply goes out,
	// and JournalErr tells the server not to trust it.
	_ = h.ctx.JournalErr()
	if r := h.do("SET", "a", "1"); r != "+OK\r\n" {
		t.Fatalf("SET replied %q", r)
	}
	if h.ctx.JournalErr() == nil {
		t.Fatal("a refused SET record left no trace in JournalErr")
	}
	if h.do("DEL", "a") != rInt(1) || h.ctx.JournalErr() == nil {
		t.Fatal("DEL")
	}
	if h.ctx.LastSeq != 0 {
		t.Fatalf("LastSeq = %d although nothing was appended", h.ctx.LastSeq)
	}
}

// TestJournalOrderEqualsApplyOrderUnderConcurrentWriters hammers one key from
// many connections with writes that do not commute (APPEND, SET, DEL, EXPIRE).
// Replaying the log, in log order, on a fresh server must give the key's final
// value and TTL. That holds only if every record was appended while the key's
// shard lock was still held. Run it with -race.
func TestJournalOrderEqualsApplyOrderUnderConcurrentWriters(t *testing.T) {
	h := newHarness(t)
	shared := h.ctx.Store // the unjournaled store every connection uses
	prop := &fakeProp{}

	const writers, per = 8, 400
	var wg sync.WaitGroup
	for w := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			buf := &bytes.Buffer{}
			ctx := &Context{Store: shared, W: protocol.NewWriter(buf), Config: h.ctx.Config, Conn: &ConnState{ID: uint64(w + 2)}}
			ctx.EnableJournal(context.Background(), prop)
			r := rand.New(rand.NewPCG(uint64(w), 99))
			for i := range per {
				tok := fmt.Sprintf("<%d.%d>", w, i)
				var args []string
				switch n := r.IntN(100); {
				case n < 70:
					args = []string{"APPEND", "k", tok}
				case n < 85:
					args = []string{"SET", "k", "s" + tok}
				case n < 92:
					args = []string{"DEL", "k"}
				default:
					args = []string{"EXPIRE", "k", "1000"}
				}
				h.d.Dispatch(ctx, toArgv(args))
				_ = ctx.W.Flush()
				buf.Reset()
			}
		}()
	}
	wg.Wait()

	replica := newHarness(t)
	records := prop.records()
	if len(records) == 0 {
		t.Fatal("nothing was logged")
	}
	for _, rec := range records {
		if r := replica.do(parseRecord(t, rec)...); strings.HasPrefix(r, "-") {
			t.Fatalf("replaying %q failed: %q", renderRaw(t, [][]byte{rec}), r)
		}
	}
	for _, cmd := range [][]string{{"GET", "k"}, {"PTTL", "k"}} {
		if got, want := replica.do(cmd...), h.do(cmd...); got != want {
			t.Fatalf("%v: replaying the log gives %q but the live server holds %q", cmd, got, want)
		}
	}
}
