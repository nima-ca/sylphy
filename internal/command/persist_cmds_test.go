package command

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

type stubPersistence struct {
	saveErr, bgErr error
	aof            bool
	last           time.Time
	saves, bgs     int
}

var _ Persistence = (*stubPersistence)(nil)

func (p *stubPersistence) Save(context.Context) error { p.saves++; return p.saveErr }
func (p *stubPersistence) BGSave() error              { p.bgs++; return p.bgErr }
func (p *stubPersistence) LastSave() time.Time        { return p.last }
func (p *stubPersistence) AOFEnabled() bool           { return p.aof }

func TestPersistenceCommandsWhenOff(t *testing.T) {
	h := newHarness(t)
	for _, name := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF"} {
		if got := h.do(name); !strings.HasPrefix(got, "-ERR persistence is disabled") {
			t.Errorf("%s replied %q", name, got)
		}
	}
	if got := h.do("LASTSAVE"); got != rInt(0) {
		t.Errorf("LASTSAVE = %q", got)
	}
}

func TestPersistenceCommands(t *testing.T) {
	h := newHarness(t)
	p := &stubPersistence{last: time.Unix(1_700_000_123, 999), aof: true}
	h.ctx.Persist = p

	if got := h.do("SAVE"); got != "+OK\r\n" || p.saves != 1 {
		t.Fatalf("SAVE = %q (%d saves)", got, p.saves)
	}
	if got := h.do("save"); got != "+OK\r\n" { // names are case-insensitive
		t.Fatalf("save = %q", got)
	}
	if got := h.do("BGSAVE"); got != "+Background saving started\r\n" || p.bgs != 1 {
		t.Fatalf("BGSAVE = %q (%d)", got, p.bgs)
	}
	if got := h.do("BGREWRITEAOF"); got != "+Background append only file rewriting started\r\n" || p.bgs != 2 {
		t.Fatalf("BGREWRITEAOF = %q (%d)", got, p.bgs)
	}
	if got := h.do("LASTSAVE"); got != rInt(1_700_000_123) {
		t.Fatalf("LASTSAVE = %q", got)
	}
	for _, name := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF", "LASTSAVE"} {
		if got := h.do(name, "extra"); !strings.HasPrefix(got, "-ERR wrong number of arguments") {
			t.Errorf("%s with an argument replied %q", name, got)
		}
	}
}

func TestPersistenceCommandsWhileACheckpointRuns(t *testing.T) {
	h := newHarness(t)
	p := &stubPersistence{saveErr: ErrCheckpointRunning, bgErr: ErrCheckpointRunning, aof: true}
	h.ctx.Persist = p
	want := map[string]string{
		"SAVE":         "-ERR Background save already in progress\r\n",
		"BGSAVE":       "-ERR Background save already in progress\r\n",
		"BGREWRITEAOF": "-ERR Background append only file rewriting already in progress\r\n",
	}
	for name, w := range want {
		if got := h.do(name); got != w {
			t.Errorf("%s replied %q, want %q", name, got, w)
		}
	}
}

func TestPersistenceCommandFailureIsAnInternalError(t *testing.T) {
	h := newHarness(t)
	h.ctx.Persist = &stubPersistence{saveErr: errors.New("/secret/path: disk on fire"), bgErr: errors.New("also secret")}
	for _, name := range []string{"SAVE", "BGSAVE"} {
		if got := h.do(name); got != "-ERR internal error\r\n" {
			t.Errorf("%s replied %q: internals must not leak", name, got)
		}
	}
}

func TestBGRewriteAOFNeedsTheLog(t *testing.T) {
	h := newHarness(t)
	p := &stubPersistence{aof: false}
	h.ctx.Persist = p
	if got := h.do("BGREWRITEAOF"); !strings.HasPrefix(got, "-ERR the append-only file is not enabled") || p.bgs != 0 {
		t.Fatalf("got %q (%d checkpoints started)", got, p.bgs)
	}
	if got := h.do("BGSAVE"); got != "+Background saving started\r\n" { // snapshot mode can still BGSAVE
		t.Fatalf("BGSAVE = %q", got)
	}
}

// The persistence commands are admin commands, not writes: they must work
// while persistence is failed (a checkpoint is how it recovers) and are never
// logged.
func TestPersistenceCommandsAreNotWrites(t *testing.T) {
	h := newHarness(t)
	h.ctx.Persist = &stubPersistence{aof: true, last: time.Unix(42, 0)}
	prop := &fakeProp{err: errors.New("disk full")}
	h.ctx.EnableJournal(context.Background(), prop)
	for _, c := range []struct{ name, want string }{
		{"BGSAVE", "+Background saving started\r\n"}, {"SAVE", "+OK\r\n"},
		{"BGREWRITEAOF", "+Background append only file rewriting started\r\n"}, {"LASTSAVE", rInt(42)},
	} {
		if got := h.do(c.name); got != c.want {
			t.Errorf("%s while persistence is failed: %q, want %q", c.name, got, c.want)
		}
	}
	if prop.admits != 0 || len(prop.records()) != 0 {
		t.Fatalf("persistence commands touched the log: %d admits, %d records", prop.admits, len(prop.records()))
	}
	if got := h.do("SET", "k", "v"); !strings.HasPrefix(got, "-MISCONF") {
		t.Fatalf("a write must still be refused: %q", got)
	}
}

func TestPersistenceCommandsAreRegisteredAsAdmin(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"SAVE", "BGSAVE", "BGREWRITEAOF", "LASTSAVE"} {
		sp, ok := r.Lookup([]byte(name))
		if !ok {
			t.Fatalf("%s is not registered", name)
		}
		if sp.Flags&FlagAdmin == 0 || sp.Flags&FlagWrite != 0 || sp.Arity != 1 || sp.FirstKey != 0 {
			t.Errorf("%s: %+v", name, sp)
		}
	}
}
