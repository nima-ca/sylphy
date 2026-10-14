package persist

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/nima-ca/sylphy/internal/command"
)

var errCmdRunning = command.ErrCheckpointRunning

func TestCommandsAdapterThroughTheDispatcher(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeAOF, policy: PolicyAlways})
	s.ctx.Persist = NewCommands(s.m)
	s.mustAck("SET", "a", "1")

	s.now.Store(1_700_000_777)
	if r := s.do("SAVE"); r != "+OK\r\n" {
		t.Fatalf("SAVE = %q", r)
	}
	if r := s.do("LASTSAVE"); r != ":1700000777\r\n" {
		t.Fatalf("LASTSAVE = %q", r)
	}
	if man := s.manifest(); man.Base.Name == "" {
		t.Fatalf("SAVE wrote no base: %+v", man)
	}

	// A checkpoint in progress is reported, not queued.
	hit, release := make(chan struct{}), make(chan struct{})
	rel := sync.OnceFunc(func() { close(release) })
	t.Cleanup(rel)
	var once sync.Once
	fs.SetHook(func(op Op) error {
		if op.Kind == OpWrite && strings.HasSuffix(op.Name, ".snap.tmp") {
			once.Do(func() {
				close(hit)
				<-release
			})
		}
		return nil
	})
	if r := s.do("BGSAVE"); r != "+Background saving started\r\n" {
		t.Fatalf("BGSAVE = %q", r)
	}
	<-hit
	for cmd, want := range map[string]string{
		"BGSAVE":       "-ERR Background save already in progress\r\n",
		"SAVE":         "-ERR Background save already in progress\r\n",
		"BGREWRITEAOF": "-ERR Background append only file rewriting already in progress\r\n",
	} {
		if r := s.do(cmd); r != want {
			t.Errorf("%s during a checkpoint: %q", cmd, r)
		}
	}
	rel()
	waitBG(t, s.m)
	if r := s.do("BGREWRITEAOF"); r != "+Background append only file rewriting started\r\n" {
		t.Fatalf("BGREWRITEAOF = %q", r)
	}
	waitBG(t, s.m)
}

func TestCommandsAdapterInSnapshotMode(t *testing.T) {
	fs := newMemDir(t)
	s := newSession(t, fs, sessOpts{mode: ModeSnapshot})
	c := NewCommands(s.m)
	s.ctx.Persist = c
	if c.AOFEnabled() {
		t.Fatal("snapshot mode has no append-only file")
	}
	if r := s.do("BGREWRITEAOF"); !strings.HasPrefix(r, "-ERR the append-only file is not enabled") {
		t.Fatalf("BGREWRITEAOF = %q", r)
	}
	s.mustAck("SET", "a", "1")
	if r := s.do("SAVE"); r != "+OK\r\n" {
		t.Fatalf("SAVE = %q", r)
	}
	if man := s.manifest(); man.Base.Name == "" || len(man.Segments) != 0 {
		t.Fatalf("manifest %+v", man)
	}
}

func TestCommandsAdapterTranslatesErrors(t *testing.T) {
	if !errors.Is(translate(ErrCheckpointRunning), errCmdRunning) {
		t.Fatal("ErrCheckpointRunning must map to the command package's sentinel")
	}
	boom := errors.New("boom")
	if translate(boom) != boom || translate(nil) != nil {
		t.Fatal("other errors must pass through")
	}
}
