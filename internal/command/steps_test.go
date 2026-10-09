package command

import (
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/store"
)

// Reply fixtures shared by the Phase 2 command tests.
const (
	rOK        = "+OK\r\n"
	rNil       = "$-1\r\n"
	rSyntax    = "-ERR syntax error\r\n"
	rNotInt    = "-ERR value is not an integer or out of range\r\n"
	rWrongType = "-WRONGTYPE Operation against a key holding the wrong kind of value\r\n"
)

func rInt(n int64) string    { return ":" + strconv.FormatInt(n, 10) + "\r\n" }
func rBulk(s string) string  { return fmt.Sprintf("$%d\r\n%s\r\n", len(s), s) }
func rErr(msg string) string { return "-" + msg + "\r\n" }
func rBadExpire(cmd string) string {
	return rErr("ERR invalid expire time in '" + cmd + "' command")
}

// step is one scripted command. adv advances the fake clock before the
// command runs; a step with no args only advances the clock.
type step struct {
	args []string
	want string
	adv  time.Duration
}

func do(want string, args ...string) step { return step{args: args, want: want} }
func wait(d time.Duration) step           { return step{adv: d} }

func (h *harness) run(t *testing.T, steps []step) {
	t.Helper()
	for _, s := range steps {
		if s.adv > 0 {
			h.clk.Advance(s.adv)
		}
		if s.args == nil {
			continue
		}
		if got := h.do(s.args...); got != s.want {
			t.Errorf("%v\n got: %q\nwant: %q", s.args, got, s.want)
		}
	}
}

// testList is a stand-in Collection so handlers can be tested against a key of
// the wrong kind before real lists exist.
type testList struct{ n int }

func (*testList) Kind() store.Kind { return store.KindList }
func (l *testList) Len() int       { return l.n }

func (h *harness) putList(t *testing.T, key string) {
	t.Helper()
	st := h.ctx.Store.(*fakeStore)
	err := st.Mutate(key, func(store.Value) (store.Value, error) { return &testList{n: 1}, nil })
	if err != nil {
		t.Fatal(err)
	}
}
