package persist

import (
	"slices"
	"strconv"
	"testing"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/store"
)

// *store.Store is a recovery Target, so main passes it straight to Recover.
var _ Target = (*store.Store)(nil)

// TestRecoverIntoTheRealStore repeats the "expired while the server was down"
// scenario with the real store.Load and store.BeginReplay instead of the test
// adapter.
func TestRecoverIntoTheRealStore(t *testing.T) {
	clk := store.NewFakeClock(baseMs)
	st, err := store.NewWithOptions(store.Options{Shards: 4, Clock: clk})
	mustDo(t, err)
	reg, err := command.NewDefaultRegistry()
	mustDo(t, err)

	d := newDir(t)
	ref := d.writeBase(1, []SnapshotEntry{
		{Key: "z", Value: mkString("1"), DeadlineMs: baseMs + 5_000, HasDeadline: true},
		{Key: "keep", Value: mkString("k")},
		{Key: "late", Value: mkString("L"), DeadlineMs: baseMs + 3_600_000, HasDeadline: true},
	})
	d.put("seg-000002.aof", segBytes(
		rec("INCR", "z"),
		rec("SET", "w", "1", "PXAT", strconv.Itoa(baseMs+2_000)),
		rec("APPEND", "w", "y"),
		rec("INCR", "keep2"),
	))
	d.commit(Manifest{Generation: 2, Base: ref, Segments: []string{"seg-000002.aof"}})

	clk.Set(baseMs + 10_000) // ten seconds of downtime
	res, err := Recover(tctx(t), RecoverOptions{
		FS: d.m, Dir: "data", Target: st, Replay: NewCommandReplayer(reg, st, nil),
	})
	mustDo(t, err)
	if res.Report.Records != 4 || res.Report.BaseKeys != 3 {
		t.Fatalf("report %+v", res.Report)
	}
	if got := st.NowMs(); got != baseMs+10_000 {
		t.Fatalf("the clock was left frozen at %d", got)
	}
	keys := st.Keys("*")
	slices.Sort(keys)
	if !slices.Equal(keys, []string{"keep", "keep2", "late"}) {
		t.Fatalf("live keys %v", keys)
	}
	// The zombies are still stored; PurgeExpired removes exactly them.
	if n := st.PurgeExpired(); n != 2 {
		t.Fatalf("PurgeExpired removed %d, want 2 (z and w)", n)
	}
}
