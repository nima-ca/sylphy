package zset

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/nima-ca/sylphy/internal/store"
)

var inf = math.Inf(1)

func newSeeded(seed uint64) *ZSet {
	r := rand.New(rand.NewPCG(seed, seed+1))
	return NewWithRand(r.IntN)
}

func memberNames(es []Entry) []string {
	out := make([]string, len(es))
	for i, e := range es {
		out[i] = e.Member
	}
	return out
}

func mustCheck(t *testing.T, z *ZSet) {
	t.Helper()
	if err := z.check(); err != nil {
		t.Fatalf("invariant violated: %v", err)
	}
}

func build(t *testing.T, z *ZSet, es ...Entry) {
	t.Helper()
	for _, e := range es {
		z.Set(e.Member, e.Score)
	}
	mustCheck(t, z)
}

// fixture returns a set holding n:-inf, a:1 .. e:5, z:+inf.
func fixture(t *testing.T) *ZSet {
	t.Helper()
	z := newSeeded(7)
	build(t, z, Entry{"n", -inf}, Entry{"a", 1}, Entry{"b", 2}, Entry{"c", 3},
		Entry{"d", 4}, Entry{"e", 5}, Entry{"z", inf})
	return z
}

func TestKindAndLen(t *testing.T) {
	z := New()
	if z.Kind() != store.KindZSet {
		t.Fatalf("Kind = %v", z.Kind())
	}
	if z.Len() != 0 {
		t.Fatalf("Len = %d", z.Len())
	}
	z.Set("a", 1)
	if z.Len() != 1 {
		t.Fatalf("Len = %d", z.Len())
	}
}

func TestSetReportsPreviousScore(t *testing.T) {
	z := newSeeded(1)
	steps := []struct {
		member  string
		score   float64
		wantOld float64
		wantEx  bool
	}{
		{"a", 1, 0, false},
		{"a", 1, 1, true}, // unchanged score is a no-op
		{"a", 2, 1, true},
		{"b", 2, 0, false},
		{"a", -inf, 2, true},
	}
	for _, s := range steps {
		old, ex := z.Set(s.member, s.score)
		if old != s.wantOld || ex != s.wantEx {
			t.Errorf("Set(%q, %v) = (%v, %v), want (%v, %v)", s.member, s.score, old, ex, s.wantOld, s.wantEx)
		}
		if got, ok := z.Score(s.member); !ok || got != s.score {
			t.Errorf("Score(%q) = (%v, %v), want %v", s.member, got, ok, s.score)
		}
	}
	mustCheck(t, z)
	if _, ok := z.Score("missing"); ok {
		t.Error("Score of a missing member reported ok")
	}
}

func TestOrderByScoreThenMemberBytes(t *testing.T) {
	z := newSeeded(2)
	build(t, z,
		Entry{"b", 1}, Entry{"a", 1}, Entry{"\xff", 1}, Entry{"\x00", 1}, Entry{"ab", 1},
		Entry{"hi", inf}, Entry{"lo", -inf}, Entry{"", 1})
	want := []string{"lo", "", "\x00", "a", "ab", "b", "\xff", "hi"}
	if got := memberNames(z.RangeByRank(0, z.Len(), false)); !slices.Equal(got, want) {
		t.Fatalf("order = %q, want %q", got, want)
	}
}

func TestRank(t *testing.T) {
	z := fixture(t)
	for i, m := range []string{"n", "a", "b", "c", "d", "e", "z"} {
		if r, ok := z.Rank(m); !ok || r != i {
			t.Errorf("Rank(%q) = (%d, %v), want %d", m, r, ok, i)
		}
	}
	if _, ok := z.Rank("missing"); ok {
		t.Error("Rank of a missing member reported ok")
	}
}

func TestRemove(t *testing.T) {
	z := fixture(t)
	if !z.Remove("c") || z.Remove("c") || z.Remove("missing") {
		t.Fatal("Remove results wrong")
	}
	mustCheck(t, z)
	if r, _ := z.Rank("d"); r != 3 {
		t.Errorf("Rank(d) after removing c = %d, want 3", r)
	}
	if z.Len() != 6 {
		t.Errorf("Len = %d", z.Len())
	}
}

func TestRangeByRank(t *testing.T) {
	z := newSeeded(3)
	build(t, z, Entry{"a", 1}, Entry{"b", 2}, Entry{"c", 3}, Entry{"d", 4}, Entry{"e", 5})
	tests := []struct {
		name   string
		lo, hi int
		rev    bool
		want   []string
	}{
		{"all", 0, 5, false, []string{"a", "b", "c", "d", "e"}},
		{"all reversed", 0, 5, true, []string{"e", "d", "c", "b", "a"}},
		{"middle", 1, 3, false, []string{"b", "c"}},
		{"middle reversed", 1, 3, true, []string{"d", "c"}},
		{"negative lo clamps", -3, 2, false, []string{"a", "b"}},
		{"hi clamps", 3, 100, false, []string{"d", "e"}},
		{"hi clamps reversed", 3, 100, true, []string{"b", "a"}},
		{"inverted", 4, 2, false, nil},
		{"past the end", 5, 9, false, nil},
		{"empty", 0, 0, false, nil},
	}
	for _, tt := range tests {
		got := memberNames(z.RangeByRank(tt.lo, tt.hi, tt.rev))
		if !slices.Equal(got, tt.want) {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
	got := z.RangeByRank(0, 2, false)
	if got[1].Score != 2 {
		t.Errorf("score not returned: %+v", got)
	}
}

func TestRangeByScore(t *testing.T) {
	z := fixture(t)
	all := []string{"n", "a", "b", "c", "d", "e", "z"}
	tests := []struct {
		name          string
		r             ScoreRange
		rev           bool
		offset, limit int
		want          []string
	}{
		{"inclusive", ScoreRange{Min: 2, Max: 4}, false, 0, -1, []string{"b", "c", "d"}},
		{"both exclusive", ScoreRange{Min: 2, Max: 4, MinExclusive: true, MaxExclusive: true}, false, 0, -1, []string{"c"}},
		{"max exclusive", ScoreRange{Min: 2, Max: 4, MaxExclusive: true}, false, 0, -1, []string{"b", "c"}},
		{"everything", ScoreRange{Min: -inf, Max: inf}, false, 0, -1, all},
		{"infinities excluded", ScoreRange{Min: -inf, Max: inf, MinExclusive: true, MaxExclusive: true}, false, 0, -1, []string{"a", "b", "c", "d", "e"}},
		{"only -inf", ScoreRange{Min: -inf, Max: -inf}, false, 0, -1, []string{"n"}},
		{"point", ScoreRange{Min: 3, Max: 3}, false, 0, -1, []string{"c"}},
		{"exclusive point", ScoreRange{Min: 3, Max: 3, MinExclusive: true}, false, 0, -1, nil},
		{"inverted", ScoreRange{Min: 4, Max: 2}, false, 0, -1, nil},
		{"below all finite", ScoreRange{Min: -100, Max: -50}, false, 0, -1, nil},
		{"above all finite", ScoreRange{Min: 6, Max: 100}, false, 0, -1, nil},
		{"reverse", ScoreRange{Min: 2, Max: 4}, true, 0, -1, []string{"d", "c", "b"}},
		{"reverse everything", ScoreRange{Min: -inf, Max: inf}, true, 0, -1, []string{"z", "e", "d", "c", "b", "a", "n"}},
		{"offset and limit", ScoreRange{Min: 1, Max: 5}, false, 1, 2, []string{"b", "c"}},
		{"offset without limit", ScoreRange{Min: 1, Max: 5}, false, 1, -1, []string{"b", "c", "d", "e"}},
		{"limit zero", ScoreRange{Min: 1, Max: 5}, false, 0, 0, nil},
		{"offset past end", ScoreRange{Min: 1, Max: 5}, false, 10, -1, nil},
		{"negative offset", ScoreRange{Min: 1, Max: 5}, false, -1, -1, nil},
		{"reverse offset and limit", ScoreRange{Min: 1, Max: 5}, true, 1, 2, []string{"d", "c"}},
	}
	for _, tt := range tests {
		got := memberNames(z.RangeByScore(tt.r, tt.rev, tt.offset, tt.limit))
		if !slices.Equal(got, tt.want) {
			t.Errorf("%s: got %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestCount(t *testing.T) {
	z := fixture(t)
	tests := []struct {
		name string
		r    ScoreRange
		want int
	}{
		{"inclusive", ScoreRange{Min: 2, Max: 4}, 3},
		{"exclusive", ScoreRange{Min: 2, Max: 4, MinExclusive: true, MaxExclusive: true}, 1},
		{"everything", ScoreRange{Min: -inf, Max: inf}, 7},
		{"finite only", ScoreRange{Min: -inf, Max: inf, MinExclusive: true, MaxExclusive: true}, 5},
		{"inverted", ScoreRange{Min: 4, Max: 2}, 0},
		{"point", ScoreRange{Min: 3, Max: 3}, 1},
		{"exclusive point", ScoreRange{Min: 3, Max: 3, MaxExclusive: true}, 0},
		{"beyond", ScoreRange{Min: 10, Max: 20}, 0},
		{"only -inf", ScoreRange{Min: -inf, Max: -inf}, 1},
		{"top", ScoreRange{Min: 5, Max: inf}, 2},
	}
	for _, tt := range tests {
		if got := z.Count(tt.r); got != tt.want {
			t.Errorf("%s: Count = %d, want %d", tt.name, got, tt.want)
		}
	}
}

func tenMembers(t *testing.T) *ZSet {
	t.Helper()
	z := newSeeded(11)
	for i := 0; i < 10; i++ {
		z.Set("m"+strconv.Itoa(i), float64(i))
	}
	mustCheck(t, z)
	return z
}

func TestRemoveRangeByRank(t *testing.T) {
	z := tenMembers(t)
	if got := z.RemoveRangeByRank(2, 5); got != 3 {
		t.Fatalf("removed %d, want 3", got)
	}
	mustCheck(t, z)
	for _, m := range []string{"m2", "m3", "m4"} {
		if _, ok := z.Score(m); ok {
			t.Errorf("%s survived", m)
		}
	}
	if got := z.RemoveRangeByRank(-5, 1); got != 1 { // clamps to [0, 1)
		t.Fatalf("removed %d, want 1", got)
	}
	if got := z.RemoveRangeByRank(4, 2); got != 0 {
		t.Fatalf("inverted range removed %d", got)
	}
	if got := z.RemoveRangeByRank(0, 100); got != 6 {
		t.Fatalf("removed %d, want the remaining 6", got)
	}
	if z.Len() != 0 {
		t.Fatalf("Len = %d", z.Len())
	}
	mustCheck(t, z)
}

func TestRemoveRangeByScore(t *testing.T) {
	z := tenMembers(t)
	if got := z.RemoveRangeByScore(ScoreRange{Min: 3, Max: 6}); got != 4 {
		t.Fatalf("removed %d, want 4", got)
	}
	mustCheck(t, z)
	want := []string{"m0", "m1", "m2", "m7", "m8", "m9"}
	if got := memberNames(z.RangeByRank(0, z.Len(), false)); !slices.Equal(got, want) {
		t.Fatalf("left %q, want %q", got, want)
	}
	if got := z.RemoveRangeByScore(ScoreRange{Min: 50, Max: 60}); got != 0 {
		t.Fatalf("removed %d from an empty range", got)
	}
	if got := z.RemoveRangeByScore(ScoreRange{Min: -inf, Max: inf}); got != 6 {
		t.Fatalf("removed %d, want 6", got)
	}
	mustCheck(t, z)
}

func TestPop(t *testing.T) {
	z := tenMembers(t)
	if got := memberNames(z.PopMin(2)); !slices.Equal(got, []string{"m0", "m1"}) {
		t.Errorf("PopMin(2) = %q", got)
	}
	if got := memberNames(z.PopMax(2)); !slices.Equal(got, []string{"m9", "m8"}) {
		t.Errorf("PopMax(2) = %q", got)
	}
	if z.PopMin(0) != nil || z.PopMin(-1) != nil || z.PopMax(0) != nil {
		t.Error("non-positive count must pop nothing")
	}
	mustCheck(t, z)
	if got := memberNames(z.PopMax(100)); !slices.Equal(got, []string{"m7", "m6", "m5", "m4", "m3", "m2"}) {
		t.Errorf("PopMax(100) = %q", got)
	}
	if z.Len() != 0 || z.PopMin(1) != nil {
		t.Error("set should be empty")
	}
	mustCheck(t, z)
}

func TestHeightClamping(t *testing.T) {
	tall := NewWithRand(func(int) int { return 0 }) // always promotes
	build(t, tall, Entry{"a", 1}, Entry{"b", 2}, Entry{"c", 3})
	if tall.sl.level != maxLevel {
		t.Errorf("level = %d, want %d", tall.sl.level, maxLevel)
	}
	flat := NewWithRand(func(int) int { return 1 }) // never promotes
	build(t, flat, Entry{"a", 1}, Entry{"b", 2}, Entry{"c", 3})
	if flat.sl.level != 1 {
		t.Errorf("level = %d, want 1", flat.sl.level)
	}
	flat.Remove("b")
	tall.Remove("b")
	mustCheck(t, flat)
	mustCheck(t, tall)
}

func shape(z *ZSet) []int {
	var out []int
	for x := z.sl.header.levels[0].forward; x != nil; x = x.levels[0].forward {
		out = append(out, len(x.levels))
	}
	return out
}

func TestSeededStructureIsDeterministic(t *testing.T) {
	a, b := newSeeded(99), newSeeded(99)
	for i := 0; i < 50; i++ {
		a.Set(strconv.Itoa(i), float64(i%7))
		b.Set(strconv.Itoa(i), float64(i%7))
	}
	if sa, sb := shape(a), shape(b); len(sa) != 50 || !slices.Equal(sa, sb) {
		t.Fatalf("same seed gave different shapes:\n%v\n%v", sa, sb)
	}
}

func TestLevelDistribution(t *testing.T) {
	z := newSeeded(5)
	const n = 20000
	for i := 0; i < n; i++ {
		z.Set(strconv.Itoa(i), float64(i))
	}
	tall := 0
	for _, h := range shape(z) {
		if h >= 2 {
			tall++
		}
	}
	if ratio := float64(tall) / n; ratio < 0.22 || ratio > 0.28 {
		t.Fatalf("fraction of nodes with >= 2 levels = %.3f, want about 0.25", ratio)
	}
}

func TestRankMatchesPositionAtScale(t *testing.T) {
	z := newSeeded(42)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := 0; i < 5000; i++ {
		z.Set(strconv.Itoa(i), float64(rng.IntN(500))) // many ties
	}
	mustCheck(t, z)
	all := z.RangeByRank(0, z.Len(), false)
	if !slices.IsSortedFunc(all, cmpEntry) {
		t.Fatal("entries not sorted by (score, member)")
	}
	for i, e := range all {
		if n := z.sl.nodeByRank(i + 1); n == nil || n.member != e.Member {
			t.Fatalf("nodeByRank(%d) is wrong", i+1)
		}
		if r, ok := z.Rank(e.Member); !ok || r != i {
			t.Fatalf("Rank(%q) = (%d, %v), want %d", e.Member, r, ok, i)
		}
	}
	if z.sl.nodeByRank(0) != nil || z.sl.nodeByRank(len(all)+1) != nil {
		t.Fatal("nodeByRank must reject out-of-range ranks")
	}
}

func TestCompactShrinksMemberMap(t *testing.T) {
	z := newSeeded(8)
	for i := 0; i < 1000; i++ {
		z.Set(strconv.Itoa(i), float64(i))
	}
	if z.peak != 1000 {
		t.Fatalf("peak = %d", z.peak)
	}
	for i := 5; i < 1000; i++ {
		z.Remove(strconv.Itoa(i))
	}
	if z.peak >= 100 {
		t.Errorf("peak = %d after mass removal, map was not rebuilt", z.peak)
	}
	mustCheck(t, z)
	if got := memberNames(z.RangeByRank(0, 10, false)); len(got) != 5 {
		t.Errorf("left %q", got)
	}
}

func TestStoreDeletesEmptyZSet(t *testing.T) {
	st, err := store.New(4)
	if err != nil {
		t.Fatal(err)
	}
	z := New()
	z.Set("a", 1)
	if err := st.Mutate("k", func(store.Value) (store.Value, error) { return z, nil }); err != nil {
		t.Fatal(err)
	}
	if st.Exists("k") != 1 {
		t.Fatal("key missing after store")
	}
	err = st.Mutate("k", func(cur store.Value) (store.Value, error) {
		zz := cur.(*ZSet)
		zz.Remove("a")
		return zz, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if st.Exists("k") != 0 {
		t.Fatal("empty sorted set was not deleted by the store")
	}
}
