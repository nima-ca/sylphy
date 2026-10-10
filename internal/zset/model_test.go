package zset

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

// modelOps is the number of random operations per run.
const modelOps = 5000

func cmpEntry(a, b Entry) int {
	if c := cmp.Compare(a.Score, b.Score); c != 0 {
		return c
	}
	return strings.Compare(a.Member, b.Member)
}

// refSet is the naive reference: one slice kept sorted by (score, member),
// with linear scans everywhere. It shares no code with the skip list, and its
// range test is written independently of ScoreRange's helpers.
type refSet struct{ es []Entry }

func (r *refSet) size() int { return len(r.es) }

func (r *refSet) index(member string) int {
	for i, e := range r.es {
		if e.Member == member {
			return i
		}
	}
	return -1
}

func (r *refSet) score(member string) (float64, bool) {
	if i := r.index(member); i >= 0 {
		return r.es[i].Score, true
	}
	return 0, false
}

func (r *refSet) set(member string, score float64) {
	if i := r.index(member); i >= 0 {
		r.es = slices.Delete(r.es, i, i+1)
	}
	e := Entry{Member: member, Score: score}
	i, _ := slices.BinarySearchFunc(r.es, e, cmpEntry)
	r.es = slices.Insert(r.es, i, e)
}

func (r *refSet) remove(member string) bool {
	i := r.index(member)
	if i < 0 {
		return false
	}
	r.es = slices.Delete(r.es, i, i+1)
	return true
}

func (r *refSet) removeEntries(es []Entry) {
	for _, e := range es {
		r.remove(e.Member)
	}
}

func inRange(rg ScoreRange, s float64) bool {
	lo := s > rg.Min || (!rg.MinExclusive && s == rg.Min)
	hi := s < rg.Max || (!rg.MaxExclusive && s == rg.Max)
	return lo && hi
}

func (r *refSet) rangeByScore(rg ScoreRange, rev bool, offset, limit int) []Entry {
	var in []Entry
	for _, e := range r.es {
		if inRange(rg, e.Score) {
			in = append(in, e)
		}
	}
	if rev {
		slices.Reverse(in)
	}
	if offset < 0 || offset >= len(in) {
		return nil
	}
	in = in[offset:]
	if limit >= 0 && limit < len(in) {
		in = in[:limit]
	}
	return in
}

func (r *refSet) rangeByRank(lo, hi int, rev bool) []Entry {
	all := slices.Clone(r.es)
	if rev {
		slices.Reverse(all)
	}
	lo, hi = max(lo, 0), min(hi, len(all))
	if lo >= hi {
		return nil
	}
	return all[lo:hi]
}

func memberPool(rng *rand.Rand, n int) []string {
	seen := map[string]bool{"": true}
	out := []string{""}
	for len(out) < n {
		b := make([]byte, 1+rng.IntN(3))
		for i := range b {
			b[i] = byte(rng.IntN(256)) // binary-safe members, including 0x00 and 0xff
		}
		if s := string(b); !seen[s] {
			seen[s] = true
			out = append(out, s)
		}
	}
	return out
}

func verify(t *testing.T, z *ZSet, ref *refSet, step int, desc string, deep bool, pool []string) {
	t.Helper()
	if got := z.RangeByRank(0, z.Len(), false); !slices.Equal(got, ref.es) {
		t.Fatalf("step %d after %s: contents diverged\n got: %v\nwant: %v", step, desc, got, ref.es)
	}
	rev := slices.Clone(ref.es)
	slices.Reverse(rev)
	if got := z.RangeByRank(0, z.Len(), true); !slices.Equal(got, rev) {
		t.Fatalf("step %d after %s: reverse order diverged\n got: %v\nwant: %v", step, desc, got, rev)
	}
	if err := z.check(); err != nil {
		t.Fatalf("step %d after %s: %v", step, desc, err)
	}
	if !deep {
		return
	}
	for _, m := range pool {
		wantRank := ref.index(m)
		rank, ok := z.Rank(m)
		if ok != (wantRank >= 0) || (ok && rank != wantRank) {
			t.Fatalf("step %d after %s: Rank(%q) = (%d, %v), want %d", step, desc, m, rank, ok, wantRank)
		}
		wantScore, wantOK := ref.score(m)
		if s, ok := z.Score(m); ok != wantOK || s != wantScore {
			t.Fatalf("step %d after %s: Score(%q) = (%v, %v), want (%v, %v)", step, desc, m, s, ok, wantScore, wantOK)
		}
	}
}

func runModel(t *testing.T, z *ZSet, seed uint64) {
	t.Helper()
	rng := rand.New(rand.NewPCG(seed, 0xabcdef))
	pool := memberPool(rng, 150)
	ref := &refSet{}

	pickMember := func() string { return pool[rng.IntN(len(pool))] }
	pickScore := func() float64 {
		switch rng.IntN(10) {
		case 0:
			return inf
		case 1:
			return -inf
		case 2:
			return rng.Float64()*2000 - 1000
		default:
			return float64(rng.IntN(21)-10) / 2 // -5..5 in halves: lots of ties
		}
	}
	pickBound := func() float64 {
		switch rng.IntN(8) {
		case 0:
			return inf
		case 1:
			return -inf
		default:
			return float64(rng.IntN(25)-12) / 2 // lands on stored scores often
		}
	}
	pickRange := func() ScoreRange {
		lo, hi := pickBound(), pickBound()
		if lo > hi && rng.IntN(5) != 0 {
			lo, hi = hi, lo // mostly valid, sometimes inverted
		}
		return ScoreRange{Min: lo, Max: hi, MinExclusive: rng.IntN(2) == 0, MaxExclusive: rng.IntN(2) == 0}
	}

	for step := 0; step < modelOps; step++ {
		n := ref.size()
		op := rng.IntN(100)
		if step < 100 || n == 0 {
			op = 0 // build up a population first
		}
		var desc string
		bad := func(got, want any) {
			t.Helper()
			t.Fatalf("step %d %s: got %v, want %v", step, desc, got, want)
		}

		switch {
		case op < 40:
			m, s := pickMember(), pickScore()
			desc = fmt.Sprintf("Set(%q, %v)", m, s)
			wantOld, wantEx := ref.score(m)
			old, ex := z.Set(m, s)
			if ex != wantEx || old != wantOld {
				bad(fmt.Sprint(old, ex), fmt.Sprint(wantOld, wantEx))
			}
			ref.set(m, s)
		case op < 50:
			m := pickMember()
			desc = fmt.Sprintf("Remove(%q)", m)
			if got, want := z.Remove(m), ref.remove(m); got != want {
				bad(got, want)
			}
		case op < 55:
			m := pickMember()
			desc = fmt.Sprintf("Rank/Score(%q)", m)
			wantRank := ref.index(m)
			rank, ok := z.Rank(m)
			if ok != (wantRank >= 0) || (ok && rank != wantRank) {
				bad(fmt.Sprint(rank, ok), wantRank)
			}
		case op < 65:
			lo, hi, rev := rng.IntN(n+8)-4, rng.IntN(n+8)-4, rng.IntN(2) == 0
			desc = fmt.Sprintf("RangeByRank(%d, %d, rev=%v)", lo, hi, rev)
			if got, want := z.RangeByRank(lo, hi, rev), ref.rangeByRank(lo, hi, rev); !slices.Equal(got, want) {
				bad(got, want)
			}
		case op < 75:
			rg, rev := pickRange(), rng.IntN(2) == 0
			off, lim := rng.IntN(7)-1, rng.IntN(8)-1
			desc = fmt.Sprintf("RangeByScore(%+v, rev=%v, off=%d, lim=%d)", rg, rev, off, lim)
			if got, want := z.RangeByScore(rg, rev, off, lim), ref.rangeByScore(rg, rev, off, lim); !slices.Equal(got, want) {
				bad(got, want)
			}
		case op < 80:
			rg := pickRange()
			desc = fmt.Sprintf("Count(%+v)", rg)
			if got, want := z.Count(rg), len(ref.rangeByScore(rg, false, 0, -1)); got != want {
				bad(got, want)
			}
		case op < 85:
			rg := pickRange()
			desc = fmt.Sprintf("RemoveRangeByScore(%+v)", rg)
			want := ref.rangeByScore(rg, false, 0, -1)
			if got := z.RemoveRangeByScore(rg); got != len(want) {
				bad(got, len(want))
			}
			ref.removeEntries(want)
		case op < 90:
			lo, hi := rng.IntN(n+8)-4, rng.IntN(n+8)-4
			desc = fmt.Sprintf("RemoveRangeByRank(%d, %d)", lo, hi)
			want := ref.rangeByRank(lo, hi, false)
			if got := z.RemoveRangeByRank(lo, hi); got != len(want) {
				bad(got, len(want))
			}
			ref.removeEntries(want)
		case op < 95:
			k := rng.IntN(7) - 1
			desc = fmt.Sprintf("PopMin(%d)", k)
			want := slices.Clone(ref.es[:min(max(k, 0), n)])
			if got := z.PopMin(k); !slices.Equal(got, want) {
				bad(got, want)
			}
			ref.removeEntries(want)
		default:
			k := rng.IntN(7) - 1
			desc = fmt.Sprintf("PopMax(%d)", k)
			want := slices.Clone(ref.es[n-min(max(k, 0), n):])
			slices.Reverse(want)
			if got := z.PopMax(k); !slices.Equal(got, want) {
				bad(got, want)
			}
			ref.removeEntries(want)
		}
		verify(t, z, ref, step, desc, step%50 == 0, pool)
	}
}

// TestModelAgainstNaiveReference replays thousands of seeded random operations
// against the skip list and a sorted-slice reference and compares the full
// contents, both directions and every skip-list invariant after each step.
// The degenerate height sources turn the skip list into a plain linked list
// and into a stack of 32 full levels, the two extremes of the span arithmetic.
func TestModelAgainstNaiveReference(t *testing.T) {
	variants := []struct {
		name string
		mk   func(seed uint64) *ZSet
	}{
		{"random", func(seed uint64) *ZSet {
			r := rand.New(rand.NewPCG(seed, 7))
			return NewWithRand(r.IntN)
		}},
		{"linked-list", func(uint64) *ZSet { return NewWithRand(func(int) int { return 1 }) }},
		{"full-height", func(uint64) *ZSet { return NewWithRand(func(int) int { return 0 }) }},
	}
	for _, v := range variants {
		for seed := uint64(1); seed <= 4; seed++ {
			t.Run(fmt.Sprintf("%s/seed%d", v.name, seed), func(t *testing.T) {
				runModel(t, v.mk(seed), seed)
			})
		}
	}
}
