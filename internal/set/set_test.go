package set

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/nima-ca/sylphy/internal/store"
)

func checkInvariants(t testing.TB, s *Set) {
	t.Helper()
	if len(s.members) != len(s.pos) {
		t.Fatalf("len(members)=%d but len(pos)=%d", len(s.members), len(s.pos))
	}
	for i, m := range s.members {
		if got, ok := s.pos[m]; !ok || got != i {
			t.Fatalf("pos[%q] = %d,%v; want %d", m, got, ok, i)
		}
	}
	if cap(s.members) > shrinkMinCap && len(s.members) <= cap(s.members)/4 {
		t.Fatalf("cap %d not shrunk for len %d", cap(s.members), len(s.members))
	}
}

func sorted(ss []string) []string {
	out := slices.Clone(ss)
	slices.Sort(out)
	return out
}

func filled(n int) *Set {
	s := New()
	for i := 0; i < n; i++ {
		s.Add("m" + strconv.Itoa(i))
	}
	return s
}

func TestKindAndCollection(t *testing.T) {
	var v store.Value = New()
	if v.Kind() != store.KindSet {
		t.Fatalf("Kind = %v", v.Kind())
	}
	if c, ok := v.(store.Collection); !ok || c.Len() != 0 {
		t.Fatal("a Set must be an empty store.Collection")
	}
}

func TestZeroValueIsUsable(t *testing.T) {
	var s Set
	if s.Has("x") || s.Remove("x") || s.Len() != 0 {
		t.Fatal("empty zero value reported content")
	}
	if !s.Add("x") || s.Add("x") || s.Len() != 1 {
		t.Fatal("Add on the zero value misbehaved")
	}
	checkInvariants(t, &s)
}

func TestAddRemoveHas(t *testing.T) {
	s := New()
	for _, m := range []string{"a", "b", "c"} {
		if !s.Add(m) {
			t.Fatalf("Add(%q) reported duplicate", m)
		}
	}
	if s.Add("b") {
		t.Fatal("duplicate Add must report false")
	}
	if !s.Has("a") || s.Has("z") {
		t.Fatal("Has is wrong")
	}
	// Documented behavior: the last member fills the hole.
	if !s.Remove("a") || s.Remove("a") {
		t.Fatal("Remove must report true once")
	}
	if got := s.Members(); !slices.Equal(got, []string{"c", "b"}) {
		t.Fatalf("Members = %v", got)
	}
	if s.At(0) != "c" || s.At(1) != "b" || s.At(2) != "" || s.At(-1) != "" {
		t.Fatal("At is wrong")
	}
	checkInvariants(t, s)
}

func TestMembersIsACopyAndRangeStops(t *testing.T) {
	s := filled(5)
	m := s.Members()
	m[0] = "changed"
	if s.At(0) != "m0" {
		t.Fatal("Members aliased the set")
	}
	n := 0
	s.Range(func(string) bool { n++; return n < 3 })
	if n != 3 {
		t.Fatalf("Range visited %d members, want 3", n)
	}
}

func TestEmptyAndBinaryMembers(t *testing.T) {
	s := New()
	s.Add("")
	s.Add("a\r\n\x00")
	if !s.Has("") || !s.Has("a\r\n\x00") || s.Len() != 2 {
		t.Fatal("empty or binary member lost")
	}
}

func TestShrinkAfterMassRemove(t *testing.T) {
	s := filled(5000)
	for i := 0; i < 4990; i++ {
		if !s.Remove("m" + strconv.Itoa(i)) {
			t.Fatalf("Remove %d failed", i)
		}
		checkInvariants(t, s)
	}
	if cap(s.members) > 64 {
		t.Fatalf("capacity %d after shrinking to %d members", cap(s.members), s.Len())
	}
}

func TestSampleIsDistinctAndBounded(t *testing.T) {
	rng := rand.New(rand.NewPCG(5, 6))
	s := filled(50)
	for _, k := range []int{-3, 0, 1, 10, 49, 50, 51, 1 << 40} {
		got := s.Sample(k, rng.IntN)
		want := max(min(k, 50), 0)
		if len(got) != want {
			t.Fatalf("Sample(%d) returned %d members, want %d", k, len(got), want)
		}
		seen := map[string]bool{}
		for _, m := range got {
			if !s.Has(m) || seen[m] {
				t.Fatalf("Sample(%d): %q unknown or repeated", k, m)
			}
			seen[m] = true
		}
	}
	if s.Len() != 50 {
		t.Fatal("Sample modified the set")
	}
	if got := New().Sample(3, rng.IntN); got != nil {
		t.Fatalf("Sample on an empty set = %v", got)
	}
}

func TestSampleRepeat(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	s := filled(3)
	got := s.SampleRepeat(200, rng.IntN)
	if len(got) != 200 {
		t.Fatalf("len = %d", len(got))
	}
	seen := map[string]bool{}
	for _, m := range got {
		if !s.Has(m) {
			t.Fatalf("unknown member %q", m)
		}
		seen[m] = true
	}
	if len(seen) != 3 {
		t.Fatalf("200 draws from 3 members produced only %d distinct", len(seen))
	}
	if New().SampleRepeat(5, rng.IntN) != nil || s.SampleRepeat(0, rng.IntN) != nil {
		t.Fatal("empty set or k <= 0 must return nil")
	}
}

// Fixed seed, wide margins: a biased shuffle would miss these bounds by many
// standard deviations, an unbiased one cannot get near them.
func TestSampleIsUniform(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	s := filled(4)
	one, two := map[string]int{}, map[string]int{}
	const rounds = 4000
	for i := 0; i < rounds; i++ {
		one[s.Sample(1, rng.IntN)[0]]++
		for _, m := range s.Sample(2, rng.IntN) {
			two[m]++
		}
	}
	for m := range s.pos {
		if c := one[m]; c < 800 || c > 1200 {
			t.Errorf("Sample(1): %q drawn %d times of %d", m, c, rounds)
		}
		if c := two[m]; c < 1700 || c > 2300 {
			t.Errorf("Sample(2): %q drawn %d times of %d", m, c, rounds)
		}
	}
}

func TestPopRandomAndPopN(t *testing.T) {
	rng := rand.New(rand.NewPCG(11, 12))
	s := filled(20)
	all := map[string]bool{}
	for _, m := range s.Members() {
		all[m] = true
	}
	take := func(ms []string) {
		t.Helper()
		for _, m := range ms {
			if !all[m] {
				t.Fatalf("popped %q twice or never stored", m)
			}
			delete(all, m)
		}
	}
	m, ok := s.PopRandom(rng.IntN)
	if !ok {
		t.Fatal("PopRandom failed")
	}
	take([]string{m})
	take(s.PopN(5, rng.IntN))
	if s.Len() != 14 {
		t.Fatalf("Len = %d, want 14", s.Len())
	}
	checkInvariants(t, s)
	if got := s.PopN(0, rng.IntN); got != nil || s.Len() != 14 {
		t.Fatal("PopN(0) must do nothing")
	}
	rest := s.PopN(1000, rng.IntN) // more than there is: everything
	if s.Len() != 0 {
		t.Fatalf("Len = %d after popping everything", s.Len())
	}
	if !slices.Equal(sorted(rest), sorted(keys(all))) {
		t.Fatalf("PopN(all) = %v, want %v", sorted(rest), sorted(keys(all)))
	}
	checkInvariants(t, s)
	if _, ok := s.PopRandom(rng.IntN); ok {
		t.Fatal("PopRandom on an empty set must report false")
	}
	if !s.Add("again") || !s.Has("again") { // usable after being emptied
		t.Fatal("set unusable after PopN emptied it")
	}
	checkInvariants(t, s)
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func TestRandomOperationsMatchModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(13, 14))
	s := New()
	model := map[string]bool{}
	member := func() string { return "m" + strconv.Itoa(rng.IntN(50)) }
	for step := 0; step < 20000; step++ {
		m := member()
		switch rng.IntN(7) {
		case 0, 1, 2:
			if got := s.Add(m); got == model[m] {
				t.Fatalf("step %d: Add(%q) = %v, present %v", step, m, got, model[m])
			}
			model[m] = true
		case 3, 4:
			if got := s.Remove(m); got != model[m] {
				t.Fatalf("step %d: Remove(%q) = %v, present %v", step, m, got, model[m])
			}
			delete(model, m)
		case 5:
			got, ok := s.PopRandom(rng.IntN)
			if ok != (len(model) > 0) || (ok && !model[got]) {
				t.Fatalf("step %d: PopRandom = %q,%v", step, got, ok)
			}
			delete(model, got)
		case 6:
			k := rng.IntN(5)
			for _, got := range s.PopN(k, rng.IntN) {
				if !model[got] {
					t.Fatalf("step %d: PopN returned unknown %q", step, got)
				}
				delete(model, got)
			}
		}
		if s.Len() != len(model) {
			t.Fatalf("step %d: Len %d, model %d", step, s.Len(), len(model))
		}
		for k := range model {
			if !s.Has(k) {
				t.Fatalf("step %d: lost %q", step, k)
			}
		}
		checkInvariants(t, s)
	}
}
