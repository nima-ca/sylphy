package list

import (
	"math"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/nima-ca/sylphy/internal/store"
)

// checkInvariants verifies the representation invariants documented on List.
func checkInvariants(t testing.TB, l *List) {
	t.Helper()
	c := len(l.buf)
	if c != 0 && (c < minCap || c&(c-1) != 0) {
		t.Fatalf("capacity %d is not zero or a power of two >= %d", c, minCap)
	}
	if l.n < 0 || l.n > c {
		t.Fatalf("n = %d with capacity %d", l.n, c)
	}
	if c == 0 && (l.n != 0 || l.head != 0) {
		t.Fatalf("unallocated list has n=%d head=%d", l.n, l.head)
	}
	if c > 0 && (l.head < 0 || l.head >= c) {
		t.Fatalf("head %d outside capacity %d", l.head, c)
	}
	if c > minCap && l.n <= c/4 {
		t.Fatalf("capacity %d not shrunk for n=%d", c, l.n)
	}
	live := make([]bool, c)
	for i := 0; i < l.n; i++ {
		live[l.slot(i)] = true
	}
	for i, ok := range live {
		if !ok && l.buf[i] != nil {
			t.Fatalf("slot %d outside the live window still holds a reference", i)
		}
	}
}

// contents reads the list element by element through At.
func contents(t testing.TB, l *List) []string {
	t.Helper()
	out := make([]string, l.Len())
	for i := range out {
		b, ok := l.At(i)
		if !ok {
			t.Fatalf("At(%d) out of range with Len %d", i, l.Len())
		}
		out[i] = string(b)
	}
	return out
}

func strs(bs [][]byte) []string {
	out := make([]string, len(bs))
	for i, b := range bs {
		out[i] = string(b)
	}
	return out
}

// rotated builds a list holding ss whose head has been moved r places, so the
// elements wrap around the end of the buffer.
// rotated builds a list holding ss whose head has been moved r places, so the
// elements wrap around the end of the buffer.
func rotated(ss []string, r int) *List {
	l := New()
	for _, s := range ss {
		l.PushBack([]byte(s))
	}
	if l.n == 0 || len(l.buf) == 0 {
		return l
	}
	// Advance the head by r positions, wrapping the live elements so that
	// the logical order of ss is preserved but the physical storage wraps.
	c := len(l.buf)
	r %= c
	if r == 0 {
		return l
	}
	nb := make([][]byte, c)
	for i := 0; i < l.n; i++ {
		nb[(r+i)&(c-1)] = l.buf[l.slot(i)]
	}
	l.buf = nb
	l.head = r
	return l
}

func TestKindAndCollection(t *testing.T) {
	var v store.Value = New()
	if v.Kind() != store.KindList {
		t.Fatalf("Kind = %v", v.Kind())
	}
	if c, ok := v.(store.Collection); !ok || c.Len() != 0 {
		t.Fatal("a List must be an empty store.Collection")
	}
}

func TestPushPopBothEnds(t *testing.T) {
	l := New()
	for _, s := range []string{"1", "2", "3"} {
		l.PushBack([]byte(s))
	}
	l.PushFront([]byte("0"))
	if got := contents(t, l); !slices.Equal(got, []string{"0", "1", "2", "3"}) {
		t.Fatalf("contents = %v", got)
	}
	if b, ok := l.PopFront(); !ok || string(b) != "0" {
		t.Fatalf("PopFront = %q,%v", b, ok)
	}
	if b, ok := l.PopBack(); !ok || string(b) != "3" {
		t.Fatalf("PopBack = %q,%v", b, ok)
	}
	checkInvariants(t, l)
	l.PopFront()
	l.PopBack()
	if _, ok := l.PopFront(); ok {
		t.Fatal("PopFront on an empty list must report false")
	}
	if _, ok := l.PopBack(); ok {
		t.Fatal("PopBack on an empty list must report false")
	}
	if l.buf != nil {
		t.Fatal("an emptied list must release its buffer")
	}
	checkInvariants(t, l)
}

// A sliding window forces the live region to wrap around the buffer end many
// times without ever growing it.
func TestWrapAround(t *testing.T) {
	l := New()
	var model []string
	for i := 0; i < 200; i++ {
		v := strconv.Itoa(i)
		l.PushBack([]byte(v))
		model = append(model, v)
		if len(model) > 5 {
			b, _ := l.PopFront()
			if string(b) != model[0] {
				t.Fatalf("step %d: popped %q, want %q", i, b, model[0])
			}
			model = model[1:]
		}
		if got := contents(t, l); !slices.Equal(got, model) {
			t.Fatalf("step %d: contents %v, want %v", i, got, model)
		}
		checkInvariants(t, l)
	}
	if len(l.buf) != minCap {
		t.Fatalf("capacity grew to %d for a 5-element window", len(l.buf))
	}
}

func TestCopySemantics(t *testing.T) {
	l := New()
	src := []byte("hello")
	l.PushBack(src)
	src[0] = 'X'
	if b, _ := l.At(0); string(b) != "hello" {
		t.Fatalf("store aliased the caller's slice: %q", b)
	}
	l.PushBack([]byte("world"))
	r := l.Range(0, 2)
	r[0][0] = 'Y'
	if b, _ := l.At(0); string(b) != "hello" {
		t.Fatalf("Range aliased the list: %q", b)
	}
	r[0] = append(r[0], '!') // must not overwrite the neighboring copy
	if string(r[1]) != "world" {
		t.Fatalf("append through one copy clobbered its neighbor: %q", r[1])
	}
	l.Set(0, src)
	src[1] = 'Z'
	if b, _ := l.At(0); string(b) != "Xello" {
		t.Fatalf("Set aliased the caller's slice: %q", b)
	}
}

func TestEmptyAndBinaryElements(t *testing.T) {
	l := New()
	l.PushBack(nil)
	l.PushBack([]byte{})
	l.PushBack([]byte("a\r\n\x00b"))
	got := l.Range(0, 3)
	if len(got) != 3 || len(got[0]) != 0 || len(got[1]) != 0 || string(got[2]) != "a\r\n\x00b" {
		t.Fatalf("Range = %q", got)
	}
	if got[0] == nil {
		t.Fatal("an empty element must stay a non-nil empty slice")
	}
	if i := l.IndexOf(nil); i != 0 {
		t.Fatalf("IndexOf(empty) = %d", i)
	}
}

func TestBoundsAreChecked(t *testing.T) {
	l := rotated([]string{"a", "b", "c"}, 1)
	for _, i := range []int{-1, 3, math.MaxInt, math.MinInt} {
		if _, ok := l.At(i); ok {
			t.Errorf("At(%d) succeeded", i)
		}
		if l.Set(i, []byte("x")) {
			t.Errorf("Set(%d) succeeded", i)
		}
	}
	for _, i := range []int{-1, 4, math.MaxInt} {
		if l.Insert(i, []byte("x")) {
			t.Errorf("Insert(%d) succeeded", i)
		}
	}
	if got := contents(t, l); !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("failed operations changed the list: %v", got)
	}
	if r := l.Range(2, 2); r != nil {
		t.Fatalf("empty range = %v", r)
	}
	if r := l.Range(-5, 100); len(r) != 3 {
		t.Fatalf("clamped range has %d elements", len(r))
	}
	if r := l.Range(5, 1); r != nil {
		t.Fatalf("inverted range = %v", r)
	}
}

// Every insert position, for every list length up to 20 and every rotation of
// the ring, against slices.Insert.
func TestInsertEveryPositionAndRotation(t *testing.T) {
	for n := 0; n <= 20; n++ {
		base := make([]string, n)
		for i := range base {
			base[i] = "e" + strconv.Itoa(i)
		}
		for r := 0; r < 20; r++ {
			for i := 0; i <= n; i++ {
				l := rotated(base, r)
				if !l.Insert(i, []byte("NEW")) {
					t.Fatalf("n=%d r=%d i=%d: Insert failed", n, r, i)
				}
				want := slices.Insert(slices.Clone(base), i, "NEW")
				if got := contents(t, l); !slices.Equal(got, want) {
					t.Fatalf("n=%d r=%d i=%d: got %v, want %v", n, r, i, got, want)
				}
				checkInvariants(t, l)
			}
		}
	}
}

func TestRemove(t *testing.T) {
	const initial = "a b a c a b a"
	tests := []struct {
		name    string
		count   int
		target  string
		removed int
		want    string
	}{
		{"all", 0, "a", 4, "b c b"},
		{"first two", 2, "a", 2, "b c a b a"},
		{"last two", -2, "a", 2, "a b a c b"},
		{"first b", 1, "b", 1, "a a c a b a"},
		{"last b", -1, "b", 1, "a b a c a a"},
		{"no match", 3, "z", 0, initial},
		{"count above matches", 100, "a", 4, "b c b"},
		{"negative count above matches", -100, "a", 4, "b c b"},
		{"min int", math.MinInt, "a", 4, "b c b"},
		{"max int", math.MaxInt, "a", 4, "b c b"},
		{"remove everything", 0, "q", 0, initial},
	}

	split := strings.Fields
	for _, tt := range tests {
		for r := 0; r < 10; r++ {
			l := rotated(split(initial), r)
			if got := l.Remove(tt.count, []byte(tt.target)); got != tt.removed {
				t.Fatalf("%s (rot %d): removed %d, want %d", tt.name, r, got, tt.removed)
			}
			if got := contents(t, l); !slices.Equal(got, split(tt.want)) {
				t.Fatalf("%s (rot %d): got %v, want %v", tt.name, r, got, split(tt.want))
			}
			checkInvariants(t, l)
		}
	}

	l := rotated([]string{"x", "x"}, 1)
	if got := l.Remove(0, []byte("x")); got != 2 || l.Len() != 0 || l.buf != nil {
		t.Fatalf("removing every element: removed %d, len %d", got, l.Len())
	}
	if got := New().Remove(0, []byte("x")); got != 0 {
		t.Fatalf("Remove on an empty list = %d", got)
	}
}

func TestTrim(t *testing.T) {
	base := []string{"a", "b", "c", "d", "e"}
	tests := []struct {
		lo, hi int
		want   []string
	}{
		{1, 4, []string{"b", "c", "d"}},
		{0, 5, base},
		{-3, 2, []string{"a", "b"}},
		{3, 100, []string{"d", "e"}},
		{4, 2, nil},
		{2, 2, nil},
		{0, 0, nil},
		{5, 9, nil},
		{4, 5, []string{"e"}},
	}
	for _, tt := range tests {
		for r := 0; r < 10; r++ {
			l := rotated(base, r)
			l.Trim(tt.lo, tt.hi)
			if got := contents(t, l); !slices.Equal(got, tt.want) {
				t.Fatalf("Trim(%d,%d) rot %d: got %v, want %v", tt.lo, tt.hi, r, got, tt.want)
			}
			if tt.want == nil && l.buf != nil {
				t.Fatalf("Trim(%d,%d): emptied list kept its buffer", tt.lo, tt.hi)
			}
			checkInvariants(t, l)
		}
	}
}

func TestPopN(t *testing.T) {
	l := rotated([]string{"a", "b", "c", "d", "e"}, 3)
	if got := strs(l.PopFrontN(2)); !slices.Equal(got, []string{"a", "b"}) {
		t.Fatalf("PopFrontN(2) = %v", got)
	}
	if got := strs(l.PopBackN(2)); !slices.Equal(got, []string{"e", "d"}) {
		t.Fatalf("PopBackN(2) = %v", got)
	}
	for _, k := range []int{0, -4} {
		if got := l.PopFrontN(k); len(got) != 0 || l.Len() != 1 {
			t.Fatalf("PopFrontN(%d) = %v, Len = %d", k, got, l.Len())
		}
	}
	// A huge k must be clamped, not allocated.
	if got := strs(l.PopBackN(math.MaxInt)); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("PopBackN(MaxInt) = %v", got)
	}
	checkInvariants(t, l)
	if l.Len() != 0 || l.buf != nil {
		t.Fatal("list should be empty and unallocated")
	}
}

func TestGrowAndShrink(t *testing.T) {
	l := New()
	for i := 0; i < 4096; i++ {
		l.PushBack([]byte(strconv.Itoa(i)))
	}
	checkInvariants(t, l)
	if len(l.buf) != 4096 {
		t.Fatalf("capacity = %d, want 4096", len(l.buf))
	}
	for l.Len() > 10 {
		l.PopFront()
		checkInvariants(t, l)
	}
	if len(l.buf) != 32 {
		t.Fatalf("capacity after shrinking = %d, want 32", len(l.buf))
	}
	if b, _ := l.At(0); string(b) != "4086" {
		t.Fatalf("first element = %q", b)
	}
	for l.Len() > 0 {
		l.PopBack()
		checkInvariants(t, l)
	}
}

// --- model-based test ---

func modelRemove(m []string, count int, v string) ([]string, int) {
	limit := count
	if limit < 0 {
		limit = -limit
	}
	if count == 0 {
		limit = len(m)
	}
	drop := make([]bool, len(m))
	removed := 0
	if count >= 0 {
		for i := 0; i < len(m) && removed < limit; i++ {
			if m[i] == v {
				drop[i] = true
				removed++
			}
		}
	} else {
		for i := len(m) - 1; i >= 0 && removed < limit; i-- {
			if m[i] == v {
				drop[i] = true
				removed++
			}
		}
	}
	var out []string
	for i, s := range m {
		if !drop[i] {
			out = append(out, s)
		}
	}
	return out, removed
}

// TestRandomOperationsMatchModel drives the ring buffer and a plain slice with
// the same random operations and compares them after every step.
func TestRandomOperationsMatchModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	vals := []string{"a", "b", "c", "d", "e"}
	pick := func() string { return vals[rng.IntN(len(vals))] }
	l := New()
	var model []string

	for step := 0; step < 30000; step++ {
		var what string
		switch op := rng.IntN(20); {
		case op < 5:
			v := pick()
			what = "PushFront " + v
			l.PushFront([]byte(v))
			model = slices.Insert(model, 0, v)
		case op < 10:
			v := pick()
			what = "PushBack " + v
			l.PushBack([]byte(v))
			model = append(model, v)
		case op == 10:
			what = "PopFront"
			b, ok := l.PopFront()
			if ok != (len(model) > 0) || (ok && string(b) != model[0]) {
				t.Fatalf("step %d %s: got %q,%v; model %v", step, what, b, ok, model)
			}
			if ok {
				model = model[1:]
			}
		case op == 11:
			what = "PopBack"
			b, ok := l.PopBack()
			if ok != (len(model) > 0) || (ok && string(b) != model[len(model)-1]) {
				t.Fatalf("step %d %s: got %q,%v; model %v", step, what, b, ok, model)
			}
			if ok {
				model = model[:len(model)-1]
			}
		case op == 12:
			k := rng.IntN(6)
			what = "PopFrontN " + strconv.Itoa(k)
			k = min(k, len(model))
			if got := strs(l.PopFrontN(k)); !slices.Equal(got, model[:k]) {
				t.Fatalf("step %d %s: got %v, want %v", step, what, got, model[:k])
			}
			model = model[k:]
		case op == 13:
			k := rng.IntN(6)
			what = "PopBackN " + strconv.Itoa(k)
			k = min(k, len(model))
			want := slices.Clone(model[len(model)-k:])
			slices.Reverse(want)
			if got := strs(l.PopBackN(k)); !slices.Equal(got, want) {
				t.Fatalf("step %d %s: got %v, want %v", step, what, got, want)
			}
			model = model[:len(model)-k]
		case op == 14:
			i, v := rng.IntN(len(model)+1), pick()
			what = "Insert " + strconv.Itoa(i) + " " + v
			if !l.Insert(i, []byte(v)) {
				t.Fatalf("step %d %s: refused", step, what)
			}
			model = slices.Insert(model, i, v)
		case op == 15:
			count, v := rng.IntN(7)-3, pick()
			what = "Remove " + strconv.Itoa(count) + " " + v
			var want int
			model, want = modelRemove(model, count, v)
			if got := l.Remove(count, []byte(v)); got != want {
				t.Fatalf("step %d %s: removed %d, want %d", step, what, got, want)
			}
		case op == 16:
			if rng.IntN(8) != 0 { // trims are destructive; keep them rare
				continue
			}
			lo, hi := rng.IntN(len(model)+2)-1, rng.IntN(len(model)+3)
			what = "Trim " + strconv.Itoa(lo) + " " + strconv.Itoa(hi)
			l.Trim(lo, hi)
			lo, hi = max(lo, 0), min(hi, len(model))
			if lo >= hi {
				model = nil
			} else {
				model = slices.Clone(model[lo:hi])
			}
		case op == 17:
			if len(model) == 0 {
				continue
			}
			i, v := rng.IntN(len(model)), pick()
			what = "Set " + strconv.Itoa(i) + " " + v
			if !l.Set(i, []byte(v)) {
				t.Fatalf("step %d %s: refused", step, what)
			}
			model[i] = v
		case op == 18:
			v := pick()
			what = "IndexOf " + v
			if got, want := l.IndexOf([]byte(v)), slices.Index(model, v); got != want {
				t.Fatalf("step %d %s: got %d, want %d", step, what, got, want)
			}
		default:
			lo, hi := rng.IntN(len(model)+3)-1, rng.IntN(len(model)+3)
			what = "Range " + strconv.Itoa(lo) + " " + strconv.Itoa(hi)
			clo, chi := max(lo, 0), min(hi, len(model))
			var want []string
			if clo < chi {
				want = model[clo:chi]
			}
			if got := strs(l.Range(lo, hi)); !slices.Equal(got, want) {
				t.Fatalf("step %d %s: got %v, want %v", step, what, got, want)
			}
		}
		if l.Len() != len(model) {
			t.Fatalf("step %d %s: Len %d, model %d", step, what, l.Len(), len(model))
		}
		if got := contents(t, l); !slices.Equal(got, model) {
			t.Fatalf("step %d %s:\n got %v\nwant %v", step, what, got, model)
		}
		checkInvariants(t, l)
	}
}
