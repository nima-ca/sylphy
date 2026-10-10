package hash

import (
	"math/rand/v2"
	"slices"
	"strconv"
	"testing"

	"github.com/nima-ca/sylphy/internal/store"
)

func checkInvariants(t testing.TB, h *Hash) {
	t.Helper()
	if len(h.ents) != len(h.pos) {
		t.Fatalf("len(ents)=%d but len(pos)=%d", len(h.ents), len(h.pos))
	}
	for i, e := range h.ents {
		if got, ok := h.pos[e.field]; !ok || got != i {
			t.Fatalf("pos[%q] = %d,%v; want %d", e.field, got, ok, i)
		}
	}
	if cap(h.ents) > shrinkMinCap && len(h.ents) <= cap(h.ents)/4 {
		t.Fatalf("cap %d not shrunk for len %d", cap(h.ents), len(h.ents))
	}
}

func TestKindAndCollection(t *testing.T) {
	var v store.Value = New()
	if v.Kind() != store.KindHash {
		t.Fatalf("Kind = %v", v.Kind())
	}
	if c, ok := v.(store.Collection); !ok || c.Len() != 0 {
		t.Fatal("a Hash must be an empty store.Collection")
	}
}

func TestZeroValueIsUsable(t *testing.T) {
	var h Hash
	if h.Has("x") || h.Delete("x") {
		t.Fatal("empty zero value reported content")
	}
	if !h.Set("x", "1") || h.Len() != 1 {
		t.Fatal("Set on the zero value failed")
	}
	checkInvariants(t, &h)
}

func TestSetGetDelete(t *testing.T) {
	h := New()
	if !h.Set("f1", "v1") || !h.Set("f2", "v2") || !h.Set("f3", "v3") {
		t.Fatal("new fields must report true")
	}
	if h.Set("f1", "new") {
		t.Fatal("updating a field must report false")
	}
	if v, ok := h.Get("f1"); !ok || v != "new" {
		t.Fatalf("Get = %q,%v", v, ok)
	}
	if h.SetNX("f2", "x") || !h.SetNX("f4", "v4") {
		t.Fatal("SetNX must only create")
	}
	if v, _ := h.Get("f2"); v != "v2" {
		t.Fatalf("SetNX overwrote: %q", v)
	}
	if got := h.Fields(); !slices.Equal(got, []string{"f1", "f2", "f3", "f4"}) {
		t.Fatalf("Fields = %v", got)
	}
	// Documented behavior: the last entry fills the hole.
	if !h.Delete("f1") || h.Delete("f1") {
		t.Fatal("Delete must report true once")
	}
	if got := h.Fields(); !slices.Equal(got, []string{"f4", "f2", "f3"}) {
		t.Fatalf("Fields after delete = %v", got)
	}
	// Fields, Values and Flat always agree with each other.
	if got := h.Values(); !slices.Equal(got, []string{"v4", "v2", "v3"}) {
		t.Fatalf("Values = %v", got)
	}
	if got := h.Flat(); !slices.Equal(got, []string{"f4", "v4", "f2", "v2", "f3", "v3"}) {
		t.Fatalf("Flat = %v", got)
	}
	checkInvariants(t, h)
}

func TestEmptyAndBinaryFieldsAndValues(t *testing.T) {
	h := New()
	h.Set("", "")
	h.Set("a\r\n\x00", "b\r\n\x00")
	if v, ok := h.Get(""); !ok || v != "" {
		t.Fatalf("empty field: %q,%v", v, ok)
	}
	if v, ok := h.Get("a\r\n\x00"); !ok || v != "b\r\n\x00" {
		t.Fatalf("binary field: %q,%v", v, ok)
	}
}

func TestShrinkAfterMassDelete(t *testing.T) {
	h := New()
	for i := 0; i < 5000; i++ {
		h.Set("f"+strconv.Itoa(i), "v")
	}
	checkInvariants(t, h)
	for i := 0; i < 4990; i++ {
		if !h.Delete("f" + strconv.Itoa(i)) {
			t.Fatalf("Delete %d failed", i)
		}
		checkInvariants(t, h)
	}
	if cap(h.ents) > 64 {
		t.Fatalf("capacity %d after shrinking to %d fields", cap(h.ents), h.Len())
	}
	for i := 4990; i < 5000; i++ {
		if v, ok := h.Get("f" + strconv.Itoa(i)); !ok || v != "v" {
			t.Fatalf("lost field %d", i)
		}
	}
}

func TestRandomOperationsMatchModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(3, 4))
	h := New()
	model := map[string]string{}
	field := func() string { return "f" + strconv.Itoa(rng.IntN(40)) }
	for step := 0; step < 20000; step++ {
		f, v := field(), strconv.Itoa(step)
		switch rng.IntN(5) {
		case 0, 1:
			_, existed := model[f]
			if got := h.Set(f, v); got == existed {
				t.Fatalf("step %d: Set(%q) = %v, existed %v", step, f, got, existed)
			}
			model[f] = v
		case 2:
			_, existed := model[f]
			if got := h.SetNX(f, v); got == existed {
				t.Fatalf("step %d: SetNX(%q) = %v, existed %v", step, f, got, existed)
			}
			if !existed {
				model[f] = v
			}
		case 3, 4:
			_, existed := model[f]
			if got := h.Delete(f); got != existed {
				t.Fatalf("step %d: Delete(%q) = %v, existed %v", step, f, got, existed)
			}
			delete(model, f)
		}
		if h.Len() != len(model) {
			t.Fatalf("step %d: Len %d, model %d", step, h.Len(), len(model))
		}
		for k, want := range model {
			if got, ok := h.Get(k); !ok || got != want {
				t.Fatalf("step %d: Get(%q) = %q,%v; want %q", step, k, got, ok, want)
			}
		}
		checkInvariants(t, h)
	}
}
