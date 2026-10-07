package command

import (
	"slices"
	"testing"
)

func TestRegistry(t *testing.T) {
	r := NewRegistry()
	noop := func(*Context, [][]byte) error { return nil }

	if err := r.Register(Spec{Name: "Foo", Arity: 1, Handler: noop}); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"foo", "FOO", "fOo"} {
		if s, ok := r.Lookup([]byte(name)); !ok || s.Name != "FOO" {
			t.Errorf("Lookup(%q) = %v,%v", name, s, ok)
		}
	}
	if _, ok := r.Lookup([]byte("bar")); ok {
		t.Error("unexpected hit")
	}
	if _, ok := r.Lookup(make([]byte, 100)); ok {
		t.Error("over-long name must miss")
	}
	bad := []Spec{
		{Name: "", Arity: 1, Handler: noop},
		{Name: "x", Arity: 0, Handler: noop},
		{Name: "y", Arity: 1},
		{Name: "FOO", Arity: 1, Handler: noop},
	}
	for _, s := range bad {
		if err := r.Register(s); err == nil {
			t.Errorf("Register(%+v) succeeded, want error", s)
		}
	}
}

func TestDefaultRegistryHasPhase1Commands(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	names := r.Names()
	for _, want := range []string{"PING", "ECHO", "SET", "GET", "DEL", "EXISTS", "KEYS", "DBSIZE", "FLUSHALL",
		"INCR", "DECR", "INCRBY", "DECRBY", "APPEND", "STRLEN", "MSET", "MGET", "QUIT", "COMMAND", "CLIENT", "SELECT"} {
		if !slices.Contains(names, want) {
			t.Errorf("missing command %s", want)
		}
	}
}

func TestArityOK(t *testing.T) {
	tests := []struct {
		arity, n int
		want     bool
	}{{2, 2, true}, {2, 3, false}, {2, 1, false}, {-2, 2, true}, {-2, 5, true}, {-2, 1, false}}
	for _, tt := range tests {
		if got := arityOK(tt.arity, tt.n); got != tt.want {
			t.Errorf("arityOK(%d,%d) = %v", tt.arity, tt.n, got)
		}
	}
}
