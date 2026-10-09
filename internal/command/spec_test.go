package command

import (
	"slices"
	"strings"
	"testing"
)

func TestDefaultRegistrySpecsAreValid(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err) // Register validates too, so a bad table entry fails here first
	}
	names := r.Names()
	if len(names) < 35 {
		t.Fatalf("only %d commands registered", len(names))
	}
	for _, name := range names {
		sp, ok := r.Lookup([]byte(name))
		if !ok {
			t.Errorf("%s listed but not found", name)
			continue
		}
		if err := validateSpec(*sp); err != nil {
			t.Error(err)
		}
		if sp.Handler == nil {
			t.Errorf("%s: nil handler", name)
		}
		if sp.Flags&(FlagWrite|FlagReadOnly|FlagAdmin) == 0 && sp.Flags&FlagFast == 0 {
			t.Errorf("%s: no flags at all", name)
		}
	}
}

func TestPhase2CommandsRegistered(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	names := r.Names()
	for _, want := range []string{"SETNX", "SETEX", "PSETEX", "GETEX", "GETDEL", "EXPIRE", "PEXPIRE",
		"EXPIREAT", "PEXPIREAT", "TTL", "PTTL", "PERSIST", "EXPIRETIME", "PEXPIRETIME"} {
		if !slices.Contains(names, want) {
			t.Errorf("missing command %s", want)
		}
	}
}

func TestValidateSpecRejects(t *testing.T) {
	noop := func(*Context, [][]byte) error { return nil }
	base := Spec{Name: "X", Arity: -3, Flags: FlagWrite, FirstKey: 1, LastKey: 1, Step: 1, Handler: noop}
	with := func(f func(*Spec)) Spec { s := base; f(&s); return s }
	bad := map[string]Spec{
		"lower-case name":           with(func(s *Spec) { s.Name = "x" }),
		"zero arity":                with(func(s *Spec) { s.Arity = 0 }),
		"write and readonly":        with(func(s *Spec) { s.Flags = FlagWrite | FlagReadOnly }),
		"keys without write/ro":     with(func(s *Spec) { s.Flags = FlagFast }),
		"no first key but last key": with(func(s *Spec) { s.FirstKey, s.LastKey, s.Step = 0, 1, 0 }),
		"no first key but step":     with(func(s *Spec) { s.FirstKey, s.LastKey, s.Step = 0, 0, 1 }),
		"first key past arity":      with(func(s *Spec) { s.Arity, s.FirstKey, s.LastKey = 2, 2, 2 }),
		"zero step":                 with(func(s *Spec) { s.Step = 0 }),
		"last before first":         with(func(s *Spec) { s.FirstKey, s.LastKey = 2, 1 }),
		"last past arity":           with(func(s *Spec) { s.Arity, s.LastKey = 3, 3 }),
		"negative last, fixed":      with(func(s *Spec) { s.Arity, s.LastKey = 3, -1 }),
		"negative last too small":   with(func(s *Spec) { s.Arity, s.FirstKey, s.LastKey = -2, 1, -2 }),
	}
	for name, sp := range bad {
		if err := validateSpec(sp); err == nil {
			t.Errorf("%s: accepted %+v", name, sp)
		}
	}
	if err := validateSpec(base); err != nil {
		t.Fatalf("base spec rejected: %v", err)
	}
	r := NewRegistry()
	if err := r.Register(with(func(s *Spec) { s.Step = 0 })); err == nil {
		t.Fatal("Register must reject inconsistent key metadata")
	}
}

func TestSpecKeys(t *testing.T) {
	r, err := NewDefaultRegistry()
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		argv []string
		want string // keys joined by space
	}{
		{[]string{"PING"}, ""},
		{[]string{"KEYS", "*"}, ""}, // a pattern, not a key
		{[]string{"FLUSHALL"}, ""},
		{[]string{"GET", "a"}, "a"},
		{[]string{"SET", "a", "v", "EX", "10"}, "a"}, // option arguments are not keys
		{[]string{"DEL", "a", "b", "c"}, "a b c"},
		{[]string{"EXISTS", "a"}, "a"},
		{[]string{"MGET", "a", "b"}, "a b"},
		{[]string{"MSET", "a", "1", "b", "2"}, "a b"}, // step 2: values are skipped
		{[]string{"SETEX", "k", "10", "v"}, "k"},
		{[]string{"GETEX", "k", "EX", "10"}, "k"},
		{[]string{"EXPIRE", "k", "10", "NX"}, "k"},
		{[]string{"TTL", "k"}, "k"},
	}
	for _, tt := range tests {
		argv := make([][]byte, len(tt.argv))
		for i, a := range tt.argv {
			argv[i] = []byte(a)
		}
		sp, ok := r.Lookup(argv[0])
		if !ok {
			t.Fatalf("%s not registered", tt.argv[0])
		}
		var got []string
		for _, k := range sp.Keys(argv) {
			got = append(got, string(k))
		}
		if strings.Join(got, " ") != tt.want {
			t.Errorf("%v: keys = %q, want %q", tt.argv, got, tt.want)
		}
	}
	// Short argv must not panic.
	del, _ := r.Lookup([]byte("DEL"))
	if got := del.Keys([][]byte{[]byte("DEL")}); len(got) != 0 {
		t.Errorf("keys of a bare DEL = %q", got)
	}
}
