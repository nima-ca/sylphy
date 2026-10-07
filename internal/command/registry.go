package command

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// Flags describe command properties.
type Flags uint8

// Command flags.
const (
	FlagReadOnly Flags = 1 << iota
	FlagWrite
	FlagFast
	FlagAdmin
)

// Handler executes one command. args excludes the command name.
type Handler func(ctx *Context, args [][]byte) error

// Spec describes a command.
type Spec struct {
	// Name is the command name (stored upper-case).
	Name string
	// Arity follows Redis: positive = exactly that many tokens including the
	// name, negative = at least that many.
	Arity int
	// Flags carries command properties.
	Flags Flags
	// Handler runs the command.
	Handler Handler
}

// Registry maps upper-case command names to specs. It is populated at startup
// and then read-only, so concurrent Lookup needs no locking.
type Registry struct {
	cmds map[string]*Spec
}

// NewRegistry returns an empty Registry.
func NewRegistry() *Registry { return &Registry{cmds: make(map[string]*Spec)} }

// Register adds spec. It must not be called after the registry is shared.
func (r *Registry) Register(spec Spec) error {
	name := strings.ToUpper(spec.Name)
	switch {
	case name == "":
		return errors.New("command: empty name")
	case spec.Arity == 0:
		return fmt.Errorf("command %s: arity must be non-zero", name)
	case spec.Handler == nil:
		return fmt.Errorf("command %s: nil handler", name)
	}
	if _, dup := r.cmds[name]; dup {
		return fmt.Errorf("command %s: already registered", name)
	}
	spec.Name = name
	r.cmds[name] = &spec
	return nil
}

// Lookup finds a command case-insensitively without allocating.
func (r *Registry) Lookup(name []byte) (*Spec, bool) {
	var buf [32]byte
	if len(name) > len(buf) {
		return nil, false
	}
	for i, c := range name {
		if 'a' <= c && c <= 'z' {
			c -= 'a' - 'A'
		}
		buf[i] = c
	}
	s, ok := r.cmds[string(buf[:len(name)])]
	return s, ok
}

// Names returns all registered command names, sorted.
func (r *Registry) Names() []string {
	out := make([]string, 0, len(r.cmds))
	for n := range r.cmds {
		out = append(out, n)
	}
	slices.Sort(out)
	return out
}

// NewDefaultRegistry returns a Registry with all Phase 1 commands.
func NewDefaultRegistry() (*Registry, error) {
	r := NewRegistry()
	specs := []Spec{
		{Name: "PING", Arity: -1, Flags: FlagFast, Handler: cmdPing},
		{Name: "ECHO", Arity: 2, Flags: FlagFast, Handler: cmdEcho},
		{Name: "QUIT", Arity: -1, Flags: FlagFast, Handler: cmdQuit},
		{Name: "SELECT", Arity: 2, Flags: FlagFast, Handler: cmdSelect},
		{Name: "CLIENT", Arity: -2, Flags: FlagAdmin, Handler: cmdClient},
		{Name: "COMMAND", Arity: -1, Flags: FlagAdmin, Handler: cmdCommand},
		{Name: "SET", Arity: -3, Flags: FlagWrite, Handler: cmdSet},
		{Name: "GET", Arity: 2, Flags: FlagReadOnly | FlagFast, Handler: cmdGet},
		{Name: "DEL", Arity: -2, Flags: FlagWrite, Handler: cmdDel},
		{Name: "EXISTS", Arity: -2, Flags: FlagReadOnly | FlagFast, Handler: cmdExists},
		{Name: "KEYS", Arity: 2, Flags: FlagReadOnly, Handler: cmdKeys},
		{Name: "DBSIZE", Arity: 1, Flags: FlagReadOnly | FlagFast, Handler: cmdDBSize},
		{Name: "FLUSHALL", Arity: -1, Flags: FlagWrite, Handler: cmdFlushAll},
		{Name: "INCR", Arity: 2, Flags: FlagWrite | FlagFast, Handler: cmdIncr},
		{Name: "DECR", Arity: 2, Flags: FlagWrite | FlagFast, Handler: cmdDecr},
		{Name: "INCRBY", Arity: 3, Flags: FlagWrite | FlagFast, Handler: cmdIncrBy},
		{Name: "DECRBY", Arity: 3, Flags: FlagWrite | FlagFast, Handler: cmdDecrBy},
		{Name: "APPEND", Arity: 3, Flags: FlagWrite, Handler: cmdAppend},
		{Name: "STRLEN", Arity: 2, Flags: FlagReadOnly | FlagFast, Handler: cmdStrlen},
		{Name: "MSET", Arity: -3, Flags: FlagWrite, Handler: cmdMSet},
		{Name: "MGET", Arity: -2, Flags: FlagReadOnly, Handler: cmdMGet},
	}
	for _, s := range specs {
		if err := r.Register(s); err != nil {
			return nil, err
		}
	}
	return r, nil
}
