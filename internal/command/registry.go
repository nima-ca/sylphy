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
	// FirstKey, LastKey and Step locate the key arguments the way Redis does.
	// Positions count the command name as 0, so a command whose first
	// argument is a key has FirstKey 1. A negative LastKey counts from the end
	// (-1 is the last argument). Step is the distance between keys (2 for
	// MSET's key/value pairs). FirstKey 0 means the command takes no keys, and
	// then LastKey and Step must be 0.
	FirstKey, LastKey, Step int
	// Handler runs the command.
	Handler Handler
	// Rewrite, for write commands whose argv would not replay
	// deterministically, produces the commands to log instead (see
	// RewriteFunc). Nil means the request is logged verbatim. Only write
	// commands may set it.
	Rewrite RewriteFunc
}

// Keys returns the key arguments of argv (command name first) according to the
// spec's key positions, without copying. It tolerates short argv.
func (sp *Spec) Keys(argv [][]byte) [][]byte {
	if sp.FirstKey == 0 {
		return nil
	}
	last := sp.LastKey
	if last < 0 {
		last += len(argv)
	}
	last = min(last, len(argv)-1)
	var out [][]byte
	for i := sp.FirstKey; i <= last; i += sp.Step {
		out = append(out, argv[i])
	}
	return out
}

// validateSpec checks a spec's metadata for internal consistency. Register
// runs it, so a bad table entry fails at startup rather than in production.
func validateSpec(s Spec) error {
	minArgs := s.Arity
	if minArgs < 0 {
		minArgs = -minArgs
	}
	switch {
	case s.Name == "" || s.Name != strings.ToUpper(s.Name):
		return fmt.Errorf("command %q: name must be non-empty and upper-case", s.Name)
	case s.Arity == 0:
		return fmt.Errorf("command %s: arity must be non-zero", s.Name)
	case s.Flags&FlagWrite != 0 && s.Flags&FlagReadOnly != 0:
		return fmt.Errorf("command %s: cannot be both write and readonly", s.Name)
	case s.Rewrite != nil && s.Flags&FlagWrite == 0:
		return fmt.Errorf("command %s: only write commands may have a Rewrite hook", s.Name)
	}
	if s.FirstKey == 0 {
		if s.LastKey != 0 || s.Step != 0 {
			return fmt.Errorf("command %s: no first key, so last key and step must be 0", s.Name)
		}
		return nil
	}
	switch {
	case s.Flags&(FlagWrite|FlagReadOnly) == 0:
		return fmt.Errorf("command %s: commands with keys must be flagged write or readonly", s.Name)
	case s.FirstKey < 0 || s.FirstKey >= minArgs:
		return fmt.Errorf("command %s: first key %d is outside the minimum %d tokens", s.Name, s.FirstKey, minArgs)
	case s.Step < 1:
		return fmt.Errorf("command %s: step must be at least 1", s.Name)
	case s.LastKey < 0 && s.Arity > 0:
		return fmt.Errorf("command %s: a negative last key needs variable arity", s.Name)
	case s.LastKey < 0 && minArgs+s.LastKey < s.FirstKey:
		return fmt.Errorf("command %s: last key %d can fall before the first key", s.Name, s.LastKey)
	case s.LastKey >= 0 && (s.LastKey < s.FirstKey || s.LastKey >= minArgs):
		return fmt.Errorf("command %s: last key %d is outside [%d, %d)", s.Name, s.LastKey, s.FirstKey, minArgs)
	}
	return nil
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
	if err := validateSpec(spec); err != nil {
		return err
	}
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

// NewDefaultRegistry returns a Registry with every implemented command.
func NewDefaultRegistry() (*Registry, error) {
	r := NewRegistry()
	const (
		ro  = FlagReadOnly
		w   = FlagWrite
		f   = FlagFast
		adm = FlagAdmin
	)
	specs := []Spec{
		// Connection and server commands take no keys.
		{Name: "PING", Arity: -1, Flags: f, Handler: cmdPing},
		{Name: "ECHO", Arity: 2, Flags: f, Handler: cmdEcho},
		{Name: "QUIT", Arity: -1, Flags: f, Handler: cmdQuit},
		{Name: "SELECT", Arity: 2, Flags: f, Handler: cmdSelect},
		{Name: "CLIENT", Arity: -2, Flags: adm, Handler: cmdClient},
		{Name: "COMMAND", Arity: -1, Flags: adm, Handler: cmdCommand},
		{Name: "KEYS", Arity: 2, Flags: ro, Handler: cmdKeys}, // the argument is a pattern
		{Name: "DBSIZE", Arity: 1, Flags: ro | f, Handler: cmdDBSize},
		{Name: "FLUSHALL", Arity: -1, Flags: w, Handler: cmdFlushAll},

		// Strings. Commands with TTL options or conditional outcomes carry a
		// Rewrite hook; the rest replay verbatim.
		{Name: "SET", Arity: -3, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSet, Rewrite: rewriteSet},
		{Name: "SETNX", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSetNX, Rewrite: rewriteSetNX},
		{Name: "SETEX", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSetEX, Rewrite: rewriteSetWithTTL},
		{Name: "PSETEX", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdPSetEX, Rewrite: rewriteSetWithTTL},
		{Name: "GET", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdGet},
		{Name: "GETEX", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdGetEX, Rewrite: rewriteGetEX},
		{Name: "GETDEL", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdGetDel, Rewrite: rewriteGetDel},
		{Name: "INCR", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdIncr},
		{Name: "DECR", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdDecr},
		{Name: "INCRBY", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdIncrBy},
		{Name: "DECRBY", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdDecrBy},
		{Name: "APPEND", Arity: 3, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdAppend},
		{Name: "STRLEN", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdStrlen},
		{Name: "MSET", Arity: -3, Flags: w, FirstKey: 1, LastKey: -1, Step: 2, Handler: cmdMSet},
		{Name: "MGET", Arity: -2, Flags: ro, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdMGet},

		// Keyspace.
		{Name: "DEL", Arity: -2, Flags: w, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdDel},
		{Name: "EXISTS", Arity: -2, Flags: ro | f, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdExists},

		// Expiry: relative and conditional forms all rewrite to PEXPIREAT.
		{Name: "EXPIRE", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdExpire, Rewrite: rewriteExpire},
		{Name: "PEXPIRE", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdPExpire, Rewrite: rewriteExpire},
		{Name: "EXPIREAT", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdExpireAt, Rewrite: rewriteExpire},
		{Name: "PEXPIREAT", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdPExpireAt, Rewrite: rewriteExpire},
		{Name: "TTL", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdTTL},
		{Name: "PTTL", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdPTTL},
		{Name: "PERSIST", Arity: 2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdPersist, Rewrite: rewritePersist},
		{Name: "EXPIRETIME", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdExpireTime},
		{Name: "PEXPIRETIME", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdPExpireTime},
	}
	// Each data type contributes its own table; later parts append theirs here.
	specs = append(specs, listSpecs()...)
	specs = append(specs, hashSpecs()...)
	specs = append(specs, setSpecs()...)
	specs = append(specs, zsetSpecs()...)
	specs = append(specs, keySpecs()...)
	for _, s := range specs {
		if err := r.Register(s); err != nil {
			return nil, err
		}
	}
	return r, nil
}
