package command

import "strconv"

// Effects records what an executing write command actually did, for its
// RewriteFunc. Dispatch zeroes it before each write command; handlers fill in
// only the fields their Rewrite hook reads. Recording the applied deadline
// (rather than recomputing "now + ttl" later) is what makes the rewritten form
// exact: the AOF gets the very millisecond the store used.
type Effects struct {
	// Applied reports that the command changed the keyspace.
	Applied bool
	// DeadlineMs is the absolute Unix-ms deadline the command applied. It is
	// meaningful when HasDeadline is set, or for the EXPIRE family when
	// Applied is set.
	DeadlineMs int64
	// HasDeadline reports that the command set an expiry (SET ... EX, GETEX
	// PX, SETEX, ...).
	HasDeadline bool
	// KeepTTL reports that SET kept the key's existing TTL.
	KeepTTL bool
	// Persist reports that GETEX removed the key's TTL.
	Persist bool
	// Members lists the members SPOP actually removed.
	Members []string
}

// RewriteFunc turns an executed write command into the deterministic commands
// that reproduce its effect when replayed, in order. argv is the original
// request (name first). Returning nil means the command changed nothing and
// need not be logged. The result must not alias argv: the request buffers are
// reused once Dispatch returns.
//
// A command needs a hook only if replaying its argv verbatim could diverge:
// relative TTLs depend on the clock, SPOP on a random choice, and conditional
// forms (NX, XX, GT, LT, GET) on state the replay may not see identically.
type RewriteFunc func(argv [][]byte, fx *Effects) [][][]byte

// rewritten returns the commands to log for an executed write command: the
// hook's output, or the request itself when the command is deterministic.
func (sp *Spec) rewritten(argv [][]byte, fx *Effects) [][][]byte {
	if sp.Rewrite == nil {
		return [][][]byte{argv}
	}
	return sp.Rewrite(argv, fx)
}

func copyBytes(b []byte) []byte { return append(make([]byte, 0, len(b)), b...) }

// argvOf builds a command with copies of its arguments.
func argvOf(name string, args ...[]byte) [][]byte {
	out := make([][]byte, 0, 1+len(args))
	out = append(out, []byte(name))
	for _, a := range args {
		out = append(out, copyBytes(a))
	}
	return out
}

func msArg(ms int64) []byte { return strconv.AppendInt(nil, ms, 10) }

// rewriteExpire handles EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT: every form
// becomes an unconditional PEXPIREAT with the applied deadline. A deadline in
// the past, which deleted the key, replays as the same deletion.
func rewriteExpire(argv [][]byte, fx *Effects) [][][]byte {
	if !fx.Applied {
		return nil
	}
	return [][][]byte{argvOf("PEXPIREAT", argv[1], msArg(fx.DeadlineMs))}
}

// rewritePersist handles PERSIST.
func rewritePersist(argv [][]byte, fx *Effects) [][][]byte {
	if !fx.Applied {
		return nil
	}
	return [][][]byte{argvOf("PERSIST", argv[1])}
}

// rewriteSet handles SET: NX, XX and GET are dropped (the outcome is already
// decided), and a relative or absolute expiry becomes PXAT.
func rewriteSet(argv [][]byte, fx *Effects) [][][]byte {
	if !fx.Applied {
		return nil
	}
	cmd := argvOf("SET", argv[1], argv[2])
	switch {
	case fx.KeepTTL:
		cmd = append(cmd, []byte("KEEPTTL"))
	case fx.HasDeadline:
		cmd = append(cmd, []byte("PXAT"), msArg(fx.DeadlineMs))
	}
	return [][][]byte{cmd}
}

// rewriteSetNX handles SETNX: when it stored, it replays as a plain SET.
func rewriteSetNX(argv [][]byte, fx *Effects) [][][]byte {
	if !fx.Applied {
		return nil
	}
	return [][][]byte{argvOf("SET", argv[1], argv[2])}
}

// rewriteSetWithTTL handles SETEX and PSETEX (key, ttl, value).
func rewriteSetWithTTL(argv [][]byte, fx *Effects) [][][]byte {
	if !fx.Applied {
		return nil
	}
	cmd := argvOf("SET", argv[1], argv[3])
	cmd = append(cmd, []byte("PXAT"), msArg(fx.DeadlineMs))
	return [][][]byte{cmd}
}

// rewriteGetEX handles GETEX: only its TTL side effect is logged.
func rewriteGetEX(argv [][]byte, fx *Effects) [][][]byte {
	switch {
	case fx.Persist:
		return [][][]byte{argvOf("PERSIST", argv[1])}
	case fx.HasDeadline:
		return [][][]byte{argvOf("PEXPIREAT", argv[1], msArg(fx.DeadlineMs))}
	}
	return nil
}

// rewriteGetDel handles GETDEL: it replays as DEL.
func rewriteGetDel(argv [][]byte, fx *Effects) [][][]byte {
	if !fx.Applied {
		return nil
	}
	return [][][]byte{argvOf("DEL", argv[1])}
}

// rewriteSPop handles SPOP: the random choice is replaced by an SREM of the
// members actually popped, so replay removes exactly the same ones.
func rewriteSPop(argv [][]byte, fx *Effects) [][][]byte {
	if len(fx.Members) == 0 {
		return nil
	}
	cmd := argvOf("SREM", argv[1])
	for _, m := range fx.Members {
		cmd = append(cmd, []byte(m))
	}
	return [][][]byte{cmd}
}
