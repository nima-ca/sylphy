package command

import (
	"github.com/nima-ca/sylphy/internal/store"
)

// EXPIRE-family condition flags.
const (
	expNX = 1 << iota
	expXX
	expGT
	expLT
)

// parseExpireFlags parses the optional NX|XX|GT|LT arguments. Messages match
// Redis, including echoing an unknown option verbatim.
func parseExpireFlags(args [][]byte) (int, error) {
	var f int
	sc := newOptScanner(args)
	for sc.more() {
		raw := string(args[sc.pos])
		switch sc.next() {
		case "NX":
			f |= expNX
		case "XX":
			f |= expXX
		case "GT":
			f |= expGT
		case "LT":
			f |= expLT
		default:
			return 0, &ReplyError{Msg: "ERR Unsupported option " + raw}
		}
	}
	if f&expNX != 0 && f&(expXX|expGT|expLT) != 0 {
		return 0, &ReplyError{Msg: "ERR NX and XX, GT or LT options at the same time are not compatible"}
	}
	if f&expGT != 0 && f&expLT != 0 {
		return 0, &ReplyError{Msg: "ERR GT and LT options at the same time are not compatible"}
	}
	return f, nil
}

// expireAllowed applies the conditions to a new deadline. A key without a TTL
// counts as having an infinite one, so GT never matches it and LT always does.
func expireAllowed(flags int, when, cur int64, hasTTL bool) bool {
	switch {
	case flags&expNX != 0 && hasTTL:
		return false
	case flags&expXX != 0 && !hasTTL:
		return false
	case flags&expGT != 0 && (!hasTTL || when <= cur):
		return false
	case flags&expLT != 0 && hasTTL && when >= cur:
		return false
	}
	return true
}

// expireGeneric implements EXPIRE, PEXPIRE, EXPIREAT and PEXPIREAT. As in
// Redis, options are parsed before the integer, and a deadline that is not in
// the future deletes the key (reply 1). The deadline it applied is recorded in
// ctx.fx for the PEXPIREAT rewrite.
func expireGeneric(ctx *Context, args [][]byte, name string, unit timeUnit, absolute bool) error {
	flags, err := parseExpireFlags(args[2:])
	if err != nil {
		return err
	}
	v, err := parseInt(args[1])
	if err != nil {
		return err
	}
	when, ok := expireDeadline(v, unit, absolute, ctx.Store.NowMs())
	if !ok {
		return InvalidExpireTime(name)
	}
	var changed bool
	err = ctx.Store.MutateEntry(string(args[0]), func(e *store.Entry) error {
		if !e.Exists() {
			return nil
		}
		cur, hasTTL := e.ExpireAt()
		if !expireAllowed(flags, when, cur, hasTTL) {
			return nil
		}
		changed = e.SetExpireAt(when)
		return nil
	})
	if err != nil {
		return err
	}
	ctx.fx.Applied, ctx.fx.DeadlineMs = changed, when
	if changed {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
	return nil
}

func cmdExpire(ctx *Context, args [][]byte) error {
	return expireGeneric(ctx, args, "expire", unitSeconds, false)
}

func cmdPExpire(ctx *Context, args [][]byte) error {
	return expireGeneric(ctx, args, "pexpire", unitMillis, false)
}

func cmdExpireAt(ctx *Context, args [][]byte) error {
	return expireGeneric(ctx, args, "expireat", unitSeconds, true)
}

func cmdPExpireAt(ctx *Context, args [][]byte) error {
	return expireGeneric(ctx, args, "pexpireat", unitMillis, true)
}

// ttlReply writes the shared TTL/PTTL/EXPIRETIME/PEXPIRETIME reply: -2 for a
// missing key, -1 for no TTL, otherwise convert(deadline).
func ttlReply(ctx *Context, key []byte, convert func(deadline int64) int64) error {
	at := ctx.Store.ExpireAt(string(key))
	if at < 0 { // -2 missing, -1 no TTL
		ctx.W.WriteInteger(at)
		return nil
	}
	ctx.W.WriteInteger(convert(at))
	return nil
}

// remaining is the time left until deadline, never negative (a key whose
// deadline is the current millisecond is still alive with 0 left).
func (c *Context) remaining(deadline int64) int64 {
	return max(deadline-c.Store.NowMs(), 0)
}

func cmdTTL(ctx *Context, args [][]byte) error {
	return ttlReply(ctx, args[0], func(at int64) int64 { return roundedSeconds(ctx.remaining(at)) })
}

func cmdPTTL(ctx *Context, args [][]byte) error {
	return ttlReply(ctx, args[0], ctx.remaining)
}

func cmdExpireTime(ctx *Context, args [][]byte) error {
	return ttlReply(ctx, args[0], roundedSeconds)
}

func cmdPExpireTime(ctx *Context, args [][]byte) error {
	return ttlReply(ctx, args[0], func(at int64) int64 { return at })
}

// cmdPersist implements PERSIST key: 1 if a TTL was removed, else 0.
func cmdPersist(ctx *Context, args [][]byte) error {
	var removed bool
	err := ctx.Store.MutateEntry(string(args[0]), func(e *store.Entry) error {
		removed = e.Exists() && e.Persist()
		return nil
	})
	if err != nil {
		return err
	}
	ctx.fx.Applied = removed
	if removed {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
	return nil
}
