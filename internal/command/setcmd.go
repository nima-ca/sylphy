package command

import "github.com/nima-ca/sylphy/internal/store"

// stringOf copies the bytes of v so the handler can write its reply after the
// shard lock is released. A nil v reports ok=false; any other kind of value
// reports store.ErrWrongType.
func stringOf(v store.Value) (b []byte, ok bool, err error) {
	if v == nil {
		return nil, false, nil
	}
	s, is := v.(*store.String)
	if !is {
		return nil, false, store.ErrWrongType
	}
	return append([]byte(nil), s.Bytes()...), true, nil
}

// setOptions is the parsed form of SET's trailing arguments.
type setOptions struct {
	nx, xx, get, keepTTL bool
	hasExpire            bool
	expireAt             int64 // absolute Unix ms, valid when hasExpire
}

// parseSetOptions parses SET's options. Like Redis it first collects the
// tokens (reporting conflicts as syntax errors) and only then validates the
// expiry value, so "EX abc PX 5" is a syntax error, not a bad integer.
func parseSetOptions(args [][]byte, now int64) (setOptions, error) {
	var o setOptions
	var expTok string
	var expRaw []byte
	sc := newOptScanner(args)
	for sc.more() {
		switch tok := sc.next(); tok {
		case "NX":
			if o.xx {
				return o, ErrSyntax
			}
			o.nx = true
		case "XX":
			if o.nx {
				return o, ErrSyntax
			}
			o.xx = true
		case "GET":
			o.get = true
		case "KEEPTTL":
			if expTok != "" {
				return o, ErrSyntax
			}
			o.keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if expTok != "" || o.keepTTL {
				return o, ErrSyntax
			}
			raw, ok := sc.value()
			if !ok {
				return o, ErrSyntax
			}
			expTok, expRaw = tok, raw
		default:
			return o, ErrSyntax
		}
	}
	if expTok != "" {
		at, err := parseExpireValue("set", expRaw, expireOpts[expTok], now)
		if err != nil {
			return o, err
		}
		o.hasExpire, o.expireAt = true, at
	}
	return o, nil
}

// cmdSet implements SET key value [NX|XX] [GET] [EX|PX|EXAT|PXAT n|KEEPTTL].
// Plain SET takes the unconditional fast path; anything with options runs as
// one critical section so GET/NX/XX/TTL changes are atomic. The outcome and
// the applied deadline are recorded in ctx.fx for the rewrite hook.
func cmdSet(ctx *Context, args [][]byte) error {
	key := string(args[0])
	if len(args) == 2 {
		ctx.Store.Set(key, args[1]) // overwrites any type and clears the TTL
		ctx.fx.Applied = true
		ctx.W.WriteSimpleString("OK")
		return nil
	}
	o, err := parseSetOptions(args[2:], ctx.Store.NowMs())
	if err != nil {
		return err
	}
	nv := store.NewString(args[1]) // copy before taking the lock
	var old []byte
	var hadOld, applied bool
	err = ctx.Store.MutateEntry(key, func(e *store.Entry) error {
		cur := e.Value()
		if o.get {
			// Wrong type must fail before anything changes, even with NX/XX.
			var gerr error
			if old, hadOld, gerr = stringOf(cur); gerr != nil {
				return gerr
			}
		}
		if (o.nx && cur != nil) || (o.xx && cur == nil) {
			return nil
		}
		e.Put(nv)
		if !o.keepTTL {
			e.Persist()
		}
		if o.hasExpire {
			e.SetExpireAt(o.expireAt) // a deadline in the past deletes the key
		}
		applied = true
		return nil
	})
	if err != nil {
		return err
	}
	ctx.fx = Effects{Applied: applied, KeepTTL: o.keepTTL, HasDeadline: o.hasExpire, DeadlineMs: o.expireAt}
	switch {
	case o.get && hadOld:
		ctx.W.WriteBulk(old)
	case o.get, !applied:
		ctx.W.WriteNullBulk()
	default:
		ctx.W.WriteSimpleString("OK")
	}
	return nil
}

// cmdSetNX implements SETNX key value: 1 if the key was set, 0 if it existed
// (of any type).
func cmdSetNX(ctx *Context, args [][]byte) error {
	nv := store.NewString(args[1])
	var set bool
	err := ctx.Store.MutateEntry(string(args[0]), func(e *store.Entry) error {
		if e.Exists() {
			return nil
		}
		e.Put(nv)
		set = true
		return nil
	})
	if err != nil {
		return err
	}
	ctx.fx.Applied = set
	if set {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
	return nil
}

// setWithTTL implements SETEX and PSETEX: key, time to live, value.
func setWithTTL(ctx *Context, args [][]byte, name string, unit timeUnit) error {
	at, err := parseExpireValue(name, args[1], expireOpt{unit: unit}, ctx.Store.NowMs())
	if err != nil {
		return err
	}
	nv := store.NewString(args[2])
	err = ctx.Store.MutateEntry(string(args[0]), func(e *store.Entry) error {
		e.Put(nv)
		e.Persist()
		e.SetExpireAt(at)
		return nil
	})
	if err != nil {
		return err
	}
	ctx.fx = Effects{Applied: true, HasDeadline: true, DeadlineMs: at}
	ctx.W.WriteSimpleString("OK")
	return nil
}

func cmdSetEX(ctx *Context, args [][]byte) error  { return setWithTTL(ctx, args, "setex", unitSeconds) }
func cmdPSetEX(ctx *Context, args [][]byte) error { return setWithTTL(ctx, args, "psetex", unitMillis) }

// cmdGetEX implements GETEX key [EX|PX|EXAT|PXAT n|PERSIST]. The options are
// validated before the key is looked up, as in Redis, so a bad TTL errors even
// for a missing key.
func cmdGetEX(ctx *Context, args [][]byte) error {
	var persist, hasExpire bool
	var expTok string
	var expRaw []byte
	sc := newOptScanner(args[1:])
	for sc.more() {
		switch tok := sc.next(); tok {
		case "PERSIST":
			if persist || expTok != "" {
				return ErrSyntax
			}
			persist = true
		case "EX", "PX", "EXAT", "PXAT":
			if persist || expTok != "" {
				return ErrSyntax
			}
			raw, ok := sc.value()
			if !ok {
				return ErrSyntax
			}
			expTok, expRaw = tok, raw
		default:
			return ErrSyntax
		}
	}
	var at int64
	if expTok != "" {
		var err error
		if at, err = parseExpireValue("getex", expRaw, expireOpts[expTok], ctx.Store.NowMs()); err != nil {
			return err
		}
		hasExpire = true
	}

	var out []byte
	var found bool
	err := ctx.Store.MutateEntry(string(args[0]), func(e *store.Entry) error {
		var err error
		if out, found, err = stringOf(e.Value()); err != nil || !found {
			return err
		}
		switch {
		case persist:
			e.Persist()
		case hasExpire:
			e.SetExpireAt(at)
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.fx = Effects{Applied: true, Persist: persist, HasDeadline: hasExpire, DeadlineMs: at}
	ctx.W.WriteBulk(out)
	return nil
}

// cmdGetDel implements GETDEL key: return the string and delete the key.
func cmdGetDel(ctx *Context, args [][]byte) error {
	var out []byte
	var found bool
	err := ctx.Store.MutateEntry(string(args[0]), func(e *store.Entry) error {
		var err error
		if out, found, err = stringOf(e.Value()); err != nil || !found {
			return err
		}
		e.Delete()
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.fx.Applied = true
	ctx.W.WriteBulk(out)
	return nil
}
