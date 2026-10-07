package command

// cmdSet supports only "SET key value" in Phase 1; options (EX, NX, ...) get a
// syntax error rather than being silently ignored.
func cmdSet(ctx *Context, args [][]byte) error {
	if len(args) > 2 {
		return ErrSyntax
	}
	ctx.Store.Set(string(args[0]), args[1])
	ctx.W.WriteSimpleString("OK")
	return nil
}

func cmdGet(ctx *Context, args [][]byte) error {
	v, ok := ctx.Store.Get(string(args[0]))
	if !ok {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.W.WriteBulk(v)
	return nil
}

func cmdAppend(ctx *Context, args [][]byte) error {
	var n int
	err := ctx.Store.Update(string(args[0]), func(old []byte, _ bool) ([]byte, error) {
		nv := append(old, args[1]...) // old is a private copy, safe to extend
		n = len(nv)
		return nv, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// cmdStrlen copies the value via Get (Redis is O(1)); acceptable until the
// store grows a length-only accessor.
func cmdStrlen(ctx *Context, args [][]byte) error {
	v, _ := ctx.Store.Get(string(args[0]))
	ctx.W.WriteInteger(int64(len(v)))
	return nil
}

// cmdMSet writes pairs one by one: atomic per key, not across keys.
func cmdMSet(ctx *Context, args [][]byte) error {
	if len(args)%2 != 0 {
		return WrongArgs("mset")
	}
	for i := 0; i < len(args); i += 2 {
		ctx.Store.Set(string(args[i]), args[i+1])
	}
	ctx.W.WriteSimpleString("OK")
	return nil
}

func cmdMGet(ctx *Context, args [][]byte) error {
	ctx.W.WriteArrayHeader(len(args))
	for _, k := range args {
		if v, ok := ctx.Store.Get(string(k)); ok {
			ctx.W.WriteBulk(v)
		} else {
			ctx.W.WriteNullBulk()
		}
	}
	return nil
}
