package command

import "github.com/nima-ca/sylphy/internal/store"

func cmdGet(ctx *Context, args [][]byte) error {
	var out []byte
	var found bool
	err := ctx.Store.View(string(args[0]), func(v store.Value) (err error) {
		out, found, err = stringOf(v)
		return err
	})
	if err != nil {
		return err
	}
	if !found {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.W.WriteBulk(out)
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

// cmdStrlen reads the length under the read lock without copying the value.
func cmdStrlen(ctx *Context, args [][]byte) error {
	var n int
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		if v == nil {
			return nil
		}
		s, ok := v.(*store.String)
		if !ok {
			return store.ErrWrongType
		}
		n = len(s.Bytes())
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
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
