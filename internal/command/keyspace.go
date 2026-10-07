package command

import "strings"

func keysOf(args [][]byte) []string {
	keys := make([]string, len(args))
	for i, a := range args {
		keys[i] = string(a)
	}
	return keys
}

func cmdDel(ctx *Context, args [][]byte) error {
	ctx.W.WriteInteger(int64(ctx.Store.Delete(keysOf(args)...)))
	return nil
}

func cmdExists(ctx *Context, args [][]byte) error {
	ctx.W.WriteInteger(int64(ctx.Store.Exists(keysOf(args)...)))
	return nil
}

func cmdKeys(ctx *Context, args [][]byte) error {
	ks := ctx.Store.Keys(string(args[0]))
	ctx.W.WriteArrayHeader(len(ks))
	for _, k := range ks {
		ctx.W.WriteBulkString(k)
	}
	return nil
}

func cmdDBSize(ctx *Context, _ [][]byte) error {
	ctx.W.WriteInteger(int64(ctx.Store.Len()))
	return nil
}

// cmdFlushAll accepts the optional ASYNC|SYNC modifier; both behave the same.
func cmdFlushAll(ctx *Context, args [][]byte) error {
	if len(args) > 1 {
		return ErrSyntax
	}
	if len(args) == 1 {
		if m := strings.ToUpper(string(args[0])); m != "ASYNC" && m != "SYNC" {
			return ErrSyntax
		}
	}
	ctx.Store.Flush()
	ctx.W.WriteSimpleString("OK")
	return nil
}
