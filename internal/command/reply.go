package command

// writeStrings writes ss as an array of bulk strings. The strings come from
// the hash and set packages, which hand out immutable values, so this is safe
// to call after the shard lock has been released.
func writeStrings(ctx *Context, ss []string) {
	ctx.W.WriteArrayHeader(len(ss))
	for _, s := range ss {
		ctx.W.WriteBulkString(s)
	}
}
