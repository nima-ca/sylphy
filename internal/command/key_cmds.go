package command

import (
	"errors"
	"strconv"
	"strings"

	"github.com/nima-ca/sylphy/internal/store"
)

// errInvalidCursor is the reply for a SCAN cursor that is not one the store
// issued.
var errInvalidCursor = &ReplyError{Msg: "ERR invalid cursor"}

// defaultScanCount is SCAN's COUNT when none is given, as in Redis.
const defaultScanCount = 10

// keySpecs returns the key-management command table. NewDefaultRegistry
// appends it.
func keySpecs() []Spec {
	const (
		ro = FlagReadOnly
		w  = FlagWrite
		f  = FlagFast
	)
	return []Spec{
		{Name: "TYPE", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdType},
		{Name: "RENAME", Arity: 3, Flags: w, FirstKey: 1, LastKey: 2, Step: 1, Handler: cmdRename},
		{Name: "RENAMENX", Arity: 3, Flags: w | f, FirstKey: 1, LastKey: 2, Step: 1, Handler: cmdRenameNX},
		{Name: "RANDOMKEY", Arity: 1, Flags: ro, Handler: cmdRandomKey},
		// UNLINK shares DEL's handler: freeing memory is synchronous here, so
		// there is nothing to defer.
		{Name: "UNLINK", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdDel},
		{Name: "TOUCH", Arity: -2, Flags: ro | f, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdTouch},
		{Name: "SCAN", Arity: -2, Flags: ro, Handler: cmdScan},
	}
}

// cmdType implements TYPE key: the kind name, or "none" for a missing key.
func cmdType(ctx *Context, args [][]byte) error {
	kind := store.Kind(0).String() // "none"
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		if v != nil {
			kind = v.Kind().String()
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteSimpleString(kind)
	return nil
}

// renameGeneric implements RENAME and RENAMENX. Both keys are locked together
// (in ascending shard order) so no reader ever sees the value under neither
// name or under both. The value and its TTL move as one; a destination of any
// type is overwritten and its old TTL discarded.
func renameGeneric(ctx *Context, args [][]byte, nx bool) error {
	src, dst := string(args[0]), string(args[1])
	moved := false
	err := ctx.Store.Atomic([]string{src, dst}, func(es []*store.Entry) error {
		from, to := es[0], es[1]
		if !from.Exists() {
			return ErrNoSuchKey
		}
		switch {
		case from == to: // same key: RENAME is a no-op, RENAMENX refuses
			moved = !nx
		case nx && to.Exists():
			// destination taken: leave both keys alone
		default:
			from.MoveTo(to)
			moved = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	if !nx {
		ctx.W.WriteSimpleString("OK")
		return nil
	}
	if moved {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
	return nil
}

func cmdRename(ctx *Context, args [][]byte) error   { return renameGeneric(ctx, args, false) }
func cmdRenameNX(ctx *Context, args [][]byte) error { return renameGeneric(ctx, args, true) }

// cmdRandomKey implements RANDOMKEY: nil when the keyspace is empty.
func cmdRandomKey(ctx *Context, _ [][]byte) error {
	k, ok := ctx.Store.RandomKey(ctx.intN)
	if !ok {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.W.WriteBulkString(k)
	return nil
}

// cmdTouch implements TOUCH key [key ...]. There is no idle-time tracking, so
// it is EXISTS under another name; a key repeated in the arguments counts
// each time.
func cmdTouch(ctx *Context, args [][]byte) error {
	ctx.W.WriteInteger(int64(ctx.Store.Exists(keysOf(args)...)))
	return nil
}

// cmdScan implements SCAN cursor [MATCH pattern] [COUNT count] [TYPE type].
// The cursor is validated first, then the options, as in Redis. COUNT is the
// number of slots examined per call (default 10, minimum 1); MATCH and TYPE
// filter what is returned, so a reply can be empty with a non-zero cursor.
// See store.Scan for the guarantee and the per-call cost.
func cmdScan(ctx *Context, args [][]byte) error {
	cursor, cerr := strconv.ParseUint(string(args[0]), 10, 64)
	if cerr != nil {
		return errInvalidCursor
	}
	count := int64(defaultScanCount)
	var pattern, typ string
	var hasPattern, hasType bool
	sc := newOptScanner(args[1:])
	for sc.more() {
		switch sc.next() {
		case "MATCH":
			v, ok := sc.value()
			if !ok {
				return ErrSyntax
			}
			pattern, hasPattern = string(v), true
		case "COUNT":
			v, ok := sc.value()
			if !ok {
				return ErrSyntax
			}
			n, perr := parseInt(v)
			if perr != nil {
				return perr
			}
			if n < 1 {
				return ErrSyntax
			}
			count = n
		case "TYPE":
			v, ok := sc.value()
			if !ok {
				return ErrSyntax
			}
			typ, hasType = string(v), true
		default:
			return ErrSyntax
		}
	}

	var keep func(key string, kind store.Kind) bool
	if (hasPattern && pattern != "*") || hasType {
		keep = func(key string, kind store.Kind) bool {
			if hasType && !strings.EqualFold(kind.String(), typ) {
				return false
			}
			return !hasPattern || pattern == "*" || store.Match(pattern, key)
		}
	}
	next, keys, serr := ctx.Store.Scan(cursor, clampInt(count), keep)
	if serr != nil {
		if errors.Is(serr, store.ErrInvalidCursor) {
			return errInvalidCursor
		}
		return serr
	}
	ctx.W.WriteArrayHeader(2)
	ctx.W.WriteBulkString(strconv.FormatUint(next, 10))
	ctx.W.WriteArrayHeader(len(keys))
	for _, k := range keys {
		ctx.W.WriteBulkString(k)
	}
	return nil
}
