package command

import (
	"math"
	"strconv"

	"github.com/nima-ca/sylphy/internal/hash"
	"github.com/nima-ca/sylphy/internal/store"
)

// hashSpecs returns the hash command table. NewDefaultRegistry appends it.
func hashSpecs() []Spec {
	const (
		ro = FlagReadOnly
		w  = FlagWrite
		f  = FlagFast
	)
	return []Spec{
		{Name: "HSET", Arity: -4, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHSet},
		{Name: "HSETNX", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHSetNX},
		{Name: "HGET", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHGet},
		{Name: "HMGET", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHMGet},
		{Name: "HDEL", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHDel},
		{Name: "HGETALL", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHGetAll},
		{Name: "HKEYS", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHKeys},
		{Name: "HVALS", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHVals},
		{Name: "HEXISTS", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHExists},
		{Name: "HLEN", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHLen},
		{Name: "HSTRLEN", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHStrLen},
		{Name: "HINCRBY", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHIncrBy},
		{Name: "HINCRBYFLOAT", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdHIncrByFloat},
	}
}

// hashOf returns the hash stored in v: (nil, nil) for a missing key and
// store.ErrWrongType for any other kind. As with listOf, a nil *hash.Hash must
// never be returned from a Mutate closure as a store.Value.
func hashOf(v store.Value) (*hash.Hash, error) {
	if v == nil {
		return nil, nil
	}
	hs, ok := v.(*hash.Hash)
	if !ok {
		return nil, store.ErrWrongType
	}
	return hs, nil
}

// stringsOf converts command arguments to strings (the copy happens here, so
// callers can do it before taking the shard lock).
func stringsOf(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

// cmdHSet implements HSET key field value [field value ...]: the number of
// fields that were new (updated fields are not counted).
func cmdHSet(ctx *Context, args [][]byte) error {
	if len(args)%2 == 0 { // key plus complete pairs is an odd count
		return WrongArgs("hset")
	}
	kv := stringsOf(args[1:])
	var added int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		hs, err := hashOf(cur)
		if err != nil {
			return nil, err
		}
		if hs == nil {
			hs = hash.New()
		}
		for i := 0; i < len(kv); i += 2 {
			if hs.Set(kv[i], kv[i+1]) {
				added++
			}
		}
		return hs, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(added))
	return nil
}

// cmdHSetNX implements HSETNX key field value.
func cmdHSetNX(ctx *Context, args [][]byte) error {
	field, value := string(args[1]), string(args[2])
	var stored bool
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		hs, err := hashOf(cur)
		if err != nil {
			return nil, err
		}
		if hs == nil {
			hs = hash.New()
		}
		stored = hs.SetNX(field, value)
		return hs, nil
	})
	if err != nil {
		return err
	}
	if stored {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
	return nil
}

func cmdHGet(ctx *Context, args [][]byte) error {
	field := string(args[1])
	var val string
	var found bool
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		hs, err := hashOf(v)
		if err != nil || hs == nil {
			return err
		}
		val, found = hs.Get(field)
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.W.WriteBulkString(val)
	return nil
}

// cmdHMGet implements HMGET key field [field ...]: one reply slot per field,
// null for each missing one (all null when the key is missing).
func cmdHMGet(ctx *Context, args [][]byte) error {
	fields := stringsOf(args[1:])
	vals := make([]string, len(fields))
	found := make([]bool, len(fields))
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		hs, err := hashOf(v)
		if err != nil || hs == nil {
			return err
		}
		for i, f := range fields {
			vals[i], found[i] = hs.Get(f)
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteArrayHeader(len(fields))
	for i := range fields {
		if found[i] {
			ctx.W.WriteBulkString(vals[i])
		} else {
			ctx.W.WriteNullBulk()
		}
	}
	return nil
}

// cmdHDel implements HDEL key field [field ...]. Deleting the last field
// deletes the key (the store does that, TTL included).
func cmdHDel(ctx *Context, args [][]byte) error {
	fields := stringsOf(args[1:])
	var removed int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		hs, err := hashOf(cur)
		if err != nil || hs == nil {
			return nil, err
		}
		for _, f := range fields {
			if hs.Delete(f) {
				removed++
			}
		}
		return hs, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(removed))
	return nil
}

// hashSnapshot copies out some part of a hash under the read lock.
func hashSnapshot(ctx *Context, key []byte, pick func(*hash.Hash) []string) error {
	var out []string
	err := ctx.Store.View(string(key), func(v store.Value) error {
		hs, err := hashOf(v)
		if err != nil || hs == nil {
			return err
		}
		out = pick(hs)
		return nil
	})
	if err != nil {
		return err
	}
	writeStrings(ctx, out)
	return nil
}

func cmdHGetAll(ctx *Context, args [][]byte) error {
	return hashSnapshot(ctx, args[0], (*hash.Hash).Flat)
}

func cmdHKeys(ctx *Context, args [][]byte) error {
	return hashSnapshot(ctx, args[0], (*hash.Hash).Fields)
}

func cmdHVals(ctx *Context, args [][]byte) error {
	return hashSnapshot(ctx, args[0], (*hash.Hash).Values)
}

func cmdHExists(ctx *Context, args [][]byte) error {
	field := string(args[1])
	var ok bool
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		hs, err := hashOf(v)
		if err != nil || hs == nil {
			return err
		}
		ok = hs.Has(field)
		return nil
	})
	if err != nil {
		return err
	}
	if ok {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
	return nil
}

func cmdHLen(ctx *Context, args [][]byte) error {
	var n int
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		hs, err := hashOf(v)
		if hs != nil {
			n = hs.Len()
		}
		return err
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// cmdHStrLen implements HSTRLEN key field: the byte length of the value, 0 when
// the field or key is missing.
func cmdHStrLen(ctx *Context, args [][]byte) error {
	field := string(args[1])
	var n int
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		hs, err := hashOf(v)
		if err != nil || hs == nil {
			return err
		}
		val, _ := hs.Get(field)
		n = len(val)
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// cmdHIncrBy implements HINCRBY key field increment. The increment is parsed
// first, then the key's type and the field's value are checked, and only then
// is anything written, so a failure leaves the hash untouched (and never
// creates the key).
func cmdHIncrBy(ctx *Context, args [][]byte) error {
	incr, err := parseInt(args[2])
	if err != nil {
		return err
	}
	field := string(args[1])
	var result int64
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		hs, err := hashOf(cur)
		if err != nil {
			return nil, err
		}
		var old int64
		if hs != nil {
			if v, ok := hs.Get(field); ok {
				n, ok := parseInt64Strict([]byte(v))
				if !ok {
					return nil, ErrHashNotInt
				}
				old = n
			}
		}
		if (incr > 0 && old > math.MaxInt64-incr) || (incr < 0 && old < math.MinInt64-incr) {
			return nil, ErrOverflow
		}
		result = old + incr
		if hs == nil {
			hs = hash.New()
		}
		hs.Set(field, strconv.FormatInt(result, 10))
		return hs, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(result)
	return nil
}

// cmdHIncrByFloat implements HINCRBYFLOAT key field increment with Redis's
// long double arithmetic and number format (see longdouble.go). The reply is
// the new value as a bulk string, which is also what is stored.
func cmdHIncrByFloat(ctx *Context, args [][]byte) error {
	incr, ok := parseLongDouble(string(args[2]))
	if !ok {
		return ErrNotFloat
	}
	field := string(args[1])
	var result string
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		hs, err := hashOf(cur)
		if err != nil {
			return nil, err
		}
		old := newLongDouble()
		if hs != nil {
			if v, found := hs.Get(field); found {
				if old, ok = parseLongDouble(v); !ok {
					return nil, ErrHashNotFloat
				}
			}
		}
		out, err := addLongDouble(old, incr)
		if err != nil {
			return nil, err
		}
		if hs == nil {
			hs = hash.New()
		}
		hs.Set(field, out)
		result = out
		return hs, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteBulkString(result)
	return nil
}
