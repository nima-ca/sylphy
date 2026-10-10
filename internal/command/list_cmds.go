package command

import (
	"math"
	"strings"

	"github.com/nima-ca/sylphy/internal/list"
	"github.com/nima-ca/sylphy/internal/store"
)

// listSpecs returns the list command table. NewDefaultRegistry appends it.
func listSpecs() []Spec {
	const (
		ro = FlagReadOnly
		w  = FlagWrite
		f  = FlagFast
	)
	return []Spec{
		{Name: "LPUSH", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLPush},
		{Name: "RPUSH", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdRPush},
		{Name: "LPUSHX", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLPushX},
		{Name: "RPUSHX", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdRPushX},
		{Name: "LPOP", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLPop},
		{Name: "RPOP", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdRPop},
		{Name: "LLEN", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLLen},
		{Name: "LRANGE", Arity: 4, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLRange},
		{Name: "LINDEX", Arity: 3, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLIndex},
		{Name: "LSET", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLSet},
		{Name: "LREM", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLRem},
		{Name: "LTRIM", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLTrim},
		{Name: "LINSERT", Arity: 5, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdLInsert},
	}
}

// listOf returns the list stored in v. A nil v (missing key) yields (nil, nil);
// any other kind of value, including a Collection that is not a *list.List,
// yields store.ErrWrongType. Callers must check the error before using the
// list, and must never return a nil *list.List as a store.Value (a typed nil
// is not a nil Value).
func listOf(v store.Value) (*list.List, error) {
	if v == nil {
		return nil, nil
	}
	l, ok := v.(*list.List)
	if !ok {
		return nil, store.ErrWrongType
	}
	return l, nil
}

// parseCount parses the optional count of LPOP/RPOP: an integer >= 0. Like
// Redis, every failure, including a non-integer, reports "must be positive".
func parseCount(b []byte) (int64, error) {
	n, ok := parseInt64Strict(b)
	if !ok || n < 0 {
		return 0, ErrMustBePositive
	}
	return n, nil
}

// resolveIndex turns a Redis list index (negative counts from the end) into a
// position in a list of n elements.
func resolveIndex(i int64, n int) (int, bool) {
	if i < 0 {
		i += int64(n)
	}
	if i < 0 || i >= int64(n) {
		return 0, false
	}
	return int(i), true
}

// pushGeneric implements LPUSH, RPUSH, LPUSHX and RPUSHX: every list growth in
// the server goes through here, which is also where a future blocking-pop
// wake-up would hook in. onlyExisting makes a missing key a no-op that
// replies 0. Elements are pushed one by one, so "LPUSH k a b" yields b a.
func pushGeneric(ctx *Context, args [][]byte, front, onlyExisting bool) error {
	var n int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		l, err := listOf(cur)
		if err != nil {
			return nil, err
		}
		if l == nil {
			if onlyExisting {
				return nil, nil
			}
			l = list.New()
		}
		for _, e := range args[1:] {
			if front {
				l.PushFront(e)
			} else {
				l.PushBack(e)
			}
		}
		n = l.Len()
		return l, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

func cmdLPush(ctx *Context, args [][]byte) error  { return pushGeneric(ctx, args, true, false) }
func cmdRPush(ctx *Context, args [][]byte) error  { return pushGeneric(ctx, args, false, false) }
func cmdLPushX(ctx *Context, args [][]byte) error { return pushGeneric(ctx, args, true, true) }
func cmdRPushX(ctx *Context, args [][]byte) error { return pushGeneric(ctx, args, false, true) }

// popGeneric implements LPOP and RPOP [count]. As in Redis, the count is
// validated before the key is looked up, a missing key replies nil (null array
// when a count was given), and a count of 0 on an existing list replies an
// empty array. The popped elements are owned by the handler, so the reply is
// written after the lock is released. The count never sizes an allocation:
// the list clamps it to its length.
func popGeneric(ctx *Context, args [][]byte, front bool) error {
	name := "rpop"
	if front {
		name = "lpop"
	}
	if len(args) > 2 {
		return WrongArgs(name)
	}
	hasCount := len(args) == 2
	var count int64
	if hasCount {
		var err error
		if count, err = parseCount(args[1]); err != nil {
			return err
		}
	}
	var popped [][]byte
	var found bool
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		l, err := listOf(cur)
		if err != nil {
			return nil, err
		}
		if l == nil {
			return nil, nil
		}
		found = true
		k := int(min(count, int64(l.Len())))
		switch {
		case hasCount && front:
			popped = l.PopFrontN(k)
		case hasCount:
			popped = l.PopBackN(k)
		case front:
			b, _ := l.PopFront()
			popped = [][]byte{b}
		default:
			b, _ := l.PopBack()
			popped = [][]byte{b}
		}
		return l, nil // an emptied list is deleted by the store, TTL included
	})
	if err != nil {
		return err
	}
	switch {
	case !found && hasCount:
		ctx.W.WriteNullArray()
	case !found:
		ctx.W.WriteNullBulk()
	case hasCount:
		ctx.W.WriteBulkArray(popped)
	default:
		ctx.W.WriteBulk(popped[0])
	}
	return nil
}

func cmdLPop(ctx *Context, args [][]byte) error { return popGeneric(ctx, args, true) }
func cmdRPop(ctx *Context, args [][]byte) error { return popGeneric(ctx, args, false) }

func cmdLLen(ctx *Context, args [][]byte) error {
	var n int
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		l, err := listOf(v)
		if l != nil {
			n = l.Len()
		}
		return err
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// cmdLRange implements LRANGE key start stop. The range is copied out in one
// allocation under the read lock, so the cost under the lock is O(range bytes).
func cmdLRange(ctx *Context, args [][]byte) error {
	start, err := parseInt(args[1])
	if err != nil {
		return err
	}
	stop, err := parseInt(args[2])
	if err != nil {
		return err
	}
	var items [][]byte
	err = ctx.Store.View(string(args[0]), func(v store.Value) error {
		l, err := listOf(v)
		if err != nil || l == nil {
			return err
		}
		if lo, hi, ok := normalizeRange(start, stop, l.Len()); ok {
			items = l.Range(lo, hi)
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteBulkArray(items)
	return nil
}

func cmdLIndex(ctx *Context, args [][]byte) error {
	idx, err := parseInt(args[1])
	if err != nil {
		return err
	}
	var item []byte
	var found bool
	err = ctx.Store.View(string(args[0]), func(v store.Value) error {
		l, err := listOf(v)
		if err != nil || l == nil {
			return err
		}
		if i, ok := resolveIndex(idx, l.Len()); ok {
			b, _ := l.At(i)
			item, found = append([]byte(nil), b...), true
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
	ctx.W.WriteBulk(item)
	return nil
}

// cmdLSet implements LSET key index element. Unlike the push commands it never
// creates the key: a missing key is "ERR no such key". The index is parsed
// before the lookup, as in Redis.
func cmdLSet(ctx *Context, args [][]byte) error {
	idx, err := parseInt(args[1])
	if err != nil {
		return err
	}
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		l, err := listOf(cur)
		if err != nil {
			return nil, err
		}
		if l == nil {
			return nil, ErrNoSuchKey
		}
		i, ok := resolveIndex(idx, l.Len())
		if !ok {
			return nil, ErrIndexOutOfRange
		}
		l.Set(i, args[2])
		return l, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteSimpleString("OK")
	return nil
}

// cmdLRem implements LREM key count element: count > 0 removes from the head,
// count < 0 from the tail, 0 removes all matches. It replies the number removed.
func cmdLRem(ctx *Context, args [][]byte) error {
	count, err := parseInt(args[1])
	if err != nil {
		return err
	}
	limit := int(max(min(count, math.MaxInt), math.MinInt))
	var removed int
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		l, err := listOf(cur)
		if err != nil || l == nil {
			return nil, err
		}
		removed = l.Remove(limit, args[2])
		return l, nil // deleted by the store if everything was removed
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(removed))
	return nil
}

// cmdLTrim implements LTRIM key start stop. An empty resulting range deletes
// the key; a missing key is OK.
func cmdLTrim(ctx *Context, args [][]byte) error {
	start, err := parseInt(args[1])
	if err != nil {
		return err
	}
	stop, err := parseInt(args[2])
	if err != nil {
		return err
	}
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		l, err := listOf(cur)
		if err != nil || l == nil {
			return nil, err
		}
		lo, hi, ok := normalizeRange(start, stop, l.Len())
		if !ok {
			return nil, nil // nothing kept: delete the key and its TTL
		}
		l.Trim(lo, hi)
		return l, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteSimpleString("OK")
	return nil
}

// cmdLInsert implements LINSERT key BEFORE|AFTER pivot element: the new length,
// -1 if the pivot is absent, 0 if the key is missing. The first matching pivot
// from the head wins.
func cmdLInsert(ctx *Context, args [][]byte) error {
	before := strings.EqualFold(string(args[1]), "BEFORE")
	if !before && !strings.EqualFold(string(args[1]), "AFTER") {
		return ErrSyntax
	}
	var n int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		l, err := listOf(cur)
		if err != nil || l == nil {
			return nil, err
		}
		i := l.IndexOf(args[2])
		if i < 0 {
			n = -1
			return l, nil
		}
		if !before {
			i++
		}
		l.Insert(i, args[3])
		n = l.Len()
		return l, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}
