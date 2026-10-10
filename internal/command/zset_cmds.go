package command

import (
	"math"
	"strings"

	"github.com/nima-ca/sylphy/internal/store"
	"github.com/nima-ca/sylphy/internal/zset"
)

// zsetSpecs returns the sorted-set command table. NewDefaultRegistry appends it.
func zsetSpecs() []Spec {
	const (
		ro = FlagReadOnly
		w  = FlagWrite
		f  = FlagFast
	)
	return []Spec{
		{Name: "ZADD", Arity: -4, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZAdd},
		{Name: "ZREM", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRem},
		{Name: "ZSCORE", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZScore},
		{Name: "ZMSCORE", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZMScore},
		{Name: "ZINCRBY", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZIncrBy},
		{Name: "ZCARD", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZCard},
		{Name: "ZCOUNT", Arity: 4, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZCount},
		{Name: "ZRANK", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRank},
		{Name: "ZREVRANK", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRevRank},
		{Name: "ZRANGE", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRange},
		{Name: "ZREVRANGE", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRevRange},
		{Name: "ZRANGEBYSCORE", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRangeByScore},
		{Name: "ZREVRANGEBYSCORE", Arity: -4, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRevRangeByScore},
		{Name: "ZREMRANGEBYRANK", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRemRangeByRank},
		{Name: "ZREMRANGEBYSCORE", Arity: 4, Flags: w, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZRemRangeByScore},
		{Name: "ZPOPMIN", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZPopMin},
		{Name: "ZPOPMAX", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdZPopMax},
	}
}

// zsetOf returns the sorted set stored in v: (nil, nil) for a missing key and
// store.ErrWrongType for any other kind. As with listOf, a nil *zset.ZSet must
// never be returned from a Mutate closure as a store.Value.
func zsetOf(v store.Value) (*zset.ZSet, error) {
	if v == nil {
		return nil, nil
	}
	z, ok := v.(*zset.ZSet)
	if !ok {
		return nil, store.ErrWrongType
	}
	return z, nil
}

// writeEntries writes entries as a flat array of members, or of member/score
// pairs (the RESP2 shape of WITHSCORES). Entries hold immutable strings, so
// this is safe after the shard lock is released.
func writeEntries(ctx *Context, es []zset.Entry, withScores bool) {
	if withScores {
		ctx.W.WriteArrayHeader(2 * len(es))
	} else {
		ctx.W.WriteArrayHeader(len(es))
	}
	for _, e := range es {
		ctx.W.WriteBulkString(e.Member)
		if withScores {
			ctx.W.WriteBulkString(formatScore(e.Score))
		}
	}
}

// cmdZAdd implements ZADD key [NX|XX] [GT|LT] [CH] [INCR] score member ...
// All scores are parsed before the key is touched, so a bad score changes
// nothing. GT and LT only restrain updates of existing members; new members
// are always added unless XX forbids it. Without INCR the reply counts added
// members (plus changed ones with CH); with INCR it is the member's new score,
// or nil when NX/XX/GT/LT skipped it.
func cmdZAdd(ctx *Context, args [][]byte) error {
	o, pairs, err := parseZAddArgs(args)
	if err != nil {
		return err
	}
	n := len(pairs) / 2
	scores := make([]float64, n)
	members := make([]string, n)
	for i := range n {
		if scores[i], err = parseFloat(pairs[2*i]); err != nil {
			return err
		}
		members[i] = string(pairs[2*i+1])
	}

	var added, updated int
	var final float64
	var processed bool
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		z, err := zsetOf(cur)
		if err != nil {
			return nil, err
		}
		if z == nil {
			z = zset.New() // an empty result is dropped by the store
		}
		for i, m := range members {
			score := scores[i]
			old, exists := z.Score(m)
			if (!exists && o.xx) || (exists && o.nx) {
				continue
			}
			if exists {
				if o.incr {
					score += old
					if math.IsNaN(score) {
						return nil, errNaNScore // INCR takes a single pair: nothing was modified yet
					}
				}
				if (o.lt && score >= old) || (o.gt && score <= old) {
					continue
				}
				if score != old {
					z.Set(m, score)
					updated++
				}
			} else {
				z.Set(m, score)
				added++
			}
			processed = true
			final = score
		}
		return z, nil
	})
	if err != nil {
		return err
	}
	switch {
	case o.incr && !processed:
		ctx.W.WriteNullBulk()
	case o.incr:
		ctx.W.WriteBulkString(formatScore(final))
	case o.ch:
		ctx.W.WriteInteger(int64(added + updated))
	default:
		ctx.W.WriteInteger(int64(added))
	}
	return nil
}

// cmdZRem implements ZREM key member [member ...]. Removing the last member
// deletes the key, TTL included (the store does that).
func cmdZRem(ctx *Context, args [][]byte) error {
	members := stringsOf(args[1:])
	var removed int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		z, err := zsetOf(cur)
		if err != nil || z == nil {
			return nil, err
		}
		for _, m := range members {
			if z.Remove(m) {
				removed++
			}
		}
		return z, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(removed))
	return nil
}

func cmdZScore(ctx *Context, args [][]byte) error {
	member := string(args[1])
	var score float64
	var found bool
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		z, err := zsetOf(v)
		if err != nil || z == nil {
			return err
		}
		score, found = z.Score(member)
		return nil
	})
	if err != nil {
		return err
	}
	if !found {
		ctx.W.WriteNullBulk()
		return nil
	}
	ctx.W.WriteBulkString(formatScore(score))
	return nil
}

func cmdZMScore(ctx *Context, args [][]byte) error {
	members := stringsOf(args[1:])
	scores := make([]float64, len(members))
	found := make([]bool, len(members))
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		z, err := zsetOf(v)
		if err != nil || z == nil {
			return err
		}
		for i, m := range members {
			scores[i], found[i] = z.Score(m)
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteArrayHeader(len(members))
	for i := range members {
		if found[i] {
			ctx.W.WriteBulkString(formatScore(scores[i]))
		} else {
			ctx.W.WriteNullBulk()
		}
	}
	return nil
}

// cmdZIncrBy implements ZINCRBY key increment member. An increment that would
// make the score NaN (inf + -inf) is an error and changes nothing.
func cmdZIncrBy(ctx *Context, args [][]byte) error {
	inc, err := parseFloat(args[1])
	if err != nil {
		return err
	}
	member := string(args[2])
	var result float64
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		z, err := zsetOf(cur)
		if err != nil {
			return nil, err
		}
		if z == nil {
			z = zset.New()
		}
		result = inc
		if old, ok := z.Score(member); ok {
			result = old + inc
		}
		if math.IsNaN(result) {
			return nil, errNaNScore
		}
		z.Set(member, result)
		return z, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteBulkString(formatScore(result))
	return nil
}

func cmdZCard(ctx *Context, args [][]byte) error {
	var n int
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		z, err := zsetOf(v)
		if z != nil {
			n = z.Len()
		}
		return err
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// cmdZCount implements ZCOUNT key min max. The bounds are validated before
// the key is looked up.
func cmdZCount(ctx *Context, args [][]byte) error {
	r, err := parseScoreRange(args[1], args[2])
	if err != nil {
		return err
	}
	var n int
	err = ctx.Store.View(string(args[0]), func(v store.Value) error {
		z, err := zsetOf(v)
		if z != nil {
			n = z.Count(r)
		}
		return err
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// zrankGeneric implements ZRANK and ZREVRANK key member [WITHSCORE]. A missing
// member replies nil, or a null array with WITHSCORE.
func zrankGeneric(ctx *Context, args [][]byte, rev bool) error {
	withScore := false
	switch {
	case len(args) > 3:
		return ErrSyntax
	case len(args) == 3:
		if !strings.EqualFold(string(args[2]), "WITHSCORE") {
			return ErrSyntax
		}
		withScore = true
	}
	member := string(args[1])
	var rank, n int
	var score float64
	var found bool
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		z, err := zsetOf(v)
		if err != nil || z == nil {
			return err
		}
		if rank, found = z.Rank(member); !found {
			return nil
		}
		score, _ = z.Score(member)
		n = z.Len()
		return nil
	})
	if err != nil {
		return err
	}
	switch {
	case !found && withScore:
		ctx.W.WriteNullArray()
		return nil
	case !found:
		ctx.W.WriteNullBulk()
		return nil
	}
	if rev {
		rank = n - 1 - rank
	}
	if withScore {
		ctx.W.WriteArrayHeader(2)
		ctx.W.WriteInteger(int64(rank))
		ctx.W.WriteBulkString(formatScore(score))
		return nil
	}
	ctx.W.WriteInteger(int64(rank))
	return nil
}

func cmdZRank(ctx *Context, args [][]byte) error    { return zrankGeneric(ctx, args, false) }
func cmdZRevRank(ctx *Context, args [][]byte) error { return zrankGeneric(ctx, args, true) }

// zrangeReply runs a parsed range request and writes the reply.
func zrangeReply(ctx *Context, key string, r rangeReq) error {
	var out []zset.Entry
	err := ctx.Store.View(key, func(v store.Value) error {
		z, err := zsetOf(v)
		if err != nil || z == nil {
			return err
		}
		if r.byScore {
			out = z.RangeByScore(r.scores, r.rev, clampInt(r.offset), clampInt(r.count))
		} else if lo, hi, ok := normalizeRange(r.start, r.stop, z.Len()); ok {
			out = z.RangeByRank(lo, hi, r.rev)
		}
		return nil
	})
	if err != nil {
		return err
	}
	writeEntries(ctx, out, r.withScores)
	return nil
}

// cmdZRange implements ZRANGE key start stop [BYSCORE] [REV] [LIMIT offset
// count] [WITHSCORES]. BYLEX is not supported (see docs/COMMANDS.md).
func cmdZRange(ctx *Context, args [][]byte) error {
	r, err := parseZRange(args, rngWithScores|rngLimit|rngRev|rngByScore, rangeReq{})
	if err != nil {
		return err
	}
	return zrangeReply(ctx, string(args[0]), r)
}

func cmdZRevRange(ctx *Context, args [][]byte) error {
	r, err := parseZRange(args, rngWithScores, rangeReq{rev: true})
	if err != nil {
		return err
	}
	return zrangeReply(ctx, string(args[0]), r)
}

func cmdZRangeByScore(ctx *Context, args [][]byte) error {
	r, err := parseZRange(args, rngWithScores|rngLimit, rangeReq{byScore: true})
	if err != nil {
		return err
	}
	return zrangeReply(ctx, string(args[0]), r)
}

func cmdZRevRangeByScore(ctx *Context, args [][]byte) error {
	r, err := parseZRange(args, rngWithScores|rngLimit, rangeReq{byScore: true, rev: true})
	if err != nil {
		return err
	}
	return zrangeReply(ctx, string(args[0]), r)
}

// cmdZRemRangeByRank implements ZREMRANGEBYRANK key start stop (inclusive,
// negative indexes count from the end).
func cmdZRemRangeByRank(ctx *Context, args [][]byte) error {
	start, err := parseInt(args[1])
	if err != nil {
		return err
	}
	stop, err := parseInt(args[2])
	if err != nil {
		return err
	}
	var removed int
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		z, err := zsetOf(cur)
		if err != nil || z == nil {
			return nil, err
		}
		if lo, hi, ok := normalizeRange(start, stop, z.Len()); ok {
			removed = z.RemoveRangeByRank(lo, hi)
		}
		return z, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(removed))
	return nil
}

// cmdZRemRangeByScore implements ZREMRANGEBYSCORE key min max.
func cmdZRemRangeByScore(ctx *Context, args [][]byte) error {
	r, err := parseScoreRange(args[1], args[2])
	if err != nil {
		return err
	}
	var removed int
	err = ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		z, err := zsetOf(cur)
		if err != nil || z == nil {
			return nil, err
		}
		removed = z.RemoveRangeByScore(r)
		return z, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(removed))
	return nil
}

// zpopGeneric implements ZPOPMIN and ZPOPMAX key [count]. The count (>= 0) is
// validated before the key is looked up; the reply is always a flat
// member/score array, empty for a missing key or a count of 0.
func zpopGeneric(ctx *Context, args [][]byte, highest bool) error {
	if len(args) > 2 {
		return ErrSyntax
	}
	count := int64(1)
	if len(args) == 2 {
		n, err := parseInt(args[1])
		if err != nil {
			return err
		}
		if n < 0 {
			return ErrMustBePositive
		}
		count = n
	}
	var popped []zset.Entry
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		z, err := zsetOf(cur)
		if err != nil || z == nil {
			return nil, err
		}
		if highest {
			popped = z.PopMax(clampInt(count))
		} else {
			popped = z.PopMin(clampInt(count))
		}
		return z, nil // an emptied set is deleted by the store, TTL included
	})
	if err != nil {
		return err
	}
	writeEntries(ctx, popped, true)
	return nil
}

func cmdZPopMin(ctx *Context, args [][]byte) error { return zpopGeneric(ctx, args, false) }
func cmdZPopMax(ctx *Context, args [][]byte) error { return zpopGeneric(ctx, args, true) }
