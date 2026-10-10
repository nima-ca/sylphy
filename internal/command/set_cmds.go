package command

import (
	"cmp"
	"math"
	"slices"

	"github.com/nima-ca/sylphy/internal/set"
	"github.com/nima-ca/sylphy/internal/store"
)

// maxRepeatSample bounds how many members SRANDMEMBER with a negative count
// may return: the reply is built in memory, so an unbounded count would let one
// request allocate gigabytes. Redis streams it and has no such limit.
const maxRepeatSample = 1 << 20

// setSpecs returns the set command table. NewDefaultRegistry appends it.
func setSpecs() []Spec {
	const (
		ro = FlagReadOnly
		w  = FlagWrite
		f  = FlagFast
	)
	return []Spec{
		{Name: "SADD", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSAdd},
		{Name: "SREM", Arity: -3, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSRem},
		{Name: "SMEMBERS", Arity: 2, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSMembers},
		{Name: "SISMEMBER", Arity: 3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSIsMember},
		{Name: "SMISMEMBER", Arity: -3, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSMIsMember},
		{Name: "SCARD", Arity: 2, Flags: ro | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSCard},
		// SPOP picks members at random, so it is logged as an SREM of the members it removed.
		{Name: "SPOP", Arity: -2, Flags: w | f, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSPop, Rewrite: rewriteSPop},
		{Name: "SRANDMEMBER", Arity: -2, Flags: ro, FirstKey: 1, LastKey: 1, Step: 1, Handler: cmdSRandMember},
		{Name: "SMOVE", Arity: 4, Flags: w | f, FirstKey: 1, LastKey: 2, Step: 1, Handler: cmdSMove},
		{Name: "SUNION", Arity: -2, Flags: ro, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdSUnion},
		{Name: "SINTER", Arity: -2, Flags: ro, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdSInter},
		{Name: "SDIFF", Arity: -2, Flags: ro, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdSDiff},
		// For the *STORE forms every argument is a key: the destination comes first.
		{Name: "SUNIONSTORE", Arity: -3, Flags: w, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdSUnionStore},
		{Name: "SINTERSTORE", Arity: -3, Flags: w, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdSInterStore},
		{Name: "SDIFFSTORE", Arity: -3, Flags: w, FirstKey: 1, LastKey: -1, Step: 1, Handler: cmdSDiffStore},
	}
}

// setOf returns the set stored in v: (nil, nil) for a missing key and
// store.ErrWrongType for any other kind. A nil *set.Set must never be returned
// from a Mutate closure as a store.Value.
func setOf(v store.Value) (*set.Set, error) {
	if v == nil {
		return nil, nil
	}
	ss, ok := v.(*set.Set)
	if !ok {
		return nil, store.ErrWrongType
	}
	return ss, nil
}

func writeBool(ctx *Context, b bool) {
	if b {
		ctx.W.WriteInteger(1)
	} else {
		ctx.W.WriteInteger(0)
	}
}

// cmdSAdd implements SADD key member [member ...]: the number of members that
// were not already present.
func cmdSAdd(ctx *Context, args [][]byte) error {
	members := stringsOf(args[1:]) // copy before taking the lock
	var added int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		ss, err := setOf(cur)
		if err != nil {
			return nil, err
		}
		if ss == nil {
			ss = set.New()
		}
		for _, m := range members {
			if ss.Add(m) {
				added++
			}
		}
		return ss, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(added))
	return nil
}

// cmdSRem implements SREM key member [member ...]. Removing the last member
// deletes the key (the store does that, TTL included).
func cmdSRem(ctx *Context, args [][]byte) error {
	members := stringsOf(args[1:])
	var removed int
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		ss, err := setOf(cur)
		if err != nil || ss == nil {
			return nil, err
		}
		for _, m := range members {
			if ss.Remove(m) {
				removed++
			}
		}
		return ss, nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(removed))
	return nil
}

func cmdSMembers(ctx *Context, args [][]byte) error {
	var out []string
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		ss, err := setOf(v)
		if err != nil || ss == nil {
			return err
		}
		out = ss.Members()
		return nil
	})
	if err != nil {
		return err
	}
	writeStrings(ctx, out)
	return nil
}

func cmdSIsMember(ctx *Context, args [][]byte) error {
	m := string(args[1])
	var ok bool
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		ss, err := setOf(v)
		if err != nil || ss == nil {
			return err
		}
		ok = ss.Has(m)
		return nil
	})
	if err != nil {
		return err
	}
	writeBool(ctx, ok)
	return nil
}

func cmdSMIsMember(ctx *Context, args [][]byte) error {
	members := stringsOf(args[1:])
	res := make([]bool, len(members))
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		ss, err := setOf(v)
		if err != nil || ss == nil {
			return err
		}
		for i, m := range members {
			res[i] = ss.Has(m)
		}
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteArrayHeader(len(res))
	for _, b := range res {
		writeBool(ctx, b)
	}
	return nil
}

func cmdSCard(ctx *Context, args [][]byte) error {
	var n int
	err := ctx.Store.View(string(args[0]), func(v store.Value) error {
		ss, err := setOf(v)
		if ss != nil {
			n = ss.Len()
		}
		return err
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

// cmdSPop implements SPOP key [count]. Without a count it replies a bulk (nil
// for a missing key); with one it always replies an array, empty for a missing
// key or a count of 0. The count is validated before the key is looked up, and
// extra arguments are a syntax error, as in Redis. The members actually popped
// are recorded in ctx.fx for the SREM rewrite.
func cmdSPop(ctx *Context, args [][]byte) error {
	if len(args) > 2 {
		return ErrSyntax
	}
	hasCount := len(args) == 2
	var count int64
	if hasCount {
		var err error
		if count, err = parseCount(args[1]); err != nil {
			return err
		}
	}
	var popped []string
	var found bool
	err := ctx.Store.Mutate(string(args[0]), func(cur store.Value) (store.Value, error) {
		ss, err := setOf(cur)
		if err != nil || ss == nil {
			return nil, err
		}
		found = true
		if hasCount {
			popped = ss.PopN(int(min(count, int64(ss.Len()))), ctx.intN)
		} else {
			m, _ := ss.PopRandom(ctx.intN)
			popped = []string{m}
		}
		return ss, nil // an emptied set is deleted by the store, TTL included
	})
	if err != nil {
		return err
	}
	ctx.fx.Members = popped
	switch {
	case hasCount:
		writeStrings(ctx, popped)
	case !found:
		ctx.W.WriteNullBulk()
	default:
		ctx.W.WriteBulkString(popped[0])
	}
	return nil
}

// cmdSRandMember implements SRANDMEMBER key [count]. A positive count returns
// that many distinct members (all of them if the set is smaller), a negative
// count returns |count| members that may repeat.
func cmdSRandMember(ctx *Context, args [][]byte) error {
	if len(args) > 2 {
		return WrongArgs("srandmember")
	}
	key := string(args[0])
	if len(args) == 1 {
		var m string
		var found bool
		err := ctx.Store.View(key, func(v store.Value) error {
			ss, err := setOf(v)
			if err != nil || ss == nil {
				return err
			}
			m, found = ss.At(ctx.intN(ss.Len())), true
			return nil
		})
		if err != nil {
			return err
		}
		if !found {
			ctx.W.WriteNullBulk()
			return nil
		}
		ctx.W.WriteBulkString(m)
		return nil
	}
	count, err := parseIntRange(args[1], -math.MaxInt64, math.MaxInt64)
	if err != nil {
		return err
	}
	if count < -maxRepeatSample {
		return ErrSampleTooLarge
	}
	var out []string
	err = ctx.Store.View(key, func(v store.Value) error {
		ss, err := setOf(v)
		if err != nil || ss == nil || count == 0 {
			return err
		}
		if count > 0 {
			out = ss.Sample(int(min(count, int64(ss.Len()))), ctx.intN)
		} else {
			out = ss.SampleRepeat(int(-count), ctx.intN)
		}
		return nil
	})
	if err != nil {
		return err
	}
	writeStrings(ctx, out)
	return nil
}

// cmdSMove implements SMOVE source destination member atomically across both
// keys. As in Redis a missing source replies 0 before the destination's type
// is looked at, a wrong type on either key is an error, and moving a member
// within one key just reports whether it is there. Everything is validated
// before anything is written.
func cmdSMove(ctx *Context, args [][]byte) error {
	member := string(args[2])
	var moved bool
	err := ctx.Store.Atomic([]string{string(args[0]), string(args[1])}, func(es []*store.Entry) error {
		src, dst := es[0], es[1]
		if !src.Exists() {
			return nil
		}
		srcSet, err := setOf(src.Value())
		if err != nil {
			return err
		}
		dstSet, err := setOf(dst.Value())
		if err != nil {
			return err
		}
		if src == dst { // repeated keys share one Entry
			moved = srcSet.Has(member)
			return nil
		}
		if !srcSet.Has(member) {
			return nil
		}
		srcSet.Remove(member)
		if dstSet == nil {
			dstSet = set.New()
		}
		dstSet.Add(member)
		src.Put(srcSet) // deletes the key, TTL included, if it was the last member
		dst.Put(dstSet) // keeps the destination's TTL
		moved = true
		return nil
	})
	if err != nil {
		return err
	}
	writeBool(ctx, moved)
	return nil
}

// --- SUNION, SINTER, SDIFF and the *STORE forms ---

type setOp uint8

const (
	opUnion setOp = iota
	opInter
	opDiff
)

// setsOf type-checks every value and returns the sets, with nil for missing
// keys (which behave as empty sets).
func setsOf(vals []store.Value) ([]*set.Set, error) {
	out := make([]*set.Set, len(vals))
	for i, v := range vals {
		ss, err := setOf(v)
		if err != nil {
			return nil, err
		}
		out[i] = ss
	}
	return out, nil
}

func unionOf(vals []store.Value) ([]string, error) {
	sets, err := setsOf(vals)
	if err != nil {
		return nil, err
	}
	total := 0
	for _, ss := range sets {
		if ss != nil {
			total += ss.Len()
		}
	}
	seen := make(map[string]struct{}, total)
	out := make([]string, 0, total)
	for _, ss := range sets {
		if ss == nil {
			continue
		}
		ss.Range(func(m string) bool {
			if _, dup := seen[m]; !dup {
				seen[m] = struct{}{}
				out = append(out, m)
			}
			return true
		})
	}
	return out, nil
}

// interOf walks the keys in order, like Redis: a missing key ends the
// computation with an empty result before later keys are even type-checked.
// Otherwise it iterates the smallest set and probes the others, so the cost is
// O(smallest * number of sets).
func interOf(vals []store.Value) ([]string, error) {
	sets := make([]*set.Set, 0, len(vals))
	for _, v := range vals {
		ss, err := setOf(v)
		if err != nil {
			return nil, err
		}
		if ss == nil {
			return nil, nil
		}
		sets = append(sets, ss)
	}
	slices.SortFunc(sets, func(a, b *set.Set) int { return cmp.Compare(a.Len(), b.Len()) })
	var out []string
	sets[0].Range(func(m string) bool {
		for _, other := range sets[1:] {
			if !other.Has(m) {
				return true
			}
		}
		out = append(out, m)
		return true
	})
	return out, nil
}

// diffOf returns the members of the first set that are in none of the others.
// Every key is type-checked, even when the first one is missing.
func diffOf(vals []store.Value) ([]string, error) {
	sets, err := setsOf(vals)
	if err != nil {
		return nil, err
	}
	if sets[0] == nil {
		return nil, nil
	}
	var out []string
	sets[0].Range(func(m string) bool {
		for _, other := range sets[1:] {
			if other != nil && other.Has(m) {
				return true
			}
		}
		out = append(out, m)
		return true
	})
	return out, nil
}

func setOpResult(op setOp, vals []store.Value) ([]string, error) {
	switch op {
	case opUnion:
		return unionOf(vals)
	case opInter:
		return interOf(vals)
	default:
		return diffOf(vals)
	}
}

// setOpReply implements SUNION, SINTER and SDIFF: all keys are read-locked
// together (ascending shard order), so the result is a consistent snapshot.
func setOpReply(ctx *Context, args [][]byte, op setOp) error {
	var res []string
	err := ctx.Store.AtomicView(keysOf(args), func(vals []store.Value) (err error) {
		res, err = setOpResult(op, vals)
		return err
	})
	if err != nil {
		return err
	}
	writeStrings(ctx, res)
	return nil
}

// setOpStore implements the *STORE forms. args[0] is the destination, whatever
// it holds: the result replaces it, type and TTL included, and an empty result
// deletes it. The sources are validated and the result computed before the
// destination is touched; the destination may also be one of the sources.
func setOpStore(ctx *Context, args [][]byte, op setOp) error {
	var n int
	err := ctx.Store.Atomic(keysOf(args), func(es []*store.Entry) error {
		vals := make([]store.Value, len(es)-1)
		for i, e := range es[1:] {
			vals[i] = e.Value()
		}
		res, err := setOpResult(op, vals)
		if err != nil {
			return err
		}
		n = len(res)
		dst := es[0]
		if n == 0 {
			dst.Delete()
			return nil
		}
		out := set.New()
		for _, m := range res {
			out.Add(m)
		}
		dst.Put(out)
		dst.Persist()
		return nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(int64(n))
	return nil
}

func cmdSUnion(ctx *Context, args [][]byte) error { return setOpReply(ctx, args, opUnion) }
func cmdSInter(ctx *Context, args [][]byte) error { return setOpReply(ctx, args, opInter) }
func cmdSDiff(ctx *Context, args [][]byte) error  { return setOpReply(ctx, args, opDiff) }

func cmdSUnionStore(ctx *Context, args [][]byte) error { return setOpStore(ctx, args, opUnion) }
func cmdSInterStore(ctx *Context, args [][]byte) error { return setOpStore(ctx, args, opInter) }
func cmdSDiffStore(ctx *Context, args [][]byte) error  { return setOpStore(ctx, args, opDiff) }
