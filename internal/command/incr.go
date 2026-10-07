package command

import (
	"math"
	"strconv"
)

// parseInt64Strict parses a signed 64-bit decimal exactly like Redis: no
// whitespace, no '+', no leading zeros (except "0" itself), no "-0".
func parseInt64Strict(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	if len(b) == 1 && b[0] == '0' {
		return 0, true
	}
	i, neg := 0, false
	if b[0] == '-' {
		neg, i = true, 1
	}
	if i >= len(b) || b[i] < '1' || b[i] > '9' {
		return 0, false
	}
	var n uint64
	for ; i < len(b); i++ {
		d := b[i] - '0'
		if d > 9 || n > (math.MaxUint64-uint64(d))/10 {
			return 0, false
		}
		n = n*10 + uint64(d)
	}
	if neg {
		if n > 1<<63 {
			return 0, false
		}
		return -int64(n), true
	}
	if n > math.MaxInt64 {
		return 0, false
	}
	return int64(n), true
}

// incrBy atomically adds delta to the integer stored at key (missing = 0).
func incrBy(ctx *Context, key string, delta int64) error {
	var result int64
	err := ctx.Store.Update(key, func(old []byte, exists bool) ([]byte, error) {
		var cur int64
		if exists {
			v, ok := parseInt64Strict(old)
			if !ok {
				return nil, ErrNotInteger
			}
			cur = v
		}
		if (delta > 0 && cur > math.MaxInt64-delta) || (delta < 0 && cur < math.MinInt64-delta) {
			return nil, ErrOverflow
		}
		result = cur + delta
		return strconv.AppendInt(nil, result, 10), nil
	})
	if err != nil {
		return err
	}
	ctx.W.WriteInteger(result)
	return nil
}

func cmdIncr(ctx *Context, args [][]byte) error { return incrBy(ctx, string(args[0]), 1) }
func cmdDecr(ctx *Context, args [][]byte) error { return incrBy(ctx, string(args[0]), -1) }

func cmdIncrBy(ctx *Context, args [][]byte) error {
	d, ok := parseInt64Strict(args[1])
	if !ok {
		return ErrNotInteger
	}
	return incrBy(ctx, string(args[0]), d)
}

func cmdDecrBy(ctx *Context, args [][]byte) error {
	d, ok := parseInt64Strict(args[1])
	if !ok {
		return ErrNotInteger
	}
	if d == math.MinInt64 { // -MinInt64 is not representable
		return ErrOverflow
	}
	return incrBy(ctx, string(args[0]), -d)
}
