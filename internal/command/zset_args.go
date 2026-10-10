package command

import (
	"math"
	"strconv"
	"strings"

	"github.com/nima-ca/sylphy/internal/zset"
)

// Redis error replies specific to sorted sets.
var (
	errMinMaxNotFloat  = &ReplyError{Msg: "ERR min or max is not a float"}
	errNaNScore        = &ReplyError{Msg: "ERR resulting score is not a number (NaN)"}
	errZAddNXXX        = &ReplyError{Msg: "ERR XX and NX options at the same time are not compatible"}
	errZAddGTLTNX      = &ReplyError{Msg: "ERR GT, LT, and/or NX options at the same time are not compatible"}
	errZAddIncr        = &ReplyError{Msg: "ERR INCR option supports a single increment-element pair"}
	errLimitNeedsScore = &ReplyError{
		Msg: "ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX",
	}
)

// formatScore renders a score the way ZSCORE replies do: the shortest decimal
// that round-trips, "inf" / "-inf" for infinities, and exponent notation only
// outside [1e-6, 1e21). The exponent carries no leading zeros ("1e-7", not
// "1e-07"), as in Redis.
func formatScore(f float64) string {
	if math.IsInf(f, 0) {
		if f > 0 {
			return "inf"
		}
		return "-inf"
	}
	if a := math.Abs(f); a != 0 && (a < 1e-6 || a >= 1e21) {
		mant, exp, _ := strings.Cut(strconv.FormatFloat(f, 'e', -1, 64), "e")
		digits := strings.TrimLeft(exp[1:], "0") // exp is "+21" or "-07"
		if digits == "" {
			digits = "0"
		}
		return mant + "e" + exp[:1] + digits
	}
	return strconv.FormatFloat(f, 'f', -1, 64)
}

// clampInt converts a Redis long to int without wrapping on 32-bit platforms.
func clampInt(v int64) int { return int(min(max(v, math.MinInt), math.MaxInt)) }

// parseScoreBound parses one end of a score interval: a float, optionally
// prefixed with '(' to make it exclusive. Every failure, including NaN, is
// "min or max is not a float".
func parseScoreBound(b []byte) (v float64, exclusive bool, err error) {
	if len(b) > 0 && b[0] == '(' {
		exclusive, b = true, b[1:]
	}
	f, perr := parseFloat(b)
	if perr != nil {
		return 0, false, errMinMaxNotFloat
	}
	return f, exclusive, nil
}

// parseScoreRange parses a (min, max) pair of bound arguments.
func parseScoreRange(lo, hi []byte) (zset.ScoreRange, error) {
	var r zset.ScoreRange
	var err error
	if r.Min, r.MinExclusive, err = parseScoreBound(lo); err != nil {
		return r, err
	}
	if r.Max, r.MaxExclusive, err = parseScoreBound(hi); err != nil {
		return r, err
	}
	return r, nil
}

// zaddOptions are the flags of ZADD.
type zaddOptions struct{ nx, xx, gt, lt, ch, incr bool }

// parseZAddArgs splits ZADD's arguments (key first) into options and the
// trailing score/member pairs. Checks run in Redis's order: pair-count syntax
// first, then option conflicts.
func parseZAddArgs(args [][]byte) (zaddOptions, [][]byte, error) {
	var o zaddOptions
	i := 1
opts:
	for ; i < len(args); i++ {
		switch strings.ToUpper(string(args[i])) {
		case "NX":
			o.nx = true
		case "XX":
			o.xx = true
		case "GT":
			o.gt = true
		case "LT":
			o.lt = true
		case "CH":
			o.ch = true
		case "INCR":
			o.incr = true
		default:
			break opts
		}
	}
	pairs := args[i:]
	if len(pairs) == 0 || len(pairs)%2 != 0 {
		return o, nil, ErrSyntax
	}
	switch {
	case o.nx && o.xx:
		return o, nil, errZAddNXXX
	case (o.gt && o.nx) || (o.lt && o.nx) || (o.gt && o.lt):
		return o, nil, errZAddGTLTNX
	case o.incr && len(pairs) > 2:
		return o, nil, errZAddIncr
	}
	return o, pairs, nil
}

// rangeOpt is a bit set of the optional tokens a range command accepts.
type rangeOpt uint8

const (
	rngWithScores rangeOpt = 1 << iota
	rngLimit
	rngRev
	rngByScore
)

// rangeReq is a parsed ZRANGE-family request.
type rangeReq struct {
	byScore, rev, withScores, hasLimit bool
	offset, count                      int64 // LIMIT; count < 0 means "all"
	start, stop                        int64 // rank range (by-rank mode)
	scores                             zset.ScoreRange
}

// parseZRange parses the arguments of ZRANGE, ZREVRANGE, ZRANGEBYSCORE and
// ZREVRANGEBYSCORE (key, then two range arguments, then options). preset
// carries what the command name implies (rev, byScore); allowed lists the
// option tokens the command accepts. Options are read first, then the range,
// as in Redis. With rev in score mode the first argument is the maximum.
func parseZRange(args [][]byte, allowed rangeOpt, preset rangeReq) (rangeReq, error) {
	r := preset
	r.count = -1
	sc := newOptScanner(args[3:])
	for sc.more() {
		switch tok := sc.next(); {
		case tok == "WITHSCORES" && allowed&rngWithScores != 0:
			r.withScores = true
		case tok == "REV" && allowed&rngRev != 0:
			r.rev = true
		case tok == "BYSCORE" && allowed&rngByScore != 0:
			r.byScore = true
		case tok == "LIMIT" && allowed&rngLimit != 0:
			off, _ := sc.value()
			cnt, ok := sc.value()
			if !ok {
				return r, ErrSyntax
			}
			var err error
			if r.offset, err = parseInt(off); err != nil {
				return r, err
			}
			if r.count, err = parseInt(cnt); err != nil {
				return r, err
			}
			r.hasLimit = true
		default:
			return r, ErrSyntax
		}
	}
	if r.hasLimit && !r.byScore {
		return r, errLimitNeedsScore
	}
	if r.byScore {
		lo, hi := args[1], args[2]
		if r.rev {
			lo, hi = hi, lo
		}
		sr, err := parseScoreRange(lo, hi)
		if err != nil {
			return r, err
		}
		r.scores = sr
		return r, nil
	}
	var err error
	if r.start, err = parseInt(args[1]); err != nil {
		return r, err
	}
	if r.stop, err = parseInt(args[2]); err != nil {
		return r, err
	}
	return r, nil
}
