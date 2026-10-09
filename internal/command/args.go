package command

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// Shared argument parsing. Every helper reports failures as the *ReplyError
// Redis would send, so handlers just return the error.

// parseInt parses a signed 64-bit integer argument exactly like Redis (see
// parseInt64Strict). Any failure is ErrNotInteger.
func parseInt(b []byte) (int64, error) {
	n, ok := parseInt64Strict(b)
	if !ok {
		return 0, ErrNotInteger
	}
	return n, nil
}

// parseIntRange is parseInt plus an inclusive bounds check.
func parseIntRange(b []byte, lo, hi int64) (int64, error) {
	n, err := parseInt(b)
	if err != nil {
		return 0, err
	}
	if n < lo || n > hi {
		return 0, &ReplyError{Msg: fmt.Sprintf("ERR value is out of range, must be between %d and %d", lo, hi)}
	}
	return n, nil
}

// parsePositive parses an integer that must be at least 1 (counts, sizes).
func parsePositive(b []byte) (int64, error) {
	n, err := parseInt(b)
	if err != nil {
		return 0, err
	}
	if n < 1 {
		return 0, ErrMustBePositive
	}
	return n, nil
}

// parseFloat parses a float argument like Redis: no surrounding whitespace, no
// NaN, and no literal that overflows or underflows to zero. "inf" and "-inf"
// are accepted (sorted-set scores use them); callers that cannot take an
// infinity check math.IsInf themselves.
func parseFloat(b []byte) (float64, error) {
	s := string(b)
	if strings.IndexByte(s, '_') >= 0 { // Go accepts digit separators; strtod does not
		return 0, ErrNotFloat
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(f) || underflowed(s, f) {
		return 0, ErrNotFloat
	}
	return f, nil
}

// underflowed reports a literal with a non-zero mantissa that parsed to zero:
// strtod flags that as ERANGE and Redis rejects it, Go silently returns 0.
func underflowed(s string, f float64) bool {
	if f != 0 {
		return false
	}
	t := strings.TrimLeft(s, "+-")
	hex := strings.HasPrefix(t, "0x") || strings.HasPrefix(t, "0X")
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		case !hex && (c == 'e' || c == 'E'), hex && (c == 'p' || c == 'P'):
			return false // exponent reached: the mantissa was all zeros
		case c >= '1' && c <= '9':
			return true
		case hex && (c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F'):
			return true
		}
	}
	return false
}

// normalizeRange resolves Redis start/stop indexes (both inclusive, negative
// counting from the end) against a collection of n elements. It returns the
// half-open Go range [lo, hi), or ok=false when the range is empty. This is the
// rule shared by LRANGE, LTRIM, ZRANGE by rank and friends.
func normalizeRange(start, stop int64, n int) (lo, hi int, ok bool) {
	l := int64(n)
	if start < 0 {
		start += l
		if start < 0 {
			start = 0
		}
	}
	if stop < 0 {
		stop += l
	}
	if start > stop || start >= l {
		return 0, 0, false
	}
	if stop >= l {
		stop = l - 1
	}
	return int(start), int(stop) + 1, true
}

// optScanner walks the trailing option tokens of a command. Tokens match
// case-insensitively; value arguments keep their bytes.
type optScanner struct {
	args [][]byte
	pos  int
}

func newOptScanner(args [][]byte) *optScanner { return &optScanner{args: args} }

// more reports whether tokens remain.
func (s *optScanner) more() bool { return s.pos < len(s.args) }

// next consumes the next token and returns it upper-cased. Call only when
// more() is true.
func (s *optScanner) next() string {
	t := strings.ToUpper(string(s.args[s.pos]))
	s.pos++
	return t
}

// value consumes the next argument as an option's value.
func (s *optScanner) value() ([]byte, bool) {
	if s.pos >= len(s.args) {
		return nil, false
	}
	v := s.args[s.pos]
	s.pos++
	return v, true
}

// timeUnit is the unit of an expiry argument.
type timeUnit uint8

const (
	unitSeconds timeUnit = iota
	unitMillis
)

// expireOpt describes one expiry option: its unit and whether its argument is
// an absolute Unix timestamp rather than a time to live.
type expireOpt struct {
	unit     timeUnit
	absolute bool
}

// expireOpts are the expiry options shared by SET and GETEX.
var expireOpts = map[string]expireOpt{
	"EX":   {unitSeconds, false},
	"PX":   {unitMillis, false},
	"EXAT": {unitSeconds, true},
	"PXAT": {unitMillis, true},
}

// expireDeadline converts the integer v of an expiry argument into an absolute
// Unix-millisecond deadline. Relative values are added to now. ok is false when
// the conversion would overflow int64.
func expireDeadline(v int64, unit timeUnit, absolute bool, now int64) (ms int64, ok bool) {
	if unit == unitSeconds {
		if v > math.MaxInt64/1000 || v < math.MinInt64/1000 {
			return 0, false
		}
		v *= 1000
	}
	if !absolute {
		if v > math.MaxInt64-now {
			return 0, false
		}
		v += now
	}
	return v, true
}

// parseExpireValue validates the argument of SET/GETEX/SETEX/PSETEX style
// options: an integer that must be positive and must not overflow. cmd names
// the command in the error text.
func parseExpireValue(cmd string, raw []byte, o expireOpt, now int64) (int64, error) {
	v, err := parseInt(raw)
	if err != nil {
		return 0, err
	}
	if v <= 0 {
		return 0, InvalidExpireTime(cmd)
	}
	at, ok := expireDeadline(v, o.unit, o.absolute, now)
	if !ok {
		return 0, InvalidExpireTime(cmd)
	}
	return at, nil
}

// roundedSeconds converts non-negative milliseconds to seconds, rounding half
// up like Redis's (ms+500)/1000 but without overflowing near MaxInt64.
func roundedSeconds(ms int64) int64 {
	q := ms / 1000
	if ms%1000 >= 500 {
		q++
	}
	return q
}
