package command

import (
	"math/big"
	"strings"
)

// Redis computes HINCRBYFLOAT with C long double, which on x86-64 is the 80-bit
// x87 format: a 64-bit mantissa and a 15-bit exponent. To print the same
// digits Redis does ("10.6", not 10.59999999999999964), this file does the
// arithmetic in math/big at 64 bits of precision with round-half-even, which
// is exactly what one x87 addition does, and formats like Redis's ld2string:
// "%.17Lf" with trailing zeros removed.
//
// Known deviations: hexadecimal floats ("0x1p3") and subnormal long doubles
// are rejected, where strtold would accept them.
const (
	ldPrec             = 64       // mantissa bits of an x87 long double
	ldMaxExp           = 16384    // |x| < 2^16384, the long double range
	ldMinExp           = -16444   // smallest subnormal is 2^-16445
	maxLongDoubleChars = 5 * 1024 // Redis's MAX_LONG_DOUBLE_CHARS
	ldFracDigits       = 17       // digits after the point, as in "%.17Lf"
)

// newLongDouble returns +0 with long double precision.
func newLongDouble() *big.Float {
	return new(big.Float).SetPrec(ldPrec).SetMode(big.ToNearestEven)
}

// parseLongDouble parses s like Redis's string2ld: no surrounding whitespace,
// "inf"/"infinity" accepted (with an optional sign), "nan" rejected, and
// values outside the long double range rejected (strtold's ERANGE). The
// result has long double precision.
func parseLongDouble(s string) (*big.Float, bool) {
	if s == "" || len(s) >= maxLongDoubleChars {
		return nil, false
	}
	neg, body := false, s
	switch s[0] {
	case '+':
		body = s[1:]
	case '-':
		neg, body = true, s[1:]
	}
	switch strings.ToLower(body) {
	case "inf", "infinity":
		return newLongDouble().SetInf(neg), true
	}
	// Anything that does not start like a number (nan, spaces, a second sign,
	// letters) is not a float.
	if body == "" || (body[0] != '.' && (body[0] < '0' || body[0] > '9')) {
		return nil, false
	}
	f, _, err := newLongDouble().Parse(s, 10)
	if err != nil || f.IsInf() {
		return nil, false
	}
	if f.Sign() != 0 {
		if e := f.MantExp(nil); e > ldMaxExp || e < ldMinExp {
			return nil, false
		}
	}
	return f, true
}

// addLongDouble returns a+b formatted for storage and for the HINCRBYFLOAT
// reply. It fails with ErrNaNOrInf when either operand is infinite or the sum
// leaves the long double range.
func addLongDouble(a, b *big.Float) (string, error) {
	if a.IsInf() || b.IsInf() { // also keeps big from panicking on Inf + -Inf
		return "", ErrNaNOrInf
	}
	sum := newLongDouble().Add(a, b)
	if sum.Sign() != 0 && sum.MantExp(nil) > ldMaxExp {
		return "", ErrNaNOrInf
	}
	return formatLongDouble(sum), nil
}

// formatLongDouble renders f as ld2string does in LD_STR_HUMAN mode: fixed
// notation with 17 fractional digits, trailing zeros (and a bare point)
// removed, and "-0" written as "0".
func formatLongDouble(f *big.Float) string {
	s := f.Text('f', ldFracDigits)
	if strings.IndexByte(s, '.') >= 0 {
		s = strings.TrimRight(s, "0")
		s = strings.TrimSuffix(s, ".")
	}
	if s == "-0" {
		s = "0"
	}
	return s
}
