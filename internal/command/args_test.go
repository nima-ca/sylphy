package command

import (
	"errors"
	"math"
	"testing"
)

func TestParseInt(t *testing.T) {
	if n, err := parseInt([]byte("-42")); err != nil || n != -42 {
		t.Fatalf("got %d, %v", n, err)
	}
	for _, in := range []string{"", "1.5", "+1", "01", " 1", "abc", "9223372036854775808"} {
		if _, err := parseInt([]byte(in)); !errors.Is(err, ErrNotInteger) {
			t.Errorf("parseInt(%q) err = %v, want ErrNotInteger", in, err)
		}
	}
}

func TestParseIntRange(t *testing.T) {
	tests := []struct {
		in      string
		want    int64
		wantErr string
	}{
		{"5", 5, ""}, {"0", 0, ""}, {"10", 10, ""},
		{"11", 0, "ERR value is out of range, must be between 0 and 10"},
		{"-1", 0, "ERR value is out of range, must be between 0 and 10"},
		{"x", 0, ErrNotInteger.Msg},
	}
	for _, tt := range tests {
		got, err := parseIntRange([]byte(tt.in), 0, 10)
		switch {
		case tt.wantErr == "" && (err != nil || got != tt.want):
			t.Errorf("parseIntRange(%q) = %d, %v", tt.in, got, err)
		case tt.wantErr != "" && (err == nil || err.Error() != tt.wantErr):
			t.Errorf("parseIntRange(%q) err = %v, want %q", tt.in, err, tt.wantErr)
		}
	}
}

func TestParsePositive(t *testing.T) {
	if n, err := parsePositive([]byte("1")); err != nil || n != 1 {
		t.Fatalf("got %d, %v", n, err)
	}
	for _, in := range []string{"0", "-3"} {
		if _, err := parsePositive([]byte(in)); !errors.Is(err, ErrMustBePositive) {
			t.Errorf("parsePositive(%q) err = %v", in, err)
		}
	}
	if _, err := parsePositive([]byte("x")); !errors.Is(err, ErrNotInteger) {
		t.Errorf("non-integer err = %v", err)
	}
}

func TestParseFloat(t *testing.T) {
	ok := []struct {
		in   string
		want float64
	}{
		{"0", 0}, {"-0", 0}, {"1.5", 1.5}, {"-2", -2}, {"1e3", 1000}, {"0e5", 0}, {"0.000", 0},
		{"inf", math.Inf(1)}, {"+inf", math.Inf(1)}, {"-inf", math.Inf(-1)}, {"Infinity", math.Inf(1)},
		{"0x1p-2", 0.25}, {"0x0p5", 0},
	}
	for _, tt := range ok {
		got, err := parseFloat([]byte(tt.in))
		if err != nil || got != tt.want {
			t.Errorf("parseFloat(%q) = %v, %v; want %v", tt.in, got, err, tt.want)
		}
	}
	bad := []string{
		"", " 1", "1 ", "abc", "nan", "NaN", "-nan", "1e999", "-1e999",
		"1e-999", "0x1p-2000", "1_0", "0x", "--1",
	}
	for _, in := range bad {
		if got, err := parseFloat([]byte(in)); !errors.Is(err, ErrNotFloat) {
			t.Errorf("parseFloat(%q) = %v, %v; want ErrNotFloat", in, got, err)
		}
	}
}

func TestNormalizeRange(t *testing.T) {
	tests := []struct {
		name         string
		start, stop  int64
		n            int
		lo, hi       int
		wantNonEmpty bool
	}{
		{"all", 0, -1, 5, 0, 5, true},
		{"middle", 1, 3, 5, 1, 4, true},
		{"tail", -2, -1, 5, 3, 5, true},
		{"clamped", -100, 100, 5, 0, 5, true},
		{"single", 0, 0, 5, 0, 1, true},
		{"inverted", 3, 1, 5, 0, 0, false},
		{"start past end", 5, 10, 5, 0, 0, false},
		{"both before start", -100, -50, 5, 0, 0, false},
		{"empty collection", 0, -1, 0, 0, 0, false},
		{"extreme bounds", math.MinInt64, math.MaxInt64, 5, 0, 5, true},
		{"extreme start", math.MaxInt64, math.MaxInt64, 5, 0, 0, false},
		{"stop negative past start", 0, -6, 5, 0, 0, false},
	}
	for _, tt := range tests {
		lo, hi, ok := normalizeRange(tt.start, tt.stop, tt.n)
		if ok != tt.wantNonEmpty || (ok && (lo != tt.lo || hi != tt.hi)) {
			t.Errorf("%s: normalizeRange(%d,%d,%d) = %d,%d,%v; want %d,%d,%v",
				tt.name, tt.start, tt.stop, tt.n, lo, hi, ok, tt.lo, tt.hi, tt.wantNonEmpty)
		}
	}
}

func TestOptScanner(t *testing.T) {
	sc := newOptScanner([][]byte{[]byte("ex"), []byte("10"), []byte("NX")})
	if !sc.more() || sc.next() != "EX" {
		t.Fatal("first token must be upper-cased EX")
	}
	if v, ok := sc.value(); !ok || string(v) != "10" {
		t.Fatalf("value = %q,%v", v, ok)
	}
	if sc.next() != "NX" || sc.more() {
		t.Fatal("expected NX then end")
	}
	if _, ok := sc.value(); ok {
		t.Fatal("value past the end must report false")
	}
}

func TestExpireDeadline(t *testing.T) {
	tests := []struct {
		name     string
		v        int64
		unit     timeUnit
		absolute bool
		now      int64
		want     int64
		ok       bool
	}{
		{"relative seconds", 10, unitSeconds, false, 1000, 11000, true},
		{"relative millis", 250, unitMillis, false, 1000, 1250, true},
		{"absolute seconds", 5, unitSeconds, true, 999999, 5000, true},
		{"absolute millis ignores now", 7, unitMillis, true, 999999, 7, true},
		{"negative relative", -5, unitSeconds, false, 10000, 5000, true},
		{"seconds overflow", math.MaxInt64/1000 + 1, unitSeconds, true, 0, 0, false},
		{"seconds underflow", math.MinInt64/1000 - 1, unitSeconds, true, 0, 0, false},
		{"largest seconds", math.MaxInt64 / 1000, unitSeconds, true, 0, (math.MaxInt64 / 1000) * 1000, true},
		{"relative add overflow", math.MaxInt64, unitMillis, false, 1, 0, false},
		{"relative add exact fit", math.MaxInt64 - 1, unitMillis, false, 1, math.MaxInt64, true},
		{"absolute max millis", math.MaxInt64, unitMillis, true, 12345, math.MaxInt64, true},
	}
	for _, tt := range tests {
		got, ok := expireDeadline(tt.v, tt.unit, tt.absolute, tt.now)
		if ok != tt.ok || (ok && got != tt.want) {
			t.Errorf("%s: got %d,%v; want %d,%v", tt.name, got, ok, tt.want, tt.ok)
		}
	}
}

func TestParseExpireValue(t *testing.T) {
	ex := expireOpts["EX"]
	if at, err := parseExpireValue("set", []byte("5"), ex, 1000); err != nil || at != 6000 {
		t.Fatalf("got %d, %v", at, err)
	}
	for _, in := range []string{"0", "-1"} {
		_, err := parseExpireValue("set", []byte(in), ex, 1000)
		if err == nil || err.Error() != "ERR invalid expire time in 'set' command" {
			t.Errorf("%q: err = %v", in, err)
		}
	}
	if _, err := parseExpireValue("set", []byte("x"), ex, 1000); !errors.Is(err, ErrNotInteger) {
		t.Errorf("err = %v, want ErrNotInteger", err)
	}
	if _, err := parseExpireValue("getex", []byte("9223372036854775807"), ex, 1000); err == nil ||
		err.Error() != "ERR invalid expire time in 'getex' command" {
		t.Errorf("overflow err = %v", err)
	}
}

func TestRoundedSeconds(t *testing.T) {
	tests := []struct{ in, want int64 }{
		{0, 0}, {499, 0}, {500, 1}, {1499, 1}, {1500, 2}, {1_700_000_050_500, 1_700_000_051},
		{math.MaxInt64, 9223372036854776}, // (ms+500)/1000 would overflow here
	}
	for _, tt := range tests {
		if got := roundedSeconds(tt.in); got != tt.want {
			t.Errorf("roundedSeconds(%d) = %d, want %d", tt.in, got, tt.want)
		}
	}
}
