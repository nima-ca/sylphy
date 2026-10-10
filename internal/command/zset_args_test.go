package command

import (
	"math"
	"testing"
)

func TestFormatScore(t *testing.T) {
	tests := []struct {
		in   float64
		want string
	}{
		{0, "0"}, {math.Copysign(0, -1), "-0"}, {1, "1"}, {-1.5, "-1.5"}, {0.1, "0.1"},
		{3.14159, "3.14159"}, {123456789012, "123456789012"}, {1e20, "100000000000000000000"},
		{1e21, "1e+21"}, {-1e21, "-1e+21"}, {1.5e30, "1.5e+30"},
		{1e-6, "0.000001"}, {1e-7, "1e-7"}, {1.5e-10, "1.5e-10"}, {-2e-300, "-2e-300"},
		{math.Inf(1), "inf"}, {math.Inf(-1), "-inf"},
	}
	for _, tt := range tests {
		if got := formatScore(tt.in); got != tt.want {
			t.Errorf("formatScore(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestParseScoreBound(t *testing.T) {
	tests := []struct {
		in   string
		v    float64
		excl bool
		ok   bool
	}{
		{"1", 1, false, true}, {"(1", 1, true, true}, {"-2.5", -2.5, false, true},
		{"inf", math.Inf(1), false, true}, {"+inf", math.Inf(1), false, true},
		{"-inf", math.Inf(-1), false, true}, {"(inf", math.Inf(1), true, true},
		{"", 0, false, false}, {"(", 0, false, false}, {"abc", 0, false, false},
		{"nan", 0, false, false}, {"(nan", 0, false, false}, {"((1", 0, false, false},
		{" 1", 0, false, false},
	}
	for _, tt := range tests {
		v, excl, err := parseScoreBound([]byte(tt.in))
		if (err == nil) != tt.ok {
			t.Errorf("parseScoreBound(%q) err = %v, want ok=%v", tt.in, err, tt.ok)
			continue
		}
		if tt.ok && (v != tt.v || excl != tt.excl) {
			t.Errorf("parseScoreBound(%q) = (%v, %v), want (%v, %v)", tt.in, v, excl, tt.v, tt.excl)
		}
		if !tt.ok && err != errMinMaxNotFloat {
			t.Errorf("parseScoreBound(%q) err = %v, want errMinMaxNotFloat", tt.in, err)
		}
	}
}

func TestParseScoreRangeReportsEitherSide(t *testing.T) {
	if _, err := parseScoreRange([]byte("1"), []byte("x")); err != errMinMaxNotFloat {
		t.Errorf("bad max: %v", err)
	}
	if _, err := parseScoreRange([]byte("x"), []byte("1")); err != errMinMaxNotFloat {
		t.Errorf("bad min: %v", err)
	}
	r, err := parseScoreRange([]byte("(1"), []byte("5"))
	if err != nil || r.Min != 1 || !r.MinExclusive || r.Max != 5 || r.MaxExclusive {
		t.Errorf("got %+v, %v", r, err)
	}
}

func TestClampInt(t *testing.T) {
	if clampInt(5) != 5 || clampInt(-5) != -5 {
		t.Error("small values must pass through")
	}
	if clampInt(math.MaxInt64) != math.MaxInt || clampInt(math.MinInt64) != math.MinInt {
		t.Error("extremes must clamp to the int range")
	}
}
