package command

import "testing"

func TestAddLongDoubleMatchesRedisFormatting(t *testing.T) {
	tests := []struct{ a, b, want string }{
		{"10.5", "0.1", "10.6"}, // float64 would print 10.59999999999999964
		{"10.6", "-5", "5.6"},
		{"5.0e3", "2.0e2", "5200"},
		{"0", "0.1", "0.1"},
		{"0.1", "0.2", "0.3"},
		{"1", "2", "3"},
		{"1", "-1", "0"},
		{"-0", "-0", "0"},
		{"100", "-0.5", "99.5"},
		{"1e2", "1", "101"},
		{"-1.5", "0.5", "-1"},
		{"3", "1e-17", "3.00000000000000001"}, // beyond float64 precision
		{"0.00000000000000001", "0", "0.00000000000000001"},
		{"1e-18", "0", "0"},  // rounds to 17 digits: all zeros
		{"-1e-18", "0", "0"}, // "-0" is written as "0"
		{"+7", "1", "8"},
	}
	for _, tt := range tests {
		a, ok := parseLongDouble(tt.a)
		if !ok {
			t.Fatalf("parseLongDouble(%q) failed", tt.a)
		}
		b, ok := parseLongDouble(tt.b)
		if !ok {
			t.Fatalf("parseLongDouble(%q) failed", tt.b)
		}
		got, err := addLongDouble(a, b)
		if err != nil || got != tt.want {
			t.Errorf("%s + %s = %q,%v; want %q", tt.a, tt.b, got, err, tt.want)
		}
	}
}

func TestParseLongDoubleRejects(t *testing.T) {
	for _, s := range []string{
		"", " 1", "1 ", "abc", "nan", "NaN", "-nan", "1_0", "--1", "+-1", "1e", "e5", "0x", ".", "-", "+",
		"1e5000",  // above the long double range
		"1e-5000", // underflows to zero in strtold
		"1,5",
	} {
		if _, ok := parseLongDouble(s); ok {
			t.Errorf("parseLongDouble(%q) succeeded", s)
		}
	}
}

func TestParseLongDoubleAccepts(t *testing.T) {
	for _, s := range []string{"0", "-0", "1", "-1.5", "+2", "1e10", "1E-3", "5.0e3", "007", "1e4900"} {
		if _, ok := parseLongDouble(s); !ok {
			t.Errorf("parseLongDouble(%q) failed", s)
		}
	}
	for _, s := range []string{"inf", "-inf", "+INF", "Infinity", "-infinity"} {
		f, ok := parseLongDouble(s)
		if !ok || !f.IsInf() {
			t.Errorf("parseLongDouble(%q) = %v,%v; want an infinity", s, f, ok)
		}
	}
}

func TestAddLongDoubleRejectsInfAndOverflow(t *testing.T) {
	one, _ := parseLongDouble("1")
	inf, _ := parseLongDouble("inf")
	ninf, _ := parseLongDouble("-inf")
	huge, _ := parseLongDouble("1e4932") // within range, but doubling it is not
	if _, err := addLongDouble(one, inf); err != ErrNaNOrInf {
		t.Errorf("1 + inf: err = %v", err)
	}
	if _, err := addLongDouble(inf, ninf); err != ErrNaNOrInf { // would be NaN
		t.Errorf("inf + -inf: err = %v", err)
	}
	if _, err := addLongDouble(huge, huge); err != ErrNaNOrInf {
		t.Errorf("1e4932 + 1e4932: err = %v", err)
	}
}
