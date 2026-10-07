package store

import (
	"strings"
	"testing"
	"time"
)

func TestMatch(t *testing.T) {
	tests := []struct {
		pattern, s string
		want       bool
	}{
		{"*", "anything", true},
		{"*", "", true},
		{"", "", true},
		{"", "a", false},
		{"hello", "hello", true},
		{"hello", "hell", false},
		{"h?llo", "hello", true},
		{"h?llo", "hllo", false},
		{"h*llo", "heeeello", true},
		{"h*llo", "hllo", true},
		{"h[ae]llo", "hallo", true},
		{"h[ae]llo", "hillo", false},
		{"h[^e]llo", "hallo", true},
		{"h[^e]llo", "hello", false},
		{"h[a-b]llo", "hbllo", true},
		{"h[a-b]llo", "hcllo", false},
		{"h[b-a]llo", "hallo", true},
		{`a\*b`, "a*b", true},
		{`a\*b`, "axb", false},
		{`a\\b`, `a\b`, true},
		{`trailing\`, `trailing\`, true},
		{"*a*b*c", "xaxbxc", true},
		{"*a*b*c", "xaxcxb", false},
		{"[", "a", false},
		{"a[bc", "ab", true},
		{"user:*:name", "user:42:name", true},
		{"user:*:name", "user:42:nam", false},
		{`[\]]`, "]", true},
		{"[^a-c]x", "dx", true},
		{"[^a-c]x", "bx", false},
	}
	for _, tt := range tests {
		if got := Match(tt.pattern, tt.s); got != tt.want {
			t.Errorf("Match(%q, %q) = %v, want %v", tt.pattern, tt.s, got, tt.want)
		}
	}
}

func TestMatchPathologicalIsFast(t *testing.T) {
	start := time.Now()
	if Match(strings.Repeat("*a", 30)+"b", strings.Repeat("a", 2000)) {
		t.Fatal("unexpected match")
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("matcher too slow: %v", d)
	}
}
