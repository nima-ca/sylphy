package main

import (
	"io"
	"strings"
	"testing"
)

// Invalid sweeper settings must stop startup with exit code 2 and name the
// offending setting, before any store or listener exists.
func TestRunRejectsInvalidExpireSettings(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"tick flag", []string{"-expire-tick-interval", "0s"}, nil, "expire-tick-interval"},
		{"sample flag", []string{"-expire-sample-size", "0"}, nil, "expire-sample-size"},
		{"budget flag", []string{"-expire-cycle-budget", "-1s"}, nil, "expire-cycle-budget"},
		{"threshold flag", []string{"-expire-stale-threshold", "1"}, nil, "expire-stale-threshold"},
		{"threshold env", nil, map[string]string{"SYLPHY_EXPIRE_STALE_THRESHOLD": "7"}, "expire-stale-threshold"},
		{"unparsable env", nil, map[string]string{"SYLPHY_EXPIRE_SAMPLE_SIZE": "lots"}, "SYLPHY_EXPIRE_SAMPLE_SIZE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr strings.Builder
			getenv := func(k string) string { return tt.env[k] }
			if code := run(tt.args, getenv, io.Discard, &stderr); code != 2 {
				t.Fatalf("exit code = %d, want 2; stderr: %s", code, stderr.String())
			}
			if !strings.Contains(stderr.String(), tt.want) {
				t.Fatalf("stderr %q does not mention %q", stderr.String(), tt.want)
			}
		})
	}
}

func TestRunHelpAndVersionExitZero(t *testing.T) {
	var out strings.Builder
	if code := run([]string{"-version"}, func(string) string { return "" }, &out, io.Discard); code != 0 || !strings.Contains(out.String(), "sylphy") {
		t.Fatalf("version: code=%d out=%q", code, out.String())
	}
	out.Reset()
	if code := run([]string{"-h"}, func(string) string { return "" }, io.Discard, &out); code != 0 || !strings.Contains(out.String(), "-expire-sample-size") {
		t.Fatalf("help: code=%d out=%q", code, out.String())
	}
}
