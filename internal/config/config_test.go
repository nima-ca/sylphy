package config

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
	"time"
)

func env(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestLoadDefaults(t *testing.T) {
	cfg, err := Load(nil, env(nil), io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg != Default() {
		t.Fatalf("got %+v", cfg)
	}
}

func TestEnvThenFlagPrecedence(t *testing.T) {
	e := env(map[string]string{"SYLPHY_ADDR": "0.0.0.0:7000", "SYLPHY_SHARDS": "64", "SYLPHY_IDLE_TIMEOUT": "30s"})
	cfg, err := Load([]string{"-shards", "128"}, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Addr != "0.0.0.0:7000" || cfg.Shards != 128 || cfg.IdleTimeout != 30*time.Second {
		t.Fatalf("got %+v", cfg)
	}
}

func TestInvalid(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"shards not power of two", []string{"-shards", "3"}, nil, "power of two"},
		{"bad addr", []string{"-addr", "nope"}, nil, "host:port"},
		{"bad log level", []string{"-log-level", "loud"}, nil, "log-level"},
		{"bad env number", nil, map[string]string{"SYLPHY_SHARDS": "many"}, "SYLPHY_SHARDS"},
		{"unknown flag", []string{"-bogus"}, nil, "bogus"},
		{"stray arg", []string{"extra"}, nil, "unexpected"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(tt.args, env(tt.env), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
}

func TestHelp(t *testing.T) {
	var sb strings.Builder
	_, err := Load([]string{"-h"}, env(nil), &sb)
	if !errors.Is(err, flag.ErrHelp) || !strings.Contains(sb.String(), "-addr") {
		t.Fatalf("err=%v out=%q", err, sb.String())
	}
}
