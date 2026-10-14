package config

import (
	"errors"
	"flag"
	"io"
	"strings"
	"testing"
)

func TestPersistenceDefaults(t *testing.T) {
	d := Default()
	if d.PersistenceMode != "off" || d.PersistenceEnabled() {
		t.Fatalf("persistence must be off by default: %+v", d)
	}
	if d.Dir != "./data" || d.AppendFsync != "everysec" || d.AutoRewritePercentage != 100 ||
		d.AutoRewriteMinSize != 64<<20 || !d.AOFLoadTruncated || d.SnapshotOnShutdown != "auto" {
		t.Fatalf("defaults: %+v", d)
	}
	if d.SnapshotOnShutdownEnabled() {
		t.Fatal("a final checkpoint must not be on while persistence is off")
	}
	if err := d.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestPersistenceEnvThenFlagPrecedence(t *testing.T) {
	e := env(map[string]string{
		"SYLPHY_PERSISTENCE_MODE":        "aof",
		"SYLPHY_DIR":                     "/var/lib/sylphy",
		"SYLPHY_APPEND_FSYNC":            "always",
		"SYLPHY_AUTO_REWRITE_PERCENTAGE": "50",
		"SYLPHY_AUTO_REWRITE_MIN_SIZE":   "8mb",
		"SYLPHY_AOF_LOAD_TRUNCATED":      "false",
		"SYLPHY_SNAPSHOT_ON_SHUTDOWN":    "false",
	})
	cfg, err := Load(nil, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PersistenceMode != "aof" || cfg.Dir != "/var/lib/sylphy" || cfg.AppendFsync != "always" ||
		cfg.AutoRewritePercentage != 50 || cfg.AutoRewriteMinSize != 8<<20 || cfg.AOFLoadTruncated ||
		cfg.SnapshotOnShutdown != "false" {
		t.Fatalf("env not applied: %+v", cfg)
	}
	if !cfg.PersistenceEnabled() || cfg.SnapshotOnShutdownEnabled() {
		t.Fatal("derived settings")
	}

	// Flags win over the environment, field by field.
	cfg, err = Load([]string{
		"-persistence-mode", "snapshot", "-dir=/tmp/x", "-append-fsync", "no",
		"-auto-rewrite-percentage=0", "-auto-rewrite-min-size", "1g",
		"-aof-load-truncated=true", "-snapshot-on-shutdown", "auto",
	}, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.PersistenceMode != "snapshot" || cfg.Dir != "/tmp/x" || cfg.AppendFsync != "no" ||
		cfg.AutoRewritePercentage != 0 || cfg.AutoRewriteMinSize != 1<<30 || !cfg.AOFLoadTruncated ||
		cfg.SnapshotOnShutdown != "auto" || !cfg.SnapshotOnShutdownEnabled() {
		t.Fatalf("flags did not override env: %+v", cfg)
	}
}

func TestParseSize(t *testing.T) {
	good := map[string]int64{
		"0": 0, "123": 123, "64mb": 64 << 20, "64MB": 64 << 20, "64MiB": 64 << 20, "64m": 64 << 20,
		"1k": 1 << 10, "1KB": 1 << 10, "2kib": 2 << 10, "3g": 3 << 30, "1GiB": 1 << 30, " 5 mb ": 5 << 20, "10b": 10,
	}
	for in, want := range good {
		if got, err := ParseSize(in); err != nil || got != want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, in := range []string{"", "mb", "-1", "-5mb", "1.5g", "12x", "9223372036854775807k", "99999999999999999999"} {
		if _, err := ParseSize(in); err == nil {
			t.Errorf("ParseSize(%q) accepted", in)
		}
	}
}

func TestPersistenceInvalid(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"bad mode", []string{"-persistence-mode", "disk"}, nil, "persistence-mode"},
		{"bad fsync", []string{"-append-fsync", "sometimes"}, nil, "append-fsync"},
		{"negative percentage", []string{"-auto-rewrite-percentage", "-1"}, nil, "auto-rewrite-percentage"},
		{"bad size flag", []string{"-auto-rewrite-min-size", "lots"}, nil, "auto-rewrite-min-size"},
		{"bad size env", nil, map[string]string{"SYLPHY_AUTO_REWRITE_MIN_SIZE": "lots"}, "SYLPHY_AUTO_REWRITE_MIN_SIZE"},
		{"negative size env", nil, map[string]string{"SYLPHY_AUTO_REWRITE_MIN_SIZE": "-1"}, "SYLPHY_AUTO_REWRITE_MIN_SIZE"},
		{"bad bool env", nil, map[string]string{"SYLPHY_AOF_LOAD_TRUNCATED": "perhaps"}, "SYLPHY_AOF_LOAD_TRUNCATED"},
		{"bad snapshot-on-shutdown", []string{"-snapshot-on-shutdown", "maybe"}, nil, "snapshot-on-shutdown"},
		{"final checkpoint without persistence", []string{"-snapshot-on-shutdown", "true"}, nil, "needs persistence-mode"},
		{"empty dir", []string{"-persistence-mode", "aof", "-dir", ""}, nil, "dir must not be empty"},
		{"bad pct env", nil, map[string]string{"SYLPHY_AUTO_REWRITE_PERCENTAGE": "many"}, "SYLPHY_AUTO_REWRITE_PERCENTAGE"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := Load(tt.args, env(tt.env), io.Discard)
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("err = %v, want it to mention %q", err, tt.want)
			}
		})
	}
	// With persistence off, the data directory is never used, so an empty one is fine.
	if _, err := Load([]string{"-dir", ""}, env(nil), io.Discard); err != nil {
		t.Fatalf("empty dir with persistence off: %v", err)
	}
}

func TestSnapshotOnShutdownEnabled(t *testing.T) {
	tests := []struct {
		mode, setting string
		want          bool
	}{
		{"off", "auto", false}, {"off", "false", false},
		{"snapshot", "auto", true}, {"snapshot", "true", true}, {"snapshot", "false", false},
		{"aof", "auto", true}, {"aof", "true", true}, {"aof", "false", false},
	}
	for _, tt := range tests {
		c := Default()
		c.PersistenceMode, c.SnapshotOnShutdown = tt.mode, tt.setting
		if got := c.SnapshotOnShutdownEnabled(); got != tt.want {
			t.Errorf("mode %s, setting %s: %v, want %v", tt.mode, tt.setting, got, tt.want)
		}
	}
}

func TestHelpListsPersistenceFlags(t *testing.T) {
	var sb strings.Builder
	_, err := Load([]string{"-h"}, env(nil), &sb)
	if !errors.Is(err, flag.ErrHelp) {
		t.Fatal(err)
	}
	for _, f := range []string{"-persistence-mode", "-dir", "-append-fsync", "-auto-rewrite-percentage",
		"-auto-rewrite-min-size", "-aof-load-truncated", "-snapshot-on-shutdown"} {
		if !strings.Contains(sb.String(), f) {
			t.Errorf("help does not mention %s", f)
		}
	}
}
