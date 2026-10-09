package config

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/nima-ca/sylphy/internal/store"
)

// config does not import store, so this test is what keeps the duplicated
// sweeper defaults from drifting apart.
func TestExpireDefaultsMatchStore(t *testing.T) {
	d := Default()
	want := store.DefaultSweepConfig()
	got := store.SweepConfig{
		TickInterval:   d.ExpireTickInterval,
		SampleSize:     d.ExpireSampleSize,
		CycleBudget:    d.ExpireCycleBudget,
		StaleThreshold: d.ExpireStaleThreshold,
	}
	if got != want {
		t.Fatalf("config defaults %+v differ from store defaults %+v", got, want)
	}
	if err := got.Validate(); err != nil {
		t.Fatal(err)
	}
}

func TestExpireEnvThenFlagPrecedence(t *testing.T) {
	e := env(map[string]string{
		"SYLPHY_EXPIRE_TICK_INTERVAL":   "250ms",
		"SYLPHY_EXPIRE_SAMPLE_SIZE":     "50",
		"SYLPHY_EXPIRE_CYCLE_BUDGET":    "10ms",
		"SYLPHY_EXPIRE_STALE_THRESHOLD": "0.5",
	})

	cfg, err := Load(nil, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExpireTickInterval != 250*time.Millisecond || cfg.ExpireSampleSize != 50 ||
		cfg.ExpireCycleBudget != 10*time.Millisecond || cfg.ExpireStaleThreshold != 0.5 {
		t.Fatalf("env not applied: %+v", cfg)
	}

	// Flags win over the environment, field by field.
	cfg, err = Load([]string{
		"-expire-tick-interval", "1s",
		"-expire-sample-size=7",
		"-expire-cycle-budget", "2ms",
		"-expire-stale-threshold", "0.1",
	}, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExpireTickInterval != time.Second || cfg.ExpireSampleSize != 7 ||
		cfg.ExpireCycleBudget != 2*time.Millisecond || cfg.ExpireStaleThreshold != 0.1 {
		t.Fatalf("flags did not override env: %+v", cfg)
	}

	// A flag for one setting leaves the others on their env values.
	cfg, err = Load([]string{"-expire-sample-size", "9"}, e, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.ExpireSampleSize != 9 || cfg.ExpireTickInterval != 250*time.Millisecond {
		t.Fatalf("got %+v", cfg)
	}
}

func TestExpireInvalid(t *testing.T) {
	tests := []struct {
		name string
		args []string
		env  map[string]string
		want string
	}{
		{"zero tick flag", []string{"-expire-tick-interval", "0s"}, nil, "expire-tick-interval"},
		{"negative tick flag", []string{"-expire-tick-interval", "-1s"}, nil, "expire-tick-interval"},
		{"zero sample flag", []string{"-expire-sample-size", "0"}, nil, "expire-sample-size"},
		{"negative sample flag", []string{"-expire-sample-size", "-3"}, nil, "expire-sample-size"},
		{"zero budget flag", []string{"-expire-cycle-budget", "0s"}, nil, "expire-cycle-budget"},
		{"threshold one", []string{"-expire-stale-threshold", "1"}, nil, "expire-stale-threshold"},
		{"threshold negative", []string{"-expire-stale-threshold", "-0.1"}, nil, "expire-stale-threshold"},
		{"threshold NaN", []string{"-expire-stale-threshold", "NaN"}, nil, "expire-stale-threshold"},
		{"bad tick env", nil, map[string]string{"SYLPHY_EXPIRE_TICK_INTERVAL": "soon"}, "SYLPHY_EXPIRE_TICK_INTERVAL"},
		{"bad sample env", nil, map[string]string{"SYLPHY_EXPIRE_SAMPLE_SIZE": "many"}, "SYLPHY_EXPIRE_SAMPLE_SIZE"},
		{"bad budget env", nil, map[string]string{"SYLPHY_EXPIRE_CYCLE_BUDGET": "x"}, "SYLPHY_EXPIRE_CYCLE_BUDGET"},
		{"bad threshold env", nil, map[string]string{"SYLPHY_EXPIRE_STALE_THRESHOLD": "high"}, "SYLPHY_EXPIRE_STALE_THRESHOLD"},
		{"out-of-range threshold env", nil, map[string]string{"SYLPHY_EXPIRE_STALE_THRESHOLD": "2"}, "expire-stale-threshold"},
		{"flag cannot rescue bad env", []string{"-expire-sample-size", "5"}, map[string]string{"SYLPHY_EXPIRE_CYCLE_BUDGET": "x"}, "SYLPHY_EXPIRE_CYCLE_BUDGET"},
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

func TestHelpListsExpireFlags(t *testing.T) {
	var sb strings.Builder
	_, _ = Load([]string{"-h"}, env(nil), &sb)
	for _, f := range []string{"-expire-tick-interval", "-expire-sample-size", "-expire-cycle-budget", "-expire-stale-threshold"} {
		if !strings.Contains(sb.String(), f) {
			t.Errorf("help does not mention %s", f)
		}
	}
}
