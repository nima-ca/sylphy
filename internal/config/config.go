package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strconv"
	"time"

	"github.com/nima-ca/sylphy/internal/protocol"
)

// Config holds all server settings.
type Config struct {
	// Addr is the TCP listen address.
	Addr string
	// Shards is the number of store shards (power of two).
	Shards int
	// MaxClients is the maximum number of concurrent client connections.
	MaxClients int
	// IdleTimeout closes connections idle for this long; 0 disables it.
	IdleTimeout time.Duration
	// ShutdownGracePeriod is how long graceful shutdown waits before
	// force-closing connections.
	ShutdownGracePeriod time.Duration
	// LogLevel is one of debug, info, warn, error.
	LogLevel string
	// LogFormat is text or json.
	LogFormat string
	// ExpireTickInterval is the time between active-expiry sweeper cycles.
	ExpireTickInterval time.Duration
	// ExpireSampleSize is how many expiring keys the sweeper examines per
	// shard per batch.
	ExpireSampleSize int
	// ExpireCycleBudget caps the real time one sweeper cycle may spend.
	ExpireCycleBudget time.Duration
	// ExpireStaleThreshold is the expired fraction of a batch, in [0, 1),
	// above which the sweeper samples the same shard again.
	ExpireStaleThreshold float64
	// MaxBulkLen, MaxArrayLen, MaxInlineLen and MaxDepth bound RESP parsing.
	MaxBulkLen   int64
	MaxArrayLen  int64
	MaxInlineLen int
	MaxDepth     int
}

// Default returns the default configuration.
func Default() Config {
	l := protocol.DefaultLimits()
	return Config{
		Addr:                "127.0.0.1:6380",
		Shards:              32,
		MaxClients:          10000,
		IdleTimeout:         0,
		ShutdownGracePeriod: 10 * time.Second,
		LogLevel:            "info",
		LogFormat:           "text",
		// Sweeper defaults mirror store.DefaultSweepConfig (a test keeps them
		// in sync); config avoids importing store.
		ExpireTickInterval:   100 * time.Millisecond,
		ExpireSampleSize:     20,
		ExpireCycleBudget:    25 * time.Millisecond,
		ExpireStaleThreshold: 0.25,
		MaxBulkLen:           l.MaxBulkLen,
		MaxArrayLen:          l.MaxArrayLen,
		MaxInlineLen:         l.MaxInlineLen,
		MaxDepth:             l.MaxDepth,
	}
}

// ProtocolLimits returns the RESP parser limits.
func (c Config) ProtocolLimits() protocol.Limits {
	return protocol.Limits{
		MaxBulkLen:   c.MaxBulkLen,
		MaxArrayLen:  c.MaxArrayLen,
		MaxInlineLen: c.MaxInlineLen,
		MaxDepth:     c.MaxDepth,
	}
}

// SlogLevel converts LogLevel to a slog.Level (info if unrecognized).
func (c Config) SlogLevel() slog.Level {
	switch c.LogLevel {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Validate reports every problem with c, joined into one error.
func (c Config) Validate() error {
	var errs []error
	add := func(format string, a ...any) { errs = append(errs, fmt.Errorf(format, a...)) }

	if _, _, err := net.SplitHostPort(c.Addr); err != nil {
		add("addr %q is not a valid host:port: %v", c.Addr, err)
	}
	if c.Shards <= 0 || c.Shards&(c.Shards-1) != 0 {
		add("shards must be a positive power of two, got %d", c.Shards)
	}
	if c.MaxClients <= 0 {
		add("max-clients must be positive, got %d", c.MaxClients)
	}
	if c.IdleTimeout < 0 {
		add("idle-timeout must not be negative, got %v", c.IdleTimeout)
	}
	if c.ShutdownGracePeriod < 0 {
		add("shutdown-grace-period must not be negative, got %v", c.ShutdownGracePeriod)
	}

	switch c.LogLevel {
	case "debug", "info", "warn", "error":
	default:
		add("log-level must be debug, info, warn or error, got %q", c.LogLevel)
	}

	switch c.LogFormat {
	case "text", "json":
	default:
		add("log-format must be text or json, got %q", c.LogFormat)
	}

	if c.ExpireTickInterval <= 0 {
		add("expire-tick-interval must be positive, got %v", c.ExpireTickInterval)
	}
	if c.ExpireSampleSize <= 0 {
		add("expire-sample-size must be positive, got %d", c.ExpireSampleSize)
	}
	if c.ExpireCycleBudget <= 0 {
		add("expire-cycle-budget must be positive, got %v", c.ExpireCycleBudget)
	}
	if !(c.ExpireStaleThreshold >= 0 && c.ExpireStaleThreshold < 1) { // also rejects NaN
		add("expire-stale-threshold must be in [0, 1), got %v", c.ExpireStaleThreshold)
	}
	if c.MaxBulkLen <= 0 || c.MaxArrayLen <= 0 || c.MaxInlineLen <= 0 || c.MaxDepth <= 0 {
		add("protocol limits (max-bulk-len, max-array-len, max-inline-len, max-depth) must be positive")
	}

	return errors.Join(errs...)
}

// Load builds a Config from defaults, then SYLPHY_* environment variables (via
// getenv), then command-line args (flags win). out receives usage text for -h.
// It returns flag.ErrHelp when help was requested.
func Load(args []string, getenv func(string) string, out io.Writer) (Config, error) {
	cfg := Default()
	if err := applyEnv(&cfg, getenv); err != nil {
		return Config{}, err
	}

	fs := flag.NewFlagSet("sylphy-server", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	fs.StringVar(&cfg.Addr, "addr", cfg.Addr, "TCP listen address (env SYLPHY_ADDR)")
	fs.IntVar(&cfg.Shards, "shards", cfg.Shards, "store shard count, power of two (env SYLPHY_SHARDS)")
	fs.IntVar(&cfg.MaxClients, "max-clients", cfg.MaxClients, "max concurrent clients (env SYLPHY_MAX_CLIENTS)")
	fs.DurationVar(&cfg.IdleTimeout, "idle-timeout", cfg.IdleTimeout, "close idle connections after this long, 0=never (env SYLPHY_IDLE_TIMEOUT)")
	fs.DurationVar(&cfg.ShutdownGracePeriod, "shutdown-grace-period", cfg.ShutdownGracePeriod, "graceful shutdown deadline (env SYLPHY_SHUTDOWN_GRACE_PERIOD)")
	fs.StringVar(&cfg.LogLevel, "log-level", cfg.LogLevel, "debug|info|warn|error (env SYLPHY_LOG_LEVEL)")
	fs.StringVar(&cfg.LogFormat, "log-format", cfg.LogFormat, "text|json (env SYLPHY_LOG_FORMAT)")
	fs.DurationVar(&cfg.ExpireTickInterval, "expire-tick-interval", cfg.ExpireTickInterval, "active expiry cycle interval (env SYLPHY_EXPIRE_TICK_INTERVAL)")
	fs.IntVar(&cfg.ExpireSampleSize, "expire-sample-size", cfg.ExpireSampleSize, "expiring keys sampled per shard per batch (env SYLPHY_EXPIRE_SAMPLE_SIZE)")
	fs.DurationVar(&cfg.ExpireCycleBudget, "expire-cycle-budget", cfg.ExpireCycleBudget, "max time one expiry cycle may run (env SYLPHY_EXPIRE_CYCLE_BUDGET)")
	fs.Float64Var(&cfg.ExpireStaleThreshold, "expire-stale-threshold", cfg.ExpireStaleThreshold, "expired fraction in [0,1) that triggers another sample (env SYLPHY_EXPIRE_STALE_THRESHOLD)")
	fs.Int64Var(&cfg.MaxBulkLen, "max-bulk-len", cfg.MaxBulkLen, "max bulk string bytes (env SYLPHY_MAX_BULK_LEN)")
	fs.Int64Var(&cfg.MaxArrayLen, "max-array-len", cfg.MaxArrayLen, "max array elements (env SYLPHY_MAX_ARRAY_LEN)")
	fs.IntVar(&cfg.MaxInlineLen, "max-inline-len", cfg.MaxInlineLen, "max inline line bytes (env SYLPHY_MAX_INLINE_LEN)")
	fs.IntVar(&cfg.MaxDepth, "max-depth", cfg.MaxDepth, "max array nesting (env SYLPHY_MAX_DEPTH)")

	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			fs.SetOutput(out)
			_, _ = fmt.Fprintln(out, "Usage: sylphy-server [flags]")
			fs.PrintDefaults()
		}
		return Config{}, err
	}
	if fs.NArg() > 0 {
		return Config{}, fmt.Errorf("unexpected arguments: %v", fs.Args())
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, fmt.Errorf("invalid configuration:\n%w", err)
	}
	return cfg, nil
}

func applyEnv(cfg *Config, getenv func(string) string) error {
	var errs []error
	str := func(name string, dst *string) {
		if v := getenv(name); v != "" {
			*dst = v
		}
	}
	parse := func(name string, set func(string) error) {
		if v := getenv(name); v != "" {
			if err := set(v); err != nil {
				errs = append(errs, fmt.Errorf("%s=%q: %w", name, v, err))
			}
		}
	}
	intVar := func(name string, dst *int) {
		parse(name, func(v string) (err error) { *dst, err = strconv.Atoi(v); return })
	}
	int64Var := func(name string, dst *int64) {
		parse(name, func(v string) (err error) { *dst, err = strconv.ParseInt(v, 10, 64); return })
	}
	floatVar := func(name string, dst *float64) {
		parse(name, func(v string) (err error) { *dst, err = strconv.ParseFloat(v, 64); return })
	}
	durVar := func(name string, dst *time.Duration) {
		parse(name, func(v string) (err error) { *dst, err = time.ParseDuration(v); return })
	}

	str("SYLPHY_ADDR", &cfg.Addr)
	str("SYLPHY_LOG_LEVEL", &cfg.LogLevel)
	str("SYLPHY_LOG_FORMAT", &cfg.LogFormat)
	intVar("SYLPHY_SHARDS", &cfg.Shards)
	intVar("SYLPHY_MAX_CLIENTS", &cfg.MaxClients)
	intVar("SYLPHY_MAX_INLINE_LEN", &cfg.MaxInlineLen)
	intVar("SYLPHY_MAX_DEPTH", &cfg.MaxDepth)
	int64Var("SYLPHY_MAX_BULK_LEN", &cfg.MaxBulkLen)
	int64Var("SYLPHY_MAX_ARRAY_LEN", &cfg.MaxArrayLen)
	durVar("SYLPHY_IDLE_TIMEOUT", &cfg.IdleTimeout)
	durVar("SYLPHY_SHUTDOWN_GRACE_PERIOD", &cfg.ShutdownGracePeriod)
	durVar("SYLPHY_EXPIRE_TICK_INTERVAL", &cfg.ExpireTickInterval)
	durVar("SYLPHY_EXPIRE_CYCLE_BUDGET", &cfg.ExpireCycleBudget)
	intVar("SYLPHY_EXPIRE_SAMPLE_SIZE", &cfg.ExpireSampleSize)
	floatVar("SYLPHY_EXPIRE_STALE_THRESHOLD", &cfg.ExpireStaleThreshold)
	return errors.Join(errs...)
}
