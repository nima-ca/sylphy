package config

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"strconv"
	"strings"
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

	// PersistenceMode is off (the default: nothing is written to disk),
	// snapshot (checkpoints only: SAVE, BGSAVE and shutdown) or aof (every write
	// is logged, plus checkpoints).
	PersistenceMode string
	// Dir is the data directory used when PersistenceMode is not off.
	Dir string
	// AppendFsync is the AOF fsync policy: always, everysec or no. It only
	// matters in aof mode.
	AppendFsync string
	// AutoRewritePercentage triggers a checkpoint when the log has grown by this
	// percentage over the base snapshot; 0 disables the automatic trigger.
	AutoRewritePercentage int
	// AutoRewriteMinSize is the smallest base plus log size, in bytes, at which
	// the automatic trigger may fire.
	AutoRewriteMinSize int64
	// AOFLoadTruncated repairs a partial record at the end of the last log
	// segment at startup instead of refusing to start.
	AOFLoadTruncated bool
	// SnapshotOnShutdown is auto, true or false: whether shutdown takes a final
	// checkpoint. Auto means yes whenever persistence is on. It is a string so
	// that Config stays comparable.
	SnapshotOnShutdown string
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

		PersistenceMode:       "off",
		Dir:                   "./data",
		AppendFsync:           "everysec",
		AutoRewritePercentage: 100,
		AutoRewriteMinSize:    64 << 20,
		AOFLoadTruncated:      true,
		SnapshotOnShutdown:    "auto",
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

// PersistenceEnabled reports whether anything is persisted to disk.
func (c Config) PersistenceEnabled() bool { return c.PersistenceMode != "off" }

// SnapshotOnShutdownEnabled resolves SnapshotOnShutdown: true and false are
// taken as they are, and auto means on whenever persistence is on.
func (c Config) SnapshotOnShutdownEnabled() bool {
	switch c.SnapshotOnShutdown {
	case "true":
		return true
	case "false":
		return false
	}
	return c.PersistenceEnabled()
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

	switch c.PersistenceMode {
	case "off", "snapshot", "aof":
	default:
		add("persistence-mode must be off, snapshot or aof, got %q", c.PersistenceMode)
	}
	switch c.AppendFsync {
	case "always", "everysec", "no":
	default:
		add("append-fsync must be always, everysec or no, got %q", c.AppendFsync)
	}
	if c.AutoRewritePercentage < 0 {
		add("auto-rewrite-percentage must not be negative, got %d", c.AutoRewritePercentage)
	}
	if c.AutoRewriteMinSize < 0 {
		add("auto-rewrite-min-size must not be negative, got %d", c.AutoRewriteMinSize)
	}
	switch c.SnapshotOnShutdown {
	case "auto", "false":
	case "true":
		if c.PersistenceMode == "off" {
			add("snapshot-on-shutdown=true needs persistence-mode snapshot or aof")
		}
	default:
		add("snapshot-on-shutdown must be auto, true or false, got %q", c.SnapshotOnShutdown)
	}
	if c.PersistenceEnabled() && strings.TrimSpace(c.Dir) == "" {
		add("dir must not be empty when persistence is on")
	}

	return errors.Join(errs...)
}

// ParseSize parses a byte count: a plain integer, or an integer with a binary
// suffix (k, kb, kib, m, mb, mib, g, gb, gib, b), case-insensitive. All
// suffixes are powers of 1024.
func ParseSize(s string) (int64, error) {
	t := strings.ToLower(strings.TrimSpace(s))
	mult := int64(1)
	for _, u := range []struct {
		suffix string
		mult   int64
	}{
		{"kib", 1 << 10}, {"mib", 1 << 20}, {"gib", 1 << 30},
		{"kb", 1 << 10}, {"mb", 1 << 20}, {"gb", 1 << 30},
		{"k", 1 << 10}, {"m", 1 << 20}, {"g", 1 << 30}, {"b", 1},
	} {
		if strings.HasSuffix(t, u.suffix) {
			t, mult = strings.TrimSpace(strings.TrimSuffix(t, u.suffix)), u.mult
			break
		}
	}
	n, err := strconv.ParseInt(t, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid size %q (want a number of bytes, optionally with a k, m or g suffix)", s)
	}
	if n < 0 {
		return 0, fmt.Errorf("invalid size %q: must not be negative", s)
	}
	if n > math.MaxInt64/mult {
		return 0, fmt.Errorf("invalid size %q: too large", s)
	}
	return n * mult, nil
}

// sizeFlag is a flag.Value for byte sizes.
type sizeFlag struct{ p *int64 }

func (f sizeFlag) String() string {
	if f.p == nil {
		return "0"
	}
	return strconv.FormatInt(*f.p, 10)
}

func (f sizeFlag) Set(s string) error {
	v, err := ParseSize(s)
	if err != nil {
		return err
	}
	*f.p = v
	return nil
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
	fs.StringVar(&cfg.PersistenceMode, "persistence-mode", cfg.PersistenceMode, "off|snapshot|aof (env SYLPHY_PERSISTENCE_MODE)")
	fs.StringVar(&cfg.Dir, "dir", cfg.Dir, "data directory for snapshots and the append-only file (env SYLPHY_DIR)")
	fs.StringVar(&cfg.AppendFsync, "append-fsync", cfg.AppendFsync, "always|everysec|no, aof mode only (env SYLPHY_APPEND_FSYNC)")
	fs.IntVar(&cfg.AutoRewritePercentage, "auto-rewrite-percentage", cfg.AutoRewritePercentage, "checkpoint when the log has grown this percent over the base snapshot, 0=never (env SYLPHY_AUTO_REWRITE_PERCENTAGE)")
	fs.Var(sizeFlag{&cfg.AutoRewriteMinSize}, "auto-rewrite-min-size", "smallest base+log size in bytes (suffixes k, m, g allowed) that may trigger a checkpoint (env SYLPHY_AUTO_REWRITE_MIN_SIZE)")
	fs.BoolVar(&cfg.AOFLoadTruncated, "aof-load-truncated", cfg.AOFLoadTruncated, "repair a partial last log record at startup instead of refusing to start (env SYLPHY_AOF_LOAD_TRUNCATED)")
	fs.StringVar(&cfg.SnapshotOnShutdown, "snapshot-on-shutdown", cfg.SnapshotOnShutdown, "auto|true|false: final checkpoint at shutdown; auto means whenever persistence is on (env SYLPHY_SNAPSHOT_ON_SHUTDOWN)")

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
	sizeVar := func(name string, dst *int64) {
		parse(name, func(v string) (err error) { *dst, err = ParseSize(v); return })
	}
	boolVar := func(name string, dst *bool) {
		parse(name, func(v string) (err error) { *dst, err = strconv.ParseBool(v); return })
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
	str("SYLPHY_PERSISTENCE_MODE", &cfg.PersistenceMode)
	str("SYLPHY_DIR", &cfg.Dir)
	str("SYLPHY_APPEND_FSYNC", &cfg.AppendFsync)
	intVar("SYLPHY_AUTO_REWRITE_PERCENTAGE", &cfg.AutoRewritePercentage)
	sizeVar("SYLPHY_AUTO_REWRITE_MIN_SIZE", &cfg.AutoRewriteMinSize)
	boolVar("SYLPHY_AOF_LOAD_TRUNCATED", &cfg.AOFLoadTruncated)
	str("SYLPHY_SNAPSHOT_ON_SHUTDOWN", &cfg.SnapshotOnShutdown)
	return errors.Join(errs...)
}
