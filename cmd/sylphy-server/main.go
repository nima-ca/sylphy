// Command sylphy-server runs the Sylphy in-memory database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
	"github.com/nima-ca/sylphy/internal/persist"
	"github.com/nima-ca/sylphy/internal/server"
	"github.com/nima-ca/sylphy/internal/store"
)

// Set at build time via -ldflags "-X main.version=...".
var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func main() {
	os.Exit(run(os.Args[1:], os.Getenv, os.Stdout, os.Stderr))
}

func run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) > 0 && (args[0] == "-version" || args[0] == "--version") {
		_, _ = fmt.Fprintf(stdout, "sylphy %s (commit %s, built %s)\n", version, commit, date)
		return 0
	}

	cfg, err := config.Load(args, getenv, stderr)
	if errors.Is(err, flag.ErrHelp) {
		return 0
	}
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "sylphy: %v\n(run with -h for usage)\n", err)
		return 2
	}

	logger := newLogger(cfg, stderr)

	// The first SIGINT/SIGTERM starts a graceful shutdown; a second one skips
	// the final checkpoint (persistence is still flushed and fsynced).
	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(sigs)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	abort, abortCancel := context.WithCancel(context.Background())
	defer abortCancel()
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-sigs:
			cancel()
		case <-done:
			return
		}
		select {
		case <-sigs:
			logger.Warn("second signal: skipping the final checkpoint")
			abortCancel()
		case <-done:
		}
	}()

	return serve(ctx, abort, cfg, logger, nil)
}

// newManager builds the persistence manager for cfg (persistence must be on).
func newManager(cfg config.Config, st *store.Store, reg *command.Registry, logger *slog.Logger) (*persist.Manager, error) {
	policy, err := persist.ParsePolicy(cfg.AppendFsync)
	if err != nil {
		return nil, err
	}
	mode := persist.ModeSnapshot
	if cfg.PersistenceMode == "aof" {
		mode = persist.ModeAOF
	}
	return persist.NewManager(persist.ManagerOptions{
		FS:                    persist.OSFS(),
		Dir:                   cfg.Dir,
		Store:                 st,
		Registry:              reg,
		Config:                &cfg,
		Mode:                  mode,
		Policy:                policy,
		AutoRewritePercentage: cfg.AutoRewritePercentage,
		AutoRewriteMinSize:    cfg.AutoRewriteMinSize,
		LoadTruncated:         cfg.AOFLoadTruncated,
		SnapshotOnShutdown:    cfg.SnapshotOnShutdownEnabled(),
		Logger:                logger,
	})
}

// serve runs the server until ctx is cancelled and returns the process exit
// code. abort cancels the final checkpoint at shutdown. ready, if non-nil, is
// called with the listening address once the server is listening (tests use it
// with port 0).
//
// Startup order matters: with persistence on, the data directory is recovered
// first, with no listener and no sweeper running; then persistence starts (it
// opens the log, installs the expiry hook, and logs and fsyncs the purge of keys
// that expired while the server was down); only then does the sweeper start and
// the listener open. Shutdown runs the other way: stop accepting and drain
// clients, stop the sweeper, and last of all stop persistence (final
// checkpoint, flush and fsync the log).
func serve(ctx, abort context.Context, cfg config.Config, logger *slog.Logger, ready func(net.Addr)) (code int) {
	st, err := store.NewWithOptions(store.Options{
		Shards: cfg.Shards,
		Sweep: store.SweepConfig{
			TickInterval:   cfg.ExpireTickInterval,
			SampleSize:     cfg.ExpireSampleSize,
			CycleBudget:    cfg.ExpireCycleBudget,
			StaleThreshold: cfg.ExpireStaleThreshold,
		},
	})
	if err != nil {
		logger.Error("creating store", "err", err)
		return 1
	}
	reg, err := command.NewDefaultRegistry()
	if err != nil {
		logger.Error("building command registry", "err", err)
		return 1
	}
	srv := server.New(cfg, st, reg, logger)

	logger.Info("sylphy starting",
		"version", version, "commit", commit, "built", date,
		"addr", cfg.Addr, "shards", cfg.Shards,
		"expire_tick", cfg.ExpireTickInterval, "expire_sample", cfg.ExpireSampleSize,
		"expire_budget", cfg.ExpireCycleBudget, "expire_stale", cfg.ExpireStaleThreshold,
		"persistence", cfg.PersistenceMode)

	if cfg.PersistenceEnabled() {
		mgr, err := newManager(cfg, st, reg, logger)
		if err != nil {
			logger.Error("configuring persistence", "err", err)
			return 1
		}
		// Registered first, so it runs last: after clients are drained and the
		// sweeper is stopped. A failed start never takes the final checkpoint.
		defer func() {
			if err := mgr.Shutdown(abort); err != nil {
				logger.Error("persistence shutdown failed", "err", err)
				code = 1
			}
		}()
		logger.Info("recovering", "dir", cfg.Dir, "mode", cfg.PersistenceMode)
		if _, err := mgr.Recover(ctx); err != nil {
			logger.Error("recovery failed; refusing to start so the data directory is left untouched", "err", err)
			return 1
		}
		if err := mgr.Start(ctx); err != nil {
			logger.Error("starting persistence", "err", err)
			return 1
		}
		srv.SetPersistence(persist.NewCommands(mgr), mgr.Propagator(),
			cfg.PersistenceMode == "aof" && cfg.AppendFsync == "always")
	}

	// The store owns the expiry sweeper goroutine. Close runs on every return
	// path, after the server has drained clients, so the sweeper never outlives
	// the server and never appends to a closed log.
	st.Start()
	defer st.Close()

	var lc net.ListenConfig
	ln, err := lc.Listen(ctx, "tcp", cfg.Addr)
	if err != nil {
		logger.Error("listening", "addr", cfg.Addr, "err", err)
		return 1
	}
	if ready != nil {
		ready(ln.Addr())
	}

	err = srv.Serve(ctx, ln)
	switch {
	case err == nil:
		logger.Info("sylphy stopped cleanly")
		return 0
	case errors.Is(err, context.DeadlineExceeded):
		logger.Warn("grace period elapsed; remaining connections were force-closed")
		return 0
	case errors.Is(err, server.ErrServerClosed):
		return 0
	default:
		logger.Error("server failed", "err", err)
		return 1
	}
}

func newLogger(cfg config.Config, w io.Writer) *slog.Logger {
	opts := &slog.HandlerOptions{Level: cfg.SlogLevel()}
	if cfg.LogFormat == "json" {
		return slog.New(slog.NewJSONHandler(w, opts))
	}
	return slog.New(slog.NewTextHandler(w, opts))
}
