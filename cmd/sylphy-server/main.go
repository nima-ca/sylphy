// Command sylphy-server runs the Sylphy in-memory database.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/nima-ca/sylphy/internal/command"
	"github.com/nima-ca/sylphy/internal/config"
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
	st, err := store.New(cfg.Shards)
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

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	logger.Info("sylphy starting",
		"version", version, "commit", commit, "built", date,
		"addr", cfg.Addr, "shards", cfg.Shards)

	err = srv.ListenAndServe(ctx)
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
