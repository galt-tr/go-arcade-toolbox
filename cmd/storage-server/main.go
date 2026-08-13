// Command storage-server hosts a go-arcade-toolbox storage.Provider behind the
// REST /storage/v1 API, so remote wallets can use it over HTTP as a drop-in
// wdk.WalletStorageProvider (via pkg/storage.NewClient). It wires the tx oracle
// (Arcade) and headers source (ChainTracks), builds the provider over the
// configured backend (SQLite | PostgreSQL | Aerospike-hybrid), migrates it, and
// optionally runs the monitor daemon (SSE apply pipeline + reject→release
// reconciler + scheduled tasks) alongside the HTTP server. Shutdown on
// SIGINT/SIGTERM is graceful.
//
// Usage:
//
//	storage-server -config config.yaml
//
// See config.example.yaml for the full configuration surface.
//
// AUTH NOTE: the default server authenticator trusts an X-Identity-Key header
// (see storage.HeaderAuthenticator) — appropriate for a trusted network or
// behind a gateway that terminates real auth, NOT for direct exposure to an
// untrusted network. Full BRC-103/104 mutual auth is a documented follow-up
// (storage.WithAuthenticator is the seam).
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/monitor"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/perfprovider"
)

func main() {
	var configPath string
	fs := flag.NewFlagSet("storage-server", flag.ExitOnError)
	fs.StringVar(&configPath, "config", "", "path to the YAML config file (see config.example.yaml)")
	_ = fs.Parse(os.Args[1:])

	cfg, err := LoadConfig(configPath)
	if err != nil {
		fmt.Fprintln(os.Stderr, "storage-server: config:", err)
		os.Exit(1)
	}

	logger := newLogger(cfg)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := run(ctx, logger, cfg); err != nil {
		logger.Error("storage-server exited with error", slog.String("error", err.Error()))
		os.Exit(1)
	}
	logger.Info("storage-server stopped cleanly")
}

// run builds the server, serves until ctx is canceled, then shuts down
// gracefully. It is separated from main so tests can drive it.
func run(ctx context.Context, logger *slog.Logger, cfg Config) error {
	handler, shutdown, err := buildServer(ctx, logger, cfg)
	if err != nil {
		return err
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout())
		defer cancel()
		if err := shutdown(sctx); err != nil {
			logger.Warn("shutdown reported an error", slog.String("error", err.Error()))
		}
	}()

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddress,
		Handler:           handler,
		ReadHeaderTimeout: 10 * time.Second,
	}

	serveErr := make(chan error, 1)
	go func() {
		logger.Info("storage-server listening",
			slog.String("address", cfg.HTTPAddress),
			slog.String("backend", cfg.Backend),
			slog.String("network", cfg.Network),
			slog.Bool("monitor", cfg.MonitorEnabled))
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			serveErr <- err
			return
		}
		serveErr <- nil
	}()

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-serveErr:
		if err != nil {
			return fmt.Errorf("http server: %w", err)
		}
		return nil
	}

	sctx, cancel := context.WithTimeout(context.Background(), cfg.shutdownTimeout())
	defer cancel()
	if err := httpSrv.Shutdown(sctx); err != nil {
		return fmt.Errorf("graceful shutdown: %w", err)
	}
	return nil
}

// buildServer wires the oracle, headers, provider (migrated), the optional
// monitor daemon, and the REST server, returning the HTTP handler and a
// shutdown func that stops the monitor and closes the stores.
func buildServer(ctx context.Context, logger *slog.Logger, cfg Config) (http.Handler, func(context.Context) error, error) {
	oracle := arcade.New(logger, nil, cfg.arcadeConfig())

	hdrs, err := headers.New(logger, cfg.chainTracksConfig())
	if err != nil {
		return nil, nil, fmt.Errorf("build headers client: %w", err)
	}

	provider, closeProv, err := perfprovider.New(ctx, logger, cfg.perfproviderConfig(), oracle, hdrs)
	if err != nil {
		return nil, nil, fmt.Errorf("build storage provider: %w", err)
	}

	if _, err := provider.Migrate(ctx, cfg.StorageName, cfg.StorageIdentityKey); err != nil {
		_ = closeProv(context.Background())
		return nil, nil, fmt.Errorf("migrate storage: %w", err)
	}

	var mon *monitor.Daemon
	if cfg.MonitorEnabled {
		monCfg := defs.DefaultMonitorConfig()
		mon, err = monitor.NewDaemon(logger, provider, hdrs, oracle, monCfg)
		if err != nil {
			_ = closeProv(context.Background())
			return nil, nil, fmt.Errorf("build monitor: %w", err)
		}
		if err := mon.Start(ctx, monCfg.Tasks.EnabledTasks()); err != nil {
			_ = closeProv(context.Background())
			return nil, nil, fmt.Errorf("start monitor: %w", err)
		}
	}

	var serverOpts []storage.ServerOption
	if cfg.MaxRequestBodyBytes > 0 {
		serverOpts = append(serverOpts, storage.WithMaxRequestBody(cfg.MaxRequestBodyBytes))
	}
	restSrv := storage.NewServer(logger, provider, serverOpts...)

	shutdown := func(sctx context.Context) error {
		if mon != nil {
			if err := mon.Stop(); err != nil {
				logger.Warn("monitor stop error", slog.String("error", err.Error()))
			}
		}
		return closeProv(sctx)
	}

	return restSrv.Handler(), shutdown, nil
}

// newLogger builds the process logger from config.
func newLogger(cfg Config) *slog.Logger {
	level, err := defs.ParseLogLevelStr(cfg.LogLevel)
	if err != nil {
		level = defs.LogLevelInfo
	}
	handler, err := defs.ParseHandlerTypeStr(cfg.LogHandler)
	if err != nil {
		handler = defs.TextHandler
	}
	return logging.New().WithLevel(level).WithHandler(handler, os.Stdout).Logger()
}

func (c *Config) shutdownTimeout() time.Duration {
	if c.ShutdownTimeoutSeconds <= 0 {
		return 15 * time.Second
	}
	return time.Duration(c.ShutdownTimeoutSeconds) * time.Second
}
