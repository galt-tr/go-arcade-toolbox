// Command perfrunner drives the go-arcade-toolbox performance harness for a
// full-length benchmark run and renders the resulting JSON into a Markdown
// report.
//
// # Usage
//
//	perfrunner [flags]                 run a benchmark, write JSON (+ optional MD)
//	perfrunner render <result.json>    render an existing result JSON to Markdown
//
// The default run wires the wallet + storage stack to the in-process mockarcade
// doubles, so it measures STORAGE + WALLET throughput (broadcasts are accepted
// instantly, no network). Pass -real-arcade URL to broadcast through a live
// Arcade service instead, measuring true end-to-end cost including the network.
//
// SQLite runs are fully self-contained. Postgres and the Aerospike hybrid
// require connection details via flags (this binary does not manage containers;
// the perf test suite, test/perf, spins them via internal/testenv):
//
//	perfrunner -backend postgres -pg-dsn "postgres://..." -workers 64 -duration 5m
//	perfrunner -backend aerospike-hybrid -pg-dsn "postgres://..." \
//	           -aero-host 127.0.0.1 -aero-port 3000 -aero-namespace test
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/perf"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
)

func main() {
	if len(os.Args) >= 2 && os.Args[1] == "render" {
		if err := runRender(os.Args[2:]); err != nil {
			fmt.Fprintln(os.Stderr, "perfrunner render:", err)
			os.Exit(1)
		}
		return
	}
	if err := runBench(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "perfrunner:", err)
		os.Exit(1)
	}
}

func runBench(argv []string) error {
	fs := flag.NewFlagSet("perfrunner", flag.ExitOnError)
	var (
		backend    = fs.String("backend", "sqlite", "backend: sqlite|postgres|aerospike-hybrid")
		workers    = fs.Int("workers", 32, "number of concurrent worker goroutines")
		tps        = fs.Float64("tps", 0, "target TPS (0 = unbounded/max)")
		duration   = fs.Duration("duration", 5*time.Minute, "measured run duration")
		warmup     = fs.Duration("warmup", 30*time.Second, "warmup excluded from measurement")
		poolSize   = fs.Int("pool-size", 2000, "number of coins to pre-mint into the pool")
		denom      = fs.Uint64("denomination", 1_000_000, "satoshis per pre-minted coin")
		payment    = fs.Uint64("payment", 1000, "satoshis of the payment output per op")
		mode       = fs.String("mode", "twostep", "op mode: twostep|signandprocess")
		throughput = fs.Bool("throughput", false, "throughput mode: fund via the denominated fuel-pool ClaimExact fast path (Strategy=throughput) instead of the tiered privacy claim")
		maxDBConns = fs.Int("max-db-conns", 0, "cap the shared SQL connection pool (0=driver default)")
		noMonitor  = fs.Bool("no-monitor", false, "disable the monitor daemon")
		noMine     = fs.Bool("no-mine", false, "disable the auto-miner (SSE MINED maturation)")
		realArcade = fs.String("real-arcade", "", "live Arcade base URL (swaps out mockarcade for true e2e incl. network)")
		sqlitePath = fs.String("sqlite-path", "", "SQLite file path (default: a temp file)")
		pgDSN      = fs.String("pg-dsn", "", "PostgreSQL DSN (postgres/aerospike-hybrid)")
		aeroHost   = fs.String("aero-host", "", "Aerospike host (aerospike-hybrid)")
		aeroPort   = fs.Int("aero-port", 3000, "Aerospike port (aerospike-hybrid)")
		aeroNS     = fs.String("aero-namespace", "test", "Aerospike namespace (aerospike-hybrid)")
		aeroSet    = fs.String("aero-set", "perf", "Aerospike set (aerospike-hybrid)")
		outDir     = fs.String("out", "perf-results/", "directory to write the result JSON")
		renderMD   = fs.Bool("render", true, "also render a Markdown report next to the JSON")
		mdDir      = fs.String("md-out", "docs/benchmarks/", "directory for the rendered Markdown report")
		label      = fs.String("label", "", "freeform run label recorded in the report")
		verbose    = fs.Bool("v", false, "verbose (warn-level) logging")
	)
	if err := fs.Parse(argv); err != nil {
		return err
	}

	be, err := perfprovider.ParseBackend(*backend)
	if err != nil {
		return err
	}

	cfg := perf.DefaultConfig(be)
	cfg.Workers = *workers
	cfg.TargetTPS = *tps
	cfg.Duration = *duration
	cfg.Warmup = *warmup
	cfg.PoolSize = *poolSize
	cfg.Denomination = *denom
	cfg.PaymentSats = *payment
	cfg.Mode = perf.Mode(*mode)
	cfg.Throughput = *throughput
	cfg.MaxDBConns = *maxDBConns
	cfg.RunMonitor = !*noMonitor
	cfg.Mine = !*noMine && cfg.RunMonitor
	cfg.RealArcadeURL = *realArcade
	cfg.Network = defs.NetworkTestnet
	cfg.Label = *label

	cfg.PostgresDSN = *pgDSN
	cfg.AeroHost = *aeroHost
	cfg.AeroPort = *aeroPort
	cfg.AeroNamespace = *aeroNS
	cfg.AeroSet = *aeroSet
	if be == perfprovider.BackendSQLite {
		cfg.SQLitePath = *sqlitePath
		if cfg.SQLitePath == "" {
			f, err := os.CreateTemp("", "perfrunner-*.db")
			if err != nil {
				return fmt.Errorf("create temp sqlite: %w", err)
			}
			_ = f.Close()
			cfg.SQLitePath = f.Name()
			defer func() { _ = os.Remove(cfg.SQLitePath) }()
		}
	}

	level := slog.LevelError
	if *verbose {
		level = slog.LevelWarn
	}
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: level}))

	fmt.Printf("perfrunner: backend=%s mode=%s workers=%d tps=%s duration=%s warmup=%s pool=%d\n",
		be, cfg.Mode, cfg.Workers, tpsStr(cfg.TargetTPS), cfg.Duration, cfg.Warmup, cfg.PoolSize)
	if cfg.RealArcadeURL != "" {
		fmt.Printf("perfrunner: broadcasting through LIVE arcade %s (auto-miner disabled)\n", cfg.RealArcadeURL)
	}

	ctx := context.Background()
	start := time.Now()
	result, err := perf.Run(ctx, cfg, logger)
	if err != nil {
		return err
	}

	suffix := ""
	if cfg.Throughput {
		suffix = "-throughput"
	}

	jsonName := perf.DefaultResultFilename(string(be), string(cfg.Mode)+suffix, start)
	jsonPath := filepath.Join(*outDir, jsonName)
	if err := perf.WriteJSON(result, jsonPath); err != nil {
		return err
	}
	fmt.Printf("perfrunner: [%s] sustained %.1f TPS (%.1f%% of %.0f target)\n",
		result.Config.FundingPath, result.Throughput.SustainedTPS, result.Throughput.PctOfTarget, perf.TargetTPS)
	fmt.Printf("perfrunner: wrote %s\n", jsonPath)

	if *renderMD {
		mdName := fmt.Sprintf("%s-%s-%s%s.md", start.Format("20060102"), be, cfg.Mode, suffix)
		mdPath := filepath.Join(*mdDir, mdName)
		if err := writeMarkdown(result, mdPath); err != nil {
			return err
		}
		fmt.Printf("perfrunner: wrote %s\n", mdPath)
	}
	return nil
}

func runRender(argv []string) error {
	fs := flag.NewFlagSet("render", flag.ExitOnError)
	out := fs.String("o", "", "output Markdown path (default: stdout)")
	if err := fs.Parse(argv); err != nil {
		return err
	}
	if fs.NArg() != 1 {
		return fmt.Errorf("usage: perfrunner render [-o out.md] <result.json>")
	}
	result, err := perf.ReadJSON(fs.Arg(0))
	if err != nil {
		return err
	}
	if *out == "" {
		fmt.Print(perf.Render(result))
		return nil
	}
	return writeMarkdown(result, *out)
}

func writeMarkdown(result *perf.RunResult, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o750); err != nil {
		return fmt.Errorf("mkdir for report: %w", err)
	}
	if err := os.WriteFile(path, []byte(perf.Render(result)), 0o644); err != nil { //nolint:gosec // world-readable report
		return fmt.Errorf("write report: %w", err)
	}
	return nil
}

func tpsStr(t float64) string {
	if t <= 0 {
		return "max"
	}
	return fmt.Sprintf("%.0f", t)
}
