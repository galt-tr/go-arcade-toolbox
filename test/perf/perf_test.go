//go:build perf

// Package perf_test is the performance suite. It drives internal/perf against
// each benchmark backend, spinning the backend via internal/testenv (podman,
// graceful skip when no runtime is available), running a BOUNDED perf run,
// writing the result JSON to <repo>/perf-results/, and asserting a conservative
// sanity floor so CI catches gross regressions. The full 5-minute run and the
// headline numbers come from cmd/perfrunner (or these tests with longer env
// overrides), not from the CI floor.
//
// Tagged `perf` so it never runs in the default CI suite. Run with:
//
//	go test -tags perf -run TestPerf -timeout 30m ./test/perf/...
//
// Duration/warmup/workers/pool are env-overridable so the same suite serves
// both the quick CI floor and a longer manual capture:
//
//	PERF_DURATION=90s PERF_WARMUP=15s PERF_WORKERS=64 PERF_POOL=4000 \
//	  go test -tags perf -run TestPerf_PostgresModeA -timeout 30m ./test/perf/...
package perf_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/perf"
	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
)

// baseConfig returns a bounded run config with env overrides applied.
func baseConfig(t *testing.T, backend perfprovider.Backend) perf.Config {
	t.Helper()
	cfg := perf.DefaultConfig(backend)
	cfg.Duration = envDuration("PERF_DURATION", 20*time.Second)
	cfg.Warmup = envDuration("PERF_WARMUP", 5*time.Second)
	cfg.Workers = envInt("PERF_WORKERS", 16)
	cfg.PoolSize = envInt("PERF_POOL", 800)
	cfg.Denomination = uint64(envInt("PERF_DENOM", 1_000_000))
	cfg.PaymentSats = uint64(envInt("PERF_PAYMENT", 1000))
	cfg.MaxDBConns = envInt("PERF_MAX_DB_CONNS", cfg.Workers+8)
	return cfg
}

// perfModes returns the op modes to run as subtests. Default is two-step only
// (keeps the CI floor a single quick run); set PERF_MODES to a space-separated
// list to capture more, e.g. PERF_MODES="twostep signandprocess".
func perfModes() []perf.Mode {
	v := os.Getenv("PERF_MODES")
	if v == "" {
		return []perf.Mode{perf.ModeTwoStep}
	}
	var out []perf.Mode
	for _, f := range strings.Fields(v) {
		out = append(out, perf.Mode(f))
	}
	return out
}

func TestPerf_PostgresModeA(t *testing.T) {
	pg := testenv.StartPostgres(t)
	for _, mode := range perfModes() {
		t.Run(string(mode), func(t *testing.T) {
			cfg := baseConfig(t, perfprovider.BackendPostgres)
			cfg.Mode = mode
			cfg.PostgresDSN = pg.IsolatedSchemaDSN(t) // fresh isolated schema per subtest
			cfg.Label = "Postgres Mode A (shared SQL) — bounded perf run"
			runAndAssert(t, cfg, 20.0)
		})
	}
}

func TestPerf_AerospikeHybridModeB(t *testing.T) {
	pg := testenv.StartPostgres(t)
	aero := testenv.StartAerospike(t)
	for _, mode := range perfModes() {
		t.Run(string(mode), func(t *testing.T) {
			cfg := baseConfig(t, perfprovider.BackendAerospikeHybrid)
			cfg.Mode = mode
			cfg.PostgresDSN = pg.IsolatedSchemaDSN(t)
			cfg.AeroHost = aero.Host()
			cfg.AeroPort = aero.Port()
			cfg.AeroNamespace = aero.Namespace()
			cfg.AeroSet = fmt.Sprintf("perf%d", time.Now().UnixNano()) // fresh set per subtest
			cfg.Label = "Aerospike + Postgres hybrid Mode B (split stores) — bounded perf run"
			runAndAssert(t, cfg, 20.0)
		})
	}
}

func TestPerf_SQLiteBaseline(t *testing.T) {
	for _, mode := range perfModes() {
		t.Run(string(mode), func(t *testing.T) {
			cfg := baseConfig(t, perfprovider.BackendSQLite)
			cfg.Mode = mode
			cfg.SQLitePath = filepath.Join(t.TempDir(), "perf.db")
			// SQLite serializes writes; it is a baseline, not a target, low floor.
			cfg.Workers = envInt("PERF_WORKERS", 8)
			cfg.PoolSize = envInt("PERF_POOL", 400)
			cfg.Label = "SQLite Mode A — baseline (not a target)"
			runAndAssert(t, cfg, 2.0)
		})
	}
}

// runAndAssert executes a run, writes the JSON + Markdown report, and asserts a
// sanity floor.
func runAndAssert(t *testing.T, cfg perf.Config, floorTPS float64) {
	t.Helper()
	ctx := context.Background()
	result, err := perf.Run(ctx, cfg, nil)
	require.NoError(t, err)
	require.NotNil(t, result)

	root := repoRoot(t)
	jsonPath := filepath.Join(root, "perf-results", perf.DefaultResultFilename(string(cfg.Backend), string(cfg.Mode), result.GeneratedAt))
	require.NoError(t, perf.WriteJSON(result, jsonPath))
	t.Logf("wrote %s", jsonPath)

	t.Logf("[%s] sustained %.1f TPS over %.0fs (%d ops); e2e p50=%.2fms p99=%.2fms; contention retries=%d",
		cfg.Backend, result.Throughput.SustainedTPS, result.Throughput.WindowSeconds, result.Throughput.TotalOps,
		result.Phases["e2e"].P50Ms, result.Phases["e2e"].P99Ms, result.Contention.ContentionRetries)

	require.Positive(t, result.Throughput.TotalOps, "the run must complete at least one op")
	require.GreaterOrEqual(t, result.Throughput.SustainedTPS, floorTPS,
		"sustained TPS %.1f below sanity floor %.1f (gross regression?)", result.Throughput.SustainedTPS, floorTPS)
}

func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok, "runtime.Caller failed")
	// this file is <root>/test/perf/perf_test.go
	return filepath.Clean(filepath.Join(filepath.Dir(file), "..", ".."))
}

func envDuration(key string, def time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			return d
		}
	}
	return def
}

func envInt(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
