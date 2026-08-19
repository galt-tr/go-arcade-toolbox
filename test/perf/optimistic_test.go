//go:build perf

package perf_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/perf"
	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/perfprovider"
)

// TestPerf_PostgresOptimisticCeiling answers one question: with everything in
// the toolbox's favour, how many transactions per second can it create?
//
// "Everything in its favour" is made explicit rather than assumed, because a
// ceiling measured under quietly-unfavorable conditions is not a ceiling:
//
//   - A large pool of ready, mined, denominated coins (PERF_POOL, default
//     100,000). Large enough that the run never competes for the same coin and
//     never exhausts the pool inside the window.
//   - One input, one output, NO change — see internal/perf/optimistic.go for
//     the dust-floor arithmetic and TestOptimisticShape_OneInputNoChange in
//     pkg/storage for the assertion that it still holds. No change means
//     ProcessAction performs no Mint at all.
//   - An instant 202 from the in-process mock oracle, so broadcast cost is
//     effectively zero and the number is the toolbox's own.
//   - No chained spending: nothing waits for a coin to reach TierUnproven,
//     because no transaction spends another's change.
//   - No monitor daemon and no miner, so nothing competes for the connection
//     pool and no async maturation work runs.
//
// What remains is exactly the synchronous write path: claim a coin, persist the
// metadata, commit, sign, hand to the oracle, record the spend.
//
// The sweep exists because a single worker count cannot show a ceiling — only
// the shape of the curve distinguishes "we ran out of capacity" from "we ran out
// of concurrency". Results print as a table and each run's JSON lands in
// perf-results/.
//
//	go test -tags perf -run TestPerf_PostgresOptimisticCeiling -timeout 60m ./test/perf/...
//
// Seeding dominates start-up: the pool is minted in 500-output chunks, so
// 100,000 coins is ~200 InternalizeAction calls. Use PERF_POOL=10000 for a quick
// smoke of the harness itself.
func TestPerf_PostgresOptimisticCeiling(t *testing.T) {
	// max_connections must exceed the largest pool this sweep opens, or the
	// database becomes the limit instead of the thing being measured: the image
	// defaults to 100, and the 128-worker run (144 connections) failed with
	// ~17,800 connection resets before this was raised. It cannot be set from
	// inside the test — max_connections needs a restart, not a reload.
	pg := testenv.StartPostgres(t, testenv.WithPostgresServerArgs("max_connections=600"))

	workers := envIntSlice("PERF_WORKER_SWEEP", []int{32, 64, 128, 256})
	pool := envInt("PERF_POOL", 100_000)
	duration := envDuration("PERF_DURATION", 30*time.Second)
	warmup := envDuration("PERF_WARMUP", 8*time.Second)

	type row struct {
		workers int
		tps     float64
		p50     float64
		p95     float64
		p99     float64
		retries int64
		fails   int64
	}
	var rows []row

	for _, w := range workers {
		t.Run(fmt.Sprintf("workers=%d", w), func(t *testing.T) {
			cfg := perf.DefaultConfig(perfprovider.BackendPostgres)
			cfg.PostgresDSN = pg.IsolatedSchemaDSN(t)
			// signandprocess is one call and reports only an e2e phase. twostep
			// splits create from sign_process, which is what a per-stage timing
			// breakdown needs — same work, two timers.
			cfg.Mode = perf.Mode(envString("PERF_MODE", string(perf.ModeSignAndProcess)))
			cfg.Workers = w
			cfg.TargetTPS = 0 // ungated: measure the ceiling, do not pace to it
			cfg.Duration = duration
			cfg.Warmup = warmup
			cfg.PoolSize = pool
			cfg.Denomination = perf.OptimisticDenomination
			cfg.PaymentSats = perf.OptimisticPaymentSats
			cfg.Throughput = true // ClaimExact fast path over the denominated pool
			cfg.RunMonitor = false
			cfg.Mine = false
			cfg.MaxDBConns = envInt("PERF_MAX_DB_CONNS", w+16)

			// The pool must outlast the window: with no change output, every
			// transaction consumes exactly one coin and returns nothing. A run
			// that drains the pool stops measuring throughput and starts
			// measuring ErrNotEnoughFunds.
			require.Greater(t, pool, 0)

			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
			defer cancel()

			// The whole meaning of this number depends on the commit being
			// durable, and nothing in the harness sets that either way — it is
			// inherited from the server. An unasserted inheritance is exactly how
			// a relaxed-durability number gets published as a durable one, so
			// assert it rather than trusting the default.
			requireDurableCommit(t, cfg.PostgresDSN)

			res, err := perf.Run(ctx, cfg, nil)
			require.NoError(t, err)

			// A run that exhausted its pool is not a ceiling measurement — it is a
			// measurement of ErrNotEnoughFunds. The harness classifies that as
			// CONTENTION, not OtherErrors (see the notes in snapshot.go), so this
			// is the bucket that proves the pool outlasted the window.
			require.Zero(t, res.Contention.ContentionFails,
				"%d ops failed on funding. With one coin consumed per transaction and "+
					"no change returned, the pool drains at the op rate — raise "+
					"PERF_POOL above sustainedTPS × (warmup+duration)",
				res.Contention.ContentionFails)

			// OtherErrors carries one in-flight op per worker, cancelled when the
			// window closes; that is an artifact of stopping, not a failure. Anything
			// beyond that is real and would mean the write path is erroring under load.
			require.LessOrEqualf(t, res.Contention.OtherErrors, int64(w),
				"%d non-contention errors for %d workers. Up to one per worker is the "+
					"expected shutdown artifact; more than that means ops are failing "+
					"during the measured window", res.Contention.OtherErrors, w)

			e2e := res.Phases["e2e"]
			rows = append(rows, row{
				workers: w,
				tps:     res.Throughput.SustainedTPS,
				p50:     e2e.P50Ms, p95: e2e.P95Ms, p99: e2e.P99Ms,
				retries: res.Contention.ContentionRetries,
				fails:   res.Contention.ContentionFails,
			})

			out := filepath.Join(repoRoot(t), "perf-results",
				fmt.Sprintf("optimistic-postgres-%s-%dw.json", cfg.Mode, w))
			require.NoError(t, perf.WriteJSON(res, out))
			t.Logf("workers=%d sustained=%.1f TPS  p50=%.1fms p95=%.1fms p99=%.1fms  retries=%d",
				w, res.Throughput.SustainedTPS, e2e.P50Ms, e2e.P95Ms, e2e.P99Ms,
				res.Contention.ContentionRetries)
		})
	}

	t.Log("")
	t.Logf("OPTIMISTIC CREATE CEILING — PostgreSQL Mode A, pool=%d, 1-in/1-out, no change, instant 202", pool)
	t.Logf("%8s %12s %10s %10s %10s %10s", "workers", "TPS", "p50 ms", "p95 ms", "p99 ms", "retries")
	for _, r := range rows {
		t.Logf("%8d %12.1f %10.1f %10.1f %10.1f %10d",
			r.workers, r.tps, r.p50, r.p95, r.p99, r.retries)
	}
}

// envIntSlice parses a space-separated int list from the environment, so the
// sweep can be narrowed without editing the test (PERF_WORKER_SWEEP="64 256").
func envIntSlice(key string, def []int) []int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []int
	for _, f := range strings.Fields(v) {
		n, err := strconv.Atoi(f)
		if err != nil {
			return def
		}
		out = append(out, n)
	}
	if len(out) == 0 {
		return def
	}
	return out
}

// requireDurableCommit fails the test unless PostgreSQL is running with full
// per-commit durability.
//
// `synchronous_commit=off` is worth roughly 3.5x on this workload, so a
// throughput figure captured with it relaxed is a different measurement wearing
// the same label. Both settings are server-side and neither is set by this
// suite, which means the number's meaning rests on a default nobody restated —
// the kind of assumption that survives right up until someone tunes a container
// and silently triples the published ceiling.
func requireDurableCommit(t *testing.T, dsn string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var syncCommit, fsync string
	require.NoError(t, db.QueryRow("SHOW synchronous_commit").Scan(&syncCommit))
	require.NoError(t, db.QueryRow("SHOW fsync").Scan(&fsync))

	require.Equalf(t, "on", syncCommit,
		"synchronous_commit=%s: this run would NOT be measuring durable commits, "+
			"and the result must not be reported as a durable ceiling", syncCommit)
	require.Equalf(t, "on", fsync,
		"fsync=%s: the server is not flushing to disk, so this is not a durable "+
			"ceiling", fsync)
	t.Logf("durability verified: synchronous_commit=%s fsync=%s", syncCommit, fsync)
}

// envString reads a string override from the environment.
func envString(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// TestPerf_PostgresGroupCommit sweeps PostgreSQL's group-commit and WAL settings
// against the optimistic shape, holding everything else at the configuration
// that produced the measured peak.
//
// Why this test exists: a transaction costs THREE durable commits — one closing
// CreateAction (create.go), and two inside ProcessAction (processNewTx, then
// applyAcceptedBroadcast after the broadcast returns). At ~13 ms per durable
// commit that is roughly 44% of the 87.8 ms end-to-end median, and `LWLock:WALInsert`
// was already measured at 57.4% of active backends at pool saturation. Group
// commit attacks exactly that: commit_delay holds a committing transaction
// briefly so concurrent commits share one fsync.
//
// It is the one lever named repeatedly across the docs as the path to 1000+ TPS
// durably and never once tried — no code, no test, no result. It is also
// config-only and costs no durability, which is why it is measured before any
// of the invasive alternatives.
//
//	go test -tags perf -run TestPerf_PostgresGroupCommit -timeout 90m ./test/perf/...
//
// Each configuration needs its own container: these are server GUCs, and
// commit_delay/wal_buffers cannot be changed on a running server the way a
// session setting can. Budget ~2.5 minutes per configuration, most of it seeding.
func TestPerf_PostgresGroupCommit(t *testing.T) {
	pool := envInt("PERF_POOL", 100_000)
	workers := envInt("PERF_WORKERS", 384)
	duration := envDuration("PERF_DURATION", 30*time.Second)
	warmup := envDuration("PERF_WARMUP", 8*time.Second)

	// Held identical to the 20260818 optimistic run so the baseline row is
	// directly comparable to its 1,096-1,140 TPS band. Changing the pool size or
	// the window here would make the whole sweep uncomparable to that report.
	configs := []struct {
		name string
		args []string
	}{
		{"baseline", nil},
		{"commit_delay=50", []string{"commit_delay=50"}},
		{"commit_delay=100", []string{"commit_delay=100"}},
		{"commit_delay=200", []string{"commit_delay=200"}},
		{"commit_delay=500", []string{"commit_delay=500"}},
		{"commit_delay=1000", []string{"commit_delay=1000"}},
		// commit_delay only engages once commit_siblings other transactions are
		// active. At 384 workers the default 5 should always be met; lowering it
		// tests that assumption rather than trusting it.
		{"commit_delay=200,siblings=2", []string{"commit_delay=200", "commit_siblings=2"}},
		// WAL capacity, independent of the delay.
		{"wal_buffers=64MB", []string{"wal_buffers=64MB"}},
		{"max_wal_size=32GB", []string{"max_wal_size=32GB"}},
	}
	if only := os.Getenv("PERF_GC_CONFIGS"); only != "" {
		want := map[string]bool{}
		for _, f := range strings.Fields(only) {
			want[f] = true
		}
		var kept []struct {
			name string
			args []string
		}
		for _, c := range configs {
			if want[c.name] {
				kept = append(kept, c)
			}
		}
		require.NotEmpty(t, kept, "PERF_GC_CONFIGS matched no configuration")
		configs = kept
	}

	type row struct {
		name string
		tps  float64
		p50  float64
		p99  float64
	}
	var rows []row

	for _, c := range configs {
		t.Run(c.name, func(t *testing.T) {
			args := append([]string{"max_connections=600"}, c.args...)
			pg := testenv.StartPostgres(t, testenv.WithPostgresServerArgs(args...))

			cfg := perf.DefaultConfig(perfprovider.BackendPostgres)
			cfg.PostgresDSN = pg.IsolatedSchemaDSN(t)
			cfg.Mode = perf.ModeSignAndProcess
			cfg.Workers = workers
			cfg.TargetTPS = 0
			cfg.Duration = duration
			cfg.Warmup = warmup
			cfg.PoolSize = pool
			cfg.Denomination = perf.OptimisticDenomination
			cfg.PaymentSats = perf.OptimisticPaymentSats
			cfg.Throughput = true
			cfg.RunMonitor = false
			cfg.Mine = false
			cfg.MaxDBConns = envInt("PERF_MAX_DB_CONNS", workers+16)

			// Group commit is only interesting because it keeps durability. A
			// sweep that silently relaxed it would just reproduce the known 3.5x
			// and prove nothing about commit_delay.
			requireDurableCommit(t, cfg.PostgresDSN)
			// And the flag has to have actually applied — a mistyped GUC would
			// otherwise produce a full sweep of identical baselines.
			logServerSettings(t, cfg.PostgresDSN, c.args)

			ctx, cancel := context.WithTimeout(context.Background(), 45*time.Minute)
			defer cancel()

			res, err := perf.Run(ctx, cfg, nil)
			require.NoError(t, err)
			require.Zero(t, res.Contention.ContentionFails,
				"%d ops failed on funding — the pool drained inside the window",
				res.Contention.ContentionFails)
			// commit_delay changes flush timing, not locking. Contention appearing
			// here would mean something other than the intended variable moved.
			require.Zero(t, res.Contention.ContentionRetries,
				"%d contention retries under %s: group commit should not affect locking",
				res.Contention.ContentionRetries, c.name)

			e2e := res.Phases["e2e"]
			rows = append(rows, row{c.name, res.Throughput.SustainedTPS, e2e.P50Ms, e2e.P99Ms})

			out := filepath.Join(repoRoot(t), "perf-results",
				fmt.Sprintf("groupcommit-%s.json", strings.NewReplacer("=", "", ",", "-").Replace(c.name)))
			require.NoError(t, perf.WriteJSON(res, out))
			t.Logf("%-28s sustained=%.1f TPS  p50=%.1fms  p99=%.1fms",
				c.name, res.Throughput.SustainedTPS, e2e.P50Ms, e2e.P99Ms)
		})
	}

	t.Log("")
	t.Logf("GROUP-COMMIT SWEEP — PostgreSQL Mode A, durable, %d workers, pool=%d", workers, pool)
	t.Logf("%-28s %12s %10s %10s", "config", "TPS", "p50 ms", "p99 ms")
	for _, r := range rows {
		t.Logf("%-28s %12.1f %10.1f %10.1f", r.name, r.tps, r.p50, r.p99)
	}
	t.Log("Baseline variance at 384 workers is ~4% (1140.0 vs 1096.2 measured 2026-08-18);")
	t.Log("treat anything inside that band as unchanged until it is re-run.")
}

// logServerSettings reports the GUCs this run actually got, so a typo in a
// server flag shows up as a logged default rather than as a sweep of
// indistinguishable results.
func logServerSettings(t *testing.T, dsn string, args []string) {
	t.Helper()
	db, err := sql.Open("pgx", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	for _, name := range []string{"commit_delay", "commit_siblings", "wal_buffers", "max_wal_size"} {
		var v string
		if err := db.QueryRow("SHOW " + name).Scan(&v); err != nil {
			t.Logf("SHOW %s: %v", name, err)
			continue
		}
		t.Logf("  %-16s = %s", name, v)
	}
	if len(args) > 0 {
		t.Logf("  requested: %s", strings.Join(args, " "))
	}
}
