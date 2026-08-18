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
