package perf

import (
	"context"
	"time"

	sdk "github.com/bsv-blockchain/go-sdk/wallet"
)

// txStatusCompleted is the wdk.TxStatus string for a proof-verified action.
const txStatusCompleted = "completed"

const bucketWidth = 10 * time.Second

// snapshot assembles the final RunResult from the collector and config.
func snapshot(ctx context.Context, cfg Config, st *stack, coll *collector, measureStart, measureEnd time.Time) *RunResult {
	all := coll.mergedSamples()
	measured := make([]opSample, 0, len(all))
	var createMs, signMs, e2eMs []float64
	for _, s := range all {
		if s.doneAt.Before(measureStart) || s.doneAt.After(measureEnd) {
			continue
		}
		measured = append(measured, s)
		if s.createNs > 0 {
			createMs = append(createMs, nsToMs(s.createNs))
		}
		if s.signNs > 0 {
			signMs = append(signMs, nsToMs(s.signNs))
		}
		if s.e2eNs > 0 {
			e2eMs = append(e2eMs, nsToMs(s.e2eNs))
		}
	}

	window := measureEnd.Sub(measureStart).Seconds()
	if window <= 0 {
		window = 1
	}
	sustained := float64(len(measured)) / window
	pctTarget := sustained / TargetTPS * 100

	phases := map[string]PhaseStats{"e2e": phaseStatsFromMillis(e2eMs)}
	if len(createMs) > 0 {
		phases["create"] = phaseStatsFromMillis(createMs)
	}
	if len(signMs) > 0 {
		phases["sign_process"] = phaseStatsFromMillis(signMs)
	}

	c := &coll.counters
	result := &RunResult{
		SchemaVersion: SchemaVersion,
		GeneratedAt:   time.Now(),
		Label:         cfg.Label,
		Backend:       string(cfg.Backend),
		Mode:          string(cfg.Mode),
		Environment:   captureEnvironment(),
		Config: ConfigEcho{
			Workers:      cfg.Workers,
			TargetTPS:    cfg.TargetTPS,
			DurationSec:  cfg.Duration.Seconds(),
			WarmupSec:    cfg.Warmup.Seconds(),
			PoolSize:     cfg.PoolSize,
			Denomination: cfg.Denomination,
			PaymentSats:  cfg.PaymentSats,
			MaxDBConns:   cfg.MaxDBConns,
			Network:      string(cfg.Network),
		},
		Throughput: Throughput{
			SustainedTPS:  round3(sustained),
			TotalOps:      len(measured),
			WindowSeconds: round3(window),
			TargetTPS:     TargetTPS,
			PctOfTarget:   round3(pctTarget),
			BucketWidthS:  bucketWidth.Seconds(),
			Buckets:       bucketTPS(measured, measureStart, measureEnd, bucketWidth),
		},
		Phases: phases,
		Contention: Contention{
			Attempted:         c.attempted.Load(),
			Succeeded:         c.succeeded.Load(),
			ContentionRetries: c.contentionRetries.Load(),
			DeadlockRetries:   c.deadlockRetries.Load(),
			ContentionFails:   c.contentionFails.Load(),
			DeadlockFails:     c.deadlockFails.Load(),
			OtherErrors:       c.otherErrors.Load(),
		},
		Monitor: monitorStats(ctx, cfg, st, c),
		Notes:   notes(cfg),
	}

	for _, ps := range coll.pool {
		result.PoolDepth = append(result.PoolDepth, poolSampleJSON{
			AtSec:       round3(ps.atSec),
			BalanceSats: ps.balanceSats,
		})
	}

	return result
}

// monitorStats gathers async-loop stats: tip height and a sampled maturation
// ratio (fraction of persisted actions promoted to completed by the SSE
// pipeline). Best-effort — failures leave fields zero.
func monitorStats(ctx context.Context, cfg Config, st *stack, c *counters) MonitorStats {
	m := MonitorStats{
		Enabled:      cfg.RunMonitor,
		Mining:       cfg.Mine,
		MinedEmitted: c.minedEmitted.Load(),
	}
	limit := uint32(10000)
	res, err := st.wallet.ListActions(ctx, sdk.ListActionsArgs{
		Labels: []string{},
		Limit:  &limit,
	}, originator)
	if err != nil || res == nil {
		return m
	}
	m.ActionsTotal = res.TotalActions
	var completed uint32
	for _, a := range res.Actions {
		if string(a.Status) == txStatusCompleted {
			completed++
		}
	}
	m.ActionsCompleted = completed
	if n := len(res.Actions); n > 0 {
		m.MaturationPct = round3(float64(completed) / float64(n) * 100)
	}
	return m
}

// notes documents the harness model + any run-specific caveats in the result.
func notes(cfg Config) []string {
	out := []string{
		"TIERED PATH ONLY: these numbers measure the bounded tiered (privacy) funding path. Plain wallet.CreateAction always funds from the change basket with Denomination=0; the denominated fuel-pool ClaimExact fast path that the 1000-TPS design targets is NOT YET wired to CreateAction (tracked as a follow-up) and is expected to be substantially higher. Do not read this sustained TPS as the design ceiling — the Aerospike hybrid here already shows 0 claim contention.",
		"Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.",
		"Recycling is implicit: each payment's change re-enters the change basket ('default') and is re-selected by subsequent claims (mined-first, then unproven). This exercises real claim contention.",
		"Contention counts are HIGH-VARIANCE run to run (SKIP-LOCKED collisions depend on scheduling); observed anywhere from ~0 to tens of thousands of retries at near-identical config. Do not over-read a single run's contention figure.",
		"otherErrors is the residual bucket: write-path errors not matched as contention (contention/conflict/not-enough-funds/insufficient) or deadlock (deadlock/serialization/40001/40P01) — e.g. transient BEEF-assembly or reference/timeout errors, including ops interrupted at shutdown. Typically <0.5% of ops here and not individually root-caused.",
	}
	if cfg.RunMonitor {
		out = append(out, "The monitor daemon runs the real SSE apply pipeline; the auto-miner emits status-SSE MINED frames (with proof headers) so change matures unproven->mined through the async loop under load (best-effort: frames may drop when the pipeline is behind). The auto-miner does NOT advance the chaintracks tip stream.")
	}
	switch cfg.Mode {
	case ModeTwoStep:
		out = append(out, "Two-step mode: 'create' = CreateAction (fund+reserve+persist); 'sign_process' = SignAction (sign+broadcast+commit). Finer broadcast/commit split is not separable at the wallet API boundary.")
	case ModeSignAndProcess:
		out = append(out, "Single-call mode: one CreateAction does create+sign+process+broadcast; only the end-to-end phase is timed (fewest round trips). Fewer round trips is only plausibly ~2x on the SQL/hybrid backends; on write-serialized SQLite it can be marginally slower.")
	}
	return out
}
