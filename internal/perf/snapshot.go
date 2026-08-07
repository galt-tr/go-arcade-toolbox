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
			FundingPath:  fundingPath(cfg),
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

// fundingPath names the funding route the run exercised, for the report header.
func fundingPath(cfg Config) string {
	if cfg.Throughput {
		return "throughput (ClaimExact fuel pool)"
	}
	return "tiered (privacy)"
}

// notes documents the harness model + any run-specific caveats in the result.
func notes(cfg Config) []string {
	var out []string
	if cfg.Throughput {
		out = []string{
			"FUEL-POOL PATH: the provider runs with UTXOManagement.Strategy=throughput, so each worker's wallet.CreateAction funds via the funder's closed-form ClaimExact fast path (FundArgs.Denomination>0) over a denominated pool — no tiered SKIP-LOCKED best-fit scan. This is the 1000-TPS design's funding route (Task 27 wiring).",
			"POOL BASKET = 'default' (a measurement choice, not the production layout): the pool must hold wallet-SIGNABLE coins so every op can sign+broadcast through the real wallet, and the only public API that mints BRC-29-signable coins (InternalizeAction wallet-payment) books them into the default basket. ClaimExact selects strictly by (basket, tier, satoshis==denomination), so the non-denominated change that also lands in 'default' is invisible to the fast path. A dedicated 'fuel' basket is now supported via shaped-change minting (FanOutFuel/FuelShape, implemented in storage.CreateAction); the perf harness still books the pool in 'default' as a measurement convenience to avoid the fan-out step.",
			"NO RECYCLING: unlike the tiered path, each op's change is NOT re-claimed (it is not denomination-sized), so the pool strictly drains ~1 coin per op. The pool is sized to outlast warmup+duration; if ClaimExact ever underflowed it would fall back to the tiered walk over 'default' (visible as a contention/not-enough-funds spike). A clean run shows ~0 contention and ~0 not-enough-funds.",
			"Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.",
			"otherErrors is the residual bucket: write-path errors not matched as contention or deadlock — e.g. transient BEEF-assembly or reference/timeout errors, including the one in-flight op per worker interrupted at shutdown. Typically <0.5% of ops and not individually root-caused.",
		}
	} else {
		out = []string{
			"TIERED PATH: these numbers measure the bounded tiered (privacy) funding path — plain wallet.CreateAction funding from the change basket with Denomination=0. Compare against the sibling -throughput run for the denominated fuel-pool ClaimExact fast path.",
			"Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.",
			"Recycling is implicit: each payment's change re-enters the change basket ('default') and is re-selected by subsequent claims (mined-first, then unproven). This exercises real claim contention.",
			"Contention counts are HIGH-VARIANCE run to run (SKIP-LOCKED collisions depend on scheduling); observed anywhere from ~0 to tens of thousands of retries at near-identical config. Do not over-read a single run's contention figure.",
			"otherErrors is the residual bucket: write-path errors not matched as contention (contention/conflict/not-enough-funds/insufficient) or deadlock (deadlock/serialization/40001/40P01) — e.g. transient BEEF-assembly or reference/timeout errors, including ops interrupted at shutdown. Typically <0.5% of ops here and not individually root-caused.",
		}
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
