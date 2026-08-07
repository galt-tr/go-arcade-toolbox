package perf

import (
	"fmt"
	"strings"
)

// Render turns a RunResult into a Markdown benchmark report.
func Render(r *RunResult) string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Benchmark: %s (%s, %s mode)\n\n", title(r.Backend), r.Backend, r.Mode)
	if r.Label != "" {
		fmt.Fprintf(&b, "_%s_\n\n", r.Label)
	}
	fmt.Fprintf(&b, "Generated: %s\n\n", r.GeneratedAt.Format("2006-01-02 15:04:05 MST"))

	// Headline.
	fmt.Fprintf(&b, "## Sustained throughput\n\n")
	fmt.Fprintf(&b, "| Metric | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| **Sustained TPS** | **%.1f** |\n", r.Throughput.SustainedTPS)
	fmt.Fprintf(&b, "| Target TPS | %.0f |\n", r.Throughput.TargetTPS)
	fmt.Fprintf(&b, "| %% of target | %.1f%% |\n", r.Throughput.PctOfTarget)
	fmt.Fprintf(&b, "| Total ops (measured) | %d |\n", r.Throughput.TotalOps)
	fmt.Fprintf(&b, "| Measured window | %.1fs |\n", r.Throughput.WindowSeconds)
	fmt.Fprintf(&b, "\n")

	// Phase latencies.
	fmt.Fprintf(&b, "## Phase latency percentiles (ms)\n\n")
	fmt.Fprintf(&b, "| Phase | count | p50 | p95 | p99 | max | mean |\n|---|---:|---:|---:|---:|---:|---:|\n")
	for _, key := range []string{"create", "sign_process", "e2e"} {
		ps, ok := r.Phases[key]
		if !ok || ps.Count == 0 {
			continue
		}
		fmt.Fprintf(&b, "| %s | %d | %.2f | %.2f | %.2f | %.2f | %.2f |\n",
			phaseLabel(key), ps.Count, ps.P50Ms, ps.P95Ms, ps.P99Ms, ps.MaxMs, ps.MeanMs)
	}
	fmt.Fprintf(&b, "\n")

	// Contention.
	c := r.Contention
	fmt.Fprintf(&b, "## Contention & errors\n\n")
	fmt.Fprintf(&b, "| Metric | Value |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Ops attempted | %d |\n", c.Attempted)
	fmt.Fprintf(&b, "| Ops succeeded | %d |\n", c.Succeeded)
	fmt.Fprintf(&b, "| Claim-contention retries | %d |\n", c.ContentionRetries)
	fmt.Fprintf(&b, "| Deadlock retries | %d |\n", c.DeadlockRetries)
	fmt.Fprintf(&b, "| Contention failures (retries exhausted) | %d |\n", c.ContentionFails)
	fmt.Fprintf(&b, "| Deadlock failures (retries exhausted) | %d |\n", c.DeadlockFails)
	fmt.Fprintf(&b, "| Other errors | %d |\n", c.OtherErrors)
	fmt.Fprintf(&b, "\n")

	// Monitor / async loop.
	m := r.Monitor
	fmt.Fprintf(&b, "## Async loop (monitor + SSE maturation)\n\n")
	fmt.Fprintf(&b, "| Metric | Value |\n|---|---:|\n")
	fmt.Fprintf(&b, "| Monitor enabled | %v |\n", m.Enabled)
	fmt.Fprintf(&b, "| Auto-miner enabled | %v |\n", m.Mining)
	fmt.Fprintf(&b, "| MINED frames emitted | %d |\n", m.MinedEmitted)
	fmt.Fprintf(&b, "| Actions total (sampled) | %d |\n", m.ActionsTotal)
	fmt.Fprintf(&b, "| Actions completed (mined) | %d |\n", m.ActionsCompleted)
	fmt.Fprintf(&b, "| Maturation %% | %.1f%% |\n", m.MaturationPct)
	fmt.Fprintf(&b, "\n")

	// TPS over time.
	if len(r.Throughput.Buckets) > 0 {
		fmt.Fprintf(&b, "## TPS over time (%.0fs buckets)\n\n", r.Throughput.BucketWidthS)
		fmt.Fprintf(&b, "| Window start (s) | TPS |\n|---:|---:|\n")
		for _, bk := range r.Throughput.Buckets {
			fmt.Fprintf(&b, "| %.0f | %.1f |\n", bk.StartSec, bk.TPS)
		}
		fmt.Fprintf(&b, "\n")
	}

	// Pool depth.
	if len(r.PoolDepth) > 0 {
		fmt.Fprintf(&b, "## Spendable pool value over time\n\n")
		fmt.Fprintf(&b, "| At (s) | Balance (sats) |\n|---:|---:|\n")
		for _, ps := range r.PoolDepth {
			fmt.Fprintf(&b, "| %.0f | %d |\n", ps.AtSec, ps.BalanceSats)
		}
		fmt.Fprintf(&b, "\n")
	}

	// Environment.
	e := r.Environment
	fmt.Fprintf(&b, "## Environment\n\n")
	fmt.Fprintf(&b, "| Field | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Host | %s |\n", e.Hostname)
	fmt.Fprintf(&b, "| CPU | %s |\n", e.CPUModel)
	fmt.Fprintf(&b, "| Logical cores | %d |\n", e.LogicalCores)
	fmt.Fprintf(&b, "| RAM | %.1f GiB |\n", float64(e.RAMBytes)/(1<<30))
	fmt.Fprintf(&b, "| OS / Arch | %s/%s |\n", e.OS, e.Arch)
	fmt.Fprintf(&b, "| Kernel | %s |\n", e.Kernel)
	fmt.Fprintf(&b, "| Go | %s |\n", e.GoVersion)
	if e.PodmanVersion != "" {
		fmt.Fprintf(&b, "| Podman | %s |\n", e.PodmanVersion)
	}
	fmt.Fprintf(&b, "\n")

	// Config.
	cfg := r.Config
	fmt.Fprintf(&b, "## Run configuration\n\n")
	fmt.Fprintf(&b, "| Knob | Value |\n|---|---|\n")
	fmt.Fprintf(&b, "| Workers | %d |\n", cfg.Workers)
	fmt.Fprintf(&b, "| Target TPS | %s |\n", tpsLabel(cfg.TargetTPS))
	fmt.Fprintf(&b, "| Duration | %.0fs (+%.0fs warmup) |\n", cfg.DurationSec, cfg.WarmupSec)
	fmt.Fprintf(&b, "| Pool size | %d coins |\n", cfg.PoolSize)
	fmt.Fprintf(&b, "| Denomination | %d sats |\n", cfg.Denomination)
	fmt.Fprintf(&b, "| Payment | %d sats |\n", cfg.PaymentSats)
	if cfg.MaxDBConns > 0 {
		fmt.Fprintf(&b, "| Max DB conns | %d |\n", cfg.MaxDBConns)
	}
	fmt.Fprintf(&b, "| Network | %s |\n", cfg.Network)
	fmt.Fprintf(&b, "\n")

	// Notes.
	if len(r.Notes) > 0 {
		fmt.Fprintf(&b, "## Notes\n\n")
		for _, n := range r.Notes {
			fmt.Fprintf(&b, "- %s\n", n)
		}
		fmt.Fprintf(&b, "\n")
	}

	return b.String()
}

func phaseLabel(key string) string {
	switch key {
	case "create":
		return "create (fund+reserve+persist)"
	case "sign_process":
		return "sign_process (sign+broadcast+commit)"
	case "e2e":
		return "end-to-end"
	default:
		return key
	}
}

func title(backend string) string {
	switch backend {
	case "postgres":
		return "PostgreSQL (Mode A)"
	case "aerospike-hybrid":
		return "Aerospike + PostgreSQL hybrid (Mode B)"
	case "sqlite":
		return "SQLite (Mode A, baseline)"
	default:
		return backend
	}
}

func tpsLabel(t float64) string {
	if t <= 0 {
		return "unbounded (max)"
	}
	return fmt.Sprintf("%.0f", t)
}
