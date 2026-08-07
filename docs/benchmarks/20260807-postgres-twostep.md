# Benchmark: PostgreSQL (Mode A) (postgres, twostep mode)

_Postgres Mode A (shared SQL) — bounded perf run_

Generated: 2026-08-07 03:52:31 EDT

## Sustained throughput

| Metric | Value |
|---|---|
| **Sustained TPS** | **203.7** |
| Target TPS | 1000 |
| % of target | 20.4% |
| Total ops (measured) | 12224 |
| Measured window | 60.0s |

## Phase latency percentiles (ms)

| Phase | count | p50 | p95 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|
| create (fund+reserve+persist) | 12224 | 171.18 | 562.45 | 909.41 | 2182.31 | 212.39 |
| sign_process (sign+broadcast+commit) | 12224 | 62.42 | 168.48 | 505.26 | 1269.26 | 79.92 |
| end-to-end | 12224 | 249.71 | 672.72 | 1029.61 | 2243.07 | 292.31 |

## Contention & errors

| Metric | Value |
|---|---:|
| Ops attempted | 17230 |
| Ops succeeded | 16856 |
| Claim-contention retries | 10534 |
| Deadlock retries | 0 |
| Contention failures (retries exhausted) | 310 |
| Deadlock failures (retries exhausted) | 0 |
| Other errors | 56 |

## Async loop (monitor + SSE maturation)

| Metric | Value |
|---|---:|
| Monitor enabled | true |
| Auto-miner enabled | true |
| MINED frames emitted | 16856 |
| Actions total (sampled) | 16872 |
| Actions completed (mined) | 9972 |
| Maturation % | 99.7% |

## TPS over time (10s buckets)

| Window start (s) | TPS |
|---:|---:|
| 0 | 210.4 |
| 10 | 168.6 |
| 20 | 145.5 |
| 30 | 129.1 |
| 40 | 278.2 |
| 50 | 290.6 |
| 60 | 0.0 |

## Spendable pool value over time

| At (s) | Balance (sats) |
|---:|---:|
| 9 | 3989800340 |
| 19 | 3994398557 |
| 28 | 3981581680 |
| 38 | 3987056237 |
| 47 | 3986692815 |
| 56 | 3984493452 |
| 66 | 3972540390 |

## Environment

| Field | Value |
|---|---|
| Host | fedora |
| CPU | 13th Gen Intel(R) Core(TM) i9-13900K |
| Logical cores | 32 |
| RAM | 62.6 GiB |
| OS / Arch | linux/amd64 |
| Kernel | 7.1.5-100.fc43.x86_64 |
| Go | go1.26.3 |
| Podman | podman version 5.8.4 |

## Run configuration

| Knob | Value |
|---|---|
| Workers | 64 |
| Target TPS | unbounded (max) |
| Duration | 60s (+15s warmup) |
| Pool size | 4000 coins |
| Denomination | 1000000 sats |
| Payment | 1000 sats |
| Max DB conns | 72 |
| Network | test |

## Notes

- TIERED PATH ONLY: these numbers measure the bounded tiered (privacy) funding path. Plain wallet.CreateAction always funds from the change basket with Denomination=0; the denominated fuel-pool ClaimExact fast path that the 1000-TPS design targets is NOT YET wired to CreateAction (tracked as a follow-up) and is expected to be substantially higher. Do not read this sustained TPS as the design ceiling — the Aerospike hybrid here already shows 0 claim contention.
- Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.
- Recycling is implicit: each payment's change re-enters the change basket ('default') and is re-selected by subsequent claims (mined-first, then unproven). This exercises real claim contention.
- Contention counts are HIGH-VARIANCE run to run (SKIP-LOCKED collisions depend on scheduling); observed anywhere from ~0 to tens of thousands of retries at near-identical config. Do not over-read a single run's contention figure.
- otherErrors is the residual bucket: write-path errors not matched as contention (contention/conflict/not-enough-funds/insufficient) or deadlock (deadlock/serialization/40001/40P01) — e.g. transient BEEF-assembly or reference/timeout errors, including ops interrupted at shutdown. Typically <0.5% of ops here and not individually root-caused.
- The monitor daemon runs the real SSE apply pipeline; the auto-miner emits status-SSE MINED frames (with proof headers) so change matures unproven->mined through the async loop under load (best-effort: frames may drop when the pipeline is behind). The auto-miner does NOT advance the chaintracks tip stream.
- Two-step mode: 'create' = CreateAction (fund+reserve+persist); 'sign_process' = SignAction (sign+broadcast+commit). Finer broadcast/commit split is not separable at the wallet API boundary.

