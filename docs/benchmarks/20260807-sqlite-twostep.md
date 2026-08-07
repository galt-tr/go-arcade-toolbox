# Benchmark: SQLite (Mode A, baseline) (sqlite, twostep mode)

_SQLite Mode A — baseline (not a target)_

Generated: 2026-08-07 03:58:08 EDT

## Sustained throughput

| Metric | Value |
|---|---|
| **Sustained TPS** | **62.2** |
| Target TPS | 1000 |
| % of target | 6.2% |
| Total ops (measured) | 1867 |
| Measured window | 30.0s |

## Phase latency percentiles (ms)

| Phase | count | p50 | p95 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|
| create (fund+reserve+persist) | 1867 | 20.78 | 194.85 | 555.73 | 2046.36 | 52.76 |
| sign_process (sign+broadcast+commit) | 1867 | 4.02 | 130.39 | 533.71 | 2037.99 | 30.71 |
| end-to-end | 1867 | 32.67 | 346.26 | 760.26 | 2287.15 | 83.46 |

## Contention & errors

| Metric | Value |
|---|---:|
| Ops attempted | 3059 |
| Ops succeeded | 2921 |
| Claim-contention retries | 3042 |
| Deadlock retries | 0 |
| Contention failures (retries exhausted) | 130 |
| Deadlock failures (retries exhausted) | 0 |
| Other errors | 8 |

## Async loop (monitor + SSE maturation)

| Metric | Value |
|---|---:|
| Monitor enabled | true |
| Auto-miner enabled | true |
| MINED frames emitted | 2921 |
| Actions total (sampled) | 2924 |
| Actions completed (mined) | 2375 |
| Maturation % | 81.2% |

## TPS over time (10s buckets)

| Window start (s) | TPS |
|---:|---:|
| 0 | 64.7 |
| 10 | 65.9 |
| 20 | 56.1 |
| 30 | 0.0 |

## Spendable pool value over time

| At (s) | Balance (sats) |
|---:|---:|
| 5 | 595344783 |
| 10 | 597824074 |
| 14 | 598469462 |
| 19 | 598208598 |
| 24 | 593934487 |
| 29 | 596546248 |
| 33 | 597286403 |

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
| Workers | 8 |
| Target TPS | unbounded (max) |
| Duration | 30s (+8s warmup) |
| Pool size | 600 coins |
| Denomination | 1000000 sats |
| Payment | 1000 sats |
| Max DB conns | 16 |
| Network | test |

## Notes

- TIERED PATH ONLY: these numbers measure the bounded tiered (privacy) funding path. Plain wallet.CreateAction always funds from the change basket with Denomination=0; the denominated fuel-pool ClaimExact fast path that the 1000-TPS design targets is NOT YET wired to CreateAction (tracked as a follow-up) and is expected to be substantially higher. Do not read this sustained TPS as the design ceiling — the Aerospike hybrid here already shows 0 claim contention.
- Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.
- Recycling is implicit: each payment's change re-enters the change basket ('default') and is re-selected by subsequent claims (mined-first, then unproven). This exercises real claim contention.
- Contention counts are HIGH-VARIANCE run to run (SKIP-LOCKED collisions depend on scheduling); observed anywhere from ~0 to tens of thousands of retries at near-identical config. Do not over-read a single run's contention figure.
- otherErrors is the residual bucket: write-path errors not matched as contention (contention/conflict/not-enough-funds/insufficient) or deadlock (deadlock/serialization/40001/40P01) — e.g. transient BEEF-assembly or reference/timeout errors, including ops interrupted at shutdown. Typically <0.5% of ops here and not individually root-caused.
- The monitor daemon runs the real SSE apply pipeline; the auto-miner emits status-SSE MINED frames (with proof headers) so change matures unproven->mined through the async loop under load (best-effort: frames may drop when the pipeline is behind). The auto-miner does NOT advance the chaintracks tip stream.
- Two-step mode: 'create' = CreateAction (fund+reserve+persist); 'sign_process' = SignAction (sign+broadcast+commit). Finer broadcast/commit split is not separable at the wallet API boundary.

