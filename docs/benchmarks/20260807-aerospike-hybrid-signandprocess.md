# Benchmark: Aerospike + PostgreSQL hybrid (Mode B) (aerospike-hybrid, signandprocess mode)

_Aerospike + Postgres hybrid Mode B (split stores) — bounded perf run_

Generated: 2026-08-07 03:57:09 EDT

## Sustained throughput

| Metric | Value |
|---|---|
| **Sustained TPS** | **153.6** |
| Target TPS | 1000 |
| % of target | 15.4% |
| Total ops (measured) | 9215 |
| Measured window | 60.0s |

## Phase latency percentiles (ms)

| Phase | count | p50 | p95 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|
| end-to-end | 9215 | 388.15 | 631.44 | 800.38 | 1115.47 | 416.59 |

## Contention & errors

| Metric | Value |
|---|---:|
| Ops attempted | 11741 |
| Ops succeeded | 11677 |
| Claim-contention retries | 0 |
| Deadlock retries | 0 |
| Contention failures (retries exhausted) | 0 |
| Deadlock failures (retries exhausted) | 0 |
| Other errors | 64 |

## Async loop (monitor + SSE maturation)

| Metric | Value |
|---|---:|
| Monitor enabled | true |
| Auto-miner enabled | true |
| MINED frames emitted | 11677 |
| Actions total (sampled) | 11695 |
| Actions completed (mined) | 10000 |
| Maturation % | 100.0% |

## TPS over time (10s buckets)

| Window start (s) | TPS |
|---:|---:|
| 0 | 179.8 |
| 10 | 180.5 |
| 20 | 177.2 |
| 30 | 150.9 |
| 40 | 112.9 |
| 50 | 120.2 |
| 60 | 0.0 |

## Spendable pool value over time

| At (s) | Balance (sats) |
|---:|---:|
| 9 | 3996503089 |
| 19 | 3987800340 |
| 28 | 3986064983 |
| 38 | 3980336759 |
| 47 | 3980718587 |
| 56 | 3986270588 |
| 66 | 3986266873 |
| 75 | 3977180619 |

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
- Single-call mode: one CreateAction does create+sign+process+broadcast; only the end-to-end phase is timed (fewest round trips). Fewer round trips is only plausibly ~2x on the SQL/hybrid backends; on write-serialized SQLite it can be marginally slower.

