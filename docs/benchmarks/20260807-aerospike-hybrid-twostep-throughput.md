# Benchmark: Aerospike + PostgreSQL hybrid (Mode B) (aerospike-hybrid, twostep mode)

**Funding path: throughput (ClaimExact fuel pool)**

_Aerospike + Postgres hybrid Mode B (split stores) — bounded perf run_

Generated: 2026-08-07 04:38:23 EDT

## Sustained throughput

| Metric | Value |
|---|---|
| **Sustained TPS** | **200.6** |
| Target TPS | 1000 |
| % of target | 20.1% |
| Total ops (measured) | 12033 |
| Measured window | 60.0s |

## Phase latency percentiles (ms)

| Phase | count | p50 | p95 | p99 | max | mean |
|---|---:|---:|---:|---:|---:|---:|
| create (fund+reserve+persist) | 12033 | 81.25 | 1048.81 | 1808.66 | 5440.67 | 238.31 |
| sign_process (sign+broadcast+commit) | 12033 | 66.57 | 150.47 | 401.48 | 1285.97 | 83.95 |
| end-to-end | 12033 | 157.29 | 1164.09 | 1988.26 | 5570.13 | 322.25 |

## Contention & errors

| Metric | Value |
|---|---:|
| Ops attempted | 12397 |
| Ops succeeded | 12333 |
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
| MINED frames emitted | 12333 |
| Actions total (sampled) | 12427 |
| Actions completed (mined) | 4966 |
| Maturation % | 49.7% |

## TPS over time (10s buckets)

| Window start (s) | TPS |
|---:|---:|
| 0 | 64.1 |
| 10 | 62.9 |
| 20 | 62.1 |
| 30 | 211.2 |
| 40 | 426.4 |
| 50 | 376.6 |
| 60 | 0.0 |

## Spendable pool value over time

| At (s) | Balance (sats) |
|---:|---:|
| 8 | 35998471139 |
| 16 | 35996956544 |
| 24 | 35996435835 |
| 33 | 35996925316 |
| 41 | 35933266023 |
| 49 | 35990951216 |
| 57 | 35959433628 |

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
| Funding path | throughput (ClaimExact fuel pool) |
| Workers | 64 |
| Target TPS | unbounded (max) |
| Duration | 60s (+5s warmup) |
| Pool size | 36000 coins |
| Denomination | 1000000 sats |
| Payment | 1000 sats |
| Max DB conns | 72 |
| Network | test |

## Notes

- FUEL-POOL PATH: the provider runs with UTXOManagement.Strategy=throughput, so each worker's wallet.CreateAction funds via the funder's closed-form ClaimExact fast path (FundArgs.Denomination>0) over a denominated pool — no tiered SKIP-LOCKED best-fit scan. This is the 1000-TPS design's funding route (Task 27 wiring).
- POOL BASKET = 'default' (a measurement choice, not the production layout): the pool must hold wallet-SIGNABLE coins so every op can sign+broadcast through the real wallet, and the only public API that mints BRC-29-signable coins (InternalizeAction wallet-payment) books them into the default basket. ClaimExact selects strictly by (basket, tier, satoshis==denomination), so the non-denominated change that also lands in 'default' is invisible to the fast path. A dedicated 'fuel' basket would need shaped-change minting (FanOutFuel/FuelShape) which storage does not yet implement — see the benchmark README gap analysis.
- NO RECYCLING: unlike the tiered path, each op's change is NOT re-claimed (it is not denomination-sized), so the pool strictly drains ~1 coin per op. The pool is sized to outlast warmup+duration; if ClaimExact ever underflowed it would fall back to the tiered walk over 'default' (visible as a contention/not-enough-funds spike). A clean run shows ~0 contention and ~0 not-enough-funds.
- Measures storage + wallet throughput: broadcasts hit the in-process mockarcade (202 instantly), not a live network.
- otherErrors is the residual bucket: write-path errors not matched as contention or deadlock — e.g. transient BEEF-assembly or reference/timeout errors, including the one in-flight op per worker interrupted at shutdown. Typically <0.5% of ops and not individually root-caused.
- The monitor daemon runs the real SSE apply pipeline; the auto-miner emits status-SSE MINED frames (with proof headers) so change matures unproven->mined through the async loop under load (best-effort: frames may drop when the pipeline is behind). The auto-miner does NOT advance the chaintracks tip stream.
- Two-step mode: 'create' = CreateAction (fund+reserve+persist); 'sign_process' = SignAction (sign+broadcast+commit). Finer broadcast/commit split is not separable at the wallet API boundary.

