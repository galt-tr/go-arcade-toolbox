# Optimistic create ceiling — PostgreSQL, durable (2026-08-18)

**Question:** with everything in the toolbox's favor, how many transactions per
second can it *create*? Not "what does a realistic payment cost" — that is the
2026-08-07 sweep — but the ceiling of the synchronous write path itself.

**Answer: ~1,118 TPS at 384 workers, durable**, on a curve that turns over at
512. Roughly double the realistic-payment figure, and the gap is workload, not
tuning.

Produced by `TestPerf_PostgresOptimisticCeiling` (`test/perf/optimistic_test.go`).

```sh
PERF_POOL=100000 PERF_DURATION=30s PERF_WARMUP=8s PERF_WORKER_SWEEP="32 64 128 256 384 512" \
  go test -tags perf -run TestPerf_PostgresOptimisticCeiling -timeout 60m ./test/perf/...
```

## Conditions

Every one of these is enforced by the test rather than assumed:

| Condition | How |
|---|---|
| 100,000 ready mined coins | `PERF_POOL`; `ContentionFails == 0` asserts the pool outlasted the window |
| One input | `ClaimExact` closed form lands on n=1 — asserted by `TestOptimisticShape_OneInputNoChange` |
| **No change output** | Remainder falls below the dust floor and is donated. See `internal/perf/optimistic.go` for the arithmetic |
| Instant 202 | In-process mockarcade |
| No chained spending | Nothing waits for `TierUnproven`; no transaction spends another's change |
| No monitor daemon | `RunMonitor=false`, `Mine=false` — nothing competes for the connection pool |
| **Durable** | `synchronous_commit=on` and `fsync=on` verified by query inside every run |

No change means `ProcessAction` performs **no `Mint` at all**, and the pool
drains exactly one coin per transaction.

## Results

i9-13900K, 32 logical cores, PostgreSQL 17 in rootless podman, 30 s window after
an 8 s warmup, `signandprocess` mode, ungated rate.

| Workers | Sustained TPS | e2e p50 | e2e p95 | e2e p99 | Contention retries |
|---:|---:|---:|---:|---:|---:|
| 32 | 306.0 | 88.1 ms | 159.3 ms | 531.9 ms | 0 |
| 64 | 503.1 | 110.6 ms | 208.7 ms | 338.1 ms | 0 |
| 128 | 798.1 | 141.1 ms | 271.6 ms | 496.2 ms | 0 |
| 256 | 1036.9 | 218.0 ms | 421.8 ms | 714.7 ms | 0 |
| **384** | **1140.0 / 1096.2** | 295.7 / 308.0 ms | 723.5 / 549.8 ms | 962.3 / 1651.1 ms | 0 |
| 512 | 1097.6 | 397.5 ms | 920.0 ms | 1501.9 ms | 0 |

384 was run twice; the spread (1140.0 vs 1096.2, ~4%) is this rig's run-to-run
variance and the honest error bar on every other row.

**512 is slower than 384.** That turnover is what makes this a ceiling rather
than an unfinished ramp — past the peak the extra concurrency buys latency, not
throughput.

**Zero contention retries at every point, including 512.** With a pool this size
the claim path never competes; whatever limits throughput here, it is not the
lock.

## Per-stage timing (`twostep`, so create and sign_process are timed separately)

At 32 workers, low enough that these read as service times rather than queue:

| Phase | p50 | p95 | p99 | mean |
|---|---:|---:|---:|---:|
| create | 29.18 ms | 55.15 ms | 102.14 ms | 35.78 ms |
| sign_process | 56.70 ms | 119.48 ms | 229.67 ms | 69.99 ms |
| end-to-end | 87.75 ms | 158.68 ms | 341.51 ms | 105.77 ms |
| `ClaimExact` (query + index only) | 0.087 ms | — | — | — |

The same split at 384 workers is create 126.2 / sign_process 204.4 / e2e 349.4 —
same ratio, inflated by queue.

**`sign_process` costs about twice `create`.** There are two durable commits per
transaction, one closing each call, plus real secp256k1 signing between them.
This contradicts the 2026-08-07 gap analysis, which named the create phase the
wall; on this shape it is the cheaper half.

## Pool size: 1,000,000 coins

| Pool | Workers | TPS | p50 | p99 | Contention retries |
|---:|---:|---:|---:|---:|---:|
| 100,000 | 384 | 1096–1140 | ~300 ms | 0.96–1.65 s | 0 |
| 1,000,000 | 384 | 1043.6 | 283.2 ms | 2121.0 ms | 0 |

About **5% lower** with p99 more than doubling. Small enough not to change the
planning number, real enough not to call it noise — and notably NOT free, which
the flat claim benchmark (87 µs at both 1k and 250k) would have predicted. The
claim itself stays free; this is buffer and index pressure from a larger table.

## `max_connections` is the first limit you hit

The 128-worker run initially failed with **17,773 connection resets** and killed
its container. PostgreSQL's default `max_connections` is 100; the run opened 144
(`MaxDBConns = workers + 16`).

**Every figure above 64 workers requires raising it**, and it cannot be changed
at runtime — it needs a server restart, which is why `testenv.WithPostgresServerArgs`
exists. In a deployment this binds well before anything in the toolbox does, and
it presents as connection errors rather than as a throughput limit.

## Why this is double the 2026-08-07 figure

Same code. The earlier sweep reached ~575 TPS at 256 workers because it carried a
change output (a `Mint` per transaction), ran the monitor daemon against the same
pool, and funded from 4,000 coins. Removing those is worth roughly 2×.

The fsync is still the wall — it is why 384 concurrent workers are needed to
reach 1,118 — but it amortizes considerably further than the earlier sweep
suggested.

## Limits of this data

- The broadcaster is an in-process mock. This is the toolbox's own cost with the
  network removed, not an end-to-end promise.
- One box, one payment shape. Multi-output actions, larger BEEF ancestry and
  chained spending are all unmeasured and all move these numbers.
- Group commit (`commit_delay` / `commit_siblings`) is the obvious next lever
  against the peak and has still not been tested.
