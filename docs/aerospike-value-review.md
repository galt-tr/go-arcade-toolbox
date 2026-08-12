# Is Aerospike earning its keep?

**Reviewed 2026-08-12. Recommendation: no — plan to drop Mode B and run Postgres-only,
after one controlled A/B on the current rig confirms it.**

The question asked was "why are we using Aerospike at all, and could we move entirely to
Postgres and see the same performance values". The short answer is that the improvement
Aerospike has been credited with was delivered by something else, and the bottleneck it was
meant to relieve is one it structurally cannot relieve.

---

## 1. The structural argument (the important one)

**The metastore is always Postgres.** Mode B moves only the UTXO *inventory* to Aerospike;
every transaction still commits its metadata — `transactions`, `outputs`, `known_txs` — to
Postgres. So every operation pays a Postgres durable commit in *both* modes.

Our own sweep identifies that commit as the saturated resource. From
`docs/high-throughput-guide.md`:

> Both scale **sub-linearly** while e2e latency grows ~linearly with worker count — the
> signature of a saturated resource (the durable commit) where extra workers buy queueing,
> not throughput.

If the durable Postgres commit is what saturates, then relocating the UTXO inventory cannot
raise the ceiling — it removes work from a resource that was not the constraint. That is a
structural limit, not a tuning problem, and no amount of Aerospike capacity changes it.

It also explains why Mode B has to be *more* complex rather than less: splitting the stores
breaks the shared transaction, which is what the outbox exists to compensate for.

## 2. The win was `ClaimExact`, and it shipped on Postgres first

The claim-contention collapse (117,585 retries, 18.2% op-failure) was cured by the
`ClaimExact` fuel-pool path. The commit timeline is decisive:

| commit | time | what |
|---|---|---|
| `b185cf7` | 08-06 17:09 | `sqlstore` added — **already contains `ClaimExact`** |
| `6c896f6` | 08-07 02:35 | Aerospike provider + Mode B wiring |
| `8603bd7` | 08-07 03:36 | the perf harness that measured the contention |

`ClaimExact` landed on Postgres **9.5 hours before Aerospike existed**, and the harness that
measured the problem Aerospike supposedly solved landed **an hour after it**. Its own commit
message says it was built "per the approved plan §3.4" — it was plan-driven, not
measurement-driven, and was never validated as the fix.

`ClaimExact` is declared on `pkg/utxostore/store.go` — the interface — and the funder calls it
with zero backend branches, so **Mode A gets the identical fuel-pool fast path**.

The 319 ms → 81 ms create improvement often cited for Aerospike is a *tiered → fuel-pool*
delta measured within Mode B. Mode A made the same move 171 ms → 85 ms. Post-fix the two
converge at **81 vs 85 ms**. Aerospike's larger headline improvement is only because it
started from a worse place: on the tiered path it is *slower* than Postgres
(152.7 vs 203.7 TPS twostep; 153.6 vs 211.2 signandprocess — Postgres ahead by 33–37%).

## 3. What the controlled A/B actually shows

The durable, fuel-pool sweep with matched pool and connection counts:

| Backend | Workers | Sustained TPS | e2e p50 | e2e p99 |
|---|---:|---:|---:|---:|
| Postgres Mode A | 64 | **393.8** | 150 ms | **417 ms** |
| Aerospike Mode B | 64 | 382.7 | 149 ms | 474 ms |
| Postgres Mode A | 128 | 473.9 | 254 ms | **602 ms** |
| Aerospike Mode B | 128 | **489.7** | 228 ms | 736 ms |
| Postgres Mode A | 256 | 575.7 | 405 ms | **989 ms** |
| Aerospike Mode B | 256 | **645.6** | 349 ms | 1407 ms |

Aerospike's entire advantage is **+12% at 256 workers, bought with a 42% worse p99**. At 64
workers Postgres wins outright on both throughput and tail. And the guide's own advice is not
to run past 256 workers, because "pushing past 256 only inflates the tail" — so the +12% sits
exactly in the region we are told not to operate in.

Meanwhile the best sustained figure anywhere in the corpus is **pure Postgres at 2,045 TPS**
(`20260807-122926`, 61,367 ops in 30 s, 64 workers, `maxDbConns=48`) — a different, lighter
configuration, so not a like-for-like entry in the table above, but it does establish that
Postgres alone has already run well past the ~1,350 TPS knee we measured on this rig.

## 4. What Mode B costs

- **~2,500 lines retired** by dropping it: `aerostore` is 2,173 non-test LOC, plus
  `metastore/outbox.go` (124) and the compensation paths that exist *solely* because split
  stores cannot share a transaction. `releaseDirect` (Mode A) is **17 lines**;
  `releaseViaOutbox` (Mode B) is **45**, plus a durable queue, 8 `DrainOutbox` call sites, a
  table and a partial index — on the same Postgres it was meant to relieve.
- **`claim_cache.go` (190 lines, two mutexes, single-flight, generation counter) exists purely
  to route around the Aerospike client's per-node query lock** — the lock that once capped us
  at ~600 TPS with 281 of 704 goroutines parked in it. Mode A never had that wall.
- **Community Edition has no durable deletes.** Our own tests record it: "durable delete auto
  disabled on Community", `ENTERPRISE_ONLY`. Deleted UTXO rows can reappear after a cold
  restart — for a wallet that is a double-spend hazard. Production would need Enterprise.
- **`Balance` streams every record client-side** (~300k sindex entries) and froze the dashboard
  during a load run. Postgres does it as one indexed aggregate.
- **A funding footgun with no Mode A equivalent**: funding without `HIGH_THROUGHPUT=1` writes
  the coin to Postgres but not Aerospike, giving "not enough funds" forever against a balance
  that looks correct. A shared transaction makes this state unrepresentable.
- **~0.6–0.9 cores at idle** (measured from cgroup `usage_usec` over 5 s intervals).
  *Correction: I previously quoted "1.6 cores idle" from `podman stats`, which reports a
  lifetime average inflated by earlier load runs. The real idle cost is roughly half that, so
  this is a minor point, not the decisive one.*

## 5. Why the storage backend is not today's bottleneck anyway

Today's 1000→4000 TPS ladder put the knee at **~1,350 TPS, flat**, and it is host-bound:
load 127 on 32 cores, PSI cpu ~53%, **~40% of app CPU in secp256k1**, `database/sql` only
**~14%** of the block profile, and Aerospike essentially idle. arcade RECEIVED sat at 353–451
(the v0.12.0 8-partition sharding is working and is no longer the constraint).

Swapping the UTXO backend cannot move a ceiling set by signature verification on a saturated
host.

---

## Recommendation

1. **Default to Mode A (Postgres-only).** `./scripts/reset.sh --postgres-only` in
   `toolbox-app-arcade` selects it without hand-editing env.
2. **Run one controlled A/B on the current rig before deleting anything**, because every number
   above predates arcade v0.12.0 and this host's current state. Control for:
   - **`MAX_DB_CONNS` asymmetry** — in Mode A that pool serves workers + monitor **+ the
     utxostore**; in Mode B only workers + monitor. Sizing both the same starves Mode A. Sweep it.
   - **Host CPU accounting** — compare app-TPS-per-host-core with Aerospike's container share
     broken out, not raw TPS.
   - **Identical `synchronous_commit`** — a ~3.5× lever that dwarfs the backend delta.

   The switch is `-backend postgres` vs `-backend aerospike-hybrid`; mode is auto-detected from
   whether the utxostore shares the metastore's `*sql.DB` (`provider.go:372`), so the comparison
   is honest by construction.
3. **If Mode A lands within ~10% at equal worker counts, delete `aerostore` and the outbox.**
   The ~2,500 LOC and the CE fund-safety hazard settle it.

## What this review does *not* say

It does not say Aerospike is a bad database, nor that the Teranode utxostore should change.
Teranode's use is a different shape: Aerospike *is* the whole store there, with no
relational metastore sharing a transaction, so nothing forces a Postgres commit per operation.
Our Mode B keeps that commit and adds a second store beside it, which is what gives away the
structural advantage while keeping the operational cost.

The 100k+ TPS ambition is not unreachable *because of Postgres* — it is unreachable while every
transaction takes a durable relational commit and 40% of CPU goes to secp256k1. Getting there
is a batching, sharding and signature-verification problem, not a key-value-store problem.
