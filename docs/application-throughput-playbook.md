# Application throughput playbook

This is the guide for the **application author**: you are writing a program on
`go-arcade-toolbox` that must sustain hundreds to thousands of transactions per
second, and you want to know what to do, in what order, and what will bite you.

It is the companion to the [high-throughput guide](high-throughput-guide.md),
which is written from the *library's* point of view — the measured single-node
ceilings, the funding-path comparison, the durability lever. Read that one for
the numbers. Read this one for the decisions you make in your own code. Nothing
here restates its benchmark tables.

Every claim below is traceable to code (`path:line`) or to a measured number in
[`docs/benchmarks`](benchmarks/README.md). Where a number comes from our own
work rather than a published benchmark it says so and gives the conditions.

Two applications are used as running examples:

- **The payment-shaped reference app** — `toolbox-app-arcade`'s self-payment
  blaster, which produced most of the cluster benchmarks in `docs/benchmarks`.
  Its measured peak is ~1,600–1,900 TPS sustained create with zero failures, and
  the benchmark verdict is that the ceiling was **arcade-side propagation, not
  the toolbox** (`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:118-131`).
  Its README is stale for throughput — it predates `internal/blaster`,
  `HIGH_THROUGHPUT`, Aerospike and ~20 tuning environment variables. Trust its
  `internal/config/config.go` and the benchmark reports, not its README.
- **The Rule 110 covenant app** — 128 covenant-protected UTXO chains, each an
  unbroken self-spending chain of ~4 KB transactions with custom locking
  scripts, spent through the two-step `CreateAction` → `SignAction` path. It is
  the hard case, and it surfaced several things the payment-shaped app cannot.

## What actually limits you, in order

Work down this list. Each item can hide the one below it, so fixing them out of
order wastes time.

1. **The shape of your own transaction graph.** If transaction N+1 spends N's
   output, that chain's rate is `1/latency` and no amount of concurrency
   changes it. If the chain never confirms, it eventually hits the node's
   mempool ancestor limit and the rejection cascades to every descendant.
2. **What you hand the library per call.** An application that passes its own
   accumulated ancestry as `InputBEEF` grows its storage by one transaction per
   link, forever. This is the single most expensive mistake available to you and
   it is invisible until the database is tens of gigabytes.
3. **The transaction's shape and price.** A dropped sub-dust change output
   breaks any covenant that commits to the output count. The default 100 sat/kB
   fee rate leaves no margin against a validator that prices the extended
   format, so a transaction that looks correctly funded comes back as a final
   4xx.
4. **The durable commit.** The single-node ceiling documented in the
   [high-throughput guide](high-throughput-guide.md#the-durability-tradeoff-the-biggest-lever).
   This is where most tuning advice starts and it is the fourth thing that binds
   you, not the first.
5. **The status pipeline.** Apply concurrency, the callback token, and the SSE
   fan-out. Get these wrong and transactions are created fine but never reach a
   known status, which looks exactly like the write path failing.
6. **The network.** Arcade's single-partition propagation topic caps intake at
   ~1.6–1.9k TPS (`benchmarks/20260810-postfix-validation-and-partition-ceilings.md:16-21`),
   and its SSE fan-out at ~1,600 events/s
   (`benchmarks/20260811-1000tps-45min-transition-timing.md:289-294`). You cannot
   tune past these from the application.

## The checklist, ordered by leverage

| # | Do this | Why | Where |
|---|---|---|---|
| 0 | Run the monitor daemon | Not optional. Change becomes claimable only when a `SEEN` status is applied, and only the monitor applies it | [Wiring](#7-wiring-the-library-for-throughput) |
| 1 | Prefer many independent coins over one long chain | A serial chain's rate is `1/latency`; parallelism across chains buys TPS, never per-chain rate | [Shape the workload](#1-shape-the-workload-before-you-tune-anything) |
| 2 | Govern unconfirmed chain depth explicitly | Past the mempool ancestor limit the deepest tx is rejected **and it cascades to every descendant** | [Chain depth](#12-unconfirmed-chain-depth-vs-the-mempool-ancestor-limit) |
| 3 | Never pass or store atomic BEEF of your own chain | Grows one transaction per link, forever. Measured 1,194 kB per 4 KB transaction, still climbing | [Bound the BEEF](#2-bound-what-you-hand-the-library) |
| 4 | `storage.WithRequiredChangeOutput()` if your scripts commit to output shape | The funder silently drops sub-dust change and donates it to the miner | [Output shape](#31-the-dust-floor-and-withrequiredchangeoutput) |
| 5 | Set the fee rate to 125 sat/kB, not the default 100 | Arcade's validator prices the **extended-format** size, which the toolbox's fee arithmetic does not count; the default leaves no margin | [Fee rate](#33-fee-rate-125-not-100) |
| 6 | Never retry a 4xx; always retry a 503 | 4xx is a durable terminal rejection; 503 means the tx was not queued | [Broadcast outcomes](#34-4xx-is-final-503-is-backpressure) |
| 7 | Barrier-free worker pool + token bucket; retry `ErrNotEnoughFunds` | Self-throttles to the fuel supply rate instead of counting failures | [The load loop](#5-the-load-loop) |
| 8 | `Strategy=throughput` + a sized fuel pool + the keeper | Removes claim contention entirely (0 retries, deterministically) | [Fuel](#6-fuel-the-throughput-pool-end-to-end) |
| 9 | `wallet.WithThroughputMode(true)` | Removes two serialization points on the shared BEEF-party mutex from the create hot path | [Wiring](#71-walletwiththroughputmodetrue) |
| 10 | `monitor.WithApplyConcurrency(n)` well above the default 8 | At ~1000 TPS the default cannot drain; the SSE reader blocks and events are lost | [Wiring](#72-monitorwithapplyconcurrencyn) |
| 11 | `headers.WithCacheDepth(0)` | The ~thousand proofs sharing one block do one header fetch instead of one each | [Wiring](#73-headerswithcachedepth0) |
| 12 | Derive the arcade callback token | Untokenized clients get no mid-stream catchup. Measured cost: 23,745 stranded transactions | [Wiring](#75-derive-the-callback-token) |
| 13 | `MaxDBConns ≈ workers + margin` | The pool serves your workers **and** the monitor | [Wiring](#76-perfprovidernew-and-maxdbconns) |
| 14 | `synchronous_commit=off` if your durability posture allows | A measured 3.5× | [Wiring](#77-durability) |
| 15 | Divide by measured elapsed, not a nominal tick | A dropped ticker inflates reported throughput by exactly the overrun factor | [Measure honestly](#8-measure-honestly) |

---

## 1. Shape the workload before you tune anything

### 1.1 Per-chain latency, not parallelism, sets a serial chain's rate

If transaction N+1 spends an output of transaction N, that chain is strictly
serial. Its ceiling is:

```
per-chain TPS = 1 / (round-trip latency of one create+sign+broadcast)
```

No number of workers changes it. Workers buy you *chains*, not depth:

```
total TPS = number of independent chains ÷ per-transaction latency
```

Concretely, with the measured create latency from the 1000-TPS run — create
total p50 134.7 ms including sign and persist
(`benchmarks/20260811-1000tps-45min-transition-timing.md:102`) — a single serial
chain tops out near **7 TPS**. To reach 512 TPS you need roughly
`512 × 0.135 ≈ 70` independent chains, and to reach 1000 TPS roughly 135. Our
Rule 110 app runs 128 chains for exactly this reason: the automaton's width is
also its concurrency budget.

The same identity governs the client side of a closed-loop generator, where it
is written as `workers ÷ RTT`. At the measured ceiling the reference blaster ran
512 workers against a ~280 ms `POST /tx` RTT and plateaued at ~1,830/s — which
is what it measured (`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:126`).
A requested TPS is only an upper bound; what you actually get is
`min(requested, workers ÷ RTT)`.

### 1.2 Unconfirmed chain depth vs the mempool ancestor limit

This is the limit nobody documents for application authors, and it is the one
that ends runs.

Every node bounds how deep a chain of *unconfirmed* transactions it will accept
into its mempool. Your depth per chain is:

```
unconfirmed depth ≈ per-chain tx rate × block interval
```

At 4 transactions per second per chain and a 10-minute block interval that is
2,400 deep. Nothing you do locally prevents the node rejecting it.

**What happens when you cross it is worse than a rejection.** The 2026-08-08
benchmark recorded arcade rejecting the deepest transaction with
`ProcessTransaction (4): failed to validate` and the rejection **cascading to
every descendant** with `parent rejected (ancestor …)`
(`benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:106-118`). At
~1,450 TPS this produced ~480 rejections per minute, steady and cascading; the
same workload with an independent, shallow-ancestry pool produced **zero**
(`benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:138-142`).

Two library changes came out of that, and you should understand both because
they constrain your design:

- **Promote-on-SEEN.** Change is promoted to claimable only on the real `SEEN`
  status, never on the 202 — a 202 is acceptance for processing, not validation.
  A rejected parent never SEENs, so its change never becomes claimable and no
  child can spend a dead output
  (`benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:121-126`). The
  consequence for you: **pool replenishment is coupled to SEEN-apply
  throughput**, so a lagging status pipeline shows up as fuel starvation.
- **`Throughput.RecycleChangeToPool` defaults off.** Routing a payment's change
  back into the fuel pool makes the pool self-replenish 1:1, which bounds the
  local ledger — but it also chains every payment onto the previous one's
  change. The field doc is explicit
  (`pkg/defs/utxo_management.go:96-105`):

  ```go
  // RecycleChangeToPool routes a payment's change straight back into the fuel
  // pool so it self-replenishes 1:1 (bounding the ledger and removing the
  // keeper's per-payment recycle). This chains each payment onto the previous
  // one's change, so it is SAFE ONLY when the chain confirms fast enough to
  // keep unconfirmed ancestry within the network's mempool ancestor limit —
  // i.e. mining keeps pace. On a backlogged chain a sustained self-payment
  // blast would grow the unconfirmed chain past that limit and be rejected, so
  // this defaults OFF (change goes to the default basket; the keeper feeds the
  // pool from confirmed deposits, keeping ancestry shallow).
  ```

**Our own measurement.** The Rule 110 app sustained **139-deep unconfirmed
chains with zero rejections** on the `dev-ovh-1` scale network: 128 parallel
chains, ~4 KB transactions with custom locking scripts, run continuously against
arcade v0.11.6. That is a data point about one network's configuration at one
moment, not a portable constant — treat it as evidence that a few hundred is
survivable there, not as a limit you can rely on elsewhere.

**Guidance.**

- Prefer **wide and shallow**: many independent coins, each spent once. This is
  what every clean benchmark used.
- If a chain is inherent to the workload (a covenant, a state machine, an
  append-only log), **govern the depth explicitly**. Track the confirmed depth
  per chain and stop advancing a chain that is too far ahead of its last mined
  ancestor. Treat that stop as backpressure, exactly like an empty fuel pool —
  not as an error.
- Do not assume you will get a clean signal. Backlogged transactions **skip
  SEEN entirely**, going straight `RECEIVED → MINED` when a block sweeps them
  (`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:148-150`), so a
  consumer gating on SEEN sees those coins as unpropagated right up until they
  are mined.

### 1.3 Two ceilings, not one

Intake and settlement are different limits and they bind at different times.
Intake is what you feel during a run; settlement is what you feel afterwards.
The 2026-08-10 run separated them cleanly: propagation pegged at its CPU limit
during the blast, then went idle while the backlog drained in block-quantized
steps (`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:136-146`). If
your application's correctness depends on transactions being *mined* rather than
merely *accepted*, size against settlement, not intake.

---

## 2. Bound what you hand the library

### 2.1 Never pass or store the atomic BEEF of your own chain

An atomic BEEF contains the whole ancestry of the transaction it wraps. For an
application that spends its own outputs in a chain, that ancestry is the entire
history of the chain, and it grows by one transaction per link **forever**.

This is the single most expensive mistake available to an application author,
and it does not announce itself: throughput degrades slowly, the database grows,
and nothing fails.

**Our own measurement, Rule 110 app, before and after.** 128 chains of ~4 KB
transactions with custom locking scripts, PostgreSQL metastore, run
continuously. The BEEF grew about **11 kB per chain per generation** and was
never going to stop:

| | passing atomic BEEF | storing the parent raw tx and rewrapping |
|---|---:|---:|
| `known_txs.input_beef`, mean per transaction | **1,194 kB** and still climbing | **8.1 kB** |
| Application state file | 315 MB at 63 generations | **1.05 MB, flat** |
| PostgreSQL total | 12 GB after 8,000 transactions | — |
| Projection to 100k generations | **~19 PB** | **~51 GB** |
| Per-generation wall time | 2.1 s and climbing | **0.9 s, steady** |

The fix is to store only the **parent's raw transaction** — not a BEEF, not an
ancestry — and rewrap it into a fresh BEEF for each `CreateAction`. The 8.1 kB
that remains is two transactions, because each step spends its own previous
output *and* a funding coin; storage clears the column entirely once the
transaction is mined ([§2.2](#22-why-that-is-safe)).

### 2.2 Why that is safe

Four properties of the arcade-only design make a single-generation BEEF
sufficient. Each is worth checking yourself before you rely on it.

1. **Storage only looks up the direct source.** `hydrateInputs`
   (`pkg/storage/process.go:513`) attaches source transactions so scripts can be
   verified and the extended format can be built. It does a flat lookup, not a
   walk:

   ```go
   func (p *Provider) hydrateInputs(tx *transaction.Transaction, beefBytes []byte) error {
   	if len(beefBytes) == 0 {
   		return nil
   	}
   	beef, err := transaction.NewBeefFromBytes(beefBytes)
   	if err != nil {
   		return fmt.Errorf("storage: parse input beef: %w", err)
   	}
   	for _, in := range tx.Inputs {
   		if in.SourceTransaction != nil || in.SourceTXID == nil {
   			continue
   		}
   		if src := beef.FindTransactionByHash(in.SourceTXID); src != nil {
   			in.SourceTransaction = src
   		}
   	}
   	return nil
   }
   ```

2. **Arcade validates from the inline prevout.** The broadcast is extended
   format, which carries each input's source satoshis and locking script inline.
   Deep ancestry is BRC-62 verifier completeness the arcade-only model does not
   require — the same reasoning `WithDirectInputBEEF` is built on
   (`pkg/storage/provider.go:240-246`).

3. **The create path never verifies BEEF.** `VerifyBeef` has exactly one
   non-test caller in the whole library: `InternalizeAction`
   (`pkg/storage/internalize.go:52`), where the trust anchor genuinely matters
   because you are recording someone else's payment. Nothing on the create path
   calls it.

4. **Storage drops `input_beef` on MINED.** Once a proof anchors the
   transaction its ancestry is dead weight, and both copies are cleared —
   `known_txs` inside `SetProof`
   (`pkg/storage/internal/metastore/knowntx.go:471-483`) and the `transactions`
   row explicitly (`pkg/storage/status_updates.go:255-262`):

   ```go
   // The proof now anchors the tx, so its input BEEF ancestry is dead weight
   // (see SetProof, which drops the known_txs copy). Drop the transactions
   // copy too — the dominant blob under sustained load.
   if err := p.meta.Transactions().ClearInputBEEFByTxID(ctx, txid); err != nil {
   	return fmt.Errorf("storage: mined: clear input beef: %w", err)
   }
   ```

   So the steady state of a healthy deployment is `raw_tx` alone. If your
   `input_beef` column is growing without bound, either your transactions are
   not being mined or you are handing in ancestry you do not need.

### 2.3 The rewrap recipe, and the API trap

The obvious call does not work. `transaction.NewBeefFromTransaction` on a bare
parsed transaction fails, because it calls
`collectAncestors(txid, txns, false)` — `allowPartial` false — which errors on
the first input whose `SourceTransaction` is nil
(`go-sdk@v1.3.3/transaction/beef.go:288` and `:472-478`):

```go
for _, input := range t.Inputs {
	if input.SourceTransaction == nil {
		if allowPartial {
			continue
		} else {
			return nil, fmt.Errorf("missing previous transaction for %s", t.TxID())
		}
	}
```

A transaction you just parsed from stored bytes has no `SourceTransaction` on
any input, so this always fails. Build the BEEF explicitly instead:

```go
// parentRaw is the parent transaction's raw bytes — the only thing the
// application stores per chain. Not a BEEF, not an ancestry.
beef := transaction.NewBeefV2()
if _, err := beef.MergeRawTx(parentRaw, nil); err != nil {
	return nil, fmt.Errorf("merge parent raw tx: %w", err)
}
inputBEEF, err := beef.Bytes()
if err != nil {
	return nil, fmt.Errorf("serialize input beef: %w", err)
}
```

Pass `inputBEEF` as `CreateActionArgs.InputBEEF`. It contains exactly one
transaction and its size is constant in the length of the chain.

### 2.4 The trap: `WithDirectInputBEEF` does not save you

`storage.WithDirectInputBEEF()` (`pkg/storage/provider.go:247`) is the right
setting for a high-throughput arcade-only deployment and you should set it. But
be precise about what it does: **it bounds what storage assembles, not what you
hand in.**

It is read at exactly one place — inside storage's own recursive builder
(`pkg/storage/beef.go:94-96`), where it stops the walk into the ancestry of the
coins *storage* allocated. Your `args.InputBEEF` is parsed independently
(`pkg/storage/create.go:229`) and becomes the *base* that the assembled BEEF is
merged into, and the whole thing is then persisted verbatim into
`transactions.input_beef` (`pkg/storage/create.go:229-241, 254-265`):

```go
base, err := p.parseProvidedBEEF(args.InputBEEF)
if err != nil {
	return nil, err
}
allocTxids := allocatedTxids(fundRes.AllocatedUTXOs)
beef, err := p.getBEEFForTxIDs(ctx, allocTxids, base, stringsFrom(args.Options.KnownTxids))
```

So `WithDirectInputBEEF` cannot shrink a bloated `InputBEEF` you supplied. Both
sides have to be bounded, and only you can bound your side.

### 2.5 Budget the metadata write amplification

Even with a bounded BEEF, `known_txs` is the most update-heavy table in the
workload: created once, rewritten on every status transition. The live 1000-TPS
runs measured **roughly 18 row-writes per created transaction**, and a table at
1.54M live rows had grown to 1,978 MB of heap
(`pkg/storage/internal/metastore/migrations/postgres/00005_known_txs_fillfactor.sql:1-6`).
The library ships `fillfactor = 70` for this: in the measured A/B the table ends
**44% smaller after identical churn**, at the cost of a 40% larger cold size. If
you are sizing storage for a long run, budget from that measurement, not from
your transaction size. Note it is a catalog-only change — existing pages keep
their packing until rewritten, so on an already-bloated table the benefit
arrives gradually unless you `VACUUM FULL` out of band.

---

## 3. Get the transaction's shape and price right

### 3.1 The dust floor and `WithRequiredChangeOutput`

The funder stops collecting coins the moment they cover the outputs plus the
fee, and keeps the remainder as change **only if it clears the dust floor**.
Below that, the remainder is donated to the miner and **no change output is
produced at all** (`pkg/storage/internal/funder/collector.go:208-215`):

```go
// If the change amount is below the dust floor, it is uneconomical to create any change output.
// Discard all change outputs and give the amount as extra fee to the miner.
if changeAmount < c.dustFloor {
	c.changeOutputsCount = 0
}
```

The floor is twice the fee of a minimal future spend
(`pkg/storage/internal/funder/collector.go:72-76`):

```go
minSpendTxSize := txutils.TransactionSizeFromScriptLengths(
	[]uint64{txutils.P2PKHUnlockingScriptLength},
	[]uint64{txutils.P2PKHLockingScriptLength},
)
c.dustFloor = satoshi.Value(math.Max(1, math.Ceil(float64(minSpendTxSize)/1000*feeCalculator.value)*2))
```

With `P2PKHUnlockingScriptLength = 107` and `P2PKHLockingScriptLength = 25`
(`pkg/internal/txutils/inputs_outputs_sizes.go:7,9`) that is a 192-byte minimal
spend, so `ceil(192/1000 × 100) × 2` = **40 satoshis at the default 100 sat/kB**
(`pkg/defs/fee_model.go:37-42`).

That is the right trade for an ordinary payment and the wrong one for you if
your unlocking scripts commit to the transaction's shape. A covenant that
reconstructs `[continuation, change]` inside its sighash preimage **cannot be
satisfied by a one-output transaction**, so the action has to be abandoned. The
failure mode is nasty in three ways:

- it is **intermittent**, because it depends on where coin selection happens to
  land;
- it is **silent** — the output count simply changes, nothing errors at
  construction time;
- it gets **worse over time**, because each transition returns its change as a
  smaller coin, so a pool grinds down and the totals land inside the sub-dust
  window more often.

We hit this in the Rule 110 app and added the fix upstream. Set it:

```go
storage.WithRequiredChangeOutput()
```

The option (`pkg/storage/provider.go:139-163`) makes the funder keep allocating
until the change clears the dust floor, and report `ErrNotEnoughFunds` if it
cannot (`pkg/storage/internal/funder/collector.go:129-140`). Enable it whenever
a transaction's unlocking scripts depend on its output count; leave it off for
ordinary payments, where donating dust to the fee is the better trade.

### 3.2 Pinning the output count

If you need a *fixed* number of outputs rather than merely a non-zero change,
also pin `MaxChangeOutputsPerTx` (`pkg/defs/change_basket.go:7`, default **8**,
`:15`). The funder splits change into up to that many outputs
(`pkg/storage/internal/funder/collector.go:249-259`), so a covenant expecting
exactly `[continuation, change]` needs it set to 1.

The reference app does exactly this in throughput mode, for a different but
instructive reason (`toolbox-app-arcade/internal/walletsetup/walletsetup.go:258-270`):
splitting a self-payment's change into 8 sub-denomination coins produced coins
the fuel keeper could not re-aggregate into chunks without a giant many-input
transaction, stalling refills. One change output keeps recycled value in
fuel-sized coins.

```go
ppExtra = append(ppExtra, storage.WithChangeBasket(defs.ChangeBasket{
	NumberOfDesiredUTXOs:    1,
	MinimumDesiredUTXOValue: 1,
	MaxChangeOutputsPerTx:   1,
}))
```

**Know what pinning it costs you.** The change basket's default of 8
(`pkg/defs/change_basket.go:15`) exists to build a wide pool of spendable coins.
Pinning it to 1 means the change pool grows by one coin per transaction rather
than eight, so concurrent callers contend for coins on the tiered funding path.
If you must pin the output count *and* you need real concurrency, the
denominated fuel pool ([§6](#6-fuel-the-throughput-pool-end-to-end)) is the
answer — its coins are interchangeable, so the claim never collides.

Note that the fuel-pool denomination deliberately does **not** use the generic
dust floor: a fuel UTXO's spend fee is priced into its denomination by
construction, so the floor there is the marginal fuel-input fee
(`pkg/defs/utxo_management.go:185-191`).

### 3.3 Fee rate: 125, not 100

The toolbox's default fee model is 100 sat/kB (`pkg/defs/fee_model.go:36-42`).
Leaving it there gives you **zero margin**, and the thing the margin protects
you from is not what this section used to claim.

**Correction.** This section previously said arcade prices the extended-format
transaction, so a fee computed at 100 sat/kB over the standard size necessarily
falls below the floor. That is wrong, and it was measured to be wrong by driving
arcade's BDK validator directly: a transaction whose source locking script is
1000 bytes has a **73-byte standard** encoding and a **1090-byte extended**
encoding, and at 100 sat/kB it is accepted with a fee of **7** satoshis —
`floor(100 × 73 / 1000)`, not `floor(100 × 1090 / 1000) = 109`. The prevout
satoshis and source locking scripts that extended format carries inline are
handed to the validator as separate spent-coin data and are not billed
(`/git/arcade/validator/convert.go:22-45`). The node's arithmetic also
*truncates* where the toolbox's *rounds up*
(`pkg/storage/internal/funder/fee_calculator.go:50`), so at the same size the
toolbox pays at or above the floor.

**What actually bites.** The fee is committed during `CreateAction` from an
*estimate* of the transaction's size, made before the unlocking scripts exist,
and nothing ever rechecks it against the finished transaction. Every input whose
real unlocking script is longer than declared eats the margin directly — and an
input that declares neither an `unlockingScript` nor an `unlockingScriptLength`
is silently assumed to be a 107-byte P2PKH (`pkg/storage/create.go:587-591`). A
covenant input with a two-kilobyte unlocking script priced as 107 bytes
underpays by ~190 satoshis at 100 sat/kB.

So: **declare `unlockingScriptLength` on every caller-provided input and
over-estimate it**, and enable `storage.WithMinBroadcastFeeRate(100)`, which
measures the finished transaction and turns an underpayment into a local error
naming the shortfall instead of a remote 4xx. See
`docs/rejection-hardening-audit.md` finding 3.

Set **125 sat/kB** as well. That is the reference app's own default
(`toolbox-app-arcade/internal/config/config.go:198`) and every cluster benchmark
in `docs/benchmarks` that reached four figures ran `FEE_SAT_PER_KB=125` — see
`benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:13`,
`benchmarks/20260809-sse-delivery-and-mined-apply-fix-validation.md:30`,
`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:43`.

125 is a margin, not a computation — margin against your own size estimate being
wrong, which is the failure that is actually reachable.

### 3.4 4xx is final; 503 is backpressure

Get this distinction into your retry logic before you write anything else,
because retrying the wrong one is how a small problem becomes a large one. The
mapping is in `pkg/arcade/client.go:253-276` and documented at
`docs/arcade-integration.md:56-66`:

| HTTP | Meaning | Surfaced as | Retry? |
|---|---|---|---|
| 2xx (202) | Accepted into the pipeline with an early status. **Not** a verdict. | `*BroadcastResult{TxID, Status}` | n/a |
| **4xx** | Transaction-level rejection: **final and durable** (arcade persists a terminal `REJECTED` row). | `*BroadcastResult{Rejected: true}`, `err == nil` | **never** |
| **503** | Backpressure: the tx was **not** queued, so resubmitting is safe. Arcade sends `Retry-After: 1`. | `*BackpressureError{RetryAfter}` | yes, after the delay |
| ≥500 / transport | Opaque: the fate is unknown. | plain `error` | reconcile via `GetTx` first |

Two consequences that catch people:

- A 4xx returns `err == nil`. If your code only checks the error you will treat
  a permanent rejection as a success.
- A 202 is an intake receipt. `docs/architecture.md:100` puts it plainly: *a
  broadcast is an intake, not a verdict.* Do not build state on it. The
  library's own promote-on-SEEN change reflects exactly this.

---

## 4. The two-step `CreateAction` → `SignAction` path

If your inputs carry caller-supplied unlocking scripts — a covenant, a
hash-locked output, anything the wallet cannot sign for you — you use the
two-step form: `CreateAction` returns a signable transaction, you produce the
unlocking scripts, and `SignAction` completes it. Six constraints bite.

**`RandomizeOutputs` defaults to true.** The mapping layer defaults a nil
pointer to `true` (`pkg/wallet/internal/mapping/mapping_create_action_args.go:108`)
and storage shuffles the output plans before assigning vouts
(`pkg/storage/create.go:214-219`):

```go
if args.Options.RandomizeOutputs {
	p.rand.Shuffle(len(plans), func(i, j int) { plans[i], plans[j] = plans[j], plans[i] })
}
```

If output position matters — and it does for any covenant that reconstructs the
output set in its sighash preimage — you must pass a pointer to `false`.

**`InputBEEF` is not recovered for you.** Storage tries its own metastore
first, but an outpoint it does not already own must come from the BEEF you
supply, and it hard-errors otherwise
(`pkg/storage/create_inputs.go:26-68`): `storage: input %d (%s:%d) not found
locally and no input BEEF provided`.

**Custom outputs come back `Spendable=false`.** This is by construction, not a
bug. Caller outputs are planned as non-change, `OutputTypeCustom`,
`ProvidedByYou` (`pkg/storage/create.go:324-335`); only `Change == true` rows are
minted into the utxostore (`pkg/storage/process.go:157-180`); and `Spendable` is
not a stored column at all — it is computed from utxostore liveness
(`pkg/storage/outputs.go:219-235`, and `docs/storage.md`). An output that was
never minted is not found, so it reports false. Your application owns the
lifecycle of its own custom outputs; the wallet will not track them for you.

**Descriptions have a 5-byte minimum.** `String5to2000Bytes.Validate`
(`pkg/wdk/primitives/strings.go:16-24`) returns `at least 5 length`, surfaced as
`the description parameter must be at least 5 length`,
`inputDescription must be at least 5 length` and `outputDescription must be at
least 5 length` (`pkg/internal/validate/valid_create_action_args.go:62,122,138`).
It is a byte length, so `"ok"` fails and a 2-character emoji may pass. Set real
descriptions; they are cheap and they are what you will grep for later.

**`UnlockingScriptLength` is fee sizing only.** It feeds `providedInputSizes`
(`pkg/storage/create.go:584-595`) → `currentTxSize` (`create.go:107`) →
`FundArgs.CurrentTxSize` (`create.go:144`), and the real unlocking script is
deliberately stripped before it reaches storage
(`pkg/wallet/internal/mapping/mapping_create_action_args.go:70-71`). Getting the
number wrong costs you fee accuracy, not correctness — but an underestimate
becomes an underpaid transaction, which is a 4xx (see
[§3.3](#33-fee-rate-125-not-100)). Derive it from your actual code part and
preimage geometry rather than padding a guess; a large pad is a permanent
overpayment on every transaction you ever send.

**Storage runs the real script interpreter before broadcast — this is a
feature.** `VerifyScripts` (`pkg/storage/verifiers.go:76-92`) executes every
input's unlocking/locking pair through the go-sdk interpreter, and it is called
on the process path *before* the transaction is persisted or sent
(`pkg/storage/process.go:123`, `:237`). A wrong unlocking script therefore fails
locally with `storage: script verification failed for input N: …` instead of
becoming a network rejection you have to diagnose from a status code. Do not
route around it with `WithScriptsVerifier` unless you have a real reason.

Better still, run the interpreter yourself on the assembled transaction
*before* calling `SignAction`. It costs microseconds against a create path
measured in the hundreds of milliseconds, and it lets you report the failure
against your own object — which chain, which state, which rule — instead of
`script verification failed for input 0` several layers down. At a thousand
transactions a second, the diagnosis is the expensive part.

One era note: the default is Genesis rules. Covenants whose generated preamble
emits `OP_2MUL` (or `OP_2DIV`) need `storage.WithChronicleOpcodes()`
(`pkg/storage/provider.go:135`), which is off by default because it accepts
scripts a Genesis-rules node would reject
(`pkg/storage/provider.go:130-132`).

---

## 5. The load loop

### 5.1 A barrier-free worker pool fed by a token bucket

The reference blaster is the shape to copy. Long-lived workers range over a
shared channel; a producer admits work through a `golang.org/x/time/rate`
limiter. There is exactly one `WaitGroup.Wait()` per run, at shutdown
(`toolbox-app-arcade/internal/blaster/blaster.go:223-240`):

```go
jobs := make(chan struct{}, cfg.Workers)
var wg sync.WaitGroup
var failed atomic.Uint64
for i := 0; i < cfg.Workers; i++ {
	wg.Add(1)
	go func() {
		defer wg.Done()
		for range jobs {
			// context.Canceled is graceful shutdown (blast stopped /
			// duration elapsed), not a real failure — don't count it.
			if err := bl.sendOne(parent, cfg, now, &opCount, sampleAt, &sampled, traceEvery); err != nil && !errors.Is(err, context.Canceled) {
				...
			}
		}
	}()
}
```

and the producer (`blaster.go:242-257`):

```go
limiter := rate.NewLimiter(rate.Limit(cfg.TPS), 1)
for {
	if err := limiter.Wait(produceCtx); err != nil {
		break
	}
	select {
	case <-produceCtx.Done():
		close(jobs)
		wg.Wait()
		return
	case jobs <- struct{}{}:
	}
}
```

Four properties are load-bearing:

- **Burst 1.** `rate.NewLimiter(rate.Limit(cfg.TPS), 1)` forbids the limiter
  banking unused capacity during a stall and then releasing a thundering herd.
- **The limiter gates admission, not execution.** `Wait` is called in the
  producer; workers never rate-limit individually.
- **Workers get the parent context, not the duration-bounded one**
  (`blaster.go:233`). An expiring duration stops *scheduling* new work; it never
  cancels work in flight. The sibling HTTP runner says so explicitly
  (`toolbox-app-arcade/internal/buyer/buyer.go:175-177`).
- **No barrier.** Workers exit only when the channel closes.

### 5.2 Never use a per-batch `WaitGroup.Wait()`

A loop that dispatches a batch, waits for all of it, then dispatches the next
batch runs every batch at its **slowest member's** latency. With a p50 of 135 ms
and a p95 of 210 ms (`benchmarks/20260811-1000tps-45min-transition-timing.md:99-102`),
a 128-wide batch waits on the tail, so effective throughput is
`batch ÷ p_max` rather than `workers ÷ p50`, and the loss grows with batch width
because a wider batch samples further into the tail.

We built the Rule 110 app this way first — one barrier per generation, 128 cells
per batch — and it makes the automaton's rate the rate of its worst cell. The
replacement is one long-lived worker per chain fed from a monotonic target, so a
slow chain falls behind instead of holding every other chain up. Feed the pool;
let slow items overlap fast ones.

### 5.3 Retry `ErrNotEnoughFunds` — it is backpressure, not failure

An empty fuel pool is not an error. It is the fuel supply telling you your
demand exceeds it. If you count it as a failure you get a run that reports
thousands of failures and a throughput number that means nothing; if you retry
it you get a run that self-throttles to the sustainable supply rate with zero
errors.

The reference implementation (`toolbox-app-arcade/internal/blaster/blaster.go:143-179`):

```go
for {
	start := now()
	result, err := bl.wallet.CreateAction(callCtx, sdk.CreateActionArgs{...}, bl.originator)

	// Back-pressure: an empty fuel pool is not a failure. Wait for the
	// keeper to mint and retry the same payment so the blast self-throttles
	// to the sustainable fuel-supply rate (0 errors, sustained TPS = supply
	// rate). Do not record it as a completed op — it did no work.
	if errors.Is(err, wdk.ErrNotEnoughFunds) {
		bl.metrics.RecordBackpressure()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backpressureRetry):
			continue
		}
	}
	...
	bl.metrics.RecordResult(now().Sub(start), err)
	return err
}
```

Details that matter:

- Detection is `errors.Is` against the library sentinel `wdk.ErrNotEnoughFunds`
  (`pkg/wdk/errors.go:11-12`), never a string match.
- The pause is a flat 5 ms (`blaster.go:34-37`) — short enough that a worker
  resumes promptly once the keeper mints, long enough not to spin. Retries are
  unbounded in count; the only exit is context cancellation.
- `start` is re-taken **inside** the loop, so the fuel wait does not pollute
  your latency percentiles.
- Backpressure is counted in its own counter, published as its own field, and
  never mixed into failures
  (`toolbox-app-arcade/internal/metrics/metrics.go:236-239`). Benchmarks report
  them as separate columns for exactly this reason.

The same shape applies to *any* backpressure signal your application defines —
a chain that is too deep to advance, a rate you have chosen to cap. Count it,
retry it, keep it out of the failure number.

---

## 6. Fuel: the throughput pool end to end

The high-throughput guide covers *why* the fuel pool exists (it removes claim
contention: every throughput run shows exactly 0 contention retries,
deterministically). This section covers what you have to get right in your own
configuration.

### 6.1 Configure and validate

```go
um := defs.DefaultUTXOManagement()
um.Strategy = defs.StrategyThroughput
um.Throughput.ExpectedTxSizeBytes = 400        // signed size INCLUDING the fuel input
um.Throughput.ExpectedOutputSatoshis = 0       // pure fee/data action
um.Throughput.TargetTPS = 1000
um.Throughput.ExpectedConfirmationSeconds = 300
um.Throughput.SpendPolicy = defs.SpendPolicyPreferMined

if err := um.Validate(defs.DefaultFeeModel(), defs.Commission{}); err != nil {
	log.Fatalf("throughput config invalid: %v", err)
}
denom, _ := um.Throughput.Denomination(defs.DefaultFeeModel(), defs.Commission{})
```

- **`Denomination()`** (`pkg/defs/utxo_management.go:168`) is the fee for the
  expected signed size plus the expected output satoshis, plus the commission
  output's bytes and value when commission is enabled. Size it so one
  `ClaimExact` claims exactly one coin per action.
- **`TargetPool()`** (`pkg/defs/utxo_management.go:195`) is
  `ceil(TargetTPS × ExpectedConfirmationSeconds × PoolHeadroomFactor)`, default
  headroom 1.5 (`:139`). The pool must absorb every claim during the window a
  freshly minted coin takes to confirm.
- **`SpendPolicy`** (`pkg/defs/utxo_management.go:27-37`) is `mined_only`,
  `prefer_mined` (default) or `any`. `any` also spends still-sending coins,
  which is how you build unconfirmed depth without meaning to — see
  [§1.2](#12-unconfirmed-chain-depth-vs-the-mempool-ancestor-limit).

**Call `Validate`.** It only enforces the throughput invariants when the
throughput strategy is selected (`pkg/defs/utxo_management.go:217-219`), and it
catches configurations that would otherwise fail hours into a run:

- **Baskets must differ and neither may be `default`**
  (`pkg/defs/utxo_management.go:243-253`) — `pool_basket and reserve_basket must
  differ, both are %q` and `pool_basket and reserve_basket must not be the
  default change basket "default"`. The pool basket keeps the pool out of the
  change basket's scans; sharing them defeats the point.
- **The denomination must exceed the marginal fuel-input fee** (`:256-265`).
- **Pool sizing must resolve to something** (`:267-281`), including
  `0 < low_water_percent <= high_water_percent <= 100`.
- **The sustained-throughput identity** (`:296-312`) — this is the one people
  trip over, and note precisely what it relates:

  ```go
  mintCapacity := float64(t.FanoutOutputsPerTx) * float64(t.FanoutMaxTxsPerRound)
  required := float64(t.TargetTPS) * float64(t.TopUp.IntervalSeconds) * fanoutRecoveryMargin
  if mintCapacity < required {
  	return fmt.Errorf(
  		"sustained-throughput identity violated: fanout_outputs_per_tx × fanout_max_txs_per_round = %.0f must be >= target_tps × top_up.interval_seconds × %.1f = %.0f",
  		mintCapacity, fanoutRecoveryMargin, required,
  	)
  }
  ```

  Per-round mint capacity must exceed rated consumption per round by
  `fanoutRecoveryMargin = 1.2` (`:52-55`), so the pool can climb back from low
  water while still absorbing load. It says nothing about the pool size; that is
  the separate `TargetPool()` relation above.

### 6.2 What `ClaimExact` actually does

Under the throughput strategy, `CreateAction` funds from `Throughput.PoolBasket`
with the resolved denomination (`pkg/storage/create.go:415-452`) and the funder
takes a closed-form fast path (`pkg/storage/internal/funder/throughput.go`):

```
n = ceil((target + baseFee) / (denomination − marginalInputFee))
```

then a single `ClaimExact(scope, reservation, denomination, n)` per tier, safest
first. Because every pool coin is interchangeable, the `SKIP LOCKED` claim never
collides. `len(result) < count` signals pool underflow and is **not** an error
(`pkg/utxostore/store.go:81-84`) — it surfaces to you as `ErrNotEnoughFunds`,
which is the correct throughput-mode outcome (`pkg/storage/create.go:428-431`)
and which you should treat as backpressure ([§5.3](#53-retry-errnotenoughfunds--it-is-backpressure-not-failure)).

### 6.3 Run the keeper, and tune it for your profile

```go
cfg := fuelkeeper.FromThroughput(um.Throughput, denom)
cfg.Originator = "my-app.example.com"
keeper, _ := fuelkeeper.New(w, cfg, logger)
go keeper.Run(ctx)
```

`FromThroughput` (`pkg/wallet/fuelkeeper/keeper.go:138`) derives the keeper
config from the same server-side throughput config, so both sides agree on the
denomination — a mismatch makes every leaf shape fail validation and silently
disables minting. `Run` (`:297`) executes rounds until cancellation, first round
immediate; a round that finds inventory at or above the low-water mark returns
without minting (`:350-353`), and catch-up rounds run back-to-back with a 10 ms
yield rather than waiting for the interval.

Four knobs matter at high throughput. Each exists because of a specific measured
failure:

| Knob | Default | Set it when | Why |
|---|---|---|---|
| `MintConcurrency` | 0 ⇒ 1 (serial) | always, at ≥1000 TPS | A single serial mint loop cannot refill fast enough to match a high-TPS stream's burn rate (`keeper.go:87-92`). Benchmarks used 64. |
| `DisableStreamYield` | false | dedicated blast deployments | The fair-share yield is proportional to RPC duration, so under load it inflates and throttles the keeper *below* the burn rate exactly when full speed is needed (`keeper.go:77-85`). Leave it false when the keeper shares a wallet with latency-sensitive foreground work. |
| `StreamLeafCap` | 10 | raise for large pools | Caps leaf fan-outs per round while a stream is active so inventory is re-measured often (`keeper.go:67-71`). Benchmarks used 2000. |
| `RecycleBasket` / `RecycleCount` | "" / 8 | when change accumulates as fuel-sized coins | Mints leaves funded **directly** from the change basket, skipping the serial reserve-chunk aggregation (`keeper.go:94-102`). |

The direct-recycle path exists because of a concrete failure: after two blast
phases consumed the pool, 331k tiny change coins sat in `default` and the chunk
fan-out path failed every round — `create action failed` aggregating thousands
of tiny inputs — until direct-recycle was enabled, which then rebuilt 1.48M
leaves in ~10 minutes
(`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:197-202`). Note the
admission test: direct-recycle engages only when the basket holds at least two
distinct claimable coins per concurrent leaf, so parallel leaves do not all
contend on the same handful of coins (`keeper.go:385-398`).

### 6.4 A note on the dedicated basket

Older text in the [high-throughput
guide](high-throughput-guide.md#the-dedicated-fuel-basket) and the [benchmarks
gap analysis](benchmarks/README.md#closed-gap-the-dedicated-signable-fuel-basket)
said storage does not honour `Options.FuelShape`, so `FanOutFuel` mints ordinary
change into `default`. **That text predated the implementation and both documents
have since been corrected.** Storage reads `FuelShape` on the create path — it
sizes the fan-out outputs
(`pkg/storage/create.go:97-102`), adds their value to the funding target
(`:113-115`), resolves the fan-out's source basket (`:134`, `:455-467`) and
emits them as shaped change into the pool or reserve basket (`:357`, `:381`).
Mint into a dedicated `fuel` basket; do not size around the old caveat.

---

## 7. Wiring the library for throughput

**Before any of this: run the monitor.** It is not an observability add-on, it
is part of the write path. Change is minted at `TierSending` and promoted to
`TierUnproven` — the first claimable tier — only when a `SEEN` status is
*applied*, and `ApplyStatusBatch` is driven exclusively by the monitor daemon
(`docs/architecture.md:98-113`). Without it, statuses never land, every change
coin stays unspendable, and the funder reports `not enough funds` against a
balance that looks perfectly healthy. The scheduled tasks matter as much as the
stream: the SSE feed carries only events from the moment it connects, so a
status that fired before startup is recovered by the poll, not the stream
([arcade-integration](arcade-integration.md#cold-start-caveat--the-poll-fallback-is-mandatory)).

### 7.1 `wallet.WithThroughputMode(true)`

```go
w, err := wallet.New(network, priv, provider,
	wallet.WithServices(svc),
	wallet.WithLogger(logger),
	wallet.WithThroughputMode(true),
)
```

This removes **two serialization points on the create hot path, both contending
on the single `wdk.BeefParty` mutex** (`pkg/wdk/beef_party.go:25`):

- the `GetKnownTxIDs` party-graph snapshot
  (`pkg/wallet/internal/actions/wallet_create_action.go:48`), which takes the
  mutex once per merged txid plus once to validate *every* transaction in the
  shared graph;
- the `MergeBeefFromParty` merge on the way back out
  (`wallet_create_action.go:78`), which the early return skips along with the
  `ReturnTXIDOnly` re-verification.

Both are dominated by MerklePath root recomputation, which caps `CreateAction`
throughput regardless of concurrency
(`pkg/wallet/internal/wallet_opts/wallet_opts.go:47-56`).

**What you give up:** `KnownTxids` stays nil, so storage returns full ancestry
instead of txid-only-compacted BEEF — more bytes on the wire per call; there is
no in-process ancestry cache; and `ReturnTXIDOnly` responses are not locally
verified. Storage remains authoritative for input BEEF ancestry, so signing and
broadcast are unaffected.

Note the interaction with [§2](#2-bound-what-you-hand-the-library): throughput
mode makes storage return *more* BEEF per call, so pair it with
`storage.WithDirectInputBEEF()` — which is what the reference app does
(`toolbox-app-arcade/internal/walletsetup/walletsetup.go:255-257`).

### 7.2 `monitor.WithApplyConcurrency(n)`

The default is **8** (`pkg/monitor/status_events.go:34`,
`pkg/monitor/monitor.go:112`) and it is not enough at ~1000 TPS. The option's
own doc explains the causal chain (`pkg/monitor/options.go:25-33`):

> Once recent block headers are cached (see `headers.WithCacheDepth`), proof
> application is DB-bound, so a sustained ~1000-TPS deployment must raise this
> above the default 8 for the apply pipeline to keep pace with mining: when the
> appliers cannot drain the hand-off queue, the SSE reader blocks and arcade
> drops events for us — which is how transactions end up with no arcade status
> at all.

Locally the hand-off queue is bounded at 16,384 and **blocks**, it does not drop
(`pkg/monitor/status_events.go:44`, `:147-155`). The drop happens at arcade,
because a blocked reader stops draining the socket. This was not theoretical:
`applyShards` used to be a hardcoded constant, making the option dead code, and
the result was ~25,000 events/s logged as *"dropped events for slow SSE client"*
and 23,745 transactions stranded with no status
(`pkg/monitor/status_events.go:100-106`,
`benchmarks/20260810-postfix-validation-and-partition-ceilings.md:101-111`).

**Be honest about attribution, though.** The 2026-08-11 run showed that at 1000
TPS with the knob wired the toolbox was demonstrably *idle* — 6 of 32 cores,
apply batches averaging 15 records against a 512 cap, the queue near empty — and
the real ceiling was arcade's own single-goroutine SSE fan-out at ~1,600
events/s (`benchmarks/20260811-1000tps-45min-transition-timing.md:15-19`,
`:160-166`, `:289-294`). So: raise the knob, because a low value definitely
causes the failure; but if you still see drops with it raised, measure your own
apply lag before believing the log line. The toolbox's own apply step measured
p50 29–35 ms and p95 under 800 ms for every status
(`benchmarks/20260811-1000tps-45min-transition-timing.md:113-121`).

### 7.3 `headers.WithCacheDepth(0)`

Default is 100 (`pkg/headers/client.go:31`), meaning only immutable headers are
cached. At depth 0 the guard degenerates to "cache everything at or below the
last-observed tip" (`pkg/headers/client.go:311-319`), so the ~thousand proofs
sharing one block do a **single** header fetch instead of one each.

It stays SPV-safe because a reorg evicts every cached header at or above its
fork height *before* any consumer acts on it, and `DemoteReorgedProofs`
re-verifies affected proofs — a cached header can only be an orphan for the
brief, self-healing window until the reorg event lands
(`pkg/headers/client.go:301-310`).

### 7.4 Immediate vs delayed broadcast — the honest version

The [config table in the high-throughput
guide](high-throughput-guide.md#config-tuning-knobs) lists delayed broadcast as
a throughput lever, and the mechanism is real: `AcceptDelayedBroadcast` defaults
to true (`pkg/wdk/primitives/boolean.go:18-23`), which defers the send to the
monitor's `SendWaitingTransactions` task
(`pkg/storage/status_updates.go:958`) and takes arcade's round trip off your
create path. `storage.WithSendConcurrency(n)` (`pkg/storage/provider.go:231`)
then lets the background drainer keep pace — the default is effectively 1, i.e.
sequential (`pkg/storage/status_updates.go:976-979`), which cannot.

**But every cluster benchmark that reached 1000+ TPS used *immediate*
broadcast.** `IMMEDIATE_BROADCAST=true` is in the recorded environment of the
run that produced 1,500 TPS
(`benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:11`), the one
that validated the SSE and MINED-apply fixes at 1,500 TPS with zero failures
(`benchmarks/20260809-sse-delivery-and-mined-apply-fix-validation.md:27`), and
the three-phase run that peaked at 1,876 TPS
(`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:43`).

The reason is that at high rates the delayed path trades one problem for
another. It shortens your create critical path, but it makes the delayed queue a
second thing that has to keep pace, and it defers the moment you learn a
transaction was rejected — which matters a great deal if you are chaining. It
also interacts badly with unbounded BEEF: `WithDirectInputBEEF`'s doc calls out
delayed broadcast specifically as the case where an unproven chain's ancestry
would otherwise grow without bound (`pkg/storage/provider.go:243-246`).

You can opt out at either granularity. `storage.WithImmediateBroadcast()` is a
**provider-wide hard override** of the per-action flag, applied before
persistence and status selection (`pkg/storage/process.go:33-35`); an explicit
no-send is left untouched. Or set `AcceptDelayedBroadcast` to a pointer to
`false` per action, which is the right choice when only *some* of your actions
need the synchronous verdict. With either in force, `WithSendConcurrency` has
almost nothing left to drain — though setting it anyway is cheap insurance for
the stragglers that do end up queued (a circuit-breaker-stranded broadcast, a
`sendWith` batch).

**Recommendation:** start with immediate broadcast. It is what the measured
configurations used and it gives you the rejection signal synchronously.
Consider delayed only if you have measured that arcade's `POST /tx` RTT is your
binding constraint *and* your workload has no chaining that needs the early
verdict — and if you do, raise `WithSendConcurrency` to the arcade client's
connection budget at the same time, or the queue simply grows.

### 7.5 Derive the callback token

`defs.Arcade.CallbackToken` scopes the SSE stream to your wallet instance. When
empty, it is supposed to be derived from the wallet identity key at wiring time
— but *you* have to do the wiring:

```go
ws := defs.DefaultServicesConfig(network)
if ws.Arcade.CallbackToken == "" && identityKeyHex != "" {
	ws.Arcade.CallbackToken = wdk.DeriveArcadeCallbackToken(identityKeyHex)
}
```

(`toolbox-app-arcade/internal/walletsetup/walletsetup.go:93-95`.)
`DeriveArcadeCallbackToken` (`pkg/wdk/arcade_token.go:12`) is an HMAC-SHA256 of
a versioned domain-separation string keyed by the DER-hex identity public key —
deterministic, so the stream scope and `Last-Event-ID` replay survive restarts.

**The measured consequence of not doing this**: `DeriveArcadeCallbackToken`
shipped with unit tests and `defs.Arcade` documented the behaviour, but nothing
called it. Wallets connected tokenless, and arcade only offers mid-stream
catchup to token-scoped clients. The result was 136 log lines of *"dropped
events for slow SSE client"* at ~25,000 events/s, unrecoverable, and 23,745
transactions left with an empty `arcade_status`
(`benchmarks/20260810-postfix-validation-and-partition-ceilings.md:107-111`). A
token also filters the stream to your own transactions, which removes most of
the drop pressure to begin with.

### 7.6 `perfprovider.New` and `MaxDBConns`

`perfprovider` is perf-named but it is the intended public seam for "build a
`storage.Provider` from configuration" — `cmd/storage-server` and the runnable
examples use it too. It lives under `pkg/storage` so it can reach the otherwise
internal metastore and funder subsystems
(`pkg/storage/perfprovider/perfprovider.go:1-15`).

```go
provider, closeProv, err := perfprovider.New(ctx, logger, perfprovider.Config{
	Backend:      perfprovider.BackendPostgres,
	PostgresDSN:  dsn,
	Network:      network,
	StorageName:  "my-app",
	MaxDBConns:   workers + margin,
	FeeModel:     defs.FeeModel{Type: defs.SatPerKB, Value: 125},
	ExtraOptions: []storage.Option{
		storage.WithUTXOManagement(um),
		storage.WithImmediateBroadcast(),
		storage.WithDirectInputBEEF(),
		storage.WithRequiredChangeOutput(), // only if your scripts pin the shape
	},
}, oracle, hdrs)
```

`ExtraOptions` are appended last, so they override the built-in network, name
and fee-model options (`perfprovider.go:179`, `:238`).

**`MaxDBConns`** is applied to both `SetMaxOpenConns` and `SetMaxIdleConns`
(`perfprovider.go:160-164` for Mode A, `:216-218` for Mode B) and its own doc
calls it *a primary throughput knob for the SQL backends: too low and workers
queue on connections, too high and PostgreSQL context-switches*
(`perfprovider.go:71-75`). The published sweep used `conns ≈ workers + a margin`
([high-throughput guide](high-throughput-guide.md#the-worker--connection-sweep-durable-fuel-pool-path)).

Size it for **workers plus the monitor**, not workers alone — the pool is
shared, and starving the monitor behind your write workers is a documented way
to make coins slow to become spendable. The reference app pins its idle
secondary wallet to 16 connections for exactly this reason, so both wallets stay
under PostgreSQL's `max_connections`
(`toolbox-app-arcade/cmd/facts-app/main.go:178`).

### 7.7 Durability

`synchronous_commit=off` is a measured **3.5×** at 64 workers: 393.8 → 1379.0
TPS, e2e p50 150 ms → 45 ms
([high-throughput guide](high-throughput-guide.md#the-durability-tradeoff-the-biggest-lever)).

The honest caveat: it keeps crash-safety of the database *structure* but opens a
bounded window — a few hundred milliseconds — in which the last committed
transactions can be lost on an OS or hardware crash. For this wallet that is not
an ordinary durability trade, because **the database is the only record of your
coins**: there is no UTXO discovery and no restore-from-seed
(`docs/architecture.md:177-183`). Read
[operations](operations.md#backup-is-a-correctness-requirement) before you flip
it, and make sure your backup posture actually covers the window you are
opening.

### 7.8 `FullStatusUpdates` and the SSE budget

`defs.Arcade.FullStatusUpdates` defaults to **true**
(`pkg/defs/services.go:103`) and sets `X-FullStatusUpdates: true` on the
broadcast request (`pkg/arcade/client.go:139-141`). It asks arcade for every
status transition rather than the milestones.

Budget for it. At 1000 TPS with full updates each transaction emits four
transitions (`ACCEPTED_BY_NETWORK → SEEN_ON_NETWORK → SEEN_MULTIPLE_NODES →
MINED`), i.e. **~4,000 events/s**
(`benchmarks/20260811-1000tps-45min-transition-timing.md:155-157`) against a
measured arcade fan-out ceiling of **~1,600 events/s** (`:150-158`, `:289-294`)
— roughly 2.5× oversubscribed. For comparison, an earlier run at the same rate
observed ~2,000 events/s when the milestones were a `SEEN` and a `MINED` per
transaction (`benchmarks/20260808-app-blast-end-to-end-postgres.md:29-30`).

The toolbox only sets the header; which statuses arcade adds is arcade-side and
is not defined in this repository. The known status set is in
`pkg/arcade/wire.go` and the enum is deliberately open — see
[arcade-integration](arcade-integration.md#the-status-lifecycle), including the
`SEEN_MULTIPLE_NODES` spelling trap.

If your application does not need the intermediate transitions, turning this off
is the cheapest available reduction in status-pipeline pressure. If it does need
them, accept that at four figures of TPS the status stream is a real capacity
question and that the repair poll — not the stream — is your convergence
guarantee.

---

## 8. Measure honestly

Your throughput number is the thing every decision downstream rests on. Getting
it wrong is worse than not having it, because it sends you tuning the wrong
component. Four rules, each of which we got wrong first.

**Divide by measured elapsed, never a nominal tick.** `time.Ticker` silently
drops missed ticks, so if your sampler does anything slow inline, dividing a
counter delta by "one second" overstates throughput by exactly the overrun
factor. The reference app's `/api/status` overstated by a variable **5–15×**
before this was fixed — reported 3,749/s against a true 987/s over a 3,799 ms
interval, and a series max of 15,831 against a true peak of 1,891
(`benchmarks/20260811-1000tps-45min-transition-timing.md:307-325`). The
inflation is not a constant factor; it worsens exactly when the app is busy.
The fix (`toolbox-app-arcade/internal/metrics/metrics.go:121-125`):

```go
// sampleInterval is the target period of the fast sampler (counters, rates and
// percentiles). Actual sample durations are measured, not assumed: a ticker
// silently drops ticks when a sample overruns, so dividing by a nominal 1s
// would overstate throughput by exactly the overrun factor.
const sampleInterval = time.Second
```

**Smooth with a ratio of sums, not a mean of rates.** A closed-loop generator
bunches completions after a downstream stall, so the instantaneous rate spikes
after near-zero samples. Averaging per-sample rates over-weights short samples;
`Σdelta / Σelapsed` stays correct when sample durations differ
(`toolbox-app-arcade/internal/metrics/metrics.go:469-488`). The unit test pins
this with deliberately unequal samples — 1000 in 1 s then 1000 in 9 s must give
200, and the failure message names the wrong answer a mean would produce
(`metrics_test.go:71-85`).

**Keep gauges off the rate-sampling path.** Wallet and storage gauges do
PostgreSQL and Aerospike round trips that take seconds. Run them on their own
goroutine so a slow read delays only the gauges and never stretches the interval
your rates are measured over
(`toolbox-app-arcade/internal/metrics/metrics.go:127-131`, `:276-296`). If you
split them, write down which goroutine owns which fields — the reference app
does, as a race contract (`metrics.go:90-96`) — and test it under `-race`
(`metrics_test.go:311`).

**Say something when a gauge fails.** A snapshot that carries a failed gauge's
previous value forward makes the failure *invisible*: the number simply stops
moving. During a 1000-TPS run this made a frozen transaction total look exactly
like "status events stop arriving during a blast", when in fact they had been
applying continuously and one expensive gauge was failing its budget every tick
(`toolbox-app-arcade/internal/metrics/metrics.go:170-183`). Log the transitions,
throttled.

And count backpressure separately from failures
([§5.3](#53-retry-errnotenoughfunds--it-is-backpressure-not-failure)). One more
counting trap worth knowing: a metrics layer that counts *any* non-nil error as
a failure will book every in-flight operation as a failure when you stop the
run, because they all return `context.Canceled` at once. A 1,520-entry failure
count in the 2026-08-11 report is entirely this artefact, clustered at the three
blast stops (`benchmarks/20260811-1000tps-45min-transition-timing.md:246-254`).
Filter `context.Canceled` in the counter, not just at the call site.

---

## 9. Application-side traps that are not the library's fault

These are ours, not the library's. Each cost real time on the Rule 110 app or
inside the toolbox itself, and none is covenant-specific — they apply to any
stateful high-throughput application.

**Never key long-lived state by an index into a trimmed window.** We kept a
sliding window of recent generations and mapped each broadcast txid to its
*position* in that window. The window trims from the front, which shifts every
index down by one, so past the trim point a confirmation was written into a
different generation's row, on a chain that never broadcast that transaction —
silently, with nothing in the program able to notice, producing state that was
internally consistent and wrong. It was invisible at the sixty generations we
had run and certain at scale.

Key by a stable identifier — a sequence number, a chain id, a txid — and resolve
the position from it under the lock at the point of use, never the reverse. As a
bonus, indexing by number also removed a startup re-index scan that was
`O(unsettled × window)`. The library makes the same choice: the status apply
pipeline shards by txid so a transaction's events always land on the same shard,
preserving per-txid order (`pkg/monitor/status_events.go:95-98`).

**A shallow copy of a slice of structs containing slices is not a snapshot.**
Our `Snapshot()` copied the outer `[]Generation` but not the `[]CellTx` inside
each one, so every inner backing array stayed shared with the live engine. Both
HTTP handlers marshal the result *after* the read lock is released — which is
the whole point of taking a snapshot — while the writers keep writing into those
same arrays. That is a genuine data race on a string header, not a stale read.

Deep-copy the inner slices. Then write the test that marshals the snapshot
concurrently with mutation, run it under `-race`, and **verify it fails without
the fix**. Ours reported `DATA RACE` against the old copy and passed against the
new one; a race test you have not seen fail is not evidence of anything.

**A per-batch `WaitGroup.Wait()` makes every batch run at its slowest member's
latency.** See [§5.2](#52-never-use-a-per-batch-waitgroupwait). Prefer a
continuously fed worker pool.

**Do not hold a global lock across a synchronous database write.** We held the
engine's state mutex across the per-transaction persist, which serialises the
whole pipeline behind one disk write — at hundreds of transactions per second
that is the pipeline. Snapshot under the lock, release, then write. And do not
rewrite a large state file on every change: make it a periodic checkpoint whose
only job is to spare a restart from rebuilding, and let the durable per-record
store be the record. This is the same shape as the library's amortized
replay-cursor write, which lets up to 64 applied batches share one cursor
persist and flushes the moment the queue runs dry
(`pkg/monitor/status_events.go:65-71`).

**A `status NOT IN (...)` predicate will not use an ordinary index on
`status`.** It is a full-table scan, and the table it scans is usually the widest
one you have — the one carrying raw transactions and proofs. An index on the
column does not help a negated set predicate; what you want is a **partial**
index whose `WHERE` clause matches the query's, or a boolean/enum column that
the query can test positively.

The library hit exactly this and the measurement is on record: a deployment-wide
status rollup over a 1.99M-row `known_txs` ran a sequential scan at **557.2 ms**
with 311,312 shared buffer hits; a narrow btree that made it an index-only scan
took it to **134.4 ms** with 13,417 — 4.2× faster and 23× less buffer traffic,
for a 13 MB index
(`pkg/storage/internal/metastore/migrations/postgres/00006_known_txs_arcade_status_index.sql:10-18`).
Worse, that 557 ms sat on the metrics gauge sampler and blocked the whole
snapshot publish, so the dashboard reported counts hundreds of thousands of rows
behind the database and looked like a stalled pipeline.

Note the shape of the toolbox's other fix in the same table: the *repair* scan
gets its own **partial** index whose predicate matches the query
(`... WHERE arcade_status IS NULL OR arcade_status = ''`,
`pkg/storage/internal/metastore/migrations/postgres/00004_known_txs_last_polled_at.sql:30-36`).
That is the pattern to copy for an "everything not yet settled" query. We have
this trap open in our own app's history store today; it is easy to write and
easy to miss precisely because the naive index *looks* like it covers it.

The related trap is ordering a repair queue by a column that only advances on
success. A row the sweep cannot apply keeps its `updated_at`, is re-selected at
the head of every tick, and pins the entire backlog behind it — *"every row
behind it was never SELECTed at all"*. That is precisely how a 30-minute
1000-TPS run left 23,745 transactions with a frozen count while arcade held all
of them as MINED. The fix is a separate attempt clock stamped on every attempt
regardless of outcome, and ordering by that
(`pkg/storage/internal/metastore/migrations/postgres/00004_known_txs_last_polled_at.sql:1-36`,
`pkg/storage/internal/metastore/monitor.go:129-142`). If your application has
its own retry sweep, copy the pattern.

---

## 10. Operational requirements

**Fund the wallet with the same profile you will run it with.** The throughput
funder claims from the pool basket in the utxostore; a wallet funded while
running a different backend or strategy puts the coin somewhere the funder
cannot see it, and you get `not enough funds` forever against a balance that
looks correct. This happened in the 2026-08-11 pre-flight: the Aerospike hybrid
is only wired under the throughput profile, so a coin imported without it
existed in PostgreSQL but not in Aerospike (`test:utxos` = 0 objects) and the
funder reported not-enough-funds indefinitely
(`benchmarks/20260811-1000tps-45min-transition-timing.md:44-51`).

**Keep pprof on.** Every ceiling in `docs/benchmarks` was attributed with it,
and in several cases it is what proved the toolbox was *not* the bottleneck —
~9 of 32 cores at the plateau with no lock contention in the top
(`benchmarks/20260810-three-phase-1500tps-and-ceiling.md:128-131`), 6 of 32 with
3 of 32 apply goroutines busy during the 2026-08-11 drain
(`benchmarks/20260811-1000tps-45min-transition-timing.md:167-172`). A throughput
argument without a profile is a guess.

**Single-writer safety is mandatory if your application owns specific UTXOs.**
An application that maintains named chains — one covenant per chain, one live
output per chain — cannot run two replicas: both would spend the same outputs
and double-spend every chain. The library will not save you; it is designed
around *the wallet being the sole writer of its own rows*
(`docs/architecture.md:11`). Enforce single-writer at the deployment layer (a
lease, a `StatefulSet` of one, an advisory lock taken at startup) and make the
loss of the lease a clean stop, not a crash.

**Design for running out of funds as a normal state.** Empty pool → retry, stay
observable, resume unattended when coin arrives. Never fail, never corrupt,
never require a restart. This is the same discipline as
[§5.3](#53-retry-errnotenoughfunds--it-is-backpressure-not-failure), applied to
the whole application rather than one call.

**Back up.** Losing the local database loses the funds even though the keys are
intact. See [operations](operations.md#backup-is-a-correctness-requirement).
This is not advice; it is the design.

---

## Failure-mode playbook

Keyed by what you actually observe. For library-side symptoms (claim contention,
the durable-commit ceiling, reconciler backlog) see the [high-throughput guide's
playbook](high-throughput-guide.md#failure-mode-playbook) — this one covers the
symptoms an application author produces.

| Symptom | Likely cause | What to do |
|---|---|---|
| Database grows without bound; `input_beef` in the hundreds of kB or MB per transaction | You are passing accumulated ancestry as `InputBEEF`, or your transactions never reach MINED (storage clears `input_beef` only then, `pkg/storage/status_updates.go:255-262`) | Store the **parent raw transaction** and rewrap per call ([§2.3](#23-the-rewrap-recipe-and-the-api-trap)). Add `storage.WithDirectInputBEEF()` for the assembly side. Check MINED is actually being applied |
| Per-operation latency climbing steadily over a long run, no error rate | Same cause — BEEF assembly and persistence cost growing with chain length | As above. Measured: 2.1 s/generation and climbing → 0.9 s steady after the fix |
| `ProcessTransaction (4): failed to validate` on a fresh transaction, no competing txs | Underpriced against the extended-format size the validator measures | Raise the fee model to 125 sat/kB, or compute the margin for your input shape ([§3.3](#33-fee-rate-125-not-100)). **Do not retry** — 4xx is final and durable |
| `parent rejected (ancestor …)` cascading across many transactions | Unconfirmed chain depth exceeded the mempool ancestor limit; the deepest rejection cascaded to descendants | Govern chain depth ([§1.2](#12-unconfirmed-chain-depth-vs-the-mempool-ancestor-limit)). Turn off `RecycleChangeToPool`. Go wide and shallow. Measured: ~480 rejections/min → 0 on an independent shallow pool |
| Intermittent "wrong number of outputs" / covenant unlock fails on some transactions only, worsening over time | The funder dropped a sub-dust change output and donated it to the miner | `storage.WithRequiredChangeOutput()`, plus `MaxChangeOutputsPerTx: 1` if you need an exact count ([§3.1](#31-the-dust-floor-and-withrequiredchangeoutput)) |
| `not enough funds` while the balance is clearly sufficient | **Most often: the monitor is not running**, so change is stuck at `TierSending` and never becomes claimable. Otherwise the coin is not where the funder looks — wrong basket, wrong backend, or funded under a different profile | Confirm the monitor daemon is up and applying statuses ([§7](#7-wiring-the-library-for-throughput)). Then check the coin is in `Throughput.PoolBasket` at the configured denomination, and that you funded with the profile you are running ([§10](#10-operational-requirements)) |
| `not enough funds` intermittently under load, pool non-empty | Normal pool underflow — this is backpressure | Retry it and count it separately ([§5.3](#53-retry-errnotenoughfunds--it-is-backpressure-not-failure)). Raise `TargetPoolSize` / `MintConcurrency` if the counter is persistently non-zero |
| Statuses never arrive; `arcade_status` empty and the count frozen | Events were dropped and the repair sweep cannot reach the affected rows | Derive the callback token ([§7.5](#75-derive-the-callback-token)); raise `WithApplyConcurrency` ([§7.2](#72-monitorwithapplyconcurrencyn)); confirm your own sweep orders by an attempt clock, not `updated_at` ([§9](#9-application-side-traps-that-are-not-the-librarys-fault)) |
| Statuses arrive but lag by tens of seconds in bursts, p50 fine and p95 terrible | A block's MINED wave and the live stream share one SSE connection; head-of-line blocking | Expected today. Measured p50 86 ms vs p95 21.6 s across a block boundary (`benchmarks/20260811-1000tps-45min-transition-timing.md:126-140`). Do not gate application progress on live status; use the poll as the convergence guarantee |
| Reported throughput implausibly high, spiky, and worse when the app is busiest | Rate divided by a nominal tick, not measured elapsed | [§8](#8-measure-honestly). Measured inflation was a variable 5–15× |
| Throughput dips to near zero periodically, CPU idle | MINED-apply bursts contending with create on the local stores | Known toolbox-side behaviour (`benchmarks/20260810-postfix-validation-and-partition-ceilings.md:74-76`). Raise apply concurrency and, on Mode B, keep the apply I/O off the same pool |
| Coins double-spent, chains broken, after a deploy | Two replicas of a UTXO-owning application ran at once | Single-writer enforcement ([§10](#10-operational-requirements)) |

---

## Known gaps and rough edges

Stated plainly, because you will meet them:

- **Recovery from dropped statuses is slow.** The repair sweep runs a fixed
  4,000 rows per tick regardless of how large the backlog is, so a backlog of a
  few hundred thousand rows converges in tens of minutes to hours depending on
  the tick cadence. Correctness — eventual convergence — is guaranteed; the rate
  is not adaptive, and it should scale with the missing-status count
  (`benchmarks/20260811-1000tps-45min-transition-timing.md:333-341`,
  `benchmarks/20260810-postfix-validation-and-partition-ceilings.md:133-136`).
- **One SSE connection carries both bulk MINED and live status**, so a large
  block delays live transitions. A second subscription or an arcade-side filter
  is the fix and neither exists yet
  (`benchmarks/20260811-1000tps-45min-transition-timing.md:326-331`).
- **Arcade's status fan-out is the binding constraint above ~1,600 events/s**,
  which at four transitions per transaction is ~400 TPS of *fully-traced* status
  throughput. You cannot tune around this from the application; you can only
  reduce demand (`FullStatusUpdates`) or tolerate the lag.
- **Arcade intake caps at ~1.6–1.9k TPS** on a single-partition propagation
  topic, and more CPU or replicas do not help
  (`benchmarks/20260810-postfix-validation-and-partition-ceilings.md:26-40`).
- **Multi-arcade HA does not exist.** At most one instance is supported
  (`docs/arcade-integration.md:234-243`), so arcade is a single point of failure
  for your write path.
- **`FanOutFuel`'s multi-level bootstrap waits for each level's SEEN**, so
  building a very large pool from a single deep coin is latency-bound at
  startup — a bootstrap cost, not a throughput limit
  (`benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:146-150`).
- **Library docs can lag the code.** The dedicated-fuel-basket caveat in the
  [high-throughput guide](high-throughput-guide.md#the-dedicated-fuel-basket) and
  the [benchmarks gap analysis](benchmarks/README.md#closed-gap-the-dedicated-signable-fuel-basket)
  once contradicted the `FuelShape` implementation in `pkg/storage/create.go`;
  both have been corrected, and the dated benchmark reports still carry the
  original wording as a record of the conditions each run was captured under.
  When a doc and the code disagree, read the code.

## See also

- [High-throughput guide](high-throughput-guide.md) — measured ceilings, the
  funding-path comparison, the durability lever.
- [Architecture](architecture.md) — the three sources of truth, the write path,
  the async status lifecycle.
- [Arcade integration](arcade-integration.md) — the status lattice, broadcast
  semantics, the SSE contract, the reject→release reconciler.
- [Operations](operations.md) — backup as a correctness requirement, monitoring,
  Aerospike fund-safety.
- [Benchmarks](benchmarks/README.md) — every number cited above.
- [`examples/highthroughput`](../examples/highthroughput) — runnable fuel-pool
  wiring.
