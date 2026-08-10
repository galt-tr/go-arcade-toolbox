# Arcade integration

Arcade is this toolbox's **sole transaction-truth oracle**: the single broadcast
target and the only adjudicator of transaction status and double-spends. It is
**asynchronous** — a broadcast is an intake, not a verdict. The authoritative
lifecycle arrives out of band over a Server-Sent-Events stream, with a REST poll
as the fallback and the source of terminal history.

Ports inside an Arcade deployment (`pkg/defs/network_endpoints.go`):

| Service | Location |
|---|---|
| Arcade broadcast/status API | the configured URL |
| Arcade SSE `/events` stream | `<arcade-host>:8082` |
| ChainTracks headers API | `<arcade-host>:8083/chaintracks/v2` |

## The status lifecycle

The wire status strings are copied verbatim from Arcade's own model so they
round-trip byte-for-byte (`pkg/arcade/wire.go`). The enum is **open**: a future
Arcade may emit values this package does not know; unknown values unmarshal
verbatim, are treated as non-terminal, and can supersede any prior status.

| Constant | Wire string | Terminal? | Notes |
|---|---|---|---|
| `StatusUnknown` | `UNKNOWN` | no | |
| `StatusReceived` | `RECEIVED` | no | typical early status echoed on the 202 |
| `StatusSentToNetwork` | `SENT_TO_NETWORK` | no | |
| `StatusAcceptedByNetwork` | `ACCEPTED_BY_NETWORK` | no | may recover a `REJECTED` tx |
| `StatusSeenOnNetwork` | `SEEN_ON_NETWORK` | no | may recover a `REJECTED` tx |
| `StatusSeenMultipleNodes` | `SEEN_MULTIPLE_NODES` | no | **trap: the wire value is `SEEN_MULTIPLE_NODES`, not `SEEN_ON_MULTIPLE_NODES`.** May recover a `REJECTED` tx |
| `StatusDoubleSpendAttempted` | `DOUBLE_SPEND_ATTEMPTED` | **yes** | |
| `StatusRejected` | `REJECTED` | **yes** | can recover (see the lattice) |
| `StatusPendingRetry` | `PENDING_RETRY` | no | retryable broadcast failure, will be retried |
| `StatusStumpProcessing` | `STUMP_PROCESSING` | no | has a STUMP, building the BUMP |
| `StatusMined` | `MINED` | **yes** | |
| `StatusImmutable` | `IMMUTABLE` | **yes** | buried deep enough to be final |

The four terminal statuses are `REJECTED`, `DOUBLE_SPEND_ATTEMPTED`, `MINED`,
`IMMUTABLE`. `Status.IsTerminal()` and `Status.CanSupersede(prev)` expose the
lattice; the two load-bearing edges:

- **`MINED → IMMUTABLE` is the only transition allowed to leave a terminal
  state.** Nothing lower-priority may clobber a `MINED`/`IMMUTABLE` tx.
- **`SEEN_ON_NETWORK` / `SEEN_MULTIPLE_NODES` / `ACCEPTED_BY_NETWORK` may recover
  a `REJECTED` tx** — a peer accepting after another rejected is not a
  contradiction. This is exactly the false-positive case the reject→release
  reconciler must not release on.

See `ExampleStatus_IsTerminal` / `ExampleStatus_CanSupersede` in `pkg/arcade` for
runnable illustrations.

> There is no `SEEN_IN_ORPHAN_MEMPOOL` status in this package; the only "seen"
> trap is the `SEEN_MULTIPLE_NODES` spelling above.

## EF broadcast semantics

`POST /tx` takes a binary Extended Format transaction and its three outcomes
demand different caller behavior (`pkg/arcade/doc.go`, `client.go`):

| HTTP | Meaning | Surfaced as | Retry? |
|---|---|---|---|
| **2xx (202)** | Accepted into the pipeline with an early status. NOT a final verdict. | `*BroadcastResult{TxID, Status}` | n/a |
| **4xx** | Tx-level rejection: **final and durable** (Arcade persists a terminal `REJECTED` row). | `*BroadcastResult{Rejected: true}`, `err == nil` | **never** |
| **503** | Backpressure: the tx was **not** queued, so resubmitting is safe. Arcade sends `Retry-After: 1`. | `*BackpressureError{RetryAfter}` | yes, after the delay |
| **≥500 / transport** | Opaque failure: the tx's fate is unknown. | plain `error` | reconcile via `GetTx` first |

### Circuit breaker

`arcade.Client` wraps broadcast in a single-target circuit breaker
(`pkg/arcade/circuit_breaker.go`). Only **opaque failures** count against it — a
202 accept, a 4xx rejection, and a 503 backpressure reply are all evidence Arcade
is serving, so each resets the failure counter. Once
`ArcadeCircuitBreaker.FailureThreshold` consecutive opaque failures occur the
breaker opens and `Broadcast` fails fast with `ErrCircuitOpen` (retry-safe, the
tx was not submitted); while open it probes `GET /health` at most once per
`HealthProbeIntervalSeconds` and closes on a healthy probe. There is **no
failover chain** — multi-instance HA is a future router (see below). A zero
`FailureThreshold` disables the breaker.

## The SSE status stream

`TxOracle.StreamStatus` maintains a long-lived connection to `/events`
(`pkg/arcade/sse.go`):

- **Event id is a nanosecond timestamp**, passed back as `Last-Event-ID` to
  resume without gaps. Delivery is **at-least-once**, so the same id may be seen
  more than once; consumers must be idempotent.
- `Last-Event-ID` advances **only for delivered frames** (intentionally not
  SSE-spec behavior), so a frame read but not delivered is redelivered after a
  reconnect.
- Auto-reconnect with exponential backoff (1s base, ×2, 60s cap). A read-liveness
  watchdog (60s) treats silence as a dead peer — Arcade sends `: keepalive`
  comments every 15s.

### Cold-start caveat — the poll fallback is mandatory

A fresh connection (empty `Last-Event-ID`) **replays only NON-terminal
statuses**; terminal history is not replayed (it is unbounded and queryable over
REST). So:

> Consumers reconnecting after downtime must poll `GET /tx` for any transaction
> that may have reached a terminal state while they were disconnected.

The monitor implements this: its status-poll task (`SynchronizeTransactionStatuses`)
covers SSE outages and the cold-start terminal gap. `GET /tx/{txid}` is both the
poll fallback and the source of terminal history — it returns the current status
plus `blockHash`/`blockHeight`, the merkle path (BRC-74 BUMP), the raw tx, and
any competing txs.

## ChainTracks (headers)

The headers client (`pkg/headers`) talks to ChainTracks under
`<host>:8083/chaintracks/v2`, requesting `/height`, `/tip`,
`/header/height/{n}`, and the `/tip/stream` + `/reorg/stream` SSE streams.

**Merkle-root verification is LOCAL.** ChainTracks exposes no
`isValidRootForHeight` route, so `VerifyMerkleRoot(root, height)` fetches the
header for `height` and byte-compares its merkle root against the candidate — the
SPV trust anchor stays on our side of the wire. A header above the current tip
returns "not yet valid" (not an error); a miss at/below tip is an error.

The tip and reorg SSE streams carry no resumable event id; the contract is that
both channels close only on ctx-cancel, implementations auto-reconnect and emit a
fresh tip after every reconnect, and **the reorg stream is best-effort /
advisory** — authoritative reorg safety comes from re-verifying stored proofs
against the headers client, never from the stream alone.

## The reject→release reconciler

> **Why this exists (vs go-wallet-toolbox unfail):** see the full comparison in
> [reject-release-vs-unfail.md](reject-release-vs-unfail.md). Short version:
> unfail is a manual, MerklePath-centric recovery of already-failed txs;
> reject→release is an automatic two-phase quarantine that only frees inputs
> after Arcade re-verification (two-pass grace for pure rejects, winner-union
> for double-spends), so false-positive `REJECTED` statuses cannot cause a
> wallet-induced double-spend and UTXO leaks no longer wait on an operator.

Under the async model a rejection can be a **transient false positive**, so
releasing a rejected transaction's inputs eagerly risks resurrecting a still-live
input into a real double spend. The reconciler (`pkg/storage/reconciler.go`,
scheduled by the monitor's `reject_release` task) is built around never doing
that. It runs in two phases:

- **Phase 1** (during status apply): an async-rejected tx is marked
  `suspectFailed` **without** releasing its inputs.
- **Phase 2** (the leased reconciler pass): re-verify each suspect against Arcade
  (`GetTx`) and release inputs **only when the tx is provably dead**.

Three guard rules make this safe:

1. **Two-pass false-positive guard (pure `REJECTED`).** A suspect is released
   only after `GetTx` returns `REJECTED` on **two passes separated by the grace
   window** (`verified_rejected_at` is stamped on the first pass and checked on
   the second, default grace 90s). A transient `REJECTED` that recovers to a
   `SEEN_*`/`ACCEPTED`/`MINED` status in between is caught by the recovered branch
   and **nothing is released** — its frozen change is unfrozen and it re-enters
   the normal apply path. “Pure” means ExtraInfo is **not** a spend-conflict
   class (fee/script/policy): every funding input is assumed still free on chain
   and may be returned to the pool.

2. **Double-spend / spend-conflict winner-union** (`DOUBLE_SPEND_ATTEMPTED`, or
   `REJECTED` whose ExtraInfo / `competingTxs` / ARC status code assert a spent
   input — e.g. `UTXO_SPENT (70): <txid>:<vout> … spent by tx <spender>` after
   Arcade surfaces Teranode failure lists). Inputs are **not** all freed. The
   reconciler unions:
   - outpoints Arcade’s ExtraInfo asserts are already spent (trusted without
     requiring the spender to be MINED in our local view), and
   - inputs of every `CompetingTxs` / ExtraInfo spender that is itself
     terminal-successful (`MINED`/`IMMUTABLE`) with a readable rawTx.
   Only residual inputs (in neither set) are released; spent inputs stay spent.
   If conflict is detected but no spent set can be proven, release is **deferred**
   (safe bias → `stuck`), never “free everything.” If any confirmed winner’s
   rawTx is unreadable, the whole residual release is deferred.

3. **No-double-release CAS.** The terminal status flip
   (`suspectFailed → invalidTx/doubleSpend`) is a positive compare-and-set, so a
   crash re-run or a racing pass is refused; the release ops themselves are
   idempotent (`Unspend`'s `spent_by == txid` guard, `ReleaseOutpoints`'
   reservation guard, `RemoveByMintTx`'s already-gone skip), so even a double
   pass never releases an input twice — and an input another tx has since
   reclaimed is never stomped.

The reconciler also **cascades**: removing a dead tx's phantom change re-marks any
child tx that spent that change as suspect for the next pass (an iterative cascade
that converges without a conflicting-children graph), never clobbering a completed
tx. A suspect that stays unresolved past the max-quarantine ceiling (default 24h)
is escalated to `stuck` — operator-visible and never auto-released.

Mode A applies the release atomically in one `metastore.Do`; Mode B writes the
terminal statuses + enqueues the utxo ops to the outbox in one `Do`, then
inline-executes them, with `DrainOutbox` healing a crash between the two.

### Metrics

Each pass emits (logged by the monitor):

- `reconciler_released_total` — suspects whose inputs were released as provably dead.
- `reconciler_false_positive_total` — suspects that recovered (a transient rejection).
- `reconciler_stuck_total` — suspects escalated past quarantine for operator attention.

plus `scanned`, `ambiguous`, `cascaded`, and the outbox drain report
(`drained`/`failed`/`parked`). Tuning: `DefaultRejectReleaseInterval = 60s`,
`DefaultSuspectGrace = 90s`, `DefaultMaxQuarantine = 24h`
(`pkg/defs/reconciler.go`).

## Multi-Arcade HA (follow-up)

Today at most **one** Arcade instance is supported: `defs.Arcade.Validate()`
rejects `len(Instances) > 1`, and `arcade.Client` targets a single deployment
with no failover chain. The `defs.ArcadeInstance` type (`url`, `events_url`,
`callback_token`) is **reserved** for a future high-availability router that will
implement the same `arcade.TxOracle` interface behind the scenes — that is why
`ErrTxNotFound` is distinct (the HA router must tell "this instance does not know
the tx" from "this instance is broken"). Multi-Arcade HA is a documented
follow-up.
