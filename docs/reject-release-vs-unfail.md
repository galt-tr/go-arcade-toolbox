# Reject→release vs unfail

> **Browser-friendly version:** open
> [`reject-release-vs-unfail.html`](reject-release-vs-unfail.html) locally
> (double-click or `open docs/reject-release-vs-unfail.html`) — single file,
> no server required.

This document explains how `go-arcade-toolbox` recovers spendable inputs after a
transaction is rejected, why that differs from the manual **unfail** model in
`go-wallet-toolbox`, and why reject→release is the safer default for an
**Arcade-asynchronous** wallet.

Implementation: `pkg/storage/reconciler.go` (Phase 2), status apply in
`pkg/storage/status_updates.go` (Phase 1), monitor task `reject_release`.
Reference detail also lives in [arcade-integration.md](arcade-integration.md#the-rejectrelease-reconciler).

---

## 1. The problem both models try to solve

A wallet that broadcasts a transaction has already **reserved or spent** the
funding inputs in its local ledger. If that transaction later fails, those
inputs must become claimable again — otherwise the wallet **leaks UTXOs**
(balance exists on chain / in theory, but the inventory never returns them to
the funder).

That sounds simple until you put Arcade in the middle:

```
POST /tx  ──▶  202 RECEIVED          (intake — not a final verdict)
                │
                ├── later: SEEN_* / MINED     (tx is live / settled)
                ├── later: REJECTED           (may still recover to SEEN_*)
                └── later: DOUBLE_SPEND_*     (a competitor may win)
```

Arcade is **asynchronous**. A `REJECTED` status is not always permanent:
`SEEN_ON_NETWORK`, `SEEN_MULTIPLE_NODES`, and `ACCEPTED_BY_NETWORK` are allowed
to recover a prior `REJECTED` (false positive). Releasing funding inputs the
moment you first see `REJECTED` can put a still-live coin back into the claimable
pool while the original tx (or a competitor) is still valid on the network —
which is how you get a **real double-spend from your own wallet**.

So the design tension is:

| Goal | Risk if done wrong |
|---|---|
| **Never leak UTXOs** forever after a dead tx | Capital stuck; balance looks wrong; rails stall |
| **Never free inputs while a tx may still be live** | Wallet-induced double-spend; funds lost on chain |

Reject→release and unfail are two answers to that tension. They are not the
same answer.

---

## 2. The old model: unfail (`go-wallet-toolbox`)

### Shape

Unfail is a **recover-from-failed** workflow, not a continuous reject pipeline.

1. Something marks a known-tx as **failed** / eventually **`unfail`** (often via
   the wallet API: `ListFailedActions(..., unfail=true)`, which attaches an
   `"unfail"` pseudo-label that storage routes into the unfail queue).
2. The monitor’s **`un_fail`** task periodically scans known-txs with status
   `unfail` (`pkg/storage/internal/actions/process_unfail.go` in GWT).
3. For each candidate it asks services for a **MerklePath**:
   - **Path found** → treat as recovered: known-tx → `unmined`, user tx →
     `unproven`, rematerialize spendable outputs / UTXOs.
   - **Path not found** → treat as dead: known-tx → `invalid`, cascade user txs
     to `failed`, **recreate spent inputs** and mark created outputs not spendable.

```
failed / unfail-labeled
        │
        ▼
  monitor UnFail task
        │
        ├── MerklePath?  yes ──▶ revive (unmined / unproven + rematerialize UTXOs)
        │
        └── MerklePath?  no  ──▶ invalid + restore spent inputs
```

### What unfail optimizes for

- **Operator-driven recovery** of txs already sitting in a failed bucket.
- A binary on-chain check: “does a proof exist yet?” as the authority for revive
  vs release.
- Compatibility with a multi-provider services world (MerklePath from whichever
  provider answers).

### Where unfail is weak for Arcade

| Weakness | Why it matters under Arcade |
|---|---|
| **Manual / opt-in** | Failed txs sit until someone lists them with `unfail=true` (or equivalent). Async rejections do not automatically enter a verified release pipeline. UTXO leaks hide until an operator notices. |
| **Wrong question for false positives** | “Has a merkle path?” is a **mined** check. A tx can be `REJECTED` then recover to `SEEN_*` **without** being mined yet. Unfail’s “no path → release” path can free inputs while the tx is still in the mempool graph. |
| **Not Arcade-status-native** | Does not model `suspectFailed`, grace windows, or the Arcade status lattice (`CanSupersede`). Recovery and death are collapsed into one MerklePath poll. |
| **Double-spend is under-specified** | No winner-union rule over competing txs. Partial consumption by multiple winners is not first-class. |
| **Observability is binary** | You get “processed unfail items,” not false-positive / stuck / cascaded counters. The leak rate is hard to measure. |
| **Label-driven API** | Coupling recovery to `ListFailedActions` + a magic label is easy to misuse and hard to automate safely at high TPS. |

Unfail can be made careful in a single DB with good tests (GWT’s
`process_unfail` does CAS and unit-of-work restores). The structural problem is
the **workflow model**: human-gated, proof-centric, not continuous
reject-verification against an async oracle.

---

## 3. The new model: reject→release (`go-arcade-toolbox`)

### Shape

Reject→release is a **two-phase automatic reconciler** owned by storage + the
monitor. It assumes Arcade is the status oracle and that rejections can lie for
a while.

#### Phase 1 — mark, do not free

On async reject (`REJECTED` / `DOUBLE_SPEND_ATTEMPTED` via SSE or poll), or on a
synchronous `POST /tx` 4xx:

- Known-tx is marked **`suspectFailed`** (with `suspect_since`, competing txids
  when present).
- **Funding inputs are not released.**
- Synchronous 4xx also **removes phantom change** minted in the same
  ProcessAction (that change never hit the chain). Async reject freezes /
  quarantines change until Phase 2 decides.

Phase 1 is the deliberate asymmetry: **status can go bad immediately; capital
stays locked until verification.**

#### Phase 2 — re-verify, then free only if provably dead

The monitor’s leased **`reject_release`** task runs
`VerifyAndReleaseSuspects` on a cadence (default **60s**):

1. Load suspects older than the grace window (default **90s**).
2. Re-query Arcade (`GetTx`) for authoritative status.
3. Branch:

| Re-check result | Action |
|---|---|
| Recovered to `SEEN_*` / `ACCEPTED` / `MINED` / … | **False positive** — unfreeze change, re-enter normal apply path, **release nothing** |
| Still `REJECTED` | **Two-pass guard** (below) |
| `DOUBLE_SPEND_ATTEMPTED` | **Winner-union rule** (below) |
| Ambiguous / GetTx error / no winner yet | Stay in quarantine; freeze change |
| Past max quarantine (default **24h**) | Escalate to **`stuck`** — operator-visible, **never auto-released** |

```
async REJECTED / DOUBLE_SPEND / sync 4xx
        │
        ▼
  Phase 1: suspectFailed   (inputs still held)
        │
        ▼
  Phase 2: GetTx re-verify (every ~60s, after grace)
        │
        ├── recovered ──────────────▶ false positive (no release)
        ├── REJECTED ×2, grace apart ▶ release inputs (provably dead)
        ├── DOUBLE_SPEND + winner ──▶ release only inputs winner did not take
        └── unresolved > 24h ───────▶ stuck (human, never auto-release)
```

### Three safety cores

#### 1. Two-pass false-positive guard (pure `REJECTED`)

A pure rejection is only released after **two** independent `GetTx` results of
`REJECTED`, separated by at least the grace window:

- **Pass 1:** stamp `verified_rejected_at`, freeze change, leave suspect.
- **Pass 2 (stamp + grace aged):** still `REJECTED` → build release plan and free
  inputs.

If between passes the status recovers, the recovered branch runs — **zero
release**. That is exactly the Arcade lattice case unfail’s MerklePath check
does not model.

#### 2. Double-spend / spend-conflict winner-union rule

Applies to `DOUBLE_SPEND_ATTEMPTED` **and** to pure `REJECTED` whose ExtraInfo /
`competingTxs` / ARC code assert a spend conflict (production Arcade often
reports `UTXO_SPENT (70): <txid>:<vout> … spent by tx <spender>` as `REJECTED`,
not `DOUBLE_SPEND_ATTEMPTED`):

- Inputs the winner(s) consumed, or that ExtraInfo names as already spent, stay
  recorded as **spent** (they are gone).
- Only **residual** inputs (not in that spent set) are released back to the pool.
- The reconciler **unions every confirmed winner’s inputs** plus ExtraInfo
  outpoints. Stopping at the first winner would resurrect an outpoint another
  mined competitor already spent.
- If conflict is known but no spent set can be proven, or any confirmed winner’s
  `rawTx` is unreadable, release is **deferred** (never free everything on an
  incomplete consumption set).

#### 3. No-double-release CAS + idempotent inventory ops

- Terminal flip is a positive CAS: `suspectFailed → invalidTx | doubleSpend`.
- Crash re-run or a racing pass is refused once the gate has flipped.
- Inventory ops are idempotent (`Unspend` guarded by `spent_by`, reservation
  release skips free rows, `RemoveByMintTx` skips missing).
- Mode B enqueues release ops to the outbox then executes; `DrainOutbox` heals a
  crash mid-inline-execute.

### Cascades

Removing a dead tx’s phantom change re-marks any **child** that spent that
change as suspect for a later pass. The cascade is iterative (no full
conflicting-children graph), and never clobbers a completed tx.

### Metrics (the leak becomes countable)

Each pass logs counters (see `defs.ReconcilerReport`):

| Counter | Meaning |
|---|---|
| `reconciler_released_total` | Provably dead; inputs returned to the pool |
| `reconciler_false_positive_total` | Transient reject; two-pass guard did its job |
| `reconciler_stuck_total` | Past quarantine; **human required** |
| plus `scanned`, `ambiguous`, `cascaded`, outbox `drained`/`failed`/`parked` | |

Under unfail, a silent UTXO leak was an operator mystery. Under reject→release,
false positives and stuck capital are first-class telemetry.

---

## 4. Side-by-side

| Dimension | **unfail** (GWT) | **reject→release** (arcade-toolbox) |
|---|---|---|
| Trigger | Manual / label (`ListFailedActions(unfail=true)`) + monitor scan of `unfail` status | Automatic on reject status / sync 4xx → `suspectFailed` |
| When inputs free | After unfail decides “no MerklePath” | After **verified** death (two-pass REJECTED or winner-confirmed double-spend) |
| Authority | MerklePath presence | Arcade `GetTx` status lattice + optional winner rawTx |
| False-positive model | Weak (proof-centric) | First-class (grace + two passes + recover branch) |
| Double-spend | Restore inputs when invalid; no winner-union | Winner-union; partial release; defer on ambiguity |
| Default bias under uncertainty | Can free too early if “no path yet” | **Never free** (quarantine → stuck) |
| Operator role | Required to start recovery | Required only for `stuck` / parked outbox |
| High-TPS fit | Human-in-the-loop does not scale | Continuous leased task; metrics for backlog |
| Failure visibility | Easy to miss stuck capital | `false_positive` / `stuck` / `released` counters |
| Coupling | Wallet list API + magic label | Storage + monitor only |

---

## 5. Why reject→release is better

### 5.1 It matches Arcade’s actual contract

Arcade says: **broadcast is intake; status is out-of-band; `REJECTED` can recover.**
Reject→release encodes that contract in the ledger. Unfail encodes a different
world: “failed means maybe mined, check for a proof.”

If your truth oracle is Arcade, the reconciler asks the right question:
**“Is this tx still dead on the oracle after a deliberate wait?”** — not
**“Does any provider have a merkle path yet?”**

### 5.2 It prevents the catastrophic failure mode

The expensive bug is not a delayed UTXO return. It is **freeing an input that
is still committed to a live (or soon-live) transaction**, then spending it
again in a new createAction.

Reject→release’s default under ambiguity is **hold**. That trades temporary
capital lock for permanent fund safety. At payment-rail TPS, that trade is
correct: a stuck UTXO is recoverable with ops; a double-spent UTXO is not.

### 5.3 It closes the UTXO-leak class without humans

Under unfail, async rejects that nobody unfails become **silent inventory
leaks**. Under reject→release, every suspect is on a timer:

- recover → rejoin lifecycle  
- verify dead → auto-release  
- cannot decide → `stuck` alert  

There is no “failed forever in a dark corner unless someone lists with a flag.”

### 5.4 Double-spend is handled as a set problem, not a boolean

Real conflicts are messy: multiple competitors, disjoint input subsets, missing
rawTx. Winner-union + defer-on-unreadable is the difference between a wallet
that can free residual change after a conflict and one that either:

- frees too much (double-spend), or  
- frees nothing after a partial conflict (leak).

### 5.5 It is operable at scale

| unfail at 1k TPS | reject→release at 1k TPS |
|---|---|
| Who calls `ListFailedActions(unfail=true)`? | Monitor does the work |
| How do you know leak rate? | `released` / `false_positive` / `stuck` |
| What if MerklePath lag is minutes? | Grace + status re-check, not proof-only |
| Multi-instance safety | Lease on `reject_release` + CAS gate |

### 5.6 Sync 4xx vs async reject stay consistent on the hard rule

Both paths **refuse to free funding inputs in Phase 1**. Sync 4xx eagerly drops
**change** (never on chain, non-abortable failed tx). Async reject leaves change
quarantined until Phase 2. The invariant is the same: **inputs free only after
verified death.**

---

## 6. What reject→release is *not* better at (honest limits)

| Limit | Implication |
|---|---|
| **Capital can sit locked for ≥ grace (and up to quarantine)** | Size fuel pools for worst-case quarantine if rejections are common. Defaults: 90s grace, 24h stuck ceiling. |
| **`stuck` never auto-releases** | Rising `reconciler_stuck_total` is a page, not a log line. Human investigation required (Arcade down, unreadable competitor rawTx, endless ambiguity). |
| **Requires Arcade + monitor** | If `reject_release` is disabled or the oracle is unreachable, suspects pile up — same class of failure as “nobody ran unfail,” but **visible**. |
| **Not a substitute for FailAbandoned** | Pre-broadcast `unsigned`/`nosend` cleanup is a different task. Forever-`RECEIVED` / never-mined tails need oracle terminal status (or a future policy), not Phase 2 alone. |
| **Wallet `ListFailedActions(unfail=…)` is not the control plane** | Do not expect the GWT label workflow. Recovery is the reconciler; the old API is not the mechanism. |

Safe bias is intentional: **prefer stuck capital over unsafe free.**

---

## 7. Worked examples

### Example A — Transient reject (false positive)

1. Wallet broadcasts tx `A`; inputs reserved/spent locally; change at `TierSending`.
2. SSE: `REJECTED` → Phase 1: `suspectFailed`, inputs **held**.
3. 30s later SSE: `SEEN_ON_NETWORK`.
4. Phase 2 re-check (or status apply) sees recovery → **false positive**, no release;
   change promotes on the normal SEEN path.

**Unfail path risk:** if an operator (or automated label) pushed unfail while
still “no MerklePath,” inputs could be restored while `A` was about to be seen —
then a second payment reuses those outpoints.

### Example B — Durable reject

1. Tx `B` rejected; Phase 1 suspects.
2. Pass 1 of reconciler: still `REJECTED`, stamp `verified_rejected_at`.
3. ≥ 90s later, pass 2: still `REJECTED` → release funding inputs, remove
   phantom change, cascade children that spent that change.

No human call required; leak closed on a bounded timer.

### Example C — Double-spend, two winners, disjoint inputs

1. Tx `C` spends inputs `{i1, i2, i3}`; status `DOUBLE_SPEND_ATTEMPTED`.
2. Competitor `W1` mines with `{i1}`; competitor `W2` mines with `{i2}`.
3. Winner-union → consumption set `{i1, i2}`; release only `{i3}`.

A “first winner wins” rule would free `i2` after seeing only `W1` — incorrect.
Reject→release unions winners; unfail has no equivalent structure.

### Example D — Ambiguity past 24h

1. Double-spend, but no competitor ever reaches `MINED` / rawTx unreadable.
2. After max quarantine → **`stuck`**, inputs still held, metric increments.
3. Operator inspects Arcade / competitors; manual remediation only if policy allows.

Prefer this over guessing.

---

## 8. Migration notes (GWT → arcade-toolbox)

| GWT habit | Arcade-toolbox equivalent |
|---|---|
| `ListFailedActions(ctx, args, true, originator)` to queue unfail | Do **not** rely on this for input recovery; ensure monitor `reject_release` is enabled |
| Monitor task `un_fail` = MerklePath recovery | Config key may still exist as `un_fail`, but the daemon **repurposes** it to **status poll** (`SynchronizeTransactionStatuses`). Reject recovery is **`reject_release`** |
| Expect failed bucket to hold capital until ops | Expect `suspectFailed` → auto path; watch `stuck` |
| Restore spent outputs via unfail invalid path | `Unspend` / `ReleaseReservation` / `RemoveByMintTx` via reconciler plan |

See also [migration-from-go-wallet-toolbox.md](migration-from-go-wallet-toolbox.md).

---

## 9. One-paragraph summary

**Unfail** is a human-triggered, MerklePath-centric recovery of already-failed
transactions: good enough when failure is rare and an operator is in the loop,
but the wrong control plane for Arcade’s async status lattice and high-TPS
rails. **Reject→release** never frees funding inputs on first sight of failure;
it quarantines as `suspectFailed`, re-verifies against Arcade with a two-pass
grace for pure rejects and a winner-union rule for double-spends, auto-releases
only when death is proven, and escalates ambiguity to an operator-visible
`stuck` state with metrics. That is better because it aligns with Arcade’s
false-positive-capable rejections, prevents wallet-induced double-spends,
closes silent UTXO leaks without manual labels, and makes recovery operable at
scale — at the deliberate cost of holding capital under uncertainty instead of
guessing.

---

## References (in-repo)

| Topic | Where |
|---|---|
| Reconciler implementation | `pkg/storage/reconciler.go` |
| Phase 1 status apply | `pkg/storage/status_updates.go` (`ApplyStatusUpdate`) |
| Sync 4xx path | `pkg/storage/process.go` (`commitRejected`) |
| Defaults (60s / 90s / 24h) | `pkg/defs/reconciler.go` |
| Monitor wiring | `pkg/monitor/monitor.go` (`RejectReleaseMonitorTask`) |
| Arcade lattice / recover edges | [arcade-integration.md](arcade-integration.md) |
| Lifecycle sketch | [architecture.md](architecture.md#the-async-status-lifecycle) |
| Ops metrics | [operations.md](operations.md#monitoring) |
| GWT unfail (spec repo) | `go-wallet-toolbox/pkg/storage/internal/actions/process_unfail.go` |
