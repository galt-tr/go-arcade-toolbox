# Rejection-hardening audit

**Question.** Every way a client can end up with a transaction rejected by
arcade or teranode that this library could have prevented, or at least
explained, locally.

**Method.** Every claim below is cited to `path:line`. Where a claim concerns
what the *network* does rather than what the toolbox does, it is cited to
arcade (`/git/arcade`), to teranode, or — for the two questions that mattered
most — to a measurement taken by driving arcade's real BDK validator directly.
Two of the five premises this audit started from turned out to be wrong, and
saying so is more useful than confirming them; see [Premises that did not hold](#premises-that-did-not-hold).

**Scope note.** The grounding application runs 128 covenant-protected UTXO
chains through the two-step `CreateAction` → `SignAction` flow with ~4 kB
transactions and caller-supplied unlocking scripts. That workload is the
useful stress case precisely because it exercises every path where the toolbox
has to trust the caller for something it could instead verify.

---

## Findings, by severity

| # | Finding | Severity | Preventable locally? | Status |
|---|---|---|---|---|
| 1 | The rejection reason was never persisted or logged | **Critical** | Yes, fully | **Fixed** |
| 2 | Consensus rules are selected per transaction, not per input | **High** | Yes, given the activation height | **Fixed (opt-in)** |
| 3 | The committed fee is never checked against the finished transaction | **High** | Yes, exactly | **Fixed (opt-in)** |
| 4 | A caller-provided input can be re-selected by the funder | **High** | Yes, trivially | Recommended |
| 5 | Nothing tracks unconfirmed ancestry depth | **Medium** | Partially | Recommended |
| 6 | A caller-provided input is never checked for being already spent | **Medium** | Partially | Recommended |
| 7 | `DefaultMaxOutputScript` overstates the network limit by 2000× | **Medium** | Yes | Recommended |
| 8 | Local script verification omits the node's standardness flags | **Low** | Partially | Recommended |
| 9 | `VerifyScripts` runs twice per transaction | **Low** (cost) | n/a | Recommended |
| 10 | nLockTime finality is never checked | **Low** | Yes | Recommended |

---

## 1. A rejection whose cause is unknowable — **Critical, fixed**

### The failure

A `REJECTED` arrived over SSE with an **empty** `extraInfo`, and
`GET /tx/<txid>` afterwards returned 404. The cause was recoverable only by
reading teranode's propagation-pod logs by hand.

### What the code did

`arcade.TxRecord` carries `ExtraInfo`
(`pkg/arcade/wire.go:274`) and it survives intact all the way from the SSE
frame (`pkg/arcade/sse.go:137-159`) through the monitor into
`ApplyStatusBatch`. It was then **dropped on the floor** at the two places that
handle a rejection:

- `pkg/storage/status_updates.go:285-291` (`applyRejected`) — passed
  `rec.CompetingTxs` and `rec.Status` to `MarkSuspect` and never read
  `rec.ExtraInfo`.
- `pkg/storage/status_updates.go:875-883` (`applyRejectedBatch`) — identical.

Neither function contained a single log call. There was **no column** to put a
reason in: `known_txs` had 21 columns and not one of them was free text for a
failure (`pkg/storage/internal/metastore/migrations/sqlite/00001_init.sql:131-151`,
plus `verified_rejected_at` and `last_polled_at` from migrations 3 and 4).

The reconciler *did* read the reason — `pkg/storage/reconciler.go:264` — but
only to run a regex classifier over it
(`pkg/storage/spend_conflict.go:72-111`), and only from a **live `GetTx`**
(`pkg/storage/reconciler.go:134`). When that `GetTx` 404s, the reconciler takes
the safe branch and learns nothing: `pkg/storage/reconciler.go:136-139`.

The synchronous path did marginally better — `pkg/storage/process.go:361` put
the 4xx body into the returned `ReviewActionResult` — but that is an in-memory
value returned to one caller, never stored.

So the benchmarks' observation that "per-status timestamps are not persisted
anywhere" understated the problem: **the reason was not persisted either, and
unlike a timestamp it cannot be reconstructed.**

### Why the reason really was empty

This is worth stating precisely, because it is not a toolbox bug and the fix
has to work around it rather than assume it away.

Arcade populates `extraInfo` on the persisted row for every rejection
(`/git/arcade/services/api_server/handlers.go:969-975` at intake,
`/git/arcade/services/propagation/propagator.go:2639-2645` asynchronously).
But the **async status event carries no reason at all**:
`/git/arcade/services/propagation/propagator.go:910-927` builds the published
template from `Status`, `Timestamp` and `TxIDs` only. The SSE manager copies
that template verbatim (`/git/arcade/services/sse/manager.go:255-260`), and
`extraInfo` is `omitempty`, so the field vanishes. All three async REJECTED
publish sites (`propagator.go:617`, `:902`, `:2175`) produce a reason-free
frame — contradicting the manager's own doc comment at `manager.go:878-883`,
which is accurate only for the intake path.

Two consequences for us:

1. **A REJECTED with empty `extraInfo` is normal, not exceptional.** Any design
   that treats an empty reason as "nothing to record" records nothing in the
   common case.
2. **The reason usually still exists in arcade's store**, reachable via
   `GET /tx/{txid}` — note the path is `/tx/{txid}`, **not** `/api/v1/tx/{txid}`
   (`/git/arcade/services/api_server/routes.go:282`; `/api/v1/*` is registered
   only for merkle callbacks and block routes at `routes.go:278-284`). A
   request to `/api/v1/tx/...` hits gin's default no-route handler and returns
   `{"message":"404 page not found"}`, which is textually distinct from
   arcade's own `{"error":"transaction not found"}`
   (`/git/arcade/services/api_server/handlers.go:599`). **If the observed 404
   body was the former, the record was never missing and the URL was wrong.**
   That is worth checking before blaming retention — though arcade has no TTL
   of its own on status rows in Postgres or Pebble, and the one real expiry
   vector is the Aerospike namespace `default-ttl`, which arcade leaves at the
   server default (`/git/arcade/store/aerospike/aerospike.go:274-280`) and is
   therefore pure operator configuration.

### What was implemented

A `reject_reason` column, written by every path that learns a reason, and a
`WARN` log at the instant it is learned.

- Migrations `00007_known_txs_reject_reason.sql` (both engines).
- `pkg/storage/internal/metastore/knowntx.go` — column in `knownTxCols`, a
  `RejectReason *string` field, and a **sticky** conflict clause
  (`COALESCE(EXCLUDED.reject_reason, known_txs.reject_reason)`) so a later
  upsert that carries no reason cannot erase one already recorded — the same
  rule `was_broadcast` already had via `stickyOr`.
- `MarkSuspect` (`pkg/storage/internal/metastore/monitor.go:168`) and
  `MarkSuspectFailed` (`pkg/storage/internal/metastore/knowntx.go:424`) both
  take the reason and bind it through `COALESCE(?, reject_reason)`, so an
  **empty reason preserves an earlier one** — which is exactly the arcade
  behavior described above.
- `pkg/storage/status_updates.go:306` — `logRejection`, called from all four
  ingest points: `applyRejected` (`:286`), `applyRejectedBatch` (`:901`),
  `sendOneWaiting`'s 4xx branch (`:1123`), and `commitRejected`
  (`pkg/storage/process.go:434`). It logs at **WARN**, and an absent reason is
  rendered `(arcade supplied no reason)` rather than omitted, because "arcade
  refused this and told us nothing" is itself the finding.
- A cascade now records *which ancestor died* rather than the useless string
  `"cascade"` (`pkg/storage/reconciler.go:730-751`) — the case where the
  child's own reason is least informative.
- `KnownTxRow.RejectReason` (`pkg/storage/insights.go:28`) surfaces it on the
  drill-down an operator actually reads.

Tests: `pkg/storage/reject_reason_test.go` — synchronous 4xx, async event,
empty-reason-preserves-earlier, no-reason-stays-NULL, and the drill-down.

**Residual gap the toolbox cannot close:** if arcade never had a reason and the
record is genuinely gone, nothing recovers it. The fix guarantees we keep
whatever was said, once, forever — not that something was said.

---

## 2. Consensus rules are per input, not per transaction — **High, fixed (opt-in)**

### The premise holds, but not for the reason given

The observed failure — `utxoHeights=410|410 error=Push value size limit
exceeded` — is real and is the pre-Genesis 520-byte `MAX_SCRIPT_ELEMENT_SIZE`
applied to a UTXO created at height 410.

The toolbox's verifier selects **one era for the whole transaction** from a
provider-level bool: `pkg/storage/verifiers.go:97-109` (before this change)
built `WithAfterGenesis()` or `WithAfterChronicle()` from
`WithChronicleOpcodes` (`pkg/storage/provider.go:145`). go-sdk's own flag is
named `UTXOAfterGenesis` — "defines that the **utxo** was created after
genesis" (`script/interpreter/scriptflag/scriptflag.go:71-73`) — so the API was
telling us it is an input property and we were setting it globally.

Critically, the verifier **never selects pre-Genesis at all**: the interpreter
thread starts on `beforeGenesisConfig` (`script/interpreter/thread.go:56`) and
the era options only move it forward (`thread.go:283,290`). So an old UTXO was
always judged by the newest rules — passing locally, rejected remotely.

**But the divergence is not against arcade.** Arcade deliberately reports an
unknown-parent sentinel for *every* input
(`/git/arcade/validator/validator.go:169-172`), which teranode resolves in
policy mode to a height above every activation
(`.../services/validator/ScriptVerifierGoBDK.go:54-82`), so arcade's own
validator treats everything as post-Genesis and post-Chronicle. That was a
deliberate fix — arcade's own regression test
(`/git/arcade/validator/validator_test.go:262-302`) pins that a >520-byte push
is refused at source height 1 and accepted above it.

So the shape of the real incident is: **arcade accepts and answers 202;
teranode, which resolves the real per-UTXO heights, refuses asynchronously.**
That is the worst failure shape to debug, and the strongest argument for
catching it locally.

### What was implemented

`WithGenesisActivationHeight(height)` (`pkg/storage/provider.go:178`). When
set, `executionOptions` (`pkg/storage/verifiers.go:108`) selects the era **per
input** from that input's own source height, read from the merkle proof on its
source transaction (`isPreGenesisUTXO`, `pkg/storage/verifiers.go:148`). The
data was already there: `hydrateInputs` (`pkg/storage/process.go:583`) attaches
source transactions from the stored input BEEF, and BEEF parsing attaches each
proven ancestor's `MerklePath` — carrying `BlockHeight` — at
`go-sdk/transaction/beef.go:89`, matching how `buildBEEF`
(`pkg/storage/beef.go:70-81`) writes it.

Deliberate choices, all defensible in the failure direction:

- **An unproven parent is newest-era, never oldest.** An output that is not yet
  mined cannot predate something that is. Guessing the other way would refuse a
  wallet its own freshly-created change.
- **Bip16 (P2SH) is not enabled** even under pre-Genesis rules. The goal is to
  catch the limits that actually reject transactions, not to re-enact a retired
  era; enabling P2SH changes evaluation of any script matching the exact
  pattern, which is a behavioral claim not worth making unverified.
- **Off by default.** The toolbox genuinely cannot derive the height. Genesis
  activated at 620538 on mainnet and 1344302 on testnet, but a private or
  scaling network picks its own, and inventing one would refuse valid spends.
- **Proofs are not verified here.** A caller-supplied BEEF could understate a
  height and provoke a spurious refusal — a denial of service by the caller
  against itself, not a route to getting an invalid transaction accepted.

Tests: `pkg/storage/verifiers_era_test.go` — 521 bytes refused pre-Genesis, 520
accepted, 521 accepted post-Genesis, unproven-parent uses newest era, and
height 0 leaves existing behavior byte-identical.

**What is still not covered:** Chronicle activation height. The same mechanism
would extend to it, but Chronicle's height is even less discoverable, and
`WithChronicleOpcodes` already gives a working global switch for the era that
matters to covenant users. Adding a second height knob without a network that
exercises it would be guesswork.

---

## 3. The committed fee is never checked against the finished transaction — **High, fixed (opt-in)**

### The original premise is wrong; the underlying hazard is real

The premise was that arcade prices the **extended-format** size, so a wallet at
exactly 100 sat/kB necessarily underpays. **That is not what happens.** I
measured it by driving arcade's real BDK engine
(`/git/arcade/validator`, `Policy{MinFeePerKB: 100}`):

| source locking script | standard size | EF size | `floor(100·std/1000)` | `floor(100·EF/1000)` | accepted at |
|---|---|---|---|---|---|
| 1 byte | 73 | 89 | 7 | 8 | **7** (6 refused) |
| 1000 bytes | 73 | 1090 | 7 | **109** | **7** (6 refused) |

A transaction whose EF encoding is 1090 bytes is accepted with a **7-satoshi**
fee. The fee is priced against the **standard serialization**; the prevout
satoshis and source locking scripts that extended format carries inline are
handed to the validator as separate spent-coin data and are not billed. The
code path agrees: `/git/arcade/validator/convert.go:22-45` builds the validated
transaction from `tx.Bytes()` (standard) and attaches prevouts as struct
fields; the EF blob at `ScriptVerifierGoBDK.go:285` is only the cgo transport
encoding.

The arithmetic is also **truncating**, with a one-satoshi minimum — teranode
documents it as matching bitcoin-sv's `CFeeRate::GetFee`
(`.../settings/policy_settings.go:22`). The toolbox computes
`ceil(size/1000 · rate)` (`pkg/storage/internal/funder/fee_calculator.go:50`),
which is always ≥ the floor for the same size. **So at 100 sat/kB the toolbox
pays at or above arcade's requirement — provided the size it priced is the size
it shipped.**

Two consequences: the `docs/application-throughput-playbook.md:474-500`
explanation of *why* 125 sat/kB is needed is incorrect (the recommendation
itself is still fine — 125 is simply more margin), and the real hazard is
somewhere else entirely.

### The real hazard: the fee is priced from an estimate that is never rechecked

The fee is committed during `CreateAction`, from a size built out of *declared*
script lengths: `pkg/storage/create.go:107` calls
`txutils.TransactionSizeFromScriptLengths` over `providedInputSizes`
(`pkg/storage/create.go:584-595`). Nothing ever revisits it. `ProcessAction`
receives the finished transaction and never compares its actual fee against its
actual size.

The estimate can under-shoot in two ways:

1. **Silently.** `pkg/storage/create.go:587-591`: a caller-provided input whose
   `ScriptLength()` errors — neither `UnlockingScript` nor
   `unlockingScriptLength` supplied — is assumed to be a **107-byte P2PKH**
   unlocking script. A covenant input carrying a two-kilobyte unlocking script
   priced as 107 bytes underpays by ~190 satoshis at 100 sat/kB. No warning.
2. **By the caller's own arithmetic.** A declared `unlockingScriptLength` is an
   estimate the caller computes; the reference app derives it from preimage
   geometry (`/git/rule-110-arcade/internal/chain/step.go:242-257`) and falls
   back to a flat 4096 on error. Any shortfall goes straight into the fee.

There is precedent for exactly this class of bug being shipped and fixed once
already: `pkg/storage/internal/funder/collector.go:152-159` documents that
pricing only the unlocking script and not the whole input undercounted each
input by 41 bytes and "at a fee rate sitting on arcade's GoBDK min-fee floor,
drops the broadcast below it". The mechanism is identical; only the source of
the undercount differs.

Two things currently *mask* it, which is why this is intermittent rather than
constant: the funder rounds up, and
`pkg/storage/internal/funder/collector.go:226-240` adds the change-output bytes
cumulatively without subtracting the previous addition, charging them up to
N+1 times for N inputs. Both are accidental cushions, not guarantees.

### What was implemented

`WithMinBroadcastFeeRate(satPerKB)` (`pkg/storage/provider.go:251`) and
`checkBroadcastFeeRate` (`pkg/storage/process.go:187`), called once from
`processNewTx` (`pkg/storage/process.go:137`) at the moment the signed
transaction first becomes real — before it is persisted, before change is
minted, before anything is sent, so a refusal leaves nothing to clean up.

It is **measured, not estimated**: fee = Σ inputs − Σ outputs, read from the
source outputs the caller already had to supply for script verification; size =
the serialized length of the very bytes that will be broadcast. The required
minimum is `txutils.MinRequiredFee` (`pkg/internal/txutils/tx_size.go:71`),
which reproduces `CFeeRate::GetFee`'s integer arithmetic exactly — truncating,
with the one-satoshi minimum — because approximating it would put the client a
satoshi away from the node on some sizes, which is the whole failure mode.

The error names the shortfall in satoshis and points at the cause:

```
storage: transaction fee is below the configured broadcast floor:
23 sat over 10226 bytes is 999 sat short of the 100 sat/kB floor
(the committed fee was computed from a size estimate that the signed
transaction exceeded — check every input's declared unlockingScriptLength)
```

It takes an explicit rate rather than reusing the wallet's own fee model, and
is off by default, because **the floor is the receiving deployment's policy,
not a protocol constant**: arcade's default is 100
(`/git/arcade/validator/validator.go:76`), an operator may set any value, and
`accept_zero_fee` removes it entirely (`/git/arcade/app/app.go:318-325`).
Defaulting it on at 100 would refuse good transactions on a zero-fee network;
deriving it from the wallet's own (higher) target rate would refuse
transactions the network would have taken.

Serialization was made single-pass while doing this — `processNewTx` already
called `tx.Bytes()` for the known-tx row, so the check is free and the two
cannot disagree about what was priced.

Tests: `pkg/internal/txutils/min_fee_test.go` (including a case pinning the
BDK-measured 73-byte/7-satoshi result) and `pkg/storage/fee_floor_test.go`
(refused with the shortfall named, nothing persisted, coin still reserved;
accepted when adequate; off by default).

---

## 4. A caller-provided input can be re-selected by the funder — **High, recommended**

`funder.FundArgs` (`pkg/storage/internal/funder/funder.go:106-151`) has **no
exclusion list**. `CreateAction` resolves the caller's explicit inputs
(`pkg/storage/create.go:72`) and then funds from a basket
(`pkg/storage/create.go:139-153`) with no knowledge of them. A caller-provided
input that is also a live claimable coin in the funding basket can therefore be
selected a second time, producing a transaction with **duplicate inputs** —
which BDK rejects (`bad-txns-inputs-duplicate`, in its DoS taxonomy at
`gobdk/script/doserror.go:13-32`).

It is not reachable in the grounding app, whose cell UTXOs live in a separate
basket from its funding. It is trivially reachable for any wallet that names an
explicit input drawn from its own change basket.

**Fix:** pass the provided outpoints into `FundArgs` and skip them during
claiming; or, cheaper and with no funder change, reject the duplicate in
`CreateAction` after funding by intersecting `providedInputs` with
`fundRes.AllocatedUTXOs`. **Not implemented here** because the correct fix
touches the funder's claim path, which is the hottest code in the library and
has its own contention and fast-path invariants — that deserves its own change
with its own benchmark, not a rider on an audit.

---

## 5. Unconfirmed ancestry depth — **Medium, recommended**

Confirmed: the toolbox has no notion of how deep an unconfirmed chain it is
building. The failure is documented at
`docs/benchmarks/20260808-app-blast-end-to-end-aerospike-hybrid.md:106-131` —
a self-payment chain outgrows the mempool ancestor limit, the deepest
transaction is rejected, and the rejection cascades to every descendant.

Two corrections to how this is usually described:

- **Arcade does not enforce it.** `LimitAncestorCount: 1000000` at
  `/git/arcade/validator/validator.go:226-227` is a **dead value**: no setter
  for it exists in the gobdk binding, and teranode's own doc says "Setting
  exists but ancestor tracking not actively enforced in current Teranode
  implementation" (`.../settings/policy_settings.go:16`). The limit is enforced
  further down, outside both repos.
- The only ancestor bound inside arcade is a graph-walk depth of 8 in the async
  propagator (`/git/arcade/services/propagation/propagator.go:792`), used to
  decide whether a child inherits a parent's REJECTED verdict — which is what
  turns one rejection into a cascade.

The library has already responded to this structurally rather than numerically:
change is promoted to claimable only on `SEEN`, never on the 202
(`pkg/storage/process.go:326-335`), so a child cannot be built on a parent the
network has not accepted; and self-replenish is opt-in
(`pkg/storage/create.go:478-489`). Those are the right primitives.

**What is still missing** is observability: nothing reports the depth, so an
operator learns about it from a rejection storm. Depth is computable — walk
each input's parent through `known_txs` until a row has a `block_height`, which
`SetProof` sets (`pkg/storage/internal/metastore/knowntx.go:511`) — but it is
O(depth) per transaction on the hot path, and the useful bound is
network-specific.

**Recommendation:** track depth incrementally rather than by walking — a
`unconfirmed_depth` column on `known_txs` set at create time as
`1 + max(parent depth)`, reset to 0 on proof — and expose it in `StateReport`
alongside the existing tier counts. Refusing past a bound should stay opt-in
for the same reason the fee floor is: the real limit belongs to the node.
**Not implemented** because it needs a schema change on the hottest table plus
a benchmark to show the incremental maintenance is free, and I could not
measure that here.

The grounding app has already had to build this itself
(`MaxUnconfirmedDepth`, `/git/rule-110-arcade/internal/engine/worker.go:141-143`),
which is the clearest evidence it belongs in the library.

---

## 6. The crash window between broadcast and record — **Medium, partially preventable**

The premise asked whether a client crash between broadcast and record can
produce a double spend. **For coins the toolbox owns, it cannot**, and the
existing design is sound:

- Inputs are reserved **durably at `CreateAction`**, inside the same
  transaction that persists the metadata (`pkg/storage/create.go:167-195`), so
  the reservation is on disk long before any broadcast.
- `broadcastOne` posts and only then commits (`pkg/storage/process.go:285-333`),
  so a crash mid-flight leaves `was_broadcast = false` with the raw tx stored —
  which `FindResendable`
  (`pkg/storage/internal/metastore/monitor.go:250-266`) picks up and
  `SendWaitingTransactions` (`pkg/storage/status_updates.go:983`) re-broadcasts.
  **Re-broadcasting identical bytes is idempotent** — same txid, and arcade
  dedups at `/git/arcade/services/api_server/handlers.go:788-834`. It is not a
  double spend.
- `SweepStaleReservations` explicitly refuses to free inputs of a transaction
  `SendWaiting` still owns (`pkg/storage/status_updates.go:1041-1060`), which
  is the guard that would otherwise turn the crash window into a real double
  spend.

`spend_conflict.go` is **not** the guard for this. It is post-hoc: it parses
arcade's rejection text to decide which inputs are safe to release after a
conflict has already happened (`pkg/storage/spend_conflict.go:29-41`).

**The real gap is caller-provided inputs**, which is exactly the grounding
app's case. `resolveProvidedInputs` (`pkg/storage/create_inputs.go:26-68`)
resolves satoshis and a locking script and **never reserves the outpoint and
never checks whether it is already spent**. The toolbox has the answer
available — `outpointSpendable` (`pkg/storage/outputs.go:219-235`) reads
`ReservedBy` / `SpentBy` / `Frozen` from the utxostore — but it is used only
for *reporting* (`pkg/storage/outputs.go:47,169`, `pkg/storage/actions.go:96`),
never as a gate.

**Recommendation:** in `processNewTx`, before persisting, refuse any input the
local utxostore already records as `SpentBy` a **different** txid. That is a
precise, cheap, locally-decidable refusal, and comparing against the
transaction's own txid keeps idempotent re-submission working. **Not
implemented** because it needs care that a coin spent by an earlier attempt of
the *same* logical action is not misread as a conflict, and I would want the
reservation semantics for caller-provided inputs settled first — reserving them
is arguably the better fix, and doing half of it would be worse than doing
neither.

For an outpoint the wallet does not track at all, the toolbox genuinely cannot
know. That is the caller's write-ahead log to keep — and the grounding app
keeps one (`/git/rule-110-arcade/internal/engine/worker.go:174-180`).

---

## 7. `DefaultMaxOutputScript` overstates the limit by 2000× — **Medium, recommended**

`pkg/storage/provider.go:22` reports a maximum output locking-script length of
`1024 * 1024 * 1024` — 1 GiB — in `TableSettings`. The network's actual policy
is `MaxScriptSizePolicy: 500_000` (`/git/arcade/validator/validator.go`, pushed
at `ScriptVerifierGoBDK.go:183`), and a transaction is additionally capped at
`MaxTxSizePolicy: 10_485_760` and by arcade's 32 MiB request-body limit
(`/git/arcade/services/api_server/handlers.go:645`).

A wallet advertising 1 GiB is telling callers something no deployment will
honor. **Recommendation:** report a value derived from the deployment's policy,
defaulting to 500 000, and reject an oversized output script in `CreateAction`
with a local error naming the limit. This is a one-line default change plus a
guard, but it is a **behavior change to a published settings value**, so it
belongs in its own change with a note in the migration guide rather than
buried in an audit.

---

## 8. Local script verification omits the node's flags — **Low, recommended**

`executionOptions` (`pkg/storage/verifiers.go:108-140`) sets `WithForkID` plus
an era, and nothing else. The interpreter supports, and the node applies,
several more: `VerifyStrictEncoding`, `VerifyLowS`, `VerifyNullFail`,
`VerifyDERSignatures`, `VerifyMinimalData`, `VerifyCleanStack`,
`VerifySigPushOnly`, `VerifyMinimalIf`
(`script/interpreter/scriptflag/scriptflag.go:8-84`).

Practical exposure is limited but non-zero. Arcade runs with
`RequireStandard: false` and `AcceptNonStandardOutput: true`
(`ScriptVerifierGoBDK.go:202-203`), which removes most standardness pressure at
intake, and go-sdk's own signing produces low-S DER signatures. The gap that
matters is **caller-supplied unlocking scripts** — a covenant emitting a
non-minimal push or a non-push-only scriptSig passes here and can still be
refused downstream.

**Recommendation:** an opt-in `WithStrictScriptFlags()` that adds the
standardness set, so a caller building custom unlocking scripts can opt into
the stricter local check. **Not implemented** because I cannot verify which
flags this network's teranode actually enforces, and enabling the wrong ones
would refuse valid spends — precisely the failure mode this audit exists to
prevent.

---

## 9. `VerifyScripts` runs twice — **Low (cost), recommended**

Confirmed, and on **both** paths, not just the delayed one:

- Immediate: `processNewTx` (`pkg/storage/process.go:124`) then `broadcastOne`
  (`pkg/storage/process.go:302`).
- Delayed: `processNewTx` (`:124`) then `sendOneWaiting`
  (`pkg/storage/status_updates.go:1106`).

The bytes are immutable between the two calls — `broadcastOne` and
`sendOneWaiting` both re-parse the *stored* raw tx — so the second execution
cannot reach a different verdict. It is pure cost: a full interpreter run per
input, on the throughput-critical path, for a covenant that may be kilobytes of
script.

**Recommendation:** drop the re-verification at the broadcast sites and keep
the single authoritative check in `processNewTx`, which is where a failure is
actionable (nothing persisted, nothing sent). **Not implemented** because the
second call is also the only verification a transaction gets if it entered
`known_txs` by some path other than `processNewTx` — internalize
(`pkg/storage/internalize.go`) and sync (`pkg/storage/sync.go`) both write
rows — and proving that no such row is ever resendable needs more confidence
than an audit-time read gives. Removing a verification on a hunch is the wrong
trade.

---

## 10. nLockTime finality is never checked — **Low, recommended**

`wdk.NLockTimeIsFinal` exists (`pkg/wdk/locktime.go:18`) and is exposed on the
services interface (`pkg/wdk/services.interface.go:19`), but is **never called
from `CreateAction` or `ProcessAction`** — the only non-test references are the
definition and the interface. A non-final transaction is broadcast and arcade
answers 400 with ARC 476
(`/git/arcade/services/api_server/handlers.go:771-782`).

Low severity because a wallet that never sets a locktime cannot trip it, and
arcade's own check **fails open** (`/git/arcade/finality/mtp.go:126-144`).
**Recommendation:** call it in `ProcessAction` when `LockTime != 0`, and refuse
locally. Cheap; left out only because it is unreachable for every current
caller and I preferred to keep this change set to things with evidence behind
them.

---

## Broadcast failure taxonomy — verified correct

The 4xx / 503 / 5xx split is right, and a client cannot accidentally retry a
final rejection.

| HTTP | Client mapping | Retry? | Verified |
|---|---|---|---|
| 202 | `BroadcastResult{Status}`, `Rejected=false`, `err=nil` | n/a | `pkg/arcade/client.go:254-258` |
| 4xx | `Rejected=true`, reason in `ExtraInfo`, **`err=nil`** | **never** | `pkg/arcade/client.go:268-276` |
| 503 | `*BackpressureError` with `Retry-After`, result nil | safe | `pkg/arcade/client.go:260-262` |
| ≥500 / transport | plain error, result nil | **reconcile first** | `pkg/arcade/client.go:264-266` |

This matches arcade exactly (`/git/arcade/services/api_server/handlers.go:750`,
`:854-860`, `:862-864`), including that validation failures are 400 and never
5xx, so the "unknown fate" bucket really does mean unknown.

Three details worth recording as deliberately right:

- **A 4xx returns `err = nil`.** Retry logic keyed on `err != nil` therefore
  cannot retry a rejection by accident; the only way to retry one is to ignore
  `Rejected`. `broadcastWithBackpressure` (`pkg/storage/process.go:338-354`)
  retries **only** `*BackpressureError`, once.
- **An opaque failure is not retried blindly.** It leaves the transaction
  in-flight and reports `ServiceError` (`pkg/storage/process.go:315-322`);
  reconciliation happens through `SynchronizeTransactionStatuses`
  (`pkg/storage/status_updates.go:1192`) and the reject reconciler's two-pass
  guard (`pkg/storage/reconciler.go:199-237`), which requires two independent
  authoritative `GetTx` observations separated by a grace window before it will
  release an input.
- **Caller cancellation counts as neither success nor failure** for the circuit
  breaker (`pkg/arcade/client.go:186-218`), with a comment recording the
  incident where it did.

One asymmetry is worth flagging rather than fixing: the synchronous 4xx path
removes the phantom change (`pkg/storage/process.go:441-456`) while the async
one does not, deferring to the reconciler. That is documented and intentional
(`pkg/storage/process.go:411-416`), and the asymmetry is load-bearing — a
synchronous 4xx is a first-hand observation, an async REJECTED may be a false
positive.

---

## Premises that did not hold

Stated plainly, because acting on them would have made things worse.

**"Arcade's GoBDK enforces its minimum over the extended-format size."**
No. Measured against the real BDK engine: a transaction with a 1090-byte EF
encoding and a 73-byte standard encoding is accepted at 100 sat/kB with a
7-satoshi fee — `floor(100·73/1000)`, not `floor(100·1090/1000) = 109`. The fee
is priced against the standard serialization. Since the toolbox rounds *up*
where the node truncates *down*, a wallet at exactly 100 sat/kB pays at or above
the floor for the size it priced. The genuine hazard is the size **estimate**
(finding 3), and `docs/application-throughput-playbook.md:474-500` should be
corrected — its recommendation of 125 sat/kB is still sound as margin, but its
stated reason is not the mechanism.

**"Arcade validates each input against the era of the UTXO being spent."**
Not arcade — arcade does the opposite, deliberately. It reports an
unknown-parent sentinel for every input
(`/git/arcade/validator/validator.go:169-172`) so that everything is judged
post-Genesis and post-Chronicle, which was the fix for its own issue #214. The
per-UTXO-height rule is real, but it is **teranode's**, applied with real
heights after arcade has already answered 202. The finding stands and is the
most valuable one here; only the attribution changes — and the change matters,
because it explains why the failure arrived asynchronously with no synchronous
4xx to correlate it against.

**"Arcade enforces a mempool ancestor limit."** Not in arcade, and not in BDK
either. The configured `LimitAncestorCount` is a dead value with no setter
(finding 5). The limit is real but lives further down.

---

## What a client must still do itself

Honestly delimited: these are things the toolbox cannot know, not things it has
merely not got to.

1. **Keep a write-ahead record around the sign/broadcast boundary.** Signing
   broadcasts. For an input the wallet does not own — a caller-supplied
   outpoint — the toolbox has no reservation and no spend state, so only the
   caller can know that an attempt was in flight when the process died. The
   grounding app's `StatusAttempting` row
   (`/git/rule-110-arcade/internal/engine/worker.go:174-180`) is the right
   shape: write the intent before building, retract it on any error that
   provably predates the broadcast.

2. **Distinguish "not broadcast" from "unknown".** A caller that cannot tell
   these apart must treat every failure as unknown and reconcile. The grounding
   app's `ErrNotBroadcast` (`/git/rule-110-arcade/internal/chain/step.go:22`)
   marks the difference at the one line where it changes
   (`step.go:183`).

3. **Declare `unlockingScriptLength` on every caller-provided input, and
   over-estimate it.** The toolbox silently assumes 107 bytes when it is absent
   (`pkg/storage/create.go:587-591`). With finding 3's guard enabled an
   undercount becomes a clean local error; without it, it becomes a remote 4xx.

4. **Choose the ancestry depth bound.** The toolbox can (and should) expose
   depth; only the caller knows what the network it is on will tolerate, and
   only the caller can decide whether to stall or to fan out.

5. **Verify custom unlocking scripts against the contract before signing.**
   The toolbox's verifier runs the script engine, not your covenant's
   intent. The grounding app checks its own covenant locally first
   (`/git/rule-110-arcade/internal/chain/step.go:178-180`), which converts a
   whole class of opaque remote rejections into a named local failure.

6. **Supply the network's activation heights and fee floor.** Both are
   deployment configuration. The toolbox now consumes them
   (`WithGenesisActivationHeight`, `WithMinBroadcastFeeRate`) but will not
   invent them, because a wrong guess refuses valid transactions — which is a
   worse failure than the one being prevented.
