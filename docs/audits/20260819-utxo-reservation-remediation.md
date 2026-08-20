# UTXO selection & reservation audit — remediation closeout

**Audited invariant.** *It must be impossible for two processes to select the
same UTXO and create a double spend.*

**Scope.** `pkg/utxostore/` (interface + `sqlstore`/`aerostore`/`memstore`
backends), `pkg/storage/internal/funder/`, and the reservation-touching parts of
`pkg/storage/` (create, create_inputs, process, abort, status_updates,
reconciler, sync, outputs, storage_manager), plus `internal/sqlkit`,
`internal/sqltx`, `pkg/monitor`.

**Baseline.** `f832ba0` — *Merge pull request #19 from
galt-tr/storage/scale-coverage-and-conformance-issues* (2026-08-19).

**Final remediation SHA.** `14a27a4` — *Say the store contract once, and name
what a wallet client can act on* (2026-08-19). 23 commits, `f832ba0..14a27a4`.

**Source review.** `go-arcade-toolbox-utxo-reservation-review.md` — 9 finder
angles (5 correctness, 3 cleanup, 1 altitude) plus a conventions pass and
line-by-line lead analysis. 5 P0, 7 P1, 10 P2, one altitude deep-fix, and a
cleanup-nit list. Nothing was posted to GitHub.

**Closeout date.** 2026-08-19.

---

## The deep fix: pinned state

The review's root cause was that a coin sat at plain `reserved` across the whole
broadcast round-trip with **no committed pre-broadcast state**, so six bespoke
guards each independently tried to mean "don't free the inputs of an in-flight
send" — and at least two had holes. The remediation replaces all six with one
committed bit and one arbiter row:

1. **Pin-with-raw-tx atomicity.** `pinned` is written as the *first* statement of
   the unit of work that stores a signed transaction's bytes. In Mode A the pin
   rides the same database transaction as the raw tx (one commit, no window); in
   Mode B it commits *before* the metadata, so a rolled-back meta half can only
   cost availability, never leave broadcastable bytes over sweepable inputs.
2. **Release paths are structurally pin-blind.** `ReleaseReservation` skips
   pinned rows and `FindStaleReservations` cannot see them, so only the
   transaction's own lifecycle ends a pin — Spend, Unspend, a token-guarded
   `ReleaseOutpoints`, `Unpin`, or deletion. `Unpin` deliberately leaves the row
   RESERVED, so a crash degrades to a stale reservation, never to a free coin.
3. **One-row `known_txs` arbiter.** `TransitionToAborted` (the abort fence) and
   `ClaimForSend` (the broadcast gate) share one positive-CAS predicate over the
   SAME row, so abort and broadcast contend and exactly one can win.
   `ReclaimStaleSend` recovers a claimant that died mid-POST, taking its cutoff
   from the same `resendGrace` constant `FindResendable` selects with.
4. **Fact-mode spend.** At broadcast-accept the spend is a fact of the network,
   not a transition the store may adjudicate: `Spend(force=true)` records over a
   lost reservation and a freeze, and the caller's tolerance collapses to
   `NotFoundError` (genuinely external) alone.
5. **Fence-first sweep.** `SweepStaleReservations` kills the transaction row
   through the same abort CAS *before* any coin moves, and takes its disposition
   from that row rather than from the coin. `reservationResendable` is deleted.
6. **Outbox-routed Mode B abort.** The abort commits no utxostore write inside
   `meta.Do`; phase 1 commits fence + durable intent atomically, phase 2 executes
   inline with `DrainOutbox` replay.
7. **MINED repair + backfill.** A header-verified proof on a written-off row
   (`invalidTx`, `doubleSpend`, `stuck`, `aborted`) diverts *ahead of* the
   terminal guard to re-spend the released inputs; a `CheckProofs`-borne backfill
   heals rows already carrying the divergence.

The claim hot path is untouched by construction: a pinned row is a reserved row,
which `idx_utxos_claim`'s partial predicate (`reserved_by IS NULL …`) already
excludes, and the three PostgreSQL claim statements come through the whole
remediation byte-identical apart from the shared column list they `RETURNING` —
which is why `TestClaimUsesPartialIndex` passes unmodified over its 250k-row pool
at every step.

---

## Finding-by-finding

Status legend: **FIXED** · **FIXED-WITH-AMENDMENT** (fixed, but not as the review
specified — the amendment is recorded) · **ALREADY-FIXED-PRE-AUDIT** ·
**RECLASSIFIED** (the finding as written does not hold) · **NOT ADDRESSED**.

### P0 — double spend, high reachability

| # | Status | Fix commit(s) | Guarding test(s) | Mechanism |
|---|---|---|---|---|
| **P0-1** Caller-provided inputs are never reserved | FIXED | `Reserve caller-named inputs, not just the ones the funder picked` (store half); `Hold the coins a caller named, so two actions cannot both have one` (provider half) | conformance `ReserveOutpointsExclusivity` (4 backends); provider conformance `ProvidedInputExclusivity` (PG Mode A, Aerospike/PG hybrid Mode B, memstore+SQLite, REST); `TestCreateAction_ProvidedInputReserved`, `…_ProvidedInputExclusivity`, `…_ProvidedInputSpentOnAccept`, `…_ProvidedInputRefused`, `…_ProvidedInputBeatsFunder`, `…_ProvidedInputPinnedWhenDelayed`, `…_ExternalProvidedInputUntouched`; `TestReserveOutpointsAmbientTxLeavesNothingReserved` (commits the ambient transaction on purpose — the all-or-nothing guarantee has to survive that) | New all-or-nothing `ReserveOutpoints` holds the caller's named wallet-owned coins under the action's reference, run BEFORE `Fund` so this action's own funder cannot re-allocate one. External inputs stay untouched (no inventory row, no exclusivity authority); refusals surface as `wdk.ErrInputUnavailable` wrapping the typed cause. |
| **P0-2** `spendReservedInputs` swallows a lost reservation as "external input" | FIXED | `Record a spend as a fact once the network has accepted it` (store half); `Record an accepted broadcast's spend as a fact, not a request` (caller half) | conformance `SpendFactMode`; `TestAcceptedBroadcast_RecordsTheSpendOfAStrandedReservation`, `…_ExternalInputIsStillTolerated`, `…_InputSpentByAnotherTxIsAHardError`, `…_ContentionIsRetryableAndConverges`, `…_ToleratedItemsCannotExplainAWholeCallFailure` | `Spend` gains a `force` parameter that picks preconditions, never effects. Fact mode records over a reservation mismatch and a freeze; only `NotFoundError` (external) and a `SpentError` by a *different* spender survive. The caller tolerates `NotFoundError` only when the top-level error is `ErrBatch`, the store's promise that per-item verdicts are the whole accounting. |
| **P0-3** Abort leaves the `known_txs` row broadcastable | FIXED | `Fence aborted known txs and make txid binding a CAS` (primitives); `Claim the row before broadcasting, so the abort fence has an arbiter` (send gate); `Fence the raw tx as part of the abort, before any coin moves` (abort wiring) | provider conformance `AbortFence` — the audit's acceptance test, which tries all three doors (the sweep, an explicit `SendWith` re-drive, and a re-drive after the coin was handed to a second action) and asserts on the oracle's broadcast counter. It runs on **three of the four** provider legs — PostgreSQL Mode A, the Aerospike/PG hybrid Mode B, and memstore+SQLite — and **deliberately skips the REST client leg**, which supplies no `RejectReleaseEnv`: across the HTTP hop the oracle and movable clock are unreachable, and the subtest refuses to run rather than pass without being able to count broadcasts (it says so in the skip message); `TestAbortAction_FencesTheRawTx`, `…_LosesToAnInFlightSend`, `TestSendWith_RefusedAfterTheAbortFence`, `TestSendWith_RefusedForEveryTerminalStatus`, `TestQueueDelayed_RefusesToRequeueAFencedKnownTx`, `TestProcessNewTx_RefusesToResurrectAFencedKnownTx`, `TestSendOneWaiting_DropsARowFencedMidSweep`, `TestSendWaiting_RecoversAStrandedSend`, `TestBroadcastOne_TakesOverAStrandedSend`; metastore `knowntx_fence_test.go` CAS matrices + `TestFindResendable` | `fenceAborted` runs the transactions-row CAS, `TransitionToAborted` and `ClearSpentBy` in one metastore transaction (both tables always share a database), so "aborted but still broadcastable" is unrepresentable. Both send paths must first win `ClaimForSend` on the same row. `raw_tx` is deliberately NOT nulled — the fence is structural on status, and the bytes have live post-abort consumers. |
| **P0-4** Stale-reservation sweep frees a still-signable unsigned tx's inputs | FIXED | `Pin a reservation so no janitor can free an in-flight send's inputs` (store half); `Pin a signed transaction's inputs in the transaction that stores its bytes` (provider half); `Kill the transaction before reclaiming its coins` (fence-first sweep) | conformance `PinLifecycle`, `PinScopingAndNoOps`, `PinClearedBySpendAndUnspend`, `ReleaseOutpointsOverridesPin`, `FindStaleReservationsSkipsPinned`, `FindStaleReservationsIncludingPinned`; `TestProcessNewTx_PinsReservedInputsWithTheRawTx`, `TestSweepStaleReservations_LeavesALiveTransactionAlone`, `…_WillNotCancelAQueuedDelayedSend`, `…_ReleasesAFunderOrphan`, `…_ReclaimsAnOrphanPin`, `…_ReleasesStuckReservation`, `…_WarnsRatherThanFreeingACompletedSpend`, `TestSweepRacingCreateActionNeverDoubleAllocates`, `TestSweepLeavesARedrivableReservationAlone`, `TestASweptReservationIsImmediatelyClaimableAgain`, `TestProcessAction_AcceptedBroadcastClearsThePin`, `TestAbortAction_LiftsThePinBeforeReleasing` | The old guard could not close this — it asked whether broadcastable bytes ALREADY existed, and a late signer's do not exist yet. The sweep is now fence-first: nothing is released until the transaction row is killed through the same CAS `AbortAction` uses. Its abortable set is deliberately narrower than `AbortAction`'s (`unprocessed`/`nonfinal` are owned by `SendWaiting` and by an nLockTime) — what a user may abort by decision, a janitor may not abort on a timer. The 15m TTL is demoted to a backstop. |
| **P0-5** Mode B: utxo release inside `meta.Do` is not atomic with the metastore tx | FIXED | `Fence the raw tx as part of the abort, before any coin moves` | `TestAbortViaOutbox_FenceSurvivesAFailedUtxoHalf`, `…_UnsignedEnqueuesOnlyTheRelease`, `…_ParkedReleaseIsHealedByTheSweep`, `TestAbortOutboxKey_IsATxidWidthSurrogate`, `TestReconciler_OutboxDrain_CrashRecovery`, `TestOutbox_SkipLocked_Postgres` | In Mode B the `Do` commits no utxo write at all. `abortViaOutbox` mirrors `releaseViaOutbox`: phase 1 commits fence + durable intent atomically on the metadata side, phase 2 executes inline best-effort with `DrainOutbox` replay. Two new op types (`ABORT_RELEASE`, keyed by `sha256("abort:"+reference)` because an unsigned abort has no txid; `ABORT_REMOVE_CHANGE`). `abortTxRow` now states the invariant that callers must sit OUTSIDE any ambient `meta.Do`. |

### P1 — double spend / coin loss, narrower trigger

| # | Status | Fix commit(s) | Guarding test(s) | Mechanism |
|---|---|---|---|---|
| **P1-1** Aerospike unguarded read-then-delete destroys a concurrently-reserved coin | FIXED (memstore half RECLASSIFIED) | `Guard aerostore deletes so a raced coin is never destroyed` | `TestAerostore_RemoveDeleteRace` (one case per filter conjunct via a `removeRaceHook` seam; each conjunct verified load-bearing by deletion), `TestAerostore_RemoveClaimConcurrent` (real churn, no seam), `TestAerostore_RemoveDurableDelete`; conformance `Remove`, `RemoveByMintTx`, `RemoveSpentBy` | Each delete re-asserts its classification server-side through one `deleteRecordGuarded` helper returning removed / guard-lost / absent. `FilterExpression` rather than `GenerationPolicy`, because generation counts ANY concurrent write and a benign Promote would abort a legitimate remove. Per-caller filters: `removeOne` unreserved∧unspent∧unfrozen; `RemoveByMintTx` unreserved∧unspent (a frozen phantom is still removed, per contract); `RemoveSpentBy` reuses the scan's own `spentBy == spendingTxID`. **The review's memstore half is not a defect**: memstore's methods are single critical sections, so nothing can land between a row's classification and its delete — recorded as a package doc note rather than changed. |
| **P1-2** Sync mints at `TierMined` ignoring spent state; writer picks `stores[0]` | FIXED | `Stop sync from resurrecting spent coins, and gate writes on the active store` | `TestSync_SpentAtSource_IsNotMinted`, `…_ReservedAtSource_IsNotMinted`, `…_UnspentChange_IsStillMinted`, `…_UntranslatableSpentBy_StaysNullAndUnminted`, `TestSync_OldSourceChunk_WarnsThatNothingWasRebuilt`, `TestListOutputs_SpendabilityIntersection`, `TestManager_WriteRefusedWhenNotTheActiveStore`, `…_WriteAllowedOnTheActiveStore`, `…_SetActiveOnCurrentStoreLiftsTheRefusal`, `…_SetActiveRejectsNeverAvailableStore`, `…_WriteWithoutActiveStorageErrors`, `…_BootstrapWritesAllowedBeforeAvailability` | Three halves. **Receiver**: the mint gate becomes `Change && TxID != nil && Spendable && SpentBy == nil`, with the source-local spending id translated through the chunk's old→new map (an untranslatable id degrades to NULL, never to mintable). Minting-then-marking was rejected — the utxostore has no token-less spend, so the coin would transit a claimable state. **Source**: `GetSyncChunk` intersects each change output with its own utxostore through the same `outpointSpendable` helper the read path uses, so the wire cannot grow a second definition of spendability; spent, reserved (pinned implies reserved), frozen and absent all ship non-spendable. **Manager**: `getActiveWriter` returns `ErrStorageNotActive` unless the store's identity key matches the user's selected active storage; `MakeAvailable` and `SetActive` are the two documented exemptions. A pre-change source ships false for everything and is NOT self-healing — the remedy is a resync into a fresh target, warned about after commit. |
| **P1-3** Sweep frees a broadcast-accepted tx's still-reserved inputs (`txRow==nil`) | FIXED | `Record an accepted broadcast's spend as a fact, not a request` (the zero-rows hole); `Kill the transaction before reclaiming its coins` (deletes the sweep's side) | `TestAcceptedBroadcast_NoTransactionRowRefusesToCommit`, `…_ResolvesTheRowAcrossUsers`, `…_TransactionRowQueryErrorPropagates`, `TestSweepStaleReservations_WarnsRatherThanFreeingACompletedSpend` | `applyAcceptedBroadcast` resolves the transaction row itself, CROSS-USER (a `SendWith` carrying another user's txid previously found nothing and took exactly this branch). A query error propagates — "could not be read" and "there is no row" are different facts — and zero rows is a hard error raised BEFORE any write. `firstTxByTxID`'s identical swallow is deleted. Spend-first ordering makes a returned error safe in Mode B. |
| **P1-4** `processNewTx` re-drive + unconditional `SetTxID` → two resendable txs | FIXED | `Fence aborted known txs and make txid binding a CAS` (the fence); `Pin a signed transaction's inputs…` (the diagnosable pre-check); `Say the store contract once…` (the exported sentinel) | `TestProcessNewTx_DivergentReDriveRefused`, `…_SameTxIDReplayIsIdempotent`, `TestDivergentReDriveCASErr_CarriesTheCause`, `TestREST_ErrorMapping_DivergentReDrive`; metastore `SetTxID` cases in `runMetastoreSuite` (SQLite + PostgreSQL) | `SetTxID` becomes a CAS on the NULL / same-txid arms; a divergent re-drive hard-fails with `ErrTxIDMismatch` instead of silently repointing the row and orphaning a still-resendable raw tx. A pre-check sited BELOW the status switch names the binding it protects (so only `unprocessed` reaches it and aborted rows keep the better message). Re-reading the winning txid is deliberately forbidden — the read is not atomic with the CAS. Exported as `storage.ErrDivergentReDrive` with a 409 wire row. |
| **P1-5** SQLite DSN uses the wrong driver's pragma keys | **FIXED-WITH-AMENDMENT** | `Harden SQLiteDSN to the driver-portable _pragma=name(value) form` | `TestSQLiteDSNAppliedPragmas` — opens a real file-backed DB through `SQLiteDSN` and asserts what the driver ACTUALLY applied via `PRAGMA` queries; `TestSQLiteDSNPragmas` — the DSN string shape and the absence of the shorthand keys | **Amendment: this is hardening, not a live bug.** `modernc.org/sqlite` **v1.55.0**, the version this module pins, *does* parse the mattn-style shorthand (`_journal_mode=WAL`, `_busy_timeout=5000`, `_foreign_keys=on`), so those pragmas were in fact being applied and the review's "actual runtime: journal_mode=delete, busy_timeout=0, foreign_keys=off" did not hold on the pinned driver. Every *earlier* modernc version silently dropped them with no parse error, so a routine downgrade — or a bump to a version that changes shorthand support again — could silently regress the concurrency posture. `SQLiteDSN` now expresses every pragma through `_pragma=name(value)`, the one form modernc has honored since v1.14.7 (`_txlock=immediate` is unaffected; it configures BEGIN mode, not a pragma). The **runtime pragma test is the guard**: because v1.55.0 parses both forms it passes against the pre-hardening DSN too, so its job is to pin the applied pragmas so a future driver change cannot silently revert them. |
| **P1-6** Wrong reconciler release is permanent — terminal-failure guard blocks repair | FIXED | `Take back the coins of a written-off transaction the network mined` | `TestMinedAfterVerifiedRelease_ReclaimsTheReleasedInputs` (the P1-6 acceptance test), `TestMinedAfterAbort_RepairsRatherThanSilentlyCompleting`, `TestMinedRepair_IsIdempotent`, `…_RefusesAnUnverifiableProof`, `…_CompetingSpendIsAlertedNotFatal`, `…_DeferredProofIsRecoveredByTheBackfill`, `TestMinedBatchOnAFencedRow_RepairsToo`, `TestCheckProofs_BackfillsAPreRepairDivergence`, `TestBackfill_AnUnrepairableRowDoesNotBlockThePage`, `TestBackfill_ARevisedVerdictLeavesTheWorkList`, `TestProofBetweenReconcilerPasses_BlocksTheRelease`, `TestHybrid_MinedAfterAbort_RepairsTheCoins` | `ApplyStatusUpdate` diverts MINED/IMMUTABLE on a written-off row (`minedRepairStatuses`: invalidTx, doubleSpend, stuck, **aborted** — spelled out, because 'aborted' is not derivable from `IsTerminalFailure`) to `applyMinedRepair`, AHEAD of the terminal guard rather than through it. SEEN-class events are untouched: only a header-verified proof repairs, because a mempool sighting is not proof the wallet was wrong. The repair verifies the BUMP with `applyMined`'s refusals, then in one `meta.Do` fact-spends the inputs parsed from `kt.RawTx`, `SetProof`s, CASes the row to completed, re-mints change at `TierMined`, prunes. Coins first, so a crash leaves work the backfill re-reaches. A materialized double spend is an ERROR-level ALERT that continues, not a failure. The review found only the P1-6 half; the remediation's own probe found a second, worse door — **'aborted' is not a terminal failure, so no guard saw it at all**, and a MINED walked into the ordinary apply and silently completed the known tx with no fact-spend and no change re-mint. |
| **P1-7** `New(SQLite)` doesn't pin a connection; unguarded spend UPDATE | FIXED | `Guard sqlstore's mutations in the WHERE, and validate the SQLite pin` | `TestGuardedMutationsAreOneGuardedStatement`, `TestSpendGuardsAreInTheWhere`, `TestRemoveGuardsAreInTheWhere` (statement-level, fail against the old shape), `TestNewRejectsUnpinnedSQLitePool`; conformance `SpendLifecycle`, `SpendPerItemErrors`, `SpendPrecedence`, `SpendFactMode`, `Remove`, `RemoveByMintTx` | Closed from both ends. **Invert to write-first**: the guarded UPDATE/DELETE carries every precondition itself, so the write is self-defending on any engine and the pre-flight `SELECT … FOR UPDATE` is dropped (on PostgreSQL it serializes on the row lock it takes anyway). Only a write that matched nothing pays for a classifying read, which reproduces the per-mode taxonomy exactly; a row that comes back eligible means a peer released and re-reserved mid-flight, so the mutation retries once before reporting `ErrContention`. **Validate rather than mutate the pool**: `sqlstore.New` and `metastore.New` refuse a shared SQLite handle not pinned to one connection, naming `SetMaxOpenConns(1)` — silently resizing a pool the caller owns (in Mode A, the very handle the other store shares) is the worse surprise. `forUpdate()` keeps exactly one caller, `classifyForReserve`. |

### P2 — correctness / availability

| # | Status | Fix commit(s) | Guarding test(s) | Mechanism |
|---|---|---|---|---|
| **P2-1** `ClaimLargestInsufficient` tie order diverges | **FIXED-WITH-AMENDMENT (direction REVERSED, user sign-off)** | `Break largest-insufficient ties newest-first, everywhere` | conformance `ClaimTieBreak` (mints four equal-value coins as four distinct transactions and asserts the claimed OUTPOINTS, since satoshi values cannot tell them apart; gated on `WithExactSelection`, aerostore skips); `TestClaimUsesPartialIndex` | **Amendment: the review proposed aligning sqlstore to the documented insertion-order tie-break. That direction was rejected, with user sign-off, on an index-servability constraint.** A descending walk asking for `(satoshis DESC, seq ASC)` cannot be served by `idx_utxos_claim`: PostgreSQL must materialize and sort the matching rows — an O(pool) sort per claim on precisely the equal-value pool this ordering governs — and it breaks `TestClaimUsesPartialIndex`, the EXPLAIN guard that keeps the 1000-TPS hot path a pure index walk. So the **spec moved to the engine**: `ClaimLargestInsufficient` breaks ties NEWEST first, `ClaimSmallestSufficient` and `ClaimExact` stay oldest first, memstore's comparator flipped to match, and the asymmetry is documented as what it is — the tie direction follows the index each shape walks. No SQL and no sort logic changed. Accepted trade-off recorded below in follow-ups. |
| **P2-2** Claims bypass the lock-error retry | **ALREADY-FIXED-PRE-AUDIT** | PR #19 (`f832ba0`, the audit baseline itself) | `TestClaimRetriesLockErrors`, `TestClaimRetryDoesNotDoubleAllocate`, `TestClaimSurfacesLockErrorAfterRetriesExhausted` | Verified against the baseline tree: at `f832ba0` `runClaim` already wrapped its statement in `sqlkit.WithRetry`, and `claim_retry_test.go` already existed. The review's P2-2 text describes the pre-#19 code. No remediation commit was needed. |
| **P2-3** Fund's compensating release reuses a possibly-canceled context | FIXED | `Run the compensating reservation release on a detached context`; later folded into one helper by `Hold the coins a caller named…` | `TestFund_ReleaseRunsOnDetachedContext`, `TestCreateAction_ModeBCompensationRunsOnDetachedContext` (both fail before the change; the fakes record `ctx.Err()` and deadline presence at call time) | Both terminal releases run on `context.WithoutCancel(ctx)` bounded by a 5s timeout and log whether the request context was already canceled. `WithoutCancel` preserves VALUES, so in Mode A the ambient `sqltx` transaction is still found and the release enlists rather than opening a second one. The mid-flow `releaseOutpoints` tails (`drainBatch`, the throughput fast path) deliberately stay on the request context — they are not compensations, and detaching them would only mask cancellation on the success path. Both sites now go through `releaseReservationDetached`. |
| **P2-4** Mode A ambient-tx holds SKIP LOCKED locks → spurious insufficient-funds | **FIXED-WITH-AMENDMENT (probe relocated)** | `Tell a starved funding pass from an empty pool` | `TestFund_HiddenInventoryIsContentionNotInsufficientFunds`, `TestFund_NoHiddenInventoryStaysInsufficientFunds`, `TestFund_LockedTierDoesNotPreemptAnotherTier` (**the regression test for the rejected design**), `TestFund_StoreWithoutProbeIsUnchanged`, `TestFund_ProbeFailureFallsBackToInsufficientFunds`, `TestClaimableExistsSeesWhatAnAmbientTxHidFromTheClaim`, `TestClaimableExistsAnswersFalseForEveryUnclaimablePool`, `TestClaimableExistsOnSQLite`, `TestClaimableExistsRejectsAnUnderspecifiedScope`, `TestClaimableProbeUsesPartialIndex`, `TestClaimProbePredicateMatchesTheClaimStatements`, `TestClaimProbeSQLIsANonLockingExistenceCheck`, `TestClaimsStillReportNoneOnAnEmptyResult` | The store gains the missing FACT, not a verdict: `ClaimableExists`, a non-locking `SELECT EXISTS` over the claim's own candidate predicate, reachable by type assertion so backends that cannot answer simply do not offer it. **Amendment: the probe is NOT wired where the original design put it.** Review found that probing inside the store on every empty claim would (a) tax the ALLOCATING path — an empty `ClaimSmallestSufficient` is the ordinary precondition of the drain and a denominated `ClaimExact` misses on every tier holding no fuel — costing a query per payment at full throughput on an already-documented PostgreSQL limiter, and (b) let one big coin locked by an uncommitted peer PRE-EMPT a fund a lower tier could have covered, aborting the walk mid-pass. So the probe moved to the funder's **TERMINAL failure point**: a pass about to report `ErrNotEnoughFunds` asks each tier once; a hit means `utxostore.ErrContention` and the existing bounded jittered retry engages; a probe that errors is logged and swallowed. Drift is guarded by `claim_predicate_test.go`, which requires the probe's predicate to EQUAL each claim statement's `WHERE…ORDER BY` segment (equality, not containment); on SQLite the claim statements are BUILT from the shared constant, so drift is impossible by construction. |
| **P2-5** aerostore `probedEmpty` per-process negative cache | FIXED | `Stop the claim cache from hiding coins it can no longer see` | `TestClaimCache_EmptyProbeSuppressionExpires`, `…_EmptyTTLZeroNeverSuppresses`, `…_InProcessRestoreBeatsTheTTL`, `TestClaimCache_CrossProcessMintVisibility` (integration, real cluster) | The empty-bucket verdict now EXPIRES: `probedAt` + `emptyTTL` (default 1s, `WithClaimCacheEmptyTTL`, 0 = never trust). One second bounds cross-process staleness and still caps idle probing of a genuinely empty tier at one query per bucket per second. |
| **P2-6** aerostore cache: (a) stale candidates, (b) filter-less refill, (c) snapshot-tier invalidation | FIXED | `Stop the claim cache from hiding coins it can no longer see` | (a) `TestClaimCache_EmptyRefillDropsStaleCandidates`; (b) `TestClaimCache_SufficientCoinOutsideSample`, `…_FilteredProbeFallback`, `…_NoFilteredProbeWhenBucketEmpty`, `…_NoFilteredProbeWhenPartiallySatisfied`; (c) `TestNoteClaimableAllTiers`, `TestClaimCache_PromoteRaceRestoreVisibleInNewTier`, `TestAerostore_PromoteRestoreRace`; plus `TestClaimCache_ProbeErrorLeavesSnapshotIntact`, `TestClaimCache_SingleFlight` | (a) A refill REPLACES the snapshot unconditionally — an unfiltered probe returning nothing proves every cached candidate dead and cannot be a truncation artifact. (b) When a refill yields the caller nothing at all and the bucket is not empty, one value-filtered probe asks the server the narrow question — only when the result is empty, since topping up a partial one could return a row still in the snapshot. (c) The restore paths derive the claimKey's tier SERVER-side, so Release/Unspend/Unfreeze now mark all three tier keys (three map ops, no I/O); Mint and Promote keep marking the exact tier they wrote. |
| **P2-7** aerostore reserve CAS not idempotent under client auto-retry | **FIXED-WITH-AMENDMENT (pinned by assertion; nonce design documented, not built)** | `Stop the claim cache from hiding coins it can no longer see` | `TestReservePolicy_NoRetries`, `TestWarnOnDynamicClientConfig` | **Amendment: the path is UNREACHABLE as configured.** `NewWritePolicy` sets `MaxRetries = 0`, so nothing auto-retries the reserve, and a per-call nonce to make the CAS retry-safe would be unreachable code guarding an unreachable path. The design is written down in `reservePolicy` and **pinned by an assertion that fails the day this package stops holding to it**. The one route no assertion here can cover is a *dynamic client configuration* — enabled by the `AEROSPIKE_CLIENT_CONFIG_URL` environment variable rather than by any code of ours, it patches a COPY of every write policy at command time, so `max_retries` there would re-enable retries invisibly. That one is announced with a startup warning rather than refused, because the damage is a coin held by an abandoned token until the stale sweep reclaims it. |
| **P2-8** aerostore misc: unbounded bucket map, ignored ctx | FIXED | `Stop the claim cache from hiding coins it can no longer see`; `Say the store contract once…` (`setPinned`'s ctx + recordset goroutine leak) | `TestClaimCache_LRUEviction`, `…_EvictedBucketStillClaims`, `…_UnboundedWhenCapDisabled`, `…_HonorsContext`, `TestQueryHonorsContext` | The bucket map is capped LRU (default 1024, `WithClaimCacheBuckets`) because `claimKey` embeds the user id; eviction can only cost a re-probe, since the sole thing an evicted bucket carried was a suppression and in-flight holders finish against CAS-guarded hints. The claim, release and scan loops honor their context — probe, both bucket walks, `ClaimExact`, `ReleaseReservation` and `RemoveSpentBy` — with only the single-flight wait uninterruptible, bounded by one probe. |
| **P2-9** Latent: cross-DB misrouting via the shared `sqltx` key | FIXED | `Bind ambient transactions to their owning *sql.DB` | `TestForeignTransactionNotEnlisted` (two separate SQLite utxostores), `TestModeB_ForeignTransactionNotReused` (two separate SQLite metastores), `TestFromAndWith`, `TestAmbientTransaction`, `TestSharesDatabase` | `With` and `From` carry/require the owning `*sql.DB`, so `From` reports a hit only under pointer equality with the owner it is passed — a convention becomes an enforced predicate. Mode A is unaffected (`SharesDatabase` already implies pointer equality); in Mode B a foreign transaction is never enlisted and the store falls back to its own pool exactly as if ctx carried none, so `runClaim`'s ambient-bypass no longer misfires and out-of-mode claims keep their `sqlkit.WithRetry`. `sqlkit.Execer` now takes a concrete `*sql.DB`. |
| **P2-10** Latent: `reservationResendable` drifted from `FindResendable` | FIXED | `Kill the transaction before reclaiming its coins` | the `TestSweepStaleReservations_*` suite; metastore `TestFindResendable` | `reservationResendable` is **deleted**. The sweep's disposition now comes from the transaction row through the same abort CAS, so there is no second Go re-encoding of `FindResendable`'s SQL predicate left to drift. (The review called it "safe today by accident"; it had in fact already drifted strictly broader and pinned the inputs of terminal rows forever.) |

### Deep-fix (altitude)

| Item | Status | Fix commit(s) | Guarding test(s) | Mechanism |
|---|---|---|---|---|
| No committed pre-broadcast "pinned" state; six bespoke guards; `spendReservedInputs` error-sniffing | FIXED | `Pin a reservation…`, `Pin a signed transaction's inputs…`, `Record a spend as a fact…` (×2), `Fence aborted known txs…`, `Claim the row before broadcasting…`, `Fence the raw tx as part of the abort…`, `Kill the transaction before reclaiming its coins`, `Take back the coins of a written-off transaction the network mined` | the union of the P0-2/P0-3/P0-4/P0-5/P1-3/P1-6 test sets, plus four cross-backend conformance families — `Pin*`, `SpendFactMode` and `FindStaleReservations*` at the store level (memstore, SQLite, PostgreSQL, Aerospike), and `AbortFence` at the provider level (three of the four legs; see P0-3 for the REST skip) | Exactly the review's prescription, carried further: `reserved → pinned` committed in the same transaction that stores the raw tx; `FindStaleReservations` excludes pinned rows at the store; abort contends on the same `known_txs` row as the broadcast; `reservationResendable` deleted outright; the TTL demoted to a backstop for genuine Mode-B orphans; and `Spend` gains a fact-recording mode so the caller collapses to "NotFound = external, anything else = hard error". |

### Cleanup nits

| Nit | Status | Fix commit(s) / note |
|---|---|---|
| `validateClaim`, `joinBatch`, the batch-error helper, the `RemoveByMintTx` mint-output guard triplicated across backends | FIXED | `Say the store contract once…` — the rules now live once in `pkg/utxostore/validate.go`, exported so an out-of-tree backend registered through `Register` enforces the same contract in the same words. Deduped: `ValidateClaim`/`ValidateScope`/`ValidateReservation`, `ValidateReserveOutpoints`, `ValidateMint`, `ValidateSpend` (3 backends but 4 sites — sqlstore validated in both the per-op loop and the set-based batch), `ValidateMintOutpoints`, `JoinBatch`, `BatchCountErr`. Error text now reads `utxostore:` rather than the backend prefix; nothing matched on those strings, deliberately, since they name a programmer error. |
| `changeOutpoints` / `changeOutpointsByTxID` / `changeOutpointsByTxIDs` three near-identical copies | PARTIALLY FIXED (RECLASSIFIED remainder) | `Say the store contract once…` — `changeOutpointsByTxID` becomes a one-element call of the bulk form (logically identical predicate). `changeOutpoints` deliberately KEEPS its own query: it is scoped to a `(userID, transactionID)` and the bulk form to a txid, and the same txid legitimately appears under more than one user — delegating would let an abort reach into another user's change. |
| `idx_utxos_reserved` omits `spent_by IS NULL`, never shrinks | FIXED | `Cover the sweep with the holder index, and stop feeding it spent rows` — migration `00003` rewrites it as `(reserved_by, user_id) INCLUDE (reserved_at, seq, pinned) WHERE reserved_by IS NOT NULL AND spent_by IS NULL`. Guarded by `TestSweepUsesReservedIndex`, `TestStaleScanIsIndexDriven`, `TestMigrationsRollBack`. `pinned` is INCLUDEd rather than folded into the predicate because Pin/Unpin look rows up BY the current pin state through this index. SQLite has no INCLUDE, so the covering columns are trailing key columns (plus `spent_by`, which only PostgreSQL discharges against the query's identical term). |
| `FindStaleReservations`' `GROUP BY (user_id, reserved_by)` has no covering index (full scan each tick) | FIXED | Same migration — the inner aggregate becomes an **index-only scan** that also groups in index order (Sorted, not Hashed) and never visits the heap. `TestSweepUsesReservedIndex` asserts `Index Only Scan` on `idx_utxos_reserved` specifically, because name-presence and no-Seq-Scan both PASSED on the old schema. |
| sqlstore `ReleaseOutpoints`/`Spend`/`Promote` do N sequential round-trips | FIXED (PostgreSQL) | `Send a whole batch in one statement, instead of one per coin` — the target rows join `unnest($1::bytea[], $2::bigint[]) AS k(txid, vout)`, carrying the guard conjuncts verbatim. Two arrays rather than N tuples keeps the statement text (and cached plan) constant across batch sizes and clear of the 65535-parameter ceiling; the casts are load-bearing. The **two-argument** `unnest` is deliberate: it is DEFINED to walk positionally, where `SELECT unnest($1), unnest($2)` depends on planner behavior that has changed across major versions and would cross a txid with the wrong vout. SQLite deliberately keeps its loops (pinned local writer, no `unnest`, no derived-table column-alias list) — pinned as a decision by `guarded_stmt_test.go`. Tests: `TestSetBasedMutationsAreOneStatement`, `TestSetBasedStatementsRejectUntypedArrays`, `TestArrayBindingWideBatch`, `TestBatchSemantics_Postgres`/`_SQLite` (`MixedTxIDBatch` is the case that can see a mis-pairing; a genuine cross product was verified to fail it and only it). |
| `buildResultInputs` and `markInputsSpent` are per-input N+1 lookups | FIXED | Same commit — `metastore.OutputsRepo.FindOutputsByOutpoints` replaces both loops with one statement per 400 outpoints on both engines, spelled `(t.txid, o.vout) IN (VALUES …)` because SQLite rejects the alias list a derived table would need. Tests: `TestFindOutputsByOutpoints_Postgres`/`_SQLite`. |
| aerostore `ReleaseReservation`/`ReleaseOutpoints`/`Unspend` do N round-trips where `BatchOperate` would do | **NOT ADDRESSED** | The batching commit covered sqlstore and metastore only; aerostore still issues one guarded CAS per outpoint. Not a correctness issue — each op is the same single-record CAS that provides the exclusivity guarantee — and the Aerospike hybrid is under a standing recommendation to be retired (`docs/aerospike-value-review.md`). Left as a known efficiency gap. |
| `pullLocked` is O(n²) under single-item drain while holding `dataMu` | **RECLASSIFIED** | `Say the store contract once…` (verify-only pass). It is a selection scan per pulled item, so the cost is **O(n·want)**, not O(n²). `ClaimSmallestSufficient` passes `want=1`: one pass. `ClaimExact` passes `prefer=nil`, so the scan breaks at the first match and the swap-with-last keeps a dense bucket at index 0 — but that is the BEST case, not the shape: a bucket holding several denominations (`bucket = floor(log2(sats))`) makes matches sparse, so the honest bound is O(n·count) there too. **The deliberately quadratic one is `ClaimLargestInsufficient`**: `prefer != nil` scans the whole snapshot per item, ~512·limit comparisons under `dataMu`. Bounded by a small limit today; carried to follow-ups. |
| `providedInputSizes` has a dead error return | FIXED | `Say the store contract once…` |
| `bytesEqual` duplicates `bytes.Equal` | FIXED | `Say the store contract once…` |
| `SpendTiers` has two identical switch arms | FIXED | `Say the store contract once…` |
| `feeCalc.bytes` is a write-once field | FIXED | `Say the store contract once…` — it becomes the constant it always was. |
| `Store.pool` is a write-once field | **NOT ADDRESSED** | Cosmetic and arguably correct as written: `sqlstore.Store.pool` is an option carrier, set by the `WithPool` option and consumed once by `ApplyTo` during construction. Collapsing it would remove the option seam. |
| The three SQLite claim statements inline near-identical SQL (unlike the PG constants) | **ALREADY-FIXED-PRE-AUDIT** (verified) + hardened | `Say the store contract once…` verified they are constant-expression concatenations of `claimCandidateSQLite` and the `sats*` constants, folded at compile time, so no per-claim string building reaches the hot path. `Tell a starved funding pass from an empty pool` went further: the SQLite claim statements are now BUILT from the shared predicate constant, making probe/claim drift impossible by construction on that engine. |
| `Spend`/`ErrContention` doc drift | ALREADY-FIXED / FIXED | Interface↔sqlstore alignment was already done pre-remediation; `Say the store contract once…` fixed the remaining stale one, aerostore's own `Spend` doc, which named no contention exit at all despite being the backend `errors.go` singles out for lacking parity. |

---

## Summary counts

**Numbered findings** (P0-1…P0-5, P1-1…P1-7, P2-1…P2-10, plus the altitude
deep-fix) — 23 items:

| Status | Count | Items |
|---|---:|---|
| FIXED | 18 | P0-1, P0-2, P0-3, P0-4, P0-5, P1-1, P1-2, P1-3, P1-4, P1-6, P1-7, P2-3, P2-5, P2-6, P2-8, P2-9, P2-10, deep-fix |
| FIXED-WITH-AMENDMENT | 4 | P1-5 (hardening, not a live bug on the pinned driver), P2-1 (direction reversed), P2-4 (probe relocated), P2-7 (pinned by assertion, nonce not built) |
| ALREADY-FIXED-PRE-AUDIT | 1 | P2-2 (PR #19, i.e. the audit baseline itself) |
| **Total** | **23** | |

One RECLASSIFICATION sits *inside* a FIXED finding rather than replacing it:
P1-1's memstore half is not a defect (single critical section), so only the
aerostore half was changed.

**Cleanup nits** (the review's non-blocking bullet list, expanded to 15 items):

| Status | Count | Items |
|---|---:|---|
| FIXED | 10 | backend-rule triplication; `idx_utxos_reserved` predicate; sweep covering index; sqlstore N round-trips (PostgreSQL); `buildResultInputs`/`markInputsSpent` N+1; `providedInputSizes`; `bytesEqual`; `SpendTiers`; `feeCalc.bytes`; aerostore `Spend`/`ErrContention` doc |
| PARTIALLY FIXED | 1 | the `changeOutpoints` trio — two collapse, the user-scoped one deliberately does not |
| ALREADY-FIXED-PRE-AUDIT | 1 | SQLite claim SQL already compile-time constants (verified, then hardened further) |
| RECLASSIFIED | 1 | `pullLocked` is O(n·want), not O(n²) |
| NOT ADDRESSED | 2 | aerostore batch round-trips; `Store.pool` |
| **Total** | **15** | |

**All 5 P0, all 7 P1, all 10 P2 and the altitude deep-fix are resolved.** The two
NOT-ADDRESSED items are both from the non-blocking cleanup list, and neither
touches the audited invariant.

---

## Follow-ups (accumulated review debt)

Carried forward from the per-task reviews. None blocks the audited invariant;
they are recorded here so the next reader does not have to rediscover them.

- **Input BEEF is not assembled for caller-named coins.** `getBEEFForTxIDs`
  walks the funder's allocated txids only (`create.go` passes
  `allocatedTxids(fundRes.AllocatedUTXOs)`), so a caller-named input's parent
  transaction is not merged into the stored input BEEF. The send-time walk heals
  this from storage — but that combination is untested and worth a test.
- **`ClaimForSend`'s non-HOT `known_txs` UPDATE** costs roughly +3 index entries
  per broadcast. Measure in the next blast; consider folding the claim into
  `FindResendable` via `SELECT FOR UPDATE SKIP LOCKED`.
- **Sweep head-of-line blocking** under a longer-than-TTL arcade outage with more
  than `limit` queued delayed sends: the pinned-inclusive listing can fill a page
  with live pinned rows the sweep will always skip. Self-healing and
  availability-only; observable via the `skipped_in_flight` tick counter.
- **`ABORT_REMOVE_CHANGE` parked rows are a permanent gauge floor.** The
  fence-first sweep's healer arm retires `ABORT_RELEASE` intents but not this
  one, so `outbox_parked_total` never returns to zero once such a row parks.
  Read the gauge per op type.
- **Backfill parking / attempt ceiling**, per the outbox precedent. The
  mined-repair backfill is currently an unbounded WARN plus a `GetTx` per tick
  for permanently-unverifiable proofs.
- **`ErrFeeBelowFloor` has no wire row**, so a deterministic client-caused
  refusal reaches a remote caller as a 500 `ERR_INTERNAL`. (Confirmed: no
  mapping in `rest_wire.go`.)
- **The sync wire carries no tier.** A `TierSending` or `TierUnproven` coin at
  the source is rebuilt as `TierMined` on the target.
- **`suspectFailed` + sync-4xx MINED divergence** is healed by the backfill
  rather than by a live event. Decision pending on whether that is the right
  boundary.
- **`ClaimLargestInsufficient`'s O(n·limit) `pullLocked` scan** in memstore —
  `prefer != nil` scans the whole snapshot per item under `dataMu`. Bounded by a
  small limit today.
- **aerostore repair-path `ErrContention` breadth.** The hybrid test added by the
  MINED-repair work covers the fact-mode-spend-against-released-reservation case;
  deeper coverage is optional.
- **The multi-instance `SendWaiting` debug counter counts claim-skips as
  broadcasts.**
- **Tie-order LIFO means fuel-pool coins never rotate.** The accepted consequence
  of P2-1's reversed direction: on a pool of identical denominations
  `ClaimLargestInsufficient` drains newest-first, so the oldest coins sit at the
  bottom until the pool drains below them. Age-based sweeps must not assume
  rotation.

---

## Appendix — verification evidence

Full sweep run on the closeout branch at `14a27a4` + this document. Host: Fedora
(kernel 7.1.5), rootless podman via `DOCKER_HOST=unix:///run/user/1000/podman/podman.sock`.

| # | Command | Result |
|---|---|---|
| 1a | `go build ./...` | **PASS** — clean |
| 1b | `go vet ./...` | **PASS** — clean |
| 1c | `go build -tags integration ./...` | **PASS** — clean |
| 1d | `go vet -tags integration ./...` | **PASS** — clean |
| 2 | `go test ./... -count=1` | **PASS** — 40 packages ok, 0 fail |
| 3 | `go test -race ./pkg/utxostore/... ./pkg/storage/... -count=1` | **PASS** — 7 packages ok, 0 fail, 0 race reports |
| 4a | `make check-podman` | `podman.socket is active` |
| 4b | `go test -tags integration ./... -count=1` | **PASS** — 41 packages ok, 0 fail. Slowest legs: `metastore` 297s, `sqlstore` 235s, `storage` 176s, `aerostore` 99s, `funder` 35s |
| 4c | the four EXPLAIN guards, re-run with `-v` to confirm they executed rather than skipped | **PASS** (all four, with plans captured) — `TestClaimUsesPartialIndex` all 3 shapes `Index Scan` on `idx_utxos_claim` over a 250k pool (Forward / **Backward** for largest-insufficient / Forward); `TestClaimableProbeUsesPartialIndex` `Index Only Scan` on `idx_utxos_claim`; `TestSweepUsesReservedIndex` **`Index Only Scan` on `idx_utxos_reserved` with `Strategy: "Sorted"`** (not Hashed) for both the plain and pinned-inclusive listings, and the release plan filtering `(spent_by IS NULL) AND (NOT pinned)` inside the index; `TestStaleScanIsIndexDriven` SQLite `SEARCH utxos USING COVERING INDEX idx_utxos_reserved` for both listings |
| 4d | the two audit ACCEPTANCE tests re-run with `-v` across every provider leg: `-run 'TestProviderConformance_.*/(AbortFence\|ProvidedInputExclusivity)'` | **PASS**, and it surfaced one coverage caveat worth recording. `ProvidedInputExclusivity` (P0-1) runs on **all four** legs: PostgreSQL Mode A, Aerospike/PG hybrid Mode B, memstore+SQLite, REST. `AbortFence` (P0-3) runs on **three** — it **SKIPS the REST leg**, which supplies no `RejectReleaseEnv`, because across the HTTP hop the oracle and movable clock are unreachable and the subtest declines to pass without being able to count broadcasts. That skip is deliberate and self-documenting, not a silent gap. |
| 5a | `golangci-lint cache clean` (v2.12.2) | ok |
| 5b | `golangci-lint run ./...` | **PASS** — 0 issues |
| 5c | `golangci-lint run --build-tags integration ./...` | **PASS** — 0 issues |
| 6a | `BenchmarkClaim_Postgres` — `BENCH_PG_DSN` pointed at a throwaway `postgres:17-alpine` container, `synchronous_commit=off` to match the recorded baseline | **RAN.** pool=1k **101 µs/op**, pool=250k **105 µs/op** — flat in pool size, which is what the benchmark exists to prove. **3143 B/op, 70 allocs/op**, identical to the profile recorded when the set-based batching landed. (Host i9-13900K; the baseline's 143–163 µs was a different host. A control run with the default `synchronous_commit=on` costs 11.3 ms/op — fsync-bound and equally flat — confirming the setting, not the code, is what moves this number.) |
| 6b | `BenchmarkClaim_SQLite` | **RAN.** pool=1k 50.5 µs/op, pool=250k 82.4 µs/op; 2471 B/op, 69 allocs/op |
| 7a | `go test ./pkg/storage/ -run 'TestE2E_' -count=1` (`wallet_e2e_test.go` + `monitor_e2e_test.go` — both untagged, so also covered by run 2) | **PASS** — 9/9: `TestE2E_WritePath_Broadcast`, `…_TwoStepSignAction`, `…_NoSend`, `…_Delayed`, `…_ImmediateBroadcastOverridesDelayed`, `TestE2E_StateReport`, `TestE2E_FanOutFuel_BootstrapsPool`, `TestE2E_InternalizeTrustAnchor_Negative`, `TestE2E_Monitor_SSE_MINED_PromotesOwnSentTx` |
| 7b | `go test -tags integration ./test/... -count=1` | **No packages.** `test/perf` is tagged `perf`, not `integration`, so this pattern matches nothing — expected, not a failure. |
| 7c | `go vet -tags perf ./test/...` + compile check | **PASS** — clean |
| 7d | `go test -tags perf -run TestPerf -timeout 45m ./test/perf/...` — the runnable, bounded, container-backed legs | **PASS — 5/5** in 1682s: `TestPerf_PostgresOptimisticCeiling` (411s), `TestPerf_PostgresGroupCommit` (1142s), `TestPerf_PostgresModeA` (50s), `TestPerf_AerospikeHybridModeB` (53s), `TestPerf_SQLiteBaseline` (26s). Every sanity floor held and **`contention retries=0` on all three backend legs** — the remediation added no claim contention. Numbers below. |

**What `test/perf` is and is not.** All five legs spin their own backend through
`internal/testenv` (podman, graceful skip when no runtime is available), run a
BOUNDED run (default 20s + 5s warmup, env-overridable) and assert a conservative
sanity floor. They are *not* the external scale-network blast, which is driven by
`cmd/perfrunner` against a live cluster and was deliberately not attempted here.

Numbers captured on this run (i9-13900K, rootless podman) — recorded as evidence
that the remediation did not regress the write path, not as headline figures:

| Leg | Result |
|---|---|
| `PostgresModeA` | 157.2 TPS over 20s, e2e p50 96.7ms / p99 165.8ms, **contention retries=0** |
| `AerospikeHybridModeB` | 156.6 TPS over 20s, e2e p50 96.9ms / p99 198.7ms, **contention retries=0** |
| `SQLiteBaseline` | 134.5 TPS over 20s, e2e p50 56.3ms / p99 147.7ms, **contention retries=0** |
| `PostgresOptimisticCeiling` | 293.7 / 460.9 / 718.9 / 977.4 TPS at 32 / 64 / 128 / 256 workers, **0 retries at every level**, durability verified `synchronous_commit=on fsync=on` |
| `PostgresGroupCommit` | 1005–1140 TPS across all 9 WAL configurations (baseline 1059.8; best `max_wal_size=32GB` at 1139.9), durability verified throughout |

<!--VERIFICATION-RESULTS-->
</content>
</invoke>
