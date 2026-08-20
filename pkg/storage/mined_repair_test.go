package storage_test

// The MINED REPAIR path (audit P1-6, plus the aborted door C5's probe found).
//
// Every test here is a variation on one situation: the wallet decided a
// transaction was dead, ACTED on that decision by giving its funding coins
// back, and the network then mined the transaction anyway. Whatever the wallet
// believes, the coins are consumed on chain — so leaving them claimable is not
// a stale record, it is the wallet arming its own double spend. The repair is
// the only thing that disarms it.
//
// Two doors led into that state and both had to be closed:
//
//   - the TERMINAL-WALLET-STATE guard, which made every later status a no-op
//     for an invalidTx/doubleSpend row, so a false-positive rejection the
//     reconciler had already released could never be repaired (P1-6); and
//   - SetProof on an ABORTED row, which is not a terminal failure and so was
//     never guarded at all: the MINED silently completed the known tx with no
//     fact-spend and no change re-mint.

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

const repairHeight = uint32(902_500)

// --- end-to-end: the P1-6 acceptance ---------------------------------------

// TestMinedAfterVerifiedRelease_ReclaimsTheReleasedInputs is audit P1-6 in
// executable form, driven entirely through the public seam.
//
// The reconciler is DESIGNED to release a provably-dead rejection's inputs, and
// it is right to: a rejection that survives two verified passes a grace apart is
// as dead as the oracle can say. But arcade is not the network. A peer that
// never saw the rejection can mine the transaction afterwards, and the moment it
// does, the coins the reconciler handed back are gone on chain while the wallet
// still offers them to the next CreateAction.
//
// The terminal-wallet-state guard is what made that unrecoverable: it turned
// every later status for an invalidTx row into a no-op, so the MINED that
// carries the proof — the one event that could tell the wallet it was wrong —
// was the one event it refused to read.
func TestMinedAfterVerifiedRelease_ReclaimsTheReleasedInputs(t *testing.T) {
	ctx := context.Background()
	clock := newRespendClock()
	oracle := &conformance.FakeOracle{}
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, oracle,
		&conformance.FakeHeaders{}, clock.fn(),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))

	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)

	res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, res.Inputs)
	txid := respendProcess(t, p, auth, res)

	// Arcade rejects, and keeps rejecting across both verification passes.
	oracle.ScriptTx(txid, arcade.StatusRejected,
		conformance.WithExtraInfo("PROCESSING (4): failed to validate transaction"))
	require.NoError(t, p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))
	for range 2 {
		clock.Advance(respendGrace + time.Second)
		_, verr := p.VerifyAndReleaseSuspects(ctx, respendGrace, respendMaxQ, 128)
		require.NoError(t, verr)
	}
	require.True(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the reconciler never released the rejected transaction's input, so this "+
			"test cannot prove the repair takes it back")

	// A peer mined it after all. This is the event the terminal guard swallowed.
	rec, _ := minedRecord(t, txid, repairHeight)
	require.NoError(t, p.ApplyStatusUpdate(ctx, rec))

	assert.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the wallet is still offering a coin a mined transaction has consumed. "+
			"The next CreateAction that takes it authors a double spend against a "+
			"transaction already on chain — which is exactly what P1-6 describes")

	assert.Equal(t, wdk.TxStatusCompleted, repairTxStatus(t, p, auth, txid),
		"the transaction is on chain with a header-verified proof; leaving its row "+
			"failed misreports a settled payment as a lost one")

	// And the change the release had removed is back and spendable: the mined
	// transaction really did create it.
	change := repairChangeOutputs(t, p, auth, txid)
	require.NotEmpty(t, change, "the mined transaction's change was never re-minted")
	for _, row := range change {
		assert.True(t, row.Spendable,
			"change of a mined transaction is unspendable, so the wallet has silently "+
				"written off funds it owns")
	}
}

// --- end-to-end: the aborted door ------------------------------------------

// TestMinedAfterAbort_RepairsRatherThanSilentlyCompleting closes the second
// door. 'aborted' is not in [wdk.ProvenTxReqStatus.IsTerminalFailure], so the
// terminal guard never saw these rows at all: a MINED walked straight into the
// ordinary apply, whose SetProof flipped the known tx to completed and wrote a
// proof — with NO fact-spend of the inputs the abort had just handed back, and
// no re-mint of the change the abort had removed.
//
// That is strictly worse than the P1-6 case, because it looks like success. The
// known tx reads 'completed' with a valid proof while the wallet's coins say the
// transaction never happened.
func TestMinedAfterAbort_RepairsRatherThanSilentlyCompleting(t *testing.T) {
	ctx := context.Background()
	oracle := &conformance.FakeOracle{}
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, oracle,
		&conformance.FakeHeaders{}, nil,
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))

	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)

	txid, _ := repairAbortedAction(t, p, auth)

	require.True(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the abort never gave the funding coin back, so this test cannot prove the "+
			"repair takes it away again")
	assert.Equal(t, wdk.TxStatusAborted, repairTxStatus(t, p, auth, txid))

	// The client held the signed bytes all along and broadcast them out of band;
	// the network mined them. Nothing local makes that un-happen.
	rec, _ := minedRecord(t, txid, repairHeight)
	require.NoError(t, p.ApplyStatusUpdate(ctx, rec))

	assert.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"an aborted transaction was mined and the wallet still offers its input. "+
			"The abort's release is the whole hazard: those coins are consumed on "+
			"chain, and the next action that claims one double spends")

	assert.Equal(t, wdk.TxStatusCompleted, repairTxStatus(t, p, auth, txid),
		"the known tx was completed by SetProof while its transaction row stayed "+
			"aborted — the silent completion this repair exists to replace")

	// The descriptive ledger must agree with the coins. The abort cleared this
	// input's spend history; a completed transaction that lists its own inputs as
	// unspent is a contradiction anyone reading the wallet would have to resolve
	// by hand.
	assert.NotNil(t, repairOutput(t, p, auth, fundTxID, 0).SpentBy,
		"the repaired transaction's input still shows no spend history")

	change := repairChangeOutputs(t, p, auth, txid)
	require.NotEmpty(t, change)
	for _, row := range change {
		assert.True(t, row.Spendable,
			"the abort removed this change and the mined proof did not bring it back, "+
				"so the wallet has lost track of coins the network says it owns")
	}
}

// TestMinedBatchOnAFencedRow_RepairsToo asserts the SSE batch path routes a
// fenced row's MINED to the repair instead of bulk-SetProof-ing it.
//
// [Provider.ApplyStatusBatch] documents itself as EXACTLY EQUIVALENT to calling
// ApplyStatusUpdate per record, and the batch is the path production actually
// takes — the per-event entry point only runs for the poll fallbacks. A repair
// that existed only on the per-event path would therefore not exist at all
// under load, and worse: the batch's own SetProof loop would burn the fence
// (completing the known tx) before any poll could get to it.
func TestMinedBatchOnAFencedRow_RepairsToo(t *testing.T) {
	ctx := context.Background()
	oracle := &conformance.FakeOracle{}
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, oracle,
		&conformance.FakeHeaders{}, nil,
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))

	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)
	txid, _ := repairAbortedAction(t, p, auth)
	require.True(t, respendSpendable(t, p, auth, fundTxID, 0))

	rec, _ := minedRecord(t, txid, repairHeight)
	require.NoError(t, p.ApplyStatusBatch(ctx, []arcade.TxRecord{rec}))

	assert.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the batch applied a fenced row's proof without repairing its coins, so the "+
			"per-event repair is unreachable on the path production uses")
	assert.Equal(t, wdk.TxStatusCompleted, repairTxStatus(t, p, auth, txid))
	change := repairChangeOutputs(t, p, auth, txid)
	require.NotEmpty(t, change)
	assert.True(t, change[0].Spendable, "the batch repair did not re-mint the change")
}

// --- mechanism: the unrecoverable case, the backfill, the interleaving ------

// TestMinedRepair_CompetingSpendIsAlertedNotFatal covers the one outcome the
// repair cannot fix. A released input can be re-claimed AND re-spent by a second
// transaction before the first one's proof arrives; when it is, two spend facts
// exist for one coin and only the chain can settle which is real.
//
// The repair must NOT overwrite the recorded winner (that would erase the
// evidence) and must NOT abort (the rest of the repair — the other inputs, the
// proof, the change — is still correct and still needed). It alerts and carries
// on, which is the honest shape: the conflict is now visible, the coin is not
// double-recorded, and the ledger is as close to the chain as local state can be.
func TestMinedRepair_CompetingSpendIsAlertedNotFatal(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xC1)
	competitor := newTxID(0xC2)
	in := opFor(0x71, 0)

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	// The released input, re-claimed and re-spent by a second transaction.
	h.mintAndSpendFunding(in, competitor)
	// …and this transaction already written off and its change removed, which is
	// where the reconciler's release leaves it.
	h.forceDeadTx(txid, wdk.ProvenTxStatusInvalid, wdk.TxStatusFailed)
	h.removeChange(txid, changeOp)

	rec := h.minedRec(txid)
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec), "a materialized double spend must not fail the repair")

	assert.Contains(t, h.logs.String(), "double spend materialized",
		"the one outcome nothing automatic can repair was applied silently")
	h.requireSpentBy(in, competitor,
		"the repair overwrote the recorded winner, erasing the evidence of the conflict")

	// Everything the repair CAN do, it still did.
	require.Equal(t, wdk.ProvenTxStatusCompleted, h.knownTx(txid).Status)
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	h.requireClaimable(changeOp, "the mined transaction's change was not re-minted")
}

// TestCheckProofs_BackfillsAPreRepairDivergence covers the rows that already
// exist. Before this change a MINED on an aborted row completed the known tx
// and wrote its proof while leaving the transaction row aborted and the funding
// coins claimable — so a deployment carries that divergence in its database, and
// no live event will ever come back for it: the poll work lists are all keyed on
// non-terminal statuses, and these rows are 'completed'.
//
// The signature is what makes them findable: a transaction the wallet wrote off
// (aborted/failed) whose known tx carries arcade's MINED verdict.
func TestCheckProofs_BackfillsAPreRepairDivergence(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xD1)
	in := opFor(0x81, 0)

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	h.mintCoin(in, "fund-"+in.String(), 10_000, utxostore.TierMined)

	// Reproduce the pre-repair divergence exactly: proof stored, known tx
	// completed, transaction row aborted, change gone, input claimable.
	h.storeProof(txid)
	h.setArcadeStatus(txid, arcade.StatusMined)
	h.forceTxStatus(txid, wdk.TxStatusAborted)
	h.removeChange(txid, changeOp)
	h.requireClaimable(in, "seeded state: the aborted action's input is back in the pool")

	require.NoError(t, h.p.CheckProofs(ctx, 100))

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid),
		"the backfill did not heal a transaction the database already recorded as "+
			"both mined and aborted")
	h.requireRemoved(in, "the mined transaction's input is still in the claimable pool")
	h.requireClaimable(changeOp, "the mined transaction's change was not re-minted")

	// Second tick: nothing left to do. The healer has to quiesce, or it re-drives
	// the same rows forever.
	before := h.knownTx(txid).UpdatedAt
	require.NoError(t, h.p.CheckProofs(ctx, 100))
	require.Equal(t, before, h.knownTx(txid).UpdatedAt, "the backfill re-repaired a healed row")
}

// TestBackfill_AnUnrepairableRowDoesNotBlockThePage is the anti-starvation
// property, and it is the one that decides whether the healer works at all in
// the field.
//
// Several backfill outcomes write NOTHING to the known tx — arcade has
// forgotten the transaction, or revised its verdict away from mined — and the
// work list is a LIMIT page over an ordered scan. A row that is selected and
// then left untouched keeps its place at the head of every subsequent page, so
// one such row at the front of a limit-1 page hides the entire backlog behind
// it forever. The poll fallbacks learned this the hard way; the fix is the same
// one, an attempt stamp recorded before the work.
func TestBackfill_AnUnrepairableRowDoesNotBlockThePage(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)

	// The blocker: arcade reported it mined and has since forgotten it, so no
	// tick can ever repair it. Seeded FIRST so it sorts to the head.
	blocker := newTxID(0xAA)
	h.seedKnownTx(blocker, []utxostore.Outpoint{opFor(0xAB, 0)}, 0)
	h.setArcadeStatus(blocker, arcade.StatusMined)
	h.forceDeadTx(blocker, wdk.ProvenTxStatusInvalid, wdk.TxStatusFailed)

	// The repairable row behind it.
	h.clock.Advance(time.Minute)
	txid := newTxID(0xAC)
	in := opFor(0xAD, 0)
	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	h.mintCoin(in, "fund-"+in.String(), 10_000, utxostore.TierMined)
	h.storeProof(txid)
	h.setArcadeStatus(txid, arcade.StatusMined)
	h.forceTxStatus(txid, wdk.TxStatusAborted)
	h.removeChange(txid, changeOp)

	// One row per tick, so the blocker occupies the whole first page.
	require.NoError(t, h.p.CheckProofs(ctx, 1))
	require.NoError(t, h.p.CheckProofs(ctx, 1))

	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid),
		"the second row was never reached: an unrepairable row is pinning the head "+
			"of the work list and the rest of the backlog is invisible behind it")
	h.requireRemoved(in, "the mined transaction's input is still claimable")
	h.requireClaimable(changeOp, "the mined transaction's change was not re-minted")

	// And the pass must not have claimed credit for the blocker.
	require.Contains(t, h.logs.String(), "disagreed=1",
		"a row nothing could repair was counted as repaired, so the pass line "+
			"reports a drain that did not happen")
}

// TestBackfill_ARevisedVerdictLeavesTheWorkList: arcade may withdraw a MINED it
// once reported. Recording the revision is what takes the row off the healer's
// list — and it has to be recorded, or the row is re-fetched from arcade on
// every tick for as long as the wallet runs.
func TestBackfill_ARevisedVerdictLeavesTheWorkList(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xBA)

	h.seedKnownTx(txid, []utxostore.Outpoint{opFor(0xBB, 0)}, 0)
	h.setArcadeStatus(txid, arcade.StatusMined)
	h.forceDeadTx(txid, wdk.ProvenTxStatusInvalid, wdk.TxStatusFailed)
	h.scriptTx(txid, arcade.StatusRejected)

	require.NoError(t, h.p.CheckProofs(ctx, 100))

	kt := h.knownTx(txid)
	require.NotNil(t, kt.ArcadeStatus)
	require.Equal(t, string(arcade.StatusRejected), *kt.ArcadeStatus,
		"the withdrawn verdict was not recorded, so the row stays on the backfill's "+
			"work list and costs a GetTx every tick forever")
	require.Equal(t, wdk.ProvenTxStatusInvalid, kt.Status, "nothing may be repaired without a mined verdict")
	require.Equal(t, wdk.TxStatusFailed, h.txStatus(txid))
}

// TestMinedRepair_RefusesAnUnverifiableProof: the repair is a bigger write than
// an ordinary apply — it records spends and re-mints coins — so it must clear
// the SAME bar. A BUMP whose root the header source rejects is not evidence of
// anything, and acting on one would let a forged proof consume a wallet's coins.
func TestMinedRepair_RefusesAnUnverifiableProof(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xE1)
	in := opFor(0x91, 0)

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	h.mintCoin(in, "fund-"+in.String(), 10_000, utxostore.TierMined)
	h.forceDeadTx(txid, wdk.ProvenTxStatusInvalid, wdk.TxStatusFailed)
	h.removeChange(txid, changeOp)

	// A BUMP the headers source does not recognize at that height.
	rec, _ := minedRecord(h.t, txid, repairHeight)
	var wrong [32]byte
	for i := range wrong {
		wrong[i] = 0xff
	}
	h.hdrs.register(repairHeight, wrong)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec), "a bad proof is skipped, not an error")

	require.Equal(t, wdk.ProvenTxStatusInvalid, h.knownTx(txid).Status, "known tx must not be completed")
	require.Empty(t, h.knownTx(txid).MerklePath, "no proof stored for a root that failed verification")
	require.Equal(t, wdk.TxStatusFailed, h.txStatus(txid))
	h.requireClaimable(in, "an unverifiable proof must not consume the wallet's coins")
	h.requireRemoved(changeOp, "an unverifiable proof must not re-mint change")

	// But arcade's verdict IS recorded, and that is not incidental: it is the
	// only thing that keeps the divergence findable. See the next test.
	kt := h.knownTx(txid)
	require.NotNil(t, kt.ArcadeStatus)
	require.Equal(t, string(arcade.StatusMined), *kt.ArcadeStatus,
		"a refused proof left no trace, so nothing will ever come back for this row: "+
			"a fenced known tx is on no poll work list")
}

// TestMinedRepair_DeferredProofIsRecoveredByTheBackfill is the convergence half
// of the test above, and the reason the repair records arcade's verdict before
// it verifies anything.
//
// Deferring is the COMMON case, not the exotic one: our header view routinely
// lags arcade by a block, and applyMined's answer to that — leave it unproven,
// the proof poll will retry — is unavailable here, because a fenced row appears
// on no poll work list. Without a durable trace the wallet would simply forget
// that arcade said its written-off transaction was mined, and the released coins
// would stay claimable forever.
func TestMinedRepair_DeferredProofIsRecoveredByTheBackfill(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xE7)
	in := opFor(0x9A, 0)

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	h.mintCoin(in, "fund-"+in.String(), 10_000, utxostore.TierMined)
	h.forceDeadTx(txid, wdk.ProvenTxStatusInvalid, wdk.TxStatusFailed)
	h.removeChange(txid, changeOp)

	// The live event arrives while the header is not yet available: nothing is
	// verified, so nothing is written.
	rec, root := minedRecord(h.t, txid, repairHeight)
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec))
	require.Equal(t, wdk.ProvenTxStatusInvalid, h.knownTx(txid).Status, "nothing may be written before verification")
	h.requireClaimable(in, "nothing may be written before verification")

	// The header lands, and arcade still has the record with its BUMP.
	h.hdrs.register(repairHeight, root)
	h.scriptTx(txid, arcade.StatusMined, withMinedProof(rec))

	require.NoError(t, h.p.CheckProofs(ctx, 100))

	require.Equal(t, wdk.ProvenTxStatusCompleted, h.knownTx(txid).Status,
		"the deferred proof was never retried, so the divergence is permanent")
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	h.requireRemoved(in, "the mined transaction's input is still claimable")
	h.requireClaimable(changeOp, "the mined transaction's change was not re-minted")
}

// withMinedProof copies a built record's BUMP onto a scripted GetTx answer.
func withMinedProof(from arcade.TxRecord) func(*arcade.TxRecord) {
	return func(r *arcade.TxRecord) {
		r.MerklePath = from.MerklePath
		r.BlockHeight = from.BlockHeight
	}
}

// TestProofBetweenReconcilerPasses_BlocksTheRelease is the interleaving that
// the repair does NOT cover, and must not have to.
//
// The repair only ever fires on rows the reconciler has already finished with,
// so release-then-repair — the designed order — is what
// TestMinedAfterVerifiedRelease_ReclaimsTheReleasedInputs drives end to end.
// The dangerous direction is a proof landing while a reconciler pass is still
// mid-flight on the same suspect, and that is handled by the ORDINARY apply
// rather than by anything added here: a MINED on 'suspectFailed' completes the
// row, and the release gate is a positive CAS OFF 'suspectFailed', so pass 2
// finds nothing to release.
//
// It is asserted because that CAS is the only thing standing between a stale
// reconciler snapshot and the inputs of a transaction now provably on chain —
// and because the repair's own safety argument leans on it.
func TestProofBetweenReconcilerPasses_BlocksTheRelease(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xF1)
	in := opFor(0xA1, 0)

	h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	h.mintAndSpendFunding(in, txid)
	h.markSuspect(txid, arcade.StatusRejected, nil)
	h.scriptTx(txid, arcade.StatusRejected)

	// Pass 1 stamps the two-pass guard; nothing is released yet.
	h.clock.Advance(reconGrace + time.Second)
	require.Equal(t, 0, h.verify().Released)

	// The proof lands between the passes.
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, h.minedRec(txid)))
	require.Equal(t, wdk.ProvenTxStatusCompleted, h.knownTx(txid).Status)

	// Pass 2 must find nothing to release: the CAS off 'suspectFailed' cannot fire.
	h.clock.Advance(reconGrace + time.Second)
	after := h.verify()
	require.Equal(t, 0, after.Released,
		"the reconciler released the inputs of a transaction that had since been "+
			"proven mined; got %+v", after)
	h.requireNotClaimable(in, "a mined transaction's input was handed back to the funder")
}

// TestMinedRepair_IsIdempotent: the SSE pool delivers at-least-once and the
// backfill re-scans, so a repair that ran twice must be indistinguishable from
// one that ran once.
func TestMinedRepair_IsIdempotent(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0xB7)
	in := opFor(0xB8, 0)

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{in}, 5000)
	h.mintCoin(in, "fund-"+in.String(), 10_000, utxostore.TierMined)
	h.forceDeadTx(txid, wdk.ProvenTxStatusInvalid, wdk.TxStatusFailed)
	h.removeChange(txid, changeOp)

	rec := h.minedRec(txid)
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec))
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, rec), "a re-delivered MINED must be a no-op")

	require.Equal(t, wdk.ProvenTxStatusCompleted, h.knownTx(txid).Status)
	require.Equal(t, wdk.TxStatusCompleted, h.txStatus(txid))
	h.requireRemoved(in, "the mined transaction's input must leave the hot inventory")
	h.requireClaimable(changeOp, "change re-minted exactly once and claimable")
}

// --- helpers ----------------------------------------------------------------

// requireNotClaimable asserts the funder can no longer hand op to a new action.
// A mined transaction's input satisfies it either way — recorded spent, or
// pruned from the hot inventory once the proof made the spend permanent — and
// which of the two is an implementation detail of the apply, not the property.
func (h *reconStack) requireNotClaimable(op utxostore.Outpoint, msg string) {
	h.t.Helper()
	u, err := h.utxo.Get(context.Background(), op)
	if err != nil {
		require.ErrorIs(h.t, err, &utxostore.NotFoundError{}, msg)
		return
	}
	require.False(h.t, u.SpentBy == nil && u.ReservedBy == "" && !u.Frozen, "%s", msg)
}

// minedRec builds a MINED record for txid whose single-leaf BUMP the harness's
// headers source accepts.
func (h *reconStack) minedRec(txid string) arcade.TxRecord {
	h.t.Helper()
	rec, root := minedRecord(h.t, txid, repairHeight)
	h.hdrs.register(repairHeight, root)
	return rec
}

// forceDeadTx puts a seeded tx into the state a completed reconciler release
// leaves behind: a terminal known tx and a failed transaction row.
func (h *reconStack) forceDeadTx(txid string, kt wdk.ProvenTxReqStatus, tx wdk.TxStatus) {
	h.t.Helper()
	require.NoError(h.t, h.meta.KnownTx().UpdateStatus(context.Background(), txid, kt))
	h.forceTxStatus(txid, tx)
}

func (h *reconStack) forceTxStatus(txid string, status wdk.TxStatus) {
	h.t.Helper()
	require.NoError(h.t, h.meta.Transactions().UpdateStatusByTxID(context.Background(), txid, status))
}

func (h *reconStack) setArcadeStatus(txid string, status arcade.Status) {
	h.t.Helper()
	require.NoError(h.t, h.meta.KnownTx().SetArcadeStatus(context.Background(), txid, string(status)))
}

// storeProof writes a header-verified proof the way the pre-repair applyMined
// did: SetProof, which also flips the known tx to completed.
func (h *reconStack) storeProof(txid string) {
	h.t.Helper()
	rec, root := minedRecord(h.t, txid, repairHeight)
	h.hdrs.register(repairHeight, root)
	require.NoError(h.t, h.meta.KnownTx().SetProof(context.Background(), txid,
		repairHeight, nil, rec.MerklePath, root[:]))
}

// removeChange deletes a transaction's minted change, which is what both the
// abort and the reconciler's release do to it.
func (h *reconStack) removeChange(txid string, op utxostore.Outpoint) {
	h.t.Helper()
	if op == (utxostore.Outpoint{}) {
		return
	}
	_, err := h.utxo.RemoveByMintTx(context.Background(), mustHash(h.t, txid), []utxostore.Outpoint{op})
	require.NoError(h.t, err)
}

// repairAbortedAction creates, signs, delay-processes and then ABORTS one
// action, returning its txid and reference. The delayed process is what leaves
// the transaction abortable: its bytes are stored and its coins pinned, but
// nothing has been broadcast.
func repairAbortedAction(t *testing.T, p *storage.Provider, auth wdk.AuthID) (txid, reference string) {
	t.Helper()
	ctx := context.Background()

	res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, res.Inputs)

	tx := conformance.BuildSignedTx(t, res)
	txid = tx.TxID().String()
	ref := res.Reference
	id := primitives.TXIDHexString(txid)
	_, err = p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsDelayed: true,
		Reference: &ref,
		TxID:      &id,
		RawTx:     primitives.ExplicitByteArray(tx.Bytes()),
	})
	require.NoError(t, err)

	abr, err := p.AbortAction(ctx, auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
	require.NoError(t, err)
	require.True(t, abr.Aborted)
	return txid, res.Reference
}

func repairTxStatus(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string) wdk.TxStatus {
	t.Helper()
	rows, err := p.ListActions(context.Background(), auth, wdk.ListActionsArgs{Limit: 100})
	require.NoError(t, err)
	for i := range rows.Actions {
		if strings.EqualFold(rows.Actions[i].TxID, txid) {
			return wdk.TxStatus(rows.Actions[i].Status)
		}
	}
	t.Fatalf("no action row for %s", txid)
	return ""
}

// repairOutput returns one output row as the application sees it.
func repairOutput(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string, vout uint32) wdk.TableOutput {
	t.Helper()
	rows, err := p.FindOutputsAuth(context.Background(), auth,
		wdk.FindOutputsArgs{TxID: &txid, Vout: &vout})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	return rows[0]
}

// repairChangeOutputs returns the change rows of txid as the application sees
// them (Spendable computed against the live inventory).
func repairChangeOutputs(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string) []wdk.TableOutput {
	t.Helper()
	change := true
	rows, err := p.FindOutputsAuth(context.Background(), auth,
		wdk.FindOutputsArgs{TxID: &txid, Change: &change})
	require.NoError(t, err)
	return rows
}
