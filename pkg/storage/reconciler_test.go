package storage_test

// Correctness torture tests for the reject→release reconciler (Task 19). They
// run against a metastore (SQLite temp file) + memstore utxostore — which is
// NOT a shared *sql.DB, so the provider is in Mode B and every release here
// exercises the outbox enqueue + inline-execute path (with the drain worker as
// the crash-recovery half). A shared testClock drives both the metastore and the
// provider so grace / quarantine windows are deterministic. The fake oracle's
// GetTx is scriptable per-txid so a suspect can be made to stay rejected, to
// recover, or to have a competitor win.

import (
	"context"
	"encoding/hex"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

const (
	reconGrace = time.Minute
	reconMaxQ  = time.Hour
)

// --- clock -----------------------------------------------------------------

type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// --- scriptable oracle -----------------------------------------------------

// scriptTx registers the record GetTx returns for txid (overwriting any prior).
func (h *reconStack) scriptTx(txid string, status arcade.Status, opts ...func(*arcade.TxRecord)) {
	rec := arcade.TxRecord{TxID: txid, Status: status}
	for _, o := range opts {
		o(&rec)
	}
	h.scriptMu.Lock()
	h.scripts[txid] = rec
	h.scriptMu.Unlock()
}

func withCompeting(txids ...string) func(*arcade.TxRecord) {
	return func(r *arcade.TxRecord) { r.CompetingTxs = txids }
}

func withRawTx(raw []byte) func(*arcade.TxRecord) {
	return func(r *arcade.TxRecord) { r.RawTx = raw }
}

func withExtraInfo(s string) func(*arcade.TxRecord) {
	return func(r *arcade.TxRecord) { r.ExtraInfo = s }
}

// --- harness ---------------------------------------------------------------

type reconStack struct {
	t        *testing.T
	p        *storage.Provider
	meta     *metastore.Store
	utxo     utxostore.Store
	oracle   *fakeOracle
	userID   int
	refSeq   int
	clock    *testClock
	scriptMu sync.Mutex
	scripts  map[string]arcade.TxRecord
}

func newReconStack(t *testing.T) *reconStack {
	t.Helper()
	ctx := context.Background()
	logger := logging.NewTestLogger(t)
	clock := &testClock{t: time.Now()}

	meta, err := metastore.OpenSQLite(ctx, t.TempDir()+"/meta.db", metastore.WithClock(clock.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo := memstore.New()
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	oracle := &fakeOracle{}

	h := &reconStack{t: t, meta: meta, utxo: utxo, oracle: oracle, clock: clock, scripts: map[string]arcade.TxRecord{}}

	// Scriptable GetTx: return the registered record for a txid, else not-found.
	oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		h.scriptMu.Lock()
		rec, ok := h.scripts[id]
		h.scriptMu.Unlock()
		if !ok {
			return nil, arcade.ErrTxNotFound
		}
		r := rec
		return &r, nil
	}

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, newFakeHeaders(),
		storage.WithScriptsVerifier(stubScripts{}),
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("recon-test"),
		storage.WithClock(clock.Now),
	)
	require.NoError(t, err)
	require.False(t, p.ModeA(), "recon harness must be Mode B (memstore utxostore) to exercise the outbox path")
	_, err = p.Migrate(ctx, "recon-test", "recon-identity-key")
	require.NoError(t, err)
	resp, err := p.FindOrInsertUser(ctx, "02"+chainhash.Hash{}.String()[2:])
	require.NoError(t, err)

	h.p = p
	h.userID = resp.User.UserID
	return h
}

func (h *reconStack) verify() defs.ReconcilerReport {
	h.t.Helper()
	rep, err := h.p.VerifyAndReleaseSuspects(context.Background(), reconGrace, reconMaxQ, 100)
	require.NoError(h.t, err)
	return rep
}

func (h *reconStack) verifyMaxQ(maxQ time.Duration) defs.ReconcilerReport {
	h.t.Helper()
	rep, err := h.p.VerifyAndReleaseSuspects(context.Background(), reconGrace, maxQ, 100)
	require.NoError(h.t, err)
	return rep
}

func opFor(seed byte, vout uint32) utxostore.Outpoint {
	var h chainhash.Hash
	for i := range h {
		h[i] = seed
	}
	return utxostore.Outpoint{TxID: h, Vout: vout}
}

// seedKnownTx creates a transaction row + change output/mint (TierUnproven when
// changeSats>0) + a known_tx whose retained raw tx encodes `inputs`. It does NOT
// spend the inputs or mark the tx suspect. Returns the change outpoint (zero
// value when changeSats==0).
func (h *reconStack) seedKnownTx(txid string, inputs []utxostore.Outpoint, changeSats uint64) utxostore.Outpoint {
	h.t.Helper()
	ctx := context.Background()
	require.NoError(h.t, h.meta.Baskets().FindOrCreate(ctx, h.userID, wdk.BasketNameForChange))

	h.refSeq++
	txID, err := h.meta.Transactions().Insert(ctx, metastore.NewTx{
		UserID:     h.userID,
		Status:     wdk.TxStatusUnproven,
		Reference:  fmt.Sprintf("ref-%s-%d", txid[:8], h.refSeq),
		IsOutgoing: true,
		Satoshis:   int64(changeSats),
	})
	require.NoError(h.t, err)
	require.NoError(h.t, h.meta.Transactions().SetTxID(ctx, txID, txid))

	var changeOp utxostore.Outpoint
	if changeSats > 0 {
		basket := wdk.BasketNameForChange
		_, err = h.meta.Outputs().Insert(ctx, metastore.NewOutput{
			UserID: h.userID, TransactionID: txID, Vout: 0, Satoshis: int64(changeSats),
			Basket: &basket, Change: true, Type: "P2PKH", ProvidedBy: "storage", Purpose: "change",
		})
		require.NoError(h.t, err)
		hash := mustHash(h.t, txid)
		changeOp = utxostore.Outpoint{TxID: hash, Vout: 0}
		h.mintCoin(changeOp, wdk.BasketNameForChange, changeSats, utxostore.TierUnproven)
	}

	require.NoError(h.t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID: txid, Status: wdk.ProvenTxStatusUnconfirmed, RawTx: buildRawTxWithInputs(inputs),
	}))
	return changeOp
}

// buildRawTxWithInputs builds a raw tx whose inputs are exactly `inputs` (its own
// txid is irrelevant — the reconciler reads only the input outpoints).
func buildRawTxWithInputs(inputs []utxostore.Outpoint) []byte {
	tx := transaction.NewTransaction()
	for i := range inputs {
		src := inputs[i].TxID
		tx.AddInput(&transaction.TransactionInput{
			SourceTXID: &src, SourceTxOutIndex: inputs[i].Vout, SequenceNumber: transaction.DefaultSequenceNumber,
		})
	}
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: 1, LockingScript: trueScript()})
	return tx.Bytes()
}

func (h *reconStack) mintCoin(op utxostore.Outpoint, basket string, sats uint64, tier utxostore.Tier) {
	h.t.Helper()
	m := &utxostore.Mint{Outpoint: op, UserID: int64(h.userID), Basket: basket, Satoshis: sats, InputSize: utxostore.DefaultP2PKHInputSize, Tier: tier}
	require.NoError(h.t, h.utxo.Mint(context.Background(), []*utxostore.Mint{m}))
	require.NoError(h.t, m.Err)
}

// spendCoin claims `op` (which must be the smallest sufficient coin in its scope)
// and records it spent by txid.
func (h *reconStack) spendCoin(op utxostore.Outpoint, basket string, tier utxostore.Tier, txid string) {
	h.t.Helper()
	ctx := context.Background()
	resv := "seed-" + op.String()
	u, err := h.utxo.ClaimSmallestSufficient(ctx, utxostore.Scope{UserID: int64(h.userID), Basket: basket, Tier: tier}, resv, 1)
	require.NoError(h.t, err)
	require.NotNil(h.t, u, "coin %s must be claimable in %s/%s", op, basket, tier)
	require.Equal(h.t, op, u.Outpoint)
	sp := &utxostore.SpendOp{Outpoint: op, Reservation: resv, SpendingTxID: mustHash(h.t, txid)}
	require.NoError(h.t, h.utxo.Spend(ctx, []*utxostore.SpendOp{sp}))
	require.NoError(h.t, sp.Err)
}

// mintAndSpendFunding mints a fresh funding coin in its own basket and records it
// spent by txid (the async-accepted-then-rejected input state).
func (h *reconStack) mintAndSpendFunding(op utxostore.Outpoint, txid string) {
	h.t.Helper()
	basket := "fund-" + op.String()
	h.mintCoin(op, basket, 10000, utxostore.TierMined)
	h.spendCoin(op, basket, utxostore.TierMined, txid)
}

func (h *reconStack) markSuspect(txid string, status arcade.Status, competing []string) {
	h.t.Helper()
	require.NoError(h.t, h.p.ApplyStatusUpdate(context.Background(), arcade.TxRecord{TxID: txid, Status: status, CompetingTxs: competing}))
	require.Equal(h.t, metastore.KnownTxStatusSuspectFailed, h.knownTx(txid).Status)
}

func (h *reconStack) knownTx(txid string) *metastore.KnownTx {
	h.t.Helper()
	kt, found, err := h.meta.KnownTx().FindByTxID(context.Background(), txid)
	require.NoError(h.t, err)
	require.True(h.t, found, "known tx %s must exist", txid)
	return kt
}

func (h *reconStack) txStatus(txid string) wdk.TxStatus {
	h.t.Helper()
	rows, err := h.meta.Transactions().FindByTxIDAllUsers(context.Background(), txid)
	require.NoError(h.t, err)
	require.NotEmpty(h.t, rows)
	return rows[0].Status
}

func (h *reconStack) get(op utxostore.Outpoint) *utxostore.UTXO {
	h.t.Helper()
	u, err := h.utxo.Get(context.Background(), op)
	require.NoError(h.t, err)
	return u
}

func (h *reconStack) requireClaimable(op utxostore.Outpoint, msg string) {
	h.t.Helper()
	u := h.get(op)
	require.Nil(h.t, u.SpentBy, "%s: coin still spent", msg)
	require.Empty(h.t, u.ReservedBy, "%s: coin still reserved", msg)
	require.False(h.t, u.Frozen, "%s: coin still frozen", msg)
}

func (h *reconStack) requireSpentBy(op utxostore.Outpoint, txid, msg string) {
	h.t.Helper()
	u := h.get(op)
	require.NotNil(h.t, u.SpentBy, "%s: coin not spent", msg)
	require.Equal(h.t, mustHash(h.t, txid), *u.SpentBy, "%s: wrong spender", msg)
}

func (h *reconStack) requireRemoved(op utxostore.Outpoint, msg string) {
	h.t.Helper()
	_, err := h.utxo.Get(context.Background(), op)
	require.Error(h.t, err, msg)
}

// --- 1. async reject → verified release exactly once -----------------------

func TestReconciler_AsyncReject_VerifiedReleaseExactlyOnce(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xA1)
	inA, inB := opFor(0x11, 0), opFor(0x22, 0)

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{inA, inB}, 5000)
	h.mintAndSpendFunding(inA, txid)
	h.mintAndSpendFunding(inB, txid)
	h.markSuspect(txid, arcade.StatusRejected, nil)
	h.scriptTx(txid, arcade.StatusRejected) // stays rejected across every pass

	// Elapse the grace so FindSuspectFailed considers the suspect.
	h.clock.Advance(reconGrace + time.Second)

	// Pass 1: stamps verified_rejected_at, freezes change, releases NOTHING.
	r1 := h.verify()
	require.Equal(t, 0, r1.Released, "pass 1 must not release (two-pass guard)")
	require.Equal(t, 1, r1.Ambiguous)
	require.NotNil(t, h.knownTx(txid).VerifiedRejectedAt, "pass 1 stamped verified_rejected_at")
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(txid).Status)
	h.requireSpentBy(inA, txid, "input A not released on pass 1")
	h.requireSpentBy(inB, txid, "input B not released on pass 1")

	// Pass 2 (after the grace separation): still rejected → provably dead → release.
	h.clock.Advance(reconGrace + time.Second)
	r2 := h.verify()
	require.Equal(t, 1, r2.Released, "pass 2 releases the provably-dead tx")
	h.requireClaimable(inA, "input A released on pass 2")
	h.requireClaimable(inB, "input B released on pass 2")
	require.Equal(t, wdk.TxStatusFailed, h.txStatus(txid))
	require.Equal(t, wdk.ProvenTxStatusInvalid, h.knownTx(txid).Status)
	h.requireRemoved(changeOp, "phantom change removed on release")

	// Pass 3: the tx is terminal (no longer suspect) → not scanned, nothing happens.
	r3 := h.verify()
	require.Equal(t, 0, r3.Scanned, "released tx is no longer a suspect")
	require.Equal(t, 0, r3.Released)

	// No-double-release: another tx reclaims + spends input A. A stray Unspend by
	// the dead txid must NOT stomp the new spend (the spent_by==txid guard).
	otherTxid := newTxID(0xCC)
	h.spendCoin(inA, "fund-"+inA.String(), utxostore.TierMined, otherTxid)
	n, err := h.utxo.Unspend(context.Background(), mustHash(t, txid), []utxostore.Outpoint{inA})
	require.NoError(t, err)
	require.Equal(t, 0, n, "Unspend guard prevents re-release of a reclaimed input")
	h.requireSpentBy(inA, otherTxid, "reclaimed input still owned by the new spender")
}

// --- 2. false positive never releases --------------------------------------

func TestReconciler_FalsePositive_NeverReleases(t *testing.T) {
	h := newReconStack(t)

	// (a) A suspect that recovers to SEEN is routed back to unproven, never released.
	seenTxid := newTxID(0xB1)
	inSeen := opFor(0x31, 0)
	changeSeen := h.seedKnownTx(seenTxid, []utxostore.Outpoint{inSeen}, 4000)
	h.mintAndSpendFunding(inSeen, seenTxid)
	h.markSuspect(seenTxid, arcade.StatusRejected, nil)
	h.scriptTx(seenTxid, arcade.StatusSeenOnNetwork) // recovered!

	// (b) A suspect stuck at PENDING_RETRY stays suspect, never released.
	retryTxid := newTxID(0xB2)
	inRetry := opFor(0x32, 0)
	h.seedKnownTx(retryTxid, []utxostore.Outpoint{inRetry}, 0)
	h.mintAndSpendFunding(inRetry, retryTxid)
	h.markSuspect(retryTxid, arcade.StatusRejected, nil)
	h.scriptTx(retryTxid, arcade.StatusPendingRetry)

	h.clock.Advance(reconGrace + time.Second)
	rep := h.verify()

	require.Equal(t, 0, rep.Released, "no false positive is ever released")
	require.Equal(t, 1, rep.FalsePositive, "the SEEN recovery is counted a false positive")

	// (a) SEEN: routed back into the lifecycle (unproven / unconfirmed), input never released.
	require.Equal(t, wdk.TxStatusUnproven, h.txStatus(seenTxid))
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(seenTxid).Status)
	h.requireSpentBy(inSeen, seenTxid, "recovered tx input must NOT be released")
	require.Equal(t, utxostore.TierUnproven, h.get(changeSeen).Tier, "recovered change spendable at unproven")
	require.False(t, h.get(changeSeen).Frozen, "recovered change unfrozen")

	// (b) PENDING_RETRY: still suspect, input untouched.
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(retryTxid).Status)
	h.requireSpentBy(inRetry, retryTxid, "ambiguous tx input must NOT be released")
}

// --- 2b. REJECTED + UTXO_SPENT ExtraInfo → partial release (winner-union) --

// TestReconciler_Rejected_UTXOSpent_PartialRelease pins the production case
// where Arcade reports pure REJECTED (not DOUBLE_SPEND_ATTEMPTED) but ExtraInfo
// asserts a concrete outpoint is already spent by another tx. Blind two-pass
// release would free that outpoint and re-fund reject churn; we must hold the
// spent input and only free residual inputs (same invariant as winner-union).
func TestReconciler_Rejected_UTXOSpent_PartialRelease(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xC1)
	inSpent := opFor(0x51, 2) // Arcade says this one is already spent on chain
	inFree := opFor(0x52, 0)  // not mentioned → safe residual

	// Build ExtraInfo in the live Teranode shape (outpoint + spender txid).
	extra := "UTXO_SPENT (70): " + inSpent.TxID.String() + ":2 utxo already spent by tx " +
		newTxID(0xEE) + "[0]\n"

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{inSpent, inFree}, 2500)
	h.mintAndSpendFunding(inSpent, txid)
	h.mintAndSpendFunding(inFree, txid)
	h.markSuspect(txid, arcade.StatusRejected, nil)
	h.scriptTx(txid, arcade.StatusRejected, withExtraInfo(extra))

	h.clock.Advance(reconGrace + time.Second)
	// Pass 1: stamp only.
	r1 := h.verify()
	require.Equal(t, 0, r1.Released)
	require.Equal(t, 1, r1.Ambiguous)
	h.requireSpentBy(inSpent, txid, "no release on pass 1")
	h.requireSpentBy(inFree, txid, "no release on pass 1")

	// Pass 2: partial release.
	h.clock.Advance(reconGrace + time.Second)
	r2 := h.verify()
	require.Equal(t, 1, r2.Released)
	h.requireClaimable(inFree, "residual input not named by UTXO_SPENT is released")
	h.requireSpentBy(inSpent, txid, "UTXO_SPENT input must NOT re-enter the claimable pool")
	require.Equal(t, wdk.ProvenTxStatusDoubleSpend, h.knownTx(txid).Status,
		"spend-conflict REJECTED terminalizes as doubleSpend, not invalid")
	h.requireRemoved(changeOp, "phantom change still removed")
}

// TestReconciler_Rejected_UTXOSpent_AllInputsHeld proves that when ExtraInfo
// covers every funding input, we still finalize the suspect (remove change)
// but release zero coins back to the pool.
func TestReconciler_Rejected_UTXOSpent_AllInputsHeld(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xC2)
	inA := opFor(0x53, 0)
	inB := opFor(0x54, 1)

	extra := "UTXO_SPENT (70): " + inA.TxID.String() + ":0 already spent\n" +
		"UTXO_SPENT (70): " + inB.TxID.String() + ":1 already spent\n"

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{inA, inB}, 1000)
	h.mintAndSpendFunding(inA, txid)
	h.mintAndSpendFunding(inB, txid)
	h.markSuspect(txid, arcade.StatusRejected, nil)
	h.scriptTx(txid, arcade.StatusRejected, withExtraInfo(extra))

	h.clock.Advance(reconGrace + time.Second)
	_ = h.verify() // pass 1
	h.clock.Advance(reconGrace + time.Second)
	r2 := h.verify()
	require.Equal(t, 1, r2.Released, "release still runs to terminalize + remove change")
	h.requireSpentBy(inA, txid, "held spent A")
	h.requireSpentBy(inB, txid, "held spent B")
	h.requireRemoved(changeOp, "change removed even when no residual inputs")
	require.Equal(t, wdk.TxStatusFailed, h.txStatus(txid))
}

// TestReconciler_Rejected_ConflictClass_NoParsedSpent_Defers: ExtraInfo looks
// like a spend conflict but names no outpoints and CompetingTxs are empty /
// not mined — must NOT release-all (the old pure-REJECTED bug).
func TestReconciler_Rejected_ConflictClass_NoParsedSpent_Defers(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xC3)
	inA := opFor(0x55, 0)

	h.seedKnownTx(txid, []utxostore.Outpoint{inA}, 0)
	h.mintAndSpendFunding(inA, txid)
	h.markSuspect(txid, arcade.StatusRejected, nil)
	// Conflict-class keyword but no outpoint / spender we can act on.
	h.scriptTx(txid, arcade.StatusRejected, withExtraInfo("TX_CONFLICTING (36): conflict detected"))

	h.clock.Advance(reconGrace + time.Second)
	_ = h.verify()
	h.clock.Advance(reconGrace + time.Second)
	r2 := h.verify()
	require.Equal(t, 0, r2.Released, "must not free inputs without a proven spent set")
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(txid).Status)
	h.requireSpentBy(inA, txid, "input stays held under conflict ambiguity")
}

// --- 3. double-spend winner rule -------------------------------------------

func TestReconciler_DoubleSpend_WinnerRule(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xD1)
	winner := newTxID(0xE1)
	inShared := opFor(0x41, 0) // consumed by the winner
	inOurs := opFor(0x42, 0)   // exclusively ours

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{inShared, inOurs}, 3000)
	h.mintAndSpendFunding(inShared, txid)
	h.mintAndSpendFunding(inOurs, txid)
	h.markSuspect(txid, arcade.StatusDoubleSpendAttempted, []string{winner})
	h.scriptTx(txid, arcade.StatusDoubleSpendAttempted, withCompeting(winner))

	h.clock.Advance(reconGrace + time.Second)

	// Competitor has not won yet (only SEEN): no release.
	h.scriptTx(winner, arcade.StatusSeenOnNetwork)
	r1 := h.verify()
	require.Equal(t, 0, r1.Released, "no competitor has won yet")
	require.Equal(t, 1, r1.Ambiguous)
	h.requireSpentBy(inShared, txid, "no release while double spend unresolved")
	h.requireSpentBy(inOurs, txid, "no release while double spend unresolved")

	// Competitor MINED, carrying a raw tx that spends inShared: release ONLY inOurs.
	winnerRaw := buildRawTxWithInputs([]utxostore.Outpoint{inShared})
	h.scriptTx(winner, arcade.StatusMined, withRawTx(winnerRaw))
	r2 := h.verify()
	require.Equal(t, 1, r2.Released)
	h.requireClaimable(inOurs, "non-winner input released")
	h.requireSpentBy(inShared, txid, "winner-consumed input kept spent (not returned to the pool)")
	require.Equal(t, wdk.TxStatusFailed, h.txStatus(txid))
	require.Equal(t, wdk.ProvenTxStatusDoubleSpend, h.knownTx(txid).Status)
	h.requireRemoved(changeOp, "double-spent tx change removed")
}

// TestReconciler_DoubleSpend_MultipleWinners_UnionConsumed proves the winner set
// is the UNION of every confirmed competitor's inputs. Two competitors each mine
// a DISJOINT subset of our inputs (they conflict with us but not each other);
// releasing only the first winner's non-consumed set would resurrect the second
// winner's on-chain-spent input. Only the input taken by NEITHER winner is
// released; an input taken by either is left spent-external forever.
func TestReconciler_DoubleSpend_MultipleWinners_UnionConsumed(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xDA)
	c1 := newTxID(0xE1) // mines inX
	c2 := newTxID(0xE2) // mines inY
	inX := opFor(0x43, 0)
	inY := opFor(0x44, 0)
	inZ := opFor(0x45, 0) // taken by neither winner → safe to release

	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{inX, inY, inZ}, 3000)
	h.mintAndSpendFunding(inX, txid)
	h.mintAndSpendFunding(inY, txid)
	h.mintAndSpendFunding(inZ, txid)
	h.markSuspect(txid, arcade.StatusDoubleSpendAttempted, []string{c1, c2})
	h.scriptTx(txid, arcade.StatusDoubleSpendAttempted, withCompeting(c1, c2))

	// BOTH competitors mined, taking disjoint subsets of our inputs.
	h.scriptTx(c1, arcade.StatusMined, withRawTx(buildRawTxWithInputs([]utxostore.Outpoint{inX})))
	h.scriptTx(c2, arcade.StatusImmutable, withRawTx(buildRawTxWithInputs([]utxostore.Outpoint{inY})))

	h.clock.Advance(reconGrace + time.Second)
	r := h.verify()
	require.Equal(t, 1, r.Released)

	// Only inZ (taken by neither winner) is released; inX and inY are provably
	// consumed on-chain by the winners and MUST stay spent-external.
	h.requireClaimable(inZ, "input taken by neither winner is released")
	h.requireSpentBy(inX, txid, "input mined by competitor 1 kept spent, never resurrected")
	h.requireSpentBy(inY, txid, "input mined by competitor 2 kept spent, never resurrected")
	require.Equal(t, wdk.ProvenTxStatusDoubleSpend, h.knownTx(txid).Status)
	h.requireRemoved(changeOp, "double-spent tx change removed")
}

// TestReconciler_DoubleSpend_UnreadableWinner_Defers proves that when ANY
// confirmed winner's inputs are unreadable (its consumption set is unknown) the
// whole release is DEFERRED — no input can be proven safe, so none is released.
func TestReconciler_DoubleSpend_UnreadableWinner_Defers(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0xDB)
	readable := newTxID(0xE3)   // mined, rawTx readable, spends inX
	unreadable := newTxID(0xE4) // mined, but no rawTx → consumption set unknown
	inX := opFor(0x46, 0)
	inY := opFor(0x47, 0)

	h.seedKnownTx(txid, []utxostore.Outpoint{inX, inY}, 0)
	h.mintAndSpendFunding(inX, txid)
	h.mintAndSpendFunding(inY, txid)
	h.markSuspect(txid, arcade.StatusDoubleSpendAttempted, []string{readable, unreadable})
	h.scriptTx(txid, arcade.StatusDoubleSpendAttempted, withCompeting(readable, unreadable))
	h.scriptTx(readable, arcade.StatusMined, withRawTx(buildRawTxWithInputs([]utxostore.Outpoint{inX})))
	h.scriptTx(unreadable, arcade.StatusMined) // no rawTx

	h.clock.Advance(reconGrace + time.Second)
	r := h.verify()
	require.Equal(t, 0, r.Released, "an unreadable winner defers the whole release")
	require.Equal(t, 1, r.Ambiguous)
	h.requireSpentBy(inX, txid, "nothing released while a winner's consumption set is unknown")
	h.requireSpentBy(inY, txid, "nothing released while a winner's consumption set is unknown")
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(txid).Status, "still suspect")
}

// --- 4. cascade converges --------------------------------------------------

func TestReconciler_Cascade_Converges(t *testing.T) {
	h := newReconStack(t)
	txA := newTxID(0xA0) // parent, verified dead
	txB := newTxID(0xB0) // child that spent A's change
	inA := opFor(0x51, 0)
	inB := opFor(0x52, 0)

	// Parent A with one funding input and change coin changeA.
	changeA := h.seedKnownTx(txA, []utxostore.Outpoint{inA}, 4000)
	h.mintAndSpendFunding(inA, txA)
	h.markSuspect(txA, arcade.StatusRejected, nil)
	h.scriptTx(txA, arcade.StatusRejected)

	// Child B spends A's change (while changeA is the only change/unproven coin)
	// plus its own funding input inB. B has no change and is NOT yet suspect.
	h.spendCoin(changeA, wdk.BasketNameForChange, utxostore.TierUnproven, txB)
	h.mintAndSpendFunding(inB, txB)
	h.seedKnownTx(txB, []utxostore.Outpoint{changeA, inB}, 0)
	h.scriptTx(txB, arcade.StatusRejected)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(txB).Status, "B starts non-suspect")

	// Pass 1+2: release A (only A is suspect). RemoveByMintTx(changeA) reports B
	// as AlreadySpentBy → B is cascaded into the suspect queue.
	h.clock.Advance(reconGrace + time.Second)
	require.Equal(t, 1, h.verify().Scanned, "only A is a suspect on the first passes")
	h.clock.Advance(reconGrace + time.Second)
	r2 := h.verify()
	require.Equal(t, 1, r2.Released, "A released")
	require.Equal(t, 1, r2.Cascaded, "child B cascaded into the suspect queue")
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(txB).Status, "B is now suspect")
	h.requireClaimable(inA, "A's input released")

	// Pass 3+4: B is now a suspect; two rejected passes release it too.
	h.clock.Advance(reconGrace + time.Second)
	require.Equal(t, 1, h.verify().Scanned, "B is the remaining suspect")
	h.clock.Advance(reconGrace + time.Second)
	r4 := h.verify()
	require.Equal(t, 1, r4.Released, "B released")
	h.requireClaimable(inB, "B's real funding input released")
	// changeA is A's change — A is provably dead, so it never existed on-chain.
	// When B (which spent it) dies, it must be REMOVED as phantom, NOT resurrected
	// into the claimable pool.
	h.requireRemoved(changeA, "A's phantom change removed (not resurrected) when B died")
	require.Equal(t, wdk.ProvenTxStatusInvalid, h.knownTx(txB).Status)

	// Converged: no suspects remain.
	h.clock.Advance(reconGrace + time.Second)
	require.Equal(t, 0, h.verify().Scanned, "cascade converged, no suspects remain")
}

// --- 5. max-quarantine stuck escalation ------------------------------------

func TestReconciler_MaxQuarantine_StuckEscalation(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0x5C)
	competitor := newTxID(0x5D)
	inA := opFor(0x61, 0)

	h.seedKnownTx(txid, []utxostore.Outpoint{inA}, 0)
	h.mintAndSpendFunding(inA, txid)
	h.markSuspect(txid, arcade.StatusDoubleSpendAttempted, []string{competitor})
	h.scriptTx(txid, arcade.StatusDoubleSpendAttempted, withCompeting(competitor))
	h.scriptTx(competitor, arcade.StatusSeenOnNetwork) // competitor never wins

	maxQ := 2 * time.Hour

	// Past grace but within quarantine: ambiguous, never released, never stuck.
	h.clock.Advance(reconGrace + time.Second)
	r1 := h.verifyMaxQ(maxQ)
	require.Equal(t, 0, r1.Released)
	require.Equal(t, 0, r1.Stuck)
	require.Equal(t, 1, r1.Ambiguous)
	h.requireSpentBy(inA, txid, "no release for an unresolved double spend")

	// Past max-quarantine: escalate to the terminal stuck state, never released.
	h.clock.Advance(maxQ)
	r2 := h.verifyMaxQ(maxQ)
	require.Equal(t, 1, r2.Stuck)
	require.Equal(t, 0, r2.Released)
	require.Equal(t, metastore.KnownTxStatusStuck, h.knownTx(txid).Status)
	require.Equal(t, wdk.TxStatusFailed, h.txStatus(txid))
	h.requireSpentBy(inA, txid, "a stuck tx NEVER auto-releases its inputs")

	// Stuck is terminal: no longer a suspect.
	r3 := h.verifyMaxQ(maxQ)
	require.Equal(t, 0, r3.Scanned, "stuck tx is not re-scanned")
}

// --- 6. outbox drain crash recovery ----------------------------------------

func TestReconciler_OutboxDrain_CrashRecovery(t *testing.T) {
	ctx := context.Background()
	h := newReconStack(t)
	txid := newTxID(0x7B)
	inA := opFor(0x71, 0)
	h.mintAndSpendFunding(inA, txid) // inA spent_by=txid

	// Simulate a crash AFTER the release enqueued its UNSPEND op but BEFORE the
	// inline execute ran: leave a pending outbox row, unexecuted.
	rawTxid, err := hexTxid(txid)
	require.NoError(t, err)
	payload := []byte(fmt.Sprintf(`{"spendingTxId":%q,"ops":[%q]}`, mustHash(t, txid).String(), inA.String()))
	require.NoError(t, h.meta.Outbox().Enqueue(ctx, rawTxid, "UNSPEND", 0, payload))
	h.requireSpentBy(inA, txid, "input still spent before the drain runs")

	// The drain worker replays it idempotently and marks it done.
	rep, err := h.p.DrainOutbox(ctx, 100)
	require.NoError(t, err)
	require.Equal(t, 1, rep.Drained)
	h.requireClaimable(inA, "drain worker released the input")
	pending, err := h.meta.Outbox().CountPending(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, pending, "completed op removed")

	// A poison op (unknown op_type) parks at the attempts ceiling.
	poisonTxid, err := hexTxid(newTxID(0x7C))
	require.NoError(t, err)
	require.NoError(t, h.meta.Outbox().Enqueue(ctx, poisonTxid, "BOGUS_OP", 0, []byte(`{}`)))
	var parked bool
	for range metastore.MaxOutboxAttempts {
		r, derr := h.p.DrainOutbox(ctx, 100)
		require.NoError(t, derr)
		if r.Parked > 0 {
			parked = true
		}
	}
	require.True(t, parked, "poison op reported parked once it crossed the attempts ceiling")
	pending, err = h.meta.Outbox().CountPending(ctx)
	require.NoError(t, err)
	require.Equal(t, 0, pending, "parked poison op drops out of the pending set")
}

// hexTxid encodes a txid the way the metastore (and the reconciler's outbox
// enqueue) do: a plain hex decode of the display string.
func hexTxid(txid string) ([]byte, error) {
	return hex.DecodeString(txid)
}

// --- 7. concurrent double pass never double-releases (run with -race) -------

func TestReconciler_ConcurrentDoublePass_NoDoubleRelease(t *testing.T) {
	h := newReconStack(t)
	txid := newTxID(0x9A)
	inA := opFor(0x91, 0)
	changeOp := h.seedKnownTx(txid, []utxostore.Outpoint{inA}, 2000)
	h.mintAndSpendFunding(inA, txid)
	h.markSuspect(txid, arcade.StatusRejected, nil)
	h.scriptTx(txid, arcade.StatusRejected)

	// Reach pass 2 eligibility (stamped + grace apart).
	h.clock.Advance(reconGrace + time.Second)
	require.Equal(t, 0, h.verify().Released, "pass 1 stamps only")
	h.clock.Advance(reconGrace + time.Second)

	// Two goroutines race the releasing pass; the CAS gate must admit exactly one.
	const workers = 4
	var released atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, err := h.p.VerifyAndReleaseSuspects(context.Background(), reconGrace, reconMaxQ, 100)
			require.NoError(t, err)
			released.Add(int64(rep.Released))
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), released.Load(), "exactly one racing pass releases the tx")
	h.requireClaimable(inA, "input released exactly once")
	h.requireRemoved(changeOp, "change removed exactly once")
	require.Equal(t, wdk.ProvenTxStatusInvalid, h.knownTx(txid).Status)
}
