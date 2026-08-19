package storage

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// reSign returns a different SERIALIZATION of the same action template: same
// inputs, same outputs, a different nLockTime — hence a different txid. Nothing
// is actually signed here; the harness verifies scripts with
// alwaysValidScripts, so what these tests exercise is a caller re-driving
// ProcessAction with bytes that are not the ones already stored.
func reSign(t *testing.T, res *wdk.StorageCreateActionResult) *transaction.Transaction {
	t.Helper()
	tx := buildSignedTx(t, res)
	tx.LockTime = res.LockTime + 1
	return tx
}

// TestProcessNewTx_PinsReservedInputsWithTheRawTx is the P0-4 provider half: the
// moment storage owns a signed, broadcastable transaction, that transaction's
// inputs must become untouchable by every janitor. The pin is committed by the
// same unit of work that stores the raw tx, so there is no window in which the
// bytes exist and the coins are still sweepable.
func TestProcessNewTx_PinsReservedInputsWithTheRawTx(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x31, 100_000, 40_000)

	// Before: reserved but sweepable — CreateAction produces an ordinary hold.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Equal(t, res.Reference, u.ReservedBy)
	require.False(t, u.Pinned, "CreateAction must produce an ordinary, unpinned reservation")

	_, err = h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// After: still reserved, still unspent — and now pinned.
	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy, "the hold is still the action's")
	assert.Nil(t, u.SpentBy, "a delayed send has not spent anything yet")
	assert.True(t, u.Pinned, "the signed raw tx is stored; its inputs must be pinned")

	// The janitors are now structurally unable to free it.
	freed, err := h.utxo.ReleaseReservation(ctx, int64(h.userID), res.Reference)
	require.NoError(t, err)
	assert.Equal(t, 0, freed, "no janitor may free the inputs of an in-flight send")

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy, "the refused release left the hold intact")

	stale, err := h.utxo.FindStaleReservations(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	assert.Empty(t, stale, "a pinned reservation is invisible to the stale reaper")
}

// TestProcessNewTx_DivergentReDriveRefused is P1-4: a second ProcessAction for
// the same reference carrying DIFFERENT bytes must be refused outright. Letting
// it repoint the transactions row would orphan the raw tx already queued under
// the first txid — still resendable in known_txs, with nothing in transactions
// pointing at it — so the wallet could broadcast bytes it no longer believes it
// owns.
func TestProcessNewTx_DivergentReDriveRefused(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x32, 100_000, 40_000)
	txid1 := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	other := reSign(t, res)
	txid2 := other.TxID().String()
	require.NotEqual(t, txid1, txid2, "the re-drive must carry genuinely different bytes")

	_, err = h.processDelayed(t, res, other)
	require.Error(t, err, "a differently-signed re-drive must be refused, not applied")
	assert.ErrorIs(t, err, metastore.ErrTxIDMismatch)
	assert.Contains(t, err.Error(), txid1, "the refusal names the binding it is protecting")
	assert.Contains(t, err.Error(), txid2, "and the bytes it refused")

	// The row still points at the first signing.
	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, txRow.TxID)
	assert.Equal(t, txid1, *txRow.TxID, "the binding is unchanged")

	// And the second signing left no trace that a broadcaster could pick up.
	_, found, err = h.meta.KnownTx().FindByTxID(ctx, txid2)
	require.NoError(t, err)
	assert.False(t, found, "the refused bytes must not be stored as broadcastable")

	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid1)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.ProvenTxStatusUnsent, kt.Status, "the real transaction is still queued to send")
	assert.NotEmpty(t, kt.RawTx)
}

// TestDivergentReDriveCASErr_CarriesTheCause pins the branch the test above
// cannot reach: the pre-check passes and SetTxID's CAS catches the divergence
// instead, because a racer bound the row in between. That branch must keep the
// cause — it holds the transaction id, the only identity available once the
// winning txid is (deliberately) not re-read.
func TestDivergentReDriveCASErr_CarriesTheCause(t *testing.T) {
	// Exactly the shape SetTxID returns on a lost CAS.
	cause := fmt.Errorf("metastore: set txid: transaction 7: %w", metastore.ErrTxIDMismatch)
	err := divergentReDriveCASErr("ref-1", "txid-2", cause)
	assert.ErrorIs(t, err, metastore.ErrTxIDMismatch, "still matchable as a divergence")
	assert.Contains(t, err.Error(), "transaction 7", "the row identity survives the wrap")
}

// TestProcessNewTx_SameTxIDReplayIsIdempotent guards the benign half of the
// re-drive rule: the same bytes arriving twice (a retried request) must not be
// refused, and must not double any state. Pin reports 0 newly-pinned rows on the
// replay, which is a no-op, not a failure — the assertions are about the state,
// not the count.
func TestProcessNewTx_SameTxIDReplayIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x33, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	_, err = h.processDelayed(t, res, signed)
	require.NoError(t, err, "replaying the identical signing is idempotent, not a divergence")

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.True(t, u.Pinned, "still pinned after the replay")
	assert.Equal(t, res.Reference, u.ReservedBy)
	assert.Nil(t, u.SpentBy)

	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.ProvenTxStatusUnsent, kt.Status)

	rows, err := h.meta.Transactions().FindByTxID(ctx, h.userID, txid)
	require.NoError(t, err)
	assert.Len(t, rows, 1, "the replay created no second transaction row")
}

// TestProcessNewTx_RefusesToResurrectAFencedKnownTx: processNewTx's Upsert
// writes a PRE-broadcast known-tx status, so it is a backward transition and
// must carry the never-requeue skip set. A row that is aborted, terminal or
// mid-POST has been fenced deliberately; a re-drive that silently reset it to
// 'unsent' would hand those bytes straight back to the send sweep.
//
// Its second half is the only executable proof of the Mode B orphan-pin
// backstop. The refused action rolls its metadata back but NOT its pin — the
// utxostore is a separate store here and already committed — which is exactly
// the fail-safe asymmetry processNewTx documents. What makes that acceptable is
// that the orphan is reclaimable, and AbortAbandoned is what reclaims it.
func TestProcessNewTx_RefusesToResurrectAFencedKnownTx(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	require.False(t, h.p.ModeA(), "this test is about the split-store ordering; the harness must be Mode B")

	res, signed, coin := h.createAndSign(t, 0x34, 100_000, 40_000)
	txid := signed.TxID().String()

	// The known tx is fenced before the (late) signer arrives.
	require.NoError(t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID:   txid,
		Status: metastore.KnownTxStatusAborted,
	}))

	_, err := h.processDelayed(t, res, signed)
	require.Error(t, err, "a fenced known tx must not be re-queued for broadcast")
	assert.ErrorIs(t, err, metastore.ErrStatusUpdateSkipped)
	assert.Contains(t, err.Error(), txid)
	assert.Contains(t, err.Error(), string(metastore.KnownTxStatusAborted),
		"the refusal names the status that refused, not just that one did")

	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, metastore.KnownTxStatusAborted, kt.Status, "the fence held")
	assert.Empty(t, kt.RawTx, "the refused upsert stored no broadcastable bytes")

	// The metadata unit of work rolled back whole: nothing half-applied.
	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Nil(t, txRow.TxID, "the failed action bound no txid")
	assert.Equal(t, wdk.TxStatusUnsigned, txRow.Status)

	// The pin did NOT roll back with it: in Mode B it committed first, against a
	// store that has no part in the metastore's transaction. It now holds coins
	// for a transaction row that will never be signed.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.True(t, u.Pinned, "the pin committed before the meta half and outlives its rollback")
	require.Equal(t, res.Reference, u.ReservedBy)

	// The backstop: the row is unsigned and never broadcast, so the abandoned
	// sweep owns it. It unpins, then releases, and the coin is claimable again.
	require.NoError(t, h.p.AbortAbandoned(ctx, time.Now().Add(time.Hour), 10))

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.False(t, u.Pinned, "AbortAbandoned reclaimed the orphan pin")
	assert.Equal(t, "", u.ReservedBy, "and gave the coin back")
}

// TestProcessAction_AcceptedBroadcastClearsThePin: the pin is a pre-broadcast
// hold, not a permanent one. It has to be observed across two calls, because a
// single immediate ProcessAction pins and spends inside one call and shows
// nothing. So: persist delayed (pinned, unspent), then send it with a second
// action and watch the spend lift the pin.
func TestProcessAction_AcceptedBroadcastClearsThePin(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x35, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.True(t, u.Pinned, "the stored, unsent transaction holds its inputs")
	require.Nil(t, u.SpentBy)

	// The monitor's send, driven here as a SendWith batch.
	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	require.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy, "the accepted broadcast spent the input")
	assert.False(t, u.Pinned, "spending lifts the pin — pinned always implies unspent")
}

// TestAbortAction_LiftsThePinBeforeReleasing is the other half of the pin's
// lifecycle, and the reason [Provider.abortTxRow] unpins: abort is entitled to
// declare a pre-broadcast transaction dead, so it must be able to give the
// coins back. Without the lift the release is silently refused and the funding
// coin is stranded, reserved forever.
func TestAbortAction_LiftsThePinBeforeReleasing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x36, 100_000, 40_000)

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.True(t, u.Pinned)

	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
	require.NoError(t, err)
	require.True(t, abr.Aborted)

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.False(t, u.Pinned, "abort lifted the pin")
	assert.Equal(t, "", u.ReservedBy, "and gave the coin back")
}
