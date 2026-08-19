package storage

// The ABORT half of the P0-3 one-row broadcast arbiter, plus the P0-5 Mode B
// atomicity story.
//
// [Provider.AbortAction] used to touch only the wallet-side transactions row: it
// released the funding reservation and marked the transaction 'aborted' while
// leaving known_txs exactly as it found it. known_txs is the only table the send
// sweep reads, so the raw bytes of an aborted transaction stayed fully
// broadcastable — with their inputs already handed back to the funder. That is
// the double spend the audit records as P0-3, and the first two tests here are
// its executable form.
//
// The rest are about HOW the abort commits. In Mode B the metastore and the
// utxostore are different databases, so the release cannot be in the same
// transaction as the fence; the abort therefore commits fence + durable INTENT
// and executes the utxo half afterwards, exactly as the reject->release
// reconciler does. What that buys is asserted directly: a utxo half that fails
// leaves a fenced transaction and held coins (never freed coins backing
// sendable bytes), and the drain worker finishes the job.

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// TestAbortAction_FencesTheRawTx is P0-3's abort half, end to end. A delayed
// transaction is parked at known-tx 'unsent' for the sweep to send later;
// aborting it gives the funding coin back. If the abort does not also fence the
// known tx, the sweep's next tick POSTs bytes whose inputs the wallet has
// already re-lent — a double spend the wallet authored itself.
//
// The assertion that matters is the oracle's call counter. Every other check
// here (status, work list, re-claimability) describes the mechanism; the
// counter is the property.
func TestAbortAction_FencesTheRawTx(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x51, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	require.Equal(t, wdk.ProvenTxStatusUnsent, h.knownTx(t, txid).Status,
		"the delayed transaction is parked on the sweep's work list")

	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.NoError(t, err)
	require.True(t, abr.Aborted)

	assert.Equal(t, metastore.KnownTxStatusAborted, h.knownTx(t, txid).Status,
		"the abort must land on the SAME row the broadcaster claims")

	// The row is off the work list at any age: 'aborted' is in none of
	// FindResendable's three status arms.
	waiting, err := h.meta.KnownTx().FindResendable(ctx, resendGrace, 100)
	require.NoError(t, err)
	for i := range waiting {
		assert.NotEqual(t, txid, waiting[i].TxID, "an aborted transaction is never resendable")
	}

	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Zero(t, h.oracle.calls,
		"the sweep broadcast a transaction the wallet had aborted and already refunded")

	// And the coin really did come back — to the funder, not merely in the
	// utxostore's opinion of itself.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.False(t, u.Pinned, "abort lifts the pre-broadcast pin")
	require.Equal(t, "", u.ReservedBy, "and releases the reservation")

	second, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, second.Inputs)
	assert.Equal(t, coin.TxID.String(), second.Inputs[0].SourceTxID,
		"the aborted action's coin is claimable by a new one")
}

// TestAbortAction_LosesToAnInFlightSend is the arbiter's other direction. A
// broadcaster that has already claimed the row may be mid-POST; the bytes can
// be on the wire. Aborting then would release inputs for a live transaction,
// so the abort must LOSE — and lose whole: the transactions-side CAS it already
// applied has to roll back with it, or the wallet is left with a transaction
// marked aborted whose bytes a broadcaster is still sending.
func TestAbortAction_LosesToAnInFlightSend(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x52, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// A broadcaster takes the row (the sweep or a synchronous SendWith would do
	// exactly this before its POST).
	require.NoError(t, h.meta.KnownTx().ClaimForSend(ctx, txid))

	_, err = h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.Error(t, err, "a claimed transaction may already be on the wire; it is not abortable")
	assert.ErrorIs(t, err, wdk.ErrNotAbortableAction)

	assert.Equal(t, wdk.ProvenTxStatusSending, h.knownTx(t, txid).Status,
		"the live claim is untouched")

	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.TxStatusUnprocessed, txRow.Status,
		"the losing abort rolled its own status write back")

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.True(t, u.Pinned, "and did not lift the in-flight send's pin")
	assert.Equal(t, res.Reference, u.ReservedBy, "nor hand its inputs back")

	n, err := h.meta.Outbox().CountPending(ctx)
	require.NoError(t, err)
	assert.Zero(t, n, "a refused abort records no release intent either")
}

// TestAbortViaOutbox_FenceSurvivesAFailedUtxoHalf is P0-5. In Mode B the fence
// (metastore) and the release (utxostore) cannot share a transaction, so the
// abort commits the fence plus a durable INTENT and runs the utxo half after.
//
// The ordering is the safety property, and this test asserts the bad half. With
// the utxostore refusing the release, the abort still reports success — the
// fence is committed and the intent is recorded — and the state it leaves is
// the SAFE one: a transaction nobody can broadcast whose coins are still held.
// The opposite ordering (release first, fence after) would leave freed coins
// behind sendable bytes, which is the double spend, and the old code could
// produce exactly that because its release committed inside a metastore.Do that
// could still roll back and re-run.
func TestAbortViaOutbox_FenceSurvivesAFailedUtxoHalf(t *testing.T) {
	ctx := context.Background()
	h, flaky := newFlakyUTXOHarness(t)
	require.False(t, h.p.ModeA(), "the outbox path only exists for split stores")

	res, signed, coin := h.createAndSign(t, 0x53, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	flaky.failRelease.Store(true)
	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.NoError(t, err, "the utxo half is best-effort; the fence already committed")
	require.True(t, abr.Aborted)

	// The fence is durable, by all three of the ways a send can be attempted.
	assert.Equal(t, metastore.KnownTxStatusAborted, h.knownTx(t, txid).Status)
	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.TxStatusAborted, txRow.Status)

	_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), string(metastore.KnownTxStatusAborted))
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Zero(t, h.oracle.calls, "no bytes may reach the network, failed utxo half or not")

	// And the coins are still HELD — the safe residue. Unpin ran before the
	// refused release, so the row is reserved-but-unpinned: recorded intent, no
	// coin handed out.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy, "a failed release must not free the coin")
	assert.False(t, u.Pinned)

	// The intent is parked in the outbox, with the change removal (which did
	// succeed inline) already marked done.
	pending := h.pendingOutbox(t)
	require.Len(t, pending, 1, "exactly the op that failed is left to replay")
	assert.Equal(t, opAbortRelease, pending[0].OpType)
	assert.Equal(t, 1, pending[0].Attempts, "and its failure was recorded")
	require.NotNil(t, pending[0].LastError)

	// The drain worker heals it, and the coin comes back.
	flaky.failRelease.Store(false)
	rep, err := h.p.DrainOutbox(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Drained)

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, "", u.ReservedBy, "the drained release gave the coin back")

	n, err := h.meta.Outbox().CountPending(ctx)
	require.NoError(t, err)
	assert.Zero(t, n)

	// Replaying a drained abort is a no-op, not a second release: the ops are
	// token-guarded, and the row is now free of the reservation they name.
	require.NoError(t, h.p.meta.Outbox().Enqueue(ctx, abortOutboxKey(res.Reference), opAbortRelease, 0,
		mustJSON(abortReleasePayload{UserID: int64(h.userID), Reservation: res.Reference})))
	rep, err = h.p.DrainOutbox(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Drained, "a replay drains cleanly")
	assert.Zero(t, rep.Failed)
}

// TestAbortViaOutbox_ParkedReleaseIsHealedByTheSweep pins the known residual of
// the two-phase abort, the gauge that makes it visible, and — the C4 half — the
// janitor that finally clears it.
//
// The pending outbox row heals a release that failed once. It does not heal one
// that fails MaxOutboxAttempts times — roughly ten minutes of continuous
// utxostore unavailability at the drain's cadence — because the row then drops
// out of FetchPending for good. The transaction stays safely fenced, so this is
// never a double spend, but the coins are left PINNED and reserved: invisible
// to the ordinary stale sweep by design (a pin is what stops a janitor freeing
// an in-flight send's inputs), and un-abortable a second time because the
// transactions CAS refuses an already-aborted row. Nothing reclaimed them.
//
// The fence-first sweep does, through the one listing that sees pinned rows.
// That is safe HERE and nowhere else: the pin is a proxy for "these bytes might
// still go out", and this transaction's fence already proves they cannot.
//
// ParkedTotal remains the operator's view of the interval between the two: every
// per-pass counter reads zero once a row has parked, so a standing backlog would
// otherwise be indistinguishable from an empty outbox.
func TestAbortViaOutbox_ParkedReleaseIsHealedByTheSweep(t *testing.T) {
	ctx := context.Background()
	h, flaky := newFlakyUTXOHarness(t)
	res, signed, coin := h.createAndSign(t, 0x55, 100_000, 40_000)
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// The whole utxostore is down, so the abort's unpin fails along with its
	// release — which is what leaves the residue PINNED rather than merely
	// reserved.
	flaky.unavailable()
	_, err = h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.NoError(t, err)

	// The inline attempt already burned one; drain until the row exhausts.
	var rep defs.OutboxDrainReport
	for range metastore.MaxOutboxAttempts {
		rep, err = h.p.DrainOutbox(ctx, 100)
		require.NoError(t, err)
	}

	pendingN, err := h.meta.Outbox().CountPending(ctx)
	require.NoError(t, err)
	assert.Zero(t, pendingN, "an exhausted row is no longer handed out")

	parkedN, err := h.meta.Outbox().CountParked(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, parkedN, "and it has not gone away — it is parked")
	assert.Equal(t, 1, rep.ParkedTotal,
		"the drain report carries the standing backlog, not just this pass's delta")
	assert.Zero(t, rep.Drained, "while every per-pass counter reads empty")
	assert.Zero(t, rep.Parked)

	// The residual itself: fenced, but the coins are still held — and pinned.
	assert.Equal(t, metastore.KnownTxStatusAborted, h.knownTx(t, signed.TxID().String()).Status)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Equal(t, res.Reference, u.ReservedBy, "a parked release strands the coins")
	require.True(t, u.Pinned, "and leaves them pinned, which is what hides them from every janitor")

	// The store comes back. The intent is still gone — nothing re-enqueues it —
	// so recovery has to come from the sweep, and the ORDINARY listing cannot
	// even see the work.
	flaky.available()
	stale, err := h.utxo.FindStaleReservations(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Empty(t, stale, "the pinned residue is invisible to the ordinary stale listing")
	inclusive, err := h.utxo.FindStaleReservationsIncludingPinned(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, inclusive, 1, "only the pinned-inclusive listing surfaces it")
	require.Equal(t, res.Reference, inclusive[0].Reservation)

	require.NoError(t, h.p.SweepStaleReservations(ctx, time.Now().Add(time.Hour), 100))

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.False(t, u.Pinned, "the sweep lifted the pin of an already-fenced transaction")
	assert.Equal(t, "", u.ReservedBy, "and gave the coin back")

	parkedN, err = h.meta.Outbox().CountParked(ctx)
	require.NoError(t, err)
	assert.Zero(t, parkedN,
		"the healed intent must be retired, or parked_total alerts on funds that came back")

	// Claimable in practice, not merely unreserved.
	second, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, second.Inputs)
	assert.Equal(t, coin.TxID.String(), second.Inputs[0].SourceTxID,
		"the healed coin funds a new action")
}

// TestSweepStaleReservations_WillNotCancelAQueuedDelayedSend is the limit on
// how far the fence-first sweep may go, and it is the reason the sweep's
// abortable set is narrower than the one AbortAction accepts.
//
// A delayed transaction sits at 'unprocessed' with its bytes stored, its inputs
// pinned, and SendWaiting owning the send. That status IS abortable — a user may
// call AbortAction on their own delayed payment — but a janitor doing the same
// thing on a timer is a payment canceled because arcade was slow or the circuit
// breaker was open for fifteen minutes. The pinned-inclusive listing means the
// sweep can now SEE these reservations, so the refusal has to be a decision
// rather than a side effect of the query.
//
// The assertion is that the send still happens afterwards: the coins are held,
// the fence is not set, and the sweep's next tick finds a broadcast instead of a
// corpse.
func TestSweepStaleReservations_WillNotCancelAQueuedDelayedSend(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x56, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.True(t, u.Pinned, "a stored delayed transaction pins its inputs")

	// Aged far past any reservation TTL, and visible to the sweep's listing.
	inclusive, err := h.utxo.FindStaleReservationsIncludingPinned(ctx, time.Now().Add(time.Hour), 10)
	require.NoError(t, err)
	require.Len(t, inclusive, 1, "the sweep's listing does surface it — refusing is a choice, not luck")

	require.NoError(t, h.p.SweepStaleReservations(ctx, time.Now().Add(time.Hour), 100))

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.True(t, u.Pinned, "the sweep must not unpin a transaction SendWaiting still owns")
	assert.Equal(t, res.Reference, u.ReservedBy, "nor hand its inputs back")
	assert.Equal(t, wdk.ProvenTxStatusUnsent, h.knownTx(t, txid).Status, "and must not fence its bytes")

	// The payment the janitor did not cancel goes out on the next tick.
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Equal(t, 1, h.oracle.calls, "the delayed send survived the sweep and was broadcast")
}

// TestAbortViaOutbox_UnsignedEnqueuesOnlyTheRelease covers the ErrNotFound arm
// of the fence: an action aborted before it was ever signed has no known_txs
// row to fence and no change to remove. It must still release, and it must not
// enqueue work that does not exist.
func TestAbortViaOutbox_UnsignedEnqueuesOnlyTheRelease(t *testing.T) {
	ctx := context.Background()
	h, flaky := newFlakyUTXOHarness(t)
	coin := h.mintFunding(t, 0x54, 100_000)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)

	flaky.failRelease.Store(true)
	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.NoError(t, err)
	require.True(t, abr.Aborted)

	pending := h.pendingOutbox(t)
	require.Len(t, pending, 1, "an unsigned action has no minted change to remove")
	assert.Equal(t, opAbortRelease, pending[0].OpType)
	assert.Equal(t, abortOutboxKey(res.Reference), pending[0].TxID,
		"and it is keyed by the reference surrogate, because there is no txid")

	flaky.failRelease.Store(false)
	rep, err := h.p.DrainOutbox(ctx, 100)
	require.NoError(t, err)
	assert.Equal(t, 1, rep.Drained)

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, "", u.ReservedBy, "an unsigned abort still gives the coin back")
}

// TestAbortOutboxKey_IsATxidWidthSurrogate pins the two properties the abort
// outbox key needs, since the column it goes in is the known-tx txid.
//
// It also stands in for the WithRetry re-run case that is otherwise not
// reachable from this harness. metastore.Do wraps its closure in
// sqlkit.WithRetry, so a serialization failure re-runs the whole fence
// including the Enqueue; there is no injection seam for that (SQLite's single
// writer never produces the retryable classes, and Do takes no hook), so what
// is asserted instead is the property the re-run relies on: the enqueue is
// keyed and deduplicated, so running it twice leaves ONE row.
func TestAbortOutboxKey_IsATxidWidthSurrogate(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	key := abortOutboxKey("some-reference")
	require.Len(t, key, 32, "the outbox txid column holds a 32-byte hash")
	assert.NotEqual(t, key, abortOutboxKey("some-other-reference"))
	assert.Equal(t, key, abortOutboxKey("some-reference"), "and it is a pure function of the reference")

	payload := mustJSON(abortReleasePayload{UserID: 1, Reservation: "some-reference"})
	require.NoError(t, h.meta.Outbox().Enqueue(ctx, key, opAbortRelease, 0, payload))
	require.NoError(t, h.meta.Outbox().Enqueue(ctx, key, opAbortRelease, 0, payload))

	n, err := h.meta.Outbox().CountPending(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, n, "a re-run of the fence closure must not double-enqueue")
}

// --- harness ---------------------------------------------------------------

// pendingOutbox returns the outbox rows still awaiting a drain.
func (h *harness) pendingOutbox(t *testing.T) []metastore.OutboxEntry {
	t.Helper()
	rows, err := h.meta.Outbox().FetchPending(context.Background(), 100)
	require.NoError(t, err)
	return rows
}

// flakyUTXO wraps a utxostore and can be told to refuse the two writes an
// abort's utxo half makes, so a test can drive the crash-consistency half of
// the Mode B abort without a real crash. Embedding the INTERFACE (not the
// concrete memstore) also keeps the provider out of Mode A, which is what the
// outbox path requires.
//
// The two switches are separate because they produce DIFFERENT residues, and
// the difference is the whole subject of the parked-row tests. Failing the
// release alone leaves coins reserved-but-UNPINNED (the unpin got through), a
// state the ordinary stale sweep can still see. A store that is properly
// unavailable fails both, and the residue is then pinned AND reserved — which
// no ordinary sweep can see at all, and which is what
// [Provider.SweepStaleReservations]' aborted arm exists to heal.
type flakyUTXO struct {
	utxostore.Store
	failRelease atomic.Bool
	failUnpin   atomic.Bool
}

func (f *flakyUTXO) ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error) {
	if f.failRelease.Load() {
		return 0, errors.New("utxostore unavailable")
	}
	return f.Store.ReleaseReservation(ctx, userID, reservation)
}

func (f *flakyUTXO) Unpin(ctx context.Context, userID int64, reservation string) (int, error) {
	if f.failUnpin.Load() {
		return 0, errors.New("utxostore unavailable")
	}
	return f.Store.Unpin(ctx, userID, reservation)
}

// unavailable / available toggle the whole utxo half at once, which is the
// realistic shape: an outbox row parks after ~ten minutes of a store being
// down, not after ten selective failures of one method.
func (f *flakyUTXO) unavailable() {
	f.failUnpin.Store(true)
	f.failRelease.Store(true)
}

func (f *flakyUTXO) available() {
	f.failUnpin.Store(false)
	f.failRelease.Store(false)
}

// newFlakyUTXOHarness is [newHarness] with the utxostore wrapped in
// [flakyUTXO]. The harness's own utxo handle stays the INNER memstore, so every
// assertion reads through the unwrapped store.
func newFlakyUTXOHarness(t *testing.T) (*harness, *flakyUTXO) {
	t.Helper()
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metastore.OpenSQLite(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	mem := memstore.New()
	t.Cleanup(func() { _ = mem.Close(ctx) })
	flaky := &flakyUTXO{Store: mem}

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, flaky, defs.DefaultFeeModel())
	oracle := &fakeOracle{}
	hdrs := &fakeHeaders{}

	p, err := New(logger, meta, flaky, fnd, oracle, hdrs,
		WithNetwork(defs.NetworkTestnet),
		WithStorageName("test-storage"),
		WithScriptsVerifier(alwaysValidScripts{}),
	)
	require.NoError(t, err)
	_, err = p.Migrate(ctx, "test-storage", "storage-identity-key")
	require.NoError(t, err)

	resp, err := p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	require.True(t, resp.IsNew)

	uid := resp.User.UserID
	return &harness{
		p: p, meta: meta, path: path, utxo: mem, oracle: oracle, hdrs: hdrs,
		userID: uid,
		auth:   wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid},
	}, flaky
}
