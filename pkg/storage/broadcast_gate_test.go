package storage

// The SEND half of the P0-3 one-row broadcast arbiter, from the provider's side.
//
// The metastore primitives (ClaimForSend / ReclaimStaleSend / the abort CAS) are
// pinned in pkg/storage/internal/metastore; these tests are about the two places
// that must actually TAKE the row before touching the network — the synchronous
// [Provider.broadcastOne] and the sweep's [Provider.sendOneWaiting] — plus the
// one requeue-shaped write that could walk a fenced row back onto the send work
// list, [Provider.queueDelayed]. The fence is only worth as much as its arbiter,
// so every assertion here is paired with the oracle's call counter: the point is
// never merely that a status is right, it is that no bytes left the process.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/validate"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// gateClock is a movable clock for the tests that have to age a claim past
// resendGrace. Mutex-guarded because SendWaitingTransactions reads it from its
// broadcast goroutines while the test body moves it.
type gateClock struct {
	mu sync.Mutex
	t  time.Time
}

func newGateClock() *gateClock {
	return &gateClock{t: time.Unix(1_700_000_000, 0).UTC()}
}

func (c *gateClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *gateClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// knownTx reads a known tx that must exist.
func (h *harness) knownTx(t *testing.T, txid string) metastore.KnownTx {
	t.Helper()
	kt, found, err := h.meta.KnownTx().FindByTxID(context.Background(), txid)
	require.NoError(t, err)
	require.True(t, found, "known tx %s must exist", txid)
	return *kt
}

// TestSendWith_RefusedAfterTheAbortFence is the send-side half of P0-3. The
// abort fence writes 'aborted' onto the very row the broadcaster claims, but a
// fence with no arbiter on the send side fences nothing: broadcastOne used to
// POST any SendWith txid that had stored bytes, without ever asking what state
// the row was in. Aborting releases the inputs, so re-POSTing afterwards is a
// double spend the wallet itself authored.
//
// The claim is what closes it, and the assertion that matters is the call
// counter: refusing after the POST would be no refusal at all.
func TestSendWith_RefusedAfterTheAbortFence(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x41, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	require.Equal(t, wdk.ProvenTxStatusUnsent, h.knownTx(t, txid).Status)

	// The abort half of the arbiter. C3 wires AbortAction to this; the primitive
	// is what the fence is made of either way.
	require.NoError(t, h.meta.KnownTx().TransitionToAborted(ctx, txid))

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.Error(t, err, "a fenced transaction must not be broadcastable")
	assert.Nil(t, out)
	assert.ErrorIs(t, err, metastore.ErrStatusUpdateSkipped, "the refusal is matchable as a lost/blocked CAS")
	assert.Contains(t, err.Error(), txid)
	assert.Contains(t, err.Error(), string(metastore.KnownTxStatusAborted),
		"the refusal names the status that refused, not just that one did")

	assert.Zero(t, h.oracle.calls, "no bytes may reach the network after the fence")
	assert.Equal(t, metastore.KnownTxStatusAborted, h.knownTx(t, txid).Status, "the fence held")
}

// TestSendWith_RefusedForEveryTerminalStatus generalizes the test above past
// the abort case. Every status the claim CAS refuses and that is not broadcast
// evidence is a status whose bytes must not be POSTed: a terminal failure is as
// unbroadcastable as a fenced abort, and the gate must not special-case its way
// into sending one.
func TestSendWith_RefusedForEveryTerminalStatus(t *testing.T) {
	for _, st := range []wdk.ProvenTxReqStatus{
		metastore.KnownTxStatusAborted,
		metastore.KnownTxStatusSuspectFailed,
		metastore.KnownTxStatusStuck,
		wdk.ProvenTxStatusInvalid,
		wdk.ProvenTxStatusDoubleSpend,
	} {
		t.Run(string(st), func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			res, signed, _ := h.createAndSign(t, 0x42, 100_000, 40_000)
			txid := signed.TxID().String()

			_, err := h.processDelayed(t, res, signed)
			require.NoError(t, err)
			require.NoError(t, h.meta.KnownTx().UpdateStatus(ctx, txid, st))

			_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
				SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
			})
			require.Error(t, err)
			assert.Contains(t, err.Error(), string(st))
			assert.Zero(t, h.oracle.calls, "a %s transaction is not broadcastable", st)
			assert.Equal(t, st, h.knownTx(t, txid).Status)
		})
	}
}

// TestQueueDelayed_RefusesToRequeueAFencedKnownTx is the other half of the same
// hole, and the one the claim alone cannot close. A DELAYED ProcessAction never
// reaches broadcastOne: it walks the row to 'unsent' and lets the sweep send it
// later. With the old guard (beyond-broadcast only) an aborted row was walked
// straight back to 'unsent', from where FindResendable's ungraced arm would
// re-POST bytes whose inputs the abort had already released — the fence
// resurrected by a write that never looked at it.
func TestQueueDelayed_RefusesToRequeueAFencedKnownTx(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x43, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	require.NoError(t, h.meta.KnownTx().TransitionToAborted(ctx, txid))

	_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsDelayed: true,
		SendWith:  []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.Error(t, err, "queueing a fenced transaction for the sweep must be refused")
	assert.Contains(t, err.Error(), string(metastore.KnownTxStatusAborted))

	kt := h.knownTx(t, txid)
	assert.Equal(t, metastore.KnownTxStatusAborted, kt.Status,
		"the delayed queue must not resurrect an aborted row to 'unsent'")

	// And the sweep's own work list agrees: the row is nowhere on it.
	waiting, err := h.meta.KnownTx().FindResendable(ctx, resendGrace, 100)
	require.NoError(t, err)
	for i := range waiting {
		assert.NotEqual(t, txid, waiting[i].TxID, "a fenced row is never resendable")
	}
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Zero(t, h.oracle.calls, "and the sweep POSTs nothing for it")
}

// TestQueueDelayed_TolerantOfRowsAlreadyPastBroadcast guards the other
// direction: widening the skip set must not turn the benign, idempotent skips
// into failures. Re-queueing a transaction the network already holds is a no-op
// a retrying client is entitled to make, and it was a no-op before this change.
func TestQueueDelayed_TolerantOfRowsAlreadyPastBroadcast(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x44, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	require.NoError(t, h.meta.KnownTx().UpdateStatus(ctx, txid, wdk.ProvenTxStatusUnconfirmed))

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsDelayed: true,
		SendWith:  []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err, "re-queueing an already-broadcast tx is idempotent, not an error")
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status,
		"and it did not walk the row backwards")
}

// TestSendWith_AlreadyBroadcastIsIdempotent: a SendWith naming a txid the
// network already accepted must report success WITHOUT a second POST. Before
// the gate this re-POSTed the bytes on every call — harmless at arcade (a
// duplicate is idempotent by txid) but a wasted round trip per retry, and it
// meant the provider had no opinion at all about the row's state.
func TestSendWith_AlreadyBroadcastIsIdempotent(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x45, 100_000, 40_000)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)
	require.Equal(t, 1, h.oracle.calls)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status)

	out, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err, "re-sending an accepted transaction is idempotent success")
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)
	require.Len(t, out.NotDelayedResults, 1)
	assert.Equal(t, wdk.ReviewActionResultStatusSuccess, out.NotDelayedResults[0].Status)
	assert.Equal(t, 1, h.oracle.calls, "the second SendWith re-POSTs nothing")
}

// TestSendOneWaiting_SkipsARowAnotherInstanceClaimed is the sweep-side race the
// claim exists for, in its smallest deterministic form.
//
// FindResendable hands out a work list; between that SELECT and the POST,
// another instance can take the same row. The stale KnownTx value passed here
// is exactly what the losing instance would still be holding — status 'unsent',
// as its snapshot saw it — while the row in the database has already moved to
// 'sending'. Losing that race must be a silent no-op, not an error and above
// all not a second POST.
func TestSendOneWaiting_SkipsARowAnotherInstanceClaimed(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x46, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	stale := h.knownTx(t, txid) // the work-list snapshot: still 'unsent'
	require.Equal(t, wdk.ProvenTxStatusUnsent, stale.Status)

	// The winning instance claims it.
	require.NoError(t, h.meta.KnownTx().ClaimForSend(ctx, txid))

	beefs, err := h.p.inputBEEFForBatch(ctx, []metastore.KnownTx{stale})
	require.NoError(t, err)
	require.NoError(t, h.p.sendOneWaiting(ctx, &stale, beefs[txid]),
		"losing the claim is a no-op for this tick, not a failure")
	assert.Zero(t, h.oracle.calls, "the row belongs to the other instance; it must not be POSTed twice")
	assert.Equal(t, wdk.ProvenTxStatusSending, h.knownTx(t, txid).Status, "and its claim is untouched")
}

// TestSendOneWaiting_DropsARowFencedMidSweep is the sweep's other race, and the
// one whose WRONG handling would be loudest. FindResendable selects a row, and
// before the POST an abort (or a rejection) fences it. The claim CAS refuses,
// correctly — but returning that refusal as an error puts it through the
// sweep's failure path, which logs "will retry next cycle" and increments a
// failed counter. There is no next cycle: FindResendable excludes every fenced
// and terminal status at any age, so the row is gone from the work list for
// good and the counter would climb forever describing an outcome that is the
// fence working.
//
// The stale KnownTx passed here is exactly the work-list snapshot the sweep
// would still be holding — status 'unsent', as the SELECT saw it — while the
// row in the database has already moved to 'aborted'.
func TestSendOneWaiting_DropsARowFencedMidSweep(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x4c, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	stale := h.knownTx(t, txid) // the work-list snapshot, taken before the fence
	require.Equal(t, wdk.ProvenTxStatusUnsent, stale.Status)

	// The fence lands between the SELECT and the send. C3 wires AbortAction to
	// this; the primitive is the fence either way.
	require.NoError(t, h.meta.KnownTx().TransitionToAborted(ctx, txid))

	beefs, err := h.p.inputBEEFForBatch(ctx, []metastore.KnownTx{stale})
	require.NoError(t, err)
	require.NoError(t, h.p.sendOneWaiting(ctx, &stale, beefs[txid]),
		"a row that left the broadcastable set is dropped from the sweep, not reported as a retryable failure")

	assert.Zero(t, h.oracle.calls, "the fence held even though the sweep had already selected the row")
	assert.Equal(t, metastore.KnownTxStatusAborted, h.knownTx(t, txid).Status)

	// And the row really is off the work list, which is what makes "will retry
	// next cycle" the wrong thing to have said.
	waiting, err := h.meta.KnownTx().FindResendable(ctx, resendGrace, 100)
	require.NoError(t, err)
	for i := range waiting {
		assert.NotEqual(t, txid, waiting[i].TxID, "there is no next cycle for a fenced row")
	}
}

// TestSendOneWaiting_TransientReadFailureStaysRetryable is the counterweight.
// Dropping fenced rows silently must not turn into dropping everything
// silently: a refusal the gate could not attribute to a status is transient,
// and transient failures have to keep reaching the sweep's warn-and-retry path.
// The distinction is the error type, so this pins it at that level.
func TestSendOneWaiting_TransientReadFailureStaysRetryable(t *testing.T) {
	fenced := notBroadcastableErr("abc", metastore.KnownTxStatusAborted)
	var as *notBroadcastableError
	require.ErrorAs(t, fenced, &as, "the sweep recognizes a fenced row by type")
	assert.Equal(t, metastore.KnownTxStatusAborted, as.status, "and reads the status off it, without a re-query")
	assert.ErrorIs(t, fenced, metastore.ErrStatusUpdateSkipped, "while staying matchable as a blocked CAS")

	vanished := vanishedWhileClaimingErr("abc", metastore.ErrNotFound)
	assert.NotErrorAs(t, vanished, &as, "a vanished row is not a fenced one; it must stay retryable")
	assert.NotContains(t, vanished.Error(), "no stored raw tx",
		"and it must not borrow the missing-bytes message, which means something else entirely")
}

// TestSendWaiting_RecoversAStrandedSend is the crash-recovery arm end to end. A
// claimant that dies mid-POST leaves the row at 'sending' with no owner alive.
// Nothing may touch it inside the grace — the original send could still be in
// flight — and past the grace exactly one sweep re-drives it.
//
// The two halves are asserted with the same counter, which is the only way to
// tell "correctly waited" from "quietly lost".
//
// NOT a red proof: FindResendable's graced 'sending' arm already did this
// before the claim gate existed, so this test passed on the parent commit too.
// It is here as the end-to-end regression guard for the recovery path the gate
// now sits in the middle of — and for one thing that IS new, asserted at the
// end: the 202 has to be able to advance a row OUT of the 'sending' the claim
// put it in.
func TestSendWaiting_RecoversAStrandedSend(t *testing.T) {
	ctx := context.Background()
	clock := newGateClock()
	h := newHarnessClock(t, clock.Now)
	res, signed, _ := h.createAndSign(t, 0x47, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// A claimant takes the row and then "dies" without POSTing.
	require.NoError(t, h.meta.KnownTx().ClaimForSend(ctx, txid))
	claimed := h.knownTx(t, txid)
	require.Equal(t, wdk.ProvenTxStatusSending, claimed.Status)

	// Inside the grace the send is presumed live and is nobody else's to drive.
	clock.Advance(resendGrace - time.Second)
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	require.Zero(t, h.oracle.calls, "a live claim inside the grace must not be re-POSTed")
	require.Equal(t, wdk.ProvenTxStatusSending, h.knownTx(t, txid).Status)

	// Past it the claimant is presumed dead and the send is re-driven exactly once.
	clock.Advance(2 * time.Second)
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Equal(t, 1, h.oracle.calls, "the stranded send is re-driven")

	// The 202 advances the row out of 'sending' — the forward write must not be
	// blocked by the fenced status the claim put it in.
	kt := h.knownTx(t, txid)
	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, kt.Status, "'sending' is not beyond-broadcast; the 202 applies")
	assert.True(t, kt.WasBroadcast, "and the acceptance is sticky")
}

// TestSendWaiting_StrandedSendBacksOffAFullGrace pins the second thing the
// re-claim buys. A send that keeps failing must be retried once per GRACE, not
// once per tick: the re-claim re-stamps updated_at, which is what drops the row
// out of FindResendable's graced arm again. Without the re-stamp a permanently
// stuck transaction becomes an unbounded POST loop against arcade.
func TestSendWaiting_StrandedSendBacksOffAFullGrace(t *testing.T) {
	ctx := context.Background()
	clock := newGateClock()
	h := newHarnessClock(t, clock.Now)
	h.oracle.broadcast = func(context.Context, string, []byte) (*arcade.BroadcastResult, error) {
		return nil, errors.New("arcade unreachable")
	}
	res, signed, _ := h.createAndSign(t, 0x48, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	require.NoError(t, h.meta.KnownTx().ClaimForSend(ctx, txid))
	before := h.knownTx(t, txid)

	clock.Advance(resendGrace + time.Second)
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	require.Equal(t, 1, h.oracle.calls)

	after := h.knownTx(t, txid)
	assert.Equal(t, wdk.ProvenTxStatusSending, after.Status,
		"a transport failure leaves the row claimed, for the graced arm to recover")
	assert.True(t, after.UpdatedAt.After(before.UpdatedAt), "the re-claim re-stamped the clock")

	// Same tick, same instant: the row has backed off and is not re-POSTed.
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Equal(t, 1, h.oracle.calls, "a stuck send is retried once per grace, not once per tick")

	clock.Advance(resendGrace + time.Second)
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	assert.Equal(t, 2, h.oracle.calls, "and it does come back once the grace has elapsed again")
}

// TestBroadcastOne_TakesOverAStrandedSend: the synchronous path is a recovery
// path too. A client re-driving ProcessAction{SendWith} long after the process
// that claimed the row died must be able to take the row over, exactly as the
// sweep would — and must NOT be able to when the claim is still live.
//
// The cutoff both branches turn on is resendGrace, the same constant
// FindResendable selects with. Sourcing it anywhere else would let the taker
// and the selector disagree about what "stranded" means.
func TestBroadcastOne_TakesOverAStrandedSend(t *testing.T) {
	ctx := context.Background()
	clock := newGateClock()
	h := newHarnessClock(t, clock.Now)
	res, signed, _ := h.createAndSign(t, 0x49, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	require.NoError(t, h.meta.KnownTx().ClaimForSend(ctx, txid))

	// A live claim: report in-flight, POST nothing.
	clock.Advance(resendGrace - time.Second)
	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err, "a live send elsewhere is not this caller's error")
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusSending, out.SendWithResults[0].Status)
	require.Zero(t, h.oracle.calls, "the live claimant owns the POST")

	// A non-'unproven' send result in a non-delayed batch must carry a review
	// entry explaining itself. Nothing was POSTed, but the wallet-side validator
	// fails such a batch and builds its error out of these two lists, so an
	// entry-less in-flight result would fail the caller's action while saying
	// nothing about the transaction that caused the failure.
	require.Len(t, out.NotDelayedResults, 1, "the in-flight tx explains itself")
	assert.Equal(t, wdk.ReviewActionResultStatusServiceError, out.NotDelayedResults[0].Status)
	assert.Equal(t, txid, string(out.NotDelayedResults[0].TxID))
	require.Contains(t, out.NotDelayedResults[0].Errors, "storage")
	assert.Contains(t, out.NotDelayedResults[0].Errors["storage"].Error(), "concurrent broadcast attempt")

	// A stranded one: take it over and drive it home.
	clock.Advance(2 * time.Second)
	out, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)
	assert.Equal(t, 1, h.oracle.calls, "the stranded send was taken over")
	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status)
}

// TestBroadcastOne_RejectionFromSendingMarksSuspect is the residue guard for
// the 4xx arm under the new flow. The claim means commitRejected now always
// runs against a row at 'sending' rather than at 'unsent'/'unprocessed', so the
// suspect write has to accept that as its from-state; if it ever grows a status
// predicate, this is the test that catches it.
func TestBroadcastOne_RejectionFromSendingMarksSuspect(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.oracle.broadcast = func(_ context.Context, txid string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusRejected, Rejected: true, ExtraInfo: "bad tx"}, nil
	}
	res, signed, _ := h.createAndSign(t, 0x4a, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusFailed, out.SendWithResults[0].Status)
	require.Equal(t, 1, h.oracle.calls)

	kt := h.knownTx(t, txid)
	assert.Equal(t, metastore.KnownTxStatusSuspectFailed, kt.Status,
		"a 4xx must move the claimed row to suspectFailed, not strand it at 'sending'")
	require.NotNil(t, kt.RejectReason)
	assert.Equal(t, "bad tx", *kt.RejectReason)
}

// TestSendOneWaiting_RejectionFromSendingMarksSuspect is the same guard on the
// sweep's writer, which is a different statement (MarkSuspect, not
// MarkSuspectFailed) and so needs its own coverage.
func TestSendOneWaiting_RejectionFromSendingMarksSuspect(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.oracle.broadcast = func(_ context.Context, txid string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{
			TxID: txid, Status: arcade.StatusRejected, Rejected: true, ExtraInfo: "sweep says no",
		}, nil
	}
	res, signed, _ := h.createAndSign(t, 0x4b, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	require.NoError(t, h.p.SendWaitingTransactions(ctx, 100))
	require.Equal(t, 1, h.oracle.calls)

	kt := h.knownTx(t, txid)
	assert.Equal(t, metastore.KnownTxStatusSuspectFailed, kt.Status)
	require.NotNil(t, kt.RejectReason)
	assert.Equal(t, "sweep says no", *kt.RejectReason)
}

// TestSendWith_MixedBatchIsWellFormedForTheValidator is the shape check the
// review entry above exists for, exercised through the real validator.
//
// A non-delayed batch of two, one accepted and one already claimed elsewhere,
// legitimately fails validate.NotDelayedProcessActionResult — that rule fires
// whenever the send results are not ALL 'unproven', and it did so long before
// the claim gate. What the gate must not do is make the failure undiagnosable.
// The validator's error is handed to the caller wrapped in a
// pkgerrors.ProcessActionError carrying both result lists, so the invariant
// worth pinning is the pairing: every send result that is not 'unproven' has a
// review entry against the SAME txid saying why.
func TestSendWith_MixedBatchIsWellFormedForTheValidator(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	resA, signedA, _ := h.createAndSign(t, 0x4d, 100_000, 40_000)
	txidA := signedA.TxID().String()
	_, err := h.processDelayed(t, resA, signedA)
	require.NoError(t, err)

	resB, signedB, _ := h.createAndSign(t, 0x4e, 100_000, 40_000)
	txidB := signedB.TxID().String()
	_, err = h.processDelayed(t, resB, signedB)
	require.NoError(t, err)

	// B is claimed by another instance and its claim is live.
	require.NoError(t, h.meta.KnownTx().ClaimForSend(ctx, txidB))

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{
			primitives.TXIDHexString(txidA),
			primitives.TXIDHexString(txidB),
		},
	})
	require.NoError(t, err, "one tx being in flight elsewhere must not fail the whole batch at storage")
	require.Equal(t, 1, h.oracle.calls, "only the unclaimed transaction is POSTed")

	require.Len(t, out.SendWithResults, 2)
	assert.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status, "A was accepted")
	assert.Equal(t, wdk.SendWithResultStatusSending, out.SendWithResults[1].Status, "B is in flight elsewhere")

	// The pairing invariant, checked the way a caller reading the error would.
	reviews := map[string]wdk.ReviewActionResult{}
	for _, r := range out.NotDelayedResults {
		reviews[string(r.TxID)] = r
	}
	for _, swr := range out.SendWithResults {
		if swr.Status == wdk.SendWithResultStatusUnproven {
			continue
		}
		assert.Contains(t, reviews, string(swr.TxID),
			"a %s send result must come with a review entry explaining it", swr.Status)
	}
	assert.Equal(t, wdk.ReviewActionResultStatusSuccess, reviews[txidA].Status)
	assert.Equal(t, wdk.ReviewActionResultStatusServiceError, reviews[txidB].Status)

	// The batch does fail validation — that is the rule, not a regression — and
	// the caller now gets an error whose payload describes both transactions.
	verr := validate.NotDelayedProcessActionResult(out)
	require.Error(t, verr, "a batch whose send results are not all unproven fails the not-delayed rule")
	assert.Len(t, out.NotDelayedResults, 2, "and both transactions are represented in the payload it carries")
}
