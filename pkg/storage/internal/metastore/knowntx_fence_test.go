package metastore_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// mustKnownTx inserts a known tx in the given status/broadcast state.
func mustKnownTx(ctx context.Context, t *testing.T, s *metastore.Store, txid string, status wdk.ProvenTxReqStatus, wasBroadcast bool) {
	t.Helper()
	require.NoError(t, s.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID: txid, Status: status, RawTx: []byte{0x01, 0x02}, WasBroadcast: wasBroadcast,
	}))
}

// notPreBroadcastStatuses is every status outside the arbiter's pre-broadcast
// from-set, so NEITHER half may move a row in one of them: both
// TransitionToAborted and ClaimForSend must refuse the lot. The two CAS targets
// ('aborted', 'sending') are in here deliberately — that is what makes the
// interlock mutual.
//
// The first six isolate the STATUS predicate on its own: Upsert only stamps
// was_broadcast for beyond-broadcast statuses, so these rows reach the CAS with
// was_broadcast=false and the refusal can only have come from the status. For
// unmined/unconfirmed/completed both predicates fail at once, so those cases
// prove refusal but not which half did it.
var notPreBroadcastStatuses = []wdk.ProvenTxReqStatus{
	wdk.ProvenTxStatusSending,
	metastore.KnownTxStatusAborted,
	metastore.KnownTxStatusSuspectFailed,
	metastore.KnownTxStatusStuck,
	wdk.ProvenTxStatusInvalid,
	wdk.ProvenTxStatusDoubleSpend,
	wdk.ProvenTxStatusUnmined,
	wdk.ProvenTxStatusUnconfirmed,
	wdk.ProvenTxStatusCompleted,
}

// knownTxStatus reads back a known tx's current status.
func knownTxStatus(ctx context.Context, t *testing.T, s *metastore.Store, txid string) wdk.ProvenTxReqStatus {
	t.Helper()
	kt, found, err := s.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	return kt.Status
}

// testKnownTxTransitionToAborted pins the ABORT half of the one-row CAS arbiter
// (audit P0-3): a wallet transaction aborted before any broadcast evidence
// existed fences its known tx at 'aborted', from which no sweep, re-drive or
// SendWith batch can resurrect it. The CAS applies only from the three
// pre-broadcast statuses and only while was_broadcast is still false; every
// other row reports [metastore.ErrStatusUpdateSkipped] and is left untouched.
func testKnownTxTransitionToAborted(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	// The three abortable pre-broadcast statuses each transition.
	for _, from := range []wdk.ProvenTxReqStatus{
		wdk.ProvenTxStatusUnsent,
		wdk.ProvenTxStatusUnprocessed,
		wdk.ProvenTxStatusNoSend,
	} {
		txid := randTxID(t)
		mustKnownTx(ctx, t, s, txid, from, false)
		require.NoError(t, s.KnownTx().TransitionToAborted(ctx, txid), "abortable from %s", from)
		require.Equal(t, metastore.KnownTxStatusAborted, knownTxStatus(ctx, t, s, txid))
	}

	// Everything else is refused, and the row is left exactly as it was. The
	// aborted row itself is in this list — the positive CAS excludes its own
	// target, the same convention as TransitionSuspect — so a second abort
	// reports a skip rather than nil. Callers must treat that skip as
	// NOT-ABORTABLE and roll back, never re-read the status and call it
	// success: the read is not atomic with the CAS.
	for _, from := range notPreBroadcastStatuses {
		txid := randTxID(t)
		mustKnownTx(ctx, t, s, txid, from, false)
		require.ErrorIs(t, s.KnownTx().TransitionToAborted(ctx, txid), metastore.ErrStatusUpdateSkipped,
			"not abortable from %s", from)
		require.Equal(t, from, knownTxStatus(ctx, t, s, txid), "row unchanged from %s", from)
	}

	// Broadcast evidence fences the abort even from an abortable status: the
	// bytes may already be on the network, so the wallet must never pretend
	// they were not.
	broadcast := randTxID(t)
	mustKnownTx(ctx, t, s, broadcast, wdk.ProvenTxStatusUnsent, true)
	require.ErrorIs(t, s.KnownTx().TransitionToAborted(ctx, broadcast), metastore.ErrStatusUpdateSkipped,
		"was_broadcast row is never abortable")
	require.Equal(t, wdk.ProvenTxStatusUnsent, knownTxStatus(ctx, t, s, broadcast))

	// A txid that does not exist is ErrNotFound, never a skip.
	require.ErrorIs(t, s.KnownTx().TransitionToAborted(ctx, randTxID(t)), metastore.ErrNotFound)

	// Idempotency: the second abort of the same row reports a skip.
	twice := randTxID(t)
	mustKnownTx(ctx, t, s, twice, wdk.ProvenTxStatusUnsent, false)
	require.NoError(t, s.KnownTx().TransitionToAborted(ctx, twice))
	require.ErrorIs(t, s.KnownTx().TransitionToAborted(ctx, twice), metastore.ErrStatusUpdateSkipped,
		"re-abort of an already-aborted row is a skip, not a second apply")
	require.Equal(t, metastore.KnownTxStatusAborted, knownTxStatus(ctx, t, s, twice))
}

// testKnownTxClaimForSend pins the SEND half of the same arbiter: a broadcast
// attempt claims the row into 'sending' with the identical CAS, so abort and
// broadcast contend on ONE row and exactly one of them can ever win.
func testKnownTxClaimForSend(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)

	for _, from := range []wdk.ProvenTxReqStatus{
		wdk.ProvenTxStatusUnsent,
		wdk.ProvenTxStatusUnprocessed,
		wdk.ProvenTxStatusNoSend,
	} {
		txid := randTxID(t)
		mustKnownTx(ctx, t, s, txid, from, false)
		require.NoError(t, s.KnownTx().ClaimForSend(ctx, txid), "claimable from %s", from)
		require.Equal(t, wdk.ProvenTxStatusSending, knownTxStatus(ctx, t, s, txid))
	}

	for _, from := range notPreBroadcastStatuses {
		txid := randTxID(t)
		mustKnownTx(ctx, t, s, txid, from, false)
		require.ErrorIs(t, s.KnownTx().ClaimForSend(ctx, txid), metastore.ErrStatusUpdateSkipped,
			"not claimable from %s", from)
		require.Equal(t, from, knownTxStatus(ctx, t, s, txid), "row unchanged from %s", from)
	}

	broadcast := randTxID(t)
	mustKnownTx(ctx, t, s, broadcast, wdk.ProvenTxStatusUnsent, true)
	require.ErrorIs(t, s.KnownTx().ClaimForSend(ctx, broadcast), metastore.ErrStatusUpdateSkipped,
		"an already-broadcast row is not claimed again")
	require.Equal(t, wdk.ProvenTxStatusUnsent, knownTxStatus(ctx, t, s, broadcast), "row unchanged")

	require.ErrorIs(t, s.KnownTx().ClaimForSend(ctx, randTxID(t)), metastore.ErrNotFound)

	// THE INTERLOCK. Abort and claim contend on one row; whichever CAS lands
	// first wins outright and the loser is refused — the property that makes
	// "aborted but still broadcastable" (P0-3) unrepresentable.
	abortFirst := randTxID(t)
	mustKnownTx(ctx, t, s, abortFirst, wdk.ProvenTxStatusUnsent, false)
	require.NoError(t, s.KnownTx().TransitionToAborted(ctx, abortFirst))
	require.ErrorIs(t, s.KnownTx().ClaimForSend(ctx, abortFirst), metastore.ErrStatusUpdateSkipped,
		"an aborted row can never be claimed for broadcast")
	require.Equal(t, metastore.KnownTxStatusAborted, knownTxStatus(ctx, t, s, abortFirst))

	claimFirst := randTxID(t)
	mustKnownTx(ctx, t, s, claimFirst, wdk.ProvenTxStatusUnsent, false)
	require.NoError(t, s.KnownTx().ClaimForSend(ctx, claimFirst))
	require.ErrorIs(t, s.KnownTx().TransitionToAborted(ctx, claimFirst), metastore.ErrStatusUpdateSkipped,
		"an in-flight send can never be aborted out from under the broadcaster")
	require.Equal(t, wdk.ProvenTxStatusSending, knownTxStatus(ctx, t, s, claimFirst))
}

// testKnownTxReclaimStaleSend pins the recovery-path CAS: a send stranded at
// 'sending' past the grace is taken by exactly one sweep, and taking it
// re-stamps updated_at so the row backs off for a full grace instead of being
// re-POSTed every tick. The status is deliberately NOT changed — 'sending' is
// already the fenced state.
func testKnownTxReclaimStaleSend(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	clock := &manualClock{t: baseTime}
	s := factory(t, metastore.WithClock(clock.now))
	kt := s.KnownTx()

	const grace = 30 * time.Second

	// Everything here is created against the base clock; the clock then jumps
	// well past the grace, so these rows are stale and anything created after
	// the jump is not. Both the finder and the taker derive their cutoff from
	// this same store clock, so the test never computes one itself.
	stale := randTxID(t)
	broadcast := randTxID(t)
	aborted := randTxID(t)
	mustKnownTx(ctx, t, s, stale, wdk.ProvenTxStatusSending, false)
	mustKnownTx(ctx, t, s, broadcast, wdk.ProvenTxStatusSending, true)
	mustKnownTx(ctx, t, s, aborted, metastore.KnownTxStatusAborted, false)
	for _, other := range []wdk.ProvenTxReqStatus{
		wdk.ProvenTxStatusUnsent,
		wdk.ProvenTxStatusUnprocessed,
		metastore.KnownTxStatusAborted,
		wdk.ProvenTxStatusUnconfirmed,
	} {
		txid := randTxID(t)
		mustKnownTx(ctx, t, s, txid, other, false)
		clock.advance(time.Minute)
		require.ErrorIs(t, kt.ReclaimStaleSend(ctx, txid, grace), metastore.ErrStatusUpdateSkipped,
			"only a 'sending' row is reclaimable, not %s", other)
		require.Equal(t, other, knownTxStatus(ctx, t, s, txid), "row unchanged from %s", other)
	}

	clock.advance(time.Minute)
	fresh := randTxID(t)
	mustKnownTx(ctx, t, s, fresh, wdk.ProvenTxStatusSending, false)

	// A live send inside the grace is nobody's to take.
	require.ErrorIs(t, kt.ReclaimStaleSend(ctx, fresh, grace), metastore.ErrStatusUpdateSkipped,
		"a fresh claim is a live send, not a stranded one")

	// An already-broadcast row is out of scope however stale it looks.
	require.ErrorIs(t, kt.ReclaimStaleSend(ctx, broadcast, grace), metastore.ErrStatusUpdateSkipped,
		"was_broadcast fences the re-claim too")

	// The genuinely stranded row is taken, and the take only moves the clock.
	before, found, err := kt.FindByTxID(ctx, stale)
	require.NoError(t, err)
	require.True(t, found)
	require.NoError(t, kt.ReclaimStaleSend(ctx, stale, grace))
	after, _, err := kt.FindByTxID(ctx, stale)
	require.NoError(t, err)
	require.Equal(t, wdk.ProvenTxStatusSending, after.Status, "the fenced status is unchanged")
	require.True(t, after.UpdatedAt.After(before.UpdatedAt), "the re-claim re-stamps updated_at")

	// Exactly-one-winner: the second sweep this window is refused, because the
	// first one's re-stamp moved the row past the cutoff.
	require.ErrorIs(t, kt.ReclaimStaleSend(ctx, stale, grace), metastore.ErrStatusUpdateSkipped,
		"a second sweep in the same window loses the race")

	// And that re-stamp is the backoff: the row has left FindResendable's
	// graced arm until it has aged another full grace.
	resendable, err := kt.FindResendable(ctx, grace, 100)
	require.NoError(t, err)
	require.NotEmpty(t, resendable, "the sweep must return SOMETHING, or the exclusions below assert nothing")
	require.NotContains(t, knownTxIDs(resendable), stale, "a just-reclaimed row backs off for a full grace")

	// The P0-3 fence, asserted here so it is covered on PostgreSQL too: an
	// aborted row carrying a raw tx and no broadcast evidence is never
	// resendable, however long it has sat.
	require.NotContains(t, knownTxIDs(resendable), aborted,
		"an aborted tx is fenced from the resend sweep (P0-3)")

	// Once it ages again it is reclaimable again — a permanently stuck send is
	// retried once per grace, not once per tick.
	clock.advance(time.Minute)
	resendable, err = kt.FindResendable(ctx, grace, 100)
	require.NoError(t, err)
	require.Contains(t, knownTxIDs(resendable), stale, "a still-stranded row returns after the grace")
	require.NotContains(t, knownTxIDs(resendable), aborted, "still fenced after further aging")
	require.NoError(t, kt.ReclaimStaleSend(ctx, stale, grace))

	require.ErrorIs(t, kt.ReclaimStaleSend(ctx, randTxID(t), grace), metastore.ErrNotFound)
}

// testTransactionsSetTxIDCAS pins the P1-4 fix: binding a transaction row to a
// txid is a compare-and-set against NULL-or-the-same-txid, so a ProcessAction
// re-drive that produced a DIFFERENT signing is refused rather than silently
// repointing the row and orphaning the raw tx already queued under the old
// txid.
func testTransactionsSetTxIDCAS(t *testing.T, factory storeFactory) {
	ctx := context.Background()
	s := factory(t)
	uid := mustUser(ctx, t, s, "settxid-cas-user")

	id, err := s.Transactions().Insert(ctx, metastore.NewTx{
		UserID: uid, Status: wdk.TxStatusUnsigned, Reference: "settxid-ref",
	})
	require.NoError(t, err)

	first := randTxID(t)
	require.NoError(t, s.Transactions().SetTxID(ctx, id, first), "NULL txid binds")

	require.NoError(t, s.Transactions().SetTxID(ctx, id, first), "re-binding the same txid is idempotent")

	other := randTxID(t)
	require.ErrorIs(t, s.Transactions().SetTxID(ctx, id, other), metastore.ErrTxIDMismatch,
		"a divergent signing must be refused, not repointed")

	got, found, err := s.Transactions().FindByID(ctx, id)
	require.NoError(t, err)
	require.True(t, found)
	require.NotNil(t, got.TxID)
	require.Equal(t, first, *got.TxID, "the row keeps its original txid")

	require.ErrorIs(t, s.Transactions().SetTxID(ctx, id+10_000, randTxID(t)), metastore.ErrNotFound,
		"a missing row is ErrNotFound, never a mismatch")
}

// TestKnownTxNeverRequeueStatuses pins the membership of the skip set that
// queueDelayed-style UpdateStatus calls and processNewTx's Upsert pass, so a
// re-drive can never regress a row back to 'unsent'. It is derived from
// [wdk.ProvenTxReqBeyondBroadcastStageStatuses] so a future beyond-broadcast
// status is fenced automatically.
func TestKnownTxNeverRequeueStatuses(t *testing.T) {
	in := make(map[wdk.ProvenTxReqStatus]bool, len(metastore.KnownTxNeverRequeueStatuses))
	for _, st := range metastore.KnownTxNeverRequeueStatuses {
		require.False(t, in[st], "no duplicate entries: %s", st)
		in[st] = true
	}

	for _, st := range wdk.ProvenTxReqBeyondBroadcastStageStatuses {
		require.True(t, in[st], "every beyond-broadcast status is fenced: %s", st)
	}
	for _, st := range []wdk.ProvenTxReqStatus{
		metastore.KnownTxStatusAborted,
		metastore.KnownTxStatusSuspectFailed,
		metastore.KnownTxStatusStuck,
		wdk.ProvenTxStatusInvalid,
		wdk.ProvenTxStatusDoubleSpend,
		// 'sending' is deliberately IN the set: an Upsert or queueDelayed must
		// never regress an in-flight send back to the delayed queue. Genuinely
		// stranded senders are recovered by FindResendable's graced arm, which
		// is a read-side sweep, not a status regression.
		wdk.ProvenTxStatusSending,
	} {
		require.True(t, in[st], "fenced status missing from the skip set: %s", st)
	}

	for _, st := range []wdk.ProvenTxReqStatus{
		wdk.ProvenTxStatusUnsent,
		wdk.ProvenTxStatusUnprocessed,
		wdk.ProvenTxStatusNoSend,
		wdk.ProvenTxStatusNonFinal,
	} {
		require.False(t, in[st], "pre-broadcast status must stay re-queueable: %s", st)
	}
}
