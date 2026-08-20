package storage

// Audit P0-2 (caller half) and P1-3: what an ACCEPTED broadcast owes the
// inventory.
//
// Once arcade has the bytes, the spend is a fact about the network and no local
// row state may un-happen it. The two failures these tests pin are the two ways
// the wallet used to let a local opinion win:
//
//   - the guarded spend tolerated [utxostore.ReservedError] with HeldBy == "",
//     which is NOT "external input" — it is "in the inventory, currently
//     CLAIMABLE". One lost reservation therefore left an accepted
//     transaction's own input free for the funder to lend again. That is the
//     amplifier: every release race became a double spend the wallet authored.
//   - a missing transaction row made the apply skip the spend ENTIRELY while
//     still advancing the statuses past the broadcast stage, where no sweep
//     re-drives them. The spend was not deferred, it was lost.
//
// Both are now refusals, and a refusal is safe precisely because the known tx
// is still at 'sending' when it happens: FindResendable's graced arm re-drives
// it, arcade is idempotent by txid, and a fact-mode spend by the SAME spender is
// an idempotent success. Several tests below assert that convergence rather
// than only the refusal.

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// captureLogs redirects the provider's logger into a capture the caller can
// assert on. Tests are in-package, so this beats threading an option through
// the harness for one field.
func (h *harness) captureLogs() *logCapture {
	c := &logCapture{}
	h.p.logger = slog.New(c)
	return c
}

// stringAttrs flattens a record's string attributes into a name→value map (the
// string twin of intAttrs).
func stringAttrs(r slog.Record) map[string]string {
	out := make(map[string]string)
	r.Attrs(func(a slog.Attr) bool {
		if a.Value.Kind() == slog.KindString {
			out[a.Key] = a.Value.String()
		}
		return true
	})
	return out
}

// strandTheReservation reproduces a LOST reservation on a stored, unsent
// transaction: the coin is back in the claimable pool while broadcastable bytes
// that spend it still exist.
//
// It has to go through Unpin first. The pre-broadcast pin makes a reserved row
// invisible to every janitor, which is exactly the point of it — so the only
// remaining routes to this state are the ones production actually takes: a
// fence-first sweep that unpinned and released, or a crash between the two
// halves of an abort. Either way what the send path then meets is an unreserved,
// unspent, claimable coin.
func (h *harness) strandTheReservation(t *testing.T, reference string) {
	t.Helper()
	ctx := context.Background()
	unpinned, err := h.utxo.Unpin(ctx, int64(h.userID), reference)
	require.NoError(t, err)
	require.Equal(t, 1, unpinned, "precondition: the stored transaction pinned its input")
	freed, err := h.utxo.ReleaseReservation(ctx, int64(h.userID), reference)
	require.NoError(t, err)
	require.Equal(t, 1, freed, "precondition: the reservation was there to lose")
}

// TestAcceptedBroadcast_RecordsTheSpendOfAStrandedReservation is P0-2's caller
// half, end to end.
//
// The coin's reservation is lost before the send. Under the guarded spend the
// store answered ReservedError{HeldBy: ""}, the apply read that as "external
// input" and skipped it, and the transaction went out with its own funding coin
// left CLAIMABLE — so the next CreateAction could spend it a second time. Fact
// mode has no such answer: the only skip left is a row that does not exist.
func TestAcceptedBroadcast_RecordsTheSpendOfAStrandedReservation(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x71, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	h.strandTheReservation(t, res.Reference)

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Empty(t, u.ReservedBy, "precondition: the coin is claimable again")
	require.Nil(t, u.SpentBy)

	// Arcade accepts the transaction anyway — nothing about a local reservation
	// is visible to the network.
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))
	require.Equal(t, 1, h.oracle.calls)

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy, "an accepted broadcast must record the spend of its own input")
	assert.Equal(t, txid, u.SpentBy.String())

	// The property, not the column: the funder can no longer reach the coin.
	_, err = h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	assert.ErrorIs(t, err, funder.ErrNotEnoughFunds,
		"a recorded spend takes the coin out of the funder's reach; the old skip left it lendable")

	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status)
}

// TestAcceptedBroadcast_ExternalInputIsStillTolerated is the counterweight. The
// tolerance did not disappear, it narrowed to the ONE case that means what it
// says: no row at all. A transaction funded by somebody else's coin must still
// commit.
func TestAcceptedBroadcast_ExternalInputIsStillTolerated(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x72, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// Make the input external in the only way the store can express it.
	require.NoError(t, h.utxo.Remove(ctx, []utxostore.Outpoint{coin}, true))
	_, err = h.utxo.Get(ctx, coin)
	require.ErrorIs(t, err, &utxostore.NotFoundError{}, "precondition: the input is not ours")

	require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))
	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status,
		"a transaction spending an external input still commits")
}

// TestAcceptedBroadcast_InputSpentByAnotherTxIsAHardError draws the alert
// boundary. A stranded reservation is recoverable — fact mode records the spend
// and the coin is safe. Two RECORDED spends of one coin are not: they cannot
// both be true, nothing local can repair it, and overwriting the winner would
// destroy the only evidence. So the apply fails and says so at ERROR.
func TestAcceptedBroadcast_InputSpentByAnotherTxIsAHardError(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x73, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// A competitor gets there first — recorded as a FACT, which is the only way
	// a coin ends up spent by a transaction that never held its reservation.
	var winner chainhash.Hash
	winner[0] = 0xEE
	op := &utxostore.SpendOp{Outpoint: coin, Reservation: "a-competitor", SpendingTxID: winner}
	require.NoError(t, h.utxo.Spend(ctx, []*utxostore.SpendOp{op}, true))
	require.NoError(t, op.Err)

	logs := h.captureLogs()
	// The sweep never fails as a whole for one bad transaction; the evidence is
	// what it left behind.
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))

	rec, found := logs.find(slog.LevelError, "double spend")
	require.True(t, found, "a materialized double spend must be reported at ERROR")
	attrs := stringAttrs(rec)
	assert.Equal(t, txid, attrs["txid"])
	assert.Equal(t, winner.String(), attrs["winner"], "the log names the transaction that won the coin")

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy)
	assert.Equal(t, winner.String(), u.SpentBy.String(), "the recorded winner is never overwritten")

	kt := h.knownTx(t, txid)
	assert.Equal(t, wdk.ProvenTxStatusSending, kt.Status,
		"the apply failed whole: the row stays where the claim put it")
	assert.False(t, kt.WasBroadcast, "and is never stamped as broadcast evidence")
}

// contendedSpendStore fails the first `remaining` Spend calls with
// [utxostore.ErrContention] and then behaves normally.
//
// It carries both SHAPES the contract allows, because the backends do not agree
// on one: every current backend reports contention per item on SpendOp.Err under
// an ErrBatch top level, while the interface explicitly permits a whole-call
// failure that explains no item. A caller that handles only the first shape
// silently swallows the second — which is what the old loop did, falling out of
// its per-item walk and returning nil.
type contendedSpendStore struct {
	utxostore.Store
	remaining atomic.Int32
	wholeCall bool
}

func (c *contendedSpendStore) Spend(ctx context.Context, ops []*utxostore.SpendOp, force bool) error {
	if c.remaining.Add(-1) < 0 {
		return c.Store.Spend(ctx, ops, force)
	}
	if c.wholeCall {
		return fmt.Errorf("teststore: spend batch: %w", utxostore.ErrContention)
	}
	for _, op := range ops {
		op.Err = fmt.Errorf("teststore: spend %s: %w", op.Outpoint, utxostore.ErrContention)
	}
	return fmt.Errorf("%w: %d of %d items failed", utxostore.ErrBatch, len(ops), len(ops))
}

// TestAcceptedBroadcast_ContentionIsRetryableAndConverges pins the third
// verdict. Contention is neither a skip nor an alert: the store is saying "ask
// me again". The apply must fail (never commit statuses without the spend) in a
// way the send path can recover from — and recovery is not hypothetical here,
// it is driven and asserted.
func TestAcceptedBroadcast_ContentionIsRetryableAndConverges(t *testing.T) {
	for _, tc := range []struct {
		name      string
		wholeCall bool
	}{
		{"per-item verdict", false},
		{"whole-call failure", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			clk := newGateClock()
			h := newHarnessClock(t, clk.Now)
			res, signed, coin := h.createAndSign(t, 0x74, 100_000, 40_000)
			txid := signed.TxID().String()

			_, err := h.processDelayed(t, res, signed)
			require.NoError(t, err)

			contended := &contendedSpendStore{Store: h.utxo, wholeCall: tc.wholeCall}
			contended.remaining.Store(1)
			h.p.utxo = contended

			// Pass one: the store will not settle, so nothing commits.
			require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))
			require.Equal(t, 1, h.oracle.calls, "the bytes did reach arcade")

			u, err := h.utxo.Get(ctx, coin)
			require.NoError(t, err)
			require.Nil(t, u.SpentBy, "no spend was recorded")
			kt := h.knownTx(t, txid)
			require.Equal(t, wdk.ProvenTxStatusSending, kt.Status,
				"the row must stay on FindResendable's graced recovery arm")
			require.False(t, kt.WasBroadcast)

			// Pass two, one grace later: the graced 'sending' arm re-drives it, the
			// re-POST is idempotent at arcade, and the fact-mode spend settles.
			clk.Advance(resendGrace + time.Second)
			require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))

			u, err = h.utxo.Get(ctx, coin)
			require.NoError(t, err)
			require.NotNil(t, u.SpentBy, "the re-drive converges: contention is retryable, not terminal")
			assert.Equal(t, txid, u.SpentBy.String())
			assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status)
		})
	}
}

// partialThenFatalSpendStore reproduces the shape a batch produces when an
// EARLIER item resolved to a per-item verdict and the call then failed as a
// WHOLE: op[0] carries its Err, and the returned error is NOT
// [utxostore.ErrBatch].
//
// sqlstore does exactly this. Its items run inside one transaction, so a driver
// error on a later item makes withTx roll back the earlier writes and return
// the driver error raw — while the per-item Err set before the rollback
// survives in memory. Reading that residue as "one external input, carry on"
// commits the statuses over a spend the store threw away.
type partialThenFatalSpendStore struct {
	utxostore.Store
	fired atomic.Bool
}

var errDriverMidBatch = errors.New("teststore: driver failure mid-batch")

func (s *partialThenFatalSpendStore) Spend(ctx context.Context, ops []*utxostore.SpendOp, force bool) error {
	if s.fired.Swap(true) {
		return s.Store.Spend(ctx, ops, force)
	}
	// The item the store classified before it hit the wall.
	ops[0].Err = &utxostore.NotFoundError{Op: ops[0].Outpoint}
	// The wall. No ErrBatch: the per-item verdicts are NOT the whole story.
	return errDriverMidBatch
}

// TestAcceptedBroadcast_ToleratedItemsCannotExplainAWholeCallFailure closes the
// gap between "every failing item was skippable" and "the call succeeded".
//
// A NotFound item is tolerable ONLY when the store also said the per-item
// verdicts were the complete accounting — which is what ErrBatch means. Without
// that check, a batch holding one external input plus any non-item failure
// (driver error, closed store) reads as fully tolerated and returns nil, and
// the apply then commits the statuses: was_broadcast stamped sticky, the row
// past the broadcast stage where no sweep re-drives it, and the real inputs
// reserved-and-unspent forever. That is the P1-3 shape reached by a different
// road, and C6's caller-provided inputs widen the NotFound population that can
// trigger it.
func TestAcceptedBroadcast_ToleratedItemsCannotExplainAWholeCallFailure(t *testing.T) {
	ctx := context.Background()
	clk := newGateClock()
	h := newHarnessClock(t, clk.Now)
	res, signed, coin := h.createAndSign(t, 0x7B, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	h.p.utxo = &partialThenFatalSpendStore{Store: h.utxo}

	require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))
	require.Equal(t, 1, h.oracle.calls)

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Nil(t, u.SpentBy, "the store rolled the batch back; nothing was spent")
	assert.Equal(t, res.Reference, u.ReservedBy, "and the inputs are still held for the re-drive")

	kt := h.knownTx(t, txid)
	assert.Equal(t, wdk.ProvenTxStatusSending, kt.Status,
		"a whole-call failure must not be explained away by a tolerated item")
	assert.False(t, kt.WasBroadcast, "was_broadcast is sticky; stamping it here is unrecoverable")

	// The refusal is safe because it is re-drivable: one grace later the row is
	// picked up again and the (now healthy) store settles it.
	clk.Advance(resendGrace + time.Second)
	require.NoError(t, h.p.SendWaitingTransactions(ctx, 10))

	u, err = h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy, "the re-drive completes the apply")
	assert.Equal(t, txid, u.SpentBy.String())
	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status)
}

// TestAcceptedBroadcast_NoTransactionRowRefusesToCommit is P1-3.
//
// The old apply took a possibly-nil transaction row and, when it was nil,
// advanced the statuses and skipped the spend + spend-history entirely. That is
// the worst available outcome: known_txs moves to 'unconfirmed' (was_broadcast
// stamped sticky), which is beyond the broadcast stage and off every work list,
// while the inputs stay reserved-and-unspent forever. Nothing ever comes back
// for them.
//
// There is no legitimate flow that broadcasts bytes with no transaction row
// behind them, so this is corruption and the only safe answer is to refuse
// BEFORE writing anything.
func TestAcceptedBroadcast_NoTransactionRowRefusesToCommit(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x75, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	before := h.knownTx(t, txid)

	// Lose the row the apply needs (its FK children first).
	for _, stmt := range []string{
		"DELETE FROM transaction_labels",
		"DELETE FROM outputs",
		"DELETE FROM transactions",
	} {
		_, err = h.meta.DB().ExecContext(ctx, stmt)
		require.NoError(t, err, stmt)
	}

	err = h.p.applyAcceptedBroadcast(ctx, txid, signed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "has no transaction row")

	kt := h.knownTx(t, txid)
	assert.Equal(t, before.Status, kt.Status, "no status write may precede the refusal")
	assert.False(t, kt.WasBroadcast, "was_broadcast is sticky; stamping it here is unrecoverable")

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Nil(t, u.SpentBy)
	assert.Equal(t, res.Reference, u.ReservedBy, "the inputs are left exactly as the re-drive needs them")
}

// TestAcceptedBroadcast_TransactionRowQueryErrorPropagates is the other half of
// deleting firstTxByTxID's swallow. "The row could not be read" and "there is
// no row" were the same value — a nil pointer — so a transient database failure
// took the same branch as corruption, and that branch used to write statuses.
func TestAcceptedBroadcast_TransactionRowQueryErrorPropagates(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, coin := h.createAndSign(t, 0x76, 100_000, 40_000)
	txid := signed.TxID().String()

	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)
	before := h.knownTx(t, txid)

	// Make the read fail rather than answer empty.
	_, err = h.meta.DB().ExecContext(ctx, "ALTER TABLE transactions RENAME TO transactions_hidden")
	require.NoError(t, err)
	t.Cleanup(func() {
		_, _ = h.meta.DB().ExecContext(ctx, "ALTER TABLE transactions_hidden RENAME TO transactions")
	})

	err = h.p.applyAcceptedBroadcast(ctx, txid, signed)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "resolve transaction row")

	kt := h.knownTx(t, txid)
	assert.Equal(t, before.Status, kt.Status, "an unreadable row is not an absent one; write nothing")
	assert.False(t, kt.WasBroadcast)

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Nil(t, u.SpentBy)
}

// TestAcceptedBroadcast_ResolvesTheRowAcrossUsers is P1-3 reached through a
// real flow, and the reason the resolution moved INTO applyAcceptedBroadcast.
//
// commitAccepted used to look the row up USER-SCOPED, under whoever called
// ProcessAction. A SendWith carrying a txid whose row belongs to another user
// therefore found nothing, took the nil branch, and committed the statuses
// without ever spending the inputs — silently, with a success returned to the
// caller. The monitor works by txid and so must this: the row is resolved across
// users, and its user is the one the spend-history is recorded under.
func TestAcceptedBroadcast_ResolvesTheRowAcrossUsers(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	// Funded by an INTERNALIZED coin, not the bare mint the other tests use, so
	// the input carries a local output row and the spend-history half of the
	// apply is observable too.
	fundTxID := h.internalizeMinedPayment(t, 0x77, 100_000)
	fundHash, err := chainhash.NewHashFromHex(fundTxID)
	require.NoError(t, err)
	coin := utxostore.Outpoint{TxID: *fundHash, Vout: 0}

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	signed := buildSignedTx(t, res)
	txid := signed.TxID().String()
	_, err = h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// A DIFFERENT user drives the send.
	const otherIdentityKey = "0379be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"
	other, err := h.p.FindOrInsertUser(ctx, otherIdentityKey)
	require.NoError(t, err)
	require.True(t, other.IsNew)
	otherID := other.User.UserID
	require.NotEqual(t, h.userID, otherID)
	otherAuth := wdk.AuthID{IdentityKey: otherIdentityKey, UserID: &otherID}

	out, err := h.p.ProcessAction(ctx, otherAuth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	require.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy, "the owner's input must be spent even when another user sent the bytes")
	assert.Equal(t, txid, u.SpentBy.String())

	// The spend-history is recorded under the ROW's user, not the caller's — the
	// old code passed the caller's id straight into the (user-scoped) lookup.
	rows, err := h.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
		UserID: &h.userID,
		TxID:   &fundTxID,
	})
	require.NoError(t, err)
	require.Len(t, rows, 1)
	assert.NotNil(t, rows[0].SpentBy, "the input's local output row records its spender")
}

// --- SEEN on a fenced row --------------------------------------------------

// TestApplySeen_RefusesToAdvanceAFencedRow: a SEEN is arcade's word, and the two
// statuses below are states in which the wallet has ALREADY acted on the
// transaction being dead. Advancing off them would contradict a decision already
// applied to the coins, so the event is recorded and no wallet state moves.
//
// This is the irreducible race — an abort cannot recall bytes the network has —
// and it is reported at ERROR because the aborted transaction's coins went back
// to the funder when the fence went up.
func TestApplySeen_RefusesToAdvanceAFencedRow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status wdk.ProvenTxReqStatus
		fence  func(t *testing.T, h *harness, res *wdk.StorageCreateActionResult, txid string)
	}{
		{
			name:   "aborted",
			status: metastore.KnownTxStatusAborted,
			fence: func(t *testing.T, h *harness, res *wdk.StorageCreateActionResult, _ string) {
				t.Helper()
				abr, err := h.p.AbortAction(context.Background(), h.auth, wdk.AbortActionArgs{
					Reference: primitives.Base64String(res.Reference),
				})
				require.NoError(t, err)
				require.True(t, abr.Aborted)
			},
		},
		{
			name:   "stuck",
			status: metastore.KnownTxStatusStuck,
			fence: func(t *testing.T, h *harness, _ *wdk.StorageCreateActionResult, txid string) {
				t.Helper()
				ctx := context.Background()
				require.NoError(t, h.meta.KnownTx().MarkSuspect(ctx, txid, h.p.now(), nil, "REJECTED", "gone"))
				require.NoError(t, h.meta.KnownTx().TransitionSuspect(ctx, txid, metastore.KnownTxStatusStuck))
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			for _, apply := range []struct {
				name string
				run  func(h *harness, rec arcade.TxRecord) error
			}{
				{"single", func(h *harness, rec arcade.TxRecord) error {
					return h.p.ApplyStatusUpdate(context.Background(), rec)
				}},
				{"batch", func(h *harness, rec arcade.TxRecord) error {
					return h.p.ApplyStatusBatch(context.Background(), []arcade.TxRecord{rec})
				}},
			} {
				t.Run(apply.name, func(t *testing.T) {
					ctx := context.Background()
					h := newHarness(t)
					res, signed, coin := h.createAndSign(t, 0x78, 100_000, 40_000)
					txid := signed.TxID().String()
					_, err := h.processDelayed(t, res, signed)
					require.NoError(t, err)
					chOp := changeOutpoint(t, res, txid)

					tc.fence(t, h, res, txid)
					require.Equal(t, tc.status, h.knownTx(t, txid).Status)

					logs := h.captureLogs()
					require.NoError(t, apply.run(h, arcade.TxRecord{TxID: txid, Status: arcade.StatusSeenOnNetwork}))

					kt := h.knownTx(t, txid)
					assert.Equal(t, tc.status, kt.Status, "a SEEN must not walk a fenced row forward")
					assert.False(t, kt.WasBroadcast, "and must not stamp broadcast evidence on it")
					require.NotNil(t, kt.ArcadeStatus, "what arcade said is still recorded for the operator")
					assert.Equal(t, string(arcade.StatusSeenOnNetwork), *kt.ArcadeStatus)

					_, found := logs.find(slog.LevelError, "fenced transaction on the network")
					assert.True(t, found, "the irreducible race must be reported at ERROR")

					// No coin moved either: an aborted transaction's change is gone,
					// a stuck one's must not become spendable.
					if u, gerr := h.utxo.Get(ctx, chOp); gerr == nil {
						assert.NotEqual(t, utxostore.TierUnproven, u.Tier,
							"a fenced transaction's change is never promoted to claimable")
					}
					if u, gerr := h.utxo.Get(ctx, coin); gerr == nil {
						assert.Nil(t, u.SpentBy)
					}
				})
			}
		})
	}
}

// TestApplySeen_StillAdvancesASuspectFailedRow is the constraint that decided
// the fenced set, and it is a dependency, not a preference.
//
// The reject→release reconciler's false-positive branch recovers a suspect by
// routing arcade's recovered record straight back through ApplyStatusUpdate —
// [Provider.handleRecovered] — and relies on the SEEN arm to advance the row to
// unconfirmed. Adding 'suspectFailed' to the fence would strand every false
// positive the reconciler exists to rescue, so it is deliberately absent and
// this test is what keeps it absent.
func TestApplySeen_StillAdvancesASuspectFailedRow(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x79, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	require.NoError(t, h.meta.KnownTx().MarkSuspect(ctx, txid, h.p.now(), nil, "REJECTED", "transient"))
	require.Equal(t, metastore.KnownTxStatusSuspectFailed, h.knownTx(t, txid).Status)

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusSeenOnNetwork}))
	assert.Equal(t, wdk.ProvenTxStatusUnconfirmed, h.knownTx(t, txid).Status,
		"a false-positive suspect must still be recoverable by a SEEN")
}

// TestApplySeenBatch_FenceSurvivesAnAbortRacingTheApply is why the widened skip
// set exists on top of the classifier check.
//
// Both classifiers read the known-tx row OUTSIDE the write transaction, so an
// abort landing between that read and the write is an ordinary concurrent
// event, not an exotic one. Only a predicate carried IN the write can refuse it,
// which is what seenAdvanceSkipStatuses is for. Driving applySeenBatch directly
// is the race, held still.
func TestApplySeenBatch_FenceSurvivesAnAbortRacingTheApply(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x7A, 100_000, 40_000)
	txid := signed.TxID().String()
	_, err := h.processDelayed(t, res, signed)
	require.NoError(t, err)

	// The classifier's read happened; now the abort lands.
	abr, err := h.p.AbortAction(ctx, h.auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.NoError(t, err)
	require.True(t, abr.Aborted)

	// The applier runs with the stale classification.
	require.NoError(t, h.meta.Do(ctx, func(ctx context.Context) error {
		return h.p.applySeenBatch(ctx, []arcade.TxRecord{{TxID: txid, Status: arcade.StatusSeenOnNetwork}})
	}))

	assert.Equal(t, metastore.KnownTxStatusAborted, h.knownTx(t, txid).Status,
		"the guard in the write is the one that survives the race")
}
