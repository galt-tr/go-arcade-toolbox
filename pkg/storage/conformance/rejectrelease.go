package conformance

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// Grace and quarantine bounds for the reconciler passes these subtests drive.
// Short, because the clock is manual — the point is to cross the boundary, not
// to wait at it.
const (
	rrGrace = time.Minute
	rrMaxQ  = time.Hour
)

// reconcilable is the reject->release seam. It is declared here, structurally,
// rather than imported from pkg/monitor: monitor.MonitoredStorage is the same
// shape, but pkg/monitor documents at interface.go:27 that the assertion binding
// it to *storage.Provider deliberately lives in tests to avoid a production
// storage->monitor import, and this package should not reintroduce that edge for
// two method signatures.
//
// A provider that cannot reconcile does not satisfy it and the subtest skips
// rather than fails. That is the honest outcome for the REST Client in
// particular: reconciliation is not a wire operation — /storage/v1 has no route
// for it — because the monitor runs in-process alongside the Provider.
type reconcilable interface {
	ApplyStatusUpdate(ctx context.Context, rec arcade.TxRecord) error
	VerifyAndReleaseSuspects(
		ctx context.Context, grace, maxQuarantine time.Duration, limit int,
	) (defs.ReconcilerReport, error)
}

// rejectRelease asserts the property the swimlanes call "marking the spend
// available again on a failed broadcast", for any backend.
//
// It was previously proven for exactly one: the hermetic
// reject_respend_test.go covers memstore+SQLite, and reconciler_integration_test
// covers PostgreSQL but hand-seeds its rows. Since the suite never carried the
// property, a new storage backend could pass conformance in full while never
// releasing a rejected transaction's inputs — the failure mode being a wallet
// that silently loses a coin on every rejection.
//
// The two halves are inseparable and both are asserted here. Releasing too
// eagerly hands out a coin whose transaction the network is about to accept;
// never releasing leaks the coin permanently. The reconciler's answer to both is
// the two-pass guard, so the test drives two passes across a grace boundary.
func (s *suite) rejectRelease(t *testing.T) {
	if s.cfg.rrEnv == nil {
		t.Skip("backend supplied no RejectReleaseEnv (see conformance.WithRejectReleaseEnv); " +
			"reject->release is NOT covered for this backend")
	}

	t.Run("released once the rejection is verified", func(t *testing.T) {
		ctx := context.Background()
		env := s.freshEnv(t)
		rec := s.requireReconcilable(t, env)

		auth := s.newAuth(t, env.Provider, NewIdentityKey(t))

		// Exactly one candidate coin, deliberately: given a choice, the retry
		// below could fund from a different coin and satisfy the final
		// assertion without the rejected one ever coming back.
		fundTxID := rrFund(t, env.Provider, auth, 0x41, 60_000)

		first, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(40_000))
		require.NoError(t, err)
		require.NotEmpty(t, first.Inputs, "the action funded from nothing")
		claimed := rrOutpoints(first)

		txid := rrProcess(t, env.Provider, auth, first)

		// Arcade rejects. Phase one holds the inputs on purpose: a 4xx is not
		// proof the transaction is dead.
		env.Oracle.ScriptTx(txid, arcade.StatusRejected,
			WithExtraInfo("PROCESSING (4): failed to validate transaction"))
		require.NoError(t, rec.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))

		assert.False(t, rrSpendable(t, env.Provider, auth, fundTxID, 0),
			"the input was released the instant a rejection arrived. Release is the "+
				"reconciler's job precisely because a REJECTED verdict can be revised, "+
				"and releasing on the first one races that revision")

		// Two verified passes, a grace apart.
		total := rrReconcile(ctx, t, rec, env.Advance)
		assert.Equal(t, 1, total.Released,
			"the reconciler did not report releasing the rejected transaction; "+
				"got %+v", total)

		// The assertion that matters: spendable in practice, not merely in the
		// utxostore's opinion of itself.
		second, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(40_000))
		require.NoError(t, err,
			"a rejected action's inputs never came back. An application's only "+
				"recovery from a rejection is to build again, so a coin still "+
				"reserved after its transaction is provably dead is a coin lost "+
				"for good")
		require.NotEmpty(t, second.Inputs)

		assert.ElementsMatch(t, claimed, rrOutpoints(second),
			"the retry funded from different coins, so this passed without ever "+
				"proving the rejected one came back")
	})

	t.Run("recovered rejection releases nothing", func(t *testing.T) {
		ctx := context.Background()
		env := s.freshEnv(t)
		rec := s.requireReconcilable(t, env)

		auth := s.newAuth(t, env.Provider, NewIdentityKey(t))
		fundTxID := rrFund(t, env.Provider, auth, 0x42, 60_000)

		res, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(40_000))
		require.NoError(t, err)
		txid := rrProcess(t, env.Provider, auth, res)

		require.NoError(t, rec.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))

		// Arcade revises its verdict, which its own status lifecycle documents
		// as possible: SEEN_ON_NETWORK recovers a REJECTED transaction.
		env.Oracle.ScriptTx(txid, arcade.StatusSeenOnNetwork)

		total := rrReconcile(ctx, t, rec, env.Advance)
		assert.Zero(t, total.Released,
			"the reconciler released inputs for a transaction the network went on "+
				"to accept; got %+v", total)
		assert.Positive(t, total.FalsePositive,
			"the recovery was never recognized as a false positive, so this passed "+
				"for the wrong reason — the transaction may simply have been left "+
				"ambiguous rather than routed back into the normal lifecycle; got %+v",
			total)

		assert.False(t, rrSpendable(t, env.Provider, auth, fundTxID, 0),
			"an input was released for a transaction the network went on to ACCEPT. "+
				"That coin is spent on chain; handing it back to the funder produces "+
				"the double spend this wallet exists to prevent")
	})
}

// requireReconcilable asserts the env's provider can drive the reconciler, or
// skips. Kept separate from the nil-env check so the two reasons a backend opts
// out read differently in the log.
func (s *suite) requireReconcilable(t *testing.T, env RejectReleaseEnv) reconcilable {
	t.Helper()
	rec, ok := env.Provider.(reconcilable)
	if !ok {
		t.Skipf("provider %T cannot reconcile (no ApplyStatusUpdate/VerifyAndReleaseSuspects); "+
			"reject->release is NOT covered for this backend", env.Provider)
	}
	return rec
}

// rrReconcile runs the two verified passes the release path requires, crossing
// the grace boundary before each, and returns their combined report.
//
// Two passes is the contract, not a retry: the first stamps verified_rejected_at
// and the second confirms the verdict survived the window. A single pass must
// never release, which is what makes a transient rejection safe.
func rrReconcile(
	ctx context.Context, t *testing.T, rec reconcilable, advance func(time.Duration),
) defs.ReconcilerReport {
	t.Helper()
	var total defs.ReconcilerReport
	for range 2 {
		advance(rrGrace + time.Second)
		r, err := rec.VerifyAndReleaseSuspects(ctx, rrGrace, rrMaxQ, 128)
		require.NoError(t, err)
		total.Add(r)
	}
	return total
}

// rrFund internalizes exactly one mined coin and returns its txid. seed varies
// per subtest so two subtests sharing a backend never collide on an outpoint.
func rrFund(
	t *testing.T, p wdk.WalletStorageProvider, auth wdk.AuthID, seed byte, sats uint64,
) string {
	t.Helper()
	atomicBEEF, txid := BuildMinedAtomicBEEF(t, seed, 900_100, sats)
	_, err := p.InternalizeAction(context.Background(), auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomicBEEF),
		Outputs: []*wdk.InternalizeOutput{WalletPaymentOutput(0, NewIdentityKey(t))},
	})
	require.NoError(t, err)
	return txid
}

// rrProcess signs and processes res, returning the txid signing fixed.
func rrProcess(
	t *testing.T, p wdk.WalletStorageProvider, auth wdk.AuthID, res *wdk.StorageCreateActionResult,
) string {
	t.Helper()
	tx := BuildSignedTx(t, res)
	txid := tx.TxID().String()
	ref := res.Reference
	id := primitives.TXIDHexString(txid)
	_, err := p.ProcessAction(context.Background(), auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: &ref,
		TxID:      &id,
		RawTx:     primitives.ExplicitByteArray(tx.Bytes()),
	})
	require.NoError(t, err)
	return txid
}

func rrOutpoints(res *wdk.StorageCreateActionResult) []string {
	out := make([]string, 0, len(res.Inputs))
	for _, in := range res.Inputs {
		out = append(out, fmt.Sprintf("%s.%d", in.SourceTxID, in.SourceVout))
	}
	return out
}

// rrSpendable reports whether one outpoint is currently claimable, as the funder
// would see it — through the exported interface, not the utxostore directly, so
// the assertion matches what an application observes.
func rrSpendable(
	t *testing.T, p wdk.WalletStorageProvider, auth wdk.AuthID, txid string, vout uint32,
) bool {
	t.Helper()
	rows, err := p.FindOutputsAuth(context.Background(), auth,
		wdk.FindOutputsArgs{TxID: &txid, Vout: &vout})
	require.NoError(t, err)
	require.Len(t, rows, 1, "expected exactly one output row for %s:%d", txid, vout)
	return rows[0].Spendable
}
