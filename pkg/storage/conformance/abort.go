package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// abortRestores: CreateAction then AbortAction releases the funding
// reservation, removes any minted change, restores the funding coin as
// claimable again, and marks the transaction aborted — both for a
// never-processed (unsigned) action and for a noSend-processed one (which
// mints change before the abort).
func (s *suite) abortRestores(t *testing.T) {
	t.Run("Unsigned", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		fundTxID := internalizeMinedPayment(t, p, auth, sender, 0x40, 60_000)

		res, err := p.CreateAction(ctx, auth, PaymentArgs(20_000))
		require.NoError(t, err)
		assertSpendable(t, p, auth, fundTxID, 0, false, "funding coin reserved before abort")

		abr, err := p.AbortAction(ctx, auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
		require.NoError(t, err)
		assert.True(t, abr.Aborted)

		assertSpendable(t, p, auth, fundTxID, 0, true, "funding coin released on abort")
		status := txStatusByReference(t, p, auth, res.Reference)
		assert.Equal(t, wdk.TxUpdateStatusAborted, status.Status)
	})

	t.Run("NoSendRestores", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		fundTxID := internalizeMinedPayment(t, p, auth, sender, 0x41, 80_000)

		res, err := p.CreateAction(ctx, auth, PaymentArgs(30_000))
		require.NoError(t, err)

		signed := BuildSignedTx(t, res)
		txid := signed.TxID().String()
		_, err = p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
			IsNewTx:   true,
			IsNoSend:  true,
			Reference: strPtr(res.Reference),
			TxID:      txidPtr(txid),
			RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
		})
		require.NoError(t, err)

		// Change minted; funding coin still reserved (noSend never spends).
		changeTxID, changeVout := ChangeOutpoint(t, res, txid)
		assertSpendable(t, p, auth, fundTxID, 0, false, "funding coin reserved pre-abort")

		abr, err := p.AbortAction(ctx, auth, wdk.AbortActionArgs{Reference: primitives.Base64String(res.Reference)})
		require.NoError(t, err)
		assert.True(t, abr.Aborted)

		assertSpendable(t, p, auth, fundTxID, 0, true, "funding coin restored on abort")
		assertSpendable(t, p, auth, changeTxID, changeVout, false, "minted change is no longer a live coin after abort")
	})
}

// sendSweeper is the delayed-send sweep, declared structurally for the same
// reason [reconcilable] is: the sweep is driven by the in-process monitor, not
// through [wdk.WalletStorageProvider], and this package must not grow an import
// edge to pkg/monitor for one method. A provider that cannot sweep (the REST
// Client: /storage/v1 has no route for it) skips rather than fails.
type sendSweeper interface {
	SendWaitingTransactions(ctx context.Context, limit int) error
}

// abortFence is the audit's P0-3 acceptance test, for any backend.
//
// Aborting used to be a purely wallet-side act: it released the funding
// reservation and marked the transactions row 'aborted' without touching
// known_txs. known_txs is the ONLY table the delayed-send sweep reads, so the
// signed bytes of an aborted transaction stayed on its work list — and the
// sweep would go on to broadcast a transaction whose inputs the wallet had
// already lent to someone else. A wallet that double-spends its own coins is
// the worst thing this storage layer can do, and no backend may be conformant
// while it can.
//
// So the property is not "the status says aborted". It is that NO BYTES LEAVE,
// through any of the three doors — the sweep, an explicit SendWith re-drive,
// and a re-drive after the coin has been handed to a different action — and
// that the coin is genuinely re-usable rather than merely marked free.
func (s *suite) abortFence(t *testing.T) {
	if s.cfg.rrEnv == nil {
		t.Skip("backend supplied no RejectReleaseEnv (see conformance.WithRejectReleaseEnv); " +
			"the abort fence is NOT covered for this backend — its oracle and clock are " +
			"unreachable, and an abort test that cannot count broadcasts proves nothing")
	}
	ctx := context.Background()
	env := s.freshEnv(t)
	sweep, ok := env.Provider.(sendSweeper)
	if !ok {
		t.Skipf("provider %T cannot drive the delayed-send sweep (no SendWaitingTransactions); "+
			"the abort fence is NOT covered for this backend", env.Provider)
	}

	auth := s.newAuth(t, env.Provider, NewIdentityKey(t))

	// Exactly one candidate coin: with a choice, the re-claim at the end could
	// fund from a different coin and pass without the aborted one coming back.
	rrFund(t, env.Provider, auth, 0x51, 60_000)

	res, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(30_000))
	require.NoError(t, err)
	require.NotEmpty(t, res.Inputs, "the action funded from nothing")
	claimed := rrOutpoints(res)

	// DELAYED, which is the case with teeth: the transaction is signed, its raw
	// bytes are stored, and it is deliberately parked for the sweep to send
	// later. Everything needed for the broadcast is already durable when the
	// abort arrives.
	signed := BuildSignedTx(t, res)
	txid := signed.TxID().String()
	ref := res.Reference
	id := primitives.TXIDHexString(txid)
	_, err = env.Provider.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsDelayed: true,
		Reference: &ref,
		TxID:      &id,
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	before := env.Oracle.Calls()

	abr, err := env.Provider.AbortAction(ctx, auth, wdk.AbortActionArgs{
		Reference: primitives.Base64String(res.Reference),
	})
	require.NoError(t, err)
	require.True(t, abr.Aborted)

	// Door one: the sweep, at any age. The delayed queue is ungraced, so this
	// would fire on the very next tick; the advance is here to prove the row is
	// not merely waiting out a grace it will later cross.
	env.Advance(time.Hour)
	require.NoError(t, sweep.SendWaitingTransactions(ctx, 128))
	require.Equal(t, before, env.Oracle.Calls(),
		"the send sweep broadcast a transaction the wallet had aborted and refunded. "+
			"Its work list is keyed on known_txs alone, so an abort that does not fence "+
			"that row fences nothing")

	// Door two: an explicit re-drive naming the aborted transaction. It must be
	// refused, and the refusal must say which state refused it — "aborted" and
	// "already mined" call for opposite responses from the caller.
	_, err = env.Provider.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.Error(t, err, "an aborted transaction must not be broadcastable on request either")
	assert.Contains(t, err.Error(), "aborted", "the refusal names the state that refused")
	require.Equal(t, before, env.Oracle.Calls(), "and it refused BEFORE the POST, not after")

	// The coin is genuinely back: spendable in practice, not merely in the
	// store's opinion of itself.
	second, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(30_000))
	require.NoError(t, err,
		"an aborted action's inputs never came back; the coin is lost for good")
	require.NotEmpty(t, second.Inputs)
	assert.ElementsMatch(t, claimed, rrOutpoints(second),
		"the second action funded from different coins, so this passed without ever "+
			"proving the aborted one came back")

	// Door three: the same re-drive once the coin belongs to someone else. This
	// is the double spend in its literal form — two live claims on one outpoint
	// — and it is the last thing the fence has to hold against.
	_, err = env.Provider.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
		SendWith: []primitives.TXIDHexString{primitives.TXIDHexString(txid)},
	})
	require.Error(t, err)
	require.NoError(t, sweep.SendWaitingTransactions(ctx, 128))
	assert.Equal(t, before, env.Oracle.Calls(),
		"the wallet broadcast a transaction spending a coin it had already lent to "+
			"another action — a double spend of its own making")
}
