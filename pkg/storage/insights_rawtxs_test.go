package storage_test

import (
	"context"
	"strings"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/fixtures"
)

// TestRawTxs_ReturnsTheBroadcastBytes pins what Provider.RawTxs is for: a caller
// holding nothing but a txid gets back bytes that hash to it.
//
// That property is the whole value. A blob keyed by txid cannot go stale — it
// either hashes to the txid or it does not — so a caller that has recorded "this
// outpoint is mine" somewhere else can reconstruct the transaction to spend it
// without keeping a second, mutable copy of the bytes that can drift.
func TestRawTxs_ReturnsTheBroadcastBytes(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "raw txs payment"
		a.Labels = []string{"rawtxs"}
		a.Outputs[0].Satoshis = e2ePaymentAmount
	})
	res, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err)
	require.NotNil(t, res.Txid)
	txid := res.Txid.String()

	raws, err := s.provider.RawTxs(ctx, []string{txid})
	require.NoError(t, err)
	require.Contains(t, raws, txid, "a broadcast transaction must have its raw bytes retained")

	tx, err := transaction.NewTransactionFromBytes(raws[txid])
	require.NoError(t, err, "the returned bytes must parse as a transaction")
	require.Equal(t, txid, tx.TxID().String(), "the returned bytes must hash to the requested txid")
}

// A txid the wallet has never seen is simply absent. It is not an error and not
// a nil entry: the provider cannot know whether a miss means "not ours", "not
// retained" or "wrong network", so it must not pretend to.
func TestRawTxs_UnknownTxIDIsAbsentNotAnError(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)

	unknown := strings.Repeat("ab", 32)
	raws, err := s.provider.RawTxs(ctx, []string{unknown})
	require.NoError(t, err, "an unknown txid must not fail the whole batch")
	require.NotContains(t, raws, unknown)

	// And an empty request is a no-op rather than a malformed IN () query.
	raws, err = s.provider.RawTxs(ctx, nil)
	require.NoError(t, err)
	require.Empty(t, raws)
}
