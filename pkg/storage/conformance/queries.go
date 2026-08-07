package conformance

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// internalizeMinedPayment internalizes a single mined P2PKH wallet-payment
// output (vout 0) for auth and returns its txid.
func internalizeMinedPayment(t *testing.T, p *storage.Provider, auth wdk.AuthID, senderIdentityKey string, seed byte, sats uint64) string {
	t.Helper()
	atomic, txid := BuildMinedAtomicBEEF(t, seed, 800_000+uint32(seed), sats)
	_, err := p.InternalizeAction(context.Background(), auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{WalletPaymentOutput(0, senderIdentityKey)},
	})
	require.NoError(t, err)
	return txid
}

// outputRow returns the descriptive output row for (txid, vout) via
// FindOutputsAuth (scoped to auth's user), failing the test if it is not
// found or is ambiguous.
func outputRow(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string, vout uint32) wdk.TableOutput {
	t.Helper()
	rows, err := p.FindOutputsAuth(context.Background(), auth, wdk.FindOutputsArgs{TxID: &txid, Vout: &vout})
	require.NoError(t, err)
	require.Len(t, rows, 1, "expected exactly one output row for %s:%d", txid, vout)
	return rows[0]
}

// assertSpendable asserts (txid, vout)'s live spendability as observed
// through FindOutputsAuth.
func assertSpendable(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string, vout uint32, want bool, msgAndArgs ...any) {
	t.Helper()
	row := outputRow(t, p, auth, txid, vout)
	assert.Equal(t, want, row.Spendable, msgAndArgs...)
}

// txStatusByReference finds a user's [wdk.CurrentTxStatus] by its Reference
// (the token CreateAction/ProcessAction/AbortAction key off), scanning the
// user's transaction list — [wdk.ListTransactionsArgs] has no reference
// filter, and a user may have other transactions (e.g. the incoming
// transaction InternalizeAction itself records for a seeded payment).
func txStatusByReference(t *testing.T, p *storage.Provider, auth wdk.AuthID, reference string) wdk.CurrentTxStatus {
	t.Helper()
	res, err := p.ListTransactions(context.Background(), auth, wdk.ListTransactionsArgs{Limit: 10000})
	require.NoError(t, err)
	for _, tx := range res.Transactions {
		if tx.Reference == reference {
			return tx
		}
	}
	require.Fail(t, "no transaction found for reference", reference)
	return wdk.CurrentTxStatus{}
}

func uint32Ptr(v uint32) *uint32 { return &v }
