package testutils

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/require"
)

// BEEFFromBytes is ported from go-wallet-toolbox (see upstream docs).
func BEEFFromBytes(t testing.TB, beefBytes []byte) *transaction.Beef {
	t.Helper()

	beef, err := transaction.NewBeefFromBytes(beefBytes)
	require.NoError(t, err)

	return beef
}
