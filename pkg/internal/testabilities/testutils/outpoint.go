package testutils

import (
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/require"
)

// SdkOutpoint is ported from go-wallet-toolbox (see upstream docs).
func SdkOutpoint(t testing.TB, strOutpoint string) *transaction.Outpoint {
	t.Helper()
	outpoint, err := transaction.OutpointFromString(strOutpoint)
	require.NoError(t, err)
	return outpoint
}
