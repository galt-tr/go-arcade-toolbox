//go:build integration

package register_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	_ "github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/aerostore/register"
)

// TestRegister_OpenViaDSN proves the blank-import factory path: after importing
// the register package, utxostore.Open dispatches an aerospike:// DSN to the
// Aerospike provider and returns a working store.
func TestRegister_OpenViaDSN(t *testing.T) {
	ctr := testenv.StartAerospike(t)
	ctx := context.Background()

	dsn := fmt.Sprintf("aerospike://%s:%d/%s?set=regtest", ctr.Host(), ctr.Port(), ctr.Namespace())
	store, err := utxostore.Open(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(func() { _ = store.Close(ctx) })

	status, _, err := store.Health(ctx, true)
	require.NoError(t, err)
	require.Equal(t, 200, status)

	// Smoke test a round trip through the interface returned by the factory.
	op := utxostore.Outpoint{Vout: 0}
	op.TxID[0] = 0x7a
	m := &utxostore.Mint{Outpoint: op, UserID: 1, Basket: "default", Satoshis: 1000, InputSize: 107, Tier: utxostore.TierMined}
	require.NoError(t, store.Mint(ctx, []*utxostore.Mint{m}))
	require.NoError(t, m.Err)

	got, err := store.Get(ctx, op)
	require.NoError(t, err)
	require.Equal(t, uint64(1000), got.Satoshis)
}
