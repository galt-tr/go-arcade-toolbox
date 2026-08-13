package storage_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// errBalance is the failure a slow Balance surfaces once its budget is enforced.
var errBalance = errors.New("balance: context deadline exceeded")

// failingBalanceStore wraps a real store and fails only Balance, which is the
// live failure shape: Balance walks a secondary index (~300k entries per bval
// against a ~900k-coin fuel basket) while every other read is an indexed count,
// so under load it is the one component that blows a caller's budget.
type failingBalanceStore struct {
	utxostore.Store
	basket string
}

func (f failingBalanceStore) Balance(ctx context.Context, userID int64, basket string) (utxostore.Balance, error) {
	if basket == f.basket {
		return utxostore.Balance{}, errBalance
	}
	return f.Store.Balance(ctx, userID, basket)
}

// TestStateReport_DegradesPerComponent pins the contract that a StateReport
// component which cannot be read is omitted and named, while the others are
// still collected.
//
// The regression this guards is not cosmetic. StateReport used to return
// (nil, err) the moment any basket's Balance failed, before it ever reached the
// status rollups. Callers poll this for a dashboard and carry the previous
// snapshot forward on error, so one expensive basket froze EVERY number —
// silently. Observed on a live 1000-TPS run: the reported transaction total sat
// at 2,115,335 for 62 seconds while the database climbed to 2,174,414 (a gap of
// 59,079), then snapped to the true value the instant the load stopped. It read
// as "status events stop arriving during a blast" when they had in fact been
// applying continuously at p50 72ms the whole time.
func TestStateReport_DegradesPerComponent(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	// Sanity: undegraded, everything present.
	rep, err := s.provider.StateReport(ctx, wdk.AuthID{IdentityKey: s.recipientHex}, []string{"default"})
	require.NoError(t, err)
	require.Empty(t, rep.Degraded, "a healthy report reports nothing degraded")
	require.NotNil(t, rep.ArcadeStatuses)

	// Now make the expensive component fail, exactly as a blown budget does.
	degraded := newE2EStackWithUTXO(t, func(base utxostore.Store) utxostore.Store {
		return failingBalanceStore{Store: base, basket: "fuel"}
	})
	degraded.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)
	rep, err = degraded.provider.StateReport(ctx,
		wdk.AuthID{IdentityKey: degraded.recipientHex}, []string{"default", "fuel"})

	// The whole call must NOT fail: that is the bug.
	require.NoError(t, err, "one failing component must not fail the whole report")

	// The cheap components are still collected — this is what froze before.
	require.NotNil(t, rep.ArcadeStatuses, "arcade statuses must still be read when a balance fails")
	require.NotNil(t, rep.TxStatuses, "tx statuses must still be read when a balance fails")

	// The failure is named, not silent.
	require.Len(t, rep.Degraded, 1)
	require.Equal(t, "balance", rep.Degraded[0].Component)
	require.Equal(t, "fuel", rep.Degraded[0].Basket)
	require.Contains(t, rep.Degraded[0].Error, "deadline exceeded")

	// The failing basket is omitted, never zero-filled: a fuel basket reported
	// as empty is indistinguishable from "we ran out of fuel" and is worse than
	// an acknowledged gap.
	for _, b := range rep.Baskets {
		require.NotEqual(t, "fuel", b.Basket, "a failed basket is omitted, not reported as zero")
	}
	require.Len(t, rep.Baskets, 1, "the healthy basket is still reported")
	require.Equal(t, "default", rep.Baskets[0].Basket)
}
