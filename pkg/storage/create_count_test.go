package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// seedChangeBasketOutputs inserts n descriptive output rows into the provider's
// change basket for the harness user (one transaction row + n outputs), so
// changeBasketCount has something to count.
func (h *spyHarness) seedChangeBasketOutputs(t *testing.T, n int) {
	t.Helper()
	ctx := context.Background()
	txID, err := h.meta.Transactions().Insert(ctx, metastore.NewTx{
		UserID: h.userID, Status: wdk.TxStatusCompleted, Reference: "seed-change-ref",
	})
	require.NoError(t, err)
	basket := wdk.BasketNameForChange
	for i := 0; i < n; i++ {
		_, err := h.meta.Outputs().Insert(ctx, metastore.NewOutput{
			UserID: h.userID, TransactionID: txID, Vout: uint32(i), //nolint:gosec // small test index
			Satoshis: 1000, Basket: &basket, Change: true, Type: "P2PKH",
		})
		require.NoError(t, err)
	}
}

// TestChangeBasketCount_PrivacyCountsForClamp proves the default (privacy)
// create hot path STILL counts the change basket, so the funder's change-output
// clamp (NumberOfDesiredUTXOs − ExistingBasketCount, exercised by
// funder.TestFund_ExistingBasketCountClamp) keeps receiving a real count. This
// is the correctness guard for Task 28: the COUNT optimization must not weaken
// tiered change-clamping.
func TestChangeBasketCount_PrivacyCountsForClamp(t *testing.T) {
	ctx := context.Background()
	h := newSpyHarness(t) // default: privacy strategy
	h.seedChangeBasketOutputs(t, 4)

	n, err := h.p.changeBasketCount(ctx, h.userID)
	require.NoError(t, err)
	require.EqualValues(t, 4, n, "privacy path must count the change basket so the funder clamp is fed")
}

// TestChangeBasketCount_ThroughputSkips proves the throughput (fuel-pool)
// create hot path SKIPS the per-op COUNT entirely (returns 0 = do-not-clamp),
// even when the change basket is non-empty. Removing this count is the Task-28
// win: on the throughput path the funding pool lives in the change basket, so
// the old COUNT scaled with pool size on every op.
func TestChangeBasketCount_ThroughputSkips(t *testing.T) {
	ctx := context.Background()
	h := newSpyHarness(t, WithUTXOManagement(throughputUTXOMgmt(100_000)))
	h.seedChangeBasketOutputs(t, 4)

	n, err := h.p.changeBasketCount(ctx, h.userID)
	require.NoError(t, err)
	require.Zero(t, n, "throughput path must skip the change-basket count (returns 0)")
}
