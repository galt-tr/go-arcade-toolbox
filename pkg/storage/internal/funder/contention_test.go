package funder_test

import (
	"context"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

func contentionFunder(t *testing.T, store utxostore.Store, backoffs *atomic.Int64) *funder.Funder {
	t.Helper()
	backoff := func(context.Context, int) {
		if backoffs != nil {
			backoffs.Add(1)
		}
	}
	return funder.New(testLogger(t), store, defs.FeeModel{Type: defs.SatPerKB, Value: 1}, funder.WithBackoffForTest(backoff))
}

// TestFund_ContentionRetriesThenSucceeds: an optimistic store that contends on
// the first two claim calls must be retried and ultimately succeed within the
// bounded attempt budget.
func TestFund_ContentionRetriesThenSucceeds(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 5000)
	store := newContentionStore(inner, 2) // fail twice, then delegate

	var backoffs atomic.Int64
	f := contentionFunder(t, store, &backoffs)

	result, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

	require.NoError(t, err, "must succeed after retrying past the transient contention")
	require.Equal(t, []uint64{5000}, allocatedSats(result))
	require.EqualValues(t, 2, backoffs.Load(), "two contended attempts must each back off before the third succeeds")
}

// TestFund_ContentionExhaustedReleasesReservation: persistent contention across
// every attempt yields ErrUTXOContention and leaves nothing reserved.
func TestFund_ContentionExhaustedReleasesReservation(t *testing.T) {
	inner := newMemStore()
	mintCoins(t, inner, "tx", utxostore.TierMined, 5000)
	store := newContentionStore(inner, 1_000) // always contend

	var backoffs atomic.Int64
	f := contentionFunder(t, store, &backoffs)

	_, err := f.Fund(t.Context(), baseArgs(1000, 44, 1))

	require.ErrorIs(t, err, funder.ErrUTXOContention)
	require.Zero(t, reservedSats(t, inner), "the reservation must be released on contention exhaustion")
	// maxFundingAttempts = 3 -> two backoffs between the three attempts.
	require.EqualValues(t, 2, backoffs.Load())
}
