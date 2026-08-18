package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/perf"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// TestOptimisticShape_OneInputNoChange is the guard that makes the optimistic
// throughput ceiling mean what it claims.
//
// TestPerf_PostgresOptimisticCeiling reports a transactions-per-second number
// for the most favorable shape the toolbox can produce: one denominated input,
// one payment output, and NO change. "No change" is not configured anywhere —
// it falls out of the arithmetic in internal/perf/optimistic.go, where the
// remainder after the payment and fee lands below the funder's dust floor and is
// donated rather than minted.
//
// That makes it fragile in exactly the way an unasserted assumption always is: a
// change to the default fee rate, the dust-floor formula, or the P2PKH size
// constants would silently start minting a change output, ProcessAction would
// silently start doing a Mint per transaction, and the throughput number would
// silently begin measuring a different workload while still being reported as
// the optimistic ceiling.
//
// This test runs the same denomination and payment against a real provider on
// the same throughput funding path and asserts the shape directly, so that
// drift fails a fast hermetic test instead of quietly devaluing a benchmark.
func TestOptimisticShape_OneInputNoChange(t *testing.T) {
	ctx := context.Background()

	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet,
		&conformance.FakeOracle{}, &conformance.FakeHeaders{}, nil,
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
		storage.WithUTXOManagement(defs.UTXOManagement{
			Strategy: defs.StrategyThroughput,
			Throughput: defs.Throughput{
				DenominationSatoshis: perf.OptimisticDenomination,
				SpendPolicy:          defs.SpendPolicyPreferMined,
				PoolBasket:           "default",
				ReserveBasket:        "reserve",
			},
		}),
	)

	key := conformance.NewIdentityKey(t)
	resp, err := p.FindOrInsertUser(ctx, key)
	require.NoError(t, err)
	uid := resp.User.UserID
	auth := wdk.AuthID{IdentityKey: key, UserID: &uid}

	// A pool of denominated, mined coins — the same shape seedPool mints.
	const poolCoins = 8
	sats := make([]uint64, poolCoins)
	for i := range sats {
		sats[i] = perf.OptimisticDenomination
	}
	atomicBEEF, _ := conformance.BuildMinedAtomicBEEF(t, 0x61, 900_300, sats...)
	outs := make([]*wdk.InternalizeOutput, poolCoins)
	sender := conformance.NewIdentityKey(t)
	for i := range outs {
		outs[i] = conformance.WalletPaymentOutput(uint32(i), sender) //nolint:gosec // i < poolCoins
	}
	_, err = p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomicBEEF),
		Outputs: outs,
	})
	require.NoError(t, err)

	res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(perf.OptimisticPaymentSats))
	require.NoError(t, err,
		"the optimistic denomination no longer funds its own payment. Denomination "+
			"(%d) must cover the payment (%d) plus the transaction fee; if the fee "+
			"model changed, internal/perf/optimistic.go needs re-deriving",
		perf.OptimisticDenomination, perf.OptimisticPaymentSats)

	// ONE input: the ClaimExact fast path must close on n=1. More than one means
	// the closed form is allocating extra coins and the run would be measuring a
	// multi-input transaction.
	require.Len(t, res.Inputs, 1,
		"expected exactly one funding input at denomination %d for a %d-sat payment; "+
			"got %d. The optimistic ceiling is defined as a one-input transaction",
		perf.OptimisticDenomination, perf.OptimisticPaymentSats, len(res.Inputs))

	// NO change: every returned output must be the caller's own payment. A
	// storage-provided change output here means the remainder cleared the dust
	// floor and ProcessAction will Mint per transaction — a different workload
	// than the one the ceiling claims to measure.
	for _, o := range res.Outputs {
		assert.NotEqualf(t, wdk.ProvidedByStorage, o.ProvidedBy,
			"a change output was created (vout %d, %d sats). The remainder after "+
				"payment and fee cleared the dust floor, so this is no longer the "+
				"no-change shape the optimistic ceiling measures — re-derive the "+
				"denomination in internal/perf/optimistic.go", o.Vout, o.Satoshis)
	}
}
