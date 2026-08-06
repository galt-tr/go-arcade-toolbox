package funder_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

const (
	benchTarget      = satoshi.Value(75_000)
	benchTxSize      = uint64(44)
	benchOutputCount = uint64(1)
	benchMinSats     = 500
	benchMaxSats     = 50_000
)

// seedPool mints poolSize mined coins spread across [minSats, maxSats] and
// returns a funder wired to a recording store over the seeded memstore.
func seedPool(tb testing.TB, poolSize int) (*funder.Funder, *recordingStore) {
	tb.Helper()
	inner := newMemStore()

	spread := benchMaxSats - benchMinSats
	sats := make([]uint64, poolSize)
	for i := range sats {
		sats[i] = uint64(benchMinSats + (i*spread)/poolSize) //nolint:gosec // positive, bounded
	}
	mintCoins(tb, inner, "pool", utxostore.TierMined, sats...)

	rec := newRecordingStore(inner)
	f := funder.New(testLogger(tb), rec, defs.FeeModel{Type: defs.SatPerKB, Value: 1})
	return f, rec
}

// BenchmarkFund funds a fixed multi-input target against pools of 1000 and
// 10000 coins. The custom "claims/op" metric is the bounded-selection
// guarantee: the funder issues the SAME small number of store claims regardless
// of pool size (wall-clock ns/op still tracks memstore's O(n) full-scan per
// claim, which an indexed backend removes).
func BenchmarkFund(b *testing.B) {
	b.Run("pool_1000", func(b *testing.B) { benchmarkFund(b, 1000) })
	b.Run("pool_10000", func(b *testing.B) { benchmarkFund(b, 10000) })
}

func benchmarkFund(b *testing.B, poolSize int) {
	ctx := context.Background()
	f, rec := seedPool(b, poolSize)
	args := baseArgs(benchTarget, benchTxSize, benchOutputCount)

	// Pre-flight: a benchmark of an erroring path is worthless.
	result, err := f.Fund(ctx, args)
	require.NoError(b, err)
	require.NotEmpty(b, result.AllocatedUTXOs, "pre-flight must allocate coins")
	_, err = rec.ReleaseReservation(ctx, testUserID, testRef)
	require.NoError(b, err)
	rec.reset()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := f.Fund(ctx, args); err != nil {
			b.Fatalf("Fund failed on iteration %d: %v", i, err)
		}
		// Restore the pool for the next iteration; not part of the timed work.
		b.StopTimer()
		if _, err := rec.ReleaseReservation(ctx, testUserID, testRef); err != nil {
			b.Fatalf("release failed on iteration %d: %v", i, err)
		}
		b.StartTimer()
	}
	b.StopTimer()

	if b.N > 0 {
		b.ReportMetric(float64(rec.count())/float64(b.N), "claims/op")
	}
}
