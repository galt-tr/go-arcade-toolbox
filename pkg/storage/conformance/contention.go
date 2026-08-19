package conformance

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/stress"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// contentionClaim runs N concurrent CreateActions against a single shared,
// funded basket: no outpoint is ever allocated to more than one successful
// result (no double-spend; the union of allocated inputs across all
// successes is disjoint), and every failure is a clean, expected funding
// error — never an opaque one. Under exact selection (the default) the pool
// is sized so every request must succeed; under
// [WithApproximateSelection] only the safety properties are required.
func (s *suite) contentionClaim(t *testing.T) {
	const (
		denomination = 50_000
		pay          = 40_000
	)
	// Scaled by ARCADE_STRESS (see internal/stress). The pool is sized from n
	// so the "every request must succeed" bar under exact selection holds at
	// any factor.
	n := stress.Scale(12)

	ctx := context.Background()
	p := s.freshProvider(t)
	sender := NewIdentityKey(t)
	auth := s.newAuth(t, p, NewIdentityKey(t))

	sats := make([]uint64, n)
	for i := range sats {
		sats[i] = denomination
	}
	atomic, _ := BuildMinedAtomicBEEF(t, 0x20, 900_000, sats...)
	outs := make([]*wdk.InternalizeOutput, n)
	for i := range outs {
		outs[i] = WalletPaymentOutput(uint32(i), sender) //nolint:gosec // i < n, small
	}
	_, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomic),
		Outputs: outs,
	})
	require.NoError(t, err)

	bal0, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, uint64(n)*denomination, bal0) //nolint:gosec // n is a small positive worker count

	type outcome struct {
		res *wdk.StorageCreateActionResult
		err error
	}
	results := make([]outcome, n)
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := p.CreateAction(ctx, auth, PaymentArgs(pay))
			results[i] = outcome{res: res, err: err}
		}(i)
	}
	wg.Wait()

	var successes int
	claimed := make(map[string]int) // "sourceTxID:sourceVout" -> claim count
	for i, o := range results {
		if o.err != nil {
			assert.True(t,
				errors.Is(o.err, funder.ErrNotEnoughFunds) || errors.Is(o.err, funder.ErrUTXOContention),
				"worker %d: unexpected error shape (want a clean ErrNotEnoughFunds/contention): %v", i, o.err)
			continue
		}
		successes++
		require.NotEmpty(t, o.res.Inputs, "worker %d: a successful CreateAction must allocate at least one input", i)
		for _, in := range o.res.Inputs {
			key := fmt.Sprintf("%s:%d", in.SourceTxID, in.SourceVout)
			claimed[key]++
			assert.Equal(t, 1, claimed[key], "input %s claimed by more than one successful CreateAction", key)
		}
	}

	// Disjointness cross-check: every claimed key was seen exactly once.
	var totalClaims int
	for _, c := range claimed {
		totalClaims += c
	}
	assert.Equal(t, len(claimed), totalClaims, "every claimed input must be claimed exactly once across all successes")

	if s.cfg.approximateSelection {
		assert.LessOrEqual(t, successes, n)
		assert.Positive(t, successes, "at least some requests must succeed from a fully-sufficient pool")
		return
	}
	assert.Equal(t, n, successes, "exact selection: every request must succeed from N sufficient, disjoint coins")
}
