package funder

import (
	"context"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// maxExactClaim caps the closed-form fast-path count so the uint64→int
// conversion is always safe; any residual need beyond it is covered by the
// bounded tiered walk. A repo-market action needs only a handful of fuel
// inputs, so this bound is never approached in practice.
const maxExactClaim = 1 << 20

// claimExactFastPath is the throughput strategy's happy path: the denominated
// fuel pool holds coins of exactly args.Denomination, so the number of inputs
// needed is closed-form —
//
//	n = ceil((target + baseFee) / (denomination − marginalInputFee))
//
// where marginalInputFee is the fee one fuel input adds. It claims n coins in a
// single ClaimExact against the safest tier and allocates them until funded,
// releasing any surplus. Underflow (a short pool) or a residual need after the
// exact claim is left for the bounded tiered walk to top up.
func (f *Funder) claimExactFastPath(ctx context.Context, args FundArgs, collector *utxoCollector) error {
	marginal := defs.MarginalFuelInputFee(f.feeModel)
	if args.Denomination <= marginal {
		return fmt.Errorf("funder: denomination %d must exceed the marginal input fee %d", args.Denomination, marginal)
	}

	baseFee, err := f.feeCalculator.Calculate(args.CurrentTxSize)
	if err != nil {
		return fmt.Errorf("failed to calculate base fee: %w", err)
	}

	need, err := satoshi.Add(args.TargetSat, baseFee)
	if err != nil {
		return fmt.Errorf("failed to compute fast-path need: %w", err)
	}
	if need <= 0 {
		// Nothing to prefund; the bounded walk (if anything at all is needed)
		// takes over.
		return nil
	}

	net := args.Denomination - marginal
	n := ceilDiv(need.MustUInt64(), net)
	if n == 0 {
		return nil
	}
	if n > maxExactClaim {
		n = maxExactClaim
	}

	// Claim exact-denomination fuel coins across the spend tiers, safest first.
	// On a real network the pool lingers at TierUnproven (coins are broadcast-
	// accepted but rarely mined), so a mined-only claim would miss the entire
	// pool and force every payment onto the contended bounded walk — walking the
	// tiers keeps the low-contention fast path alive regardless of settlement.
	remaining := int(n) //nolint:gosec // n <= maxExactClaim
	var claimed []*utxostore.UTXO
	for _, tier := range args.Tiers {
		if remaining <= 0 {
			break
		}
		scope := utxostore.Scope{UserID: args.UserID, Basket: args.Basket, Tier: tier}
		got, err := f.store.ClaimExact(ctx, scope, args.Reservation, args.Denomination, remaining)
		if err != nil {
			return fmt.Errorf("failed to claim exact fuel coins: %w", err)
		}
		claimed = append(claimed, got...)
		remaining -= len(got)
	}

	consumed := 0
	for _, u := range claimed {
		if collector.isFunded() {
			break
		}
		if err = collector.allocateUTXO(u); err != nil {
			return fmt.Errorf("failed to allocate fuel coin: %w", err)
		}
		consumed++
	}

	// Mid-flow, so deliberately on the REQUEST context: a failure here
	// propagates to Fund, whose detached terminal release frees the whole token
	// (see Funder.release).
	if consumed < len(claimed) {
		return f.releaseOutpoints(ctx, args.Reservation, claimed[consumed:])
	}
	return nil
}

// ceilDiv returns ceil(a / b) for positive b.
func ceilDiv(a, b uint64) uint64 {
	if b == 0 {
		return 0
	}
	return (a + b - 1) / b
}

// ShapeChange computes how many exact-value fuel outputs of `denomination`
// satoshis a given amount can be split into, capped at maxOutputs. It is the
// funder-level shaping primitive the throughput strategy's fan-out relies on:
// the wallet / fuel keeper drives the actual createAction (choosing baskets,
// keys, and broadcasting), while the funder owns the arithmetic of how many
// denominated outputs an amount affords. It returns 0 when the amount cannot
// fund even one output.
func ShapeChange(amount satoshi.Value, denomination, maxOutputs uint64) uint64 {
	if denomination == 0 || maxOutputs == 0 || amount <= 0 {
		return 0
	}
	count := amount.MustUInt64() / denomination
	if count > maxOutputs {
		count = maxOutputs
	}
	return count
}
