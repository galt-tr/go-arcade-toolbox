package conformance

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// multiUserIsolation: two users provisioned on the same provider never see or
// spend each other's outputs, and their balances stay independent.
func (s *suite) multiUserIsolation(t *testing.T) {
	ctx := context.Background()
	p := s.freshProvider(t)

	senderA, senderB := NewIdentityKey(t), NewIdentityKey(t)
	authA := s.newAuth(t, p, NewIdentityKey(t))
	authB := s.newAuth(t, p, NewIdentityKey(t))

	txA := internalizeMinedPayment(t, p, authA, senderA, 0x50, 10_000)
	txB := internalizeMinedPayment(t, p, authB, senderB, 0x51, 1_000_000)

	balA, err := p.GetBalance(ctx, authA, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(10_000), balA)

	balB, err := p.GetBalance(ctx, authB, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(1_000_000), balB)

	// Each user's ListOutputs shows their OWN coin and NOT the other's. The
	// NotEmpty preconditions matter: without them the absence loops would pass
	// vacuously if ListOutputs regressed to returning zero rows.
	listA, err := p.ListOutputs(ctx, authA, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300(wdk.BasketNameForChange), Limit: 50,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listA.Outputs, "user A must see their own output")
	for _, o := range listA.Outputs {
		assert.NotEqual(t, primitives.NewOutpointString(txB, 0), o.Outpoint, "user A must not see user B's output")
	}
	listB, err := p.ListOutputs(ctx, authB, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300(wdk.BasketNameForChange), Limit: 50,
	})
	require.NoError(t, err)
	require.NotEmpty(t, listB.Outputs, "user B must see their own output")
	for _, o := range listB.Outputs {
		assert.NotEqual(t, primitives.NewOutpointString(txA, 0), o.Outpoint, "user B must not see user A's output")
	}

	// FindOutputsAuth is scoped too: user A looking up user B's exact outpoint
	// finds nothing (never leaks the row across users).
	crossRows, err := p.FindOutputsAuth(ctx, authA, wdk.FindOutputsArgs{TxID: &txB, Vout: uint32Ptr(0)})
	require.NoError(t, err)
	assert.Empty(t, crossRows, "user A must not be able to look up user B's output by outpoint")

	// User A cannot spend into user B's funds: a target beyond A's own
	// balance, but well within the combined pool, must fail cleanly rather
	// than succeed by reaching into B's basket.
	_, err = p.CreateAction(ctx, authA, PaymentArgs(500_000))
	require.Error(t, err)
	assert.True(t, errors.Is(err, funder.ErrNotEnoughFunds), "user A must not be able to fund from user B's coins")

	// B's balance is untouched by A's failed attempt.
	balB2, err := p.GetBalance(ctx, authB, "")
	require.NoError(t, err)
	assert.Equal(t, balB, balB2)
}
