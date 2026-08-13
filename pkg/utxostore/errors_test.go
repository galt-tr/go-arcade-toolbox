package utxostore_test

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// TestTypedErrorsIsAs verifies the errors.Is/As ergonomics of the taxonomy:
// type-only Is matching against zero-value targets, field extraction via As,
// and traversal through wrapped/joined batch errors.
func TestTypedErrorsIsAs(t *testing.T) {
	op := utxostoretest.NewOutpoint("err", 7)
	winner := utxostoretest.NewTxID("winner")

	cases := []struct {
		name   string
		err    error
		target error
	}{
		{"NotFound", &utxostore.NotFoundError{Op: op}, &utxostore.NotFoundError{}},
		{"AlreadyExists", &utxostore.AlreadyExistsError{Op: op}, &utxostore.AlreadyExistsError{}},
		{"Reserved", &utxostore.ReservedError{Op: op, HeldBy: "r"}, &utxostore.ReservedError{}},
		{"Spent", &utxostore.SpentError{Op: op, Winner: winner}, &utxostore.SpentError{}},
		{"Frozen", &utxostore.FrozenError{Op: op}, &utxostore.FrozenError{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			// Direct and wrapped Is matching, type-only.
			require.ErrorIs(t, tc.err, tc.target)
			require.ErrorIs(t, fmt.Errorf("wrapped: %w", tc.err), tc.target)
			require.NotEmpty(t, tc.err.Error())

			// Typed errors are distinct categories.
			for _, other := range cases {
				if other.name != tc.name {
					require.NotErrorIs(t, tc.err, other.target)
				}
			}
		})
	}

	// The joined batch shape providers return for []Outpoint methods:
	// ErrBatch findable via Is, per-item errors findable via As.
	batch := fmt.Errorf("%w: %w", utxostore.ErrBatch,
		errors.Join(&utxostore.ReservedError{Op: op, HeldBy: "r1"}, &utxostore.FrozenError{Op: op}))
	require.ErrorIs(t, batch, utxostore.ErrBatch)
	var reserved *utxostore.ReservedError
	require.ErrorAs(t, batch, &reserved)
	require.Equal(t, "r1", reserved.HeldBy)
	require.Equal(t, op, reserved.Op)
	var frozen *utxostore.FrozenError
	require.ErrorAs(t, batch, &frozen)
	require.Equal(t, op, frozen.Op)
}

// TestReservedErrorMessage pins the HeldBy convention: "" means the row is
// not reserved at all.
func TestReservedErrorMessage(t *testing.T) {
	op := utxostoretest.NewOutpoint("err", 0)
	require.Contains(t, (&utxostore.ReservedError{Op: op}).Error(), "not reserved")
	require.Contains(t, (&utxostore.ReservedError{Op: op, HeldBy: "res-1"}).Error(), `"res-1"`)
}

// TestTierString covers the Tier helpers.
func TestTierString(t *testing.T) {
	require.Equal(t, "sending", utxostore.TierSending.String())
	require.Equal(t, "unproven", utxostore.TierUnproven.String())
	require.Equal(t, "mined", utxostore.TierMined.String())
	require.Equal(t, "Tier(9)", utxostore.Tier(9).String())

	require.True(t, utxostore.TierSending.Valid())
	require.True(t, utxostore.TierMined.Valid())
	require.False(t, utxostore.Tier(0).Valid())
	require.False(t, utxostore.Tier(4).Valid())
}
