package sqlstore_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// ClaimableExists on the engine where the Mode A false-empty cannot happen.
//
// SQLite pins a single writer connection, so the PostgreSQL scenario — a second
// claimer arriving while an uncommitted peer holds row locks — is not
// constructible here: a competing writer blocks (and is retried as a lock error)
// instead of skipping locked rows and seeing a false-empty pool. The probe is
// still compiled, bound and answered on this engine, though, and the funder will
// call it, so its answers have to be right.
//
// This is therefore the SQLite drift guard for the probe predicate. A probe
// whose WHERE clause is LOOSER than the claim's — a dropped reserved_by/
// spent_by/frozen term, a missing scope column, the wrong satoshi comparison —
// turns each false case below into a true, which the funder reads as contention
// and retries three times before failing the user with ErrUTXOContention instead
// of the honest ErrNotEnoughFunds. It also proves the SQLite probe statement
// parses and binds (four placeholders: three scope columns plus the bound).
func TestClaimableExistsOnSQLite(t *testing.T) {
	ctx := context.Background()

	cases := []struct {
		name    string
		seed    func(t *testing.T, s *sqlstore.Store)
		minSats uint64
		want    bool
	}{
		{
			name: "empty pool",
			seed: func(*testing.T, *sqlstore.Store) {},
			want: false,
		},
		{
			name: "every coin reserved by another token",
			seed: func(t *testing.T, s *sqlstore.Store) {
				utxostoretest.MintTx(t, s, "probe-reserved", 1, "default", utxostore.TierMined, 50_000)
				held, err := s.ClaimSmallestSufficient(ctx, minedScope, "holder", 10_000)
				require.NoError(t, err)
				require.NotNil(t, held)
			},
			want: false,
		},
		{
			name: "every coin frozen",
			seed: func(t *testing.T, s *sqlstore.Store) {
				ops := utxostoretest.MintTx(t, s, "probe-frozen", 1, "default", utxostore.TierMined, 50_000)
				require.NoError(t, s.Freeze(ctx, ops))
			},
			want: false,
		},
		{
			name: "every coin below the bound",
			seed: func(t *testing.T, s *sqlstore.Store) {
				utxostoretest.MintTx(t, s, "probe-small", 1, "default", utxostore.TierMined, 900)
			},
			minSats: 10_000,
			want:    false,
		},
		{
			name: "coins only in other scopes",
			seed: func(t *testing.T, s *sqlstore.Store) {
				utxostoretest.MintTx(t, s, "probe-other-tier", 1, "default", utxostore.TierUnproven, 50_000)
				utxostoretest.MintTx(t, s, "probe-other-basket", 1, "fuel", utxostore.TierMined, 50_000)
				utxostoretest.MintTx(t, s, "probe-other-user", 2, "default", utxostore.TierMined, 50_000)
			},
			want: false,
		},
		{
			name: "a claimable coin in scope",
			seed: func(t *testing.T, s *sqlstore.Store) {
				utxostoretest.MintTx(t, s, "probe-claimable", 1, "default", utxostore.TierMined, 50_000)
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := newSQLiteStore(t)
			tc.seed(t, s)

			got, err := s.ClaimableExists(ctx, minedScope, tc.minSats)
			require.NoError(t, err)
			require.Equal(t, tc.want, got)
		})
	}
}

// TestClaimableExistsRejectsAnUnderspecifiedScope: the probe answers a question
// about a scope, so an unusable scope is a programmer error, not a false. A
// silent false would read as "you are out of money".
func TestClaimableExistsRejectsAnUnderspecifiedScope(t *testing.T) {
	s := newSQLiteStore(t)
	ctx := context.Background()

	for _, sc := range []utxostore.Scope{
		{UserID: 0, Basket: "default", Tier: utxostore.TierMined},
		{UserID: 1, Basket: "", Tier: utxostore.TierMined},
		{UserID: 1, Basket: "default", Tier: utxostore.Tier(99)},
	} {
		_, err := s.ClaimableExists(ctx, sc, 0)
		require.Error(t, err, "scope %+v must be rejected", sc)
	}
}

// TestClaimsStillReportNoneOnAnEmptyResult pins the contract the probe did NOT
// change: claims answer (nil, nil) when they find nothing, on every shape. The
// diagnosis is a separate, opt-in query — nothing on the allocating path pays
// for it.
func TestClaimsStillReportNoneOnAnEmptyResult(t *testing.T) {
	s := newSQLiteStore(t)
	ctx := context.Background()

	utxostoretest.MintTx(t, s, "none", 1, "default", utxostore.TierMined, 900)

	got, err := s.ClaimSmallestSufficient(ctx, minedScope, "probe", 10_000)
	require.NoError(t, err)
	require.Nil(t, got)

	batch, err := s.ClaimLargestInsufficient(ctx, minedScope, "probe", 500, 4)
	require.NoError(t, err)
	require.Empty(t, batch)

	exact, err := s.ClaimExact(ctx, minedScope, "probe", 25_000, 2)
	require.NoError(t, err)
	require.Empty(t, exact)
}
