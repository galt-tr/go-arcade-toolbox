//go:build integration

package storage_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/testenv"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// TestProviderConformance_PostgresModeA runs the exported provider-level
// conformance suite (pkg/storage/conformance) against a Provider built on
// sqlstore + metastore sharing ONE PostgreSQL database (Mode A), via
// [testenv]. Skips gracefully (through [testenv.StartPostgres]) when no
// container runtime is available.
func TestProviderConformance_PostgresModeA(t *testing.T) {
	pg := testenv.StartPostgres(t)

	conformance.RunProviderSuite(
		t,
		func(t *testing.T) wdk.WalletStorageProvider {
			return newPostgresModeAProvider(t, pg, &conformance.FakeHeaders{})
		},
		conformance.WithRejectingHeadersProvider(func(t *testing.T) wdk.WalletStorageProvider {
			return newPostgresModeAProvider(t, pg, conformance.RejectingHeaders())
		}),
		conformance.WithRejectReleaseEnv(func(t *testing.T) conformance.RejectReleaseEnv {
			return newPostgresModeAEnv(t, pg)
		}),
	)
}

// newPostgresModeAEnv is newPostgresModeAProvider plus the oracle and the
// movable clock the reject->release and concurrent-lifecycle subtests need.
//
// This is the wiring that matters most: Mode A over one shared PostgreSQL
// database is the configuration those subtests were written for, because it is
// the one where the release competes with concurrent claims under real row
// locks rather than a mutex.
func newPostgresModeAEnv(t *testing.T, pg *testenv.PostgresContainer) conformance.RejectReleaseEnv {
	t.Helper()
	ctx := context.Background()

	clock := newTestClock()
	oracle := &conformance.FakeOracle{}

	meta, err := metastore.OpenPostgres(ctx, pg.IsolatedSchemaDSN(t), metastore.WithClock(clock.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo, err := sqlstore.New(ctx, meta.DB(), sqlstore.Engine(string(meta.Engine())),
		sqlstore.WithClock(clock.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, &conformance.FakeHeaders{},
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("conformance-postgres-rr"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
		storage.WithClock(clock.Now),
	)
	require.NoError(t, err)
	require.True(t, p.ModeA(), "provider must detect Mode A over the shared database")

	_, err = p.Migrate(ctx, "conformance", "conformance-identity-key")
	require.NoError(t, err)

	return conformance.RejectReleaseEnv{Provider: p, Oracle: oracle, Advance: clock.Advance}
}

// newPostgresModeAProvider builds a fresh, unmigrated Provider over an
// isolated schema in pg, sharing one *sql.DB between the metastore and the
// sqlstore utxostore (Mode A), wired with the given headers fake.
func newPostgresModeAProvider(t *testing.T, pg *testenv.PostgresContainer, hdrs headers.Headers) *storage.Provider {
	t.Helper()
	ctx := context.Background()

	meta, err := metastore.OpenPostgres(ctx, pg.IsolatedSchemaDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo, err := sqlstore.New(ctx, meta.DB(), sqlstore.Engine(string(meta.Engine())))
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	require.True(t, meta.SharesDatabase(meta.DB()))
	require.True(t, utxo.SharesDatabase(meta.DB()))

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	oracle := &conformance.FakeOracle{}

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("conformance-postgres"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
	)
	require.NoError(t, err)
	require.True(t, p.ModeA(), "provider must detect Mode A over the shared database")
	return p
}
