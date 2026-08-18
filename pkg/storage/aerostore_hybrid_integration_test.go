//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync/atomic"
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
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/aerostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

var hybridSetCounter atomic.Int64

var hybridQuietLogger = slog.New(slog.NewTextHandler(io.Discard, nil))

// TestProviderConformance_AerospikePGHybridModeB runs the exported provider-level
// conformance suite against a Provider whose inventory is Aerospike and whose
// metadata is PostgreSQL — the split "Mode B" wiring, with the two stores unable
// to share a transaction. It proves the whole BRC-100 stack works on the hybrid
// (CreateAction reserves in Aerospike; ProcessAction spends + mints change
// directly; AbortAction compensates). Selection is bucket-approximate, so the
// suite runs WithApproximateSelection. Requires both a Postgres and an
// Aerospike container (skips gracefully without a runtime).
func TestProviderConformance_AerospikePGHybridModeB(t *testing.T) {
	pg := testenv.StartPostgres(t)
	aero := testenv.StartAerospike(t)

	conformance.RunProviderSuite(
		t,
		func(t *testing.T) wdk.WalletStorageProvider {
			return newHybridModeBProvider(t, pg, aero, &conformance.FakeHeaders{})
		},
		conformance.WithApproximateSelection(),
		conformance.WithRejectingHeadersProvider(func(t *testing.T) wdk.WalletStorageProvider {
			return newHybridModeBProvider(t, pg, aero, conformance.RejectingHeaders())
		}),
		conformance.WithRejectReleaseEnv(func(t *testing.T) conformance.RejectReleaseEnv {
			return newHybridModeBEnv(t, pg, aero)
		}),
	)
}

// newHybridModeBEnv is newHybridModeBProvider plus the oracle and movable clock
// the reject->release and concurrent-lifecycle subtests need.
//
// Mode B is the interesting case for release: the metastore and the utxostore
// are separate stores, so a release cannot be one atomic transaction across
// both. The reconciler routes it through the durable utxo_ops_outbox instead,
// and these subtests exercise that path rather than the Mode A direct one.
func newHybridModeBEnv(
	t *testing.T, pg *testenv.PostgresContainer, aero *testenv.AerospikeContainer,
) conformance.RejectReleaseEnv {
	t.Helper()
	ctx := context.Background()

	clock := newTestClock()
	oracle := &conformance.FakeOracle{}

	meta, err := metastore.OpenPostgres(ctx, pg.IsolatedSchemaDSN(t), metastore.WithClock(clock.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	set := fmt.Sprintf("h%d", hybridSetCounter.Add(1))
	utxo, err := aerostore.New(ctx, aero.Host(), aero.Port(), aero.Namespace(),
		aerostore.WithSet(set), aerostore.WithLogger(hybridQuietLogger),
		aerostore.WithClock(clock.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, &conformance.FakeHeaders{},
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("conformance-hybrid-rr"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
		storage.WithClock(clock.Now),
	)
	require.NoError(t, err)
	require.False(t, p.ModeA(), "aerospike+postgres hybrid must be Mode B (split stores)")

	_, err = p.Migrate(ctx, "conformance", "conformance-identity-key")
	require.NoError(t, err)

	return conformance.RejectReleaseEnv{Provider: p, Oracle: oracle, Advance: clock.Advance}
}

// newHybridModeBProvider builds a fresh, unmigrated Provider over an isolated
// Postgres schema (metastore) and an isolated Aerospike set (utxostore) — a
// split-store deployment the provider must detect as Mode B.
func newHybridModeBProvider(t *testing.T, pg *testenv.PostgresContainer, aero *testenv.AerospikeContainer, hdrs headers.Headers) *storage.Provider {
	t.Helper()
	ctx := context.Background()

	meta, err := metastore.OpenPostgres(ctx, pg.IsolatedSchemaDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	set := fmt.Sprintf("h%d", hybridSetCounter.Add(1))
	utxo, err := aerostore.New(ctx, aero.Host(), aero.Port(), aero.Namespace(),
		aerostore.WithSet(set), aerostore.WithLogger(hybridQuietLogger))
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	oracle := &conformance.FakeOracle{}

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("conformance-hybrid"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
	)
	require.NoError(t, err)
	require.False(t, p.ModeA(), "aerospike+postgres hybrid must be Mode B (split stores)")
	return p
}
