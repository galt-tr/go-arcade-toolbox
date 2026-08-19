package storage_test

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// TestProviderConformance_MemstoreSQLite runs the exported provider-level
// conformance suite (pkg/storage/conformance) against a Provider built on
// memstore (utxostore) + SQLite (metastore) — Mode B, the fast/default
// developer-loop combination. It is untagged so it always runs.
func TestProviderConformance_MemstoreSQLite(t *testing.T) {
	conformance.RunProviderSuite(
		t,
		func(t *testing.T) wdk.WalletStorageProvider {
			return newMemstoreSQLiteProvider(t, &conformance.FakeHeaders{})
		},
		conformance.WithRejectingHeadersProvider(func(t *testing.T) wdk.WalletStorageProvider {
			return newMemstoreSQLiteProvider(t, conformance.RejectingHeaders())
		}),
		conformance.WithRejectReleaseEnv(newMemstoreSQLiteEnv),
	)
}

// newMemstoreSQLiteEnv is newMemstoreSQLiteProvider with the two things the
// reject->release and concurrent-lifecycle subtests need on top of the provider:
// the oracle they must script, and a clock they can move across the reconciler's
// grace window. The clock reaches all three layers that stamp time — provider,
// metastore and utxostore — because a reservation aged on the wall clock while
// the provider's clock is faked would never look stale.
func newMemstoreSQLiteEnv(t *testing.T) conformance.RejectReleaseEnv {
	t.Helper()
	ctx := context.Background()

	clock := newTestClock()
	oracle := &conformance.FakeOracle{}

	path := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metastore.OpenSQLite(ctx, path, metastore.WithClock(clock.Now))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo := memstore.New(memstore.WithClock(clock.Now))
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, &conformance.FakeHeaders{},
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("conformance-sqlite-rr"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
		storage.WithClock(clock.Now),
	)
	require.NoError(t, err)
	_, err = p.Migrate(ctx, "conformance", "conformance-identity-key")
	require.NoError(t, err)

	return conformance.RejectReleaseEnv{Provider: p, Oracle: oracle, Advance: clock.Advance}
}

// newTestClock seeds the mutex-guarded testClock from reconciler_test.go at a
// fixed instant. The reconciler's release is gated on a grace period, so the
// alternative to a movable clock is a test that sleeps for it — and a test that
// sleeps is a test the first person in a hurry deletes. The mutex matters here
// beyond the usual: the concurrent-lifecycle subtest reads the clock from many
// goroutines while the main one moves it.
func newTestClock() *testClock {
	return &testClock{t: time.Unix(1_700_000_000, 0).UTC()}
}

// newMemstoreSQLiteProvider builds a fresh, unmigrated Provider over a
// throwaway SQLite metastore file and a fresh memstore utxostore, wired with
// the given headers fake. Accepts testing.TB so BenchmarkCreateProcess (see
// bench_test.go) can reuse it too.
func newMemstoreSQLiteProvider(t testing.TB, hdrs headers.Headers) *storage.Provider {
	t.Helper()
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metastore.OpenSQLite(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo := memstore.New()
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	oracle := &conformance.FakeOracle{}

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("conformance-sqlite"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
	)
	require.NoError(t, err)
	return p
}
