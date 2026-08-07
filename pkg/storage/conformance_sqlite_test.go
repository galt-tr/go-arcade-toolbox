package storage_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
)

// TestProviderConformance_MemstoreSQLite runs the exported provider-level
// conformance suite (pkg/storage/conformance) against a Provider built on
// memstore (utxostore) + SQLite (metastore) — Mode B, the fast/default
// developer-loop combination. It is untagged so it always runs.
func TestProviderConformance_MemstoreSQLite(t *testing.T) {
	conformance.RunProviderSuite(
		t,
		func(t *testing.T) *storage.Provider {
			return newMemstoreSQLiteProvider(t, &conformance.FakeHeaders{})
		},
		conformance.WithRejectingHeadersProvider(func(t *testing.T) *storage.Provider {
			return newMemstoreSQLiteProvider(t, conformance.RejectingHeaders())
		}),
	)
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
