package conformance

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
)

// NewInMemoryProvider builds and migrates a ready-to-use storage.Provider backed
// by an in-memory utxostore and a temp-file SQLite metastore, wired to the given
// oracle and headers source. It is an exported test helper (usable from any
// package, since the metastore/funder subsystems are internal to pkg/storage) so
// higher layers — notably the wallet's BRC-100 conformance harness — can stand
// up a real provider without duplicating the internal wiring. Pass a
// [FakeOracle] and [FakeHeaders] for a deterministic, offline provider.
func NewInMemoryProvider(t testing.TB, net defs.BSVNetwork, oracle arcade.TxOracle, hdrs headers.Headers) *storage.Provider {
	t.Helper()
	ctx := context.Background()
	logger := logging.NewTestLogger(t)

	meta, err := metastore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "conformance-meta.db"))
	require.NoError(t, err)
	utxo := memstore.New()
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())

	p, err := storage.New(logger, meta, utxo, fnd, oracle, hdrs,
		storage.WithNetwork(net),
		storage.WithStorageName("conformance"),
	)
	require.NoError(t, err)
	_, err = p.Migrate(ctx, "conformance", "conformance-storage-identity-key")
	require.NoError(t, err)
	return p
}
