package conformance

import (
	"context"
	"path/filepath"
	"testing"
	"time"

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
	return NewInMemoryProviderClock(t, net, oracle, hdrs, nil)
}

// NewInMemoryProviderClock is [NewInMemoryProvider] with a caller-supplied
// clock. A nil clock means time.Now.
//
// The clock goes into ALL THREE layers that have one — the provider, the
// metastore and the utxostore — and that completeness is the reason this exists
// as its own constructor. Every existing hermetic harness in this repo builds
// the utxostore as a bare memstore.New(), so reservation timestamps run on the
// real wall clock even where the provider's clock is faked. Any test that
// asserts on reservation AGE (FindStaleReservations, SweepStaleReservations)
// therefore could not be written against those harnesses without sleeping.
//
// The reconciler's release path is gated on a grace period measured from a
// stamp the metastore writes and the provider reads, so a test of "a rejection
// eventually returns its inputs" is unwritable without this. That is a whole
// class of test the exported seam could not previously express.
// Extra options are appended last, so a caller can add anything the default
// wiring leaves out. [AlwaysValidScripts] is the common one: [BuildSignedTx] —
// also exported from this package — produces dummy unlocking scripts, so the
// two helpers do not compose without it.
func NewInMemoryProviderClock(
	t testing.TB, net defs.BSVNetwork, oracle arcade.TxOracle, hdrs headers.Headers,
	now func() time.Time, extra ...storage.Option,
) *storage.Provider {
	t.Helper()
	ctx := context.Background()
	logger := logging.NewTestLogger(t)

	metaOpts := []metastore.Option{}
	utxoOpts := []memstore.Option{}
	storeOpts := []storage.Option{
		storage.WithNetwork(net),
		storage.WithStorageName("conformance"),
	}
	if now != nil {
		metaOpts = append(metaOpts, metastore.WithClock(now))
		utxoOpts = append(utxoOpts, memstore.WithClock(now))
		storeOpts = append(storeOpts, storage.WithClock(now))
	}

	meta, err := metastore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "conformance-meta.db"), metaOpts...)
	require.NoError(t, err)
	utxo := memstore.New(utxoOpts...)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())

	p, err := storage.New(logger, meta, utxo, fnd, oracle, hdrs, append(storeOpts, extra...)...)
	require.NoError(t, err)
	_, err = p.Migrate(ctx, "conformance", "conformance-storage-identity-key")
	require.NoError(t, err)
	return p
}
