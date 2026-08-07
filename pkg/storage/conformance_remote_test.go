package storage_test

import (
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// TestProviderConformance_RemoteHTTP runs the EXACT same exported provider-level
// conformance suite (pkg/storage/conformance) that validates the direct
// Provider — but through the REST adapter: a [storage.Client] over an
// httptest.Server wrapping a [storage.Server] over a real memstore+SQLite
// Provider. Passing the whole suite through client -> HTTP -> server ->
// provider is the proof that the REST transport is a faithful, lossless
// implementation of wdk.WalletStorageProvider.
//
// It reuses newMemstoreSQLiteProvider (conformance_sqlite_test.go) as the
// server-side provider, so the direct and remote runs are held to the same
// exact-selection bar (no WithApproximateSelection).
func TestProviderConformance_RemoteHTTP(t *testing.T) {
	conformance.RunProviderSuite(
		t,
		func(t *testing.T) wdk.WalletStorageProvider {
			return newRemoteProvider(t, &conformance.FakeHeaders{})
		},
		conformance.WithRejectingHeadersProvider(func(t *testing.T) wdk.WalletStorageProvider {
			return newRemoteProvider(t, conformance.RejectingHeaders())
		}),
	)
}

// newRemoteProvider stands up a fresh, unmigrated server-side Provider (wired to
// the given headers fake), serves it over an in-process httptest.Server via the
// REST adapter, and returns a remote Client pointed at it. Each call gets its
// own provider+server+client trio, so suite subtests stay isolated. The suite
// migrates through the returned client itself.
func newRemoteProvider(t *testing.T, hdrs headers.Headers) wdk.WalletStorageProvider {
	t.Helper()

	prov := newMemstoreSQLiteProvider(t, hdrs)

	srv := storage.NewServer(logging.NewTestLogger(t), prov)
	ts := httptest.NewServer(srv.Handler())
	t.Cleanup(ts.Close)

	client, cleanup, err := storage.NewClient(ts.URL, storage.WithClientLogger(logging.NewTestLogger(t)))
	require.NoError(t, err)
	t.Cleanup(cleanup)

	return client
}
