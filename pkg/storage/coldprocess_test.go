package storage

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// reopenCold builds a NEW Provider over metastore state that already exists on
// disk, so a test can discard the first Provider and come back to the same
// durable rows with nothing cached.
//
// What is genuinely cold is the metastore handle, the Provider and every
// in-memory cache — precisely the surface that would have to be holding the
// input BEEF for the pre-txid window to work by accident. The utxostore is
// deliberately shared: the reservation and funding coin live there, and in
// production it is a separate durable service, not process state.
func reopenCold(t *testing.T, path string, utxo *memstore.Store) (*Provider, *metastore.Store) {
	t.Helper()
	ctx := context.Background()

	meta, err := metastore.OpenSQLite(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	logger := logging.NewTestLogger(t)
	p, err := New(logger, meta, utxo, funder.New(logger, utxo, defs.DefaultFeeModel()),
		&fakeOracle{}, &fakeHeaders{},
		WithNetwork(defs.NetworkTestnet),
		WithStorageName("test-storage"),
		WithScriptsVerifier(alwaysValidScripts{}),
	)
	require.NoError(t, err)
	return p, meta
}

// TestColdProcess_TransactionsInputBEEFIsLoadBearing is the regression test for
// the pre-txid window.
//
// CreateAction writes the input BEEF to the TRANSACTIONS row while the
// transaction is still unsigned and has NO txid; ProcessAction — a separate
// request, in production possibly a separate process — reads it back to hydrate
// the inputs before verifying and EF-encoding. The wallet cannot resupply it:
// its copy lives in an in-memory pending-sign map with no API to hand it back.
// Storage's on-disk copy is the only thing that bridges the gap, which is why
// the transactions column is authoritative and known_txs no longer duplicates it.
func TestColdProcess_TransactionsInputBEEFIsLoadBearing(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x21, 100_000, 40_000)
	txid := signed.TxID().String()

	// The window: ancestry on the row, no txid, and no known_txs row at all.
	row, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	require.Nil(t, row.TxID, "unsigned — there is no txid to key known_txs by")
	require.NotEmpty(t, row.InputBEEF, "the ancestry is retained on the transactions row")

	_, found, err = h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.False(t, found, "known_txs cannot hold the ancestry yet — the row does not exist")

	// Go cold: drop the Provider and the metastore handle entirely.
	require.NoError(t, h.meta.Close(ctx))
	p, meta := reopenCold(t, h.path, h.utxo)

	out, err := p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err, "processing must succeed off the stored ancestry alone")
	require.Len(t, out.SendWithResults, 1)
	require.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)

	kt, found, err := meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.NotEmpty(t, kt.RawTx, "known_txs still retains the raw tx it needs to rebroadcast")
	require.Empty(t, kt.InputBEEF, "the duplicate ancestry is no longer written to known_txs")
}

// TestColdProcess_WithoutTransactionsInputBEEF_ProcessFails is the negative
// control for the test above, and exists solely to prove that assertion is not
// vacuous: if the stored ancestry were not load-bearing, deleting it would
// change nothing and the test would pass for the wrong reason.
//
// Behaves identically before and after the de-duplication, by design.
func TestColdProcess_WithoutTransactionsInputBEEF_ProcessFails(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	res, signed, _ := h.createAndSign(t, 0x22, 100_000, 40_000)

	_, err := h.meta.DB().ExecContext(ctx,
		"UPDATE transactions SET input_beef = NULL WHERE reference = ?", res.Reference)
	require.NoError(t, err)

	require.NoError(t, h.meta.Close(ctx))
	p, _ := reopenCold(t, h.path, h.utxo)

	_, err = p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(signed.TxID().String()),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.Error(t, err, "without the stored ancestry the inputs cannot be hydrated")
}
