//go:build integration

package storage

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/sqlstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// modeAHarness wires a metastore and a sqlstore over ONE PostgreSQL *sql.DB
// (Mode A) plus a provider over them.
type modeAHarness struct {
	p      *Provider
	meta   *metastore.Store
	utxo   utxostore.Store
	oracle *fakeOracle
	userID int
	auth   wdk.AuthID
}

func newModeAHarness(t *testing.T, pg *testenv.PostgresContainer) *modeAHarness {
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
	oracle := &fakeOracle{}

	p, err := New(logger, meta, utxo, fnd, oracle, &fakeHeaders{},
		WithNetwork(defs.NetworkTestnet), WithStorageName("modea"),
		WithScriptsVerifier(alwaysValidScripts{}))
	require.NoError(t, err)
	require.True(t, p.ModeA(), "provider must detect Mode A over the shared database")

	_, err = p.Migrate(ctx, "modea", "modea-identity")
	require.NoError(t, err)
	resp, err := p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	uid := resp.User.UserID

	return &modeAHarness{
		p:      p,
		meta:   meta,
		utxo:   utxo,
		oracle: oracle,
		userID: uid,
		auth:   wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid},
	}
}

func (h *modeAHarness) mintFunding(t *testing.T, seed byte, sats uint64) utxostore.Outpoint {
	t.Helper()
	ctx := context.Background()
	src := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = seed
	src.AddInput(&transaction.TransactionInput{SourceTXID: &srcHash, SourceTxOutIndex: 0, SequenceNumber: transaction.DefaultSequenceNumber})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: testP2PKH(t)})
	txid := src.TxID().String()
	require.NoError(t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{TxID: txid, Status: wdk.ProvenTxStatusCompleted, RawTx: src.Bytes()}))
	op := utxostore.Outpoint{TxID: *src.TxID(), Vout: 0}
	m := &utxostore.Mint{Outpoint: op, UserID: int64(h.userID), Basket: wdk.BasketNameForChange, Satoshis: sats, InputSize: utxostore.DefaultP2PKHInputSize, Tier: utxostore.TierMined}
	require.NoError(t, h.utxo.Mint(ctx, []*utxostore.Mint{m}))
	require.NoError(t, m.Err)
	return op
}

// TestModeA_CreateProcessAccept_Atomic proves a full CreateAction→ProcessAction
// (fake oracle accept) commits inventory + metadata together over one database.
func TestModeA_CreateProcessAccept_Atomic(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	h := newModeAHarness(t, pg)
	coin := h.mintFunding(t, 0x11, 100_000)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)

	// After CreateAction: coin reserved and unsigned tx row both committed.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Equal(t, res.Reference, u.ReservedBy)
	_, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)

	signed := buildSignedTx(t, res)
	txid := signed.TxID().String()
	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	assert.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status)

	// Metadata + inventory both reflect acceptance.
	txRow, _, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	assert.Equal(t, wdk.TxStatusUnproven, txRow.Status)

	spent, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, spent.SpentBy)

	// Change is minted but not yet claimable: a 202 is acceptance-for-processing,
	// not validation, so change stays at TierSending until arcade SEENs the tx.
	chOp := changeOutpoint(t, res, txid)
	ch, err := h.utxo.Get(ctx, chOp)
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierSending, ch.Tier, "change not claimable on the 202, only on SEEN")

	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusSeenOnNetwork}))
	ch, err = h.utxo.Get(ctx, chOp)
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierUnproven, ch.Tier, "SEEN promotes change to claimable")
}

// TestModeA_BroadcastFailure_Consistent proves an opaque broadcast failure
// leaves a consistent committed state (persistence landed; no torn writes).
func TestModeA_BroadcastFailure_Consistent(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	h := newModeAHarness(t, pg)
	h.oracle.broadcast = func(context.Context, string, []byte) (*arcade.BroadcastResult, error) {
		return nil, assertOpaqueErr
	}
	coin := h.mintFunding(t, 0x12, 100_000)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	signed := buildSignedTx(t, res)
	txid := signed.TxID().String()

	out, err := h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Len(t, out.NotDelayedResults, 1)
	assert.Equal(t, wdk.ReviewActionResultStatusServiceError, out.NotDelayedResults[0].Status)

	// Consistent committed state: persistence landed (change minted at
	// TierSending, tx in-flight), input still reserved (never spent).
	ch, err := h.utxo.Get(ctx, changeOutpoint(t, res, txid))
	require.NoError(t, err)
	assert.Equal(t, utxostore.TierSending, ch.Tier)
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy)
	assert.Nil(t, u.SpentBy)

	txRow, _, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	assert.Equal(t, wdk.TxStatusSending, txRow.Status)
}

var assertOpaqueErr = &opaqueError{}

type opaqueError struct{}

func (*opaqueError) Error() string { return "arcade: opaque broadcast failure" }
