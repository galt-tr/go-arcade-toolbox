package storage

import (
	"context"
	"path/filepath"
	"sort"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

const testIdentityKey = "0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798"

// --- fakes ---------------------------------------------------------------

type fakeOracle struct {
	calls     int
	lastEF    []byte
	broadcast func(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error)
}

func (f *fakeOracle) Broadcast(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error) {
	f.calls++
	f.lastEF = ef
	if f.broadcast == nil {
		return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusReceived}, nil
	}
	return f.broadcast(ctx, txid, ef)
}

func (f *fakeOracle) GetTx(context.Context, string) (*arcade.TxRecord, error) {
	return nil, arcade.ErrTxNotFound
}

func (f *fakeOracle) StreamStatus(context.Context, string, func(arcade.StatusEvent) error) error {
	return nil
}

func (f *fakeOracle) Health(context.Context) (*arcade.Health, error) {
	return &arcade.Health{Healthy: true}, nil
}

type fakeHeaders struct {
	verify func(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error)
	height uint32
	hash   chainhash.Hash
}

func (f *fakeHeaders) CurrentHeight(context.Context) (uint32, error) { return f.height, nil }
func (f *fakeHeaders) HeaderByHeight(_ context.Context, height uint32) (*headers.Header, error) {
	return &headers.Header{Height: height, Hash: f.hash}, nil
}

func (f *fakeHeaders) VerifyMerkleRoot(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error) {
	if f.verify == nil {
		return true, nil
	}
	return f.verify(ctx, root, height)
}

// alwaysValidScripts is a ScriptsVerifier that accepts every transaction, so
// tests can build unsigned/dummy-signed transactions without real keys.
type alwaysValidScripts struct{}

func (alwaysValidScripts) VerifyScripts(context.Context, *transaction.Transaction) (bool, error) {
	return true, nil
}

// --- harness -------------------------------------------------------------

type harness struct {
	p      *Provider
	meta   *metastore.Store
	utxo   *memstore.Store
	oracle *fakeOracle
	hdrs   *fakeHeaders
	userID int
	auth   wdk.AuthID
}

func newHarness(t *testing.T, opts ...Option) *harness {
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
	oracle := &fakeOracle{}
	hdrs := &fakeHeaders{}

	baseOpts := []Option{
		WithNetwork(defs.NetworkTestnet),
		WithStorageName("test-storage"),
		WithScriptsVerifier(alwaysValidScripts{}),
	}
	baseOpts = append(baseOpts, opts...)

	p, err := New(logger, meta, utxo, fnd, oracle, hdrs, baseOpts...)
	require.NoError(t, err)

	_, err = p.Migrate(ctx, "test-storage", "storage-identity-key")
	require.NoError(t, err)

	resp, err := p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	require.True(t, resp.IsNew)

	uid := resp.User.UserID
	return &harness{
		p:      p,
		meta:   meta,
		utxo:   utxo,
		oracle: oracle,
		hdrs:   hdrs,
		userID: uid,
		auth:   wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid},
	}
}

// mintFunding builds a synthetic source transaction with one P2PKH output of
// sats, stores it in known_txs (so BEEF ancestry resolves), and mints the coin
// into the change basket at TierMined. Returns the coin's outpoint.
func (h *harness) mintFunding(t *testing.T, seed byte, sats uint64) utxostore.Outpoint {
	t.Helper()
	ctx := context.Background()

	src := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = seed
	src.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: testP2PKH(t)})

	txid := src.TxID().String()
	require.NoError(t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID:   txid,
		Status: wdk.ProvenTxStatusCompleted,
		RawTx:  src.Bytes(),
	}))

	op := utxostore.Outpoint{TxID: *src.TxID(), Vout: 0}
	m := &utxostore.Mint{
		Outpoint:  op,
		UserID:    int64(h.userID),
		Basket:    wdk.BasketNameForChange,
		Satoshis:  sats,
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      utxostore.TierMined,
	}
	require.NoError(t, h.utxo.Mint(ctx, []*utxostore.Mint{m}))
	require.NoError(t, m.Err)
	return op
}

func testP2PKH(t *testing.T) *script.Script {
	t.Helper()
	priv, err := ec.NewPrivateKey()
	require.NoError(t, err)
	addr, err := script.NewAddressFromPublicKey(priv.PubKey(), false)
	require.NoError(t, err)
	ls, err := p2pkh.Lock(addr)
	require.NoError(t, err)
	return ls
}

// buildSignedTx assembles a transaction from a CreateAction result: inputs in
// Vin order (dummy unlocking scripts) and outputs in vout order (provided
// scripts as given, a dummy P2PKH for storage-derived change).
func buildSignedTx(t *testing.T, res *wdk.StorageCreateActionResult) *transaction.Transaction {
	t.Helper()
	tx := transaction.NewTransaction()
	tx.Version = res.Version
	tx.LockTime = res.LockTime

	ins := append([]*wdk.StorageCreateTransactionSdkInput(nil), res.Inputs...)
	sort.Slice(ins, func(i, j int) bool { return ins[i].Vin < ins[j].Vin })
	for _, in := range ins {
		h, err := chainhash.NewHashFromHex(in.SourceTxID)
		require.NoError(t, err)
		tx.AddInput(&transaction.TransactionInput{
			SourceTXID:       h,
			SourceTxOutIndex: in.SourceVout,
			UnlockingScript:  script.NewFromBytes([]byte{0x00}),
			SequenceNumber:   transaction.DefaultSequenceNumber,
		})
	}

	outs := append([]*wdk.StorageCreateTransactionSdkOutput(nil), res.Outputs...)
	sort.Slice(outs, func(i, j int) bool { return outs[i].Vout < outs[j].Vout })
	for _, out := range outs {
		var ls *script.Script
		if out.LockingScript != "" {
			var err error
			ls, err = script.NewFromHex(out.LockingScript.String())
			require.NoError(t, err)
		} else {
			ls = testP2PKH(t) // storage-derived change
		}
		tx.AddOutput(&transaction.TransactionOutput{Satoshis: uint64(out.Satoshis), LockingScript: ls})
	}
	return tx
}

func paymentArgs(sats uint64) wdk.ValidCreateActionArgs {
	return wdk.ValidCreateActionArgs{
		Description: "test payment",
		Outputs: []wdk.ValidCreateActionOutput{{
			LockingScript:     "76a914dbc0a7c84983c5bf199b7b2d41b3acf0408ee5aa88ac",
			Satoshis:          primitives.SatoshiValue(sats),
			OutputDescription: "payment",
		}},
		IsNewTx: true,
	}
}

// --- tests: create + balance --------------------------------------------

func TestCreateAction_HappyPath(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	coin := h.mintFunding(t, 0x11, 100_000)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	require.NotNil(t, res)

	// Template shape.
	assert.NotEmpty(t, res.Reference)
	assert.NotEmpty(t, res.DerivationPrefix)
	assert.NotEmpty(t, res.InputBeef)
	require.NotEmpty(t, res.Inputs, "at least the allocated input")
	require.NotEmpty(t, res.Outputs, "payment + change outputs")

	// The allocated coin is reserved under the reference.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy, "funding coin reserved under the tx reference")

	// The unsigned transaction was persisted.
	txRow, found, err := h.meta.Transactions().FindByReference(ctx, h.userID, res.Reference)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, wdk.TxStatusUnsigned, txRow.Status)
	assert.True(t, txRow.IsOutgoing)

	// There is at least one change output in the template.
	var hasChange bool
	for _, o := range res.Outputs {
		if o.ProvidedBy == wdk.ProvidedByStorage && o.Purpose == wdk.ChangePurpose {
			hasChange = true
		}
	}
	assert.True(t, hasChange, "expected a storage change output")
}

func TestCreateAction_InsufficientFunds(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.mintFunding(t, 0x22, 1_000) // far too little

	_, err := h.p.CreateAction(ctx, h.auth, paymentArgs(50_000))
	require.Error(t, err)
	assert.ErrorIs(t, err, funder.ErrNotEnoughFunds)
}

func TestGetBalance_Composition(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.mintFunding(t, 0x31, 30_000)
	h.mintFunding(t, 0x32, 20_000)

	bal, err := h.p.GetBalance(ctx, h.auth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(50_000), bal, "claimable coins summed")

	// Reserving via a create action removes the coin from the claimable balance.
	_, err = h.p.CreateAction(ctx, h.auth, paymentArgs(10_000))
	require.NoError(t, err)

	bal2, err := h.p.GetBalance(ctx, h.auth, "")
	require.NoError(t, err)
	assert.Less(t, bal2, uint64(50_000), "reserved coin no longer counts as spendable")
}

func TestMakeAvailable(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	settings, err := h.p.MakeAvailable(ctx)
	require.NoError(t, err)
	assert.Equal(t, "storage-identity-key", settings.StorageIdentityKey)
	assert.Equal(t, defs.NetworkTestnet, settings.Chain)
	assert.Equal(t, defs.DBTypeSQLite, settings.DbType)
}

func TestListOutputs_SpendabilityIntersection(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	// Internalize a mined output so it appears as a spendable basket output.
	txid := h.internalizeMinedPayment(t, 0x41, 25_000)

	res, err := h.p.ListOutputs(ctx, h.auth, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300(wdk.BasketNameForChange),
		Limit:  10,
	})
	require.NoError(t, err)
	require.NotEmpty(t, res.Outputs)
	var found bool
	for _, o := range res.Outputs {
		if o.Outpoint == primitives.NewOutpointString(txid, 0) {
			found = true
			assert.True(t, o.Spendable, "mined internalized output is spendable")
		}
	}
	assert.True(t, found)
}
