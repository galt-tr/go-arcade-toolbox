package storage

import (
	"context"
	"log/slog"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

func TestCertificates_InsertListRelinquish(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)

	cert := &wdk.TableCertificateX{
		TableCertificate: wdk.TableCertificate{
			Type:               "Y2VydC10eXBlMDAwMDAwMDAwMDAwMDAwMDA=",
			SerialNumber:       "c2VyaWFsMDAwMDAwMDAwMDAwMDAwMDAwMDA=",
			Certifier:          "0320bbfb879bbd6761ecd2962badbb41ba9d60ca88327d78b07ae7141af6b6c810",
			Subject:            primitives.PubKeyHex(testIdentityKey),
			RevocationOutpoint: "0000000000000000000000000000000000000000000000000000000000000000.0",
			Signature:          "30440220",
		},
		Fields: []*wdk.TableCertificateField{
			{FieldName: "name", FieldValue: "alice"},
		},
	}
	id, err := h.p.InsertCertificateAuth(ctx, h.auth, cert)
	require.NoError(t, err)
	assert.Positive(t, id)

	list, err := h.p.ListCertificates(ctx, h.auth, wdk.ListCertificatesArgs{Limit: 10})
	require.NoError(t, err)
	require.Equal(t, primitives.PositiveInteger(1), list.TotalCertificates)
	require.Len(t, list.Certificates, 1)
	assert.Equal(t, "alice", list.Certificates[0].Fields["name"])

	require.NoError(t, h.p.RelinquishCertificate(ctx, h.auth, wdk.RelinquishCertificateArgs{
		Type:         cert.Type,
		SerialNumber: cert.SerialNumber,
		Certifier:    cert.Certifier,
	}))

	list, err = h.p.ListCertificates(ctx, h.auth, wdk.ListCertificatesArgs{Limit: 10})
	require.NoError(t, err)
	assert.True(t, list.HasNoCertificates())
}

func TestInsertCertificate_SubjectMustBeCaller(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	cert := &wdk.TableCertificateX{TableCertificate: wdk.TableCertificate{
		Subject: "0000000000000000000000000000000000000000000000000000000000000000",
	}}
	_, err := h.p.InsertCertificateAuth(ctx, h.auth, cert)
	assert.ErrorIs(t, err, ErrAuthorization)
}

func TestListActions_And_Transactions(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.mintFunding(t, 0x91, 100_000)
	_, err := h.p.CreateAction(ctx, h.auth, paymentArgs(30_000))
	require.NoError(t, err)

	acts, err := h.p.ListActions(ctx, h.auth, wdk.ListActionsArgs{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, primitives.PositiveInteger(1), acts.TotalActions)

	txs, err := h.p.ListTransactions(ctx, h.auth, wdk.ListTransactionsArgs{Limit: 10})
	require.NoError(t, err)
	assert.Equal(t, primitives.PositiveInteger(1), txs.TotalTransactions)
}

func TestFindOutputsAndBaskets(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	txid := h.internalizeMinedPayment(t, 0xA1, 12_000)

	baskets, err := h.p.FindOutputBasketsAuth(ctx, h.auth, wdk.FindOutputBasketsArgs{})
	require.NoError(t, err)
	assert.NotEmpty(t, baskets)

	spendable := true
	outs, err := h.p.FindOutputsAuth(ctx, h.auth, wdk.FindOutputsArgs{Spendable: &spendable})
	require.NoError(t, err)
	require.NotEmpty(t, outs)
	assert.Equal(t, txid, *outs[0].TxID)
	assert.True(t, outs[0].Spendable)
}

func TestRelinquishOutput(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	txid := h.internalizeMinedPayment(t, 0xB1, 5_000)

	err := h.p.RelinquishOutput(ctx, h.auth, wdk.RelinquishOutputArgs{
		Basket: wdk.BasketNameForChange,
		Output: string(primitives.NewOutpointString(txid, 0)),
	})
	require.NoError(t, err)

	// No longer spendable.
	bal, err := h.p.GetBalance(ctx, h.auth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(0), bal)
}

func TestSync_RoundTrip(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)

	// Internalize a mined tx with a wallet-payment output (→ default/change
	// basket) and a basket-insertion output (→ a custom basket).
	atomic, _ := buildMinedAtomicBEEF(t, 0xC1, 830000, 21_000, 4_000)
	_, err := a.p.InternalizeAction(ctx, a.auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{
			{
				OutputIndex:       0,
				Protocol:          wdk.WalletPaymentProtocol,
				PaymentRemittance: &wdk.WalletPayment{DerivationPrefix: "cHJlZml4", DerivationSuffix: "c3VmZml4", SenderIdentityKey: testIdentityKey},
			},
			{
				OutputIndex:         1,
				Protocol:            wdk.BasketInsertionProtocol,
				InsertionRemittance: &wdk.BasketInsertion{Basket: "collectibles"},
			},
		},
	})
	require.NoError(t, err)

	syncArgs := syncArgsAB()
	chunk, err := a.p.GetSyncChunk(ctx, syncArgs)
	require.NoError(t, err)
	require.NotEmpty(t, chunk.Transactions)
	require.NotEmpty(t, chunk.Outputs)

	// Fresh storage B.
	b := newSyncTargetProvider(t)
	res, err := b.p.ProcessSyncChunk(ctx, syncArgs, chunk)
	require.NoError(t, err)
	assert.Positive(t, res.Inserts)

	uid := b.userID(ctx, t, testIdentityKey)
	bAuth := wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid}

	// Change/inventory rebuilt on B.
	bal, err := b.p.GetBalance(ctx, bAuth, "")
	require.NoError(t, err)
	assert.Equal(t, uint64(21_000), bal, "inventory rebuilt on the target storage")

	// The custom-basket output survived the round-trip (did NOT land in default).
	custom, err := b.p.ListOutputs(ctx, bAuth, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300("collectibles"),
		Limit:  10,
	})
	require.NoError(t, err)
	require.Len(t, custom.Outputs, 1, "basket-insertion output preserved in its own basket")
	assert.Equal(t, primitives.SatoshiValue(4_000), custom.Outputs[0].Satoshis)

	// And it did NOT leak into the default basket.
	def, err := b.p.ListOutputs(ctx, bAuth, wdk.ListOutputsArgs{
		Basket: primitives.StringUnder300(wdk.BasketNameForChange),
		Limit:  10,
	})
	require.NoError(t, err)
	require.Len(t, def.Outputs, 1, "only the wallet-payment change is in default")

	// The ancestry must survive the round-trip. Since the input-BEEF
	// de-duplication, transactions.input_beef is the authoritative (and for
	// create-path rows, the only) copy — so sync.go's `InputBEEF: t.InputBEEF`
	// is no longer redundant with a known_txs copy, and dropping it would
	// silently strip a synced storage of every transaction's ancestry.
	dstRows, _, err := b.meta.Transactions().ListActions(ctx, metastore.ListActionsFilter{UserID: uid, Limit: 10})
	require.NoError(t, err)
	require.NotEmpty(t, dstRows)
	require.NotEmpty(t, chunk.Transactions[0].InputBEEF, "the chunk carries the ancestry")
	require.Equal(t, []byte(chunk.Transactions[0].InputBEEF), []byte(dstRows[0].InputBEEF),
		"ProcessSyncChunk must persist the ancestry byte-for-byte")
}

// TestSync_RoundTrip_DoesNotPopulateKnownTxs characterizes a PRE-EXISTING gap,
// recorded here so it is not later mistaken for a consequence of the input-BEEF
// de-duplication.
//
// ProcessSyncChunk rebuilds transactions and outputs but never writes known_txs.
// A synced storage therefore cannot re-broadcast or assemble BEEF for the
// migrated transactions — getBEEFForTxIDs stubs their parents — and that was
// true before this change too. What the de-duplication DID change is that
// transactions.input_beef is now the only ancestry a synced storage receives,
// which is exactly what the assertion above pins.
func TestSync_RoundTrip_DoesNotPopulateKnownTxs(t *testing.T) {
	ctx := context.Background()
	a := newHarness(t)

	atomic, txid := buildMinedAtomicBEEF(t, 0xC2, 830000, 21_000, 4_000)
	_, err := a.p.InternalizeAction(ctx, a.auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{{
			OutputIndex:       0,
			Protocol:          wdk.WalletPaymentProtocol,
			PaymentRemittance: &wdk.WalletPayment{DerivationPrefix: "cHJlZml4", DerivationSuffix: "c3VmZml4", SenderIdentityKey: testIdentityKey},
		}},
	})
	require.NoError(t, err)

	syncArgs := syncArgsAB()
	chunk, err := a.p.GetSyncChunk(ctx, syncArgs)
	require.NoError(t, err)

	b := newSyncTargetProvider(t)
	_, err = b.p.ProcessSyncChunk(ctx, syncArgs, chunk)
	require.NoError(t, err)

	_, found, err := b.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.False(t, found, "sync does not populate known_txs — a pre-existing gap, not a regression")
}

// syncTarget is a second provider used as a sync destination.
type syncTarget struct {
	p    *Provider
	meta *metastore.Store
}

func newSyncTargetProvider(t *testing.T) *syncTarget {
	t.Helper()
	return newSyncTargetProviderWithLogger(t, logging.NewTestLogger(t))
}

// newSyncTargetProviderWithLogger is newSyncTargetProvider with the provider's
// logger injected, so a test can assert on what the sync path logs.
func newSyncTargetProviderWithLogger(t *testing.T, logger *slog.Logger) *syncTarget {
	t.Helper()
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "meta-b.db")
	meta, err := metastore.OpenSQLite(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	utxo := memstore.New()
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	p, err := New(logger, meta, utxo, fnd, &fakeOracle{}, &fakeHeaders{},
		WithNetwork(defs.NetworkTestnet), WithScriptsVerifier(alwaysValidScripts{}))
	require.NoError(t, err)
	_, err = p.Migrate(ctx, "storage-b", "storage-b")
	require.NoError(t, err)
	return &syncTarget{p: p, meta: meta}
}

func (s *syncTarget) userID(ctx context.Context, t *testing.T, identityKey string) int {
	t.Helper()
	u, found, err := s.meta.Users().FindByIdentityKey(ctx, identityKey)
	require.NoError(t, err)
	require.True(t, found)
	return u.UserID
}
