// Package testabilities is a compact, arcade-native test harness for the
// pkg/wallet BRC-100 conformance suite (and future wallet tests). It stands up a
// real wallet.Wallet over a real storage.Provider (in-memory utxostore + SQLite
// metastore, via conformance.NewInMemoryProvider) wired to deterministic,
// offline doubles (conformance.FakeOracle broadcasts succeed; conformance
// FakeHeaders accepts every merkle root). It deliberately does NOT reproduce the
// go-wallet-toolbox harness's GORM/RPC/service-mock machinery — the wallet only
// needs a wdk.WalletStorageProvider and a working sign/broadcast path, which
// this provides. The mock arcade/chaintracks HTTP servers (internal/testenv/
// mockarcade) back the e2e write-path test instead, where the real HTTP/SSE
// clients are exercised.
package testabilities

import (
	"context"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/brc29"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/fixtures"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/services"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wallet"
)

// StorageType selects a storage backend. This harness backs every type with the
// same in-memory provider; the distinct constants exist for source-compatibility
// with go-wallet-toolbox call sites.
type StorageType string

const (
	// StorageTypeSQLite is ported from go-wallet-toolbox (see upstream docs).
	StorageTypeSQLite StorageType = "sqlite"
	// StorageTypeMemory is ported from go-wallet-toolbox (see upstream docs).
	StorageTypeMemory StorageType = "memory"
	// StorageTypeMocked is ported from go-wallet-toolbox (see upstream docs).
	StorageTypeMocked StorageType = "mocked"
)

// aliceRootKey is a fixed, well-known secp256k1 private key ("1") used for the
// default "Alice" wallet.
const aliceRootKey = "0000000000000000000000000000000000000000000000000000000000000001"

// faucetSenderKey is the fixed sender private key the faucet pays from.
const faucetSenderKey = "0000000000000000000000000000000000000000000000000000000000000002"

// Fixture is the top-level test fixture returned by Given.
type Fixture struct {
	t   testing.TB
	net defs.BSVNetwork
}

// Given returns a wallet test fixture and a cleanup func.
func Given(t testing.TB) (*Fixture, func()) {
	t.Helper()
	return &Fixture{t: t, net: defs.NetworkTestnet}, func() {}
}

// Wallet begins building a wallet.
func (f *Fixture) Wallet() *WalletBuilder {
	return &WalletBuilder{f: f, net: f.net}
}

// WalletForRootKey builds a default wallet (SQLite storage, services) for the
// given root private-key hex.
func (f *Fixture) WalletForRootKey(rootKeyHex string) *wallet.Wallet {
	return f.Wallet().WithSQLiteStorage().WithServices().ForRootKey(rootKeyHex)
}

// AliceWalletWithStorage builds the default Alice wallet backed by the given
// storage type (all types map to the in-memory provider here).
func (f *Fixture) AliceWalletWithStorage(_ StorageType) *wallet.Wallet {
	return f.Wallet().WithServices().ForRootKey(aliceRootKey)
}

// Faucet returns a faucet that funds userWallet.
func (f *Fixture) Faucet(userWallet *wallet.Wallet) *Faucet {
	return &Faucet{f: f, w: userWallet}
}

// WalletBuilder configures and constructs a wallet.Wallet.
type WalletBuilder struct {
	f            *Fixture
	net          defs.BSVNetwork
	withServices bool
}

// WithNetwork sets the wallet network.
func (b *WalletBuilder) WithNetwork(net defs.BSVNetwork) *WalletBuilder {
	b.net = net
	return b
}

// WithSQLiteStorage selects SQLite-backed storage (the default here).
func (b *WalletBuilder) WithSQLiteStorage() *WalletBuilder { return b }

// WithServices attaches a services shim over the offline doubles.
func (b *WalletBuilder) WithServices() *WalletBuilder {
	b.withServices = true
	return b
}

// ForRootKey builds the wallet for the given root private-key hex.
func (b *WalletBuilder) ForRootKey(rootKeyHex string) *wallet.Wallet {
	t := b.f.t
	t.Helper()
	logger := logging.NewTestLogger(t)

	oracle := &conformance.FakeOracle{}
	hdrs := &conformance.FakeHeaders{}
	provider := conformance.NewInMemoryProvider(t, b.net, oracle, hdrs)

	var w *wallet.Wallet
	var err error
	if b.withServices {
		svc := services.New(logger, oracle, hdrs, defs.DefaultServicesConfig(b.net))
		w, err = wallet.New(b.net, rootKeyHex, provider, wallet.WithServices(svc), wallet.WithLogger(logger))
	} else {
		w, err = wallet.New(b.net, rootKeyHex, provider, wallet.WithLogger(logger))
	}
	require.NoError(t, err, "invalid test setup: could not create wallet")
	return w
}

// Faucet funds a wallet by internalizing a mined BRC-29 payment.
type Faucet struct {
	f *Fixture
	w *wallet.Wallet
}

// FaucetTx wraps the transaction the faucet built so callers can read its txid.
type FaucetTx struct{ tx *transaction.Transaction }

// TX returns the underlying faucet transaction.
func (ft *FaucetTx) TX() *transaction.Transaction { return ft.tx }

// TopUp funds the wallet with sats by internalizing a synthetic mined BRC-29
// payment (a single-leaf BUMP that FakeHeaders accepts). It returns the funding
// transaction and the payment remittance.
func (fa *Faucet) TopUp(sats uint64) (*FaucetTx, *sdk.Payment) {
	t := fa.f.t
	t.Helper()
	ctx := context.Background()

	recipient, err := fa.w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, fixtures.DefaultOriginator)
	require.NoError(t, err)

	senderPriv, err := ec.PrivateKeyFromHex(faucetSenderKey)
	require.NoError(t, err)

	keyID := brc29.KeyID{DerivationPrefix: fixtures.DerivationPrefix, DerivationSuffix: fixtures.DerivationSuffix}
	lockingScript, err := brc29.LockForCounterparty(brc29.PrivHex(faucetSenderKey), keyID, brc29.PubHex(recipient.PublicKey.ToDERHex()))
	require.NoError(t, err)

	tx := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = 0x22
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: lockingScript})

	txidHash := tx.TxID()
	trueVal := true
	mp := transaction.NewMerklePath(800000, [][]*transaction.PathElement{
		{{Offset: 0, Hash: txidHash, Txid: &trueVal}},
	})
	require.NoError(t, tx.AddMerkleProof(mp))

	beef := transaction.NewBeefV2()
	_, err = beef.MergeTransaction(tx)
	require.NoError(t, err)
	atomic, err := beef.AtomicBytes(txidHash)
	require.NoError(t, err)

	payment := &sdk.Payment{
		DerivationPrefix:  fixtures.DerivationPrefixBytes,
		DerivationSuffix:  fixtures.DerivationSuffixBytes,
		SenderIdentityKey: senderPriv.PubKey(),
	}
	res, err := fa.w.InternalizeAction(ctx, sdk.InternalizeActionArgs{
		Tx: atomic,
		Outputs: []sdk.InternalizeOutput{{
			OutputIndex:       0,
			Protocol:          sdk.InternalizeProtocolWalletPayment,
			PaymentRemittance: payment,
		}},
		Description: "faucet top up",
		Labels:      []string{"faucet"},
	}, fixtures.DefaultOriginator)
	require.NoError(t, err, "faucet internalize failed")
	require.True(t, res.Accepted)

	return &FaucetTx{tx: tx}, payment
}
