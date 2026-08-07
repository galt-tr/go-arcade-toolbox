// Package demoenv wires a fully in-process, runnable go-arcade-toolbox wallet
// for the runnable examples under examples/.
//
// It stands up a REAL wallet.Wallet over a REAL storage.Provider (SQLite, the
// shared-database "Mode A" deployment) whose Arcade transaction-oracle and
// ChainTracks headers source are the in-process HTTP/SSE mock doubles from
// internal/testenv/mockarcade. The wallet reaches those doubles through the
// production arcade.Client and headers.Client, so the examples exercise the
// true HTTP/SSE code paths — there are no fakes on the trust-critical seams
// (broadcast, merkle-root verification).
//
// This is DEMO scaffolding, not a production recipe. Two things here are demo
// shortcuts you MUST replace in a real deployment:
//
//   - The Arcade/ChainTracks URLs point at the in-process mocks. In production
//     you point defs.Arcade.URL / defs.ChainTracks.URL at a real Arcade
//     deployment (see docs/arcade-integration.md).
//   - SeedFunds is a local "faucet" that mints a valid mined BRC-29 payment and
//     internalizes it. In production the wallet is funded by receiving a real
//     external payment via InternalizeAction (see examples/internalize) — a
//     fresh wallet has zero spendable balance and there is NO restore-from-seed
//     (see docs/operations.md).
//
// The private keys below are well-known test keys. Never hardcode a key in
// production.
package demoenv

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv/mockarcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/brc29"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/services"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/perfprovider"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wallet"
)

const (
	// WalletKeyHex is the demo wallet owner's secp256k1 private key (the key "1").
	WalletKeyHex = "0000000000000000000000000000000000000000000000000000000000000001"

	// senderKeyHex is the external "faucet" sender that pays the demo wallet.
	senderKeyHex = "0000000000000000000000000000000000000000000000000000000000000002"

	// Originator identifies the calling application on every wallet call. BRC-100
	// requires a non-empty, FQDN-shaped originator.
	Originator = "arcade-toolbox-example.com"
)

// Network is the BSV network the demo wallet runs against.
const Network = defs.NetworkTestnet

// The demo faucet's BRC-29 derivation material. The KeyID carries the
// base64-encoded form the derivation records; the sdk.Payment remittance carries
// the raw bytes. They must describe the same prefix/suffix — the storage layer
// re-derives the locking script from the remittance and records the derivation
// so the coin is spendable.
var (
	demoDerivationPrefix = []byte("arcade-toolbox-demo-prefix")
	demoDerivationSuffix = []byte("arcade-toolbox-demo-suffix")
)

// Env is a ready-to-use in-process wallet plus the mock services backing it.
type Env struct {
	// Wallet is the BRC-100 wallet under test.
	Wallet *wallet.Wallet
	// Provider is the storage backend the wallet writes through.
	Provider *storage.Provider
	// Arcade is the mock transaction oracle; inspect Broadcasts()/emit SSE events.
	Arcade *mockarcade.Arcade
	// ChainTracks is the mock headers source; register synthetic proven headers.
	ChainTracks *mockarcade.ChainTracks
	// RecipientHex is the wallet's identity public key (DER hex) — its receive key.
	RecipientHex string
	// Logger is the process logger (warn level, to keep demo output readable).
	Logger *slog.Logger

	closeFns []func()
}

// Setup builds the whole stack and returns it ready to use. Call Env.Close when
// done to shut down the mock servers and remove the temporary SQLite database.
func Setup(ctx context.Context) (*Env, error) {
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
	env := &Env{Logger: logger}

	// The two external services, as in-process HTTP/SSE mocks.
	arc, closeArc := mockarcade.NewArcadeServer()
	ct, closeCT := mockarcade.NewChainTracksServer()
	env.Arcade, env.ChainTracks = arc, ct
	env.closeFns = append(env.closeFns, closeArc, closeCT)

	// The production clients, pointed at the mocks. Swap these URLs for a real
	// Arcade deployment in production.
	oracle := arcade.New(logger, nil, defs.Arcade{Enabled: true, URL: arc.URL(), EventsURL: arc.URL()})
	hdrs, err := headers.New(logger, defs.ChainTracks{Enabled: true, URL: ct.URL()})
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("build headers client: %w", err)
	}

	// SQLite storage in a temp dir (Mode A: metastore + utxostore share one file).
	dir, err := os.MkdirTemp("", "arcade-toolbox-example-*")
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("temp dir: %w", err)
	}
	env.closeFns = append(env.closeFns, func() { _ = os.RemoveAll(dir) })

	provider, closeProv, err := perfprovider.New(ctx, logger, perfprovider.Config{
		Backend:     perfprovider.BackendSQLite,
		SQLitePath:  filepath.Join(dir, "wallet.db"),
		Network:     Network,
		StorageName: "example-storage",
	}, oracle, hdrs)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("build storage provider: %w", err)
	}
	env.Provider = provider
	env.closeFns = append(env.closeFns, func() { _ = closeProv(context.Background()) })

	// Create the schema + settings row. Do this once per storage instance.
	if _, err := provider.Migrate(ctx, "example-storage", "example-storage-identity-key"); err != nil {
		env.Close()
		return nil, fmt.Errorf("migrate storage: %w", err)
	}

	// The wdk.Services compatibility shim over the lean arcade+headers contracts.
	svc := services.New(logger, oracle, hdrs, defs.DefaultServicesConfig(Network))

	w, err := wallet.New(
		Network, WalletKeyHex, provider,
		wallet.WithServices(svc),
		wallet.WithLogger(logger),
	)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("build wallet: %w", err)
	}
	env.Wallet = w

	// The wallet binds storage lazily on first call. GetPublicKey(IdentityKey)
	// forces that bind and hands back the wallet's receive (identity) key.
	pub, err := w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, Originator)
	if err != nil {
		env.Close()
		return nil, fmt.Errorf("get identity key: %w", err)
	}
	env.RecipientHex = pub.PublicKey.ToDERHex()

	return env, nil
}

// Close shuts everything down. It is safe to call once.
func (e *Env) Close() {
	for i := len(e.closeFns) - 1; i >= 0; i-- {
		e.closeFns[i]()
	}
	e.closeFns = nil
}

// SeedFunds funds the demo wallet with sats by internalizing a mined BRC-29
// payment from the faucet sender. It registers the payment's (single-leaf)
// merkle root at the given block height in the mock ChainTracks so the real
// headers client validates the proof — the header-verified trust anchor,
// exercised end to end. It returns the seed transaction's id.
//
// This is a demo faucet. In production a wallet is funded by receiving a real
// external payment (see examples/internalize), not by minting its own.
func (e *Env) SeedFunds(ctx context.Context, sats uint64, height uint32) (string, error) {
	atomicBEEF, root, txid, err := buildMinedWalletPaymentBEEF(e.RecipientHex, sats, height)
	if err != nil {
		return "", err
	}
	e.ChainTracks.RegisterHeader(height, root)

	senderPriv, err := ec.PrivateKeyFromHex(senderKeyHex)
	if err != nil {
		return "", err
	}

	res, err := e.Wallet.InternalizeAction(ctx, sdk.InternalizeActionArgs{
		Tx: atomicBEEF,
		Outputs: []sdk.InternalizeOutput{{
			OutputIndex: 0,
			Protocol:    sdk.InternalizeProtocolWalletPayment,
			PaymentRemittance: &sdk.Payment{
				DerivationPrefix:  demoDerivationPrefix,
				DerivationSuffix:  demoDerivationSuffix,
				SenderIdentityKey: senderPriv.PubKey(),
			},
		}},
		Description: "example seed funds",
		Labels:      []string{"faucet"},
	}, Originator)
	if err != nil {
		return "", fmt.Errorf("internalize seed payment: %w", err)
	}
	if !res.Accepted {
		return "", fmt.Errorf("seed payment was not accepted")
	}
	return txid, nil
}

// buildMinedWalletPaymentBEEF builds a single-transaction, single-leaf-proven
// atomic BEEF whose sole output is a BRC-29 payment to recipientHex from the
// faucet sender key. Because the merkle path has one leaf marked as the txid,
// the computed merkle root equals the txid, so registering that root at height
// in the mock ChainTracks makes the real headers client validate the BUMP.
func buildMinedWalletPaymentBEEF(recipientHex string, sats uint64, height uint32) ([]byte, chainhash.Hash, string, error) {
	keyID := brc29.KeyID{
		DerivationPrefix: base64.StdEncoding.EncodeToString(demoDerivationPrefix),
		DerivationSuffix: base64.StdEncoding.EncodeToString(demoDerivationSuffix),
	}
	lockingScript, err := brc29.LockForCounterparty(brc29.PrivHex(senderKeyHex), keyID, brc29.PubHex(recipientHex))
	if err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("derive BRC-29 locking script: %w", err)
	}

	tx := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = 0x11
	tx.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	tx.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: lockingScript})

	txidHash := tx.TxID()
	trueVal := true
	mp := transaction.NewMerklePath(height, [][]*transaction.PathElement{
		{{Offset: 0, Hash: txidHash, Txid: &trueVal}},
	})
	if err := tx.AddMerkleProof(mp); err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("add merkle proof: %w", err)
	}

	computedRoot, err := mp.ComputeRoot(txidHash)
	if err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("compute merkle root: %w", err)
	}

	beef := transaction.NewBeefV2()
	if _, err := beef.MergeTransaction(tx); err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("merge into BEEF: %w", err)
	}
	atomicBEEF, err := beef.AtomicBytes(txidHash)
	if err != nil {
		return nil, chainhash.Hash{}, "", fmt.Errorf("encode atomic BEEF: %w", err)
	}
	return atomicBEEF, *computedRoot, txidHash.String(), nil
}
