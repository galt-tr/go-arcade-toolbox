package storage_test

// End-to-end write-path test: the first time the whole stack runs as a usable
// BRC-100 wallet. It wires wallet.New over a real storage.Provider whose oracle
// is the REAL arcade.Client (→ mockarcade) and whose header source is the REAL
// headers.Client (→ mock chaintracks). No fakes on the trust-critical seams; no
// containers (memstore + SQLite temp file + httptest), so it runs untagged.
//
// SCOPE (M3): this exercises the synchronous write path — InternalizeAction of a
// mined payment (the header-verified-proof trust anchor), CreateAction (which,
// for a fully storage-funded payment, signs and processes internally with REAL
// BRC-29 signatures verified by the provider's real scripts verifier), broadcast
// through the real arcade client, plus the noSend and delayed variants.
//
// DEFERRED to M4 (monitor): the SSE-driven MINED promotion of the wallet's OWN
// sent transaction. That requires the monitor consuming arcade's status SSE
// stream, which does not exist yet. Here the header-verified-proof trust anchor
// is instead proven through the InternalizeAction-of-a-mined-tx path: a real
// synthetic BUMP validated by the real headers.Client against the mock
// chaintracks (positive), and rejected when the root is not registered
// (negative) — proving VerifyMerkleRoot is genuinely consulted end to end.

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/go-softwarelab/common/pkg/to"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv/mockarcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/brc29"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/fixtures"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/services"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wallet"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

const (
	e2eNet           = defs.NetworkTestnet
	e2eWalletKeyHex  = "0000000000000000000000000000000000000000000000000000000000000001"
	e2eSenderKeyHex  = "0000000000000000000000000000000000000000000000000000000000000002"
	e2eStorageIDKey  = "e2e-storage-identity-key"
	e2eBlockHeight   = uint32(800000)
	e2eOriginator    = fixtures.DefaultOriginator
	e2eSeedSatoshis  = uint64(100000)
	e2ePaymentAmount = uint64(1000)
)

type e2eStack struct {
	t            *testing.T
	wallet       *wallet.Wallet
	provider     *storage.Provider
	arc          *mockarcade.Arcade
	ct           *mockarcade.ChainTracks
	recipientHex string

	// Exposed for the M4 monitor SSE-MINED e2e (monitor_e2e_test.go): the real
	// arcade oracle + chaintracks subscriber to drive a monitor.Daemon, and the
	// underlying stores for direct proof/tier assertions.
	oracle   arcade.TxOracle
	chainSub headers.ChainSubscriber
	meta     *metastore.Store
	utxo     utxostore.Store
}

func newE2EStack(t *testing.T, extraOpts ...storage.Option) *e2eStack {
	t.Helper()
	ctx := context.Background()
	logger := logging.NewTestLogger(t)

	arc := mockarcade.NewArcade(t)
	ct := mockarcade.NewChainTracks(t)

	oracle := arcade.New(logger, nil, defs.Arcade{Enabled: true, URL: arc.URL(), EventsURL: arc.URL()})
	hdrs, err := headers.New(logger, defs.ChainTracks{Enabled: true, URL: ct.URL()})
	require.NoError(t, err)

	meta, err := metastore.OpenSQLite(ctx, filepath.Join(t.TempDir(), "meta.db"))
	require.NoError(t, err)
	utxo := memstore.New()
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())

	provider, err := storage.New(logger, meta, utxo, fnd, oracle, hdrs,
		append([]storage.Option{
			storage.WithNetwork(e2eNet),
			storage.WithStorageName("e2e-storage"),
		}, extraOpts...)...,
	)
	require.NoError(t, err)
	_, err = provider.Migrate(ctx, "e2e-storage", e2eStorageIDKey)
	require.NoError(t, err)

	svc := services.New(logger, oracle, hdrs, defs.DefaultServicesConfig(e2eNet))

	w, err := wallet.New(e2eNet, e2eWalletKeyHex, provider,
		wallet.WithServices(svc),
		wallet.WithLogger(logger),
	)
	require.NoError(t, err)

	// The wallet lazily binds storage (MakeAvailable/FindOrInsertUser) on first
	// call; GetPublicKey(IdentityKey) forces auth + gives us the recipient key.
	pub, err := w.GetPublicKey(ctx, sdk.GetPublicKeyArgs{IdentityKey: true}, e2eOriginator)
	require.NoError(t, err)

	return &e2eStack{
		t: t, wallet: w, provider: provider, arc: arc, ct: ct,
		recipientHex: pub.PublicKey.ToDERHex(),
		oracle:       oracle, chainSub: hdrs, meta: meta, utxo: utxo,
	}
}

// buildMinedWalletPaymentBEEF builds a single-transaction, single-leaf-proven
// atomic BEEF whose sole output is a BRC-29 payment to recipientHex from the
// e2e sender key. Because the merkle path has one leaf marked as the txid, the
// computed merkle root equals the txid — so registering that root at height in
// the mock chaintracks makes the real headers.Client validate the BUMP.
func buildMinedWalletPaymentBEEF(t *testing.T, recipientHex string, sats uint64, height uint32) (atomicBEEF []byte, root chainhash.Hash, txid string) {
	t.Helper()

	keyID := brc29.KeyID{DerivationPrefix: fixtures.DerivationPrefix, DerivationSuffix: fixtures.DerivationSuffix}
	lockingScript, err := brc29.LockForCounterparty(brc29.PrivHex(e2eSenderKeyHex), keyID, brc29.PubHex(recipientHex))
	require.NoError(t, err)

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
	require.NoError(t, tx.AddMerkleProof(mp))

	computedRoot, err := mp.ComputeRoot(txidHash)
	require.NoError(t, err)

	beef := transaction.NewBeefV2()
	_, err = beef.MergeTransaction(tx)
	require.NoError(t, err)
	atomic, err := beef.AtomicBytes(txidHash)
	require.NoError(t, err)
	return atomic, *computedRoot, txidHash.String()
}

// authID resolves the storage auth for the wallet's identity (for direct
// provider-level assertions such as output spendability).
func (s *e2eStack) authID(ctx context.Context) wdk.AuthID {
	resp, err := s.provider.FindOrInsertUser(ctx, s.recipientHex)
	require.NoError(s.t, err)
	return wdk.AuthID{IdentityKey: s.recipientHex, UserID: &resp.User.UserID}
}

// outputSpendable directly reads the Spendable flag of the output at (txid, 0)
// via the provider — a direct check, not inferred from balance.
func (s *e2eStack) outputSpendable(ctx context.Context, txid string) bool {
	vout := uint32(0)
	outs, err := s.provider.FindOutputsAuth(ctx, s.authID(ctx), wdk.FindOutputsArgs{TxID: &txid, Vout: &vout})
	require.NoError(s.t, err)
	require.Len(s.t, outs, 1, "expected exactly one output at %s.0", txid)
	return outs[0].Spendable
}

// seedMinedPayment funds the wallet with sats via InternalizeAction of a mined
// BRC-29 payment. It registers the BUMP's merkle root at height so the real
// headers.Client validates the proof — this is the header-verified trust anchor
// exercised end to end through the real arcade+headers clients.
func (s *e2eStack) seedMinedPayment(sats uint64, height uint32) (seedTxid string) {
	s.t.Helper()
	ctx := context.Background()

	atomicBEEF, root, txid := buildMinedWalletPaymentBEEF(s.t, s.recipientHex, sats, height)
	s.ct.RegisterHeader(height, root)

	senderPriv, err := ec.PrivateKeyFromHex(e2eSenderKeyHex)
	require.NoError(s.t, err)

	res, err := s.wallet.InternalizeAction(ctx, sdk.InternalizeActionArgs{
		Tx: atomicBEEF,
		Outputs: []sdk.InternalizeOutput{{
			OutputIndex: 0,
			Protocol:    sdk.InternalizeProtocolWalletPayment,
			PaymentRemittance: &sdk.Payment{
				DerivationPrefix:  fixtures.DerivationPrefixBytes,
				DerivationSuffix:  fixtures.DerivationSuffixBytes,
				SenderIdentityKey: senderPriv.PubKey(),
			},
		}},
		Description: "e2e seed funds",
		Labels:      []string{"faucet"},
	}, e2eOriginator)
	require.NoError(s.t, err, "internalize of the mined payment must succeed (trust anchor, positive)")
	require.True(s.t, res.Accepted)
	return txid
}

// TestE2E_WritePath_Broadcast is the M3 payoff: seed → create+sign+process →
// broadcast, asserting the broadcast hit the real arcade server, the tx is
// unproven, the input was spent, change was minted, and the balance moved.
func TestE2E_WritePath_Broadcast(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	seedTxid := s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	balanceBefore, err := s.wallet.Balance(ctx)
	require.NoError(t, err)
	require.Equal(t, e2eSeedSatoshis, balanceBefore, "seeded balance should equal the mined payment amount")

	// Direct check: the seeded coin is spendable before we spend it.
	require.True(t, s.outputSpendable(ctx, seedTxid), "seed coin must be spendable before the payment")

	require.Zero(t, s.arc.BroadcastCount(), "no broadcast before CreateAction")

	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "e2e payment"
		a.Labels = []string{"e2e"}
		a.Outputs[0].Satoshis = e2ePaymentAmount
	})
	res, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err, "create+sign+process of a storage-funded payment must succeed (real BRC-29 signing)")
	require.NotNil(t, res)
	require.NotNil(t, res.Txid, "a broadcast action returns its txid")

	// Broadcast hit mockarcade: exactly one POST /tx carrying a non-empty EF.
	broadcasts := s.arc.Broadcasts()
	require.Len(t, broadcasts, 1, "exactly one broadcast should have hit the arcade server")
	require.NotEmpty(t, broadcasts[0].EF, "the POST /tx body (the Extended Format tx) must be non-empty")

	// Tx status is unproven (broadcast accepted, not yet mined).
	actions, err := s.wallet.ListActions(ctx, sdk.ListActionsArgs{Labels: []string{"e2e"}}, e2eOriginator)
	require.NoError(t, err)
	require.Equal(t, uint32(1), actions.TotalActions)
	require.Equal(t, string(wdk.TxStatusUnproven), string(actions.Actions[0].Status), "status should be unproven after broadcast")

	// Exact value accounting, derived from the ACTUAL broadcast transaction (not
	// a hard-coded fee): the sole input is the seed coin, so fee = seed - outputs
	// and the minted change = outputs - payment. The wallet's spendable balance
	// must equal that change exactly, and value must be conserved.
	tx, err := transaction.NewTransactionFromBEEF(res.Tx)
	require.NoError(t, err)
	require.Len(t, tx.Inputs, 1, "the payment should be funded by the single seed coin")
	var totalOut uint64
	for _, o := range tx.Outputs {
		totalOut += o.Satoshis
	}
	require.Less(t, totalOut, e2eSeedSatoshis, "outputs must be less than the input (a positive fee)")
	fee := e2eSeedSatoshis - totalOut
	expectedChange := totalOut - e2ePaymentAmount
	require.Positive(t, fee, "fee must be positive")

	balanceAfter, err := s.wallet.Balance(ctx)
	require.NoError(t, err)
	require.Equal(t, expectedChange, balanceAfter, "spendable balance must equal seed - payment - fee exactly")
	require.Equal(t, e2eSeedSatoshis, balanceAfter+e2ePaymentAmount+fee, "value conservation: seed == change + payment + fee")

	// Direct check: the seeded input flipped to Spendable=false (it was spent).
	require.False(t, s.outputSpendable(ctx, seedTxid), "the seeded input must flip to Spendable=false after being spent")

	// The minted change output is present and spendable in the change basket.
	outputs, err := s.wallet.ListOutputs(ctx, sdk.ListOutputsArgs{Basket: wdk.BasketNameForChange}, e2eOriginator)
	require.NoError(t, err)
	require.NotEmpty(t, outputs.Outputs, "the change output should be listed")
	spendable := 0
	for _, o := range outputs.Outputs {
		if o.Spendable {
			spendable++
		}
	}
	require.Positive(t, spendable, "at least one spendable change output should be minted back to the wallet")
}

// TestE2E_WritePath_TwoStepSignAction exercises the explicit two-step write
// path — the core SignAction method, not the single-call signAndProcess shortcut.
// CreateAction with signAndProcess=false returns a signable transaction and does
// NOT broadcast; a subsequent SignAction (all inputs storage-managed, so no
// client spends) signs, processes, and broadcasts through the real arcade client.
// This drives the pending-sign-actions repository and the sign_action / mapping
// paths that the single-step case skips.
func TestE2E_WritePath_TwoStepSignAction(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	seedTxid := s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	balanceBefore, err := s.wallet.Balance(ctx)
	require.NoError(t, err)
	require.Equal(t, e2eSeedSatoshis, balanceBefore)

	// Step 1: CreateAction with signAndProcess=false → signable tx, no broadcast.
	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "e2e two-step payment"
		a.Labels = []string{"e2e-2step"}
		a.Outputs[0].Satoshis = e2ePaymentAmount
		a.Options.SignAndProcess = to.Ptr(false)
	})
	createRes, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err)
	require.NotNil(t, createRes.SignableTransaction, "signAndProcess=false must return a signable transaction")
	require.NotEmpty(t, createRes.SignableTransaction.Reference, "the signable transaction carries a reference for SignAction")
	require.Zero(t, s.arc.BroadcastCount(), "CreateAction(signAndProcess=false) must NOT broadcast")

	// Step 2: SignAction with the returned reference (no client spends — the sole
	// input is storage-managed) → sign + process → broadcast.
	signRes, err := s.wallet.SignAction(ctx, sdk.SignActionArgs{
		Reference: createRes.SignableTransaction.Reference,
	}, e2eOriginator)
	require.NoError(t, err, "SignAction must sign the storage-managed input and process")
	require.NotNil(t, signRes)
	require.NotNil(t, signRes.Txid, "SignAction returns the processed txid")

	// Broadcast hit arcade during SignAction (not during CreateAction).
	broadcasts := s.arc.Broadcasts()
	require.Len(t, broadcasts, 1, "SignAction must broadcast exactly once through the real arcade client")
	require.NotEmpty(t, broadcasts[0].EF)

	// Status unproven + change minted + seed input spent.
	actions, err := s.wallet.ListActions(ctx, sdk.ListActionsArgs{Labels: []string{"e2e-2step"}}, e2eOriginator)
	require.NoError(t, err)
	require.Equal(t, uint32(1), actions.TotalActions)
	require.Equal(t, string(wdk.TxStatusUnproven), string(actions.Actions[0].Status))

	balanceAfter, err := s.wallet.Balance(ctx)
	require.NoError(t, err)
	require.Less(t, balanceAfter, balanceBefore, "balance must move after the two-step sign+process")
	require.Positive(t, balanceAfter, "change should be minted back to the wallet")
	require.False(t, s.outputSpendable(ctx, seedTxid), "the seeded input must flip to Spendable=false after SignAction spends it")
}

// TestE2E_WritePath_NoSend proves the noSend path commits locally without
// broadcasting: the arcade server sees zero POST /tx.
func TestE2E_WritePath_NoSend(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "e2e noSend payment"
		a.Outputs[0].Satoshis = e2ePaymentAmount
		a.Options.NoSend = to.Ptr(true)
	})
	res, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotEmpty(t, res.NoSendChange, "a noSend action returns its noSendChange outpoints")
	require.Zero(t, s.arc.BroadcastCount(), "noSend must NOT broadcast")
}

// TestE2E_WritePath_Delayed proves the delayed path defers the broadcast: the
// tx is created and committed but NOT broadcast synchronously — the monitor
// (M4) sends it later, so the arcade server sees zero POST /tx here.
func TestE2E_WritePath_Delayed(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "e2e delayed payment"
		a.Outputs[0].Satoshis = e2ePaymentAmount
		a.Options.AcceptDelayedBroadcast = to.Ptr(true)
	})
	res, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Txid)
	// DEFERRED to M4: with no monitor running, a delayed broadcast is queued but
	// never sent, so no POST /tx reaches arcade in M3.
	require.Zero(t, s.arc.BroadcastCount(), "delayed broadcast is deferred to the M4 monitor")
}

// TestE2E_WritePath_ImmediateBroadcastOverridesDelayed is the counterpart to the
// delayed test: with storage.WithImmediateBroadcast() the provider must ignore
// acceptDelayedBroadcast=true and send synchronously, so the exact same delayed
// CreateAction reaches arcade with a POST /tx before the call returns (no
// dependency on the monitor's SendWaiting task).
func TestE2E_WritePath_ImmediateBroadcastOverridesDelayed(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t, storage.WithImmediateBroadcast())
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "e2e immediate-broadcast payment"
		a.Outputs[0].Satoshis = e2ePaymentAmount
		a.Options.AcceptDelayedBroadcast = to.Ptr(true) // caller asks to delay...
	})
	res, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.NotNil(t, res.Txid)

	// ...but immediate mode wins: exactly one POST /tx carrying a non-empty EF
	// reached arcade synchronously.
	require.Equal(t, 1, s.arc.BroadcastCount(),
		"immediate mode must broadcast synchronously despite acceptDelayedBroadcast=true")
	require.NotEmpty(t, s.arc.Broadcasts()[0].EF)
}

// TestE2E_StateReport verifies the observability read surfaced to the dashboard:
// after seeding a mined payment, StateReport shows it in the "default" basket's
// mined tier (count + sats), tiers ordered sending → unproven → mined, and the
// status maps initialized. It resolves the user from the identity key alone (no
// pre-resolved numeric UserID), which is what a UI caller has.
func TestE2E_StateReport(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	rep, err := s.provider.StateReport(ctx, wdk.AuthID{IdentityKey: s.recipientHex}, []string{"default"})
	require.NoError(t, err)
	require.Len(t, rep.Baskets, 1)
	require.Equal(t, "default", rep.Baskets[0].Basket)

	tiers := rep.Baskets[0].Tiers
	require.Equal(t, []string{"sending", "unproven", "mined"},
		[]string{tiers[0].Tier, tiers[1].Tier, tiers[2].Tier}, "tiers ordered sending → unproven → mined")

	mined := tiers[2]
	require.GreaterOrEqual(t, mined.ClaimableCount, 1, "the seeded mined coin is counted in the mined tier")
	require.GreaterOrEqual(t, mined.ClaimableSats, e2eSeedSatoshis)

	require.NotNil(t, rep.TxStatuses)
	require.NotNil(t, rep.ArcadeStatuses)

	// A bad identity key is an authorization error, not a silent empty report.
	_, err = s.provider.StateReport(ctx, wdk.AuthID{IdentityKey: "not-a-user"}, []string{"default"})
	require.ErrorIs(t, err, storage.ErrAuthorization)
}

// assertBasketCoins asserts a (user, basket) holds exactly count claimable coins
// summing to count*denom sats — i.e. count coins of exactly denom each.
func assertBasketCoins(t *testing.T, ctx context.Context, store utxostore.Store, userID int64, basket string, count int, denom uint64) {
	t.Helper()
	bal, err := store.Balance(ctx, userID, basket)
	require.NoError(t, err)
	gotCount := 0
	for _, c := range bal.ClaimableCount {
		gotCount += c
	}
	var gotSats uint64
	for _, s := range bal.Claimable {
		gotSats += s
	}
	require.Equalf(t, count, gotCount, "basket %q claimable coin count", basket)
	require.Equalf(t, uint64(count)*denom, gotSats, "basket %q claimable sats", basket)
}

// TestE2E_FanOutFuel_BootstrapsPool drives the throughput FuelKeeper's two-stage
// fan-out END TO END through the wallet (not a pre-seeded pool): a chunk fan-out
// mints reserve coins funded from the default deposit, then a leaf fan-out mints
// pool coins funded from the reserve — proving storage now implements FuelShape
// shaped-change minting (funds from the source basket, mints Count coins of
// exactly Satoshis into the destination basket, ClaimExact-selectable).
func TestE2E_FanOutFuel_BootstrapsPool(t *testing.T) {
	ctx := context.Background()
	tp := defs.UTXOManagement{Strategy: defs.StrategyThroughput, Throughput: defs.DefaultUTXOManagement().Throughput}
	// Immediate broadcast so each fan-out promotes its minted coins
	// sending→unproven, making them spendable by the next stage.
	s := newE2EStack(t, storage.WithUTXOManagement(tp), storage.WithImmediateBroadcast())
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)
	userID := int64(*s.authID(ctx).UserID)

	// Stage 1 — chunk fan-out: fund from the default deposit, mint into reserve.
	const chunkCount, chunkDenom = 2, 20000
	_, err := s.wallet.FanOutFuel(ctx, wdk.ShapedChange{Count: chunkCount, Satoshis: chunkDenom, Basket: "reserve"}, e2eOriginator)
	require.NoError(t, err, "chunk fan-out must fund from the default basket and mint reserve coins")
	assertBasketCoins(t, ctx, s.utxo, userID, "reserve", chunkCount, chunkDenom)

	// Stage 2 — leaf fan-out: fund from the reserve, mint into the fuel pool.
	const leafCount, leafDenom = 5, 1000
	_, err = s.wallet.FanOutFuel(ctx, wdk.ShapedChange{Count: leafCount, Satoshis: leafDenom, Basket: "fuel"}, e2eOriginator)
	require.NoError(t, err, "leaf fan-out must fund from the reserve basket and mint pool coins")
	assertBasketCoins(t, ctx, s.utxo, userID, "fuel", leafCount, leafDenom)

	// The pool coins are selectable by exact denomination — the ClaimExact fast
	// path the throughput funder relies on.
	claimed, err := s.utxo.ClaimExact(ctx,
		utxostore.Scope{UserID: userID, Basket: "fuel", Tier: utxostore.TierUnproven}, "claim-fuel", leafDenom, leafCount)
	require.NoError(t, err)
	require.Len(t, claimed, leafCount, "all pool coins claimable by exact denomination")
	for _, u := range claimed {
		require.EqualValues(t, leafDenom, u.Satoshis)
	}
}

// TestE2E_InternalizeTrustAnchor_Negative proves the header-verified-proof trust
// anchor is genuinely consulted end to end: with the BUMP's merkle root NOT
// registered in the mock chaintracks, the real headers.Client's VerifyMerkleRoot
// returns false and InternalizeAction rejects the mined proof.
func TestE2E_InternalizeTrustAnchor_Negative(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)

	// Build a valid mined BEEF, but register a DELIBERATELY WRONG merkle root at
	// its height: the mock chaintracks serves a header, so the real headers.Client
	// fetches it and byte-compares — and the comparison FAILS. This proves
	// VerifyMerkleRoot actually ran the comparison end to end (not merely that a
	// fetch errored).
	atomicBEEF, realRoot, _ := buildMinedWalletPaymentBEEF(t, s.recipientHex, e2eSeedSatoshis, e2eBlockHeight)
	var wrongRoot chainhash.Hash
	for i := range wrongRoot {
		wrongRoot[i] = 0xff
	}
	require.NotEqual(t, realRoot, wrongRoot)
	s.ct.RegisterHeader(e2eBlockHeight, wrongRoot)

	senderPriv, err := ec.PrivateKeyFromHex(e2eSenderKeyHex)
	require.NoError(t, err)

	_, err = s.wallet.InternalizeAction(ctx, sdk.InternalizeActionArgs{
		Tx: atomicBEEF,
		Outputs: []sdk.InternalizeOutput{{
			OutputIndex: 0,
			Protocol:    sdk.InternalizeProtocolWalletPayment,
			PaymentRemittance: &sdk.Payment{
				DerivationPrefix:  fixtures.DerivationPrefixBytes,
				DerivationSuffix:  fixtures.DerivationSuffixBytes,
				SenderIdentityKey: senderPriv.PubKey(),
			},
		}},
		Description: "e2e unverifiable proof",
	}, e2eOriginator)
	require.Error(t, err, "a mismatched merkle root must fail verification through the real headers client")
	require.ErrorContains(t, err, "merkle proof", "the failure must be a merkle-root/header verification failure, not an unrelated error")
}
