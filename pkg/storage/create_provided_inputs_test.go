package storage

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// providedCoinBasket holds the seeded coin every test here hands to
// CreateAction as an EXPLICIT input. It is deliberately NOT the change basket:
// the funder only claims from the change basket, so a coin parked here can only
// become reserved because the provided-input path reserved it — which is the
// property under test. (The one exception is
// TestCreateAction_ProvidedInputBeatsFunder, which puts the coin in the change
// basket precisely so the two paths compete for it.)
const providedCoinBasket = "provided-coins"

// seedProvidedCoin internalizes one mined transaction crediting vout 0 as an
// ordinary wallet payment (change basket: the funder's money) and vout 1 into
// [providedCoinBasket] (a wallet-owned coin the funder will never claim). It
// returns the txid and the source transaction's PLAIN BEEF bytes, which a
// caller passes as ValidCreateActionArgs.InputBEEF when the action has to be
// processed afterwards.
//
// The metastore output row is what makes vout 1 "wallet-owned" as far as
// resolveProvidedInputs is concerned, and InternalizeAction is the only way to
// create the metastore row and the utxostore coin together through the public
// surface — h.mintFunding mints the coin alone, which reads as an EXTERNAL
// input on the provided-input path.
func (h *harness) seedProvidedCoin(t *testing.T, seed byte, fundSats, providedSats uint64) (txid string, inputBEEF []byte) {
	t.Helper()
	ctx := context.Background()

	atomic, txid := buildMinedAtomicBEEF(t, seed, 800000+uint32(seed), fundSats, providedSats)
	_, err := h.p.InternalizeAction(ctx, h.auth, wdk.InternalizeActionArgs{
		Tx: primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{
			{
				OutputIndex: 0,
				Protocol:    wdk.WalletPaymentProtocol,
				PaymentRemittance: &wdk.WalletPayment{
					DerivationPrefix:  "cHJlZml4",
					DerivationSuffix:  "c3VmZml4",
					SenderIdentityKey: testIdentityKey,
				},
			},
			{
				OutputIndex:         1,
				Protocol:            wdk.BasketInsertionProtocol,
				InsertionRemittance: &wdk.BasketInsertion{Basket: providedCoinBasket},
			},
		},
	})
	require.NoError(t, err)

	beef, _, err := transaction.NewBeefFromAtomicBytes(atomic)
	require.NoError(t, err)
	plain, err := beef.Bytes()
	require.NoError(t, err)
	return txid, plain
}

// providedInputArgs is [paymentArgs] with (txid, vout) named as an explicit
// caller-provided input.
func providedInputArgs(txid string, vout uint32, pay uint64) wdk.ValidCreateActionArgs {
	args := paymentArgs(pay)
	unlockLen := primitives.PositiveInteger(txDefaultUnlockLen)
	args.Inputs = []wdk.ValidCreateActionInput{{
		Outpoint:              wdk.OutPoint{TxID: txid, Vout: vout},
		InputDescription:      "caller-provided coin",
		UnlockingScriptLength: &unlockLen,
	}}
	return args
}

func outpointOf(t *testing.T, txid string, vout uint32) utxostore.Outpoint {
	t.Helper()
	h, err := chainhash.NewHashFromHex(txid)
	require.NoError(t, err)
	return utxostore.Outpoint{TxID: *h, Vout: vout}
}

// TestCreateAction_ProvidedInputReserved is the base fact the exclusivity
// property rests on: a wallet-owned coin the caller names explicitly comes out
// of CreateAction held under that action's reference, exactly like a coin the
// funder claimed.
func TestCreateAction_ProvidedInputReserved(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	txid, _ := h.seedProvidedCoin(t, 0x61, 100_000, 7_000)
	provided := outpointOf(t, txid, 1)

	res, err := h.p.CreateAction(ctx, h.auth, providedInputArgs(txid, 1, 40_000))
	require.NoError(t, err)

	u, err := h.utxo.Get(ctx, provided)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy,
		"a wallet-owned provided input must be reserved under the action's reference")
	assert.False(t, u.Pinned, "CreateAction reserves; only ProcessAction pins")

	// The template still describes it as jointly provided, unchanged.
	var found bool
	for _, in := range res.Inputs {
		if in.SourceTxID == txid && in.SourceVout == 1 {
			found = true
			assert.Equal(t, wdk.ProvidedByYouAndStorage, in.ProvidedBy)
		}
	}
	assert.True(t, found, "the provided input is missing from the signing template")
}

// TestCreateAction_ProvidedInputExclusivity is the audit's P0-1 acceptance
// test: two concurrent CreateActions naming the SAME wallet-owned outpoint as
// an explicit input. Before provided inputs were reserved, both succeeded and
// the wallet handed one coin to two transactions — a self-inflicted double
// spend that no later stage could catch, because neither action was ever wrong
// about anything it could see.
func TestCreateAction_ProvidedInputExclusivity(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	// Two funding coins so a loss can only be attributed to the shared provided
	// input, never to the racers starving each other of change.
	txid, _ := h.seedProvidedCoin(t, 0x62, 100_000, 7_000)
	h.mintFunding(t, 0x63, 100_000)
	provided := outpointOf(t, txid, 1)

	type outcome struct {
		res *wdk.StorageCreateActionResult
		err error
	}
	results := make([]outcome, 2)
	var wg sync.WaitGroup
	for i := range results {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := h.p.CreateAction(ctx, h.auth, providedInputArgs(txid, 1, 40_000))
			results[i] = outcome{res: res, err: err}
		}(i)
	}
	wg.Wait()

	var winner *wdk.StorageCreateActionResult
	var losers int
	for i, o := range results {
		if o.err == nil {
			require.Nil(t, winner, "both CreateActions took the same coin: %d and the other", i)
			winner = o.res
			continue
		}
		losers++
		assert.ErrorIs(t, o.err, wdk.ErrInputUnavailable,
			"the loser must say the named input is unavailable, not something opaque")
		var reserved *utxostore.ReservedError
		require.ErrorAs(t, o.err, &reserved,
			"the typed cause must survive the storage wrap so a caller can tell WHY")
		assert.Equal(t, provided, reserved.Op)
	}
	require.NotNil(t, winner, "neither CreateAction got the coin")
	require.Equal(t, 1, losers)

	u, err := h.utxo.Get(ctx, provided)
	require.NoError(t, err)
	assert.Equal(t, winner.Reference, u.ReservedBy, "the coin is held by the winner alone")
}

// TestCreateAction_ProvidedInputBeatsFunder is the same exclusivity across the
// two DIFFERENT paths that can take a coin: one action names it explicitly
// while a concurrent action's funder would claim it. The coin sits in the
// change basket and is the only one there, so both paths genuinely want it.
func TestCreateAction_ProvidedInputBeatsFunder(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	only := h.mintFunding(t, 0x64, 100_000)
	// mintFunding creates the coin but no metastore output row; the provided
	// path needs the row to call it wallet-owned, so internalize it instead.
	txid := h.internalizeMinedPayment(t, 0x65, 100_000)
	// Retire the mintFunding coin: exactly one claimable coin must exist.
	require.NoError(t, h.utxo.Freeze(ctx, []utxostore.Outpoint{only}))
	provided := outpointOf(t, txid, 0)

	errs := make([]error, 2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, errs[0] = h.p.CreateAction(ctx, h.auth, providedInputArgs(txid, 0, 40_000))
	}()
	go func() {
		defer wg.Done()
		_, errs[1] = h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	}()
	wg.Wait()

	var successes int
	for _, err := range errs {
		if err == nil {
			successes++
			continue
		}
		assert.True(t,
			errors.Is(err, wdk.ErrInputUnavailable) ||
				errors.Is(err, funder.ErrNotEnoughFunds) ||
				errors.Is(err, funder.ErrUTXOContention),
			"unexpected loser error shape: %v", err)
	}
	assert.Equal(t, 1, successes, "one coin, two claimants, exactly one winner")

	u, err := h.utxo.Get(ctx, provided)
	require.NoError(t, err)
	assert.NotEmpty(t, u.ReservedBy, "the winner holds the coin")
}

// TestCreateAction_ProvidedInputRefused covers the three states a named coin
// can be in that make it unavailable. Each must refuse immediately, with the
// typed cause intact, and leave NOTHING reserved — the reserve runs before the
// funder, so a refusal must not cost the caller a funding coin either.
func TestCreateAction_ProvidedInputRefused(t *testing.T) {
	var spender chainhash.Hash
	spender[0] = 0xEE

	for _, tc := range []struct {
		name   string
		arm    func(t *testing.T, h *harness, op utxostore.Outpoint)
		assert func(t *testing.T, err error, op utxostore.Outpoint)
	}{
		{
			name: "ReservedByAnother",
			arm: func(t *testing.T, h *harness, op utxostore.Outpoint) {
				require.NoError(t, h.utxo.ReserveOutpoints(context.Background(),
					int64(h.userID), "someone-elses-action", []utxostore.Outpoint{op}))
			},
			assert: func(t *testing.T, err error, op utxostore.Outpoint) {
				var e *utxostore.ReservedError
				require.ErrorAs(t, err, &e)
				assert.Equal(t, op, e.Op)
				assert.Equal(t, "someone-elses-action", e.HeldBy)
			},
		},
		{
			name: "Frozen",
			arm: func(t *testing.T, h *harness, op utxostore.Outpoint) {
				require.NoError(t, h.utxo.Freeze(context.Background(), []utxostore.Outpoint{op}))
			},
			assert: func(t *testing.T, err error, op utxostore.Outpoint) {
				var e *utxostore.FrozenError
				require.ErrorAs(t, err, &e)
				assert.Equal(t, op, e.Op)
			},
		},
		{
			name: "Spent",
			arm: func(t *testing.T, h *harness, op utxostore.Outpoint) {
				require.NoError(t, h.utxo.Spend(context.Background(), []*utxostore.SpendOp{{
					Outpoint:     op,
					Reservation:  "already-broadcast",
					SpendingTxID: spender,
				}}, true))
			},
			assert: func(t *testing.T, err error, op utxostore.Outpoint) {
				var e *utxostore.SpentError
				require.ErrorAs(t, err, &e)
				assert.Equal(t, op, e.Op)
				assert.Equal(t, spender, e.Winner)
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			h := newHarness(t)
			txid, _ := h.seedProvidedCoin(t, 0x66, 100_000, 7_000)
			provided := outpointOf(t, txid, 1)
			funding := outpointOf(t, txid, 0)
			tc.arm(t, h, provided)

			_, err := h.p.CreateAction(ctx, h.auth, providedInputArgs(txid, 1, 40_000))
			require.Error(t, err, "an unavailable named input must fail the action")
			assert.ErrorIs(t, err, wdk.ErrInputUnavailable)
			tc.assert(t, err, provided)

			// Nothing was funded and nothing is left held: the reserve refused
			// before the funder ever ran, and the Mode B insurance release (plus
			// the Mode A rollback) leaves no residue behind it.
			u, err := h.utxo.Get(ctx, funding)
			require.NoError(t, err)
			assert.Empty(t, u.ReservedBy, "the funding coin must not be held by a refused action")
		})
	}
}

// TestCreateAction_ExternalProvidedInputUntouched: an input the wallet does not
// own has no row in the inventory and therefore no exclusivity authority here.
// It must flow through exactly as before — resolved from the caller's BEEF,
// marked ProvidedByYou, and never handed to ReserveOutpoints (which would fail
// the whole action with a NotFound).
func TestCreateAction_ExternalProvidedInputUntouched(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	h.mintFunding(t, 0x67, 100_000)

	// A coin of somebody else's, known only through the BEEF the caller supplies.
	atomic, extTxID := buildMinedAtomicBEEF(t, 0x68, 800_068, 5_000)
	beef, _, err := transaction.NewBeefFromAtomicBytes(atomic)
	require.NoError(t, err)
	beefBytes, err := beef.Bytes()
	require.NoError(t, err)

	args := providedInputArgs(extTxID, 0, 40_000)
	args.InputBEEF = primitives.BEEF(beefBytes)

	res, err := h.p.CreateAction(ctx, h.auth, args)
	require.NoError(t, err, "an external provided input must not be refused")

	var found bool
	for _, in := range res.Inputs {
		if in.SourceTxID == extTxID && in.SourceVout == 0 {
			found = true
			assert.Equal(t, wdk.ProvidedByYou, in.ProvidedBy,
				"an external input is provided by the caller alone")
			assert.Equal(t, int64(5_000), in.SourceSatoshis)
		}
	}
	assert.True(t, found, "the external provided input is missing from the signing template")

	_, err = h.utxo.Get(ctx, outpointOf(t, extTxID, 0))
	assert.ErrorIs(t, err, &utxostore.NotFoundError{},
		"an external input must not have been minted or reserved")
}

// TestCreateAction_ProvidedInputSpentOnAccept walks a provided input the whole
// way: reserved at create, pinned once a broadcastable transaction exists, and
// spent under the action's own reference when arcade accepts it. All three
// stages are whole-TOKEN operations, so the provided input rides along with the
// funded coins for free — this test is what proves that claim rather than
// assuming it.
func TestCreateAction_ProvidedInputSpentOnAccept(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	txid, inputBEEF := h.seedProvidedCoin(t, 0x69, 100_000, 7_000)
	provided := outpointOf(t, txid, 1)

	args := providedInputArgs(txid, 1, 40_000)
	args.InputBEEF = primitives.BEEF(inputBEEF)
	res, err := h.p.CreateAction(ctx, h.auth, args)
	require.NoError(t, err)

	signed := buildSignedTx(t, res)
	signedTxID := signed.TxID().String()
	_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(signedTxID),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)

	u, err := h.utxo.Get(ctx, provided)
	require.NoError(t, err)
	require.NotNil(t, u.SpentBy, "the provided input must be spent when the transaction is accepted")
	assert.Equal(t, signedTxID, u.SpentBy.String())
}

// TestCreateAction_ProvidedInputPinnedWhenDelayed is the middle stage of the
// walk above, which the accepted path passes through too fast to observe: a
// delayed action stores broadcastable bytes and stops, and the provided input
// must be pinned by then or the stale-reservation sweep could free the inputs
// of a transaction that is still going to be sent.
func TestCreateAction_ProvidedInputPinnedWhenDelayed(t *testing.T) {
	ctx := context.Background()
	h := newHarness(t)
	txid, inputBEEF := h.seedProvidedCoin(t, 0x6a, 100_000, 7_000)
	provided := outpointOf(t, txid, 1)

	args := providedInputArgs(txid, 1, 40_000)
	args.InputBEEF = primitives.BEEF(inputBEEF)
	res, err := h.p.CreateAction(ctx, h.auth, args)
	require.NoError(t, err)

	signed := buildSignedTx(t, res)
	signedTxID := signed.TxID().String()
	_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		IsDelayed: true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(signedTxID),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)

	u, err := h.utxo.Get(ctx, provided)
	require.NoError(t, err)
	assert.True(t, u.Pinned, "the whole-token pin must cover the provided input too")
	assert.Equal(t, res.Reference, u.ReservedBy)
}
