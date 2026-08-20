package conformance

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// providedInputBasket holds the coin these subtests hand to CreateAction as an
// EXPLICIT input. It is deliberately not the change basket: the funder only
// claims from the change basket, so a coin parked here can become reserved for
// exactly one reason — the provided-input path reserved it — and an assertion
// about its state cannot be satisfied by accident.
const providedInputBasket = "provided-inputs"

// providedInputExclusivity is the audit's P0-1 acceptance test, for any backend.
//
// A caller-provided input used to be READ (resolved for its satoshis and its
// locking script) and then left in the open: nothing held it. Two CreateActions
// naming the same wallet-owned outpoint both succeeded, and the wallet handed
// one coin to two transactions. Nothing downstream could catch it, because
// neither action was ever wrong about anything it could observe — they were
// each told the coin was theirs. The first broadcast to reach a miner wins and
// the second is a double spend of the wallet's own making, which is the same
// class of failure the abort fence (P0-3) exists to prevent, arriving through a
// different door.
//
// So the property is not "CreateAction reserves something". It is that a named
// coin is EXCLUSIVE — exactly one of two racing actions may have it, the loser
// is told why in terms it can act on, and the coin stays held afterwards rather
// than merely appearing to have been contested.
func (s *suite) providedInputExclusivity(t *testing.T) {
	t.Run("Concurrent", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		// vout 0 is the contested coin; vouts 1 and 2 are one funding coin per
		// racer, so a loss can only be attributed to the shared named input and
		// never to the two starving each other of change.
		atomic, txid := BuildMinedAtomicBEEF(t, 0x70, 870_000, 20_000, 100_000, 100_000)
		_, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
			Tx: primitives.ExplicitByteArray(atomic),
			Outputs: []*wdk.InternalizeOutput{
				BasketInsertionOutput(0, providedInputBasket),
				WalletPaymentOutput(1, sender),
				WalletPaymentOutput(2, sender),
			},
		})
		require.NoError(t, err)
		assertSpendable(t, p, auth, txid, 0, true, "the named coin starts available")

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
				res, cerr := p.CreateAction(ctx, auth, ProvidedInputArgs(txid, 0, 40_000, nil))
				results[i] = outcome{res: res, err: cerr}
			}(i)
		}
		wg.Wait()

		var winner *wdk.StorageCreateActionResult
		for i, o := range results {
			if o.err == nil {
				require.Nil(t, winner,
					"worker %d and the other BOTH took the same named coin: the wallet has "+
						"promised one outpoint to two transactions", i)
				winner = o.res
				continue
			}
			// ErrInputUnavailable is the expected refusal. Contention is the one
			// other legal answer: an optimistic backend that lost a CAS and then
			// could not classify the loss says "ask me again" rather than
			// guessing, and that is a retryable non-answer, not a second winner.
			assert.True(t,
				errors.Is(o.err, wdk.ErrInputUnavailable) || errors.Is(o.err, wdk.ErrUTXOContention),
				"worker %d: the loser must name the unavailable input (or report retryable "+
					"contention), not fail opaquely: %v", i, o.err)
		}
		require.NotNil(t, winner, "neither CreateAction got the coin")

		// Held, not merely contested: the coin is unavailable afterwards, and the
		// winner's signing template says storage is a party to it.
		assertSpendable(t, p, auth, txid, 0, false,
			"the winner's named input must be reserved, exactly as a funded coin is")
		assertProvidedInput(t, winner, txid, 0)
	})

	t.Run("SpentThroughAccept", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		atomic, txid := BuildMinedAtomicBEEF(t, 0x71, 871_000, 20_000, 100_000)
		_, err := p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
			Tx: primitives.ExplicitByteArray(atomic),
			Outputs: []*wdk.InternalizeOutput{
				BasketInsertionOutput(0, providedInputBasket),
				WalletPaymentOutput(1, sender),
			},
		})
		require.NoError(t, err)

		res, err := p.CreateAction(ctx, auth, ProvidedInputArgs(txid, 0, 40_000, PlainBEEF(t, atomic)))
		require.NoError(t, err)
		assertProvidedInput(t, res, txid, 0)

		signed := BuildSignedTx(t, res)
		signedTxID := signed.TxID().String()
		_, err = p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
			IsNewTx:   true,
			Reference: strPtr(res.Reference),
			TxID:      txidPtr(signedTxID),
			RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
		})
		require.NoError(t, err)

		// The named input rode the whole reference through: pinned before the
		// broadcast and spent on acceptance, both whole-token operations that
		// never had to know a provided input from a funded one. The proof that it
		// is genuinely SPENT rather than merely unavailable is that the wallet
		// now refuses to lend it out again — and refuses for the right reason.
		assertSpendable(t, p, auth, txid, 0, false, "an accepted transaction's named input is gone")
		_, err = p.CreateAction(ctx, auth, ProvidedInputArgs(txid, 0, 10_000, nil))
		require.Error(t, err, "a spent coin must not be re-lendable as an explicit input")
		assert.ErrorIs(t, err, wdk.ErrInputUnavailable)
	})

	t.Run("ExternalInputUntouched", func(t *testing.T) {
		ctx := context.Background()
		p := s.freshProvider(t)
		sender := NewIdentityKey(t)
		auth := s.newAuth(t, p, NewIdentityKey(t))

		internalizeMinedPayment(t, p, auth, sender, 0x72, 100_000)

		// A coin this wallet does not own, known only through the BEEF the caller
		// supplies. It has no inventory row, so it has no exclusivity to enforce
		// and must not be dragged into the all-or-nothing hold — doing so would
		// refuse every externally-funded action with a not-found.
		external, extTxID := BuildMinedAtomicBEEF(t, 0x73, 873_000, 5_000)
		res, err := p.CreateAction(ctx, auth, ProvidedInputArgs(extTxID, 0, 40_000, PlainBEEF(t, external)))
		require.NoError(t, err, "an external provided input must not be refused")

		for _, in := range res.Inputs {
			if in.SourceTxID == extTxID && in.SourceVout == 0 {
				assert.Equal(t, wdk.ProvidedByYou, in.ProvidedBy,
					"an external input is provided by the caller alone")
				return
			}
		}
		t.Fatal("the external provided input is missing from the signing template")
	})
}

// assertProvidedInput asserts (txid, vout) appears in a CreateAction result as
// a jointly-provided input: the caller named it, and storage owns it.
func assertProvidedInput(t *testing.T, res *wdk.StorageCreateActionResult, txid string, vout uint32) {
	t.Helper()
	for _, in := range res.Inputs {
		if in.SourceTxID == txid && in.SourceVout == vout {
			assert.Equal(t, wdk.ProvidedByYouAndStorage, in.ProvidedBy)
			return
		}
	}
	t.Fatalf("the provided input %s:%d is missing from the signing template", txid, vout)
}

// ProvidedInputArgs is [PaymentArgs] with (txid, vout) named as an explicit
// caller-provided input, optionally carrying the input BEEF that resolves it.
func ProvidedInputArgs(txid string, vout uint32, pay uint64, inputBEEF []byte) wdk.ValidCreateActionArgs {
	args := PaymentArgs(pay)
	// P2PKH unlocking-script estimate: the caller declares the length it will
	// sign to, so the fee the action commits to covers the signed size.
	unlockLen := primitives.PositiveInteger(107)
	args.Inputs = []wdk.ValidCreateActionInput{{
		Outpoint:              wdk.OutPoint{TxID: txid, Vout: vout},
		InputDescription:      "caller-provided coin",
		UnlockingScriptLength: &unlockLen,
	}}
	args.InputBEEF = primitives.BEEF(inputBEEF)
	return args
}

// PlainBEEF re-encodes atomic BEEF bytes (what [BuildMinedAtomicBEEF] returns,
// and what InternalizeAction takes) as ordinary BEEF, which is the shape
// ValidCreateActionArgs.InputBEEF expects.
func PlainBEEF(t testing.TB, atomicBEEF []byte) []byte {
	t.Helper()
	beef, _, err := transaction.NewBeefFromAtomicBytes(atomicBEEF)
	require.NoError(t, err)
	b, err := beef.Bytes()
	require.NoError(t, err)
	return b
}
