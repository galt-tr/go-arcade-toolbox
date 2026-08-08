//go:build integration

package storage_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/aerostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// TestHybrid_ApplyStatusBatch_SeenThenMinedPrunes exercises the SSE-driven async
// status-apply path — Provider.ApplyStatusBatch — end to end on the split
// Aerospike (inventory) + PostgreSQL (metadata) "Mode B" hybrid, the one gap the
// existing hybrid conformance suite leaves (it covers create/process/abort/
// internalize but never the batched SEEN/MINED apply).
//
// It drives a real create → sign → process broadcast-accept (so the funding coin
// ends up SpentBy the payment tx and the change coin is minted), then feeds the
// provider the two status frames the monitor's apply pool would deliver:
//
//   - a SEEN_MULTIPLE_NODES batch → the tx stays unproven, the change is
//     spendable at TierUnproven, and the known tx's arcade_status advances;
//   - a MINED batch carrying a headers-verifiable single-leaf BUMP → the tx
//     completes, the proof is stored, the change is promoted to TierMined, the
//     spent funding input is PRUNED from the Aerospike inventory (RemoveSpentBy),
//     and both input_beef copies (known_txs + transactions) are dropped.
//
// Finally it re-applies the MINED batch to prove idempotency (a no-op).
//
// The Aerospike-level tier and pruning facts are not observable through the
// metastore-backed public provider surface (a pruned inventory row still has a
// descriptive metastore output row), so this test holds the metastore/utxostore
// handles directly to assert them — the same seam the sibling status_updates
// unit tests use. Everything else (balance, spendability, tx status, input_beef)
// goes through the public provider / metastore read surface.
//
// Requires a Postgres and an Aerospike container; testenv skips gracefully when
// no container runtime is available.
func TestHybrid_ApplyStatusBatch_SeenThenMinedPrunes(t *testing.T) {
	ctx := context.Background()
	pg := testenv.StartPostgres(t)
	aero := testenv.StartAerospike(t)

	// A zero-value FakeHeaders VERIFIES every merkle root, so the single-leaf
	// BUMP that minedRecord builds passes header verification and the proof is
	// stored (the mirror of the conformance suite's accept-everything wiring).
	p, meta, utxo := newHybridStatusStores(t, pg, aero, &conformance.FakeHeaders{})

	_, err := p.Migrate(ctx, "conformance", "conformance-identity-key")
	require.NoError(t, err)

	identityKey := conformance.NewIdentityKey(t)
	resp, err := p.FindOrInsertUser(ctx, identityKey)
	require.NoError(t, err)
	require.True(t, resp.IsNew)
	uid := resp.User.UserID
	auth := wdk.AuthID{IdentityKey: identityKey, UserID: &uid}

	// --- 2. Fund the wallet: internalize a mined P2PKH payment (100k). --------
	sender := conformance.NewIdentityKey(t)
	atomic, fundTxID := conformance.BuildMinedAtomicBEEF(t, 0x41, 900_100, 100_000)
	_, err = p.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{conformance.WalletPaymentOutput(0, sender)},
	})
	require.NoError(t, err)

	bal0, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, uint64(100_000), bal0, "seeded funds spendable")

	// --- 3. CreateAction → sign → ProcessAction (synchronous broadcast-accept).
	res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, res.Inputs, "at least the funding input was allocated")

	// Capture the spent input outpoints (the funding coin) and the change outpoint.
	inputOps := make([]utxostore.Outpoint, 0, len(res.Inputs))
	for _, in := range res.Inputs {
		inputOps = append(inputOps, utxostore.Outpoint{TxID: mustHash(t, in.SourceTxID), Vout: in.SourceVout})
	}
	fundOp := utxostore.Outpoint{TxID: mustHash(t, fundTxID), Vout: 0}
	require.Contains(t, inputOps, fundOp, "the funding coin is a spent input of the payment tx")

	signed := conformance.BuildSignedTx(t, res)
	txid := signed.TxID().String()
	ref := res.Reference
	txidHex := primitives.TXIDHexString(txid)
	out, err := p.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: &ref,
		TxID:      &txidHex,
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)
	require.Len(t, out.SendWithResults, 1)
	require.Equal(t, wdk.SendWithResultStatusUnproven, out.SendWithResults[0].Status, "broadcast accepted")

	changeTxID, changeVout := conformance.ChangeOutpoint(t, res, txid)
	require.Equal(t, txid, changeTxID)
	changeOp := utxostore.Outpoint{TxID: mustHash(t, txid), Vout: changeVout}

	// Post-process baseline: inputs recorded spent (still present in inventory),
	// change minted, known tx unconfirmed with input_beef and no arcade status.
	for _, op := range inputOps {
		u, gerr := utxo.Get(ctx, op)
		require.NoError(t, gerr, "spent input still present in inventory pre-mining")
		require.NotNil(t, u.SpentBy, "input recorded SpentBy the payment tx")
	}
	ktBefore, found, err := meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, ktBefore.Status)
	require.Nil(t, ktBefore.ArcadeStatus, "synchronous accept path does not record an arcade_status")
	require.NotEmpty(t, ktBefore.InputBEEF, "known tx carries input_beef before mining")
	txRowsBefore, err := meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	require.NoError(t, err)
	require.NotEmpty(t, txRowsBefore)
	require.NotEmpty(t, txRowsBefore[0].InputBEEF, "transactions row carries input_beef before mining")

	balBroadcast, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Positive(t, balBroadcast, "change spendable after acceptance")
	require.Less(t, balBroadcast, bal0, "spend + fee reduced the balance")

	// --- 4. SEEN_MULTIPLE_NODES batch → unproven, change at TierUnproven. -----
	require.NoError(t, p.ApplyStatusBatch(ctx, []arcade.TxRecord{
		{TxID: txid, Status: arcade.StatusSeenMultipleNodes},
	}))

	require.Equal(t, wdk.TxUpdateStatusBroadcasted, hybridTxStatus(t, p, auth, ref), "tx broadcasted/unproven after SEEN")
	require.True(t, hybridSpendable(t, p, auth, txid, changeVout), "change spendable after SEEN")

	changeU, err := utxo.Get(ctx, changeOp)
	require.NoError(t, err)
	require.Equal(t, utxostore.TierUnproven, changeU.Tier, "change promoted to TierUnproven by SEEN")

	ktSeen, found, err := meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusUnconfirmed, ktSeen.Status, "known tx unconfirmed")
	require.NotNil(t, ktSeen.ArcadeStatus, "SEEN recorded an arcade_status (known tx advanced)")
	require.Equal(t, string(arcade.StatusSeenMultipleNodes), *ktSeen.ArcadeStatus)

	// The spent input is NOT yet pruned (mining prunes it, not SEEN).
	for _, op := range inputOps {
		_, gerr := utxo.Get(ctx, op)
		require.NoError(t, gerr, "spent input still present after SEEN")
	}

	balSeen, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, balBroadcast, balSeen, "SEEN does not change the balance")

	// --- 5. MINED batch → completed, proof stored, TierMined, input pruned. ---
	const minedHeight = uint32(900_500)
	minedRec, _ := minedRecord(t, txid, minedHeight) // FakeHeaders verifies its root
	require.NoError(t, p.ApplyStatusBatch(ctx, []arcade.TxRecord{minedRec}))

	// (a) tx completed (public read + metastore).
	require.Equal(t, wdk.TxUpdateStatusMined, hybridTxStatus(t, p, auth, ref), "tx completed after MINED")
	ktMined, found, err := meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusCompleted, ktMined.Status)
	require.NotEmpty(t, ktMined.MerklePath, "BUMP stored")
	require.NotEmpty(t, ktMined.MerkleRoot, "verified root stored")
	require.NotNil(t, ktMined.BlockHeight)
	require.Equal(t, minedHeight, *ktMined.BlockHeight)
	require.NotNil(t, ktMined.ArcadeStatus)
	require.Equal(t, string(arcade.StatusMined), *ktMined.ArcadeStatus)

	// (b) change spendable at TierMined.
	require.True(t, hybridSpendable(t, p, auth, txid, changeVout), "change spendable after MINED")
	changeMinedU, err := utxo.Get(ctx, changeOp)
	require.NoError(t, err)
	require.Equal(t, utxostore.TierMined, changeMinedU.Tier, "change promoted to TierMined by MINED")
	balMined, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, balSeen, balMined, "promoting the change to mined does not change the balance")

	// (c) THE STAR: the spent funding input is gone from the Aerospike inventory.
	for _, op := range inputOps {
		_, gerr := utxo.Get(ctx, op)
		require.Error(t, gerr, "spent input pruned from inventory on mining")
		require.ErrorIs(t, gerr, &utxostore.NotFoundError{}, "pruned input reports NotFound")
	}

	// (d) input_beef dropped for the mined tx (both copies).
	require.Empty(t, ktMined.InputBEEF, "known_txs input_beef dropped by SetProof")
	txRowsMined, err := meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	require.NoError(t, err)
	require.NotEmpty(t, txRowsMined)
	require.Empty(t, txRowsMined[0].InputBEEF, "transactions input_beef cleared on mining")

	// --- 6. Idempotency: re-applying the same MINED batch is a no-op. ---------
	require.NoError(t, p.ApplyStatusBatch(ctx, []arcade.TxRecord{minedRec}), "re-apply is a no-op")

	require.Equal(t, wdk.TxUpdateStatusMined, hybridTxStatus(t, p, auth, ref), "still completed after re-apply")
	changeReU, err := utxo.Get(ctx, changeOp)
	require.NoError(t, err)
	require.Equal(t, utxostore.TierMined, changeReU.Tier, "change still TierMined")
	for _, op := range inputOps {
		_, gerr := utxo.Get(ctx, op)
		require.ErrorIs(t, gerr, &utxostore.NotFoundError{}, "input stays pruned after re-apply")
	}
	balReapply, err := p.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, balMined, balReapply, "balance unchanged by the re-apply")
	ktReapply, found, err := meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusCompleted, ktReapply.Status)
	require.Empty(t, ktReapply.InputBEEF)
}

// newHybridStatusStores builds a fresh, unmigrated Mode-B Provider over an
// isolated Postgres schema (metastore) and an isolated Aerospike set (utxostore)
// — the same split-store wiring as newHybridModeBProvider, but it also returns
// the metastore and utxostore handles so the test can observe the Aerospike-level
// tier promotions and input pruning that the metastore-backed public surface does
// not expose.
func newHybridStatusStores(t *testing.T, pg *testenv.PostgresContainer, aero *testenv.AerospikeContainer, hdrs headers.Headers) (*storage.Provider, *metastore.Store, utxostore.Store) {
	t.Helper()
	ctx := context.Background()

	meta, err := metastore.OpenPostgres(ctx, pg.IsolatedSchemaDSN(t))
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	set := fmt.Sprintf("sa%d", hybridSetCounter.Add(1))
	utxo, err := aerostore.New(ctx, aero.Host(), aero.Port(), aero.Namespace(),
		aerostore.WithSet(set), aerostore.WithLogger(hybridQuietLogger))
	require.NoError(t, err)
	t.Cleanup(func() { _ = utxo.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, utxo, defs.DefaultFeeModel())
	oracle := &conformance.FakeOracle{}

	p, err := storage.New(
		logger, meta, utxo, fnd, oracle, hdrs,
		storage.WithNetwork(defs.NetworkTestnet),
		storage.WithStorageName("hybrid-status-apply"),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}),
	)
	require.NoError(t, err)
	require.False(t, p.ModeA(), "aerospike+postgres hybrid must be Mode B (split stores)")
	return p, meta, utxo
}

// hybridSpendable reports whether (txid, vout) is spendable as seen through the
// public FindOutputsAuth surface (the metastore output ledger).
func hybridSpendable(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string, vout uint32) bool {
	t.Helper()
	rows, err := p.FindOutputsAuth(context.Background(), auth, wdk.FindOutputsArgs{TxID: &txid, Vout: &vout})
	require.NoError(t, err)
	require.Len(t, rows, 1, "exactly one output row for %s:%d", txid, vout)
	return rows[0].Spendable
}

// hybridTxStatus returns a user's transaction standardized status by its
// Reference, via the public ListTransactions surface (ListTransactionsArgs has no
// reference filter, so it scans the user's transactions).
func hybridTxStatus(t *testing.T, p *storage.Provider, auth wdk.AuthID, reference string) wdk.StandardizedTxStatus {
	t.Helper()
	res, err := p.ListTransactions(context.Background(), auth, wdk.ListTransactionsArgs{Limit: 10000})
	require.NoError(t, err)
	for _, tx := range res.Transactions {
		if tx.Reference == reference {
			return tx.Status
		}
	}
	require.FailNow(t, "no transaction found for reference", reference)
	return ""
}
