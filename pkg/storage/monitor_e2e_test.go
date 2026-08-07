package storage_test

// The full async loop the arcade-only design is about (completes the M3-deferred
// SSE-MINED e2e): wallet CreateAction → process → broadcast (mockarcade 202)
// returns early with the tx UNPROVEN; then the monitor's SSE apply pipeline,
// running against the REAL arcade client → mockarcade, consumes a MINED frame
// carrying a BUMP that the REAL headers client validates against the mock
// chaintracks — and the wallet's own sent tx is promoted to completed, its proof
// stored (root-verified), and its change promoted to TierMined.
//
// No fakes on the trust-critical seams (real arcade client, real headers client,
// real VerifyMerkleRoot); untagged (mockarcade + mock chaintracks + SQLite temp
// file + memstore), so it runs with the normal suite.

import (
	"context"
	"encoding/hex"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	sdk "github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/fixtures"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/monitor"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

func TestE2E_Monitor_SSE_MINED_PromotesOwnSentTx(t *testing.T) {
	ctx := context.Background()
	s := newE2EStack(t)
	seedTxid := s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)
	require.NotEmpty(t, seedTxid)

	// Create + sign + process + broadcast a payment. Returns early: the tx is
	// UNPROVEN (broadcast accepted, not yet mined).
	args := fixtures.DefaultWalletCreateActionArgs(t, func(a *sdk.CreateActionArgs) {
		a.Description = "e2e sse-mined payment"
		a.Labels = []string{"sse-mined"}
		a.Outputs[0].Satoshis = e2ePaymentAmount
	})
	res, err := s.wallet.CreateAction(ctx, args, e2eOriginator)
	require.NoError(t, err)
	txid := res.Txid.String()
	require.Len(t, txid, 64, "a broadcast action returns its txid")
	require.Len(t, s.arc.Broadcasts(), 1, "the payment was broadcast once")

	requireActionStatus(t, s, "sse-mined", wdk.TxStatusUnproven)

	// The change coin is at TierUnproven before mining.
	changeOp := s.changeOutpoint(txid)
	require.Equal(t, utxostore.TierUnproven, s.tierOf(changeOp), "change is unproven pre-mine")

	// Start the monitor's SSE apply pipeline against the REAL arcade client.
	mon, err := monitor.NewDaemon(logging.NewTestLogger(t), s.provider, s.chainSub, s.oracle,
		defs.DefaultMonitorConfig(), monitor.WithoutDistributedLock())
	require.NoError(t, err)
	require.NoError(t, mon.Start(ctx, nil)) // SSE pipeline only, no cron tasks
	t.Cleanup(func() { _ = mon.Stop() })

	// Build a MINED BUMP for the wallet's own txid at a fresh height. The single
	// leaf IS the txid, so the computed merkle root equals the txid; registering
	// that root at the height makes the REAL headers client validate the proof.
	mineHeight := e2eBlockHeight + 1
	txidHash, err := chainhash.NewHashFromHex(txid)
	require.NoError(t, err)
	trueVal := true
	mp := transaction.NewMerklePath(mineHeight, [][]*transaction.PathElement{
		{{Offset: 0, Hash: txidHash, Txid: &trueVal}},
	})
	root, err := mp.ComputeRoot(txidHash)
	require.NoError(t, err)
	s.ct.RegisterHeader(mineHeight, *root)

	// EmitStatus drops events with no connected subscriber, and the pipeline
	// connects asynchronously, so keep emitting the MINED frame until the wallet
	// tx flips to completed.
	require.Eventually(t, func() bool {
		s.arc.EmitStatus(txid, "MINED", map[string]any{
			"merklePath":  hex.EncodeToString(mp.Bytes()),
			"blockHeight": mineHeight,
		})
		return s.actionStatus(t, "sse-mined") == wdk.TxStatusCompleted
	}, 6*time.Second, 100*time.Millisecond, "the SSE MINED frame must complete the wallet's own sent tx")

	// The proof is stored, header-verified, and the change promoted to TierMined.
	kt, found, err := s.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusCompleted, kt.Status)
	require.NotEmpty(t, kt.MerklePath, "BUMP stored")
	require.NotEmpty(t, kt.MerkleRoot, "computed (verified) root stored")
	require.NotEmpty(t, kt.BlockHash, "block hash stored from the real headers client")
	require.NotNil(t, kt.BlockHeight)
	require.Equal(t, mineHeight, *kt.BlockHeight)

	require.Equal(t, utxostore.TierMined, s.tierOf(changeOp), "change promoted to mined")
}

// --- small e2e helpers -----------------------------------------------------

func requireActionStatus(t *testing.T, s *e2eStack, label string, want wdk.TxStatus) {
	t.Helper()
	require.Equal(t, want, s.actionStatus(t, label))
}

func (s *e2eStack) actionStatus(t *testing.T, label string) wdk.TxStatus {
	t.Helper()
	actions, err := s.wallet.ListActions(context.Background(), sdk.ListActionsArgs{Labels: []string{label}}, e2eOriginator)
	require.NoError(t, err)
	require.Equal(t, uint32(1), actions.TotalActions, "exactly one action for label %q", label)
	return wdk.TxStatus(actions.Actions[0].Status)
}

// changeOutpoint returns the (single) change outpoint minted by txid.
func (s *e2eStack) changeOutpoint(txid string) utxostore.Outpoint {
	change := true
	rows, err := s.meta.Outputs().FindOutputs(context.Background(), wdk.FindOutputsArgs{TxID: &txid, Change: &change})
	require.NoError(s.t, err)
	require.NotEmpty(s.t, rows, "a change output exists for %s", txid)
	hash, err := chainhash.NewHashFromHex(txid)
	require.NoError(s.t, err)
	return utxostore.Outpoint{TxID: *hash, Vout: rows[0].Vout}
}

func (s *e2eStack) tierOf(op utxostore.Outpoint) utxostore.Tier {
	u, err := s.utxo.Get(context.Background(), op)
	require.NoError(s.t, err)
	return u.Tier
}
