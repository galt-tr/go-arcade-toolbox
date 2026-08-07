//go:build integration

package storage

// Mode A (shared PostgreSQL database) integration tests for the reject→release
// reconciler. They run the full production path — CreateAction → ProcessAction
// (202-accepted, inputs spent) → async REJECTED (suspect) → reconciler release —
// over a real Postgres so the release's atomicity (metastore + utxostore in one
// transaction) and the no-double-release guard are exercised against the actual
// DB, including a concurrent double-pass.

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/internal/testenv"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// seedAcceptedSuspect runs the real accept path and then marks the tx suspect via
// an async REJECTED, returning the txid, its single funding-coin outpoint, and
// its change outpoint — the exact state the reconciler must release.
func (h *modeAHarness) seedAcceptedSuspect(t *testing.T) (txid string, coin, change utxostore.Outpoint) {
	t.Helper()
	ctx := context.Background()

	coin = h.mintFunding(t, 0x11, 100_000)
	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)

	signed := buildSignedTx(t, res)
	txid = signed.TxID().String()

	// Broadcast is accepted (SEEN): inputs flip reserved→spent, change → unproven.
	h.oracle.broadcast = func(_ context.Context, id string, _ []byte) (*arcade.BroadcastResult, error) {
		return &arcade.BroadcastResult{TxID: id, Status: arcade.StatusSeenOnNetwork}, nil
	}
	_, err = h.p.ProcessAction(ctx, h.auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: strptr(res.Reference),
		TxID:      txidPtr(txid),
		RawTx:     primitives.ExplicitByteArray(signed.Bytes()),
	})
	require.NoError(t, err)

	spent, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.NotNil(t, spent.SpentBy, "funding coin spent on acceptance")

	change = changeOutpoint(t, res, txid)

	// Async REJECTED arrives over SSE → suspect (no release yet).
	require.NoError(t, h.p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))
	h.oracle.getTx = func(_ context.Context, id string) (*arcade.TxRecord, error) {
		if id == txid {
			return &arcade.TxRecord{TxID: id, Status: arcade.StatusRejected}, nil
		}
		return nil, arcade.ErrTxNotFound
	}
	return txid, coin, change
}

func TestModeA_Reconciler_ReleaseAtomic(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	h := newModeAHarness(t, pg)
	require.True(t, h.p.ModeA())

	txid, coin, change := h.seedAcceptedSuspect(t)

	// grace=0: pass 1 stamps verified_rejected_at, pass 2 releases (the two-pass
	// guard is satisfied immediately under real time with a zero separation).
	r1, err := h.p.VerifyAndReleaseSuspects(ctx, 0, time.Hour, 100)
	require.NoError(t, err)
	require.Equal(t, 0, r1.Released, "pass 1 stamps only")

	r2, err := h.p.VerifyAndReleaseSuspects(ctx, 0, time.Hour, 100)
	require.NoError(t, err)
	require.Equal(t, 1, r2.Released, "pass 2 releases the provably-dead tx")

	// The release committed atomically across both stores over the one DB.
	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Nil(t, u.SpentBy, "funding coin released back to claimable")
	require.Empty(t, u.ReservedBy)

	_, err = h.utxo.Get(ctx, change)
	require.Error(t, err, "phantom change removed")

	kt, found, err := h.meta.KnownTx().FindByTxID(ctx, txid)
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, wdk.ProvenTxStatusInvalid, kt.Status)

	rows, err := h.meta.Transactions().FindByTxIDAllUsers(ctx, txid)
	require.NoError(t, err)
	require.NotEmpty(t, rows)
	require.Equal(t, wdk.TxStatusFailed, rows[0].Status)

	// A third pass is a no-op (terminal, no longer a suspect).
	r3, err := h.p.VerifyAndReleaseSuspects(ctx, 0, time.Hour, 100)
	require.NoError(t, err)
	require.Equal(t, 0, r3.Scanned)
}

func TestModeA_Reconciler_ConcurrentDoublePass_NoDoubleRelease(t *testing.T) {
	pg := testenv.StartPostgres(t)
	ctx := context.Background()
	h := newModeAHarness(t, pg)

	txid, coin, _ := h.seedAcceptedSuspect(t)

	// Pass 1 stamps.
	_, err := h.p.VerifyAndReleaseSuspects(ctx, 0, time.Hour, 100)
	require.NoError(t, err)

	// Several passes race the release; the suspectFailed→invalidTx CAS admits one.
	const workers = 6
	var released atomic.Int64
	var wg sync.WaitGroup
	for range workers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			rep, verr := h.p.VerifyAndReleaseSuspects(ctx, 0, time.Hour, 100)
			require.NoError(t, verr)
			released.Add(int64(rep.Released))
		}()
	}
	wg.Wait()

	require.Equal(t, int64(1), released.Load(), "exactly one racing pass releases")

	u, err := h.utxo.Get(ctx, coin)
	require.NoError(t, err)
	require.Nil(t, u.SpentBy, "coin released exactly once")

	// A stray Unspend by the dead txid must not stomp a reclaimed coin.
	other, err := chainhash.NewHashFromHex(txid)
	require.NoError(t, err)
	n, err := h.utxo.Unspend(ctx, *other, []utxostore.Outpoint{coin})
	require.NoError(t, err)
	require.Equal(t, 0, n, "no re-release: the coin is no longer spent_by the dead tx")
}
