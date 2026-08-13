package storage_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// respendClock is a manual clock. The release path is gated on a grace period,
// so the alternative is a test that sleeps for it — and a test that sleeps for
// ninety seconds is a test the first person in a hurry deletes.
type respendClock struct {
	t time.Time
}

func (c *respendClock) Now() time.Time          { return c.t }
func (c *respendClock) Advance(d time.Duration) { c.t = c.t.Add(d) }
func newRespendClock() *respendClock            { return &respendClock{t: time.Unix(1_700_000_000, 0).UTC()} }
func (c *respendClock) fn() func() time.Time    { return c.Now }

const (
	respendGrace = time.Minute
	respendMaxQ  = time.Hour
)

// TestRejectedActionReleasesItsInputsAndTheCoinCanBeSpentAgain is the loop every
// application built on this toolbox depends on, and until now nothing asserted
// it end to end.
//
// Both halves were already covered, separately. TestProcessAction_Reject
// asserts inputs are NOT released when a rejection arrives — correct, release is
// the reconciler's job. reconciler_test.go asserts a suspect's coin becomes
// claimable again. But the hermetic reconciler harness never calls CreateAction
// at all: it hand-seeds known_txs rows. And the one test that does drive the
// real path is Postgres-tagged and stops at "the coin is claimable" without ever
// spending it.
//
// So the question an application actually has — "my transaction was rejected;
// can I just try again?" — had no answer in the test suite. This is that answer,
// hermetic and untagged:
//
//	CreateAction -> ProcessAction -> REJECTED -> reconciler -> CreateAction again
func TestRejectedActionReleasesItsInputsAndTheCoinCanBeSpentAgain(t *testing.T) {
	ctx := context.Background()
	clock := newRespendClock()
	oracle := &conformance.FakeOracle{}
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, oracle,
		&conformance.FakeHeaders{}, clock.fn(),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))

	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)

	// 1. Claim a coin and broadcast. The oracle takes it, as arcade's 202 does.
	first, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	require.NotEmpty(t, first.Inputs, "the action funded from nothing")
	claimed := respendOutpoints(first)

	txid := respendProcess(t, p, auth, first)

	// 2. Arcade rejects it. Phase one deliberately holds the inputs: a 4xx is not
	//    proof the transaction is dead, and releasing on it would let a
	//    double-spend through the moment arcade changed its mind.
	oracle.ScriptTx(txid, arcade.StatusRejected,
		conformance.WithExtraInfo("PROCESSING (4): failed to validate transaction"))
	require.NoError(t, p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))

	assert.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the input was released the instant a rejection arrived. Release is the "+
			"reconciler's job precisely because a REJECTED verdict can be revised — "+
			"SEEN_ON_NETWORK recovers one — and releasing early races that")

	// 3. The reconciler verifies and, past the grace, releases.
	clock.Advance(respendGrace + time.Second)
	_, err = p.VerifyAndReleaseSuspects(ctx, respendGrace, respendMaxQ, 128)
	require.NoError(t, err)
	clock.Advance(respendGrace + time.Second)
	_, err = p.VerifyAndReleaseSuspects(ctx, respendGrace, respendMaxQ, 128)
	require.NoError(t, err)

	// 4. THE ASSERTION NOTHING MADE: the coin is spendable again, in practice and
	//    not merely in the utxostore's opinion of itself.
	second, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err,
		"a rejected action's inputs never came back. The application's only "+
			"recovery from a rejection is to build again, so a coin that stays "+
			"reserved after its transaction is provably dead is a coin lost for good")
	require.NotEmpty(t, second.Inputs)

	assert.ElementsMatch(t, claimed, respendOutpoints(second),
		"the retry funded from different coins, so this passed without ever "+
			"proving the rejected one came back")
}

// A rejection that arcade later revises must NOT release anything. This is the
// other half of the same decision, and getting it wrong is worse: it hands out a
// coin whose transaction is about to be accepted.
func TestARecoveredRejectionKeepsItsInputsHeld(t *testing.T) {
	ctx := context.Background()
	clock := newRespendClock()
	oracle := &conformance.FakeOracle{}
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, oracle,
		&conformance.FakeHeaders{}, clock.fn(),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))

	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)

	res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	txid := respendProcess(t, p, auth, res)

	require.NoError(t, p.ApplyStatusUpdate(ctx, arcade.TxRecord{TxID: txid, Status: arcade.StatusRejected}))

	// Arcade changes its mind, which its own status lifecycle documents as
	// possible: SEEN_ON_NETWORK may recover a REJECTED transaction.
	oracle.ScriptTx(txid, arcade.StatusSeenOnNetwork)

	clock.Advance(respendGrace + time.Second)
	_, err = p.VerifyAndReleaseSuspects(ctx, respendGrace, respendMaxQ, 128)
	require.NoError(t, err)
	clock.Advance(respendGrace + time.Second)
	_, err = p.VerifyAndReleaseSuspects(ctx, respendGrace, respendMaxQ, 128)
	require.NoError(t, err)

	assert.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"an input was released for a transaction the network went on to ACCEPT. "+
			"That coin is spent on chain; handing it back to the funder produces a "+
			"double spend from a wallet whose whole job is preventing one")
}

// --- helpers ---------------------------------------------------------------

func respendUser(t *testing.T, p *storage.Provider) wdk.AuthID {
	t.Helper()
	key := conformance.NewIdentityKey(t)
	resp, err := p.FindOrInsertUser(context.Background(), key)
	require.NoError(t, err)
	uid := resp.User.UserID
	return wdk.AuthID{IdentityKey: key, UserID: &uid}
}

// respendFund internalizes one mined coin so there is exactly ONE candidate,
// and returns its txid.
//
// One coin is deliberate: given a choice, a retry that funded from a different
// coin would satisfy the final assertion without the rejected coin ever having
// come back.
func respendFund(t *testing.T, p *storage.Provider, auth wdk.AuthID, sats uint64) string {
	t.Helper()
	atomic, txid := conformance.BuildMinedAtomicBEEF(t, 0x31, 900_100, sats)
	_, err := p.InternalizeAction(context.Background(), auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomic),
		Outputs: []*wdk.InternalizeOutput{conformance.WalletPaymentOutput(0, conformance.NewIdentityKey(t))},
	})
	require.NoError(t, err)
	return txid
}

func respendProcess(t *testing.T, p *storage.Provider, auth wdk.AuthID,
	res *wdk.StorageCreateActionResult) string {
	t.Helper()
	tx := conformance.BuildSignedTx(t, res)
	txid := tx.TxID().String()
	ref := res.Reference
	id := primitives.TXIDHexString(txid)
	_, err := p.ProcessAction(context.Background(), auth, wdk.ProcessActionArgs{
		IsNewTx:   true,
		Reference: &ref,
		TxID:      &id,
		RawTx:     primitives.ExplicitByteArray(tx.Bytes()),
	})
	require.NoError(t, err)
	return txid
}

func respendOutpoints(res *wdk.StorageCreateActionResult) []string {
	out := make([]string, 0, len(res.Inputs))
	for _, in := range res.Inputs {
		out = append(out, fmt.Sprintf("%s.%d", in.SourceTxID, in.SourceVout))
	}
	return out
}

// respendSpendable reports whether one outpoint is currently claimable, as the
// funder would see it.
func respendSpendable(t *testing.T, p *storage.Provider, auth wdk.AuthID, txid string, vout uint32) bool {
	t.Helper()
	rows, err := p.FindOutputsAuth(context.Background(), auth,
		wdk.FindOutputsArgs{TxID: &txid, Vout: &vout})
	require.NoError(t, err)
	require.Len(t, rows, 1, "expected exactly one output row for %s:%d", txid, vout)
	return rows[0].Spendable
}
