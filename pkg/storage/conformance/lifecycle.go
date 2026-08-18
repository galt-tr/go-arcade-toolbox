package conformance

import (
	"context"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/internal/stress"
	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// concurrentLifecycle races the WHOLE write path, not just its first step.
//
// [suite.contentionClaim] stops at CreateAction, so the two transitions that
// actually move money have never been exercised under concurrency: the
// reserved->spent flip that happens AFTER arcade returns 202, and the release
// that happens when it does not. Both commit against the same rows the funder is
// concurrently claiming from, and both are where a double spend would come from
// if the storage layer's locking were wrong.
//
// Each worker runs CreateAction -> BuildSignedTx -> ProcessAction against one
// shared basket. The oracle partitions them deterministically by index:
//
//	accepted   -> 202, inputs must end up spent by that txid
//	rejected   -> 4xx, inputs must stay reserved until the reconciler
//	recovered  -> 4xx then SEEN, inputs must stay spent, never released
//
// Phase 1 asserts what every backend must hold. Phase 2 needs the reconciler
// seam and asserts the release, so backends without it still gain the rest
// rather than skipping the whole subtest.
func (s *suite) concurrentLifecycle(t *testing.T) {
	if s.cfg.rrEnv == nil {
		t.Skip("backend supplied no RejectReleaseEnv (see conformance.WithRejectReleaseEnv); " +
			"the concurrent write path is NOT covered for this backend")
	}

	const (
		denomination = 50_000
		pay          = 40_000
	)
	// Scaled by ARCADE_STRESS. Three groups of equal size, so the partition
	// stays balanced at any factor.
	perGroup := stress.Scale(4)
	n := perGroup * 3

	ctx := context.Background()
	env := s.freshEnv(t)
	auth := s.newAuth(t, env.Provider, NewIdentityKey(t))

	// One sufficient coin per worker: every CreateAction must be able to
	// succeed, so a failure is a real defect rather than an undersized pool.
	sats := make([]uint64, n)
	for i := range sats {
		sats[i] = denomination
	}
	atomicBEEF, _ := BuildMinedAtomicBEEF(t, 0x51, 900_200, sats...)
	outs := make([]*wdk.InternalizeOutput, n)
	for i := range outs {
		outs[i] = WalletPaymentOutput(uint32(i), NewIdentityKey(t)) //nolint:gosec // i < n
	}
	_, err := env.Provider.InternalizeAction(ctx, auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomicBEEF),
		Outputs: outs,
	})
	require.NoError(t, err)

	minted := uint64(n) * denomination //nolint:gosec // n is a small positive worker count
	bal0, err := env.Provider.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	require.Equal(t, minted, bal0, "the pool did not fund as expected")

	// Broadcast verdict by worker index, decided before any goroutine starts so
	// the partition is deterministic under -race and at any stress factor.
	verdictFor := func(i int) string {
		switch {
		case i < perGroup:
			return "accepted"
		case i < 2*perGroup:
			return "rejected"
		default:
			return "recovered"
		}
	}

	type result struct {
		verdict   string
		txid      string
		outpoints []string
		err       error
	}
	results := make([]result, n)

	// Every broadcast is ACCEPTED (the oracle's default 202) and the rejections
	// arrive afterwards, asynchronously. That is the shape the reconciler exists
	// for and the harder one to get right: on the 202 the inputs are already
	// flipped reserved->spent, so a later REJECTED has to un-spend a coin rather
	// than merely un-reserve it — while other workers are concurrently claiming
	// from the same basket. A synchronous 4xx never reaches that state.
	var wg sync.WaitGroup
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v := verdictFor(i)

			res, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(pay))
			if err != nil {
				results[i] = result{verdict: v, err: err}
				return
			}
			ops := rrOutpoints(res)

			tx := BuildSignedTx(t, res)
			txid := tx.TxID().String()

			ref := res.Reference
			id := primitives.TXIDHexString(txid)
			_, perr := env.Provider.ProcessAction(ctx, auth, wdk.ProcessActionArgs{
				IsNewTx:   true,
				Reference: &ref,
				TxID:      &id,
				RawTx:     primitives.ExplicitByteArray(tx.Bytes()),
			})
			results[i] = result{verdict: v, txid: txid, outpoints: ops, err: perr}
		}(i)
	}
	wg.Wait()

	// --- Phase 1: properties every backend must hold ----------------------

	claimedBy := map[string]int{} // outpoint -> worker index
	rejectedOutpoints := map[string]bool{}
	var accepted, rejected, recovered int

	for i, r := range results {
		if r.err != nil {
			// A funding failure is tolerable only in its clean, expected
			// shapes; an opaque error here means the write path broke under
			// concurrency rather than declining cleanly.
			assert.True(t,
				errors.Is(r.err, funder.ErrNotEnoughFunds) || errors.Is(r.err, funder.ErrUTXOContention),
				"worker %d (%s): unexpected error shape: %v", i, r.verdict, r.err)
			continue
		}
		require.NotEmpty(t, r.outpoints, "worker %d: a successful action allocated no input", i)

		for _, o := range r.outpoints {
			if prev, dup := claimedBy[o]; dup {
				t.Errorf("outpoint %s was allocated to worker %d AND worker %d. Two "+
					"transactions funded from one coin, which is the double spend "+
					"this storage layer exists to make impossible", o, prev, i)
				continue
			}
			claimedBy[o] = i
		}

		switch r.verdict {
		case "accepted":
			accepted++
		case "rejected":
			rejected++
			for _, o := range r.outpoints {
				rejectedOutpoints[o] = true
			}
			// The verdict the reconciler will read back on GetTx: still
			// rejected on both passes, so the release is warranted.
			env.Oracle.ScriptTx(r.txid, arcade.StatusRejected,
				WithExtraInfo("PROCESSING (4): failed to validate transaction"))
		case "recovered":
			recovered++
			// Arcade revises its verdict between the rejection and the
			// reconciler's read, which its own status lifecycle allows.
			env.Oracle.ScriptTx(r.txid, arcade.StatusSeenOnNetwork)
		}
	}

	assert.Positive(t, accepted, "no worker was accepted, so the accept path never ran")
	assert.Positive(t, rejected, "no worker was rejected, so the reject path never ran")

	// Balance conservation: nothing was created or destroyed by racing the
	// write path. Spent coins leave the claimable+reserved accounting, so the
	// two live buckets plus what the accepted transactions consumed must still
	// add up to what was minted (change re-enters as its own coin, so compare
	// against the total the provider reports rather than a hand-rolled sum).
	balAfter, err := env.Provider.GetBalance(ctx, auth, "")
	require.NoError(t, err)
	assert.LessOrEqual(t, balAfter, minted,
		"the wallet reports more satoshis than were ever minted: the concurrent "+
			"write path created value")

	if s.cfg.approximateSelection {
		// An approximate backend may legitimately decline some requests.
		assert.Positive(t, len(claimedBy), "no coin was allocated at all")
	} else {
		assert.Equal(t, n, accepted+rejected+recovered,
			"exact selection: every worker must fund from a pool sized one "+
				"sufficient coin per worker")
	}

	// --- Phase 2: the release, where the seam exists -----------------------

	rec, ok := env.Provider.(reconcilable)
	if !ok {
		t.Logf("provider %T cannot reconcile; the release half of the lifecycle "+
			"is NOT covered for this backend", env.Provider)
		return
	}

	// Drive every rejected transaction to suspect, then reconcile.
	for _, r := range results {
		if r.err != nil || r.verdict == "accepted" {
			continue
		}
		require.NoError(t, rec.ApplyStatusUpdate(ctx,
			arcade.TxRecord{TxID: r.txid, Status: arcade.StatusRejected}))
	}

	report := rrReconcile(ctx, t, rec, env.Advance)

	assert.Equal(t, rejected, report.Released,
		"every rejected transaction must release exactly once — no more (a double "+
			"release re-hands a coin another action may already hold) and no less "+
			"(an unreleased coin is lost for good); got %+v", report)
	assert.Equal(t, recovered, report.FalsePositive,
		"every recovered transaction must be recognized as a false positive and "+
			"release nothing; got %+v", report)

	// The released coins must be spendable in practice, not merely marked so.
	if rejected > 0 {
		retry, err := env.Provider.CreateAction(ctx, auth, PaymentArgs(pay))
		require.NoError(t, err,
			"no released coin could be funded from after the reconciler ran, so "+
				"the release did not restore anything an application can use")
		for _, o := range rrOutpoints(retry) {
			assert.True(t, rejectedOutpoints[o],
				"the retry funded from %s, which no rejected transaction held. The "+
					"released coins were not the ones that came back", o)
		}
	}
}
