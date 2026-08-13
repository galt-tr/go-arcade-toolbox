package storage_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// Every concurrency test in this module races HOMOGENEOUS operations: many
// claimers, or many releasers. Nothing races a claimer against a releaser, and
// that is the pairing where a double allocation would actually come from — one
// goroutine handing a coin back while another is taking it.
//
// These tests close that. They are deliberately black-box, through the provider,
// because that is the surface an application uses.

// TestSweepRacingCreateActionNeverDoubleAllocates runs the recovery sweep
// against live CreateActions.
//
// The shape is the one a real deployment has: some reservations are genuinely
// stale (a process died between CreateAction and SignAction, so they must be
// reclaimed) while others are seconds old and belong to work in flight. The
// sweep has to reclaim the first without ever touching the second, and it has to
// do that while the funder is claiming concurrently.
//
// The invariant is the same one the whole wallet exists for: no outpoint is ever
// allocated to two successful actions.
func TestSweepRacingCreateActionNeverDoubleAllocates(t *testing.T) {
	const (
		coins    = 24
		stranded = 8
		workers  = 12
		denom    = 50_000
		pay      = 40_000
		ttl      = 15 * time.Minute
	)

	ctx := context.Background()
	clock := newRespendClock()
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, &conformance.FakeOracle{},
		&conformance.FakeHeaders{}, clock.fn(),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))
	auth := respendUser(t, p)
	raceFund(t, p, auth, coins, denom)

	// The ledger tracks LIVE claims only. A coin whose reservation was swept is
	// legitimately reallocated — that is the sweep's entire purpose — so counting
	// reuse-after-sweep as a collision would assert the opposite of the desired
	// behavior. What must never happen is two live actions sharing a coin.
	seen := &outpointLedger{seen: map[string]string{}}

	// Strand some reservations: CreateAction with no ProcessAction, exactly as a
	// process killed between the two leaves behind. These are dead by
	// construction, so their coins are the ones the sweep should hand back.
	strandedCoins := map[string]bool{}
	for range stranded {
		res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(pay))
		require.NoError(t, err)
		for _, o := range respendOutpoints(res) {
			strandedCoins[o] = true
		}
	}

	// Age them past the sweep's TTL. The reservations taken below will be young,
	// so a correct sweep reclaims the first set and leaves the second alone.
	clock.Advance(ttl + time.Minute)

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// The releaser: sweeping continuously, as the monitor's fail_abandoned task
	// does on its timer.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			if err := p.SweepStaleReservations(ctx, clock.Now().Add(-ttl), 256); err != nil {
				t.Errorf("sweep: %v", err)
				return
			}
		}
	}()

	// The claimers.
	wg.Add(workers)
	for w := range workers {
		go func(w int) {
			defer wg.Done()
			for i := range 4 {
				res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(pay))
				if err != nil {
					// Only clean, expected funding errors are acceptable. An
					// opaque one means a race surfaced as a crash rather than
					// as backpressure.
					if !errors.Is(err, funder.ErrNotEnoughFunds) &&
						!errors.Is(err, funder.ErrUTXOContention) {
						t.Errorf("worker %d: unexpected error: %v", w, err)
					}
					continue
				}
				seen.claim(t, fmt.Sprintf("worker-%d-%d", w, i), respendOutpoints(res))
			}
		}(w)
	}

	// Let the claimers finish, then stop the sweeper.
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	time.Sleep(250 * time.Millisecond)
	close(stop)
	<-done

	// And the sweep must actually have done its job, or this test would pass on
	// a build where SweepStaleReservations was a no-op: every leaked coin stays
	// reserved, the workers simply use the other coins, and no collision is
	// possible because nothing was ever reused.
	reclaimed := 0
	for o := range strandedCoins {
		if _, used := seen.seen[o]; used {
			reclaimed++
		}
	}
	assert.NotZero(t, reclaimed,
		"not one of the %d stranded reservations was reclaimed and reused. Either "+
			"the sweep is not running or leaked coins are lost for good — and either "+
			"way the collision check above proved nothing, because nothing was reused",
		len(strandedCoins))
}

// A reservation whose transaction can still be re-driven must never be swept.
//
// reservationResendable is the guard, and it had no concurrent test and no
// clock-driven one. Sweeping a transaction that SendWaiting is about to
// re-broadcast would manufacture exactly the double spend the sweep exists to
// prevent: the coin goes back to the funder while a signed transaction spending
// it is still queued.
func TestSweepLeavesARedrivableReservationAlone(t *testing.T) {
	const ttl = 15 * time.Minute

	ctx := context.Background()
	clock := newRespendClock()
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, &conformance.FakeOracle{},
		&conformance.FakeHeaders{}, clock.fn(),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))
	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)

	res, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	// ProcessAction stores the raw tx, which is what makes it re-drivable.
	respendProcess(t, p, auth, res)

	require.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the funding coin should be reserved after a processed action")

	// Age it well past the TTL and sweep hard.
	clock.Advance(ttl * 4)
	require.NoError(t, p.SweepStaleReservations(ctx, clock.Now().Add(-ttl), 256))

	assert.False(t, respendSpendable(t, p, auth, fundTxID, 0),
		"the sweep reclaimed a coin belonging to a transaction that is still "+
			"re-drivable. That transaction is signed and queued; handing its input "+
			"back to the funder is the double spend the sweep exists to prevent")
}

// The other half: a reservation that genuinely cannot be re-driven must come
// back, and must be immediately usable rather than merely un-reserved.
func TestASweptReservationIsImmediatelyClaimableAgain(t *testing.T) {
	const ttl = 15 * time.Minute

	ctx := context.Background()
	clock := newRespendClock()
	p := conformance.NewInMemoryProviderClock(t, defs.NetworkTestnet, &conformance.FakeOracle{},
		&conformance.FakeHeaders{}, clock.fn(),
		storage.WithScriptsVerifier(conformance.AlwaysValidScripts{}))
	auth := respendUser(t, p)
	fundTxID := respendFund(t, p, auth, 60_000)

	// CreateAction and then nothing — the shape a crash between create and sign
	// leaves behind. No raw tx, so it is not re-drivable.
	_, err := p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	require.NoError(t, err)
	require.False(t, respendSpendable(t, p, auth, fundTxID, 0))

	clock.Advance(ttl + time.Minute)
	require.NoError(t, p.SweepStaleReservations(ctx, clock.Now().Add(-ttl), 256))

	require.True(t, respendSpendable(t, p, auth, fundTxID, 0),
		"a leaked reservation was never reclaimed; the coin is lost for good")

	// Usable in practice, not merely flagged spendable.
	_, err = p.CreateAction(ctx, auth, conformance.PaymentArgs(40_000))
	assert.NoError(t, err,
		"the swept coin reads as spendable but the funder will not take it")
}

// outpointLedger records which LIVE action claimed which outpoint, and fails
// the moment one is handed to two of them at once.
type outpointLedger struct {
	mu   sync.Mutex
	seen map[string]string
}

func (l *outpointLedger) claim(t *testing.T, who string, outpoints []string) {
	t.Helper()
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, o := range outpoints {
		if prev, dup := l.seen[o]; dup {
			t.Errorf("outpoint %s allocated to both %q and %q — a coin was handed to "+
				"two actions at once, which is the double spend this wallet exists to "+
				"make impossible", o, prev, who)
			continue
		}
		l.seen[o] = who
	}
}

// raceFund internalizes n mined coins of the given denomination.
func raceFund(t *testing.T, p *storage.Provider, auth wdk.AuthID, n int, sats uint64) {
	t.Helper()
	amounts := make([]uint64, n)
	for i := range amounts {
		amounts[i] = sats
	}
	atomic, _ := conformance.BuildMinedAtomicBEEF(t, 0x44, 900_200, amounts...)
	sender := conformance.NewIdentityKey(t)
	outs := make([]*wdk.InternalizeOutput, n)
	for i := range outs {
		outs[i] = conformance.WalletPaymentOutput(uint32(i), sender) //nolint:gosec // n is small
	}
	_, err := p.InternalizeAction(context.Background(), auth, wdk.InternalizeActionArgs{
		Tx:      primitives.ExplicitByteArray(atomic),
		Outputs: outs,
	})
	require.NoError(t, err)
}
