package storage_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/conformance"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// releaseObservation is what a compensating ReleaseReservation saw when it ran:
// the error state of the context it was handed, and whether that context
// carried a deadline of its own.
type releaseObservation struct {
	ctxErr      error
	hasDeadline bool
}

// cancelOnClaimStore reproduces the live shape of a request that dies between
// funding and persistence: it cancels the request context the moment the funder
// reserves a coin (a client hanging up, or a request deadline expiring, while
// the coins are already held), and records the context every subsequent
// ReleaseReservation is handed.
//
// memstore ignores contexts entirely, so the release itself always "works"
// here; what the recording pins is which context the store is given, which is
// the part a SQL store acts on.
type cancelOnClaimStore struct {
	utxostore.Store

	mu       sync.Mutex
	cancel   context.CancelFunc
	releases []releaseObservation
}

// armCancel installs the cancel func fired by the next successful claim. It is
// set after the stack is built so the seeding path runs on a live context.
func (c *cancelOnClaimStore) armCancel(cancel context.CancelFunc) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.cancel = cancel
}

func (c *cancelOnClaimStore) fireCancel() {
	c.mu.Lock()
	cancel := c.cancel
	c.cancel = nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

func (c *cancelOnClaimStore) ClaimSmallestSufficient(ctx context.Context, s utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	utxo, err := c.Store.ClaimSmallestSufficient(ctx, s, reservation, minSats)
	if utxo != nil {
		c.fireCancel()
	}
	return utxo, err
}

func (c *cancelOnClaimStore) ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error) {
	_, hasDeadline := ctx.Deadline()
	c.mu.Lock()
	c.releases = append(c.releases, releaseObservation{ctxErr: ctx.Err(), hasDeadline: hasDeadline})
	c.mu.Unlock()
	return c.Store.ReleaseReservation(ctx, userID, reservation)
}

func (c *cancelOnClaimStore) observedReleases() []releaseObservation {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]releaseObservation(nil), c.releases...)
}

// TestCreateAction_ModeBCompensationRunsOnDetachedContext pins the Mode B half
// of the release-on-cancellation contract.
//
// In Mode B (split metastore / utxostore) CreateAction funds first and persists
// second, compensating a persist failure by releasing the whole reservation
// token. That compensation used to run on the request context — the very
// context whose cancellation is the most likely reason the persist failed in
// the first place. Against a SQL utxostore a canceled context fails the release
// at BeginTx without touching a row, so every coin the action reserved stayed
// held until the 15-minute stale-reservation sweep freed it.
//
// Here the funder's claim kills the request context mid-flight, so the ambient
// metastore transaction cannot even begin; the compensating release must still
// be handed a live, deadline-bounded context.
func TestCreateAction_ModeBCompensationRunsOnDetachedContext(t *testing.T) {
	var spy *cancelOnClaimStore
	s := newE2EStackWithUTXO(t, func(base utxostore.Store) utxostore.Store {
		spy = &cancelOnClaimStore{Store: base}
		return spy
	})
	require.False(t, s.provider.ModeA(), "the split memstore/SQLite stack must be Mode B for this path to run")
	s.seedMinedPayment(e2eSeedSatoshis, e2eBlockHeight)

	auth := s.authID(context.Background())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	spy.armCancel(cancel)

	_, err := s.provider.CreateAction(ctx, auth, conformance.PaymentArgs(e2ePaymentAmount))
	require.Error(t, err, "persisting cannot begin its transaction on the canceled request context")
	require.Error(t, ctx.Err(), "sanity: the request context died while the funder held the reservation")

	releases := spy.observedReleases()
	require.Len(t, releases, 1, "the failed persist must compensate with exactly one whole-token release")
	require.NoError(t, releases[0].ctxErr,
		"the compensating release must run on a context detached from the canceled request; "+
			"a canceled context fails a SQL release at BeginTx and orphans every reserved coin")
	require.True(t, releases[0].hasDeadline,
		"the detached release must stay bounded by its own timeout, not run unbounded through shutdown")
}
