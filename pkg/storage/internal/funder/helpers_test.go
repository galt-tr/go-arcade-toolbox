package funder_test

import (
	"context"
	"log/slog"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore/utxostoretest"
)

// testLogger returns a debug slog.Logger wired to the test log.
func testLogger(t testing.TB) *slog.Logger { return logging.NewTestLogger(t) }

const (
	testUserID = int64(1)
	testBasket = "default"
	testRef    = "ref-1"
)

// noBackoff is injected in contention tests so the retry loop never actually sleeps.
func noBackoff(context.Context, int) {}

// mintCoins mints one synthetic transaction (seed) worth of coins at the given
// tier into store, returning their outpoints (vout i carries sats[i]). Built
// directly (not via utxostoretest.MintTx) so it works for benchmarks too.
func mintCoins(t testing.TB, store utxostore.Store, seed string, tier utxostore.Tier, sats ...uint64) []utxostore.Outpoint {
	t.Helper()
	mints := make([]*utxostore.Mint, len(sats))
	ops := make([]utxostore.Outpoint, len(sats))
	for i, s := range sats {
		ops[i] = utxostoretest.NewOutpoint(seed, uint32(i)) //nolint:gosec // i bounded by len(sats)
		mints[i] = utxostoretest.NewMint(ops[i], testUserID, testBasket, tier, s)
	}
	require.NoError(t, store.Mint(context.Background(), mints))
	for _, m := range mints {
		require.NoError(t, m.Err)
	}
	return ops
}

// baseArgs is a privacy-mode FundArgs: mined+unproven tiers, a 3-desired-UTXO /
// 1000-min / 8-max change basket, no existing basket coins.
func baseArgs(target satoshi.Value, txSize, outputCount uint64) funder.FundArgs {
	return funder.FundArgs{
		UserID:                  testUserID,
		Basket:                  testBasket,
		Reservation:             testRef,
		TargetSat:               target,
		CurrentTxSize:           txSize,
		OutputCount:             outputCount,
		Tiers:                   []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven},
		NumberOfDesiredUTXOs:    3,
		MinimumDesiredUTXOValue: 1000,
		MaxChangeOutputsPerTx:   8,
	}
}

// allocatedSats returns the satoshi values of every allocated coin, in order.
func allocatedSats(result *funder.Result) []uint64 {
	sats := make([]uint64, 0, len(result.AllocatedUTXOs))
	for _, u := range result.AllocatedUTXOs {
		sats = append(sats, u.Satoshis)
	}
	return sats
}

// reservedCount returns how many satoshis are currently reserved for the user's
// basket, used to assert reservations were (or were not) released.
func reservedSats(t testing.TB, store utxostore.Store) uint64 {
	t.Helper()
	bal, err := store.Balance(context.Background(), testUserID, testBasket)
	require.NoError(t, err)
	return bal.Reserved
}

// claimKind identifies which claim method a recordingStore observed.
type claimKind string

const (
	kindSufficient   claimKind = "sufficient"
	kindInsufficient claimKind = "insufficient"
	kindExact        claimKind = "exact"
)

type claimCall struct {
	kind  claimKind
	tier  utxostore.Tier
	bound uint64 // minSats / capSats / denomination
}

// recordingStore wraps a Store and records every claim call, delegating the
// actual work. It embeds the interface so only the three claim methods are
// overridden.
type recordingStore struct {
	utxostore.Store
	mu    sync.Mutex
	calls []claimCall
}

func newRecordingStore(inner utxostore.Store) *recordingStore {
	return &recordingStore{Store: inner}
}

func (r *recordingStore) record(c claimCall) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = append(r.calls, c)
}

func (r *recordingStore) tiersQueried() []utxostore.Tier {
	r.mu.Lock()
	defer r.mu.Unlock()
	tiers := make([]utxostore.Tier, 0, len(r.calls))
	for _, c := range r.calls {
		tiers = append(tiers, c.tier)
	}
	return tiers
}

func (r *recordingStore) kinds() []claimKind {
	r.mu.Lock()
	defer r.mu.Unlock()
	kinds := make([]claimKind, 0, len(r.calls))
	for _, c := range r.calls {
		kinds = append(kinds, c.kind)
	}
	return kinds
}

// count returns the total number of claim calls observed so far.
func (r *recordingStore) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.calls)
}

// reset clears the recorded calls (used to exclude bench warm-up).
func (r *recordingStore) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.calls = nil
}

func (r *recordingStore) ClaimSmallestSufficient(ctx context.Context, s utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	r.record(claimCall{kind: kindSufficient, tier: s.Tier, bound: minSats})
	return r.Store.ClaimSmallestSufficient(ctx, s, reservation, minSats)
}

func (r *recordingStore) ClaimLargestInsufficient(ctx context.Context, s utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	r.record(claimCall{kind: kindInsufficient, tier: s.Tier, bound: capSats})
	return r.Store.ClaimLargestInsufficient(ctx, s, reservation, capSats, limit)
}

func (r *recordingStore) ClaimExact(ctx context.Context, s utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	r.record(claimCall{kind: kindExact, tier: s.Tier, bound: denomination})
	return r.Store.ClaimExact(ctx, s, reservation, denomination, count)
}

// releaseObservation is what a compensating ReleaseReservation saw when it ran:
// the error state of the context it was handed, and whether that context
// carried a deadline.
type releaseObservation struct {
	ctxErr      error
	hasDeadline bool
}

// releaseSpy wraps a Store and records the context state observed at
// ReleaseReservation call time, so a test can assert Fund's terminal
// compensating release runs on a LIVE, deadline-bounded context even when the
// request context that entered Fund is already canceled.
type releaseSpy struct {
	utxostore.Store
	mu    sync.Mutex
	calls []releaseObservation
}

func newReleaseSpy(inner utxostore.Store) *releaseSpy { return &releaseSpy{Store: inner} }

func (r *releaseSpy) ReleaseReservation(ctx context.Context, userID int64, reservation string) (int, error) {
	_, hasDeadline := ctx.Deadline()
	r.mu.Lock()
	r.calls = append(r.calls, releaseObservation{ctxErr: ctx.Err(), hasDeadline: hasDeadline})
	r.mu.Unlock()
	return r.Store.ReleaseReservation(ctx, userID, reservation)
}

// releases returns a copy of every ReleaseReservation observation so far.
func (r *releaseSpy) releases() []releaseObservation {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]releaseObservation(nil), r.calls...)
}

// contentionStore fails the first failCount claim calls with
// utxostore.ErrContention before delegating, simulating an optimistic backend
// whose CAS candidate set is exhausted by concurrent claimers.
type contentionStore struct {
	utxostore.Store
	mu        sync.Mutex
	remaining int
}

func newContentionStore(inner utxostore.Store, failCount int) *contentionStore {
	return &contentionStore{Store: inner, remaining: failCount}
}

func (c *contentionStore) shouldFail() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.remaining > 0 {
		c.remaining--
		return true
	}
	return false
}

func (c *contentionStore) ClaimSmallestSufficient(ctx context.Context, s utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	if c.shouldFail() {
		return nil, utxostore.ErrContention
	}
	return c.Store.ClaimSmallestSufficient(ctx, s, reservation, minSats)
}

func (c *contentionStore) ClaimLargestInsufficient(ctx context.Context, s utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	if c.shouldFail() {
		return nil, utxostore.ErrContention
	}
	return c.Store.ClaimLargestInsufficient(ctx, s, reservation, capSats, limit)
}

func (c *contentionStore) ClaimExact(ctx context.Context, s utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	if c.shouldFail() {
		return nil, utxostore.ErrContention
	}
	return c.Store.ClaimExact(ctx, s, reservation, denomination, count)
}

// newMemStore is a convenience for tests that just need an empty memstore.
func newMemStore() *memstore.Store { return memstore.New() }

// lockedTierStore models the Mode A false-empty at the store boundary: for one
// tier every claim comes back EMPTY (as SKIP LOCKED makes it, when an
// uncommitted peer holds the rows) while the non-locking ClaimableExists probe
// still reports the coins are there. Every other tier is served normally by the
// inner store.
//
// probeErr, when set, is returned by ClaimableExists instead — the "the
// diagnosis itself failed" case.
type lockedTierStore struct {
	utxostore.Store
	locked   utxostore.Tier
	probeErr error

	mu     sync.Mutex
	probes []utxostore.Tier
}

func newLockedTierStore(inner utxostore.Store, locked utxostore.Tier) *lockedTierStore {
	return &lockedTierStore{Store: inner, locked: locked}
}

// probedTiers returns the tiers ClaimableExists was asked about, in order. The
// COUNT is the point: a probe on the allocating path would show up here.
func (l *lockedTierStore) probedTiers() []utxostore.Tier {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]utxostore.Tier(nil), l.probes...)
}

func (l *lockedTierStore) ClaimSmallestSufficient(ctx context.Context, s utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	if s.Tier == l.locked {
		return nil, nil
	}
	return l.Store.ClaimSmallestSufficient(ctx, s, reservation, minSats)
}

func (l *lockedTierStore) ClaimLargestInsufficient(ctx context.Context, s utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	if s.Tier == l.locked {
		return nil, nil
	}
	return l.Store.ClaimLargestInsufficient(ctx, s, reservation, capSats, limit)
}

func (l *lockedTierStore) ClaimExact(ctx context.Context, s utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	if s.Tier == l.locked {
		return nil, nil
	}
	return l.Store.ClaimExact(ctx, s, reservation, denomination, count)
}

// ClaimableExists is the capability the funder type-asserts for. The locked
// tier answers true (the coins exist, the claim just could not see them); every
// other tier delegates to the inner store's balance.
func (l *lockedTierStore) ClaimableExists(ctx context.Context, s utxostore.Scope, minSats uint64) (bool, error) {
	l.mu.Lock()
	l.probes = append(l.probes, s.Tier)
	l.mu.Unlock()

	if l.probeErr != nil {
		return false, l.probeErr
	}
	if s.Tier == l.locked {
		return true, nil
	}
	bal, err := l.Balance(ctx, s.UserID, s.Basket)
	if err != nil {
		return false, err
	}
	return bal.Claimable[s.Tier] >= minSats && bal.Claimable[s.Tier] > 0, nil
}

// contendingTierStore is the REJECTED design in harness form: a store that
// reports ErrContention from a claim the moment its tier looks empty, exactly
// as an in-store probe would have. It exists to pin what that costs — see
// TestFund_LockedTierDoesNotPreemptAnotherTier.
type contendingTierStore struct {
	utxostore.Store
	locked utxostore.Tier
}

func (c *contendingTierStore) ClaimSmallestSufficient(ctx context.Context, s utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	if s.Tier == c.locked {
		return nil, utxostore.ErrContention
	}
	return c.Store.ClaimSmallestSufficient(ctx, s, reservation, minSats)
}

func (c *contendingTierStore) ClaimLargestInsufficient(ctx context.Context, s utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	if s.Tier == c.locked {
		return nil, utxostore.ErrContention
	}
	return c.Store.ClaimLargestInsufficient(ctx, s, reservation, capSats, limit)
}

func (c *contendingTierStore) ClaimExact(ctx context.Context, s utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	if s.Tier == c.locked {
		return nil, utxostore.ErrContention
	}
	return c.Store.ClaimExact(ctx, s, reservation, denomination, count)
}
