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
