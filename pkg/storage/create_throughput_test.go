package storage

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore/memstore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// claimSpy wraps a utxostore.Store and records which claim primitives the
// funder invoked, plus the scope/denomination it claimed under. It proves the
// funder took the throughput (ClaimExact) fast path vs. the tiered
// (ClaimSmallestSufficient) walk, and against which basket.
type claimSpy struct {
	utxostore.Store

	exactCalls     int
	exactBasket    string
	exactDenom     uint64
	exactCount     int
	smallestCalls  int
	smallestBasket string
}

func (s *claimSpy) ClaimExact(ctx context.Context, sc utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	s.exactCalls++
	s.exactBasket = sc.Basket
	s.exactDenom = denomination
	s.exactCount = count
	return s.Store.ClaimExact(ctx, sc, reservation, denomination, count)
}

func (s *claimSpy) ClaimSmallestSufficient(ctx context.Context, sc utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	s.smallestCalls++
	s.smallestBasket = sc.Basket
	return s.Store.ClaimSmallestSufficient(ctx, sc, reservation, minSats)
}

// spyHarness is a provider wired over a claim-recording store.
type spyHarness struct {
	p      *Provider
	meta   *metastore.Store
	spy    *claimSpy
	userID int
	auth   wdk.AuthID
}

func newSpyHarness(t *testing.T, opts ...Option) *spyHarness {
	t.Helper()
	ctx := context.Background()

	path := filepath.Join(t.TempDir(), "meta.db")
	meta, err := metastore.OpenSQLite(ctx, path)
	require.NoError(t, err)
	t.Cleanup(func() { _ = meta.Close(ctx) })

	spy := &claimSpy{Store: memstore.New()}
	t.Cleanup(func() { _ = spy.Close(ctx) })

	logger := logging.NewTestLogger(t)
	fnd := funder.New(logger, spy, defs.DefaultFeeModel())

	baseOpts := []Option{
		WithNetwork(defs.NetworkTestnet),
		WithStorageName("test-storage"),
		WithScriptsVerifier(alwaysValidScripts{}),
	}
	baseOpts = append(baseOpts, opts...)

	p, err := New(logger, meta, spy, fnd, &fakeOracle{}, &fakeHeaders{}, baseOpts...)
	require.NoError(t, err)

	_, err = p.Migrate(ctx, "test-storage", "storage-identity-key")
	require.NoError(t, err)

	resp, err := p.FindOrInsertUser(ctx, testIdentityKey)
	require.NoError(t, err)
	require.True(t, resp.IsNew)

	uid := resp.User.UserID
	return &spyHarness{
		p:      p,
		meta:   meta,
		spy:    spy,
		userID: uid,
		auth:   wdk.AuthID{IdentityKey: testIdentityKey, UserID: &uid},
	}
}

// mintInBasket mints one mined coin of sats into the given basket and records
// the coin's source transaction in known_txs so BEEF ancestry resolves.
func (h *spyHarness) mintInBasket(t *testing.T, seed byte, sats uint64, basket string) utxostore.Outpoint {
	t.Helper()
	ctx := context.Background()

	src := transaction.NewTransaction()
	var srcHash chainhash.Hash
	srcHash[0] = seed
	src.AddInput(&transaction.TransactionInput{
		SourceTXID:       &srcHash,
		SourceTxOutIndex: 0,
		SequenceNumber:   transaction.DefaultSequenceNumber,
	})
	src.AddOutput(&transaction.TransactionOutput{Satoshis: sats, LockingScript: testP2PKH(t)})

	txid := src.TxID().String()
	require.NoError(t, h.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
		TxID:   txid,
		Status: wdk.ProvenTxStatusCompleted,
		RawTx:  src.Bytes(),
	}))

	op := utxostore.Outpoint{TxID: *src.TxID(), Vout: 0}
	m := &utxostore.Mint{
		Outpoint:  op,
		UserID:    int64(h.userID),
		Basket:    basket,
		Satoshis:  sats,
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      utxostore.TierMined,
	}
	require.NoError(t, h.spy.Mint(ctx, []*utxostore.Mint{m}))
	require.NoError(t, m.Err)
	return op
}

// throughputUTXOMgmt returns a minimal throughput configuration with an
// explicit denomination and pool basket (New does not validate it).
func throughputUTXOMgmt(denomination uint64) defs.UTXOManagement {
	return defs.UTXOManagement{
		Strategy: defs.StrategyThroughput,
		Throughput: defs.Throughput{
			DenominationSatoshis: denomination,
			SpendPolicy:          defs.SpendPolicyPreferMined,
			PoolBasket:           "fuel",
			ReserveBasket:        "reserve",
		},
	}
}

// TestCreateAction_ThroughputRoutesThroughClaimExact proves the Part-27 wiring:
// with the throughput strategy enabled and a denominated pool, CreateAction
// sets FundArgs.Denomination > 0 and Basket = PoolBasket, so funding takes the
// funder's closed-form ClaimExact fast path against the pool basket — not the
// tiered ClaimSmallestSufficient walk.
func TestCreateAction_ThroughputRoutesThroughClaimExact(t *testing.T) {
	ctx := context.Background()
	const denom = 100_000
	h := newSpyHarness(t, WithUTXOManagement(throughputUTXOMgmt(denom)))

	// A single denominated fuel coin in the pool covers a 40k payment + fee.
	op := h.mintInBasket(t, 0x11, denom, "fuel")

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	require.NotNil(t, res)

	// The fast path was taken: ClaimExact against the pool basket at the
	// resolved denomination, and no tiered smallest-sufficient scan.
	assert.Equal(t, 1, h.spy.exactCalls, "throughput funding must call ClaimExact")
	assert.Equal(t, "fuel", h.spy.exactBasket, "ClaimExact must target the fuel pool basket")
	assert.Equal(t, uint64(denom), h.spy.exactDenom, "ClaimExact denomination must be the resolved fuel denomination")
	assert.Positive(t, h.spy.exactCount, "ClaimExact must request >= 1 coin")
	assert.Zero(t, h.spy.smallestCalls, "a fully-funded fast path must not fall back to the tiered walk")

	// The pool coin is reserved under the action reference (funding succeeded
	// from the pool, not from 'default').
	u, err := h.spy.Get(ctx, op)
	require.NoError(t, err)
	assert.Equal(t, res.Reference, u.ReservedBy)
	assert.Equal(t, "fuel", u.Basket)
}

// TestCreateAction_PrivacyRoutesThroughTieredWalk proves the default path is
// untouched: with throughput OFF, Denomination stays 0 and funding takes the
// tiered ClaimSmallestSufficient walk over the change basket.
func TestCreateAction_PrivacyRoutesThroughTieredWalk(t *testing.T) {
	ctx := context.Background()
	h := newSpyHarness(t) // default: privacy strategy

	h.mintInBasket(t, 0x22, 100_000, wdk.BasketNameForChange)

	res, err := h.p.CreateAction(ctx, h.auth, paymentArgs(40_000))
	require.NoError(t, err)
	require.NotNil(t, res)

	assert.Zero(t, h.spy.exactCalls, "privacy funding must not call ClaimExact")
	assert.Positive(t, h.spy.smallestCalls, "privacy funding must use the tiered smallest-sufficient walk")
	assert.Equal(t, wdk.BasketNameForChange, h.spy.smallestBasket, "tiered walk must target the change basket")
}

// TestChangeDestinationBasket proves the self-replenishing routing: in the
// throughput strategy a payment's change goes back into the fuel pool (so the
// pool refills 1:1 without the keeper recycling per payment), while a fan-out
// mint's change returns to its funding source and the privacy strategy keeps
// change in the default basket.
func TestChangeDestinationBasket(t *testing.T) {
	const denom = 100_000
	tp := newSpyHarness(t, WithUTXOManagement(throughputUTXOMgmt(denom)))

	// Payment (no fuel shape) → the pool basket self-replenishes.
	assert.Equal(t, "fuel", tp.p.changeDestinationBasket(nil),
		"throughput payment change must route into the fuel pool")

	// Fan-out leaf mint (destination = pool) → change returns to the reserve
	// source, never the pool it is filling.
	leaf := &wdk.ShapedChange{Count: 1, Satoshis: denom, Basket: "fuel"}
	assert.Equal(t, "reserve", tp.p.changeDestinationBasket(leaf),
		"fan-out leaf change must return to the reserve funding source")

	// Privacy strategy: change stays in the default basket.
	pv := newSpyHarness(t)
	assert.Equal(t, wdk.BasketNameForChange, pv.p.changeDestinationBasket(nil),
		"privacy change must stay in the default basket")
}
