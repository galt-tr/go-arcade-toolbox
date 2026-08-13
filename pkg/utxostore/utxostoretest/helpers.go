// Package utxostoretest is the backend-agnostic conformance suite for
// [utxostore.Store]. Every provider (memstore, SQLite, PostgreSQL, Aerospike)
// wires its constructor into [RunStoreSuite] and must pass.
//
// The suite asserts BEHAVIOR — state transitions, per-item error shapes,
// errors.Is/As matching — never implementation details. Providers whose claim
// selection is approximate (bucket-based backends such as Aerospike) still
// pass: assertions that require optimal selection (the true smallest
// sufficient coin, strict largest-first ordering, oldest-first stale
// reservations) are gated behind [WithExactSelection], which exact backends
// (memstore, SQL) enable and approximate backends omit. Without the option
// the suite still asserts the predicate itself (sufficiency, cap, scope,
// exclusivity) on every returned coin. [utxostore.ErrContention] is treated
// as a legal transient response in the concurrency subtest, so optimistic
// backends pass without weakening the exclusivity assertion.
//
// Timestamps: providers whose reservation timestamps come from an injectable
// clock should also pass [WithManualClock]; it makes the timestamp-sensitive
// subtests deterministic (no sleeps) and enables the oldest-row reservation
// dating subtest. Without it, the suite falls back to short wall-clock
// sleeps, so backends enabling [WithExactSelection] must persist reservation
// timestamps with sub-millisecond fidelity.
package utxostoretest

import (
	"context"
	"crypto/sha256"
	"testing"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/stretchr/testify/require"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// NewTxID returns a deterministic transaction ID derived from seed, so tests
// can mint reproducible outpoints without building real transactions.
func NewTxID(seed string) chainhash.Hash {
	return chainhash.Hash(sha256.Sum256([]byte(seed)))
}

// NewOutpoint returns a deterministic outpoint: output vout of the synthetic
// transaction identified by txSeed.
func NewOutpoint(txSeed string, vout uint32) utxostore.Outpoint {
	return utxostore.Outpoint{TxID: NewTxID(txSeed), Vout: vout}
}

// NewMint builds a mint item for op with the default P2PKH input size.
func NewMint(op utxostore.Outpoint, userID int64, basket string, tier utxostore.Tier, satoshis uint64) *utxostore.Mint {
	return &utxostore.Mint{
		Outpoint:  op,
		UserID:    userID,
		Basket:    basket,
		Satoshis:  satoshis,
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      tier,
	}
}

// MintTx mints one synthetic transaction (identified by txSeed) with
// len(satoshis) outputs into s and returns their outpoints, vout i carrying
// satoshis[i]. It fails the test on any error.
func MintTx(t *testing.T, s utxostore.Store, txSeed string, userID int64, basket string, tier utxostore.Tier, satoshis ...uint64) []utxostore.Outpoint {
	t.Helper()

	mints := make([]*utxostore.Mint, len(satoshis))
	ops := make([]utxostore.Outpoint, len(satoshis))
	for i, sats := range satoshis {
		ops[i] = NewOutpoint(txSeed, uint32(i)) //nolint:gosec // i < len(satoshis), never overflows
		mints[i] = NewMint(ops[i], userID, basket, tier, sats)
	}
	require.NoError(t, s.Mint(context.Background(), mints))
	for _, m := range mints {
		require.NoError(t, m.Err)
	}
	return ops
}
