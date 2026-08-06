package utxostore

import (
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// DefaultP2PKHInputSize is the default estimated unlocking-script size, in
// bytes, for a P2PKH input (signature + public key push).
const DefaultP2PKHInputSize uint32 = 107

// Tier tracks how settled a coin's minting transaction is. Tiers order risk;
// the funder walks them explicitly, safest first. See the package doc for the
// full tier model.
type Tier uint8

const (
	// TierSending marks a coin whose mint transaction's broadcast is still in
	// flight — the wallet built it but the oracle has not accepted it yet.
	TierSending Tier = 1
	// TierUnproven marks a coin whose mint transaction was accepted by the
	// oracle (SEEN_* status) but has no header-verified proof yet.
	TierUnproven Tier = 2
	// TierMined marks a coin whose mint transaction has a header-verified
	// merkle proof.
	TierMined Tier = 3
)

// Valid reports whether t is one of the defined tiers.
func (t Tier) Valid() bool {
	return t >= TierSending && t <= TierMined
}

// String returns the tier's name, or "Tier(n)" for undefined values.
func (t Tier) String() string {
	switch t {
	case TierSending:
		return "sending"
	case TierUnproven:
		return "unproven"
	case TierMined:
		return "mined"
	default:
		return fmt.Sprintf("Tier(%d)", uint8(t))
	}
}

// Outpoint identifies a transaction output: the minting transaction's ID and
// the output index. It is comparable and usable as a map key.
type Outpoint struct {
	TxID chainhash.Hash
	Vout uint32
}

// String renders the outpoint as "txid:vout" (txid in standard reversed hex).
func (o Outpoint) String() string {
	return fmt.Sprintf("%s:%d", o.TxID.String(), o.Vout)
}

// Scope narrows claim queries. ALL fields are mandatory — one tier per
// micro-query; the funder walks tiers explicitly. Keeping the tier inside the
// scope means every claim is a single index walk on every backend.
type Scope struct {
	UserID int64
	Basket string
	Tier   Tier
}

// UTXO is a coin row as stored: identity, value, and lifecycle state.
// Values returned by [Store.Get] and the claim methods are copies; mutating
// them does not affect the store.
type UTXO struct {
	Outpoint

	// UserID and Basket identify the owner; all operations are scoped to them.
	UserID int64
	Basket string

	// Satoshis is the coin's value.
	Satoshis uint64
	// InputSize is the estimated unlocking-script size in bytes used for fee
	// estimation (P2PKH default: [DefaultP2PKHInputSize]).
	InputSize uint32

	// Tier tracks the settlement state of the minting transaction.
	Tier Tier

	// ReservedBy is the reservation token holding this coin; "" = claimable.
	ReservedBy string
	// ReservedAt is when the coin was reserved; zero unless reserved.
	ReservedAt time.Time

	// SpentBy, when non-nil, is the transaction the wallet spent this coin in
	// (recorded spend, pending oracle/proof confirmation).
	SpentBy *chainhash.Hash

	// Frozen marks an explicit hold: invisible to claims, refuses Spend.
	Frozen bool

	// CreatedAt is when the coin was minted into the store.
	CreatedAt time.Time
}

// Mint is one item of a [Store.Mint] batch: the coin to create plus a
// per-item error slot filled by the store.
type Mint struct {
	Outpoint

	UserID    int64
	Basket    string
	Satoshis  uint64
	InputSize uint32
	Tier      Tier

	// Err is set by the store when this item fails (out parameter). A batch
	// with any Err set also returns [ErrBatch] at the top level.
	Err error
}

// SpendOp is one item of a [Store.Spend] batch: the coin to spend, the
// reservation guard, the spending transaction, and a per-item error slot.
type SpendOp struct {
	Outpoint

	// Reservation is the exact-match guard: the row must currently be
	// reserved by this token. Must be non-empty.
	Reservation string
	// SpendingTxID is the transaction spending the coin.
	SpendingTxID chainhash.Hash

	// Err is set by the store when this item fails (out parameter). A batch
	// with any Err set also returns [ErrBatch] at the top level.
	Err error
}

// Balance summarizes a (user, basket)'s satoshis. Claimable maps each tier to
// the sum of coins that are unspent, unreserved, and not frozen; absent keys
// mean zero. Reserved is the sum of reserved-but-unspent coins across all
// tiers. Frozen unreserved coins and spent coins appear in neither bucket.
type Balance struct {
	Claimable map[Tier]uint64
	Reserved  uint64
}

// ReservationRef describes one reservation: its token, owner, age, and the
// outpoints it currently holds. ReservedAt is the OLDEST row's reservation
// time (a funder may claim several times under one token).
type ReservationRef struct {
	Reservation string
	UserID      int64
	ReservedAt  time.Time
	Outpoints   []Outpoint
}

// RemoveByMintReport classifies the outcome of [Store.RemoveByMintTx] so the
// reconciler can cascade correctly.
type RemoveByMintReport struct {
	// Removed lists the outpoints deleted (they were unreserved and unspent).
	Removed []Outpoint
	// AlreadyReserved lists reservations holding descendants in flight —
	// cascade candidates the reconciler must release or wait out.
	AlreadyReserved []ReservationRef
	// AlreadySpentBy lists the child transactions that spent these coins
	// (deduplicated) — the monitor re-verifies them against the oracle.
	AlreadySpentBy []chainhash.Hash
}
