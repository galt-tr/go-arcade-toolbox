package utxostore

import (
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Input validation shared by every backend.
//
// The preconditions [Store] documents are the same on all of them — a claim
// needs a token and a fully specified scope, a mint needs an owner, a spend
// needs the funding run it belongs to — so the checks live here rather than
// once per backend. They were triplicated verbatim (aerostore, memstore,
// sqlstore) and the copies could only ever drift apart; a store that answered
// a malformed call differently from its peers would break the conformance
// suite's central promise that the three are interchangeable.
//
// Everything here reports a PROGRAMMER ERROR: a plain error, never one of the
// typed refusals in errors.go, because no row state is involved and no retry
// can help. They are exported so an out-of-tree backend registered through
// [Register] can enforce the same contract with the same words.

// ValidateReservation rejects an empty reservation token. Every method that
// takes one requires it: the token is what ties a held coin back to the action
// holding it, so an unnamed hold could never be released by its owner.
func ValidateReservation(reservation string) error {
	if reservation == "" {
		return errors.New("utxostore: reservation must be non-empty")
	}
	return nil
}

// ValidateScope rejects an underspecified [Scope]. All three fields are
// mandatory — see the type's doc for why the tier belongs inside the scope.
func ValidateScope(sc Scope) error {
	switch {
	case sc.UserID <= 0:
		return errors.New("utxostore: scope user id must be positive")
	case sc.Basket == "":
		return errors.New("utxostore: scope basket must be non-empty")
	case !sc.Tier.Valid():
		return fmt.Errorf("utxostore: invalid scope tier %d", sc.Tier)
	}
	return nil
}

// ValidateClaim rejects underspecified claim inputs: the reservation token
// plus the scope, in that order. It guards the three Claim* methods.
func ValidateClaim(sc Scope, reservation string) error {
	if err := ValidateReservation(reservation); err != nil {
		return err
	}
	return ValidateScope(sc)
}

// ValidateReserveOutpoints rejects underspecified [Store.ReserveOutpoints]
// inputs. An empty op list is a programmer error rather than a degenerate
// success: the caller asked for an all-or-nothing hold and named nothing to
// hold.
func ValidateReserveOutpoints(reservation string, ops []Outpoint) error {
	if err := ValidateReservation(reservation); err != nil {
		return err
	}
	if len(ops) == 0 {
		return errors.New("utxostore: ops must be non-empty")
	}
	return nil
}

// ValidateMint rejects an underspecified [Mint] item. Satoshis and InputSize
// are deliberately unchecked: a zero-value coin is odd but representable, while
// an unowned or unbasketed one is not addressable by any claim.
func ValidateMint(m *Mint) error {
	switch {
	case m.UserID <= 0:
		return fmt.Errorf("utxostore: mint %s: user id must be positive", m.Outpoint)
	case m.Basket == "":
		return fmt.Errorf("utxostore: mint %s: basket must be non-empty", m.Outpoint)
	case !m.Tier.Valid():
		return fmt.Errorf("utxostore: mint %s: invalid tier %d", m.Outpoint, m.Tier)
	}
	return nil
}

// ValidateSpend rejects an underspecified [SpendOp]. The reservation is
// required in BOTH spend modes — fact mode does not CHECK it, but the caller
// must still name the funding run whose coins it believes it is recording.
func ValidateSpend(sp *SpendOp) error {
	if sp.Reservation == "" {
		return fmt.Errorf("utxostore: spend %s: reservation must be non-empty", sp.Outpoint)
	}
	return nil
}

// ValidateMintOutpoints enforces [Store.RemoveByMintTx]'s whole-call guard:
// every op must be an output of mintTxID. A mismatch is a caller bug, so it
// fails the entire call rather than being reported per item — the caller has
// misidentified which transaction it is invalidating, and the ops it named
// cannot be trusted to be the phantom coins it meant.
func ValidateMintOutpoints(mintTxID chainhash.Hash, ops []Outpoint) error {
	for _, op := range ops {
		if op.TxID != mintTxID {
			return fmt.Errorf("utxostore: outpoint %s is not an output of mint tx %s", op, mintTxID.String())
		}
	}
	return nil
}
