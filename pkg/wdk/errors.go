package wdk

import (
	"errors"
	"fmt"
)

// ErrNotFoundError represents an error indicating that a requested resource or item was not found.
var ErrNotFoundError = fmt.Errorf("not found")

// ErrNotEnoughFunds is returned when a transaction cannot be funded due to insufficient UTXOs.
var ErrNotEnoughFunds = errors.New("not enough funds")

// ErrUTXOContention is returned when a concurrent transaction reserved one or more of the
// UTXOs this transaction selected, before this transaction could reserve them itself. Callers
// may retry the operation with a fresh UTXO selection.
var ErrUTXOContention = errors.New("utxo contention: concurrent transaction reserved one or more selected UTXOs")

// ErrInputUnavailable is returned when an input the caller named EXPLICITLY
// cannot be held for its action: the coin is already spent, held by another
// in-flight action, frozen, or not in this user's inventory at all.
//
// It exists because neither existing sentinel tells the caller the truth about
// this failure, and the difference decides what they should do next.
// ErrNotEnoughFunds says "storage could not find coins", which invites a retry
// storage would satisfy from anywhere; ErrUTXOContention says "retry with a
// fresh selection", which is advice the caller cannot take — they did not
// select this coin, they named it, and no other coin substitutes for it. What
// this sentinel says is: the outpoint you asked for is not yours to spend right
// now. Whether it ever will be depends on the cause, which travels with the
// error — errors.As it to the utxostore's typed refusal (this package cannot
// name those types without importing the store, so they are spelled out here):
// a SpentError means never, a ReservedError means once the holding action
// completes or aborts, a FrozenError means once an operator lifts the hold, and
// a NotFoundError means never under this user.
var ErrInputUnavailable = errors.New("provided input unavailable")
