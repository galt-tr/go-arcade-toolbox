package funder

import "errors"

var (
	// ErrNotEnoughFunds is returned when a full walk over the configured tiers
	// cannot cover the funding target. The reservation is released before it is
	// returned, so no coins are left held.
	ErrNotEnoughFunds = errors.New("funder: not enough funds to cover the funding target")

	// ErrUTXOContention is returned when optimistic-store contention
	// ([utxostore.ErrContention]) persists across every bounded retry. The
	// reservation is released before it is returned. It is transient at the
	// caller's level: retrying the whole action later may succeed.
	ErrUTXOContention = errors.New("funder: persistent UTXO contention, retries exhausted")
)
