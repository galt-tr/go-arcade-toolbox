package funder

import (
	"errors"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

var (
	// ErrNotEnoughFunds is returned when a full walk over the configured tiers
	// cannot cover the funding target. The reservation is released before it is
	// returned, so no coins are left held. It wraps the public
	// [wdk.ErrNotEnoughFunds] sentinel so callers outside the module can treat
	// insufficient-funds as back-pressure via errors.Is without importing this
	// internal package.
	ErrNotEnoughFunds = fmt.Errorf("funder: not enough funds to cover the funding target: %w", wdk.ErrNotEnoughFunds)

	// ErrUTXOContention is returned when optimistic-store contention
	// ([utxostore.ErrContention]) persists across every bounded retry. The
	// reservation is released before it is returned. It is transient at the
	// caller's level: retrying the whole action later may succeed.
	ErrUTXOContention = errors.New("funder: persistent UTXO contention, retries exhausted")
)
