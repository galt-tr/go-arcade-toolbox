package funder

import (
	"fmt"

	"github.com/go-softwarelab/common/pkg/seq"

	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// Result is the outcome of a successful funding call: the coins the funder
// reserved in the store, the fee those inputs must pay, and the change shape
// the caller should mint. The reserved coins are held under the reservation
// token passed in FundArgs; the caller spends them (or releases the
// reservation) — the funder does not release a successful allocation.
type Result struct {
	// AllocatedUTXOs are the coins reserved in the store for this transaction.
	AllocatedUTXOs []*utxostore.UTXO
	// ChangeOutputsCount is how many change outputs the caller should create;
	// 0 means the change was below the dust floor and is donated to the fee.
	ChangeOutputsCount uint64
	// ChangeAmount is the total satoshis available for change (inputs − target − fee).
	ChangeAmount satoshi.Value
	// Fee is the fee the allocated inputs pay at the configured rate.
	Fee satoshi.Value
	// DustFloor is the minimum economically-viable value for a single change output.
	DustFloor satoshi.Value
}

// TotalAllocated sums the satoshis of every allocated coin.
func (fr *Result) TotalAllocated() (satoshi.Value, error) {
	total, err := satoshi.Sum(seq.Map(seq.FromSlice(fr.AllocatedUTXOs), func(utxo *utxostore.UTXO) uint64 {
		return utxo.Satoshis
	}))
	if err != nil {
		return 0, fmt.Errorf("failed to sum allocated UTXOs: %w", err)
	}

	return total, nil
}
