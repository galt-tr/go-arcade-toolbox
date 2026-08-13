package funder

import (
	"fmt"
	"math"

	"github.com/go-softwarelab/common/pkg/to"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/satoshi"
)

// feeCalc is a static sat/kB fee model with per-byte precision: it never rounds
// the transaction size up to the nearest kilobyte before applying the rate.
type feeCalc struct {
	bytes float64
	value float64
}

// newFeeCalculator builds a feeCalc from a defs.FeeModel. It panics on an
// unsupported model type or a negative rate — both are configuration bugs.
func newFeeCalculator(model defs.FeeModel) *feeCalc {
	if model.Type != defs.SatPerKB {
		panic("unsupported fee model")
	}

	if model.Value < 0 {
		panic("fee model value cannot be negative")
	}

	feeValue, err := to.Float64(model.Value)
	if err != nil {
		panic("invalid fee model value: " + err.Error())
	}

	return &feeCalc{
		value: feeValue,
		bytes: 1000,
	}
}

// Calculate returns the fee for a transaction of txSize bytes, computed with
// per-byte precision and rounded up (ceil) to the next whole satoshi.
func (f *feeCalc) Calculate(txSize uint64) (satoshi.Value, error) {
	size, err := to.Float64FromUnsigned(txSize)
	if err != nil {
		return 0, fmt.Errorf("invalid transaction size: %w", err)
	}

	feeAmount := math.Ceil(size / f.bytes * f.value)

	fee, err := to.Int64(feeAmount)
	if err != nil {
		return 0, fmt.Errorf("failed to calculate fee value: %w", err)
	}

	sats, err := satoshi.From(fee)
	if err != nil {
		return 0, fmt.Errorf("failed to convert fee to satoshi: %w", err)
	}

	return sats, nil
}
