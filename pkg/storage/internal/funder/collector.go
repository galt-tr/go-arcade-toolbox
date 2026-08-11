package funder

import (
	"fmt"
	"math"

	"github.com/go-softwarelab/common/pkg/must"
	"github.com/go-softwarelab/common/pkg/to"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/txutils"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

// changeOutputSize is the serialized byte length of one P2PKH change output;
// every change output the collector plans grows the transaction by this much.
var changeOutputSize = txutils.P2PKHOutputSize

// utxoCollector accrues allocated coins, the running fee (recomputed from the
// transaction size as inputs and change outputs are added), and the change
// shape. It is a behavioral port of the go-wallet-toolbox funder collector: the
// fee and change math below is reproduced verbatim.
type utxoCollector struct {
	txSats satoshi.Value
	txSize uint64

	fee           satoshi.Value
	feeCalculator *feeCalc

	satsCovered    satoshi.Value
	allocatedUTXOs []*utxostore.UTXO

	outputCount             uint64
	numberOfDesiredUTXOs    uint64
	minimumDesiredUTXOValue uint64
	maxChangeOutputsPerTx   uint64
	changeOutputsCount      uint64
	minimumChange           uint64
	// requireChange forces a change output even when the transaction already has
	// one of its own. See FundArgs.RequireChange.
	requireChange bool
	// dustFloor is the minimum satoshi value a change output must have to be economically viable.
	// An output below this threshold costs more to spend in a future transaction than it is worth.
	dustFloor satoshi.Value
}

func newCollector(txSats satoshi.Value, txSize, outputCount uint64, numberOfDesiredUTXOs int64, minimumDesiredUTXOValue uint64, feeCalculator *feeCalc, maxChangeOutputsPerTx uint64, requireChange bool) (c *utxoCollector, err error) {
	c = &utxoCollector{
		txSats:        txSats,
		outputCount:   outputCount,
		requireChange: requireChange,
		// Clamped to at least 1: a zero value would divide-by-zero in calculateChangeCount.
		// Baskets should never carry 0 here, but this is the last line of defense against
		// any caller/DB state that slips through.
		minimumDesiredUTXOValue: to.NoLessThan(minimumDesiredUTXOValue, 1),
		maxChangeOutputsPerTx:   maxChangeOutputsPerTx,
		feeCalculator:           feeCalculator,
		allocatedUTXOs:          make([]*utxostore.UTXO, 0),
	}

	err = c.increaseSize(txSize)
	if err != nil {
		return nil, fmt.Errorf("failed to increase transaction size: %w", err)
	}

	c.numberOfDesiredUTXOs = must.ConvertToUInt64(to.NoLessThan(numberOfDesiredUTXOs, 1))

	// Calculate dust floor: the minimum satoshi value for a change output to be worth spending.
	// We model the smallest possible future spend (1 P2PKH input + 1 P2PKH output)
	// and require each output to be worth at least 2× that future fee.
	// The absolute floor of 1 prevents nonsensical behavior at fee rate 0.
	minSpendTxSize := txutils.TransactionSizeFromScriptLengths(
		[]uint64{txutils.P2PKHUnlockingScriptLength},
		[]uint64{txutils.P2PKHLockingScriptLength},
	)
	c.dustFloor = satoshi.Value(math.Max(1, math.Ceil(float64(minSpendTxSize)/1000*feeCalculator.value)*2))

	c.calculateMinimumChange()

	err = c.calculateChangeOutputs()
	if err != nil {
		return nil, fmt.Errorf("failed to calculate change outputs: %w", err)
	}

	return c, nil
}

// changeMandatory reports whether this transaction is invalid without a change
// output — either because it carries no other output at all, or because the
// caller pinned the output shape (FundArgs.RequireChange). In both cases the
// walk must keep allocating until the change clears the dust floor, rather than
// stopping the moment the target is covered and donating the remainder.
func (c *utxoCollector) changeMandatory() bool {
	return c.outputCount == 0 || c.requireChange
}

func (c *utxoCollector) remaining() satoshi.Value {
	if c.changeMandatory() {
		change := c.change()
		if c.changeOutputsCount > 0 && change < c.dustFloor {
			feeWithNextInput, err := c.feeCalculator.Calculate(c.txSize + txutils.P2PKHEstimatedInputSize)
			if err != nil {
				panic(fmt.Errorf("failed to calculate fee for next change input: %w", err))
			}

			toCover := satoshi.MustAdd(satoshi.MustAdd(c.txSats, feeWithNextInput), c.dustFloor)
			if toCover > c.satsCovered {
				return satoshi.MustSubtract(toCover, c.satsCovered)
			}
		}

		if c.changeOutputsCount == 0 {
			feeWithFirstChangeOutput, err := c.feeCalculator.Calculate(c.txSize + txutils.P2PKHEstimatedInputSize + changeOutputSize)
			if err != nil {
				panic(fmt.Errorf("failed to calculate fee for first change output: %w", err))
			}

			toCover := satoshi.MustAdd(satoshi.MustAdd(c.txSats, feeWithFirstChangeOutput), c.dustFloor)
			if toCover > c.satsCovered {
				return satoshi.MustSubtract(toCover, c.satsCovered)
			}
		}
	}

	return satoshi.MustSubtract(c.satsToCover(), c.satsCovered)
}

// isFunded reports whether the collected coins cover the target plus fee.
func (c *utxoCollector) isFunded() bool {
	// A valid Bitcoin transaction must have at least one output, and a caller
	// that pinned its output shape cannot absorb a dropped change output. In
	// either case "covered" is not enough: the change must also survive
	// prepareResult's dust-floor check, or the transaction comes out the wrong
	// shape. Keep allocating until it does.
	if c.changeMandatory() {
		return c.changeOutputsCount > 0 && c.change() >= c.dustFloor
	}

	return c.satsCovered >= c.satsToCover()
}

// result returns the funding result when funded; otherwise ErrNotEnoughFunds.
func (c *utxoCollector) result() (*Result, error) {
	if c.isFunded() {
		return c.prepareResult()
	}
	return nil, ErrNotEnoughFunds
}

func (c *utxoCollector) allocateUTXO(utxo *utxostore.UTXO) (err error) {
	c.addToAllocated(utxo)

	// InputSize is the unlocking-SCRIPT length (P2PKH default 107); the input's
	// full serialized contribution also includes the 41-byte outpoint+sequence+
	// scriptLen-varint overhead. Price the whole input — the same figure the
	// look-ahead in remaining() uses (txutils.P2PKHEstimatedInputSize) — so the
	// committed fee covers the real tx size. Counting only the script here
	// undercounts each input by 41 bytes and, at a fee rate sitting on arcade's
	// GoBDK min-fee floor, drops the broadcast below it (insufficient-fee).
	err = c.increaseSize(txutils.TransactionInputSize(uint64(utxo.InputSize)))
	if err != nil {
		return fmt.Errorf("failed to increase tx size: %w", err)
	}

	err = c.increaseValue(satoshi.MustFrom(utxo.Satoshis))
	if err != nil {
		return fmt.Errorf("failed to increase tx value: %w", err)
	}

	err = c.calculateChangeOutputs()
	if err != nil {
		return fmt.Errorf("failed to calculate change outputs: %w", err)
	}

	return nil
}

func (c *utxoCollector) addToAllocated(utxo *utxostore.UTXO) {
	c.allocatedUTXOs = append(c.allocatedUTXOs, utxo)
}

func (c *utxoCollector) increaseSize(size uint64) (err error) {
	c.txSize += size
	c.fee, err = c.feeCalculator.Calculate(c.txSize)
	if err != nil {
		return fmt.Errorf("failed to calculate fee: %w", err)
	}
	return nil
}

func (c *utxoCollector) increaseValue(sats satoshi.Value) error {
	var err error
	c.satsCovered, err = satoshi.Add(c.satsCovered, sats)
	if err != nil {
		return fmt.Errorf("cannot increase tx value: %w", err)
	}
	return nil
}

func (c *utxoCollector) satsToCover() satoshi.Value {
	return satoshi.MustAdd(c.txSats, c.fee)
}

func (c *utxoCollector) change() satoshi.Value {
	return satoshi.MustSubtract(c.satsCovered, c.satsToCover())
}

func (c *utxoCollector) prepareResult() (*Result, error) {
	changeAmount := c.change()

	// If the change amount is below the dust floor, it is uneconomical to create any change output.
	// Discard all change outputs and give the amount as extra fee to the miner.
	if changeAmount < c.dustFloor {
		c.changeOutputsCount = 0
	}

	return &Result{
		AllocatedUTXOs:     c.allocatedUTXOs,
		Fee:                c.fee,
		ChangeAmount:       changeAmount,
		ChangeOutputsCount: c.changeOutputsCount,
		DustFloor:          c.dustFloor,
	}, nil
}

func (c *utxoCollector) calculateChangeOutputs() error {
	change := c.change()
	if change <= 0 {
		return nil
	}

	c.calculateChangeCount(must.ConvertToUInt64(change))

	err := c.increaseSize(c.changeOutputsCount * changeOutputSize)
	if err != nil {
		return fmt.Errorf("failed to increase transaction size: %w", err)
	}

	return nil
}

func (c *utxoCollector) calculateChangeCount(changeVal uint64) {
	c.changeOutputsCount = changeVal/c.minimumDesiredUTXOValue + 1

	if changeVal%c.minimumDesiredUTXOValue < c.minimumChange {
		c.changeOutputsCount--
	}

	capCount := c.numberOfDesiredUTXOs
	if c.maxChangeOutputsPerTx < capCount {
		capCount = c.maxChangeOutputsPerTx
	}

	c.changeOutputsCount = to.ValueBetween(c.changeOutputsCount, 1, capCount)

	dustFloorU64 := c.dustFloor.MustUInt64()
	for c.changeOutputsCount > 1 && changeVal/c.changeOutputsCount < dustFloorU64 {
		c.changeOutputsCount--
	}
}

// calculateMinimumChange determines the minimum change amount based on the **Desired** minimum UTXO value.
// The "desired" minimum UTXO value represents the user's preference for common UTXO values in the basket.
// In contrast, the minimum change is the threshold below which a new UTXO is not created.
func (c *utxoCollector) calculateMinimumChange() {
	c.minimumChange = c.minimumDesiredUTXOValue / 4
}
