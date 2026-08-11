// Package txutils holds the transaction-size arithmetic the funder's fee
// calculator and dust-floor computation depend on. It is a deliberately minimal
// port of go-wallet-toolbox's txutils: only the byte-size helpers the funder
// actually uses were carried over.
package txutils

import (
	sdk "github.com/bsv-blockchain/go-sdk/util"
)

const (
	txEnvelopeSize = 4 + 4 // version + locktime
	satoshisSize   = 8
	inputConstSize = 32 + 4 + 4 // txID + vout + sequence
)

// TransactionInputSize calculates the size in bytes of a transaction input
// with the given script size.
func TransactionInputSize(scriptSize uint64) uint64 {
	return inputConstSize +
		varIntSize(scriptSize) +
		scriptSize
}

// TransactionOutputSize calculates the serialized byte length of a transaction
// output with the given script size in bytes.
func TransactionOutputSize(scriptSize uint64) uint64 {
	return varIntSize(scriptSize) +
		scriptSize +
		satoshisSize
}

// TransactionSizeFromScriptLengths calculates the total size of a transaction
// from concrete slices of input unlocking-script lengths and output
// locking-script lengths.
func TransactionSizeFromScriptLengths(inputScriptLengths, outputScriptLengths []uint64) uint64 {
	var inputsSize uint64
	for _, scriptLen := range inputScriptLengths {
		inputsSize += TransactionInputSize(scriptLen)
	}

	var outputsSize uint64
	for _, scriptLen := range outputScriptLengths {
		outputsSize += TransactionOutputSize(scriptLen)
	}

	return txEnvelopeSize +
		varIntSize(uint64(len(inputScriptLengths))) +
		inputsSize +
		varIntSize(uint64(len(outputScriptLengths))) +
		outputsSize
}

// MinRequiredFee returns the smallest fee, in satoshis, that a node applying a
// satPerKB floor will accept for a transaction of sizeBytes.
//
// It reproduces bitcoin-sv's CFeeRate::GetFee exactly — integer arithmetic, so
// TRUNCATING division, then a one-satoshi minimum for any non-empty transaction
// at a non-zero rate — because that is the arithmetic the receiving node runs.
// Reimplementing it in floating point, or rounding the other way, would put the
// client's idea of "enough" a satoshi away from the node's on some sizes, which
// is the entire failure mode this function exists to rule out.
//
// The size is the STANDARD serialization, not the extended format. Extended
// format is only the encoding a transaction is submitted in; the prevout
// satoshis and source locking scripts it carries inline are handed to the
// validator as separate spent-coin data and are not billed. Verified directly
// against arcade's BDK engine: a 1-input transaction whose source locking script
// is 1000 bytes has a 73-byte standard size and a 1090-byte extended size, and
// at 100 sat/kB it is accepted with a fee of 7 — floor(100*73/1000) — not 109.
func MinRequiredFee(sizeBytes uint64, satPerKB int64) uint64 {
	if satPerKB <= 0 || sizeBytes == 0 {
		return 0
	}
	fee := sizeBytes * uint64(satPerKB) / 1000
	if fee == 0 {
		return 1
	}
	return fee
}

func varIntSize(val uint64) uint64 {
	length := sdk.VarInt(val).Length()
	return toU64(length)
}

//nolint:gosec // No need to check for overflows from int to uint64 here
func toU64(val int) uint64 {
	return uint64(val)
}
