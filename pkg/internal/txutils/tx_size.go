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

func varIntSize(val uint64) uint64 {
	length := sdk.VarInt(val).Length()
	return toU64(length)
}

//nolint:gosec // No need to check for overflows from int to uint64 here
func toU64(val int) uint64 {
	return uint64(val)
}
