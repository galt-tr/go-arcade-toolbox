package txutils

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestInputSize(t *testing.T) {
	// given:
	unlockingScriptSize := uint64(100)

	// when:
	size := TransactionInputSize(unlockingScriptSize)

	// then:
	require.Equal(t, 40+unlockingScriptSize+1, size)
}

func TestOutputSize(t *testing.T) {
	// given:
	lockingScriptSize := uint64(100)

	// when:
	size := TransactionOutputSize(lockingScriptSize)

	// then:
	require.Equal(t, 8+lockingScriptSize+1, size)
}

func TestTransactionSizeFromScriptLengths(t *testing.T) {
	tests := map[string]struct {
		inputScriptLengths  []uint64
		outputScriptLengths []uint64
		expected            uint64
	}{
		"two inputs, two outputs": {
			inputScriptLengths:  []uint64{100, 200},
			outputScriptLengths: []uint64{300, 400},
			expected: 8 + // tx envelope size
				1 + // varint size of inputs count
				141 + // 40+100+1 // input [0] size
				241 + // 40+200+1 // input [1] size
				1 + // varint size of outputs count
				311 + // 8+300+3 // output [0] size
				411, // 8+400+3 // output [1] size
		},
		"zero inputs, two outputs": {
			inputScriptLengths:  []uint64{},
			outputScriptLengths: []uint64{300, 400},
			expected: 8 +
				1 +
				1 +
				311 +
				411,
		},
		"minimal p2pkh 1-in 1-out": {
			inputScriptLengths:  []uint64{P2PKHUnlockingScriptLength},
			outputScriptLengths: []uint64{P2PKHLockingScriptLength},
			// 8 + 1 + (40+107+1) + 1 + (8+25+1) = 192
			expected: 192,
		},
	}
	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			require.Equal(t, test.expected, TransactionSizeFromScriptLengths(test.inputScriptLengths, test.outputScriptLengths))
		})
	}
}

func TestP2PKHConstants(t *testing.T) {
	require.Equal(t, uint64(148), P2PKHEstimatedInputSize, "40 + 107 + 1")
	require.Equal(t, uint64(34), P2PKHOutputSize, "8 + 25 + 1")
}
