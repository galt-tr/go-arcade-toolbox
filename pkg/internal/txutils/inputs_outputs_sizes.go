package txutils

// P2PKH script lengths in bytes.
const (
	// P2PKHUnlockingScriptLength is the estimated unlocking-script size of a
	// P2PKH input (signature + public key push).
	P2PKHUnlockingScriptLength = 107
	// P2PKHLockingScriptLength is the locking-script size of a P2PKH output.
	P2PKHLockingScriptLength = 25
)

var (
	// P2PKHOutputSize is the serialized byte length of a P2PKH output.
	P2PKHOutputSize = TransactionOutputSize(P2PKHLockingScriptLength)
	// P2PKHEstimatedInputSize is the serialized byte length of a P2PKH input
	// using the estimated unlocking-script size.
	P2PKHEstimatedInputSize = TransactionInputSize(P2PKHUnlockingScriptLength)
)
