// Package walletargs is ported from go-wallet-toolbox (see upstream docs).
package walletargs

import (
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/wallet"
	"github.com/go-softwarelab/common/pkg/to"
)

// WithLockingScript is ported from go-wallet-toolbox (see upstream docs).
func WithLockingScript(lockingScript script.Script) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Outputs[0].LockingScript = lockingScript
		args.Outputs[0].CustomInstructions = ""
	}
}

// WithNoOutputs is ported from go-wallet-toolbox (see upstream docs).
func WithNoOutputs() func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Outputs = nil
	}
}

// WithInputBEEF is ported from go-wallet-toolbox (see upstream docs).
func WithInputBEEF(inputBEEF []byte) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.InputBEEF = inputBEEF
	}
}

// WithInputs is ported from go-wallet-toolbox (see upstream docs).
func WithInputs(inputs []wallet.CreateActionInput) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Inputs = inputs
	}
}

// WithInput is ported from go-wallet-toolbox (see upstream docs).
func WithInput(inputSource CreateActionInputSource) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.InputBEEF = inputSource.InputBEEFBytes()
		args.Inputs = []wallet.CreateActionInput{
			inputSource.CreateActionInput(),
		}
	}
}

// WithSignAndProcess is ported from go-wallet-toolbox (see upstream docs).
func WithSignAndProcess(signAndProcess bool) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Options.SignAndProcess = to.Ptr(signAndProcess)
	}
}

// WithNoSend is ported from go-wallet-toolbox (see upstream docs).
func WithNoSend(noSend bool) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Options.NoSend = to.Ptr(noSend)
	}
}

// WithSendWith is ported from go-wallet-toolbox (see upstream docs).
func WithSendWith(sendWith ...chainhash.Hash) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Options.SendWith = sendWith
	}
}

// WithDelayedBroadcast is ported from go-wallet-toolbox (see upstream docs).
func WithDelayedBroadcast() func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Options.AcceptDelayedBroadcast = to.Ptr(true)
	}
}

// WithSatoshisAsFirstOutput is ported from go-wallet-toolbox (see upstream docs).
func WithSatoshisAsFirstOutput(satoshis uint64) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		if len(args.Outputs) == 0 {
			panic("no provided outputs")
		}
		args.Outputs[0].Satoshis = satoshis
	}
}

// WithNoSendChangeOutputs is ported from go-wallet-toolbox (see upstream docs).
func WithNoSendChangeOutputs(changeOutputs ...transaction.Outpoint) func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Options.NoSendChange = changeOutputs
	}
}

// WithoutProvidedOutputs is ported from go-wallet-toolbox (see upstream docs).
func WithoutProvidedOutputs() func(args *wallet.CreateActionArgs) {
	return func(args *wallet.CreateActionArgs) {
		args.Outputs = nil
	}
}
