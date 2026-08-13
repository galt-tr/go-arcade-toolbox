package mapping

import (
	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/go-softwarelab/common/pkg/to"

	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/assembler"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// MapProcessActionArgsForNewTx is ported from go-wallet-toolbox (see upstream docs).
func MapProcessActionArgsForNewTx(txid *chainhash.Hash, tx *assembler.AssembledTransaction, reference string, wdkArgs wdk.ValidCreateActionArgs) wdk.ProcessActionArgs {
	processActionArgs := wdk.ProcessActionArgs{
		IsNewTx:    true,
		IsSendWith: wdkArgs.IsSendWith,
		IsNoSend:   wdkArgs.IsNoSend,
		IsDelayed:  wdkArgs.IsDelayed,
		SendWith:   to.IfThen(wdkArgs.IsSendWith, wdkArgs.Options.SendWith).ElseThen([]primitives.TXIDHexString{}),
		TxID:       to.Ptr(primitives.TXIDHexString(txid.String())),
		RawTx:      tx.Bytes(),
		Reference:  &reference,
	}

	return processActionArgs
}

// MapProcessActionArgsForSendWith is ported from go-wallet-toolbox (see upstream docs).
func MapProcessActionArgsForSendWith(wdkArgs wdk.ValidCreateActionArgs) wdk.ProcessActionArgs {
	processActionArgs := wdk.ProcessActionArgs{
		IsNewTx:    false,
		IsNoSend:   false,
		SendWith:   to.IfThen(wdkArgs.Options.SendWith != nil, wdkArgs.Options.SendWith).ElseThen([]primitives.TXIDHexString{}),
		IsSendWith: true,
	}
	return processActionArgs
}
