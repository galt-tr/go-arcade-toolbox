package fixtures

import (
	"testing"

	"github.com/bsv-blockchain/universal-test-vectors/pkg/testabilities"
	"github.com/go-softwarelab/common/pkg/to"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// DefaultProcessActionArgs is ported from go-wallet-toolbox (see upstream docs).
func DefaultProcessActionArgs(_ *testing.T) wdk.ProcessActionArgs {
	spec := testabilities.GivenTX().WithInput(1000).WithP2PKHOutput(999)

	return wdk.ProcessActionArgs{
		IsNewTx:    true,
		IsSendWith: false,
		IsNoSend:   false,
		IsDelayed:  false,
		Reference:  to.Ptr(Reference),
		TxID:       to.Ptr(primitives.TXIDHexString(spec.ID().String())),
		RawTx:      spec.TX().Bytes(),
		SendWith:   []primitives.TXIDHexString{},
	}
}
