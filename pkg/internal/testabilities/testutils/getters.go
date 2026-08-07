package testutils

import (
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// SatoshiValue is ported from go-wallet-toolbox (see upstream docs).
func SatoshiValue(p *wdk.StorageCreateTransactionSdkOutput) primitives.SatoshiValue {
	return p.Satoshis
}
