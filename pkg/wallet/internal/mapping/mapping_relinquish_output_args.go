package mapping

import (
	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// MapRelinquishOutputArgs is ported from go-wallet-toolbox (see upstream docs).
func MapRelinquishOutputArgs(args sdk.RelinquishOutputArgs) wdk.RelinquishOutputArgs {
	return wdk.RelinquishOutputArgs{
		Basket: args.Basket,
		Output: args.Output.String(),
	}
}
