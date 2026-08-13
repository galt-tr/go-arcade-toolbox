// Package mapping is ported from go-wallet-toolbox (see upstream docs).
package mapping

import (
	sdk "github.com/bsv-blockchain/go-sdk/wallet"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// MapAbortActionArgs maps sdk.AbortActionArgs to wdk.AbortActionArgs
func MapAbortActionArgs(args sdk.AbortActionArgs) wdk.AbortActionArgs {
	return wdk.AbortActionArgs{
		Reference: primitives.Base64String(args.Reference),
	}
}

// MapAbortActionResult maps wdk.AbortActionResult to sdk.AbortActionResult
func MapAbortActionResult(result *wdk.AbortActionResult) *sdk.AbortActionResult {
	return &sdk.AbortActionResult{
		Aborted: result.Aborted,
	}
}
