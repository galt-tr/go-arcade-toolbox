package mapping

import (
	"github.com/bsv-blockchain/go-sdk/overlay"

	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
)

// MapToOverlayNetwork is ported from go-wallet-toolbox (see upstream docs).
func MapToOverlayNetwork(chain defs.BSVNetwork) overlay.Network {
	switch chain {
	case defs.NetworkMainnet:
		return overlay.NetworkMainnet
	case defs.NetworkTestnet, defs.NetworkTTN, defs.NetworkTSTN:
		// ttn/tstn are testnet-based; overlay only distinguishes mainnet vs testnet.
		return overlay.NetworkTestnet
	default:
		return overlay.NetworkTestnet
	}
}
