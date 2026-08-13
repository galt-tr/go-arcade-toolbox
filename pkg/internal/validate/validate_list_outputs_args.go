package validate

import (
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

const (
	// MaxPaginationLimit is ported from go-wallet-toolbox (see upstream docs).
	MaxPaginationLimit = 10000
	// MaxPaginationOffset is ported from go-wallet-toolbox (see upstream docs).
	MaxPaginationOffset = 1_000_000
	// MinPaginationLimit is ported from go-wallet-toolbox (see upstream docs).
	MinPaginationLimit = 1
)

// ListOutputsArgs is ported from go-wallet-toolbox (see upstream docs).
func ListOutputsArgs(args *wdk.ListOutputsArgs) error {
	if args == nil {
		return fmt.Errorf("args cannot be nil")
	}

	if err := args.TagQueryMode.Validate(); err != nil {
		return fmt.Errorf("invalid tagQueryMode: %s", *args.TagQueryMode)
	}

	if args.Limit < MinPaginationLimit {
		return fmt.Errorf("limit must be greater than 0")
	}
	if args.Limit > MaxPaginationLimit {
		return fmt.Errorf("limit exceeds max allowed value of %d", MaxPaginationLimit)
	}
	if args.Offset > MaxPaginationOffset {
		return fmt.Errorf("offset is too large")
	}

	for _, txid := range args.KnownTxids {
		if err := primitives.TXIDHexString(txid).Validate(); err != nil {
			return fmt.Errorf("invalid txid: %w", err)
		}
	}

	return nil
}
