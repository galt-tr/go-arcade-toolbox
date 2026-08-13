package validate

import (
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// ValidRelinquishOutputArgs is ported from go-wallet-toolbox (see upstream docs).
func ValidRelinquishOutputArgs(args *wdk.RelinquishOutputArgs) error {
	err := primitives.OutpointString(args.Output).Validate()
	if err != nil {
		return fmt.Errorf("invalid outpoint: %w", err)
	}

	if args.Basket == "" {
		// NOTE: An empty basket is allowed - this way any basket can be relinquished.
		return nil
	}

	err = primitives.StringUnder300(args.Basket).Validate()
	if err != nil {
		return fmt.Errorf("invalid basket: %w", err)
	}

	return nil
}
