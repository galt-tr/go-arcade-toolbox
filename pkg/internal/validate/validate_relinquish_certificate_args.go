package validate

import (
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// RelinquishCertificateArgs is ported from go-wallet-toolbox (see upstream docs).
func RelinquishCertificateArgs(args *wdk.RelinquishCertificateArgs) error {
	err := args.Type.Validate()
	if err != nil {
		return fmt.Errorf("invalid type argument: %w", err)
	}

	err = args.SerialNumber.Validate()
	if err != nil {
		return fmt.Errorf("invalid serialNumber argument: %w", err)
	}

	err = args.Certifier.Validate()
	if err != nil {
		return fmt.Errorf("invalid certifier argument: %w", err)
	}
	return nil
}
