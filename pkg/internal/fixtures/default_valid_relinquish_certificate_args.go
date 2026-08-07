package fixtures

import (
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// DefaultValidRelinquishCertificateArgs is ported from go-wallet-toolbox (see upstream docs).
func DefaultValidRelinquishCertificateArgs() *wdk.RelinquishCertificateArgs {
	return &wdk.RelinquishCertificateArgs{
		Type:         TypeField,
		SerialNumber: SerialNumber,
		Certifier:    Certifier,
	}
}
