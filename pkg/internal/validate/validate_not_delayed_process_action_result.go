package validate

import (
	"fmt"

	"github.com/go-softwarelab/common/pkg/seq"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// NotDelayedProcessActionResult is ported from go-wallet-toolbox (see upstream docs).
func NotDelayedProcessActionResult(result *wdk.ProcessActionResult) error {
	if len(result.NotDelayedResults) == 0 || len(result.SendWithResults) == 0 {
		return nil
	}

	allSent := seq.Every(seq.FromSlice(result.SendWithResults), func(it wdk.SendWithResult) bool {
		return it.Status == wdk.SendWithResultStatusUnproven
	})

	if allSent {
		return nil
	}

	return fmt.Errorf("unexpected not delayed results when send with results are not all unproven")
}
