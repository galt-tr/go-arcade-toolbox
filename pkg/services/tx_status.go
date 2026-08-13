package services

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// getStatusConcurrency bounds parallel GET /tx/{txid} lookups issued by
// [Services.GetStatusForTxIDs]: a status-sync caller may ask about thousands
// of txids at once, and one-by-one polling would make that window minutes
// long against a real Arcade instance.
const getStatusConcurrency = 16

// GetStatusForTxIDs reports mined/known/unknown status per txID by querying
// Arcade ([arcade.TxOracle.GetTx]) for each, with lookups bounded to
// [getStatusConcurrency] in flight at once.
//
// Mapping:
//   - MINED / IMMUTABLE -> "mined", with Depth = tip - blockHeight + 1 (min 1
//     when the tip can't be resolved or looks inconsistent with blockHeight).
//   - REJECTED / UNKNOWN / not found (404) -> "unknown" (not on chain), no depth.
//   - any other lifecycle status -> "known" (seen, unconfirmed), depth 0.
//
// The tip used for depth is resolved once per call via headers.CurrentHeight;
// a failure there is tolerated (logged, depth then defaults to the
// conservative minimum of 1 for mined txs) rather than failing the whole
// call — status is still useful without a precise depth.
//
// Individual lookup failures (anything other than a clean 404) are tolerated
// as long as at least one lookup succeeds; failed txids are simply absent
// from Results. If every lookup fails, the first such error is returned.
func (s *Services) GetStatusForTxIDs(ctx context.Context, txIDs []string) (*wdk.GetStatusForTxIDsResult, error) {
	if len(txIDs) == 0 {
		return nil, fmt.Errorf("services: no txIDs provided")
	}

	tip := s.currentTipHeightOrZero(ctx)

	type outcome struct {
		detail wdk.TxStatusDetail
		err    error
	}

	outcomes := make([]outcome, len(txIDs))
	sem := make(chan struct{}, getStatusConcurrency)
	var wg sync.WaitGroup

	for i, txid := range txIDs {
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, txid string) {
			defer wg.Done()
			defer func() { <-sem }()

			rec, err := s.oracle.GetTx(ctx, txid)
			switch {
			case err == nil:
				outcomes[i] = outcome{detail: toStatusDetail(txid, rec, tip)}
			case errors.Is(err, arcade.ErrTxNotFound):
				outcomes[i] = outcome{detail: wdk.TxStatusDetail{
					TxID:   txid,
					Status: wdk.ResultStatusForTxIDNotFound.String(),
				}}
			default:
				outcomes[i] = outcome{err: err}
			}
		}(i, txid)
	}
	wg.Wait()

	var results []wdk.TxStatusDetail
	var firstErr error
	for _, o := range outcomes {
		if o.err != nil {
			if firstErr == nil {
				firstErr = o.err
			}
			continue
		}
		results = append(results, o.detail)
	}

	if len(results) == 0 {
		if firstErr != nil {
			return nil, fmt.Errorf("services: get status for txIDs: %w", firstErr)
		}
		return nil, fmt.Errorf("services: no results found for provided txIDs")
	}

	return &wdk.GetStatusForTxIDsResult{
		Name:    defs.ArcadeServiceName,
		Status:  wdk.GetStatusSuccess,
		Results: results,
	}, nil
}

// currentTipHeightOrZero resolves the chain tip for depth calculation, or 0
// (logged, not returned as an error) when the header source fails.
func (s *Services) currentTipHeightOrZero(ctx context.Context) uint32 {
	tip, err := s.hdrs.CurrentHeight(ctx)
	if err != nil {
		s.logger.WarnContext(ctx, "services: failed to resolve chain tip for status depth; using minimum depth for mined txs",
			"error", err)
		return 0
	}
	return tip
}

// toStatusDetail maps one Arcade [arcade.TxRecord] to the shared status
// contract, computing confirmation depth for mined transactions from tip.
func toStatusDetail(txid string, rec *arcade.TxRecord, tip uint32) wdk.TxStatusDetail {
	switch rec.Status {
	case arcade.StatusMined, arcade.StatusImmutable:
		depth := 1
		if tip > 0 && rec.BlockHeight > 0 && uint64(tip) >= rec.BlockHeight {
			// G115: safe: block heights are far below the int range on any
			// platform this runs on (64-bit int), and the guard above ensures
			// the subtraction cannot be negative.
			depth = int(uint64(tip)-rec.BlockHeight) + 1 //nolint:gosec
		}
		return wdk.TxStatusDetail{
			TxID:   txid,
			Depth:  &depth,
			Status: wdk.ResultStatusForTxIDMined.String(),
		}
	case arcade.StatusRejected, arcade.StatusUnknown:
		return wdk.TxStatusDetail{
			TxID:   txid,
			Status: wdk.ResultStatusForTxIDNotFound.String(),
		}
	default:
		// Received, SentToNetwork, AcceptedByNetwork, SeenOnNetwork,
		// SeenMultipleNodes, DoubleSpendAttempted, PendingRetry,
		// StumpProcessing, and any future/unknown status (the enum is
		// deliberately open — see arcade.Status) are all "seen, unconfirmed".
		depth := 0
		return wdk.TxStatusDetail{
			TxID:   txid,
			Depth:  &depth,
			Status: wdk.ResultStatusForTxIDKnown.String(),
		}
	}
}
