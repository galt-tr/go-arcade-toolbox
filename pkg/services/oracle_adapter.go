package services

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// ErrStreamingNotSupported is returned by the [arcade.TxOracle] returned from
// [OracleFromServices] on every call to StreamStatus. A [wdk.Services] has no
// push/event surface (that is an Arcade-specific SSE feature the wdk.Services
// interface never had a slot for), so there is nothing to adapt: the returned
// oracle fails fast instead of silently behaving as if it were streaming.
// Callers must fall back to polling GetTx.
var ErrStreamingNotSupported = errors.New("services: OracleFromServices does not support StreamStatus (wdk.Services has no event/push surface); poll GetTx instead")

// oracleAdapter adapts a [wdk.Services] into an [arcade.TxOracle]. See
// [OracleFromServices].
type oracleAdapter struct {
	svc wdk.Services
}

// compile-time proof that oracleAdapter satisfies arcade.TxOracle.
var _ arcade.TxOracle = (*oracleAdapter)(nil)

// OracleFromServices adapts svc — a caller-supplied [wdk.Services] — into an
// [arcade.TxOracle], so it can be handed to anything that wants the lean
// oracle contract (e.g. pkg/storage's constructor) instead of the wide
// wdk.Services interface.
//
// This is a DEGRADED bridge, necessarily: wdk.Services was never designed to
// expose Arcade's exact lifecycle or its push/event stream. Concretely:
//
//   - GetTx's status resolution is coarse. It is derived from
//     svc.GetStatusForTxIDs, which only distinguishes mined / known / not-found
//     (see [wdk.ResultStatusForTxIDMined] and friends) — nowhere near Arcade's
//     full RECEIVED -> ... -> MINED -> IMMUTABLE lattice. "mined" maps to
//     [arcade.StatusMined]; "known" maps to [arcade.StatusSeenOnNetwork] (a
//     reasonable non-terminal proxy for "seen, unconfirmed"); anything else
//     (including a lookup miss) is [arcade.ErrTxNotFound]. GetTx additionally
//     makes best-effort follow-up calls to svc.MerklePath and svc.RawTx to
//     populate the proof/raw-tx fields when the underlying Services has them;
//     a failure on either of those follow-ups is tolerated (the fields are
//     just left empty), since the status itself is the primary answer.
//   - Broadcast wraps ef in a single-transaction [transaction.Beef] (via
//     [transaction.Beef.MergeTransaction]) and calls svc.PostFromBEEF. Success
//     always reports [arcade.StatusReceived] regardless of what the
//     underlying Services actually observed — there is no way to recover a
//     precise Arcade-style early status from the wdk.Services result shape.
//     A rejection (an error on the per-txid result) maps to Rejected=true.
//     There is no backpressure signal in wdk.Services at all, so
//     [arcade.BackpressureError] is never produced here: any transport-level
//     failure surfaces as a plain, non-retry-classified error.
//   - StreamStatus always fails with [ErrStreamingNotSupported] — see its doc.
//   - Health is a heuristic: it treats a successful svc.CurrentHeight call as
//     "healthy" and reports that height as [arcade.Health.BlockHeight], even
//     though that is the HEADER service's height, not Arcade's own processed
//     height (wdk.Services has no direct equivalent of Arcade's /health).
func OracleFromServices(svc wdk.Services) arcade.TxOracle {
	return &oracleAdapter{svc: svc}
}

// Broadcast adapts to svc.PostFromBEEF. See [OracleFromServices] for the exact
// (degraded) mapping.
func (a *oracleAdapter) Broadcast(ctx context.Context, txid string, ef []byte) (*arcade.BroadcastResult, error) {
	tx, err := transaction.NewTransactionFromBytes(ef)
	if err != nil {
		return nil, fmt.Errorf("services: oracle-from-services broadcast: parse ef for %s: %w", txid, err)
	}
	if actual := tx.TxID().String(); actual != txid {
		return nil, fmt.Errorf("services: oracle-from-services broadcast: ef txid %s does not match requested %s", actual, txid)
	}

	beef := transaction.NewBeefV2()
	if _, err := beef.MergeTransaction(tx); err != nil {
		return nil, fmt.Errorf("services: oracle-from-services broadcast: merge tx %s into beef: %w", txid, err)
	}

	results, err := a.svc.PostFromBEEF(ctx, beef, []string{txid})
	if err != nil {
		return nil, fmt.Errorf("services: oracle-from-services broadcast %s: %w", txid, err)
	}

	for _, r := range results {
		if r == nil {
			continue
		}
		if r.Error != nil {
			return nil, fmt.Errorf("services: oracle-from-services broadcast %s: %w", txid, r.Error)
		}
		if r.PostedBEEFResult == nil {
			continue
		}
		for _, posted := range r.PostedBEEFResult.TxIDResults {
			if posted.TxID != txid {
				continue
			}
			if posted.Error != nil || posted.Result == wdk.PostedTxIDResultError {
				return &arcade.BroadcastResult{
					TxID:      txid,
					Status:    arcade.StatusRejected,
					Rejected:  true,
					ExtraInfo: rejectionReason(posted),
				}, nil
			}
			// Success or AlreadyKnown: report the generic early, non-final
			// status — see the degraded-mapping note on OracleFromServices.
			return &arcade.BroadcastResult{TxID: txid, Status: arcade.StatusReceived}, nil
		}
	}

	return nil, fmt.Errorf("services: oracle-from-services broadcast %s: underlying Services returned no result for this txid", txid)
}

// rejectionReason renders a PostedTxID that failed into a human-readable
// reason for BroadcastResult.ExtraInfo.
func rejectionReason(posted wdk.PostedTxID) string {
	if posted.Error != nil {
		return posted.Error.Error()
	}
	return fmt.Sprintf("broadcast result: %s", posted.Result)
}

// GetTx adapts to svc.GetStatusForTxIDs (status) plus best-effort svc.MerklePath
// and svc.RawTx follow-ups (proof / raw bytes). See [OracleFromServices] for
// the exact (degraded) mapping.
func (a *oracleAdapter) GetTx(ctx context.Context, txid string) (*arcade.TxRecord, error) {
	statusRes, err := a.svc.GetStatusForTxIDs(ctx, []string{txid})
	if err != nil {
		if errors.Is(err, wdk.ErrNotFoundError) {
			return nil, fmt.Errorf("services: oracle-from-services get tx %s: %w", txid, arcade.ErrTxNotFound)
		}
		return nil, fmt.Errorf("services: oracle-from-services get tx %s: %w", txid, err)
	}

	var detail *wdk.TxStatusDetail
	if statusRes != nil {
		for i := range statusRes.Results {
			if statusRes.Results[i].TxID == txid {
				detail = &statusRes.Results[i]
				break
			}
		}
	}
	if detail == nil {
		return nil, fmt.Errorf("services: oracle-from-services get tx %s: no status returned: %w", txid, arcade.ErrTxNotFound)
	}

	rec := &arcade.TxRecord{TxID: txid}
	switch detail.Status {
	case wdk.ResultStatusForTxIDMined.String():
		rec.Status = arcade.StatusMined
	case wdk.ResultStatusForTxIDKnown.String():
		rec.Status = arcade.StatusSeenOnNetwork
	default:
		return nil, fmt.Errorf("services: oracle-from-services get tx %s: %w", txid, arcade.ErrTxNotFound)
	}

	// Best-effort enrichment. Neither failure is fatal: GetTx's primary job is
	// to report status, and callers that need the proof/raw bytes have their
	// own error handling for an empty result.
	if mp, mpErr := a.svc.MerklePath(ctx, txid); mpErr == nil && mp != nil && mp.MerklePath != nil {
		rec.MerklePath = mp.MerklePath.Bytes()
		if mp.BlockHeader != nil {
			rec.BlockHash = mp.BlockHeader.Hash
			rec.BlockHeight = uint64(mp.BlockHeader.Height)
		}
	}
	if raw, rawErr := a.svc.RawTx(ctx, txid); rawErr == nil {
		rec.RawTx = raw.RawTx
	}

	return rec, nil
}

// StreamStatus always returns [ErrStreamingNotSupported]. See its doc and the
// [OracleFromServices] degraded-mapping note.
func (a *oracleAdapter) StreamStatus(_ context.Context, _ string, _ func(arcade.StatusEvent) error) error {
	return ErrStreamingNotSupported
}

// Health treats a successful svc.CurrentHeight call as "healthy" — a
// heuristic; see the [OracleFromServices] degraded-mapping note.
func (a *oracleAdapter) Health(ctx context.Context) (*arcade.Health, error) {
	height, err := a.svc.CurrentHeight(ctx)
	if err != nil {
		return &arcade.Health{Healthy: false}, fmt.Errorf("services: oracle-from-services health: current height: %w", err)
	}
	return &arcade.Health{Healthy: true, BlockHeight: uint64(height)}, nil
}
