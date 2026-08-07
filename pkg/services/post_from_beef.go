package services

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// PostFromBEEF broadcasts each of txids (found within beef) to Arcade via
// [arcade.TxOracle.Broadcast]. There is a single broadcast target — Arcade —
// so, unlike the old go-wallet-toolbox, there is no multi-provider fan-out and
// no failover chain here: retry/backpressure/circuit-breaker behavior already
// lives in the concrete [arcade.Client] this Services wraps.
//
// For each txid:
//   - if it is not found in beef, or has already been merged with a merkle
//     path (i.e. already mined), it is skipped without an error (an
//     already-mined tx has nothing left to broadcast; note that unlike a
//     missing txid, this produces NO entry in the returned result at all —
//     the same choice the old services.PostFromBEEF made).
//   - otherwise its Extended Format bytes are built via
//     [transaction.Beef.FindTransactionForSigning] (which wires each direct
//     input's SourceTransaction so [transaction.Transaction.EF] can succeed)
//     and broadcast.
//
// The returned [wdk.PostFromBeefResult] always uses [defs.ArcadeServiceName]
// as the per-result Name, since Arcade is the only broadcaster.
func (s *Services) PostFromBEEF(ctx context.Context, beef *transaction.Beef, txids []string) (wdk.PostFromBeefResult, error) {
	if beef == nil {
		return nil, fmt.Errorf("services: PostFromBEEF requires a non-nil beef")
	}

	var results wdk.PostFromBeefResult
	for _, txid := range txids {
		tx := beef.FindTransactionForSigning(txid)
		if tx == nil {
			results = append(results, &wdk.PostFromBEEFServiceResult{
				Name:  defs.ArcadeServiceName,
				Error: fmt.Errorf("services: transaction %s not found in beef", txid),
			})
			continue
		}

		if tx.MerklePath != nil {
			continue // already mined; nothing to broadcast.
		}

		ef, err := tx.EF()
		if err != nil {
			results = append(results, &wdk.PostFromBEEFServiceResult{
				Name:  defs.ArcadeServiceName,
				Error: fmt.Errorf("services: build extended format for %s: %w", txid, err),
			})
			continue
		}

		results = append(results, s.broadcastOne(ctx, txid, ef))
	}

	return results, nil
}

// broadcastOne submits one transaction and maps [arcade.BroadcastResult] (or a
// transport/backpressure/circuit-breaker error) onto the wdk PostFromBEEF
// result shape.
//
// A transport failure, [*arcade.BackpressureError] or [arcade.ErrCircuitOpen]
// all become a top-level Error on the [wdk.PostFromBEEFServiceResult] (the
// transaction's fate is unknown / it was never queued — see the [arcade.TxOracle]
// contract). A tx-level rejection (Rejected=true) is FINAL and durable, but it
// is a successful call to Arcade, so it is reported as a nested
// [wdk.PostedTxID] with Result=Error — mirroring the old services' convention
// that the outer Error is reserved for call-level (transport) failures, never
// for an application-level verdict. [wdk.PostFromBeefResult.Aggregated] and
// [wdk.PostFromBeefResult.ServiceErrors] both key off that distinction.
func (s *Services) broadcastOne(ctx context.Context, txid string, ef []byte) *wdk.PostFromBEEFServiceResult {
	res, err := s.oracle.Broadcast(ctx, txid, ef)
	if err != nil {
		return &wdk.PostFromBEEFServiceResult{
			Name:  defs.ArcadeServiceName,
			Error: fmt.Errorf("services: broadcast %s: %w", txid, err),
		}
	}

	if res.Rejected {
		return &wdk.PostFromBEEFServiceResult{
			Name: defs.ArcadeServiceName,
			PostedBEEFResult: &wdk.PostedBEEF{TxIDResults: []wdk.PostedTxID{{
				TxID:   res.TxID,
				Result: wdk.PostedTxIDResultError,
				Error:  fmt.Errorf("services: arcade rejected tx %s: %s", txid, res.ExtraInfo),
			}}},
		}
	}

	// A 202 is an early, non-final status; Status == Mined/Immutable here means
	// this was an idempotent re-submit of a tx Arcade already fully processed.
	alreadyKnown := res.Status == arcade.StatusMined || res.Status == arcade.StatusImmutable
	result := wdk.PostedTxIDResultSuccess
	if alreadyKnown {
		result = wdk.PostedTxIDResultAlreadyKnown
	}

	return &wdk.PostFromBEEFServiceResult{
		Name: defs.ArcadeServiceName,
		PostedBEEFResult: &wdk.PostedBEEF{TxIDResults: []wdk.PostedTxID{{
			TxID:         res.TxID,
			Result:       result,
			AlreadyKnown: alreadyKnown,
		}}},
	}
}
