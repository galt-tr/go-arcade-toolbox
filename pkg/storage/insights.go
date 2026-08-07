package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// defaultKnownTxListLimit bounds a status drill-down when the caller passes a
// non-positive or oversized limit.
const defaultKnownTxListLimit = 200

// KnownTxRow is one transaction in a status drill-down.
type KnownTxRow struct {
	TxID          string  `json:"txid"`
	Status        string  `json:"status"`
	ArcadeStatus  string  `json:"arcade_status"`
	BlockHeight   *uint32 `json:"block_height,omitempty"`
	UpdatedAtUnix int64   `json:"updated_at_unix"`
}

func clampListLimit(limit int) int {
	if limit <= 0 || limit > 1000 {
		return defaultKnownTxListLimit
	}
	return limit
}

func toKnownTxRows(rows []metastore.KnownTx) []KnownTxRow {
	out := make([]KnownTxRow, 0, len(rows))
	for i := range rows {
		kt := &rows[i]
		arcade := ""
		if kt.ArcadeStatus != nil {
			arcade = *kt.ArcadeStatus
		}
		out = append(out, KnownTxRow{
			TxID:          kt.TxID,
			Status:        string(kt.Status),
			ArcadeStatus:  arcade,
			BlockHeight:   kt.BlockHeight,
			UpdatedAtUnix: kt.UpdatedAt.Unix(),
		})
	}
	return out
}

// ListKnownTxByArcadeStatus returns up to limit known transactions whose arcade
// status equals arcadeStatus (empty string = not yet reported), most-recent
// first. Read-only, deployment-wide (known_txs is not user-scoped). Backs the
// dashboard's click-a-status-bucket drill-down.
func (p *Provider) ListKnownTxByArcadeStatus(ctx context.Context, arcadeStatus string, limit int) ([]KnownTxRow, error) {
	rows, err := p.meta.KnownTx().ListByArcadeStatus(ctx, arcadeStatus, clampListLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("storage: list known txs by arcade status: %w", err)
	}
	return toKnownTxRows(rows), nil
}

// ListKnownTxByStatus is the wallet-side-status counterpart of
// [Provider.ListKnownTxByArcadeStatus].
func (p *Provider) ListKnownTxByStatus(ctx context.Context, status string, limit int) ([]KnownTxRow, error) {
	rows, err := p.meta.KnownTx().ListByStatus(ctx, wdk.ProvenTxReqStatus(status), clampListLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("storage: list known txs by status: %w", err)
	}
	return toKnownTxRows(rows), nil
}

// ListKnownTxRecent returns the most-recent known transactions regardless of
// status — the "all recent / in-flight transactions" view (during a blast, the
// stream of fuel-funded txs), each with its txid for an arcade drill-down.
func (p *Provider) ListKnownTxRecent(ctx context.Context, limit int) ([]KnownTxRow, error) {
	rows, err := p.meta.KnownTx().ListRecent(ctx, clampListLimit(limit))
	if err != nil {
		return nil, fmt.Errorf("storage: list recent known txs: %w", err)
	}
	return toKnownTxRows(rows), nil
}

// TierState is the claimable inventory of one settlement tier within a basket.
type TierState struct {
	// Tier is the tier name: "sending" | "unproven" | "mined".
	Tier string `json:"tier"`
	// ClaimableCount is the number of claimable (unreserved, unfrozen, unspent)
	// coins in this tier.
	ClaimableCount int `json:"claimable_count"`
	// ClaimableSats is the satoshi total of those claimable coins.
	ClaimableSats uint64 `json:"claimable_sats"`
}

// BasketState is the per-tier inventory of one basket plus its reserved (a
// claim is holding them mid-funding) coins.
type BasketState struct {
	Basket string `json:"basket"`
	// Tiers is always ordered sending → unproven → mined.
	Tiers         []TierState `json:"tiers"`
	ReservedCount int         `json:"reserved_count"`
	ReservedSats  uint64      `json:"reserved_sats"`
}

// StateReport is a point-in-time snapshot of one user's storage inventory for
// observability: the UTXO tier breakdown per basket (what the funder can spend
// and where coins sit in the sending → unproven → mined lifecycle) and the
// transaction lifecycle as status counts. It is read-only and cheap; a UI can
// poll it to visualize the toolbox's live state.
type StateReport struct {
	Baskets []BasketState `json:"baskets"`
	// TxStatuses maps each transactions.status to its count for this user
	// (unsigned/unprocessed/sending/nosend/unproven/completed/failed/…).
	TxStatuses map[string]int `json:"tx_statuses"`
	// ArcadeStatuses maps each known-tx arcade status to its count
	// (SEEN_ON_NETWORK/SEEN_MULTIPLE_NODES/MINED/REJECTED/…); "" means broadcast
	// not yet reported. known_txs is deployment-wide, not user-scoped.
	ArcadeStatuses map[string]int `json:"arcade_statuses"`
}

// tierOrder is the fixed sending → unproven → mined ordering used in reports.
var tierOrder = []utxostore.Tier{utxostore.TierSending, utxostore.TierUnproven, utxostore.TierMined}

// reportUserID resolves the user for a read-only report. Unlike the write path
// (which requires a pre-resolved numeric UserID), a report caller may pass only
// the identity key; it is looked up here. Never creates a user.
func (p *Provider) reportUserID(ctx context.Context, auth wdk.AuthID) (int, error) {
	if auth.UserID != nil {
		return *auth.UserID, nil
	}
	if auth.IdentityKey == "" {
		return 0, ErrAuthorization
	}
	u, found, err := p.meta.Users().FindByIdentityKey(ctx, auth.IdentityKey)
	if err != nil {
		return 0, fmt.Errorf("storage: resolve user for state report: %w", err)
	}
	if !found {
		return 0, ErrAuthorization
	}
	return u.UserID, nil
}

// StateReport returns a snapshot of the user's UTXO tiers (for each requested
// basket) and transaction-status counts. Baskets that do not exist simply
// report zeros. It is intended for observability/visualization, not for
// funding decisions.
func (p *Provider) StateReport(ctx context.Context, auth wdk.AuthID, baskets []string) (*StateReport, error) {
	userID, err := p.reportUserID(ctx, auth)
	if err != nil {
		return nil, err
	}

	rep := &StateReport{}
	for _, basket := range baskets {
		bal, err := p.utxo.Balance(ctx, int64(userID), basket)
		if err != nil {
			return nil, fmt.Errorf("storage: state report balance for basket %q: %w", basket, err)
		}
		bs := BasketState{
			Basket:        basket,
			ReservedCount: bal.ReservedCount,
			ReservedSats:  bal.Reserved,
		}
		for _, t := range tierOrder {
			bs.Tiers = append(bs.Tiers, TierState{
				Tier:           t.String(),
				ClaimableCount: bal.ClaimableCount[t],
				ClaimableSats:  bal.Claimable[t],
			})
		}
		rep.Baskets = append(rep.Baskets, bs)
	}

	if rep.TxStatuses, err = p.meta.Transactions().CountByStatus(ctx, userID); err != nil {
		return nil, err
	}
	if rep.ArcadeStatuses, err = p.meta.KnownTx().CountByArcadeStatus(ctx); err != nil {
		return nil, err
	}
	return rep, nil
}
