package storage

import (
	"context"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// defaultKnownTxListLimit bounds a status drill-down when the caller passes a
// non-positive or oversized limit.
const defaultKnownTxListLimit = 200

// KnownTxRow is one transaction in a status drill-down.
type KnownTxRow struct {
	TxID         string  `json:"txid"`
	Status       string  `json:"status"`
	ArcadeStatus string  `json:"arcade_status"`
	BlockHeight  *uint32 `json:"block_height,omitempty"`
	// RejectReason is arcade's own explanation of a refusal, captured when it was
	// first learned. It is what makes a REJECTED bucket in this drill-down
	// actionable: arcade discards a rejected transaction's record on its own
	// schedule, so by the time an operator clicks through, GET /tx may well
	// answer 404. Empty when the transaction was never rejected — or when arcade
	// rejected it and said nothing, which is a finding in itself.
	RejectReason  string `json:"reject_reason,omitempty"`
	UpdatedAtUnix int64  `json:"updated_at_unix"`
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
		reason := ""
		if kt.RejectReason != nil {
			reason = *kt.RejectReason
		}
		out = append(out, KnownTxRow{
			TxID:          kt.TxID,
			Status:        string(kt.Status),
			ArcadeStatus:  arcade,
			BlockHeight:   kt.BlockHeight,
			RejectReason:  reason,
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
	// Degraded names the components that could not be read, with the reason.
	// The report is best-effort: a component that fails is OMITTED (never
	// zero-filled, which would read as real data) and the rest are still
	// collected. Empty on a fully successful report.
	//
	// This exists because the components have wildly different costs. Balance
	// walks a secondary index — measured at ~300k entries per bval on a live
	// fuel basket of ~900k coins — while the status rollups are indexed counts
	// in the low hundreds of milliseconds. Failing the whole report on the
	// expensive one meant a caller polling this for a dashboard silently froze
	// every cheap number too, for as long as the load lasted.
	Degraded []DegradedComponent `json:"degraded,omitempty"`
}

// DegradedComponent identifies one part of a StateReport that could not be
// read, so a caller can surface it rather than mistaking stale or absent data
// for a quiet system.
type DegradedComponent struct {
	// Component is "balance", "tx_statuses" or "arcade_statuses".
	Component string `json:"component"`
	// Basket is set only for "balance".
	Basket string `json:"basket,omitempty"`
	Error  string `json:"error"`
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
//
// It is BEST-EFFORT and degrades per component. A component that cannot be read
// is omitted and named in [StateReport.Degraded]; the others are still
// collected. Only a failure that makes the whole report meaningless — resolving
// the user — returns an error.
//
// This matters because the components differ in cost by orders of magnitude.
// [utxostore.Store.Balance] walks a secondary index (~300k entries per bval
// against a ~900k-coin fuel basket on the live system) while the status rollups
// are indexed counts. When one slow basket failed the whole call, a dashboard
// polling this froze ENTIRELY for the duration of a load run — every cheap
// number included — and, because callers typically carry the previous snapshot
// forward on error, did so silently. Reported statuses appeared to stop arriving
// the moment a blast started and to resume the moment it ended, when in fact
// they had been applying continuously the whole time. Callers should surface
// Degraded rather than treat a partial report as a healthy one.
func (p *Provider) StateReport(ctx context.Context, auth wdk.AuthID, baskets []string) (*StateReport, error) {
	userID, err := p.reportUserID(ctx, auth)
	if err != nil {
		return nil, err
	}

	rep := &StateReport{}
	for _, basket := range baskets {
		bal, err := p.utxo.Balance(ctx, int64(userID), basket)
		if err != nil {
			// Omit, never zero-fill: a basket reported as empty reads as real
			// data and is worse than an acknowledged gap (an empty fuel basket
			// is indistinguishable from "we ran out of fuel").
			rep.Degraded = append(rep.Degraded, DegradedComponent{
				Component: "balance", Basket: basket, Error: err.Error(),
			})
			continue
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
		rep.TxStatuses = nil
		rep.Degraded = append(rep.Degraded, DegradedComponent{Component: "tx_statuses", Error: err.Error()})
	}
	if rep.ArcadeStatuses, err = p.meta.KnownTx().CountByArcadeStatus(ctx); err != nil {
		rep.ArcadeStatuses = nil
		rep.Degraded = append(rep.Degraded, DegradedComponent{Component: "arcade_statuses", Error: err.Error()})
	}
	return rep, nil
}

// maxRawTxBatch bounds one RawTxs query. It is a placeholder-count limit, not a
// tuning knob: PostgreSQL caps a statement at 65535 bind parameters and SQLite
// at 32766 (SQLITE_MAX_VARIABLE_NUMBER since 3.32), so a caller with a large
// txid list must be chunked rather than allowed to fail at the driver.
const maxRawTxBatch = 512

// RawTxs returns the retained raw transaction bytes for each requested txid.
//
// Read-only, and deliberately bulk: it exists for callers that hold a set of
// txids and need the transactions themselves back — reconstructing a spend of
// their outputs, or verifying that some recorded txid really is the transaction
// it claims to be. Going through the provider rather than the caller's own SQL
// keeps it backend-agnostic, which matters when the caller's own records live in
// a different database from the wallet.
//
// A txid with no known_txs row, or a row whose raw_tx was never retained, is
// simply ABSENT from the result — never an error and never a nil entry. The
// caller must decide what a miss means; this cannot know.
//
// raw_tx is retained for the life of the row: [metastore.KnownTxRepo.SetProof]
// clears input_beef once a proof anchors the transaction but deliberately keeps
// raw_tx, "needed to reconstruct the tx when spending its outputs". So a mined
// transaction from long ago is still expected to be here.
func (p *Provider) RawTxs(ctx context.Context, txids []string) (map[string][]byte, error) {
	p.trace(ctx, "RawTxs")
	out := make(map[string][]byte, len(txids))
	for start := 0; start < len(txids); start += maxRawTxBatch {
		batch := txids[start:min(start+maxRawTxBatch, len(txids))]
		rows, err := p.meta.KnownTx().FindByTxIDs(ctx, batch)
		if err != nil {
			return nil, fmt.Errorf("storage: raw txs: %w", err)
		}
		for i := range rows {
			if len(rows[i].RawTx) == 0 {
				continue
			}
			out[rows[i].TxID] = rows[i].RawTx
		}
	}
	return out, nil
}
