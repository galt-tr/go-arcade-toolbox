package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// ListOutputs returns descriptive output rows from the metastore with their
// spendability composed from the utxostore live set (the spendability seam).
func (p *Provider) ListOutputs(ctx context.Context, auth wdk.AuthID, args wdk.ListOutputsArgs) (*wdk.ListOutputsResult, error) {
	p.trace(ctx, "ListOutputs")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}

	filter := metastore.ListOutputsFilter{
		UserID:       userID,
		Basket:       string(args.Basket),
		Tags:         stringsFrom(args.Tags),
		TagQueryMode: args.TagQueryMode,
		Statuses:     metastore.DefaultListOutputsStatuses,
		Limit:        int(args.Limit),
		Offset:       int(args.Offset), //nolint:gosec // pagination offset within int range
		IncludeTags:  args.IncludeTags,
	}
	rows, total, err := p.meta.Outputs().ListOutputs(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("storage: list outputs: %w", err)
	}

	result := &wdk.ListOutputsResult{
		TotalOutputs: primitives.PositiveInteger(total), //nolint:gosec // count non-negative
		Outputs:      make([]*wdk.WalletOutput, 0, len(rows)),
	}
	var txids []string
	for i := range rows {
		row := &rows[i]
		spendable, err := p.outpointSpendable(ctx, row.TxID, row.Vout)
		if err != nil {
			return nil, err
		}
		wo := &wdk.WalletOutput{
			Satoshis:  primitives.SatoshiValue(row.Satoshis), //nolint:gosec // satoshi non-negative
			Spendable: spendable,
			Outpoint:  outpointString(row.TxID, row.Vout),
		}
		if args.IncludeCustomInstructions {
			wo.CustomInstructions = row.CustomInstructions
		}
		if args.IncludeLockingScripts && len(row.LockingScript) > 0 {
			ls := primitives.HexString(fmt.Sprintf("%x", row.LockingScript))
			wo.LockingScript = &ls
		}
		if args.IncludeTags {
			wo.Tags = stringsUnder300(row.Tags)
		}
		result.Outputs = append(result.Outputs, wo)
		if row.TxID != nil {
			txids = append(txids, *row.TxID)
		}
	}

	if args.IncludeTransactions && len(txids) > 0 {
		beef, err := p.getBEEFForTxIDs(ctx, dedupe(txids), nil, args.KnownTxids)
		if err != nil {
			return nil, err
		}
		b, err := beef.Bytes()
		if err != nil {
			return nil, fmt.Errorf("storage: serialize outputs beef: %w", err)
		}
		result.BEEF = primitives.ExplicitByteArray(b)
	}
	return result, nil
}

// GetBalance returns the total spendable (claimable) satoshis in the basket for
// the authenticated user. Empty basket defaults to the change basket.
func (p *Provider) GetBalance(ctx context.Context, auth wdk.AuthID, basket string) (uint64, error) {
	p.trace(ctx, "GetBalance")
	userID, err := p.userID(auth)
	if err != nil {
		return 0, err
	}
	if basket == "" {
		basket = p.changeBasketName()
	}
	bal, err := p.utxo.Balance(ctx, int64(userID), basket)
	if err != nil {
		return 0, fmt.Errorf("storage: balance: %w", err)
	}
	var total uint64
	for _, v := range bal.Claimable {
		total += v
	}
	return total, nil
}

// GetClaimableCount returns the number of claimable (unreserved, unspent,
// unfrozen) coins in the basket across the spendable tiers, for observability
// and fuel-keeper sizing. Empty basket defaults to the change basket.
func (p *Provider) GetClaimableCount(ctx context.Context, auth wdk.AuthID, basket string) (int, error) {
	p.trace(ctx, "GetClaimableCount")
	userID, err := p.userID(auth)
	if err != nil {
		return 0, err
	}
	if basket == "" {
		basket = p.changeBasketName()
	}
	bal, err := p.utxo.Balance(ctx, int64(userID), basket)
	if err != nil {
		return 0, fmt.Errorf("storage: claimable count: %w", err)
	}
	var total int
	for _, v := range bal.ClaimableCount {
		total += v
	}
	return total, nil
}

// FindOutputBasketsAuth returns the user's output baskets matching the filters.
func (p *Provider) FindOutputBasketsAuth(ctx context.Context, auth wdk.AuthID, filters wdk.FindOutputBasketsArgs) (wdk.TableOutputBaskets, error) {
	p.trace(ctx, "FindOutputBasketsAuth")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}
	if filters.UserID != nil && *filters.UserID != userID {
		return nil, ErrAuthorization
	}
	filters.UserID = &userID
	baskets, err := p.meta.Baskets().Find(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("storage: find baskets: %w", err)
	}
	return baskets, nil
}

// FindOutputsAuth returns the user's outputs matching the filters, with
// spendability composed from the utxostore. The Spendable filter (which the
// metastore ignores) is applied here after the intersection.
func (p *Provider) FindOutputsAuth(ctx context.Context, auth wdk.AuthID, filters wdk.FindOutputsArgs) (wdk.TableOutputs, error) {
	p.trace(ctx, "FindOutputsAuth")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}
	if filters.UserID != nil && *filters.UserID != userID {
		return nil, ErrAuthorization
	}
	filters.UserID = &userID
	rows, err := p.meta.Outputs().FindOutputs(ctx, filters)
	if err != nil {
		return nil, fmt.Errorf("storage: find outputs: %w", err)
	}
	out := make(wdk.TableOutputs, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		spendable, err := p.outpointSpendable(ctx, row.TxID, row.Vout)
		if err != nil {
			return nil, err
		}
		if filters.Spendable != nil && *filters.Spendable != spendable {
			continue
		}
		to := row.TableOutput
		to.Spendable = spendable
		out = append(out, to)
	}
	return out, nil
}

// RelinquishOutput removes an output from its basket and from the utxostore
// inventory (so it leaves the spendable set).
func (p *Provider) RelinquishOutput(ctx context.Context, auth wdk.AuthID, args wdk.RelinquishOutputArgs) error {
	p.trace(ctx, "RelinquishOutput")
	userID, err := p.userID(auth)
	if err != nil {
		return err
	}
	txid, vout, err := primitives.OutpointString(args.Output).Get()
	if err != nil {
		return fmt.Errorf("storage: parse outpoint: %w", err)
	}

	return p.meta.Do(ctx, func(ctx context.Context) error {
		if err := p.meta.Outputs().RelinquishOutput(ctx, userID, args.Basket, txid, vout); err != nil {
			return fmt.Errorf("storage: relinquish output: %w", err)
		}
		hash, err := chainhash.NewHashFromHex(txid)
		if err != nil {
			return fmt.Errorf("storage: parse txid: %w", err)
		}
		op := utxostore.Outpoint{TxID: *hash, Vout: vout}
		if err := p.utxo.Remove(ctx, []utxostore.Outpoint{op}, false); err != nil {
			// Tolerate reserved/spent/not-found refusals: the coin is not
			// spendable anyway; the metastore basket removal is the source of truth.
			if !errors.Is(err, utxostore.ErrBatch) {
				return fmt.Errorf("storage: remove inventory: %w", err)
			}
		}
		return nil
	})
}

// outpointSpendable reports whether (txid, vout) is a live, claimable coin in
// the utxostore (unreserved, unspent, not frozen). A nil txid (never-signed) is
// not spendable.
func (p *Provider) outpointSpendable(ctx context.Context, txid *string, vout uint32) (bool, error) {
	if txid == nil {
		return false, nil
	}
	hash, err := chainhash.NewHashFromHex(*txid)
	if err != nil {
		return false, nil //nolint:nilerr // an unparseable txid is simply not a live coin
	}
	u, err := p.utxo.Get(ctx, utxostore.Outpoint{TxID: *hash, Vout: vout})
	if err != nil {
		if errors.Is(err, &utxostore.NotFoundError{}) {
			return false, nil
		}
		return false, fmt.Errorf("storage: get inventory: %w", err)
	}
	return u.ReservedBy == "" && u.SpentBy == nil && !u.Frozen, nil
}

func outpointString(txid *string, vout uint32) primitives.OutpointString {
	if txid == nil {
		return ""
	}
	return primitives.NewOutpointString(*txid, vout)
}

func dedupe(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}
