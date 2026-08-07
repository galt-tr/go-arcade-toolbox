package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// syncReferenceLength is the length of a generated sync-state reference token.
const syncReferenceLength = 12

// FindOrInsertSyncStateAuth returns the sync state for (user, storage),
// creating one (with a fresh reference and empty sync map) when absent.
func (p *Provider) FindOrInsertSyncStateAuth(ctx context.Context, auth wdk.AuthID, storageIdentityKey, storageName string) (*wdk.FindOrInsertSyncStateAuthResponse, error) {
	p.trace(ctx, "FindOrInsertSyncStateAuth")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}

	existing, found, err := p.meta.SyncState().FindByUserAndStorage(ctx, userID, storageIdentityKey)
	if err != nil {
		return nil, fmt.Errorf("storage: find sync state: %w", err)
	}
	if found {
		return &wdk.FindOrInsertSyncStateAuthResponse{SyncState: existing, IsNew: false}, nil
	}

	ref, err := p.rand.Base64(syncReferenceLength)
	if err != nil {
		return nil, fmt.Errorf("storage: generate sync ref: %w", err)
	}
	ss := wdk.TableSyncState{
		UserID:             userID,
		StorageIdentityKey: storageIdentityKey,
		StorageName:        storageName,
		Status:             wdk.SyncStatusUnknown,
		RefNum:             ref,
		SyncMap:            "{}",
	}
	id, err := p.meta.SyncState().Insert(ctx, ss)
	if err != nil {
		return nil, fmt.Errorf("storage: insert sync state: %w", err)
	}
	ss.SyncStateID = id
	return &wdk.FindOrInsertSyncStateAuthResponse{SyncState: &ss, IsNew: true}, nil
}

// GetSyncChunk returns a chunk of the user's metadata to synchronize.
//
// Simplification: this is a correct-but-minimal round-trip of the essential
// entities (user, output baskets, transactions, outputs). It does NOT sync
// labels/tags/certificates/commissions, and applies MaxItems/Since coarsely
// (an in-memory filter, no per-entity offsets or byte-size bounding). It is
// sufficient to move balances between storages and rebuild inventory.
func (p *Provider) GetSyncChunk(ctx context.Context, args wdk.RequestSyncChunkArgs) (*wdk.SyncChunk, error) {
	p.trace(ctx, "GetSyncChunk")
	if p.storageIdentityKey != "" && args.FromStorageIdentityKey != "" && args.FromStorageIdentityKey != p.storageIdentityKey {
		return nil, fmt.Errorf("storage: sync from-key %q is not this storage %q", args.FromStorageIdentityKey, p.storageIdentityKey)
	}

	user, found, err := p.meta.Users().FindByIdentityKey(ctx, args.IdentityKey)
	if err != nil {
		return nil, fmt.Errorf("storage: sync find user: %w", err)
	}
	if !found {
		return nil, fmt.Errorf("storage: sync unknown user %q", args.IdentityKey)
	}

	chunk := wdk.NewSyncChunk(args.FromStorageIdentityKey, args.ToStorageIdentityKey, args.IdentityKey)
	if args.Since == nil || user.UpdatedAt.After(*args.Since) {
		chunk.User = user
	}

	baskets, err := p.meta.Baskets().Find(ctx, wdk.FindOutputBasketsArgs{UserID: &user.UserID})
	if err != nil {
		return nil, fmt.Errorf("storage: sync baskets: %w", err)
	}
	// Carry basket identity through the wire: the metastore has no numeric
	// basket key, so assign a synthetic per-chunk BasketID and index the outputs
	// against it (TableOutput has no basket-name field of its own).
	nameToID := make(map[string]int, len(baskets))
	for i := range baskets {
		b := baskets[i]
		b.BasketID = i + 1
		nameToID[string(b.Name)] = b.BasketID
		chunk.OutputBaskets = append(chunk.OutputBaskets, &b)
	}

	txRows, _, err := p.meta.Transactions().ListActions(ctx, metastore.ListActionsFilter{UserID: user.UserID})
	if err != nil {
		return nil, fmt.Errorf("storage: sync transactions: %w", err)
	}
	for i := range txRows {
		if !sinceMatch(args.Since, txRows[i].UpdatedAt) {
			continue
		}
		t := txRows[i]
		chunk.Transactions = append(chunk.Transactions, &t)
	}

	outRows, _, err := p.meta.Outputs().ListOutputs(ctx, metastore.ListOutputsFilter{UserID: user.UserID})
	if err != nil {
		return nil, fmt.Errorf("storage: sync outputs: %w", err)
	}
	for i := range outRows {
		if !sinceMatch(args.Since, outRows[i].UpdatedAt) {
			continue
		}
		o := outRows[i].TableOutput
		if outRows[i].Basket != nil {
			if id, ok := nameToID[*outRows[i].Basket]; ok {
				bid := id
				o.BasketID = &bid
			}
		}
		chunk.Outputs = append(chunk.Outputs, &o)
	}

	return chunk, nil
}

// ProcessSyncChunk applies a received chunk: it upserts baskets, transactions
// and outputs for the user and rebuilds spendable inventory in the utxostore.
//
// Simplification: the reader→writer id translation is reference-based (a chunk's
// outputs are linked to its transactions by the transaction Reference, not a
// persisted sync_map), and inventory is rebuilt by minting each change output
// that carries a txid into its basket at TierMined. Entities other than
// baskets/transactions/outputs are ignored. An empty transactions+outputs chunk
// signals Done.
func (p *Provider) ProcessSyncChunk(ctx context.Context, args wdk.RequestSyncChunkArgs, chunk *wdk.SyncChunk) (*wdk.ProcessSyncChunkResult, error) {
	p.trace(ctx, "ProcessSyncChunk")
	if chunk == nil {
		return &wdk.ProcessSyncChunkResult{Done: true}, nil
	}

	user, _, err := p.meta.Users().FindOrInsertUser(ctx, args.IdentityKey)
	if err != nil {
		return nil, fmt.Errorf("storage: sync find/insert user: %w", err)
	}

	result := &wdk.ProcessSyncChunkResult{}
	var maxUpdated time.Time

	// Reverse the synthetic BasketID index so synced outputs keep their basket.
	idToName := make(map[int]string, len(chunk.OutputBaskets))
	for _, b := range chunk.OutputBaskets {
		if b != nil {
			idToName[b.BasketID] = string(b.Name)
		}
	}

	err = p.meta.Do(ctx, func(ctx context.Context) error {
		for _, b := range chunk.OutputBaskets {
			if b == nil {
				continue
			}
			if err := p.meta.Baskets().Upsert(ctx, user.UserID, b.BasketConfiguration, true); err != nil {
				return fmt.Errorf("storage: sync upsert basket: %w", err)
			}
		}

		// Insert transactions, mapping old id -> new id via the unique reference.
		oldToNew := make(map[uint]uint, len(chunk.Transactions))
		for _, t := range chunk.Transactions {
			if t == nil {
				continue
			}
			trackMax(&maxUpdated, t.UpdatedAt)
			_, found, ferr := p.meta.Transactions().FindByReference(ctx, user.UserID, string(t.Reference))
			if ferr != nil {
				return ferr
			}
			if found {
				result.Updates++
				continue
			}
			id, ierr := p.meta.Transactions().Insert(ctx, metastore.NewTx{
				UserID:      user.UserID,
				Status:      t.Status,
				Reference:   string(t.Reference),
				IsOutgoing:  t.IsOutgoing,
				Satoshis:    t.Satoshis,
				Description: t.Description,
				Version:     t.Version,
				LockTime:    t.LockTime,
				InputBEEF:   t.InputBEEF,
			})
			if ierr != nil {
				return fmt.Errorf("storage: sync insert transaction: %w", ierr)
			}
			if t.TxID != nil {
				if err := p.meta.Transactions().SetTxID(ctx, id, *t.TxID); err != nil {
					return fmt.Errorf("storage: sync set txid: %w", err)
				}
			}
			oldToNew[t.TransactionID] = id
			result.Inserts++
		}

		// Insert outputs and rebuild inventory.
		for _, o := range chunk.Outputs {
			if o == nil {
				continue
			}
			trackMax(&maxUpdated, o.UpdatedAt)
			newTxID, ok := oldToNew[o.TransactionID]
			if !ok {
				continue // parent tx not in this chunk; skip (flagged simplification)
			}
			basket := p.changeBasketName()
			if o.BasketID != nil {
				if name, ok := idToName[*o.BasketID]; ok && name != "" {
					basket = name
				}
			}
			if err := p.applySyncedOutput(ctx, user.UserID, newTxID, basket, o); err != nil {
				return err
			}
			result.Inserts++
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: process sync chunk: %w", err)
	}

	if !maxUpdated.IsZero() {
		result.MaxUpdatedAt = &maxUpdated
	}
	result.Done = len(chunk.Transactions) == 0 && len(chunk.Outputs) == 0
	return result, nil
}

// applySyncedOutput inserts one synced output row into the given basket and
// mints its inventory when it is a change coin carrying a txid.
func (p *Provider) applySyncedOutput(ctx context.Context, userID int, transactionID uint, basket string, o *wdk.TableOutput) error {
	if basket == "" {
		basket = p.changeBasketName()
	}
	if err := p.meta.Baskets().FindOrCreate(ctx, userID, basket); err != nil {
		return fmt.Errorf("storage: sync ensure basket %q: %w", basket, err)
	}
	newOut := metastore.NewOutput{
		UserID:            userID,
		TransactionID:     transactionID,
		Vout:              o.Vout,
		Satoshis:          o.Satoshis,
		LockingScript:     o.LockingScript,
		Basket:            &basket,
		Change:            o.Change,
		Type:              o.Type,
		ProvidedBy:        o.ProvidedBy,
		Purpose:           o.Purpose,
		Description:       o.OutputDescription,
		DerivationPrefix:  o.DerivationPrefix,
		DerivationSuffix:  o.DerivationSuffix,
		SenderIdentityKey: o.SenderIdentityKey,
	}
	if _, err := p.meta.Outputs().Insert(ctx, newOut); err != nil {
		return fmt.Errorf("storage: sync insert output: %w", err)
	}
	if !o.Change || o.TxID == nil {
		return nil
	}
	hash, err := chainhash.NewHashFromHex(*o.TxID)
	if err != nil {
		return nil //nolint:nilerr // unparseable txid: skip inventory rebuild
	}
	mint := &utxostore.Mint{
		Outpoint:  utxostore.Outpoint{TxID: *hash, Vout: o.Vout},
		UserID:    int64(userID),
		Basket:    basket,
		Satoshis:  uint64(o.Satoshis), //nolint:gosec // non-negative
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      utxostore.TierMined,
	}
	if err := p.utxo.Mint(ctx, []*utxostore.Mint{mint}); err != nil {
		return fmt.Errorf("storage: sync mint: %w", err)
	}
	return mint.Err
}

func sinceMatch(since *time.Time, updatedAt time.Time) bool {
	if since == nil {
		return true
	}
	return !updatedAt.Before(*since)
}

func trackMax(maxT *time.Time, t time.Time) {
	if t.After(*maxT) {
		*maxT = t
	}
}
