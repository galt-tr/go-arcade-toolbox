package storage

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
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
//
// Each change output carries this storage's live spend state — Spendable
// (intersected with the local utxostore) and SpentBy — because that is what
// lets the receiver decide which coins it may rebuild as claimable.
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
		// Ship the source's live spend state with each change coin. The
		// metastore has no spendable column (spendability is the utxostore's),
		// so intersect with this storage's own inventory: a coin that is spent,
		// reserved (mid-flight in a create/broadcast here), pinned (a signed tx
		// already spends it — pinned implies ReservedBy != "", so the
		// reservation check already covers it), frozen or simply absent is NOT
		// spendable, and the receiver must not resurrect it as claimable.
		// o.SpentBy (the source-local spending transaction id) already travels
		// with the row via outputCols. Non-change outputs keep the historical
		// always-false, since the receiver mints only change coins.
		//
		// The intersection uses outpointSpendable — the SAME helper ListOutputs
		// and FindOutputs use — so the wire's notion of "spendable" is the
		// storage's own metastore/utxostore seam by construction: there is no
		// second definition of spendability here that could drift from the one
		// the read path serves.
		//
		// Per-output Get is acceptable on this cold path; a batch Get would be
		// the obvious future optimization.
		if outRows[i].Change && o.TxID != nil {
			spendable, serr := p.outpointSpendable(ctx, o.TxID, o.Vout)
			if serr != nil {
				return nil, fmt.Errorf("sync spendability %s:%d: %w", *o.TxID, o.Vout, serr)
			}
			o.Spendable = spendable
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
// the source reports as still LIVE (unspent and spendable there) into its
// basket at TierMined — see applySyncedOutput. Entities other than
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

	// Inventory-rebuild counters. They live OUTSIDE the Do closure so the
	// summary can be logged after the transaction commits (a rolled-back
	// attempt must not be reported as applied), and are reset at the top of
	// each attempt so a retry cannot double-count.
	var changeCandidates, skippedNoParentTx, mintedCount int

	// Reverse the synthetic BasketID index so synced outputs keep their basket.
	idToName := make(map[int]string, len(chunk.OutputBaskets))
	for _, b := range chunk.OutputBaskets {
		if b != nil {
			idToName[b.BasketID] = string(b.Name)
		}
	}

	err = p.meta.Do(ctx, func(ctx context.Context) error {
		// Do retries the whole closure on a transient failure, and a retried
		// attempt re-walks the same chunk — so every counter this closure owns
		// must be attempt-scoped, or a single retry doubles the reported work.
		//
		// maxUpdated is deliberately NOT reset: trackMax keeps a running
		// maximum, which is idempotent under a re-walk of the same rows. Do not
		// "fix" it — zeroing it here would be harmless but pointless, and
		// zeroing it anywhere the walk can be partial would lose the watermark.
		changeCandidates, skippedNoParentTx, mintedCount = 0, 0, 0
		result.Inserts, result.Updates = 0, 0

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
			if o.Change && o.TxID != nil {
				changeCandidates++
			}
			newTxID, ok := oldToNew[o.TransactionID]
			if !ok {
				// No NEW parent transaction id for this output. oldToNew is
				// populated only for transactions this chunk INSERTED, so this
				// covers both "the parent tx is not in this chunk" and "the
				// parent tx was already present on this storage and was
				// skipped as an update" — in the latter case the output is
				// dropped even though its parent exists locally. Flagged
				// simplification: it makes re-syncing into a non-empty target
				// a partial operation.
				skippedNoParentTx++
				continue
			}
			basket := p.changeBasketName()
			if o.BasketID != nil {
				if name, ok := idToName[*o.BasketID]; ok && name != "" {
					basket = name
				}
			}
			// Translate the source-local spending transaction id into this
			// storage's id space. An untranslatable id (the spending tx is not
			// in this chunk) drops the history to NULL — best effort — but it
			// does NOT make the coin mintable: applySyncedOutput decides that
			// from the chunk's own SpentBy, not from the translated value.
			var spentBy *uint
			if o.SpentBy != nil {
				if id, ok := oldToNew[*o.SpentBy]; ok {
					spentBy = &id
				}
			}
			minted, aerr := p.applySyncedOutput(ctx, user.UserID, newTxID, basket, spentBy, o)
			if aerr != nil {
				return aerr
			}
			if minted {
				mintedCount++
			}
			result.Inserts++
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: process sync chunk: %w", err)
	}

	// Committed: report what the rebuild actually did. A chunk full of change
	// coins that minted nothing is the visible signature of the two silent
	// failure modes — a source that predates spendability sync (Spendable is
	// false for everything) and a target that already holds the transactions
	// (every output skipped for want of a freshly inserted parent).
	if changeCandidates > 0 && mintedCount == 0 {
		p.logger.WarnContext(ctx, "sync: chunk carried change outputs but rebuilt no spendable inventory; "+
			"source predates spendability sync, has no live coins, or this target already holds these transactions",
			slog.Int("changeCandidates", changeCandidates),
			slog.Int("minted", mintedCount),
			slog.Int("skippedNoParentTx", skippedNoParentTx),
			slog.Int("inserts", result.Inserts),
			slog.Int("updates", result.Updates))
	} else {
		p.logger.DebugContext(ctx, "sync: chunk applied",
			slog.Int("changeCandidates", changeCandidates),
			slog.Int("minted", mintedCount),
			slog.Int("skippedNoParentTx", skippedNoParentTx))
	}

	if !maxUpdated.IsZero() {
		result.MaxUpdatedAt = &maxUpdated
	}
	result.Done = len(chunk.Transactions) == 0 && len(chunk.Outputs) == 0
	return result, nil
}

// applySyncedOutput inserts one synced output row into the given basket and
// mints its inventory only when the source says the coin is still live. It
// reports whether it minted, so the caller can tell an empty rebuild from a
// full one. spentBy, when non-nil, is the ALREADY-TRANSLATED spending
// transaction id in this storage's id space.
func (p *Provider) applySyncedOutput(ctx context.Context, userID int, transactionID uint, basket string, spentBy *uint, o *wdk.TableOutput) (bool, error) {
	if basket == "" {
		basket = p.changeBasketName()
	}
	if err := p.meta.Baskets().FindOrCreate(ctx, userID, basket); err != nil {
		return false, fmt.Errorf("storage: sync ensure basket %q: %w", basket, err)
	}
	newOut := metastore.NewOutput{
		UserID:            userID,
		TransactionID:     transactionID,
		Vout:              o.Vout,
		Satoshis:          o.Satoshis,
		LockingScript:     o.LockingScript,
		Basket:            &basket,
		SpentBy:           spentBy,
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
		return false, fmt.Errorf("storage: sync insert output: %w", err)
	}
	// Mint gate. A coin is rebuilt as claimable inventory only when the SOURCE
	// still holds it as a live change coin: spent-at-source (o.SpentBy != nil)
	// or non-spendable-at-source (reserved/pinned/frozen/spent/absent) coins
	// stay descriptive history only. Resurrecting either would hand this
	// storage an input the source has already committed elsewhere — a
	// wallet-inflicted double spend (audit P1-2).
	//
	// Minting-then-marking is deliberately NOT done: the utxostore has no
	// token-less spend primitive, so a spent coin would have to transit through
	// a claimable state, and a concurrent funding run could grab it in that
	// window. Not minting at all is the only state that is safe at every
	// instant.
	//
	// Note the wire implication: a chunk produced by a storage older than this
	// change ships Spendable=false for everything, so the receiver rebuilds NO
	// inventory. That direction is the right one for a double-spend guard, but
	// it is not self-healing: re-running the sync against the SAME target after
	// upgrading the source does not repair it, because the target already holds
	// those transactions and ProcessSyncChunk skips every output whose parent
	// tx is not freshly inserted (see the skippedNoParentTx branch above). The
	// remedy is a resync into a FRESH target. ProcessSyncChunk warns when a
	// chunk carried change candidates and minted none, which is the signature.
	if !o.Change || o.TxID == nil || !o.Spendable || o.SpentBy != nil {
		return false, nil
	}
	hash, err := chainhash.NewHashFromHex(*o.TxID)
	if err != nil {
		return false, nil //nolint:nilerr // unparseable txid: skip inventory rebuild
	}
	// LIMITATION: the sync wire carries no tier — wdk.TableOutput has no field
	// for it — so a coin that is TierSending or TierUnproven at the source is
	// rebuilt here as TierMined. The target therefore treats an in-flight or
	// unproven coin as fully settled. This is pre-existing (every synced coin
	// was already minted at TierMined) and is NOT addressed here; carrying Tier
	// on the wire is the follow-up.
	mint := &utxostore.Mint{
		Outpoint:  utxostore.Outpoint{TxID: *hash, Vout: o.Vout},
		UserID:    int64(userID),
		Basket:    basket,
		Satoshis:  uint64(o.Satoshis), //nolint:gosec // non-negative
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      utxostore.TierMined,
	}
	if err := p.utxo.Mint(ctx, []*utxostore.Mint{mint}); err != nil {
		return false, fmt.Errorf("storage: sync mint: %w", err)
	}
	if mint.Err != nil {
		return false, mint.Err
	}
	return true, nil
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
