package storage

import (
	"context"
	"errors"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// internalizedOutput is the provider's plan for one output being internalized.
type internalizedOutput struct {
	vout               uint32
	satoshis           int64
	lockingScript      []byte
	basket             string
	change             bool
	outputType         string
	providedBy         string
	purpose            string
	derivationPrefix   *string
	derivationSuffix   *string
	senderIdentityKey  *string
	customInstructions *string
	tags               []string
}

// InternalizeAction brings foreign transaction outputs into the wallet. It is
// the ONLY path by which externally-created outputs become wallet inventory. It
// parses the provided AtomicBEEF, verifies every BUMP merkle root against the
// headers source (the trust anchor — a bad proof is rejected), retains the
// transaction (and its ancestry) in known_txs, and credits the requested
// outputs into their baskets.
func (p *Provider) InternalizeAction(ctx context.Context, auth wdk.AuthID, args wdk.InternalizeActionArgs) (*wdk.InternalizeActionResult, error) {
	p.trace(ctx, "InternalizeAction")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}

	beef, txidHash, err := transaction.NewBeefFromAtomicBytes(args.Tx)
	if err != nil {
		return nil, fmt.Errorf("storage: parse atomic beef: %w", err)
	}

	// Trust anchor: verify BUMP merkle roots against the header source.
	ok, err := p.beef.VerifyBeef(ctx, beef, false)
	if err != nil {
		return nil, fmt.Errorf("storage: verify beef: %w", err)
	}
	if !ok {
		return nil, fmt.Errorf("storage: beef verification failed (bad merkle proof)")
	}

	tx := beef.FindAtomicTransactionByHash(txidHash)
	if tx == nil {
		tx = beef.FindTransactionByHash(txidHash)
	}
	if tx == nil {
		return nil, fmt.Errorf("storage: atomic transaction %s not found in beef", txidHash.String())
	}
	txid := txidHash.String()

	mp := beef.FindBumpByHash(txidHash)
	mined := mp != nil

	// Build the output plans and cumulative credit.
	plans, cumulative, err := p.planInternalizedOutputs(tx, args.Outputs)
	if err != nil {
		return nil, err
	}

	// Determine known-tx + transaction statuses and the inventory tier.
	ktxStatus := wdk.ProvenTxStatusUnconfirmed
	txStatus := wdk.TxStatusUnproven
	tier := utxostore.TierUnproven
	if mined {
		ktxStatus = wdk.ProvenTxStatusCompleted
		txStatus = wdk.TxStatusCompleted
		tier = utxostore.TierMined
	}

	reference, err := p.rand.Base64(referenceLength)
	if err != nil {
		return nil, fmt.Errorf("storage: generate reference: %w", err)
	}

	// Detect a merge (the tx already exists for this user).
	existing, err := p.meta.Transactions().FindByTxID(ctx, userID, txid)
	if err != nil {
		return nil, fmt.Errorf("storage: find existing transaction: %w", err)
	}
	isMerge := len(existing) > 0

	var blockHeight *uint32
	var blockHash, merkleRoot, merklePath []byte
	if mined {
		merklePath = mp.Bytes()
		h := mp.BlockHeight
		blockHeight = &h
		if root, rerr := mp.ComputeRoot(txidHash); rerr == nil {
			merkleRoot = root[:]
		}
		if hdr, herr := p.hdrs.HeaderByHeight(ctx, mp.BlockHeight); herr == nil && hdr != nil {
			blockHash = hdr.Hash[:]
		}
	}

	err = p.meta.Do(ctx, func(ctx context.Context) error {
		// Retain the transaction + ancestry.
		if err := p.meta.KnownTx().Upsert(ctx, metastore.KnownTx{
			TxID:         txid,
			Status:       ktxStatus,
			WasBroadcast: mined,
			RawTx:        tx.Bytes(),
			InputBEEF:    args.Tx,
			BlockHeight:  blockHeight,
			BlockHash:    blockHash,
			MerklePath:   merklePath,
			MerkleRoot:   merkleRoot,
		}); err != nil && !errors.Is(err, metastore.ErrStatusUpdateSkipped) {
			return fmt.Errorf("storage: upsert known tx: %w", err)
		}

		var transactionID uint
		if isMerge {
			transactionID = existing[0].TransactionID
			if len(args.Labels) > 0 {
				if err := p.meta.Labels().Attach(ctx, userID, transactionID, stringsFrom(args.Labels)...); err != nil {
					return fmt.Errorf("storage: attach labels: %w", err)
				}
			}
		} else {
			id, err := p.meta.Transactions().Insert(ctx, metastore.NewTx{
				UserID:      userID,
				Status:      txStatus,
				Reference:   reference,
				IsOutgoing:  false,
				Satoshis:    cumulative,
				Description: string(args.Description),
				InputBEEF:   args.Tx,
				Labels:      stringsFrom(args.Labels),
			})
			if err != nil {
				return fmt.Errorf("storage: insert transaction: %w", err)
			}
			transactionID = id
			if err := p.meta.Transactions().SetTxID(ctx, transactionID, txid); err != nil {
				return fmt.Errorf("storage: set txid: %w", err)
			}
		}

		// Persist output rows + mint inventory.
		for i := range plans {
			if err := p.creditInternalizedOutput(ctx, userID, transactionID, txid, tier, &plans[i]); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("storage: internalize: %w", err)
	}

	return &wdk.InternalizeActionResult{
		Accepted: true,
		IsMerge:  isMerge,
		TxID:     txid,
		Satoshis: cumulative,
	}, nil
}

// planInternalizedOutputs maps the internalize args to output plans, resolving
// each against the transaction's actual outputs.
func (p *Provider) planInternalizedOutputs(tx *transaction.Transaction, outputs []*wdk.InternalizeOutput) ([]internalizedOutput, int64, error) {
	plans := make([]internalizedOutput, 0, len(outputs))
	var cumulative int64
	for _, o := range outputs {
		if o == nil {
			continue
		}
		if err := o.Validate(); err != nil {
			return nil, 0, fmt.Errorf("storage: invalid internalize output: %w", err)
		}
		if int(o.OutputIndex) >= len(tx.Outputs) {
			return nil, 0, fmt.Errorf("storage: output index %d out of range", o.OutputIndex)
		}
		txo := tx.Outputs[o.OutputIndex]
		var ls []byte
		if txo.LockingScript != nil {
			ls = txo.LockingScript.Bytes()
		}
		sats := int64(txo.Satoshis) //nolint:gosec // satoshi value fits int64

		plan := internalizedOutput{
			vout:          o.OutputIndex,
			satoshis:      sats,
			lockingScript: ls,
		}
		switch o.Protocol {
		case wdk.WalletPaymentProtocol:
			// The wallet-payment output is our own change-style receipt; storage
			// records the BRC-29 remittance material and the on-chain script but
			// does not re-derive the address (it holds no keys).
			pr := o.PaymentRemittance
			prefix := string(pr.DerivationPrefix)
			suffix := string(pr.DerivationSuffix)
			sender := string(pr.SenderIdentityKey)
			plan.basket = p.changeBasketName()
			plan.change = true
			plan.outputType = string(wdk.OutputTypeP2PKH)
			plan.providedBy = string(wdk.ProvidedByStorage)
			plan.purpose = wdk.ChangePurpose
			plan.derivationPrefix = &prefix
			plan.derivationSuffix = &suffix
			plan.senderIdentityKey = &sender
			cumulative += sats
		case wdk.BasketInsertionProtocol:
			ir := o.InsertionRemittance
			plan.basket = string(ir.Basket)
			plan.change = false
			plan.outputType = string(wdk.OutputTypeCustom)
			plan.providedBy = string(wdk.ProvidedByYou)
			plan.customInstructions = ir.CustomInstructions
			plan.tags = stringsFrom(ir.Tags)
		default:
			return nil, 0, fmt.Errorf("storage: unsupported internalize protocol %q", o.Protocol)
		}
		plans = append(plans, plan)
	}
	return plans, cumulative, nil
}

// creditInternalizedOutput persists one output row and mints its inventory.
func (p *Provider) creditInternalizedOutput(ctx context.Context, userID int, transactionID uint, txid string, tier utxostore.Tier, plan *internalizedOutput) error {
	if err := p.meta.Baskets().FindOrCreate(ctx, userID, plan.basket); err != nil {
		return fmt.Errorf("storage: ensure basket %q: %w", plan.basket, err)
	}
	basket := plan.basket
	if _, err := p.meta.Outputs().Insert(ctx, metastore.NewOutput{
		UserID:             userID,
		TransactionID:      transactionID,
		Vout:               plan.vout,
		Satoshis:           plan.satoshis,
		LockingScript:      plan.lockingScript,
		Basket:             &basket,
		Change:             plan.change,
		Type:               plan.outputType,
		ProvidedBy:         plan.providedBy,
		Purpose:            plan.purpose,
		DerivationPrefix:   plan.derivationPrefix,
		DerivationSuffix:   plan.derivationSuffix,
		SenderIdentityKey:  plan.senderIdentityKey,
		CustomInstructions: plan.customInstructions,
		Tags:               plan.tags,
	}); err != nil {
		return fmt.Errorf("storage: insert internalized output: %w", err)
	}

	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("storage: parse txid: %w", err)
	}
	mint := &utxostore.Mint{
		Outpoint:  utxostore.Outpoint{TxID: *hash, Vout: plan.vout},
		UserID:    int64(userID),
		Basket:    plan.basket,
		Satoshis:  uint64(plan.satoshis), //nolint:gosec // satoshi value non-negative
		InputSize: utxostore.DefaultP2PKHInputSize,
		Tier:      tier,
	}
	if err := p.utxo.Mint(ctx, []*utxostore.Mint{mint}); err != nil {
		return fmt.Errorf("storage: mint internalized output: %w", err)
	}
	if mint.Err != nil {
		return fmt.Errorf("storage: mint internalized output %s: %w", mint.Outpoint, mint.Err)
	}
	return nil
}
