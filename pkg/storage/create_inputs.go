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
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// resolvedInput is a caller-provided input with its resolved source data.
type resolvedInput struct {
	input         wdk.ValidCreateActionInput
	satoshis      int64
	lockingScript []byte
	known         bool // the outpoint is also a known local output of this user
	// op is the parsed outpoint, meaningful only when known. It is the key the
	// utxostore holds a coin under, and only a wallet-owned coin has a row
	// there to hold — so this is set on exactly the inputs this provider has
	// the authority (and the duty) to reserve.
	op utxostore.Outpoint
}

// resolveProvidedInputs resolves each caller-provided input's satoshis and
// source locking script from the local metastore (preferred) or the provided
// input BEEF, and returns the total provided-input satoshis for the target math.
func (p *Provider) resolveProvidedInputs(ctx context.Context, userID int, inputs []wdk.ValidCreateActionInput, providedBeef *transaction.Beef) ([]resolvedInput, int64, error) {
	out := make([]resolvedInput, 0, len(inputs))
	var total int64
	for i := range inputs {
		in := inputs[i]
		txid := in.Outpoint.TxID
		vout := in.Outpoint.Vout

		ri := resolvedInput{input: in}

		// Prefer a local known output row.
		rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
			UserID: &userID,
			TxID:   &txid,
			Vout:   &vout,
		})
		if err != nil {
			return nil, 0, fmt.Errorf("storage: resolve input %d: %w", i, err)
		}
		if len(rows) > 0 {
			ri.satoshis = rows[0].Satoshis
			ri.lockingScript = rows[0].LockingScript
			ri.known = true
			h, herr := chainhash.NewHashFromHex(txid)
			if herr != nil {
				// The row was found by this very txid string, so a parse failure
				// here means the metastore holds something that is not a txid.
				// Fail loudly rather than silently dropping the coin from the set
				// that gets reserved.
				return nil, 0, fmt.Errorf("storage: resolve input %d: parse txid %q: %w", i, txid, herr)
			}
			ri.op = utxostore.Outpoint{TxID: *h, Vout: vout}
		} else if providedBeef != nil {
			srcTx := providedBeef.FindTransaction(txid)
			if srcTx != nil && int(vout) < len(srcTx.Outputs) {
				o := srcTx.Outputs[vout]
				ri.satoshis = int64(o.Satoshis) //nolint:gosec // satoshi value fits int64
				if o.LockingScript != nil {
					ri.lockingScript = o.LockingScript.Bytes()
				}
			} else {
				return nil, 0, fmt.Errorf("storage: input %d (%s:%d) not found locally or in provided BEEF", i, txid, vout)
			}
		} else {
			return nil, 0, fmt.Errorf("storage: input %d (%s:%d) not found locally and no input BEEF provided", i, txid, vout)
		}

		total += ri.satoshis
		out = append(out, ri)
	}
	return out, total, nil
}

// walletOwnedOutpoints returns the outpoints of the caller-provided inputs this
// wallet owns — the ones with a row in the utxostore, and therefore the only
// ones this provider can (or may) hold exclusively.
//
// EXTERNAL inputs are deliberately absent. They have no inventory row, so
// ReserveOutpoints would refuse them with a [utxostore.NotFoundError] and fail
// the whole all-or-nothing batch; and refusing them would be wrong anyway,
// because this wallet has no exclusivity authority over a coin it does not own.
// Their fate is decided at the accept path instead, which tolerates NotFound on
// exactly this basis.
func walletOwnedOutpoints(provided []resolvedInput) []utxostore.Outpoint {
	ops := make([]utxostore.Outpoint, 0, len(provided))
	for i := range provided {
		if provided[i].known {
			ops = append(ops, provided[i].op)
		}
	}
	return ops
}

// reserveProvidedInputs holds the caller's own named coins under the action's
// reference, so a second CreateAction — and the funder of this very one —
// cannot also take them (audit P0-1).
//
// It is a no-op for an empty set: [utxostore.Store.ReserveOutpoints] rejects a
// zero-op batch as a programmer error ("you asked for an all-or-nothing hold
// and named nothing to hold"), and the overwhelmingly common action names no
// inputs at all.
func (p *Provider) reserveProvidedInputs(ctx context.Context, userID int, reference string, ops []utxostore.Outpoint) error {
	if len(ops) == 0 {
		return nil
	}
	if err := p.utxo.ReserveOutpoints(ctx, int64(userID), reference, ops); err != nil {
		return mapProvidedInputError(err)
	}
	return nil
}

// mapProvidedInputError turns a ReserveOutpoints refusal into an error the
// wallet caller can act on, WITHOUT losing the typed cause: both wraps use %w,
// so errors.Is finds the sentinel and errors.As still reaches the
// [utxostore.SpentError] / [utxostore.ReservedError] / [utxostore.FrozenError]
// / [utxostore.NotFoundError] underneath (see [wdk.ErrInputUnavailable] for why
// a distinct sentinel and not one of the funding ones).
//
// [utxostore.ErrContention] is not a refusal at all — an optimistic backend
// saying "ask me again" — so it maps to the retryable contention sentinel
// instead. Nothing was reserved in that case either.
func mapProvidedInputError(err error) error {
	if errors.Is(err, utxostore.ErrContention) {
		return fmt.Errorf("storage: reserve provided inputs: %w: %w", wdk.ErrUTXOContention, err)
	}
	op, ok := refusedOutpoint(err)
	if !ok {
		// Not a per-item refusal: a closed store, a dead connection, a validation
		// bug. Nothing about it is the caller's input, so it must not be dressed
		// up as an unavailable coin.
		return fmt.Errorf("storage: reserve provided inputs: %w", err)
	}
	return fmt.Errorf("storage: provided input %s is unavailable: %w: %w", op, wdk.ErrInputUnavailable, err)
}

// refusedOutpoint names one coin from a ReserveOutpoints batch error, for the
// message. The search order is the store's own refusal precedence (spent >
// reserved > frozen > not found), so when a caller named several coins and
// several were refused, the message leads with the most final reason rather
// than with whichever happened to be listed first.
func refusedOutpoint(err error) (utxostore.Outpoint, bool) {
	var (
		spent    *utxostore.SpentError
		reserved *utxostore.ReservedError
		frozen   *utxostore.FrozenError
		missing  *utxostore.NotFoundError
	)
	switch {
	case errors.As(err, &spent):
		return spent.Op, true
	case errors.As(err, &reserved):
		return reserved.Op, true
	case errors.As(err, &frozen):
		return frozen.Op, true
	case errors.As(err, &missing):
		return missing.Op, true
	}
	return utxostore.Outpoint{}, false
}

// buildResultInputs assembles the signing template's inputs: caller-provided
// inputs first (in order), then storage-allocated inputs, each with source data
// drawn from the metastore and/or the assembled input BEEF.
func (p *Provider) buildResultInputs(ctx context.Context, userID int, provided []resolvedInput, allocated []*utxostore.UTXO, beef *transaction.Beef, includeSourceTx bool) ([]*wdk.StorageCreateTransactionSdkInput, error) {
	inputs := make([]*wdk.StorageCreateTransactionSdkInput, 0, len(provided)+len(allocated))
	vin := 0

	for i := range provided {
		ri := &provided[i]
		var unlockLen *primitives.PositiveInteger
		if l, err := ri.input.ScriptLength(); err == nil {
			pl := primitives.PositiveInteger(l)
			unlockLen = &pl
		}
		providedBy := wdk.ProvidedByYou
		if ri.known {
			providedBy = wdk.ProvidedByYouAndStorage
		}
		in := &wdk.StorageCreateTransactionSdkInput{
			Vin:                   vin,
			SourceTxID:            ri.input.Outpoint.TxID,
			SourceVout:            ri.input.Outpoint.Vout,
			SourceSatoshis:        ri.satoshis,
			SourceLockingScript:   hexString(ri.lockingScript),
			UnlockingScriptLength: unlockLen,
			ProvidedBy:            providedBy,
			Type:                  wdk.OutputTypeCustom,
		}
		if includeSourceTx {
			in.SourceTransaction = sourceTxBytesFor(beef, ri.input.Outpoint.TxID)
		}
		inputs = append(inputs, in)
		vin++
	}

	// One lookup for every storage-allocated input, rather than a keyed query
	// per coin: a funded action can carry dozens, and this runs on the create
	// path of every transaction.
	allocatedOps := make([]wdk.OutPoint, 0, len(allocated))
	for _, u := range allocated {
		allocatedOps = append(allocatedOps, wdk.OutPoint{TxID: u.TxID.String(), Vout: u.Vout})
	}
	localRows, err := p.localOutputsByOutpoint(ctx, userID, allocatedOps)
	if err != nil {
		return nil, err
	}

	for i, u := range allocated {
		txid := u.TxID.String()
		// Absent stays absent: a coin with no local output row falls back to
		// the BEEF for its locking script and carries no derivation material,
		// exactly as the per-coin lookup's nil row did.
		row := localRows[allocatedOps[i]]

		lockingScript := lockingScriptFor(row, beef, txid, u.Vout)
		unlockLen := primitives.PositiveInteger(txDefaultUnlockLen)
		in := &wdk.StorageCreateTransactionSdkInput{
			Vin:                   vin,
			SourceTxID:            txid,
			SourceVout:            u.Vout,
			SourceSatoshis:        int64(u.Satoshis), //nolint:gosec // satoshi fits int64
			SourceLockingScript:   hexString(lockingScript),
			UnlockingScriptLength: &unlockLen,
			ProvidedBy:            wdk.ProvidedByStorage,
			Type:                  wdk.OutputTypeP2PKH,
		}
		if row != nil {
			in.DerivationPrefix = row.DerivationPrefix
			in.DerivationSuffix = row.DerivationSuffix
			in.SenderIdentityKey = row.SenderIdentityKey
		}
		if includeSourceTx {
			in.SourceTransaction = sourceTxBytesFor(beef, txid)
		}
		inputs = append(inputs, in)
		vin++
	}
	return inputs, nil
}

// sourceTxBytesFor returns the serialized source transaction for txid from the
// ancestry BEEF, or nil when absent. The wallet needs each input's source
// transaction to build the atomic BEEF of a signable (two-step SignAction)
// transaction; this populates it when the caller sets IncludeAllSourceTransactions.
func sourceTxBytesFor(beef *transaction.Beef, txid string) primitives.ExplicitByteArray {
	if beef == nil {
		return nil
	}
	if src := beef.FindTransaction(txid); src != nil {
		return src.Bytes()
	}
	return nil
}

const txDefaultUnlockLen = 107 // P2PKH unlocking script estimate

// localOutputsByOutpoint resolves the metastore output rows for ops under
// userID in one batched query, keyed by outpoint. Outpoints with no local row
// are simply absent from the map, so a caller reads a miss as a nil row.
//
// When two rows share an outpoint the LOWEST output_id wins. That case is
// reachable — outputs is unique on (transaction_id, vout), but transactions.txid
// is NOT unique, so two transaction rows can carry the same txid — and picking
// the first preserves what the per-outpoint FindOutputs lookup this replaces
// returned (it took rows[0] of an output_id-ordered result).
//
// Keys round-trip exactly: callers build ops from chainhash.Hash.String(), the
// metastore stores the hex decode of that string, and OutputRow.TxID re-encodes
// it with the same lowercase hex alphabet.
func (p *Provider) localOutputsByOutpoint(ctx context.Context, userID int, ops []wdk.OutPoint) (map[wdk.OutPoint]*metastore.OutputRow, error) {
	if len(ops) == 0 {
		return nil, nil
	}
	rows, err := p.meta.Outputs().FindOutputsByOutpoints(ctx, userID, ops)
	if err != nil {
		return nil, fmt.Errorf("storage: find local outputs: %w", err)
	}
	byOp := make(map[wdk.OutPoint]*metastore.OutputRow, len(rows))
	for i := range rows {
		if rows[i].TxID == nil {
			continue
		}
		op := wdk.OutPoint{TxID: *rows[i].TxID, Vout: rows[i].Vout}
		if _, dup := byOp[op]; dup {
			continue
		}
		byOp[op] = &rows[i]
	}
	return byOp, nil
}

// lockingScriptFor resolves a source locking script: the metastore row's stored
// script if present, else the source tx's output script from the BEEF.
func lockingScriptFor(row *metastore.OutputRow, beef *transaction.Beef, txid string, vout uint32) []byte {
	if row != nil && len(row.LockingScript) > 0 {
		return row.LockingScript
	}
	if beef != nil {
		if srcTx := beef.FindTransaction(txid); srcTx != nil && int(vout) < len(srcTx.Outputs) {
			if ls := srcTx.Outputs[vout].LockingScript; ls != nil {
				return ls.Bytes()
			}
		}
	}
	return nil
}

func hexString(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	return fmt.Sprintf("%x", b)
}
