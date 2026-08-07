package storage

import (
	"context"
	"fmt"

	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk/primitives"
)

// resolvedInput is a caller-provided input with its resolved source data.
type resolvedInput struct {
	input         wdk.ValidCreateActionInput
	satoshis      int64
	lockingScript []byte
	known         bool // the outpoint is also a known local output of this user
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

// buildResultInputs assembles the signing template's inputs: caller-provided
// inputs first (in order), then storage-allocated inputs, each with source data
// drawn from the metastore and/or the assembled input BEEF.
func (p *Provider) buildResultInputs(ctx context.Context, userID int, provided []resolvedInput, allocated []*utxostore.UTXO, beef *transaction.Beef) ([]*wdk.StorageCreateTransactionSdkInput, error) {
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
		inputs = append(inputs, &wdk.StorageCreateTransactionSdkInput{
			Vin:                   vin,
			SourceTxID:            ri.input.Outpoint.TxID,
			SourceVout:            ri.input.Outpoint.Vout,
			SourceSatoshis:        ri.satoshis,
			SourceLockingScript:   hexString(ri.lockingScript),
			UnlockingScriptLength: unlockLen,
			ProvidedBy:            providedBy,
			Type:                  wdk.OutputTypeCustom,
		})
		vin++
	}

	for _, u := range allocated {
		txid := u.TxID.String()
		row, err := p.findLocalOutput(ctx, userID, txid, u.Vout)
		if err != nil {
			return nil, err
		}

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
		inputs = append(inputs, in)
		vin++
	}
	return inputs, nil
}

const txDefaultUnlockLen = 107 // P2PKH unlocking script estimate

// findLocalOutput returns the metastore output row for (txid, vout) under
// userID, or nil when absent.
func (p *Provider) findLocalOutput(ctx context.Context, userID int, txid string, vout uint32) (*metastore.OutputRow, error) {
	rows, err := p.meta.Outputs().FindOutputs(ctx, wdk.FindOutputsArgs{
		UserID: &userID,
		TxID:   &txid,
		Vout:   &vout,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: find local output %s:%d: %w", txid, vout, err)
	}
	if len(rows) == 0 {
		return nil, nil
	}
	return &rows[0], nil
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
