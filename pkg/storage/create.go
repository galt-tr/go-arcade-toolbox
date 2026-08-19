package storage

import (
	"context"
	"errors"
	"fmt"
	"time"

	ec "github.com/bsv-blockchain/go-sdk/primitives/ec"
	"github.com/bsv-blockchain/go-sdk/script"
	"github.com/bsv-blockchain/go-sdk/transaction"
	"github.com/bsv-blockchain/go-sdk/transaction/template/p2pkh"

	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/galt-tr/go-arcade-toolbox/pkg/internal/txutils"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/galt-tr/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

const (
	referenceLength       = 12
	derivationPrefixBytes = 16
	derivationSuffixBytes = 16

	// releaseTimeout bounds the detached whole-token release that compensates a
	// failed Mode B persist: generous for one indexed UPDATE, short enough that
	// a shutdown is never held up waiting on it. Twin of funder.releaseTimeout,
	// which bounds the same compensation on the funding path.
	releaseTimeout = 5 * time.Second
)

// outputPlan is the provider's internal description of one output while
// assembling a create-action, before vouts are assigned.
type outputPlan struct {
	satoshis           int64
	lockingScript      []byte // nil for storage-derived change (wallet derives)
	basket             *string
	change             bool
	outputType         string
	providedBy         string
	purpose            string
	description        string
	customInstructions *string
	derivationPrefix   *string
	derivationSuffix   *string
	tags               []string
	vout               uint32
}

// CreateAction funds and reserves coins for a new transaction, persists the
// unsigned metadata (transaction row, output rows, commission), and returns the
// signing template plus the input BEEF assembled from local ancestry. It makes
// NO external calls and mints NO change inventory (the txid is unknown until the
// wallet signs — see the package doc); change is minted at ProcessAction.
func (p *Provider) CreateAction(ctx context.Context, auth wdk.AuthID, args wdk.ValidCreateActionArgs) (*wdk.StorageCreateActionResult, error) {
	p.trace(ctx, "CreateAction")
	userID, err := p.userID(auth)
	if err != nil {
		return nil, err
	}

	reference, err := p.rand.Base64(referenceLength)
	if err != nil {
		return nil, fmt.Errorf("storage: generate reference: %w", err)
	}
	derivationPrefix, err := p.rand.Base64(derivationPrefixBytes)
	if err != nil {
		return nil, fmt.Errorf("storage: generate derivation prefix: %w", err)
	}

	// --- resolve provided inputs (satoshis + source data from BEEF) ---
	providedBeef, err := p.parseProvidedBEEF(args.InputBEEF)
	if err != nil {
		return nil, err
	}
	providedInputs, providedInputSats, err := p.resolveProvidedInputs(ctx, userID, args.Inputs, providedBeef)
	if err != nil {
		return nil, err
	}

	// --- target + size math ---
	providedOutputSats, outputScriptLens, err := providedOutputSizes(args.Outputs)
	if err != nil {
		return nil, err
	}
	commissionEnabled := p.commission.Enabled()
	commissionScript, err := p.commissionLockingScript()
	if err != nil {
		return nil, err
	}
	nonChangeOutputCount := uint64(len(args.Outputs))
	if commissionEnabled {
		outputScriptLens = append(outputScriptLens, txutils.P2PKHLockingScriptLength)
		nonChangeOutputCount++
	}
	// Fuel fan-out (throughput strategy): the action mints shape.Count
	// self-owned P2PKH outputs of shape.Satoshis each into the destination
	// (pool/reserve) basket. Account for their serialized bytes and count here so
	// the fee and change math cover them; their value is added to the target and
	// the outputs themselves are emitted by buildOutputPlans.
	if shape := args.Options.FuelShape; shape != nil {
		for i := uint64(0); i < shape.Count; i++ {
			outputScriptLens = append(outputScriptLens, txutils.P2PKHLockingScriptLength)
		}
		nonChangeOutputCount += shape.Count
	}
	inputScriptLens, err := providedInputSizes(args.Inputs)
	if err != nil {
		return nil, err
	}
	currentTxSize := txutils.TransactionSizeFromScriptLengths(inputScriptLens, outputScriptLens)

	target := providedOutputSats
	if commissionEnabled {
		target += int64(p.commission.Satoshis) //nolint:gosec // commission satoshis are small
	}
	if shape := args.Options.FuelShape; shape != nil {
		target += int64(shape.Count) * int64(shape.Satoshis) //nolint:gosec // count*denomination fits int64 for realistic fan-outs
	}
	target -= providedInputSats
	targetSat, err := satoshi.From(target)
	if err != nil {
		return nil, fmt.Errorf("storage: invalid funding target: %w", err)
	}

	// existingBasketCount MUST be fetched before opening any DB transaction
	// (SQLite single-connection deadlock avoidance).
	existingCount, err := p.changeBasketCount(ctx, userID)
	if err != nil {
		return nil, err
	}

	// Resolve the funding source. Under the throughput strategy the action funds
	// from the denominated fuel pool via the funder's closed-form ClaimExact fast
	// path (Denomination > 0, Basket = the pool); otherwise it takes the default
	// bounded tiered claim from the change basket (Denomination stays 0). Change
	// computation, tiers, and existingBasketCount are unchanged on both paths.
	fundBasket, denomination, err := p.fundingSource(args.Options.FuelShape)
	if err != nil {
		return nil, err
	}

	fundArgs := funder.FundArgs{
		UserID:                  int64(userID),
		Basket:                  fundBasket,
		Reservation:             reference,
		TargetSat:               targetSat,
		CurrentTxSize:           currentTxSize,
		OutputCount:             nonChangeOutputCount,
		Tiers:                   p.spendTiers(),
		NumberOfDesiredUTXOs:    p.changeBasket.NumberOfDesiredUTXOs,
		MinimumDesiredUTXOValue: p.changeBasket.MinimumDesiredUTXOValue,
		MaxChangeOutputsPerTx:   p.maxChangeOutputsPerTx(),
		ExistingBasketCount:     existingCount,
		Denomination:            denomination,
		RequireChange:           p.requireChangeOutput,
	}

	var result *wdk.StorageCreateActionResult

	persist := func(ctx context.Context, fundRes *funder.Result) error {
		r, perr := p.persistCreateAction(ctx, userID, reference, derivationPrefix, args,
			providedInputs, fundRes, commissionScript)
		if perr != nil {
			return perr
		}
		result = r
		return nil
	}

	if p.modeA {
		// Fund + persist atomically over the shared database.
		err = p.meta.Do(ctx, func(ctx context.Context) error {
			fundRes, ferr := p.funder.Fund(ctx, fundArgs)
			if ferr != nil {
				return ferr
			}
			return persist(ctx, fundRes)
		})
		if err != nil {
			return nil, mapFundError(err)
		}
		return result, nil
	}

	// Mode B: fund against the split utxostore, then persist metadata; on a
	// metadata failure, compensate by releasing the reservation.
	fundRes, err := p.funder.Fund(ctx, fundArgs)
	if err != nil {
		return nil, mapFundError(err)
	}
	if err := p.meta.Do(ctx, func(ctx context.Context) error { return persist(ctx, fundRes) }); err != nil {
		// Compensate on a context DETACHED from the request's cancellation: a
		// canceled request is one of the likeliest reasons the persist just
		// failed, and a canceled context fails the release at BeginTx without
		// touching a row — leaving every coin this action reserved held until
		// the stale-reservation sweep. WithoutCancel keeps context values, so
		// nothing else about the call changes.
		rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), releaseTimeout)
		defer cancel()
		if _, rerr := p.utxo.ReleaseReservation(rctx, int64(userID), reference); rerr != nil {
			p.logger.WarnContext(rctx, "failed to release reservation after create failure",
				"reference", reference, "error", rerr, "requestCtxCanceled", ctx.Err() != nil)
		}
		return nil, fmt.Errorf("storage: persist create action: %w", err)
	}
	return result, nil
}

// persistCreateAction writes the unsigned transaction, its outputs and the
// signing template. It runs inside an ambient transaction (metastore.Do).
func (p *Provider) persistCreateAction(
	ctx context.Context,
	userID int,
	reference, derivationPrefix string,
	args wdk.ValidCreateActionArgs,
	providedInputs []resolvedInput,
	fundRes *funder.Result,
	commissionScript []byte,
) (*wdk.StorageCreateActionResult, error) {
	// Build the ordered output plans.
	plans, err := p.buildOutputPlans(args, fundRes, derivationPrefix, commissionScript)
	if err != nil {
		return nil, err
	}
	if args.Options.RandomizeOutputs {
		p.rand.Shuffle(len(plans), func(i, j int) { plans[i], plans[j] = plans[j], plans[i] })
	}
	for i := range plans {
		plans[i].vout = uint32(i) //nolint:gosec // output index fits uint32
	}

	totalAllocated, err := fundRes.TotalAllocated()
	if err != nil {
		return nil, err
	}
	txNet := int64(fundRes.ChangeAmount) - int64(totalAllocated)

	// Assemble the input BEEF (provided BEEF merged with the local ancestry of
	// the allocated coins) BEFORE inserting the tx, so it lands in one insert.
	base, err := p.parseProvidedBEEF(args.InputBEEF)
	if err != nil {
		return nil, err
	}
	allocTxids := allocatedTxids(fundRes.AllocatedUTXOs)
	beef, err := p.getBEEFForTxIDs(ctx, allocTxids, base, stringsFrom(args.Options.KnownTxids))
	if err != nil {
		return nil, err
	}
	inputBeefBytes, err := beef.Bytes()
	if err != nil {
		return nil, fmt.Errorf("storage: serialize input beef: %w", err)
	}

	labels := stringsFrom(args.Labels)
	var version, lockTime *uint32
	if args.Version != 0 {
		v := args.Version
		version = &v
	}
	if args.LockTime != 0 {
		lt := args.LockTime
		lockTime = &lt
	}

	txID, err := p.meta.Transactions().Insert(ctx, metastore.NewTx{
		UserID:      userID,
		Status:      wdk.TxStatusUnsigned,
		Reference:   reference,
		IsOutgoing:  true,
		Satoshis:    txNet,
		Description: string(args.Description),
		Version:     version,
		LockTime:    lockTime,
		InputBEEF:   inputBeefBytes,
		Labels:      labels,
	})
	if err != nil {
		return nil, fmt.Errorf("storage: insert transaction: %w", err)
	}

	for i := range plans {
		pl := &plans[i]
		if _, err := p.meta.Outputs().Insert(ctx, metastore.NewOutput{
			UserID:             userID,
			TransactionID:      txID,
			Vout:               pl.vout,
			Satoshis:           pl.satoshis,
			LockingScript:      pl.lockingScript,
			Basket:             pl.basket,
			Change:             pl.change,
			Type:               pl.outputType,
			ProvidedBy:         pl.providedBy,
			Purpose:            pl.purpose,
			Description:        pl.description,
			DerivationPrefix:   pl.derivationPrefix,
			DerivationSuffix:   pl.derivationSuffix,
			CustomInstructions: pl.customInstructions,
			Tags:               pl.tags,
		}); err != nil {
			return nil, fmt.Errorf("storage: insert output %d: %w", pl.vout, err)
		}
	}

	sdkInputs, err := p.buildResultInputs(ctx, userID, providedInputs, fundRes.AllocatedUTXOs, beef, args.IncludeAllSourceTransactions)
	if err != nil {
		return nil, err
	}
	sdkOutputs := buildResultOutputs(plans)

	res := &wdk.StorageCreateActionResult{
		InputBeef:               primitives.ExplicitByteArray(inputBeefBytes),
		Inputs:                  sdkInputs,
		Outputs:                 sdkOutputs,
		NoSendChangeOutputVouts: noSendChangeVouts(args.IsNoSend, plans, p.changeDestinationBasket(args.Options.FuelShape)),
		DerivationPrefix:        derivationPrefix,
		Version:                 args.Version,
		LockTime:                args.LockTime,
		Reference:               reference,
	}
	return res, nil
}

// buildOutputPlans assembles provided + commission + change output plans.
func (p *Provider) buildOutputPlans(args wdk.ValidCreateActionArgs, fundRes *funder.Result, derivationPrefix string, commissionScript []byte) ([]outputPlan, error) {
	//nolint:gosec // change-output count is small, within int range
	plans := make([]outputPlan, 0, len(args.Outputs)+int(fundRes.ChangeOutputsCount)+1)

	// Provided outputs (ProvidedByYou).
	for i := range args.Outputs {
		o := &args.Outputs[i]
		ls, err := hexBytes(string(o.LockingScript))
		if err != nil {
			return nil, fmt.Errorf("storage: decode output %d locking script: %w", i, err)
		}
		plans = append(plans, outputPlan{
			satoshis:           int64(o.Satoshis), //nolint:gosec // satoshi value within int64
			lockingScript:      ls,
			basket:             optStringUnder300(o.Basket),
			change:             false,
			outputType:         string(wdk.OutputTypeCustom),
			providedBy:         string(wdk.ProvidedByYou),
			purpose:            "",
			description:        string(o.OutputDescription),
			customInstructions: o.CustomInstructions,
			tags:               stringsFrom(o.Tags),
		})
	}

	// Commission output (ProvidedByStorage).
	if p.commission.Enabled() {
		plans = append(plans, outputPlan{
			satoshis:      int64(p.commission.Satoshis), //nolint:gosec // small
			lockingScript: commissionScript,
			change:        false,
			outputType:    string(wdk.OutputTypeP2PKH),
			providedBy:    string(wdk.ProvidedByStorage),
			purpose:       wdk.StorageCommissionPurpose,
			description:   "Storage Service Charge",
		})
	}

	// Fuel fan-out outputs (throughput strategy, ProvidedByStorage): shape.Count
	// self-owned wallet-derived BRC29 P2PKH coins of exactly shape.Satoshis each,
	// booked into the destination (pool/reserve) basket. Emitted as change-purpose
	// outputs so the same derivation, signing, and mint machinery applies — the
	// only differences from ordinary change are the exact per-output value and the
	// destination basket, which make them ClaimExact-selectable fuel coins.
	if shape := args.Options.FuelShape; shape != nil {
		dest := string(shape.Basket)
		for i := uint64(0); i < shape.Count; i++ {
			suffix, err := p.rand.Base64(derivationSuffixBytes)
			if err != nil {
				return nil, fmt.Errorf("storage: generate fuel derivation suffix: %w", err)
			}
			s := suffix
			prefix := derivationPrefix
			name := dest
			plans = append(plans, outputPlan{
				satoshis:         int64(shape.Satoshis), //nolint:gosec // denomination fits int64
				change:           true,
				outputType:       string(wdk.OutputTypeP2PKH),
				providedBy:       string(wdk.ProvidedByStorage),
				purpose:          wdk.ChangePurpose,
				basket:           &name,
				derivationPrefix: &prefix,
				derivationSuffix: &s,
			})
		}
	}

	// Change outputs (ProvidedByStorage, wallet-derived BRC29 P2PKH).
	changeName := p.changeDestinationBasket(args.Options.FuelShape)
	changeValues := distributeChange(fundRes.ChangeAmount, fundRes.ChangeOutputsCount)
	for _, v := range changeValues {
		suffix, err := p.rand.Base64(derivationSuffixBytes)
		if err != nil {
			return nil, fmt.Errorf("storage: generate derivation suffix: %w", err)
		}
		s := suffix
		prefix := derivationPrefix
		name := changeName
		plans = append(plans, outputPlan{
			satoshis:         int64(v), //nolint:gosec // change value fits int64
			change:           true,
			outputType:       string(wdk.OutputTypeP2PKH),
			providedBy:       string(wdk.ProvidedByStorage),
			purpose:          wdk.ChangePurpose,
			basket:           &name,
			derivationPrefix: &prefix,
			derivationSuffix: &s,
		})
	}
	return plans, nil
}

// distributeChange splits total into count roughly-even values (last output
// absorbs the remainder). count==0 yields no change outputs.
func distributeChange(total satoshi.Value, count uint64) []uint64 {
	if count == 0 || total <= 0 {
		return nil
	}
	t := total.MustUInt64()
	out := make([]uint64, count)
	base := t / count
	rem := t % count
	for i := range out {
		out[i] = base
	}
	out[count-1] += rem
	return out
}

// fundingSource resolves which basket a create-action funds from and, under the
// throughput strategy, the fuel denomination that enables the funder's
// closed-form ClaimExact fast path.
//
// Throughput ON: fund from Throughput.PoolBasket with the resolved denomination
// (> 0). The funder claims denomination coins from the pool in a single
// ClaimExact and, on a short pool, tops up via the bounded walk over the SAME
// pool basket — a non-pool payment is never routed to some other basket. A
// truly empty pool surfaces as ErrNotEnoughFunds, which is the correct
// throughput-mode outcome (deposits fund the pool, not ad-hoc change).
//
// Throughput OFF: fund from the change basket ("default") with denomination 0,
// i.e. the unchanged bounded tiered (privacy) claim.
func (p *Provider) fundingSource(shape *wdk.ShapedChange) (basket string, denomination uint64, err error) {
	// Fuel fan-out: fund from the shape's SOURCE basket with a bounded tiered
	// claim (Denomination 0 — source coins are arbitrary, not denominated). A
	// leaf shape (destination = pool) draws from the reserve basket; a chunk
	// shape (destination = reserve) draws from the default change basket. This
	// is what lets the keeper bootstrap the pool from ordinary deposits instead
	// of trying to fund a fan-out from the (empty) pool it is trying to fill.
	if shape != nil {
		return p.fanOutSourceBasket(shape), 0, nil
	}
	if !p.utxoMgmt.Enabled() {
		return p.changeBasketName(), 0, nil
	}
	denomination, err = p.utxoMgmt.Throughput.Denomination(p.feeModel, p.commission)
	if err != nil {
		return "", 0, fmt.Errorf("storage: resolve fuel denomination: %w", err)
	}
	return p.utxoMgmt.Throughput.PoolBasket, denomination, nil
}

// fanOutSourceBasket resolves which basket a fan-out draws its funding from,
// given the shape's destination basket (see [Provider.fundingSource]).
func (p *Provider) fanOutSourceBasket(shape *wdk.ShapedChange) string {
	// An explicit source basket wins: the fuel keeper uses it to fund a leaf
	// directly from the change basket (high-throughput recycling), bypassing
	// the reserve-chunk pre-aggregation the default routing would pick.
	if shape.SourceBasket != "" {
		return string(shape.SourceBasket)
	}
	if p.utxoMgmt.Enabled() && string(shape.Basket) == p.utxoMgmt.Throughput.PoolBasket {
		return p.utxoMgmt.Throughput.ReserveBasket
	}
	return p.changeBasketName()
}

// changeDestinationBasket resolves which basket a create-action's change returns
// to. In the throughput strategy a payment's change is routed straight into the
// fuel PoolBasket so the pool self-replenishes 1:1 (one pool coin spent, one
// change coin minted back) — this bounds the local ledger and removes the fuel
// keeper's need to recycle change per payment. A fan-out mint (shape != nil) is
// the exception: its change returns to the fan-out's own funding source, so a
// leaf mint funded from the reserve cannot drain-then-refill the pool it fills.
// The privacy strategy is unchanged: change stays in the default basket.
func (p *Provider) changeDestinationBasket(shape *wdk.ShapedChange) string {
	switch {
	case shape != nil:
		return p.fanOutSourceBasket(shape)
	case p.utxoMgmt.Enabled() && p.utxoMgmt.Throughput.RecycleChangeToPool:
		// Opt-in self-replenish: only safe when mining keeps unconfirmed
		// ancestry within the mempool ancestor limit (see RecycleChangeToPool).
		return p.utxoMgmt.Throughput.PoolBasket
	default:
		return p.changeBasketName()
	}
}

// spendTiers resolves the status-tier walk for funding.
func (p *Provider) spendTiers() []utxostore.Tier {
	if p.utxoMgmt.Enabled() {
		return funder.SpendTiers(p.utxoMgmt.Throughput.SpendPolicy)
	}
	return funder.SpendTiers("prefer_mined")
}

// maxChangeOutputsPerTx returns the per-tx change-output cap (>= 1).
func (p *Provider) maxChangeOutputsPerTx() uint64 {
	if p.changeBasket.MaxChangeOutputsPerTx == 0 {
		return 1
	}
	return p.changeBasket.MaxChangeOutputsPerTx
}

// changeBasketCount returns an approximate count of outputs already in the
// change basket. It feeds funder.FundArgs.ExistingBasketCount, which clamps how
// many new change outputs a create-action mints (NumberOfDesiredUTXOs −
// existing, floored at 1). It counts descriptive rows (an over-approximation is
// safe: it only shrinks the change budget) and is fetched BEFORE any DB
// transaction opens (SQLite single-connection deadlock avoidance).
//
// Throughput mode SKIPS the count and returns 0 ("do not clamp on basket
// fullness"). Under the fuel-pool strategy funding comes from a fixed-
// denomination pool whose closed-form exact claims leave ~no change, so
// clamping the change fan-out on how full the change basket is is degenerate.
// Crucially this removes a per-op COUNT whose cost scales with the pool's row
// count — the dominant scalable cost on the throughput hot path (the funding
// pool lives in the change basket, so the count grew with pool size).
//
// The tiered (privacy) path keeps the clamp: the count still runs, now as a
// cheap indexed count over idx_outputs_user_basket with no join to transactions
// (an identical value — see OutputsRepo.CountInBasket).
func (p *Provider) changeBasketCount(ctx context.Context, userID int) (int64, error) {
	if p.utxoMgmt.Enabled() {
		return 0, nil
	}
	total, err := p.meta.Outputs().CountInBasket(ctx, userID, p.changeBasketName())
	if err != nil {
		return 0, fmt.Errorf("storage: count change basket: %w", err)
	}
	return total, nil
}

// commissionLockingScript builds the P2PKH commission locking script, or nil
// when commission is disabled.
func (p *Provider) commissionLockingScript() ([]byte, error) {
	if !p.commission.Enabled() {
		return nil, nil
	}
	pub, err := ec.PublicKeyFromString(string(p.commission.PubKeyHex))
	if err != nil {
		return nil, fmt.Errorf("storage: parse commission pubkey: %w", err)
	}
	addr, err := script.NewAddressFromPublicKey(pub, !p.network.IsTestnetBased())
	if err != nil {
		return nil, fmt.Errorf("storage: commission address: %w", err)
	}
	ls, err := p2pkh.Lock(addr)
	if err != nil {
		return nil, fmt.Errorf("storage: commission locking script: %w", err)
	}
	return ls.Bytes(), nil
}

// mapFundError normalizes funder errors to the storage surface.
func mapFundError(err error) error {
	switch {
	case errors.Is(err, funder.ErrNotEnoughFunds):
		return fmt.Errorf("storage: %w", err)
	case errors.Is(err, funder.ErrUTXOContention):
		return fmt.Errorf("storage: %w", err)
	default:
		return fmt.Errorf("storage: fund: %w", err)
	}
}

// --- small helpers -------------------------------------------------------

func providedOutputSizes(outputs []wdk.ValidCreateActionOutput) (totalSats int64, scriptLens []uint64, err error) {
	scriptLens = make([]uint64, 0, len(outputs))
	for i := range outputs {
		totalSats += int64(outputs[i].Satoshis) //nolint:gosec // satoshi value within int64
		l, lerr := outputs[i].ScriptLength()
		if lerr != nil {
			return 0, nil, fmt.Errorf("storage: output %d script length: %w", i, lerr)
		}
		scriptLens = append(scriptLens, l)
	}
	return totalSats, scriptLens, nil
}

func providedInputSizes(inputs []wdk.ValidCreateActionInput) ([]uint64, error) {
	lens := make([]uint64, 0, len(inputs))
	for i := range inputs {
		l, err := inputs[i].ScriptLength()
		if err != nil {
			// Unknown length: assume a standard P2PKH unlocking script.
			l = txutils.P2PKHUnlockingScriptLength
		}
		lens = append(lens, l)
	}
	return lens, nil
}

func allocatedTxids(utxos []*utxostore.UTXO) []string {
	seen := make(map[string]struct{}, len(utxos))
	out := make([]string, 0, len(utxos))
	for _, u := range utxos {
		txid := u.TxID.String()
		if _, ok := seen[txid]; ok {
			continue
		}
		seen[txid] = struct{}{}
		out = append(out, txid)
	}
	return out
}

func buildResultOutputs(plans []outputPlan) []*wdk.StorageCreateTransactionSdkOutput {
	out := make([]*wdk.StorageCreateTransactionSdkOutput, 0, len(plans))
	for i := range plans {
		pl := &plans[i]
		var ls primitives.HexString
		if len(pl.lockingScript) > 0 {
			ls = primitives.HexString(fmt.Sprintf("%x", pl.lockingScript))
		}
		out = append(out, &wdk.StorageCreateTransactionSdkOutput{
			ValidCreateActionOutput: wdk.ValidCreateActionOutput{
				LockingScript:      ls,
				Satoshis:           primitives.SatoshiValue(pl.satoshis), //nolint:gosec // satoshi value non-negative
				OutputDescription:  primitives.String5to2000Bytes(pl.description),
				Basket:             basketPtr(pl.basket),
				CustomInstructions: pl.customInstructions,
				Tags:               stringsUnder300(pl.tags),
			},
			Vout:             pl.vout,
			ProvidedBy:       wdk.ProvidedBy(pl.providedBy),
			Purpose:          pl.purpose,
			DerivationSuffix: pl.derivationSuffix,
		})
	}
	return out
}

func noSendChangeVouts(isNoSend bool, plans []outputPlan, changeBasket string) []int {
	if !isNoSend {
		return nil
	}
	var vouts []int
	for i := range plans {
		if plans[i].change && plans[i].basket != nil && *plans[i].basket == changeBasket {
			vouts = append(vouts, int(plans[i].vout))
		}
	}
	return vouts
}

func stringsFrom[T ~string](in []T) []string {
	if len(in) == 0 {
		return nil
	}
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = string(v)
	}
	return out
}

func stringsUnder300(in []string) []primitives.StringUnder300 {
	if len(in) == 0 {
		return nil
	}
	out := make([]primitives.StringUnder300, len(in))
	for i, v := range in {
		out[i] = primitives.StringUnder300(v)
	}
	return out
}

func optStringUnder300(p *primitives.StringUnder300) *string {
	if p == nil {
		return nil
	}
	s := string(*p)
	return &s
}

func basketPtr(p *string) *primitives.StringUnder300 {
	if p == nil {
		return nil
	}
	v := primitives.StringUnder300(*p)
	return &v
}

func hexBytes(s string) ([]byte, error) {
	if s == "" {
		return nil, nil
	}
	sc, err := script.NewFromHex(s)
	if err != nil {
		return nil, err
	}
	return sc.Bytes(), nil
}

func (p *Provider) parseProvidedBEEF(b primitives.BEEF) (*transaction.Beef, error) {
	if len(b) == 0 {
		return nil, nil
	}
	beef, err := transaction.NewBeefFromBytes(b)
	if err != nil {
		return nil, fmt.Errorf("storage: parse provided input beef: %w", err)
	}
	return beef, nil
}
