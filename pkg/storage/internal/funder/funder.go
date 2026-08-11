package funder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/internal/satoshi"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

const (
	// insufficientBatchSize bounds how many largest-insufficient coins are
	// claimed per micro-query. It amortizes round trips when many small inputs
	// are needed while keeping the number of reserved rows small.
	insufficientBatchSize = 16

	// maxFundingAttempts bounds how many times the whole selection is retried
	// when an optimistic store reports contention. Lock-based stores (SQL,
	// memstore) never trigger a retry.
	maxFundingAttempts = 3

	// backoff bounds for the jittered wait between contention retries.
	minBackoff = 25 * time.Millisecond
	maxBackoff = 100 * time.Millisecond
)

// Funder selects and reserves coins from a [utxostore.Store] to cover a funding
// target, computing the fee and change shape along the way. It implements the
// BoundedTieredClaim algorithm: per-allocation, target-aware micro-queries that
// reserve rows as they go (claim-as-you-go), so its cost is flat in pool size
// and it needs no cross-call lock or exclusion-list threading.
type Funder struct {
	logger        *slog.Logger
	store         utxostore.Store
	feeCalculator *feeCalc
	feeModel      defs.FeeModel

	// backoff waits before a contention retry. Overridable in tests so the
	// retry logic can be exercised without real sleeps.
	backoff func(ctx context.Context, attempt int)
}

// Option configures a Funder.
type Option func(*Funder)

// New creates a Funder over store using the given static fee model.
//
// It panics on an unsupported fee model type or a negative rate — both are
// configuration bugs, not runtime conditions. Callers should validate the
// model with [defs.FeeModel.Validate] before constructing the funder.
func New(logger *slog.Logger, store utxostore.Store, feeModel defs.FeeModel, opts ...Option) *Funder {
	f := &Funder{
		logger:        logging.Child(logger, "funder"),
		store:         store,
		feeCalculator: newFeeCalculator(feeModel),
		feeModel:      feeModel,
		backoff:       defaultBackoff,
	}
	for _, opt := range opts {
		opt(f)
	}
	return f
}

// defaultBackoff sleeps for a jittered duration in [minBackoff, maxBackoff),
// aborting early if ctx is canceled. attempt is accepted for parity with
// overrides but the default wait is flat-jittered, not exponential.
func defaultBackoff(ctx context.Context, _ int) {
	span := maxBackoff - minBackoff
	//nolint:gosec // jitter for retry spacing, not a security-sensitive random
	wait := minBackoff + time.Duration(rand.Int64N(int64(span)))
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// SpendTiers maps a spend policy to the status-tier walk, safest first.
func SpendTiers(policy defs.SpendPolicy) []utxostore.Tier {
	switch policy {
	case defs.SpendPolicyMinedOnly:
		return []utxostore.Tier{utxostore.TierMined}
	case defs.SpendPolicyAny:
		return []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven, utxostore.TierSending}
	case defs.SpendPolicyPreferMined:
		return []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven}
	default:
		return []utxostore.Tier{utxostore.TierMined, utxostore.TierUnproven}
	}
}

// FundArgs groups the inputs for a single Fund call.
//
// ExistingBasketCount is passed IN by the caller, fetched BEFORE the caller
// opens any database transaction. This is deliberate: fetching it lazily inside
// the funder would, on SQLite (single connection held by the ambient
// transaction), deadlock. Passing it in keeps the funder pure and testable.
type FundArgs struct {
	// UserID and Basket scope every claim; Reservation is the token every
	// reserved row is held under (the transaction reference).
	//
	// Reservation MUST be unique to this fund attempt: do not share a token
	// with any other in-flight fund or with any unspent reservation for the
	// same user. On ANY error, Fund releases the ENTIRE token (a whole-token
	// ReleaseReservation), so a shared token would free another transaction's
	// coins out from under it.
	UserID      int64
	Basket      string
	Reservation string

	// TargetSat is the net satoshis the inputs must cover (may be negative when
	// provided inputs exceed provided outputs). CurrentTxSize and OutputCount
	// describe the transaction as it stands before the funder adds inputs.
	TargetSat     satoshi.Value
	CurrentTxSize uint64
	OutputCount   uint64

	// Tiers is the status-tier walk, safest first (see SpendTiers).
	Tiers []utxostore.Tier

	// Change-basket parameters. MaxChangeOutputsPerTx must be >= 1.
	NumberOfDesiredUTXOs    int64
	MinimumDesiredUTXOValue uint64
	MaxChangeOutputsPerTx   uint64

	// RequireChange forbids the funder from omitting the change output.
	//
	// By default, once the allocated coins cover the target plus fee, whatever
	// is left over is only kept if it clears the dust floor; below that it is
	// donated to the miner and NO change output is produced. That is the right
	// call for an ordinary payment — a sub-dust output costs more to spend than
	// it holds — but it silently changes the transaction's output count, and a
	// caller whose spending conditions commit to the output shape (a covenant
	// reconstructing [continuation, change], for instance) cannot spend the
	// result.
	//
	// With RequireChange the walk instead keeps allocating until the change
	// clears the dust floor, and reports ErrNotEnoughFunds if it cannot. The
	// transaction then always has exactly the shape the caller planned for.
	RequireChange bool
	// ExistingBasketCount is the number of coins already in the change basket,
	// used to cap how many new change outputs are created.
	ExistingBasketCount int64

	// Denomination, when > 0, enables the throughput fast path: a single
	// closed-form ClaimExact of denomination coins, topped up via the bounded
	// walk on underflow.
	Denomination uint64
}

func (a *FundArgs) validate() error {
	switch {
	case a.Reservation == "":
		return errors.New("funder: reservation must be non-empty")
	case a.UserID <= 0:
		return errors.New("funder: user id must be positive")
	case a.Basket == "":
		return errors.New("funder: basket must be non-empty")
	case len(a.Tiers) == 0:
		return errors.New("funder: at least one tier is required")
	case a.MaxChangeOutputsPerTx == 0:
		return errors.New("funder: max change outputs per tx must be >= 1")
	}
	for _, tier := range a.Tiers {
		if !tier.Valid() {
			return fmt.Errorf("funder: invalid tier %d", tier)
		}
	}
	return nil
}

// Fund reserves coins to cover args.TargetSat and returns the allocation, fee,
// and change shape. On success the coins stay reserved under args.Reservation
// (the caller spends them). On ANY error the ENTIRE reservation is released (a
// whole-token ReleaseReservation), so nothing is left held — which is why
// args.Reservation must be unique to this attempt (see FundArgs.Reservation).
//
// Contention (from optimistic stores) is retried up to maxFundingAttempts times
// with a jittered backoff, then surfaced as ErrUTXOContention; lock-based
// stores never trigger a retry.
func (f *Funder) Fund(ctx context.Context, args FundArgs) (*Result, error) {
	if err := args.validate(); err != nil {
		return nil, err
	}

	for attempt := 0; ; attempt++ {
		result, err := f.fundOnce(ctx, args)
		switch {
		case err == nil:
			return result, nil
		case errors.Is(err, utxostore.ErrContention):
			// Release whatever this attempt reserved, then back off and retry.
			f.release(ctx, args.UserID, args.Reservation)
			if attempt+1 >= maxFundingAttempts {
				f.logger.WarnContext(ctx, "funding abandoned: contention retries exhausted",
					logging.UserID(int(args.UserID)), logging.Reference(args.Reservation))
				return nil, ErrUTXOContention
			}
			f.backoff(ctx, attempt)
		default:
			// ErrNotEnoughFunds or a hard store error: release and surface it.
			f.release(ctx, args.UserID, args.Reservation)
			return nil, err
		}
	}
}

// fundOnce runs one full selection pass: the optional throughput fast path
// followed by the bounded tiered walk. It never retries contention itself —
// that is Fund's job.
func (f *Funder) fundOnce(ctx context.Context, args FundArgs) (*Result, error) {
	collector, err := newCollector(
		args.TargetSat,
		args.CurrentTxSize,
		args.OutputCount,
		args.NumberOfDesiredUTXOs-args.ExistingBasketCount,
		args.MinimumDesiredUTXOValue,
		f.feeCalculator,
		args.MaxChangeOutputsPerTx,
		args.RequireChange,
	)
	if err != nil {
		return nil, fmt.Errorf("failed to start collecting utxos: %w", err)
	}

	// Throughput fast path: grab most of the fuel in a single exact claim.
	if args.Denomination > 0 {
		if err = f.claimExactFastPath(ctx, args, collector); err != nil {
			return nil, err
		}
		if collector.isFunded() {
			return collector.result()
		}
	}

	// Bounded tiered best-fit selection.
	if err = f.boundedTieredClaim(ctx, args, collector); err != nil {
		return nil, err
	}
	return collector.result()
}

// boundedTieredClaim drives allocation rounds until the collector is funded, a
// round recomputes a non-positive remaining need (GetResult then decides funded
// vs. ErrNotEnoughFunds), or a full pass over all tiers allocates nothing.
func (f *Funder) boundedTieredClaim(ctx context.Context, args FundArgs, collector *utxoCollector) error {
	for !collector.isFunded() {
		remaining := collector.remaining()
		if remaining <= 0 {
			return nil
		}

		allocated, err := f.round(ctx, args, collector, remaining)
		if err != nil {
			return err
		}
		if !allocated {
			return ErrNotEnoughFunds
		}
	}
	return nil
}

// round tries each tier safest-first and stops at the first tier that allocates.
func (f *Funder) round(ctx context.Context, args FundArgs, collector *utxoCollector, remaining satoshi.Value) (bool, error) {
	for _, tier := range args.Tiers {
		allocated, err := f.fromTier(ctx, args, collector, tier, remaining)
		if err != nil {
			return false, err
		}
		if allocated {
			return true, nil
		}
	}
	return false, nil
}

// fromTier applies the smallest-sufficient claim, then (if none) the
// largest-insufficient batch drain, within a single tier.
func (f *Funder) fromTier(ctx context.Context, args FundArgs, collector *utxoCollector, tier utxostore.Tier, remaining satoshi.Value) (bool, error) {
	scope := utxostore.Scope{UserID: args.UserID, Basket: args.Basket, Tier: tier}

	utxo, err := f.store.ClaimSmallestSufficient(ctx, scope, args.Reservation, remaining.MustUInt64())
	if err != nil {
		return false, fmt.Errorf("failed to claim smallest sufficient utxo: %w", err)
	}
	if utxo != nil {
		if err = collector.allocateUTXO(utxo); err != nil {
			return false, fmt.Errorf("failed to allocate utxo: %w", err)
		}
		return true, nil
	}

	batch, err := f.store.ClaimLargestInsufficient(ctx, scope, args.Reservation, remaining.MustUInt64(), insufficientBatchSize)
	if err != nil {
		return false, fmt.Errorf("failed to claim largest insufficient utxos: %w", err)
	}
	return f.drainBatch(ctx, args, collector, batch)
}

// drainBatch consumes the DESC largest-insufficient batch while each row stays
// equivalent to the old pool best-fit stage-3 pick, then RELEASES the
// unconsumed remainder (the batch reserved every row it returned).
//
// Drain rule (per-step equivalence with the old best-fit selection): the
// sufficient claim in fromTier returned nothing, so every claimable row in this
// tier is < remaining and the DESC batch holds the tier's largest rows —
// batch[i] is always the largest still-unallocated row. Consume batch[i] only
// while !IsFunded() AND batch[i].Satoshis is still below the freshly recomputed
// remaining: then no row >= remaining exists (batch[i] is the maximum), so
// best-fit would pick exactly batch[i]. The moment batch[i].Satoshis >=
// remaining — remaining shrank past it, or grew via the dust-floor branch — the
// "largest insufficient" premise breaks and best-fit would instead pick the
// SMALLEST sufficient row, which need not be batch[i]; so we stop draining,
// release the untouched tail, and re-enter the outer loop, whose sufficient
// claim finds that row.
func (f *Funder) drainBatch(ctx context.Context, args FundArgs, collector *utxoCollector, batch []*utxostore.UTXO) (bool, error) {
	allocated := false
	consumed := 0
	for _, u := range batch {
		if collector.isFunded() {
			break
		}
		remaining := collector.remaining()
		if remaining <= 0 || u.Satoshis >= remaining.MustUInt64() {
			break
		}
		if err := collector.allocateUTXO(u); err != nil {
			// Leave the tail reserved; Fund's ReleaseReservation cleans up the
			// whole token on this error path.
			return allocated, fmt.Errorf("failed to allocate utxo: %w", err)
		}
		consumed++
		allocated = true
	}

	// Release the unconsumed remainder so those coins re-enter the claimable
	// pool for the next round's smallest-sufficient claim.
	if consumed < len(batch) {
		if err := f.releaseOutpoints(ctx, args.Reservation, batch[consumed:]); err != nil {
			return allocated, err
		}
	}
	return allocated, nil
}

// release frees every row held under (userID, reservation); best-effort, errors
// are logged, not propagated (the primary outcome is already decided).
func (f *Funder) release(ctx context.Context, userID int64, reservation string) {
	if _, err := f.store.ReleaseReservation(ctx, userID, reservation); err != nil {
		f.logger.WarnContext(ctx, "failed to release reservation",
			logging.Reference(reservation), logging.Error(err))
	}
}

// releaseOutpoints frees a specific set of reserved coins by outpoint.
func (f *Funder) releaseOutpoints(ctx context.Context, reservation string, utxos []*utxostore.UTXO) error {
	ops := make([]utxostore.Outpoint, len(utxos))
	for i, u := range utxos {
		ops[i] = u.Outpoint
	}
	if err := f.store.ReleaseOutpoints(ctx, reservation, ops); err != nil {
		return fmt.Errorf("failed to release unconsumed batch: %w", err)
	}
	return nil
}
