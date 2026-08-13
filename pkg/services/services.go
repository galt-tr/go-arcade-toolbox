package services

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/bsv-blockchain/go-sdk/chainhash"
	"github.com/bsv-blockchain/go-sdk/transaction"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
	"github.com/galt-tr/go-arcade-toolbox/pkg/headers"
	"github.com/galt-tr/go-arcade-toolbox/pkg/logging"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// localSourceName is the [wdk.RawTxResult.Name] / result Name reported when a
// raw tx was resolved from the local [RawTxSource] instead of Arcade.
const localSourceName = "local"

// RawTxSource is the local-first lookup [Services.RawTx] and [Services.GetBEEF]
// consult before falling back to the Arcade oracle. A wallet's own storage
// retains full ancestry for transactions it created or that were explicitly
// internalized into it; consulting that store first means RawTx/GetBEEF work
// (and avoid a network round trip) even for transactions Arcade itself never
// learned about.
//
// This interface exists so pkg/services never has to import pkg/storage (see
// the package doc for why that would be a layering cycle). The composition
// root that owns both a Services and a storage.Provider is expected to supply
// an adapter over the provider's own known-tx lookups via
// [WithLocalRawTxSource].
type RawTxSource interface {
	// LocalRawTx returns the raw transaction bytes for txid if the source has
	// them locally. found is false — with a nil error — on a clean miss; this
	// is not an error case, it just means the caller should fall back to
	// Arcade.
	LocalRawTx(ctx context.Context, txid string) (rawTx []byte, found bool, err error)
}

// Services implements the wide [wdk.Services] compatibility interface over
// the toolbox's lean [arcade.TxOracle] + [headers.Headers] clients. See the
// package doc for the design rationale.
type Services struct {
	logger *slog.Logger

	oracle arcade.TxOracle
	hdrs   headers.Headers

	getBeefMaxDepth uint
	rawTxSource     RawTxSource
}

// compile-time proof that Services satisfies the full wdk.Services interface
// (PostFromBEEF/MerklePath/FindChainTipHeader/RawTx/GetBEEF/NLockTimeIsFinal/
// GetStatusForTxIDs plus the embedded chaintracker.ChainTracker and
// BlockHeaderLoader methods).
var _ wdk.Services = (*Services)(nil)

// Services also satisfies the narrower HeightProvider interface (CurrentHeight
// alone), which is what NLockTimeIsFinal's height-based branch needs.
var _ wdk.HeightProvider = (*Services)(nil)

// Option configures a [Services] constructed by [New].
type Option func(*Services)

// WithLocalRawTxSource wires a local-first raw-tx lookup (see [RawTxSource])
// that [Services.RawTx] and [Services.GetBEEF] consult before Arcade. Without
// it (the default), RawTx/GetBEEF are Arcade-only — correct, but limited to
// transactions Arcade itself knows about (see the arcade-only design doc's
// "hard limitation" section).
func WithLocalRawTxSource(src RawTxSource) Option {
	return func(s *Services) { s.rawTxSource = src }
}

// New constructs a [Services] backed by oracle (the Arcade transaction-truth
// client) and hdrs (the block-header source). cfg supplies GetBEEF's ancestry
// depth bound (cfg.GetBeefMaxDepth; defaulted to
// [defs.DefaultGetBeefMaxDepth] when zero).
func New(logger *slog.Logger, oracle arcade.TxOracle, hdrs headers.Headers, cfg defs.WalletServices, opts ...Option) *Services {
	maxDepth := cfg.GetBeefMaxDepth
	if maxDepth == 0 {
		maxDepth = defs.DefaultGetBeefMaxDepth
	}

	s := &Services{
		logger:          logging.Child(logger, "services"),
		oracle:          oracle,
		hdrs:            hdrs,
		getBeefMaxDepth: maxDepth,
	}

	for _, opt := range opts {
		opt(s)
	}

	return s
}

// RawTx attempts to obtain the raw transaction bytes for txID: the local
// [RawTxSource] (when configured) is consulted first, falling back to Arcade's
// GET /tx ([arcade.TxOracle.GetTx]) rawTx field. Returns an error matching
// [wdk.ErrNotFoundError] when neither source has the transaction.
func (s *Services) RawTx(ctx context.Context, txID string) (wdk.RawTxResult, error) {
	if s.rawTxSource != nil {
		raw, found, err := s.rawTxSource.LocalRawTx(ctx, txID)
		if err != nil {
			return wdk.RawTxResult{}, fmt.Errorf("services: local raw tx lookup for %s: %w", txID, err)
		}
		if found {
			return wdk.RawTxResult{TxID: txID, Name: localSourceName, RawTx: raw}, nil
		}
	}

	rec, err := s.oracle.GetTx(ctx, txID)
	if err != nil {
		if errors.Is(err, arcade.ErrTxNotFound) {
			return wdk.RawTxResult{}, fmt.Errorf("services: raw tx %s not found: %w", txID, wdk.ErrNotFoundError)
		}
		return wdk.RawTxResult{}, fmt.Errorf("services: get raw tx %s: %w", txID, err)
	}
	if len(rec.RawTx) == 0 {
		return wdk.RawTxResult{}, fmt.Errorf("services: raw tx %s not available from arcade: %w", txID, wdk.ErrNotFoundError)
	}

	return wdk.RawTxResult{TxID: txID, Name: defs.ArcadeServiceName, RawTx: rec.RawTx}, nil
}

// GetBEEF assembles a BEEF covering txID and (recursively) its ancestry, using
// [Services.RawTx] (local-first, then Arcade) for the transaction bytes and
// [Services.MerklePath] (Arcade-only) to attach proofs and stop the walk at
// the first mined ancestor. knownTxIDs are txids the caller asserts the
// wallet already holds; they are merged as txid-only stubs rather than
// walked.
//
// Ancestry is bounded by cfg.GetBeefMaxDepth (see [New]); per the arcade-only
// design, ancestry is inherently limited to transactions the configured
// RawTxSource or Arcade itself knows about — an ancestor neither source has
// surfaces as an error, not a silent truncation.
func (s *Services) GetBEEF(ctx context.Context, txID string, knownTxIDs []string) (*transaction.Beef, error) {
	beef := transaction.NewBeefV2()

	known := make(map[string]struct{}, len(knownTxIDs))
	for _, k := range knownTxIDs {
		known[k] = struct{}{}
	}

	if err := s.walkBEEF(ctx, beef, txID, known, 0); err != nil {
		return nil, fmt.Errorf("services: get beef for %q: %w", txID, err)
	}
	return beef, nil
}

func (s *Services) walkBEEF(ctx context.Context, beef *transaction.Beef, txid string, known map[string]struct{}, depth uint) error {
	if depth > s.getBeefMaxDepth {
		return fmt.Errorf("services: beef ancestry exceeds max depth %d at %s", s.getBeefMaxDepth, txid)
	}

	hash, err := chainhash.NewHashFromHex(txid)
	if err != nil {
		return fmt.Errorf("services: parse txid %q: %w", txid, err)
	}
	if beef.FindTransactionByHash(hash) != nil {
		return nil // already present
	}

	rawTxResult, err := s.RawTx(ctx, txid)
	if err != nil {
		return fmt.Errorf("services: get raw tx for beef ancestor %s: %w", txid, err)
	}

	tx, err := transaction.NewTransactionFromBytes(rawTxResult.RawTx)
	if err != nil {
		return fmt.Errorf("services: parse raw tx %s: %w", txid, err)
	}

	merklePathResult, err := s.MerklePath(ctx, txid)
	if err != nil && !errors.Is(err, wdk.ErrNotFoundError) {
		return fmt.Errorf("services: get merkle path for %s: %w", txid, err)
	}

	isMined := merklePathResult != nil && merklePathResult.MerklePath != nil
	if isMined {
		tx.MerklePath = merklePathResult.MerklePath
	}

	if _, err := beef.MergeTransaction(tx); err != nil {
		return fmt.Errorf("services: merge tx %s into beef: %w", txid, err)
	}

	if isMined {
		return nil // a proven tx's own ancestry need not be walked further.
	}

	for _, input := range tx.Inputs {
		if input.SourceTXID == nil {
			continue
		}
		if beef.Transactions[*input.SourceTXID] != nil {
			continue
		}

		sourceTxID := input.SourceTXID.String()
		if _, ok := known[sourceTxID]; ok {
			beef.MergeTxidOnly(input.SourceTXID)
			continue
		}

		if err := s.walkBEEF(ctx, beef, sourceTxID, known, depth+1); err != nil {
			return fmt.Errorf("services: get beef ancestor %s: %w", sourceTxID, err)
		}
	}

	return nil
}
