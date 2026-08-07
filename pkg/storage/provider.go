package storage

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"time"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/arcade"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/defs"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/headers"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/logging"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/funder"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/storage/internal/metastore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/wdk"
)

// DefaultMaxOutputScript is the maximum output locking-script length surfaced in
// [wdk.TableSettings]; it mirrors the old repo's DefaultMaxScriptLength.
const DefaultMaxOutputScript = 1024 * 1024 * 1024

// settingsKey is the key_values key under which the storage settings JSON lives.
const settingsKey = "settings"

// ErrAuthorization is returned when a request lacks a resolved user id, or when
// a caller tries to access another user's data.
var ErrAuthorization = errors.New("storage: access is denied due to an authorization error")

// utxoDB is the optional capability a SQL-backed [utxostore.Store] exposes so
// the provider can detect Mode A (metastore and utxostore over one *sql.DB) and
// share a single transaction across both.
type utxoDB interface {
	SharesDatabase(*sql.DB) bool
	DB() *sql.DB
}

// Provider is the wallet storage backend. It satisfies
// [wdk.WalletStorageProvider] by orchestrating the metastore (metadata), the
// utxostore (inventory) and the funder (coin selection), broadcasting through
// an [arcade.TxOracle] and verifying merkle roots against a [headers.Headers]
// source. Safe for concurrent use (the subsystems are).
type Provider struct {
	logger *slog.Logger

	meta   *metastore.Store
	utxo   utxostore.Store
	funder *funder.Funder
	oracle arcade.TxOracle
	hdrs   headers.Headers

	scripts wdk.ScriptsVerifier
	beef    wdk.BeefVerifier
	rand    wdk.Randomizer

	feeModel     defs.FeeModel
	changeBasket defs.ChangeBasket
	utxoMgmt     defs.UTXOManagement
	commission   defs.Commission
	network      defs.BSVNetwork

	storageName        string
	storageIdentityKey string

	// immediateBroadcast forces ProcessAction to broadcast synchronously,
	// overriding the caller's acceptDelayedBroadcast option (see
	// WithImmediateBroadcast).
	immediateBroadcast bool

	// sendConcurrency bounds how many delayed transactions SendWaitingTransactions
	// broadcasts to arcade in parallel per run. 1 (default) preserves the original
	// sequential behavior; higher values let the background drainer keep pace with
	// a high creation rate so the delayed queue does not grow unbounded (see
	// WithSendConcurrency).
	sendConcurrency int

	// directInputBEEF, when true, assembles a transaction's stored input BEEF
	// from only the directly-spent coins' transactions rather than their full
	// recursive ancestry. Arcade validates the extended-format broadcast from
	// each input's inline prevout, so the direct source is sufficient; the deep
	// ancestry is BRC-62-verifier completeness the arcade-only model does not
	// need. It bounds assembly to O(inputs) and prevents a deep unproven chain
	// (delayed mode, nothing mined yet) from blowing up buildBEEF's MerklePath
	// root recomputation (see WithDirectInputBEEF).
	directInputBEEF bool

	now func() time.Time

	// modeA is true when the metastore and utxostore share one *sql.DB, so the
	// two-store write cores commit atomically in a single metastore.Do.
	modeA bool
}

// compile-time assertion that Provider satisfies the 21-method interface.
var _ wdk.WalletStorageProvider = (*Provider)(nil)

// Option configures a Provider.
type Option func(*Provider)

// WithScriptsVerifier overrides the default (go-sdk interpreter) scripts verifier.
func WithScriptsVerifier(v wdk.ScriptsVerifier) Option {
	return func(p *Provider) { p.scripts = v }
}

// WithBeefVerifier overrides the default (go-sdk beef.Verify over the headers
// source) BEEF verifier.
func WithBeefVerifier(v wdk.BeefVerifier) Option {
	return func(p *Provider) { p.beef = v }
}

// WithRandomizer overrides the default crypto/rand randomizer.
func WithRandomizer(r wdk.Randomizer) Option {
	return func(p *Provider) { p.rand = r }
}

// WithFeeModel sets the static fee model used for change/fee computation.
func WithFeeModel(m defs.FeeModel) Option {
	return func(p *Provider) { p.feeModel = m }
}

// WithChangeBasket sets the default change-basket configuration.
func WithChangeBasket(b defs.ChangeBasket) Option {
	return func(p *Provider) { p.changeBasket = b }
}

// WithUTXOManagement sets the UTXO-management strategy configuration.
func WithUTXOManagement(u defs.UTXOManagement) Option {
	return func(p *Provider) { p.utxoMgmt = u }
}

// WithCommission enables/sets the storage commission.
func WithCommission(c defs.Commission) Option {
	return func(p *Provider) { p.commission = c }
}

// WithNetwork sets the BSV network reported in settings.
func WithNetwork(n defs.BSVNetwork) Option {
	return func(p *Provider) { p.network = n }
}

// WithStorageName sets the human-readable storage name reported in settings.
func WithStorageName(name string) Option {
	return func(p *Provider) { p.storageName = name }
}

// WithClock overrides the provider clock (tests).
func WithClock(now func() time.Time) Option {
	return func(p *Provider) { p.now = now }
}

// WithImmediateBroadcast forces ProcessAction to broadcast every transaction
// synchronously, ignoring the BRC-100 acceptDelayedBroadcast option (which
// defaults to true, deferring the send to the monitor's SendWaiting task). With
// this set, a transaction is posted to arcade before ProcessAction returns and
// its on-network status is known immediately.
//
// This trades away the delayed-mode throughput headroom (delayed broadcast is
// what keeps arcade's round-trip off the client's critical path in the
// high-TPS design) for immediate confirmation. Explicit no-send transactions
// are unaffected — they still wait for their sendWith batch.
func WithImmediateBroadcast() Option {
	return func(p *Provider) { p.immediateBroadcast = true }
}

// WithSendConcurrency sets how many delayed transactions SendWaitingTransactions
// broadcasts to arcade in parallel per run. The default of 1 broadcasts
// sequentially; a high-throughput deployment running delayed broadcast should
// raise it (e.g. to the arcade client's connection budget) so the background
// drainer sustains the creation rate and the delayed queue stays bounded. n < 1
// is treated as 1.
func WithSendConcurrency(n int) Option {
	return func(p *Provider) {
		if n < 1 {
			n = 1
		}
		p.sendConcurrency = n
	}
}

// WithDirectInputBEEF bounds stored input-BEEF assembly to the directly-spent
// coins' transactions (no recursive ancestry). It is the correct setting for an
// arcade-only deployment: arcade validates the EF broadcast from inline
// prevouts, so deep ancestry is unnecessary, and omitting it keeps input-BEEF
// assembly O(inputs) instead of O(chain length) — essential for high-throughput
// delayed broadcast, where unproven coins accumulate faster than they are
// proven and the ancestry would otherwise grow without bound.
func WithDirectInputBEEF() Option {
	return func(p *Provider) { p.directInputBEEF = true }
}

// New builds a Provider over the given subsystems. The scripts/beef verifiers
// default to local go-sdk-backed implementations, the randomizer to a
// crypto/rand implementation, and the config to the defs defaults; override any
// with an [Option]. It detects Mode A when the utxostore shares the metastore's
// *sql.DB.
func New(
	logger *slog.Logger,
	meta *metastore.Store,
	utxo utxostore.Store,
	fnd *funder.Funder,
	oracle arcade.TxOracle,
	hdrs headers.Headers,
	opts ...Option,
) (*Provider, error) {
	if meta == nil || utxo == nil || fnd == nil {
		return nil, errors.New("storage: metastore, utxostore and funder are required")
	}
	p := &Provider{
		logger:       logging.Child(logger, "storage"),
		meta:         meta,
		utxo:         utxo,
		funder:       fnd,
		oracle:       oracle,
		hdrs:         hdrs,
		feeModel:     defs.DefaultFeeModel(),
		changeBasket: defs.DefaultChangeBasket(),
		utxoMgmt:     defs.DefaultUTXOManagement(),
		commission:   defs.DefaultCommission(),
		network:      defs.NetworkMainnet,
		now:          time.Now,
	}
	for _, opt := range opts {
		opt(p)
	}
	if p.rand == nil {
		p.rand = newDefaultRandomizer()
	}
	if p.scripts == nil {
		p.scripts = newDefaultScriptsVerifier()
	}
	if p.beef == nil {
		if hdrs == nil {
			return nil, errors.New("storage: a headers source or an explicit BeefVerifier is required")
		}
		p.beef = newDefaultBeefVerifier(headersChainTracker{hdrs})
	}

	// Mode A detection: the utxostore is a SQL store over the metastore's DB.
	if s, ok := utxo.(utxoDB); ok && meta.SharesDatabase(s.DB()) {
		p.modeA = true
	}
	p.logger.Debug("storage provider constructed", slog.Bool("modeA", p.modeA))
	return p, nil
}

// ModeA reports whether the metastore and utxostore share one database (so the
// write cores commit atomically in a single transaction). Exposed for tests.
func (p *Provider) ModeA() bool { return p.modeA }

// userID resolves and validates the authenticated user id, returning
// [ErrAuthorization] when absent.
func (p *Provider) userID(auth wdk.AuthID) (int, error) {
	if auth.UserID == nil {
		return 0, ErrAuthorization
	}
	return *auth.UserID, nil
}

// changeBasketName is the destination basket for change ("default").
func (p *Provider) changeBasketName() string { return wdk.BasketNameForChange }

// changeBasketConfig returns the wdk basket configuration for the change basket.
func (p *Provider) changeBasketConfig() wdk.BasketConfiguration {
	return wdk.BasketConfiguration{
		Name:                    wdk.BasketNameForChange,
		NumberOfDesiredUTXOs:    p.changeBasket.NumberOfDesiredUTXOs,
		MinimumDesiredUTXOValue: p.changeBasket.MinimumDesiredUTXOValue,
	}
}

// seedBaskets ensures a new user has the change basket (and, under the
// throughput strategy, the fuel and reserve baskets). Runs against ctx's execer.
func (p *Provider) seedBaskets(ctx context.Context, userID int) error {
	if err := p.meta.Baskets().Upsert(ctx, userID, p.changeBasketConfig(), true); err != nil {
		return err
	}
	if p.utxoMgmt.Enabled() {
		for _, name := range []string{p.utxoMgmt.Throughput.PoolBasket, p.utxoMgmt.Throughput.ReserveBasket} {
			if name == "" {
				continue
			}
			if err := p.meta.Baskets().FindOrCreate(ctx, userID, name); err != nil {
				return err
			}
		}
	}
	return nil
}

// dbType maps the metastore engine to the wdk/defs DBType reported in settings.
func (p *Provider) dbType() defs.DBType {
	return defs.DBType(string(p.meta.Engine()))
}

// trace is a tiny helper for consistent debug logging of a method entry.
func (p *Provider) trace(ctx context.Context, method string, attrs ...slog.Attr) {
	p.logger.LogAttrs(ctx, slog.LevelDebug, method, attrs...)
}
