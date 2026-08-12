// Package aerostore is the Aerospike provider for [utxostore.Store]: the
// hot-path UTXO inventory backend for the 1000-TPS wallet design. It stores one
// record per UTXO, keyed by the digest of (txid ‖ vout), so that a wallet
// claiming individual outputs across many users never serializes on a shared
// per-transaction record generation.
//
// # Atomic transitions without Lua
//
// Every lifecycle transition (reserve, spend, unspend, freeze, promote, …) is a
// single-record compare-and-set expressed as an Operate whose write policy
// carries a FilterExpression and RecordExistsAction=UPDATE_ONLY. The filter is
// the state precondition; when it evaluates false the server returns
// FILTERED_OUT, which the provider maps to "lost the race" (claims) or the
// appropriate per-item error taxonomy (spend/remove). No user-defined functions
// are used. This was verified against a live Aerospike Community Edition server
// (Operate + FilterExpression + UPDATE_ONLY yields FILTERED_OUT on a false
// guard, and PutOp together with a bin removal apply atomically in one Operate).
//
// # Claimability, encoded in the record
//
//   - claimKey ("{userId}|{basket}|{tier}|{bucket}") is present ONLY while a
//     coin is claimable (unreserved, unspent, not frozen). Reserving, spending
//     or freezing removes it; releasing, unspending or unfreezing restores it.
//     Because a reserved coin's claimKey is gone, it vanishes from the claimKey
//     secondary index synchronously — the basis of the exclusivity guarantee.
//   - invKey ("{userId}|{basket}") is present until a coin is spent (or
//     removed); it backs Balance and per-(user, basket) scans.
//   - resBy/resAt mark a reservation (kept as provenance after a spend);
//     spentBy marks a recorded spend; frozen marks an explicit hold.
//   - bucket = floor(log2(sats)) gives 64 best-fit buckets. A denominated pool
//     shares one bucket, so ClaimExact is a pure index hit; arbitrary amounts
//     are served by walking buckets (smallest- or largest-first) with a bounded
//     best-of-sample approximation — never over/under-spending, never
//     double-claiming, only relaxing selection optimality.
//
// # Fund safety
//
// A wallet's UTXO set is the only record of the coins it controls, so the store
// refuses to start unless the namespace's default-ttl is 0 (rows must never
// silently expire) and issues durable deletes when the server supports them
// (Enterprise Edition; Community Edition is detected and durable deletes are
// disabled with a loud warning). Deletion is explicit only — there is no
// background expiry.
package aerostore

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	as "github.com/aerospike/aerospike-client-go/v8"
	"github.com/aerospike/aerospike-client-go/v8/types"
	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/bsv-blockchain/go-arcade-toolbox/pkg/utxostore"
)

// Bin names (each ≤ 15 chars, the Aerospike bin-name limit).
const (
	binTxID     = "txid"
	binVout     = "vout"
	binUserID   = "userId"
	binBasket   = "basket"
	binTier     = "tier"
	binSats     = "sats"
	binInpSize  = "inpSize"
	binResBy    = "resBy"
	binResAt    = "resAt"
	binSpentBy  = "spentBy"
	binFrozen   = "frozen"
	binCreated  = "createdAt"
	binClaimKey = "claimKey"
	binInvKey   = "invKey"
)

// errClosed is returned by every method after Close.
var errClosed = errors.New("aerostore: store is closed")

// compile-time interface check.
var _ utxostore.Store = (*Store)(nil)

// Store is an Aerospike-backed [utxostore.Store]. Create it with [New] or
// [Open]. Safe for concurrent use.
type Store struct {
	client    *as.Client
	namespace string
	set       string

	now            func() time.Time
	logger         *slog.Logger
	durableDeletes bool

	// claimCache amortizes ClaimExact's claimKey index probe across many claims.
	// nil when disabled (WithClaimCache(0)), in which case ClaimExact falls back
	// to a fresh probe per call.
	claimCache *claimCache

	// restoreRaceHook, when non-nil, is invoked by the release/unspend/unfreeze
	// restore paths immediately BEFORE their server-side restore Operate. It is
	// a TEST-ONLY seam for deterministically landing a concurrent Promote inside
	// the caller's snapshot→restore window (the stale-tier claimKey race);
	// production leaves it nil so the check is a single predictable branch.
	restoreRaceHook func()

	closeOnce sync.Once
	closed    atomic.Bool
}

// Option configures a Store.
type Option func(*config)

type config struct {
	now            func() time.Time
	logger         *slog.Logger
	set            string
	user           string
	password       string
	durableDeletes *bool // nil = auto-detect from server edition
	indexTimeout   time.Duration
	claimCacheSize int // ClaimExact candidate-cache refill batch; <=0 disables
}

// defaultClaimCacheSize is the ClaimExact candidate-cache refill batch: one
// index probe fetches this many candidates, which are then served to that many
// claims via direct CAS reserves before the next probe. Sized to keep the
// per-claim index-query rate (and its single-node routing-lock contention) low
// while bounding queued memory to O(refill) records per live denomination.
const defaultClaimCacheSize = 512

// WithClock overrides the store's clock (used for ReservedAt and CreatedAt).
// Intended for tests that need deterministic timestamps.
func WithClock(now func() time.Time) Option { return func(c *config) { c.now = now } }

// WithLogger sets the store's logger (defaults to slog.Default()).
func WithLogger(l *slog.Logger) Option { return func(c *config) { c.logger = l } }

// WithSet overrides the Aerospike set name (defaults to [DefaultSet]).
func WithSet(set string) Option { return func(c *config) { c.set = set } }

// WithAuth sets Aerospike authentication credentials (Enterprise security).
func WithAuth(user, password string) Option {
	return func(c *config) { c.user, c.password = user, password }
}

// WithDurableDeletes forces durable-delete behavior on or off, overriding the
// automatic edition detection. Forcing it on against a Community Edition server
// makes every delete fail, so this is intended for Enterprise deployments that
// want to assert the invariant explicitly.
func WithDurableDeletes(on bool) Option { return func(c *config) { c.durableDeletes = &on } }

// WithClaimCache sets the ClaimExact candidate-cache refill batch size. A single
// claimKey index probe fetches up to n candidates that subsequent ClaimExact
// calls drain with direct CAS reserves, collapsing the per-claim query rate that
// otherwise serializes on the aerospike client's per-node query-routing lock.
// n <= 0 disables the cache (a fresh probe per claim). Defaults to
// [defaultClaimCacheSize].
func WithClaimCache(n int) Option { return func(c *config) { c.claimCacheSize = n } }

// New connects to the Aerospike cluster at host:port serving namespace, runs
// the fund-safety and edition checks, ensures the secondary indexes exist, and
// returns a ready store. It fails if the namespace's default-ttl is not 0.
func New(ctx context.Context, host string, port int, namespace string, opts ...Option) (*Store, error) {
	cfg := config{
		now:            time.Now,
		logger:         slog.Default(),
		set:            DefaultSet,
		indexTimeout:   30 * time.Second,
		claimCacheSize: defaultClaimCacheSize,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	clientPolicy := as.NewClientPolicy()
	clientPolicy.Timeout = 10 * time.Second
	if cfg.user != "" {
		clientPolicy.User = cfg.user
		clientPolicy.Password = cfg.password
	}

	client, aerr := as.NewClientWithPolicyAndHost(clientPolicy, as.NewHost(host, port))
	if aerr != nil {
		return nil, fmt.Errorf("aerostore: connect to %s:%d: %w", host, port, aerr)
	}

	s := &Store{
		client:    client,
		namespace: namespace,
		set:       cfg.set,
		now:       cfg.now,
		logger:    cfg.logger.With("component", "aerostore"),
	}
	if cfg.claimCacheSize > 0 {
		s.claimCache = newClaimCache(cfg.claimCacheSize)
	}

	if err := s.checkFundSafety(ctx); err != nil {
		client.Close()
		return nil, err
	}
	s.resolveDurableDeletes(ctx, cfg.durableDeletes)
	if err := s.ensureIndexes(ctx, cfg.indexTimeout); err != nil {
		client.Close()
		return nil, err
	}
	return s, nil
}

// Open builds a Store from an "aerospike://host:port/namespace?set=utxos" DSN.
// It is the entry point registered with the utxostore factory.
func Open(ctx context.Context, dsn string, opts ...Option) (*Store, error) {
	cfg, err := parseDSN(dsn)
	if err != nil {
		return nil, err
	}
	all := []Option{WithSet(cfg.set)}
	if cfg.user != "" {
		all = append(all, WithAuth(cfg.user, cfg.password))
	}
	all = append(all, opts...)
	return New(ctx, cfg.host, cfg.port, cfg.namespace, all...)
}

// checkFundSafety refuses to start unless the namespace default-ttl is 0. A
// wallet must never let the database evict its funds.
func (s *Store) checkFundSafety(ctx context.Context) error {
	key := "namespace/" + s.namespace
	info, ierr := s.requestInfo(ctx, key)
	if ierr != nil {
		return fmt.Errorf("aerostore: query namespace config: %w", ierr)
	}
	raw, ok := info[key]
	if !ok {
		return fmt.Errorf("aerostore: namespace %q not found on server", s.namespace)
	}
	return validateDefaultTTL(s.namespace, raw)
}

// validateDefaultTTL parses an Aerospike "namespace/<ns>" info string and
// returns an error unless its default-ttl is 0. A wallet's UTXO rows must never
// silently expire (see the package doc). Extracted from the network path so the
// fund-safety rule is unit-testable without a server.
func validateDefaultTTL(namespace, raw string) error {
	ttl, found := "", false
	for _, kv := range strings.Split(raw, ";") {
		if v, ok := strings.CutPrefix(kv, "default-ttl="); ok {
			ttl, found = v, true
			break
		}
	}
	if !found {
		return fmt.Errorf("aerostore: namespace %q config has no default-ttl", namespace)
	}
	if ttl != "0" {
		return fmt.Errorf("aerostore: REFUSING TO START: namespace %q has default-ttl=%s (must be 0 so UTXO rows never silently expire)", namespace, ttl)
	}
	return nil
}

// resolveDurableDeletes decides whether deletes carry a tombstone. Explicit
// configuration wins; otherwise durable deletes are enabled only on Enterprise
// Edition (Community rejects them with ENTERPRISE_ONLY).
func (s *Store) resolveDurableDeletes(ctx context.Context, forced *bool) {
	if forced != nil {
		s.durableDeletes = *forced
		if *forced {
			return
		}
		s.logger.Warn("durable deletes explicitly disabled; deleted UTXO rows may reappear after a cold restart")
		return
	}
	enterprise := false
	if info, ierr := s.requestInfo(ctx, "edition"); ierr == nil {
		enterprise = strings.Contains(strings.ToLower(info["edition"]), "enterprise")
	}
	s.durableDeletes = enterprise
	if !enterprise {
		s.logger.Warn("Aerospike Community Edition detected: durable deletes unavailable; deleted UTXO rows are NOT tombstoned and could reappear after a cold restart. Use Enterprise Edition in production.")
	}
}

// indexSpec describes one secondary index the store maintains.
type indexSpec struct {
	bin       string
	indexType as.IndexType
}

func (s *Store) indexSpecs() []indexSpec {
	return []indexSpec{
		{binClaimKey, as.STRING}, // claim probes
		{binInvKey, as.STRING},   // balance / inventory scans
		{binResBy, as.STRING},    // release-by-reservation
		{binResAt, as.NUMERIC},   // stale-reservation sweep (naturally partial)
	}
}

// ensureIndexes creates each secondary index idempotently and waits for it to
// build (the teranode index-task-wait pattern). An already-existing index
// (INDEX_FOUND) is a success.
func (s *Store) ensureIndexes(ctx context.Context, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for _, spec := range s.indexSpecs() {
		name := "idx_" + s.set + "_" + spec.bin
		task, err := s.client.CreateIndex(nil, s.namespace, s.set, name, spec.bin, spec.indexType)
		if err != nil {
			if err.Matches(types.INDEX_FOUND) {
				continue // already exists
			}
			return fmt.Errorf("aerostore: create index %s on %s: %w", name, spec.bin, err)
		}
		if err := waitForIndex(ctx, task, deadline); err != nil {
			return fmt.Errorf("aerostore: build index %s on %s: %w", name, spec.bin, err)
		}
	}
	return nil
}

func waitForIndex(ctx context.Context, task *as.IndexTask, deadline time.Time) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		done, err := task.IsDone()
		if err != nil {
			return err
		}
		if done {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out waiting for index build")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *Store) firstNode() (*as.Node, error) {
	nodes := s.client.GetNodes()
	if len(nodes) == 0 {
		return nil, errors.New("aerostore: no active cluster nodes")
	}
	return nodes[0], nil
}

// infoRetryBudget bounds how long the startup/liveness info probes retry a
// freshly-connected cluster that is briefly not yet accepting info requests.
const infoRetryBudget = 5 * time.Second

// requestInfo issues an info command against the cluster, retrying transient
// "not ready yet" conditions (a fresh client can momentarily report no node /
// SERVER_NOT_AVAILABLE right after connect) until infoRetryBudget elapses.
func (s *Store) requestInfo(ctx context.Context, cmd string) (map[string]string, error) {
	deadline := time.Now().Add(infoRetryBudget)
	for {
		if cerr := ctx.Err(); cerr != nil {
			return nil, cerr
		}
		node, err := s.firstNode()
		if err == nil {
			info, ierr := node.RequestInfo(as.NewInfoPolicy(), cmd)
			if ierr == nil {
				return info, nil
			}
			if !ierr.Matches(types.SERVER_NOT_AVAILABLE, types.INVALID_NODE_ERROR, types.TIMEOUT, types.NO_AVAILABLE_CONNECTIONS_TO_NODE) {
				return nil, ierr
			}
			err = ierr
		}
		if time.Now().After(deadline) {
			return nil, err
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// Health implements [utxostore.Store].
func (s *Store) Health(ctx context.Context, checkLiveness bool) (int, string, error) {
	if s.closed.Load() {
		return http.StatusServiceUnavailable, "closed", errClosed
	}
	if !s.client.IsConnected() {
		return http.StatusServiceUnavailable, "not connected", errors.New("aerostore: client not connected")
	}
	if checkLiveness {
		if _, ierr := s.requestInfo(ctx, "namespace/"+s.namespace); ierr != nil {
			return http.StatusServiceUnavailable, "backend unreachable", fmt.Errorf("aerostore: liveness probe: %w", ierr)
		}
	}
	return http.StatusOK, "OK", nil
}

// Close implements [utxostore.Store]. Idempotent; all other methods error after.
func (s *Store) Close(_ context.Context) error {
	s.closed.Store(true)
	s.closeOnce.Do(func() { s.client.Close() })
	return nil
}

// nowMillis returns the store clock in Unix milliseconds (the resAt/createdAt
// wire representation).
func (s *Store) nowMillis() int64 { return s.now().UnixMilli() }

// msToTime converts a Unix-millisecond timestamp back to a time.Time.
func msToTime(ms int64) time.Time { return time.UnixMilli(ms) }

// keyFor derives the record key for an outpoint.
func (s *Store) keyFor(op utxostore.Outpoint) (*as.Key, error) {
	k, err := as.NewKey(s.namespace, s.set, outpointKey(op))
	if err != nil {
		return nil, fmt.Errorf("aerostore: build key for %s: %w", op, err)
	}
	return k, nil
}

// deleteWritePolicy returns a write policy for deletes, honoring the resolved
// durable-delete setting.
func (s *Store) deleteWritePolicy() *as.WritePolicy {
	wp := as.NewWritePolicy(0, 0)
	wp.DurableDelete = s.durableDeletes
	return wp
}

// recordToUTXO reconstructs a UTXO from a stored record's bins.
func recordToUTXO(rec *as.Record) (*utxostore.UTXO, error) {
	if rec == nil {
		return nil, errors.New("aerostore: nil record")
	}
	b := rec.Bins

	txidBytes, ok := b[binTxID].([]byte)
	if !ok || len(txidBytes) != 32 {
		return nil, fmt.Errorf("aerostore: record missing/invalid %s bin", binTxID)
	}
	var txid [32]byte
	copy(txid[:], txidBytes)

	vout, ok := asInt64(b[binVout])
	if !ok {
		return nil, fmt.Errorf("aerostore: record missing %s bin", binVout)
	}
	userID, _ := asInt64(b[binUserID])
	sats, _ := asInt64(b[binSats])
	inpSize, _ := asInt64(b[binInpSize])
	tier, _ := asInt64(b[binTier])
	createdAt, _ := asInt64(b[binCreated])

	// These bins are written by this package from wallet-domain values that
	// never exceed their target widths (vout/inputSize fit uint32, satoshis are
	// well under 2^63, tier is 1..3), so the conversions back cannot overflow.
	u := &utxostore.UTXO{
		Outpoint:  utxostore.Outpoint{TxID: txid, Vout: uint32(vout)}, //nolint:gosec // vout fits uint32 by construction
		UserID:    userID,
		Basket:    asString(b[binBasket]),
		Satoshis:  uint64(sats),         //nolint:gosec // satoshis are non-negative and < 2^63
		InputSize: uint32(inpSize),      //nolint:gosec // input size fits uint32 by construction
		Tier:      utxostore.Tier(tier), //nolint:gosec // tier is a small enum (1..3)
		CreatedAt: time.UnixMilli(createdAt),
	}
	if resBy := asString(b[binResBy]); resBy != "" {
		u.ReservedBy = resBy
		if at, ok := asInt64(b[binResAt]); ok {
			u.ReservedAt = time.UnixMilli(at)
		}
	}
	if sb, ok := b[binSpentBy].([]byte); ok && len(sb) == 32 {
		var hash chainhash.Hash
		copy(hash[:], sb)
		u.SpentBy = &hash
	}
	if f, ok := asInt64(b[binFrozen]); ok && f != 0 {
		u.Frozen = true
	}
	return u, nil
}

// asInt64 coerces an Aerospike integer bin (int on 64-bit, int64 elsewhere) to
// int64. Returns false for a nil or non-integer value.
func asInt64(v any) (int64, bool) {
	switch n := v.(type) {
	case int:
		return int64(n), true
	case int64:
		return n, true
	case uint64:
		return int64(n), true //nolint:gosec // stored integers originate as int64/int and never exceed 2^63
	case float64:
		return int64(n), true
	default:
		return 0, false
	}
}

func asString(v any) string {
	s, _ := v.(string)
	return s
}

// --- shared op helpers ------------------------------------------------------

// getRecord fetches the record for op. found is false (with nil error) when the
// outpoint is absent.
func (s *Store) getRecord(op utxostore.Outpoint) (rec *as.Record, found bool, err error) {
	key, kerr := s.keyFor(op)
	if kerr != nil {
		return nil, false, kerr
	}
	r, aerr := s.client.Get(nil, key)
	if aerr != nil {
		if aerr.Matches(types.KEY_NOT_FOUND_ERROR) {
			return nil, false, nil
		}
		return nil, false, fmt.Errorf("aerostore: get %s: %w", op, aerr)
	}
	return r, true, nil
}

// noteClaimable tells the claim cache (when enabled) that a coin has been made
// claimable in the (userID, basket, tier, value-bucket) it names, so a bucket
// last seen empty is re-probed on the next claim. A no-op when the cache is
// disabled. Callers invoke it AFTER the backing write commits so the coin is
// already visible to a probe.
func (s *Store) noteClaimable(userID int64, basket string, tier utxostore.Tier, sats uint64) {
	if s.claimCache == nil {
		return
	}
	s.claimCache.markClaimable(claimKeyFor(userID, basket, tier, bucketOf(sats)))
}

// restoreClaimKeyOp returns an operation that RESTORES the claimKey bin (making
// the row claimable again), deriving the key's embedded tier from the record's
// OWN live tier bin, but ONLY when skipWhen evaluates false — all evaluated
// atomically server-side. This closes the stale-tier window: a concurrent
// Promote that rewrote the tier bin between the caller's snapshot read and this
// write is reflected in the restored key, so the coin can never become
// claimable under a stale tier's secondary-index bucket.
//
// aerospike-client-go/v8 has no server-side string concatenation, but tier is a
// tiny enum (1..3), so the client precomputes the three candidate claimKey
// strings and the SERVER selects the one whose embedded tier equals the live
// tier bin (an ExpCond ladder). The expression reads only bins this Operate
// does NOT modify (tier, plus skipWhen's bins), so it always observes their
// live committed values regardless of intra-Operate operation ordering.
//
// skipWhen names the states in which the coin must NOT be resurrected as
// claimable (frozen, for the release/unspend paths; reserved or spent, for the
// unfreeze path); when it holds, ExpUnknown + EvalNoFail skips the write and the
// bin stays absent. An unexpected tier (impossible — tiers are validated on
// mint/promote) likewise falls through to a skip rather than writing a bogus
// key.
func restoreClaimKeyOp(skipWhen *as.Expression, u *utxostore.UTXO) *as.Operation {
	bucket := bucketOf(u.Satoshis)
	// ExpCond form: test1, action1, test2, action2, …, defaultAction. Skip
	// (write nothing) when skipWhen holds; otherwise select the claimKey string
	// whose embedded tier matches the record's LIVE tier bin.
	pairs := []*as.Expression{skipWhen, as.ExpUnknown()}
	for _, t := range []utxostore.Tier{utxostore.TierSending, utxostore.TierUnproven, utxostore.TierMined} {
		pairs = append(pairs,
			as.ExpEq(as.ExpIntBin(binTier), as.ExpIntVal(int64(t))),
			as.ExpStringVal(claimKeyFor(u.UserID, u.Basket, t, bucket)),
		)
	}
	pairs = append(pairs, as.ExpUnknown()) // unexpected tier → skip
	return as.ExpWriteOp(binClaimKey, as.ExpCond(pairs...), as.ExpWriteFlagEvalNoFail)
}

// fireRestoreRaceHook invokes the test-only restore-race seam if one is set.
// See the restoreRaceHook field.
func (s *Store) fireRestoreRaceHook() {
	if s.restoreRaceHook != nil {
		s.restoreRaceHook()
	}
}

// removeBinOp returns an operation that removes a bin (a Put of a nil value).
func removeBinOp(name string) *as.Operation {
	return as.PutOp(as.NewBin(name, nil))
}

// joinBatch wraps per-item typed errors under the ErrBatch sentinel, or returns
// nil when there are none (for Remove/Freeze/Unfreeze, whose ops carry no Err
// slot). errors.Is finds ErrBatch; errors.As finds the item errors.
func joinBatch(itemErrs []error) error {
	if len(itemErrs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", utxostore.ErrBatch, errors.Join(itemErrs...))
}

// batchCountErr returns the top-level ErrBatch sentinel (with a count) when any
// item in a Mint/Spend batch failed, or nil.
func batchCountErr(failed, total int) error {
	if failed == 0 {
		return nil
	}
	return fmt.Errorf("%w: %d of %d items failed", utxostore.ErrBatch, failed, total)
}
