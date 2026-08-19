// Package memstore is the in-memory reference implementation of
// [utxostore.Store]: a map guarded by a single mutex, written for correctness
// and readability over speed. It exists to prove the interface is
// implementable, to anchor the utxostoretest conformance suite, and to serve
// as the fake for funder unit tests. It is fully thread-safe; every method is
// a single critical section, which trivially satisfies the per-item atomicity
// and single-round-trip claim contracts.
package memstore

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/bsv-blockchain/go-sdk/chainhash"

	"github.com/galt-tr/go-arcade-toolbox/pkg/utxostore"
)

// errClosed is returned by every method after Close.
var errClosed = errors.New("memstore: store is closed")

// row is a stored coin plus its insertion sequence number, which provides the
// deterministic tiebreak for claim selection.
type row struct {
	utxo utxostore.UTXO
	seq  uint64
}

// Store is an in-memory [utxostore.Store]. Create it with [New].
type Store struct {
	mu      sync.Mutex
	rows    map[utxostore.Outpoint]*row
	nextSeq uint64
	closed  bool
	now     func() time.Time
}

// compile-time interface check.
var _ utxostore.Store = (*Store)(nil)

// Option configures a Store.
type Option func(*Store)

// WithClock overrides the store's clock (used for ReservedAt and CreatedAt).
// Intended for tests that need deterministic timestamps.
func WithClock(now func() time.Time) Option {
	return func(s *Store) { s.now = now }
}

// New creates an empty in-memory store.
func New(opts ...Option) *Store {
	s := &Store{
		rows: make(map[utxostore.Outpoint]*row),
		now:  time.Now,
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// Health implements [utxostore.Store].
func (s *Store) Health(_ context.Context, _ bool) (int, string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return http.StatusServiceUnavailable, "closed", errClosed
	}
	return http.StatusOK, "OK", nil
}

// Mint implements [utxostore.Store]. Same-identity replays are no-op
// successes; identity conflicts fail per item with
// [utxostore.AlreadyExistsError].
func (s *Store) Mint(_ context.Context, mints []*utxostore.Mint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}

	failed := 0
	for _, m := range mints {
		m.Err = s.mintOne(m)
		if m.Err != nil {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%w: %d of %d items failed", utxostore.ErrBatch, failed, len(mints))
	}
	return nil
}

// mintOne creates a single coin; the caller holds the mutex.
func (s *Store) mintOne(m *utxostore.Mint) error {
	switch {
	case m.UserID <= 0:
		return fmt.Errorf("memstore: mint %s: user id must be positive", m.Outpoint)
	case m.Basket == "":
		return fmt.Errorf("memstore: mint %s: basket must be non-empty", m.Outpoint)
	case !m.Tier.Valid():
		return fmt.Errorf("memstore: mint %s: invalid tier %d", m.Outpoint, m.Tier)
	}

	if existing, ok := s.rows[m.Outpoint]; ok {
		// Idempotency compares immutable identity only; tier and lifecycle
		// state may legitimately have moved on since the original mint.
		u := existing.utxo
		if u.UserID == m.UserID && u.Basket == m.Basket &&
			u.Satoshis == m.Satoshis && u.InputSize == m.InputSize {
			return nil
		}
		return &utxostore.AlreadyExistsError{Op: m.Outpoint}
	}

	s.nextSeq++
	s.rows[m.Outpoint] = &row{
		seq: s.nextSeq,
		utxo: utxostore.UTXO{
			Outpoint:  m.Outpoint,
			UserID:    m.UserID,
			Basket:    m.Basket,
			Satoshis:  m.Satoshis,
			InputSize: m.InputSize,
			Tier:      m.Tier,
			CreatedAt: s.now(),
		},
	}
	return nil
}

// Get implements [utxostore.Store].
func (s *Store) Get(_ context.Context, op utxostore.Outpoint) (*utxostore.UTXO, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}

	r, ok := s.rows[op]
	if !ok {
		return nil, &utxostore.NotFoundError{Op: op}
	}
	return copyUTXO(&r.utxo), nil
}

// Remove implements [utxostore.Store]. Missing rows are no-ops; without
// force, reserved/spent/frozen rows are refused per item.
func (s *Store) Remove(_ context.Context, ops []utxostore.Outpoint, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}

	var itemErrs []error
	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok {
			continue // missing = no-op
		}
		if !force {
			switch {
			case r.utxo.SpentBy != nil:
				itemErrs = append(itemErrs, &utxostore.SpentError{Op: op, Winner: *r.utxo.SpentBy})
				continue
			case r.utxo.ReservedBy != "":
				itemErrs = append(itemErrs, &utxostore.ReservedError{Op: op, HeldBy: r.utxo.ReservedBy})
				continue
			case r.utxo.Frozen:
				itemErrs = append(itemErrs, &utxostore.FrozenError{Op: op})
				continue
			}
		}
		delete(s.rows, op)
	}
	return joinBatch(itemErrs)
}

// claimable reports whether the row can be handed to a claim in scope sc;
// the caller holds the mutex.
func claimable(r *row, sc utxostore.Scope) bool {
	u := &r.utxo
	return u.UserID == sc.UserID && u.Basket == sc.Basket && u.Tier == sc.Tier &&
		u.ReservedBy == "" && u.SpentBy == nil && !u.Frozen
}

// validateClaim rejects underspecified claim inputs; see the interface doc.
func validateClaim(sc utxostore.Scope, reservation string) error {
	switch {
	case reservation == "":
		return errors.New("memstore: reservation must be non-empty")
	case sc.UserID <= 0:
		return errors.New("memstore: scope user id must be positive")
	case sc.Basket == "":
		return errors.New("memstore: scope basket must be non-empty")
	case !sc.Tier.Valid():
		return fmt.Errorf("memstore: invalid scope tier %d", sc.Tier)
	}
	return nil
}

// reserve marks r as held by reservation and returns a copy; the caller
// holds the mutex.
func (s *Store) reserve(r *row, reservation string) *utxostore.UTXO {
	r.utxo.ReservedBy = reservation
	r.utxo.ReservedAt = s.now()
	return copyUTXO(&r.utxo)
}

// ClaimSmallestSufficient implements [utxostore.Store] with true best-fit
// semantics: the global minimum Satoshis >= minSats over claimable rows in
// scope, ties broken by insertion order.
func (s *Store) ClaimSmallestSufficient(_ context.Context, sc utxostore.Scope, reservation string, minSats uint64) (*utxostore.UTXO, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}

	var best *row
	for _, r := range s.rows {
		if !claimable(r, sc) || r.utxo.Satoshis < minSats {
			continue
		}
		if best == nil ||
			r.utxo.Satoshis < best.utxo.Satoshis ||
			(r.utxo.Satoshis == best.utxo.Satoshis && r.seq < best.seq) {
			best = r
		}
	}
	if best == nil {
		return nil, nil
	}
	return s.reserve(best, reservation), nil
}

// ClaimLargestInsufficient implements [utxostore.Store]: up to limit
// claimable coins with Satoshis < capSats, largest first, ties broken by
// insertion order.
func (s *Store) ClaimLargestInsufficient(_ context.Context, sc utxostore.Scope, reservation string, capSats uint64, limit int) ([]*utxostore.UTXO, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}
	if limit <= 0 {
		return nil, nil
	}

	var candidates []*row
	for _, r := range s.rows {
		if claimable(r, sc) && r.utxo.Satoshis < capSats {
			candidates = append(candidates, r)
		}
	}
	slices.SortFunc(candidates, func(a, b *row) int {
		if c := cmp.Compare(b.utxo.Satoshis, a.utxo.Satoshis); c != 0 {
			return c // largest first
		}
		return cmp.Compare(a.seq, b.seq)
	})
	candidates = candidates[:min(limit, len(candidates))]

	claimed := make([]*utxostore.UTXO, 0, len(candidates))
	for _, r := range candidates {
		claimed = append(claimed, s.reserve(r, reservation))
	}
	return claimed, nil
}

// ClaimExact implements [utxostore.Store]: up to count claimable coins with
// Satoshis == denomination, in insertion order. Fewer than count is pool
// underflow, not an error.
func (s *Store) ClaimExact(_ context.Context, sc utxostore.Scope, reservation string, denomination uint64, count int) ([]*utxostore.UTXO, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}
	if err := validateClaim(sc, reservation); err != nil {
		return nil, err
	}
	if count <= 0 {
		return nil, nil
	}

	var candidates []*row
	for _, r := range s.rows {
		if claimable(r, sc) && r.utxo.Satoshis == denomination {
			candidates = append(candidates, r)
		}
	}
	slices.SortFunc(candidates, func(a, b *row) int { return cmp.Compare(a.seq, b.seq) })
	candidates = candidates[:min(count, len(candidates))]

	claimed := make([]*utxostore.UTXO, 0, len(candidates))
	for _, r := range candidates {
		claimed = append(claimed, s.reserve(r, reservation))
	}
	return claimed, nil
}

// heldBy reports whether the row belongs to the live (unspent) membership of
// (userID, reservation): the set the janitor release, Pin, and Unpin all act on.
func heldBy(r *row, userID int64, reservation string) bool {
	return r.utxo.UserID == userID && r.utxo.ReservedBy == reservation && r.utxo.SpentBy == nil
}

// ReleaseReservation implements [utxostore.Store]: frees every unspent,
// UNPINNED row held by (userID, reservation). Idempotent; never touches spent
// rows, and never frees the inputs of an in-flight send (see [Store.Pin]).
func (s *Store) ReleaseReservation(_ context.Context, userID int64, reservation string) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errClosed
	}
	if reservation == "" {
		return 0, errors.New("memstore: reservation must be non-empty")
	}

	released := 0
	for _, r := range s.rows {
		if heldBy(r, userID, reservation) && !r.utxo.Pinned {
			r.utxo.ReservedBy = ""
			r.utxo.ReservedAt = time.Time{}
			released++
		}
	}
	return released, nil
}

// Pin implements [utxostore.Store]: marks every unspent row held by (userID,
// reservation) as pinned, returning how many it NEWLY pinned. Idempotent.
func (s *Store) Pin(_ context.Context, userID int64, reservation string) (int, error) {
	return s.setPinned(userID, reservation, true)
}

// Unpin implements [utxostore.Store]: clears the pin on every unspent row held
// by (userID, reservation), leaving them reserved. Idempotent.
func (s *Store) Unpin(_ context.Context, userID int64, reservation string) (int, error) {
	return s.setPinned(userID, reservation, false)
}

// setPinned drives Pin/Unpin: it flips the pin on the reservation's live
// membership and counts only the rows that actually changed.
func (s *Store) setPinned(userID int64, reservation string, pinned bool) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errClosed
	}
	if reservation == "" {
		return 0, errors.New("memstore: reservation must be non-empty")
	}

	changed := 0
	for _, r := range s.rows {
		if !heldBy(r, userID, reservation) || r.utxo.Pinned == pinned {
			continue
		}
		r.utxo.Pinned = pinned
		changed++
	}
	return changed, nil
}

// ReleaseOutpoints implements [utxostore.Store]: guard mismatches, spent rows
// and missing outpoints are per-item skips, not errors. A matching token
// overrides the pin — this is the verified-dead release path.
func (s *Store) ReleaseOutpoints(_ context.Context, reservation string, ops []utxostore.Outpoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}
	if reservation == "" {
		return errors.New("memstore: reservation must be non-empty")
	}

	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok || r.utxo.ReservedBy != reservation || r.utxo.SpentBy != nil {
			continue // skip: stale or foreign release intent
		}
		r.utxo.ReservedBy = ""
		r.utxo.ReservedAt = time.Time{}
		r.utxo.Pinned = false
	}
	return nil
}

// Spend implements [utxostore.Store]: reserved(reservation) → spent(txid),
// idempotent for the same spender, [utxostore.SpentError] for a different one.
// With force it records a spend the network has already accepted, skipping the
// reservation and freeze guards (see [utxostore.Store.Spend]).
func (s *Store) Spend(_ context.Context, spends []*utxostore.SpendOp, force bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}

	failed := 0
	for _, sp := range spends {
		sp.Err = s.spendOne(sp, force)
		if sp.Err != nil {
			failed++
		}
	}
	if failed > 0 {
		return fmt.Errorf("%w: %d of %d items failed", utxostore.ErrBatch, failed, len(spends))
	}
	return nil
}

// spendOne applies a single spend transition; the caller holds the mutex.
func (s *Store) spendOne(sp *utxostore.SpendOp, force bool) error {
	if sp.Reservation == "" {
		return fmt.Errorf("memstore: spend %s: reservation must be non-empty", sp.Outpoint)
	}

	r, ok := s.rows[sp.Outpoint]
	if !ok {
		return &utxostore.NotFoundError{Op: sp.Outpoint}
	}
	// The spent_by arbiter runs in BOTH modes: two spend facts cannot both hold.
	if r.utxo.SpentBy != nil {
		if *r.utxo.SpentBy == sp.SpendingTxID {
			return nil // idempotent: same spender replay
		}
		return &utxostore.SpentError{Op: sp.Outpoint, Winner: *r.utxo.SpentBy}
	}
	// Fact mode skips exactly these two guards: the spend is already on the
	// network, so neither a freeze nor a stale reservation can un-happen it.
	if !force {
		if r.utxo.Frozen {
			return &utxostore.FrozenError{Op: sp.Outpoint}
		}
		if r.utxo.ReservedBy != sp.Reservation {
			return &utxostore.ReservedError{Op: sp.Outpoint, HeldBy: r.utxo.ReservedBy}
		}
	}

	spentBy := sp.SpendingTxID
	r.utxo.SpentBy = &spentBy
	r.utxo.Pinned = false // the send it protected has landed; the coin is consumed
	return nil
}

// Unspend implements [utxostore.Store]: exact-guard release back to the
// claimable pool; guard mismatches and missing outpoints are skips.
func (s *Store) Unspend(_ context.Context, spendingTxID chainhash.Hash, ops []utxostore.Outpoint) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errClosed
	}

	released := 0
	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok || r.utxo.SpentBy == nil || *r.utxo.SpentBy != spendingTxID {
			continue // skip: guard mismatch
		}
		r.utxo.SpentBy = nil
		r.utxo.ReservedBy = ""
		r.utxo.ReservedAt = time.Time{}
		r.utxo.Pinned = false
		released++
	}
	return released, nil
}

// RemoveSpentBy implements [utxostore.Store]: deletes every row spent by
// spendingTxID, a now-terminal (mined) tx whose inputs are permanently
// consumed. Idempotent; returns the number of rows removed.
func (s *Store) RemoveSpentBy(_ context.Context, spendingTxID chainhash.Hash) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errClosed
	}

	removed := 0
	for op, r := range s.rows {
		if r.utxo.SpentBy != nil && *r.utxo.SpentBy == spendingTxID {
			delete(s.rows, op)
			removed++
		}
	}
	return removed, nil
}

// Promote implements [utxostore.Store]: retiers rows in either direction;
// missing rows are skipped, unchanged rows do not count.
func (s *Store) Promote(_ context.Context, ops []utxostore.Outpoint, to utxostore.Tier) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return 0, errClosed
	}
	if !to.Valid() {
		return 0, fmt.Errorf("memstore: invalid target tier %d", to)
	}

	changed := 0
	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok || r.utxo.Tier == to {
			continue
		}
		r.utxo.Tier = to
		changed++
	}
	return changed, nil
}

// RemoveByMintTx implements [utxostore.Store]: removes phantom coins of an
// invalidated mint transaction and classifies the survivors.
func (s *Store) RemoveByMintTx(_ context.Context, mintTxID chainhash.Hash, ops []utxostore.Outpoint) (utxostore.RemoveByMintReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var report utxostore.RemoveByMintReport
	if s.closed {
		return report, errClosed
	}
	for _, op := range ops {
		if op.TxID != mintTxID {
			return report, fmt.Errorf("memstore: outpoint %s is not an output of mint tx %s", op, mintTxID.String())
		}
	}

	reservedRefs := make(map[string]*utxostore.ReservationRef) // by token
	var refOrder []string
	seenSpenders := make(map[chainhash.Hash]bool)

	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok {
			continue // already gone
		}
		switch {
		case r.utxo.SpentBy != nil:
			if w := *r.utxo.SpentBy; !seenSpenders[w] {
				seenSpenders[w] = true
				report.AlreadySpentBy = append(report.AlreadySpentBy, w)
			}
		case r.utxo.ReservedBy != "":
			ref, ok := reservedRefs[r.utxo.ReservedBy]
			if !ok {
				ref = &utxostore.ReservationRef{
					Reservation: r.utxo.ReservedBy,
					UserID:      r.utxo.UserID,
					ReservedAt:  r.utxo.ReservedAt,
				}
				reservedRefs[r.utxo.ReservedBy] = ref
				refOrder = append(refOrder, r.utxo.ReservedBy)
			}
			if r.utxo.ReservedAt.Before(ref.ReservedAt) {
				ref.ReservedAt = r.utxo.ReservedAt
			}
			ref.Outpoints = append(ref.Outpoints, op)
		default:
			// Unreserved and unspent: remove, even when frozen — a frozen
			// phantom coin is strictly worse than no row.
			delete(s.rows, op)
			report.Removed = append(report.Removed, op)
		}
	}

	for _, token := range refOrder {
		report.AlreadyReserved = append(report.AlreadyReserved, *reservedRefs[token])
	}
	return report, nil
}

// Freeze implements [utxostore.Store]: missing outpoints are per-item
// [utxostore.NotFoundError]s under [utxostore.ErrBatch]; present rows are
// still frozen.
func (s *Store) Freeze(_ context.Context, ops []utxostore.Outpoint) error {
	return s.setFrozen(ops, true)
}

// Unfreeze implements [utxostore.Store]; the mirror of Freeze.
func (s *Store) Unfreeze(_ context.Context, ops []utxostore.Outpoint) error {
	return s.setFrozen(ops, false)
}

func (s *Store) setFrozen(ops []utxostore.Outpoint, frozen bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errClosed
	}

	var itemErrs []error
	for _, op := range ops {
		r, ok := s.rows[op]
		if !ok {
			itemErrs = append(itemErrs, &utxostore.NotFoundError{Op: op})
			continue
		}
		r.utxo.Frozen = frozen
	}
	return joinBatch(itemErrs)
}

// Balance implements [utxostore.Store].
func (s *Store) Balance(_ context.Context, userID int64, basket string) (utxostore.Balance, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	b := utxostore.Balance{
		Claimable:      make(map[utxostore.Tier]uint64),
		ClaimableCount: make(map[utxostore.Tier]int),
	}
	if s.closed {
		return b, errClosed
	}

	for _, r := range s.rows {
		u := &r.utxo
		if u.UserID != userID || u.Basket != basket || u.SpentBy != nil {
			continue
		}
		switch {
		case u.ReservedBy != "":
			b.Reserved += u.Satoshis
			b.ReservedCount++
		case !u.Frozen:
			b.Claimable[u.Tier] += u.Satoshis
			b.ClaimableCount[u.Tier]++
		}
	}
	return b, nil
}

// FindStaleReservations implements [utxostore.Store]: reservations of
// unspent, unpinned rows whose oldest ReservedAt is before olderThan, oldest
// first. Pinned rows are excluded entirely — the reaper must never see the
// inputs of an in-flight send.
func (s *Store) FindStaleReservations(_ context.Context, olderThan time.Time, limit int) ([]utxostore.ReservationRef, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errClosed
	}
	if limit <= 0 {
		return nil, nil
	}

	type group struct {
		ref    utxostore.ReservationRef
		minSeq uint64
	}
	type key struct {
		userID int64
		token  string
	}
	groups := make(map[key]*group)

	for _, r := range s.rows {
		u := &r.utxo
		if u.ReservedBy == "" || u.SpentBy != nil || u.Pinned {
			continue // pinned rows are in flight, never stale work
		}
		k := key{userID: u.UserID, token: u.ReservedBy}
		g, ok := groups[k]
		if !ok {
			g = &group{
				ref: utxostore.ReservationRef{
					Reservation: u.ReservedBy,
					UserID:      u.UserID,
					ReservedAt:  u.ReservedAt,
				},
				minSeq: r.seq,
			}
			groups[k] = g
		}
		if u.ReservedAt.Before(g.ref.ReservedAt) {
			g.ref.ReservedAt = u.ReservedAt
		}
		if r.seq < g.minSeq {
			g.minSeq = r.seq
		}
		g.ref.Outpoints = append(g.ref.Outpoints, u.Outpoint)
	}

	stale := make([]*group, 0, len(groups))
	for _, g := range groups {
		if g.ref.ReservedAt.Before(olderThan) {
			stale = append(stale, g)
		}
	}
	slices.SortFunc(stale, func(a, b *group) int {
		if c := a.ref.ReservedAt.Compare(b.ref.ReservedAt); c != 0 {
			return c // oldest first
		}
		return cmp.Compare(a.minSeq, b.minSeq)
	})
	stale = stale[:min(limit, len(stale))]

	refs := make([]utxostore.ReservationRef, 0, len(stale))
	for _, g := range stale {
		refs = append(refs, g.ref)
	}
	return refs, nil
}

// Close implements [utxostore.Store]. Idempotent; all other methods error
// afterwards.
func (s *Store) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

// copyUTXO returns a deep copy so callers can never mutate stored state.
func copyUTXO(u *utxostore.UTXO) *utxostore.UTXO {
	c := *u
	if u.SpentBy != nil {
		spentBy := *u.SpentBy
		c.SpentBy = &spentBy
	}
	return &c
}

// joinBatch wraps per-item errors under the ErrBatch sentinel, or returns
// nil when there are none. errors.Is finds ErrBatch; errors.As finds the
// item errors.
func joinBatch(itemErrs []error) error {
	if len(itemErrs) == 0 {
		return nil
	}
	return fmt.Errorf("%w: %w", utxostore.ErrBatch, errors.Join(itemErrs...))
}
