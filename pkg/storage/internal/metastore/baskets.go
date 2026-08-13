package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// BasketsRepo persists output baskets, keyed by (user_id, name).
//
// The schema has no numeric basket id (the PK is (user_id, name)), so the
// returned [wdk.TableOutputBasket.BasketID] is always zero — that field is a
// legacy API-interop artifact, not a database key. Outputs reference their
// basket by name (the outputs.basket text column).
type BasketsRepo struct{ s *Store }

// Baskets returns the output-baskets repository.
func (s *Store) Baskets() BasketsRepo { return BasketsRepo{s} }

const basketCols = "user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at, deleted_at"

func (r BasketsRepo) scan(sc rowScanner) (*wdk.TableOutputBasket, error) {
	var (
		b         wdk.TableOutputBasket
		uid       int64
		name      string
		nDesired  int64
		minValue  int64
		createdAt tsScan
		updatedAt tsScan
		deletedAt tsScan
	)
	if err := sc.Scan(&uid, &name, &nDesired, &minValue,
		r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt), r.s.tsDest(&deletedAt)); err != nil {
		return nil, err
	}
	b.UserID = int(uid)
	b.Name = primitives.StringUnder300(name)
	b.NumberOfDesiredUTXOs = nDesired
	//nolint:gosec // stored satoshi-scale value, round-trips within int64/uint64.
	b.MinimumDesiredUTXOValue = uint64(minValue)
	b.CreatedAt = r.s.tsTime(createdAt)
	b.UpdatedAt = r.s.tsTime(updatedAt)
	b.IsDeleted = r.s.tsTimePtr(deletedAt) != nil
	return &b, nil
}

// FindByName returns the basket named name for userID, or found=false when
// absent.
func (r BasketsRepo) FindByName(ctx context.Context, userID int, name string) (*wdk.TableOutputBasket, bool, error) {
	q := r.s.rebind("SELECT " + basketCols + " FROM output_baskets WHERE user_id = ? AND name = ?")
	b, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, userID, name))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find basket: %w", err)
	}
	return b, true, nil
}

// FindOrCreate inserts an empty (0/0) basket named name for userID if it does
// not already exist. Idempotent (ON CONFLICT DO NOTHING).
func (r BasketsRepo) FindOrCreate(ctx context.Context, userID int, name string) error {
	return r.Upsert(ctx, userID, wdk.BasketConfiguration{Name: primitives.StringUnder300(name)}, false)
}

// Upsert inserts the basket configuration for userID, or (when overwrite is
// true) updates its desired-utxo knobs. Returns nil on success. Used to seed a
// new user's default/change baskets and to create baskets on demand.
func (r BasketsRepo) Upsert(ctx context.Context, userID int, cfg wdk.BasketConfiguration, overwrite bool) error {
	now := r.s.encTime(r.s.now())
	setClause := "name = output_baskets.name" // no-op keeps DO UPDATE valid
	if overwrite {
		setClause = "number_of_desired_utxos = EXCLUDED.number_of_desired_utxos, " +
			"minimum_desired_utxo_value = EXCLUDED.minimum_desired_utxo_value, updated_at = EXCLUDED.updated_at"
	}
	q := r.s.rebind(
		`INSERT INTO output_baskets
		 (user_id, name, number_of_desired_utxos, minimum_desired_utxo_value, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)
		 ON CONFLICT (user_id, name) DO UPDATE SET ` + setClause)
	//nolint:gosec // NumberOfDesiredUTXOs/MinimumDesiredUTXOValue are wallet config, well within int64.
	_, err := r.s.execer(ctx).ExecContext(ctx, q,
		userID, string(cfg.Name), cfg.NumberOfDesiredUTXOs, int64(cfg.MinimumDesiredUTXOValue), now, now)
	if err != nil {
		return fmt.Errorf("metastore: upsert basket: %w", err)
	}
	return nil
}

// Find returns the (non-deleted) baskets matching args for the given user,
// ordered by name. It backs wdk FindOutputBasketsAuth.
func (r BasketsRepo) Find(ctx context.Context, args wdk.FindOutputBasketsArgs) ([]wdk.TableOutputBasket, error) {
	var (
		conds = []string{"deleted_at IS NULL"}
		vals  []any
	)
	if args.UserID != nil {
		conds = append(conds, "user_id = ?")
		vals = append(vals, *args.UserID)
	}
	if args.Name != nil {
		conds = append(conds, "name = ?")
		vals = append(vals, *args.Name)
	}
	if args.NumberOfDesiredUTXOs != nil {
		conds = append(conds, "number_of_desired_utxos = ?")
		vals = append(vals, *args.NumberOfDesiredUTXOs)
	}
	if args.MinimumDesiredUTXOValue != nil {
		conds = append(conds, "minimum_desired_utxo_value = ?")
		//nolint:gosec // satoshi-scale value, well within int64.
		vals = append(vals, int64(*args.MinimumDesiredUTXOValue))
	}

	q := r.s.rebind("SELECT " + basketCols + " FROM output_baskets WHERE " +
		joinAnd(conds) + " ORDER BY name ASC")
	rows, err := r.s.execer(ctx).QueryContext(ctx, q, vals...)
	if err != nil {
		return nil, fmt.Errorf("metastore: find baskets: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var out []wdk.TableOutputBasket
	for rows.Next() {
		b, err := r.scan(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *b)
	}
	return out, rows.Err()
}
