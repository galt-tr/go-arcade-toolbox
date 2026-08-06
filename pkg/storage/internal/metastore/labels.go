package metastore

import (
	"context"
	"fmt"
)

// LabelsRepo persists transaction labels and their M2M edges to transactions.
type LabelsRepo struct{ s *Store }

// Labels returns the labels repository.
func (s *Store) Labels() LabelsRepo { return LabelsRepo{s} }

// FindOrCreate upserts the label (userID, name) and returns its id, clearing
// any prior soft-delete. Idempotent.
func (r LabelsRepo) FindOrCreate(ctx context.Context, userID int, name string) (int64, error) {
	return r.s.upsertNamed(ctx, "labels", "label_id", userID, name)
}

// Attach ensures each label exists and links it to the transaction. Idempotent.
func (r LabelsRepo) Attach(ctx context.Context, userID int, transactionID uint, labels ...string) error {
	return r.s.inTx(ctx, func(ctx context.Context) error {
		for _, name := range labels {
			if err := r.attach(ctx, userID, transactionID, name); err != nil {
				return err
			}
		}
		return nil
	})
}

// attach links one label (creating it if needed) to a transaction. It assumes
// an ambient transaction (Insert / Attach provide one).
func (r LabelsRepo) attach(ctx context.Context, userID int, transactionID uint, name string) error {
	labelID, err := r.FindOrCreate(ctx, userID, name)
	if err != nil {
		return err
	}
	now := r.s.encTime(r.s.now())
	q := r.s.rebind(
		`INSERT INTO transaction_labels (transaction_id, label_id, is_deleted, created_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (transaction_id, label_id) DO UPDATE SET is_deleted = ?`)
	_, err = r.s.execer(ctx).ExecContext(ctx, q,
		transactionID, labelID, r.s.boolVal(false), now, r.s.boolVal(false))
	if err != nil {
		return fmt.Errorf("metastore: link label: %w", err)
	}
	return nil
}

// TagsRepo persists output tags and their M2M edges to outputs.
type TagsRepo struct{ s *Store }

// Tags returns the tags repository.
func (s *Store) Tags() TagsRepo { return TagsRepo{s} }

// FindOrCreate upserts the tag (userID, name) and returns its id, clearing any
// prior soft-delete. Idempotent.
func (r TagsRepo) FindOrCreate(ctx context.Context, userID int, name string) (int64, error) {
	return r.s.upsertNamed(ctx, "tags", "tag_id", userID, name)
}

// Attach ensures each tag exists and links it to the output. Idempotent.
func (r TagsRepo) Attach(ctx context.Context, userID int, outputID uint, tags ...string) error {
	return r.s.inTx(ctx, func(ctx context.Context) error {
		for _, name := range tags {
			if err := r.attach(ctx, userID, outputID, name); err != nil {
				return err
			}
		}
		return nil
	})
}

// attach links one tag (creating it if needed) to an output. It assumes an
// ambient transaction.
func (r TagsRepo) attach(ctx context.Context, userID int, outputID uint, name string) error {
	tagID, err := r.FindOrCreate(ctx, userID, name)
	if err != nil {
		return err
	}
	now := r.s.encTime(r.s.now())
	q := r.s.rebind(
		`INSERT INTO output_tags (output_id, tag_id, is_deleted, created_at)
		 VALUES (?, ?, ?, ?)
		 ON CONFLICT (output_id, tag_id) DO UPDATE SET is_deleted = ?`)
	_, err = r.s.execer(ctx).ExecContext(ctx, q,
		outputID, tagID, r.s.boolVal(false), now, r.s.boolVal(false))
	if err != nil {
		return fmt.Errorf("metastore: link tag: %w", err)
	}
	return nil
}

// upsertNamed inserts (or un-deletes) a (user_id, name) row in a labels/tags
// table and returns its identity id. The ON CONFLICT DO UPDATE form returns the
// id whether the row was freshly inserted or already existed.
func (s *Store) upsertNamed(ctx context.Context, table, idCol string, userID int, name string) (int64, error) {
	now := s.encTime(s.now())
	q := s.rebind(fmt.Sprintf(
		`INSERT INTO %s (user_id, name, is_deleted, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)
		 ON CONFLICT (user_id, name) DO UPDATE SET is_deleted = ?, updated_at = ?
		 RETURNING %s`, table, idCol))
	var id int64
	if err := s.execer(ctx).QueryRowContext(ctx, q,
		userID, name, s.boolVal(false), now, now, s.boolVal(false), now).Scan(&id); err != nil {
		return 0, fmt.Errorf("metastore: upsert %s: %w", table, err)
	}
	return id, nil
}
