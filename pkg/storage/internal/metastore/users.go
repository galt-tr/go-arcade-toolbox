package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
)

// UsersRepo persists wallet users (identity → user_id + active storage).
type UsersRepo struct{ s *Store }

// Users returns the users repository.
func (s *Store) Users() UsersRepo { return UsersRepo{s} }

const userCols = "user_id, identity_key, active_storage, created_at, updated_at"

func (r UsersRepo) scan(sc rowScanner) (*wdk.TableUser, error) {
	var (
		u         wdk.TableUser
		uid       int64
		createdAt tsScan
		updatedAt tsScan
	)
	if err := sc.Scan(&uid, &u.IdentityKey, &u.ActiveStorage, r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt)); err != nil {
		return nil, err
	}
	u.UserID = int(uid)
	u.CreatedAt = r.s.tsTime(createdAt)
	u.UpdatedAt = r.s.tsTime(updatedAt)
	return &u, nil
}

// FindByIdentityKey returns the user for identityKey, or found=false when
// absent (never an error for a missing row).
func (r UsersRepo) FindByIdentityKey(ctx context.Context, identityKey string) (*wdk.TableUser, bool, error) {
	q := r.s.rebind("SELECT " + userCols + " FROM users WHERE identity_key = ?")
	u, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, identityKey))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find user: %w", err)
	}
	return u, true, nil
}

// FindByID returns the user for userID, or found=false when absent.
func (r UsersRepo) FindByID(ctx context.Context, userID int) (*wdk.TableUser, bool, error) {
	q := r.s.rebind("SELECT " + userCols + " FROM users WHERE user_id = ?")
	u, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, userID))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find user by id: %w", err)
	}
	return u, true, nil
}

// FindOrInsertUser returns the user for identityKey, inserting a fresh row when
// none exists. wasNew reports whether this call created the row. It is
// idempotent and race-safe: the insert is ON CONFLICT DO NOTHING, so a
// concurrent caller that lost the race falls through to a re-read and is
// reported wasNew=false (mirroring the old provider's user.ID==0 signal).
func (r UsersRepo) FindOrInsertUser(ctx context.Context, identityKey string) (user *wdk.TableUser, wasNew bool, err error) {
	err = r.s.inTx(ctx, func(ctx context.Context) error {
		existing, found, ferr := r.FindByIdentityKey(ctx, identityKey)
		if ferr != nil {
			return ferr
		}
		if found {
			user, wasNew = existing, false
			return nil
		}

		now := r.s.encTime(r.s.now())
		q := r.s.rebind(
			`INSERT INTO users (identity_key, active_storage, created_at, updated_at)
			 VALUES (?, '', ?, ?)
			 ON CONFLICT (identity_key) DO NOTHING
			 RETURNING ` + userCols)
		inserted, serr := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, identityKey, now, now))
		if serr == nil {
			user, wasNew = inserted, true
			return nil
		}
		if !errors.Is(serr, sql.ErrNoRows) {
			return fmt.Errorf("metastore: insert user: %w", serr)
		}

		// Lost the ON CONFLICT race: the row now exists, re-read it.
		reread, found2, rerr := r.FindByIdentityKey(ctx, identityKey)
		if rerr != nil {
			return rerr
		}
		if !found2 {
			return errors.New("metastore: user vanished after conflict")
		}
		user, wasNew = reread, false
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	return user, wasNew, nil
}

// SetActiveStorage updates a user's active-storage identity key.
func (r UsersRepo) SetActiveStorage(ctx context.Context, userID int, activeStorage string) error {
	q := r.s.rebind("UPDATE users SET active_storage = ?, updated_at = ? WHERE user_id = ?")
	res, err := r.s.execer(ctx).ExecContext(ctx, q, activeStorage, r.s.encTime(r.s.now()), userID)
	if err != nil {
		return fmt.Errorf("metastore: set active storage: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: set active storage: user %d: %w", userID, ErrNotFound)
	}
	return nil
}
