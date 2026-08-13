package metastore

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk"
	"github.com/galt-tr/go-arcade-toolbox/pkg/wdk/primitives"
)

// SyncStateRepo persists per-(user, storage) synchronization state, including
// the opaque sync_map JSON round-tripped unchanged.
//
// TableSyncState.SyncMap MUST be valid JSON: the sync_map column is JSONB on
// PostgreSQL and TEXT on SQLite, so a non-JSON value passes silently on SQLite
// but is rejected at write time on PostgreSQL. Insert/Update default an empty
// SyncMap to "{}".
type SyncStateRepo struct{ s *Store }

// SyncState returns the sync-state repository.
func (s *Store) SyncState() SyncStateRepo { return SyncStateRepo{s} }

const syncStateCols = "sync_state_id, user_id, storage_identity_key, storage_name, status, " +
	"init, ref_num, sync_map, when_ts, satoshis, error_local, error_other, created_at, updated_at"

func (r SyncStateRepo) scan(sc rowScanner) (*wdk.TableSyncState, error) {
	var (
		ss         wdk.TableSyncState
		id         int64
		userID     int64
		status     string
		initFlag   boolScan
		syncMap    []byte
		whenTS     tsScan
		satoshis   sql.NullInt64
		errorLocal sql.NullString
		errorOther sql.NullString
		createdAt  tsScan
		updatedAt  tsScan
	)
	dest := []any{
		&id, &userID, &ss.StorageIdentityKey, &ss.StorageName, &status,
		r.s.boolDest(&initFlag), &ss.RefNum, &syncMap, r.s.tsDest(&whenTS), &satoshis,
		&errorLocal, &errorOther, r.s.tsDest(&createdAt), r.s.tsDest(&updatedAt),
	}
	if err := sc.Scan(dest...); err != nil {
		return nil, err
	}

	ss.SyncStateID = uint(id) //nolint:gosec // DB identity id, always positive.
	ss.UserID = int(userID)
	ss.Status = wdk.SyncStatus(status)
	ss.Init = r.s.boolGet(initFlag)
	ss.SyncMap = string(syncMap)
	ss.When = r.s.tsTimePtr(whenTS)
	if satoshis.Valid {
		v := primitives.SatoshiValue(satoshis.Int64) //nolint:gosec // satoshi value, non-negative.
		ss.Satoshis = &v
	}
	ss.ErrorLocal = nullStr(errorLocal)
	ss.ErrorOther = nullStr(errorOther)
	ss.CreatedAt = r.s.tsTime(createdAt)
	ss.UpdatedAt = r.s.tsTime(updatedAt)
	return &ss, nil
}

// FindByUserAndStorage returns the sync state for (userID, storageIdentityKey),
// or found=false when absent.
func (r SyncStateRepo) FindByUserAndStorage(ctx context.Context, userID int, storageIdentityKey string) (*wdk.TableSyncState, bool, error) {
	q := r.s.rebind("SELECT " + syncStateCols + " FROM sync_states WHERE user_id = ? AND storage_identity_key = ?")
	ss, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, userID, storageIdentityKey))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find sync state: %w", err)
	}
	return ss, true, nil
}

// FindByRefNum returns the sync state with ref_num, or found=false when absent.
func (r SyncStateRepo) FindByRefNum(ctx context.Context, refNum string) (*wdk.TableSyncState, bool, error) {
	q := r.s.rebind("SELECT " + syncStateCols + " FROM sync_states WHERE ref_num = ?")
	ss, err := r.scan(r.s.execer(ctx).QueryRowContext(ctx, q, refNum))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, false, nil
	}
	if err != nil {
		return nil, false, fmt.Errorf("metastore: find sync state by ref: %w", err)
	}
	return ss, true, nil
}

// Insert creates a sync-state row and returns its generated id. sync_map is
// stored verbatim (defaulting to "{}").
func (r SyncStateRepo) Insert(ctx context.Context, ss wdk.TableSyncState) (uint, error) {
	now := r.s.encTime(r.s.now())
	syncMap := ss.SyncMap
	if syncMap == "" {
		syncMap = "{}"
	}
	var satoshis any
	if ss.Satoshis != nil {
		satoshis = int64(*ss.Satoshis) //nolint:gosec // satoshi value, well within int64.
	}
	q := r.s.rebind(
		`INSERT INTO sync_states
		 (user_id, storage_identity_key, storage_name, status, init, ref_num, sync_map,
		  when_ts, satoshis, error_local, error_other, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		 RETURNING sync_state_id`)
	var id int64
	if err := r.s.execer(ctx).QueryRowContext(ctx, q,
		ss.UserID, ss.StorageIdentityKey, ss.StorageName, string(ss.Status), r.s.boolVal(ss.Init),
		ss.RefNum, syncMap, r.s.encTimePtr(ss.When), satoshis,
		strPtrArg(ss.ErrorLocal), strPtrArg(ss.ErrorOther), now, now,
	).Scan(&id); err != nil {
		return 0, fmt.Errorf("metastore: insert sync state: %w", err)
	}
	return uint(id), nil //nolint:gosec // DB identity id, always positive.
}

// Update rewrites the mutable fields of an existing sync-state row (identified
// by SyncStateID): status, init, sync_map, when, satoshis and the error slots.
func (r SyncStateRepo) Update(ctx context.Context, ss wdk.TableSyncState) error {
	syncMap := ss.SyncMap
	if syncMap == "" {
		syncMap = "{}"
	}
	var satoshis any
	if ss.Satoshis != nil {
		satoshis = int64(*ss.Satoshis) //nolint:gosec // satoshi value, well within int64.
	}
	q := r.s.rebind(
		`UPDATE sync_states
		 SET status = ?, init = ?, sync_map = ?, when_ts = ?, satoshis = ?,
		     error_local = ?, error_other = ?, updated_at = ?
		 WHERE sync_state_id = ?`)
	res, err := r.s.execer(ctx).ExecContext(ctx, q,
		string(ss.Status), r.s.boolVal(ss.Init), syncMap, r.s.encTimePtr(ss.When), satoshis,
		strPtrArg(ss.ErrorLocal), strPtrArg(ss.ErrorOther), r.s.encTime(r.s.now()), ss.SyncStateID)
	if err != nil {
		return fmt.Errorf("metastore: update sync state: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("metastore: update sync state: id %d: %w", ss.SyncStateID, ErrNotFound)
	}
	return nil
}
