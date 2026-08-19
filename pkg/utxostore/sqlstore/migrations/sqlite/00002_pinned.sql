-- +goose Up
-- See the postgres migration for the pinned-state rationale. SQLite has no
-- boolean type, so the flag is an INTEGER 0/1 like frozen.
ALTER TABLE utxos ADD COLUMN pinned INTEGER NOT NULL DEFAULT 0;

-- The stale-scan index excludes pinned rows, so they never enter it and its
-- predicate stays in step with the sweep's own. "NOT pinned" over an INTEGER
-- 0/1 evaluates correctly in SQLite (NOT 0 = 1, NOT 1 = 0), exactly as "NOT
-- frozen" already does in Balance, so ONE predicate text serves both engines.
-- SQLite matches a partial index by comparing predicate text, so this clause is
-- issued from a single constant shared with the queries (notPinned in
-- sqlstore.go) and TestStaleScanIsIndexDriven proves the two still match.
DROP INDEX idx_utxos_reserved_at;
CREATE INDEX idx_utxos_reserved_at ON utxos (reserved_at)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL AND NOT pinned;

-- +goose Down
DROP INDEX idx_utxos_reserved_at;
CREATE INDEX idx_utxos_reserved_at ON utxos (reserved_at)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL;
ALTER TABLE utxos DROP COLUMN pinned;
