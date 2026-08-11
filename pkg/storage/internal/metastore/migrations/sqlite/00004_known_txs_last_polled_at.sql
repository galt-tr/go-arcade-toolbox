-- +goose Up
-- See the postgres 00004 migration for the rationale (the poll-attempt clock
-- that stops a row the poll cannot apply from pinning the head of the
-- ASC-ordered work list forever). Stored as Unix MICROSECONDS (INTEGER),
-- matching every other SQLite timestamp column; 0 is the epoch = never polled,
-- which sorts first exactly like the postgres TIMESTAMPTZ 'epoch' default.
ALTER TABLE known_txs ADD COLUMN last_polled_at INTEGER NOT NULL DEFAULT 0;

CREATE INDEX idx_known_txs_status_polled ON known_txs (status, last_polled_at);
CREATE INDEX idx_known_txs_no_arcade_status ON known_txs (status, last_polled_at)
    WHERE arcade_status IS NULL OR arcade_status = '';

-- +goose Down
DROP INDEX IF EXISTS idx_known_txs_no_arcade_status;
DROP INDEX IF EXISTS idx_known_txs_status_polled;
ALTER TABLE known_txs DROP COLUMN last_polled_at;
