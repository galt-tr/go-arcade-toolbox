-- +goose Up
-- See the postgres 00003 migration for the rationale. Stored as Unix
-- microseconds (INTEGER), matching every other SQLite timestamp column.
ALTER TABLE known_txs ADD COLUMN verified_rejected_at INTEGER NULL;

-- +goose Down
ALTER TABLE known_txs DROP COLUMN verified_rejected_at;
