-- +goose Up
-- See the postgres 00007 migration for the rationale. TEXT in both engines: the
-- reason is arcade's own prose, stored verbatim (bounded by the writer, not by
-- the column).
ALTER TABLE known_txs ADD COLUMN reject_reason TEXT NULL;

-- +goose Down
ALTER TABLE known_txs DROP COLUMN reject_reason;
