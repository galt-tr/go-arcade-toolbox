-- +goose Up
-- Mirrors postgres 00006: index the column CountByArcadeStatus groups by, so
-- the deployment-wide observability rollup stops scanning the whole of
-- known_txs — the widest table in the schema, carrying raw_tx / input_beef /
-- BUMP blobs.
--
-- SQLite has no index-only-scan planner note to point at, but the shape of the
-- win is the same: a covering index on the single grouped column lets the
-- rollup read the index instead of every row. See the postgres file for the
-- measured before/after (557ms -> 134ms, 23x less buffer traffic on a 1.99M-row
-- table) and for why indexing this hot column is an accepted trade.
CREATE INDEX idx_known_txs_arcade_status ON known_txs (arcade_status);

-- +goose Down
DROP INDEX IF EXISTS idx_known_txs_arcade_status;
