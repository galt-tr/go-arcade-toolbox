-- +goose Up
-- Mirrors postgres 00008: the partial index the mined-repair backfill's
-- written-off-transaction subquery seeks on
-- (KnownTxRepo.FindMinedOnDeadTransactions), so a recurring healer does not
-- sequentially scan the transactions table on every tick. See the postgres file
-- for why the index is tiny, why it indexes txid rather than status, and why
-- its write cost is negligible.
--
-- SQLite supports the same partial-index form, and 00001 already uses it here
-- (idx_transactions_abandoned, on the other two rebuild-eligible statuses).
CREATE INDEX idx_transactions_dead_txid ON transactions (txid)
    WHERE txid IS NOT NULL AND status IN ('aborted', 'failed');

-- +goose Down
DROP INDEX IF EXISTS idx_transactions_dead_txid;
