-- +goose Up
-- last_polled_at is the POLL-ATTEMPT clock, deliberately distinct from
-- updated_at (the state-change clock).
--
-- WHY a second clock: the poll fallbacks selected their work list with
-- `... ORDER BY updated_at ASC` and only ever advanced updated_at when a row's
-- state actually CHANGED. A row the poll could not apply (arcade returns a
-- proof our header view cannot verify yet, a GetTx transport failure, a batch
-- apply that errors) therefore kept the exact same updated_at, was re-selected
-- at the head of the very next tick, and pinned the ASC-ordered queue forever —
-- every row behind it was never SELECTed at all. That is how a 30-minute
-- 1000-TPS run left 23,745 transactions with an empty arcade_status and a
-- FROZEN count while arcade held all of them as MINED.
--
-- Stamping last_polled_at on every poll ATTEMPT (applied or not) and ordering
-- by it makes the sweep a strict round-robin: a row can never be re-selected
-- ahead of a row that has waited longer, so the poll always makes progress past
-- whatever it cannot apply. Defaulting to the epoch means "never polled", which
-- sorts first — a brand new backlog drains oldest-attempt-first.
--
-- Reusing updated_at for this would have been wrong: FindResendable's
-- 'unprocessed' grace window and the sync-staleness cutoff both read updated_at
-- as "when the row last CHANGED", and bumping it on a mere poll attempt would
-- silently defer re-broadcasts.
ALTER TABLE known_txs ADD COLUMN last_polled_at TIMESTAMPTZ NOT NULL DEFAULT TIMESTAMPTZ 'epoch';

-- The poll work-list scan: status filter, least-recently-attempted first.
CREATE INDEX idx_known_txs_status_polled ON known_txs (status, last_polled_at);

-- The REPAIR scan: rows the wallet knows about that never received ANY arcade
-- status. These are the permanently-diverged rows (arcade is the source of
-- truth and has a status for all of them), so they get their own partial index
-- and their own reachable-regardless-of-ordering query rather than depending on
-- where they happen to sort inside the general poll list.
CREATE INDEX idx_known_txs_no_arcade_status ON known_txs (status, last_polled_at)
    WHERE arcade_status IS NULL OR arcade_status = '';

-- +goose Down
DROP INDEX IF EXISTS idx_known_txs_no_arcade_status;
DROP INDEX IF EXISTS idx_known_txs_status_polled;
ALTER TABLE known_txs DROP COLUMN last_polled_at;
