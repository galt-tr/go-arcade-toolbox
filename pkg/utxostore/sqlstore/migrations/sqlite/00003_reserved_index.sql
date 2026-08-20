-- +goose Up
-- See the postgres migration for the two defects and the covering design.
-- SQLite has no INCLUDE, so the covering columns are ordinary TRAILING KEY
-- columns instead. That is equivalent for every use here — the leading
-- (reserved_by, user_id) prefix still drives every seek, and the extra columns
-- only ride along — and costs a little index size for their sort keys.
--
-- spent_by is a SIXTH column here even though PostgreSQL needs it in neither
-- the key nor the INCLUDE list, and that asymmetry is the whole point: both
-- engines use spent_by only as the partial predicate, but only PostgreSQL
-- DISCHARGES it. Given "WHERE ... AND spent_by IS NULL", PostgreSQL knows the
-- index's own WHERE already proves the term and drops it, leaving an index-only
-- scan; SQLite matches the partial index but still re-evaluates the term, and
-- with no spent_by in the index it must read the table row to do it — which
-- costs the covering property for the sake of a condition that cannot be false.
-- Carrying the column (always NULL for every row in this index, so nearly free)
-- turns the stale scan into "SEARCH utxos USING COVERING INDEX
-- idx_utxos_reserved"; TestStaleScanIsIndexDriven asserts exactly that.
--
-- The predicate is spelled with the same "NOT pinned"-free terms on both
-- engines because SQLite matches a partial index by comparing predicate text
-- against the query's WHERE: "spent_by IS NULL" appears verbatim in
-- ReleaseReservation, Pin/Unpin and the stale aggregate, and
-- "reserved_by IS NOT NULL" is either verbatim (the aggregate) or implied by
-- "reserved_by = ?" (the rest), which SQLite proves for itself.
DROP INDEX idx_utxos_reserved;
CREATE INDEX idx_utxos_reserved ON utxos (reserved_by, user_id, reserved_at, seq, pinned, spent_by)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL;

-- +goose Down
-- Restore 00001's form exactly. This ordering is load-bearing on SQLite, which
-- refuses to drop a column an index still references: 00003's Down must retire
-- the pinned reference here BEFORE 00002's Down drops the pinned column, the
-- same constraint 00002's own Down already satisfies for idx_utxos_reserved_at
-- (TestMigrationsRollBack walks the whole chain down and back up).
DROP INDEX idx_utxos_reserved;
CREATE INDEX idx_utxos_reserved ON utxos (reserved_by, user_id)
    WHERE reserved_by IS NOT NULL;
