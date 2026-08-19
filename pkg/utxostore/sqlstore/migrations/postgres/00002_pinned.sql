-- +goose Up
-- The committed pre-broadcast "pinned" state: set in the same transaction that
-- stores a signed tx's raw bytes, cleared only by the tx's own lifecycle
-- (spend / abort / reconciler release). Invariant: pinned => reserved & unspent.
-- idx_utxos_claim is deliberately untouched: pinned rows are reserved, so the
-- claim predicate (reserved_by IS NULL ...) already excludes them.
ALTER TABLE utxos ADD COLUMN pinned BOOLEAN NOT NULL DEFAULT FALSE;

-- The sweep must never see pinned rows: fold the pin into the stale-scan
-- index's predicate, so pinned rows never enter the index and its WHERE stays
-- in step with the sweep's own predicate (the store issues both from one
-- constant, notPinned).
DROP INDEX idx_utxos_reserved_at;
CREATE INDEX idx_utxos_reserved_at ON utxos (reserved_at)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL AND NOT pinned;

-- +goose Down
DROP INDEX idx_utxos_reserved_at;
CREATE INDEX idx_utxos_reserved_at ON utxos (reserved_at)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL;
ALTER TABLE utxos DROP COLUMN pinned;
