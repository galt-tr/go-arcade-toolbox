-- +goose Up
-- +goose StatementBegin
CREATE TABLE utxos (
    txid        BYTEA    NOT NULL,
    vout        INTEGER  NOT NULL,
    user_id     BIGINT   NOT NULL,
    basket      TEXT     NOT NULL,
    tier        SMALLINT NOT NULL,
    satoshis    BIGINT   NOT NULL CHECK (satoshis > 0),
    input_size  INTEGER  NOT NULL DEFAULT 107,
    seq         BIGINT   GENERATED ALWAYS AS IDENTITY,
    reserved_by TEXT        NULL,
    reserved_at TIMESTAMPTZ NULL,
    spent_by    BYTEA       NULL,
    frozen      BOOLEAN  NOT NULL DEFAULT FALSE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (txid, vout)
);
-- +goose StatementEnd

-- The claim hot path: partial index over the claimable pool only. reserved_by
-- must NOT be embedded in a full composite index (the planner then sequential-
-- scans a large equal-value pool); the WHERE-clause partial index keeps every
-- claim a pure index walk. The column order (user_id, basket, tier, satoshis,
-- seq) matches the claim query's equality predicates + ORDER BY satoshis, seq
-- so no separate Sort node is needed.
CREATE INDEX idx_utxos_claim ON utxos (user_id, basket, tier, satoshis, seq)
    WHERE reserved_by IS NULL AND spent_by IS NULL AND NOT frozen;

-- ReleaseReservation / FindStaleReservations lookups by holder.
CREATE INDEX idx_utxos_reserved ON utxos (reserved_by, user_id)
    WHERE reserved_by IS NOT NULL;

-- FindStaleReservations scans unspent reserved rows ordered by reserved_at.
CREATE INDEX idx_utxos_reserved_at ON utxos (reserved_at)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL;

-- Unspend / RemoveByMintTx lookups by spender.
CREATE INDEX idx_utxos_spent_by ON utxos (spent_by)
    WHERE spent_by IS NOT NULL;

-- +goose Down
DROP TABLE utxos;
