-- +goose Up
-- +goose StatementBegin
CREATE TABLE utxos (
    txid        BLOB    NOT NULL,
    vout        INTEGER NOT NULL,
    user_id     INTEGER NOT NULL,
    basket      TEXT    NOT NULL,
    tier        INTEGER NOT NULL,
    satoshis    INTEGER NOT NULL CHECK (satoshis > 0),
    input_size  INTEGER NOT NULL DEFAULT 107,
    seq         INTEGER NOT NULL,
    reserved_by TEXT       NULL,
    reserved_at INTEGER    NULL,
    spent_by    BLOB       NULL,
    frozen      INTEGER NOT NULL DEFAULT 0,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (txid, vout)
);
-- +goose StatementEnd

-- See the postgres migration for the partial-index rationale. SQLite (>=3.35,
-- bundled by modernc) supports partial indexes and RETURNING. Timestamps are
-- stored as INTEGER microseconds since the Unix epoch (see the sqlstore
-- package doc, "Design notes"), so reserved_at ordering is a plain integer
-- comparison over this index.
CREATE INDEX idx_utxos_claim ON utxos (user_id, basket, tier, satoshis, seq)
    WHERE reserved_by IS NULL AND spent_by IS NULL AND frozen = 0;

CREATE INDEX idx_utxos_reserved ON utxos (reserved_by, user_id)
    WHERE reserved_by IS NOT NULL;

CREATE INDEX idx_utxos_reserved_at ON utxos (reserved_at)
    WHERE reserved_by IS NOT NULL AND spent_by IS NULL;

CREATE INDEX idx_utxos_spent_by ON utxos (spent_by)
    WHERE spent_by IS NOT NULL;

-- Monotonic, gap-tolerant sequence source for the seq column. Postgres uses a
-- GENERATED IDENTITY column; SQLite has no per-column identity separate from
-- the (composite) primary key, so a single-row counter supplies seq. It is
-- bumped once per mint attempt under the single writer connection; a wasted
-- bump on an idempotent no-op mint is fine because seq only needs to be
-- monotonic and gap-tolerant, never contiguous.
CREATE TABLE utxo_seq (
    id  INTEGER PRIMARY KEY CHECK (id = 0),
    val INTEGER NOT NULL
);
INSERT INTO utxo_seq (id, val) VALUES (0, 0);

-- +goose Down
DROP TABLE utxos;
DROP TABLE utxo_seq;
