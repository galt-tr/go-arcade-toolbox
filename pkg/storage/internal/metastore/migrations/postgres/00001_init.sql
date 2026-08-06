-- +goose Up

-- +goose StatementBegin
CREATE TABLE users (
    user_id        BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    identity_key   VARCHAR(66) NOT NULL UNIQUE,
    active_storage TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE output_baskets (
    user_id                    BIGINT      NOT NULL REFERENCES users(user_id),
    name                       TEXT        NOT NULL,
    number_of_desired_utxos    BIGINT      NOT NULL DEFAULT 0,
    minimum_desired_utxo_value BIGINT      NOT NULL DEFAULT 0,
    created_at                 TIMESTAMPTZ NOT NULL,
    updated_at                 TIMESTAMPTZ NOT NULL,
    deleted_at                 TIMESTAMPTZ NULL,
    PRIMARY KEY (user_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE transactions (
    transaction_id BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id        BIGINT      NOT NULL REFERENCES users(user_id),
    proven_tx_id   BIGINT      NULL,
    status         TEXT        NOT NULL,
    reference      TEXT        NOT NULL UNIQUE,
    txid           BYTEA       NULL,
    is_outgoing    BOOLEAN     NOT NULL DEFAULT FALSE,
    satoshis       BIGINT      NOT NULL DEFAULT 0,
    description    TEXT        NOT NULL DEFAULT '',
    version        BIGINT      NULL,
    lock_time      BIGINT      NULL,
    input_beef     BYTEA       NULL,
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL
);
-- +goose StatementEnd

-- ListActions/ListTransactions scans by owner filtered on status, newest first.
CREATE INDEX idx_transactions_user_status ON transactions (user_id, status, created_at);
-- FindByTxID lookups over the (usually small) set of rows that carry a txid.
CREATE INDEX idx_transactions_txid ON transactions (txid) WHERE txid IS NOT NULL;
-- The abandoned sweep scans only the two rebuild-eligible statuses by age.
CREATE INDEX idx_transactions_abandoned ON transactions (created_at)
    WHERE status IN ('unsigned', 'nosend');

-- +goose StatementBegin
CREATE TABLE labels (
    label_id   BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users(user_id),
    name       TEXT        NOT NULL,
    is_deleted BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE transaction_labels (
    transaction_id BIGINT      NOT NULL REFERENCES transactions(transaction_id),
    label_id       BIGINT      NOT NULL REFERENCES labels(label_id),
    is_deleted     BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (transaction_id, label_id)
);
-- +goose StatementEnd

-- The reverse edge: resolve a label to its transactions (the ListActions join).
CREATE INDEX idx_transaction_labels_label ON transaction_labels (label_id, transaction_id);

-- +goose StatementBegin
CREATE TABLE outputs (
    output_id           BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             BIGINT      NOT NULL REFERENCES users(user_id),
    transaction_id      BIGINT      NOT NULL REFERENCES transactions(transaction_id),
    vout                INTEGER     NOT NULL,
    satoshis            BIGINT      NOT NULL DEFAULT 0,
    locking_script      BYTEA       NULL,
    basket              TEXT        NULL,
    spent_by            BIGINT      NULL REFERENCES transactions(transaction_id),
    change              BOOLEAN     NOT NULL DEFAULT FALSE,
    output_type         TEXT        NOT NULL DEFAULT '',
    provided_by         TEXT        NOT NULL DEFAULT '',
    purpose             TEXT        NOT NULL DEFAULT '',
    description         TEXT        NOT NULL DEFAULT '',
    derivation_prefix   TEXT        NULL,
    derivation_suffix   TEXT        NULL,
    sender_identity_key TEXT        NULL,
    custom_instructions TEXT        NULL,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    UNIQUE (transaction_id, vout)
);
-- +goose StatementEnd

-- ListOutputs scans a user's outputs within a basket.
CREATE INDEX idx_outputs_user_basket ON outputs (user_id, basket);
-- The spend cascade resolves a spending transaction back to its consumed outputs.
CREATE INDEX idx_outputs_spent_by ON outputs (spent_by) WHERE spent_by IS NOT NULL;

-- +goose StatementBegin
CREATE TABLE tags (
    tag_id     BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id    BIGINT      NOT NULL REFERENCES users(user_id),
    name       TEXT        NOT NULL,
    is_deleted BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE output_tags (
    output_id  BIGINT      NOT NULL REFERENCES outputs(output_id),
    tag_id     BIGINT      NOT NULL REFERENCES tags(tag_id),
    is_deleted BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (output_id, tag_id)
);
-- +goose StatementEnd

CREATE INDEX idx_output_tags_tag ON output_tags (tag_id, output_id);

-- +goose StatementBegin
CREATE TABLE known_txs (
    txid                 BYTEA       PRIMARY KEY,
    status               TEXT        NOT NULL,
    arcade_status        TEXT        NULL,
    attempts             BIGINT      NOT NULL DEFAULT 0,
    rebroadcast_attempts BIGINT      NOT NULL DEFAULT 0,
    was_broadcast        BOOLEAN     NOT NULL DEFAULT FALSE,
    notified             BOOLEAN     NOT NULL DEFAULT FALSE,
    batch                TEXT        NULL,
    notify               TEXT        NOT NULL DEFAULT '{}',
    raw_tx               BYTEA       NULL,
    input_beef           BYTEA       NULL,
    block_height         BIGINT      NULL,
    block_hash           BYTEA       NULL,
    merkle_path          BYTEA       NULL,
    merkle_root          BYTEA       NULL,
    competing_txs        JSONB       NULL,
    suspect_since        TIMESTAMPTZ NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL
);
-- +goose StatementEnd

-- The monitor scans known txs by status, oldest update first.
CREATE INDEX idx_known_txs_status ON known_txs (status, updated_at);
-- Batch broadcasting resolves a batch back to its member txs.
CREATE INDEX idx_known_txs_batch ON known_txs (batch) WHERE batch IS NOT NULL;

-- +goose StatementBegin
CREATE TABLE certificates (
    certificate_id      BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             BIGINT      NOT NULL REFERENCES users(user_id),
    type                TEXT        NOT NULL,
    serial_number       TEXT        NOT NULL,
    certifier           TEXT        NOT NULL,
    subject             TEXT        NOT NULL,
    verifier            TEXT        NULL,
    revocation_outpoint TEXT        NOT NULL DEFAULT '',
    signature           TEXT        NOT NULL DEFAULT '',
    is_deleted          BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at          TIMESTAMPTZ NOT NULL,
    updated_at          TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, type, serial_number, certifier)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE certificate_fields (
    certificate_id BIGINT      NOT NULL REFERENCES certificates(certificate_id),
    user_id        BIGINT      NOT NULL REFERENCES users(user_id),
    field_name     TEXT        NOT NULL,
    field_value    TEXT        NOT NULL,
    master_key     TEXT        NOT NULL DEFAULT '',
    created_at     TIMESTAMPTZ NOT NULL,
    updated_at     TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (certificate_id, field_name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE sync_states (
    sync_state_id        BIGINT      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id              BIGINT      NOT NULL REFERENCES users(user_id),
    storage_identity_key TEXT        NOT NULL,
    storage_name         TEXT        NOT NULL DEFAULT '',
    status               TEXT        NOT NULL,
    init                 BOOLEAN     NOT NULL DEFAULT FALSE,
    ref_num              TEXT        NOT NULL UNIQUE,
    sync_map             JSONB       NOT NULL DEFAULT '{}',
    when_ts              TIMESTAMPTZ NULL,
    satoshis             BIGINT      NULL,
    error_local          TEXT        NULL,
    error_other          TEXT        NULL,
    created_at           TIMESTAMPTZ NOT NULL,
    updated_at           TIMESTAMPTZ NOT NULL,
    UNIQUE (user_id, storage_identity_key)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE key_values (
    key        TEXT        PRIMARY KEY,
    value      BYTEA       NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE utxo_ops_outbox (
    txid       BYTEA       NOT NULL,
    op_type    TEXT        NOT NULL,
    chunk      INTEGER     NOT NULL DEFAULT 0,
    payload    JSONB       NOT NULL,
    attempts   INTEGER     NOT NULL DEFAULT 0,
    last_error TEXT        NULL,
    created_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (txid, op_type, chunk)
);
-- +goose StatementEnd

-- FetchPending drains the not-yet-exhausted rows oldest first.
CREATE INDEX idx_utxo_ops_outbox_pending ON utxo_ops_outbox (created_at) WHERE attempts < 10;

-- +goose Down
DROP TABLE utxo_ops_outbox;
DROP TABLE key_values;
DROP TABLE sync_states;
DROP TABLE certificate_fields;
DROP TABLE certificates;
DROP TABLE known_txs;
DROP TABLE output_tags;
DROP TABLE tags;
DROP TABLE outputs;
DROP TABLE transaction_labels;
DROP TABLE labels;
DROP TABLE transactions;
DROP TABLE output_baskets;
DROP TABLE users;
