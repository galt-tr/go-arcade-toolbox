-- +goose Up
-- SQLite parity with the postgres set: BYTEA->BLOB, TIMESTAMPTZ->INTEGER
-- (Unix microseconds, set explicitly by the store clock), IDENTITY->INTEGER
-- PRIMARY KEY (rowid alias), JSONB->TEXT, BOOLEAN->INTEGER (0/1). Foreign keys
-- are enforced (the DSN sets _foreign_keys=on).

-- +goose StatementBegin
CREATE TABLE users (
    user_id        INTEGER PRIMARY KEY,
    identity_key   TEXT    NOT NULL UNIQUE,
    active_storage TEXT    NOT NULL DEFAULT '',
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE output_baskets (
    user_id                    INTEGER NOT NULL REFERENCES users(user_id),
    name                       TEXT    NOT NULL,
    number_of_desired_utxos    INTEGER NOT NULL DEFAULT 0,
    minimum_desired_utxo_value INTEGER NOT NULL DEFAULT 0,
    created_at                 INTEGER NOT NULL,
    updated_at                 INTEGER NOT NULL,
    deleted_at                 INTEGER NULL,
    PRIMARY KEY (user_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE transactions (
    transaction_id INTEGER PRIMARY KEY,
    user_id        INTEGER NOT NULL REFERENCES users(user_id),
    proven_tx_id   INTEGER NULL,
    status         TEXT    NOT NULL,
    reference      TEXT    NOT NULL UNIQUE,
    txid           BLOB    NULL,
    is_outgoing    INTEGER NOT NULL DEFAULT 0,
    satoshis       INTEGER NOT NULL DEFAULT 0,
    description    TEXT    NOT NULL DEFAULT '',
    version        INTEGER NULL,
    lock_time      INTEGER NULL,
    input_beef     BLOB    NULL,
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL
);
-- +goose StatementEnd

CREATE INDEX idx_transactions_user_status ON transactions (user_id, status, created_at);
CREATE INDEX idx_transactions_txid ON transactions (txid) WHERE txid IS NOT NULL;
CREATE INDEX idx_transactions_abandoned ON transactions (created_at)
    WHERE status IN ('unsigned', 'nosend');

-- +goose StatementBegin
CREATE TABLE labels (
    label_id   INTEGER PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(user_id),
    name       TEXT    NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (user_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE transaction_labels (
    transaction_id INTEGER NOT NULL REFERENCES transactions(transaction_id),
    label_id       INTEGER NOT NULL REFERENCES labels(label_id),
    is_deleted     INTEGER NOT NULL DEFAULT 0,
    created_at     INTEGER NOT NULL,
    PRIMARY KEY (transaction_id, label_id)
);
-- +goose StatementEnd

CREATE INDEX idx_transaction_labels_label ON transaction_labels (label_id, transaction_id);

-- +goose StatementBegin
CREATE TABLE outputs (
    output_id           INTEGER PRIMARY KEY,
    user_id             INTEGER NOT NULL REFERENCES users(user_id),
    transaction_id      INTEGER NOT NULL REFERENCES transactions(transaction_id),
    vout                INTEGER NOT NULL,
    satoshis            INTEGER NOT NULL DEFAULT 0,
    locking_script      BLOB    NULL,
    basket              TEXT    NULL,
    spent_by            INTEGER NULL REFERENCES transactions(transaction_id),
    change              INTEGER NOT NULL DEFAULT 0,
    output_type         TEXT    NOT NULL DEFAULT '',
    provided_by         TEXT    NOT NULL DEFAULT '',
    purpose             TEXT    NOT NULL DEFAULT '',
    description         TEXT    NOT NULL DEFAULT '',
    derivation_prefix   TEXT    NULL,
    derivation_suffix   TEXT    NULL,
    sender_identity_key TEXT    NULL,
    custom_instructions TEXT    NULL,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    UNIQUE (transaction_id, vout)
);
-- +goose StatementEnd

CREATE INDEX idx_outputs_user_basket ON outputs (user_id, basket);
CREATE INDEX idx_outputs_spent_by ON outputs (spent_by) WHERE spent_by IS NOT NULL;

-- +goose StatementBegin
CREATE TABLE tags (
    tag_id     INTEGER PRIMARY KEY,
    user_id    INTEGER NOT NULL REFERENCES users(user_id),
    name       TEXT    NOT NULL,
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    UNIQUE (user_id, name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE output_tags (
    output_id  INTEGER NOT NULL REFERENCES outputs(output_id),
    tag_id     INTEGER NOT NULL REFERENCES tags(tag_id),
    is_deleted INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (output_id, tag_id)
);
-- +goose StatementEnd

CREATE INDEX idx_output_tags_tag ON output_tags (tag_id, output_id);

-- +goose StatementBegin
CREATE TABLE known_txs (
    txid                 BLOB    PRIMARY KEY,
    status               TEXT    NOT NULL,
    arcade_status        TEXT    NULL,
    attempts             INTEGER NOT NULL DEFAULT 0,
    rebroadcast_attempts INTEGER NOT NULL DEFAULT 0,
    was_broadcast        INTEGER NOT NULL DEFAULT 0,
    notified             INTEGER NOT NULL DEFAULT 0,
    batch                TEXT    NULL,
    notify               TEXT    NOT NULL DEFAULT '{}',
    raw_tx               BLOB    NULL,
    input_beef           BLOB    NULL,
    block_height         INTEGER NULL,
    block_hash           BLOB    NULL,
    merkle_path          BLOB    NULL,
    merkle_root          BLOB    NULL,
    competing_txs        TEXT    NULL,
    suspect_since        INTEGER NULL,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL
);
-- +goose StatementEnd

CREATE INDEX idx_known_txs_status ON known_txs (status, updated_at);
CREATE INDEX idx_known_txs_batch ON known_txs (batch) WHERE batch IS NOT NULL;

-- +goose StatementBegin
CREATE TABLE certificates (
    certificate_id      INTEGER PRIMARY KEY,
    user_id             INTEGER NOT NULL REFERENCES users(user_id),
    type                TEXT    NOT NULL,
    serial_number       TEXT    NOT NULL,
    certifier           TEXT    NOT NULL,
    subject             TEXT    NOT NULL,
    verifier            TEXT    NULL,
    revocation_outpoint TEXT    NOT NULL DEFAULT '',
    signature           TEXT    NOT NULL DEFAULT '',
    is_deleted          INTEGER NOT NULL DEFAULT 0,
    created_at          INTEGER NOT NULL,
    updated_at          INTEGER NOT NULL,
    UNIQUE (user_id, type, serial_number, certifier)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE certificate_fields (
    certificate_id INTEGER NOT NULL REFERENCES certificates(certificate_id),
    user_id        INTEGER NOT NULL REFERENCES users(user_id),
    field_name     TEXT    NOT NULL,
    field_value    TEXT    NOT NULL,
    master_key     TEXT    NOT NULL DEFAULT '',
    created_at     INTEGER NOT NULL,
    updated_at     INTEGER NOT NULL,
    PRIMARY KEY (certificate_id, field_name)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE sync_states (
    sync_state_id        INTEGER PRIMARY KEY,
    user_id              INTEGER NOT NULL REFERENCES users(user_id),
    storage_identity_key TEXT    NOT NULL,
    storage_name         TEXT    NOT NULL DEFAULT '',
    status               TEXT    NOT NULL,
    init                 INTEGER NOT NULL DEFAULT 0,
    ref_num              TEXT    NOT NULL UNIQUE,
    sync_map             TEXT    NOT NULL DEFAULT '{}',
    when_ts              INTEGER NULL,
    satoshis             INTEGER NULL,
    error_local          TEXT    NULL,
    error_other          TEXT    NULL,
    created_at           INTEGER NOT NULL,
    updated_at           INTEGER NOT NULL,
    UNIQUE (user_id, storage_identity_key)
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE key_values (
    key        TEXT    PRIMARY KEY,
    value      BLOB    NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE utxo_ops_outbox (
    txid       BLOB    NOT NULL,
    op_type    TEXT    NOT NULL,
    chunk      INTEGER NOT NULL DEFAULT 0,
    payload    TEXT    NOT NULL,
    attempts   INTEGER NOT NULL DEFAULT 0,
    last_error TEXT    NULL,
    created_at INTEGER NOT NULL,
    PRIMARY KEY (txid, op_type, chunk)
);
-- +goose StatementEnd

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
