-- +goose Up
-- The monitor's distributed job lease. One row per gocron job name; ownership
-- is holding an unexpired lease (owner = this instance AND lease_until in the
-- future). lease_until is Unix NANOSECONDS (BIGINT) so the acquire/release CAS
-- is dialect-neutral (no TIMESTAMPTZ vs INTEGER divergence between engines).

-- +goose StatementBegin
CREATE TABLE monitor_job_locks (
    job_name    TEXT   PRIMARY KEY,
    owner       TEXT   NOT NULL,
    lease_until BIGINT NOT NULL
);
-- +goose StatementEnd

-- +goose Down
DROP TABLE monitor_job_locks;
