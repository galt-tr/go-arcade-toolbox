-- +goose Up
-- verified_rejected_at is the SECOND, independent timestamp the reject→release
-- reconciler (Task 19) needs beyond suspect_since. suspect_since records when a
-- tx first became suspect from an SSE/broadcast reject event (which may be a
-- transient false positive); verified_rejected_at records when the reconciler
-- FIRST re-verified it as still-REJECTED via an authoritative GetTx. The
-- two-pass false-positive guard releases inputs only after the tx has stayed
-- rejected across two reconciler passes separated by the grace window — i.e.
-- once verified_rejected_at is old enough. It is reset to NULL whenever a tx
-- (re-)enters the suspect state, so every suspect cycle starts fresh.
ALTER TABLE known_txs ADD COLUMN verified_rejected_at TIMESTAMPTZ NULL;

-- +goose Down
ALTER TABLE known_txs DROP COLUMN verified_rejected_at;
