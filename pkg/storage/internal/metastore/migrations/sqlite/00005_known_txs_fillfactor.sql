-- +goose Up
-- Deliberately empty. The postgres 00005 migration lowers known_txs' fillfactor
-- to reserve same-page room for the table's very high update rate; SQLite has no
-- equivalent knob (its page fill is governed by auto_vacuum / page_size, both
-- database-wide and both set at open time, not per table).
--
-- The file exists so the two engines' migration chains stay at the same version
-- number, which is the convention every earlier migration follows.
SELECT 1;

-- +goose Down
SELECT 1;
