// Package monitor is the background daemon that drives the arcade-only async
// transaction lifecycle. It consumes arcade's status SSE stream and applies
// each event to the wallet state through a [MonitoredStorage] (the
// storage.Provider), promoting broadcast-accepted transactions to unproven and,
// once a header-verified merkle proof arrives, to mined — or marking them
// suspect on rejection. A set of leased, scheduled fallback tasks (send-waiting,
// abort-abandoned, status/proof polling) cover SSE outages, and channel
// consumers react to chain tip and reorg events.
//
// Exactly-one-instance execution of each scheduled task across a multi-instance
// deployment is provided by a SQL lease (see [LeaseLocker]); the SSE apply
// pipeline (see the status-events file) is at-least-once with a per-batch,
// non-regressing replay cursor, relying on [MonitoredStorage.ApplyStatusUpdate]
// being idempotent.
package monitor

import (
	"context"
	"time"

	"github.com/galt-tr/go-arcade-toolbox/pkg/arcade"
	"github.com/galt-tr/go-arcade-toolbox/pkg/defs"
)

// MonitoredStorage is everything the daemon needs from the storage layer. It is
// satisfied by *storage.Provider (a compile-time assertion lives in the storage
// tests to avoid a production storage→monitor import).
//
// All methods must be idempotent: the SSE pool delivers at-least-once and the
// poll fallbacks re-apply. The sweep methods take a per-run limit; a
// non-positive limit means "the storage default".
type MonitoredStorage interface {
	// ApplyStatusUpdate applies one arcade status record to the wallet state.
	// It is the single entry point the SSE apply pool calls per event and the
	// polls call per GetTx result. It routes by status (SEEN → unproven, MINED →
	// header-verified proof + mined, REJECTED → suspect, …) and is idempotent
	// with terminal/lattice guards.
	ApplyStatusUpdate(ctx context.Context, rec arcade.TxRecord) error

	// ApplyStatusBatch applies a whole SSE apply-batch of status records in one
	// call with batched DB writes. It is EXACTLY EQUIVALENT to calling
	// ApplyStatusUpdate on each rec in arrival order (same terminal/lattice
	// guards, same per-txid outcome, idempotent) and is the high-throughput entry
	// point the SSE apply pool uses; the poll fallbacks still call
	// ApplyStatusUpdate per tx.
	ApplyStatusBatch(ctx context.Context, recs []arcade.TxRecord) error

	// SendWaitingTransactions broadcasts delayed (unsent) transactions and
	// advances their state on acceptance.
	SendWaitingTransactions(ctx context.Context, limit int) error

	// AbortAbandoned aborts never-broadcast transactions created at or before
	// olderThan (released reservation, removed change, status → aborted).
	AbortAbandoned(ctx context.Context, olderThan time.Time, limit int) error

	// SweepStaleReservations reclaims funding reservations older than olderThan
	// whose transaction can no longer be sent (leaked inputs). It is
	// fence-first: an action whose reservation it takes back is ABORTED, so
	// nothing can sign or broadcast it afterwards. Reservations belonging to a
	// live transaction are left alone whatever their age.
	SweepStaleReservations(ctx context.Context, olderThan time.Time, limit int) error

	// SynchronizeTransactionStatuses is the poll fallback: it re-polls stale
	// non-terminal transactions via the oracle and routes them through
	// ApplyStatusUpdate (covers SSE outages and the cold-start terminal gap).
	SynchronizeTransactionStatuses(ctx context.Context, limit int) error

	// CheckProofs is the proof poll fallback: it re-polls broadcast-accepted but
	// unproven transactions for a merkle proof.
	CheckProofs(ctx context.Context, limit int) error

	// DemoteReorgedProofs demotes any stored proof at or above forkHeight whose
	// merkle root no longer matches the header, re-entering it into the proof
	// pipeline. Best-effort; the poll path is the authority.
	DemoteReorgedProofs(ctx context.Context, forkHeight uint32) error

	// VerifyAndReleaseSuspects is the reject→release reconciler pass: it
	// re-verifies suspect-failed transactions against the oracle and releases the
	// inputs of the provably-dead ones (two-pass rejected guard + double-spend
	// winner rule + max-quarantine stuck escalation). grace is the suspect age /
	// two-pass separation; maxQuarantine is the escalate-to-stuck ceiling. Idempotent.
	VerifyAndReleaseSuspects(ctx context.Context, grace, maxQuarantine time.Duration, limit int) (defs.ReconcilerReport, error)

	// DrainOutbox replays pending utxo-ops outbox rows against the inventory
	// store, idempotently — the Mode B crash-recovery half of a release. A no-op
	// in Mode A. Run before each reconciler pass to heal a crashed release.
	DrainOutbox(ctx context.Context, limit int) (defs.OutboxDrainReport, error)

	// GetKeyValue / SetKeyValue expose the small key-value store where the SSE
	// replay cursor lives.
	GetKeyValue(ctx context.Context, key string) ([]byte, bool, error)
	SetKeyValue(ctx context.Context, key string, value []byte) error

	// AcquireLease / ReleaseLease back the distributed job lease (see
	// [LeaseLocker]). AcquireLease returns true when this owner holds the lease
	// for job afterward, false when another live instance holds it.
	AcquireLease(ctx context.Context, job, owner string, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, job, owner string) error
}
