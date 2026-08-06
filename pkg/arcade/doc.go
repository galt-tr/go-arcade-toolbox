// Package arcade defines the wire types and the transaction-oracle contract
// for talking to an Arcade node — this toolbox's sole transaction-truth oracle.
//
// # The async oracle model
//
// Arcade is asynchronous: a broadcast is an intake, not a verdict.
//
//   - POST /tx accepts a transaction (binary Extended Format) and replies 202
//     with an EARLY status (typically RECEIVED). 202 is NOT a final outcome —
//     it only means the transaction entered arcade's pipeline. See
//     [BroadcastResult].
//   - The authoritative lifecycle is delivered out of band over Server-Sent
//     Events (a separate SSE service). Each event carries a [TxRecord] and a
//     nanosecond-timestamp id used as Last-Event-ID to resume a dropped
//     stream. See [StatusEvent] and [TxOracle.StreamStatus].
//   - GET /tx/{txid} is the poll fallback and the source of terminal history:
//     it returns the current status plus blockHash/blockHeight, the merklePath
//     (BRC-74 BUMP), the rawTx, and any competingTxs. See [TxOracle.GetTx].
//
// # Failure taxonomy on broadcast
//
// The three broadcast outcomes are deliberately distinct because they demand
// different caller behavior:
//
//   - 4xx is a tx-level rejection: FINAL. Arcade persists a terminal REJECTED
//     row, so the verdict is durable and subscribers observe it. Never retry.
//     Surfaced as a [BroadcastResult] with Rejected=true and err=nil.
//   - 503 is backpressure, not a verdict: the transaction was NOT queued, so a
//     retry is safe. Arcade sends Retry-After: 1. Surfaced as a
//     [*BackpressureError].
//   - >=500 (other) and transport errors are opaque failures surfaced as a
//     plain error; the transaction's fate is unknown and the caller should
//     reconcile via GET /tx before retrying.
//
// # Cold-start caveat
//
// A fresh SSE connection (empty Last-Event-ID) replays only NON-terminal
// statuses; terminal history is intentionally not replayed (it is unbounded
// and queryable over REST). Consumers reconnecting after downtime must poll
// GET /tx for any transaction that may have reached a terminal state while
// they were disconnected.
//
// This package contains only types, interfaces, and their contract
// documentation. The concrete HTTP/SSE [TxOracle] implementation lands in a
// later task; a multi-instance HA router is expected to implement the same
// interface behind the scenes.
package arcade
