// Package headers defines the block-headers contract the toolbox uses as its
// chain trust anchor, backed by a ChainTracks service.
//
// # Local verification is the trust anchor
//
// Merkle-root verification is performed LOCALLY, not delegated to the header
// service. ChainTracks exposes no "is this root valid for this height?" HTTP
// route, so [Headers.VerifyMerkleRoot] is defined to fetch the header for the
// given height via the same [Headers] and byte-compare its merkle root against
// the candidate. This keeps the toolbox in control of the comparison: the
// header service is trusted only to return the correct header for a height, and
// the SPV decision (does this BUMP's computed root match the chain?) stays on
// our side of the wire.
//
// # Streams
//
// [ChainSubscriber] exposes the two ChainTracks SSE streams — tip updates and
// reorg notifications — as Go channels. The streams carry no resumable event id
// (unlike the arcade status stream), so the channel contract is:
//
//   - Both channels close ONLY when ctx is canceled; implementations MUST
//     auto-reconnect transparently on transport errors.
//   - After every (re)connect the implementation MUST emit a fresh TipEvent
//     carrying the current tip on the tip channel, so consumers always
//     converge on real state after a gap.
//   - The reorg stream is BEST-EFFORT / advisory: events may be missed across
//     reconnect gaps and are never replayed. Consumers requiring correctness
//     (the monitor) must not rely on it exclusively — authoritative reorg
//     safety comes from re-verifying stored proofs against [Headers] (the
//     poll path).
//
// This package contains only types, interfaces, and their contract
// documentation; the concrete ChainTracks-backed implementation lands in a
// later task.
package headers
