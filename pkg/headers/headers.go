package headers

import (
	"context"

	"github.com/bsv-blockchain/go-sdk/chainhash"
)

// Headers is the read contract for block headers and merkle-root verification,
// backed by a ChainTracks service. Implemented by the concrete ChainTracks
// client in a later task.
type Headers interface {
	// CurrentHeight returns the height of the current chain tip as seen by the
	// header service.
	CurrentHeight(ctx context.Context) (uint32, error)

	// HeaderByHeight returns the block header at the given height. It errors if
	// the height is above the current tip or otherwise unknown to the service.
	HeaderByHeight(ctx context.Context, height uint32) (*Header, error)

	// VerifyMerkleRoot reports whether root is the merkle root of the block at
	// the given height.
	//
	// Verification is LOCAL: the implementation fetches the header for height
	// (see HeaderByHeight) and byte-compares root against that header's merkle
	// root. ChainTracks exposes no isValidRootForHeight HTTP route, so there is
	// nothing to delegate to — the comparison is deliberately performed on our
	// side. Returns (false, err) if the header for height cannot be retrieved;
	// (false, nil) when the header is available but the roots differ.
	VerifyMerkleRoot(ctx context.Context, root *chainhash.Hash, height uint32) (bool, error)
}

// ChainSubscriber exposes the ChainTracks tip and reorg SSE streams as Go
// channels.
//
// Close semantics (the contract implementations MUST honor):
//
//   - Both channels close ONLY when ctx is canceled. A transport error never
//     closes a channel: implementations MUST auto-reconnect transparently and
//     keep the channel open across the gap.
//   - The underlying SSE streams carry no resumable event id, so events
//     occurring during a reconnect gap are lost, not replayed. The tip channel
//     self-heals (see SubscribeTip); the reorg channel is best-effort (see
//     SubscribeReorg).
type ChainSubscriber interface {
	// SubscribeTip returns a channel that emits a TipEvent whenever the chain
	// tip advances. After EVERY (re)connect — the initial connect and each
	// transparent reconnect — the implementation MUST emit a fresh TipEvent
	// carrying the current tip, so consumers always converge on real chain
	// state after a gap without waiting for the next block.
	SubscribeTip(ctx context.Context) <-chan TipEvent

	// SubscribeReorg returns a channel that emits a ReorgEvent whenever a chain
	// reorganization is detected.
	//
	// This stream is BEST-EFFORT / advisory: reorg events occurring during a
	// reconnect gap are missed and never replayed (the stream has no resume
	// cursor and no on-connect state snapshot equivalent to the tip's).
	// Consumers requiring correctness — notably the monitor — must not rely on
	// it exclusively; authoritative reorg safety comes from re-verifying stored
	// proofs against [Headers] (the poll path). Treat a delivered ReorgEvent as
	// a prompt to re-verify sooner, never as the sole source of truth.
	SubscribeReorg(ctx context.Context) <-chan ReorgEvent
}

// Header is a block header: the six 80-byte serialized fields plus this block's
// position (Height) and identifier (Hash).
//
// The ChainTracks tip stream and header endpoints actually carry the full
// header; this is the toolbox's own clean representation of it. Hash fields use
// go-sdk's chainhash.Hash (internal byte order); render with .String() for the
// conventional reversed hex.
type Header struct {
	// Version is the 4-byte block version.
	Version int32
	// PrevBlock is the hash of the previous block's header.
	PrevBlock chainhash.Hash
	// MerkleRoot is the root of the block's transaction merkle tree. This is the
	// value VerifyMerkleRoot compares against.
	MerkleRoot chainhash.Hash
	// Timestamp is the block's Unix creation time (4 bytes).
	Timestamp uint32
	// Bits is the compact-encoded difficulty target (4 bytes).
	Bits uint32
	// Nonce is the mining nonce (4 bytes).
	Nonce uint32
	// Height is the block's position in the chain (0 = genesis).
	Height uint32
	// Hash is the double-SHA256 hash of the 80-byte header — the block id.
	Hash chainhash.Hash
}

// TipEvent is emitted when the chain tip advances. The ChainTracks tip stream
// carries the full header; TipEvent projects the two identifying fields a
// consumer needs to react (fetch the full header via [Headers.HeaderByHeight]
// if more is required).
type TipEvent struct {
	// Height is the new tip height.
	Height uint32
	// Hash is the new tip block hash.
	Hash chainhash.Hash
}

// ReorgEvent is emitted when a chain reorganization is detected.
//
// Field availability is shaped by what the ChainTracks reorg stream actually
// carries (its ReorgEvent: OrphanedHashes, CommonAncestor, NewTip, Depth):
//
//   - ForkHeight and NewTip map directly to CommonAncestor.Height and
//     NewTip.Hash.
//   - OrphanedHashes is the stream's primary payload and is carried through
//     verbatim so consumers can invalidate any merkle paths / block references
//     pointing at those blocks. Depth is len(OrphanedHashes) and is therefore
//     not duplicated here.
//   - OldTip is a BEST-EFFORT convenience field the stream does NOT provide
//     directly: the reorg payload has no single labeled "old tip", only the
//     full orphaned set — and OrphanedHashes carry no heights, so identifying
//     the highest orphan requires chaining the orphaned headers together (or
//     looking each hash up), which an implementation may not always be able to
//     do. An implementation derives OldTip as the pre-reorg tip when it can and
//     leaves it zero otherwise. Consumers needing the authoritative set should
//     use OrphanedHashes.
type ReorgEvent struct {
	// ForkHeight is the height of the common ancestor where the abandoned and
	// new chains diverge; blocks above it on the old chain are orphaned.
	ForkHeight uint32
	// OldTip is the pre-reorg chain tip (derived; see type doc). Zero if
	// unavailable.
	OldTip chainhash.Hash
	// NewTip is the post-reorg chain tip.
	NewTip chainhash.Hash
	// OrphanedHashes lists every block removed from the main chain by this
	// reorg.
	OrphanedHashes []chainhash.Hash
}
