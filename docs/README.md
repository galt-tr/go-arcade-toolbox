# Documentation

- [Architecture](architecture.md) — the three sources of truth, the package map, the write-path and async status lifecycle, the trust model, and what was removed vs go-wallet-toolbox and why.
- [Storage](storage.md) — the `WalletStorageProvider` contract, the `AuthID` multi-user model, the pluggable `utxostore`, Mode A vs Mode B, the spendability seam, and how to add + conformance-test a backend.
- [Arcade integration](arcade-integration.md) — the full status lifecycle table (with wire values), EF broadcast semantics, the SSE contract, ChainTracks local root verification, and the reject→release reconciler.
- [High-throughput guide](high-throughput-guide.md) — the denominated fuel-pool workflow, config tuning, the **measured** throughput/durability tradeoff, hardware notes, and a failure-mode playbook.
- [Migration from go-wallet-toolbox](migration-from-go-wallet-toolbox.md) — the import-path rewrite, the byte-compatibility guarantees, behavior deltas, dropped features, and config-file migration.
- [Operations](operations.md) — **backup is a correctness requirement**, per-backend backup/restore, the Aerospike fund-safety requirements, monitoring, running the storage server, and the rootless-podman test setup.
- [Benchmarks](benchmarks/README.md) — the raw measured write-path throughput numbers.

Runnable programs live in [`../examples`](../examples).
