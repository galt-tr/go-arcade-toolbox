# Documentation

New here? Start with the [**Getting started guide**](../GETTING_STARTED.md), or hand
[**AGENTS.md**](../AGENTS.md) to a coding agent. The documents below are the reference
material behind them.

- [Architecture](architecture.md) — the three sources of truth, the package map, the write-path and async status lifecycle, the trust model, and what was removed vs go-wallet-toolbox and why.
- [Storage](storage.md) — the `WalletStorageProvider` contract, the `AuthID` multi-user model, the pluggable `utxostore`, Mode A vs Mode B, the spendability seam, and how to add + conformance-test a backend.
- [Arcade integration](arcade-integration.md) — the full status lifecycle table (with wire values), EF broadcast semantics, the SSE contract, ChainTracks local root verification, and the reject→release reconciler.
- [Reject→release vs unfail](reject-release-vs-unfail.md) — why the automatic verified reconciler replaces manual unfail, how the two-pass and winner-union guards work, and why that is safer under Arcade’s async status model. ([HTML](reject-release-vs-unfail.html) for local browser viewing)
- [High-throughput guide](high-throughput-guide.md) — the denominated fuel-pool workflow, config tuning, the **measured** throughput/durability tradeoff, hardware notes, and a failure-mode playbook.
- [Application throughput playbook](application-throughput-playbook.md) — the **application author's** companion to the above: what limits you and in what order, bounding the input BEEF, unconfirmed chain depth vs the mempool ancestor limit, output-shape fragility, the two-step covenant path, the load loop, and a failure-mode playbook keyed by observed symptom.
- [Migration from go-wallet-toolbox](migration-from-go-wallet-toolbox.md) — the import-path rewrite, the byte-compatibility guarantees, behavior deltas, dropped features, and config-file migration.
- [Operations](operations.md) — **backup is a correctness requirement**, per-backend backup/restore, the Aerospike fund-safety requirements, monitoring, running the storage server, and the rootless-podman test setup.
- [Rejection hardening audit](rejection-hardening-audit.md) — every way a client can get a transaction rejected that the library could prevent or explain locally, the broadcast failure taxonomy, and what a client must still do itself.
- [Aerospike value review](aerospike-value-review.md) — whether Mode B is earning its keep. Current recommendation: no — plan to drop it and run PostgreSQL-only.
- [Benchmarks](benchmarks/README.md) — the raw measured write-path throughput numbers.

Runnable programs live in [`../examples`](../examples).
