# internalize

Funding a wallet the only way an arcade-only wallet can be funded from the
outside: by **receiving** a payment via `InternalizeAction`.

```sh
go run ./examples/internalize
```

The program plays both roles:

1. **Sender** builds a mined BRC-29 payment to the wallet's identity key and
   registers its merkle proof in the mock ChainTracks.
2. **Recipient** wallet internalizes it: it re-derives the BRC-29 locking script
   from the payment remittance, verifies the merkle proof against ChainTracks
   (the SPV trust anchor), and records the output as a spendable coin.

In production the sender is someone else and the BEEF + derivation material
arrive out of band (e.g. over a BRC-29 payment protocol). The recipient half —
the `InternalizeAction` call — is identical.

Two protocols exist:

- **`wallet payment`** (used here) records the BRC-29 derivation material, so the
  received coin is wallet-signable and spendable.
- **`basket insertion`** tags an output into a named basket but records no
  derivation material, so those coins are not wallet-signable today.

Because there is no restore-from-seed, the local database is the *only* record
that these coins are yours — see [`../../docs/operations.md`](../../docs/operations.md).
