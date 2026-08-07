# quickstart

The 5-minute tour. Builds a SQLite-backed wallet, funds it, sends a payment, and
reads the balance and outputs back — end to end against in-process mocks.

```sh
go run ./examples/quickstart
```

Expected output (txids and change split will differ per run):

```
wallet identity (receive) key: 0279be667ef9dcbbac55a06295ce870b07029bfcdb2dce28d959f2815b16f81798
balance before funding: 0 sat
funded with 100000 sat via tx ...
balance after funding: 100000 sat
sent payment in tx ...
arcade received 1 broadcast(s)
balance after payment: 98957 sat (change from seed - payment - fee)
wallet has N spendable output(s):
  ...
```

What to notice:

- **Balance starts at 0.** An arcade-only wallet learns coins only from
  `InternalizeAction` and from transactions it created — there is no
  restore-from-seed.
- **`CreateAction` with `SignAndProcess` does everything in one call**: fund from
  the wallet's coins, sign with real BRC-29 signatures, and broadcast the
  Extended Format transaction through the Arcade client.
- **Change returns to the wallet** as new spendable coins, so the balance after
  the payment is `seed − payment − fee`.

The wallet wiring is in [`../internal/demoenv`](../internal/demoenv/demoenv.go).
