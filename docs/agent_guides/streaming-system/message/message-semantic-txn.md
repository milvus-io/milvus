# Transaction Messages

Messages managing transactions on a single VChannel.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| BeginTxn | Single VChannel (no Broadcast) | No | — |
| CommitTxn | Single VChannel (no Broadcast) | No | — |
| RollbackTxn | Single VChannel (no Broadcast) | No | — |
| Txn *(synthetic)* | Single VChannel (no Broadcast) | — | — |

- **BeginTxn**: Opens a new transaction session, allocating a TxnID and establishing a keepalive window.
- **CommitTxn**: Commits an open transaction, finalizing all body messages.
- **RollbackTxn**: Aborts an open transaction, discarding all body messages.
- **Txn**: Synthetic message — complete committed transaction assembled on consumer side, never directly appended.

## Key Invariants

- BeginTxn must precede all body messages and CommitTxn/RollbackTxn for the same transaction.
- Body messages must arrive within the keepalive window, otherwise the transaction expires. After expiration, no messages for that transaction will appear in the WAL.
- CommitTxn/RollbackTxn must come after all body messages have completed (inFlightCount == 0).
- The consumer assembles Begin + body messages into a single ImmutableTxnMessage using **CommitTxn's TimeTick and LastConfirmedMessageID** as the transaction's overall TimeTick and LastConfirmedMessageID.
- On RollbackTxn, the consumer discards the corresponding Begin + body messages.

```
BeginTxn@tt=10 → Insert@tt=11 → Insert@tt=12 → CommitTxn@tt=15
Consumer sees: ImmutableTxnMessage@tt=15 (contains all body messages)

BeginTxn@tt=10 → Insert@tt=11 → RollbackTxn@tt=15
Consumer sees: (nothing — body messages discarded)
```
