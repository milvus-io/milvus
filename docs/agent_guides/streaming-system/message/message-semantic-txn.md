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
- Body messages must arrive within the keepalive window, otherwise the transaction expires. After expiration, local clients must begin a new transaction. A replicated transaction aborted by local Flush may reuse its source TxnID during complete replay.
- CommitTxn/RollbackTxn must come after all body messages have completed (inFlightCount == 0).
- Commit requires at least one successful body append. After waiting for in-flight bodies, an empty transaction is rejected and its session is rolled back and cleaned up. Failed body appends do not make a transaction nonempty.
- Recovery accepts historical empty committed transactions as data no-ops: it preserves PChannel Summary/AckTracker progress but does not dispatch them to VChannel resources or advance query MVCC.
- With partial-update CAS enabled, the lock interceptor enforces the body-before-commit order for local transactions before TimeTick: a CommitTxn freezes its transaction on arrival, later bodies are rejected with `TRANSACTION_EXPIRED`, and a commit that arrives while a body admitted before it is still in flight is itself rejected with `TRANSACTION_EXPIRED` (the session stays InFlight until its lease ends), so an accepted commit's row locks cover the complete write set. See the [partial-update design](../../../design-docs/design_docs/20260601-partial-update-optimistic-lock-design.md).
- The producer sets `CommitTxnMessageHeader.body_cnt` to the logical body count. Both primary and secondary reject nonzero mismatches before appending Commit. Consumption and recovery rely on this write-side invariant without rechecking the count. `body_cnt == 0` always skips validation, including for nonempty legacy transactions.
- The consumer assembles Begin + body messages into a single ImmutableTxnMessage using **CommitTxn's TimeTick and LastConfirmedMessageID** as the transaction's overall TimeTick and LastConfirmedMessageID.
- On RollbackTxn, the consumer discards the corresponding Begin + body messages.
- Replicated transactions use an infinite lease. Local Flush aborts receiving sessions after durable append; CDC replays the complete source transaction with the same TxnID. A nonzero `CommitTxnMessageHeader.body_cnt` proves completeness against the accepted source-ID set before Commit; zero skips validation for compatibility. All successful exclusive operations use the same scoped abort for local and replicated sessions; failed or ignored operations do not abort. WAL cancellation/fencing requires recovery rather than reopening a prepared Commit on the old writer. See [Local Flush Control](../wal/local-flush.md).

```
BeginTxn@tt=10 → Insert@tt=11 → Insert@tt=12 → CommitTxn@tt=15
Consumer sees: ImmutableTxnMessage@tt=15 (contains all body messages)

BeginTxn@tt=10 → Insert@tt=11 → RollbackTxn@tt=15
Consumer sees: (nothing — body messages discarded)
```
