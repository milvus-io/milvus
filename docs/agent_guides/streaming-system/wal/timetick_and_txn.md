# TimeTick & Transaction

## TimeTick

PChannel-level monotonically increasing log sequence number assigned to every WAL message. Defines total order within a PChannel and serves as the MVCC visibility boundary. **Only comparable within the same PChannel.**

See [TimeTick Message Semantic](../message/message-semantic-time-tick.md) for the TimeTick message itself.

### Allocation

Every message receives a unique TimeTick from the TSO via `AckManager.Allocate()`. Transaction sub-messages each get their own TimeTick; the assembled transaction uses CommitTxn's TimeTick as its overall TimeTick.

### Confirm and Sync

Every TimeTick transitions: **Allocated** → **Confirmed** → **Synced**.

- **Confirmed**: Append completed and `Acker.Ack()` called. The confirmed watermark (`lastConfirmedTimeTick`) advances only when all TimeTicks ≤ it are acknowledged — any in-flight message blocks advancement.
- **Synced**: A background `TimeTickSyncInspector` periodically drains confirmed entries, constructs a TimeTick message with `Timestamp` = confirmed watermark, and appends it to the WAL. When no real messages exist in the batch, a non-persisted TimeTick is generated (skips WAL backend write).

### Consumer-Side Reordering

`ReOrderByTimeTickBuffer` collects messages in a min-heap. On TimeTick message arrival, drains all messages with TimeTick ≤ T in order — restoring total order regardless of physical WAL write order.

## Transaction

The TxnManager coordinates multi-message transactions within a single VChannel. Each sub-message gets its own TimeTick during allocation; at the consumer side, the assembled transaction uses CommitTxn's TimeTick as its overall TimeTick, making it one logical WAL entry.

See [Transaction Messages](../message/message-semantic-txn.md) for message types and invariants.

### Transaction Lifecycle

1. **Begin**: Allocate a TxnID and a TimeTick. Create a `TxnSession` tracking in-flight message count.
2. **Append**: Each message in the transaction increments the in-flight counter and refreshes the session TTL.
3. **Commit**: Mark the transaction as committed. The transaction's messages become visible to consumers as a single atomic group at the assigned TimeTick.
4. **Rollback**: Mark the transaction as rolled back. All messages in the transaction are discarded.
5. **Expire**: Expiration TimeTick = `lastTimetick + keepalive`. Each successful body append refreshes `lastTimetick` (lease renewal). Once current TimeTick ≥ expiration, the transaction is unrecoverably failed (`STREAMING_CODE_TRANSACTION_EXPIRED`).
6. **Force-fail**: The [Lock](lock.md) interceptor force-fails all active transactions at a VChannel when an exclusive operation is appended.

### Interceptor-Originated Transactions

An append interceptor can turn one append into a transaction with
`txn.AppendInTxn`, so that a message and the messages derived from it become
visible at the same TimeTick. Two independent top-level messages can never
share a TimeTick. A transaction is the only way.

- **Position**: the caller must sit above the TimeTick interceptor (every
  message of the group needs its own TimeTick, and BeginTxn must get the
  smallest one) and below the Lock interceptor (the whole group runs under the
  lock acquired for the original append, so an exclusive message cannot
  force-fail it halfway).
- **Accepted input**: the message is an autocommit DML message (Insert, Delete)
  or a CommitTxn. Every derived message is an autocommit DML message of the same
  VChannel. Replicated messages (messages with a replicate header) are rejected,
  because on a secondary WAL the local BeginTxn and CommitTxn written here would
  enter a replicated stream. Exclusive and self-controlled messages are rejected,
  because they must never become transaction bodies. The derived messages are
  modified in place, so the caller builds a fresh set for every attempt.
- **Autocommit message**: BeginTxn, a copy of the message, the derived messages
  and CommitTxn are appended. The caller's message is never modified, because
  the Redo interceptor may run the whole chain again. A BarrierTimeTick of the
  message is copied to BeginTxn, so that BeginTxn does not take a TimeTick below
  the barrier.
- **CommitTxn of a client transaction**: the derived messages are appended as
  bodies before CommitTxn. If one of them fails, CommitTxn is not appended and
  the transaction is left to expire by keepalive, because the client owns it and
  no rollback is written here.
- **Failure of a self-built transaction**: once the session exists, every failed
  step leads to a RollbackTxn, a failed CommitTxn included. The rollback is safe
  and self-selecting: the TimeTick interceptor rejects it before anything is
  written when the session is no longer in flight, and a persisted CommitTxn
  always moved the session out of the in-flight state first. Without it a failed
  CommitTxn would keep the session in flight for the whole keepalive and hold
  back `lastConfirmedMessageID` of the PChannel.
- **Error of a self-built transaction**: a step that fails with
  `STREAMING_CODE_TRANSACTION_EXPIRED` or `STREAMING_CODE_INVALID_TRANSACTION_STATE`
  is reported as `STREAMING_CODE_INNER`. The client appended one autocommit
  message and knows nothing about the transaction, and it treats a transaction
  error as unrecoverable. Every other error is returned unchanged.
- **Result**: the caller must return the MessageID of CommitTxn, so that the
  client of the original append observes the MessageID, TimeTick and
  LastConfirmedMessageID of CommitTxn. The TxnContext is removed from the append
  result, because the client did not ask for a transaction.
- **Metrics**: one `wal.Append` records one metric entry for the whole group, so
  the WAL record count and byte count under-report an upgraded append.

## LastConfirmedMessageID

A property attached to every ImmutableMessage. **Guarantee**: reading from this MessageID ensures all subsequent messages have TimeTick strictly greater than this message's TimeTick (including assembled transaction messages).

`lastConfirmedManager` maintains a min-heap of in-flight messages ordered by MessageID. A message can be popped from the heap (advancing `lastConfirmedMessageID`) only when **both** conditions are met:

1. **No concurrent write in-flight**: `EndTimestamp` (= `lastAllocatedTimeTick` at ack time, recording which TimeTicks were already allocated when this message finished writing) < current confirmed TimeTick — all messages allocated up to that point have been confirmed, so no smaller MessageID can still enter the heap.
2. **Txn resolved** (if transactional): the transaction is committed, rolled back, or expired — no more body messages can appear under this transaction.

When both hold, no future message can have a MessageID smaller than this entry, so it is safe to advance.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/timetick/` — TimeTick interceptor, `AckManager`, `lastConfirmedManager`, sync inspector
- `internal/streamingnode/server/wal/interceptors/txn/` — `TxnManager`, `TxnSession`, `AppendInTxn`
- `internal/streamingnode/server/wal/utility/` — `ReOrderByTimeTickBuffer`
