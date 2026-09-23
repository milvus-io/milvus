# TimeTick & Transaction

## TimeTick

PChannel-level monotonically increasing log sequence number assigned to every WAL message. Defines total order within a PChannel and serves as the MVCC visibility boundary. **Only comparable within the same PChannel.**

See [TimeTick Message Semantic](../message/message-semantic-time-tick.md) for the TimeTick message itself.

### Allocation

Every message receives a unique TimeTick from the TSO via `AckManager.Allocate()`. Transaction sub-messages each get their own TimeTick; the assembled transaction uses CommitTxn's TimeTick as its overall TimeTick.

A message type whose `FreshTimeTick` property is set takes `AckManager.AllocateFresh()` instead: it discards the node's cached TSO batch and fetches a new one before assigning the TimeTick, so the tick is greater than every tick any node had received before that fetch. SplitShard is the only type marked `FreshTimeTick`, because its replicas land on different PChannels — the source's fence and a target's genesis are the same broadcast message, but each PChannel allocates its own TimeTick locally from whatever batch it holds cached. Without a fresh batch, a target's genesis TimeTick could be drawn from a batch fetched before the source's fence and sort at or before `T_switch`; discarding the cache on every replica (source, target, and control channel alike) is necessary but not sufficient on its own — the guarantee also depends on the broadcaster landing the source group before any target, via `append_first_vchannels` (see broadcaster.md). Both mechanisms are required together: FreshTimeTick alone does not order the replicas, and append-first alone does not stop a target from drawing a TimeTick that sorts at or before `T_switch`.

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

## LastConfirmedMessageID

A property attached to every ImmutableMessage. **Guarantee**: reading from this MessageID ensures all subsequent messages have TimeTick strictly greater than this message's TimeTick (including assembled transaction messages).

`lastConfirmedManager` maintains a min-heap of in-flight messages ordered by MessageID. A message can be popped from the heap (advancing `lastConfirmedMessageID`) only when **both** conditions are met:

1. **No concurrent write in-flight**: `EndTimestamp` (= `lastAllocatedTimeTick` at ack time, recording which TimeTicks were already allocated when this message finished writing) < current confirmed TimeTick — all messages allocated up to that point have been confirmed, so no smaller MessageID can still enter the heap.
2. **Txn resolved** (if transactional): the transaction is committed, rolled back, or expired — no more body messages can appear under this transaction.

When both hold, no future message can have a MessageID smaller than this entry, so it is safe to advance.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/timetick/` — TimeTick interceptor, `AckManager`, `lastConfirmedManager`, sync inspector
- `internal/streamingnode/server/wal/interceptors/txn/` — `TxnManager`, `TxnSession`
- `internal/streamingnode/server/wal/utility/` — `ReOrderByTimeTickBuffer`
