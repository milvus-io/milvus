# TimeTick & Transaction

## TimeTick

PChannel-level monotonically increasing log sequence number assigned to every WAL message. Defines total order within a PChannel and serves as the MVCC visibility boundary. **Only comparable within the same PChannel.**

See [TimeTick Message Semantic](../message/message-semantic-time-tick.md) for the TimeTick message itself.

### Allocation

Every message receives a unique TimeTick from the TSO via `AckManager.AllocateForAppend()`. Transaction sub-messages each get their own TimeTick; the assembled transaction uses CommitTxn's TimeTick as its overall TimeTick.

This append-specific entry retries both timestamp allocation and barrier advancement
without an attempt or elapsed-time limit, until success or cancellation of its caller's
context. For a WAL append, that context follows the WAL lifecycle, not the client RPC.
The shared allocator's per-RPC 10-second timeout (or another transient coordinator error)
must not escape as a failed append on a live WAL after an earlier interceptor changed
in-memory state. Only successful allocation publishes an acker and advances the allocated
watermark.

Retries occur outside both the shared allocator's mutex and the AckManager mutex.
Barrier waits also remain outside the AckManager mutex: the allocator's slow barrier
path may need other allocations to advance it. Background TimeTick sync uses the
single-attempt `Allocate()` entry and retries on the inspector's next sync; it has no
preceding append mutation to preserve, and its context belongs to the process-wide
inspector rather than the individual WAL. Making that path retry forever while holding
the AckManager mutex could prevent a WAL from shutting down during a TSO outage.

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

1. **Begin**: Allocate a TxnID and a TimeTick. Create a `TxnSession` tracking in-flight and successful logical body counts.
2. **Append**: Each message in the transaction increments the in-flight counter and refreshes the session TTL.
3. **Commit**: Close body admission and wait for in-flight appends. Check a nonzero Commit `body_cnt` against successful appends; zero skips validation for legacy transactions. A mismatch rolls back the session and releases references, causing a whole-transaction retry. After successful Commit append, mark the transaction as committed. The transaction's messages become visible to consumers as a single atomic group at the assigned TimeTick.
4. **Rollback**: Mark the transaction as rolled back. All messages in the transaction are discarded.
5. **Expire**: Expiration TimeTick = `lastTimetick + keepalive`. Each successful body append refreshes `lastTimetick` (lease renewal). Once current TimeTick ≥ expiration, the transaction is unrecoverably failed (`STREAMING_CODE_TRANSACTION_EXPIRED`).
6. **Force-fail**: The [Lock](lock.md) interceptor force-fails locally originated active transactions at a VChannel when an exclusive operation is appended.

A replicated Commit interrupted by WAL cancellation/fencing stays unresolved until
recovery; it is neither marked committed nor reset for retry on the old writer.
Ordinary backend errors retry below the interceptor. See
[Append retry and writer lifetime](../walbackend/walbackend.md#append-retry-and-writer-lifetime).
Local Flush durably aborts scoped receiving sessions, allowing CDC to replay the
full transaction. See [Local Flush Control](local-flush.md).

### Scope and duplicate handling

A transaction belongs to one VChannel even if it writes multiple segments. Local
ManualFlush therefore resolves only that business VChannel's transactions; FlushAll
resolves the whole PChannel. Incomplete replicated transactions need not finish
before a local Flush: its durable abort boundary allows the complete transaction
to be replayed with new local segment assignments.

Distinguish three kinds of duplicate when reasoning about `body_cnt`:

| Duplicate | Existing protection |
| --- | --- |
| A backend retries one logical append | Same local TimeTick; scanner physical deduplication removes retry copies before transaction assembly. |
| CDC replays a source body | Same immutable source MessageID; the receiver deduplicates before append and does not increase the accepted-body count. |
| A primary producer retries a body as another logical append | A new TimeTick can reach the WAL; the producer's expected body count detects excess successful appends at Commit. |

Count equality proves completeness only with the producer contract: all intended
bodies have succeeded before Commit, or, on a replica, accepted distinct source IDs
are a subset of one immutable source transaction. A count alone cannot establish
message identity. Zero deliberately skips validation for compatibility. These
preconditions do not remove the local transaction's mutex or in-flight wait: primary
body appends can be concurrent even though replicated appends are serialized.

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
