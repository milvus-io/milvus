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

### WAB delivery and producer backpressure

The WriteAheadBuffer (WAB) queues every MVCCBarrier, including volatile
barriers, behind its ordered data prefix. Only ordinary volatile TimeTicks may
coalesce into the latest confirmation. The inspector retains pending scoped
requests only; it does not keep carrying scopes from previously published barriers.

RecoveryStorage acknowledges a complete consumed prefix after observing a
confirmation, including Summary coverage and ordered VChannel query-barrier
events. This only makes the prefix eligible for eviction. The original cache
policy still evicts when total cached bytes exceed capacity or a record's
keepalive expires, and only within the consumed prefix. Prefetching a record
or sending it to the recovery channel is not consumption. Capacity and elapsed
keepalive never authorize eviction of an unconsumed record. This also protects
live messages while startup recovery is paused at the RecoveryBarrier.

The latest persisted confirmation is retained separately from the pending queue;
it does not pin its suffix. Consumed volatile barriers can expire or be evicted
for capacity without waiting for another durable write. A reader at or past this
confirmation can rejoin the remaining queue, or receive the latest heartbeat
when the queue is empty, after consumed volatile confirmations were evicted.
This applies to both catchup handoff and an existing tailing reader's offset.
Readers behind an evicted durable prefix must still catch up through the WAL;
RecoveryStorage never skips unconsumed barriers. Cache capacity is a target,
not a hard limit on unconsumed records.

WAB pressure has independent live configuration under
`streaming.walRateLimit.writeAheadBuffer`:

These parameters are not exported to the generated `milvus.yaml`; explicit
configuration overrides and live updates remain supported.

| Key | Default | Meaning |
| --- | --- | --- |
| `slowdownRatio` | `1.5` | Multiple S of the WAB instance capacity; finite and at least 1 |
| `slowdownRate` | `32m` | Initial slowdown rate R in bytes/second |
| `rejectRatio` | `2` | Rejection multiple Q of C; finite and greater than 1 |

For current total cached estimated bytes B and threshold C = capacity * S:

- B <= C: normal admission.
- C < B < Q*C: slowdown rate `max(1, floor(R * (Q - B/C) / (Q - 1)))`.
- B >= Q*C: reject new DML through the existing WAL admission path.

Append, consumption, and config-change callbacks recalculate pressure immediately.
Changing S or Q can enter or leave rejection even with no queue traffic.
S below 1 is clamped to 1; invalid/non-finite S uses 1. Invalid Q uses 2;
nonpositive/invalid rate uses its default.
There is no elapsed-time ramp, timer, or additional goroutine. Watchers are
removed when the WAB closes.

The `writeAheadBuffer` source uses existing multi-source producer notifications
independently of recovery persistence-tail pressure. Draining WAB clears only
its own source. WAB.Append never waits for capacity; confirmations and accepted
appends can still progress. In-flight work and non-DML messages can exceed the
threshold, so this is not a hard RSS bound.

`walWriteAheadBuffer.capacity` and `keepalive` retain their cache eviction
roles. Each WAB captures them at construction as before; live slowdownRatio
scales that same instance capacity. RecoveryStorage consumption adds an eviction
constraint, not an immediate release command. Scanner `RateLimitControl`/`AppendRateCounter`, its read-rate
checker, and the unused FlusherRecovering controller/configuration are removed.
`recoveryStorage.adaptiveRateLimit` remains active for RecoveryTail persistence
pressure, and node-memory/append-rate controls remain independent.

## Transaction

The TxnManager coordinates multi-message transactions within a single VChannel. Each sub-message gets its own TimeTick during allocation; at the consumer side, the assembled transaction uses CommitTxn's TimeTick as its overall TimeTick, making it one logical WAL entry.

See [Transaction Messages](../message/message-semantic-txn.md) for message types and invariants.

### Transaction Lifecycle

1. **Begin**: Allocate a TxnID and a TimeTick. Create a `TxnSession` tracking in-flight and successful logical body counts.
2. **Append**: Each message in the transaction increments the in-flight counter and refreshes the session TTL.
3. **Commit**: Close body admission and wait for in-flight appends. Check a nonzero Commit `body_cnt` against successful appends; zero skips validation for legacy transactions. A mismatch rolls back the session and releases references, causing a whole-transaction retry. After successful Commit append, mark the transaction as committed. The transaction's messages become visible to consumers as a single atomic group at the assigned TimeTick.
4. **Rollback**: Mark the transaction as rolled back. All messages in the transaction are discarded.
5. **Expire**: Expiration TimeTick = `lastTimetick + keepalive`. Each successful body append refreshes `lastTimetick` (lease renewal). Once current TimeTick ≥ expiration, the transaction is unrecoverably failed (`STREAMING_CODE_TRANSACTION_EXPIRED`).
6. **Force-fail**: The [Lock](lock.md) interceptor aborts all active transactions in the affected scope after an exclusive operation is successfully appended. Failed, deduplicated and explicitly ignored operations do not abort.

A Commit of either origin interrupted by WAL cancellation/fencing stays unresolved until
recovery; it is neither marked committed nor reset for retry on the old writer.
Ordinary backend errors retry below the interceptor. See
[Append retry and writer lifetime](../walbackend/walbackend.md#append-retry-and-writer-lifetime).

All exclusive operations, including Flush and ForcePromote, use one scope-based
terminal-session cleanup: mark unfinished sessions rolled back, release references,
remove them from active and recovered maps, and notify recovery/shutdown waiters.
Committed sessions keep their outcome. There is no transaction-origin selector.
Terminal state releases confirmation barriers without waiting for lease expiry.
The replication helper clears the corresponding prefix under the same WAL lock;
CDC can then replay the full source transaction. See [Local Flush Control](local-flush.md).

### Scope and duplicate handling

A transaction belongs to one VChannel even if it writes multiple segments. Local
ManualFlush therefore resolves only that business VChannel's transactions; FlushAll
resolves the whole PChannel. Incomplete replicated transactions need not finish
before a local Flush: its durable abort boundary allows the complete transaction
to be replayed with new local segment assignments.

At consumption, every exclusive operation immediately discards unfinished
transactions in the same scope, regardless of `_ur` or replication origin.
An AlterReplicateConfig record marked Ignore does not abort.
ManualFlush on CChannel does not abort business transactions.

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

### Successful Commit result retention

Native Commit retries consult a bounded tombstone cache before allocating another
TimeTick or looking up the active session. A hit returns the original MessageID,
TimeTick, LCMID and transaction context without appending or advancing MVCC. Commits
of the same TxnID are serialized through result publication. Only successful appends
publish results; session cleanup and scoped abort do not remove committed results.

`streaming.txn.tombstoneMaxCount` (default 1024 per WAL) and
`streaming.txn.tombstoneMaxLifetime` (default 1m of WAL TimeTick) bound retention.
Either limit at zero disables retention. Duplicate requests do not renew the age.
Tombstones retain neither transaction bodies nor active-session references and do
not pin LCMID. Startup recovery reconstructs native Commit results from complete
transactions in its replay range, preserving their original age. Records before
the checkpoint are not restored: this is best-effort retention, not a durable
exactly-once window. CDC keeps its source-checkpoint deduplication.

After an ambiguous native Commit attempt, a producer retry that encounters an
unavailable transaction reports an unrecoverable unknown outcome. Missing history
does not authorize replay with a new TxnID. A first, definite expiration or body-count
rejection retains the existing retry behavior, including CAS retry restrictions.

## LastConfirmedMessageID

A property attached to every ImmutableMessage. **Guarantee**: reading from this MessageID ensures all subsequent messages have TimeTick strictly greater than this message's TimeTick (including assembled transaction messages).

`lastConfirmedManager` maintains a min-heap of in-flight messages ordered by MessageID. A message can be popped from the heap (advancing `lastConfirmedMessageID`) only when **both** conditions are met:

1. **No concurrent write in-flight**: `EndTimestamp` (= `lastAllocatedTimeTick` at ack time, recording which TimeTicks were already allocated when this message finished writing) < current confirmed TimeTick — all messages allocated up to that point have been confirmed, so no smaller MessageID can still enter the heap.
2. **Txn resolved** (if transactional): the transaction is committed, rolled back, or expired — no more body messages can appear under this transaction.

When both hold, no future message can have a MessageID smaller than this entry, so it is safe to advance.

Recovery rebuilds this same heap from every unfinished transaction's Begin and
body messages, referencing the actual recovered `TxnSession`. The recovery barrier
is their conservative EndTimestamp because it fences all old-writer appends. The
initial LCMID remains the minimum of the barrier MessageID and unfinished Begin
LCMIDs; subsequent ordinary ACKs remain constrained by the reconstructed heap.
Commit, rollback, expiration and scoped abort release the recovered entries through
the normal session lifecycle. Historical entries are not republished into the
write-ahead buffer as new acknowledgements.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/timetick/` — TimeTick interceptor, `AckManager`, `lastConfirmedManager`, sync inspector
- `internal/streamingnode/server/wal/interceptors/wab/` — consumed-prefix retention and occupancy-based backpressure
- `internal/streamingnode/server/wal/interceptors/txn/` — `TxnManager`, `TxnSession`
- `internal/streamingnode/server/wal/utility/` — `ReOrderByTimeTickBuffer`
