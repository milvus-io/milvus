# TransformLog Design

TransformLog is the VChannel-level ordered transform stream owned by
`VChannelRecoveryModule`. Delete is the initial transform payload. QueryNode
and StreamingNode query resources consume the stream to advance transform
visibility.

Per-message ownership is defined by
[WAL Message Ack Design](../message_ack.md).

## 1. Ownership

```text
RecoveryStorage
  -> PChannelRecoveryManager
       +-- VChannelRecoveryModule A
       |     +-- TransformLog A
       +-- VChannelRecoveryModule B
       |     +-- TransformLog B
       +-- transformlog.StreamManager
             +-- PChannel stream
                   +-- per-VChannel subscriptions
```

TransformLog owns:

- conversion of Delete and Txn(Delete) messages into transform entries;
- an open buffer and durable object-storage chunks;
- a readable retained chunk window;
- transform SyncUp visibility;
- L0 materialization and chunk truncation state;
- one continuous component `checkpoint_time_tick`.

## 2. Persistent Metadata

Conceptually, the snapshot contains:

```proto
message VChannelTransformLogMeta {
    uint64 checkpoint_time_tick = 1;
    uint64 truncate_time_tick = 2;
    uint64 first_chunk_id = 3;
    uint64 next_chunk_id = 4;
    uint64 materialized_time_tick = 5;
}
```

The persisted TimeTicks have distinct meanings:

| Frontier | Meaning |
|---|---|
| `checkpoint_time_tick` | Complete recoverable effects of every TransformLog-relevant message through this TimeTick. All preceding Delete entries are durable in chunks and payload-free ranges are known to contain no missing transform. It is also the recovered SyncUp frontier. |
| `materialized_time_tick` | Delete payloads through this point have been emitted as L0 output. |
| `truncate_time_tick` | Earlier entries are outside the retained readable window. |

There is no separately persisted last-Delete or SyncUp frontier. The chunk id
range identifies durable chunk objects, and `checkpoint_time_tick` already
proves that all TransformLog effects through the value are recoverable. Only
the PChannel global WAL checkpoint starts recovery or truncates WAL.

## 3. Message Classification

| Kind | WAL messages | TransformLog effect |
|---|---|---|
| Payload | Delete, committed Txn containing Delete | Append one ordered Delete entry. |
| Sync-up | RecoveryBarrier, Flush, ManualFlush, FlushAll, DropPartition, DropCollection, TruncateCollection, schema-changing AlterCollection, AlterWAL | Advance transform visibility and flush preceding payload when required. |
| None | Insert and other messages | No TransformLog effect or handle. |

A committed Txn creates one entry at the outer Txn TimeTick and stores Delete
blocks for all Delete children. The outer Txn owns one TransformLog handle.

## 4. Observe

There is one Observe path for recovery and live messages:

1. classify the message;
2. return for `None`;
3. if `message.TimeTick <= checkpoint_time_tick`, return as a durable
   no-op;
4. deduplicate against the current pending queue;
5. append a Delete entry or stage a SyncUp transition;
6. clone a retained handle if chunk durability work is required;
7. expose new entries and SyncUp changes to local subscribers in WAL order;
8. schedule chunk persistence as needed.

There is no `MetaOnly`, `DataOnly`, or `MetaAndData` state.

A SyncUp message without payload still advances the stable
`checkpoint_time_tick`, but only after every earlier TransformLog payload is
durable. The live in-memory SyncUp frontier may run ahead while work is pending;
it is not an additional persisted field.

## 5. Chunk Persistence

The open buffer flushes on size pressure, an explicit sync-up requirement, or a
VChannel `PersistThrough` request.

```text
select open entries through target T
  -> write TransformLogChunk(next_chunk_id)
  -> install chunk descriptor
  -> commit continuous pending message effects
  -> advance checkpoint_time_tick when gaps permit
  -> mark stable TransformLog metadata dirty
  -> release covered retained handles
```

Chunk ids are VChannel-local and dense in
`[first_chunk_id, next_chunk_id)`. The object path is deterministic:

```text
<chunk-root>/transform-log/<pchannel>/<vchannel>/chunks/<chunk_id>.pb
```

Handle release does not wait for catalog IO. Stable metadata is installed and
marked dirty first; RecoveryStorage writes that snapshot before publishing a
global checkpoint that covers the message.

## 6. PersistThrough

```go
PersistThrough(ctx context.Context, targetTimeTick uint64)
```

The request flushes pending transform payload necessary to cover the target and
may include later entries already in the same open chunk. It is a no-op when
`checkpoint_time_tick` or an existing task already covers the target.

It does not materialize L0 merely to satisfy RecoveryStorage. Chunk durability
is the source-message completion condition.

## 7. Dirty Snapshot Publication

`ConsumeDirtySnapshots` returns an immutable clone of the stable metadata.
`MarkPersisted` advances exact persisted bookkeeping through the captured
`checkpoint_time_tick` and leaves newer changes dirty.

There are no MetaTimeTick/DataTimeTick snapshot fields. TransformLog emits one
component snapshot with one `checkpoint_time_tick`.

## 8. L0 Materialization

Materialization converts retained Delete entries into DataCoord-managed L0
deltalogs. It may be triggered by explicit sync-up policy or size pressure.

Materialization:

- does not retain source WAL messages;
- does not delay BroadcastAck;
- does not gate the global recovery checkpoint;
- updates `materialized_time_tick` in TransformLog stable metadata;
- may be retried idempotently at the logical level.

Physical duplicate L0 output after a crash is outside the WAL checkpoint
protocol and requires lifecycle idempotency or reconciliation.

## 9. Recovery

1. load TransformLog stable metadata;
2. reconstruct cold descriptors for `[first_chunk_id, next_chunk_id)`;
3. initialize `checkpoint_time_tick` and the independent materialization and
   truncation state from the snapshot;
4. receive the single PChannel replay from the global checkpoint;
5. skip relevant messages at or before `checkpoint_time_tick`;
6. append later Delete entries and SyncUp transitions with fresh handles;
7. continue into live observation after RecoveryBarrier catch-up.

Cold chunk loading validates chunk id, non-empty content, strict entry ordering,
and ordering across adjacent chunks.

## 10. Subscription

A PChannel stream supports multiple VChannel subscriptions. On creation, a
subscription captures:

```text
syncUpTarget = max(checkpoint_time_tick, open-buffer tail, live sync-up frontier)
```

Catch-up delivers entries after `StartAfterTimeTick` through the captured
target, emits `SyncUp(target)`, and then joins live delivery. Moving entries
from the open buffer into chunks never creates subscription events or changes
cursor semantics.

A start point older than `truncate_time_tick` is rejected.

## 11. GC And Defrag

TransformLog GC truncates only entries no longer required by active readers and
already protected by the persisted materialization frontier. Metadata removal
is published before old chunk objects are deleted.

Defrag may merge small TransformLog chunks and atomically replace their object
references. It cannot alter entry TimeTicks, subscription cursors, component
`checkpoint_time_tick`, or the PChannel global checkpoint.

## 12. Invariants

1. TransformLog is VChannel-owned.
2. All entry positions use source WAL TimeTick.
3. `checkpoint_time_tick` is a continuous TransformLog-relevant prefix.
4. A Delete handle releases only after covering chunk durability and dirty
   stable metadata installation.
5. Payload-free SyncUp visibility is represented by `checkpoint_time_tick`
   before the global checkpoint may pass it.
6. L0 materialization does not gate source-message Ack.
7. Catalog metadata stops referencing a chunk before its object is deleted.
