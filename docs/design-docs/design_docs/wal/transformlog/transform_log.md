# TransformLog Design

This document describes the current vchannel-level TransformLog design in
RecoveryStorage. It replaces the older design that modeled TransformLog as an
independent RecoveryStorage module beside vchannel and segment modules.

TransformLog is now a component owned by `VChannelRecoveryModule`. A
`PChannelRecoveryManager` owns all vchannel modules on one PChannel and exposes
the PChannel-level TransformLog stream manager.

Per-message persistence completion and checkpoint semantics are defined by
[WAL Message Ack Design](../message_ack.md).

## 1. TransformLog Definition

TransformLog is the vchannel-level ordered log of transform events used by both
QueryNode and StreamingNode query resources:

- QueryNode consumes it to advance sealed-segment transform visibility.
- StreamingNode growing runtime consumes a bounded TransformLog suffix while
  building a query-resource WAL view, so recovered growing segments can catch up
  with Deletes that happened after the segment snapshot start point and before
  the view's base transform TimeTick.

The first transform payload is Delete. Messages that do not carry a transform
payload can still advance transform visibility, but they are represented as
volatile `SyncUp` frontier updates rather than durable TransformLog entries.

TransformLog has four responsibilities:

1. Convert Delete WAL messages into durable `TransformLogEntry` objects.
2. Keep a readable retained window over flushed chunks plus the open buffer.
3. Maintain a volatile transform frontier for MVCC sync-up when no Delete entry
   exists at a barrier TimeTick.
4. Feed TransformLog subscriptions through a PChannel stream with per-vchannel
   subscriptions for QueryNode sealed-segment transforms and StreamingNode
   growing-segment delete replay.

### Ownership

```text
RecoveryStorage
  |
  v
PChannelRecoveryManager
  |
  +-- VChannelRecoveryModule(vchannel A)
  |     +-- VChannelView
  |     +-- SegmentView(s)
  |     +-- TransformLog
  |
  +-- VChannelRecoveryModule(vchannel B)
  |     +-- ...
  |
  +-- transformlog.StreamManager
        +-- pchannel-level TransformLogStream
              +-- per-vchannel subscriptions
```

`VChannelRecoveryModule.ObserveMessage` receives one Retained dispatch handle
for a single vchannel. It first handles vchannel metadata and segment state,
then forwards that same handle to `TransformLog.ObserveMessage`.

`PChannelRecoveryManager` owns the vchannel index and registers active
vchannels into `transformlog.StreamManager`. QueryNode accesses TransformLog
through `wal.TransformLogStreamManager.AcquireStream(pchannel)`, then subscribes
to one or more vchannels on that stream.

### Entry Model

All TransformLog positions use the source WAL message TimeTick. There is no
separate transform TimeTick.

```proto
message TransformDeleteBlock {
    int64 partition_id = 1;
    schema.IDs primary_keys = 2;
}

message TransformDeleteEntry {
    repeated TransformDeleteBlock blocks = 1;
}

message TransformLogEntry {
    uint64 time_tick = 1;

    oneof entry {
        TransformDeleteEntry delete = 2;
    }
}

message TransformSubscriptionSyncUp {
    int64 subscription_id = 1;
    string vchannel = 2;
    uint64 time_tick = 3;
}

message TransformLogChunk {
    uint64 chunk_id = 1;
    repeated TransformLogEntry entries = 2;
}

message VChannelTransformLogMeta {
    uint64 checkpoint_time_tick = 1;
    uint64 truncate_time_tick = 2;
    uint64 first_chunk_id = 3;
    uint64 next_chunk_id = 4;
    uint64 materialized_time_tick = 5;
}
```

Message sources are classified by
`messageutil.ClassifyTransformLogMessage`:

| Kind | WAL messages | TransformLog effect |
|---|---|---|
| Payload transform | Delete, committed Txn containing Delete | `delete` |
| Sync-up barrier | CreateCollection, RecoveryBarrier, Flush, ManualFlush, FlushAll, DropPartition, DropCollection, TruncateCollection, schema-changing AlterCollection, AlterWAL | volatile `SyncUp` frontier |
| None | Insert and other messages | no effect |

A committed Txn is treated as one atomic WAL message. If it contains Deletes,
TransformLog creates one `delete` entry at the Txn TimeTick, using Delete blocks
for the Delete bodies inside the Txn.

### TimeTick Frontiers

| Frontier | Meaning |
|---|---|
| `entry.time_tick` | WAL TimeTick and transform MVCC TimeTick for one Delete entry. |
| `checkpoint_time_tick` | Delete entries up to this TimeTick are durably written into TransformLog chunks and published in in-memory meta. It is not a barrier frontier. |
| `sync_up_time_tick` | Volatile in-memory transform frontier. It advances on Delete entries and sync-up barrier messages and is delivered to subscribers through `SyncUp(TimeTick)`. |
| `persistedDataTimeTick` | Persisted `checkpoint_time_tick`; it is used by TransformLog recovery and cleanup logic. |
| `materialized_time_tick` | Delete entries up to this TimeTick have been emitted as L0 deltalog output and published in in-memory meta. |
| `persistedMaterialized` | Persisted `materialized_time_tick`, used by TransformLog GC. |
| `truncate_time_tick` | Entries with `time_tick <= truncate_time_tick` are outside the readable retained window. A subscription starting before this point is invalid. |

## 2. Writing, Persistence, And Recovery

### Observe And Append

`TransformLog.ObserveMessage` only works after the owning
`VChannelRecoveryModule` switches into MetaAndData mode. Recovery replay before
that mode may rebuild in-memory state, but it does not publish new dirty
TransformLog state.

For every observed WAL message:

1. Classify the message using `ClassifyTransformLogMessage`.
2. Ignore `TransformLogKindNone`.
3. For Delete or Txn(Delete), build one `delete` entry.
4. For sync-up barrier messages, advance the volatile `sync_up_time_tick`.
5. Skip a Delete message if its TimeTick is already covered by
   `checkpoint_time_tick` or the open buffer tail.
6. Append Delete entries to the open buffer.
7. Notify local scanners and the PChannel stream manager.
8. Call `Clone()` on the Retained dispatch handle and attach the returned
   retained immutable message to the
   entry or barrier before exposing required asynchronous flush work.
9. Submit flush or materialization tasks when the message requires them.
10. Return after synchronous observation. Metadata is exposed later through
    `ConsumeDirtySnapshots`; data-side completion is represented only by the
    retained message handles.

The Delete append step is immediately readable by live subscriptions. It is not
object-storage durable until a TransformLog chunk is written. It is not
recoverably published until the corresponding meta is persisted to etcd. A
sync-up barrier is immediately visible to live subscriptions as a
`SyncUp(TimeTick)` event, but it is not written into chunks and does not advance
`checkpoint_time_tick` when there is no preceding payload to flush.

### Chunk Flush

The open buffer is the mutable tail of the TransformLog. It flushes when either:

- row count reaches the configured threshold; or
- a sync-up message needs preceding Delete entries to become durable before
  materialization can run.

Flush is an asynchronous scheduler task. It serializes with other TransformLog
flush and materialization tasks through task preconditions.

Flush flow:

```text
open buffer entries up to target T
  -> TransformLogChunk{chunk_id = next_chunk_id, entries}
  -> Store.WriteTransformLogChunk(vchannel, chunk)
  -> append loaded chunk descriptor
  -> discard flushed entries from buffer
  -> advance next_chunk_id
  -> advance checkpoint_time_tick to the last flushed Delete entry TimeTick
  -> mark TransformLog dirty
  -> release retained message handles contained in the committed chunk
  -> RecoveryStorage persists VChannelTransformLogMeta
  -> MarkSnapshotPersisted advances persistedDataTimeTick
```

Handle release does not wait for `MarkSnapshotPersisted`: object data completion
does not wait for catalog IO. Before releasing covered handles, the flush task
commits the updated TransformLog state and marks it dirty. A persist batch then
captures that metadata and writes it before the batch checkpoint.

Chunk object path is deterministic:

```text
<chunk-root>/transform-log/<pchannel>/<vchannel>/chunks/<chunk_id>.pb
```

Chunk ids are vchannel-local and dense in
`[first_chunk_id, next_chunk_id)`. `first_chunk_id` and `next_chunk_id` are the
only chunk range stored in catalog meta. Per-chunk paths and time ranges are not
persisted in catalog.

### Dirty Snapshot Publication

`VChannelRecoveryModule.ConsumeDirtySnapshots` exports TransformLog meta as a
`ModuleNameTransformLog` dirty snapshot keyed by `{PChannel, VChannel}`.

For TransformLog snapshots:

- `MetaTimeTick = checkpoint_time_tick`
- `DataTimeTick = checkpoint_time_tick`
- payload is `VChannelTransformLogMeta`
- `MarkPersisted` updates both `persistedDataTimeTick` and
  `persistedMaterialized` from the persisted snapshot

RecoveryStorage freezes the batch boundary before consuming this snapshot. A
batch whose DataPoint covers a TransformLog message therefore satisfies both
conditions:

```text
TransformLog message Ack is complete
AND its DirtySnapshot is persisted before the batch checkpoint
```

This prevents WAL checkpoint publication from passing a Delete whose object
data or recovery metadata is incomplete.

### L0 Materialization

TransformLog subscription and L0 materialization are separate outputs.
Subscription reads TransformLog entries directly. L0 materialization converts
retained Delete entries into DataCoord-managed L0 deltalogs for compatibility
with sealed data pipelines.

Materialization is triggered by:

- explicit sync-up messages: DropCollection, ManualFlush, FlushAll;
- size pressure after a flush, based on pending Delete rows or bytes.

Materialization has a sync frontier precondition:

```text
LatestTransformTimeTick() >= target materialize TimeTick
```

The scheduler serializes materialization after preceding TransformLog flush
tasks. Materialization scans Delete entries in the retained window and ignores
sync-up frontier updates because they are not payload entries.

Materialization does not retain a message handle. BroadcastAck and
RecoveryStorage Data checkpoint advancement do not wait for
`materialized_time_tick`.

Materialization flow:

```text
entries in (materialized_time_tick, target]
  -> filter delete entries
  -> group by partition and primary-key type
  -> split by row or byte limit
  -> allocate one L0 segment per group
  -> write one deltalog per L0 segment through syncmgr
  -> update materialized_time_tick
  -> mark TransformLog dirty
  -> RecoveryStorage persists VChannelTransformLogMeta
  -> MarkSnapshotPersisted advances persistedMaterialized
```

Sync-up barriers do not create L0 deltalog files. They can still advance
`materialized_time_tick`, which records that all Delete payloads up to the
sync-up target have been emitted or proven absent.

The current materialization recovery cursor is only `materialized_time_tick`.
If StreamingNode crashes after L0 files are accepted but before the updated
TransformLog meta is persisted, the same range may be materialized again after
recovery. Batch-level idempotency is outside this design.

### Recovery

During RecoveryStorage startup:

1. RecoveryStorage loads `VChannelTransformLogMeta` records with the other
   vchannel and segment recovery metadata.
2. `PChannelRecoveryManager` creates one `VChannelRecoveryModule` for each
   vchannel discovered from vchannel meta, segment meta, segment data-version
   summary, or transform-log meta.
3. Each `VChannelRecoveryModule` constructs a `TransformLog` from its
   `VChannelTransformLogMeta`.
4. TransformLog reconstructs cold chunk descriptors for the dense chunk id range
   `[first_chunk_id, next_chunk_id)`.
5. `persistedDataTimeTick` is initialized from `checkpoint_time_tick`.
6. `persistedMaterialized` is initialized from `materialized_time_tick`.
7. WAL replay resumes from the recovered WAL checkpoint.
8. Replayed Delete messages with TimeTick already covered by
   `checkpoint_time_tick` are skipped.
9. Replayed sync-up barrier messages advance volatile `sync_up_time_tick` again.
10. Delete messages after `checkpoint_time_tick` append to the open buffer,
    retain fresh message handles, and are flushed again before the Ack completed
    frontier can pass them.

Cold chunks are loaded on demand by subscription, materialization, or truncation.
Loading validates:

- chunk id matches the expected id;
- chunk is non-empty;
- entries are strictly ordered by `time_tick`;
- adjacent loaded chunks remain ordered.

A missing or corrupt retained chunk makes the retained TransformLog incomplete.
The vchannel cannot safely serve a subscription that needs that chunk.

## 3. TransformLog Subscription

TransformLog subscription is a PChannel-level stream with per-vchannel
subscriptions.

### Server-Side Stream

`PChannelRecoveryManager.AcquireStream(ctx, pchannel)` delegates to
`transformlog.StreamManager`. The stream validates the PChannel, then supports
multiple subscription requests:

```text
AcquireStream(pchannel)
  -> Subscribe(vchannel A, start_after)
  -> Subscribe(vchannel B, start_after)
  -> ...
```

The stream manager registers only active vchannels. If a vchannel is removed or
becomes inactive, the stream manager removes its TransformLog and notifies live
subscriptions with `ErrTransformLogVChannelUnavailable`.

### Subscription Creation

Each subscription requires:

- a non-empty vchannel;
- a handler;
- `StartAfterTimeTick >= truncate_time_tick`;
- an optional `EndTimeTick`.

On create, the server captures `syncUpTarget` from the TransformLog's latest
readable TimeTick:

```text
syncUpTarget = max(checkpoint_time_tick, open_buffer_tail_time_tick, sync_up_time_tick)
```

If `EndTimeTick` is set and smaller than this target, the target is capped by
`EndTimeTick`.

### Catch-Up And Live Delivery

Subscription delivery has two phases:

1. **Catch-up**: a worker reads Delete entries with
   `entry.time_tick > StartAfterTimeTick` until it reaches the captured
   `syncUpTarget` or `EndTimeTick`, then emits `SyncUp(syncUpTarget)`.
2. **Live**: after a `SyncUp` event is delivered, the subscription is attached
   to live dispatch and receives future Delete entries and future `SyncUp`
   events as the TransformLog advances.

`SyncUp(TimeTick=T)` means all TransformLog payload entries with
`entry.time_tick <= T` have been delivered to that subscription. It is the
TransformLog-level signal used to align transformMVCC when there is no Delete
entry at `T`. `SyncUp` is not persisted in chunks and may be emitted during both
initial catch-up and live dispatch.

Flush does not produce subscription events. Moving entries from open buffer to
chunks must not create gaps or duplicates because subscription cursors are based
only on TransformLog entry TimeTick.

### Local And Remote Access

The WAL-level interface is:

```go
type TransformLogStreamManager interface {
    AcquireStream(ctx context.Context, pchannel string) (TransformLogStream, error)
}

type TransformLogStream interface {
    Subscribe(ctx context.Context, opt TransformLogSubscriptionOption) (TransformLogSubscription, error)
    Done() <-chan struct{}
    Error() error
    Close() error
}
```

The distributed access layer wraps the raw handler-client stream with a
resumable stream:

```text
distributed TransformLog stream
  -> acquire local WAL TransformLog stream when PChannel is local
  -> otherwise open remote StreamingNode SubscribeTransform stream
  -> if the underlying stream breaks, reacquire and resubscribe
```

Each resumable subscription records the last delivered
`TransformLogEntry.time_tick` as `nextStartAfter`. Re-subscription is exclusive:
after entry `T` has been forwarded, the next attempt starts after `T`.

Non-retryable semantic errors are delivered to the subscription handler. These
include invalid options, unavailable vchannel, and start point older than
`truncate_time_tick`.

## 4. TransformLog GC

TransformLog GC controls retained chunk count and object-storage usage. It is
based on the minimum transform start point still required by QueryViews and
DataViews that may need to subscribe from this StreamingNode, and on the
materialization frontier that protects L0 recovery.

### Retention Rule

For each vchannel:

```text
required_start_after =
    min(transform_start_after_timetick required by active QueryViews/DataViews)

truncate_target =
    min(required_start_after, persistedMaterialized)
```

If there is no active QueryView/DataView requirement for the vchannel,
`required_start_after` is treated as unbounded, so `persistedMaterialized`
becomes the truncation limit.

Entries with `time_tick <= truncate_target` are no longer required by active
readers and have already been materialized into persisted L0 output. They may be
removed from the retained TransformLog window. A new subscription with
`StartAfterTimeTick < truncate_time_tick` must fail with
`ErrTransformLogStartPointTruncated`.

`checkpoint_time_tick` alone is not a safe GC bound. It only proves that
TransformLog chunks are durable. The retained entries may still be needed to
rebuild L0 output if materialization has not been persisted.

### Truncation Procedure

TransformLog truncation updates metadata before deleting objects:

```text
input truncate target T
  -> if T <= current truncate_time_tick: no-op
  -> set truncate_time_tick = T
  -> load cold leading chunks until their end TimeTick is known
  -> remove leading chunk descriptors with to_time_tick <= T
  -> advance first_chunk_id
  -> mark TransformLog dirty
  -> RecoveryStorage persists VChannelTransformLogMeta
  -> after meta persistence succeeds, delete old chunk objects asynchronously
```

Object deletion must happen after catalog meta is persisted. Deleting chunk
objects first can leave recovered meta pointing at missing retained chunks after
a crash.

Truncation does not move `checkpoint_time_tick` backward and does not directly
advance the RecoveryStorage data checkpoint.

### Current Implementation Boundary

The `TransformLog` type already contains the local truncation primitive that:

- advances `truncate_time_tick`;
- loads cold leading chunks when their end TimeTick is unknown;
- drops in-memory descriptors for chunks fully covered by the truncate point;
- advances `first_chunk_id`;
- marks TransformLog meta dirty.

The PChannel-level GC driver and public vchannel truncation API still need to be
wired through `PChannelRecoveryManager` / `VChannelRecoveryModule`. That driver
should compute the retention rule above from QueryView/DataView requirements and
invoke the TransformLog truncation primitive. It should also schedule
post-persistence object deletion for chunks whose ids are below the persisted
`first_chunk_id`.

### Safety Invariants

1. A subscription can only start from a point inside the retained window.
2. Chunk metadata is removed from catalog before the corresponding objects are
   deleted.
3. Recovery reconstructs only chunks in `[first_chunk_id, next_chunk_id)`.
4. Materialization must not request entries before `truncate_time_tick`.
5. TransformLog GC is independent of subscription progress. QueryNode does not
   ack consumption back to StreamingNode.
