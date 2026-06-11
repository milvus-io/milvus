# TransformLog View Module

> This document describes the target design for vchannel-level TransformLog
> storage and subscription used by QueryViews.
>
> References: [WAL Recovery Architecture](wal-recovery-architecture.md),
> [Message Workflow](message-workflow.md),
> [Distributed Query View Design](../qviews/README.md),
> [QueryView State Machine](../qviews/query_view_state_machine.md),
> [Syncer Design](../qviews/syncer.md), [streaming.proto](../../../../pkg/proto/streaming.proto),
> [view.proto](../../../../pkg/proto/view.proto).

## 1. Goal

QueryViews move sealed-segment transform application from StreamingNode to
QueryNode.

StreamingNode consumes WAL Delete messages and stores them in a vchannel-level
TransformLog owned by an independent RecoveryStorage module. QueryNode
subscribes once per vchannel when it first loads a QueryView for that vchannel,
buffers TransformLog entries locally, and applies them to local sealed segments.

The first transform type is Delete. The design keeps the protocol extensible,
but this document only defines Delete behavior.

## 2. Core Constraints

1. **TransformLog is the source for QueryNode subscription**. QueryNode
   subscribes to TransformLog entries, not WAL records and not L0 segments.
2. **One TimeTick semantic**. `TransformLogEntry.time_tick` is the WAL Delete
   TimeTick and the MVCC TimeTick. There is no separate data/effect timetick.
3. **Transform protocol emits transform entries**. SubscribeTransform does not
   expose WAL message id, last-confirmed id, transaction context, WAL
   properties, or WAL scanner internals.
4. **Subscription is vchannel-level**. One QueryNode keeps one local transform
   buffer per vchannel. Individual sealed segments do not create upstream
   subscriptions.
5. **QueryView start point is transform-based**. The QueryView field formerly
   named `delete_apply_start_after_timetick` should become
   `transform_start_after_timetick`. It is an exclusive TransformLog TimeTick
   start point.
6. **QueryView start points on the same QueryNode/vchannel are monotonic**.
   Later QueryViews on the same QueryNode for the same vchannel use a
   `transform_start_after_timetick` greater than or equal to the previous one.
   Therefore normal QueryView iteration never needs a new upstream subscription
   or server-side refresh.
7. **Caught-up is only a subscription barrier**. It proves the initial retained
   suffix after the requested start point has been sent and the live stream is
   attached. It is not a TimeTick value.
8. **No server-side subscription ack**. QueryNode progress does not ack
   subscriptions back to StreamingNode. TransformLog truncation is driven by an
   explicit storage truncation interface, not by subscription acks.
9. **TransformLog is an independent RecoveryStorage module**. RecoveryStorage
   owns module dispatch and checkpoint/barrier aggregation. `VChannelModule`,
   `SegmentModule`, and `TransformLogModule` own their own metadata,
   tombstones, dirty snapshots, and persistence tasks.
10. **TransformLog is part of RecoveryStorage data persistence**. It
    contributes a DataBarrier so the RecoveryStorage data checkpoint cannot pass
    a Delete whose TransformLog entry is not durably published.
11. **L0 segments are independent output**. L0 materialization is how
    TransformLog interacts with DataCoord/Coordinator, but it is not the
    QueryNode subscription replay source. `DropCollection`, `ManualFlush`, and
    `FlushAll` additionally wait for L0 materialization through the
    materialized frontier.
12. **Cross-module reads are intentionally narrow**. `SegmentModule` may read
    `SchemaAt(vchannel, partitionID, timetick)` from `VChannelModule` when
    creating segment state. `TransformLogModule` does not read VChannel or
    Segment state; Delete validity is guaranteed by WAL write-time checks and
    WAL message ordering.

## 3. Architecture

```text
WAL Delete / Txn(Delete)
    |
    v
RecoveryStorage
    |
    +---------------------> VChannelModule
    |                           |
    |                           +-- SchemaAt(...) read-only view
    |
    +---------------------> SegmentModule
    |
    +---------------------> TransformLogModule
                                |
                                +---------------------> SubscribeTransform
                                |                           |
                                |                           v
                                |                   QN vchannel transform buffer
                                |                           |
                                |                           v
                                |                   QN sealed segment Delete apply
                                |
                                v
                        L0 materialization
                                |
                                v
                        DataCoord / Coordinator
```

The three workflows are independent and must not be collapsed into one progress
model:

1. TransformLog persistence and recovery.
2. TransformLog subscription implementation.
3. TransformLog output to DataCoord/Coordinator through L0 segments.

## 4. TimeTick Model

All TransformLog positions live on the same vchannel WAL TimeTick timeline.

| Name | Meaning |
| --- | --- |
| `entry.time_tick` | TimeTick of the WAL Delete message, also the Delete MVCC TimeTick. |
| `checkpoint_time_tick` | TransformLog entries up to this TimeTick are durably published in recovery catalog. |
| `transform_start_after_timetick` | QueryView/DataView exclusive consumption start. QueryNode consumes entries with `entry.time_tick > S`. |
| `truncate_time_tick` | Entries with `entry.time_tick <= truncate_time_tick` may be removed from StreamingNode TransformLog storage. Requests must start at or after this point. |
| `materialized_time_tick` | L0 materializer has emitted TransformLog entries up to this TimeTick. |
| `caught_up` | Subscription barrier, not a TimeTick. |

```
single vchannel TransformLog timeline

        truncated TransformLog            retained TransformLog window
... ---------------+-------------------------------------------------->
                   |
                   T = truncate_time_tick

retained entries:
                   11      18      23      31      45      52
                   |       |       |       |       |       |
                   D       D       D       D       D       D
                   Delete  Delete  Delete  Delete  Delete  Delete

                         M = materialized_time_tick
                                  C = checkpoint_time_tick
                                         S = transform_start_after_timetick
```

Meanings on the same timeline:

- `T`: `start_after < T` cannot be served without loss.
- `M`: L0 output has consumed entries `<= M`.
- `C`: TransformLog entries `<= C` are protected by recovery catalog state.
- `S`: a QueryView or local QueryNode consumption point; consume entries `> S`.

## 5. Data Model

### 5.1 Transform Start Point

`transform_start_after_timetick` is exclusive:

```text
subscription start = S
SN sends entries with entry.time_tick > S
QN applies entries with entry.time_tick > S
```

The field is inherited from DataView into QueryView when the view is generated.
For the current Delete-only implementation, it means all Delete entries with
`time_tick > transform_start_after_timetick` must be applied to sealed segments
covered by the view.

### 5.2 Transform Delete Block

`TransformDeleteBlock` contains only the Delete payload scoped by the enclosing
entry's TimeTick.

```proto
message TransformDeleteBlock {
    int64 partition_id = 1;
    schema.IDs primary_keys = 2;
}
```

### 5.3 Transform Log Entry

One TransformLog entry corresponds to one WAL Delete message. Transactions are
handled as one atomic WAL message and therefore one TransformLog entry.

```proto
message TransformDeleteEntry {
    repeated TransformDeleteBlock blocks = 1;
}

message TransformLogEntry {
    uint64 time_tick = 1;

    oneof entry {
        TransformDeleteEntry delete = 2;
    }
}
```

### 5.4 Transform Log Chunk

Chunks are the object-storage write unit. A chunk file is non-empty and contains
entries ordered by `time_tick`.

```proto
message TransformLogChunk {
    uint64 chunk_id = 1;
    repeated TransformLogEntry entries = 2;
}
```

Chunk-level `from_time_tick` and `to_time_tick` are intentionally omitted. They
are derived from `entries[0].time_tick` and
`entries[len(entries)-1].time_tick` when needed.

### 5.5 Transform Log Meta

`VChannelTransformLogMeta` is stored as an independent RecoveryStorage catalog
record under the pchannel transform-log namespace. It stores one small durable
entry point for the vchannel TransformLog; it does not store per-chunk paths or
per-chunk time ranges and is not embedded in `VChannelMeta`.

```proto
message VChannelTransformLogMeta {
    // TransformLog durable frontier published in recovery catalog.
    uint64 checkpoint_time_tick = 1;

    // Entries with time_tick <= truncate_time_tick may be removed.
    uint64 truncate_time_tick = 2;

    // Retained dense chunk id range: [first_chunk_id, next_chunk_id).
    uint64 first_chunk_id = 3;
    uint64 next_chunk_id = 4;

    // L0 materializer cursor.
    uint64 materialized_time_tick = 5;
}
```

Chunk path is deterministic from `(pchannel, vchannel, chunk_id)`:

```text
transform-log/<pchannel>/<vchannel>/chunks/<chunk_id>.pb
```

Chunk ids are dense and vchannel-local:

```text
first_chunk_id = 0
next_chunk_id  = 3

chunk 0: [ 11, 18, 23 ]
chunk 1: [ 31, 45 ]
chunk 2: [ 52 ]
```

No manifest or per-chunk catalog key is part of the core design. Most recovery
and subscription startup paths need to read all retained chunks anyway, so a
separate range index is not required. The important control is timely
truncation, which keeps `[first_chunk_id, next_chunk_id)` bounded.

## 6. TransformLog Storage

TransformLog follows the same checkpoint publication model as growing segment
data: object data can be written first, but RecoveryStorage data checkpoint
advances only after the corresponding recovery catalog meta has been persisted.

### 6.1 Ownership

RecoveryStorage owns the module list, WAL dispatch, and checkpoint/barrier
aggregation. Runtime ownership is split into three independent modules:

- `VChannelModule` owns vchannel metadata only: collection, partition, schema,
  lifecycle state, vchannel tombstones, dirty snapshots, and meta checkpoint.
- `SegmentModule` owns Insert persistence state, segment assignment metadata,
  segment tombstones, L1 output, and segment data checkpoint.
- `TransformLogModule` owns Delete persistence state, transform-log chunks,
  retained chunk replay, scanner fanout, transform-log meta, transform-log
  tombstones, and delete data checkpoint.

`VChannelModule` is not the Delete buffer owner, subscription service, or
public truncation API. `TransformLogModule` consumes Delete and Txn(Delete)
messages directly from RecoveryStorage, handles each Txn as one atomic WAL
message, and lets TransformLog meta persistence advance the delete DataBarrier.

`SegmentModule` has the only required cross-module read: when observing
`CreateSegment`, it obtains the historical schema through
`SchemaAt(vchannel, partitionID, timetick)`. Tombstone finalize and cleanup are
module-local responsibilities. VChannel tombstones do not wait for Segment or
TransformLog tombstones, and TransformLog tombstones do not inspect VChannel or
Segment state.

### 6.2 Runtime State

```text
TransformLogModule
  meta                       // VChannelTransformLogMeta in memory
  persistedCheckpointTimeTick // catalog-persisted checkpoint frontier
  persistedMaterializedTimeTick // catalog-persisted L0 materialized frontier
  pending                    // in-memory entries not yet handed to a flush task
  pendingFlushChunks          // chunks handed to pending/running flush tasks
  pendingTasks                // unfinished flush/materialization/truncate cleanup tasks
  dirty                       // meta changed and must be persisted
```

`meta.checkpoint_time_tick` records the newest TransformLog TimeTick already
written to object storage and published in in-memory meta.
`persistedCheckpointTimeTick` records the newest TransformLog TimeTick whose
meta has been persisted to the recovery catalog. The DataBarrier exposes
`persistedCheckpointTimeTick`.

`meta.materialized_time_tick` records the newest TransformLog TimeTick whose
entries have been emitted as DataCoord-managed L0 segments. The materialized
frontier exposes `persistedMaterializedTimeTick`, so a synchronous flush/drop
ack only completes after the L0 commit and the TransformLog meta update have
both been persisted.

### 6.3 Append And Chunk Flush

```text
RecoveryStorage data stage observes WAL Delete@T
  -> TransformLogModule observes the message
  -> TransformLog.Append(message@T)
  -> append entry to pending buffer
  -> flush policy moves pending into pendingFlushChunks
  -> write TransformLogChunk object using next_chunk_id
  -> update in-memory meta:
       checkpoint_time_tick = chunk.last_entry.time_tick
       next_chunk_id++
       dirty = true
  -> require recovery catalog persist
```

`Append` is not durable by itself. The durable object-storage unit is a chunk,
and the RecoveryStorage checkpoint-visible unit is the recovery catalog meta
that publishes that chunk.

Flush is normally triggered by size, such as entry count, row count, or bytes.
It may also be triggered by checkpoint pressure or vchannel lifecycle barriers
so a low-rate Delete stream does not block RecoveryStorage checkpoint forever.

### 6.4 Publish Order And DataBarrier

The publication order is a hard invariant:

```text
1. pending entries become a TransformLogChunk object.
2. chunk object write succeeds.
3. in-memory TransformLogMeta publishes next_chunk_id and checkpoint_time_tick.
4. TransformLogModule exposes a TransformLog DirtySnapshot.
5. RecoveryStorage persists TransformLogMeta under the independent transform-log key.
6. DirtySnapshot.MarkPersisted advances the persisted TransformLog checkpoint.
7. TransformLog DataBarrier advances.
```

Therefore:

```text
TransformLog DataBarrier = persistedCheckpointTimeTick
```

RecoveryStorage data checkpoint cannot pass Delete@T until TransformLog's
DataBarrier is at least T.

### 6.5 L0 Materialization

L0 materialization has a separate progress cursor and does not change
TransformLog subscription durability.

```text
TransformLog checkpoint_time_tick  -> chunk/recovery/subscription durability
TransformLog materialized_time_tick -> L0 Segment output committed to DataCoord
```

`FlushTo(T)` and `MaterializeTo(T)` are separate operations:

- `FlushTo(T)` makes TransformLog entries up to `T` durable in TransformLog
  chunks and advances `checkpoint_time_tick` after catalog persistence.
- `MaterializeTo(T)` reads retained entries in
  `(materialized_time_tick, T]`, writes L0 deltalog files, commits L0 segments to
  DataCoord, and advances `materialized_time_tick` after catalog persistence.

Materialization requires TransformLog chunk durability first. A materialization
task for target `T` has a precondition that the same vchannel's durable
frontier has reached `T`.

The materializer groups Delete blocks by `(vchannel, partitionID)` and writes
one or more L0 segments per group. Each L0 Segment owns exactly one deltalog
file. If a selected range must be split by row or size limit, the materializer
creates multiple L0 segments instead of attaching multiple deltalogs to one L0
Segment. The collection id is a vchannel-level property and is obtained from
the owning TransformLog, not from every Delete block.

```text
retained TransformLog entries
  -> select entries with materialized_time_tick < entry.time_tick <= T
  -> group Delete blocks by partitionID
  -> build storage.DeleteData with primary keys and entry.time_tick
  -> allocate L0 segment ids
  -> write one deltalog per L0 segment through the existing syncmgr / pack-writer path
  -> SaveBinlogPaths(SegLevel_L0, Flushed=true)
  -> update meta.materialized_time_tick = T
  -> mark TransformLog DirtySnapshot
  -> RecoveryStorage persists TransformLogMeta
  -> DirtySnapshot.MarkPersisted publishes the materialized frontier
```

The completion point for synchronous flush/drop messages is:

```text
DataCoord has accepted all L0 segments for the target range
AND TransformLogMeta.materialized_time_tick >= target timetick is persisted
```

It does not wait for L0 compaction and does not wait for QueryNode
subscription consumption.

The first implementation uses only `materialized_time_tick` as the recovery
cursor. It does not persist a pending materialization batch. If StreamingNode
crashes after `SaveBinlogPaths` succeeds but before
`materialized_time_tick` is persisted, recovery may materialize the same range
again. That idempotency gap is outside this batch and should be addressed by a
later batch-level idempotency design.

### 6.6 Crash Rules

| Crash point | Recovery behavior |
| --- | --- |
| Before entry enters pending buffer | RecoveryStorage checkpoint has not passed the Delete; WAL replay appends it again. |
| Entry is only in pending or pendingFlushChunks | Not visible after recovery; WAL replay appends it again. |
| Chunk object is written but meta is not persisted | Chunk is orphaned and ignored by recovery; WAL replay appends entries again. |
| In-memory meta changed but catalog persist did not complete | Recovery uses old meta; WAL replay appends entries again. |
| Catalog persist completed | `persistedCheckpointTimeTick` can advance; RecoveryStorage checkpoint may pass the Delete. |

TransformLog append is idempotent by `(vchannel, time_tick)`. If a replayed
Delete has `time_tick <= checkpoint_time_tick`, it has already been published
by TransformLog and can be skipped. If the same `time_tick` appears with
different Delete content, it is a recovery consistency error.

L0 materialization recovery is based on `materialized_time_tick`: entries with
`time_tick <= materialized_time_tick` are considered already emitted to
DataCoord; entries after it are eligible for future materialization.

### 6.7 Recovery

On StreamingNode recovery:

1. RecoveryStorage loads `VChannelMeta`, `SegmentAssignmentMeta`, and
   independent `VChannelTransformLogMeta` records.
2. RecoveryStorage constructs `VChannelModule`, `SegmentModule`, and
   `TransformLogModule` from their own snapshots. `TransformLogModule` creates
   one TransformLog per recovered vchannel from transform-log meta keys.
3. TransformLog reads all deterministic chunk files in
   `[first_chunk_id, next_chunk_id)`.
4. TransformLog validates each chunk:
   - `chunk.chunk_id` matches the expected id;
   - chunk is non-empty;
   - entries are ordered by `time_tick`;
   - concatenated chunks are ordered by `time_tick`.
5. TransformLog reconstructs its in-memory retained entries and local chunk
   state.
6. `persistedCheckpointTimeTick` is initialized from
   `meta.checkpoint_time_tick`.
7. `persistedMaterializedTimeTick` is initialized from
   `meta.materialized_time_tick`.
8. RecoveryStorage resumes WAL consumption after its recovered checkpoint.
9. Replayed Delete messages are appended idempotently.

If a chunk inside `[first_chunk_id, next_chunk_id)` is missing or corrupt, the
retained TransformLog is incomplete and the vchannel cannot serve safely. Chunks
outside this range are not part of recovered TransformLog and may be removed by
asynchronous GC.

### 6.8 Truncate

Truncation controls the number of retained object-storage files and therefore
is more important than a per-chunk catalog index.

StreamingNode TransformLog truncation is driven by QueryView/DataView
requirements through an explicit storage truncation interface:

```text
truncate_time_tick =
    min(transform_start_after_timetick required by active QueryViews/DataViews)
```

Because retained chunks are loaded in memory, TransformLog can decide truncation
from chunk contents:

```text
for chunk in retained chunks ordered by chunk_id:
    if chunk.entries[last].time_tick <= truncate_time_tick:
        drop chunk from memory
        advance first_chunk_id
    else:
        stop
```

Truncation publish order:

```text
1. Advance in-memory truncate_time_tick and first_chunk_id.
2. Mark a TransformLog DirtySnapshot.
3. RecoveryStorage persists independent TransformLogMeta.
4. After meta publish succeeds, asynchronously delete old chunk objects.
```

The object deletion must happen after meta publish. Deleting objects first can
leave recovery catalog meta pointing at missing retained chunk files after a
crash.

Truncation does not move `checkpoint_time_tick` backward and does not affect
RecoveryStorage data checkpoint progress.

## 7. ModuleAPI Implementation

`TransformLogModule` implements the core `Module` API, data checkpoint/frontier
views, checkpoint persisted notification, and the TransformLog provider
interface:

```go
type TransformLogModule struct {
    logs map[string]TransformLog
}

var _ moduleapi.Module = (*TransformLogModule)(nil)
var _ moduleapi.DataCheckpointView = (*TransformLogModule)(nil)
var _ moduleapi.DataFrontierView = (*TransformLogModule)(nil)
var _ moduleapi.CheckpointPersistedObserver = (*TransformLogModule)(nil)
var _ TransformLogProvider = (*TransformLogModule)(nil)
```

### Module.Name

Returns `ModuleNameTransformLog`.

### Module.ObserveMessage

`ObserveMessage` handles TransformLog-owned messages:

- `Delete`
- Delete bodies inside committed `Txn`
- flush-style durable barriers from `DropPartition`, `DropCollection`,
  `TruncateCollection`, schema-changing `AlterCollection`, `ManualFlush`,
  `FlushAll`, and `AlterWAL`
- materialized barriers from `DropCollection`, `ManualFlush`, and `FlushAll`

For a plain Delete, TransformLogModule appends the Delete WAL message to the
vchannel TransformLog buffer in MetaAndData mode and returns a Data barrier.

For a committed Txn, TransformLogModule handles the Txn message directly as one
atomic WAL message. It collects all Delete bodies, groups them into one
TransformLog entry per vchannel at the transaction timetick, and returns
TransformLog Data barriers. RecoveryStorage and SegmentModule must not split
Txn(Delete) before it reaches TransformLogModule.

TransformLogModule does not read VChannelModule or SegmentModule state when
replaying Delete data.

`DropCollection`, `ManualFlush`, and `FlushAll` force both `FlushTo(T)` and
`MaterializeTo(T)`. Other flush-style messages force only `FlushTo(T)` unless
their message semantics are explicitly extended to wait for L0 materialization.

### Module.SwitchIntoMetaAndData

Switches retained TransformLogs into MetaAndData mode and returns:

```go
type TransformLogModuleSnapshot struct {
    TransformLogs map[string]*streamingpb.VChannelTransformLogMeta
}
```

### Module.ConsumeDirtySnapshots

Returns one dirty snapshot per vchannel TransformLog key. The operation only
snapshots module-local memory and does not return an error:

```text
ModuleName = transformlog
Key        = {PChannel, VChannel}
Op         = Upsert or Delete
Payload    = *streamingpb.VChannelTransformLogMeta for Upsert
```

Upsert snapshots publish TransformLog checkpoint, truncate cursor, retained
chunk id range, and materialized cursor. Delete snapshots drop retained
TransformLog meta after cleanup is safe.

### DirtySnapshot.MarkPersisted

The TransformLog dirty snapshot calls back into its owning TransformLog:

```go
func (s *transformLogDirtySnapshot) MarkPersisted() {
    s.owner.markSnapshotPersisted(s)
}
```

The owner records persisted `checkpoint_time_tick` and
`materialized_time_tick`, clears the matching in-flight dirty snapshot,
recomputes dirty state against the current TransformLog view, and advances the
TransformLog durable and materialized frontiers. Delete snapshots remove
retained TransformLog state after catalog drop succeeds.

### DataCheckpointView

`DataCheckpointTimeTick()` returns the minimum persisted TransformLog data
checkpoint across retained vchannel logs that still own pending or durable
TransformLog work. Idle TransformLogs that have no pending work and no retained
data to protect must not pin the global data checkpoint.

### DataFrontierView

`DataFrontier(scope)` returns a barrier for TransformLog progress:

- `ScopeAll`: all retained local TransformLogs;
- `ScopeVChannel`: the target vchannel TransformLog;
- `ScopePartition`: the owning vchannel TransformLog, because Delete data is
  stored at vchannel granularity.

`scope.Kind == DataProgressDurable` returns the catalog-persisted
`checkpoint_time_tick` frontier. `scope.Kind == DataProgressMaterialized`
returns the catalog-persisted `materialized_time_tick` frontier.

AckModule uses this through RecoveryStorage's composed `DataFrontierProvider`.

### CheckpointPersistedObserver

`NotifyCheckpointPersisted(metaTimeTick, dataTimeTick)` lets
TransformLogModule detect cleanup opportunities for tombstoned TransformLogs
and old chunk objects. Cleanup produces TransformLog DirtySnapshots or
asynchronous object deletion work. Catalog updates still flow through
RecoveryStorage persistence and `DirtySnapshot.MarkPersisted`.

### TransformLogProvider

`GetTransformLog(vchannel)` returns the module-owned TransformLog handle used
by local subscription and truncation callers. This provider is not part of the
common RecoveryStorage `Module` API.

## 8. TransformLog Access Interfaces

The storage owner and the subscription/truncation users are deliberately split.
`TransformLogModule` exposes a vchannel lookup layer; callers do not operate on
module internals or `VChannelModule` directly.

The TransformLog domain API lives in an independent `transformlog` package. It
contains only TransformLog read/truncate contracts, scanner events, errors, and
proto conversion helpers. It does not own assignment discovery, gRPC dialing, or
StreamingNode client retry logic.

```go
type TransformLogProvider interface {
    GetTransformLog(vchannel string) (TransformLogHandle, error)
}

type TransformLogHandle interface {
    TransformLogReader
    TransformLogTruncator
}
```

The reader interface follows the WAL `Read` / `Scanner` style:

```go
type TransformLogReader interface {
    Read(ctx context.Context, opt transformlog.ReadOption) transformlog.Scanner
}

type ReadOption struct {
    Name string
    VChannel string
    StartAfterTimeTick uint64
}

type Scanner interface {
    Name() string
    Chan() <-chan Event
    Error() error
    Done() <-chan struct{}
    Close() error
}

type Event struct {
    Entry *streamingpb.TransformLogEntry
    CaughtUp *CaughtUp
}

type CaughtUp struct {
    StartAfterTimeTick uint64
}
```

`Read` creates a vchannel-level scanner. The scanner first emits retained
entries with `entry.time_tick > StartAfterTimeTick`, then emits one `CaughtUp`
event, then keeps forwarding live entries. `CaughtUp` is an event in the same
stream instead of a separate method.

The truncation interface is separate from the reader interface:

```go
type TransformLogTruncator interface {
    Truncate(ctx context.Context, timeTick uint64) error
}
```

`Truncate` advances the vchannel TransformLog storage cursor. It is not a
subscription ack, and it does not depend on QueryNode stream progress.

## 9. Flattened Local And Remote Subscription

QueryNode consumes one flattened access interface, modeled after WAL
`WALAccesser.Read`:

```go
type TransformLogAccesser interface {
    Read(ctx context.Context, opt transformlog.ReadOption) transformlog.Scanner
}
```

The public entry is a sub-capability of the existing WAL accesser:

```go
type WALAccesser interface {
    // Existing WAL methods omitted.
    TransformLog() transformlog.Accesser
}
```

`TransformLog().Read` mirrors the existing `WALAccesser.Read` implementation:

```text
TransformLog.Read(ctx, opt)
  -> validate opt.VChannel
  -> pchannel = ToPhysicalChannel(opt.VChannel)
  -> create a resumable TransformLog scanner
       factory = walAccesser.handlerClient.ReadTransformLog
```

The resumable scanner is the only distributed-level wrapper. It tracks the last
delivered TransformLog TimeTick and recreates the underlying scanner from that
point if the stream breaks. Assignment discovery, local/remote selection,
wait-for-ready, server-id picking, gRPC dialing, interceptors, and rebalance
error reporting remain in the existing StreamingNode `handlerClient`
infrastructure.

`handlerClient.ReadTransformLog` uses the same pattern as `CreateConsumer`:

```text
ReadTransformLog(ctx, opt)
  -> createHandlerAfterStreamingNodeReady(pchannel)
      -> try local WAL registry
          -> local TransformLog accesser from the WAL TransformLog module
      -> otherwise use remote StreamingNodeHandlerService
          -> open SubscribeTransform and wrap it as transformlog.Scanner
```

There are two underlying scanner implementations behind this access path:

- local scanner: the target vchannel is owned by the local StreamingNode
  process, so `handlerClient` resolves the local WAL from the existing local
  registry and reads the in-process TransformLog state directly;
- remote scanner: the target vchannel is owned by another StreamingNode, so
  `handlerClient` uses the existing handler service client to open
  `SubscribeTransform` and wrap the gRPC stream as the same
  `transformlog.Scanner`.

QueryNode does not depend on whether the scanner is local or remote. This keeps
the first implementation compatible with a gRPC stream and leaves room for a
future local optimization that skips the gRPC stream layer.

The implementation packages follow existing WAL client boundaries:

```text
internal/streamingnode/transformlog/
    accesser.go    // Accesser, ReadOption, Scanner
    event.go       // Event, CaughtUp, batch/error event variants
    errors.go      // truncated range, unavailable, closed errors
    codec.go       // streamingpb conversion helpers

internal/distributed/streaming/internal/transformlog/
    scanner.go       // resumable scanner interface wrapper
    scanner_impl.go  // resume loop and last delivered TimeTick

internal/streamingnode/client/handler/transformlog/
    scanner.go        // raw remote SubscribeTransform scanner
    stream_client.go  // gRPC send/recv conversion

internal/streamingnode/server/service/handler/transformlog/
    subscribe_server.go
    subscription.go
    grpc_server_helper.go
```

The `transformlog` package is independent, but it is not a new client stack.
The distributed scanner and remote scanner reuse the existing WAL client
infrastructure through `walAccesserImpl.handlerClient`.

## 10. Workflow 1: TransformLog Persistence And Recovery

This workflow is the operational path implemented by the storage design above.

```text
WAL Delete@T
  -> RecoveryStorage consumes Delete@T in data stage
  -> TransformLog appends entry@T to pending buffer
  -> pending entries are flushed as dense chunk files
  -> TransformLogMeta publishes checkpoint_time_tick
  -> recovery catalog persists independent TransformLogMeta
  -> TransformLog DataBarrier advances to persisted checkpoint
  -> RecoveryStorage data checkpoint may pass T
```

TransformLog does not affect WAL write admission. It is not part of the WAL
write-before path; it is part of RecoveryStorage's data persistence path.

## 11. Workflow 2: TransformLog Subscription Implementation

This workflow describes how QueryNode gets TransformLog entries and applies
them to sealed segments.

### 11.1 Initial Subscription

1. QueryNode first loads a QueryView for a vchannel.
2. It reads `QueryView.transform_start_after_timetick = S`.
3. QueryNode calls `WAL().TransformLog().Read` to create one vchannel-level
   TransformLog scanner from `S`.
4. The distributed TransformLog scanner calls
   `handlerClient.ReadTransformLog`, which reuses the existing WAL assignment
   and local/remote selection infrastructure:
   - local: resolve the local WAL through the local registry and read the
     WAL TransformLog module's accesser;
   - remote: open `SubscribeTransform` through the existing
     `StreamingNodeHandlerService` client and wrap the gRPC stream as a
     `transformlog.Scanner`.
5. The serving side validates vchannel ownership and truncated range:
   - if `S < truncate_time_tick`, the scanner is unavailable;
   - otherwise entries with `entry.time_tick > S` are sent.
6. The scanner emits `CaughtUp` after the retained suffix is drained and the
   live stream is attached.
7. QueryNode stores entries in its local vchannel transform buffer.
8. QueryNode applies those entries to the sealed segments loaded for the
   QueryView.
9. The QueryView can report Ready only after segment load succeeds and required
   TransformLog entries have been consumed from the local buffer.

### 11.2 Later QueryView Updates

Later QueryViews on the same QueryNode/vchannel do not create another upstream
subscription.

The start point is monotonic:

```text
old view start <= new view start
```

The existing vchannel subscription already keeps receiving live TransformLog
entries into the local buffer. Therefore the new QueryView consumes directly
from the local buffer:

```text
new QueryView start = S
apply local buffered entries with entry.time_tick > S
```

There is no normal gap-fill path for later QueryView updates. If the local
buffer cannot cover a later view whose start point is supposed to be monotonic,
that is a local correctness failure or a subscription gap, not a refreshable
view-update case.

### 11.3 Local Buffer Truncation

As QueryViews on the QueryNode/vchannel move forward, the local buffer can drop
entries that no active local QueryView can still require.

```text
local truncate point =
    min(transform_start_after_timetick of active local QueryViews on this QN/vchannel)
```

Entries with `entry.time_tick <= local truncate point` can be removed from the
local buffer.

### 11.4 Reconnect

If the upstream subscription stream breaks, QueryNode reconnects and recreates
the vchannel subscription from the oldest local point it must still cover. If
StreamingNode can still serve entries after that point, the local buffer is repaired
and live consumption continues.

If StreamingNode TransformLog storage can no longer cover the required point,
affected QueryViews become Unrecoverable. The first implementation does not
fill the gap from L0 or from another source.

## 12. Workflow 3: TransformLog And L0 Segment

This workflow describes how TransformLog publishes its accumulated entries to
DataCoord/Coordinator.

TransformLog periodically materializes retained entries into L0 delete segments.
This path is independent from QueryNode subscription progress, but selected
flush/drop messages can wait for it through the materialized frontier.

### 12.1 Materialization Policy

The materializer accumulates TransformLog entries and triggers when enough data
has been collected. The primary trigger is accumulated unmaterialized Delete
data size:

- if accumulated rows after `materialized_time_tick` reach or exceed
  `l0.maxRowNum`;
- or if accumulated bytes after `materialized_time_tick` reach or exceed
  `l0.maxSize`.

Small ranges should stay in TransformLog until the threshold is met. L0
materialization is not triggered by QueryNode subscription ack or local buffer
progress.

`DropCollection`, `ManualFlush`, and `FlushAll` are force triggers. When one of
these messages is observed in MetaAndData mode, TransformLogModule submits
materialization work up to the message timetick after the corresponding
TransformLog chunks are durable.

### 12.2 Materialization Flow

1. Ensure `checkpoint_time_tick >= target_timetick` for the same vchannel.
2. Select retained TransformLog entries with
   `materialized_time_tick < entry.time_tick <= target_timetick`.
3. Group Delete blocks by `(vchannel, partitionID)`.
4. Build `storage.DeleteData` from primary keys and the enclosing entry
   timetick.
5. Split the selected data into one or more L0 segments. Each L0 segment
   contains exactly one deltalog file.
6. Allocate L0 segment ids and write one deltalog per L0 segment through the
   existing syncmgr / pack-writer path.
7. Commit the L0 segments to DataCoord with
   `SaveBinlogPaths(SegLevel_L0, Flushed=true)`.
8. Advance `TransformLogMeta.materialized_time_tick` to `target_timetick`.
9. Mark a TransformLog DirtySnapshot.
10. RecoveryStorage persists independent TransformLogMeta.
11. DirtySnapshot.MarkPersisted publishes the materialized cursor.

`materialized_time_tick` is on the same TransformLog timeline as
`entry.time_tick`. No separate materialized data/effect cursor exists.

### 12.3 Non-Goals

L0 materialization does not:

- drive QueryNode subscription catch-up;
- provide normal gap filling for QueryNode reconnect;
- decide TransformLog truncation;
- decide QueryNode local buffer truncation.

### 12.4 Synchronous Flush/Drop Completion

`DropCollection`, `ManualFlush`, and `FlushAll` complete only after both L1 and
L0 data effects for their scope are committed and the corresponding module
frontiers are persisted.

```text
DropCollection(vchannel, T):
  Segment durable/materialized frontier for vchannel >= T
  TransformLog materialized frontier for vchannel >= T

ManualFlush(vchannel, T):
  Segment durable/materialized frontier for vchannel >= T
  TransformLog materialized frontier for vchannel >= T

FlushAll(T):
  all local Segment durable/materialized frontiers >= T
  all local TransformLog materialized frontiers >= T
```

SegmentModule maps the materialized frontier to its normal Data frontier,
because L1 flush is already its DataCoord-visible materialization. TransformLog
uses a distinct materialized frontier based on `materialized_time_tick`.

## 13. SubscribeTransform Protocol

The protocol is a bidirectional stream under
`StreamingNodeHandlerService.SubscribeTransform`.

Like WAL `Consume`, the stream-level pchannel assignment is passed in gRPC
metadata, not in every per-vchannel subscription request:

```proto
message CreateTransformStreamRequest {
    PChannelInfo pchannel = 1;
}
```

Client-side context helpers mirror the existing consumer helpers:

```text
contextutil.WithCreateTransformStream(ctx, req)
contextutil.GetCreateTransformStream(ctx)
```

The server first resolves the assigned WAL with
`walManager.GetAvailableWAL(req.pchannel)`. Per-subscription requests then only
carry the vchannel-level TransformLog start point.

### 13.1 Request Types

| Request | Meaning |
| --- | --- |
| `create` | Create a remote vchannel scanner from `start_after_time_tick`. |
| `close_subscription` | Close one vchannel subscription on the stream. |
| `close_stream` | Gracefully close the whole stream. |

There is no `refresh` request in the target protocol. Reconnect creates a new
scanner from the oldest local point QueryNode still needs to cover.

### 13.2 Response Types

| Response | Meaning |
| --- | --- |
| `create` | Acknowledge scanner creation and report `truncate_time_tick`. |
| `message_batch` | Carry ordered `TransformLogEntry` values, currently Delete entries. |
| `caught_up` | Barrier for the create/reconnect subscription request. |
| `subscription_error` | Error scoped to one subscription on a multiplexed stream. |
| `close_stream` | Acknowledge graceful stream close. |

### 13.3 Caught-Up

`caught_up` means:

```text
For this scanner request, all currently retained entries after the requested
start point have been sent, and the live stream is now attached.
```

It does not expose a latest TimeTick. It is not an idle progress message.

## 14. Component Responsibilities

| Component | Responsibility |
| --- | --- |
| `RecoveryStorage` | Owns WAL data consumption, module dispatch, checkpoint advancement, recovery catalog persistence, and barrier composition. |
| `VChannelModule` | Owns vchannel metadata, schema, partition lifecycle, vchannel tombstones, dirty snapshots, and exposes only narrow read-only schema lookup to `SegmentModule`. |
| `SegmentModule` | Owns Insert data, segment assignment metadata, segment tombstones, L1 output, and segment data checkpoint. It may read `SchemaAt` from `VChannelModule` when creating segment state. |
| `TransformLogModule` | Owns Delete and Txn(Delete) data, transform-log chunks, retained chunk replay, scanner fanout, transform-log meta, transform-log tombstones, truncation, L0 materialization, and delete data checkpoint. |
| `internal/streamingnode/transformlog` | Defines the independent TransformLog Accesser, ReadOption, Scanner, events, errors, and proto conversion helpers. |
| `WALAccesser.TransformLog()` | Provides the public WAL-Read-style TransformLog access entry. It creates a resumable scanner and reuses the existing WAL `handlerClient`. |
| `handlerClient.ReadTransformLog` | Reuses WAL assignment discovery, local registry, wait-for-ready, server-id picker, gRPC service client, and rebalance error reporting to choose local or remote scanner creation. |
| `SubscribeTransform` server | Remote transport adapter: resolves the assigned WAL from stream metadata, opens a TransformLog scanner, sends retained entries, emits caught-up, then forwards live entries. |
| `QN TransformClient` | Owns TransformLog scanners and one local transform buffer per vchannel. |
| `QN vchannel transform buffer` | Stores received TransformLog entries for local QueryView consumption and truncates old entries as local views advance. |
| `QN SegmentManager/DeleteApplier` | Loads sealed segments, consumes the local vchannel transform buffer, applies Delete entries, and reports Ready or Unrecoverable. |
| `L0 materializer` | Converts accumulated TransformLog entries into DataCoord-managed L0 delete segments when thresholds or force barriers are met. |

## 15. Errors

| Scenario | Result |
| --- | --- |
| VChannel is not served by this StreamingNode | Subscription error with channel-not-exist or channel-fenced code. |
| Requested start is older than `truncate_time_tick` | Replay unavailable; affected QueryView becomes Unrecoverable. |
| Stream breaks | QueryNode reconnects and recreates the vchannel subscription from the oldest required local point. |
| Reconnect cannot cover required local point | Affected QueryViews become Unrecoverable. |
| TransformLog chunk write fails | DataBarrier does not advance; RecoveryStorage checkpoint cannot pass the Delete. |
| TransformLog meta persist fails | DataBarrier does not advance; WAL replay will rebuild unpublished entries after recovery. |
| Retained chunk is missing during recovery | Vchannel TransformLog recovery fails; subscriptions cannot be served safely. |

## 16. Invariants

1. `TransformLogEntry.time_tick` is the WAL Delete TimeTick and MVCC TimeTick.
2. There is no separate data timetick or effect timetick in TransformLog.
3. `transform_start_after_timetick` is exclusive.
4. `TransformDeleteBlock` does not carry a TimeTick; it uses the enclosing
   entry's TimeTick.
5. Each `TransformLogChunk` is non-empty.
6. Chunk ids are dense and vchannel-local.
7. Retained chunk files are exactly `[first_chunk_id, next_chunk_id)`.
8. Chunk paths are deterministic from `(pchannel, vchannel, chunk_id)`.
9. Entries are ordered by `time_tick` inside each chunk.
10. Retained chunks ordered by chunk id form a complete ordered TransformLog.
11. TransformLog DataBarrier is backed by catalog-persisted
    `checkpoint_time_tick`, not by pending buffer or object write alone.
12. RecoveryStorage data checkpoint cannot pass a Delete before TransformLog
    meta that covers the Delete has been persisted.
13. Truncate meta is published before old chunk objects are deleted.
14. TransformLog is owned by an independent RecoveryStorage module, not by
    VChannelModule or SegmentModule.
15. SubscribeTransform sends TransformLog entries, not WAL records.
16. QueryNode subscribes at vchannel granularity.
17. Local and remote TransformLog subscriptions are flattened behind the same
    WAL-Read-style scanner interface.
18. TransformLog uses an independent package for the domain interface, but
    reuses the existing WAL accesser and StreamingNode handler client
    infrastructure for assignment, local/remote selection, retry, and gRPC.
19. `SubscribeTransform` carries pchannel assignment in stream metadata, while
    per-subscription requests carry only vchannel and start TimeTick.
20. A QueryNode keeps one local transform buffer per vchannel.
21. Later QueryViews on the same QueryNode/vchannel consume from the local
    buffer and do not create a new upstream subscription.
22. QueryView start points on the same QueryNode/vchannel are monotonic.
23. L0 materialization is independent output to DataCoord/Coordinator.
24. L0 is not the normal QueryNode subscription replay source.
25. `DropCollection`, `ManualFlush`, and `FlushAll` wait for the
    TransformLog materialized frontier before acking.
26. The materialized frontier is backed by catalog-persisted
    `materialized_time_tick`, not by DataCoord RPC success alone.
27. Caught-up is a subscription barrier, not a TimeTick watermark.
28. Delete delivery may be at least once; Delete apply must be idempotent.
29. TransformLogModule does not read VChannel or Segment state for Delete
    replay, tombstone finalize, or cleanup.
30. The only required cross-module read is `SegmentModule -> VChannelModule`
    `SchemaAt(vchannel, partitionID, timetick)` for segment creation.

## 17. Implementation Stages

### Stage 1: TransformLog Storage

- Add independent TransformLogMeta catalog records with checkpoint, truncate,
  dense chunk id range, and materialized cursor.
- Store TransformLog state in an independent `TransformLogModule` registered
  directly with RecoveryStorage.
- Add object-storage chunk files with deterministic dense chunk paths.
- Implement pending buffer, pending flush chunks, and chunk flush tasks.
- Wire TransformLog DataBarrier through catalog-persisted
  `checkpoint_time_tick`.
- Implement recovery by reading retained chunks in `[first_chunk_id,
  next_chunk_id)`.
- Implement truncation by advancing `first_chunk_id` before asynchronous object
  deletion.

### Stage 2: TransformLog Read Interface And QN Buffer

- Add an independent `internal/streamingnode/transformlog` package with
  `Accesser`, `ReadOption`, `Scanner`, events, errors, and proto conversion
  helpers.
- Add `WALAccesser.TransformLog().Read`, modeled after current
  `WALAccesser.Read`.
- Add `handlerClient.ReadTransformLog` and implement it with the existing
  `createHandlerAfterStreamingNodeReady` path.
- Implement local and remote scanner creation behind `handlerClient`:
  - local: local WAL registry and WAL TransformLog module accesser;
  - remote: existing `StreamingNodeHandlerService` client and
    `SubscribeTransform` stream.
- Add stream metadata helpers for `CreateTransformStreamRequest`, matching the
  existing `CreateConsumerRequest` pattern.
- Implement one upstream subscription per QueryNode/vchannel.
- Send retained entries after `start_after_time_tick`, then caught-up, then
  live entries.
- Store received entries in a local vchannel buffer.
- Make QueryView segment readiness consume from the local buffer.
- Truncate the local buffer as active QueryViews advance.

### Stage 3: L0 Materialization

- Materialize TransformLog entries into L0 delete segments when thresholds are
  met.
- Force materialization for `DropCollection`, `ManualFlush`, and `FlushAll`.
- Commit L0 segments to DataCoord.
- Advance `materialized_time_tick` independently from QueryNode subscription
  progress.
- Expose a materialized frontier for AckModule preconditions.

### Stage 4: Naming And Protocol Cleanup

- Rename QueryView/DataView start-point terminology from
  `delete_apply_start_after_timetick` to `transform_start_after_timetick`.
- Keep Delete as the first transform payload.
- Remove refresh-style subscription requests from the target protocol.

## 18. Open Questions

1. Exact proto migration plan for renaming
   `delete_apply_start_after_timetick` to `transform_start_after_timetick`.
2. Chunk flush thresholds and checkpoint-pressure trigger policy.
3. Memory limit and backpressure policy for QueryNode local vchannel transform
   buffers.
4. Minimum untruncated range policy for StreamingNode TransformLog when no
   active QueryView exists yet but future QueryNode first subscription may
   occur.
5. Later idempotency design for crash after DataCoord L0 commit but before
   `materialized_time_tick` persistence.
