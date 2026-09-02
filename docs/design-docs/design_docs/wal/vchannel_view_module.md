# VChannel Recovery Module

`VChannelRecoveryModule` is the owner of all recovery state for one VChannel.
It is created and indexed by `PChannelRecoveryManager`.

Message completion is defined by
[WAL Message Ack Design](message_ack.md).

## 1. Ownership

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       +-- VChannelView
       +-- SegmentView*
       +-- TransformLog
       +-- DataView recovery state
       +-- QueryRuntime bridge
```

`VChannelRecoveryModule` owns:

- VChannel identity, collection metadata, partition state, and schema history;
- VChannel lifecycle state and tombstones;
- SegmentView lookup, creation, routing, and dirty aggregation;
- the VChannel TransformLog and its stream registration;
- DataView recovery state and Segment DataVersion summaries;
- QueryRuntime creation and live event forwarding;
- passing the same ref-counted immutable message to actual Segment and
  TransformLog consumers.

It does not own:

- PChannel checkpoint persistence;
- the ordered global Ack tracker;
- coordinator broadcast acknowledgement;
- QueryView state transitions.

## 2. Message Routing

`PChannelRecoveryManager` selects a VChannel module by message scope. A
PChannel-wide message is routed to all relevant VChannels using the same
ref-counted immutable message.

Within one VChannel:

```text
ObserveMessage(Retained)
  -> update VChannelView metadata
  -> route the same ref-counted message to affected SegmentViews
  -> route the same message to TransformLog
  -> forward a live resource event to QueryRuntime when present
  -> mark mutated recovery components dirty
```

QueryRuntime observation is not a WAL persistence completion condition and does
not retain message handles. A live event that carries a ref-counted RecoveryStorage
message is synchronously cloned before it enters the QueryRuntime queue, so the
queued event owns an ordinary immutable message and may outlive Message Ack
completion.

## 3. VChannel Metadata Rules

### CreateCollection And CreatePartition

Create or update VChannel identity, partition membership, and schema history.
The mutation marks the VChannelView dirty for the next persist batch.

### DropPartition And DropCollection

Record logical tombstones before physical cleanup. Segment and TransformLog
consumers retain their own message handles for required data work.

### TruncateCollection

Advance VChannel truncation metadata and route the message to SegmentViews and
TransformLog for any data work required at the truncation point.

### AlterCollection And SchemaChange

Append the new schema version and preserve historical schemas still required by
existing SegmentViews. Schema-changing messages may cause Segment and
TransformLog data work; non-schema changes are metadata-only.

### AlterLoadConfig And DropLoadConfig

QueryView metadata, not VChannel recovery metadata, is the source of truth for
query-resource acquisition. These messages do not create QueryRuntime
references in `VChannelRecoveryModule`.

## 4. Dirty Snapshots And Persist Batch

`VChannelRecoveryModule.ConsumeDirtySnapshots()` aggregates stable snapshots
from:

- `VChannelView`;
- dirty SegmentViews;
- TransformLog;
- owned DataView recovery state where applicable.

Each snapshot has component-specific `MarkPersisted()` behavior. RecoveryStorage
freezes a checkpoint boundary before consuming the snapshots, persists them into
independent catalog keys in etcd, and persists the frozen checkpoint last.

Message Ack does not replace metadata snapshots. The frozen batch points are:

```text
MetaPoint = latest completely observed WAL point
DataPoint = min(MetaPoint, Ack completed frontier)
```

An asynchronous VChannel-owned consumer updates recovery metadata and marks its
component dirty before releasing its retained message handle.

## 5. Segment And TransformLog Interaction

SegmentView and TransformLog data completion is joined by the shared
ref-counted immutable message:

- Segment handles release after segment data/lifecycle work succeeds;
- TransformLog handles release after containing chunks are durable;
- BroadcastAck registers the Owner's one-shot exclusive callback, which proves
  that all Segment and TransformLog Retained handles are gone, then lets its
  background dispatcher perform Coordinator Ack under the ResourceKey partial
  order.

Historical schema lookup is an internal VChannel ownership operation when a new
SegmentView is created. TransformLog does not inspect Segment private state for
Delete replay.

## 6. Query Runtime Boundary

`VChannelRecoveryModule` may build a `VChannelWALView` and create one shared
QueryRuntime for active QueryViews. It forwards live DML events after updating
recovery state.

Before capturing a WAL view, it waits for bounded RecoveryStorage replay to
complete and resolves every retained `FLUSHED` SegmentView whose
`SealedAtDataVersion` is nil. Resolution triggers or reuses the SegmentView's
idempotent final-commit task. Once every flushed segment has its exact first
DataView membership version, the module classifies segments independently
against the target QueryView DataVersion. It does not maintain a VChannel-level
DataVersion fence.

QueryView references protect temporary serving resources only. They do not:

- retain WAL message handles;
- affect RecoveryStorage persist-batch boundaries;
- affect broadcast acknowledgement;
- own TransformLog object durability.

See
[StreamingNode Query Resource Design](../qviews/snview/streamingnode_resource_manager.md).

## 7. Tombstone And Cleanup

VChannel cleanup is owned by the VChannel component and is independent from
Segment and TransformLog private cleanup decisions:

1. persist the VChannel tombstone;
2. retain state while recovery or serving references require it;
3. emit cleanup snapshots when VChannel-local conditions are satisfied;
4. remove the VChannel index entry only after owned cleanup has completed.

Segment and TransformLog catalog records are cleaned through their own retained
state and snapshots.

## 8. Recovery

`PChannelRecoveryManager` creates VChannel modules from the union of persisted
VChannel, Segment, TransformLog, and DataView records. This allows recovery even
when one component's base metadata has already entered tombstone cleanup while
another component still has retained state.

After construction:

1. metadata replay rebuilds in-memory state;
2. modules switch into MetaAndData mode;
3. data replay starts from the persisted Data checkpoint;
4. replay creates new Tracker entries and ref-counted wrappers for unfinished
   messages;
5. bounded replay reaches the recovery boundary;
6. QueryView state recovery independently reacquires query resources, waiting
   for any recovered final commits before WAL view capture.

## 9. Invariants

1. One `VChannelRecoveryModule` owns all recovery components for one VChannel.
2. SegmentView and TransformLog are internal components, not sibling top-level
   recovery modules.
3. VChannel metadata publication is part of the frozen persist batch.
4. Data checkpoint advancement is Message Ack based and bounded by the batch
   MetaPoint.
5. QueryRuntime borrows the underlying immutable message; its observation does
   not participate in Message Ack.
6. Every broadcast-related Segment/TransformLog consumer retains its own message
   handle.
7. Async VChannel-owned consumers mark metadata dirty before releasing a handle.
8. QueryView readiness requires no retained `FLUSHED` SegmentView with a nil
   `SealedAtDataVersion`; it does not require an aggregate DataVersion fence.
