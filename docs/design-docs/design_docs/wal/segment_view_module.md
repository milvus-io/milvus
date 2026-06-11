# Segment View Module

`SegmentModule` owns growing segment assignment and Insert/L1 persistence state.
It is independent from VChannel and TransformLog state except for a narrow
schema lookup when creating segment state.

## 1. Ownership

`SegmentModule` owns:

- Segment assignment metadata;
- growing segment lifecycle state;
- Insert statistics and L1 buffers;
- segment flush and lifecycle side effects;
- segment tombstones;
- segment dirty snapshots and persistence;
- segment Meta/Data barriers.

`SegmentModule` does not own:

- VChannel collection or partition metadata;
- schema history;
- Delete or Txn(Delete) data;
- TransformLog chunks, scanners, truncation, or L0 materialization;
- broadcast acknowledgement.

## 2. Dependency

`SegmentModule` has one required cross-module read:

```text
SegmentModule -> VChannelModule.SchemaAt(vchannel, partitionID, timetick)
```

This happens when observing `CreateSegment`. The returned schema snapshot is
stored in SegmentModule state. Existing segments do not need to refresh schema
when `AlterCollection` appends a new schema version; schema-changing
AlterCollection flushes old segments before the schema metadata advances.

## 3. Observe Rules

### CreateSegment

If the target segment is absent, SegmentModule creates Segment View state using
the message assignment and `SchemaAt`. The Segment View records collection,
partition, vchannel, segment id, storage version, growing state, create
timetick, row limits, schema snapshot, and `MetaTimeTick`.

In MetaAndData mode, SegmentModule submits an EnsureGrowingSegment task and
returns a Data barrier until the lifecycle side effect and dirty snapshot
persistence complete.

### Insert

SegmentModule finds the target Segment View from the message assignment,
updates Meta statistics synchronously, and appends the Insert payload to the L1
buffer in MetaAndData mode.

Flush policy may submit a Segment-owned FlushBuffer task. The Data barrier
advances only after L1 output is durable and the dirty snapshot that records the
new Data progress is persisted.

### Flush

Flush closes the specified Segment View at the flush timetick. In MetaAndData
mode, SegmentModule submits a CommitL1Segment task. The Segment Data barrier
remains until pending L1 output is durable, lifecycle commit completes,
`DataTimeTick` advances, and the dirty snapshot is persisted.

### ManualFlush

Flushes every retained segment in the target vchannel whose create timetick is
older than the message timetick. Segment id hints in the message body are not
part of recovery semantics.

### DropPartition, DropCollection, TruncateCollection

These messages have flush semantics for SegmentModule. SegmentModule flushes
covered retained segments using the message timetick and returns Data barriers
for affected segment work. VChannel metadata changes are owned by
VChannelModule.

### FlushAll and AlterWAL

These are PChannel-wide flush barriers. SegmentModule flushes every retained
segment whose create timetick is older than the message timetick.

### Txn

For Insert bodies, SegmentModule updates each affected Segment View once at the
transaction timetick and appends the insert payload in MetaAndData mode.
Delete bodies are ignored by SegmentModule and handled by TransformLogModule.

## 4. Tombstone And Cleanup

Segment tombstones are independent from VChannel and TransformLog tombstones.
SegmentModule can finalize a segment tombstone when the segment's own Data
progress reaches the close timetick. It can physically delete retained segment
metadata when the tombstone state is persisted and both physical checkpoint
lanes have passed the tombstone timetick:

```text
Meta physical checkpoint > tombstone timetick
Data physical checkpoint > tombstone timetick
```

Segment cleanup does not require reading VChannel tombstone state. Scope-level
ack preconditions compose module Data frontiers outside SegmentModule.

## 5. Recovery

On WAL open, RecoveryStorage loads Segment snapshots from catalog and constructs
SegmentModule in MetaOnly mode. Historical WAL replay uses the same
`ObserveMessage` implementation. Data tasks are enabled only after switching to
MetaAndData mode.

Repeated `CreateSegment`, `Insert`, and `Flush` messages must be idempotent
against persisted Segment state and Data checkpoints.

## 6. ModuleAPI Implementation

`SegmentModule` implements the core `Module` API plus data checkpoint/frontier
views:

```go
type SegmentModule struct {
    segments       map[int64]*SegmentView
    schemaProvider SchemaProvider
}

var _ moduleapi.Module = (*SegmentModule)(nil)
var _ moduleapi.DataCheckpointView = (*SegmentModule)(nil)
var _ moduleapi.DataFrontierView = (*SegmentModule)(nil)
var _ moduleapi.CheckpointPersistedObserver = (*SegmentModule)(nil)
```

### Module.Name

Returns `ModuleNameSegment`.

### Module.ObserveMessage

`ObserveMessage` handles Segment-owned messages:

- `CreateSegment`
- `Insert`
- `Flush`
- Insert bodies inside committed `Txn`
- flush-style barriers from `ManualFlush`, `FlushAll`, `DropPartition`,
  `DropCollection`, `TruncateCollection`, schema-changing `AlterCollection`,
  and `AlterWAL`

It reads `SchemaAt` from VChannelModule only while creating a new Segment View.
It returns Meta barriers for Segment metadata changes and Data barriers for
pending L1 output or lifecycle side effects.

Delete and Txn(Delete) bodies are ignored by SegmentModule.

### Module.SwitchIntoMetaAndData

Switches retained Segment views into MetaAndData mode and returns:

```go
type SegmentModuleSnapshot struct {
    Segments map[int64]*streamingpb.SegmentAssignmentMeta
}
```

### Module.ConsumeDirtySnapshots

Returns one dirty snapshot per Segment key. The operation only snapshots
module-local memory and does not return an error:

```text
ModuleName = segment
Key        = {PChannel, SegmentID}
Op         = Upsert or Delete
Payload    = *streamingpb.SegmentAssignmentMeta for Upsert
```

Upsert snapshots publish Segment Meta and Data progress. Delete snapshots drop
retained tombstoned Segment metadata after cleanup is safe.

### DirtySnapshot.MarkPersisted

The Segment dirty snapshot calls back into its owning Segment View. The owner
records persisted Meta/Data timeticks, clears the matching in-flight dirty
snapshot, recomputes dirty state against the current Segment view, and advances
corresponding Meta/Data barriers. Delete snapshots remove the retained Segment
View after the catalog drop succeeds.

### DataCheckpointView

`DataCheckpointTimeTick()` returns the minimum persisted Data checkpoint across
retained Segment views that still own Data work. Idle or fully cleaned segment
state must not pin the global data checkpoint.

### DataFrontierView

`DataFrontier(scope)` returns a barrier for Segment Data progress in the given
scope:

- `ScopeAll`: all retained local segments;
- `ScopeVChannel`: segments in the target vchannel;
- `ScopePartition`: segments in the target vchannel/partition.

`scope.Kind == DataProgressDurable` and
`scope.Kind == DataProgressMaterialized` return the same Segment frontier.
SegmentModule's Data work commits L1 output through DataCoord, so its normal
durable frontier is already coordinator-visible materialization.

AckModule uses this through RecoveryStorage's composed `DataFrontierProvider`.

### CheckpointPersistedObserver

`NotifyCheckpointPersisted(metaTimeTick, dataTimeTick)` lets SegmentModule
detect segment tombstone cleanup opportunities. Cleanup produces Segment Delete
dirty snapshots and still uses RecoveryStorage-owned catalog persistence.

## 7. Invariants

1. SegmentModule owns Insert/L1 state and segment assignment metadata.
2. SegmentModule does not own VChannel lifecycle or Delete data.
3. `SchemaAt` is read only when creating segment state.
4. Existing segment schema snapshots are not refreshed by later schema changes.
5. Segment Data barriers are backed by persisted Segment state, not by pending
   buffers alone.
6. Segment tombstone and cleanup decisions are module-local.
