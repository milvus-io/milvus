# Segment View Module

`SegmentModule` owns growing segment assignment and Insert/L1 persistence state.
It is independent from VChannel and TransformLog state except for a narrow
schema lookup when creating segment state.

## 1. Ownership

`SegmentModule` owns:

- Segment assignment metadata;
- vchannel-level Segment DataVersion summary;
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
- LoadConfig or QueryView state;
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

When DataCoord returns a sealed DataVersion for the committed segment,
SegmentModule records it in `SegmentAssignmentMeta.sealed_at_data_version`.
This segment-level value remains the primary source for deriving the current
vchannel DataVersion while the flushed segment metadata is retained.

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

### VChannelWALView Segment Snapshot

RecoveryStorage creates `VChannelWALView` when it observes `AlterLoadConfig`
or recovers an existing load config during WAL open. The view captures the
Segment snapshot at the serialized WAL observe point:

```text
snapshot.DataVersion = SegmentSnapshotDataVersion(vchannel)
snapshot.Segments    = VisibleSegments(vchannel, snapshot.DataVersion)
```

The visible segment rule is:

```text
VisibleSegments(vchannel, D) =
  current GROWING segments in vchannel
  union retained FLUSHED segments whose sealed_at_data_version is absent
  union retained FLUSHED segments whose sealed_at_data_version > D
```

The absent `sealed_at_data_version` case keeps flushed segment metadata visible
until its sealed DataVersion is known.

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

Segment cleanup is also responsible for preserving the vchannel DataVersion
summary before flushed segment metadata is physically removed. SegmentModule
does not update this summary on every flush. While flushed segment metadata is
retained, the latest observed DataVersion can be derived from
`SegmentAssignmentMeta.sealed_at_data_version`. Before cleanup deletes flushed
segment metadata that would otherwise be the durable source of the maximum
observed DataVersion, SegmentModule first persists a
`SegmentDataVersionSummary`.

The summary update is therefore lazy and GC-driven:

```text
summary.data_version =
  max(existing summary.data_version,
      sealed_at_data_version of flushed segment metadata being removed)
```

If retained flushed segment metadata still covers the maximum observed
DataVersion, cleanup does not need to update the summary.

## 5. DataVersion Summary

SegmentModule owns the vchannel-level Segment DataVersion summary:

```proto
message SegmentDataVersionSummary {
  view.DataVersion data_version = 1;
}
```

The summary object only records the latest DataVersion known by SegmentModule.
Its catalog key carries the pchannel and vchannel identity; the proto does not
need to duplicate them.

Snapshot DataVersion is derived as:

```text
SegmentSnapshotDataVersion(vchannel) =
  max(
    persisted SegmentDataVersionSummary.data_version,
    max(sealed_at_data_version of retained FLUSHED segments in vchannel),
  )
```

If a vchannel has neither summary nor retained flushed segment metadata,
SegmentModule returns the empty DataVersion for the snapshot. This represents a
new or fully growing-only local state; current growing segments are still
included by `VisibleSegments`.

## 6. Recovery

On WAL open, RecoveryStorage loads Segment snapshots and
`SegmentDataVersionSummary` records from catalog, then constructs
SegmentModule in MetaOnly mode. Historical WAL replay uses the same
`ObserveMessage` implementation. Data tasks are enabled only after switching to
MetaAndData mode.

Repeated `CreateSegment`, `Insert`, and `Flush` messages must be idempotent
against persisted Segment state and Data checkpoints.

## 7. ModuleAPI Implementation

`SegmentModule` implements the core `Module` API plus data checkpoint/frontier
views:

```go
type SegmentModule struct {
    segments             map[int64]*SegmentView
    dataVersionSummaries map[string]*SegmentDataVersionSummary
    schemaProvider       SchemaProvider
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

### VChannelWALView Snapshot Function

SegmentModule provides the Segment part of `VChannelWALView`:

```go
func (m *SegmentModule) VisibleSnapshot(
    vchannel string,
    baseGrowingTimeTick uint64,
) walview.VisibleSegmentSnapshot
```

The function computes `SegmentSnapshotDataVersion` from SegmentModule state and
returns visible segment metadata plus retained Insert buffers needed by the
query-side growing recovery path.

### Module.SwitchIntoMetaAndData

Switches retained Segment views into MetaAndData mode and returns:

```go
type SegmentModuleSnapshot struct {
    Segments             map[int64]*streamingpb.SegmentAssignmentMeta
    DataVersionSummaries map[string]*streamingpb.SegmentDataVersionSummary
}
```

This recovery snapshot exports module persistence state. `VisibleSnapshot`
exports query-visible Segment state for `VChannelWALView`.

### Module.ConsumeDirtySnapshots

Returns dirty snapshots for Segment keys and Segment DataVersion summary keys.
The operation only snapshots module-local memory and does not return an error:

```text
ModuleName = segment
Key        = {PChannel, SegmentID}
Op         = Upsert or Delete
Payload    = *streamingpb.SegmentAssignmentMeta for Upsert

ModuleName = segment
Key        = {PChannel, VChannel, "data-version-summary"}
Op         = Upsert
Payload    = *streamingpb.SegmentDataVersionSummary
```

Upsert snapshots publish Segment Meta and Data progress. Delete snapshots drop
retained tombstoned Segment metadata after cleanup is safe.

Summary Upsert snapshots publish the GC-preserved vchannel DataVersion. The
summary has no Delete snapshot in normal cleanup; an absent summary is
equivalent to an empty DataVersion.

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

## 8. Invariants

1. SegmentModule owns Insert/L1 state and segment assignment metadata.
2. SegmentModule is the only owner of Segment DataVersion summaries.
3. SegmentModule does not own VChannel lifecycle, LoadConfig, QueryView state,
   or Delete data.
4. `SchemaAt` is read only when creating segment state.
5. Existing segment schema snapshots are not refreshed by later schema changes.
6. Segment Data barriers are backed by persisted Segment state, not by pending
   buffers alone.
7. Segment tombstone and cleanup decisions are module-local.
8. `VisibleSnapshot` selects the Segment snapshot DataVersion from
   SegmentModule state.
9. Segment GC persists `SegmentDataVersionSummary` before deleting flushed
   metadata that carries the maximum observed DataVersion.
