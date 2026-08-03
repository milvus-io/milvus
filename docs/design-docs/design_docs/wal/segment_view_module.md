# Segment View Module

`SegmentModule` owns growing segment assignment and Insert/L1 persistence state.
It collaborates with VChannel state for schema lookup and for crash-safe
retention of the segment DataVersion summary. TransformLog state remains
independent.

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

- VChannel collection or partition metadata, including the durable
  `segment_data_version_summary` embedded in `VChannelMeta`;
- schema history;
- Delete or Txn(Delete) data;
- LoadConfig or QueryView state;
- TransformLog chunks, scanners, truncation, or L0 materialization;
- broadcast acknowledgement.

## 2. Dependency

`SegmentModule` has two narrow VChannel interactions:

```text
SegmentModule -> VChannelModule.SchemaAt(vchannel, partitionID, timetick)
SegmentModule -> VChannelModule.AdvanceSegmentDataVersionSummary(dataVersion)
```

The schema lookup happens when observing `CreateSegment`. The returned schema
snapshot is stored in SegmentModule state. Existing segments do not need to
refresh schema when `AlterCollection` appends a new schema version;
schema-changing AlterCollection flushes old segments before the schema metadata
advances.

The summary advancement happens when a flushed segment tombstone is finalized.
It monotonically raises the summary stored in `VChannelMeta`. Segment cleanup
also reads the persisted summary and does not delete the assignment until that
persisted value covers the segment's sealed DataVersion.

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
summary before flushed segment metadata is physically removed. The summary is
not updated on every flush. While flushed segment metadata is retained, the
latest observed DataVersion can be derived from
`SegmentAssignmentMeta.sealed_at_data_version`. When the segment tombstone is
finalized, SegmentModule advances `VChannelMeta.segment_data_version_summary`.
RecoveryStorage persists that VChannel base meta before cleanup is allowed to
delete the segment assignment.

The summary update is therefore lazy and GC-driven:

```text
vchannel_meta.segment_data_version_summary =
  max(existing vchannel_meta.segment_data_version_summary,
      sealed_at_data_version of flushed segment metadata being removed)
```

The persistence and cleanup protocol is deliberately two phase:

```text
finalize segment tombstone
  -> advance VChannelMeta summary
  -> persist VChannel base meta
  -> acknowledge the persisted VChannel snapshot
  -> delete covered SegmentAssignmentMeta
```

If the process fails between the VChannel write and assignment deletion,
recovery observes either the retained assignment or the durable summary. The
assignment deletion is idempotent and is retried only after the persisted
summary covers its sealed DataVersion.

## 5. DataVersion Summary

`VChannelMeta` owns the vchannel-level Segment DataVersion summary:

```proto
message VChannelMeta {
  // Other VChannel metadata is omitted.
  view.DataVersion segment_data_version_summary = 7;
}
```

There is no standalone summary record or `sdv` catalog key. Summary-only
changes use the VChannel base-meta catalog operation so separately stored schema
records are not rewritten.

Snapshot DataVersion is derived as:

```text
SegmentSnapshotDataVersion(vchannel) =
  max(
    VChannelMeta.segment_data_version_summary,
    max(sealed_at_data_version of retained FLUSHED segments in vchannel),
  )
```

If a vchannel has neither summary nor retained flushed segment metadata,
SegmentModule returns the empty DataVersion for the snapshot. This represents a
new or fully growing-only local state; current growing segments are still
included by `VisibleSegments`.

## 6. Recovery

On WAL open, RecoveryStorage loads VChannel metadata and Segment snapshots from
catalog, then constructs the modules in MetaOnly mode. The embedded summary is
initialized as both the current and persisted VChannel summary. Historical WAL
replay uses the same `ObserveMessage` implementation. Data tasks are enabled
only after switching to MetaAndData mode.

For a recovered tombstoned segment, recovery compares
`sealed_at_data_version` with the embedded summary. If the assignment carries a
newer version, recovery advances and persists the VChannel base meta before the
assignment becomes eligible for deletion. This repairs crashes that occurred
after the assignment tombstone was durable but before the summary was durable.

Repeated `CreateSegment`, `Insert`, and `Flush` messages must be idempotent
against persisted Segment state and Data checkpoints.

## 7. ModuleAPI Implementation

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
    Segments map[int64]*streamingpb.SegmentAssignmentMeta
}
```

This recovery snapshot exports module persistence state. `VisibleSnapshot`
exports query-visible Segment state for `VChannelWALView`.

### Module.ConsumeDirtySnapshots

Returns dirty snapshots for Segment keys. The operation only snapshots
module-local memory and does not return an error:

```text
ModuleName = segment
Key        = {PChannel, SegmentID}
Op         = Upsert or Delete
Payload    = *streamingpb.SegmentAssignmentMeta for Upsert

```

Upsert snapshots publish Segment Meta and Data progress. Delete snapshots drop
retained tombstoned Segment metadata after cleanup is safe.

Advancing the summary dirties the owning VChannel view. Its snapshot uses
`SnapshotOpUpsertBase`, with a `*streamingpb.VChannelMeta` payload, unless the
same stable snapshot also contains a schema change. RecoveryStorage routes the
base-only operation to `SaveVChannelBaseMetas`; schema-changing snapshots use
the normal VChannel save path.

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
2. VChannelMeta is the durable owner of the Segment DataVersion summary;
   SegmentModule only advances it while finalizing segment tombstones.
3. SegmentModule does not own VChannel lifecycle, LoadConfig, QueryView state,
   or Delete data.
4. `SchemaAt` is read only when creating segment state.
5. Existing segment schema snapshots are not refreshed by later schema changes.
6. Segment Data barriers are backed by persisted Segment state, not by pending
   buffers alone.
7. Segment tombstone finalization is module-local, but physical assignment
   cleanup also requires the persisted VChannel summary to cover the sealed
   DataVersion.
8. `VisibleSnapshot` selects the Segment snapshot DataVersion from the embedded
   VChannel summary plus retained Segment state.
9. Segment GC persists the advanced VChannel base meta before deleting flushed
   metadata that carries the maximum observed DataVersion.
