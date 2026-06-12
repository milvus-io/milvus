# DataView Design

> This document describes the DataView design for distributed query views.
> Reference: [Distributed Query View Design](README.md), [Balancer & CollectionLoadManager Design](balancer_design.md), [view.proto](../../../../pkg/proto/view.proto).

## 1. Purpose

DataView is the data-side snapshot that separates **what data exists** from
**who serves the data**.

```
DataView(DataVersion)
    "which segments are currently allowed to be loaded, organized by shard and partition"

QueryView(DataVersion, QueryVersion)
    "on top of a DataView, which nodes serve which segments"
```

DataCoord owns and persists DataView. QueryCoord consumes DataView as immutable
input for QueryView generation.

## 2. Concepts

### 2.1 DataView

DataView is the complete, non-duplicated loadable segment membership snapshot
of a collection.

It answers only:

```
At DataVersion D, which segments are allowed to be loaded in each vchannel and partition?
```

It does not contain QueryNode placement, resource groups, load state, index
state, segment content version, manifest version, or QueryView lifecycle state.

Once a segment is already in DataView, later changes to that segment's content
or load workflow are outside DataView. Those changes may still require QueryNode
reopen/reload through other mechanisms, but they do not change DataView
membership or DataVersion.

### 2.2 Loadable Segment

A segment joins a QueryCoord-visible DataView only when DataCoord considers it
allowed to be loaded by QueryNode.

The loadable predicate is:

```
state == Flushed
&& !is_invisible
&& !is_importing
&& level != L0
&& !dropped
```

Segments outside this predicate do not join a QueryCoord-visible DataView.
Examples:

- Growing, Sealed, Flushing, Importing, Dropped segments are not in DataView.
- Invisible flushed segments are not in DataView until they become visible.
- L0 delete segments are not in DataView membership.

After a segment has joined a persisted DataView, DataView membership is the
loadability authority for that version. Later segment-meta state changes such
as `Dropped` or `Compacted` do not by themselves make the segment unloadable for
retained DataViews that still reference it. Physical GC must wait until no
retained DataView or QueryView can reference the segment.

The current flush plus sort-compaction path has one transitional exception:
DataCoord may persist an internal DataView that contains a temporary flushed
segment, but it marks that DataView unavailable in memory and does not expose it
to QueryCoord. Once sort compaction produces the final segment, DataCoord
publishes a QueryCoord-visible DataView for QueryView construction.

### 2.3 DataVersion

DataVersion is a collection-level composite version:

```
(streaming_version, compact_version)
```

Versions are ordered lexicographically.

`streaming_version` advances when new loadable segments join the view from the
streaming/write side. `compact_version` advances when the loadable membership
of the current streaming epoch is rewritten or trimmed.

Version rules:

| Membership Change | Version Transition | Meaning |
|---|---|---|
| Segment joins DataView from write/import/copy-segment-complete path | `(S, C) -> (S+1, 0)` | New loadable data joins the view. |
| Segment leaves DataView, or membership is replaced/trimmed | `(S, C) -> (S, C+1)` | Existing loadable membership changes without a new streaming epoch. |
| Segment content changes without membership change | unchanged | DataView does not track content mutation. |
| Delete frontier changes without membership change | unchanged | `delete_apply_start_after_timetick` is derived metadata, not a version source. |
| DropCollection | delete DataView | The whole collection view is removed. |

`compact_version` resets to `0` whenever `streaming_version` advances.

The first DataView version of a collection starts from `(1, 0)`. This applies
to the first event-created DataView and to recovery rebuild when no previous
DataView exists.

DataView is advanced by DataCoord events, not by QueryCoord or by a generic
recalculation loop. Each event carries the segment mutation that may make
segments join or leave the view. The event handler still checks the loadable
predicate, so an event can be recorded as pending and complete only when its
segments become allowed to load.

Flush, import, and copy-segment completion advance DataVersion only when they
introduce new loadable membership. Compaction, drop partition, and truncate
advance DataVersion only when they remove, replace, or trim loadable
membership.

### 2.4 QueryViewVersion

QueryViewVersion is:

```
(DataVersion, QueryVersion)
```

DataVersion changes when DataView changes. QueryVersion changes when the same
DataView is served by a different placement plan, for example balance, recovery,
or resource-group adjustment.

When DataVersion advances, QueryVersion starts from `1` for the new QueryView.
When only placement changes, DataVersion stays unchanged and QueryVersion
increments.

## 3. Data Structures

DataView uses the proto definitions in `view.proto` for persistence and
transport:

```proto
message DataViewOfCollection {
    int64 collection_id = 1;
    repeated DataViewOfShard shards = 2;
    DataVersion data_version = 3;
}

message DataViewOfShard {
    string vchannel = 1;
    repeated DataViewOfPartition partitions = 2;
    uint64 delete_apply_start_after_timetick = 3;
}

message DataViewOfPartition {
    int64 partition_id = 1;
    repeated int64 segment_ids = 2;
}
```

The durable DataView record stores membership and DataVersion only. Segment
metadata such as row count, memory size, binlogs, manifest path, schema version,
storage version, segment-level data version, and segment-level
`delete_apply_start_after_timetick` remain in DataCoord segment metadata.
QueryCoord may fetch that metadata separately for balancer scoring, but it must
not recompute DataView membership from it.

`DataViewOfShard.delete_apply_start_after_timetick` is a snapshot/transport
field derived from the current loadable membership. It does not need to be
stored durably with DataView; DataCoord may recompute it from segment metadata
when publishing snapshots or syncing StreamingNode. If the serialized proto
contains this field in a persisted DataView value, DataCoord still treats it as
derived cache and recomputes it from segment metadata on load/recovery. The
source value is a separate persisted field on each segment metadata record, not
`segment.dml_position.Timestamp`.

## 4. Ownership

### 4.1 DataCoord

DataCoord is the source of truth for DataView.

Responsibilities:

1. Recover DataView from the metastore on startup.
2. Keep an in-memory resident DataView per collection.
3. Persist every loadable-membership mutation.
4. Advance DataVersion according to the rules above.
5. Expose DataView snapshots to QueryCoord.
6. Derive and sync `delete_apply_start_after_timetick` to StreamingNode when it changes.

### 4.2 QueryCoord

QueryCoord consumes DataView.

Responsibilities:

1. Maintain a `DataViewProvider` cache for Balancer.
2. Publish immutable `DataViewSnapshot` objects.
3. Trigger Balancer when a collection's DataVersion advances.
4. Build QueryViews from `DataView membership + assignments`.
5. Refresh DataView-derived metadata such as delete timetick without changing
   QueryViewVersion.

QueryCoord must not modify DataView or assign DataVersion.

### 4.3 DataViewManager State Model

The first implementation keeps one resident state per collection in DataCoord:

```go
type collectionDataViewState struct {
    collectionID int64

    latestResident *viewpb.DataViewOfCollection
    latestVisible  *viewpb.DataViewOfCollection
}
```

Definitions:

- **latest persisted DataView**: the maximum DataVersion found under the
  collection's DataView version prefix in the metastore.
- **latest resident DataView**: the latest linear DataView held by DataCoord in
  memory. It may be unavailable for QueryView construction, for example the
  temporary flush DataView before sort-compaction handoff.
- **latest visible DataView**: the latest resident DataView that DataCoord has
  marked available for QueryCoord/Balancer.

QueryCoord can only consume `latestVisible`. DataCoord internal event handling
continues from `latestResident`, so unavailable temporary DataViews still keep
the collection's DataView history linear.

## 5. Persistence

The first implementation stores one full collection snapshot per DataVersion.
It does not shard the persisted DataView payload.

Suggested keys:

```
datacoord/dataview/{collectionID}/versions/{streamingVersion}/{compactVersion}
```

`versions/{S}/{C}` stores a serialized `DataViewOfCollection` membership
snapshot. The durable record may omit
`delete_apply_start_after_timetick`; that value can be recomputed when
DataCoord publishes a snapshot or syncs StreamingNode.

Persistence and visibility semantics:

- A DataView version is durably complete if and only if its full snapshot key
  exists.
- The latest persisted DataView is the maximum DataVersion under the
  collection's `versions/*` prefix.
- QueryCoord-visible DataView snapshots may lag the latest persisted snapshot
  when DataCoord marks a snapshot as temporarily unavailable in memory, for
  example during the current flush plus sort-compaction path.
- QueryCoord and DataViewProvider must obtain DataView from DataCoord or the
  DataViewProvider cache; they must not derive membership from segment metadata.
- Failed writes may leave no new version key. Because each version is one full
  snapshot value, a visible key is treated as a complete DataView snapshot.

### 5.1 SegmentMeta-First Publication

DataView and segment metadata are managed by separate components in the first
implementation. DataCoord updates segment metadata first, then persists and
publishes DataView.

Normal flow:

```
1. Apply and persist segment metadata mutation.
2. Build the next full DataView snapshot from the previous resident DataView
   plus the DataCoord event.
3. Persist versions/{D'}.
4. If the new DataView is available for QueryView construction, notify
   QueryCoord after versions/{D'} is persisted.
```

This is not a strict atomic transaction between segment metadata and DataView.
It is a segmentMeta-first, DataView-lagging publication model. The important
ordering guarantee is:

```
DataView may lag segment metadata.
DataView must not publish membership that segment metadata cannot describe.
```

If DataView persistence fails after segment metadata has already advanced,
no new DataView version is persisted and QueryCoord is not notified. Later
events or recovery may compact multiple segment metadata changes into one
DataView update.

DropPartition and truncate are exceptions to the default ordering. These
destructive trim operations should update DataView first, then update segment
metadata. This prevents QueryCoord from building new QueryViews over membership
that has already been logically removed by DDL/trim intent.

### 5.2 Reconciliation Semantics

DataCoord may reconcile DataView from segment metadata only on recovery or when
repairing a failed DataView publication. Normal event handling should remain
event-driven.

If reconciliation finds that segment metadata has advanced beyond the latest
persisted DataView, DataCoord persists one new full DataView snapshot that
represents the final loadable membership. It does not need to replay every
missed event.

Version choice during reconciliation, in priority order:

| Reconciliation Diff | Version Transition |
|---|---|
| New import or flush segments appear | `(S, C) -> (S+1, 0)` |
| Compact handoff appears | `(S, C) -> (S, C+1)` |
| Only delete frontier differs | unchanged; refresh the latest DataView-derived timetick |
| No membership or delete frontier diff | unchanged |

This means several segment events may be compacted into a single DataView
snapshot. DataVersion reflects persisted DataView changes, not every
individual segment metadata event.

The priority is intentional. Recovery uses the same segment visibility rules as
the current `GetRecoveryInfoV2` path to compute the expected DataView. If the
computed DataView is identical to the latest DataView, recovery does not create
another version. If it differs, recovery classifies the final diff rather than
replaying every missed event. New flush/import data is treated as a streaming
epoch update and has priority over compact handoff when both appear in the same
recovery diff, because it is also the signal used by the query side to evict
overlapped growing data. Compaction handoff is treated as a compact-version
update only when no streaming-version event is present. A pure delete frontier
mismatch only updates the in-memory/sync payload for the latest view and does
not advance DataVersion.

Recovery classification does not require a new durable reason field. It can use
existing segment metadata, such as segment state and compact-from/compaction
lineage, to distinguish flush/import additions from compaction handoff.

## 6. Mutation Semantics

DataViewManager is the DataCoord component that applies DataCoord events to the
persisted loadable membership.

Suggested interface shape:

```go
type DataViewManager interface {
    OnFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error)
    OnImport(ctx context.Context, event ImportDataViewEvent) (*viewpb.DataVersion, error)
    OnCopySegmentComplete(ctx context.Context, event CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error)
    OnCompact(ctx context.Context, event CompactDataViewEvent) (*viewpb.DataVersion, error)
    OnL0Compact(ctx context.Context, event L0CompactDataViewEvent) (*viewpb.DataVersion, error)
    OnExternalRefresh(ctx context.Context, event ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error)
    OnDropPartition(ctx context.Context, event DropPartitionDataViewEvent) (*viewpb.DataVersion, error)
    OnTruncate(ctx context.Context, event TruncateDataViewEvent) (*viewpb.DataVersion, error)
    OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)

    LatestVisibleDataView(ctx context.Context, collectionID int64) (*viewpb.DataViewOfCollection, error)
    Snapshot(ctx context.Context, collectionIDs []int64) ([]*viewpb.DataViewOfCollection, error)
}
```

Every `On*` method returns the DataVersion generated or affected by the event.
If the event only refreshes derived metadata, for example L0 compaction changing
`delete_apply_start_after_timetick`, it returns the current DataVersion without
advancing it.

Internal helpers should keep the normal path event-driven:

```go
buildNextView(base, mutation) -> next
classifyVersionAdvance(mutation) -> streaming/compact/none
isVisibleForQueryView(view, segmentMeta) -> bool
deriveDeleteTimetick(view, segmentMeta) -> per-shard timetick
persistFullSnapshot(view) error
notifyQCIfVisible(view)
```

The event types should carry the affected collection, vchannel, partition, and
segment IDs from the completed DataCoord mutation. DataViewManager must not
discover the next view by scanning all segment metadata on the normal path.
Scanning segment metadata is only a recovery/reconciliation fallback.

The event handler checks whether each affected segment is already loadable:

- If the affected segment is loadable and not already in DataView, add it.
- If an old member is dropped, compacted away, or removed by DDL/truncate, remove
  it.
- If the event only changes segment content or delete frontier, keep
  DataVersion unchanged.
- If the event's output is still not loadable, keep it pending or ignore it
  until the later visibility event completes the same logical operation.

A single DataCoord mutation batch advances DataVersion at most once.

Event handlers must be idempotent. Repeated events, events already compacted by
recovery, and events whose segments are no longer present in the resident view
are normal no-op success cases.

### 6.1 Flush Events

`OnFlush` is triggered by StreamingNode flush completion through
`SaveBinlogPaths`.

Flush is special in the current implementation. StreamingNode flush can first
produce a temporary sealed segment. A DataView that contains this temporary
segment is not yet usable by QueryView, because the system still needs the
follow-up sort compaction to produce the final loadable segment. DataCoord
therefore keeps an in-memory availability state for such DataViews and does not
expose them to QueryCoord/Balancer as QueryView input until the sorted handoff
is ready.

If the flushed segment is immediately usable, it joins DataView and advances:

```
(S, C) -> (S+1, 0)
```

If the flush output must pass through sort compaction first, the flush-side
DataView advances to `(S+1, 0)` in DataCoord but remains unavailable to
QueryView in memory. The later sort-compaction handoff replaces the temporary
segment with the final segment, advances compact version in the same streaming
epoch, for example `(S+1, 0) -> (S+1, 1)`, and makes the resulting DataView
available. QueryCoord first observes this data with the increased
`streaming_version`, which is the signal needed to evict overlapped growing
data. In the future, when StreamingNode produces sorted flush output directly,
this temporary unavailable state can disappear and `OnFlush` can add the usable
segment directly.

Dropped flush output, partial checkpoint updates, and binlog-only updates do
not change DataView.

### 6.2 Import Events

`OnImport` is triggered by import completion.

Import preallocation and import progress do not affect DataView. Imported
segments join only after the import path has completed all gates required for
QueryNode loading. In the current flow this can be delayed until compaction and
index build finish, and 2PC import must also clear `is_importing`.

When imported segments become loadable, they join DataView and advance:

```
(S, C) -> (S+1, 0)
```

L0 import output follows the L0 rules and does not join membership.

### 6.3 Compact Events

`OnCompact` is triggered by compaction completion.

For non-L0 compaction, DataView changes only when loadable membership changes:

- loadable input segments leave the view;
- loadable output segments join the view;
- invisible output segments wait until the required visibility gate, such as
  stats/index build, completes.

When the compaction rewrite becomes visible to QueryNode loading, DataVersion
advances:

```
(S, C) -> (S, C+1)
```

Schema-bump compaction that updates the same segment's manifest/schema/storage
metadata without changing membership does not advance DataView. If a schema-bump
task performs a full replacement with a different loadable segment ID, it is a
compact rewrite and advances `compact_version`.

### 6.4 L0 Compact Events

`OnL0Compact` is triggered by Level-0 delete compaction.

L0 segments are delete-log carriers, not loadable sealed segments. L0 compaction
can update manifests, append deltalogs to target segments, and drop L0 input
segments, but it does not add or remove `DataViewOfPartition.segment_ids`.

Therefore `OnL0Compact` only refreshes the derived
`delete_apply_start_after_timetick`. It does not advance DataVersion unless the
same mutation also changes non-L0 loadable membership.

### 6.5 DDL / Trim Events

Some membership changes are not caused by segment production:

- `OnDropPartition` removes loadable segments in the dropped partition.
- `OnTruncate` removes loadable segments before the truncate fence.
- `OnDropCollection` deletes the whole DataView record.

DropPartition and truncate advance `compact_version` if they remove loadable
membership:

```
(S, C) -> (S, C+1)
```

DropCollection does not need to advance DataVersion because the collection view
no longer exists.

DropPartition and truncate are DataView-first operations: DataCoord removes the
membership from DataView before marking the affected segment metadata dropped or
trimmed. This keeps QueryCoord from seeing a new QueryView candidate that still
contains logically removed membership.

Recovery must also respect the persisted DDL/trim intent, such as
collection/partition metadata or truncate metadata, so a crash after DataView
update but before segment metadata update does not cause the removed segments to
be rebuilt back into DataView.

There is no separate `OnDropChannel` event in the current DataCoord behavior.
Channel-level effects should be represented by the actual DDL/trim operation
that changes membership, or by collection drop.

### 6.6 Delayed Visibility

Some flows write segment metadata before the segment is allowed to be loaded.

Examples:

- Sort-enabled flush may mark a flushed segment invisible while stats/index
  preparation continues.
- Clustering compaction writes invisible output segments first, then marks them
  visible only after the required preparation succeeds.

Invisible segments are not in DataView. DataVersion advances only when the
segment crosses into or out of the loadable predicate.

If an invisible output segment is later dropped without becoming loadable,
DataView does not change.

### 6.7 Non-Membership Updates

Changes to an already-loadable segment do not change DataView when membership is
unchanged.

Examples:

- `BatchUpdateManifest` updates a manifest pointer or column groups.
- Segment-level `data_version` changes for QueryNode reopen.
- Binlog, manifest, schema, storage-version, or stats metadata changes.
- Index metadata changes.
- L0 deltalogs are appended to an existing loadable segment's manifest.

These changes may affect QueryNode load/reopen behavior through other metadata
paths, but DataView does not advance DataVersion for them.

### 6.8 L0 Delete Segments

L0 segments are delete-log carriers, not loadable sealed segments.

Rules:

- L0 create does not add a segment to `DataViewOfPartition.segment_ids`.
- L0 compaction/drop does not advance DataVersion unless loadable membership
  also changes.
- L0 state can affect the derived `delete_apply_start_after_timetick`.

### 6.9 Copy Segment Complete Events

`OnCopySegmentComplete` is triggered when a copy segment task has persisted the
target segment result into DataCoord metadata.

Snapshot restore is the upstream business flow that creates copy segment jobs,
but the restore request itself does not make data loadable. A copied target
segment joins DataView only after its binlog/manifest/index metadata has been
written and it satisfies the loadable predicate, for example:

```
state == Flushed
&& !is_importing
&& !is_invisible
&& level != L0
```

When target segments become loadable, the join advances:

```
(S, C) -> (S+1, 0)
```

Partial copy progress, restore job creation, copied index metadata before the
target segment is loadable, and task state updates that do not change loadable
membership do not change DataView.

### 6.10 External Collection Refresh

`OnExternalRefresh` is triggered when an external collection refresh applies its
segment patch.

External refresh can add new loadable segments, patch existing segment content,
and remove stale loadable segments. DataView handles only membership changes:

- pure addition of new loadable segments advances `(S, C) -> (S+1, 0)`;
- removal or replacement of existing loadable membership advances
  `(S, C) -> (S, C+1)`;
- patching an existing segment without changing membership does not advance
  DataVersion.

If one refresh batch both adds and removes membership, it is a rewrite of the
external source snapshot and should advance `compact_version` once.

### 6.11 DropCollection

DropCollection deletes the whole DataView. It does not need to advance
DataVersion because the collection view no longer exists. QueryCoord releases
QueryViews through the load-config release path.

## 7. QueryCoord Consumption

QueryCoord implements Balancer's `DataViewProvider` by reading DataCoord's
visible DataView snapshot.

```
DataCoord DataViewManager
        |
        | GetDataView / watch / refresh
        v
QueryCoord DataViewProvider
        |
        v
BalancerSnapshot.DataViewSnapshot
        |
        v
BalancePolicy.Plan(...)
        |
        v
QueryViewAtCoordBuilder(DataView, assignments)
```

The Balancer treats DataView as immutable during one reconcile cycle. If
DataVersion advances while a plan is being built, that new DataView is consumed
by the next reconcile cycle.

QueryCoord must request a visible view, for example through
`LatestVisibleDataView(collectionID)`. It must not select the maximum persisted
DataVersion by itself, because the latest persisted DataView may be a temporary
flush view that is not yet allowed to participate in QueryView construction.

Phase 1 uses DataVersion comparison:

```
current Up QueryView DataVersion < latest DataView DataVersion -> Must prepare new QueryView
```

Phase 2 uses the DataView shard membership as the segment set to allocate.

QueryCoord must communicate with segment metadata through DataView semantics.
It must not derive loadable membership from raw segment metadata and must not
repair DataView holes locally.

LoadInfo is outside DataView management. QueryNode obtains load metadata through
its own additional RPC path. DataView only decides which segment IDs belong to a
view; it does not own binlog/manifest/index lookup or QueryNode load metadata
delivery.

## 8. Delete Data Eviction

`DataViewOfShard.delete_apply_start_after_timetick` is DataView-derived
metadata because it depends on the data that must remain queryable, not on query
placement.

It is derived from the segment IDs contained in the current DataView.
Conceptually, for one shard:

```
delete_apply_start_after_timetick =
    min(segment.delete_apply_start_after_timetick for every segment in the current DataView shard)
```

The calculation does not re-check whether those segments are currently
loadable, and it does not include historical retained DataViews. It only uses
the segment IDs already present in the current DataView membership.

`segment.dml_position.Timestamp` must not be used as this source. Its existing
meaning is overloaded:

- normal flush updates it as the segment checkpoint / end position;
- import sets it from imported row timestamp range;
- compaction recalculates it from output binlog timestamp range or input
  fallback positions;
- GC and truncate already apply their own effective timestamp rules.

DataCoord therefore owns a separate persisted segment metadata field:

```proto
message SegmentInfo {
    // Exclusive lower bound for delete data that must be retained/applied when
    // this segment is loaded. DataView derives shard-level
    // delete_apply_start_after_timetick from this field.
    uint64 delete_apply_start_after_timetick = 37;
}
```

New segment-producing paths must populate this field explicitly:

- **Flush / StreamingNode flush**: use the segment start position or create
  segment timetick. For StreamingNode-managed L1 segments this is the segment
  assignment's create-segment timetick, not the data checkpoint timetick.
- **Import**: import segments join DataView only after commit. Use the import
  commit timestamp, matching the QueryNode rule that an import segment becomes
  visible at its commit fence.
- **Copy / snapshot restore**: copy the source segment's
  `delete_apply_start_after_timetick`.
- **Non-L0 compaction**: inherit the minimum
  `delete_apply_start_after_timetick` from all input segments. Compaction
  replaces membership and must not shorten the delete retention window by using
  output binlog timestamp ranges.
- **Sort compaction**: inherit the input segment's
  `delete_apply_start_after_timetick`, even if row timestamps or positions are
  rewritten in the output segment.
- **L0 compaction**: L0 segments do not join DataView membership. L0 compaction
  can refresh the derived shard timetick, but it does not write membership for
  a loadable segment and does not advance DataVersion by itself.

For existing segment metadata that predates this field, DataCoord derives a
compatible value when building or recovering DataView:

```go
func segmentDeleteApplyStartAfterTimetick(segment) uint64 {
    if segment.delete_apply_start_after_timetick != 0 {
        return segment.delete_apply_start_after_timetick
    }
    if segment.commit_timestamp != 0 {
        return segment.commit_timestamp
    }
    if segment.start_position != nil {
        return segment.start_position.Timestamp
    }
    return 0
}
```

This fallback is intentionally conservative. Old normal flushed segments fall
back to their start position, old committed import segments fall back to their
commit timestamp, and very old segments without start position fall back to
`0`, which may retain more delete data but will not evict required deletes.

Snapshot/restore metadata must persist the new segment field. If an older
snapshot manifest does not contain it, restored segments use the same fallback
rules above. The DataView record itself still does not need to persist
`delete_apply_start_after_timetick`; it is derived from segment metadata when
DataView is returned or synced.

L0 delete state can affect this derived frontier, but L0 segments still do not
join DataView membership.

StreamingNode delete eviction is safe without depending on the latest DataView
alone because StreamingNode applies its own minimum across the DataView
timeticks it has received and still needs to retain.

Delete frontier changes do not advance DataVersion. They also do not require a
new QueryView because segment membership is unchanged.

DataCoord syncs the lightweight timetick through `SyncDataView`:

```proto
message DataViewShardTimeTick {
    string vchannel = 1;
    uint64 delete_apply_start_after_timetick = 2;
}
```

This path is valid for loaded and unloaded collections. QueryView may carry the
same field as part of a normal membership update, but timetick-only changes are
metadata refreshes and must not drive QueryView state-machine transitions.

## 9. Snapshot Semantics

DataView is a loadable segment membership snapshot at a DataVersion.

When a QueryView is built from DataVersion `D`, workers execute that QueryView
against exactly the loadable membership in `DataView(D)`. Later membership
changes produce a newer DataVersion and do not change existing QueryViews in
place.

Segment content changes after a segment has joined DataView do not change
`DataView(D)`. They are handled by segment metadata, QueryNode reopen/reload, or
other content-version mechanisms.

Delete frontier changes after a segment has joined DataView also do not change
`DataView(D)` or QueryViewVersion. They are propagated as DataView-derived
metadata refreshes.

This provides:

1. Stable query inputs during two-phase query execution.
2. Clear generation of replacement QueryViews when loadable membership changes.
3. Independent QueryVersion evolution for placement-only changes.

## 10. GC And Retention

Segment state and physical GC are separated from DataView membership.
Compaction, DropPartition, and truncate may mark segment metadata as
`Dropped`/`Compacted` before old DataViews are gone. That state means the
segment should not join future DataViews; it does not mean the segment can no
longer be loaded by an already-retained DataView.

Physical cleanup of segment files, binlogs, manifests, and indexes is allowed
only after all of the following are true:

1. No retained DataView references the segment.
2. No QueryCoord-side QueryView can still reference the segment.
3. Existing DataCoord GC safety checks for object storage and metadata cleanup
   also pass.

Therefore GC must use DataView retention as one of its inputs. DataView
versions can be removed only after QueryCoord no longer has QueryViews that
reference them.

Because DataCoord and QueryCoord run in the same coordinator process for this
design, DataView GC does not need a separate cross-service retention protocol in
the first implementation:

- If a collection is not loaded, DataCoord only keeps the latest DataView, or a
  small configurable number of latest DataViews.
- If a collection is loaded, any DataView referenced by a live QueryView cannot
  be GCed.
- Segment physical GC runs after DataView GC. A segment can be deleted only
  after no retained DataView and no QueryCoord-side QueryView can still
  reference it.

DataView history is linear, so the last retained DataView contains enough
membership information for future event handling. GC only checks whether a
DataView is referenced by QueryCoord-side QueryViews; it does not need to
consider the DataView's visible/unavailable state.

## 11. Recovery

On DataCoord startup:

1. Load DataView records from the metastore.
2. Load segment metadata.
3. Compute the expected DataView from segment metadata using the same logic as
   the current `GetRecoveryInfoV2` path.
4. Select the maximum persisted DataVersion for each collection as the latest
   persisted DataView.
5. Compare the computed DataView with the latest persisted DataView.
6. If they match, use segment metadata to decide whether the latest DataView can
   participate in load. Expose it only when it is available for QueryView
   construction.
7. If they differ, persist one reconciled full snapshot using the priority rules
   in Section 5.2, then use segment metadata to decide whether the reconciled
   DataView can participate in load.

Recovery reconciliation must preserve DataVersion monotonicity. If DataView is
missing or older than segment metadata for a collection, DataCoord rebuilds the
loadable membership from segment metadata, assigns the next DataVersion using
the reconciliation rules in Section 5.2, and persists the rebuilt version before
exposing it to QueryCoord. A pure `delete_apply_start_after_timetick`
difference does not create a new DataVersion; DataCoord refreshes the derived
timetick for the latest view and syncs it through the normal DataView metadata
refresh path.

For DataView-first DropPartition or truncate, recovery must apply the persisted
DDL/trim metadata when computing the expected DataView, even if the segmentMeta
state mutation has not completed yet.

Because each DataView version is persisted as a single full snapshot value,
DataCoord treats every readable version key as a complete persisted snapshot.
There is no separate durable unpublished-version namespace in the first
implementation. QueryCoord visibility for temporary flush snapshots is an
in-memory DataCoord state recovered from segment metadata.

DataView's shard-level `delete_apply_start_after_timetick` is recomputed during
recovery from segment metadata. The per-segment source field is durable segment
metadata; if it is absent on old segment records or old snapshot manifests,
DataCoord uses the compatibility fallback from Section 8.

On QueryCoord startup:

1. Recover QueryView state from its own catalog.
2. Refresh DataView snapshots from DataCoord.
3. Trigger a full Balancer reconcile.

If recovered QueryViews are older than DataView, Balancer creates replacement
QueryViews. If load config is absent, Balancer releases residual QueryViews.

## 12. Test Matrix

The first implementation should cover at least:

1. SegmentMeta-first event succeeds in segment metadata but DataView persistence
   fails; recovery later rebuilds one reconciled DataView.
2. Flush temporary DataView `(S+1, 0)` is persisted but unavailable; after
   restart it is still not exposed to QueryCoord.
3. Sort-compaction handoff replaces the temporary flush segment, advances
   `(S+1, 0) -> (S+1, 1)`, and makes the DataView visible.
4. Recovery sees both flush/import additions and compact handoff; streaming
   version wins and advances to `(S+1, 0)`.
5. DropPartition or truncate updates DataView first, crashes before segmentMeta
   mutation, and recovery does not add removed segments back.
6. L0 compact refreshes `delete_apply_start_after_timetick` without advancing
   DataVersion.
7. Delete-timetick-only recovery refreshes metadata without creating a new
   DataVersion.
8. Duplicate, stale, or already-reconciled events are no-op success cases.

## 13. Invariants

1. DataView contains no duplicate segment IDs.
2. QueryCoord-visible DataView membership contains only segments that are
   allowed to participate in load for that DataVersion.
3. DataVersion never rolls back for an existing collection DataView.
4. A DataVersion observed by QueryCoord corresponds to a complete persisted
   loadable segment membership.
5. QueryCoord never mutates DataView or assigns DataVersion.
6. QueryView generation uses DataView as immutable input.
7. DataView membership changes are represented by DataVersion changes.
8. Placement changes are represented by QueryVersion changes.
9. Delete frontier changes are represented by DataView-derived metadata refresh,
   not by DataVersion or QueryVersion changes.
10. Segment content changes after joining DataView do not change DataView
    membership or DataVersion.
11. QueryCoord derives loadable membership only from DataView, not from raw
    segment metadata.
12. Physical segment GC must wait until no retained DataView or QueryCoord-side
    QueryView can reference the segment.
13. DataView may lag segment metadata, but DataView must not publish membership
    that segment metadata cannot describe.
14. QueryCoord only consumes DataViews that DataCoord marks visible/available
    for QueryView construction.

## 14. Open Implementation Choices

1. **Notification path**: QueryCoord can initially refresh DataView through
   polling or explicit trigger, then later switch to watch/event delivery.
2. **Unavailable flush DataViews**: implementation should define the exact
   in-memory state name and transition point for the current flush plus
   sort-compaction path.
3. **Rebuild trigger**: implementation should define when DataCoord performs
   recovery-only reconciliation versus online repair after a failed DataView
   persistence attempt.
4. **Delete frontier notification**: implementation should decide whether
   timetick-only refreshes are pushed directly by DataCoord to StreamingNode or
   routed through a QueryCoord cache before `SyncDataView`.
