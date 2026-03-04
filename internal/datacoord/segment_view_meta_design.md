# Segment View Meta Design

## Overview

`segmentViewMeta` brings two related responsibilities into one module:

1. **DataView management**: versioned queryable segment views for flush, import, snapshot restore, compaction, and GC.
2. **Segment metadata integration**: segment metadata and DataView updates share the same collection-level lock and are persisted through coordinated catalog mutations.

The design goal is to make query visibility explicit. A segment is queryable only when it belongs to the current DataView. Segment state and query visibility are intentionally separated so DataCoord can persist segment mutations before the segment becomes visible to QueryNode.

---

## DataView Design

### What Is A DataView

A DataView is the queryable segment view of one collection. It records the set of segments that can serve queries at a point in time, organized by shard and partition.

A segment enters a DataView only after it is ready for query service:

- **Flush**: StreamingNode produces sorted flushed segments. Flush and DataView update complete in one atomic operation.
- **Compaction**: Compaction output segments need index readiness before they can serve queries. Segment metadata is persisted first; DataView is updated after index readiness.
- **Import and snapshot restore**: Segments are registered first, then activated into DataView after data and index readiness.

DataView uses a two-dimensional version:

```text
(streaming_version, compact_version)
```

- `streaming_version` is increased by flush, import activation, snapshot restore activation, partition drop, and collection truncate.
- `compact_version` is increased by compaction handoff.
- When `streaming_version` increases, `compact_version` is reset to `0`.

Each version records the segment distribution of the collection at that moment.

### Flush: One Atomic Step

StreamingNode already sorts segments before flush. Flushed segments are queryable immediately, so segment metadata mutation and DataView update happen in one catalog batch.

```go
FlushSegments(ctx context.Context, collectionID int64, segmentIDs []int64, operators ...UpdateOperator)
  lock collection
    1. Prepare segment mutations through UpdateOperator.
    2. Build a new DataView:
       streaming_version = current.streaming_version + 1
       compact_version   = 0
       clone current view, then add flushed segments by InsertChannel and PartitionID
    3. catalog.AlterSegmentsAndSaveDataView()
    4. Update segment memory through setSegment.
    5. Update DataView memory through addDataViewForFlush.
  unlock collection
```

The operation supports multi-segment flush, including manual flush. One batch produces one new DataView version.

### Segment Registration And Activation

Import and snapshot restore share the same two-step lifecycle:

1. Register segment metadata while the segments are not queryable.
2. Activate all ready segments into DataView atomically.

The two workflows differ only in readiness:

- **Import**: DataNode writes data, sorts it, and builds indexes. Activation waits for index readiness.
- **Snapshot restore**: DataNode copies binlog and index files from the source collection through `CopySegmentAndIndexFiles`. The copied index is ready when the copy task finishes.

#### RegisterSegments

`RegisterSegments` persists a batch of segments and inserts them into memory without changing DataView.

```go
RegisterSegments(ctx, collectionID, segments)
  lock collection
    1. Persist segments with state=Importing and IsImporting=true.
    2. Insert segments into memory.
    3. Leave DataView unchanged.
  unlock collection
```

Call sites:

- **Import**: when an import job enters the Importing phase, segments are created by vchannel, partition, and size.
- **Snapshot restore**: `snapshotManager.createRestoreJob` allocates new target segment IDs for source segments and registers them.

During import execution, DataNode reports row count and binlogs incrementally. `UpdateSegments` updates segment metadata and may set the segment state to Flushed, while `IsImporting` remains true.

When sort compaction is enabled, import completion triggers sort compaction for each imported segment and then follows the regular compaction lifecycle.

#### ActivateSegments

`ActivateSegments` clears `IsImporting` and adds all ready segments to a new DataView version atomically.

```go
ActivateSegments(ctx, collectionID, segmentIDs)
  lock collection
    1. Set IsImporting=false for each segment.
    2. Build a new DataView:
       streaming_version = current.streaming_version + 1
       clone current view, then add all activated segments
    3. catalog.AlterSegmentsAndSaveDataView()
    4. Update segment memory and DataView memory.
  unlock collection
```

Call sites:

- **Import**: `importChecker` calls activation after all indexes are ready. All segments under the import job enter DataView in the same operation.
- **Snapshot restore**: `copySegmentChecker.finishJob` calls activation after all copy tasks finish. Index files are already copied with segment files.

If import goes through sort compaction, the segments entering DataView are the sorted segment IDs, not the original imported segment IDs.

Growing segments are registered by StreamingNode through the segment registration path owned by `segmentViewMeta`.

### Compaction: Two-Step Handoff

Compaction output segments need indexes before query service. Adding unindexed output segments to DataView and dropping input segments would degrade query performance, so compaction uses two coordinated steps.

#### CompleteCompactionMutation

After DataNode finishes compaction execution, DataCoord persists compactTo segments immediately, but DataView still references compactFrom segments.

```go
CompleteCompactionMutation(ctx, task, result)
  lock collection
    1. Validate input segments.
    2. Create compactTo segments in Flushed state.
    3. Mark compactFrom segments as State=Dropped and Compacted=true.
    4. catalog.AlterSegments()
    5. Update segment memory.
    6. Leave DataView unchanged.
  unlock collection
```

State after mutation:

- `compactFrom`: `State=Dropped`, `Compacted=true`, `isCompacting=true`, and still present in DataView. QueryNode continues using it. GC cannot reclaim it because active DataView versions still reference it.
- `compactTo`: `State=Flushed`, `isCompacting=true`, and not present in DataView. It waits for index readiness.

`isCompacting=true` remains until handoff completes, so both input and output segments are excluded from further compaction selection.

After mutation, DataCoord sends build-index notifications for compactTo segments:

```go
for _, segID := range compactToSegIDs {
    select {
    case getBuildIndexChSingleton() <- segID:
    default:
    }
}
```

#### DataView Update

Compaction output segments enter DataView only after index readiness. DataView update is driven by the compaction task state machine.

After segment metadata is saved, the task enters the `indexing` state. `compactionInspector` polls index status from in-memory index metadata. Once all result segments are ready, it completes the handoff.

```text
pipelining -> executing -> meta_saved -> indexing -> completed -> cleaned
                              |            |           |
                         mutation       wait index   handoff
```

`processMetaSaved` selects the next state based on output segment properties:

- **Sort compaction**: output is already sorted. It does not wait for index in this path and moves to `completed`.
- **Mix compaction, mergeSortMultipleSegments path**: output has `IsSorted=true` and moves to `indexing`.
- **Mix compaction, mergeSplit path**: output has `IsSorted=false`. The task triggers sort compaction through `statsTaskCh`, enters the `statistic` stage, waits for sort completion, and then moves to `indexing`.

`processIndexing` polls all result segment index states. If the collection has no index definition, the task can complete directly. When every required index is `Finished`, it calls `completeTask`, which performs `CompleteCompactionHandoff`.

```go
CompleteCompactionHandoff(ctx, collectionID, compactFromIDs, compactToIDs, vchannel, partitionID)
  lock collection
    1. Build a new DataView by removing compactFrom and adding compactTo.
    2. catalog.SaveDataView()
    3. Update DataView memory.
    4. resetSegmentCompacting()
  unlock collection
```

This keeps all DataView handoff logic in the compaction task lifecycle and avoids multiple trigger points between index completion, DropIndex compensation, and startup recovery.

#### Startup Recovery For Compaction Products

During startup, DataCoord rebuilds in-memory metadata before starting compaction scheduling:

```text
Server.Init()
  |
  +-- initMeta()
  |     +-- newMeta()
  |          +-- reloadFromKV()          // segments, channel checkpoints, DataViews
  |          +-- newIndexMeta()          // index definitions and segment index states
  |          +-- newCompactionTaskMeta() // compaction tasks
  |          +-- newAnalyzeMeta()
  |          +-- newPartitionStatsMeta()
  |          +-- newStatsTaskMeta()
  |          +-- newSnapshotMeta()
  |
  +-- initCompaction()
  |     +-- newCompactionInspector()
  |     +-- loadMeta()
  |     +-- recoverOrphanCompactionProducts()
  |     +-- NewCompactionTriggerManager()
  |
  +-- startCompaction()
        +-- compactionInspector.start()
        +-- compactionTrigger.start()
```

`recoverOrphanCompactionProducts` runs after `loadMeta()` in `initCompaction()`. At that point:

- segments are loaded, so all segment metadata can be scanned;
- compaction task metadata is loaded, so existing task result segments can be indexed;
- DataViews are loaded, so membership can be checked;
- index metadata is loaded, so synthetic tasks can enter `processIndexing` normally.

Recovery builds a reverse index from result segment ID to compaction task, scans flushed compaction products that are not in any DataView, and restores an `indexing` task for each product.

Existing compaction tasks are reused when possible. If no task metadata remains for a compaction product, recovery creates a synthetic MixCompaction task with:

- `State=indexing`
- `Type=MixCompaction`
- `CollectionID`, `PartitionID`, and `Channel` from the segment
- `InputSegments` from `CompactionFrom`
- `ResultSegments` containing the orphan output segment

The synthetic task then follows the same index polling and handoff path as a normal task.

### Compaction Task State And DataView State

```text
pipelining / executing
  compactFrom: Flushed, isCompacting=true
  compactTo:   absent
  DataView:    contains compactFrom

meta_saved / indexing
  compactFrom: Dropped, Compacted=true
               still in DataView and still used by QueryNode
  compactTo:   Flushed, waiting for index
               not in DataView
  DataView:    unchanged, still contains compactFrom

after DataView handoff
  compactFrom: Dropped
               removed from DataView and eligible for GC when no view references it
  compactTo:   Flushed, indexed
               added to DataView
  DataView:    compact_version + 1, contains compactTo
```

### IsInvisible Retirement

DataView owns query visibility:

- A segment outside DataView is not queryable.
- `compactTo.isCompacting=true` prevents compactTo segments from being selected for compaction before handoff.
- `GetRecoveryInfo` reads segment lists from DataView.
- Cluster compaction temporary segments are sorted through `statsTaskCh`; they do not depend on `IsInvisible` filtering in candidate selection.

`IsInvisible` remains during the transition while `retrieveSegment` still depends on it. It can leave the query visibility path once DataView fully owns recovery and query views.

### Failure Recovery

| Crash point | Restart behavior |
|-------------|------------------|
| During compaction execution | The task remains in `executing` and is dispatched to DataNode again. |
| Segment metadata saved, index not ready | The task restores to `indexing` and continues polling index state. |
| Index ready, DataView write interrupted | `CompleteCompactionHandoff` is idempotent and can run again. |
| Compaction product has no task metadata | `recoverOrphanCompactionProducts` creates a synthetic task and resumes from `indexing`. |

DataView never exposes a partial handoff state. It either still references compactFrom or fully references compactTo.

### GC And DataView

GC can physically remove a Dropped segment only when all of the following are true:

1. `dropTolerance` has expired.
2. No active DataView version references the segment.
3. No QueryNode has loaded the segment.
4. No snapshot references the segment.

---

## Segment Metadata Integration

### Motivation

Segment metadata and DataView updates must be coordinated. Flush, import activation, snapshot restore activation, compaction handoff, partition drop, and collection truncate all need segment state changes and DataView changes to be consistent.

`segmentViewMeta` stores segment metadata and DataViews together, uses the same collection-level lock, and persists coordinated mutations through catalog methods.

### Data Structures

`segmentViewMeta` owns segment metadata, secondary indexes, compaction lineage, and DataViews:

```go
type segmentViewMeta struct {
    ctx      context.Context
    catalog  metastore.DataCoordCatalog
    collLock *lock.KeyLock[int64] // per-collection RWMutex

    // segments is safe for concurrent access across collections.
    // coll2Segments inner maps are protected by collLock.
    segments      *ConcurrentMap[UniqueID, *SegmentInfo]
    coll2Segments *ConcurrentMap[int64, map[UniqueID]*SegmentInfo]
    compactionTo  *ConcurrentMap[UniqueID, []UniqueID]

    // dataViews outer map is concurrent; each collectionDataViews value is protected by collLock.
    dataViews *ConcurrentMap[int64, *collectionDataViews]
}
```

### DataView In-Memory Layout

The persisted `DataViewOfCollection` proto uses repeated nested fields, which are suitable for serialization. In memory, DataView uses maps for O(1) updates and lookups.

```go
type CollectionDataView struct {
    collectionID int64
    version      *viewpb.DataVersion
    shards       map[string]*ShardDataView // vchannel -> shard
}

type ShardDataView struct {
    vchannel                      string
    deleteApplyStartAfterTimetick uint64
    partitions                    map[int64]map[int64]struct{} // partitionID -> segmentID set
}

type collectionDataViews struct {
    views          map[dataViewVersionKey]*CollectionDataView
    currentVersion *viewpb.DataVersion
    versionList    []*viewpb.DataVersion // sorted by version ascending
}
```

### Lock Model

`collLock` is a per-collection RWMutex based on `lock.KeyLock[int64]`. Different collections can proceed independently.

The module uses two layers of protection:

- **ConcurrentMap layer**: `segments` and `compactionTo` support safe individual reads and writes.
- **Collection lock layer**: `coll2Segments` inner maps and `collectionDataViews` are protected by `collLock`.

Read methods use locks only when needed:

| Method category | Lock behavior |
|-----------------|---------------|
| `GetSegment`, `GetCompactionTo` | Read through ConcurrentMap only. |
| `GetSegmentsBySelector` with collectionID | Use `collLock.RLock` and read `coll2Segments`. |
| `GetSegmentsBySelector` without collectionID | Use `segments.Range()` without an additional collection lock. |
| DataView reads | Use `collLock.RLock` for `collectionDataViews`. |

All write methods hold `collLock.Lock(collectionID)`.

### SegmentManager Integration

With StreamingNode enabled, segment allocation and sealing decisions are owned by StreamingNode. DataCoord's SegmentManager responsibilities reduce to metadata registration and cleanup, which are covered by `segmentViewMeta`.

#### SN-Mode Method Status

Methods not used by the SN path:

| Method | Notes |
|--------|-------|
| `AllocSegment` | Deprecated. StreamingNode manages row quota directly. |
| `SealAllSegments` | Called only when streaming service is disabled. |
| `GetFlushableSegments` | No external callers. |
| `ExpireAllocations` | No external callers. |
| `tryToSealSegment` | Internal helper used only by `GetFlushableSegments`. |

Methods with remaining SN-mode behavior:

| Method | Output |
|--------|--------|
| `AllocNewGrowingSegment` | Builds a `SegmentInfo` from SN-provided fields and registers it. |
| `CleanZeroSealedSegmentsOfChannel` | Cleans zero-row sealed segments after channel checkpoint advances. |
| `DropSegment` | Maintains in-memory channel indexes. |
| `DropSegmentsOfChannel` | Maintains in-memory channel indexes when a channel is offline. |
| `DropSegmentsOfPartition` | Maintains in-memory channel indexes when a partition is dropped. |
| `loadSegmentsFromMeta` | Rebuilds channel indexes during startup. |

#### SegmentManager Outputs Covered By segmentViewMeta

`segmentViewMeta` covers the remaining outputs directly:

1. **Growing segment registration**: StreamingNode provides collection ID, partition ID, segment ID, channel, and storage version. `segmentViewMeta.RegisterGrowingSegment()` builds and registers the `SegmentInfo`.
2. **Zero-row sealed segment cleanup**: candidates can be found from segment metadata by channel and state, so cleanup is handled in the channel checkpoint update path.

`channel2Growing` and `channel2Sealed` are in-memory mirrors of segment state. Their lookups are covered by `coll2Segments` plus channel and state filtering.

Startup reconstruction is covered by `segmentViewMeta.reloadFromKV`, which loads segment metadata and DataViews together.

### API Surface

The final segment metadata API has four groups:

1. Native `segmentViewMeta` methods.
2. Thin `meta` wrappers that add cross-meta logic or semantic checks.
3. Direct selector usage for one-off convenience queries.
4. Memory-only mutation options through `ModifySegments`.

#### Methods Outside The Final API Surface

These methods have no external production callers or are covered by selector-based access:

| Method | Coverage |
|--------|----------|
| `HasSegments(segIDs)` | Batch existence checks use selector or direct `GetSegment`. |
| `GetSegmentsByChannel(channel)` | Channel queries use `SelectSegments`. |
| `GetSegmentsIDOfCollection(ctx, collID)` | Collection segment ID queries use `SelectSegments`. |
| `GetSegmentsIDOfPartition(ctx, collID, partID)` | Partition segment ID queries use `SelectSegments`. |
| `GetSegmentsChannels(segIDs)` | Segment channel mapping uses direct segment reads. |
| `SetRowCount(segID, count)` | Row count updates go through segment update operators. |
| `SetSegmentCompacting(segID, bool)` | Compacting is set in batches through `SetSegmentsCompacting`. |
| `SetSegmentLevel(segID, level)` | Segment level updates go through segment update operators. |

#### Native segmentViewMeta Methods

Segment read methods:

| Method | Purpose |
|--------|---------|
| `GetSegment(segID)` | Direct lookup by segment ID, O(1) through ConcurrentMap. |
| `GetSegments()` | Return all segments. |
| `GetSegmentsBySelector(filters...)` | Generic filtered query. Uses `coll2Segments` when collection ID is available. |
| `GetCompactionTo(segID)` | Read compaction lineage. |

Segment write methods:

| Method | Purpose |
|--------|---------|
| `DropSegment(ctx, collID, segID)` | Physical segment removal for GC, including catalog and memory updates. |
| `UpdateSegments(ctx, collID, operators...)` | Unified persisted segment proto field update entry. |
| `ModifySegments(collID, segIDs, opts...)` | Batch updates for memory-only transient fields such as `isCompacting`, allocations, and flush timestamps. |

Segment and DataView atomic operations:

| Method | Purpose |
|--------|---------|
| `FlushSegments(ctx, collID, segmentIDs, operators...)` | Update flushed segment state and DataView in one operation. |
| `CompleteCompactionMutation(ctx, task, result)` | Persist compactTo segments and compactFrom segment state without DataView handoff. |
| `CompleteCompactionHandoff(ctx, collID, fromIDs, toIDs, vchannel, partID)` | Update DataView after compaction output index readiness. |
| `RegisterSegments(ctx, collID, segments)` | Register import or snapshot restore segments with `IsImporting=true`. |
| `ActivateSegments(ctx, collID, segmentIDs)` | Clear `IsImporting` and add ready segments to DataView. |
| `DropPartition(ctx, collID, partitionIDs)` | Mark partition segments Dropped and remove the partition from DataView. |
| `TruncateCollection(ctx, collID, segmentIDs, flushTsList)` | Mark truncated segments Dropped, update shard delete timestamps, and bump DataView version. |

DataView methods:

| Method | Purpose |
|--------|---------|
| `GetCurrentVersion(collID)` | Return the current DataView version. |
| `GetDataView(collID, version)` | Return a deep copy of a specific DataView version. |
| `ListDataViews(collID)` | Return all versions in ascending order. |
| `DropDataView(ctx, collID, version)` | Drop an obsolete DataView version. |
| `DropDataViewsByCollection(ctx, collID)` | Drop all DataViews after collection drop. |

Internal helpers:

| Method | Purpose |
|--------|---------|
| `setSegment(segID, segment)` | Insert or replace one segment in memory and maintain indexes. |
| `dropSegmentFromMemory(segID)` | Remove one segment from memory and maintain indexes. |
| `addSecondaryIndex`, `removeSecondaryIndex` | Maintain `coll2Segments`. |
| `addCompactionTo`, `removeCompactionTo` | Maintain compaction lineage. |
| `addDataView`, `addDataViewForFlush`, `addDataViewForCompaction` | Insert DataViews into memory. |
| `buildCompactionDataView` | Build the DataView produced by compaction handoff. |
| `reloadDataViews` | Load DataViews from catalog. |
| `reloadFromKV(ctx, collectionIDs)` | Load segments and DataViews from catalog. |

#### Thin meta Wrappers

These methods remain on `meta` because they add cross-meta logic, semantic checks, or compatibility wrappers over `segmentViewMeta`.

| Method | Call count | Reason |
|--------|------------|--------|
| `GetHealthySegment(ctx, segID)` | 11 | Adds state checks on top of `GetSegment`. |
| `GetSegmentInfos(segIDs)` | 2 | Batch lookup by segment ID for import and compaction paths. |
| `GetSegments(segIDs, filterFunc)` | 7 | Applies custom filters to a specific segment ID list. |
| `SetState(ctx, segID, state)` | 2 | Looks up collection ID and delegates to `UpdateSegments`. |
| `UpdateSegmentsInfo(ctx, operators...)` | 14 | Compatibility entry that resolves collection ID from segment ID and delegates to `UpdateSegments`. |
| `CheckAndSetSegmentsCompacting(ctx, collID, segIDs)` | 1 | Atomically checks and sets compacting flags through `ModifySegments`. |
| `SetSegmentsCompacting(ctx, collID, segIDs, bool)` | 4 | Batch compacting flag update through `ModifySegments`. |
| `UpdateDropChannelSegmentInfo(ctx, channel, segments)` | 1 | Drop-channel specific segment merge and Dropped marking logic. |

Update entry consolidation:

- `UpdateSegments` is the only persisted segment proto field update entry.
- `UpdateSegmentsInfo(ctx, operators...)` resolves collection ID from segment IDs and delegates to `UpdateSegments`.
- `task_stats.go` and `copy_segment_task.go` use `UpdateOperator` to write TextIndexLogs and JsonKeyIndexLogs.
- `SegmentOperator func(*SegmentInfo) bool` and `UpdateSegment(segID, ...SegmentOperator)` are not part of the final API surface.

#### Direct Selector Usage

These convenience queries have one narrow use case and are represented by direct `SelectSegments` or `GetSegment` usage:

| Query | Equivalent |
|-------|------------|
| `GetRealSegmentsForChannel(channel)` | `SelectSegments(ctx, WithChannel(ch), SegmentFilterFunc(!IsFake))` |
| `GetAllSegmentsUnsafe()` | `SelectSegments(ctx)` |
| `GetSegmentsTotalNumRows(segIDs)` | Loop over `GetSegment` and sum `NumOfRows`. |
| `GetSegmentsOfCollection(ctx, collID)` | `SelectSegments(ctx, WithCollection(collID), SegmentFilterFunc(isSegmentHealthy))` |
| `GetSegmentsIDOfCollectionWithDropped(ctx, collID)` | `SelectSegments(ctx, WithCollection(collID), ...)`, then collect IDs. |
| `GetSegmentsIDOfPartitionWithDropped(ctx, collID, partID)` | `SelectSegments(ctx, WithCollection(collID), WithPartition(partID), ...)`, then collect IDs. |
| `GetNumRowsOfPartition(ctx, collID, partID)` | `SelectSegments`, then sum `NumOfRows`. |
| `GetUnFlushedSegments()` | `SelectSegments(ctx, SegmentFilterFunc(isGrowingOrSealed))` |
| `GetFlushingSegments()` | `SelectSegments(ctx, WithState(Flushing))` |
| `IsSegmentCompacting(segID)` | `GetSegment(segID).isCompacting` |
| `GetCompactableSegmentGroupByCollection()` | `SelectSegments`, then group by collection. |
| `GetEarliestStartPositionOfGrowingSegments(label)` | `SelectSegments`, then compute the minimum start position. |

#### Memory-Only Setter Options

Transient in-memory fields are updated through `svm.ModifySegments(collID, segIDs, opts...)`.

```go
func (m *meta) SetSegmentsCompacting(ctx context.Context, collID int64, segIDs []UniqueID, compacting bool) {
    m.svm.ModifySegments(collID, segIDs, SetIsCompacting(compacting))
}

func (m *meta) SetAllocations(collID int64, segID UniqueID, allocs []*Allocation) {
    m.svm.ModifySegments(collID, []UniqueID{segID}, SetAllocations(allocs))
}

func (m *meta) SetLastWrittenTime(collID int64, segID UniqueID) {
    m.svm.ModifySegments(collID, []UniqueID{segID}, SetLastWrittenTime())
}
```

`SetRowCount`, `SetDmlPosition`, and `SetStartPosition` are not part of this API surface for production usage.

### KV Storage

DataView is stored under:

```text
datacoord-meta/dataview/{collectionID}/{streamingVersion}/{compactVersion}
  -> DataViewOfCollection (proto bytes)
```

Catalog methods:

```go
SaveDataView(ctx, collectionID, view) error
ListDataViews(ctx) (map[int64][]*viewpb.DataViewOfCollection, error)
DropDataView(ctx, collectionID, version) error
DropDataViewsByCollection(ctx, collectionID) error
AlterSegmentsAndSaveDataView(ctx, segments, collectionID, view, binlogs...) error
```

`AlterSegmentsAndSaveDataView` writes segment KV entries and the DataView KV entry through the same `SaveByBatch` call. When `view` is nil, it behaves as a regular `AlterSegments` mutation.
