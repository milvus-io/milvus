# LoadDiff-Based Segment Self-Managed Loading Design Document

## Overview

This document describes the LoadDiff-based segment self-managed loading architecture in Milvus. The design moves segment loading orchestration from the Go layer to the C++ segcore layer, enabling segments to autonomously manage their loading process and support incremental updates (reopen) without full segment reloads.

## Related Issues and PRs

### Issues
- **[#45060](https://github.com/milvus-io/milvus/issues/45060)**: Make segment itself manage loading
- **[#46358](https://github.com/milvus-io/milvus/issues/46358)**: Support reopen segment on query nodes

### Key PRs (Merged)
| PR | Description | Status |
|----|-------------|--------|
| [#45061](https://github.com/milvus-io/milvus/pull/45061) | Add `NewSegmentWithLoadInfo` API to support segment self-managed loading | Merged |
| [#45488](https://github.com/milvus-io/milvus/pull/45488) | Move segment loading logic from Go layer to segcore | Merged |
| [#46359](https://github.com/milvus-io/milvus/pull/46359) | Support reopen segment for data/schema changes | Merged |
| [#46394](https://github.com/milvus-io/milvus/pull/46394) | QueryCoord support segment reopen when manifest path changes | Merged |
| [#46536](https://github.com/milvus-io/milvus/pull/46536) | Unify segment Load and Reopen through diff-based loading | Merged |
| [#46598](https://github.com/milvus-io/milvus/pull/46598) | Handle legacy binlog format (v1) in segment load diff computation | Merged |
| [#47061](https://github.com/milvus-io/milvus/pull/47061) | Support lazy load for index-has-raw-data fields in Storage V3 LoadDiff | Merged |
| [#47412](https://github.com/milvus-io/milvus/pull/47412) | Integrate default value filling into LoadDiff computation | Merged |

## Architecture

### Previous Architecture

In the previous design, the Go layer (`segment_loader.go`) was responsible for:
- Resource estimation and requests
- Concurrency control
- Loading orchestration (Load → LoadSegment → loadSealedSegment/LoadMultiFieldData)
- Waiting and synchronization

The C++ layer (segcore) was passive:
- Passively receiving data (LoadFieldData, LoadIndex, LoadDeletedRecord)
- No autonomous scheduling capability

### New Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              QueryCoord                                      │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     SegmentChecker                                   │    │
│  │  - Detects segment state changes (lacks, redundancies, updates)     │    │
│  │  - Creates Load/Reopen/Reduce tasks based on diff with target       │    │
│  │  - getSealedSegmentDiff() checks ManifestPath changes               │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ SegmentLoadInfo
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              QueryNode (Go)                                  │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     segmentLoader                                    │    │
│  │  - Resource estimation and protection                               │    │
│  │  - Delegates actual loading to segcore via C API                    │    │
│  │  - ReopenSegments() for incremental updates                         │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     LocalSegment                                     │    │
│  │  - Load() / Reopen() pass SegmentLoadInfo to C++                    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
                                    │
                                    │ C API (SegmentLoad / ReopenSegment)
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Segcore (C++)                                   │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     SegmentLoadInfo                                  │    │
│  │  - Wraps protobuf SegmentLoadInfo with caching                      │    │
│  │  - ComputeDiff(new_info) → LoadDiff                                 │    │
│  │  - GetLoadDiff() → LoadDiff (for initial load from empty state)    │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
│                                    │                                         │
│  ┌─────────────────────────────────────────────────────────────────────┐    │
│  │                     ChunkedSegmentSealedImpl                         │    │
│  │  - SetLoadInfo(proto) stores segment load configuration             │    │
│  │  - Load() calls GetLoadDiff() + ApplyLoadDiff()                     │    │
│  │  - Reopen(new_info) calls ComputeDiff() + ApplyLoadDiff()           │    │
│  │  - ApplyLoadDiff() executes incremental changes                     │    │
│  └─────────────────────────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Core Components

### 1. SegmentLoadInfo Class

Location: `internal/core/src/segcore/SegmentLoadInfo.h/cpp`

The `SegmentLoadInfo` class wraps the protobuf `SegmentLoadInfo` message and provides:

- **Accessor methods**: GetSegmentID(), GetNumOfRows(), GetIndexInfos(), GetBinlogPaths(), etc.
- **Caching**: Builds internal caches for quick field lookups
- **Index conversion**: Converts FieldIndexInfo to LoadIndexInfo with mmap settings
- **Diff computation**: ComputeDiff() and GetLoadDiff() methods

```cpp
class SegmentLoadInfo {
public:
    explicit SegmentLoadInfo(const ProtoType& info);

    // Snapshot inputs describe retained resources, not in-flight load progress.
    LoadDiff ComputeDiff(
        SegmentLoadInfo& new_info, const SealedFieldLayoutPtr& fields,
        const std::shared_ptr<const PublishedSegmentState>& existing_snapshot = {});
    LoadDiff GetLoadDiff(
        const SealedFieldLayoutPtr& fields,
        const std::shared_ptr<const PublishedSegmentState>& existing_snapshot = {});

    // Column groups support (Storage V3)
    std::shared_ptr<milvus_storage::api::ColumnGroups> GetColumnGroups();

private:
    void BuildCache();
    void ComputeDiffIndexes(LoadDiff& diff, SegmentLoadInfo& new_info);
    void ComputeDiffBinlogs(LoadDiff& diff, SegmentLoadInfo& new_info);
    void ComputeDiffColumnGroups(LoadDiff& diff, SegmentLoadInfo& new_info);
};
```

### 2. LoadDiff Structure

Location: `internal/core/src/segcore/SegmentLoadInfo.h`

The `LoadDiff` struct represents the difference between two segment load states:

```cpp
struct LoadDiff {
    // Indexes to load (field_id -> list of LoadIndexInfo)
    std::unordered_map<FieldId, std::vector<LoadIndexInfo>> indexes_to_load;

    // Binlogs to load (Storage V1/V2)
    std::vector<std::pair<std::vector<FieldId>, proto::segcore::FieldBinlog>> binlogs_to_load;

    // Column groups to load (Storage V3 manifest mode)
    std::vector<std::pair<int, std::vector<FieldId>>> column_groups_to_load;

    // Column groups for lazy loading (index has raw data)
    std::vector<std::pair<int, std::vector<FieldId>>> column_groups_to_lazyload;

    // Fields to reload (when index raw data availability changes)
    std::vector<FieldId> fields_to_reload;

    // Fields to fill with default values (schema evolution)
    std::vector<FieldId> fields_to_fill_default;

    // Indexes to drop
    std::set<FieldId> indexes_to_drop;

    // Field data to drop
    std::unordered_set<FieldId> field_data_to_drop;

    // Manifest update flag
    bool manifest_updated = false;
    std::string new_manifest_path;

    bool HasChanges() const;
    std::string ToString() const;  // For debugging
};
```

### 3. ApplyLoadDiff Method

Location: `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`

`ApplyLoadDiff()` prepares a cloned runtime state and uses the same
`ExecuteLoadDiffAsync()` orchestration as Reopen. It publishes the new snapshot
only after all preparation and resource finalization succeeds. Existing readers
retain their original snapshot while the new state is prepared.

### 4. Unified Load and Reopen

Both Load and Reopen operations now use the same diff-based mechanism:

Load calls `GetLoadDiff()`; Reopen calls `ComputeDiff()` with the target load
info and field layout. Both pass their immutable input state as
`existing_snapshot`; planning reads retained runtime resources directly from that
snapshot. They execute the resulting plan against staged resources and publish it
once preparation succeeds.

**Schema-aware Reopen:**

Schema changes no longer use a separate sealed-segment path that directly fills missing fields. QueryNode passes the latest collection schema together with `SegmentLoadInfo` into segcore, so `ComputeDiff()` can see newly added or removed fields while computing load/default-fill/drop actions.

For schema-only lazy checks from query/retrieve plans, `Reopen(SchemaPtr)` reuses the current load info and computes a diff against the newer schema. Default-value filling, field data drops, and index drops are therefore represented through `LoadDiff` and executed by `ApplyLoadDiff()`.

## Storage Mode Support

### Storage V1 (Legacy Binlog)

- Each field has its own binlog files
- `child_fields` is empty, field_id == group_id
- Diff computed via `ComputeDiffBinlogs()`

### Storage V2 (Column Groups)

- Multiple fields can share column groups
- `child_fields` contains field IDs in the group
- Diff computed via `ComputeDiffBinlogs()`

### Storage V3 (Manifest Mode)

- Uses Loon manifest for column group metadata
- Supports lazy loading for fields with index raw data
- Diff computed via `ComputeDiffColumnGroups()`
- `ManifestPath` changes trigger segment reopen

## QueryCoord Integration

### Segment Checker

Location: `internal/querycoordv2/checkers/segment_checker.go`

The segment checker detects segments that need reopen:

```go
func (c *SegmentChecker) getSealedSegmentDiff(...) (toLoad, loadPriorities, toRelease, toUpdate) {
    isSegmentUpdate := func(segment *datapb.SegmentInfo) bool {
        segInDist, existInDist := distMap[segment.ID]
        return existInDist && segInDist.ManifestPath != segment.GetManifestPath()
    }
    // ... detect updated segments and create reopen tasks
}

func (c *SegmentChecker) createSegmentReopenTasks(...) []task.Task {
    // Creates ActionTypeReopen tasks for updated segments
}
```

### Action Types

```go
const (
    ActionTypeGrow   ActionType = iota + 1
    ActionTypeReduce
    ActionTypeUpdate
    ActionTypeReopen  // New action type for segment reopen
)
```

## Current Capabilities

| Feature | Status | Description |
|---------|--------|-------------|
| Initial Load | ✅ Implemented | Load segment from empty state via GetLoadDiff() |
| Index Load | ✅ Implemented | Load new indexes (vector/scalar) |
| Index Drop | ✅ Implemented | Drop indexes when removed from target |
| Binlog Load | ✅ Implemented | Load field binlogs (Storage V1/V2) |
| Column Group Load | ✅ Implemented | Load column groups (Storage V3) |
| Lazy Loading | ✅ Implemented | Skip loading fields with index raw data |
| Field Reload | ✅ Implemented | Reload when index raw data changes |
| Schema Evolution | ✅ Implemented | Fill default values for new fields |
| Manifest Update | ✅ Implemented | Reopen when manifest path changes |
| Legacy Format | ✅ Implemented | Handle V1 binlog format |

## Future Goals

The ultimate goal is for **all load state changes** to use this LoadDiff-based mechanism:

### Planned Extensions

1. **Online Index Building**
   - Detect when new index is built for a field
   - Reopen segment to load the new index
   - Drop field data if index has raw data

2. **Online Schema Changes**
   - Add new fields with default values
   - Drop fields (remove field data)
   - Modify field properties

3. **Index Type Changes**
   - Switch between index types
   - Handle index parameter changes

4. **Compaction Integration**
   - Update segment after compaction
   - Handle segment merging

5. **Tiered Storage**
   - Move data between tiers
   - Update storage locations

## Benefits

1. **Clear Responsibilities**: Loading logic is entirely managed by the segment itself
2. **Performance Optimization**: Reduces Go-C++ cross-language calls
3. **Precise Resource Management**: C++ layer directly handles resource allocation
4. **Caching Integration**: Segments can directly interact with the caching layer
5. **Proactive Schema Evolution**: Segments are aware of all their own information
6. **Incremental Updates**: Avoid full segment reloads for minor changes
7. **Unified API**: Single ApplyLoadDiff() handles all loading scenarios

## Proto Definition

Location: `pkg/proto/segcore.proto`

```protobuf
message SegmentLoadInfo {
  int64 segmentID = 1;
  int64 partitionID = 2;
  int64 collectionID = 3;
  int64 dbID = 4;
  int64 flush_time = 5;
  repeated FieldBinlog binlog_paths = 6;
  int64 num_of_rows = 7;
  repeated FieldBinlog statslogs = 8;
  repeated FieldBinlog deltalogs = 9;
  repeated int64 compactionFrom = 10;
  repeated FieldIndexInfo index_infos = 11;
  string insert_channel = 13;
  int64 readableVersion = 14;
  int64 storageVersion = 15;
  bool is_sorted = 16;
  map<int64, TextIndexStats> textStatsLogs = 17;
  repeated FieldBinlog bm25logs = 18;
  map<int64, JsonKeyStats> jsonKeyStatsLogs = 19;
  common.LoadPriority priority = 20;
  string manifest_path = 21;  // Storage V3 manifest path
}
```

## Planning and Execution

### One raw-data action per field

`ComputeDiffBinlogs()` and `ComputeDiffColumnGroups()` classify each target field
using its previous source, target files, default-filled state, and previous/target
index raw-data capability. Losing index raw-data coverage is another reason for
the same field action, rather than a second pass that appends a duplicate
replacement. Grouped binlogs and eager manifest fields stay grouped; lazy
manifest entries remain one per field so unrelated lazy columns are not loaded
together. Eager versus lazy preparation follows the target field layout.

For example, if both an index change and new manifest files require field 101 to
be restored, the diff contains one replacement for 101. It does not first eagerly
restore the old selection and then replace it again with a lazy target column.
External manifests retain their dedicated loading path and select changed or
newly required data using the input snapshot.

### Text and interim prerequisites

`text_indexes_to_create` maps each field to `TextIndexBuildDependencies`:

- `Existing`: no new column preparation is scheduled.
- `Load`: wait for the field's load/replacement preparation.
- `FillDefault`: wait for default-value filling.
- `load_index`: also wait for the field's new/replacement scalar index when
  present. The Text builder supports both columns and scalar value readers;
  these are preparation dependencies, not a forced choice of data source.

Default planning uses the input snapshot's normalized field-ready bits to exclude
existing physical columns, even when a legacy direct load did not record them in
binlog metadata. Reopen no longer
rewrites the default actions or Text dependencies after computing the diff.
Loading an existing Text index from files remains independent of creating one
from raw values.

`interim_indexes_to_build` groups supported vector fields being prepared without
a persisted index in the complete target metadata or an interim index retained
from the input snapshot. Persisted-index identity survives metadata compaction.
Unchanged external columns do not cause new interim attempts. Execution still
checks configuration, supported index types, row thresholds, and nullable data;
it no longer uses another task's current ready state to decide whether to build.
Legacy direct `LoadFieldData()` constructs its fixed plan from its entry snapshot.
Each group builds serially, retaining the original data loader's grouping;
distinct groups may run in parallel. In particular, moving interim construction
out of an eager multi-vector column group does not fan its fields out into
additional concurrent builds. Lazy fields keep their separate groups.

### Execution boundary

Load and both Reopen paths retain their caller-side lock, input snapshot, staged
resources, and final publication. A single outer `blockingWait` runs
`ExecuteLoadDiffAsync()` on ASYNC_LOAD:

1. Capture source columns and index raw-data capabilities from the input snapshot
   and target metadata before starting any tasks. Initialize shared LOB metadata
   before concurrent readers can access it.
2. Start independent formal-index, raw-field, reload, pre-built Text, and JSON
   preparation tasks together. Their task vector is joined before dependent
   operations begin.
3. Build struct offsets, create Text indexes using existing/loaded data, fill
   defaults, and create Text indexes requiring defaults in an ordered MIDDLE task.
4. Finalize resource removals in staged state, then execute the planned interim
   groups. Each group's fields build serially; separate groups may overlap.
5. Return to the caller and publish the complete snapshot.

ASYNC_LOAD runs orchestration and asynchronous reader waits. MIDDLE runs the
synchronous leaves, including materialization, cache-slot construction, and index
Deserialize. A MIDDLE leaf does not submit more MIDDLE work and block waiting for
it. Batch orchestration awaits futures without occupying a MIDDLE worker. Native
Load phase timing counts the overlapping preparation tree once as
`parallel_prepare`; existing batch and worker metrics retain their own timings.
Default filling, Text creation, finalization and publication remain sequential
phase intervals.

Preparation failures are collected only after every started task finishes.
Submission failures also drain work already submitted by that batch. Awaiting
those borrowed-state operations suppresses early coroutine cancellation; the
operations retain their existing `OpContext` cancellation checks. The caller's
snapshot and staged state therefore outlive all workers, and failed preparation
does not publish a partial result.

Concurrent preparation can make more categories of work active at once; existing
load budgets continue to govern admission. This change does not claim a lower
peak memory footprint. It does not introduce asynchronous cache-slot APIs or
remove MIDDLE, and preserves the restriction against dropping a vector index
while retaining its field.

## Testing

Unit tests are located in `internal/core/src/segcore/SegmentLoadInfoTest.cpp`:

- Test diff computation for indexes
- Test diff computation for binlogs (V1 and V4 formats)
- Test diff computation for column groups
- Test mixed format handling
- Test schema evolution scenarios
