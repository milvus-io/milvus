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
    explicit SegmentLoadInfo(const ProtoType& info, SchemaPtr schema);

    // Compute diff between current and new load info (for Reopen)
    LoadDiff ComputeDiff(SegmentLoadInfo& new_info);

    // Compute diff from empty state (for initial Load)
    LoadDiff GetLoadDiff();

    // Column groups support (Storage V3)
    std::shared_ptr<milvus_storage::api::ColumnGroups> GetColumnGroups();

private:
    void BuildCache();
    void ComputeDiffIndexes(LoadDiff& diff, SegmentLoadInfo& new_info);
    void ComputeDiffBinlogs(LoadDiff& diff, SegmentLoadInfo& new_info);
    void ComputeDiffColumnGroups(LoadDiff& diff, SegmentLoadInfo& new_info);
    void ComputeDiffReloadFields(LoadDiff& diff, SegmentLoadInfo& new_info);
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

The `ApplyLoadDiff` method executes incremental changes based on the computed diff:

```cpp
void ChunkedSegmentSealedImpl::ApplyLoadDiff(
    SegmentLoadInfo& segment_load_info,
    LoadDiff& diff,
    milvus::OpContext* op_ctx) {

    // 1. Load new indexes
    if (!diff.indexes_to_load.empty()) {
        LoadBatchIndexes(trace_ctx, diff.indexes_to_load, op_ctx);
    }

    // 2. Reload fields (MUST before drop index)
    if (!diff.fields_to_reload.empty()) {
        ReloadColumns(diff.fields_to_reload, op_ctx);
    }

    // 3. Drop indexes (MUST after reload to maintain data availability)
    if (!diff.indexes_to_drop.empty()) {
        for (auto field_id : diff.indexes_to_drop) {
            DropIndex(field_id);
        }
    }

    // 4. Load column groups (eager + lazy)
    if (!diff.column_groups_to_load.empty()) {
        LoadColumnGroups(..., diff.column_groups_to_load, true, op_ctx);
    }
    if (!diff.column_groups_to_lazyload.empty()) {
        LoadColumnGroups(..., diff.column_groups_to_lazyload, false, op_ctx);
    }

    // 5. Load field binlogs (Storage V1/V2)
    if (!diff.binlogs_to_load.empty()) {
        LoadBatchFieldData(trace_ctx, diff.binlogs_to_load, op_ctx);
    }

    // 6. Fill default values for schema evolution
    if (!diff.fields_to_fill_default.empty()) {
        FillDefaultValueFields(diff.fields_to_fill_default);
    }

    // 7. Drop field data
    if (!diff.field_data_to_drop.empty()) {
        for (auto field_id : diff.field_data_to_drop) {
            DropFieldData(field_id);
        }
    }
}
```

### 4. Unified Load and Reopen

Both Load and Reopen operations now use the same diff-based mechanism:

**Initial Load:**
```cpp
void ChunkedSegmentSealedImpl::Load(
    milvus::tracer::TraceContext& trace_ctx,
    milvus::OpContext* op_ctx) {

    auto diff = segment_load_info_.GetLoadDiff();
    ApplyLoadDiff(segment_load_info_, diff, op_ctx);
}
```

**Reopen (Incremental Update):**
```cpp
void ChunkedSegmentSealedImpl::Reopen(
    const milvus::proto::segcore::SegmentLoadInfo& new_load_info) {

    SegmentLoadInfo new_seg_load_info(new_load_info, schema_);

    // Store old info and update to new
    SegmentLoadInfo current(segment_load_info_);
    segment_load_info_ = new_seg_load_info;

    // Compute diff between old and new
    auto diff = current.ComputeDiff(new_seg_load_info);
    ApplyLoadDiff(new_seg_load_info, diff);
}
```

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

## Execution Order

The order of operations in `ApplyLoadDiff()` is critical:

1. **Load new indexes** - Can run in parallel
2. **Reload columns** - MUST happen before dropping indexes
3. **Drop indexes** - MUST happen after reload to maintain data availability
4. **Load column groups** - Eager and lazy loading
5. **Load field binlogs** - For Storage V1/V2
6. **Fill default values** - For schema evolution
7. **Drop field data** - Clean up removed fields

## Testing

Unit tests are located in `internal/core/src/segcore/SegmentLoadInfoTest.cpp`:

- Test diff computation for indexes
- Test diff computation for binlogs (V1 and V4 formats)
- Test diff computation for column groups
- Test mixed format handling
- Test schema evolution scenarios
