# External Table: Index, Load & Query Design Document

## Overview

This document describes how external collections (tables backed by external Parquet files in object storage) implement **indexing**, **loading**, and **querying** in Milvus. The implementation spans Proxy, DataCoord, DataNode, QueryNode (Go), and Segcore (C++).

## Architecture

```
                         +---------+
                         |  Client |
                         +----+----+
                              |
                    RefreshExternalCollection
                    CreateIndex / LoadCollection
                    Search / Query
                              |
                         +----v----+
                         |  Proxy  |  -- validates external collection, injects virtual PK
                         +----+----+
                              |
              +---------------+----------------+
              |                                |
        +-----v------+                  +------v------+
        |  DataCoord  |                 |  QueryCoord  |
        +-----+------+                  +------+------+
              |                                |
      Refresh (segment                  Load (assign segments
      creation & manifest)              to query nodes)
              |                                |
        +-----v------+                  +------v------+
        |  DataNode   |                 |  QueryNode   |
        +------------+                  +------+------+
                                               |
                                        +------v------+
                                        |   Segcore   |  -- C++ engine
                                        |  (sealed    |     loads parquet via
                                        |   segment)  |     milvus-storage Reader
                                        +-------------+
```

---

## 1. Refresh: From External Files to Segments

### 1.1 RPC Flow

```
Client.RefreshExternalCollection(collectionName)
  -> Proxy.RefreshExternalCollection()
       validates collection, resolves collectionID
  -> DataCoord.RefreshExternalCollection()
       pre-allocates JobID, broadcasts to WAL
  -> WAL callback -> Manager.SubmitRefreshJobWithID()
       creates Job + Task, enqueues to Scheduler
  -> Scheduler dispatches Task to DataNode
  -> DataNode.UpdateExternalCollection()
       scans external source, creates manifests
  -> DataCoord.SetJobInfo()
       atomic segment metadata update (drop old + add new)
```

### 1.2 DataNode: Fragment Discovery & Segment Organization

**File**: `internal/datanode/external/task_update.go`

The DataNode receives an `UpdateExternalCollectionRequest` containing:
- `CurrentSegments`: existing segment list
- `PreAllocatedSegmentIds`: ID range [begin, end) pre-allocated by DataCoord
- `Schema`, `ExternalSource`, `ExternalSpec`, `StorageConfig`

**Step 1: Fetch Fragments from External Source**

```
ExternalSource (e.g., "external-data/my_collection/")
  -> packed.ExploreFiles()        -- FFI: scan directory for parquet files
  -> packed.GetFileInfo()          -- FFI: get row count per file
  -> packed.SplitFileToFragments() -- split large files (>1M rows) into fragments
  -> []Fragment{FilePath, StartRow, EndRow, RowCount}
```

A **Fragment** is the smallest unit of data assignment:
```go
type Fragment struct {
    FragmentID int64
    FilePath   string   // e.g., "external-data/my_collection/data0.parquet"
    StartRow   int64    // inclusive
    EndRow     int64    // exclusive
    RowCount   int64    // EndRow - StartRow
}
```

**Step 2: Compare with Current Segments**

For each current segment, read its manifest to get its fragments. If ALL fragments of a segment still exist in the new external source, the segment is **kept**. Otherwise, its fragments become **orphans**.

```
currentSegmentFragments = BuildCurrentSegmentFragments(currentSegments)

for each currentSegment:
    if all its fragments exist in newFragments:
        mark as KEPT, record used fragments
    else:
        mark as INVALIDATED, fragments become orphans

orphanFragments = newFragments - usedFragments
```

**Step 3: Balance Orphan Fragments into New Segments**

Uses **greedy bin-packing** (largest-first, fill-lightest-bin):

```
targetRowsPerSegment = config (default from paramtable)
numSegments = ceil(totalOrphanRows / targetRowsPerSegment)

sort fragments by rowCount DESC
for each fragment:
    assign to bin with minimum current rowCount
```

**Step 4: Create Manifest for Each New Segment**

```go
manifestPath = packed.CreateSegmentManifest(
    basePath: "external/{collectionID}/segments/{segmentID}",
    format: "parquet",
    columns: schema field names,
    fragments: assigned fragments,
    storageConfig: S3/MinIO config,
)
```

The manifest is a milvus-storage transaction that records which column groups (files + row ranges) belong to this segment.

**Step 5: Return Results**

```go
Response{
    KeptSegments:    []int64{seg1, seg2},           // unchanged segments
    UpdatedSegments: []*SegmentInfo{newSeg1, ...},  // newly created segments
}
```

### 1.3 DataCoord: Atomic Segment Update

**File**: `internal/datacoord/task_refresh_external_collection.go` (`SetJobInfo`)

The DataCoord atomically:
1. **Drops** segments not in `KeptSegments` (sets `State = Dropped`)
2. **Adds** new segments from `UpdatedSegments` (fills in `InsertChannel`, `PartitionID`, sets `State = Flushed`)
3. Updates via `meta.UpdateSegmentsInfo(operators...)` in a single transaction

Safety checks:
- Refuses to delete ALL segments if active segments exist (prevents data loss)
- Warns if deletion ratio exceeds 90% (configurable)

### 1.4 Manifest Path Format

```
external/{collectionID}/segments/{segmentID}/manifest_v{version}
```

The manifest stores:
- Column groups (each group = one parquet file path + row range)
- Column names and their mapping to parquet columns
- Format metadata (parquet/csv/json)

---

## 2. Loading: From Segment to Queryable Data

### 2.1 QueryNode Go Layer

**File**: `internal/querynodev2/segments/segment_loader.go`

When loading an external collection segment, the loader takes a **different path** from normal segments:

```go
func (loader *segmentLoader) Load(ctx context.Context, segment *LocalSegment, loadInfo *querypb.SegmentLoadInfo) error {
    // Normal segment: load binlogs, delta logs, bloom filters
    // External segment: different handling

    // 1. Load segment data via CGO (triggers C++ LoadExternalCollectionFields)
    segment.Load(loadInfo)

    // 2. Skip delta logs (external collections are read-only)
    if !IsExternalCollection(collection.Schema()) {
        loader.loadDeltalogs(ctx, segment, loadInfo.GetDeltalogs())
    }

    // 3. Use ExternalSegmentCandidate instead of BloomFilter
    if IsExternalCollection(collection.Schema()) {
        candidate := pkoracle.NewExternalSegmentCandidate(
            loadInfo.GetSegmentID(),
            loadInfo.GetPartitionID(),
            segment.Type(),
        )
        segment.SetPKCandidate(candidate)
    } else {
        bfs := loader.loadSingleBloomFilterSet(...)
        segment.SetBloomFilter(bfs)
    }
}
```

Key differences from normal segment loading:
| Aspect | Normal Segment | External Segment |
|--------|----------------|------------------|
| Data source | Binlog files | Parquet via milvus-storage Reader |
| Delta logs | Loaded | Skipped (read-only) |
| PK check | BloomFilterSet (probabilistic) | ExternalSegmentCandidate (deterministic) |
| PK type | Real user-defined PK | Virtual PK: `(segmentID << 32) \| offset` |

### 2.2 Segcore C++ Layer: LoadExternalCollectionFields

**File**: `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`

This is the core loading function that creates in-memory column representations for external data:

```cpp
void ChunkedSegmentSealedImpl::LoadExternalCollectionFields() {
    int64_t num_rows = segment_load_info_.GetNumOfRows();

    // Guard: skip 0-row segments
    if (num_rows == 0) {
        update_row_count(0);
        system_ready_count_++;
        return;
    }

    // Shared mutex for serializing Reader access (Reader is NOT thread-safe)
    auto reader_mutex = std::make_shared<std::mutex>();
    std::vector<std::shared_ptr<ExternalFieldChunkedColumn>> ext_columns;

    for (const auto& [field_id, field_meta] : schema_->get_fields()) {
        // Skip system fields
        if (field_id.get() < START_USER_FIELDID) continue;

        // 1. Virtual PK field -> VirtualPKChunkedColumn
        if (field_meta.get_name() == "__virtual_pk__") {
            auto column = std::make_shared<VirtualPKChunkedColumn>(id_, num_rows);
            fields_.emplace(field_id, column);
            continue;
        }

        // 2. External data field -> ExternalFieldChunkedColumn
        if (field_meta.is_external_field()) {
            auto column = std::make_shared<ExternalFieldChunkedColumn>(
                num_rows, data_type, nullable, field_id, dim,
                field_meta.get_external_field(),  // parquet column name
                reader_.get(),                    // milvus-storage Reader
                reader_mutex                      // shared mutex
            );
            ext_columns.push_back(column);
            fields_.emplace(field_id, column);
            continue;
        }
    }

    // 3. Eagerly materialize ALL external columns during load
    //    (avoids concurrent Reader access during query)
    for (auto& col : ext_columns) {
        col->EagerMaterialize();
    }

    // 4. Build PK->offset index for virtual PKs
    insert_record_.insert_pks(DataType::INT64, pk_column);
    insert_record_.seal_pks();

    // 5. Initialize synthetic timestamps (all zeros = always visible)
    std::vector<Timestamp> timestamps(num_rows, 0);
    insert_record_.init_timestamps(timestamps, index);

    // 6. Mark segment ready
    update_row_count(num_rows);
    system_ready_count_++;
}
```

### 2.3 Column Implementations

#### VirtualPKChunkedColumn

**File**: `internal/core/src/mmap/VirtualPKChunkedColumn.h`

Generates virtual primary keys on-the-fly without storing actual data:

```
Virtual PK format:  [63:32] truncated_segment_id | [31:0] row_offset
Max rows per segment: 2^32 (~4 billion)
```

```cpp
class VirtualPKChunkedColumn : public ChunkedColumnInterface {
    // Bulk access: computes PKs on-the-fly (zero memory for filtered queries)
    void BulkPrimitiveValueAt(dst, offsets, count) {
        for (int64_t i = 0; i < count; i++) {
            typed_dst[i] = GetVirtualPK(truncated_segment_id_, offsets[i]);
        }
    }

    // Span access: lazy-materializes all PKs (for full scans)
    PinWrapper<SpanBase> Span() {
        EnsureMaterialized();  // std::call_once
        return SpanBase(materialized_pks_.data(), num_rows_, sizeof(int64_t));
    }
};
```

#### ExternalFieldChunkedColumn

**File**: `internal/core/src/mmap/ExternalFieldChunkedColumn.h`

Wraps external parquet data with lazy/eager materialization:

```cpp
class ExternalFieldChunkedColumn : public ChunkedColumnInterface {
    // Eager materialization during load (called from LoadExternalCollectionFields)
    void EagerMaterialize() {
        EnsureMaterialized();
    }

    void EnsureMaterialized() {
        std::call_once(materialize_once_, [this]() {
            // Read ALL rows from parquet via milvus-storage Reader
            std::vector<int64_t> all_indices(num_rows_);
            std::iota(all_indices.begin(), all_indices.end(), 0);

            std::lock_guard<std::mutex> lock(*reader_mutex_);
            auto table = reader_->take(all_indices);

            // Copy Arrow data to contiguous memory buffer
            auto column = table->GetColumnByName(external_field_name_);
            // Handle: FIXED_SIZE_BINARY, LIST, FIXED_SIZE_LIST for vectors
            // Handle: INT8..INT64, FLOAT, DOUBLE, BOOL for scalars
            CopyToBuffer(column, materialized_data_);
        });
    }

    // Query-time access: reads from materialized buffer (no Reader needed)
    void BulkPrimitiveValueAt(dst, offsets, count) {
        EnsureMaterialized();
        auto src = materialized_data_.data();
        for (int64_t i = 0; i < count; i++) {
            memcpy(dst + i * element_size, src + offsets[i] * element_size, element_size);
        }
    }
};
```

### 2.4 ExternalSegmentCandidate (PK Oracle)

**File**: `internal/querynodev2/pkoracle/external_segment_candidate.go`

Replaces BloomFilter for external segments. Uses virtual PK format for deterministic segment matching:

```go
type ExternalSegmentCandidate struct {
    segmentID          int64
    truncatedSegmentID int64  // segmentID & 0xFFFFFFFF
    partitionID        int64
    segType            commonpb.SegmentState
}

func (c *ExternalSegmentCandidate) MayPkExist(lc *storage.LocationsCache) bool {
    pk := lc.GetPk()
    if int64Pk, ok := pk.(*storage.Int64PrimaryKey); ok {
        extractedSegmentID := int64(uint64(int64Pk.Value) >> 32)
        return extractedSegmentID == c.truncatedSegmentID
    }
    return false  // only int64 virtual PKs
}
```

| Property | BloomFilter | ExternalSegmentCandidate |
|----------|-------------|--------------------------|
| False positives | Yes (probabilistic) | No (deterministic) |
| Memory | O(n) based on cardinality | O(1) - zero |
| PK types | Any | Int64 only (virtual PK) |

---

## 3. Indexing

### 3.1 Current Status: Brute-Force Search

External collections currently use **brute-force (flat) search** with no vector index. When a search request arrives:

1. All vector data is already materialized in memory (from `EagerMaterialize` during load)
2. The segcore search engine performs flat L2/IP/Cosine distance computation directly on the materialized buffer
3. No index build step is required

This means:
- `CreateIndex` on external collection embedding fields is a **no-op** or uses AutoIndex which falls back to flat
- Search latency scales linearly with segment size
- Memory usage equals raw vector data size (no index overhead, but also no compression)

### 3.2 Why No Index (Yet)

External collection segments are different from normal segments:
- Normal segments store data in Milvus-managed binlog format with well-defined paths
- External segments reference parquet files in user-controlled storage
- The existing index build pipeline (IndexNode) expects to read binlogs, not parquet via manifest

### 3.3 Future Index Support (Planned)

Potential approaches:
1. **Pre-built index**: Users provide pre-built index files alongside parquet data
2. **On-load index build**: Build index from materialized data after loading
3. **IndexNode adaptation**: Extend IndexNode to read external parquet via manifest

---

## 4. Query Execution

### 4.1 Query Flow

```
Client.Search(collection, vectors, topK, filter)
  -> Proxy: route to QueryNode shards
  -> QueryNode: iterate over loaded segments

For each external segment:
  1. PK Pruning: ExternalSegmentCandidate.MayPkExist()
     -> deterministic: (virtualPK >> 32) == truncatedSegmentID

  2. Expression Evaluation (filter):
     -> Access VirtualPKChunkedColumn for PK comparisons
     -> Access ExternalFieldChunkedColumn for scalar filters
     -> All data from materialized memory buffers

  3. Vector Search (ANN):
     -> Brute-force on materialized vector data
     -> L2/IP/Cosine distance computation

  4. Result Assembly:
     -> Return virtual PKs, distances, output fields
     -> Virtual PKs are transparent to users (AutoID=true)
```

### 4.2 Supported Query Types (Verified by E2E Tests)

| Query Type | Example | Verified |
|------------|---------|----------|
| Count aggregation | `count(*)` | Yes |
| Scalar filter | `id < 100` with output fields | Yes |
| ANN Search | top-K L2 single vector | Yes |
| Hybrid Search | Search + scalar filter `id >= 500 && id < 1000` | Yes |
| Pagination | `offset=10, limit=20` | Yes |
| Multi-vector Search | Multiple query vectors, top-K each | Yes |
| Vector output | Query with embedding in output fields | Yes |

### 4.3 Timestamp Handling

External collection segments use **synthetic timestamps** (all zeros):

```cpp
std::vector<Timestamp> timestamps(num_rows, 0);  // timestamp = 0 for all rows
```

This means:
- All rows are visible at any query timestamp (no TTL expiration)
- Consistency level settings still apply but timestamps are always satisfied
- External data is treated as "always present"

---

## 5. Virtual Primary Key Design

### 5.1 Injection at Create Time

**File**: `internal/proxy/util.go` (`injectVirtualPKForExternalCollection`)

When creating an external collection, Proxy automatically injects a virtual PK field:

```go
virtualPKField := &schemapb.FieldSchema{
    Name:         "__virtual_pk__",
    DataType:     schemapb.DataType_Int64,
    IsPrimaryKey: true,
    AutoID:       true,
    Description:  "Virtual primary key: (segmentID << 32) | offset",
}
schema.Fields = append(schema.Fields, virtualPKField)
```

### 5.2 Format

```
Bit layout:  [63:32] truncated_segment_id  |  [31:0] row_offset

GetVirtualPK(segmentID, offset) = (segmentID << 32) | (offset & 0xFFFFFFFF)
ExtractSegmentID(vpk)           = int64(uint64(vpk) >> 32)
ExtractOffset(vpk)              = vpk & 0xFFFFFFFF
```

Constraints:
- Max rows per segment: 2^32 (~4.29 billion)
- Segment ID truncated to lower 32 bits (theoretical collision if >2^32 segments)
- Only Int64 type (not VarChar)

### 5.3 Consistency Across Layers

The same virtual PK logic is implemented in three places:

| Layer | File | Language |
|-------|------|----------|
| C++ | `internal/core/src/common/VirtualPK.h` | C++ inline |
| C++ | `internal/core/src/mmap/VirtualPKChunkedColumn.h` | C++ class |
| Go | `internal/querynodev2/segments/utils.go` | Go functions |
| Go | `internal/querynodev2/pkoracle/external_segment_candidate.go` | Go struct |

---

## 6. Thread Safety: Reader Mutex

### Problem

`milvus_storage::api::Reader` is **NOT thread-safe**. Multiple fields (id, value, embedding) of the same segment share one Reader instance. During loading, concurrent calls to `reader_->take()` from different threads cause SIGSEGV.

### Solution

```cpp
// Shared mutex across all ExternalFieldChunkedColumn instances of the same segment
auto reader_mutex = std::make_shared<std::mutex>();

// Each column holds a reference to the shared mutex
ExternalFieldChunkedColumn(..., reader_mutex);

// Reader access is serialized
void EnsureMaterialized() {
    std::call_once(materialize_once_, [this]() {
        std::lock_guard<std::mutex> lock(*reader_mutex_);
        auto table = reader_->take(all_indices);
        // ... copy to buffer
    });
}
```

Additionally, **eager materialization** during load ensures that by the time queries arrive, all data is already in memory buffers and no Reader access is needed:

```cpp
// In LoadExternalCollectionFields:
for (auto& col : ext_columns) {
    col->EagerMaterialize();  // materialize during load, not query
}
```

---

## 7. Data Flow Summary

```
                    REFRESH PHASE
                    =============

    External Source (S3/MinIO)
    ├── data0.parquet (1000 rows)
    ├── data1.parquet (1000 rows)
    └── data2.parquet (500 rows)
              │
              ▼
    ExploreFiles() + GetFileInfo()
              │
              ▼
    Fragments:
    ├── frag_0: data0.parquet [0, 1000)
    ├── frag_1: data1.parquet [0, 1000)
    └── frag_2: data2.parquet [0, 500)
              │
              ▼
    balanceFragmentsToSegments()
    (greedy bin-packing)
              │
              ▼
    Segment_A: [frag_0, frag_2]  1500 rows
    Segment_B: [frag_1]          1000 rows
              │
              ▼
    CreateManifestForSegment()
    ├── external/{coll}/segments/{segA}/manifest_v1
    └── external/{coll}/segments/{segB}/manifest_v1


                    LOAD PHASE
                    ==========

    QueryNode receives segment load request
              │
              ▼
    LoadExternalCollectionFields()
              │
              ▼
    ┌──────────────────────────────────────────────┐
    │ For each field in schema:                     │
    │                                               │
    │ __virtual_pk__ → VirtualPKChunkedColumn       │
    │   generates: (segID<<32)|offset               │
    │                                               │
    │ id (int64) → ExternalFieldChunkedColumn       │
    │   reads: parquet column "id" via Reader       │
    │                                               │
    │ value (float) → ExternalFieldChunkedColumn    │
    │   reads: parquet column "value" via Reader    │
    │                                               │
    │ embedding (vec) → ExternalFieldChunkedColumn  │
    │   reads: parquet column "embedding"           │
    └──────────────────────────────────────────────┘
              │
              ▼
    EagerMaterialize() for all columns
    (reader_->take([0,1,...,N-1]) with mutex)
              │
              ▼
    All data in memory buffers, Reader no longer needed
              │
              ▼
    Build PK→offset index, init timestamps(=0)
              │
              ▼
    Segment READY for queries


                    QUERY PHASE
                    ===========

    Search(vectors, topK=5, filter="id < 100")
              │
              ▼
    ExternalSegmentCandidate.MayPkExist()
    (deterministic segment pruning)
              │
              ▼
    Expression filter: access materialized id column
    → offsets = [0, 1, ..., 99]
              │
              ▼
    Vector search: brute-force on materialized embedding
    → top-5 nearest neighbors within filtered offsets
              │
              ▼
    Output: virtual PKs + distances + requested fields
```

---

## 8. File Inventory

### New Files

| File | Language | Purpose |
|------|----------|---------|
| `internal/core/src/common/VirtualPK.h` | C++ | Virtual PK utility functions |
| `internal/core/src/mmap/VirtualPKChunkedColumn.h` | C++ | Virtual PK column implementation |
| `internal/core/src/mmap/ExternalFieldChunkedColumn.h` | C++ | External field column with lazy materialization |
| `internal/querynodev2/pkoracle/external_segment_candidate.go` | Go | PK oracle for external segments |
| `internal/storagev2/packed/explore_ffi.go` | Go | FFI: explore external parquet files |
| `internal/storagev2/packed/manifest_ffi.go` | Go | FFI: create/read segment manifests |
| `internal/storagev2/packed/utils.go` | Go | Fragment splitting, segment manifest creation |
| `internal/datanode/external/external_spec.go` | Go | Parse external collection spec (format) |
| `internal/datanode/external/task_update.go` | Go | DataNode refresh task execution |
| `internal/datacoord/external_collection_refresh_meta.go` | Go | Job/Task metadata management |
| `internal/datacoord/external_collection_refresh_manager.go` | Go | Refresh lifecycle management |
| `internal/datacoord/external_collection_refresh_checker.go` | Go | Job timeout & GC |
| `internal/datacoord/external_collection_refresh_inspector.go` | Go | Task scheduling & recovery |
| `internal/datacoord/task_refresh_external_collection.go` | Go | Refresh task scheduler wrapper |
| `internal/datacoord/ddl_callbacks_external_collection.go` | Go | WAL callback for refresh |

### Modified Files

| File | Language | Change |
|------|----------|--------|
| `internal/core/src/common/Schema.h/.cpp` | C++ | `external_source_`, `external_spec_` fields |
| `internal/core/src/common/FieldMeta.h/.cpp` | C++ | `external_field_` mapping to parquet column name |
| `internal/core/src/segcore/ChunkedSegmentSealedImpl.h/.cpp` | C++ | `LoadExternalCollectionFields()`, Reader member |
| `internal/core/src/segcore/SegmentLoadInfo.h/.cpp` | C++ | External segment load info support |
| `internal/core/src/segcore/segment_c.h/.cpp` | C++ | New C API for external segment loading |
| `internal/querynodev2/segments/segment_loader.go` | Go | External segment loading path |
| `internal/querynodev2/segments/segment.go` | Go | `pkCandidate` field, `SetPKCandidate()` |
| `internal/querynodev2/segments/segment_interface.go` | Go | `SetPKCandidate()` interface method |
| `internal/querynodev2/segments/utils.go` | Go | `IsExternalCollection()`, virtual PK utils |
| `internal/proxy/task.go` | Go | Virtual PK injection at collection creation |
| `internal/proxy/util.go` | Go | `injectVirtualPKForExternalCollection()` |
| `internal/proxy/impl.go` | Go | Refresh/Progress/List RPC handlers |
| `internal/datacoord/services.go` | Go | Refresh/Progress/List RPC handlers |
| `internal/datacoord/handler.go` | Go | External segment filter in handler |

---

## 9. Key Design Decisions

### 9.1 Eager vs Lazy Materialization

**Chosen: Eager materialization during load**

Rationale:
- milvus-storage Reader is not thread-safe
- Multiple query threads would need concurrent Reader access
- Eager materialization moves all I/O to the load phase
- Query-time access is pure memory copy (predictable latency)

Trade-off: Higher memory usage (all external data in memory), but consistent query performance.

### 9.2 Virtual PK vs Real PK

**Chosen: Auto-generated virtual PK**

Rationale:
- External parquet files may not have a unique PK column
- Avoids reading and deduplicating real PKs from external storage
- Enables deterministic segment-to-PK mapping (no bloom filter needed)
- Transparent to users (AutoID=true, users never see/use virtual PKs directly)

Trade-off: Cannot do PK-based point lookups by user-defined ID. Users use scalar filters instead.

### 9.3 No Delta Logs

External collections are **read-only** (no insert/delete/upsert). Therefore:
- No delta log loading
- No compaction
- No growing segments
- Data changes only through `RefreshExternalCollection`

### 9.4 Segment ID Pre-allocation

**Chosen: DataCoord pre-allocates ID batch, DataNode uses sequentially**

Rationale:
- Eliminates round-trips between DataNode and DataCoord during segment creation
- All manifest files written to final paths (no temporary paths or cleanup)
- Failed manifests remain but are never registered in metadata

### 9.5 Fragment-based Data Assignment

**Chosen: Fragment as atomic unit (file + row range)**

Rationale:
- Supports large parquet files by splitting into sub-ranges
- Enables incremental refresh: only new/changed fragments need new segments
- Greedy bin-packing produces balanced segment sizes

---

## 10. Configuration

| Config Key | Description | Default |
|------------|-------------|---------|
| `ExternalCollectionCheckInterval` | Inspector/Checker check interval | 60s |
| `ExternalCollectionJobTimeout` | Maximum job duration | 3600s |
| `ExternalCollectionJobRetention` | GC retention for completed jobs | 86400s |
| `ExternalCollectionTargetRowsPerSegment` | Target rows per segment for balancing | Configured |
| `ExternalCollectionDropRatioWarn` | Warn if segment deletion ratio exceeds this | 0.9 |
