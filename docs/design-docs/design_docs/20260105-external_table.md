# External Table (External Collection) Design Document

## 1. Overview

### 1.1 Background

External Table (External Collection) allows Milvus to query data in external
storage through source-file manifests without first importing the dataset into
Milvus-managed data files. QueryNode reads and caches source data as needed while
exposing the standard Milvus query interfaces.

### 1.2 Goals

1. **Support External Table Creation**
   - Create external collections through standard `CreateCollection` API with `external_source` parameter
   - Map external data files (Parquet, etc.) to Milvus storage format via manifest files
   - Support field mapping between external columns and Milvus schema fields
   - Inject a virtual primary key when no user primary key is supplied

2. **Support Vector Index Building on External Tables**
   - Enable index creation on vector fields of external collections
   - Leverage milvus-storage library for efficient data access during index building
   - Support common index types (IVF, HNSW, etc.) on external data

3. **Support External Table Loading**
   - Load external collection segments into QueryNode memory
   - Load manifest column groups through `ManifestGroupTranslator`
   - Generate virtual PKs from the lower 32 bits of segment ID and row offset
   - Use `ExternalSegmentCandidate` for virtual PK routing and bloom filters for
     real-PK `milvus-table` segments

4. **Support External Table Querying**
   - Provide unified query interface consistent with regular collections
   - Support vector similarity search on external data
   - Support scalar filtering and hybrid search
   - Enable search/query operations while blocking write operations

5. **Support External Table Data Updates**
   - Support manual trigger to refresh external table data (automatic detection not supported yet)
   - Synchronize external data changes with segment-level granularity
   - Keep unchanged segments, patch missing columns, drop obsolete segments and
     add new segments
   - Balance orphan file fragments into new segments; preserve source-segment
     boundaries for `milvus-table`

### 1.3 Non-Goals

The following features are explicitly **NOT supported** for external tables in the current implementation:

1. **Write Operations**
   - No support for insert, delete, upsert, or import operations
   - External tables are read-only; data modifications must be done at the source

2. **User-Defined Function Features**
   - No support for user-defined functions (UDFs)
   - Built-in function outputs are covered by `20260521-external-table-function-output.md`

3. **Schema Modifications**
   - Additive external fields are supported by `AlterCollectionSchema`
     followed by `RefreshExternalCollection`; see
     [External Table Add-Column Refresh](20260526-external_table_add_column_refresh.md).
   - No support for dropping fields, renaming fields, changing field data
     types, changing vector dimensions, or remapping `external_field` after
     creation.
   - No support for altering existing field properties other than additive
     external-field schema changes.

4. **Dynamic Schema Features**
   - No support for dynamic fields (`EnableDynamicField`)
   - Fields must be explicitly declared; additive fields can be added later

5. **Auto ID**
   - No support for auto-generated IDs (`AutoID`)
   - Collections without a user primary key use a virtual PK derived from segment
     ID and row offset

6. **Automatic Data Source Synchronization**
   - No support for automatic/periodic detection of external data source changes
   - Data updates must be triggered manually

7. **Other Limitations**
   - No support for partition key fields
   - No support for clustering key fields
   - Text match support is covered by `20260521-external-table-function-output.md`
   - No support for struct array fields
   - No support for namespace fields

---

## 2. Architecture

### 2.1 System Architecture Diagram

```
                                +--------------------------+
                                |          Client          |
                                +--------------------------+
                                              |
                                              v
                                +--------------------------+
                                |          Proxy           |
                                |    Validate and route    |
                                +--------------------------+
                                              |
              +-------------------------------+-------------------------------+
              |                               |                               |
      CreateCollection           RefreshExternalCollection             Search / Query
              v                               v                               v
+--------------------------+    +--------------------------+    +--------------------------+
|        RootCoord         |    |        DataCoord         |    |        QueryNode         |
| Validate resolved schema |    |  Submit refresh via WAL  |    |  Execute Search / Query  |
|      Persist schema      |    | Explore / schedule tasks |    |    Read external data    |
|                          |    |    Apply task results    |    |                          |
+--------------------------+    +-------+-----------+------+    +--------------------------+
                                        |           ^
                             Dispatch   |           |   Results
                                        v           |
                                +-------+-----------+------+
                                |         DataNode         |
                                |  Execute refresh tasks   |
                                |    Prepare manifests     |
                                +--------------------------+
```

### 2.2 Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| `Proxy` | Normalize and validate schema, inject a virtual PK when needed, block write operations, handle refresh requests |
| `RootCoord` | Validate resolved schema, store external source configuration |
| `DataCoord` | Disable compaction, allow text/eligible JSON stats, explore sources, schedule refresh tasks and apply results |
| `DataNode` | Read assigned source fragments, organize or patch segments, create manifests |
| `QueryNode` | Load external data with virtual or real PK support, apply deletes and execute queries |

---

## 3. External Table API Design

### 3.1 API Reuse Strategy

External tables reuse existing Milvus APIs to minimize API surface and maintain consistency:

| Operation | API | Description |
|-----------|-----|-------------|
| Create external table | `CreateCollection` | Declare source-backed fields and optionally supply the source/spec pair |
| Drop external table | `DropCollection` | Standard drop collection API |
| Load external table | `LoadCollection` | Standard load collection API |
| Query external table | `Search` / `Query` | Standard search and query APIs |

### 3.1.1 New APIs for Data Refresh

The following new APIs are introduced specifically for external table data refresh:

| Operation | API | Description |
|-----------|-----|-------------|
| Trigger refresh | `RefreshExternalCollection` | Manually trigger data refresh from external source |
| Get refresh progress | `GetRefreshExternalCollectionProgress` | Get progress of a specific refresh job |
| List refresh jobs | `ListRefreshExternalCollectionJobs` | List all refresh jobs for a collection |

### 3.1.2 RefreshExternalCollection API

Manually triggers a data refresh job for an external collection.

**Proto Definition** (`milvus.proto`):
```protobuf
// Job state enumeration for external table refresh
enum RefreshExternalCollectionState {
    RefreshPending = 0;      // Job is queued, waiting to execute
    RefreshInProgress = 1;   // Job is currently executing
    RefreshCompleted = 2;    // Job completed successfully
    RefreshFailed = 3;       // Job failed with error
}

message RefreshExternalCollectionRequest {
    common.MsgBase base = 1;
    string db_name = 2;              // Database name
    string collection_name = 3;      // Collection name (required)
    string external_source = 4;      // Optional: new external source path
    string external_spec = 5;        // Optional: new external spec configuration
}

message RefreshExternalCollectionResponse {
    common.Status status = 1;
    int64 job_id = 2;               // Unique job identifier for tracking
}
```

**Behavior**:
- `external_source` and `external_spec` must be provided together or both omitted.
- When omitted, the job uses the persisted external source configuration.
- Overrides apply to the refresh job. After successful refresh, DataCoord attempts
  to persist the new pair in the collection schema; a schema-update failure is
  logged and can leave the persisted configuration stale.
- Only one active refresh job is allowed per collection.
- Returns a `job_id` that can be used to track progress
- Job runs asynchronously; use `GetRefreshExternalCollectionProgress` to monitor

**Example Usage**:
```python
# Refresh with current data source
job_id = client.refresh_external_collection(
    collection_name="my_external_collection"
)

# Refresh with updated data source path
job_id = client.refresh_external_collection(
    collection_name="my_external_collection",
    external_source="s3://my-bucket/new-path/",
    external_spec='{"format": "parquet"}'
)
```

### 3.1.3 GetRefreshExternalCollectionProgress API

Gets the current progress and status of a refresh job.

**Proto Definition** (`milvus.proto`):
```protobuf
message GetRefreshExternalCollectionProgressRequest {
    common.MsgBase base = 1;
    int64 job_id = 2;               // Job ID from RefreshExternalCollection response
}

message RefreshExternalCollectionJobInfo {
    int64 job_id = 1;                   // Job identifier
    string collection_name = 2;          // Collection name
    RefreshExternalCollectionState state = 3; // Current job state
    int64 progress = 4;                  // Progress percentage (0-100)
    string reason = 5;                   // Error message if failed
    string external_source = 6;          // External source used for this job
    int64 start_time = 7;                // Job start timestamp (Unix ms)
    int64 end_time = 8;                  // Job end timestamp (Unix ms; 0 if not completed)
    string external_spec = 9;            // External spec, with credentials redacted
}

message GetRefreshExternalCollectionProgressResponse {
    common.Status status = 1;
    RefreshExternalCollectionJobInfo job_info = 2;
}
```

**Behavior**:
- Returns detailed progress information for the specified job
- State transitions: Pending → InProgress → Completed/Failed

**Example Usage**:
```python
# Get progress of a specific job
progress = client.get_refresh_external_collection_progress(job_id=job_id)
print(f"State: {progress.state}")
print(f"Progress: {progress.progress}%")
```

### 3.1.4 ListRefreshExternalCollectionJobs API

Lists all refresh jobs for a collection.

**Proto Definition** (`milvus.proto`):
```protobuf
message ListRefreshExternalCollectionJobsRequest {
    common.MsgBase base = 1;
    string db_name = 2;              // Database name
    string collection_name = 3;      // Collection name (optional, if empty lists all)
}

message ListRefreshExternalCollectionJobsResponse {
    common.Status status = 1;
    repeated RefreshExternalCollectionJobInfo jobs = 2;
}
```

**Behavior**:
- Returns jobs sorted by start_time (most recent first)
- If `collection_name` is empty, returns jobs for all external collections
- Jobs are retained for a configurable period after completion (default: 24 hours)

**Example Usage**:
```python
# List all jobs for a collection
jobs = client.list_refresh_external_collection_jobs(
    collection_name="my_external_collection"
)
for job in jobs:
    print(f"Job {job.job_id}: {job.state} ({job.progress}%)")

# List all external table refresh jobs
all_jobs = client.list_refresh_external_collection_jobs()
```

### 3.2 Create External Table

External collections are created through `CreateCollection` with source-backed
field mappings. The source/spec pair may be supplied at creation or deferred to a
later refresh:

```go
schema := &schemapb.CollectionSchema{
    Name:           "my_external_collection",
    ExternalSource: "s3://bucket/path/to/data",
    ExternalSpec:   `{"format": "parquet"}`,
    Fields: []*schemapb.FieldSchema{
        {
            Name:          "text_field",
            DataType:      schemapb.DataType_VarChar,
            ExternalField: "source_text_column",  // Maps to external column
            TypeParams:    []*commonpb.KeyValuePair{{Key: "max_length", Value: "256"}},
        },
        {
            Name:          "vector_field",
            DataType:      schemapb.DataType_FloatVector,
            ExternalField: "source_embedding",
            TypeParams:    []*commonpb.KeyValuePair{{Key: "dim", Value: "128"}},
        },
    },
}
```

### 3.3 Schema Restrictions

External collection creation uses `NormalizeAndValidateExternalCollectionSchema()`;
resolved schemas are checked by `ValidateExternalCollectionResolvedSchema()`:

| Feature | Status | Reason |
|---------|--------|--------|
| Primary Key | Conditional | `milvus-table` may map the source primary key; otherwise a virtual PK is injected |
| Dynamic Field | Not Allowed | Fields must be explicitly declared |
| Partition Key | Not Allowed | External data partitioning not supported |
| Clustering Key | Not Allowed | No clustering compaction |
| Auto ID | Not Allowed | IDs use source primary keys or generated virtual PKs |
| Text Match | Supported | Text-index stats tasks build persisted indexes |
| Struct Array Fields | Not Allowed | Milvus struct-array fields are unsupported |
| Namespace Field | Not Allowed | External isolation not supported |
| Add Field | Supported | Requires `external_field` mapping and a subsequent refresh |
| Multiple Shards | Not Allowed | External collections use one shard |

**Supported Field Types** (`isExternalFieldTypeSupported`):

The following types are accepted for source-backed fields. Existing field
constraints, such as vector dimensions and array element types, still apply.
External types below are Arrow types returned by the source reader; the source
file format must also support the selected representation.

| Category | Data Types | Supported External Types (Arrow) | Notes |
|----------|------------|----------------------------------|-------|
| Boolean | `Bool` | `bool` | |
| Integer | `Int8`, `Int16`, `Int32`, `Int64` | `int8`, `int16`, `int32`, `int64`, respectively | Exact type match; no automatic integer-width conversion |
| Floating Point | `Float`, `Double` | `float32`, `float64`, respectively | Exact type match |
| String | `VarChar`, `Text` | `string`, `binary` | String content |
| JSON | `JSON` | `string`, `binary` | Serialized JSON, not Arrow Map/Struct |
| Array | `Array` | `list<T>`, `binary` | `T` must match the declared scalar element type; Binary contains a serialized Milvus `ScalarField` |
| Timestamp | `Timestamptz` | `timestamp`, `int64` | Timestamps are converted to microseconds; `int64` already represents microseconds |
| Geometry | `Geometry` | `string`, `binary` | WKT strings or WKB bytes |
| Dense Vector | `FloatVector` | `list<float32>`, `fixed_size_list<float32>`; raw-byte forms below | `dim` float32 values |
| Dense Vector | `Float16Vector` | `list<float16>`, `fixed_size_list<float16>`; raw-byte forms below | `dim` float16 values |
| Dense Vector | `BFloat16Vector` | `list<uint8>`, `fixed_size_list<uint8>`, `binary`, `fixed_size_binary` | Encoded bfloat16 bytes: `2 * dim` bytes per vector |
| Dense Vector | `Int8Vector` | `list<int8>`, `fixed_size_list<int8>`; raw-byte forms below | `dim` int8 values |
| Binary Vector | `BinaryVector` | `list<uint8>`, `fixed_size_list<uint8>`, `binary`, `fixed_size_binary` | Packed bits: `dim / 8` bytes per vector |
| Sparse Vector | `SparseFloatVector` | `map<I, V>`, `struct<indices: list<I>, values: list<V>>`, `binary` | `I`: int32/uint32/int64/uint64; `V`: float32/float64. Binary uses native `(uint32, float32)` pairs; no `dim` required |
| Vector Array | `ArrayOfVector` | `list<list<T>>`, `list<fixed_size_list<T>>`, `list<fixed_size_binary>` | Inner vectors follow the declared vector element type and dimension |

Fixed-width vectors also accept raw bytes as `list<uint8>`,
`fixed_size_list<uint8>`, `fixed_size_binary`, or `binary` for nullable fields.
The byte count must match the vector type and dimension; uint8 values are
interpreted as encoded bytes, not converted to numeric vector elements.

The reader normalizes `large_string`/`string_view` and
`large_binary`/`binary_view` to the corresponding types above. For Array and
vector-list inputs, `large_list`/`list_view` are also accepted. Sparse Struct
children accept `list` or `large_list`.

**Implementation**: `pkg/util/typeutil/schema.go`

- External collections are identified by source-backed fields with `external_field`.
  `external_source` and `external_spec` must both be set or both be empty; the
  empty pair allows the source to be supplied on a later refresh.
- Each source-backed field needs a unique `external_field` mapping and a supported
  data type. Mappings must not collide with generated function output columns.
- Function outputs must not specify `external_field`; they are excluded from
  source-field validation and normalization.
- Source-backed fields are normalized to nullable for ordinary external formats.
  `milvus-table` preserves source nullability and checks alignment with the snapshot
  schema.

### 3.4 Primary Key Handling

**Implementation**: `internal/proxy/task.go`, `pkg/util/typeutil/schema.go`

For collections without a user primary key, `CreateCollection` injects the
`__virtual_pk__` field before normal primary-key validation. Its values are
computed from the target segment ID and row offset.

`milvus-table` also supports a user primary key mapped to the source snapshot's
primary key. This field is loaded from the source instead of synthesized; see
[Milvus Snapshot as External Table Source](20260526-milvus-table-external-source.md).

### 3.5 External Field Mapping

Each source-backed user field in the schema must specify `external_field` to map
to the external data source column. Function output fields are generated by
Milvus and must not specify `external_field`; see
`20260521-external-table-function-output.md` for that extended model.

**Proto Definition** (`schema.proto`):
```protobuf
message FieldSchema {
    // ... other fields ...
    string external_field = 17;  // external field name - maps to column name in external source
}
```

**Validation Rules** (`pkg/util/typeutil/schema.go`):
- Source-backed user fields in external collections must have `external_field`
  set.
- Function output fields and system/virtual fields must not have
  `external_field` set.
- If a source-backed field has empty `external_field`, validation fails with
  error: `field 'xxx' in external collection must have external_field mapping`.

**Example Usage**:
```go
field := &schemapb.FieldSchema{
    Name:          "vector",           // Milvus field name
    DataType:      schemapb.DataType_FloatVector,
    ExternalField: "embedding_col",    // Column name in external Parquet file
}
```

---

## 4. Data Structures

### 4.1 CollectionSchema Extension

**File**: Proto definition

```protobuf
message CollectionSchema {
    string name = 1;
    // ... other fields ...
    string external_source = 11;  // External data source (e.g., "s3://bucket/path")
    string external_spec = 12;    // External source config (JSON)
}
```

### 4.2 Collection Model Extension

**File**: `internal/metastore/model/collection.go`

```go
type Collection struct {
    // ... existing fields ...
    ExternalSource string
    ExternalSpec   string
}
```

### 4.3 External Spec Format

```json
{
    "format": "parquet"
}
```

Supported formats:
- `parquet` - Apache Parquet files
- `lance-table` - Lance dataset directory
- `vortex` - Vortex files
- `iceberg-table` - Apache Iceberg tables
- `milvus-table` - Milvus snapshot metadata and source segment manifests

`external_spec` also supports `columns`, `extfs` storage overrides, and
`snapshot_id` for Iceberg snapshot selection. Parsing and validation are defined
in `pkg/util/externalspec/specutil/spec.go`.

### 4.4 Fragment Structure

**File**: `internal/storagev2/packed/manifest_ffi.go`

```go
type Fragment struct {
    FragmentID int64                 // Unique fragment identifier
    FilePath   string                // File path in external storage
    StartRow   int64                 // Start row index within the file (inclusive)
    EndRow     int64                 // End row index within the file (exclusive)
    RowCount   int64                 // Number of rows (EndRow - StartRow)
    Deltalogs  []*datapb.FieldBinlog // Source delete logs for milvus-table
}
```

### 4.5 Segment Row Mapping

**File**: `internal/datanode/external/task_update.go`

`buildCurrentSegmentFragments` reads each current segment's manifest into a
segment-to-fragments map. Fragment identity uses the source file path and row
range. `organizeSegments` compares these fragments with the refreshed source to
identify unchanged segments, segments requiring a manifest patch, and orphan
fragments requiring new segments.

---

## 5. Feature Restrictions for External Collections

### 5.1 Compaction Disabled

**Files**:
- `internal/datacoord/compaction_policy_single.go`
- `internal/datacoord/compaction_policy_l0.go`
- `internal/datacoord/compaction_policy_clustering.go`
- `internal/datacoord/compaction_trigger.go`
- `internal/datacoord/compaction_trigger_v2.go`

All compaction types are skipped for external collections:

```go
// In each compaction policy/trigger
if collection.IsExternal() {
    log.Info("skip compaction for external collection", zap.Int64("collectionID", collection.ID))
    continue  // or return nil
}
```

| Compaction Type | Status |
|-----------------|--------|
| Single Compaction | Disabled |
| L0 Compaction | Disabled |
| Clustering Compaction | Disabled |
| Sort Compaction | Disabled |

### 5.2 Stats Tasks

**File**: `internal/datacoord/stats_inspector.go`

External collections allow text-index stats tasks for persisted `text_match`
support. They also allow JSON key stats tasks for StorageV3 segments that have
already committed a manifest path, because the stats result can be written back
through the manifest. Other stats task types are still skipped:

```go
func (si *statsInspector) SubmitStatsTask(..., subJobType indexpb.StatsSubJob, ...) {
    if si.isExternalCollection(segment.GetCollectionID()) {
        if subJobType == indexpb.StatsSubJob_JsonKeyIndexJob &&
            !canBuildExternalJSONKeyIndex(segment) {
            log.Info("skip submit external json stats task without v3 manifest")
            return nil
        }
        if subJobType != indexpb.StatsSubJob_TextIndexJob &&
            subJobType != indexpb.StatsSubJob_JsonKeyIndexJob {
            log.Info("skip submit stats task for external collection")
            return nil
        }
    }
    // ... submit task
}
```

| Stats Task Type | Status |
|-----------------|--------|
| Text Index Stats | Enabled for persisted `text_match` support |
| JSON Key Index Stats | Enabled only for StorageV3 external segments with a non-empty manifest path |
| BM25 Stats | Disabled; BM25 function stats are generated during refresh |

### 5.3 Write and Schema Operations

**Files**:
- `internal/proxy/task_insert.go`
- `internal/proxy/task_delete.go`
- `internal/proxy/task_upsert.go`
- `internal/proxy/task_import.go`
- `internal/proxy/task_flush.go`
- `internal/proxy/task.go` (add field, alter field, create/drop partition)
- `internal/proxy/impl.go` (manual compaction)

| Operation | Status | Details |
|-----------|--------|---------------|
| Insert | Blocked | "insert operation is not supported for external collection" |
| Delete | Blocked | "delete operation is not supported for external collection" |
| Upsert | Blocked | "upsert operation is not supported for external collection" |
| Import | Blocked | "import operation is not supported for external collection" |
| Flush | Blocked | "flush operation is not supported for external collection" |
| Add Field | Allowed with constraints | Requires `external_field` mapping; refresh makes the added source column available |
| Alter Field | Blocked | "alter field operation is not supported for external collection" |
| Create Partition | Blocked | "create partition operation is not supported for external collection" |
| Drop Partition | Blocked | "drop partition operation is not supported for external collection" |
| Manual Compaction | Blocked | "manual compaction is not supported for external collection" |

---

## 6. QueryNode Loading Support

### 6.1 Virtual Primary Key

Collections without a user primary key use a virtual PK. `milvus-table` collections
with a mapped source primary key use the source values instead.

**Format**: `((segmentID & 0xFFFFFFFF) << 32) | (offset & 0xFFFFFFFF)`

**File**: `internal/core/src/common/VirtualPK.h`

```cpp
inline int64_t GetVirtualPK(int64_t segment_id, int64_t offset) {
    return ((segment_id & 0xFFFFFFFF) << 32) | (offset & 0xFFFFFFFF);
}

inline int64_t ExtractSegmentIDFromVirtualPK(int64_t virtual_pk) {
    return static_cast<int64_t>(static_cast<uint64_t>(virtual_pk) >> 32);
}

inline int64_t ExtractOffsetFromVirtualPK(int64_t virtual_pk) {
    return virtual_pk & 0xFFFFFFFF;
}
```

The encoding reserves 32 bits for the row offset and retains only the lower
32 bits of the segment ID. Segment matching compares these truncated IDs;
uniqueness requires their values to be distinct among concurrently loaded
segments of the same collection.

### 6.2 VirtualPKChunkedColumn

**File**: `internal/core/src/mmap/VirtualPKChunkedColumn.h`

`VirtualPKChunkedColumn` implements `ChunkedColumnInterface` for collections
without a source primary key. It synthesizes values from the segment ID and row
offset. Accessors that require a contiguous buffer materialize the PK values
locally; no source PK column is read.

### 6.3 Manifest Column Loading

**Files**:
- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`
- `internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.cpp`

External fields are read through manifest column groups. Each load group uses a
`ManifestGroupTranslator` and `ChunkedColumnGroup`, with a `ProxyChunkColumn` for
each field. Warmup policy determines eager or lazy loading; source primary-key
fields are loaded eagerly.

Search and Query use the storage reader's `take()` API for eligible output fields
when `queryNode.externalCollection.useTakeForOutput` is enabled (the default).
Fields not filled by Take use the column-read path.

### 6.4 ExternalSegmentCandidate

**File**: `internal/querynodev2/pkoracle/external_segment_candidate.go`

Virtual-PK collections use `ExternalSegmentCandidate` for segment matching instead
of source primary-key bloom filters. It extracts the upper 32 bits of a virtual
PK using an unsigned shift and compares them with `segmentID & 0xFFFFFFFF`.

Real-PK `milvus-table` segments use source primary-key bloom-filter statistics.
Loading fails if the required bloom-filter candidate cannot be built.

### 6.5 Loading Flow

**File**: `internal/querynodev2/segments/segment_loader.go`

1. Load manifest column groups and indexes using the resolved field mappings.
2. Synthesize virtual PKs when needed. For real-PK `milvus-table` segments, load
   the source primary key and insert timestamps.
3. Load applicable delta logs. For real-PK `milvus-table` segments, source deltas
   use external storage configuration and target-owned deltas use target storage
   configuration. Virtual-PK source deletes are converted during refresh.
4. Install an `ExternalSegmentCandidate` for virtual PKs, or source bloom-filter
   statistics for real PKs.

See [Milvus Snapshot as External Table Source](20260526-milvus-table-external-source.md)
for snapshot delete handling.

---

## 7. Update Task System

### 7.1 Task Flow Overview

External data refresh is manually triggered through `RefreshExternalCollection`.
One refresh job may contain multiple DataNode tasks; clients track the job as a
whole.

```
Client: RefreshExternalCollection
                 |
                 v
Proxy: validate source/spec pair and forward
                 |
                 v
DataCoord: allocate job ID and broadcast refresh through WAL
                 |
                 v
ACK callback: persist job idempotently
                 |
                 v
Refresh Manager: explore source and persist explore manifest
                 |
                 v
Split file ranges into tasks and enqueue in the scheduler
                 |
                 v
DataNode tasks: read assigned fragments, keep/patch/create segments
                 |
                 v
DataCoord: persist each task's result
                 |
                 v
All tasks finished: aggregate results and apply segment updates
                 |
                 v
Complete job; attempt to persist changed source/spec in collection schema
                 |
                 v
Client: GetRefreshExternalCollectionProgress(job_id)
```

Task failures are reflected in the job state. Segment updates are applied from
aggregated results after all tasks finish, rather than independently by each
worker.

### 7.1.1 Job Lifecycle

```
RefreshExternalCollection()
              |
              v
           Pending ---- exploration failure / timeout ----> Failed
              |
              | Tasks scheduled
              v
          InProgress -------- task failure / timeout -----> Failed
              |
              | All results persisted and segment updates applied
              v
          Completed
```

**State Descriptions**:
- **Pending**: Job is persisted; source exploration or task scheduling is pending
- **InProgress**: DataNode tasks are executing or retrying
- **Completed**: Job finished successfully, segments updated
- **Failed**: Job encountered an error, reason stored in job info

### 7.1.2 Parallel Task Partitioning and Segment Ownership

Parallel refresh partitions the newly explored external files, but existing
segments cannot be partitioned independently from those files. A DataNode
decides whether an existing segment can be kept by checking whether every old
fragment is present in the task's newly explored fragment set. If a segment is
sent to a task that sees only part of its files, the task would incorrectly
treat the fragments owned by a sibling task as removed.

To prevent this, DataCoord builds one file-level ownership plan for the whole
job. The plan guarantees:

1. Every explored file index belongs to exactly one continuous task range.
2. Every baseline segment is owned by exactly one task.
3. The owner task's file range contains every still-existing file referenced
   by that segment.
4. A task receives only the baseline segments it owns.
5. Segment changes are applied once at the job level, never independently by
   sibling tasks.

The relevant implementation is in:

- `internal/datacoord/external_collection_refresh_manager.go`
- `internal/datacoord/external_collection_refresh_planner.go`
- `internal/datacoord/task_refresh_external_collection.go`
- `internal/datanode/external/task_update.go`

#### Planning Inputs

DataCoord performs `Explore` once per planning attempt and writes the complete,
ordered file list to a shared explore manifest. All tasks in one successfully
published plan use that manifest. If planning fails before publication, a retry
may run `Explore` again and create a new attempt manifest. DataCoord then reads
the manifests of all currently healthy segments as that attempt's baseline and
builds:

- `file path -> explored file index`
- `segment ID -> old fragment file paths`

Task file ranges are half-open indexes into the shared manifest:
`[file_index_begin, file_index_end)`.

#### Base File Ranges

`dataCoord.externalCollectionFilesPerTask` controls the target size of the
initial split. For `N` explored files and target `T`:

```text
base_task_count = ceil(N / T)
base_chunk_size = ceil(N / base_task_count)
```

DataCoord then creates balanced continuous base ranges of
`base_chunk_size` files. Therefore, the parameter controls the initial task
count and approximate task size; it is not a hard final limit.

For example, 10 files with a target of 6 produce two balanced base ranges of
5 files instead of ranges of 6 and 4.

#### Continuous-Range Closure

For each baseline segment, DataCoord finds the minimum and maximum indexes of
the segment's old file paths that are still present in the new explore result.
All base ranges between those two indexes must be merged into one task range.
The segment is owned by the task containing its first still-existing file.

Range merging is transitive. Consider eight files and a target of two files
per task:

```text
Base ranges:
T0 = [f0, f1]    T1 = [f2, f3]
T2 = [f4, f5]    T3 = [f6, f7]

S1 references f1 and f4  => merge T0 through T2
S2 references f5 and f6  => merge T2 through T3

Closure:
T0 through T3 become one task because the two required ranges overlap at T2.
```

This closure may remove several base boundaries. In the extreme case, chained
segment references can merge every base range and the job falls back to one
task. Conversely, when no segment crosses a base boundary, the original base
task count is preserved.

The planner operates at file-path level. DataNode still performs the exact
fragment comparison using `(file_path, start_row, end_row)`. File-level
planning is sufficient because all fragments of one explored file are read by
the same task range.

#### Missing Files and New Files

- If only some old files of a segment remain, the surviving files determine
  its owner and closure range. DataNode sees the missing old fragment and
  correctly rebuilds or removes the segment.
- If none of a segment's old files remain, the segment is conservatively owned
  by the first task. That task validates the segment as removed.
- Newly added files do not need segment ownership. They are processed by the
  task whose file range contains them, so a task may legitimately own no old
  segments.

#### Persisted Plan and Execution

Each `ExternalCollectionRefreshTask` persists:

- `explore_manifest_path`
- `file_index_begin` and `file_index_end`
- `ownership_plan_version`
- `owned_segment_ids`

After publication, persisting ownership makes task retries and DataCoord
restart recovery use the same plan. At dispatch time, DataCoord reloads only
`owned_segment_ids` and sends those segments, together with the task's file
range, to DataNode.

Each successful DataNode task returns `kept_segments` and `updated_segments`.
DataCoord persists these per-task results without changing segment metadata.
After all sibling tasks finish, DataCoord validates the results against the
persisted ownership plan, aggregates them, and performs one job-level segment
update. Baseline segments that are neither kept nor updated are removed in
that single apply operation.

### 7.2 External Collection Refresh Manager

**Files**:
- `internal/datacoord/external_collection_refresh_manager.go`
- `internal/datacoord/ddl_callbacks_external_collection.go`

`SubmitRefreshJobWithID` accepts the job ID allocated before WAL broadcast and
persists the job idempotently. Only one active job is allowed per collection.

The manager explores the source on DataCoord, writes an explore manifest, and
splits the discovered file ranges into tasks using
`dataCoord.externalCollectionFilesPerTask`. Tasks are persisted and submitted to
the shared scheduler. The ownership planner merges ranges crossed by existing
segments and assigns each baseline segment to one task, as described in Section
7.1.2. Each task receives only its owned segments and checks their fragments
against its assigned file range.

When all tasks finish with persisted results, the manager aggregates their kept
segment IDs and updated segments and applies the collection-level change. It then
attempts to update changed source/spec values in the collection schema. This
schema update is best-effort: failure is logged without undoing the completed
segment update.

### 7.3 External Collection Refresh Metadata

**Files**:
- `internal/datacoord/external_collection_refresh_meta.go`
- `pkg/proto/data_coord.proto`

| Record | Purpose | Persisted Information |
|--------|---------|-----------------------|
| `ExternalCollectionRefreshJob` | Client-visible refresh operation | Job/collection IDs, source/spec, state, progress, timestamps, failure reason, task IDs |
| `ExternalCollectionRefreshTask` | Scheduler and worker execution unit | Task/job IDs, version, worker ID, state, explore manifest and file range, ownership plan version and owned segment IDs, kept segments, updated segments, result-ready flag |

The metadata layer supports recovery, per-collection active-job checks and
aggregation of task state and progress. A finished task must also have its result
payload persisted before the job can apply segment updates.

**Job Retention Policy**:
- Finished/Failed jobs are retained for `dataCoord.externalCollectionJobRetention`
  (seconds; default: 86400).
- The refresh checker handles timeouts and garbage collection of expired jobs.

### 7.4 RefreshExternalCollectionTask (DataCoord side)

**File**: `internal/datacoord/task_refresh_external_collection.go`

`refreshExternalCollectionTask` adapts persisted refresh tasks to the shared
scheduler. `CreateTaskOnWorker`, `QueryTaskOnWorker` and `DropTaskOnWorker` manage
worker execution. The request includes the assigned explore-manifest range,
current segments, schema and pre-allocated segment IDs.

`SetJobInfo` persists the worker's kept-segment IDs and updated-segment payload
through `UpdateResultWithMeta`, then notifies the manager to process the job.
Segment changes are applied at job level after result aggregation.

### 7.5 ExternalCollectionManager (DataNode side)

**File**: `internal/datanode/external/manager.go`

Manages task execution on DataNode:

```go
type ExternalCollectionManager struct {
    ctx       context.Context
    mu        sync.RWMutex
    tasks     map[TaskKey]*TaskInfo
    pool      *conc.Pool[any]
}

func (m *ExternalCollectionManager) SubmitTask(
    clusterID string,
    req *datapb.RefreshExternalCollectionTaskRequest,
    taskFunc func(context.Context) (*datapb.RefreshExternalCollectionTaskResponse, error),
) error
```

### 7.6 RefreshExternalCollectionTask (DataNode side)

**File**: `internal/datanode/external/task_update.go`

`RefreshExternalCollectionTask.Execute` performs the update for its assigned
source range:

1. Read source fragments from the explore manifest and assigned file range.
2. Read current segment manifests and build the segment-to-fragments mapping.
3. Compare fragments to retain unchanged segments, patch existing manifests, or
   collect orphan fragments for new segments.
4. Return unchanged segment IDs separately from updated segment payloads. Updated
   segments include both same-ID patches and newly allocated segments.

### 7.7 Segment Update Strategy

```
Current Segments in Milvus: [S1, S2, S3, S4, S5]

Aggregated Task Results:
  - keptSegments:    [S1]              (unchanged)
  - updatedSegments: [S3', S6, S7]     (same-ID patch and new segments)

Processing:
  1. Keep S1 unchanged.
  2. Patch S3 with its updated manifest and field metadata.
  3. Drop S2, S4 and S5.
  4. Add S6 and S7 with IDs allocated for the worker tasks.

Final Segments: [S1, S3', S6, S7]
```

Add-column refresh can patch an existing segment while preserving its ID and row
count. See [External Table Add-Column Refresh](20260526-external_table_add_column_refresh.md).

### 7.8 Fragment to Segment Organization

`balanceFragmentsToSegments` organizes orphan fragments for new segments:

1. Calculate total rows and read
   `dataNode.externalCollection.targetRowsPerSegment` (default: 1,000,000).
2. For ordinary file formats, sort fragments by row count descending and assign
   each to the bin with the fewest rows.
3. For `milvus-table`, map each source fragment to one target segment instead of
   bin-packing, preserving alignment with the source manifest.
4. Allocate segment IDs from the task's pre-allocated range and create manifests
   and segment metadata.

---

## 8. Manifest System

### 8.1 Manifest Creation

**File**: `internal/storagev2/packed/manifest_ffi.go`

Manifests are created to describe segment contents:

```go
func CreateManifestForSegment(
    basePath string,
    columns []string,
    format string,
    fragments []Fragment,
    storageConfig *indexpb.StorageConfig,
) (string, error) {
    // 1. Create column groups from fragments
    // 2. Begin transaction
    // 3. Commit transaction with column groups
    // 4. Return manifest path
}
```

### 8.2 Manifest Reading

```go
func ReadFragmentsFromManifest(
    manifestPath string,
    storageConfig *indexpb.StorageConfig,
    columns []string,
) ([]Fragment, error) {
    // 1. Parse manifest path to get base path
    // 2. Create properties from storage config
    // 3. Call exttable_read_column_groups FFI
    // 4. Extract fragments from matching column groups (all if columns is empty)
    // 5. Return fragment list
}
```

---

## 9. Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `dataNode.externalCollection.targetRowsPerSegment` | Target rows per segment for ordinary file formats | 1000000 |
| `dataCoord.externalCollectionCheckInterval` | Refresh checker interval, in seconds | 10 |
| `dataCoord.externalCollectionJobTimeout` | Refresh job timeout, in seconds | 3600 |
| `dataCoord.externalCollectionJobRetention` | Retention of finished/failed jobs, in seconds | 86400 |
| `dataCoord.externalCollectionFilesPerTask` | Target files per base refresh task; ownership closure may produce larger final tasks | 10000 |
| `dataCoord.externalCollectionPreAllocSegments` | IDs pre-allocated per task; segments and fake binlogs consume separate IDs | 500000 |
| `dataCoord.externalCollectionDropRatioWarn` | Segment-drop ratio above which a warning is logged | 0.9 |
| `queryNode.externalCollection.useTakeForOutput` | Use Take for eligible external output fields | true |
| `queryNode.externalCollection.samplePerSegment` | During new-segment creation: true samples each new segment; false reuses the first new segment's sample within each task. Add-column patches sample independently. | false |
| `queryNode.externalCollection.sampleRows` | Rows sampled for external segment size estimation | 100 |
| `queryNode.externalCollection.rawDataFactor` | Peak memory amplification factor for external loading | 2.0 |

The DataNode external-task worker pool is currently constructed with 8 workers in
`internal/datanode/data_node.go`; it is not controlled by an external-collection
pool-size parameter. Refresh admission allows one active job per collection.

---

## 10. Future Enhancements

1. **More Data Formats**: Support Delta Lake, ORC, etc.
2. **Partition Mapping**: Map external data partitions to Milvus partitions
3. **Change Data Capture**: Support CDC-based incremental updates
4. **Cross-source Query**: Query across multiple external sources

---

## 11. References

- [Milvus Storage Library](https://github.com/milvus-io/milvus-storage)
- [Apache Parquet Format](https://parquet.apache.org/)
- [Data Lakehouse Architecture](https://www.databricks.com/glossary/data-lakehouse)
