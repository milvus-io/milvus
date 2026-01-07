# External Table (External Collection) Design Document

## 1. Overview

### 1.1 Background

External Table (External Collection) is a special type of data collection in Milvus that allows users to access data from external storage systems (such as S3, Iceberg, Delta Lake, etc.) without copying the data into Milvus local storage. This enables Milvus to serve as a query layer over existing data lakes while maintaining compatibility with standard Milvus query interfaces.

### 1.2 Goals

1. **Support External Table Creation**
   - Create external collections through standard `CreateCollection` API with `external_source` parameter
   - Map external data files (Parquet, etc.) to Milvus storage format via manifest files
   - Support field mapping between external columns and Milvus schema fields
   - Auto-inject virtual primary key for external collections

2. **Support Vector Index Building on External Tables**
   - Enable index creation on vector fields of external collections
   - Leverage milvus-storage library for efficient data access during index building
   - Support common index types (IVF, HNSW, etc.) on external data

3. **Support External Table Loading**
   - Load external collection segments into QueryNode memory
   - Use `ExternalFieldChunkedColumn` for lazy data loading from external sources
   - Generate virtual PKs using `(segmentID << 32) | offset` encoding
   - Use `ExternalSegmentCandidate` for PK-based segment matching (replacing bloom filters)

4. **Support External Table Querying**
   - Provide unified query interface consistent with regular collections
   - Support vector similarity search on external data
   - Support scalar filtering and hybrid search
   - Enable search/query operations while blocking write operations

5. **Support External Table Data Updates**
   - Support manual trigger to refresh external table data (automatic detection not supported yet)
   - Synchronize external data changes with segment-level granularity
   - Implement incremental update strategy: keep unchanged segments, drop obsolete segments, add new segments
   - Balance orphan fragments into new segments using bin-packing algorithm

### 1.3 Non-Goals

The following features are explicitly **NOT supported** for external tables in the current implementation:

1. **Write Operations**
   - No support for insert, delete, upsert, or import operations
   - External tables are read-only; data modifications must be done at the source

2. **Function Features**
   - No support for user-defined functions (UDFs)
   - No support for embedding functions or built-in transformation functions

3. **Schema Modifications**
   - No support for schema evolution or alterations after creation
   - No support for adding new columns (AddField)
   - No support for altering existing field properties (AlterField)

4. **Dynamic Schema Features**
   - No support for dynamic fields (`EnableDynamicField`)
   - Schema must be fixed and fully defined at creation time

5. **Auto ID**
   - No support for auto-generated IDs (`AutoID`)
   - Virtual PK is generated using `(segmentID << 32) | offset` encoding instead

6. **Automatic Data Source Synchronization**
   - No support for automatic/periodic detection of external data source changes
   - Data updates must be triggered manually

7. **Other Limitations**
   - No support for partition key fields
   - No support for clustering key fields
   - No support for text match functionality
   - No support for struct array fields
   - No support for namespace fields

---

## 2. Architecture

### 2.1 System Architecture Diagram

```
+-------------------------------------------------------------------------+
|                              Client                                      |
|  - CreateCollection(schema with external_source)                         |
|  - AlterCollection (modify data source / trigger manual refresh)         |
+-----------------------------------+-------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                             Proxy                                        |
|  - ValidateExternalCollectionSchema()                                    |
|  - Block write operations for external collections                       |
+-----------------------------------+-------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                           RootCoord                                      |
|  - ValidateExternalCollectionSchema()                                    |
|  - Store ExternalSource/ExternalSpec in collection model                 |
+-----------------------------------+-------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                           DataCoord                                      |
|  +---------------------------------------------------------------+      |
|  |              Compaction (DISABLED for external)                |      |
|  |  - Single/L0/Clustering compaction skipped                     |      |
|  +---------------------------------------------------------------+      |
|  +---------------------------------------------------------------+      |
|  |              Stats Inspector (DISABLED for external)           |      |
|  |  - Text/JSON/BM25 stats tasks skipped                          |      |
|  +---------------------------------------------------------------+      |
|  +---------------------------------------------------------------+      |
|  |              UpdateExternalCollectionTask                      |      |
|  |  - Handle manual refresh requests                              |      |
|  |  - Coordinate with DataNode for data sync                      |      |
|  +---------------------------------------------------------------+      |
+-----------------------------------+-------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                           DataNode                                       |
|  +---------------------------------------------------------------+      |
|  |              ExternalCollectionManager                         |      |
|  |  - Task lifecycle management                                   |      |
|  |  - Worker pool for async execution                             |      |
|  +---------------------------------------------------------------+      |
|  +---------------------------------------------------------------+      |
|  |              UpdateExternalTask                                |      |
|  |  - Fetch fragments from external source                        |      |
|  |  - Compare with current segments                               |      |
|  |  - Organize orphan fragments into new segments                 |      |
|  |  - Create manifest files for segments                          |      |
|  +---------------------------------------------------------------+      |
+-----------------------------------+-------------------------------------+
                                    |
                                    v
+-------------------------------------------------------------------------+
|                           QueryNode                                      |
|  - Virtual PK generation: (segmentID << 32) | offset                    |
|  - ExternalFieldChunkedColumn: Lazy load via milvus-storage             |
|  - ExternalSegmentCandidate: PK-based segment matching                  |
|  - Skip delta logs and bloom filters for external collections           |
+-------------------------------------------------------------------------+
```

### 2.2 Component Responsibilities

| Component | Responsibility |
|-----------|---------------|
| `Proxy` | Validate external collection schema, skip PK validation, block write operations, handle refresh requests |
| `RootCoord` | Validate schema, store external source configuration |
| `DataCoord` | Disable compaction/stats, manage external collection update tasks |
| `DataNode` | Execute external source scanning, organize segments, create manifests |
| `QueryNode` | Load external data with virtual PK support, execute queries |

---

## 3. External Table API Design

### 3.1 API Reuse Strategy

External tables reuse existing Milvus APIs to minimize API surface and maintain consistency:

| Operation | API | Description |
|-----------|-----|-------------|
| Create external table | `CreateCollection` | Set `external_source` in schema to create external table |
| Drop external table | `DropCollection` | Standard drop collection API |
| Load external table | `LoadCollection` | Standard load collection API |
| Query external table | `Search` / `Query` | Standard search and query APIs |

### 3.1.1 New APIs for Data Refresh

The following new APIs are introduced specifically for external table data refresh:

| Operation | API | Description |
|-----------|-----|-------------|
| Trigger refresh | `RefreshExternalTable` | Manually trigger data refresh from external source |
| Get refresh progress | `GetRefreshExternalTableProgress` | Get progress of a specific refresh job |
| List refresh jobs | `ListRefreshExternalTableJobs` | List all refresh jobs for a collection |

### 3.1.2 RefreshExternalTable API

Manually triggers a data refresh job for an external collection.

**Proto Definition** (`milvus.proto`):
```protobuf
// Job state enumeration for external table refresh
enum RefreshExternalTableState {
    RefreshStatePending = 0;      // Job is queued, waiting to execute
    RefreshStateInProgress = 1;   // Job is currently executing
    RefreshStateCompleted = 2;    // Job completed successfully
    RefreshStateFailed = 3;       // Job failed with error
}

message RefreshExternalTableRequest {
    common.MsgBase base = 1;
    string db_name = 2;              // Database name
    string collection_name = 3;      // Collection name (required)
    string external_source = 4;      // Optional: new external source path
    string external_spec = 5;        // Optional: new external spec configuration
}

message RefreshExternalTableResponse {
    common.Status status = 1;
    string job_id = 2;               // Unique job identifier for tracking
}
```

**Behavior**:
- If `external_source` or `external_spec` is provided, updates the collection's external configuration before triggering refresh
- If not provided, refreshes using the current external source configuration
- Returns a `job_id` that can be used to track progress
- Job runs asynchronously; use `GetRefreshExternalTableProgress` to monitor

**Example Usage**:
```python
# Refresh with current data source
response = client.refresh_external_table(
    collection_name="my_external_collection"
)
job_id = response.job_id

# Refresh with updated data source path
response = client.refresh_external_table(
    collection_name="my_external_collection",
    external_source="s3://my-bucket/new-path/",
    external_spec='{"format": "parquet"}'
)
```

### 3.1.3 GetRefreshExternalTableProgress API

Gets the current progress and status of a refresh job.

**Proto Definition** (`milvus.proto`):
```protobuf
message GetRefreshExternalTableProgressRequest {
    common.MsgBase base = 1;
    string job_id = 2;               // Job ID from RefreshExternalTable response
}

message RefreshExternalTableJobInfo {
    string job_id = 1;                   // Job identifier
    string collection_name = 2;          // Collection name
    RefreshExternalTableState state = 3; // Current job state
    int64 progress = 4;                  // Progress percentage (0-100)
    string reason = 5;                   // Error message if failed
    string external_source = 6;          // External source used for this job
    int64 start_time = 7;                // Job start timestamp
    int64 end_time = 8;                  // Job end timestamp (0 if not completed)
}

message GetRefreshExternalTableProgressResponse {
    common.Status status = 1;
    RefreshExternalTableJobInfo job_info = 2;
}
```

**Behavior**:
- Returns detailed progress information for the specified job
- State transitions: Pending → InProgress → Completed/Failed

**Example Usage**:
```python
# Get progress of a specific job
progress = client.get_refresh_external_table_progress(job_id="job_123456")
print(f"State: {progress.state}")
print(f"Progress: {progress.progress}%")
```

### 3.1.4 ListRefreshExternalTableJobs API

Lists all refresh jobs for a collection.

**Proto Definition** (`milvus.proto`):
```protobuf
message ListRefreshExternalTableJobsRequest {
    common.MsgBase base = 1;
    string db_name = 2;              // Database name
    string collection_name = 3;      // Collection name (optional, if empty lists all)
    int64 limit = 4;                 // Max number of jobs to return (default: 100)
}

message ListRefreshExternalTableJobsResponse {
    common.Status status = 1;
    repeated RefreshExternalTableJobInfo jobs = 2;
}
```

**Behavior**:
- Returns jobs sorted by start_time (most recent first)
- If `collection_name` is empty, returns jobs for all external collections
- Jobs are retained for a configurable period after completion (default: 24 hours)

**Example Usage**:
```python
# List all jobs for a collection
jobs = client.list_refresh_external_table_jobs(
    collection_name="my_external_collection"
)
for job in jobs:
    print(f"Job {job.job_id}: {job.state} ({job.progress}%)")

# List all external table refresh jobs
all_jobs = client.list_refresh_external_table_jobs()
```

### 3.2 Create External Table

External collections are created through the standard `CreateCollection` API by setting `external_source` in the schema:

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

External collections have the following restrictions enforced by `ValidateExternalCollectionSchema()`:

| Feature | Status | Reason |
|---------|--------|--------|
| Primary Key | Not Allowed | Virtual PK generated automatically |
| Dynamic Field | Not Allowed | Schema must be fixed |
| Partition Key | Not Allowed | External data partitioning not supported |
| Clustering Key | Not Allowed | No clustering compaction |
| Auto ID | Not Allowed | IDs come from external source |
| Text Match | Not Allowed | Requires internal indexing |
| Struct Array Fields | Not Allowed | Complex types not supported |
| Namespace Field | Not Allowed | External isolation not supported |

**Implementation**: `pkg/util/typeutil/schema.go`

```go
// IsExternalCollection returns true when schema describes an external collection.
func IsExternalCollection(schema *schemapb.CollectionSchema) bool {
    if schema == nil {
        return false
    }
    return strings.TrimSpace(schema.GetExternalSource()) != ""
}

// ValidateExternalCollectionSchema ensures unsupported features are disabled for external collections.
func ValidateExternalCollectionSchema(schema *schemapb.CollectionSchema) error {
    if !IsExternalCollection(schema) {
        return nil
    }

    if schema.GetEnableDynamicField() {
        return fmt.Errorf("external collection %s does not support dynamic field", schema.GetName())
    }

    if len(schema.GetStructArrayFields()) > 0 {
        return fmt.Errorf("external collection %s does not support struct fields", schema.GetName())
    }

    for _, field := range schema.GetFields() {
        // Skip system fields (RowID and Timestamp)
        if field.GetName() == common.RowIDFieldName || field.GetName() == common.TimeStampFieldName {
            continue
        }

        if field.GetIsPrimaryKey() {
            return fmt.Errorf("external collection %s does not support primary key field %s", schema.GetName(), field.GetName())
        }
        if field.GetIsPartitionKey() {
            return fmt.Errorf("external collection %s does not support partition key field %s", schema.GetName(), field.GetName())
        }
        if field.GetIsClusteringKey() {
            return fmt.Errorf("external collection %s does not support clustering key field %s", schema.GetName(), field.GetName())
        }
        if field.GetAutoID() {
            return fmt.Errorf("external collection %s does not support auto id on field %s", schema.GetName(), field.GetName())
        }

        helper := CreateFieldSchemaHelper(field)
        if helper.EnableMatch() {
            return fmt.Errorf("external collection %s does not support text match on field %s", schema.GetName(), field.GetName())
        }

        // Validate external_field mapping is set for all user fields
        if field.GetExternalField() == "" {
            return fmt.Errorf("field '%s' in external collection %s must have external_field mapping", field.GetName(), schema.GetName())
        }
    }

    return nil
}
```

### 3.4 No Primary Key
**Primary Key Validation**: `internal/proxy/task.go`

For external collections, primary key validation is skipped during `CreateCollection`:

```go
func (t *createCollectionTask) PreExecute(ctx context.Context) error {
    // ...
    isExternalCollection := typeutil.IsExternalCollection(t.schema)
    if err := typeutil.ValidateExternalCollectionSchema(t.schema); err != nil {
        return err
    }

    // validate primary key definition when needed
    if !isExternalCollection {
        if err := validatePrimaryKey(t.schema); err != nil {
            return err
        }
    }
    // ...
}
```

### 3.5 External Field Mapping

Each field in the schema must specify `external_field` to map to the external data source column. This field is defined in `milvus-proto` schema definition:

**Proto Definition** (`schema.proto`):
```protobuf
message FieldSchema {
    // ... other fields ...
    string external_field = 17;  // external field name - maps to column name in external source
}
```

**Validation Rules** (`internal/proxy/util.go`):
- All fields in external collections (except system fields like RowID and Timestamp) must have `external_field` set
- If `external_field` is empty, validation will fail with error: `field 'xxx' in external collection must have external_field mapping`

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
- More formats to be added (e.g., `iceberg`, `delta`)

### 4.4 Fragment Structure

**File**: `internal/storagev2/exttable/manifest_ffi.go`

```go
type Fragment struct {
    FragmentID int64   // Unique fragment identifier
    FilePath   string  // File path in external storage
    StartRow   int64   // Start row index within the file (inclusive)
    EndRow     int64   // End row index within the file (exclusive)
    RowCount   int64   // Number of rows (EndRow - StartRow)
}
```

### 4.5 Segment Row Mapping

**File**: `internal/datanode/external/task_update.go`

```go
type SegmentRowMapping struct {
    SegmentID int64
    TotalRows int64
    Ranges    []FragmentRowRange
    Fragments []exttable.Fragment
}

type FragmentRowRange struct {
    FragmentID int64
    StartRow   int64  // inclusive
    EndRow     int64  // exclusive
}
```

---

## 5. Disabled Features for External Collections

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

### 5.2 Stats Tasks Disabled

**File**: `internal/datacoord/stats_inspector.go`

All stats task types are skipped for external collections:

```go
func (si *statsInspector) SubmitStatsTask(...) {
    if si.isExternalCollection(segment.GetCollectionID()) {
        log.Info("skip submit stats task for external collection")
        return nil
    }
    // ... submit task
}
```

| Stats Task Type | Status |
|-----------------|--------|
| Text Index Stats | Disabled |
| JSON Key Index Stats | Disabled |
| BM25 Stats | Disabled |

### 5.3 Write Operations Blocked

**Files**:
- `internal/proxy/task_insert.go`
- `internal/proxy/task_delete.go`
- `internal/proxy/task_upsert.go`
- `internal/proxy/task_import.go`
- `internal/proxy/task_flush.go`
- `internal/proxy/task.go` (add field, alter field, create/drop partition)
- `internal/proxy/impl.go` (manual compaction)

| Operation | Status | Error Message |
|-----------|--------|---------------|
| Insert | Blocked | "insert operation is not supported for external collection" |
| Delete | Blocked | "delete operation is not supported for external collection" |
| Upsert | Blocked | "upsert operation is not supported for external collection" |
| Import | Blocked | "import operation is not supported for external collection" |
| Flush | Blocked | "flush operation is not supported for external collection" |
| Add Field | Blocked | "add field operation is not supported for external collection" |
| Alter Field | Blocked | "alter field operation is not supported for external collection" |
| Create Partition | Blocked | "create partition operation is not supported for external collection" |
| Drop Partition | Blocked | "drop partition operation is not supported for external collection" |
| Manual Compaction | Blocked | "manual compaction is not supported for external collection" |

---

## 6. QueryNode Loading Support

### 6.1 Virtual Primary Key

External collections don't have a primary key field. Instead, a **virtual PK** is generated:

**Format**: `(segmentID << 32) | offset`

**File**: `internal/core/src/common/VirtualPK.h`

```cpp
inline int64_t GenerateVirtualPK(int64_t segment_id, int32_t offset) {
    return (segment_id << 32) | static_cast<int64_t>(offset);
}

inline std::pair<int64_t, int32_t> ParseVirtualPK(int64_t virtual_pk) {
    int64_t segment_id = virtual_pk >> 32;
    int32_t offset = static_cast<int32_t>(virtual_pk & 0xFFFFFFFF);
    return {segment_id, offset};
}
```

This encoding allows:
- Up to 2^32 segments per collection
- Up to 2^32 rows per segment
- Efficient segment identification from PK

### 6.2 VirtualPKChunkedColumn

**File**: `internal/core/src/mmap/VirtualPKChunkedColumn.h`

Generates virtual PKs on-the-fly during loading without storing actual data:

```cpp
class VirtualPKChunkedColumn : public ChunkedColumnBase {
public:
    // Generates PKs based on segment ID and row offset
    // No actual data storage needed
    int64_t GetPK(int64_t row_offset) const {
        return GenerateVirtualPK(segment_id_, row_offset);
    }
};
```

### 6.3 ExternalFieldChunkedColumn

**File**: `internal/core/src/mmap/ExternalFieldChunkedColumn.h`

Lazy-loads field data from external storage via milvus-storage library:

```cpp
class ExternalFieldChunkedColumn : public ChunkedColumnBase {
    // Loads data chunks from external source on demand
    // Uses milvus-storage library for S3/HDFS/etc. access
};
```

### 6.4 ExternalSegmentCandidate

**File**: `internal/querynodev2/pkoracle/external_segment_candidate.go`

Replaces bloom filter for PK-based segment matching:

```go
type ExternalSegmentCandidate struct {
    segmentID int64
    partition int64
    typ       commonpb.SegmentState
}

func (c *ExternalSegmentCandidate) MayPkExist(pk storage.PrimaryKey) bool {
    // Parse virtual PK to extract segment ID
    virtualPK := pk.GetValue().(int64)
    segmentID := virtualPK >> 32
    return segmentID == c.segmentID
}
```

### 6.5 Loading Flow

**File**: `internal/querynodev2/segments/segment_loader.go`

```go
func (loader *segmentLoader) Load(...) {
    if isExternalCollection(collectionSchema) {
        // 1. Virtual PK field already injected during creation

        // 2. Skip delta logs (no delete support)
        // 3. Skip bloom filter building
        // 4. Use ExternalFieldChunkedColumn for field data
        // 5. Use VirtualPKChunkedColumn for PK field
        // 6. Use ExternalSegmentCandidate instead of bloom filter
    }
}
```

---

## 7. Update Task System

### 7.1 Task Flow Overview

External table data refresh is **manually triggered** through the `RefreshExternalTable` API. This design provides users with full control over when data synchronization occurs and allows them to track progress.

```
                            Client
                              |
                              | RefreshExternalTable(collection_name)
                              v
                            Proxy
                              |
                              | Validate & Forward
                              v
                          DataCoord
                              |
                              | Create RefreshJob
                              v
                    ExternalCollectionTaskMeta
                    (Store job with Pending state)
                              |
                              v
                    ExternalCollectionScheduler
                              |
                              | Schedule job execution
                              v
                    CreateTaskOnWorker
                              |
                              v
                        DataNode
                (ExternalCollectionManager)
                              |
                              v
                    UpdateExternalTask
                              |
    +-------------------------+-------------------------+
    |                         |                         |
    v                         v                         v
Fetch fragments      Compare with          Organize orphan
from source         current segments       fragments to new
                                           segments
                              |
                              v
                    Create manifests
                              |
                              | Report progress
                              v
                        DataCoord
                  (UpdateJobProgress callback)
                              |
                              | Update job state & progress
                              v
                    ExternalCollectionTaskMeta
                              |
    +-------------------------+-------------------------+
    |                         |                         |
    v                         v                         v
Keep unchanged       Drop obsolete         Add new
segments             segments              segments
                              |
                              v
                    Mark job as Completed/Failed
                              |
                              v
                        Client
                              |
              GetRefreshExternalTableProgress(job_id)
                              |
                    [Poll until completed]
```

### 7.1.1 Job Lifecycle

```
     RefreshExternalTable()
              |
              v
       +------+------+
       |   Pending   |  <-- Job created, queued for execution
       +------+------+
              |
              | (Scheduler picks up job)
              v
       +------+------+
       | InProgress  |  <-- DataNode executing refresh task
       +------+------+
              |
        +-----+-----+
        |           |
        v           v
  +-----+----+  +---+-----+
  | Completed|  | Failed  |
  +----------+  +---------+
```

**State Descriptions**:
- **Pending**: Job is created and waiting to be scheduled
- **InProgress**: Job is actively being executed by DataNode
- **Completed**: Job finished successfully, segments updated
- **Failed**: Job encountered an error, reason stored in job info

### 7.2 ExternalCollectionScheduler

**File**: `internal/datacoord/external_collection_scheduler.go`

Manages the scheduling and execution of external collection refresh jobs:

```go
type ExternalCollectionScheduler interface {
    Start()
    Stop()

    // SubmitRefreshJob creates a new refresh job for the collection
    // Returns job_id for tracking
    SubmitRefreshJob(ctx context.Context, req *RefreshJobRequest) (string, error)

    // GetJobProgress returns the current progress of a job
    GetJobProgress(ctx context.Context, jobID string) (*RefreshJobProgress, error)

    // ListJobs returns all jobs for a collection (or all if collectionID is 0)
    ListJobs(ctx context.Context, collectionID int64, limit int) ([]*RefreshJobInfo, error)
}

type RefreshJobRequest struct {
    CollectionID   int64
    CollectionName string
    ExternalSource string  // Optional: update source before refresh
    ExternalSpec   string  // Optional: update spec before refresh
}

func (s *externalCollectionScheduler) SubmitRefreshJob(ctx context.Context, req *RefreshJobRequest) (string, error) {
    // 1. Generate unique job ID
    jobID := fmt.Sprintf("refresh_%d_%d", req.CollectionID, time.Now().UnixNano())

    // 2. If external source/spec provided, update collection metadata
    if req.ExternalSource != "" || req.ExternalSpec != "" {
        if err := s.updateCollectionExternalConfig(ctx, req); err != nil {
            return "", err
        }
    }

    // 3. Create job record with Pending state
    job := &ExternalCollectionTask{
        JobID:          jobID,
        CollectionID:   req.CollectionID,
        CollectionName: req.CollectionName,
        ExternalSource: req.ExternalSource,
        ExternalSpec:   req.ExternalSpec,
        State:          RefreshStatePending,
        StartTime:      time.Now().UnixMilli(),
    }

    if err := s.taskMeta.AddTask(job); err != nil {
        return "", err
    }

    // 4. Enqueue for execution
    s.jobQueue <- job

    return jobID, nil
}
```

### 7.3 ExternalCollectionTaskMeta

**File**: `internal/datacoord/external_collection_task_meta.go`

Manages job records and state persistence:

```go
type ExternalCollectionTaskMeta interface {
    // Job management
    GetJob(jobID string) (*ExternalCollectionTask, error)
    AddTask(task *ExternalCollectionTask) error
    UpdateTask(task *ExternalCollectionTask) error

    // Query methods
    ListJobsByCollection(collectionID int64, limit int) ([]*ExternalCollectionTask, error)
    ListAllJobs(limit int) ([]*ExternalCollectionTask, error)

    // Cleanup
    CleanupCompletedJobs(olderThan time.Duration) error
}

type ExternalCollectionTask struct {
    JobID          string                     // Unique job identifier
    CollectionID   int64                      // Collection ID
    CollectionName string                     // Collection name
    ExternalSource string                     // External source path used for this job
    ExternalSpec   string                     // External spec used for this job
    State          RefreshExternalTableState  // Current job state

    // Progress tracking
    TotalFragments     int64  // Total fragments to process
    ProcessedFragments int64  // Fragments processed so far
    NewSegments        int64  // Number of new segments created
    DroppedSegments    int64  // Number of segments dropped
    KeptSegments       int64  // Number of segments kept unchanged

    // Timestamps
    StartTime int64  // Job start time (Unix epoch ms)
    EndTime   int64  // Job end time (0 if not completed)

    // Error info
    Reason string  // Error message if failed
}
```

**Job Retention Policy**:
- Completed/Failed jobs are retained for `external.collection.job.retention.duration` (default: 24 hours)
- A background goroutine periodically cleans up old jobs

### 7.4 UpdateExternalCollectionTask (DataCoord side)

**File**: `internal/datacoord/task_update_external_collection.go`

Manages the lifecycle of external collection updates on coordinator:

```go
type UpdateExternalCollectionTask struct {
    taskID         int64
    collectionID   int64
    externalSource string
    externalSpec   string
    // ...
}

// Task lifecycle methods
func (t *UpdateExternalCollectionTask) CreateTaskOnWorker() error
func (t *UpdateExternalCollectionTask) QueryTaskOnWorker() error
func (t *UpdateExternalCollectionTask) SetJobInfo() error      // Process results
func (t *UpdateExternalCollectionTask) DropTaskOnWorker() error
```

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
    req *datapb.UpdateExternalCollectionRequest,
    taskFunc func(context.Context) (*datapb.UpdateExternalCollectionResponse, error),
) error
```

### 7.6 UpdateExternalTask (DataNode side)

**File**: `internal/datanode/external/task_update.go`

Executes the actual update logic:

```go
type UpdateExternalTask struct {
    ctx    context.Context
    req    *datapb.UpdateExternalCollectionRequest
    // ...
}

func (t *UpdateExternalTask) Execute(ctx context.Context) error {
    // 1. Fetch fragments from external source
    newFragments, err := t.fetchFragmentsFromExternalSource(ctx)

    // 2. Build current segment -> fragments mapping
    currentSegmentFragments := t.buildCurrentSegmentFragments()

    // 3. Compare and organize segments
    updatedSegments, err := t.organizeSegments(ctx, currentSegmentFragments, newFragments)

    return nil
}
```

### 7.7 Segment Update Strategy

```
Current Segments in Milvus:  [S1, S2, S3, S4, S5]

Worker Response:
  - keptSegments: [S1, S3]      (fragments unchanged)
  - updatedSegments: [S6', S7']  (new segments from orphan fragments)

Processing:
  1. Keep: S1, S3 (unchanged)
  2. Drop: S2, S4, S5 (mark as Dropped)
  3. Add:  S6, S7 (allocate new segment IDs)

Final Segments: [S1, S3, S6, S7]
```

### 7.8 Fragment to Segment Organization

The `balanceFragmentsToSegments` function organizes orphan fragments into balanced segments:

```go
func (t *UpdateExternalTask) balanceFragmentsToSegments(
    ctx context.Context,
    fragments []exttable.Fragment,
) ([]*datapb.SegmentInfo, error) {
    // 1. Calculate total rows
    // 2. Determine target rows per segment (default: 1M rows)
    // 3. Sort fragments by row count descending
    // 4. Greedy bin-packing: assign each fragment to bin with lowest row count
    // 5. Create manifest for each segment
    // 6. Return SegmentInfo list
}
```

---

## 8. Manifest System

### 8.1 Manifest Creation

**File**: `internal/storagev2/exttable/manifest_ffi.go`

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
) ([]Fragment, error) {
    // 1. Parse manifest path to get base path
    // 2. Create properties from storage config
    // 3. Call exttable_read_column_groups FFI
    // 4. Extract fragments from column groups
    // 5. Return fragment list
}
```

---

## 9. Configuration Parameters

| Parameter | Description | Default |
|-----------|-------------|---------|
| `external.collection.target.rows.per.segment` | Target rows per segment | 1,000,000 |
| `external.collection.worker.pool.size` | DataNode worker pool size | 4 |
| `external.collection.job.retention.duration` | How long to keep completed/failed jobs | 24h |
| `external.collection.job.max.concurrent` | Max concurrent refresh jobs | 2 |
| `external.collection.job.timeout` | Timeout for a single refresh job | 1h |

---

## 10. Future Enhancements

1. **Index Building**: Support index creation on external collections
2. **AlterCollection Support**: Add API to modify `external_source`/`external_spec`
3. **More Data Formats**: Support Iceberg, Delta Lake, ORC, etc.
4. **Partition Mapping**: Map external data partitions to Milvus partitions
5. **Change Data Capture**: Support CDC-based incremental updates
6. **Cross-source Query**: Query across multiple external sources

---

## 11. References

- [Milvus Storage Library](https://github.com/milvus-io/milvus-storage)
- [Apache Parquet Format](https://parquet.apache.org/)
- [Data Lakehouse Architecture](https://www.databricks.com/glossary/data-lakehouse)
