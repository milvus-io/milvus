# MEP: External Collection with Lakehouse Integration

## Summary

This MEP proposes an **External Collection** mechanism that enables Milvus to query lakehouse tables (Apache Paimon, Iceberg, Delta Lake) directly without data import, implementing a zero-ETL architecture. External Collections use **Virtual Segments** that reference lakehouse data splits rather than storing binlogs, enabling cost-efficient vector search over lakehouse data.

---

## Motivation

### Current Pain Points

1.  **Data Duplication**: Bulk import creates a second copy of data, doubling storage costs
    
2.  **Complex ETL**: Maintaining sync between lakehouse and Milvus requires additional pipelines
    
3.  **Delayed Freshness**: Batch import causes staleness
    
4.  **Operational Burden**: Managing two storage systems independently
    

### Value Proposition

*   **Zero-ETL**: Query lakehouse tables directly without data movement
    
*   **50%+ Cost Reduction**: Eliminate duplicate storage
    
*   **Simplified Architecture**: Single source of truth in lakehouse
    
*   **Real-time Freshness**: Query latest snapshots on-demand
    

### Use Cases

1.  **ML Feature Store Integration**: Query embeddings from feature stores (Feast, Tecton)
    
2.  **Data Lakehouse Analytics**: Search over vectors in Databricks/Snowflake tables
    
3.  **Hybrid Search**: Combine lakehouse metadata filtering with vector similarity
    
4.  **Cost Optimization**: Reduce TCO for large-scale vector workloads
    

---

## Public Interfaces

### 1. Collection Properties for External Tables

New properties in `CollectionSchema`:

```go
const (
	CollectionExternalEnabled                 = "collection.external.enabled"                    // "true" | "false"
	CollectionExternalTableType               = "collection.external.table_type"                 // "ICEBERG" | "PAIMON" | "HUDI"
	CollectionExternalFileFormat              = "collection.external.file_format"                // "PARQUET" | "ORC" | "LANCE"
	CollectionExternalCatalogURI              = "collection.external.catalog_uri"                // "http://rest-catalog:8181"
	CollectionExternalWarehouse               = "collection.external.warehouse"                  // "oss://bucket/path"
	CollectionExternalDatabase                = "collection.external.database"                   // "default"
	CollectionExternalTableName               = "collection.external.table_name"                 // "my_table"
	CollectionExternalTargetSplitSizeMB       = "collection.external.target_split_size_mb"       // "2048"
	CollectionExternalDataRefresh             = "collection.external.data_refresh"               // "LATEST" | "STATIC", default "LATEST"
	CollectionExternalSnapshotID              = "collection.external.snapshot_id"                // "123456"
	CollectionExternalTimestampMS             = "collection.external.timestamp_ms"               // "1704067200000"
)

```

### 2. Lakehouse Catalog Interface

```go
// pkg: internal/util/lakehouse
type Catalog interface {
    // Initialize connection to lakehouse catalog
    Init(ctx context.Context) error
  
    // Get temporary credentials (for STS-based access)
    GetCredentials(ctx context.Context) (*Credentials, error)
  
    // Get table metadata (schema, location, partitions)
    GetTableMetadata(ctx context.Context) (*TableMetadata, error)
    
    // Get latest snapshot ID
	GetSnapshotByMode(ctx context.Context, fillMode string, snapshotID int64, timestampMS int64) (*Snapshot, error)    
    
    // Plan data splits for parallel reading
    PlanSplits(ctx context.Context, snapshot *Snapshot, targetSizeMB int64, 
               tableLocation string, storageOptions map[string]string) ([]*lakehousepb.DataSplit, error)
}

```

### 3. Virtual Segment Proto

```protobuf
// pkg/proto/data_coord.proto
import "lakehouse.proto";

// ...
message SegmentInfo {
  // ...

  // Virtual Segment extra information for External Collection
  // If this field is set (non-nil), the segment is a Virtual Segment
  // If this field is nil, the segment is a Normal Segment
  lakehouse.VirtualSegmentExtra virtual_extra = 31;
}
```

```protobuf
// pkg/proto/lakehouse.proto
message VirtualSegmentExtra {
  map<string, string> properties = 1;   // External collection config
  bytes data_split = 2;                 // Lakehouse data split metadata
  int64 fill_job_id = 3;                // Fill job that created this segment
  int64 snapshot_id = 4;                // Lakehouse snapshot ID
  int64 timestamp_ms = 5;               // Creation timestamp
  string table_location = 6;            // Table path for index building
}

message DataSplit {
  int64 id = 1;              // Global unique ID (for shard assignment)
  int64 row_count = 2;       // Estimated row count from statistics
 
  PaimonDataSplit paimon_split = 10;
  IcebergDataSplit iceberg_split = 11;
}

message PaimonDeletionFile {
  string path = 1;              // Path to the deletion file
  int64 offset = 2;               // Size of the deletion file in bytes
  int64 length = 3;               // Size of the deletion file in bytes
  optional int64 cardinality = 4;    // Number of deletions, if known
}

// DataFileMeta 数据文件元信息
//
// 描述单个Paimon数据文件的完整元数据
// 参考paimon-cpp中的DataFileMeta结构
message PaimonDataFileMeta {
  string file_name = 1;              // Name of the data file
  int64 file_size = 2;               // Size in bytes
  int64 row_count = 3;               // Number of rows in this file
  int64 min_sequence_number = 4;     // Minimum sequence number
  int64 max_sequence_number = 5;     // Maximum sequence number
  int64 schema_id = 6;               // Schema version ID
  int32 level = 7;                   // File level (e.g., L0, L1)
  int64 creation_time = 8;           // Creation timestamp (epoch millis)
 
  // Statistics for optimization
  bytes min_key = 10;                // Minimum key in the file (BinaryRow)
  bytes max_key = 11;                // Maximum key in the file (BinaryRow)
  bytes key_stats = 12;                // Maximum key in the file (BinaryRow)
  bytes value_stats = 13;                // Maximum key in the file (BinaryRow)
  
  // Advanced features (对齐 paimon-cpp DataFileMeta)
  optional bytes embedded_index = 20; // File index filter bytes (small indexes)
  repeated string extra_files = 21;  // Changelog files (Paimon 0.2 legacy)
  optional string external_path = 22;  // External storage path (S3, HDFS, etc.)
  optional string file_source = 23;   // File source metadata (FileSource serialized)
  repeated string value_stats_cols = 24; // Columns included in value_stats
  optional int64 first_row_id = 25;   // First row ID in this file
  optional int64 delete_row_count = 26; // Number of deleted rows
  repeated string write_cols = 27;    // Columns written in this file
}

message PaimonDataSplit {
  int64 id = 1;                      // Split ID
  int64 snapshot_id = 2;             // Snapshot ID this split belongs to
  bytes partition = 3;               // Partition key (BinaryRow serialized)
  int32 bucket = 4;                  // Bucket number
  string bucket_path = 5;            // Path to the bucket
  optional int32 total_buckets = 6;  // Total number of buckets
  
  // Before files for incremental reading
  repeated PaimonDataFileMeta before_files = 10;
  repeated PaimonDeletionFile before_deletion_files = 11;
  
  // Current data files to read
  repeated PaimonDataFileMeta data_files = 12;
  repeated PaimonDeletionFile data_deletion_files = 13;
  
  // Flags
  bool is_streaming = 20;            // Whether this is a streaming split
  bool raw_convertible = 21;         // Whether can be converted to raw format
  
  // Computed properties
  int64 row_count = 30;              // Total row count across all data files
  optional int64 latest_file_creation_time = 31; // Latest creation time among files
}
```


```

### 4. Fill Job API

```protobuf
// pkg/proto/data_coord.proto
message FillJob {
    int64 jobID = 1;
    int64 collectionID = 3;
    repeated int64 partitionIDs = 5;
    repeated string vchannels = 6;
    schema.CollectionSchema schema = 7;
    internal.ImportJobState state = 11;
    repeated lakehouse.DataSplit splits = 14;
    repeated common.KeyValuePair options = 15;
    uint64 data_ts = 18;              // Data timestamp
    int64 snapshot_id = 19;            // Lakehouse snapshot ID
    string table_location = 22;        // Table location
}

message PreFillTask {
    int64 jobID = 1;
    int64 taskID = 2;
    int64 collectionID = 3;
    ImportTaskStateV2 state = 6;
    repeated FillSplitStats split_stats = 7; // Metadata statistics
}

message FillTask {
    int64 jobID = 1;
    int64 taskID = 2;
    int64 collectionID = 3;
    repeated int64 segmentIDs = 4;      // Virtual segment IDs
    int64 nodeID = 5;
    ImportTaskStateV2 state = 6;
    repeated FillSplitStats split_stats = 7;
}

```

### 5. Table Reader Interface (C++)

```c++
// internal/core/src/segcore/segment_c.h

CStatus
NewSegment(CCollection collection,
           SegmentType seg_type,
           int64_t segment_id,
           const CProto* virtual_sgment_extra,
           CSegmentInterface* newSegment,
           bool is_sorted_by_pk);
```
```cpp
// internal/core/src/segcore/storagev2translator/TableGroupChunkTranslator.h
class TableGroupChunkTranslator : public Translator<GroupChunk> {
public:
    TableGroupChunkTranslator(
        int64_t segment_id,
        const std::unordered_map<FieldId, FieldMeta>& field_metas,
        FieldDataInfo column_group_info,
        const std::string& table_location,
        const std::map<std::string, std::string>& storage_options,
        const std::vector<std::shared_ptr<::lakehouse::DataSplit>>& splits,
        bool use_mmap,
        LoadPriority load_priority
    );
    
    // Load data chunk from lakehouse on-demand
    std::unique_ptr<GroupChunk> load_group_chunk(const std::shared_ptr<arrow::Table>& table, cid_t cid);
};


```
```c++
// internal/core/src/storage/FileManager.h

namespace milvus::storage {
  
struct FileManagerContext {
    FileManagerContext(
        const proto::paimon::VirtualSegmentExtra* vsegExt = nullptr)
        : chunkManagerPtr(nullptr), vsegExt(vsegExt) {
    }
    FileManagerContext(
        const ChunkManagerPtr& chunkManagerPtr,
        const proto::paimon::VirtualSegmentExtra* vsegExt = nullptr)
        : chunkManagerPtr(chunkManagerPtr), vsegExt(vsegExt) {
    }
    FileManagerContext(
        const FieldDataMeta& fieldDataMeta,
        const IndexMeta& indexMeta,
        const ChunkManagerPtr& chunkManagerPtr,
        milvus_storage::ArrowFileSystemPtr fs = nullptr,
        const proto::paimon::VirtualSegmentExtra* vsegExt = nullptr)
        : fieldDataMeta(fieldDataMeta),
          indexMeta(indexMeta),
          chunkManagerPtr(chunkManagerPtr),
          vsegExt(vsegExt),
          fs(fs) {
    }
    // ...
}
}  // namespace milvus::storage
```
```c++
// internal/core/src/storage/Util.cpp

std::vector<FieldDataPtr>
GetFieldDatasFromStorageV2(std::vector<std::vector<std::string>>& remote_files,
                           const std::string& field_name,
                           int64_t field_id,
                           DataType data_type,
                           DataType element_type,
                           int64_t dim,
                           milvus_storage::ArrowFileSystemPtr fs,
                           const proto::paimon::VirtualSegmentExtra* vseg_ext) {
    if (vseg_ext) {
        return getFieldDatasFromTable(
            *vseg_ext, field_name, field_id, data_type, element_type, dim);
    } else {
        return getFieldDatasFromStorageV2(
            remote_files, field_id, data_type, element_type, dim, fs);
    }
}
```

### 6. SDK Interface (PyMilvus)

```python
# Define schema (must match lakehouse table schema)
schema = CollectionSchema(fields=[
    FieldSchema(name="id", dtype=DataType.INT64, is_primary=True),
    FieldSchema(name="embedding", dtype=DataType.FLOAT_VECTOR, dim=768),
    FieldSchema(name="metadata", dtype=DataType.VARCHAR, max_length=512)
])

# Create external collection & triggers Fill Job and create Virtual Segments
collection = Collection(
    name="paimon_embeddings",
    schema=schema,
    properties={
        "collection.external.enabled": "true",
        "collection.external.table_type": "ICEBERG",
        "collection.external.file_format": "PARQUET",
        "collection.external.catalog_uri": "http://rest-catalog:8181",
        "collection.external.warehouse": "test_iceberg",
        "collection.external.database": "default",
        "collection.external.table_name": "embeddings_v1",
        "collection.external.data_refresh": "LATEST",
        "collection.external.target_split_size_mb": "2048",
    }
)

# TODO: Use Bulk Import For now to trigger manual Fill Job
job_id = bulk_import(
    files=[],
    options={
        "data_refresh": "STATIC",
        "snapshot_id": "12345"
    }
)

# Load triggers Fill Job to refresh data if data_refresh is LATEST
collection.load()

# Query as usual (reads from lakehouse on-demand)
results = collection.search(
    data=[[0.1] * 768],
    anns_field="embedding",
    param={"metric_type": "L2", "params": {"nprobe": 16}},
    limit=10
)

```
---

## Design Details

### Architecture Overview

```plaintext
┌─────────────────────────────────────────────────────────────┐
│                        Milvus Cluster                        │
├─────────────────────────────────────────────────────────────┤
│  RootCoord  │  DataCoord   │  QueryCoord  │   QueryNode    │
│             │  ┌─────────┐ │              │  ┌──────────┐  │
│             │  │FillJob  │ │              │  │Virtual   │  │
│             │  │Manager  │ │              │  │Segment   │  │
│             │  └────┬────┘ │              │  │Loader    │  │
│             │       │      │              │  └────┬─────┘  │
│             │       ▼      │              │       │        │
│             │  ┌─────────┐ │              │       ▼        │
│             │  │Fill     │ │              │  ┌──────────┐  │
│             │  │Checker  │ │              │  │Table     │  │
│             │  └────┬────┘ │              │  │Chunk     │  │
│             │       │      │              │  │Translator│  │
└─────────────┼───────┼──────┼──────────────┼──────┼────────┘
              │       │      │              │      │         
              ▼       ▼      ▼              ▼      ▼         
         ┌────────────────────────────────────────────┐      
         │         Lakehouse Catalog Interface        │      
         │  (Paimon REST Catalog / Iceberg Catalog)   │      
         └────────────────┬───────────────────────────┘      
                          ▼                                   
                ┌──────────────────────┐                     
                │  Lakehouse Storage   │                     
                │  (OSS / S3 / HDFS)   │                     
                │                      │                     
                │  ┌────────────────┐  │                     
                │  │ Parquet Files  │  │                     
                │  │ (Data Splits)  │  │                     
                │  └────────────────┘  │                     
                └──────────────────────┘                     

```

### Component Interactions

#### 1. Collection Creation Flow

```plaintext
User (PyMilvus)
  │
  ├─ CreateCollection(schema, properties)
  │
  ▼
RootCoord
  │
  ├─ Validate schema compatibility with lakehouse table
  │
  ├─ Check field types (no Array/JSON for external collections)
  │
  └─ Save collection metadata with external properties

```

#### 2. Load Collection Flow (Fill Pipeline)

```plaintext
User
  │
  ├─ collection.load()
  │
  ▼
Proxy
  │
  └─ LoadCollection request
     │
     ▼
QueryCoord
  │
  └─ Trigger Fill Job in DataCoord
     │
     ▼
DataCoord (FillChecker)
  │
  ├─ Step 1: Validate collection is external
  ├─ Step 2: Connect to Lakehouse Catalog
  ├─ Step 3: Get latest snapshot
  ├─ Step 4: PlanSplits (divide table into DataSplits)
  ├─ Step 5: Create PreFillTasks (one per DataSplit)
  │  └─ PreFillTask: Scan metadata, estimate row count
  ├─ Step 6: Create FillTasks
  │  └─ FillTask: Create Virtual Segments
  │     ├─ AllocVirtualSegment(dataSplit, snapshotID, tableLocation)
  │     └─ Segment only stores VirtualSegmentExtra (no binlog)
  │
  └─ Mark FillJob as Completed

```

#### 3. Query Execution Flow

```plaintext
User
  │
  ├─ collection.search(vectors)
  │
  ▼
Proxy
  │
  └─ SearchRequest
     │
     ▼
QueryNode
  │
  ├─ Load Virtual Segment
  │  └─ VirtualSegmentSealedImpl
  │     ├─ Read VirtualSegmentExtra
  │     └─ Create TableGroupChunkTranslator
  │        ├─ DataSplit → Table Reader
  │        └─ Read Parquet files from OSS/S3
  │
  ├─ Execute vector search
  │  └─ Use index if available, else brute-force scan
  │
  └─ Return topK results

```

#### 4. Index Building Flow

```plaintext
IndexNode
  │
  ├─ Build index request for Virtual Segment
  │
  └─ VirtualSegmentSealedImpl
     │
     ├─ Read field data from lakehouse via TableChunkTranslator
     │  └─ getFieldDatasFromTable(field_name)
     │     ├─ Create Table Reader with DataSplit
     │     └─ Read Arrow batches → Convert to Milvus FieldData
     │
     ├─ Build vector index (HNSW/IVF_FLAT/etc)
     │
     └─ Save index to Milvus storage (index is NOT virtual)

```

### Key Design Decisions

#### Virtual Segment Lifecycle

1.  **Creation**: Fill Job creates Virtual Segments with only metadata
    
    *   No binlog paths (empty `BinlogPaths` field)
        
    *   `VirtualExtra` contains DataSplit, snapshot ID, table location
        
    *   Row count from lakehouse metadata (not actual scan)
        
2.  **Loading**: QueryNode loads Virtual Segment into memory
    
    *   Reads VirtualExtra to get DataSplit
        
    *   Creates TableChunkTranslator for on-demand data access
        
    *   Lazy loading: Data read only when needed (query/index build)
        
3.  **Cleanup**: Old Virtual Segments removed when new Fill Job completes
    
    *   Compare `fill_job_id` in VirtualExtra
        
    *   Mark segments from old jobs as `Dropped`
        
    *   Ensures only latest snapshot's segments are active
        

#### Constraints on Virtual Segments

| Operation | Allowed? | Reason |
| --- | --- | --- |
| Compaction | ❌ No | Data not owned by Milvus |
| Insert/Delete | ❌ No | External table is read-only |
| Index Build | ✅ Yes | Indexes stored in Milvus |
| Query | ✅ Yes | Read lakehouse data on-demand |
| Binlog | ❌ No | No binlog paths |

#### Shard Assignment Strategy

Virtual Segments are distributed across shards using DataSplit ID:

```go
splitID := dataSplit.GetId()  // Global unique ID
vchannelIdx := int(splitID % int64(len(vchannels)))
partitionIdx := int(splitID % int64(len(partitionIDs)))

```

This ensures:

*   Balanced load across QueryNodes
    
*   Deterministic assignment (same split → same shard)
    
*   No hot partitions
    

---

## Compatibility, Deprecation, and Migration Plan

### Schema Compatibility

#### Scalar Type

| Milvus Type | Paimon Type | Supported? |
| --- | --- | --- |
| `Bool` | `BOOLEAN` | ✅ Yes |
| `Int8` | `TINYINT` | ✅ Yes |
| `Int16` | `SMALLINT` | ✅ Yes |
| `Int32` | `INT` | ✅ Yes |
| `Int64` | `BIGINT` | ✅ Yes |
| `Float` | `FLOAT` | ✅ Yes |
| `Double` | `DOUBLE` | ✅ Yes |
| `VarChar` | `STRING` / `VARCHAR` / `CHAR` | ✅ Yes |
| JSON | ❌ | ❌ Not supported in v1 |
| ARRAY | ❌ | ❌ Not supported in v1 |

#### Vector Type

| Milvus Type | Paimon Type | Supported? |
| --- | --- | --- |
| `FloatVector` | `ARRAY<FLOAT>` / `ARRAY<DOUBLE>` | ✅ Yes |
| `Float16Vector` | `ARRAY<FLOAT>` / `ARRAY<DOUBLE>` | ✅ Yes |
| `BinaryVector` | `ARRAY<BOOL>` | ✅ Yes |
| `Int8Vector` | `ARRAY<TINYINT>` | ✅ Yes |
| `SparseFloatVector` | ❌ | ❌ Not supported in v1 |

#### Array Type

| Milvus Type | Paimon Type | Supported? |
| --- | --- | --- |
| `Array<Bool>` | `ARRAY<BOOLEAN>` | ✅ Yes |
| `Array<Int8>` | `ARRAY<TINYINT>` | ✅ Yes |
| `Array<Int16>` | `ARRAY<SMALLINT>` | ✅ Yes |
| `Array<Int32>` | `ARRAY<INT>` | ✅ Yes |
| `Array<Int64>` | `ARRAY<BIGINT>` | ✅ Yes |
| `Array<Float>` | `ARRAY<FLOAT>` | ✅ Yes |
| `Array<Double>` | `ARRAY<DOUBLE>` | ✅ Yes |
| `Array<VarChar>` | `ARRAY<STRING>` | ✅ Yes |

### Backward Compatibility

*   Existing collections are **not affected**
    
*   External collections are **opt-in** (require explicit properties)
    
*   No breaking changes to existing APIs
    

---