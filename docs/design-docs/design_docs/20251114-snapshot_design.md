# Milvus Snapshot Design Document

## Implementation Overview

Milvus Snapshot mechanism provides complete collection-level data snapshot capabilities, implementing point-in-time backup and restore functionality. The implementation includes the following core components:

### Architecture Components

**1. Snapshot Storage Layer (S3/Object Storage)**
- Stores complete snapshot data using Iceberg-like manifest format
- Metadata files (JSON): snapshot basic information, schema, index definitions, and manifest file paths array
- Manifest files (Avro): detailed segment descriptions and file paths

**2. Snapshot Metadata Management (Etcd)**
- Stores basic SnapshotInfo metadata
- Maintains references from snapshots to segments/indexes
- Provides fast query and list operations

**3. Collection Meta Restoration**
- Restores collection schema and index definitions from snapshot
- Maintains Field ID consistency: ensures field IDs remain unchanged via PreserveFieldId
- Maintains Index ID consistency: ensures index IDs remain unchanged via PreserveIndexId
- ID consistency guarantees full compatibility between snapshot data files and new collection

**4. Data Restore Mechanism (Copy Segment)**
- Implements fast recovery through direct segment file copying
- Manages copy operations using CopySegmentJob and CopySegmentTask
- Supports parallel copying of multiple segments to improve restore speed

**5. Garbage Collection Integration**
- Protects segments referenced by snapshots from accidental deletion
- Protects indexes referenced by snapshots to ensure restore availability
- Automatically cleans up associated storage files when snapshot is deleted

## Snapshot Creation Process

### Creation Workflow

**Execution Steps**:

1. **Acquire Snapshot Timestamp**: Obtain the minimum timestamp from channel seek positions as snapshotTs
2. **Filter Segments**: Select all non-dropped segments where StartPosition < snapshotTs
3. **Collect Metadata**: Retrieve collection schema, index definitions, and segment details (binlog/deltalog/indexfile paths)
4. **Write to S3**: Write complete metadata, schema, index, and segment information to S3 in manifest format
5. **Write to Etcd**: Save basic SnapshotInfo to Etcd and establish segment/index references

**Important Notes**:

- **CreateSnapshot does not actively flush data**: Only collects existing sealed segments
- **SnapshotTs Source**: Obtained from the minimum value of current consumption positions (seek positions) across all channels
- **Data Coverage**: All segments where StartPosition < snapshotTs
- **Best Practice (Strongly Recommended)**: Call Flush API before creating snapshot to ensure latest data is persisted. Flush is not mandatory but highly recommended to avoid missing data in growing segments

**Data Point-in-Time**:

- Snapshot contains all sealed segment data before snapshotTs
- To include latest data, it is strongly recommended to call Flush API before creating snapshot (not mandatory)
- Data in growing segments will not be included in the snapshot

## Snapshot Storage Implementation

### Storage Architecture

Adopts layered storage architecture with separation of responsibilities:

**Etcd Storage Layer** - Fast query and management
- SnapshotInfo basic information (name, description, collection_id, create_ts, s3_location)
- Used for fast list and query operations
- Does not contain complete schema/segment/index detailed information

**S3 Storage Layer** - Complete data persistence
- CollectionDescription: complete schema definition
- IndexInfo list: all index definitions and parameters
- SegmentDescription list: all segment file paths (binlog/deltalog/statslog/indexfile)

### S3 Storage Path Structure

Data is organized using Iceberg-like manifest format with the following directory structure:

```
snapshots/{collection_id}/
├── metadata/
│   └── {snapshot_id}.json         # Snapshot metadata (JSON format)
│
└── manifests/
    └── {snapshot_id}/             # Directory for each snapshot
        ├── {segment_id_1}.avro    # Individual segment manifest (Avro format)
        ├── {segment_id_2}.avro
        └── ...
```

**Design Note**: Each segment has its own manifest file to enable:
- Parallel reading of segment data
- Incremental updates without rewriting large files
- Better fault isolation (corrupted file affects only one segment)

### File Format Details

**1. metadata/{snapshot_id}.json (JSON format)**
- SnapshotInfo: snapshot basic information
- CollectionDescription: complete collection schema
- IndexInfo list: all index definitions
- ManifestList: string array containing paths to all manifest files
- SegmentIDs/IndexIDs: pre-computed ID lists for fast reload
- StorageV2ManifestList: segment-to-manifest mappings for StorageV2 format (optional)

**Structure of metadata JSON:**
```go
type SnapshotMetadata struct {
    SnapshotInfo          *datapb.SnapshotInfo          // Snapshot basic info
    Collection            *datapb.CollectionDescription // Complete schema
    Indexes               []*indexpb.IndexInfo          // Index definitions
    ManifestList          []string                      // Array of manifest file paths
    SegmentIDs            []int64                       // Pre-computed segment IDs for fast reload
    IndexIDs              []int64                       // Pre-computed index IDs for fast reload
    StorageV2ManifestList []*StorageV2SegmentManifest   // StorageV2 (Lance/Arrow) manifest mappings
}

type StorageV2SegmentManifest struct {
    SegmentID int64  // Segment ID
    Manifest  string // Path to StorageV2 manifest file
}
```

**Fast Reload Optimization**: The `SegmentIDs` and `IndexIDs` fields enable DataCoord to quickly reload snapshot metadata during startup without parsing heavy Avro manifest files. Full segment data is loaded on-demand.

**2. manifests/{snapshot_id}/{segment_id}.avro (Avro format)**
- One file per segment for parallel I/O and fault isolation
- Each file contains a single ManifestEntry with complete SegmentDescription:
  - segment_id, partition_id, segment_level
  - binlog_files, deltalog_files, statslog_files, bm25_statslog_files
  - index_files, text_index_files, json_key_index_files
  - num_of_rows, channel_name, storage_version
  - start_position, dml_position
  - is_sorted (whether segment data is sorted by primary key)
- Uses Avro format to provide schema evolution support

**StorageV2 Support**: For segments using Milvus's newer storage format, additional manifest paths are stored in `StorageV2ManifestList`. This enables seamless snapshot/restore for both legacy and new storage formats.

### Write Process (SnapshotWriter)

```
1. Save() - Main entry point for writing snapshot
   a. Create manifest directory: manifests/{snapshot_id}/
   b. For each segment, call writeSegmentManifest():
      - Convert SegmentDescription to Avro ManifestEntry
      - Serialize to Avro binary format
      - Write to S3: manifests/{snapshot_id}/{segment_id}.avro
   c. Collect StorageV2 manifest paths (if applicable)
   d. Call writeMetadataFile()

2. writeSegmentManifest() - Write individual segment manifest
   - Convert SegmentDescription to ManifestEntry (Avro struct)
   - Include all binlog, deltalog, statslog, index files
   - Include position info (start_position, dml_position)
   - Serialize and write to: manifests/{snapshot_id}/{segment_id}.avro

3. writeMetadataFile() - Write main metadata file
   - Create SnapshotMetadata containing:
     * SnapshotInfo
     * Collection schema
     * Index definitions
     * ManifestList array (paths from step 1)
     * SegmentIDs/IndexIDs (pre-computed for fast reload)
     * StorageV2ManifestList (if applicable)
   - Serialize to JSON format
   - Write to S3: metadata/{snapshot_id}.json
```

### Read Process (SnapshotReader)

```
1. ReadSnapshot(metadataFilePath, includeSegments) - Read snapshot data
   a. Read and parse metadata JSON file
   b. If includeSegments=false:
      - Return only SnapshotInfo, Collection, Indexes
      - Use pre-computed SegmentIDs/IndexIDs for fast reload
   c. If includeSegments=true:
      - For each path in ManifestList array:
        * Read Avro binary data
        * Parse ManifestEntry records
        * Convert to SegmentDescription
      - Populate StorageV2 manifest paths if present
   d. Return complete SnapshotData

2. ListSnapshots(collectionID) - List all snapshots for a collection
   - List all files in metadata/ directory
   - Parse each metadata file to extract SnapshotInfo
   - Return list of SnapshotInfo
```

**Performance Optimization**: The `includeSegments` parameter allows DataCoord to defer loading heavy segment data. During startup, only metadata is loaded (includeSegments=false), and full segment data is loaded on-demand when needed for restore operations.

## Snapshot Data Management

### SnapshotManager

**SnapshotManager** centralizes all snapshot-related business logic, providing a unified interface for snapshot lifecycle management and restore operations.

**Design Principles**:
- Encapsulates business logic from RPC handlers
- Manages dependencies through constructor injection
- Eliminates code duplication (state conversion, progress calculation)
- Maintains separation from background services (Checker/Inspector)

**Core Interface**:
```go
type SnapshotManager interface {
    // Snapshot lifecycle management
    CreateSnapshot(ctx context.Context, collectionID int64, name, description string) (int64, error)
    DropSnapshot(ctx context.Context, name string) error
    DescribeSnapshot(ctx context.Context, name string) (*SnapshotData, error)
    ListSnapshots(ctx context.Context, collectionID, partitionID int64) ([]string, error)

    // Restore operations
    RestoreSnapshot(ctx context.Context, snapshotName string, targetCollectionID int64) (int64, error)
    GetRestoreState(ctx context.Context, jobID int64) (*datapb.RestoreSnapshotInfo, error)
    ListRestoreJobs(ctx context.Context, collectionID int64) ([]*datapb.RestoreSnapshotInfo, error)
}
```


### Snapshot Metadata Management

**SnapshotMeta** manages snapshot reference relationships and lifecycle:

```go
type SnapshotDataInfo struct {
    snapshotInfo *datapb.SnapshotInfo
    SegmentIDs   typeutil.UniqueSet   // List of segments referenced by this snapshot
    IndexIDs     typeutil.UniqueSet    // List of indexes referenced by this snapshot
}

type snapshotMeta struct {
    catalog metastore.DataCoordCatalog
    snapshotID2DataInfo *typeutil.ConcurrentMap[UniqueID, *SnapshotDataInfo]
    reader *SnapshotReader
    writer *SnapshotWriter
}
```

**Core Functions**:

**1. SaveSnapshot() - Two-Phase Commit (2PC)**

SaveSnapshot uses a two-phase commit approach to ensure atomic creation and enable GC cleanup of orphan files:

```
Phase 1 (Prepare):
  - Save snapshot with PENDING state to catalog (etcd)
  - Snapshot ID is allocated and stored
  - PendingStartTime is recorded for timeout tracking

Write S3 Data:
  - Write segment manifest files to S3
  - Write metadata.json to S3
  - Update S3Location in snapshot info

Phase 2 (Commit):
  - Update snapshot state to COMMITTED in catalog
  - Insert into in-memory cache with precomputed ID sets
```

**Failure Handling**:
| Failure Point | State | Recovery Action |
|--------------|-------|-----------------|
| Phase 1 fails | No changes | Return error, no cleanup needed |
| S3 write fails | PENDING in catalog | GC will cleanup S3 files using snapshot ID |
| Phase 2 fails | PENDING in catalog, S3 data exists | GC will cleanup S3 files and catalog record |

**Key Design Points**:
- Snapshot ID is used to compute S3 paths, no S3 list operations needed for cleanup
- PENDING snapshots are filtered out during DataCoord reload
- GC uses `PendingStartTime` + timeout to identify orphaned snapshots

**2. DropSnapshot()**
- Removes snapshot metadata from memory and Etcd
- Calls SnapshotWriter.Drop() to clean up files on S3
- Deletes metadata file and all manifest files referenced in ManifestList

**3. GetSnapshotBySegment()/GetSnapshotByIndex()**
- Queries which snapshots reference a specific segment or index
- Used during GC to determine if resources can be deleted
- Uses precomputed SegmentIDs/IndexIDs sets for O(1) lookup per snapshot

**4. GetPendingSnapshots()**
- Returns all PENDING snapshots that have exceeded the timeout
- Used by GC to find orphaned snapshots for cleanup

**5. CleanupPendingSnapshot()**
- Removes a pending snapshot record from catalog
- Called by GC after S3 files have been cleaned up

### Garbage Collection Integration

**Pending Snapshot GC (recyclePendingSnapshots)**:

Cleans up orphaned snapshot files from failed 2PC commits:

```
Process flow:
1. Get all PENDING snapshots from catalog that have exceeded timeout
2. For each pending snapshot:
   a. Compute manifest directory: snapshots/{collection_id}/manifests/{snapshot_id}/
   b. Compute metadata file: snapshots/{collection_id}/metadata/{snapshot_id}.json
   c. Delete manifest directory using RemoveWithPrefix (no S3 list needed)
   d. Delete metadata file
   e. Delete etcd record
```

**Key Design Points**:
- NO S3 list operations: Uses RemoveWithPrefix for directory cleanup
- File paths computed from collection_id + snapshot_id stored in etcd
- Timeout mechanism (configurable via `SnapshotPendingTimeout`) prevents cleanup of snapshots still being created
- Runs as part of DataCoord's regular GC cycle

**Configuration**:
```yaml
dataCoord:
  gc:
    snapshotPendingTimeout: 10m  # Time before pending snapshot is considered orphaned
```

**Segment GC Protection**:

Snapshot protection mechanism is integrated in DataCoord's garbage_collector:

```go
func (gc *garbageCollector) isSnapshotSegment(collectionID, segmentID int64) bool {
    snapshotIDs := gc.meta.GetSnapshotMeta().GetSnapshotBySegment(ctx, collectionID, segmentID)
    return len(snapshotIDs) > 0
}
```

- In clearEtcd(), skips dropped segments referenced by snapshots
- In clearS3(), skips segment files referenced by snapshots
- GC is only allowed when segment is not referenced by any snapshot

**Index GC Protection**:

Index protection mechanism is integrated in IndexCoord:

```go
func (gc *garbageCollector) recycleUnusedIndexes() {
    // Check if index is referenced by snapshots
    snapshotIDs := gc.meta.GetSnapshotMeta().GetSnapshotByIndex(ctx, collectionID, indexID)
    if len(snapshotIDs) > 0 {
        // Skip index files that are still referenced by snapshots
        continue
    }
}
```

- Prevents drop index from deleting index files referenced by snapshots
- Ensures index data is available when restoring snapshots

### Snapshot Lifecycle

```
Create:
  User Request -> Proxy CreateSnapshot -> DataCoord
    -> GetSnapshotTs() (obtain minimum timestamp from channels)
    -> SelectSegments() (filter segments where StartPosition < snapshotTs)
    -> Collect Schema/Indexes/Segment detailed information
    -> SnapshotWriter.Save() (write to S3)
    -> SnapshotMeta.SaveSnapshot() (write to Etcd + memory)

Drop:
  User Request -> Proxy DropSnapshot -> DataCoord
    -> SnapshotMeta.DropSnapshot()
    -> Remove from Etcd
    -> SnapshotWriter.Drop() (delete S3 files)
    -> Remove from memory

List:
  User Request -> Proxy ListSnapshots -> DataCoord
    -> SnapshotMeta.ListSnapshots()
    -> Filter by collection/partition

Describe:
  User Request -> Proxy DescribeSnapshot -> DataCoord
    -> SnapshotMeta.GetSnapshot()
    -> Return SnapshotInfo

Restore:
  User Request -> Proxy RestoreSnapshot -> RootCoord
    -> DescribeSnapshot() from DataCoord (get schema, partitions, indexes)
    -> CreateCollection() with PreserveFieldId=true
    -> CreatePartition() for each user partition
    -> RestoreSnapshotDataAndIndex() to DataCoord
       -> Create indexes with PreserveIndexId=true
       -> Create CopySegmentJob
    -> Return job_id for async tracking
```

## Restore Snapshot Implementation

### Overview

Restore is implemented using the **Copy Segment mechanism**, which significantly improves recovery speed by directly copying segment files, avoiding the overhead of rewriting data and rebuilding indexes.

### Architecture Overview

The restore operation follows a layered architecture with clear separation of responsibilities:

```
User Request
    │
    ▼
┌─────────┐
│  Proxy  │  ← Entry point, request validation
└────┬────┘
     │
     ▼
┌───────────┐
│ RootCoord │  ← Orchestration: CreateCollection, CreatePartition, coordinate DataCoord
└─────┬─────┘
      │
      ▼
┌───────────┐
│ DataCoord │  ← Data restoration: CopySegmentJob creation, index creation
└─────┬─────┘
      │
      ▼
┌──────────┐
│ DataNode │  ← Execution: Copy segment files from S3
└──────────┘
```

### Restore Process

#### Phase 1: Request Entry (Proxy Layer)

```
1. Proxy RestoreSnapshotTask.Execute():
   - Validates request parameters
   - Delegates to RootCoord.RestoreSnapshot()
   - RootCoord orchestrates the entire restore process
```

**Design Notes**:
- Proxy layer is simplified to just delegate to RootCoord
- All orchestration logic is centralized in RootCoord for better transactional control

#### Phase 2: Collection and Schema Recreation (RootCoord Layer)

```
2. RootCoord restoreSnapshotTask.Execute():
   a. Get snapshot info from DataCoord
      - DescribeSnapshot() retrieves complete snapshot information (schema, partitions, indexes)

   b. Create collection with preserved field IDs
      - CreateCollection() with PreserveFieldId=true
      - Schema properties are completely copied (consistency level, num_shards, etc.)

   c. Create user partitions
      - CreatePartition() for each user-created partition in snapshot

   d. Restore data and indexes via DataCoord
      - RestoreSnapshotDataAndIndex() handles data copying and index creation
```

**Key Design Points**:
- `PreserveFieldId=true`: Ensures new collection field IDs match those in snapshot
- RootCoord handles rollback on failure (drops collection/partitions if restore fails)
- Schema properties are completely copied, including consistency level, num_shards, etc.

#### Phase 3: Data and Index Restoration (DataCoord Layer)

```
3. DataCoord RestoreSnapshotDataAndIndex():
   a. Read snapshot data
      - SnapshotMeta.ReadSnapshotData() reads complete segment information from S3

   b. Generate mappings
      - Channel Mapping: Map snapshot channels to new collection channels
      - Partition Mapping: Map snapshot partition IDs to new partition IDs

   c. Create indexes (if requested)
      - Create indexes with PreserveIndexId=true
      - Ensures index IDs match, allowing direct use of index files from snapshot

   d. Pre-register Target Segments
      - Allocate new segment IDs
      - Create SegmentInfo (with correct PartitionID and InsertChannel)
      - Pre-register to meta via meta.AddSegment(), State set to Importing

   e. Create Copy Segment Job
      - Create lightweight ID mappings (source_segment_id -> target_segment_id)
      - Generate CopySegmentJob and save to meta
      - CopySegmentChecker automatically creates CopySegmentTasks
```

**ID Mapping Structure**:
```protobuf
message CopySegmentIDMapping {
    int64 source_segment_id = 1;  // Segment ID in snapshot
    int64 target_segment_id = 2;  // Segment ID in new collection
    int64 partition_id = 3;       // Target partition ID (cached for grouping)
}
```

**Design Notes**:

- **Lightweight Design**: IDMapping only stores segment ID mappings (~48 bytes/segment), used to guide subsequent file copy operations
- **Actual Mapping Storage**: Real channel and partition mapping relationships are stored in SegmentInfo in meta
  - When creating job, target segments are pre-registered via `meta.AddSegment()`
  - Each target segment contains complete metadata: `PartitionID`, `InsertChannel`, `State`, etc.
  - Copy task reads segment information from meta during execution, no need to duplicate storage in IDMapping
- **Mapping Application Flow**: Calculate channel/partition mappings during job creation → Create target segments and write to meta → IDMapping only retains ID references

#### Phase 4: Copy Segment Execution (DataNode Layer)

```
4. CopySegmentChecker monitors job execution:
   
   Pending -> Executing:
   - Group segments by maxSegmentsPerCopyTask (currently default 10/group)
   - Create one CopySegmentTask for each group
   - Distribute tasks to DataNodes for execution
   
   Executing:
   - DataNode CopySegmentTask executes:
     a. Read all file paths of source segment
     b. Copy files to new paths (update segment_id/partition_id/channel_name)
     c. Update segment's binlog/deltalog/indexfile information in meta
   - CopySegmentChecker monitors progress of all tasks
   
   Executing -> Completed:
   - After all tasks complete
   - Update all target segments status to Flushed
   - Job marked as Completed
```

**Task Granularity Concurrency Design**:

- **Current Implementation**: Supports task-level concurrency based on segment grouping
  - Create tasks grouped by maxSegmentsPerCopyTask (default 10 segments)
  - Each group generates independent CopySegmentTask, which can be distributed to different DataNodes for parallel execution
  
  - **Long-term Optimization**: If copy performance is insufficient, implement concurrency acceleration at task level
    - **Adjust Grouping Granularity**: Reduce maxSegmentsPerCopyTask value to increase task count
      - Example: Reduce from 10 segments/task to 5 or fewer
      - More tasks can be scheduled in parallel to multiple DataNodes
    - **File-level Concurrency**: Parallelize copying of multiple files within a single task
      - Parallel copying of binlog/deltalog/indexfile
      - Need to coordinate concurrency count to avoid object storage throttling

**Copy Segment Details**:

File copy operations on DataNode:
```go
// For each segment in the task
1. Copy Binlog files
   - Read all binlog files of source segment
   - Update segment_id/partition_id/channel in file paths
   - Copy to new paths

2. Copy Deltalog files
   - Process delete logs
   - Update paths and copy

3. Copy Statslog files
   - Process statistics files
   - Update paths and copy

4. Copy Index files
   - Copy vector index, scalar index
   - Keep index ID unchanged (because PreserveIndexId=true)

5. Update Segment metadata
   - Create new SegmentInfo with target_segment_id
   - Preserve num_of_rows, storage_version, and other information
   - State set to Importing (will be updated to Flushed later)
```

### Copy Segment Task Management

**CopySegmentJob**:
```protobuf
message CopySegmentJob {
    int64 job_id = 1;
    int64 db_id = 2;
    int64 collection_id = 3;
    string collection_name = 4;
    CopySegmentJobState state = 5;
    string reason = 6;
    repeated CopySegmentIDMapping id_mappings = 7;  // Lightweight ID mapping (~48 bytes per segment)
    uint64 timeout_ts = 8;
    uint64 cleanup_ts = 9;
    string start_time = 10;
    string complete_time = 11;
    repeated common.KeyValuePair options = 12;       // Option configuration (e.g. copy_index)
    int64 total_segments = 13;
    int64 copied_segments = 14;
    int64 total_rows = 15;
    string snapshot_name = 16;                       // For restore snapshot scenario
}
```

**CopySegmentTask**:
```protobuf
message CopySegmentTask {
    int64 task_id = 1;
    int64 job_id = 2;
    int64 collection_id = 3;
    int64 node_id = 4;                              // DataNode ID executing the task
    int64 task_version = 5;
    int64 task_slot = 6;
    ImportTaskStateV2 state = 7;
    string reason = 8;
    repeated CopySegmentIDMapping id_mappings = 9;  // Segments this task is responsible for
    string created_time = 10;
    string complete_time = 11;
}
```

**Task Scheduling**:
- CopySegmentInspector: Assigns pending tasks to DataNodes
- CopySegmentChecker: Monitors job and task status, handles failures
- Supports task-level retry (max 3 retries)
- Supports parallel execution of multiple tasks

### Task Retry and Failure Cleanup Mechanisms

#### Retry Mechanism

**DataCoord Layer Automatic Retry**:
- **Query Failure Retry**: When querying task status on DataNode fails, automatically reset task to `Pending` state and wait for rescheduling
- **DataNode Return Retry**: When DataNode explicitly returns `Retry` status, reset task to `Pending` state

**Retry Configuration**:
- `max_retries`: Default 3 times
- `retry_count`: Records current retry count

**Retry Flow**:
1. When task encounters error, status is reset to `Pending`
2. `CopySegmentInspector` periodically scans tasks in `Pending` state
3. Re-enqueue task to scheduler for scheduling
4. Implements implicit retry: through state transition rather than explicit retry counting

**DataNode Layer Failure Strategy**:
- Adopts fail-fast strategy, does not implement local retry
- Any file copy error immediately reports `Failed` status
- DataCoord layer makes unified retry decision

#### Failure Cleanup Mechanism

**1. Task-level Cleanup (Inspector)**:
- When task fails, `CopySegmentInspector` automatically iterates through all ID mappings of the task
- Finds corresponding target segments in meta and marks status as `Dropped`
- Prevents invalid segment data from remaining in meta

**2. Job-level Cleanup (Checker)**:
- When job fails, `CopySegmentChecker` finds all tasks in `Pending` and `InProgress` states for that job
- Cascades update of all related tasks to `Failed` status and synchronizes failure reason
- Ensures Job and Task state consistency

**3. Timeout Mechanism**:
- `CopySegmentChecker` periodically checks job's `timeout_ts`
- When current time exceeds timeout, automatically marks job as `Failed`
- Prevents job from being stuck for long periods, triggers subsequent cleanup process

**4. GC Mechanism**:
- `CopySegmentChecker` regularly checks jobs in `Completed` or `Failed` state
- When `cleanup_ts` time is reached, sequentially deletes tasks and job metadata
- Cleanup order: Remove all tasks first → then remove job

**GC Protection Strategy**:
- Only cleans up jobs in `Completed` or `Failed` state
- GC is executed only after reaching `CleanupTs` (based on `ImportTaskRetention` configuration)
- **Protection Mechanisms**:
  - If target segments of failed job still exist in meta → Delay cleanup (requires manual intervention)
  - If task is still running on DataNode → Delay cleanup (wait for node release)
- **Cleanup Order**: Delete tasks first → then delete job

#### Error Recovery Summary

| Mechanism | Trigger Condition | Action | Purpose |
|------|---------|---------|------|
| **Auto Retry** | Query failure, DataNode returns Retry | Reset to Pending, reschedule | Automatically recover temporary errors |
| **Task Cleanup** | Task marked as Failed | Delete target segments | Clean up invalid meta data |
| **Job Cleanup** | Job marked as Failed | Cascade mark all tasks | Ensure state consistency |
| **Timeout Protection** | Exceeds TimeoutTs | Mark job as Failed | Prevent long-term resource occupation |
| **GC Collection** | Reaches CleanupTs | Delete job/task metadata | Prevent meta bloat |

### Restore State Tracking

**State Machine**:
```
RestoreSnapshotPending
  ↓
RestoreSnapshotExecuting (copying segments)
  ↓
RestoreSnapshotCompleted / RestoreSnapshotFailed
```

**Progress Calculation**:
```
Progress = (copied_segments / total_segments) * 100
```

**Job Monitoring APIs**:
- GetRestoreSnapshotState(job_id): Query single restore job status
- ListRestoreSnapshotJobs(): List all restore jobs

### Performance Characteristics

**Advantages**:
1. **Fast Recovery**: Direct file copying avoids data rewriting and index rebuilding
2. **Parallel Execution**: Multiple segments copied in parallel, fully utilizing I/O bandwidth
3. **Incremental Progress**: Supports real-time progress queries, good user experience
4. **Resume from Breakpoint**: Task-level retry mechanism, partial failure does not affect overall progress

**Comparison with Bulk Insert**:
- Bulk Insert: Requires data rewriting and index building, time-consuming
- Copy Segment: Direct file copying, 10-100x faster (depending on data volume)

### Error Handling

**1. Collection Creation Failure**:
- Automatic rollback, delete created collection

**2. Copy Task Failure**:
- Automatic retry mechanism: Temporary errors automatically reset to Pending state, rescheduled
- Failure cleanup mechanism: Automatically clean up target segments of failed task (marked as Dropped)
- Detailed mechanism see [Task Retry and Failure Cleanup Mechanisms](#task-retry-and-failure-cleanup-mechanisms)

**3. Job Timeout and GC**:
- Timeout protection: Automatically mark job as Failed when exceeding timeout_ts
- GC collection: Automatically clean up job/task metadata after reaching cleanup_ts
- Protection strategy: Delay GC before segments of failed job are cleaned up, prevent data loss

### Limitations and Considerations

**1. Channel/Partition Count Must Match**:
- New collection's shard count must match snapshot
- Partition count must match (auto-created partitions + user-created partitions)

**2. Field ID and Index ID Preservation**:
- Ensure compatibility through PreserveFieldId and PreserveIndexId
- Must be correctly set during CreateCollection

**3. TTL Handling**:
- Current implementation does not handle collection TTL
- Restored historical data may conflict with TTL mechanism
- Recommend disabling TTL or adjusting TTL time during snapshot restore

## Data Access Patterns

### Read on Milvus

Reading snapshot data through Milvus service:

1. Restore data from snapshot to new collection using RestoreSnapshot feature
2. Execute all Milvus-supported query operations on restored collection
3. New collection is no different from regular collection, supports CRUD operations

### Read without Milvus

Offline access through snapshot data on S3:

**Core Design Goals**:
- Snapshot data on S3 achieves self-describing state
- Third-party tools can directly read snapshot data
- Independent of Milvus service operation

**Technical Implementation**:
- Uses Iceberg-like manifest format
- Metadata.json contains complete schema definition
- Manifest files contain all data file paths
- Third-party tools can parse Avro format manifests to obtain data locations

## API Reference

### CreateSnapshot

**Function**: Create snapshot for specified collection, automatically export data to object storage

**Request**:
```protobuf
message CreateSnapshotRequest {
  common.MsgBase base = 1;
  string db_name = 2;               // database name
  string collection_name = 3;       // collection name
  string name = 4;                  // user-defined snapshot name (unique)
  string description = 5;           // user-defined snapshot description
}
```

**Response**:
```protobuf
message CreateSnapshotResponse {
  common.Status status = 1;
}
```

**Implementation Details**:
1. Obtain minimum timestamp from current channels as snapshotTs
2. Filter sealed segments where StartPosition < snapshotTs (exclude dropped/importing segments)
3. Collect collection schema, index definitions, segment detailed information
4. Write complete data to S3 in manifest format
5. Save SnapshotInfo in Etcd and establish reference relationships

**Notes**:
- Does not actively flush data, only collects existing sealed segments
- **Best Practice (Strongly Recommended)**: Call Flush API before creating snapshot to ensure latest data is persisted. Flush is not mandatory but highly recommended
- Creation will fail if collection has no sealed segments

### DropSnapshot

**Function**: Delete specified snapshot and all its files on S3

**Request**:
```protobuf
message DropSnapshotRequest {
  common.MsgBase base = 1;
  string name = 2;                  // snapshot name to drop
}
```

**Response**:
```protobuf
message DropSnapshotResponse {
  common.Status status = 1;
}
```

**Implementation Details**:
1. Delete SnapshotInfo from Etcd
2. Delete metadata file and all manifest files referenced in ManifestList from S3
3. Remove reference relationships from memory

### ListSnapshots

**Function**: List all snapshots for specified collection

**Request**:
```protobuf
message ListSnapshotsRequest {
  common.MsgBase base = 1;
  string db_name = 2;               // database name
  string collection_name = 3;       // collection name
}
```

**Response**:
```protobuf
message ListSnapshotsResponse {
  common.Status status = 1;
  repeated string snapshots = 2;    // list of snapshot names
}
```

### DescribeSnapshot

**Function**: Get detailed information about snapshot

**Request**:
```protobuf
message DescribeSnapshotRequest {
  common.MsgBase base = 1;
  string name = 2;                  // snapshot name
}
```

**Response**:
```protobuf
message DescribeSnapshotResponse {
  common.Status status = 1;
  string name = 2;
  string description = 3;
  int64 create_ts = 4;
  string collection_name = 5;
  repeated string partition_names = 6;
}
```

**Return Information**:
- Snapshot basic information (name, description, create_ts)
- Collection and partition names
- S3 storage location

### RestoreSnapshot

**Function**: Restore data from snapshot to new collection

**Request** (milvuspb - User-facing API):
```protobuf
message RestoreSnapshotRequest {
  common.MsgBase base = 1;
  string name = 2;                  // snapshot name to restore
  string db_name = 3;               // target database name
  string collection_name = 4;       // target collection name (must not exist)
}
```

**Response**:
```protobuf
message RestoreSnapshotResponse {
  common.Status status = 1;
  int64 job_id = 2;                 // restore job ID for progress tracking
}
```

**Internal API** (datapb - DataCoord API):
```protobuf
// RestoreSnapshotDataAndIndex - Called by RootCoord after collection/partition creation
message RestoreSnapshotRequest {
  common.MsgBase base = 1;
  string name = 2;                  // snapshot name
  int64 collection_id = 3;          // target collection ID (already created by RootCoord)
  bool create_indexes = 4;          // whether to create indexes during restore
}
```

**Implementation Flow**:
1. **Proxy**: Validates request and delegates to RootCoord
2. **RootCoord** (orchestration):
   - Get snapshot info from DataCoord (schema, partitions, indexes)
   - Create new collection with PreserveFieldId=true
   - Create user partitions
   - Call DataCoord.RestoreSnapshotDataAndIndex()
   - Handle rollback on failure
3. **DataCoord** (data restoration):
   - Create indexes with PreserveIndexId=true (if requested)
   - Create CopySegmentJob for data recovery
4. Return job_id for progress tracking

**Limitations**:
- Target collection must not exist
- New collection's shard/partition count will match snapshot
- Does not automatically handle TTL-related issues

### GetRestoreSnapshotState

**Function**: Query restore job execution status

**Request**:
```protobuf
message GetRestoreSnapshotStateRequest {
  common.MsgBase base = 1;
  int64 job_id = 2;                 // restore job ID
}
```

**Response**:
```protobuf
message GetRestoreSnapshotStateResponse {
  common.Status status = 1;
  RestoreSnapshotInfo info = 2;
}

message RestoreSnapshotInfo {
  int64 job_id = 1;
  string snapshot_name = 2;
  string db_name = 3;
  string collection_name = 4;
  RestoreSnapshotState state = 5;   // Pending/Executing/Completed/Failed
  int32 progress = 6;               // 0-100
  string reason = 7;                // error reason if failed
  uint64 time_cost = 8;             // milliseconds
}
```

**State Values**:
- RestoreSnapshotPending: Waiting for execution
- RestoreSnapshotExecuting: Currently copying segments
- RestoreSnapshotCompleted: Restore successful
- RestoreSnapshotFailed: Restore failed

### ListRestoreSnapshotJobs

**Function**: List all restore jobs (optionally filter by collection)

**Request**:
```protobuf
message ListRestoreSnapshotJobsRequest {
  common.MsgBase base = 1;
  string collection_name = 2;       // optional: filter by collection
}
```

**Response**:
```protobuf
message ListRestoreSnapshotJobsResponse {
  common.Status status = 1;
  repeated RestoreSnapshotInfo jobs = 2;
}
```
