# Milvus Snapshot User Guide

## Overview

Milvus snapshot feature allows users to create point-in-time copies of collections. This powerful capability enables data backup, versioning, and restoration scenarios. Snapshots capture the complete state of data including vector data, metadata, indexes, and schema information at a specific timestamp.

## Key Features

- Point-in-time consistency: Snapshots capture data at a specific timestamp, ensuring data consistency
- Metadata preservation: Snapshots include schema, indexes, and collection properties
- Efficient storage: Snapshots use a manifest-based approach for efficient storage in object storage (S3)
- Restore capability: Restore snapshots to new collections with data integrity

## Core Concepts

### Snapshot Components

A Milvus snapshot consists of:
1. Snapshot Metadata: Basic information including name, description, collection ID, and creation timestamp
2. Collection Description: Schema definition, partition information, and collection properties
3. Segment Data: Vector data files (binlogs), deletion logs (deltalogs), and index files
4. Index Information: Index metadata and file paths

### Storage Structure

Snapshots are stored in object storage with the following structure:

```
snapshots/{collection_id}/
├── metadata/
│   ├── 00001-{uuid}.json          # Snapshot metadata (JSON format)
│   ├── 00002-{uuid}.json          # Additional snapshot versions
│   └── ...
└── manifests/
    ├── data-file-manifest-{uuid}.avro         # Segment details (Avro format)
    └── ...
```

**Note**: The metadata JSON file directly contains an array of manifest file paths, eliminating the need for a separate manifest list file.

## API Reference

### Create Snapshot

Create a snapshot for a collection.

**Best Practice (Strongly Recommended)**: Call `flush()` before creating a snapshot to ensure all data is persisted. The `create_snapshot` operation only captures existing sealed segments and does not trigger data flushing automatically. Data in growing segments will not be included in the snapshot.

**Note**: Calling flush is not mandatory, but highly recommended to avoid data loss. If you skip flush, only data that has already been flushed to sealed segments will be included in the snapshot.

**Python SDK Example:**
```python
from pymilvus import MilvusClient

client = MilvusClient(uri="http://localhost:19530")

# Recommended: Flush data before creating snapshot to ensure all data is included
client.flush(collection_name="my_collection")

# Create snapshot for entire collection
client.create_snapshot(
    collection_name="my_collection",
    snapshot_name="backup_20240101",
    description="Daily backup for January 1st, 2024"
)
```

**Go SDK Example:**
```go
import (
    "context"
    "github.com/milvus-io/milvus/client/v2/milvusclient"
)

client, err := milvusclient.New(context.Background(), &milvusclient.ClientConfig{
    Address: "localhost:19530",
})

// Recommended: Flush data before creating snapshot to ensure all data is included
err = client.Flush(context.Background(), milvusclient.NewFlushOption("my_collection"))
if err != nil {
    log.Fatal(err)
}

// Create snapshot
createOpt := milvusclient.NewCreateSnapshotOption("backup_20240101", "my_collection").
    WithDescription("Daily backup for January 1st, 2024")

err = client.CreateSnapshot(context.Background(), createOpt)
```

Parameters:
- snapshot_name (string): User-defined unique name for the snapshot
- collection_name (string): Name of the collection to snapshot
- description (string, optional): Description of the snapshot

### List Snapshots

List existing snapshots for collections.

**Python SDK Example:**
```python
# List all snapshots for a collection
snapshots = client.list_snapshots(collection_name="my_collection")
```

**Go SDK Example:**
```go
// List snapshots for collection
listOpt := milvusclient.NewListSnapshotsOption().
    WithCollectionName("my_collection")

snapshots, err := client.ListSnapshots(context.Background(), listOpt)
```

Parameters:
- collection_name (string, optional): Filter snapshots by collection

Returns:
- List of snapshot names

### Describe Snapshot

Get detailed information about a specific snapshot.

**Python SDK Example:**
```python
snapshot_info = client.describe_snapshot(
    snapshot_name="backup_20240101",
    include_collection_info=True
)

print(f"Snapshot ID: {snapshot_info.id}")
print(f"Collection: {snapshot_info.collection_name}")
print(f"Created: {snapshot_info.create_ts}")
print(f"Description: {snapshot_info.description}")
```

**Go SDK Example:**
```go
describeOpt := milvusclient.NewDescribeSnapshotOption("backup_20240101")
resp, err := client.DescribeSnapshot(context.Background(), describeOpt)

fmt.Printf("Snapshot ID: %d\n", resp.GetSnapshotInfo().GetId())
fmt.Printf("Collection: %s\n", resp.GetSnapshotInfo().GetCollectionName())
```

Parameters:
- snapshot_name (string): Name of the snapshot to describe
- include_collection_info (bool, optional): Whether to include collection schema and index information

Returns:
- SnapshotInfo: Basic snapshot information
- CollectionDescription: Collection schema and properties (if requested)
- IndexInfo[]: Index information (if requested)

### Restore Snapshot

Restore a snapshot to a new collection. This operation is asynchronous and returns a job ID for tracking the restore progress.

**Restore Mechanism**: Snapshot restore uses a **Copy Segment** mechanism instead of traditional bulk insert. This approach:
- Directly copies segment files (binlogs, deltalogs, index files) from snapshot storage
- Preserves Field IDs and Index IDs to ensure compatibility with existing data files
- Avoids data rewriting and index rebuilding, resulting in significantly faster restore times
- Typically 10-100x faster than traditional backup/restore methods

**Python SDK Example:**
```python
# Restore snapshot to new collection
job_id = client.restore_snapshot(
    snapshot_name="backup_20240101",
    collection_name="restored_collection",
)

# Wait for restore to complete
import time
while True:
    state = client.get_restore_snapshot_state(job_id=job_id)
    if state.state == "RestoreSnapshotCompleted":
        print(f"Restore completed in {state.time_cost}ms")
        break
    elif state.state == "RestoreSnapshotFailed":
        print(f"Restore failed: {state.reason}")
        break
    print(f"Restore progress: {state.progress}%")
    time.sleep(1)
```

**Go SDK Example:**
```go
restoreOpt := milvusclient.NewRestoreSnapshotOption("backup_20240101", "restored_collection")

jobID, err := client.RestoreSnapshot(context.Background(), restoreOpt)
if err != nil {
    log.Fatal(err)
}

// Poll for restore completion
for {
    state, err := client.GetRestoreSnapshotState(context.Background(), 
        milvusclient.NewGetRestoreSnapshotStateOption(jobID))
    if err != nil {
        log.Fatal(err)
    }
    
    if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotCompleted {
        log.Printf("Restore completed in %dms", state.GetTimeCost())
        break
    }
    
    if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotFailed {
        log.Fatalf("Restore failed: %s", state.GetReason())
    }
    
    log.Printf("Restore progress: %d%%", state.Progress)
    time.Sleep(time.Second)
}
```

Parameters:
- snapshot_name (string): Name of the snapshot to restore
- collection_name (string): Name of the target collection to create

Returns:
- job_id (int64): Restore job ID for tracking progress

### Drop Snapshot

Delete a snapshot permanently.

**Python SDK Example:**
```python
client.drop_snapshot(snapshot_name="backup_20240101")
```

**Go SDK Example:**
```go
dropOpt := milvusclient.NewDropSnapshotOption("backup_20240101")
err := client.DropSnapshot(context.Background(), dropOpt)
```

Parameters:
- snapshot_name (string): Name of the snapshot to drop

### Get Restore Snapshot State

Query the status and progress of a restore snapshot job.

**Python SDK Example:**
```python
state = client.get_restore_snapshot_state(job_id=12345)

print(f"Job ID: {state.job_id}")
print(f"Snapshot Name: {state.snapshot_name}")
print(f"Collection ID: {state.collection_id}")
print(f"State: {state.state}")
print(f"Progress: {state.progress}%")
if state.state == "RestoreSnapshotFailed":
    print(f"Failure Reason: {state.reason}")
print(f"Time Cost: {state.time_cost}ms")
```

**Go SDK Example:**
```go
stateOpt := milvusclient.NewGetRestoreSnapshotStateOption(12345)
state, err := client.GetRestoreSnapshotState(context.Background(), stateOpt)
if err != nil {
    log.Fatal(err)
}

fmt.Printf("Job ID: %d\n", state.GetJobId())
fmt.Printf("Snapshot Name: %s\n", state.GetSnapshotName())
fmt.Printf("Collection ID: %d\n", state.GetCollectionId())
fmt.Printf("State: %s\n", state.GetState())
fmt.Printf("Progress: %d%%\n", state.GetProgress())
if state.GetState() == milvuspb.RestoreSnapshotState_RestoreSnapshotFailed {
    fmt.Printf("Failure Reason: %s\n", state.GetReason())
}
fmt.Printf("Time Cost: %dms\n", state.GetTimeCost())
```

Parameters:
- job_id (int64): The restore job ID returned from RestoreSnapshot

Returns:
- RestoreSnapshotInfo with the following fields:
  - job_id (int64): Restore job ID
  - snapshot_name (string): Snapshot name being restored
  - collection_id (int64): Target collection ID
  - state (enum): Current state (Pending, InProgress, Completed, Failed)
  - progress (int32): Progress percentage (0-100)
  - reason (string): Error reason if failed
  - time_cost (uint64): Time cost in milliseconds

### List Restore Snapshot Jobs

List all restore snapshot jobs, optionally filtered by collection name.

**Python SDK Example:**
```python
# List all restore jobs
jobs = client.list_restore_snapshot_jobs()

for job in jobs:
    print(f"Job {job.job_id}: {job.snapshot_name} -> Collection {job.collection_id}")
    print(f"  State: {job.state}, Progress: {job.progress}%")

# List restore jobs for a specific collection
jobs = client.list_restore_snapshot_jobs(collection_name="my_collection")
```

**Go SDK Example:**
```go
// List all restore jobs
listOpt := milvusclient.NewListRestoreSnapshotJobsOption()
jobs, err := client.ListRestoreSnapshotJobs(context.Background(), listOpt)
if err != nil {
    log.Fatal(err)
}

for _, job := range jobs {
    fmt.Printf("Job %d: %s -> Collection %d\n", 
        job.GetJobId(), job.GetSnapshotName(), job.GetCollectionId())
    fmt.Printf("  State: %s, Progress: %d%%\n", 
        job.GetState(), job.GetProgress())
}

// List restore jobs for a specific collection
listOpt = milvusclient.NewListRestoreSnapshotJobsOption().
    WithCollectionName("my_collection")
jobs, err = client.ListRestoreSnapshotJobs(context.Background(), listOpt)
```

Parameters:
- collection_name (string, optional): Filter jobs by target collection name

Returns:
- List of RestoreSnapshotInfo objects for all matching restore jobs

## Use Cases

### 1. Data Backup and Recovery

Snapshots provide a lightweight and efficient backup solution compared to traditional tools like milvus-backup.

```python
import datetime

# Create daily backup
today = datetime.date.today().strftime("%Y%m%d")
snapshot_name = f"daily_backup_{today}"

# Recommended: Flush data to ensure all changes are persisted
client.flush(collection_name="production_vectors")

# Create snapshot
client.create_snapshot(
    collection_name="production_vectors",
    snapshot_name=snapshot_name,
    description=f"Daily backup for {today}"
)
```

**Comparison: Snapshot vs. milvus-backup**

| Operation | milvus-backup | Snapshot |
|-----------|---------------|----------|
| **Backup Creation** | Copies all data files | Creates metadata only (milliseconds) |
| **Restore Process** | Imports data and rebuilds indexes | Copies existing data and index files directly |
| **Performance** | Slower, resource-intensive | Fast and lightweight |
| **System Impact** | High I/O and CPU usage | Minimal impact |

**Why Snapshots are More Efficient:**
- **Creation**: Only generates snapshot metadata without copying any data files
- **Restoration**: Directly copies existing data files and index files, no data rewriting or index rebuilding needed
- **Speed**: Backup in milliseconds, restore in seconds to minutes (vs. hours for large collections)

The snapshot capability provides a foundation for significantly improving the milvus-backup tool

### 2. Offline Data Processing with Spark

Snapshots enable efficient offline data processing by providing stable, consistent data sources for analytical workloads. Users can directly access snapshot data stored in object storage (S3) with Spark or other big data processing frameworks without impacting the live Milvus cluster.

**Key Benefits:**
- **Direct Access**: Read snapshot data directly from S3 without going through Milvus query APIs
- **Data Stability**: Snapshot mechanism ensures data remains available and unchanged during long-running batch jobs
- **No Cluster Impact**: Offline processing doesn't affect production Milvus query performance
- **Cost-Effective**: Leverage cheaper compute resources for batch analytics instead of online query nodes

**Use Case Example: Vector Similarity Analysis**

```python
from pyspark.sql import SparkSession
import datetime
import json

# Step 1: Create snapshot for offline processing
snapshot_name = f"analytics_snapshot_{datetime.date.today().strftime('%Y%m%d')}"

# Recommended: Flush data to ensure all changes are persisted
client.flush(collection_name="user_embeddings")
client.create_snapshot(
    collection_name="user_embeddings",
    snapshot_name=snapshot_name,
    description="Snapshot for daily analytics job"
)

# Step 2: Get snapshot metadata to locate data files in S3
snapshot_info = client.describe_snapshot(
    snapshot_name=snapshot_name,
    include_collection_info=True
)

# Step 3: Process snapshot data with Spark
spark = SparkSession.builder \
    .appName("VectorAnalytics") \
    .config("spark.hadoop.fs.s3a.access.key", "YOUR_ACCESS_KEY") \
    .config("spark.hadoop.fs.s3a.secret.key", "YOUR_SECRET_KEY") \
    .getOrCreate()

# Read and parse snapshot metadata to get actual file paths
# Note: This is a simplified example. In practice, you need to:
# 1. Parse the metadata JSON file to get manifest file paths
# 2. Parse manifest Avro files to get binlog/deltalog paths
# 3. Read the actual data files
s3_path = snapshot_info.s3_location

# Example: Read binlog files directly if you know the structure
# In reality, you would parse the manifest files first
df = spark.read.format("your_format").load(f"s3a://{s3_path}/binlogs/")

# Perform analytics operations
# Example: Compute vector statistics, clustering, or quality metrics
result = df.groupBy("partition_id").agg({
    "vector_dim": "count",
    "timestamp": "max"
})

result.write.mode("overwrite").parquet("s3a://analytics-results/daily_stats/")

# Step 4: Clean up snapshot after processing completes
client.drop_snapshot(snapshot_name=snapshot_name)
```

**Common Offline Processing Scenarios:**
- **Vector Quality Analysis**: Analyze embedding distributions and detect anomalies
- **Data Migration**: Transform and migrate data between different Milvus clusters
- **ETL Pipelines**: Extract vectors for training or fine-tuning machine learning models
- **Compliance Auditing**: Generate reports on data usage and access patterns
- **Feature Engineering**: Derive new features from existing vector embeddings for downstream tasks


### 3. Data Versioning

Maintain multiple versions of data for experimentation:

```python
# Create version snapshots before major updates
# Recommended: Flush to ensure all data is captured
client.flush(collection_name="ml_embeddings")

client.create_snapshot(
    collection_name="ml_embeddings",
    snapshot_name="v1.0_baseline",
    description="Baseline model embeddings"
)

# After model update, flush and create new snapshot
client.flush(collection_name="ml_embeddings")

client.create_snapshot(
    collection_name="ml_embeddings",
    snapshot_name="v1.1_improved",
    description="Improved model embeddings"
)
```

### 4. Testing and Development

Create snapshots for testing environments:

```python
# Create test data snapshot
# Recommended: Flush to ensure all test data is captured
client.flush(collection_name="test_collection")

client.create_snapshot(
    collection_name="test_collection",
    snapshot_name="test_dataset_v1",
    description="Test dataset for regression testing"
)

# Restore for testing with progress tracking
job_id = client.restore_snapshot(
    snapshot_name="test_dataset_v1",
    collection_name="test_environment"
)

# Monitor restore progress
import time
while True:
    state = client.get_restore_snapshot_state(job_id=job_id)
    if state.state == "RestoreSnapshotCompleted":
        print(f"Test environment ready! Restored in {state.time_cost}ms")
        break
    elif state.state == "RestoreSnapshotFailed":
        print(f"Restore failed: {state.reason}")
        break
    print(f"Setting up test environment: {state.progress}%")
    time.sleep(1)
```

### 5. Managing Multiple Restore Operations

Track multiple restore jobs simultaneously:

```python
# Start multiple restore operations
job_ids = []
for i in range(3):
    job_id = client.restore_snapshot(
        snapshot_name=f"snapshot_v{i}",
        collection_name=f"test_env_{i}"
    )
    job_ids.append(job_id)

# Monitor all jobs
while job_ids:
    completed = []
    for job_id in job_ids:
        state = client.get_restore_snapshot_state(job_id=job_id)
        if state.state in ["RestoreSnapshotCompleted", "RestoreSnapshotFailed"]:
            completed.append(job_id)
            print(f"Job {job_id} finished: {state.state}")
    
    for job_id in completed:
        job_ids.remove(job_id)
    
    if job_ids:
        time.sleep(1)

# Alternatively, list all restore jobs
jobs = client.list_restore_snapshot_jobs()
for job in jobs:
    print(f"Job {job.job_id}: {job.snapshot_name} - {job.progress}% ({job.state})")
```

## Best Practices

### 1. Naming Conventions

Use consistent and descriptive naming:

```python
# Good naming examples
"daily_backup_20240101"
"v2.1_production_release"
"test_dataset_regression_suite"

# Avoid generic names
"backup1", "test", "snapshot"
```

### 2. Snapshot Management

- Regular cleanup: Remove old snapshots to save storage
- Documentation: Use descriptive descriptions for future reference
- Verification: Always verify snapshot creation and restoration
- Monitoring: Track snapshot creation times and storage usage
- Job tracking: Store restore job IDs for monitoring and troubleshooting

### 3. Storage Considerations

- Snapshots consume storage space proportional to collection size
- Object storage costs apply for snapshot retention
- Consider compression and deduplication at storage layer
- Plan retention policies based on business requirements

### 4. Performance Optimization

- Create snapshots during low-traffic periods
- Avoid creating multiple simultaneous snapshots
- Monitor impact on system resources

### 5. Restore Operation Management

- Asynchronous operations: Restore operations are asynchronous and return immediately with a job ID
- Progress monitoring: Always poll restore job status using the job ID for large collections
- Timeout handling: Implement appropriate timeout and retry logic for restore operations
- Error recovery: Check restore job state and reason field if restoration fails
- Resource planning: Ensure sufficient system resources (memory, disk, CPU) before restoring large snapshots
- Job listing: Use ListRestoreSnapshotJobs to monitor all ongoing restore operations
- Completion verification: After restore completes, verify collection data integrity before using it

### 6. Performance and Storage Considerations

#### Execution Time

**Snapshot Creation:**
- Creation is typically completed in milliseconds
- The operation only generates snapshot metadata and stores it to object storage
- Lightweight operation with minimal performance impact on the system

**Snapshot Restore:**
- Restore time ranges from seconds to minutes depending on data volume
- The operation involves copying data from the snapshot to the target collection
- Key factors affecting restore time:
  - Total data volume in the snapshot (binlogs, deltalogs, index files)
  - Network bandwidth between Milvus cluster and object storage
  - Object storage throughput limits and concurrent I/O operations
- Use `GetRestoreSnapshotState` API to monitor restore progress for large snapshots

#### Storage Impact

**Important Storage Behavior:**
- Data and indexes referenced by snapshots are **NOT automatically garbage collected**
- Even if the original segment is dropped, snapshot-referenced data remains in object storage
- This prevents data loss but increases storage consumption

**GC Protection Mechanism:**
- **Segment-level protection**: The garbage collector checks `GetSnapshotBySegment()` before deleting any segment
  - Segments referenced by any snapshot are skipped during GC
  - Protection applies to both metadata (Etcd) and data files (S3)
- **Index-level protection**: The garbage collector checks `GetSnapshotByIndex()` before deleting index files
  - Index files referenced by snapshots are preserved even after drop index operations
  - Ensures index data availability during snapshot restore
- **Implementation location**: `internal/datacoord/garbage_collector.go`

**Storage Cost Considerations:**
- In extreme cases, a single snapshot can double your object storage costs
- Example: If original collection uses 100GB, and you create a snapshot then compact the collection to generate new segments, you may have up to 200GB in object storage
- Snapshot metadata itself is minimal (typically < 1MB per snapshot)

**Storage Management Best Practices:**
- **Regular cleanup**: Explicitly drop snapshots that are no longer needed to free storage
- **Retention policies**: Define and enforce snapshot lifecycle policies (e.g., keep only last 7 days)
- **Monitor usage**: Track object storage consumption and identify orphaned snapshot data
- **Cost planning**: Factor in potential storage doubling when planning capacity and budgets
- **Snapshot audit**: Periodically review and remove obsolete snapshots to reclaim storage space

## Limitations and Considerations

### Current Limitations

1. **Read-only snapshots**: Snapshots are immutable once created
2. **Cross-cluster restoration**: Snapshots are currently tied to the originating cluster (cross-cluster restore not yet supported)
3. **Schema compatibility**: Restored collections maintain original schema
4. **Resource usage**: Snapshot creation may impact system performance during metadata collection
5. **Channel/Partition matching**:
   - The restored collection's shard count must match the snapshot's channel count
   - Partition count must match (including both auto-created and user-created partitions)
6. **TTL handling**:
   - Current implementation does not automatically handle collection TTL settings
   - Restored historical data may conflict with TTL policies
   - Recommendation: Disable TTL or adjust TTL time before restoring snapshots
7. **Field/Index ID preservation**:
   - Restore process uses `PreserveFieldId=true` and `PreserveIndexId=true`
   - These flags ensure compatibility between snapshot data files and restored collection

### Planning Considerations

1. Storage costs: Factor in long-term storage costs for snapshots
2. Recovery time: Larger snapshots take longer to restore (monitor via job progress)
3. Network bandwidth: Restoration involves data transfer
4. Consistency model: Snapshots reflect point-in-time consistency
5. Asynchronous operations: Restore operations run in background; plan for monitoring and status checking
6. Job management: Keep track of restore job IDs for production environments

## Troubleshooting

### Common Issues

Snapshot creation fails:
- Verify collection exists and is accessible
- Check available storage space
- Ensure proper permissions for object storage
- Verify system resources are sufficient
Restoration fails:
- Confirm snapshot exists and is accessible
- Check target collection name doesn't already exist
- Verify sufficient system resources
- Ensure object storage connectivity
- Query restore job state to get specific failure reason
- Check restore job progress to identify at which stage it failed

Performance issues:
- Monitor system resource usage during operations
- Consider creating snapshots during maintenance windows

### Error Messages

Common error patterns and solutions:

Error: "snapshot not found"
Solution: Verify snapshot name and check if it was deleted

Error: "collection already exists"
Solution: Use a different target collection name for restoration

Error: "insufficient storage"
Solution: Free up storage space or increase limits

Error: "permission denied"
Solution: Check object storage credentials and permissions

Error: "restore job not found"
Solution: Verify job ID is correct; job may have expired or been cleaned up

Error: "restore snapshot failed" (check state.reason for details)
Solution: Query GetRestoreSnapshotState for specific failure reason and address the underlying issue

## Conclusion

Milvus snapshots provide a robust solution for data backup, versioning, and recovery scenarios. By following the best practices and understanding the limitations, users can effectively leverage snapshots to ensure data durability and enable sophisticated data management workflows.

For additional support and advanced configuration options, refer to the Milvus documentation or contact the Milvus community.