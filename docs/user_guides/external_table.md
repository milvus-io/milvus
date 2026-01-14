# Milvus External Table User Guide

## 1. Overview

### 1.1 What is External Table

External Table (External Collection) is a special type of data collection in Milvus that allows users to directly access data stored in external storage systems (such as S3, HDFS, etc.) without copying the data into Milvus local storage.

This enables Milvus to serve as a query layer over existing data lakes while maintaining compatibility with standard Milvus query interfaces.

### 1.2 Core Benefits

- **Zero Data Copy**: Query data directly from external storage without ETL process
- **Unified Query Interface**: Use standard Search/Query APIs to query external data
- **Vector Index Support**: Build vector indexes on external data for efficient similarity search
- **Data Lake Integration**: Seamlessly integrate with existing data lake infrastructure

### 1.3 Use Cases

- Large amounts of vector data already stored in S3 or other object storage
- Need to perform vector search on data lake data
- Want to maintain separation between data storage and query engine
- Need to query periodically updated external data

---

## 2. Quick Start

### 2.1 Create External Collection

External Collection is created through the standard `CreateCollection` API by setting `external_source` in the schema.

#### Python SDK Example

```python
from pymilvus import MilvusClient, DataType

client = MilvusClient("http://localhost:19530")

# Define Schema
schema = client.create_schema()

# Add fields - must specify external_field to map to column names in external data source
schema.add_field(
    field_name="text",
    datatype=DataType.VARCHAR,
    max_length=256,
    external_field="source_text_column"  # Maps to column name in external Parquet file
)

schema.add_field(
    field_name="vector",
    datatype=DataType.FLOAT_VECTOR,
    dim=128,
    external_field="embedding_column"    # Maps to column name in external Parquet file
)

# Set external data source
schema.external_source = "s3://my-bucket/path/to/data"
schema.external_spec = '{"format": "parquet"}'

# Create Collection
client.create_collection(
    collection_name="my_external_collection",
    schema=schema
)
```

#### Go SDK Example

```go
package main

import (
    "context"
    "log"

    "github.com/milvus-io/milvus/client/v2"
    "github.com/milvus-io/milvus/client/v2/entity"
)

func main() {
    ctx := context.Background()

    // Connect to Milvus
    cli, err := client.New(ctx, &client.ClientConfig{
        Address: "localhost:19530",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close(ctx)

    // Define Schema with external source
    schema := entity.NewSchema().
        WithName("my_external_collection").
        WithExternalSource("s3://my-bucket/path/to/data").
        WithExternalSpec(`{"format": "parquet"}`).
        WithField(entity.NewField().
            WithName("text").
            WithDataType(entity.FieldTypeVarChar).
            WithMaxLength(256).
            WithExternalField("source_text_column")).  // Maps to external column
        WithField(entity.NewField().
            WithName("vector").
            WithDataType(entity.FieldTypeFloatVector).
            WithDim(128).
            WithExternalField("embedding_column"))     // Maps to external column

    // Create Collection
    err = cli.CreateCollection(ctx, client.NewCreateCollectionOption(
        "my_external_collection",
        schema,
    ))
    if err != nil {
        log.Fatal(err)
    }
}
```

### 2.2 Field Mapping Rules

| Schema Parameter | Description | Example |
|-----------------|-------------|---------|
| `external_source` | External data source path | `s3://bucket/path` |
| `external_spec` | Data source configuration (JSON format) | `{"format": "parquet"}` |
| `external_field` | Maps field to external column name | Must be specified for each field |

**Note**: All user-defined fields must set `external_field` to map to column names in the external data source.


---

## 3. Supported Operations

### 3.1 Load Collection

```python
# Load External Collection into memory
client.load_collection("my_external_collection")
```

### 3.2 Vector Search

```python
# Execute vector search
results = client.search(
    collection_name="my_external_collection",
    data=[[0.1, 0.2, ...]],  # Query vector
    anns_field="vector",
    limit=10,
    output_fields=["text"]
)
```

### 3.3 Scalar Query

```python
# Execute scalar query
results = client.query(
    collection_name="my_external_collection",
    filter="text like 'hello%'",
    output_fields=["text", "vector"],
    limit=10
)
```

### 3.4 Create Index

```python
# Create index on vector field
index_params = client.prepare_index_params()
index_params.add_index(
    field_name="vector",
    index_type="HNSW",
    metric_type="L2",
    params={"M": 16, "efConstruction": 200}
)

client.create_index(
    collection_name="my_external_collection",
    index_params=index_params
)
```

### 3.5 Drop Collection

```python
# Drop External Collection
client.drop_collection("my_external_collection")
```

---

## 4. Unsupported Operations

External Collection is **read-only**. The following operations are not supported:

| Operation | Status | Description |
|-----------|--------|-------------|
| Insert | Not Supported | Data must be modified at external source |
| Delete | Not Supported | Data must be modified at external source |
| Upsert | Not Supported | Data must be modified at external source |
| Import | Not Supported | Data comes directly from external source |
| Flush | Not Supported | No local data cache |
| Add Field | Not Supported | Schema is fixed after creation |
| Alter Field | Not Supported | Schema is fixed after creation |
| Create/Drop Partition | Not Supported | Partitions not supported |
| Manual Compaction | Not Supported | Not needed |

### 4.1 Schema Restrictions

When creating an External Collection, the following features cannot be used:

| Feature | Status | Reason |
|---------|--------|--------|
| Primary Key Field | Not Allowed | System auto-generates virtual PK |
| Dynamic Field | Not Allowed | Schema must be fixed |
| Partition Key | Not Allowed | External data partitioning not supported |
| Clustering Key | Not Allowed | No clustering compaction |
| Auto ID | Not Allowed | Uses virtual PK |
| Text Match | Not Allowed | Requires internal indexing |
| Namespace Field | Not Allowed | External isolation not supported |

---

## 5. Data Updates

External table data refresh is **manually triggered** using the `RefreshExternalTable` API. This design gives you full control over when data synchronization occurs and allows you to track progress.

### 5.1 Refresh APIs

#### 5.1.1 RefreshExternalTable

Triggers a data refresh job for an external collection.

```python
# Basic refresh - re-scan current data source
response = client.refresh_external_table(
    collection_name="my_external_collection"
)
job_id = response.job_id
print(f"Refresh job started: {job_id}")

# Refresh with updated data source path
response = client.refresh_external_table(
    collection_name="my_external_collection",
    external_source="s3://my-bucket/path/to/new_data",
    external_spec='{"format": "parquet"}'
)
```

**Parameters:**
| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `collection_name` | str | Yes | Name of the external collection |
| `external_source` | str | No | New external source path (optional) |
| `external_spec` | str | No | New external spec configuration (optional) |

**Returns:** `job_id` for tracking progress

#### 5.1.2 GetRefreshExternalTableProgress

Gets the current progress and status of a refresh job.

```python
# Get progress of a specific job
progress = client.get_refresh_external_table_progress(job_id="job_123456")

print(f"State: {progress.state}")           # Pending/InProgress/Completed/Failed
print(f"Progress: {progress.progress}%")
print(f"New segments: {progress.new_segments}")
print(f"Dropped segments: {progress.dropped_segments}")
print(f"Kept segments: {progress.kept_segments}")

if progress.state == "Failed":
    print(f"Error: {progress.reason}")
```

**Progress States:**
| State | Description |
|-------|-------------|
| `Pending` | Job is queued, waiting to execute |
| `InProgress` | Job is currently executing |
| `Completed` | Job completed successfully |
| `Failed` | Job failed with error |

#### 5.1.3 ListRefreshExternalTableJobs

Lists all refresh jobs for a collection.

```python
# List all jobs for a specific collection
jobs = client.list_refresh_external_table_jobs(
    collection_name="my_external_collection",
    limit=10
)

for job in jobs:
    print(f"Job: {job.job_id}")
    print(f"  State: {job.state}")
    print(f"  Progress: {job.progress}%")
    print(f"  Started: {job.start_time}")
    print(f"  Source: {job.external_source}")

# List all external table refresh jobs across all collections
all_jobs = client.list_refresh_external_table_jobs()
```

### 5.2 Complete Refresh Workflow

```python
from pymilvus import MilvusClient
import time

client = MilvusClient("http://localhost:19530")

# Step 1: Trigger refresh
response = client.refresh_external_table(
    collection_name="my_external_collection"
)
job_id = response.job_id
print(f"Refresh job started: {job_id}")

# Step 2: Poll for completion
while True:
    progress = client.get_refresh_external_table_progress(job_id=job_id)

    print(f"Progress: {progress.progress}% ({progress.state})")

    if progress.state == "Completed":
        print("Refresh completed successfully!")
        print(f"  New segments: {progress.new_segments}")
        print(f"  Dropped segments: {progress.dropped_segments}")
        print(f"  Kept segments: {progress.kept_segments}")
        break
    elif progress.state == "Failed":
        print(f"Refresh failed: {progress.reason}")
        break

    time.sleep(5)  # Poll every 5 seconds

# Step 3: Re-load collection to query refreshed data
client.load_collection("my_external_collection")
```

### 5.3 Incremental Update Strategy

The system uses segment-level incremental update strategy:

1. **Keep**: Segments whose external fragments are unchanged remain intact
2. **Drop**: Segments whose corresponding external fragments are deleted/modified are removed
3. **Add**: New external fragments are organized into new segments

This strategy minimizes data reloading during updates.

**Note**: Current version does not support automatic detection of external data source changes. Users must manually trigger refresh using `refresh_external_table`.

---

## 6. Supported Data Formats

| Format | Status | Description |
|--------|--------|-------------|
| Parquet | Supported | Apache Parquet format |

---

## 7. Storage Configuration

### 7.1 S3 Configuration Example

```python
schema.external_source = "s3://my-bucket/vector-data/"
schema.external_spec = '''
{
    "format": "parquet"
}
'''
```

External Collection reuses storage configuration from Milvus configuration file (`minio.*` or `s3.*` configuration items).

---

## 8. Important Notes

1. **Immutable Schema**: Schema cannot be modified after creation. Plan carefully before creation.
2. **Read-Only Mode**: All data modifications must be done at the external data source.
3. **Manual Refresh**: External data changes require manual trigger using `refresh_external_table` API. Use `get_refresh_external_table_progress` to track progress.
4. **Field Mapping**: Each field must correctly map to column names in the external data source.
5. **Data Type Matching**: Ensure Milvus field types are compatible with external data column types.
6. **Re-load After Refresh**: After refresh job completes, call `load_collection` to make the updated data available for queries.

---

## 9. Complete Example

### 9.1 Python SDK Complete Example

```python
from pymilvus import MilvusClient, DataType

# Connect to Milvus
client = MilvusClient("http://localhost:19530")

# Create Schema
schema = client.create_schema()

# Add text field
schema.add_field(
    field_name="title",
    datatype=DataType.VARCHAR,
    max_length=512,
    external_field="doc_title"
)

# Add vector field
schema.add_field(
    field_name="embedding",
    datatype=DataType.FLOAT_VECTOR,
    dim=768,
    external_field="text_embedding"
)

# Configure external data source
schema.external_source = "s3://my-data-lake/documents/"
schema.external_spec = '{"format": "parquet"}'

# Create External Collection
client.create_collection(
    collection_name="document_search",
    schema=schema
)

# Create vector index
index_params = client.prepare_index_params()
index_params.add_index(
    field_name="embedding",
    index_type="HNSW",
    metric_type="COSINE",
    params={"M": 32, "efConstruction": 256}
)
client.create_index("document_search", index_params)

# ============================================
# Refresh data when external source changes
# ============================================
import time

# Step 1: Trigger refresh job
response = client.refresh_external_table(
    collection_name="document_search",
    external_source="s3://my-data-lake/documents/v1",
    external_spec='{"format": "parquet"}',
)
job_id = response.job_id
print(f"Refresh job started: {job_id}")

# Step 2: Poll for completion
while True:
    progress = client.get_refresh_external_table_progress(job_id=job_id)
    print(f"Progress: {progress.progress}% ({progress.state})")

    if progress.state == "Completed":
        print("Refresh completed!")
        break
    elif progress.state == "Failed":
        print(f"Refresh failed: {progress.reason}")
        break

    time.sleep(5)

# Step 3: Re-load collection to query refreshed data
client.load_collection("document_search")

# Now search will use the refreshed data
results = client.search(
    collection_name="document_search",
    data=[query_embedding],
    anns_field="embedding",
    limit=10,
    output_fields=["title"]
)

# ============================================
# List all refresh jobs for this collection
# ============================================
jobs = client.list_refresh_external_table_jobs(
    collection_name="document_search"
)
for job in jobs:
    print(f"Job {job.job_id}: {job.state} ({job.progress}%)")
```

### 9.2 Go SDK Complete Example

```go
package main

import (
    "context"
    "fmt"
    "log"

    "github.com/milvus-io/milvus/client/v2"
    "github.com/milvus-io/milvus/client/v2/entity"
    "github.com/milvus-io/milvus/client/v2/index"
)

func main() {
    ctx := context.Background()

    // Connect to Milvus
    cli, err := client.New(ctx, &client.ClientConfig{
        Address: "localhost:19530",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer cli.Close(ctx)

    collectionName := "document_search"

    // ============================================
    // Create External Collection
    // ============================================

    // Define Schema with external source
    schema := entity.NewSchema().
        WithName(collectionName).
        WithExternalSource("s3://my-data-lake/documents/").
        WithExternalSpec(`{"format": "parquet"}`).
        WithField(entity.NewField().
            WithName("title").
            WithDataType(entity.FieldTypeVarChar).
            WithMaxLength(512).
            WithExternalField("doc_title")).
        WithField(entity.NewField().
            WithName("embedding").
            WithDataType(entity.FieldTypeFloatVector).
            WithDim(768).
            WithExternalField("text_embedding"))

    // Create Collection
    err = cli.CreateCollection(ctx, client.NewCreateCollectionOption(collectionName, schema))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("External collection created successfully")

    // ============================================
    // Create Vector Index
    // ============================================

    indexTask, err := cli.CreateIndex(ctx, client.NewCreateIndexOption(
        collectionName,
        "embedding",
        index.NewHNSWIndex(entity.COSINE, 32, 256),
    ))
    if err != nil {
        log.Fatal(err)
    }
    err = indexTask.Await(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Index created successfully")

    // ============================================
    // Load Collection
    // ============================================

    loadTask, err := cli.LoadCollection(ctx, client.NewLoadCollectionOption(collectionName))
    if err != nil {
        log.Fatal(err)
    }
    err = loadTask.Await(ctx)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Collection loaded successfully")

    // ============================================
    // Search
    // ============================================

    // Query embedding (replace with actual query vector)
    queryEmbedding := make([]float32, 768)
    for i := range queryEmbedding {
        queryEmbedding[i] = 0.1
    }

    results, err := cli.Search(ctx, client.NewSearchOption(
        collectionName,
        10, // limit
        []entity.Vector{entity.FloatVector(queryEmbedding)},
    ).WithANNSField("embedding").WithOutputFields("title"))
    if err != nil {
        log.Fatal(err)
    }

    for _, result := range results {
        for i := 0; i < result.ResultCount; i++ {
            title, _ := result.Fields.GetColumn("title").Get(i)
            fmt.Printf("Result %d: title=%v, score=%f\n", i, title, result.Scores[i])
        }
    }

    // ============================================
    // Drop Collection (cleanup)
    // ============================================

    err = cli.DropCollection(ctx, client.NewDropCollectionOption(collectionName))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Println("Collection dropped successfully")
}
```

---

## 10. Future Plans (Roadmap)

The following features are planned for future releases:

### 10.1 Scalar Index Support

Support creating scalar indexes on external collections to accelerate filtering queries:

```python
# Future: Create scalar index on external collection
index_params.add_index(
    field_name="category",
    index_type="INVERTED"
)
```

### 10.2 Function Support

Support embedding functions and other built-in transformation functions for external collections:

```python
# Future: Use embedding function with external collection
schema.add_function(
    name="text_to_vector",
    function_type=FunctionType.EMBEDDING,
    input_field="text",
    output_field="vector",
    params={"model": "text-embedding-3-small"}
)
```

### 10.3 Schema Evolution (Add/Drop Fields)

Support adding or removing fields from external collections after creation:

```python
# Future: Add new field to external collection
client.add_field(
    collection_name="my_external_collection",
    field_name="new_column",
    datatype=DataType.VARCHAR,
    max_length=128,
    external_field="source_new_column"
)

# Future: Drop field from external collection
client.drop_field(
    collection_name="my_external_collection",
    field_name="old_column"
)
```

### 10.4 Additional Planned Features

| Feature | Description | Priority |
|---------|-------------|----------|
| **More Data Formats** | Support Apache Iceberg, Delta Lake, ORC formats | High |
| **Auto Data Sync** | Automatic detection of external data source changes with scheduled refresh | Low |
| **Partition Mapping** | Map external data partitions to Milvus partitions | Medium |
| **Text Match** | Support full-text search on external collections | Medium |
| **Cross-source Query** | Query across multiple external data sources | Low |
| **Change Data Capture** | Support CDC-based incremental updates | Low |

---

## 11. Related Documentation

- [Design Document: External Table](../design_docs/20260105-external_table.md)
