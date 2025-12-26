# Entity-level TTL Design

## Background

Currently, Milvus supports **collection-level TTL** for data expiration, but does not support defining an independent expiration time for individual entities (rows). As application scenarios become more diverse, for example:

* Data from different tenants or businesses stored in the same collection but with different lifecycles;
* Hot and cold data mixed together, where short-lived data should be cleaned automatically while long-term data is retained;
* IoT / logging / MLOps data that requires record-level retention policies;

Relying solely on collection-level TTL can no longer satisfy these requirements. If users want to retain only part of the data, they must periodically perform **upsert** operations to refresh the timestamps of those entities. This approach is unintuitive and increases operational and maintenance costs.

Therefore, **Entity-level TTL** becomes a necessary feature.

Related issues:

* [milvus-io/milvus#45917](https://github.com/milvus-io/milvus/issues/45917)
* [milvus-io/milvus#45923](https://github.com/milvus-io/milvus/issues/45923)

---

## Design Principles

* Fully compatible with existing collection-level TTL behavior.
* Allow users to choose whether to enable entity-level TTL.
* User-controllable: support explicit declaration in schema or transparent system management.
* Minimize changes to compaction and query logic; expiration is determined only by the TTL column and write timestamp.
* Support dynamic upgrade for existing collections.

---

## Basic Approach

Milvus already supports the `TIMESTAMPTZ` data type. Entity TTL information will therefore be stored in a field of this type.

---

## Design Details

Entity-level TTL is implemented by allowing users to explicitly add a `TIMESTAMPTZ` column in the schema and mark it in collection properties:

```text
"collection.ttl.field": "ttl"
```

Here, `ttl` is the name of the column that stores TTL information. This mechanism is **mutually exclusive** with collection-level TTL.

---

### Terminology and Conventions

* **TTL column / TTL field** : A field of type `TIMESTAMPTZ` declared in the schema and marked with `is_ttl = true`.
* **ExpireAt** : The value stored in the TTL field, representing the absolute expiration timestamp of an entity (UTC by default if no timezone is specified).
* **Collection-level TTL** : The existing mechanism where retention duration is defined at the collection level (e.g., retain 30 days).
* **insert_ts / mvcc_ts** : Existing Milvus write or MVCC timestamps, used as fallback when needed.
* **expirQuantiles** : A time point corresponding to a certain percentile of expired data within a segment, used to quickly determine whether compaction should be triggered.

Example:

* 20% of data expires at time `t1`
* 40% of data expires at time `t2`

---

### 1. Collection Properties and Constraints

* Only fields with `DataType == TIMESTAMPTZ` can be configured as a TTL field.
* Mutually exclusive with collection-level TTL:
  * If collection-level TTL is enabled, specifying a TTL field is not allowed.
  * Collection-level TTL must be disabled first.
* One TTL field per collection:
  * A collection may contain multiple `TIMESTAMPTZ` fields, but only one can be designated as the TTL field.

---

### 2. Storage Semantics

* Unified convention: the TTL field stores an **absolute expiration time** (`ExpireAt`).
* Duration-based TTL is not supported.
* `NULL` value semantics:
  * A `NULL` TTL value means the entity never expires.

---

### 3. Compatibility Rules

#### Existing Collections

For an existing collection to enable entity-level TTL:

1. Disable collection-level TTL using `AlterCollection`.
2. Add a new `TIMESTAMPTZ` field using `AddField`.
3. Update collection properties via `AlterCollection` to mark the new field as the TTL field.

If historical data should also have expiration times, users must perform an **upsert** operation to backfill the TTL field.

---

### 4. SegmentInfo Extension and Compaction Trigger

#### 4.1 SegmentInfo Metadata Extension

A new field `expirQuantiles` is added to the segment metadata:

```proto
message SegmentInfo {
  int64 ID = 1;
  int64 collectionID = 2;
  int64 partitionID = 3;
  string insert_channel = 4;
  int64 num_of_rows = 5;
  common.SegmentState state = 6;
  int64 max_row_num = 7 [deprecated = true]; // deprecated, we use the binary size to control the segment size but not a estimate rows.
  uint64 last_expire_time = 8;
  msg.MsgPosition start_position = 9;
  msg.MsgPosition dml_position = 10;
  // binlogs consist of insert binlogs
  repeated FieldBinlog binlogs = 11;
  repeated FieldBinlog statslogs = 12;
  // deltalogs consists of delete binlogs. FieldID is not used yet since delete is always applied on primary key
  repeated FieldBinlog deltalogs = 13;
  bool createdByCompaction = 14;
  repeated int64 compactionFrom = 15;
  uint64 dropped_at = 16; // timestamp when segment marked drop
  // A flag indicating if:
  // (1) this segment is created by bulk insert, and
  // (2) the bulk insert task that creates this segment has not yet reached `ImportCompleted` state.
  bool is_importing = 17;
  bool is_fake = 18;

  // denote if this segment is compacted to other segment.
  // For compatibility reasons, this flag of an old compacted segment may still be False.
  // As for new fields added in the message, they will be populated with their respective field types' default values.
  bool compacted = 19;

  // Segment level, indicating compaction segment level
  // Available value: Legacy, L0, L1, L2
  // For legacy level, it represent old segment before segment level introduced
  // so segments with Legacy level shall be treated as L1 segment
  SegmentLevel level = 20;
  int64 storage_version = 21;

  int64 partition_stats_version = 22;
  // use in major compaction, if compaction fail, should revert segment level to last value
  SegmentLevel last_level = 23;
  // use in major compaction, if compaction fail, should revert partition stats version to last value 
  int64 last_partition_stats_version = 24;

  // used to indicate whether the segment is sorted by primary key.
  bool is_sorted = 25;

  // textStatsLogs is used to record tokenization index for fields.
  map<int64, TextIndexStats> textStatsLogs = 26;
  repeated FieldBinlog bm25statslogs = 27;

  // This field is used to indicate that some intermediate state segments should not be loaded.
  // For example, segments that have been clustered but haven't undergone stats yet.
  bool is_invisible = 28;


  // jsonKeyStats is used to record json key index for fields.
  map<int64, JsonKeyStats> jsonKeyStats = 29;
    // This field is used to indicate that the segment is created by streaming service.
  // This field is meaningful only when the segment state is growing.
  // If the segment is created by streaming service, it will be a true.
  // A segment generated by datacoord of old arch, will be false.
  // After the growing segment is full managed by streamingnode, the true value can never be seen at coordinator.
  bool is_created_by_streaming = 30;
  bool is_partition_key_sorted = 31;

  // manifest_path stores the fullpath of LOON manifest file of segemnt data files.
  // we could keep the fullpath since one segment shall only have one active manifest
  // and we could keep the possiblity that manifest stores out side of collection/partition/segment path
  string manifest_path = 32;

  // expirQuantiles records the expiration timestamps of the segment
  // at the 20%, 40%, 60%, 80%, and 100% data distribution levels
  repeated int64 expirQuantiles = 33;
}
```

Meaning:

* `expirQuantiles`: The expiration timestamps corresponding to the 20%, 40%, 60%, 80%, and 100% percentiles of data within the segment.

---

#### 4.2 Metadata Writing

* Statistics are collected  **only during compaction** .
* `expirQuantiles` is computed during sort or mix compaction tasks.
* For streaming segments, sort compaction is required as the first step, making this approach sufficient.

---

#### 4.3 Compaction Trigger Strategy

* Based on a configured expired-data ratio, select the corresponding percentile from `expirQuantiles` (rounded down).
* Compare the selected expiration time with the current time.
* If the expiration condition is met, trigger a compaction task.

Special cases:

* If `expirQuantiles` is `NULL`, the segment is treated as non-expiring.
* For old segments without a TTL field, expiration logic is skipped.
* Subsequent upsert operations will trigger the corresponding L0 compaction.

---

### 5. Query / Search Logic

* Each query is executed with an MVCC timestamp assigned by Milvus.
* When loading a collection, the system records which field is configured as the TTL field.

During query execution, expired data is filtered by comparing the TTL field value with the MVCC timestamp inside `mask_with_timestamps`.

---

## PyMilvus Example

### 1. Create Collection

Specify the TTL field in the schema:

```python
schema = client.create_schema(auto_id=False, description="test entity ttl")
schema.add_field("id", DataType.INT64, is_primary=True)
schema.add_field("ttl", DataType.TIMESTAMPTZ, nullable=True)
schema.add_field("vector", DataType.FLOAT_VECTOR, dim=dim)

prop = {"collection.ttl.field": "ttl"}
client.create_collection(
    collection_name,
    schema=schema,
    enable_dynamic_field=True,
    properties=prop,
)
```

---

### 2. Insert Data

Insert data the same way as a normal `TIMESTAMPTZ` field:

```python
rows = [
    {"id": 0, "vector": [random.random() for _ in range(dim)], "ttl": None},
    {"id": 1, "vector": [random.random() for _ in range(dim)], "ttl": None},
    {"id": 2, "vector": [random.random() for _ in range(dim)], "ttl": None},
    {"id": 3, "vector": [random.random() for _ in range(dim)], "ttl": "2025-12-31T00:00:00Z"},
    {"id": 4, "vector": [random.random() for _ in range(dim)], "ttl": "2025-12-31T01:00:00Z"},
    {"id": 5, "vector": [random.random() for _ in range(dim)], "ttl": "2025-12-31T02:00:00Z"},
    {"id": 6, "vector": [random.random() for _ in range(dim)], "ttl": "2025-12-31T03:00:00Z"},
    {"id": 7, "vector": [random.random() for _ in range(dim)], "ttl": "2025-12-31T04:00:00Z"},
    {"id": 8, "vector": [random.random() for _ in range(dim)], "ttl": "2025-12-31T23:59:59Z"},
]

insert_result = client.insert(collection_name, rows)
client.flush(collection_name)
```

---

### 3. Index and Load

Index creation and loading are unaffected. Indexes can still be built on the TTL field if needed.

```python-repl
index_params = client.prepare_index_params()
index_params.add_index(
    field_name="vector",
    index_type="IVF_FLAT",
    index_name="vector",
    metric_type="L2",
    params={"nlist": 128},
)
client.create_index(collection_name, index_params=index_params)

client.load_collection(collection_name)
```

---

### 4. Search / Query

Use queries with different timestamps to validate expiration behavior:

```python
query_expr = "id > 0"
res = client.query(
    collection_name,
    query_expr,
    output_fields=["id", "ttl"],
    limit=100,
)
print(res)
```
