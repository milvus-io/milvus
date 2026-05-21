# Design Document: Drop Collection Field / Function

**Branch**: `feat/drop-collection-field`
**Author**: sijie-ni-0214
**Date**: April 2026
**Scope**: 30 files, +2,182/-600 lines

---

## 1. Overview

### 1.1 Motivation

Schema evolution is essential for production vector databases. While Milvus already supports adding function fields via `AlterCollectionSchema` (PR #48810), there is no mechanism to remove obsolete fields or functions from a collection schema. Users who added experimental BM25 fields, deprecated scalar columns, or misconfigured function fields must currently recreate the entire collection and re-ingest all data.

This feature enables users to dynamically drop fields and functions from existing collection schemas without data re-ingestion, completing the schema evolution lifecycle alongside the existing add-field capability.

### 1.2 Key Requirements

1. **Non-disruptive schema evolution**: Drop fields/functions without collection recreation
2. **Backward compatibility**: Existing segments with dropped-field data must remain loadable and queryable
3. **Cascade cleanup**: Indexes on dropped fields must be automatically removed
4. **Field ID safety**: Dropped field IDs must never be reused to prevent data corruption
5. **Concurrency safety**: Reuse the existing `AlterCollectionSchema` mutual exclusion and schema version consistency gateway
6. **Unified API**: Integrate into the existing `AlterCollectionSchema` RPC rather than introducing a separate RPC

### 1.3 Design Principles

- **Schema-driven filtering**: All components use the latest schema to determine field accessibility; dropped fields are invisible without deleting underlying data
- **Lazy cleanup**: Binlogs of dropped fields are not immediately deleted; they are simply skipped during segment loading
- **Inline cascade**: Index deletion executes within the broadcast ack callback to avoid deadlocks, following the same pattern as `DropCollection`
- **ID monotonicity**: A persistent `max_field_id` property ensures field IDs only increase, even after drops

---

## 2. Architecture Overview

### 2.1 High-Level Data Flow

```
+------------------------------------------------------------------------------+
|                      AlterCollectionSchema (DropRequest)                      |
+------------------------------------------------------------------------------+
                                        |
                                        v
+------------------------------------------------------------------------------+
|                                   PROXY                                      |
|  * Mutual exclusion: alterSchemaInFlight (one schema change per collection)  |
|  * Schema version consistency check (all segments aligned)                   |
|  * Validate drop constraints (not PK, not partition key, not last vector...) |
|  * Forward to RootCoord via MixCoord                                         |
+------------------------------------------------------------------------------+
                                        |
                                        v
+------------------------------------------------------------------------------+
|                                ROOTCOORD                                     |
|  * Build new schema (remove field/function, increment version)               |
|  * Persist max_field_id to collection properties                             |
|  * Broadcast AlterCollectionMessage to WAL (control + virtual channels)      |
|  * Ack callback: cascade drop indexes on dropped fields                      |
+------------------------------------------------------------------------------+
                                        |
                    +-------------------+-------------------+
                    v                                       v
+----------------------------------+   +-----------------------------------+
|       QUERYNODE (Go + C++)       |   |            DATACOORD              |
| * Segment loader: skip binlogs   |   | * DropIndex: tolerate dropped     |
|   and indexes for dropped fields |   |   fields (skip vector-index       |
| * C++ segcore: has_field() check |   |   validation when field is nil)   |
|   in ComputeDiff*, LoadFieldData |   |                                   |
| * Parquet reader: skip columns   |   |                                   |
|   not in field_metas             |   |                                   |
+----------------------------------+   +-----------------------------------+
```

### 2.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Proxy** | Request validation, concurrency control, schema version consistency gate |
| **RootCoord** | Schema construction, field ID management, WAL broadcast, cascade index deletion |
| **QueryNode (Go)** | Filter dropped-field binlogs and indexes during segment loading |
| **Segcore (C++)** | Schema-driven field filtering in `ComputeDiff*`, `LoadFieldData`, column group loading |
| **DataCoord** | Tolerate dropped fields in `DropIndex` flow; skip vector-index validation for nil fields |

### 2.3 What This Feature Does NOT Change

The drop field feature reuses the existing `AlterCollectionSchema` infrastructure without modifying:

- **Streaming/WAL layer**: No new message types; uses existing `AlterCollectionMessage`
- **DataNode**: No backfill; dropped-field binlogs are left in place
- **Schema version consistency**: Reuses the existing gateway from add-field (DataCoord reports consistent/total segment counts)

---

## 3. API Design

### 3.1 RPC Endpoint

Drop field/function uses the existing `AlterCollectionSchema` RPC with a new `DropRequest` action:

```protobuf
rpc AlterCollectionSchema(AlterCollectionSchemaRequest) returns (AlterCollectionSchemaResponse) {}
```

### 3.2 Proto Message Changes

#### DropRequest Definition

```protobuf
message AlterCollectionSchemaRequest {
    // ... existing fields (db_name, collection_name, etc.)

    message DropRequest {
        oneof identifier {
            string field_name = 1;     // Drop field by name
            int64 field_id = 2;        // Drop field by ID
            string function_name = 3;  // Drop function (cascades to output fields)
        }
    }

    message Action {
        oneof op {
            AddRequest add_request = 1;    // Existing: add field/function
            DropRequest drop_request = 2;  // New: drop field/function
        }
    }

    Action action = 5;
}
```

#### AlterCollectionMessage Extension

```protobuf
// messages.proto - AlterCollectionMessageBody.Updates
message Updates {
    // ... existing fields (schema, properties, etc.)
    repeated int64 dropped_field_ids = 9;  // Field IDs removed from schema,
                                            // used to cascade-delete indexes
                                            // in ack callback
}
```

#### UpdateMask Difference: Drop vs Add

| Operation | UpdateMask Fields |
|-----------|-------------------|
| **Add** (field/function) | `schema` only |
| **Drop** (field/function) | `schema` + `properties` |

Drop requires updating `properties` because it must persist `max_field_id` to prevent field ID reuse.

### 3.3 Client SDK (pymilvus)

```python
def alter_collection_schema(
    self,
    collection_name: str,
    *,
    field_name: str = "",       # Drop by field name
    field_id: int = 0,          # Drop by field ID
    function_name: str = "",    # Drop by function name (cascade)
    db_name: str = "",
    timeout: Optional[float] = None,
) -> None:
    """Drop a field or function from the collection schema.

    Exactly one of field_name, field_id, or function_name must be specified.
    """
```

---

## 4. Component Design Details

### 4.1 Proxy Layer

**Files**: `internal/proxy/impl.go`, `internal/proxy/task.go`

#### Concurrency Control (Inherited from Add-Field)

Drop operations share the same concurrency safeguards as add operations:

1. **`alterSchemaInFlight`** (`sync.Map`): Ensures only one `AlterCollectionSchema` request per collection at a time. Key is `dbName/collectionName`.

2. **Schema Version Consistency Gate**: Before any schema change, the proxy queries DataCoord for segment schema version statistics:
   ```
   consistentSegments == totalSegments  -->  proceed
   consistentSegments != totalSegments  -->  reject (previous schema change still propagating)
   ```

3. **DDL Queue**: Tasks execute serially within the DDL queue, preventing race conditions.

#### PreExecute: Drop Validation

```go
func (t *alterCollectionSchemaTask) preExecuteDrop(ctx context.Context) error {
    dropReq := t.req.GetAction().GetOp().(*milvuspb.AlterSchemaAction_DropRequest).DropRequest

    switch id := dropReq.GetIdentifier().(type) {
    case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FunctionName:
        return validateDropFunction(t.oldSchema, id.FunctionName)
    case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldId:
        // Resolve field ID to field name, then validate
        fieldName := resolveFieldName(t.oldSchema, id.FieldId)
        return validateDropField(t.oldSchema, fieldName)
    case *milvuspb.AlterCollectionSchemaRequest_DropRequest_FieldName:
        return validateDropField(t.oldSchema, id.FieldName)
    }
}
```

#### validateDropField Constraints

| Constraint | Error Message |
|-----------|---------------|
| Field name is empty | `field name cannot be empty` |
| Field not found in schema | `field not found: {name}` |
| System field (`$rowid`, `$timestamp`, `$meta`, `$namespace`) | `cannot drop system field: {name}` |
| Primary key field | `cannot drop primary key field: {name}` |
| Partition key field | `cannot drop partition key field: {name}` |
| Clustering key field | `cannot drop clustering key field: {name}` |
| Dynamic field (`IsDynamic=true`) | `cannot drop dynamic field directly, use AlterCollection to disable` |
| Last vector field in schema | `cannot drop the last vector field: {name}` |
| Field is function input | `field is referenced by function {fn} as input` |
| Field is function output | `field is referenced by function {fn} as output, drop the function instead` |
| Target is a sub-field of a struct array field | `cannot drop sub-field of struct array field: {struct}.{sub}` |
| Dropping whole struct array field would leave no vector | `cannot drop struct array field {name}: it would leave no vector field in the collection` |

**Struct array field scope**:
- Dropping a **whole struct array field by name or ID** is supported; it is equivalent to batch-dropping the struct entry plus all its sub-fields (`droppedFieldIds` covers the struct ID and every sub-field ID). The existing index cascade and segcore `has_field()` filtering cover the removal without any C++ changes.
- Dropping an **individual sub-field** is not supported in this change. Milvus has no runtime API for adding a struct array field or an individual sub-field to an existing collection — struct array fields and their sub-fields are declared once at collection creation (neither `add_collection_field` nor `AlterCollectionSchema.AddRequest` accepts a struct-shaped payload). Dropping a single sub-field would therefore be asymmetric with no restore path, and is explicitly rejected.

#### validateDropFunction Constraints

| Constraint | Error Message |
|-----------|---------------|
| Function name is empty | `function name is empty` |
| Function not found | `function not found: {name}` |
| Cascade removal of output fields would leave no vector in schema | `cannot drop function {name}: it would leave no vector field in the collection` |


### 4.2 RootCoord Layer

**File**: `internal/rootcoord/ddl_callbacks_alter_collection_schema.go`

#### broadcastAlterCollectionSchemaDrop

```
1. Resolve identifier to target (field or function)
      |
      v
2. Build new schema:
   * buildSchemaForDropField(coll, fieldName, fieldID)
     OR buildSchemaForDropFunction(coll, functionName)
      |
      v
3. Returns: (newSchema, newProperties, droppedFieldIds)
      |
      v
4. Broadcast AlterCollectionMessage:
   * UpdateMask: [schema, properties]
   * Body: { schema, properties, droppedFieldIds }
   * Channels: control channel + all virtual channels
      |
      v
5. Ack Callback:
   a. meta.AlterCollection() -- persist metadata
   b. cascadeDropFieldIndexesInline() -- delete indexes
   c. BroadcastAlteredCollection() -- notify proxies
   d. ExpireCaches() -- invalidate metadata caches
```

#### buildSchemaForDropField

```go
func buildSchemaForDropField(coll *model.Collection, fieldName string, fieldID int64) (
    *schemapb.CollectionSchema, []*commonpb.KeyValuePair, []int64, error,
) {
    // 1. Locate target field by name or ID
    var targetField *model.Field
    for _, field := range coll.Fields {
        if (fieldName != "" && field.Name == fieldName) ||
           (fieldID > 0 && field.FieldID == fieldID) {
            targetField = field
            break
        }
    }

    // 2. Persist max_field_id to prevent ID reuse
    maxFieldID := nextFieldID(coll) - 1
    properties := updateMaxFieldIDProperty(coll.Properties, maxFieldID)

    // 3. Build new schema without the target field
    newFields := []*schemapb.FieldSchema{}
    for _, field := range coll.Fields {
        if field.FieldID != targetField.FieldID {
            newFields = append(newFields, marshal(field))
        }
    }

    schema := &schemapb.CollectionSchema{
        Fields:    newFields,
        Functions: marshalFunctions(coll.Functions),
        Version:   coll.SchemaVersion + 1,
    }

    return schema, properties, []int64{targetField.FieldID}, nil
}
```

#### buildSchemaForDropFunction

Drops a function and **all its output fields** (cascade):

```go
func buildSchemaForDropFunction(coll *model.Collection, functionName string) (
    *schemapb.CollectionSchema, []*commonpb.KeyValuePair, []int64, error,
) {
    // 1. Find target function
    var targetFunc *model.Function
    for _, fn := range coll.Functions {
        if fn.Name == functionName { targetFunc = fn; break }
    }

    // 2. Collect output field IDs as droppedFieldIds
    droppedFieldIds := targetFunc.OutputFieldIDs

    // 3. Remove output fields from schema
    outputFieldIDSet := make(map[int64]bool)
    for _, id := range droppedFieldIds { outputFieldIDSet[id] = true }

    newFields := filterFields(coll.Fields, outputFieldIDSet)

    // 4. Remove function from schema
    newFunctions := filterFunctions(coll.Functions, targetFunc.Name)

    // 5. Persist max_field_id, increment schema version
    // ...

    return schema, properties, droppedFieldIds, nil
}
```

### 4.3 Field ID Reuse Prevention

**Problem**: If field IDs are assigned based on `max(current_fields)`, dropping the field with the highest ID would cause the next added field to reuse that ID. Since binlogs of the dropped field still exist on storage with the old field ID, this creates data corruption.

**Solution**: Persist `max_field_id` in collection properties.

```go
// nextFieldID reads max field ID from three sources and returns max + 1:
func nextFieldID(coll *model.Collection) int64 {
    maxFieldID := int64(common.StartOfUserFieldID)  // 100

    // Source 1: Current fields
    for _, field := range coll.Fields {
        if field.FieldID > maxFieldID { maxFieldID = field.FieldID }
    }

    // Source 2: Struct array sub-fields
    for _, sf := range coll.StructArraySubFields { /* ... */ }

    // Source 3: Persisted max_field_id (survives field deletion)
    for _, kv := range coll.Properties {
        if kv.Key == "max_field_id" {
            if v, err := strconv.ParseInt(kv.Value, 10, 64); err == nil {
                if v > maxFieldID { maxFieldID = v }
            }
        }
    }

    return maxFieldID + 1
}
```

**Key**: `updateMaxFieldIDProperty` is called during every drop operation, ensuring the high-water mark is persisted even if the field with the highest ID is removed.

### 4.4 Cascade Index Deletion

**File**: `internal/rootcoord/ddl_callbacks_alter_collection_properties.go`

#### Why Inline in Ack Callback?

`broadcastAlterCollectionSchema()` holds a broadcast lock on the collection. Calling `DropIndex` as a separate RPC would attempt to acquire the same lock, causing a deadlock. Instead, index deletion is executed inline within the ack callback (the same pattern used by `DropCollection`).

#### cascadeDropFieldIndexesInline

```go
func cascadeDropFieldIndexesInline(ctx context.Context, c *Core, msg *msgstream.AlterCollectionMsg) error {
    droppedFieldIDs := msg.Body.Updates.DroppedFieldIds
    if len(droppedFieldIDs) == 0 {
        return nil  // No fields dropped, nothing to cascade
    }

    // 1. Query all indexes on the collection
    resp, err := c.mixCoord.DescribeIndex(ctx, &indexpb.DescribeIndexRequest{
        CollectionID: msg.CollectionID,
    })

    // 2. Find indexes on dropped fields
    droppedSet := make(map[int64]bool)
    for _, id := range droppedFieldIDs { droppedSet[id] = true }

    for _, index := range resp.IndexInfos {
        if droppedSet[index.FieldID] {
            // 3. Broadcast DropIndex message via control channel
            registry.CallMessageAckCallback(ctx, DropIndexMessage{
                IndexID: index.IndexID,
                // ...
            })
        }
    }
    return nil
}
```

#### DataCoord Tolerance for Dropped Fields

When `cascadeDropFieldIndexesInline` triggers `DropIndex`, the field no longer exists in the schema. The DataCoord `DropIndex` handler must tolerate this:

```go
// internal/datacoord/index_service.go
field := typeutil.GetField(schema, index.FieldID)
if field == nil {
    // Field already dropped from schema -- skip vector-index validation,
    // proceed with index cleanup
    log.Info("field already dropped, proceeding with index drop")
    continue
}
```

Without this tolerance, the cascade would fail because `DropIndex` normally validates that vector indexes cannot be dropped on loaded collections.

### 4.5 Disable Dynamic Field

Disabling the dynamic schema (`AlterCollection` with `EnableDynamicField=false`) reuses the drop-field infrastructure:

```go
func broadcastDisableDynamicField(ctx context.Context, coll *model.Collection) error {
    // 1. Find the $meta (dynamic) field
    var dynamicFieldID int64
    for _, field := range coll.Fields {
        if field.IsDynamic { dynamicFieldID = field.FieldID; break }
    }

    // 2. Build schema without $meta, set EnableDynamicField=false
    // 3. Broadcast with DroppedFieldIds = [dynamicFieldID]
    // 4. Ack callback: cascadeDropFieldIndexesInline removes $meta indexes
}
```

**Proxy-side gate coverage**: Enabling/disabling the dynamic field mutates the schema and bumps `SchemaVersion + 1`, so it must respect the same concurrency invariants as `AlterCollectionSchema` (see §6.1). The `AlterCollection` handler inspects `request.Properties` and, when `dynamicfield.enabled` is present, routes the request through the same `alterSchemaInFlight` + `checkSchemaVersionConsistency` gates as `AlterCollectionSchema`, sharing the same `sync.Map` so the two handlers are mutually exclusive per collection. Requests that only alter unrelated properties (e.g. `collection.ttl.seconds`, `mmap.enabled`) bypass the gate and keep their existing lightweight path.

### 4.6 Segcore (C++) Layer

**Files**: `internal/core/src/segcore/SegmentLoadInfo.cpp`, `ChunkedSegmentSealedImpl.cpp`, `SegmentGrowingImpl.cpp`

#### Schema-Driven Filtering Strategy

All C++ components use the **latest schema** (not the segment's original schema) to determine field visibility. The core check is:

```cpp
// Schema.h
bool has_field(FieldId field_id) const {
    return fields_.count(field_id) > 0;
}
```

This single method gates all data access:

| Function | File | Behavior |
|----------|------|----------|
| `ComputeDiffBinlogs` | SegmentLoadInfo.cpp | Skip binlog entries for dropped fields |
| `ComputeDiffIndexes` | SegmentLoadInfo.cpp | Skip index entries for dropped fields |
| `ComputeDiffColumnGroups` | SegmentLoadInfo.cpp | Skip column groups for dropped fields |
| `ComputeDiffReloadFields` | SegmentLoadInfo.cpp | Exclude dropped fields from reload set |
| `LoadFieldData` | ChunkedSegmentSealedImpl.cpp | Defense-in-depth: skip if field not in schema |
| `load_field_data_internal` | SegmentGrowingImpl.cpp | Skip growing segment data for dropped fields |
| `load_column_group_data_internal` | SegmentGrowingImpl.cpp | Skip column groups for dropped fields |
| `TranslateGroupChunk` | GroupChunkTranslator.cpp | Skip parquet columns not in field_metas |

**Key Design Decision**: Use `new_info.schema_` (the latest schema from the load request) rather than `this->schema_` (the segment's cached schema) in all `ComputeDiff*` functions. This ensures that even if the segment was loaded before the field was dropped, a reload/delta-load will correctly filter out the dropped field.

### 4.7 QueryNode Segment Loader (Go)

**File**: `internal/querynodev2/segments/segment_loader.go`

The Go segment loader filters dropped fields at two points:

```go
// Index loading
fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
if err != nil {
    log.Info("skip index for dropped field", zap.Int64("fieldID", fieldID))
    continue  // Field dropped, skip its index
}

// Binlog/column group loading
fieldSchema, err := schemaHelper.GetFieldFromID(fieldID)
if err != nil {
    log.Info("skip binlog for dropped field", zap.Int64("fieldID", fieldID))
    continue  // Field dropped, skip it
}
```

**Important**: The loader uses `continue` (not `return err`) to ensure that other fields in the same column group are still loaded correctly.

---

## 5. Drop Field vs Drop Function: Semantic Differences

| Aspect | Drop Field | Drop Function |
|--------|-----------|---------------|
| **Target** | A single field | A function + all its output fields |
| **DroppedFieldIds** | `[fieldID]` | `[outputFieldID1, outputFieldID2, ...]` |
| **Input fields** | N/A | Preserved (not removed) |
| **Index cascade** | Drops indexes on the field | Drops indexes on all output fields |
| **Validation** | Cannot drop if referenced by a function | No restriction (output fields cascade) |

### Difference from DropCollectionFunction (Existing API)

| Aspect | DropCollectionFunction (existing) | AlterCollectionSchema Drop Function (this feature) |
|--------|----------------------------------|-----------------------------------------------------|
| Output fields | Preserved, marked `IsFunctionOutput=false` | **Cascade deleted** from schema |
| Indexes | Not touched | Cascade deleted |
| Field ID protection | No `max_field_id` update | `max_field_id` updated |

---

## 6. Concurrency and Consistency

### 6.1 Three Layers of Protection

Drop operations inherit the same concurrency model as add operations through the unified `AlterCollectionSchema` entry point:

1. **`alterSchemaInFlight` Mutex** (`sync.Map`): Only one schema modification per collection at a time. A second request immediately receives an error response.

2. **DDL Queue Serial Execution**: All DDL tasks (including schema changes) execute serially within the proxy's DDL queue.

3. **Schema Version Consistency Gate**: The proxy checks DataCoord's segment statistics before allowing any schema change. If a previous change (e.g., add-field backfill) hasn't propagated to all segments, the new request is rejected.

**Coverage across RPC entry points**: Because the proxy rootcoord lock serializes concurrent *writes* but not *writer-vs-in-flight-backfill*, layers (1) and (3) must guard every RPC entry that mutates the schema. In this PR, both `AlterCollectionSchema` and the dynamic-field branch of `AlterCollection` go through the same two gates and share the same `alterSchemaInFlight` `sync.Map`, so any two schema mutations targeting the same collection — regardless of which RPC they arrived on — are mutually exclusive, and neither can begin until the previous mutation's backfill has reached 100% of segments.

### 6.2 Ack Callback Idempotency

The ack callback (`cascadeDropFieldIndexesInline` + `meta.AlterCollection`) may be retried with exponential backoff if it fails. Both operations are idempotent:
- `meta.AlterCollection`: Overwrites metadata; repeated calls produce the same result
- `MarkIndexAsDeleted`: Skips already-deleted indexes

### 6.3 Cascade Intermediate Window

A drop proceeds in two phases within the ack-callback pipeline:

1. **Metadata phase**: `meta.AlterCollection` removes the field from `coll.Fields`. After this step `DescribeCollection` no longer reports the field.
2. **Index cascade phase**: `cascadeDropFieldIndexesInline` broadcasts `DropIndex` for each surviving index on the dropped field ID; once those acks complete, `indexMeta` entries are cleared.

**Observable window**: Between phase 1 and phase 2 — typically milliseconds, but bounded only by the `DropIndex` broadcast round-trip — an external observer can see:

- `DescribeCollection` → field is gone
- `ListIndexes` → an index on the dropped field name is still reported

This state is **transient**, not an orphaned-index bug. It is equivalent to the window that `DropCollection` exposes between deleting the collection meta and the cascade cleanup of its indexes.

**Retry convergence**: `cascadeDropFieldIndexesInline` is driven by the standard broadcast ack-callback framework — if the `DropIndex` broadcast fails (network, rootcoord restart, etc.), the ack infrastructure retries until success, and rootcoord re-drives outstanding callbacks from the broadcast log after restart. Final consistency is guaranteed; the window does not accumulate.

**On-call diagnosis**: to distinguish a transient drop-field cascade window from a true orphaned-index bug:

- RootCoord logs `cascade dropping index on dropped field` with `fieldID`, `indexName`, `indexID` for every cascade target; the presence of a recent entry for the observed index confirms the current state is transient.
- Re-query after ~30s — if `ListIndexes` still returns the dropped field, the cascade has genuinely failed to converge and should be treated as an orphaned-index bug through the existing runbook.

### 6.4 In-flight Request Semantics

Segcore's schema-driven filtering (§4.6) gates **loading**, not **query plan compilation or execution**. This section specifies how in-flight search / query / groupby requests behave relative to a concurrent drop.

**A request "observes" a dropped field only when it actually references the field**, through any of:
- `anns_field`
- `filter` / expression
- `output_fields` (explicit or via `"*"` expansion)
- `group_by_field`
- partition-key expression

Requests that do not reference the dropped field pass through unaffected — plan compilation never looks up the removed field, and the executor never issues a column access for it.

**When a request does reference the dropped field**, one of three outcomes applies, determined by the race between the proxy's `globalMetaCache` invalidation and the querynode's segment reload:

| Case | Proxy cache state | QueryNode segment state | Outcome |
|------|-------------------|-------------------------|---------|
| 1. Post-invalidation (common) | new schema (N+1) | any | proxy's `CreateSearchPlanArgs` schema-helper cannot resolve the field; `task_search.go` wraps the error as `ErrParameterInvalid: "failed to create query plan: field not found: <field>"` — returned to the client |
| 2. Cross-version (narrow) | stale (N) | new schema (N+1), segment reloaded without the field | plan compiles against schema N, reaches querynode; segcore's `chunk_data_impl`/`chunk_array_view_impl` check `field_data_ready_bitset_`, the bit is 0 for the dropped field, `AssertInfo` raises `SegcoreError` — surfaced to the client as an error status |
| 3. No race | stale or new | still has the field | executes normally |

Cases 1 and 2 both return a clear error to the client; **neither aborts or corrupts state**. Case 2 is bounded in time by the proxy cache invalidation latency (rootcoord broadcast → per-proxy cache invalidate, milliseconds to seconds in practice), and further bounded by the schema-version-consistency gate in §6.1: a second schema mutation cannot begin until the previous one has propagated, so the cross-version window is always tied to a single in-flight mutation and never accumulates.

SDKs cache collection schema in a process-wide `GlobalCache`. `add_collection_field` and `add_collection_function` previously did not invalidate this cache, relying on `@retry_on_schema_mismatch` on `insert_rows` / `upsert_rows` to recover from staleness on the write path.

Introducing drop-field / drop-function surfaces a case this passive model cannot recover from: for bytes-input search, the SDK consults the cache for the `anns_field` vector type to encode the placeholder. After `drop_collection_field` followed by `add_collection_field` re-creating the field with a different type, the stale cache returns the old type, the server rejects the request, and because `search` has no schema-mismatch retry (unlike `insert_rows`), the error persists on every subsequent call.

To close this, all four schema-mutating public methods — `drop_collection_field`, `drop_collection_function`, `add_collection_field` and `add_collection_function` (sync + async) — now invalidate the cache on success, so the next request re-fetches fresh schema from the server.

---

## 7. Sequence Diagram

### 7.1 Drop Field Flow

```
Client              Proxy           RootCoord        WAL/Streaming      QueryNode (C++)
  |                   |                 |                |                    |
  | AlterCollectionSchema               |                |                    |
  | (DropRequest: field_name="vec2")    |                |                    |
  |------------------>|                 |                |                    |
  |                   |                 |                |                    |
  |                   | alterSchemaInFlight.LoadOrStore  |                    |
  |                   | (check no concurrent schema op) |                    |
  |                   |                 |                |                    |
  |                   | checkSchemaVersionConsistency   |                    |
  |                   | (all segments aligned?)         |                    |
  |                   |                 |                |                    |
  |                   | validateDropField               |                    |
  |                   | (not PK, not last vector, etc.) |                    |
  |                   |                 |                |                    |
  |                   | AlterCollectionSchema           |                    |
  |                   |---------------->|                |                    |
  |                   |                 |                |                    |
  |                   |                 | buildSchemaForDropField             |
  |                   |                 | (remove field, version+1,          |
  |                   |                 |  persist max_field_id)             |
  |                   |                 |                |                    |
  |                   |                 | Broadcast AlterCollectionMessage   |
  |                   |                 |--------------->|                    |
  |                   |                 |                |                    |
  |                   |                 |         (WAL append + flush)       |
  |                   |                 |                |                    |
  |                   |                 | Ack Callback:  |                    |
  |                   |                 | 1. AlterCollection (persist)       |
  |                   |                 | 2. cascadeDropFieldIndexesInline   |
  |                   |                 |    -> DropIndex for vec2 indexes   |
  |                   |                 | 3. BroadcastAlteredCollection      |
  |                   |                 | 4. ExpireCaches                    |
  |                   |                 |                |                    |
  |                   |<----------------|                |                    |
  |<------------------|                 |                |                    |
  |                   |                 |                |                    |
  |                   |                 |                | (on next load/reload)
  |                   |                 |                |                    |
  |                   |                 |                | ComputeDiffBinlogs |
  |                   |                 |                | has_field("vec2")  |
  |                   |                 |                | -> false, skip     |
  |                   |                 |                |                    |
```

---

## 8. Key Design Decisions

### 8.1 Lazy Data Cleanup (No Immediate Binlog Deletion)

**Decision**: Dropped-field binlogs remain on object storage; they are simply skipped during segment loading.

**Rationale**:
- Immediate deletion would require scanning all segments for the field's binlogs, which is expensive
- Binlogs are naturally cleaned up during compaction (compacted segments only contain current-schema fields)
- Avoids complex rollback logic if deletion fails partway
- Storage cost is bounded and decreasing (compaction gradually removes stale data)

### 8.2 Inline Cascade via Ack Callback

**Decision**: Execute index deletion inline within the broadcast ack callback, not as a separate RPC call.

**Rationale**:
- `broadcastAlterCollectionSchema()` holds a broadcast lock on the collection
- Calling `DropIndex` as a separate RPC would deadlock (attempts to acquire the same lock)
- The ack callback pattern is proven: `DropCollection` uses the same approach for cascade cleanup
- Ack callbacks support exponential-backoff retry with idempotent operations

### 8.3 Persistent max_field_id

**Decision**: Store the historical maximum field ID in collection properties (`max_field_id` key).

**Rationale**:
- Without this, dropping the highest-ID field would cause `nextFieldID()` to return a previously-used ID
- Reused field IDs would cause QueryNode to incorrectly load stale binlogs for the new field
- The property persists through metadata updates and is read by `nextFieldID()` alongside current field IDs
- Minimal storage overhead (one key-value pair per collection)

### 8.4 Unified Entry Point (No Separate RPC)

**Decision**: Use `AlterCollectionSchema` with `DropRequest` instead of a dedicated `DropCollectionField` RPC.

**Rationale**:
- Inherits all existing concurrency controls (mutual exclusion, version consistency gate)
- Single API surface for all schema evolution operations
- Consistent with the Add/Drop symmetry pattern
- Simplifies client SDK and documentation

### 8.5 Schema-Driven Filtering in C++

**Decision**: Use the new/latest schema (`new_info.schema_`) for all `ComputeDiff*` filtering, not the segment's cached schema.

**Rationale**:
- The segment's cached schema may predate the drop operation
- Using the latest schema ensures correct filtering on reload/delta-load
- Single source of truth: the schema from the load request always reflects current state
- Defense-in-depth: multiple layers check `has_field()` to catch edge cases

---

## 9. Testing Strategy

### 9.1 Unit Tests

| Component | Test Scope | Test Count |
|-----------|-----------|------------|
| **Proxy: validateDropField** | All constraint violations (PK, partition key, clustering key, dynamic, last vector, function reference, system field, not found) | 12 |
| **Proxy: preExecuteDrop** | DropRequest by field_name / field_id / function_name, nil action, unknown action | 11 |
| **Proxy: validateDropFunction** | Function found / not found | 4 |
| **RootCoord: broadcastAlterCollectionSchemaDrop** | Full broadcast flow with ack callback | 1 |
| **RootCoord: buildSchemaForDropFunction** | Function removal, output field cascade, droppedFieldIds | 3 |
| **RootCoord: nextFieldID** | Properties-based max_field_id, historical collection compat | 4 |
| **RootCoord: updateMaxFieldIDProperty** | Property creation and update | 4 |
| **C++: SegmentLoadInfo** | ComputeDiffBinlogs/Indexes/ColumnGroups with dropped fields | 8 |

### 9.2 E2E Tests

| # | Scenario | Verification |
|---|----------|-------------|
| 1 | Drop scalar field (empty collection) | Schema updated, field absent |
| 2 | Drop scalar field (with data) | Query does not return dropped field |
| 3 | Drop indexed field | Index cascade deleted |
| 4 | Drop one of multiple vector fields | Remaining vector field searchable |
| 5 | Insert after drop | New data lacks dropped field, search works |
| 6 | Drop + add same-name field | New field gets different field ID, no crash |
| 7 | Constraint rejection | PK / partition key / last vector correctly refused |
| 8 | Dynamic field disable/enable | Idempotent toggle, index cascade |
| 9 | Loaded collection drop + reload | Search and query work after reload |
| 10 | Drop BM25 function | Function + output fields + indexes all removed, input preserved |
| 11 | Drop function input field | Rejected (must drop function first) |
| 12 | Field ID reuse prevention | Drop + Add same-name different-type, search old data no crash |
| 13 | Add + Drop serial interaction | Schema version consistency gate works |

---

## 10. Future Enhancements

### 10.1 Physical Binlog Cleanup

Add a background GC task that scans object storage and removes binlogs for fields no longer present in any segment's schema.

### 10.2 Batch Drop

Support dropping multiple fields/functions in a single `AlterCollectionSchema` request.

### 10.3 Drop Field with Data Migration

Support dropping a field while migrating its data to another field (e.g., renaming).

---

## 11. References

- Add Function Field Design: [20260129-add-function-field-design.md](./20260129-add-function-field-design.md)
- AlterCollectionSchema RPC: PR [#48810](https://github.com/milvus-io/milvus/pull/48810)
- Milvus Architecture: [docs/architecture.md](../architecture.md)
