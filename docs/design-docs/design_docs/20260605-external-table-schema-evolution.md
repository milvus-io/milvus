# External Table Schema Evolution

- **Created:** 2026-06-05
- **Updated:** 2026-07-16
- **Author(s):** Wei Liu
- **Status:** Draft
- **Component:** Proxy, RootCoord, DataCoord, DataNode, QueryCoord, QueryNode, SDK
- **Related Docs:** `20260105-external_table.md`, `20260129-add-function-field-design.md`, `20260413-drop-collection-field-design.md`, `20260521-external-table-function-output.md`

## Summary

This document extends external collections with online schema evolution through
`AlterCollectionSchema`. The target API supports the following operations while
the external collection is unloaded or loaded:

- add a source-backed field;
- add a built-in function with one new output field;
- add a BM25 function and sparse output field;
- drop a source-backed field;
- drop a function and all of its output fields;
- drop a BM25 function and sparse output field.

The design separates three states that are coupled for internal collections but
must be explicit for external collections:

1. **Schema visibility**: the field or function exists in collection metadata.
2. **Durable materialization**: DataCoord segment metadata points to manifests
   that contain the required external or generated columns.
3. **Serving readiness**: every loaded replica has switched to QueryNode
   segments whose manifest and index state can serve the field.

`AlterCollectionSchema` changes schema metadata and bumps the collection schema
version. It never rewrites external source files and never uses internal
`BumpSchemaVersionCompaction` for external collections. Existing loaded queries
on already-available fields continue to run after the schema mutation. Added
function outputs become queryable after `RefreshExternalCollection`
materializes them and QueryCoord finishes reloading the affected loaded
segments. Added source-backed fields are schema-visible before refresh, but
they are not considered materialized until refresh updates segment manifests
and fake-binlog field coverage. Reads that reference an uncovered field use the
existing execution-path error. Exact read-path error normalization is handled
separately by
[#50416](https://github.com/milvus-io/milvus/issues/50416). Drop operations
become effective through schema filtering and index cache invalidation.

## Motivation

External collections can already declare BM25, MinHash, and TextEmbedding
function outputs at create time. Refresh computes generated columns and persists
them as Milvus-owned StorageV3 column groups beside source-backed external
columns.

Users also need to evolve external collection schemas after creation:

1. Add a source column that exists in the external files but was not declared
   during collection creation.
2. Add a BM25 sparse field over an existing text column.
3. Add a TextEmbedding or MinHash output over an existing source column.
4. Drop an obsolete source-backed field.
5. Drop a misconfigured function and all generated output columns.
6. Drop a BM25 setup without recreating and refreshing the whole collection.

Recreating the collection is expensive because users must rebuild indexes,
reload data, and re-run refresh over all source fragments. Requiring release
before every schema mutation is also operationally expensive for long-lived
external collections. Loaded support should keep existing queries on already
materialized fields available while schema metadata and external manifests move
forward.

## Design Principles

1. **One schema mutation API**: external schema evolution uses
   `AlterCollectionSchema`. SDK helpers for add field, add function, add BM25,
   drop field, drop function, and drop BM25 all build this RPC.
2. **Collection-type-specific materialization**: internal collections keep their
   existing write, function, and compaction behavior. External collections use
   refresh-time materialization.
3. **Metadata first, materialization later**: schema mutation succeeds after
   metadata is updated and broadcast. Added fields and functions are visible in
   schema immediately. Refresh is the durable materialization boundary for
   generated function outputs and for DataCoord segment coverage metadata.
4. **Read-path behavior is explicit work**: this PR keeps already-covered fields
   serving while schema and refresh metadata move forward. Precise errors for
   requests that reference schema-visible but uncovered fields are tracked by
   [#50416](https://github.com/milvus-io/milvus/issues/50416), not duplicated
   here.
5. **Drops are schema-driven**: dropped fields disappear from the latest schema.
   Old source columns, generated columns, indexes, and stats may remain on
   storage until later garbage collection, but they must be unreachable from
   new query plans.
6. **Clear field classes**: source-backed fields require `external_field`.
   Function output fields must not define `external_field`.
7. **No field ID reuse**: every drop keeps `max_field_id` pinned to the
   historical high-water mark.
8. **No hidden source writes**: Milvus never rewrites external source files. It
   only writes generated columns, stats, fake binlogs, and manifests that it
   owns.
9. **Online target switch**: loaded collections keep serving already-available
   fields while refresh updates segment manifests. QueryCoord reopens a loaded
   segment when its target manifest advances and loads newly built indexes when
   their metadata and files become available.

## Goals

1. Support add source-backed field on external collections through
   `AlterCollectionSchema(AddRequest)` with an empty `func_schema`.
2. Support add function on external collections for refresh-supported built-in
   functions: BM25, MinHash, and TextEmbedding.
3. Support add BM25 as a normal add-function case with one new sparse vector
   output field.
4. Support drop field for ordinary source-backed fields.
5. Support drop function for built-in functions, cascading to all output
   fields.
6. Support drop BM25 as drop function on a BM25 function.
7. Support the same operations while the external collection is loaded.
8. Keep existing internal collection `AlterCollectionSchema(AddRequest)`
   behavior backward-compatible.
9. Avoid internal schema bump compaction side effects for external collections.
10. Provide deterministic validation for invalid external schema mutations.
    User-visible read-path errors for unmaterialized fields are tracked by
    [#50416](https://github.com/milvus-io/milvus/issues/50416).

## Non-Goals

1. No automatic refresh after add field, add function, or add BM25 in this
   design. Users still call `RefreshExternalCollection`.
2. No support for user-defined functions.
3. No support for renaming fields or remapping `external_field`.
4. No support for changing the data type or vector dimension of an existing
   field.
5. No direct drop of a function output field. Users must drop the owning
   function.
6. No physical garbage collection of old generated column files, source columns,
   or stats in this feature.
7. No batch request that mixes multiple add and drop operations in one
   `AlterCollectionSchema` request.
8. No expansion beyond the existing internal BM25 and MinHash
   add-function-with-output-field path. Adding a new TextEmbedding output field
   to an internal collection remains unsupported because the schema-bump
   compactor cannot materialize it for historical segments.
9. No attempt to define final read-path semantics for schema-visible but
   uncovered fields. This includes whether the request returns a field-not-ready
   error, an index-not-loaded error, or another existing execution-path result;
   [#50416](https://github.com/milvus-io/milvus/issues/50416) owns that
   normalization.

## API Model

### Unified Schema Evolution API

All external schema mutations use `AlterCollectionSchema`:

```protobuf
message AlterCollectionSchemaRequest {
  message FieldInfo {
    schema.FieldSchema field_schema = 1;
  }

  message AddRequest {
    repeated FieldInfo field_infos = 1;
    repeated schema.FunctionSchema func_schema = 2;
    bool do_physical_backfill = 3;
  }

  message DropRequest {
    oneof identifier {
      string field_name = 1;
      int64 field_id = 2;
      string function_name = 3;
    }
  }

  message Action {
    oneof op {
      AddRequest add_request = 1;
      DropRequest drop_request = 2;
    }
  }

  Action action = 5;
}
```

The external interpretation is:

| Request shape | Operation |
|---------------|-----------|
| `AddRequest.field_infos` has exactly one field and `func_schema` is empty | Add source-backed field |
| `AddRequest.field_infos` contains exactly one new output field and `func_schema` has exactly one function | Add function or add BM25 |
| `DropRequest.field_name` or `DropRequest.field_id` | Drop source-backed field |
| `DropRequest.function_name` | Drop function or drop BM25 |

`do_physical_backfill=true` is rejected for external collections. Physical
materialization is driven by `RefreshExternalCollection`. For internal
collections the flag is accepted but does not select a separate materialization
path; nullable/default handling and schema-bump compaction keep their existing
behavior.

### Legacy APIs

`AddCollectionField`, `AddCollectionFunction`, and `DropCollectionFunction` keep
their existing compatibility behavior. New external table SDK helpers should
prefer `AlterCollectionSchema` so the external add/drop surface is symmetric.

Existing clients that still call `AddCollectionField` for external
source-backed fields may remain supported as a compatibility path, but the
canonical external schema-evolution path is `AlterCollectionSchema`.

### Refresh Metadata

Refresh jobs must persist the schema version used by the job. Existing proto
messages previously removed schema-version fields and reserved those field
numbers, so new field numbers must be used:

```protobuf
message ExternalCollectionRefreshJob {
  reserved 12;  // removed: schema_version
  int32 schema_version = 13;
}

message ExternalCollectionRefreshTask {
  reserved 18;  // removed: schema_version
  int32 schema_version = 19;
}
```

`RefreshExternalCollectionTaskRequest.schema.version` remains the worker
execution snapshot. The job/task metadata version is the durable commit guard
used after restart and during job aggregation.

## Behavior Matrix

| Operation | API | Schema result | Materialization result | Loaded serving result |
|-----------|-----|---------------|------------------------|-----------------------|
| Add source-backed field | `AlterCollectionSchema(AddRequest)` | Adds a nullable source-backed field with `external_field`. | Refresh patches or rebuilds external manifests so active segments carry explicit coverage for the new source column. | Existing queries on old fields continue. Reads and index creation for the new field are not guaranteed until refresh updates segment coverage. |
| Add function | `AlterCollectionSchema(AddRequest)` | Adds exactly one built-in function and one new output field. | Refresh computes the generated output column and writes it into external manifests. | Existing queries continue. The output field becomes usable after refresh, index build, and target reopen/load. |
| Add BM25 | `AlterCollectionSchema(AddRequest)` | Adds a BM25 function and sparse vector output field. | Refresh computes sparse vectors and BM25 stats. | BM25 search becomes usable after output columns, BM25 stats, index build, and loaded-target reopen are ready. |
| Drop source-backed field | `AlterCollectionSchema(DropRequest)` | Removes the field from schema and records `DroppedFieldIds`. | No physical source rewrite. Future refresh ignores the dropped field. | New query plans cannot reference the field. QueryNode drops loaded index/cache state for the field. |
| Drop function | `AlterCollectionSchema(DropRequest)` | Removes the function and its output field. | The generated column remains on storage but becomes unreachable. | The output field cannot be referenced. Its loaded index, stats references, and caches are invalidated. |
| Drop BM25 | `AlterCollectionSchema(DropRequest)` | Removes the BM25 function and sparse output field. | BM25 stats remain on storage but become unreachable. | BM25 search on the dropped output field fails after schema update. Input text field remains queryable. |

## Internal Collection Compatibility

`AlterCollectionSchema` is already the internal add-function-field entry point.
The implementation must branch on collection type before applying
external-specific validation or materialization rules.

| Area | Internal collection | External collection |
|------|---------------------|---------------------|
| Add ordinary field through `AlterCollectionSchema` | Allowed when exactly one nullable field is provided without `external_field`. The legacy `AddCollectionField` API remains supported. Historical rows use null/default filling. | Allowed when `func_schema` is empty, exactly one nullable field is provided, and it has `external_field`. Historical rows are not default-filled. |
| Add BM25 or MinHash with a new output field | Allowed. Requires StorageV3, schema-bump compaction, storage-version compaction, a VARCHAR input for historical backfill, and bound vector index parameters. | Allowed. Metadata changes immediately; refresh computes output columns and BM25 stats when required. |
| Add TextEmbedding with a new output field | Rejected because internal schema-bump compaction does not materialize TextEmbedding outputs. | Allowed because refresh-time execution supports TextEmbedding. |
| Add function using an existing output field | Rejected; `AlterCollectionSchema` requires the function and its new output field in one request. | Rejected; `AlterCollectionSchema` requires the function and its new output field in one request. |
| Drop function while keeping output fields | Rejected; `AlterCollectionSchema` always removes the function together with its output fields. | Rejected; `AlterCollectionSchema` always removes the function together with its output fields. |
| `do_physical_backfill` | Accepted but not persisted as a materialization selector. Existing null/default and schema-bump behavior applies. | Rejected when true; refresh is the only materialization path. |
| Loaded collection behavior | Keep existing online schema-evolution behavior. | Support online schema mutation with materialization gating and online target reload. |
| Field class checks | No `external_field` requirement. Function outputs are ordinary Milvus-owned fields marked as function outputs. | Source-backed fields require `external_field`; function outputs must not define it. |
| Schema bump compaction | Internal stale sealed segments may be reconciled by `BumpSchemaVersionCompaction`. | Must be skipped. External refresh owns materialization. |

## Field Classes

External schema validation distinguishes three classes:

| Field class | `external_field` | Data source | Storage column name |
|-------------|------------------|-------------|---------------------|
| Source-backed user field | Required | External files | `external_field` value |
| Function output field | Forbidden | Milvus generated data | decimal field ID |
| Virtual/system field | Forbidden | Computed internally | internal only |

This classification must hold both at collection creation and after every
schema mutation.

## Visibility And Materialization

External loaded alter requires field-level readiness instead of relying only on
collection schema version.

### Definitions

| State | Meaning | Owner |
|-------|---------|-------|
| Schema-visible | The latest collection schema contains the field/function. | RootCoord |
| Durable-materialized | Every active healthy DataCoord segment contains the required manifest coverage for the field. | DataCoord |
| Serving-materialized | Every QueryCoord current-target segment in every loaded replica reports a manifest and index/stats state that can serve the field. | QueryCoord/QueryNode |
| Queryable | The field is schema-visible and serving-materialized, or the operation does not require physical field data. | Proxy/QueryNode |

### Field Coverage

The current implementation represents durable field coverage in committed
segment metadata:

1. Fake binlog `ChildFields` lists every field materialized by the segment.
2. For source-backed fields, refresh patches the manifest and then rebuilds the
   fake binlog coverage using the current schema.
3. For generated function outputs, refresh requires the numeric output-field
   column in the manifest and fake-binlog coverage for the field ID.
4. BM25 outputs additionally require a non-empty BM25 statslog for each output
   field.
5. DataNode may inspect child fields and manifests while deciding whether a
   segment can be reused. DataCoord `CreateIndex` uses fake-binlog coverage and
   does not rescan external source files.

QueryCoord does not independently scan manifests to infer field
materialization. DataCoord is the index-metadata entry point and rejects an
explicit index request for an uncovered external field. Once legal index
metadata and index files exist, QueryCoord is responsible for loading them.
Manifest-path and data-version changes independently trigger segment reopen.

### Load Before Refresh Contract

An external collection is allowed to stay loaded, or to be loaded again, after
an add-field or add-function schema mutation and before
`RefreshExternalCollection` completes. Load success only means that the current
serving target can serve already-materialized fields. It does not imply that
every schema-visible field is physically present in every loaded external
manifest.

The load path must therefore separate schema installation from field
materialization:

1. QueryCoord may build a target whose external segment manifests are older
   than the latest collection schema.
2. QueryNode may load those external manifests and install the latest schema.
3. Segcore must not use the internal add-field default/null filling path for
   external fields.
4. Requests that only reference already-available fields execute normally.
5. Generated function outputs generally require refresh before useful reads.
6. Source-backed fields are not guaranteed to be readable before refresh. The
   current Go E2E expects the uncovered field read to fail while existing fields
   remain available.
7. Requests that reference schema-visible but unavailable fields use existing
   execution-path errors until [#50416](https://github.com/milvus-io/milvus/issues/50416)
   normalizes the read-path contract.

This contract intentionally differs from internal collection add-field
behavior. Internal historical rows can use nullable/default filling because
the field did not exist when those rows were written. External source-backed
fields may already have real values in the external files; this PR does not
synthesize nulls for missing external data.

### Query Semantics

| Request type | Added but not serving-materialized field | Dropped field |
|--------------|------------------------------------------|---------------|
| `DescribeCollection` | Shows the field/function because schema mutation succeeded. | Does not show the field/function. |
| Query output field | Uses the existing read-path error until refresh materializes the field and the refreshed segment is loaded. | Fails normal field-not-found validation. |
| Filter expression | Uses the existing read-path error until refresh materializes the field and the refreshed segment is loaded. | Fails normal field-not-found validation. |
| Search field | Generated vector outputs generally require refresh, index build, and loaded-target reopen; pre-ready errors are existing execution-path errors until [#50416](https://github.com/milvus-io/milvus/issues/50416). | Fails normal field-not-found validation. |
| Existing fields | Continue to serve from the current ready target. | Continue normally unless they depend on the dropped function output. |

The nullable requirement for external added fields is still kept as a schema
evolution compatibility rule. It reflects the fact that external files may
contain nulls; it is not a request to synthesize fake null chunks.

## Add Source-Backed Field Flow

1. Client sends `AlterCollectionSchema(AddRequest)` with exactly one
   `field_info` and no `func_schema`.
2. Proxy validates:
   - collection is external;
   - `do_physical_backfill=false`;
   - the field is not a system, virtual, primary key, partition key, clustering
     key, or dynamic field;
   - the field is nullable;
   - the field has `external_field`;
   - no existing source-backed field owns the same `external_field`;
   - the `external_field` value does not collide with generated numeric field-ID
     column names.
3. RootCoord assigns a new field ID, updates `max_field_id`, increments schema
   version, and broadcasts the schema update.
4. Loaded QueryNodes update collection schema through the existing schema update
   broadcast path and set the schema-version load barrier.
5. The new field is schema-visible. Existing loaded fields remain queryable,
   while requests that reference the uncovered new field use the existing
   read-path error until refresh updates the segment manifest.
6. User triggers `RefreshExternalCollection`.
7. DataNode refresh patches reusable segment manifests with the missing source
   column or rebuilds segments when needed.
8. DataCoord atomically commits refreshed segment metadata with the refresh
   schema version.
9. QueryCoord observes manifest changes and schedules online segment reopen or
   load/release actions.
10. QueryCoord marks the next target current only after all loaded replicas
    report matching or newer manifests.
11. The field becomes queryable.

## Add Function Flow

1. Client sends `AlterCollectionSchema(AddRequest)` with exactly one
   `FunctionSchema` and exactly one newly-created output field in
   `field_infos`.
2. Proxy loads the current collection schema.
3. Proxy validates that `field_infos.field_schema.name` exactly matches the
   single `FunctionSchema.OutputFieldNames` entry.
4. Proxy marks the output field as `IsFunctionOutput=true` before external
   schema validation.
5. Proxy validates the merged schema:
   - function type is supported for external collections;
   - input fields exist and have valid types;
   - the output field has a valid type;
   - the output field is newly added;
   - the output field does not define `external_field`;
   - ordinary source-backed fields still define `external_field`;
   - no duplicate external source column owners exist;
   - generated numeric field-ID column names do not collide with
     source-backed `external_field` values.
6. RootCoord assigns a new field ID and a new function ID.
7. RootCoord updates `max_field_id`, increments schema version, and broadcasts
   the new schema.
8. Loaded QueryNodes update collection schema. The new output field is
   schema-visible, but useful reads generally require refresh and target reload.
9. User triggers `RefreshExternalCollection`.
10. DataNode refresh reads source input columns, runs functions through the
    function execution path, and writes only generated output columns.
11. DataCoord commits refreshed segment metadata if the refresh schema version
    still matches the current collection schema version.
12. QueryCoord reloads the affected loaded segments and switches target after
    readiness checks pass.
13. The output field becomes queryable. Vector search additionally requires
    index readiness if the field uses an index.

### Supported Add Function Types

| Function type | Inputs | Outputs | Notes |
|---------------|--------|---------|-------|
| BM25 | one text field | one sparse vector field | The add-function-field path requires one input and one output. Output is not raw-retrievable. BM25 stats are written during refresh. |
| MinHash | one text field | one binary vector field | Output can be retrieved and indexed with MinHash index. |
| TextEmbedding | one text field | embedding vector output accepted by the provider checker | External source-backed text input may be nullable at the schema level because external collections normalize source fields to nullable. Refresh still fails if actual input rows contain unsupported empty/null text. Output can be retrieved and indexed as normal vector data after refresh. |

## Drop Field Flow

1. Client sends `AlterCollectionSchema(DropRequest)` by `field_name` or
   `field_id`.
2. Proxy validates the target field:
   - field exists;
   - field is not a system, virtual, primary key, partition key, clustering key,
     or dynamic field;
   - dropping it would not remove the last vector field;
   - no function references it as input or output.
3. RootCoord builds a schema without the target field.
4. RootCoord updates `max_field_id`, increments schema version, and broadcasts
   the new schema with `DroppedFieldIds`.
5. RootCoord cascades index metadata drops for the removed field ID.
6. Loaded QueryNodes update collection schema.
7. QueryNode invalidates loaded indexes, stats references, raw-data caches, and
   plan caches associated with the dropped field.
8. New query plans cannot reference the dropped field. Old manifest columns may
   remain on storage but are schema-invisible.

Dropping a function output field directly is rejected. Users must drop the
owning function so the function and all output fields are removed atomically.

## Drop Function Flow

1. Client sends `AlterCollectionSchema(DropRequest function_name)`.
2. Proxy validates the target function exists.
3. Proxy verifies that cascade removal of output fields would not leave the
   collection without any vector field.
4. RootCoord removes the function from `schema.Functions`.
5. RootCoord removes every output field from `schema.Fields`.
6. RootCoord records all output field IDs in `DroppedFieldIds`.
7. RootCoord updates `max_field_id`, increments schema version, and broadcasts
   the new schema.
8. RootCoord cascades index metadata drops for all output fields.
9. Loaded QueryNodes update schema and invalidate loaded indexes, BM25 stats
   references, raw-data caches, and plan caches for all output fields.
10. Existing generated column groups and stats remain on storage but are no
    longer referenced by schema or new query plans.

BM25 is handled by the same drop-function flow. The input text field remains a
normal source-backed field.

## Loaded Collection Protocol

Loaded support requires coordinated behavior across schema broadcast, refresh
commit, target observation, and QueryNode segment reload.

### Alter While Loaded

1. Proxy no longer rejects loaded external collections solely because they are
   loaded.
2. Proxy does not reject schema mutation merely because an external refresh job
   is active. Refresh jobs are bound to the schema version captured at
   submission time; if schema changes while a refresh is running, the refresh
   job fails before committing stale segment metadata.
3. RootCoord serializes schema mutation through the existing DDL queue and
   collection lock.
4. RootCoord broadcasts schema updates to the control channel and collection
   vchannels.
5. QueryNode consumes the schema update and calls `UpdateSchema`.
6. The delegator schema-version barrier prevents concurrent segment loads using
   an older collection schema after the schema event is observed.
7. Existing fields remain queryable. Added source-backed fields and generated
   function outputs become reliably queryable after refresh and the required
   segment/index reopen or load finishes. Dropped fields become invalid for new
   plans after schema propagation.

### Refresh While Loaded

1. Refresh submission records the current collection schema version.
2. DataNode computes updated manifests and generated columns against that schema
   snapshot.
3. DataCoord checks the current collection schema version before dispatching
   work and before applying results. Early checks reduce wasted work; the final
   commit check is the correctness boundary.
4. If the version changed, the refresh job fails and leaves existing segment
   metadata unchanged. The user reruns refresh against the new schema.
5. If the version matches, DataCoord atomically updates active segment metadata:
   - new segment IDs for rebuilt segments;
   - patched manifest paths for reusable segments;
   - fake binlogs that cover the materialized field IDs;
   - the refresh schema version on every patched or rebuilt segment;
   - storage version and data version updates as needed.
   Unchanged kept segments preserve their actual older schema version.
6. QueryCoord target observer sees the DataCoord segment metadata change and
   builds a next target.
7. For patched segments that keep the same segment ID but receive a newer
   manifest path, QueryCoord schedules `LoadScope_Reopen` or an equivalent
   online reload action. For rebuilt segments, QueryCoord loads new segments and
   releases old ones after replacement is ready.
8. QueryNode loads the new manifest into a replacement segment object. The old
   segment remains serving until the new segment is ready.
9. QueryNode reports the loaded manifest path and data version in distribution.
10. QueryCoord compares distribution manifest paths and target manifest paths.
    A segment is ready only when the distribution manifest is equal to or newer
    than the target manifest and data version checks pass.
11. QueryCoord promotes the next target to current only after all required
    segments are ready on every loaded replica.
12. Added fields become serving-materialized.

### Failure Handling

| Failure | Required behavior |
|---------|-------------------|
| QueryNode fails to reopen a refreshed segment | Keep the old current target serving already-available fields. QueryCoord retries reload. Generated outputs added by the failed refresh are not reliably queryable. |
| Refresh job fails before commit | No segment metadata changes. Already-available fields continue to serve. Generated outputs from that refresh are not reliably queryable. |
| Refresh commit detects schema version mismatch | Fail the refresh job and leave existing segment metadata unchanged. |
| QueryCoord restart during target switch | Rebuild current and next target from DataCoord segment metadata and QueryNode distribution. Manifest comparison prevents premature readiness. |
| QueryNode restart after alter | Load uses the latest collection schema and target segment metadata. The loaded manifest still controls whether added-field data is available. |

## QueryCoord And QueryNode Changes

### QueryCoord

QueryCoord must treat external manifest changes as data changes for loaded
collections:

1. Include segment manifest path, data version, and index/stats info in target
   comparison.
2. Schedule online reload when a target segment has the same segment ID but a
   newer manifest path or data version than the loaded distribution.
3. Keep the current target serving until all next-target segments are ready.
4. Use manifest comparison for readiness. A loaded segment with an older
   manifest than the target is not ready.
5. Schedule index reopen from existing index metadata and available index
   files. Do not suppress a legal index load merely because the segment schema
   version is older than the collection schema version.

### QueryNode

QueryNode must support schema and manifest changes independently:

1. `UpdateSchema` updates the loaded collection schema and segcore schema.
2. `UpdateSchema` invalidates plan caches that were built from an older schema.
3. Drop updates remove loaded index state for dropped field IDs and cascade
   function output field IDs.
4. Drop updates remove or ignore BM25 stats references for dropped output
   fields.
5. Segment reload for external manifests is atomic: build the replacement from
   the new manifest first, then swap into distribution.
6. Segment execution uses external manifests and generated column metadata for
   external fields instead of internal add-field default filling.
7. External collection execution must bypass the internal default/null filling
   path used for internal add-field schema evolution.
8. Exact read-path behavior for schema-visible but uncovered fields is tracked
   by [#50416](https://github.com/milvus-io/milvus/issues/50416).

## Refresh Interaction

Refresh is the only path that materializes added source-backed fields and
function outputs for external collections.

After add source-backed field:

1. Existing external segments may not contain the field's manifest column
   mapping.
2. Refresh checks each reusable segment for missing source-backed columns.
3. Segments missing the source column are patched by appending manifest column
   mappings when the source fragments are unchanged, or rebuilt when needed.
4. DataCoord commits updated manifest paths and fake binlogs.

After add function or add BM25:

1. Existing external segments may not contain the new output field ID column.
2. Refresh checks whether each reusable segment manifest contains every
   required function output column.
3. Segments missing required output columns are invalidated or patched.
4. Their fragments become orphan fragments when rebuild is required.
5. DataNode creates or patches segments, computes outputs, writes generated
   columns, writes BM25 stats when needed, and commits a new manifest.

After drop field or drop function:

1. The dropped fields disappear from schema immediately.
2. Old source columns, generated output columns, and stats may still exist in
   old manifests.
3. Query and load use schema-driven filtering and do not request dropped
   fields.
4. Future refresh jobs build manifests using the new schema.

## Concurrency And Versioning

Schema mutation and refresh must not commit incompatible metadata.

External schema evolution still increments the collection schema version in
RootCoord. This is required for metadata ordering, cache invalidation, and
refresh version guards. The external path must not try to keep the collection
schema version unchanged.

What external collections must avoid is the internal segment reconciliation
path that treats stale segment schema versions as work for
`BumpSchemaVersionCompaction`. For internal collections, that compaction can
rewrite or patch old flushed segments so their `SegmentInfo.SchemaVersion`
catches up with the latest collection schema. For external collections,
`RefreshExternalCollection` owns physical materialization. A generic schema bump
compaction must not rewrite external manifests or generated columns.

The rules are:

1. `AlterCollectionSchema` is serialized by the existing DDL queue and
   RootCoord collection lock.
2. `RefreshExternalCollection` records the collection schema version at job
   submission.
3. DataCoord persists that version in both refresh job and refresh task
   metadata using new proto field numbers.
4. The DataNode worker receives that schema and computes segment updates against
   it.
5. DataCoord checks the current collection schema version against the durable
   job schema version before committing the refresh result.
6. If the version changed, the refresh job fails with a retriable
   schema-changed error. Users rerun refresh.
7. DataNode writes every refreshed or patched external segment with
   `SegmentInfo.SchemaVersion = schema.version`. Unchanged kept segments retain
   their real schema version.
8. DataCoord excludes external collections from
   `BumpSchemaVersionCompaction` scheduling.
9. `AlterCollectionSchema` is allowed while a refresh job for the same
   collection is active. Active refresh jobs whose captured schema version no
   longer matches the current collection schema fail before committing results.
10. DataCoord may optionally cancel or drop worker tasks for stale refresh jobs
    after a schema change is observed. This is a resource optimization, not the
    correctness mechanism.

### Avoiding Internal Schema Bump Side Effects

The implementation should use two explicit guards:

1. **Policy guard**: `bumpSchemaVersionPolicy.Trigger()` skips collections where
   `collection.IsExternal()` is true before scanning stale flushed segments.
   This prevents noisy views and avoids scheduling work that refresh owns.
2. **Submit guard**: `SubmitBumpSchemaVersionViewToScheduler()` keeps a second
   `collection.IsExternal()` check before allocating IDs or creating a
   compaction task. This protects future callers that might bypass the policy
   trigger.

With these guards, external schema mutation can safely bump the collection
schema version for metadata correctness while leaving old external segments
untouched until refresh. Before refresh, old external segments may have an older
`SegmentInfo.SchemaVersion`; they remain physically unchanged. Drop operations
are hidden by the latest schema, while newly added fields are not considered
serving-materialized until refresh and target switch complete.

## Index And Stats Handling

| Operation | Index behavior | Stats behavior |
|-----------|----------------|----------------|
| Add source-backed field | Creating an index on the new field is rejected until durable materialization is complete for every active segment. Loaded index becomes usable only after QueryCoord loads it. | No generated stats are required unless the field has scalar or JSON stats support. |
| Add BM25 | The DDL creates bound index metadata, but the index inspector skips segments whose fake binlogs do not cover the output field. An explicit index request is rejected until refresh writes sparse output and BM25 stats. | Refresh writes `bm25.<fieldID>` stats into manifest. QueryNode cannot serve BM25 search until stats are loaded. |
| Add MinHash/TextEmbedding | Bound index metadata is created with the field, but the index inspector skips segments until fake-binlog coverage includes the output field. An explicit index request is rejected before materialization. | No BM25 stats required. |
| Drop source field | RootCoord cascades index metadata drops on that field. QueryNode drops loaded index state and caches. | Existing source data remains in external files. |
| Drop function/BM25 | RootCoord cascades indexes on all output fields. QueryNode drops loaded index state and BM25 stats references. | Existing generated stats remain on storage but are no longer reachable. |

DataCoord owns external field coverage validation for index creation and build
scheduling. QueryCoord consumes the resulting index metadata and files and
loads or reopens the index without re-deriving materialization from collection
schema version. A successful schema mutation alone is not enough to build or
load an index on an added external field.

## SDK And REST Surface

SDKs should expose external schema evolution through helpers that build
`AlterCollectionSchema` requests:

```python
client.add_external_collection_field(
    collection_name,
    field_schema,
    external_field="category",
)

client.add_collection_function(
    collection_name,
    function,
    output_fields=[...],
)

client.drop_collection_field(collection_name, field_name="score")
client.drop_collection_function(collection_name, function_name="bm25_fn")
```

These helpers must invalidate collection schema cache on success. This applies
to add field, add function, drop field, and drop function.

REST can follow the same logical surface:

| REST action | Internal RPC |
|-------------|--------------|
| `/collections/fields/add` | `AlterCollectionSchema(AddRequest)` for external collections |
| `/collections/functions/add_with_output_fields` | `AlterCollectionSchema(AddRequest)` |
| `/collections/fields/drop` | `AlterCollectionSchema(DropRequest)` |
| `/collections/functions/drop` | `AlterCollectionSchema(DropRequest)` for external collections |

The existing `/collections/add_function` and `/collections/drop_function`
endpoints keep legacy semantics for non-external collections unless separately
migrated.

## Error Handling

| Case | Error |
|------|-------|
| Add source-backed field without `external_field` | `add field operation on external collection requires external_field mapping, field name = {field}` |
| Add source-backed field that is not nullable | `added field must be nullable, please check it, field name = {field}` |
| External add function without an output field | `external collection add function must include its output field` |
| Output field has `external_field` | `function output field {field} in external collection {collection} must not have external_field mapping` |
| Source-backed field lacks `external_field` | `field {field} in external collection {collection} must have external_field mapping` |
| Function output name differs from the new field name | `function output field {output} must be the newly-added field {field}` |
| Function declares multiple inputs or outputs | `{function_type} function should have exactly one input field and exactly one output field` |
| External add with `do_physical_backfill=true` | `external collection does not support physical backfill; run RefreshExternalCollection after schema mutation` |
| Add unsupported function type | `For now, only BM25 and MinHash functions are supported in add_function_field interface` |
| Drop external function without output fields | `external collection function must be dropped with its output field: {function}` |
| Drop field referenced by function input | `field is referenced by function {function} as input, drop function first` |
| Drop field referenced by function output | `field is referenced by function {function} as output, drop function first` |
| Drop function removes last vector field | `cannot drop function {function}: it would leave no vector field in the collection` |
| Refresh commits after schema changed | `external collection schema changed during refresh; rerun refresh` |
| Query references added field before refresh and target switch | Request fails; exact error is tracked by [#50416](https://github.com/milvus-io/milvus/issues/50416). |
| Create index on added field before refresh | `external field {field} is not materialized; run RefreshExternalCollection before creating index` |

## Implementation Summary

1. **Proxy validation**
   - Use `AlterCollectionSchema` for external add field, add function, add BM25,
     drop field, drop function, and drop BM25.
   - Remove the unconditional loaded external collection reject.
   - Branch validation by collection type before applying external-only rules.
   - Keep internal `AlterCollectionSchema(AddRequest)` ordinary-field,
     BM25, and MinHash behavior unchanged.
   - For external collections, allow BM25, MinHash, and TextEmbedding
     add-function requests.
   - For external source-backed add field, require exactly one nullable field
     with `external_field` and no function schema.
   - For external add function, require exactly one new output field matching
     the function's single output name.
   - Mark add-request output fields as function outputs before validating the
     merged external schema.
   - Reject direct drop of function output fields.

2. **RootCoord schema builder**
   - Branch function-type allowlists by collection type.
   - Preserve the existing internal BM25 and MinHash add-function-field path.
   - Reuse field ID and function ID assignment logic.
   - Preserve `max_field_id` high-water behavior.
   - Broadcast dropped field IDs for field and function drops.

3. **Refresh version guard**
   - Persist schema version in refresh job/task metadata using non-reserved
     field numbers.
   - Store the collection schema version when the refresh job is submitted.
   - Copy that version into each refresh task and worker request schema
     snapshot.
   - Reject refresh commit if the current collection schema version differs
     from the persisted job schema version.
   - Commit refreshed or patched external segments with the schema version used
     by the refresh task.

4. **Materialization state**
   - Record durable field coverage in fake-binlog `ChildFields`.
   - Let DataNode inspect child fields and manifests when reusing or rebuilding
     external segments.
   - Let DataCoord reject explicit index creation and defer bound-index builds
     when a required field is absent from segment coverage.
   - Keep QueryCoord focused on loading existing legal index metadata and
     reopening changed manifests.

5. **QueryCoord loaded reload**
   - Detect changed external manifest paths, data versions, and index/stats
     readiness in target comparison.
   - Schedule `LoadScope_Reopen` or equivalent reload for same-segment-ID
     manifest updates.
   - Keep old target serving until new target passes manifest/data-version
     readiness checks.
   - Promote next target only after every loaded replica is ready.

6. **QueryNode online schema and manifest handling**
   - Update segcore schema when schema update messages arrive.
   - Invalidate plan cache on schema version change.
   - Drop loaded index/cache/stats state for `DroppedFieldIds`.
   - Filter dropped fields and skip external default/null filling for uncovered
     fields.
   - Atomically replace external segments after reopen succeeds.

7. **SDK and REST helpers**
   - Add helpers that build `AlterCollectionSchema` requests.
   - Invalidate schema cache on successful schema mutation.
   - Surface materialization errors with refresh-oriented guidance.

8. **Tests**
   - Add focused unit tests for validation and schema builders.
   - Add QueryCoord/QueryNode unit tests for manifest reopen and readiness.
   - Add external collection E2E tests for loaded add/drop flows.

## Test Matrix

| Layer | Test case |
|-------|-----------|
| Proxy unit | External add source-backed field through `AlterCollectionSchema` succeeds when nullable and mapped. |
| Proxy unit | External add source-backed field without `external_field` fails. |
| Proxy unit | External alter does not fail solely because a refresh job is active. |
| Proxy unit | External add BM25 with output field lacking `external_field` succeeds. |
| Proxy unit | External add BM25 with output field defining `external_field` fails. |
| Proxy unit | External add MinHash and TextEmbedding validate input/output fields. |
| Proxy unit | Internal add BM25 keeps existing validation and does not require `external_field`. |
| Proxy unit | Internal add BM25/MinHash with a new output field remains allowed, while TextEmbedding with a new output field is rejected. |
| Proxy unit | External `do_physical_backfill=true` is rejected with a refresh-oriented error. |
| Proxy unit | External function output pointing to an existing field fails; the internal non-BM25 function-only compatibility path remains allowed. |
| Proxy unit | The function output name differing from the single new field name fails. |
| Proxy unit | A supported function declaring anything other than one input and one output fails. |
| Proxy unit | Loaded external collection no longer rejects solely due to loaded state. |
| Proxy unit | Drop source field referenced by a function fails. |
| Proxy unit | Drop function that removes the last vector field fails. |
| RootCoord unit | Add source-backed field assigns a new field ID and updates `max_field_id`. |
| RootCoord unit | Add function assigns a new field ID and function ID, updates `max_field_id`. |
| RootCoord unit | Internal and external add-function requests use different function-type allowlists. |
| RootCoord unit | Drop function removes function and output fields, reports `DroppedFieldIds`. |
| DataCoord unit | Refresh job/task metadata persists schema version using non-reserved field numbers. |
| DataCoord unit | Refresh commit fails if current collection schema version differs from job schema version. |
| DataCoord unit | Bump schema version policy skips external collections with stale segment schema versions. |
| DataCoord unit | Fake-binlog coverage reports a source-backed added field missing before refresh. |
| DataCoord unit | Fake-binlog coverage reports generated output fields present after refresh. |
| QueryCoord unit | Same segment ID with newer manifest path schedules reopen. |
| QueryCoord unit | Current target is not promoted while distribution manifest is older than target manifest. |
| QueryCoord unit | Current target is promoted after every replica reports target manifest readiness. |
| QueryNode unit | `UpdateSchema` invalidates plan cache and updates segcore schema. |
| QueryNode unit | Dropped field removes loaded indexes and stats references. |
| QueryNode unit | Reopen loads new manifest and swaps atomically after success. |
| QueryNode/Segcore unit | Loading an external segment whose manifest misses a schema-visible added field does not use internal default/null chunk filling. Exact read-path errors are tracked by [#50416](https://github.com/milvus-io/milvus/issues/50416). |
| QueryNode unit | Loading an external segment whose manifest misses a schema-visible added field succeeds. |
| QueryNode unit | External missing-field execution does not use internal default/null chunk filling. |
| DataNode unit | Segment missing newly added source-backed columns is patched. |
| DataNode unit | Segment missing newly added function output columns is rebuilt or patched. |
| DataNode unit | Refreshed or patched external segments carry the refresh schema version. |
| E2E | Load external collection, add source-backed field, query existing fields succeeds, query the uncovered field fails, refresh, reload, and query the added field succeeds. |
| E2E | Load external collection, add BM25, old vector search succeeds, refresh, index, wait target ready, BM25 search succeeds. |
| E2E | Load external collection, add MinHash, refresh, index, wait target ready, search and retrieve output. |
| E2E | Add TextEmbedding to an external collection, refresh, index, load, search, and retrieve output. |
| E2E | Drop BM25 while unloaded, reload, verify BM25 search fails and an unrelated vector field remains searchable. |
| E2E | Load external collection, drop source field, verify output fields and filters no longer accept the field. |
| E2E | Start refresh, run alter, verify alter succeeds and the old-schema refresh job fails with schema-changed error. |
| E2E | Start alter after refresh submit race, verify refresh commit fails with schema-changed error and leaves segment metadata unchanged. |

### Current Go E2E Coverage

The current Go client suite has six top-level external schema-evolution E2E
tests:

1. `TestExternalTableSchemaEvolutionBM25`
2. `TestExternalTableSchemaEvolutionMinHash`
3. `TestExternalTableSchemaEvolutionTextEmbedding`
4. `TestExternalTableLoadedAddFieldQueryAcrossRefresh`
5. `TestExternalTableLoadedBM25QueryAcrossRefresh`
6. `TestRefreshExternalCollectionAfterAddColumnReturnsCorrectData`

Their operation-level coverage is:

| Scenario | Internal collection | External collection |
|----------|---------------------|---------------------|
| Add ordinary field | 12 top-level tests exercise `AddCollectionField`: 10 positive and 2 validation/boundary tests. Seven positive tests exercise pre-existing data. No internal E2E calls `AlterCollectionSchema` directly. | Three positive scenarios: two through `AlterCollectionSchema` and one legacy `AddCollectionField` compatibility case. |
| Add function with a new output field | No Go E2E. | Four scenarios: BM25 twice, MinHash once, and TextEmbedding once. |
| Drop ordinary field | No Go E2E. | Two scenarios, including one loaded collection case. |
| Drop function with output fields | No Go E2E. | Four scenarios covering BM25, MinHash, and TextEmbedding. The mutation itself is currently executed while unloaded. |
| Loaded add/refresh | Existing internal add-field tests cover loaded query/search behavior. | Loaded source-field add/drop, loaded BM25 add, and loaded MinHash add are covered. Loaded TextEmbedding add is not covered. |

Remaining Go E2E gaps are:

1. Internal `AlterCollectionSchema` add/drop field and function flows.
2. Internal schema-bump compaction backfill for BM25 and MinHash.
3. Dropping an external function while the collection remains loaded.
4. Online source-field refresh followed by query without release/reload.
5. Explicit index creation rejection before external field materialization.
6. Schema alter racing with refresh dispatch or atomic refresh commit.
7. Schema evolution for the `milvus-table` external format.

## Compatibility

Existing external collections continue to work. Collections that already have
create-time function outputs keep the same refresh and query behavior.

Existing clients that use `AddCollectionField` for external source-backed fields
continue to work if compatibility is preserved, provided they set
`external_field`. New helpers should use `AlterCollectionSchema`.

Legacy function mutation APIs are unchanged. This avoids silently changing
`DropCollectionFunction` from "remove the function only" into "cascade delete
output fields".

Loaded support is backward-compatible for existing unloaded workflows:

1. release;
2. alter schema;
3. refresh if fields or outputs were added;
4. create or drop indexes as needed;
5. load again.

The loaded workflow removes the release requirement but keeps refresh explicit.

## Future Work

1. Automatically trigger refresh after add field or add function and return a
   refresh job ID.
2. Expose field-level materialization status in `DescribeCollection` or a
   dedicated progress API.
3. Add physical garbage collection for generated columns and stats that are no
   longer referenced by any live schema.
4. Support batch schema evolution requests after single-operation semantics are
   proven stable.
5. Support function output replacement as an explicit drop-then-add workflow
   with clearer SDK ergonomics.
