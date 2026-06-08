# External Table Schema Evolution

- **Created:** 2026-06-05
- **Author(s):** Wei Liu
- **Status:** Draft
- **Branch:** `analyze-drop-field`
- **Component:** Proxy, RootCoord, DataCoord, DataNode, QueryCoord, QueryNode, SDK
- **Related Docs:** `20260105-external_table.md`, `20260129-add-function-field-design.md`, `20260413-drop-collection-field-design.md`, `20260521-external-table-function-output.md`

## Summary

This document extends external collections with online schema evolution through
`AlterCollectionSchema`. The target API supports the following operations while
the external collection is unloaded or loaded:

- add a source-backed field;
- add a built-in function with new output fields;
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
`BumpSchemaVersionCompaction` for external collections. Added external fields
and function outputs become queryable only after `RefreshExternalCollection`
materializes them and QueryCoord finishes reloading the affected loaded
segments. External collections may still be loaded before refresh completes.
By default, any request that reads, filters, searches, or indexes an
unmaterialized field fails with a refresh-oriented error instead of returning
null or partial results. A coarse QueryNode escape hatch,
`queryNode.externalCollection.allowUnmaterializedFieldAccess`, can skip the
read-path materialization gate and let the storage path return its legacy null
behavior. Drop operations become effective through schema filtering and index
cache invalidation.

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
   metadata is updated and broadcast. Added external fields and function
   outputs are visible in schema immediately but are not queryable until
   refresh materializes them and loaded replicas switch to the refreshed target.
4. **No false data by default**: external added fields must not silently return
   null when the external source column exists but the current manifest has not
   been patched. Requests that reference unmaterialized external fields fail
   with a refresh-oriented error unless the coarse QueryNode compatibility
   switch is explicitly enabled.
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
9. **Online target switch**: loaded collections keep serving the old ready
   target while refresh builds and loads the next target. A field becomes
   queryable only after the next target is ready on every loaded replica.

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
10. Provide deterministic validation and user-visible error messages for
    unmaterialized fields and invalid external schema mutations.

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
8. No expansion of non-BM25 add-function support for internal collections unless
   their write, import/backfill, query, and index paths are implemented and
   verified for historical data.
9. No attempt to make partially refreshed external fields return partial
   results. A field is queryable only when every serving segment in the loaded
   target can serve it.

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
| `AddRequest.field_infos` contains the new output fields and `func_schema` has exactly one function | Add function or add BM25 |
| `DropRequest.field_name` or `DropRequest.field_id` | Drop source-backed field |
| `DropRequest.function_name` | Drop function or drop BM25 |

`do_physical_backfill=true` is rejected for external collections. Physical
materialization is driven by `RefreshExternalCollection`. Internal collections
keep the existing behavior for this field.

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
| Add source-backed field | `AlterCollectionSchema(AddRequest)` | Adds a nullable source-backed field with `external_field`. | Refresh patches or rebuilds external manifests so active segments cover the new source column. | Existing queries on old fields continue. Requests that reference the new field fail until refresh commits and QueryCoord loads a ready target. |
| Add function | `AlterCollectionSchema(AddRequest)` | Adds exactly one built-in function and all new output fields. | Refresh computes generated output columns and writes them into external manifests. | Existing queries continue. Requests that reference output fields fail until refresh and target switch finish. |
| Add BM25 | `AlterCollectionSchema(AddRequest)` | Adds a BM25 function and sparse vector output field. | Refresh computes sparse vectors and BM25 stats. | BM25 search fails until output columns, BM25 stats, and any required index are ready on the loaded target. |
| Drop source-backed field | `AlterCollectionSchema(DropRequest)` | Removes the field from schema and records `DroppedFieldIds`. | No physical source rewrite. Future refresh ignores the dropped field. | New query plans cannot reference the field. QueryNode drops loaded index/cache state for the field. |
| Drop function | `AlterCollectionSchema(DropRequest)` | Removes the function and all output fields. | Generated columns remain on storage but become unreachable. | Output fields cannot be referenced. Loaded output indexes, stats references, and caches are invalidated. |
| Drop BM25 | `AlterCollectionSchema(DropRequest)` | Removes the BM25 function and sparse output field. | BM25 stats remain on storage but become unreachable. | BM25 search on the dropped output field fails after schema update. Input text field remains queryable. |

## Internal Collection Compatibility

`AlterCollectionSchema` is already the internal add-function-field entry point.
The implementation must branch on collection type before applying
external-specific validation or materialization rules.

| Area | Internal collection | External collection |
|------|---------------------|---------------------|
| Add source-backed field through `AlterCollectionSchema` | Rejected. Internal collections continue to use their existing add-field API and nullable/default rules. | Allowed when `func_schema` is empty, exactly one field is provided, the field is nullable, and it has `external_field`. |
| Add BM25 function | Keep existing add-function-field behavior. New writes generate function output through the normal write/function path. Existing data follows existing backfill or lazy materialization policy. | Metadata changes immediately. Existing segments become usable for the output only after refresh computes generated columns and stats. |
| Add MinHash/TextEmbedding | Rejected unless the internal write, import/backfill, query, and index paths are implemented and verified for historical data. | Allowed because refresh-time execution already supports these outputs for external data. |
| `do_physical_backfill` | Keep existing behavior. | Rejected when true; refresh is the only materialization path. |
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

DataCoord and QueryCoord need a shared resolver for external field coverage:

1. For source-backed fields, coverage means the active segment manifest can map
   the field ID to the requested `external_field` column.
2. For generated function outputs, coverage means the manifest contains the
   generated output column group for the field ID.
3. For BM25 outputs, coverage also requires the BM25 stats referenced by the
   manifest for the output field ID.
4. Coverage must be derived from committed segment metadata, fake binlog child
   field IDs, column group metadata, and manifest paths. The resolver must not
   scan external source files on every query.

The authoritative execution guard lives in QueryNode/Segcore: by default, a
request that references a field not covered by all serving segments must fail
instead of returning partial or misleading results. Proxy and QueryCoord may
add preflight checks for clearer errors, but they are not the only line of
defense. When `queryNode.externalCollection.allowUnmaterializedFieldAccess` is
enabled, QueryNode skips this read-path guard and lets the lower storage path
decide the result, which currently provides the legacy null behavior for
missing external columns.

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
3. Segcore must not synthesize default or null chunks for external fields that
   are missing from the manifest.
4. QueryNode records per-segment field coverage from the loaded manifest,
   generated column groups, BM25 stats, and loaded indexes.
5. Requests that only reference covered fields execute normally.
6. Requests that reference any schema-visible but uncovered field fail the
   whole request with a materialization error unless the coarse QueryNode
   compatibility switch is enabled.

This contract intentionally differs from internal collection add-field
behavior. Internal historical rows can use nullable/default filling because
the field did not exist when those rows were written. External source-backed
fields may already have real values in the external files; before refresh,
Milvus has not proven or loaded those values. Returning null would present an
unknown value as a true null.

### Query Semantics

| Request type | Added but not serving-materialized field | Dropped field |
|--------------|------------------------------------------|---------------|
| `DescribeCollection` | Shows the field/function because schema mutation succeeded. | Does not show the field/function. |
| Query output field | Fails with `external field {field} is not materialized; run RefreshExternalCollection`. | Fails normal field-not-found validation. |
| Filter expression | Fails with the same materialization error. | Fails normal field-not-found validation. |
| Search field | Fails with the same materialization error, or index-not-ready if materialized data exists but index is not ready. | Fails normal field-not-found validation. |
| Existing fields | Continue to serve from the current ready target. | Continue normally unless they depend on the dropped function output. |

By default, Milvus must not return nulls for an unmaterialized external
source-backed field because the external source may contain real values.
Returning null would hide data rather than represent a true nullable value.
The coarse compatibility switch intentionally relaxes this default for users
who prefer availability and legacy null behavior over strict materialization
visibility.

The nullable requirement for external added fields is still kept as a schema
evolution compatibility rule, but it does not grant permission to serve fake
nulls before refresh. Nullable only defines valid values after the field is
materialized.

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
5. The new field is schema-visible but not queryable.
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
   `FunctionSchema` and all newly-created output fields in `field_infos`.
2. Proxy loads the current collection schema.
3. Proxy validates that `field_infos.field_schema.name` exactly matches
   `FunctionSchema.OutputFieldNames` as a set.
4. Proxy marks every output field as `IsFunctionOutput=true` before external
   schema validation.
5. Proxy validates the merged schema:
   - function type is supported for external collections;
   - input fields exist and have valid types;
   - output fields have valid types;
   - output fields are newly added fields;
   - output fields do not define `external_field`;
   - ordinary source-backed fields still define `external_field`;
   - no duplicate external source column owners exist;
   - generated numeric field-ID column names do not collide with
     source-backed `external_field` values.
6. RootCoord assigns new field IDs and a new function ID.
7. RootCoord updates `max_field_id`, increments schema version, and broadcasts
   the new schema.
8. Loaded QueryNodes update collection schema. The new output fields are
   schema-visible but not queryable.
9. User triggers `RefreshExternalCollection`.
10. DataNode refresh reads source input columns, runs functions through the
    function execution path, and writes only generated output columns.
11. DataCoord commits refreshed segment metadata if the refresh schema version
    still matches the current collection schema version.
12. QueryCoord reloads the affected loaded segments and switches target after
    readiness checks pass.
13. Output fields become queryable. Vector search additionally requires index
    readiness if the field uses an index.

### Supported Add Function Types

| Function type | Inputs | Outputs | Notes |
|---------------|--------|---------|-------|
| BM25 | one text field, or text plus analyzer-name field for multi-analyzer | one sparse vector field | Output is not raw-retrievable. BM25 stats are written during refresh. |
| MinHash | one text field | one binary vector field | Output can be retrieved and indexed with MinHash index. |
| TextEmbedding | provider-specific text inputs | embedding vector outputs accepted by the provider checker | Output can be retrieved and indexed as normal vector data after refresh. |

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
7. Added fields remain not queryable until refresh and loaded-target switch
   finish. Dropped fields become invalid for new plans after schema propagation.

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
   - `SegmentInfo.SchemaVersion = schema.version`;
   - storage version and data version updates as needed.
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
| QueryNode fails to reopen a refreshed segment | Keep the old current target serving old materialized fields. QueryCoord retries reload. Added fields remain not queryable. |
| Refresh job fails before commit | No segment metadata changes. Added fields remain not queryable. |
| Refresh commit detects schema version mismatch | Fail the refresh job and leave existing segment metadata unchanged. |
| QueryCoord restart during target switch | Rebuild current and next target from DataCoord segment metadata and QueryNode distribution. Manifest comparison prevents premature readiness. |
| QueryNode restart after alter | Load uses latest collection schema and target segment metadata. Segment field coverage still controls added-field queryability. |

## QueryCoord And QueryNode Changes

### QueryCoord

QueryCoord must treat external manifest changes as data changes for loaded
collections:

1. Include segment manifest path, data version, schema version, and index/stats
   info in target comparison.
2. Schedule online reload when a target segment has the same segment ID but a
   newer manifest path or data version than the loaded distribution.
3. Keep the current target serving until all next-target segments are ready.
4. Use manifest comparison for readiness. A loaded segment with an older
   manifest than the target is not ready.
5. Expose or internally maintain field-level serving materialization state so
   QueryNode and, optionally, Proxy can reject requests that reference
   unmaterialized added fields.

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
6. Segment execution checks field coverage before reading external columns,
   generated columns, indexes, or stats.
7. External collection execution must bypass the internal default/null filling
   path used for internal add-field schema evolution.
8. If a request references an unmaterialized field, QueryNode returns a clear
   schema/materialization error instead of partial results unless the coarse
   compatibility switch is enabled.

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
7. DataNode writes refreshed or patched external segments with
   `SegmentInfo.SchemaVersion = schema.version`.
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
| Add BM25 | Creating or loading the BM25 output index is rejected until refresh writes sparse output and BM25 stats. | Refresh writes `bm25.<fieldID>` stats into manifest. QueryNode must reject BM25 search until stats are serving-ready. |
| Add MinHash/TextEmbedding | Vector index build is rejected until output columns are durable-materialized. | No BM25 stats required. |
| Drop source field | RootCoord cascades index metadata drops on that field. QueryNode drops loaded index state and caches. | Existing source data remains in external files. |
| Drop function/BM25 | RootCoord cascades indexes on all output fields. QueryNode drops loaded index state and BM25 stats references. | Existing generated stats remain on storage but are no longer reachable. |

Index build and index load must use the same external field coverage resolver as
query execution. A successful schema mutation alone is not enough to build or
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
| Add function without output fields | `function output fields are required` |
| Output field has `external_field` | `function output field {field} in external collection {collection} must not have external_field mapping` |
| Source-backed field lacks `external_field` | `field {field} in external collection {collection} must have external_field mapping` |
| Function output missing from `field_infos` | `function output field {field} must be included in AddRequest.field_infos` |
| Function output references existing field | `function output field {field} must be one of the newly-added fields` |
| Added function output field is not referenced by the function | `added function output field {field} is not referenced by function {function}` |
| Duplicate function output field name | `duplicate function output field {field}` |
| External add with `do_physical_backfill=true` | `external collection does not support physical backfill; run RefreshExternalCollection after schema mutation` |
| Add unsupported function type | `unsupported function type for external collection schema evolution: {type}` |
| Drop field referenced by function input | `field is referenced by function {function} as input, drop function first` |
| Drop field referenced by function output | `field is referenced by function {function} as output, drop function first` |
| Drop function removes last vector field | `cannot drop function {function}: it would leave no vector field in the collection` |
| Refresh commits after schema changed | `external collection schema changed during refresh; rerun refresh` |
| Query references added field before refresh and target switch | `external field {field} is not materialized; run RefreshExternalCollection and wait until loaded target is ready` |
| Create index on added field before refresh | `external field {field} is not materialized; run RefreshExternalCollection before creating index` |

## Current Implementation Baseline

Part of this design has already landed on the `analyze-drop-field` branch. The
target design above includes additional work for loaded support.

Already on branch:

- `AlterCollectionSchema(AddRequest)` has an external source-backed add-field
  path when `func_schema` is empty.
- Proxy can distinguish source-backed external add field from function-output
  add.
- RootCoord can assign a new field ID for external add field, update
  `max_field_id`, bump schema version, and broadcast schema/properties.
- Drop field and drop function with output-field cascade and `DroppedFieldIds`
  reporting are implemented in the alter-schema callback.
- Refresh can patch reusable external segments for missing source-backed
  columns and can rebuild segments missing generated function outputs.
- Refreshed or patched external segments carry the refresh schema version.
- DataCoord schema-bump compaction policy has external-collection skip guards.
- Proxy currently rejects loaded external collections before schema evolution.

Still required for loaded support:

- Replace the loaded external collection reject with the loaded protocol in this
  document.
- Ensure active refresh jobs fail cleanly if collection schema version changes
  before their results are committed.
- Add field-level durable and serving materialization checks.
- Ensure QueryCoord schedules online reopen for external segments whose manifest
  path changes without segment ID changes.
- Ensure QueryNode atomically swaps reopened external segments.
- Invalidate QueryNode indexes, BM25 stats references, raw-data caches, and plan
  caches for dropped fields and function outputs.
- Gate query/search/index creation on external field materialization state.
- Add tests for loaded add/drop behavior and refresh-driven target switch.

## Implementation Plan

1. **Proxy validation**
   - Use `AlterCollectionSchema` for external add field, add function, add BM25,
     drop field, drop function, and drop BM25.
   - Remove the unconditional loaded external collection reject.
   - Branch validation by collection type before applying external-only rules.
   - Keep internal `AlterCollectionSchema(AddRequest)` BM25 behavior unchanged.
   - For external collections, allow BM25, MinHash, and TextEmbedding
     add-function requests.
   - For external source-backed add field, require exactly one nullable field
     with `external_field` and no function schema.
   - For external add function, validate exact equality between `field_infos`
     and `FunctionSchema.OutputFieldNames`.
   - Mark add-request output fields as function outputs before validating the
     merged external schema.
   - Reject direct drop of function output fields.

2. **RootCoord schema builder**
   - Branch function-type allowlists by collection type.
   - Preserve the existing internal BM25-only add path.
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
   - Implement a shared external field coverage resolver.
   - Compute durable materialization from active DataCoord segment metadata.
   - Compute serving materialization from QueryCoord current target and
     QueryNode distribution.
   - Gate query/search/index build/index load on materialization state.
   - Keep QueryNode execution checks as the authoritative guard.

5. **QueryCoord loaded reload**
   - Detect changed external manifest paths, data versions, schema versions,
     and index/stats readiness in target comparison.
   - Schedule `LoadScope_Reopen` or equivalent reload for same-segment-ID
     manifest updates.
   - Keep old target serving until new target passes manifest/data-version
     readiness checks.
   - Promote next target only after every loaded replica is ready.

6. **QueryNode online schema and manifest handling**
   - Update segcore schema when schema update messages arrive.
   - Invalidate plan cache on schema version change.
   - Drop loaded index/cache/stats state for `DroppedFieldIds`.
   - Add per-segment field coverage checks.
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
| Proxy unit | Internal add MinHash/TextEmbedding remains rejected. |
| Proxy unit | External `do_physical_backfill=true` is rejected with a refresh-oriented error. |
| Proxy unit | Function output pointing to an existing field fails. |
| Proxy unit | `OutputFieldNames` entry missing from `field_infos` fails. |
| Proxy unit | Extra `field_infos` not referenced by `OutputFieldNames` fails. |
| Proxy unit | Duplicate output field names fail. |
| Proxy unit | Loaded external collection no longer rejects solely due to loaded state. |
| Proxy unit | Drop source field referenced by a function fails. |
| Proxy unit | Drop function that removes the last vector field fails. |
| RootCoord unit | Add source-backed field assigns a new field ID and updates `max_field_id`. |
| RootCoord unit | Add function assigns new field IDs and function ID, updates `max_field_id`. |
| RootCoord unit | Internal and external add-function requests use different function-type allowlists. |
| RootCoord unit | Drop function removes function and output fields, reports `DroppedFieldIds`. |
| DataCoord unit | Refresh job/task metadata persists schema version using non-reserved field numbers. |
| DataCoord unit | Refresh commit fails if current collection schema version differs from job schema version. |
| DataCoord unit | Bump schema version policy skips external collections with stale segment schema versions. |
| DataCoord unit | Materialization resolver reports missing source-backed added field before refresh. |
| DataCoord unit | Materialization resolver reports generated output fields present after refresh. |
| QueryCoord unit | Same segment ID with newer manifest path schedules reopen. |
| QueryCoord unit | Current target is not promoted while distribution manifest is older than target manifest. |
| QueryCoord unit | Current target is promoted after every replica reports target manifest readiness. |
| QueryNode unit | `UpdateSchema` invalidates plan cache and updates segcore schema. |
| QueryNode unit | Dropped field removes loaded indexes and stats references. |
| QueryNode unit | Reopen loads new manifest and swaps atomically after success. |
| QueryNode unit | Query referencing unmaterialized field fails without partial results. |
| QueryNode unit | Loading an external segment whose manifest misses a schema-visible added field succeeds, but accessing that field fails. |
| QueryNode unit | External missing-field execution does not use internal default/null chunk filling. |
| DataNode unit | Segment missing newly added source-backed columns is patched. |
| DataNode unit | Segment missing newly added function output columns is rebuilt or patched. |
| DataNode unit | Refreshed or patched external segments carry the refresh schema version. |
| E2E | Load external collection, add source-backed field, query old fields succeeds, query new field fails, refresh, wait target ready, query new field succeeds. |
| E2E | Load external collection, add BM25, old vector search succeeds, BM25 search fails, refresh, index, wait target ready, BM25 search succeeds. |
| E2E | Load external collection, add MinHash, refresh, index, wait target ready, search and retrieve output. |
| E2E | Load external collection, add TextEmbedding, refresh, index, wait target ready, search and retrieve output. |
| E2E | Load external collection, drop BM25, verify BM25 search fails and input text field remains queryable. |
| E2E | Load external collection, drop source field, verify output fields and filters no longer accept the field. |
| E2E | Start refresh, run alter, verify alter succeeds and the old-schema refresh job fails with schema-changed error. |
| E2E | Start alter after refresh submit race, verify refresh commit fails with schema-changed error and leaves segment metadata unchanged. |

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

## Rollout

1. Keep the unloaded external add/drop path stable.
2. Add refresh schema-version commit guard and stale-refresh failure handling.
3. Add materialization state resolver and query/index gating.
4. Add QueryCoord reopen scheduling for same-segment-ID manifest updates.
5. Add QueryNode atomic external segment reopen and dropped-field cache cleanup.
6. Enable loaded drop field/drop function/drop BM25 first because no new
   materialization is required.
7. Enable loaded add source-backed field after materialization gating and
   refresh-driven target switch are verified.
8. Enable loaded add function/add BM25 after generated output, BM25 stats, and
   index readiness are verified.
9. Add SDK/REST helpers after the server API is stable.
10. Update user docs and release notes with the loaded behavior matrix.

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
