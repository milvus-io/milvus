# External Table Function Output and Text Match

- **Created:** 2026-05-21
- **Author(s):** Wei Liu
- **Status:** Implemented
- **Commit:** `7a00320998cc3b5dabbe5b3a1e7ceb5a8968a650`
- **Component:** Proxy, DataNode, DataCoord, QueryNode, StorageV3, Segcore
- **Related Docs:** `20260105-external_table.md`, `20260129-add-function-field-design.md`, `20260226-manifest-format.md`

## Summary

External collections can now define function output fields and text-match fields.
Function outputs are computed during external-table refresh and persisted as
StorageV3 packed column groups beside the external source column groups. External
source columns remain referenced in-place; Milvus only materializes the derived
function output columns.

The implementation supports:

- BM25 function outputs for external text fields.
- MinHash function outputs for external text fields.
- TextEmbedding function outputs for external text fields.
- `enable_match` text index generation on external text fields.
- Search/retrieve `take()` output for user-visible non-BM25 function output fields.

BM25 function output is intentionally not raw-retrievable by users;
`CanRetrieveRawFieldData` returns false for BM25 outputs.

## Motivation

External collections originally exposed only columns that already existed in the
external data source. That prevented common lakehouse search patterns:

1. Text data in Parquet should be searchable through BM25 sparse vectors.
2. Text columns should support `text_match(...)` filters.
3. Text data should support derived vector fields such as TextEmbedding or
   MinHash without rewriting source files.
4. QueryNode should load and query these outputs through the same StorageV3
   manifest abstraction used by regular packed data.

Copying complete external data into Milvus would defeat the external-table
model. The core design is therefore to compute and store only derived columns
while keeping original source columns as external file references.

## Goals

1. Allow external collection schemas to contain function output fields.
2. Require ordinary external fields to map to source columns via
   `external_field`.
3. Forbid `external_field` on function output fields because outputs are
   produced by Milvus, not read from the external source.
4. Compute function outputs at refresh time with bounded memory usage.
5. Store function outputs in the segment manifest using normal StorageV3 column
   groups.
6. Let QueryNode and Segcore search function outputs and retrieve user-visible
   non-BM25 outputs through the existing manifest reader and `take()` path.
7. Persist BM25 stats and text index stats in the manifest so search and
   text-match load are deterministic.
8. Keep unchanged external segments reusable when they already contain all
   required function output columns.

## Non-Goals

1. No writes to external collections through insert/delete/upsert.
2. No lazy function evaluation at query time for external segment data.
3. No materialization of original external columns into Milvus-owned packed
   files.
4. No raw retrieval of BM25 function output fields.
5. No dynamic fields, user-defined primary keys, partition keys, clustering
   keys, autoID, or struct fields for external collections.
6. No automatic refresh when the external source changes.

## User Model

### External source fields

A normal user field in an external collection must set `external_field`. The
mapping binds a Milvus field name to a physical column name in the external
source.

Example:

```python
schema.add_field("doc", DataType.VARCHAR, external_field="doc_text")
```

### Function output fields

A function output field must not set `external_field`. The field is declared in
Milvus schema and listed as a function output.

Example:

```python
schema.add_field("sparse", DataType.SPARSE_FLOAT_VECTOR)
schema.add_function(
    name="bm25_fn",
    input_fields=["doc"],
    output_fields=["sparse"],
    function_type=FunctionType.BM25,
)
```

During `CreateCollection`, Proxy validates functions first so output fields are
marked as `IsFunctionOutput=true` before external schema validation runs.
External validation then skips source-column checks for those fields.

### Text match fields

External varchar fields may enable analyzer and text match:

```python
schema.add_field(
    "doc",
    DataType.VARCHAR,
    external_field="doc",
    enable_analyzer=True,
    enable_match=True,
)
```

After refresh creates external segments, DataCoord may schedule text-index stats
tasks for external collections, and DataNode persists text index stats into the
segment manifest.

## Architecture

```
Client
  |
  | CreateCollection(schema: external_source + functions + enable_match)
  v
Proxy
  | validateFunction() marks function outputs
  | NormalizeAndValidateExternalCollectionSchema()
  v
RootCoord / DataCoord
  | store schema and external source metadata
  | no segment data or function output is generated here

Client
  |
  | RefreshExternalCollection()
  v
DataCoord
  | scan external source metadata
  | allocate refresh work
  | trigger DataNode refresh task
  v
DataNode External Refresh
  | scan external source fragments
  | keep reusable segments when fragments and function-output columns match
  | build new segments for orphan fragments
  | ExecuteFunctionsForSegment()
  v
StorageV3 Manifest
  | external source column groups: external_field column names
  | function output column group: numeric field-id column names
  | stats: bm25.<fieldID>, text_index.<fieldID>
  v
QueryNode / Segcore
  | load external manifest reader
  | resolve manifest columns to FieldId
  | search/query/retrieve via chunk readers and take()
```

## Schema Validation

External schema validation uses two field classes:

| Field class | `external_field` | Source data check | Storage column name |
|-------------|------------------|-------------------|---------------------|
| External input field | required | yes | `external_field` |
| Function output field | forbidden | no | decimal `fieldID` |
| Virtual/system field | forbidden | no | computed internally |

Proxy create flow:

1. Unmarshal collection schema.
2. Run `validateFunction()` first.
3. Detect external schema via `external_field` mappings.
4. Validate `external_source` and `external_spec` pair.
5. Validate external schema constraints.
6. Inject virtual primary key for external collection.
7. Continue normal collection validation.

This ordering matters. If external validation ran before function validation,
function output fields would look like ordinary unmapped fields and fail the
`external_field` requirement.

## Refresh-Time Function Execution

`RefreshExternalCollectionTask` organizes external fragments into segments. For
each segment that needs creation or rebuild:

1. Create an input manifest referencing external source fragments.
2. Open `FFIPackedReader` over only function input columns.
3. Build three schemas:
   - input schema: source fields needed by functions;
   - execution schema: input fields plus function output fields;
   - output schema: function output fields only.
4. Stream Arrow batches from the external source.
5. Convert each batch to `InsertData`.
6. Run functions in-place through `embedding.RunAll()`.
7. Write only output columns into a new StorageV3 packed column group.
8. Finalize BM25 stats if BM25 output fields exist.
9. Commit the manifest and store its path in `SegmentInfo.ManifestPath`.

The pipeline is streaming. Peak memory is bounded by one Arrow batch plus
function output buffers, not by full segment size.

### Function execution order

`embedding.RunAll()` executes functions in this order:

1. TextEmbedding
2. BM25
3. MinHash

This order avoids confusing the function planner with pre-populated BM25 output
fields while still sharing one canonical function execution path with import.

### Segment reuse

Refresh reuses an existing segment only when:

1. all its fragments still exist in the external source; and
2. if the collection has functions, its manifest already contains every required
   function output column.

Function output columns are checked by numeric field-id column names. Old
segments created before function output support are invalidated and rebuilt.

## Manifest and Column Naming

StorageV3 manifests store column groups by column name. External collections use
two naming rules in the same manifest:

| Data kind | Manifest column name | Reason |
|-----------|----------------------|--------|
| External source column | `external_field` value | matches Parquet/source schema |
| Function output column | decimal `fieldID` string | matches internal StorageV3 convention |

Example manifest composition:

```
segment base path/
  _metadata/manifest-N.avro      # input manifest: external columns only
  _metadata/manifest-N+1.avro    # function output column group appended
  _metadata/manifest-N+2.avro    # BM25 stats registered, if needed
  _metadata/manifest-N+k.avro    # text index stats registered later, if needed
  _data/...                      # Milvus-owned function output packed files
  _stats/bm25.<fieldID>/0        # BM25 stats blob, if needed
  _stats/text_index.<fieldID>/...# text index files, if needed
```

Important rules:

- Input manifest creation excludes `IsFunctionOutput` fields.
- `ManifestReader` and Segcore request `external_field` for external input
  fields and `fieldID` strings for generated fields.
- C++ `Schema::ResolveColumnFieldId()` maps external column names back through
  `external_field`, then falls back to parsing numeric field IDs.
- `Schema::get_storage_column_name()` is the shared rule used by `take()`.

## QueryNode and Segcore Loading

External segments are loaded through the external manifest path. QueryNode uses
StorageV3 metadata and builds a schemaless milvus-storage reader so Parquet
source types can be normalized into Milvus internal types.

For external collections, Segcore:

1. injects external filesystem properties from `external_source` and
   `external_spec`;
2. asks the schema for needed external column names;
3. creates a milvus-storage reader on the manifest column groups;
4. resolves each column group column name back to a `FieldId`;
5. loads or lazily takes fields according to warmup and query needs.

Search and retrieve output paths use `take()` for external input fields and
user-visible non-BM25 function output fields. For each requested field, Segcore
uses `get_storage_column_name(fieldID)`, reads Arrow data from the manifest
reader, normalizes Arrow types, and fills Milvus `DataArray` results.

Virtual primary key is still computed from segment ID and row offset. It is not
stored in external files or function output column groups.

## Stats and Indexes

### BM25 stats

BM25 function output needs per-segment stats for IDF. During refresh, DataNode
accumulates BM25 output over all streamed batches and serializes one stats blob
per BM25 output field.

Stats key format:

```
bm25.<fieldID>
```

File layout:

```
_stats/bm25.<fieldID>/0
```

The stats blob is written before manifest registration. The manifest remains the
visibility commit point. A retry may overwrite the same deterministic path, but
readers only see stats after `AddStatsToManifest()` commits a new manifest.

### Text index stats

External collections may use `enable_match` on varchar fields. Refresh creates
the external segments first; it does not synchronously build the text index.
After segments are registered, DataCoord allows `TextIndexJob` for external
collections while still skipping other stats jobs such as JSON key stats and BM25
stats inspector jobs.

DataNode builds text index stats for every `EnableMatch()` field and registers
manifest stats with key:

```
text_index.<fieldID>
```

The result is also dual-written to segment metadata as a placeholder so the stats
inspector does not re-trigger text-index jobs for StorageV3 segments that already
have manifest stats.

QueryNode load requires these persisted text-index stats for enable-match fields.
If they are not ready yet, load fails with `TextIndexNotFound` and can be retried
after the stats task commits a manifest containing `text_index.<fieldID>`.

### Vector indexes on function output fields

Function output vector fields are normal schema fields after refresh. Index
building reads them from the manifest by numeric field-id column name.

Covered cases:

- BM25 sparse vector output with sparse inverted index.
- MinHash binary vector output with `MINHASH_LSH`.
- TextEmbedding float vector output with vector index.

## Memory Accounting

External segments use synthetic StorageV3 binlogs so downstream components can
process them like regular packed segments. The fake binlog includes all child
field IDs and a conservative memory estimate:

```
MemorySize = (sampledExternalBytesPerRow + functionOutputBytesPerRow) * rowCount
```

External bytes are sampled from external source fields. Function output bytes are
estimated from output field schemas because those columns do not exist in source
files. If every external sampling attempt fails or produces non-positive bytes,
refresh fails instead of writing zero-sized fake binlogs. This avoids QueryNode
resource estimation collapse and possible OOM during load.

## Failure Handling and Idempotence

Refresh fails loudly when:

- schema is nil;
- no function output field exists but function execution is invoked;
- function input field is missing from schema;
- external function input lacks `external_field`;
- Arrow batch reading fails;
- function execution fails;
- output batch writing fails;
- BM25 stats serialization or manifest registration fails;
- all memory-size sampling attempts fail.

Partially written output files or stats blobs are not visible until a manifest
commit references them. Retrying refresh recomputes outputs for the same segment
base path and commits a new manifest version.

## API and Behavior Matrix

| Feature | External collection behavior |
|---------|------------------------------|
| Declare BM25 output at create time | supported; data is generated by manual refresh |
| Declare MinHash output at create time | supported; data is generated by manual refresh |
| Declare TextEmbedding output at create time | supported; data is generated by manual refresh |
| `enable_match` on varchar field | supported; text index becomes available after stats task |
| Raw retrieve BM25 output | not supported |
| Raw retrieve MinHash/TextEmbedding output | supported |
| Insert/delete/upsert | not supported |
| Refresh after source changes | manual trigger required |
| AddField/schema evolution | not supported |

## Implementation Notes

Key implementation points:

- `internal/proxy/task.go`: validates functions before external schema checks
  so create-time function output fields are marked before external validation.
- `pkg/util/typeutil/schema.go`: external schema validator skips function output
  fields and rejects `external_field` on them.
- `internal/datanode/external/function_executor.go`: streams external input
  batches, runs functions, writes output column groups, and registers BM25 stats.
- `internal/datanode/external/task_update.go`: invalidates old segments missing
  output columns and routes manifest creation through the function executor.
- `internal/storagev2/packed/utils.go`: excludes function output fields from
  external source column list.
- `internal/storage/record_reader.go`: reads `external_field` columns for source
  fields and numeric field-id columns for generated fields.
- `internal/core/src/common/Schema.cpp`: maps external column names and function
  output field-id column names to `FieldId`.
- `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`: loads external
  manifests and uses `take()` for external and function output fields.
- `internal/datacoord/stats_inspector.go`: permits external `TextIndexJob` and
  skips other external stats jobs.
- `internal/datanode/index/task_stats.go`: builds text index stats and registers
  them into the manifest.

## Test Coverage

Unit and integration coverage includes:

1. Schema validation:
   - function output fields skip `external_field` requirement;
   - function output fields cannot define `external_field`;
   - text match is allowed on external collections;
   - BM25 output is not raw-retrievable.
2. Refresh behavior:
   - function output columns are generated during refresh;
   - old segments without output columns are rebuilt;
   - missing function inputs and execution errors fail refresh;
   - BM25 stats are serialized and registered.
3. Storage and Segcore:
   - external source columns use `external_field` names;
   - function output columns use numeric field-id names;
   - Segcore resolves both naming schemes.
4. End-to-end external collection scenarios:
   - text match over external varchar fields;
   - BM25 output search over sparse vector field;
   - MinHash output search and raw output retrieval;
   - TextEmbedding output search and raw dense-vector retrieval.

## Future Work

1. Support automatic source change detection and refresh scheduling.
2. Support more function types when runtime and storage semantics are defined.
3. Support external collection schema evolution when refresh/backfill semantics are defined.
4. Add GC for unreferenced files left by failed manifest commits.
5. Improve memory estimation for variable-size function outputs.
