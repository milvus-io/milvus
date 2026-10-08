# Group-by scalar reads from JSON shredding

- Created: 2026-09-12
- Author(s): @liliu-z
- Status: Under Review
- Component: segcore / JSON stats
- Related issue: [milvus-io/milvus#53409](https://github.com/milvus-io/milvus/issues/53409)

## Problem and scope

Sealed JSON group-by currently parses raw JSON for each ANN candidate even
when JSON stats already contain a typed column for the exact path. After the
raw getter allocation fix, it pins a chunk once and wraps only the requested
row; this proposal adds a typed source before that existing raw fallback.

Supported output types are explicitly typed VARCHAR, BOOL, INT8, INT16,
INT32 and INT64. Integer outputs read an INT64 stats column and use the same
`static_cast` as the raw getter. An untyped string result uses
`Json::at_string_any`, which retains JSON representation, and stays raw.
Root paths, paths with numeric components, object/array outputs and paths
without a typed column stay raw. JSON path scalar indexes, growing segments,
JSON filter execution and the JSON stats writer are outside this change.

## Read interface and ownership

`JsonKeyStats::CreateShreddingReader(pointer, JSONType, segment_rows)` returns
a query-local reader for an exact scalar column, or `nullptr` when the path or
type is unsupported or has no typed column. It checks that the stats and
column row counts match the sealed segment; inconsistent metadata is an
error. Paths with a numeric component are rejected, as in the JSON stats
filter path, because raw JSON may address an array position there while
stats store arrays whole.

`ShreddingReader::Get(op_ctx, row_id)` returns an optional owning variant of
bool, int64 or string. It resolves row_id using the selected column's own
chunk geometry, caches one pin per touched chunk, checks row validity and
reads only the requested value. String rows use `StringChunk::operator[]`;
fixed values use `ValueAt` and memcpy to avoid unaligned integer loads.
There is no full-column scan, whole-chunk view vector, per-row Take object,
or index Reverse_Lookup.

An empty optional means **raw fallback required**, never an authoritative
null group. A typed NULL can be a missing path, another JSON type, JSON null,
a null top-level row or an empty string, which the stats writer stores as
NULL. Evaluating such rows with the unchanged raw getter preserves empty
strings, wrong-type strict_cast errors, missing-path behavior and top-level
nulls. A valid typed row bypasses raw chunk lookup and parsing altogether.

The reader retains its column and all touched chunk pins for the getter's
single-driver lifetime. Returned strings own their bytes, so map keys do
not depend on pin lifetime. Independent raw and typed chunk boundaries need
not align: both use absolute segment row IDs. The stats object is selected
once during getter construction under the existing sealed search lease;
there is no per-candidate segment snapshot capture.

The reader honors the same switch as JSON stats filters:
`common.usingJSONShreddingForQuery`, or its per-request override, reaches
the group-by node as the `expr_use_json_stats` plan option. When it is off,
the getter does not fetch JSON stats at all, so it neither reads typed
columns nor triggers deferred stats initialization.

This reader uses the current raw Chunk API because streamed ANN candidates
arrive incrementally. The finite-offset Take API would require gathering
unknown future candidates or recreating a one-element result per Get.
Adding a general streaming random-access interface is a separate migration.

## Known divergence from raw JSON

A valid typed value is what the JSON stats writer stored, so group-by
inherits the writer's semantics exactly as JSON stats filters already do.
Where the writer and raw simdjson lookup disagree, the typed value wins:

- Duplicate object keys: the writer keeps the last occurrence, raw lookup
  the first, and a duplicated parent can hide nested values.
- Legacy rows holding primitives that are not valid JSON (for example `01`),
  which the writer's tokenizer accepts and the raw parser rejects. The insert
  path no longer stores such bytes.

Escaped object keys, which the writer does not decode, are a further
candidate. This mismatch belongs to the stats writer, not to any one
consumer, and is tracked in
[milvus-io/milvus#54022](https://github.com/milvus-io/milvus/issues/54022).
After the writer is fixed and `JSONStatsDataFormatVersion` is bumped, query
nodes skip old-format stats and data coord rebuilds them, which corrects
filters and group-by together; this reader needs no change for it.

## Errors and cancellation

No query-time catch or status conversion is introduced. Unsupported paths
and typed invalidity are the only normal fallback signals. Column loading,
pinning, allocation, cancellation and corrupt-data errors escape through the
existing search error path; they must not silently trigger a raw read. A
failed pin is not inserted into the cache. Existing search/operator
cancellation checks are unchanged; uncached loads receive the same OpContext
and the ManifestGroupTranslator checks cancellation before loading. This
feature does not claim to repair pre-existing upstream error classification
or add new retry wiring.

## Validation and costs

Regression coverage includes:

- Real StringChunkWriter / JSONChunkWriter columns with different raw and
  typed chunk boundaries, sparse repeated candidates and no whole-chunk
  views or raw pins on successful typed reads.
- Exact output comparison, empty/missing/null/mixed values, strict casts,
  untyped JSON representation, owning strings, stats replacement and pin
  failure propagation with no fallback.
- Build → upload → load → getter using real STRING, BOOL and INT64 columns,
  including INT64 limits, narrow integer casts and escaped pointer tokens.
- The writer's empty-string representation followed by reader fallback.

This design does not claim any measured production latency reduction.
Query-time pins remain proportional to touched chunks, matching the raw
getter's existing retention policy. Bounded pin eviction is not introduced.
The stats writer and build path are unchanged.

## References

- [JSON storage design](20250308-json_storage.md)
- [Scalar local format and Scan/Take contracts](20260305-local_format.md)
- `internal/core/src/index/json_stats/JsonKeyStats.cpp`
- `internal/core/src/exec/operator/search-groupby/SearchGroupByOperator.h`
