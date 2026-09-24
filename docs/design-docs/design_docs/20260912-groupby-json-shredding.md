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
Root paths, paths with numeric components, object/array outputs, missing
typed columns and old stats stay raw. JSON path scalar indexes, growing
segments and JSON filter execution are outside this change.

## Compatibility must be certified at the source

Typed validity alone does not prove equivalence with raw JSON:

1. The current writer stores an empty string through `AppendNull`. A typed
   invalid row may therefore be a real empty string, JSON null, a missing
   path, a different type or a null top-level row.
2. The writer's `values[JsonKey] = value` retains the last duplicate key.
   `Json::at` uses simdjson ondemand, which returns the first matching key.
   Duplicated parents can also hide nested values.
3. Object keys are taken directly from jsmn token bytes without unescaping.
   Escaped spellings and decoded raw JSON pointer lookup can disagree.
4. jsmn tokenization accepts some primitive forms that are not valid JSON.

Do not change these persisted values, the Parquet layout, or existing Search
stats behavior in this feature. Add the optional integer metadata entry
`group_by_scalar_read_version = 1` only for certified new builds:

- After classification, start eligible only if an actual shredded STRING,
  BOOL or INT64 column exists. Decide once before the first row is processed;
  a build with only ARRAY, DOUBLE or SHARED keys skips certification. Never
  enable certification after an earlier row has skipped validation.
- Reuse one simdjson DOM parser to validate each non-null document completely
  while eligibility remains true. This is a full parse, not lazy ondemand
  document initialization. Allocation failure propagates as
  `MemAllocateFailed`; invalid JSON prevents certification.
- In the existing object traversal, detect duplicate sibling keys and any
  JSON-escaped object key. Either condition disables certification for the
  entire stats object. The condition is monotonic across rows and batches.
- Arrays are not traversed for certification because they remain whole in
  stats and no reader path can descend through an array index.
- Literal `/`, `~`, and empty object keys use the same JSON pointer encoding
  as raw lookup. They have a source-level regression, including an empty
  intermediate key. The existing group-by pointer normalization drops a
  trailing empty token; both sources receive that same normalized pointer.
  Correcting that existing behavior is outside this change.

The existing extensible JSON metadata serializer accepts this integer key.
Old readers ignore it. New readers check the marker during the existing
metadata parse that constructs only the non-SHARED key map; there is no second
parse or reconstruction of the complete layout. They require integer version
1; absent, zero, unknown or non-integer versions do not enable the feature.
Existing stats therefore retain raw group-by behavior until a normal rebuild
produces certification.
No automatic rewrite/rebuild is introduced. A segment containing even one
ambiguous document conservatively retains raw behavior for every path.

## Read interface and ownership

`JsonKeyStats::CreateShreddingReader(pointer, JSONType, segment_rows)` returns
a query-local reader only for a certified exact scalar column. It checks
that the stats and column row counts match the sealed segment. Unsupported
capability returns `nullptr`; inconsistent metadata is an error.

`ShreddingReader::Get(op_ctx, row_id)` returns an optional owning variant of
bool, int64 or string. It resolves row_id using the selected column's own
chunk geometry, caches one pin per touched chunk, checks row validity and
reads only the requested value. String rows use `StringChunk::operator[]`;
fixed values use `ValueAt` and memcpy to avoid unaligned integer loads.
There is no full-column scan, whole-chunk view vector, per-row Take object,
or index Reverse_Lookup.

An empty optional means **raw fallback required**, never an authoritative
null group. That preserves empty strings, wrong-type strict_cast errors,
missing-path behavior and top-level nulls. An invalid typed row is evaluated
by the raw getter unchanged. A valid typed row bypasses raw chunk lookup and
parsing altogether.

The reader retains its column and all touched chunk pins for the getter's
single-driver lifetime. Returned strings own their bytes, so map keys do
not depend on pin lifetime. Independent raw and typed chunk boundaries need
not align: both use absolute segment row IDs. The stats object is selected
once during getter construction under the existing sealed search lease;
there is no per-candidate segment snapshot capture. Existing lazy single
column projection and warmup grouped projection are preserved.

This reader uses the current raw Chunk API because streamed ANN candidates
arrive incrementally. The finite-offset Take API would require gathering
unknown future candidates or recreating a one-element result per Get.
Adding a general streaming random-access interface is a separate migration.

## Errors and cancellation

No query-time catch or status conversion is introduced. Missing capability
and typed invalidity are the only normal fallback signals. Column loading,
pinning, allocation, cancellation and corrupt-data errors escape through the
existing search error path; they must not silently trigger a raw read. A
failed pin is not inserted into the cache. Existing search/operator
cancellation checks are unchanged; uncached loads receive the same OpContext
and the ManifestGroupTranslator checks cancellation before loading. Cached
row reads do not introduce a new cancellation polling policy. This feature
does not claim to repair pre-existing upstream error classification or add
new retry wiring.

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
- Duplicate and escaped object keys, nested shadowing, monotonic disabling,
  and absent, unknown or non-integer capability markers.
- Skipping certification when classification produces no supported scalar
  column, including supported scalar keys that remain SHARED.
- Existing writer empty-string representation followed by reader fallback.

The source-extracted ASAN harness verifies getter/reader semantics and a
million-row sparse-read allocation symptom. It does not replace the checked
in gtests, full Milvus integration execution, or production load benchmarks.
Validation results belong in the PR description; this design does not claim
any measured production latency reduction.

Build-time certification adds one complete DOM parse per eligible row and
sibling-key tracking during the existing traversal. The parser reuses its
buffer; work stops after the first unsafe row and is skipped entirely when
classification produces no supported scalar column. A source-body probe of
the parse/traverse step measured additional work for eligible builds; it
excludes classification, Parquet/BSON writing and storage I/O. Complete-build
throughput and peak memory remain unmeasured, and certification is not free.
Query-time pins remain proportional to touched chunks, matching the raw
getter's existing retention policy. Bounded pin eviction is not introduced.

## References

- [JSON storage design](20250308-json_storage.md)
- [Scalar local format and Scan/Take contracts](20260305-local_format.md)
- `internal/core/src/index/json_stats/JsonKeyStats.cpp`
- `internal/core/src/index/json_stats/parquet_writer.cpp`
- `internal/core/src/exec/operator/search-groupby/SearchGroupByOperator.h`
