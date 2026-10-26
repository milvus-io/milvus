# ARRAY and typed JSON ARRAY indexes with STL_SORT

## Scope

Use parent-row BITMAP/SORT indexes for ordinary ARRAY and typed JSON ARRAY
paths. Ordinary ARRAY supports BOOL, INT8, INT16, INT32, INT64, FLOAT, DOUBLE,
and VARCHAR. Typed JSON paths support ARRAY_BOOL, ARRAY_DOUBLE, and
ARRAY_VARCHAR. This extracts the array selection work from
[PR #53972](https://github.com/milvus-io/milvus/pull/53972) without its NaN changes.

A typed JSON path supplies its element type through json_cast_type. The JSON
projection extracts the path, applies the existing element conversion, and
preserves field-null and path-existence information before handing the typed
array to the ordinary ARRAY builder. Full JSON indexing remains INVERTED.
Explicit STL_SORT is supported for ordinary ARRAY fields and the three typed
JSON ARRAY casts. Existing JSON ARRAY INVERTED remains supported. Explicit
JSON ARRAY BITMAP, full JSON objects, whole structs, and vectors are outside
the new SORT entry points; scalar BOOL and nested BOOL admission stay unchanged.

NaN semantics, floating-point encodings, ingestion, and client serialization
are outside this change.

## Selection and compatibility

| Input | Low cardinality | High cardinality |
| --- | --- | --- |
| Ordinary ARRAY, software gate disabled or scalar engine version < 5 | BITMAP | INVERTED |
| Ordinary ARRAY, software gate enabled and scalar engine version >= 5 | BITMAP | STL_SORT |
| Nested ARRAY, scalar engine version < 4 | BITMAP | INVERTED |
| Nested ARRAY, scalar engine version >= 4 | BITMAP | STL_SORT |
| Typed JSON ARRAY HYBRID, software gate enabled, scalar engine version 3–4 | BITMAP | INVERTED |
| Typed JSON ARRAY HYBRID, software gate enabled, scalar engine version >= 5 | BITMAP | STL_SORT |

The table describes HYBRID selection. Explicit STL_SORT always uses SORT,
requires the existing packed-V3 engine floor, and is independent of cardinality.

ARRAY selection retains its existing cardinality threshold and ignores the
configurable scalar low/high family choices. Default ordinary BOOL ARRAY
AUTOINDEX remains directly BITMAP.

This change enables ordinary ARRAY HYBRID to select SORT at engine version 5
once the software gate opens, reusing previously available ARRAY SORT reader
support. Before this change, high-cardinality ordinary ARRAY always selected
INVERTED. JSON ARRAY HYBRID reuses the existing packed format and serializers:
SORT/BITMAP entries, the HYBRID family selector, and JSON existence sidecars are
unchanged. Master retains scalar current/maximum version 5; this feature does
not change index engine versions or add DataNode capability reporting.

Reader entry points still need to be upgraded: previous adapters reject
explicit STL_SORT for ordinary ARRAY, while previous JSON readers reject
ARRAY casts under HYBRID/SORT and treat JSON SORT/BITMAP values as one scalar
per parent. Ordinary ARRAY HYBRID construction also observes the software gate.
The existing VersionGateSwitcher controls the new configuration
`dataCoord.enableArraySortIndex`. Its default `auto` resolves to false
until registered online software versions reach `3.1.0` for the one-minute
stability window, then the existing confirmator writes true to the config
center. The setting is registered with the shared gate mechanism; no extra
node watcher or version manager is introduced.

The switch protects ordinary ARRAY HYBRID SORT selection, ordinary ARRAY
explicit SORT, and JSON ARRAY HYBRID/SORT. Each build request carries the
coordinator-resolved `enable_array_sort_index` flag; missing or false keeps
high-cardinality ordinary ARRAY HYBRID on INVERTED. This also covers new segment
builds for existing HYBRID definitions, without rewriting their metadata.
Nested ARRAY selection retains its existing engine-version rule.
Before activation, AUTOINDEX requests for the new logical entry points fall
back to INVERTED; explicit creation and snapshot restoration of those types
return ServiceNotReady. Ordinary ARRAY HYBRID definitions remain admitted,
with their high-cardinality build family kept on INVERTED. When enabled, new
ARRAY entry points require the existing packed-V3 engine floor; ordinary
JSON scalar paths retain their original version-4 threshold. The existing
engine target resolver and worker clamping remain unchanged.

Explicit true/false settings retain the shared framework's override semantics.
This is a one-shot software-upgrade gate, not a continuous rollback guard: it
does not reset after activation if an old binary later rejoins. Unregistered
workers are outside its session scan. Rollout planning must account for these
existing framework boundaries.

Existing indexes retain their persisted physical family. An AUTOINDEX
definition already rewritten to INVERTED remains INVERTED; it is not silently
rewritten to HYBRID after upgrade. Existing HYBRID definitions keep BITMAP for
low cardinality; newly built high-cardinality segments use SORT only when both
the build-time software gate and engine threshold permit it. No on-disk layout
is changed.
Rollback after writing these new index types requires compatible readers or
rebuilding those indexes as INVERTED before downgrading.

## Representation and queries

Each array element contributes its parent row to postings. For
[[1, 3], [3], []], membership is 1 -> {0} and 3 -> {0, 1}; repeated elements do
not duplicate result rows. Nested indexes retain flattened element offsets.

ARRAY readers disable scalar value lookup and scalar range complement
optimization. Whole-array equality and positional access retain the original
array requirement. Packed V3 persists row validity independently of postings,
so a valid empty array remains distinct from NULL.

JSON SORT/BITMAP loading recognizes the ARRAY cast and disables scalar-only
coordinate validation and reverse lookup. HYBRID and explicit SORT predicate validity require
the path value to be an actual array: missing, null, or scalar paths produce
UNKNOWN for nonempty contains queries, including under NOT. Empty arrays and
arrays with no castable elements remain valid. Path existence uses its separate
non-exist state, so it is not inferred from predicate validity. Element
extraction follows the existing INVERTED conversion rules; the existing
INVERTED validity behavior is unchanged.

## Validation

Tests cover all ordinary ARRAY element types, the existing version gates,
BITMAP/SORT selection, V3 synchronous/asynchronous heap/mmap reloads, parent
rows, duplicates, NULL and empty arrays, and actual expression execution.

Ordinary ARRAY tests exercise the public explicit-SORT adapter for all eight
element types. Typed JSON ARRAY tests exercise HYBRID and explicit SORT, all
three casts, low/high cardinality, missing
and null paths, field NULL, empty and mixed arrays, duplicate terms, and real
contains ANY/ALL comparisons with raw evaluation. The regression matrix includes
HYBRID high-cardinality INVERTED at engine versions 3 and 4 and SORT at version
5, plus closed/missing build gates, using the existing packed format. Go tests cover software
gate activation and overrides, the existing packed-format floor, AUTOINDEX
fallback, explicit creation, and snapshot restore. Existing INVERTED and full JSON behavior are retained.

Separate-process ordinary ARRAY artifact readback checks the saved unchanged
reader implementation; it is not a full stock-binary rolling-upgrade test and
does not establish old-reader compatibility for the new explicit ARRAY SORT
or JSON ARRAY entry points. No NaN,
performance, or complete deployment E2E claim is made.
