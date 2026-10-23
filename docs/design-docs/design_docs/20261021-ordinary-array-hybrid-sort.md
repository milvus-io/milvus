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

| HYBRID input | Low cardinality | High cardinality |
| --- | --- | --- |
| Ordinary ARRAY, scalar engine version < 6 | BITMAP | INVERTED |
| Ordinary ARRAY, scalar engine version >= 6 | BITMAP | STL_SORT |
| Nested ARRAY, scalar engine version < 4 | BITMAP | INVERTED |
| Nested ARRAY, scalar engine version >= 4 | BITMAP | STL_SORT |
| Typed JSON ARRAY, scalar engine version >= 6 | BITMAP | STL_SORT |

Explicit ordinary ARRAY STL_SORT and typed JSON ARRAY HYBRID/STL_SORT
require negotiated scalar engine version 6. Existing typed JSON ARRAY
artifacts from older writers remain readable; this requirement applies to new
public builds, not loading. Scalar JSON path indexes retain their version-4
floor. ARRAY selection retains its existing cardinality threshold and ignores
the configurable scalar low/high family choices. Default ordinary BOOL ARRAY
AUTOINDEX remains directly BITMAP.

Scalar engine current/maximum are 6/6. The version is a reader capability,
not a change to the packed-V3 container layout. SORT/BITMAP entries, HYBRID
selectors, parent-row validity and JSON existence sidecars keep their existing
representation. NaN ordering and float encodings are not changed.

This follows the index-engine capability approach in
[PR #54537](https://github.com/milvus-io/milvus/pull/54537), while retaining
all eight ordinary ARRAY element types and the three typed JSON ARRAY casts
from [PR #54505](https://github.com/milvus-io/milvus/pull/54505).
The separate `dataCoord.enableArraySortIndex` software switch and per-build
`enable_array_sort_index` flag are removed.
Capability negotiation follows the existing
[Scalar Index Version Management design](https://github.com/milvus-io/milvus-design-docs/blob/main/design_docs/20260313-scalar_index_version_management.md).

With default target settings, DataCoord resolves the minimum current scalar
engine version reported by online QueryNodes. Mixed v5/v6 readers therefore
keep new ordinary ARRAY HYBRID segments on INVERTED at high cardinality; once
all online readers report v6, those builds may select SORT. Administrator target
settings and the existing minimum/maximum clamping rules still apply. A
QueryNode reports its compiled capability when it registers after deployment;
there is no software-release-number check or one-minute activation timer.

Creation and snapshot restore share the v6 check for the new logical entry
points. During creation, AUTOINDEX requests fall back to INVERTED when that
capability is not ready; explicit requests return ServiceNotReady. Snapshot
restore returns ServiceNotReady without rewriting saved index definitions.
Existing ordinary ARRAY
HYBRID definitions remain admitted and choose their physical child per build
version. Every dispatch rechecks explicit ordinary ARRAY SORT and typed JSON
ARRAY HYBRID/SORT so queued jobs wait when reader capability is unavailable,
including the stock-binary no-QueryNode restart window with default target
settings. The C++ build boundary rejects these
new entry points below v6 if a request bypasses coordinator admission. Nested
selection retains its existing version-4 rule.

Build workers must be upgraded before readers enable v6 builds. The current
manager negotiates reader capabilities; it does not add worker capability
reporting or a persistent cluster version. The v6 format check does not define
new upgrade/downgrade admission rules for older nodes rejoining the cluster.

Existing indexes retain their saved physical family. AUTOINDEX definitions
already rewritten to INVERTED remain INVERTED and require recreation to adopt
HYBRID. Later builds and rebuilds use the resolved target. Upgrading the binary
does not itself rewrite existing index files; the existing automatic/forced
index-rebuild settings still apply. `autoUpgradeSegmentIndex` defaults to false.
Downgrade after writing
these entry points requires compatible readers or rebuilding them as INVERTED.

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
older HYBRID high-cardinality INVERTED artifacts at engine versions 3, 4 and 5,
and SORT at version 6, using the existing packed format. CAPI adapter tests
cover rejection below v6 and admission at v6 for all eight ordinary ARRAY
types. JSON CAPI tests additionally build from binlogs, reload, and query all
three casts at v6 after checking v5 rejection. Go tests cover the v6 engine floor,
AUTOINDEX fallback, explicit
creation, snapshot restore and queued-task retries across reader capability
changes. The SDK compatibility test switches forced targets v5/v6 and restores
the original settings; current-binary CI Helm values target v6. Existing
INVERTED and full JSON behavior are retained.

Separate-process ordinary ARRAY artifact readback checks the saved unchanged
reader implementation; it is not a full stock-binary rolling-upgrade test and
does not establish old-reader compatibility for the new explicit ARRAY SORT
or JSON ARRAY entry points. No NaN,
performance, or complete deployment E2E claim is made.
