# Sorted indexes for ARRAY and typed JSON ARRAY paths on 3.0

## Scope

Enable sorted parent-row postings for high-cardinality ordinary ARRAY fields
selected by HYBRID. Supported element types are BOOL, INT8, INT16, INT32,
INT64, FLOAT, DOUBLE, and VARCHAR. Also allow HYBRID for the typed JSON casts
ARRAY_BOOL, ARRAY_DOUBLE, and ARRAY_VARCHAR. Explicit STL_SORT is supported
on ordinary arrays of those eight element types and the three typed JSON array
casts. FULL JSON, whole struct fields, and vector fields are excluded. This extracts the ARRAY selection
work from PR #53972 without changing NaN semantics, comparisons, floating-point
encodings, ingestion, full-JSON indexes, or client serialization.

## Selection and compatibility

| Input | Low cardinality | High cardinality |
| --- | --- | --- |
| Ordinary ARRAY, software gate disabled or scalar engine version < 3 | BITMAP | INVERTED |
| Ordinary ARRAY, software gate enabled and scalar engine version >= 3 | BITMAP | STL_SORT |
| Nested ARRAY, scalar engine version < 4 | BITMAP | INVERTED |
| Nested ARRAY, scalar engine version >= 4 | BITMAP | STL_SORT |
| Typed JSON ARRAY HYBRID, feature enabled and scalar engine version >= 3 | BITMAP | STL_SORT |
| Explicit STL_SORT on ordinary or typed JSON ARRAY, feature enabled and scalar engine version >= 3 | STL_SORT | STL_SORT |

The ordinary ARRAY engine threshold requires the existing packed V3 representation, which
persists parent-row validity independently of element postings. This change
keeps scalar current version 3 and maximum version 4 on the 3.0 branch; it does
not introduce an engine capability or on-disk format version. The nested
compatibility gate is unchanged. ARRAY selection ignores configurable scalar
low/high families. The default BOOL ARRAY index remains BITMAP.

Ordinary ARRAY HYBRID SORT selection, explicit ordinary ARRAY STL_SORT, and
typed JSON ARRAY HYBRID/STL_SORT share
`dataCoord.enableArraySortIndex`. Its code and YAML defaults are `auto`, matching
master. The registered VersionGateSwitcher resolves false until all registered
nodes reach 3.0.4 and the one-minute stability window elapses, then the existing
confirmator persists true. Each build request carries the coordinator-resolved
`enable_array_sort_index` flag; missing or false keeps high-cardinality ordinary
ARRAY HYBRID on INVERTED. The flag is evaluated for each new segment build,
including existing HYBRID definitions. Existing persisted indexes keep their
physical family; nested ARRAY selection and INVERTED are unchanged.

Explicit false disables the new entry points and ordinary ARRAY HYBRID SORT selection; explicit true bypasses release
confirmation and requires the operator to have completed the upgrade. The
one-shot gate does not provide continuous rollback protection or per-task
worker routing. No additional node capability tracking is introduced.

The feature uses the existing packed V3 writer. Its admission check accepts
scalar version 3 once the feature is enabled, so the default scalar target -1
works without a target-version upgrade. Other JSON scalar index families retain
their existing version-4 requirement. No DataNode session capability fields,
worker maps, or new version-resolution logic are added.

Below the typed JSON ARRAY gate, AutoIndex creation falls back to INVERTED;
explicit creation and snapshot restoration return a retryable service-not-ready
error. Ordinary ARRAY AutoIndex requests configured to use SORT also fall back
to INVERTED before the gate opens. Explicit ordinary ARRAY SORT creation and
restore use the same gate and existing version-3 minimum, without changing
nested SORT admission. The fallback persists INVERTED in the index definition, so enabling the
feature later does not convert that definition to HYBRID. Recreate the index
through AutoIndex after the gate opens to obtain the new selection.

Existing indexes load their persisted physical family. Merely upgrading does
not rewrite them; newly built or rebuilt high-cardinality ordinary ARRAY
HYBRID indexes use SORT only when the software gate is enabled and the resolved
build version meets the existing packed-format threshold.
Cross-version reader compatibility still requires validation against the
target reader binary, especially when planning a rollback.

## Implementation

Unlike current master, this branch's ScalarIndexSort and StringIndexSort
classes did not build ordinary ARRAY input. Add parent-row build methods to
both classes. Each element contributes its parent row to the matching bitmap;
repeated elements do not duplicate results. NULL parents are skipped and valid
empty arrays remain valid even though they contribute no postings.

The existing packed writer and loader persist validity and offsets; their
format is unchanged. The legacy BinarySet writer is not extended. Scalar
reverse lookup cannot reconstruct an ARRAY and returns no value. Numeric
ARRAY range matching uses the forward match path: removing an out-of-range
element's parent could otherwise erase a parent that also has a matching
element. Empty numeric postings return empty IN matches and preserve valid
parents for NOT IN without searching an empty buffer.

Ordinary ARRAY HYBRID cardinality counting skips NULL parent rows before
accessing their compact FieldData payload. Other scalar field behavior and
nested element-coordinate behavior remain unchanged.

## Typed JSON ARRAY projection

The existing JSON extraction callbacks select only elements matching the
requested BOOL, DOUBLE, or VARCHAR cast. The new projection retains all such
elements in an ARRAY FieldData row, rather than retaining only the first
element as the scalar projection does. The HYBRID child or explicit SORT reader receives ARRAY schema
metadata and produces parent-row postings. Values after the first element and
duplicate values are preserved during extraction. These new ARRAY casts reject
`json_cast_function`, including empty or unknown values, because element
conversion functions are not supported. Existing scalar cast validation and
INVERTED behavior retain this branch's baseline.

Predicate validity follows raw contains: a source row is valid only when its
indexed path actually holds an array. Empty arrays and arrays without matching
element types remain valid but contribute no postings; NULL source fields,
missing/null paths, and non-array path values are invalid. This matters for
NOT contains, which must not select those unknown rows. Path existence remains
independent through the existing non-exist sidecar, including its treatment of
empty arrays and null values. This validity correction applies to the new
HYBRID and SORT projections; FULL JSON/JSON_FLAT and existing INVERTED behavior
are not changed. Both new consumers receive ARRAY schema metadata, so SORT keeps
parent-row membership, disables scalar reverse lookup and range complements,
and preserves validity separately from element postings. The C++ writer reuses the existing packed entries and imposes no new
scalar engine version requirement.

The INVERTED fallback also needs master's length-aware keyword batch-query
interface: the 3.0 writer already preserves embedded NUL bytes, but its old
C-string query interface truncates them. Backport the batch-query interface
used by `In` so ARRAY contains queries preserve the complete term on both
SORT and INVERTED. Keep the old ABI entry point for existing callers; stored
terms and index formats are unchanged. This does not change other string-query
interfaces or claim to resolve every legacy NUL-related behavior.

## Validation

The regression matrix exercises all eight element types, ordinary gate
versions 2/3/4, enabled/disabled/missing build flags, low versus high cardinality,
and packed heap/mmap reloads.
It checks parent-row membership, duplicate elements, NULL versus empty arrays,
contains-all bitmap intersection, and ranges with matching and nonmatching
elements in the same parent. Direct sorted builds also test an entirely empty
posting list. Existing nested version-gate tests remain enabled.

The expression regression installs a packed HYBRID index with version 3 and
threshold 1, asserts its physical SORT selection, and checks contains ANY/ALL
across multiple execution batches against raw arrays. A call counter verifies
the contains path actually queries the index. Whole-array equality and
inequality are checked on the same segment, including empty, reordered, and
duplicate-element literals. Both heap and mmap readers are exercised.
Count-only field fixtures check that excessive parent-row counts are rejected
before allocating posting buffers, including overflow across input batches.

Typed JSON ARRAY tests cover all three casts and both BITMAP/SORT branches,
two source batches, NULL/missing/empty/wrong-type paths, mixed element types,
duplicate values, and embedded-NUL/Unicode strings. Packed heap/mmap reloads
check membership and existence, then compare JSON contains ANY/ALL and their
negations against raw execution, including validity and empty query arrays.
The native JSON matrix covers HYBRID BITMAP, HYBRID SORT, and explicit SORT at
scalar engine version 3, plus HYBRID INVERTED when the build gate is closed or
missing and the legacy missing-version selection fallback. The missing-version
case checks selection of INVERTED; its roundtrip still uses packed V3 and does
not validate a legacy v2 writer or reader. Ordinary ARRAY tests use the public factory for
both HYBRID and explicit SORT across all eight element types, with a separate
actual-expression check for explicit SORT contains/equality. A call counter
confirms contains uses SORT; whole-array equality retains raw fallback because
this branch does not implement SORT membership callbacks. Go tests
exercise actual CreateIndex rejection/fallback, admission at the default scalar
version once enabled, actual ordinary ARRAY SORT restore admission, JSON restore
checks, and the unchanged scalar JSON version-4 gate.

These tests make no claim about NaN behavior, benchmark improvements, or a
completed cross-version deployment test.
