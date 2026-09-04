# JSON query cross-path semantics

## Purpose

A JSON predicate can run through several physical paths in Milvus. An index or
stats artifact is an accelerator, so its result must normally agree with the
raw JSON scan. This document defines that agreement, records the one case
that still falls back, and makes the intentional differences explicit.

This is a current implementation and test contract, not a historical design
proposal. Change this document and the cross-path regression test together.

## Execution paths

| Path | Representation | Selection | Main limitation |
|---|---|---|---|
| Raw/Growing | Original JSON parsed with simdjson | Always available when raw data is loaded | Full row scan |
| V4 typed stats | Automatically selected typed Parquet columns | Dense and type-stable paths | Only the selected path/type pairs are shredded |
| V4 shared stats | BSON shared column plus key/offset lookup | Values not represented by a typed column | Column scan and BSON decoding |
| Path index | User-selected path and fixed `json_cast_type` | One configured typed projection | Answers within the configured projection, which is `DOUBLE` by default or `INT8`/`INT16`/`INT32`/`INT64` when an integer cast is configured |
| Flat index | Recursively indexed primitive JSON leaves | Whole JSON field | Does not fully preserve container boundaries, empty containers, or invalid-number row coverage |

V3 stats remain readable and queryable. Successfully built V3 artifacts only
contain numbers the old parser accepted, so their numeric content agrees with
the current executors. The single known V3 gap is the empty-string null
sentinel below; it closes once the segment migrates to V4.

## Normative contract

### Results include validity

Every comparison is over the pair:

```text
(result bitmap, validity bitmap)
```

`false, valid=true` means a known non-match. `false, valid=false` means
`UNKNOWN`. Comparing only the result bitmap can hide a semantic regression,
especially under `NOT`, `AND`, or `OR`.

### Raw semantics are the default oracle

Stats and Flat are automatic accelerators: the user never asked for them, so
they must return exactly the raw result and validity bitmaps, or decline.

A Path index is different. It exists only because the user configured a path
and a `json_cast_type`, so that projection *is* the requested semantics. A
Path index answers within its projection and is allowed to differ from raw;
every such difference is listed under "Current documented difference cases".

In no case may an accelerator report `UNKNOWN` as a known non-match, or drop a
row's validity, merely to stay on the index.

### Semantic decline and capability routing

A JSON predicate can leave an accelerator for the raw scan when its **operand
shape** cannot be represented. The operand list mixes value types, or contains
an array literal whose element boundaries a typed projection does not preserve.
This is master behavior. It is implemented by
`PhyJsonContainsFilterExpr::JsonOperandShapeIsIndexable()` for the CONTAINS
family, and the equivalent `kArrayVal` check in `PhyUnaryRangeFilterExpr`.
`TermExpr` rejects a mixed-type list at plan time, except for a mixed
`int64`/`float` list, which is numerically well defined. A `DOUBLE` Path or
Flat index can execute that list directly; an `INT*` Path index declines it
because it cannot represent the float operands.

There is deliberately **no numeric-precision fallback**. A typed Path index is
the projection the user asked for, and it answers within that projection even
when the result differs from a raw scan. See "A numeric Path index is a double
projection" below.

Other RawData routes are index selection or index capability decisions, not
numeric-precision fallback:

- An index whose cast type does not accept the operand, or which does not
  cover the queried path, is never chosen; the expression runs on stats or raw
  as usual.
- A string index that physically cannot serve an operator (NGRAM on a range,
  FMINDEX on equality) declines that operator. That is a property of the index
  type, not of JSON semantics.
- A Flat index cannot distinguish a numeric object key from an array offset in
  a relative path, so paths containing numeric segments use raw JSON.
- ARRAY element-level predicates, empty ARRAY CONTAINS operands, and JSON
  `is_in_field` terms use raw data where the scalar-index interface cannot
  represent the requested operation.

### Numeric values

- JSON `int64` and `double` remain distinguishable while parsing and storage
  permit it.
- Numeric equality is value based: integer `2` and double `2.0` compare equal.
- `IN`, `NOT IN`, `CONTAINS`, `CONTAINS_ANY`, and `CONTAINS_ALL` use the same
  integer/double equality contract.
- Bool and string values stay in separate type domains; `true` is not numeric
  `1`, and string `"2"` is not numeric `2`.
- Raw, stats and Flat compare integers exactly, including beyond 2^53.
- A typed `DOUBLE` path index compares in double, including beyond 2^53. It
  does not fall back; see the documented difference below.
- A typed `INT8`/`INT16`/`INT32`/`INT64` path index compares in the configured
  integer width. Build-time strict cast leaves out-of-range or non-integral
  numbers null, so a query for a value outside the configured width matches
  nothing rather than truncating; see the documented difference below.
  A nonempty `IN` list whose candidates all overflow still uses the typed
  row-validity bitmap: failed casts remain null under `NOT IN`. Only a literal
  empty `IN []` has the unconditional false/all-valid contract.
- Milvus's current `uint64 > INT64_MAX` behavior uses the legacy
  uint64-to-double comparison contract on every path; it is not an exact
  uint64 model.

### Presence, null, and invalid values

The current `EXISTS` contract is a property of the target value, not of its
children:

| Target value | `EXISTS` |
|---|---:|
| Missing path | false |
| JSON `null` | false |
| JSON number rejected by the numeric parser, such as `1e400` | false |
| Empty string `""` | true |
| Empty array `[]` | true |
| Empty object `{}` | true |
| Container containing only null/empty values | true |
| Present string that fails an explicitly requested numeric cast | true |

An invalid JSON number is represented as a Parquet null in a typed stats column
and as BSON null in the shared column. Predicate validity and presence logic
must preserve the same invalid/null contract across both layouts.

Typed Path indexes use the existing scalar-index engine version to identify the
persisted `non_exist_offsets` semantics:

- Artifacts built with scalar-index engine versions below V6 retain the legacy
  presence bitmap. The bitmap remains authoritative when loaded, so an upgrade
  does not reinterpret an existing artifact in place.
- V6 records the target-value contract above: `[]`, `{}`, and `[null]` exist;
  missing, null, and an unrepresentable target number do not.
- QueryNodes and DataNodes publish their scalar-index engine capability in
  their sessions. DataCoord starts gradual one-input compaction only after all
  online nodes in both roles support V6 and the vector artifact rebuild target
  is inside the intersection of QueryNode reader and DataNode writer ranges.
  Capabilities are rechecked at each trigger. This follows the existing
  monotonic rolling-upgrade contract: after migration starts, an older
  QueryNode or DataNode must not late-join, and rolling back to a binary that
  cannot consume or reproduce V6 artifacts is unsupported. Flat and NGRAM
  indexes are outside this migration.
- If any typed Path index on a segment has a version newer than the local
  target, or any relevant index build is still unfinished, migration skips the
  whole segment rather than downgrading or racing it.

`CurrentScalarIndexVersion` is already persisted per segment index and in the
existing snapshot format, so this migration adds neither a Path-specific
metadata field nor a snapshot format version. During the migration window, a
loaded pre-V6 Path artifact may retain its legacy `EXISTS` result until
compaction rebuilds it with V6 semantics.

Snapshot restore checks copied JSON artifacts before creating the target
collection. A V6-or-newer Path artifact requires QueryNodes whose reader range
contains its persisted version. V4 stats require the V6 reader release, which
introduced V4 stats support; future stats formats are rejected. These checks
use reader capabilities, not the configured build-version override.

### Containers

Raw, stats, and compatible path-index execution preserve the target container
and its immediate element boundaries. For example:

```text
data:  {"a": [[1]]}
query: JSON_CONTAINS(a, 1)
result: false, valid=true
```

The top-level element is `[1]`, not `1`. Stats preserve this. Flat index has
a documented exception below.

### Explicit path-index casts

A configured cast is the only normal case in which a path index intentionally
changes value interpretation:

```text
data:       {"cast": "1.0"}
path index: /cast, DOUBLE, STRING_TO_DOUBLE
query:      cast == 1.0
```

Raw, stats, and flat see a string and return `UNKNOWN` for the numeric
predicate. The configured path index persists double `1.0` and returns true.
This difference is user-requested behavior, not an accelerator mismatch.

When a sealed segment has both V4 stats and a compatible typed Path index for
the queried path, the typed Path index has execution priority. Flat indexes do
not receive this priority. Operand shapes that the typed projection cannot
represent prevent that Path index from being selected. Once selected, a Path
index answers within its configured projection; numeric precision does not
trigger a raw fallback.

## Behavior matrix

`Direct` means the path can answer without raw fallback. `Fallback` means the
final answer is aligned only because execution declines the incomplete
accelerator.

| Case | Raw | Typed stats | Shared stats | Path index | Flat index | Status |
|---|---|---|---|---|---|---|
| `2 == 2.0` | Direct | Direct | Direct | Direct | Direct | Aligned |
| Mixed numeric `IN` / `NOT IN` | Direct | Direct | Direct | Direct | Direct | Aligned |
| Large int64 exact match | Direct, exact | Direct, exact | Direct, exact | Direct in double | Exact for `==`/`IN`; ranges over-match | Expected Path and Flat difference |
| `uint64 > INT64_MAX` numeric lookup | Direct with legacy uint64-to-double comparison | Direct | Direct | Direct in double | Direct | Aligned |
| `[2] CONTAINS 2.0` | Direct | Direct | Direct | Direct with compatible array index | Direct | Aligned |
| Mixed bool/string/int/double `CONTAINS_ANY/ALL` | Direct | Direct from shredded ARRAY BSON | Direct | Raw fallback for mixed operand | Raw fallback for mixed operand | Aligned |
| Empty string | Direct | Direct in V4 | Direct | Direct with `VARCHAR` | Direct | Aligned |
| V3 empty-string sentinel | Direct | Direct with accepted null conflation in shredded columns; shared BSON preserves `""` | Direct | Not applicable | Not applicable | Expected V3 difference on shredded paths |
| Missing or null target | Direct | Direct | Direct | V6 direct from validity/presence metadata | Direct when represented | Aligned for V6 |
| Invalid target number | Direct (`UNKNOWN`) | Direct from Parquet null | Direct from BSON null | Direct from invalid typed row | Artifact build rejected | Expected Flat difference |
| Invalid sibling, valid queried path | Direct | Direct | Direct | Direct on configured path | Artifact build rejected | Expected Flat difference |
| Nested/empty containers | Direct | Direct from BSON | Direct from BSON | Direct when the cast type covers the operand, otherwise not selected | Direct but incomplete/flattened | Expected Flat difference |
| Explicit `STRING_TO_DOUBLE` | Direct on original string | Direct on original string | Direct on original string | Direct on cast value | Direct on original string | Expected Path difference |

## Current documented difference cases

There are eight documented difference cases/categories. They are not eight
query correctness bugs: cases 2 and 3 are two consequences of the same Flat
all-or-nothing build limitation, case 4 is explicitly configured semantics,
case 5 is an accepted parser compatibility difference, case 6 is transient
during the pre-V6-to-V6 Path migration, case 7 is the accepted V3
empty-string sentinel gap that V4 migration closes, and case 8 records the
numeric projection a typed Path index is defined by, plus the narrower Flat
range gap that shares its cause. The number is not a count of individual rows
or assertions.

### 1. Flat container flattening and coverage

For the six container-relevant rows from the eight-row canonical fixture:

```json
{"a": 1}
{"a": [1]}
{"a": [[1]]}
{"a": []}
{"a": [null]}
{"a": [[]]}
```

the canonical Flat comparison has 14 row/predicate differences. A
`(predicate,row)` pair contributes one when either its result bit or validity bit
differs; a pair is not counted twice when both bits differ.

| Predicate | Divergent rows | Reason |
|---|---:|---|
| `a == 1` | 2 | Flat also sees the leaf in `[1]` and `[[1]]` |
| `a IN [1]` | 2 | Same recursive-leaf behavior |
| `JSON_CONTAINS(a, 1)` | 5 | Scalar/nested semantics and empty-container validity differ |
| `NOT JSON_CONTAINS(a, 1)` | 5 | The same result/validity differences survive `NOT` |

For example, raw returns `false, valid=true` for `[] CONTAINS 1`, while Flat
cannot prove that the empty array exists and returns `UNKNOWN`.

### 2. Flat rejects an invalid target number

Building a Flat artifact for `{"bad":1e400}` throws. Raw, typed stats, shared
stats, and a compatible path index retain the row and treat `/bad` as invalid.
This is an artifact-availability difference. A higher-level fallback may still
produce the correct query result, but the Flat artifact itself is not partial.

### 3. Flat rejects a document with an invalid sibling

For `{"bad":1e400,"ok":7}`, raw/stats and a `/ok` path index can answer
`ok == 7` as true. Flat build still rejects the whole document, so one bad
sibling prevents acceleration of an otherwise valid path.

### 4. Explicit `STRING_TO_DOUBLE` changes Path semantics

The `{"cast":"1.0"}` example above is intentional. Tests must assert the
difference rather than require raw parity.

### 5. Legacy and strict `STRING_TO_DOUBLE` parsing differ

Legacy `std::stod("1.5junk")` accepts the numeric prefix and returns `1.5`.
The strict simdjson implementation requires a complete JSON number and rejects
the trailing content. Consequently, an old persisted path artifact may differ
from one rebuilt with the strict parser. Current tests pin the parser contract;
they do not load a historical artifact fixture.

This compatibility difference is currently accepted. If artifact compatibility
becomes required, add an index format/version rule and a persisted-artifact
load test rather than weakening the parser.

### 6. A legacy pre-V6 Path index retains its presence bitmap

A pre-V6 typed Path artifact can report `EXISTS=false` for `[]`, `{}`, or
`[null]`. The loader deliberately keeps that persisted result: the old bitmap
cannot distinguish a missing path from an empty-but-present target, so it
cannot be repaired safely at load time. Ordinary one-input compaction rebuilds
the segment with V6 semantics. A production-style INVERTED Path-index test
exercises `Build(config)`, packed `UploadUnified`, and fresh-instance
`LoadUnified` for both V5 and V6 artifacts. It verifies the persisted bitmap
for `[]`, `{}`, `[null]`, missing, and null; this is not a historical artifact
fixture or coverage of every physical Path-index type.

### 7. V3 stats conflate a real empty string with null in typed columns

V3 used an empty string as the typed-column null sentinel, so a real `""`
reads back as null (UNKNOWN, non-match) through a shredded V3 STRING column,
while raw and the shared BSON column (which preserves `""`) report a match.
This matches the pre-upgrade behavior. Ordinary one-input compaction rebuilds
the segment with V4 stats, which preserve `""` as a real value. The
`JsonStatsV3CompatibilityTest.V3EmptyStringSentinelReadsBackAsNull` test pins
the shredded-column behavior, while the cross-path contract fixture keeps /s
shared and pins full V3/raw agreement there.

### 8. A numeric Path index is a typed projection

The numeric cast types are `DOUBLE`/`ARRAY_DOUBLE` and
`INT8`/`INT16`/`INT32`/`INT64`. A `DOUBLE` Path index stores
`simdjson::number::as_double()` for every value, and an `int64` query literal
is converted to `double` before the index is probed
(`PhyUnaryRangeFilterExpr::Eval`, `PhyBinaryRangeFilterExpr::Eval`,
`PhyJsonContainsFilterExpr::Eval`). An `INT*` Path index stores the value only
when it is an integral, in-range number; out-of-range and non-integral values
are null (build-time strict cast), and the query literal is narrowed to the
configured width.

Beyond 2^53 that projection is not injective, so a Path index differs from raw
in both directions:

| Data | Query | Raw / stats | Path index |
|---|---|---|---|
| `{"n": 9007199254740992}` | `n == 9007199254740993` | false | **true** |
| `{"n": 9007199254740993}` | `9007199254740992 < n < 9007199254740994` | true | **false** |

The first is a false positive: 2^53 and 2^53+1 share one double. The second is
a false negative: 2^53+1 is stored as 2^53, which an exclusive lower bound at
2^53 rejects.

Milvus previously declined the index and re-ran these predicates on raw data.
That fallback is removed. A Path index is a user-configured projection, in the
same sense as `STRING_TO_DOUBLE` in case 4, and answering inside it keeps one
predictable rule instead of a precision-dependent path switch. Applications
that need exact integer semantics past 2^53 should query a path covered by
JSON stats, which keeps an exact integer representation on both its typed and
its shared layout.

Validity is unaffected: the projection changes which rows match, never which
rows are `UNKNOWN`.

The same fallback removal also reaches the Flat index, whose exactness past
2^53 is only partial:

- **Equality and `IN` stay exact.** `json_terms_query` probes the `f64` column
  only when the integer round-trips through a double without loss, so
  `n == 9007199254740993` still matches that row alone.
- **Ranges over-match.** `JsonFlatIndexQueryExecutor::Range` unions the typed
  integer query with an `f64` fan-out that casts the bound with a bare
  `static_cast<double>` and reads integer values coerced to double. So
  `n BETWEEN 9007199254740993 AND 9007199254740993` also returns the row
  holding 2^53. Before this change the predicate never reached the index.

Closing that gap means translating an integral bound into the double domain by
moving its inclusivity (no double lies strictly between an integer and its
nearest double), in `OrF64Range` and in the mixed-bound conversion in
`PhyBinaryRangeFilterExpr::Eval`. That is deliberately not done here: the
current behavior is recorded and pinned instead.

Pinned by `JsonIndexTest.LargeInt64LiteralAliasesInDoublePathIndex`,
`JsonIndexTest.TestJsonContains` (equality and CONTAINS),
`JsonIndexTest.JsonBinaryRangePathIndexMatchesRawData` (the exclusive-range
false negative), `JsonFlatIndexExprTest.LargeInt64LiteralPrecisionOnFlatIndex`
(the Flat split above), and the `large-int64` block of
`JsonCrossPathContractTest.RawStatsPathAndFlatAgreeUnlessDocumented`.

`JsonNumericCastTest` additionally covers integer and floating-point JSON
sources through DOUBLE and INT8/16/32/64 projections, using INVERTED and
STL_SORT indexes without raw JSON. It checks range, equality, IN and NOT
validity for fractional values, overflow, missing paths, JSON null and root
null. `JsonNumericCastArtifactTest` repeats the source/cast matrix through
V6 binlog build, upload and reload, checking projected values, nulls and
EXISTS after persistence. `ArrayContainsOverflowTest` checks that native integer ARRAY
`CONTAINS_ALL` never drops an impossible out-of-range requirement.

## Regression-test contract

The main C++ contract test is:

```text
internal/core/src/exec/expression/JsonContainsByStatsTest.cpp
JsonCrossPathContractTest.RawStatsPathAndFlatAgreeUnlessDocumented
```

It builds Raw, V4 typed stats, V4 shared stats, typed path indexes, a Flat
index, and a V3 gap case over the same fixture.

The test must continue to enforce all of the following:

1. Compare both result and validity bitmaps for aligned cases.
2. Check whether an expression can execute fully on the selected index, so a
   parity assertion cannot silently pass through an unintended raw fallback.
3. Use same-path indexes for positive route assertions.
4. Include an alias control row when testing large integers: the fixture must
   hold both 2^53 and 2^53+1 so the Path double projection is observable.
5. Exercise both typed and shared stats.
6. Assert expected Flat results explicitly and dynamically count the canonical
   14 row/predicate differences.
7. Assert invalid-number Flat build rejection explicitly.
8. Keep explicit-cast differences separate from ordinary path-index parity.

Run the focused test with:

```bash
./build/Release/unittest/all_tests \
  --gtest_filter='JsonCrossPathContractTest.*'
```

When adding a JSON operator, value type, cast, or index representation, extend
the common fixture with:

- a positive witness;
- a known non-match;
- an `UNKNOWN` row;
- the corresponding `NOT` case when validity can affect the answer;
- a same-path direct-index assertion where supported; and
- an expected-divergence assertion only when the difference is documented in
  this file.

Do not convert an unexpected difference into an expected one solely to make the
matrix pass. For stats and Flat, an unexplained difference is a bug: they are
automatic and must match raw. For a Path index, first establish that the
difference follows from the configured projection alone; if it does not, it is
also a bug. Adding a new numeric-precision fallback is not the fix; RawData
routing must be justified by an operand, path, or operator the selected index
cannot represent.

## Implementation references

- [Cross-path C++ test](../../../internal/core/src/exec/expression/JsonContainsByStatsTest.cpp)
- [JSON numeric comparison helpers](../../../internal/core/src/exec/expression/JsonNumberComparison.h)
- [Path-index typed wrapper](../../../internal/core/src/index/JsonScalarIndexWrapper.h)
- [Path-index JSON extraction](../../../internal/core/src/index/JsonIndexBuilder.cpp)
- [Path-index V5/V6 artifact round trip](../../../internal/core/src/index/JsonPathIndexTest.cpp)
- [Path-index scalar-engine V6 migration policy](../../../internal/datacoord/compaction_policy_json_path_index.go)
- [V4 JSON stats](../../../internal/core/src/index/json_stats/JsonKeyStats.cpp)
- [Flat index](../../../internal/core/src/index/JsonFlatIndex.cpp)
- [JSON storage design background](../../design-docs/design_docs/20250308-json_storage.md)
