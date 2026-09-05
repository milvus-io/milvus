# JSON path index integer cast types (INT8/INT16/INT32/INT64)

## Motivation

A JSON path index is a user-configured typed projection: `json_cast_type` fixes
the Milvus scalar type that the path is shredded into before indexing. Until now
the only numeric cast type was `DOUBLE`, which stores every number via
`simdjson::number::as_double()` and therefore loses exact integer semantics
past 2^53 (the double projection documented in
`docs/agent_guides/json-filtering/cross-path-semantics.md`, case 8).

Users whose JSON numbers are all integers (identifiers, counters, timestamps)
want an exact integer index, both for storage compactness (`INT8`/`INT16`/
`INT32`) and for exact equality/range beyond 2^53 (`INT64`). This design adds
the four integer cast types and makes them first-class alongside `DOUBLE`.

## Semantics

### Build time: strict cast, fail → null

A JSON number is indexed under an `INT*` cast type only when it is an integral
value within the configured width:

- a JSON `int64` within the width is stored as-is;
- a JSON `uint64` within the width is stored;
- a JSON `double` that is integral and round-trips exactly (e.g. `2.0`) is
  stored;
- a fractional `double` (`2.5`), an out-of-range integer, and an unrepresentable
  number (`1e400`) are **null** in the typed column.

The value is never truncated or wrapped. `EXISTS` for the target path is
unaffected: a present-but-unrepresentable number keeps its `EXISTS` semantics,
only the typed predicate becomes `UNKNOWN`/null.

### Query time: narrow, never truncate

A JSON integer query literal is always `int64` (JSON has no `int8/16/32`
literal), so the executor narrows it to the configured width:

- values within range are probed exactly;
- a query value outside the width matches nothing (the strict build already
  excluded such values from the index), rather than truncating to a
  false-positive neighbor.

Mixed `int64`/`float` `IN` lists cannot be answered by an integer projection (a
`float` literal has no integer representation) and decline the index, falling
back to raw/stats — the same operand-shape rule as the existing `DOUBLE`
projection.

## Behavior

| Data value | `INT64` cast | `INT8` cast |
|---|---|---|
| `42` | 42 | 42 |
| `2.0` | 2 | 2 |
| `2.5` | null | null |
| `9223372036854775808` | null | null |
| `1e400` | null | null |
| `"42"` | null | null |

`a == 42` uses the index exactly. `a == 300` against an `INT8` index returns no
match (correct: 300 is out of range and was never indexed).

## Changes

- `common/JsonCastType.{h,cpp}`: add `INT8`/`INT16`/`INT32`/`INT64` to the
  enum, string map, formatter, and `ToMilvusDataType`.
- `index/JsonIndexBuilder.{h,cpp}`: add `StrictCastJsonNumberToInteger<T>` and
  route integral cast types through it in `ProcessJsonFieldData`; instantiate
  `int8_t`/`int16_t`/`int32_t` (and reuse `int64_t`).
- `index/IndexFactory.cpp`: dispatch the four integer cast types for
  INVERTED, SORT, BITMAP, and HYBRID indexes.
- `index/JsonIndexBuilder.cpp` `IsDataTypeSupported`: accept `INT64` query
  literals against integer cast types (narrowing).
- `exec/expression`: dispatch `int64` JSON literals to the exact integer
  executor (`UnaryExpr`, `BinaryRangeExpr`, `TermExpr`, `JsonContainsExpr`) via
  the new `PinnedJsonIndexCastElementType()`; decline integer-cast indexes for
  `float` bounds and mixed `int64`/`float` IN lists.
- `internal/util/indexparamcheck`: accept `INT8`/`INT16`/`INT32`/`INT64` in the
  INVERTED, STL_SORT, BITMAP, and HYBRID JSON cast-type allowlists.

## Out of scope

- Creating integer-cast indexes during a rolling upgrade. Applications must
  complete the binary upgrade before using the new cast types; CreateIndex
  does not add a rolling-upgrade gate for them.
- `ARRAY_INT*` cast types.
- An exact `uint64` model: `uint64 > INT64_MAX` continues to use the legacy
  uint64-to-double comparison contract on every path.

## Testing

- `JsonPathIndexTest.ConvertInt64_StrictCast`,
  `JsonPathIndexTest.ConvertInt8_StrictCastRejectsOutOfRange`: build-time strict
  cast and null/non-exist semantics.
- Executor route assertions follow the existing
  `JsonCrossPathContractTest` fixture in
  `internal/core/src/exec/expression/JsonContainsByStatsTest.cpp`.
- `JsonNumericCastTest.IntegerAndDoubleSourcesPreserveProjectionValidity`
  tests integer and double source values against DOUBLE and every INT width,
  including failed-cast nulls and all-overflow IN/NOT IN on INVERTED/STL_SORT.
  The original `JsonIndexTestFixture<int64_t>` remains an integer-source,
  DOUBLE-projection test with a correctly typed C++ index pointer.
