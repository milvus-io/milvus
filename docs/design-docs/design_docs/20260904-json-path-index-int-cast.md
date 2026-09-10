# JSON path index INT64 cast type

## Motivation

A JSON path index is a user-configured typed projection: `json_cast_type` fixes
the Milvus scalar type that the path is shredded into before indexing. Until now
the only numeric cast type was `DOUBLE`, which stores every number via
`simdjson::number::as_double()` and therefore loses exact integer semantics
past 2^53 (the double projection documented in
`docs/agent_guides/json-filtering/cross-path-semantics.md`, case 8).

Users whose JSON numbers are integers such as identifiers, counters, and
timestamps need exact equality and range semantics beyond 2^53. This design
adds `INT64` as a first-class cast type alongside `DOUBLE`.

## Semantics

### Build time: strict cast, fail → null

A JSON number is indexed under an `INT64` cast type only when it is an integral
value within the signed 64-bit range:

- a JSON `int64` within the width is stored as-is;
- a JSON `uint64` within the width is stored;
- a JSON `double` that is integral and round-trips exactly (e.g. `2.0`) is
  stored;
- a fractional `double` (`2.5`), an out-of-range integer, and an unrepresentable
  number (`1e400`) are **null** in the typed column.

The value is never truncated or wrapped. Ordinary cast failures keep the target
path present while making the typed predicate `UNKNOWN`/null. A JSON number the
parser cannot represent follows Milvus's existing `EXISTS=false` contract.

### Query time: exact int64

A JSON integer query literal is `int64`, so the executor probes the INT64 index
without narrowing:

- values within range are probed exactly;
- JSON numbers outside the signed 64-bit range are null in the typed
  projection and cannot match the index.

Mixed `int64`/`float` `IN` lists are split by the planner into homogeneous
predicates joined by `OR`; `NOT IN` negates the combined result. The integer
branch can use the INT64 projection, while the floating-point branch uses a
compatible index, stats, or raw data. Each branch retains its own validity:
`UNKNOWN OR true` is true, but `UNKNOWN OR false` remains UNKNOWN.

## Behavior

| Data value | `INT64` cast |
|---|---|
| `42` | 42 |
| `2.0` | 2 |
| `2.5` | null |
| `9223372036854775808` | null |
| `1e400` | null |
| `"42"` | null |

`a == 42` uses the index exactly.

## Changes

- `common/JsonCastType.{h,cpp}`: add `INT64` to the
  enum, string map, formatter, and `ToMilvusDataType`.
- `index/JsonIndexBuilder.{h,cpp}`: add `StrictCastJsonNumberToInteger<T>` and
  route the integer cast type through it in `ProcessJsonFieldData`; reuse the
  existing `int64_t` instantiation.
- `index/IndexFactory.cpp`: dispatch the INT64 cast type for
  INVERTED, SORT, BITMAP, and HYBRID indexes.
- `index/JsonIndexBuilder.cpp` `IsDataTypeSupported`: accept `INT64` query
  literals against the `INT64` cast type.
- `exec/expression`: dispatch `int64` JSON literals to the exact integer
  executor (`UnaryExpr`, `BinaryRangeExpr`, `TermExpr`, `JsonContainsExpr`) via
  the new `PinnedJsonIndexCastElementType()`; decline the integer-cast index for
  `float` bounds and floating-point membership branches.
- `internal/util/indexparamcheck`: accept `INT64` in the
  INVERTED, STL_SORT, BITMAP, and HYBRID JSON cast-type allowlists.

## Out of scope

- Creating integer-cast indexes during a rolling upgrade. Applications must
  complete the binary upgrade before using the new cast types; CreateIndex
  does not add a rolling-upgrade gate for them.
- Narrow integer and integer-array cast types.
- An exact `uint64` model: `uint64 > INT64_MAX` continues to use the legacy
  uint64-to-double comparison contract on every path.

## Testing

- `JsonPathIndexTest.ConvertInt64_StrictCast`: build-time strict cast and
  null/non-exist semantics.
- Executor route assertions follow the existing
  `JsonCrossPathContractTest` fixture in
  `internal/core/src/exec/expression/JsonContainsByStatsTest.cpp`.
- `JsonNumericCastTest.IntegerAndDoubleSourcesPreserveProjectionValidity`
  tests integer and double source values against DOUBLE and INT64,
  including failed-cast nulls on INVERTED/STL_SORT.
  The original `JsonIndexTestFixture<int64_t>` remains an integer-source,
  DOUBLE-projection test with a correctly typed C++ index pointer.
