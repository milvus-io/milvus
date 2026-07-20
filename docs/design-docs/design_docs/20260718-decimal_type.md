# MEP: Decimal Field Type

Current state: Under Discussion

ISSUE: [[Feature]: Support Decimal/fixed-point exact numeric field type #50956](https://github.com/milvus-io/milvus/issues/50956)

Keywords: Decimal, DataType, Indexing, Precision

Released: unreleased (targeting master)

## Summary

Add a new scalar field type, `Decimal`, that stores fixed-point exact numeric values (e.g. currency amounts, financial quantities) without the binary floating-point rounding error inherent to `FLOAT`/`DOUBLE`.

## Motivation

Milvus currently only offers `FLOAT`/`DOUBLE` for non-integer numeric fields. Both are IEEE-754 binary floating point, which cannot exactly represent most decimal fractions (e.g. `0.1`), so repeated arithmetic or comparison on values like prices, balances, or measurements accumulates rounding error. Users working with financial or exact-quantity data need a type that preserves the literal decimal value they inserted.

`Decimal` fills this gap: it is defined by a fixed `(precision, scale)` per field (analogous to SQL `DECIMAL(p, s)`), stores values as an unscaled fixed-point integer internally, and guarantees exact round-tripping of the text the client sent.

## Public Interfaces

Add a new enum value in `schema.proto` (source of truth: `milvus-proto`, already merged as [PR #630](https://github.com/milvus-io/milvus-proto/pull/630), commit `0fb0d5b`):

```proto
enum DataType {
  ...
  Decimal = 30;
}
```

Field-level type parameters (set via `TypeParams` at schema creation, mirroring how `VARCHAR` uses `max_length`):

```
"precision" -> string, 1..18 (MaxDecimalPrecision)
"scale"     -> string, 0..precision
```

Wire representation for values (both insert and query result): decimal values travel as UTF-8 literal text (e.g. `"19.99"`) in the existing `bytes_data` (`BytesArray`) oneof slot of `ScalarField` — previously unused, so no proto wire-format change was required beyond the enum addition.

## Design Details

### Why unscaled int64, not a new arithmetic type

Given a fixed `scale` per field, a decimal value can be represented exactly as an integer: `"19.99"` at `scale=4` becomes the unscaled integer `199900`. As long as every value in a field shares the same scale, this integer encoding is **order-preserving** — comparisons, sorting, min/max, and range queries on the unscaled integers give identical results to comparing the original decimal values.

This means `Decimal` can reuse essentially all of `Int64`'s (and `Timestamptz`'s, which took the same approach for a different reason) existing storage, comparison, and indexing machinery unmodified, rather than requiring a new arbitrary-precision arithmetic type threaded through the query engine. `MaxDecimalPrecision = 18` was chosen specifically so this holds safely: `10^18 - 1` fits in a signed `int64`, `10^19 - 1` does not.

### Decode/encode boundaries

Decimal text is decoded to unscaled `int64` exactly once at each of two independent entry points, since QueryNode (growing segments) and DataNode (durable storage) are two independent parallel consumers of the same insert stream:

- Go, DataNode flush path: `ColumnBasedInsertMsgToInsertData` (`internal/storage/utils.go`)
- C++, QueryNode growing-segment insert path: `VectorBase::set_data_raw` (`internal/core/src/segcore/ConcurrentVector.cpp`)

Decoding is pure string/integer arithmetic (`EncodeUnscaledInt64` in Go, `DecodeDecimalUnscaled` in C++) — never routed through `float`/`double` — to avoid reintroducing the exact rounding error Decimal exists to eliminate.

Query results re-encode the stored unscaled `int64` back to decimal text at exactly two points, since clients expect text back, not a raw integer:

- Growing-segment result builder (`internal/core/src/segcore/SegmentGrowingImpl.cpp`)
- Sealed-segment result builder (`internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`)

Reverse conversion (`int64` → text) is implemented once in C++ as `EncodeDecimalText` (`internal/core/src/common/Decimal.h`) and reused at both points.

### Schema/metadata

`FieldMeta` (C++) pre-parses type-specific metadata into typed structs at schema-load time rather than doing generic runtime key lookups, so `Decimal` needed a dedicated `DecimalInfo` struct (precision + scale) alongside a new constructor, following the existing pattern used for e.g. `VARCHAR`'s `max_length`.

### Indexing

Because `Decimal` is stored as ordinary `int64` under a fixed scale, it can use `STL_SORT` (sorted array + binary search) exactly as `Int64`/`Timestamptz` do — no new index implementation was required. This is the only index type currently wired up for `Decimal`:

- `internal/util/indexparamcheck/stl_sort_checker.go` — allow `Decimal` at the top-level field check
- `pkg/util/paramtable/autoindex_param.go` — `AutoIndexConfig.ScalarDecimalIndexType`, defaulting to `STL_SORT`, so `Decimal` fields get a sane index automatically when the user doesn't specify one
- `internal/proxy/task_index.go` — `getPrimitiveIndexType()` routes `Decimal` to `ScalarDecimalIndexType`
- `internal/core/src/index/IndexFactory.cpp` / `internal/core/src/indexbuilder/IndexFactory.h` — generic dispatch, unchanged; restriction to `STL_SORT` happens entirely at the Go `indexparamcheck` layer by design (C++ index dispatch is deliberately kept type-agnostic)

**Explicitly out of scope for this iteration:** `BITMAP` and `HYBRID` (cardinality-based auto-selection between sorted-array and bitmap indexes) are not yet supported for `Decimal`. They are a natural follow-up once the `STL_SORT`-only path has landed and been validated, since `HYBRID`'s cardinality measurement and `BITMAP`'s per-distinct-value bit-list both need their own validation against Decimal's unscaled-int64 representation before being enabled.

### Arithmetic (Add/Sub only)

`price - 5 > 10` parses as `Compare(BinaryArithExpr(price, Sub, 5), 10)`. Both the arithmetic operand (`5`) and the comparison threshold (`10`) are re-derived from their exact source text and rescaled to the column's declared scale — reusing the same `EncodeUnscaledInt64` fixup mechanism plain comparisons already use — so the unscaled-`int64` subtraction and the final comparison both stay correct. The parser-level fixup (`fixupDecimalOperands` in `parser_visitor.go`) now handles both shapes: a direct `Decimal` column, or a `BinaryArithExpr` wrapping one. On the C++ side, `DataType::DECIMAL` reuses `ExecRangeVisitorImpl<int64_t>` verbatim in `BinaryArithOpEvalRangeExpr.cpp` — identical to `INT64` — since Add/Sub-by-a-rescaled-constant is scale-preserving.

**Multiply/divide/modulo are explicitly rejected** (`internal/parser/planparserv2/parser_visitor.go`, `VisitMulDivMod`): unlike Add/Sub, an integer multiplier must stay *unscaled* while a fractional one needs real fixed-point rescaling to avoid corrupting the result's magnitude — that logic isn't implemented yet, so these ops fail loudly with a clear error rather than silently producing a wrong answer. Field-vs-field arithmetic (`price - other_price`) was already rejected for all types before this change (`handleBinaryArithExpr`), so it remains out of scope here too — mixing two potentially different scales needs its own alignment logic.

## Appendix: Modification Points

### Proto (milvus-proto, separate repo — merged)

| File | Change |
| --- | --- |
| `schema.proto` | `Decimal = 30` added to `DataType` enum |

### Go — Type System & Validation

| File | Change |
| --- | --- |
| `pkg/util/typeutil/schema.go` | `IsDecimalType()`, wired into `IsPrimitiveType`; `CalcScalarSize` crash fix |
| `pkg/util/typeutil/gen_empty_field_data.go` | empty-`FieldData` case for `Decimal` |
| `pkg/util/parameterutil/get_decimal_params.go` | `GetPrecisionAndScale`, `ValidateDecimalString`, `EncodeUnscaledInt64` |
| `internal/proxy/validate_util.go` | `checkDecimalFieldData`, null/default-value fill wiring |
| `internal/rootcoord/util.go` | schema/default-value precision-scale validation |

### Go — Storage Layer

| File | Change |
| --- | --- |
| `internal/storage/insert_data.go` | `DecimalFieldData` type, full `FieldData` interface impl, wired into `NewFieldData` |
| `internal/storage/utils.go` | decode case in `ColumnBasedInsertMsgToInsertData`, `mergeDecimalField` + `MergeFieldData` switch entry |
| `internal/storage/serde.go`, `data_codec.go`, `payload_writer.go`, `payload.go` | Arrow/binlog serialization, both StorageV1 and V2/V3 paths |
| `internal/storage/data_sorter.go` | compaction sort support (fixed a latent panic on missing case) |
| `internal/storage/field_stats.go` | segment min/max stats + bloom filter wiring |

### Go — Parser (filter expressions)

| File | Change |
| --- | --- |
| `internal/parser/planparserv2/parser_visitor.go` | `fixupDecimalLiteral`, `fixupDecimalOperands` (renamed from `fixupDecimalComparisonOperands`, now also used by `VisitAddSub`), `fixupDecimalRangeBound`, `fixupDecimalTermValues`; wired into `VisitEquality`/`VisitRelational`/`VisitRange`/`VisitReverseRange`/`VisitTerm`/`VisitAddSub`; `VisitMulDivMod` explicitly rejects `Decimal` operands |
| `internal/parser/planparserv2/utils.go` | `canBeComparedDataType`/`castValue` Decimal cases; `canArithmeticDataType`/`getTargetType` Decimal cases; `decimalArithColumnInfo` helper (resolves the underlying column through a `BinaryArithExpr` wrapper) |

### C++ — Query Execution: Arithmetic

| File | Change |
| --- | --- |
| `internal/core/src/exec/expression/BinaryArithOpEvalRangeExpr.cpp` | `DataType::DECIMAL` case in `Eval()` and `PrefetchRawData()`, reusing `ExecRangeVisitorImpl<int64_t>`/`PrefetchRawData<int64_t>` verbatim from `INT64` |

### Go — Indexing / AUTOINDEX

| File | Change |
| --- | --- |
| `internal/util/indexparamcheck/stl_sort_checker.go` | allow `Decimal` at top-level field check |
| `pkg/util/paramtable/autoindex_param.go` | `ScalarDecimalIndexType` param, `"decimal": "STL_SORT"` default |
| `internal/proxy/task_index.go` | `getPrimitiveIndexType()` routes `Decimal` → `ScalarDecimalIndexType` |

### C++ — Type System

| File | Change |
| --- | --- |
| `internal/core/src/common/Types.h` / `.cpp` | `DataType::DECIMAL = 27` enum + dispatch functions |
| `internal/core/src/common/Consts.h` | `DECIMAL_PRECISION` / `DECIMAL_SCALE` string constants |
| `internal/core/src/common/FieldMeta.h` / `.cpp` | `DecimalInfo` struct, constructor, accessors, `ParseFrom`/`ToProto` |
| `internal/core/src/common/Decimal.h` | `DecodeDecimalUnscaled()`, `EncodeDecimalText()` |

### C++ — Insert Path (Growing Segments)

| File | Change |
| --- | --- |
| `internal/core/src/segcore/ConcurrentVector.cpp` | decode wiring at `VectorBase::set_data_raw` |

### C++ — Query Execution

| File | Change |
| --- | --- |
| `internal/core/src/exec/expression/UnaryExpr.cpp` | comparison dispatch |
| `internal/core/src/exec/expression/TermExpr.cpp` | `IN` dispatch |
| `internal/core/src/exec/expression/BinaryRangeExpr.cpp` | range dispatch |
| `internal/core/src/exec/expression/NullExpr.cpp` | null-check dispatch |

### C++ — Storage Read Path & Query Results

| File | Change |
| --- | --- |
| `internal/core/src/common/FieldData.cpp`, `ChunkWriter.cpp` | chunked storage read/write |
| `internal/core/src/segcore/SegmentChunkReader.cpp` | mmap-backed chunk reads |
| `internal/core/src/mmap/ChunkedColumn.h` / `ChunkedColumnGroup.h` / `ChunkedColumnInterface.h` | mmap column support |
| `internal/core/src/storage/Util.cpp` | storage utility dispatch |
| `internal/core/src/segcore/SegmentGrowingImpl.cpp` | memory-size accounting, growing-segment result building (`EncodeDecimalText` call site) |
| `internal/core/src/segcore/Utils.cpp` | `SetUpScalarFieldData` pre-allocation (prerequisite fix — missing pre-allocation would have crashed on out-of-bounds write) |
| `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp` | raw accessor, sealed-segment result building (`EncodeDecimalText` call site), external-table export |
| `internal/core/src/segcore/SegmentInterface.cpp` | default-value backfill |

### C++ — Indexing

| File | Change |
| --- | --- |
| `internal/core/src/index/IndexFactory.cpp` | `CreatePrimitiveScalarIndex`, `CreateScalarIndex` — generic dispatch, unchanged behavior |
| `internal/core/src/indexbuilder/IndexFactory.h` | `CreateIndex` — generic dispatch, unchanged behavior |

## Open Follow-ups

1. `BITMAP` / `HYBRID` indexing for `Decimal` (cardinality-based auto-selection).
2. `Multiply`/`Divide`/`Modulo` arithmetic on `Decimal` (needs fixed-point rescaling logic distinct from `Add`/`Sub`'s constant-rescale approach — see "Arithmetic" above). `Add`/`Sub` are implemented.
3. Field-vs-field arithmetic (`price - other_price`), including the general case of two `Decimal` columns with different scales.
4. C++ build/test verification in a resource-adequate environment (local dev machine is memory-constrained for a from-scratch `make build-cpp`; considering CI-based verification instead). Note: the Go-side parser changes (including arithmetic, this section) *have* been compiled and test-verified natively — see `internal/parser/planparserv2/decimal_test.go`.
