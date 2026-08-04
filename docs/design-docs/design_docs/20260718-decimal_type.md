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

### Wire representation

Decimal values travel in the `bytes_data` (`BytesArray`) oneof slot of `ScalarField`, in both directions (insert and query result), encoded as:

> the signed **unscaled** integer (`value * 10^scale`) as **exactly 8 bytes, little-endian, two's complement**.

Because `precision <= 18`, every representable value fits an `int64`, so the width is fixed rather than the 8-or-16 that arbitrary-precision decimals would need. Null rows carry an empty bytes placeholder and are marked invalid via `FieldData.valid_data`.

This is a **public contract**, not an implementation detail: every SDK and the server must produce byte-identical output for the same value, or values corrupt silently across the wire. It is therefore documented in `milvus-proto` alongside the enum ([`BytesArray` comment](https://github.com/milvus-io/milvus-proto/blob/master/proto/schema.proto)) and pinned by cross-language golden vectors (see [Testing](#testing)).

Users never handle these bytes. The SDK accepts a decimal string (or a language-native decimal value), converts it using the schema's `scale`, and sends the canonical binary form; on the way back it renders the binary into text again. Little-endian was chosen to match Milvus's existing internal scalar/vector binary buffers, so the server can often consume wire bytes without a byte swap.

Filter expressions are unaffected by this: a predicate like `price > 19.99` carries its literal as text *inside the expression string*, which the parser converts (see [Query filters](#query-filters)). That is expression syntax, not wire data.

## Design Details

### Why unscaled int64, not a new arithmetic type

Given a fixed `scale` per field, a decimal value can be represented exactly as an integer: `"19.99"` at `scale=4` becomes the unscaled integer `199900`. As long as every value in a field shares the same scale, this integer encoding is **order-preserving** — comparisons, sorting, min/max, and range queries on the unscaled integers give identical results to comparing the original decimal values.

This means `Decimal` can reuse essentially all of `Int64`'s (and `Timestamptz`'s, which took the same approach for a different reason) existing storage, comparison, and indexing machinery unmodified, rather than requiring a new arbitrary-precision arithmetic type threaded through the query engine. `MaxDecimalPrecision = 18` was chosen specifically so this holds safely: `10^18 - 1` fits in a signed `int64`, `10^19 - 1` does not.

### Internal enum value must mirror the proto

The internal C++ `milvus::DataType` enum (`internal/core/src/common/Types.h`) mirrors the proto `DataType` numbering exactly, because proto→internal conversion is a **raw cast** rather than a lookup — `FieldMeta::ParseFrom` does `DataType(schema_proto.data_type())`, and `expr/ITypeExpr.h`, `indexbuilder/index_c.cpp`, `clustering/analyze_c.cpp` and `query/PlanProto.cpp` all `static_cast` the same way.

`DECIMAL` is therefore `30`, matching `schema.proto`. Any drift makes every `case DataType::DECIMAL` branch silently unreachable across storage, query execution, and indexing at once — no compile error, no runtime error, just dead code. `FieldMetaTest.DecimalEnumMirrorsProto` pins the two values together.

### Decode/encode boundaries

The wire bytes are decoded to unscaled `int64` exactly once at each of two independent entry points, since QueryNode (growing segments) and DataNode (durable storage) are two independent parallel consumers of the same insert stream:

- Go, DataNode flush path: `ColumnBasedInsertMsgToInsertData` (`internal/storage/utils.go`)
- C++, QueryNode growing-segment insert path: `VectorBase::set_data_raw` (`internal/core/src/segcore/ConcurrentVector.cpp`)

Query results re-encode the stored unscaled `int64` back into the canonical wire form at exactly two points:

- Growing-segment result builder (`internal/core/src/segcore/SegmentGrowingImpl.cpp`)
- Sealed-segment result builder (`internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp`)

The codec itself lives in one place per language — `EncodeUnscaledBytes`/`DecodeUnscaledBytes` in Go (`pkg/util/parameterutil`), `EncodeDecimalBytes`/`DecodeDecimalBytes` in C++ (`internal/core/src/common/Decimal.h`). Both shift byte-by-byte rather than reinterpreting an `int64` in place, so the emitted bytes are identical regardless of host endianness.

Default values follow the same contract: `ValueField.bytes_data` holds the canonical 8-byte form, decoded in `internal/rootcoord/util.go`, `internal/proxy/validate_util.go`, and `internal/core/src/storage/Util.cpp`.

### Nullable rows

For a nullable field, the proxy expands null rows into an **empty bytes placeholder** and marks them in `valid_data`. Both decode boundaries must consult `valid_data` and skip those positions — attempting to decode the placeholder would fail an otherwise valid insert. Skipped positions keep the zero value, which is never read because `valid_data` marks the row invalid.

One ordering subtlety: proxy validation (`checkDecimalFieldData`) runs **before** null expansion (`fillWithValue`), so it normally sees a compact array of real values, while `internal/storage` and segcore run **after** and see the padded array. The null check is written to be correct in both shapes rather than assuming one.

### Precision and scale validation

`DECIMAL(p, s)` admits at most `p` significant digits of which `s` are fractional, so the invariant is:

```
fractional_digits <= s          AND          integer_digits <= p - s
```

where leading zeros are not significant digits. The second half matters independently of how many fractional digits a literal actually spells out: counting only the digits present would accept `"12345"` into `DECIMAL(5,2)` (it scales up to `1234500` — seven digits, past the declared precision) and reject `"0.001"` from `DECIMAL(3,3)` (which is legal — no significant integer digits, three fractional). Both cases are pinned in `TestValidateDecimalStringPrecisionScaleInvariant`.

On the server side, values arriving as canonical bytes are checked against `ValidateUnscaledValue` — `|unscaled| <= 10^precision - 1` — which is the same invariant expressed on the already-scaled integer.

### Schema/metadata

`FieldMeta` (C++) pre-parses type-specific metadata into typed structs at schema-load time rather than doing generic runtime key lookups, so `Decimal` needed a dedicated `DecimalInfo` struct (precision + scale) alongside a new constructor, following the existing pattern used for e.g. `VARCHAR`'s `max_length`.

### Indexing

Because `Decimal` is stored as ordinary `int64` under a fixed scale, it can use `STL_SORT` (sorted array + binary search) exactly as `Int64`/`Timestamptz` do — no new index implementation was required. This is the only index type currently wired up for `Decimal`:

- `internal/util/indexparamcheck/stl_sort_checker.go` — allow `Decimal` at the top-level field check
- `pkg/util/paramtable/autoindex_param.go` — `AutoIndexConfig.ScalarDecimalIndexType`, defaulting to `STL_SORT`, so `Decimal` fields get a sane index automatically when the user doesn't specify one
- `internal/proxy/task_index.go` — `getPrimitiveIndexType()` routes `Decimal` to `ScalarDecimalIndexType`
- `internal/core/src/index/IndexFactory.cpp` / `internal/core/src/indexbuilder/IndexFactory.h` — generic dispatch, unchanged; restriction to `STL_SORT` happens entirely at the Go `indexparamcheck` layer by design (C++ index dispatch is deliberately kept type-agnostic)

**Explicitly out of scope for this iteration:** `BITMAP` and `HYBRID` (cardinality-based auto-selection between sorted-array and bitmap indexes) are not yet supported for `Decimal`. They are a natural follow-up once the `STL_SORT`-only path has landed and been validated, since `HYBRID`'s cardinality measurement and `BITMAP`'s per-distinct-value bit-list both need their own validation against Decimal's unscaled-int64 representation before being enabled.

### Query filters

Filter expressions carry decimal literals as text (`price > 19.99`), because they arrive inside the expression string rather than as wire data. The parser re-derives each literal from its **exact source text** at the column's declared scale (`EncodeUnscaledInt64`), instead of trusting the value `VisitInteger`/`VisitFloating` already parsed via `strconv.ParseFloat` — that path is lossy and would silently reintroduce the binary rounding error Decimal exists to eliminate. The fixups (`fixupDecimalLiteral`, `fixupDecimalOperands`, `fixupDecimalRangeBound`, `fixupDecimalTermValues`) cover comparisons, ranges, `IN` lists, and arithmetic operands.

### Arithmetic (Add/Sub only)

`price - 5 > 10` parses as `Compare(BinaryArithExpr(price, Sub, 5), 10)`. Both the arithmetic operand (`5`) and the comparison threshold (`10`) are re-derived from their exact source text and rescaled to the column's declared scale — reusing the same `EncodeUnscaledInt64` fixup mechanism plain comparisons already use — so the unscaled-`int64` subtraction and the final comparison both stay correct. The parser-level fixup (`fixupDecimalOperands` in `parser_visitor.go`) now handles both shapes: a direct `Decimal` column, or a `BinaryArithExpr` wrapping one. On the C++ side, `DataType::DECIMAL` reuses `ExecRangeVisitorImpl<int64_t>` verbatim in `BinaryArithOpEvalRangeExpr.cpp` — identical to `INT64` — since Add/Sub-by-a-rescaled-constant is scale-preserving.

**Multiply/divide/modulo are explicitly rejected** (`internal/parser/planparserv2/parser_visitor.go`, `VisitMulDivMod`): unlike Add/Sub, an integer multiplier must stay *unscaled* while a fractional one needs real fixed-point rescaling to avoid corrupting the result's magnitude — that logic isn't implemented yet, so these ops fail loudly with a clear error rather than silently producing a wrong answer. Field-vs-field arithmetic (`price - other_price`) was already rejected for all types before this change (`handleBinaryArithExpr`), so it remains out of scope here too — mixing two potentially different scales needs its own alignment logic.

## Appendix: Modification Points

### Proto (milvus-proto, separate repo)

| File | Change |
| --- | --- |
| `schema.proto` | `Decimal = 30` added to `DataType` enum (merged, `0fb0d5b`) |
| `schema.proto` | `BytesArray` / `Decimal` comments define the canonical 8-byte little-endian wire encoding (`doc/decimal-wire-format`) |

### Go — Type System & Validation

| File | Change |
| --- | --- |
| `pkg/util/typeutil/schema.go` | `IsDecimalType()`, wired into `IsPrimitiveType`; `CalcScalarSize` crash fix |
| `pkg/util/typeutil/gen_empty_field_data.go` | empty-`FieldData` case for `Decimal` |
| `pkg/util/parameterutil/get_decimal_params.go` | `GetPrecisionAndScale`; `ValidateDecimalString` (incl. the `integer_digits <= p - s` invariant); `EncodeUnscaledInt64` (text→int64, SDK/parser-facing); `EncodeUnscaledBytes`/`DecodeUnscaledBytes` (canonical 8-byte wire codec); `MaxUnscaledValue`/`ValidateUnscaledValue` |
| `internal/proxy/validate_util.go` | `checkDecimalFieldData` (wire-width + precision, skipping null placeholders via `valid_data`); default-value decode; null/default fill wiring |
| `internal/rootcoord/util.go` | schema validation; default value decoded from the canonical wire form |

### Go — Storage Layer

| File | Change |
| --- | --- |
| `internal/storage/insert_data.go` | `DecimalFieldData` type, full `FieldData` interface impl, wired into `NewFieldData` |
| `internal/storage/utils.go` | wire decode in `ColumnBasedInsertMsgToInsertData` (skips null placeholders via `valid_data`), `mergeDecimalField` + `MergeFieldData` switch entry |
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
| `internal/core/src/common/Types.h` / `.cpp` | `DataType::DECIMAL = 30` enum (mirrors the proto value — see "Internal enum value must mirror the proto") + dispatch functions |
| `internal/core/src/common/Consts.h` | `DECIMAL_PRECISION` / `DECIMAL_SCALE` string constants |
| `internal/core/src/common/FieldMeta.h` / `.cpp` | `DecimalInfo` struct, constructor, accessors, `ParseFrom`/`ToProto` |
| `internal/core/src/common/Decimal.h` | `kDecimalBytesLen`, `DecodeDecimalBytes()`, `EncodeDecimalBytes()` — the canonical wire codec |

### C++ — Insert Path (Growing Segments)

| File | Change |
| --- | --- |
| `internal/core/src/segcore/ConcurrentVector.cpp` | wire decode at `VectorBase::set_data_raw`, skipping null placeholders via `valid_data` |

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
| `internal/core/src/storage/Util.cpp` | storage utility dispatch; default value decoded from the canonical wire form |
| `internal/core/src/segcore/SegmentGrowingImpl.cpp` | memory-size accounting, growing-segment result building (`EncodeDecimalBytes` call site) |
| `internal/core/src/segcore/Utils.cpp` | `SetUpScalarFieldData` pre-allocation (prerequisite fix — missing pre-allocation would have crashed on out-of-bounds write) |
| `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp` | raw accessor, sealed-segment result building (`EncodeDecimalBytes` call site), external-table export |
| `internal/core/src/segcore/SegmentInterface.cpp` | default-value backfill |

### C++ — Indexing

| File | Change |
| --- | --- |
| `internal/core/src/index/IndexFactory.cpp` | `CreatePrimitiveScalarIndex`, `CreateScalarIndex` — generic dispatch, unchanged behavior |
| `internal/core/src/indexbuilder/IndexFactory.h` | `CreateIndex` — generic dispatch, unchanged behavior |

## Testing

### Cross-language golden vectors

Because the wire encoding is a contract shared by the server and every SDK, it is pinned by a fixture rather than by each side's own idea of correctness. The same JSON — positive, negative, zero, smallest unit, scale-0, ±max precision 18, trailing-zero padding, and `scale == precision` — is consumed by both languages:

| Side | Fixture | Test |
| --- | --- | --- |
| Go | `pkg/util/parameterutil/testdata/decimal_golden_vectors.json` | `TestDecimalGoldenVectors` |
| C++ | `internal/core/unittest/testdata/decimal/golden_vectors.json` (identical copy) | `DecimalWireFormat.GoldenVectorsMatchGo` |

Each case asserts the full chain `literal -> unscaled -> bytes` and back. If Go and C++ ever disagree about a single byte, both suites fail rather than the two silently exchanging different values. `DecimalWireFormat.EncodingIsLittleEndian` additionally pins the byte order on its own, so a flip to big-endian fails loudly instead of corrupting data.

### Unit coverage

| Area | Test |
| --- | --- |
| C++ schema parsing / enum drift | `FieldMetaTest.DecimalEnumMirrorsProto`, `FieldMetaTest.DecimalParseFromRoundTrip` |
| Wire codec round-trip, wrong-width rejection (incl. the empty null placeholder) | `TestUnscaledBytesRoundTrip`, `TestDecodeUnscaledBytesRejectsWrongWidth`, `DecimalWireFormat.EncodeDecodeRoundTrip` |
| Precision/scale invariant | `TestValidateDecimalStringPrecisionScaleInvariant`, `TestValidateUnscaledValue` |
| Filter expressions (comparisons, ranges, `IN`, null checks, Add/Sub arithmetic, Mul/Div/Mod rejection) | `internal/parser/planparserv2/decimal_test.go` |

## Open Follow-ups

1. Binlog read-back: `PayloadReader.GetDataFromPayload` has no `Decimal` case, so flushed data cannot be reloaded or compacted.
2. Query-output and upsert helpers (`PrepareResultFieldData`, `AppendFieldData`, `AppendFieldDataByColumn`, `getScalarDataLen`, `getData`) do not yet copy `Decimal` `BytesData`.
3. End-to-end coverage for the nullable growing/flush path, flush-and-reload/compaction, sealed and growing query output, upsert, and filter templates.
4. `BITMAP` / `HYBRID` indexing for `Decimal` (cardinality-based auto-selection).
5. `Multiply`/`Divide`/`Modulo` arithmetic on `Decimal` (needs fixed-point rescaling logic distinct from `Add`/`Sub`'s constant-rescale approach — see "Arithmetic" above). `Add`/`Sub` are implemented.
6. Field-vs-field arithmetic (`price - other_price`), including the general case of two `Decimal` columns with different scales.
