# MEP: Decimal Field Type

Current state: Under Discussion

ISSUE: [[Feature]: Support Decimal/fixed-point exact numeric field type #50956](https://github.com/milvus-io/milvus/issues/50956)

Keywords: Decimal, DataType, Indexing, Precision

Released: unreleased (targeting master)

## Summary

Add a new scalar field type, `Decimal`, that stores fixed-point exact numeric values (e.g. currency amounts, financial quantities) without the binary floating-point rounding error inherent to `FLOAT`/`DOUBLE`.

This revision responds to review feedback asking for the full support matrix — precision/scale bounds, the Decimal64/128/256 growth path, rounding/overflow semantics, storage/compaction/schema-evolution behavior, query/index/aggregation scope, and SDK/REST/bulk-import responsibilities — to be settled and written down *before* further implementation, rather than discovered incrementally. Where a question doesn't yet have an implementation, this doc says so explicitly rather than describing aspirational behavior as done.

## Motivation

Milvus currently only offers `FLOAT`/`DOUBLE` for non-integer numeric fields. Both are IEEE-754 binary floating point, which cannot exactly represent most decimal fractions (e.g. `0.1`), so repeated arithmetic or comparison on values like prices, balances, or measurements accumulates rounding error. Users working with financial or exact-quantity data need a type that preserves the literal decimal value they inserted.

`Decimal` fills this gap: it is defined by a fixed `(precision, scale)` per field (analogous to SQL `DECIMAL(p, s)` / PostgreSQL `NUMERIC(p, s)`), stores values as an unscaled fixed-point integer internally, and guarantees exact round-tripping of the text the client sent.

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
"precision" -> string, 1..18 in v1 (MaxDecimalPrecision), 1..76 reserved for the full growth path — see "Precision/scale bounds" below
"scale"     -> string, 0..precision
```

### Precision/scale bounds

| Question | Decision | Rationale |
| --- | --- | --- |
| Minimum precision | 1 | A `DECIMAL(0, ...)` field can hold no digits; not useful. |
| Maximum precision, v1 | 18 (`MaxDecimalPrecision`) | Every value fits an unscaled `int64` (`10^18 - 1` fits, `10^19 - 1` doesn't), so v1 reuses `Int64` storage/comparison/indexing unmodified — see "Why unscaled int64" below. |
| Maximum precision, reserved | 76 | Matches the largest tier this doc's growth path defines (Decimal256). Schemas cannot declare precision > 18 until that tier ships; the number is reserved here so the eventual bump is a range-check change, not a new design question. |
| Minimum scale | 0 | A scale-0 `Decimal` is an exact integer with no fractional part — useful on its own (e.g. `DECIMAL(10, 0)` as an exact-arithmetic alternative to `INT64` that participates in the same comparison/index machinery as other Decimal columns). |
| Maximum scale | `== precision` | `DECIMAL(3, 3)` is legal: zero integer digits, three fractional (`0.001`..`0.999`). Rejecting `scale == precision` would arbitrarily exclude "pure fraction" columns for no representational reason. |
| **Negative scale** | **Not allowed, v1 or reserved.** | PostgreSQL `NUMERIC` allows negative scale (rounds to a power of ten left of the decimal point, e.g. scale `-2` stores multiples of 100). Milvus does not support it: the target workloads (currency, measurement, exact-quantity vector-DB payloads) don't need it, and admitting it would require deciding whether inserts *round* to the nearest multiple or are *rejected* when not already a multiple — an extra semantic decision with no motivating use case yet. If a real need for negative scale surfaces, it composes independently of the Decimal64/128/256 growth path below (it doesn't change wire width, only the valid range check and the digit-counting rule), so deferring it here isn't a dead end. |

`ValidateDecimalString` / `EncodeUnscaledInt64` (`pkg/util/parameterutil/get_decimal_params.go`) enforce `fractional_digits <= scale` and `integer_digits <= precision - scale`, matching this table.

### Wire representation

Decimal values travel in the `bytes_data` (`BytesArray`) oneof slot of `ScalarField`, in both directions (insert and query result), encoded as:

> the signed **unscaled** integer (`value * 10^scale`) as a fixed-width two's-complement integer, **little-endian**, width determined by the field's declared `precision` — see the table below.

| Precision range | Tier | Width | Status |
| --- | --- | --- | --- |
| 1 – 18 | Decimal64 | 8 bytes | **Implemented (v1)** |
| 19 – 38 | Decimal128 | 16 bytes | Reserved, not implemented |
| 39 – 76 | Decimal256 | 32 bytes | Reserved, not implemented |

Because `precision <= 18` in v1, every representable value fits an `int64`, so only the 8-byte tier exists in code today; the 16/32-byte rows are the reserved growth path (see next section). Null rows carry an empty bytes placeholder and are marked invalid via the field's `valid_data` — see "Nullable rows and the `valid_data` migration" below for where that now lives.

This is a **public contract**, not an implementation detail: every SDK and the server must produce byte-identical output for the same value, or values corrupt silently across the wire. It is therefore documented in `milvus-proto` alongside the enum ([`BytesArray` comment](https://github.com/milvus-io/milvus-proto/blob/master/proto/schema.proto)) and pinned by cross-language golden vectors (see [Testing](#testing)).

Users never handle these bytes. The SDK accepts a decimal string (or a language-native decimal value), converts it using the schema's `scale`, and sends the canonical binary form; on the way back it renders the binary into text again. Little-endian was chosen to match Milvus's existing internal scalar/vector binary buffers, so the server can often consume wire bytes without a byte swap.

Filter expressions are unaffected by this: a predicate like `price > 19.99` carries its literal as text *inside the expression string*, which the parser converts (see [Query filters](#query-filters)). That is expression syntax, not wire data.

### Decimal64 → Decimal128/256: the growth path and why it isn't a breaking change

The previous revision of this doc permanently coupled `Decimal` to `int64`/precision≤18. Review feedback asked for a public type closer to PostgreSQL `NUMERIC`, with a clear path to wider precision that doesn't require redefining the wire contract for fields that already exist.

**The mechanism: width is a pure function of the field's own declared `precision`, never a runtime tag on the wire.** A reader decoding `bytes_data` for a given field already knows that field's schema — including its `precision` — before it reads a single byte. So the decode width is looked up from schema metadata (`FieldMeta::DecimalInfo` in C++, the `(precision, scale)` type params in Go), not sniffed from the payload. This is exactly the convention Apache Arrow already uses for `Decimal128`/`Decimal256` (precision selects the physical width), so `DECIMAL_PRECISION`/`DECIMAL_SCALE` schema metadata plus the table above is sufficient — no additional wire-format version byte or envelope is needed.

Concretely, this means:
- A `DECIMAL(10, 2)` field written today will *always* decode as 8 bytes, forever — its precision never changes after creation (schema evolution can add new fields but does not currently support widening an existing field's declared precision). Old data is never reinterpreted under a new rule.
- Shipping Decimal128 later means: (a) bump `MaxDecimalPrecision` past 18, (b) add the 16-byte codec (`EncodeDecimalBytes128`/`DecodeDecimalBytes128` alongside, not replacing, the existing 8-byte ones), (c) add a second growing-segment storage representation for the 19–38 precision tier (see "Growing-segment memory layout"), (d) add index support for 128-bit comparison. None of that touches how existing precision≤18 fields are stored, read, or compared.
- This is why "precision selects width" was chosen over a self-describing envelope (e.g. a leading width byte on every value): a per-value tag would cost 1 byte on every single Decimal value forever, for information the schema already carries for free.

**What this doc commits to now:** only Decimal64 (precision 1–18) is implemented. Decimal128/256 are a reserved range and a stated mechanism, not code — they are out of scope for this PR and tracked as follow-up work once Decimal64 has landed and proven out the pattern.

### Rounding, overflow, and cross-scale arithmetic semantics

| Case | Behavior | Rationale |
| --- | --- | --- |
| Insert literal has more fractional digits than `scale` | **Rejected** (`ValidateDecimalString` error), not rounded or truncated. | Decimal's entire purpose is exact round-tripping; silently rounding on insert would reintroduce the precision loss it exists to eliminate. |
| Insert literal's integer part needs more than `precision - scale` digits | **Rejected** (`ValidateUnscaledValue` / the `integer_digits <= precision - scale` invariant). | Same reasoning — no silent truncation of magnitude. |
| `Add`/`Sub` arithmetic result overflows `int64` | **Not currently detected — matches `INT64`'s existing behavior.** Milvus does not special-case overflow detection for plain `INT64` arithmetic today, and `Decimal` reuses `INT64`'s `ExecRangeVisitorImpl<int64_t>` verbatim (see "Arithmetic" below), so it inherits the same silent-wraparound behavior. This is called out explicitly rather than left implicit, since a user filtering `price - 5 > 10` on a value near `int64` bounds gets undefined results, same as they would filtering `some_int64_field - 5 > 10` today. | Fixing this is a pre-existing `INT64` arithmetic gap, not something specific to introduce and solve only for `Decimal`. Tracked as a shared follow-up, not blocking this PR. |
| Cross-scale arithmetic between two different-scale `Decimal` columns (`price_usd - price_eur`) | **Rejected outright** — field-vs-field arithmetic was already rejected for all types before this change (`handleBinaryArithExpr`), and this PR does not lift that restriction for `Decimal`. | Aligning two different scales before subtracting needs its own rescale-to-common-scale logic; out of scope until there's a concrete need. |
| `Decimal` column arithmetic against a literal (`price - 5`) | The literal is re-derived from its exact source text and rescaled to the column's declared scale before the operation — so no cross-scale case actually arises here, only same-scale `int64` arithmetic. | See "Arithmetic" below; this is implemented today. |

### Growing-segment memory layout

`Decimal` (Decimal64) reuses `Int64`'s `ConcurrentVector` chunked layout unmodified: 8 bytes per value, the same per-chunk validity bitmap every nullable scalar type uses, no separate memory representation. The `(precision, scale)` interpretation is attached at the `FieldMeta` level (`DecimalInfo`), not encoded into the growing-segment buffer itself — the buffer is indistinguishable in memory from an `Int64` column of the same nullability.

If Decimal128/256 ship later, they would need their own growing-segment representation (16 or 32 bytes/value is not what any existing `ConcurrentVector<T>` instantiation stores today), which is part of why that work is scoped separately rather than folded into this PR.

### Storage V1/V2 binlogs and Storage V3 Arrow representation

- **V1/V2 binlogs:** `PayloadWriter`/`PayloadReader` treat Decimal64 exactly like `Int64` at the physical (Arrow/Parquet) layer — the binlog payload is raw `int64` values (`AddDecimalToPayload`/`GetDecimalFromPayload`, `internal/storage/payload_reader.go`, `payload.go`). The `(precision, scale)` interpretation lives in collection schema metadata, not in the binlog payload, so the payload format needs no changes to support wider precision tiers later — only the growing-segment/`ConcurrentVector` and codec layers do.
- **Storage V3 (Arrow-native columnar):** Decimal64 is currently written as a plain Arrow `Int64` array, **not** Arrow's native `Decimal128` logical type. This is a known, explicitly-scoped simplification for v1: a tool reading a Storage V3 file directly via Arrow, bypassing Milvus's own read path, sees an ordinary int64 column with no self-describing indication it's actually a scaled decimal — it would need the collection schema's `(precision, scale)` out-of-band to reinterpret it correctly. Mapping to Arrow's native `Decimal128`/`Decimal256` logical type in Storage V3 (which *is* self-describing) is tracked as follow-up work, not done here.

### Flush, reload, mmap, compaction, and schema-evolution behavior

- **Flush/reload:** round-trips through the V1/V2/V3 paths above with the same guarantees as `Int64`/`Timestamptz` — decoded values are the same raw `int64` at every stage, only the proxy/SDK boundary does bytes↔text conversion.
- **mmap:** reuses the generic `ChunkedColumn` fixed-width-element mmap machinery already used for `Int64`; no Decimal-specific mmap code exists or is needed, since Decimal64 is memory-layout-identical to `Int64`.
- **Compaction:** `internal/storage/data_sorter.go` sorts Decimal as a plain comparable `int64` — the order-preserving property of the unscaled-integer encoding (see "Why unscaled int64") is what makes this correct without any Decimal-aware comparator.
- **Schema evolution (add-field):** adding a new nullable-or-defaulted `Decimal` field to an existing collection backfills existing segments through the same default-value path every scalar type uses (`SegmentInterface.cpp`'s default-value backfill) — it writes the same default value to every pre-existing row unconditionally. This does **not** interact with per-row `valid_data` (there's no "some old rows are null, some aren't" distinction to make; it's a whole-column backfill), which is why it wasn't part of the `valid_data`-migration fallout described below.

### Query operations and aggregations supported

**Supported today:** equality/relational comparison (`=`, `!=`, `<`, `<=`, `>`, `>=`), range (`BETWEEN`-style), `IN`, `IS NULL`/`IS NOT NULL`, and `Add`/`Sub` arithmetic against a literal (see "Query filters" and "Arithmetic" below).

**Not supported, explicitly out of scope for this PR:**
- `Multiply`/`Divide`/`Modulo` arithmetic (rejected with a clear error — needs real fixed-point rescaling logic, not just constant-rescale).
- Field-vs-field arithmetic.
- Aggregation functions (`SUM`, `AVG`, `MIN`/`MAX` as aggregates rather than index-assisted point lookups). These have not been wired up or tested against `Decimal` — this is flagged as an open gap rather than assumed to work "because it's just an int64 underneath," since Milvus's aggregation dispatch is its own type-switch layer (parallel to the query-filter and query-output switches this PR already had to add cases to) that hasn't been audited for a `Decimal`/`BytesData` case.

### Supported index types and raw-data retrieval

Only `STL_SORT` (sorted array + binary search) is wired up, exactly as for `Int64`/`Timestamptz` — see the existing "Indexing" section below for the modification points. `BITMAP`/`HYBRID` remain explicitly out of scope (unchanged from the prior revision of this doc). Raw-data retrieval (`output_fields`, get-by-id) goes through the same `bulk_subscript` encode path documented under "Nullable rows and the `valid_data` migration" — already correct and unchanged by this revision.

### SDK, REST, and bulk-import responsibilities

| Surface | Responsibility | Status |
| --- | --- | --- |
| SDK (Go/Python/etc.) | Accept a decimal string or language-native decimal type from the user; convert to the canonical unscaled wire bytes using the field's schema-declared scale before sending; convert wire bytes back to text/native type on read. The user never sees raw bytes. | Go SDK implemented (`EncodeUnscaledInt64`, wire codec) — other language SDKs not yet audited against this contract; flagged as open. |
| REST v2 (JSON) | Represent Decimal values as **JSON strings**, not JSON numbers — a JSON number is parsed as an IEEE-754 double by virtually every JSON library, which would silently reintroduce the exact rounding-error problem Decimal exists to eliminate. | **Not yet implemented/verified.** No REST-layer changes have been made in this PR; tracked as an open follow-up rather than assumed to inherit SDK behavior. |
| Bulk import (JSON/Parquet/CSV) | Same string-representation rationale as REST; should route through the same `EncodeUnscaledInt64`/`ValidateDecimalString` validation the proxy uses for regular inserts, so bulk-imported and directly-inserted rows are held to identical rules. | **Not yet implemented/verified.** Tracked as an open follow-up. |

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

### Nullable rows and the `valid_data` migration

For a nullable field, the proxy expands null rows into an **empty bytes placeholder** and marks them via the field's `valid_data`. Both decode boundaries must consult `valid_data` and skip those positions — attempting to decode the placeholder would fail an otherwise valid insert.

**Where `valid_data` lives changed underneath this PR, independently of Decimal.** Milvus master migrated `valid_data` off the top-level `FieldData` message and onto `ScalarField`/`VectorField` directly ([milvus#52203](https://github.com/milvus-io/milvus/pull/52203), adapting to a `milvus-proto` change, [milvus#51162](https://github.com/milvus-io/milvus/issues/51162)). The motivation: nested nullable types (nullable vector-arrays, nullable struct-array sub-fields) need validity to live *at the nesting level it describes*, not only at the outermost row level, so a single flat `FieldData.valid_data` array can't express "element 3 inside this row is null." `FieldData.valid_data` is now a legacy-compatibility fallback only; `typeutil.ValidateAndNormalizeFieldDataValidData` runs once at the proxy input boundary and moves validity into the new location, clearing the legacy one.

Decimal is a flat scalar with no nesting, so none of the new recursive semantics apply to it directly — the only consequence is *where* the same one-bool-per-row array physically lives. But it means every direct read of the old location silently breaks once normalization has run. This was caught during this PR's own rebase onto master (the migration landed on master after this branch's proto pin) and fixed at all five call sites that still read the legacy field directly:

- `internal/proxy/validate_util.go`: `checkDecimalFieldData`, `FillWithNullValue`'s and `FillWithDefaultValue`'s `ScalarField_BytesData` cases (3 call sites — all now route through `typeutil.GetFieldDataValidData`, matching every sibling scalar type's case in the same functions)
- `internal/storage/utils.go`: `ColumnBasedInsertMsgToInsertData`'s Decimal case (now uses `typeutil.GetFieldDataValidData`, matching the ~20 sibling type cases in the same switch)
- `internal/core/src/segcore/ConcurrentVector.cpp`: `VectorBase::set_data_raw`'s `DECIMAL` case (now uses `GetFieldDataRowValidData`, matching the `VECTOR_ARRAY` case in the same function)

The query-*output* side (`SegmentGrowingImpl.cpp`'s and `ChunkedSegmentSealedImpl.cpp`'s `bulk_subscript`) needed no change: both already set validity generically for every field, of any type, once per call, *before* dispatching into the per-type switch — so Decimal inherited correct behavior there automatically. Only the insert/decode paths, which do type-specific work inline per case, had the stale references.

One ordering subtlety unrelated to the migration: proxy validation (`checkDecimalFieldData`) runs **before** null expansion (`fillWithValue`), so it normally sees a compact array of real values, while `internal/storage` and segcore run **after** and see the padded array. The null check is written to be correct in both shapes rather than assuming one.

### Precision and scale validation

`DECIMAL(p, s)` admits at most `p` significant digits of which `s` are fractional, so the invariant is:

```
fractional_digits <= s          AND          integer_digits <= p - s
```

where leading zeros are not significant digits. The second half matters independently of how many fractional digits a literal actually spells out: counting only the digits present would accept `"12345"` into `DECIMAL(5,2)` (it scales up to `1234500` — seven digits, past the declared precision) and reject `"0.001"` from `DECIMAL(3,3)` (which is legal — no significant integer digits, three fractional). Both cases are pinned in `TestValidateDecimalStringPrecisionScaleInvariant`.

On the server side, values arriving as canonical bytes are checked against `ValidateUnscaledValue` — `|unscaled| <= 10^precision - 1` — which is the same invariant expressed on the already-scaled integer.

### Worked examples

All examples assume `scale <= precision` per the bounds table above; unscaled values are shown in base 10 for readability (the actual wire form is the 8-byte little-endian encoding of this integer).

| Field | Literal | Result | Unscaled value | Why |
| --- | --- | --- | --- | --- |
| `DECIMAL(5, 2)` | `"19.99"` | Accepted | `1999` | 2 integer digits ≤ `5-2=3`, 2 fractional digits ≤ `2`. |
| `DECIMAL(5, 2)` | `"019.99"` | Accepted | `1999` | Leading zero on the integer part is not a significant digit. |
| `DECIMAL(5, 2)` | `"12345"` | **Rejected** | — | No fractional part written, but scaling up still needs 5 integer digits, and `5 > 5-2=3`. |
| `DECIMAL(5, 2)` | `"999.99"` | **Rejected** | — | 3 integer digits > `3`. |
| `DECIMAL(5, 2)` | `"1.999"` | **Rejected** | — | 3 fractional digits > `2` — not rounded to `2.00`, rejected outright (see "Rounding, overflow, and cross-scale arithmetic semantics"). |
| `DECIMAL(3, 3)` | `"0.001"` | Accepted | `1` | 0 integer digits ≤ `3-3=0`, 3 fractional digits ≤ `3`. |
| `DECIMAL(3, 3)` | `"1.000"` | **Rejected** | — | 1 integer digit > `3-3=0`. |
| `DECIMAL(10, 0)` | `"42"` | Accepted | `42` | Scale-0 Decimal behaves as an exact integer. |
| `DECIMAL(10, 0)` | `"4.2"` | **Rejected** | — | 1 fractional digit > `0`. |
| `DECIMAL(6, 2)` | `"-123.45"` | Accepted | `-12345` | Sign doesn't count as a digit; magnitude check is on digits only. |
| `DECIMAL(6, 2)` | `"0.00"` | Accepted | `0` | Zero is representable at any scale. |
| `DECIMAL(18, 0)` | `"999999999999999999"` | Accepted | `999999999999999999` | Exactly `MaxDecimalPrecision`, fits `int64` — pinned by `TestValidateUnscaledValue`'s limit case. |
| `DECIMAL(19, 0)` | any | **Rejected at schema-creation time** | — | `precision > MaxDecimalPrecision (18)`; the Decimal128 tier this would need is reserved but not implemented (see growth-path section). |
| `DECIMAL(5, -1)` | any | **Rejected at schema-creation time** | — | Negative scale is not allowed, v1 or reserved (see bounds table). |

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

`price - 5 > 10` parses as `Compare(BinaryArithExpr(price, Sub, 5), 10)`. Both the arithmetic operand (`5`) and the comparison threshold (`10`) are re-derived from their exact source text and rescaled to the column's declared scale — reusing the same `EncodeUnscaledInt64` fixup mechanism plain comparisons already use — so the unscaled-`int64` subtraction and the final comparison both stay correct. The parser-level fixup (`fixupDecimalOperands` in `parser_visitor.go`) now handles both shapes: a direct `Decimal` column, or a `BinaryArithExpr` wrapping one. On the C++ side, `DataType::DECIMAL` reuses `ExecRangeVisitorImpl<int64_t>` verbatim in `BinaryArithOpEvalRangeExpr.cpp` — identical to `INT64` — since Add/Sub-by-a-rescaled-constant is scale-preserving. See "Rounding, overflow, and cross-scale arithmetic semantics" above for what happens (and doesn't) on overflow.

**Multiply/divide/modulo are explicitly rejected** (`internal/parser/planparserv2/parser_visitor.go`, `VisitMulDivMod`): unlike Add/Sub, an integer multiplier must stay *unscaled* while a fractional one needs real fixed-point rescaling to avoid corrupting the result's magnitude — that logic isn't implemented yet, so these ops fail loudly with a clear error rather than silently producing a wrong answer. Field-vs-field arithmetic (`price - other_price`) was already rejected for all types before this change (`handleBinaryArithExpr`), so it remains out of scope here too — mixing two potentially different scales needs its own alignment logic.

## Appendix: Modification Points

### Proto (milvus-proto, separate repo)

| File | Change |
| --- | --- |
| `schema.proto` | `Decimal = 30` added to `DataType` enum ([PR #630](https://github.com/milvus-io/milvus-proto/pull/630), `0fb0d5b`) |
| `schema.proto` | `BytesArray` / `Decimal` comments define the canonical 8-byte little-endian wire encoding ([PR #650](https://github.com/milvus-io/milvus-proto/pull/650), `c45eae9`) |

### Go — Type System & Validation

| File | Change |
| --- | --- |
| `pkg/util/typeutil/schema.go` | `IsDecimalType()`, wired into `IsPrimitiveType`; `CalcScalarSize` crash fix |
| `pkg/util/typeutil/gen_empty_field_data.go` | empty-`FieldData` case for `Decimal` |
| `pkg/util/parameterutil/get_decimal_params.go` | `GetPrecisionAndScale`; `ValidateDecimalString` (incl. the `integer_digits <= p - s` invariant); `EncodeUnscaledInt64` (text→int64, SDK/parser-facing); `EncodeUnscaledBytes`/`DecodeUnscaledBytes` (canonical 8-byte wire codec); `MaxUnscaledValue`/`ValidateUnscaledValue` |
| `internal/proxy/validate_util.go` | `checkDecimalFieldData` (wire-width + precision, using `typeutil.GetFieldDataValidData` to skip null placeholders); `FillWithNullValue`/`FillWithDefaultValue`'s `BytesData` cases (same); default-value decode; null/default fill wiring |
| `internal/rootcoord/util.go` | schema validation; default value decoded from the canonical wire form |

### Go — Storage Layer

| File | Change |
| --- | --- |
| `internal/storage/insert_data.go` | `DecimalFieldData` type, full `FieldData` interface impl, wired into `NewFieldData` |
| `internal/storage/utils.go` | wire decode in `ColumnBasedInsertMsgToInsertData` (uses `typeutil.GetFieldDataValidData` to skip null placeholders), `mergeDecimalField` + `MergeFieldData` switch entry |
| `internal/storage/serde.go`, `data_codec.go`, `payload_writer.go`, `payload.go`, `payload_reader.go` | Arrow/binlog serialization, both StorageV1/V2 (plain `Int64` layout — see "Storage V1/V2 binlogs" above) and V3 paths |
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
| `internal/core/src/segcore/ConcurrentVector.cpp` | wire decode at `VectorBase::set_data_raw`, using `GetFieldDataRowValidData` to skip null placeholders |

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
| `internal/core/src/segcore/SegmentGrowingImpl.cpp` | memory-size accounting, growing-segment result building (`EncodeDecimalBytes` call site; validity already handled generically pre-dispatch — see "Nullable rows and the `valid_data` migration") |
| `internal/core/src/segcore/Utils.cpp` | `SetUpScalarFieldData` pre-allocation (prerequisite fix — missing pre-allocation would have crashed on out-of-bounds write) |
| `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp` | raw accessor, sealed-segment result building (`EncodeDecimalBytes` call site; validity already handled generically pre-dispatch), external-table export (Int64-only, does not round-trip to decimal text — see "Storage V3" above) |
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

### Unit coverage (implemented)

| Area | Test |
| --- | --- |
| C++ schema parsing / enum drift | `FieldMetaTest.DecimalEnumMirrorsProto`, `FieldMetaTest.DecimalParseFromRoundTrip` |
| Wire codec round-trip, wrong-width rejection (incl. the empty null placeholder) | `TestUnscaledBytesRoundTrip`, `TestDecodeUnscaledBytesRejectsWrongWidth`, `DecimalWireFormat.EncodeDecodeRoundTrip` |
| Precision/scale invariant | `TestValidateDecimalStringPrecisionScaleInvariant`, `TestValidateUnscaledValue` |
| Filter expressions (comparisons, ranges, `IN`, null checks, Add/Sub arithmetic, Mul/Div/Mod rejection) | `internal/parser/planparserv2/decimal_test.go` |
| Query/upsert result copying (`PrepareResultFieldData`, `AppendFieldData`, `AppendFieldDataByColumn`, `getScalarDataLen`, `getData`) | `TestAppendFieldDataDecimal`, `TestAppendFieldDataByColumnDecimal`, `TestScalarDataLenAndGetDataDecimal`, `TestDeleteFieldDataDecimal`, `TestUpdateFieldDataDecimal`, `TestUpdateFieldDataByColumnDecimal`, `TestMergeFieldDataDecimal` (`pkg/util/typeutil/schema_test.go`) |

### End-to-end coverage (required before merge, not yet implemented)

Per review feedback, unit and golden-vector coverage alone don't exercise the full insert→...→output round trip. The following scenarios need real E2E tests (Go integration tests under `tests/integration/`, following the pattern of existing suites like `tests/integration/null_data`) before this is merge-ready — none of these exist yet:

1. Insert and immediate growing-segment query/search.
2. Nullable and default-value rows, including the `valid_data`-location fix above.
3. Literal and template filters (comparisons, ranges, `IN`, Add/Sub).
4. Multi-segment search output (result merging across segments).
5. Flush and sealed-segment reload.
6. `STL_SORT` indexed retrieval.
7. Storage V3 growing-source flush.
8. Compaction and restart.
9. Add-field schema evolution (default backfill on existing segments).
10. Precision/scale/rounding/overflow boundaries (mirroring the worked-examples table above, but through the real insert path rather than unit-testing the validator directly).

The same logical Decimal value should round-trip identically through SDK input → wire encoding → growing storage → flush → sealed storage → indexing → compaction/restart → SDK output. That end-to-end identity is the acceptance bar for this feature and is not yet demonstrated by any single test.

## Open Follow-ups

1. Decimal128/256 tiers (precision 19–76) — mechanism reserved (see growth-path section), no implementation.
2. `BITMAP`/`HYBRID` indexing for `Decimal` (cardinality-based auto-selection).
3. `Multiply`/`Divide`/`Modulo` arithmetic on `Decimal` (needs fixed-point rescaling logic distinct from `Add`/`Sub`'s constant-rescale approach).
4. Field-vs-field arithmetic (`price - other_price`), including the general case of two `Decimal` columns with different scales.
5. `Add`/`Sub` overflow detection — currently silent-wraparound, matching (not worse than) existing `INT64` behavior; a real fix is a shared `INT64`/`Decimal` concern, not Decimal-specific.
6. Aggregation functions (`SUM`/`AVG`/etc.) over `Decimal` — not audited, not tested, status unknown.
7. Storage V3 Arrow-native `Decimal128` logical-type mapping (currently plain `Int64`, not self-describing to external Arrow readers).
8. REST v2 and bulk-import (JSON/Parquet/CSV) support — not yet implemented or verified against the string-representation contract SDKs use.
9. Negative-scale support — deliberately deferred, no motivating use case yet (see bounds table).
10. The full end-to-end test matrix above.
