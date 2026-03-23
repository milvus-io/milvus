# INT64_MULTIVALUE Field Type — Implementation Plan

## Problem

DoorDash-style search: stores have delivery zones represented as 40K+ H3 Level 8 cell IDs (int64). A user query maps their location to one H3 cell and filters stores whose delivery zone contains that cell, combined with vector search.

Current ARRAY type has a hard cap of 4096 elements (`defaultMaxArrayCapacity` in `internal/proxy/util.go:76`), which is insufficient. Raising the array limit is rejected because ARRAY carries generic semantics (range, equality, null, element_filter) that are unnecessary and expensive at 40K+ elements.

## Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Name** | `INT64_MULTIVALUE` | Honest — no dedup, not a mathematical set |
| **Dedup** | No | Unnecessary latency; `contains` semantics identical with/without dupes |
| **v1 operators** | `contains` only (single value) | DoorDash use case is always "does zone contain my H3 cell?" |
| **Capacity** | Dedicated param `max_elements`, built-in cap 65536 | Not `max_capacity` — distinct type, distinct contract |
| **Index** | Explicit `create_index` required. Load fails without it | No auto-index magic. User controls when index is built |
| **Wire format** | Reuse existing `array_data` (ArrayArray, element_type=Int64) | No new proto message. Codec and client plumbing stays unchanged |
| **Storage** | Reuse `FieldData<Array>` / `ArrayFieldData` internally | No new C++ class. Element type hardwired to INT64 |
| **Output fields** | Rejected. Filter-only | New `IsFilterOnlyField()` check, not BM25 repurpose |
| **Nullability** | Not nullable in v1 | Empty array = no memberships. No null vs empty distinction needed |

## Index Lifecycle State Machine

```
create_collection(schema with INT64_MULTIVALUE field)
    → schema stored, no index

insert / import
    → data stored as array-of-int64 in binlog (existing array_data wire format)

create_index(field_name, index_type="BITMAP")
    → datacoord builds BitmapIndex on the field
    → auto-index selection: getPrimitiveIndexType(Int64) → BITMAP or HYBRID
    → follows existing scalar index path (task_index.go:301-338)

load
    → querycoord job_load.go checks: INT64_MULTIVALUE field must have index_id != 0
    → if no index: return error "INT64_MULTIVALUE field requires an index; call create_index first"

search / query with "contains(field, value)"
    → sealed segments: BitmapIndex lookup (O(1) per value)
    → growing segments: brute-force ExecArrayContains (acceptable for small growing buffer)
```

**Precedent note**: No existing Milvus type requires an index to load. This is a new restriction specific to INT64_MULTIVALUE, justified because brute-force scan over 40K+ elements per row per query is unusable. Enforcement point: `internal/querycoordv2/job/job_load.go:138`.

## Proto Change

Only change: add `Int64Multivalue = 28` to `DataType` enum in `schema.proto`.

**No new message type.** Wire format reuses `ArrayArray` with `element_type = Int64`. This avoids multiplying codec and compatibility work across `client/column/array.go`, `internal/core/src/segcore/SegmentInterface.cpp:341`, and all other plumbing built around `array_data`.

## C++ Changes

### Layer 1 — Type System (3 files, 6 touches)

| File | Line(s) | Change |
|------|---------|--------|
| `src/common/Types.h` | 184 | `case DataType::INT64_MULTIVALUE: return proto::schema::DataType::Int64Multivalue;` |
| `src/common/Types.h` | 244 | `case DataType::INT64_MULTIVALUE:` alongside ARRAY → `arrow::binary()` |
| `src/common/Types.h` | 332 | `case DataType::INT64_MULTIVALUE: return "int64_multivalue";` |
| `src/common/Types.h` | ~760 | `TypeTraits<DataType::INT64_MULTIVALUE>` specialization (copy ARRAY traits) |
| `src/common/Types.h` | 440 | Add to `IsArrayDataType()` or add new `IsMultivalueDataType()` |
| `src/common/Types.cpp` | 49 | `case DataType::INT64_MULTIVALUE: return false;` (variable width) |

### Layer 2 — Storage & Serialization (3 files, 5 touches)

| File | Line(s) | Change |
|------|---------|--------|
| `src/storage/Util.cpp` | 445 | Arrow builder — `case DataType::INT64_MULTIVALUE:` → `arrow::BinaryBuilder` |
| `src/storage/Util.cpp` | 656 | Arrow schema — → `arrow::binary()` |
| `src/storage/Util.cpp` | 1229 | FieldData creation — → `FieldData<Array>` |
| `src/storage/Event.cpp` | 303 | Payload serialization — same as ARRAY |
| `src/common/FieldData.cpp` | 282 | Arrow deserialization — same as ARRAY |

**Key decision**: Reuse `Array` / `FieldData<Array>` as the in-memory representation. INT64_MULTIVALUE is stored identically to an Array of int64. This avoids a new C++ class and leverages all existing bulk operations.

### Layer 3 — Segment Operations (5 files, 12 touches)

| File | Line(s) | Change |
|------|---------|--------|
| `src/segcore/InsertRecord.h` | 1100 | `case DataType::INT64_MULTIVALUE:` → `append_data<Array>(...)` |
| `src/segcore/ConcurrentVector.cpp` | 134 | `set_data_raw` — same as ARRAY |
| `src/segcore/Utils.cpp` | 192, 427, 627, 948 | Size calc, DataArray init, binlog prep (4 switches) |
| `src/segcore/ChunkedSegmentSealedImpl.cpp` | 1732, 2151 | Bulk subscript — same as ARRAY |
| `src/segcore/SegmentGrowingImpl.cpp` | 1405, 1731, 421 | Bulk subscript, memory calc (3 switches) |

### Layer 4 — Index Factory (3 files, 3 touches)

| File | Line(s) | Change |
|------|---------|--------|
| `src/indexbuilder/IndexFactory.h` | 63 | `case DataType::INT64_MULTIVALUE:` → `CreateScalarIndex(...)` |
| `src/index/IndexFactory.cpp` | 666 | `case DataType::INT64_MULTIVALUE:` → `CreateCompositeScalarIndex(...)` |
| `src/index/BitmapIndex.cpp` | 167 | **CRITICAL**: `case DataType::INT64_MULTIVALUE: return BuildArrayField<int64_t>(...)` |

`BitmapIndex.cpp:167` is the single most critical line. Without it, no index gets built. `BuildArrayField` (lines 182-199) already iterates each row's array elements and calls `data_[val].add(offset)` — the exact bitmap structure needed for `contains`.

### Layer 5 — Expression Evaluation (1 file, 1 touch)

| File | Line(s) | Change |
|------|---------|--------|
| `src/exec/expression/JsonContainsExpr.cpp` | 159 | `case DataType::INT64_MULTIVALUE:` — index path for int64 only |

**No changes to**: `UnaryExpr.cpp`, `BinaryRangeExpr.cpp`, `TermExpr.cpp`, `NullExpr.cpp`, `ValueExpr.cpp`. v1 is `contains` only — no range, equality, term, or null semantics on the field.

### Layer 6 — Plan Validation (1 file, ~10 touches)

| File | Line(s) | Change |
|------|---------|--------|
| `src/query/PlanProto.cpp` | 832, 858, 877, ... | `Assert(data_type == DataType::ARRAY)` → `Assert(data_type == DataType::ARRAY \|\| data_type == DataType::INT64_MULTIVALUE)` |

Only the asserts guarding the `contains` expression path need updating in v1. Asserts for unary, range, and binary range expressions stay ARRAY-only since those expressions aren't wired for INT64_MULTIVALUE.

### C++ Summary: 16 files, ~37 touch points

## Go Changes

### Layer 7 — Type Utilities (3 files, 5 touches)

| File | Change |
|------|--------|
| `pkg/util/typeutil/schema.go` | Add `IsInt64MultivalueType()`, include in `IsVariableDataType()`, size estimation |
| `pkg/util/funcutil/func.go` | `GetNumRowOfFieldData` — 2 case statements |
| `pkg/util/parameterutil/get_max_len.go` | New `GetMaxElements()` for INT64_MULTIVALUE — distinct from `GetMaxCapacity()` |

### Layer 8 — Proxy Validation (3 files, 7 touches)

| File | Change |
|------|--------|
| `internal/proxy/util.go:558` | Schema validation: INT64_MULTIVALUE requires `max_elements` param, element_type fixed to INT64 |
| `internal/proxy/util.go:1671,1715` | Output field rejection via new `IsFilterOnlyField()` |
| `internal/proxy/validate_util.go:175` | Add `checkInt64MultivalueFieldData()` — validate int64 values, check `max_elements ≤ 65536` |
| `internal/proxy/task_index.go:319-324` | Add INT64_MULTIVALUE → `getPrimitiveIndexType(DataType_Int64)` |

### Layer 9 — Load Validation (1 file, 1 touch)

| File | Change |
|------|--------|
| `internal/querycoordv2/job/job_load.go:138` | After building `fieldIndexIDs`, check: for each INT64_MULTIVALUE field, `indexId != 0` or return error `"INT64_MULTIVALUE field requires an index; call create_index first"` |

This is a new restriction with no existing precedent. Current Milvus allows loading any field without index (falling back to brute-force scan). The restriction is justified because 40K+ element brute-force scan per row is unusable.

### Layer 10 — Storage (6 files, 10 touches)

| File | Change |
|------|--------|
| `internal/storage/insert_data.go:371` | → `ArrayFieldData` with element_type=Int64 |
| `internal/storage/payload_writer.go:205,1144` | Same binary payload as ARRAY |
| `internal/storage/payload_reader.go:172` | Same binary reader as ARRAY |
| `internal/storage/data_codec.go:420,702` | Same event encoding/decoding |
| `internal/storage/serde.go:460` | Map to `eagerArrayEntry` |
| `internal/storage/data_sorter.go:119` | Same sorting |

### Layer 11 — Import (5 files, 8 touches)

| File | Change |
|------|--------|
| `internal/util/importutilv2/parquet/util.go` | → Arrow List(Int64) |
| `internal/util/importutilv2/parquet/field_reader.go` | Read as array of int64 |
| `internal/util/importutilv2/csv/row_parser.go` | Parse JSON array of int64, validate `max_elements` |
| `internal/util/importutilv2/json/row_parser.go` | Parse array of int64, validate `max_elements` |
| `internal/util/importutilv2/common/` | Capacity validation with 65536 limit |

### Layer 12 — HTTP Server (3 files, ~12 touches)

| File | Change |
|------|--------|
| `internal/distributed/proxy/httpserver/utils.go` | ~12 type-switch cases (JSON serialization, schema display, etc.) |
| `internal/distributed/proxy/httpserver/handler_v2.go` | Schema display |
| `internal/distributed/proxy/httpserver/request_v2.go` | RESTful API validation |

### Layer 13 — Plan Parser (3 files, 4 touches)

| File | Change |
|------|--------|
| `internal/parser/planparserv2/parser_visitor.go:1343` | Allow `array_contains` on INT64_MULTIVALUE |
| `internal/parser/planparserv2/utils.go:167` | Type resolution |
| `internal/parser/planparserv2/rewriter/util.go:40` | Include INT64_MULTIVALUE alongside ARRAY/JSON |

### Layer 14 — RootCoord (1 file, 3 touches)

| File | Change |
|------|--------|
| `internal/rootcoord/util.go:361,465,517` | Schema validation for new type |

### Go Summary: 28 files, ~64 touch points

## What's Cut From v1

| Cut | Reason |
|-----|--------|
| `contains_any`, `contains_all` | Single-value lookup covers the use case |
| UnaryExpr, BinaryRangeExpr, TermExpr, NullExpr wiring | `contains` only — no range/equality/null on the field itself |
| New proto message | Reuse `array_data` |
| Auto-index at load | Explicit `create_index` required |
| Dedup | Unnecessary for `contains` semantics |
| Nullable | Empty array = no memberships |
| `max_capacity` reuse | Dedicated `max_elements` param |

## Total Scope

| | Files | Touch Points |
|--|-------|-------------|
| Proto | 1 | 1 |
| C++ | 16 | ~37 |
| Go | 28 | ~64 |
| **Total** | **45 files** | **~102 touch points** |

## Risk Assessment

**Low risk (mechanical type-switch additions)**: Layers 1-3, 7, 10-11 — copy ARRAY pattern, change case label. ~70% of the work.

**Medium risk**:
- Layer 9 (load validation) — new restriction with no precedent, needs clear error message and test coverage for both "no index" and "index exists" paths.
- Layer 12 (HTTP server) — many touch points in a large utils.go, easy to miss one.

**Highest risk**:
- Layer 6 (`PlanProto.cpp` asserts) — must identify exactly which asserts guard the `contains` path vs other expression paths. Opening the wrong assert would expose expression paths we didn't wire.
- Layer 4 (`BitmapIndex.cpp:167`) — single most critical line; if missed, index silently fails to build.

## Code Evidence

Key files that prove the scope:
- `internal/core/src/index/BitmapIndex.cpp:167` — hard-coded `DataType::ARRAY` dispatch to `BuildArrayField`
- `internal/core/src/exec/expression/JsonContainsExpr.cpp:159` — hard-coded `DataType::ARRAY` for contains expression
- `internal/core/src/query/PlanProto.cpp:829+` — ~10 `Assert(data_type == DataType::ARRAY)` for element-level operations
- `internal/core/src/storage/Util.cpp:1229` — hard-coded `DataType::ARRAY` in `CreateFieldData`
- `internal/proxy/util.go:76` — `defaultMaxArrayCapacity = 4096` (the limit we're bypassing)
- `internal/querycoordv2/job/job_load.go:138` — load validation enforcement point
- `internal/proxy/task_index.go:301-338` — existing scalar auto-index selection path
