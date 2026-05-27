# MEP: Support Sort/Bitmap/Hybrid Index Types for JSON Path Index

- **Created:** 2026-04-10
- **Author(s):** @zhengbuqian
- **Status:** Draft
- **Component:** Index | QueryNode | Proxy | DataCoord
- **Related Issues:** TBD

## Summary

Extend JSON Path Index to support Sort, Bitmap, and Hybrid index types. AUTOINDEX for JSON Path Index routes to HYBRID, which dynamically selects the optimal index type based on data cardinality — matching the behavior of regular scalar columns.

## Motivation

JSON Path Index currently only supports the Inverted (Tantivy) index type. Different query patterns benefit from different index structures:

- **Sort Index**: Efficient for range queries (`>`, `<`, `>=`, `<=`, `BETWEEN`) on numeric JSON keys. Binary search with O(log n) lookup.
- **Bitmap Index**: Efficient for equality and `IN` queries on low-cardinality JSON keys (e.g., status codes, enum-like string values). Roaring bitmaps provide compact storage and fast set operations.
- **Hybrid Index**: Automatically selects between Bitmap and Sort based on data cardinality at build time — the best default when the user doesn't know the data distribution.

Regular scalar columns already support AUTOINDEX → HYBRID. JSON Path Index should have the same capability.

## Public Interfaces

### CreateIndex API

No proto changes required. The existing `index_params` map already supports `index_type`, `json_path`, and `json_cast_type`. Users specify a different `index_type`:

```python
# Sort index on a numeric JSON key
index_params = {
    "index_type": "STL_SORT",
    "json_path": "metadata['price']",
    "json_cast_type": "DOUBLE",
}

# Bitmap index on a low-cardinality string JSON key
index_params = {
    "index_type": "BITMAP",
    "json_path": "metadata['status']",
    "json_cast_type": "VARCHAR",
}
```

### Compatibility Matrix: json_cast_type x index_type

| json_cast_type | INVERTED | STL_SORT | BITMAP | HYBRID |
|---|---|---|---|---|
| BOOL | Y | N | Y | Y |
| DOUBLE | Y | Y | N | Y |
| VARCHAR | Y | Y | Y | Y |
| ARRAY_BOOL | Y | N | N | N |
| ARRAY_DOUBLE | Y | N | N | N |
| ARRAY_VARCHAR | Y | N | N | N |

Rationale for exclusions:
- **Sort + BOOL**: Boolean has only 2 values, sort is meaningless.
- **Bitmap + DOUBLE**: Floating point has high cardinality, bitmap degrades.

#### Why ARRAY_* cast types are excluded from Sort/Bitmap/Hybrid

Regular Array columns currently do not support Sort or Bitmap indexes either (`STLSORTChecker` and `BITMAPChecker` reject top-level Array types). JSON Path ARRAY_* cast types follow the same constraint — no reason to support more index types for JSON arrays than for native arrays. HYBRID is also excluded to keep the upgrade path clean: when Array support is added for Sort/Bitmap later, HYBRID behavior goes from "not supported" → "Bitmap/Sort selection" without a breaking change.

### AUTOINDEX Routing

Update `scalarAutoIndex.params.build` default config to route JSON AUTOINDEX to **HYBRID** (was `INVERTED`):

```json
{"json": "HYBRID", ...}
```

### Single Index Per Path

Milvus does not support multiple indexes on the same column, and for JSON fields does not support multiple indexes on the same path:

- One index type per `(field_id, json_path)` pair.
- Creating a second index on the same path with a different cast type or index type is rejected with an error ("at most one distinct index is allowed per field").
- No query-time index selection logic needed.

## Design Details

### 1. Build-Time: JSON Data Extraction via Pre-processing

The core approach is **pre-processing JSON field data into typed FieldData**, then feeding it to existing unmodified `ScalarIndexSort<T>::BuildWithFieldData()`, `BitmapIndex<T>::BuildWithFieldData()`, or `HybridScalarIndex<T>::BuildWithFieldData()`.

`ProcessJsonFieldData<T>()` (`internal/core/src/index/JsonIndexBuilder.cpp`) already implements the complete JSON path extraction logic (path lookup, type casting, null/non-exist handling). A new function `ConvertJsonToTypedFieldData<T>()` reuses it with callbacks that populate a typed `FieldData<T>`:

```cpp
template <typename T>
std::vector<FieldDataPtr> ConvertJsonToTypedFieldData(
    const std::vector<FieldDataPtr>& json_field_datas,
    const proto::schema::FieldSchema& schema,
    const std::string& nested_path,
    const JsonCastType& cast_type,
    JsonCastFunction cast_function);
```

`ConvertJsonToTypedFieldData` returns a `JsonToTypedResult` containing:
- **`field_datas`**: Typed FieldData with nullable semantics. Rows are marked invalid (`valid = false`) when the path is missing, null, OR the value fails to cast.
- **`non_exist_offsets`**: Offsets of rows where the JSON path truly does not exist (row null, path missing, or path value null). This is a **subset** of invalid rows — rows where the path exists but the cast fails are NOT included.

This distinction is critical for EXISTS semantics:
- `valid_bitset_[i] = false` means row `i` cannot be indexed (path missing OR cast fail) → controls Range/In/NotIn filtering
- `non_exist_offsets_` contains only rows where the path is absent → controls EXISTS queries

### 2. EXISTS Query Support via non_exist_offsets

`valid_bitset_` alone is insufficient for EXISTS. A row like `{'a': 'hello'}` with a DOUBLE index has `valid = false` (cast fails), but `EXISTS json_field['a']` must return true because the path exists.

Each JSON wrapper (Sort, Bitmap, Hybrid) maintains its own `non_exist_offsets_` and overrides `Exists()` to compute the bitmap from it — the same approach used by `JsonInvertedIndex`. The offsets are serialized via `WriteEntries` and restored via `LoadEntries` to survive the Build → Upload → Load lifecycle across IndexNode and QueryNode.

`IndexBase` provides a virtual `Exists()` method. `ExistsExpr` calls it uniformly:

```cpp
cached_index_chunk_res_ = std::make_shared<TargetBitmap>(index->Exists());
```

No cast_type switch or dynamic_cast to concrete index types needed.

### 3. IndexFactory Routing (C++)

`IndexFactory::CreateJsonIndex()` currently only accepts INVERTED and NGRAM. Extend to accept STL_SORT, BITMAP, and HYBRID.

#### 3.1 Schema context and dual file-manager pattern

Base indexes like `BitmapIndex` and `HybridScalarIndex` derive their internal type dispatching from the `FileManagerContext`'s `field_schema.data_type()`. If this is `JSON`, their switch statements hit the `default:` error branch. Additionally, `Build(const Config&)` calls `CacheRawDataAndFillMissing()` to read field data from storage — if the schema is modified to the cast type, this would produce typed FieldData instead of JSON FieldData, which would break `ConvertJsonToTypedFieldData`.

Solution: each JSON wrapper maintains **two file managers**:
- **Original-schema file manager** (`json_file_manager_`): for reading raw JSON binlog data
- **Cast-schema file manager** (inherited from base via `MakeJsonCastContext`): for schema-dependent dispatching in the base index

`MakeJsonCastContext()` creates a copy of the `FileManagerContext` with `field_schema.data_type()` set to the cast type's Milvus type (BOOL/DOUBLE/VARCHAR).

#### 3.2 Wrapper for Sort/Bitmap

`JsonScalarIndexWrapper<T, BaseIndex>` overrides `Build(const Config&)` — not `BuildWithFieldData` — to read raw JSON data via `json_file_manager_`, convert to typed FieldData, then call `BaseIndex::BuildWithFieldData` with the typed data:

```cpp
template <typename T, typename BaseIndex>
class JsonScalarIndexWrapper : public BaseIndex {
public:
    // Base constructed with MakeJsonCastContext (cast-type schema)
    // json_file_manager_ constructed with original JSON schema

    void Build(const Config& config) override {
        auto json_field_datas =
            storage::CacheRawDataAndFillMissing(json_file_manager_, config);
        auto typed_datas = ConvertJsonToTypedFieldData<T>(
            json_field_datas, json_schema_, nested_path_, cast_type_, cast_function_);
        BaseIndex::BuildWithFieldData(typed_datas);
    }
};
```

#### 3.3 Hybrid Index for JSON Path

`JsonHybridScalarIndex<T>` extends `HybridScalarIndex<T>` and overrides `Build(const Config&)`. It cannot delegate to the base `Build` because `HybridScalarIndex::Build` reads data, does cardinality selection, and builds internally — all in one method. Instead, it reimplements the build flow using protected methods `SelectIndexBuildType` and `BuildInternal`:

```cpp
template <typename T>
class JsonHybridScalarIndex : public HybridScalarIndex<T> {
public:
    void Build(const Config& config) override {
        // Read config (cardinality limit, index type preferences)
        // Read raw JSON data via json_file_manager_
        // Convert to typed data
        // SelectIndexBuildType(typed_datas)  -- cardinality selection
        // BuildInternal(typed_datas)         -- delegates to internal Bitmap or Sort
    }
};
```

`SelectIndexBuildType` and `BuildInternal` are promoted from private to protected in `HybridScalarIndex` to enable this. The internal indexes created by `GetInternalIndex()` (e.g., `BitmapIndex`) use the cast-type `file_manager_context_` inherited from the base, so they see the correct data type in their schema.

### 4. Segment Loading (C++)

`ChunkedSegmentSealedImpl::LoadIndex()` stores JSON indexes into `json_indices`. The `JsonIndex` struct already has the necessary fields (`nested_path`, `field_id`, `cast_type`, `index`). Sort, Bitmap, and Hybrid JSON path indexes are stored in the same `json_indices` vector — no structural changes needed.

### 5. PinJsonIndex Query Matching (C++)

`PinJsonIndex()` matches query paths to indexed paths via exact path match + `IsDataTypeSupported` check. Since there is only one index per path and the wrapper exposes `GetCastType()`, the existing matching logic works unchanged.

### 6. Query Execution Path (C++)

For range/comparison expressions, the current dispatch dynamic_casts to `ScalarIndex<T>*` if the index is not a `JsonFlatIndex`. This already works for Sort, Bitmap, and Hybrid because they are all `ScalarIndex<T>` subclasses with `Range()`, `In()`, `NotIn()` implementations. **No changes needed.**

### 7. Go-Side Parameter Validation

#### STL_SORT Checker (`stl_sort_checker.go`)

- `CheckValidDataType`: Add `JSON` to valid data types.
- `CheckTrain`: When `dataType == JSON`, validate `json_cast_type` in `{"DOUBLE", "VARCHAR"}` and `json_path` present.

#### Bitmap Checker (`bitmap_index_checker.go`)

- `CheckValidDataType`: Add `JSON` to valid data types.
- `CheckTrain`: When `dataType == JSON`, validate `json_cast_type` in `{"BOOL", "VARCHAR"}` and `json_path` present.

#### HYBRID Checker (`hybrid_index_checker.go`)

- `CheckValidDataType`: Add `JSON` to valid data types.
- `CheckTrain`: When `dataType == JSON`, validate `json_cast_type` in `{"BOOL", "DOUBLE", "VARCHAR"}` and `json_path` present.

HYBRID remains blocked for direct user specification (`task_index.go:289` rejects `IsHYBRIDChecker`). It is only reachable via AUTOINDEX routing, same as regular scalar columns.

#### AUTOINDEX Config (`autoindex_param.go`)

```go
// Change default:
// "json": "INVERTED" → "json": "HYBRID"
DefaultValue: `{"int": "HYBRID","varchar": "HYBRID","bool": "BITMAP", "float": "HYBRID", "json": "HYBRID", "geometry": "RTREE", "timestamptz": "STL_SORT"}`
```

### 8. Rolling Upgrade Compatibility

In a mixed-version cluster, some QueryNodes may not support JSON path Sort/Bitmap/Hybrid indexes.

Each QN registers `ScalarIndexEngineVersion` in its session. DataCoord's `ResolveScalarIndexVersion()` returns MIN(all QNs' current version) — the highest version ALL QNs support.

**Version gate at CreateIndex (collection level)**:

Bump `CurrentScalarIndexEngineVersion` to 3 for QNs that support JSON path Sort/Bitmap/Hybrid. In `index_service.go`'s `CreateIndex`, when the request is for a JSON field with `index_type` in {`STL_SORT`, `BITMAP`, `HYBRID`}:

```go
if isJsonField && isNewJsonIndexType(indexType) {
    clusterVersion := s.indexEngineVersionManager.ResolveScalarIndexVersion()
    if clusterVersion < 3 {
        return merr.Status(merr.WrapErrParameterInvalidMsg(
            "index type %s on JSON path requires all QueryNodes to support scalar index version >= 3, "+
            "but cluster resolved version is %d; please complete the rolling upgrade first",
            indexType, clusterVersion)), nil
    }
}
```

This is a hard reject at index metadata creation time. During rolling upgrade, AUTOINDEX → HYBRID → rejected. Users needing a JSON Path Index during upgrade can explicitly specify `INVERTED`.

## Architecture Summary

```
        CreateIndex(index_type=AUTOINDEX, json_path="/price", json_cast_type=DOUBLE)
                                                |
                                                v
        ┌─────────────── Go Proxy ──────────────────────────────────────────┐
        │  1. AUTOINDEX → HYBRID (via scalarAutoIndex config)              │
        │  2. HYBRIDChecker.CheckTrain(): validate cast_type + path        │
        └──────────────────────────────────────────────────────────────────┘
                                                |
                                                v
        ┌─────────────── Go DataCoord ──────────────────────────────────────┐
        │  1. Version check: ResolveScalarIndexVersion() >= 3              │
        │  2. ParseAndVerifyNestedPath: json['price'] → /price             │
        └──────────────────────────────────────────────────────────────────┘
                                                |
                                                v
        ┌─────────────── C++ IndexFactory::CreateJsonIndex() ──────────────┐
        │                                                                   │
        │  INVERTED  → JsonInvertedIndex<T>                    (existing)  │
        │  NGRAM     → NgramInvertedIndex                      (existing)  │
        │  STL_SORT  → JsonScalarIndexWrapper<T, ScalarIndexSort<T>>       │
        │  BITMAP    → JsonScalarIndexWrapper<T, BitmapIndex<T>>           │
        │  HYBRID    → JsonHybridScalarIndex<T>                            │
        └──────────────────────────────────────────────────────────────────┘
                                                |
                                                v
        ┌─────────────── Build Phase ──────────────────────────────────────┐
        │                                                                   │
        │  1. ConvertJsonToTypedFieldData<T>()                             │
        │     - path exists + cast OK → typed value, valid=true            │
        │     - path missing / null / cast fail → valid=false              │
        │  2. Delegate to base index's BuildWithFieldData()                │
        │     - Sort: sorted array + binary search                         │
        │     - Bitmap: per-value roaring bitmaps                          │
        │     - Hybrid: count distinct → pick Bitmap or STL_SORT → build   │
        │     - valid_bitset_ naturally tracks which rows have values       │
        └──────────────────────────────────────────────────────────────────┘
                                                |
                                                v
        ┌─────────────── Query Phase (mostly unchanged) ───────────────────┐
        │                                                                   │
        │  PinJsonIndex → exact path match → returns ScalarIndex<T>*       │
        │  Range/In/NotIn → ScalarIndex<T> interface   (unchanged)         │
        │  EXISTS → IndexBase::Exists() → non_exist_offsets_               │
        └──────────────────────────────────────────────────────────────────┘
```

## File Change Summary

| File | Change |
|---|---|
| `internal/core/src/index/IndexFactory.cpp` | `CreateJsonIndex()`: accept STL_SORT/BITMAP/HYBRID |
| `internal/core/src/index/JsonScalarIndexWrapper.h` | **New**: template wrapper for Sort/Bitmap |
| `internal/core/src/index/JsonHybridScalarIndex.h` | **New**: hybrid wrapper with cardinality-based selection |
| `internal/core/src/index/JsonIndexBuilder.h/.cpp` | Add `ConvertJsonToTypedFieldData<T>()` |
| `internal/core/src/index/Index.h` | Add virtual `Exists()` method returning `const TargetBitmap&` |
| `internal/core/src/exec/expression/ExistsExpr.cpp` | Simplify to `index->Exists()` |
| `internal/util/indexparamcheck/stl_sort_checker.go` | Accept JSON, validate json_cast_type/json_path |
| `internal/util/indexparamcheck/bitmap_index_checker.go` | Accept JSON, validate json_cast_type/json_path |
| `internal/util/indexparamcheck/hybrid_index_checker.go` | Accept JSON, validate json_cast_type/json_path |
| `pkg/util/paramtable/autoindex_param.go` | Default `json` from `INVERTED` to `HYBRID` |
| `pkg/common/common.go` | Bump `Current/MaximumScalarIndexEngineVersion` to 3 |
| `internal/datacoord/index_service.go` | Version check in `CreateIndex` for JSON path Sort/Bitmap/Hybrid |

## Test Plan

### Unit Tests (C++)

1. **ConvertJsonToTypedFieldData**: Normal values, missing paths, null values, cast failures, mixed rows.
2. **Sort on JSON**: Build from JSON data, verify `Range()`, `In()`, `NotIn()`, `Exists()`.
3. **Bitmap on JSON**: Build from JSON data, verify `In()`, `Range()` on strings, `Exists()`.
4. **Hybrid on JSON**: Low cardinality → Bitmap selected; high cardinality → STL_SORT selected; query results correct in both cases.
5. **Serialize/Deserialize roundtrip**: Build → Serialize → Load → query, verify results match.

### Integration Tests (Go)

1. Create collection with JSON field, insert mixed data (some rows have target key, some don't).
2. Create Sort/Bitmap index on JSON paths, verify filter queries: range, equality, IN, EXISTS.
3. Verify AUTOINDEX on JSON path creates HYBRID and queries work.
4. Verify creating a second index on the same path with a different cast type is rejected.

### Rolling Upgrade Tests

1. Mixed cluster (some QN v2, some v3): CreateIndex with JSON path STL_SORT/BITMAP/HYBRID → expect clear error.
2. Mixed cluster: CreateIndex with JSON path INVERTED → expect success.
3. Full upgrade to v3: CreateIndex with JSON path HYBRID → success.
4. Full upgrade: AUTOINDEX on JSON path → HYBRID → success.

### Edge Cases

- All rows missing the target path → index is all-null, EXISTS returns all-false.
- Empty collection → index builds with 0 rows.
- `json_cast_function` (STRING_TO_DOUBLE) with Sort/Bitmap/Hybrid indexes.

## Rejected Alternatives

### A. Subclassing Sort/Bitmap for JSON

Creating dedicated `JsonSortIndex<T>` / `JsonBitmapIndex<T>` subclasses. Rejected: duplicates JSON extraction logic across classes and requires parallel class hierarchies. The wrapper approach achieves the same result with less code.

### B. Using valid_bitset_ alone for EXISTS

Using `valid_bitset_` from the base index (Sort/Bitmap) for EXISTS queries without maintaining separate `non_exist_offsets_`. Rejected: `valid_bitset_[i] = false` conflates two distinct cases — "path doesn't exist" and "path exists but cast fails". EXISTS must return true for the latter case (e.g., `{'a': 'hello'}` with a DOUBLE index). Separate `non_exist_offsets_` is required, matching the approach used by `JsonInvertedIndex`.

### C. Silently degrade Hybrid + ARRAY_* to INVERTED

When HYBRID encounters an ARRAY_* cast type, silently fall back to INVERTED. Rejected: when Bitmap/Sort ARRAY_* support is added later, HYBRID behavior would change from "INVERTED" to "Bitmap/Sort" — a breaking change. Explicit rejection keeps the upgrade path clean.

## References

- JSON Path Index: `internal/core/src/index/JsonInvertedIndex.h/.cpp`
- JSON data extraction: `internal/core/src/index/JsonIndexBuilder.h/.cpp`
- Sort index: `internal/core/src/index/ScalarIndexSort.h/.cpp`
- Bitmap index: `internal/core/src/index/BitmapIndex.h/.cpp`
- Hybrid index: `internal/core/src/index/HybridScalarIndex.h/.cpp`
- Index factory: `internal/core/src/index/IndexFactory.cpp`
- Go-side validation: `internal/util/indexparamcheck/`
- AUTOINDEX config: `pkg/util/paramtable/autoindex_param.go`
- Proxy AUTOINDEX routing: `internal/proxy/task_index.go:301-337`
- Scalar index version: `pkg/common/common.go`, `internal/datacoord/index_engine_version_manager.go`
