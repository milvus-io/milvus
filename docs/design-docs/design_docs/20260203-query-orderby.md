# Query ORDER BY

- **Created:** 2026-02-03
- **Author(s):** @MrPresent-Han
- **Status:** Under Review
- **Component:** QueryNode / Proxy
- **Related Issues:** N/A
- **Released:** N/A

## Summary

Query ORDER BY pushes sorting into the segcore execution pipeline. Each segment filters, projects, sorts locally, and returns top-K results. The QueryNode merges results from multiple segments, and the proxy performs global deduplication, merge-sort, pagination, and field re-mapping before returning to the client.

## Motivation

Query ORDER BY is fundamentally different from Search ORDER BY and needs a dedicated design document to define its plan format, execution path, merge semantics, and edge-case behavior.

## Design Details

Detailed design is documented in Sections 2-13 below, including API/proto contract, segcore plan and pipeline, proxy/querynode merge behavior, GROUP BY interaction, edge cases, and known limitations.

## Test Plan

- Unit tests for parse/validate/remap/order operators in proxy.
- Integration tests for end-to-end ORDER BY correctness across multi-segment and multi-querynode paths.
- Edge-case tests for schema evolution, null ordering, tie-order behavior, and limit/offset boundaries.
- Stress tests for bounded TopK memory behavior on high-cardinality ORDER BY queries, including large-limit coverage for the missing segcore `max_sort_rows` guard.

## 1. Overview

Query ORDER BY pushes sorting into the segcore execution pipeline. Each segment filters, projects, sorts locally, and returns top-K results. The QueryNode merges results from multiple segments, and the proxy performs global deduplication, merge-sort, pagination, and field re-mapping before returning to the client.

This differs fundamentally from Search ORDER BY, which operates at the proxy level after vector search requery. Query ORDER BY has no vector search step — it is a pure filter-project-sort pipeline.

### End-to-End Data Flow

```
Client SDK
  │  order_by=["price:asc", "rating:desc"]
  ▼
Proxy (task_query.go)
  │  Parse order_by strings → OrderByField protos
  │  Validate field types, build QueryPlanNode
  ▼
QueryNode (segment-level)
  │  Segcore pipeline: Filter → Project → OrderBy → output
  │  FillOrderByResult: set field_id, fetch deferred fields, populate system fields
  │  MergeSegcoreRetrieveResults: merge results from multiple segments
  ▼
Proxy Pipeline (dql/query_pipeline.go)
  │  ReduceByPK → OrderOperator → SliceOperator → FieldIDRemapOperator
  │  Populate FieldName from schema, filter to user's output_fields
  ▼
Client SDK
  │  Results with correct field names and user-requested field order
```

### Supported Data Types

| Sortable | Not Sortable |
|----------|-------------|
| Bool, Int8, Int16, Int32, Int64 | FloatVector, BFloat16Vector, ... |
| Float, Double | Array, JSON (without path) |
| String, VarChar | Timestamptz (not yet supported) |

## 2. API & Proto

### Client API (PyMilvus)

Format: `"field_name:direction"` where direction is `asc` (default) or `desc`. Multiple fields specify multi-key sort priority.

### Proto Definition (plan.proto)

`OrderByField` message carries field_id, ascending (bool, default true), and nulls_first (bool, proto default false, but proxy sets it per SQL convention: ASC → false/NULLS LAST, DESC → true/NULLS FIRST). `QueryPlanNode` includes a repeated `OrderByField order_by_fields` field alongside predicates, limit, group_by, and aggregates.

## 3. Proxy: Request Parsing & Validation

**File**: `internal/proxy/dql/task_query.go`

### 3.1 Parsing order_by Strings

`translateOrderByFields` parses user-provided strings like `"price:desc"` into OrderByField protos:

- Splits on `:` or space → field name + direction
- Looks up field_id from collection schema
- Validates the field type is sortable via `isSortableFieldType`
- Default direction: ascending. Default null ordering follows SQL/PostgreSQL convention: **ASC → NULLS LAST, DESC → NULLS FIRST** (NULL is treated as larger than any non-null value). Users can override with explicit `:nulls_first` or `:nulls_last` suffix (e.g., `"price:desc:nulls_last"`).

### 3.2 Validation Rules

1. **Type check**: Only sortable types are allowed (see table above). Vectors, arrays, JSON (without path) are rejected.
2. **GROUP BY compatibility**: When GROUP BY is present, ORDER BY can only reference columns in the GROUP BY clause. Ordering by aggregate function results (for example, `sum(price)`) is not yet supported and is rejected during validation.
3. **limit required**: ORDER BY always requires an explicit limit, regardless of whether a predicate is present. The finite limit lets segcore use a bounded TopK buffer; without it, SortBuffer would have to materialize all matching rows in memory, where proxy-side `MaxOutputSize` provides no protection. Requests with ORDER BY but no limit are rejected with `"ORDER BY requires explicit limit"`.

### 3.3 Plan Assembly

The parsed OrderByField list is set on the QueryPlanNode and sent to the QueryNode via gRPC.

## 4. Segcore: Plan Creation & Pipeline

**File**: `internal/core/src/query/PlanProto.cpp`

### 4.1 Pipeline Structure

For `output_fields=["A","B","C"], order_by=["B:asc","C:desc"]`:

**Single-project mode** (all non-sort output fields are fixed-width):

```
FilterBitsNode → MvccNode → ProjectNode[pk, B, C, A, SegOffset] → OrderByNode(B, C, limit)
```

**Two-project mode** (any non-sort output field is variable-width, e.g., VARCHAR):

```
FilterBitsNode → MvccNode → ProjectNode[pk, B, C, SegOffset] → OrderByNode(B, C, limit)
                                                                       │
                                        deferred fields [A] fetched later by FillOrderByResult
```

### 4.2 Two-Project Mode: Why It Matters

VARCHAR values require heap allocation during RowContainer storage. Consider a segment with 10,000 matching rows and `limit=10`:

- **Without deferred fetch**: The bounded TopK buffer would copy the VARCHAR output for candidate rows that may later be discarded (up to 20 resident candidates with the current `2 * limit` capacity).
- **Two-project**: Only projects sort keys and fixed-width columns into the sort buffer. After TopK selects 10 winners, it bulk-fetches exactly the 10 VARCHAR output values via segment offsets.

Bounded TopK caps the **peak live candidate set** at `2 * limit`; it does not cap cumulative allocations over the entire scan. Before the first shrink, every row is copied, and an improving input stream can continue to admit rows after later shrinks. Two-project mode avoids copying deferred variable-width output fields for all of those transient candidates.

### 4.3 Mode Decision Logic

For each non-sort output column: fixed-width types (bool, int8..int64, float, double) are included in the ProjectNode; variable-width types (VARCHAR, STRING) trigger two-project mode.

Any variable-width column causes ALL non-sort output columns to be deferred. This "all or nothing" deferral simplifies the positional layout: pipeline always produces `[pk, orderby_fields, SegOffset]` in two-project mode, regardless of how many fixed-width fields exist.

### 4.4 Positional Layout Contract

The pipeline returns columns in a fixed positional order. SegmentOffsetFieldID is always appended as the last column.

```
Pipeline output: [pk, B, C, (A if single-project), SegOffset]
                  ^   ^----^   ^                      ^
                  |   |        |                      +-- segment offset for deferred fetch
                  |   |        +-- non-sort output (single-project only)
                  |   +---------- ORDER BY fields (sort key order)
                  +-------------- PK (for proxy reduce/dedup)
```

If a field appears in both ORDER BY and output_fields, it occupies only the ORDER BY position and is not repeated.

### 4.5 OrderByNode & SortBuffer

The OrderByNode wraps a SortBuffer:

- **Storage**: Projected columns for retained candidates are stored in RowContainer. Fixed-width values are inline; VARCHAR values are heap-allocated pointers.
- **Sorting**: Sorts row pointers (8 bytes each), not row data. Uses sort_keys_ to compare — non-sort columns are completely ignored during comparison.
- **Bounded TopK**: For a finite segment limit `k` (the request's `offset + limit`), the buffer retains at most `2 * k` live candidates and uses `std::nth_element` to shrink back to `k`. No rejection threshold exists while the initial buffer is filling. The first shrink establishes it, and each later shrink replaces it with the worst row among the newly selected top `k`. Rows that cannot beat the latest threshold are compared in place and not stored. Finalization performs one last shrink before sorting so discarded variable-width values are released promptly. All matching rows are considered; peak candidate memory is O(`k * projected_row_size`), while cumulative copies or allocations can still grow with the number of admitted candidates.
- **Output**: Extracts columns from sorted row pointers via copy into new ColumnVectors.

## 5. Segcore: Result Assembly (FillOrderByResult)

**File**: `internal/core/src/segcore/SegmentInterface.cpp`

After the pipeline produces sorted top-K rows, FillOrderByResult assembles the final RetrieveResults proto in five steps:

### Step 1: Move Pipeline Columns

Move all columns except the last (SegmentOffsetFieldID) into results.fields_data. Set field_id on each DataArray using pipeline_field_ids_ — this is critical because pipeline-produced DataArrays have field_id=0 by default, and the QN-side merge matches by field_id.

### Step 2: Extract Segment Offsets

The last pipeline column contains segment-level row offsets. These are stored in results.offset() for deferred field fetching (Step 3) and QN-side merge validation (MergeSegcoreRetrieveResults checks offset length > 0).

### Step 3: Bulk-Fetch Deferred Fields (Two-Project Mode)

For each deferred field, bulk_subscript is called with the segment offsets to fetch exactly the top-K rows. Special handling exists for:
- **Dynamic fields**: JSON subfield projection via target_dynamic_fields_
- **Schema evolution**: Fields absent in this segment get default values via bulk_subscript_not_exist_field. Both deferred fields (this path) and eagerly projected ORDER BY fields (ProjectNode) are covered — see Section 12.3.
- **Array type**: element_type is set on the output DataArray

### Step 4: Populate System Fields

Fetch system fields (e.g., TimestampField) via segment offsets. The timestamp field is required by QN-side pk+ts deduplication logic.

### Step 5: Set IDs from PK Column

Extract PK values from position 0 and populate results.ids (either int_id or str_id). This is required by the proxy's ReduceByPK operator.

## 6. ShouldIgnoreNonPk Bypass

**File**: `internal/core/src/segcore/plan_c.cpp`

Normal queries use a two-phase retrieval optimization: first fetch only PKs, then re-fetch full fields via offsets. This is controlled by ShouldIgnoreNonPk.

ORDER BY queries **must bypass** this optimization because:
- The pipeline returns data in a positional layout [pk, orderby, remaining]
- The proxy's Remap operator depends on this exact positional order
- Two-phase retrieval would re-fetch via FillTargetEntry in field_ids_ order, breaking the positional contract

When has_order_by_ is true, ShouldIgnoreNonPk returns false so the pipeline output is used directly.

## 7. QueryNode: Segment Merge

**File**: `internal/querynodev2/segments/result.go`

MergeSegcoreRetrieveResults merges results from multiple segments on the same QueryNode:

- Uses PrepareResultFieldData to create output containers, copying field schema from the first non-empty segment result (field_id, field_name, field_type)
- AppendFieldData matches fields by field_id (not position) using a map keyed by FieldId
- Preserves the positional layout from segcore — the merged result has the same column order as individual segment results

**Critical invariant**: Since AppendFieldData matches by field_id, FillOrderByResult must set correct field_id values on pipeline-produced DataArrays (Step 1 above). Without this, all columns would have field_id=0 and data would be corrupted during merge.

## 8. Proxy Pipeline

**File**: `internal/proxy/dql/query_pipeline.go`

The proxy-side pipeline processes the merged results from all QueryNodes:

```
input → ReduceByPK → OrderOperator → SliceOperator → FieldIDRemapOperator → output
```

### 8.1 ReduceByPK

Deduplicates rows across QueryNodes using the PK column (position 0). Rows with the same PK but older timestamps are discarded.

### 8.2 OrderOperator

**File**: `internal/util/queryutil/order_op.go`

Global merge-sort of results from all QueryNodes.

- Uses **positional comparison**: ORDER BY field i is at FieldsData[i+1] (position 0 is PK)
- When offset + limit < rowCount, uses **heap-based partial sort** O(N log K) instead of full sort O(N log N)
- Supports null handling with configurable NULLS FIRST / NULLS LAST
- **Tie-order is non-deterministic**: when all ORDER BY keys compare equal, relative row order is not guaranteed across runs.

Tie-order notes:
- The merge input order depends on segment/querynode execution and merge path, so equal-key rows may appear in different relative order.
- To get deterministic order today, users should add an explicit final key (typically primary key), for example: `ORDER BY price DESC, id ASC`.
- Future option (not implemented): append PK as an implicit final tiebreaker so internal ordering becomes equivalent to `ORDER BY <user_keys...>, pk`.

### 8.3 SliceOperator

Applies user-specified offset and limit to the sorted result. For pagination: page 2 with limit=10 uses offset=10, limit=10.

### 8.4 FieldIDRemapOperator

**File**: `internal/util/queryutil/remap_op.go`

Selects and reorders FieldsData to the user's requested output field order by matching field IDs at runtime.

For example:

```
Actual result field IDs: [pk=100, price=101, rating=102, name=103, category=104]
Desired field IDs:       [103, 104, 101]
Output fields:           [name, category, price]
```

This path does not reconstruct the C++ positional layout. Fields that are only in ORDER BY (not in the user's output_fields) and the implicit PK are stripped by this step.

## 9. Proxy: Response Assembly

**File**: `internal/proxy/dql/task_query.go`

After the pipeline completes, two final steps prepare the response:

### 9.1 FieldName Population

Segcore does not set field_name on DataArray protos (only field_id). PyMilvus uses field_data.field_name (not response.output_fields) to build result dict keys. Without this step, all fields would have field_name="".

The proxy iterates over all FieldsData entries, looks up each field_id in the collection schema, and sets FieldName, Type, and IsDynamic accordingly. This mirrors the search pipeline's endOperator behavior.

### 9.2 Output Field Filtering

Filter FieldsData to only include fields the user requested in output_fields. Removes any internal fields (PK if not requested, sort-only fields, system fields) that leaked through the pipeline.

## 10. GROUP BY + ORDER BY Interaction

### 10.1 Segcore Pipeline (C++)

When GROUP BY is present, the segcore pipeline becomes:

```
FilterBitsNode → MvccNode → ProjectNode → AggregationNode → OrderByNode
```

**AggregationNode** groups matching rows via a hash table (`GroupingSet`) and computes aggregate accumulators per group. Its output layout is a fixed schema:

```
[groupKey_0, groupKey_1, ..., aggResult_0, aggResult_1, ...]
```

The aggregation layout identifies group keys by physical `field_id` and aggregate results by their planned expression identity. The current ORDER BY contract uses only the group-key portion of that layout:

- **GROUP BY key columns**: identified by **`field_id`** (schema-stable physical ID).
- **Aggregate result columns**: present in the aggregation output, but not currently valid ORDER BY targets.

`OrderByNode` resolves supported sorting keys from their group-key field IDs.

### 10.2 ORDER BY Field Resolution

ORDER BY in GROUP BY context can only reference:
- **GROUP BY columns**: resolved to `field_id`

Resolution rules:
1. If token matches a GROUP BY column, bind to its `field_id`.
2. If the token is an aggregate expression (for example, `sum(price)` or `count(*)`), reject it as not yet supported.
3. Otherwise reject it as an invalid ORDER BY field.

This validation is enforced by `validateOrderByFieldsWithGroupBy` at the proxy level before plan creation.

### 10.3 Proxy Pipeline (Go)

The proxy pipeline for GROUP BY + ORDER BY differs significantly from plain ORDER BY:

```
input → ReduceByGroups → OrderOperator → SliceOperator → output
```

Key differences from plain ORDER BY pipeline:

| Aspect | Plain ORDER BY | GROUP BY + ORDER BY |
|--------|---------------|---------------------|
| Reduce | ReduceByPK (dedup by primary key) | ReduceByGroups (hash-merge groups, accumulate aggregates) |
| Order | Positional comparison (field at index i+1) | Positional comparison using positions derived from group-key field IDs |
| Remap | FieldIDRemapOperator selects and reorders by field ID | `newAggRemapOperator` converts the raw aggregation layout to user-visible output order |
| PK | Always at position 0 | Not projected |

**ReduceByGroups** (`GroupAggReducer`): Merges grouped results from multiple QueryNodes using a hash map keyed on grouping columns. For matching groups, aggregate accumulators are combined (e.g., sums are added, counts are added). Special case: `avg` is internally expanded to `sum + count`, merged separately, and divided at the final assembly step.

**OrderOperator** in GROUP BY context: `ComputeGroupByOrderPositions` maps the validated group-key field IDs to positions in the raw reducer layout. The operator sorts by those positions before slicing.

### 10.4 Different Remap Contracts

Plain ORDER BY uses `FieldIDRemapOperator`, which selects and reorders the actual result fields by field ID without rebuilding the segcore layout. GROUP BY + ORDER BY keeps the raw reducer layout `[group_cols..., aggregate_cols...]` through sorting and slicing, then `newAggRemapOperator` uses `AggregationFieldMap` to produce the requested output order and to compute derived `avg` values from `sum + count`.

## 11. Key Implementation Files

| Layer | File | Purpose |
|-------|------|---------|
| Proto | `pkg/proto/plan.proto` | OrderByField, QueryPlanNode definition |
| Proxy | `internal/proxy/dql/task_query.go` | Parse, validate, plan creation, FieldName population |
| Proxy | `internal/proxy/dql/query_pipeline.go` | Pipeline construction (reduce → order → slice → remap) |
| Proxy | `internal/util/queryutil/order_op.go` | OrderOperator: global merge-sort with partial sort optimization |
| Proxy | `internal/util/queryutil/remap_op.go` | FieldIDRemapOperator for plain ORDER BY; positional RemapOperator primitive used by grouped paths |
| Proxy | `internal/util/reduce/orderby/types.go` | OrderByField type definition, sortable type check |
| C++ | `internal/core/src/query/PlanProto.cpp` | BuildOrderByProjectNode, BuildOrderByNode |
| C++ | `internal/core/src/query/PlanNode.h` | RetrievePlanNode: deferred_field_ids_, pipeline_field_ids_ |
| C++ | `internal/core/src/segcore/SegmentInterface.cpp` | FillOrderByResult: deferred fetch, system fields, IDs |
| C++ | `internal/core/src/segcore/plan_c.cpp` | ShouldIgnoreNonPk bypass for ORDER BY |
| C++ | `internal/core/src/exec/SortBuffer.h` | RowContainer sort with TopK optimization |
| QN | `internal/querynodev2/segments/result.go` | MergeSegcoreRetrieveResults: field_id-based merge |

## 12. Error Handling & Edge Cases

### 12.1 limit=0 and Missing limit

| Condition | Behavior |
|-----------|----------|
| `limit=0` (explicit) | Rejected by `validateMaxQueryResultWindow`: `"limit [0] is invalid, should be greater than 0"` |
| No limit + ORDER BY (with or without predicate) | Rejected at PreExecute: `"ORDER BY requires explicit limit"`. A finite limit is required to activate bounded TopK storage; without one, SortBuffer would materialize all matching rows. `MaxOutputSize` only applies at the proxy reduce stage and cannot protect segment-level sorting. |

### 12.2 Empty Result Sets

The ORDER BY and remap operators handle empty results gracefully:

- **OrderOperator** (`order_op.go`): If result is nil, FieldsData is empty, or rowCount <= 1, the operator passes through the input unchanged — no sort logic executes.
- **FieldIDRemapOperator** (`remap_op.go`): Passes through nil or empty results and selects available fields from the runtime field-ID map.
- **Grouped aggregate remap** (`dql/query_pipeline.go`): Passes through nil or empty results; non-empty results are remapped through `AggregationFieldMap` after slicing.
- **MergeSegcoreRetrieveResults** (`result.go`): Segments with zero rows are filtered out before merge. If all segments are empty, an empty result is returned without error.

### 12.3 ORDER BY Fields Missing Due to Schema Evolution

When a collection schema evolves (e.g., a new field is added), older segments may not contain the ORDER BY field. There are two distinct code paths with **different behaviors**:

#### Deferred fields (two-project mode) — handled correctly

In `FillOrderByResult` (Step 3), deferred fields are fetched after sorting. This path checks `is_field_exist(field_id)` before calling `bulk_subscript`, and falls back to `bulk_subscript_not_exist_field` for missing fields:
- If the field has a **default value**: fills all rows with the default value, `valid_data = true`.
- If the field has **no default value**: fills all rows with placeholder data, `valid_data = false` (NULL).

#### Eagerly projected fields (ORDER BY columns in ProjectNode)

ORDER BY fields are eagerly projected in the ProjectNode (Section 4.1).

**Previous behavior (without the fix)**: `PhyProjectNode::GetOutput()` called `bulk_subscript_field_data` → `segment->bulk_subscript` without checking field existence. When an ORDER BY field was absent in an older segment, `bulk_subscript` hit `AssertInfo(column != nullptr, ...)` (`ChunkedSegmentSealedImpl.cpp:1590`). Note: `AssertInfo` in Milvus throws a C++ exception (not `abort()`/`SIGABRT`), so this would fail the individual request with an error rather than crash the process — but it is still incorrect behavior for a legitimate user query on an evolved schema.

**Current fix**: `PhyProjectNode::GetOutput()` (`ProjectNode.cpp:86`) now checks `is_field_exist(field_id)` before accessing each field. When a field is absent in an older segment (schema evolution), it creates an all-NULL ColumnVector (`valid_data` all false) instead of calling `bulk_subscript`. This ensures:
1. The SortBuffer receives valid NULL markers for the missing ORDER BY column.
2. `SortBuffer::CompareNulls` applies the configured NULLS FIRST/LAST semantics.
3. The end-to-end behavior is: **missing ORDER BY fields due to schema evolution are treated as NULL and sorted according to nulls_first setting** (ASC → NULLS LAST, DESC → NULLS FIRST).

#### Go layer consistency

No additional alignment is needed on the Go side — the C++ layer guarantees that every segment result contains the same set of fields (with NULLs/defaults for missing ones). The OrderOperator then applies standard null comparison (NULLS LAST by default) to these synthesized values.

**Consistency contract**: Both C++ (`SortBuffer::CompareNulls`) and Go (`OrderOperator.compareFieldValuesAt`) implement identical null-handling semantics — configurable per-field via `nulls_first`. The default follows SQL/PostgreSQL convention: ASC → NULLS LAST, DESC → NULLS FIRST (set by `ParseOrderByFields` in `types.go:141`). This ensures that segment-level sort order (C++) is compatible with the proxy-level global merge-sort (Go).

### 12.4 Known Limitation: Large-Limit Memory Risk

**WARNING (current behavior)**: There is currently no enforced segcore-side `max_sort_rows` guard in the ORDER BY path. With a finite segment limit `k`, SortBuffer bounds resident candidates to at most `2 * k`, but a very large requested limit can still consume substantial memory before proxy-side controls take effect.

Implications:
- All matching rows must still be compared, so broad predicates increase CPU work even though candidate storage is bounded.
- Memory scales with the segment limit (`offset + limit`) and projected row width, not the total number of matches.
- Proxy-side `MaxOutputSize` cannot protect memory consumed inside segcore `SortBuffer`; a sufficiently large limit can still cause high memory usage or OOM.

Current mitigation:
- Use conservative limits and offsets to bound candidate memory.
- Use selective predicates to reduce comparison work on high-cardinality collections.

## 13. Follow-up Items

### 13.1 Result Layout Contracts and Testing

Plain ORDER BY and GROUP BY + ORDER BY use different result-layout contracts:

- **Plain ORDER BY**: `BuildOrderByProjectNode` determines the C++ pipeline layout and `FillOrderByResult` stamps every output column with its field ID from `pipeline_field_ids_`. The proxy's `FieldIDRemapOperator` then selects and reorders fields by field ID at runtime, so user-visible remapping does not reconstruct the C++ positional layout.
- **GROUP BY + ORDER BY**: Aggregation produces the fixed layout `[group_cols..., aggregate_cols...]`. Go's `ComputeGroupByOrderPositions` maps ORDER BY fields to that layout, so this path retains a positional contract between aggregation output and the QueryNode/Proxy reducers.

**Risk**: A GROUP BY aggregation layout change that is not reflected in `ComputeGroupByOrderPositions` can make ORDER BY read the wrong column. Plain ORDER BY is protected from this class of mismatch by field-ID-based remapping.

**Mitigation**: End-to-end conformance tests should cover multiple field type combinations (fixed-width only, mixed fixed/variable-width, ORDER BY field overlapping output_fields, ORDER BY field not in output_fields) and verify final output by **value** — not just field names and types. GROUP BY + ORDER BY tests should cover each supported group key and verify that every aggregate value remains attached to the correct group after ordering, slicing, and remapping. Ordering by aggregate results is future work and must not be listed as current test coverage until the API accepts it.

### 13.2 Other Non-Blocking Follow-ups

- **Deterministic tie-order option**: Evaluate appending PK as an implicit final sort key for ORDER BY, and align behavior across segcore local sort and proxy global merge-sort.
- **GROUP BY + ORDER BY detailed design**: Full specification of aggregation pipeline interaction (Section 10 is overview only).
- **Aggregate-result ordering**: Extend validation, position resolution, and cross-layer tests before supporting aggregate expressions as ORDER BY targets.
- **Implement segcore `max_sort_rows` guard (fix for known limitation in Section 12.4)**: SortBuffer should enforce a configurable upper bound on the segment TopK limit (or estimated candidate bytes) and reject requests that exceed it, with a clear user-facing error.
- **Deferred fetch optimization**: Explore lazy evaluation for two-project mode to avoid bulk-fetching all deferred fields when only a subset is needed.
