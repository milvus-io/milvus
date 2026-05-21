# Milvus Search Order By Feature Design Document

## Overview

The Order By feature enables users to sort vector search results by scalar fields instead of (or in addition to) the default distance/similarity score ordering. This is useful for scenarios where users want to prioritize results based on business-relevant attributes like price, timestamp, rating, etc.

## Motivation

Vector search results are typically sorted by similarity score (distance). However, users often need to:
1. Sort results by a business-relevant field (e.g., "sort by price ascending")
2. Apply secondary sorting criteria after the primary vector similarity ranking
3. Re-order grouped results (from group_by searches) based on scalar attributes

## Feature Scope

### Supported Operations
- **Common Search**: Single vector search with `order_by_fields`
- **Hybrid Search**: Multi-vector search with `order_by_fields` in main search params
- **Search with Group By**: Order by applies to groups (sorts groups by first row's value)

**Cross-link**: Query ORDER BY is documented separately in:
- `design_docs/20260203-query-orderby.md`

### Unsupported Combinations
- **Search Iterator**: `order_by` is not supported when using search iterators
- **Function Score + Order By**: Cannot use both simultaneously (conflicting sort criteria)

## API Design

### User Interface

Users specify order by fields via the `order_by_fields` search parameter:

```python
# PyMilvus Example
collection.search(
    data=[[0.1, 0.2, 0.3, ...]],
    anns_field="embedding",
    param={"metric_type": "L2", "params": {"nprobe": 10}},
    limit=10,
    order_by_fields=[
        {"field": "price", "order": "asc"}
    ]
)
```

### Syntax

```python
order_by_fields = [
    {
        "field": "field_name",           # Required: field name or JSON path
        "order": "asc" | "desc"          # Optional: default is "asc"
    },
    # ... additional fields for multi-field sorting
]
```

**Field specification formats:**
- Simple field: `{"field": "price", "order": "asc"}`
- Dynamic field: `{"field": "age", "order": "desc"}` (automatically resolved from dynamic field storage)
- Dynamic field with path: `{"field": "metadata[\"price\"]", "order": "asc"}` (nested access in dynamic field)
- Nested dynamic field path: `{"field": "user[\"profile\"][\"score\"]", "order": "desc"}`

**Note**: JSON path is only supported for dynamic fields. Regular JSON fields (defined in schema) cannot be used for ordering.

**Order options:**
- `"asc"` or `"ascending"`: Sort in ascending order (default)
- `"desc"` or `"descending"`: Sort in descending order

### Supported Field Types

| Field Type | Supported | Notes |
|------------|-----------|-------|
| Bool | Yes | false < true |
| Int8/Int16/Int32/Int64 | Yes | Numeric comparison |
| Float/Double | Yes | Numeric comparison (NaN rejected at insert) |
| String/VarChar | Yes | Lexicographic comparison |
| JSON | No | Comparing JSON values on bytes is meaningless |
| Dynamic field subpath | Yes | Access via JSON path (e.g., `age` or `metadata["price"]`) |
| Array | No | Not sortable |
| Vector types | No | Not sortable |

### Dynamic Field Support

Dynamic fields are fully supported. Access them directly by field name:

```python
# Access dynamic field directly
order_by_fields=[
    {"field": "age", "order": "desc"}  # age is a dynamic field
]

# Multiple dynamic fields work the same as scalar fields
order_by_fields=[
    {"field": "rating", "order": "desc"},   # dynamic field
    {"field": "timestamp", "order": "asc"}  # mixing with schema-defined scalar fields
]
```

**Note**: Dynamic fields are automatically resolved. If a field name is not found in the schema, Milvus will look for it in the dynamic field storage.

### JSON Path Support (Dynamic Fields Only)

JSON path is only supported for dynamic fields, not for regular JSON fields. This is because Milvus currently only supports JSON path expressions for dynamic field storage. Ordering by a regular JSON field is not supported as comparing raw JSON bytes is meaningless.

Dynamic field subpaths can be accessed using bracket notation:

```python
# Access dynamic field subpath
order_by_fields=[
    {"field": "metadata[\"price\"]", "order": "asc"}  # metadata is a dynamic field key
]

# Nested path in dynamic field
order_by_fields=[
    {"field": "user[\"profile\"][\"score\"]", "order": "desc"}  # user is a dynamic field key
]

# Multiple order by fields (mixing simple and nested dynamic fields)
order_by_fields=[
    {"field": "price", "order": "asc"},              # simple dynamic field
    {"field": "timestamp", "order": "desc"}          # another dynamic field
]
```

**Note**: If you have a regular JSON field (defined in schema with JSON type), you cannot use it for ordering. Only dynamic fields support JSON path access for ordering.

## Architecture

### Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Proxy (Search Task)                             │
├─────────────────────────────────────────────────────────────────────────────┤
│  1. Parse order_by_fields from search params                                 │
│  2. Validate field names against schema                                      │
│  3. Add order_by fields to output_fields for requery                        │
│  4. Execute search on QueryNodes                                            │
│  5. Run search pipeline with order_by operator                              │
└─────────────────────────────────────────────────────────────────────────────┘
                                      │
                                      ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Search Pipeline                                    │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  ┌──────────┐   ┌─────────┐   ┌───────────┐   ┌──────────┐   ┌──────────┐  │
│  │  Reduce  │ → │ Requery │ → │ Organize  │ → │ Order By │ → │   End    │  │
│  └──────────┘   └─────────┘   └───────────┘   └──────────┘   └──────────┘  │
│                                                                              │
│  For Hybrid Search:                                                          │
│  ┌──────────┐   ┌────────┐   ┌─────────┐   ┌───────────┐   ┌──────────┐    │
│  │  Reduce  │ → │ Rerank │ → │ Requery │ → │ Order By  │ → │   End    │    │
│  └──────────┘   └────────┘   └─────────┘   └───────────┘   └──────────┘    │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Key Components

#### 1. OrderByField Structure

```go
// internal/proxy/search_util.go
type OrderByField struct {
    FieldName       string // Top-level field name for result lookup
    FieldID         int64  // Field ID for validation
    JSONPath        string // JSON Pointer format: "/price" or "/user/age"
    Ascending       bool   // true for ASC, false for DESC
    OutputFieldName string // Field name to request in requery
    IsDynamicField  bool   // true if field is resolved from dynamic field storage
}
```

#### 2. Search Info Extension

```go
// internal/proxy/search_util.go
type SearchInfo struct {
    planInfo      *planpb.QueryInfo
    offset        int64
    isIterator    bool
    collectionID  int64
    orderByFields []OrderByField  // NEW: Order by specifications
}
```

#### 3. Order By Operator

The `orderByOperator` is a pipeline operator that sorts search results:

```go
// internal/proxy/search_pipeline.go
type orderByOperator struct {
    orderByFields  []OrderByField
    groupByFieldId int64
    groupSize      int64
}
```

## Implementation Details

### Parsing Order By Fields

Location: `internal/proxy/search_util.go:parseOrderByFields()`

1. Extract `order_by_fields` parameter from search params (list of dictionaries)
2. For each dictionary in the list:
   - Extract `"field"` key as field name (required)
   - Extract `"order"` key as direction (optional, default: "asc")
   - Handle JSON bracket notation in field name (e.g., `metadata["price"]`)
   - Resolve dynamic field references (fields not in schema are automatically looked up in dynamic field storage)
   - Validate field against schema
   - Build `OrderByField` struct with parsed values

### Pipeline Integration

Location: `internal/proxy/search_pipeline.go`

Two new pipeline definitions are added:

**searchWithOrderByPipe** (for common search):
```
reduce → merge_ids → requery → gen_ids → organize → pick → order_by
```

**hybridSearchWithOrderByPipe** (for hybrid search):
```
reduce → rerank → pick_ids → requery → organize → result → order_by
```

### Order By Operator Execution

Location: `internal/proxy/search_pipeline.go:orderByOperator.run()`

1. **Validation**: Verify all order_by fields exist in result
2. **Per-Query Sorting**: For nq > 1, sort each query's results independently
3. **Group-By Handling**: If group_by is active, sort groups by first row's value
4. **JSON Value Caching**: Pre-extract JSON values to avoid repeated parsing
5. **Stable Sort**: Use `sort.SliceStable` to maintain original order for equal values
6. **Result Reordering**: Reorder all result arrays (IDs, scores, fields) based on sorted indices

### Comparison Logic

#### Regular Fields
- Numeric types: Standard numeric comparison
- String types: Lexicographic comparison
- Bool: false < true

#### Dynamic Field Subpaths
- Extract value using JSON path from dynamic field storage
- Compare based on JSON value type (Number, String, Bool)
- Mixed types: Fallback to raw JSON string comparison
- NULLS FIRST: Missing or null values sort before non-null

**Note**: Regular JSON fields (defined in schema) are not supported for ordering because comparing JSON values on raw bytes is meaningless.

### Null Handling

The implementation uses NULLS FIRST semantics:
- Null values (from nullable fields) sort before non-null values
- Non-existent JSON paths are treated as null
- Explicit JSON null values are treated as null

## Error Handling

| Error Condition | Error Message |
|-----------------|---------------|
| Missing "field" key | "missing 'field' key in order_by_fields entry" |
| Empty field name | "empty field name in order_by_fields" |
| Invalid direction | "invalid order direction 'X' for field 'Y' (must be 'asc', 'desc', 'ascending', or 'descending')" |
| Field not found | "order_by field 'X' does not exist in collection schema" |
| Unsortable type | "order_by field 'X' has unsortable type Y" |
| Regular JSON field | "order_by is not supported for JSON field 'X', use dynamic field with JSON path instead" |
| Iterator conflict | "order_by is not supported when using search iterator" |
| Function score conflict | "order_by and function_score cannot be used together" |
| Invalid JSON path | "invalid JSON path in order_by field 'X'" |

## Performance Considerations

### Optimizations Implemented

1. **JSON Value Caching**: Pre-extract JSON values before sorting to avoid O(n log n) extractions
2. **Sparse Cache for nq > 1**: Only cache values for indices being sorted
3. **Field Data Map**: Build O(1) lookup map for field data
4. **Stable Sort**: Maintain original order stability without additional comparison overhead

### Complexity Analysis

| Operation | Time Complexity | Space Complexity |
|-----------|-----------------|------------------|
| Parsing | O(f * k) | O(f) |
| JSON Extraction | O(n * f) | O(n * f) |
| Sorting | O(n log n * f) | O(n) |
| Reordering | O(n * d) | O(n * d) |

Where:
- n = number of results
- f = number of order_by fields
- k = average JSON path depth
- d = number of output fields

### Limitations

1. **Requery Required**: Order by always triggers a requery to fetch field values
2. **Memory Overhead**: All order_by field data must be loaded into memory
3. **Single Proxy Sorting**: Sorting happens entirely at the Proxy level
4. **No Regular JSON Field Support**: Regular JSON fields (defined in schema) cannot be used for ordering because Milvus only supports JSON path for dynamic fields, and comparing raw JSON bytes is meaningless

## Testing

### Unit Tests

Location: `internal/proxy/search_pipeline_test.go`

Coverage includes:
- Basic field ordering (ascending/descending)
- Multiple order_by fields
- Dynamic field subpath extraction
- Dynamic field support
- Group-by integration
- Null value handling
- nq > 1 scenarios
- Edge cases (empty results, single result)
- Regular JSON field rejection

### Test Categories

1. **Parsing Tests**: Validate order_by_fields parameter parsing
2. **Comparison Tests**: Verify comparison logic for all data types
3. **Pipeline Tests**: End-to-end pipeline execution with order_by
4. **Error Tests**: Verify error conditions are properly handled

## Files Modified

| File | Changes |
|------|---------|
| `internal/proxy/search_util.go` | OrderByField struct, parseOrderByFields() for parsing dictionary entries |
| `internal/proxy/search_pipeline.go` | orderByOperator, pipeline definitions, comparison functions |
| `internal/proxy/task_search.go` | orderByFields field, pipeline integration |
| `internal/proxy/task.go` | OrderByFieldsKey constant |
| `internal/proxy/search_pipeline_test.go` | Unit tests for dictionary-based order_by_fields |

## Query ORDER BY (Moved)

The Query ORDER BY design has been separated into:
- `design_docs/20260203-query-orderby.md`

## Future Improvements

1. **Push-down Optimization**: Push order_by to QueryNode for early sorting
2. **Index-based Sorting**: Leverage scalar indexes for efficient sorting
3. **Pagination Support**: Combine with offset/limit for efficient pagination
4. **Expression-based Ordering**: Support computed expressions (e.g., `price * quantity`)

## Appendix

### Dynamic Field JSON Path Conversion

For dynamic fields, JSON Pointer (RFC 6901) is converted to gjson path format:

JSON Pointer uses `/` as separator:
- `/user/name` → `user.name`
- `~0` → `~` (escaped tilde)
- `~1` → `/` (escaped slash)

gjson uses `.` as separator:
- Dots in keys escaped with `\.`

Example: `/key~1with~1slash` → `key\/with\/slash` (single key with slashes)

**Note**: This conversion only applies to dynamic fields. Regular JSON fields (defined in schema) are not supported for ordering.

### Requery Field Selection

The `requeryOperator` unions order_by fields with output fields:
```go
for _, orderByField := range t.orderByFields {
    outputFieldNames.Insert(orderByField.OutputFieldName)
}
```

This ensures order_by field data is fetched even if not explicitly requested.
