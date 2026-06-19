# Milvus Embedded Group By - Design Document

**Version**: 1.0
**Date**: 2026-01-30
**Author**: Milvus Development Team
**Status**: Draft

---

## 1. Overview

### 1.1 Background

Current Milvus Search Group By only supports single-field grouping. This document describes the design for **Embedded Group By**, which introduces:

1. **Nested Grouping**: Multi-level hierarchical grouping (category → brand → ...)
2. **Per-Group Metrics**: Aggregate statistics (count/max/min/avg/sum) at each group level
3. **Structured Results**: Tree-shaped JSON response avoiding data duplication

### 1.2 Design Principles

1. **Segcore**: Only extend for **multi-field flat group by** (no nesting, no metrics awareness)
2. **Reduce**: Reuse existing reducers (flat composite key merge)
3. **EmbeddedGroupOperator**: New proxy-side operator handles nesting and metrics

---

## 2. Architecture

### 2.1 Data Flow

```
┌──────────────────────────────────────────────────────────────────────────┐
│                         Proxy (Pre-Processing)                            │
│  1. Parse embedded_group_by                                               │
│  2. Flatten to multi-field group_by_field_ids                            │
│  3. Ensure metric fields in output_fields                                 │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│                              QueryNode                                    │
│  Segcore: PhySearchGroupByNode (extended for multi-field)                │
│    - Group by [category, brand] as flat composite key                    │
│  Reduce: Existing flat reduce by composite key                           │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│                         Proxy (Reduce)                                    │
│  MilvusAggReducer: Merge results from shards by composite key (flat)     │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
┌──────────────────────────────────────────────────────────────────────────┐
│                    Proxy (EmbeddedGroupOperator)                          │
│  1. Build nested tree from flat composite keys                           │
│  2. Compute metrics at each level (bottom-up)                            │
│  3. Apply size limits (prune tree)                                       │
│  4. Format nested JSON response                                          │
└──────────────────────────────────────────────────────────────────────────┘
                                    ↓
                                 Client
```

### 2.2 Layer Responsibilities

| Layer | Responsibility | Nesting Aware? | Metrics Aware? |
|-------|---------------|----------------|----------------|
| Segcore | Multi-field flat group by | No | No |
| QueryNode Reduce | Merge by composite key | No | No |
| Proxy Reduce | Merge by composite key | No | No |
| **EmbeddedGroupOperator** | Tree + metrics + pruning | **Yes** | **Yes** |

---

## 3. API Design

### 3.1 Request

```python
results = client.search(
    collection_name="products",
    data=[query_vector],
    limit=5,  # documents per leaf group
    embedded_group_by={
        "field": "category",
        "size": 10,
        "metrics": [{"type": "count"}, {"type": "avg", "field": "price"}],
        "sub_group_by": {
            "field": "brand",
            "size": 5,
            "metrics": [{"type": "count"}, {"type": "max", "field": "rating"}]
        }
    }
)
```

### 3.2 Response

```json
{
    "groups": [
        {
            "key": "electronics",
            "doc_count": 100,
            "metrics": {"count": 100, "avg_price": 549.99},
            "sub_groups": [
                {
                    "key": "Apple",
                    "doc_count": 45,
                    "metrics": {"count": 45, "max_rating": 4.9},
                    "documents": [{"id": 123, "distance": 0.15}, ...]
                }
            ]
        }
    ]
}
```

### 3.3 Constraints

- Maximum nesting depth: 3 levels
- Maximum size per level: 1000
- Supported metrics: count, sum, avg, min, max

---

## 4. Segcore: Multi-Field Group By

### 4.1 Key Change

Extend `PhySearchGroupByNode` to support grouping by multiple fields using a **CompositeGroupKey**.

### 4.2 CompositeGroupKey

| Property | Description |
|----------|-------------|
| Structure | `vector<GroupByValueType>` - one value per field |
| Hash | FNV-1a combining hash of each value |
| Equality | Element-wise comparison |

### 4.3 Grouping Logic

1. Iterate vector search results via iterator
2. For each result, read values for all group-by fields → build `CompositeGroupKey`
3. Use `unordered_map<CompositeGroupKey, entries>` for grouping
4. Each composite group keeps at most `group_size` results
5. Early termination when all groups are full

### 4.4 SearchResult Extension

Add `composite_group_by_values_` field to store one `CompositeGroupKey` per result.

---

## 5. Reduce: Composite Key Merge

### 5.1 Logic

Reduce operates on **flat composite keys** - no tree awareness.

1. Use priority queue (min-heap by distance)
2. Track count per composite key: `map<CompositeGroupKey, count>`
3. Accept result if `count[key] < group_size`
4. Merge results from multiple segments/shards

### 5.2 Reuse

- QueryNode: Extend existing `SearchGroupByReduce` for composite keys
- Proxy: Extend existing `MilvusAggReducer` for composite keys

---

## 6. EmbeddedGroupOperator

### 6.1 Purpose

Post-reduction operator that transforms flat results into nested structure with metrics.

### 6.2 Execution Steps

| Step | Input | Output |
|------|-------|--------|
| 1. Build Tree | Flat composite keys | Nested GroupNode tree |
| 2. Compute Metrics | Document field values | Metrics at each node |
| 3. Prune Tree | Size limits per level | Trimmed tree |
| 4. Format Response | GroupNode tree | Nested JSON |

### 6.3 Tree Building

Transform flat `(category, brand)` composite keys into nested structure:

```
Flat: [("electronics", "Apple"), ("electronics", "Samsung"), ("books", "Penguin")]
         ↓
Tree:
  ├─ electronics
  │    ├─ Apple → [documents]
  │    └─ Samsung → [documents]
  └─ books
       └─ Penguin → [documents]
```

### 6.4 Metrics Computation

**Strategy**: Bottom-up computation from leaf to root.

| Node Type | Computation |
|-----------|-------------|
| Leaf | Compute from documents (e.g., avg = sum of prices / count) |
| Non-leaf | Roll up from children (e.g., count = sum of child counts) |

**Roll-up rules**:

| Metric | Roll-Up |
|--------|---------|
| count | Sum of child counts |
| sum | Sum of child sums |
| avg | (Sum of child sums) / (Sum of child counts) |
| min | Min of child mins |
| max | Max of child maxes |

### 6.5 Tree Pruning

At each level, keep only top `size` groups (sorted by doc_count descending).

---

## 7. Proto Changes

### 7.1 SearchInfo (Segcore)

```protobuf
message SearchInfo {
    // Existing single-field (backward compatible)
    optional int64 group_by_field_id = 8;

    // New: multi-field flat group by
    repeated int64 group_by_field_ids = 15;
}
```

### 7.2 SearchResultData

```protobuf
message CompositeGroupByValue {
    repeated GenericValue values = 1;
}

message SearchResultData {
    // New: composite keys for multi-field group by
    repeated CompositeGroupByValue composite_group_by_values = 20;
}
```

---

## 8. Key Design Decisions

### 8.1 Why Proxy-Side Metrics?

| Option | Pros | Cons |
|--------|------|------|
| Segcore metrics | Less data transfer | More segcore complexity |
| **Proxy metrics** | Simple segcore, reuse agg infra | Transfer field values |

**Decision**: Proxy-side. Result set is limited, transfer overhead acceptable.

### 8.2 Why Flat Reduce + Post Transform?

| Option | Pros | Cons |
|--------|------|------|
| Tree reduce at each layer | Incremental | Complex, new reduce logic |
| **Flat reduce + transform** | Reuse existing reduce | Proxy does more work |

**Decision**: Flat reduce. Reuses existing infrastructure, complexity isolated in one operator.

### 8.3 Backward Compatibility

- Single-field `group_by_field` API unchanged
- `embedded_group_by` is new parameter, mutually exclusive with `group_by_field`

---

## 9. File Changes

### New Files

| File | Purpose |
|------|---------|
| `internal/proxy/embedded_group_operator.go` | Tree building, metrics, pruning |

### Modified Files

| File | Change |
|------|--------|
| `internal/core/src/common/Types.h` | Add `CompositeGroupKey` |
| `internal/core/src/common/QueryResult.h` | Add `composite_group_by_values_` |
| `internal/core/src/exec/operator/SearchGroupByNode.cpp` | Multi-field support |
| `internal/core/src/segcore/reduce/GroupReduce.cpp` | Composite key reduce |
| `internal/proxy/search_util.go` | Parse `embedded_group_by` |
| `internal/proxy/task_search.go` | Integrate EmbeddedGroupOperator |
| `pkg/proto/plan.proto` | Add `group_by_field_ids`, `CompositeGroupByValue` |

---

## 10. Implementation Phases

### Phase 1: Multi-Field Group By (Segcore)

- CompositeGroupKey type and hash
- Extend PhySearchGroupByNode
- Extend reduce for composite keys
- Proto changes

### Phase 2: EmbeddedGroupOperator (Proxy)

- Parse embedded_group_by parameter
- Tree building from flat results
- Metrics computation (bottom-up)
- Tree pruning

### Phase 3: Integration & Testing

- End-to-end integration
- PyMilvus client support
- Unit and integration tests

---

## 11. Summary

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────────────────┐
│   Segcore   │ --> │  Existing Reduce │ --> │  EmbeddedGroupOperator  │
│ Multi-field │     │  (flat composite │     │  - Build tree           │
│ flat group  │     │   key merge)     │     │  - Compute metrics      │
│ by          │     │                  │     │  - Prune & format       │
└─────────────┘     └──────────────────┘     └─────────────────────────┘
```

**Key Points**:
1. Segcore only handles flat multi-field grouping
2. Reduce reuses existing infrastructure with composite keys
3. EmbeddedGroupOperator handles all nesting and metrics logic at proxy

---

**Document End**
