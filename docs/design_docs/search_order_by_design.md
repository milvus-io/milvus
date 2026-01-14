# Search Order By - Initial Design Interface

This document is a starting point for user-facing design of `order_by` support after vector search.

## Goal
Provide a basic calling interface that lets users sort vector search results by one or more scalar fields, and optionally combine with `group_by`.

## Basic API

### Python (PyMilvus-style)

```python
res = milvus_client.search(
    collection_name="example",
    data=vectors,
    limit=10,
    anns_field="embeddings",
    output_fields=["id", "price", "rating", "category"],
    order_by=[
        {"field": "price", "order": "asc"},
        {"field": "rating", "order": "desc"}
    ]
)
```

### Group By + Order By

```python
res = milvus_client.search(
    collection_name="example",
    data=vectors,
    limit=10,
    anns_field="embeddings",
    output_fields=["id", "price", "rating", "category"],
    group_by_field="category",
    group_size=3,
    strict_group_size=True,
    order_by=[
        {"field": "price", "order": "asc"}
    ]
)
```

## Notes
- `order_by` applies after vector search (post-TopK) unless specified otherwise.
- Multiple fields are applied in order as lexicographic sort keys.
- `group_by` + `order_by`: group first, then order groups by the first item's order_by field value in each group. The order within each group is preserved (determined by SearchGroupByNode).

## Pipeline Sketch (Search)
Relative placement of `OrderByNode` in the segcore search pipeline.

### Order By Only (无过滤条件)
```
MvccNode
  -> VectorSearchNode
    -> [OrderByNode]
```

### Order By Only (有过滤条件)
```
FilterBitsNode
  -> MvccNode
    -> VectorSearchNode
      -> [OrderByNode]
```

### Group By + Order By (无过滤条件)
```
MvccNode
  -> VectorSearchNode
    -> [SearchGroupByNode]
      -> [OrderByNode]
```

### Group By + Order By (有过滤条件)
```
FilterBitsNode
  -> MvccNode
    -> VectorSearchNode
      -> [SearchGroupByNode]
        -> [OrderByNode]
```

**Node Execution Order**:
- `FilterBitsNode` (if predicates exist) -> `MvccNode` -> `VectorSearchNode` -> `[SearchGroupByNode]` -> `[OrderByNode]`
- `FilterBitsNode` filters data based on predicates (business logic filtering)
- `MvccNode` filters deleted and expired data based on query timestamp (MVCC filtering)
- `OrderByNode` runs after vector search; if present, it runs after Group By

## SearchOrderByOperator 设计

### 核心概念

`SearchOrderByOperator` 负责对向量搜索结果按标量字段排序。它有两种输入形式，取决于 Pipeline 中是否存在 `SearchGroupByNode`。

### 输入形式

#### 形式 1：无 Group By（从 VectorSearchNode 接收）

**输入来源**：`VectorSearchNode` 通过 `QueryContext` 写入的 `SearchResult`

**输入数据**：
- `SearchResult.distances_`：相似度距离
- `SearchResult.seg_offsets_`：Segment 内的 offset 数组
- `SearchResult.primary_keys_`：主键数组（可选）
- `SearchResult.topk_per_nq_prefix_sum_`：每个 query 的结果数量前缀和

**处理逻辑**：
1. 从 `QueryContext` 读取 `SearchResult`
2. 根据 `seg_offsets_` 读取 `order_by` 字段的值
3. 按 `order_by` 字段对结果排序（支持多字段字典序）
4. 更新 `SearchResult` 中的 `seg_offsets_` 和 `distances_`（保持一致性）

#### 形式 2：有 Group By（从 SearchGroupByNode 接收）

**输入来源**：`SearchGroupByNode` 通过 `QueryContext` 更新的 `SearchResult`

**输入数据**：
- `SearchResult.distances_`：相似度距离
- `SearchResult.seg_offsets_`：Segment 内的 offset 数组（已分组）
- `SearchResult.group_by_values_`：Group By 字段值数组（已存在）
- `SearchResult.topk_per_nq_prefix_sum_`：每个 query 的结果数量前缀和

**处理逻辑**：
1. 从 `QueryContext` 读取 `SearchResult`（包含 `group_by_values_`）
2. 识别每个 Group（通过 `group_by_values_`）
3. **对 Group 进行排序**：使用每个 Group 的第一条数据的 `order_by` 字段值来对 Group 进行排序
4. 保持每个 Group 内的结果顺序不变（由 `SearchGroupByNode` 决定）
5. 更新 `SearchResult` 中的 `seg_offsets_`、`distances_` 和 `group_by_values_`（保持一致性）

### 数据访问模式

**读取 order_by 字段值**：
- 通过 `seg_offsets_` 访问 Segment 数据
- 使用 `DataGetter` 模式（类似 `SearchGroupByOperator`）
- 支持多种数据类型（INT8, INT16, INT32, INT64, FLOAT, DOUBLE, VARCHAR, JSON 等）

**排序策略**：
- 多字段排序：按字段顺序进行字典序排序
- 升序/降序：每个字段可独立指定排序方向
- 稳定性：保持相同排序键的相对顺序

### 与现有组件的交互

**与 VectorSearchNode**：
- `VectorSearchNode` 将搜索结果写入 `QueryContext::search_result_`
- `SearchOrderByOperator` 从 `QueryContext` 读取并更新 `SearchResult`

**与 SearchGroupByNode**：
- `SearchGroupByNode` 更新 `SearchResult`，添加 `group_by_values_`
- `SearchOrderByOperator` 在分组结果基础上进行排序
- 需要保持 `seg_offsets_`、`distances_` 和 `group_by_values_` 的一致性

## Implementation Notes

### Key Design Decisions

1. **数据传递方式**：通过 `QueryContext::search_result_` 传递，而非 Operator 的 `input_`/`output_`
   - 与 `VectorSearchNode` 和 `SearchGroupByNode` 保持一致
   - `SearchResult` 包含大量数据，避免多次拷贝

2. **输入形式统一**：两种输入形式都从 `QueryContext` 读取 `SearchResult`
   - 形式 1：`SearchResult` 由 `VectorSearchNode` 写入
   - 形式 2：`SearchResult` 由 `SearchGroupByNode` 更新（添加 `group_by_values_`）

3. **排序范围**：
   - 无 Group By：对所有结果排序（按 query 分组）
   - 有 Group By：对 Group 进行排序（使用每个 Group 的第一条数据的 order_by 字段值），Group 内的结果顺序保持不变

4. **字段访问**：使用 `DataGetter` 模式（参考 `SearchGroupByOperator`）
   - 支持多种数据类型
   - 支持 JSON 字段路径访问
   - 高效的数据读取

## Reduce 阶段设计

### 核心概念

Reduce 阶段负责合并多个 Segment 的搜索结果。当存在 `order_by` 时，需要按 `order_by` 字段值合并，而不是按距离合并。

### Reduce 执行流程

```
Pipeline (单个 Segment)
  -> SearchOrderByOperator (如果有 order_by，单个 Segment 内排序)
    -> 返回 SearchResult (已按 order_by 排序)
      -> Reduce (跨 Segment 合并)
        -> 按 order_by 字段值合并多个 Segment 的结果
          -> 返回最终的 SearchResultData
```

### Order By 对 Reduce 的影响

#### 无 Order By（原有逻辑）
- 按**距离**合并多个 Segment 的结果
- 使用优先队列（heap），距离小的优先
- 对于相同距离，按 Primary Key 排序

#### 有 Order By（新逻辑）
- 按**order_by 字段值**合并多个 Segment 的结果
- 使用优先队列（heap），按 order_by 字段值排序
- 如果 order_by 值相同，按距离作为 tie-breaker（metric-aware）
- 如果距离也相同，按**相对顺序**（原始索引）作为最终 tie-breaker，保持稳定排序
- **注意**：PK uniqueness 由 reduce 阶段的 `pk_set` 机制保证，不需要在排序时使用 PK 作为 tie-breaker

#### Group By + Order By
- 按 **Group 的第一条数据的 order_by 字段值**来合并 Group
- 而不是按 Group By 值本身合并
- 每个 Group 内的结果顺序保持不变

### 实现方案

#### 方案：创建 OrderByReduceHelper

**类继承关系**：
```
ReduceHelper (基础类)
  ├── GroupReduceHelper (Group By 专用)
  └── OrderByReduceHelper (Order By 专用，新增)
      └── GroupOrderByReduceHelper (Group By + Order By，新增)
```

**关键修改点**：

1. **SearchResultPair 结构体**（`ReduceStructure.h`）：
   - 添加 `order_by_values_` 字段，存储当前结果的 order_by 字段值
   - 修改 `advance()` 方法，更新 `order_by_values_`

2. **SearchResultPairComparator**（`ReduceStructure.h`）：
   - 添加 `order_by_fields_` 字段
   - 修改比较逻辑：如果有 order_by，按 order_by 值比较；否则按距离比较

3. **OrderByReduceHelper**（新建）：
   - 继承自 `ReduceHelper`
   - 重写 `ReduceSearchResultForOneNQ()`：
     - 读取 order_by 字段值
     - 使用 order_by 值进行比较和合并

4. **GroupOrderByReduceHelper**（新建）：
   - 继承自 `GroupReduceHelper` 或 `OrderByReduceHelper`
   - 处理 Group By + Order By 的组合场景

5. **reduce_c.cpp**：
   - 修改 `ReduceSearchResultsAndFillData()`：
     ```cpp
     if (has_group_by && has_order_by) {
         reduce_helper = std::make_shared<GroupOrderByReduceHelper>(...);
     } else if (has_order_by) {
         reduce_helper = std::make_shared<OrderByReduceHelper>(...);
     } else if (has_group_by) {
         reduce_helper = std::make_shared<GroupReduceHelper>(...);
     } else {
         reduce_helper = std::make_shared<ReduceHelper>(...);
     }
     ```

### 数据访问

**读取 order_by 字段值**：
- 在 `ReduceSearchResultForOneNQ()` 中，为每个 order_by 字段创建 `DataGetter`
- 通过 `SearchResult->segment_` 访问 Segment
- 使用 `seg_offsets_` 读取字段值
- 支持多种数据类型和 JSON 字段路径访问

**性能优化**：
- 考虑缓存 order_by 字段值，避免重复读取
- 对于多字段排序，可以批量读取

### 与 Pipeline 的关系

- **Pipeline 阶段**：在单个 Segment 内部按 order_by 排序
- **Reduce 阶段**：在多个 Segment 之间按 order_by 合并

**为什么需要两个阶段**：
- Pipeline 阶段：每个 Segment 独立执行，结果已按 order_by 排序
- Reduce 阶段：需要将多个 Segment 的已排序结果合并成全局排序结果

