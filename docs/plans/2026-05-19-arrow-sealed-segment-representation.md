# Arrow as In-Memory Representation for Sealed Segments

## Context

Milvus currently loads sealed-segment raw data into SegCore using Milvus-specific field/chunk abstractions. This design explores replacing only the QueryNode-local representation of loaded sealed-segment raw data with Apache Arrow. The scope is intentionally limited to QueryNode and SegCore serving paths on a single node.

This does not change public APIs, message formats, Datanode flush behavior, or persisted object-storage segment compatibility. QueryNode-local on-disk cache files are provisional and may change format freely because they can be rebuilt from remote segment data.

## Current POC Status

The current POC implements the first narrow slice of the design:

- Arrow-backed sealed-segment test segment: `ArrowSealedSegment`.
- Cache cell shape: one `RecordBatchCell` per column group and row stripe.
- Segment-side reader interface: `ArrowRecordBatchReader`.
- Expression integration point: `PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData`.
- Supported expression shape: simple `INT64` / `TIMESTAMPTZ` unary range filters.
- Normal query path: `query::ExecuteQueryExpr(...)` and `FilterBitsNode`.

The implemented flow is:

```text
query::ExecuteQueryExpr(...)
  -> PhyFilterBitsNode::GetOutput()
  -> ExprSet::Eval(...)
  -> PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData<T>
  -> ProcessArrowDataChunks<T>
  -> SegmentInternalInterface::CreateArrowRecordBatchReader(...)
  -> ArrowRecordBatchReader::Next()
  -> arrow::RecordBatch
  -> arrow::Int64Array
  -> arrow::Int64Array::raw_values()
  -> existing UnaryElementFunc<T> logic over T*
```

So the expression framework now owns the scalar filter semantics. Arrow supplies the physical batch. The normal path no longer uses a `FilterBitsNode -> segment executes whole expression` shortcut.

The POC still keeps `ExecuteArrowNativeExprForTest(...)` and `ExecuteArrowNativeExpr(...)` as direct helpers, but tests assert that the normal `query::ExecuteQueryExpr(...)` path creates an Arrow `RecordBatch` reader and does not call the direct segment-level executor.

Current limitations:

- Only `INT64` and `TIMESTAMPTZ` are supported.
- Only unary range filters are supported.
- No offset-input / random-access path.
- No string, JSON, array, vector, or sparse vector Arrow evaluator.
- No multi-field aligned Arrow batch reader yet.
- No production QueryNode loader or local disk cache format change yet.

## Goals

- Represent loaded sealed-segment raw data as a logical Arrow dataset.
- Remove the Milvus chunk concept from sealed-segment query and expression execution semantics.
- Let expression execution consume an iterator of Arrow batch views.
- Keep cache, mmap, loading, and eviction details hidden behind the dataset layer.
- Preserve stable row offsets for delete bitsets, retrieve, PK lookup, and query results.
- Enable zero-copy or low-copy access to dense vector raw data where practical.
- Allow independent partial load and eviction across column groups and row ranges.

## Non-Goals

- Do not expose Arrow through Milvus APIs or RPCs.
- Do not rewrite Datanode, import, compaction, or object-storage persistence as part of this change.
- Do not require Knowhere indexes to store data in Arrow format.
- Do not require all vector types, especially sparse vectors, to be zero-copy in the first phase.

## Proposed Model

A sealed segment should be modeled as one logical Arrow dataset, but not as one giant physical Arrow object:

```text
SealedSegment
  -> LogicalArrowDataset
      -> ColumnGroupArrowStore(s)
          -> CacheSlot<ArrowColumnGroupCell>
              -> cell 0
              -> cell 1
              -> ...
      -> BatchIterator(required_fields, candidate_rows)
          -> ArrowBatchView(row_base, row_count, columns, visibility, pins)
```

`LogicalArrowDataset` is a Milvus-side logical object. It is table-like, but it does not imply that the whole segment is materialized as one `arrow::Table` or one `arrow::RecordBatch`.

`ArrowBatchView` is the unit consumed by expression execution. It is a logical view over aligned row ranges across requested columns. The expression framework should not know whether the data came from one Arrow `RecordBatch`, multiple Arrow arrays, a row group, an mmap file, or a cache cell. If the view references evictable data, it must hold the necessary cache pins for its lifetime.

Internally, the Arrow dataset may still split data into cells for resource management:

```text
Remote segment data
  -> QueryNode-local Arrow cache/mmap files
  -> ColumnGroupArrowStore
  -> cachinglayer CacheSlot<ArrowColumnGroupCell>
  -> LogicalArrowDataset
  -> ArrowBatchIterator
```

The important separation is:

- Query semantics: batch iterator, no chunk concept.
- Resource management: internal cells for pinning, eviction, lazy loading, and accounting.

The current POC uses a smaller interface before introducing the full `LogicalArrowDataset` object:

```cpp
struct ArrowRecordBatchView {
    int64_t row_begin;
    int64_t row_count;
    PinWrapper<std::shared_ptr<arrow::RecordBatch>> batch;
};

class ArrowRecordBatchReader {
 public:
    virtual bool HasNext() const = 0;
    virtual ArrowRecordBatchView Next() = 0;
};

virtual bool
CanUseArrowRecordBatchReader(FieldId field_id, DataType data_type) const;

virtual std::unique_ptr<ArrowRecordBatchReader>
CreateArrowRecordBatchReader(milvus::OpContext* op_ctx,
                             std::vector<FieldId> field_ids,
                             int64_t start_row_stripe_id) const;
```

This is the minimal bridge that lets the existing expression framework consume Arrow-backed batches while preserving the broader target model.

## Object Scope

Use different scopes for different responsibilities:

```text
Segment scope:
  LogicalArrowDataset

Column-group scope:
  ColumnGroupArrowStore

Cache cell scope:
  ArrowColumnGroupCell

Execution scope:
  ArrowBatchView
```

The base physical loading unit should be a column-group cell:

```text
ArrowColumnGroupCell
  column_group_id
  row_base
  row_count
  arrow arrays / record batch for fields in the group
  optional mmap/cache-file guard
  CellByteSize()
```

Fields in the same column group must share the same cell row boundaries. Different column groups should be allowed to use different boundaries because row size can vary significantly across field types.

Example:

```text
scalar column group:
  cell 0: rows [0, 512K)
  cell 1: rows [512K, 1M)

dense vector column group:
  cell 0: rows [0, 32K)
  cell 1: rows [32K, 64K)

json column group:
  cell 0: rows [0, 64K)
  cell 1: rows [64K, 120K)
```

There should be no segment-wide canonical row stripe size.

## Arrow Concepts Used

- `arrow::Buffer`: raw contiguous memory bytes.
- `arrow::ArrayData`: physical array metadata plus buffers.
- `arrow::Array`: typed column chunk wrapper.
- `arrow::RecordBatch`: equal-length arrays forming a batch of rows.
- `arrow::ChunkedArray`: one logical column split into multiple arrays.
- `arrow::Table`: logical multi-column dataset, commonly backed by chunked arrays.

For a sealed segment, `arrow::Table` is a natural logical model. For execution, `arrow::RecordBatch`-like views are the practical iteration unit.

## Vector Interaction with Knowhere

For indexed sealed-segment search, Knowhere mostly remains independent:

```text
query vector + bitset -> Knowhere index -> offsets/distances
```

Knowhere does not need Arrow raw data for normal indexed search. Arrow raw vector storage matters for retrieve, flat/no-index search, fallback paths, and index building if QueryNode performs local builds.

Dense-vector zero-copy input to Knowhere is realistic if Arrow stores vectors in a layout Knowhere can view directly:

- `FloatVector`: `FixedSizeList<float32>` or `FixedSizeBinary`
- `BinaryVector`: `FixedSizeBinary`
- `Float16/BFloat16`: `FixedSizeBinary`
- `Int8Vector`: `FixedSizeBinary` or `FixedSizeList<int8>`

This gives zero-copy input handoff, not zero-copy index construction. Knowhere will still build its own index structures.

Sparse-vector zero-copy is harder. A standard Arrow nested layout such as `List<Struct<dim,value>>` is expressive but may still require conversion. True zero-copy sparse handoff likely needs a CSR-like Arrow buffer contract:

```text
row_offsets / indptr
indices
values
```

## QueryNode Local Cache

The local cache format can change without compatibility concerns because it is provisional QueryNode data. A candidate format is Arrow IPC or another mmap-friendly Arrow buffer layout stored under a versioned local cache namespace.

Required properties:

- cache format versioning
- segment/load-version namespacing
- field or column-group namespacing
- safe cleanup on segment release, restart, or cache mismatch
- disk quota integration
- rebuild from remote data on cache miss or corruption

## Caching Layer Evolution

Keep `cachinglayer`, but change the raw sealed-data cell payload from Milvus chunks to Arrow-backed cells.

`cachinglayer` is not a global key-based cache. The model is slot/cell based:

```text
owner object creates CacheSlot<CellT>
CacheSlot owns cells_[0..num_cells)
caller pins by uid
CacheSlot maps uid -> cid
Translator loads cid -> CellT
```

`translator->key()` identifies a slot for logging, metrics, and debugging. It should not be treated as a lookup key for global cached objects.

Current shape:

```text
CacheSlot<Chunk>
```

Target shape:

```text
CacheSlot<ArrowColumnGroupCell>
```

`ArrowColumnGroupCell` is a Milvus-side wrapper concept, not an Arrow concept. A cell may hold an Arrow `RecordBatch`, Arrow arrays, mmap guards, row range metadata, and field ids.

Each cell must expose accurate resource accounting:

- heap Arrow buffers -> `memory_bytes`
- local mmap/cache files -> `file_bytes`
- Arrow metadata/descriptors -> `memory_bytes`
- temporary decode/materialization buffers -> loading overhead

`CellByteSize()` must deduplicate shared Arrow buffers, otherwise memory usage may be overcounted.

Partial load and eviction are handled by pinning cells inside each column-group slot. Different column groups may have different loaded row ranges at the same time:

```text
scalar group loaded: rows [0, 1M)
vector group loaded: rows [64K, 128K)
json group loaded:   nothing
```

When a query needs rows `[80K, 90K)` for scalar and vector fields, `LogicalArrowDataset` resolves the overlapping cells in each relevant column group, pins them, and exposes aligned slices through `ArrowBatchView`.

## Execution Flow

### Target Flow

1. Query planning identifies required fields.
2. SegCore asks the logical Arrow dataset for a batch iterator.
3. The dataset maps requested fields to column groups.
4. The dataset maps the current logical row range to overlapping cells in each column group.
5. `cachinglayer` pins the required cells and loads them if needed.
6. The dataset builds aligned Arrow slices from pinned physical cells.
7. The dataset exposes `ArrowBatchView` objects to expression execution. The view holds pin guards for all referenced cells.
8. Expression operators evaluate vectorized batches and produce bitsets/results.
9. Pins are released when the batch view is no longer used.

The expression framework should not manually pin cells. It should only consume `ArrowBatchView`. The invariant is:

```text
no stable raw Arrow buffer access without a lifetime guard
```

### Current POC Flow

The POC flow is narrower and still type-specific:

1. `FilterBitsNode` invokes the normal `ExprSet` / `EvalCtx` path.
2. `PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData<T>` checks whether the segment can provide an Arrow reader.
3. For `T == int64_t`, no offset input, and supported field types, it creates an `ArrowRecordBatchReader`.
4. The reader pins one `RecordBatchCell` at a time and returns an `ArrowRecordBatchView`.
5. The expression operator extracts `arrow::Int64Array::raw_values()` as `const int64_t*`.
6. Arrow nulls are converted into a temporary `FixedVector<bool>` only when needed.
7. The existing Milvus unary batch evaluator runs over the `T*` data and produces the result bitmap.
8. If Arrow is unsupported for the expression, the existing Milvus chunk path remains the fallback.

This means the current POC validates the data-access direction, but it has not yet replaced all chunk-aware expression code with a general Arrow batch abstraction.

## Main Benefits

- Sealed-segment data is immutable, which matches Arrow well.
- Query execution becomes batch/iterator-oriented instead of chunk-oriented.
- Arrow buffers can reduce copies for scalar and dense vector raw data.
- QueryNode-local cache can become mmap-friendly and disposable.
- Nullability uses Arrow validity bitmaps.
- `cachinglayer` can continue managing memory, disk, warmup, and eviction.

## Main Risks

- Existing SegCore expression code is chunk-aware and needs an iterator-oriented refactor.
- Dense vector zero-copy depends on choosing a physical layout compatible with Knowhere.
- Sparse vector zero-copy needs additional design with Knowhere.
- Arrow arrays may share buffers, making resource accounting easy to get wrong.
- Local cache lifecycle must be robust against restarts, release races, and disk pressure.
- Cross-column-group execution must align fields whose physical cell boundaries differ.

## Suggested Phases

1. Keep the current `ArrowRecordBatchReader` bridge and expand scalar type support beyond `Int64Array`.
2. Add offset-input support by grouping candidate offsets by `RecordBatchCell`.
3. Introduce multi-field batch alignment for conjuncts and compare expressions across column groups with different row stripe boundaries.
4. Introduce `LogicalArrowDataset` and a more general `ArrowBatchView` / `ColumnView` abstraction so expression code does not branch directly on Milvus span vs Arrow array.
5. Introduce `ColumnGroupArrowStore` and production `ArrowColumnGroupCell` loading behind `cachinglayer`.
6. Support PK, timestamp, delete visibility, retrieve, and scalar fields in the production Arrow-backed sealed segment.
7. Add dense vector raw-data support with a zero-copy Knowhere input adapter where possible.
8. Add QueryNode-local Arrow mmap/cache materialization.
9. Evaluate sparse vector and complex types after the core path is stable.
