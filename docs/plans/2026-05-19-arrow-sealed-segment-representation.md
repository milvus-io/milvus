# Arrow In-Memory and Vortex Local Format for Sealed Segments

## Context

Milvus currently loads sealed-segment raw data into SegCore using
Milvus-specific field/chunk abstractions. The target design replaces that
QueryNode-local serving representation with:

```text
Vortex
  QueryNode-local file format and physical pruning metadata

Arrow
  in-memory batch representation consumed by expression execution

cachinglayer
  private pin/load/evict/accounting mechanism for physical cells

expression framework
  exact Milvus expression semantics over Arrow batches
```

This design is internal to QueryNode and SegCore. Arrow and Vortex are not
exposed to clients, RPCs, Datanode flush behavior, or the remote object-store
segment format. QueryNode-local cache files are provisional and can be rebuilt
from remote segment data.

## Current POC Status

The current POC implements the first narrow Arrow-in-memory slice:

- Arrow-backed sealed-segment test segment: `ArrowSealedSegment`.
- Cache cell shape: one `RecordBatchCell` per column group and row stripe.
- Segment-side access interface: `Prune(...)`, `Iterate(...)`, and `Take(...)`.
- Expression integration point: `PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData`.
- Supported expression shape: simple `INT64` / `TIMESTAMPTZ` unary range filters.
- Null handling: Arrow column validity bitmaps are the source of truth.
- Normal query path: `query::ExecuteQueryExpr(...)` and `FilterBitsNode`.

Implemented flow:

```text
query::ExecuteQueryExpr(...)
  -> PhyFilterBitsNode::GetOutput()
  -> ExprSet::Eval(...)
  -> PhyUnaryRangeFilterExpr::ExecRangeVisitorImplForData<T>
  -> ProcessArrowDataChunks<T>
  -> SegmentInternalInterface::Prune(...)
  -> SegmentInternalInterface::Iterate(...)
  -> ArrowBatchIterator::Next()
  -> arrow::RecordBatch
  -> Arrow unary evaluator reads arrow::Int64Array values and validity bitmap
  -> existing typed comparison logic
```

The expression framework owns scalar filter semantics. The segment supplies
projected Arrow batches over candidate rows. There is no segment-level Arrow
expression executor in the POC.

Current limitations:

- Only `INT64` and `TIMESTAMPTZ` are supported.
- Only unary range filters are supported.
- No offset-input / random-access expression path.
- No string, JSON, array, vector, or sparse vector Arrow evaluator.
- Multi-field projected batch iteration exists for aligned row stripes, but
  cross-column-group row-boundary alignment is still POC-level.
- No production QueryNode loader, Vortex local file, or local disk cache format
  change yet.

Tests include nullable Arrow scalar and string arrays built with Arrow builders
and `AppendNull()`. They verify that scalar filters exclude null field values,
retrieve results preserve validity, and the legacy chunk bridge only materializes
a Milvus validity vector when Arrow has nulls. The Arrow unary evaluator consumes
the packed validity bitmap directly instead of calling `IsNull(i)` for every row.

## Target Direction

The end state should not expose Milvus chunks, spans, or chunk ids to expression
execution. A sealed segment exposes a small data-access contract:

```cpp
class Segment {
 public:
    CandidateSelection Prune(PrunePredicate predicate,
                             CandidateSelection input) const;

    ArrowBatchIterator Iterate(Projection fields,
                               CandidateSelection input) const;

    ArrowBatchIterator Take(Projection fields,
                            RowIdBatch row_ids) const;
};
```

Internally, a segment may still divide data into stripes, pages, row groups, or
cache cells. Those are storage and cache implementation details, not expression
framework concepts.

The target goals are:

- Represent loaded sealed-segment raw data as a logical Arrow dataset.
- Remove the Milvus chunk concept from sealed-segment query semantics.
- Let expression execution consume an iterator of pinned Arrow batch views.
- Keep cache, mmap, loading, and eviction hidden behind the dataset layer.
- Preserve stable row offsets for delete bitsets, retrieve, PK lookup, and query
  results.
- Enable zero-copy or low-copy dense vector raw-data access where practical.
- Allow independent partial load and eviction across column groups and row ranges.

Non-goals:

- Do not expose Arrow or Vortex through Milvus APIs.
- Do not require Knowhere indexes to store data in Arrow format.
- Do not require all vector types, especially sparse vectors, to be zero-copy in
  the first phase.

## Segment Access Semantics

### CandidateSelection

`CandidateSelection` represents rows still under consideration by query
execution. It is a logical concept. Possible physical representations include:

- all rows in a segment
- row ranges
- row id lists
- bitmaps
- private physical stripe/page candidates

Expression code should treat it as opaque and pass it back to segment
data-access methods.

### Prune

`Prune` is approximate filtering using storage-level metadata such as zone maps,
min/max statistics, null counts, or page-level metadata.

Contract:

```text
input:
  simple prunable predicate
  current CandidateSelection

output:
  narrower CandidateSelection

guarantee:
  no false negatives
  false positives are allowed
```

Example:

```text
20 physical row stripes
predicate: a > 40
zone map says only 5 stripes have max(a) > 40

Prune output:
  candidates covering those 5 stripes

Expr must still evaluate a > 40 exactly over Arrow data from those candidates.
```

`Prune` must not own full Milvus expression semantics. It should operate on a
narrow `PrunePredicate` extracted from the expression tree:

```text
field op scalar
field between scalar and scalar
field in scalar set
```

Complex semantics such as function calls, JSON logic, array operators,
dynamic-field behavior, and exact nullable behavior remain in the expression
framework.

### Iterate

`Iterate` is the normal value access path.

Contract:

```text
input:
  projected fields
  CandidateSelection

output:
  stream of pinned Arrow record batches
```

Each returned batch is a row-aligned Arrow view for the requested fields. It may
be assembled from multiple physical column groups with different internal cell
boundaries. Expression execution should not know that.

### Take

`Take` is random or sparse row access.

Typical callers:

- retrieve by row ids
- top-k materialization
- scalar fields needed after vector search

Contract:

```text
input:
  projected fields
  explicit row ids

output:
  stream of pinned Arrow record batches
```

The segment implementation may group row ids by private physical cells, pin
those cells, and emit Arrow batches preserving the required logical row order.

## Logical and Physical Model

A sealed segment should be modeled as one logical Arrow dataset, but not as one
giant physical Arrow object:

```text
SealedSegment
  -> LogicalArrowDataset
      -> ColumnGroupStore(s)
          -> CacheSlot<CellT>
              -> cell 0
              -> cell 1
              -> ...
      -> BatchIterator(required_fields, candidate_rows)
          -> ArrowBatchView(row_base, row_count, columns, visibility, pins)
```

Use different scopes for different responsibilities:

```text
Segment scope:
  LogicalArrowDataset

Column-group scope:
  ColumnGroupStore

Cache cell scope:
  ArrowRecordBatchCell or VortexStripeCell

Execution scope:
  ArrowBatchView / PinnedArrowBatch
```

Column groups are physical layout units. They decide:

- local file format
- row stripe size
- cache cell boundaries
- pruning metadata
- Arrow materialization strategy

Fields inside the same column group must share physical row stripe boundaries.
Different column groups may use different boundaries because row size varies
significantly across field types.

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

There should be no segment-wide canonical row stripe size. If requested fields
come from groups with different boundaries, `Iterate` may split output at the
union of physical boundaries, but expression execution still receives only
pinned Arrow batches.

## Arrow Execution ABI

Arrow is the in-memory execution ABI:

- expression operators consume `arrow::RecordBatch`-like batch views
- nullability uses Arrow validity bitmaps
- typed scalar access uses Arrow arrays and buffers
- dense vector raw data may use Arrow layouts that Knowhere can view directly
  where practical

Relevant Arrow concepts:

- `arrow::Buffer`: raw contiguous memory bytes.
- `arrow::ArrayData`: physical array metadata plus buffers.
- `arrow::Array`: typed column chunk wrapper.
- `arrow::RecordBatch`: equal-length arrays forming a batch of rows.
- `arrow::ChunkedArray`: one logical column split into multiple arrays.
- `arrow::Table`: logical multi-column dataset, commonly backed by chunked arrays.

For a sealed segment, `arrow::Table` can describe the logical model. For
execution, `arrow::RecordBatch`-like pinned views are the practical iteration
unit.

### Nullability Contract

The execution ABI uses Arrow's field-level null representation:

- no separate `RecordBatch`-level row-null bitmap
- each materialized Arrow array carries its own validity bitmap when the field
  is nullable and the batch has nulls
- expression evaluation combines Arrow field validity with delete visibility,
  `CandidateSelection`, and operator results
- expression evaluation should consume packed Arrow validity bitmaps directly;
  it should not materialize temporary per-row null arrays for exact evaluation
- retrieve/take materialization preserves field validity in returned Arrow
  arrays before converting to Milvus `DataArray` output

For the memory-only path, `ArrowRecordBatchCell` stores Arrow arrays with their
validity buffers intact.

For the Vortex file-backed path, Vortex may store validity in its native
encoding and expose null counts for `Prune`, but `Iterate` and `Take` must
materialize those bits as Arrow array validity bitmaps. Vortex null metadata
must not become a second expression-facing null model.

## Expression Flow

The expression framework owns exact evaluation:

```cpp
CandidateSelection candidates = CandidateSelection::All(segment.RowCount());

for (const auto& predicate : ExtractPrunePredicates(expr)) {
    candidates = segment.Prune(predicate, candidates);
}

auto iter = segment.Iterate(expr.ReferencedFields(), candidates);

while (auto batch = iter.Next()) {
    auto result = ExecExprOnArrow(expr, batch->batch);
    output.Merge(result);
}
```

The segment may reduce work through `Prune`, but final correctness comes from
Arrow-based expression evaluation.

Pinned batch invariant:

```text
no Arrow buffer view may outlive the pins that protect its physical storage
```

The expression framework should not manually pin cells. It should only consume
`ArrowBatchView` / `PinnedArrowBatch`.

## Cachinglayer Integration

`cachinglayer` remains slot/cell based:

```text
owner creates CacheSlot<CellT>
CacheSlot owns cells_[0..num_cells)
Translator maps logical uid to physical cid
Translator loads cid into CellT
CellAccessor pins loaded cells
```

The new segment APIs hide those mechanics:

```text
Prune
  maps CandidateSelection to physical metadata cells
  pins metadata or Vortex cells
  reads zone maps
  returns narrower CandidateSelection

Iterate
  maps CandidateSelection to physical data cells
  pins cells
  loads on miss
  builds Arrow RecordBatch views
  returns PinnedArrowBatch objects

Take
  maps row ids to physical data cells
  pins cells
  emits Arrow batches for requested rows
```

Expression code should never call `CacheSlot`, `PinCells`, or `GetChunk`.

Each cell must expose accurate resource accounting:

- heap Arrow buffers -> `memory_bytes`
- local mmap/cache files -> `file_bytes`
- Arrow metadata/descriptors -> `memory_bytes`
- temporary decode/materialization buffers -> loading overhead

`CellByteSize()` must deduplicate shared Arrow buffers, otherwise memory usage
may be overcounted.

Partial load and eviction are handled by pinning cells inside each column-group
slot. Different column groups may have different loaded row ranges at the same
time:

```text
scalar group loaded: rows [0, 1M)
vector group loaded: rows [64K, 128K)
json group loaded:   nothing
```

When a query needs rows `[80K, 90K)` for scalar and vector fields, the logical
dataset resolves overlapping cells in each relevant column group, pins them,
and exposes aligned slices through `ArrowBatchView`.

## Cache Cell Shapes

There are two useful physical modes.

### Memory-Only Loading

When QueryNode loads data directly into memory:

```text
remote data
  -> ArrowRecordBatchCell
  -> CacheSlot<ArrowRecordBatchCell>
  -> Iterate / Take
```

The cell accounts decoded Arrow memory:

```cpp
struct ArrowRecordBatchCell {
    std::shared_ptr<arrow::RecordBatch> batch;

    ResourceUsage CellByteSize() const {
        return {decoded_arrow_bytes, 0};
    }
};
```

### File-Backed Loading

When QueryNode uses local Vortex files:

```text
local Vortex file / stripe
  -> VortexStripeCell
  -> CacheSlot<VortexStripeCell>
  -> Prune

local Vortex file / stripe
  -> ArrowRecordBatchCell
  -> CacheSlot<ArrowRecordBatchCell>
  -> Iterate / Take
```

The Vortex slot accounts file resources and small reader metadata. The Arrow
slot accounts decoded in-memory Arrow data.

If Arrow buffers are zero-copy views over mmap or Vortex-owned buffers, the
Arrow cell must hold the source pin:

```cpp
struct ArrowRecordBatchCell {
    std::shared_ptr<arrow::RecordBatch> batch;
    std::shared_ptr<CellAccessor<VortexStripeCell>> source_pin;

    ResourceUsage CellByteSize() const {
        return {arrow_metadata_or_decode_bytes, 0};
    }
};
```

This keeps the mmap/file-backed storage alive while Arrow arrays are visible to
expression execution.

Avoid mutable "upgrade a Vortex cell into an Arrow cell" unless `cachinglayer`
gets explicit delta accounting:

```cpp
struct VortexStripeCell {
    VortexStripeHandle vortex;
    std::shared_ptr<arrow::RecordBatch> decoded_arrow; // filled later
};
```

The issue is resource accounting. `CacheSlot` charges loaded resources when a
cell is loaded and `CellByteSize()` is observed. If decoded Arrow memory is
attached later, memory usage grows after the original charge. Eviction and
metrics become inaccurate unless the cell explicitly reserves, charges, and
refunds the added memory.

Reasonable choices:

- use separate Vortex and Arrow slots
- make a single immutable cell load all resources up front
- add explicit mutable-cell accounting APIs to `cachinglayer`
- decode Arrow transiently and do not cache decoded Arrow memory

The two-slot model is the cleanest fit for the current `cachinglayer` contract.

## Vortex Role

Vortex is the QueryNode-local physical storage format:

- stores local scalar column groups
- provides zone maps and physical metadata for `Prune`
- may provide encoded pages, mmap buffers, or readers used by Arrow
  materialization

Vortex is not the general expression engine. Vortex exact scan paths should
become physical pruning and materialization helpers behind `ColumnGroupStore`,
not the long-term expression framework contract.

## QueryNode Local Cache

The local cache format can change because it is provisional QueryNode data.
Candidate formats include Vortex files for scalar column groups and mmap-friendly
Arrow buffer layouts for memory-only or decoded data.

Required properties:

- cache format versioning
- segment/load-version namespacing
- field or column-group namespacing
- safe cleanup on segment release, restart, or cache mismatch
- disk quota integration
- rebuild from remote data on cache miss or corruption

## Vector Interaction with Knowhere

For indexed sealed-segment search, Knowhere mostly remains independent:

```text
query vector + bitset -> Knowhere index -> offsets/distances
```

Knowhere does not need Arrow raw data for normal indexed search. Arrow raw vector
storage matters for retrieve, flat/no-index search, fallback paths, and index
building if QueryNode performs local builds.

Dense-vector zero-copy input to Knowhere is realistic if Arrow stores vectors in
a layout Knowhere can view directly:

- `FloatVector`: `FixedSizeList<float32>` or `FixedSizeBinary`
- `BinaryVector`: `FixedSizeBinary`
- `Float16/BFloat16`: `FixedSizeBinary`
- `Int8Vector`: `FixedSizeBinary` or `FixedSizeList<int8>`

This gives zero-copy input handoff, not zero-copy index construction. Knowhere
will still build its own index structures.

Sparse-vector zero-copy is harder. A standard Arrow nested layout such as
`List<Struct<dim,value>>` is expressive but may still require conversion. True
zero-copy sparse handoff likely needs a CSR-like Arrow buffer contract:

```text
row_offsets / indptr
indices
values
```

## Relation to Draft PR 49908

Draft PR 49908 introduces Vortex as a local scalar field format and adds scan
paths through `ChunkedColumnInterface`.

The end-state design should keep the useful parts:

- Vortex local file format
- Vortex metadata and zone maps
- Vortex-backed translators and cache slots
- QueryNode-local physical pruning

But the expression-facing API should move away from `ChunkedColumnInterface`
and toward:

```text
Prune -> Iterate -> Arrow expression evaluation
```

PR 49908's exact scan paths become internal Vortex `ColumnGroupStore`
implementation details.

## Migration From Current POC

The current Arrow POC validates the first expression-facing batch path:

```text
expression operator
  -> Prune(...)
  -> Iterate(...)
  -> ArrowBatchIterator
  -> arrow::RecordBatch
  -> Arrow evaluator reads array values and validity bitmap
  -> typed expression result
```

Next architectural steps:

1. Expand `CandidateSelection` beyond contiguous row ranges while keeping it
   opaque to expression operators.
2. Add `PrunePredicate` extraction for simple scalar constraints.
3. Expand expression operators that consume Arrow batches for exact evaluation.
4. Move chunk/span access behind a legacy `ColumnGroupStore` implementation.
5. Add a Vortex-backed `ColumnGroupStore` that supports `Prune` and Arrow
   materialization.
6. Add production `ArrowRecordBatchCell` loading behind `cachinglayer`.
7. Support PK, timestamp, delete visibility, retrieve, and scalar fields in the
   production Arrow-backed sealed segment.
8. Add dense vector raw-data support with a zero-copy Knowhere input adapter
   where possible.
9. Remove direct chunk/span dependencies from expression execution.

## Benefits

- Sealed-segment data is immutable, which matches Arrow well.
- Query execution becomes batch/iterator-oriented instead of chunk-oriented.
- Vortex can provide local physical pruning without owning exact expression
  semantics.
- Arrow buffers can reduce copies for scalar and dense vector raw data.
- QueryNode-local cache can become mmap-friendly and disposable.
- Nullability uses Arrow validity bitmaps.
- `cachinglayer` can continue managing memory, disk, warmup, and eviction.

## Risks

- Existing SegCore expression code is chunk-aware and needs an iterator-oriented
  refactor.
- Dense vector zero-copy depends on choosing a physical layout compatible with
  Knowhere.
- Sparse vector zero-copy needs additional design with Knowhere.
- Arrow arrays may share buffers, making resource accounting easy to get wrong.
- Vortex decode plus Arrow materialization may create double accounting unless
  cache ownership is explicit.
- Local cache lifecycle must be robust against restarts, release races, and disk
  pressure.
- Cross-column-group execution must align fields whose physical cell boundaries
  differ.

## Invariants

- Expression execution owns exact Milvus expression semantics.
- Storage pruning must have no false negatives.
- Candidate selections are logical row candidates, not exposed chunk ids.
- Arrow batches returned to expression execution must be pinned.
- Physical cache cells are private to column groups and `cachinglayer`.
- Column groups may choose different row stripe sizes.
- Fields in one column group share physical row stripe boundaries.
- Resource accounting must include decoded Arrow memory and local file usage.
- Vortex and Arrow must not become competing segment abstractions.

## Open Questions

- What physical forms should `CandidateSelection` support first?
- Should `Prune` return only row ranges initially, or allow bitmaps from the
  beginning?
- How should delete bitsets compose with `CandidateSelection`?
- What is the first Arrow representation for variable-length strings, JSON,
  arrays, and sparse vectors?
- Should decoded Arrow memory be cached separately from Vortex file cells, or
  should `cachinglayer` grow mutable delta accounting?
- How much of PR 49908's scan API can be reused as an internal Vortex
  `ColumnGroupStore` implementation?
