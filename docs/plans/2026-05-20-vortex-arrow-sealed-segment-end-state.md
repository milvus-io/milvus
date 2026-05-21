# Vortex Local Format and Arrow In-Memory Sealed Segment End State

## Context

This document describes the target sealed-segment serving architecture after the
current Arrow POC evolves beyond the legacy `ChunkedSegment` model.

The desired split is:

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

The design is intentionally internal to QueryNode and SegCore. Arrow and Vortex
are not exposed to Milvus clients.

## Core Direction

The end state should not expose Milvus chunks, spans, or chunk ids to expression
execution. A sealed segment should expose a small data-access contract:

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

## Semantics

### CandidateSelection

`CandidateSelection` represents the rows that should still be considered by
query execution.

It is a logical concept. Possible physical representations include:

- all rows in a segment
- row ranges
- row id list
- bitmap
- private physical stripe/page candidates

Expression code should treat it as an opaque row candidate set and pass it back
to segment data-access methods.

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

For nullable fields, Vortex-side null counts or validity metadata may be used
only for coarse pruning. Exact null semantics remain in Arrow expression
evaluation after materialization.

`Prune` must not own full Milvus expression semantics. It should operate on a
narrow `PrunePredicate` extracted from the expression tree:

```text
field op scalar
field between scalar and scalar
field in scalar set
```

Complex Milvus semantics such as function calls, JSON logic, array operators,
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

The iterator returns pinned batches:

```cpp
struct PinnedArrowBatch {
    int64_t row_base;
    int64_t row_count;
    std::shared_ptr<arrow::RecordBatch> batch;
    PinLifetime pins;
};
```

`PinLifetime` is a Milvus-side guard. It may hold one or more `CellAccessor`
objects or other RAII state. The invariant is:

```text
no Arrow buffer view may outlive the pins that protect its physical storage
```

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

## Column Group Model

A sealed segment is a set of column groups:

```text
Segment
  -> ColumnGroupStore(field set A)
  -> ColumnGroupStore(field set B)
  -> ColumnGroupStore(field set C)
```

Column groups are physical layout units. They decide:

- local file format
- row stripe size
- cache cell boundaries
- pruning metadata
- Arrow materialization strategy

Fields inside the same column group must share physical row stripe boundaries.
Different column groups may use different stripe sizes.

Example:

```text
small scalar group:
  large row stripes

dense vector group:
  smaller row stripes

JSON or variable-length group:
  stripe size chosen by byte budget
```

The segment layer aligns output batches across column groups. If requested
fields come from groups with different boundaries, `Iterate` may split output at
the union of physical boundaries, but expression execution still receives only
Arrow batches.

## Vortex and Arrow Roles

Vortex should be treated as the local physical storage format:

- stores QueryNode-local scalar column groups
- provides zone maps and physical metadata for `Prune`
- may provide encoded pages, mmap buffers, or readers used by Arrow
  materialization

Arrow should be treated as the in-memory execution ABI:

- expression operators consume `arrow::RecordBatch`
- nullability uses Arrow validity bitmaps
- typed scalar access uses Arrow arrays and buffers
- dense vector raw data may use Arrow layouts that Knowhere can view directly
  where practical

Vortex is not the general expression engine. Arrow is not the cache manager.

### Nullability Contract

The execution ABI uses Arrow's built-in field null representation:

- no separate `RecordBatch`-level row-null bitmap
- each materialized Arrow array carries its own validity bitmap when the field
  is nullable and the batch has nulls
- expression evaluation combines Arrow field validity with delete visibility,
  `CandidateSelection`, and operator results
- retrieve/take materialization preserves field validity in returned Arrow
  arrays before converting to Milvus `DataArray` output

For the memory-only path, `ArrowRecordBatchCell` stores Arrow arrays with their
validity buffers intact.

For the Vortex file-backed path, Vortex may store validity in its native
encoding and expose null counts for `Prune`, but `Iterate` and `Take` must
materialize those bits as Arrow array validity bitmaps. Vortex null metadata
must not become a second expression-facing null model.

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

## Cache Cell Shapes

There are two useful physical modes.

### Memory-Only Loading

When QueryNode loads data directly into memory:

```text
remote data
  -> ArrowRecordBatchCell
  -> CacheSlot<ArrowRecordBatchCell>
  -> Iterate
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

## Why Not Mutable Cell Upgrade

Avoid this shape unless `cachinglayer` gains explicit delta accounting:

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

## Relation to PR 49908

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

In that model, PR 49908's exact scan paths become physical pruning and
materialization helpers behind `ColumnGroupStore`, not the long-term expression
framework contract.

## Migration From Current Arrow POC

The current Arrow POC validates the first expression-facing batch path:

```text
expression operator
  -> Prune(...)
  -> Iterate(...)
  -> ArrowBatchIterator
  -> arrow::RecordBatch
  -> Arrow evaluator reads array values and validity
  -> typed expression result
```

The next architectural steps are:

1. Expand `CandidateSelection` beyond contiguous row ranges while keeping it
   opaque to expression operators.
2. Add `PrunePredicate` extraction for simple scalar constraints.
3. Expand expression operators that consume Arrow batches for exact evaluation.
4. Move chunk/span access behind a legacy `ColumnGroupStore` implementation.
5. Add a Vortex-backed `ColumnGroupStore` that supports `Prune` and Arrow
   materialization.
6. Remove direct chunk/span dependencies from expression execution.

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
