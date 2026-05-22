# Segment Raw Data Access with Prune, Iterate, and Take

## Context

This note answers a design question:

```text
For Milvus QueryNode / SegCore raw-data serving, can a segment expose
Prune / Iterate / Take and still support the current feature set that depends
on ChunkedSegment raw data?
```

The scope is raw data needed by query execution inside one QueryNode. Vector,
scalar, JSON, array, system-field, and retrieve materialization paths are in
scope. Persistent object-store compatibility, client APIs, Datanode flush
format, and index internals are out of scope. Vector indexes are out of scope,
but raw vector brute-force search is in scope because it consumes raw segment
data.

## Summary

`Prune`, `Iterate`, and `Take` are sufficient as the central raw-data access
model if they are defined as semantic primitives over logical row and element
selections. They are not sufficient if interpreted as only three narrow methods
over contiguous row ranges.

The design should be:

```cpp
Selection Prune(PrunePredicate predicate, Selection input);

PinnedBatchIterator Iterate(Projection fields,
                            Selection input,
                            IterateOptions options);

PinnedBatchIterator Take(Projection fields,
                         OrderedRowIds row_ids,
                         TakeOptions options);
```

And it needs companion contracts for:

- selection representation
- visibility filtering
- element-level row/element mapping
- vector raw-data adapters
- output materialization
- PK lookup and ordered first-N selection
- cache pin/load/evict lifetime

With those contracts, Milvus can remove chunk/span/chunk-id concepts from the
expression-facing sealed-segment model while preserving current features.

## Fundamental Model

The segment exposes logical data access. Physical storage can still use row
stripes, column groups, pages, Vortex arrays, Arrow record batches, mmap files,
or cache cells. Those units are private to the segment implementation.

Expression execution should see:

```text
logical row ids
logical element ids, when evaluating element-level predicates
projected Arrow batches
validity bitmaps
stable row identity for result offsets
```

Expression execution should not see:

```text
Milvus chunks
chunk ids
Span<T>
physical cache cell ids
page ids
mmap block layout
```

## Selection

`Selection` is the key concept. If it is too weak, `Prune / Iterate / Take`
cannot cover current Milvus behavior.

Required selection forms:

- `AllRows(row_count)`
- one row range
- multiple row ranges
- row bitmap
- ordered row ids
- ordered row ids with duplicates, for take/retrieve semantics
- element ids or element ranges for array/vector-array element-level execution
- optional cursor and limit state for iterative retrieval/search

`Selection` is logical. A segment may internally translate it into stripe ids,
page ids, cache cell ids, or physical row ranges, but that translation must not
escape to expression code.

## Prune

`Prune` performs conservative storage-level pruning:

```cpp
Selection Prune(PrunePredicate predicate, Selection input);
```

`Prune` may use:

- min/max zone maps
- null counts
- row-count metadata
- page/stripe statistics
- Vortex metadata
- Arrow or Vortex physical layout metadata

`Prune` must not own full Milvus expression semantics. It may over-include rows.
It must not drop rows that can match.

Example flow:

```cpp
Selection candidates = Selection::AllRows(segment.RowCount());

for (auto predicate : ExtractStoragePrunablePredicates(expr)) {
    candidates = segment.Prune(predicate, candidates);
}

for (auto batch : segment.Iterate(expr.ReferencedFields(), candidates, opts)) {
    auto exact = expr.Eval(batch);
    output.Append(exact);
}
```

This keeps exact correctness in the expression framework and lets storage only
reduce IO and CPU.

## Iterate

`Iterate` is the sequential scan primitive:

```cpp
PinnedBatchIterator Iterate(Projection fields,
                            Selection input,
                            IterateOptions options);
```

Required semantics:

- Return projected Arrow batches for the requested logical rows.
- Batches must be row-aligned across all requested fields.
- The iterator may split output by private physical boundaries, but callers
  must not depend on those boundaries.
- Each batch must carry enough row identity to map output back to segment
  offsets.
- Each batch must keep physical cache cells pinned for the lifetime of all Arrow
  buffers referenced by that batch.
- Nulls are represented through Arrow validity bitmaps.
- Missing fields, default fields, and virtual/system fields must have defined
  behavior, either here or in materialization.

A batch view should conceptually look like:

```cpp
struct PinnedBatchView {
    std::shared_ptr<arrow::RecordBatch> batch;

    // Fast path for contiguous logical rows.
    std::optional<int64_t> row_begin;

    // General path for non-contiguous or reordered selections.
    std::shared_ptr<arrow::Array> row_ids;

    // Optional local candidate mask if the physical batch contains rows outside
    // the logical selection.
    std::shared_ptr<arrow::Buffer> selection_bitmap;

    // Owns cache pins, mmap pins, file handles, or decoded buffers.
    PinLifetime pins;

    // Present for element-level iteration.
    std::shared_ptr<const ElementMapping> element_mapping;
};
```

For normal scalar filters, expressions consume Arrow arrays directly from
`batch`. For offset-input paths, the framework can either use `Take` or use
`Iterate` over an ordered row-id selection.

## Take

`Take` is the random-access primitive:

```cpp
PinnedBatchIterator Take(Projection fields,
                         OrderedRowIds row_ids,
                         TakeOptions options);
```

Required semantics:

- Preserve input row-id order.
- Preserve duplicates.
- Return null/default/missing-field behavior consistently with `Iterate`.
- Support wide projections for retrieve/search output materialization.
- Support small random batches and large take batches.
- Be cancellable, because remote/local cache loads can be expensive.

`Take` can return one batch or an iterator. Returning an iterator is safer for
large result sets and avoids forcing all projected rows into one batch.

`Take` should be the basis for:

- retrieve by segment offsets
- search result target field fill
- offset-input expression evaluation
- late materialization after order-by or vector search
- external/local file take paths

## Visibility

Timestamp and delete filtering are not raw user-field access, but they are
required for all correct query results. The design should model visibility as a
selection-producing layer:

```cpp
Selection ApplyVisibility(Selection input,
                          Timestamp query_ts,
                          Timestamp collection_ttl);
```

This can remain a separate segment API, but it should produce or update the same
`Selection` type consumed by `Prune`, `Iterate`, and `Take`.

Recommended high-level query flow:

```cpp
Selection candidates = Selection::AllRows(segment.RowCount());
candidates = segment.ApplyVisibility(candidates, query_ts, ttl);
candidates = segment.Prune(prunable_predicates, candidates);

for (auto batch : segment.Iterate(fields, candidates, opts)) {
    exact_filter.Eval(batch);
}
```

## PK Lookup and Ordered First-N

Milvus currently needs PK-based operations beyond raw column scanning:

- `search_ids`
- `pk_range`
- `pk_binary_range`
- `find_first_n`
- element-level `find_first_n`
- cursor continuation for iterative query paths

These do not have to be part of `Iterate`, but they must produce or consume the
same `Selection` abstraction.

Recommended design:

```cpp
Selection SelectByPrimaryKeys(IdArray ids);
Selection SelectByPrimaryKeyRange(PKRange range, Selection input);

OrderedRowIds FirstN(Selection input,
                     int64_t limit,
                     RowOrdering ordering,
                     std::optional<QueryCursor> cursor);
```

For unsorted segments, `RowOrdering` can mean PK ordering through PK metadata.
For sorted-by-PK segments, it can be logical row order. The expression framework
does not need to know which physical strategy is used.

## Element-Level Access

Array and vector-array features require logical element ids in addition to row
ids.

The design needs an `ElementMapping` contract:

```cpp
struct ElementMapping {
    int64_t RowCount() const;
    int64_t ElementCount() const;

    RowId ElementToRow(ElementId element_id) const;
    ElementRange RowToElements(RowId row_id) const;

    ElementSelection RowSelectionToElementSelection(Selection rows) const;
    Selection ElementSelectionToRowSelection(ElementSelection elements) const;
};
```

Element-level expression execution can then use:

```cpp
ElementSelection element_candidates =
    mapping.RowSelectionToElementSelection(row_candidates);

for (auto batch : segment.IterateElement(field, element_candidates, opts)) {
    expr.EvalElementBatch(batch);
}
```

This can be represented as a specialized `Iterate` mode or as a companion
`IterateElement` API. The important point is that row-level `Selection` alone is
not enough.

## Vector Raw-Data Search

Raw vector brute-force search is a raw-data consumer even when vector indexes
are excluded.

Dense vector, sparse vector, and vector-array search need:

- pinned raw vector buffers
- vector count per physical batch
- logical-row to physical-vector offset mapping for nullable vector fields
- vector-array row-to-element offsets
- metric/index info needed by Knowhere brute-force code
- result offset remapping back to logical segment row ids

There are two viable designs.

### Option A: Specialized Vector Iterator

```cpp
VectorBatchIterator IterateVector(FieldId vector_field,
                                  Selection input,
                                  VectorIterateOptions options);
```

`VectorBatchView` exposes Arrow-owned buffers plus Knowhere-ready views:

```cpp
struct VectorBatchView {
    RawDatasetView knowhere_dataset;
    LogicalOffsetMapping offset_mapping;
    std::shared_ptr<const ElementMapping> element_mapping;
    PinLifetime pins;
};
```

This keeps vector-specific concerns out of scalar `RecordBatch` evaluation.

### Option B: Arrow Batch Plus Adapter

`Iterate` returns Arrow vector arrays, and a `KnowhereArrowAdapter` converts
them into `RawDatasetView`.

This keeps the public segment API smaller, but the adapter still needs the same
metadata: physical vector count, null offset mapping, vector-array offsets, and
pins.

Either option is compatible with the `Prune / Iterate / Take` design. The design
must not assume that scalar Arrow `RecordBatch` iteration alone is enough for
raw vector search.

## Output Materialization

Query and retrieve results still need Milvus output formats. The segment access
model should separate data access from output encoding:

```cpp
MaterializedColumns Materialize(Projection fields,
                                OrderedRowIds row_ids,
                                OutputFormat format,
                                MaterializeOptions options);
```

or:

```cpp
auto batches = segment.Take(fields, row_ids, opts);
auto output = Materializer::ToDataArray(schema, batches, options);
```

The materializer must cover:

- scalar fields
- string/varchar
- JSON
- dynamic JSON subfields
- geometry
- arrays
- dense vectors
- sparse vectors
- vector arrays
- nullable vectors
- TEXT / LOB values
- system fields such as timestamp and segment offset
- missing fields and default values

This should not require exposing chunks. It only requires stable row ids,
projected values, validity, and field metadata.

## Cache and Pinning

Cache behavior is internal, but the API must preserve lifetime correctness.

Required invariant:

```text
No Arrow buffer, string view, vector buffer, or sparse row view may outlive the
pins that protect its physical storage.
```

`PinnedBatchView`, `VectorBatchView`, and materialization iterators must own the
necessary pins. Expression execution should not manually pin cache cells.

Cache cells can be:

- Arrow record batches
- Vortex stripes decoded to Arrow
- column-group row stripes
- vector raw-data blocks
- LOB reference blocks

The public contract should only expose resource-aware pinned views. The cache
layer can account memory and disk at the physical cell level.

## Feature Mapping

| Current raw-data need | Design coverage |
| --- | --- |
| Scalar predicate scan | `Prune` + `Iterate` |
| Offset-input scalar expression | `Take` or ordered-row-id `Iterate` |
| Column-to-column comparison | multi-field row-aligned `Iterate` |
| Null, exists, is-null | Arrow validity bitmaps |
| JSON predicates | Arrow JSON representation + expression-side exact eval |
| Dynamic JSON subfield output | materializer or virtual projected field |
| Array row-level predicates | Arrow array columns through `Iterate` |
| Array element-level predicates | `ElementMapping` + element selection |
| Vector-array element search/filter | vector adapter + `ElementMapping` |
| Dense vector raw brute-force search | `IterateVector` or Arrow-to-Knowhere adapter |
| Sparse vector raw brute-force search | sparse vector adapter |
| Nullable vector fields | logical-to-physical offset mapping |
| Search target field fill | `Take` + materializer |
| Retrieve by offsets | `Take` + materializer |
| Order by / aggregation | `Iterate` into expression/operator column vectors or Arrow-native operators |
| Timestamp/delete visibility | `ApplyVisibility` producing `Selection` |
| PK term/range filtering | PK selection APIs producing `Selection` |
| First-N result offsets | `FirstN(Selection, ordering, cursor)` |
| Local cache load/eviction | hidden behind pinned batch/vector views |

## Design Verdict

`Prune / Iterate / Take` can replace the current expression-facing
`ChunkedSegment` raw-data model, but only as the core of a broader logical data
access contract.

The sufficient end-state contract is:

```text
Selection algebra
  all rows, ranges, bitmaps, ordered ids, element ids, cursor/limit

Prune
  conservative storage-level pruning

Iterate
  projected, row-aligned, pinned Arrow batches over a selection

Take
  ordered, duplicate-preserving random access over row ids

Visibility
  timestamp/delete filtering as selection transformation

Element mapping
  row/element conversion for array and vector-array features

Vector raw-data adapter
  Knowhere-ready views over Arrow-owned or cache-owned buffers

Materialization
  Arrow/pinned batches to Milvus DataArray/search/retrieve outputs
```

With these pieces, chunks remain an internal physical implementation detail.
Without these pieces, the three names alone are too small to cover Milvus'
current raw-data feature set.
