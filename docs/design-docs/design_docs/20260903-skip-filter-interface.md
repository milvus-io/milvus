# Chunk-Level Skip Filter Interface

- **Created:** 2026-09-03
- **Author(s):** @xiaofanluan
- **Status:** Draft
- **Component:** QueryNode (segcore)
- **Related PR:** #51441

## Summary

Define the single, source-agnostic seam for chunk-level skip pruning in segcore. A
chunk's skip decision is answered from a `FieldChunkMetrics` object — a per-chunk
min/max / null-state / bloom summary — regardless of whether that summary was read from
a Parquet footer, a Vortex footer, or computed by Milvus itself over raw data. The
seam has three pieces:

1. `index::FieldChunkMetrics` — the canonical in-memory representation of one chunk's
   statistics plus the `CanSkipUnaryRange` / `CanSkipBinaryRange` / `CanSkipIn`
   predicates that decide pruning.
2. `index::SkipIndexStatsBuilder::Build(...)` — the adapter that turns a raw statistics
   source into a `FieldChunkMetrics`.
3. `milvus::FieldSkipMetricsView` + `FieldChunkMetricsProvider` — a view resolved
   directly from a field column that retains its generation and answers skip queries.

The goal of this document is to pin down that interface so future storage formats
(Storage V3 Vortex and Parquet) and Milvus's own statistics can plug in without
changing the pruning decision logic.

The current production integration is **Storage V2 Parquet footer statistics only**,
enabled by the startup-only `common.parquetStatsSkipIndex.enabled` flag (default
`true`). Manifest-based Storage V3, including Vortex and Parquet, does not yet
populate skip metrics. The Arrow-batch builder exists as a reusable capability;
automatic fallback from missing footer statistics to a raw-data scan is not wired in.

## Motivation

Chunk-level skip pruning avoids reading a chunk whose statistics prove it cannot match a
predicate. The statistics that back pruning come from different places depending on the
storage format:

- **Storage V2 (Parquet footer).** Per-row-group `parquet::Statistics` carry min/max and
  null counts. With the flag enabled (the default), every column group uses one
  row group per cache cell, including groups without usable statistics or
  skippable fields. Disabling the flag allows target-size packing and disables
  footer pruning. With 1:1 packing, the
  footer statistics align 1:1 with cells, then converts each `parquet::Statistics` into a
  `FieldChunkMetrics` via the Parquet overload of `SkipIndexStatsBuilder::Build`.
- **Milvus-native stats (builder available, producer integration deferred).** The
  Arrow-batch overload computes a summary for one selected column across a vector
  of batches. A future producer can use it when footer statistics are unavailable
  or a richer summary is needed; the current V2 loader simply fails open.
- **Storage V3 (future integration).** A format-specific footer adapter would
  convert available statistics into summaries covering complete cache cells.
  This requires handling the V3 row-group-to-cell mapping described below.

Existing builders and future adapters share the same output type
(`FieldChunkMetrics`), so pruning consumers need no format-specific decision logic.

## Interface

### `FieldChunkMetrics` (the canonical summary + predicate)

Defined in `index/skipindex_stats/SkipIndexStats.h`. It is the only type the skip filter
consumes:

```cpp
class FieldChunkMetrics {
 public:
    virtual std::unique_ptr<FieldChunkMetrics> Clone() const = 0;
    virtual FieldChunkMetricsType GetMetricsType() const = 0;
    virtual bool CanSkipUnaryRange(OpType op_type, const Metrics& val) const = 0;
    virtual bool CanSkipBinaryRange(const Metrics& lower, const Metrics& upper,
                                    bool lower_inclusive, bool upper_inclusive) const;
    virtual bool CanSkipIn(const std::vector<Metrics>& values) const;
    bool HasUsableStats() const;
    NullState GetNullState() const;
    // ...
};
```

Concrete metrics are `NoneFieldChunkMetrics` (fail open), `IntFieldChunkMetrics<T>`,
`FloatFieldChunkMetrics<T>`, `StringFieldChunkMetrics`, and
`BooleanFieldChunkMetrics`. Metrics carry `NullState` alongside their bounds;
`FieldSkipMetricsView::Precheck` implements the all-null shortcut before invoking
their predicates. `NoneFieldChunkMetrics::CanSkip*` always returns `false`, even
when that object carries `NullState::AllNulls`.

Available metric classes do not imply production support for every type. The V2
loader selects only INT8/16/32/64, FLOAT, DOUBLE, and VARCHAR; it does not collect
BOOL statistics. The view disables bool unary/binary range pruning, although its
`CanSkipInQuery<bool>` overload is supported for a provider supplying bool metrics.

### `SkipIndexStatsBuilder` (the adapter)

`SkipIndexStatsBuilder::Build` converts Parquet statistics or Arrow batches into a
`FieldChunkMetrics`. The actual overloads are:

```cpp
std::unique_ptr<FieldChunkMetrics>
Build(DataType data_type,
      const std::shared_ptr<parquet::Statistics>& statistic) const;

std::unique_ptr<FieldChunkMetrics>
Build(const std::vector<std::shared_ptr<arrow::RecordBatch>>& batches,
      int col_idx,
      arrow::Type::type data_type) const;
```

The Parquet overload backs V2 footer pruning. The Arrow overload summarizes column
`col_idx` across all supplied batches using the specified Arrow type; a producer
must ensure those batches describe the intended cache cell.

A future Vortex adapter can add an overload or equivalent construction path while
preserving the `FieldChunkMetrics` and `FieldSkipMetricsView` consumer interfaces.

### `FieldSkipMetricsView` (the expression's metrics view)

Sealed segment expressions call `GetFieldSkipMetrics(field_id)` once during
initialization. The segment resolves the column from `RuntimeResourceState::fields`
and constructs the view with `FieldSkipMetricsView::FromProvider(column)`.
The segment maintains no separate field-to-provider SkipIndex map.

```cpp
// Segment API: resolve the current field column into an owning view.
FieldSkipMetricsView GetFieldSkipMetrics(FieldId field_id) const;

// View factory: resolve a provider's optional metrics list once.
static FieldSkipMetricsView
FromProvider(std::shared_ptr<FieldChunkMetricsProvider> owner);
```

The view retains a `shared_ptr` to the column generation. Its provider exposes either
an aligned metrics list, a null list (no metrics, never prune), or `std::nullopt`
(use `GetSkipMetrics(chunk_id)` per cell). List-backed views index by `chunk_id`
without repeating field lookups. Storage V1 columns expose a null list. A known
null or empty list does not bind a column filter; per-chunk-only providers remain
eligible. During V2 loading, misaligned lists and lists with neither usable bounds
nor all-null chunks are discarded. This cleanup only controls filter availability;
cell packing is determined solely by the startup flag, regardless of the retained
statistics.

For skip-metrics binding, replacing or dropping a field only needs to update
`runtime.fields`. A newly resolved view
uses the new column's metrics, or fails open if that column has none or the field was
dropped. An existing view keeps its original provider alive. Callers must also bind
layout and data reads to that same generation: the production Search/Retrieve C API
holds a `SegmentReadLease` throughout the operation, preventing publication during
expression construction and execution.

For Storage V2, metrics still live in `GroupCTMeta`, retained through
`ProxyChunkColumn -> ChunkedColumnGroup -> CacheSlot`. Removing the segment map does
not give each field independently reclaimable metrics: other columns in the same group
can keep the group's metrics alive. Moving metrics ownership to individual fields is
a separate follow-up.

For supported calls, `FieldSkipMetricsView` first resolves metadata-only decisions:
all-null chunks are skippable; otherwise `HasUsableStats() == false` fails open before inspecting query
values. For `IN`, prefetch and scan reuse one prepared `vector<Metrics>` per expression.
String entries borrow immutable query literals retained by the expression, so per-cell
checks do not copy string contents or allocate temporary value vectors. The string
range hull is still computed during each check; query min/max precomputation is not part
of this change. Footer string bounds remain owning.

The generic `SkipIndex` helper and its `LoadSkipSource` / `ResolveField` / `Clone` /
`Erase` APIs remain for standalone tests and the `JsonKeyStats` callback interface.
Sealed segment field loading and expression evaluation no longer use that map.

Sequential `Scan` and positional `Take` bind the predicate through a
`ColumnFilter`. `GetDataScanResources(field_id)` returns the column and its
`FieldSkipMetricsView` together from one runtime snapshot; the bound callback
retains that view for the cursor/result lifetime. Scan keeps master's global
row cursor and candidate-mask handling. Expressions enable skip filtering only
for non-nullable fields or a null-rejecting consumer. For nullable fields whose
consumer distinguishes UNKNOWN from FALSE (including below `NOT`), they pass no
skip filter and use normal scanning, positional reads, and prefetch.

## Extensibility

### Plugging in Vortex / Parquet (Storage V3)

This section describes future work. The current V3 load path uses
`ManifestGroupTranslator` with `milvus_storage::api::ChunkReader` and does not
install skip metrics. A future footer adapter must expose a cell-aligned metrics
list through the column's `FieldChunkMetricsProvider`.

V3 currently packs multiple row groups into a cache cell according to the target
byte size. `ManifestGroupTranslator` records the mapping in
`GroupCTMeta::cell_row_group_ranges_`. Consequently, row-group statistics cannot be indexed directly by
cell ID. Integration must either:

- Aggregate statistics over every row group in each cell's range. Bounds must
  conservatively cover all values in that cell; all-null is valid only when every
  contributing row group is known all-null. Missing or incompatible contributing
  statistics must disable the affected pruning capability. Bloom filters require
  a compatible combination covering every contributing row group, or must be omitted.
- Explicitly change V3 packing to one row group per cell and evaluate the resulting
  cache and I/O costs, as the enabled V2 path already does.

For example, if cell 0 contains row groups with ranges `[0, 9]` and `[100, 109]`,
its numeric bounds must cover `[0, 109]`. Using only row group 0's bounds would
incorrectly prune `field == 105`.

Entry *i* of the published per-field list must describe the complete cache cell
*i* in the same column generation. `GroupCTMeta::InstallSkipMetrics` drops a field's
list when its length differs from the cell count; it cannot detect wrongly ordered
or incomplete bounds in a list of the correct length. The producer owns that
semantic alignment. Expressions can then keep consuming `FieldSkipMetricsView`
without storage-specific pruning logic, for either Vortex or Parquet-backed V3.

### Supporting Milvus's own stats

The Arrow-batch overload already computes bounds or boolean presence, null state,
and optional bloom filters for supported types. Bloom construction defaults to off;
`enable_bloom_filter` enables it for integer and string data, with an additional
ngram bloom for applicable strings. Parquet footer metrics currently contain no
bloom filters.

A future producer can use this builder for missing or untrusted footer statistics,
or for richer summaries. It must supply all batches for the intended cell and own
the resulting metrics in that column generation. Neither automatic raw-data
fallback nor combining native bloom filters with footer bounds is implemented by
the current V2 loader.

## Fail-open contract

Missing lists, null metric pointers, and out-of-range list accesses resolve to a
default `NoneFieldChunkMetrics` in the view. Builders also use NONE for unsupported
or unusable bounds, while preserving any known null state. The NONE object's own
predicates never prune.

For supported calls, the view first returns `true` for a known all-null chunk,
including a NONE object carrying `NullState::AllNulls`. Otherwise it returns
`false` when `HasUsableStats()` is false, and delegates to the metric predicate
only when usable statistics exist. Thus missing bounds alone never justify
pruning; independently known all-null state can still rule out a match.

`CanUseSkipFilter` checks the expression's NULL semantics before enabling pruning.
Nullable fields retain normal evaluation when result validity remains observable,
so `NOT` keeps three-valued logic. The expression-result cache continues to store
the complete result and validity bitmaps from JSON Stats paths, keyed by the
expression signature; null rejection does not participate in the cache key.

## Storage V1: no chunk pruning under this interface

Before this seam existed, a Storage V1 sealed column registered a lazily-computed
`FieldChunkMetrics` cache slot that scanned the chunk's own data to derive min/max. That
path is removed for two reasons. First, it was the last remaining producer of metrics
whose lifecycle was independent of the `runtime->fields` column-generation map: a second
field-to-metrics bookkeeping path that had to stay in sync with the primary one, with no
way for the seam to guarantee it did. Second, its lazy computation decoded the chunk
itself to derive the bound, which couples the skip-index holder to the raw column data
rather than to a published, generation-owned summary. Both properties are what this seam
exists to eliminate, so the V1 path is retired rather than preserved.

The consequence is explicit and intentional: **a Storage V1 sealed segment now performs
no statistics-based chunk pruning through this interface**. Storage V2 enables
footer pruning and 1:1 row-group/cell packing by default; this does not restore
pruning for V1. This is a pruning regression for clusters that still hold V1
segments; queries stay correct,
they read more chunks. Storage V1 segments are converted to V2 by storage-version
compaction (`dataCoord.compaction.storageVersion.enabled`, on by default), so the
exposure shrinks over time.

Restoring pruning for V1 is a follow-up, and it plugs into this same seam rather than
reviving the old path: a V1 column implements `FieldChunkMetricsProvider` over metrics
built once at load with a suitable `SkipIndexStatsBuilder::Build` overload and owned by
that column generation. It is deliberately not bundled here so that the V1 metrics'
memory cost and load-time cost can be measured on their own.

## Non-Goals

- Arithmetic predicates. Skip pruning is disabled for arithmetic expressions; enabling it
  again requires an exact proof for overflow, conversion, rounding, and comparator
  boundaries.
- Growing segments. Statistics for growing segments are mutable under concurrent insert;
  this interface targets sealed, immutable column generations.
- Memory accounting for the metrics themselves. Footer metrics live in `GroupCTMeta` for
  the column group's lifetime and are counted neither in `stats_.mem_size` nor in the
  tiered-cache budget. With the flag on this is one `FieldChunkMetrics` per row group per
  skippable field (VARCHAR entries own two `std::string` bounds); bounding or accounting
  it is a follow-up.
