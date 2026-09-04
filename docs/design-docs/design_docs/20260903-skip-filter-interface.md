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
3. `milvus::SkipIndex` + `FieldChunkMetricsProvider::GetSkipMetrics(chunk_id)` — the
   runtime holder that maps a field to a metrics source and answers skip queries.

The goal of this document is to pin down that interface so future storage formats
(Storage V3 Vortex and Parquet) and Milvus's own statistics can plug in without
changing the pruning decision logic.

## Motivation

Chunk-level skip pruning avoids reading a chunk whose statistics prove it cannot match a
predicate. The statistics that back pruning come from different places depending on the
storage format:

- **Storage V2 (Parquet footer).** Per-row-group `parquet::Statistics` carry min/max and
  null counts. The loader forces one row group per cache cell for skippable types so the
  footer statistics align 1:1 with cells, then converts each `parquet::Statistics` into a
  `FieldChunkMetrics` via `SkipIndexStatsBuilder::Build(DataType, parquet::Statistics)`.
- **Milvus-native stats.** When footer statistics are absent or untrusted, Milvus can
  compute the same summary from the data it already has — an `arrow::RecordBatch`.
  `SkipIndexStatsBuilder` already has a `Build(arrow::RecordBatch)` overload for this.
- **Storage V3 (Vortex).** Vortex exposes per-row-group zone maps through
  `milvus_storage::vortex::VortexFooterReader`. Wiring these into pruning is a new
  adapter on the same seam, not a new decision path.

Each of these produces the *same* type (`FieldChunkMetrics`), so the skip filter does not
care which format the statistics came from.

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
    NullState GetNullState() const;
    // ...
};
```

Concrete metrics are `NoneFieldChunkMetrics` (fail open), `IntFieldChunkMetrics<T>`,
`FloatFieldChunkMetrics<T>`, `StringFieldChunkMetrics`, and
`BooleanFieldChunkMetrics`. A `NullState` is carried alongside the bounds so that a
chunk of all-null rows is always skippable even when its min/max are empty.

### `SkipIndexStatsBuilder` (the adapter)

`SkipIndexStatsBuilder::Build` is the single place a raw statistics source becomes a
`FieldChunkMetrics`:

| Overload | Source | Purpose |
| --- | --- | --- |
| `Build(DataType, parquet::Statistics)` | Parquet footer | Storage V2 footer pruning |
| `Build(arrow::RecordBatch)` | Arrow batches | Milvus-native stats |

A Storage V3 Vortex adapter adds a `Build(vortex_zone_map)` overload here; it does not
touch `FieldChunkMetrics` or `SkipIndex`.

### `SkipIndex` (the runtime holder)

`SkipIndex` maps `FieldId` to a metrics source and answers `CanSkip*` queries. After the
Storage V1 lazy-compute path was removed, the source is any `FieldChunkMetricsProvider` —
a `ChunkedColumnInterface` generation today, a Vortex/Parquet group or Milvus-native
stats tomorrow:

```cpp
class SkipIndex {
 public:
    void LoadSkipSource(FieldId field_id,
                        std::shared_ptr<FieldChunkMetricsProvider> source);
    bool CanSkipUnaryRange<T>(...);
    bool CanSkipBinaryRange<T>(...);
    bool CanSkipInQuery<T>(...);
    std::shared_ptr<SkipIndex> Clone() const;
    void Erase(FieldId field_id);
};
```

The metrics pointer returned by `GetSkipMetrics` is owned by the provider, which
`SkipIndex` holds via `shared_ptr`, so it remains valid for the `SkipIndex` snapshot's
lifetime.

`LoadSkipSource` binds a field to exactly one column generation and is called for every
loaded column, including those with no metrics at all. Binding unconditionally is what
makes a replaced column retire its predecessor's source: a field that is reloaded from a
different storage version must not keep bounds that describe the previous cell layout.

## Extensibility

### Plugging in Vortex / Parquet (Storage V3)

A Storage V3 column group loads footer zone maps with
`milvus_storage::vortex::VortexFooterReader`, converts each row group's zone map into a
`FieldChunkMetrics` (a new `SkipIndexStatsBuilder::Build` overload or an equivalent
constructor), stores them positionally in the group's metadata, and exposes them through
`FieldChunkMetricsProvider::GetSkipMetrics`. `SkipIndex` is unchanged. The same holds for a
Parquet-backed V3 group.

The one invariant the loader must preserve is positional alignment: index *i* of the
per-field metrics list must describe cache cell *i*. A mismatch must disable pruning for
that field rather than associate a bound with the wrong cell (see `GroupChunkTranslator`).

### Supporting Milvus's own stats

The `Build(arrow::RecordBatch)` overload already computes min/max, null state, and
(optionally) a bloom filter from raw Milvus data. This is the path to use wherever
footer statistics are unavailable, untrusted, or Milvus wants a richer summary
(e.g. a bloom filter on top of footer bounds).

## Fail-open contract

A missing, incompatible, or unusable statistic is always represented by
`NoneFieldChunkMetrics`, which never prunes. The skip filter may lose a pruning
opportunity but never drops a matching row. Null-state semantics are preserved
separately: an all-null chunk is skippable regardless of bounds, and a skipped chunk of
a nullable column is still materialized for its validity bitmap whenever result validity
remains observable (see `SkippedChunkNeedsValidity`), so `NOT` keeps three-valued logic.

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
no chunk-level skip pruning at all**, and because `common.parquetStatsSkipIndex.enabled`
defaults to `false`, the default configuration performs none for Storage V2 either. This
is a pruning regression for clusters that still hold V1 segments; queries stay correct,
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
