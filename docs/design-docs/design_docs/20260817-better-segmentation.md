# Better Segmentation: Predictable Segment Sizing

**Date**: August 2026
**Status**: Draft
**Scope**: Segment size metric (`wholeRow`/`mainIndex`), data-driven main index column selection with schema fallback, optional hard whole-row ceiling for QueryNode loadability, and consistent wiring across streaming / DataCoord / QueryNode / compaction / import
**Feature DRI**: @Congqi Xia
**Primary Approver**: @tedxu
**Independent Approver**: TBD
**Design Review**: TBD
**Review Record**: TBD

---

## 1. Overview

### 1.1 Motivation

Today `dataCoord.segment.maxSize` (default 1024MB,
`pkg/util/paramtable/component_param.go:5677`) constrains a segment's
**whole-row binary size** — the sum over every column of every row. This is
enforced on two paths that are semantically equal:

- **StreamingNode write path (effective constraint).** The proxy leaves
  `Partitions[].BinarySize = 0` (`internal/proxy/task_insert_streaming.go:350`),
  so the shard interceptor falls back to `msg.EstimateSize()` — the full
  serialized insert payload, i.e. all columns (`shard_interceptor.go:198-202`,
  `pkg/streaming/util/message/message_impl.go:126`). Accumulated
  `Modified.BinarySize` is compared against `MaxBinarySize`
  (`utils/stats.go:108-141`), where `MaxBinarySize = jitter(1±10%) × maxSize ×
  sealProportion(0.12)` (`shards/segment_limitation_policy.go:82-84`) — a
  segment seals at ≈12% × 1024MB ≈ **122MB of whole-row binary**.
- **DataCoord row-count upper limit.** `calBySchemaPolicy` derives
  `maxRows = maxSize×1MB / EstimateSizePerRecord(schema)`, and
  `EstimateSizePerRecord` sums **all fields**
  (`segment_allocation_policy.go:39-53`, `pkg/util/typeutil/schema.go:134`).
  Streaming-created segments carry `MaxRowNum = 0` (`segment_manager.go:432`,
  "deprecated, we use binary size"), so the write path above is authoritative in
  streaming mode.

Whole-row is the wrong constraint for a segment's *expensive* resource. The cost
driver for a Milvus segment is the **vector index**: its memory, build time, and
per-query cost scale with the main vector column (`dim × element_size × rows`),
not with scalar columns (cheap to index and scan). Under whole-row semantics, a
collection whose rows are dominated by scalars (long `VarChar`/`JSON`/`Array`,
multiple vectors, wide dynamic fields) seals "too early": each segment carries a
small vector index, producing many small, inefficient indexes and excessive
segment counts.

Conversely, on **extreme schemas** — a tiny vector next to very large scalar or
indexed-scalar columns — a main-index-only budget lets a segment grow far beyond
what a single QueryNode can load. QueryNode has **no per-segment hard limit**
(`checkLoadingResource`, `segment_loader.go:1819-1896`, only checks the node
aggregate), so such a segment fails load and can never be served.

A vector-column size model already exists as precedent: the clustering **analyze**
task sizes clusters by the vector column of the clustering key only —
`totalSegmentsRawDataSize = totalRows × dim × VectorTypeSize(fieldType)` and
`numClusters = rawSize / (maxSize × ratio)` (`task_analyze.go:207-208`,
`typeutil.VectorTypeSize` at `schema.go:642-655`). This design generalizes that
idea to the segmentation constraint.

### 1.2 Goals

1. Let `maxSize`/`diskSegmentMaxSize` constrain the main index column instead of
   the whole row, behind an opt-in metric.
2. Select the main index column **data-driven** — the vector field with the
   largest actual index memory — wherever actual per-field size is measurable,
   falling back to schema inference only where it is not.
3. Keep every consumer that shares the "segment max size" concept on the same
   metric so a segment's row capacity is consistent no matter which path creates
   it (streaming, import, compaction output).
4. Preserve current behavior when the metric is not enabled; keep the write-path
   change version-safe (single enforcement point; proxy unchanged).
5. Provide an optional hard whole-row ceiling so the final sealed segment stays
   loadable by a single QueryNode even on extreme schemas; the ceiling is
   configurable per deployment (default off).

### 1.3 Non-Goals

- Per-collection metric selection (deferred; cluster-wide config first).
- Budgeting multiple columns or partial row bytes (the main index is exactly one
  column; secondary vectors contribute `0`).
- Changing `sealProportion`, `sealProportionJitter`, or the numeric value of
  `maxSize` itself.
- Re-measuring or migrating existing segments; the metric applies at segment
  creation (invariant I1).
- Altering the flush HWM/LWM memory-pressure model (it must keep counting
  whole-row bytes — §5).
- A per-segment QueryNode load-memory *prediction*
  (`estimateLoadingResourceUsageOfSegment`). The ceiling is expressed in
  whole-row raw bytes — a conservative, schema-free proxy for load memory (§7).
- Streaming / spill-to-disk scalar index build and disk-resident scalar indexes
  (§8 — separate topic).

### 1.4 Current Code Facts

The following current implementation details shape the design:

- `dataCoord.segment.maxSize` (1024MB), `sealProportion` (0.12),
  `sealProportionJitter` (0.1), `diskSegmentMaxSize` (2048MB) live in
  `pkg/util/paramtable/component_param.go:5677-5711`. `SegmentMaxSize` is
  non-refreshable.
- StreamingNode write-path budget generation:
  `shards/segment_limitation_policy.go:78-90`; enforcement in
  `utils/stats.go:108-141` (`AllocRows`, `MaxBinarySize`, `ReachLimit`).
- The interceptor substitutes the proxy's zero `BinarySize` with
  `msg.EstimateSize()` (`shard_interceptor.go:198-202`); the message body is
  deserialized and function fields materialized before segment assignment, so
  the actual vector columns are in memory at that point.
- `Modified.BinarySize` feeds **both** the seal check and the memory-pressure
  path: flush HWM/LWM, `RuntimeFlushSize`, `totalFlushSize`
  (`stats_manager.go:171-190, 223-250`), growing-bytes metrics, and datacoord's
  `sealByTotalGrowingSegmentsSize` (`segment_allocation_policy.go:200`).
- `MaxBinarySize` is persisted in recovery meta
  (`streamingpb.SegmentAssignmentStat`, restored from
  `CreateSegmentMessageHeader.MaxSegmentSize`, `segment_recovery_info.go:48`),
  so it is fixed per segment and survives restart.
- DataCoord row cap: `calBySchemaPolicy` (`segment_allocation_policy.go:39-53`);
  `MaxRowNum` set on new segments (`segment_manager.go:432`); `SetMaxRowCount`
  exists but is test-only.
- QueryNode segcore row cap: `ComposeIndexMeta` computes `MaxIndexRowCount`
  (`index_meta.go:44-52`) → `CreateCCollection` (`collection.go:606`). In segcore
  it is an **index-build threshold** (`GetBuildThreshold = max_index_row_count ×
  build_ratio`, `internal/core/src/segcore/IndexConfigGenerator.cpp:99-104`),
  not a hard per-segment cap.
- Compaction target: `getExpectedSegmentSize` (`compaction_trigger_v2.go:872-880`,
  DiskANN → `diskSegmentMaxSize`); merge decisions measure actual sizes with
  `segment.getSegmentSize()` = `InsertBinlog + Stats + Delta`
  (`segment_info.go:519-522`) and `SegmentView.Size` = `InsertBinlogSize`
  (`compaction_view.go:178`). Per-field binlog size is available via
  `getFieldBinlogSize(fieldID)` (`segment_info.go:524-545`).
- DataCoord already models scalar index build memory from a field's binlog
  `MemorySize`: `calculateIndexTaskSlot(fieldSize, numRows, ...)` /
  `estimateFMIndexBuildPeakBytes` (`internal/datacoord/util.go:470-557`).
- Clustering compaction derives a row target from
  `expectedSize / rowSize` where `rowSize = SegmentView.Size / NumOfRows`
  (`compaction_policy_clustering.go:242-275`).
- Import: `GetSegmentMaxSize` → `getExpectedSegmentSize`
  (`import_util.go:140-146`); splitter allocates one segment per `segmentMaxSize`
  of file bytes (`import_util.go:191-205`). `ImportFileStats` /
  `PartitionImportStats` carry only whole-file aggregates (`TotalRows`,
  `TotalMemorySize`, `PartitionDataSize`) — no per-field sizes
  (`pkg/proto/datapb/data_coord.pb.go:8385-8450`).
- Per-row actual-size machinery already exists:
  `calcVectorSize`/`EstimateEntitySize` handle dense, sparse (per-row contents
  bytes) and `ArrayOfVector` (per-row inner vectors)
  (`pkg/util/typeutil/schema.go:289-391`).
- Proxy inserts set `BinarySize = 0` with a TODO ("message estimate size is
  used") (`task_insert_streaming.go:350`).

---

## 2. Design Principles

### 2.1 The Main Index Column Is the Cost Driver

The budget must track the resource that actually dominates a segment's cost: the
largest vector index. All other columns (scalars, secondary vectors) are cheap
to index and scan, and inflating the budget with them causes premature sealing.

### 2.2 Data-Driven Selection With Schema Fallback

Where actual per-field memory is measurable, the main index column is the vector
field with the **largest actual size**. The schema inference (`dim × elem`, the
schema-widest dense column) is used **only** where data is unavailable. For
fixed-dim dense columns the two agree exactly; they diverge only for
variable-size columns (`ArrayOfVector`, sparse) and multi-vector collections
where the true cost driver is not the schema-widest column.

### 2.3 Decouple Seal Size From Binary Size

`Modified.BinarySize` serves two purposes today: the seal capacity check and the
memory-pressure path. Under `mainIndex` they must be split: the seal check uses
the dominant vector column's bytes; flush HWM/LWM, growing-bytes metrics, and
the whole-row ceiling keep using actual whole-row bytes.

### 2.4 One Metric, One Unit, Across Every Producer

The effective row cap must agree among all producers (streaming seal, DataCoord
`MaxRowNum`, QueryNode segcore `MaxIndexRowCount`, compaction target), and every
size-vs-budget comparison must use the active metric on **both** sides. §9
states these as invariants I1–I4.

### 2.5 Loadability Is a Source-Time Bound, Not a Load-Time Check

QueryNode rejects an unloadable segment only at load time — too late. The hard
whole-row ceiling bounds the final segment at creation, sealing it before it can
become unloadable.

---

## 3. Size Metric: Public Interface

Two new non-refreshable config items alongside `dataCoord.segment.maxSize`:

```yaml
dataCoord:
  segment:
    maxSize: 1024                # unchanged: budget, now interpreted in the active metric
    sizeMetric: wholeRow         # NEW. "wholeRow" (default, current behavior) | "mainIndex"
    maxFullSegmentSize: -1          # NEW. Optional hard ceiling on actual whole-row bytes (MB).
                                 # -1 = no limit (default). Only consulted when sizeMetric="mainIndex".
```

- `dataCoord.segment.sizeMetric = "wholeRow" | "mainIndex"` — string constants,
  not a numeric enum. Non-refreshable, consistent with `SegmentMaxSize`.
  `"mainIndex"` interprets both `maxSize` and `diskSegmentMaxSize` in
  main-index-column bytes.
- `dataCoord.segment.maxFullSegmentSize` (MB, non-refreshable, default `-1`). An
  optional hard ceiling on a segment's actual whole-row bytes, independent of the
  metric. `-1` (or any non-positive value) disables it — the segment is bounded
  only by the metric budget, and loadability is **not** guaranteed on extreme
  schemas. A positive value caps the final segment so a single QueryNode can load
  it (§7). Only consulted when `sizeMetric="mainIndex"`; when
  `sizeMetric="wholeRow"` the existing whole-row constraint is already the bound.
- No proto, SDK, or metric shape changes. `MaxBinarySize` in
  `streamingpb.SegmentAssignmentStat` keeps its field; only its unit changes for
  newly created segments.

---

## 4. Main Index Column Definition

The main index column is the **vector field with the largest index memory
footprint**. Selection is data-driven where the actual size is measurable, and
falls back to schema inference only where it is not.

### 4.1 Measured Selection (Preferred)

Whenever a measurement scope can see actual per-field memory, the main index
column is the vector field with the **largest actual memory size** in that scope:

- write-path insert batch → actual bytes per vector column in the message
  (`EstimateVectorColumnSize`; §5);
- sealed segment → per-field binlog `MemorySize` (the largest vector field, §6.3);
- per-segment load info → the same per-field binlog sizes.

For fixed-dim dense columns, actual bytes = `dim × elem × rows`, so measured
selection and the schema inference agree exactly. The measured path only
diverges when a variable-size column (`ArrayOfVector`, sparse, or nulls)
actually dominates, or when a multi-vector collection's true cost driver is not
the schema-widest column. This is exactly the case schema inference cannot
capture — a narrow `ArrayOfVector` with many inner vectors per row can cost far
more than a wider fixed-dim column.

### 4.2 Schema Fallback (Only Where Measurement Is Unavailable)

Contexts with schema only — legacy DataCoord row cap / `AllocSegment`, QueryNode
`ComposeIndexMeta`, import before the preimport extension (§6.5), cipher messages
on the write path:

- pick the dense vector field maximizing `dim × VectorTypeSize(dt)`:
  `FloatVector: dim×4`, `Float16Vector/BFloat16Vector: dim×2`,
  `Int8Vector: dim`, `BinaryVector: dim/8`;
- no dense vector field, or only sparse fields: fall back to whole-row
  (`sizeMetric` behaves as `wholeRow` for the collection) — sparse and
  `ArrayOfVector` sizes are per-row-variable and unbounded by schema;
- multi-vector: only the selected column counts; the others contribute `0` to the
  budget (secondary vectors are still indexed but not budgeted — the measured
  path auto-corrects this when their actual size dominates).

### 4.3 Helpers

```go
// EstimateMainIndexSizePerRecord returns per-row bytes of the widest dense
// vector column (dim × element_size) — the SCHEMA FALLBACK in §4.2.
// 0 means "no applicable column"; callers fall back to whole-row semantics.
func EstimateMainIndexSizePerRecord(schema *schemapb.CollectionSchema) (int, error)

// EstimateVectorColumnSize returns the ACTUAL in-memory bytes of a vector
// FieldData column: dense = len×elem; SparseFloatVector = sum of per-row
// contents bytes; ArrayOfVector = sum of inner vector bytes (each via
// calcVectorSize on the element type). O(column bytes).
func EstimateVectorColumnSize(fieldData *schemapb.FieldData) (int, error)

// SelectMainIndexField returns the vector field ID with the largest ACTUAL
// column size among the given per-field sizes; empty if none. Callers use this
// when measured sizes are available, else fall back to §4.2.
func SelectMainIndexField(schema *schemapb.CollectionSchema, fieldIDToSize map[int64]int) (int64, bool)
```

---

## 5. StreamingNode Write Path — Decouple Seal Size From Binary Size

`Modified.BinarySize` serves two purposes and must be split under the new metric:

1. **Seal capacity check** (`utils/stats.go:108`, `AllocRows`) — must use the
   active metric (dominant vector column).
2. **Memory pressure** — flush HWM/LWM, `RuntimeFlushSize`, `totalFlushSize`,
   growing-bytes metrics (`stats_manager.go:171-190, 223-250`), and
   `sealByTotalGrowingSegmentsSize` (`segment_allocation_policy.go:200`) — about
   **actual in-memory / whole-row bytes** and must stay whole-row, otherwise the
   memory watermarks silently under-count.

### 5.1 Implementation Sketch

- Keep `ModifiedMetrics.BinarySize` = actual serialized payload (whole-row), fed
  by `msg.EstimateSize()` as today; it drives flush HWM/LWM, metrics, **and** the
  whole-row ceiling accumulator.
- Add a seal-specific accumulator (e.g. `ModifiedMetrics.SealSize`) computed in
  the shard interceptor **after function-field materialization**:
  - **measured (preferred):** the insert body holds the actual vector columns, so
    compute each vector column's bytes with `EstimateVectorColumnSize` and take
    the largest → `SealSize = max over vector columns`. For fixed-dim dense
    columns this is O(1) (`dim × elem × rows`, exact); only when the batch
    contains a variable-size column (`ArrayOfVector`/sparse) or the schema's
    widest column may not be the cost driver is an O(column) scan needed. If the
    message body is unavailable (e.g. cipher without plaintext at this point),
    fall back to the schema estimate.
  - **fallback (no data):**
    `SealSize = partition.GetRows() × typeutil.EstimateMainIndexSizePerRecord(schema)`.
  - When `sizeMetric = wholeRow`, `SealSize = BinarySize`.
- **Dual seal condition** in `AllocRows`/`ShouldBeSealed`: a segment is sealed
  when it reaches its budget **or** its ceiling:
  - budget: `SealSize ≥ MaxBinarySize`, where `MaxBinarySize` remains
    `jitter × maxSize × proportion` but is now interpreted in the metric's unit;
  - ceiling (only when `maxFullSegmentSize > 0`, and only meaningful under
    `mainIndex`): `BinarySize ≥ maxFullSegmentSize` (whole-row actual bytes) — the
    loadability guarantee.
- The segment limitation policy (`shards/segment_limitation_policy.go:78-90`)
  becomes schema-aware (or the metric decision is hoisted to the alloc worker,
  which already persists the chosen `limitation` across retries,
  `segment_alloc_worker.go:55,134-156`).

This keeps the proxy untouched (`task_insert_streaming.go:350` stays `0`) and
makes the streaming node the single enforcement point, which is the safest
surface for a rolling upgrade.

---

## 6. Consumer Wiring

| # | Consumer | Today (whole-row) | Main-index mode | Measurement source | Enforce at |
|---|----------|-------------------|-----------------|--------------------|------------|
| 1 | StreamingNode seal limit | payload bytes (`msg.EstimateSize`) | largest vector column in the batch (budget) **or whole-row ceiling** | **actual** per-insert vector columns (`EstimateVectorColumnSize`); schema fallback only when body unavailable | `AllocRows`/`stats.go:108` |
| 2 | StreamingNode flush HWM/LWM + metrics | actual bytes | **unchanged** (actual bytes) | payload bytes | `stats_manager.go` |
| 3 | DataCoord row-cap / legacy `AllocSegment` | `EstimateSizePerRecord` | `min(mainIndex budget, ceiling)` rows | schema estimate (no data available; unused in streaming mode) | `calBySchemaPolicy` (`segment_allocation_policy.go:39-53`) |
| 4 | DataCoord seal policies (capacity/idle) | `MaxRowNum`-derived | derives from #3 (`MaxRowNum`) | — | `segment_manager.go` |
| 5 | QueryNode segcore `MaxIndexRowCount` | `EstimateSizePerRecord` | `min(mainIndex budget, ceiling)` rows | schema estimate at compose time (build threshold, not a hard cap — `IndexConfigGenerator.cpp:99-104`) | `ComposeIndexMeta` (`index_meta.go:44-52`) → `CreateCCollection` (`collection.go:606`) |
| 6 | Mix-compaction merge target | actual whole-row (`getSegmentSize`) | largest vector field per segment, capped by ceiling | **actual** per-field binlog `MemorySize` (`getFieldBinlogSize`, `segment_info.go:524`) | `getExpectedSegmentSize` comparisons (`compaction_trigger.go:624,934`), knapsack |
| 7 | Clustering compaction config | `SegmentView.Size` (insert-binlog whole-row) | largest vector field per segment, capped by ceiling | **actual** per-field binlog (or `rows × mainIndexPerRecord` for growing) | `estimateRowsBySegmentSize` (`compaction_policy_clustering.go:242`) |
| 8 | Clustering analyze `numClusters` | vector column of clustering key (already) | unchanged (align denominator semantics) | `rows × dim × VectorTypeSize` | `task_analyze.go:207-208` |
| 9 | Force-merge target | whole-row | largest vector field per segment, capped by ceiling | **actual** per-field binlog | `compaction_view_forcemerge.go` |
| 10 | Import segment split | file bytes (`PartitionDataSize`) | see §6.5 (whole-row, ceiling-bounded) | file bytes / rows; actual per-field after preimport extension | `AssignSegments` (`import_util.go:191-205`) |

### 6.1 Row-Cap Formula

Every row-cap and merge-target above is the **minimum** of the budget row bound
and — only when the ceiling is enabled (`maxFullSegmentSize > 0`) — the ceiling row
bound. `mainIndexPerRecord` is the dominant vector column's per-row bytes:
**measured** (`EstimateVectorColumnSize` / per-field binlog ÷ rows) where the
measurement scope has data, else the **schema fallback**
(`EstimateMainIndexSizePerRecord`):

```text
rowCap          = (ceiling enabled) ? min(budgetRows, ceilingRows) : budgetRows
budgetRows      = (measured scope) ? proportion×maxSize / dominantColumnPerRow
                                    : (metric == mainIndex) ? proportion×maxSize / EstimateMainIndexSizePerRecord(schema)
                                                             : maxSize / wholeRowPerRecord
ceilingRows     = maxFullSegmentSize / wholeRowPerRecord
```

For fixed-dim dense schemas the measured and schema values coincide exactly, so
`rowCap` is stable across producers (I2); for variable-size main columns the
measured path is authoritative where it exists and the schema fallback only
covers schema-only contexts.

### 6.2 DataCoord Row Cap / `MaxRowNum`

DataCoord has only the schema at allocation time → schema fallback. In streaming
mode `MaxRowNum = 0` is unused, so this only affects the legacy `AllocSegment`
path and the capacity/idle seal policies. No correctness impact on the streaming
constraint; documented divergence for `ArrayOfVector`/sparse.

### 6.3 Compaction (Mix / Clustering / Force-Merge) — Measurement Mismatch

Under whole-row, compaction measures actual sizes with `segment.getSegmentSize()`
(`segment_info.go:519-522`) and `SegmentView.Size` = `InsertBinlogSize`
(`compaction_view.go:178`). Under `mainIndex`, **the target (budget) is in
main-column bytes while the measurement is whole-row bytes**, so merge decisions
would never converge. Every size comparison in #6/#7/#9 must therefore switch to
main-column bytes. This is a **measured** context: `getFieldBinlogSize(fieldID)`
(`segment_info.go:524-545`) returns the per-field binlog size for any sealed
segment, so the main column is the vector field with the **largest per-field
size** (`SelectMainIndexField`), not a fixed schema choice. For growing segments
(no per-field binlogs yet) use `rows × EstimateMainIndexSizePerRecord` (schema
fallback). This mirrors the datacoord scalar-index memory model already in
production, which sizes index build slots from the field's binlog `MemorySize`
(`util.go:470-557`).

### 6.4 Clustering Analyze `numClusters`

`numClusters = totalRows × dim × VectorTypeSize / (maxSize × ratio)`
(`task_analyze.go:207-208`) is already vector-column based; it stays unchanged
when the clustering key is the main column. Denominator semantics are aligned
with the metric.

### 6.5 Import

`GetSegmentMaxSize` → `getExpectedSegmentSize` (`import_util.go:140-146`) already
returns the same budget; the splitter allocates one segment per `segmentMaxSize`
of file bytes (`import_util.go:191-205`). Options:

- **Option A (recommended for v1): import keeps whole-row splitting.** Imported
  segments carry fewer rows per byte than streaming segments under `mainIndex`.
  The divergence is bounded and safe (imported segments are simply smaller), and
  import needs no parsing change. Documented as a known divergence; compaction
  (#6/#7, which runs on all L1 segments) rebalances row capacities over time.
- **Option B (aligning, follow-up): split by actual dominant vector column
  bytes.** `ImportFileStats` today carries only `TotalRows`/`TotalMemorySize`/
  `PartitionDataSize` (whole-file aggregates, `data_coord.pb.go:8385-8450`) —
  there is **no per-field size**, so import cannot measure the main column from
  stats yet. Pre-import already parses every row into columns on DataNode, so
  the extension is small: report per-field bytes in `ImportFileStats` (proto +
  DataNode pre-import accumulation), then split by the largest vector field's
  actual bytes via `SelectMainIndexField` + `EstimateVectorColumnSize`. Until
  then import falls back to schema inference
  (`rows × EstimateMainIndexSizePerRecord`), which is wrong for
  `ArrayOfVector`/sparse — hence Option A for v1.

### 6.6 Metrics and Flush HWM/LWM

Flush HWM/LWM, growing-bytes metrics, and `sealByTotalGrowingSegmentsSize` stay
on whole-row actual bytes (#2) — they bound real memory, not the metric.

---

## 7. Hard Ceiling and QueryNode Loadability (`maxFullSegmentSize`)

QueryNode has **no per-segment hard limit**: `checkLoadingResource`
(`segment_loader.go:1819-1896`) only rejects when the *aggregate* committed +
predicted memory exceeds the node budget
(`totalMem × OverloadedMemoryThresholdPercentage`); the per-segment
`maxSegmentSize` it computes (`segment_loader.go:1806`) is used for logging only.
A segment whose load memory alone exceeds the node budget therefore fails the
load and can never be served — this is what the `mainIndex` metric can provoke on
extreme schemas (e.g. dim=8 FloatVector + 1KB scalars ⇒ ~30× row inflation ⇒ a
segment that is gigabytes in whole-row bytes).

The ceiling fixes this at the *source* (segment creation), not at load time:

- **Quantity bounded.** `maxFullSegmentSize` bounds a segment's **whole-row raw
  data bytes** — the conservative, schema-free proxy for load memory. The two
  terms a QueryNode pays are (a) index memory — dominated by the main column and
  therefore already bounded to `≈ proportion × maxSize × indexFactor` by the
  budget — and (b) raw data + deltalogs + per-field overhead — bounded by the
  ceiling. Setting the ceiling to a value that a single QueryNode can load makes
  loadability a guarantee.
- **Default and who sets it.** Open-source default is `-1` (no limit) to keep
  `mainIndex` opt-in and behavior-predictable for existing deployments; the
  loadability guarantee is the operator's choice. Zilliz Cloud sets it **per CU
  class**: each CU class fixes a QueryNode memory budget, and the ceiling is
  derived so one ceiling-sized segment fits that budget (plus headroom for index
  memory and the memory-load factors below). Open-source guidance (the doc to
  ship with the feature): set `maxFullSegmentSize` from the QueryNode memory budget —
  a segment's load memory is roughly
  `wholeRowBytes × loadMemoryUsageFactor + mainIndexBytes × memoryIndexLoadFactor`
  (`segment_loader.go:1770-1778`), so choose `maxFullSegmentSize` such that a single
  such segment plus the node's other commitments stays under
  `totalMem × OverloadedMemoryThresholdPercentage` (`segment_loader.go:1874`).
  Leave `-1` only when extreme schemas are known absent or the workload tolerates
  unloadable segments.
- **Enabled only when positive.** Any non-positive value disables the ceiling.
- **Enforced at both ends of the lifecycle.**
  - Write path (#1): seal when `BinarySize ≥ maxFullSegmentSize` regardless of how
    far the segment is from its `mainIndex` budget.
  - Compaction (#6/#7/#9): merge/view planning must not produce an output segment
    whose whole-row bytes exceed the ceiling (`ceilingRows` in §6.1); the
    whole-row measurement already exists (`getSegmentSize`, `SegmentView.Size`).
  - Import (#10): splitting is whole-row, so it is ceiling-bounded by
    construction; if `maxFullSegmentSize < segmentMaxSize`, import must split by
    `min(segmentMaxSize, maxFullSegmentSize)`.
- **Interaction with the amplification guard.** §11 keeps the ratio-based guard
  (`maxRowAmplification`, falls back to whole-row for pathological schemas) as the
  *quality* bound (typical segment shape), while the ceiling is the *safety*
  bound (absolute loadability). Both are optional knobs on top of the metric;
  when set to a positive value the ceiling alone is sufficient to guarantee
  loadability.

---

## 8. Case Review: Large Scalar Index (Build Requires a Full Field Load)

**Scenario.** The widest dense vector is small, but the collection carries a
*wide indexed scalar field*: a long `VarChar`/`Text` full-text or BM25 field, a
large `JSON`/`Array` inverted index, or a wide `Geometry` index. Under
`sizeMetric="mainIndex"` the budget counts only the vector column, so the segment
may legally grow to far more rows than the scalar field can be handled in memory.

**Current state.** Building a scalar index requires the **entire field column of
the segment in memory**:

- IndexNode builds the index from the segment's binlogs by fully materializing
  the field; an inverted index's resident footprint is a multiple of the raw
  field size (postings + dictionary overhead), and full-text/BM25 needs the
  complete token stream plus IDF statistics.
- QueryNode loads the built index files and, depending on
  `PreferFieldDataWhenIndexHasRawData`, may also retain the raw field data
  (`segment_loader.go:2210-2213`).

The `mainIndex` budget bounds none of this — it only bounds the vector column.

**How this design covers it.** `maxFullSegmentSize` bounds **whole-row raw bytes**,
which includes the large scalar field. The ceiling therefore bounds how many rows
— and thus how much scalar content — an index build/load must hold, exactly as it
bounds whole-row data for the vector case in §7. When sizing the ceiling (§7),
deployments with heavy scalar indexes must fold in the scalar-index memory factor
(raw field bytes × per-index-type factor, plus text tokenization/IDF working
set), not just the raw-data factor. DataCoord already models this memory from the
field's binlog size (`calculateIndexTaskSlot`/`estimateFMIndexBuildPeakBytes`,
`util.go:470-557`). This is the more common extreme than a giant vector (small
vector + large text/JSON), so the ceiling's role here is not an edge case — it is
a primary motivation.

**Separate topic (explicitly out of scope).** Streaming field reads for index
build, spill-to-disk indexing (index content on disk), and lazy/partial scalar
index load would relax the memory requirement so the ceiling could be raised.
They are follow-up optimizations and do not change the ceiling's role in this
design; they are tracked separately so this design's memory bound stays
conservative.

---

## 9. Consistency Invariants

- **I1 — One metric per segment lifetime.** `MaxBinarySize` is fixed at segment
  creation and persisted (`utils/stats.go:46`, recovery
  `segment_recovery_info.go:48`). A metric change affects only segments created
  after the change; old segments keep their persisted budget.
- **I2 — Consistent row capacity across producers.** The effective row cap
  (`min(mainIndex budget rows, ceiling rows)`) must agree among: streaming seal
  (#1), DataCoord `MaxRowNum` (#3), segcore `MaxIndexRowCount` (#5), and
  compaction target (#6/#7). A mismatch makes compaction oscillate or stall.
- **I3 — Same unit in comparison.** Every "size vs budget" comparison must use
  the active metric on **both** sides (target and measurement). §6.3 is the trap.
- **I4 — Loadability (hard ceiling, optional).** When `maxFullSegmentSize > 0`, no
  path (streaming seal, compaction output, import) may publish a segment whose
  whole-row bytes exceed it under `sizeMetric="mainIndex"`. §7 and the `min()` in
  §6.1 enforce it at both ends of the lifecycle; a violation is a hard error, not
  a load-time rejection. With the default `-1` the invariant is vacuous and
  loadability is the operator's responsibility (§7).

---

## 10. Compatibility and Rollout

### 10.1 Feature Flag Semantics

- `sizeMetric` defaults to `"wholeRow"` and `maxFullSegmentSize` defaults to `-1`;
  nothing else changes by default.
- No proto or persisted-schema change. `SegmentAssignmentStat.MaxBinarySize` and
  `CreateSegmentMessageHeader.MaxSegmentSize` keep their fields; unit is implied
  by the cluster config at creation time (I1 makes this safe).
- No migration or rollback cleanup. To revert, set `sizeMetric="wholeRow"` and
  let existing main-column-budget segments age out via normal flush/compaction
  (their persisted `MaxBinarySize` is honored until then).

### 10.2 Rolling Upgrade / Mixed Versions

- Default `sizeMetric="wholeRow"` ⇒ zero behavior change; binaries can be
  upgraded independently.
- Enabling `sizeMetric="mainIndex"` requires **all** of StreamingNode, DataCoord,
  and QueryNode to run a build that understands the metric, because #1, #3, and
  #5 are wired on each side respectively. A mixed cluster with the metric on
  violates **I2** (e.g. new DataCoord computes main-column `MaxRowNum` while an
  old StreamingNode still seals on whole-row payload). Gate enabling on the
  minimum version; document it.
- A positive `maxFullSegmentSize` must be set with the **same version gate**: an old
  StreamingNode that does not know the ceiling can seal a segment above it,
  silently breaking I4. Rolling back to `-1` is always safe — it only relaxes the
  bound.
- Proxy and DataNode need no change.
- `MaxBinarySize` persists in recovery meta, so a restart/upgrade preserves the
  budget of existing growing segments automatically; only newly created segments
  pick up the new unit.

### 10.3 Open Source vs Managed

- Open-source documentation (shipped with the feature) explains when to enable
  `sizeMetric="mainIndex"`, how to size `maxFullSegmentSize` from the QueryNode
  memory budget (§7), and the loadability trade-off of leaving it at `-1`.
- Zilliz Cloud configures the ceiling **per CU class** from the class memory
  budget, so managed users get the loadability guarantee without configuration.

---

## 11. Risks and Mitigations

1. **Memory amplification (highest).** Counting only the main column inflates
   actual bytes per segment by `EstimateSizePerRecord / mainIndexPerRecord`
   (e.g. dim=8 FloatVector + 1KB scalars ⇒ ~30×). Growing segments, DataNode
   buffers, index build, query load, and storage all consume whole-row bytes.
   Mitigations: default-off; when enabled, the **hard ceiling** `maxFullSegmentSize`
   (§7, I4) caps the *final* segment in absolute whole-row bytes so it stays
   QueryNode-loadable no matter the ratio — including the large-scalar-index case
   where the scalar field must be fully loaded for index build (§8) — but note it
   is **`-1` by default**, so without it the amplification is unbounded on extreme
   schemas (mitigated by the quality guard below); keep flush HWM/LWM and
   `sealByTotalGrowingSegmentsSize` on whole-row (#2, §5); add a **quality guard**
   — reject/warn enabling for a collection whose `wholeRow / mainIndex` ratio
   exceeds a new threshold (e.g. `dataCoord.segment.maxRowAmplification`, default
   20), falling back to whole-row for that collection; verify with the e2e memory
   test in §13. Note the ceiling bounds the *sealed* segment, not the growing
   segment's transient memory — the growing-phase bound stays the flush HWM/LWM
   (whole-row).
2. **Compaction never converges** if #6/#7/#9 stay on whole-row (I3). Must be
   shipped together; a follow-up slice may ship compaction wiring second only if
   the metric is temporarily treated as whole-row there.
3. **Estimate vs actual (narrowed).** Where data is available the metric uses
   **actual** vector-column bytes (write path, per-field binlog in compaction);
   the schema estimate survives only in schema-only contexts — legacy DataCoord
   row cap, QueryNode `MaxIndexRowCount` (a build threshold, not a hard cap),
   import until the preimport extension, and cipher bodies on the write path.
   Sparse/`ArrayOfVector`/multi-vector divergence is confined to those contexts
   (§4). When the measured main column differs from the schema-widest (e.g. a
   narrow `ArrayOfVector` that actually dominates), only the measured contexts
   reflect it — the schema-only contexts keep the conservative schema-widest cap.
4. **Import divergence** (§6.5 Option A): imported segments have fewer rows per
   byte; safe but visible in segment count. Optional alignment in Option B.
5. **Mixed-version enablement** (§10.2) violates I2. Gate on a minimum version.
   `maxFullSegmentSize` follows the same gate: an old StreamingNode that does not
   know the ceiling can publish a segment above it, silently breaking I4.
6. **Wide behavior surface.** Segment counts, index-build batching, compaction
   frequency, and query distribution all change. Requires the G2-style
   failure-mode pass (memory ceiling, no-index collections, sparse-only
   collections, multi-vector, import, force-merge) before claiming the benefit.
7. `sizeMetric` and `maxFullSegmentSize` are non-refreshable; changing them requires
   a rolling restart (consistent with `SegmentMaxSize`).

---

## 12. Implementation Plan

### Phase 0: Helpers and Config

1. Add `dataCoord.segment.sizeMetric` (string enum) and `maxFullSegmentSize`
   (default `-1`) to `component_param.go`.
2. Add `EstimateMainIndexSizePerRecord`, `EstimateVectorColumnSize`,
   `SelectMainIndexField` to `pkg/util/typeutil/schema.go` with unit tests.

### Phase 1: StreamingNode Write Path

1. Split `SealSize` (dominant vector column, measured or schema fallback) from
   `BinarySize` (whole-row, unchanged).
2. Dual seal condition (budget OR ceiling).
3. Make the segment limitation policy schema-aware.

### Phase 2: DataCoord / QueryNode Row Caps

1. `calBySchemaPolicy` and `ComposeIndexMeta` use the `min(budget, ceiling)`
   row-cap formula with the schema fallback.

### Phase 3: Compaction

1. `getExpectedSegmentSize` comparisons, knapsack sizing, clustering
   `estimateRowsBySegmentSize`, and force-merge use the largest vector field's
   per-field binlog bytes (`SelectMainIndexField` + `getFieldBinlogSize`),
   capped by the ceiling.
2. Align clustering analyze denominator semantics.

### Phase 4: Hard Ceiling Enforcement

1. Wire `maxFullSegmentSize` into seal, compaction output, and import split
   (`min(segmentMaxSize, maxFullSegmentSize)`).

### Phase 5: Import Alignment (Option B, follow-up)

1. Extend `ImportFileStats` with per-field bytes; accumulate during DataNode
   pre-import; split by the actual dominant vector column.

### Phase 6: Docs and Cloud Config

1. Open-source feature guide (§10.3).
2. Zilliz Cloud per-CU-class ceiling derivation.

---

## 13. Testing Strategy

### 13.1 Unit Tests

| Component | Test Scope |
|-----------|------------|
| typeutil | `EstimateMainIndexSizePerRecord` (schema fallback) and `EstimateVectorColumnSize` (actual: dense `len×elem`, sparse `sum contents`, `ArrayOfVector` `sum inner vectors`) for all vector types, no-vector, sparse-only, multi-vector, `BinaryVector` rounding; `SelectMainIndexField` picks the largest given size and returns `false` with no vector fields |
| streamingnode | `SealSize` measured vs fallback; dual seal condition; cipher fallback; flush HWM/LWM still whole-row |

### 13.2 Selection and Metric Tests

- **Measured vs schema selection (§4):** a schema with a narrow `ArrayOfVector`
  (small dim, many inner vectors per row) next to a wider fixed-dim FloatVector —
  measured selection picks the `ArrayOfVector` as main and the segment seals on
  its actual bytes; the schema fallback would have picked the FloatVector.
  Fixed-dim multi-vector: measured selection equals the schema-widest (assert
  identical `rowCap`). Sparse-only: falls back to whole-row in both paths.
- Write path: with `sizeMetric="mainIndex"`, a segment seals when the largest
  vector column's bytes cross `maxSize×proportion×jitter`, regardless of scalar
  payload; with `"wholeRow"` behavior is byte-identical to today. Flush HWM/LWM
  still track whole-row bytes.
- Consistency (I2): same schema yields the same effective row cap from #1, #3,
  #5 in isolation tests.

### 13.3 Ceiling and Loadability Tests

- **Ceiling (I4):** with `maxFullSegmentSize > 0` and an extreme schema (dim=8
  FloatVector + 1KB scalars) where the `mainIndex` budget's implied whole-row
  size is far above the ceiling, the segment seals on the ceiling and its actual
  whole-row bytes never exceed it; the same holds for compaction output and
  import splits. A QueryNode with a memory budget below one ceiling-sized segment
  can still load a sealed segment (loadability e2e). With the default
  `maxFullSegmentSize=-1`, the same schema seals only on the `mainIndex` budget
  (unbounded whole-row), and the quality guard (§11 #1) is what keeps pathology
  out.
- **Large scalar index (I4, §8):** a small vector + wide indexed `Text`
  (full-text/BM25) or `JSON`/`Array` inverted index under `mainIndex`. With the
  ceiling enabled, the segment seals before the scalar field grows past the
  ceiling, and IndexNode can build the index from the sealed segment without
  exceeding its memory budget; with `-1` the scalar index build may need to
  materialize the full field of a much larger segment (documented risk).
- Compaction (I3): mix, clustering, force-merge converge on the largest vector
  field's bytes (measured per-field binlog), capped by the ceiling when enabled;
  a handcrafted "large scalar / small vector" segment set merges as expected and
  never emits an output above the ceiling when `maxFullSegmentSize > 0`.
- Memory e2e: heavy-scalar collection under `mainIndex` stays under the
  amplification ceiling and — when enabled — the hard ceiling; with
  `maxFullSegmentSize=-1` the whole-row size is unbounded by the ceiling and only
  the quality guard bounds it. Flush HWM does not under-count.

### 13.4 Import / Upgrade Tests

- Import: Option A keeps current splitting; Option B splits by actual dominant
  vector field bytes after the preimport per-field-stat extension
  (`SelectMainIndexField` + `EstimateVectorColumnSize`); both honor
  `min(segmentMaxSize, maxFullSegmentSize)`. Until Option B lands, import reports
  the known schema-fallback divergence.
- Analyze: `numClusters` unchanged when clustering key == main column.
- Upgrade: mixed-version cluster with metric on is refused; old segments keep
  persisted `MaxBinarySize` after restart; setting a positive ceiling under a
  mixed version is refused (I4).
- Build, DataCoord/StreamingNode/QueryNode tests, static checks pass.

---

## 14. Open Questions

1. Should the quality guard (`maxRowAmplification`) be a hard reject at
   collection-create / index-create time, or a warning with whole-row fallback?
2. When the measured dominant vector column changes between batches of the same
   segment (multi-vector workloads with unstable dominant column), is the
   per-message `SealSize = max over vector columns` accumulation the right model,
   or should the segment pin one main column at creation?
3. For the cipher write path, is the plaintext body available in the interceptor
   today, or must the cipher case always use the schema fallback?

---

## 15. References

- `pkg/util/paramtable/component_param.go:5677-5711` — `SegmentMaxSize` /
  `sealProportion` / jitter; new `sizeMetric` / `maxFullSegmentSize` alongside.
- `internal/streamingnode/server/wal/interceptors/shard/shards/segment_limitation_policy.go:78-90` —
  write-path budget generation.
- `internal/streamingnode/server/wal/interceptors/shard/utils/stats.go:108-141` —
  `AllocRows` / `MaxBinarySize` enforcement.
- `internal/proxy/task_insert_streaming.go:350` — proxy `BinarySize = 0`; the
  interceptor substitutes `msg.EstimateSize()` (`shard_interceptor.go:198-202`).
- `internal/datacoord/segment_allocation_policy.go:39-53` — `calBySchemaPolicy`.
- `internal/querynodev2/segments/index_meta.go:44-52` — segcore `MaxIndexRowCount`.
- `internal/core/src/segcore/IndexConfigGenerator.cpp:99-104` — `MaxIndexRowCount`
  is an index-build threshold (`max_index_row_count_ × build_ratio`), not a hard cap.
- `internal/datacoord/compaction_trigger_v2.go:872-880` — `getExpectedSegmentSize`.
- `internal/datacoord/compaction_policy_clustering.go:242-275` — `estimateRowsBySegmentSize`.
- `internal/datacoord/task_analyze.go:207-208` — vector-column cluster sizing.
- `internal/datacoord/import_util.go:140-146,191-205` — import budget & split.
- `pkg/proto/datapb/data_coord.pb.go:8385-8450` — `ImportFileStats` /
  `PartitionImportStats` carry only whole-file aggregates; the per-field
  extension for import (Option B) lives here.
- `internal/datacoord/segment_info.go:519-545` — `getSegmentSize` / `getFieldBinlogSize`.
- `internal/datacoord/util.go:470-557` — `calculateIndexTaskSlot` /
  `estimateFMIndexBuildPeakBytes`: the existing field-binlog-based scalar-index
  memory model that the compaction measurement (§6.3) mirrors.
- `pkg/util/typeutil/schema.go:134-243,642-655` — `EstimateSizePerRecord` /
  `VectorTypeSize`; `schema.go:289-391` — `calcVectorSize` / `EstimateEntitySize`,
  the existing per-row actual-size machinery reused by `EstimateVectorColumnSize`.
- `internal/querynodev2/segments/segment_loader.go:1819-1896, 1770-1778, 1874` —
  no per-segment load cap; load memory factors and threshold used by the
  `maxFullSegmentSize` sizing guidance.
- Open-source docs to ship: feature guide for `sizeMetric="mainIndex"` and
  `maxFullSegmentSize` sizing from the QueryNode memory budget (§7); Zilliz Cloud
  sets the ceiling per CU class.
