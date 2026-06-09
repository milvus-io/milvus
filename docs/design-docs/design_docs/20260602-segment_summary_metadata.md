# Segment Summary Metadata in SegmentInfo

## Background

Milvus V3 storage format uses LOON manifests stored in object storage (S3/MinIO) to manage segment data. Each segment's manifest is a versioned transaction log that tracks insert binlogs, stats (bloom filters, BM25, text index, JSON key stats), and delta logs. The manifest path is stored as a JSON string in the `manifest_path` field of SegmentInfo.

In the V2 storage format, segment metadata — including individual binlog paths, stats paths, and delta log paths — is persisted as separate KV entries in etcd under prefixes like `datacoord-meta/binlog/`, `datacoord-meta/statslog/`, `datacoord-meta/deltalog/`, etc. Each segment with N fields produces roughly 3N+1 KV entries. DataCoord loads these on startup and iterates over the FieldBinlog arrays to compute aggregate metrics such as total segment size, row counts, and deletion counts for scheduling decisions (compaction triggers, load balancing, garbage collection thresholds).

V3 segments currently follow the same pattern: even though the authoritative data lives in manifests, the system still writes binlog/stats/delta metadata into etcd KVs. This is because DataCoord needs fast access to aggregate metrics without reading manifests from object storage on every decision.

## Problem

There is a fundamental mismatch between what DataCoord needs and what is stored.

DataCoord only needs aggregate metrics for decision-making. Compaction triggers check total segment size, deletion ratio, and file counts. Load balancing checks segment sizes. None of these operations need individual binlog paths or LogIDs. Yet today, the only way to obtain these aggregates is to iterate over FieldBinlog arrays — arrays that exist in etcd solely to serve this iteration.

For V3 segments, this creates redundancy on two levels. First, the authoritative data already lives in manifests, so the binlog KV entries in etcd are duplicates. Second, the aggregates that DataCoord actually needs (total size, total rows, deletion count) are never stored directly — they are recomputed on every access by scanning the arrays.

The current implementation also scatters the aggregation logic across multiple functions (`getSegmentSize`, `getDeltaCount`, `GetResidualSegmentSize`, `GetBinlogCount`, `GetBinlogEntriesNum`, `GetBinlogSizeAsBytes`, etc.) that each independently iterate binlog arrays. These ~45 call sites all perform the same pattern: loop over FieldBinlog arrays, sum a field. The in-memory atomic caches (`size`, `deltaRowcount`) partially address this for flushed segments, but they are not persisted and must be recomputed after every DataCoord restart.

For V3 segments specifically, the ~30 binlog KV entries per segment add write amplification on flush, increase etcd storage, and slow metadata loading — all for data that no component in the V3 path actually reads from etcd.

## Requirements

1. DataCoord must be able to make scheduling decisions (compaction, load balancing, GC) using persisted aggregate metadata, without reading manifests from object storage or iterating binlog arrays.

2. The aggregate metadata must survive DataCoord restarts.

3. V3 segments should not need to write per-field binlog KV entries to etcd. The manifest is the source of truth for paths; only aggregate summaries need to be in etcd.

4. The solution must be backward-compatible. Existing V2 segments and existing V3 segments (that currently have binlog KVs) must continue to work.

5. The aggregate values must be kept consistent with the underlying data. For V3 segments, they are extracted alongside manifest generation. For V2 segments, they are computed from binlog arrays.

## Design

### Statistics proto on SegmentInfo

A `Statistics` message is added to `data_coord.proto` and embedded in `SegmentInfo.stats`. Read sites (compaction trigger, GC, scheduler, allocator, server metrics) consume these scalar fields directly with no branching on storage version. The fields, all per-segment cumulative:

- `insert_binlog_size` — sum of `MemorySize` across every insert FieldBinlog.
- `stats_binlog_size` — bloom-filter + BM25 blob footprint (manifest-side for V3, statslog-array-side for V2).
- `delta_binlog_size` — sum of `MemorySize` across every delta FieldBinlog.
- `delete_num_rows` — sum of `EntriesNum` across every delta FieldBinlog. The authoritative segment-wide delete count.
- `insert_binlog_count` — number of insert binlog files across all fields.
- `delta_binlog_count` — number of delta binlog files.
- `timestamp_from` / `timestamp_to` — segment-wide min/max of insert row timestamps (binlog-level, not row-level).
- `delta_timestamp_from` / `delta_timestamp_to` — segment-wide min/max of delete row timestamps.
- `timestamp_quantiles` — five values (20/40/60/80/100 percentile) approximated from per-binlog `TimestampTo` weighted by `EntriesNum`. Consumed by the compaction trigger's quantile-based expiry check.
- `null_counts` — per-field cumulative null-row count (only for nullable fields). Consumed by the index task's effective-row-count derivation for nullable vector fields.

Insert row count is intentionally omitted — `SegmentInfo.num_of_rows` is the authoritative segment row count, and summing per-binlog `EntriesNum` across every field would over-count by the field/group count. Field number 7 of the `Statistics` message is reserved for a previously drafted but dropped `stats_memory_size` field.

### NullCounts presence contract

`Statistics.null_counts` contains an entry — zero included — for every field
physically present in the segment's data:

- Flush: V2/V3 pack writers stamp per-binlog `field_null_counts` on the
  insert FieldBinlogs they ship. Per sync the writer hands those binlogs to
  the growing segment's cumulative `StatisticsCollector`, which digests them
  (via the `MergeStatistics` metacache action → `Digest`), seeding a
  zero-included `null_counts` entry for every written column. At flush the
  collector publishes the whole `Statistics` (`null_counts` included) and
  DataCoord stores it wholesale — no segment-wide recompute on the receiver.
  The presence guarantee therefore lives in the writer's output: every
  column it writes seeds a `null_counts` key.
- Compaction: `buildCompactionOutputStats` derives NullCounts from the
  compaction writer's own output binlogs and DataCoord copies the result
  verbatim onto the new SegmentInfo.
- Import: importv2 produces binlogs through the same syncmgr pack writers
  as flush (so they carry `child_fields` / `field_null_counts`), and
  DataCoord's import completion rebuilds Stats from the shipped arrays via
  `UpdateBinlogsOperator` → `BuildStatsFromFieldBinlogs`, so imported
  segments inherit the completion rule. L0 imports carry only deltas and
  need no presence entries.
- Pre-existing segments: `BuildStatsFromFieldBinlogs` (migration back-fill)
  applies the completion rule — every member field (child_fields, else
  field_id) of every non-empty insert FieldBinlog gets an entry, defaulting
  to zero when legacy binlogs carry no `field_null_counts` metadata
  (storage V1, pre-#46903). Delta binlogs never mark presence.

A missing key therefore means the field has no data in the segment (added
to the schema after the segment was flushed); consumers must treat every
row as null for it. `indexBuildTask.CreateTaskOnWorker` relies on this to
skip index builds on added nullable vector fields.

Known limitations (accepted):
- Scalar members of pre-ChildFields packed (early 2.6) binlogs cannot be
  recovered by the completion rule (their membership was never persisted);
  the guarantee is full for vector/text fields on all formats and
  self-heals for scalars on compaction.
- A segment whose growing life spans an add-field DDL carries the added
  field's null counts only for post-add-field rows; `effectiveRows` in the
  index task over-estimates by the pre-add row count until the first
  compaction rewrites the segment. In the worst case (all post-add rows
  null) an index build may be dispatched for a column with zero valid
  vectors — same exposure class as pre-#46903 behavior, window closes at
  first compaction.
- OPEN: snapshot restore (copy-segment) degrades the contract for packed
  (V2/V3) segments. The snapshot Avro schema (`AvroFieldBinlog` /
  `AvroBinlog` in `internal/datacoord/snapshot.go`) persists neither
  `child_fields` nor `field_null_counts`, so the restored binlogs reach
  `UpdateBinlogsOperator` → `BuildStatsFromFieldBinlogs` stripped of both.
  The completion rule then falls back to the column-group ID: vector/text
  fields (own group, group ID == field ID) keep their presence entries, but
  nullable scalar members of the short-column group get no entry — an
  invariant violation for those scalars — and all null counts reset to
  zero. Same exposure class as the pre-ChildFields limitation above
  (vector consumers safe, self-heals on first compaction of the restored
  segment), but it affects current-format segments through any snapshot
  round-trip until the Avro schema carries the two fields.

### Wire format

Two requests already carry FieldBinlog arrays between the data path and DataCoord; both gain a `Statistics` field so the receiver does not have to recompute from arrays where the writer already has authoritative values.

`SaveBinlogPathsRequest.stats` — populated by every flush (V2 and V3) with the **complete cumulative** `Statistics` for the segment. The writer no longer builds a per-flush object: a `StatisticsCollector` lives on the growing segment's metacache `SegmentInfo` (beside the bloom filter `bfs` and `bm25stats`, shared by pointer across `Clone`, guarded by its own mutex). Each sync hands its FieldBinlog metadata plus the batch's timestamp range and stats-blob footprint to the collector, which `Digest`s them directly into its cumulative memory (`MergeStatistics`, dispatched immediately for V2 and deferred into `pendingMetaCacheActions` until manifest commit for V3). At flush the writer publishes the whole object: `seg.Statistics().Publish()` — a plain read of the accumulated state, no scaling. `stats_binlog_size` is carried in the same object (V2 sums the emitted statslog/bm25 FieldBinlog sizes per sync, V3 feeds the manifest blob counter) — there is no separate `stats_binlog_size` thread and no V2/V3 receiver branch.

The receiver (`UpdateSegmentStats`) stores the shipped `Statistics` wholesale onto `SegmentInfo.stats`: no per-field recompute, no `stats_binlog_size` special case. Array derivation (`BuildStatsFromFieldBinlogs`) survives on the receiver only as the **nil-stats fallback** for storage-V1 / pre-Statistics datanodes during a rolling upgrade (a flush that ships no `Statistics`).

`CompactionSegment.stats` — populated by every compactor (mix, sort, schema-bump, L0) at write completion and copied verbatim onto the new SegmentInfo by DataCoord. Compaction outputs are single-shot — the compactor sees every row of the output once, so the writer-side Statistics IS the segment-wide Statistics. There is no per-flush vs cumulative gap to bridge. Insert and delta aggregates come from the FieldBinlog arrays the compactor ships. The stats blob footprint (bloom-filter + BM25) comes from a counter the `BinlogRecordWriter` accumulates inside its stats-writing path — `writeStats` for V2 (sums each emitted statslog/bm25 FieldBinlog's MemorySize), `appendV3Stats` for V3 (sums each raw blob length committed into the manifest). The counter is exposed via `BinlogRecordWriter.GetStatsBlobSize()` and threaded into `CompactionSegment.stats.stats_binlog_size`. This explicit channel sidesteps the trap that V3 writers leave the statslog and bm25 FieldBinlogs nil because stats live in the manifest, so an array-based recompute would silently report zero.

### Stats lifecycle (live-segment path)

A growing segment's `Statistics` follows one uniform lifecycle, identical for V2 and V3:

- **Collected cumulatively** on the growing segment. The `StatisticsCollector` (in `internal/storage/statistics.go`) `Digest`s each sync's writes — insert/stats/delta sizes and counts, null counts (zero-included per written column), a `{tsTo, rows}` quantile entry, and the delete/timestamp ranges. The collector hangs off the metacache `SegmentInfo.stats` (`SegmentStats` wrapper, `internal/flushcommon/metacache/segment_stats.go`) and is fed via the `MergeStatistics` action through the same copy-on-write `UpdateSegments` path as the bloom filter.

- **Published whole at each flush** via `seg.Statistics().Publish()` — a plain read of the accumulated `Statistics`, no scaling. The collector holds the exact segment-wide cumulative because it has digested every sync, so the published value is exact. (V3 defers its `MergeStatistics` to manifest-commit, so its publish folds the current sync into a `Clone` of the collector to include it without mutating the shared copy before commit.)

- **Stored wholesale** by `UpdateSegmentStats`: `segment.Stats = requestStats`, no per-field recompute, no `stats_binlog_size` special case. The shipped value is the absolute cumulative `Statistics`, so a wholesale store is idempotent under `SaveBinlogPaths` retries.

- **Restored on recovery, not persisted as collector state.** The collector lives in datanode memory, but its *output* — the cumulative `Statistics` — is persisted on `SegmentInfo` in etcd by every flush. On datanode restart `NewMetaCache` reseeds each recovered growing segment's collector from that persisted `Stats` via `NewSegmentStatsFromStats(seg.GetStats(), seg.GetNumOfRows())`, so the segment resumes accumulating exactly where it left off — no estimation, no manifest read (the V3 binlog arrays are not in etcd, but `Stats` always is). The persisted `timestamp_quantiles` are rebuilt as synthetic per-bucket entries, so quantiles round-trip and keep accumulating. Sealed segments keep their flush-time `Stats` frozen.

`BuildStatsFromFieldBinlogs` (array derivation) survives **only** outside this live flush path: the nil-stats receiver fallback (storage V1 / pre-Statistics datanodes), the lazy `NewSegmentInfo` legacy-migration back-fill, and the one-shot producers below.

### Non-flush producers (exempt from the collector)

Three producers populate `Statistics` without a growing-segment collector, and are unaffected by the live-flush redesign:

- **Compaction** (`internal/datanode/compactor/stats.go buildCompactionOutputStats`): a compaction output is a one-shot full rewrite — the compactor materializes every output row exactly once, so the writer-side `Statistics` already IS the segment-wide value. It builds the object from the output's own insert/delta FieldBinlogs via the untouched `BuildStatsFromFieldBinlogs` and sets `stats_binlog_size` from the writer's `GetStatsBlobSize()` counter; DataCoord copies it verbatim onto `CompactionSegment.Stats`. No accumulating growing segment, so no collector.
- **Import** (`internal/datacoord/import_task_import.go` → `UpdateBinlogsOperator` → `computeStatsFromBinlogs`): imported segments are initialized one-shot from the supplied FieldBinlog arrays, array-derived through `BuildStatsFromFieldBinlogs` (completion rule covers `null_counts` presence). Unaffected.
- **L0 delta updates** (`meta.go addDeltalogsToSegment`): a partial update on a sealed segment that refreshes only the three delta fields (`delta_binlog_size`, `delete_num_rows`, `delta_binlog_count`) and explicitly preserves the rest of `Stats` (including `null_counts`). It is not a stats publication and calls no removed method. Unaffected.

### Compaction-output path

Compactors construct `CompactionSegment.stats` via a shared `buildCompactionOutputStats(insertLogs, deltalogs, statsBlobSize)` helper. Insert and delta aggregates come from `storage.BuildStatsFromFieldBinlogs(insertLogs, nil, deltalogs)`; `stats_binlog_size` is supplied explicitly by the caller from the writer's tracked counter. The receiver in DataCoord copies `resultSegment.GetStats()` directly onto `SegmentInfo.stats` at the compaction-completion sites in `meta.go` and does not call `computeStatsFromBinlogs` for compaction outputs.

Sites and stats blob size sources:
- Mix compaction (via `MultiSegmentWriter.closeWriter`): `writer.GetStatsBlobSize()` where `writer` is the underlying `BinlogRecordWriter`.
- Sort compaction (`sortCompactionTask.compactInner`): `srw.GetStatsBlobSize()` from the sort's `BinlogRecordWriter`.
- Schema-bump full rewrite: `writer.GetStatsBlobSize()` from the V3 writer.
- Schema-bump partial writer: a `statsBlobSize` field accumulated on the `bumpSchemaVersionWriterResult` by `addV3Stats`.
- L0 compaction: 0 (L0 outputs carry only deltas, no stats blobs).
- Schema-bump-only path: leaves `stats` nil — the receiver Clones the input segment and only mutates `SchemaVersion`/`Binlogs`/`StorageVersion`/`ManifestPath`, so the existing `oldSegment.Stats` is preserved as-is.
- Empty placeholder output from mix compaction (no rows survive): leaves `stats` nil; the receiver's `NewSegmentInfo` back-fills an all-zero Statistics from the empty arrays.

### V3 etcd KV write skip

`kv_catalog.go AlterSegments` checks `isV3Segment(segment)` (i.e., `manifest_path != ""`) and skips the per-FieldBinlog KV writes for V3 segments. The segment KV itself still carries the SegmentInfo proto, which now carries `Statistics`. New V3 segments persist exactly 1 etcd KV per segment instead of ~3N+1; aggregate scheduling reads consult `Statistics` directly. Existing V3 segments persisted before this change still have their per-FieldBinlog KVs in etcd; `applyBinlogInfo` continues to load them so the in-memory FieldBinlog arrays are populated and `NewSegmentInfo` can back-fill Statistics from them. Those orphaned KVs are not pre-emptively cleaned up; they are dropped on segment delete.

### Migration

Existing segments persisted without `Statistics` (nil after unmarshal) get back-filled on first load: `NewSegmentInfo` at `reloadFromKV` time computes Statistics from whatever FieldBinlog arrays the segment has and writes it to the in-memory `SegmentInfo.stats`. For V2 segments this is the full set of arrays from etcd; for pre-existing V3 segments it's whatever etcd KVs are present. **Migration is lazy and opportunistic**: the back-filled Statistics lives only in memory until the next mutating operator (flush, compaction, status change) triggers an `AlterSegments` that persists the SegmentInfo proto. A segment that never mutates again will be re-derived on every restart. This is acceptable because the back-fill is cheap relative to the work the segment is already doing on those mutating operators, and the data is identical across restarts; the alternative (an explicit one-time persist pass on startup) was rejected as additional complexity without correctness benefit.

`SegmentInfo.EnsureStats()` is the read-side safety net for legacy in-memory construction paths: it returns the persisted `s.Stats` if non-nil and a transient array-derived value otherwise. It does NOT write back — concurrent RLock readers would race. All operator-side writes go through `NewSegmentInfo` (eager init at construction) or `UpdateSegmentStats` (under the `meta.segMu.Lock` write lock).

### Receiver-side V2 / V3 gate

The doc-level "read with no version branching" guarantee applies to the aggregate-query read sites. The flush receiver (`UpdateSegmentStats`) no longer branches on V2 vs V3 at all: both ship the complete cumulative `Statistics` (including `stats_binlog_size`, which V2 sums from emitted statslog/bm25 sizes per sync and V3 feeds from the manifest blob counter), and the receiver stores it wholesale. The only remaining receiver branch is the nil-stats fallback (request ships no `Statistics`), which array-derives via `BuildStatsFromFieldBinlogs` for storage-V1 / pre-Statistics datanodes during a rolling upgrade.

### Quantile under-estimate

The compaction trigger's quantile-based expiry check (`ShouldDoSingleCompaction`) walks the five `timestamp_quantiles` buckets to derive an `expired_fraction`. The fraction is then multiplied by `insert_binlog_size` to estimate expired bytes. Because the byte conversion assumes uniform per-row size and binlog sizes vary, this product can over-estimate expired bytes and over-trigger compaction. To avoid that, the trigger reports `percentiles[qualifying_idx-1]` instead of `percentiles[qualifying_idx]` — one bucket down. This guarantees the byte estimate is a strict lower bound across realistic row-size distributions; some near-threshold segments compact one cycle later, which is preferable to triggering on segments whose actual expired footprint is below threshold.

## Comparison with Current Implementation

In the current implementation, aggregate metrics are derived by iterating FieldBinlog arrays at query time, with partial in-memory caching via atomic fields. The proposed change extracts these metrics once at the point of data generation and persists them as first-class fields. This eliminates the iteration, makes the aggregates available immediately after restart, and for V3 segments removes the need to store ~30 redundant binlog KV entries per segment in etcd.

The FieldBinlog arrays remain in the SegmentInfo proto for V2 segments and for pass-through use cases (compaction plans sent to DataNode, recovery info sent to QueryNode, etc.). They are not removed — the summary fields supplement them for aggregate queries and replace them as the persistence mechanism for V3 segments.
