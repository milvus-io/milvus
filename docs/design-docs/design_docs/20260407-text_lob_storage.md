# MEP: TEXT Storage with LOB and Growing-Segment Flush

- **Created:** 2026-04-07
- **Author(s):** @zhagnlu
- **Status:** Draft
- **Component:** Storage, QueryNode, DataCoord, DataNode
- **Related Issues:** milvus-io/milvus#48783
- **Related PR:** milvus-io/milvus#47567
- **Released:** 3.0.0

## Summary

Introduce a new end-to-end storage path for the `TEXT` scalar type in Milvus
3.0. The design has four pillars:

1. **Unified V3 Manifest model.** TEXT data and metadata flow through the
   Storage V3 manifest / column-group machinery already in `milvus-storage`.
2. **C++-native read and write.** All TEXT IO is done inside the C++
   `milvus-storage` library; the Go layer is a thin coordination wrapper. No
   per-row data crosses the CGO boundary.
3. **Growing-segment direct flush.** TEXT collections do *not* use the
   StreamingNode write-buffer flush path. The QueryNode's Growing Segment
   persists its own data, avoiding storing TEXT in two places.
4. **Vortex on-disk format for LOB payloads.** Large TEXT payloads are stored
   in Vortex files, replacing the Parquet path used by the previous LOB
   experiment. Vortex gives better compression for long strings and native,
   index-based random access.

The on-disk layout is **hybrid**: a per-segment *reference binlog* lives
inside the segment column-group manifest, while the actual LOB Vortex files
live at the **partition** level so that multiple segments can share them and
compaction can reuse files without copying bytes.

## Motivation

Milvus 3.0 promotes `TEXT` to a first-class scalar type with full-text
search. Storing TEXT inline in the existing columnar files conflates two very
different access patterns (long tail of small strings + a few very large
documents) and causes:

- **Memory pressure** in the growing segment from megabyte-class payloads.
- **Compaction write amplification** — the entire TEXT column is rewritten
  whenever any row in a segment changes.
- **Double storage** of TEXT payloads if the StreamingNode write buffer is
  used: the QueryNode growing segment already holds the rows for query,
  and the StreamingNode would hold them again purely for flush. For
  document-sized payloads this doubles RAM and adds WAL bandwidth.
- **Go/CGO overhead** in the previous LOB prototype, which kept the LOB
  manager and writer in Go and called into a Parquet writer through CGO
  per row. The boundary cost dominated for large payloads.
- **Parquet's weaker random-access story** for opaque large strings, where
  Vortex's split index gives O(log N) row lookups natively.

The design below addresses all four problems together. They are tightly
coupled: moving TEXT IO into C++ is what makes growing-segment direct flush
practical, and the partition-level Vortex layout is what makes compaction
reuse possible without rewriting payloads.

## Public Interfaces

No SDK / proto / query-syntax change is exposed to end users. The change
surface is:

### New configuration

| Key | Default | Refresh | Description |
|-----|---------|---------|-------------|
| `dataNode.text.inlineThreshold` | `65536` B | dynamic | TEXT values strictly smaller than this are encoded inline in the reference binlog; larger values go to a LOB Vortex file. |
| `dataNode.text.maxLobFileBytes` | `67108864` B (64 MiB) | dynamic | Maximum size of a single LOB Vortex file. |
| `dataNode.text.flushThresholdBytes` | `16777216` B (16 MiB) | dynamic | Spillover flush trigger for the growing segment LOB writer. |
| `dataNode.compaction.lobHoleRatioThreshold` | `0.3` | dynamic | If overall hole ratio across source segments < threshold, compaction runs in REUSE_ALL mode; otherwise REWRITE_ALL. |
| `dataCoord.gc.lob.enabled` | `true` | static | Enable the dedicated LOB GC. |
| `dataCoord.gc.lob.safetyWindow` | `3600` s | dynamic | Minimum file age before LOB GC may delete an orphan. |
| `dataCoord.gc.lob.checkInterval` | `1800` s | dynamic | LOB GC scan interval. |

### New milvus-storage FFI surface

Two FFI surfaces are used. For **compaction and import**, the Go side
configures TEXT columns via `TextColumnConfig { FieldID, LobBasePath,
InlineThreshold, MaxLobFileBytes, FlushThresholdBytes, RewriteMode }` and
passes them to the `loon_segment_writer_*` FFI (`loon_segment_writer_new`,
`loon_segment_writer_write`, `loon_segment_writer_close`). For **growing-
segment flush**, the Go side calls `FlushGrowingSegmentData` in segcore,
which extracts unflushed rows and writes them through the milvus-storage
writer entirely in C++ — no per-row data crosses the CGO boundary. Inside
the C++ writer, each row is classified by `InlineThreshold`, LOB-sized
values are appended to Vortex files via `VortexFileWriter`, and references
are encoded into the reference binlog. The C struct exposed to segcore is
`CFlushConfig`; the C struct exposed to the segment-writer FFI is
`LoonLobColumnConfig`.

### Reused interfaces (no change)

- The V3 column-group manifest / `Transaction<ColumnGroups>` API in
  `milvus-storage`.
- `flushcommon` `SyncPolicy`, `SyncManager`, `MetaWriter`,
  `ChannelCheckpointUpdater`.
- `DataCoord.SaveBinlogPaths`.

## Design Details

### Architecture overview

```
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Control plane                                                        │
 │  ┌─────────────────────────┐    ┌────────────────────────────────┐   │
 │  │ DataCoord               │    │ DataCoord LOB GC               │   │
 │  │  - SegmentInfo          │◀──▶│  - scans reference binlogs     │   │
 │  │  - SaveBinlogPaths      │    │  - safety window               │   │
 │  └─────────────────────────┘    └────────────────────────────────┘   │
 └──────────────────────────────────────────────────────────────────────┘
 ┌──────────────────────────────────────────────────────────────────────┐
 │ Data plane                                                           │
 │                                                                      │
 │  ┌──────────────────────────────────────────────────────────────┐    │
 │  │ QueryNode                                                    │    │
 │  │  pipeline → delegator → growing segment (segcore, C++)       │    │
 │  │                                                              │    │
 │  │  GrowingFlushManager (Go, new)                               │    │
 │  │   reuses flushcommon SyncPolicy / SyncManager / MetaWriter   │    │
 │  │   uses CheckpointTracker (Go, new)                           │    │
 │  │   calls segcore FlushGrowingSegmentData (C FFI)              │    │
 │  └──────────────────────────────────────────────────────────────┘    │
 │                                                                      │
 │  ┌──────────────────────────┐  ┌────────────────────────────────┐    │
 │  │ DataNode compactor       │  │ DataNode importer              │    │
 │  │  text-aware:             │  │  csv / json / parquet          │    │
 │  │   REUSE_ALL  (file copy) │  │  routes large TEXT through     │    │
 │  │   REWRITE_ALL            │  │  loon_segment_writer_* FFI     │    │
 │  │   SKIP (L0 deletes)      │  │                                │    │
 │  └──────────────────────────┘  └────────────────────────────────┘    │
 │            │                                  │                      │
 │            ▼                                  ▼                      │
 │  ┌──────────────────────────────────────────────────────────────┐    │
 │  │ C++ milvus-storage                                           │    │
 │  │  Transaction<ColumnGroups>     (segment-level reference)     │    │
 │  │  PackedWriter / Reader         (Parquet, reference binlog)   │    │
 │  │  VortexFileWriter              (LOB payloads)                │    │
 │  │  UUID file naming              (partition-level LOB files)    │    │
 │  └──────────────────────────────────────────────────────────────┘    │
 └──────────────────────────────────────────────────────────────────────┘
```

### On-disk model

The layout is **hybrid**: reference data is per-segment, LOB payload data is
per-partition.

```
{root}/insert_log/{collection_id}/{partition_id}/
│
├── {segment_id}/                       # SEGMENT level
│   ├── _metadata/
│   │   └── manifest-{version}.avro     # column-group manifest (existing V3)
│   └── _data/
│       ├── cg0_xxx.parquet             # normal columns: pk, vector, scalar
│       └── cg1_yyy.parquet             # TEXT *reference binlog* (parquet)
│                                       #   each row = inline bytes OR
│                                       #              16-byte LOBReference
│
└── lobs/                               # PARTITION level (shared!)
    └── {field_id}/
        └── _data/
            ├── {file_id_1}.vx          # Vortex file, append-only
            ├── {file_id_2}.vx
            └── ...
```

Key consequences of this layout:

- **LOB files are NOT under the segment basePath.** Multiple segments can
  reference the same `{file_id}.vx`. Compaction reuse is therefore a pure
  metadata operation — the LOB bytes never move.
- **LOB files do not have their own manifest.** A `file_id` *is* the path:
  `{partition}/lobs/{field}/_data/{file_id}.vx`. Each writer generates a
  UUID for the `file_id`, so multiple writers can share the same partition
  without coordination through Go or etcd.
- **Reference binlog reuses the existing column-group manifest.** From the
  manifest's point of view, the TEXT field is just another column group of
  Parquet files; the only special thing is the row encoding inside.
- **Cross-segment LOB references are allowed**, but **cross-partition** is
  not — every reference is local to a partition, so segment / partition
  isolation is preserved.

### Data model

#### Reference encoding (inside the parquet reference binlog)

Every row of a TEXT column carries a 1-byte flag prefix:

```
0x00  inline:    [0x00] [text bytes ...]                     (variable length)
0x01  LOB ref:   [0x01] [pad x3] [file_id : uuid] [row_offset : int32]
                 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                 24 bytes total, 4-byte aligned

Byte layout of a LOB reference:
  Offset  Size  Field
  0       1     flag         = 0x01
  1-3     3     padding      (reserved, 0x00)
  4-19    16    file_id      binary UUID of the LOB Vortex file
  20-23   4     row_offset   int32, row index in the Vortex file
```

The `file_id` is a 16-byte binary UUID; the standard string form
(`xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx`) is used when building the
object-store path `{partition}/lobs/{field}/_data/{file_id}.vx`.

The `row_offset` is a **row index** inside the Vortex file, not a byte
offset. Vortex's split index resolves it in O(log N) where N is the number
of splits. Using row indices keeps the reference valid after Vortex
re-encodes / recompresses splits.

#### Invariants

1. A LOB Vortex file is **immutable** once closed.
2. A LOB file is reachable iff at least one live segment's reference binlog
   contains a LOB reference naming its `file_id`.
3. References never cross partition boundaries.

### Components

| Component | Layer | Responsibility |
|-----------|-------|----------------|
| `VortexFileWriter` | C++ milvus-storage | Native Vortex IO for LOB payloads. |
| `loon_segment_writer_*` | FFI (C → Go) | Segment-writer FFI surface used by compaction and import. Accepts `LoonLobColumnConfig` for TEXT columns. |
| `FlushGrowingSegmentData` | C FFI (segcore) | Extracts unflushed rows from a growing segment and writes them through the milvus-storage writer entirely in C++. Accepts `CFlushConfig`. |
| `GrowingFlushManager` | Go (`querynodev2/segments`) | Per-channel scheduler that drives growing-segment flush for TEXT collections. |
| `GrowingSegmentBuffer` | Go (`querynodev2/segments`) | Adapter that exposes a growing segment to the existing `SyncPolicy` interface (`IsFull`, `MinTimestamp`, `MemorySize`). |
| `CheckpointTracker` | Go (`querynodev2/segments`) | Maps `(segmentID, row offset)` → `MsgPosition`, derived from the `StartPosition` already carried by `delegator.InsertData`. Drives checkpoint reporting. |

### Why TEXT collections flush from the growing segment

A normal collection writes through StreamingNode: WAL → write buffer →
`SyncPolicy` → `SyncManager` → `MetaWriter`. The QueryNode then loads sealed
segments from object storage.

For TEXT collections that path would force the data to live in **two**
nodes simultaneously: the QueryNode growing segment must hold it for queries,
and the StreamingNode write buffer must also hold it for flush. For
document-sized payloads this doubles RAM and adds WAL bandwidth that the
data plane cannot afford.

The growing segment already has the data in memory in segcore. By giving
the QueryNode a flush manager that reuses the existing flushcommon
infrastructure, we keep the same `SyncPolicy` semantics, the same
`SaveBinlogPaths` contract, and the same checkpoint propagation, but
without ever sending the TEXT bytes through the WAL or storing them in
StreamingNode.

This is the key architectural choice; everything in the write-path
description below follows from it.

### Write path

1. **Pipeline → Delegator.** WAL consumer feeds `delegator.InsertData`
   (which already carries `StartPosition`) to the growing segment.
2. **Insert + record batch.** `ProcessInsert` writes rows into segcore as
   today, and additionally calls
   `CheckpointTracker.RecordBatch(segID, endOffset, position)` to remember
   which `MsgPosition` covers which row range.
3. **Background sync loop.** `GrowingFlushManager` ticks periodically. It
   wraps each growing segment as a `GrowingSegmentBuffer` and runs the
   existing flushcommon `SyncPolicy`s (`FullBuffer`, `StaleBuffer`,
   `Sealed`, `FlushTs`). Selected segments are dispatched to a sync
   worker.
4. **C++ flush (single FFI call).** Go calls
   `segment.FlushData(ctx, flushedOffset, currentOffset, flushConfig)`,
   which invokes `FlushGrowingSegmentData` in segcore. The entire
   extract → classify → write → commit sequence runs in C++:
   - Segcore extracts unflushed rows from the growing segment's
     `ConcurrentVector` for `[flushedOffset, currentOffset)`.
   - Each TEXT row is classified by `InlineThreshold`.
   - LOB-sized values are appended to the current Vortex file via
     `VortexFileWriter`. The first time a LOB write happens in the writer
     instance, a new UUID is generated as the `file_id` for the Vortex
     file.
   - The reference binlog is written via the existing `PackedWriter`,
     with each row encoded as either `[0x00 | bytes]` or
     `[0x01 | pad | file_id | row_offset]`.
   - On completion, Vortex files are closed, the reference parquet is
     closed, the column-group `Transaction` is committed, and the
     manifest path + LOB `file_id`s are returned to Go.
   No per-row data crosses the CGO boundary.
5. **Metadata update.** Go updates the segment manifest via the existing
   `MetaWriter` (`SaveBinlogPaths` with `Flushed=false`,
   `WithFullBinlogs=false`). Checkpoint propagation reuses
   `ChannelCheckpointUpdater`. After `SaveBinlogPaths` succeeds, the
   `CheckpointTracker` updates the flushed offset and acknowledged
   manifest version.

Failure semantics: a crash before step 4 completes leaves an orphan
Vortex file under `lobs/{field}/_data/`, never referenced from any
`SegmentInfo`. The LOB GC sweeps it after the safety window.

### Read path

`GetText(rowID)` reads one row of the reference binlog and dispatches on
the flag byte:

- `0x00` → return the inline bytes after the flag.
- `0x01` → decode `(file_id, row_offset)`, fetch through
  `TextColumnReader::Take`, which:
  1. Groups requested references by `file_id`.
  2. For each file, opens (or reuses, via the per-segment LOB reader
     cache) `{partition}/lobs/{field}/_data/{file_id}.vx`.
  3. Calls `VortexFileReader::take(row_indices)` for batched, vectorized
     random access.
  4. Reorders results back to the caller's original index.

The query and expression layers above `GetText` are unchanged.

### Compaction

Compaction decides upfront by scanning the reference binlogs of the source
segments to collect per-`file_id` reference counts:

```
total_refs = Σ ref_count over all (segment, file)
total_rows = Σ row_count over all distinct file_ids   (from Vortex file metadata)
hole_ratio = 1 - total_refs / total_rows
```

- **REUSE_ALL** (`hole_ratio < threshold`, default 0.3): the TEXT column
  in the output is built by **byte-copying** the encoded reference rows
  from each source binlog. `file_id` and `row_offset` are unchanged. LOB
  Vortex files are not touched. This is the common
  case and is essentially free.
- **REWRITE_ALL** (`hole_ratio ≥ threshold`): for each live row, the
  compactor reads the original text (inline or via LOB) and feeds it
  through `loon_segment_writer_*` again. The output gets brand-new LOB
  files with dense `row_offset`s starting from 0. Old LOB files that
  lose all references are reclaimed by GC.

Some compaction kinds bypass the hole-ratio decision and force a fixed
strategy because their data movement is predetermined:

- **Clustering compaction** (including `ClusteringPartitionKeySortCompaction`):
  always REWRITE_ALL — data is repartitioned across the output segments,
  so LOB references cannot be reused as-is.
- **Mix compaction with split (1 → N)**: always REWRITE_ALL — rows are
  redistributed across multiple output segments.
- **Sort compaction** (including `PartitionKeySortCompaction`): always
  REUSE_ALL — only row order changes, the underlying byte content of
  each row is unchanged, so reference rows can be byte-copied into the
  output.
- **L0 delete compaction**: always SKIP — only applies delete logs to
  segments; LOB references in the source segments are unchanged.
- **Non-split mix compaction**: uses the hole-ratio decision above.

### Garbage collection

A new `garbage_collector_lob.go` runs alongside the existing segment GC:

- **Reachable set.** Scan the reference binlogs of all live (non-dropped)
  segments, union all `file_id`s found in LOB references.
- **Scan.** For each `lobs/{field_id}/_data/`, list files. Any file
  whose `file_id` is not in the reachable set **and** whose mtime is
  older than `dataCoord.gc.lob.safetyWindow` is deleted.
- **Safety window.** Protects the gap between "Vortex file closed" and
  "`lob_file_refs` committed in etcd".
- The standard segment GC is updated to skip the partition `lobs/`
  prefix so the two GCs do not race.

### Configuration parameters

See *Public Interfaces*. All parameters are exported in
`configs/milvus.yaml` and registered in `paramtable`.

## Compatibility, Deprecation, and Migration Plan

**Impact on existing users.** None at the API level. SDK clients, the
proto query language, and the rest of the schema system see no change.
Behaviorally, large TEXT inserts that previously stressed memory or
StreamingNode bandwidth now succeed cheaply.

**V2 segments.** Untouched. The new path is V3-only.

**Existing V3 segments without TEXT data.** Untouched. The TEXT
column-group only appears in collections that declare a TEXT field.

**Pre-LOB experimental TEXT path.** The previous Go-side LOB manager +
Parquet writer prototype is removed in the same change. Any segment that
had been written by the prototype is migrated by reading through the old
reader and re-flushing through `TextColumnWriter` during the next
compaction; no separate offline migration is required.

**StreamingNode behavior change for TEXT collections.** For collections
that contain a TEXT field, StreamingNode no longer flushes those
segments — Growing Segment does. This is internal and gated by the
collection schema. For non-TEXT collections, StreamingNode behavior is
unchanged.

**Phasing out the older behavior.** The legacy "TEXT inline in the
columnar file" behavior is not removed; it is the natural fallback when
every value is below `inlineThreshold`. Operators can effectively
disable LOB by setting `inlineThreshold` to a very large value.

**Migration tools.** None required.

**Removal timeline.** No legacy path is deprecated by this MEP.

**Rolling-upgrade ordering.** All Milvus components that touch TEXT
segments must include the LOB read path before any component is upgraded
to the LOB write path. Within this single feature branch all components
are upgraded together, so the cluster-internal contract is consistent at
the version boundary.

## Test Plan

System / integration:

- End-to-end insert → growing-segment flush → query for mixed inline +
  LOB workloads on V3, with value sizes that straddle `inlineThreshold`.
- Compaction REUSE_ALL: synthetic deletes that keep `hole_ratio < τ`;
  verify LOB Vortex files are not rewritten and the output segment's
  `lob_file_refs` correctly merges source `ref_count`s.
- Compaction REWRITE_ALL: synthetic deletes that push `hole_ratio ≥ τ`;
  verify new LOB files are produced and old ones are reclaimed by GC
  after the safety window.
- Cross-segment LOB sharing: two segments referencing the same
  `file_id`; verify both can be queried independently and that GC does
  not delete the file while either is alive.
- Orphan LOB file: kill QueryNode between Vortex close and metadata
  commit; verify GC reclaims the orphan after the safety window and
  not before.
- Failure recovery: kill QueryNode mid-ingest; restart, replay WAL from
  the last reported `MsgPosition`, and verify the segment converges
  without data loss or duplication.
- Import (CSV / JSON / Parquet) of large TEXT values; verify produced
  segments have the same on-disk shape as inserted ones.
- Python E2E suite: `tests/python_client/testcases/test_text_lob.py`.

Lower-level coverage:

- C++ unit: `VortexFileWriter`, `FlushGrowingSegmentData`,
  LOB reference encoding / decoding.
- Go unit: `growing_flush_manager`, `growing_segment_buffer`,
  `checkpoint_tracker`, `garbage_collector_lob`, compaction REUSE/REWRITE
  decision matrix.

The implementation is considered correct when:

1. The integration suite passes on V3 with TEXT enabled.
2. A long-running soak test with continuous insert + delete + compaction
   shows bounded LOB-file count, bounded object-store space, and
   correct query answers throughout.

## Rejected Alternatives

- **Store TEXT inline in the columnar file, no LOB path.** Rejected:
  reproduces the memory and write-amplification problems the feature is
  meant to solve.

- **Keep LOB files inside the segment basePath.** Rejected: makes
  CopySegment / restore / GC simpler but **kills compaction reuse** —
  the same payload would have to be copied into every output segment's
  basePath. For a column with low churn this is exactly the cost we
  want to avoid.

- **Per-value object on the chunk manager (one S3 key per LOB).**
  Rejected: real object-store per-object overhead destroys throughput
  for millions of medium-sized values.

- **Use Parquet for LOB files (the previous prototype).** Rejected:
  weaker compression on long opaque strings, and Parquet's random-row
  access is not as cheap as Vortex's split index. The previous
  prototype also kept the writer in Go, so per-row CGO calls dominated.

- **Manage `file_id` allocation in Go via etcd.** Rejected: forces
  every C++ writer to round-trip through Go for each new file and
  prevents segcore-side autonomy. UUID generation keeps allocation
  entirely in C++ with no coordination overhead.

- **Use a separate manifest file for LOB files.** Rejected:
  `file_id → path` is computable; an extra manifest adds another
  consistency surface and another concurrent-write hazard with no
  benefit.

- **Flush TEXT collections through StreamingNode like everything
  else.** Rejected: would require holding TEXT payloads in two
  nodes simultaneously and would push document-sized payloads through
  the WAL. Growing-segment direct flush is the whole point of the
  feature.

- **Drop the inline path entirely; everything is a LOB.** Rejected:
  short strings would pay an extra Vortex round-trip per row on read,
  and the per-`file_id` open cost would amortize poorly for tag-like
  columns.

## References

- Source commit: `e482d0259e1f4f895ddddff963e42e00a31910a7` —
  `feat: support text lob`.
- Internal design doc (Feishu): TEXT 类型的全新存储设计.
- Related Milvus issue: milvus-io/milvus#48783.
- Related Milvus PR: milvus-io/milvus#47567.
- V3 column-group manifest format MEP: `20260226-manifest-format.md`.
- Related JSON storage MEP: `20250308-json_storage.md`.
