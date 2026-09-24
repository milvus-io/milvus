# MEP: Import V3 — Sort-Based Re-shard

- **Created:** 2026-09-21
- **Status:** Implemented behind the one-way `dataCoord.import.enableImportV3` gate
  (default `false`), commit `23aaaffe2b feat: implement Import V3 reshard`
- **Component:** DataCoord, DataNode, Storage

## Summary

Import V2 reads every source file twice: `PreImportTask` parses a whole file and
keeps only statistics, then `ImportTaskV2` reads it again, hashes rows per
file-group, and writes directly into pre-allocated segments. The same
`(vchannel, partition)` bucket is split by every file-group task, producing many
small segments and a `hashed_stats` table that grows with `V × P`.

Import V3 moves the reorganization to the **first and only read**. Each source
is parsed once by a `ReshardTask`, normalized, routed to its bucket, sorted, and
written as immutable **fragments**. `Planning` packs fragments back into
per-segment, bucket-local plans across all source tasks. Each `ImportTaskV3`
then runs a strict one-head k-way merge over one plan and writes exactly one
formal segment through the existing Storage V2/V3 writers. The pipeline — not
the file-group boundary — decides segment shape, and the object store sees each
fragment exactly once.

Three DataNode worker kinds implement this:

| Kind | Role |
| --- | --- |
| `preimport v2` | Count-only: exact per-file rows/size for ordinary imports (no hashing) |
| `reshard` | Read sources once, route to buckets, write sorted fragments + manifest |
| `import v3` | Merge one segment plan's fragments into one formal segment |

## Motivation

- **Parse the source once.** Current PreImport leaves no reusable output.
- **Bucket ownership must exist before task boundaries.** Routing inside
  per-file tasks makes cross-file segment planning impossible.
- **Metadata stays under etcd's limit.** No `hashed_stats` table that grows
  with `V × P` and can exceed etcd's default 1.5 MiB request size, so e.g.
  1024-partition tables import natively.
- **One final sorted segment, one write.** V3 output is already sorted for the
  index, so the post-import `Sorting` compaction is skipped.
- **Explicit failure instead of an execution-time schema-change deadlock.** V2
  can hang in `IndexBuilding` when a collection is altered mid-import; V3
  compares an import-relevant schema projection and fails loudly.

## Goals and Non-Goals

Goals:

- Parse each source exactly once; make the first read directly consumable.
- Materialize bucket ownership into temporary data before task boundaries.
- Bound task memory by a slot budget, spilling overflow to local disk.
- Produce the final sorted segment in one pass, reusing Storage V2/V3,
  TEXT/LOB, BM25 and stats paths.
- Cover JSON/CSV/Parquet/NumPy, backup/binlog, AutoID, explicit PK, partition
  key, namespace, functions, and existing 2PC Import visibility.
- Define retry, recovery, cancellation and rolling upgrade.

Non-goals:

- **L0 import stays on Import V2** (`NeedsFileIDRanges` is false for it).
- **No collection DML delete in the normal final merge.** V3 does not read the
  collection delete snapshot or add deltalog refs/watermarks. Deletes arriving
  after import are hidden by Query visibility and reclaimed by later
  compaction, matching V2.
- No new user-visible options, public API or external fragment input.
- No global PK-range uniqueness; only per-fragment and per-segment ordering.
- No partial success: any required source, fragment or segment failure fails or
  retries the whole job.
- No replacement of Storage V2/V3 formats, the function framework or TEXT LOB.

## Architecture Overview

```text
Pending
  ├─ ordinary import ──▶ PreImporting ──▶ AssigningIDRange ─┐
  └─ backup import ──────────────────────────────────────────┴─▶ Resharding
        ─▶ Planning ─▶ Importing ─▶ IndexBuilding
        ─▶ Uncommitted ─▶ Committing ─▶ Completed

any state before Committing ─▶ Failed
```

- **Ordinary imports** must know the exact per-file row count before Reshard can
  generate deterministic PK/RowID, so they first run a count-only preimport,
  then allocate and broadcast exact per-file ID ranges (`AssigningIDRange`).
- **Backup imports** carry their own PK/RowID/timestamps and go straight from
  `Pending` to Resharding (`reshardTaskPlanner.CreateTasks`).
- A zero-row job short-circuits to `Completed` (auto-commit) or `Uncommitted`.
- Job state machine lives in `internal/datacoord/import_v3_state.go`; the
  orchestrator (two loops + per-tick dispatch) in `import_checker_v3.go`;
  planning in `import_v3_planner.go`; execution-input derivation in
  `import_v3_plan_factory.go`.

Task records (`ReshardTask`, `ImportTaskV3`) persist only what recovery cannot
re-derive. Everything else is re-derived at dispatch from the frozen job.

Core invariants:

1. Every source row belongs to exactly one bucket.
2. Every fragment belongs to one bucket and is sorted by the shared `SortSpec`.
3. Every winning fragment enters exactly one segment plan; fragments are never
   split or shared.
4. A segment plan is a contiguous interval of one bucket's canonical order.
5. A merge reader has at most one head; heap size is `O(fan_in)`.
6. Duplicate PKs are preserved; equal-key relative order is not a contract.
7. All functions run once in Reshard (ordinary only); TEXT LOB and formal stats
   run only in the final round.
8. Every formal segment keeps `is_importing=true` until the CommitImport
   per-vchannel fence clears it.
9. DataCoord accepts only the complete result of a task's current `run_id`.
10. Task version and per-file ID ranges are fixed in the WAL/job.

## Determinism and the WAL

`msgpb.ImportMsg.version` (`int64`) is the durable execution
compatibility marker: `0`/absent and `2` mean ImportTaskV2, `3` means
ReshardTask + ImportTaskV3. A direct request arrives unspecified (`0`; the proxy
sets no version), and DataCoord resolves it **before broadcast** — V2 by
default, upgraded to V3 only when `dataCoord.import.enableImportV3=true` and the
job is not L0 — so the broadcast ImportMsg carries the resolved `2` or `3`. On
the ACK callback (`createImportJobFromAck`), `3` maps to `ImportJobVersionV3`
and `0`/absent/`2` map to `ImportJobVersionV1`; this is the only Task→Job
version boundary. `ImportJobVersion` (`V1`, `V3`) is a separate DataCoord
state-machine version. An unknown explicit Task version with a valid job ID is
persisted as a minimal terminal V1 failure envelope; without a job ID it is
corrupt and left unacknowledged.

**ID ranges (ordinary imports).** V3 reuses the two-phase ID-range mechanism
added in PR #53544 (`internal/util/importutilv2/importid`): the ImportMsg
broadcast carries no ranges; once the exact per-file row counts are known the
job parks in `AssigningIDRange`, the primary allocates one contiguous exact
range per file, and the `ImportIDRange` WAL message makes every cluster apply
the same ranges, so both derive identical PK/RowID
(`generated_id = begin + row_offset`). V3's only additions are the count-only
preimport that supplies those counts and `importV3NeedsIDRanges` (PK present,
neither backup nor L0; no local-allocator fallback). The DataNode consumes each
file's range with a `FileIDRange` cursor in Reshard
(`AppendPreallocatedSystemFields`), and the shared cross-cluster divergence gate
still fails a job whose local row count disagrees with the reserved width.
Backup and L0 need no ranges.

Shard routing uses the local collection metadata's vchannel order and the
literal `ImportMsg.partitionIDs` order (slice index = partition ordinal);
neither is sorted, deduped or regenerated. `DataTs` comes from the broadcast
max timetick, is stored on the job, and is reused by every run.

Backup keeps source PK/RowID/timestamp and applies no ranges; the binlog reader
retains its schema validation, time-range and deltalog filtering.

## Stage 0 — Count-only PreImport (`preimport`)

For ordinary imports, `importV3PreImportStage` groups not-yet-covered files
(`FilesPerPreImportTask`) into count-only `PreImportV2` tasks. The worker
computes exact per-file rows and decoded size:

- Parquet/numpy read the exact count from file metadata (footer/header) bounded
  by `dataCoord.import.parquetFooterMaxSize` (default 64 MiB);
- JSON/CSV fall back to a scan.

It checks `dataNode.import.maxImportFileSizeInGB` and never hashes. On completion
the job either parks in `AssigningIDRange` (primary allocates + broadcasts;
secondary waits for replication) or, once ranges are present, runs the shared
tail: divergence gate → zero-row exit → Resharding.

## Stage 1 — Reshard (`reshard`)

### Grouping (DataCoord)

A complete `ImportFile` is atomic (NumPy multi-column paths, a backup source's
insert+delta prefix set cannot split). Before the first read, files are packed
with a deterministic one-dimensional Best-Fit-Decreasing:

- size = preimport-measured decoded bytes when available, else physical object
  bytes (backup expands its object list once via `importbinlog.ExpandObjects`
  and sizes concurrently);
- target = `min(fragmentTarget × V × P × 2, maxSizeInMBPerImportTask)`;
- tie-break by `file_id`/`task_id` ascending; no backtracking; oversized files
  own a bin;
- grouping is persisted (`source_ids`) and never recomputed on recovery — only
  missing coverage is filled.

### Dispatch plan

`ReshardTaskPlan` is re-derived at dispatch from the frozen job by
`importV3PlanFactory.reshardPlan` and travels inside `ReshardTaskRequest` (never
written to object storage): schema, `temp_schema`, vchannels, partitions,
`SortSpec`, `fragment_size`, `SourceFileSpec`s (CSV separator/null-key or backup
start/end ts + storage version), and a `backup` flag. `sortSpec` is `[PK]`, or
`[partition key, PK]` for namespace collections; Int64 ascending, VarChar
byte-lexicographic ascending, no hidden tie-breakers.

The task's slot is charged by `reshardmem.Model.WorkingSet` at
`reshardmem.MemoryPerSlot(dataNode.workerSlotUnit)`; the DataNode derives its
runtime budget from the same model, so charge and enforcement cannot drift.

### Execution pipeline

```text
source queue → single reader, sequential
 → prepare stage: parse / normalize / materialize RowID·AutoID / functions
 → route to BucketKey (vchannel, partition)
 → bounded per-bucket buffers
 → storage.Sort → one packed Parquet fragment (detached write pool)
 → manifest-last
```

Ordinary normalization follows the V2 order: `CheckRowsEqual` →
`CheckStructArrayConsistency` → `AppendNullableDefaultFieldsData` →
`FillDynamicData` → `AppendPreallocatedSystemFields` (file IDRange) → hash
routing → sort. All functions (TextEmbedding, BM25, MinHash) run here once per
batch, at the V2 pipeline position; backup skips function execution because its
sources already carry the outputs.

`temporarySchema`:
- ordinary = user fields + `RowID` (timestamp is supplied at final merge), with
  TEXT mapped to VarChar carrying `proxy.maxTextLength`;
- backup = user fields + full system fields (`RowID`, `Timestamp`), TEXT mapped
  the same way.

TEXT is stored as raw UTF-8 in fragments/intermediates; only the final writer
produces manifest LOB references. `temp_schema` and the target schema are kept
distinct on purpose.

### Memory model and spill

`importutilv2/reshardmem` is the single source of truth for the reshard memory
contract. A run's peak = fixed IO overhead + pipeline copies + resident buckets
+ detached fragment inputs + their structural overhead + one sort copy per
in-flight write, scaled by `reshardMemoryExpansionFactor`. Key knobs:

- `dataCoord.import.fragmentSizeInMB` (128) — per-bucket flush trigger and
  fragment soft target.
- `dataCoord.import.reshardResidentBucketCap` (16) — how many buckets stay fully
  resident; beyond it, tails are capped at `cap × F / buckets` and stream to
  disk.
- `dataCoord.import.reshardFlushConcurrency` (2) — detached fragment writes in
  flight.
- `dataCoord.import.reshardMemoryExpansionFactor` (1.5) — scales the whole live
  footprint (GC headroom).

A run never blocks on a global allocator. When a bucket reaches the fragment
target it is detached to the flush pool (sort + encode + upload). Otherwise, if
a bucket tail crosses `BucketTailCap`, or if real free memory drops below the
dynamic `CheckpointFloor` (flush spike + 10% system reserve), the largest
bucket is appended to a run-local, fixed-shard **Arrow IPC spill log**
(`dataNode.import.reshardSpillMaxStreams`, 128; buckets hash to one stream so a
bucket's ranges stay in one file). The spill root is node-local under
`import_v3_spill`, deliberately outside the durable `import_v3` prefix; startup
`CleanImportV3Prefixes` wipes it.

### Fragment output

Each fragment is one bucket, one column group, one packed Parquet object; `seq`
is monotonic within the task. A run publishes `ReshardManifest` only after all
fragments are written and all writers closed:

```text
write fragments → close writers → build descriptors
→ {root}/import_v3/{job}/reshard/{task}/manifests/{run}.pb → Completed
```

A run without a manifest is never complete. The manifest keeps only
`channel_index`, `partition_id`, `seq`, `path`, `rows`, `logical_bytes`.

## Stage 2 — Planning

Once every ReshardTask has a catalog-verified `Completed` marker (gated on
acceptance validating its manifest), `importV3Planner.Plan`:

1. Validates the schema projection and storage version; cleans up leftover
   `None` tasks with their pre-allocated segments.
2. Loads each task's manifest, validates descriptors, and sorts the global
   fragment list by `(channel_index, partition_id, reshard_task_id, seq)`.
3. **Sequential Next-Fit packing** per bucket against
   `getExpectedSegmentSize`; fragments are never split and a fragment larger
   than the target owns a plan.
4. **One ImportTaskV3 per segment plan**, persisting only fragment ownership,
   `vchannel`, `partition_id`, `rows`, `segment_id`, reserved `log_range`, and
   `slot`. The full `ImportTaskPlan` (SortSpec, WriterSpec, schemas, fan-in,
   `data_ts`) is re-derived at dispatch and passed in the CreateTask RPC.
5. **Pre-allocates the formal SegmentInfo** (`State=Importing`,
   `IsImporting=true`, `NumOfRows=0`, `LastExpireTime=MaxUint64`, explicit
   storage/schema version, `Stats=nil`). The `(task InProgress + run_id)` marker
   is persisted before the RPC (marker-last), and `None → Pending` protects the
   task/segment preparation window.
6. **`log_range`** per segment: `1` (stats/manifest) + BM25 outputs, plus one
   per column group for Storage V2; a width outside `(0, MaxUint32]` fails
   planning explicitly.
7. **Disk quota**: `CheckImportV3DiskQuota` reserves `totalFragmentBytes` against
   the global/collection quota and stores it as `RequestedDiskSize`.
   `totalFragmentBytes` is the plan's normalized decoded volume, the same metric
   `CheckDiskQuota` reserves.

Recovery keeps already-created tasks and fills missing fragment coverage
(`missingImportTaskV3Specs`) without re-assigning fragments. An empty job
(0 fragments/rows) shortcuts to `Uncommitted`/`Completed`.

`WriterSpec` is derived from the frozen target schema + live config:
storage version, schema version, writer format, explicit column groups
(`storagecommon.SplitColumns` + `FillColumnGroupFormats`), TEXT config, V2
packed IO sizes, TTL, PK-stats capacity (`max(rows,1)`), BM25 field IDs, Bloom
type/fpp.

## Stage 3 — Importing (`import`) and Final Merge

### Strict one-head k-way merge

`MergeExecutor` uses `storage.MergeSort` (typed-value heap, one head per reader,
borrowed-record lifetime, exactly-once predicate, monotonicity check). Direct
fan-in is `min(fragmentMergeFanIn=128, len(fragments))`, clamped to `[2,1024]`.
When a plan has more inputs than the fan-in, the executor performs
**hierarchical merge**: split the canonical sequence into contiguous groups of
at most `fan_in`, merge each into an intermediate, repeat. Singleton groups pass
through without I/O.

Intermediate merge rounds are written to **node-local Arrow IPC files**
(`{localStorage}/import_v3_spill/{job}/{task}/{run}/merge/{seg}/{round}_{group}.arrow`)
and deleted with the task. The object store therefore sees each fragment only
once; the doubled IO of a bounded-fan-in merge stays local. Intermediate row
counts and monotonicity are checked; intermediates apply no predicate/transform.

### Final transform

```text
ordered fragment readers
 → k-way MergeSort predicate  (TTL / TTL field only, empty delete map)
 → sorted surviving RecordBatch
 → final writer: RecordToInsertData → materialize timestamp → formal writer
 → bloom/BM25 stats, manifest/binlogs → SegmentResult
```

- The predicate is **TTL-only** and always gives `EntityFilter` an empty delete
  map; `time.Now()` is captured per batch and never persisted.
- Ordinary import materializes `timestamp = ImportJob.DataTs`; backup keeps the
  source timestamp carried in the fragment and requires it.
- Writer options come from the `WriterSpec`; the formal writer encrypts with the
  **target** collection's zone (no read plugin context), while fragment readers
  use the read/source plugin context.
- `sorted=true` for ordinary, `namespace_sorted=true` for namespace. Importing
  goes straight to `IndexBuilding` (no post-import Sorting).
- A lazy final writer means a fully filtered plan creates no writer; the result
  is `SegmentResult{rows=0}` with no output.

### Result acceptance and visibility

The worker returns one `SegmentResult` only on `Completed`. Under the
import-meta task-key lock, DataCoord validates job/task/run identity and the
result contract (`validateImportResults`), then `applyImportResults` fills the
pre-allocated segment (`Flushed`, keeping `IsImporting=true`), records
statistics/position/sorted flags, bumps the segment schema version, and drops
zero-row placeholders. The task `Completed` marker is written last. A hot-flipped
`common.storage.useLoonFFI` that makes the written layout disagree with the
segment's recorded storage version fails the task explicitly.

Formal segments become visible only when `HandleCommitVchannel` clears
`is_importing` after `commit_timestamp >= SegmentInfo.Stats.TimestampTo`.
`Committing` is the point of no return: timeout/Abort can no longer fail the
job, and committed vchannels are never rolled back.

## DataNode Worker Scheduling

- **Slot scheduler** (`internal/datanode/importv3/scheduler.go`) is pure
  admission: a submitted task queues until `used + slot <= capacity`; queued
  slots count as used in `QuerySlot` so DataCoord stops water-filling the node.
  A queued task answers `Pending` (never "not found"), so DataCoord waits for
  its slot instead of retrying elsewhere. A task whose own slot exceeds capacity
  runs alone once the node is otherwise idle.
- **TaskManager** keys by `task_id` and fences by `run_id`: same run ⇒
  idempotent; older run ⇒ stale no-op; newer run ⇒ cancel/replace. Dropped or
  superseded runs keep charging their slot until their pool closure exits.
  Worker errors map to `Retry` (ctx canceled or non-terminal) or `Failed`
  (terminal denylist `common.IsTerminalImportV3Err`), classified once at the
  worker; DataCoord trusts the state and never reinterprets `reason`.

## Failure, Retry and Recovery

Two retry layers: bounded in-run operation retry for retryable storage/node
errors, and a new run after node loss / exhausted retries / resource failure.
Re-dispatch assigns a **new** `run_id`, a new physical segment ID and a new
`log_range`; object names embed `run_id`, so a late writer cannot overwrite a
new run's output. DataCoord never reuses a run whose Create result is uncertain.

| Case | Code | Retry |
| --- | --- | --- |
| User file/schema/content error | `ErrImportFailed` 2100 (Input) | no |
| Per-file ID range > `MaxUint32` (pre-alloc) | `ErrParameterInvalid` 1100 (Input) | no |
| Runtime ID range exhausted | `ErrImportFailed` 2100 | no |
| Fragment disorder / result-segment mismatch / zero-row with output | `ErrDataIntegrity` 1009 (System) | no |
| Invalid fan-in, plan assembly, schema projection mismatch, `ErrIDRangeTooSmall` | `ErrImportSysFailed` 2101 (System) | no |
| Disk quota exceeded | `ErrServiceQuotaExceeded` (terminal at checker) | no |
| Transient object-store/node failure | keep `ErrService*` / `ErrIo*` retryable codes | new run after exhaustion |
| Node lost / task-not-found | ownership-lost control branch | re-dispatch with new run |
| Stale run/result | no error | idempotent no-op |

`ErrNodeNotFound`, session loss and task-not-found are ownership-lost signals
and re-dispatch; they must not be classified as terminal failure. DataCoord
recovery is incremental: keep ready tasks, fill missing source/fragment
coverage, then advance the job state; it never guesses progress via OSS LIST.

## Paths and GC

```text
{root}/import_v3/{job}/
├── reshard/{task}/
│   ├── fragments/{channel_ordinal}/{partition_id}/{run}_{seq}.parquet
│   └── manifests/{run}.pb
└── import/{task}/        (no object-store intermediates)
{localStorage}/import_v3_spill/{job}/{task}/{run}/...   (reshard spill, merge intermediates)
```

Formal segments always use the existing `insert_log`/stats/manifest paths, never
the `import_v3` prefix. Barrier/Planning/Importing read only ID-derived exact
paths; LIST is reserved for GC and diagnostics.

Terminal-job GC (`importV3JobGC`) is derived from durable facts, with no GC
record: **Quiesce** (re-send version-aware Drop while still bound; drop the
failed job's import segments; remove task records only after `cleanupTs`) →
**Retain** (until `cleanupTs`) → **Delete** (broadcast `RollbackImport` for a
failed replicate job, then `RemoveWithPrefix("import_v3/{job}/")`, remove
remaining tasks, remove the job). Orphan `import_v3/{job}/` prefixes with no
live job and no write for `dataCoord.gc.missingTolerance` are swept by the GC
orphan scan. On restart, `importInspector.reconcileOrphanImportSegments` drops
pre-allocated `Importing` segments no task references.

## Schema Change During Import

V2 can deadlock when a collection schema is altered mid-import. V3 compares an
**import-relevant projection** (field IDs/types/nullable/default/dynamic,
PK/partition key/namespace, TEXT physical rules and resources, function
type/inputs/outputs, relevant properties) between the frozen job schema and the
current collection schema at Planning, at each result acceptance, and before
IndexBuilding:

- equivalent ⇒ continue; the segment's schema version is safely bumped to the
  current version;
- different ⇒ `ErrImportSysFailed` (2101), non-retriable: fail, quiesce and
  reclaim, recording the first differing item.

## Rollout and Configuration

- `dataCoord.import.enableImportV3` (default `false`) is a one-way release gate
  selecting V3 for **new unversioned** ordinary/backup requests; L0 and any
  explicit/message version keep their chosen path. It must be opened only after
  all DataNodes and all potentially-active DataCoords support V3. The version
  is persisted in the WAL and job, so a created V3 job always completes as V3;
  no scheduler per-task version filter is added.
- Rollback to a binary that does not understand V3 is forbidden while any V3
  broadcaster task, non-terminal job/task or uncleaned temporary prefix exists.

New knobs:

| Config | Default | Role |
| --- | --- | --- |
| `dataCoord.import.enableImportV3` | `false` | one-way V3 gate |
| `dataCoord.import.fragmentSizeInMB` | `128` | per-bucket fragment soft target |
| `dataCoord.import.fragmentMergeFanIn` | `128` | max direct merge fan-in, `[2,1024]` |
| `dataCoord.import.reshardResidentBucketCap` | `16` | buckets kept fully resident before spilling |
| `dataCoord.import.reshardFlushConcurrency` | `2` | detached fragment writes in flight |
| `dataCoord.import.reshardMemoryExpansionFactor` | `1.5` | live-footprint expansion |
| `dataCoord.import.parquetFooterMaxSize` | `67108864` | max footer read for count-only preimport |
| `dataNode.import.reshardSpillMaxStreams` | `128` | reshard spill files (Arrow IPC) |

Reused: `dataNode.import.readBufferSizeInMB` (base buffer),
`dataNode.workerSlotUnit` / `dataCoord.import.memoryLimitPerSlot` (slot
conversion), `dataCoord.import.maxSizeInMBPerImportTask` (8192, reshard BFD
target cap), `dataNode.import.maxImportFileSizeInGB`, `dataCoord.import.taskRetention`,
`dataCoord.gc.missingTolerance`.

## Job Progress Reporting

`GetJobProgress` maps the persisted job state to a user-visible percentage and
external state, returning `(progress, externalState, importedRows, totalRows,
reason)`. V1 and V3 use different weights because V3 has no `Sorting` stage and
has two extra internal stages (`Resharding`, `Planning`).

```text
// ImportJobVersionV1:
// 10%: Pending
// 30%: PreImporting/AssigningIDRange
// 30%: Importing
// 10%: Stats        (Sorting)
// 10%: IndexBuilding
// 10%: Completed
// ImportJobVersionV3:
// 10%: Pending
//  5%: PreImporting/AssigningIDRange
// 30%: Resharding
//  5%: Planning
// 10%: Importing
// 30%: IndexBuilding
// 10%: Completed
```

Sub-progress within each weighted band:

| State | V1 | V3 |
| --- | --- | --- |
| Pending | files covered by a `PreImportTask` / total files | same `pendingFileProgress` (legacy `PreImportTask` only, so effectively 0 until the job advances) |
| PreImporting / AssigningIDRange | completed `PreImportTask`s / total | completed count-only `PreImportV2` tasks / total |
| Resharding | — | completed ReshardTasks / total (100% if none) |
| Planning | — | constant band (no sub-progress); instant stage |
| Importing | sum of segment `NumOfRows` / planned rows | importedRows / total planned rows |
| Stats (Sorting) | sorted segments healthy / total | — |
| IndexBuilding | indexed segments / target segments (`100%` when `waitForIndex=false`) | same, over non-zero V3 segments |
| Uncommitted / Committing | `99%` (external state `Importing` when `auto_commit=true`) | same |
| Completed | `100%` | `100%` |
| Failed | `0%` + reason | `0%` + reason |

Notes:

- V3 `ImportedRows`/`TotalRows` come from the `ImportTaskV3` records' `rows` and
  each pre-allocated segment's `NumOfRows`; V2 uses the `ImportTaskV2` file
  stats. Both report the total planned rows once a terminal-ish state is
  reached.
- `Planning` always reports its full band constant (no finer signal), because
  the stage is a single fast catalog write.
- Every internal V3 stage except `Pending`, `Uncommitted`, `Committing`,
  `Completed`, `Failed` is projected to the external `Importing` state; the
  real stage is preserved only in logs and metrics.
