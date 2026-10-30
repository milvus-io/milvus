# DataCoord Segment Change Staging — Atomic Publication of Batch Segment Changes

- **Created:** 2026-09-09
- **Status:** Draft
- **Component:** DataCoord / Data View (GetRecoveryInfo family / DataView entity)
- **Related work:**
  - `docs/design-docs/design_docs/20260817-datacoord-segment-manifest-commit.md`
  - DataView entity (@chyezh, PR #52537 `feat: add DataView lifecycle management`)
  - QueryView series (@chyezh, PR #51887 / #52432 / #52588 / #52653, issue #40451)
  - `docs/design-docs/design_docs/qviews/data_view.md`

## Summary

Batch data changes (import, mix/sort/clustering/forcemerge compaction, backfill,
copy segment) write their output segments into meta first, then a **async
pipeline** (sort → stats → index) gradually makes them ready. Today DataCoord's
data view (`GetRecoveryInfoV2` / `GetQueryVChanPositions`) decides **at read
time**, per segment, whether a segment is "ready", and uses lineage
(`CompactionFrom`) to **fall back** from a not-ready segment to its parents when
deciding what to expose. This design has the following problems:

1. **Atomic publication is hard**: members of one batch become visible one by
   one; the visible set is a function of async progress, not a committed fact.
   The query target set can flip between two `GetRecoveryInfo` polls.
2. **Lineage must be traced by the view**: the view has to understand the
   compaction lineage DAG, handling M:N outputs, partial ancestor coverage,
   crash-recovery intermediate states, blocked roots, etc.
   (`retrieveSegment` is a ~230-line fixed-point algorithm), and every new
   compaction type extends it.
3. **Three visibility mechanisms coexist**: clustering uses `IsInvisible=true`
   + per-segment flips, mix/sort uses the lineage fallback, import uses
   `IsImporting` + `CommitTimestamp`. Semantics, migration paths and debugging
   differ.
4. **Readiness is not persisted**: index/stats progress lives in
   `indexMeta`/`statsTaskMeta`, but "is this batch of segments ready as a whole"
   exists only as read-time derivation. After a DataCoord restart the view can
   only re-approximate the same conclusion by rescanning async state, during
   which the set drifts.

In parallel, the **DataView entity** (@chyezh, PR #52537, issue #40451) already
implements the storage-side view as an **immutable, versioned, etcd-persisted
collection snapshot** (`DataViewOfCollection` = Collection → VChannel →
Partition → (SegmentID, ManifestVersion), versioned by
`(streaming_version, compact_version)`), and publishes the **flush path
atomically** (`UpdateSegmentsInfoAndDataView` commits SegmentMeta + the snapshot
in one catalog txn). It already makes "not-ready segments invisible" natively —
the loadable projection (`loadableProjection`) only accepts Flushed, non-L0,
non-importing, non-invisible segments with a data footprint.

**But DataView does not yet solve atomic publication for batch changes**: apart
from flush, compaction/import/copy visibility still goes through asynchronous
`Recompute` full-snapshot rebuilds (eventually consistent, non-atomic with
SegmentMeta), and the compaction paths still rely on scattered per-segment
`IsInvisible` flips plus the legacy view's lineage fallback. That is exactly
where this design lands.

This design proposes **segment change staging**: move readiness evaluation from
**read time to write time with persistence**, and use a **change group** as the
atomicity unit, expressing "this batch is now visible" as **one DataView
snapshot atomic publication**:

- The batch's output segments first enter a **staged** state (present in meta,
  `IsInvisible=true`, invisible to both the DataView projection and the legacy
  view);
- When the async pipeline converges, the existing inspectors mark the whole
  group **ready** at write time (group-level persisted state, no read-time
  re-derivation);
- When the group is ready, DataCoord publishes all members into a **new DataView
  snapshot** (`compact_version` +1) in **one atomic transaction** (reusing
  `CommitSegmentManifests` + an extension of `UpdateSegmentsInfoAndDataView`),
  simultaneously retiring the superseded parents;
- Views (both the legacy `GetRecoveryInfoV2`/`GetQueryVChanPositions` and the
  new QueryView consuming DataView) only ever build their visible set from
  **committed** members — **staged = natively invisible**, and the legacy
  `retrieveSegment` lineage fallback can ultimately be deleted.

## Problem: Current Design

### Segment Lifecycle and the Data View

DataCoord exposes the query/load view through two interfaces
(`internal/datacoord/handler.go`):

- `GetQueryVChanPositions` (`handler.go:141`): per-channel classification of
  segments into flushed / unflushed / dropped / L0, returned to QueryCoord as
  the target data source.
- `GetRecoveryInfoV2` (`services.go:1052`): on top of the above, collects
  per-segment details (rows, level, sorted, manifest, ...) of visible segments;
  QueryCoord builds current/next targets from it
  (`internal/querycoordv2/meta/target_manager.go:139`).

Streaming-written segments follow `Growing → Sealed → Flushed` with no readiness
gate: flushed means visible (`UnflushedSegmentIds` / `FlushedSegmentIds`), and
the channel checkpoint governs the seek position.

### The Batch Data Change Async Pipeline

Batch-change output segments must enter meta while **data is written but
post-processing is unfinished** (otherwise task state, GC, and progress
reporting have no anchor), producing the "segment visible first, post-processing
catches up async" structure:

| Path | Segment entry shape | Async post-processing |
|---|---|---|
| Import (`import_util.go:269`) | `State=Importing`, `IsImporting=true` | sort compaction → stats → index |
| Sort compaction (`meta.go:3454`) | `State=Flushed`, `CompactionFrom=[origin]`；`IsInvisible` **仅当 origin `CreatedByCompaction=true` 时继承**——`completeSortCompactionMutation` 对非 compaction 起源（streaming flush / import）**清空** `IsInvisible` | stats → index |
| Mix compaction (`meta.go:2649`) | `State=Flushed`, `IsInvisible=false`, `CompactionFrom=inputs` | stats → index |
| Clustering compaction (`meta.go:2592`) | `State=Flushed`, `IsInvisible=true`, `CompactionFrom=inputs` | stats → index → per-segment visibility flips |
| Backfill / copy segment | V3 manifest-version update / new segment | index / awaited by external job |

Three inspectors drive the post-processing:

- `statsInspector` (`stats_inspector.go`): text/JSON-key stats tasks converge and
  write back.
- `indexInspector` (`index_inspector.go`): `getUnIndexTaskSegments` /
  `createIndexesForSegment` converge when `indexMeta.GetSegmentIndexState`
  reaches `Finished`.
- sort state is written directly by the compactor as `IsSorted` /
  `IsSortedByNamespace`.

### Read-Time Fallback in the Data View

`GetQueryVChanPositions` decides per segment, on the spot, whether it can serve
queries:

- `handler.go:184`: segments with `IsInvisible && CreatedByCompaction` are
  skipped entirely;
- `handler.go:226` `segmentIndexed`: a segment is ready if it has an index, or
  is (sorted && rows < `MinSegmentNumRowsToEnableIndex`);
- `handler.go:229` `fallbackParentReady`: the fallback target must be an indexed
  parent;
- `retrieveSegment` (`handler.go:251`): for each not-ready flushed segment,
  walk `CompactionFrom` upward to find ready ancestors to show instead, with
  fixed-point normalization for M:N outputs, partial ancestor coverage, and
  crash intermediate states.

`FilterInIndexedSegments` (`util.go:77`) re-applies index-readiness filtering on
the view side.

### Problem Summary

1. **Visibility = a function of async progress, not a committed fact.** Members
   of one batch (one import, one compaction) become ready and visible one by
   one; the QueryCoord next target differs between two polls. There is no
   writable, persistable carrier for "this batch of segments takes effect now,
   as a whole".
2. **The lineage DAG is consumed by the view algorithm.** `retrieveSegment`
   exists to paper over the non-atomic intermediate state of "output published,
   input not yet retired", forcing ancestor-coverage checks, blocked roots, and
   M:N overlap elimination. Lineage should be consumed once at publication time
   (retiring parents), not be a resident computation of the view.
3. **Three visibility mechanisms coexist**: `IsInvisible` (clustering), lineage
   fallback (mix/sort), `IsImporting` + `CommitTimestamp` (import).
4. **Readiness is not persisted**, so a restart re-approximates it by scanning
   async state, and the set drifts in between.

## The In-Flight DataView Entity: Alignment

### What It Already Provides (PR #52537)

`internal/dataview` (manager + recompute queue + `pkg/proto/view.proto`):

1. **Immutable collection snapshots** `DataViewOfCollection`: `Collection →
   VChannel → Partition → (SegmentID, ManifestVersion)` parallel packed arrays;
   every effective commit appends a new etcd key
   `coord/dv/{collectionID}/versions/{streaming}/{compact}`, with Ref/Deref
   reference counting + `GarbageCollect(retainLatest)` snapshot GC.
2. **Version semantics**: `DataVersion = (streaming_version, compact_version)`
   ordered lexicographically. `streaming_version` advances only via the flush
   atomic txn (`PrepareFlush`); every other membership change
   (compaction/import/copy/refresh/drop partition/truncate/L0 manifest advance)
   advances `compact_version` via `Recompute`.
3. **Atomic flush publication**: `UpdateSegmentsInfoAndDataView` (PR #52537)
   puts SegmentMeta actions and the DataView snapshot into the **same
   catalog.Update**, retrying catalog failures in-function until durable;
   `commit()/abort()` callbacks idempotently load the snapshot into memory or
   release the lock. Lock order: `DataView Collection lock → segMu`.
4. **The loadable projection is the visibility filter**:
   `meta.loadableProjection` returns only Flushed/Flushing, non-L0, non-importing,
   **non-`IsInvisible`**, healthy segments with a data footprint (binlogs or a
   StorageV3 manifest); the Manifest version is parsed from `manifest_path`, 0
   meaning "resolve via SegmentMeta watch" (indirect loading mode).
5. **Async convergence and recovery**: `Recompute` (per-Collection deduplicated
   queue + single worker) rebuilds the snapshot from the projection and does not
   write when content is unchanged (multiple pending mutations collapse into one
   snapshot write); `RecoverManager` reconciles every live Collection against
   SegmentMeta at recovery and bootstraps Collections that predate DataView
   management.
6. **Wiring points**: mix/sort/clustering/L0/bump-schema-version compaction,
   copy segment, import commit (`HandleCommitVchannel`), drop partition,
   truncate, and BatchUpdateManifest all call `meta.recomputeDataView(...)`
   (`dataview_adapter.go`) after their SegmentMeta commit; the worker rebuilds
   the snapshot async.

### The Batch-Change Atomicity Gap (where staging lands)

- **Batch-change visibility is still eventually consistent**: compaction/import/
  copy member flips go through async `Recompute` rebuilds, **not in the same
  txn** as the SegmentMeta commit; members of one batch and unrelated offline
  tasks may collapse into one snapshot, and publication order is
  nondeterministic.
- **Visibility flips are still scattered across task state machines**:
  clustering's `markResultSegmentsVisible` (per-segment
  `SetSegmentIsInvisible(false)`) + `markInputSegmentsDropped` remain two
  independent `UpdateSegmentsInfo` calls; mix/sort outputs are
  `IsInvisible=false` and still rely on the legacy view's lineage fallback
  (PR #52537 itself acknowledges the "invisible-while-published window is an
  acknowledged contradiction" in a `SaveBinlogPaths` comment).
- **The legacy view (`GetRecoveryInfoV2`) has not switched to DataView**: the
  DataView consumed by QueryView only serves the new query protocol
  (PR #52653); existing `GetRecoveryInfoV2` target building still goes through
  the `handler.go` lineage fallback.

### Alignment Principles

1. **The DataView snapshot is the single visibility carrier.** Staging does not
   invent a second "view". A group's `STAGED → READY → COMMITTED` transition is
   concretely "its members enter, and only enter, one committed DataView
   snapshot". The `loadableProjection` filters (non-`IsInvisible`, etc.) are
   exactly the staged-invisibility guarantee.
2. **Atomic batch publication = one DataView version advance + one SegmentMeta
   txn.** Add a batch-publish primitive to the DataView Manager, symmetric to
   `PrepareFlush`, that in one catalog txn commits "members `IsInvisible=false`,
   superseded parents `Dropped`, and a new `DataViewOfCollection` snapshot
   (`compact_version` +1)".
3. **`Recompute` remains the convergence/repair path**, not the default
   publication path for batch changes. Default: "group ready → atomic snapshot
   publish". `Recompute` is used only for: old-data bootstrap, crash
   reconciliation, and paths not yet staged (Phase-1 dual-track fallback).
4. **`streaming_version` respects DataView semantics**; staging never touches
   it. Any membership change outside flush advances `compact_version`, never
   racing the flush counter.

## Goals / Non-goals

### Goals

1. A batch change's visibility is **atomic**: all group members are visible or
   none are; no half-visible window.
2. **Staged / not-ready segments are invisible by construction**; no read-time
   lineage fallback.
3. Readiness is **decided and persisted at write time** at the group level;
   DataCoord restarts can replay it, and reads only filter.
4. Lineage (`CompactionFrom`) leaves the view algorithms and degrades to a
   one-time superseded-retirement semantics at publication + audit.
5. Reuse existing atomic primitives (`CommitSegmentManifests` /
   `UpdateSegmentsInfo` / `UpdateSegmentsInfoAndDataView`); no distributed txn.
6. Zero semantic change for the streaming path (Growing/Sealed/Flushed).
7. **Align with the DataView entity (PR #52537)**: visibility is expressed only
   through DataView snapshots; batch atomic publication = one `compact_version`
   advance + one SegmentMeta txn; `Recompute` degrades to convergence/repair.
8. **Do not break QueryView consumption**: staged segments never enter any
   committed DataView snapshot; QueryView (PR #52653) and legacy
   `GetRecoveryInfoV2` agree on the same visible set.

### Non-goals

- Change the execution model of the async sort/stats/index pipeline.
- Introduce distributed locks or distributed transactions across DataCoord
  leadership.
- Distributed commits between object storage and etcd (keep the existing
  manifest txn boundary).
- Unify the QueryCoord target subscription mechanism (only stabilize the
  DataCoord-side view).
- Modify existing DataView Manager semantics (`streaming_version` ownership,
  Ref/Deref, GC, recovery bootstrap, Manifest-version monotonicity) — only add
  one batch-publish primitive.

## Design: Segment Change Staging

### Core Model

Two persisted entities:

1. **Change group (`SegmentChangeGroup`)**: one atomic publication unit carrying
   the group state machine, member list, superseded-parent list, and commit_ts.
2. **Per-segment view state**: `SegmentInfo` extension for staged semantics,
   sharing one field with the streaming path.

#### 1.1 `SegmentChangeGroup` (datapb, new)

```proto
enum SegmentChangeState {
  STAGED    = 0;
  READY     = 1;
  COMMITTED = 2;
  FAILED    = 3;
  ABORTED   = 4;
}

enum SegmentChangeSource {
  SOURCE_IMPORT_JOB       = 1;
  SOURCE_SORT_COMPACTION  = 2;
  SOURCE_MIX_COMPACTION   = 3;
  SOURCE_CLUSTERING       = 4;
  SOURCE_FORCE_MERGE      = 5;
  SOURCE_STORAGE_VERSION  = 6;
  SOURCE_BUMP_SCHEMA      = 7;
  SOURCE_COPY_SEGMENT     = 8;
  SOURCE_EXTERNAL_REFRESH = 9;
  SOURCE_CDC_REPLICATED   = 10;
}

message SegmentChangeGroup {
  int64  group_id;              // AllocID
  SegmentChangeSource source;
  int64  collection_id;
  int64  partition_id;          // 0 = cross-partition (import usually)
  int64  source_job_id;         // import jobID / compaction planID / copy taskID
  SegmentChangeState state;
  repeated int64 new_segment_ids;         // current staged outputs; controlled sort replacement described below
  repeated int64 superseded_segment_ids;  // parents retired at publication
  // superseded_l0_segment_ids: the subset of superseded_segment_ids exempted
  // from the anti-duplication invariant as L0 delta segments at REGISTRATION
  // time (C11/C27). Persisted so recovery is stable: an L0 parent may be GC'd
  // from SegmentMeta later, and the persisted decision (not a live segment
  // lookup) must drive recovery — otherwise identical persisted bytes would
  // flip from conflict-free into a startup conflict once the L0 segment
  // disappears.
  repeated int64 superseded_l0_segment_ids;
  uint64 commit_ts;                       // allocated at publication; 0 while staged
  int64  create_ts;
  int64  ready_ts;                        // observability: staged -> ready duration
  int64  commit_time;
  string fail_reason;
  // Idempotency/replay helper: monotonic counter of the publish txn, used for
  // READY replay detection (see §7).
  int64  publish_epoch;
}
```

Write-time constraints (violation ⇒ `merr.WrapErrDataIntegrityMsg`):

- `new_segment_ids` and `superseded_segment_ids` are disjoint;
- `new_segment_ids` may be empty: compaction can retire parents with no output,
  and an empty import can complete as a no-op. Public state transitions cannot
  change membership. The private STAGED sort-materialization path may replace
  one member with its sort result; the JSON model preserves the producer's
  `initialSegmentIds` and records each plan/input/output in `sortJobs`. This is
  a composite group/output write with ownership checks, not a second group.
  Finalized zero-row outputs are retired rather than published.
- **an L1/L2 segment may be referenced — as a `new_segment` or as a
  `superseded` parent — by at most one ALIVE (`STAGED`/`READY`) group per
  collection.** This is the anti-duplication invariant: two alive groups that
  both replace the same parent would, on double publish, expose two visible
  outputs covering the same logical rows (data duplication). The compaction
  planner/inspector already forbids this; the group layer enforces it
  independently at registration, using two reverse indexes —
  `stagedSegmentToGroup` (members) and `supersededSegmentToGroup` (parents) —
  so no defensive gap. A terminal group releases its references.
- **L0 is the explicit exception**: L0 materialization is monotonic delta
  application to a shared L1 target and multiple L0 ops may reference the same
  segment. L0 does not stage in this design (see §6.2) and will be absorbed by
  the DataView delta watermark (`transform_start_after_timetick`) rather than
  by groups. A group MAY therefore list an L0 parent as `superseded`, and that
  exemption is persisted in `superseded_l0_segment_ids` at registration (C11):
  the uniqueness check skips it and recovery uses the persisted decision, not a
  live segment-level lookup, so GC of the L0 parent cannot flip identical
  persisted bytes into a startup conflict.
- **The L0 exemption is uniqueness-only, not retirement-only (chyezh#C)**:
  exemption covers the anti-duplication *claim* check, but an L0-exempt
  superseded parent must still be retired (marked Dropped) by the group's
  publish exactly like any other superseded — the publish failure matrix
  enforces it. A group superseding an L0 segment retires it on publication;
  skipping the drop would leave the L0 parent live alongside the published
  member.

#### 1.2 `SegmentInfo` extension (datapb)

```
int64 change_group_id = <new field>; // 0 = not a batch-change member
```

Conventions:

- For a segment with `change_group_id != 0`, `IsInvisible` may only be flipped
  by the owning group's publish txn; no other path may touch it (including the
  sort-enabled implicit flip in `SaveBinlogPaths`, see §6.1).
- After the group reaches `COMMITTED`, members keep `change_group_id` for
  diagnostics/audit; it can be cleared by `groupCleaner` once all superseded
  segments are retired.

> **Known limitation (F4, base PR scope)**: the `change_group_id` field is not
> yet landed on `SegmentInfo` (it requires a `datapb` regeneration). The
> publish-time ownership check (purpose 1) is equivalently enforced by the
> `stagedSegmentToGroup` reverse index, but the flip protection (purpose 2) is
> **unenforced until the field lands (Phase 2)**: nothing currently stops e.g.
> the sort-enabled `SaveBinlogPaths` implicit flip from touching a staged
> member's `IsInvisible`. This is accepted for the base PR and must be declared
> in the PR description.

#### 1.3 etcd key layout and catalog operations

```
datacoord-meta/segment-change-group/{collectionID}/{groupID}      -> SegmentChangeGroup (SaveSegmentChangeGroup)
coord/dv/{collectionID}/versions/{S}/{C}                          -> DataViewOfCollection (unchanged, PR #52537)
```

The group key lives under the `datacoord-meta` prefix like every other
DataCoord metadata entity (segments, imports, compaction tasks, snapshots,
...). The DataView key at `coord/dv` is a deliberate cross-coordinator
exception — the DataView manager is MixCoord-level and shared with QueryView —
and is NOT the convention for datacoord-internal state.

`metastore.UpdateAction` gains `metastore.SaveSegmentChangeGroup(group)` and
`DeleteSegmentChangeGroup(groupID)` actions so the group record can join the
**same catalog.Update** as `UpdateSegmentsInfoAndDataView` (§5.4).

Recovery order (`reloadFromMeta`):

1. Load all `datacoord-meta/segment-change-group` → group by collection;
2. Per collection: if CollectionMeta is Dropping/Dropped → delete all its group
   keys; otherwise reconcile per group state (§7);
3. Only after reconciliation may publish replay proceed (avoid visibility drift
   during reconciliation).

### State Machine (full table)

| # | Transition | Trigger | Persisted | Failure/crash point | Recovery action |
|---|---|---|---|---|---|
| 1 | (none)→STAGED | batch task output (pre-registration before import commit / at compaction output creation) | group + members `IsInvisible=true, change_group_id` (same txn) | group written but members not → orphan group | members missing at recovery → group FAILED |
| 2 | STAGED→STAGED | `groupInspector` periodic re-eval | none (re-eval not persisted) | — | — |
| 3 | STAGED→READY | `groupInspector`: all members satisfy §5.3 | group `state=READY, ready_ts` (independent catalog txn) | half write (etcd txn atomic, impossible) | recovery: READY → publish replay |
| 4 | STAGED/READY→FAILED | `groupInspector`: any member terminally failed / schema fence unconvergeable | group `state=FAILED, fail_reason` | crash | recovery: FAILED → member reclamation (§8) |
| 5 | STAGED→ABORTED | timeout (`SegmentChangeStagingTimeout`) / superseded externally dropped / collection drop | group `state=ABORTED` | crash | recovery: ABORTED → member reclamation |
| 6 | READY→STAGED | R5 readiness regression: publish Phase-2 re-check rejects a nonempty member whose data/index/sort/stats/schema readiness regressed after READY | group `state=STAGED, ready_ts=0` (independent catalog txn) | half write (etcd txn atomic, impossible) | recovery: STAGED → re-evaluate (§7) |
| 7 | READY→COMMITTED | `publishGroup` (§5.4) | **same txn**: group COMMITTED + member flips + superseded Dropped + DataView snapshot | §5.4 failure matrix | recovery: COMMITTED → keep |
| 8 | COMMITTED→(deleted) | `groupCleaner`: superseded all Dropped and no longer referenced by any snapshot | delete group key | crash | recovery: leftover COMMITTED kept then rescanned |

**Key invariant**: `READY → COMMITTED` persistence is atomic (group state +
SegmentMeta + DataView snapshot in one txn). There is therefore **no
"COMMITTED state lost" window**; replay logic only handles the
"txn committed but response lost" case (§5.4), using `publish_epoch`.

> **Atomicity boundary (F3, C28)**: the invariant above holds strictly only
> while the composite op count ≤ the store's `MaxTxnOps`. Above the limit,
> `txn.Commit` falls back to the chunked path: the non-commit ops (segment
> flips / superseded retirement) are flushed first in recorded order, and
> commit-marked ops land LAST as the visibility marker. The group record's
> commit-marker semantics are STATE-QUALIFIED (C13): only a TERMINAL group
> (`SaveSegmentChangeGroup` on COMMITTED/FAILED/ABORTED) is commit-marked and
> lands last; an ALIVE group (STAGED/READY) is a plain in-order Save that the
> composite write places BEFORE its staged members. A crash in the terminal
> case leaves the intermediate state
> "members visible but group still STAGED/READY" — the very state the
> "members visible but group still STAGED/READY" — the very state the
> same-txn argument excludes. The reconciler's READY probe must therefore
> explicitly handle this state (e.g. treat "members flipped but no COMMITTED
> evidence" as a publish that must be finalized or aborted), not assume it
> cannot occur. The per-group member cap (subgroup splitting, §6.8) bounds
> group size but does not eliminate the fallback for large publishes.

### Readiness Predicate (precise)

`groupInspector` evaluates every nonempty `new_segment_id`; all must pass to flip
STAGED→READY. It shares its source of truth with
`FilterInIndexedSegments`/`loadableProjection` (extract a shared predicate to
avoid write/read drift):

```
ready(segment) :=
    dataReady(segment)
 && (collection has no index requirement  OR  segmentIndexed(segment)  OR  smallExempt(segment))
 && (sortCompaction disabled  OR  collection.IsExternal()  OR  segment.IsSorted  OR  segment.IsSortedByNamespace)
 && statsReady(segment)
 && schemaReady(segment)
```

Per-term definition and edge cases:

- **dataReady**: `NumOfRows > 0`, `isFlushState(state)`, and has a data footprint
  (`len(Binlogs)>0 || ManifestPath!=""`). **Note**: members are still
  `IsInvisible=true` during STAGED→READY (flip happens at publish), so dataReady
  must **not** require `!IsInvisible`. This is a deliberate difference from
  `loadableProjection`: the projection filters "visible after publication", the
  predicate gates "eligible to publish".
- **No index requirement**: `!indexMeta.HasIndex(collection)` exempts the whole
  index term. **Alignment point**: today `FilterInIndexedSegments(..., 
  skipNoIndexCollection=false)` filters out all compaction children when
  `indexed` is empty (children invisible, parents already retired → data
  temporarily vanishes from the view). Staging uses the `true` semantics, which
  is a **behavior fix**; lock it with unit tests (for a no-index collection, a
  child may be published as soon as its data is complete).
- **segmentIndexed**: `indexMeta.GetIndexedSegments(collection, {id},
  targetFieldIds)` contains the segment, with `targetFieldIds` = all vector
  fields + (when `DVForceAllIndexReady`) all scalar-indexed fields. **Edge
  case**: a pure-scalar collection (no vector field) leaves `targetFieldIds`
  empty → must explicitly degrade to "all scalar indexes `Finished`", otherwise
  the term is always false.
- **smallExempt**: `(IsSorted || IsSortedByNamespace) && NumOfRows <
  Params.DataCoordCfg.MinSegmentNumRowsToEnableIndex`. Same as today.
- **sort term**: exempt when `enableSortCompaction()` (both
  `DataCoordCfg.EnableSortCompaction` and `EnableCompaction`) is false; exempt
  for external collections. **Edge case**: sort-compaction outputs are natively
  `IsSorted=true`; direct import segments (sort disabled) need no sorted flag.
- **statsReady**: exempt when the collection has no text-index requirement and
  no JSON-key-index requirement; otherwise requires the corresponding
  text/JSON-key stats artifacts to be recorded in SegmentMeta (including the
  committed V3-manifest completion markers). Text stats must exist for every
  required field; JSON-key stats must also use the current data format. Task
  scheduling eligibility is not evidence of completion: an unsorted segment
  still requires its artifacts when sort compaction is disabled. In that mode
  the stats inspector builds directly on unsorted ALIVE-group outputs;
  group-less segments retain their existing scheduling behavior. The existing
  external non-V3 JSON-key capability exemption is retained.
- **schemaReady**: `segment.SchemaVersion >= collection.SchemaVersion`
  (including the function-field materialization constraint). **Edge case (C23)**:
  a schema bump landing while a group is staged is UNCONVERGEABLE, not a wait —
  the schema-bump materialization only targets visible segments
  (`isSchemaBumpDataSegment` requires `!IsInvisible`, invariant R8), and staged
  members are invisible by construction, so nothing can advance their
  SchemaVersion. The readiness evaluation must therefore treat "member
  SchemaVersion < collection SchemaVersion while staged" as a FAILED/ABORTED
  condition (bounded by the staging timeout), not wait for a bump that can
  never target the staged members; otherwise the whole batch burns its timeout
  and is silently redone.

Excluded from readiness evaluation:

- **L0 segments**: never staged (§6.2).
- **Zero-row outputs** (C32, revised): a finalized zero-row result means **no
  output**, matching mix/sort compaction's Dropped result and import's skipped
  sort task. Before registration, producers may omit the segment entirely.
  After registration, retain its producer identity, skip its
  readiness terms, and mark it Dropped in the publish write; never add it to
  DataView or assign it a row commit timestamp. A pre-existing Dropped zero-row
  result is also valid. GC retains such a member's metadata while the group is
  ALIVE, preserving the no-output evidence across restart; after the group
  finishes it can delete the tombstone normally. Growing/Flushing/Importing zero counts
  are provisional and must wait for finalization. Missing members remain errors.
  All-empty groups still commit the superseded retirement; with no superseded
  parents they complete as no-ops without advancing DataView. Zero-row outputs
  do not abort the rest of a batch.
- **Members marked compacting**: `SetSegmentsCompacting` does not affect the
  predicate, but publish must re-verify the member is still healthy.

### Atomic Publish Transaction (`publishGroup`)

#### 5.4.1 Pseudo-code (locks and I/O points)

```go
// New dataview.Manager primitive, symmetric to PrepareFlush:
PublishChange(ctx, event ChangeGroupDataViewEvent)
  -> (view *viewpb.DataViewOfCollection, commit func(), abort func(), err error)

message ChangeGroupDataViewEvent {
  int64         collection_id;
  repeated LoadableSegment new_segments;   // ready group members (with ManifestVersion)
  repeated int64 superseded_segment_ids;
  bool          advance_compact_version;   // always true (staging only moves compact)
}
```

```text
publishGroup(ctx, g):
  # Phase -1: allocate commit_ts as early as possible (rootcoord TSO RPC, before
  #           holding any lock, so the network RTT never sits on a lock;
  #           TSO only increases, failures may retry with a fresh value)
  commitTs := allocTimestamp(ctx)

  # Phase 0: acquire manifest locks (same two-phase acquisition as
  #          CommitSegmentManifests, already implemented in #52537)
  locks := m.getSegmentManifestLocks()
  acquireSegmentManifestLocks(ctx, locks, sortedUnique(g.new_segment_ids))

  # Phase 1: DataView Collection lock (acquisition order: manifest -> DataView -> segMu)
  state, unlock := m.dataViewManager.lockStateForMutation(g.collection_id)
  if state == nil: return errCollectionDropped   # collection dropped -> ABORTED

  # Phase 2: validate (reads SegmentMeta under segMu.RLock)
  m.segMu.RLock()
  for id in g.new_segment_ids:
      seg := m.segments.GetSegment(id)
      must(seg != nil && seg.change_group_id == g.group_id && seg.IsInvisible)
      must(seg.SchemaVersion >= collSchemaVersion(seg.CollectionID))
      if finalized zero-row output: continue        # retired, not loaded
      must(ready(seg))                              # recheck all five terms
      must(manifest complete / binlogs non-empty)
      must(commitTs >= max(binlog.TimestampTo))      # existing invariant, §9.1
      must(index state re-checked in phase 2 is still Finished or exempt)  # §5.5 R5
  for id in g.superseded_segment_ids:
      must(seg exists && seg.state != Dropped)       # idempotent: already Dropped -> skip (not fail)
  m.segMu.RUnlock()

  # Phase 3a: prepare the DataView snapshot (inside the DataView Manager; reuses
  #           addSegments / monotonicity validation)
  changeView, commitView, abortView := m.dataViewManager.PublishChange(ctx, ChangeGroupDataViewEvent{
      CollectionID: g.collection_id,
      NewSegments:  loadableOf(g.new_segment_ids),   # parse versions from latest ManifestPath
      Superseded:   g.superseded_segment_ids,
  })                                                  # clone latest, compact_version +1

  # Phase 3b: generate each V3 member's manifest revision (holding manifest locks,
  #           object-storage I/O outside segMu, like CommitSegmentManifests stage 2);
  #           produce prepared revisions; skip non-V3 / no-manifest-change members.

  # Phase 4: one catalog txn (SegmentMeta + DataView snapshot + group state)
  ops := []
  for id in g.new_segment_ids:
      if finalized zero-row output:
          ops += UpdateStatusOperator(id, Dropped)
          continue
      ops += SetSegmentIsInvisible(id, false)
      # C5: commit_ts applies ONLY to sources whose rows carry a data timestamp
      # allocated at creation (import / CDC; normalized copy implementations): those rows become
      # "officially present" at commit_ts. Compaction outputs (mix/sort/
      # clustering/forcemerge) MUST keep CommitTimestamp=0 — their rows keep
      # per-input normalized timestamps, and a non-zero commit_ts makes segcore
      # treat every row as inserted at commit_ts, dropping any delete with
      # timestamps[i] <= commit_ts and resurrecting already-deleted rows
      # (MVCC/TTL shift).
      # Current file-copy workers set preserveMemberTimestamps and keep their
      # existing row/delete timeline; see §6.7.
      if source.usesPublishCommitTs(g) && !g.preserveMemberTimestamps:
          ops += UpdateCommitTimestamp(id, commitTs)
      ops += (optional) UpdateSegmentPartitionStatsVersionOperator(id, <planID>)
  for id in g.superseded_segment_ids:
      ops += UpdateStatusOperator(id, Dropped)        # superseded semantics; M:N see §6.3
  ops += publishSegmentManifestOperator(prepared...)   # revisions from 3b published in the same txn
  committed, err := m.UpdateSegmentsInfoAndDataView(ctx, changeView,
                append(ops, SaveSegmentChangeGroup(g.withState(COMMITTED, commitTs))...)...)
  if err != nil: abortView(); return err             # nothing persisted, back to STAGED
  if !committed: abortView(); return errAlreadyRetired  # DataView dropped meanwhile
  commitView()                                       # load snapshot into memory

  # Phase 5: release locks
  unlock(); locks.UnlockMany(...)
```

#### 5.4.2 Lock-order argument (deadlock refutation)

`publishGroup` acquisition order:

1. `segmentManifestLocks` (Phase 0, first) — `acquireSegmentManifestLocks`
   returns on failure without taking any other lock;
2. `DataView collection lock` (Phase 1, `lockStateForMutation`);
3. `segMu` (Phase 4, inside `UpdateSegmentsInfoAndDataView`).

Total order: `manifest locks → DataView lock → segMu`. Manifest locks precede
the DataView lock so that the potentially long manifest-lock wait (up to the
escalation threshold of 30s) never blocks the whole collection's flush/recompute
by sitting on the collection-level lock; while waiting, no collection-level
resource is held.

Cycle refutation — every path holding ≥1 lock, ordered by acquisition:

| Path | Acquisition order | Consistent with total order |
|---|---|---|
| `CommitSegmentManifest(s)` | manifest → segMu | ✓ (first two) |
| `Recompute` worker | DataView → segMu | ✓ (last two; never takes manifest) |
| flush `PrepareFlush`→`UpdateSegmentsInfoAndDataView` | DataView → segMu | ✓ (last two; never takes manifest) |
| single-segment stats/index | manifest → segMu (`CommitSegmentManifest` path) | ✓ |
| GC (`recycleDroppedSegments`, recycles only Dropped segment files) | segMu only (meta reads + object-storage deletes), no manifest/DataView | ✓ |
| `publishGroup` | manifest → DataView → segMu | ✓ (full order) |

**No path waits for a manifest lock while holding the DataView lock**
(Recompute/flush never take manifest), and **no path waits for DataView or
manifest while holding segMu** (`CommitSegmentManifests` takes segMu after
manifest and then acquires nothing further). Hence acyclic. Key guarantee: once
manifest locks are held, the DataView-lock holders (flush/recompute) hold it
only briefly (bounded catalog I/O) and never request manifest, so
`publishGroup`'s wait is bounded.

#### 5.4.3 Failure matrix and replay

| Step | Failure mode | System state | Handling |
|---|---|---|---|
| 0 | manifest-lock acquisition timeout / ctx canceled | untouched | retry / defer to next inspector round |
| 1 | collection dropped (state==nil) | untouched | group→ABORTED, reclaim members |
| 2 validate | member missing / externally dropped / `change_group_id` mismatch | untouched | group→FAILED (external drop is unconvergeable) |
| 2 validate | schema version behind | untouched | reject publish; regress to STAGED, then FAILED under C23 (staged rows cannot converge through visible-only schema bump) |
| 2 validate | superseded already Dropped | untouched | treated as idempotent, skip (not fail) — covers **same-group publish replay** and **external DDL racing the parent** (truncate/drop-partition dropped it first); it is NOT a path that legitimizes two alive groups sharing a parent, which is rejected at registration by the anti-duplication invariant (§1.1) |
| 2 validate | publish does not retire a superseded parent (chyezh#C) | untouched | group→FAILED. A COMMITTED publish must drop every superseded parent in the same write (unless it is already Dropped in meta): otherwise the parent and the now-visible member both cover the same rows — duplicated query results — and the terminal group can no longer repair it |
| 3 | `PublishChange` monotonicity conflict (manifest version pushed higher by L0 then re-read lower) | untouched | stale error, back to STAGED, re-read and retry |
| 4 | catalog txn fails (etcd error) | untouched (txn atomic) | in-function retry (same as flush semantics); still failing → abortView + STAGED |
| 4 | **txn committed but process crashed** (response lost) | COMMITTED (etcd is authoritative) | recovery sees `state=COMMITTED` → keep, no replay |
| 4 | `committed==false` (DataView dropped in the window) | SegmentMeta committed, snapshot not written | group→ABORTED, reclaim members; DataView already dropped, no consumer |
| 5 | `commitView()` in-memory load fails | etcd COMMITTED | recovery reconciles memory via `Latest()` (as in #52537) |

**Idempotency key**: `publish_epoch` increments with the txn. At recovery a
`COMMITTED` group is used as-is from the etcd record; **no cross-txn
compensating write** is introduced.

### Concurrency and Races

| Race | Participants | Outcome | Control |
|---|---|---|---|
| R1 publish ∥ publish (different groups, same collection) | two `publishGroup` | serialized by the DataView lock; second group bases on first's snapshot +1 | lock order |
| R2 publish ∥ Recompute (same collection) | `publishGroup` vs worker | serialized; projection and base read at the same serialized point, so no identical-content duplicate-version snapshot (projection reads SegmentMeta only after acquiring the DataView lock) | lock order + §7 |
| R3 publish ∥ single-segment stats/index/GC | no DataView-lock participant | manifest locks / segMu mutually exclude; GC only recycles Dropped, staged members unaffected | locks |
| R4 publish ∥ flush (same collection) | `publishGroup` vs `SaveBinlogPaths` | serialized by DataView lock; flush advances streaming_version, publish advances compact_version, lexicographically compatible | lock order |
| R5 groupInspector READY ∥ member index task failure | inspector vs index task | after READY a member's index state can regress (retry in flight) → publish Phase 2 re-checks `GetSegmentIndexState`; not Finished → abort back to STAGED | Phase-2 recheck |
| R6 member manifest advanced by L0 ∥ publish | L0 task vs publish | L0 target selection excludes unpublished group members. New L0 plans are deferred on channels with unpublished groups or active copy admissions; queued undispatched plans are cleaned without retiring delete inputs. Ordinary group-less invisible flush inputs remain eligible. | selector + admission guard |
| R7 superseded dropped externally (partition/truncate/collection) ∥ publish | DDL vs publish | publish treats superseded idempotently (already Dropped → skip); non-idempotent for `new_segment` → FAILED | §5.4.3 |
| R8 member re-selected for a second compaction ∥ publish | planner vs publish | Selectors and reservation exclude ALIVE-group members and parents, except that a single unsorted STAGED member can enter its owning group's Sort subtask. The exception is rechecked under `segMu`; group-less awaiting-sort flush inputs retain their old admission. | membership index + reservation |
| R9 two alive groups referencing the same L1/L2 segment (new or superseded) | registration vs registration | **rejected at `AddSegmentChangeGroup`** via the `stagedSegmentToGroup`/`supersededSegmentToGroup` reverse indexes (anti-duplication invariant §1.1); a registration race is serialized by segMu | invariant + §1.1 |
| R10 seek-position read ∥ staged | `getEarliestSegmentDMLPos` (`handler.go:583`) | **existing bug**: filters `IsImporting` but not staged members; a staged segment's DML position can pollute the channel-seek fallback. **Scope note**: this fix targets the LEGACY (non-dataview) view only — the dataview path (`loadableProjection`) already excludes staged members natively. Filter by **ALIVE-group membership** (`stagedSegmentToGroup` reverse index, exposed as `aliveStagedMembers`), NOT by blanket `IsInvisible`: sort-enabled flush's awaiting-sort segments (§6.1) are `IsInvisible=true` but not group members and must keep their growing/DML contribution — a blanket filter would regress the sort-enabled collection's seek. Applies to `getEarliestSegmentDMLPos`, `GetQueryVChanPositions`, `GetCurrentSegmentsView`. This is a deliberate deviation from the earlier "filter `IsInvisible`" wording, gated on O1 (§6.1): if O1 later makes awaiting-sort segments fully invisible, the filter widens to `IsInvisible` | ALIVE-group filter (§10) |
| R11 metrics/partition stats ∥ staged | none | clustering's `UpdateSegmentPartitionStatsVersionOperator` runs only at publish | publish txn |

### View Reading (delete the fallback)

`GetQueryVChanPositions` and `GetRecoveryInfoV2` become:

1. Filter: `IsImporting`, `IsInvisible`, empty segments, partition/channel
   filters (same as today).
2. **Delete `retrieveSegment` and its helpers** (`segmentIndexed`,
   `fallbackParentReady`, `allParentsReady`, ancestor coverage, blocked roots,
   M:N overlap elimination) entirely.
3. **Remove the view-side `FilterInIndexedSegments` call**: index readiness is
   guaranteed by the group READY predicate.

The new QueryView path (PR #52653) consumes the DataView snapshot directly:
`loadableProjection`'s filters (non-`IsInvisible`, non-importing, non-L0, with
footprint) already guarantee staged segments never appear in any snapshot, so
**the old and new query paths observe the same visible set**.

The lineage fields `CompactionFrom` / `CreatedByCompaction` are retained only
for: superseded retirement in the publish txn, GC reference counting, and
audit/observability. No view path walks lineage anymore.

### Mapping with Existing Mechanisms

| Current mechanism | After staging |
|---|---|
| clustering: `IsInvisible=true` + per-segment `markResultSegmentsVisible` (`compaction_task_clustering.go:545`) | outputs enter a group; `completeTask` calls `publishGroup`: one flip of all members + one DataView `compact_version` advance (the PR's `recomputeDataView` replaced by atomic publish) |
| mix/sort: `IsInvisible=false` + view lineage fallback | outputs `IsInvisible=true` enter a group; published as a whole by `publishGroup` after index convergence (replacing the PR's `recomputeDataView`) |
| import: `IsImporting=true` + visible right after commit (`ddl_callbacks_import.go` / `import_checker.go`) | import segments stay `IsImporting` (invisible); sort→stats→index convergence then `publishGroup` makes them visible at once (`HandleCommitVchannel`'s `recomputeDataView` replaced by atomic publish) |
| backfill: BatchUpdateManifest broadcast per segment (`services_commit_backfill.go`) | result/updated segments grouped; published as a whole after the schema fence passes |
| streaming flush | existing `PrepareFlush` + `UpdateSegmentsInfoAndDataView` (`streaming_version` +1); staging does not intervene; `SOURCE_STREAMING_FLUSH` is only a trivial-group abstraction |
| DataView `Recompute` (the PR's default batch publication) | downgraded to convergence/repair: old-data bootstrap, crash reconciliation, fallback for paths not yet staged |

### Failure and Reclamation

- **Staged-member reclamation**: members of FAILED/ABORTED groups are never
  published and are handed to the existing GC for object-lifetime cleanup
  (binlogs/manifest have no visible pointer and are recycled); superseded
  parents are unaffected.
- **Publish txn failure**: whole txn rolls back, group returns to STAGED,
  `groupInspector` re-evaluates with backoff and retries; duplicate-publication
  avoidance relies on the existing operator idempotency
  (`errIgnoredSegmentMetaOperation`).
- **DataCoord restart**: `reloadFromMeta` loads the group table; COMMITTED
  groups are kept, STAGED groups re-trigger readiness evaluation, READY groups
  replay the publish txn (idempotent, §7).

## Per-Path Adaptation and Edge Cases

### 6.1 Ordinary streaming flush (sort compaction enabled)

Today (master + #52537): `SaveBinlogPaths` sets `IsInvisible=true` on flushed
non-L0 segments **and** publishes them into DataView (advancing
`streaming_version`) — the "invisible-while-published" contradiction window.

After staging:

- sort compaction **disabled**: the original `PrepareFlush` path, member visible
  immediately (`SOURCE_STREAMING_FLUSH` trivial semantics, `streaming_version`
  +1); staging does not intervene.
- sort compaction **enabled**: preserve the existing synchronous publication
  and `sealed_at_data_version` binding. The raw flush remains "awaiting sort"
  (`IsInvisible=true`, no group); its sorted output enters a group that retires
  the raw input only on publish. `loadableProjection` retains raw invisible
  segments with a durable seal binding so an unrelated recompute cannot remove
  the already-published input while sorting/indexing proceeds. The output
  replaces it by advancing `compact_version`, not `streaming_version`.
- Deferring the raw flush's first publication remains a separate handoff change
  requiring `transform_start_after_timetick`; this implementation selects the
  compatibility branch of O1 and does not defer the SN handshake.

### 6.2 L0 — never staged

L0 is a delta segment with no sort/index; L0 compaction only advances target
segments' manifest versions without changing membership. Keep the #52537
behavior: `recomputeDataView` (content change → `compact_version` +1).
**No group**.

While a change group is unpublished, new L0 work on its channel is deferred.
Already queued, undispatched L0 tasks are failed and cleaned so they release
their planning slots without dropping their L0 inputs; the next planner pass
can retry them after publication. Pending or executing copy jobs also block
new L0 admission on their collection channels because copied rows preserve
their original timestamps. Existing groups keep this guard after opt-out.
This safety guard can starve L0 under continuous flush→Sort groups. The same
long-lived group reservations can starve clustering compaction for the same
partition/channel while sort and index work completes. A bounded wait alone
cannot safely release deletes: new outputs may not yet contain them. Production
opt-in requires a channel-level fairness rule that stops new group admission,
drains existing groups, and then runs queued L0 and clustering work. This
remains an open admission/scheduling requirement.

### 6.3 Mix / Sort compaction — deferred superseded retirement

Today: `completeMixCompactionMutation` / `completeSortCompactionMutation` drop
the inputs in the **same txn that creates the outputs**. After staging:

- outputs are created as `IsInvisible=true, change_group_id=g` with
  `superseded=inputs`; **inputs stay Flushed and visible** (which eliminates
  exactly the crash intermediate state the `retrieveSegment` comment describes,
  "compactTo published before compactFrom retired");
- inputs keep the `compacting` marker so the planner cannot re-select them
  before the group commits;
- on publish: outputs flip + inputs Dropped + snapshot +1 in one txn;
- **M:N**: both `superseded` and `new_segment_ids` are sets; the publish txn
  drops each input; the GC compactTo protection (`garbage_collector.go:865`)
  keeps inputs from being recycled before their output is eventually dropped —
  unchanged, no code change needed.

**Migration risk**: the current path couples input drop with output creation.
During Phase-1 dual-track, unstaged compactions keep today's semantics (inputs
dropped at output creation) while staged ones defer. **Both semantics coexist in
one collection**, so the `retrieveSegment` compatibility path must handle both
"staged output whose inputs are retired" and "output whose inputs are not yet
retired" — therefore the fallback algorithm is **not deleted in Phase 1**
(consistent with the parent design).

### 6.4 Clustering compaction — TMP→result chain

Today: `completeClusterCompactionMutation` creates `IsInvisible=true` results;
`completeTask` first `markResultSegmentsVisible` (per-segment flips) then
`markInputSegmentsDropped` (two independent txns). After staging:

- result segments get `change_group_id=g` with `superseded=tmp` (TMP segments,
  the intermediate clustering artifacts, retire alongside the inputs);
- `markResultSegmentsVisible` + `markInputSegmentsDropped` merge into the
  publish txn;
- clustering's `regeneratePartitionStats` stays as-is (stats file written
  first, `UpdateSegmentPartitionStatsVersionOperator` executed at publish).

### 6.5 Import (including the sort chain)

Today's chain: import segments (`IsImporting=true`) → sort-compaction outputs
(origin as `CompactionFrom`, origin then Dropped) → stats → index →
`HandleCommitVchannel` clears `IsImporting`.

After staging:

- Before primary CommitImport append, both sort-enabled and sort-disabled
  imports register their final private outputs and wait for readiness under
  the collection resource key. `awaitCommitFence=true` with `commitTS=0` prevents
  premature publication while async work runs. Existing import preparation/sort
  remains private before registration; retired originals and outputs already
  published by a partial legacy commit are omitted. After staging, `IsInvisible`
  plus group membership protects the outputs while `IsImporting` is cleared
  for async work.
- Each group uses its business vchannel's WAL commit timestamp, including CDC
  replay; it does not allocate a new row timestamp. The primary validates the
  readiness/schema fence before appending CommitImport. The callback assigns
  each group's WAL timestamp, then waits for publication before job completion.
  With staging opted in it also completes legacy jobs; per-channel SN RPCs are
  idempotent participants. CDC receivers may prepare their local groups only
  when the replicated commit arrives and then wait for local artifacts.
- **A durable WAL commit cannot be aborted by a staging timeout.** The WAL may
  be durable before its ack callback assigns any group's `CommitTS`, so the
  inspector never times out or reclaims an import group waiting for that
  assignment. Group registration alone does not stop the import job timeout:
  preparation can remain STAGED or fail schema validation before any WAL
  append. After all groups become READY, the primary durably marks the import
  job's `commit_broadcast_started` immediately before calling Broadcast.
  The marker and timeout/failure transitions use the import-job lock, so a
  timeout either wins before the marker (and prevents the append) or sees the
  marker and leaves the job intact. Explicit rollback is also rejected after
  the marker. A broadcast that never obtains an ack may need operator
  intervention; a future durable WAL outcome probe should restore an automatic
  liveness bound.
  Once the fence is assigned, groups retain data and retry publication.
  Missing outputs during recovery fail startup instead of reclaiming accepted
  import rows.
- **Large imports** split independent outputs into bounded groups per vchannel,
  using the first local output ID as group ID. `subgroupOrdinal` and
  `previousGroupID` persist the order. Every subgroup may become READY before
  WAL commit, but publication waits for its predecessor to become COMMITTED.
  Job completion waits for every subgroup. An already-admitted job finishes
  its remaining subgroups even after opt-out.

### 6.6 Backfill / BatchUpdateManifest — no group

Backfill and batch-update-manifest only **rewrite the content of existing
visible members** (V3: manifest version +1; V2: column-group upsert); no new
members. Keep `recomputeDataView` triggering (content change →
`compact_version` +1), **no group introduced**. V3 members' visibility is
guaranteed by the monotonic manifest-pointer advance (already in #52537); V2 by
the broadcast ack. **Edge case**: if a backfill targets a staged (invisible)
member, the backfill should not hit it (planner filters invisible, R8); if it
does (defensive), the publish txn's Phase-2 manifest-version check rejects the
lower version and the group returns to STAGED and re-reads.

### 6.7 Copy segment / external refresh / CDC

- copy segment: new segments, data complete; readiness speed depends on whether
  indexes are copied → enters a group (`SOURCE_COPY_SEGMENT`); if the copy
  already carries full index/stats it is ready immediately upon creation.
  The current worker copies files without normalizing historical row/delete
  timestamps. Such groups persist `preserveMemberTimestamps=true`: publication
  changes visibility/DataView while preserving each copied segment's existing
  commit timestamp. Retimestamping copied rows would suppress historical deletes.
  Worker-task completion releases its slot; copy-job completion waits for groups.
- external refresh: a membership-only update enters a group (`SOURCE_EXTERNAL_REFRESH`);
  `allowUnsorted` exempts the sort term (external tables have no PK ordering
  requirement). New segment admission and baseline retirement use the group;
  the refresh job stays InProgress after registration until group publication
  commits. Same-ID manifest patches, including mixed patch/membership jobs,
  keep the existing content-update protocol because a group cannot currently
  roll back a visible patch if publication fails. A failed refresh aborts
  unpublished new outputs. Atomic mixed content/membership publication is a
  separate protocol extension.
- CDC-replicated import: `SOURCE_CDC_REPLICATED`; commit_ts comes from the CDC
  source; the publish txn uses `max(source commit_ts, existing invariant)`.

### 6.8 Subgroup splitting threshold

`SegmentChangeGroup.MaxMembers` (new config, default e.g. 512): oversized task
output splits into multiple sequentially published groups (the previous subgroup
COMMITTED before the next publishes). `source_job_id` is shared;
`publish_epoch` is globally monotonic.

> **Subgroup splitting and superseded retirement (C6, C20)**: subgroup
> splitting applies ONLY to independent task outputs that own DISJOINT
> superseded sets. It NEVER splits the output set of a single M:N compaction,
> because every output of a compaction carries the FULL input set as
> `CompactionFrom` (`meta.go:2748`) — sharing that input set across subgroups
> is impossible under the anti-duplication invariant (a parent may be listed by
> at most one ALIVE group), and even if it were allowed it would create either
> a duplication window (earlier subgroups' members visible while the parent is
> not yet retired, and the read-time lineage fallback that used to hide a child
> whose parents are all present is deleted in §7) or permanent duplication
> (a later subgroup fails → the parent is never retired). A compaction is
> therefore exactly one group, and its superseded parents are retired
> atomically at that single group's publish; each subgroup publishes and
> retires only its own disjoint parents. The earlier "reference-counted
> retirement by last covering subgroup" formulation is withdrawn: it contradicts
> the anti-duplication invariant and cannot be implemented.

## Crash Recovery and Replay

`RecoverManager`/`reloadFromMeta` reconcile the group table per collection:

| group.state | Recovery action |
|---|---|
| STAGED | recompute readiness per member; members externally dropped → FAILED; rest stay STAGED |
| READY | **replay `publishGroup`**. Before replay, probe: are members actually visible (`IsInvisible=false`) with COMMITTED evidence (some DataView snapshot contains all members)? If yes → set the group COMMITTED directly (txn committed, response-lost case); otherwise re-execute the publish txn |
| COMMITTED | keep; `commit_time==0` (should not happen, defensive) → backfill in-memory state from DataView `Latest()` |
| FAILED/ABORTED | trigger member reclamation (§8) |

Replay safety:

- `publishGroup` idempotency relies on **the publish txn itself being atomic** +
  "probe before execute" at recovery: probing that members are already visible
  stops the replay, avoiding a duplicate `compact_version` advance.
- **C7 — the probe must never reclaim visible members.** Above `MaxTxnOps` the
  chunked fallback CAN leave "members flipped but group still STAGED/READY"
  (F3), so the probe has three outcomes, not two:
  1. members visible AND COMMITTED evidence (a DataView snapshot contains all
     members) → finalize the group as COMMITTED (response-lost case);
  2. members visible but NO COMMITTED evidence → the publish's segment chunk
     committed while the commit marker did not. Re-executing the publish is
     idempotent (member flips are no-ops, superseded already retired are
     skipped); the group must be finalized COMMITTED, NOT aborted — aborting
     would drop already-visible members while their parents may already be
     retired, losing both sides;
  3. members not visible → re-execute the publish txn.
  Reclamation (`UpdateStatusOperator(Dropped)` on members) is legal ONLY when
  no member is already visible; otherwise it is data loss. The earlier draft's
  "ABORTED + reclamation on probe failure" rule is therefore withdrawn.
- Recovery runs inside the recovery barrier (provided by #52537); external RPCs
  do not enter, so a half-recovered state is never read.

> **Known limitation (C24)**: recovery is fail-closed on cross-record conflicts
> (duplicate group IDs, member/superseded overlaps), which bricks `newMeta` and
> thus Coordinator startup. This assumes there is NO "persisted but reported
> failure" window — i.e. every group write either returns success (in-memory
> updated) or did not commit. A lost etcd response or a partial
> `commitFallback` chunk flush could leave an alive group that the running
> process cannot see; a subsequent batch would then legally claim the same
> superseded parent and persist a conflicting second group, bricking the NEXT
> restart. The base PR has no producers, so the window is unreachable today;
> the mitigation — the per-collection recovery disposition that marks a STAGED
> group whose members are missing as FAILED (reclaimable) instead of aborting
> startup — is part of the reconciler follow-up (M5).

> **Partial-publish atomicity (tedxu): resolved WITH DataView integration, out
> of this design's scope.** A COMMITTED publish whose composite op count exceeds
> the store's `MaxTxnOps` (etcd default 128) uses the chunked fallback: a crash
> mid-flush can leave some members `IsInvisible=false` (serving queries) and
> others still invisible, with no COMMITTED marker. On restart the group loads
> as STAGED/READY and the partially flipped segments keep serving queries —
> recovery today only detects persisted *conflicts* (two groups claiming the
> same segment), not a single group with partially-applied member visibility.
>
> This is the exact atomicity violation staging exists to prevent, and it will
> be **resolved together with the DataView integration (PR #52537)**, not in
> this design's scope: the composite write then composes a DataView snapshot
> action as the single visibility marker, and `UpdateSegmentsInfoAndDataView`'s
> atomic publish plus the DataView's committed-snapshot recovery make the
> chunked-fallback partial state unreachable. Until then the operator runbook
> for a DataCoord crash during a >128-op publish is:
>
> 1. Identify the ALIVE group whose members have mixed `IsInvisible` values
>    (query `coord/segchange/{collection}/{group}`; cross-check member
>    `IsInvisible` in SegmentMeta).
> 2. Do NOT re-run or retry the batch blindly — members already visible would
>    be duplicated.
> 3. Either drop the already-visible members' superseded parents by hand to
>    complete the publication, or mark the group FAILED and reclaim its
>    members; then delete the group record key.
> 4. Report the incident so the DataView integration can be prioritized.
>
> Terminal-group GC is wired into collection drop
> (`DropSegmentChangeGroupsOfCollection`) and a periodic inspector sweep after
> producer completion and segment retirement. The recovery-time
> member-visibility consistency check remains a reconciler follow-up (M5).

## GC and Staged-Member Reclamation

- **Staged (unpublished) nonempty members**: state is Flushed + `IsInvisible`.
  `recycleDroppedSegments` only recycles Dropped, so staged members are **never
  physically GC'd** — safe, but they still consume object storage and meta
  entries; long-tailed groups can leave files resident.
- **FAILED/ABORTED reclamation**: after the group state is persisted, each
  member is `UpdateStatusOperator(Dropped)` in an independent, per-segment
  retriable txn, then handed to the existing GC; V3 manifest cleanup goes
  through the existing dropped-segment path (nothing new needed).
- **Superseded Dropped at publish**: the GC compactTo protection
  (`garbage_collector.go:865`) waits until the output is also Dropped before
  recycling inputs — while a staged output is unpublished, inputs are Dropped
  but file-protected, **no data loss**.
- **DataView snapshot GC**: `GarbageCollect(retainLatest)` is independent of the
  group table; after a publish, old snapshots (without the new members) should
  become recyclable promptly, `retainLatest ≥ 1` suffices.
- **Snapshot export** (`handler.GenSnapshot`): export contains only published
  members. The candidate projection excludes unpublished group outputs while
  retaining their still-visible parents; restore therefore cannot copy both.
- **Terminal group cleanup**: the inspector sweeps terminal groups after each
  evaluation pass. It keeps the record while the owning compaction task,
  import job, copy task/job, or external refresh job can still retry. A
  COMMITTED group waits until all superseded segment records have been removed
  by the existing GC, which also enforces snapshot references. FAILED/ABORTED
  groups wait for unpublished members to be Dropped or removed. The cleaner
  then deletes the catalog key and in-memory entry; restart repeats the sweep.

- **Zero-row result metadata**: a Dropped zero-row member of an ALIVE group
  remains a durable no-output marker until the group terminates. `DropSegment`
  defers deleting this record; zero-row files can follow existing cleanup.
  Publish/recovery skip its manifest, stats and indexes and retire it rather
  than making it visible. A READY group with only zero-row/no output members
  completes parent retirement before DataView reconstruction even though there
  are no visible output members to probe.

## Temporal-Semantics Interactions

1. **commit_ts invariant**: the publish txn validates
   `commit_ts >= max(binlog.TimestampTo)` (aligning with the existing rule in
   `commit_timestamp_test.go`); import/CDC commit_ts is allocated by the group,
   covering `segmentEffectiveTs`/`segmentEffectiveDmlTs`
   (`segment_info.go:587`) semantics.
2. **GC time gate**: `recycleDroppedSegments` compares
   `segmentEffectiveDmlTs` against the channel cp — only for Dropped segments;
   staged segments do not participate.
3. **truncate / drop-by-time** (`DropSegmentsByTime`): batch Dropped by
   `flushTs`. **Edge case**: a staged member hit by truncate is set Dropped →
   the group's `new_segment` is missing → FAILED (§5.4.3); a superseded parent
   hit by truncate is skipped idempotently at publish. A truncate should trigger
   one group scan to converge quickly.
4. **channel seek / checkpoint**: `GetChannelSeekPosition` prefers the channel
   checkpoint; its fallback `getEarliestSegmentDMLPos` **must** exclude staged
   members, otherwise a staged segment's DML position misleads the seek. The
   exclusion is by **ALIVE-group membership** (`aliveStagedMembers`, the
   `stagedSegmentToGroup` reverse index), the same filter applied to
   `GetQueryVChanPositions` / `GetCurrentSegmentsView` (§5.5 R10). This is a
   legacy (non-dataview) view fix — the dataview path (`loadableProjection`)
   already excludes staged members natively. A blanket `IsInvisible` filter
   would also drop sort-enabled flush's awaiting-sort segments (§6.1), which
   are `IsInvisible=true` but not group members and legitimately contribute
   their growing/DML position; if O1 (§6.1) later makes awaiting-sort segments
   fully invisible, the filter can be widened to `IsInvisible`.

## Implementation Notes

### Producer prerequisites (2026-10-27)

The recovery, reclamation and ownership prerequisites are implemented before
enabling producers. The first producer slice below is opt-in.

- Startup reconciles groups **before** `dataview.RecoverManager` projects
  SegmentMeta or starts its worker. An entirely unpublished STAGED/READY group
  resumes through the inspector after startup. A missing/dropped member fails
  an entirely unpublished group and reclaims its remaining members. Collections
  with group records require a successful synchronous DataView rebuild before
  startup completes; the general recovery manager's best-effort rebuild alone
  is insufficient for this barrier.
- A READY group with visible members is an interrupted publish. Recovery
  completes its member flips and parent retirement before DataView recovery,
  without re-evaluating readiness. Import/copy/CDC member timestamps already
  written by the publish are preserved. Visible STAGED members, an incomplete
  partially published batch, inconsistent timestamps, or visible members of a
  FAILED/ABORTED group fail startup rather than reclaiming possibly-live data.
- The inspector retries FAILED/ABORTED member retirement; recovery retries it
  as well. Only invisible members may be reclaimed, parents are untouched,
  and a failed group cannot be deleted before its members are Dropped/GC'd.
- Legacy views/seek and the DataView projection exclude unpublished members,
  including terminal groups awaiting reclamation. Ordinary segment/manifest
  writes cannot expose an ALIVE member; only its owning COMMITTED composite
  write may do so.
- Sort/storage-version selectors exclude unpublished members and ALIVE
  superseded parents; the compaction reservation rechecks ALIVE claims under
  `segMu`. L0 selection excludes unpublished members, while still allowing
  delta application to visible parents. These prerequisite selector guards use
  group membership rather than a blanket `IsInvisible` exclusion, preserving
  existing group-less awaiting-sort behavior. This refines the broader C29
  selector proposal above without resolving O1 or changing flush semantics.

### Opt-in producer coverage (2026-10-28)

`dataCoord.segmentChange.enabled` defaults to `false` and is refreshable. It
controls admission of **new** groups only. Group recovery, readiness evaluation,
publication and reclamation run regardless of its value, so disabling it drains
existing groups instead of stranding their invisible outputs or releasing their
parents early.

- Mix compaction registers its output metadata and a STAGED group in the same
  composite write. The globally allocated plan ID is also the group ID, and
  `source_job_id` records the plan ID. This durable identity covers zero-output
  tasks too; retries consult it before the flag or input-health checks.
- Sorted and unsorted Mix results both enter the protocol. Sort admission has a
  narrow exception for unsorted STAGED members; all other compactions still
  exclude them. A sort result replaces its private input inside the same group,
  preserving initial producer IDs and per-sort identities for replay. Recovery
  also reclaims historical private sort inputs if registration was interrupted.
  Each inner Sort task completes and releases its planning slot once its output
  is registered; the root task continues waiting for group publication. Cleaning
  a completed inner Sort never aborts the root group.
- Input metadata stays live and reserved while the group is STAGED/READY.
  The task waits in `meta_saved`, which releases its worker execution slot.
  Group publication marks the inputs Dropped/Compacted and publishes the outputs
  in the DataView together; only then does the task complete. FAILED/ABORTED
  groups fail the task; explicit task cleanup aborts and reclaims unpublished
  outputs.
- Recovery recognizes the plan/group identity even when the task-meta save was
  lost after output registration. Active tasks resume `meta_saved` rather than
  reserving their own parents again or dispatching another worker. Terminal task
  decisions are preserved for cleanup. An empty output already
  collected after COMMITTED does not invalidate this identity.
- Flush keeps its synchronous version handshake; both Flush→Mix and Flush→Sort
  publish their replacement through a group and preserve SN retry bindings.
- Clustering registers final private results after its TMP/sort preparation.
  Its persisted group carries partition-stats publication metadata. The stats
  info, current-version pointer, member visibility, input/TMP retirement and
  DataView publication share the composite write. The version pointer is a
  final commit marker on the chunked path; zero-row outputs are omitted from
  the published stats set. Restart restores the clustering task at indexing.
- Import/CDC, copy and external refresh use the producer anchors described
  above. Import registers ordered bounded groups per vchannel; copy uses its
  task ID and bounds newly planned task size by the group limit; refresh uses
  its job ID. Retries find existing
  groups before consulting the flag, including after member GC.
  A copy task with an existing group resumes publication without re-running
  the file-copy worker, including when its task record is still in progress.
- Once the import WAL ack binds the commit timestamp, its callback publishes
  READY subgroups in ordinal order before releasing the collection resource
  key. The periodic inspector retries transient failures.
- Schema bump's full-rewrite replacement uses a group and waits in meta_saved;
  its in-place manifest/materialization form has no membership change and keeps
  its existing manifest transaction. Force-merge and storage-version rewrites
  routed through Mix inherit Mix staging. L0, backfill and BatchUpdateManifest
  remain content-only operations with no group.

Tests cover opt-in versus legacy behavior, the private sort chain, group/task
recovery after disabling the flag, nonempty/empty/no-output results, transient
catalog failure, missing stats, cancellation, L0 retention, snapshot protection and the flush
version handshake. These are coordinator integration/unit tests, not a running
cluster e2e validation of query results.

Periodic terminal-record deletion, codec migration and removing the legacy
lineage fallback remain follow-ups. The failure matrix follows C23 fail-fast
for nonempty staged members before a durable import fence; committed imports
instead retain data and retry, with corrupt recovery failing closed.
Publish rechecks all five readiness terms; finalized zero-row outputs are
retired as no output. See C32 for the revised empty-result semantics.

### New modules

```
internal/dataview/
  publish_change.go           // Manager PublishChange: clone latest + append members + compact_version +1
                              //   (symmetric with PrepareFlush; shares addSegments / monotonic check / commit-abort)
internal/datacoord/
  segment_change_meta.go      // SegmentChangeGroup CRUD + state machine (meta layer)
  segment_change_inspector.go // groupInspector: STAGED->READY evaluation + publishGroup trigger + timeout
  segment_change_group.go     // group lifecycle logic (publishGroup txn assembly)
  segment_change_reconciler.go // restart replay + association recovery with CompactionTask/ImportJob
```

### DataView wiring

- **Add `dataview.Manager.PublishChange`**: signature symmetric with
  `PrepareFlush` (`PublishChange(ctx, ChangeGroupDataViewEvent) (view, commit,
  abort, err)`), advancing only `compact_version`. Reuse the core logic from
  PR #52537 directly: `addSegments` Manifest-version monotonicity validation,
  `canonicalizeDataView`, `persistMemoryLocked`, and the idempotent
  commit/abort callbacks.
- **`meta.loadableProjection`** excludes unpublished group members, including
  failed/aborted groups awaiting reclamation. Raw invisible flush inputs with a
  durable seal binding remain projected until their replacement publishes.
  This projection fix also applies when the opt-in switch is off; it preserves
  already sealed flush inputs during a concurrent DataView recomputation.
- **Replace wiring points**: the `meta.recomputeDataView(...)` calls scattered
  across compaction / copy / import commit / BatchUpdateManifest in PR #52537
  are replaced path-by-path with "publishGroup once the staged group is ready";
  `recomputeDataView` is kept for paths not yet staged and crash reconciliation.
- **`change_group_id` reaches DataView snapshot consumers**: the QueryView
  builder (`QueryViewAtCoordBuilder`, PR #52653) loads by `LoadableSegment` and
  does not need to understand groups; legacy `GetRecoveryInfoV2` just filters by
  visibility.

### Group and existing task metadata association

- Compaction: `CompactionTask` already carries `InputSegments` /
  `ResultSegments` and a state machine. `SegmentChangeGroup` associates via
  `source_job_id=planID`; the two state machines are decoupled — compaction owns
  "data produced and correct", the group owns "view visibility". Clustering's
  `markResultSegmentsVisible`/`markInputSegmentsDropped` become members of the
  group's publish txn.
- Import: `ImportJob`'s state machine (Sorting/IndexBuilding/Uncommitted/
  Committing) aligns with the group state machine; `getIndexBuildingProgress` /
  `getStatsProgress` (`import_util.go:555/576`) can read group state directly,
  dropping the duplicated "scan meta to infer progress" logic.

### Single source for the readiness predicate

Extract the index-readiness decision from `FilterInIndexedSegments` into
`segmentReadiness.Prepare(collection) -> func(segment) bool`, shared by (a) the
groupInspector's write-time evaluation and (b) the legacy-read compatibility
path for group-less Flushed segments, so the two can never drift.

### Oversized groups

A configurable per-group member cap (e.g. import task or compaction output
scale). Batches above the cap are split into multiple sequentially published
subgroups to bound single-txn size and the "entire batch invisible" wait.

## Code Change List (including #52537 impact)

| File | Change |
|---|---|
| `internal/dataview/manager.go` | add `PublishChange` (§5.4); reuse `lockStateForMutation` |
| `internal/datacoord/meta.go` | composite group updates and `loadableProjection` group exclusion; retain the raw invisible flush while its sorted replacement is staged |
| `internal/datacoord/segment_change_*.go` | new files (§Implementation Notes) |
| `internal/datacoord/handler.go` | `getEarliestSegmentDMLPos` / `GetQueryVChanPositions` / `GetCurrentSegmentsView` exclude ALIVE-group members via `aliveStagedMembers` (R10; NOT blanket `IsInvisible` — see §6.1 awaiting-sort, gated on O1); view read path deletes `retrieveSegment` (Phase 2) |
| `internal/datacoord/compaction_task_mix.go` / `sort` / `clustering` / `bump_schema_version` | outputs enter groups, retirement deferred to publish, `recomputeDataView` replaced by publish trigger |
| `internal/datacoord/services.go` | `SaveBinlogPaths` preserves synchronous seal binding; `HandleCommitVchannel` joins staged import publication |
| `internal/datacoord/copy_segment_task.go` | completion enters a group |
| `internal/datacoord/compaction_policy_single.go` | allow Sort only for unsorted STAGED group members and ordinary group-less awaiting-sort flush outputs; reject other group members |
| `internal/datacoord/compaction_task_l0.go` / `compaction_inspector.go` | exclude unpublished members as targets; defer new and queued L0 on channels with unpublished groups or active copy jobs |
| `internal/datacoord/compaction_policy_storage_version.go` | segment selector gains a `!IsInvisible` exclusion (C29) |
| `internal/datacoord/garbage_collector.go` | unchanged (§8 protections already hold) |
| `internal/datacoord/snapshot_manager.go` | export/import filters staged members |
| `internal/metastore/kv/datacoord/*` | new key prefix + `UpdateAction` types |
| `pkg/proto/datapb` | `SegmentChangeGroup`, `SegmentInfo.change_group_id` |

## Metrics and Observability

- `datacoord_segment_change_group_state{state, source}` gauge;
- `datacoord_segment_change_staging_duration_seconds` (create→ready→commit
  percentiles);
- `datacoord_segment_change_publish_attempts/failures` (failure-matrix
  categories);
- `datacoord_dataview_version_count{collection}` (version-number burn
  observability);
- alert on groups stuck in STAGED past a threshold (paired with the
  unconvergeable detection of §5.3).

## Migration Path

Three phases, always keeping old data and old semantics compatible, with the
DataView entity (PR #52537) as prerequisite/parallel:

1. **Phase 1 — introduce the model, dual-track**: add the `SegmentChangeGroup`
   table and `change_group_id`; cluster compaction and import stage first (their
   `recomputeDataView` replaced by `publishGroup`); the DataView Manager gains
   `PublishChange` (zero change to `Recompute` semantics). The legacy view
   **keeps** the `retrieveSegment` lineage fallback as the compatibility path —
   Flushed segments without `change_group_id` (existing data, mix/sort outputs)
   stay visible under current logic. Behavior matches master + PR #52537.
 2. **Phase 2 — stage all compactions, delete the fallback**: mix/sort/forcemerge
   outputs become staged (their `recomputeDataView` replaced by `publishGroup`);
   the legacy view deletes `retrieveSegment` and `FilterInIndexedSegments`,
   filtering only by `IsImporting`/`IsInvisible`. Existing segments remain
   visible (treated as group-less, already committed); new changes all go
   through groups. The new QueryView path (PR #52653) already reads DataView
   snapshots and agrees with the legacy view.

   > **C30 — do NOT delete `retrieveSegment` until the no-group residue is
   > handled.** Today the clustering flow retires its inputs in a SEPARATE
   > step after publishing the outputs (a crash between the two leaves parent
   > AND child both `Flushed`, no group). `retrieveSegment`'s
   > "all direct parents present → keep parents, remove child" rule hides that
   > residue from queries; `recycleDroppedSegments` only walks Dropped
   > segments, so it never reclaims it. Deleting the fallback first would
   > expose both parent and child on every query — permanently duplicating the
   > children's rows. Phase 2 must either close the two-step window (retire
   > superseded atomically with output publication) or keep a normalization
   > that drops the still-live parents of a published child, BEFORE removing
   > `retrieveSegment`.
 3. **Phase 3 — unify and clean up**: backfill, copy segment, CDC onboard;
   `IsImporting` converges to an alias of `IsInvisible`; delete the per-segment
   flip logic in compaction tasks and the `Recompute` default publication path
   (keep only recovery/bootstrap fallback); merge the `CompactionTask` and group
   state machines into a single "task = data + publication" model; the
   DataCoord-side view is uniformly provided by DataView snapshots.

> **Group record encoding migration (C19)**: the group record is persisted as
> JSON today, with the model intended to become a `datapb` message (§1.1).
> Because `ListSegmentChangeGroups` propagates every decode error into a
> `newMeta` failure, a bare codec switch would brick every DataCoord on upgrade
> once live group records exist. Phase 3 must therefore carry an explicit
> encoding migration step — e.g. a `codec_version` field on the record that the
> loader uses to dispatch between encodings for one upgrade window (tolerating
> both, migrating on write), or keeping JSON as the stable on-disk format with
> the proto message used only in memory. This is declared here so it is not
> discovered at upgrade time like F4's `change_group_id` gap.

## Risks and Trade-offs

- **Higher whole-batch visibility latency**: group-atomic publication means
  all-or-nothing; a large batch's slowest member delays the whole batch's
  visibility. Mitigation: subgroup splitting, publishing subgroups in readiness
  order, observing `ready_ts - create_ts`.
- **Race with `Recompute`**: other offline tasks in PR #52537 may still trigger
  async `Recompute`. `publishGroup` and `Recompute` serialize on the DataView
  Collection lock, so members are never lost; but identical-content snapshots
  could be written twice (one wasted `compact_version`) or adjacent changes
  could collapse into one snapshot. Mitigation: `Recompute` does not write when
  content is unchanged (existing behavior); once all paths are staged,
  `Recompute` exits those paths.
- **Compatibility-read burden**: legacy Flushed segments have no group metadata;
  the view keeps a "group-less segments are visible by default" fallback until
  Phase 3 backfills (or accepts current-logic handling of group-less segments).
- **Predicate drift**: write-time and legacy read-time readiness must share one
  `segmentReadiness` source; otherwise "group says ready, view used to say
  not-ready" diverges. Lock with the shared predicate + compatibility-path unit
  tests.
- **Transaction size**: one oversized group's `UpdateSegmentsInfoAndDataView`
  write may exceed the etcd single-txn limit (snapshot + full SegmentMeta +
  manifest actions). Bound and shard publication like
  `segmentManifestLockEscalationThreshold`; also account for the full-snapshot
  rebuild cost (`Recompute` full projection) on very large collections.
- **Snapshot/replication/GC interaction**: snapshot export (`snapshot_manager.go`)
  and GC must filter staged members by visibility (staged segments neither enter
  exports nor count as referenced preconditions); DataView `GarbageCollect`
  protects referenced snapshots, so old versions pinned by a live `DataViewRef`
  are not collected (guaranteed by PR #52537).
- **Version-number burn**: each group consumes one `compact_version` advance;
  high-frequency small batches accelerate `compact_version` growth and snapshot
  key counts. Mitigation: merged subgroup publication, `Recompute` no-write on
  unchanged content, periodic old-version GC.

## Open Questions (require review adjudication)

- **O1 (compatibility branch selected)**: §6.1 preserves raw flush publication
  and the synchronous SN seal binding; the sorted replacement is staged.
  Deferring first publication still requires the separate QueryView/SN barrier.
- **O2**: during Phase-1 dual-track, "staged groups retire late" and "unstaged
  compaction retires immediately" coexist in one collection; must the
  `retrieveSegment` compatibility path keep its present-parents normalization
  heuristic? Recommended: keep it as-is, delete in Phase 2.
- **O3**: the pure-scalar-collection `segmentIndexed` degradation (§5.3) needs
  confirmation with the index team of `GetIndexedSegments`' empty-field
  semantics.
- **O4**: benchmark the group table's etcd volume/scan cost and the very-large-
  collection snapshot rebuild (`Recompute` full projection) in Phase 1.
- **O5**: is `publish_epoch` needed at all? (Equivalent alternative: the
  probe-already-visible check makes publish idempotent.) Recommended: no
  standalone field; recovery probing covers it. Keep the field only for
  observability.

## Appendix: Key Code Locations

| Concern | Location |
|---|---|
| View construction (flushed/unflushed/L0/dropped classification) | `internal/datacoord/handler.go:141` |
| Lineage fallback algorithm (fixed-point + M:N + blocked root) | `internal/datacoord/handler.go:251` (`retrieveSegment`) |
| `GetRecoveryInfoV2` (consumed by QueryCoord) | `internal/datacoord/services.go:1052` |
| Index-readiness filter (view read time) | `internal/datacoord/util.go:77` (`FilterInIndexedSegments`) |
| Atomic batch-publish primitive | `internal/datacoord/segment_manifest_commit.go:501` (`CommitSegmentManifests`) |
| Single-txn segment metadata update | `internal/datacoord/meta.go:2029` (`UpdateSegmentsInfo`) |
| DataView Manager (snapshot/Ref/GC/recovery) | `internal/dataview/manager.go` (PR #52537) |
| Flush atomic publication (SegmentMeta + DataView in one txn) | `internal/datacoord/meta.go` (`UpdateSegmentsInfoAndDataView`, PR #52537) |
| Loadable projection (visibility filter) | `internal/datacoord/meta.go` (`loadableProjection`, PR #52537) |
| DataView wiring (`recomputeDataView` forwarder) | `internal/datacoord/dataview_adapter.go` (PR #52537) |
| DataView protocol | `pkg/proto/view.proto` (`DataViewOfCollection` / `DataVersion`) |
| QueryView consuming DataView | `internal/views/queryclient` (PR #52653), `internal/querycoordv2` (PR #52432) |
| Import segments entering meta | `internal/datacoord/import_util.go:269` (`AllocImportSegment`) |
| Sort compaction completion | `internal/datacoord/meta.go:3454` |
| Mix compaction completion | `internal/datacoord/meta.go:2649` |
| Clustering compaction completion + visibility flip | `internal/datacoord/meta.go:2592`, `compaction_task_clustering.go:545` |
| Backfill commit | `internal/datacoord/services_commit_backfill.go:44` |
| commit_ts temporal semantics | `internal/datacoord/segment_info.go:587` |
