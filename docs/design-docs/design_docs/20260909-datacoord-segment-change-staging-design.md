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
| Sort compaction (`meta.go:3454`) | `State=Flushed`, inherits `IsInvisible`, `CompactionFrom=[origin]` | stats → index |
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
  repeated int64 new_segment_ids;         // staged members
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
| 5 | STAGED→ABORTED | timeout (`SegmentChangeStagingTimeout`) / superseded externally dropped / member zero-row dropped / collection drop | group `state=ABORTED` | crash | recovery: ABORTED → member reclamation |
| 6 | READY→COMMITTED | `publishGroup` (§5.4) | **same txn**: group COMMITTED + member flips + superseded Dropped + DataView snapshot | §5.4 failure matrix | recovery: COMMITTED → keep |
| 7 | COMMITTED→(deleted) | `groupCleaner`: superseded all Dropped and no longer referenced by any snapshot | delete group key | crash | recovery: leftover COMMITTED kept then rescanned |

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

`groupInspector` evaluates every `new_segment_id`; all must pass to flip
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

- **dataReady**: `isFlushState(state)` and has a data footprint
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
  `statsTaskMeta` `StatsSubJob` to have converged (text/JSON-key stats written
  back or already present in the V3 manifest).
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
- **Zero-row members**: created as Dropped, never enter a group; if a member is
  discovered zero-row after creation (import `createSortCompactionTask` today),
  it is removed from the group; an empty group → ABORTED.
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
      must(manifest complete / binlogs non-empty or zero-row already excluded)
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
      ops += SetSegmentIsInvisible(id, false)
      # C5: commit_ts applies ONLY to sources whose rows carry a data timestamp
      # allocated at creation (import / CDC / copy): those rows become
      # "officially present" at commit_ts. Compaction outputs (mix/sort/
      # clustering/forcemerge) MUST keep CommitTimestamp=0 — their rows keep
      # per-input normalized timestamps, and a non-zero commit_ts makes segcore
      # treat every row as inserted at commit_ts, dropping any delete with
      # timestamps[i] <= commit_ts and resurrecting already-deleted rows
      # (MVCC/TTL shift).
      if source.usesPublishCommitTs(g): ops += UpdateCommitTimestamp(id, commitTs)
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
| 2 validate | schema version behind | untouched | stay STAGED waiting for bump convergence (**not** FAILED) |
| 2 validate | superseded already Dropped | untouched | treated as idempotent, skip (not fail) — covers **same-group publish replay** and **external DDL racing the parent** (truncate/drop-partition dropped it first); it is NOT a path that legitimizes two alive groups sharing a parent, which is rejected at registration by the anti-duplication invariant (§1.1) |
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
| R6 member manifest advanced by L0 ∥ publish | L0 task vs publish | L0 only targets **visible** segments; staged members cannot be L0 targets; after publish the member is visible and L0 may push it higher → handled by `Recompute` with monotonicity | §5.4.3 |
| R7 superseded dropped externally (partition/truncate/collection) ∥ publish | DDL vs publish | publish treats superseded idempotently (already Dropped → skip); non-idempotent for `new_segment` → FAILED | §5.4.3 |
| R8 member re-selected for a second compaction ∥ publish | planner vs publish | planner excludes `IsInvisible` (`compaction_util.go:126`), staged members cannot be selected | invariant |
| R9 two alive groups referencing the same L1/L2 segment (new or superseded) | registration vs registration | **rejected at `AddSegmentChangeGroup`** via the `stagedSegmentToGroup`/`supersededSegmentToGroup` reverse indexes (anti-duplication invariant §1.1); a registration race is serialized by segMu | invariant + §1.1 |
| R10 seek-position read ∥ staged | `getEarliestSegmentDMLPos` (`handler.go:551`) | **existing bug**: filters `IsImporting` but not `IsInvisible`; a staged segment's DML position can pollute the channel-seek fallback | must also filter `IsInvisible` (§10) |
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
- sort compaction **enabled**: the flushed segment becomes `IsInvisible=true`
  but is **not** published into DataView (`change_group_id` stays 0, marked
  "awaiting sort" rather than a group member); the later sort-compaction output
  enters a group. **Note**: this changes the #52537 semantics of "flush
  synchronously advances streaming_version" — the advance is deferred until the
  sort output publishes, which directly affects StreamingNode's
  growing→sealed handoff watermark. That handoff depends on
  `transform_start_after_timetick` (explicitly unimplemented in #52537), so the
  correctness of this path depends on the QueryView/SN shard barrier landing.
  This is a prerequisite/parallel dependency of staging and **must** be listed
  as an open question (O1, §15), not silently changed.

### 6.2 L0 — never staged

L0 is a delta segment with no sort/index; L0 compaction only advances target
segments' manifest versions without changing membership. Keep the #52537
behavior: `recomputeDataView` (content change → `compact_version` +1).
**No group**.

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

- sort **disabled**: import segments become visible directly after commit
  (`HandleCommitVchannel`), stats/index async — **not staged** (no readiness
  gate, same as today).
- sort **enabled**: import segments stay `IsImporting=true` (invisible); the
  sort-compaction output enters a group (`superseded=import segment`); after
  stats→index convergence the group publishes as a whole and the superseded
  import segment retires. `HandleCommitVchannel`'s `recomputeDataView` is
  replaced by "publish if the vchannel's group is READY".
- **Edge case**: one import job has multiple tasks/vchannels → one group per
  task's sorted segments (or one group per job, depending on the txn-size
  threshold §6.8). Groups of the same job may publish in sequence (subgroup
  splitting), preserving import 2PC semantics (`WaitForIndex` and progress
  computation read group state instead of scanning meta).

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
- external refresh: enters a group (`SOURCE_EXTERNAL_REFRESH`);
  `allowUnsorted` exempts the sort term (external tables have no PK ordering
  requirement).
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

## GC and Staged-Member Reclamation

- **Staged (unpublished) members**: state is Flushed + `IsInvisible`.
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
- **Snapshot export** (`snapshot_manager.go`): export must contain only
  published members — staged (invisible) members must not enter the exported
  set; restore/import likewise. Add the filter in Phase 1.

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
   checkpoint; its fallback `getEarliestSegmentDMLPos` **must** filter
   `IsInvisible` (§5.5 R10), otherwise a staged segment's DML position misleads
   the seek.

## Implementation Notes

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
- **`meta.loadableProjection` unchanged**: staged segments (`IsInvisible=true` /
  `IsImporting=true`) are natively filtered, so even if `Recompute` fires during
  Phase 1/2 from other offline tasks it cannot project unpublished members early.
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
| `internal/datacoord/meta.go` | `UpdateSegmentsInfoAndDataView` supports `SaveSegmentChangeGroup` action; `loadableProjection` unchanged |
| `internal/datacoord/segment_change_*.go` | new files (§Implementation Notes) |
| `internal/datacoord/handler.go` | `getEarliestSegmentDMLPos` filters `IsInvisible`; view read path deletes `retrieveSegment` (Phase 2) |
| `internal/datacoord/compaction_task_mix.go` / `sort` / `clustering` / `bump_schema_version` | outputs enter groups, retirement deferred to publish, `recomputeDataView` replaced by publish trigger |
| `internal/datacoord/services.go` | `SaveBinlogPaths` sort-enabled branch: "awaiting sort, not published" (§6.1, subject to O1); `HandleCommitVchannel` triggers publish |
| `internal/datacoord/copy_segment_task.go` | completion enters a group |
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

- **O1 (blocking)**: §6.1 — sort-enabled flush's `streaming_version` semantics
  and the QueryView/SN growing→sealed handoff depend on
  `transform_start_after_timetick` (unimplemented in #52537). If changing
  `streaming_version` is rejected, sort-enabled flush segments must remain
  "published but invisible" (keeping #52537's current contradiction) and staging
  covers only compaction/import outputs. Pick one.
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
