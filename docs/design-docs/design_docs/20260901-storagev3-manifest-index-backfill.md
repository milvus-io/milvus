# StorageV3 Manifest Index Backfill and Rollback

- **Created:** 2026-09-01
- **Updated:** 2026-09-15
- **Status:** Design; implementation in the dependent stack layers
- **Component:** DataCoord, StorageV3
- **Depends on:** [StorageV3 Manifest Index Publication](20260811-storagev3-manifest-index-publication.md),
  [DataCoord Segment-Scoped Manifest Commit Framework](20260817-datacoord-segment-manifest-commit.md)

## Summary

`dataCoord.index.writeSegmentIndexToManifest` changes where a completed
StorageV3 index artifact is made durable. It does not by itself move indexes
that finished while the switch was off: those artifacts still have a finished
`SegmentIndex` row in etcd and no entry in their segment manifest.

The optional manifest index backfill migrates that historical state. For each
eligible record it uses one ordinary `CommitSegmentManifest` operation to:

1. publish the existing artifact as a typed manifest index entry;
2. advance `SegmentInfo.manifest_path` and set the
   `manifest_has_index` recovery marker; and
3. delete the historical `SegmentIndex` catalog row.

Those catalog changes are one atomic transaction. The finished record remains
in DataCoord memory and is reconstructed from the manifest after restart.

There is deliberately no index-prune task and no durable per-build migration
marker. Under the exclusive-placement design, the etcd row itself is the
backlog marker and the row is retired by the same transaction that publishes
its replacement. Separating publication and pruning would recreate a
read-then-delete window and require a second convergence protocol for no gain.

The independent rollback migration restores manifest-resident index records to
etcd before a downgrade that requires catalog-backed indexes. It retracts their
manifest entries in the same publication transaction. Records already retired
by GC are not recreated; their remaining manifest entries are only retracted.
Neither direction moves index files or changes artifact paths or build IDs.

| Direction | Source | Destination | Activation |
|---|---|---|---|
| Backfill | Historical finished `SegmentIndex` catalog rows | Manifest entries; corresponding catalog rows retired | Publication and backfill enabled, rollback disabled |
| Rollback | Current manifest entries and catalog-absent records retained in memory | Existing records restored to etcd; manifest entries retracted | Independent rollback switch overrides forward publication and backfill |

- [Backfill to manifest](#backfill-to-manifest)
- [Rollback to etcd](#rollback-to-etcd)
- [Verification](#verification)
- [Base and PR dependency order](#base-and-pr-dependency-order)

## Goals

- Move historical, usable StorageV3 index metadata from etcd to manifests,
  and restore manifest-resident records to etcd through an explicit rollback.
- Publish each placement change atomically with its segment manifest pointer.
- Reuse the foreground publication entry format, validation, lock order, and
  recovery path.
- Keep the migration opt-in, bounded, observable, crash-safe, and idempotent.
- Preserve mixed placement while the migration is incomplete.
- Leave task-only and legacy-storage records on their existing lifecycle path.

## Durable-placement Provenance

`indexMeta.segmentIndexCatalogAbsent` is a process-local set of build IDs whose
records came from, or were successfully moved to, a manifest. Its negative
shape is intentional:

- records loaded from `ListSegmentIndexes`, created in etcd, or rewritten into
  etcd are absent from the set;
- records recovered from a manifest, installed by copy/restore as
  manifest-resident, or published by foreground/backfill are present; and
- record removal clears the entry.

Unknown therefore means "assume the catalog row exists." A missed bookkeeping
update can cause one harmless idempotent publication/delete attempt, but cannot
silently exempt a real etcd row forever. The set is not persisted because the
durable source is re-derived exactly at each boot: etcd rows load first, and
manifest reload inserts only build IDs that etcd did not already supply.

This provenance is also what lets a successful migration converge without a
manifest read on every inspector tick. Removing the etcd row does not remove
the record from memory; marking its source manifest-resident is enough to take
it out of the next scan.

## Backfill to Manifest

### Non-goals

- Migrating StorageV1/V2 or L0 segments. They have no usable manifest index
  destination, so the etcd row remains their only durable record.
- Representing Unissued, InProgress, Retry, Failed, deleted, or fake-Finished
  task state in a manifest. A manifest describes an artifact, not a task.
- Backfilling an index whose definition has already been deleted. Existing GC
  owns that terminal record and its files.
- Reading every manifest to discover work. Durable placement is already known
  from the source that supplied each in-memory record.
- Automatically reversing placement when publication is disabled. Explicit
  [rollback](#rollback-to-etcd) uses an independent switch in a later stack layer.

### Candidate Definition

A record is eligible only when all of the following are true when the mutation is
staged under the publication locks:

- its segment is healthy, exactly StorageV3, and not L0;
- its collection, partition, and segment identity matches the segment receiving
  the manifest revision;
- its parent index definition is live;
- the record is the current `(segment_id, index_id)` occupant;
- it is Finished, not deleted, and carries at least one index file key; and
- DataCoord knows that its catalog row still exists.

The file-key test matches foreground publication. A Finished record with no
files is the small-segment/fake-Finished case and has no artifact to publish.
Filtering through `GetSegmentIndexes` excludes records already owned by
dropped-index GC. The commit repeats the mutable-record checks under the
BuildID lock so scan results are only hints, never authorization to publish.
An index definition may be dropped after staging; the record remains available
to GC, whose segment lock orders cleanup after publication.

A visible StorageV3 segment with an empty manifest path is an invariant
violation. Such a record remains counted as pending and its attempted entry
construction fails; silently excluding it would allow the operator-facing
completion signal to reach zero while a historical artifact was never moved.

### Commit Protocol

One candidate produces one `SegmentIndexBackfill` catalog mutation and one
typed `ManifestUpdates.Indexes` entry. The framework enforces that the update
contains the same BuildID and that the manifest entry is an exact projection of
the current task record.

The protocol is:

```text
segmentManifestLock(segmentID)
  -> keyLock(buildID)
     -> snapshot current segment and current finished record
     -> validate live definition/current slot/entry projection
     -> commit next immutable manifest revision
     -> segMu
        -> catalog.Update(
             AlterSegment(new manifest_path, manifest_has_index=true),
             DropSegmentIndex(historical row),
           )
        -> install new segment pointer and catalog-absent provenance in memory
```

The catalog refuses its chunked fallback whenever a `SegmentIndex` action is
present, so the pointer/marker and row deletion cannot become visible
separately. The record itself is not reinserted during install: it is already
the authoritative in-memory object, and reinserting a scan-time pointer could
overwrite a newer `(segment, index)` occupant.

Several selected records for the same segment are committed sequentially.
Different segments use a bounded worker pool. This preserves the framework's
single-record transaction and lock invariants, keeps every etcd transaction to
one segment write plus one row deletion, and avoids workers blocking one
another on the same segment lock.

### Crash and Retry Semantics

| Failure point | Durable result | Retry behavior |
|---|---|---|
| Before manifest commit | Old pointer and etcd row | Same record is selected again. |
| Manifest write succeeds, catalog transaction fails | Unreferenced immutable revision; old pointer and etcd row | Retry starts from the still-visible base. |
| Catalog transaction succeeds, process crashes before/after memory install | New pointer and no etcd row | Startup follows `manifest_has_index` and reconstructs the record from the manifest. |
| Same entry is already in the base manifest | Logical replacement by `index_id` | Safe, and the historical row is still retired atomically. |
| Segment or record becomes ineligible before staging | No mutation | Counted skipped; GC or the current task path owns it. |

No state exists in which the visible pointer contains the migrated entry while
the historical row is only planned for later pruning. That is the main
difference from the superseded dual-write design.

### Lifecycle Interactions

#### Foreground index completion

Foreground completion and backfill use the same segment and BuildID locks, the
same entry builder, and the same task-projection validation. Whichever commits
first changes the record provenance to manifest-resident; a later backfill scan
skips it. Redelivery of an already-finished worker result is an idempotent
replacement under the same manifest `index_id`.

#### Startup and failover

Startup first loads task/catalog records, then fail-closed reads every retained
healthy or Dropped, marked non-L0 StorageV3 segment manifest. Manifest entries whose BuildID was not
supplied by etcd are installed as Finished records and marked catalog-absent in
process. An unreadable or unusable marked manifest still aborts startup; the
backfill does not weaken that GC-safety contract.

#### Rolling upgrade and rollback

Both publication and backfill default off. They must be enabled only after
every DataCoord replica that can become leader runs a version that reloads
manifest-resident indexes. Otherwise an older leader would see neither an
etcd row nor the manifest entry as an index record.

After the first record is migrated, DataCoord must not be downgraded to a
version without manifest-index reload until the independent
[rollback migration](#rollback-to-etcd) has completed.
Disabling publication or forward backfill alone changes future work only; it
does not recreate retired etcd rows. Mixed placement remains supported by
manifest-aware DataCoord versions throughout the rollout.

#### Garbage collection

Backfill ignores a record after its index definition or segment is gone. GC
can observe an index-free manifest before backfill publishes an entry, then
reach record-only cleanup after publication. Record-only GC therefore acquires
`segmentManifestLock(segmentID)` and rechecks the observed pointer and marker
before deleting any files. If a healthy segment advanced, it leaves the files
and record for the next cycle, which resolves and retracts the new entry.

The lock spans file deletion and record removal; removal acquires the BuildID
lock in the same order as publication. This also allows removal of a
manifest-resident old build whose absence was verified in the unchanged current
revision (for example, after replacement). A blanket catalog-provenance guard
would strand that old build indefinitely. Existing batched manifest retractions
and cleanup of dropped/missing segments keep their merged lifecycle.
Dropped-segment GC needs no equivalent conditional path. A segment that becomes
unhealthy before the final publication check makes the backfill commit fail;
if publication wins first, dropped-segment GC reads the newly marked manifest
and includes its index files before deleting the segment directory and records.

#### Copy, restore, snapshots, and compaction

Copy/restore target indexes installed from the worker's verified manifest are
marked catalog-absent immediately and never enter the backfill queue. Targets
whose selected placement is etcd remain catalog-backed and can be migrated
later if they meet the ordinary predicate.

Snapshot metadata continues to pin build IDs and segments for GC; backfill
does not change artifact paths or build IDs, so it does not change snapshot
ownership. External snapshot restore retains the publication PR's existing
foreign-bucket behavior.

Compaction and stats commits share the per-segment manifest lock. Backfill
opens at the current pointer and therefore appends its entry to their latest
visible revision rather than publishing a sibling.

### Inspector, Limits, and Fairness

`manifestIndexBackfillInspector` is created with the other DataCoord
inspectors. It starts only when both restart-scoped choices are true:

```yaml
dataCoord:
  index:
    writeSegmentIndexToManifest: true
    manifestIndexBackfill:
      enabled: true
      interval: 60
      batchSize: 1000
      concurrency: 16
```

`batchSize` counts records, while `concurrency` counts segments. Both are
clamped to at least one; concurrency is also capped at `MaxInt32`, matching the
pool backend. Interval, batch size, and concurrency are refreshable. Enabling
the inspector and choosing manifest publication require a DataCoord restart.

Candidates are sorted by BuildID and each bounded scan starts after the last
selected BuildID, wrapping at the end. Thus one deterministic failure cannot
occupy the first batch forever and starve the rest of a large cluster.
The loop runs its first scan immediately so the pending gauge does not retain
Prometheus's default zero value for a full interval after DataCoord starts.

### Observability and Runbook

- `milvus_datacoord_manifest_index_backfill_pending_records` is the full count
  of currently eligible catalog-backed records before batch limiting. A canceled scan does not overwrite the gauge with a
partial count.
- `milvus_datacoord_manifest_index_backfill_records_total{status=...}` counts
  `success`, `failed`, `stale`, and `skipped` outcomes.
- The transition from a positive pending count to zero logs once.

Recommended rollout:

1. Upgrade every DataCoord replica that can become leader while both switches
   retain their defaults; do not enable migration while an older DataCoord can
   take leadership.
2. Enable `writeSegmentIndexToManifest` so new eligible completions use the
   exclusive manifest placement.
3. Enable `manifestIndexBackfill.enabled` and restart DataCoord.
4. Watch pending and failed outcomes. Zero means no *eligible historical
   catalog row* remains; task-only, legacy-storage, and GC-owned rows are not
   migration debt.
5. Disable the backfill inspector in a later restart if desired. No prune phase
   or additional destructive switch follows.

Once step 2 or 3 has published a manifest-only record, complete the independent
rollback migration before downgrading to a version without manifest-index
reload. Turning the forward switches off alone does not reverse migration.

Mixed placement is supported throughout and after this sequence. Disabling
foreground manifest publication later affects only new completions; the
segment marker keeps already migrated records recoverable from manifests.
GC clears that marker only after verifying that the resulting immutable
manifest contains no index entries, as implemented by #53048.

## Rollback to etcd

### Goal and compatibility boundary

Provide an independently enabled reverse migration for operators preparing to
run a DataCoord version that supports StorageV3 and the existing index artifact
path layouts, but does not recover index records from segment manifests.
Restore the durable `SegmentIndex` records required by that version, including
records on retained Dropped segments and records whose definitions were deleted.
Keep artifact paths, build IDs, files, segment state, and unrelated manifest
sections intact.

This does not downgrade StorageV3, index formats, or snapshot formats. The
target must understand the existing artifact layout and index engine version.
Manifest entries do not store the historical worker assignment or task timing;
recovered records retain those fields when available in memory, otherwise use
the existing Finished-record projection used during startup. No index rebuild
or artifact copy is required.

### Controls

```yaml
dataCoord:
  index:
    manifestIndexRollback:
      enabled: false
      interval: 60
      batchSize: 1000
      concurrency: 8
```

`enabled` is restart-scoped and defaults off. When enabled it takes precedence
over `writeSegmentIndexToManifest` and `manifestIndexBackfill.enabled`:
foreground completions and newly dispatched copy tasks choose etcd, and the
forward inspector is inactive. Operators need only this independent switch to
enter rollback mode. Before downgrading, also persist both forward switches as
false because the target version will not understand the new rollback switch.

`interval`, `batchSize`, and `concurrency` are refreshable. Batch size bounds
segments processed per scan; one visit migrates at most `maxEtcdTxnNum - 1`
index records. Different segments run concurrently, capped at 256 workers and
the amount of selected work. A rotating segment-ID cursor prevents one broken
manifest from starving later segments. Native read concurrency also uses the
existing process-wide manifest-read budget.

### Durable transition

```text
Before: SegmentInfo -> manifest with index; no SegmentIndex catalog row
After:  SegmentInfo -> manifest without index; durable SegmentIndex row
```

For each selected segment, acquire the existing segment manifest lock and
read the current manifest, then acquire selected BuildID locks in sorted order.
Validate segment identity, artifact paths, and the authoritative in-memory
records. A selected entry must match the current artifact when its only durable
source is the manifest. An already catalog-backed record wins a conflict,
preserving its current state, version and timestamps rather than replacing it
with historical manifest metadata. After active copy targets are excluded, a
record missing under its BuildID lock has already been retired. Retract its
remaining manifest entry without recreating a catalog row; index GC can leave
such entries on retained Dropped segments. Count only actual catalog PUTs as
restored records, after their publication succeeds.

Construct a structured revision with conditional `DropIndexes` entries using
both IndexID and ExpectedBuildID. Do not delete any artifact files. Reuse the
manifest commit framework for final publication:

1. Create the immutable revision from the locked source pointer.
2. Read back the resulting index section to determine `manifest_has_index`.
3. Under `segMu`, recheck source pointer, segment identity and retained state.
4. Atomically publish the segment pointer/marker and PUT all selected
   `SegmentIndex` records with `catalog.Update`.
5. Clear catalog-absent provenance only after the transaction succeeds. Preserve
   current in-memory objects and the current `(segment, index)` build slot.

Include catalog-absent superseded builds still retained in memory, even when
a replacement has removed their entry from the current manifest. Group these
records by segment and prove their absence in the same locked source read. If
the batch has no manifest entries to retract, a private Noop commit atomically
PUTs the records and the unchanged segment pointer, with the same source-pointer
checks. Public Noop commits continue to reject index-record mutations. This also
covers catalog-absent records on segments whose current marker is already false.

The rollback entry point prepares its retractions from a manifest read inside
the segment lock. It shares the locked commit implementation, without weakening
the public structured-commit rule against caller-supplied `ExpectedManifest`.
Only this entry point may perform rollback mutations or advance a retained
Dropped segment's manifest. Ordinary foreground publication on Dropped segments
continues to fail.

The metastore adds a PUT action for `SegmentIndexEntry`, using exactly the same
protobuf/key encoding as `CreateSegmentIndex`/`AlterSegmentIndexes`. Any action
set containing segment-index records still uses `CommitWithoutFallback`:
oversized transactions must fail without publishing a partial migration.
Rollback uses the record-only segment encoding, preserving existing binlog KVs
and keeping the transaction count at one segment PUT plus the index PUTs.

### GC, replacement, copy, and restart

Both record-only index GC and dropped-segment GC take the segment manifest lock
across file deletion and metadata retirement. Dropped-segment GC refreshes the
segment under that lock; it cannot remove a prefix while rollback is writing a
new revision into it. Index GC may have selected an old manifest revision;
conditional drops and BuildID locks keep retirement ordered, and rollback
never installs a stale record over a replacement build.

Catalog reload selects the highest BuildID for each `(segment, index)` slot,
independently of catalog key iteration order. Superseded records remain available
by BuildID for GC. A short memory-map lock serializes current-slot installation
and removal across different BuildID locks; removing an older record checks
the slot's BuildID and preserves its replacement. This lock covers no I/O.

Rollback includes retained Dropped segments, including snapshot-pinned parents.
Snapshot references continue to pin the same build IDs and physical files.
Historical immutable snapshot revisions remain untouched. Unreadable or invalid
manifests remain pending, including partially deleted Dropped segments; ordinary
GC can finish retiring those segments, after which they leave the backlog.
An empty but marked manifest is verified and its marker cleared without
creating any index row.

Copy tasks retain their dispatch-time placement. Rollback must not reinterpret
an existing worker result by changing that saved mode. The inspector obtains
active copy/restore targets before scanning segments, skips those targets, and
reports unfinished copy tasks separately. After their existing installation or
cleanup protocol finishes, their published segments are migrated normally.
This prevents rollback from racing a result installer between pointer adoption
and in-memory index installation, or reporting completion before an old task
publishes a manifest. New tasks use the effective etcd mode.

Result synchronization checks the current persisted task state under the copy
result lock. Repeated results for a completed task cannot reinstall a manifest
pointer that rollback has already advanced. Failed tasks are also rejected in
rollback mode, including historical tasks without a cleanup plan, so a late
result cannot invalidate a scan that already treated that task as terminal.
Rejected results continue to re-arm durable cleanup where the task still exists.

On restart, partial progress is derived from durable metadata: catalog rows
load first; remaining manifest entries recover normally. There is no separate
checkpoint to commit and no dual-write cleanup phase. A migrated segment with
an empty index section has `manifest_has_index=false` and needs no manifest
index read, matching the target version's catalog-only startup behavior.

### Completion and downgrade runbook

Expose bounded-cardinality metrics:

- `manifest_index_rollback_pending_segments`: all marked segments and segments
  with catalog-absent records, including unsupported/unreadable/temporarily
  blocked ones, before batch limiting;
- `manifest_index_rollback_pending_copy_tasks`: unfinished copy/restore tasks;
- `manifest_index_rollback_pending_records`: remaining catalog-absent records
  in memory, including inconsistent records without a marked segment;
- `manifest_index_rollback_ready`: 0 until an active complete scan observes all
  three counts at zero; reset on start/stop and before each scan;
- `manifest_index_rollback_records_total`: successfully restored records;
- `manifest_index_rollback_segments_total{status=success|failed|stale}`:
  segment migration attempts by outcome.

Readiness requires a subsequent complete scan after the final batch. A failed
or canceled scan never reports ready. Zero is a migration observation, not a
cluster-wide version gate: verify every potential leader is running rollback
mode and prevent new external manifest adoption while preparing the downgrade.

1. Deploy this version to every potential DataCoord leader and enable rollback.
2. Quiesce copy/restore and external data-manifest adoption jobs during the
   downgrade window. Existing copy tasks finish under their captured mode.
3. Wait for `ready=1` and all pending gauges to reach zero. Resolve failed
   manifests or let ordinary GC finish terminal cleanup; do not bypass failures.
4. Persist `writeSegmentIndexToManifest=false` and forward backfill disabled.
5. Restart the current version and verify the catalog-only recovery boundary,
   then roll to the intended StorageV3-compatible target version.

The rollback switch remains active until disabled in a later restart. Disabling
it does not re-migrate anything; forward migration resumes only if the separate
forward choices are enabled. Existing external stale-manifest adoption issues
#53328/#53329 are not a reason to bypass the source-pointer checks and must be
quiesced for either migration direction.

## Verification

Run all build, formatting, generation, and tests inside the repository's
Milvus development container with the Conan cache mounted. Validate the full
DataCoord package, the affected catalog/configuration packages, and focused
race tests. Go race does not instrument native C++/Rust operations.

A real local packed-manifest round trip should verify that artifact paths and
records survive metadata restart. Unit/fault-injection results do not establish
cluster failover, snapshot restore, QueryNode end-to-end acceptance, or actual
target-binary downgrade acceptance.

### Backfill

- Final-state test asserts the same artifact moves from etcd-only to
  manifest-only, remains present in memory, and survives a full metadata
  restart from the manifest.
- Catalog failure injection asserts the pointer and row deletion are both
  hidden while an orphan immutable revision is harmless and retryable.
- Switch tests cover both independent gates.
- Predicate tests exclude task-only, fake-Finished, deleted, and already
  manifest-resident records.
- Copy/restore installation seeds manifest-resident provenance and is not
  mistaken for historical etcd work.
- Bounded scans rotate across BuildIDs while reporting the entire backlog.
- Commit validation rejects a backfill mutation without its matching entry.
- Candidate tests cover healthy exact-StorageV3 selection, reject L0/legacy/
  dropped segments, and reject a catalog record whose segment identity is
  inconsistent with the manifest target.
- The GC stale-observation test preserves files and the manifest-resident
  record so the next cycle can finish ordinary retraction and clear the marker.

Additional failure-driven coverage:

- manifest write failure and catalog transaction failure, with retry;
- pointer movement and segment drop while manifest I/O is in progress;
- changed task projection, replaced build, and deleted index definition after
  candidate selection;
- GC selection before migration, followed by manifest-aware retirement;
- both path layouts, multiple indexes on one segment, and restart with
  publication disabled;
- bounded segment concurrency and cancellation/draining on shutdown.

Shutdown cancels new segment groups and drains every started group before
returning. Native manifest calls already in progress are synchronous and cannot
be interrupted by the Go context; shutdown waits for those calls to return.

### Rollback

Cover source read failure, invalid paths/identity, manifest write failure,
read-back failure, catalog transaction failure and transaction-size rejection.
At every failure, check both the durable pointer and every involved catalog
row, then retry or restart. Test source pointer advancement and segment drop
during I/O; conditional build replacement; existing catalog rows with newer
task state; multiple indexes over several atomic batches; deleted definitions;
retained Dropped/snapshot-pinned segments; GC selected before/during rollback;
GC-retired records whose entries still need retraction, including all-retired
and mixed batches, catalog failure/retry, readiness, and restart without
recreating the retired records;
superseded build restoration and retirement; late copy installation and repeated
completed/failed results; cancellation and bounded worker concurrency.

Use real packed manifests for both supported artifact layouts and verify the
same bytes remain readable, other manifest sections survive, forward/reverse
round trips converge, and a full metadata reload succeeds with manifest reads
disabled after migration. That last test models the target's catalog-only index
recovery boundary; actual target-binary/cluster acceptance is recorded separately.

## Base and PR Dependency Order

This design targets merged [PR #53048](https://github.com/milvus-io/milvus/pull/53048),
merge commit `240b9c0a916f9633da53230e58854da08d8412f8`, on master snapshot
`2e36c6ff0151b32c187e4c32d40f76cbc8dd6379`. Its `SegmentIndexes` mutation slice,
batched GC retractions, bounded recovery, verified marker clearing, and durable
copy cleanup are the baseline.

1. **Design only** (`feat/datacoord-manifest-index-backfill-design`, targets
   master): goals, protocol, rollout, failure cases, and validation plan in this
   document.
2. **Complete historical index migration**
   (`feat/datacoord-manifest-index-backfill`, targets the design branch):
   inspector, catalog provenance, atomic backfill mutation, GC integration,
   configuration/metrics, and regression tests as one independently testable
   capability.
3. **Complete reverse migration** (`feat/datacoord-manifest-index-rollback`,
   targets the forward implementation): independent switch, atomic restoration
   to etcd, GC/copy coordination, completion checks and tests, as specified in
   the [rollback protocol](#rollback-to-etcd).

Each backfill supplies exactly one `SegmentIndexBackfill` in
`SegmentCatalogMutation.SegmentIndexes`. It must satisfy the same publication
validation as a foreground upsert; multi-record publication and Noop adoption
are rejected. Existing multi-record GC removals retain their merged contract.

This migration concerns historical **index metadata**. Spark/add-field backfill
and external refresh can adopt independently generated data manifests through
`UpdateManifestVersion`; their stale-base adoption races remain separate work
([#53328](https://github.com/milvus-io/milvus/issues/53328),
[#53329](https://github.com/milvus-io/milvus/issues/53329)). Operators must not
overlap those jobs with index publication or this migration on the same segment.
A structured backfill commit detects a pointer changed during its own I/O and
retries from the new base; it does not fence a later external adoption.
