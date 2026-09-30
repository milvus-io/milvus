# StorageV3 Manifest Index Backfill and Rollback

- **Created:** 2026-09-01
- **Updated:** 2026-09-29
- **Status:** Under Review; design and implementation in one PR
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
etcd. It retracts only their matching manifest entries in the same publication
transaction. Manifest entries without in-memory records are left to GC.
Neither direction moves index files or changes artifact paths or build IDs.

| Direction | Source | Destination | Activation |
|---|---|---|---|
| Backfill | Historical finished `SegmentIndex` catalog rows | Manifest entries; corresponding catalog rows retired | Publication and backfill enabled, rollback disabled |
| Rollback | Manifest-resident index records retained in memory | Existing records restored to etcd; manifest entries retracted | Independent rollback switch overrides forward publication and backfill |

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

`model.SegmentIndex.ManifestPublished` is a process-local flag indicating that
this build was recovered from, or successfully published to, a manifest and
has no catalog row:

- records loaded from `ListSegmentIndexes`, created in etcd, or successfully
  rewritten into etcd have `ManifestPublished = false`;
- records recovered from a manifest, installed by copy/restore as
  manifest-resident, or published by foreground/backfill have it set to `true`;
- superseded builds retain the flag until written back to etcd or removed,
  even if their entry is no longer in the current manifest.

The zero value conservatively means "assume the catalog row exists." The flag
is cloned with the in-memory record but never serialized to protobuf. At boot,
etcd rows load first, and manifest reload inserts only build IDs that etcd did
not already supply. Placement changes install a cloned record only after the
catalog transaction succeeds, preserving any newer build's current index slot.

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
  [rollback](#rollback-to-etcd) uses an independent switch.

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
The scan starts from index records, rejects manifest-resident and task-only
records before looking up their segments, and checks the live definition and
current slot to exclude dropped-index GC and superseded builds. The commit
repeats the mutable-record checks under the
BuildID lock so scan results are only hints, never authorization to publish.
An index definition may be dropped after staging; the record remains available
to GC, whose segment lock orders cleanup after publication.

A visible StorageV3 segment with an empty manifest path is an invariant
violation. Such a record remains counted as pending and its attempted entry
construction fails; silently excluding it would allow the operator-facing
completion signal to reach zero while a historical artifact was never moved.

### Commit Protocol

Each candidate produces one `SegmentIndexBackfill` catalog mutation and one
typed `ManifestUpdates.Indexes` entry. Candidates for the same segment share
one manifest revision and one atomic catalog transaction, within the transaction
operation limit. The framework enforces that each update contains the same
BuildID and that the manifest entry is an exact projection of the current task
record.

The protocol is:

```text
segmentManifestLock(segmentID)
  -> keyLock(all selected BuildIDs, sorted)
     -> snapshot current segment and all selected finished records
     -> validate live definition/current slot/entry projection
     -> commit next immutable manifest revision
     -> segMu
        -> catalog.Update(
             AlterSegment(new manifest_path, manifest_has_index=true),
             DropSegmentIndex(each selected historical row),
           )
        -> install new segment pointer and catalog-absent provenance in memory
```

The catalog refuses its chunked fallback whenever a `SegmentIndex` action is
present, so the pointer/marker and row deletion cannot become visible
separately. Installation updates the locked record through a clone with
`ManifestPublished = true`. It updates the `(segment, index)` slot only if the
slot still belongs to that BuildID, preserving a newer occupant.

All selected indexes of a segment are published together when they fit in one
transaction. Only groups exceeding `maxEtcdTxnNum - 1` records are split into
sequential batches, reserving one operation for the healthy segment pointer PUT.
Different segments use a bounded worker pool. A failed batch publishes none of
its catalog mutations or in-memory placement flags and is retried on a later
scan; other batches and segments can continue. Every record is revalidated
under its BuildID lock before manifest I/O. Mixed foreground/backfill mutations
and duplicate index IDs in a backfill batch are rejected.

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

#### Placement controls

Both publication and backfill default off. Disabling either changes future
work only; it does not recreate retired etcd rows. The independent
[rollback migration](#rollback-to-etcd) restores manifest-resident records to
etcd. Mixed placement remains supported throughout either migration.

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

`batchSize` is a target record budget, while `concurrency` counts segments.
Selection always includes a whole segment, so the final selected segment may
exceed the remaining record budget. Both are clamped to at least one;
concurrency is also capped at `MaxInt32`, matching the
pool backend. Interval, batch size, and concurrency are refreshable. Enabling
the inspector and choosing manifest publication require a DataCoord restart.

Candidate groups are sorted by SegmentID and each scan starts after the last
selected SegmentID, wrapping at the end. Records within each group are sorted
by BuildID. Thus one deterministic failure cannot occupy the first batch
forever and starve the rest of a large cluster.
The loop runs its first scan immediately so the pending gauge does not retain
Prometheus's default zero value for a full interval after DataCoord starts.

After a complete scan finds no eligible records, periodic scanning stops. The
inspector waits only for shutdown or a coalesced index-record notification; no
segment-completion flag or completed-segment set is maintained. A successful
catalog write that installs a finished, artifact-bearing record sends that
notification after publishing the record in memory. This includes late legacy
copy/restore results and rewrites of previously manifest-resident records.
Notifications arriving during a scan survive its transition to idle. While
work remains, retries and later batches follow the configured interval.

On restart, the immediate scan derives work from the recovered records and
`ManifestPublished` provenance. Atomic catalog-row deletion remains the durable
migration progress; task-only rows need not disappear before scanning stops.

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
5. Periodic scanning stops automatically when no eligible records remain.
   New catalog-backed finished records wake it again. Disable the inspector in
   a later restart if desired; no separate prune phase follows.

Mixed placement is supported throughout and after this sequence. Disabling
foreground manifest publication later affects only new completions; the
segment marker keeps already migrated records recoverable from manifests.
GC clears that marker only after verifying that the resulting immutable
manifest contains no index entries, as implemented by #53048.

## Rollback to etcd

### Goal and scope

Provide an independently enabled reverse metadata migration. Restore existing
in-memory manifest-resident `SegmentIndex` records to etcd, including records on
retained Dropped segments and records whose definitions were deleted. Keep
artifact paths, build IDs, files, segment state, and unrelated manifest sections
intact. No index rebuild or artifact copy is required.

Rollback does not discover work by enumerating manifest entries. Entries with
no in-memory record belong to GC; rollback neither restores nor retracts them.
Version downgrade and old-version readability are outside this migration's scope.

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
enter rollback mode.

`interval`, `batchSize`, and `concurrency` are refreshable. Batch size bounds
segments processed per scan; one visit migrates at most `maxEtcdTxnNum - 1`
index records. Different segments run concurrently, capped at 256 workers and
the amount of selected work. A rotating segment-ID cursor prevents one broken
manifest from starving later segments. Native read concurrency also uses the
existing process-wide manifest-read budget.

Like forward backfill, rollback discovers ordinary work from in-memory index
records and groups it by SegmentID. Its predicate is `ManifestPublished=true`;
superseded builds and dropped definitions are retained because their catalog
rows must also be restored. Catalog-backed records are skipped before segment
lookup. No segment-marker sweep follows the record scan, and copy task state
does not participate in candidate selection or readiness.

Both inspectors use the same scan/retry/idle loop. Rollback continues periodic
checks while manifest-resident records remain. After an empty scan it stops the
timer and waits for record notifications. Installing a manifest-resident record
clears readiness and wakes the inspector. Restart always performs an immediate
scan from recovered placement metadata.

### Durable transition

```text
Before: selected record is manifest-resident, with no catalog row
After:  selected record has a catalog row; only its manifest entry is removed
```

For each selected segment, acquire the existing segment manifest lock and
resolve only the selected BuildIDs in the current manifest. This read determines
which selected records still have an entry; it never adds records to the batch.
Acquire selected BuildID locks in sorted order and revalidate the records.
If a record disappeared or became catalog-backed, abandon the batch before
manifest publication and let the next scan select the remaining work. Validate
segment identity, artifact paths, and the selected records' manifest projection.
Unselected manifest entries remain untouched.

Construct a structured revision with conditional `DropIndexes` entries using
both IndexID and ExpectedBuildID. Do not delete any artifact files. Reuse the
manifest commit framework for final publication:

1. Create the immutable revision from the locked source pointer.
2. Read back the resulting index section to determine `manifest_has_index`.
3. Under `segMu`, recheck source pointer, segment identity and retained state.
4. Atomically publish the segment pointer/marker and PUT all selected
   `SegmentIndex` records with `catalog.Update`.
5. Clear catalog-absent provenance only after the transaction succeeds. Install
   cloned records with `ManifestPublished = false`, preserving the current
   `(segment, index)` build slot.

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
by BuildID for GC. `segmentIndexMapLocks`, keyed by SegmentID, serializes updates
to each segment's `indexID -> record` map across different per-BuildID `keyLock`s.
Different segments can update their maps independently.
Dropping an index and creating another on the same field assigns different index
and build IDs, while GC retires the old record asynchronously. Without this lock,
GC can observe an empty segment map, a creator can insert a new index into it,
and GC can then unlink the entire map, losing the new index's in-memory mapping.
The same critical section makes checking a slot's BuildID and updating or removing
it indivisible with respect to other writers for that segment. These locks cover
no I/O and never acquire `segMu` or `fieldIndexLock`; manifest publication may
take the map lock while already holding `segMu`. They are separate from the
segment manifest locks that serialize the full manifest transaction.

Rollback includes retained Dropped segments, including snapshot-pinned parents.
Snapshot references continue to pin the same build IDs and physical files.
Historical immutable snapshot revisions remain untouched. Unreadable manifests
or invalid selected entries remain pending while their records exist. Entries whose records GC has retired are not rollback work, and
marker-only segments are not scanned. `manifest_has_index` may therefore remain
true after rollback completes; it still accurately describes the remaining
manifest entries.

Copy tasks retain their dispatch-time placement. The inspector does not depend
on `CopySegmentMeta` or wait for tasks to complete. If a result has published a
manifest but installed only some of its index records, rollback migrates only
those installed records and leaves other entries intact. Each later
`AddSegmentIndexFromManifest` installation wakes rollback, including after an
empty scan. New tasks use the effective etcd mode.

Result synchronization checks the current persisted task state under the copy
result lock. Repeated results for a completed task cannot reinstall a manifest
pointer that rollback has already advanced. Failed tasks are also rejected in
rollback mode, including historical tasks without a cleanup plan, so a late
result cannot republish data belonging to a failed task.
Rejected results continue to re-arm durable cleanup where the task still exists.

On restart, partial progress is derived from durable metadata: catalog rows
load first; remaining manifest entries recover normally. There is no separate
checkpoint to commit and no dual-write cleanup phase. A migrated segment with
an empty index section has `manifest_has_index=false` and needs no manifest
index read. Remaining GC-owned entries retain their existing recovery lifecycle.

### Completion and operation

Expose bounded-cardinality metrics:

- `manifest_index_rollback_pending_segments`: segments discovered from
  manifest-resident records before batch limiting;
- `manifest_index_rollback_pending_records`: remaining manifest-resident records
  in memory, including inconsistent records without a segment;
- `manifest_index_rollback_ready`: 1 after an active complete scan observes no
  pending records; reset on start/stop, before each scan, and when a new
  manifest-resident record is installed;
- `manifest_index_rollback_records_total`: successfully restored records;
- `manifest_index_rollback_segments_total{status=success|failed|stale}`:
  segment migration attempts by outcome.

Readiness requires a subsequent complete scan after the final batch. A canceled
scan never reports ready. This is an observation of the current record backlog,
not a guarantee that manifests are empty or that no new copy results can arrive.
Later record installations wake migration again.

Enable rollback and watch the record backlog and failure counts. Resolve failed
migrations or let ordinary GC retire their records. The switch remains active
until disabled in a later restart. Disabling it does not re-migrate anything;
forward migration resumes only if the separate forward choices are enabled.

## Verification

Run all build, formatting, generation, and tests inside the repository's
Milvus development container with the Conan cache mounted. Validate the full
DataCoord package, the affected catalog/configuration packages, and focused
race tests. Go race does not instrument native C++/Rust operations.

A real local packed-manifest round trip should verify that artifact paths and
records survive metadata restart. Unit/fault-injection results do not establish
cluster failover, snapshot restore, or QueryNode end-to-end acceptance.

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
- Bounded scans rotate across SegmentIDs while reporting the entire record
  backlog and keeping each selected segment together.
- Completed and superseded records are rejected before segment lookup.
- Empty scans stop the timer; catalog additions and rewrites wake migration,
  including a write between the final empty scan and the idle wait.
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
GC-retired records whose entries must remain untouched, including all-retired
and mixed batches, catalog failure/retry, and readiness; selected records
retired or rewritten to etcd before their BuildID locks are acquired;
superseded build restoration and retirement; late copy installation and repeated
completed/failed results; cancellation and bounded worker concurrency.

Use real packed manifests for both supported artifact layouts and verify the
same bytes remain readable, other manifest sections survive, forward/reverse
round trips converge, and a full metadata reload succeeds with manifest reads
disabled after all records in the fixture have migrated.

## Base and PR Dependency Order

This design targets merged [PR #53048](https://github.com/milvus-io/milvus/pull/53048),
merge commit `240b9c0a916f9633da53230e58854da08d8412f8`, on master snapshot
`2e36c6ff0151b32c187e4c32d40f76cbc8dd6379`. Its `SegmentIndexes` mutation slice,
batched GC retractions, bounded recovery, verified marker clearing, and durable
copy cleanup are the baseline.

The complete change is delivered in one PR targeting `master`:
[PR #53479](https://github.com/milvus-io/milvus/pull/53479), on
`feat/datacoord-manifest-index-rollback`. It includes this design, historical
index backfill, reverse migration, configuration/metrics, GC/copy coordination,
and their regression tests. Both migration directions build on the already
merged publication framework; neither requires a separate unmerged PR.

Each segment batch supplies one `SegmentIndexBackfill` per selected record in
`SegmentCatalogMutation.SegmentIndexes`. It must satisfy the same publication
validation as a foreground upsert. A batch may contain only backfill
publications, with one distinct index entry per record; mixed mutations and
Noop adoption are rejected. Existing multi-record GC removals retain their
merged contract.

This migration concerns historical **index metadata**. Spark/add-field backfill
and external refresh can adopt independently generated data manifests through
`UpdateManifestVersion`; their stale-base adoption races remain separate work
([#53328](https://github.com/milvus-io/milvus/issues/53328),
[#53329](https://github.com/milvus-io/milvus/issues/53329)). Operators must not
overlap those jobs with index publication or this migration on the same segment.
A structured backfill commit detects a pointer changed during its own I/O and
retries from the new base; it does not fence a later external adoption.
