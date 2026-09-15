# StorageV3 Manifest Index Rollback to etcd

- **Created:** 2026-09-15
- **Component:** DataCoord
- **Depends on:** [historical index backfill](20260901-storagev3-manifest-index-backfill.md)
  and [manifest publication](20260811-storagev3-manifest-index-publication.md).

## Goal and compatibility boundary

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

## Controls

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

## Durable transition

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
with historical manifest metadata. A missing in-memory record is retried: copy
installation may still be in progress, or GC may be retiring the segment.

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

## GC, replacement, copy, and restart

Both record-only index GC and dropped-segment GC take the segment manifest lock
across file deletion and metadata retirement. Dropped-segment GC refreshes the
segment under that lock; it cannot remove a prefix while rollback is writing a
new revision into it. Index GC may have selected an old manifest revision;
conditional drops and BuildID locks keep retirement ordered, and rollback
never installs a stale record over a replacement build.

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

On restart, partial progress is derived from durable metadata: catalog rows
load first; remaining manifest entries recover normally. There is no separate
checkpoint to commit and no dual-write cleanup phase. A migrated segment with
an empty index section has `manifest_has_index=false` and needs no manifest
index read, matching the target version's catalog-only startup behavior.

## Completion and downgrade runbook

Expose bounded-cardinality metrics:

- `manifest_index_rollback_pending_segments`: all marked segments, including
  unsupported/unreadable/temporarily blocked ones, before batch limiting;
- `manifest_index_rollback_pending_copy_tasks`: unfinished copy/restore tasks;
- `manifest_index_rollback_pending_records`: remaining catalog-absent records
  in memory, including inconsistent records without a marked segment;
- `manifest_index_rollback_ready`: 0 until an active complete scan observes all
  three counts at zero; reset on start/stop and before each scan;
- `manifest_index_rollback_records_total{status=...}`: migrated records and
  failed/stale segment attempts.

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

## Failure verification

Cover source read failure, invalid paths/identity, manifest write failure,
read-back failure, catalog transaction failure and transaction-size rejection.
At every failure, check both the durable pointer and every involved catalog
row, then retry or restart. Test source pointer advancement and segment drop
during I/O; conditional build replacement; existing catalog rows with newer
task state; multiple indexes over several atomic batches; deleted definitions;
retained Dropped/snapshot-pinned segments; GC selected before/during rollback;
late copy installation; cancellation and bounded worker concurrency.

Use real packed manifests for both supported artifact layouts and verify the
same bytes remain readable, other manifest sections survive, forward/reverse
round trips converge, and a full metadata reload succeeds with manifest reads
disabled after migration. That last test models the target's catalog-only index
recovery boundary; actual target-binary/cluster acceptance is recorded separately.

All formatting, builds and tests run in the Milvus development container.
Run the full DataCoord and affected metastore/configuration suites, plus focused
Go race tests. Go race does not instrument native C++/Rust operations.

## Stack

1. `feat/datacoord-manifest-index-backfill-design`: design documents only,
   including both migration directions and their validation plans.
2. `feat/datacoord-manifest-index-backfill`: complete forward migration.
3. `feat/datacoord-manifest-index-rollback`: complete independently enabled
   reverse migration, catalog support, GC coordination, configuration, metrics
   and regression tests. It depends on layer 2.
