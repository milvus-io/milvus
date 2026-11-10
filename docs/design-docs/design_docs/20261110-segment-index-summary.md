# Segment index summaries for new clusters

Related issue: #53047.

## Goal and scope

Keep completed index inventory in SegmentInfo and artifact details in the
segment manifest. Do not reconstruct completed SegmentIndex objects at startup.
Collection index definitions and active or pending-cleanup build tasks remain
coordinator metadata. The default mode retains existing behavior.

The new mode is selected only when an empty cluster first starts and is persisted
in the metadata namespace. Subsequent starts must select the same mode. Existing
clusters cannot enable it, and summary-mode clusters cannot disable it. StorageV3
and manifest publication are required. Historical migration, rollback, and
conversion of incompatible snapshots are outside this version. Legacy direct
manifest-version adoption (BatchUpdateManifest and pre-baked backfill results)
is rejected before publication because it cannot update index inventory or fence
active builds. Structured backfill remains supported by the commit framework.

Enable at the first startup, with both index migration switches disabled:

```yaml
dataCoord:
  index:
    segmentIndexSummariesEnabled: true
    writeSegmentIndexToManifest: true
```

The durable selection is `datacoord-meta/index-metadata-mode`. A restart with a
different selection fails before recovery or background work starts.

## Metadata and publication

SegmentInfo.index_summaries identifies completed indexes by IndexID, FieldID and
BuildID, including completion without an artifact. It retains sizes and engine
versions needed by coordinator scheduling and accounting. It contains no file
list. Active task state remains keyed by globally allocated BuildID.

A successful build publishes its artifact in the manifest, replaces the summary,
and deletes the durable task record in the existing segment commit transaction.
An empty completion publishes only the summary and retires its task atomically.
In-memory completed records are removed after publication. Stale completion and
removal must not replace or erase a newer build. Data replacement invalidates
summaries under the same segment lock as the manifest change.

Readiness, build deduplication, progress and compaction decisions use summaries
plus active tasks. Load responses resolve artifact details from the exact manifest
revision paired with the segment snapshot. Manifest read failures propagate;
absence of a cached record never proves absence of a live artifact.

## Cleanup and restore

GC keeps active tasks, current manifest references, importing targets and
snapshot-pinned builds. Superseded artifacts are reclaimed by the build-aware
segment-rooted orphan scan after retention. Dropped index cleanup reads one
segment manifest at a time and retracts its matching summary atomically. Dropped
segments retain the existing whole-directory cleanup and snapshot protections.
No full-cluster artifact metadata cache is introduced.

Copy/restore verifies compatibility before creating work or copying files.
Compatible targets publish their manifest and summaries before becoming visible.
An incompatible snapshot returns an explicit unsupported error; no format
conversion or legacy artifact publication is attempted.

The mode and physical index inventory are local cluster state. CDC continues to
replicate logical index DDL; each cluster constructs its own segments, build IDs
and artifacts. No new replicated side effect or WAL message is introduced.

## Stack and validation

1. Persist completed inventory with segment manifest publication, with replacement
   and failure tests. Keep existing readers and startup behavior.
2. Add summary readers and on-demand artifact loading; retire completed
   in-memory records. Keep the new mode inaccessible until lifecycle integration.
3. Add immutable new-cluster selection, GC and compatible snapshot/copy
   integration, then expose the mode. Validate failures, cleanup races and restart.

Use the matching Milvus development container for generation, formatting, builds
and tests. Check catalog failure leaves both pointer and summaries unchanged;
repeated completion, drop/recreate, empty completion, restart, data replacement,
snapshot pins, incompatible restore, bounded manifest reads and memory retention.
Report source-traced paths separately from executed integration evidence.

Executed validation in the matching development container:

- Native C++ build and standard Go protobuf generation passed.
- Full DataCoord index and metadata package tests passed.
- Focused summary/publication, GC, copy, restore, catalog and configuration tests
  passed, including failed catalog publication and the task-retirement GC race.
- The full paramtable suite is blocked by its existing storage-I/O test using
  `/var/lib/milvus/data` on the read-only container root; focused mode tests passed.

Live-cluster end-to-end build/load/restart and memory benchmarks have not run.
