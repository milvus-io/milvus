# Segment index summaries for new clusters

Related issue: #53047.

## Goal and scope

Keep completed index inventory in SegmentInfo and artifact details in the
segment manifest. Do not reconstruct completed SegmentIndex objects at startup.
Collection index definitions and active or pending-cleanup build tasks remain
coordinator metadata. The default mode retains its index metadata source and
publication behavior.

Completed inventory has one publication mode: legacy mode retains completed
SegmentIndex records and does not write summaries; summary mode writes summaries
and retires completed records. Manifest artifact entries remain in both modes.

The new mode is selected only when an empty cluster first starts and is persisted
in the metadata namespace. Subsequent starts must select the same mode. Existing
clusters cannot enable it, and summary-mode clusters cannot disable it. Manifest
publication and `common.storage.useLoonFFI=true` are required. Summary mode
pins the storage, publication, backfill and rollback settings at startup.
Historical migration, rollback and conversion of incompatible snapshots are
outside this version. Legacy direct manifest-version adoption (BatchUpdateManifest
and pre-baked backfill results) is rejected before publication because it cannot
update index inventory or fence active builds. Structured backfill remains
supported by the commit framework.

Enable at the first startup with both index migration switches disabled:

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

`GetAllSegmentIndexSummaries` returns a separate in-memory inventory type for
both legacy and summary modes. It contains collection/partition/segment/index/build
identity, task state, deletion state, artifact presence, sizes and engine versions, including active and
pending-cleanup tasks. It has no file metadata and performs no manifest I/O.
Summary lookup, deletion staging and accounting consume this inventory directly.
A summary-only removal has no SegmentIndex task record or task-catalog action;
its inventory and counters are retired only after the segment update succeeds.
Consumers that need files resolve task records or the owning manifest explicitly;
a task record takes precedence when the same BuildID also has a persisted summary.

Readiness, build deduplication, progress and compaction decisions use summaries
plus active tasks. Load responses resolve artifact details from the exact manifest
revision paired with the segment snapshot. Load also reads live finished task
rows, such as a build that completes after its segment is dropped. Batch reads
share the manifest I/O budget and preserve empty results for missing segments.
Manifest read failures propagate.

Resource preflight requests an index projection alongside `GetSegmentInfo`.
In summary mode the projection combines the returned segment's summaries with
live index definitions and finished tasks. Artifact presence, memory size and
build row count suffice for estimation. A response entry with zero indexes is
a complete projection. A missing entry uses `GetIndexInfos` for compatibility.
New summaries retain the build's row count. For summaries written before that
field existed, estimates use the segment's persisted row count. Full load
responses still resolve manifest-backed indexes from the manifest.

For an ordinary StorageV3 QueryView segment in summary mode, QueryNode can
request local index resolution. DataCoord returns the summaries, retained
finished tasks, manifest pointer and matching replay cursor from the accepted
snapshot without opening the manifest. QueryNode opens that revision once per
preparation and uses it for index artifacts, column groups, delta paths and stats.
The manifest closes when preparation ends. Read states of the same revision
share the imported column groups. The View's index definitions supply its load
parameters. A finished task for the same build supplies the artifact metadata.
Complete load metadata remains available for older consumers, external
collections and StorageV2. In summary mode, segment diagnostics use the
selected segment's summaries and finished tasks directly. Full metadata batches
use the shared manifest I/O limit and return per-segment errors.

Manifest reuse also applies to ordinary non-L0 StorageV3 QueryView loads when
summary mode is off. The default `common.storage.useLoonFFI=true` allows
QueryNode to share the selected revision for column groups, delta paths and stats.
Those loads use DataCoord's index file paths from the complete load metadata.

## Cleanup and restore

GC keeps active tasks, current manifest references, importing targets and
snapshot-pinned builds. Superseded artifacts are reclaimed by the build-aware
segment-rooted orphan scan after retention. Dropped index cleanup reads one
segment manifest at a time and retracts its matching summary atomically. Dropped
segments retire completed readiness in one catalog update before removing
files. Their manifests and task rows supply the retry list. Orphan scans reuse
one manifest revision at a time and check snapshot protection again at deletion.
A pending snapshot blocks new index deletions in its collection and waits for
already admitted deletions to finish before selecting revisions. The block lasts
until publication installs the build pins or snapshot creation fails.

Snapshot creation collects build IDs from the selected segments' summaries and
persists them with the snapshot's exact manifest pointers. The snapshot metadata
marks that manifest-backed file records are deferred. Summaries without files
remain explicit completion records. Restore and export use DataCoord's shared
manifest read limit to resolve deferred records from those versions and verify
that every captured build is accounted for. The self-contained export writes
the complete file list with rewritten paths.

To move a summary-mode snapshot to a version without manifest-backed snapshot
support, export it on the source version and restore the self-contained bundle.
The referenced snapshot's V6 metadata uses `index_files_from_manifest` to mark
the deferred files. Older readers ignore that field and can accept the snapshot
without copying its index artifacts, causing the target to rebuild them.

Copy/restore verifies compatibility before creating work or copying files.
Compatible targets publish their manifest and summaries before becoming visible.
For older snapshots, restore checks incomplete index inventories against the pinned
manifest and the snapshot's live definitions. An entry for a dropped index does not require files
in the snapshot. Workers that do not report a separate copied-index memory
estimate retain the serialized-size fallback.
L0 delete segments keep their existing format. When a snapshot captures no
index definitions, physical manifest entries left by dropped indexes are
discarded during copy.
An incompatible snapshot returns an explicit unsupported error; no format
conversion or legacy artifact publication is attempted.

The mode and physical index inventory are local cluster state. CDC continues to
replicate logical index DDL; each cluster constructs its own segments, build IDs
and artifacts. No new replicated side effect or WAL message is introduced.

## Stack and validation

1. Add the default-off immutable mode and all summary writes: normal and empty
   completion, replacement/invalidation, retirement, and copy/restore publication.
   Legacy mode retains completed SegmentIndex records without summaries; summary
   mode publishes summaries and retires completed records. Include the metadata
   access needed to validate writes, and test persistence, failure and restart.
2. Adapt consumers: readiness, scheduling, progress, compaction, load responses,
   snapshot extraction and GC. Read artifact details from the pinned manifest on
   demand, without rebuilding a resident completed-index cache. Validate consumers
   against the first layer's publications, including read failures and cleanup races.

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
