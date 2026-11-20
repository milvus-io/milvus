# Segment index summaries

Related issue: #53047.

## Goal and scope

Store completed StorageV3 vector and scalar index inventory in
`SegmentInfo.index_summaries`. The paired segment manifest retains file lists.
Collection index definitions, active tasks, pending cleanup and legacy index
records that still own artifacts remain in the catalog.

`dataCoord.index.writeSegmentIndexToManifest` controls publication and forward
migration. It defaults to true and takes effect on restart. It requires Loon FFI.
Disabling it chooses etcd for newly scheduled builds. Previously published
summaries remain readable in either setting. In-flight builds and copy tasks
keep their persisted publication and file layout choices. StorageV1/V2
continue to use catalog records.

Upgrade all coordinators that can acquire leadership before enabling
migration on a deployment with older binaries. Use the switch set to false
during a mixed-binary rollout. An older binary cannot reconstruct an
artifact-free summary or maintain summary coverage while publishing a new
manifest revision. The legacy manifest-to-etcd rollback inspector applies to
deployments without persisted summaries. Disabling the publication switch
does not reverse migrated metadata.

## Recovery and migration

`manifest_has_index` describes file entries. `index_summary_version=1` means
the catalog carries enough inventory for the segment's current manifest,
including the case where the inventory is empty. Each artifact is represented
by a summary or by its authoritative catalog record. Related task and legacy
catalog records can coexist with this version. The absence of eligible legacy
records indicates that the segment's catalog backfill has converged.

DataCoord initializes using only the catalog. A marked legacy manifest with
version zero has pending inventory. After DataCoord is healthy, the existing
bounded recovery worker reads that revision, validates the file entries, and
persists the complete inventory and version in the segment row. It checks that
the pointer is unchanged under the segment manifest and writer locks. Historical
artifacts that already live in a manifest keep their file layout. When forward
publication is disabled, the worker recovers unconverted legacy manifest
records into memory and leaves their durable format unchanged.

The existing backfill inspector owns Finished catalog records. For a segment
with older artifact paths it copies eligible files through the existing
server-side copy mechanism, publishes a new manifest revision, writes the
summary, and retires the corresponding records in one catalog transaction.
A Finished record without files writes the summary and retires its catalog
record in a metadata-only transaction. Existing transaction and I/O budgets
apply. Local historical file layouts that need relocation remain catalog-backed
until their indexes are rebuilt or their segments are replaced. Old-layout
tasks completing during migration retain the existing completion path, and
the inspector will see newly eligible records.

After a failed read or catalog transaction, pending segments remain protected
and are retried. The worker can complete other segments concurrently. Restart
uses the persisted progress without loading migrated manifests into memory.

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
plus active and legacy catalog records. Load responses resolve artifact details from the exact manifest
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
Restore checks incomplete index inventories against the pinned manifest and the
snapshot's live definitions. An entry for a dropped index does not require files
in the snapshot. Workers that do not report a separate copied-index memory
estimate retain the serialized-size fallback.
L0 delete segments keep their existing format. When a snapshot captures no
index definitions, physical manifest entries left by dropped indexes are
discarded during copy.
Legacy snapshot and in-flight copy results keep their existing compatibility
paths. Targets with verified manifest placement publish summaries; legacy
targets publish catalog records.

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
3. Combine the publication controls, remove manifest index reads from startup,
   and migrate each eligible segment using the existing recovery worker and
   backfill inspector. During recovery, affected DataViews keep their published
   frontiers. Index RPCs report a retryable pending status, and index scheduling
   and manifest publication wait for the segment inventory. The build-rooted
   orphan scan waits while any inventory is pending because its prefix carries
   only a build ID. Once recovery completes, GC protects historical file aliases
   while a migrated build remains referenced.

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
