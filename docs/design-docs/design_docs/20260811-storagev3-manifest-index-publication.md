# StorageV3 Manifest Index Publication

- **Created:** 2026-08-11
- **Status:** Implemented
- **Component:** DataCoord, StorageV3
- **Depends on:** [DataCoord Segment-Scoped Manifest Commit Framework](20260817-datacoord-segment-manifest-commit.md)
- **Related upstream change:** [milvus-storage#620](https://github.com/milvus-io/milvus-storage/pull/620/), [milvus-storage#622](https://github.com/milvus-io/milvus-storage/pull/622/)

## Summary

A completed StorageV3 index artifact is recorded in the segment's manifest.
Every such manifest revision is created by DataCoord inside
`meta.CommitSegmentManifest`, in the same segment-scoped critical section that
advances `SegmentInfo.manifest_path` and persists the index task metadata.

Publication is opt-in and exclusive with the etcd `SegmentIndex` row, behind
`dataCoord.index.writeSegmentIndexToManifest` (default off). Off is the pure
legacy path: every task state goes to etcd and no manifest index entry is
produced. On changes only the terminal artifact placement: Unissued,
InProgress, Failed, and fake-Finished records still persist to etcd; a Finished
StorageV3 build with files is published to the manifest, and that publication
deletes its etcd task row in the same catalog transaction. StorageV1/V2 always
remain etcd-backed. There is no durable dual-write mode.

The same switch may be changed in either direction. A
`SegmentInfo.manifest_has_index` marker conservatively records whether the
current manifest may contain index metadata. Additions set it atomically with
the pointer. Retractions read the final immutable revision and clear the marker
in that same catalog transaction only when its index section is empty. Startup
recovery and GC follow this marker,
not the switch's current value, so disabling publication redirects new results
to etcd without hiding or leaking indexes written while it was enabled. An
all-etcd cluster has no marked segments and performs no manifest index reads.

Index workers keep their existing responsibility — build the index and upload
its files — and keep their existing result contract. They do not open a
manifest transaction and the worker result carries no manifest path.

## Motivation

An index built from a StorageV3 segment is valid only in the manifest lineage
that supplied its source data. Two properties are required:

1. the artifact must be discoverable from the manifest, so manifest-aware
   consumers do not depend on etcd `SegmentIndex` rows; and
2. the revision that contains the artifact must be the revision the segment
   pointer advances to, with no window in which one exists without the other.

Publishing from the worker cannot provide (2). The worker holds a revision it
read when the build started; between that read and DataCoord's etcd write the
segment can have advanced (a delta log, a stats entry, another index). The
worker's revision is then either a stale base or a sibling of the visible one,
and DataCoord has to detect and reject the mismatch after the fact.

## Design

```
index worker uploads index files                (unchanged worker contract)
        |
        v
DataCoord projects the worker result onto the index task record
        |
        v
meta.CommitSegmentManifest(segmentID)           <- per-segment commit lock
        |-- read the currently published manifest pointer
        |-- packed transaction: add the typed index entry
        |-- catalog transaction: SegmentInfo.manifest_path + delete SegmentIndex
        `-- install both in memory
        |
        v
QueryNode handoff continues to use SegmentIndex metadata
```

`packed.ManifestUpdates` carries `Indexes` and `DropIndexes` alongside the
existing data/deltalog/stat entries, so an index change is an ordinary
structured mutation of the framework rather than a separate transaction API.
There is no `AddIndexInfoToManifest`-style entry point that a caller outside
the framework could use to publish a revision.

`ManifestIndexInfo` uses the typed `LoonIndexInfo` fields: index name/type,
field/index/build IDs, artifact and engine versions, row count, serialized and
memory sizes, path-layout version, and the relative index-file keys.
`properties` carries index-specific parameters such as metric type and Knowhere
options. This preserves Milvus's multi-file index layout without encoding
required load metadata as strings.

DataCoord builds every one of those fields from metadata it already owns: the
collection schema (column name), the collection index definition (index name,
type, params), and the index task record the worker result was projected onto
(file keys, sizes, engine versions, row count). No new field on the worker
result is needed, and no worker-supplied value is trusted as a manifest
revision.

### Artifact paths

The manifest artifact path is intentionally distinct from the legacy etcd
`SegmentIndex` path: index bytes keep their existing
`index_v1/<collection>/<partition>/<segment>/<build>/<version>` layout.
DataCoord stores that prefix in `LoonIndexInfo.path` relative to the segment's
`_index` directory, so milvus-storage's relative/absolute normalization
round-trips it back to the legacy prefix on read. The legacy etcd handoff
remains on its existing global `index_files`/`index_v1` hierarchy.

### Concurrency

Publication is serialized per segment by `segmentManifestLocks[segmentID]`. A
commit reads the segment's current pointer under that lock and opens the packed
transaction at exactly that revision, so an index task and a concurrent stats
or delta-log commit on the same segment produce a linear revision chain instead
of two siblings. Index tasks on different segments stay fully concurrent.

For a commit that also mutates SegmentIndex records, the lock order is
`segmentManifestLocks[segmentID] -> indexMeta.keyLock[BuildID] -> segMu`.
The BuildID locks are acquired before the initial segment snapshot and retained
through manifest I/O and catalog publication, preserving the task projection
used to construct the entry. Only `segMu` is released during object-storage I/O.
The packed OVERWRITE resolver applies updates to the specified read manifest;
it does not merge a newer revision created by a writer outside this framework.

Because the base revision is chosen at commit time rather than at build time,
there is no "index revision does not follow the segment revision" failure mode
and no stale-publication error for the scheduler to handle.

A rebuild of the same user index republishes under the same `index_id`;
milvus-storage replaces the existing entry rather than appending, so a segment
never accumulates two entries for one index. `index_id` — not
`(column_name, index_type)` — is the replacement key, which is what lets several
JSON-path indexes on one field coexist in a manifest.

One milvus-storage rule is load-bearing here and worth stating explicitly:
appending column-group files to a manifest auto-drops every index entry on the
affected columns. That is the right semantics (the index no longer covers all
the data), and it is unreachable for a published index today, because DataCoord
only appends files while building a new segment or adding a new column, never to
a column of a sealed segment that already carries an index. Should that change,
the manifest entry disappears while the `SegmentIndex` record still reads
`Finished`; the readers below degrade to the legacy path rather than failing.

### Removal

GC deletes an unused artifact's bytes first, then removes its index entry through
the same framework as a `DropIndexes` mutation. Keeping metadata until file
deletion succeeds makes partial failures retryable for both path layouts. The drop is resolved against the exact
revision the transaction is opened at and carries the expected build ID, so a
drop issued from stale GC metadata cannot delete an artifact a rebuild
republished under the same index ID. A drop for an entry that is already gone
is skipped rather than committed as an empty revision, which keeps a retried GC
cycle idempotent.

### Copy / restore

A copy target is a fresh segment whose worker returns a complete first manifest
pointer, index entries included; DataCoord publishes that pointer inline.

The copied manifest object is a byte copy of the source's, and re-basing its
pointer onto the target path is faithful for everything stored relative to the
segment base — column groups and their per-file properties, stats, LOB. It is
not faithful for index entries: an index artifact lives outside the segment
directory, so its stored relative path walks back out of the base and thereby
hardcodes the source collection/partition/segment/build IDs. Re-basing moves
where that walk starts, not the IDs it encodes, so an inherited entry keeps
pointing at the source's artifacts. The worker therefore retracts every
inherited entry and records entries re-derived from the artifacts it actually
copied, in one transaction on top of the copied manifest.

Committing on the target also constrains what the copy may bring over. The copy
carries the segment directory wholesale, and the manifest directory is part of
it; but a snapshot pins one revision while the source segment keeps evolving
later. The copy keeps only that pinned revision to avoid copying unrelated
history. The pinned milvus-storage implementation (`3ae6ac4`) applies OVERWRITE
updates to the specified read manifest, not the highest revision's contents.
It uses the highest revision only to choose the new version number. A stale
revision left by a previous attempt therefore does not inject its entries into
the new result. Generated target revisions are included in worker cleanup;
coordinator cleanup also covers the entire task-owned target segment directory.

The worker owns only physical facts (where the artifact landed, its build ID,
sizes, engine versions). Identity does not survive the snapshot boundary —
`RestoreIndexes()` allocates fresh index IDs and index name is the only stable
key — so DataCoord resolves it when assembling the request and ships target
index definitions keyed by name. The worker obtains row count from the source
description. It enumerates inherited entries from the copied manifest unless
the snapshot's captured marker proves that manifest index-free.

The target-definition map is the switch's lever on this path. DataCoord
persists the selected placement on the copy task before dispatch, so a switch
flip or DataCoord restart while the task runs cannot change where its result is
installed. With manifest publication off DataCoord ships an empty map, so the
worker retracts inherited entries and writes no target entries - the copied
records go to etcd like any legacy build. With it on the map flows and the
worker republishes. Current workers acknowledge the completed rewrite and the
build IDs actually published. An acknowledgement of an empty index section avoids
read-back. For artifact-bearing results, DataCoord reads the manifest and checks
the target index ID, field, name, parameters, artifact directory, version, layout,
and file keys. Older workers always take this read-back path. A missing or
mismatched entry fails before the target segment becomes visible. Installation
keeps the verified index ID even if a same-name definition is replaced after the
check; it never rebinds a manifest artifact to that replacement. A verified entry is installed in memory without creating a redundant
etcd row; empty-artifact records still go to etcd.

On the source side, DataCoord reads a local source manifest while assembling
the request only when snapshot metadata has no index files and the captured
marker does not prove the manifest index-free. Snapshot manifest format V5
persists this marker, while V1-V4 remain unknown and take the conservative read
path. Retraction is not sent separately: after copying, the worker either uses
the marker's proof of emptiness or enumerates and removes every inherited entry
from the target manifest itself.

### Failure semantics

The transaction runs only after index bytes are uploaded. A publication failure
can therefore leave unreferenced uploaded files, but cannot expose an index
artifact through an incorrect manifest revision; normal index-file GC reclaims
such files. A manifest revision whose catalog write fails is never referenced by
any `SegmentInfo` and is invisible.

If the index task is deleted while its worker result is in flight, the commit is
abandoned without publishing: an orphaned revision is invisible and self-
cleaning, whereas a published entry with no `SegmentIndex` row would have no
record to drive its GC.

## Compatibility and Scope

The index-build worker protocol is unchanged; `workerpb.IndexTaskInfo` gains no
field. Copy/restore adds optional source-marker, persisted-placement and worker
acknowledgement fields. Their absence is the rolling-upgrade signal: old
snapshots and old workers use the conservative manifest-read path. No local
`minor_version` is introduced: milvus-storage removed that field from its
manifest model.

DataCoord passes completed `SegmentIndex` metadata through QueryCoord to
QueryNode without reading object storage on every request path. `GetIndexInfos`
and snapshot export carry no per-request manifest fallback because memory is
made complete at startup and publication updates it atomically:

- a segment with no `SegmentIndex` record has no index artifact. A manifest
  index entry is only ever published by `CommitSegmentManifest`, which installs
  the matching record in memory in the same commit, or by the copy worker, whose
  target records `syncVectorScalarIndexes` writes from the same worker result;
  GC retracts an entry and removes its record in one catalog transaction; and
  reload rebuilds manifest-resident records from every healthy non-L0
  StorageV3 segment marked `manifest_has_index` before the server serves,
  independently of the current write mode. A marked segment without a manifest
  pointer fails startup. Reads run in bounded batches with per-segment retries;
  an exhausted read fails startup without replaying all successful reads through
  the outer metastore retry loop.
- a finished record with no `index_file_keys` has no manifest entry either: the
  only build that records no files is a fake-finished one (a segment too small
  to train), which `publishIndexToManifest` skips.

This matters for cost as well as clarity: `GetIndexInfos` is driven by
QueryCoord's index checker every `checkIndexInterval` for exactly the segments
that are missing an index, so a fallback read there is paid repeatedly by the
segments it can never help.

Segment index GC reads the manifest for a dropped StorageV3 segment marked
`manifest_has_index`, even if publication has since been disabled - it must,
since it is deciding whether artifacts may be deleted rather than which paths
to serve. Unmarked segments stay on the legacy etcd path and perform no
manifest index reads. The marked read fails closed, with one
exception: if the manifest file itself is no longer in object storage there is
nothing left to protect, and blocking would strand the segment's metadata
permanently, so the segment is recycled. The FFI reports every manifest read
failure as one transient class, which is why existence is checked through the
chunk manager rather than inferred from the error.

A segment that is already dropped is skipped by both index paths, in each case
because it will publish no further manifest revision: an index build that
finishes against it records its result the legacy way, and GC deletes its
artifacts without retracting the entry rather than retrying a commit that can
never succeed. These fallback and decline states remain durable in etcd even
when publication is enabled.

## Verification

- StorageV3 manifest round-trip covers every typed index load field, properties,
  version increment, and republication under one index ID.
- `CommitSegmentManifest` writes the segment pointer and deletes the completed
  `SegmentIndex` task row in one catalog transaction, and refuses a chunked
  fallback that could expose them separately.
- DataCoord read paths use `SegmentIndex` metadata with no manifest I/O at all,
  including for a segment with no records and for a finished record carrying no
  `index_file_keys`.
- Drop semantics are exercised against the real FFI: a drop whose expected build
  no longer matches the manifest is refused, a drop for an absent index is a
  no-op rather than an empty commit (which loon rejects), and a drop removes
  only the matching index.
- GC blocks a dropped StorageV3 segment while its manifest is unreadable and
  recycles it once the manifest is gone.
- The milvus-storage C FFI library is the version already pinned on master
  (`3ae6ac4`), which contains both the index publication and the
  `drop_index(index_id)` APIs. Both the drop key and the `AddIndex` replacement
  key were read from that exact revision: `index_id`, not
  `(column_name, index_type)`.

Not verified end-to-end: no cluster run exercised a QueryNode load driven purely
by manifest-resolved index metadata, and the copy/restore path was not run
against a real snapshot. Both are covered by unit tests only.

## Recovery and restore resource bounds

All DataCoord manifest index reads during recovery, copy request assembly,
copy result verification and GC share one metadata-owner semaphore. Waiting
for admission is cancellable and does not enter cgo. The configured concurrency
is capped by positive `minio.maxConnections`; zero means unspecified. Result
verification prefetches under this bound, validates every result before installing
any target, and keeps the verified BuildID-to-IndexID mapping for installation.

Historical sticky markers are normalized once at startup: after an empty index
section is read, recovery persists false against that exact manifest pointer.
Subsequent startups skip the segment. A failed read or catalog write aborts
recovery. A pointer change prevents clearing its marker. The candidate set still
includes entries for dropped definitions so their files remain discoverable by GC.
No extra count field or scan of collection index definitions is required.

Copy requests carry collection index definitions once at request scope. Before
sending non-empty shared definitions, DataCoord queries the selected worker's
`copy_segment_shared_indexes` capability. Unknown/old workers receive the legacy
per-target encoding. Current workers also continue to accept that encoding.
This optimization changes wire size for capable workers; it does not remove the
legacy workers' payload-size limit.

Before dispatch, DataCoord persists the newly allocated target index directories
and the V3 target segment directory on the copy task. Importing targets receive
a version-zero manifest base so normal segment GC can find them even if result
validation refuses the first real pointer. If a completed worker result is
rejected, a durable cleanup intent keeps the failed task until the inspector
successfully removes its planned directories. Cleanup uses coordinator-derived
paths, including each retry's new BuildIDs, and retries object-storage failures
across DataCoord restart without relying on the worker's in-memory file list.
The plan applies to dispatches made by this implementation; it cannot recover
unrecorded BuildIDs from tasks dispatched before this change. Partition-level
LOB files remain covered by the existing LOB orphan collector and its safety
window; a shared partition directory is never a task cleanup prefix.

These changes do not enable concurrent backfill adoption or binary downgrade.
Before enabling manifest-only publication, all DataCoords eligible for leadership
must support recovery and GC of manifest-resident indexes. Turning publication
off does not restore compatibility with pre-feature DataCoord binaries.
