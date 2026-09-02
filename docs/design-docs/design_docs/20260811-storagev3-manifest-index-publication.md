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
legacy path: every record goes to etcd and no manifest index entry is produced
at all. On records the index in the manifest and, rather than writing the
`SegmentIndex` etcd row for the manifest-backed segment, deletes any row the
same build left behind before the switch was turned on (in the transaction
that publishes the entry - see the framework doc for why a plain skip would
make an off-era in-flight row rebuild forever); the commit also sets the segment's
sticky `manifest_has_index` marker in the same transaction, which is what
drives recovery (see the framework doc's "Retiring the etcd SegmentIndex
Record"). There is no dual-write mode.

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
        |-- catalog transaction: SegmentInfo.manifest_path + SegmentIndex
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

GC removes an index entry through the same framework, as a `DropIndexes`
mutation, before it deletes any bytes. The drop is resolved against the exact
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
afterwards, so by copy time that directory can hold revisions newer than the
pinned one. milvus-storage discovers the current version by listing the manifest
directory and taking the highest revision number, and a commit whose read
version is behind that resolves against the highest revision and writes one past
it. Copying the newer revisions would therefore make the target's next commit
merge onto the source's post-snapshot state and publish it as the target's own.
The copy keeps only the pinned revision, so the target's manifest history starts
exactly where the snapshot ended. This hazard predates index publication - any
later commit on a restored segment would hit it - but publishing indexes is what
makes a copied segment commit at all, so the guard belongs here.

The worker owns only physical facts (where the artifact landed, its build ID,
sizes, engine versions). Identity does not survive the snapshot boundary —
`RestoreIndexes()` allocates fresh index IDs and index name is the only stable
key — so DataCoord resolves it when assembling the request and ships it in
`CopySegmentTarget`: the target index definitions keyed by name, the target's
row count, and the inherited entry IDs to retract.

The target-definition map is the switch's lever on this path. With manifest
publication off DataCoord ships an empty map, so the worker retracts the
inherited entries and writes no target entries - the copied records go to etcd
like any legacy build. With it on the map flows and the worker republishes;
DataCoord then verifies the read-back and hard-fails the task if a synced
build has no entry (a DataNode predating republication), because the gated
etcd write would otherwise leave that record memory-only and it would silently
vanish on restart. A verified republication also sets the target segment's
`manifest_has_index` marker in the same transaction as its pointer.

On the source side, DataCoord reads the source manifest's index entries once
while assembling the request — but only when the snapshot's captured
`manifest_has_index` marker is set. The marker mirrors the sticky segment
marker and is snapshotted from the same record as the manifest pointer, so an
unmarked segment provably has no entries and its manifest is not read at all;
snapshots from before the field existed read false and are equally safe, since
their manifests predate index publication. Snapshot manifest format V5
persists this marker; V1-V4 readers default it to false. That read supplies the
retraction list, and, for
a snapshot whose segments record indexes only in their manifests, also the
artifact list to copy — filtered there to the index definitions the snapshot
still carries. The retraction list is not filtered: an entry whose definition
the snapshot no longer has is not copied, so leaving it behind would point the
target at the source's files.

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

The worker protocol is unchanged; `workerpb.IndexTaskInfo` gains no field. No
local `minor_version` is introduced: milvus-storage removed that field from its
manifest model.

DataCoord passes completed `SegmentIndex` metadata through QueryCoord to
QueryNode without reading object storage, on every read path and without
exception. `GetIndexInfos` and the snapshot-export projection carry no manifest
fallback, because neither of the two states one could recover is reachable:

- a segment with no `SegmentIndex` record has no index artifact. A manifest
  index entry is only ever published by `CommitSegmentManifest`, which installs
  the matching record in memory in the same commit, or by the copy worker, whose
  target records `syncVectorScalarIndexes` writes from the same worker result;
  GC retracts an entry and removes its record in one catalog transaction; and
  reload rebuilds manifest-resident records from the manifests of segments
  marked `manifest_has_index` at startup, before the server serves.
- a finished record with no `index_file_keys` has no manifest entry either: the
  only build that records no files is a fake-finished one (a segment too small
  to train), which `publishIndexToManifest` skips.

This matters for cost as well as clarity: `GetIndexInfos` is driven by
QueryCoord's index checker every `checkIndexInterval` for exactly the segments
that are missing an index, so a fallback read there is paid repeatedly by the
segments it can never help.

Segment index GC does read the manifest for a dropped StorageV3 segment that no
longer has `SegmentIndex` rows - it must, since it is deciding whether artifacts
may be deleted rather than which paths to serve - but only when the segment's
`manifest_has_index` marker says the manifest can actually carry entries; an
unmarked segment is recycled without a manifest read, so an all-etcd cluster
has no `gcBlockedByManifest` failure surface. That read fails closed, with one
exception: if the manifest file itself is no longer in object storage there is
nothing left to protect, and blocking would strand the segment's metadata
permanently, so the segment is recycled. The FFI reports every manifest read
failure as one transient class, which is why existence is checked through the
chunk manager rather than inferred from the error.

A segment that is already dropped is skipped by both index paths, in each case
because it will publish no further manifest revision: an index build that
finishes against it records its result the legacy way, and GC deletes its
artifacts without retracting the entry rather than retrying a commit that can
never succeed. With the switch on, that legacy `FinishTask` fallback's etcd
write is itself gated for a manifest-backed segment, so the fallback record -
and every other publish decline (a dropped index definition, a fake-finished
build) - is memory-only: it is lost on restart and either reissued by the
inspector or retired with its segment. Under `storePathVersion: 0` the orphan
sweep reclaims any files such a record left behind; under version 1 a small
leak window exists until the segment itself is collected.

## Verification

- StorageV3 manifest round-trip covers every typed index load field, properties,
  version increment, and republication under one index ID.
- `CommitSegmentManifest` writes the segment record and the `SegmentIndex`
  record in one catalog transaction, and refuses the write rather than falling
  back to a chunked flush that could expose them separately.
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
  (`8632a0f`), which contains both the index publication and the
  `drop_index(index_id)` APIs. Both the drop key and the `AddIndex` replacement
  key were read from that exact revision: `index_id`, not
  `(column_name, index_type)`.

Not verified end-to-end: no cluster run exercised a QueryNode load driven purely
by manifest-resolved index metadata, and the copy/restore path was not run
against a real snapshot. Both are covered by unit tests only.
