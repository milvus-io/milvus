# Segment-rooted index artifacts for manifest publication

- **Created:** 2026-10-19
- **Author(s):** @somfornot
- **Status:** Under Review
- **Component:** DataCoord, DataNode, QueryNode, Storage
- **Related Issues:** #53047

## Goal and scope

When `dataCoord.index.writeSegmentIndexToManifest=true`, newly created
StorageV3 vector/scalar index builds write their files into the owning segment's
`_index` directory. New manifest index entries must never reference another
namespace, including through `..`. Storage also enforces this invariant when
adding/updating index entries; existing entries can remain unchanged.

This changes artifact placement, in addition to the metadata publication
introduced by the [manifest publication design](20260811-storagev3-manifest-index-publication.md).
TEXT and JSON stats use their existing `_stats` contracts. StorageV1/V2 and
builds created with the switch disabled keep the existing layout selection.
There is no migration of historical artifacts or repair of corrupted manifests.

## Layout and interfaces

`IndexStorePathVersion=2` (`SEGMENT_ROOTED`) identifies:

```text
<segment manifest base>/_index/<buildID>/<indexVersion>/<fileKey>
```

For a canonical segment the base is
`<storage root>/insert_log/<collection>/<partition>/<segment>`.
An existing legacy local manifest keeps its actual base; its new indexes must
also remain inside that base's `_index`, rather than using today's canonical
root. Object storage keys are bucket-relative and local paths are absolute.

DataCoord persists version 2 when creating the build task. The request carries
both that version and the complete artifact prefix in `IndexFilePrefix`.
DataNode forwards them to the C++ file manager. The same prefix is used by
memory/disk and packed index uploads. QueryNode reconstructs its file-manager
prefix from the recorded complete file list. Go load, snapshot and GC consumers
use the persisted version and the owning segment's manifest base.

DataCoord stores `<buildID>/<indexVersion>` in `ManifestIndexInfo.Path`.
`ManifestIndexRelativePath` requires literal containment under this segment's
`_index/` and rejects noncanonical relative components. The FFI staging boundary
also rejects absolute paths and parent traversal. Unrelated manifest updates do
not restage existing index entries.

## Retries, mode changes and upgrades

A task's path version is durable. Toggling the switch does not redirect an
already dispatched upload. An old version-0/1 task completed after enabling
manifest publication fails path validation; it cannot publish an external
reference or silently finish in etcd. Such a task needs a new build. No automatic
migration/rebuild policy is introduced here. Turning the switch off still permits
an already dispatched version-2 build to finish using etcd metadata, with its
physical path unchanged.

Upgrade DataCoord, index workers and QueryNodes before enabling this behavior.
An older native worker treats unknown path versions as collection-rooted and
can echo the requested version despite writing the old layout. The existing
switch is an operator gate, not a per-worker capability negotiation. Downgrading
readers while version-2 artifacts exist is unsupported.

## Snapshot restore

A new copy task captures version 2 together with manifest publication mode in
its durable metadata. `CopySegmentTarget.index_store_path_version` tells the
worker the destination layout independently of the source layout. Sources in
versions 0, 1 or 2 can therefore be copied to the new segment's `_index`.
Absent fields retain the old task's source layout; any attempt to publish an
external-path entry still fails. Target paths are validated before any file copy.

The segment-directory listing excludes `_index`: only the pinned snapshot's
explicit index list selects builds to copy. Otherwise obsolete builds and old
BuildIDs would be inherited. Cleanup prefixes and result admission use the same
persisted destination version. A result claiming a different layout is rejected
before making the target segment visible.

## Garbage collection

Recorded-file deletion uses version 2 and the segment's actual manifest base.
The orphan scanner applies the existing build-state, import, age and snapshot
protections to `insert_log/.../_index`. It skips ordinary data/stats/manifests,
retains active builds and recorded finished files, and reclaims unrecorded builds
or obsolete attempt files. The generic V3 binlog scan alone is insufficient
because it skips registered V3 segments. Dropped-segment GC still owns whole
segment deletion. Historical noncanonical local namespace orphan-scan gaps
remain as documented in the storage path contract.

## Validation and PR organization

One layer on `master` contains the protocol, producer/consumer changes, copy
and GC integration, regression tests and documentation. No intermediate layer
publishes version 2 before the consumers understand it.

Focused validation covers layout selection, native upload/read, repeated real
manifest commits, old-task rejection without metadata publication, version
mismatch, restart/load metadata, conversion from all source layouts, exclusion
of unpinned builds, copy-task persistence, and orphan/snapshot/import GC rules.
Run Go tests with `-tags dynamic,test -gcflags="all=-N -l"` and native checks in
the matching Milvus development container. Record actual executed results
separately from source-traced upgrade and distributed failure scenarios.

### Local validation (2026-10-19)

The matching development container completed the native build and six focused
native tests, including upload/read under an existing local manifest's `_index`.
The metautil path tests and focused DataCoord, importv2 and packed tests passed.
These cover old-task rejection across restart, new-layout task requests,
manifest-only recovery, copy conversion and GC protections. Real local FFI tests
also passed repeated manifest commits and rejection of an external-path addition.
Legacy manifest recovery tests passed separately. No live mixed-version cluster
or object-storage deployment was exercised.

With PCH disabled, the baseline build required the missing `<optional>` include
in `RegexQuery.h`; this change includes that build prerequisite.
