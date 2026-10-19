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

A task's durable path version pins both artifact placement and publication mode
at creation. Version-2 builds finish in the manifest even after the switch is
turned off; version-0/1 builds finish in etcd even after it is turned on. Completion
uses the coordinator's persisted task version, not the current switch or the
worker's claimed version. Worker results still undergo layout validation.
Retries and coordinator restarts preserve this choice. Pre-version-2 tasks also
finish in etcd because their metadata cannot establish an on-mode choice for the
new layout. No artifact migration or automatic rebuild is required by a switch
change, and external index paths are never added to a manifest.

Upgrade DataCoord, index workers and QueryNodes before enabling this behavior.
An older native worker treats unknown path versions as collection-rooted and
can echo the requested version despite writing the old layout. The existing
switch remains an operator gate. DataCoord additionally checks the selected
worker's `QuerySlot.segment_rooted_indexes` capability before dispatching a v2
build or copy destination. A missing capability fails the task with an explicit
upgrade-required reason, without dispatching or downgrading its layout. RPC
failures remain retryable. Downgrading readers while version-2 artifacts exist
is unsupported.

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
or obsolete attempt files. This check runs inside the existing `insert_log`
traversal, before the generic registered-V3 skip; there is no second recursive
LIST of the prefix. It remains active even when the write switch is off, because
historical v2 files still need reclamation. Dropped-segment GC still owns whole
segment deletion. Historical noncanonical local namespace orphan-scan gaps
remain as documented in the storage path contract.

## Validation and PR organization

One layer on `master` contains the protocol, producer/consumer changes, copy
and GC integration, regression tests and documentation. No intermediate layer
publishes version 2 before the consumers understand it.

Focused validation covers layout selection, native upload/read, repeated real
manifest commits, creation-time publication across switch changes and restart, version
mismatch, restart/load metadata, conversion from all source layouts, exclusion
of unpinned builds, copy-task persistence, and orphan/snapshot/import GC rules.
Run Go tests with `-tags dynamic,test -gcflags="all=-N -l"` and native checks in
the matching Milvus development container. Record actual executed results
separately from source-traced upgrade and distributed failure scenarios.

### Local validation (2026-10-19)

The matching development container completed the native build and six focused
native tests, including upload/read under an existing local manifest's `_index`.
The metautil path tests and focused DataCoord, importv2 and packed tests passed.
These covered the original old-task rejection across restart, new-layout task requests,
manifest-only recovery, copy conversion and GC protections. Real local FFI tests
also passed repeated manifest commits and rejection of an external-path addition.
Legacy manifest recovery tests passed separately. No live mixed-version cluster
or object-storage deployment was exercised.

The subsequent creation-time publication and review fixes were validated in
the matching development container through the data-backed Docker daemon.
Focused tests passed for version-0/1 off-to-on and version-2 on-to-off commits,
with and without a coordinator restart; manifest recovery; rejection of worker
layout changes; old-worker capability rejection and durable task failure; and
worker capability advertisement. The GC tests passed for the merged traversal,
including a single insert_log listing with the write switch off, live V3 data,
snapshots, active builds, importing segments, malformed paths and removal errors.
The DataNode service suite could not initialize because etcd was unreachable;
capability advertisement was also verified directly without registration.
A broader GC/manifest run was interrupted before completion and is not counted
as passing. No live mixed-version cluster validation was performed.

With PCH disabled, the baseline build required the missing `<optional>` include
in `RegexQuery.h`; this change includes that build prerequisite.
