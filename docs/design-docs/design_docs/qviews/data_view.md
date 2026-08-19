# DataView Design

This document defines the DataView-only contract implemented by the current
DataView PR. QueryView placement, SegmentMeta, Balancer, snapshot I/O, and
physical Segment GC are deliberately outside this component.

## Model

`DataViewOfCollection` is an immutable collection snapshot once handed to a
consumer through `DataViewRef`:

```text
Collection -> VChannel -> Partition -> loadable (Segment ID, Manifest version)
```

Each partition stores `segment_ids` and `segment_manifest_versions` as parallel
packed arrays. The manager also stores the collection-level `DataVersion`. A
caller must pass a `LoadableSegment` descriptor after its own loadability gate:

```go
type LoadableSegment struct {
    SegmentID       int64
    VChannel        string
    PartitionID     int64
    ManifestVersion int64
}
```

The manager does not query SegmentMeta or infer state, level, visibility,
importing, indexes, manifest versions, delete frontiers, or compaction lineage.
The event producer supplies the Manifest version or leaves it at zero.
Within one snapshot it keeps a Segment ID in only one VChannel/Partition:
re-adding it at the same location with the same Manifest version is a no-op, a
higher Manifest version updates it, and a lower version is a data-integrity
error. Changing its location is also a data-integrity error. It cannot detect
duplicate logical data stored under different Segment IDs; loadability,
completeness, and lineage are event-producer responsibilities.

Manifest version has two modes:

- `0` means indirect loading. QueryCoord watches Coordinator SegmentMeta
  changes, resolves the latest complete `SegmentInfo`, and freezes it into the
  corresponding QueryView/load operation. This covers current and legacy
  Segments, non-StorageV3 Segments, unstable base paths, and manifests that are
  not yet suitable for direct loading.
- A positive value identifies a committed canonical StorageV3 Manifest.
  QueryNode can construct
  `{rootPath}/insert_log/{collectionID}/{partitionID}/{segmentID}/_metadata/manifest-{version}.avro`
  and derive all Segment loading information from it. Such a Segment no longer
  needs the Coordinator SegmentMeta watcher for data updates.

Zero is a resolution mode, not a comparable data revision. Existing persisted
DataViews omit `segment_manifest_versions`; recovery expands the missing values
to zero. Manifest versions are monotonic within successive DataViews: an
existing Segment may keep its version or advance to a higher version, but it
cannot regress. Coordinator SegmentMeta changes alone do not rewrite DataView;
an advance must be carried by one of the existing Segment publication events
or by `OnL0Compact`. There is no generic Manifest-update API. Current membership
event producers publish zero; L0 compaction can replace it with the committed
Manifest version after updating the target Segment.

TODO: The current branch deliberately does not persist or publish
`transform_start_after_timetick`. A safe monotonic frontier depends on a
StreamingNode-owned shard Flush barrier that is outside this PR. The required
producer protocol is described in
[Transform Start-After TimeTick](transform_start_after_timetick.md).

## Lifecycle

The normal Collection lifecycle is:

```text
absent
  -> OnCreateCollection: persist the empty (1,0,0) snapshot
  -> active: append immutable snapshots for membership changes
  -> CollectionMeta Dropping: delete every persisted snapshot and manager state
  -> absent from DataViewManager
```

`OnCreateCollection` records all declared VChannels but no Segments. It is
idempotent: if the Collection already has a latest snapshot, it returns that
snapshot's version. The normal DDL path creates this snapshot before any
Segment publication event.

An active Collection owns a `latest` entry and a map of retained versions.
Every `On*` call is serialized by the Collection lock, clones the latest
immutable snapshot, applies the mutation, canonicalizes the result, persists
the complete snapshot, and only then publishes it as the new in-memory latest.
A failed catalog write therefore leaves the previous latest unchanged.
Membership events and effective `OnL0Compact` updates write a new immutable
DataVersion. Already issued Refs retain the prior entry, while `Latest` returns
the newly persisted entry.

Collection drop is coordinated by CollectionMeta rather than by a DataView
state or tombstone. RootCoord first persists CollectionMeta as Dropping, then
DataViewManager deletes the Collection's snapshot prefix and removes its
manager state. New access no longer finds the Collection, while an already
issued `DataViewRef` continues to own its in-memory snapshot until `Deref`.

## Versioning

DataVersion is ordered lexicographically as `(streaming_version,
compact_version, transform_version)`.

| Event | Version transition |
|---|---|
| Create | first snapshot `(1,0,0)` |
| Flush | `(S,C,T) -> (S+1,0,0)` |
| Import, copy completion, compact, external refresh, drop partition, truncate | `(S,C,T) -> (S,C+1,0)` |
| L0 compact | `(S,C,T) -> (S,C,T+1)` when a Manifest version advances |
| No membership or Manifest-version change | return the current version without persisting |
| Drop collection | delete all persisted snapshots; no new snapshot |

`streaming_version` is the publication epoch used by StreamingNode to decide
whether a flushed Segment must still participate in growing-side queries. Only
StreamingNode Flush performs that growing-to-sealed handoff. Every other
membership-changing event advances `compact_version`, even when it only adds
Segments. L0 compaction advances `transform_version`: it changes target Segment
Manifest versions without changing membership.
Streaming and compact advances reset their lower-order version components
because their new snapshot already absorbs the latest transform state.

StreamingNode Flush has a stronger completion contract: a successful `OnFlush`
means every supplied Segment is immediately allowed to load on QueryNode and is
already part of the published DataView. There is no temporary/unavailable
flush state.

The current no-op behavior returns the latest DataVersion. That behavior is
sufficient for membership idempotency but is not, by itself, a stable
`sealed_at_streaming_version` contract. Before the returned Flush version is
used by StreamingNode, integration code must durably preserve the original
publication version for each flushed Segment; otherwise a retry after another
Flush could observe a newer StreamingVersion. The mapping and its delivery to
StreamingNode are outside this DataView-only PR.

The DataView snapshot commit is atomic within DataView's catalog. SegmentMeta
and DataView are not one transaction in this PR. Event owners publish only
after their Segment operation reaches the loadable completion point, and may
retry the same `On*` event safely because membership no-ops reuse the current
version. Recovery never hides a missed event by scanning SegmentMeta; durable
cross-catalog delivery is a separate integration concern.

Removal events use a deliberate fail-safe ordering. Drop-partition and truncate
first remove the affected membership from DataView and only then delete it from
SegmentMeta. If the second operation fails, the data is hidden early and the
event can retry idempotently; the inverse order could leave already-deleted data
queryable after a DataView write failure.

Event producers pass final membership facts to the manager. `OnImport` receives
only the final loadable Segment IDs: when an imported Segment has been sorted,
the sorted Segment replaces the original rather than being published alongside
it. `OnCompact` receives `CompactFrom` and the final loadable `CompactTo`,
validates them against the latest snapshot, and persists their removal and
addition as one DataView commit. A retry whose inputs are already absent and
whose outputs are already present is a no-op. A partially applied or otherwise
inconsistent replacement is rejected without changing the snapshot.

SortCompaction follows the same replacement contract. Its flushed input
Segment is immediately loadable and is first published by `OnFlush`; it is not
kept invisible while waiting for sorting. After the sorted output Segment is
durably loadable, the compaction owner publishes one `OnCompact` event that
removes the flushed input and adds the final output, advancing CompactVersion.
If SegmentMeta replacement succeeds but DataView publication fails, the
persisted compaction lineage makes completion replayable and the task retries
the idempotent DataView replacement. A future implementation should perform
sorting of newly flushed Segments on StreamingNode and publish the final sorted
Segment directly, eliminating this intermediate replacement.

Intermediate invisible Segments do not produce DataView membership events.
In particular, clustering compaction does not publish its temporary Segments;
it publishes exactly once after its final result Segments are visible and
loadable. Non-clustering compaction serializes its SegmentMeta mutation and
DataView commit under the SegmentMeta mutation lock so causal replacements on
the same Segment chain cannot reach DataView out of order.

Import commit keeps the DataView catalog write outside the ImportMeta write
lock. `HandleCommitVchannel` first commits the per-vchannel ImportMeta state and
releases that lock, then calls `OnImport` with the final loadable membership.
An `OnImport` failure fails the RPC so StreamingNode retries; a retry republishes
the same idempotent membership even when ImportMeta already records the
vchannel as committed.

## Manager API

Mutation APIs are the `On*` methods on `dataview.Manager`. They return a
`DataVersion`, not an implicitly retained view. Access uses:

```go
Latest(ctx, collectionID) (DataViewRef, error)
Get(ctx, collectionID, version) (DataViewRef, error)
```

`DataViewRef.DataView()` and `Version()` return copies. The consumer owns the
reference and must call `Deref()` exactly when it finishes using the snapshot;
`Deref()` is idempotent.

`OnL0Compact` accepts one vchannel and the affected Segment Manifest versions.
It never changes membership. An effective Manifest-version update advances
TransformVersion and appends an immutable snapshot; an event that advances no
Manifest version is a no-op. Manifest versions may only increase; a lower
version is rejected. Segments absent from the latest DataView are ignored
rather than added.

`Latest` returns a Ref to the current latest entry and `Get` returns a Ref to
the exact requested version. An unknown Collection, a nil version, or a version
that has already been collected returns `(nil, nil)`.

When QueryView integration is introduced, a QueryView must retain its exact
`DataViewRef` through Preparing, Ready, Up, Down, Unrecoverable, and Dropping,
and release it only after reaching Dropped. Recovered QueryViews must reacquire
their persisted DataVersion before GC is enabled. This lifecycle integration is
outside the current PR.

Because DataVersion and DataViewRef are Collection-scoped while QueryView is
Shard-scoped, one Shard can otherwise keep an old complete Collection snapshot
pinned. QueryView generation may coalesce intermediate DataVersions, but every
loaded Shard must eventually converge to the latest Collection DataVersion so
old Collection refs can be released.

TransformVersion is a soft QueryView trigger. A QueryView at an older
TransformVersion remains query-correct because it keeps its old DataViewRef and
consumes the equivalent deletes from TransformLog. It is not invalidated by
each L0 completion. The system must nevertheless generate a newer QueryView
eventually—usually piggybacked on the next hard update or balance, or forced by
maintenance policy—so the old Manifest can be garbage-collected. TransformLog
frontier and retention integration is deferred to the TODO design linked
above. QueryView scheduling is outside the current DataView-only PR.

## Persistence and recovery

Each version is stored as one complete snapshot under:

```text
coord/dv/{collectionID}/versions/{streaming}/{compact}/{transform}
```

Every effective commit, including `OnL0Compact`, appends a new immutable key.
For compatibility, versions whose TransformVersion is zero retain the legacy
two-component key; positive TransformVersions use the third path component.
A new `Latest` observes the new entry, while `Get` can still acquire any exact
retained version. Each version has its own reference counter and remains
protected from GC while a QueryView holds its Ref.

Recovery scans every snapshot under `coord/dv`, groups them by Collection, and
asks the already-recovered CollectionMeta for a decision. A Created Collection
is recovered; a Creating, Dropping, Dropped, or nonexistent Collection is not
recoverable and all of its DataView keys are deleted. A CollectionMeta lookup
failure or unknown state aborts recovery without speculative cleanup. There is
no separate persisted DataView tombstone; CollectionMeta is the authoritative
lifecycle record.

For a valid Collection, recovery picks the maximum version as latest,
initializes runtime ref counts to zero, and rejects conflicting snapshots with
the same version. It expands absent legacy `segment_manifest_versions` entries
to zero but does not repair or rebuild membership from SegmentMeta or any other
metadata source.

Recovery validates every discovered Collection against CollectionMeta before
deleting any stale prefix. This prevents a transient validation failure from
causing partial, speculative cleanup.

The combined Coordinator uses one in-process recovery barrier. RootCoord first
recovers CollectionMeta, then DataCoord and QueryCoord recover their metadata;
DataCoord's Collection details are part of this synchronous initialization and
are no longer left to a background reload. MixCoord registers the DataCoord,
QueryCoord, RootCoord, and WAL DDL callbacks only after all three Coordinator
initialization/start phases have completed and MixCoord has entered Healthy
state, so the StreamingCoord broadcaster may discover pending callback work but
cannot replay it during recovery.

The distributed MixCoord constructs the gRPC server and registers its services
before recovery, but delays `grpcServer.Serve` until the same recovery barrier
is ready. Consequently external RPCs cannot enter Coordinator handlers during
recovery, while in-process Coordinator calls and Coordinator-owned background
loops are unaffected. Normal CollectionMeta and DataViewManager APIs do not
carry or wait on the barrier.

## Garbage collection

`GarbageCollect(collectionID, retainLatest)` deletes DataView snapshots only.
A snapshot is retained if it is latest, among the newest `retainLatest`
versions, or protected by a live `DataViewRef`. Physical Segment deletion is a
separate concern.

`retainLatest` is normalized to at least one. The current DataCoord metadata GC
invokes this operation for live Collections with `retainLatest = 1`.

Holding a `DataViewRef` currently protects only the DataView snapshot. Future
physical Segment GC integration must also treat every Segment in a live
referenced DataView as protected. Until QueryView references have been
recovered, that integration must fail closed. The current PR does not add this
cross-component protection path.

Dropping a collection persists the CollectionMeta Dropping state before deleting
all DataView snapshots. Existing Refs remain valid from their in-memory snapshot
until released, while new lookups return no DataView after the manager removes
the Collection state. If the process crashes between the two writes, recovery
recognizes the CollectionMeta tombstone and removes the remaining DataView keys.

## Removed responsibilities

The manager no longer owns `SegmentStore`, loadability checks, resident/visible
split, temporary flush snapshots, SegmentMeta-derived delete-frontier
projection, repair/reconciliation, Balancer snapshots, Segment reference
queries, or caller-supplied protected-version lists. `OnL0Compact` accepts
explicit Manifest versions only. The delete frontier remains a TODO until the
StreamingNode shard barrier is implemented.
