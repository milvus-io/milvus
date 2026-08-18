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

`transform_start_after_timetick` is persisted on the latest shard snapshot.
`OnL0Compact` advances it monotonically using the completed compaction event.

## Lifecycle

The normal Collection lifecycle is:

```text
absent
  -> OnCreateCollection: persist the empty (1,0) snapshot
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
Membership events write a new DataVersion. `OnL0Compact` overwrites the latest
version's persisted key and replaces the latest in-memory entry without
changing DataVersion; already issued Refs retain the prior immutable entry.

Collection drop is coordinated by CollectionMeta rather than by a DataView
state or tombstone. RootCoord first persists CollectionMeta as Dropping, then
DataViewManager deletes the Collection's snapshot prefix and removes its
manager state. New access no longer finds the Collection, while an already
issued `DataViewRef` continues to own its in-memory snapshot until `Deref`.

## Versioning

DataVersion is ordered lexicographically as `(streaming_version,
compact_version)`.

| Event | Version transition |
|---|---|
| Create | first snapshot `(1,0)` |
| Flush | `(S,C) -> (S+1,0)` |
| Import, copy completion, compact, external refresh, drop partition, truncate | `(S,C) -> (S,C+1)` |
| L0 compact | unchanged; overwrite and persist the latest snapshot |
| No membership or Manifest-version change | return the current version without persisting |
| Drop collection | delete all persisted snapshots; no new snapshot |

`streaming_version` is the publication epoch used by StreamingNode to decide
whether a flushed Segment must still participate in growing-side queries. Only
StreamingNode Flush performs that growing-to-sealed handoff. Every other
membership-changing event advances `compact_version`, even when it only adds
Segments. L0 compaction is the exception: it advances target Segment Manifest
versions and the shard delete frontier in the latest snapshot without changing
DataVersion.

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

Event producers pass final membership facts to the manager. `OnImport` receives
only the final loadable Segment IDs: when an imported Segment has been sorted,
the sorted Segment replaces the original rather than being published alongside
it. `OnCompact` receives `CompactFrom` and the final loadable `CompactTo`,
validates them against the latest snapshot, and persists their removal and
addition as one DataView commit. A retry whose inputs are already absent and
whose outputs are already present is a no-op. A partially applied or otherwise
inconsistent replacement is rejected without changing the snapshot.

Intermediate invisible Segments do not produce DataView membership events.
In particular, clustering compaction does not publish its temporary Segments;
it publishes exactly once after its final result Segments are visible and
loadable. Non-clustering compaction serializes its SegmentMeta mutation and
DataView commit under the SegmentMeta mutation lock so causal replacements on
the same Segment chain cannot reach DataView out of order.

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

`OnL0Compact` accepts one vchannel, the affected Segment Manifest versions, and
`transform_start_after_timetick`. It never changes membership or DataVersion.
Manifest versions may only increase; a lower version is rejected. The shard
timetick advances by max so out-of-order completion cannot move it backward.
Segments absent from the latest DataView are ignored rather than added.

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

## Persistence and recovery

Each version is stored as one complete snapshot under:

```text
coord/dv/{collectionID}/versions/{streaming}/{compact}
```

Normal commits append a new key. `OnL0Compact` persists by overwriting the
current latest key with the same DataVersion. A new `Latest` or `Get` observes
the replacement; a Ref acquired before the overwrite continues to own its old
in-memory snapshot until `Deref()`. The old and replacement entries share the
same per-DataVersion reference counter, so GC retains the persisted version
until every Ref from either generation has been released.

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
queries, or caller-supplied protected-version lists. `OnL0Compact` now accepts
explicit Manifest versions and a delete frontier from its event producer.
