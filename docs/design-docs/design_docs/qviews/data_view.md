# DataView Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @congqixia
- Design Review: 2026-07-29

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
packed arrays. The manager also stores the collection-level `DataVersion`
(`streaming_version`, `compact_version`). A caller must pass a `LoadableSegment`
descriptor after its own loadability gate:

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
The projection function supplied to `Recompute` is the single source of
membership: it reads the current SegmentMeta and returns the loadable Segments.
Within one snapshot it keeps a Segment ID in only one VChannel/Partition:
re-adding it at the same location with the same Manifest version is a no-op, a
higher Manifest version updates it, and a lower version is a data-integrity
error. Changing its location is also a data-integrity error. It cannot detect
duplicate logical data stored under different Segment IDs; loadability,
completeness, and lineage are projection-function responsibilities.

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
cannot regress. `Recompute` rebuilds every Manifest version from the current
SegmentMeta, so a projection that reports a Manifest version lower than the
stored one is rejected as a data-integrity error. A projection that reports
zero for a Segment whose stored version is positive is preserved (the stored
version is kept) rather than regressed: after an L0 compaction advanced the
Manifest, a replay that has not yet observed the new manifest must not roll the
snapshot back.

TODO: The current branch deliberately does not persist or publish
`transform_start_after_timetick`. A safe monotonic frontier depends on a
StreamingNode-owned shard Flush barrier that is outside this PR. The required
producer protocol is described in
[Transform Start-After TimeTick](transform_start_after_timetick.md).

## Lifecycle

The normal Collection lifecycle is:

```text
absent
  -> OnCreateCollection: persist the empty (1,0) snapshot
  -> active: Recompute rebuilds the snapshot from SegmentMeta; PrepareFlush
     advances streaming for flush
  -> CollectionMeta Dropping: delete every persisted snapshot and manager state
  -> absent from DataViewManager
```

`OnCreateCollection` records all declared VChannels but no Segments. It is
idempotent: if the Collection already has a latest snapshot, it returns that
snapshot's version. The normal DDL path creates this snapshot before any
membership mutation.

An active Collection owns a `latest` entry and a map of retained versions.
Membership mutations are serialized by the Collection lock. `Recompute` clones
the latest immutable snapshot, rebuilds membership and Manifest versions from
the projection, canonicalizes the result, and only when the content changed
persists the complete snapshot and publishes it as the new in-memory latest. A
no-op recompute (unchanged content) returns the current version without
persisting, so any number of pending mutations collapse into a single snapshot
write ("only the last view is updated"). A failed catalog write leaves the
previous latest unchanged. Already issued Refs retain the prior entry, while
`Latest` returns the newly persisted entry.

Collection drop is coordinated by CollectionMeta rather than by a persisted
DataView tombstone. RootCoord first persists CollectionMeta as Dropping, then
DataViewManager tombstones the Collection in memory, removes its manager state,
and deletes the Collection's snapshot prefix. New access no longer finds the
Collection, while an already issued `DataViewRef` continues to own its
in-memory snapshot until `Deref`.

The in-memory `dropped` tombstone is what lets the drop avoid holding the
manager map lock across the prefix deletion: a late mutation (in-flight flush,
queued recompute) observes the tombstone and no-ops instead of recreating the
state or persisting an orphan key behind the prefix delete. The catalog prefix
delete runs under the per-Collection state lock only, so the rare Collection
DDL does not block membership events of other Collections for the duration of
the etcd round trip. The manager-global lock is never held across `state.mu`:
`lockStateForMutation` takes `state.mu` first and only then re-validates under
`m.mu.RLock`, so holding `m.mu` across `state.mu.Lock` would invert the order
and deadlock the whole coordinator.

## Versioning

DataVersion is ordered lexicographically as `(streaming_version,
compact_version)`.

| Event | Version transition |
|---|---|
| Create | first snapshot `(1,0)` |
| Bootstrap | first snapshot `(1,0)` for a Collection that predates DataView management (upgrade migration) |
| Flush (PrepareFlush commit) | `(S,C) -> (S+1,C)` |
| Compaction, import, copy, external refresh, drop partition, truncate, L0 manifest advance (Recompute) | `(S,C) -> (S,C+1)` when the rebuilt snapshot differs |
| No membership or Manifest-version change | return the current version without persisting |
| Drop collection | delete all persisted snapshots; no new snapshot |

`streaming_version` is the publication epoch used by StreamingNode to decide
whether a flushed Segment must still participate in growing-side queries. Only
the flush atomic txn performs that growing-to-sealed handoff. Every other
membership-changing mutation advances `compact_version`. In particular, L0
compaction advancing a Segment's Manifest version hard-triggers `compact_version
+1`: a Manifest-version change has no dedicated counter and is expressed as a
content change of the rebuilt snapshot. The flush path is the only
`streaming_version` advance, and it is
independent of membership content (a flush of a Segment already present is an
idempotent replay returning the current snapshot).

Flush membership is published atomically with SegmentMeta. `PrepareFlush`
builds the post-flush snapshot under the Collection lock; the caller composes it
into the same catalog transaction as the SegmentMeta actions
(`UpdateSegmentsInfoAndDataView`), then calls `commit()` on success (loads the
snapshot into memory and releases the lock) or `abort()` on failure (releases
the lock without touching memory). Both callbacks are idempotent. A DataView
that becomes visible therefore implies its SegmentMeta is already committed; the
previous behavior of reporting a DataView catalog failure to StreamingNode
after SegmentMeta committed is gone.

## Async reconciliation

TODO: `Recompute` is a stopgap. It rebuilds the whole snapshot from the
SegmentMeta projection, so every offline mutation (compaction, index build,
etc.) converges by reconstruction instead of being applied directly. Once the
DataCoord offline side (compact/index pipelines) is refactored, those owners
should update the DataView directly through specific DataView events, and this
projection-based reconciliation can be retired.

Compaction (mix/clustering/L0/sort/bump-schema-version), import commit, copy
completion, external refresh, drop partition, and truncate do not publish
membership events. Instead, after their SegmentMeta mutation commits, the owner
requests an asynchronous reconciliation by calling
`Manager.Recompute(ctx, collectionID)`. Two additional SegmentMeta mutations
advance a segment's Manifest version without changing membership and also
request a recompute: a stats task (e.g. sort) that records a new manifest, and
a `BatchUpdateManifest` V3 item that bumps a stored Manifest version.
The queue and its worker live inside the Manager: a non-blocking, per-Collection
deduplicated request (a Collection with a pending request is not queued again),
drained by a single worker that rebuilds the snapshot against the injected
SegmentMeta projection, so multiple pending mutations of one Collection
collapse into one snapshot write. An over-capacity queue drops the request with
a warning. The queue is best-effort - a lost entry (crash, failed recompute,
dropped request) is converged by the recovery rebuild, so the DataView is
eventually consistent with SegmentMeta. The projection is injected at
construction (`NewManager(catalog, project)` / `RecoverManager(..., project)`)
and the worker starts with the manager: it is bounded by the constructor's ctx
for `RecoverManager` and by the process lifetime for `NewManager`.

The projection runs under the Collection lock and reads SegmentMeta only: it is
non-blocking and must not re-enter the Manager. It resolves the loadable
membership - `Flushed`, non-L0, non-importing, visible Segments with a data
footprint - including each Segment's Manifest version parsed from its
SegmentMeta. A projection that reports a Manifest version lower than the stored
one is a data-integrity error (monotonicity); zero preserves the stored
version.

## Manager API

Mutation APIs on `dataview.Manager`:

```go
OnCreateCollection(ctx, event) (*viewpb.DataVersion, error)
OnBootstrapCollection(ctx, event) (*viewpb.DataVersion, error)
PrepareFlush(ctx, event) (view, commit func(), abort func(), err error)
Recompute(ctx, collectionID) error                       // async request
RecomputeNow(ctx, collectionID, project) (*viewpb.DataVersion, error) // sync
OnDropCollection(ctx, collectionID) (*viewpb.DataVersion, error)
```

The projection and the async worker are wired by the constructors, not by
post-construction calls: `NewManager(catalog, project)` builds a manager
serving synchronous operations plus async reconciliation (worker bounded by
the process lifetime), and `RecoverManager(ctx, catalog, validator, project,
liveCollectionIDs, collectionVChannels)` performs the whole recovery pass in
one call - it loads persisted snapshots, reconciles every recoverable live
Collection against the loadable SegmentMeta projection (SegmentMeta is the
source of truth; a no-op when the snapshot already matches), seeds a first
snapshot through the declared vchannel skeleton for a Collection that predates
DataView management (`collectionVChannels` supplies the declared VChannels),
and starts the worker bounded by ctx.

Access uses:

```go
Latest(ctx, collectionID) (DataViewRef, error)
Get(ctx, collectionID, version) (DataViewRef, error)
```

`DataViewRef.DataView()` and `Version()` return copies. The consumer owns the
reference and must call `Deref()` exactly when it finishes using the snapshot;
`Deref()` is idempotent.

`RecomputeNow` synchronously reconciles the Collection snapshot with the
projection: membership and Manifest versions are rebuilt from scratch (a
materialized view of SegmentMeta), so it absorbs every pending mutation since
the last write - compaction input retirement, output publication, L0 manifest
bumps, import, copy, refresh, truncate and partition drops are all expressed by
the projection's return value. A snapshot whose content is unchanged is not
persisted and the current DataVersion is returned. The async worker runs the
same step; `Recompute` is the non-blocking request form of it.

`Projector` is injected by datacoord and runs while the Collection lock is
held: it reads SegmentMeta under `segMu` (RLock) only, is non-blocking, and
must not call back into the Manager. Violating these obligations is a caller
bug (see the precondition on the type).

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

Every effective commit appends a new immutable key. A new `Latest` observes the
new entry, while `Get` can still acquire any exact retained version. Each
version has its own reference counter and remains protected from GC while a
QueryView holds its Ref.

Recovery scans every snapshot under `coord/dv`, groups them by Collection, and
asks the already-recovered CollectionMeta for a decision. A Created Collection
is recovered; a Creating, Dropping, Dropped, or nonexistent Collection is not
recoverable and all of its DataView keys are deleted. A CollectionMeta lookup
failure or unknown state aborts recovery without speculative cleanup. There is
no separate persisted DataView tombstone; CollectionMeta is the authoritative
lifecycle record.

For a valid Collection, recovery picks the maximum version as latest,
initializes runtime ref counts to zero, and keeps the first of two identical
snapshots under one version. It expands absent legacy
`segment_manifest_versions` entries to zero. A malformed snapshot (missing
DataVersion, invalid identity, or misaligned segment arrays) or a conflicting
second snapshot under one version is skipped with a warning instead of aborting
recovery, so a single bad key cannot brick the Coordinator. The snapshot is not
deleted automatically: automatic deletion could turn metadata corruption into
silent data loss. The bad key stays in etcd for operator inspection and manual
removal. The recovery barrier keeps external writes closed until recovery
completes.

After recovery, `RecoverManager` reconciles every recovered live Collection
against SegmentMeta inside the constructor: SegmentMeta is
the truth, and the DataView snapshot is rebuilt to match it. This converges any
event missed by the async queue (crash between SegmentMeta commit and drain,
dropped request, failed recompute) and is also the one-time seed for a
Collection created before DataView management existed: the bootstrap passes the
declared VChannels plus the currently loadable Segments (Flushed, non-L0,
visible, with a data footprint) resolved from SegmentMeta, and the subsequent
reconcile keeps it converged. A per-Collection failure is logged and skipped so
one anomalous Collection cannot brick Coordinator startup.

Recovery validates every discovered Collection against CollectionMeta before
deleting any stale prefix. This prevents a transient validation failure from
causing partial, speculative cleanup.

The combined Coordinator uses one in-process recovery barrier. RootCoord first
recovers CollectionMeta, then DataCoord and QueryCoord recover their metadata;
DataCoord's Collection details are part of this synchronous initialization and
are no longer left to a background reload. MixCoord registers the DataCoord,
QueryCoord, RootCoord, and WAL DDL callbacks only after all three Coordinator
initialization/start phases have completed, and only then transitions to
Healthy. The StreamingCoord broadcaster may discover pending callback work but
cannot replay it during recovery.

The distributed MixCoord constructs the gRPC server and registers its services
before recovery, but delays `grpcServer.Serve` until the same recovery barrier
is ready in both normal and active/standby deployments. A standby therefore
does not expose Coordinator gRPC until it wins election and completes recovery;
its liveness is reported through the process HTTP health endpoint, which treats
StandBy as healthy. Consequently external RPCs cannot enter Coordinator or
StreamingCoord handlers during recovery. In-process Coordinator calls and
Coordinator-owned background loops are unaffected. Normal CollectionMeta and
DataViewManager APIs do not carry or wait on the barrier.

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
projection, event-driven repair, Balancer snapshots, Segment reference queries,
or caller-supplied protected-version lists. The event API is reduced to
Create/Bootstrap/PrepareFlush/Recompute/Drop; membership is a materialized view
of SegmentMeta rather than an event-accumulated log. The delete frontier remains
a TODO until the StreamingNode shard barrier is implemented.
