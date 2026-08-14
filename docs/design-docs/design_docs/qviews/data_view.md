# DataView Design

This document defines the DataView-only contract implemented by the current
DataView PR. QueryView placement, SegmentMeta, Balancer, snapshot I/O, and
physical Segment GC are deliberately outside this component.

## Model

`DataViewOfCollection` is an immutable collection snapshot:

```text
Collection -> VChannel -> Partition -> loadable Segment IDs
```

The manager stores membership and the collection-level `DataVersion` only. A
caller must pass a `LoadableSegment` descriptor after its own loadability gate:

```go
type LoadableSegment struct {
    SegmentID   int64
    VChannel    string
    PartitionID int64
}
```

The manager does not query SegmentMeta or infer state, level, visibility,
importing, indexes, manifests, delete frontiers, or compaction lineage.

`transform_start_after_timetick` remains a wire-compatible derived field in
the protobuf, but this manager clears it and never persists or computes it.
Delete-frontier projection may be implemented later by an adapter that has
SegmentMeta access.

## Versioning

DataVersion is ordered lexicographically as `(streaming_version,
compact_version)`.

| Event | Version transition |
|---|---|
| Create | first snapshot `(1,0)` |
| Flush | `(S,C) -> (S+1,0)` |
| Import, copy completion, compact, external refresh, drop partition, truncate | `(S,C) -> (S,C+1)` |
| No membership change | return the current version without persisting |
| Drop collection | persist terminal marker; no new snapshot |

`streaming_version` is the publication epoch used by StreamingNode to decide
whether a flushed Segment must still participate in growing-side queries. Only
StreamingNode Flush performs that growing-to-sealed handoff. Every other
membership change advances `compact_version`, even when it only adds Segments.

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

Recovery reads only these snapshots and the collection drop markers. It picks
the maximum version as latest, initializes runtime ref counts to zero, and
rejects conflicting snapshots with the same version. It does not repair or
rebuild membership from any other metadata source.

## Garbage collection

`GarbageCollect(collectionID, retainLatest)` deletes DataView snapshots only.
A snapshot is retained if it is latest, among the newest `retainLatest`
versions, or protected by a live `DataViewRef`. Physical Segment deletion is a
separate concern.

Holding a `DataViewRef` currently protects only the DataView snapshot. Future
physical Segment GC integration must also treat every Segment in a live
referenced DataView as protected. Until QueryView references have been
recovered, that integration must fail closed. The current PR does not add this
cross-component protection path.

Dropping a collection marks it terminal and rejects new access. Existing Refs
remain valid until released; later integration code may finalize terminal
snapshot cleanup.

## Removed responsibilities

The manager no longer owns `SegmentStore`, loadability checks, resident/visible
split, temporary flush snapshots, `OnL0Compact`, delete-frontier projection,
repair/reconciliation, Balancer snapshots, Segment reference queries, or
caller-supplied protected-version lists.
