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
| Flush, import, copy completion | `(S,C) -> (S+1,0)` |
| Compact, drop partition, truncate | `(S,C) -> (S,C+1)` |
| External refresh | rewrite if anything is removed, otherwise streaming addition |
| No membership change | return the current version without persisting |
| Drop collection | persist terminal marker; no new snapshot |

StreamingNode Flush has a stronger completion contract: a successful `OnFlush`
means every supplied Segment is immediately allowed to load on QueryNode and is
already part of the published DataView. There is no temporary/unavailable
flush state.

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

Dropping a collection marks it terminal and rejects new access. Existing Refs
remain valid until released; later integration code may finalize terminal
snapshot cleanup.

## Removed responsibilities

The manager no longer owns `SegmentStore`, loadability checks, resident/visible
split, temporary flush snapshots, `OnL0Compact`, delete-frontier projection,
repair/reconciliation, Balancer snapshots, Segment reference queries, or
caller-supplied protected-version lists.
