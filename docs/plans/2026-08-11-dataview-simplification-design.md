# DataView Simplification Design

## Status

This document records the agreed implementation scope for the DataView-only
PR. The implementation is intentionally limited to DataView production,
versioned access, reference ownership, recovery, and DataView snapshot GC.

## Scope

DataViewManager owns only:

1. immutable loadable-segment membership snapshots;
2. collection-level `DataVersion` allocation;
3. the business response APIs that publish membership changes;
4. exact-version access through `DataViewRef`;
5. runtime reference counts and DataView snapshot GC;
6. recovery from DataView's own persisted records.

This PR does not integrate DataView with SegmentMeta, Balancer, QueryView,
StreamingNode, physical Segment GC, or snapshot restore.

## Core Model

A DataView contains only:

```text
Collection
  -> VChannel
    -> Partition
      -> loadable Segment IDs
```

Only a Segment that is already allowed to be loaded by QueryNode may be passed
to DataViewManager. The caller owns all business completion gates and passes
the minimum membership descriptor:

```go
type LoadableSegment struct {
    SegmentID   int64
    VChannel    string
    PartitionID int64
}
```

DataViewManager does not query SegmentMeta and does not check segment state,
level, importing state, visibility, compaction lineage, indexes, or manifests.

`DeleteApplyStartAfterTimetick` is not managed or persisted by
DataViewManager. It is a projection that may be derived after obtaining a
DataView by looking up the SegmentMeta for the fixed membership. Consequently,
L0 compaction is not a DataView event and `OnL0Compact` is removed.

The protobuf keeps `transform_start_after_timetick` as a compatibility field
for future projection adapters. The membership-only Manager canonicalizes it
to zero, so it is neither a persisted DataView responsibility nor a source of
DataVersion changes in this PR.

## Streaming Flush Constraint

A Segment produced by StreamingNode flush is immediately loadable by
QueryNode. Therefore a successful `OnFlush` always means:

1. every supplied Segment was accepted into membership;
2. the immutable DataView snapshot was persisted;
3. the latest DataView was advanced;
4. the returned `DataVersion` identifies that published snapshot.

There is no temporary or unavailable DataView, no delayed flush publication,
and no `latestResident/latestVisible` split.

## DataVersion

DataVersion remains:

```text
(streaming_version, compact_version)
```

Transitions are explicit:

- StreamingNode Flush: `(S, C) -> (S+1, 0)`;
- every other membership change: `(S, C) -> (S, C+1)`;
- no membership change: return the current version without persistence;
- first persisted DataView: `(1, 0)`.

`streaming_version` is the publication epoch used by StreamingNode to decide
whether a flushed Segment must still participate in growing-side queries. Only
`OnFlush` performs that growing-to-sealed handoff. Import, copy completion,
compaction, external refresh, partition drop, and truncate all advance
`compact_version`. The core commit path never infers an operation from
SegmentMeta or compaction lineage.

## DataViewManager Interface

```go
type Manager interface {
    OnCreateCollection(ctx context.Context, event CreateCollectionDataViewEvent) (*viewpb.DataVersion, error)
    OnFlush(ctx context.Context, event FlushDataViewEvent) (*viewpb.DataVersion, error)
    OnImport(ctx context.Context, event ImportDataViewEvent) (*viewpb.DataVersion, error)
    OnCopySegmentComplete(ctx context.Context, event CopySegmentCompleteDataViewEvent) (*viewpb.DataVersion, error)
    OnCompact(ctx context.Context, event CompactDataViewEvent) (*viewpb.DataVersion, error)
    OnExternalRefresh(ctx context.Context, event ExternalRefreshDataViewEvent) (*viewpb.DataVersion, error)
    OnDropPartition(ctx context.Context, event DropPartitionDataViewEvent) (*viewpb.DataVersion, error)
    OnTruncate(ctx context.Context, event TruncateDataViewEvent) (*viewpb.DataVersion, error)
    OnDropCollection(ctx context.Context, collectionID int64) (*viewpb.DataVersion, error)

    Latest(ctx context.Context, collectionID int64) (DataViewRef, error)
    Get(ctx context.Context, collectionID int64, version *viewpb.DataVersion) (DataViewRef, error)

    GarbageCollect(ctx context.Context, collectionID int64, retainLatest int) error
}
```

`On*` methods return a version, not a Ref. Mutation callers do not implicitly
own a DataView. A consumer that needs the view calls `Get` or `Latest` and must
release the returned Ref.

## Event Contracts

```go
type CreateCollectionDataViewEvent struct {
    CollectionID int64
    VChannels    []string
}

type FlushDataViewEvent struct {
    CollectionID int64
    Segments     []LoadableSegment
}

type ImportDataViewEvent struct {
    CollectionID int64
    Segments     []LoadableSegment
}

type CopySegmentCompleteDataViewEvent struct {
    CollectionID int64
    Segments     []LoadableSegment
}

type CompactDataViewEvent struct {
    CollectionID int64
    CompactFrom  []int64
    CompactTo    []LoadableSegment
}

type ExternalRefreshDataViewEvent struct {
    CollectionID int64
    AddSegments  []LoadableSegment
    DropSegments []int64
}

type DropPartitionDataViewEvent struct {
    CollectionID int64
    PartitionIDs []int64
}

type TruncateDataViewEvent struct {
    CollectionID int64
    SegmentIDs   []int64
}
```

Version behavior:

| API | Transition |
|---|---|
| `OnCreateCollection` | first view `(1,0)` |
| `OnFlush` | `(S,C) -> (S+1,0)` |
| `OnImport` | `(S,C) -> (S,C+1)` |
| `OnCopySegmentComplete` | `(S,C) -> (S,C+1)` |
| `OnCompact` | `(S,C) -> (S,C+1)` |
| `OnExternalRefresh` | `(S,C) -> (S,C+1)` for any membership change |
| `OnDropPartition` | `(S,C) -> (S,C+1)` when membership is removed |
| `OnTruncate` | `(S,C) -> (S,C+1)` when membership is removed |
| `OnDropCollection` | terminal marker; no new version |

Repeated events that produce the same membership are successful no-ops and
return the current DataVersion.

## Unified Commit Path

All membership events translate into:

```go
type membershipMutation struct {
    Add             []LoadableSegment
    Remove          []int64
    RemovePartition map[int64]struct{}
}
```

The collection-scoped commit path:

1. rejects a terminal collection;
2. clones the latest immutable snapshot;
3. applies removals and additions;
4. validates that one Segment ID has one location;
5. sorts and deduplicates shards, partitions, and Segment IDs;
6. returns the current version for a membership no-op;
7. allocates the next DataVersion;
8. persists the complete snapshot;
9. installs it as the latest in-memory view.

The commit above is atomic for DataView's own catalog state. SegmentMeta and
DataView remain separate commits in this PR: an `On*` caller must invoke the
Manager only after its business operation has made every added Segment
loadable. Retrying the same `On*` event is safe because an unchanged membership
is a no-op and does not allocate another version. Durable cross-catalog event
delivery is intentionally left to a later integration change; it is not
implemented by teaching DataViewManager to scan SegmentMeta during recovery.

Membership idempotency does not make the version returned by a retried Flush a
stable per-Segment publication version. If Segment A first joins at S=11, the
response is lost, and Segment B later advances the collection to S=12, a retry
for A currently returns the latest version S=12. Before StreamingNode consumes
the Flush result as `sealed_at_streaming_version`, integration code must
durably preserve the original `(collectionID, segmentID) -> streamingVersion`
mapping. The mapping, a single-Segment or equivalent unambiguous Flush
contract, and delivery to StreamingNode are deferred beyond this PR.

## Reference Ownership

Every public access returns a Ref before exposing the view:

```go
type DataViewRef interface {
    DataView() *viewpb.DataViewOfCollection
    Version() *viewpb.DataVersion
    Deref()
}
```

`Latest` and `Get` locate the exact immutable version and increment its runtime
reference count while holding the same collection lock used by GC. This is the
linearization point that removes the fetch-then-pin race.

`DataView()` returns a clone so consumers cannot mutate Manager-owned state.
`Deref()` is non-blocking and idempotent; a double call does not decrement the
shared count twice.

Future QueryView integration owns the Ref for the complete QueryView lifecycle:
acquire the exact DataVersion while creating or recovering a QueryView, retain
it through Preparing/Ready/Up/Down/Unrecoverable/Dropping, and release it only
after Dropped. Because DataVersion and DataViewRef are Collection-scoped while
QueryView is Shard-scoped, QueryView generation may coalesce intermediate
DataVersions, but every loaded Shard must eventually converge to the latest
Collection DataVersion so old Collection refs can be released.

There are no public `PinDataView`, `UnpinDataView`, or
`RecoverDataViewReference` APIs. Recovery consumers use ordinary `Get`.

## Recovery

Recovery reads only DataView's own persisted snapshots and terminal markers.
For each collection it installs every retained version, selects the maximum
DataVersion as latest, and initializes runtime reference counts to zero.

Recovery does not:

- scan or reconcile SegmentMeta;
- create a new DataVersion;
- classify missed flush or compaction events;
- rebuild resident/visible states;
- derive delete frontiers.

Missing or corrupt DataView persistence is a DataView integrity failure. It is
not repaired by inventing membership from another subsystem.

## Garbage Collection

GC deletes DataView snapshots only. It does not decide whether physical Segment
data can be removed.

The later physical Segment GC integration must treat every Segment contained
by a live referenced DataView as protected. QueryView recovery must reacquire
all persisted DataVersion refs before that GC path is enabled, or the GC path
must fail closed until recovery completes. Neither the Segment protection
adapter nor QueryView recovery ordering is implemented in this PR.

A version is retained when any of these conditions is true:

```text
it is the latest published version
or it is in retainLatest newest versions
or its runtime ref_count is greater than zero
```

Callers no longer pass protected-version lists. Ref protection is an internal
invariant of the Manager that both serves and deletes DataViews.

## DropCollection

`OnDropCollection` durably marks the collection terminal and rejects new
`Latest` and `Get` acquisitions. Existing Refs remain valid. Historical
snapshots are deleted only after their Refs are released, through DataView GC
or terminal finalization introduced by a later integration PR.

## Removed Concepts

- `SegmentStore` and the DataView-local Segment model;
- loadability inference inside DataViewManager;
- `TemporaryUnavailable` and `AllowInvisibleTo`;
- `latestResident` and `latestVisible`;
- `OnL0Compact` and delete-frontier refresh;
- repair/reconciliation from SegmentMeta;
- Balancer snapshots and Segment metadata projections;
- `ShardTimeTicks` and `IsSegmentReferenced`;
- public Pin/Unpin reference bookkeeping;
- caller-supplied protected versions for GC.

## Required Invariants

1. Every supplied `LoadableSegment` is already allowed to load.
2. Every successful membership-changing `On*` persists one complete immutable snapshot.
3. A successful `OnFlush` immediately publishes all supplied Segments.
4. Published DataVersion never rolls back.
5. A no-op retry does not allocate a new version.
6. One Segment ID occurs at most once in a DataView.
7. Every public view access acquires a Ref before exposing the view.
8. A live Ref prevents its exact version from being collected.
9. The latest version is never collected.
10. Recovery never derives or invents membership from SegmentMeta.
