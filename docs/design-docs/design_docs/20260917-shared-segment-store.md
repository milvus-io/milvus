# Centralized Metacache for MixCoord

Track: Memory Deduplication
Status: Draft

## Motivation

In MixCoord mode DataCoord (DC) and QueryCoord (QC) run in a single
process yet hold separate copies of segment and collection metadata.
Segment duplication happens via GetRecoveryInfoV2, which allocates
new datapb.SegmentInfo protos despite the call being a direct Go
method call (no gRPC). Collection schema is duplicated three ways:
RootCoord (authoritative), DC (cached collectionInfo), and QC
(per-collection Schema).

This duplication wastes memory and creates consistency windows where
DC and QC disagree on segment state. The distributed coordinator
mode has been removed (PR #41006), so all coordinators always share
a process — making a shared in-memory store both safe and
straightforward.

## Goals

- Eliminate duplicate segment proto storage between DC and QC.
- Provide a single source of truth for segment and collection
  metadata in MixCoord.
- Preserve DC's ownership of transient runtime state (allocations,
  compaction locks, flush timing) outside the shared store.
- Centralize segment metric emission so transitions are tracked
  in one place instead of scattered across mutation sites.
- Maintain the existing persistence model (etcd catalog) unchanged.

## Non-Goals

- Replacing the etcd catalog or persistence layer.
- Eviction of dropped segments (separate follow-up work).
- Sharing index metadata (remains in DC's indexMeta).

## Architecture

```
                MixCoord process (proposed)
    ┌─────────────────────────────────────────────┐
    │           metacache.MetaStore                │
    │  ┌──────────────────────────────────────┐   │
    │  │ segments     map[id]*datapb.SegmentInfo│   │
    │  │ segCollIdx   map[coll]Set[id]        │   │
    │  │ segChanIdx   map[ch]Set[id]          │   │
    │  │                                      │   │
    │  │ collections  map[id]*CollectionInfo  │   │
    │  │                                      │   │
    │  │ checkpoints  map[ch]*MsgPosition     │   │
    │  └──────────────┬───────────────────────┘   │
    │                 │                            │
    │       ┌─────────┼──────────┐                │
    │       │ MetaStore          │ MetaView        │
    │       │ (read-write)       │ (read-only)     │
    │       ▼                    ▼                │
    │  DataCoord            QueryCoord            │
    │  ┌──────────────┐    ┌──────────────┐       │
    │  │ dcState:     │    │ TargetManager │       │
    │  │  allocations │    │  segIDs only  │       │
    │  │  flushTime   │    │  (resolves    │       │
    │  │  compacting  │    │   details     │       │
    │  │  (transient  │    │   from        │       │
    │  │   only)      │    │   MetaView)   │       │
    │  └──────────────┘    └──────────────┘       │
    └─────────────────────────────────────────────┘
```

## Interface Design

The store exposes two interfaces backed by a single struct
protected by sync.RWMutex:

**MetaView** (read-only, what QC is given):
- Segment lookups by ID, by ID set, by collection, by channel, and all
- Collection metadata reads (schema, partitions, vchannels, properties)

MetaView deliberately exposes neither state-based segment lookup nor
channel-checkpoint reads. There is no state index, and the channel
checkpoint accessors live on MetaStore because only DC needs them.

**MetaStore** (read-write, embeds MetaView, used only by DC):
- Segment CRUD with automatic index maintenance
- Channel checkpoint updates with timestamp-based deduplication
  and catalog persistence
- Channel lifecycle (add, drop, existence checks)
- Batch operations for compaction and flush workflows
- Collection metadata writes
- GC confirmation via catalog
- Concurrent per-collection segment loading from catalog

MetaStore is the backbone of the production MixCoord path. The
no-arg constructors are gone, but datacoord.Server.initMeta keeps
nil fallbacks for catalog and store so a standalone DataCoord can
still be constructed in tests.

## DataCoord Integration

DC's SegmentsInfo drops its local segments map and secondary
indexes entirely. MetaStore replaces them. Only DC-specific
transient runtime state stays local in a separate struct:

- allocations: growing segment row ID reservations
- lastFlushTime: throttle for flush decisions
- isCompacting: prevents concurrent compaction scheduling
- lastWrittenTime: tracks write activity

DC's GetSegment assembles a SegmentInfo from the shared proto
plus transient state on each call. Callers must not mutate the
returned proto — DC clones before modifying.

DC's meta struct drops its local channel checkpoint map. All
checkpoint operations (update, get, drop, mark-dropped) are
proxied to MetaStore. DC retains per-channel locks for
serializing same-channel updates, and a condition variable for
WatchChannelCheckpoint. DC-specific clamping logic (min growing
segment checkpoint) stays in meta's proxy methods.

DC no longer holds a local collections map: collectionInfo is an
alias of metacache.CollectionInfo, and AddCollection /
GetCollection / GetCollections delegate to MetaStore. DC does
still hold a catalog reference, for the metadata it continues to
own (index meta, compaction tasks, binlog persistence).

### Metric Emission

Segment metrics (DataCoordNumSegments) are emitted by the store as
segments are installed. The store compares all five label
dimensions (state, level, sorted, storageVersion, format) between
old and new values and emits Dec/Inc pairs only when the labels
change.

The label set is unchanged by this PR: DC's previous
segMetricMutation already tracked all five (segmentMetricStateChange
is a five-level map, segmentMetricLabelValues returns five values).
What changes is where emission happens -- it moves into the single
store write path, so the explicit commit() call every mutation site
had to remember is gone.

## QueryCoord Integration

### CollectionTarget — ID Sets Only

CollectionTarget changes from storing full segment protos to
storing only segment ID sets with secondary indexes by channel
and partition. When QC needs segment details (for load tasks,
balance decisions, observer checks), it does a point lookup from
MetaView. All such lookups are O(1) under RLock.

A segment ID in the target but absent from MetaView means the
target is stale (segment was dropped/compacted). TargetObserver
rebuilds on its next cycle. Existing callers already handle nil.

### CollectionManager

QC's CollectionManager reads schema from MetaView instead of
calling DescribeCollection via broker, eliminating the third
copy of collection schema.

### GetRecoveryInfoV2

Still called, and still traversed. TargetManager.UpdateCollectionNextTarget
calls it, receives segmentInfos, and walks them to derive the
target's segment ID set and its channel/partition grouping -- plus
the DmChannel info (seek positions, unflushed/dropped segment IDs)
that has no MetaStore equivalent.

What this PR removes is not the call but the retention: CollectionTarget
no longer keeps the full segment protos it used to copy out of that
response. It keeps the ID set and the grouping, and resolves details
from MetaView on demand. That is where the duplication goes away.

## Wiring

MixCoord creates the catalog and an **empty** MetaStore before any
coordinator, and injects it. Loading is DataCoord's job, not
MixCoord's: MixCoord deliberately does not seed the store itself,
because DC.Init already walks RootCoord for exactly the same
collections into the same store, and DC's copy is fatal on error --
so a second walk in MixCoord would double RootCoord's boot load
while being unable to degrade.

Injection is by constructor option for DC (WithMetaStore,
WithCatalog) and by setter for QC (SetMetaView), which runs after
NewQueryCoord has already returned.

Init ordering (internal/coordinator/mix_coord.go):
1. Create catalog (from etcd KV) and NewMetaStore(catalog) — empty
2. Inject: DC via options, QC via SetMetaView
3. RC.Init + RC.Start — RootCoord is now available
4. **DC.Init** — newMeta -> MetaStore.LoadFromCatalog (concurrent
   per-collection segment loading + channel checkpoints), then
   meta.reloadCollectionsFromRootcoord for collections, then the
   DC-only sub-metas (index, compaction, stats, ...). This is where
   the store becomes populated.
5. DC.Start
6. QC.Init + QC.Start — the store is populated by now;
   TargetManager.Recover reads the saved target (segment ID sets)
   from etcd and resolves details from MetaView

A collection created *after* boot is not in the store until
something asks for it: DC backfills lazily on a miss through
ServerHandler.GetCollection -> loadCollectionFromRootCoord.

## Concurrency

Single sync.RWMutex in MetaStore. DC writes (flush, compaction,
state transitions) acquire Lock. QC reads acquire RLock. Multiple
readers proceed concurrently. Write frequency is low — segment
mutations are not on the hot read path.

Lock ordering: DC's segMu is always acquired before MetaStore's
mu. The store never calls back into DC, so deadlock is impossible.

## Persistence and Recovery

MetaStore holds the catalog it loads through, and is the single
owner of segment, channel checkpoint and collection metadata --
there is one copy of each, and DC reaches checkpoints through
MetaStore methods rather than through the catalog.

Loading is driven by DC, not by the store on its own: DC.Init calls
MetaStore.LoadFromCatalog for segments and checkpoints, and
meta.reloadCollectionsFromRootcoord for collections. QC does no
segment, checkpoint or collection reload of its own; it recovers
only its target ID sets from its own catalog and resolves the rest
through MetaView.

LoadFromCatalog resets the store before loading, so a retried
newMeta (it runs under retry.Do) cannot leave segments behind from
a failed attempt.

For sub-meta operations (indexes, compaction tasks, stats), DC uses
its own catalog reference directly.

## Design Decisions

1. **MetaStore is the backbone in production.** Distributed
   coordinator mode was removed in PR #41006, so MixCoord is the
   only production path and the store is always injected there.
   datacoord.Server.initMeta does keep `if s.catalog == nil` and
   `if s.metaStore == nil` fallbacks, for standalone DataCoord
   construction in tests; production never takes them.

2. **MetaStore owns the shared segment and collection storage.**
   It is the single owner of segment, channel checkpoint and
   collection metadata, and DC reaches checkpoints through it
   rather than through the catalog. DC still holds a catalog
   reference of its own -- MixCoord passes WithCatalog(catalog) --
   for the metadata it continues to own: index meta, compaction
   tasks, stats, and segment/binlog persistence.

3. **DataCoord loads the store; QueryCoord only reads it.**
   MixCoord injects an empty store and DC.Init populates it, before
   QC.Init runs. QC never writes and never loads.

4. **Constructor injection where the constructor allows it.** DC
   takes the store as an explicit constructor option. QC receives
   it through SetMetaView after construction, because
   NewQueryCoord's signature predates the store; the setter runs
   before QC.Init, so the dependency is still in place before
   first use.

5. **Single interface for segments and collections.** They are
   one data model. One interface, one injection point, one mutex.

6. **Quota computation stays in DC.** It needs DC-internal
   filters (isSegmentHealthy, GetIsImporting, DB-name
   resolution) that do not belong in the shared store.

7. **Proto immutability by convention.** Callers must not mutate
   returned protos. DC clones before modifying.

## Open Questions

1. **DmChannel info**: GetRecoveryInfoV2 also returns
   VchannelInfo. Small and per-channel. Keep as direct method
   call or add to the store?

2. **Dropped segment accumulation**: Dropped segments accumulate
   until GC. The shared store grows unbounded without eviction.
   A follow-up can add RemoveDropped(olderThan) triggered by
   DC's GC.

3. **Write-through persistence**: Currently PutSegment is
   in-memory only; DC separately calls catalog.AlterSegments
   for etcd persistence. A follow-up could make MetaStore
   persist transparently.
