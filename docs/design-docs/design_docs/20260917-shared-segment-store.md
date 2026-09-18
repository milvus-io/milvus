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
    │  │ segments  map[id]*datapb.SegmentInfo │   │
    │  │ segCollIdx   map[coll]Set[id]        │   │
    │  │ segChanIdx   map[ch]Set[id]          │   │
    │  │ segStateIdx  map[state]Set[id]       │   │
    │  │                                      │   │
    │  │ collections  map[id]*CollectionInfo  │   │
    │  │ collDBIdx    map[dbID]Set[collID]    │   │
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

**MetaView** (read-only, used by both DC and QC):
- Segment lookups by ID, collection, channel, or state
- Channel checkpoint reads
- Collection metadata reads (schema, partitions, vchannels)

**MetaStore** (read-write, embeds MetaView, used only by DC):
- Segment CRUD with automatic index maintenance
- Channel checkpoint updates with timestamp-based deduplication
- Batch operations for compaction and flush workflows
- Collection metadata writes
- Catalog ownership for non-segment persistence

MetaStore is the backbone — there is no standalone mode, no
nil-check branches, no no-arg constructors.

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

DC's meta struct also drops its local collections map and
catalog reference. All collection operations and catalog access
go through MetaStore.

### Metric Emission

Segment metrics (DataCoordNumSegments) are emitted by the store
on every state transition. The store compares all three label
dimensions (state, level, sorted) between old and new values and
emits Dec/Inc pairs only when labels change. This replaces the
previous segMetricMutation batching mechanism in DC, which only
tracked state transitions and required explicit commit calls at
every mutation site.

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

No longer needed for segment protos in MixCoord mode. Still
needed for DmChannel info (seek positions, unflushed/dropped
segment IDs). DmChannel info is small and per-channel.

## Wiring — Constructor Injection

MixCoord creates the catalog and MetaStore before any coordinator,
loads data after RootCoord is up, then passes the populated
MetaStore to DC and QC via constructor option functions
(WithMetaStore, WithCatalog).

Init ordering:
1. Create catalog (from etcd KV) and NewMetaStore(catalog)
2. RC.Init + RC.Start — RootCoord is now available
3. Load collections from RC broker into MetaStore
4. MetaStore.LoadFromCatalog — reads segments, builds indexes
5. DC.Init — MetaStore arrives populated; DC only loads channel
   checkpoints and index meta via MetaStore.Catalog()
6. QC.Init — MetaView is populated; TargetManager.Recover reads
   saved target (segment ID sets) from etcd, resolves details
   from MetaView

## Concurrency

Single sync.RWMutex in MetaStore. DC writes (flush, compaction,
state transitions) acquire Lock. QC reads acquire RLock. Multiple
readers proceed concurrently. Write frequency is low — segment
mutations are not on the hot read path.

Lock ordering: DC's segMu is always acquired before MetaStore's
mu. The store never calls back into DC, so deadlock is impossible.

## Persistence and Recovery

MetaStore owns the catalog and loads independently. Neither DC
nor QC does its own segment or collection reload. MetaStore is
the single owner of segment and collection metadata. For
non-segment catalog operations (channel checkpoints, indexes),
DC and QC use MetaStore.Catalog().

## Design Decisions

1. **MetaStore is the backbone, not optional.** No standalone
   mode, no nil branches. Distributed coordinator mode was
   removed in PR #41006.

2. **MetaStore owns the catalog.** Neither DC nor QC creates or
   holds a catalog reference directly.

3. **MetaStore loads independently.** MixCoord populates it
   before DC/QC start. They receive a ready store.

4. **Constructor injection over setter injection.** Dependencies
   are explicit constructor parameters.

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
