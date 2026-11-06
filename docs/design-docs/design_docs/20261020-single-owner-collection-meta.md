# Single Owner for Collection Metadata

Track: Memory Deduplication
Status: Draft
Issue: #54489

Scope: DataCoord and QueryCoord stop keeping and fetching their own copy
of collection, partition and database metadata, and read RootCoord's
instead. RootCoord's stores are not moved into metacache (approach A
below); metacache still owns only segments and channel checkpoints.

## Motivation

After #53579, MixCoord holds collection metadata twice:

- RootCoord (RC) `MetaTable` (`internal/rootcoord/meta_table.go`) is
  authoritative. It holds `collID2Meta map[id]*model.Collection`,
  `dbName2Meta`, `partitionName2ID`, and the `names` / `aliases`
  indexes under one `ddLock sync.RWMutex`. It is mutated by DDL ack
  callbacks (which the broadcaster serializes by resource key), by the
  tombstone sweeper (`RemoveCollection`, `RemovePartition`, with no
  resource-key lock), by file-resource operations, and by `reload` at
  boot.
- `metacache.metaStore.collections map[id]*CollectionInfo`
  (`internal/metacache/store.go`) is a converted copy that DataCoord
  (DC) and QueryCoord (QC) read. DC fills it three ways:
  - At boot, `meta.reloadCollectionsFromRootcoord` walks ListDatabases,
    ShowCollections, DescribeCollectionInternal and
    ShowPartitionsInternal through the in-process broker.
  - On a miss, `ServerHandler.GetCollection` calls
    `loadCollectionFromRootCoord` (HasCollection, Describe, ShowPartitions).
  - On the AlterCollection and Truncate acks, RC calls
    `DC.BroadcastAlteredCollection`, which replaces the entry.
  - The only removal path is `FinishDropChannel` → `meta.DropCollection`,
    and `FinishDropChannel` has no production caller (its last caller,
    `ChannelManagerImpl.finishRemoveChannel`, was removed in #44532).

The copy is wrong in these cases:

1. **Stale partitions.** The CreatePartition and DropPartition acks do
   not push to DC, so `CollectionInfo.Partitions` stays wrong until the
   next alter or truncate.
2. **Lost update (traced by hand, not reproduced).** The lazy backfill
   reads RC without the collection's resource lock. If an alter ack
   lands between the read and `PutCollection`, the older snapshot
   overwrites the newer one, because `PutCollection` replaces the entry
   with no version check. DC and QC (`GetCollectionSchema`) then stay on
   the old schema until the next alter.
3. **Create is not pushed.** A new collection appears only after a miss.
   QC cannot trigger DC's backfill, so it falls back to
   `DescribeCollection` through the broker.
4. **Inconsistent partition visibility.** The boot and lazy paths use
   RC's internal describe (`allowUnavailable=true`, so Dropping
   partitions are included). The alter push uses
   `GetCollectionByID(..., false)`, which filters them out. The same
   collection's partition list depends on which path wrote it last.
5. **Drop visibility depends on restarts.** Because nothing removes the
   entry, a dropped collection stays in DC's copy, looking live, until
   the coordinator restarts. After a restart the boot walk uses
   ShowCollections (available only), so the same collection is gone.
   Separately, DC's `broker.HasCollection` (used by GC) goes through
   RC's public describe, which already reports a Dropping collection as
   not found. So DC's own copy and DC's GC disagree about the same
   collection.

On top of that, DC calls RC through the broker for collection, partition
and database metadata in about 33 places (plus one direct
`mixCoord.DescribeCollectionInternal` call in `services.go`
`getChannelsByCollectionID`) and QC in about 14, all in the same
process.

## Goals

- One owner and one consistent version of collection, partition and
  database metadata in MixCoord: RC's `MetaTable`.
- DC and QC see every DDL as soon as its RC ack callback has applied it,
  including create, partition changes and drops.
- Remove the metacache collection copy, the boot walk, the lazy
  backfill and the alter push.
- Keep DC hot-path collection reads cheap: no proto conversion and no
  `ddLock`, so no waiting behind etcd I/O done by any `ddLock` writer.
- Keep every existing `ddLock` invariant in RC unchanged.

## Non-Goals

- Moving RC's maps physically into metacache (approach A below).
- Changing the proxy meta cache. The proxy talks to MixCoord over gRPC
  even in standalone and shares nothing in-process.
- Time-travel reads (`ts` below max). DC and QC only read the latest
  state; RC's time-travel path is untouched.
- Reducing the memory of the schema representation itself (see
  "Memory").

## Alternatives

| | Approach | Why not chosen |
|---|---|---|
| A | Move RC's maps into metacache; `MetaTable` becomes a facade that writes the catalog | Rewrites most of `meta_table.go` (~3.5k lines) and its mocks. Either two locks with ordering rules, or one lock shared by segment hot paths and DDL. Gains little over B. |
| **B** | metacache collection reads delegate to RC's `MetaTable` | Chosen. |
| C | RC pushes every mutation into metacache's copy | Keeps a second owner and a second write path. B also keeps a derived representation (see "Memory"), but it is published by the same writer under the same `ddLock` section as the model, so it cannot diverge. In C the copy lives in metacache with its own lock and its own writers: a full-snapshot push is idempotent, but every mutation point (create, partitions, drop, rename) must push, and the pull paths must be removed or version-guarded; today's bugs are exactly missing pushes and an unguarded pull. |

## Design

### Ownership

RC's `MetaTable` is the only writer and owner of collection metadata.
metacache keeps segments and channel checkpoints and no longer stores
collections. Its collection reads go to a `CollectionSource` that
`MetaTable` implements:

```go
// in internal/metacache
type CollectionSource interface {
    GetCollection(collectionID int64) (*CollectionInfo, bool)
    GetCollections() []*CollectionInfo
    GetCollectionIDs() []int64
    // Added in M1.8: lookup by name or alias, and the database view.
    GetCollectionByName(dbName, name string) (*CollectionInfo, bool)
    GetDatabase(dbName string) (*DatabaseInfo, bool)

    // OnCollectionAdded / OnCollectionRemoved register fn, called after
    // a collection has entered (AddCollection) or left (RemoveCollection)
    // the view; OnCollectionDropping after DropCollection marks it
    // Dropping; OnCollectionAltered after AlterCollection replaces a
    // Created collection's view, with the views before and after.
    // Called outside ddLock, once per event, in event order.
    // fn must not block.
    OnCollectionAdded(fn func(collectionID int64))
    OnCollectionRemoved(fn func(collectionID int64))
    OnCollectionDropping(fn func(collectionID int64))
    OnCollectionAltered(fn func(old, updated *CollectionInfo))
}
```

`MetaView`'s collection methods keep their signatures and forward to
the source. `MetaStore` maps an empty database name to the default
database before forwarding, and relays the hooks of whichever source
is current, so a registration survives `SetCollectionSource`.
`MetaStore.PutCollection` and `RemoveCollection` stay for a store that
owns its collections (standalone DataCoord, tests) and panic once RC's
source is installed.

### The CollectionInfo view

`MetaTable` keeps a derived view,
`collViews *typeutil.ConcurrentMap[int64, *metacache.CollectionInfo]`,
next to `collID2Meta`. It is written only while the writer holds
`ddLock.Lock`, and read **without** `ddLock` (see "Locking"). It is
rebuilt for one collection whenever that collection's model changes. A
read is then one concurrent-map lookup, with no conversion, no
allocation and no `ddLock`.

To make this hard to get wrong, every mutation of `collID2Meta` goes
through one helper, called with `ddLock.Lock` held:

```go
func (mt *MetaTable) setCollectionLocked(coll *model.Collection)  // replaces model, publishes view
func (mt *MetaTable) deleteCollectionLocked(collectionID int64)   // removes both
```

`reload` publishes the view for every loaded collection before
`MetaTable` is returned, so the view is complete before DC.Init runs.

`AddPartition`, `DropPartition` and `RemovePartition` assign a new
`coll.Partitions` slice on the stored pointer. The slice is already
copy-on-write, so readers holding a shallow clone are unaffected, and
these mutators can keep doing that as long as they then call
`setCollectionLocked` with the updated model. PR M1.6 only routes every
mutation through the helpers; at that point the helper only replaces
the model. PR M1.7 adds the view and makes the helper publish it, so
from then on every model change publishes its view before the writer
releases `ddLock`.

Alternative considered: a lazy memo keyed by (collection, version),
filled on read. It needs a version counter bumped at every mutation
point (`AddPartition` does not touch `UpdateTimestamp` today), plus a
memo map with its own concurrency. That is the same number of mutation
points to touch as the eager view, with more moving parts, so the eager
view is chosen.

The view is immutable once published. A mutation builds a new
`CollectionInfo`; readers holding the old pointer keep a consistent
snapshot. This keeps #53579's "callers must not mutate returned
protos" convention. A `CollectionInfo` shares its slices, maps and
schema with RC's model, which RC replaces rather than modifies, so no
field is copied at publish time. A caller copies a field only before
handing it to code that may modify it: for example QC copies the
schema into the requests it sends to an in-process QueryNode, and
GenSnapshot copies what it writes into a snapshot.

### What the view contains

`CollectionInfo` keeps its current fields (ID, Schema, Partitions,
VChannelNames, Properties, DatabaseName, DatabaseID, StartPositions,
CreatedAt). It gains:

- `State` (Created or Dropping), so a reader can tell a dropping
  collection from a live one.
- `Partitions` lists only Created partitions, and a new
  `DroppingPartitions` lists the Dropping ones, until RemovePartition
  removes them. A DropPartition ack therefore moves the ID from one
  list to the other in the published view, so it is visible
  immediately (bug 1), and every entry has one partition semantics
  (bug 4). The only production reader of `Partitions` today,
  `task_refresh_external_collection.go`, expects exactly one live
  partition, which matches the Created-only list. A reader that needs
  every partition still being cleaned up reads both lists.

M1.8 adds the fields its callers read from describe and ShowPartitions:

- `PartitionIDsByName`, over Created and Dropping partitions as the
  internal ShowPartitions lists them. RC frees a name once its
  partition starts dropping, so a live partition may reuse it; the live
  one wins. Snapshot restore maps partitions by name through
  `LivePartitionID`, which ignores a Dropping partition: restoring into
  one would write data that is about to be removed, so restore fails
  instead.
- `UpdateTimestamp` (QC's schema barrier), `ShardsNum` (with
  describe's one-per-vchannel fallback), `ConsistencyLevel`, and
  `PropertyPairs`, the properties in RC's order, which snapshot restore
  preserves.

`DatabaseInfo` holds a database's ID, name and properties. RC publishes
it from `setDatabaseLocked` / `deleteDatabaseLocked`, which carry every
write to `dbName2Meta`, the way `setCollectionLocked` does for
collections.

`GetCollectionByName` reads RC's existing name and alias maps
(`nameDb`), aliases first as describe does, then the view by ID. Like
describe by name it finds only Created collections: a Dropping
collection keeps its name until `RemoveCollection`, but the name is
already free for a new one.
`nameDb` takes its own RWMutex so the lookup needs no ddLock; writers
already go through `nameDb`, so there is no second index to keep.

The schema is built the way RC's describe path (`convertModelToDesc`)
builds it: `model.Collection.ToCollectionSchemaPB`, then `Schema.DbName`
overwritten with the database name resolved from `DBID` (the model's
`DBName` may be out of sync, per the comment there). `DatabaseName`
comes from the same resolved name. M1.7 extracts this into one helper
used by both `convertModelToDesc` and the view builder, so the two
cannot drift, and DC and QC see what `DescribeCollectionInternal`
returns today. The DDL table test also compares the view's schema with
`DescribeCollectionInternal` output.

Publication never fails after the catalog write. When the database
cannot be resolved from `DBID` (a Dropping collection outliving
DropDatabase), the view falls back to the model's `DBName`, then to
`default`; legacy collections without a `DBID` are normalized to the
default database first. A collection or partition in the deprecated
`Creating` state, which RC's restore treats as a tombstone pending
removal, is published as Dropping.

### Lifetime and drop semantics

A collection is in the view from the ack that adds it to `MetaTable`
until `RemoveCollection`, which the tombstone sweeper runs after DC's
`GcConfirm`. Between DropCollection's ack and `RemoveCollection` it is
in the view with `State = Dropping`. This does not depend on restarts:
`reload` publishes Dropping collections too, since `MetaTable` keeps
them.

One exception, inherited from `MetaTable`: `reload` lists collections
per database still in `dbName2Meta`, and DropDatabase is allowed once
only non-available collections remain. A Dropping collection whose
database was dropped before its `RemoveCollection` is therefore not
reloaded after a restart. It is absent from `MetaTable` itself, so it
is also absent from the view, and `OnCollectionRemoved` does not fire
for it. The view follows `MetaTable` here; changing what `reload`
loads is out of scope.

Rules:

- `MetaView.GetCollection` and DC's `meta.GetCollection` return Dropping
  collections, with `State` set. This matches what DC's copy does
  today between restarts (bug 5: nothing removes the entry), so most
  of DC's ~54 read sites see no change in the common case. They do see
  a change across a restart, where today the collection would vanish.
- A read site that must skip dropped collections checks
  `State == Created` explicitly. A nil result means only "RC has no
  such collection".
- GC keeps today's semantics. The `broker.HasCollection` checks in
  `garbage_collector.go` treat a Dropping collection as not found
  today, and GC runs `GcConfirm`, channel-checkpoint cleanup and
  orphan-snapshot cleanup on that. With B they become
  "present in the view and `State == Created`". A presence-only check
  would postpone those steps until after `RemoveCollection` instead of
  running them during the Dropping window. It would not block
  `RemoveCollection`: the sweeper's `GcConfirm` checks only that the
  segment keys are gone.
- Since #54900, GC also drops a recycled collection's segment change
  groups, and calls `meta.DropCollection`, before its checkpoints.
  With B, GC keeps the change groups and checkpoints and drops the
  `meta.DropCollection` call: the entry is RC's, and RC removes it.
- QC's `GetCollectionSchema` returns the schema for Dropping
  collections, as RC's internal describe does today.

There is no DC-side "finished drop" set: the collection state in the
view is enough, and nothing in DC writes such a set (see bug 5).

M1.7 includes an audit table of every DC and QC collection read site,
recording whether it needs `State == Created`, what it gets today in
both the no-restart and the after-restart case, and what it gets with
B.

### Locking

`ddLock` locking in `MetaTable` is unchanged. The view is what keeps
DC and QC reads off it.

Why not make view readers take `ddLock.RLock`: many writers hold
`ddLock.Lock` across catalog (etcd) I/O. That includes AlterCollection,
Truncate, AddPartition, DropPartition, RemovePartition,
RemoveCollection, DropAlias, AlterAlias, the database mutators and
AddFileResource/RemoveFileResource. `persistCollectionDropping` also
holds `RLock` across a catalog write. Go's `RWMutex` blocks new readers
once a writer is waiting, so DC's hot-path reads would wait for etcd
round trips.

Why not move the catalog I/O out of `ddLock`: not every writer is
serialized by the broadcaster resource-key lock. The tombstone sweeper
(`tombstone/scheduler.go`) calls `RemoveCollection` and
`RemovePartition` from a background goroutine after `GcConfirm`,
without any resource-key lock. Today only `ddLock` keeps it apart from
a concurrent AlterCollection, Truncate or AddPartition on the same
collection. The legacy partition layout makes this matter in etcd as
well as in memory: `kc.DropPartition` read-modify-writes the whole
collection record, which is why `persistCollectionDropping` holds
`RLock` (see its comment). Moving I/O out of the lock would let a
sweeper removal and a DDL on the same collection each publish a model
built from the same snapshot, losing one of them.

So the design keeps every existing `ddLock` invariant and changes only
the read side for DC and QC:

- Writers publish the new view inside their existing `Lock` section,
  after the catalog write and the model update succeed. Every model
  change publishes its view before `ddLock` is released, and
  publications for one collection happen in `ddLock` order, so the
  latest published view of a collection is the latest model.
- `CollectionSource` reads (`GetCollection`, `GetCollections`,
  `GetCollectionIDs`) read `collViews` only and never take `ddLock`.
  Etcd stalls under `ddLock`, including from database, file-resource
  and sweeper writers, never reach these reads. DC and QC reads that
  still go through the in-process broker take `ddLock` until M1.8
  turns them into `CollectionSource` reads.
- Each view entry is an immutable snapshot of one collection.
  `GetCollections` ranges the concurrent map, so it is not a
  point-in-time snapshot across collections, and a reader does not get
  one snapshot across `collViews`, `names` and `aliases`. DC and QC key
  everything by collection ID and do not rely on either today: their
  current copy is filled one collection at a time.
- RC's own readers (proxy describe, quota, `CheckIf*` validators)
  keep using `ddLock` as today.

Lock ordering: `ddLock` and metacache's `mu` are never held together.
metacache forwards collection reads to the source without holding
`mu`, and the source takes no lock. The only calls from RC toward DC
are the collection hooks, which RC invokes after releasing `ddLock`.
The DC handlers touch metrics, and the altered handler enqueues the
builds an alter can make due: the collection's stats builds, since an
added field may need text or JSON key stats on existing segments, and,
when the alter publishes a new external source/spec, its index builds.
A refresh applies its segments before it publishes the source/spec they
were read from, and a build reads the source/spec when it is
dispatched. Before, both ran in `BroadcastAlteredCollection`.

### Wiring

`mix_coord.go` already creates RC before DC and QC and runs RC
Init/Start first. With B:

1. Create catalog and `NewMetaStore(catalog)` as today.
2. After `rootcoordServer.Init()`, call
   `metaStore.SetCollectionSource(rootcoordServer.MetaTable())`. RC's
   `MetaTable` is loaded in RC.Init, before DC.Init runs.
3. DC.Init no longer calls `reloadCollectionsFromRootcoord`.
4. `ServerHandler.GetCollection` no longer backfills. It reads the
   view, and a miss means RC does not have the collection.
   `loadCollectionFromRootCoord` is deleted.
5. RC's AlterCollection and Truncate ack callbacks stop calling
   `BroadcastAlteredCollection`. RC is its only caller and all
   coordinators share one process, so after M1.7 the DC method has no
   caller. Until the proto RPC is removed in a later cleanup, the DC
   method returns success without doing anything, and the existing
   forwarding in `mixCoordImpl` and `distributed/mixcoord/service.go`
   stays as is.

For standalone DataCoord construction in tests (the `initMeta` nil
fallback), metacache gets an in-memory `CollectionSource` fake that
tests fill directly. This replaces today's `meta.AddCollection` calls
in tests.

### Metrics

`DataCoordNumCollections` is set by `PutCollection` and
`RemoveCollection` today. With B those are gone. RC already emits
`RootCoordNumOfCollections` per database, and the in-repo Grafana
dashboard (`deployments/monitor/grafana/milvus-dashboard.json`) uses
only the RC metric; the DC metric is listed in the metrics README. M1.7
keeps the DC metric so external dashboards do not break; DC sets it to
the number of Created collections: it seeds a set of Created IDs at
boot and updates it in its `OnCollectionAdded`, `OnCollectionDropping`
and `OnCollectionRemoved` handlers under one mutex. Each event is
idempotent for its collection, so concurrent events cannot leave a
stale count, and no event rescans every collection. RC's `setCollectionLocked` does not touch
DC metrics. The gauge then follows creates and removals as
they happen, which is fresher than today, where a new collection is
counted only at its first DC lookup.

`MetaStore.RemoveCollection` also calls
`metrics.CleanupDataCoordWithCollectionID`, which deletes the
per-collection series of several DataCoord metrics. At the baseline
that never runs in production (bug 5); #54900 has GC run it through
`meta.DropCollection`. With B, DC's `OnCollectionRemoved` handler
calls it when RC removes the collection.

### Direct reads (M1.8)

Once the view is the source of truth, the in-process broker calls in
DC and QC for collection, partition and database metadata become direct
reads, in small PRs grouped by caller:

- QC: `DescribeCollection` in `target_observer.go`, `task/executor.go`,
  `job_load.go`, `index_checker.go` (the miss fallback goes away),
  `collection_manager.go`, and the load-config callbacks; `GetPartitions`
  in `alter_load_info_load_collection.go`; `DescribeDatabase` in
  `executor.go`.
- DC: `DescribeCollectionInternal`, `ShowPartitions` and `HasCollection`
  in `ddl_callbacks*.go`, `index_service.go`, `services*.go`,
  `handler.go`, `snapshot_manager.go`, `import_snapshot.go`, and GC,
  plus the direct `mixCoord.DescribeCollectionInternal` call in
  `services.go` `getChannelsByCollectionID`.

These lists are by file. M1.8 starts with a grep-based inventory of
every call site (about 33 in DC, 14 in QC at the baseline) and checks
each one off.

GC's `HasCollection` checks need care, because GC reclaims data on a
negative answer. Today's `HasCollection` is negative for a Dropping
collection (RC's public describe, `allowUnavailable=false`). The
replacement is "in the view and `State == Created`", not presence (see
"Lifetime and drop semantics"). M1.8 adds a test per GC call site
that a Dropping collection gets the same answer as today.

Database lookups (`DescribeDatabase`) read the database view
(`GetDatabase`): QC's executor and load-config defaults, and DC's
snapshot restore (CMEK check and target validation).

DC and QC keep their describe semantics: DC's accessors return a
Dropping collection with its State, as the internal describe did,
while QC's report it as not found, as the public describe did. DC's
`GetCollections` returns Created collections only, for the compaction
and stats loops; GC and snapshot listing use `GetAllCollections`.
`ServerHandler.GetCollection` retries a miss for up to 10s, since a
collection whose CreateCollection is in the WAL is visible only after
RC's ack callback applies it.

After M1.8 neither coordinator reads collection, partition or database
metadata through the broker, and those methods are removed from both
brokers.

## Memory

B removes the duplication of ownership and the fill paths. It does not
by itself shrink memory: RC keeps the schema as `model.Field` and
friends, and the view keeps it as `schemapb.CollectionSchema`, which is
the same total as RC model plus metacache copy today. For dropped
collections B is equal or better: today DC's copy is never pruned
without a restart (bug 5), so it keeps dropped collections
indefinitely, while the view drops them at `RemoveCollection`. Only
compared with a freshly restarted DC, which skips Dropping
collections, does the view hold more during a drop window. Sharing one
representation (for example making `model.Collection` carry the
`schemapb` it would build) is a possible follow-up, and is easier once
the view is the only consumer-facing form.

## Rollout and Compatibility

- No persisted format changes. Catalog keys and values are unchanged.
- No wire changes in M1.7. `BroadcastAlteredCollection` loses its only
  caller; the proto RPC is removed in a later cleanup.
- All coordinators always share one process (#41006), so there is no
  distributed mode to keep working.

## Verification

- Table-driven test over every collection DDL (create, drop, alter
  schema, alter properties, rename within and across databases, create
  partition, drop partition, remove partition, truncate, add/drop alias):
  after the ack callback returns, `MetaView.GetCollection` equals the
  `CollectionInfo` built from RC's model, and the test also asserts
  the expected `State`, `Partitions` and `DroppingPartitions` for each
  step (so DropPartition must move the ID). This covers bugs 1, 3 and 4.
- A concurrency test that runs readers against `MetaView` while DDL acks
  apply, with `-race`, and checks readers only ever see a published
  view.
- Bug 2 cannot recur because the backfill path is deleted. The test
  above, run concurrently with collection reads, guards against a new
  writer path being added.
- Drop (bug 5): from the DropCollection ack until `RemoveCollection`,
  DC and QC see the collection with `State = Dropping`, both before
  and after a simulated restart (`reload`); after `RemoveCollection`
  they see nil, and `OnCollectionRemoved` fires exactly once and
  cleans the per-collection DC metric series. `OnCollectionAdded`
  fires exactly once per create, and the gauge matches the view size
  after each.
- GC: for a Dropping collection, each GC call site that used
  `HasCollection` takes the same branch as today.
- Locking (M1.7): a test that holds the catalog write of an
  AddPartition open (fake catalog that blocks, so `ddLock.Lock` stays
  held) and checks a concurrent `MetaView.GetCollection` returns
  without waiting and still sees the pre-DDL view; after the write
  completes it sees the new one. The same test with a blocked
  `RemovePartition` from the sweeper and with a blocked database DDL.
- M1.6 is a refactor with no behavior change: existing `MetaTable`
  tests, plus a check that no code outside the two helpers writes
  `collID2Meta`.
- Boot: DC.Init no longer issues ListDatabases, ShowCollections or
  per-collection describe calls. Check with a counting broker fake.

## PR Breakdown

| PR | Content |
|----|---------|
| M1.6 | `MetaTable`: route every `collID2Meta` mutation through `setCollectionLocked` / `deleteCollectionLocked` (model only). No locking change |
| M1.7 | `collViews` published by the helpers and by `reload`; lock-free `CollectionSource` in metacache; wiring; delete the metacache collection copy, boot walk, lazy backfill and alter push; drop semantics; metrics; the audit table |
| M1.8 | Direct reads in place of in-process broker calls, split by caller group |

## Read-Site Audit (M1.7)

Every production DC and QC read of collection metadata through the view,
and what it sees for a Dropping collection. "As before" means the same
as today without a restart, where DC's copy kept the entry looking live.
Before M1.7, after a restart, a Dropping collection was absent (nil, or
ErrCollectionNotFound from the backfill).

The drop order makes most sites harmless: the DropCollection ack releases
the collection in QC, drops its indexes and snapshots, marks it Dropping,
and then DropVirtualChannel marks its segments Dropped and its checkpoints
MaxUint64.

| Site | Use | Verdict |
|---|---|---|
| `handler.go` `getCollectionStartPos` | StartPositions fallback | OK (as before) |
| `meta.go` `GetQuotaInfo` | db label for per-collection metrics | OK: only healthy segments count |
| `meta.go` `collectionTextFieldIDs` | TEXT field lookup | OK: schema lookup |
| `server.go` `initMeta` | DataView recovery set | Filtered to `IsCreated` (same set as the ShowCollections walk) |
| `server.go` `watchCollectionMetrics` | `DataCoordNumCollections` | Counts Created collections only (see Metrics) |
| `services.go` `flushCollection`, `AssignSegmentID`, `GetCollectionStatistics` | vchannels, cache warm-up, schema version | OK (as before) |
| `services.go` `createImportJobFromAck` | create an import job | **Guarded with `IsCreated`**: a replayed ack after restart would otherwise start a job |
| `services.go` `CreateSnapshot`, `DropSnapshot`, `ExportSnapshot`, `PinSnapshotData` | db and name for locks | OK: the drop ack removes the collection's snapshots before it goes Dropping, so Pin, Export and Drop find no snapshot; CreateSnapshot rechecks `IsCreated` under the collection lock |
| compaction trigger and policies (`compaction_trigger*.go`, `compaction_policy_*.go`) | config, TTL, schema, external flag | OK (as before): segments are Dropped |
| `compaction_task_clustering.go` `retryableProcess` | in-flight task existence | OK |
| `compaction_inspector.go` `cleanPartitionStats` | nil means delete stats | OK: stats kept until RootCoord removes the collection |
| `segment_manager.go` `estimateMaxNumOfRows` | schema for allocation | OK: a replay after restart no longer fails. The live caller, the streaming node's `AllocSegment`, is WAL-ordered: a segment is created before the vchannel's DropCollection. The legacy `AssignSegmentID` has no production caller |
| `index_inspector.go`, `task_index.go`, `index_service.go` `AlterIndex` | schema, external flag | OK: index meta is dropped first; dropped targets abort |
| `task_stats.go`, `stats_inspector.go`, `task_analyze.go` | schema, collection loop | OK (as before): only healthy flushed segments are picked |
| `garbage_collector.go` `recycleDataViews` | DataView GC loop | OK: desired |
| `manifest_index.go`, `import_task_import*.go`, `import_util.go`, `snapshot_manager.go` | field name, labels, TTL, filters | OK |
| `util.go` `CheckCheckPointsHealth` | skip lag check for a missing collection | **Guarded with `IsCreated`**: a dropped collection's checkpoints stop advancing but stay until GC |
| `import_v3_schema.go` `validateImportV3Schema` | frozen-schema check | OK (as before): the import checker fails the job via the broker |
| `server_external_schema_updater.go` `updateExternalSchemaViaWAL` | AlterCollection by name | **Guarded with `IsCreated`**: the name may belong to a new collection |
| `external_collection_refresh_manager.go` `SubmitRefreshJobWithID`, `exploreExternalFiles` | create refresh job and tasks | **Guarded with `IsCreated`** |
| `task_refresh_external_collection.go` `applyExternalCollectionSegmentUpdateForBaseline`, `CreateTaskOnWorker` | add segments, dispatch task | **Guarded with `IsCreated`**: new segments would never be dropped and would stall GcConfirm |
| QC `collection_manager.go` `GetCollectionSchema` (used by `index_checker.go`) | schema for stats/index checks | OK: QC releases the collection before it goes Dropping |

`Partitions` readers: only `task_refresh_external_collection.go`, which
expects exactly one partition on an external collection; Created-only is
what it needs. QC's own `meta.GetCollection` sites read QC's loaded
collections, not the view, and are unaffected.
