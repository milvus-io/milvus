# Balancer Cache

The resident `cache.Cache` replaces the runtime `BalancerSnapshot` and
`SnapshotBuilder` path. Tests publish inputs directly into the cache and assert
planning behavior; no snapshot-era implementation is retained. Batch ordering and logical row accounting remain unchanged.
[Replica placement](replica_placement.md) adds balanced target node sets,
suspension and compatible cross-replica resource reuse. Production runtime
wiring and RPC changes are outside this work.

See [Balancer design](balancer_design.md) for the allocation algorithm,
[DataView](data_view.md) for publication and reference ownership, and
[Shard view management](shard_view_management.md) for QueryView lifecycle.

## 1. Contract and ownership

`cache.Cache` holds all facts required for reconciliation. Upstream managers
remain the authoritative owners and synchronously publish their committed
in-memory state through registered hooks. The cache owns the derived indexes
and aggregates. Balancer reads the cache through `Get` and key iteration;
reconciliation does not pull manager snapshots or rebuild the row-count ledger.

The consistency contract is object-local:

- A published object and everything reachable through its read API are
  immutable. Readers may retain it without holding a lock and must not mutate
  it. Writers replace objects using copy-on-write and structural sharing.
- One `Get` returns a complete object version. Separate reads may observe
  different publication times, including different fields' source versions in
  a Collection entry. There is no global version barrier or global transaction.
- Before an upstream mutation returns, its cache publication and affected-key
  notification have completed. Previously acquired references remain valid
  representations of older state.
- Target node layouts remain stable under ordinary view progress and DataView
  changes. Placement now obeys replica isolation and may differ from the old
  RG-wide policy. Lifecycle safety and eventual convergence remain required.

This is an in-process publication mechanism, not a distributed informer
protocol. It does not require a replicated event log, network watch, or relist
worker. It also does not require lock-free reads.

## 2. Objects and copy boundaries

| Object | Contents | Publication boundary |
|---|---|---|
| `CollectionEntry` | LoadConfig, DataView, resident shard index | Small header plus changed child/index paths |
| `ShardEntry` | View lifecycle summary, placement, row contributions | One shard or its changed immutable blocks |
| `NodeEntry` | Identity, liveness, Stopping, RG, row totals, contribution index | One node header plus changed contribution paths |
| `ResourceGroupEntry` | Candidate node IDs and desired collection index | One RG's changed index paths |
| `BalanceConfig` | Policy parameters | One small object |

### 2.1 Collection and shard entries

`CollectionEntry`, keyed by CollectionID, has independent immutable children:

```text
CollectionEntry
  ├── LoadConfig + source revision
  ├── PublishedDataView
  │     ├── DataVersion
  │     └── shard → partition → segment membership and RowNum
  │           └── per-shard TotalRows and SegmentCount
  └── resident ShardID → ShardEntry
```

Each source replaces only its own field. A collection-scoped cache write lock
merges concurrent source updates, so publishing DataView cannot overwrite a
concurrent LoadConfig update. Missing desired configuration does not remove
resident shards: they must remain discoverable for release.

Collection is an access/ownership unit, not a deep-copy unit. Updating a load
configuration shares the previous DataView and shard children. Updating one
shard shares the other shards. Large child indexes require structural sharing;
copying an ordinary Go map still copies all of its entries and is not a bounded
small update.

`ShardEntry` retains the `(ReplicaID, VChannel)` identity. Its view summary,
placement, known/unknown row statistics, and per-node contributions describe
one publication. The existing merge order remains
`Up > Ready > Preparing > Unrecoverable` for each `(segment, node)` within the
shard. Ready includes reusable Down-view placements; only the existing
Up/Pending accounting states contribute rows. Replica placements remain
separate contributions.

### 2.2 Node entries

Each immutable `NodeEntry` contains both its row totals and an index of the
shard contributions included in those totals:

```text
NodeEntry
  ├── topology: NodeID, Alive, Stopping, ResourceGroup
  ├── UpRowCount, PendingRowCount
  └── ShardID → RowContribution
```

For every published NodeEntry, each total equals the sum of the corresponding
field in its contribution index. The index stores row summaries, not segment
lists. It can nevertheless be large, so writes share unchanged index paths
rather than cloning the complete node's contribution map. The exact persistent
tree/block representation is an implementation choice to validate with focused
benchmarks; only object lookup, not every nested lookup, is assumed O(1).

Topology and row contributions have different sources. Their updates merge
under the node's cache write lock. Removing node membership marks the node
ineligible but retains contributions until the corresponding views disappear.
An entry may be reclaimed when both its identity and contributions are absent.

### 2.3 Secondary indexes

The cache maintains replica-to-collection lookup, collection discovery,
resident shard discovery, node-to-placed-shard lookup, RG candidate nodes, and
RG-to-desired-collection lookup. The last index is required for scale-out:
a new node has no existing placements but must trigger its RG's collections.

Zero-row placements still belong in reverse indexes. Removing LoadConfig
updates desired indexes without hiding residual QueryViews. These are derived
indexes, not independent authorities. Hook publication updates the affected
indexes before enqueuing work.

## 3. Read interface and concurrency

The implemented read interface is:

```go
type Reader interface {
    GetCollection(collectionID int64) *CollectionEntry
    GetNode(nodeID int64) *NodeEntry
    GetResourceGroup(name string) *ResourceGroupEntry
    GetBalanceConfig() *BalanceConfig

    CollectionForReplica(replicaID int64) (int64, bool)
    RangeCollectionIDs(func(int64) bool)
    RangeNodeIDs(func(int64) bool)
}
```

Accessors such as `collection.DataView()`, `collection.GetShard(id)`,
`node.Info()`, and `node.Contribution(id)` read the receiver's version;
they do not perform another live-cache lookup. Iteration copies at most keys
or references, never segment payloads, and does not invoke callers under cache
locks. Cross-object iteration is not a globally consistent list operation.

An RWMutex protects directory keys and slots; each Collection/Node slot has
a writer mutex and an atomic immutable pointer. `Get` holds the directory lock
only to find the slot. RG and contribution indexes use a persistent radix tree
(`go-immutable-radix`) so writes copy only changed paths. Updating an entry does not
clone the global directory. Published maps, slices, protobufs, and backing
arrays cannot subsequently be mutated or reused. An input without an immutable
ownership contract must be copied at the write boundary. Old in-memory objects
are reclaimed by Go GC after readers release their references.

## 4. Aggregation versus planning

### 4.1 Aggregates maintained before reconciliation

| Value | Update owner and event |
|---|---|
| Desired shard `TotalRows` and `SegmentCount` | DataView publication, from that version's membership and RowNum |
| Actual shard contribution to each node | Placement/statistics publisher, on relevant view changes |
| Node Up/Pending totals | Cache applies the difference between old and new shard contributions |
| RG candidate set and ordering | Node/RG publication |

`TotalRows`, `SegmentCount`, membership, and per-segment RowNum are published
together. Balancer uses the summaries for shard ordering and fanout calculation
without traversing segments to rediscover these facts. Source-provided summaries
may be shared; otherwise the publication adapter computes them once per new
version, not once per reconcile.

Cache updates have replacement semantics. They derive differences from the
previously accepted contribution, rather than accepting an unversioned
`AddRows(delta)` that would double-count duplicate publication. A change to one
shard updates only affected node contributions. A periodic full reconcile does
not clear or rebuild this accounting.

### 4.2 PlanningContext and predicted loads

The policy still owns temporary calculations describing the effect of the
proposed batch. It pins each CollectionEntry, RG entry, and BalanceConfig on
first read for the lifetime of the batch. Classification, ordering, and
allocation reuse those references without copying their segment data.

The initial implementation enumerates node keys and calls `GetNode` once per
node before planning candidates, retaining these immutable references and
their scalar loads. This O(M) operation does not require a common publication
time. An RG index is a candidate discovery aid; eligibility is checked against
the pinned node facts. Changes during discovery are covered by subsequent work.

The total and the contribution subtracted from it must come from the same
pinned NodeEntry, not a separately read ShardEntry:

```text
base[n]       = pinnedNode[n].UpRowCount + pinnedNode[n].PendingRowCount
old[n, s]     = pinnedNode[n].Contribution(s).TotalRows
projected[n]  = base[n] + acceptedDelta[n]

candidateBase[n] = projected[n] - old[n, s]

on accepting candidate s:
    acceptedDelta[n] += candidateRows[n, s] - old[n, s]
on releasing s:
    acceptedDelta[n] -= old[n, s]
```

Each shard is processed at most once per batch. Rejected optional candidates
leave the delta unchanged. Replacement covers old and new nodes, including
historical nodes outside the desired RG. Independently reading a newer shard
contribution and subtracting it from an older node total is not valid;
clamping a negative result would not repair that inconsistency.

Candidate segment placement, partial assigned rows, opened-node tracking, and
the following score reference remain policy calculations:

```text
ReferenceRows =
    (sum(candidateBase over eligible nodes) + desiredShard.TotalRows) / N
```

Earlier accepted candidates change this value for later shards. A cached
static RG total cannot replace it. It is computed once per candidate and held
fixed while placing that candidate's segments. No predicted value is written
back into the cache. Apply begins after the whole batch has been planned, so
Preparing publications cannot be counted again as this batch's predictions.

The first version does not lazily introduce new node baselines after accepting
candidates; doing so would require rebasing all earlier accepted replacements.
The cache refactor eliminates repeated actual-state aggregation and deep
copies, not segment sorting, candidate scoring, or all shard-by-node traversal.

## 5. Source hooks and publication

### 5.1 Ordering

For a normal source mutation that persists before becoming visible:

```text
upstream object lock
  → persist successfully, when required by that source
  → commit upstream in-memory state
  → synchronously publish cache objects and derived indexes
  → mark affected reconcile keys dirty
  → unlock and return
```

QueryView hooks follow in-memory state-machine transitions and do not wait for
asynchronous persistence; existing persist-before-sync rules remain intact.
DataView drop publishes absence at logical removal/tombstoning, not after
physical prefix cleanup. Cleanup failures must not leave the cache advertising
a DataView that the manager already made inaccessible.

Hooks perform bounded in-memory work and may wait for short locks; they are
not arbitrary asynchronous callbacks and are not promised to be lock-free.
They do not perform I/O, execute Policy, or re-enter a source manager. Lock
ordering is upstream → cache, with a fixed order for internal cache locks.
Readers never hold cache locks while applying a plan or acquiring a DataViewRef.
Affected nodes may be published individually; no cross-node atomic commit is
required.

### 5.2 Sources

| Source | Publication events | Cache fields |
|---|---|---|
| DataViewManager | Create/bootstrap, recompute publication, flush commit, logical drop, recovery footprint initialization | Collection DataView and desired shard summaries |
| LoadConfigStore | Put/Remove in-memory commit | Desired config, replica/RG indexes |
| ShardViewManager/Registry | Recovery, view/placement/progress changes, final removal | Shard state, actual contributions, node and shard indexes |
| Node/RG owners | Add/remove, Stopping, labels, RG reassignment | Node topology and RG indexes |
| Config owner | Policy parameter changes | BalanceConfig |

Use source-local publication revisions and object-instance identities for
initial replay deduplication and stale-instance callbacks. A DataVersion alone
is insufficient: recovery can initialize previously unknown RowNum without
advancing that version. Same-source synchronous ordering is the primary
mechanism; no distributed sequencing protocol is introduced.

Membership notifications followed by full node/RG reads cannot satisfy this
contract. Source owners must implement `QueryNodeStatePublisher` and `ResourceGroupStatePublisher`
(or an equivalent synchronous `NodePublisher`) at their commit points;
`nodeview.QueryNodePublisher` merges these keyed facts and replays them to cache
subscribers.
An asynchronous refresh after an unkeyed notification would retain a stale
cache window. Production assembly remains outside this PR.

### 5.3 DataView ownership and row statistics

PublishedDataView wraps immutable membership and the matching RowNum footprint.
It must not expose the mutable `versionEntry` reference counter/tombstone, nor
call the cloning `DataViewRef.DataView()` on cache reads. Read indexes and
summaries are built once at publication. Upstream's existing full DataView
recompute/clone cost is not automatically removed by this adapter.

For resident placement accounting, known exact-version DataViewRef statistics
win, including a known zero. Unknown statistics may fall back to retained
known per-collection segment rows; otherwise they contribute zero. Retain
fallback data while latest/resident views need it, then reclaim it. Prefer
completing recovery statistics before initial cache publication; a later fill
must replace published objects and update affected contributions.

An immutable cache pointer protects in-memory readability, not the physical
DataVersion against GC. `AddPreparing` still acquires an exact DataViewRef;
a collected version requires replanning. Existing QueryViews keep their refs
until durable final removal.

## 6. Initialization, notification, and lifecycle

Register hooks before source recovery/publication where possible. If runtime
registration is necessary, the source must provide registration plus initial
replay with no missing-update window. A separate List followed by registration
is insufficient. Reconcile starts only after every required source has seeded
its entries and declared readiness. Unknown/uninitialized is not absence.

`Cache.WaitForReady(ctx)` waits on a one-way channel closed by `MarkReady`.
The controller may start during recovery, but Reconcile waits before taking
pending work, reading planning inputs, or applying a plan. Cancellation leaves
pending work untouched and lets Stop terminate the loop without waiting for
recovery. Waiting for initialization is not an allocation failure and does not
enter retry backoff.

Queue signals are coalesced wakeups; the full flag and dirty-key sets retain the
work. Consuming a wakeup before waiting does not consume those sets. Closing the
ready channel resumes the waiting reconcile without requiring another balance
signal. `MarkReady` also requests a full pass; if it ran before the controller
subscribed, Start's initial full request covers that ordering. The barrier is
idempotent and wakes all current and future waiters. Callers must only mark it
ready after all source recovery/replay has completed.

RemoveLoadConfig clears desired state but preserves actual shards for cleanup.
A desired collection with an unavailable DataView waits/retries; it is not
treated as released. Removal or update callbacks from a retired manager must
not erase a new manager with the same key. Reclamation checks instance identity.

After cache publication, notifications coalesce affected keys. An update that
arrives while a key is being processed must schedule a successor pass. Sources
need not trigger expensive optimization for every progress report:

- Desired config or DataView changes enqueue the collection.
- Preparing completion and Unrecoverable transitions enqueue the shard.
- Node loss/Stopping enqueues its placed shards and the affected RG collections,
  including collections without placements on that node.
- Node addition/recovery enqueues its RG's desired collections; RG migration
  covers both old and new groups.
- Ordinary row/progress changes update the cache; periodic optimization can
  evaluate their wider effects without enqueuing every collection each time.

Periodic full reconciliation enumerates configured and residual cache keys;
it does not repull upstream state or rebuild accounting. It cannot repair an
upstream update that was never published into the cache. Hook completeness
must therefore be verified independently. Apply failures and invalidated plans
explicitly requeue affected keys with bounded retry/backoff, rather than relying
only on the next periodic scan.

## 7. Applying plans from mixed-time reads

Plans retain the relevant source versions/identities. Local pre-apply reads may
reject obviously superseded plans; unrelated collection or node-load updates
must not invalidate the whole batch. DataVersion rollback prevention, exact
DataViewRef acquisition, and existing state-machine invariants remain required.

Such a precheck is not an atomic transaction with other managers. Desired
configuration or node state can change immediately afterwards. This design
allows temporary stale plans that the next reconcile corrects. It does not
promise that no old Prepare can be accepted after LoadConfig removal returns.
That stronger promise would require additional collection-scoped serialization
between desired-state writes and plan acceptance and is outside this refactor.
Likewise, a conditional shard-apply API is a possible future refinement, not a
requirement to change existing preemption policy as part of cache adoption.

## 8. Migration, performance, and verification

Implement the cache and read contracts in an isolated package with source
adapters. Keep shared read types in a dependency-leaf API package; upstream
managers must not import the concrete Balancer policy. Then add source
publication/seed interfaces and switch Policy to Reader + PlanningContext.
The old SnapshotBuilder, DataViewSnapshot APIs, and node snapshot pull adapter
have been removed; reconciliation reads only published cache entries. Do not
add production wiring or change the score formulas, batch order, or plan emission rules.

The cache removes reconcile-time segment materialization and actual-load
ledger rebuilds. It does not remove `O(S log S + S*N)` allocation work, upstream
DataView reconstruction, persistence, or RPC cost. In particular, the existing
`statsLocked()` may initially publish a fully rebuilt immutable ShardStats;
sharing it avoids another copy but does not eliminate that writer-side hotspot.
Incremental placement-block/statistics publication is a separate optimization.

Required validation:

1. Assert exact plans for static inputs, including ordering, mandatory and
   optional emission, row replacement, zero-row segments, and replicas.
   Batch tests must verify Must priority, descending shard size, deterministic
   ties, and shared predictions independently of input order.
2. Retain old references while concurrently updating; verify deep immutability
   and run race tests. Concurrent source-field updates must not overwrite one
   another.
3. Verify each NodeEntry's totals equal its contribution index, including
   duplicate publications, overlapping views, unknown rows, removals, and
   zero-row reverse-index entries.
4. Deliberately interleave node and shard reads; subtraction must use the
   contribution included in the retained NodeEntry.
5. Verify registration/replay, source readiness, updates during reconcile,
   scale-out with no placements, RG migration, and old-instance callbacks.
6. Flush abort/failure must not publish uncommitted membership. Logical drop,
   recovery footprint fill, DataView GC races, and apply failure must converge.
7. Assert reconcile uses cached shard totals/counts without scanning segments
   for those aggregates, and full reconcile does not pull sources or rebuild
   the row ledger. ReferenceRows must still reflect accepted batch deltas.
8. Benchmark Get allocations, publication copy size, retained-old-object heap
   usage, and full-reconcile CPU/allocations for both huge collections and many
   small collections. Index choices must satisfy the small-write objective.

Key implementation packages are `internal/views/coord/balancer/cache/`,
`internal/views/coord/balancer/api/`, `internal/views/coord/balancer/`,
`internal/views/coord/loadmgr/`, `internal/views/coord/coordview/`,
`internal/views/coord/nodeview/`, and `internal/dataview/`.

## 9. Implemented component boundaries

- `cache.NewFromSources` registers synchronous replay hooks with
  LoadConfigStore, DataViewManager, ShardViewRegistry, and a NodePublisher; it
  marks the cache ready only after all replays return. The owner stops the
  controller before calling `Cache.Close` and closing the node publisher.
- DataViewManager retains one finalized native projection for its latest
  collection publication. Historical DataVersion entries do not each retain
  a second native segment tree. Recovery footprint initialization replaces
  the entry while sharing its reference counter; old readers remain immutable.
- `NewDefaultBalancer` consumes the cache, and failed allocation/apply work is
  requeued with a 100 ms to 5 s loop backoff. `UpdateBalanceConfig` publishes
  a validated immutable configuration, ignores identical/invalid updates, and
  requests a full pass only for policy changes. The controller subscribes to
  ParamTable and separately wakes/resets its timer on interval changes.
- Per-collection replica-use counts avoid scanning resident shards while
  publishing one shard or removing desired config. Empty Collection/Node/RG
  entries are reclaimed once their desired/actual references disappear.
- Assembly in MixCoord/QueryCoord and concrete node/RG owner hooks remain
  outside this PR. There are no new RPCs or wire-protocol changes.

## 10. Package boundaries

- `balancer/cache`: Reader, immutable entries, publication, derived indexes,
  contribution accounting, source registration, and cache-only tests. Construct
  with `cache.New` or `cache.NewFromSources`.
- `balancer/api`: shared DataView, BalanceConfig, BalanceNode, NodeInfo, and
  TriggerScope types. This dependency-leaf package does not import the cache or
  the policy.
- `balancer`: PlanningContext, scope expansion, policy/scoring, trigger queue,
  and controller/apply. It consumes `cache.Reader` through public accessors;
  cache internals do not depend on the parent package.
- `nodeview.QueryNodePublisher` publishes through the cache/API contracts
  without depending on the concrete balancing policy.

The parent package retains aliases for the existing public scalar types; the
cache implementation, constructors, and read-entry types live in the subpackage.
Planning and integration tests stay in the parent; cache-only tests and COW
benchmarks live beside the cache. Policy and scope tests seed inputs through
public cache publication APIs; controller tests connect real registry hooks.
Old snapshot builders, ledgers, scope resolvers, and compatibility fixtures have
been removed. Explicit expected assignments replace the migration-time old/new
planner comparison.

## Replica placement facts

See [Replica Placement](replica_placement.md). Collection entries additionally maintain immutable per-node replica footprints and reference-counted ready-resource indexes. Shard statistics retain Up/Preparing node footprints and resident nodes through durable deletion. Node loss, stopping, addition and RG changes all invalidate the affected RG collections. Actual row accounting remains logical per-shard accounting; it is not physical resource deduplication. Desired target layouts are owned by the default policy, never published as cache facts.
