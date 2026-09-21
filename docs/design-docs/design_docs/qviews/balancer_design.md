# Balancer & CollectionLoadManager Design

> The resident `cache.Cache` now replaces `BalancerSnapshot` and
> `SnapshotBuilder` on the reconcile path. Batch ordering, score formulas,
> and plan emission remain
> unchanged. Production runtime wiring and RPC changes are outside this work.
>
> References: [Balancer Cache](balancer_cache.md),
> [Distributed Query View](README.md),
> [Shard View Management](shard_view_management.md), [Syncer](syncer.md),
> [QueryView State Machine](query_view_state_machine.md).

## 1. Overview

The Balancer and CollectionLoadManager manage QueryViews across replicas and
shards. State ownership is separate from planning:

1. Upstream managers own desired configuration, DataViews, node topology, and
   actual QueryView state.
2. `BalancerCache` receives synchronous publication hooks and maintains
   immutable read objects, actual-load aggregates, and reverse indexes.
3. Balancer resolves dirty scopes from the cache and requests one batch plan.
4. BalancePolicy reads immutable objects and maintains private predicted loads.
5. The executor applies the completed plan through ShardViewManager.

### Architecture

```text
LoadConfigStore    DataViewManager    Node/RG owners    ShardViewRegistry
       \                |                 |                  /
        +---------------+-----------------+-----------------+
                        | synchronous publication hooks
                        v
                  BalancerCache
                  - Collection / Shard entries
                  - Node totals + contribution indexes
                  - RG / discovery indexes and policy config
                        | publish, then enqueue affected keys
                        v
                  Balancer work queue
                        |
                        v
                  Single reconcile loop
                  1. Detach pending scopes
                  2. Resolve keys from cache
                  3. Plan(cache Reader, dirty shards)
                     - pin objects through Get
                     - maintain private projectedRows
                  4. Apply complete batch
                        |
                        v
                  ShardViewManager
                        |
                  Persist and sync scheduler
```

### Design Principles

- **Level-triggered reconciliation**: Hooks publish state before enqueueing
  affected keys. Repeated events coalesce; events arriving during processing
  remain pending for a successor pass.
- **Object-local consistency**: Each read object is immutable and internally
  consistent. Different Get calls need not represent one global instant.
- **Write-side aggregation**: Node totals, shard contributions, desired shard
  totals/counts, and RG candidate indexes are maintained on publication.
  Reconcile does not reconstruct these facts from all segments.
- **Copy-on-write with structural sharing**: Node and Collection are the main
  access units; large Collection children and node contribution indexes share
  unchanged structure. Neither reads nor small writes copy global segment data.
- **Preserved batch planning**: Must before MayOptimize, then larger shards
  first with deterministic tie-breaking. All candidates share one private
  projected-load tracker. Apply starts only after planning the batch.
- **Separated state ownership**: The cache is a derived view, not a new owner
  of load lifecycle or QueryView state. No predicted plan is written into it.

## 2. Components

### 2.1 Balancer

The controller owns a work queue and one reconcile goroutine. It resolves
scope, delegates batch decisions, and applies the completed plan.

```go
type Balancer interface {
    Start(ctx context.Context)
    Stop()
    Trigger(scopes ...TriggerScope)
}

type TriggerScope struct {
    NodeChanged      bool
    DirtyNodes       []int64
    DirtyShards      []qviews.ShardID
    DirtyCollections []int64
}
```

Existing explicit Trigger calls remain supported. `Trigger()` with no scope
requests a full cache scan. The periodic interval remains unchanged (default
10 seconds); it runs optimization and catches pending lifecycle work, without
repulling upstream state or rebuilding row accounting.

#### Main Loop

The target control flow is schematic:

```text
wait for queued work, periodic tick, retry, or cancellation
cache.WaitForReady(ctx)  // cancellation preserves pending work
pending := queue.TakePending()
dirty := resolveScope(cache, pending)
plan := policy.Plan(cache, dirty)
failedOrInvalidated := apply(plan)
requeue affected failures with bounded backoff
```

The queue detaches work before scope resolution. A concurrent publication must
remain scheduled for a later pass. The loop may start during recovery, but waits
for all sources to finish initial cache seeding before consuming pending work.
A periodic cache scan cannot repair an update that the upstream never published.

#### Scope Resolution

Scope resolution uses cache keys and indexes:

- A collection scope combines configured replicas × DataView vchannels with
  resident shards, including residual views after desired configuration removal.
- A direct shard scope targets that ShardID.
- A node-loss/Stopping scope expands through its placed-shard index.
- Node addition or recovery expands to desired collections in its RG; a new
  node has no placements to discover through the placed-shard index.
- RG reassignment covers both the old and new groups.
- Full scope combines all desired collections and all residual resident shards.
  An unkeyed node notification or an unresolvable legacy scope may fall back
  to this full scope without rebuilding the cache.

Shard details are obtained from retained CollectionEntry references. Missing
LoadConfig means release only after source readiness is known. Desired config
with a missing/unready DataView waits or retries, rather than releasing views.

#### External System Integration

| Source event | Cache publication | Reconcile scope |
|---|---|---|
| LoadConfig Put/Remove | Desired field and replica/RG indexes | Collection |
| DataView publication/drop | Membership, RowNum, per-shard totals/counts | Collection |
| Preparing completion / Unrecoverable | Actual shard and node contributions | Shard |
| Ordinary progress report | Actual shard and affected node contributions | Periodic optimization may evaluate wider effects |
| Node loss / Stopping | Node eligibility | Placed shards |
| Node addition / recovery | Node and RG indexes | RG's desired collections |
| RG migration | Old/new RG indexes and node membership | Both RGs |
| Periodic tick | None | Full cache scope |

The full hook, replay, and lock contracts are in
[Balancer Cache](balancer_cache.md#5-source-hooks-and-publication).

### 2.2 BalancePolicy

Policy retains the batch algorithm but takes a read-only cache interface:

```go
type BalancePolicy interface {
    Plan(reader cache.Reader, dirty []qviews.ShardID) *BalancePlan
}

type BalancePlan struct {
    Prepares map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder
    Releases []qviews.ShardID
}
```

These are target signatures. A call-local PlanningContext fixes each input
object after its first Get, so classification, sorting, and allocation reuse
that object's contents. Objects acquired at different times may have different
source revisions. Temporary row predictions belong only to this call; the
policy never mutates cache objects.

| Scenario | Input difference | Algorithm behavior |
|---|---|---|
| Initial load | No current placements | Place from scratch |
| Node crash | Node ineligible | Reassign lost placements, preserve surviving reuse |
| DataVersion change | New immutable desired membership | Rebuild at the new version |
| Scale-out | Additional eligible RG node | Evaluate optional moves with the same scores |
| Periodic balance | Updated actual row totals | Re-evaluate complete candidates |

### 2.3 CollectionLoadManager

Load-config lifecycle facade in `loadmgr`. It absorbs the desired-state parts
of the legacy `CollectionManager` and `ReplicaManager`: parsing DDL callback
messages, persisting `LoadConfig`, and notifying Balancer reconciliation.
Replica node membership is not persisted in load config; Balancer expands each
replica's resource group to live QueryNodes when it allocates QueryViews.

```
loadmgr.CollectionLoadManager
├── LoadConfigStore          ← desired state
│   ├── LoadConfig per collection (persisted)
│   ├── Replica RG constraints (persisted, embedded in LoadConfig)
│   └── Full-config writes with orphan cleanup
│
└── source publication hook  ← synchronous cache update, then dirty notification
```

#### LoadConfigStore

Owns **desired state**: per-collection load config and replica resource-group
constraints. Does not know about ShardID, views, or live node membership —
focused on LoadConfig persistence and immutable per-collection publication.
The cache registers a source hook; its publish-before-notify contract replaces
the Balancer dependency on a global LoadConfigSnapshot.

```go
// Concrete desired-state owner. Existing snapshot APIs may remain for
// compatibility; Balancer consumes the cache publication instead.
type LoadConfigStore struct { /* ... */ }

// Sole constructor: loads persisted state from ETCD at construction.
func RecoverLoadConfigStore(ctx context.Context, catalog metastore.QueryCoordCatalog) (*LoadConfigStore, error)

// Put persists the full LoadConfig. Always writes all current keys
// (CollectionLoadInfo + all PartitionLoadInfo + all Replica), and deletes
// orphan partitions / replicas present in the previous state but absent
// from the new config.
func (s *LoadConfigStore) Put(ctx context.Context, cfg *LoadConfig) error

// Remove deletes all persisted state for a collection
// (CollectionLoadInfo + PartitionLoadInfo keys + all Replicas).
// No-op if the collection is not present.
func (s *LoadConfigStore) Remove(ctx context.Context, collectionID int64) error

func (s *LoadConfigStore) Snapshot() *LoadConfigSnapshot

// LoadConfig is the complete load configuration for a collection.
type LoadConfig struct {
    DbID                     int64
    CollectionID             int64
    PartitionIDs             []int64
    LoadFields               []*messagespb.LoadFieldConfig  // field_id + index_id
    UserSpecifiedReplicaMode bool
    Replicas                 []*ReplicaAssignment
}

// ReplicaAssignment unifies DDL replica config. Runtime node membership is
// derived from ResourceGroup by Balancer.
type ReplicaAssignment struct {
    ReplicaID     int64                   // from DDL
    ResourceGroup string                  // from DDL
    Priority      commonpb.LoadPriority   // from DDL
}

// Deep-copy helpers for mutation by callers.
func (c *LoadConfig) Clone() *LoadConfig
func (r *ReplicaAssignment) Clone() *ReplicaAssignment

// Builder from a DDL message.
func FromAlterLoadConfigMessage(msg *messagespb.AlterLoadConfigMessageHeader) *LoadConfig
```

**Persistence**: Split storage, reusing legacy key formats for upgrade compatibility:

- `querycoord-collection-loadinfo/{collectionID}` — CollectionLoadInfo proto
- `querycoord-partition-loadinfo/{collectionID}/{partitionID}` — PartitionLoadInfo proto
- `querycoord-replica/{collectionID}/{replicaID}` — Replica proto

Legacy proto fields are kept for wire compatibility but ignored by the new design: `ro_nodes`, `rw_sq_nodes`, `ro_sq_nodes`, `channel_node_infos`, `status`, `recover_times`, `load_percentage`, `replica_number`, `load_type`, `released_partitions`. New design uses only `nodes` (RW only), `resource_group`, `ID`, `collectionID` on Replica, and `collectionID`, `dbID`, `load_fields`, `field_indexID`, `user_specified_replica_mode` on CollectionLoadInfo. **TODO**: `Priority` is carried in memory but not yet persisted (needs a new Replica proto field).

**Copy-On-Write semantics**: Put clones caller-owned configuration and never
mutates a published value. The target publication hook shares this immutable
value and its source revision with the cache before Put returns. Remove
publishes desired absence before returning. Existing Snapshot APIs can remain
compatibility views but are not the Balancer read path.

**Write amplification**: Put always writes the full config (no diff). Orphan partitions / replicas (present in previous state but absent from new config) are deleted. This is intentionally simple — dedup / diff optimization can be added later if write volume becomes a concern.

#### ShardViewRegistry

Owns **actual view state**: ShardViewManager lifecycle and view-derived indexes.

```go
type ShardViewRegistry struct { /* ... */ }

func RecoverShardViewRegistry(
    ctx context.Context,
    catalog queryview.QueryViewCatalog,
    syncer syncer.ReliableSyncer,
    dataViewRefs qviews.DataViewRefProvider,
) (*ShardViewRegistry, error)
func (r *ShardViewRegistry) Ensure(shardID qviews.ShardID) *ShardViewManager
func (r *ShardViewRegistry) Get(shardID qviews.ShardID) *ShardViewManager
func (r *ShardViewRegistry) Snapshot() *ShardViewSnapshot
func (r *ShardViewRegistry) SnapshotForShards(shardIDs []qviews.ShardID) *ShardViewSnapshot
func (r *ShardViewRegistry) CollectionShards(collectionID int64) []qviews.ShardID
func (r *ShardViewRegistry) NodeShards(nodeID int64) []qviews.ShardID
func (r *ShardViewRegistry) ShardIDs() []qviews.ShardID
func (r *ShardViewRegistry) RegisterStatsObserver(observer func(qviews.ShardID, *ShardStats))
```

Registry remains the lifecycle owner of shard managers. With the resident cache,
its source adapter publishes actual shard state and removal into the cache;
cache indexes supply Balancer scope resolution. The listed Snapshot and
RegisterStatsObserver APIs describe the existing implementation, not the new
publication contract. Registration must additionally cover initial replay and
manager removal without missing updates.

The cache shares published immutable statistics, then updates affected node
contributions and reverse indexes. Keeping the existing full `statsLocked()`
calculation initially is possible, but does not eliminate its writer-side
cost. Incremental statistics publication is a separate optimization.

An ordinary empty manager may remain resident. A released manager is removed
after its last view completes durable removal; its cache state is removed with
an identity check so a late callback cannot erase a replacement manager.

#### CollectionLoadManager (Facade)

```go
type CollectionLoadManager interface {
    // Facade parses WAL ack results and calls LoadConfigStore.
    // Store publication updates cache before enqueuing Balancer work.
    UpdateLoadConfig(ctx context.Context, result message.BroadcastResultAlterLoadConfigMessageV2) error
    ReleaseCollection(ctx context.Context, msg *messagespb.DropLoadConfigMessageHeader) error
}
```

**DDL callback integration**:

```
WAL ack of CChannel AlterLoadConfig broadcast
        → CollectionLoadManager.UpdateLoadConfig(result)
         ├── parse result.Message.Header() → LoadConfig
         ├── LoadConfigStore.Put(fullCfg)  // full write + orphan cleanup
         └── store hook: publish cache, then enqueue collID

WAL ack of CChannel DropLoadConfig broadcast
        → CollectionLoadManager.ReleaseCollection(msg.Header())
         ├── LoadConfigStore.Remove(collID)
         └── store hook: publish cache, then enqueue collID
         // ShardViewRegistry cleanup via reconcile: Phase 1
         // sees "desired absent + current exists" → actionRelease
```

`AlterLoadConfig` and `DropLoadConfig` are CChannel-only broadcasts. Coord uses the
broadcast completion callback to update `CollectionLoadManager`. StreamingNode does not
persist a vchannel-local load config; QueryView metadata identifies the versioned load
info used when the local state machine acquires resources.

**Release semantics (Option A)**: `ReleaseCollection` removes LoadConfig; its
source hook clears the cache desired field and enqueues the collection. Orphan
views are detected by Phase 1 and released via `RequestRelease`. No separate
"releasing" desired state is needed; crash recovery uses the same reconciliation.

**Load status derivation**: `LoadStatus` and `LoadPercentage` are derived from view states:

- **Loaded**: All shards for the collection have an Up view.
- **Loading**: At least one shard has no Up view.
- **LoadPercentage**: `count(shards with Up view) / count(total shards) * 100`.

**Recovery**: Register source hooks before recovery, or use a registration and
initial-replay contract without a missing-update window. Recover LoadConfig,
DataView references, shard state, and topology; populate cache aggregates and
indexes; then mark all sources ready and start reconciliation. No first-pass
ledger hydration is required.

## 3. Policy Planning

The Policy organizes work into three phases and processes all dirty shards
in one batch so decisions across shards remain coordinated.

The cache Reader supplies eligible nodes, actual node row totals and their
contributions, per-segment RowNum, desired shard summaries, and current
placement. PlanningContext retains immutable references and private predictions.
No additional RPC, protobuf, or QueryView lifecycle state is required.

```
Plan(reader, dirty)
    │
    ▼
Phase 1: Classify each dirty shard
    │
    │  for each shardID in dirty:
    │    actionRelease     → add to plan.Releases
    │    actionMust        → queue as mandatory candidate
    │    actionMayOptimize → queue as optional candidate
    │    actionNone        → skip
    │
    ▼
Phase 2: Order candidates + build complete placements with a shared
         steady-state row tracker
    │
    │  Mandatory first, then optional.
    │  Rebuild each shard from an empty partial candidate.
    │  After each accepted candidate, update projectedRows so later shards see it.
    │
    ▼
Phase 3: Emit mandatory candidates; emit optional candidates only when
         their complete assignment changed
    │
    ▼
Return BalancePlan { Prepares, Releases }
```

### 3.1 Phase 1: Classify

Pure state comparison. For each dirty shard:

| Condition (checked in order) | Action |
|---|---|
| Desired absent, current exists | **Release** |
| Both absent | **None** |
| Desired present, no Up view, Preparing exists | **None** (avoid stacking) |
| Desired present, no Up view or only Unrecoverable state | **Must** (initial load or post-Unrecoverable) |
| Current Up view and a Preparing view exists | **None** (avoid stacking; retry latest state after the in-flight view finishes) |
| Current DataVersion < DataView DataVersion | **Must** (data changed) |
| Current view references unavailable node | **Must** (node lost) |
| Current `LoadInfoVersion` differs from the collection load-config version | **Must** (partition/field/settings changed) |
| None of the above | **MayOptimize** (steady-state balance) |

### 3.2 Phase 2: Normalized Ordered Allocation

Candidates are processed in priority order — mandatory (Must) first, optional
(MayOptimize) last. Within each group, larger shards by total `RowNum` are
processed first, then by ShardID. Within a shard, segments are processed by
`RowNum` descending and SegmentID ascending. The explicit secondary ordering
makes planning deterministic.

A shared `projectedRows` tracker starts from pinned NodeEntry row totals.
Before rebuilding a shard, Policy subtracts its contribution from those same
NodeEntry versions; it must not use independently refreshed shard statistics
for that subtraction. The candidate adds each desired segment exactly once.
Accepted replacements update a private delta, so later shards see their
steady-state effect. Desired shard size and segment count come from cached
DataView summaries. See [batch accounting](balancer_cache.md#42-planningcontext-and-predicted-loads).

This is deliberately different from transient preparation accounting. It does
not keep the old shard placement and add the complete replacement on top: doing
so would double-count moved segments and same-node reuse. Preparation overlap
and migration concurrency are execution-layer concerns.

**Hard constraints** (any failure excludes the node):

| Constraint | Description |
|---|---|
| Node Health | Must be alive and not stopping |
| Resource Group | Must belong to this replica's resource group |

**Soft constraints** are three independent normalized scores. Every component
and their weighted combination is bounded in `[0, 1]`.

#### StickinessScore

Stickiness is local to one `(segment, candidateNode)` decision. It never
accumulates the rows moved by earlier segments.

```text
MovePenalty(segment) =
    min(segment.RowNum / StickyRowsScale, 1.0)

StickinessScore(segment, node) =
    1.0
        if node has a valid reusable copy
        or the segment has no historical placement
        or the segment has no eligible reusable location

    1.0 - MovePenalty(segment)
        otherwise
```

The mandatory exception is segment-local. A mandatory shard rebuild still
preserves stickiness for surviving reusable segments; only a segment whose old
location cannot be reused becomes neutral. There are no time factors or
state-specific fractional affinity weights.

#### NodeLoadScore

For a shard with `N` eligible nodes, remove the current shard rows from the
shared tracker and define:

```text
BaseRows(node) = projected rows excluding the shard being rebuilt

ReferenceRows =
    (sum(BaseRows over eligible nodes) + ShardTotalRows) / N
```

`ReferenceRows` is fixed for the entire shard candidate. When tentatively
placing a segment:

```text
ProjectedRows(node) =
    BaseRows(node)
  + rows already assigned to node in the partial candidate
  + segment.RowNum

NodeLoadScore(segment, node) =
    ReferenceRows / (ReferenceRows + ProjectedRows(node))
```

If `ReferenceRows == 0`, the score is `1.0` on every node. Otherwise a
tentative placement with `ProjectedRows == 0` scores `1.0`, a placement at the
reference load scores `0.5`, and increasingly heavy projected placements
approach `0.0`. An empty `BaseRows` entry still includes the tentative
segment's rows in `ProjectedRows`. The fixed reference keeps the normalization
stable while the partial candidate grows.

#### FanoutScore

Fanout is a one-time cost for opening another QueryNode for this shard:

```text
FanoutBudget = min(
    EligibleNodeCount,
    SegmentCount,
    max(1, ceil(ShardTotalRows / TargetRowsPerShardNode)),
)
```

`FanoutBudget` is a free budget, not a fanout target that must be reached. Let
`OpenedNodes` contain nodes already used by the partial candidate. It starts
empty and is not pre-populated from the old placement.

```text
FanoutScore(segment, node | partialCandidate) =
    1.0  if node is already open
    1.0  if node is new and len(OpenedNodes) < FanoutBudget
    0.0  if node is new and len(OpenedNodes) >= FanoutBudget
```

If an over-budget node still wins, only the segment that opens it pays the
fanout cost. Later segments on the same node reuse the opening with score
`1.0`.

#### Combined placement intent

```text
PlacementIntent(segment, node | partialCandidate) =
    (
        StickinessWeight * StickinessScore
      + NodeLoadWeight   * NodeLoadScore
      + FanoutWeight     * FanoutScore
    )
    / (StickinessWeight + NodeLoadWeight + FanoutWeight)
```

Weights are non-negative and at least one must be positive. They control only
relative contribution; they are not normalization constants. A reusable
segment moves only when the weighted node-load and fanout benefit exceeds its
weighted stickiness loss.

The production calibration is:

```text
StickinessWeight       = 1
NodeLoadWeight         = 1
FanoutWeight           = 1
StickyRowsScale        = 1,000,000 rows
TargetRowsPerShardNode = 100,000 rows
```

These defaults intentionally give the two full-point penalties strong boundary
semantics:

- A segment at or above `StickyRowsScale` cannot be moved by node-load benefit
  alone when fanout is equal. It is one indivisible migration whose movement
  cost has saturated; mandatory relocation is still neutral when no eligible
  reusable copy exists. Large shards remain balanceable by moving their
  smaller, unsaturated segments.
- When stickiness is equal, node-load benefit alone cannot open a node beyond
  `FanoutBudget`. This prevents a shard that fits its row-derived budget from
  spreading only to improve a small load difference. Recalibrating relative
  weights can relax this boundary, but is a policy change that must repeat the
  fanout and migration-cost experiments.

When scores are numerically equal, the allocator prefers a valid reusable copy,
then an already-open node, then lower projected rows, then lower NodeID. The
comparison epsilon handles floating-point precision only and is not a balance
threshold.

### 3.3 Phase 3: Candidate Emission

A complete shard candidate is only the final mapping
`SegmentID -> QueryNodeID`. It does not receive a second aggregate score.
Summing the per-segment values would repeatedly count node load, turn a
one-time fanout opening into a per-segment cost, and reintroduce segment-count
bias.

- Release actions are always emitted.
- Mandatory candidates are emitted whenever allocation succeeds, even if their
  assignments are unchanged, because DataVersion or settings may still need to
  advance.
- Optional candidates are emitted only when the complete assignment differs
  from the current assignment. A changed optional candidate is accepted
  directly.

There is no plan-level `BalanceThreshold`, `CostEfficiencyThreshold`, or
migration-gain score. Stickiness already provides the migration-benefit gate at
the segment decision point. Local version validation, prepare concurrency,
and migration-row throttling belong to plan execution and may delay work, but
do not decide whether a placement is economically worthwhile.

### 3.4 Computation and Cost Boundaries

The cache moves actual-state aggregation to publication. It does not change
candidate allocation: collecting/sorting desired segments costs `O(S log S)`
and evaluating eligible nodes normally costs `O(S*N)` per allocated shard.
Overlapping placement copies can add stickiness-scan work. FanoutBudget remains
a score, not a hard cap on candidate enumeration. A stable optional shard may
still require a full allocation before assignment equality suppresses emission.

The target reconcile work is:

| Work | Location and cost boundary |
|---|---|
| Read Collection/Node objects | Get references; no segment deep copy or global snapshot construction |
| Order dirty shards | Cached TotalRows lookup plus `O(D log D)` sorting |
| Establish node baselines | `O(M)` retained references/scalars; no global consistency requirement |
| Replace candidate contributions | Read the pinned node's contribution index; lookup cost depends on its shared representation |
| Calculate ReferenceRows | Sum predicted eligible-node loads once per candidate |
| Allocate segments | Existing segment sort and node scoring |
| Return accepted plans | Memory proportional to accepted segment assignments |

RG candidate indexes are maintained when topology changes. No reconcile scans
all placements to recompute node totals, or scans desired segments solely to
compute TotalRows/SegmentCount. Contribution replacement and scoring may still
traverse many nodes; this design does not claim to eliminate every `D*M` term.
The old implementation's detailed bounds are retained in Appendix A as a
baseline, not a guarantee of the target data structure's lookup complexity.

## 4. BalancerCache and PlanningContext

The complete object model, interfaces, hooks, initialization, and consistency
contract are in [Balancer Cache](balancer_cache.md). It replaces the previous
per-cycle BalancerSnapshot and SnapshotBuilder model.

### 4.1 Actual-State Aggregation

Upstream publication and cache updates maintain:

- DataView shard TotalRows and SegmentCount, together with membership and RowNum.
- Actual shard-to-node contributions from retained QueryView statistics.
- Each NodeEntry's Up/Pending totals and its matching contribution index.
- Node/RG eligibility and collection/shard discovery indexes.

Published RowNum is the only load metric. Known zero is distinct from unknown;
exact-version statistics take precedence over historical fallback. This design
adds neither MemSize nor a segment-count node-load score. Transform start-after
TimeTick production remains the separate TODO in
[the frontier design](transform_start_after_timetick.md); cache adoption does
not implement that protocol.

### 4.2 Within-Batch Prediction

PlanningContext pins object references and owns the only mutable planning data:

```text
base[n] = pinnedNode[n].TotalRows
old[n,s] = pinnedNode[n].Contribution(s).TotalRows
projected[n] = base[n] + acceptedDelta[n]

for each candidate s in the existing policy order:
    candidateBase[n] = projected[n] - old[n,s]
    candidate = allocate(s, candidateBase)
    if accepted:
        acceptedDelta[n] += candidate.Rows[n] - old[n,s]
```

Releases subtract their pinned old contributions before allocation; rejected
optional candidates leave predictions unchanged. The node total and subtracted
contribution belong to one object version, while different nodes and
collections may reflect different times. ReferenceRows and partial assignments
are recomputed from these predictions, not written back to the cache.

### 4.3 Apply and Convergence

The whole batch is planned before any of its actions are applied. Apply retains
DataVersion non-regression and exact-version reference acquisition. A lost
version or failed action requeues affected keys. Local prechecks may reject
obviously superseded work, but do not form a cross-manager transaction. The
refactor permits temporary stale plans that subsequent reconciliation corrects;
stronger serialization of desired-state writes against plan acceptance is
outside scope.

## 5. Event Processing Examples

### 5.1 Node Crash Recovery

1. The node owner publishes QN3 as ineligible, then enqueues its placed shards.
2. QueryView failure transitions independently publish actual contributions.
3. Balancer detaches work, obtains cache references, and classifies candidates.
4. Lost placements are redistributed with surviving-copy stickiness preserved.
   Accepted candidates update only the private projected-load delta.
5. Apply creates Preparing views; their source hooks publish actual state.
   Updates during the batch remain queued for a successor pass.

### 5.2 Load Collection

1. CollectionLoadManager persists the desired config through LoadConfigStore.
2. The store hook publishes the Collection desired field and indexes before
   enqueueing C1 and returning.
3. Scope resolution combines configured replicas and published DataView shards.
4. Policy uses cached shard totals for ordering and allocates complete placements
   with one shared projected-load tracker.
5. Apply calls AddPreparing; the exact DataViewRef is acquired before existing
   views can be preempted. Missing versions cause replanning.

### 5.3 Optional Scale-Out

1. Node/RG publication adds the node and enqueues desired collections in the RG,
   even though the new node has no placed shards.
2. Stable shards classify as MayOptimize; existing scores decide each placement.
3. Each complete candidate is compared against its current assignment. Unchanged
   candidates emit no plan; accepted candidates update the shared prediction.
4. The executor applies the completed batch. Cache load values change only
   through actual-state hooks, not through speculative plan construction.

## 6. Thread Safety

| Component | Concurrency model |
|---|---|
| Balancer | Single reconcile goroutine; thread-safe key enqueue |
| BalancerCache | Object/partition locks for publication and lookup; immutable published object graphs |
| PlanningContext | Call-local retained references and mutable prediction deltas |
| BalancePolicy | No shared mutable planning state; one context per Plan call |
| LoadConfigStore | Existing desired-state serialization, then synchronous publication |
| ShardViewRegistry/Manager | Existing lifecycle locks; synchronous actual-state publication |
| Source adapters | Never re-enter upstream from cache locks or perform I/O in hooks |

Readers release cache locks before allocation or plan application. Writers
follow upstream-to-cache lock ordering. Different fields of a shared entry are
merged under its cache lock; published descendants remain immutable.

## 7. Component Responsibilities

| Component | Responsibility | State |
|---|---|---|
| Balancer | Resolve queued scopes, call Policy, apply/requeue | Work queue |
| BalancerCache | Publish readable actual/desired facts and derived indexes | Immutable objects, aggregate contribution indexes |
| PlanningContext | Reuse per-object reads and coordinate batch predictions | References, candidate-local data, accepted deltas |
| BalancePolicy | Classify, order, allocate, compare | No cross-call state |
| CollectionLoadManager | Desired load lifecycle through LoadConfigStore | Existing store |
| ShardViewRegistry | Actual manager lifecycle and source publication | Resident managers |
| ShardViewManager | QueryView state and placement/statistics publication | Per-shard state machines and DataViewRefs |

## 8. Package Layout and Migration

The existing implementation lives in:

- `internal/views/coord/balancer/`: loop, scope queue, policy and scoring;
  snapshot_builder.go and snapshot.go currently implement the old read path.
- `internal/views/coord/loadmgr/`: load config owner and lifecycle facade.
- `internal/views/coord/coordview/`: actual QueryView owners and sync scheduler.
- `internal/views/coord/nodeview/`: current pull-based topology adapter.
- `internal/dataview/`: immutable version publication and reference ownership.

Add an isolated cache implementation and dependency-leaf read types, then source
publication/initialization adapters, then switch Policy to Reader and its private
PlanningContext. Do not create upstream-to-policy import cycles. Remove the
snapshot builder from reconcile; compatibility snapshot APIs can remain for
other consumers. Production runtime wiring is still a separate task.

## 9. Future Considerations

1. **Preparing timeout eviction**: Periodic reconcile can detect shards stuck in Preparing beyond a timeout → mark as Unrecoverable to release the slot.
2. **Global optimization passes**: The current Policy uses deterministic per-shard greedy allocation with a shared steady-state row tracker. For batches where many shards need rebalancing simultaneously (e.g., scale-out), a second optimization pass could detect and resolve cross-shard conflicts (two shards both wanting the same lightly-loaded node).
3. **Disk-based scoring**: Add `DiskUsage`/`DiskCapacity` back to `BalanceNode` and a disk-balance soft constraint once mmap / disk-index segments are in scope.
4. **Rate limiting**: Cap concurrent Preparing views across all shards to prevent overwhelming the cluster during large-scale events.

## 10. Verification

### 10.1 Score Invariants

1. `StickinessScore`, `NodeLoadScore`, `FanoutScore`, and `PlacementIntent` are
   always within `[0, 1]`.
2. A segment's stickiness does not change because earlier segments moved.
3. `ReferenceRows` remains fixed while one shard candidate is constructed.
4. Opening an over-budget node is penalized once; reusing it is not penalized
   again.
5. Equal-score selection is deterministic and prefers reuse before movement.

### 10.2 Policy Behavior

1. Current shard rows are removed before candidate assignments are added, so
   same-node reuse is not double-counted.
2. Ten small segments whose total rows fit one shard-node target do not
   automatically fan out to ten QueryNodes.
3. A large shard can use more QueryNodes within its row-derived fanout budget
   when node-load benefit justifies it.
4. Small load improvements do not overcome segment stickiness; sufficiently
   large improvements move unsaturated segments, while saturated stickiness is
   the maximum optional movement cost under the equal default weights.
5. Node loss neutralizes stickiness only for segments without an eligible
   reusable copy.
6. DataVersion advancement places new segments without unnecessarily moving
   surviving reusable segments.
7. Optional optimization emits no plan when the complete assignment is
   unchanged and requires no additional gain threshold when it changes.
8. Replanning an applied candidate with unchanged inputs produces no further
   optional plan.
9. Earlier accepted shards update the shared steady-state row tracker seen by
   later shards in the same batch.
10. Under production defaults, pure node-load benefit does not open a node
    beyond `FanoutBudget` when stickiness is equal.

### 10.3 End-to-End Scenarios

1. Initial load balances rows without unnecessary shard fanout.
2. Low-benefit scale-out remains a no-op.
3. High-benefit scale-out moves segments when weighted load gain exceeds
   stickiness.
4. Flush/DataVersion changes preserve reusable placements and load new
   segments.
5. QueryNode failure performs mandatory recovery and converges.
6. A small shard previously spread over many QueryNodes consolidates to a
   smaller node subset.

### 10.4 Cache Publication and Batch Accounting

1. Cache scopes cover dirty collection/shard/node events and residual releases;
   a new node with no placements still triggers its RG's desired collections.
2. Each retained object stays immutable across concurrent source publications.
   Concurrent updates to different fields do not overwrite one another.
3. Node totals match the contributions included in that same NodeEntry.
   Mixed-time reads still subtract that version's contribution correctly.
4. Desired shard totals and counts are published with their membership/RowNum;
   reconciliation does not rescan segments to compute these summaries.
5. Full reconcile performs no upstream snapshot pull or row-ledger rebuild.
6. Registration/replay, readiness, repeated updates, old-instance callbacks,
   deletion, and updates during processing do not lose work.
7. Failed apply and missing DataView versions requeue. Flush aborts do not
   expose unpublished membership, and cache refs do not bypass DataView GC.
8. Static-input plans match the previous algorithm. ReferenceRows reflects
   earlier accepted candidates rather than a static cached RG total.

See [cache verification](balancer_cache.md#8-migration-performance-and-verification)
for concurrency and performance scenarios.

## Appendix A. Pre-Refactor Implementation Complexity

The following bounds describe the existing SnapshotBuilder-era implementation,
including full projected-row map clones and per-shard node filtering/sorting.
They are retained as the optimization baseline. They do not describe the target
cache's nested-index lookup cost or claim these old copies remain required.

Let:

- `K` be the number of shard IDs passed to `Plan`, including duplicates;
- `D` be the number of unique dirty shards in one `Plan` call;
- `M` be the total number of QueryNodes in the snapshot;
- `A` be the number of dirty shards for which allocation is attempted;
- `L` be the number of release shards, where `A + L <= D`;
- `S_i` be the number of desired segments in shard `i`;
- `R_i` be the number of replica assignments in shard `i`'s load config;
- `G_i` be the number of QueryNodes in shard `i`'s Resource Group;
- `N_i` be the number of eligible QueryNodes for shard `i`;
- `P_i` be the size of shard `i`'s current placement state: its tracked segment
  records plus their `(segment, node)` state entries. In steady state `P_i` is
  `Theta(S_i)`; it can be larger while multiple views overlap.

`N_i <= G_i <= M`. The distinction matters because the current implementation
sorts every node in the Resource Group before filtering out unavailable nodes,
but evaluates scores only on the remaining eligible nodes.

The work for one allocated shard in the current implementation is:

| Step | Time |
|---|---:|
| Classify the shard and calculate its row size | `O(P_i + S_i)` |
| Remove the current shard contribution from projected rows | `O(M + P_i)` |
| Resolve the shard's replica assignment | `O(R_i)` |
| Collect and sort desired segments | `O(S_i log S_i)` |
| Build and sort the eligible-node set and fixed `ReferenceRows` | `O(M + G_i log G_i)` |
| Evaluate every eligible node for every segment | normally `O(S_i * N_i)`, strict worst case `O(N_i * (S_i + P_i))` |
| Compare an optional candidate with the current assignment | `O(P_i + S_i)` |
| Commit an accepted candidate to projected rows | `O(M + N_i)` |

`NodeLoadScore`, `FanoutScore`, weighted aggregation, and tie-breaking are all
`O(1)` for one `(segment, node)` evaluation. Logically, stickiness also needs
only the candidate node's reusable-copy state plus one segment-level boolean:
whether any eligible reusable copy exists. The current implementation derives
that boolean by scanning the segment's current copies during every candidate
node evaluation. Consequently, if a segment has `C_s` current copies, its
strict evaluation cost is `O(N_i * (1 + C_s))`; summed over the shard this is
`O(N_i * (S_i + P_i))`.

In the normal steady state, each segment has at most one relevant current copy,
so `P_i = Theta(S_i)` and candidate-node selection is `O(S_i * N_i)`. During
overlapping views, the strict bound records the additional copy scan. In the
pathological case where every segment is represented on every eligible node,
`P_i = O(S_i * N_i)` and this implementation can reach `O(S_i * N_i^2)`.
Precomputing the segment-level reusable-copy boolean once would reduce that
term back to `O(P_i + S_i * N_i)` without changing placement behavior.

The strict time complexity of one `Plan` call is therefore:

```text
O(
    K
  + D log D
  + M
  + (A + L) * M
  + sum over all dirty shards of (S_i + P_i)
  + sum over allocated shards of (
        R_i
      + S_i log S_i
      + G_i log G_i
      + N_i * (S_i + P_i)
    )
)
```

`K` covers deduplication, `D log D` covers candidate/release ordering, and the
standalone `M` term initializes `projectedRows`. The `(A + L) * M` term covers
the full-map clones used to remove a shard and, within the same asymptotic
bound, to install each accepted candidate (`accepted <= A`). Shards classified
as no-op do not pay this clone cost.

For the expected steady state (`P_i = Theta(S_i)`), the batch complexity
simplifies to:

```text
O(
    K
  + D log D
  + M
  + (A + L) * M
  + sum over all dirty shards of (S_i + P_i)
  + sum over allocated shards of (
        R_i
      + S_i log S_i
      + G_i log G_i
      + S_i * N_i
    )
)
```

With uniform upper bounds of `S` desired segments, `R` replica assignments,
`G <= M` Resource Group nodes, and `N <= G` eligible nodes per dirty shard,
this is:

```text
O(K + D log D + D * (M + R + S + S log S + G log G + S * N))
```

The shard-allocation core is therefore
`O(S log S + S * N) = O(S * (log S + N))` in steady state. Segment sorting
dominates when `log S > N`; candidate scoring dominates when `N > log S`.
Normalized scoring and fanout tracking add only constant work to each
segment-node comparison. The design deliberately avoids enumerating complete
placements, whose search space would be `N^S`.

Additional working memory, excluding the returned `BalancePlan`, is:

```text
O(D + M + max_i(S_i + N_i + P_i))
```

This covers dirty-shard deduplication and ordering, the shared projected-row
tracker, and the largest partial shard candidate being constructed. The
returned plan itself stores every accepted candidate and segment assignment
and therefore requires
`O(L + acceptedCandidates + sum over accepted candidates of S_i)` space.

These bounds exclude snapshot acquisition. `SnapshotBuilder` reads only the
resolved DataView and ShardView scope, refreshes row counts for dirty shards,
and uses the row-count ledger to populate cluster-wide node loads. Initial and
periodic full reconciles rebuild the complete ledger.
