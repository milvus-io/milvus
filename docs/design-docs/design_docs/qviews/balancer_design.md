# Balancer & CollectionLoadManager Design

> This document describes the design of the Coord-side Balancer and CollectionLoadManager, which together are responsible for generating, managing, and maintaining QueryViews across all replicas and shards.
> Reference: [Distributed Query View Design](README.md), [Shard View Management](shard_view_management.md), [Syncer Design](syncer.md), [QueryView State Machine](query_view_state_machine.md), [view.proto](../../../../pkg/proto/view.proto)

## 1. Overview

The Balancer and CollectionLoadManager sit above the per-shard `ShardViewManager` layer on the Coord side. They are responsible for:

1. **Deciding WHEN and WHICH shards** need new QueryViews (Balancer reconcile loop).
2. **Deciding HOW to assign segments to QueryNodes** (BalancePolicy).
3. **Persisting desired load state and load lifecycle callbacks** (CollectionLoadManager).
4. **Maintaining actual shard-view indexes and statistics** (ShardViewRegistry).

### Architecture

```
External Inputs (provided by other Coord modules, consumed as interfaces)
├── Node Manager         → available QueryNode list
├── Replica Manager      → Replica-to-Node mapping
├── DataView Manager     → per-shard sealed segment list (DataView)
│
DDL Callbacks (WAL message acknowledgment)
├── AlterLoadConfigMessage → CollectionLoadManager.UpdateLoadConfig()
└── DropLoadConfigMessage  → CollectionLoadManager.ReleaseCollection()
        │
        │  state changes → Balancer.Trigger()
        ▼
┌──────────────────────────────────────────────────────────────┐
│                        Balancer                              │
│  • Work queue (deduplicated shard IDs)                       │
│  • Single goroutine loop:                                    │
│      1. Detach the pending trigger batch                     │
│      2. Build BalancerSnapshot (global world view)           │
│      3. Expand trigger scopes into dirty shards              │
│      4. policy.Plan(snapshot, dirty) → BalancePlan           │
│      5. Apply plan (AddPreparing + ReleaseShardViews)        │
│  • Periodic ticker as fallback (full scan)                   │
└────────────┬────────────────────────────┬────────────────────┘
             │ build snapshot              │ apply plan
             ▼                             ▼
┌──────────────────────────────────────────────────────────────┐
│                 Coord-side QueryView state                   │
│  ┌──────────────────────────┐ ┌──────────────────────────┐  │
│  │    LoadConfigStore       │ │   ShardViewRegistry      │  │
│  │  • Desired state (load   │ │  • ShardViewManager      │  │
│  │    config + replica      │ │    lifecycle             │  │
│  │    assignments)          │ │  • Aggregates Stats()    │  │
│  │  • ETCD persistence      │ │    across all shards     │  │
│  └──────────────────────────┘ └──────────────────────────┘  │
│  CollectionLoadManager lives in loadmgr and connects DDL     │
│  broadcast results to LoadConfigStore, ShardEnsurer, and     │
│  Balancer.                                                   │
│                                                              │
│  ┌─────────────┐ ┌─────────────┐ ┌─────────────┐            │
│  │ShardViewMgr │ │ShardViewMgr │ │ShardViewMgr │  ...       │
│  │(replica1,   │ │(replica1,   │ │(replica2,   │            │
│  │ vchan0)     │ │ vchan1)     │ │ vchan0)     │            │
│  └─────────────┘ └─────────────┘ └─────────────┘            │
└──────────────────────────────────────────────────────────────┘
             │                             │
             ▼                             ▼
         Catalog (ETCD)              ReliableSyncer → Nodes
```

### Design Principles

- **Level-triggered reconciliation (Kubernetes controller pattern)**: Event sources with a notifier enqueue affected shard IDs via `Trigger(scope)`; the Balancer compares desired vs. current state and converges. The periodic full scan covers sources without a direct notifier and any missed event. Multiple enqueues for the same shard are deduplicated.
- **Unified allocation algorithm**: A single BalancePolicy handles all scenarios. Scenario differences are encoded in input variations (available nodes, current view, data view), not separate algorithms. This prevents thrashing.
- **Batch planning**: Each reconcile cycle detaches the pending trigger batch, builds a global snapshot, expands the batch into dirty shards, and asks the Policy for a complete plan. The Policy handles all dirty shards in one call, enabling cross-shard coordination (e.g., avoiding multiple shards targeting the same node).
- **External inputs as interfaces**: Node Manager, Replica Manager, and DataView Manager are external dependencies consumed via interfaces.
- **Separated desired and actual state ownership**: `loadmgr` owns load-config lifecycle and desired-state snapshots; `coordview` owns actual QueryView state, shard-view aggregation, and actual-state snapshots. The Balancer composes both snapshots during reconciliation.

## 2. Components

### 2.1 Balancer

The top-level scheduling framework. Owns a work queue and a single reconcile goroutine. Pure executor: builds snapshots, delegates decisions to Policy, applies the resulting plan.

#### Interface

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

- `Trigger()` with no scopes triggers a full scan.
- `Trigger(scope)` records only the affected scope; the reconcile cycle expands it into shard IDs from its load-config and shard-view snapshots.
- A periodic ticker (e.g., 10s) calls `Trigger()` as a safety net for missed events and steady-state balance checks.

#### Main Loop

```go
for {
    select {
    case <-queue.Signal():
    case <-ticker.C:
        queue.AddAll()   // full scan
    case <-ctx.Done():
        return
    }

    pending := b.queue.TakePending()
    if pending.Empty() {
        continue
    }

    snap  := b.buildSnapshot()
    dirty := pending.Expand(snap)
    if len(dirty) == 0 {
        continue
    }

    plan := b.policy.Plan(snap, dirty)
    b.apply(ctx, plan)
}
```

`TakePending` establishes the reconcile-cycle boundary before snapshot
construction. Triggers arriving during snapshot construction remain queued for
the next cycle, so a trigger batch is expanded only against a snapshot built
after that batch was detached from the queue.

The loop is purely infrastructural: the Policy is where all business decisions happen.

#### External System Integration

| External System | When | Trigger Call |
|---|---|---|
| Node Manager | Node crash / scale-out / graceful shutdown | `Trigger(TriggerScope{NodeChanged: true})` |
| DataView Manager | New DataVersion (Flush/Compact) | Observed by the periodic full scan; the current provider interface has no direct change notifier |
| CollectionLoadManager | Load config updated (DDL callback) | `Trigger(TriggerScope{DirtyCollections: [collID]})` |
| ShardViewManager | View becomes Unrecoverable | Node-loss cases are covered by `NodeChanged`; other cases are observed by the periodic full scan |
| Periodic ticker | Timer fires | `Trigger()` (full scan) |

### 2.2 BalancePolicy

Processes a batch of dirty shards against a global snapshot and produces an execution plan. Stateless pure function.

```go
type BalancePolicy interface {
    // Plan classifies each dirty shard and computes assignments for those that
    // need action. Sees the entire snapshot so it can coordinate across shards
    // (e.g., avoid two shards contending for the same target node).
    Plan(snap *BalancerSnapshot, dirty []qviews.ShardID) *BalancePlan
}

// BalancePlan is the complete set of actions to execute for this batch.
type BalancePlan struct {
    // Shards that need a new Preparing view.
    Prepares map[qviews.ShardID]*qviews.QueryViewAtCoordBuilder
    // Shards whose views should be released
    // (desired state absent but current views exist).
    Releases []qviews.ShardID
}
```

`BalancerSnapshot` is the global world view for this batch (see Section 4). It is built once at the start of each reconcile cycle and reused for all dirty shards in the batch.

#### Why a Unified Algorithm Works

| Scenario | Input Difference | Algorithm Behavior |
|---|---|---|
| Initial Load | CurrentStats empty | No stickiness → full placement from scratch |
| Node crash | Crashed node not in Nodes list | Crashed node's segments lose stickiness → redistributed; others stay |
| DataVersion change | DataView has new/removed segments | New segments placed optimally; existing segments sticky |
| Scale-out | New node in Nodes list | New node has low load → attracts segments; stickiness keeps most in place |
| Periodic balance | Node loads shifted | State-aware reuse prevents unnecessary moves; only large imbalances trigger migration |

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
├── ShardEnsurer             ← uses broadcast result vchannels
└── DirtyCollectionNotifier  ← injected balancer trigger callback
```

#### LoadConfigStore

Owns **desired state**: per-collection load config and replica resource-group
constraints. Does not know about ShardID, views, or live node membership —
focused purely on LoadConfig persistence plus a resident immutable snapshot for
Balancer.

```go
// Concrete type, no interface abstraction. Snapshot returns an immutable
// resident view and refreshes lazily when the live version advances.
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

**Copy-On-Write semantics**: Put clones its input before storing, so callers may reuse/mutate their input freely. Snapshot returns pointers into the store's immutable view — callers must call `.Clone()` before any mutation. The store never modifies published snapshots in place; updates advance the live version and the next Snapshot call lazily publishes a new immutable view.

**Write amplification**: Put always writes the full config (no diff). Orphan partitions / replicas (present in previous state but absent from new config) are deleted. This is intentionally simple — dedup / diff optimization can be added later if write volume becomes a concern.

#### ShardViewRegistry

Owns **actual view state**: ShardViewManager lifecycle and view-derived indexes.

```go
type ShardViewRegistry struct { /* ... */ }

func RecoverShardViewRegistry(ctx context.Context, catalog queryview.QueryViewCatalog, syncer syncer.ReliableSyncer) (*ShardViewRegistry, error)
func (r *ShardViewRegistry) Ensure(shardID qviews.ShardID) *ShardViewManager
func (r *ShardViewRegistry) Get(shardID qviews.ShardID) *ShardViewManager
func (r *ShardViewRegistry) Snapshot() *ShardViewSnapshot
```

Maintains live per-shard stats via ShardViewObserver callbacks from each ShardViewManager. The immutable `ShardViewSnapshot` is refreshed lazily when the live version advances.

#### CollectionLoadManager (Facade)

```go
type CollectionLoadManager interface {
    // Facade parses the WAL ack broadcast result, calls LoadConfigStore.Put,
    // ensures shard managers for the vchannels acknowledged by the broadcast,
    // and triggers Balancer. No CollectionShardProvider is needed.
    UpdateLoadConfig(ctx context.Context, result message.BroadcastResultAlterLoadConfigMessageV2) error
    ReleaseCollection(ctx context.Context, msg *messagespb.DropLoadConfigMessageHeader) error
}
```

**DDL callback integration**:

```
// Before (legacy):
WAL ack → LoadCollectionJob → CollectionManager.PutCollection() + ReplicaManager.Spawn()

// After (new):
WAL ack of collection-vchannel AlterLoadConfig broadcast
        → CollectionLoadManager.UpdateLoadConfig(result)
         ├── parse result.Message.Header() → LoadConfig
         ├── LoadConfigStore.Put(fullCfg)  // full write + orphan cleanup
         ├── ShardViewRegistry.Ensure(replicaID, vchannel) for result vchannels
         └── Balancer.Trigger(DirtyCollections: [collID])

WAL ack of collection-vchannel DropLoadConfig broadcast
        → CollectionLoadManager.ReleaseCollection(msg.Header())
         ├── LoadConfigStore.Remove(collID)
         └── Balancer.Trigger(DirtyCollections: [collID])
         // ShardViewRegistry cleanup via reconcile: Phase 1
         // sees "desired absent + current exists" → actionRelease
```

`AlterLoadConfig` and `DropLoadConfig` are broadcast to all vchannels of the collection,
not only to CChannel. Coord still uses the broadcast completion callback to update
`CollectionLoadManager`. StreamingNode does not persist a vchannel-local load config;
QueryView metadata identifies the versioned load info used when the local state machine
acquires resources.

**Release semantics (Option A)**: `ReleaseCollection` immediately removes the LoadConfig and triggers Balancer. Orphan views (view exists but no config) are naturally detected by reconcile Phase 1 and released via `RequestRelease`. No "releasing" state needed. Crash recovery is handled uniformly by reconcile.

**Load status derivation**: `LoadStatus` and `LoadPercentage` are derived from view states:
- **Loaded**: All shards for the collection have an Up view.
- **Loading**: At least one shard has no Up view.
- **LoadPercentage**: `count(shards with Up view) / count(total shards) * 100`.

**Recovery**: On startup: LoadConfigStore recovers from ETCD → ShardViewRegistry recovers from persisted views → Balancer triggers full reconcile.

## 3. Policy Planning

The Policy internally organizes work into three phases, but processes all dirty shards in one batch so decisions across shards are coordinated.

The existing Balancer / BalancePolicy / BalancerSnapshot / BalancePlan
boundaries already provide every input required by the normalized design:
eligible nodes, cross-shard row counts, per-segment `RowNum`, and current
segment-to-node states. This redesign does not add a provider, RPC, protobuf,
or QueryView lifecycle state. It changes only Policy-internal configuration,
steady-state row accounting, node selection, and optional-candidate emission.

```
Plan(snap, dirty)
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

A shared `projectedRows` map starts from the snapshot's cross-shard row counts.
Before rebuilding a shard, the Policy removes that shard's currently accounted
rows from the tracker. The candidate then adds each desired segment exactly
once. Once accepted, the candidate remains in `projectedRows`, so later shards
in the batch see its steady-state effect.

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
the segment decision point. Snapshot/version validation, prepare concurrency,
and migration-row throttling belong to plan execution and may delay work, but
do not decide whether a placement is economically worthwhile.

### 3.4 Algorithm Complexity

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

These bounds exclude snapshot acquisition. After its providers return their
snapshots, `SnapshotBuilder` additionally spends `O(M + P_all)` time and
`O(M)` balancer-owned space to build `BalanceNode` entries and aggregate row
load, where `P_all` is the number of placement-state entries across all shards
in the snapshot. Provider-specific snapshot construction costs are owned by
those providers.

## 4. BalancerSnapshot

The snapshot is the global world view built once per reconcile cycle. All dirty shards in the batch are processed against the same snapshot, ensuring consistent decisions.

### 4.1 Structure

```go
type BalancerSnapshot struct {
    // Provider-owned immutable snapshots composed for this reconcile cycle.
    LoadConfigSnapshot *loadmgr.LoadConfigSnapshot
    ShardViewSnapshot  *coordview.ShardViewSnapshot
    DataViewSnapshot   *DataViewSnapshot
    NodeSnapshot       *NodeSnapshot

    // Per-node info with cross-shard aggregates embedded.
    Nodes      map[int64]*BalanceNode

    // Tunables (production defaults supplied by DefaultBalanceConfig).
    Config     *BalanceConfig
}

// ShardStats is the per-shard placement snapshot returned by ShardViewManager.
type ShardStats struct {
    UpVersion         *qviews.QueryViewVersion // nil if no Up view
    UpLoadInfoVersion uint64                   // zero if no Up view
    PreparingVersion  *qviews.QueryViewVersion // nil if no Preparing/Ready view
    Segments          map[int64]*SegmentStats  // segmentID -> node states
}

type SegmentStats struct {
    SegmentID   int64
    PartitionID int64
    Nodes       map[int64]SegmentState  // nodeID -> merged state
}

type SegmentState int

const (
    SegmentStateUnrecoverable SegmentState = iota
    SegmentStatePreparing
    // SegmentStateReady also covers Down views. Down is SN-only; QueryNodes do
    // not receive Down and their loaded segments remain more reusable than
    // Preparing placements.
    SegmentStateReady
    SegmentStateUp
)

// ShardStats.Segments is a node-level segment-state summary. The same segment
// may appear on multiple nodes while views overlap, but a segment has only one
// state per node. States are derived by merging all currently relevant views:
//
//   Up            <- current Up view assignment
//   Ready         <- Preparing/Ready/Unrecoverable view assignment plus QueryNode ready report,
//                    or Down view assignment because Down is SN-only and QN segments remain loaded
//   Preparing     <- Preparing/Ready view assignment without ready report
//   Unrecoverable <- Unrecoverable view assignment without ready report
//
// When multiple views mention the same (segmentID, nodeID), states are merged
// by priority: Up > Ready > Preparing > Unrecoverable.

// SegmentInfo carries the minimum metadata the Balancer needs per segment.
type SegmentInfo struct {
    SegmentID   int64
    PartitionID int64
    MemSize     int64  // retained for compatibility and diagnostics; not scored
    RowNum      int64  // sole balance load metric
}

// BalanceNode combines identity/health with cross-shard aggregated load.
type BalanceNode struct {
    // Identity & health (Node Manager).
    NodeID         int64
    Alive          bool
    Stopping       bool
    ResourceGroup  string

    // Aggregated across all shards from SegmentInfo.RowNum.
    UpRowCount      int64  // Up segments on this node
    PendingRowCount int64  // Ready / Preparing segments on this node
}

// BalanceConfig contains only policy inputs. Each score is normalized before
// its weight is applied.
type BalanceConfig struct {
    StickinessWeight float64
    NodeLoadWeight   float64
    FanoutWeight     float64

    StickyRowsScale        int64
    TargetRowsPerShardNode int64

    TickerInterval time.Duration
}
```

**Notes**:
- The current policy assumes QueryNodes are homogeneous, so absolute assigned row count is comparable across nodes.
- `RowNum` is the only load signal used by allocation, scoring, shard ordering, stickiness, and fanout-budget calculation. `MemSize` remains in the snapshot only for compatibility and diagnostics.
- `SegmentCount` is not a node-load score. The desired shard's segment count is used only to cap `FanoutBudget` because fanout cannot exceed the number of segments.
- The three weights must be non-negative and at least one must be positive. `StickyRowsScale` and `TargetRowsPerShardNode` must be positive.
- `SegmentCountWeight`, `BaselineSegmentRows`, `BalanceThreshold`, and `CostEfficiencyThreshold` are not part of the normalized design.
- The production defaults in Section 3.2 are calibrated with deterministic unit and end-to-end scenarios; they are not used as hidden normalization constants.
- `ShardStats.Segments`, `SegmentInfo.RowNum`, and the existing node aggregates are sufficient to derive per-shard contributions and reusable-copy state. No new snapshot provider field is required.
- Memory and disk capacity are intentionally not admission-control constraints in this policy. Heterogeneous-node capacity normalization can be added later when a reliable capacity signal is available.

### 4.2 State Source Summary

```
BalancerSnapshot (built once per reconcile cycle)
│
├── LoadConfigSnapshot ← LoadConfigStore
├── ShardViewSnapshot  ← ShardViewRegistry
├── DataViewSnapshot   ← DataView Manager + segment lookup
├── NodeSnapshot       ← Node Manager + Replica Manager
├── Nodes          ← Node Manager + Replica Manager
│                     joined with per-node row counts derived from
│                     ShardViewSnapshot stats × DataViewSnapshot segment RowNum
└── Config         ← runtime-supplied BalanceConfig
                     (DefaultBalanceConfig in production wiring)
```

`RowNum` in segment metadata is the sole balance load metric. A missing or zero
row count contributes zero load. Zero-row segments are placed using
stickiness, fanout, and deterministic tie-breaking; no global segment-count
bonus is added to the node score.

`DeleteApplyStartAfterTimetick` from DataView is passed through to QueryView metadata but is NOT consumed by the allocation algorithm.

### 4.3 Consumption by Phase

| Phase | Snapshot Fields | Purpose |
|---|---|---|
| **Phase 1** | `LoadConfigSnapshot`, `ShardViewSnapshot`, `DataViewSnapshot`, `Nodes[*].Alive` | Classify each dirty shard: must / may-optimize / release / none |
| **Phase 2** | `DataViewSnapshot` shard and segment lookup, `Nodes` filtered by replica, shard segment/node states, normalized-score config | Remove current shard rows, incrementally produce the complete segment → node assignment, update partial rows and opened-node state |
| **Phase 3** | Current assignment and complete candidate assignment | Always emit mandatory work; emit optional work only when assignments differ |

### 4.4 Within-Batch Coordination

The snapshot itself is not mutated during `Plan`. The Policy owns a shared
steady-state row tracker cloned from the snapshot:

```
projectedRows := totalRows(snap.Nodes)
for each candidate in orderedCandidates:
    baseRows := projectedRows - currentRows(candidate.shardID)
    placement := allocate(snap, candidate.shardID, baseRows)

    if mandatory || placement differs from current assignment:
        projectedRows := baseRows + rows(placement)
        emit placement
```

Rejected/no-op optional candidates leave `projectedRows` unchanged. Accepted
candidates replace their old shard contribution instead of being added on top
of it. This gives cross-shard coordination without mutating the snapshot,
rebuilding it per shard, or double-counting the replacement view.

## 5. Event Processing Examples

### 5.1 Node Crash Recovery

```
1. Node Manager detects QN3 crash → Trigger(NodeChanged: true)
2. Balancer loop wakes and detaches the pending full-scan trigger batch
3. Balancer builds snapshot; QN3 is unavailable, and shard stats may already
   reflect Syncer's OnNodeLost transitions
4. Balancer expands the full-scan batch from the completed snapshot → dirty = [A, B, C]
5. policy.Plan(snap, dirty):
   Phase 1: each shard either references unavailable QN3 or is already
            Unrecoverable → all actionMust
   Phase 2: order by size desc; for each shard:
     - QN3 is ineligible → its segments have no reusable location and receive
       neutral stickiness
     - other segments retain stickiness → stay in place
     - the accepted candidate replaces the shard's old contribution in
       projectedRows so the next shard sees the shifted steady-state load
   → plan.Prepares = {A: ..., B: ..., C: ...}
6. Balancer.apply(plan) → AddPreparing for each
```

### 5.2 Load Collection

```
1. Load RPC → broadcast AlterLoadConfigMessage to all collection vchannels → WAL ack
2. DDL callback: CollectionLoadManager.UpdateLoadConfig(result)
   → persist load config + ensure result vchannel ShardViewManagers + Trigger(DirtyCollections: [C1])
3. Balancer loop wakes, snapshot includes new LoadConfig but ShardStats is empty
4. policy.Plan(snap, [shards of C1]):
   Phase 1: desired present, current absent → actionMust for each
   Phase 2: no stickiness; segments sorted largest-first
            NodeLoadScore coordinates rows across shards and FanoutScore avoids
            opening unnecessary QueryNodes inside a small shard
   → plan.Prepares = {each shard: builder}
5. Balancer.apply(plan) → AddPreparing for each
```

### 5.3 Optional Scale-Out

```
1. A new QueryNode joins the replica resource group → Trigger(NodeChanged: true)
2. Stable shards classify as actionMayOptimize
3. For each segment, the current node participates as a normal candidate:
   - moving loses segment-local StickinessScore
   - a lighter new node may gain NodeLoadScore
   - opening the node may lose FanoutScore when the shard is already at budget
4. If no segment selects a different node, no plan is emitted
5. If at least one segment selects a different node, the complete candidate is
   emitted directly; there is no second plan-level gain threshold
```

## 6. Thread Safety

| Component | Concurrency Model |
|---|---|
| Balancer | Single goroutine reconcile loop. `Trigger()` is thread-safe (enqueue only). |
| BalancePolicy | Stateless pure function; receives immutable snapshot. Thread-safe by definition. |
| CollectionLoadManager | Delegates desired-state mutation to LoadConfigStore (RWMutex); shard creation is an injected callback. |
| ShardViewManager | `sync.Mutex` per instance (existing). `Stats()` returns an atomic snapshot. |

## 7. Component Responsibilities

| Component | Responsibility | Holds State |
|---|---|---|
| Balancer | Scheduling framework: work queue + reconcile loop + snapshot builder + plan executor | Work queue only |
| BalancePolicy | Batch planning: classify dirty shards + compute normalized incremental assignments + emit changed optional candidates | None |
| CollectionLoadManager | Load-config lifecycle: DDL broadcast-result handling, desired-state persistence, shard ensure callback from result vchannels, dirty collection notify. | None beyond dependencies |
| ShardViewRegistry | Actual shard view state aggregation and resident ShardViewSnapshot publication. | Registry indexes + snapshot cache |
| ShardViewManager | Per-shard multi-version view management (existing). Exposes `Stats()` for aggregation. | Per-shard state |

## 8. Package Layout

```
internal/views/
├── coord/
│   ├── balancer/
│   │   ├── balancer.go        # Balancer + reconcile loop + plan application
│   │   ├── snapshot_builder.go # SnapshotBuilder composition + row aggregation
│   │   ├── trigger.go         # TriggerScope definition
│   │   ├── snapshot.go        # BalancerSnapshot, BalanceNode, SegmentInfo, BalanceConfig
│   │   ├── policy.go          # BalancePolicy interface + BalancePlan
│   │   ├── policy_impl.go     # Default Plan() implementation + shared steady-state row tracker
│   │   └── scoring.go         # Hard constraints + normalized stickiness/load/fanout scoring
│   ├── loadmgr/
│   │   ├── load_config.go           # LoadConfig, ReplicaAssignment types
│   │   ├── load_config_store.go     # LoadConfigStore
│   │   └── manager.go               # CollectionLoadManager facade
│   └── coordview/
│       ├── shard_view_registry.go # ShardViewRegistry (aggregates ShardViewManagers)
│       ├── shard_view_manager.go  # ShardViewManager (existing, + Stats())
│       ├── state_machine.go       # CoordQueryViewStateMachine (existing)
│       └── syncer/                # ReliableSyncer (existing)
├── worknode/                  # Work-node side QueryView handlers
└── qviews/                    # Shared types (existing)
```

## 9. Future Considerations

1. **Preparing timeout eviction**: Periodic reconcile can detect shards stuck in Preparing beyond a timeout → mark as Unrecoverable to release the slot.
2. **Global optimization passes**: The current Policy uses deterministic per-shard greedy allocation with a shared steady-state row tracker. For batches where many shards need rebalancing simultaneously (e.g., scale-out), a second optimization pass could detect and resolve cross-shard conflicts (two shards both wanting the same lightly-loaded node).
3. **Disk-based scoring**: Add `DiskUsage`/`DiskCapacity` back to `BalanceNode` and a disk-balance soft constraint once mmap / disk-index segments are in scope.
4. **Rate limiting**: Cap concurrent Preparing views across all shards to prevent overwhelming the cluster during large-scale events.
5. **Snapshot incremental rebuild**: Currently snapshots are built fresh per reconcile cycle. For very large clusters, an incremental snapshot that only re-reads changed collections/shards could reduce overhead.

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
