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
│      1. Build BalancerSnapshot (global world view)           │
│      2. Drain dirty shards                                   │
│      3. policy.Plan(snapshot, dirty) → BalancePlan           │
│      4. Apply plan (AddPreparing + ReleaseShardViews)        │
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

- **Level-triggered reconciliation (Kubernetes controller pattern)**: External systems enqueue affected shard IDs via `Trigger(scope)`; the Balancer compares desired vs. current state and converges. Multiple enqueues for the same shard are deduplicated.
- **Unified allocation algorithm**: A single BalancePolicy handles all scenarios. Scenario differences are encoded in input variations (available nodes, current view, data view), not separate algorithms. This prevents thrashing.
- **Batch planning**: Each reconcile cycle builds a global snapshot, drains dirty shards, and asks the Policy for a complete plan. The Policy handles all dirty shards in one call, enabling cross-shard coordination (e.g., avoiding multiple shards targeting the same node).
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
    DirtyShards      []qviews.ShardID
    DirtyCollections []int64
}
```

- `Trigger()` with no scopes triggers a full scan.
- `Trigger(scope)` enqueues only affected shards (expanded from the latest load-config and shard-view snapshots).
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

    snap  := b.buildSnapshot()
    dirty := b.queue.Drain()
    if len(dirty) == 0 {
        continue
    }

    plan := b.policy.Plan(snap, dirty)
    b.apply(ctx, plan)
}
```

The loop is purely infrastructural: the Policy is where all business decisions happen.

#### External System Integration

| External System | When | Trigger Call |
|---|---|---|
| Node Manager | Node crash / scale-out / graceful shutdown | `Trigger(TriggerScope{NodeChanged: true})` |
| DataView Manager | New DataVersion (Flush/Compact) | `Trigger(TriggerScope{DirtyCollections: [collID]})` |
| CollectionLoadManager | Load config updated (DDL callback) | `Trigger(TriggerScope{DirtyCollections: [collID]})` |
| ShardViewManager | View becomes Unrecoverable | `Trigger(TriggerScope{DirtyShards: [shardID]})` |
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
`CollectionLoadManager`; StreamingNode uses each vchannel-targeted WAL message to update
`VChannelMeta.load_config` and prepare or release local latest resources. On one
pchannel, a collection has at most one vchannel, so the SN consumer mutates exactly the
vchannel carried by the message.

**Release semantics (Option A)**: `ReleaseCollection` immediately removes the LoadConfig and triggers Balancer. Orphan views (view exists but no config) are naturally detected by reconcile Phase 1 and released via `RequestRelease`. No "releasing" state needed. Crash recovery is handled uniformly by reconcile.

**Load status derivation**: `LoadStatus` and `LoadPercentage` are derived from view states:
- **Loaded**: All shards for the collection have an Up view.
- **Loading**: At least one shard has no Up view.
- **LoadPercentage**: `count(shards with Up view) / count(total shards) * 100`.

**Recovery**: On startup: LoadConfigStore recovers from ETCD → ShardViewRegistry recovers from persisted views → Balancer triggers full reconcile.

## 3. Policy Planning

The Policy internally organizes work into three phases, but processes all dirty shards in one batch so decisions across shards are coordinated.

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
Phase 2: Order candidates + allocate with shared predicted-load tracker
    │
    │  Mandatory first, then optional.
    │  After each allocation, update predictedLoad so later shards see the impact.
    │
    ▼
Phase 3: For optional candidates, apply cost/benefit threshold
    │
    ▼
Return BalancePlan { Prepares, Releases }
```

### 3.1 Phase 1: Classify

Pure state comparison. For each dirty shard:

| Condition (checked in order) | Action |
|---|---|
| Desired absent, current exists | **Release** |
| Desired present, no current view | **Must** (initial load or post-Unrecoverable) |
| Both absent | **None** |
| Current DataVersion < DataView DataVersion | **Must** (data changed) |
| Current view references unavailable node | **Must** (node lost) |
| Settings changed (partition/field diff) | **Must** |
| Already has a Preparing view in flight | **None** (avoid stacking) |
| None of the above | **MayOptimize** (steady-state balance) |

### 3.2 Phase 2: Ordered Allocation with Shared Load Tracker

Candidates are processed in priority order — mandatory (Must) first, optional (MayOptimize) last. Within each group, larger shards (by total MemSize) are processed first so big allocations claim capacity before smaller ones.

A `predictedLoad` map (cloned from `snap.Nodes`) is the shared tracker. After allocating a shard, its segment assignments update `predictedLoad` so the next shard sees the effect. This is how cross-shard coordination is achieved without explicit global optimization.

**Hard constraints** (any failure excludes the node):

| Constraint | Description |
|---|---|
| Node Health | Must be alive and not stopping |
| Resource Group | Must belong to this replica's resource group |
| Memory Capacity | `predictedLoad[node].UpMemLoad + PendingMemLoad + accumulatedForThisShard` ≤ MemoryCapacity |

**Soft constraints** (weighted score, weights differ by orders of magnitude for strict priority):

| Priority | Constraint | Description |
|---|---|---|
| 1 | **Reuse / Avoidance** | Bonus or penalty based on the segment's current node-level state. `Up > Ready > Preparing > Unrecoverable`: Up has full stickiness, Ready/Preparing have weaker reuse preference, and Unrecoverable is penalized so Balancer prefers another node when possible. All factors are proportional to segment MemSize. |
| 2 | **Memory Balance** | Prefer nodes with lower predicted memory utilization ratio |
| 3 | **Segment Count Balance** | Prefer nodes with fewer total segments (secondary metric) |

**Heterogeneous segment size optimizations**:

| Optimization | Mechanism | Effect |
|---|---|---|
| Size as load metric | `accumulatedLoad += seg.MemSize` | Balance by actual memory volume, not count |
| Largest-first ordering | Sort segments by MemSize descending before assignment | Large segments spread evenly; small segments fill gaps |
| State-aware reuse | `stickinessWeight * stateFactor * seg.MemSize / baseline` | Large reusable segments rarely move; Unrecoverable placements are avoided when another feasible node exists |
| Cost-aware threshold (Phase 3) | `scoreGain / migrationCost > threshold` | Avoids high-cost low-benefit migrations |

### 3.3 Phase 3: Worth the Cost? (optional candidates only)

Mandatory candidates are always added to the plan. Optional candidates (MayOptimize) must pass both gates:

1. **Absolute improvement**: `scoreGain > balanceThreshold`
2. **Cost-efficiency**: `scoreGain / totalSizeOfMovedSegments > costEfficiencyThreshold`

This prevents both meaningless small improvements and improvements that require disproportionate migration.

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

    // Tunables (from paramtable).
    Config     *BalanceConfig
}

// ShardStats is the per-shard placement snapshot returned by ShardViewManager.
type ShardStats struct {
    UpVersion        *qviews.QueryViewVersion  // nil if no Up view
    UpSettings       *viewpb.QueryViewSettings // nil if no Up view
    PreparingVersion *qviews.QueryViewVersion  // nil if no Preparing/Ready view
    Segments         map[int64]*SegmentStats   // segmentID -> node states
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
// All other per-segment attributes (index type, vector dim, compression, etc.)
// are subsumed by MemSize — DataCoord is responsible for folding them into
// its estimate of actual memory footprint.
type SegmentInfo struct {
    SegmentID   int64
    PartitionID int64
    MemSize     int64  // bytes; primary load metric (from DataCoord's estimate)
    RowNum      int64  // row count; used as fallback when MemSize is unavailable
}

// BalanceNode combines identity/health with cross-shard aggregated load.
type BalanceNode struct {
    // Identity & health (Node Manager).
    NodeID         int64
    Alive          bool
    Stopping       bool
    ResourceGroup  string

    // Resource capacity (node registration / config).
    MemoryCapacity int64
    MemoryUsage    int64  // current reported usage (node SyncResponse cache)

    // Aggregated across all shards from Σ SegmentInfo.MemSize.
    UpMemLoad      int64  // Up segments on this node
    PendingMemLoad int64  // Ready / Preparing segments on this node
    SegmentCount   int    // total Up segments (for count-based balance)
}
```

**Notes**:
- Segment-level size: we rely on DataCoord's `MemSize` estimate (bytes). It already folds in index type, vector dim, and compression. `RowNum` is only a fallback.
- `DiskUsage` / `DiskCapacity` removed from `BalanceNode` for now — in-memory segments only. Will be added back when mmap / disk-index allocation is in scope.
- `UpMemLoad` + `PendingMemLoad` vs raw `MemoryUsage`: the former are derived from our placement model (what we expect); the latter is node-reported (what actually is). Balance decisions use the former for predictability; anomaly detection can compare the two.

### 4.2 State Source Summary

```
BalancerSnapshot (built once per reconcile cycle)
│
├── LoadConfigSnapshot ← LoadConfigStore
├── ShardViewSnapshot  ← ShardViewRegistry
├── DataViewSnapshot   ← DataView Manager + segment lookup
├── NodeSnapshot       ← Node Manager + Replica Manager
├── Nodes          ← Node Manager + Replica Manager
│                     joined with per-node segment loads derived from
│                     ShardViewSnapshot stats × DataViewSnapshot segment MemSize
└── Config         ← Paramtable
```

`MemSize` in segment metadata is the **primary load metric**. Without it, all heterogeneous optimizations degrade to count-based. Fallback: `RowNum` directly (segment-count balance), optionally `RowNum * estimated_bytes_per_row` if schema is known.

`DeleteApplyStartAfterTimetick` from DataView is passed through to QueryView metadata but is NOT consumed by the allocation algorithm.

### 4.3 Consumption by Phase

| Phase | Snapshot Fields | Purpose |
|---|---|---|
| **Phase 1** | `LoadConfigSnapshot`, `ShardViewSnapshot`, `DataViewSnapshot`, `Nodes[*].Alive` | Classify each dirty shard: must / may-optimize / release / none |
| **Phase 2** | `DataViewSnapshot` shard and segment lookup, `Nodes` (full fields, filtered by replica), shard stats (stickiness), `Config` | Produce segment → node assignment, update predictedLoad |
| **Phase 3** | Current score (from shard stats), candidate score, segment lookup (MemSize for migration cost), `Config` (thresholds) | Accept or reject optional optimization |

### 4.4 Within-Batch Coordination

The snapshot itself is not mutated during `Plan`. Instead, the Policy maintains a `predictedLoad` tracker (cloned from `snap.Nodes`) that is updated as each shard's allocation is decided:

```
predictedLoad := snap.Nodes.clone()
for each candidate in orderedCandidates:
    builder := allocate(snap, shardID, predictedLoad)
    applyToTracker(predictedLoad, builder)  // next candidate sees this shard's effect
```

This gives cross-shard coordination without requiring the snapshot to be mutable or rebuilt per shard.

## 5. Event Processing Examples

### 5.1 Node Crash Recovery

```
1. Node Manager detects QN3 crash → Trigger(NodeChanged: true)
2. Balancer loop wakes and builds snapshot
3. Meanwhile, Syncer's OnNodeLost marks affected views as Unrecoverable
4. Balancer drains queue and expands dirty nodes by scanning snapshot shard stats → dirty = [A, B, C]
5. policy.Plan(snap, dirty):
   Phase 1: each shard's current view references QN3 → all actionMust
   Phase 2: order by size desc; for each shard:
     - QN3 not in Nodes → its segments lose stickiness → redistributed
     - other segments retain stickiness → stay in place
     - predictedLoad updated so next shard sees the shifted load
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
            predictedLoad ensures large shards don't all land on the same node
   → plan.Prepares = {each shard: builder}
5. Balancer.apply(plan) → AddPreparing for each
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
| BalancePolicy | Batch planning: classify dirty shards + compute assignments + accept/reject threshold | None |
| CollectionLoadManager | Load-config lifecycle: DDL broadcast-result handling, desired-state persistence, shard ensure callback from result vchannels, dirty collection notify. | None beyond dependencies |
| ShardViewRegistry | Actual shard view state aggregation and resident ShardViewSnapshot publication. | Registry indexes + snapshot cache |
| ShardViewManager | Per-shard multi-version view management (existing). Exposes `Stats()` for aggregation. | Per-shard state |

## 8. Package Layout

```
internal/views/
├── coord/
│   ├── balancer/
│   │   ├── balancer.go        # Balancer + main loop + snapshot builder
│   │   ├── trigger.go         # TriggerScope definition
│   │   ├── snapshot.go        # BalancerSnapshot, BalanceNode, SegmentInfo, BalanceConfig
│   │   ├── policy.go          # BalancePolicy interface + BalancePlan
│   │   ├── policy_impl.go     # Default Plan() implementation (three phases)
│   │   └── scoring.go         # Hard constraints + soft constraint scoring
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
2. **Global optimization passes**: The current Policy uses per-shard greedy allocation with a shared predicted-load tracker. For batches where many shards need rebalancing simultaneously (e.g., scale-out), a second optimization pass could detect and resolve cross-shard conflicts (two shards both wanting the same lightly-loaded node).
3. **Disk-based scoring**: Add `DiskUsage`/`DiskCapacity` back to `BalanceNode` and a disk-balance soft constraint once mmap / disk-index segments are in scope.
4. **Rate limiting**: Cap concurrent Preparing views across all shards to prevent overwhelming the cluster during large-scale events.
5. **Snapshot incremental rebuild**: Currently snapshots are built fresh per reconcile cycle. For very large clusters, an incremental snapshot that only re-reads changed collections/shards could reduce overhead.
