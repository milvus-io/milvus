# Channel Exclusive Mode Design Document

**Related Issues and PRs:**
- Issue: https://github.com/milvus-io/milvus/issues/47500
- Implementation PR: https://github.com/milvus-io/milvus/pull/47505
- Released: Milvus 2.6.0

---

## 1. Background and Motivation

### 1.1 Problem Statement

In Milvus QueryCoord, the traditional load balancing strategy distributes channels and segments across QueryNodes without strict isolation. This approach can lead to several issues:

- **Resource Contention**: Multiple channels competing for resources on the same QueryNode can cause unpredictable performance degradation
- **Load Imbalance**: Channels with different workload characteristics may not be balanced effectively
- **Scalability Challenges**: As the number of channels grows, fine-grained control over channel-to-node mapping becomes difficult
- **Isolation Requirements**: Some use cases require strict isolation between channels for performance guarantees or fault isolation

### 1.2 Goals

**Channel Exclusive Mode** aims to address these challenges by introducing a channel-centric load balancing strategy with the following goals:

1. **Predictable Performance**: Assign each channel exclusively to a dedicated set of QueryNodes
2. **Better Isolation**: Prevent interference between channels by ensuring they run on separate node groups
3. **Flexible Configuration**: Allow administrators to control the degree of exclusivity through configuration parameters
4. **Backward Compatibility**: Gracefully fall back to traditional balancing when exclusivity constraints cannot be met
5. **Dynamic Adaptation**: Automatically initialize exclusive metadata when conditions are met and fall back to score-based balancing when exclusivity cannot be used

---

## 2. Architecture Overview

### 2.1 High-Level Design

Channel Exclusive Mode is implemented as an extension to Milvus QueryCoord's balancing system. The design follows these key principles:

- **Channel-First Approach**: Balance decisions are made at the channel level before considering segment-level balancing
- **Exclusive Node Assignment**: Each channel is assigned to a subset of RW (Read-Write) nodes that handle all operations for that channel
- **Fallback Mechanism**: When exclusivity requirements cannot be met (e.g., insufficient nodes), the system falls back to traditional segment-level balancing
- **Copy-on-Write Pattern**: Replica state modifications use immutable data structures to ensure thread safety

### 2.2 System Components

```
┌─────────────────────────────────────────────────────────────┐
│                   QueryCoord Services                        │
├─────────────────────────────────────────────────────────────┤
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ ReplicaManager                                        │  │
│  │ - Creates replicas with exclusive mode enabled       │  │
│  │ - Persists replica state to etcd                     │  │
│  └──────────────────────────────────────────────────────┘  │
│           │                              ↑                   │
│           │ create/update                │ read              │
│           ↓                              │                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ Replica (Immutable + Mutable)                        │  │
│  │ - Stores ChannelNodeInfos mapping                    │  │
│  │ - Manages exclusive node assignments                 │  │
│  │ - Provides COW semantics for updates                 │  │
│  └──────────────────────────────────────────────────────┘  │
│           ↑                              │                   │
│           │ monitor/update               │ query             │
│           │                              ↓                   │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ ReplicaObserver                                       │  │
│  │ - Monitors replica state periodically                │  │
│  │ - Initializes exclusive metadata dynamically         │  │
│  │ - Cleans up RO nodes when not needed                 │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
│  ┌──────────────────────────────────────────────────────┐  │
│  │ ChannelLevelScoreBalancer                             │  │
│  │ - Implements channel-aware load balancing            │  │
│  │ - Generates channel/segment movement plans           │  │
│  │ - Falls back to ScoreBasedBalancer when needed       │  │
│  └──────────────────────────────────────────────────────┘  │
│                                                              │
└─────────────────────────────────────────────────────────────┘
```

---

## 3. Core Data Structures

### 3.1 ChannelNodeInfo (Proto)

**File**: `pkg/proto/query_coord.proto`

```protobuf
message ChannelNodeInfo {
    repeated int64 rw_nodes = 6;  // RW nodes exclusively assigned to this channel
}

message Replica {
    int64 ID = 1;
    int64 collectionID = 2;
    repeated int64 nodes = 3;              // All RW nodes in this replica
    repeated int64 ro_nodes = 5;           // Read-only nodes
    string resource_group = 4;
    map<string, ChannelNodeInfo> channel_node_infos = 6;  // Channel → Node mapping
    repeated int64 rw_sq_nodes = 7;        // Streaming query nodes (RW)
    repeated int64 ro_sq_nodes = 8;        // Streaming query nodes (RO)
}
```

**Design Rationale**:
- **Map Structure**: Using a map allows O(1) lookup for a channel's assigned nodes
- **Separate RW/RO Tracking**: Distinguishes between read-write and read-only nodes for different load patterns
- **Streaming Support**: Includes separate fields for streaming query nodes to support future streaming workloads

### 3.2 Replica (Immutable)

**File**: `internal/querycoordv2/meta/replica.go`

```go
type Replica struct {
    replicaPB     *querypb.Replica       // Protobuf representation with ChannelNodeInfos
    rwNodes       typeutil.UniqueSet     // RW nodes (non-streaming)
    roNodes       typeutil.UniqueSet     // RO nodes (non-streaming)
    rwSQNodes     typeutil.UniqueSet     // RW streaming query nodes
    roSQNodes     typeutil.UniqueSet     // RO streaming query nodes
    loadPriority  commonpb.LoadPriority
}
```

**Key Methods**:
- `GetChannelRWNodes(channelName string) []int64`: Returns the exclusive RW nodes for a channel
- `IsChannelExclusiveModeEnabled() bool`: Checks if ChannelNodeInfos are initialized
- `CopyForWrite() *mutableReplica`: Creates a mutable copy for modifications

**Design Rationale**:
- **Immutability**: Prevents concurrent modification bugs and enables lock-free reads
- **UniqueSet**: Ensures no duplicate nodes in the replica
- **COW Pattern**: Modifications create new instances rather than mutating in-place

### 3.3 mutableReplica (Copy-on-Write)

```go
type mutableReplica struct {
    *Replica
    exclusiveRWNodeToChannel map[int64]string  // Reverse mapping: Node → Channel
}
```

**Key Methods**:
- `TryEnableChannelExclusiveMode(channelNames ...string)`: Initializes channel exclusive mode
- `tryBalanceNodeForChannel()`: Rebalances node assignments across channels
- `DisableChannelExclusiveMode()`: Clears ChannelNodeInfos
- `IntoReplica() *Replica`: Converts mutable copy back to immutable replica

**Design Rationale**:
- **Reverse Mapping**: `exclusiveRWNodeToChannel` enables O(1) checks for node availability
- **Transient State**: Reverse mapping is only maintained during mutations, not persisted
- **Explicit Conversion**: `IntoReplica()` enforces intentional conversion back to immutable state

---

## 4. Channel Exclusive Mode Lifecycle

### 4.1 Activation Conditions

Channel exclusive mode is activated when **all** of the following conditions are met:

1. **Balancer Configuration**: `queryCoord.balancer` is set to `"ChannelLevelScoreBalancer"`
2. **Sufficient Nodes**: `replica.RWNodesCount() >= len(channels) * channelExclusiveNodeFactor`
   - `channelExclusiveNodeFactor` defaults to `3` (at least 3 RW QueryNodes per channel)
3. **Channels Registered**: All collection channels are known to the replica

**Check Logic** (`internal/querycoordv2/meta/replica.go`):

```go
func (replica *mutableReplica) shouldEnableChannelExclusiveMode(channelInfos map[string]*querypb.ChannelNodeInfo) bool {
    balancePolicy := paramtable.Get().QueryCoordCfg.Balancer.GetValue()
    channelExclusiveFactor := paramtable.Get().QueryCoordCfg.ChannelExclusiveNodeFactor.GetAsInt()
    return balancePolicy == ChannelLevelScoreBalancerName &&
        replica.RWNodesCount() >= len(channelInfos)*channelExclusiveFactor
}
```

### 4.2 Initialization Flow

**Scenario 1: New Collection Load**

```
User.LoadCollection(collection, replicaNum=2)
    ↓
QueryCoord.LoadCollection()
    ↓
ReplicaManager.Spawn(collection, replicaNum, channels)
    ├─ Check: Is ChannelLevelScoreBalancer enabled?
    ├─ If YES:
    │  ├─ Create Replica with default nodes
    │  ├─ mutableReplica.TryEnableChannelExclusiveMode(channels...)
    │  │  ├─ Initialize ChannelNodeInfos for all channels (empty node lists)
    │  │  └─ Call tryBalanceNodeForChannel() to assign nodes
    │  └─ Persist replica with ChannelNodeInfos
    └─ If NO: Create replica without ChannelNodeInfos
```

**Scenario 2: Dynamic Enablement**

```
ReplicaObserver.checkNodesInReplica() [node-change event or periodic timeout]
    ↓
For each replica:
    ├─ Check: Is ChannelLevelScoreBalancer enabled?
    ├─ Check: Is exclusive mode already enabled?
    ├─ If should enable but not yet enabled:
    │  ├─ Load all collection channels from TargetManager
    │  ├─ mutableReplica.TryEnableChannelExclusiveMode(channels...)
    │  └─ Persist updated replica
    └─ Continue monitoring
```

### 4.3 Node Assignment Algorithm

**Location**: `internal/querycoordv2/meta/replica.go`

#### 4.3.1 Calculate Optimal Assignments

**Goal**: Distribute nodes evenly across channels

```go
func calculateOptimalAssignments(totalNodes, channelCount int) map[string]int {
    baseNodes := totalNodes / channelCount           // Base allocation per channel
    extraNodes := totalNodes % channelCount          // Remaining nodes to distribute

    assignments := make(map[string]int)
    for i, channelName := range sortedChannels {
        assignments[channelName] = baseNodes
        if i < extraNodes {
            assignments[channelName]++  // Distribute extra nodes to first few channels
        }
    }
    return assignments
}
```

**Example**:
- 7 nodes, 3 channels → `[3, 2, 2]` (first channel gets extra node)
- 6 nodes, 3 channels → `[2, 2, 2]` (even distribution)

#### 4.3.2 Rebalance Channel Nodes

**Two-Phase Process**:

**Phase 1: Release Excess Nodes**

```go
func releaseExcessNodes(targetAssignments map[string]int) {
    for _, channelName := range sortedChannels {
        currentNodes := r.GetChannelRWNodes(channelName)
        targetCount := targetAssignments[channelName]

        if len(currentNodes) > targetCount {
            // Keep first targetCount nodes, release the rest
            nodesToKeep := currentNodes[:targetCount]
            nodesToRelease := currentNodes[targetCount:]

            for _, nodeID := range nodesToRelease {
                delete(r.exclusiveRWNodeToChannel, nodeID)
            }

            r.replicaPB.ChannelNodeInfos[channelName].RwNodes = nodesToKeep
        }
    }
}
```

**Phase 2: Allocate Nodes to Under-Allocated Channels**

```go
func allocateInsufficientNodes(targetAssignments map[string]int) {
    availableNodes := r.getAvailableNodes()  // Nodes not exclusively assigned

    for _, channelName := range sortedChannels {
        currentNodes := r.GetChannelRWNodes(channelName)
        targetCount := targetAssignments[channelName]

        if len(currentNodes) < targetCount {
            neededCount := targetCount - len(currentNodes)
            selectedNodes := allocateNodesFromPool(availableNodes, neededCount)

            for _, nodeID := range selectedNodes {
                r.replicaPB.ChannelNodeInfos[channelName].RwNodes =
                    append(r.replicaPB.ChannelNodeInfos[channelName].RwNodes, nodeID)
                r.exclusiveRWNodeToChannel[nodeID] = channelName
            }

            // Remove allocated nodes from available pool
            availableNodes = removeNodes(availableNodes, selectedNodes)
        }
    }
}
```

**Allocation Order**: Channels are sorted by current node count in descending order. Channels with more current nodes are considered first when assigning the remainder and when releasing excess nodes; newly allocated nodes come from the unassigned RW node pool.

---

## 5. ChannelLevelScoreBalancer

### 5.1 Design Philosophy

The `ChannelLevelScoreBalancer` extends the traditional `ScoreBasedBalancer` with channel-awareness:

- **Composition Over Inheritance**: Embeds `ScoreBasedBalancer` and delegates to it when exclusivity is not feasible
- **Graceful Degradation**: Falls back to segment-level balancing when channel exclusive mode is disabled
- **Channel Priority**: Balances channels first, then segments within each channel's node group

**File**: `internal/querycoordv2/balance/channel_level_score_balancer.go`

```go
type ChannelLevelScoreBalancer struct {
    *ScoreBasedBalancer  // Embedded for fallback behavior
    targetMgr meta.TargetManagerInterface
}
```

### 5.2 BalanceReplica Logic

**High-Level Flow** (`channel_level_score_balancer.go:69-160`):

```
BalanceReplica(ctx, replica)
    ↓
1. Check Streaming Service
    ├─ If enabled: Call ScoreBasedBalancer.balanceChannels()
    └─ Return early if plans generated
    ↓
2. Check Exclusive Mode Eligibility
    ├─ For each channel:
    │  └─ If GetChannelRWNodes(channel) is empty → exclusive mode disabled
    └─ If disabled: Delegate to ScoreBasedBalancer.BalanceReplica() → Return
    ↓
3. Channel-Aware Balancing (Exclusive Mode Enabled)
    For each channel:
        ├─ Get channel's exclusive RW nodes
        ├─ Identify outbound nodes (nodes no longer in RW node list)
        ├─ If outbound nodes exist:
        │  ├─ genChannelPlanForOutboundNodes()
        │  └─ genSegmentPlanForOutboundNodes()
        └─ If no outbound nodes:
           ├─ If AutoBalanceChannel enabled:
           │  └─ genChannelPlan() - balance channel distribution
           └─ Else:
              └─ genSegmentPlan() - balance segments within channel nodes
    ↓
4. Return Plans
    └─ (segmentPlans, channelPlans)
```

### 5.3 Balancing Strategies

#### 5.3.1 Channel Balancing (`genChannelPlan`)

**Goal**: Distribute channels evenly across online nodes

**Location**: `channel_level_score_balancer.go:272-312`

**Algorithm**:
```
1. Calculate target: avgChannelsPerNode = totalChannels / onlineNodeCount
2. For each online node:
   - If node.channelCount > avgChannelsPerNode + 1:
     - Mark as sourceNode (overloaded)
   - If node.channelCount < avgChannelsPerNode:
     - Mark as targetNode (under-loaded)
3. For each sourceNode:
   - Select channels to move (excess channels)
   - Assign to least-loaded targetNode
   - Create ChannelPlan(channel, sourceNode → targetNode)
```

**Example**:
- 6 channels, 3 nodes: Target = 2 channels/node
- Current: `[4, 1, 1]` → Move 2 channels from node 0 → nodes 1 and 2
- Result: `[2, 2, 2]`

#### 5.3.2 Segment Balancing (`genSegmentPlan`)

**Goal**: Balance segment load within a channel's exclusive node group

**Location**: `channel_level_score_balancer.go:200-270`

**Algorithm**:
```
1. Get channel's exclusive RW nodes
2. Convert nodes to NodeItems with scores (resource-based)
3. Identify source nodes (high score) and target nodes (low score)
4. For each segment on source node:
   - Skip redundant segments (already on target)
   - Create SegmentPlan(segment, sourceNode → targetNode)
   - Update node scores
5. Return plans
```

**Node Score Calculation** (inherited from `ScoreBasedBalancer`):
```
score = α * CPU% + β * Memory% + γ * DelegatorScore
```

#### 5.3.3 Outbound Node Handling

**Outbound Node**: A node that was previously assigned to a channel but is no longer in the channel's RW node list (due to node removal or rebalancing)

**genChannelPlanForOutboundNodes()** (`channel_level_score_balancer.go:162-176`)

```go
func (b *ChannelLevelScoreBalancer) genChannelPlanForOutboundNodes(
    ctx context.Context,
    replica *meta.Replica,
    channelName string,
    channelRwNodes []int64,
    channelPlans []ChannelAssignPlan,
) []ChannelAssignPlan {
    // Find nodes with this channel that are not in channelRwNodes
    currentAssignment := b.dist.ChannelDistManager.GetByCollectionAndFilter(...)

    for _, view := range currentAssignment {
        if !lo.Contains(channelRwNodes, view.Node) {
            // Node has channel but shouldn't → Move away
            targetNode := selectLeastLoadedNode(channelRwNodes)
            plan := ChannelAssignPlan{
                Channel: view.Channel,
                From:    view.Node,
                To:      targetNode,
                Replica: replica,
            }
            channelPlans = append(channelPlans, plan)
        }
    }
    return channelPlans
}
```

**genSegmentPlanForOutboundNodes()** (`channel_level_score_balancer.go:178-195`)

Similar logic but for segments:
```
1. Find segments on outbound nodes
2. Move to nodes in channelRwNodes
3. Create SegmentPlan for each segment
```

---

## 6. Configuration Parameters

### 6.1 Key Parameters

**File**: `pkg/util/paramtable/component_param.go`

| Parameter | Config Key | Default | Version | Description |
|-----------|------------|---------|---------|-------------|
| `Balancer` | `queryCoord.balancer` | `"ChannelLevelScoreBalancer"` | 2.4+ | Balancer type selection |
| `ChannelExclusiveNodeFactor` | `queryCoord.channelExclusiveNodeFactor` | `"3"` | 2.4.2+ | Minimum RW QueryNodes per channel to enable exclusive mode |
| `AutoBalanceChannel` | `queryCoord.autoBalanceChannel` | `true` | 2.3.4 | Whether to auto-rebalance channels (inherited from parent) |

### 6.2 Configuration Examples

#### Example 1: Default (3 RW QueryNodes per channel)

```yaml
queryCoord:
  balancer: ChannelLevelScoreBalancer
  channelExclusiveNodeFactor: 3
```

**Behavior**:
- Collection with 4 channels + 12 RW nodes → Exclusive mode enabled (3 RW QueryNodes per channel)
- Collection with 4 channels + 11 RW nodes → Exclusive mode disabled (insufficient nodes)

#### Example 2: Lower Threshold (2 RW QueryNodes per channel)

```yaml
queryCoord:
  balancer: ChannelLevelScoreBalancer
  channelExclusiveNodeFactor: 2
```

**Behavior**:
- Collection with 4 channels + 8 RW nodes → Exclusive mode enabled (2 RW QueryNodes per channel)
- Collection with 4 channels + 7 RW nodes → Exclusive mode disabled (insufficient nodes)

#### Example 3: Disable Exclusive Mode

```yaml
queryCoord:
  balancer: ScoreBasedBalancer  # Use traditional balancer
```

**Behavior**:
- All replicas use segment-level balancing regardless of node count

---

## 7. Complete Data Flow Example

### 7.1 Collection Load with Exclusive Mode

```
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: User loads collection                                   │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  User: LoadCollection(                                          │
│      collection = "product_catalog",                            │
│      replicaNum = 2,                                            │
│      channels = ["channel_0", "channel_1", "channel_2"]         │
│  )                                                               │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 2: ReplicaManager.Spawn creates replicas                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Config: queryCoord.balancer = "ChannelLevelScoreBalancer"     │
│  Config: queryCoord.channelExclusiveNodeFactor = 3             │
│                                                                  │
│  For replica 1:                                                 │
│    ├─ Assign nodes: [1, 2, 3, ..., 9]                          │
│    ├─ Check: 9 nodes >= 3 channels * 3 → Eligible              │
│    ├─ TryEnableChannelExclusiveMode(channels...)               │
│    │  ├─ Initialize ChannelNodeInfos:                           │
│    │  │  "channel_0": {rw_nodes: []}                           │
│    │  │  "channel_1": {rw_nodes: []}                           │
│    │  │  "channel_2": {rw_nodes: []}                           │
│    │  └─ Call tryBalanceNodeForChannel()                       │
│    │     ├─ Calculate assignments: [3, 3, 3]                   │
│    │     │  (9 nodes / 3 channels = 3 base + 0 extra)          │
│    │     └─ Assign nodes:                                       │
│    │        "channel_0": {rw_nodes: [1, 2, 3]}                 │
│    │        "channel_1": {rw_nodes: [4, 5, 6]}                 │
│    │        "channel_2": {rw_nodes: [7, 8, 9]}                 │
│    └─ Persist replica to etcd                                   │
│                                                                  │
│  For replica 2: (same logic with different nodes)              │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 3: QueryCoord balancer runs (periodic task)               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ChannelLevelScoreBalancer.BalanceReplica(replica1)             │
│    ↓                                                             │
│    Check: Is exclusive mode enabled?                            │
│      → YES (ChannelNodeInfos exist)                             │
│    ↓                                                             │
│    For "channel_0":                                             │
│      ├─ RW nodes: [1, 2, 3]                                     │
│      ├─ Check outbound nodes: None                              │
│      └─ genSegmentPlan(segments, nodes=[1,2,3])                 │
│         → Move segments to balance load on nodes 1, 2, 3        │
│    ↓                                                             │
│    For "channel_1":                                             │
│      ├─ RW nodes: [4, 5, 6]                                     │
│      ├─ Check outbound nodes: None                              │
│      └─ genSegmentPlan(segments, nodes=[4,5,6])                 │
│    ↓                                                             │
│    For "channel_2":                                             │
│      ├─ RW nodes: [7, 8, 9]                                     │
│      ├─ Check outbound nodes: None                              │
│      └─ genSegmentPlan(segments, nodes=[7,8,9])                 │
│    ↓                                                             │
│    Return plans: (segmentPlans, channelPlans)                   │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 4: Scheduler executes plans                                │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  For each SegmentPlan:                                          │
│    └─ Move segment from source node → target node              │
│                                                                  │
│  For each ChannelPlan:                                          │
│    └─ Reassign channel from source node → target node          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 5: ReplicaObserver monitors state (continuous)            │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  On node-change event or checkNodeInReplicaInterval timeout:    │
│    ├─ Check if exclusive metadata should be initialized         │
│    │  → Already initialized, no action                          │
│    ├─ Check RO nodes for cleanup                                │
│    │  → None present                                            │
│    └─ Continue monitoring                                       │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

### 7.2 Node Removal Scenario

```
┌─────────────────────────────────────────────────────────────────┐
│ Initial State:                                                   │
│   channel_0: [1, 2, 3]                                         │
│   channel_1: [4, 5, 6]                                         │
│   channel_2: [7, 8, 9]                                         │
│                                                                  │
│ Event: Node 2 goes offline                                      │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 1: ReplicaManager detects node failure                    │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  RemoveNode(replicaID=1, nodeID=2)                              │
│    ├─ Remove node 2 from replica.rwNodes                        │
│    ├─ Trigger tryBalanceNodeForChannel()                        │
│    │  ├─ Check: 8 nodes < 3 channels * 3 → Not eligible        │
│    │  └─ Disable channel exclusive node assignments             │
│    │     channel_0: []                                          │
│    │     channel_1: []                                          │
│    │     channel_2: []                                          │
│    └─ Persist updated replica                                    │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 2: ChannelLevelScoreBalancer.BalanceReplica               │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  ChannelLevelScoreBalancer.BalanceReplica(replica1)             │
│    ├─ Sees at least one channel with no RW nodes                │
│    └─ Delegates to ScoreBasedBalancer.BalanceReplica()          │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────────┐
│ Step 3: Scheduler executes movement plans                      │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  Execute ScoreBasedBalancer plans                               │
│                                                                  │
│  Final State:                                                    │
│    ChannelNodeInfos remain present but RW node lists are empty  │
│    Segment and channel movement follows ScoreBasedBalancer      │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 8. Migration and Rollback

> **⚠️ WARNING: Performance Impact**
>
> Switching channel exclusive mode can trigger channel and segment rebalancing. The actual impact depends on collection size, segment size, index size, QueryNode capacity, and the number of channels that need to move. Operators should expect elevated CPU, memory, disk I/O, network traffic, and query latency during the transition, but this document does not provide a fixed percentage or duration guarantee.
>
> **Recommendation**: Perform during low-traffic maintenance windows, validate the impact in staging, and monitor cluster resources closely.

### 8.1 Enabling Channel Exclusive Mode

**For Existing Clusters**:

1. **Check Prerequisites**:
   ```bash
   # Ensure sufficient nodes
   # Required: totalNodes >= channels * channelExclusiveNodeFactor
   ```

2. **Update Configuration** (Runtime Configuration Change):
   ```yaml
   queryCoord:
     balancer: ChannelLevelScoreBalancer
     channelExclusiveNodeFactor: 3  # Default safety threshold
   ```

3. **Wait for Automatic Enablement**:
   - **No restart required**: The `Balancer` parameter is runtime-refreshable (`refreshable:"true"`)
   - **ReplicaObserver** checks replicas when QueryNode membership changes or when `queryCoord.checkNodeInReplicaInterval` times out
   - The default `queryCoord.checkNodeInReplicaInterval` is 60 seconds, so metadata initialization is not guaranteed to happen within 1 second
   - Channel and segment movement still depends on subsequent balance planning and task execution

4. **Monitor Transition**:
   ```bash
   # Check replica state
   # ReplicaObserver will enable exclusive metadata on its next node-change event
   # or checkNodeInReplicaInterval timeout

   # Check logs
   kubectl logs -f milvus-querycoord | grep "channel exclusive mode"
   ```

**How It Works**:
- `ReplicaObserver.checkNodesInReplica()` runs after QueryNode membership changes or after `queryCoord.checkNodeInReplicaInterval` times out
- Dynamically reads `queryCoord.balancer` configuration
- If balancer is `ChannelLevelScoreBalancer` and replica doesn't have exclusive mode:
  - Calls `mutableReplica.TryEnableChannelExclusiveMode()`
  - Persists updated replica state to etcd

**Expected Behavior**:
- No downtime: Existing queries continue to work
- No restart needed: Configuration changes are applied automatically
- Gradual transition: Channels are reassigned over time based on balancing cycles
- Automatic fallback: If node count drops below threshold, channel-level balancing falls back to `ScoreBasedBalancer`

### 8.2 Disabling Channel Exclusive Mode

**Rollback Steps**:

1. **Update Configuration** (Runtime Configuration Change):
   ```yaml
   queryCoord:
     balancer: ScoreBasedBalancer  # Switch to traditional balancer
   ```

2. **Wait for ScoreBasedBalancer to take over**:
   - **No restart required**: Configuration change is applied automatically
   - New balancer instances use `ScoreBasedBalancer` after the refreshed configuration is observed
   - Existing `ChannelNodeInfos` metadata is not synchronously deleted by `ReplicaObserver`

3. **Verify Rollback**:
   ```bash
   # Check that queryCoord.balancer is ScoreBasedBalancer
   # ChannelNodeInfos may remain in replica metadata, but ScoreBasedBalancer ignores them
   ```

**How It Works**:
- The balancer factory selects `ScoreBasedBalancer` when `queryCoord.balancer` is set to `ScoreBasedBalancer`
- `ReplicaObserver` does not have a synchronous disable branch that clears all `ChannelNodeInfos`
- `tryBalanceNodeForChannel()` checks `shouldEnableChannelExclusiveMode()` when it is invoked by replica update paths:
  - Returns `false` if balancer is not `ChannelLevelScoreBalancer`
  - Returns `false` if node count is insufficient
  - Calls `DisableChannelExclusiveMode()` to reset per-channel RW node lists

**Data Consistency**:
- No data loss: `ChannelNodeInfos` only records balancing metadata
- No segment movement required: Existing segment distribution remains valid
- No restart needed: `ScoreBasedBalancer` takes over balancing responsibility after the refreshed configuration is observed

### 8.3 Rolling Upgrade Scenario

When upgrading from a version **without** channel exclusive mode to a version **with** it enabled by default, the upgrade process involves special considerations.

**Upgrade Process**:

1. **Rolling Upgrade Starts**:
   - QueryNodes are upgraded one by one
   - Old QueryNodes gradually become RO (Read-Only, stopping state)
   - New QueryNodes start as RW (Read-Write, active state)

2. **StoppingBalancer Takes Over**:
   - **Purpose**: Rapidly evacuate data from RO nodes to maintain service availability
   - **Behavior**: Moves channels and segments from RO nodes to **any available RW nodes**
   - **Trade-off**: Temporarily breaks channel exclusive mode distribution for speed

3. **Upgrade Completes**:
   - All QueryNodes are now running the new version
   - ChannelLevelScoreBalancer resumes normal operation
   - Automatically restores channel exclusive mode distribution

**What Happens to Channel Exclusive Mode**:

```
Initial State (before upgrade):
  Collection A loaded, channel exclusive mode not enabled
  Channels distributed across nodes without isolation

During Upgrade (node-by-node):
  Step 1: Node 1 becomes RO
    └─ StoppingBalancer: Move channels from node 1 → any RW node (2,3,4,5)
       ❌ May assign channel_0 to node 5 (not in exclusive list)

  Step 2: Node 2 becomes RO
    └─ StoppingBalancer: Move channels from node 2 → any RW node (3,4,5,6)
       ❌ Distribution further deviates from exclusive mode

  ... (continues for all nodes)

After Upgrade (all nodes upgraded):
  Step 1: ReplicaObserver detects replicas without ChannelNodeInfos
    └─ Calls TryEnableChannelExclusiveMode() for all replicas
       ✅ Initializes ChannelNodeInfos for each channel

  Step 2: ChannelLevelScoreBalancer runs
    └─ Detects channels on "outbound nodes" (nodes not in ChannelNodeInfos)
    └─ Generates plans to move channels to correct exclusive nodes
    └─ Gradually restores exclusive mode distribution
```

**Timeline and Resource Impact**:

| Phase | Duration | Balancer | Resource Impact | Channel Distribution |
|-------|----------|----------|-----------------|---------------------|
| **Pre-Upgrade** | Stable | ScoreBasedBalancer | Normal | Non-exclusive, evenly distributed |
| **During Upgrade** | Depends on cluster size | StoppingBalancer | Potentially high | May break exclusive mode |
| **Post-Upgrade (Recovery)** | Depends on cluster size | ChannelLevelScoreBalancer | Potentially high | Gradually restoring exclusive mode |
| **Stable** | Ongoing | ChannelLevelScoreBalancer | Normal | Exclusive placement restored after balancing |

**Important Notes**:

> ⚠️ **Expected Behavior During Upgrade**:
> - Channel exclusive mode distribution **will be temporarily violated**
> - This is **by design** - StoppingBalancer prioritizes service availability over isolation
> - Exclusive placement should be restored by normal balancing after upgrade completes, assuming enough RW nodes are available
> - Do NOT manually intervene during the upgrade process

> ⚠️ **Resource Planning**:
> - Upgrade causes **two waves of rebalancing**:
>   1. During upgrade (StoppingBalancer evacuation)
>   2. After upgrade (ChannelLevelScoreBalancer restoration)
> - Total impact time depends on cluster size and the amount of data that must move
> - Plan for **sustained high CPU/memory/I/O** during this period

---

## 9. Test Plan and Verification

This section outlines the testing strategy for Channel Exclusive Mode, covering checked-in unit tests, integration tests, system tests, and production verification.

### 9.1 Unit Tests

**Replica Logic Tests** (`internal/querycoordv2/meta/replica_test.go`):

| Test Case | Coverage | Status |
|-----------|----------|--------|
| `TestChannelExclusiveMode` | Basic channel-exclusive metadata behavior | Checked in |
| `TestTryBalanceNodeForChannel*` | Empty, disabled, insufficient-node, balanced, unbalanced, and extra-node cases | Checked in |
| `TestShouldEnableChannelExclusiveMode` | Activation condition checks | Checked in |
| `TestClearChannelNodeInfos` | Resets per-channel RW node lists | Checked in |
| `TestGetSortedChannelsByNodeCount` | Channel sort order | Checked in |
| `TestCalculateOptimalAssignments` | Optimal assignment calculation, including remainder distribution | Checked in |

**ChannelLevelScoreBalancer Tests** (`internal/querycoordv2/balance/channel_level_score_balancer_test.go`):

| Test Case | Coverage | Status |
|-----------|----------|--------|
| `TestExclusiveChannelBalance_ChannelOutBound` | Moves channels away from nodes outside the channel RW node list | Checked in |
| `TestExclusiveChannelBalance_SegmentOutbound` | Moves segments away from nodes outside the channel RW node list | Checked in |
| `TestExclusiveChannelBalance_SegmentUnbalance` | Balances segments within a channel's RW node list | Checked in |
| Existing score-based balance tests | Non-exclusive and fallback score-based behavior | Checked in |

**ReplicaObserver Tests** (`internal/querycoordv2/observers/replica_observer_test.go`):

| Test Case | Coverage | Status |
|-----------|----------|--------|
| `TestCheckNodesInReplica` | Replica recovery, RO node cleanup, and channel-exclusive metadata registration during observer checks | Checked in |
| `TestCheckSQnodesInReplica` | Streaming QueryNode replica cleanup | Checked in |

### 9.2 Integration Tests

**End-to-End Scenarios**:

| Test File | Coverage | Status |
|-----------|----------|--------|
| `tests/integration/balance/channel_exclusive_balance_test.go` | Go integration scenarios for channel-exclusive balance | Checked in but skipped until https://github.com/milvus-io/milvus/issues/42966 is fixed |
| `tests/python_client/resource_group/test_channel_exclusive_balance.py` | Resource-group channel-exclusive balance scenarios, including QueryNode scale-up/down, `k` changes, single-channel collections, and search performance | Checked in |

### 9.3 System Tests

System tests should cover the following scenarios before changing the default in a production branch:

| Test | Configuration | Metrics to Monitor | Status |
|------|--------------|--------------------|--------|
| Sustained Query Load | Multiple collections and channels with exclusive mode enabled | QPS, latency, CPU, memory, segment load latency | Recommended |
| Node Failure During Load | Remove QueryNodes while exclusive mode is active | Query success rate, rebalancing time, channel movement count | Recommended |
| Mode Transition Under Load | Switch between `ScoreBasedBalancer` and `ChannelLevelScoreBalancer` | QPS degradation, recovery time, task backlog | Recommended |
| Skewed Channel Load | Collections whose channels have uneven row counts or query rates | Per-node memory, per-channel latency, task movement volume | Recommended |
| Many Small Collections | Many collections with one or few channels each | Global QueryNode utilization and balance plan volume | Recommended |

### 9.4 Production Verification Checklist

**Pre-Deployment:**
- [ ] Verify `queryCoord.balancer` configuration is set correctly
- [ ] Confirm node count meets minimum requirement: `nodes >= channels * channelExclusiveNodeFactor`
- [ ] Review performance impact warnings with operations team
- [ ] Schedule deployment during low-traffic maintenance window
- [ ] Prepare rollback plan (change to `ScoreBasedBalancer`)

**During Deployment:**
- [ ] Monitor QueryCoord logs for exclusive mode enablement messages
- [ ] Verify ChannelNodeInfos are populated via etcd inspection
- [ ] Check CPU and memory usage on QueryNodes
- [ ] Monitor query latency and QPS metrics
- [ ] Verify no errors in balancer logs

**Post-Deployment:**
- [ ] Confirm all collections have exclusive mode enabled (if nodes sufficient)
- [ ] Validate channel-to-node mapping via admin API
- [ ] Run synthetic query workload to verify performance
- [ ] Monitor for 24 hours to detect anomalies
- [ ] Document actual resource impact vs. estimates

**Rolling Upgrade Verification:**
- [ ] Monitor cluster state transitions (RW → RO → RW)
- [ ] Verify StoppingBalancer activates during node RO state
- [ ] Confirm temporary violation of exclusive mode (expected)
- [ ] Validate recovery after all nodes upgraded and balance tasks have completed
- [ ] Measure total upgrade duration and resource impact
- [ ] Check for any data loss or query failures during upgrade

### 9.5 Regression Testing

**Critical Paths to Test:**
1. **Balancing Logic:**
   - Verify segments are balanced only within channel's exclusive nodes
   - Confirm outbound nodes are cleaned up correctly
   - Test edge case: 1 channel, 1 node

2. **State Consistency:**
   - Verify replica state persists correctly to etcd
   - Confirm COW pattern prevents race conditions
   - Test recovery from QueryCoord restart

3. **Fallback Behavior:**
   - Verify graceful fallback when node count drops
   - Confirm `ScoreBasedBalancer` takes over after the refreshed configuration is observed
   - Test mixed collections (some exclusive, some not)

4. **Configuration Changes:**
   - Test runtime config refresh (no restart)
   - Verify detection through QueryNode membership changes or the configured `queryCoord.checkNodeInReplicaInterval`
   - Confirm atomic state transitions

### 9.6 Performance Benchmarks

This design does not include checked-in benchmark results. Before enabling this as a default, collect benchmark data for:

- **Query Performance:** baseline `ScoreBasedBalancer` QPS and latency versus `ChannelLevelScoreBalancer`
- **Transition Cost:** QPS, p99 latency, CPU, memory, disk I/O, and network usage during enable/disable transitions
- **Rebalancing Time:** plan generation time, task execution time, and total time to reach stable channel placement
- **Skew Sensitivity:** collections where channel row counts or query rates are uneven
- **Scale Sensitivity:** many small collections, high channel counts, and collections whose channel count exceeds available RW nodes

---

## 10. Conclusion

Channel Exclusive Mode enhances Milvus's query coordination by introducing channel-level resource isolation. Each channel is assigned to a dedicated set of QueryNodes, preventing resource contention and improving performance predictability.

**Key Advantages**:
- Better resource isolation between channels
- Predictable performance for individual channels
- Reduced blast radius when one channel experiences high load
- Backward compatible with existing deployments
- Automatically initializes exclusive metadata when conditions are met

**Important Considerations**:
- Requires sufficient nodes: `totalNodes >= channels * channelExclusiveNodeFactor`
- Temporary distribution violations during rolling upgrades; verify restoration after balance tasks complete
- Higher resource usage during mode transitions; validate the actual impact under production-like workload
- Runtime-refreshable configuration: no service restart required for enable/disable

**Production Readiness**:
Channel Exclusive Mode should be enabled with adequate node resources, staging validation, and a rollback plan. The design balances isolation benefits with operational simplicity, but production readiness depends on workload shape, channel skew, and the measured cost of rebalancing in the target environment.

---

## 11. References

### Code Files

- `internal/querycoordv2/meta/replica.go` - Core replica logic
- `internal/querycoordv2/meta/replica_manager.go` - Replica lifecycle management
- `internal/querycoordv2/balance/channel_level_score_balancer.go` - Main balancer implementation
- `internal/querycoordv2/observers/replica_observer.go` - State monitoring and enforcement
- `tests/integration/balance/channel_exclusive_balance_test.go` - Integration tests

### Configuration

- `pkg/util/paramtable/component_param.go` - Configuration parameters
- `configs/milvus.yaml` - Default configuration file

### Related Issues and PRs

- **Issue [#47500](https://github.com/milvus-io/milvus/issues/47500)**: Channel Exclusive Mode Feature Request
- **PR [#47505](https://github.com/milvus-io/milvus/pull/47505)**: Initial Implementation - Switch Default Balancer to ChannelLevelScoreBalancer

### Documentation and Design

- **Design Document**: This document (20260204-channel_exclusive_mode.md)
- **Milvus QueryCoord Architecture**: Internal documentation
- **Test Plan**: Section 9 of this document

### References and Related Work

- **Copy-on-Write Pattern**: Immutable data structures for thread-safe concurrent updates
- **ScoreBasedBalancer**: Original load balancing implementation
- **ChannelLevelScoreBalancer**: Extended balancer with channel-awareness
- **ReplicaObserver**: Replica state monitoring driven by QueryNode membership changes and `queryCoord.checkNodeInReplicaInterval`

---

**Document Version**: 1.1 (with Test Plan and Verification)
**Date**: 2026-02-04
**Author**: weiliu1031
**Status**: Ready for Review
**Last Updated**: 2026-02-04 (Added comprehensive test plan and production verification checklist)
