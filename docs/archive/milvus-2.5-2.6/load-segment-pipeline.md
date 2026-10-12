# Milvus Load Segment: End-to-End Pipeline

## Table of Contents

- [Overview](#overview)
- [Part 1: Milvus 2.5 Load Segment Pipeline](#part-1-milvus-25-load-segment-pipeline)
  - [1.1 Entry Point (Proxy)](#11-entry-point-proxy)
  - [1.2 QueryCoord Processing](#12-querycoord-processing)
  - [1.3 DataCoord Interaction](#13-datacoord-interaction)
  - [1.4 QueryNode Segment Loading](#14-querynode-segment-loading)
  - [1.5 Progress Tracking and Completion](#15-progress-tracking-and-completion)
  - [1.6 Error Handling and Rollback](#16-error-handling-and-rollback)
  - [1.7 Configuration Parameters (2.5)](#17-configuration-parameters-25)
- [Part 2: Milvus 2.6 Load Segment Pipeline](#part-2-milvus-26-load-segment-pipeline)
  - [2.1 Entry Point (Proxy)](#21-entry-point-proxy)
  - [2.2 QueryCoord Processing (WAL Broadcast Mode)](#22-querycoord-processing-wal-broadcast-mode)
  - [2.3 DataCoord Interaction](#23-datacoord-interaction)
  - [2.4 QueryNode Segment Loading](#24-querynode-segment-loading)
  - [2.5 Progress Tracking and Completion](#25-progress-tracking-and-completion)
  - [2.6 Error Handling and Rollback](#26-error-handling-and-rollback)
  - [2.7 Configuration Parameters (2.6)](#27-configuration-parameters-26)
- [Part 3: All Scenarios That Trigger LoadSegments](#part-3-all-scenarios-that-trigger-loadsegments)
- [Part 4: 2.5 vs 2.6 Key Differences](#part-4-25-vs-26-key-differences)
- [Part 5: Checker Framework and Task Scheduling](#part-5-checker-framework-and-task-scheduling)
- [Part 6: QueryNode State Reporting (Distribution)](#part-6-querynode-state-reporting-distribution)
- [Part 7: ReleaseCollection Flow](#part-7-releasecollection-flow)
- [Appendix A: Configuration Parameter Reference](#appendix-a-configuration-parameter-reference)
- [Appendix B: Key Source Files](#appendix-b-key-source-files)

---

## Overview

Regardless of version (2.5 or 2.6), the Load Segment core pipeline involves the following components:

```plaintext
User SDK
  │
  ▼
┌──────────┐    gRPC     ┌────────────┐   metadata query    ┌────────────┐
│  Proxy   │ ──────────▶ │ QueryCoord │ ◀────────────────── │ DataCoord  │
└──────────┘             └────────────┘                     └────────────┘
                               │                                 │
                               │ LoadSegments RPC                │ Provides Segment
                               ▼                                 │ Binlog/Index info
                         ┌────────────┐                          │
                         │ QueryNode  │ ◀─── download from ──────┘
                         │ (multiple) │      object storage
                         └────────────┘
```

**One-sentence summary:** User issues LoadCollection → Proxy validates and forwards to QueryCoord → QueryCoord fetches Segment metadata from DataCoord and determines assignment strategy → dispatches LoadSegments tasks to QueryNodes → QueryNodes download data/indexes from object storage and load into memory → Collection becomes queryable once all Segments are loaded.

### Core Concepts

| Concept | Meaning | Analogy |
|---------|---------|---------|
| **Segment** | Physical storage unit for data; a Collection contains many Segments | Like a database partition/shard |
| **Growing Segment** | Segment actively receiving writes (in memory) | Active write buffer |
| **Sealed Segment** | Segment sealed from further writes (persisted) | On-disk data file |
| **L0 Segment** | Special Segment storing only delete records (Delta Log) | Delete marker file |
| **Replica** | One copy of a Segment set distributed across a group of QueryNodes | Database read replica |
| **Resource Group** | Logical grouping of QueryNodes; Replicas are created within resource groups | Machine pool / node pool |
| **Shard / Channel** | Logical channel for data writes; each Collection has multiple Shards | Like a Kafka Partition |
| **Shard Leader (Delegator)** | Proxy node for each Shard within each Replica, responsible for routing queries | Query router |
| **Binlog** | Data files for a Segment (stored in object storage) | WAL / data files |
| **Bloom Filter** | Used to quickly determine if a primary key exists in a Segment | Bloom filter |

### Target vs Distribution — Key to Understanding All Load Behavior

```plaintext
┌─────────────────────────────────────────────────────────────────┐
│                      QueryCoord Perspective                     │
│                                                                 │
│  Next Target (desired state)    Current Target (confirmed state)│
│  "these segments should load"   "these segments confirmed ready"│
│  Source: pulled from DataCoord  Source: promoted from Next      │
│                                                                 │
│  ──────── Gap = segments that need loading ────────             │
│                                                                 │
│  Distribution (actual state)                                    │
│  "which segments are on which QueryNodes"                       │
│  Source: QueryNode heartbeat reports                            │
└─────────────────────────────────────────────────────────────────┘

The essence of all load behavior = detect the Gap between Target and Distribution,
then generate tasks to close it.
```

---

# Part 1: Milvus 2.5 Load Segment Pipeline

## 1.1 Entry Point (Proxy)

### Entry Functions

| Operation | File | Function |
|-----------|------|----------|
| LoadCollection | `internal/proxy/impl.go` | `Proxy.LoadCollection()` (line ~816) |
| LoadPartitions | `internal/proxy/impl.go` | `Proxy.LoadPartitions()` (line ~1540) |

### Processing Flow

```plaintext
User calls LoadCollection(collectionName, replicaNumber=1, resourceGroups=["default"])
  │
  ▼
Proxy creates loadCollectionTask, enqueues in DDL task queue (ddQueue)
  │
  ▼
Task Scheduler executes in order: PreExecute() → Execute() → PostExecute()
```

### Task Execution Details

**File:** `internal/proxy/task.go`

**PreExecute():**
- Validates collection name
- Retrieves collectionID via MetaCache

**Execute() (line ~1847):**
```go
// Pseudocode showing core logic
func (t *loadCollectionTask) Execute(ctx context.Context) error {
    // 1. Get collection schema
    schema := globalMetaCache.GetCollectionSchema(collectionName)

    // 2. Validate all vector fields have indexes (load requires indexes)
    ValidateIndexExistenceOnVectorFields(schema)

    // 3. Build fieldID → indexID mapping
    fieldIndexID := map[int64]int64{
        vectorFieldID: indexID,  // each vector field maps to its index
    }

    // 4. Build request and send to QueryCoord
    req := &querypb.LoadCollectionRequest{
        CollectionID:   collectionID,
        ReplicaNumber:  replicaNumber,  // default 1
        ResourceGroups: resourceGroups, // default ["default"]
        FieldIndexID:   fieldIndexID,
        LoadFields:     loadFields,     // list of fields to load
        Schema:         schema,
    }
    return queryCoord.LoadCollection(ctx, req)
}
```

**Key points:**
- `replicaNumber` defaults to 1 — only one data replica
- `resourceGroups` defaults to `["default"]` — the default resource group
- Index must be built before loading, otherwise an error is returned
- `loadFields` can specify partial field loading

## 1.2 QueryCoord Processing

### 1.2.1 Receiving the Request

**File:** `internal/querycoordv2/services.go` → `LoadCollection()` (line ~204)

```plaintext
QueryCoord.LoadCollection(req)
  │
  ├─ If req.Refresh=true → refreshCollection() path
  │
  ├─ Determine replica count and resource groups:
  │   ├─ User specified → use directly
  │   ├─ Not specified → query broker for collection/database-level pre-config
  │   └─ Neither → use cluster-level defaults:
  │       ├─ queryCoord.clusterLevelLoadReplicaNumber = 1
  │       └─ queryCoord.clusterLevelLoadResourceGroups = ["default"]
  │
  ├─ Conflict detection: if collection already loaded with different config → UpdateLoadConfigJob
  │
  └─ Create LoadCollectionJob → submit to jobScheduler → wait for completion
```

### 1.2.2 LoadCollectionJob Execution

**File:** `internal/querycoordv2/job/job_load.go`

This is the core of the Load pipeline. The Job executes in three phases:

#### PreExecute() — Parameter Validation

```go
func (job *LoadCollectionJob) PreExecute() error {
    // Validate replicaNumber > 0 (default 1)
    // Validate resourceGroups non-empty (default ["default"])
    // If collection already loaded, check for config conflicts:
    //   - replicaNumber differs → error "can't change the replica number"
    //   - loadFields differ → error "can't change the load field list"
    //   - resourceGroups differ → error "can't change the resource groups"
}
```

#### Execute() — Core Execution (8 Steps)

```plaintext
Step 1: Get target Partitions
    broker.GetPartitions(collectionID) → get all partitionIDs from RootCoord
    Filter out already-loaded partitions
    │
Step 2: Clean up stale Replicas (for new collection)
    meta.ReplicaManager.RemoveCollection()
    │
Step 3: Get Collection info
    broker.DescribeCollection() → get schema, virtual channel names
    │
Step 4: Create Replicas ★
    utils.SpawnReplicasWithRG() → create replicas in resource group
    Each replica is assigned to specific QueryNode nodes
    │
Step 5: Create Partition metadata
    Each partition gets PartitionLoadInfo:
      Status: LoadStatus_Loading  ← initial state is "loading"
    │
Step 6: Create Collection metadata
    CollectionLoadInfo:
      Status: LoadStatus_Loading
      LoadType: LoadType_LoadCollection
      LoadFields, FieldIndexID, ReplicaNumber, etc.
    │
Step 7: Persist metadata
    meta.CollectionManager.PutCollection(collection, partitions...)
    │
Step 8: Update Next Target ★★★
    targetObserver.UpdateNextTarget(collectionID)
      → pulls segment and channel info from DataCoord
      → tells QueryCoord "which segments this collection needs to load"
    │
Step 9: Register with Collection Observer
    collectionObserver.LoadCollection(collectionID)
      → Observer starts periodically monitoring load progress
```

#### PostExecute() — Rollback on Failure

If Execute() fails, rolls back via `UndoList`:
- Deletes created Collection metadata
- Deletes created Replicas
- Releases updated Target

**File:** `internal/querycoordv2/job/undo.go`

### 1.2.3 Target Manager — "Which Segments Should I Load?"

**File:** `internal/querycoordv2/meta/target_manager.go`

The Target Manager maintains two states:
- **Next Target:** Desired state — "these segments need to be loaded"
- **Current Target:** Current state — "these segments have finished loading"

```plaintext
UpdateCollectionNextTarget(collectionID):
  │
  ├─ Call DataCoord.GetRecoveryInfoV2() (with 10 retries)
  │   Returns:
  │   ├─ VchannelInfo: channel info (checkpoint, dropped/flushed segment lists)
  │   └─ SegmentInfo: segment info (binlog paths, sizes, row counts, etc.)
  │
  ├─ Filter: only keep segments belonging to loaded partitions
  │
  ├─ Extract L0 (delta) segments
  │
  └─ Create CollectionTarget object, store in targetMgr.next
```

### 1.2.4 Segment Assignment — Checker Mechanism

After Target update, the Checker is responsible for generating actual load tasks.

**File:** `internal/querycoordv2/checkers/segment_checker.go`

```plaintext
SegmentChecker (runs every 3s):
  │
  ├─ Iterate all Loading-state collections
  │
  ├─ For each collection:
  │   ├─ Get segment list from next target
  │   ├─ Compare with current state, find unloaded segments
  │   └─ Create SegmentTask (Grow action) for each unloaded segment
  │
  └─ Submit SegmentTasks to TaskScheduler
      │
      ├─ Scheduler sorts by priority:
      │   ├─ High: Channel tasks
      │   ├─ Normal: Segment tasks (load)
      │   └─ Low: Balance tasks
      │
      └─ Execute tasks: send LoadSegments RPC to target QueryNode
```

### 1.2.5 Task Execution

**File:** `internal/querycoordv2/task/task.go`

```go
// Task structure
type SegmentTask struct {
    segmentID    int64
    actions      []Action    // a segment can have multiple actions
    priority     TaskPriority
}

// Action types
type Action struct {
    Node  int64      // target QueryNode
    Type  ActionType // Grow (load) / Reduce (unload) / Update (update)
}
```

When Scheduler executes a SegmentTask:
1. Determines target QueryNode from action.Node()
2. Constructs LoadSegmentsRequest (with segment binlog, index info)
3. Sends RPC to QueryNode
4. Waits for RPC response

## 1.3 DataCoord Interaction

**File:** `internal/querycoordv2/meta/coordinator_broker.go`

QueryCoord interacts with DataCoord via Broker to obtain the following information:

| Method | Returns | Purpose |
|--------|---------|---------|
| `GetRecoveryInfoV2()` | VchannelInfo + SegmentInfo | Target update, get list of segments to load |
| `GetSegmentInfo()` | Detailed SegmentInfo | Get segment binlog paths, sizes, state |
| `GetIndexInfo()` | FieldIndexInfo | Get segment index file paths and parameters |
| `DescribeCollection()` | Schema + VChannel names | Get collection schema info |
| `GetPartitions()` | PartitionID list | Get all partitions under a collection |

**SegmentInfo returned by DataCoord includes:**

```go
type SegmentInfo struct {
    SegmentID   int64
    PartitionID int64
    NumRows     int64
    State       SegmentState  // Flushed, Sealed, Growing
    Binlogs     []*FieldBinlog  // insert data binlog paths
    Deltalogs   []*FieldBinlog  // delete data delta logs
    Statslogs   []*FieldBinlog  // statistics (bloom filters, etc.)
    IndexInfos  []*FieldIndexInfo // index file info
    MemorySize  int64           // estimated memory size
    Level       SegmentLevel    // L0 (delta) or regular
}
```

## 1.4 QueryNode Segment Loading

### 1.4.1 RPC Entry Point

**File:** `internal/querynodev2/services.go` → `LoadSegments()` (line ~417)

```plaintext
QueryNode.LoadSegments(req):
  │
  ├─ Validate node health
  ├─ Check if segment has index info
  ├─ If needTransfer=true → delegate to shard delegator
  └─ Otherwise:
      ├─ manager.Collection.PutOrRef(segmentID) → increment reference count
      └─ loader.Load(segments, loadScope) → trigger actual loading
```

### 1.4.2 Segment Loader — Core Load Logic

**File:** `internal/querynodev2/segments/segment_loader.go`

**Loader Initialization:**
```go
func NewLoader() {
    ioPoolSize = min(max(CPU * 8, 32), 256)  // IO concurrency pool size
    // can be overridden via queryNode.ioPoolSize
}
```

**Load() function — main orchestration (line ~234):**

```plaintext
segmentLoader.Load(segments):
  │
  Step 1: Filter binlog paths
  │  ├─ Keep only fields specified in loadFields
  │  └─ Skip already-loaded or currently-loading segments
  │
  Step 2: Resource acquisition ★★★
  │  requestResource(infos):
  │  ├─ Calculate estimated memory: LoadMemoryUsageFactor(default 2) × estimatedSize
  │  ├─ Check memory: physicalMemory + committed < totalMemory × threshold(90%)
  │  ├─ Check disk: diskUsage + committed < DiskCapacityLimit
  │  ├─ If LazyLoadEnabled=true → skip resource check
  │  └─ Return allowed concurrency level (ConcurrencyLevel)
  │
  Step 3: Create Segment objects
  │  NewSegment() → pre-allocate memory placeholder
  │
  Step 4: Parallel loading ★★★
  │  for each segment (concurrency = ConcurrencyLevel):
  │    └─ loadSegmentFunc(segment):
  │        │
  │        ├─ If L0 segment → load deltalogs only
  │        │
  │        └─ If regular sealed segment:
  │            │
  │            ├─ LoadSegment(): load index and raw data
  │            │   ├─ Load index files (from object storage)
  │            │   ├─ Decompress data
  │            │   └─ Populate bloom filter
  │            │
  │            ├─ loadDeltalogs(): load delete logs
  │            │
  │            ├─ loadBloomFilterSet(): load bloom filters
  │            │
  │            ├─ manager.Segment.Put(segment): register with manager
  │            │
  │            └─ notifyLoadFinish(): notify load complete
  │
  Step 5: Wait for all parallel tasks to complete
  │  waitSegmentLoadDone()
  │
  └─ Return list of loaded segments
```

### 1.4.3 Single Segment Load Details

```plaintext
LoadSegment(segment, loadInfo):
  │
  ├─ 1. Load vector index (most time-consuming)
  │     Download index files from object storage → deserialize → load into memory
  │     Supported indexes: HNSW, IVF_FLAT, IVF_SQ8, DiskANN, etc.
  │
  ├─ 2. Load scalar data
  │     Read per-field data from binlogs
  │
  ├─ 3. Load Delta Logs
  │     Read delete records from delta log
  │     Apply to segment's delete bitmap
  │
  └─ 4. Load Bloom Filter
      Read bloom filter from stats log
      Used to quickly determine if a PK exists in this segment
```

## 1.5 Progress Tracking and Completion

### 1.5.1 Collection Observer — Periodic Progress Check

**File:** `internal/querycoordv2/observers/collection_observer.go`

```plaintext
CollectionObserver (runs periodically, interval controlled by collectionObserverInterval):
  │
  for each LoadTask:
  │
  ├─ observeLoadStatus():
  │   │
  │   ├─ Get segment count from target
  │   │   targetNum = len(targetSegments) + channelTargetNum
  │   │
  │   ├─ Count loaded segments
  │   │   Query each QueryNode's load state via dist.LeaderViewManager
  │   │   Group by replica
  │   │
  │   ├─ Calculate load percentage ★
  │   │   loadPercentage = (loadedCount × 100) / (targetNum × replicaNum)
  │   │
  │   │   Example: 3 segments × 2 replicas = 6 targets
  │   │            4 loaded → (4 × 100) / 6 = 66%
  │   │
  │   └─ Update metadata
  │       meta.CollectionManager.UpdatePartitionLoadPercent(partID, percentage)
  │       When 100% is reached, triggers Proxy cache invalidation
  │
  └─ All partitions reach 100% → remove LoadTask → "Load task finish"
```

### 1.5.2 Target State Transitions

```plaintext
Next Target (desired state)
    ↓ all segments loaded on all replicas
Current Target (current state)
    → Collection becomes queryable
```

When Target Observer detects that all segments in the next target have been loaded on all replicas:
- Promotes next target to current target
- Clears next target
- Collection state transitions from `Loading` to `Loaded`

### 1.5.3 User Progress Query

```plaintext
User calls GetLoadingProgress(collectionName)
  │
  ▼
Proxy → QueryCoord.ShowLoadCollections(collectionID)
  │
  ▼
QueryCoord calculates and returns:
  InMemoryPercentage: integer 0-100
  QueryServiceAvailable: bool (whether queries can start)
```

## 1.6 Error Handling and Rollback

### Common Error Scenarios

| Error Scenario | Trigger Location | Handling |
|----------------|-----------------|----------|
| Collection doesn't exist | Proxy PreExecute | Return error directly |
| Vector field has no index | Proxy Execute | Return error directly |
| Replica count / field list conflict | QueryCoord PreExecute | Return parameter error |
| Resource group lacks nodes | QueryCoord Execute (SpawnReplicas) | Roll back created metadata |
| DataCoord segment fetch fails | UpdateNextTarget | 10 retries, failure is non-blocking (observer retries periodically) |
| QueryNode out of memory | segmentLoader.requestResource | Return OOM error, QueryCoord may reschedule to another node |
| Object storage download fails | LoadSegment | Return error, task marked failed, Scheduler can retry |

### Rollback Mechanism

**File:** `internal/querycoordv2/job/undo.go`

```go
type UndoList struct {
    CollectionID      int64
    LackPartitions    []int64
    IsNewCollection   bool
    IsReplicaCreated  bool
    IsTargetUpdated   bool
}

func (u *UndoList) RollBack() {
    if u.IsNewCollection || u.IsReplicaCreated {
        meta.CollectionManager.RemoveCollection(u.CollectionID)
    } else {
        meta.CollectionManager.RemovePartition(u.CollectionID, u.LackPartitions...)
    }
    if u.IsTargetUpdated {
        targetObserver.ReleaseCollection(u.CollectionID)
    }
}
```

### Error Recovery Examples

**Scenario 1: QueryNode Out of Memory (OOM)**

```plaintext
1. QueryNode-A receives LoadSegments RPC, calls requestResource()
2. Finds: physicalMemory(85%) + estimatedSize(10%) > threshold(90%)
3. Returns ErrOutOfMemory → Task marked as Failed
4. QueryCoord Scheduler retries:
   a. If other QueryNodes have sufficient memory → reassign to NodeB
   b. If all nodes are full → Task keeps failing, load progress stalls
   c. User needs to scale out QueryNodes or release other Collections
```

**Scenario 2: Object Storage Download Timeout**

```plaintext
1. QueryNode calls LoadSegment() to download index files
2. Object storage (S3/MinIO) response times out
3. LoadSegment() returns error → Task marked as Failed
4. Scheduler detects segment still unloaded in next SegmentChecker round (3s later)
5. Recreates SegmentTask → retries LoadSegments RPC
6. If continuously failing, Load operation times out after LoadTimeoutSeconds (600s)
```

**Scenario 3: QueryNode Crash and Recovery**

```plaintext
1. QueryNode-A crashes, all its segments disappear from Distribution
2. SegmentChecker detects: segment in CurrentTarget but not in Distribution
3. → Recovery scenario, generates Grow Task (LoadPriority_HIGH)
4. Two outcomes:
   a. NodeA restarts successfully → segments reload onto NodeA
   b. NodeA down for extended period → Balance Checker (Stopping Balance)
      migrates segments to other healthy nodes
5. Fully automatic, no manual intervention needed
```

## 1.7 Configuration Parameters (2.5)

### QueryCoord Parameters

| Parameter | Config Key | Default | Description |
|-----------|-----------|---------|-------------|
| TaskExecutionCap | `queryCoord.taskExecutionCap` | 256 | Maximum concurrent task count |
| LoadTimeoutSeconds | `queryCoord.loadTimeoutSeconds` | 600 | Load timeout in seconds |
| SegmentCheckInterval | `queryCoord.checkSegmentInterval` | 3000ms | Segment Checker interval |
| ChannelCheckInterval | `queryCoord.checkChannelInterval` | 3000ms | Channel Checker interval |
| BalanceCheckInterval | `queryCoord.checkBalanceInterval` | 300ms | Balance Checker interval |
| AutoBalance | `queryCoord.autoBalance` | true | Enable automatic balancing |
| BalanceIntervalSeconds | `queryCoord.balanceIntervalSeconds` | 60 | Auto-balance interval |
| ClusterLevelLoadReplicaNumber | `queryCoord.clusterLevelLoadReplicaNumber` | 0 | Default replica count. 0 means use collection/database-level config |
| ClusterLevelLoadResourceGroups | `queryCoord.clusterLevelLoadResourceGroups` | ["default"] | Default resource groups |
| CollectionRecoverTimesLimit | `queryCoord.collectionRecoverTimes` | 3 | Max recovery retry attempts |
| OverloadedMemoryThresholdPercentage | `queryCoord.overloadedMemoryThresholdPercentage` | 90 | Memory overload threshold (triggers balancing) |

### QueryNode Parameters

| Parameter | Config Key | Default | Description |
|-----------|-----------|---------|-------------|
| IoPoolSize | `queryNode.ioPoolSize` | CPU×8 (32~256) | IO concurrency pool size; controls load parallelism |
| LoadMemoryUsageFactor | `queryNode.loadMemoryUsageFactor` | 2 | Memory estimation multiplier for segment loading |
| EnableDisk | `queryNode.enableDisk` | - | Enable disk cache |
| DiskCapacityLimit | `queryNode.diskCapacityLimit` | - | Disk capacity limit |
| CacheMemoryLimit | `queryNode.cacheMemoryLimit` | - | Cache memory limit |
| LazyLoadEnabled | `queryNode.lazyLoadEnabled` | false | Enable lazy loading |
| LazyLoadWaitTimeout | `queryNode.lazyLoadWaitTimeout` | - | Lazy load wait timeout |
| DeltaDataExpansionRate | `querynode.deltaDataExpansionRate` | 50 | Delta data memory expansion factor |

---

# Part 2: Milvus 2.6 Load Segment Pipeline

### 2.5 → 2.6 Core Changes at a Glance

| 2.5 Problem | 2.6 Solution | Driver |
|-------------|-------------|--------|
| Load config propagated via direct RPC; inconsistency possible after node crash | WAL broadcast mode, messages persisted | **Consistency and fault tolerance** |
| All data must load into memory; insufficient for large datasets | Fine-grained Mmap control (per-field-type independent config) | **Support for larger datasets** |
| Load must wait for all data before queries possible | Enhanced Lazy Load (on-demand loading + automatic eviction) | **Faster first query** |
| Balance only considers row count | ScoreBasedBalancer multi-factor scoring | **More uniform load distribution** |

## 2.1 Entry Point (Proxy)

Essentially the same as 2.5, but adds a `Priority` parameter:

**File:** `internal/proxy/task.go`

```go
// New parameter in 2.6
type loadCollectionTask struct {
    // ... same fields as 2.5
    priority commonpb.LoadPriority  // ← new: LoadPriority_High / LoadPriority_Low
}
```

**Execute() new logic:**
- `GetLoadPriority()` → defaults to `LoadPriority_High`, can be set to `LoadPriority_Low`
- Priority propagates to QueryCoord and influences load task scheduling priority

## 2.2 QueryCoord Processing (WAL Broadcast Mode)

**This is the largest architectural change between 2.5 and 2.6.**

### 2.5 Mode vs 2.6 Mode

```plaintext
2.5: Proxy → QueryCoord → LoadCollectionJob → direct metadata ops + direct RPC
2.6: Proxy → QueryCoord → broadcast AlterLoadConfigMessage to WAL → Job consumes WAL message
```

### 2.2.1 Receiving the Request

**File:** `internal/querycoordv2/services.go` → `LoadCollection()`

```plaintext
QueryCoord.LoadCollection(req):
  │
  ├─ Health check
  ├─ Refresh mode check
  │
  └─ broadcastAlterLoadConfigCollectionV2ForLoadCollection() ← new flow
```

### 2.2.2 Broadcast Load Config (2.6 New Mechanism)

**File:** `internal/querycoordv2/ddl_callbacks_alter_load_info_load_collection.go`

```plaintext
broadcastAlterLoadConfigCollectionV2ForLoadCollection():
  │
  Step 1: Acquire Collection-level lock
  │  startBroadcastWithCollectionIDLock()
  │  → prevent concurrent load/release operations
  │
  Step 2: Get Collection metadata
  │  broker.DescribeCollection() → schema, partitionIDs
  │  broker.GetPartitions() → all partitionIDs
  │
  Step 3: Determine Replica configuration
  │  ├─ User specified → use directly
  │  └─ Not specified → getDefaultResourceGroupsAndReplicaNumber()
  │      → query pre-configured broker load info
  │
  Step 4: Validate resource group availability
  │  utils.AssignReplica() → check resource group has enough healthy nodes
  │
  Step 5: Build AlterLoadConfigRequest ★
  │  ├─ CurrentLoadConfig: current load state (read from meta)
  │  └─ ExpectedLoadConfig: target desired state
  │      ├─ ExpectedPartitionIDs
  │      ├─ ExpectedReplicaNumber (per resource group)
  │      ├─ ExpectedFieldIndexID
  │      ├─ ExpectedLoadFields
  │      └─ ExpectedPriority
  │
  Step 6: Generate and broadcast WAL message ★★★
      GenerateAlterLoadConfigMessage():
        ├─ Generate LoadFieldConfig array (fieldID + indexID)
        ├─ Generate LoadReplicaConfig array (per resource group)
        ├─ Compare previous and new header
        │   ├─ Same → return ErrIgnoredAlterLoadConfig (idempotent, no duplicate)
        │   └─ Different → continue
        └─ Broadcast AlterLoadConfigMessageBuilderV2 to streaming.WAL().ControlChannel()
```

**Why does 2.6 introduce WAL broadcast? What's wrong with 2.5's direct RPC?**

**Benefits of WAL broadcast:**
- **Consistency**: All nodes consume the same message from WAL, guaranteeing cluster consistency
- **Durability**: Messages persisted to storage; replayed on node crash recovery
- **Idempotency**: Same config won't be broadcast twice
- **Auditability**: All config changes recorded in control channel

### 2.2.3 Load Job Execution

**File:** `internal/querycoordv2/job/job_load.go`

After WAL message is consumed, executes LoadCollectionJob:

```plaintext
LoadCollectionJob.Execute():
  │
  Step 1: Parse Replica configuration
  │  ├─ UseLocalReplicaConfig → use local cluster-level config
  │  └─ Otherwise → use replica config from message
  │
  Step 2: Create Replicas ★
  │  SpawnReplicasWithReplicaConfig():
  │  ├─ Create new replica ID (idempotent, reuse if exists)
  │  ├─ Assign to QueryNodes in resource group
  │  └─ Invalidate Proxy shard leader cache
  │
  Step 3: Persist Load metadata
  │  ├─ PartitionLoadInfo: Status = Loading
  │  └─ CollectionLoadInfo: Status = Loading, LoadType = LoadCollection
  │
  Step 4: Handle Partition release (if any)
  │  targetObserver.ReleasePartition()
  │
  Step 5: Update Next Target
  │  targetObserver.UpdateNextTarget(collectionID)
  │  → pull segment metadata from DataCoord
  │
  Step 6: Register with Collection Observer
  │  collectionObserver.LoadPartitions(ctx, collectionID, partitionIDs)
  │
  Step 7: Wait for release completion (if any)
  │  WaitCurrentTargetUpdated()
  │
  └─ Return
```

### 2.2.4 Segment Assignment and Task Scheduling

Similar to 2.5, but with the following enhancements:

**Task Priority (new LoadPriority):**

```go
type SegmentTask struct {
    *baseTask
    segmentID    int64
    loadPriority commonpb.LoadPriority  // ← new in 2.6
    actions      []Action
}
```

**Action type additions (2.6):**

```go
// 2.5 Action types
Grow    // load
Reduce  // unload
Update  // update

// New Action types in 2.6
StatsUpdate  // update statistics
DropIndex    // drop index
Reopen       // reload (new config)
```

## 2.3 DataCoord Interaction

Essentially the same as 2.5.

## 2.4 QueryNode Segment Loading

### 2.4.1 RPC Entry Point

**File:** `internal/querynodev2/services.go`

2.6 adds more LoadScope options:

```plaintext
QueryNode.LoadSegments(req):
  │
  ├─ Validate node health
  ├─ Check index info
  │
  └─ Route based on LoadScope:
      ├─ DataScope_Delta → loadDeltaLogs()     (load delete logs only)
      ├─ DataScope_Index → loadIndex()         (load index only)
      ├─ DataScope_Stats → loadStats()         (load stats only) ← new in 2.6
      ├─ DataScope_All   → loader.Load()       (full load)
      └─ Reopen          → reopenSegments()    (reload) ← new in 2.6
```

### 2.4.2 Segment Loader — Enhanced Mmap

**File:** `internal/querynodev2/segments/segment_loader.go`

2.6's load flow adds fine-grained Mmap control:

```plaintext
segmentLoader.Load(segments):
  │
  Steps 1-3: Same as 2.5 (filter, resource acquisition, create Segment)
  │
  Step 4: Parallel loading (enhanced)
  │  for each segment (concurrency = ConcurrencyLevel):
  │    └─ LoadSegment(segment, loadInfo):
  │        │
  │        ├─ Load indexes ★ (Mmap enhanced)
  │        │   for each index (vector/scalar):
  │        │     ├─ enableMmap = isIndexMmapEnable(fieldSchema, indexInfo)
  │        │     │   ├─ Vector index: MmapVectorIndex (default false)
  │        │     │   └─ Scalar index: MmapScalarIndex (default false)
  │        │     │
  │        │     ├─ Determine warmup strategy (from field schema):
  │        │     │   ├─ no_warmup: no pre-load, on-demand on first access
  │        │     │   ├─ async_warmup: background async pre-load
  │        │     │   └─ sync_warmup: synchronous pre-load (blocks until done)
  │        │     │
  │        │     └─ segcore.LoadIndex():
  │        │         ├─ Mmap mode: file-mapped to virtual memory, OS manages page cache
  │        │         └─ Non-Mmap: fully loaded into memory
  │        │
  │        ├─ Load raw data ★ (Mmap enhanced)
  │        │   for each field:
  │        │     ├─ enableMmap = isDataMmapEnable(fieldSchema)
  │        │     │   ├─ Vector field: MmapVectorField (default true in 2.6)
  │        │     │   └─ Scalar field: MmapScalarField (default false)
  │        │     │
  │        │     └─ segcore.LoadFieldData(enableMmap):
  │        │         ├─ Mmap mode: write to disk cache → mmap → apply warmup
  │        │         └─ Non-Mmap: load directly into memory
  │        │
  │        ├─ Load Delta Logs
  │        │
  │        └─ Load Bloom Filter
  │
  └─ Return
```

### 2.4.3 Mmap Loading Details (2.6 Focus)

```plaintext
Mmap loading mode:
  │
  ├─ Vector indexes (HNSW, IVF, DiskANN):
  │   MmapVectorIndex = false (default off)
  │   → index file written to disk cache
  │   → mmap() maps to virtual address space
  │   → actual memory managed by OS on demand
  │   → minimal memory usage (only page cache overhead)
  │
  ├─ Scalar indexes (BitmapIndex, etc.):
  │   MmapScalarIndex = false (default off)
  │   → same as above
  │
  ├─ Vector raw data:
  │   MmapVectorField = true (default ON in 2.6)
  │   → raw data mmap-mapped
  │   → suitable for large-scale vector storage
  │
  └─ Scalar raw data:
      MmapScalarField = false (default off)
      → loaded directly into memory (scalar data typically small, faster in-memory)
      → must be explicitly enabled

Warmup strategies (configured via field schema type_params):
  ├─ no_warmup:    fully on-demand; page faults on first query
  │                Pros: fastest load  Cons: high first-query latency
  │                Use for: cold data, infrequently queried fields
  │
  ├─ async_warmup: background async page loading; does not block LoadSegment return
  │                Pros: fast load + background warmup  Cons: queries may still be slow during warmup
  │                Use for: recommended for most scenarios
  │
  └─ sync_warmup:  synchronous load of all pages; blocks until complete
                   Pros: full speed immediately after load  Cons: slowest load
                   Use for: hot data with strict query latency requirements
```

### 2.4.4 Enhanced Lazy Load (2.6)

```plaintext
Lazy Load enable conditions:
  collection.Properties sets "lazy_load_enabled" = true
  AND segmentType != SegmentTypeGrowing

Lazy Load flow:
  │
  ├─ Skip resource check during Load()
  │   → segment immediately marked as "loaded" (data not actually loaded)
  │
  ├─ First query triggers actual loading:
  │   lazyLoadRequestResourceWithRetry():
  │     ├─ Loop requesting resources, 2s interval (lazyload.requestResourceRetryInterval)
  │     ├─ 5s timeout (lazyload.requestResourceTimeout)
  │     ├─ Max 1 retry (lazyload.maxRetryTimes)
  │     ├─ On OOM: try evicting rarely-used segments (LazyLoadMaxEvictPerRetry)
  │     └─ Resources available → execute actual loading
  │
  └─ Advantages:
      ├─ LoadCollection returns near-instantly
      ├─ Suitable for large collections without need to query all data simultaneously
      └─ Cold data automatically evicted, hot data retained in memory
```

## 2.5 Progress Tracking and Completion

Essentially the same as 2.5; progress calculation method unchanged.

## 2.6 Error Handling and Rollback

Essentially the same rollback mechanism as 2.5, but with WAL-level guarantees added:

| Enhancement | Description |
|-------------|-------------|
| WAL idempotency | Same config not broadcast again, returns `ErrIgnoredAlterLoadConfig` |
| WAL persistence | Node crash recovery can replay config from WAL |
| Memory-aware batch splitting | LoadCellBatchAsync splits into batches by memory size to prevent OOM |

## 2.7 Configuration Parameters (2.6)

### QueryCoord Parameters (2.6 changes)

| Parameter | Config Key | Default | vs. 2.5 |
|-----------|-----------|---------|---------|
| TaskExecutionCap | `queryCoord.taskExecutionCap` | **360** | Was 256 in 2.5 |
| Balancer | `queryCoord.balancer` | **ScoreBasedBalancer** | Same as 2.5/2.6 |
| BalanceTriggerOrder | `queryCoord.balanceTriggerOrder` | **ByRowCount** | Introduced in v2.5.8 |
| BalanceSegmentBatchSize | `queryCoord.balanceSegmentBatchSize` | 5 | Introduced in v2.5.14 |
| BalanceChannelBatchSize | `queryCoord.balanceChannelBatchSize` | 1 | Introduced in v2.5.14 |
| QueryNodeTaskParallelismFactor | `queryCoord.queryNodeTaskParallelismFactor` | 20 | **New in 2.6** |
| ChannelTaskCapFraction | `queryCoord.channelTaskCapFraction` | 0.3 | **New in 2.6** |

### QueryNode Parameters (2.6 changes)

| Parameter | Config Key | Default | vs. 2.5 |
|-----------|-----------|---------|---------|
| MmapVectorField | `queryNode.mmap.vectorField` | **true** | Was false in 2.5 |
| MmapVectorIndex | `queryNode.mmap.vectorIndex` | **false** | Same as 2.5/2.6 |
| MmapScalarField | `queryNode.mmap.scalarField` | **false** | Same as 2.5/2.6 |
| MmapScalarIndex | `queryNode.mmap.scalarIndex` | **false** | Same as 2.5/2.6 |
| MmapPopulate | `queryNode.mmap.populate` | **true** | New in 2.6 |
| TextIndexExpansionFactor | `queryNode.textIndexExpansionFactor` | **1.0** | **New in 2.6** |

---

# Part 3: All Scenarios That Trigger LoadSegments

## 3.1 Overview: 5 Trigger Sources and Multiple Scenarios

```plaintext
                    ┌─────────────────────────────────────┐
                    │     All paths converge at:           │
                    │  cluster.LoadSegments(node, req)     │
                    │  (executor.go, line ~308)            │
                    └──────────────┬──────────────────────┘
                                   │
               ┌───────────────────┼───────────────────┐
               │                   │                   │
      ┌────────┴────────┐  ┌───────┴──────┐  ┌────────┴────────┐
      │  3 Checkers     │  │  1 Observer  │  │  1 Checker      │
      │ (produce tasks) │  │ (updates     │  │  (indirect dep) │
      └────────┬────────┘  │  Target)     │  └────────┬────────┘
               │           └───────┬──────┘           │
   ┌───────────┼──────────┐        │                  │
   │           │          │        │                  │
   ▼           ▼          ▼        ▼                  ▼
SegmentChecker IndexChecker BalanceChecker TargetObserver ChannelChecker
(direct Grow)  (Update/     (Grow+Reduce  (pull new      (prerequisite:
               Stats tasks)  migration)    Target →       channel must be
                                          drives          in place before
                                          Checkers)       segment loads)
   │
   ├─ Scenario 1: Initial Load (user calls LoadCollection)
   ├─ Scenario 2: Recovery (node failure, reload) → HIGH priority
   ├─ Scenario 3: Handoff (Growing→Sealed transition) → LOW priority
   └─ Scenario 4: Refresh/Import (load after data import)
```

### Unified Call Stack

Regardless of which path triggered it, all paths go through the same call stack:

```plaintext
[Trigger path 1-9] → detect Gap between Target and Distribution
  ↓
Generate Task (Grow / Reduce / Reopen / StatsUpdate etc.)
  ↓
Scheduler.Add(task) → queue by priority
  ↓
Executor.ExecuteTask()
  ↓
Executor.executeSegmentAction()
  ↓
Executor.loadSegment()  [Grow/Reopen/StatsUpdate all go here]
  ↓
getMetaInfo() → getLoadInfo() → packLoadSegmentRequest()
  ↓
ChannelDistManager.GetShardLeader() → determine target node
  ↓
cluster.LoadSegments(ctx, nodeID, request)  ← final RPC call
```

**File:** `internal/querycoordv2/task/executor.go` (line ~243-319)

## 3.2 User-Initiated Load (Segment Checker)

**File:** `internal/querycoordv2/checkers/segment_checker.go`

**Execution period:** Every `queryCoord.checkSegmentInterval` (default 3s)

```plaintext
SegmentChecker.Check():
  │
  for each loaded collection:
  │
  ├─ checkReplica(replica):
  │   │
  │   └─ getSealedSegmentDiff():
  │       │
  │       ├─ Iterate segments in NextTarget
  │       │   Compare with SegmentDistribution (loaded segments)
  │       │
  │       ├─ Find "should have but doesn't" segments → need Grow
  │       │
  │       └─ Assign priority by scenario: ★★★ critical logic
  │           │
  │           ├─ Segment in CurrentTarget but not in Distribution
  │           │   → Recovery scenario → LoadPriority_HIGH
  │           │
  │           ├─ Collection not fully refreshed (Import scenario)
  │           │   → Refresh scenario → replica.LoadPriority() (user config)
  │           │
  │           └─ Segment in NextTarget but not in CurrentTarget
  │               → Handoff scenario → LoadPriority_LOW
  │
  └─ createSegmentLoadTasks():
      → group by shard
      → get shard leader
      → create SegmentTask (Grow action)
      → submit to Scheduler
```

**Core priority logic** (line ~356-378):

```go
if currentTargetExist {
    _, existOnCurrent := currentTargetMap[segment.GetID()]
    if existOnCurrent {
        // In CurrentTarget but not in Distribution → Recovery (node died)
        loadPriorities = append(loadPriorities, LoadPriority_HIGH)
    } else if collection != nil && !collection.IsRefreshed() {
        // Collection in refresh state (Import) → user-configured priority
        loadPriorities = append(loadPriorities, replica.LoadPriority())
    } else {
        // In NextTarget but not in CurrentTarget → Handoff (flush produced new segment)
        loadPriorities = append(loadPriorities, LoadPriority_LOW)
    }
}
```

## 3.3 Index Build Completion (Index Checker)

**File:** `internal/querycoordv2/checkers/index_checker.go`

```plaintext
IndexChecker.Check():
  │
  for each loaded collection:
  │
  ├─ Get all index definitions from broker
  │
  ├─ for each replica:
  │   for each segment in distribution:
  │     │
  │     ├─ checkSegment(): compare current IndexInfo with latest index from broker
  │     │
  │     ├─ If index has updates (new index build completed):
  │     │   → add to segmentsToUpdate list
  │     │
  │     └─ If JSON stats need update (EnabledJSONKeyStats=true):
  │         → add to segmentsToStats list
  │
  ├─ broker.GetIndexInfo(segmentIDs) → batch fetch latest index info (1024 per batch)
  │
  ├─ Generate Update Task (ActionTypeUpdate):
  │   → priority: TaskPriorityLow
  │   → reason: "missing index"
  │   → effect: QueryNode updates segment with new index
  │
  └─ Generate StatsUpdate Task (ActionTypeStatsUpdate): [if enabled]
      → priority: TaskPriorityLow
      → reason: "missing json stats"
```

**Key points:**
- Index Checker only processes segments on RW (read-write) nodes, skips RO (read-only/stopping) nodes
- IndexChecker uses `Update` action (not `Reopen`) — QueryNode loads new index on top of existing data
- Batch queries index info, at most 1024 segments per call to avoid oversized RPCs

## 3.4 Load Balancing (Balance Checker)

**File:** `internal/querycoordv2/checkers/balance_checker.go`

### Two-Phase Balancing

```plaintext
BalanceChecker.Check():
  │
  ├─ Phase 1: Stopping Balance ★ High priority
  │   │
  │   ├─ Trigger: a node is marked RO (read-only/stopping) or ROSQ
  │   │   Detection: replica.RONodesCount() + ROSQNodesCount() > 0
  │   │
  │   ├─ Execute:
  │   │   getReplicaForStoppingBalance() → find replicas with RO nodes
  │   │   StoppingBalancer.BalanceReplica() → generate migration plan
  │   │
  │   ├─ Generate:
  │   │   ├─ Grow Task (load segment on target node): LoadPriority_HIGH
  │   │   └─ Reduce Task (unload on source node): executes after Grow completes
  │   │
  │   └─ Use: data migration before node goes offline, ensures no data loss
  │
  └─ Phase 2: Normal Balance → Low priority
      │
      ├─ Trigger conditions:
      │   ├─ AutoBalance = true (default on)
      │   ├─ Time since last balance exceeds AutoBalanceInterval (default 5s)
      │   └─ Phase 1 produced no tasks
      │
      ├─ Execute:
      │   ScoreBasedBalancer.BalanceReplica() → multi-factor scoring → migration plan
      │
      ├─ Scoring factors (2.6):
      │   ├─ RowCountFactor (0.4): row count weight
      │   ├─ SegmentCountFactor (0.4): segment count weight
      │   ├─ GlobalRowCountFactor (0.1): global row count weight
      │   └─ GlobalSegmentCountFactor (0.1): global segment count weight
      │
      ├─ Balance trigger threshold:
      │   ScoreUnbalanceTolerationFactor = 0.05
      │   → only triggers when imbalance between nodes > 5%
      │
      ├─ Generate:
      │   ├─ Grow Task (target node): LoadPriority_LOW
      │   └─ Reduce Task (source node): executes after Grow completes
      │
      └─ Batch limits:
          ├─ BalanceSegmentBatchSize = 5 (max 5 segments migrated per round)
          └─ BalanceChannelBatchSize = 1 (max 1 channel migrated per round)
```

### Balance Grow+Reduce Pairing

```plaintext
Migrating a segment from NodeA → NodeB:

1. Create Grow Action: load segment on NodeB
   → send LoadSegments RPC to NodeB

2. Wait for Grow to complete

3. Create Reduce Action: unload segment on NodeA
   → send ReleaseSegments RPC to NodeA

4. Update Leader View: shard leader routes to NodeB
```

## 3.5 Target Update — Compaction/Flush (Target Observer)

**File:** `internal/querycoordv2/observers/target_observer.go`

```plaintext
TargetObserver (runs continuously):
  │
  ├─ shouldUpdateNextTarget():
  │   ├─ Time since last update exceeds UpdateNextTargetInterval
  │   ├─ Or NextTargetSurviveTime expired
  │   └─ Or manually triggered externally
  │
  ├─ UpdateNextTarget(collectionID):
  │   │
  │   ├─ targetMgr.UpdateCollectionNextTarget():
  │   │   ├─ broker.GetRecoveryInfoV2() → get latest segment list from DataCoord
  │   │   │   Returns:
  │   │   │   ├─ New compacted segments (merged large segments)
  │   │   │   ├─ New flushed sealed segments
  │   │   │   └─ Deleted old segments
  │   │   │
  │   │   └─ Create new NextTarget (with latest segment list)
  │   │
  │   └─ Wait for segment data ready (if UpdateTargetNeedSegmentDataReady=true)
  │
  ├─ shouldUpdateCurrentTarget():
  │   ├─ All segments in NextTarget loaded on all replicas
  │   └─ Or timed out
  │
  └─ updateCurrentTarget():
      ├─ Promote NextTarget to CurrentTarget
      └─ Clear NextTarget
          │
          ▼
      SegmentChecker detects new segments in new CurrentTarget
      → automatically generates Grow Task → LoadSegments RPC
```

### Compaction Example

```plaintext
Before compaction:
  CurrentTarget: [seg1, seg2, seg3]    ← 3 small segments
  Distribution:  [seg1@Node1, seg2@Node1, seg3@Node2]

DataCoord executes compaction: seg1 + seg2 → seg4

After compaction:
  NextTarget:    [seg3, seg4]          ← seg4 is newly merged
  CurrentTarget: [seg1, seg2, seg3]    ← old ones still there

SegmentChecker detects:
  seg4 in NextTarget but not in Distribution → Grow (Handoff, LOW priority)

After seg4 loads:
  NextTarget promoted to CurrentTarget
  seg1, seg2 not in new CurrentTarget → Reduce (unload old segments)
```

## 3.6 Growing→Sealed Transition (Handoff)

**File:** `internal/querycoordv2/checkers/segment_checker.go` → `getGrowingSegmentDiff()` (line ~257-310)

```plaintext
Handoff complete flow:

1. DataNode flushes growing segment → produces sealed segment
   │
2. DataCoord updates segment list:
   │  ├─ Add sealed segment (seg_new)
   │  └─ Remove growing segment (seg_old)
   │
3. TargetObserver pulls update → NextTarget contains seg_new, not seg_old
   │
4. SegmentChecker.getSealedSegmentDiff():
   │  seg_new in NextTarget but not in Distribution
   │  and not in CurrentTarget → Handoff scenario
   │  → Grow Task: LoadPriority_LOW
   │
5. QueryNode loads seg_new (sealed segment)
   │
6. SegmentChecker.getGrowingSegmentDiff():
   │  seg_old (growing) in LeaderView but not in Target
   │  → Reduce Task: release growing segment
   │  → DataScope_Streaming
   │
7. Growing segment released, Sealed segment ready
   │
8. NextTarget promoted to CurrentTarget → handoff complete
```

**Note:** Handoff Grow and Reduce are not atomic. The sealed segment is loaded first, then the growing segment is released only after confirmation, ensuring no data loss.

## 3.7 Node Failure Recovery

**Trigger path:** SegmentChecker Recovery branch

```plaintext
Node crash and recovery flow:

1. QueryNode-A crashes
   │
2. ResourceObserver detects node offline
   │  → Node removed from replica's RW node list
   │  → Node's segments disappear from Distribution
   │
3. SegmentChecker.Check():
   │  segment in CurrentTarget but not in Distribution
   │  → Recovery scenario
   │  → Grow Task: LoadPriority_HIGH ← highest priority!
   │
4. Two recovery paths:
   │
   ├─ Path A: Node restarts successfully
   │   → segments reload onto restarted node
   │
   └─ Path B: Node down for extended period
       → segments scheduled to other healthy nodes
       (if stopping balance is active, Balance Checker handles this)
```

**Key points:**
- Recovery uses `LoadPriority_HIGH` to ensure it executes ahead of other tasks
- If node is marked RO (stopping), Balance Checker's Stopping Balance proactively migrates data
- Both mechanisms complement each other: Recovery ensures fast data restoration, Stopping Balance ensures graceful node shutdown

## 3.8 Channel Assignment (Channel Checker)

**File:** `internal/querycoordv2/checkers/channel_checker.go`

```plaintext
ChannelChecker.Check():
  │
  ├─ getDmChannelDiff():
  │   Compare channels in NextTarget vs ChannelDistribution
  │
  ├─ Missing channel:
  │   → Channel Load Task (ActionTypeGrow)
  │   → Priority: TaskPriorityHigh (all channel tasks are high priority)
  │   → Assignment strategy: RoundRobin
  │
  ├─ Excess channel:
  │   → Channel Reduce Task
  │   → Clean up released collection's channels
  │
  └─ Duplicate channel (same channel on multiple nodes):
      → Keep newer version, release old one
```

**Channel Checker does not directly trigger LoadSegments RPC**, but it is a prerequisite for segment loading. No channel → no shard leader → segments cannot be assigned.

## 3.9 Leader Route Sync (Leader Checker)

**File:** `internal/querycoordv2/checkers/leader_checker.go`

```plaintext
LeaderChecker.Check():
  │
  ├─ findNeedLoadedSegments():
  │   │
  │   ├─ Compare LeaderView (routing table) vs Distribution (actual distribution)
  │   │
  │   ├─ Segment in Distribution but missing in routing table:
  │   │   → LeaderSegmentTask (Grow): "add segment to leader view"
  │   │   → Update Delegator routing table
  │   │
  │   └─ Segment in routing table but pointing to wrong node:
  │       → Update route to correct node
  │
  └─ findNeedRemovedSegments():
      Segment in routing table but no longer in Distribution:
      → LeaderSegmentTask (Reduce): "remove segment from leader view"
```

**Note:** Leader Checker generates **LeaderSegmentTask** (not regular SegmentTask) — it updates the Delegator's in-memory routing table, does not directly trigger LoadSegments RPC. But it ensures queries are correctly routed to nodes that have loaded the segment.

## 3.10 Manual Refresh / Import

```plaintext
Import/BulkInsert flow:
  │
  1. User executes BulkInsert → DataCoord creates new segments
  │
  2. User calls LoadCollection(refresh=true)
  │   or system auto-detects new segments
  │
  3. Collection marked as "not refreshed"
  │   collection.IsRefreshed() = false
  │
  4. TargetObserver updates NextTarget (includes newly imported segments)
  │
  5. SegmentChecker detects new segments:
  │   collection.IsRefreshed() == false → Refresh scenario
  │   → Grow Task: replica.LoadPriority() (user-configured, default HIGH)
  │
  6. All new segments loaded
  │   → collection.SetRefreshed(true)
  │   → Refresh complete
```

## 3.11 Trigger Path Priority Summary

### Task Priority (Scheduler scheduling priority)

```plaintext
TaskPriorityHigh  ──── Channel tasks (all channel operations)
  │                    Stopping Balance segment tasks
  │
TaskPriorityNormal ─── User Load segment tasks
  │                    Growing segment release tasks
  │
TaskPriorityLow  ───── Index update (Update) tasks
                       Normal Balance segment tasks
                       Leader route update tasks
                       Handoff cleanup tasks
                       Stats update tasks
```

### Load Priority (loading urgency)

```plaintext
LoadPriority_HIGH  ──── Node failure recovery (data may be lost, most urgent)
  │                     Stopping Balance (node about to go offline)
  │                     User-initiated Load (default HIGH)
  │                     Import/Refresh (replica default HIGH)
  │
LoadPriority_LOW  ───── Handoff (Growing→Sealed, non-urgent)
                        Normal Balance (performance optimization, least urgent)
```

### Scenario Summary (grouped by trigger source)

| Trigger | Scenario | Condition | Action | Task Priority | Load Priority |
|---------|----------|-----------|--------|--------------|--------------|
| **SegmentChecker** | Initial Load | New segment in NextTarget | Grow | Normal | **HIGH** (default) |
| ↑ | Recovery (node failure) | In CurrentTarget but not in Dist | Grow | Normal | **HIGH** |
| ↑ | Handoff (flush) | In NextTarget but not in CurrentTarget | Grow | Normal | LOW |
| ↑ | Refresh/Import | Collection not refreshed | Grow | Normal | **HIGH** (default) |
| **IndexChecker** | Index build complete | Index version updated | **Update** | Low | LOW |
| ↑ | Stats update | JSON stats stale | StatsUpdate | Low | LOW |
| **BalanceChecker** | Stopping Balance | Has RO nodes | Grow+Reduce | Low | **HIGH** |
| ↑ | Normal Balance | Node load imbalanced | Grow+Reduce | Low | LOW |
| **TargetObserver** | Compaction/Flush | DataCoord updates segment list | *(indirect)* | - | - |
| **ChannelChecker** | Channel missing | Target has but Dist doesn't | Channel Grow | **High** | - |

---

# Part 4: 2.5 vs 2.6 Key Differences

## Architecture-Level Changes

| Dimension | Milvus 2.5 | Milvus 2.6 |
|-----------|-----------|-----------|
| **Load config propagation** | Direct RPC + metadata ops | **WAL broadcast** (AlterLoadConfigMessage) |
| **Consistency guarantee** | Relies on metadata storage | WAL guarantees ordering and persistence |
| **Idempotency** | Job-level check | WAL message level (same config not rebroadcast) |
| **Failure recovery** | Recover from metadata | Replay from WAL + metadata recovery |

## Feature-Level Changes

| Feature | Milvus 2.5 | Milvus 2.6 |
|---------|-----------|-----------|
| **Load Priority** | None | Supports High/Low priority |
| **Mmap** | Supported (all off by default) | **Vector field mmap on by default**, new MmapPopulate |
| **Warmup strategy** | None | no_warmup / async_warmup / sync_warmup |
| **Lazy Load** | Basic | Enhanced: retry mechanism, resource timeout, auto eviction |
| **LoadScope** | Delta/Index/All | New: Stats, Reopen |
| **Action types** | Grow/Reduce/Update | New: StatsUpdate, DropIndex, Reopen |

## Balance Strategy Changes

| Dimension | Milvus 2.5 | Milvus 2.6 |
|-----------|-----------|-----------|
| **Balance algorithm** | ScoreBasedBalancer (v2.0.0+) | ScoreBasedBalancer |
| **Trigger** | Fixed interval | ByRowCount or ByCollectionID ordering |
| **Batch size** | Unlimited | balanceSegmentBatchSize=5, balanceChannelBatchSize=1 |
| **Tolerance** | None | scoreUnbalanceTolerationFactor=0.05 |
| **Stopping Balance** | No special handling | **stoppingBalanceQueue** high-priority queue |

## Configuration Parameter Changes

| Parameter | 2.5 Default | 2.6 Default | Notes |
|-----------|------------|------------|-------|
| taskExecutionCap | 256 | 360 | Increased concurrent task limit |
| mmap.vectorField default | false | **true** | 2.6 enables vector field mmap by default |
| LoadPriority | N/A | Added | New load priority feature |

## End-to-End Flow Comparison

```plaintext
=== Milvus 2.5 ===

SDK → Proxy → QueryCoord.LoadCollection()
                │
                ▼
          LoadCollectionJob (direct execution)
                │
                ├─ Create Replica
                ├─ Persist metadata (Status: Loading)
                ├─ UpdateNextTarget (pull DataCoord segment info)
                └─ Register CollectionObserver
                      │
                      ▼
                SegmentChecker (every 3s)
                → Create SegmentTask (Grow)
                → Scheduler sends LoadSegments RPC to QueryNode
                      │
                      ▼
                QueryNode.LoadSegments()
                → Resource check → parallel load (index+data+delta+bloom)
                      │
                      ▼
                CollectionObserver detects 100% → Loaded


=== Milvus 2.6 ===

SDK → Proxy → QueryCoord.LoadCollection()
                │
                ▼
          broadcastAlterLoadConfigMessage() ← new step
                │
                ├─ Acquire lock
                ├─ Get metadata
                ├─ Validate resource groups
                ├─ Build AlterLoadConfigRequest
                └─ Broadcast to WAL ControlChannel ← key change
                      │
                      ▼
          WAL consumed → LoadCollectionJob (executed)
                │
                ├─ Create Replica (idempotent)
                ├─ Persist metadata (Status: Loading)
                ├─ UpdateNextTarget
                └─ Register CollectionObserver
                      │
                      ▼
                SegmentChecker (every 3s)
                → Create SegmentTask (Grow, with LoadPriority)
                → Scheduler sends LoadSegments RPC
                      │
                      ▼
                QueryNode.LoadSegments()
                → Resource check → parallel load
                → **Fine-grained Mmap control** (vector/scalar independent config)
                → **Warmup strategy** (no/async/sync)
                → **Enhanced Lazy Load** (retry + eviction)
                      │
                      ▼
                CollectionObserver detects 100% → Loaded
```

---

# Part 5: Checker Framework and Task Scheduling

## 5.1 Checker Framework Overview

All Checkers are managed by `CheckerController`, running **in parallel** in independent goroutines, each with its own timer.

**File:** `internal/querycoordv2/checkers/controller.go`

```plaintext
CheckerController.Start()
  │
  ├─ goroutine: ChannelChecker  (every 3s)   ─┐
  ├─ goroutine: SegmentChecker  (every 3s)    │
  ├─ goroutine: BalanceChecker  (every 300ms) ├─→ run in parallel, non-blocking
  ├─ goroutine: IndexChecker    (every 10s)   │
  └─ goroutine: LeaderChecker   (every 1s)   ─┘
                    │
                    ▼
              Each tick:
              Check(ctx) → generate Tasks → scheduler.Add(task)
```

### 5 Checkers Detailed

| Checker | File | Interval | Config Key | Task Priority | Responsibility |
|---------|------|----------|-----------|--------------|----------------|
| **ChannelChecker** | `channel_checker.go` | 3s | `queryCoord.checkChannelInterval` | **High** | Ensure channels are assigned |
| **SegmentChecker** | `segment_checker.go` | 3s | `queryCoord.checkSegmentInterval` | **Normal** | Ensure segments are loaded |
| **BalanceChecker** | `balance_checker.go` | 300ms | `queryCoord.checkBalanceInterval` | **Low** | Ensure inter-node load balance |
| **IndexChecker** | `index_checker.go` | 10s | `queryCoord.checkIndexInterval` | Low | Check if indexes need updating |
| **LeaderChecker** | `leader_checker.go` | 1s | `queryCoord.leaderViewUpdateInterval` | Low | Ensure shard leader routing consistency |

### Checker Dependencies

```plaintext
  ChannelChecker ──────────▶ SegmentChecker ──────────▶ LeaderChecker
  "is channel in place?"      "is segment in place?"      "is routing correct?"
       │                          │
       │                          │
       ▼                          ▼
  BalanceChecker ◀──────────────────
  "is load balanced?"
       │
       ▼
  IndexChecker (independent)
  "is index up to date?"

Implicit execution ordering:
  1. Channel must be assigned first → otherwise SegmentChecker can't find shard leader, skips
  2. Segment must be loaded first → otherwise LeaderChecker can't update routing
  3. Balance is based on current distribution → needs Segment/Channel distribution info
  4. Index runs independently → doesn't depend on other Checkers
```

## 5.2 Task Scheduler

**File:** `internal/querycoordv2/task/scheduler.go`

### Task Queue Structure

```plaintext
Scheduler
  ├─ waitQueue:    tasks waiting to execute (just submitted)
  ├─ processQueue: tasks being processed
  │
  └─ bucketed by priority:
      ├─ High bucket:   ChannelChecker tasks
      ├─ Normal bucket: SegmentChecker tasks
      └─ Low bucket:    BalanceChecker / IndexChecker / LeaderChecker tasks
```

### Task Deduplication (Conflict Resolution)

Only one active task per (replicaID, segmentID/channelID):

```plaintext
scheduler.Add(newTask):
  │
  ├─ Check if existing task with same (replicaID, segmentID)
  │
  ├─ Exists and new task has higher priority:
  │   → Cancel old task, replace with new task
  │   → Example: Balance(Low) replaced by Recovery(Normal)
  │
  ├─ Exists and new task has same or lower priority:
  │   → Reject new task
  │   → Next Checker round will regenerate
  │
  └─ No conflict:
      → Enqueue normally
```

### Concurrency Control (Per QueryNode)

```plaintext
Per-QueryNode task concurrency limit:

  Total limit = ceil(CPUNum × QueryNodeTaskParallelismFactor)
    │
    ├─ Channel task limit = ceil(total × ChannelTaskCapFraction)
    │   Default ChannelTaskCapFraction = 0.3 (30%)
    │   Minimum = 1 (prevents starvation)
    │
    └─ Non-channel task limit = total - channel limit
        Minimum = 1 (prevents starvation)

  Both task types counted independently, no mutual preemption
```

## 5.3 Executor — Task Execution

**File:** `internal/querycoordv2/task/executor.go`

```plaintext
Executor.Execute(task, step):
  │
  ├─ Check concurrency limit (channelTaskNum / nonChannelTaskNum)
  │   Over limit → reject, wait for next scheduling round
  │
  ├─ Execute asynchronously (spawn goroutine):
  │   │
  │   ├─ Segment-type Action:
  │   │   ├─ Grow / Update / StatsUpdate → loadSegment()
  │   │   │   ├─ getMetaInfo()        → get collection/partition metadata
  │   │   │   ├─ getLoadInfo()        → get segment binlog/index info
  │   │   │   ├─ packLoadSegmentRequest() → build request
  │   │   │   └─ cluster.LoadSegments(nodeID, req) → send RPC ★
  │   │   │
  │   │   └─ Reduce → releaseSegment()
  │   │       └─ cluster.ReleaseSegments(nodeID, req)
  │   │
  │   ├─ Channel-type Action:
  │   │   ├─ Grow → subscribeChannel()
  │   │   └─ Reduce → unsubscribeChannel()
  │   │
  │   └─ Leader-type Action:
  │       ├─ Grow → setDistribution() (update routing table)
  │       └─ Reduce → removeDistribution()
  │
  └─ Execution complete:
      ├─ Update Distribution Manager
      ├─ Set executedFlag (triggers distHandler to immediately pull)
      └─ Remove task
```

### Timeout Configuration

| Task Type | Config Key | Default |
|-----------|-----------|---------|
| Segment tasks | `queryCoord.segmentTaskTimeout` | 120s |
| Channel tasks | `queryCoord.channelTaskTimeout` | 60s |

---

# Part 6: QueryNode State Reporting (Distribution)

## 6.1 Pull-Based Model

Milvus uses a **pull model** rather than push: QueryCoord actively pulls distribution info from each QueryNode.

```plaintext
QueryCoord                                QueryNode
   │                                         │
   │  Every 500ms (DistPullInterval)          │
   ├─ GetDataDistribution(LastUpdateTs) ────▶│
   │                                         │
   │                                         ├─ Collect current state:
   │                                         │  ├─ Loaded sealed segments
   │                                         │  ├─ Subscribed channels
   │                                         │  ├─ Shard leader views
   │                                         │  └─ Memory/CPU info
   │                                         │
   │◀──── GetDataDistributionResponse ───────┤
   │  (segments, channels, leaderViews,      │
   │   lastModifyTs, memCapacity, cpuNum)    │
   │                                         │
   ├─ Update heartbeat timestamp             │
   ├─ If lastModifyTs unchanged → skip       │
   └─ If changed → update Distribution Mgr  │
```

**Files:**
- QueryNode side: `internal/querynodev2/services.go` → `GetDataDistribution()`
- QueryCoord side: `internal/querycoordv2/dist/dist_handler.go` → `pullDist()`

## 6.2 Distribution Manager

**File:** `internal/querycoordv2/meta/dist_manager.go`

QueryCoord maintains three managers storing distribution info pulled from all QueryNodes:

```plaintext
DistributionManager
  ├─ SegmentDistManager   → which nodes each segment is on
  │   internal/querycoordv2/meta/segment_dist_manager.go
  │
  ├─ ChannelDistManager   → which node each channel is on + LeaderView
  │   internal/querycoordv2/meta/channel_dist_manager.go
  │
  └─ Used by:
      ├─ SegmentChecker  → compare Target vs Distribution
      ├─ BalanceChecker  → analyze inter-node load differences
      ├─ LeaderChecker   → compare LeaderView vs Distribution
      └─ CollectionObserver → calculate load progress percentage
```

## 6.3 Change Detection Optimization

Timestamp-based optimization to avoid unnecessary data processing:

```plaintext
1. distHandler records lastUpdateTs (timestamp of last processing)
2. QueryNode returns LastModifyTs (timestamp of most recent state change)
3. If LastModifyTs <= lastUpdateTs → skip (no changes)
4. Otherwise → execute full Distribution update
```

## 6.4 Heartbeat and Failure Detection

```plaintext
On each successful GetDataDistribution:
  → NodeInfo.LastHeartbeat = now()
  → Update metric: QueryCoordLastHeartbeatTimeStamp

Heartbeat lag warning:
  if now() - LastHeartbeat > HeartBeatWarningLag (default 5s):
      → Log warning "QueryNode heartbeat lag too large"

Node failure determination:
  Determined by ResourceObserver / NodeManager based on heartbeat timeout
  → Triggers SegmentChecker Recovery and BalanceChecker Stopping Balance
```

---

# Part 7: ReleaseCollection Flow

## 7.1 Release End-to-End

```plaintext
User calls ReleaseCollection(collectionName)
  │
  ▼
┌──────────────────────────────────────────────────┐
│ Proxy                                            │
│ File: internal/proxy/impl.go                     │
│                                                  │
│ 1. Create releaseCollectionTask                  │
│ 2. Enqueue in DDL task queue                     │
│ 3. Get collectionID                              │
│ 4. Call QueryCoord.ReleaseCollection()           │
└──────────────┬───────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────┐
│ QueryCoord                                       │
│ File: internal/querycoordv2/services.go          │
│                                                  │
│ 1. Broadcast DropLoadConfigMessageV2 to WAL (2.6)│
│                                                  │
│ 2. ReleaseCollectionJob.Execute():               │
│    ├─ Delete Collection metadata                 │
│    │   meta.CollectionManager.RemoveCollection() │
│    │                                             │
│    ├─ Delete all Replicas                        │
│    │   meta.ReplicaManager.RemoveCollection()    │
│    │                                             │
│    ├─ Clean up Target                            │
│    │   targetObserver.ReleaseCollection()        │
│    │   → targetMgr.RemoveCollection()            │
│    │                                             │
│    ├─ Invalidate Proxy caches                    │
│    │   proxyManager.InvalidateCollectionMetaCache│
│    │   proxyManager.InvalidateShardLeaderCache() │
│    │                                             │
│    └─ Wait for Release completion                │
│       WaitCollectionReleased()                   │
│       → Poll Distribution until all              │
│         segments/channels gone from all nodes    │
└──────────────┬───────────────────────────────────┘
               │
               ▼
┌──────────────────────────────────────────────────┐
│ QueryNode (passive cleanup)                      │
│ File: internal/querynodev2/services.go           │
│                                                  │
│ ReleaseSegments(segmentIDs):                     │
│   ├─ manager.Segment.Remove(segmentID)           │
│   │   → release memory / mmap mapping            │
│   └─ manager.Collection.Unref()                  │
│       → clean up collection metadata when        │
│         reference count drops to zero            │
└──────────────────────────────────────────────────┘
```

## 7.2 Release vs Load Comparison

| Step | Load Operation | Release Operation |
|------|---------------|-------------------|
| Metadata | Create CollectionLoadInfo + PartitionLoadInfo | Delete CollectionLoadInfo + PartitionLoadInfo |
| Replica | SpawnReplicas → create replicas | RemoveCollection → delete all replicas |
| Target | UpdateNextTarget → pull segment list | RemoveCollection → clear target |
| Cache | (invalidate on load complete) | InvalidateCollectionMetaCache + ShardLeaderCache |
| QueryNode | LoadSegments → download and load data | ReleaseSegments → release memory |
| Wait for | Load progress 100% | All segments gone from Distribution |

## 7.3 Partition Release

Releasing a single Partition has two cases:

```plaintext
ReleasePartitions(partitionIDs):
  │
  ├─ Case 1: partitions still online after release
  │   → Broadcast AlterLoadConfigMessageV2 (updated partition list)
  │   → Only clean up segments for specified partitions
  │   → Collection remains queryable (other partitions)
  │
  └─ Case 2: all partitions offline after release
      → Broadcast DropLoadConfigMessageV2
      → Equivalent to ReleaseCollection
      → Clean up all resources
```

---

# Appendix A: Configuration Parameter Reference

### Load Control

| Config Key | Default | Description |
|-----------|---------|-------------|
| `queryCoord.loadTimeoutSeconds` | 600 | Overall Load operation timeout (seconds). If collection still not loaded after this time, considered failed |
| `queryCoord.taskExecutionCap` | 256 (2.5) / 360 (2.6) | Max concurrent task execution count in QueryCoord |
| `queryCoord.clusterLevelLoadReplicaNumber` | 0 | Cluster-level default replica count. 0 = use collection/database config; non-zero = override |
| `queryCoord.clusterLevelLoadResourceGroups` | ["default"] | Cluster-level default resource groups |
| `queryCoord.collectionRecoverTimes` | 3 | Max recovery retry attempts for a collection in loading state. Exceeded limit releases the collection |
| `queryNode.ioPoolSize` | 0 (=CPU×8, 32~256) | Controls goroutines for file fetching. When ≤0, auto-set to CpuNum×8, min 32, max 256 |
| `queryNode.loadMemoryUsageFactor` | 2 | Memory usage estimation multiplier when loading segments. Used to determine if node has enough memory for new segment |

### Checker Intervals

| Config Key | Default | Description |
|-----------|---------|-------------|
| `queryCoord.checkSegmentInterval` | 3000ms | SegmentChecker interval — detects segment differences between Target and Distribution |
| `queryCoord.checkChannelInterval` | 3000ms | ChannelChecker interval — detects if channels are assigned |
| `queryCoord.checkBalanceInterval` | 300ms | BalanceChecker interval — detects inter-node load balance |
| `queryCoord.checkIndexInterval` | 10000ms | IndexChecker interval — detects if segment indexes need updating |
| `queryCoord.leaderViewUpdateInterval` | 1 (seconds) | LeaderObserver interval for getting LeaderView from QueryNode (unit is seconds, not ms) |

### Task Execution

| Config Key | Default | Description |
|-----------|---------|-------------|
| `queryCoord.segmentTaskTimeout` | 120000ms | Segment task timeout (2 minutes). Includes load/unload/update segment operations |
| `queryCoord.channelTaskTimeout` | 60000ms | Channel task timeout (1 minute). Includes channel subscribe/unsubscribe |
| `queryCoord.queryNodeTaskParallelismFactor` | 20 | Per-QueryNode task parallelism factor. Allows cpuNum × factor parallel tasks per node |
| `queryCoord.channelTaskCapFraction` | 0.3 | Fraction of total task capacity for channel tasks per node (0.0-1.0). Prevents channel and segment tasks from starving each other |

### Distribution Pulling

| Config Key | Default | Description |
|-----------|---------|-------------|
| `queryCoord.distPullInterval` | 500ms | Interval for QueryCoord to pull distribution info from each QueryNode |
| `queryCoord.checkExecutedFlagInterval` | 100ms | Interval for checking task execution flag. Accelerates distribution pull after task completion |
| `queryCoord.distRequestTimeout` | 5000ms | Timeout for QueryCoord → QueryNode GetDataDistribution RPC |
| `queryCoord.heatbeatWarningLag` | 5000ms | Heartbeat lag warning threshold. Logs warning if last heartbeat exceeded this (note: config key has typo "heatbeat") |

### Balance Strategy

| Config Key | Default | Since | Description |
|-----------|---------|-------|-------------|
| `queryCoord.autoBalance` | true | 2.0.0 | Enable auto-balance. When on, automatically distributes segment load/unload across QueryNodes |
| `queryCoord.balancer` | ScoreBasedBalancer | 2.0.0 | Segment auto-balance algorithm |
| `queryCoord.balanceTriggerOrder` | ByRowCount | 2.5.8 | Collection balance ordering. `ByRowCount`: prioritize larger collections; `ByCollectionID`: order by ID |
| `queryCoord.autoBalanceInterval` | 3000ms | 2.5.3 | Minimum interval between auto-balance triggers |
| `queryCoord.balanceSegmentBatchSize` | 5 | 2.5.14 | Max segment balance tasks per round |
| `queryCoord.balanceChannelBatchSize` | 1 | 2.5.14 | Max channel balance tasks per round |
| `queryCoord.scoreUnbalanceTolerationFactor` | 0.05 | 2.0.0 | Imbalance tolerance. No migration triggered if imbalance between from/to nodes is below this value |
| `queryCoord.overloadedMemoryThresholdPercentage` | 90 | 2.0.0 | Memory overload threshold (%). **Dual use**: (1) QueryCoord side: triggers sealed segment balancing; (2) QueryNode side (same param name): memory upper bound when loading segments, rejects load if estimated memory exceeds `totalMem × this value` |

### Mmap (2.5+)

| Config Key | 2.5 Default | 2.6 Default | Description |
|-----------|------------|------------|-------------|
| `queryNode.mmap.vectorField` | false | **true** | Whether to use mmap for vector field raw data. When on, data mapped to disk, reducing memory usage |
| `queryNode.mmap.vectorIndex` | false | false | Whether to use mmap for vector indexes |
| `queryNode.mmap.scalarField` | false | false | Whether to use mmap for scalar field raw data |
| `queryNode.mmap.scalarIndex` | false | false | Whether to use mmap for scalar indexes |
| `queryNode.mmap.populate` | - | **true** | Use MAP_POPULATE flag for mmap. When on, OS pre-loads all pages into physical memory (v2.6.9+) |

### Lazy Load

| Config Key | Default | Description |
|-----------|---------|-------------|
| `queryNode.lazyload.enabled` | false | Enable lazy loading. When on, LoadCollection returns immediately; segments loaded on-demand at first query |
| `queryNode.lazyload.waitTimeout` | 30000ms | Lazy load query wait timeout. Max wait time after query triggers lazy load |
| `queryNode.lazyload.requestResourceTimeout` | 5000ms | Lazy load resource request timeout |
| `queryNode.lazyload.requestResourceRetryInterval` | 2000ms | Lazy load resource request retry interval |
| `queryNode.lazyload.maxRetryTimes` | 1 | Max retry attempts for lazy load. How many times to retry on insufficient resources |

### Disk and Memory

| Config Key | Default | Description |
|-----------|---------|-------------|
| `querynode.deltaDataExpansionRate` | 50 | Expansion factor from deltalog physical size to actual memory usage. Used to estimate memory after loading delta data |

---

# Appendix B: Key Source Files

### Proxy

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/proxy/impl.go` | `LoadCollection()`, `ReleaseCollection()`, `GetLoadingProgress()` | RPC entry points |
| `internal/proxy/task.go` | `loadCollectionTask.Execute()`, `releaseCollectionTask.Execute()` | Load/Release tasks |

### QueryCoord — Load/Release

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/querycoordv2/services.go` | `LoadCollection()`, `ReleaseCollection()` | RPC entry points |
| `internal/querycoordv2/job/job_load.go` | `LoadCollectionJob.Execute()` | Load Job |
| `internal/querycoordv2/job/job_release.go` | `ReleaseCollectionJob.Execute()` | Release Job |
| `internal/querycoordv2/job/undo.go` | `UndoList.RollBack()` | Rollback mechanism |

### QueryCoord — Target & Observer

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/querycoordv2/meta/target_manager.go` | `UpdateCollectionNextTarget()`, `RemoveCollection()` | Target management |
| `internal/querycoordv2/observers/target_observer.go` | `UpdateNextTarget()`, `ReleaseCollection()` | Target pull/release |
| `internal/querycoordv2/observers/collection_observer.go` | `observeLoadStatus()` | Progress tracking |

### QueryCoord — Checker & Scheduler

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/querycoordv2/checkers/controller.go` | `Start()`, `startChecker()` | Checker framework |
| `internal/querycoordv2/checkers/segment_checker.go` | `Check()`, `getSealedSegmentDiff()` | Segment assignment |
| `internal/querycoordv2/checkers/channel_checker.go` | `Check()`, `getDmChannelDiff()` | Channel assignment |
| `internal/querycoordv2/checkers/balance_checker.go` | `Check()` | Load balancing |
| `internal/querycoordv2/checkers/index_checker.go` | `Check()`, `checkSegment()` | Index updates |
| `internal/querycoordv2/checkers/leader_checker.go` | `Check()` | Leader route sync |
| `internal/querycoordv2/task/scheduler.go` | `Add()`, `schedule()`, `Dispatch()` | Task scheduling |
| `internal/querycoordv2/task/executor.go` | `Execute()`, `loadSegment()`, `releaseSegment()` | Task execution |
| `internal/querycoordv2/task/task.go` | `SegmentTask`, `ChannelTask` | Task definitions |

### QueryCoord — Distribution

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/querycoordv2/dist/dist_controller.go` | `StartDistInstance()`, `SyncAll()` | Distribution controller |
| `internal/querycoordv2/dist/dist_handler.go` | `pullDist()`, `handleDistResp()` | Pull and process distribution |
| `internal/querycoordv2/meta/dist_manager.go` | `DistributionManager` | Distribution info facade |
| `internal/querycoordv2/meta/segment_dist_manager.go` | `SegmentDistManager` | Segment distribution management |
| `internal/querycoordv2/meta/channel_dist_manager.go` | `ChannelDistManager`, `GetShardLeader()` | Channel + LeaderView |
| `internal/querycoordv2/meta/coordinator_broker.go` | `GetRecoveryInfoV2()` | DataCoord interaction |

### QueryCoord (2.6 WAL Broadcast)

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/querycoordv2/ddl_callbacks_alter_load_info_load_collection.go` | `broadcastAlterLoadConfigCollectionV2ForLoadCollection()` | Load broadcast |
| `internal/querycoordv2/ddl_callbacks_drop_load_info.go` | `broadcastDropLoadConfigCollectionV2ForReleaseCollection()` | Release broadcast |
| `internal/querycoordv2/ddl_callbacks_alter_load_info_release_partitions.go` | `broadcastAlterLoadConfigCollectionV2ForReleasePartitions()` | Partition Release |
| `internal/querycoordv2/job/load_config.go` | `GenerateAlterLoadConfigMessage()` | Generate WAL message |

### QueryNode

| File | Key Functions | Description |
|------|--------------|-------------|
| `internal/querynodev2/services.go` | `LoadSegments()`, `ReleaseSegments()`, `GetDataDistribution()` | RPC entry points |
| `internal/querynodev2/segments/segment_loader.go` | `Load()`, `LoadSegment()`, `requestResource()` | Core load logic |

### Configuration

| File | Key Structures | Description |
|------|--------------|-------------|
| `pkg/util/paramtable/component_param.go` | `QueryCoordCfg`, `QueryNodeCfg` | All configuration parameter definitions |
