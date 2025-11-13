# ShardClient Package

The `shardclient` package provides client-side connection management and load balancing for communicating with QueryNode shards in the Milvus distributed architecture. It manages QueryNode client connections, caches shard leader information, and implements intelligent request routing strategies.

## Overview

In Milvus, collections are divided into shards (channels), and each shard has multiple replicas distributed across different QueryNodes for high availability and load balancing. The `shardclient` package is responsible for:

1. **Connection Management**: Maintaining a pool of gRPC connections to QueryNodes with automatic lifecycle management
2. **Shard Leader Cache**: Caching the mapping of shards to their leader QueryNodes to reduce coordination overhead
3. **Load Balancing**: Distributing requests across available QueryNode replicas using configurable policies
4. **Fault Tolerance**: Automatic retry and failover when QueryNodes become unavailable

## Architecture

```
┌──────────────────────────────────────────────────────────────┐
│                      Proxy Layer                              │
│                                                                │
│  ┌─────────────────────────────────────────────────────┐    │
│  │              ShardClientMgr                          │    │
│  │  • Shard leader cache (database → collection → shards) │
│  │  • QueryNode client pool management                   │
│  │  • Client lifecycle (init, purge, close)             │
│  └───────────────────────┬──────────────────────────────┘    │
│                          │                                    │
│  ┌───────────────────────▼──────────────────────────────┐    │
│  │              LBPolicy                                 │    │
│  │  • Execute workload on collection/channels           │    │
│  │  • Retry logic with replica failover                 │    │
│  │  • Node selection via balancer                       │    │
│  └───────────────────────┬──────────────────────────────┘    │
│                          │                                    │
│         ┌────────────────┴────────────────┐                  │
│         │                                  │                  │
│  ┌──────▼────────┐              ┌─────────▼──────────┐       │
│  │ RoundRobin    │              │  LookAsideBalancer │       │
│  │ Balancer      │              │  • Cost-based      │       │
│  │               │              │  • Health check    │       │
│  └───────────────┘              └────────────────────┘       │
│                          │                                    │
│  ┌───────────────────────▼──────────────────────────────┐    │
│  │           shardClient (per QueryNode)                │    │
│  │  • Connection pool (configurable size)               │    │
│  │  • Round-robin client selection                      │    │
│  │  • Lazy initialization and expiration                │    │
│  └──────────────────────────────────────────────────────┘    │
└─────────────────────┬────────────────────────────────────────┘
                      │ gRPC
      ┌───────────────┴───────────────┐
      │                               │
┌─────▼─────┐                  ┌──────▼──────┐
│ QueryNode │                  │ QueryNode   │
│    (1)    │                  │    (2)      │
└───────────┘                  └─────────────┘
```

## Core Components

### 1. ShardClientMgr

The central manager for QueryNode client connections and shard leader information.

**File**: `manager.go`

**Key Responsibilities**:
- Cache shard leader mappings from QueryCoord (`database → collectionName → channel → []nodeInfo`)
- Manage `shardClient` instances for each QueryNode
- Automatically purge expired clients (default: 60 minutes of inactivity)
- Invalidate cache when shard leaders change

**Interface**:
```go
type ShardClientMgr interface {
    GetShard(ctx context.Context, withCache bool, database, collectionName string,
             collectionID int64, channel string) ([]nodeInfo, error)
    GetShardLeaderList(ctx context.Context, database, collectionName string,
                       collectionID int64, withCache bool) ([]string, error)
    DeprecateShardCache(database, collectionName string)
    InvalidateShardLeaderCache(collections []int64)
    GetClient(ctx context.Context, nodeInfo nodeInfo) (types.QueryNodeClient, error)
    Start()
    Close()
}
```

**Configuration**:
- `purgeInterval`: Interval for checking expired clients (default: 600s)
- `expiredDuration`: Time after which inactive clients are purged (default: 60min)

### 2. shardClient

Manages a connection pool to a single QueryNode.

**File**: `shard_client.go`

**Features**:
- **Lazy initialization**: Connections are created on first use
- **Connection pooling**: Configurable pool size (`ProxyCfg.QueryNodePoolingSize`, default: 1)
- **Round-robin selection**: Distributes requests across pool connections
- **Expiration tracking**: Tracks last active time for automatic cleanup
- **Thread-safe**: Safe for concurrent access

**Lifecycle**:
1. Created when first request needs a QueryNode
2. Initializes connection pool on first `getClient()` call
3. Tracks `lastActiveTs` on each use
4. Closed by manager if expired or during shutdown

### 3. LBPolicy

Executes workloads on collections/channels with retry and failover logic.

**File**: `lb_policy.go`

**Key Methods**:

- **`Execute(ctx, CollectionWorkLoad)`**: Execute workload in parallel across all shards
- **`ExecuteOneChannel(ctx, CollectionWorkLoad)`**: Execute workload on any single shard (for lightweight operations)
- **`ExecuteWithRetry(ctx, ChannelWorkload)`**: Execute on specific channel with retry on different replicas

**Retry Strategy**:
- Retry up to `max(retryOnReplica, len(shardLeaders))` times
- Maintain `excludeNodes` set to avoid retrying failed nodes
- Refresh shard leader cache if initial attempt fails
- Clear `excludeNodes` if all replicas exhausted

**Workload Types**:
```go
type ChannelWorkload struct {
    Db             string
    CollectionName string
    CollectionID   int64
    Channel        string
    Nq             int64           // Number of queries
    Exec           ExecuteFunc     // Actual work to execute
}

type ExecuteFunc func(context.Context, UniqueID, types.QueryNodeClient, string) error
```

### 4. Load Balancers

Two strategies for selecting QueryNode replicas:

#### RoundRobinBalancer

**File**: `roundrobin_balancer.go`

Simple round-robin selection across available nodes. No state tracking, minimal overhead.

**Use case**: Uniform workload distribution when all nodes have similar capacity

#### LookAsideBalancer

**File**: `look_aside_balancer.go`

Cost-aware load balancer that considers QueryNode workload and health.

**Features**:
- **Cost metrics tracking**: Caches `CostAggregation` (response time, service time, total NQ) from QueryNodes
- **Workload score calculation**: Uses power-of-3 formula to prefer lightly loaded nodes:
  ```
  score = executeSpeed + (1 + totalNQ + executingNQ)³ × serviceTime
  ```
- **Periodic health checks**: Monitors QueryNode health via `GetComponentStates` RPC
- **Unavailable node handling**: Marks nodes unreachable after consecutive health check failures
- **Adaptive behavior**: Falls back to round-robin when workload difference is small

**Configuration Parameters**:
- `ProxyCfg.CostMetricsExpireTime`: How long to trust cached cost metrics (default: varies)
- `ProxyCfg.CheckWorkloadRequestNum`: Check workload every N requests (default: varies)
- `ProxyCfg.WorkloadToleranceFactor`: Tolerance for workload difference before preferring lighter node
- `ProxyCfg.CheckQueryNodeHealthInterval`: Interval for health checks
- `ProxyCfg.HealthCheckTimeout`: Timeout for health check RPC
- `ProxyCfg.RetryTimesOnHealthCheck`: Failures before marking node unreachable

**Selection Strategy**:
```
if (requestCount % CheckWorkloadRequestNum == 0) {
    // Cost-aware selection
    select node with minimum workload score
    if (maxScore - minScore) / minScore <= WorkloadToleranceFactor {
        fall back to round-robin
    }
} else {
    // Fast path: round-robin
    select next available node
}
```

## Configuration

Key configuration parameters from `paramtable`:

| Parameter | Path | Description | Default |
|-----------|------|-------------|---------|
| QueryNodePoolingSize | `ProxyCfg.QueryNodePoolingSize` | Size of connection pool per QueryNode | 1 |
| RetryTimesOnReplica | `ProxyCfg.RetryTimesOnReplica` | Max retry times on replica failures | varies |
| ReplicaSelectionPolicy | `ProxyCfg.ReplicaSelectionPolicy` | Load balancing policy: `round_robin` or `look_aside` | `look_aside` |
| CostMetricsExpireTime | `ProxyCfg.CostMetricsExpireTime` | Expiration time for cost metrics cache | varies |
| CheckWorkloadRequestNum | `ProxyCfg.CheckWorkloadRequestNum` | Frequency of workload-aware selection | varies |
| WorkloadToleranceFactor | `ProxyCfg.WorkloadToleranceFactor` | Tolerance for workload differences | varies |
| CheckQueryNodeHealthInterval | `ProxyCfg.CheckQueryNodeHealthInterval` | Health check interval | varies |
| HealthCheckTimeout | `ProxyCfg.HealthCheckTimeout` | Health check RPC timeout | varies |

## Usage Example

```go
import (
    "context"
    "github.com/milvus-io/milvus/internal/proxy/shardclient"
    "github.com/milvus-io/milvus/internal/types"
)

// 1. Create ShardClientMgr with MixCoord client
mgr := shardclient.NewShardClientMgr(mixCoordClient)
mgr.Start()  // Start background purge goroutine
defer mgr.Close()

// 2. Create LBPolicy
policy := shardclient.NewLBPolicyImpl(mgr)
policy.Start(ctx)  // Start load balancer (health checks, etc.)
defer policy.Close()

// 3. Execute collection workload (e.g., search/query)
workload := shardclient.CollectionWorkLoad{
    Db:             "default",
    CollectionName: "my_collection",
    CollectionID:   12345,
    Nq:             100,  // Number of queries
    Exec: func(ctx context.Context, nodeID int64, client types.QueryNodeClient, channel string) error {
        // Perform actual work (search, query, etc.)
        req := &querypb.SearchRequest{/* ... */}
        resp, err := client.Search(ctx, req)
        return err
    },
}

// Execute on all channels in parallel
err := policy.Execute(ctx, workload)

// Or execute on any single channel (for lightweight ops)
err := policy.ExecuteOneChannel(ctx, workload)
```

## Cache Management

### Shard Leader Cache

The shard leader cache stores the mapping of shards to their leader QueryNodes:

```
database → collectionName → shardLeaders {
    collectionID: int64
    shardLeaders: map[channel][]nodeInfo
}
```

**Cache Operations**:
- **Hit**: When cached shard leaders are used (tracked via `ProxyCacheStatsCounter`)
- **Miss**: When cache lookup fails, triggers RPC to QueryCoord via `GetShardLeaders`
- **Invalidation**:
  - `DeprecateShardCache(db, collection)`: Remove specific collection
  - `InvalidateShardLeaderCache(collectionIDs)`: Remove collections by ID (called on shard leader changes)
  - `RemoveDatabase(db)`: Remove entire database

### Client Purging

The `ShardClientMgr` periodically purges unused clients:

1. Every `purgeInterval` (default: 600s), iterate all cached clients
2. Check if client is still a shard leader (via `ListShardLocation()`)
3. If not a leader and expired (`lastActiveTs` > `expiredDuration`), close and remove
4. This prevents connection leaks when QueryNodes are removed or shards rebalance

## Error Handling

### Common Errors

- **`errClosed`**: Client is closed (returned when accessing closed `shardClient`)
- **`merr.ErrChannelNotAvailable`**: No available shard leaders for channel
- **`merr.ErrNodeNotAvailable`**: Selected node is not available
- **`merr.ErrCollectionNotLoaded`**: Collection is not loaded in QueryNodes
- **`merr.ErrServiceUnavailable`**: All available nodes are unreachable

### Retry Logic

Retry is handled at multiple levels:

1. **LBPolicy level**:
   - Retries on different replicas when request fails
   - Refreshes shard leader cache on failure
   - Respects context cancellation

2. **Balancer level**:
   - Tracks failed nodes and excludes them from selection
   - Health checks recover nodes when they come back online

3. **gRPC level**:
   - Connection-level retries handled by gRPC layer

## Metrics

The package exports several metrics:

- `ProxyCacheStatsCounter`: Shard leader cache hit/miss statistics
  - Labels: `nodeID`, `method` (GetShard/GetShardLeaderList), `status` (hit/miss)
- `ProxyUpdateCacheLatency`: Latency of updating shard leader cache
  - Labels: `nodeID`, `method`

## Testing

The package includes extensive test coverage:

- `shard_client_test.go`: Tests for connection pool management
- `manager_test.go`: Tests for cache management and client lifecycle
- `lb_policy_test.go`: Tests for retry logic and workload execution
- `roundrobin_balancer_test.go`: Tests for round-robin selection
- `look_aside_balancer_test.go`: Tests for cost-aware selection and health checks

**Mock interfaces** (via mockery):
- `mock_shardclient_manager.go`: Mock `ShardClientMgr`
- `mock_lb_policy.go`: Mock `LBPolicy`
- `mock_lb_balancer.go`: Mock `LBBalancer`

## Thread Safety

All components are designed for concurrent access:

- `shardClientMgrImpl`: Uses `sync.RWMutex` for cache, `typeutil.ConcurrentMap` for clients
- `shardClient`: Uses `sync.RWMutex` and atomic operations
- `LookAsideBalancer`: Uses `typeutil.ConcurrentMap` for all mutable state
- `RoundRobinBalancer`: Uses `atomic.Int64` for index

## Related Components

- **Proxy** (`internal/proxy/`): Uses `shardclient` to route search/query requests to QueryNodes
- **QueryCoord** (`internal/querycoordv2/`): Provides shard leader information via `GetShardLeaders` RPC
- **QueryNode** (`internal/querynodev2/`): Receives and processes requests routed by `shardclient`
- **Registry** (`internal/registry/`): Provides client creation functions for gRPC connections

## Future Improvements

Potential areas for enhancement:

1. **Adaptive pooling**: Dynamically adjust connection pool size based on load
2. **Circuit breaker**: Add circuit breaker pattern for consistently failing nodes
3. **Advanced metrics**: Export more detailed metrics (per-node latency, error rates, etc.)
4. **Smart caching**: Use TTL-based cache expiration instead of invalidation-only
5. **Connection warming**: Pre-establish connections to known QueryNodes
