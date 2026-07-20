# Query Client Design

## 1. Overview

This document describes the implementation design for the Two-Phase Query Process
defined in the [Distributed Query View Design](../README.md) (Section 3).

The core idea: Proxy obtains a query plan from StreamingNode (Phase 1), then dispatches
queries directly to StreamingNode and QueryNodes in parallel (Phase 2), and reduces all
results itself. This eliminates the Delegator as a single-point bottleneck.

For node-side details, see:

- [Query Plan Node-Side Design](query_plan.md)
- [Query Execution Node-Side Design](query_execution.md)

### Current Flow

```
Client → Proxy → QueryNode (ShardLeader/Delegator)
              → fan-out to workers → reduce at Delegator
         → Proxy reduces across shards → Client
```

### New Flow

```
Client → Proxy → StreamingNode: Phase 1 (get query plan)
              → StreamingNode + QueryNodes: Phase 2 (execute, parallel)
         → Proxy streaming reduce → Client
```

## 2. Architecture

### 2.1 Service Separation

Two new gRPC services are introduced, both on the **data plane**, separate from the
existing `ViewSyncService` (control plane, Coord→Node):

- **QueryPlanService** (Phase 1): Only implemented by StreamingNode. Provides two RPCs:
  - `GetQueryPlan`: Generates a shard-level query plan (view version, MVCC frontiers,
    work node list) and runs global optimizers.
  - `GetMVCCTimestamp`: Lightweight RPC returning only the MVCC frontiers from the
    primary replica's WAL. Used for cross-replica strong consistency (see Section 2.5).

- **ViewQueryService** (Phase 2): Implemented by both StreamingNode and QueryNode.
  Provides Search, Query, and Requery operations. Each node determines which segments
  to execute based on the view version.

All proto definitions are in [view.proto](../../../../../pkg/proto/view.proto).

### 2.2 Error Handling

Errors are transported via gRPC status details (not embedded in response messages),
following the same pattern as `StreamingCode` / `StreamingError`. Key error codes:

- **VIEW_INVALIDATED**: View version is no longer valid (Down/Dropped) → retry from Phase 1.
- **VIEW_NOT_FOUND**: View version not found on this node → retry from Phase 1.
- **ON_SHUTDOWN**: Node is shutting down → retry from Phase 1 (SN may have moved).
- **NOT_PRIMARY**: Node is not the primary replica for this shard → Proxy refreshes
  primary mapping and retries (see Section 2.5).

### 2.3 Plan Reuse

A QueryPlan is a **reusable snapshot reference**. Proxy can issue multiple Phase 2 calls
against the same plan (same version + work nodes) as long as the view remains valid.
This naturally supports requery:

```
Phase 1:  Get query plan → plan with version V
Phase 2a: Search(version=V) → PKs + scores (no output fields)
          Proxy reduces → final top-k PKs
Phase 2b: Requery(version=V, PKs) → output fields for final PKs only
          Proxy merges → return to client
```

This reduces bandwidth by deferring output field retrieval until after reduce.
If the view is invalidated between 2a and 2b, the client retries from Phase 1.

### 2.4 Search and Query Model

Search and Query are separate operations due to fundamentally different result
models (SearchResults uses serialized blob with scores/ranking; RetrieveResults
uses structured FieldData rows).

**Search** supports one or more sub-searches with optional reranking:

| Scenario | Sub-searches | Reranker |
|---|---|---|
| Single Search (no rerank) | 1 | none |
| Single Search (with rerank) | 1 | Decay/Model/... |
| HybridSearch | M | RRF/Weighted/Model/... |

Reranking applies to both single search (e.g., decay function, model-based reranking
on a single vector search result) and HybridSearch (e.g., RRF/weighted merging across
multiple sub-searches). The reranker is constructed internally by the reranker.Builder
based on the request's FunctionScore or RankParams configuration.

**Query** is always a single expression-based retrieve, no reranking.

**Phase 1** is called once per shard, shared by all sub-searches within a Search
request. For HybridSearch, Phase 2 sends the full request (with `is_advanced=true`
and all sub-searches) as one RPC per work node — the node's SearchScheduler
handles internal parallelism.

### 2.5 Multi-Replica Query Flow

**PChannel replication model:** A pchannel can have multiple replicas across SNs.
One SN is the primary for a pchannel (owns WAL, supports read-write); other SNs
are secondary (subscribe to WAL, read-only). A replica's shard assigned to an SN
inherits the SN's primary/secondary status for the corresponding pchannel.

**MVCC and consistency levels:**
- **Strong consistency**: MVCC frontiers must come from the primary SN (latest WAL
  positions). Only the primary SN can guarantee the most up-to-date read point.
  Uses `consistency_level` mode in GetQueryPlanRequest.
- **Session consistency**: Same MVCC routing as Strong consistency. MVCC frontiers
  must come from the primary SN; when the selected target is not primary, Proxy
  first obtains `QueryPlanMVCC` from primary and forwards it to the target. SN
  does not distinguish Session from Strong; primary planning is sent as
  `consistency_level=Strong`.
- **Bounded / Eventual**: MVCC can come from any replica's SN. Secondary SNs use
  their WAL subscription position. Uses `consistency_level` mode.

**MVCC is pchannel-granularity.** Multiple vchannels sharing the same pchannel share
the same WAL, so `GetMVCCTimestamp` returns pchannel-level WAL read frontiers.
The request carries vchannel; the client automatically maps vchannel→pchannel for
routing, and the SN derives pchannel from vchannel internally.

**The `GetQueryPlan` request supports two mutually exclusive MVCC modes** (via oneof):
- `consistency_level`: SN generates MVCC from WAL. For Strong consistency on a
  non-primary SN, returns `NOT_PRIMARY` error.
- `query_plan_mvcc`: Proxy provides a pre-obtained `QueryPlanMVCC`. SN uses it
  directly, skipping WAL lookup.

`QueryPlanMVCC` has two frontiers:

```text
growing_timetick      = StreamingNode growing-runtime wait position
transforming_timetick = QueryNode TransformBuffer wait position
```

Both positions are WAL message TimeTicks. The query client treats them as opaque
executor-local MVCC values: StreamingNode targets consume `growing_timetick`, and
QueryNode targets consume `transforming_timetick`.

**Proxy decision flow per shard:**

```
Proxy selects target replica (load balancing):
  if non-strong consistency:
    → target SN: GetQueryPlan(consistency_level=Bounded/Eventual) → plan → Phase 2
  if strong consistency AND target is primary:
    → primary SN: GetQueryPlan(consistency_level=Strong) → plan → Phase 2
  if strong consistency AND target is NOT primary:
    → primary SN: GetMVCCTimestamp() → mvcc
    → target SN: GetQueryPlan(query_plan_mvcc=mvcc) → plan → Phase 2
  if session consistency:
    → same routing as strong; primary planning uses consistency_level=Strong
```

**Error handling for stale primary mapping:**
Proxy's knowledge of which replica is primary may be stale (e.g., after SN failover).
When a non-primary SN receives a Strong consistency request, it returns
`VIEW_CODE_NOT_PRIMARY`. Proxy then refreshes the primary mapping from
StreamingCoord/WAL binding and retries.

**Secondary SN lag during cross-replica execution:**
When Proxy forwards `QueryPlanMVCC` to a secondary replica, the secondary SN may
not have caught up to those frontiers yet. This is handled gracefully: the SN
returns the query plan normally, and during Phase 2 execution, each node's
SearchScheduler waits for MVCC confirmation before executing queries against
each segment.

## 3. Optimizer Framework

Request rewriting logic currently in the Delegator is split into two levels:

### 3.1 Global Optimizer

Runs during **Phase 1** on StreamingNode. Has access to global information (all segments
across all nodes via the query view).

| Optimizer | Description |
|---|---|
| BM25 IDF | Compute avgdl + IDF vectors from global segment stats, transform query vectors and plan |
| Search Param Tuning | Adjust topk/search params based on global segment count (QueryHook) |

SN loads BM25 statistics during the view lifecycle (Preparing→Ready). Both optimizers
are initially placeholder interfaces.

### 3.2 Local Optimizer

Runs during **Phase 2** on each node (SN/QN). Has access to local segment information only.

| Optimizer | Description |
|---|---|
| Segment Pruning | Prune segments based on local partition statistics |

Initially a placeholder interface.

## 4. Client Architecture

An independent internal query client in `internal/views/queryclient/`, decoupled from Proxy.
Future Proxy integration will use this client as a backend.

### 4.1 Interface

`ViewQueryClient` exposes `Search` and `Query` as separate entry points:

- **Search**: Handles single search and HybridSearch (multiple sub-searches with
  optional reranker). Each sub-search is an `internalpb.SearchRequest`.
- **Query**: Handles single expression-based retrieve. One `internalpb.RetrieveRequest`.

### 4.2 Execution Stages and Field Fetch Planning

Query execution has six stages. Stages 3–6 are optional depending on the
query type, reranking, and rendering requirements:

```
Plan → Search → [RerankQuery] → [Rerank] → [Requery] → Render
```

| Stage | Name | Description |
|---|---|---|
| 1 | **Plan** | Shard resolution + Reranker/Renderer construction + FieldFetchPlan generation + GetQueryPlan (Phase 1) |
| 2 | **Search** | Dispatch to work nodes + streaming reduce (Phase 2) |
| 3 | **RerankQuery** | Fetch reranker-required fields via RequeryOnView (optional) |
| 4 | **Rerank** | Cross-sub-search reranking (optional) |
| 5 | **Requery** | Fetch remaining output/render fields via RequeryOnView (optional) |
| 6 | **Render** | Result post-processing (always runs; noop for plain queries, text highlighting for BM25, etc.) |

The reranker's required fields, renderer's required fields, and the user's output
fields are all known at request time. A **FieldFetchPlanner** (injected at
construction) decides which fields to fetch at each stage:

- **SearchFields**: Returned from work nodes alongside PKs + scores during Search.
  Avoids requery but increases per-node response size.
- **RerankQueryFields**: Fetched via RequeryOnView during RerankQuery, on the full
  candidate set. Only needed if the reranker requires fields not in SearchFields.
- **RequeryFields**: Fetched via RequeryOnView during Requery, on the final top-k
  only. For output and render fields not yet available.

The three sets are disjoint; their union equals
`rerank_fields ∪ output_fields ∪ render_fields`.

Available strategies:

| Strategy | Search carries | RerankQuery | Requery | Best for |
|---|---|---|---|---|
| All-in-Search | all fields | none | none | small fields, few work nodes |
| Defer-all | PKs + scores | all fields | none | high reduce ratio |
| Split | rerank fields | none | output + render fields | small rerank fields, large output |
| Defer-split | PKs + scores | rerank fields | output + render fields | large candidate set, large fields |

The planner can be a static configuration or driven by a `CostEstimator` that
evaluates field sizes, candidate counts, and network topology.

### 4.3 Dependencies

All dependencies are injected as interfaces:

- **QueryPlanClient**: Phase 1 operations (GetQueryPlan, GetMVCCTimestamp). Takes ShardID
  as routing key. Implementation by StreamingNode HandlerClient (see Section 5.4).
- **ViewQueryServiceClient**: Phase 2 operations (SearchOnView, QueryOnView, RequeryOnView).
  Takes WorkNode as routing key. Dispatches to SN or QN sub-client based on node type
  (see Section 5.4).
- **ShardResolver**: Resolves collection → per-vchannel replica info (all replicas +
  primary replica identification). Backed by the channel assignment service discovery
  (see Section 5.5).

### 4.4 Streaming Reducer

A **shard-aware streaming reducer** that processes results incrementally as they arrive
from work nodes. Key properties:

- **Thread-safe**: `Add` can be called concurrently from multiple goroutines.
- **Per-shard rollback**: `ResetShard` discards a single shard's accumulated results
  in O(1), enabling shard-level retry without discarding other shards' results.
- **Eager reduce**: Each per-shard sub-reducer maintains only top-k entities at all
  times. When a new result arrives via `Add`, it is immediately merged into the
  sub-reducer's top-k, and excess entries are discarded. This bounds memory to
  N × top-k (N = number of shards, typically 2–16) regardless of how many work
  nodes contribute results. `Finish` performs a final cross-shard top-k merge.

Both `SearchResultReducer` and `RetrieveResultReducer` follow this pattern.
Multiple reducer implementations handle different semantics (standard top-k,
GroupBy per-group top-k, etc.).

### 4.5 Execution Flow

```
Search(req):
  [Plan]
  1. Resolve collection → shards
  2. reranker.Builder.Build(req) → Reranker (may be nil)
  3. Renderer.Build(req) → Renderer
  4. FieldFetchPlanner.Plan(reranker.RequiredFields(), renderer.RequiredFields(), outputFields) → FieldFetchPlan
  [Search] (per-shard pipelining: each shard starts Phase 2 as soon as its Phase 1 completes)
  5. For each shard concurrently:
       a. GetQueryPlan (Phase 1, once per shard)
       b. For all work nodes in plan concurrently: SearchOnView (with SearchFields)
          → Each result feeds into reducer
          → On VIEW_INVALIDATED: reducer.ResetShard, retry from step 5a for this shard
  6. reducer.Finish() → per-sub-search results
  [RerankQuery] (optional)
  7. if RerankQueryFields non-empty:
       RequeryOnView(candidate PKs, RerankQueryFields) using same plans
  [Rerank] (optional)
  8. if Reranker present:
       Reranker.Rerank(per-sub-search results) → final top-k
  [Requery] (optional)
  9. if RequeryFields non-empty:
       RequeryOnView(final top-k PKs, RequeryFields) using same plans
  [Render]
  10. Renderer.Render(results) → post-processed results
  11. Return final SearchResult

Query(req):
  [Plan]
  1. Resolve collection → shards
  2. Renderer.Build(req) → Renderer
  3. FieldFetchPlanner.Plan(renderer.RequiredFields(), outputFields) → FieldFetchPlan
  [Search] (per-shard pipelining)
  4. For each shard concurrently:
       a. GetQueryPlan (Phase 1, once per shard)
       b. For all work nodes in plan concurrently: QueryOnView (with SearchFields)
          → Each result feeds into reducer
          → On VIEW_INVALIDATED: reducer.ResetShard, retry from step 4a for this shard
  5. reducer.Finish() → reduced result
  [Requery] (optional)
  6. if RequeryFields non-empty:
       RequeryOnView(PKs, RequeryFields) using same plans
  [Render]
  7. Renderer.Render(results) → post-processed results
  8. Return final QueryResult
```

### 4.6 Retry Strategy

- **Scope**: Per-shard. Only the failed shard retries from Phase 1; other shards'
  results are preserved.
- **Max retries**: Configurable (default 3).
- **Timeout**: Shared with the overall request context.
- **On retry**: `ResetShard` discards stale results before re-executing.

## 5. Node-Side Implementation

This section is the query-client-facing summary. The authoritative node-side
Phase 1 and Phase 2 contracts are described in
[Query Plan Node-Side Design](query_plan.md) and
[Query Execution Node-Side Design](query_execution.md).

### 5.1 StreamingNode — Server Side

Implements both QueryPlanService and ViewQueryService gRPC servers.

**Phase 1 — GetQueryPlan:**
1. Find the latest Up-state query view for the requested shard.
2. Generate `QueryPlanMVCC` based on consistency level (from WAL).
   - If `consistency_level=Strong` and this SN is not primary → return `NOT_PRIMARY`.
   - If `query_plan_mvcc` provided → use directly, skip WAL lookup.
3. Run Global Optimizers on the request.
4. Build work node list from the query view (SN itself + all QNs).
   If partition_ids are specified, prune work nodes that have no segments
   for the requested partitions to reduce Phase 2 fan-out.
   SN may also be omitted when its query runtime is ready, already visible at
   the planned MVCC, and has no growing segment candidates after request-scope
   filtering. This SN pruning is fail-open: any unknown or not-yet-visible state
   keeps SN in the plan.
5. Return query plan with optimized request.

**Phase 1 — GetMVCCTimestamp:**
1. Verify this SN is the primary replica (owns WAL) → otherwise return `NOT_PRIMARY`.
2. Return the latest WAL read frontiers as `QueryPlanMVCC`.

**Phase 2 — Search/Query/Requery:**
1. Validate view version exists and is Up/UpRecovering.
2. Delegate to **SearchScheduler** for execution (see Section 5.3).

### 5.2 QueryNode — Server Side

Implements ViewQueryService gRPC server only.

**Phase 2 — Search/Query/Requery:**
1. Validate view version exists and is Ready.
2. Delegate to **SearchScheduler** for execution (see Section 5.3).

### 5.3 SearchScheduler

After view version validation, both SN and QN delegate Phase 2 execution to a
per-node **SearchScheduler**. The scheduler is responsible for:

1. **Local Optimization**: Runs LocalOptimizer (segment pruning) on the node's
   local segments under the given view version.
2. **Segment-level scheduling**: Breaks the query into per-segment tasks and
   manages their concurrent execution with resource-aware scheduling.
3. **MVCC confirmation**: Ensures each segment's data is consistent up to the
   requested `QueryPlanMVCC` before executing queries against it.
4. **Stream reduce**: Incrementally reduces results across segments as they
   complete, maintaining only top-k entries to bound memory usage.
5. Returns the final reduced result for this node.

### 5.4 Client-Side Implementation

The client-side interfaces (`QueryPlanClient`, `ViewQueryServiceClient`) are
implemented by extending existing infrastructure:

**QueryPlanClient** — implemented by extending `HandlerClient`
(`internal/streamingnode/client/handler/`):
- Reuses the existing channel assignment resolver and gRPC connection infrastructure.
- **Multi-SN routing**: With pchannel replication, a pchannel maps to multiple SNs
  (primary + secondaries). The ShardID (replica_id, vchannel) is used as routing key:
  the client resolves vchannel→pchannel, then uses shard assignment data to identify
  which SN holds the target replica, routing to that specific SN.
- `GetQueryPlan`: routes to the replica's SN via the connection pool.
- `GetMVCCTimestamp`: routes to the primary SN (identified via `channels` vs `secondary_channels`).
- Error conversion (gRPC status → ViewError) handled internally via interceptor.

**ViewQueryServiceClient** — composite client dispatching by WorkNode type:

```
ViewQueryServiceClient
  ├── HandlerClient (SN part) → for StreamingWorkNode
  └── QNClient (QN part)      → for QueryWorkNode
```

- **SN part**: Extends `HandlerClient` with SearchOnView/QueryOnView/RequeryOnView
  methods. Reuses the same SN connection pool and routing infrastructure.
- **QN part**: Independent implementation managing QN connections. References the
  existing querynodev2 cluster worker connection management pattern.
- **Dispatch**: The top-level `ViewQueryServiceClient` switches on `WorkNode` type
  and delegates to the appropriate sub-client.

### 5.5 Shard Discovery via Channel Assignment

ShardResolver is backed by the existing channel assignment service discovery
(`streaming.proto`), extended to publish per-SN shard and primary information
alongside pchannel→SN binding.

**Data flow:** Coord publishes shard assignments as part of channel assignment
full updates. `StreamingNodeAssignment` is extended with two new fields:

- `shard_assignment`: A `ShardAssignmentInfo` carrying pchannel-scoped loaded
  shards on this node. Each `PChannelShardAssignment` names one pchannel and
  carries its shard replicas as (collection_id, shard_index, replica_id).
- `secondary_channels`: Secondary (read-only) pchannel replicas on this SN.
  Primary pchannels remain in the existing `channels` field, preserving backward
  compatibility. Old clients ignore the new field.

The client-side watcher maintains a local cache, so shard resolution is a pure
local lookup with zero network overhead on the query path.

**Supply ownership and dependencies:**

- `secondary_channels` is supplied by StreamingCoord's channel assignment layer.
  StreamingCoord already owns the pchannel to StreamingNode binding and each
  `PChannelInfo` carries an `access_mode`; the assignment publisher splits
  read-write pchannels into `channels` and read-only pchannels into
  `secondary_channels`.
- `shard_assignment` is supplied by the qviews Coord layer, not inferred by the
  StreamingCoord channel manager. The authoritative source is the qviews
  load/view management pipeline (`CollectionLoadManager`, Coord-side balancer,
  `ShardViewRegistry` / `ShardViewManager`), which owns the mapping from
  `(collection_id, pchannel, shard_index, replica_id)` to the StreamingNode that
  hosts that shard replica. The client derives the vchannel with
  `funcutil.GetVirtualChannel(pchannel, collection_id, shard_index)`.
- Each `PChannelShardAssignment.pchannel` must appear in either `channels` or
  `secondary_channels` of the same `StreamingNodeAssignment`. This keeps the
  pchannel role and shard mapping in one consistent assignment snapshot.
- The assignment discovery service is the aggregation and publication boundary:
  it joins the StreamingCoord pchannel assignment snapshot with the qviews shard
  assignment snapshot into a single full assignment update. Clients should
  consume this unified snapshot instead of joining channel topology and qviews
  topology independently, so shard routing and primary detection are based on a
  consistent versioned view.

**Primary replica derivation:** The existing `channels` field contains primary
pchannels (WAL owner, read-write); the new `secondary_channels` field contains
secondary pchannels (WAL subscriber, read-only). A replica's shard inherits the
primary/secondary status of its pchannel on the same SN. The client identifies
the primary replica for each vchannel: the replica whose shard is on the SN where
the corresponding pchannel appears in `channels` (not `secondary_channels`).

All proto definitions are in `streaming.proto` under `ShardAssignmentInfo`,
`PChannelShardAssignment`, and `ShardAssignmentEntry`.

## 6. Package Layout

```
internal/views/
├── queryclient/
│   ├── client.go              # ViewQueryClient interface, SearchRequest, QueryRequest
│   ├── resolver.go            # QueryPlanClient, ViewQueryServiceClient, ShardResolver
│   ├── planner.go             # FieldFetchPlanner, FieldFetchPlan, CostEstimator
│   ├── reducer/
│   │   └── reducer.go         # SearchResultReducer, RetrieveResultReducer
│   ├── reranker/
│   │   └── reranker.go        # Reranker interface, Builder interface
│   └── renderer/
│       └── renderer.go        # Renderer interface, Builder interface, noop renderer
├── optimizer/
│   ├── global.go              # GlobalOptimizer interface + no-op placeholder
│   └── local.go               # LocalOptimizer interface + no-op placeholder
├── viewerror/
│   ├── error.go               # ViewError type (parallels StreamingError)
│   └── rpc_error.go           # gRPC status ↔ ViewError conversion
├── coordview/                 # (existing) Coord-side view management
├── nodeview/                  # (existing) Node-side view management
└── qviews/                    # (existing) Shared types

internal/streamingnode/client/handler/
└── handler_client.go          # (existing) Extended with QueryPlanClient + SN ViewQueryService
```
