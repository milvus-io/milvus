# Query Client Legacy Domain Design

## 1. Overview

This document describes the first Proxy integration boundary for QueryView query
execution.

The current Proxy `SearchPipeline` and `QueryPipeline` already own result
processing semantics:

- cross-shard reduce;
- order by;
- group by and aggregation;
- rerank;
- requery;
- render and highlight;
- output field shaping.

The first QueryView integration should not move those responsibilities into
`internal/views/queryclient`. Instead, `queryclient` adds a `Legacy()` domain
that replaces shard execution and shard-local reduction currently implemented by
`queryTask.Execute` and `searchTask.Execute`.

The integration boundary is:

```text
Proxy task
  -> queryclient.Legacy().Search/Query
       -> QueryView two-phase execution
       -> one reduced internalpb result per shard
  -> existing SearchPipeline / QueryPipeline
       -> reduce / rerank / aggregation / render
```

`Legacy` means the domain accepts the existing proxy-generated
`internalpb.SearchRequest` and `internalpb.RetrieveRequest`, and returns the raw
legacy result messages currently produced by `QueryNodeClient.Search` and
`QueryNodeClient.Query`.

## 2. Goals

The `Legacy()` domain provides a minimal runnable QueryView backend for the
existing Proxy query path:

1. Share routing, MVCC selection and retry with `queryclient.Search` and
   `queryclient.Query`.
2. Keep cross-shard processing in `SearchPipeline` and `QueryPipeline`.
3. Replace only the raw execution currently driven by `t.lb.Execute`.
4. Return `[]*internalpb.SearchResults` and `[]*internalpb.RetrieveResults`
   directly to Proxy.
5. Let Search requery reuse the normal Query path instead of implementing
   `RequeryOnView` in the first version.

## 3. Non-goals

The first `Legacy()` implementation does not:

- move cross-shard reduction, reranker, renderer, or field-fetch planning into
  the legacy domain;
- optimize Search requery with `RequeryOnView`;
- preserve old QueryNode leader routing or look-aside balancing semantics;
- use `preferredNodes` for requery locality;
- perform per-channel fast skip based on old `channelsMvcc`;
- change `SearchPipeline` or `QueryPipeline` APIs.

Those optimizations can be introduced after the primary Search/Query path is
runnable through QueryView.

## 4. Public Interfaces

`queryclient` exposes a domain accessor:

```go
type Client interface {
    Legacy() LegacyClient
}
```

The existing full client also exposes the same domain:

```go
type ViewQueryClient interface {
    Client

    Search(ctx context.Context, req *SearchRequest) (*SearchResult, error)
    Query(ctx context.Context, req *QueryRequest) (*QueryResult, error)
}
```

The legacy domain itself is raw-result oriented:

```go
type LegacyClient interface {
    Search(ctx context.Context, req *LegacySearchRequest) (*LegacySearchResult, error)
    Query(ctx context.Context, req *LegacyQueryRequest) (*LegacyQueryResult, error)
}
```

Search request and result:

```go
type LegacySearchRequest struct {
    Req *internalpb.SearchRequest
    MVCC *viewpb.QueryPlanMVCC // optional fixed frontiers, retained across retry
    BarrierMVCC uint64 // optional fixed scalar snapshot; mutually exclusive with MVCC
}

type LegacySearchResult struct {
    Results []*internalpb.SearchResults
    Plans   []ShardPlan
}
```

Query request and result:

```go
type LegacyQueryRequest struct {
    Req *internalpb.RetrieveRequest
    Schema *schemapb.CollectionSchema // exact immutable request schema
    MVCC *viewpb.QueryPlanMVCC // optional fixed frontiers
    BarrierMVCC uint64 // optional fixed scalar snapshot
    Plans []ShardPlan // non-nil reuses the original search plans
}

type LegacyQueryResult struct {
    Results []*internalpb.RetrieveResults
    Plans   []ShardPlan
}
```

`Results` feed the existing Proxy pipeline. `Plans` bind Search requery to
the original View versions, nodes and MVCCs.

## 5. Construction

The legacy domain only needs QueryView routing and execution dependencies:

- `QueryPlanClient`
- `ViewQueryServiceClient`
- `resolver.ShardResolver`
- `ReplicaPicker`
- retry configuration

It does not need:

- `FieldFetchPlanner`
- `reranker.Builder`
- `renderer.Builder`
- reducer builders
- `RequeryRunner`

The full `NewViewQueryClient` constructor can initialize `Legacy()` from the
same shard execution dependencies it already receives. For the Proxy raw
execution path, a lightweight constructor can build only the `Client`/`Legacy`
domain without requiring the not-yet-implemented result-processing dependencies.

Conceptual shape:

```go
func NewLegacyViewQueryClient(
    cfg ViewQueryClientConfig,
    queryPlanClient QueryPlanClient,
    queryServiceClient ViewQueryServiceClient,
    shardResolver resolver.ShardResolver,
    replicaPicker ReplicaPicker,
) Client
```

## 6. Execution Flow

### 6.1 Search

```text
Legacy().Search
  -> ResolveVChannels(collectionID)
  -> for each vchannel:
       -> shardViewQueryClient.Search
            -> ResolveShard
            -> Pick replica
            -> GetQueryPlan
            -> fan out SearchOnView to work nodes
            -> collect resp.legacy_results
  -> return []*internalpb.SearchResults
```

`Legacy().Search` reuses the existing shard-level implementation so retry,
view-error handling, and work-node fanout stay in one place.

### 6.2 Query

```text
Legacy().Query
  -> ResolveVChannels(collectionID)
  -> for each vchannel:
       -> shardViewQueryClient.Query
            -> ResolveShard
            -> Pick replica
            -> GetQueryPlan
            -> fan out QueryOnView to work nodes
            -> collect resp.legacy_results
  -> return []*internalpb.RetrieveResults
```

The returned raw results are equivalent to what the old proxy query path puts
into `queryTask.resultBuf`.

## 7. Collector Semantics

The legacy domain uses local collectors that adapt raw `ViewQueryService`
responses to the existing shard client reducer interface.

Collectors validate status and retain node results by shard. `ResetShard`
discards the failed attempt's bucket after its workers have completed. Once all
shards succeed, QueryViewClient reduces each bucket independently:

- Query uses the same `RunDelegatorQueryPipeline` as the legacy shard handler,
  with the request's immutable Schema. This preserves timestamp-aware PK
  deduplication, ORDER BY, aggregation, limits and empty-field behavior.
- Search uses the existing `ReduceSearchOnQueryNode` reduction. Advanced worker
  responses are already envelopes: first group their `SubResults` by `ReqIndex`,
  reduce each sub-search across workers, then assemble one shard envelope.
- A single worker is already node-reduced and can pass through. Empty shards
  still return their successful plan as an MVCC proof.
- Preserve scan counters, cost accounting, result flags and the successful
  plan's channel MVCC. Do not substitute a newer frontier after execution.

Only shard-local reduction moves into this client. Proxy continues to perform
cross-shard reduction, reranking, shaping and rendering. Query reduction also
applies to retained-plan requery; a failed retained plan propagates to the parent
operation rather than silently selecting a different snapshot.

### Explicit MVCC

Legacy and full Search/Query requests accept optional `MVCC`. When non-nil,
Phase 1 uses `query_plan_mvcc` instead of `consistency_level` on every attempt.
Phase 2 receives the plan unchanged; SN reads at its growing frontier and QN at
its transforming frontier. A per-attempt clone protects the caller's value.
Nil preserves ordinary consistency-based sampling. `Plans` takes precedence
for retained-plan requery and preserves its original MVCC.

`BarrierMVCC` accepts a fixed scalar query timestamp, mutually exclusive with
`MVCC`. SN actively establishes ordered WAL coverage through that timestamp,
using Inspector-driven `MVCCBarrier` and scoped VChannel progress. Its common
`_vc` remains empty; Header lists affected VChannels and the message TimeTick
is the confirmed frontier. It uses the regular sync persistence policy. Both returned executor
frontiers equal the requested timestamp; subsequent WAL progress never changes
the query's snapshot. Query/Search Iterator chooses this timestamp before its
first execution, returns it as SessionTs and reuses it on subsequent requests.
Complex Delete uses its allocated query timestamp. TTL uses the same fixed time.
`Plans` continues to take precedence for requery.

These entries do not pin physical Views across pages or extend historical-data
retention. Compaction/GC history guarantees remain a separate TODO.

## 8. Proxy Integration

### 8.1 Query Task

`queryTask.Execute` currently does:

```text
t.lb.Execute
  -> queryShard
      -> QueryNodeClient.Query
      -> t.resultBuf.Insert(result)
```

With QueryView legacy execution:

```text
Legacy().Query
  -> raw []*internalpb.RetrieveResults
  -> t.resultBuf.Insert(result)
```

`queryTask.PostExecute` gathers
`t.resultBuf`, updates task-level counters and storage cost, and invokes
`QueryPipeline.Execute`. For the first Iterator page it returns the exact
BarrierMVCC selected before execution as SessionTs.

### 8.2 Search Task

`searchTask.Execute` currently does:

```text
t.lb.Execute
  -> searchShard
      -> QueryNodeClient.Search
      -> t.resultBuf.Insert(result)
      -> t.queryChannelsNode[channel] = nodeID
```

With QueryView legacy execution:

```text
Legacy().Search
  -> raw []*internalpb.SearchResults
  -> t.resultBuf.Insert(result)
```

`searchTask.PostExecute` returns the exact BarrierMVCC as the first Iterator
page's SessionTs. It continues to derive:

- `queryChannelsTs` from `SearchResults.channels_mvcc`;
- storage cost from `SearchResults.scanned_*`;
- related data size from `SearchResults.cost_aggregation`;
- top-k and recall-evaluation flags from raw results.

The QueryView path does not fill `queryChannelsNode` with old QueryNode leader
information. Search requery executes Query against the retained Search plans.

## 9. Requery Behavior

SearchPipeline binds internal requery to the parent's schema and `ShardPlan`s.
`Legacy().Query` with non-nil `Plans` dispatches directly to those exact work
nodes, View versions and MVCCs without Phase 1. It then performs the same
shard-local Query reduction. Failure aborts the parent operation; this path
cannot silently switch to a newer schema or snapshot. `RequeryOnView` remains
a separate future optimization.

## 10. Error Handling

The legacy domain returns errors instead of embedding them into raw result
slices.

For every `SearchOnViewResponse` or `QueryOnViewResponse`:

1. gRPC/queryclient errors are returned directly.
2. Missing legacy result is treated as an internal QueryView execution error.
3. Non-success legacy result status is converted with `merr.Error(status)`.
4. Retryable QueryView errors continue to be handled by the existing
   shard-level retry logic.

The Proxy task sees the final error exactly at the old `Execute` boundary.
`PostExecute` is only called after successful raw execution, same as the
existing `t.lb.Execute` path.

## 11. Compatibility With Existing Pipelines

`Legacy()` returns the same raw result types already consumed by Proxy:

| Proxy stage | Required input | Legacy output |
|---|---|---|
| QueryPipeline | `[]*internalpb.RetrieveResults` | `LegacyQueryResult.Results` |
| SearchPipeline | `[]*internalpb.SearchResults` | `LegacySearchResult.Results` |

No pipeline API change is required.

The node-side `ViewQueryService` must preserve the legacy result contracts
already expected by Proxy pipelines, including:

- result status;
- field data layout;
- `channels_mvcc`;
- scanned remote/total bytes;
- cost aggregation;
- top-k reduce flags;
- recall-evaluation flags.

## 12. First Implementation Steps

1. Add `Legacy()` domain interfaces and result/request types under
   `internal/views/queryclient`.
2. Add thread-safe legacy collectors for Search and Query raw results.
3. Implement `legacyClient.Search` and `legacyClient.Query` by reusing
   `shardViewQueryClient`.
4. Add a lightweight constructor for legacy-only QueryView clients.
5. Wire `queryTask.Execute` and `searchTask.Execute` to call `Legacy()` behind
   the QueryView feature gate.
6. Keep `PostExecute`, `SearchPipeline`, and `QueryPipeline` unchanged.
