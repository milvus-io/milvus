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
that only replaces the raw query execution stage currently implemented by
`queryTask.Execute` and `searchTask.Execute`.

The integration boundary is:

```text
Proxy task
  -> queryclient.Legacy().Search/Query
       -> QueryView two-phase execution
       -> raw internalpb results
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

1. Keep the existing `queryclient.Search` and `queryclient.Query` code paths
   untouched.
2. Keep `SearchPipeline` and `QueryPipeline` untouched.
3. Replace only the raw execution currently driven by `t.lb.Execute`.
4. Return `[]*internalpb.SearchResults` and `[]*internalpb.RetrieveResults`
   directly to Proxy.
5. Let Search requery reuse the normal Query path instead of implementing
   `RequeryOnView` in the first version.

## 3. Non-goals

The first `Legacy()` implementation does not:

- implement reducer, reranker, renderer, or field-fetch planning inside
  `queryclient`;
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
}

type LegacyQueryResult struct {
    Results []*internalpb.RetrieveResults
    Plans   []ShardPlan
}
```

`Plans` are returned for observability and future requery optimization. The
first proxy integration only needs `Results`.

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

Search collector behavior:

```text
Add(shardID, SearchOnViewResponse)
  -> validate legacy_results status
  -> append legacy_results under shardID

ResetShard(shardID)
  -> drop all results collected for that shard

Results()
  -> flatten all shard buckets into []*internalpb.SearchResults
```

Query collector behavior is identical, using `QueryOnViewResponse` and
`*internalpb.RetrieveResults`.

`ResetShard` is required because the shard execution layer may retry a shard
after a retryable QueryView error. Without shard-scoped reset, partial results
from a failed attempt could be mixed with the successful retry.

Collectors are thread-safe because Phase 2 fanout calls `Add` concurrently.

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

`queryTask.PostExecute` remains unchanged. It still gathers
`t.resultBuf`, updates task-level counters and storage cost, and invokes
`QueryPipeline.Execute`.

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

`searchTask.PostExecute` remains unchanged. It still derives:

- `queryChannelsTs` from `SearchResults.channels_mvcc`;
- storage cost from `SearchResults.scanned_*`;
- related data size from `SearchResults.cost_aggregation`;
- top-k and recall-evaluation flags from raw results.

The first QueryView path does not fill `queryChannelsNode` with old QueryNode
leader information. Search requery is handled by executing a normal QueryView
query, not by preserving old leader-local requery affinity.

## 9. Requery Behavior

SearchPipeline requery currently builds a `queryTask` and calls `Proxy.query`.

Under the first QueryView integration, if `queryTask.Execute` is backed by
`Legacy().Query`, requery automatically uses the same QueryView Query path:

```text
SearchPipeline requery
  -> queryTask
  -> Legacy().Query
  -> QueryPipeline
  -> return fields to SearchPipeline organize stage
```

This is intentionally not optimized. It may query more shards than necessary,
but keeps requery correctness behind the existing Query path and avoids adding
`RequeryOnView` execution before the primary Search/Query path is stable.

Future optimization can use `LegacySearchResult.Plans` and implement
`RequeryOnView` against the original query plan.

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
