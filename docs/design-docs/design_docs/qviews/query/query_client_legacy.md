# Legacy Query Client Domain

## 1. Purpose

The Legacy domain is the first executable Query Client surface. It accepts the
same `internalpb.SearchRequest` and `internalpb.RetrieveRequest` shapes used by
the existing query pipeline and returns raw node results without performing
reduce, rerank, requery, or rendering.

This keeps the new two-phase orchestration independent from Proxy. A later Proxy
integration can consume these raw results using the existing processing
pipeline, but no Proxy code is part of this milestone.

## 2. API

The constructor returns a `Client` exposing only `Legacy()`:

```go
type Client interface {
    Legacy() LegacyClient
}

type LegacyClient interface {
    Search(context.Context, *LegacySearchRequest) (*LegacySearchResult, error)
    Query(context.Context, *LegacyQueryRequest) (*LegacyQueryResult, error)
}
```

Search and Query results contain:

- raw `internalpb.SearchResults` or `internalpb.RetrieveResults` from successful
  Phase 2 nodes;
- one successful `ShardPlan` per resolved vchannel, in vchannel order.

`ShardPlan` records the primary `ShardID`, QueryView version, MVCC frontiers,
and work nodes used by the successful attempt. It is execution evidence for the
caller; this milestone does not expose a requery operation.

## 3. Search

`Legacy().Search`:

1. validates the request wrapper and internal request;
2. resolves the collection's vchannels;
3. executes all shards concurrently;
4. sends the request through Phase 1 as
   `legacy_search_request`;
5. dispatches the optimized planned request to every work node;
6. validates each returned legacy status;
7. returns all raw successful results and shard plans.

The raw result order is unspecified because nodes and shards execute
concurrently. Callers must not associate results with shards by slice index.

## 4. Query

`Legacy().Query` follows the same flow with `legacy_retrieve_request` and
`QueryOnView`.

A successful result with no IDs, fields, or retrieve count is omitted. This
matches the existing raw-result behavior and avoids sending empty payloads into
the later reduce stage. Non-success status values are converted with
`merr.Error` and terminate the collection request.

## 5. Shard-aware Collectors

Search and Query use separate thread-safe collectors. Results are keyed by the
full `ShardID` string, not only by vchannel, because a retry may observe a new
primary replica.

Collector operations are:

- `Add`: validate and append one raw result;
- `ResetShard`: atomically discard one failed attempt's results;
- `Results`: return the currently retained raw results.

The collectors intentionally do not reduce. Adding a fake `Finish` method or a
placeholder reducer would imply functionality that the Legacy domain does not
provide.

## 6. Cancellation and Failure Semantics

Collection fan-out and per-shard work-node fan-out both use
`errgroup.WithContext`. A terminal failure cancels sibling operations and the
method waits for them to finish before returning.

For a retryable QueryView failure:

- Phase 1 failures retry without a collector reset because that attempt has not
  produced results.
- Phase 2 failures wait for all work-node calls, reset that shard, then resolve
  the primary and plan again.
- Retry exhaustion returns retriable `merr.ErrServiceUnavailable` with the
  QueryView error preserved as its cause.

Context cancellation and deadline errors are returned unchanged. Other untyped
dependency failures are classified as system errors at the public Legacy
boundary.

## 7. Test Contract

The package tests cover:

- multi-shard Search and raw results from both node types;
- non-empty and empty Query results;
- Strong, Session, Bounded, and Eventually primary planning semantics;
- growing versus transforming MVCC projection;
- malformed plans and invalid work nodes;
- Phase 2 partial-result rollback;
- retry exhaustion and primary re-resolution;
- nil requests, context errors, and typed merr propagation;
- concurrent collector access under the race detector;
- QueryView error gRPC round-trip;
- rejection of a picker result that is not the discovered primary.

Production discovery, transport, server handlers, Proxy E2E, and failover E2E
belong to their respective later integration changes.
