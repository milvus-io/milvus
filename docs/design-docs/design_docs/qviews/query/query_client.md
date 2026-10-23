# QueryView query client

## Component and integration boundary

`internal/views/queryclient` provides injectable two-phase execution. Its
`QueryPlanClient`, `ViewQueryServiceClient` and `ShardResolver` dependencies do
not depend on the QueryCoord balancer or load manager. `NewLegacyViewQueryClient`
accepts already planned internal requests and returns per-shard reduced results
and retained `ShardPlan`s. The surrounding Proxy pipeline owns API rendering.

This extraction adds the client and the SN execution support; it does not switch
Proxy Search, Query, Delete, Upsert or Iterator to QueryView. QN execution and native resource preparation are separate prerequisites and
are not imported into this master-based extraction. Coordinator runtime
activation, complete DataView publication and read-schema publication ordering
remain integration work. The generic `NewViewQueryClient` staged API also exists,
but its field-name-to-ID wiring still has TODOs; it is not the production entry.

## Planning, dispatch and retries

For each resolved vchannel, Phase 1 targets the primary StreamingNode using
`UnknownReplicaID`. The returned plan supplies the actual shard/replica identity.
Strong and Session are sent as Strong; Bounded and Eventually retain their enum
values. Strong/Session waits for completed writes; Eventually reuses the current complete
snapshot, and Bounded also enforces the request guarantee timestamp. All use primary-WAL planning. The presence of
`GetMVCCTimestamp` does not implement secondary replica selection.

Phase 2 sends `SearchOnView` or `QueryOnView` concurrently to every worker in the
plan. Requests retain the plan's View version and `QueryPlanMVCC`. The legacy
request timestamp is GrowingTimetick on SN and TransformingTimetick on QN.
The request is cloned for each worker. Empty successful retrieve responses do
not enter reduction, but the successful shard plan is retained even for an empty
worker set or an empty result. CAS consumers use the retained Strong snapshot too.

A retryable View error restarts the shard from Phase 1. The client first cancels
and joins all workers in that attempt, then clears the shard's partial results.
A response completing after cancellation therefore cannot leak into the next
attempt. Other shards retain their results. The caller context bounds all
attempts; errors outside the View retry policy are returned to the caller.

`handler.QueryViewClient` owns SN assignment lookup and RPC readiness handling.
WAL shutdown/NotPrimary errors wait for routing or readiness; View errors return
to query orchestration. gRPC WaitForReady applies to the query services.
`UpRecovering` waits inside the SN handler for local state notifications, under
the same caller deadline, rather than repeatedly generating plans.

## Reduction and retained requery

Legacy collectors partition responses by shard. Search reduction preserves
plain/hybrid sub-results, scan costs and recall flags. Retrieve reduction reuses
`segments.RunDelegatorQueryPipeline` with the request's immutable read schema.
These are additional callers of existing helpers; the old delegator is unchanged.

A non-nil `LegacyQueryRequest.Plans` (including an empty slice) selects retained
requery. It does not resolve a new shard, schema, View or MVCC. It sends the new
PK retrieve expression through `QueryOnView` to the original workers and retains
the original schema identity. A retained-plan failure aborts the parent operation;
replanning only the refill would mix snapshots with the original reduced search.
There is no separate requery RPC.

Requery may select additional output/render/rerank fields without declaring them
in Phase 1 or adding them to the initial search projection. The new request is
executed against the retained View and MVCC; field errors abort that operation
without replanning. See [plan and MVCC contracts](query_plan.md) and
[node-side execution](query_execution.md).

## Key packages and tests

- `internal/views/queryclient/{shard_client,legacy_client,legacy_reduce,requery}.go`
- `internal/streamingnode/client/handler/query_view_client.go`
- `internal/views/qviews/query_payload.go`
- `legacy_reduce_test.go`: reduction, fixed MVCC/barrier retries, late-response
  cleanup and empty-shard proof retention.
- `client_legacy_test.go`: retained requery without replanning.
- Handler `query_view_ready_test.go`: migration, readiness and cancellation.
