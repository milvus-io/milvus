# QueryView query client

## Component and integration boundary

`internal/views/queryclient` provides injectable two-phase execution. Its
`QueryPlanClient`, `ViewQueryServiceClient` and `ShardResolver` dependencies do
not depend on the QueryCoord balancer or load manager. `NewLegacyViewQueryClient`
accepts already planned internal requests and returns per-shard reduced results
and retained `ShardPlan`s. The surrounding Proxy pipeline owns API rendering. A generic ViewQueryClient
and its Legacy facade share one shard client and replica router; the owning client
closes that router once. Independently constructed Legacy clients own independent
routers.

This extraction adds the client and the SN execution support; it does not switch
Proxy Search, Query, Delete, Upsert or Iterator to QueryView. QN execution and native resource preparation are separate prerequisites and
are not imported into this master-based extraction. Coordinator runtime
activation, complete DataView publication and read-schema publication ordering
remain integration work. The generic `NewViewQueryClient` staged API also exists,
but its field-name-to-ID wiring still has TODOs; it is not the production entry.

The injected QN transport must opt into `contextutil.WithFailFastPick`, disable
hidden same-node gRPC retries, and wrap a missing session with
`viewerror.NodeSessionUnavailable`. Capture its `Changed` channel before dispatch
so a racing resolver update cannot be lost. The QN session observer/transport
implementation and Proxy ownership wiring remain in the parent integration;
this branch supplies their contracts and verifies retry behavior through injection.
Inject `streaming.WAL().QueryViewClient()` as QueryPlanClient: it combines
owner-routed query RPCs with the streaming-owned resuming replica client.
`HandlerClient.QueryViewClient()` provides only query RPCs and no longer satisfies
the discovery contract by itself. The parent Proxy must adapt this injection
when absorbing the extraction. Query clients close their logical subscriptions
before the shared WALAccesser shuts down; they do not close that shared owner.

## Planning, dispatch and retries

For each resolved VChannel, the client retains one replica subscription from the
primary StreamingNode. Its callback updates a local route snapshot and wakes
queries without per-VChannel workers. The client round-robins explicit ReplicaIDs;
there is no UnknownReplicaID fallback. Invalid/uninitialized and authoritative
empty directories wait for discovery under the request context. `Client.Close`
unsubscribes and wakes waiters before the underlying handler clients shut down.
TODO: reclaim idle route subscriptions together with Proxy metadata GC.

Phase 1 still targets the primary WAL owner. The advertised maximum Up version
is only a routing-change hint; SN selects the highest Up View matching the request
schema within the requested replica. The returned plan supplies the exact version.
Strong and Session are sent as Strong; Bounded, Eventually and Customized retain their enum
values. Strong/Session waits for completed writes; Eventually reuses the current complete
snapshot, and Bounded also enforces the request guarantee timestamp. Customized
preserves its guarantee as a logical snapshot lower bound, reusing the same
complete-prefix optimization as BarrierMVCC and establishing a scoped barrier
only when coverage is insufficient. All use primary-WAL planning. The presence of
`GetMVCCTimestamp` does not implement WAL-secondary routing.

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
attempts; non-retryable errors are returned to the caller.

`handler.QueryViewClient` owns SN assignment lookup and RPC readiness handling.
WAL shutdown/NotPrimary errors wait for routing or readiness; View errors return
to query orchestration. SN services retain WAL readiness handling. The injected QN transport is required to use
an explicit fail-fast picker mode and disable hidden same-node gRPC retries;
ordinary WAL RPCs keep their existing picker behavior.
`UpRecovering` waits inside the SN handler for local state notifications, under
the same caller deadline, rather than repeatedly generating plans.

QN transport Unavailable and retryable View errors trigger shard replanning.
Every current candidate is tried before repeating a temporarily failed one.
The existing attempt budget defaults to three, extended to cover all untried
current candidates; exhausting it returns the original failure to Proxy's outer
recovery (including AutoLoad). Waiting for an empty/uninitialized directory does
not consume attempts. After all temporary candidates fail, wait 100 ms or a
route change before retrying. Request cancellation bounds all waits.

A QN missing from the session resolver is not retried on that timer. Record the
node and the attempted replica/advertised version; try other replicas, or wait
for a new maximum Up version or QN session-directory revision. A newly planned
View still containing a known missing QN is rejected before accepting inline SN
results or dispatching workers. The parent QN client uses a single shared session observer to broadcast
resolver revisions without copying topology or creating per-query watches.
Capture its change channel before RPC dispatch; resolver changes racing with
failure cannot be lost. On a revision, clear request-local exclusions and
reconsider candidates: apparent absence may have been discovery lag, rather
than permanent removal. Missing/temporarily disconnected nodes always fail
promptly at the QN picker; no dead RPC is kept waiting for readiness.

Retries clear partial results only after canceled workers have joined. Statistics
also clears its inline SN count. Explicit paired MVCC and BarrierMVCC remain
unchanged across replicas; ordinary consistency may resample on the same primary
WAL. Other successful shards are retained.

For [segment pruning](segment_pruning.md), each worker receives its own selection
and hybrid subqueries retain separate selections. Requery uses the original
selection union. Successful empty shards still contribute an empty result and
MVCC; fieldless aggregation results use the schema-aware identity (for example,
count=0). Only the successful attempt's ExecutionSummary contributes feature usage.

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

## Runtime row statistics

The extracted `Legacy().Statistics` component is available to future Proxy
callers. Production Proxy routing and the QN provider implementation remain
separate integration work; this PR adds the SN provider and shared contracts.

For each shard the client sends a `statistics_request` through `GetQueryPlan`.
The caller supplies its normalized timestamp as the separate `barrierMVCC`
argument to `Legacy().Statistics`; the client reuses the outer `barrier_mvcc`
field and preserves it across retries. A zero argument uses the shared ordinary
Strong sampling path. `StatisticsRequest` contains only the partition scope.
SN selects the latest serving Up View (physical statistics do not select a
logical read schema), waits for the requested consumption lower bound, resolves
the View's partition scope, and counts its retained growing resources in the
same call. `GetQueryPlanResponse.growing_inline_response.statistics_response`
contains the growing row count, including an explicit response for zero rows.
The separate `plan` contains the selected View/version, MVCC frontiers, resolved
partitions and QN workers only. There is no follow-up SN execution RPC and no
expression/index optimizer work for statistics. The inline-response oneof can
later carry other SN result types without adding result fields to QueryPlan.

The client then calls `StatisticsOnView` on those QNs and sums their row counts
with the SN result. The QN provider contract requires waiting for Transform
application on the exact acquired physical registrations and counting the
View's pinned ReadStates. SN uses the same DataVersion-based growing exclusion
as queries; QN uses the selected View's sealed membership. One replica per shard contributes, including during handoff.

These are runtime `RowNum` statistics with applied deletes, not a historical
MVCC snapshot, a TTL-filtered `count(*)`, or a globally atomic row count. The
barrier ensures that changes through B are applied; its execution wait targets
may be lower where the complete prefix proves no intervening changes. Later
applied writes/deletes can still be reflected by RowNum. This reuses the existing scoped barrier path,
including Transform advancement, without a statistics-specific freshness policy. No new periodic row-count reporting or Coordinator aggregation
is introduced. Growing reuses its existing native `GetRealCount`; sealed reads
use the existing physical deletion-cardinality counter.

A retryable QN View failure cancels and joins that shard's workers, discards
both the SN sample and all partial QN counts, and restarts at SN. Other shards
retain successful results. Empty and growing-only shards still retain the SN
result even when there are no QN workers. Errors use existing View RPC status
handling; the new response carries only a successful row count.

## StreamingNode replica discovery

[Replica Discovery](replica_discovery.md) defines the resumable, PChannel-multiplexed
VChannel subscriptions exposed by StreamingNodeHandlerService. It supplies each candidate ReplicaID
and its maximum locally Up QueryViewVersion, including updates when only the
version changes. These versions are routing hints, not pinned query versions;
QueryViewClient uses this directory for explicit selection and failover as
described above. Discovery itself does not choose or pin a query View.
