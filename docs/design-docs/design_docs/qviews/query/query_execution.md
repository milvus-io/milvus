# QueryView node execution boundary

This branch combines the StreamingNode query path, the injectable
[query client](query_client.md), and QueryNode query/resource preparation.
Production Proxy routing and Coordinator runtime activation are wired. Scoped WAL
barrier production and ordered delivery are provided by master. The plan's QN
workers use the existing ViewQueryService protocol through the client's injected
ViewQueryServiceClient.

## StreamingNode task acquisition

`SNQueryViewHandler` acquires the exact plan version in Up state. UpRecovering
waits for local state-change notifications with the caller context; missing or
invalidated views return the established View error. An accessed Up view renews
its [serving lease](../query_view_lease.md). Phase 2 borrows metadata without
copying a topology it does not use.

The handler validates the request's schema version against the leased View,
waits for the fixed Growing/Transform frontiers, and resolves the View's partition
scope through the required runtime interface and acquires growing segment handles.
Handles expose the selected exact read-schema CCollection, while physical segments retain their own encoding
schema. A query error releases every acquired handle. Successful tasks own them
through execution, reduction and result conversion; QueryView replacement cannot
revoke resources from a task already acquired.

CCollection serializes schema updates and publishes its Go schema cache only
after a successful native update. The native ordering token is a WAL timestamp,
separate from the logical SchemaVersion. The growing runtime reports the logical
schema version applied on the ordered write path through `WriteSchemaVersion`;
exact read resources are owned by each QueryView, sharing collection/schema and
field-semantic resources without imposing a global read-schema watermark. Preparing an
older read schema cannot invalidate a newer View's readiness.

Partition expansion captures the write schema with its WAL snapshot. The staged
runtime advances this schema only as it replays its own ordered ALTER events;
it never derives a new segment's encoding from the shared collection's current
schema. The serving dispatcher can already be ahead during snapshot loading.
Existing segments retain their allocation schema across both paths, so replaying
an Insert before a later DropField preserves that field for retained read views.

The ALTER component test exercises the native update, preservation of existing
segment encoding and the schema selected for new segments. The complete parent's
native initialization and legacy repeated-version recovery changes remain
separate prerequisites; these component checks do not establish full dynamic-schema
integration or legacy repeated-version recovery.

The existing SN scheduler executes `SearchTask` / `QueryTask.ExecuteOnSegments`
with segment adapters. It does not use the old QueryNode scheduler or delegate
historical work to SN. The client fans out the sealed portion separately.
QN loaders, ReadState management and native preparation are described below.

## Required interfaces

- Phase 1: QueryPlanService returns actual shard identity, View version,
  QueryPlanMVCC, optimized request and work nodes.
- Phase 2: ViewQueryService accepts SearchOnView / QueryOnView with those same
  identities. QN implementation must wait and execute on the bound resources.
- Fixed-snapshot coverage: TimeTickSyncInspector.EnsureBarrier certifies
  publication of an ordered complete WAL prefix after request-scoped TSO
  admission. The query runtime must then apply the fixed Growing/Transform
  frontiers. WAL-owner cancellation allows replanning on the replacement owner.
- Load scope: GetQueryViewLoadInfoResponse carries required_fields alongside
  load_fields, partition scope and indexes. Required fields define the logical
  query scope; load_fields supplies physical loading hints. The control-plane
  producer normalizes the user-facing skip-dynamic-field option into those
  hints, so the response needs no separate boolean. Skipping dynamic-field
  preloading does not remove that field from the logical query scope.
  The integrated coordinator supplies this metadata.

Retained requery uses QueryOnView with the original plan, workers, Schema and
MVCC. It does not select new resources through Phase 1 or use a separate requery
RPC.

## Error and validation boundary

View lifecycle errors retain their ViewCode and retry semantics. The query RPC
adapters attach common.Status details for business errors, preserving their
Milvus code/classification. The ViewError detail also retains the cause rendered
by the current client error formatter. Caller cancellation and deadlines retain
their existing meaning.

The [plan contract](query_plan.md) describes View-based schema identity,
fixed snapshots, Strong snapshots for CAS and payload reuse. Component tests
inject query workers and exercise barrier cancellation and fixed-snapshot
planning. Scoped barrier production and delivery have their own WAL tests; these
checks do not prove production Proxy/QN integration or cluster-level recovery.

Key source paths:
`internal/streamingnode/server/wal/snview/{query_lease,query_task_provider}.go`,
`internal/streamingnode/server/viewquery/`,
`internal/streamingnode/server/wal/vchannel/queryresource/read_schema.go`,
`internal/views/viewerror/rpc_error.go`.

## Physical row statistics

`TaskProvider.Statistics` is a lightweight resource operation. It acquires the
requested View and partition scope, waits for visibility, retains the concrete
segment handles, samples their existing `RowNum`, and releases every handle on
success, cancellation or error. It needs no query scheduler or expression plan.
The QN provider reads `ReadableSealedSegment.QuerySegment()` so it uses the
same pinned ReadState as queries, never a new SegmentID lookup or a base's latest
state. It waits for Transform on the exact registrations behind those handles. The RPC adapter is `StatisticsOnView`; errors use the existing status
conversion and successful responses contain only `row_count`.

SN invokes this operation while selecting the statistics View and returns the
result in `GetQueryPlanResponse.growing_inline_response.statistics_response`.
The client consumes that response before QN dispatch and discards it together
with partial QN results on a shard retry. QueryPlan stays reusable without
embedding execution results; no second execution request is sent to SN. See the
[statistics orchestration contract](query_client.md#runtime-row-statistics).

## Function model resources

Growing function-chain execution on SN uses the Analyzer demand-download manager
for XGBoost model cache misses. A local name index first checks the shared model
cache: a hit acquires a model lease without a Coord metadata RPC or file download.
A miss resolves the name through Coord, downloads the resource as needed and
shares model loading by immutable resource ID and path. The file reference covers
native construction; the UBJ loader copies model state into owned memory.

The model cache remains process-wide, shared with QN/Proxy. Nodes without a pull
manager keep their synchronized-resource lookup. Demand-model leases refresh
idle retention on release; idle expiry and synchronization-driven eviction remove
the model and its corresponding name index together. Active predictions retain
their model until their final lease is released. Cold concurrent misses can still
make separate metadata RPCs; file and model loads are shared.

TODO: Bind resource IDs during query planning across shards, stages and retries.
Until then, a warm name binding is not revalidated against Coord: deleting and
recreating a name may continue to use the cached model until eviction. This local
reuse does not guarantee one resource identity for the entire distributed query.

## Live resource dispatch concurrency

The PChannel recovery manager owns one shared live-event dispatcher. Its worker
count is `max(1, floor(hardware.GetCPUNum() * ratio))`, controlled by
`streaming.queryView.liveEventDispatchConcurrencyRatioPerPChannel` (default `0.25`).
Configuration updates resize existing pools; shrinking retires workers after their
current drain finishes. Each QueryRuntime still applies events serially, while
different runtimes may drain concurrently. Closing the dispatcher unregisters its
configuration watcher and joins its workers. This adds no per-VChannel worker and
does not change remote TransformLog delivery.

## Filter-only execution controls

SearchOnView optionally carries SearchExecutionOptions (FilterOnly and
EnableExprCache). The shared server attaches these request-local controls to
the provider's task owner; both SN and QN schedulers and executors pass them
to their native search adapters. Acquisition and release retain their existing
ownership.
Filter-first orchestration resides in QueryViewClient and uses the same View,
Schema, MVCC and selection in both rounds; workers do not replan or call peers.
See the [client flow](query_client.md#filter-first-ann-search).

> The following sections describe the QueryNode execution and resource binding
> added by this PR; see the [extraction boundary](../querynode_extraction.md).

# Query Execution Node-Side Design

## 1. Overview

This document describes node-side Phase 2 execution for QueryView search and
query.

Phase 2 receives a `QueryPlan` produced by Phase 1. Proxy sends `SearchOnView`
or `QueryOnView` to every work node in the plan. Each node turns the requested
QueryView version into concrete segment tasks, executes only those local tasks,
and returns a node-local reduced result to Proxy.

Phase 1 may omit a StreamingNode work node when it can prove that the selected
growing runtime is already visible at the requested MVCC and has no growing
segment candidates after request-scope filtering. This is only an optimization.
Phase 2 task acquisition remains the correctness boundary for any node that is
included in the plan.

The first runnable implementation focuses on the primary path:

- StreamingNode executes growing-side data for an `Up` QueryView.
- QueryNode executes sealed segments for a `Ready` QueryView.
- Requery uses `QueryOnView` with the original View version and MVCC.
- Secondary-specific catch-up behavior is postponed.

The central execution boundary is:

```text
QueryView -> concrete SegmentTasks -> scheduler execution
```

The QueryView reference is only used to derive and pin the segment tasks. After
tasks are returned, execution no longer depends on the QueryView lifecycle.

## 2. Segment Task Model

`SearchSegmentTasks` and `QuerySegmentTasks` are node-local execution inputs.
They are not proto messages and are not returned to Proxy.

Conceptual shape:

```go
type SearchSegmentTasks interface {
    Tasks() []SearchSegmentTask
    Release()
}

type QuerySegmentTasks interface {
    Tasks() []QuerySegmentTask
    Release()
}
```

Each concrete segment task contains:

1. a node-specific segment handle;
2. the request payload needed by that segment;
3. the `QueryPlanMVCC` read boundary chosen by Phase 1.

`QueryPlanMVCC` in a task is a read boundary, not a wait instruction. The task
provider has already waited until local resources can serve that boundary before
returning tasks.

The selected QueryView determines the exact read schema and accessible resources.
Search, Query and requery must match that schema, including version zero, and
execute at the supplied MVCC. There is no Phase 1 field allowlist or schema-binding
proof. A later write-schema change does not independently revoke fields in a
retained serving View; View lifecycle controls availability. Invalid field access
is rejected when planned or executed against the selected Schema and LoadScope.

StreamingNode and QueryNode do not need a shared `SegmentHandle` interface. They
have different resource models and execution adapters:

- StreamingNode tasks hold growing segment handles from the growing runtime.
- QueryNode tasks hold sealed segment handles from QueryNode resource managers.

The shared contract is lifecycle and correctness, not a common Go handle type.

## 3. Task Provider

`ViewQueryService` depends on a local task provider:

```go
type ViewQueryTaskProvider interface {
    AcquireSearchSegmentTasks(
        ctx context.Context,
        shardID qviews.ShardID,
        version qviews.QueryViewVersion,
        mvcc qviews.QueryPlanMVCC,
        req *internalpb.SearchRequest,
        selection *viewpb.SegmentSelection,
    ) (SearchSegmentTasks, error)

    AcquireQuerySegmentTasks(
        ctx context.Context,
        shardID qviews.ShardID,
        version qviews.QueryViewVersion,
        mvcc qviews.QueryPlanMVCC,
        req *internalpb.RetrieveRequest,
        selection *viewpb.SegmentSelection,
    ) (QuerySegmentTasks, error)
}
```

`AcquireRequerySegmentTasks` is not a separate provider API. Retained requery
uses `QueryOnView` with the original View, MVCC and recorded worker selection;
client-side requery orchestration remains outside this extraction.

SN and QN provide this interface through their local QueryView handlers:

- `SNQueryViewHandler` is WAL/PChannel-owned and provides tasks for
  StreamingNode growing data.
- `QNQueryViewHandler` owns QueryNode-side QueryView state and provides tasks
  for sealed segment data.

The task provider owns the full `QueryView -> segment tasks` transition:

```text
Acquire*SegmentTasks
  -> acquire QueryView ref
  -> validate servable state
  -> wait MVCC visibility
  -> collect local segment candidates
  -> run LocalOptimizer
  -> acquire selected concrete segment handles
  -> release QueryView ref
  -> return concrete SegmentTasks
```

The QueryView ref protects the view version, runtime, and membership while the
provider waits for MVCC and derives segment tasks. Once selected segment handles
are acquired, the QueryView ref is released. Physical segment release waits for
both QueryView lifecycle refs and search/query task refs to finish.

## 4. ViewQueryService Flow

`ViewQueryService` is a thin RPC adapter. It validates request shape, delegates
resource acquisition to the task provider, and sends ready tasks to the local
scheduler.

Search flow:

```text
SearchOnView
  -> validate proto request shape
  -> tasks := provider.AcquireSearchSegmentTasks(...)
  -> defer tasks.Release()
  -> scheduler.Search(ctx, tasks)
  -> return node-local reduced SearchResults
```

Query flow:

```text
QueryOnView
  -> validate proto request shape
  -> tasks := provider.AcquireQuerySegmentTasks(...)
  -> defer tasks.Release()
  -> scheduler.Query(ctx, tasks)
  -> return node-local reduced RetrieveResults
```

The service does not access QueryView state machines, runtime managers, segment
managers, or raw physical segment handles directly.

## 5. MVCC During Execution

`QueryPlanMVCC` is carried by the Phase 2 proto request alongside the legacy
search/retrieve request:

```proto
message QueryPlanMVCC {
    uint64 growing_timetick = 1;
    uint64 transforming_timetick = 2;
}
```

Phase 1 chooses these executor-local wait positions. Phase 2 task providers wait
until local node resources can serve them, then attach the same MVCC to every
returned segment task as the read boundary.

StreamingNode waits on its runtime; QueryNode validates visibility on the exact physical registrations retained for execution:

| Node | Wait rule |
|---|---|
| StreamingNode | wait growing runtime visibility >= `mvcc.growing_timetick` and transform-equivalent visibility >= `mvcc.transforming_timetick` |
| QueryNode | wait View buffer delivery, then each acquired physical registration >= `mvcc.transforming_timetick` |

QueryNode consumes `transforming_timetick`. StreamingNode consumes the full
`QueryPlanMVCC` through its growing runtime visibility contract so that
growing-side data and transform-equivalent effects are both visible before
execution. If a WAL message must affect both nodes before a query can execute,
Phase 1 advances both executor-local positions from that WAL message TimeTick.

After tasks are returned:

- scheduler does not wait for MVCC;
- scheduler does not re-check resource readiness;
- segment executors read at the MVCC boundary carried by the task.

If the request context is canceled while waiting, the provider returns context
cancellation and no tasks.

## 6. StreamingNode Task Acquisition

StreamingNode local execution is backed by the PChannel runtime prepared from
WAL state:

```text
SNQueryViewHandler
  -> selected Up QueryView ref
  -> PChannelRecoveryManager / VChannelRecoveryModule / QueryRuntime
  -> growing runtime visibility
  -> concrete SN segment tasks
```

First-version `Acquire*SegmentTasks` behavior:

1. Match request `shard_id` and `version`.
2. If the exact version is `UpRecovering`, wait for local state notifications
   within the request context, then recheck it. Reject other states except `Up`.
3. Acquire the QueryView/runtime ref.
4. Wait growing visibility to `mvcc.growing_timetick`.
5. Collect local growing segment candidates from the runtime and view version.
6. Run the operation-specific LocalOptimizer.
7. Acquire growing segment handles for selected candidates.
8. Release the QueryView/runtime ref.
9. Return concrete SN Search/Query segment tasks.

StreamingNode task acquisition only returns queryable growing segment handles.
Flushed segment markers retained by the growing runtime for WAL replay
idempotency are not task candidates and are never exposed to schedulers.

Phase 1 StreamingNode pruning and Phase 2 task acquisition must use the same
request-scope filter for growing segment candidates. In the first implementation
this scope is the request `partition_ids`. If Phase 1 could not prove the
filtered candidate set was empty, Phase 2 may still acquire zero handles after
waiting for MVCC; that is a valid empty local result, not an error.

The growing runtime may internally apply TransformLog-equivalent effects such as
Delete before reporting MVCC visibility. That is an internal runtime contract;
the task provider passes the full `QueryPlanMVCC` to the runtime and does not
perform separate segment-level waits.

Segments that have already been handed off to QueryNode are excluded according to
the QueryView version and the growing runtime's DataVersion rules.

`UpRecovering` does not execute Phase 2 queries. Requests wait for the exact
version to become Up; they do not pin resources or renew the serving lease while
waiting. Recovery failure, teardown or shutdown wakes them to return the existing
view error for replanning. Cancellation and deadline expiry terminate the wait.

## 7. QueryNode Task Acquisition

QueryNode local execution is backed by sealed segment resources prepared for the
QueryView:

```text
QNQueryViewHandler
  -> selected Ready QueryView ref
  -> TransformBuffer visibility
  -> QueryNode segment/resource managers
  -> concrete QN segment tasks
```

First-version `Acquire*SegmentTasks` behavior:

1. Match request `shard_id` and `version`.
2. Reject any state except `Ready`.
3. Acquire the QueryView ref.
4. Wait node-local TransformBuffer visibility to `mvcc.transforming_timetick`.
5. Intersect the exact View and requested partitions with the plan segment selection.
6. Run the operation-specific LocalOptimizer.
7. Acquire sealed segment handles for selected candidates.
8. Release the QueryView ref.
9. Return concrete QN Search/Query segment tasks.

The provider first waits for the View buffer delivery frontier. It then acquires
handles to the selected physical ReadStates and waits for the requested Transform
MVCC on those exact registrations. It must not wait on a binding and subsequently
look up the Segment again: a concurrent metadata refresh could substitute a
physical instance whose application was never checked. Remote delivery alone is
not proof of application. Failed acquisition releases every acquired handle.

Each task borrows its View's exact CCollection Schema, independent LoadScope and
pinned physical ReadState. The View resource operation reference retains the
Collection through queueing, native completion and result conversion, including
cancellation. Releasing a View cannot revoke an executing task's handles. See
[QN resource preparation](../qnview/querynode_queryview_resource_preparation.md).

Retained physical ReadStates own shared layouts and can keep native Schema/index
metadata alive after the creating View exits. Queries still use their own plan
Schema; the physical base's current state is never a query-schema source.

Phase 2 receives `SegmentSelection` from its worker plan. An absent selection
means `All`; `Include` and `Exclude` constrain IDs within the existing View and
request partition scope; `None` selects no sealed segments. Filtering preserves
the parallel manifest-version array and ready membership without changing the
retained View or releasing its resources. Exact-instance Transform MVCC and
poison checks still apply to the handles actually acquired.

Hybrid `sub_selections` are positional and must match the subrequest count when
present. Empty `All` messages retain their wire slot; skipped subrequests do not
shift subsequent selections. When absent, each subrequest uses the worker-level
selection. Empty task sets return the regular or hybrid empty result shape and
release their task owner. SN growing resources retain their runtime/partition
selection and are not pruned using sealed-segment selections.

## 8. Local Optimizer

LocalOptimizer belongs inside the task provider flow. It runs after the provider
has a stable view ref and before selected segment handles are acquired.

The provider performs two kinds of local planning:

1. Required membership filtering:
   - select segments owned by the requested QueryView version;
   - apply deterministic request scope such as partitions and explicit segment
     constraints;
   - exclude segments that cannot contribute to the request.
2. Optional local optimization:
   - segment pruning from local metadata or statistics;
   - future PK/bloom pruning;
   - first implementation can be no-op.

The optimizer input should be lightweight segment candidates, not pinned
physical segment handles. The optimizer output is the selected candidate list
that the provider converts into concrete segment handles.

Search and Query keep separate external acquisition methods and may use separate
operation-specific optimizers. They can share internal lifecycle helpers for
view ref, MVCC wait, candidate construction, and cleanup.

## 9. Scheduler and Executor

The scheduler consumes concrete segment tasks and trusts the task provider
boundary.

Scheduler responsibilities:

- segment-level concurrency;
- request context cancellation during execution;
- invoking concrete SN/QN segment execution adapters;
- node-local reduce;
- returning a node-local successful result or execution error.

Scheduler non-responsibilities:

- QueryView version lookup;
- QueryView state validation;
- MVCC wait;
- resource-ready assertions;
- LocalOptimizer or segment pruning;
- segment handle acquisition.

SN and QN may use different concrete scheduler adapters because their resource
handles and execution paths are different. The shared contract is that they
execute ready Search/Query segment tasks and reduce results locally before
returning to Proxy.

## 10. Release and Failure Cleanup

`Acquire*SegmentTasks` has an all-or-nothing ownership contract.

On success:

- the QueryView ref has been released;
- selected segment handles have been transferred to the returned
  `SegmentTasks`;
- the caller must call `tasks.Release()`;
- `tasks.Release()` releases the search/query lifecycle refs for all segment
  handles.

On failure:

- no tasks are returned;
- the QueryView ref is released inside the provider;
- any already acquired segment handles are released inside the provider.

`ViewQueryService` should therefore use this shape:

```go
tasks, err := provider.AcquireSearchSegmentTasks(ctx, shardID, version, mvcc, req, selection)
if err != nil {
    return nil, err
}
defer tasks.Release()

return scheduler.Search(ctx, tasks)
```

After tasks are returned, QueryView invalidation does not affect the current
execution. The task refs keep selected segment resources alive until
`tasks.Release()` runs.

## 11. Requery

Requery sends `QueryOnView` requests for the reduced PKs to the original plan's
work nodes, retaining its QueryView version, exact Schema and MVCC. Requery may
add output/render fields supported by that View and its LoadScope without declaring
them during Phase 1. Field errors fail the operation without replanning only the
refill, which would mix snapshots. There is no separate requery
RPC or node-side stub. Existing local PK pruning and Query reduction apply.

## 12. Error Semantics

Errors are returned through gRPC status details and converted by the query client
error layer. Response messages contain only successful results.

| Stage | Condition | Error class | Proxy behavior |
|---|---|---|---|
| RPC validation | Invalid request shape | Input error | Fail request. |
| Task acquire | View version missing | Retriable view error | Retry shard from Phase 1. |
| Task acquire | View state not servable | Retriable view error | Retry shard from Phase 1. |
| Task acquire | Local resource closing before tasks are returned | Retriable system error | Refresh resolver and retry shard. |
| Task acquire | MVCC wait canceled by context | Context error | Abort user request. |
| Scheduler | Context canceled during execution | Context error | Abort user request. |
| Scheduler | Segment execution failure | System error | Retry according to query client policy. |

`VIEW_INVALIDATED`, `VIEW_NOT_FOUND`, and `VIEW_NOT_SERVABLE` are provider-stage
errors only. Once `Acquire*SegmentTasks` returns successfully, scheduler does not
observe QueryView invalidation.

## 13. Client Boundary

The query client should not interact with raw RPC plumbing directly.

Client-side dependencies should be:

- StreamingNode client for `QueryPlanService` and StreamingNode
  `ViewQueryService`;
- QueryNode client for QueryNode `ViewQueryService`;
- a composite `ViewQueryServiceClient` that dispatches based on `WorkNode`.

This keeps `internal/views/queryclient` independent from transport details and
matches the existing component-client pattern.

## 14. Implementation Placement

Suggested package boundaries:

```text
internal/streamingnode/server/wal/snview/
  query_tasks.go              # SN ViewQueryTaskProvider implementation

internal/streamingnode/server/viewquery/
  server.go                   # SN ViewQueryService server
  scheduler.go                # SN Search/Query scheduler adapter
  tasks.go                    # SN concrete Search/Query segment tasks

internal/querynodev2/qnview/
  query_tasks.go              # QN ViewQueryTaskProvider implementation

internal/querynodev2/viewquery/
  server.go                   # QN ViewQueryService server
  scheduler.go                # QN Search/Query scheduler adapter
  tasks.go                    # QN concrete Search/Query segment tasks
```

The distributed wrappers register `ViewQueryService` on StreamingNode and
QueryNode. Query client implementations call these services through the
component clients.

## 15. First Milestone

The first node-side Phase 2 milestone is:

1. Add `AcquireSearchSegmentTasks` and `AcquireQuerySegmentTasks` to
   `SNQueryViewHandler`.
2. Add `AcquireSearchSegmentTasks` and `AcquireQuerySegmentTasks` to
   `QNQueryViewHandler`.
3. Implement view/runtime-level MVCC wait in task providers.
4. Add no-op LocalOptimizer hooks inside task acquisition.
5. Build concrete SN and QN Search/Query segment task containers.
6. Register `ViewQueryService` on StreamingNode and QueryNode.
7. Implement `SearchOnView` and `QueryOnView` RPC paths using provider plus
   scheduler.
8. Reuse QueryOnView for requery with the original View, Schema and MVCC.

## Partition statistics and pruning

[Global segment pruning](../query/segment_pruning.md) defines immutable DataView
statistics references, SN resource readiness and shared ownership, and per-worker
Plan selections. Scalar and vector clustering pruning run on SN during Phase 1;
QN applies the recorded selection before acquiring execution handles.

## Filter-only execution

SearchOnView optionally carries SearchExecutionOptions (FilterOnly and
EnableExprCache). The server attaches these request-local controls to the
provider's task owner; SN/QN schedulers pass them to the native search adapters.
Acquisition, exact-instance Transform waits and release are identical to normal
search. QN returns filter counts and segment identities through the existing
SearchResults envelope. The client alone orchestrates the two rounds; workers
do not call other workers, replan or select another View. See the
[filter-first client flow](query_client.md#filter-first-ann-search).
