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
- `RequeryOnView` remains part of the protocol, but node-side requery task
  planning and execution are TODO.
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
    ) (SearchSegmentTasks, error)

    AcquireQuerySegmentTasks(
        ctx context.Context,
        shardID qviews.ShardID,
        version qviews.QueryViewVersion,
        mvcc qviews.QueryPlanMVCC,
        req *internalpb.RetrieveRequest,
    ) (QuerySegmentTasks, error)
}
```

`AcquireRequerySegmentTasks` is intentionally not part of the first runnable
contract. Requery segment targeting and execution are deferred until the
Search/Query path is runnable.

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

First-version wait rules are view/runtime-level, not segment-level:

| Node | Wait rule |
|---|---|
| StreamingNode | wait growing runtime visibility >= `mvcc.growing_timetick` and transform-equivalent visibility >= `mvcc.transforming_timetick` |
| QueryNode | wait TransformBuffer visibility >= `mvcc.transforming_timetick` |

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
2. Reject any state except `Up`.
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

`UpRecovering` does not serve Phase 2 queries in the primary milestone.

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
5. Collect local sealed segment candidates from the view.
6. Run the operation-specific LocalOptimizer.
7. Acquire sealed segment handles for selected candidates.
8. Release the QueryView ref.
9. Return concrete QN Search/Query segment tasks.

The first implementation waits at node-local TransformBuffer visibility. It does
not wait per selected segment. Segment catch-up is part of resource preparation;
after catch-up, live TransformLog application is a vchannel/resource-level
broadcast. A later segment-level internal optimization must keep the same task
provider contract and must only expose a single TransformBuffer visibility
frontier to the query path.

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
tasks, err := provider.AcquireSearchSegmentTasks(ctx, shardID, version, mvcc, req)
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

`RequeryOnView` uses the same QueryView version and `QueryPlanMVCC` as the
preceding search/query. It exists to fetch fields for PKs selected after
Proxy-side reduce.

Node-side requery task planning and execution are TODO for the first runnable
implementation. Open design points include:

- whether Search results should carry PK-to-segment or PK-to-node location;
- whether Requery should broadcast to all planned work nodes and rely on local
  pruning;
- how much PK/bloom pruning belongs in the Requery LocalOptimizer.

Until this is implemented, `RequeryOnView` may be registered as a protocol
stub, but it is not part of the first node-side execution milestone.

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
8. Keep `RequeryOnView` as TODO/stub until Search/Query execution is runnable.
