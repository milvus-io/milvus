# Query Plan Node-Side Design

## 1. Overview

This document describes the StreamingNode-side implementation of Phase 1 in the
QueryView query path.

Phase 1 receives a shard-level query request from Proxy, chooses a valid
QueryView, obtains the MVCC read frontiers, runs global optimizers, and returns a
`QueryPlan` that Proxy can use to fan out Phase 2 requests to StreamingNode and
QueryNodes.

This design focuses on the primary query path first. Secondary-replica planning
and cross-replica catch-up are kept as protocol-compatible extensions, but they
are not required for the first runnable implementation.

## 2. Ownership

Phase 1 needs three PChannel-local facts:

| Data | Owner | Reason |
|---|---|---|
| QueryPlan MVCC frontiers | WAL adaptor / MVCC manager | MVCC is produced by the WAL path and is only comparable inside one PChannel. |
| Current query view | `SNQueryViewHandler` | The handler owns StreamingNode QueryView state for one PChannel. |
| Growing runtime and optimizer inputs | `PChannelRecoveryManager`, `VChannelRecoveryModule`, and runtime modules | Runtime modules are prepared from WAL recovery state and live WAL events. |

The component that already joins these three facts is `walAdaptorImpl`. Therefore
the complete Phase 1 plan generation should be implemented by `walAdaptorImpl`,
but the method should not be added to the generic `wal.WAL` interface.

The recommended internal interface is narrow and query-specific:

```go
type QueryPlanProvider interface {
    GetQueryPlan(ctx context.Context, req *viewpb.GetQueryPlanRequest) (*viewpb.QueryPlan, error)
    GetMVCCTimestamp(ctx context.Context, req *viewpb.GetMVCCTimestampRequest) (*viewpb.GetMVCCTimestampResponse, error)
}
```

`QueryPlanService` depends on this interface after it has located the local
PChannel WAL. The actual implementation is the PChannel-local `walAdaptorImpl`.

## 3. Internal QueryView Lease

`walAdaptorImpl.GetQueryPlan` acquires an internal, short-lived QueryView lease.
This lease is not a proto message and is not exposed to `QueryPlanService` or
Proxy.

The first version of the lease needs to hold:

```go
type QueryViewLease struct {
    Version qviews.QueryViewVersion
    Meta    *viewpb.QueryViewMeta
    View    *viewpb.QueryViewOfShard

    Release func()
}
```

The lease has three important properties:

1. It is acquired atomically from the PChannel QueryView state boundary.
2. It represents one Up QueryView version.
3. It keeps the selected QueryView stable until `Release` is called.
4. `View` is the complete shard view, including `QueryNode` sealed-segment
   topology and `StreamingNode` growing assignment.

Only the QueryView lease needs an explicit reference count. The optimizer does
not need a separate lifetime handle: `walAdaptorImpl.GetQueryPlan` creates the
optimizer from `wal/vchannel` while holding the selected QueryView lease, uses it
inside the same call, and then calls `Release` after the plan is built, even if
optimizer execution fails.

Although `SNQueryViewHandler` prepares only StreamingNode-local resources, it
must retain and persist the full `QueryViewOfShard` received from Coord. After a
StreamingNode restart, Phase 1 planning is recovered from the SN-local
WAL-bound QueryView meta; if the persisted view only contains
`QueryViewOfStreamingNode`, `build work_nodes from lease.View` can only produce
the StreamingNode work node and sealed QueryNode results are omitted.

## 4. View Selection

`SNQueryViewHandler` remains the owner of StreamingNode QueryView state. It should
expose a query-facing read method that can acquire the latest servable view for a
shard.

Rules:

- Only `Up` views can generate query plans.
- `UpRecovering` views do not serve queries.
- `Preparing`, `Ready`, `Down`, `Dropping`, `Dropped`, and `Unrecoverable` do not
  serve plans.
- If no Up view exists, `GetQueryPlan` returns a retriable view-not-ready or
  view-not-found error.

The handler returns a read lease of the selected view, not a mutable state
machine entry. The lease must protect the selected version from being released
while plan generation is using it.

## 5. MVCC Semantics

For the primary path, `walAdaptorImpl` obtains `QueryPlanMVCC` from its local
WAL/MVCC state. `QueryPlanMVCC` carries executor-local wait positions, not raw
message category frontiers:

```proto
message QueryPlanMVCC {
    uint64 growing_timetick = 1;
    uint64 transforming_timetick = 2;
}
```

The frontiers mean:

| Field | Meaning | Phase 2 consumer |
|---|---|---|
| `growing_timetick` | WAL TimeTick position that the corresponding StreamingNode shard's growing runtime must consume before serving growing queries. | StreamingNode. |
| `transforming_timetick` | WAL TimeTick position that TransformLog-equivalent consumers must observe before serving queries. | QueryNode TransformBuffer, and StreamingNode growing runtime when growing-side execution depends on transform-equivalent effects such as deletes. |

Both values must come from persisted WAL message TimeTicks. There is no
system-clock, local-wall-clock, or synthetic non-WAL MVCC source.

`GetQueryPlan` supports two protocol modes:

| Request mode | First-version behavior |
|---|---|
| `consistency_level` | Primary RW WAL obtains the latest local `QueryPlanMVCC`. |
| `query_plan_mvcc` | Use the caller-provided `QueryPlanMVCC` directly. |

For the first runnable implementation, only primary/RW WAL is required to produce
new MVCC. RO/secondary behavior can return `NOT_PRIMARY` or another retriable
view error until secondary planning is implemented.

`GetMVCCTimestamp` is a thin primary-only helper implemented by
`walAdaptorImpl` through `QueryPlanProvider`:

1. Resolve request `vchannel` to the local PChannel.
2. Verify this WAL is RW/primary.
3. Return the latest local `QueryPlanMVCC`.

When either current frontier is not confirmed, the WAL path can trigger
TimeTick sync and return the current read frontiers. Phase 2 is responsible for
waiting until local execution resources are readable at the requested
`QueryPlanMVCC`.

First-version generation rule:

```text
growing_timetick      = latest WAL TimeTick that must be visible in the
                       StreamingNode growing runtime before SN-side query
                       execution
transforming_timetick = latest WAL TimeTick that must be visible in the
                       QueryNode TransformBuffer before QN-side query
                       execution
```

Currently, Insert advances the StreamingNode growing wait position. Delete
advances both the StreamingNode growing wait position and the QueryNode
transforming wait position because both node types must observe the delete before
serving a query at or after that TimeTick.

TransformLog is the QueryNode-side projection of all WAL messages that advance
`transforming_timetick`. The source message categories and entry kinds are
defined by [TransformLog View Module](../../wal/transform_log_view_module.md).
Future Update and DeleteByExpr messages should advance the transforming wait
position through TransformLog payload entries.

## 6. Work Node Construction

`QueryPlan.work_nodes` is a wire-level plan field consumed by query client for
Phase 2 fan-out and requery. It is not part of the internal QueryView lease.

`walAdaptorImpl.GetQueryPlan` constructs `work_nodes` from the same Up view held
by the lease. This keeps view version, MVCC, optimizer access, and dispatch
targets consistent inside one PChannel-local critical path.

The first implementation should:

1. Include the StreamingNode work node from `QueryViewOfStreamingNode` unless
   growing-side execution is explicitly ignored or can be proven empty.
2. Include every QueryNode that has at least one selected sealed segment.
3. Apply request partition pruning before returning the plan, so Proxy avoids
   Phase 2 calls to nodes that cannot contribute results.

`work_nodes` must be built from the optimized request, not from the original
legacy request. Global optimizers may rewrite request scope, serialized plans,
or other planning inputs; node pruning must reflect the request that Phase 2
will actually execute.

QueryNode pruning is deterministic from the selected QueryView:

- If the request carries no `partition_ids`, keep a QueryNode when any
  partition in its view has at least one sealed segment.
- If the request carries `partition_ids`, keep a QueryNode only when one of the
  requested partitions exists in that QueryNode view and has sealed segments.

StreamingNode pruning is an opportunistic runtime optimization. After
`QueryPlanMVCC` is resolved and after the final request is available,
`walAdaptorImpl.GetQueryPlan` may ask the selected view's query runtime whether
the StreamingNode can contribute any growing segment at the requested MVCC.

The probe must be fail-open:

- If the request sets `ignoreGrowing`, omit StreamingNode.
- If the selected QueryView has no `QueryViewOfStreamingNode`, omit
  StreamingNode.
- If the query runtime is missing, still preparing, failed, or unavailable,
  keep StreamingNode.
- If the runtime has not already applied both MVCC frontiers required by the
  probe, keep StreamingNode.
- If the runtime cannot apply the requested segment filter, keep StreamingNode.
- Only when the runtime is ready, already visible at the target MVCC, and the
  filtered growing segment candidate set is empty, omit StreamingNode.

Flushed segment markers retained by StreamingNode for WAL replay idempotency are
not growing candidates. The probe must ignore these entries because they have no
queryable segment handle and exist only to prevent old replayed segment logs
from resurrecting already sealed data.

`GetQueryPlan` must not call the blocking Phase 2 wait path just to perform this
optimization. Planning should use a non-blocking "may have visible growing
segments" style probe; inability to prove emptiness means the plan remains
conservative and includes StreamingNode.

The optimizer does not need `work_nodes` as an explicit input. If an optimizer
needs distribution information later, it should obtain it from the vchannel-
provided optimizer implementation or from the selected QueryView it already owns
through the lease boundary.

## 7. Global Optimizer

Phase 1 is the global optimizer boundary. The first implementation can use a
no-op optimizer while preserving the right ownership shape.

`GlobalOptimizer` is provided by the `wal/vchannel` package inside
`walAdaptorImpl.GetQueryPlan`, after the latest Up QueryView lease has been
acquired. It is a call-scoped capability: the optimizer implementation closes
over the selected QueryView, the prepared query runtime, and runtime modules
such as the BM25 IDF oracle.

The service-facing interface stays simple:

```go
type GlobalOptimizer interface {
    OptimizeSearch(ctx context.Context, req *internalpb.SearchRequest) error
    OptimizeRetrieve(ctx context.Context, req *internalpb.RetrieveRequest) error
}
```

Neither `QueryPlanService` nor callers outside `walAdaptorImpl` should assemble
or pass `QueryViewStats`, `IDFOracleSnapshot`, `QueryRuntime`, or other resource
objects. Those are internal to the vchannel recovery module. `walAdaptorImpl.GetQueryPlan`
clones the request, calls the optimizer, and puts the optimized request into the
returned `QueryPlan`.

The expected first concrete implementation is BM25 IDF optimization backed by
the IDF oracle runtime. Other global optimizers, such as search parameter tuning,
can be composed inside the optimizer provided by `wal/vchannel` without changing
the `QueryPlanService` contract.

## 8. QueryPlanService Flow

`QueryPlanService.GetQueryPlan` is only an RPC adapter:

```text
QueryPlanService.GetQueryPlan(req)
  -> validate collection_id and shard_id
  -> locate local PChannel WAL by shard_id.vchannel
  -> assert WAL implements QueryPlanProvider
  -> plan := provider.GetQueryPlan(ctx, req)
  -> return GetQueryPlanResponse{plan}
```

`QueryPlanService.GetMVCCTimestamp` follows the same adapter rule:

```text
QueryPlanService.GetMVCCTimestamp(req)
  -> locate local PChannel WAL by req.vchannel
  -> assert WAL implements QueryPlanProvider
  -> resp := provider.GetMVCCTimestamp(ctx, req)
  -> return resp
```

`walAdaptorImpl.GetQueryPlan` owns the actual plan generation:

```text
walAdaptorImpl.GetQueryPlan(req)
  -> acquire latest Up QueryView lease from SNQueryViewHandler
  -> defer lease.Release()
  -> resolve QueryPlanMVCC from request mode
  -> get QueryRuntime for the selected QueryView, if it is ready
  -> create GlobalOptimizer from wal/vchannel using lease.View and QueryRuntime
  -> clone request
  -> run optimizer on cloned request
  -> build work_nodes from lease.View, optimized request, QueryRuntime, and MVCC
  -> build QueryPlan {
       lease.version,
       lease.shard_id,
       mvcc,
       work_nodes,
       optimized request,
     }
  -> return plan
```

The service should not independently call `SNQueryViewHandler`, `MVCCManager`,
or runtime modules. It only locates the local WAL and delegates to
`walAdaptorImpl`.

## 9. Error Semantics

Phase 1 errors should be retriable unless the request itself is invalid.

| Condition | Error class | Proxy behavior |
|---|---|---|
| Invalid shard or collection in request | Input error | Fail request. |
| No local WAL for PChannel | Retriable system error | Refresh resolver and retry shard. |
| WAL is shutting down or fenced | Retriable system error | Refresh resolver and retry shard. |
| No Up view | Retriable view error | Retry Phase 1 for the shard. |
| Requested primary MVCC on non-primary WAL | `NOT_PRIMARY` | Refresh primary mapping and retry. |
| View invalidated while building plan | Retriable view error | Retry Phase 1 for the shard. |

Runtime-side StreamingNode pruning must not introduce new user-visible errors.
If the pruning probe cannot prove that growing-side execution is empty, planning
keeps the StreamingNode work node and lets Phase 2 perform the normal MVCC wait
and segment acquisition.

Errors should be projected through the query-view RPC error conversion layer,
not embedded inside response messages.

## 10. Implementation Placement

Suggested package boundaries:

```text
internal/streamingnode/server/wal/adaptor/
  query_plan.go                # walAdaptorImpl implements QueryPlanProvider

internal/streamingnode/server/wal/snview/
  query_lease.go               # query-facing read lease for Up SN views

internal/streamingnode/server/wal/vchannel/
  query_optimizer.go           # lease-scoped GlobalOptimizer provider

internal/streamingnode/server/queryplan/
  server.go                    # QueryPlanService server
```

`QueryPlanService` is registered by the StreamingNode distributed/server layer.
The query client should call it through StreamingNode client infrastructure, not
by constructing raw RPC clients directly.

## 11. First Milestone

The first node-side Phase 1 milestone is:

1. Register `QueryPlanService` on StreamingNode.
2. Implement `walAdaptorImpl` as `QueryPlanProvider`.
3. Add `SNQueryViewHandler` query-facing lease acquisition for latest Up view.
4. Build primary-only `QueryPlanMVCC`, optimized request, and WorkNode list inside `walAdaptorImpl.GetQueryPlan`.
5. Return a valid `QueryPlan` with a no-op global optimizer.
6. Add fail-open StreamingNode work-node pruning for already-visible empty
   growing runtime candidate sets.
7. Keep secondary and advanced optimizer behavior behind explicit follow-up work.
