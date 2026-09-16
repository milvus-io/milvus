# MEP: List Running Requests and Cancel a Request by ID

- **Created:** 2026-09-16
- **Author(s):** @czs007
- **Status:** Draft
- **Component:** Proxy, RootCoord, QueryNode
- **Related Issues:** #53509, #53508, zilliztech/knowhere#1831
- **Released:** TBD

## Summary

Add two cluster-scoped RPCs:

- `ListRunningRequests` returns the Search, HybridSearch and Query requests
  currently executing on every proxy, with optional filters on database,
  collection, user and minimum elapsed time.
- `CancelRequests` cancels the requests identified by request id. Cancelling a
  request cancels its root `context.Context`, so every proxy sub-task and every
  QueryNode sub-request that belongs to that RPC stops together. The client
  receives a dedicated, non-retriable error code.

The server performs no automatic cancellation. There is no server-side maximum
execution time and no admission-time rejection based on request shape; how long
a request may run is decided by the client's SDK timeout, exactly as today.

The cancellation path from a client deadline down to segcore already exists and
is event driven. This design adds a cancel handle on the proxy, fixes two places
on that path that treat a cancellation as a node failure, and defines the RPCs,
routing, privileges and observability around it.

## Motivation

Operators cannot see which read requests are executing in a cluster and cannot
stop one of them. When a heavy request occupies a QueryNode, the options are to
wait for the client's own timeout, if the client set one, or to restart
components.

Every major database exposes this pair of operations: `SHOW PROCESSLIST` /
`KILL QUERY` in MySQL, `pg_stat_activity` / `pg_cancel_backend` in PostgreSQL,
`system.processes` / `KILL QUERY` in ClickHouse, the `_tasks` API with
`_cancel` in Elasticsearch, `currentOp` / `killOp` in MongoDB. The column set of
those views is close to identical: request id, node, type, database, user,
statement text, elapsed time, state. This design follows that shape.

## Current cancellation path

All line numbers refer to milvus master `82bcce8867c` (2026-09-11), knowhere
main `3e15e837` and cardinal `0b9f18bb`.

| Layer | Location | Behaviour |
|---|---|---|
| Proxy entry | `internal/proxy/impl.go:3016` (Search), `:3251` (HybridSearch), `:3886` (Query) | The task stores the gRPC request ctx as is. Nothing wraps it with `WithCancel`, so there is no cancel function to call. |
| Proxy scheduler | `internal/proxy/scheduler/task_scheduler.go:567-611` | `processTask` derives its ctx from `t.TraceCtx()` for all three phases; cancelling the task ctx cancels the whole execution. |
| Proxy to QueryNode | `internal/proxy/task_search.go:1540`, `task_query.go:1125` | The task ctx is the ctx of the gRPC call; cancellation aborts the RPC. |
| QueryNode scheduler | `internal/util/searchutil/scheduler/concurrent_safe_scheduler.go:99, 230, 352, 289` | The ctx is checked before enqueue, at dequeue and before execution. During execution the task body checks. |
| Delegator | `internal/querynodev2/delegator/delegator.go:1049-1082` | Sub-requests to workers use an `errgroup` ctx derived from the caller's; gRPC propagates cancellation to remote QueryNodes. |
| Segment level | `internal/querynodev2/segments/search.go:102, 154, 169` | The ctx is checked before each segment's cgo call. |
| cgo future | `internal/util/cgo/manager_active.go:99-123`, `futures.go:169-184` | A `reflect.Select` over every future's `ctx.Done` calls `C.future_cancel`. Event driven, not polled. |
| segcore | `internal/core/src/futures/Future.h:304-344`, `exec/QueryContext.h:444` | `future_cancel` requests cancellation on a folly `CancellationSource`; the token lives in `OpContext`. Eight operators call `checkCancellation` at the top of `GetOutput`; `ExprSet::Eval` checks once per 8192-row batch. |
| segcore to index | `internal/core/src/index/VectorMemIndex.cpp:822` | `OpContext` is passed to `knowhere::Index::Search`. |
| knowhere | `faiss_hnsw.cc:1444`, `ivf.cc:935`, `flat.cc:102`, `diskann.cc:915`, `index_node.cc:98-103` | Each per-query-vector task calls `checkCancellation`, which throws `folly::FutureCancellation`. The surrounding `catch (std::exception)` turns it into `faiss_inner_error` (zilliztech/knowhere#1831). |
| cardinal | `cardinal/index/index_impl.cpp:19-25, 50, 81`, `storage/translator/chunk_translator.h:169, 261` | Same per-query-vector check, also on chunk loads; also converted to `cardinal_inner_error`. |
| milvus status mapping | `internal/core/src/common/Utils.h:255-290` | Every knowhere `*_inner_error` maps to `KnowhereError` (2099), not `FollyCancel` (2038). |

Within one segment the interruption granularity is therefore one query vector:
a cancelled request stops after the query vector currently being searched. No
additional checks are added inside index search loops.

### Two places that treat a cancellation as a node failure

1. `internal/proxy/shardclient/lb_policy.go:447-475`: any error that is neither
   an `InputError` nor retriable adds the QueryNode to the channel blacklist
   for 30 seconds. `context.Canceled`, `FollyCancel` (2038) and `KnowhereError`
   (2099) all take this branch.
2. `internal/proxy/task_search.go:1543`, `task_query.go:1128`: any error
   invalidates the shard leader cache.

And one constraint: the completion channel in
`internal/proxy/taskmodel/condition.go:52` has capacity 1 and `processTask`
sends to it unconditionally in a `defer`. Calling `Notify` on an executing task
(as `clearQueuedTasks` does for queued tasks) blocks the worker forever. An
executing task can only be cancelled through its ctx.

### Facts about request identity

- One Search RPC can produce several tasks: `impl.go:2892` retries on
  `ErrInconsistentRequery` with a new `searchTask`; `search_pipeline.go:1337`
  enqueues a separate `queryTask` for the requery; `impl.go:3549` runs a
  `queryTask` to fetch vectors before a search by primary key. They share the
  parent ctx and have no ctx of their own.
- Task ids are allocated from TSO at `Enqueue`
  (`task_scheduler.go:213-236`); a request cancelled before `Enqueue` has none.
- HybridSearch is a single `searchTask` on the proxy; sub-requests are expanded
  inside the QueryNode.
- A search iterator holds no state on the proxy; each page is an ordinary
  Search.
- `WaitToFinish` (`condition.go:41-48`) returns as soon as the ctx is
  cancelled, while the task goroutine keeps running until its QueryNode call
  returns.

## Public Interfaces

### RPCs

```proto
// milvus.proto
message ListRunningRequestsRequest {
  option (common.privilege_ext_obj) = {
    object_type: Global
    object_privilege: PrivilegeListRunningRequests
    object_name_index: -1
  };
  common.MsgBase base = 1;
  string db_name = 2;          // empty: no filter
  string collection_name = 3;  // empty: no filter
  string user = 4;             // empty: no filter
  int64 min_elapsed_ms = 5;    // 0: no filter
}

message RunningRequestInfo {
  int64 request_id = 1;
  int64 proxy_id = 2;
  string type = 3;             // Search | HybridSearch | Query
  string db_name = 4;
  string collection_name = 5;
  string user = 6;
  string client_addr = 7;
  int64 nq = 8;
  int64 topk = 9;
  string expr = 10;            // first 256 bytes
  int64 start_time_ms = 11;
  int64 queued_ms = 12;        // queue time of the first task
  int64 elapsed_ms = 13;
  string state = 14;           // Queued | Running
  repeated int64 task_ids = 15;
  string trace_id = 16;
  bool cancellable = 17;       // always true for the three registered types
}

message NodeResult {
  int64 node_id = 1;
  common.Status status = 2;
  bool unimplemented = 3;
}

message ListRunningRequestsResponse {
  common.Status status = 1;
  repeated RunningRequestInfo requests = 2;
  repeated NodeResult node_results = 3;
}

message CancelRequestsRequest {
  option (common.privilege_ext_obj) = {
    object_type: Global
    object_privilege: PrivilegeCancelRequests
    object_name_index: -1
  };
  common.MsgBase base = 1;
  repeated int64 request_ids = 2;
  string reason = 3;
}

message CancelRequestsResponse {
  common.Status status = 1;
  repeated int64 cancelled = 2;
  repeated int64 not_found = 3;
  repeated NodeResult node_results = 4;
}
```

- The four `List` filters are combined with AND; all empty means everything.
  `db_name` and `collection_name` match exactly. Filtering happens on each
  proxy, so only matching rows travel to the coordinator.
- `Cancel` accepts request ids only. An empty list is an `InputError`. An id
  whose request is not cancellable is an `InputError`, not `not_found`.
- REST v2: `POST /v2/vectordb/requests/list` and
  `POST /v2/vectordb/requests/cancel`, wired through `wrapperPost` in
  `internal/distributed/proxy/httpserver/handler_v2.go`.

### Scope of the interfaces

The two RPCs are defined over "requests registered on the proxy". The names do
not restrict the request type; the `type` field says what each row is.
Currently only Search, HybridSearch and Query are registered, and all are
cancellable. Covering another type later only requires registering it in its
gRPC method; the interface does not change. A non-cancellable type would be
listed with `cancellable = false`.

Not registered now, and why:

- Insert / Upsert / Delete: the proxy splits them into per-shard messages
  written to the WAL. Cancelling mid-way cannot recall the channels already
  written, leaving a partial write. "Cancelled means it did not happen" cannot
  be promised.
- DDL: executed on the coordinator, not through the proxy task queue.
- Import: has its own task API.

### Error codes

- `ErrRequestCancelled = newMilvusError("request cancelled", 3002, false)`:
  the request was cancelled by an operator. Non-retriable.
- `ErrRequestNotFound = newMilvusError("request not found", 3003, false)`,
  classified as `InputError`.
- `oldCode()` gains a mapping for both so that older SDKs do not see
  `UnexpectedError`.

Why a new code: `CanceledCode` (10000) is synthesized from `context.Canceled`
and also appears when the client itself disconnects, so it cannot distinguish
"the client went away" from "an operator cancelled it".
`ErrSegcoreFollyCancel` (2038) is a QueryNode-internal error. The 3000 range is
the general range in `pkg/util/merr/errors.go`; 3000 and 3001 are taken.

### Privileges

- `ObjectPrivilege` gains `PrivilegeListRunningRequests = 91` and
  `PrivilegeCancelRequests = 92` (numbering per go-api v3.0.0).
- `pkg/util/constant.go`: both are added to `ObjectPrivileges[Global]`; List
  joins `ClusterReadOnlyPrivileges`, Cancel joins
  `ClusterReadWritePrivileges`, and both reach `ClusterAdminPrivileges` through
  the existing `append` chain. Without the last step `util.GetPrivilegeLevel`
  does not treat them as cluster level and a grant with `db="*"` does not
  match.
- The request messages carry the `privilege_ext_obj` annotation in the same
  form as `FlushAllRequest`; `rbac_annotation_coverage_test.go` enforces it.

## Design Details

### Request registry on the proxy

| Item | Decision |
|---|---|
| Unit | One client RPC. Not one task: a request owns several tasks, the tasks have no ctx of their own, and cancellation can only stop the whole tree. |
| Where | The first line of `Proxy.Search`, `Proxy.HybridSearch` and `Proxy.Query`. Not in `node.search` or `node.query`: the former is called repeatedly by the retry wrapper, the latter is shared by Query, requery and search by primary key; registering there would register one request several times. |
| Cancel handle | `ctx, cancel := context.WithCancelCause(ctx)`; `cancel` is stored in the record. The cause is `merr.ErrRequestCancelled` carrying the operator's user name and the reason. |
| Request id | `MetaCache.AllocID` (`internal/proxy/metacache/meta_cache.go:1595`), backed by `rowIDAllocator`, which prefetches a batch of ids from RootCoord and hands them out locally. Cluster-unique, independent of TSO, available at registration time. |
| Fields | request id, proxy id, type, db, collection, user, client address (gRPC peer), nq, topk, first 256 bytes of expr, start time, queue time of the first task, state (Queued / Running), task ids, trace id. Elapsed time is computed at list time. |
| Link to tasks | A pointer to the record is placed in the ctx. `Enqueue` appends the task id after allocating it; `AddActiveTask` sets the state to Running. |
| Removal | A `defer` right after registration removes the record and calls `cancel` when the gRPC method returns. Normal return, error return, panic unwinding and a client disconnect that cancels the ctx all go through it. After a cancellation the row disappears immediately; the QueryNode's wind-down is not reflected in the list. The ctx and any unfinished task still hold the pointer; the record is garbage collected when those tasks end. |
| Capacity | No separate limit. The record count equals the number of DQL requests in flight on the proxy, bounded by the gRPC server's concurrent request limit; once in the scheduler queue it is further bounded by `proxy.maxTaskNum` (1024). A request whose client set no deadline and which is stuck stays in the table; it is a running request, not a leak, and disappears as soon as an operator cancels it. |

### Cancellation semantics

- Before returning, each of the three gRPC methods checks `ctx.Err() != nil`
  and then `context.Cause(ctx)`. If the cause is `ErrRequestCancelled` it is
  returned to the client; otherwise the existing client-timeout behaviour is
  unchanged. Inner code (`WaitToFinish`, `retry.Handle`, `Enqueue`) keeps
  returning `ctx.Err()`; the conversion happens once at the outermost layer.
- **Load balancer decision by ctx state**: in `ExecuteWithRetry`, after
  `Exec` fails, check `ctx.Err() != nil` first and return without adding the
  node to the blacklist or excluding it from the request. Once the request is
  cancelled or timed out, whatever the QueryNode returned says nothing about
  node health. This removes the dependency on knowhere or cardinal returning a
  dedicated cancelled status.
- `searchShard` and `queryShard` likewise skip
  `InvalidateShardLeaderCache` when `ctx.Err() != nil`.
- `BaseTaskQueue.Enqueue` checks `ctx.Err()` before allocating a TSO, matching
  the QueryNode scheduler.

Cancelling a request cancels its root ctx, so:

- proxy sub-tasks already queued are rejected at `Enqueue` or fail at
  `processTask`;
- sub-tasks currently executing see their QueryNode RPC cancelled, and the
  QueryNode stops at the next check point;
- sub-tasks not yet created (a retry that has not happened) get an already
  cancelled ctx and are rejected at `Enqueue`.

The reverse does not hold: a failing sub-task does not cancel the parent; the
parent keeps its existing retry-or-fail logic.

### Cross-proxy routing

The path of `ClearReadTaskQueue` is reused: `internal/proxy/impl.go` calls
`mixCoord` (`internal/distributed/mixcoord/client/client.go:1858`),
`internal/rootcoord/root_coord.go:2902` calls every proxy in parallel through
`internal/util/proxyutil/proxy_client_manager.go:369-420`.

- `pkg/proto/root_coord.proto` (existing rpc at `:194`) and
  `pkg/proto/proxy.proto` (`:35`) each gain the two internal rpcs. Request and
  response reuse the milvuspb types (`proxy.proto` already references
  `milvus.GetMetricsRequest`); no new messages.
- `ProxyClientManager` gains two methods with the `ClearReadTaskQueue` result
  shape: one result per proxy, a failing proxy recorded in its own result while
  the others return normally, `ErrServiceUnimplemented` from an older proxy
  marked as `unimplemented`. The whole call has a 10 second timeout (as
  `SetRatesTimeout` in `quota_center.go:59`). An empty proxy list is an error,
  not a success.
- The `ProxyClientManager` instance lives on RootCoord (`mix_coord.go:176`,
  `:1277` both go through `rootcoordServer`); the field on MixCoord itself is
  not usable.
- The proxy that receives `List` or `Cancel` always goes through MixCoord to
  every proxy, itself included, and does not query its local registry
  separately; otherwise its own requests would appear twice. `List` results are
  merged by request id. `Cancel` is sent to every proxy; each reports found or
  not found.

### QueryNode: cancellation inside a merged search group

Queued search tasks on a QueryNode are merged into groups
(`internal/util/searchutil/scheduler/queues.go:175`,
`internal/querynodev2/tasks/search_task.go:408`). After merging, only the group
owner's ctx is consulted. Cancelling the owner makes `Done(ctx.Err())` deliver
the same error to every member, so requests that were never cancelled fail, and
their proxies, whose ctx is intact, blacklist the QueryNode for 30 seconds.
Cancelling a non-owner member is silently ignored; its vectors are still
searched. A client disconnect triggers this today; this design only makes it a
routine path. It is tracked as an independent defect in #53508.

**Rule: cancellation affects only the cancelled request. Other members of the
same group neither fail nor finish early.** What happens to the cancelled
request depends on one fact, whether the group has started executing:

| Moment | Cancelled request | Other members |
|---|---|---|
| Group not yet executing | Removed from the group at dequeue and completed immediately with its own `ctx.Err()`. If the owner was cancelled, the remaining members are regrouped and executed. | Execute and receive their results normally. |
| Group already executing | Its vectors are already part of the single segcore call and cannot be removed. The call runs to completion; at `Done` it receives its own `ctx.Err()`. Its share of CPU is wasted. | Receive their results normally. |

Implementation: at dequeue, filter members whose ctx is cancelled and `Done`
each with its own `ctx.Err()`; if the owner was cancelled, rebuild the group
from the remaining members via the existing `Merge`. Run the segcore call under
a ctx that is cancelled only when every member's ctx is cancelled
(`context.WithoutCancel(owner.ctx)` to keep trace values, plus
`context.AfterFunc` on each member's ctx decrementing a counter). At `Done`, a
member whose own ctx is cancelled receives its `ctx.Err()`; the others receive
the result. `MergeWith` refuses members whose ctx is already cancelled.

### Index layer status codes (independent, not a prerequisite)

knowhere and cardinal already check the cancellation token before each query
vector, but both report the cancellation as an engine inner error. The fixes
below make logs and metrics accurate; they are not required for cancellation to
take effect or for the proxy's decisions, which rely on ctx state.

- knowhere: add `Status::cancelled` and `StatusCategory::cancelled`; map it to
  `FollyCancel` in `ToSegcoreErrorCode`; catch `folly::FutureCancellation`
  before `std::exception` in each search path; let `GuardedCall` catch it as a
  last resort (zilliztech/knowhere#1831).
- cardinal: add `CardinalStatus::cancelled`, map it in `convertStatus`, and
  throw `FutureCancellation` from `chunk_translator` instead of
  `CardinalException`. After knowhere.
- milvus: map `cancelled` to `FollyCancel` in `Utils.h` when the knowhere
  version is bumped.

### Observability

| Item | Detail |
|---|---|
| Existing | `CancelRequests` is an ordinary RPC and appears in the access log. `QueryNodeReadTaskExecuteDuration{status=cancel}`, `QueryNodeReadTaskQueueDuration{expired}`, and the cgo counters `internal_cgo_cancel_before_execute_total_search` / `internal_cgo_cancel_during_execute_total_search` count operator cancellations together with client timeouts. |
| New counter | `milvus_proxy_request_cancelled_total{type}`, incremented when the registry actually cancels a request. This differs from the API call count: one `CancelRequests` may carry several ids, or find none. |
| Audit log | One line per cancellation: operator, request id, the cancelled request's user, collection and elapsed time, reason. The access log records who called `Cancel`, not whose request was cancelled. |
| Optional | Store `cancelledAt` in the record; in `PopActiveTask`, if the task ctx's cause is `ErrRequestCancelled`, observe the time from cancellation to the end of that task. The record may already be removed from the table; the pointer held in the ctx stays valid. |

## Compatibility, Deprecation, and Migration Plan

- Two new public RPCs, two new privileges and two new error codes; no existing
  API changes.
- During a rolling upgrade, requests on older proxies cannot be listed or
  cancelled; the response marks those nodes `unimplemented`.
- Until the knowhere and cardinal status fixes land, a cancelled request
  returns `KnowhereError` (2099) from the QueryNode and logs an inner error.
  Cancellation still takes effect and the proxy's blacklist decision is
  unaffected, because it keys on ctx state.
- SDKs must treat 3002 as non-retriable.

## Known limits

- Interruption granularity: operator boundary, segment boundary, every 8192
  rows of scalar filtering, and one query vector inside an index. The worst
  case latency is the time for one query vector to finish one search on one
  segment.
- When the client receives 3002 the QueryNode may still be winding down; the
  row is already gone from the list. The optional metric above measures this
  window.
- A search iterator can only be cancelled for its current page.

## Test Plan

- Unit tests: concurrent registration and removal in the registry;
  `lb_policy` neither blacklists nor excludes a node when the ctx is cancelled;
  `Enqueue` returns immediately on a cancelled ctx; the gRPC methods return
  3002; privilege grant and denial.
- Integration tests: brute-force search on a large growing segment, cancel,
  and observe QueryNode CPU dropping and the cgo cancel counters increasing;
  cancel from a proxy that is not executing the request in a multi-proxy
  cluster; one cancel stops a HybridSearch and a Search with a requery
  entirely; correct `unimplemented` marking with a mixed-version proxy set.
- Stress test: 1000 QPS with 50 cancellations per second; the blacklist stays
  empty, the shard leader cache does not churn, and neither the proxy nor the
  QueryNode read pool leaks.

## Delivery

| # | Repository | Content | Depends on |
|---|---|---|---|
| 1 | milvus | merr 3002 / 3003 and `oldCode`; `lb_policy`, `searchShard`, `queryShard` decide by ctx state; `Enqueue` checks ctx | none |
| 2 | milvus | registry; registration and cause conversion in the three gRPC methods; `Enqueue` / `AddActiveTask` linkage; counter and audit log | 1 |
| 3 | milvus-proto | two RPCs, messages, two privilege enums | none |
| 4 | milvus | internal rpcs in `root_coord.proto` and `proxy.proto`; `ProxyClientManager` methods; RootCoord implementation and MixCoord forwarding; proxy handlers with merge and dedup; privilege tables; REST v2; regenerated mocks | 2, 3 |
| 5 | milvus | QueryNode merged-group cancellation isolation (#53508) | none, independent bug fix |
| 6 | knowhere, cardinal, milvus | `cancelled` status, released in that order | none |
| 7 | pymilvus / Go SDK | two interfaces; 3002 not retried | 3 |

PRs 1, 2 and 5 do not touch proto and can be backported to 2.6 and 3.0.

## Rejected Alternatives

### Server-side maximum execution time

A server-side cap would wrap the request ctx with a deadline and cancel the
request automatically. It duplicates the client timeout, which already drives
the same cancellation path end to end, and adds a policy layer (global
parameter, per-collection override, precedence rules) whose value exists only
for clients that set no timeout. Rejected; request lifetime stays under the
client's control.

### Admission-time rejection by request shape

Rejecting requests at arrival based on nq × topk, unindexed filter fields or
similar features requires thresholds. A wrong threshold rejects legitimate
requests and the user has no recourse. Rejected.

### Cancel by task id

Task ids exist only after `Enqueue`, one request owns several, and a retry
changes them. A registry keyed by task would show one user request as two or
three rows, and cancelling one task cannot stop the others because they share
one ctx. Rejected in favour of one record per client RPC.

### Encode the proxy id into the request id

A locally generated id (`serverID << 48 | counter`) would let the coordinator
route a cancel to a single proxy. It introduces a second id scheme for a
benefit that does not matter at the proxy counts involved; broadcasting the
cancel to every proxy is cheap. Rejected in favour of the existing ID
allocator.

### Periodic cancellation checks inside index search loops

Adding checks every N hops in the graph traversal, IVF bucket scan or
brute-force chunk loop of knowhere and cardinal would refine the granularity
below one query vector. One query vector's search is millisecond scale; the
gain does not justify touching the search loops of every index type. Rejected.

### Keep the registry row until every task has ended

A "cancelling" state kept until the last `PopActiveTask` would show the
QueryNode wind-down in the list. It requires a per-record task counter and a
two-condition removal. Rejected for simplicity; the optional metric measures
the window instead.

## References

- #53509 feature issue
- #53508 merged search group cancellation defect
- zilliztech/knowhere#1831 cancellation reported as inner error
- `internal/proxy/scheduler/task_scheduler.go`, `internal/proxy/shardclient/lb_policy.go`,
  `internal/util/proxyutil/proxy_client_manager.go`,
  `internal/querynodev2/tasks/search_task.go`
