# QueryView node execution boundary

This master-based extraction implements the StreamingNode query path and an
injectable [query client](query_client.md). QueryNode service/resource/native
implementation and production Proxy switching remain separate work. Scoped WAL
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
exact read resources are prepared by
`ReadSchemaKey` and do not impose a global read-schema watermark. Preparing an
older read schema cannot invalidate a newer View's readiness.

The ALTER component test exercises the native update, preservation of existing
segment encoding and the schema selected for new segments. The complete parent's
native initialization and legacy repeated-version recovery changes remain
separate prerequisites; these component checks do not establish full dynamic-schema
integration or legacy repeated-version recovery.

The existing SN scheduler executes `SearchTask` / `QueryTask.ExecuteOnSegments`
with segment adapters. It does not use the old QueryNode scheduler or delegate
historical work to SN. The client fans out the sealed portion separately.
This PR does not copy QN loaders, ReadState management or native implementation.

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
  Producers are supplied by the separate control-plane implementation.

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
The separately integrated QN provider must read `ReadableSealedSegment.QuerySegment()` so it uses the
same pinned ReadState as queries, never a new SegmentID lookup or a base's latest
state. It must wait for Transform on the exact registrations behind those handles. The RPC adapter is `StatisticsOnView`; errors use the existing status
conversion and successful responses contain only `row_count`.

SN invokes this operation while selecting the statistics View and returns the
result in `GetQueryPlanResponse.growing_inline_response.statistics_response`.
The client consumes that response before QN dispatch and discards it together
with partial QN results on a shard retry. QueryPlan stays reusable without
embedding execution results; no second execution request is sent to SN. See the
[statistics orchestration contract](query_client.md#runtime-row-statistics).

## Function model resources

Growing function-chain execution on SN resolves named XGBoost model resources
through the same demand-download manager used by Analyzer. A fresh name lookup
selects the immutable resource ID, then concurrent requests share its download
and model load. The file reference covers native model construction; the native
UBJ loader copies model state into owned memory. Queries retain a model lease
through prediction. Idle models expire after the configured file-resource cache
TTL; removal and shutdown cannot destroy an active model. Reusing a resource name
with a new ID never substitutes the model held by an executing query. QN/Proxy
without a pull manager retain their existing file-resource synchronization path.
