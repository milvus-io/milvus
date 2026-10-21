# QueryView node execution boundary

This master-based extraction implements the StreamingNode query path and an
injectable [query client](query_client.md). QueryNode service/resource/native
implementation, scoped WAL barrier production and production Proxy switching
remain separate work. The plan's QN workers use the existing ViewQueryService
protocol through the client's injected ViewQueryServiceClient.

## StreamingNode task acquisition

`SNQueryViewHandler` acquires the exact plan version in Up state. UpRecovering
waits for local state-change notifications with the caller context; missing or
invalidated views return the established View error. An accessed Up view renews
its [serving lease](../query_view_lease.md). Phase 2 borrows metadata without
copying a topology it does not use.

The handler validates the request's schema binding against the leased View,
waits for the fixed Growing/Transform frontiers, resolves the View's partition
scope and acquires growing segment handles. Handles expose the selected exact
read-schema CCollection, while physical segments retain their own encoding
schema. A query error releases every acquired handle. Successful tasks own them
through execution, reduction and result conversion; QueryView replacement cannot
revoke resources from a task already acquired.

The complete parent also updates the CCollection Go schema cache/native revision
on ALTER and restores legacy repeated-version segment schemas using their
allocation time. Those prerequisite implementations are not copied here:
master's existing CCollection and recovery interfaces are retained. The ALTER
component test verifies the schema/timestamp forwarded to that interface and
preservation of existing segment encoding; it does not claim dynamic-schema
integration or legacy repeated-version recovery works before those prerequisites
land.

The existing SN scheduler executes `SearchTask` / `QueryTask.ExecuteOnSegments`
with segment adapters. It does not use the old QueryNode scheduler or delegate
historical work to SN. The client fans out the sealed portion separately.
This PR does not copy QN loaders, ReadState management or native implementation.

## Required interfaces

- Phase 1: QueryPlanService returns actual shard identity, View version,
  QueryPlanMVCC, schema binding, optimized request and work nodes.
- Phase 2: ViewQueryService accepts SearchOnView / QueryOnView with those same
  identities. QN implementation must wait and execute on the bound resources.
- Fixed-snapshot coverage: QueryBarrierInspector.EnsureBarrier certifies an
  ordered complete WAL prefix. Absence of this optional capability is an explicit
  not-ready error when existing frontiers cannot serve the snapshot.
- Load scope: GetQueryViewLoadInfoResponse carries required_fields and
  skip_load_dynamic_field alongside the existing scope and indexes. Producers
  are supplied by the separate control-plane implementation.

Retained requery uses QueryOnView with the original plan, workers, Schema and
MVCC. It does not select new resources through Phase 1. The existing RequeryOnView
wire method stays available for compatibility and is not used by the new client.

## Error and validation boundary

View lifecycle errors retain their ViewCode and retry semantics. The query RPC
adapters attach common.Status details for business errors, preserving their
Milvus code/classification for new clients while keeping the original ViewError
detail readable. Caller cancellation and deadlines retain their existing meaning.

The [plan contract](query_plan.md) describes field dependencies, exact schema
validation, fixed snapshots, CAS certificates and payload reuse. Component tests
inject query workers and the optional barrier provider; they do not prove that
the separate QN or WAL implementation has been integrated.

Key source paths:
`internal/streamingnode/server/wal/snview/{query_lease,query_task_provider}.go`,
`internal/streamingnode/server/viewquery/`,
`internal/streamingnode/server/wal/vchannel/queryresource/read_schema.go`,
`internal/views/viewquery/schema_fields.go`, and
`internal/views/viewerror/rpc_error.go`.
