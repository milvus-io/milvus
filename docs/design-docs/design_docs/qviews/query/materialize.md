# QueryView Materialize

## Scope and execution path

Materialize is an optional Arrow IPC transport for search field refill. It uses
the same retained query plans and resource acquisition as `QueryOnView`.
`proxy.materialize.enabled` defaults to false. Ordinary Query/Get and unsupported
retrieve shapes continue through Query. Eligibility excludes limit/offset,
iterators, aggregation, grouping, ordering, element-level filtering and output
types unsupported by the existing IPC encoder.

```text
Proxy SearchPipeline
  -> reduce search results; create PK retrieve expression
  -> QueryTask with the parent's exact Schema and retained ShardPlans
  -> Legacy().Materialize
       -> original SN workers: MaterializeOnView -> pinned growing tasks
       -> original QN workers: MaterializeOnView -> pinned sealed tasks
            -> local PK/TS reduce -> Arrow IPC encoding
       <- one internal.MaterializeResults per worker
  -> existing materializeFill: deduplicate, order by search IDs, fill FieldData
```

The client fans out to the workers recorded in each original `ShardPlan`, not
every node in the cluster. The same reduced PK expression goes to each worker;
the retained plan does not locate individual PKs. There is no new Phase 1,
replica selection, schema resolution or worker-to-worker RPC during refill.

## Request contract

`ViewQueryService.MaterializeOnView` is parallel to `QueryOnView` and directly
returns the existing `internal.MaterializeResults`. Its request separates the
execution implementation from IPC options:

```proto
message MaterializeOnViewRequest {
    oneof request {
        QueryOnViewRequest query_by_pk = 1;
    }
    MaterializeIPCOptions ipc_options = 2;
}

message MaterializeIPCOptions {
    uint64 schema_fingerprint = 1;
    int32 format_version = 2;
}
```

`query_by_pk` carries the existing `QueryOnViewRequest`: `shard_id`, `version`,
`mvcc`, `segment_selection`, and `legacy_req`. Proxy builds a PK-IN
`planpb.PlanNode` and serializes it in `legacy_req.serialized_expr_plan`; output
field IDs and the exact read SchemaVersion travel in that retrieve request.
The RPC therefore carries an executable retrieve expression as well as routing
and snapshot identity. A future execution implementation can add a separate
oneof branch without changing the meaning of `query_by_pk`.

IPC options have distinct purposes:

| Value | Meaning | Effect |
| --- | --- | --- |
| `schema_fingerprint` | Identity of the Arrow response Schema already cached by Proxy; zero means no cached Schema. | A matching worker response omits Schema bytes; a miss sends the Schema with its fingerprint. It does not select a View or replace collection SchemaVersion. |
| `format_version` | Version of the Materialize response encoding contract, currently 1. | Workers reject unsupported formats so the whole refill can use Query. It does not select a oneof branch or identify a Milvus release. |

The current oneof and options are an unpublished contract. Intermediate draft
layouts have no adapters, reserved draft field numbers or compatibility tests.
Capability/encoding rejection of a real request still uses the normal fallback
contract described below.

## Snapshot and pruning

Refill retains the parent's exact Schema, View version, worker list and paired
`QueryPlanMVCC` frontiers. These frontiers come from one complete WAL publication
prefix; they are not independently sampled executor timestamps.

| Stage | StreamingNode growing | QueryNode sealed |
| --- | --- | --- |
| Acquisition | Wait for the retained Growing/Transform frontiers in the selected runtime. | Wait for the retained Transform frontier on the exact bound resources. |
| Native retrieve | Read at GrowingTimetick. | Read at TransformingTimetick. |

The retained client sets `legacy_req.mvcc_timestamp` to the worker's frontier.
The node adapters also rebuild that scalar from the authoritative outer
`QueryPlanMVCC` before native execution, exactly as Query does. They do not
trust an independently supplied legacy scalar. The maximum of the two frontiers
is returned as channel MVCC metadata; it is not substituted for either worker's
read timestamp. See [the MVCC contracts](query_plan.md#three-distinct-timestamp-contracts).

Materialize inherits the recorded per-worker `SegmentSelection`, including
All/Include/Exclude/None. Hybrid search retains the worker's union selection,
so refill can cover PKs from all reduced sub-searches. It does not rerun the
original vector-statistics pruning against the PK expression. QN applies the
recorded sealed selection before pinning handles; SN growing uses its retained
runtime, DataVersion and partition scope. Existing local membership/PK filtering
still applies. See [segment pruning](segment_pruning.md).

## Parallel node execution and ownership

The shared server validates `query_by_pk`, the retrieve shape and IPC options,
then calls the same `AcquireQuerySegmentTasks` provider used by `QueryOnView`.
This preserves exact-version SN Up/QN Ready admission, schema checks, MVCC
waiting, partition/selection filtering and concrete segment pins.

`Scheduler.Materialize` is a formal operation alongside Search and Query on the
existing scheduler and executor interfaces. There is no separate optional
Materialize scheduler or client capability interface. `MaterializeTasksWithOptions`
borrows the acquired query task owner to pass IPC options to the executor.

SN and QN directly invoke the existing `NewMaterializeTask` /
`ExecuteOnSegments` path on the acquired growing/sealed handles. Native PK/TS
reduction and IPC encoding are reused. Materialize requests Arrow output even
when the ordinary `common.interface.zeroCopy` switch is disabled. The service
releases its task owner only after native completion, reduction and IPC encoding;
Arrow records are released by the existing query task cleanup. Pins are
reacquired for refill against the retained View, not held continuously since the
original search.

[Serving lease closure](../query_view_lease.md#explicit-collection-teardown-control)
rejects new acquisitions during collection release. It does not cancel admitted
calls. SN's execution handles retain the View resource operation guard and exact
schema until native completion and encoding; QN retains its View/base operation
references. Materialize uses these existing protections through the shared
acquisition path.

The response stays unnested so the existing resource codec can transfer pooled
IPC bodies without copying them into another response envelope. Request
compression uses the normal gRPC configuration. The response uses gRPC identity
compression; `queryNode.materialize.codec` and `zstdMinBodySize` control the
existing none/zstd IPC encoding instead.

## Proxy assembly, input budget and fallback

`LegacyClient.Materialize` takes a local `consume(plan, worker, result)` callback.
It is not an RPC parameter. It may run concurrently and transfers each successful
response body's ownership to Proxy's existing `materializeFill`. All fan-out
workers join before the client returns. Failed or over-budget responses release
their bodies immediately; the fill owns and eventually releases accepted bodies.

Workers reduce PK/TS locally. Proxy selects the newest candidate for each PK
within a shard, assembles fields in the original reduced-search ID order, and
preserves the existing error for cross-shard duplicate PKs. Scan/cost counters,
retrieve counts, feature bits and channel MVCC remain available. An empty retained
worker list produces a successful empty shard proof without replanning or an RPC.

The client sums successful worker `Header.ResultSize` values per shard as an
input budget. This sum precedes cross-worker deduplication: an upsert may return
both an old sealed row and a new growing row. Exceeding that budget requests
whole-refill Query fallback, rather than declaring the user's deduplicated output
too large. Native per-worker output errors keep their original codes; Query
reduction and Proxy fill still enforce the actual output quota.

Unsupported implementations, encoding formats, retrieve shapes or field schemas
return `ErrServiceUnimplemented`. After all workers finish, Proxy drops
all partial Materialize bodies and reruns the entire refill via `Legacy().Query`
on the same retained plans, Schema, MVCC and selections. It never mixes partial
Materialize and Query results. A required fallback survives sibling execution
errors; caller cancellation takes precedence. Corrupted IPC is an error, not a
reason to hide invalid data with fallback. A retained View failure propagates to
the parent search for retry; only the refill cannot select a fresh snapshot.

## Difference from the earlier Materialize RPC

| Concern | Earlier non-QV path | QueryView path |
| --- | --- | --- |
| Routing | Proxy -> Delegator.Materialize -> worker.MaterializeSegments | Proxy retained client -> original SN/QN MaterializeOnView workers |
| Read admission | Delegator distribution and read admission | Exact View/Schema, dual MVCC and recorded worker selection |
| Execution | Worker query task through the old QueryNode scheduler | Existing QV direct executor on pinned growing/sealed tasks |
| Response relay | Delegator combines headers and relays IPC bodies | Each worker replies directly to Proxy |
| Shared mechanisms | Native PK/TS reduction, IPC/resource codec and Proxy field fill | Reuses the same mechanisms |

The old non-QV route remains available for its existing hosts. It is not used as
an intermediate hop by `MaterializeOnView`.

## Key packages and validation

- `pkg/proto/view.proto`: request variants and IPC options.
- `internal/proxy/dql/{task_query,materialize_fill}.go`: eligibility, whole-refill
  fallback, ownership and result assembly.
- `internal/views/queryclient/materialize.go`: retained fan-out and input budget.
- `internal/views/viewquery/{materialize,tasks,scheduler}.go`: shared RPC and task
  ownership boundary.
- `internal/{streamingnode/server,querynodev2}/viewquery/materialize.go`: parallel
  growing/sealed executors.
- `internal/querynodev2/tasks/materialize_on_segments.go` and `internal/util/materialize/`:
  reused native execution and IPC encoding.
- Business-package unit tests cover request round trips, snapshot/selection
  preservation, ownership, concurrent failure and fallback. Tests stay outside
  generated `viewpb`.
- `tests/integration/materialize/`: enabled feature integration test. SDK checks
  should additionally compare Query and Materialize on mixed growing/sealed
  upserts, deletes, nullable/JSON/vector fields, none/zstd encoding, and duplicate
  input larger than the budget while deduplicated output fits.
