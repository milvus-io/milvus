# Query Client Design

## 1. Status and Scope

The Query Client is an internal, transport-neutral orchestration package for the
QueryView two-phase query path. It is intended to become a Proxy dependency, but
this milestone does not initialize it from Proxy or route production traffic to
it.

This milestone implements only:

- the QueryPlan and ViewQuery wire contracts;
- QueryView errors carried in gRPC status details;
- Primary-only shard selection;
- Legacy Search and Query execution returning raw `internalpb` results;
- shard-level retry for retryable QueryView errors;
- injected interfaces for discovery and node transport.

It does not implement:

- production shard discovery;
- StreamingNode or QueryNode connection management;
- node-side `QueryPlanService` or `ViewQueryService` handlers;
- secondary-replica selection or cross-replica MVCC;
- Proxy construction, task routing, feature gates, or fallback;
- client-side reduce, rerank, requery, or render stages.

The implemented code is under `internal/views/queryclient/`. The QueryView RPC
error projection is under `internal/views/viewerror/`.

## 2. Service Boundary

Query execution is divided into two data-plane phases. The services are declared
in `pkg/proto/view.proto` and are independent from the existing control-plane
`ViewSyncService`.

### 2.1 Phase 1: QueryPlanService

`GetQueryPlan` is addressed to the primary StreamingNode for one `ShardID`.
The request carries:

- collection and partition selection;
- primary `ShardID`;
- consistency level;
- a Legacy Search or Retrieve request.

The top-level `partition_ids` is the authoritative partition selection for
Phase 1 planning. The nested Legacy request retains the same field for wire
compatibility and later Phase 2 execution; callers construct both from the same
request and must not give them different meanings.

The response contains a `QueryPlan` with:

- the exact `ShardID` and QueryView version;
- growing and transforming MVCC frontiers;
- the complete list of StreamingNode and QueryNode work targets;
- a small Legacy plan delta containing only fields rewritten by node-side global
  optimization;
- an explicit `skip_execution` decision when planning proves the shard has no
  Phase 2 work.

The client rejects an empty plan, missing MVCC, a missing version, a mismatched
shard, an invalid work node, and duplicate work nodes before Phase 2 starts. A
StreamingNode target requires a nonzero growing frontier and a QueryNode target
requires a nonzero transforming frontier. Their relative ordering is not
validated because a valid QueryView transition may make transforming newer than
growing. An empty work-node list is accepted only with `skip_execution=true`,
and a skipped plan must not also contain work nodes.

The deprecated full-request response fields remain readable for rolling upgrade
compatibility. New Phase 1 implementations return `legacy_search_plan` or
`legacy_retrieve_plan` and do not echo the potentially large original request.

### 2.2 Phase 2: ViewQueryService

`SearchOnView` and `QueryOnView` are addressed to every work node in the plan.
Each request carries the planned QueryView version and both MVCC frontiers.
The Legacy request's `mvcc_timestamp` is projected per node:

| Work node | Legacy `mvcc_timestamp` |
|---|---|
| StreamingNode | `growing_timetick` |
| QueryNode | `transforming_timetick` |

The client does not convert a planned MVCC frontier into
`guarantee_timestamp`. Node-side Phase 2 handlers own visibility waiting:
StreamingNode waits for the growing frontier, while QueryNode waits for the
transforming frontier. This keeps planning consistency separate from Legacy
request guarantee semantics.

The Query Client does not own connections or endpoint discovery. A
`ViewQueryServiceClient` implementation receives a typed `qviews.WorkNode`; the
provided composite dispatcher can route it to injected StreamingNode and
QueryNode subclients.

## 3. Primary-only Routing

`ShardResolver` returns the collection's vchannels and the current primary
`ShardID` for each vchannel. `PrimaryReplicaPicker` validates that the snapshot
contains a usable primary. The shard executor additionally verifies that the
picker did not return a different replica, so Primary-only is an execution
invariant rather than a constructor convention.

All consistency levels plan on the primary:

- Strong remains Strong.
- Session is sent to Phase 1 as Strong because MVCC must be derived from the
  primary WAL; the Legacy request still carries its original session metadata.
- Bounded and Eventually retain their requested consistency level.

`GetMVCCTimestamp`, caller-provided MVCC, secondary selection, and replica-aware
StreamingNode routing are deliberately absent from this protocol milestone.

## 4. Execution Flow

For each collection request, the Legacy client performs the following steps:

1. Resolve all vchannels for the collection.
2. Execute each vchannel concurrently.
3. Resolve the current primary snapshot at the start of every shard attempt.
4. Select and validate the primary `ShardID`.
5. Call `GetQueryPlan` on that primary.
6. Validate the returned plan.
7. Clone the caller's Legacy request, apply the plan delta, and project its MVCC
   for every work node.
8. Fan out Phase 2 calls concurrently with `errgroup.WithContext`.
9. Add successful raw results to a shard-aware collector.
10. Return raw results and the successful `ShardPlan` for each vchannel.

An explicit skipped plan returns an empty result for that shard. Every successful
Phase 2 response is retained, including a Retrieve response with no IDs or
fields, because it may still carry cost or other metadata.

## 5. Retry and Result Isolation

The following QueryView codes trigger a complete shard retry from Phase 1:

- `VIEW_CODE_VIEW_INVALIDATED`;
- `VIEW_CODE_VIEW_NOT_FOUND`;
- `VIEW_CODE_ON_SHUTDOWN`;
- `VIEW_CODE_NOT_PRIMARY`.

After a Phase 2 retryable failure, the collector removes all results accumulated
for that attempt's `ShardID` before re-resolving the primary. Results from other
shards remain intact. `errgroup` waits for all canceled Phase 2 goroutines before
the reset, preventing late writes from repopulating the failed attempt.

Ordinary transport retry belongs to the future concrete node clients. A
non-QueryView error is terminal for the current shard execution.

`ViewQueryClientConfig.MaxAttempts` is the maximum number of complete shard
attempts. Values below one use the default of three attempts. Retryable
QueryView failures use bounded exponential backoff configured by
`RetryInitialBackoff` and `RetryMaxBackoff`; cancellation interrupts the wait.

## 6. Error Boundary

Nodes attach `viewpb.ViewError` to gRPC status details. `viewerror.ConvertViewError`
preserves context cancellation, deadline, and EOF while retaining other gRPC
status information for QueryView detail extraction.

Retryable QueryView exhaustion and retryable QueryView errors become retriable
`merr.ErrServiceUnavailable`; non-retryable QueryView errors become
`merr.ErrServiceInternal`. gRPC `Unavailable` and gRPC `DeadlineExceeded`
without QueryView details also become `merr.ErrServiceUnavailable`. Untyped
dependency or invariant failures become `merr.ErrServiceInternal`. Existing
typed merr and local context errors pass through unchanged. Therefore the public
Legacy client boundary does not leak an unclassified QueryView error.

## 7. Injected Interfaces

The initial client depends on four interfaces:

- `resolver.ShardResolver`: collection-to-vchannel and vchannel-to-primary data;
- `ReplicaPicker`: primary selection, constrained by the executor;
- `QueryPlanClient`: Phase 1 transport;
- `ViewQueryServiceClient`: Phase 2 transport.

No implementation in this milestone reads StreamingCoord assignments, creates
gRPC connections, or references Proxy tasks.

## 8. Deferred Integration Sequence

The remaining work should land independently in this order:

1. authoritative, versioned primary shard discovery;
2. StreamingNode and QueryNode transport adapters with connection lifecycle and
   ordinary transient-error retry;
3. node-side Phase 1 and Phase 2 services;
4. guarded Proxy integration with the existing query path as fallback;
5. secondary-replica routing and cross-replica MVCC;
6. complete client-side reduce, rerank, requery, and render stages.

Each layer must be independently testable and must not imply that a later layer
is already available.
