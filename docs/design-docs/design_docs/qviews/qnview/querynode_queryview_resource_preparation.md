# QueryNode QueryView Resource Preparation Design

> References: [Distributed Query View Design](../README.md),
> [QueryView State Machine Per-Node Analysis](../query_view_state_machine.md),
> [QueryView Handler Design](../query_view_handler.md),
> [view.proto](../../../../../pkg/proto/view.proto), and
> [query_coord.proto](../../../../../pkg/proto/query_coord.proto).

## 1. Goal

This document describes the QueryNode-side resource preparation workflow for a
QueryView pushed in `Preparing` state. The workflow starts when QueryNode
acquires its local part of the QueryView and ends when QueryNode reports local
readiness or unrecoverability.

The workflow includes:

1. applying the incoming QueryView on QueryNode;
2. pinning collection runtime for the view;
3. loading assigned sealed segments from object storage;
4. registering loaded sealed segments with TransformLog and waiting for catch-up;
5. reporting incremental segment readiness to the local QueryNode state machine;
6. releasing view-scoped references after the view is dropped.

## 2. Readiness Definition

For this workflow, local QueryNode `Ready` means:

1. the collection runtime required by the QueryView is pinned locally;
2. every assigned sealed segment is physically loaded;
3. every loaded segment is registered with TransformLog;
4. every registered segment has caught up to the QueryView transform frontier.

After local `Ready`, QueryNode keeps the view-scoped resources until the same
view is applied as `Dropped`.

## 3. Component Responsibilities

The QueryNode entry point wires the resource managers as:

```text
QueryNode.NewQueryViewSegmentManager
  -> QueryViewSegmentReadinessManager
       -> ViewScopedPhysicalSegmentManager
            -> QueryViewSegmentLoadScheduler
                 -> QueryViewLoadMetadataProvider.GetQueryViewSegmentLoadInfo
                 -> PhysicalSegmentLoader.Load
       -> TransformLogBuffer
       -> QueryViewCollectionRuntimeManager
```

| Component | Responsibility |
|---|---|
| `QNQueryViewHandler` | Applies incoming QueryViews, owns per-shard QueryNode state machines, and calls `SegmentManager.Acquire` or `SegmentManager.Release`. |
| `QNQueryViewStateMachine` | Tracks local `Preparing`, `Ready`, `Unrecoverable`, `Dropping`, and `Dropped` states. Deduplicates incremental ready segment reports. |
| `QueryViewSegmentReadinessManager` | Pins TransformLog and collection runtime, tracks transform-level view/segment refs, registers loaded segments, waits catch-up, and reports segment readiness. |
| `ViewScopedPhysicalSegmentManager` | Tracks physical segment refs by QueryView, submits missing segment loads, validates late callbacks, and waits for in-flight load callbacks during release. |
| `QueryViewSegmentLoadScheduler` | Fetches DataCoord-packed segment load info, updates collection index meta, reserves resources, and invokes the physical loader. |
| `QueryViewLoadMetadataProvider` | Provides `DescribeCollection` and `GetQueryViewSegmentLoadInfo` through MixCoord/QueryCoord. |
| `QueryViewCollectionRuntimeManager` | Pins local collection runtime using collection schema/load metadata and exposes index meta update for segment load. |
| `TransformLogBuffer` | Pins the view-level transform range and registers loaded sealed segments for catch-up. |

## 4. Authoritative Acquire Execution Order

This section is the single source of truth for the current QueryNode Acquire
order. Later sections explain individual stages and contracts without changing
this sequence.

```text
Incoming QueryView(Preparing)
  -> QNQueryViewHandler.ApplyViews
       -> create QNQueryViewStateMachine
       -> SegmentManager.Acquire
            -> QueryViewSegmentReadinessManager.Acquire
                 -> TransformLogBuffer.Acquire
                 -> QueryViewCollectionRuntimeManager.Acquire
                      -> QueryViewLoadMetadataProvider.DescribeCollection
                      -> collectionManager.PutOrRef
                 -> record transform refs and waiters
                 -> report already transform-ready segments, if any
                 -> report empty OnReady if this QN has no assigned segments
                 -> ViewScopedPhysicalSegmentManager.Acquire for missing segments
                      -> record physical refs
                      -> if segment is missing:
                           -> QueryViewSegmentLoadScheduler
                                -> QueryViewLoadMetadataProvider.GetQueryViewSegmentLoadInfo
                                -> CollectionRuntimeGuard.UpdateIndexMeta
                                -> SegmentResourceEstimator.Reserve
                                -> PhysicalSegmentLoader.Load
                      -> if segment is already physically loaded:
                           -> reuse loaded segment
                      -> physical OnLoaded callback
                 -> TransformLogBuffer.RegisterSegment
                 -> TransformRegistration.WaitCatchup
                 -> OnReady(partitionID -> segmentIDs)
       -> QNQueryViewStateMachine.OnSegmentsReady
       -> OnReport(QueryView Ready or incremental Preparing progress)
```

Important ordering rules:

1. QueryNode first records the local view state machine, then starts resource
   preparation through `SegmentManager.Acquire`.
2. TransformLog guard and collection runtime are acquired before physical
   segment loading is submitted.
3. Collection runtime acquisition uses `DescribeCollection`; segment loading
   later uses `GetQueryViewSegmentLoadInfo`.
4. Physical load completion is not QueryView readiness. A segment becomes
   QueryView-ready only after TransformLog registration and catch-up.
5. QueryNode may report incremental `Preparing` progress before it reaches
   `Ready`; the final local `Ready` report is produced only when all assigned
   segments are ready.
6. QueryNode does not receive `Up` or `Down` transitions.

## 5. QueryNode State and Readiness Contract

QueryNode's local state flow is:

```text
Normal: Preparing -> Ready -> Dropping -> Dropped
Error:  Preparing -> Unrecoverable -> Dropping -> Dropped
```

`OnReady` is incremental and carries a `partitionID -> segmentIDs` delta. The
state machine deduplicates segment IDs and counts all assigned segments for the
`Preparing -> Ready` transition.

Segment readiness accounting:

1. every assigned segment blocks local `Ready`;
2. partition IDs are only the report grouping key for ready segment deltas;
3. if the QueryNode has no assigned segments, `Acquire` reports an empty
   `OnReady` so the state machine can advance.

Callback liveness contract:

1. every `Acquire` must eventually invoke `OnReady` or `OnUnrecoverable`;
2. `OnReady` may be invoked multiple times for incremental progress;
3. `OnUnrecoverable` is terminal for the local view while it is `Preparing`;
4. every `Release` must eventually invoke `OnDropped` exactly once;
5. callbacks must be asynchronous relative to the `Acquire` or `Release` call.

## 6. Collection Runtime and Segment Metadata Boundary

`qnview.QueryViewLoadMetadataProvider` deliberately exposes separate collection-level and
segment-level APIs:

```go
type QueryViewLoadMetadataProvider interface {
    DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
    GetQueryViewSegmentLoadInfo(ctx context.Context, collectionID int64, segmentIDs ...int64) ([]*querypb.SegmentLoadInfo, []*indexpb.IndexInfo, error)
}
```

Collection runtime acquisition uses `DescribeCollection` to get schema,
database name, required fields, and schema barrier timestamp. QueryNode pins the
local collection runtime through `collectionManager.PutOrRef` before any segment
load task is submitted.

Segment loading uses `GetQueryViewSegmentLoadInfo`, defined on QueryCoord proto:

```proto
message GetQueryViewSegmentLoadInfoRequest {
    common.MsgBase base = 1;
    int64 collectionID = 2;
    repeated int64 segmentIDs = 3;
}

message GetQueryViewSegmentLoadInfoResponse {
    common.Status status = 1;
    repeated SegmentLoadInfo infos = 2;
    repeated index.IndexInfo index_info_list = 3;
}
```

There is intentionally no `load_priority` request field. Load priority is not a
caller-controlled dimension for QueryView resource preparation. DataCoord packs
the returned `SegmentLoadInfo` for QueryView loading.

The production call path is:

```text
QueryNode lazyQueryViewLoadMetadataProvider
  -> MixCoord client
  -> QueryCoord.GetQueryViewSegmentLoadInfo
       -> health check
       -> mixCoord.GetQueryViewSegmentLoadInfo
            -> DataCoord.GetQueryViewSegmentLoadInfo
```

QueryCoord and DataCoord are colocated in the coordinator process, so the
MixCoord hop calls the DataCoord server directly inside the process instead of
adding a coordinator-to-coordinator gRPC round trip.

DataCoord owns the segment-level packing:

1. validate health, collection ID, and requested segment IDs;
2. return empty success for an empty segment list;
3. fetch collection-level index definitions once;
4. fetch segment index metadata for requested segment IDs;
5. validate each segment belongs to the requested collection;
6. recalculate row count when needed;
7. pack `querypb.SegmentLoadInfo` with binlogs, deltalogs, stats logs,
   manifest path, index info, storage version, data version, commit timestamp,
   and other physical-loader inputs;
8. return collection index info so QueryNode can update local collection index
   meta before loading.

The RPC supports multiple segment IDs. The current scheduler still submits one
load task per segment, so it usually calls the RPC with a single segment ID. A
future scheduler can batch this call without changing the ownership boundary:
DataCoord remains the single owner of the complete segment-level load snapshot.

## 7. Physical Load Stage

`ViewScopedPhysicalSegmentManager` is responsible for physical ref accounting and
load task submission. It maintains:

1. `views`: local QueryView refs and callbacks;
2. `segments`: physical segment state by segment ID;
3. `cancels`: view-level cancellation functions.

Acquire behavior:

1. record or replace the view ref;
2. add the QueryView key to each assigned segment's physical ref set;
3. create load state only for segments that are missing or reset;
4. submit load tasks only for segments that are not already loading or loaded;
5. if all requested segments are already physically loaded, call `OnLoaded`
   with those segments.

Load task behavior:

1. call `GetQueryViewSegmentLoadInfo(collectionID, segmentID)`;
2. require exactly one returned `SegmentLoadInfo` for the current per-segment
   task;
3. update local collection index meta with the returned index definitions;
4. reserve resources through the optional estimator;
5. call `PhysicalSegmentLoader.Load`;
6. wrap the segment with `TransformStartAfterTimeTick` if the QueryView meta has
   a delete-apply start timetick;
7. report the loaded segment back to the physical manager.

On physical load completion, the physical manager validates that the segment is
still referenced before keeping it. If no QueryView still references the
segment, the late result is released and ignored.

## 8. Transform Registration and Catch-Up Stage

`QueryViewSegmentReadinessManager` turns physically loaded segments into
QueryView-ready segments.

For each physically loaded segment:

1. mark the segment as physically loaded if it is still referenced;
2. register it with `TransformLogBuffer`;
3. store the registration and catch-up cancellation function;
4. wait for `TransformRegistration.WaitCatchup`;
5. mark the segment transform-loaded;
6. notify all waiting QueryViews through `OnReady`.

If another QueryView references a segment that is already transform-loaded, the
transform manager reports it ready immediately without reloading or
re-registering the segment.

If registration or catch-up fails:

1. cancel catch-up;
2. unregister from TransformLog if a registration exists;
3. release the loaded segment if present;
4. reset the physical segment state through `PhysicalSegmentResetter`;
5. notify affected QueryViews with `OnUnrecoverable`.

Resetting the physical state lets a later QueryView acquire retry the segment
from the beginning instead of reusing a partially registered segment.

## 9. Release Flow

When a view is applied as `Dropped`, QueryNode enters local `Dropping` and calls
`SegmentManager.Release`.

Release order:

1. `QueryViewSegmentReadinessManager` detaches the view from transform refs.
2. For each segment whose last transform ref is removed, it cancels catch-up,
   unregisters TransformLog, and releases the loaded segment.
3. It calls `ViewScopedPhysicalSegmentManager.Release` to remove physical refs.
4. The physical manager cancels still-loading segments only when the released
   view was the last physical ref.
5. The physical manager waits for the view's in-flight load callbacks.
6. The transform manager releases the view-level TransformLog guard and
   collection runtime guard.
7. `OnDropped` drives the local state machine to `Dropped`.
8. QueryNode reports `Dropped` and removes the local view entry.

`QueryViewSegmentLoadScheduler.Cancel` is currently best-effort and no-op.
Release correctness therefore depends on context cancellation, ref validation,
and waiting for in-flight callbacks, not on synchronous object-storage load
termination.

## 10. Failure Semantics

| Failure | Behavior |
|---|---|
| TransformLog guard acquire fails | The view is reported `Unrecoverable`. |
| Collection runtime acquire fails | The view is reported `Unrecoverable`; the TransformLog guard is released. |
| Segment metadata RPC fails | The affected segment load fails and waiting views are reported `Unrecoverable`. |
| RPC returns zero or multiple load infos for one segment task | The segment load is treated as unrecoverable for waiting views. |
| Collection index meta update fails | The segment load is treated as unrecoverable. |
| Resource reservation fails | The segment load is treated as unrecoverable. |
| Physical loader fails | The segment load is treated as unrecoverable. |
| Transform registration fails | The loaded segment is released, physical state is reset, and waiting views are reported `Unrecoverable`. |
| Transform catch-up fails | The registration is removed, the loaded segment is released, physical state is reset, and waiting views are reported `Unrecoverable`. |
| Release races with load completion | Late callback is validated against current refs; unreferenced loaded segment is released and ignored. |
| Repeated acquire for the same QueryView key | Not part of the current handler flow. If future same-key view replacement is needed, physical refs that existed only in the old view must be explicitly removed. |

`Unrecoverable` is view-local on QueryNode. QueryNode does not generate a
replacement view.

## 11. Invariants

1. `Acquire` and `Release` callbacks are asynchronous.
2. Every `Acquire` eventually produces `OnReady` or `OnUnrecoverable`.
3. Every `Release` eventually produces exactly one `OnDropped`.
4. QueryNode reports final local `Ready` only after all assigned segments
   complete physical load and TransformLog catch-up.
5. A physical segment load is submitted at most once while a live segment state
   is already loading or loaded.
6. A loaded segment is retained only while at least one local QueryView
   references it.
7. TransformLog registration and live segment release happen in
   `QueryViewSegmentReadinessManager`, so transform consumption is detached
   before the segment is released.
8. QueryNode does not assemble `SegmentLoadInfo` from partial metadata APIs.
   DataCoord owns the complete segment-level load snapshot.
9. Collection runtime metadata and segment load metadata intentionally use
    separate APIs: `DescribeCollection` for collection runtime and
    `GetQueryViewSegmentLoadInfo` for segment load.
10. QueryNode has no `Up` or `Down` local state; it keeps Ready resources until
    `Dropped` is pushed.
