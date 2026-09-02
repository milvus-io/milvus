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
            -> NodeScheduler.Submit(SegmentLoadTask / SegmentUpdateTask)
                 -> CollectionRuntimeGuard.UpdateIndexMeta
                 -> SegmentResourceEstimator.Reserve
                 -> PhysicalSegmentLoader.Load / Update
       -> TransformLogBuffer
       -> QueryViewCollectionRuntimeManager
```

| Component | Responsibility |
|---|---|
| `QNQueryViewHandler` | Applies incoming QueryViews, owns per-shard QueryNode state machines, and calls `SegmentManager.Acquire` or `SegmentManager.Release`. |
| `QNQueryViewStateMachine` | Tracks local `Preparing`, `Ready`, `Unrecoverable`, `Dropping`, and `Dropped` states. Deduplicates incremental ready segment reports. |
| `QueryViewSegmentReadinessManager` | Pins TransformLog and collection runtime, tracks transform-level view/segment refs, registers loaded segments, waits catch-up, and reports segment readiness. |
| `ViewScopedPhysicalSegmentManager` | Tracks physical segment refs by QueryView, builds executable load/update tasks from watched snapshots, submits them to NodeScheduler, validates late callbacks, and waits for in-flight load callbacks during release. |
| `SegmentLoadTask` / `SegmentUpdateTask` | Encapsulate index-meta refresh, resource reservation, physical load/update, callback, cancellation, and retry behavior required by NodeScheduler. |
| `SegmentLoadInfoStream` | Owns one QueryNode-level QueryCoord watch stream, maintains segment-scoped subscriptions and delivered revisions, and restores every live subscription after stream failure. |
| `QueryViewLoadMetadataProvider` | Provides collection-level `DescribeCollection` and versioned `GetQueryViewLoadInfo` through MixCoord/QueryCoord. |
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
                      -> subscribe the shared SegmentLoadInfoStream for each referenced segment
                      -> if segment is missing:
                           -> wait for a complete SegmentLoadInfoSnapshot
                           -> NodeScheduler.Submit(SegmentLoadTask)
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
3. Collection runtime acquisition uses `DescribeCollection` and
   `GetQueryViewLoadInfo`; segment loading consumes complete snapshots delivered
   by `SegmentLoadInfoStream`.
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

`qnview.QueryViewLoadMetadataProvider` exposes only collection-level metadata:

```go
type QueryViewLoadMetadataProvider interface {
    DescribeCollection(ctx context.Context, collectionID int64) (*milvuspb.DescribeCollectionResponse, error)
    GetQueryViewLoadInfo(ctx context.Context, collectionID int64, version QueryViewLoadInfoVersion) (QueryViewLoadInfo, error)
}
```

Collection runtime acquisition resolves the QueryView's load-info version, then
pins the local collection runtime through `collectionManager.PutOrRef` before
any segment load task is submitted.

Segment metadata has a separate streaming boundary. QueryNode owns one shared
`SegmentLoadInfoStream`. Every live physical segment state owns one subscription
identified by the globally unique `segmentID`. The subscription request still
carries `collectionID` for the QueryCoord RPC, but it is not part of the local
subscription key. The subscription contains its handler and its last
successfully delivered revision; the physical manager never writes that
revision back into the stream.

QueryCoord sends complete `SegmentLoadInfoSnapshot` values containing packed
`SegmentLoadInfo`, index definitions, and a revision. The stream dispatches a
snapshot to the matching subscription handler. After the handler accepts the
snapshot, the subscription advances its own delivered revision. The handler
synchronously records the snapshot in the physical manager and triggers the
corresponding asynchronous load/update task through NodeScheduler. The physical
manager coalesces newer snapshots while an update task is already running.

If the underlying gRPC stream breaks, `SegmentLoadInfoStream` keeps all live
subscriptions, reopens the stream, and re-subscribes every segment from its
internally maintained delivered revision. A transport failure therefore does
not require the physical manager to recreate subscriptions or replay revision
updates. After a QueryNode process restart the in-memory revisions are lost, so
new subscriptions start from revision zero and QueryCoord returns full current
snapshots.

DataCoord invalidates these snapshots only after the corresponding segment
index metadata is durable. A finished segment index, text-index stats, and
current-format JSON key stats notify the exact segment. CreateIndex
acknowledgement itself does not notify: each segment is refreshed when its own
index reaches Finished. These events do not change DataView, QueryView, or
LoadConfig; a newly loaded segment obtains the latest complete snapshot from
its initial subscription.

This boundary keeps task execution self-contained: a load/update task never
performs a metadata lookup. It operates only on the immutable snapshot captured
when the task was created.

## 7. Physical Load Stage

`ViewScopedPhysicalSegmentManager` is responsible for physical ref accounting and
load task submission. It maintains:

1. `views`: local QueryView refs and callbacks;
2. `segments`: physical segment state by segment ID;
3. `cancels`: view-level cancellation functions.

Acquire behavior:

1. record or replace the view ref;
2. add the QueryView key to each assigned segment's physical ref set;
3. create one segment-scoped subscription when the first QueryView references a
   new physical segment state;
4. create load state only for segments that are missing or reset;
5. submit load tasks only for segments that are not already loading or loaded;
6. if all requested segments are already physically loaded, call `OnLoaded`
   with those segments.

Load task behavior:

1. require a complete watched `SegmentLoadInfoSnapshot`;
2. update local collection index meta with the snapshot's index definitions;
3. reserve resources through the optional estimator;
4. call `PhysicalSegmentLoader.Load` with the snapshot's packed load info;
5. wrap the segment with `TransformStartAfterTimeTick` if the QueryView meta has
   a delete-apply start timetick;
6. report the loaded segment back to the physical manager.

Update task behavior:

1. classify the revision change into the required physical update actions;
2. refresh collection index meta from the new snapshot;
3. call `PhysicalSegmentLoader.Update`;
4. return `nodescheduler.ErrDelay` for non-cancellation failures so the same
   task is retried;
5. update only the physical segment state's applied revision after success.

The subscription's delivered revision is independent from the physical applied
revision. It advances when the handler has accepted the complete snapshot,
because any subsequently submitted load/update failure remains owned by the
NodeScheduler retry lifecycle. No task completion path sends a subscribe or
revision update back to `SegmentLoadInfoStream`.

`SegmentLoadInfoRevision` is a deterministic content hash and is only an
equality token; it has no ordering semantics. While an update task is in
flight, the physical manager therefore retains the latest accepted snapshot
even when its revision equals the currently applied revision. The in-flight
task may first move the physical segment to a different revision, after which
the retained snapshot must move it back to the latest metadata state.

On physical load completion, the physical manager validates that the segment is
still referenced before keeping it. If no QueryView still references the
segment, the late result is released and ignored.

## 8. Transform Registration and Catch-Up Stage

`QueryViewSegmentReadinessManager` turns physically loaded segments into
QueryView-ready segments.

Recovery baseline events written into TransformLog/TransformingBuffer are
defined by
[RecoveryBarrier](../../../../agent_guides/streaming-system/message/message-semantic-recovery-barrier.md).

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
5. The physical manager closes the segment's SegmentLoadInfo subscription when
   the final physical ref is removed.
6. The physical manager waits for the view's in-flight load callbacks.
7. The transform manager releases the view-level TransformLog guard and
   collection runtime guard.
8. `OnDropped` drives the local state machine to `Dropped`.
9. QueryNode reports `Dropped` and removes the local view entry.

Task cancellation is asynchronous. Load release correctness depends on context
cancellation, ref validation, and waiting for in-flight callbacks rather than
synchronous object-storage termination. The physical segment state retains the
active update `TaskHandle` and cancels it when the last QueryView reference is
removed or the segment is reset, preventing stale `ErrDelay` retries.

## 10. Failure Semantics

| Failure | Behavior |
|---|---|
| TransformLog guard acquire fails | The view is reported `Unrecoverable`. |
| Collection runtime acquire fails | The view is reported `Unrecoverable`; the TransformLog guard is released. |
| A watched snapshot is missing packed load info | The segment load is treated as unrecoverable for waiting views. |
| Collection index meta update fails | The segment load is treated as unrecoverable. |
| Resource reservation fails | The segment load is treated as unrecoverable. |
| Physical loader fails | The segment load is treated as unrecoverable. |
| Segment LoadInfo gRPC stream breaks | The shared stream reconnects and re-subscribes all live segments from their internally maintained delivered revisions. |
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
8. QueryNode does not assemble `SegmentLoadInfo` from partial metadata APIs;
   tasks consume complete watched snapshots.
9. Collection runtime metadata and segment load metadata intentionally use
   separate paths: versioned metadata APIs for collection runtime and the
   segment load-info watch stream for physical tasks.
10. QueryNode has no `Up` or `Down` local state; it keeps Ready resources until
    `Dropped` is pushed.
11. A physical segment state owns at most one SegmentLoadInfo subscription; the
    last view release or segment reset closes that subscription.
12. SegmentLoadInfo subscription revision advances only after its handler
    accepts a snapshot and is used only for stream recovery.
13. SegmentLoadInfo revisions are calculated from a canonical clone of the
    complete snapshot. Semantically unordered collection/segment index
    metadata, parameters, field binlogs, child fields, resource file paths,
    compaction sources, and child manifests do not change the revision, and
    revision calculation never mutates metadata owned by the caller.
