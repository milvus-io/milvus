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
2. every assigned sealed segment has reached the view's minimum `DataVersion`
   and satisfies its pinned `load_info_version` resource requirements;
3. every loaded segment is registered with TransformLog;
4. every registered segment has caught up to the QueryView transform frontier.

After local `Ready`, QueryNode keeps the view-scoped resources until the same
view is applied as `Dropped`.

## 3. Component Responsibilities

The QueryNode entry point wires the resource managers as:

```text
QueryNode.NewQueryViewSegmentManager
  -> QueryViewSegmentManager (one View/Segment registry)
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
| `QueryViewSegmentManager` | Owns one View/Segment registry and reference set for physical preparation, Transform registration/catch-up, and queries. Builds load/update plans, validates asynchronous results against the same instance, and retires resources after all owners finish. |
| `SegmentLoadTask` / `SegmentUpdateTask` | Encapsulate index-meta refresh, resource reservation, physical load/update, callback, cancellation, and retry behavior required by NodeScheduler. |
| `SegmentLoadInfoStream` | Owns one QueryNode-level QueryCoord watch stream, maintains segment-scoped subscriptions and delivered revisions, and restores every live subscription after stream failure. |
| `QueryViewLoadMetadataProvider` | Provides collection-level `DescribeCollection` and versioned `GetQueryViewLoadInfo` through MixCoord/QueryCoord. |
| `QueryViewCollectionRuntimeManager` | Pins local collection runtime using collection schema/load metadata and exposes index meta update for segment load. |
| `TransformLogBuffer` | Pins the view-level transform range and registers loaded sealed segments for catch-up. |

The production construction entry point requires a load resource budget, chunk
manager, collection metadata provider, TransformLog stream manager, and one
`SegmentLoadInfoStreamFactory`. Missing dependencies or a factory returning a
nil stream fail construction with an internal error before the scheduler or
background workers are started. Both the QueryNode wrapper and resource
constructor return `(SegmentManager, error)`; callers must propagate the error
instead of installing a partially functional manager. This check does not wait
for remote connectivity; transient transport failures remain the stream's
reconnection responsibility.

## 4. Authoritative Acquire Execution Order

This section is the single source of truth for the current QueryNode Acquire
order. Later sections explain individual stages and contracts without changing
this sequence.

```text
Incoming QueryView(Preparing)
  -> QNQueryViewHandler.ApplyViews
       -> create QNQueryViewStateMachine
       -> SegmentManager.Acquire
            -> QueryViewSegmentManager.Acquire
                 -> synchronously register View and assigned Segment references
                 -> retain an existing channel guard during asynchronous handoff
                 -> asynchronously acquire TransformLogBuffer guard
                 -> QueryViewCollectionRuntimeManager.Acquire
                      -> QueryViewLoadMetadataProvider.DescribeCollection
                      -> pin CCollection by (CollectionID, logical SchemaVersion)
                 -> activate preparation for the registered instances
                 -> report empty OnReady if this QN has no assigned segments
                 -> install per-view requirements on the same registry
                      -> subscribe the shared SegmentLoadInfoStream for each referenced segment
                      -> if segment is missing:
                           -> wait for a complete SegmentLoadInfoSnapshot
                           -> NodeScheduler.Submit(SegmentLoadTask)
                                -> CollectionRuntimeGuard.UpdateIndexMeta
                                -> SegmentResourceEstimator.Reserve
                                -> PhysicalSegmentLoader.Load
                      -> if segment is already physically loaded:
                           -> reuse only after its applied version/configuration satisfies this view
                           -> otherwise prepare the union and Reopen through resource admission
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

Collection runtime acquisition resolves and validates the QueryView's exact load-info
version and collection ID, retains an immutable LoadInfo on its guard, then pins a
native `segcore.CCollection` in a QV-owned registry keyed by `(CollectionID,
Schema.Version)`. The version is the logical schema version, not the collection
metadata update timestamp. Different versions coexist; a version is released
only after its last view, segment, and in-flight query reference is gone.
Neither acquisition nor release uses the legacy `segments.CollectionManager`.

Each physical segment owns a `segcore.CSegment` and its own collection guard.
Load and Reopen consume explicit immutable metadata snapshots, with no lookup
in the legacy SegmentManager or Loader. Reopen retains its target collection
before native work and publishes the new guard and LoadInfo only on success;
failure releases the tentative guard. This does not change CSegment schema
evolution or LazyCheckSchema behavior.

Resource estimation reuses `segcore/loadresource` against the pinned schema.
QueryNode constructs one `segments.LoadResourceBudget` and injects it into both
the legacy loader and QV admission, so their concurrent reservations compete
for the same node budget. The shared delta/Bloom-filter load helpers take an
explicit schema, chunk manager, and load target. Persisted deletes still use
`LoadDeletedRecord` on the load pool; online Transform deletes use `Delete` on
the mutate pool. Bloom-filter accounting is refunded with the owning segment.

Segment metadata has a separate streaming boundary. QueryNode owns one shared
`SegmentLoadInfoStream`. Every live physical segment state owns one subscription
identified by the globally unique `segmentID`. The subscription request still
carries `collectionID` for the QueryCoord RPC, but it is not part of the local
subscription key. The subscription contains its handler and its last
successfully delivered revision; the physical preparation stage never writes that
revision back into the stream.

QueryCoord sends complete `SegmentLoadInfoSnapshot` values containing packed
`SegmentLoadInfo`, index definitions, a content revision, and a certified
`DataVersion`. The subscription carries the minimum DataVersion and the union
of the live views' LoadInfo requirements. Changes to either target replace
the subscription; its epoch rejects callbacks from the retired subscription.
Even an unchanged content revision must carry an updated DataVersion proof
when requested. This is part of the metadata provider interface contract. The stream dispatches a
snapshot to the matching subscription handler. After the handler accepts the
snapshot, the subscription advances its own delivered revision. The handler
synchronously records the snapshot in the physical preparation stage and triggers the
corresponding asynchronous load/update task through NodeScheduler. The physical
manager coalesces newer snapshots while an update task is already running.

If the underlying gRPC stream breaks, `SegmentLoadInfoStream` keeps all live
subscriptions, reopens the stream, and re-subscribes every segment from its
internally maintained delivered revision. A transport failure therefore does
not require the physical preparation stage to recreate subscriptions or replay revision
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

Physical preparation is an internal stage of `QueryViewSegmentManager`, not a
second resource owner. `views` holds each View's requirements, cancellation and
callbacks; `segments` holds physical, Transform and query state for each shared
instance. Each Segment has exactly one set of View references.

Preparation behavior:

1. require the View reference synchronously registered by `Acquire`;
2. attach metadata and the plan to that reference without acquiring it again;
3. create one segment-scoped subscription when the first QueryView references a
   new physical segment state;
4. create load state only for segments that are missing or reset;
5. submit load tasks only for segments that are not already loading or loaded;
6. notify each view incrementally only when a segment satisfies that view's
   minimum DataVersion and pinned LoadInfo requirements.

Load task behavior:

1. require a complete watched `SegmentLoadInfoSnapshot`;
2. update local collection index meta with the snapshot's index definitions;
3. reserve resources through the optional estimator;
4. call `PlannedPhysicalSegmentLoader.LoadWithPlan` with the snapshot's packed
   load info, selected collection runtime, and explicit Transform replay floor;
5. initialize both the segment's replay floor and applied progress from that
   floor, including zero; do not substitute DeltaPosition or override only a getter;
6. report the loaded segment back to the physical preparation stage.

Shared preparation selects the collection runtime deterministically: greatest
logical SchemaVersion, then greatest LoadInfoVersion, then the lexicographically
smallest QueryViewKey string. Fields and index/load configuration still come
from the union of all referenced views. The initial Transform replay floor is
the minimum frontier of those views, independently of the selected collection.
This relies on the upstream contract that each view's frontier is safe for its
compatible loading snapshot; it adds no new snapshot-coverage protocol or
CSegment schema-compatibility behavior.

The plan is fixed for an attempt, including admission retries. Pending-attempt
references retain its collection owner even if that view drops. The manager also retains the earliest required buffer guard until a
registration takes over retention, or preparation is abandoned. This closes the
gap between view release, physical load completion and Transform registration.
Reopen preserves existing Transform progress and does not reset this baseline.
The legacy loader entry point remains available, but a scheduler using it
rejects a returned segment whose initial replay/applied progress differs from
the plan instead of masking the mismatch with a decorator.

Update task behavior:

1. classify the revision change into the required physical update actions;
2. refresh collection index meta from the new snapshot;
3. reserve resources, then call `PhysicalSegmentLoader.Update` with Reopen once;
4. use the same resource estimation and reservation as first load; return
   `nodescheduler.ErrDelay` only when resource admission fails;
5. fast-fail an actual physical Reopen error, notifying affected waiting views;
6. publish the applied revision, DataVersion and captured load requirements only
   after success. An older Ready view keeps the previously loaded instance when
   Reopen fails.

The subscription's delivered revision is independent from the physical applied
revision. It advances when the handler has accepted the complete snapshot,
because subsequent preparation is owned by the physical preparation stage: resource
admission retries through NodeScheduler, while native load/Reopen errors end
the attempted preparation. No task completion path sends a subscribe or
revision update back to `SegmentLoadInfoStream`.

`SegmentLoadInfoRevision` is a deterministic content hash and is only an
equality token; it has no ordering semantics. While an update task is in
flight, the physical preparation stage therefore retains the latest accepted snapshot
even when its revision equals the currently applied revision. The in-flight
task may first move the physical segment to a different revision, after which
the retained snapshot must move it back to the latest metadata state.

Every physical load attempt captures a manager-wide monotonically increasing
load generation. Resource-admission retries retain that attempt and generation;
a later reload receives a new generation. Removing/recreating a SegmentID never
resets the counter. This generation is independent of the
metadata content-hash revision.

On physical load completion or failure, the physical preparation stage validates both
that the current state is still loading and that its generation matches the
submission. A stale successful result releases only its own Segment; a stale
failure does not change current refs, subscriptions, revisions, or callbacks.
Both paths still complete their original load-attempt cleanup so an old view's
pending release can finish. A current result is retained only while at least
one QueryView still references it.

## 8. Transform Registration and Catch-Up Stage

`QueryViewSegmentManager` turns physically loaded segments into
QueryView-ready segments.

Recovery baseline events written into TransformLog/TransformingBuffer are
defined by
[RecoveryBarrier](../../../../agent_guides/streaming-system/message/message-semantic-recovery-barrier.md).

For each physically loaded segment:

1. mark the segment as physically loaded if it is still referenced;
2. register it with `TransformLogBuffer`;
3. store the registration and catch-up cancellation function;
4. wait for `TransformRegistration.WaitCatchup`;
5. validate that the catch-up task still belongs to the same readiness state,
   then mark the segment transform-loaded;
6. notify all waiting QueryViews through `OnReady`.

Catch-up tasks retain their readiness-state identity and cancellation context
from scheduling through registration and completion. Late registration failures,
catch-up failures, and successful completions from a retired state must not
modify a replacement with the same SegmentID. Query handles pin that concrete
state, rather than looking up the latest state by SegmentID when releasing.

If another QueryView references a segment that is already transform-loaded, the
manager still checks physical preparation against this view's
DataVersion and LoadInfo. Only then can it report Ready without re-registering
the segment.

TransformLogBuffer owns the logical streams returned by `AcquireStream` and
closes each after its final buffer reference is released. Physical RPCs are
on-demand inside the logical stream: no subscriptions means no connection;
new subscriptions restart connection establishment without replacing the logical
stream. A pending unsubscribe cancels only its physical attempt when demand
reaches zero, never the owner-held logical stream.

Pending acquisitions on one VChannel share a subscription attempt owned by that
VChannel buffer. Each caller's context bounds only its own wait, including the
caller that starts the attempt. Canceling one view releases its reference without
canceling another view's subscription preparation. The last reference cancels
the pending attempt or closes the established subscription. If subscription
success races with that final release, the retired buffer closes the late result;
it cannot install it into a newly acquired buffer for the same VChannel.

A terminated logical stream is removed from the cache for new VChannels even
while old buffers retain references. Each buffer retains the concrete stream
state it acquired, so releasing an old buffer cannot evict or close a newer
stream for the same PChannel. Existing failed VChannel buffers keep their errors;
this does not clear their errors or retry their subscriptions. A new acquisition
still reports its own failure if the underlying cause remains.

Continuous TransformLog subscriptions resume at PChannel scope across SN owner
migration. The server ends the physical RPC on provider shutdown, fencing,
stale ownership, or cancellation of an active provider operation. Explicit
unsubscription first disables that subscription's forwarding handler and then
closes its reader; its cancellation is normal cleanup and does not reconnect
other subscriptions. When the local PChannel stream has already ended, its
error takes precedence over consequential reader cancellation.

The client resume loop owns each physical connection's context and restores
only live logical subscriptions from their last accepted Entry/SyncUp cursors.
Subscription handlers never cancel physical connections. A terminal physical
stream error stops resumption just as a terminal connection-creation error does.
Subscription semantic errors and consumer rejection terminate the affected
logical subscription; UNKNOWN error text is not interpreted as owner migration.
Bounded completion, parent cancellation and explicit Close retain their existing
termination semantics. Canceling the context passed to Subscribe withdraws a
pending creation; after success, the caller releases the subscription with Close.

The TransformLogBuffer retains entries strictly after the minimum
`TransformStartAfterTimeTick` of its live view guards and pending segment
registrations. A preparation may retain its originating guard after that view
drops, until registration protects the in-flight attempt's replay range.
Releasing a view guard, completing catch-up, or removing a
registration immediately re-evaluates this boundary. A caught-up segment no
longer pins its original replay range: it receives subsequent entries through
live delivery. Trimming clears discarded entry pointers in the backing array
so they do not retain Transform payloads. A view whose frontier never advances
continues to retain its required history; the buffer does not evict that range
based on a separate size limit.

If registration or catch-up fails:

1. cancel catch-up;
2. unregister from TransformLog if a registration exists;
3. retire the failed instance from the active registry and mark affected Views
   unrecoverable while holding the same manager lock;
4. keep their references to the retired instance until `Release`; only after
   the final View, query handle and preparation/catch-up task finishes may its
   native Segment be destroyed;
5. notify affected QueryViews with `OnUnrecoverable`.

A later View can create a fresh instance without reusing the failed one. Old
View releases and callbacks retain the old instance identity and cannot affect
that replacement. There is no physical reset interface or second ref table.

### TODO: preparation fairness across PChannels

Treat catch-up scheduling fairness as a follow-up optimization, not a required
correctness fix in this PR. Waiting among VChannels on the same PChannel is
acceptable: they share that PChannel's availability boundary. The optimization
target is isolation between PChannels with different availability.

Currently, each `QueryViewSegmentManager` has one shared `catchupTasks` queue
and worker pool; its `TransformLogBuffer` has one shared `drainTasks` queue and
worker pool. Neither pool is partitioned by PChannel. The configured worker
counts (`queryNode.queryView.segmentCatchupConcurrency` and
`queryNode.queryView.transformLogDrainConcurrency`, both defaulting to 4) bound
tasks including their waits: catch-up workers wait in `WaitCatchup`, and drain
workers wait for progress when buffered entries are exhausted before the first
SyncUp.

Consequently, PChannel A can establish subscriptions, start enough Segment
catch-up tasks, then stall before its first SyncUp or lose its connection while
resumption is pending. A's tasks can occupy the shared workers and delay
Preparing views on a healthy PChannel B. Either pool can be the bottleneck. A
PChannel that is unreachable before subscription establishment does not occupy
these pools: initial subscription acquisition runs asynchronously before Segment
loading. Terminal subscription errors end catch-up rather than waiting for
recovery indefinitely.

This scheduling contention delays preparation and Ready transitions; it does
not directly stop queries or live Transform delivery for already-serving views.
An underlying data-stream outage can separately delay those views' MVCC waits.
Do not conflate that availability effect with catch-up scheduling contention.

TODO: evaluate scheduling that limits active catch-up work without letting
waiting PChannels monopolize execution capacity. Preserve per-Segment Apply
ordering, cancellation, history retention and the atomic catch-up/live handoff.
Validation must hold A before its first SyncUp with more tasks than the worker
capacity, demonstrate B can still become Ready, then verify A can resume or be
released without missed Deletes, stale callbacks or leaked references. No
additional fairness guarantee within a PChannel is required by this TODO.

### ApplyTransform failure and Poison

A registration applies each entry once. If `ApplyTransform` fails at TimeTick
`T`, it synchronously marks that concrete readiness-state instance Poison,
recording the first failed TimeTick in local state. The existing instance
identity/generation fences stale callbacks. It stops applying later entries to
that instance; the apply failure is logged locally.
Other segments continue consuming, and shared visibility advances only after
Poison has been published. Cancellation/unregistration waits for any in-flight
native Apply before the physical segment can be released.

Task providers wait for shared visibility, acquire selected segment handles,
and check Poison on those pinned instances. Queries with transforming MVCC
`>= T` fail with `VIEW_INVALIDATED`; queries `< T` may still execute. This
also covers a query already waiting for visibility when Apply fails. Entries
and SyncUp are ordered; a query that has already passed visibility cannot be
invalidated by a later entry at a lower TimeTick. Partial Delete blocks from
the failed entry all carry timestamp `T`.

Ready views retain their local Ready state and resources for historical
queries. Preparing views fail preparation, and a new view cannot reuse a
Poison instance as healthy. Poison does not trigger same-instance retries or
immediate resource release. Normal view/query reference teardown releases the
instance. A fresh load has a new generation; old callbacks cannot poison it.

Poison is exclusively QueryNode-local state. It is not included in QueryView
protobufs or reports, and Coordinator does not track poisoned segments or
trigger a Poison-specific recovery/placement workflow. A Ready view keeps its
state and emits no report merely because ApplyTransform failed. Preparing
views, including a new view attempting to reuse a poisoned instance, report
ordinary `Unrecoverable` preparation failure without Poison details. A poisoned
instance is released through normal view/query reference teardown; this local
marker does not itself guarantee automatic recovery of a Ready view.

## 9. Release Flow

When a view is applied as `Dropped`, QueryNode enters local `Dropping` and calls
`SegmentManager.Release`.

Release order:

1. The view leaves routing. If query handles still pin it, `OnDropped` may
   acknowledge logical removal while its resource requirements and guards remain
   retained; the final handle resumes resource teardown below.
2. `QueryViewSegmentManager` removes the View from its sole reference registry.
   For the last reference it retires that instance, cancels preparation and
   catch-up, unregisters TransformLog, and closes the metadata subscription.
3. Cleanup pins the retired instance until unregistration completes. A new
   instance with the same SegmentID has independent state and cleanup.
4. It waits for all load/Reopen attempts borrowing the released view's runtime,
   including canceled tasks that have not started.
5. The manager releases detached physical segments and the collection
   runtime guard. Query handles and native tasks must both be finished before
   their retained resources can be destroyed.
6. `OnDropped` acknowledges completion if it was not already acknowledged at
   logical removal.

Releasing a physical Transform segment wakes outstanding `WaitTransformApplied`
calls with a segment-not-loaded error. Subsequent waits also fail, including
waits at an already applied timestamp; release is not successful catch-up.

Task cancellation is asynchronous. Load release correctness depends on context
cancellation, ref validation, and waiting for in-flight callbacks rather than
synchronous object-storage termination. The physical segment state retains the
active update `TaskHandle` and cancels it when the last QueryView reference is
removed or the segment is reset, preventing stale `ErrDelay` retries.

## 10. Failure Semantics

| Failure | Behavior |
|---|---|
| TransformLog guard acquire fails | The view is reported `Unrecoverable`. |
| Collection runtime acquire fails | The view is reported `Unrecoverable`; its reference and acquired guards remain until `Release`. |
| A watched snapshot is missing packed load info | The segment load is treated as unrecoverable for waiting views. |
| Collection index meta update fails | The segment load is treated as unrecoverable. |
| Resource estimation/reservation fails | Retry admission with scheduler backoff; the view remains Preparing. |
| Physical loader fails | The segment load is treated as unrecoverable. |
| Segment LoadInfo gRPC stream breaks | The shared stream reconnects and re-subscribes all live segments from their internally maintained delivered revisions. |
| Transform registration fails | The instance is retired and waiting views report `Unrecoverable`; owning references remain until `Release`. |
| Transform entry Apply fails during catch-up or live delivery | Mark the instance Poison locally and stop later Applies. Preparing views report ordinary Unrecoverable; Ready views emit no Poison report and reject queries at or beyond the failed boundary. |
| Transform catch-up fails for another reason | The registration is removed and the instance is retired; failed Views retain references until `Release`. |
| Release races with load completion | Late callback is validated against current refs; unreferenced loaded segment is released and ignored. |
| Repeated acquire for the same QueryView key | Does not acquire a second reference. Late preparation must still match the original View identity. |

`Unrecoverable` is view-local on QueryNode. QueryNode does not generate a
replacement view.

## 11. Invariants

1. `Acquire` and `Release` callbacks are asynchronous.
2. Every live `Acquire` eventually produces `OnReady` or `OnUnrecoverable`;
   an intervening `Release` cancels unfinished preparation.
3. Every `Release` eventually produces exactly one `OnDropped`.
4. QueryNode reports final local `Ready` only after all assigned segments
   complete physical load and TransformLog catch-up.
5. A physical segment load is submitted at most once while a live segment state
   is already loading or loaded.
6. A loaded segment is retained while a View, query handle, preparation task,
   catch-up task, or unfinished unregistration still owns it.
7. TransformLog registration and live segment release happen in
   `QueryViewSegmentManager`, so transform consumption is detached
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

## 12. Per-view LoadInfo and shared segment preparation

Each QueryView pins its exact collection LoadInfo. For every physical Segment,
its referenced views determine an immutable preparation plan:

- Fields are the union of all referenced LoadInfos.
- A field mentioned by several versions uses the newest LoadInfo's index and
  loading parameters. An older view may share that newer configuration; an
  incoming older view must not overwrite a newer live view's requirements.
- DataVersion is a lexicographically ordered minimum, independently of the
  content-hash revision and the load-configuration version.
- A change to the union can require Reopen even if the metadata revision and
  DataVersion are unchanged. Missing requested index metadata waits for the
  subscription to deliver it.
- An in-flight attempt retains its captured plan. New references update the
  pending plan; the old attempt cannot certify the newly requested resources.
- Metadata snapshots are filtered into the union's binlogs and indexes before
  reservation and native work. Packed column groups remain whole. Manifest
  resolution and native lazy-loading policy remain the physical loader's
  responsibility.

Per-view physical readiness is combined with shared Transform catch-up readiness.
A native Reopen failure fails only the waiting views covered by that attempted
plan; it does not reset a previously usable shared Segment. A queued newer plan
can still proceed. Resource admission failures retry for both Load and Reopen.

Queries holding Segment handles also pin their originating view's resource
requirements. Routing may drop the view, but its physical references and LoadInfo
remain until those handles are released, preventing a later plan from removing
fields still needed by those queries. Task completion additionally retains its
original view-reference identity across removal and recreation.
