# StreamingNode Query Runtime Manager Design

> StreamingNode-side query runtime ownership for QueryView.
> This document defines the PChannel-local resource manager, the vchannel-level
> `QueryRuntime`, and the reference model used by WAL recovery and the
> StreamingNode QueryView state machine. Query execution, QueryCoord placement,
> and QueryNode sealed segment lifecycle are out of scope.

## 1. Purpose

`SNQueryRuntimeManager` prepares and owns StreamingNode-local query runtimes for
QueryView.

The manager is scoped to one `PChannelRuntime`. A StreamingNode may run many
PChannel runtimes, and each PChannel runtime owns one resource manager instance.
The manager does not serve resources across PChannels.

Inside one `PChannelRuntime`, it has two upstream callers:

```text
RecoveryStorage -> SNQueryRuntimeManager
QueryViewStateMachine -> snview.StreamingNodeResourceManager
```

The same PChannel-local component implements both WAL-side and QueryView-side
interfaces:

- `walview.LoadConfigListener`
- `snview.StreamingNodeResourceManager`

The manager owns resource lifetime. The actual vchannel resources are held by a
single `QueryRuntime` per loaded vchannel:

```text
SNQueryRuntimeManager
  -> QueryRuntime
       -> QueryRuntimeModule*
```

`QueryRuntime` is the only `walview.VChannelLiveObserver` for the vchannel. It
pushes live resource events into one internal buffer, initializes all query
resource modules, drains the buffer into every module through one consumer, and
enters `Ready` only after the whole vchannel resource is caught up.

`Up` is not a resource-manager event. An `Up` QueryView is persisted only as
WAL-bound QueryView meta for StreamingNode crash recovery. Resource lifetime is
driven by `OnAlterLoadConfig`, `OnDropLoadConfig`, `Acquire`, and `Release`.

## 2. Dependency Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `PChannelRuntime` | Owns one PChannel WAL instance and PChannel-local WAL submodules, including `RecoveryStorage`, `QueryViewStateMachine`, and `SNQueryRuntimeManager`. | It coordinates WAL open, recovery, handoff close, and module close order. It does not build vchannel query resources directly. |
| `RecoveryStorage` | Observes `AlterLoadConfig` / `DropLoadConfig`, restores WAL metadata on startup, builds valid `VChannelWALView`, and calls the resource manager through `LoadConfigListener`. | It does not build csegments, fetch BM25 resources, wait for query resources to become ready, or manage QueryView lifecycle. WALView capture details are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `SNQueryRuntimeManager` | Owns PChannel-local resource references, creates vchannel `QueryRuntime` instances, submits initialization tasks, waits for runtime initialization on `Acquire`, advances DataVersion watermarks, and releases resources. | It does not apply WAL events to concrete resources directly. It does not own QueryView state transitions. |
| `QueryRuntime` | VChannel-level singleton resource runtime. Implements `VChannelLiveObserver`, owns one live-event buffer and one consumer, initializes resource modules, drains buffered events into modules, and broadcasts DataVersion advancement. | It does not own QueryView references, `load_config` meta, or WAL module snapshots. |
| `QueryRuntimeModule` | Common lifecycle interface implemented by resource modules. | It does not manage references or live observer registration. |
| `QueryRuntimeModuleBuilder` | Creates unprepared `QueryRuntimeModule` instances when `AlterLoadConfig` creates a new runtime. | It does not receive QueryView references or own module initialization; `VChannelWALView` is passed later to `QueryRuntime.Initialize`. |
| `GrowingRuntime` | QueryRuntime module that owns growing segment resources for the vchannel. | It does not implement `VChannelLiveObserver`, maintain pending buffers, or decide QueryView lifecycle. Details are defined in [StreamingNode Growing Segment Runtime Design](growing_segment_runtime.md). |
| `Scheduler` | Runs `QueryRuntime` initialization tasks with bounded concurrency. | It does not know QueryView references, resource lifetime, or DataVersion watermarks. |
| `QueryViewStateMachine` | PChannel-local WAL submodule that owns QueryView transitions, calls `Acquire` when a QueryView starts using local resources, calls `Release` when a QueryView leaves this PChannel runtime, and drains local QueryViews before WAL handoff close. | It does not manage csegments, BM25 resources, live observers, or resource GC directly. |
| `QueryView Meta` | WAL-bound metadata persisted for crash recovery and owned by the PChannel-local QueryView state machine. | It is stored under the PChannel WAL identity, used by QueryView recovery and `Acquire`, and must not be scoped by StreamingNode node ID. |

The key dependency boundary is:

```text
VChannelModule / SegmentModule / TransformLogModule
        -> RecoveryStorage builds VChannelWALView
        -> SNQueryRuntimeManager
        -> QueryRuntime
        -> QueryRuntimeModule

QueryViewStateMachine
        -> snview.StreamingNodeResourceManager
        -> SNQueryRuntimeManager implementation
```

`SNQueryRuntimeManager` consumes `VChannelWALView` as the complete WAL
input package. It must not call back into WAL modules to rebuild a snapshot.
The WALView structure, capture order, live observer contract, and historical
delete replay contract are defined by
[StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md).

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

Normal load:

```text
AlterLoadConfig WAL message
        |
        v
RecoveryStorage
        |
        | OnAlterLoadConfig(VChannelWALView)
        v
SNQueryRuntimeManager
        |
        | create vchannel singleton
        v
QueryRuntime(Preparing, VChannelLiveObserver)
        |
        | submit initialization
        v
Scheduler
```

Live resource events:

```text
RecoveryStorage
        |
        | ObserveEvent
        v
QueryRuntime
        |
        | ordered ApplyLiveEvent
        v
QueryRuntimeModule
```

QueryView references:

```text
QueryViewStateMachine
        |
        | Acquire / Release
        v
SNQueryRuntimeManager
        |
        | Advance(oldestDataVersion)
        v
QueryRuntime
        |
        | module.Advance(oldestDataVersion)
        v
QueryRuntimeModule*
```

### 3.2 Reference Model

The resource manager tracks references per loaded vchannel:

```text
resource refs =
  optional initRef(load_config)
  + queryViewRefs[QueryViewVersion]
```

Reference rules:

1. `OnAlterLoadConfig` creates `initRef`.
2. `Acquire(QueryView)` creates a `queryViewRef`.
3. The first successful `Acquire` atomically registers the QueryView reference
   and removes `initRef`.
4. Later `Acquire` calls only add QueryView references.
5. `Release(QueryView)` removes the corresponding QueryView reference.
6. `OnDropLoadConfig` removes only `initRef`.
7. Resources can be released only when both `initRef` and all
   `queryViewRefs` are absent.
8. WAL handoff close drains `queryViewRefs` through
   `QueryViewStateMachine.CloseForHandoff` before the resource manager is
   finalized.

### 3.3 VChannel Resource State

The manager maintains one resource state per loaded vchannel:

```text
resources map[vchannel]vchannelResourceState

vchannelResourceState
  initRef bool
  queryViewRefs map[QueryViewVersion]QueryViewMeta
  runtime QueryRuntime
  buildTask QueryRuntimeBuildTask
```

There is no `DataVersion -> runtime` map. A loaded vchannel owns exactly one
`QueryRuntime`.

The resource key is only `vchannel`. `collectionID` is a consistency property
inside `VChannelWALView` and resource modules. It is not part of the manager's
resource identity.

The runtime owns all module state:

```text
QueryRuntime
  state Preparing | Ready | Closed
  pendingEvents []VChannelResourceEvent
  drainWorker
  modules []QueryRuntimeModule
```

### 3.4 DataVersion Advancement

The resource manager computes one watermark from active QueryView references:

```text
oldestDataVersion = min(queryViewRefs.DataVersion)
```

It calls `QueryRuntime.Advance(oldestDataVersion)` only when at least one
QueryView reference exists.

`QueryRuntime.Advance` broadcasts the same watermark to every module:

```text
each QueryRuntimeModule.Advance(oldestDataVersion)
```

Module-specific meaning:

- `GrowingRuntime` uses the watermark to release growing segment state no longer
  needed by any active QueryView.
- BM25 / IDF modules may use the watermark to asynchronously advance oracle
  state. A module must not advance beyond the oldest active QueryView.

### 3.5 Invariants

1. `RecoveryStorage` depends on `SNQueryRuntimeManager` only through
   `LoadConfigListener`.
2. `QueryViewStateMachine` depends on the resource layer only through
   `snview.StreamingNodeResourceManager`.
3. `SNQueryRuntimeManager` is the only owner of StreamingNode query
   resource lifetime for its PChannel.
4. A loaded vchannel has at most one `QueryRuntime`.
5. `QueryRuntime` is the only live observer returned to `RecoveryStorage`.
6. Resource modules do not implement `VChannelLiveObserver`.
7. `QueryRuntime.Initialize` represents whole-resource catchup, not a single
   module's catchup.
8. `AlterLoadConfig` creates `initRef` and starts `QueryRuntime`
   initialization.
9. `DropLoadConfig` removes only `initRef`.
10. QueryView `Acquire` creates QueryView references; QueryView `Release`
    removes them.
11. QueryView `Acquire` does not create or schedule a new runtime.
12. QueryView `Acquire` is monotonic for one vchannel.
13. `QueryRuntime.Advance(oldestDataVersion)` is monotonic. A non-monotonic
    advance is a critical resource-manager bug and must fail by assertion.
14. Recovery acquires QueryViews in QueryViewVersion order.
15. Resources are released only after all resource-manager references are gone.
16. PChannel handoff close must first unmount local QueryViews through
    `QueryViewStateMachine`, then close the resource manager.

## 4. Interface Description

### 4.1 PChannel Resource Manager

```go
type SNQueryRuntimeManager interface {
    walview.LoadConfigListener
    snview.StreamingNodeResourceManager
    Close()
}
```

There is one `SNQueryRuntimeManager` instance per `PChannelRuntime`.
`Close` is called by `PChannelRuntime` after the PChannel-local QueryView state
machine has drained local QueryViews.

### 4.2 WAL-Side Interface

```go
type LoadConfigListener interface {
    OnAlterLoadConfig(view walview.VChannelWALView) walview.VChannelLiveObserver
    OnDropLoadConfig(event walview.DropLoadConfigEvent)
}
```

`OnAlterLoadConfig` receives a complete WAL input view, creates the vchannel
singleton `QueryRuntime` in `Preparing` state, submits its initialization task,
and returns the runtime as the live observer.

`OnDropLoadConfig` removes the initialization reference for the vchannel. It is
not a QueryView cleanup command.

### 4.3 QueryView-Side Interface

```go
type StreamingNodeResourceManager interface {
    Acquire(req AcquireResource)
    Release(req ReleaseResource)
}

type AcquireResource struct {
    Key qviews.QueryViewKey
    Meta *viewpb.QueryViewMeta

    OnReady func()
}

type ReleaseResource struct {
    Key qviews.QueryViewKey

    OnDropped func()
}
```

`Acquire` is asynchronous. It registers the QueryView reference and returns
without calling callbacks inline. When the vchannel `QueryRuntime`
initialization task has completed successfully, the manager advances the runtime
with the oldest active QueryView DataVersion and invokes `OnReady`.

The current design does not model recoverable or unrecoverable resource errors.
`QueryRuntime.Initialize` is not expected to fail for valid WAL input. A
non-cancellation failure means critical local corruption and must fail fast.

`Release` is asynchronous. It removes the QueryView reference, advances the
runtime if QueryView references remain, and invokes `OnDropped` after the
release transition is recorded. Runtime close is a resource-manager cleanup
operation and is not part of the QueryView state-machine report path.

### 4.4 QueryRuntime

```go
type QueryRuntime interface {
    walview.VChannelLiveObserver

    Initialize(ctx context.Context, view walview.VChannelWALView) error

    Advance(oldestDataVersion qviews.DataVersion)

    Close()
}
```

`Initialize` prepares all modules from the provided `VChannelWALView`, catches
up historical inputs, drains the pending live-event buffer, and moves the
runtime to `Ready`.

`Initialize` returns successfully only after:

1. every module has finished `Prepare(ctx, view)`;
2. the runtime atomically takes the current live-event buffer batch and clears
   the shared buffer;
3. the singleton consumer has applied that initial batch to every module in WAL
   order;
4. the runtime has entered `Ready`.

Events appended while the initial batch is being drained stay in the shared
buffer. They are handled by the same singleton consumer after the runtime enters
`Ready`, but they do not block the `Ready` transition.

`Initialize` has no recoverable data failure path for valid WAL input. If it
observes invalid data or a module cannot apply valid input, the StreamingNode
must fail critically. Returning an error is reserved for lifecycle cancellation,
normally `ctx.Done` during WAL close or manager close.

`QueryRuntime` does not retain the `VChannelWALView` after `Initialize`
returns. The WALView is a preparation input package, not a runtime resource
handle.

`Advance` is called only when at least one QueryView reference exists.
`oldestDataVersion` passed to `Advance` must be monotonic non-decreasing for one
vchannel. The runtime records the latest advanced `oldestDataVersion`; a later
call with an older value is a critical bug and must fail by assertion.

`Close` is a fast resource cancellation path. It stops accepting new live
events, cancels `Initialize` if it is still running, stops the singleton
consumer, closes modules, and releases buffered events. It does not invoke
QueryView callbacks or report QueryView states.

After `Close` starts, `ObserveEvent` must not append new events to the runtime
buffer. It returns `false` if the WAL observer contract requires an acceptance
result.

### 4.5 QueryRuntimeModule

```go
type QueryRuntimeModule interface {
    Prepare(ctx context.Context, view walview.VChannelWALView) error
    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)
    Advance(oldestDataVersion qviews.DataVersion)
    Close()
}
```

Concrete resource implementations such as growing segment runtime and BM25 / IDF
runtime implement this interface. Query-facing accessors are exposed by their
own module-specific interfaces, not by `QueryRuntimeModule`.

```go
type QueryRuntimeModuleBuilder interface {
    NewRuntime() (QueryRuntimeModule, error)
}
```

`SNQueryRuntimeManager` receives module builders when it is constructed and uses
them to create one fresh module set for every loaded vchannel. The manager does
not know module-specific concepts such as BM25, IDF, or query-facing oracle
APIs.

`ApplyLiveEvent` has no recoverable error return. Failure to apply valid live
input means the WALView input or local runtime state is corrupted and the
StreamingNode must fail critically.

`QueryRuntime` is responsible for serializing calls to `ApplyLiveEvent`. Modules
must not start their own live-event consumers.

### 4.6 Scheduler

```go
type Scheduler interface {
    Submit(task QueryRuntimeBuildTask)
    Close()
}

type QueryRuntimeBuildTask interface {
    Run()
    Done() <-chan struct{}
    Result() (QueryRuntime, error)

    Cancel()
}
```

The scheduler guarantees bounded initialization concurrency. It does not manage
references, create tasks from QueryView `Acquire`, choose DataVersions, or apply
resource events.

## 5. Actual Behavior

### 5.1 Normal Load

```text
RecoveryStorage observes AlterLoadConfig
  -> builds VChannelWALView
  -> SNQueryRuntimeManager.OnAlterLoadConfig(view)
  -> manager creates initRef
  -> manager creates QueryRuntime(Preparing)
  -> manager submits QueryRuntimeBuildTask
  -> manager returns QueryRuntime as VChannelLiveObserver
```

The returned observer receives live resource events immediately. Events observed
before runtime readiness are pushed into the `QueryRuntime` live-event buffer.

### 5.2 QueryRuntime Initialization

```text
Scheduler runs QueryRuntimeBuildTask
  -> QueryRuntime.Initialize(VChannelWALView)
  -> each QueryRuntimeModule.Prepare
  -> QueryRuntime starts the singleton consumer
  -> QueryRuntime atomically takes the current buffer batch
  -> QueryRuntime drains the initial batch in WAL order
  -> each event is applied to every QueryRuntimeModule
  -> QueryRuntime enters Ready
```

`QueryRuntime` owns one live-event buffer and one consumer. `ObserveEvent`
always appends to the same buffer, both while `Initialize` is running and after
the runtime is `Ready`. The singleton consumer drains the buffer in WAL order
and applies each event to every module before moving to the next event.

The consumer starts only after all modules finish `Prepare`. During
`Initialize`, catchup is complete when the consumer drains the initial batch and
the runtime enters `Ready`. Events appended while the initial batch is draining
remain in the shared buffer. After `Ready`, the same consumer keeps draining
future events through the same serialized path.

If the initialization context is canceled, the runtime is being closed. The
manager cancels the build task, closes the runtime, and releases owned resources.
This close path is only a resource cleanup path and does not trigger QueryView
state-machine reports.

### 5.3 First QueryView Acquire

```text
QueryViewStateMachine.Acquire(qv)
  -> manager registers queryViewRef
  -> manager removes initRef in the same state update
  -> manager waits for QueryRuntimeBuildTask.Done
  -> manager calls QueryRuntime.Advance(qv.DataVersion)
  -> manager invokes OnReady asynchronously
  -> QueryView may report Up
```

The first QueryView transfers ownership from the load-config initialization
reference to QueryView references.

### 5.4 Later QueryView Acquire

```text
QueryViewStateMachine.Acquire(qv)
  -> manager registers queryViewRef
  -> manager waits for QueryRuntimeBuildTask.Done
  -> manager computes oldestDataVersion from all queryViewRefs
  -> manager calls QueryRuntime.Advance(oldestDataVersion)
  -> manager invokes OnReady asynchronously
```

`Acquire` never schedules another runtime. A vchannel already has a singleton
runtime.

### 5.5 QueryView Release

```text
QueryViewStateMachine.Release(qv)
  -> manager removes queryViewRef
  -> if queryViewRefs is non-empty:
         manager calls QueryRuntime.Advance(oldestDataVersion)
     else if initRef is absent:
         manager closes QueryRuntime
  -> manager invokes OnDropped asynchronously
```

### 5.6 Drop Load Config

```text
RecoveryStorage observes DropLoadConfig
  -> removes VChannelMeta.load_config
  -> SNQueryRuntimeManager.OnDropLoadConfig(event)
  -> manager removes initRef
  -> if queryViewRefs is empty:
         manager closes QueryRuntime
```

`DropLoadConfig` does not directly clean QueryView references and does not
delete QueryView meta.

### 5.7 Crash Recovery

Recovery rebuilds state from WAL metadata:

1. `RecoveryStorage` reads load config and QueryView metadata.
2. For each loaded vchannel, it chooses the recovery base DataVersion:
   - if persisted `Up` QueryViews exist, use the oldest `Up` QueryView
     DataVersion;
   - otherwise use the SegmentModule-provided maximum DataVersion.
3. `RecoveryStorage` builds a valid `VChannelWALView` for the chosen base.
4. `RecoveryStorage` calls `OnAlterLoadConfig(view)`.
5. `QueryViewStateMachine` replays QueryView metadata and calls `Acquire` in
   QueryViewVersion order.

### 5.8 WAL Handoff Close

WAL handoff means this node should release local resources because QueryViews
will be transferred to another node.

The close order is:

```text
PChannelRuntime.CloseForHandoff
  -> QueryViewStateMachine.CloseForHandoff
       -> Release all local QueryView refs
  -> SNQueryRuntimeManager.Close
       -> fast-cancel remaining runtimes and release resources
```

Persisted QueryView meta is not deleted by this resource close path. It remains
WAL-bound metadata for the next owner to recover.

`SNQueryRuntimeManager.Close` is not part of the QueryView state machine. It
does not call QueryView callbacks and does not report QueryView states. It stops
accepting new live events, cancels in-flight initialization, stops runtime
consumers, closes modules, and releases buffered events.
