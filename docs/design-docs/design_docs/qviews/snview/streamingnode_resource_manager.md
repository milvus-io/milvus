# StreamingNode Query Resource Design

> StreamingNode-side query runtime ownership for QueryView.

This document defines the PChannel-local recovery manager, the VChannel-level
query runtime, and the reference model used by WAL recovery and the
StreamingNode QueryView state machine.

## 1. Purpose

StreamingNode query resources are owned by the WAL recovery model:

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       -> QueryRuntime
            -> QueryRuntimeModule*
```

`RecoveryStorage` restores WAL state and exposes the recovered
`PChannelRecoveryManager`. It does not publish load callbacks, expose live
observers, or own WALView construction APIs.

`VChannelRecoveryModule` owns one VChannel's `VChannelMeta`, Segment state,
TransformLog, DataView recovery state, query-runtime references, build task, and
live DML dispatch. Once a runtime is recovered, the module continues consuming
DML and the DataView visible to the runtime only grows while the QueryView is
live.

## 2. Boundaries

| Component | Role | Boundary |
|---|---|---|
| `RecoveryStorage` | Restores PChannel WAL state, manages checkpoints, persists dirty module snapshots, and exposes `VChannelManager()`. | It does not build query runtimes, publish load callbacks, or own live observers. |
| `PChannelRecoveryManager` | Owns all `VChannelRecoveryModule` instances on one PChannel, the vchannel index, the shared build scheduler, and the live-event dispatcher. Implements `snview.StreamingNodeResourceManager`. | It does not own QueryView state transitions. |
| `VChannelRecoveryModule` | Owns one VChannel's metadata, growing segments, TransformLog, DataView recovery, QueryRuntime lifecycle, and DML event dispatch. | It does not coordinate across VChannels except through manager-provided scheduler/dispatcher. |
| `QueryRuntime` | Owns one live-event buffer, initializes resource modules from a WAL input view, drains buffered events in WAL order, and advances DataVersion watermarks. | It does not own QueryView references or WAL module snapshots. |
| `QueryRuntimeModule` | Common lifecycle interface implemented by growing segment runtime, IDF oracle runtime, and future resource modules. | It does not manage QueryView references or runtime scheduling. |
| `QueryViewStateMachine` | Owns QueryView transitions. Calls `Acquire` when a local QueryView starts using StreamingNode resources and `Release` when the QueryView leaves. | It does not build csegments, BM25 resources, TransformLog scanners, or DataView snapshots. |

## 3. Normal Acquire

Resource loading is driven by QueryView state transitions:

```text
QueryViewStateMachine.Acquire(qv)
  -> PChannelRecoveryManager.Acquire
  -> VChannelRecoveryModule registers the QueryView reference
  -> if no runtime exists:
       build WAL input view from QueryView meta + DataView + TransformLog
       create QueryRuntime
       submit build task to the shared scheduler
  -> wait for runtime build
  -> advance QueryRuntime to the oldest referenced DataVersion
  -> invoke OnReady asynchronously
```

`AlterLoadConfig` does not create VChannel-local load state. QueryView metadata
identifies the versioned load info, and the QueryView state machine is the load
trigger.

## 4. Live DML

After a runtime exists, the owning VChannel module forwards live resource events:

```text
VChannelRecoveryModule.ObserveMessage
  -> update VChannelMeta / Segment state / TransformLog
  -> QueryRuntime.ObserveEvent
  -> ordered ApplyLiveEvent on each QueryRuntimeModule
```

Events observed while the runtime build task is still running are buffered in
the same `QueryRuntime` event buffer. After the runtime becomes ready, the shared
dispatcher drains future events through the same per-runtime serialized path.

## 5. References

The resource reference model is QueryView-only:

```text
resource refs = queryViewRefs[QueryViewVersion]
```

Rules:

1. `Acquire(QueryView)` creates a QueryView reference.
2. The first successful `Acquire` creates the VChannel singleton `QueryRuntime`
   if one does not already exist.
3. Later `Acquire` calls add references and advance the runtime to the oldest
   referenced DataVersion.
4. `Release(QueryView)` removes the corresponding reference.
5. Resources can be closed only after all QueryView references are gone.
6. WAL handoff close drains QueryView references through
   `QueryViewStateMachine.CloseForHandoff` before the manager is closed.

## 6. Crash Recovery

Recovery rebuilds state from WAL metadata and QueryView metadata:

1. `RecoveryStorage` recovers `PChannelRecoveryManager` from VChannel metadata,
   Segment metadata, TransformLog metadata, and WAL replay.
2. `SNQueryViewHandler` recovers persisted QueryView state.
3. Recovered QueryView state machines call `Acquire` for local resources.
4. `VChannelRecoveryModule` builds the WAL input view from QueryView meta and
   its owned DataView/TransformLog state.
5. After runtime initialization, the module continues consuming DML and
   dispatching live resource events to the runtime.

## 7. Interfaces

```go
type StreamingNodeResourceManager interface {
    Acquire(req AcquireResource)
    Release(req ReleaseResource)
}

type QueryRuntimeModule interface {
    Prepare(ctx context.Context, view walview.VChannelWALView) error
    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)
    Advance(version qviews.DataVersion)
    Close()
}
```

The `walview.VChannelWALView` type remains an internal preparation DTO for
runtime modules. It is not exposed by `RecoveryStorage`.
