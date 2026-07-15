# StreamingNode VChannel WAL Input View

> Internal preparation DTO built by `VChannelRecoveryModule` for QueryRuntime
> modules.

## 1. Purpose

`walview.VChannelWALView` packages the WAL-derived input needed to initialize a
StreamingNode query runtime for one QueryView. It is no longer a
`RecoveryStorage` API and it is not published through load callbacks.

The owning flow is:

```text
QueryViewStateMachine.Acquire
  -> PChannelRecoveryManager.Acquire
  -> VChannelRecoveryModule.queryWALViewLocked
  -> QueryRuntime.Initialize
  -> QueryRuntimeModule.Prepare
```

## 2. Owner

`VChannelRecoveryModule` is the only component that builds the view. It already
owns all inputs needed for a consistent VChannel-level snapshot:

- `VChannelMeta` and schema history;
- growing Segment metadata and insert buffers;
- Segment DataVersion summaries;
- TransformLog metadata and scanner construction;
- the live DML observe path after recovery.

`RecoveryStorage` only recovers and persists the module state. It does not
construct WAL views, register live observers, or expose a WALView ability.

## 3. Contents

`VChannelWALView` contains:

- PChannel, VChannel, and CollectionID identity;
- base growing and transform timeticks;
- QueryView settings converted to the existing load-config-shaped runtime input
  where needed;
- collection schema;
- visible growing segment snapshot at the selected QueryView DataVersion;
- delete replay scanner for TransformLog entries needed by the snapshot.

The view is a preparation input. `QueryRuntime` and its modules must not retain
the view as an owned runtime resource after `Prepare` returns.

## 4. Live Events

After the initial view is built, live changes are delivered by the same
`VChannelRecoveryModule` that owns the runtime:

```text
VChannelRecoveryModule.ObserveMessage
  -> update VChannelMeta / Segment state / TransformLog
  -> QueryRuntime.ObserveEvent
  -> QueryRuntimeModule.ApplyLiveEvent
```

This keeps initial recovery and live DML consumption under the same VChannel
ownership boundary. `RecoveryStorage` does not expose any live observer
interface.

## 5. DataVersion Selection

The QueryView state machine provides the target QueryView meta during
`Acquire`. `VChannelRecoveryModule` uses the QueryView DataVersion and
TransformLog start boundary to build the visible segment snapshot and delete
replay scanner. Multiple live QueryViews share the same VChannel singleton
runtime; the runtime advances to the oldest DataVersion still referenced by
active QueryViews.
