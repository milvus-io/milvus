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
- TransformLog metadata and the shared PChannel stream;
- the live DML observe path after recovery.

`RecoveryStorage` only recovers and persists the module state. It does not
construct WAL views, register live observers, or expose a WALView ability.

## 3. Contents

`VChannelWALView` contains:

- PChannel, VChannel, and CollectionID identity;
- base growing and transform timeticks. The growing base is initialized from
  the persisted Data checkpoint and advances only when the VChannel observes a
  real data envelope; Meta-only replay never advances it;
- QueryView settings converted to the existing load-config-shaped runtime input
  where needed;
- collection schema;
- visible growing segment snapshot at the selected QueryView DataVersion;
- the shared PChannel TransformLog stream and the vchannel delete replay
  boundary needed by the snapshot.

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

WALView capture and QueryRuntime registration use the same VChannel lock:

```text
hold VChannel lock
  -> capture Segment state at dataObservedTimeTick
  -> capture the independent TransformLog frontier
  -> construct VChannelWALView
  -> install QueryRuntime in Preparing state
release VChannel lock
```

Messages consumed before capture are represented by persisted storage, pending
Segment buffers, pending flush chunks, or TransformLog state. Messages consumed
after capture see the installed QueryRuntime and enter its pending event queue.
`VChannelRecoveryModule` deep-copies a data-scanner message before queueing it;
QueryRuntime therefore owns an ordinary immutable copy and never receives a
Message Ack handle.
DataScanner therefore does not need to stop or catch up to the startup barrier
before QueryRuntime preparation begins.

This no-gap handoff permits resource preparation to finish before replay catches
up, but it does not make newer query-plan MVCC immediately visible. Query task
acquisition waits on the runtime's Growing and Transform frontiers when the
requested plan TimeTicks are ahead of the captured WALView bases.

## 5. DataVersion Selection

The QueryView state machine provides the target QueryView meta during
`Acquire`. `VChannelRecoveryModule` uses the QueryView DataVersion and
TransformLog start boundary to build the visible segment snapshot and delete
replay subscription parameters. `GrowingRuntime.Prepare` creates the bounded
vchannel subscription on the PChannel-owned stream and closes only that
subscription after replay. Multiple live QueryViews share the same VChannel
singleton runtime; the runtime advances to the oldest DataVersion still
referenced by active QueryViews.

WAL view capture has the following readiness preconditions:

1. bounded Meta-only recovery is complete and modules have switched into
   MetaAndData mode;
2. no retained segment is `FLUSHED` with a nil `SealedAtDataVersion`;
3. any missing value has triggered or reused the segment's idempotent final
   commit and WAL view capture waits for its completion.

There is no precondition that DataScanner has reached `RecoveryBarrier`. The
WALView declares only the data frontier actually included in its snapshot, and
later replay is delivered through QueryRuntime's pending event queue.

After these conditions hold, snapshot selection is purely per segment:

```text
GROWING
    -> SegmentSnapshot.Segments
FLUSHED && SealedAtDataVersion > target DataVersion
    -> SegmentSnapshot.Segments
FLUSHED && SealedAtDataVersion <= target DataVersion
    -> SegmentSnapshot.FlushedSegments
```

The comparison uses the complete DataVersion. There is no requirement that the
maximum sealed version observed by this VChannel reaches the QueryView version.
DataVersion is collection-level and may advance because another VChannel
flushed independently.
