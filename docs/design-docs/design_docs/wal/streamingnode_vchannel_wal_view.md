# StreamingNode VChannel WAL View Design

> VChannel-level WAL input view used by the PChannel-local
> `SNQueryRuntimeManager` to prepare StreamingNode query resources after
> `AlterLoadConfig`.
> References: [WAL Recovery Architecture](wal-recovery-architecture.md),
> [Segment View Module](segment_view_module.md),
> [TransformLog View Module](transform_log_view_module.md), and
> [StreamingNode Query Runtime Manager Design](../qviews/snview/streamingnode_resource_manager.md).

## 1. Goal

`AlterLoadConfig` is the WAL event that starts StreamingNode-side resource
preparation for a loaded vchannel. At that WAL observe point,
the PChannel-local `SNQueryRuntimeManager` needs one consistent input
package:

```text
latest schema and load_config from VChannelModule
historical Insert state from SegmentModule
historical Delete replay from TransformLogModule
live WAL/resource events after capture from RecoveryStorage
two TimeTick watermarks for growing and transform MVCC
```

This input package is `VChannelWALView`. RecoveryStorage creates it at a serialized
WAL observe point and passes it to a load-config listener implemented by the
same PChannelRuntime's `SNQueryRuntimeManager`.

The core guarantee of `VChannelWALView` is no-gap WAL input handoff:

```text
data visible at the capture point
  -> VChannelWALView historical inputs

WAL/resource events after the capture point
  -> VChannelLiveObserver
```

Non-goals:

- RecoveryStorage does not build csegments.
- RecoveryStorage does not fetch BM25 sealed resources.
- RecoveryStorage does not wait for query resources to become ready.
- This document does not define QueryNode sealed loading or query execution.

## 2. Timeline

```text
AlterLoadConfig is observed by RecoveryStorage
  -> modules observe the message and update their in-memory state
  -> RecoveryStorage captures current module views
  -> RecoveryStorage creates VChannelWALView
  -> RecoveryStorage calls LoadConfigListener.OnAlterLoadConfig(view)
  -> PChannel-local SNQueryRuntimeManager returns a VChannelLiveObserver
     to RecoveryStorage
  -> PChannel-local SNQueryRuntimeManager asynchronously loads historical
     Insert and Delete
  -> RecoveryStorage synchronously pushes later live resource events to the observer
  -> resource runtime advances source-specific applied TimeTicks
```

The capture is serialized with WAL observe. Modules do not need a
`SnapshotAt(TimeTick)` API. They expose their current state while RecoveryStorage is
at the capture point; RecoveryStorage owns later live message dispatch.

### 2.1 No-Gap Input Contract

For one vchannel and one resource base DataVersion, `VChannelWALView` guarantees
that StreamingNode resource preparation receives a continuous WAL input stream:

1. Historical Insert state before the capture point is represented by
   `SegmentSnapshot`.
2. Historical Delete/Transform state before the capture point is represented by
   `DeleteReplay`, bounded by `BaseTransformTimeTick`.
3. WAL/resource events after the capture point are delivered through the
   `VChannelLiveObserver` returned by `LoadConfigListener.OnAlterLoadConfig`.
4. RecoveryStorage registers the returned observer before it dispatches any
   later event for the vchannel to registered live observers.
5. After registration, RecoveryStorage offers later WAL message events to the
   observer in WAL observe order, after normal module observation and
   checkpoint/barrier updates.
6. Segment resource metadata events derived from module side effects, such as
   `SegmentSealedEvent`, are delivered through the same observer after the
   owning module records the state change.
7. The observer path is lossless while it is registered: a full observer buffer
   must apply backpressure instead of silently dropping messages.

Returning a nil observer means the listener declines this WALView and no no-gap
resource handoff is established for that callback. A registered observer that
later returns false is unregistered; after that point the listener has
explicitly abandoned this live handoff.

## 3. TimeTick Watermarks

`VChannelWALView` exposes two logical TimeTick watermarks. It does not expose WAL
`MessageID`; physical WAL positions remain RecoveryStorage internals.

```text
BaseGrowingTimeTick =
    max(
        SegmentModule.LatestInsertTimeTick(vchannel),
        TransformLogModule.LatestTransformTimeTick(vchannel),
    )

BaseTransformTimeTick =
    TransformLogModule.LatestTransformTimeTick(vchannel)
```

Definitions:

- `SegmentModule.LatestInsertTimeTick(vchannel)` is the latest Insert TimeTick
  observed by SegmentModule for this vchannel.
- `TransformLogModule.LatestTransformTimeTick(vchannel)` is the latest Delete or
  Txn(Delete) TimeTick observed by TransformLogModule for this vchannel. This is
  a strong constraint: it must equal the latest transform message currently visible
  in TransformLogModule state.

The two watermarks are intentionally different:

- StreamingNode growing resources observe both Insert and TransformLog, so their
  initial MVCC frontier can advance to the latest insert-or-transform TimeTick.
- Sealed resources only consume TransformLog and never see StreamingNode-only
  Insert messages, so sealed-side transform waiting can only use the transform
  TimeTick.

`VChannelWALView` exports `BaseTransformTimeTick` so the resource layer can use one
transform read bound consistently. SN growing resource preparation uses it to bound
historical Delete replay.

## 4. Ownership

```text
RecoveryStorage
  owns the serialized capture point, creates VChannelWALView, computes the two
  base TimeTick watermarks, and synchronously dispatches later live resource
  events to observers returned by the listener.

VChannelModule
  owns schema, partition, collection metadata, and persisted load_config.

SegmentModule
  owns growing segment state and exposes lightweight visible segment snapshots.
  It does not own observer fanout.

TransformLogModule
  owns durable Delete history and provides historical Delete replay descriptors.

SNQueryRuntimeManager
  implements LoadConfigListener, consumes VChannelWALView, starts an asynchronous
  WALView load task, returns a live observer, and tracks runtime apply frontier.
  It is scoped to the same PChannelRuntime as RecoveryStorage.
```

The dependency direction is one-way:

```text
RecoveryStorage -> LoadConfigListener
```

The listener receives an already captured `VChannelWALView`; it must not call back
into RecoveryStorage to pull one.

The `walview` Go package only defines these interfaces and DTOs. Concrete live
observer fanout, bounded delete replay, and module snapshot construction live in
RecoveryStorage, TransformLog, SegmentModule, or VChannelModule implementations.

## 5. Listener

The listener interface should live on the WAL side to avoid importing
StreamingNode resource-management packages from RecoveryStorage.

```go
type LoadConfigListener interface {
    OnAlterLoadConfig(view VChannelWALView) VChannelLiveObserver
    OnDropLoadConfig(event DropLoadConfigEvent)
}

type DropLoadConfigEvent struct {
    PChannel     string
    VChannel     string
    CollectionID int64
}
```

The PChannel-local concrete `SNQueryRuntimeManager` implements this
listener interface.
Callback contract:

- `OnAlterLoadConfig` and `OnDropLoadConfig` only hand off intent and handles; they
  must return quickly.
- callbacks must not build csegments, perform BM25 RPC, or replay TransformLog.
- `OnAlterLoadConfig` creates a short-lived WALView load task, starts asynchronous
  resource preparation, and returns the observer that RecoveryStorage will use for
  later resource events. Returning nil means the listener declines live observation.
- missed in-memory callbacks are recovered by RecoveryStorage scanning persisted
  `VChannelMeta.load_config` after WAL open and re-emitting `OnAlterLoadConfig`.

This callback contract is separate from live observer delivery. Live observer
delivery happens later on the WAL consumption path and may block as described in
[Live Observer](#8-live-observer). RecoveryStorage does not expose a pull-style
`CreateVChannelWALView` API; creating a view is only an `AlterLoadConfig` or
recovered-load-config side effect.

## 6. VChannelWALView

```go
type VChannelWALView struct {
    PChannel     string
    VChannel     string
    CollectionID int64

    BaseGrowingTimeTick   uint64
    BaseTransformTimeTick uint64

    LoadConfig *streamingpb.VChannelLoadConfig
    Schema     *schemapb.CollectionSchema

    SegmentSnapshot VisibleSegmentSnapshot
    DeleteReplay     wal.TransformLogScanner
}
```

`VChannelWALView` has no `Close` method. Historical data is represented by
`message.ImmutableMessage` and is released by Go GC when no runtime references it.
The live observer is not part of the view; it is returned by
`LoadConfigListener.OnAlterLoadConfig` so the listener can decide how to buffer or
apply live resource events.

## 7. Segment Snapshot

The segment snapshot is the historical Insert side of the view. It must include
all visible growing-side rows through `BaseGrowingTimeTick`, including rows that
are still only in memory and have not yet been flushed into persisted storage.

```go
type VisibleSegmentSnapshot struct {
    CollectionID        int64
    VChannel            string
    DataVersion         qviews.DataVersion
    BaseGrowingTimeTick uint64
    Segments            []VisibleSegment
}

type VisibleSegment struct {
    SegmentID   int64
    PartitionID int64

    Schema *schemapb.CollectionSchema

    Assignment          *streamingpb.SegmentAssignmentMeta
    SealedAtDataVersion *viewpb.DataVersion

    Data SegmentSnapshotData
}

type SegmentSnapshotData struct {
    PersistedStorage *streamingpb.L1SegmentPersistedStorage

    // Raw WAL messages observed by SegmentModule but not yet included in
    // PersistedStorage. These may be Insert or Txn messages and are part of
    // BaseGrowingTimeTick. Growing BM25 deltas are derived from the same messages.
    InsertMessages []message.ImmutableMessage
}
```

Snapshot rules:

1. Metadata may be cloned because it is small and protects against later mutation.
2. In-memory insert buffers are represented by immutable WAL messages, not copied
   insert data or reference-counted handles.
3. The snapshot must not expose mutable SegmentModule-owned maps or buffers.
4. `message.ImmutableMessage` must not depend on scanner buffer reuse.

For `InsertMessages`, use the simple shape above. The buffer stores raw immutable
WAL messages, including Txn messages. Consumers must use a shared parser to expand
Insert or Txn messages and select only the assignments that match the surrounding
`VisibleSegment.SegmentID`; they must not blindly load the whole WAL message into
every segment.

## 8. Live Observer

The live observer is vchannel-level, not segment-level, because Delete is scoped to
the vchannel/partition and must stay ordered with Insert. RecoveryStorage owns this
observer registration and dispatches resource events after normal module observation
or module-owned side-effect completion. SegmentModule and TransformLogModule do not
register query observers or publish query live events directly.

```go
type VChannelLiveObserver interface {
    ObserveEvent(ctx context.Context, event VChannelResourceEvent) bool
    Close()
}

type VChannelResourceEvent struct {
    Message       message.ImmutableMessage
    SegmentSealed *SegmentSealedEvent
}

type SegmentSealedEvent struct {
    SegmentID           int64
    VChannel            string
    SealedAtDataVersion qviews.DataVersion
}
```

Observer contract:

- WAL message events are emitted in WAL observe order;
- WAL message events are emitted only for WAL messages observed after this view
  is captured;
- RecoveryStorage calls `ObserveEvent` for WAL message events after modules
  observe the same WAL message and after RecoveryStorage updates its in-memory
  checkpoint/barrier state;
- dispatch uses the original `message.ImmutableMessage` inside the resource
  event, not decoded data copy;
- resource metadata events such as `SegmentSealedEvent` are emitted after the
  owning module records the state change;
- the observer must not silently drop events;
- `ObserveEvent` returns false when the observer is closed or should be
  unregistered;
- once `ObserveEvent` returns false, RecoveryStorage unregisters the observer
  and the no-gap handoff for that listener is ended;
- csegment Insert/Delete and BM25 updates are applied asynchronously by the
  resource runtime, not by RecoveryStorage or SegmentModule.

Backpressure:

- the returned observer owns a bounded live-message buffer;
- observer `ObserveEvent` appends the resource event to that buffer;
- when the buffer is full, `ObserveEvent` blocks instead of dropping events;
- blocking live observer delivery slows RecoveryStorage WAL consumption;
- this is the intended backpressure path and propagates through normal WAL
  checkpoint/ack behavior to the write side;
- creating `VChannelWALView` itself still adds no Meta/Data barrier and does not
  wait for query resource readiness.

This design does not require RecoveryStorage to create a goroutine per observer.
If `SNQueryRuntimeManager` wants asynchronous application, the returned
observer owns that buffering policy internally; RecoveryStorage still performs
one synchronous call per later event.

## 9. Historical Delete Replay

Historical Delete is supplied through a bounded `wal.TransformLogScanner` and bounded
by `BaseTransformTimeTick`. The scanner is already bound to the required historical
range when RecoveryStorage creates the `VChannelWALView`; consumers only need to
read it to completion and apply every returned Delete entry.

```go
DeleteReplay wal.TransformLogScanner
```

Contract:

```text
DeleteReplay returns all Delete entries required by the visible segment snapshot,
bounded by VChannelWALView.BaseTransformTimeTick.

The end bound is:
  VChannelWALView.BaseTransformTimeTick

The start bound is chosen internally by RecoveryStorage. A valid conservative
choice is:
  min visible segment create timetick - 1
```

RecoveryStorage may use a tighter delete start point when SegmentModule or
DataView exposes one. The start and end bounds are implementation details of
scanner construction; `SNQueryRuntimeManager` must not depend on them.

The end bound is expressed through `wal.TransformLogReadOption.EndTimeTick`.
RecoveryStorage creates the scanner directly instead of exposing a TransformLog
accesser or replay factory through `VChannelWALView`.
When `BaseTransformTimeTick` is zero, RecoveryStorage returns an already-complete
empty scanner instead of using `EndTimeTick=0`, because zero keeps the generic
TransformLog read path unbounded for existing callers.

## 10. Capture Flow

When RecoveryStorage observes `AlterLoadConfig`:

```text
1. Modules observe the AlterLoadConfig message.
2. VChannelModule persists VChannelMeta.load_config and returns a Meta barrier.
3. RecoveryStorage computes BaseGrowingTimeTick from SegmentModule and TransformLogModule summaries.
4. RecoveryStorage computes BaseTransformTimeTick from TransformLogModule summary.
5. RecoveryStorage captures schema/load_config from VChannelModule.
6. RecoveryStorage asks SegmentModule for the visible segment snapshot selected
   by the resource recovery base DataVersion.
7. RecoveryStorage asks TransformLogModule for a Delete replay scanner bounded
   by BaseTransformTimeTick.
8. RecoveryStorage calls listener.OnAlterLoadConfig(view).
9. RecoveryStorage registers the returned VChannelLiveObserver, if non-nil.
10. RecoveryStorage continues WAL consumption without waiting for query resources.
```

For later resource events:

```text
1. For WAL messages, RecoveryStorage observes the message through VChannelModule,
   SegmentModule, TransformLogModule, and AckModule.
2. RecoveryStorage updates its in-memory checkpoint/barrier state.
3. RecoveryStorage calls matching vchannel observers with a resource event that
   wraps the original immutable message.
4. For module side-effect events such as SegmentSealedEvent, RecoveryStorage
   calls matching vchannel observers after the owning module records the event.
```

## 11. Runtime Build Flow

`SNQueryRuntimeManager` consumes the view asynchronously through its
WALView load task:

```text
1. Create a WALView load task and its live message buffer.
2. Return a VChannelLiveObserver to RecoveryStorage before expensive work starts.
3. Load SegmentSnapshot into csegment runtime.
4. Read DeleteReplay to completion and apply every returned Delete entry.
5. Set growing runtime applied TimeTick to BaseGrowingTimeTick.
6. Expose BaseTransformTimeTick to the resource layer as the transform read bound.
7. Advance applied TimeTicks after each WAL message event is applied to csegment,
   growing BM25, or transform buffers.
8. Publish resource readiness only after the historical inputs are attached and
   the live observer is owned by the runtime.
```

This preserves MVCC without making `RecoveryStorage.ObserveMessage` or
`SegmentModule.ObserveMessage` synchronously call csegment `Insert` or `Delete`.
The WALView load task is volatile. It is not a retention anchor and does not
participate in DataVersion GC. Long-term retention is driven by the
`SNQueryRuntimeManager` reference model defined in
[StreamingNode Query Runtime Manager Design](../qviews/snview/streamingnode_resource_manager.md).
Repeated `AlterLoadConfig` callbacks for a vchannel that already has an in-flight
WALView load task are ignored by the resource manager and return no observer.
They do not cancel or replace the existing task.

## 12. Recovery

The listener callback itself is volatile. On StreamingNode restart:

```text
PChannelRuntime restores VChannelMeta.load_config, SegmentModule, TransformLogModule,
and persisted QueryView meta. QueryViewStateMachine provides the oldest recovered
Up QueryView DataVersion when resource recovery needs it. After bounded replay
and module switch, RecoveryStorage builds VChannelWALView using the selected
resource recovery base DataVersion and re-emits OnAlterLoadConfig callbacks.
```

The fresh view computes new base TimeTick watermarks from recovered module state.
Crash recovery does not depend on any pre-crash observer or resource runtime state.
The recovery base DataVersion selection is defined by
[StreamingNode Query Runtime Manager Design](../qviews/snview/streamingnode_resource_manager.md).
