# QueryView Event-Based Observability

## Package

The QueryView event model is defined in:

```text
internal/views/qviews/observe
```

Package layout:

```text
internal/views/qviews/observe
    event.go
    observer.go
```

`observe` depends on `internal/views/qviews`, `pkg/v3/mlog`, and the standard library. Coord,
QueryNode, and StreamingNode owner packages depend on `observe`.

## Observer

```go
type Observer interface {
    Observe(context.Context, Event)
}

type Registry struct {
    mu        sync.RWMutex
    observers []Observer
}

func NewRegistry(observers ...Observer) *Registry
func (r *Registry) Register(observer Observer)
func (r *Registry) Observe(ctx context.Context, event Event)

func Register(observer Observer)
func Observe(ctx context.Context, event Event)

type LogObserver struct{}

func (LogObserver) Observe(ctx context.Context, event Event) {
    level := event.LogLevel()
    if !mlog.LevelEnabled(level) {
        return
    }
    mlog.Log(ctx, level, "query view event", FieldEvent(event))
}
```

The package owns a global default registry initialized with `LogObserver`.
Component code emits events through `observe.Observe(ctx, event)`. Additional
observers, such as metrics observers, register through `observe.Register`.
The registry snapshots its observer list before fanout so observer execution
does not hold the registry lock.

## Event Interface

```go
type Event interface {
    mlog.ObjectMarshaler
    TriggerInfo
    ComponentInfo
    LogLevel() mlog.Level
    isQueryViewEvent()
}

type TriggerInfo interface {
    TriggerInfo() string
}

type ComponentInfo interface {
    ComponentInfo() string
}

type baseEvent struct{}

func (baseEvent) isQueryViewEvent() {}

func (baseEvent) TriggerInfo() string {
    return ""
}

func (baseEvent) ComponentInfo() string {
    return ""
}

func FieldEvent(event Event) mlog.Field {
    return mlog.Inline(event)
}
```

Every observable event is represented by one concrete Go type and embeds
`baseEvent`. Consumers log events with `FieldEvent`, which returns an inline
mlog field, and inspect events with type assertions or type switches. `LogLevel`
is part of the event contract so log observers can check whether the target
level is enabled before constructing the inline event field.

`TriggerInfo` is the low-cardinality reason associated with the event. Events
that do not represent a state-machine trigger return an empty string. Metrics
observers consume this value directly and must not re-derive trigger labels from
event type names.

`ComponentInfo` is the low-cardinality owner component associated with the
event. The current values are `coord`, `queryNode`, and `streamingNode`.
Metrics observers consume this value directly and must not re-derive component
labels from event type names.

```go
var _ Event = CoordViewQueryNodeLostAppliedEvent{}
var _ Event = QueryNodeSegmentUnrecoverableEvent{}

func Observe(ctx context.Context, event Event) {
    switch e := event.(type) {
    case CoordViewQueryNodeLostAppliedEvent:
        _ = e.Node
    case QueryNodeSegmentFailureEvent:
        _ = e.Err
    }
}
```

## Cardinality

Event type is split by cardinality. Each event payload carries the identity of
the observed object. The supported cardinalities are `node`, `view`,
`segment`, `view-segments`, `resource`, `persist`, and `sync-batch`.

## Shared Types

```go
type ViewStateTransition struct {
    CollectionID int64
    View         qviews.QueryViewKey
    From         qviews.QueryViewState
    To           qviews.QueryViewState
}
```

Events that describe a QueryView state-machine transition embed
`ViewStateTransition`. `CollectionID` is populated when the owner layer has the
collection identity available. Metrics use it only in bounded TopN diagnostic
series.

## Events By Cardinality

### Node

Node events observe one worker node.

#### Coord

```go
// CoordQueryNodeLostDetectedEvent is emitted when Coord sync code observes
// QueryNode loss.
type CoordQueryNodeLostDetectedEvent struct {
    baseEvent
    Node qviews.QueryNode
}
```

### View

View events observe one QueryView.

#### Coord

```go
// CoordViewCreatedEvent is emitted after Coord creates a new Preparing view.
type CoordViewCreatedEvent struct {
    baseEvent
    CollectionID int64
    View         qviews.QueryViewKey
    State        qviews.QueryViewState
}

// CoordViewPreemptedEvent is emitted after Coord preempts a Preparing or Ready
// view while adding a new Preparing view.
type CoordViewPreemptedEvent struct {
    baseEvent
    ViewStateTransition
    PreemptingDataVersion qviews.DataVersion
}

// CoordViewAdvancedFromUnrecoverableEvent is emitted after Coord advances an
// Unrecoverable view to Dropping.
type CoordViewAdvancedFromUnrecoverableEvent struct {
    baseEvent
    ViewStateTransition
}

// CoordViewReleaseRequestedEvent is emitted after ShardViewManager applies
// RequestRelease to a view.
type CoordViewReleaseRequestedEvent struct {
    baseEvent
    ViewStateTransition
}

// CoordViewHandoffToNewUpEvent is emitted after ShardViewManager transitions
// the previous Up view to Down because another view became Up.
type CoordViewHandoffToNewUpEvent struct {
    baseEvent
    ViewStateTransition
    NewUpView qviews.QueryViewKey
}

// CoordViewReportAppliedEvent is emitted after ShardViewManager applies a
// work-node report to a view. ResourceReadyPercent is the report-side resource
// preparation progress in [0, 100]. StreamingNode reports derive this value
// from view state: resource-ready states report 100, other states report 0.
type CoordViewReportAppliedEvent struct {
    baseEvent
    ViewStateTransition
    Node                 qviews.WorkNode
    ReportedState        qviews.QueryViewState
    ResourceReadyPercent int64
}

// CoordViewQueryNodeLostAppliedEvent is emitted after ShardViewManager applies
// QueryNode loss to a view.
type CoordViewQueryNodeLostAppliedEvent struct {
    baseEvent
    ViewStateTransition
    Node qviews.QueryNode
}
```

#### QueryNode

```go
// QueryNodeApplyCoordViewEvent is emitted after QueryNode applies a Coord view
// state.
type QueryNodeApplyCoordViewEvent struct {
    baseEvent
    ViewStateTransition
}

// QueryNodeSegmentUnrecoverableEvent is emitted after the segment
// unrecoverable callback moves a view to Unrecoverable.
type QueryNodeSegmentUnrecoverableEvent struct {
    baseEvent
    ViewStateTransition
    Err error
}

// QueryNodeReportViewEvent is emitted when QueryNode reports local view state.
type QueryNodeReportViewEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}

// QueryNodeReleaseDoneEvent is emitted after QueryNode observes release
// completion for a view.
type QueryNodeReleaseDoneEvent struct {
    baseEvent
    ViewStateTransition
}
```

#### StreamingNode

```go
// StreamingNodeApplyCoordViewEvent is emitted after StreamingNode applies a
// Coord view state.
type StreamingNodeApplyCoordViewEvent struct {
    baseEvent
    ViewStateTransition
}

// StreamingNodeRecoveringDoneEvent is emitted after StreamingNode observes
// recovery completion for a view.
type StreamingNodeRecoveringDoneEvent struct {
    baseEvent
    ViewStateTransition
}

// StreamingNodeReportViewEvent is emitted when StreamingNode reports local view
// state.
type StreamingNodeReportViewEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}

// StreamingNodeReleaseDoneEvent is emitted after StreamingNode observes release
// completion for a view.
type StreamingNodeReleaseDoneEvent struct {
    baseEvent
    ViewStateTransition
}
```

### Segment

Segment events observe one segment inside one QueryView.

#### QueryNode

```go
// QueryNodeSegmentFailureEvent is emitted when physical segment load or
// transform-log catch-up fails.
type QueryNodeSegmentFailureEvent struct {
    baseEvent
    View      qviews.QueryViewKey
    SegmentID int64
    Err       error
}
```

### View-Segments

View-segments events observe one segment batch inside one QueryView.

#### QueryNode

```go
// QueryNodeAcquireSegmentsEvent is emitted when QueryNode starts acquiring
// segments for a new Preparing view.
type QueryNodeAcquireSegmentsEvent struct {
    baseEvent
    View         qviews.QueryViewKey
    SegmentCount int
}

// QueryNodeSegmentsReadyEvent is emitted after the segment readiness callback
// moves a view forward.
type QueryNodeSegmentsReadyEvent struct {
    baseEvent
    ViewStateTransition
    ReadySegmentCount int
}

// QueryNodeReleaseSegmentsEvent is emitted when QueryNode starts releasing
// segments for a view.
type QueryNodeReleaseSegmentsEvent struct {
    baseEvent
    View qviews.QueryViewKey
}
```

### Resource

Resource events observe one StreamingNode resource for one QueryView.

#### StreamingNode

```go
// StreamingNodeAcquireResourceEvent is emitted when StreamingNode starts
// acquiring resources for a new Preparing view.
type StreamingNodeAcquireResourceEvent struct {
    baseEvent
    View qviews.QueryViewKey
}

// StreamingNodeRecoverAcquireResourceEvent is emitted when StreamingNode starts
// acquiring resources for a recovered Up view.
type StreamingNodeRecoverAcquireResourceEvent struct {
    baseEvent
    View qviews.QueryViewKey
}

// StreamingNodeResourceReadyEvent is emitted after the resource ready callback
// moves a view forward.
type StreamingNodeResourceReadyEvent struct {
    baseEvent
    ViewStateTransition
}

// StreamingNodeReleaseResourceEvent is emitted when StreamingNode starts
// releasing resources for a view.
type StreamingNodeReleaseResourceEvent struct {
    baseEvent
    View qviews.QueryViewKey
}
```

### Persist

Persist events observe one persisted QueryView state write.

#### Coord

```go
// CoordPersistViewEvent is emitted when ShardViewManager.flush persists a view
// state.
type CoordPersistViewEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}
```

#### StreamingNode

```go
// StreamingNodePersistViewEvent is emitted when StreamingNode persists local
// view state.
type StreamingNodePersistViewEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}
```

### Sync-Batch

Sync-batch events observe one QueryView sync to one worker-node batch.

#### Coord

```go
// CoordSyncViewBatchEvent is emitted when ShardViewManager.flush syncs a view
// state to one worker-node batch.
type CoordSyncViewBatchEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
}

// CoordSyncViewBatchFailedEvent is emitted when ShardViewManager.flush fails to
// sync a view state to one worker-node batch.
type CoordSyncViewBatchFailedEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    State qviews.QueryViewState
    Err   error
}
```

## Emission Semantics

Event emission runs in the owner layer that observes the action.

Transition-carrying events are emitted in the same critical section that
observes the state transition. `From` and `To` must match the owner-visible
state before and after the state-machine input.

Observer invocation is synchronous. Observer implementations must not block the
owner workflow. Observer failures must not change QueryView state-machine,
persistence, sync, or resource-release behavior.
