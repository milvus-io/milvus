# QueryView Metric-Based Observability

## 1. Goal

This document defines the first metric layer for QueryView observability.
It builds on [QueryView Event-Based Observability](event-based-observability.md)
and keeps the same owner-layer event boundary.

The metric layer should answer the primary operational question:

```text
For a collection replica shard, why is there no stable Up QueryView?
```

The first iteration focuses on QueryCoord-side lifecycle diagnosis and the
minimum worker-node progress signals needed to distinguish Coord, sync,
QueryNode preparation, and StreamingNode preparation bottlenecks.

## 2. Non-Goals

This design does not change QueryView state-machine behavior. In particular,
it does not introduce Preparing timeout eviction, new recovery policy, or
Balancer policy changes.

This design does not put high-cardinality identities into default aggregate
metric labels. QueryView version, DataVersion, segment ID, and raw error
message remain in structured event logs unless a metric is explicitly defined
as a bounded TopK diagnostic metric. Metrics provide aggregation and alerting;
logs and TopK diagnostics provide single-view investigation detail.

## 3. Architecture

The metric layer is implemented as an additional `observe.Observer`.

```text
Owner code
  -> observe.Observe(ctx, event)
      -> LogObserver
      -> MetricsObserver
```

`MetricsObserver` consumes existing QueryView events with a type switch. It has
two responsibilities:

1. Increment event-derived counters and histograms directly.
2. Maintain a small in-memory state cache for gauges that need current state or
   elapsed time.

Transition trigger labels come from `Event.TriggerInfo()`. Component labels
come from `Event.ComponentInfo()`. Metrics code must not maintain its own
event-type-to-label mapping for these fields.

Observer invocation is synchronous. `MetricsObserver` must not perform IO,
query external state, or block owner workflows. It may take short mutexes for
in-memory updates and update Prometheus metrics.

## 4. Metric Classes

### 4.1 Event Metrics

Event metrics are derived from one event at a time and do not need historical
state.

Examples:

- State transition counters.
- Unrecoverable counters.
- Sync failure counters.
- Segment preparation result counters.
- StreamingNode resource preparation result counters.

### 4.2 State Metrics

State metrics need owner-visible state accumulated across events.

Examples:

- Current QueryView count by state.
- Shard count without an Up QueryView.
- Preparing age.
- Reported worker resource readiness.
- Pending sync count.

These metrics are maintained by `MetricsObserver` state tables keyed by
QueryView and shard identities. Those keys may be high cardinality internally,
but metrics exported from these tables must be aggregated at cluster level
unless a bounded TopK or debug-only metric is explicitly introduced.

## 5. Label Policy

Allowed default labels:

| Label | Usage |
|---|---|
| `component` | QueryView event owner component: `coord`, `queryNode`, or `streamingNode`. |
| `state` | QueryView state. |
| `from_state` | State transition source. |
| `to_state` | State transition target. |
| `trigger` | Low-cardinality state-machine trigger. |
| `le` | Prometheus-compatible cumulative bucket upper bound. |
| `reason` | Low-cardinality failure or no-Up reason. |
| `node_role` | `querynode` or `streamingnode`. |
| `node_id` | Allowed for worker-specific sync and readiness metrics. |
| `status` | Low-cardinality worker-side operation result. |
| `rank` | Allowed only for bounded TopK diagnostic metrics. |
| `collection_id` | Allowed only for bounded TopK diagnostic metrics. |
| `replica_id` | Allowed only for bounded TopK diagnostic metrics. |
| `vchannel` | Allowed only for bounded TopK diagnostic metrics. |
| `query_view_version` | Allowed only for bounded TopK diagnostic metrics. |
| `data_version` | Allowed only for bounded TopK diagnostic metrics. |

Disallowed default labels:

| Value | Reason |
|---|---|
| QueryView version | High cardinality; use logs. |
| DataVersion | High cardinality; use logs. |
| Segment ID | High cardinality; use logs. |
| Raw error string | Unbounded cardinality; classify into `reason`. |
| Collection ID | High cardinality for large clusters; use logs, diagnostic APIs, or bounded TopK metrics. |
| Replica ID | Multiplies collection cardinality; use logs, diagnostic APIs, or bounded TopK metrics. |
| VChannel name | High cardinality for default dashboards; use only in targeted debug metrics. |

## 6. First-Iteration Metrics

### 6.1 `milvus_qv_view_states`

Type: Gauge

Description: Current number of active QueryViews by state.

Labels:

```text
component, state
```

Derived from:

- `CoordViewCreatedEvent`
- `CoordViewReportAppliedEvent`
- `CoordViewPreemptedEvent`
- `CoordViewAdvancedFromUnrecoverableEvent`
- `CoordViewReleaseRequestedEvent`
- `CoordViewHandoffToNewUpEvent`
- `CoordViewQueryNodeLostAppliedEvent`
- `QueryNodeSegmentsReadyEvent`
- `QueryNodeSegmentUnrecoverableEvent`
- `QueryNodeReleaseDoneEvent`
- `StreamingNodeResourceReadyEvent`
- `StreamingNodeRecoveringDoneEvent`
- `StreamingNodeReleaseDoneEvent`

State handling:

- Add a view entry when a component creates or observes a tracked view.
- Move the view entry when a component-visible transition event is observed.
- Keep state cache entries keyed by `(component, QueryViewKey)` so Coord,
  QueryNode, and StreamingNode observations do not overwrite each other.
- Delete the Coord view entry after Coord observes terminal Dropped cleanup. Existing
  `CoordViewReportAppliedEvent` and `CoordPersistViewEvent` can expose the
  Dropped transition and persistence path; if implementation cannot uniquely
  identify removal from those events, add a dedicated Coord view cleanup event
  instead of guessing from logs.

### 6.2 `milvus_qv_shard_load_states`

Type: Gauge

Description: Number of Coord-visible desired shards by shard load lifecycle
state.

Labels:

```text
state
```

State set:

| State | Meaning |
|---|---|
| `loading` | The shard is desired and has active non-release QueryViews, but has not reached Up in the current load lifecycle. |
| `loaded` | At least one active Coord view for the shard is Up. |
| `recovering` | The shard reached Up before, has no current Up view, and still has active non-release QueryViews. |

Implementation note:

The metric is maintained only by the Coord observer because worker nodes cannot
see the full shard-level Up state. The observer maintains a per-shard Coord
summary on state mutation paths and does not scan all views during scrape.

Release/unload is not represented as a load state. QueryViews in `Down`,
`Dropping`, or `Dropped` are excluded from this shard load lifecycle metric. If
all active non-release views for a shard leave the lifecycle, the shard metric
row is removed. A later LoadCollection starts a new lifecycle and is classified
as `loading`.

`recovering` is inferred from observed shard history: if the shard has reached
`loaded` in the current lifecycle and later has no Up view while non-release
views still exist, it is counted as `recovering`. This covers NodeLost-driven
recovery without requiring metrics code to derive labels from event names.

### 6.3 `milvus_qv_view_state_max_age_seconds`

Type: Gauge

Description: Per-component TopK active QueryViews by state age in seconds. The
metric is a bounded diagnostic metric, not a full per-view export. The default
TopK is 5 rows for each component.

Labels:

```text
component, state, rank, collection_id, replica_id, vchannel, query_view_version, data_version
```

Derived from:

- State entry time recorded on `CoordViewCreatedEvent` and state transition
  events.
- High-cardinality identity fields carried by the event, not re-derived by the
  metric observer.

Export behavior:

The observer stores the state entry time and maintains an oldest-first heap for
each component as events arrive. Collect-time export reads only each
component's TopK valid heap candidates, computes `now - entered_at` for output,
and assigns `rank` within that component. It does not scan or sort the full
QueryView state table on scrape.

Heap candidates use lazy deletion, but compaction does not depend on Prometheus
scrapes. The observer tracks the valid max-age candidate count for each
component. State updates and deletions trigger a low-frequency heap rebuild when
stale candidates grow much larger than that component's valid candidate count;
if no valid candidate remains, the component heap is removed. Collect-time export
still skips stale heap roots defensively before returning TopK rows.

`Up` state is intentionally skipped. A long-lived Up view is the healthy steady
state, so exporting its age adds churn without helping stuck-view diagnosis.

Operational use:

This is the primary diagnostic metric for stuck Preparing, Ready,
Unrecoverable, Dropping, or Down states. A stuck Preparing view can block new
DataVersion views and delay StreamingNode growing-resource release. Alerts
should normally aggregate by metric value and then use the TopK labels to jump
directly to the affected collection, replica, shard, and QueryView version.
Per-component TopK prevents one noisy component from hiding stuck views in
another component.

### 6.4 `milvus_qv_view_transition_total`

Type: Counter

Description: Count of QueryView state transitions by owning component.

Labels:

```text
component, from_state, to_state, trigger
```

Trigger values are provided by the corresponding event's `TriggerInfo()`:

| Event | Trigger |
|---|---|
| `CoordViewReportAppliedEvent` with Ready report | `reportReady` |
| `CoordViewReportAppliedEvent` with Unrecoverable report | `reportUnrecoverable` |
| `CoordViewPreemptedEvent` | `preempt` |
| `CoordViewHandoffToNewUpEvent` | `handoff` |
| `CoordViewReleaseRequestedEvent` | `release` |
| `CoordViewAdvancedFromUnrecoverableEvent` | `advanceUnrecoverable` |
| `CoordViewQueryNodeLostAppliedEvent` | `queryNodeLost` |
| `QueryNodeSegmentsReadyEvent` | `queryNodeSegmentsReady` |
| `QueryNodeSegmentUnrecoverableEvent` | `reportUnrecoverable` |
| `QueryNodeReleaseDoneEvent` | `queryNodeReleaseDone` |
| `StreamingNodeResourceReadyEvent` | `streamingResourceReady` |
| `StreamingNodeRecoveringDoneEvent` | `streamingRecoveringDone` |
| `StreamingNodeReleaseDoneEvent` | `streamingReleaseDone` |

### 6.5 `milvus_qv_unrecoverable_total`

Type: Counter

Description: Count of QueryViews that enter Unrecoverable state.

Labels:

```text
component, reason
```

Initial reason set:

| Component | Reason |
|---|---|
| `queryNode` | `segment_prepare_failed` |
| `streamingNode` | `resource_prepare_failed` |
| `coord` | `preempt` |
| `coord` | `queryNodeLost` |
| `coord` | `unknown` |

`MetricsObserver` should classify only reasons available from the event type
and stable fields. Raw error strings must not become label values.

### 6.6 `milvus_qv_view_ready_percent_bucket`

Type: Gauge

Description: Current number of Coord-visible non-Up QueryViews by
worker-reported resource readiness percent cumulative bucket.

Labels:

```text
component, state, le
```

Derived from:

- `CoordViewReportAppliedEvent.ResourceReadyPercent`

Bucket values:

```text
0, 25, 50, 75, 90, 99, 100, +Inf
```

The observer maintains this metric as current state, not as an event histogram.
Buckets are cumulative and use the Prometheus `le` label so PromQL helpers such
as `histogram_quantile` can be used on the current distribution. Coord view
creation enters the `0` bucket because a view without any report has no known
resource readiness progress. A Coord report moves the view from its old
`(state, le)` cumulative rows to the reported bucket's cumulative rows.
Non-report Coord state transitions preserve the last known `le` value while the
view remains non-Up. `Up` and `Dropped` states are removed from this metric.

For QueryNode reports, the percent represents segment preparation progress. For
StreamingNode reports, it is currently state-derived: resource-ready states
report 100 and other states report 0. The metric is exported from the Coord
observer because Coord owns the merged worker report view.

### 6.7 `milvus_qv_sync_pending`

Type: Gauge

Description: Number of QueryView sync entries awaiting worker response.

Labels:

```text
node_role, node_id, state
```

Status:

This metric requires additional syncer events for exact accounting.
`CoordSyncViewAcceptedEvent` identifies when ReliableSyncer accepts an entry,
but does not identify when that entry is matched and removed.

Required additional events:

```go
type CoordSyncViewPendingAddedEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    Node  qviews.WorkNode
    State qviews.QueryViewState
}

type CoordSyncViewPendingDoneEvent struct {
    baseEvent
    View  qviews.QueryViewKey
    Node  qviews.WorkNode
    State qviews.QueryViewState
}
```

The first metric implementation should leave this metric out unless these
events are added in the same change.

## 7. Worker-Side Metrics

Worker-side metrics are useful, but they are lower priority than Coord-side
lifecycle metrics because the first diagnostic path starts at QueryCoord.

### 7.1 `milvus_querynode_qv_segment_prepare_total`

Type: Counter

Description: QueryNode segment preparation result count for QueryViews.

Labels:

```text
node_id, status, reason
```

Status values:

```text
started, ready, failed, released
```

Reason values:

```text
none, metadata_failed, segment_load_failed, transform_register_failed,
transform_catchup_failed, release_race, unknown
```

Derived from:

- `QueryNodeAcquireSegmentsEvent`
- `QueryNodeSegmentsReadyEvent`
- `QueryNodeSegmentFailureEvent`
- `QueryNodeReleaseSegmentsEvent`

### 7.2 `milvus_streamingnode_qv_resource_prepare_total`

Type: Counter

Description: StreamingNode QueryView resource preparation result count.

Labels:

```text
node_id, status, reason
```

Status values:

```text
started, ready, recovered, released, failed
```

Reason values:

```text
none, runtime_failed, data_version_expired, unknown
```

Derived from:

- `StreamingNodeAcquireResourceEvent`
- `StreamingNodeRecoverAcquireResourceEvent`
- `StreamingNodeResourceReadyEvent`
- `StreamingNodeRecoveringDoneEvent`
- `StreamingNodeReleaseResourceEvent`

## 8. MetricsObserver State Model

`MetricsObserver` keeps state only for gauges that cannot be updated from a
single event.

```go
type MetricsObserver struct {
    mu sync.Mutex

    views  map[qviews.QueryViewKey]*viewMetricState
    shards map[qviews.ShardID]*shardMetricState
}

type viewMetricState struct {
    state        qviews.QueryViewState
    preparingAt time.Time
    lastReason   string
}

type shardMetricState struct {
    upView        qviews.QueryViewKey
    activeByState map[qviews.QueryViewState]int
    noUpReason    string
}
```

The state cache is not a source of truth for QueryView behavior. It is a best
effort projection of owner-emitted events for metrics. Losing this cache on
process restart is acceptable because gauges rebuild as new events arrive and
because persistent QueryView recovery should re-emit Coord-visible state.

## 9. Registration

QueryView metrics should be registered with the existing Prometheus metric
registry under the shared `qv` subsystem:

- QueryView lifecycle and sync metrics use `Subsystem: "qv"`.
- QueryNode segment preparation metrics use `Subsystem: "qv"`.
- StreamingNode resource preparation metrics use `Subsystem: "qv"`.

The default `observe` registry may include only `LogObserver` at package init
time. Component initialization should register a `MetricsObserver` after the
metrics package is initialized, avoiding package import cycles between
`internal/views/qviews/observe` and `pkg/metrics`.

## 10. Cleanup

Metric cleanup must follow collection release and view terminal cleanup.

Required cleanup behavior:

- Delete internal QueryCoord view and shard state when a collection is fully
  released or a view reaches terminal cleanup.
- Delete QueryNode worker labels when collection-local QueryView segment state
  is released.
- Delete StreamingNode worker labels when local QueryView resources are
  released.

Counters are not deleted.

## 11. Alerting Guidance

Recommended first alerts:

| Alert | Expression intent |
|---|---|
| Stuck non-Up view | `max(milvus_qv_view_state_max_age_seconds) > threshold` |
| Shard not loaded | `milvus_qv_shard_load_states{state!="loaded"} > 0` for a sustained window |
| Unrecoverable spike | Rate of `milvus_qv_unrecoverable_total` exceeds baseline |

Thresholds should be derived from expected segment load time and QueryView
preparation SLA. The metric design intentionally exposes state and bounded age
diagnostics so alerts can be tuned without changing code.

## 12. Implementation Order

1. Add QueryCoord event-derived counters:
   `view_transition_total` and `unrecoverable_total`.
2. Add QueryCoord state cache gauges:
   `view_states`, `view_state_max_age_seconds`, and
   `view_ready_percent_bucket`.
3. Add `shard_load_states` with states derivable from current Coord view
   history.
4. Add worker-side counters for QueryNode segment preparation and
   StreamingNode resource preparation.
5. Add sync pending events and then implement `sync_pending`.

This order keeps the first change useful without requiring new event contracts.

## 13. Tests

Unit tests should cover:

- Each event type updates the expected counter labels.
- State transition events move `view_state` gauges without double counting.
- Preparing age starts on Preparing creation and stops when the view leaves the
  blocking state.
- `shard_load_states` changes when the active shard load lifecycle state changes.
- Raw error text is never used as a metric label.
- `MetricsObserver.Observe` does not panic on unknown or partially populated
  events.

Tests should use the existing Prometheus test registry pattern from
`pkg/metrics` and avoid global metric pollution where possible.
