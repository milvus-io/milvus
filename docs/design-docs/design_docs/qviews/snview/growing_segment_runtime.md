# StreamingNode Growing Segment Runtime Design

> StreamingNode-side growing segment resource module for QueryView.
> This document defines the `GrowingRuntime` resource model. Query execution,
> query plan format, and QueryNode sealed segment lifecycle are out of scope.

## 1. Purpose

`GrowingRuntime` is the vchannel-level `QueryRuntimeModule` that owns
growing-side segment resources prepared from `VChannelWALView`.

It is created as part of a vchannel `QueryRuntime`:

```text
VChannelRecoveryModule
  -> QueryRuntime(Preparing)
       -> GrowingRuntime
       -> other QueryRuntimeModule implementations
```

The purpose of `GrowingRuntime` is to:

1. own all growing-side segment resources for one vchannel;
2. load historical visible segment data from `VChannelWALView.SegmentSnapshot`;
3. apply historical deletes from `VChannelWALView.DeleteReplay`;
4. apply live resource events forwarded by `QueryRuntime`;
5. retain flushed segment markers needed for recovery replay idempotency;
6. release growing segment state no longer needed by active QueryViews.

`GrowingRuntime` is not a live observer. It does not maintain pending buffers
and does not expose catchup state. `QueryRuntime.Initialize` owns buffering,
catchup, and the transition to `Ready`.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `VChannelRecoveryModule` | VChannel-local owner of QueryView references. Creates the vchannel `QueryRuntime` and submits its initialization task through the PChannel manager scheduler. | It does not build individual growing segments directly. |
| `QueryRuntime` | VChannel-level singleton runtime. Owns one live-event buffer and one consumer, calls `GrowingRuntime.Prepare`, forwards live events, and calls `GrowingRuntime.Advance`. | It does not own single-segment resource handles directly. |
| `GrowingRuntime` | QueryRuntime module that owns the vchannel segment map and segment-level dispatch. | It does not decide resource references or call WAL modules directly. |
| `GrowingSegment` | Owns one segment's local resource handle and applies segment-scoped persisted data, inserts, deletes, and sealed metadata. | It does not own vchannel-level message dispatch or DataVersion watermarks. |
| `VChannelWALView` | Provides no-gap WAL input for the selected base DataVersion. | Its contract is defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `SegmentModule` | Owns segment metadata, visible snapshot construction, and segment metadata GC. | It is consumed only through `VChannelWALView`; runtime components do not call it directly. |
| `TransformLogModule` | Owns transform log storage and delete replay scanner construction. | It is consumed only through `VChannelWALView.DeleteReplay`. |

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
QueryRuntime.Initialize
        |
        | module.Prepare
        v
GrowingRuntime.Prepare
        |
        | load historical inputs
        v
GrowingRuntime
        |
        | owns many
        v
GrowingSegment
```

Live events:

```text
RecoveryStorage
        |
        | ObserveEvent
        v
QueryRuntime
        |
        | GrowingRuntime.ApplyLiveEvent
        v
GrowingSegment
```

DataVersion advancement:

```text
VChannelRecoveryModule
        |
        | QueryRuntime.Advance(oldestDataVersion)
        v
QueryRuntime
        |
        | GrowingRuntime.Advance(oldestDataVersion)
        v
GrowingRuntime.Truncate
```

### 3.2 Runtime State

```text
GrowingRuntime
  collectionID
  vchannel
  baseDataVersion
  schema
  closed
  segments map[segmentID]GrowingSegment
  appliedGrowingTimeTick
  appliedTransformTimeTick
  oldestRetainedDataVersion
  close/cancel
```

`GrowingRuntime` is a vchannel-level data collection. There is no separate
`GrowingSegmentSet` abstraction; the runtime itself owns segment membership and
vchannel-level dispatch.

### 3.3 Segment State

```text
GrowingSegment
  segmentID
  partitionID
  flushed
  flushTimeTick
  sealedAtDataVersion
  local segment resource handle
  closed state
```

`GrowingSegment` is the single-segment resource wrapper. It hides the concrete
storage or segcore implementation from the runtime.

A `GrowingSegment` may be a non-queryable flushed segment marker. The marker has
segment ID, partition ID, flush timetick, and sealed DataVersion, but no local
segcore segment handle. It exists only to make WAL replay idempotent for
flushed segments that are already covered by QueryNode for the current QueryView
DataVersion.

### 3.4 Invariants

1. `GrowingRuntime` implements `QueryRuntimeModule`.
2. `GrowingRuntime` does not own the vchannel live-event buffer.
3. `GrowingRuntime` does not own pending live-event buffering.
4. `GrowingRuntime` does not expose a catchup handle.
5. Runtime preparation never reads `SegmentModule` or `TransformLogModule`
   directly.
6. `VChannelWALView` owns the no-gap input guarantee.
7. Snapshot segment membership during preparation comes only from
   `VChannelWALView.SegmentSnapshot.Segments`.
8. Persisted segment data is loaded before snapshot insert messages are replayed
   for the same segment.
9. `DeleteReplay` is drained and applied before `Prepare` returns.
10. `SealedAtDataVersion` updates after the WALView capture point are delivered
    by resource events from `QueryRuntime`.
11. Live apply is not a recoverable resource-level error path. If a ready
    runtime cannot apply valid live input, the input or runtime state is
    corrupted and the StreamingNode must fail critically.
12. `Advance(oldestDataVersion)` is the only external GC signal from
    QueryView references.

## 4. Interface Description

### 4.1 QueryRuntimeModule

```go
type QueryRuntimeModule interface {
    Prepare(ctx context.Context, view walview.VChannelWALView) error
    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)
    Advance(oldestDataVersion qviews.DataVersion)
    Close()
}
```

`GrowingRuntime` implements this interface.

### 4.2 GrowingRuntime

```go
type GrowingRuntime interface {
    QueryRuntimeModule

    // Query-facing growing segment accessors are module-specific and are not
    // part of the lifecycle interface.
}
```

The query-facing accessors include two categories:

1. Blocking execution accessors used by Phase 2 task providers. These wait until
   the runtime is visible at the requested MVCC, then acquire concrete growing
   segment handles for execution.
2. Non-blocking planning probes used by Phase 1. These may inspect the current
   runtime only if it is already ready and already visible at the requested
   MVCC. They must not pin segment handles or wait for catch-up.

The Phase 1 probe should have "may have candidates" semantics:

```go
type GrowingRuntime interface {
    // Returns false only when the runtime is already visible at the requested
    // MVCC and the filtered growing candidate set is definitely empty.
    // Returns true for any unknown, unsupported, or not-yet-visible state.
    MayHaveVisibleGrowingSegments(
        growingTimetick uint64,
        transformingTimetick uint64,
        partitionIDs []int64,
    ) bool
}
```

`partitionIDs` is the deterministic request scope produced by Proxy and carried
through `GetQueryPlanRequest`. An empty list means all partitions.

`Prepare` loads the historical resources from the provided `VChannelWALView`.
`GrowingRuntime` does not retain the WALView after preparation.

`ApplyLiveEvent` is called only by `QueryRuntime`, in WAL order. It dispatches
vchannel events to the affected `GrowingSegment` instances.

`Advance(oldestDataVersion)` releases growing segment state that cannot be
needed by any active QueryView.

### 4.3 GrowingSegment

```go
type GrowingSegment interface {
    ID() int64
    PartitionID() int64
    SealedAtDataVersion() *viewpb.DataVersion

    LoadPersisted(ctx context.Context, storage *streamingpb.L1SegmentPersistedStorage) error
    ApplyInsert(ctx context.Context, msg message.ImmutableMessage)
    ApplyDelete(ctx context.Context, entry *streamingpb.TransformLogEntry)
    MarkSealed(sealedAt qviews.DataVersion)

    Close()
}
```

Live resource events are not applied directly to a single segment. They first
enter `QueryRuntime`, then `GrowingRuntime`, which performs vchannel-level
dispatch and calls segment-scoped methods.

`MarkSealed` records the `DataVersion` assigned after the segment's flush commit
is acknowledged. WAL `Flush` closes the segment for writes, but it does not
carry this `DataVersion`.

Segment-scoped replay must be idempotent. For a flushed segment, any
`CreateSegment` or `Insert` at or before the segment's `flushTimeTick` is an
old WAL entry and must be ignored instead of recreating a queryable growing
segment. A `CreateSegment` or `Insert` after the flush timetick is invalid for
that segment and is treated as corrupted runtime input.

## 5. Actual Behavior

### 5.1 Preparation

```text
QueryRuntime.Initialize
  -> GrowingRuntime.Prepare
  -> load SegmentSnapshot.Segments
  -> create GrowingSegment for each visible growing or flushed-as-growing segment
  -> load SegmentSnapshot.FlushedSegments as non-queryable flushed segment markers
  -> load persisted segment data
  -> replay snapshot inserts
  -> drain DeleteReplay
  -> mark GrowingRuntime Ready
```

`SegmentSnapshot.Segments` contains query-visible segment resources. It may
include flushed segments whose `sealedAtDataVersion` is newer than the QueryView
DataVersion and therefore still need StreamingNode service.

`SegmentSnapshot.FlushedSegments` contains flushed segments whose
`sealedAtDataVersion` is already covered by the QueryView DataVersion. These
segments are not returned to query task acquisition and do not participate in
the Phase 1 "may have growing segment" probe. They remain in the runtime only
as replay guards until their old segment-scoped WAL range is known to be
consumed.

`Prepare` is synchronous from the module perspective. Build concurrency is
controlled by the `QueryRuntime` initialization scheduler, not by
`GrowingRuntime`.

### 5.2 Live Event Apply

```text
QueryRuntime.applyLiveEvent(event)
  -> GrowingRuntime.ApplyLiveEvent(event)
  -> dispatch by event type and segment ID
  -> GrowingSegment.ApplyInsert / ApplyDelete / MarkSealed / Close
```

`QueryRuntime` owns event ordering. `GrowingRuntime` assumes calls come from the
single `QueryRuntime` consumer and are already serialized in WAL order.

Recovery baseline events are defined by
[RecoveryBarrier](../../../../agent_guides/streaming-system/message/message-semantic-recovery-barrier.md).

### 5.3 Segment Seal DataVersion

Segment sealing has two relevant moments:

1. WAL flush closes the growing segment for additional writes.
2. Segment metadata later obtains `SealedAtDataVersion`.

The second value is required for retention. It is delivered through live
resource events captured by `RecoveryStorage` and forwarded by `QueryRuntime`.
`GrowingRuntime` must not query `SegmentModule` directly to refresh it.

### 5.4 Truncation

```text
VChannelRecoveryModule computes oldest active QueryView DataVersion
  -> QueryRuntime.Advance(oldestDataVersion)
  -> GrowingRuntime.Advance(oldestDataVersion)
  -> GrowingRuntime.Truncate(oldestDataVersion)
```

`Truncate` closes and removes growing segment resources whose retained state is
covered by the oldest active QueryView's required DataVersion and whose
segment-scoped replay guard is no longer needed.

A flushed segment can be removed only when both conditions hold:

```text
appliedGrowingTimeTick > segment.flushTimeTick
oldestActiveQueryViewDataVersion >= segment.sealedAtDataVersion
```

The DataVersion condition means QueryNode can serve the sealed segment. The
timetick condition means `GrowingRuntime` has consumed beyond the segment's WAL
flush point, so replay cannot later encounter old `CreateSegment` or `Insert`
messages for that segment.

If no QueryView references remain, the module closes the whole `QueryRuntime`.

### 5.5 Query Planning Probe

`GrowingRuntime` may be queried during Phase 1 to avoid dispatching a
StreamingNode Phase 2 request that cannot contribute results.

The probe is only valid when the runtime is already ready and both applied
frontiers have reached the requested MVCC. It then scans the current growing
segment map under the runtime lock, ignores non-queryable flushed segment
markers, applies the same request partition filter as Phase 2 task acquisition,
and reports whether any candidate exists.

The probe must be conservative:

- It must not wait for MVCC catch-up.
- It must not pin or return physical segment handles.
- It must not perform expensive expression or PK pruning unless the required
  runtime metadata is already local and the pruning result is exact enough to
  preserve correctness.
- If the runtime is not visible or the filter cannot be applied, the caller must
  keep StreamingNode in the `QueryPlan`.

This makes Phase 1 pruning a latency optimization only. Phase 2 remains the
source of truth for MVCC waiting, handle acquisition, and execution.

### 5.6 Close

`Close` releases all segment handles and stops module-local workers. It is
called only by `QueryRuntime.Close`.
