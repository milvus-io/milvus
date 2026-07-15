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
5. release growing segment state no longer needed by active QueryViews.

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
  sealedAtDataVersion
  local segment resource handle
  closed state
```

`GrowingSegment` is the single-segment resource wrapper. It hides the concrete
storage or segcore implementation from the runtime.

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

## 5. Actual Behavior

### 5.1 Preparation

```text
QueryRuntime.Initialize
  -> GrowingRuntime.Prepare
  -> load SegmentSnapshot.Segments
  -> create GrowingSegment for each visible growing or flushed-as-growing segment
  -> load persisted segment data
  -> replay snapshot inserts
  -> drain DeleteReplay
  -> mark GrowingRuntime Ready
```

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
strictly older than the oldest active QueryView's required DataVersion.

If no QueryView references remain, the module closes the whole `QueryRuntime`.

### 5.5 Close

`Close` releases all segment handles and stops module-local workers. It is
called only by `QueryRuntime.Close`.
