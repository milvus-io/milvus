# StreamingNode IDF Oracle Runtime Design

> VChannel-level BM25 / IDF resource module for StreamingNode QueryView.
> This document defines the resource lifecycle and preparation flow of
> `IDFOracleRuntime`. Query execution, scoring behavior, and query plan format
> are out of scope.

## 1. Purpose

`IDFOracleRuntime` is the vchannel-level `QueryRuntimeModule` that prepares and
maintains BM25 statistics used by the StreamingNode IDF oracle.

Unlike growing segment data, IDF oracle state is not retained per QueryView
DataVersion. A loaded vchannel owns one `IDFOracleRuntime` inside its singleton
`QueryRuntime`.

The purpose of `IDFOracleRuntime` is to:

1. initialize BM25 statistics from the `VChannelWALView` base DataVersion;
2. fetch sealed BM25 resources from QueryCoord for the initialized DataVersion;
3. continuously generate growing-segment BM25 statistics from live WAL resource
   events forwarded by `QueryRuntime`;
4. record each flushed growing segment's sealed DataVersion;
5. asynchronously advance the current oracle when QueryView reference watermarks
   move forward;
6. atomically apply BM25 statistics diffs so readers never observe a partially
   advanced oracle;
7. clean obsolete internal BM25 statistics and sealed cache leases by itself.

`IDFOracleRuntime` is not a live observer. It does not maintain pending buffers
and does not expose catchup state. `QueryRuntime.Initialize` owns buffering,
catchup, and the transition to `Ready`.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `SNQueryRuntimeManager` | PChannel-local owner of QueryView/init references. It creates the vchannel `QueryRuntime`, waits for runtime initialization on `Acquire`, and advances the runtime by oldest active QueryView DataVersion. | It does not compute BM25 stats diffs and does not evict IDF internal segment stats. |
| `QueryRuntime` | VChannel-level singleton runtime. Implements `VChannelLiveObserver`, owns one live-event buffer and one consumer, calls `IDFOracleRuntime.Prepare`, forwards live events, and calls `IDFOracleRuntime.Advance`. | It does not compute BM25 stats or fetch sealed resources directly. |
| `IDFOracleRuntime` | QueryRuntime module that owns the vchannel singleton oracle, growing BM25 stats store, sealed contribution leases, current DataVersion, and advance worker. | It does not implement `VChannelLiveObserver`, expose external truncation, or own QueryView references. |
| `VChannelWALView` | Provides the initial schema, settings, segment snapshot, historical insert input, and no-gap live resource event stream. | Its capture and no-gap contract are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `SealedBM25ResourceProvider` | Calls DataCoord to fetch the complete sealed BM25 resource set for a target DataVersion. | It does not cache local files or merge oracle stats. |
| `SealedBM25SegmentCache` | Downloads, parses, reuses, and leases sealed BM25 stats. | It does not decide DataVersion advancement or contribution membership. |
| `GrowingBM25StatsStore` | Maintains local BM25 stats for growing segments generated from snapshot and live WAL events, plus flushed/sealed metadata. | It does not fetch sealed resources from QueryCoord. |
| `IDFAdvanceWorker` | Serializes asynchronous oracle advancement requests and coalesces them to the newest allowed requested DataVersion. | It is internal to `IDFOracleRuntime` and is not the resource build scheduler. |

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
QueryRuntime.Initialize
        |
        | module.Prepare
        v
IDFOracleRuntime.Prepare
        |
        | sealed resources
        v
SealedBM25ResourceProvider
        |
        v
SealedBM25SegmentCache

IDFOracleRuntime.Prepare
        |
        | growing snapshot stats
        v
GrowingBM25StatsStore
```

Live events:

```text
RecoveryStorage
        |
        | ObserveEvent
        v
QueryRuntime
        |
        | IDFOracleRuntime.ApplyLiveEvent
        v
GrowingBM25StatsStore
```

DataVersion advancement:

```text
SNQueryRuntimeManager
        |
        | QueryRuntime.Advance(oldestDataVersion)
        v
QueryRuntime
        |
        | IDFOracleRuntime.Advance(oldestDataVersion)
        v
IDFAdvanceWorker
```

### 3.2 Runtime State

```text
IDFOracleRuntime
  collectionID
  vchannel
  settings
  currentDataVersion
  currentStats
  currentSealedContributions map[segmentID]SealedBM25Contribution
  currentGrowingContributions map[segmentID]GrowingBM25Contribution
  growingStore GrowingBM25StatsStore
  sealedCache SealedBM25SegmentCache
  provider SealedBM25ResourceProvider
  advanceWorker
  close/cancel
```

`currentDataVersion` describes the sealed/growing contribution boundary of the
current oracle. It is advanced by atomic diff commit. Live growing stats may
continue to update while the sealed baseline stays at the same DataVersion.

### 3.3 Contribution Model

For a target DataVersion `D`, the oracle contribution set is:

```text
ContributionSet(D):
  sealed contributions:
    complete sealed BM25 resource set returned by QueryCoord for D

  growing contributions:
    local growing BM25 stats whose segment is not in the sealed set for D
    and whose sealedAtDataVersion is absent or > D
```

The sealed set always comes from QueryCoord. StreamingNode must not infer sealed
membership for a target DataVersion from local segment metadata alone.

The local `sealedAtDataVersion` is still recorded because it determines when a
growing segment can stop contributing to the oracle and when its local growing
BM25 stats can be removed.

### 3.4 Invariants

1. `IDFOracleRuntime` implements `QueryRuntimeModule`.
2. There is only one `IDFOracleRuntime` per loaded vchannel.
3. There is no `DataVersion -> IDFOracle` map.
4. `IDFOracleRuntime` does not implement `VChannelLiveObserver`.
5. `IDFOracleRuntime` does not expose a module-level catchup handle.
6. Initial construction is triggered by `QueryRuntime.Initialize`, not by
   QueryView `Acquire`.
7. The initialized oracle DataVersion is the `VChannelWALView` base
   DataVersion.
8. Initial sealed BM25 resources are fetched from QueryCoord.
9. Initial growing BM25 stats are generated from the WALView segment snapshot.
10. Live growing BM25 stats are generated from events forwarded by
    `QueryRuntime` in WAL order.
11. The first QueryView `Up` report waits for `QueryRuntime.Initialize` to
    complete successfully.
12. `Advance(oldestDataVersion)` may enqueue asynchronous IDF advancement, but
    QueryView activation does not wait for the advancement to finish.
13. IDF advancement is vchannel-local, serial, asynchronous, and monotonic.
14. BM25 stats diff is computed outside the commit path.
15. The current oracle is changed only by one atomic diff commit.
16. The runtime owns cleanup of obsolete growing stats, sealed leases, and
    abandoned advance-task resources.
17. A valid live event that cannot be applied is a critical StreamingNode
    corruption, not a recoverable QueryView resource condition.

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

`IDFOracleRuntime` implements this interface.

### 4.2 IDFOracleRuntime

```go
type IDFOracleRuntime interface {
    QueryRuntimeModule

    // Query-facing BM25 oracle accessors are module-specific and are not part
    // of the lifecycle interface.
}
```

This is an IDF-module concept. The shared `viewresource` package depends only
on `QueryRuntimeModule` and `QueryRuntimeModuleBuilder`; it does not define or
reference `IDFOracleRuntime`.

`Prepare` builds the initial oracle for the provided WALView base DataVersion.
It fetches sealed resources and initializes growing BM25 stats from the WALView
segment snapshot. `IDFOracleRuntime` keeps the derived oracle state, not the
WALView object passed into `Prepare`.

`ApplyLiveEvent` updates growing BM25 stats and sealed-at metadata from live
events forwarded by `QueryRuntime`.

`Advance(oldestDataVersion)` requests an asynchronous handoff to
`oldestDataVersion` if it is newer than the current oracle DataVersion. If the
target is not newer, it is ignored.

There is intentionally no external `Truncate` method. Obsolete IDF internal
state is cleaned by the runtime after diff commit, segment sealed observation,
or advance-task cancellation.

### 4.3 SealedBM25ResourceProvider

```go
type SealedBM25ResourceProvider interface {
    GetSealedBM25Resources(
        ctx context.Context,
        collectionID int64,
        vchannel string,
        dataVersion qviews.DataVersion,
        settings *viewpb.QueryViewSettings,
    ) ([]*datapb.StreamingNodeBM25Resource, error)
}
```

The returned resources are the full sealed BM25 resource set for the requested
DataVersion. They are not a diff from StreamingNode's current local cache.

### 4.4 SealedBM25SegmentCache

```go
type SealedBM25SegmentCache interface {
    Acquire(
        ctx context.Context,
        resource *datapb.StreamingNodeBM25Resource,
    ) (SealedBM25Lease, error)
}

type SealedBM25Lease interface {
    SegmentID() int64
    PartitionID() int64
    Stats() BM25Stats
    Release()
}
```

`Acquire` may download remote BM25 binlogs, reuse existing local files, and parse
stats. The returned lease keeps local sealed BM25 resources alive while the
current oracle or an in-flight diff references them.

`Stats` returns BM25 stats for diff calculation. Callers must treat returned
stats as read-only or clone them before mutation.

### 4.5 GrowingBM25StatsStore

```go
type GrowingBM25StatsStore interface {
    BuildFromSnapshot(ctx context.Context, view walview.VChannelWALView) error
    ApplyLiveEvent(ctx context.Context, event walview.VChannelResourceEvent)

    ContributionFor(dataVersion qviews.DataVersion, sealedSet SegmentSet) GrowingContributionSet
    CleanupCommitted(currentDataVersion qviews.DataVersion, sealedSet SegmentSet)
    Close()
}
```

The store records BM25 stats for local growing segments and records
`sealedAtDataVersion` for flushed growing segments. It is internal to
`IDFOracleRuntime`.

### 4.6 IDFAdvanceWorker

```go
type IDFAdvanceWorker interface {
    Request(target qviews.DataVersion)
    Close()
}
```

The worker serializes advancement. Multiple requests may be coalesced as long as
the worker never commits an oracle newer than the latest allowed
`oldestDataVersion` observed from the resource manager.

## 5. Actual Behavior

### 5.1 Initial Preparation

```text
QueryRuntime.Initialize
  -> IDFOracleRuntime.Prepare
  -> fetch sealed BM25 resources for base DataVersion
  -> acquire sealed cache leases
  -> build growing BM25 stats from WALView segment snapshot
  -> assemble current oracle
```

The initial oracle DataVersion is the `VChannelWALView` base DataVersion.

### 5.2 Live Event Apply

```text
QueryRuntime.applyLiveEvent(event)
  -> IDFOracleRuntime.ApplyLiveEvent(event)
  -> GrowingBM25StatsStore.ApplyLiveEvent(event)
```

Live events update growing BM25 stats and record flushed segment
`sealedAtDataVersion`. `QueryRuntime` owns ordering and ensures the same event
sequence is also applied to the other resource modules.

### 5.3 First QueryView Up

The first QueryView `Up` report waits for `QueryRuntime.Initialize` to complete
successfully, not for an IDF-specific catchup handle.

`QueryRuntime.Initialize` returns successfully after:

1. `GrowingRuntime.Prepare` returns;
2. `IDFOracleRuntime.Prepare` returns;
3. `QueryRuntime` starts its singleton consumer;
4. `QueryRuntime` atomically takes the current live-event buffer batch;
5. every event in the initial batch has been applied to both modules.

### 5.4 Oracle Advancement

```text
QueryView references move forward
  -> SNQueryRuntimeManager computes oldestDataVersion
  -> QueryRuntime.Advance(oldestDataVersion)
  -> IDFOracleRuntime.Advance(oldestDataVersion)
  -> IDFAdvanceWorker.Request(oldestDataVersion)
```

If the target is newer than the current oracle DataVersion, the worker computes
and commits a BM25 diff asynchronously. QueryView activation does not wait for
this background handoff.

The diff model:

```text
negative contributions:
  sealed segments that leave the target contribution set
  growing segments with sealedAtDataVersion <= target DataVersion

positive contributions:
  sealed BM25 resources returned by QueryCoord for the target DataVersion
  growing segment stats still not covered by the target sealed set
```

The worker computes all stats and leases outside the commit path. Commit is one
atomic update to the current oracle.

### 5.5 Cleanup

After an advance commit, `IDFOracleRuntime` releases sealed leases and removes
growing BM25 stats that can no longer contribute to any future oracle state.

Cleanup is internal. The resource manager never calls an IDF-specific truncate
operation.

### 5.6 Close

`Close` stops the advance worker, releases current sealed leases, releases
in-flight advance resources, closes the growing stats store, and makes the
oracle unavailable. It is called only by `QueryRuntime.Close`.
