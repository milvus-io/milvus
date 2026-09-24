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
2. fetch sealed BM25 resources for the active DataVersion when materializing;
3. continuously generate growing-segment BM25 statistics from live WAL resource
   events forwarded by `QueryRuntime`;
4. record each flushed growing segment's sealed DataVersion;
5. advance the current oracle before a newer QueryView reports Ready;
6. atomically apply BM25 statistics diffs so readers never observe a partially
   advanced oracle;
7. release obsolete sealed file references and growing statistics;
8. optionally defer initial sealed-resource loading and stats materialization
   until the first BM25 query.

`IDFOracleRuntime` is not a live observer. It does not maintain pending buffers
and does not expose catchup state. `QueryRuntime.Initialize` owns buffering,
catchup, and the transition to `Ready`.

## 2. Components And Business Boundaries

| Component | Role | Boundary |
|---|---|---|
| `VChannelRecoveryModule` | VChannel-local owner of QueryView references. It creates the vchannel `QueryRuntime`, waits for runtime initialization on `Acquire`, and advances the runtime by oldest active QueryView DataVersion. | It does not compute BM25 stats diffs and does not evict IDF internal segment stats. |
| `QueryRuntime` | VChannel-level singleton runtime. Owns one live-event buffer and one consumer, calls `IDFOracleRuntime.PrepareDataVersion` before QueryView readiness, and forwards live events. | It does not compute BM25 stats or fetch sealed resources directly. |
| `IDFOracleRuntime` | QueryRuntime module that owns one rolling aggregate, a growing BM25 stats store, and sealed cache references for the current and prepared versions. | It does not own the vchannel live-event buffer, expose external truncation, or own QueryView references. |
| `VChannelWALView` | Provides the initial schema, settings, segment snapshot, historical insert input, and no-gap live resource event stream. | Its capture and no-gap contract are defined in [StreamingNode VChannel WAL View Design](../../wal/streamingnode_vchannel_wal_view.md). |
| `SealedBM25ResourceProvider` | Calls DataCoord to fetch the complete sealed BM25 resource set for a target DataVersion. | It does not cache local files or merge oracle stats. |
| `SealedBM25SegmentCache` | Downloads, parses, reuses, and retains sealed BM25 files. | It does not decide DataVersion advancement or contribution membership. |
| `GrowingBM25StatsStore` | Maintains local BM25 stats for growing segments generated from snapshot and live WAL events, plus flushed/sealed metadata. | It does not fetch sealed resources from DataCoord. |

## 3. Component Relationships And Invariants

### 3.1 Relationship Model

```text
QueryRuntime.Initialize -> IDFOracleRuntime.Prepare (eager)
First BM25 query        -> IDFOracleRuntime.BuildIDF (lazy)
        |
        | sealed resources when materializing
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
QueryRuntime.PrepareDataVersion(target)
  -> IDFOracleRuntime.PrepareDataVersion(target)
  -> materialized: load and apply the target's stats before Ready
  -> unmaterialized lazy: record target without sealed I/O
```

### 3.2 Runtime State

```text
IDFOracleRuntime
  collectionID
  vchannel
  partitionIDs / loadInfoVersion
  currentDataVersion
  currentStats map[fieldID]BM25Stats
  currentSealed map[segmentID]SealedBM25File
  currentGrowing set[segmentID]
  prepared map[DataVersion]map[segmentID]SealedBM25File
  materializationCall
  growingStore GrowingBM25StatsStore
  sealedCache SealedBM25SegmentCache
  provider SealedBM25ResourceProvider
  close/cancel
```

`currentDataVersion` describes the sealed/growing contribution boundary of the
current oracle. It is advanced by atomic diff commit. Live growing stats may
continue to update while the sealed baseline stays at the same DataVersion.
`prepared` retains only sealed file references for later versions, not complete
per-version aggregates. `currentStats` is absent until first materialization
when lazy initial loading is enabled.

### 3.3 Contribution Model

For a target DataVersion `D`, the oracle contribution set is:

```text
ContributionSet(D):
  sealed contributions:
    complete sealed BM25 resource set returned by DataCoord for D

  growing contributions:
    local growing BM25 stats whose segment is not in the sealed set for D
    and whose sealedAtDataVersion is absent or > D
```

The sealed set always comes from DataCoord. StreamingNode must not infer sealed
membership for a target DataVersion from local segment metadata alone.

The local `sealedAtDataVersion` is still recorded because it determines when a
growing segment can stop contributing to the oracle and when its local growing
BM25 stats can be removed.

Initial `VChannelWALView` construction contains no flushed segment with an
absent `sealedAtDataVersion`; the owning VChannel module resolves those final
commits before runtime preparation. The `absent` case above is retained for a
live Flush observed after WAL view capture and before its final-commit event is
delivered. Such a segment continues contributing as growing until the exact
first DataView membership version arrives.

### 3.4 Invariants

1. `IDFOracleRuntime` implements `QueryRuntimeModule`.
2. There is only one `IDFOracleRuntime` per loaded vchannel.
3. There is no `DataVersion -> IDFOracle` map.
4. Prepared DataVersions hold sealed file references, not separate full BM25
   aggregates.
5. `IDFOracleRuntime` does not own the vchannel live-event buffer.
6. `IDFOracleRuntime` does not expose a module-level catchup handle.
7. Initial construction is triggered by `QueryRuntime.Initialize`, not by
   QueryView `Acquire`.
8. The initialized oracle DataVersion is the `VChannelWALView` base
   DataVersion.
9. Eager initial preparation fetches sealed BM25 resources and materializes
   stats. Lazy initial preparation defers both operations until a BM25 query.
10. Initial preparation does not use a VChannel-level maximum DataVersion fence;
   it consumes the same per-segment classification as `GrowingRuntime`.
11. Initial growing BM25 stats are generated from the WALView segment snapshot.
12. Live growing BM25 stats are generated from events forwarded by
    `QueryRuntime` in WAL order.
13. The first QueryView `Up` report waits for `QueryRuntime.Initialize` to
    complete successfully.
14. A newer QueryView reports Ready only after a materialized aggregate reaches
    its DataVersion. An unmaterialized lazy aggregate records the target without
    sealed I/O and materializes on the first BM25 query.
15. IDF advancement is vchannel-local, serial, and monotonic. QueryView resource
    preparation runs on the node-level `NodeScheduler`.
16. Sealed BM25 contribution diffs are computed before the commit; growing
    membership is evaluated while committing under the oracle write lock.
17. A materialized DataVersion handoff updates the sealed baseline and rolling
    aggregate under one write lock. Live growing events also update the
    aggregate under that lock.
18. The runtime owns cleanup of obsolete growing stats and sealed file
    references.
19. A valid live event that cannot be applied is a critical StreamingNode
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

The concrete `idf.Runtime` implements `QueryRuntimeModule` and the optional
`QueryRuntimeVersionedModule` interface:

```go
type QueryRuntimeVersionedModule interface {
    PrepareDataVersion(context.Context, qviews.DataVersion) error
    ReleaseDataVersion(qviews.DataVersion)
}
```

It owns an `oracleRuntime` that serves `BuildIDF`. The parent `wal/vchannel`
package depends on the generic runtime interfaces and does not reference the
IDF implementation directly.

`Prepare` builds the initial oracle for the provided WALView base DataVersion.
It initializes growing BM25 stats from the WALView segment snapshot. In eager
mode it also fetches sealed resources and assembles the rolling aggregate; in
lazy mode the first BM25 query performs that materialization. The runtime keeps
the derived oracle state, not the WALView object passed into `Prepare`.

Once the initial aggregate exists, `PrepareDataVersion` fetches a newer
DataVersion's sealed resources, parses changed stats, and commits the aggregate
before its QueryView is ready. Lazy runtimes that have not yet materialized
record the target DataVersion without sealed I/O. A prepared entry contains
file references only while advancement is in progress.
`ReleaseDataVersion` drops references for a version that is no longer needed.

`ApplyLiveEvent` updates growing BM25 stats and sealed-at metadata from live
events forwarded by `QueryRuntime`.

`Advance(oldestDataVersion)` is a no-op in the IDF module. The generic
`QueryRuntime` still forwards this watermark to the growing module for cleanup.

There is intentionally no external `Truncate` method. Obsolete IDF internal
state is cleaned by the runtime after diff commit or segment sealed observation.

### 4.3 SealedBM25ResourceProvider

The provider requests the complete resource set for a collection, vchannel,
DataVersion, loaded partition set, and load-info version. It acquires sealed
cache entries concurrently under a process-wide concurrency limit. When an
aggregate is supplied, completed loads merge into it as they arrive; version
prefetch supplies no aggregate and retains only local file references.

The returned resources are the full sealed BM25 resource set for the requested
DataVersion. They are not a diff from StreamingNode's current local cache.

### 4.4 SealedBM25SegmentCache

`segmentCache.acquire(ctx, chunkManager, resource, needParse)` returns a
reference-counted `sealedBm25Stats` file entry and, when requested, temporary
decoded stats. The cache key encodes the full sealed resource descriptor, so
different resources for the same segment have separate entries. Concurrent
acquisitions of one resource wait for a single download.

Downloads stream into local files. With `needParse`, the stream is also decoded
and merged into temporary stats; otherwise the bytes are copied without
decoding. Opening and interrupted remote reads use the configured storage read
retries. `sealedBm25Stats.FetchStats()` decodes the retained local files when a
later diff needs them. Its read lock prevents deletion during decoding.

The cache keeps local files while the current oracle, a prepared version, or an
in-flight load holds a reference. Releasing the final reference removes the
entry and its files. No decoded full-segment stats remain in a cache entry.

### 4.5 GrowingBM25StatsStore

The store records BM25 stats for local growing segments and records
`sealedAtDataVersion` for flushed growing segments. It is internal to
`IDFOracleRuntime`. During version advancement, `snapshotForDataVersion`
determines growing membership and clones stats only for segments whose
membership changes.

## 5. Actual Behavior

### 5.1 Initial Preparation

```text
QueryRuntime.Initialize
  -> IDFOracleRuntime.Prepare
  -> eager mode: fetch and merge sealed stats, then load initial growing stats
  -> lazy mode: load initial growing stats and defer sealed materialization
```

The initial oracle DataVersion is the `VChannelWALView` base DataVersion.
Eager mode is the default (`queryNode.idfOracle.lazyLoadSealedStats=false`).
When enabled, lazy initialization performs no sealed resource discovery or
file download. The first BM25 query loads and merges sealed stats. Concurrent
queries share that in-flight materialization.
Once materialized, subsequent DataVersion updates load and apply their changed
stats before the new QueryView reports Ready.

### 5.2 Live Event Apply

```text
QueryRuntime.applyLiveEvent(event)
  -> IDFOracleRuntime.ApplyLiveEvent(event)
  -> GrowingBM25StatsStore updates the affected segment
```

Live events update growing BM25 stats and record flushed segment
`sealedAtDataVersion`. `QueryRuntime` owns ordering and ensures the same event
sequence is also applied to the other resource modules.

### 5.3 First QueryView Up

The first QueryView `Up` report waits for `QueryRuntime.Initialize` to complete
successfully, not for an IDF-specific catchup handle. Eager initialization
includes sealed stats loading. Lazy initialization reaches readiness after
growing preparation without materializing sealed stats.

`QueryRuntime.Initialize` returns successfully after:

1. `GrowingRuntime.Prepare` returns;
2. `IDFOracleRuntime.Prepare` returns;
3. `QueryRuntime` takes the current live-event buffer batch;
4. every event in that batch has been applied to both modules;
5. `QueryRuntime` becomes ready and schedules draining any later events.

### 5.4 Oracle Advancement

```text
QueryView resource preparation
  -> QueryRuntime.PrepareDataVersion(target)
  -> IDFOracleRuntime.PrepareDataVersion(target)
  -> materialized: compute and commit the target's stats
  -> unmaterialized lazy: advance the target version without sealed I/O
  -> report Ready
```

Once the initial aggregate exists, `PrepareDataVersion` resolves and downloads
sealed BM25 files before a QueryView with a newer DataVersion becomes ready,
decodes changed contributions, and commits the BM25 diff before reporting
Ready. If lazy initialization has not yet materialized the aggregate, version
preparation records the new target without sealed I/O. Its first BM25 query
materializes the latest recorded target.

The diff model:

```text
negative contributions:
  current sealed entries absent from the target or replaced by a new resource
  current growing segments that are no longer visible at the target

positive contributions:
  target sealed entries absent from current or replacing an old resource
  growing segments newly visible at the target
```

Advancement loads sealed stats and computes their positive and negative diffs
outside the oracle write lock. Commit applies those diffs, updates growing
membership and the current DataVersion, and publishes the new sealed file map
under one write lock. Resource preparation retries recoverable compute errors
and reports Unrecoverable for permanent failures; neither case reports Ready.
Live growing updates continue to apply to the aggregate.

### 5.5 Cleanup

After an advance commit, `IDFOracleRuntime` releases obsolete current and
prepared sealed file references and removes growing BM25 stats that can no
longer contribute. The cache deletes a local resource directory when its final
reference is released.

Cleanup is internal. The resource manager never calls an IDF-specific truncate
operation.

### 5.6 Close

`Close` marks the oracle closed, cancels any in-flight lazy materialization,
releases current and prepared sealed file references, and makes the oracle
unavailable. It is called only by `QueryRuntime.Close`.
