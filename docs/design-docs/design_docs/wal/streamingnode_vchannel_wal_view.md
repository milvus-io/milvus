# StreamingNode VChannel WAL Input View

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

`VChannelWALView` is an internal preparation DTO built by
`VChannelRecoveryModule` for QueryRuntime. It is not a RecoveryStorage API and
does not participate in the global checkpoint protocol.

> Status: implemented for SN query resource preparation in this branch.
> `VChannelRecoveryModule` captures the view and installs a shared QueryRuntime;
> GrowingRuntime performs bounded Delete replay through the local Summary-backed
> TransformLog stream. This does not enable QN remote subscriptions or replace
> the existing WAL L0 materializer.

## 1. Ownership

```text
QueryViewStateMachine.Acquire
  -> PChannelRecoveryManager.Acquire
  -> VChannelRecoveryModule.queryWALViewLocked
  -> QueryRuntime.Initialize
  -> QueryRuntimeModule.Prepare
```

The VChannel module must coordinate these inputs for a no-gap view:

- VChannel and schema history;
- growing Segment stable and pending state;
- Segment lifecycle and durable commit state;
- WALSummary readable history through the local TransformLog adaptor;
- the live message observation path.

## 2. Runtime Frontiers

The view may contain runtime Growing and Transform frontiers used for MVCC.
These are not RecoveryStorage checkpoints:

- they do not start WAL replay;
- they may describe observed in-memory pending state;
- QueryRuntime waits on them according to query-plan TimeTicks;
- they are reconstructed from component snapshots plus the one WAL replay.

The persisted global checkpoint is only the initial lower bound for startup
observation. Component `checkpoint_time_tick` fields independently suppress
effects already represented by snapshots.

## 3. No-Gap Capture

WAL-view capture and QueryRuntime registration use the same VChannel lock:

```text
hold VChannel lock
  -> capture stable and pending Segment state
  -> capture the Transform replay boundary required by the observed snapshot
  -> protect its historical start point in Summary retention
  -> construct VChannelWALView
  -> install QueryRuntime in Preparing state
release VChannel lock
```

Messages observed before capture are represented by stable objects, pending
buffers, pending tasks, or WALSummary records. Messages observed afterward see
the installed QueryRuntime and enter its pending event queue.

The captured Transform boundary describes the snapshot's required WAL prefix,
not L0Materializer's cursor. Bounded replay through the local TransformLog
adaptor must wait until Summary can completely provide that range before
reporting SyncUp/completion. Do not lower the required end because a sampled
Summary frontier is behind, or raise it to recovered Summary history ahead of
VChannel observation. RecoveryStorage installs Summary records before VChannel state/window updates;
snapshot capture and runtime registration preserve the same no-gap guarantee.

Protect the history before GC can remove it and hold that requirement through
preparation. An already truncated start is an error, not an empty replay.
Neither L0 completion nor a subscription's delivery cursor proves that retained
QueryViews no longer need historical Delete data.

QueryRuntime receives ordinary immutable copies and never retains Message Ack
handles.

## 4. Startup Readiness

The single recovery scanner reaches RecoveryBarrier before the startup
write-path snapshot is published. QueryRuntime preparation may additionally
wait for actual component conditions, including:

- every retained nonterminal flushed Segment has `sealed_at_data_version`;
- required TransformLog subscription start points remain readable;
- captured schema and segment state form a consistent VChannel snapshot.

It does not wait for a metadata scanner, data scanner, Observe-mode transition,
or second checkpoint.

## 5. Segment Lifecycle Selection

Each segment is classified from its stable metadata and runtime closing flag:

```text
GROWING, no runtime close
    -> growing Segment snapshot
GROWING, runtime close pending
    -> stop accepting data; wait for data publication and final DataCoord commit
TOMBSTONED
    -> lifecycle complete (empty/retired segments may have no DataVersion)
```

New final commits install `sealed_at_data_version` and TOMBSTONED together.
The runtime close is not part of a catalog snapshot; replay reconstructs it
from Flush/Drop after a crash. Existing legacy SEALED/FLUSHED recovery remains
separate from this path.

There is no second recovery checkpoint tied to this lifecycle classification.

## 6. Invariants

1. VChannelRecoveryModule is the only builder of VChannelWALView.
2. View capture and live observer installation have no message gap.
3. QueryRuntime owns no RecoveryStorage handle.
4. Runtime MVCC frontiers are not global recovery checkpoints.
5. Readiness depends on concrete component state, not dual recovery phases.

## 7. Current Preparation Flow

1. A Preparing QueryView, or a recovered Up view in UpRecovering, calls the
   PChannel resource manager's `Acquire`. The VChannel owner rejects inactive
   channels and versions older than its retained segment-data floor.
2. Under the VChannel lock, capture the schema, stable/pending segment snapshot,
   requested DataVersion and observed TimeTick T. Install the runtime before
   releasing the lock so subsequent messages enter its pending event queue.
   A missing write-path state or incomplete final segment commit defers capture.
3. Resolve partitions, loaded fields and indexes through `GetQueryViewLoadInfo`
   when `load_info_version` is nonzero. The current minimal Coord implementation
   supplies current load metadata; this is not a historical load-config store.
4. GrowingRuntime creates segcore resources, loads persisted growing data and
   replays snapshot Insert/Txn tails. Segments sealed after the requested
   DataVersion remain queryable for that older view.
5. GrowingRuntime subscribes to TransformLog for `(start, T]` and applies its
   Delete entries. `start` is the maximum of the earliest retained segment's
   creation TimeTick minus one (zero for missing legacy timestamps) and the
   QueryView's transform start. With no visible segments, start equals T.
   Bounded replay waits for coverage through T; truncated history fails with
   `ErrTransformLogStartPointTruncated` and the view becomes Unrecoverable.
   The current consumer buffers the full interval before applying entries;
   bounded replay here refers to the interval, not total consumer memory. The
   [incremental replay memory TODO](transform_log.md#9-todo-bound-sn-bootstrap-consumer-memory-deferred)
   is explicitly deferred from this PR.
6. IDF initializes from sealed statistics fetched for the requested DataVersion
   and growing statistics from the same snapshot. It maintains one local
   aggregate as described in [IDF Oracle Runtime](../qviews/snview/idf_oracle_runtime.md).
7. QueryRuntime applies the initial queued live batch, enters Ready and keeps
   draining ordered live events. Each QueryView additionally checks that no
   segment final commit remains pending, publishes any completed sealed
   notification not yet emitted by the owner callback, and waits for an ordered
   event barrier. Only then does the callback move Preparing to Ready, or
   UpRecovering to Up. A newly prepared view still needs Coord's Up instruction
   before accepting queries. Queries wait for their required Growing/Transform
   MVCC frontiers before using segment handles.

Later QueryViews on this VChannel reuse the runtime, without repeating initial
TransformLog replay. They repeat the same commit check and applied-event barrier;
an initialized runtime alone does not certify a new view's readiness. Their
explicit DataVersions request asynchronous IDF
refresh; readiness does not wait for a separate per-version BM25 aggregate.
The first runtime initialization does wait for its BM25 resources. Once
bootstrap finishes, Insert/Delete/Txn and lifecycle events arrive through the
VChannel event path, not a second continuous TransformLog subscription.

Implementation references (relative to repository root):

- `internal/streamingnode/server/wal/vchannel/query_resource_module.go`
- `internal/streamingnode/server/wal/vchannel/queryresource/{manager,runtime}.go`
- `internal/streamingnode/server/wal/vchannel/growingruntime/{builder,delete_replay,live}.go`
- `internal/streamingnode/server/wal/walsummary/stream.go`

## 8. Preparation Retry and Loaded Partition Scope

An initialization timeout or transient resource-read failure keeps the view in
Preparing. NodeScheduler retries with backoff even if no new WAL message arrives.
Each failed attempt closes its partial modules and buffered events outside the
owner/manager locks. The next attempt captures a fresh snapshot and installs a
fresh QueryRuntime under the same VChannel lock. It never reuses partially
prepared modules. A released/cancelled build cannot install a replacement or
report Ready for a later acquisition. Truncated TransformLog history and explicit
data-integrity failures report Unrecoverable; owner cancellation stops the build.

Once load metadata is resolved, its partition list is authoritative for the
runtime: an empty list means no loaded partitions. A nil list in a legacy,
unresolved WAL-only snapshot preserves the unrestricted behavior. Growing
snapshot loading, live segment creation/inserts, candidate probing and handle
acquisition all obey the prepared scope. An empty request partition list means
all partitions in that scope; explicitly requesting an unloaded partition fails.
IDF initialization and refresh filter both growing contributions and sealed
resource descriptors using the same nil-versus-empty load-scope convention.

TODO(#40451, pending discussion): a later QueryView with a different
load_info_version or schema still reuses the initial runtime/load scope. It does
not yet trigger resource-spec reconciliation or replacement. The minimal Coord
load-info RPC also lacks exact historical-version resolution. This follow-up
must define old/new resource ownership across Up leases while retaining one
shared current BM25 aggregate. The partition-scope fix does not implement this
load-config transition protocol.

## 9. QueryView Readiness and Version Ordering

The VChannel owner checks all current segments before each view's ready callback.
Open growing segments do not need to flush; closing segments must finish their
final commit. An incomplete commit delays the ready task with scheduler backoff,
without occupying a worker waiting for the commit task. This conservative check
can delay preparation while commits continuously overlap.

`finalCommitDone` remains a durable-write fact, not proof of query visibility.
The final task installs `SealedAtDataVersion` before its owner notification. Under
the owner lock, the ready check therefore publishes completed sealed metadata
itself when necessary, using the normal notification deduplication. It then queues
an event barrier under the same lock and, after enqueue succeeds, waits for its
completion without owner/manager locks. The barrier proves that the querying
modules have applied the handoff metadata.
Runtime closure wakes discarded-barrier waiters; after preparation the manager
rechecks the view reference and runtime identity before reporting Ready.

The resource manager maintains two distinct version boundaries:

- Admission uses the highest DataVersion already accepted during the current
  runtime/reference lifetime. A lower new acquisition is rejected through
  `OnUnrecoverable`, including acquisitions from a different replica. Equal
  versions and idempotent existing references remain valid. Removing the highest
  reference does not lower this boundary; already retained older views continue
  serving according to their lifecycle and leases.
- Reclamation uses the minimum DataVersion among retained references. Reference
  removal, recomputing this minimum and delivering `Advance` are serialized by
  the manager. Runtime keeps its non-monotonic advance assertion. Initialization's
  final watermark application, subsequent advances, live events and module close
  share the runtime's application lock, preventing a recorded initial watermark
  from overtaking or being overtaken by a later module advance.

Object-storage work in `BeforeRelease` and external ready/dropped callbacks run
outside these critical sections. The single current BM25 aggregate and deferred
Delete replay memory optimization remain unchanged.

### Deferred: slow-consumer backpressure and cancellation

TODO(#40451, explicitly deferred from this PR): when GrowingRuntime consumption
falls behind, the bounded pending event queue can fill. Enqueue then waits for
capacity while the caller holds the VChannel owner lock, backpressuring the
RecoveryStorage observation path and potentially delaying other VChannels on the
same PChannel. The capacity wait currently does not wake on context cancellation
alone; it needs capacity progress or runtime closure to unblock.

This is a performance/availability degradation under sustained backlog or a
stalled consumer. It does not let a readiness barrier overtake preceding events
or replace the query's applied-MVCC checks. Keep the current implementation in
this PR. A follow-up should make admission cancellation-aware and assess how to
avoid prolonged owner-lock waits while preserving event order and backpressure,
with full-queue slow-consumer and cancellation coverage.
