# QueryViewHandler Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

> Work-node side components that receive Coord-pushed query views and report state changes back.
> Counterpart to the Coord-side [Syncer](syncer.md).

## 1. Architecture

### Components

- **Coord (ReliableSyncer)**: Pushes query view states to work nodes via gRPC bidirectional stream. Receives state reports back.
- **ViewSyncServer**: Per-stream gRPC handler. Bridges transport and QueryViewHandler. SN/QN share the same implementation. Receives `SyncRequest` from Coord, calls `handler.ApplyViews`, collects reports via `pendingReports`, sends `SyncResponse` back.
- **QueryViewHandler**: Per-node singleton. SN and QN each provide their own implementation. Manages SM instances across shards. Outlives individual gRPC streams.
- **ShardView**: Per-shard internal component. Holds SM instances under a shard-level mutex. Routes Coord pushes and async callbacks to the correct SM.
- **pendingReports**: Per-stream internal buffer. Deduplicates reports by QueryViewKey (last-writer-wins). Bridges async `OnReport` callbacks to the send loop.
- **SegmentManager** (QN) / **ResourceManager** (SN): External dependencies injected via constructor. Drive SM progress through async callbacks.
- **Catalog** (SN only): Persistence interface for crash recovery.

### Data Flow

1. Coord sends `SyncRequest` → ViewSyncServer recv loop.
2. ViewSyncServer converts protos to `ApplyView` (each with `OnReport` callback → `pendingReports.Update`), calls `handler.ApplyViews`.
3. QueryViewHandler routes views to the appropriate ShardView by ShardID.
4. ShardView creates/drives SM instances, invokes Acquire/Release on external deps.
5. External deps call back asynchronously (OnReady, OnDropped, etc.) → ShardView drives SM → SM produces report → `OnReport` callback → `pendingReports.Update`.
6. ViewSyncServer send loop drains `pendingReports` → `stream.Send(SyncResponse)` → Coord.

### Key Invariants

- **Report path unity**: All reports (immediate and asynchronous) flow through the same `OnReport → pendingReports → send loop → stream.Send` path.
- **Callback replacement**: When a stream reconnects and Coord re-pushes, `ApplyViews` replaces old `OnReport` callbacks with new ones. Old callbacks write to a stopped `pendingReports` — silently ignored, no panic.
- **Shard-granularity locking**: Outer mutex protects the shard map; per-shard mutex serializes SM operations. Views on different shards can be applied concurrently.
- **Reachable shard ownership**: An empty shard marks itself detached before
  invoking its callback. The handler deletes only the identical map instance,
  and retries a batch rejected by a detached shard on the current replacement.

## 2. ViewSyncServer

Implements the gRPC `SyncQueryView` bidirectional streaming RPC. Per-stream — created when a stream is established, destroyed when the stream ends.

### recv loop (main goroutine)

1. Receive `SyncRequest` from Coord.
2. `SyncQueryViewsRequest`: convert protos to `ApplyView` (each with `OnReport` → `pendingReports.Update`), call `handler.ApplyViews`.
3. `SyncCloseRequest`: enqueue close signal via `pendingReports.SetCloseResponse()`, return.

### send loop (background goroutine, sole caller of `stream.Send`)

1. Wait on `pendingReports.Ready()`.
2. Drain all pending reports, batch into `SyncResponse`, send.
3. If close flag is set, send `SyncCloseResponse` and exit.

### Stream Lifecycle

1. **Established**: create `pendingReports`, start send loop, enter recv loop.
2. **Reconnection**: Coord re-pushes all views. `ApplyViews` replaces old callbacks. SMs re-report current state (fast-forward).
3. **Graceful close**: Coord sends close request → send loop drains remaining reports → sends close response → exits.
4. **Stream broken**: recv loop returns error → `pendingReports.Close()` → send loop drains and exits. Old callbacks become stale (no-op on closed `pendingReports`).

## 3. QueryViewHandler

The `QueryViewHandler` interface has a single method:

```
ApplyViews(views []ApplyView)
```

Each `ApplyView` carries a coord-pushed `View` and an `OnReport` callback. All state reports — both immediate (from Coord push handling) and asynchronous (from external dependency callbacks) — are delivered exclusively through `OnReport`.

### SM Lifecycle

- **Auto-create**: Unknown QueryViewKey + Preparing state → new SM + resource acquisition.
- **Auto-destroy**: SM reaches Dropped → entry removed from shard map → `onEmpty` callback removes shard if empty.
- **Callback replacement**: Re-apply of same QueryViewKey replaces `OnReport`. Old callback is never invoked after replacement.
- **Operation idempotency**: Duplicate Coord pushes for the same QueryViewKey
  reuse the existing handler entry and replace its callback. The SM consumes
  the pushed state and external dependencies are invoked only when the SM
  produces a new resource operation.

### Unknown View Handling

When a Coord push arrives for a view not in the handler (e.g., node restarted):

| Pushed State | Behavior |
|---|---|
| Preparing | Create new SM, start resource acquisition |
| Down | Report Dropped immediately: the SN has already lost this teardown view, so Coord can fast-forward cleanup |
| Dropped | Report Dropped immediately (let Coord finish cleanup) |
| Other | Report Unrecoverable (state lost, Coord generates replacement) |

## 4. SN vs QN Differences

| Aspect | QN | SN |
|---|---|---|
| External deps | `SegmentManager` | `ResourceManager`, `Catalog` |
| SM states | Preparing → Ready → Dropping → Dropped | Preparing → Ready → Up → Down → Dropping → Dropped |
| Recovery | None (stateless) | UpRecovering from persisted Up views |
| Persistence | None | Up → save; Down/Dropped → delete |
| ApplyViews ordering | Preparing/Up first, then teardown states | Preparing/Up first, then teardown states |

### 4.1 QN: SegmentManager Interaction

**Normal flow (Preparing → Ready → Dropped):**

1. Coord pushes Preparing → handler creates SM → calls `segMgr.Acquire(OnReady, OnUnrecoverable)`.
2. SegmentManager loads segments asynchronously.
   - **Success**: calls `OnReady(readySegments)` (may be called multiple times for incremental progress) → SM advances Preparing → Ready → report Ready to Coord.
   - **Fatal error**: calls `OnUnrecoverable` for the acquire invocation → SM advances Preparing → Unrecoverable → report Unrecoverable to Coord.
3. Coord pushes Dropped → SM enters Dropping → calls `segMgr.Release(OnDropped)`.
4. SegmentManager releases segments asynchronously → calls `OnDropped` → SM advances Dropping → Dropped → report Dropped to Coord → entry cleaned up.

All callbacks must be asynchronous (not during Acquire/Release) to avoid deadlocking the shard mutex.
Duplicate QueryViewKey handling is owned by the handler/SM pair, not by
`SegmentManager`.

### 4.2 SN: ResourceManager + Catalog Interaction

**Normal flow (Preparing → Ready → Up → Down → Dropped):**

1. Coord pushes Preparing → handler creates SM (generates Preparing report immediately) → calls `resMgr.Acquire(OnReady, OnUnrecoverable)`.
2. ResourceManager prepares resources asynchronously. `OnReady` advances Preparing → Ready; `OnUnrecoverable` advances Preparing → Unrecoverable and reports the failure to Coord.
3. Coord pushes Up → SM advances Ready → Up → **persist Up** → report Up to Coord.
4. Coord pushes Down → SM advances Up → Down → **persist Down (= delete recovery info)** → report Down to Coord.
5. Coord pushes Dropped → SM enters Dropping → **persist Dropped (= delete recovery info)** → calls `resMgr.Release(OnDropped)`.
6. ResourceManager releases resources asynchronously → calls `OnDropped` → SM advances Dropping → Dropped → report Dropped to Coord → entry cleaned up.

**Persist-before-report invariant**: Persistence is always executed before report. If SN crashes after reporting but before persisting, Coord would believe the state advanced while SN lost it.

The StreamingNode catalog wraps its metadata KV with
`ReliableWriteMetaKv`, so transient retry and undetermined-write handling are
centralized in the metastore layer. The handler passes the WAL lifecycle
context to catalog writes so shutdown cancels an in-progress reliable write.
A canceled write does not advance the corresponding report or Release. Other
write failures are terminal at this layer; the handler does not implement a
second persistence retry mechanism above `ReliableWriteMetaKv`.

**Full-view persistence invariant**: The SN-persisted Up view is the complete
`QueryViewOfShard` pushed by Coord, not just `QueryViewOfStreamingNode`. The
StreamingNode-local resource manager only consumes the SN portion. Retaining
the complete topology keeps recovery metadata self-contained for later
consumers without importing query execution in this change.

SN-local persisted key format:

- `streamingnode-meta/wal/{pchannel}/qv/{collectionID}/{replicaID}/{vchannelIndex}/{streamingVersion}/{compactVersion}/{queryVersion}` — `QueryViewOfShard` proto.

The pchannel is already present in the parent path, so the compact key stores
only the canonical vchannel index plus the QueryView/DataView version tuple.
Recovery validates the reconstructed key identity against the persisted proto.

### 4.3 SN: Crash Recovery

SN persists only the Up state. On crash recovery:

1. Load persisted full Up shard views from `Catalog`.
2. Create SMs in UpRecovering state (Coord-visible as Up).
3. Construct each recovered shard, install its identity-checked empty callback,
   and publish it in the handler map.
4. Call the injected `resMgr.Acquire(OnReady, OnUnrecoverable)` for each view.
5. `OnReady` drives UpRecovering → Up → report Up. `OnUnrecoverable` drives
   UpRecovering → Unrecoverable locally without reporting to Coord and retains
   persisted recovery metadata until Coord later pushes Dropped.

The resource interface and state-machine failure wiring are part of this change;
the concrete resource preparation implementation remains outside this scope.

### 4.4 SN: handleCoordDropped and Persistence Cleanup

When Coord pushes Dropped, the SM enters Dropping. The persist behavior depends on prior state:

| Prior State | pendingPersist |
|---|---|
| Up, UpRecovering | Delete (persisted recovery info exists) |
| Unrecoverable | Delete (may have entered from UpRecovering, stale recovery info on disk) |
| Preparing, Ready, Down | None (no persisted recovery info) |

If future resource wiring drives UpRecovering to Unrecoverable, the state
machine retains persisted Up metadata until Coord's Dropped push; deletion is
deferred to that Dropping transition.

### 4.5 SN: Handoff Release Ownership

Normal Dropping and `CloseForHandoff` share one release record per view entry.
The first path starts `ResourceManager.Release` and owns a completion channel;
subsequent paths only reuse and wait on that channel. Handoff detaches and
clears the shard under its mutex, then waits without the mutex for every
existing release callback. This guarantees one Release invocation per view and
still waits for cleanup already in flight.

## 5. Liveness Contracts

The handler's response guarantee depends on external dependencies fulfilling callback obligations:

### SegmentManager (QN)

| Operation | Obligation |
|---|---|
| `Acquire` | For each invocation issued by the SM, must eventually invoke at least one of that invocation's `OnReady` or `OnUnrecoverable` callbacks |
| `Release` | For each invocation issued by the SM, must eventually invoke that invocation's `OnDropped` callback exactly once |

### ResourceManager (SN)

| Operation | Obligation |
|---|---|
| `Acquire` | Must eventually invoke exactly one of `OnReady` or `OnUnrecoverable` |
| `Release` | Must eventually invoke `OnDropped` exactly once |

All callbacks must be asynchronous. Synchronous invocation during Acquire/Release will deadlock the shard mutex.

Violating any contract leaves the corresponding view stuck (Preparing/UpRecovering/Dropping) with no report to Coord.

## 6. Package Organization

| Package | Contents |
|---|---|
| `worknode/handler` | `ApplyView`, `QueryViewHandler` interface, `ViewSyncServer`, `pendingReports` |
| `querynodev2/qnview` | `QNQueryViewHandler`, `QNQueryViewStateMachine`, `SegmentManager` interface |
| `streamingnode/server/wal/snview` | `SNQueryViewHandler`, `SNQueryViewStateMachine`, `StreamingNodeResourceManager` interface, pchannel-bound `metastore.StreamingNodeCataLog` usage |
