# Syncer Design

> A reliable message delivery layer over unreliable gRPC bidirectional streams.
> Package: [`internal/views/coord/coordview/syncer/`](../../../../internal/views/coord/coordview/syncer/), Proto: [view.proto](../../../../pkg/proto/view.proto)

## 1. Architecture

```
┌───────────────────────────────────────────────────────────────────┐
│                     Coord Manager (Caller)                       │
│         │ SyncViews(group)          ▲ OnSyncResponse /          │
│         ▼                           │ OnQueryNodeLost           │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │                     ReliableSyncer                          │ │
│  │  • Watches node changes via ViewSyncClient                 │ │
│  │  • Lazily creates resumableSyncers per node                │ │
│  │  • Drains removed nodes (service discovery)                │ │
│  │                                                             │ │
│  │  ┌──────────────────┐ ┌──────────────────┐                 │ │
│  │  │ resumableSyncer  │ │ resumableSyncer  │ ...             │ │
│  │  │ (per work node)  │ │ (per work node)  │                 │ │
│  │  │ • pendingSync    │ │ • pendingSync    │                 │ │
│  │  │ • backoff retry  │ │ • backoff retry  │                 │ │
│  │  │ • send/recv loop │ │ • send/recv loop │                 │ │
│  │  └──────────────────┘ └──────────────────┘                 │ │
│  └─────────────────────────────────────────────────────────────┘ │
└───────────────────────────────────────────────────────────────────┘
              │                     ▲
         gRPC Stream           gRPC Stream
              ▼                     │
         Work Node (SN/QN)
```

**ReliableSyncer**: Top-level component that:
1. Lazily creates per-node `resumableSyncer` instances on first `SyncViews` call (via `getOrCreateSyncer`).
2. Watches node changes via `ViewSyncClient` and drains `resumableSyncer` instances for removed nodes.
3. Detects QueryNode loss via service discovery (not reconnect timeout).

**resumableSyncer**: Per-node component that owns a `pendingSyncQueryViews` instance. Runs a single `loop()` goroutine that creates a stream, re-pushes all pending views, and runs send/recv loops in parallel. On stream break, reconnects with exponential backoff.

## 2. Interfaces

Defined in [`internal/views/coord/coordview/syncer/reliable_syncer.go`](../../../../internal/views/coord/coordview/syncer/reliable_syncer.go).

### SyncView

Pairs a query view with its callbacks:

- **View**: The `QueryViewAtWorkNode` to push. Target node determined by `View.WorkNode()`.
- **OnSyncResponse**: Invoked when the node sends a real response. Returns `true` when the current node-targeted sync is complete and the entry can be removed from pending; returns `false` to keep monitoring.
- **OnQueryNodeLost**: Invoked when the target QueryNode is declared lost via service discovery. StreamingNode loss is not a per-view QueryView event; SN availability is handled by the channel assignment layer. The entry is removed from pending after draining.

### SyncGroup

Pre-grouped batch: `ViewsByNode map[WorkNodeKey][]SyncView`.

### ReliableSyncer

- **SyncViews(ctx, group)**: Enqueues views for delivery. Non-blocking.
- **Close()**: Gracefully closes all streams. Must only be called during Coordinator shutdown.

### ViewSyncClient

Provides service discovery and gRPC stream creation for all work node types. Internally routes to the appropriate backend based on `NodeType`.

- **WatchNodeChanged(ctx)**: Returns a channel signaling node membership changes across all node types.
- **GetAllNodes(ctx)**: Returns all currently known nodes.
- **IsNodeAlive(ctx, node)**: Local cache lookup checking if a node is alive.
- **OpenSyncStream(ctx, node)**: Opens a `SyncQueryView` bidirectional stream.
- **Close()**: Releases resources.

## 3. Per-Node Pending Views

Each `resumableSyncer` owns a `pendingSyncQueryViews` instance that tracks views dispatched to its node.

```
pendingSyncQueryViews
├── mu sync.Mutex
├── entries map[QueryViewKey]SyncView   // pending entries awaiting response
├── unsent  []*QueryViewOfShard         // protos accumulated by Upsert, drained by sendLoop
└── notify  chan struct{} (cap 1)       // signaled by Upsert
```

### Operations

| Method | Description |
|---|---|
| `Upsert(sv)` | Insert/replace entry, append proto to `unsent`, signal `notify`. |
| `Ready()` | Returns the `notify` channel for `sendLoop` to select on. |
| `DrainUnsent()` | Atomically drain and return `unsent` protos. |
| `MatchResponse(pb)` | Match response to entry. Invokes `OnSyncResponse` outside the pending mutex. If it returns true, delete the entry only when the stored revision still matches. |
| `Drain(node)` | Remove all entries. If `node` is a QueryNode, invoke `OnQueryNodeLost(node)` for pending QueryNode entries. StreamingNode drain only clears pending entries. |
| `CollectProtos()` | Return protos for all entries. Used for re-push on reconnection. |

### Concurrency

- `Upsert` is called from `SyncViews` (caller goroutine).
- `MatchResponse` is called from `recvLoop` (per-node goroutine).
- `MatchResponse` does not hold the pending mutex while calling `OnSyncResponse`; callbacks can enqueue follow-up syncs without self-deadlock.
- Each pending entry carries a revision. If a callback enqueues a replacement while it runs, a true return from the old callback only deletes the entry when the revision still matches.

## 4. resumableSyncer

Per-node component that maintains a gRPC bidirectional stream.

### Lifecycle

```
loop() goroutine:
    for ctx not cancelled:
        stream = OpenSyncStream()       // backoff on failure
        rePush(stream)                  // DrainUnsent + CollectProtos → sendBatched
        if rePush fails → continue      // skip to reconnect

        start sendLoop goroutine        // Ready() → DrainUnsent → sendBatched
        recvLoop (current goroutine)    // Recv → MatchResponse

        stream broke → cancel sendLoop → wait → backoff → retry
```

### Batched Sending

`sendBatched` sends protos in batches of 16 (`sendBatchSize`). Used by both `sendLoop` (incremental) and `rePush` (full re-push on reconnection).

On reconnection, `rePush` clears stale `unsent` protos before collecting from `entries` to avoid duplicate sends.

### Close and Drain

- **Close()**: Cancels context, waits for `loop()` goroutine to exit. Does NOT drain pending views.
- **DrainPendingIfNodeLost()**: Must only be called after `Close()`, when the node is declared lost. For QueryNodes, invokes `OnQueryNodeLost(node)` for remaining pending entries. For StreamingNodes, only clears pending entries.

## 5. ReliableSyncer Implementation

### SyncViews Flow

```
SyncViews(ctx, group):
    for each (nodeKey, views) in group.ViewsByNode:
        rs, closed = getOrCreateSyncer(ctx, nodeKey, views)
        if closed → return ErrSyncerClosed
        if rs != nil → rs.Sync(views)
        else → OnQueryNodeLost(qn) for QN views   // QueryNode not found
```

`getOrCreateSyncer` acquires the lock once and:
1. Returns existing `resumableSyncer` if found.
2. Otherwise calls `IsNodeAlive` (local cache lookup, safe under lock).
3. If alive, creates a new `resumableSyncer` and inserts it into the map.
4. If not alive, returns nil (views will be drained by caller).

### QueryNode Loss Detection

QueryNode loss is determined by **service discovery**, not by reconnect timeout. StreamingNode unavailability is handled by the channel assignment layer and is not delivered as a per-view lost callback.

A single background `watchNodes` goroutine watches for node membership changes via `ViewSyncClient.WatchNodeChanged`. On each change, `drainRemovedNodes` is called:

```
drainRemovedNodes():
    nodes = GetAllNodes()
    lock:
        for each syncer not in nodes:
            remove from map
    for each removed syncer:
        Close()
        DrainPendingIfNodeLost()   // OnQueryNodeLost(qn) for QN pending entries
```

Key design decisions:
- **Lazy creation**: `resumableSyncer` instances are created on demand by `getOrCreateSyncer`, NOT by the background watcher. The watcher only handles removals.
- **No reconnect timeout**: `resumableSyncer` retries indefinitely with backoff until closed.
- **Separation of concerns**: Stream reconnection (resumableSyncer) is independent of node liveness (service discovery).

### Concurrency

- `getOrCreateSyncer` holds `s.mu` while calling `IsNodeAlive` (local cache lookup). This ensures mutual exclusion with `drainRemovedNodes`, preventing a race where a syncer is created for a node that was just removed.
- `rs.Sync(views)` is called after releasing the lock. If `drainRemovedNodes` closes the syncer concurrently, views added after drain are lost. This is acceptable because the upper-layer state machine will retry.

### Close

```
Close():
    set closed = true
    cancel context
    wait for watchNodes goroutine
    close all remaining resumableSyncers (no drain — graceful shutdown)
```

Must only be called during Coordinator shutdown. After Close, the ReliableSyncer cannot be reused — a new instance must be created via Coordinator recovery.

## 6. Key Scenarios

### 6.1 Normal Flow

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│                           │
  │                         │─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │◄──recv(resp)──────────────│
  │                         │─OnSyncResponse(resp)→true │
  │  callback invoked       │─delete pending[key]       │
```

### 6.2 Stream Break + Reconnection

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │        ╳ stream breaks     │
  │                         │─backoff retry..            │
  │                         │─new stream opened──────────│
  │                         │─rePush: re-send pending──►│
  │                         │◄──recv(resp)──────────────│
  │                         │─OnSyncResponse(resp)→true │
  │  callback invoked       │─delete pending[key]       │
```

### 6.3 QueryNode Lost (Service Discovery)

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │                            ╳ node crashes
  │                         │◄─service discovery: node removed
  │                         │─Close resumableSyncer
  │                         │─DrainPendingIfNodeLost
  │  OnQueryNodeLost(qn)    │─delete pending[key]
```

### 6.4 OnSyncResponse Returns False (Continue Monitoring)

```
Caller                   ReliableSyncer              Node
  │─SyncViews(group)──────►│─pending[key]=sv           │
  │                         │─send(view)───────────────►│
  │                         │◄──recv(resp1)─────────────│
  │                         │─OnSyncResponse(resp1)→false
  │  (continue)             │◄──recv(resp2)─────────────│
  │                         │─OnSyncResponse(resp2)→true│
  │  (done)                 │─delete pending[key]       │
```

### 6.5 Entry Replacement

```
Caller                   ReliableSyncer
  │─SyncViews({v1,cb1})───►│─pending[key]={v1,cb1}
  │                         │─send(v1)──►
  │─SyncViews({v2,cb2})───►│─pending[key]={v2,cb2}
  │                         │  (cb1 silently replaced)
  │                         │─send(v2)──►
```

## 7. Internal Architecture

```
reliableSyncer
├── client ViewSyncClient                           // unified service discovery + stream creation
├── mu sync.Mutex
├── resumableSyncers map[WorkNodeKey]*resumableSyncer
├── closed bool
├── ctx / cancel
└── watchNodes goroutine: watch changes → drainRemovedNodes

resumableSyncer
├── node WorkNode
├── client ViewSyncClient
├── pending *pendingSyncQueryViews                  // per-node pending tracker
├── ctx / cancel
└── loop goroutine:
        create stream → rePush → sendLoop + recvLoop → on break, backoff → retry

pendingSyncQueryViews
├── mu sync.Mutex
├── entries map[QueryViewKey]SyncView
├── unsent []*QueryViewOfShard
└── notify chan struct{} (cap 1)
```

## 8. File Organization

```
internal/views/coord/coordview/
├── syncer/
│   ├── reliable_syncer.go          # ReliableSyncer interface, SyncView, SyncGroup, ViewSyncClient
│   ├── syncer_impl.go              # reliableSyncer implementation + node watcher
│   ├── resumable_syncer.go         # Per-node stream with backoff retry
│   └── pending_sync_query_views.go # Per-node pending view tracker
└── state_machine.go                # CoordQueryView state machine
```
