# Syncer Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

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

- **RegisterNodeChangedNotifier(func())**: Registers a non-blocking membership-change callback.
- **IsNodeAlive(ctx, node)**: Local cache lookup checking if a node is alive.
- **OpenSyncStream(ctx, node)**: Opens a `SyncQueryView` bidirectional stream.
- **Close()**: Releases resources.

## 3. Per-Node Pending Views

Each `resumableSyncer` owns a `pendingSyncQueryViews` instance that tracks views dispatched to its node.

```
pendingSyncQueryViews
├── mu sync.Mutex
├── entries map[QueryViewKey]SyncView   // pending entries awaiting response
├── unsent  map[QueryViewKey]*QueryViewOfShard // latest incremental proto per key
└── notify  chan struct{} (cap 1)       // signaled by Upsert
```

### Operations

| Method | Description |
|---|---|
| `Upsert(sv)` | Insert/replace the entry and its latest `unsent` proto, then signal `notify`. |
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
- `unsent` is latest-wins by QueryViewKey, so repeated updates while a node is
  disconnected consume memory proportional to pending keys rather than update
  count. Reconnection still re-pushes the authoritative `entries` snapshot.

## 4. resumableSyncer

Per-node component that maintains a gRPC bidirectional stream.

### Lifecycle

```
loop() goroutine:
    for ctx not cancelled:
        attemptCtx = WithCancel(ctx)
        stream = OpenSyncStream(attemptCtx)
        rePush(stream)                  // DrainUnsent + CollectProtos → sendBatched
        if rePush fails → cancel + CloseSend + backoff

        start sendLoop(attemptCtx)       // Ready() → DrainUnsent → sendBatched
        start recvLoop(attemptCtx)       // Recv → MatchResponse

        either loop exits
        → cancel attemptCtx → wait for both loops → CloseSend
        → backoff → retry
```

Open failures, re-push failures, send failures, receive failures, and immediate
close responses all enter the same reconnect backoff. The exponential backoff
is reset only after a valid QueryView response is received or the stream stays
healthy for the stable interval. Canceling the parent context interrupts both
the active attempt and its reconnect delay.

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

A notifier registered through `ViewSyncClient.RegisterNodeChangedNotifier`
performs a non-blocking send to a capacity-one notification channel when
membership may have changed. A dedicated ReliableSyncer worker serially drains
that channel and invokes `drainRemovedNodes`; concurrent notifications are
coalesced while preserving a follow-up pass when a change arrives during an
active drain:

```
service discovery callback:
    non-blocking enqueue to nodeChanged

node-change worker:
    wait for nodeChanged
    drainRemovedNodes():
        snapshot current syncers
        for each syncer whose IsNodeAlive is false:
            remove it if the map entry is unchanged
        for each removed syncer:
            Close()
            DrainPendingIfNodeLost()   // OnQueryNodeLost(qn) for QN pending entries
```

Key design decisions:
- **Lazy creation**: `resumableSyncer` instances are created on demand by `getOrCreateSyncer`, NOT by the background watcher. The watcher only handles removals.
- **No reconnect timeout**: `resumableSyncer` retries indefinitely with backoff until closed.
- **Separation of concerns**: Stream reconnection (resumableSyncer) is independent of node liveness (service discovery).

### Concurrency

- `syncViewsToNode` holds `s.mu` across syncer lookup, `IsNodeAlive` (a local
  cache lookup), lazy creation, and `rs.Sync(views)`. This is mutually exclusive
  with `drainRemovedNodes`, so a node-change drain cannot miss views already
  accepted by `SyncViews`.
- If the node is already absent, `syncViewsToNode` returns the affected views
  and `SyncViews` invokes `OnQueryNodeLost` after releasing `s.mu`, avoiding
  callback re-entry while the syncer map lock is held.

### Close

```
Close():
    set closed = true
    cancel context
    wait for node-change worker
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
├── nodeChanged chan struct{} (capacity 1)           // coalesced non-blocking notifier
└── node-change worker: notifications → drainRemovedNodes

resumableSyncer
├── node WorkNode
├── client ViewSyncClient
├── pending *pendingSyncQueryViews                  // per-node pending tracker
├── ctx / cancel
└── loop goroutine:
        attempt context → create stream → rePush → sendLoop + recvLoop
        → cancel + join + CloseSend → backoff → retry

pendingSyncQueryViews
├── mu sync.Mutex
├── entries map[QueryViewKey]SyncView
├── unsent map[QueryViewKey]*QueryViewOfShard
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
