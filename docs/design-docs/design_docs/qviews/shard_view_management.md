# Shard View Manager Design

> This document describes the design of `ShardViewManager`, which manages multiple QueryViews for a single shard (vchannel) within a single replica on the Coord side.
> Reference: [Distributed Query View Design](README.md), [QueryView State Machine](query_view_state_machine.md), [view.proto](../../../../pkg/proto/view.proto), [ReliableSyncer](../../../../internal/views/coord/coordview/syncer/reliable_syncer.go), [CoordQueryViewStateMachine](../../../../internal/views/coord/coordview/state_machine.go)

## 1. Overview

`ShardViewManager` is the Coord-side component responsible for:

1. Maintaining the set of active QueryViews for one shard (typically 2–3, double/triple buffer pattern).
2. Orchestrating multiple `CoordQueryViewStateMachine` instances and their cross-view interactions.
3. Persisting view state to ETCD via `QueryViewCatalog`.
4. Pushing view state to nodes via `ReliableSyncer`, with response handling through callbacks.
5. Supporting crash recovery from ETCD-persisted views + node responses.

### Architecture Position

```
Sealed Segment Balancer
        │  (generates QueryViewAtCoordBuilder)
        ▼
 ShardViewManager
        │
        ├── calls Catalog ────────→ ETCD (persistence)
        └── calls ReliableSyncer ─→ SyncQueryView RPC ─→ StreamingNode / QueryNode
                                           │
                                   Node Responses
                                           │
                                           ▼
                               ReliableSyncer routes to
                              ShardViewManager callbacks
```

### Design Principles

- **Active component**: Directly calls `Catalog` for persistence and `ReliableSyncer` for node sync.
- **Callback-driven**: Node responses are delivered via `OnSyncResponse` registered with `ReliableSyncer`. QueryNode loss for an active QN-targeted sync is delivered via `OnQueryNodeLost`. StreamingNode loss is not a per-view event.
- **Self-contained**: Once a view is added via `AddPreparing`, all subsequent state transitions are driven internally through callbacks. External input is only `AddPreparing` and `RequestRelease`.
- **Up-then-Down**: When a new view reaches Up, all older Up views are automatically transitioned to Down.
- **Preemption**: A new `AddPreparing` preempts any existing Preparing/Ready view by injecting a synthetic Unrecoverable response.
- **Batch I/O**: All persist and sync operations within a single method call are batched into one `SaveQueryViews` + one `SyncViews` call for atomicity.
- **Builds on CoordQueryViewStateMachine**: Per-view state machine logic is delegated to `CoordQueryViewStateMachine`. ShardViewManager consumes its `ConsumePersist()` / `ConsumeSync()` outputs and orchestrates multi-view interactions.

## 2. Dependencies

### 2.1 QueryViewCatalog (existing)

ETCD persistence layer. Implemented in `internal/metastore/kv/queryview/kv_catalog.go`.

### 2.2 ReliableSyncer (existing)

Reliable delivery of QueryView syncs from Coord to work nodes. Defined in `internal/views/coord/coordview/syncer/reliable_syncer.go`.

Key properties used by ShardViewManager:

- **Delivery guarantee**: Every outstanding sync eventually receives either a real node response (via `OnSyncResponse`) or, for QueryNode targets, a QueryNode-lost notification (via `OnQueryNodeLost`).
- **Callback model**: Each `SyncView` carries `OnSyncResponse` (invoked on response) and optionally `OnQueryNodeLost` (invoked only for QueryNode loss). The syncer removes the entry after the current sync completes or after draining the lost QueryNode.
- **Pre-grouped**: `SyncGroup.ViewsByNode` is a map keyed by `WorkNodeKey`, pre-grouping views by target node.
- **Replace semantics**: Calling `SyncViews` for a viewKey that already has an outstanding entry replaces the old entry and its callbacks.
- **Non-blocking**: `SyncViews` returns after enqueuing.

### 2.3 CoordQueryViewStateMachine (existing)

Per-view state machine. Defined in `internal/views/coord/coordview/state_machine.go`. Key interface used:
- `OnNodeStateReported(report)` — process a node response
- `ConsumePersist() / ConsumeSync()` — consume pending I/O signals
- `EnterDown()` — transition Up → Down
- `EnterDropping()` — transition Unrecoverable → Dropping
- `State()` — current state

## 3. Interface

```go
// NewShardViewManager creates a new ShardViewManager for the given shard.
// ctx: lifecycle context for Catalog calls within callbacks.
// recoveredViews: views loaded from ETCD during crash recovery.
// Unrecoverable views remain Unrecoverable after construction, waiting for
// AddPreparing or RequestRelease to advance them to Dropping.
// Active views in other states are pushed to their target nodes via syncer.
func NewShardViewManager(ctx, shardID, catalog, syncer, recoveredViews) *ShardViewManager

// AddPreparing adds a new view in Preparing state from a builder.
// The manager assigns QueryVersion automatically:
//   - Same DataVersion as existing views: QV = max(existing QV for same DV) + 1
//   - New DataVersion: QV = 1
// Preempts any existing Preparing/Ready view.
// Validates no DataVersion rollback against existing views.
func (m *ShardViewManager) AddPreparing(ctx, builder *qviews.QueryViewAtCoordBuilder) error

// RequestRelease initiates teardown of all views in this shard.
// Up views → Down (normal teardown). Preparing/Ready views → Unrecoverable → Dropping.
// Actual cleanup completes asynchronously through callbacks.
func (m *ShardViewManager) RequestRelease(ctx) error
```

## 4. Internal Flow

### 4.1 Batch I/O

All operations accumulate persist and sync entries into `ShardViewManager`'s inline fields:

- `pendingPersists []*viewpb.QueryViewOfShard` — accumulated persist entries.
- `pendingSyncs []syncEntry` — accumulated sync entries (`sm` + per-node views pairs).

After all state machine processing is complete, `flush()` executes:
1. One `catalog.SaveQueryViews()` call for all accumulated persists.
2. One `syncer.SyncViews()` call with all accumulated syncs merged into a single `ViewsByNode` map.

This ensures atomicity at the persistence layer and reduces I/O overhead.

### 4.2 processStateMachine

After any state machine event (node response, AddPreparing, RequestRelease), `processStateMachine(sm)` is called:

1. `ConsumePersist()` → if non-nil, append to `pendingPersists`.
2. `ConsumeSync()` → if non-nil, append to `pendingSyncs`.
3. Handle cascading effects based on current state:
   - **Preparing/Ready**: Update `preparingView` pointer.
   - **Up**: Clear `preparingView` if it was this SM, call `downOlderUpView` to cascade older Up view to Down, update `upView` pointer.
   - **Down**: Clear `upView` if it was this SM.
   - **Unrecoverable**: Clear `preparingView`/`upView` if they were this SM. Stay in Unrecoverable — the Manager defers advancement to Dropping until `AddPreparing` or `RequestRelease`, so that the old view's Dropped sync and the new view's Preparing sync can be batched together.
   - **Dropping**: Return (wait for all nodes to confirm Dropped via callbacks).
   - **Dropped**: Remove view from manager.

### 4.3 Sync Routing (collectSyncViews)

The sync proto's state determines routing:

| Sync State | Route To |
|---|---|
| Preparing | SN + all QNs |
| Up | SN only |
| Down | SN only |
| Dropped | SN + all QNs |

Views are collected into a shared `ViewsByNode` map for the batch.

### 4.4 Callback Model

Each `SyncViews` call registers per-node callbacks:
- **OnSyncResponse**: Acquires `m.mu`, finds the SM by version, calls `sm.OnNodeStateReported(resp)`, calls `processStateMachine(sm)`, flushes. Returns true when the current node-targeted sync is complete, stopping ReliableSyncer tracking for that entry.
- **OnQueryNodeLost**: Registered only for QueryNode targets. Acquires `m.mu`, finds the SM by version, calls `sm.OnQueryNodeLost(qn)`, calls `processStateMachine(sm)`, flushes. In Preparing this transitions to Unrecoverable; in Dropping it marks the QN cleanup as complete.

### 4.5 AddPreparing

1. `builder.DataVersion()` → validate no rollback against ALL views.
2. Find existing Preparing/Ready view → preempt via `sm.EnterUnrecoverable()`, `processStateMachine(sm)`.
3. Advance all Unrecoverable views to Dropping via `advanceUnrecoverableToDropping()`, so their Dropped sync is batched with the new Preparing sync.
4. Compute QueryVersion: `max(QV for views with same DV) + 1`.
5. `builder.SetQueryVersion(qv)` → `builder.Build()` → new SM → `processStateMachine(sm)`.
6. `flush()`.

> **TODO**: Add maxViews check (default 3) to limit total active views. During preemption, `maxViews+1` should be allowed temporarily (preempted view is draining).

### 4.6 RequestRelease

- **Preparing/Ready view**: `sm.EnterUnrecoverable()` → `processStateMachine(sm)`.
- **Up view**: `sm.EnterDown()` → `processStateMachine(sm)`.
- Advance all Unrecoverable views to Dropping via `advanceUnrecoverableToDropping()`.
- `flush()`.

## 5. Thread Safety

All access serialized via `m.mu`. Callbacks from ReliableSyncer's recv goroutines acquire the lock. Catalog writes happen under the lock (acceptable: ETCD writes are fast, `ReliableWriteMetaKv` only fails on ctx cancellation). `SyncViews` is non-blocking.

## 6. Invariants

1. **Preemption**: A new Preparing view preempts any existing Preparing/Ready view. At most one non-draining Preparing/Ready view at a time.
2. **Max Views**: *(TODO — not yet implemented)* Total active views ≤ `maxViews` (default 3). During preemption, `maxViews+1` should be allowed temporarily (preempted view is draining).
3. **DataVersion Rollback Prevention**: New Preparing view's DataVersion must not be lower than any existing view's DataVersion.
4. **QueryVersion Auto-Assignment**: The manager assigns QueryVersion = `max(QV for views with same DV) + 1`, or 1 if no matching DV exists.
5. **Write-Ahead Persistence**: State persisted BEFORE pushing to nodes (batch persist then batch sync).
6. **Batch Atomicity**: All persists within a single operation (AddPreparing, RequestRelease, callback) are flushed in one `SaveQueryViews` call. All syncs are flushed in one `SyncViews` call.
7. **Up-then-Down**: When a new view reaches Up, all older Up views immediately transition to Down.
8. **Deferred Dropping**: Unrecoverable views stay in Unrecoverable until `AddPreparing` or `RequestRelease` advances them to Dropping, allowing the old view's Dropped sync and the new view's Preparing sync to be batched together.

## 7. Package Location

```
internal/views/coord/coordview/
    syncer/reliable_syncer.go   // ReliableSyncer interface (existing)
    state_machine.go            // CoordQueryViewStateMachine (existing)
    shard_view_manager.go       // ShardViewManager implementation
    errors.go                   // Error sentinels
    shard_view_manager_test.go  // Tests
```
