# Shard View Manager Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

> This document describes the Coord-side management of QueryViews for one shard
> (vchannel) and the shared flush scheduler used to externalize state-machine
> effects across shards.
> Reference: [Distributed Query View Design](README.md),
> [QueryView State Machine](query_view_state_machine.md),
> [view.proto](../../../../pkg/proto/view.proto),
> [ReliableSyncer](../../../../internal/views/coord/coordview/syncer/reliable_syncer.go),
> [CoordQueryViewStateMachine](../../../../internal/views/coord/coordview/state_machine.go),
> [NodeScheduler](../../../../pkg/util/nodescheduler/scheduler.go).

## 1. Overview

`ShardViewManager` is the in-memory owner of the QueryViews for one shard on one
replica. It is responsible for:

1. Maintaining the active QueryView set, normally following a double/triple
   buffer pattern.
2. Orchestrating `CoordQueryViewStateMachine` instances and cross-view
   interactions such as preemption and Up-then-Down handoff.
3. Creating response and QueryNode-loss callbacks for node synchronization.
4. Publishing per-shard placement statistics.
5. Emitting one immutable shard-scoped dirty event after each state operation.

The manager does not perform ETCD or node-sync I/O itself. All managers owned by
one `ShardViewRegistry` share one `DirtyViewFlushScheduler`. The scheduler merges
events by `ShardID`, claims disjoint shard lanes into concurrent batch tasks,
persists QueryView states, and only then dispatches the corresponding node syncs.

### Architecture Position

```text
QueryView lifecycle caller
        │  AddPreparing / RequestRelease
        ▼
 ShardViewRegistry
        │ owns
        ├──────────────► ShardViewManager per shard
        │                        │
        │                        │ Submit(DirtyViewEvent)
        │                        ▼
        └──────────────► DirtyViewFlushScheduler
                                  │ keyed batching by ShardID
                                  │ Submit multiple batch tasks
                                  ▼
                            NodeScheduler
                                  │
                    ┌─────────────┴─────────────┐
                    ▼                           ▼
             QueryViewCatalog            ReliableSyncer
               ETCD persist              SyncQueryView RPC
                                                │
                                                ▼
                                    ShardViewManager callbacks
```

### Design Principles

- **In-memory state owner**: `ShardViewManager` performs state transitions and
  maintains fast pointers and statistics, but does not block on external I/O.
- **Keyed shared batching**: One task can contain multiple shards, multiple tasks
  can run concurrently, and one `ShardID` never appears in two running tasks.
- **Write-ahead ordering**: Every flush batch completes `SaveQueryViews` before
  calling `SyncViews`.
- **Latest-state coalescing**: Unflushed persist and sync effects are replaced
  independently by newer transitions, allowing the state machine to fast-forward
  without queuing every intermediate state.
- **Callback-driven**: `ReliableSyncer` delivers node responses and QueryNode-loss
  notifications through callbacks registered by the manager.
- **Node-level scheduling**: QueryView flush tasks reuse the common
  `NodeScheduler`; the QueryView package owns no dedicated worker goroutine.
- **Ordered non-blocking submission**: A manager consumes pending effects and
  submits the immutable event while holding `m.mu`, so event enqueue order
  matches state-transition order. `Submit` only merges and enqueues work; ETCD,
  RPC, task execution, and manager callbacks remain outside `m.mu`.

## 2. Components and Dependencies

### 2.1 ShardViewRegistry

`ShardViewRegistry` owns all `ShardViewManager` instances and exactly one
`DirtyViewFlushScheduler`. Recovery opens one explicit Begin/Commit batch,
reconstructs every manager, commits all emitted recovery events, and waits for
the resulting keyed tasks before returning.

The Registry also maintains resident-shard reverse indexes by collection and by
currently placed QueryNode. These indexes support scoped management snapshots.
An empty manager remains resident so later QueryViews for the same replica and
shard continue using the same manager lifecycle. After `RequestRelease`, once
the last QueryView completes durable removal, the released manager is removed
together with its stats and reverse-index entries. An already-empty manager is
removed immediately by `RequestRelease`.

`Close` closes the flush scheduler before the QueryView runtime closes the
underlying `ReliableSyncer`.

### 2.2 QueryViewCatalog

The ETCD persistence layer is implemented in
`internal/metastore/kv/queryview/kv_catalog.go`.

Persisted key format:

- `coord/qv/{collectionID}/{replicaID}/{vchannelIndex}/{streamingVersion}/{compactVersion}/{queryVersion}`

The collection and canonical vchannel index reconstruct shard identity while
the version tuple keeps multiple in-flight views distinct. Recovery validates
the key identity against the persisted proto and reports corruption as a data
integrity error.

### 2.3 ReliableSyncer

`ReliableSyncer` provides resumable delivery from Coord to StreamingNode and
QueryNode.

Properties used by this design:

- `SyncGroup.ViewsByNode` groups syncs by `WorkNodeKey`.
- Each `SyncView` carries `OnSyncResponse` and, for QueryNode targets,
  `OnQueryNodeLost`.
- A newer sync for the same QueryView and node replaces the older pending sync
  and its callbacks.
- `SyncViews` returns after the views have been accepted by the reliable syncer.

### 2.4 CoordQueryViewStateMachine

The per-view state machine owns the latest pending external effect:

```go
type queryViewFlush struct {
    Persist *viewpb.QueryViewOfShard
    Sync    []qviews.QueryViewAtWorkNode
}
```

`Persist` and `Sync` have replace semantics. `ConsumeFlush` atomically drains
both values. This is distinct from the reliable syncer's own pending map: the
state-machine pending value represents effects not yet handed to the external
systems, while the syncer pending map represents accepted but not yet
acknowledged RPC work.

### 2.5 DirtyViewFlushScheduler

The Registry-level scheduler owns:

- Pending immutable events merged by `ShardID` and versioned QueryView key.
- The set of inflight `ShardID` lanes.
- Explicit Begin/Commit-held shard lanes.
- Multiple queued or running one-shot batch tasks.
- Batch sizing using `MetaStoreCfg.MaxEtcdTxnNum`.
- Persist-before-sync execution.
- Lifecycle cancellation and explicit waiting for recovery and tests.

Every task runs through the global `NodeScheduler`. A task claims only shard
lanes that are not already inflight. New work for an inflight shard stays pending
until that task finishes; work for an unrelated shard may immediately enter a
different task.

## 3. Interfaces

Managers are constructed only by `ShardViewRegistry`:

```go
func newShardViewManager(
    ctx context.Context,
    shardID qviews.ShardID,
    eventSubmitter dirtyViewEventSubmitter,
    recoveredViews []*viewpb.QueryViewOfShard,
) *ShardViewManager
```

External lifecycle operations remain:

```go
func (m *ShardViewManager) AddPreparing(
    ctx context.Context,
    builder *qviews.QueryViewAtCoordBuilder,
) error

func (m *ShardViewManager) RequestRelease(ctx context.Context) error
```

`AddPreparing` assigns QueryVersion automatically, rejects DataVersion rollback,
and preempts an existing Preparing or Ready view. `RequestRelease` starts the
normal teardown of all views in the shard. Both methods mutate state under
`m.mu`, atomically consume the resulting effects into one `dirtyViewEvent`,
submit that event to the Scheduler, and then release the lock.

## 4. Internal Flow

### 4.1 State Transition and Event Submission

Every state-changing entry point follows the same pattern:

1. Acquire `m.mu`.
2. Apply the state-machine input.
3. Run `processStateMachine` for each changed state machine. It consumes that
   state machine's `ConsumeFlush` result into manager-local pending slices and
   updates `preparingView`, `upView`, and cascading Up-then-Down state.
4. Move the accumulated effects into one immutable shard-scoped event with
   persistence, node-sync, and post-persist callback information.
5. Call the non-blocking `DirtyViewFlushScheduler.Submit(event)` while still
   holding `m.mu`.
6. Release `m.mu`.

The same pattern is used by `AddPreparing`, `RequestRelease`,
`OnSyncResponse`, and `OnQueryNodeLost`. The Scheduler never calls back into a
manager to scan its state.

### 4.2 processStateMachine

`processStateMachine` consumes the current state machine's pending external
effects and handles its in-memory cross-view effects:

- **Preparing/Ready**: Update `preparingView`.
- **Up**: Clear `preparingView` when applicable, transition an older Up view to
  Down, and update `upView`.
- **Down**: Clear `upView` when applicable.
- **Unrecoverable**: Clear the fast pointers and remain stable until
  `AddPreparing` or `RequestRelease` advances the view to Dropping.
- **Dropping**: Wait for node callbacks.
- **Dropped**: Move the final ETCD deletion effect into the pending persist
  slice and register a post-persist callback. The state machine remains
  resident until that callback runs after persistence succeeds.

Effects are consumed only from state machines explicitly processed by the
current operation. Untouched resident views are not scanned.

### 4.3 Emitting One Shard Event

`consumeDirtyEventLocked` transfers the current operation's manager-local
pending effects:

1. Acquire `m.mu`.
2. Reuse the persist effects accumulated by `processStateMachine`.
3. Convert accumulated node targets into `syncer.SyncView` values with the
   correct callbacks.
4. Attach callbacks that remove Dropped state machines only after their final
   persistence succeeds.
5. Move the pending effects into one immutable `dirtyViewEvent` keyed by the
   manager's `ShardID`.
6. Clear the manager fields without retaining or reusing the event's backing
   arrays.

No Catalog or ReliableSyncer call is performed while `m.mu` is held.

### 4.4 Keyed Concurrent Batch Flush

The scheduler merges pending events per `ShardID`. Persist effects are latest-win
per versioned QueryView key; sync effects are latest-win per QueryView key and
WorkNode key. It packs ready, non-inflight shard lanes according to the configured
maximum ETCD transaction operation count. For each claimed batch it performs:

1. Flatten all `persists` and call `catalog.SaveQueryViews` once.
2. After persistence succeeds, run the batch's post-persist callbacks, including
   durable removal of Dropped state machines.
3. Group every `syncer.SyncView` by
   `WorkNodeKey`.
4. Call `syncer.SyncViews` once for the grouped node syncs.

The ordering is local to each packed batch: all included QueryView states are
persisted before any included node sync is dispatched. Different tasks contain
disjoint shard lanes and may execute concurrently.

If new work for an inflight shard arrives during I/O, it remains pending until
the task completes and is then eligible for a successor task. Unrelated shard
work can be claimed by another task immediately.

`Begin()` opens an explicit batching window without stopping existing tasks.
Events submitted within nested windows are held from new dispatch. The outermost
idempotent `Commit()` releases the held lanes and fans them out into as many
disjoint batch tasks as the configured batch size requires.

### 4.5 Sync Routing

The target state determines routing:

| Sync State | Route To |
|---|---|
| Preparing | SN + all QNs |
| Up | SN only |
| Down | SN only |
| Dropped | SN + all QNs |

### 4.6 Callback Model

Each accepted sync registers callbacks:

- **OnSyncResponse**: Looks up the state machine by version, applies
  `OnNodeStateReported`, processes in-memory cascading effects, publishes stats,
  submits the resulting shard event, and unlocks. It returns whether the current
  node-targeted sync has completed.
- **OnQueryNodeLost**: Registered only for QueryNode targets. It applies
  `OnQueryNodeLost`, processes the resulting state, publishes stats, submits
  the resulting shard event, and unlocks.
  In Preparing this makes the view Unrecoverable;
  in Dropping it treats the lost QueryNode cleanup as complete. The resulting
  shard event is submitted before unlocking to preserve transition order.

Callbacks for an already removed view stop tracking without creating new work.

### 4.7 AddPreparing

1. Validate the new DataVersion against all resident views.
2. Preempt an existing Preparing or Ready view by entering Unrecoverable.
3. Advance Unrecoverable views to Dropping so their Dropped sync can be batched
   with the replacement Preparing sync.
4. Assign `max(QueryVersion for the same DataVersion) + 1`, or 1 when the
   DataVersion is new.
5. Build and register the new state machine.
6. Update in-memory pointers and stats.
7. Emit and submit one shard event, then unlock.

### 4.8 RequestRelease

- Mark the manager as explicitly released. Ordinary QueryView cleanup does not
  make an empty manager eligible for registry removal.
- Preparing or Ready views enter Unrecoverable.
- Up views enter Down.
- All Unrecoverable views advance to Dropping.
- The manager publishes the new stats, emits and submits one shard event, then
  unlocks.
- If the manager is already empty, notify the registry after unlocking so it
  can remove this released manager immediately.

Cleanup continues asynchronously through reliable node callbacks.

## 5. Recovery and Shutdown

During recovery, persisted views are grouped by `ShardID` and reconstructed as
state machines. Recovered Preparing and Down views create pending sync effects.
Before committing the Begin/Commit window, the Registry installs manager
observers and builds its collection/node indexes, so immediate recovery
callbacks cannot be lost. It then waits for the Scheduler to become idle before
recovery completes.

On shutdown, the owner closes the Registry and its flush scheduler, then closes
`ReliableSyncer`. This prevents a flush task
from submitting new sync work after the syncer has closed.

## 6. Thread Safety

- `ShardViewManager.mu` protects its state machines, fast pointers, and atomic
  event creation.
- `DirtyViewFlushScheduler.mu` protects pending events, inflight and held shard
  lanes, queued task accounting, terminal error, and closed state.
- No ETCD, RPC, task execution, or callback runs while a manager lock is held;
  only the scheduler's non-blocking in-memory `Submit` runs under that lock.
- No Catalog or ReliableSyncer I/O runs while the Scheduler lock is held.
- The shared `NodeScheduler` queue is unbounded and non-blocking, so submitting
  an event does not wait for a batch task to execute.

## 7. Invariants

1. **Preemption**: At most one non-draining Preparing or Ready view exists per
   shard.
2. **Max Views**: The total active-view limit is still a separate TODO; a
   preempted draining view may temporarily coexist with its replacement.
3. **DataVersion Rollback Prevention**: A new Preparing view cannot have a lower
   DataVersion than any resident view.
4. **QueryVersion Assignment**: QueryVersion is one greater than the maximum for
   the same DataVersion, or 1 for a new DataVersion.
5. **Write-Ahead Persistence**: A packed flush persists every included state
   before dispatching any included node sync.
6. **Latest-State Coalescing**: Multiple unflushed transitions may skip
   intermediate external states, but retain the latest pending persist and sync
   effects independently.
7. **Dirty-State Preservation**: Work created for an inflight shard is processed
   by a successor task after that shard lane is released.
8. **Up-then-Down**: When a new view reaches Up, any older Up view immediately
   enters Down.
9. **Deferred Dropping**: Unrecoverable remains stable until replacement or
   release logic advances it to Dropping.
10. **Dropped Persistence**: A Dropped state machine is removed only after its
    final ETCD deletion has been persisted successfully.
11. **Shard-Lane Serialization**: Old and new QueryView versions of one
    `ShardID` cannot be flushed by concurrent tasks.
12. **Cross-Shard Parallelism**: Different `ShardID` lanes may execute in
    different NodeScheduler tasks concurrently.
13. **Registry Cleanup**: Only `RequestRelease` makes a manager eligible for
    registry removal. After the released manager's last QueryView completes
    durable removal, the registry removes that exact empty manager, its stats,
    and its collection/node reverse-index entries. The manager owns the release
    and emptiness preconditions; the registry only rechecks manager identity
    before deletion.

## 8. Package Location

```text
internal/views/coord/coordview/
    dirty_view_flush_scheduler.go       # Keyed event aggregation and batch tasks
    dirty_view_flush_scheduler_test.go  # Begin/Commit, batching, lane concurrency
    shard_view_registry.go        # Registry and scheduler lifecycle owner
    shard_view_manager.go         # Per-shard in-memory orchestration
    state_machine.go              # Per-view lifecycle and pending effects
    syncer/reliable_syncer.go     # Reliable node delivery
    shard_view_manager_test.go    # Manager lifecycle tests
```
