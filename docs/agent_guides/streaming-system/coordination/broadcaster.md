# Broadcaster

Executes cross-PChannel atomic broadcast for DDL/DCL messages with resource locking, ACK tracking, and callback execution. Singleton running inside StreamingCoord.

## Broadcast API

Callers use `broadcast.StartBroadcastWithResourceKeys(ctx, resourceKeys...)` to obtain a `BroadcastAPI`, which acquires resource key locks and returns after WAL-based DDL is ready. The caller then constructs a `BroadcastMutableMessage` with target VChannels (must include CChannel) and calls `Broadcast()`. `Close()` releases locks if no broadcast was issued.

Non-primary clusters reject all broadcasts with `ErrNotPrimary`.

## Broadcast Flow

1. **Lock**: Acquire ResourceKey locks in sorted order (Domain, then Key). SharedCluster is added automatically.
2. **Persist**: Allocate BroadcastID, create task in PENDING state, persist to catalog. Once persisted, the broadcast is guaranteed to eventually complete even across crashes.
3. **Append**: `broadcastScheduler` dispatches the task to a worker that calls `AppendMessages()` to write to all target PChannels.
4. **FastAck**: If `AckSyncUp` is not set, the broadcaster immediately self-acks all VChannels using the append results (no need to wait for consumer-side ACK). Otherwise, waits for StreamingNode consumers to ACK each VChannel.
5. **AckCallback**: CChannel ACK enqueues the task into `ackCallbackScheduler`. The callback executes only after all VChannels are ACKed. For tasks with conflicting ResourceKeys, callbacks execute in CChannel TimeTick order. Callbacks retry with exponential backoff until success.
6. **Tombstone & GC**: After callbacks complete, task transitions to TOMBSTONE. `tombstoneScheduler` garbage-collects aged-out tasks from the catalog.

## Idempotent Broadcast

A broadcast message carrying the `_ik` idempotency key property is additionally indexed by that key. A later broadcast presenting the same key short-circuits: it creates no task and returns the ORIGINAL broadcast's message in `BroadcastAppendResult.Duplicated`, leaving `AppendResults` nil. The lookup happens inside `broadcasterWithRK.Broadcast`, which only exists after the resource keys are held — so two concurrent same-key requests are serialized by the resource lock rather than both missing.

The index lives and dies with the task entry, so **the idempotency window a client observes equals the tombstone retention**: `maxLifetime` or `maxCount`, whichever comes first. The count bound is hard — a busy cluster can evict tombstones well before `maxLifetime`, ending the window early. Any subsystem that advertises this guarantee (currently BulkImport) must keep its own retention at least as long as `maxLifetime`, or an in-window retry can resolve to an ID its own metadata has already GC'd.

Replicated tasks are indexed too: the query path is unreachable on a secondary (`WithResourceKeys` rejects non-primary clusters), and indexing there lets a promoted secondary honor pre-failover keys.

## Resource Key Locking

Each ResourceKey has: **Domain** (resource type), **Key** (entity identifier), **Shared** (read vs exclusive). Every broadcast automatically acquires SharedCluster.

Domains: `Cluster`, `DBName`, `CollectionName`, `Privilege`, `SnapshotName`.

See [Message Semantic Docs](../message/message.md) for per-message ResourceKey usage.

## BroadcastTask State Machine

```
PENDING → TOMBSTONE → DONE (removed from catalog)
REPLICATED → TOMBSTONE → DONE (removed from catalog)
```

- **PENDING**: Created, awaiting WAL append and ACK. After append, FastAck self-acks all VChannels immediately (unless `AckSyncUp` is set, in which case waits for consumer-side ACK).
- **REPLICATED**: Task created on secondary cluster from replicated ImmutableMessage (no resource lock held). Execution order guaranteed by CChannel TimeTick ordering in `ackCallbackScheduler`.
- **TOMBSTONE**: All ACK callbacks complete, resource locks released. Awaiting GC.
- **DONE**: Removed from catalog.

## Key Packages

- `internal/streamingcoord/server/broadcaster/` — `Broadcaster`, task scheduling, resource locking, ACK callbacks, singleton accessor
