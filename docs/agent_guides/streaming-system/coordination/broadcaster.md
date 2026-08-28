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

A broadcast message carrying the `_ik` idempotency key property is additionally indexed by that key. A later broadcast presenting the same key short-circuits: it creates no task, **waits for the original broadcast's ack callback to complete**, and returns the ORIGINAL broadcast's result, with the original message in `BroadcastAppendResult.Duplicated`. The lookup and the registration are one critical section on the manager's lock, so two concurrent same-key requests cannot both miss regardless of which resource keys they hold. The wait is what makes the duplicate answer equivalent to a fresh one: the original is not always serialized in front of the retry by a resource lock — a retry that raced a rename holds the stale name's lock, and a task recovered from a replicated WAL holds no lock at all — and without the wait such a retry would be answered with a broadcastID whose effects (for import, the job created in the ack callback) do not exist yet. After the wait, `AppendResults` is rebuilt from the original's persisted per-vchannel checkpoints and is never nil, so a caller that only reads append results cannot tell a duplicate from a fresh broadcast. The wait is bounded by the request context; a caller whose context expires gets its own timeout, not an unbacked ID.

**The scope is an identity chosen by the caller, not the broadcast's lock keys.** It is `(messageType, the scope the caller bound the key to)`, where the scope is built by one of `message.New{Cluster,Database,Collection}ScopedIdempotencyKey` and carries an object **ID**. `messageType` is added by the broadcaster, because CreateIndex and DropIndex on one collection would otherwise share a scope and the second would be silently swallowed. The scope itself comes from the caller, because only the caller knows what its operation acts on.

Both halves of the name-vs-identity problem an earlier design had are closed by this:

- **Rename.** `RenameCollection` keeps the collectionID and changes the name (and can move the collection to another DB). The scope is the ID, so the key stays bound to that collection and a retry naming the renamed collection still resolves to its original broadcast. This is a statement about the scope, not about stale names: an entry point that resolves a name to an id — `importTask.PreExecute` does, through the proxy meta cache — rejects a retry still carrying the old name before it reaches the broadcaster at all. That request fails; it does not import twice.
- **Drop and recreate under the same name.** The recreated collection has a new ID, so the scope differs and the lookup MISSES: a fresh task is created, which is the correct outcome — the two requests target different collections. Import still compares the decoded `collectionID` on a hit, but as an invariant check against an encoding or scoping bug, not as a semantic guard.

There is no unscoped key: `WithIdempotencyKey` takes an `IdempotencyKey`, which only the scoped constructors produce, so a caller cannot ship a key that silently deduplicates cluster-wide by omission. Choosing `NewClusterScopedIdempotencyKey` is a decision that reads as one.

**What the caller now owes, in exchange:** the scope axis and the lock axis are no longer the same thing. The serialization guarantee above holds only if the exclusive lock the broadcast takes actually covers the object the key is scoped to. Import satisfies this (collection scope, `ExclusiveCollectionName` on that collection); an adopter that scopes to one object while locking another gets no serialization, and two concurrent same-key requests can both miss. This is a documented obligation, not an enforced one.

The broadcaster runs no admission check of its own, so **everything a caller validates runs before the lookup**. A caller enforcing a limit that its own original request is still counted against will therefore reject that request's retry, and the retry cannot recover the original `broadcastID` -- import's `dataCoord.import.maxImportJobNum` is exactly such a limit. The contract for a client is to retry the same key once the limit frees up; minting a fresh key on the rejection is what duplicates the work.

**The one case that rule does not cover is a failed original.** The duplicate branch resolves a key to the original ID without consulting that job's state, so if the original ended `Failed`, every retry under the same key returns that same failed ID for the rest of the window and the client never makes progress. That is ordinary idempotency semantics -- the key names an attempt that did happen -- but it is the one situation where a fresh key is the correct move rather than the duplicating one. A client that generalizes the rule above will spin instead. `ImportV2` logs the original job's state on every dedup hit so an operator can tell a key stuck this way from one waiting on a healthy job.

The index lives and dies with the task entry, so **the idempotency window a client observes equals the tombstone retention**: `maxLifetime` or `maxCount`, whichever comes first. The count bound is hard — a busy cluster can evict tombstones well before `maxLifetime`, ending the window early. Any subsystem that advertises this guarantee (currently BulkImport) must keep its own retention at least as long as `maxLifetime`, or an in-window retry can resolve to an ID its own metadata has already GC'd. Matching the two exactly is not enough: `tombstoneScheduler.Initialize` stamps every recovered tombstone with `time.Now()`, so a tombstone's age is measured from the last StreamingCoord start and each restart extends its remaining life, while the subsystem's own retention keeps counting from the original event. Leave margin.

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
