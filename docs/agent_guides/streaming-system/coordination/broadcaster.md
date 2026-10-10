# Broadcaster

Executes cross-PChannel atomic broadcast for DDL/DCL messages with resource locking, ACK tracking, and callback execution. Singleton running inside StreamingCoord.

## Broadcast API

Callers use `broadcast.StartBroadcastWithResourceKeys(ctx, resourceKeys...)` to obtain a `BroadcastAPI`, which acquires resource key locks and returns after WAL-based DDL is ready. The caller then constructs a `BroadcastMutableMessage` with its data VChannels (`WithBroadcast`), or with none (`WithControlChannelBroadcast`) for a CChannel-only broadcast, and calls `Broadcast()`. The broadcaster adds the CChannel to the header of every broadcast, so callers never pass it. `Close()` releases locks if no broadcast was issued.

Non-primary clusters reject all broadcasts with `ErrNotPrimary`. The exception is `broadcast.StartUnreplicableBroadcastWithResourceKeys`: it is accepted on any replicate role, and its message must carry the `Unreplicable` (`_ur`) property, so it is written to the local WAL and never replicated (used for resource group DDL, which describes this cluster's own nodes).

## Broadcast Flow

1. **Lock**: Acquire ResourceKey locks in sorted order (Domain, then Key). SharedCluster is added automatically.
2. **Persist**: Stamp BroadcastID, ResourceKeys and the CChannel into the broadcast header, create task in PENDING state, persist to catalog. Once persisted, the broadcast is guaranteed to eventually complete even across crashes.
3. **Append**: `broadcastScheduler` dispatches the task to a worker that calls `AppendMessages()` to write to all target PChannels.
4. **FastAck**: If `AckSyncUp` is not set, the broadcaster immediately self-acks all VChannels using the append results (no need to wait for consumer-side ACK). Otherwise, waits for StreamingNode consumers to ACK each VChannel.
5. **AckCallback**: CChannel ACK enqueues the task into `ackCallbackScheduler`; a broadcast without CChannel is enqueued when all target VChannels have ACKed. The callback executes only after all VChannels are ACKed. For tasks with conflicting ResourceKeys, callbacks execute in CChannel TimeTick order. Callbacks retry with exponential backoff until success.
6. **Tombstone & GC**: After callbacks complete, task transitions to TOMBSTONE. `tombstoneScheduler` garbage-collects aged-out tasks from the catalog.

## Idempotent Broadcast

A broadcast message carrying the `_ik` idempotency key property is additionally indexed by that key. A later broadcast presenting the same key short-circuits: it creates no task, **waits for the original broadcast's ack callback to complete**, and returns the ORIGINAL broadcast's result, with the original message in `BroadcastAppendResult.Duplicated`. The lookup and the registration are one critical section on the manager's lock, so two concurrent same-key requests cannot both miss regardless of which resource keys they hold. The wait is what makes the duplicate answer equivalent to a fresh one: the original is not always serialized in front of the retry by a resource lock — a retry that raced a rename holds the stale name's lock, and a task recovered from a replicated WAL holds no lock at all — and without the wait such a retry would be answered with a broadcastID whose effects (for import, the job created in the ack callback) do not exist yet. After the wait, `AppendResults` is rebuilt from the original's persisted per-vchannel checkpoints and is never nil, so a caller that only reads append results cannot tell a duplicate from a fresh broadcast. The wait is bounded by the request context; a caller whose context expires gets its own timeout, not an unbacked ID.

**The scope is an identity chosen by the caller, not the broadcast's lock keys.** It is `(messageType, the scope the caller bound the key to)`, where the scope is built by one of `message.New{Cluster,Database,Collection}ScopedIdempotencyKey` and carries an object **ID**. `messageType` is added by the broadcaster, because CreateIndex and DropIndex on one collection would otherwise share a scope and the second would be silently swallowed. The scope itself comes from the caller, because only the caller knows what its operation acts on.

Both halves of the name-vs-identity problem an earlier design had are closed by this:

- **Rename.** `RenameCollection` keeps the collectionID and changes the name (and can move the collection to another DB). The scope is the ID, so the key stays bound to that collection and a retry naming the renamed collection still resolves to its original broadcast. This is a statement about the scope, not about stale names: an entry point that resolves a name to an id — `importTask.PreExecute` does, through the proxy meta cache — rejects a retry still carrying the old name before it reaches the broadcaster at all. That request fails; it does not import twice.
- **Drop and recreate under the same name.** The recreated collection has a new ID, so the scope differs and the lookup MISSES: a fresh task is created, which is the correct outcome — the two requests target different collections. Import still compares the decoded `collectionID` on a hit, but as an invariant check against an encoding or scoping bug, not as a semantic guard.

There is no unscoped key: `WithIdempotencyKey` takes an `IdempotencyKey`, which only the scoped constructors produce, so a caller cannot ship a key that silently deduplicates cluster-wide by omission. Choosing `NewClusterScopedIdempotencyKey` is a decision that reads as one.

**Caller obligations:** choose the correct identity scope and the resource locks needed for business validation. Deduplication itself does not depend on those locks: lookup and registration share `broadcastTaskManager.mu`. A rename or different lock key therefore cannot allow two same-key requests to register simultaneously.

The ordinary broadcast path runs no lock-before-build admission check of its own, so **everything a caller validates runs before the lookup**. A caller enforcing a limit that its own original request is still counted against will therefore reject that request's retry, and the retry cannot recover the original `broadcastID` -- import's `dataCoord.import.maxImportJobNum` is exactly such a limit. The contract for a client is to retry the same key once the limit frees up; minting a fresh key on the rejection is what duplicates the work.

**The one case that rule does not cover is a failed original.** The duplicate branch resolves a key to the original ID without consulting that job's state, so if the original ended `Failed`, every retry under the same key returns that same failed ID for the rest of the window and the client never makes progress. That is ordinary idempotency semantics -- the key names an attempt that did happen -- but it is the one situation where a fresh key is the correct move rather than the duplicating one. A client that generalizes the rule above will spin instead. `ImportV2` logs the original job's state on every dedup hit so an operator can tell a key stuck this way from one waiting on a healthy job.

The index lives and dies with the task entry, so **the idempotency window a client observes equals the tombstone retention**: `maxLifetime` or `maxCount`, whichever comes first. The count bound is hard — a busy cluster can evict tombstones well before `maxLifetime`, ending the window early. Any subsystem that advertises this guarantee (currently BulkImport) must keep its own retention at least as long as `maxLifetime`, or an in-window retry can resolve to an ID its own metadata has already GC'd. Matching the two exactly is not enough: `tombstoneScheduler.Initialize` stamps every recovered tombstone with `time.Now()`, so a tombstone's age is measured from the last StreamingCoord start and each restart extends its remaining life, while the subsystem's own retention keeps counting from the original event. Leave margin.

Replicated tasks are indexed too: the query path is unreachable on a secondary (`WithResourceKeys` rejects non-primary clusters), and indexing there lets a promoted secondary honor pre-failover keys.

## Explicit Broadcast Transactions (API only)

`broadcast.StartTxnBroadcastWithResourceKey(ctx, keys...)` returns either a locked
`TxnBroadcaster` or the original Begin's `TxnBroadcastResult`. Supply at most one
`message.NewIdempotencyResourceKey(operation, scopedClientKey)`. Start acquires
that key's X lock first, checks the index, and acquires business keys only on a
miss. The complete identity includes the operation, so the final MessageType can
still be chosen under the business locks. Ordinary broadcast APIs reject this
new key domain and transaction headers; their existing `_ik` behavior is unchanged.

The new admission identity is persisted in `_bik`, separate from legacy `_ik`.
It is not a business ResourceKey in the durable header. Admission X remains held
through initial durable registration, then releases; duplicates wait only for
Begin's callback, without requesting business locks or waiting for Commit.
Business integrations must explicitly handle migration from old `_ik` scopes.

Construct Begin under the returned locks. Read `header := msg.BroadcastHeader()`,
set `header.Txn = &messagespb.BroadcastTxnContext{TxnId: id}`, and call
`msg.OverwriteBroadcastHeader(header)`. This single method replaces all known
header fields in place, including Txn; `BroadcastBegin` fills Kind/Sequence.
Admission identity remains a separate `_bik` property set through
`OverwriteBroadcastAdmissionKey`. Preparation copies properties once to isolate
background tasks from caller changes. `RecoverTxnBroadcast(ctx, id)` returns a
handle to the same unfinished internal controller without acquiring resources.
A durable Begin tombstone rejects recovery before GC as well as after restart.
`BroadcastBody` and `BroadcastCommit` fill its TxnID and serialize across handles.
All members must use the same ResourceKeys (including Shared/Exclusive modes)
and VChannel set as Begin. Omitted member ResourceKeys are inherited; explicitly
supplied keys must match. Channel comparison ignores order and includes the
automatically added CChannel. Admission validates this before deduplication;
replicated ACKs and recovery enforce the same durable invariant.
Bodies can use `_ik` for group-local retries. The first terminal is selected once;
concurrent Commit calls wait for that original result without comparing messages.
After durable completion, existing handles reject Body/Commit as well. There is
no separate cleanup option. The business still chooses commit or rollback messages.

Each member waits for its ACK policy and callback. Begin becomes `TXN_INFLIGHT`
and retains business locks. Body completes without releasing them. Commit saves
Begin and Commit as TOMBSTONE in one catalog KV transaction, then releases resources.
Request cancellation only stops waiting after admission. Close before Begin
releases both admission and business locks; after admission it closes the handle.

Each message uses the original `BroadcastTask` key, indexed by BroadcastID.
All task persistence uses `SaveBroadcastTasks`: ordinary updates pass one task,
terminal completion passes Begin and Commit, and whole-group GC passes all members.
Each call uses a single `MultiSaveAndRemove` that must never be split into batches. Recovery reads `ListBroadcastTask`, groups transaction
members by Header.TxnID and orders them by Sequence, then restores one set of
business locks per open group on the primary. Secondary replay has no such long locks. Open groups never enter GC; a closed group
occupies one retention unit, and all its task keys are deleted atomically.
An old handle cannot recreate a GC'd group. Persistence uses the same single
active coordinator and reliable write lifecycle as ordinary broadcast tasks;
there is no separate group record or revision/CAS protocol.

Primary operation supports configured replicas. Replicated ACKs reconstruct the
same transaction from its existing task records without acquiring primary business
locks. The existing CChannel admission order is retained; a member's callback also
waits for its predecessor's durable completion, including when resources are Shared.
Data-channel ACKs may arrive before Begin and are persisted without executing the
member early. Waiting conflicting callbacks cannot be bypassed by later readers.

Normal switchover's Cluster X waits for open source transactions to complete.
Force promotion fences replication and supplements missing channel copies of known
members through the existing broadcast scheduler. It drains callbacks (not just ACKs),
then transfers open transactions' business locks before opening public admission.
A cancellable admission gate covers the transfer from Cluster X to the groups;
recovery reinstates the gate for an unfinished local promotion. A wholly missing
member prevents promotion from opening admission; no Begin or terminal is fabricated.
Commit/rollback remains a business decision. TxnID must remain unique across the
replication topology, including promotion, and must not be reused.

All members must be replicable business messages; unreplicable messages and
replication configuration changes cannot be transaction members. No Import or other
business path uses this API yet. Upgrade both coordinators before enabling callers;
old coordinators do not understand TXN_INFLIGHT or atomic group GC.

Transactions are bounded: at most 64 members including Begin and Commit, each
message at most 256 KiB and 128 channels including CChannel. A member slot is reserved for Commit so the whole group stays within one atomic
GC transaction. Each task is saved independently; there is no aggregate snapshot
byte limit or rewrite. Begin/Body payloads are retained until group GC. Resource
lock acquisition retains the existing blocking behavior without context-aware
interruption; callers must Close unused handles. New transaction IDs must never
be reused.

## Import Completion

Import, CommitImport, and RollbackImport include CChannel alongside the business
VChannels so replicated callbacks share an ordered copy. CommitImport uses
FastAck and completes in the DataCoord callback: persist Committing to protect
against timeout, update segment visibility with each business VChannel's own
commit TimeTick, then persist Completed. Failures keep the broadcast task
retryable. New Import jobs persist `commit_by_coordinator=true`; no StreamingNode
per-channel RPC or L0 materialization is required for these jobs. Absent/false
flags retain legacy completion: BroadcastAckModule calls HandleCommitVchannel
before Ack on each business VChannel, and the checker completes the job after
all channel commits. The RPC is a no-op for new jobs when sent by old nodes.
See [Import commit ownership](../../../design-docs/design_docs/wal/broadcast_ack_module.md#8-import-commit-ownership).

## Resource Key Locking

Each ResourceKey has: **Domain** (resource type), **Key** (entity identifier), **Shared** (read vs exclusive). Every broadcast automatically acquires SharedCluster.

Domains: `Cluster`, `DBName`, `CollectionName`, `Privilege`, `SnapshotName`, and transaction-only admission `Idempotency` (processed separately, first).

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

## Collection Flush Completion

DataCoord Flush broadcasts ManualFlush with AckSyncUp to the collection's business
VChannels under shared DB and exclusive collection-name locks. The broadcaster
automatically includes CChannel; its copy has no data to flush. The business-channel Ack
waits for both L1 and L0 completion, without waiting for global recovery checkpoint
publication. Flush returns an empty pending segment list and preserves the existing
flushed-segment listing. See [Flush API completion](../../../design-docs/design_docs/wal/broadcast_ack_module.md#flush-api-completion).
