# Broadcaster

Executes cross-PChannel atomic broadcast for DDL/DCL messages with resource locking, ACK tracking, and callback execution. Singleton running inside StreamingCoord.

## Broadcast API

The agreed [schema-handoff refactor](../../../design-docs/design_docs/qviews/schema_evolution.md)
uses the durable ALTER message and ordered ACK callback as the target-schema
source for DataCoord and QueryView planning. Its in-process target projection
is new work, not an existing historical-schema API. Completed task cleanup is
not a permanent schema store; all new GC coordination is deferred in that design.

Callers use `broadcast.StartBroadcastWithResourceKeys(ctx, resourceKeys...)` to obtain a `BroadcastAPI`, which acquires resource key locks and returns after WAL-based DDL is ready. The caller then constructs a `BroadcastMutableMessage` with its data VChannels (`WithBroadcast`), or with none (`WithControlChannelBroadcast`) for a CChannel-only broadcast, and calls `Broadcast()`. The broadcaster adds the CChannel to the header of every broadcast, so callers never pass it. `Close()` releases locks if no broadcast was issued.

The ordinary broadcast entrance rejects non-primary clusters with `ErrNotPrimary`. For local operations, `broadcast.StartUnreplicableBroadcastWithResourceKeys`: it is accepted on any replicate role, and its message must carry the `Unreplicable` (`_ur`) property, so it is written to the local WAL and never replicated (used for resource group DDL and local Flush/FlushAll). ForcePromote has a separate secondary-only entrance. See [Local Flush Control](../wal/local-flush.md) for abort-and-replay semantics.

## Broadcast Flow

1. **Lock**: Acquire ResourceKey locks in sorted order (Domain, then Key). SharedCluster is added automatically.
2. **Persist**: Stamp BroadcastID, ResourceKeys and the CChannel into the broadcast header, create task in PENDING state, persist to catalog. Once persisted, the broadcast is guaranteed to eventually complete even across crashes.
3. **Append**: `broadcastScheduler` dispatches the task to a worker that calls `AppendMessages()` to write to all target PChannels. Local Flush aborts incomplete receiving transactions instead of waiting for their source Commit, so ForcePromote uses the existing worker pool.
4. **FastAck**: If `AckSyncUp` is not set, the broadcaster immediately self-acks all VChannels using the append results (no need to wait for consumer-side ACK). Otherwise, waits for StreamingNode consumers to ACK each VChannel.
5. **AckCallback**: CChannel ACK enqueues the task into `ackCallbackScheduler`; a broadcast without CChannel is enqueued when all target VChannels have ACKed. The callback executes only after all VChannels are ACKed. For tasks with conflicting ResourceKeys, callbacks execute in CChannel TimeTick order. Callbacks retry with exponential backoff until success.
6. **Tombstone & GC**: After callbacks complete, task transitions to TOMBSTONE. `tombstoneScheduler` removes checkpoint-eligible tasks when their TTL expires or their count exceeds `maxCount`, evicting smaller BroadcastIDs first for count pressure (see below).

Both `ackCallbackScheduler` and `tombstoneScheduler` use unbounded
`typeutil.MultipartQueue` inboxes. A short mutex protects enqueue and queue
exchange; task inspection, resource locking, callbacks, and GC run outside that
mutex. ACK enqueue and callback completion send coalesced, nonblocking wakeups.
The ACK loop drains its inbox before each scheduling pass, appending new tasks
after existing blocked tasks to preserve FIFO scheduling for conflicting keys.
Tombstone GC remains timer-driven.

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

The index lives and dies with the task entry. Tombstones are eligible for deletion
only after every participating WAL checkpoint has passed the broadcast.
`streaming.walBroadcaster.tombstone.maxLifetime` (24h by default) is the age
threshold for deleting eligible tombstones. `maxCount` (8192 by default) bounds
the remaining eligible tombstones and can shorten their retention by evicting
smaller BroadcastIDs. Recovery resets the in-memory age clock, while checkpoint
lag can extend retention indefinitely. Subsystems returning retained IDs
(currently BulkImport) must account for this extension in their own metadata
retention; `maxLifetime` is neither a minimum nor an upper bound on the
idempotency window.

Replicated tasks are indexed too: the query path is unreachable on a secondary (`WithResourceKeys` rejects non-primary clusters), and indexing there lets a promoted secondary honor pre-failover keys.

## Tombstone GC: TTL, eligible count, and persisted WAL checkpoints

Issue [#54425](https://github.com/milvus-io/milvus/issues/54425) exposes a gap
between task completion and WAL recovery. A callback may finish while an earlier
unfinished message or Summary publication holds the durable WAL checkpoint back.
Deleting the tombstone by age or count alone then allows replayed replicated ACKs
to recreate the task, rebuild a partial ACK bitmap, or schedule its callback again.

Apply the same GC rule to **all** tombstones: primary broadcasts, replicated
broadcasts on a secondary, and local unreplicable broadcasts. For tombstone `T`:

```text
checkpointEligible(T) = for every VChannel v in T's broadcast header:
    persistedCheckpoint[ToPhysicalChannel(v)].TimeTick
        > T.AckedCheckpoints[v].TimeTick

1. Delete checkpoint-eligible tombstones whose age >= maxLifetime.
2. If the remaining checkpoint-eligible count exceeds maxCount,
   delete the smallest BroadcastIDs until that count is at most maxCount.
```

TimeTick is the PChannel's LSN. Use each receiving channel's local broadcast
TimeTick, including CChannel; neither the source replication TimeTick nor
MessageID ordering participates in this comparison. When several VChannels
share a PChannel, its checkpoint must pass every corresponding broadcast copy.
Equality is insufficient. Missing checkpoints and missing/zero legacy ACK
positions retain the tombstone because they cannot prove eligibility.
Legacy tombstones without ACK positions remain permanently retained; advancing
WAL checkpoints cannot make their missing positions known. They continue to
participate in periodic GC checks, with the associated catalog-read cost.

GC runs on `checkInternal` ticks, rather than on each newly enqueued tombstone.
Completed callbacks enqueue into an unbounded `typeutil.MultipartQueue` protected
by a short mutex. Enqueue records the age timestamp while holding that mutex,
preserving FIFO age order across concurrent callbacks. At each GC tick, the
scheduler swaps out the pending queue under the mutex, then merges and processes
it outside the mutex. New arrivals stay in the new queue until the next tick.
Neither checkpoint reads nor tombstone deletion hold the queue mutex, so slow GC
cannot make callbacks retain their resource-key locks while waiting to enqueue.
The queue has no capacity backpressure; it can grow when GC falls behind.
Shutdown does not wait for it to drain: tasks were already persisted as TOMBSTONE
before enqueue and are reconstructed during coordinator recovery.

Before catalog access, the scheduler checks its in-memory queue: an empty queue,
or a queue whose total count is at most `maxCount` and whose oldest enqueue time
has not reached the TTL, needs no GC. Either TTL expiry or total count pressure
triggers checkpoint collection; total count is only a cheap precheck, not the
eligible quota. Retained entries preserve enqueue order for this age check.
Each triggered GC pass first lists all PChannels from the
StreamingCoord catalog, then reads each persisted `WALCheckpoint` through
`StreamingNodeCataLog.GetConsumeCheckpoint`. StreamingCoord constructs that
catalog with the same cluster MetaKV root used by StreamingNode. This requires
no StreamingNode RPC, new metadata key, protobuf field, or catalog interface.
The pass collects the checkpoint map before deleting any task and discards it
afterward. A read failure aborts the pass without deleting tombstones. A channel
missing from the collected map blocks only tombstones involving that channel.
No manager/task lock is held during catalog reads. Under monotonic WAL checkpoint
publication, advancing checkpoints during the pass only makes this map conservative;
a cross-channel atomic snapshot is unnecessary.

The checkpoint gate applies to both age and count eviction. Blocked tombstones
do not count toward `maxCount`, so the total retained count can exceed that limit.
Expired eligible tombstones leave the quota before count eviction is selected,
avoiding unnecessary eviction of younger tombstones. Count eviction orders by
BroadcastID, not callback completion or enqueue time. A blocked tombstone
does not prevent eligible, unrelated tombstones from being collected. Failed
catalog deletion retains the entry for retry. Task and idempotency-index removal
continue through the existing DONE transition.

The existing retention clock starts when the scheduler receives the persisted
TOMBSTONE. It is not stored in the task proto: coordinator recovery starts a new
TTL clock. Count eviction still applies immediately after recovery. This avoids
a format change while allowing age-based retention to extend across restarts.
StreamingNode restart/handoff before checkpoint advancement therefore still sees
the retained tombstone when it replays partial or complete sets of ACKs. Existing
ACK lookup suppresses those messages.

The physical LastConfirmedMessageID in a WAL checkpoint can precede broadcasts
already covered by its TimeTick. During startup, `runBoundedRecovery` therefore
observes only scanner output whose local TimeTick is strictly greater than the
recovered checkpoint's TimeTick. The filter runs after ordering and transaction
assembly, preserving old transaction fragments needed by later commits. This
prevents the overlapping completed prefix from generating new broadcast ACKs
after tombstone GC. RecoveryBarrier recognition and the retained live scanner
remain unchanged; ordinary readers are unaffected.

When migrating legacy recovery metadata, the inclusive DataCoord seek timestamp
must not be treated as a completed WAL prefix. The final migrated V2 checkpoint
keeps the safe physical start but sets its global TimeTick to zero; replay then
establishes the actual completed prefix. Until the persisted checkpoint advances,
this GC gate retains tombstones. Component and control frontiers are preserved.

The checkpoint proves completion of the consuming ACK needed to advance that
prefix. It does not drain all historical RPC attempts. Retaining eligible
tombstones additionally protects against delayed duplicate requests, including
server-side ACK handlers that survive client cancellation. This protection lasts
until TTL expiry or eligible-count eviction; TTL is not a guaranteed minimum.
This is a bounded operational protection,
not an exactly-once guarantee against arbitrarily delayed requests. Epoch fencing,
append deduplication, and changes to checkpoint ownership
publication are outside this fix. The rule assumes recovery uses the published
checkpoint and that its logical position does not regress across ownership changes.

Regression coverage must check TTL expiry and count pressure independently,
smallest-ID eviction with out-of-order enqueue, exclusion of blocked tombstones
from the quota, TTL deletion before count eviction, zero-limit behavior,
skipped catalog access when neither age nor count triggers GC,
concurrent enqueue during queue exchange, callback resource-lock release while GC
catalog I/O is stalled, enqueue-time TTL preservation, shutdown with pending entries,
checkpoint behind/equal/ahead, per-channel local timestamps, missing/legacy
positions, catalog read/delete failures, partial/all-channel ACK replay,
coordinator recovery and checkpoint term changes during handoff. New replicated
broadcasts must still be recoverable through the existing ACK path. Startup
replay must suppress ACKs at or before the recovered checkpoint while preserving
broadcasts and committed transactions beyond it and the transition to live replay.

## Import Completion

Import, CommitImport, and RollbackImport include CChannel alongside the business
VChannels so replicated callbacks share an ordered copy. CommitImport uses
FastAck and completes in the DataCoord callback: persist Committing to protect
against timeout, update segment visibility with each business VChannel's own
commit TimeTick, synchronously publish the resulting DataView, then persist
Completed. QueryView readiness is waited by Load/Refresh, not this callback.
Failures keep the broadcast task
retryable. New Import jobs persist `commit_by_coordinator=true`; no StreamingNode
per-channel RPC or L0 materialization is required for these jobs. Absent/false
flags retain legacy completion: BroadcastAckModule calls HandleCommitVchannel
before Ack on each business VChannel, and the checker completes the job after
all channel commits. The RPC is a no-op for new jobs when sent by old nodes.
Both the RPC and the callback treat a job that importMeta no longer holds as
already committed and garbage collected, except on a CDC secondary, where the
replicated CommitImport can precede the local job and the caller keeps waiting.
A secondary that already garbage collected the job waits forever. A GC tombstone
(#54425) is the follow-up that closes this case.
See [Import commit ownership](../../../design-docs/design_docs/wal/broadcast_ack_module.md#8-import-commit-ownership).

## Resource Key Locking

Each ResourceKey has: **Domain** (resource type), **Key** (entity identifier), **Shared** (read vs exclusive). Every broadcast automatically acquires SharedCluster.

Domains: `Cluster`, `DBName`, `CollectionName`, `Privilege`, `SnapshotName`.

ForcePromote retains ExclusiveCluster in the same resource-lock domain as other
broadcasts. Local Flush does not wait for source transactions, so promotion needs
no separate admission lock or worker. See [Local Flush Control](../wal/local-flush.md).

Reuse these keys for local API concurrency: the collection key serializes local
Flushes on the same collection, and ExclusiveCluster excludes FlushAll/ForcePromote
from other local broadcasts. No extra FIFO order is promised between independent
collections. Replicated DDL bypasses these entrance locks, so cross-cluster races
still require WAL lifecycle ordering and consuming ACKs; adding another local
admission lock cannot substitute for that protocol.

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

DataCoord Flush uses the unreplicable broadcast entrance and broadcasts ManualFlush with `_ur` and AckSyncUp to the collection's business
VChannels under shared DB and exclusive collection-name locks. The broadcaster
automatically includes CChannel; its copy has no data to flush. The business-channel Ack
waits for both L1 and L0 completion, without waiting for global recovery checkpoint
publication. Flush returns an empty pending segment list and preserves the existing
flushed-segment listing. See [Flush API completion](../../../design-docs/design_docs/wal/broadcast_ack_module.md#flush-api-completion).
