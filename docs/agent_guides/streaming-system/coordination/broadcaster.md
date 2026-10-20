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
6. **Tombstone & GC**: After callbacks complete, task transitions to TOMBSTONE. `tombstoneScheduler` removes a task only after its retention TTL expires and every participating PChannel's persisted WAL checkpoint has passed that channel's broadcast TimeTick (see below).

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

The index lives and dies with the task entry. Tombstones remain for at least
`streaming.walBroadcaster.tombstone.maxLifetime` (24h by default), and longer
when a participating WAL checkpoint has not passed the broadcast. `maxCount`
is a soft pressure threshold; it cannot shorten this retention. Recovery resets
the in-memory retention clock, so each restart extends the window. Subsystems
returning retained IDs (currently BulkImport) must account for this extension and
checkpoint lag in their own metadata retention; `maxLifetime` is no longer an
upper bound on the idempotency window.

Replicated tasks are indexed too: the query path is unreachable on a secondary (`WithResourceKeys` rejects non-primary clusters), and indexing there lets a promoted secondary honor pre-failover keys.

## Tombstone GC: TTL and persisted WAL checkpoints

Issue [#54425](https://github.com/milvus-io/milvus/issues/54425) exposes a gap
between task completion and WAL recovery. A callback may finish while an earlier
unfinished message or Summary publication holds the durable WAL checkpoint back.
Deleting the tombstone by age or count alone then allows replayed replicated ACKs
to recreate the task, rebuild a partial ACK bitmap, or schedule its callback again.

Apply the same GC rule to **all** tombstones: primary broadcasts, replicated
broadcasts on a secondary, and local unreplicable broadcasts. For tombstone `T`:

```text
age(T) >= streaming.walBroadcaster.tombstone.maxLifetime
AND, for every VChannel v in T's broadcast header:
    persistedCheckpoint[ToPhysicalChannel(v)].TimeTick
        > T.AckedCheckpoints[v].TimeTick
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
Each GC pass that has expired tombstones first lists all PChannels from the
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

Only expired tombstones are considered. `maxCount` remains a soft pressure
threshold for logging; it cannot bypass either prerequisite. A blocked tombstone
does not prevent eligible, unrelated tombstones from being collected. Failed
catalog deletion retains the entry for retry. Task and idempotency-index removal
continue through the existing DONE transition.

The existing retention clock starts when the scheduler receives the persisted
TOMBSTONE. It is not stored in the task proto: coordinator recovery starts a new
full TTL. This conservatively extends retention and avoids a format change.
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
prefix. It does not drain all historical RPC attempts. The TTL additionally
protects against delayed duplicate requests, including server-side ACK handlers
that survive client cancellation. This is a bounded operational protection,
not an exactly-once guarantee against arbitrarily delayed requests. Epoch fencing,
append deduplication, and changes to checkpoint ownership
publication are outside this fix. The rule assumes recovery uses the published
checkpoint and that its logical position does not regress across ownership changes.

Regression coverage must check TTL expiry and count pressure independently,
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
commit TimeTick, then persist Completed. Failures keep the broadcast task
retryable. New Import jobs persist `commit_by_coordinator=true`; no StreamingNode
per-channel RPC or L0 materialization is required for these jobs. Absent/false
flags retain legacy completion: BroadcastAckModule calls HandleCommitVchannel
before Ack on each business VChannel, and the checker completes the job after
all channel commits. The RPC is a no-op for new jobs when sent by old nodes.
See [Import commit ownership](../../../design-docs/design_docs/wal/broadcast_ack_module.md#8-import-commit-ownership).

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

## Collection Flush Completion

DataCoord Flush broadcasts ManualFlush with AckSyncUp to the collection's business
VChannels under shared DB and exclusive collection-name locks. The broadcaster
automatically includes CChannel; its copy has no data to flush. The business-channel Ack
waits for both L1 and L0 completion, without waiting for global recovery checkpoint
publication. Flush returns an empty pending segment list and preserves the existing
flushed-segment listing. See [Flush API completion](../../../design-docs/design_docs/wal/broadcast_ack_module.md#flush-api-completion).
