# Shard Management

The Shard interceptor manages per-PChannel collection/partition/segment in-memory metadata and assigns each incoming DML message to a growing segment. All state is purely in-memory, should always keep consistent with underlying-WAL; on WAL open, it is recovered from the [RecoveryStorage](recovery-storage.md) snapshot.

See [Collection Messages](../message/message-semantic-collection.md) for per-message semantics and the messages handled by this interceptor.

## Seal Policies

Growing segments are sealed when any registered policy triggers. See `internal/streamingnode/server/wal/interceptors/shard/policy/seal_policy.go` for the full list of policies.

## VChannel Registration

The manager's registration map is keyed by **collection id**, one entry per collection per PChannel, and the entry names the VChannel it describes. Every operation that consults it must match on that name, not just the collection id: a fenced source's slot is freed immediately, so a later VChannel of the same collection can hold the entry, and an operation that ignored the name would answer for, fence, or tear down the wrong shard.

Registration admission is enforced here rather than assumed of the caller:

- `CheckIfVChannelCanBeCreated` — `ErrCollectionExists` for an idempotent replay of the same VChannel; `ErrVChannelConflict` when another VChannel of the collection holds the entry, which the interceptor turns into a rejected append. Consulted by the **target replica** of a SplitShard broadcast, which registers a new VChannel the way CreateCollection does. Refusing rather than warning is deliberate: the newcomer would otherwise skip its own registration and inherit the incumbent's state — including a fenced source's fence, leaving the new shard permanently unwritable.
- `CheckIfVChannelCanBeWritten` — `nil` only when this PChannel holds that exact VChannel and it is live; `ErrVChannelFenced` when the name has a fence tombstone (the write's route is one routing commit behind, so it becomes `SHARD_FENCED` and the proxy refreshes and retries); `ErrCollectionNotFound` otherwise, which is terminal.

## Split Fence Tombstones

`SplitShard`'s **source replica** fences the VChannel and tears its registration down in **one** critical section — a reader must never see the registration gone without the tombstone in its place:

- a `SplitFence{TimeTick, TaskID}` is recorded in `fencedVChannels`, keyed by VChannel **name**. That tombstone is the only place a fence is ever recorded, since the registration it would otherwise live in is gone. It is what answers a stale proxy route, and what `GetSplitFence` returns to a re-sent fence.
- the registration is removed and every partition manager of the collection flushed and dropped. Nothing on the source needs it: no DML follows the fence, and the growing segments were sealed by `FlushAndFenceSegmentAllocUntil` while the message was being built (this message *is* the seal record; there is no separate ManualFlush). Freeing the slot here is what lets a successor VChannel of the same collection be registered on this PChannel without waiting for a routing commit.

A second fence record of the **same** task raises the tombstone's TimeTick instead of being ignored: `T_switch` is the tick of a task's *latest* fence record, every fence record of one task seals the same data (the VChannel took no DML in between), and the later tick is the one the coordinator recorded. A fence record of **another** task — including one whose task id reads zero — is refused on the append path with `SHARD_FENCED` carrying the recorded tick and task id; it must never move a fence it did not place. The task id is what lets a caller tell its own retry from a fence some other split placed.

On WAL open the map is rebuilt from the RecoveryStorage snapshot: every VChannel the snapshot reports as **SPLITTED** seeds a tombstone (`split_time_tick`, `split_task_id`) and is *skipped* when building the registration map, so a restart lands on exactly the state the fence left — including when a live successor already holds the collection's slot.

## Key Packages

- `internal/streamingnode/server/wal/interceptors/shard/` — Shard interceptor, `ShardManager`, seal policies, segment stats
