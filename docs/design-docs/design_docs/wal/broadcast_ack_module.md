# Broadcast Ack Module

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

`BroadcastAck` sends consuming-side acknowledgements for broadcast WAL messages
to StreamingCoord. It is a dedicated RecoveryStorage sink, not a data
persistence component.

The common lifetime contract is defined in
[WAL Message Ack Design](message_ack.md).

**Current runtime:** [WAL L0 Materializer](l0_materializer.md) retains Delete
handles for legacy query recovery. The [Summary consumer](summary_l0_materializer.md)
is retained for future QueryView wiring; the two implementations are not run together.

## 1. Ownership

```go
func (m *BroadcastAck) Accept(owner message.OwnedImmutableMessage)
```

`Accept` consumes top-level ownership:

- ordinary messages are released immediately;
- broadcast messages are queued in PChannel observation order;
- the caller must not clone or use the Owner after `Accept`.

## 2. Readiness Callback

BroadcastAck registers one exclusive callback. The callback fires when all
local Retained consumers have released and BroadcastAck is the only remaining
Owner holder. Explicit Flush/lifecycle requests include L0Materializer among
these consumers: readiness waits for L1 final commit, required L0 output, and
installation of dirty materialization metadata. The current WAL materializer also retains ordinary Delete handles through
output and registration. L1/L0 work on an explicit request joins independently.

For successful consumers the callback marks the task ready and nonblockingly
wakes the dispatcher; it performs no Coordinator I/O. If any consumer poisoned
the message, the callback releases the Owner to free payload memory and records
a poisoned task. That task cannot Ack and retains its ResourceKey ordering claim.

## 3. ResourceKey Ordering

Two tasks conflict when they share the same `(Domain, Key)` and at least one
side is exclusive. A task is schedulable when:

```text
exclusive callback fired
AND message is not poisoned
AND task is not in flight
AND no earlier unfinished task conflicts
```

Conflicting tasks preserve WAL observation order. Independent tasks may Ack
concurrently.

## 4. Ack And Retry

On success, BroadcastAck releases the Owner and unblocks later conflicting
tasks. On failure, it keeps the Owner and ResourceKey claim, waits for retry,
and does not block unrelated tasks.

Coordinator Ack is idempotent. A crash before global checkpoint publication may
replay and repeat it.

## 5. Recovery Tail Interaction

A stalled BroadcastAck holds the global continuous prefix but cannot be fixed
by Segment persistence. Coordinator failures use the Ack retry path; poisoned
local work remains incomplete and keeps the WAL available for replay. Explicit
Tracker blocker categories are not implemented yet, so a VChannel persist
request may still be issued for such an entry without resolving it.

## 6. Close

Close cancels dispatch and retry work. It does not release an unfinished Owner.
The message is replayed from the last published global checkpoint.

## 7. Invariants

1. `Accept` consumes the Owner exactly once.
2. A successful broadcast releases its Owner after Coordinator Ack; poisoned release never acknowledges success.
3. The readiness callback is one-shot and nonblocking.
4. Earlier conflicting tasks retain their ResourceKey claims through retry.
5. BroadcastAck has no component `checkpoint_time_tick`.
6. BroadcastAck does not wait for checkpoint catalog publication.

## Flush API Completion

FlushAll uses `AckSyncUp`: its cluster-level broadcast cannot FastAck on WAL
append. Every PChannel dispatches it to all affected VChannels and waits for
their L1 final commits and L0 output/registration before consuming-side Ack.
The RPC returns success only after all channel Acks and the broadcast callback
complete. `GetFlushAllState` is a follow-up completion endpoint and returns true
on a healthy server; it does not compare independently reported channel
checkpoints against a cross-channel maximum timestamp.

Global recovery checkpoint publication is not part of this RPC completion
boundary. A crash before publication replays unfinished recovery bookkeeping
and may repeat already durable output safely.

Collection Flush calls DataCoord's Flush RPC. DataCoord acquires the shared
DB and exclusive collection-name broadcast resource keys, then broadcasts a
ManualFlush with AckSyncUp to every collection VChannel. CChannel is omitted:
ManualFlush has no coordinator metadata callback requiring CChannel ordering.
The RPC waits for every split message's L1 final commits and L0 output/registration,
independently of unrelated VChannels holding the global recovery checkpoint.

Each VChannel uses its ManualFlush message TimeTick as the completion boundary.
No independent coordinator TSO or BarrierTimeTick is needed. The returned FlushTs
is zero: completion is already guaranteed by the successful RPC. GetFlushState
checks the supplied segment states, then returns true for zero FlushTs even if
channel checkpoints are absent. Nonzero FlushTs retains the existing channel
checkpoint checks. TimeOfSeal remains an informational wall-clock timestamp.

A successful streaming Flush returns an empty pending SegmentIDs list. Proxy
preserves the collection entry with an empty array. The existing flushed-segment
list is still collected from DataCoord metadata after completion, with the same
state and non-L0 filters. Channel checkpoints are captured before broadcasting,
as before; they are recovery positions, not proof of this Flush's completion.

## 8. Import Commit Ownership

Import, CommitImport, and RollbackImport broadcast to the business VChannels
plus CChannel. Business-channel append results supply their own commit fences;
CChannel supplies the common ordering point for replicated callbacks. These
messages do not request AckSyncUp: durable WAL append can FastAck the broadcast.

DataCoord's CommitImport callback owns the complete commit flow:

1. Wait for the job to reach Uncommitted. Persist Committing before changing
   segment visibility; a replay in Committing resumes the same callback.
2. For each business VChannel, set its imported segments' CommitTimestamp to
   that channel's append TimeTick and clear IsImporting. CChannel's timestamp
   and the maximum timestamp across channels must not replace this fence.
3. Persist Completed and completion time only after segment metadata succeeds.

Committing is a durable protection against timeout/cleanup during a partial
commit, not a wait for per-channel RPC acknowledgements. Failed writes return
errors to the broadcast callback scheduler. Its persisted task retries after
failure/restart; segment updates are idempotent and Completed replay is a no-op.
The segment and job writes are ordered, not one atomic transaction.

RecoveryStorage has no Import-specific RPC task, Flush request, or retained
completion handle. CommitImport is an ordinary Barrier for Summary/L0 window
observation; it does not force L1/L0 output. RecoveryStorage checkpoint progress
is independent of callback completion because the broadcast task owns recovery
of the coordinator-side effect. HandleCommitVchannel no longer mutates state.

This branch uses the existing segment-metadata serving path: MVCC already
tracks CommitImport's WAL position, and QueryNode uses CommitTimestamp for
import visibility and the delete replay boundary. qv's separate DataView
publication and Growing/Transforming MVCC frontiers are not introduced here.

Validation covers per-channel timestamps including a higher CChannel tick,
empty channels, readiness retries, segment/job persistence failures, callback
replay, timeout during a partial commit, and initial/commit/rollback routing.
