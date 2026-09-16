# L0 Materializer Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** Implemented in `vchannel/l0materializer`, using WALSummary's shared
bounded reader. This document owns all L0 materialization behavior.
[TransformLog](transform_log.md) remains a separate future subscription adaptor.

## 1. Ownership

`l0materializer` continuously converts one VChannel's Delete records from
[WALSummary](summary.md) into DataCoord-managed L0 segments. It is always active,
including when no external subscription exists, and has no enable switch.

```text
RecoveryStorage
  +-- WALSummary (PChannel record storage)
  +-- PChannelRecoveryManager
        +-- VChannelRecoveryModule
              +-- VChannelView
              +-- SegmentView*
              +-- L0Materializer -> Summary bounded reads -> L0 / DataCoord
```

It is an independent VChannel-owned component, not a new top-level recovery
module. It depends on a narrow Summary read interface, the L0 writer/registrar,
and the shared scheduler. It has no dependency on TransformLog, its streams, or
RPCs, and owns no separate catalog or record storage.

The VChannel module derives the L1 safety bound and aggregates materialization
progress into `VChannelMeta`. The existing field
`transform_materialized_time_tick` remains the durable L0 frontier; changing the
component name does not change the field number or require a new metadata key.

## 2. Window State

The runtime keeps three positions:

| Position | Meaning |
|---|---|
| M: materializedThrough | All Delete records through M have successfully completed L0 output and registration. |
| W: requestedThrough | The newest materialization boundary accepted by this component's ordered observation path. |
| L: upperBound | The inclusive L1 safety bound supplied by the VChannel owner. |

The outstanding window is `(M, W]`; one task targets `min(W, L)`.
The persisted copy of M can lag the running value and is tracked by normal
VChannel snapshot bookkeeping. W and L are runtime state, not additional
persistent checkpoints or WAL replay positions.

There is no pending-entry list, retained source handle, recovery-loaded payload
window, or `loadedThrough`. Window bookkeeping is constant-sized; only the
current bounded read/materialization batch holds Delete payloads.

## 3. ObserveMessage

Observation classifies each valid, ordered, routed WAL message:

| Kind | Message | Window effect |
|---|---|---|
| DeleteEntry | Delete, or committed Txn containing Delete | Advance W to the outer message TimeTick. |
| None | Insert, or nonempty committed Txn containing only Inserts | No change. |
| BarrierEntry | Every other message, including empty or mixed non-Delete Txn | Advance W to the message TimeTick. |

Delete takes precedence inside a Txn. ObserveMessage performs no I/O, copies no
payload, and retains no message handle. It monotonically merges the requested
boundary and ensures background work is scheduled when progress is possible.
BarrierEntry is a classification, not an allocated queue entry or stored record.

PChannel-level messages, including persisted TimeTicks and RecoveryBarrier,
reach every affected VChannel. Non-persisted heartbeats remain filtered by
RecoveryStorage. Messages belonging only to another VChannel do not advance W.

Observation order is part of the integration contract:

```text
Summary.ObserveMessage
  -> install records and complete readable coverage in memory
VChannel.ObserveMessage
  -> update VChannel / Segment state and L1 upper bound
  -> L0Materializer.ObserveMessage advances W last
```

This is an in-memory ordering requirement, not a Summary flush requirement.
Summary must account for coverage of payload-free messages, including
RecoveryBarrier, before exposing their materialization boundary. A background
task must never interpret not-yet-observed data as an empty interval.

## 4. L1 Safety Bound

The owner derives:

```text
L = min(create_segment_time_tick of every Segment with l1_commit_done = false)
```

With no blocker, L is unbounded. The creation TimeTick is safe to include
because rows assigned to that Segment have later TimeTicks. A newly observed
CreateSegment installs its blocker before observation advances W.

Tasks cannot read beyond W even if Summary restored newer history. This prevents
materialization from passing Segment state not yet reconstructed by replay.
A later CreateSegment has a TimeTick beyond an already observed task target;
it cannot invalidate a correctly captured older interval.

An L1 final commit or lifecycle change recomputes L and independently wakes
materialization. Progress must not depend on another WAL message arriving.

## 5. Read And Materialize

Each VChannel executes materialization batches serially:

1. Capture `target = min(W, L)`. If target is at or before M, wait for progress.
2. Ask Summary for Delete records in `(M, target]`, bounded by rows/bytes.
3. Use the returned `CoveredThrough`, not the requested target, as this batch's
   possible commit position.
4. Group Deletes by partition/PK representation, write L0 deltalogs, and
   register all resulting output with DataCoord.
5. Only after the entire batch succeeds, advance M through its covered range,
   update VChannelMeta through the owner callback, and mark the snapshot dirty.
6. Re-evaluate the latest W and L and schedule a continuation if work remains.

The reader covers durable chunks, sealed records, and the pending tail.
Materialization does not wait for Summary object persistence or manifest
publication. A Summary read failure or output/registration failure leaves M
unchanged and retries without skipping data.

An Entry, including a committed Txn's Delete children, is the smallest cursor
unit. A batch may exceed its soft row/byte limit for one oversized Entry; it
must not commit an Entry's TimeTick after processing only part of that Entry.
Physical L0 output may be split, but the batch frontier advances only after all
required outputs are registered.

A proven empty interval can advance M without producing an empty L0. An
incomplete read cannot. A read with no coverage progress must wait for the
relevant change or return an error, never spawn an endless continuation chain.
Task completion and new-window/L1-bound updates must be coordinated so a wakeup
arriving during task completion cannot strand work.

## 6. Persistence And GC

The publication sequence is:

```text
L0 output durable and DataCoord registration successful
  -> advance in-memory M
  -> owner updates VChannelMeta.transform_materialized_time_tick and marks dirty
  -> RecoveryStorage persists the captured VChannel snapshot
  -> report that snapshot's M to Summary as this consumer's release position
```

An in-memory M alone cannot authorize GC. Both full and base-only VChannel
snapshots must report their captured frontier after successful persistence.
Callbacks cannot substitute a newer in-memory value. Durable lifecycle cleanup
may provide an equivalent release position when no retained recovery/serving
state still needs the records.

Summary owns actual chunk retention and deletion. Future subscription consumers
add their own history requirements; this consumer's release position is not
permission to override those requirements.

L0 materialization retains no source WAL handles, does not delay BroadcastAck,
and does not independently gate the global recovery checkpoint. Summary's
recoverable confirmation protects Delete durability while materialization lags.

## 7. Recovery And Close

After restoring Summary and VChannel/Segment metadata:

1. Set M from the durable VChannel materialization field and initialize W = M.
2. Derive L from the restored Segment state before scheduling work.
3. Replay WAL once from the global checkpoint using the same Observe path as
   live consumption; Delete/Barrier messages advance W monotonically.
4. Route the startup RecoveryBarrier to each VChannel still requiring
   materialization. This ensures old Summary backlog is discovered even when
   no new Delete arrives.
5. Read the outstanding range lazily and continue normal background work.

For example, M=50 and global checkpoint=200 may coexist with an unmaterialized
Delete@100 already stored in Summary. WAL replay need not deliver that Delete
again: RecoveryBarrier@250 requests a window through 250, and the bounded reader
finds Delete@100 in `(50,250]`, subject to L. No startup payload preload is needed.
Do not initialize W from Summary's largest position, which may be ahead of
VChannel replay. Restored M may itself be ahead of the global checkpoint;
older observations never move it backward or request already completed work.

Closing cancels reads/tasks without inventing progress or requiring a final
materialization. Restart reconstructs the pending window from durable M and
ordered replay. If L0 registration succeeds but the updated VChannel metadata
is lost in a crash, the batch may be repeated. This design does not promise
physical exactly-once output; output idempotency/reconciliation is separate.

## 8. Invariants And Validation

- Summary is the only owner of Delete record storage.
- ObserveMessage records boundaries only, after Summary and Segment observation.
- A task never commits beyond W, L, or its complete read coverage.
- Per-VChannel batches advance through a continuous prefix of complete entries.
- Payload memory is bounded by active batches, not unmaterialized history.
- Successful L0 registration precedes M advancement; durable M precedes GC release.
- Materialization runs without subscribers and does not depend on TransformLog.

Validation must cover storage transitions during reads, empty windows,
row/byte-capped Txns, L1-bound release without new messages, observation/task
completion races, recovery with Summary ahead of replay, pre-checkpoint Delete
backlog, Barrier-only startup, and crashes between output registration and
metadata publication.
