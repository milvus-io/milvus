# Message Workflow

This document describes how RecoveryStorage consumes WAL messages and routes
work into VChannel-owned persistence components. Lifetime and checkpoint
semantics are defined by [WAL Message Ack Design](message_ack.md).

## 1. Common Data Flow

Observation is serialized per PChannel:

```text
raw message M
  -> O = Tracker.Track(M)
  -> D = O.Clone()
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> VChannelRecoveryModule observes D synchronously
       -> Segment/TransformLog clone D for actual async work
       -> QueryRuntime receives plain ImmutableMessage
  -> D.Release()
  -> RecoveryStorage advances MetaPoint from M
  -> BroadcastAck.Accept(O)
```

For non-broadcast messages, BroadcastAck releases `O` immediately. For
broadcast messages it registers a one-shot exclusive callback on `O`. The
callback only marks the task ready and nonblockingly wakes one module background
dispatcher. The dispatcher preserves observation order for conflicting
ResourceKeys and may Ack non-conflicting tasks concurrently. It releases `O`
only after Coordinator Ack succeeds. The Tracker finalizer then clears the
entry's message and advances the continuous completed checkpoint prefix.

The Tracker point is:

```text
Point.MessageID = M.LastConfirmedMessageID()
Point.TimeTick  = M.TimeTick()
```

Recovery resumes with `DeliverPolicyStartFrom`. Replaying a completed message
is safe and conservative; skipping an incomplete one is not.

## 2. Meta-Only Flow

Bounded metadata recovery uses the same module input shape without persistence
tracking:

```text
M -> temporary Owner O -> D = O.Clone()
  -> PChannelRecoveryManager.ObserveMessage(D)
  -> D.Release() -> O.Release()
  -> update MetaPoint
```

Meta-only messages do not enter Tracker or BroadcastAck. The bounded scan
rebuilds metadata and transaction state, and the resulting DirtySnapshots are
persisted before its checkpoint.

## 3. Ownership Table

| Holder | Creation | Release condition |
|---|---|---|
| Data Owner | `Tracker.Track` | BroadcastAck releases immediately for ordinary messages or after Coordinator Ack for broadcasts |
| Dispatch Retained | `Owner.Clone` | RecoveryStorage releases after synchronous manager dispatch |
| Segment Retained | Segment sees concrete async work | Object-storage/lifecycle work succeeds after metadata is marked dirty |
| TransformLog Retained | Delete/Txn/Delete or flush barrier needs chunk work | Covering TransformLog chunk is durable and metadata is dirty |
| Tracker entry message | `Tracker.Track` | Owner finalizer clears it at reference count zero |
| QueryRuntime event | Plain `ImmutableMessage` | QueryRuntime's own queue lifecycle; outside RecoveryStorage Ack |

An entry can remain in the ordered Tracker queue after its message pointer is
cleared. The point is still needed to remove the continuous completed prefix.

## 4. Typical Messages

### TimeTick

No persistence consumer clones the dispatch Retained. A non-broadcast message
therefore releases its Owner immediately and the Tracker advances.

### CreateCollection

The VChannel updates collection metadata and marks it dirty. A broadcast copy
is processed independently by each target VChannel. BroadcastAck waits for
the Owner's exclusive callback, then performs Coordinator Ack when no earlier
unfinished task conflicts by ResourceKey and releases the Owner.

### Insert

Each affected SegmentView clones the Retained dispatch handle and stores the
clone with its pending L1 pack. The clone releases only after the shared object
chunk is durable, metadata is installed, and the SegmentView is dirty.

### Delete

TransformLog appends one transform entry and retains the outer message until
the covering TransformLog chunk is durable. L0 materialization is independent
and does not delay Ack.

### Flush-Style Messages

Flush, ManualFlush, FlushAll, DropCollection, DropPartition,
TruncateCollection, schema-changing AlterCollection, and AlterWAL may trigger
several Segment flushes and a TransformLog chunk flush. Each async unit clones
the same Retained dispatch handle. A broadcast version is acknowledged only
after all those clones are released and the Owner's exclusive callback fires.

### Txn

A committed Txn is one immutable WAL message. Each affected SegmentView or
TransformLog retains the outer Txn handle; children returned by `RangeOver`
never receive independent ownership.

## 5. Checkpoint Batch

RecoveryStorage freezes the boundary before collecting dirty snapshots:

```text
MetaPoint = latest observed WAL point
DataPoint = min(MetaPoint, Tracker continuous completed point)

freeze points
  -> consume stable DirtySnapshots
  -> persist snapshots
  -> MarkPersisted
  -> persist WALCheckpoint last
```

This ordering guarantees that a checkpoint covering a completed message also
covers the metadata installed before its last Retained release. TransformLog
completion means chunk durability, not materialization.

## 6. Invariants

1. Every data message has one Owner and one Tracker entry.
2. Every async consumer receives an independent Retained clone before dispatch
   returns.
3. Tracker stores points and message references, not BroadcastAck state.
4. The one-shot Owner exclusive callback is the BroadcastAck readiness signal;
   refcount zero is Tracker finalization.
5. Meta-only messages use a temporary Owner and do not affect DataPoint.
6. QueryRuntime uses plain immutable messages and its own TimeTick filtering.
7. Ack observes persistence completion but does not define Segment or
   TransformLog task execution order.
8. Broadcast Ack uses ResourceKeys only to order conflicting Coordinator Acks;
   non-conflicting tasks may run concurrently.
