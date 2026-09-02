# WAL Recovery Architecture

RecoveryStorage restores and persists the WAL-derived state needed by a
StreamingNode PChannel. Its scope is WAL persistence into object storage and
metadata publication into etcd; QueryView and QueryRuntime are consumers of
the recovered state, not owners of this checkpoint protocol.

## 1. Runtime Components

```text
RecoveryStorage
  +-- metadata recovery and checkpoint batch
  +-- messageack.Tracker
  +-- PChannelRecoveryManager
  |     +-- VChannelRecoveryModule
  |           +-- VChannelView
  |           +-- SegmentView*
  |           +-- TransformLog
  +-- BroadcastAck
```

There is no generic RecoveryStorage Module interface. The PChannel manager and
BroadcastAck have separate APIs because they have different responsibilities.
SegmentView and TransformLog are VChannel-owned components. Their historical
snapshot names remain catalog classifications only.

## 2. Recovery Startup

1. Load the persisted WAL checkpoint and recovery metadata.
2. Append the persisted RecoveryBarrier to prove writer ownership and fence an
   old WAL writer where the backend requires it.
3. Run the bounded Meta-only scan through the RecoveryBarrier. This rebuilds
   VChannel metadata, schemas, segment assignments, TransformLog metadata, and
   the uncommitted TxnBuffer without claiming that data work has been replayed.
4. Consume and persist the resulting DirtySnapshots as required by the bounded
   recovery flow.
5. Switch VChannel components into MetaAndData mode.
6. Start the DataScanner from the persisted Data checkpoint using
   `DeliverPolicyStartFrom`.
7. Build QueryRuntime WALViews from VChannel-owned state without waiting for the
   DataScanner to catch up to the startup RecoveryBarrier.

RecoveryBarrier is a WAL writer-fencing and bounded metadata-scan endpoint. It
is not a DataBarrier and it is not a QueryRuntime readiness fence.

## 3. Data Message Flow

```text
DataScanner message M
  -> Tracker.Track(M) = Owner O
  -> O.Clone() = dispatch Retained D
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> VChannel state updates
       -> Segment/TransformLog clone D for actual async work
       -> QueryRuntime observes plain ImmutableMessage
  -> D.Release()
  -> update Meta checkpoint
  -> BroadcastAck.Accept(O)
```

The Tracker finalizer clears the message pointer for that entry only when the
Owner graph reaches reference count zero. It then advances the continuous
checkpoint prefix. A later completed message is allowed to release its payload
while an earlier message remains incomplete.

BroadcastAck releases ordinary Owners immediately. For broadcast Owners it
registers a one-shot exclusive callback. The callback marks the task ready and
nonblockingly wakes one background dispatcher. The dispatcher calls Coordinator
Ack after all earlier conflicting ResourceKey tasks finish, while independent
tasks may run concurrently. It releases the Owner only after Ack success. Thus
reference count zero means local persistence consumers and broadcast Ack have
both completed for a broadcast message.

## 4. Persistence And Checkpoints

RecoveryStorage freezes `MetaPoint` and the Tracker continuous `DataPoint`,
consumes stable dirty snapshots, persists all snapshots, marks them persisted,
and writes the WALCheckpoint last. Segment and TransformLog consumers mark
metadata dirty before releasing their Retained handles.

The WAL checkpoint represents the last message that has been consumed by the
required local persistence path. Its MessageID is the message's
`LastConfirmedMessageID`; replay from that ID is conservative and idempotent.
`AckSyncUp` only controls whether Coordinator may FastAck; it does not add a
checkpoint or ordering dependency inside RecoveryStorage.

## 5. QueryRuntime Boundary

VChannelRecoveryModule builds a no-gap WALView and sends QueryRuntime the
underlying immutable message for live events. QueryRuntime maintains its own
TimeTick filtering and ordering. It does not retain RecoveryStorage handles,
participate in DataPoint advancement, or delay BroadcastAck.

WALView capture waits for the actual VChannel conditions required by the
snapshot, including resolving flushed segments that lack `SealedAtDataVersion`.
It does not wait for a global DataScanner barrier. Multiple VChannels may
advance DataVersions independently; segment selection uses each segment's own
`SealedAtDataVersion` against the target QueryView DataVersion.

## 6. Correctness Invariants

1. WAL replay is the source of truth; an unfinished message is never skipped.
2. Tracker entries are ordered by observed WAL messages and their points are
   monotonic.
3. Per-entry message pointers are cleared at finalization, independent of
   ordered-prefix removal.
4. Async persistence units own explicit Retained handles and release only after
   their success condition.
5. Segment flush completion is object/lifecycle durability, not QueryRuntime
   readiness or L0 materialization.
6. TransformLog completion is chunk durability, not materialization.
7. DirtySnapshots precede the WALCheckpoint in every persist batch.
8. BroadcastAck waits for the Owner's exclusive callback and earlier
   conflicting ResourceKey tasks, but not checkpoint persistence.
9. Meta-only processing uses a temporary Owner and does not create DataPoint
   entries.
10. QueryRuntime's ordinary Go message reachability is independent of message
    persistence tracking.
