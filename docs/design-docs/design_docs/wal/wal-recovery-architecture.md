# WAL Recovery Architecture

RecoveryStorage restores and persists the WAL-derived state owned by one
StreamingNode PChannel. Its persistence scope is object storage plus recovery
metadata in etcd. QueryView and QueryRuntime consume recovered state, but do not
participate in the checkpoint protocol.

This document is the entry point for the WAL recovery design. Detailed rules
are split by responsibility:

- [Checkpoint And Snapshot Persistence](checkpoint-persistence.md)
- [Message Workflow](message-workflow.md)
- [WAL Message Ack Design](message_ack.md)
- [Recovery Tail Controller](recovery-tail-controller.md)
- [VChannel Recovery Module](vchannel_view_module.md)
- [Segment View Component](segment_view_module.md)
- [TransformLog Design](transformlog/transform_log.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md)

## 1. Goals

RecoveryStorage has three responsibilities:

1. replay WAL messages into recoverable VChannel-owned state;
2. publish one global checkpoint whose preceding WAL prefix is fully durable;
3. bound the logical bytes between the published checkpoint and the WAL tail.

Data layout is not a RecoveryStorage responsibility. Forced persistence may
create small or scattered objects. A future log Defrag subsystem will coalesce
those objects without changing the recovery checkpoint protocol.

## 2. Runtime Components

```text
RecoveryStorage
  +-- CheckpointPublisher
  +-- messageack.Tracker
  +-- RecoveryTailController
  +-- PChannelRecoveryManager
  |     +-- PChannelControlView
  |     +-- VChannelRecoveryModule*
  |           +-- VChannelView
  |           +-- SegmentView*
  |           +-- TransformLog
  +-- BroadcastAck
```

There is no generic top-level recovery-module interface. The PChannel manager,
BroadcastAck, SegmentView, and TransformLog keep separate APIs because their
ownership and completion conditions differ.

## 3. One Global Checkpoint

A PChannel has exactly one global recovery checkpoint:

```text
Checkpoint = largest continuous WAL prefix for which every message owner
             has completed and whose component snapshots have been published
```

The checkpoint contains only the WAL position:

- `MessageID`, using the message's `LastConfirmedMessageID`;
- `TimeTick`, using the message's unique PChannel-order TimeTick.

PChannel control state such as replication configuration and AlterWAL state is
stored in a component snapshot with its own `checkpoint_time_tick`. It is not
embedded as an independently advancing field in the global checkpoint.

The checkpoint is the only:

- WAL replay start position;
- WAL truncation position;
- externally reported recovery progress;
- starting point for recovery-tail byte accounting.

There is no Meta checkpoint, Data checkpoint, DataBarrier, or recovery mode.

## 4. Component Snapshot Checkpoints

Component snapshots may be ahead of the global checkpoint because work for
later messages can complete while an earlier message remains blocked. Every
component snapshot therefore records a `checkpoint_time_tick`.

This is a component snapshot checkpoint, not another WAL checkpoint:

- it never starts a scanner;
- it never truncates WAL;
- it does not divide recovery into phases;
- it is only a replay-idempotency boundary for that component.

A component `checkpoint_time_tick` is a continuous prefix of messages relevant
to that component. A component must not advance it from 100 to 102 while its
work for 101 is incomplete, even if the work for 102 completed first.

## 5. Recovery Startup

RW startup is one logical replay:

```text
load checkpoint and component snapshots
  -> append RecoveryBarrier to fence the old writer
  -> open one scanner from the checkpoint
  -> observe every message once with complete semantics
  -> reach RecoveryBarrier and publish the recovered write-path snapshot
  -> continue the same observation stream into the live WAL tail
```

RecoveryBarrier is only a writer fence and startup catch-up marker. It never
switches observation behavior.

If the scanner API requires a bounded scanner followed by a live scanner, the
two ranges must be adjacent and non-overlapping. That implementation still
represents one logical replay and must not dispatch any message twice.

RO startup uses a stable readable WAL frontier instead of appending a barrier.

Reaching the barrier proves that all startup WAL messages have been observed.
It does not require every asynchronous object write to finish. Pending work
continues to retain message handles and prevents the checkpoint from passing it.

## 6. Message Flow

```text
raw WAL message M
  -> Owner O = Tracker.Track(M)
  -> dispatch Retained D = O.Clone()
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> PChannel/VChannel metadata
       -> affected SegmentViews
       -> TransformLog
       -> QueryRuntime plain immutable event
  -> D.Release()
  -> BroadcastAck.Accept(O)
  -> all retained work and Coordinator Ack finish
  -> Tracker advances its continuous completed prefix
```

Each component sees every relevant message through one complete Observe path.
There are no `MetaOnly`, `DataOnly`, or `MetaAndData` modes.

## 7. Persistence

For a frozen completed prefix, RecoveryStorage:

1. consumes stable dirty component snapshots;
2. writes component deltas to catalog;
3. writes the global checkpoint last as the commit marker;
4. marks the snapshots published;
5. truncates WAL through the published checkpoint.

Component snapshots may include state beyond the frozen WAL checkpoint. Their
`checkpoint_time_tick` fields make replay from an older WAL checkpoint
idempotent if a crash happens before the checkpoint commit.

## 8. Recovery Tail Control

The primary pressure signal is:

```text
recovery_tail_bytes = observed_tail_offset - published_checkpoint_offset
```

The controller asks the AckTracker for VChannels blocking the oldest incomplete
prefix and requests persistence through a target TimeTick. SegmentView and
TransformLog own batching decisions. RecoveryStorage does not aggregate objects
across segments.

Background persistence gives a soft target. A strict upper bound requires WAL
append backpressure at a high watermark and release at a low watermark.

## 9. Branch Baseline

All RecoveryStorage changes on this branch are unpublished. The implementation
directly removes the dual-checkpoint fields, observation modes, aliases, and
migration adapters introduced by this branch. No intermediate format on this
feature branch receives a reader, writer, migration path, or fallback.

## 10. Correctness Invariants

1. WAL replay is the source of truth for all state after the global checkpoint.
2. A message is dispatched once and has one Tracker Owner.
3. Every asynchronous consumer owns an independent Retained handle.
4. A handle releases only after its concrete recoverability condition succeeds.
5. Tracker advancement uses only the continuous completed WAL prefix.
6. Component `checkpoint_time_tick` fields are continuous component-local prefixes.
7. Dirty component snapshots are written before the global checkpoint.
8. WAL truncation never passes the published global checkpoint.
9. RecoveryBarrier is not a checkpoint or observation-mode boundary.
10. QueryRuntime does not participate in persistence acknowledgement.
