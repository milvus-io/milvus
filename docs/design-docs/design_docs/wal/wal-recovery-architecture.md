# WAL Recovery Architecture

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

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
- [TransformLog Design](transform_log.md)
- [WALSummary](summary.md)
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
  +-- background persistence task (publishes the global checkpoint)
  +-- messageack.Tracker
  +-- WALSummary (chunks, manifests, independent backlog and LastAcked)
  +-- RecoveryTailController
  +-- PChannelRecoveryManager
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
Candidate = min_by_TimeTick(Tracker.CompletedPoint(), WALSummary.LastAcked())
Checkpoint = published candidate with all required component snapshots durable
```

The checkpoint's WAL position consists of:

- `MessageID`, using the message's `LastConfirmedMessageID`;
- `TimeTick`, using the message's unique PChannel-order TimeTick.

PChannel control state such as replication configuration and AlterWAL state
is embedded in the checkpoint itself (fields `replicate_config`,
`replicate_checkpoint`, `alter_wal_state`) and advances atomically with it:
the checkpoint is the single source of truth for the control state after a
crash, and a control-only change rewrites the checkpoint. Matching control state
to a candidate pinned behind observation remains an open implementation point;
see [checkpoint control state](checkpoint-persistence.md#7-pchannel-control-state).

The checkpoint is the only:

- WAL replay start position;
- WAL truncation position;
- published recovery progress returned by `GetCheckpoint`;
- starting point for recovery-tail byte accounting.

There is no Meta checkpoint, Data checkpoint, DataBarrier, or recovery mode.
`Metrics().RecoveryTimeTick` currently reports Tracker completion, which can be
ahead of Summary confirmation and published progress. The compatibility
DataCoord VChannel checkpoint updater reports flush progress, not a second WAL
recovery cursor.

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
append RecoveryBarrier to fence the old writer
  -> load and claim checkpoint with the assignment term
  -> load component snapshots and restore summary/transform windows
  -> open bounded scanner from the checkpoint
  -> observe messages with complete semantics
  -> reach RecoveryBarrier and publish the recovered write-path snapshot
  -> start live scanner and background persistence/progress checks
```

RecoveryBarrier is only a writer fence and startup catch-up marker. It never
switches observation behavior.

If the scanner API requires a bounded scanner followed by a live scanner, the
two ranges must be adjacent and non-overlapping. That implementation still
represents one logical replay and must not dispatch any message twice.

The current adaptor uses an inclusive `StartFrom` at the barrier MessageID for
live scanning, after the bounded scanner has delivered the barrier. Boundary
deduplication and transaction-buffer handoff still need verification; the
non-overlap rule above is a target contract, not an established implementation
guarantee.

The current RO opener only constructs the read-only adaptor and does not run
RecoveryStorage initialization. Recovery against a stable readable WAL frontier
is a future RO design, not the current startup behavior.

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
  -> WALSummary.ObserveMessage(M) copies records without retaining handles
  -> D.Release()
  -> BroadcastAck.Accept(O)
  -> all retained work and Coordinator Ack succeed without poison
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

AckTracker requests persistence from VChannels blocking the oldest incomplete
prefix. Summary independently checks staged-record age and tail pressure, so
already released messages do not hide its backlog. SegmentView and Summary own
their batching decisions; TransformLog materializes independently under its L1
safety bound. RecoveryStorage does not aggregate objects across segments.

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
3. Async Segment consumers own independent Retained handles; Summary and TransformLog copy records.
4. Successful release requires recoverability; poisoned release frees memory but leaves checkpoint progress blocked.
5. Tracker advancement uses only the continuous completed WAL prefix.
6. Component `checkpoint_time_tick` fields are continuous component-local prefixes.
7. Dirty component snapshots are written before the global checkpoint.
8. WAL truncation never passes the published global checkpoint.
9. RecoveryBarrier is not a checkpoint or observation-mode boundary.
10. QueryRuntime does not participate in persistence acknowledgement.
11. Summary confirmation independently bounds every published checkpoint.
