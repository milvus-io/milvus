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
- [L0 Materializer](l0_materializer.md)
- [TransformLog Subscription Adaptor](transform_log.md) (future integration)
- [WALSummary](summary.md)
- [Broadcast Ack Module](broadcast_ack_module.md)
- [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md)

The L0Materializer/shared-reader split is implemented without a copied payload
window. Integration status is tracked in
[Summary §7](summary.md#7-implementation-and-integration-status).
The revised capacity/API/Summary-backlog materialization policy is pending:
L0 has no age timer, and explicit API output waits for related L1 flush completion.
TransformLog subscription integration is deferred.

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
  |           +-- L0Materializer
  +-- BroadcastAck
```

There is no generic top-level recovery-module interface. The PChannel manager,
BroadcastAck, SegmentView, and L0Materializer keep separate APIs because their
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
`replicate_checkpoint`, `alter_wal_state`) and stored atomically with it.
Control may contain newer state than the global replay position, just like
Segment snapshots. Recovery must preserve or reconstruct the latest state and
make repeated control effects idempotent. The embedded
`control_checkpoint_time_tick` preserves Control's own applied frontier;
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
  -> load component snapshots and restore Summary plus L0 materialization cursors
  -> open one scanner from the checkpoint
  -> observe messages with complete semantics
  -> reach this open's RecoveryBarrier and capture the write-path snapshot
  -> pause raw input while initializing the write path
  -> resume the same scanner through WriteAheadBuffer
```

RecoveryBarrier is a writer fence and startup catch-up marker. It never
switches component observation behavior. Recovery and ordinary reads share the
same scanner constructor, ordering, transaction assembly, source switching, and
shutdown paths. Recovery supplies an optional startup boundary and the WAB
created by the opener; it does not create a separate kind of scanner.

The startup boundary matches the exact RecoveryBarrier MessageID and TimeTick.
Earlier barriers and TimeTicks cannot trigger the handoff. Before delivering the
boundary, the scanner takes an independent snapshot of unfinished transaction
builders, including their body slices. The live scanner retains its original
TxnBuffer, reorder buffer, and pending queue. A BeginTxn or transaction body
before the barrier can therefore be completed by CommitTxn or RollbackTxn after
it; live transaction assembly cannot mutate the write-path startup snapshot.

After write-path initialization, the scanner reads WAB exclusively after the
barrier TimeTick. The WAB is seeded by that exact barrier, so this also works
when it contains no later messages: creating the reader needs no additional
persisted TimeTick. Startup does not wait for TimeTickInspector registration.
The barrier is delivered once, and no second logical scanner is opened.

If the handoff position has been evicted, the scanner continues durable catchup.
If a tailing reader is evicted later, it resumes durable reads from the last
consumed message's safe physical LastConfirmedMessageID and filters out already
consumed TimeTicks. Both paths preserve the upper scanner's transaction and
ordering state. Cancellation releases the scanner while reading durable WAL,
paused at the startup boundary, or waiting on an empty WAB. Open failure,
AlterWAL early return, and normal close all release the retained stream.

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
  -> WALSummary.ObserveMessage(M) installs records and readable coverage
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> PChannel/VChannel metadata
       -> affected SegmentViews and L1 materialization bound
       -> L0Materializer.ObserveMessage records the requested boundary only
       -> QueryRuntime plain immutable event (future integration)
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
their batching decisions; L0Materializer reads Summary in bounded batches
under its observed window and L1 safety bound. RecoveryStorage does not aggregate
objects across segments.

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
3. Async Segment consumers own independent Retained handles; Summary copies
   records, while L0Materializer records only window boundaries.
4. Successful release requires recoverability; poisoned release frees memory but leaves checkpoint progress blocked.
5. Tracker advancement uses only the continuous completed WAL prefix.
6. Component `checkpoint_time_tick` fields are continuous component-local prefixes.
7. Dirty component snapshots are written before the global checkpoint.
8. WAL truncation never passes the published global checkpoint.
9. RecoveryBarrier is not a checkpoint or observation-mode boundary.
10. QueryRuntime does not participate in persistence acknowledgement.
11. Summary confirmation independently bounds every published checkpoint.
12. Summary installs readable records before VChannel observation advances the
    L0 materialization window; this does not require Summary persistence.
13. RecoveryBarrier rebuilds requested L0 windows without preloading payloads.
