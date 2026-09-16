# Message Workflow

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

This document describes how one WAL observation stream routes messages into
VChannel-owned persistence components. Handle lifetime and checkpoint gating
are defined by [WAL Message Ack Design](message_ack.md).

The shared observation path is implemented. The capacity/API/Summary-backlog
admission policy described below is an agreed revision, pending implementation
in [L0Materializer](l0_materializer.md).

## 1. Common Flow

Observation is serialized in PChannel WAL order:

```text
raw message M
  -> Owner O = Tracker.Track(M)
  -> dispatch Retained D = O.Clone()
  -> WALSummary.ObserveMessage(M) installs records and readable coverage
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> route to every affected VChannel
       -> each actual async Segment consumer clones its own handle
       -> after Segment/L1 updates, L0Materializer records its window boundary
       -> QueryRuntime receives a plain immutable copy when needed (future)
  -> D.Release()
  -> BroadcastAck.Accept(O)
```

This order is implemented for the [L0Materializer](l0_materializer.md) split: Summary
installs readable records and coverage before VChannel observation advances W.
Summary visibility here means in-memory readable coverage, not upload success.
Every message follows this flow during both startup replay and live consumption.
There is no metadata-only scan and no observation mode.

The Tracker point is:

```text
Point.MessageID = M.LastConfirmedMessageID()
Point.TimeTick  = M.TimeTick()
```

TimeTick is the unique, monotonic log-order identifier within one PChannel.

## 2. Component Idempotency

Each component compares the message TimeTick with the
`checkpoint_time_tick` loaded from its snapshot:

```text
message TimeTick <= component checkpoint_time_tick
  -> component effect is already durable; no-op

message TimeTick > component checkpoint_time_tick
  -> apply the complete component effect
```

The manager still routes one message to all affected components because their
frontiers may differ. It does not decide whether a message is metadata or data.

Each component's published `checkpoint_time_tick` moves only through its
continuous relevant prefix. SegmentView enforces this with serial task execution;
different Segments may complete independently. L0Materializer separately
tracks materialized/requested window positions; a VChannel metadata no-op does
not skip its observation.

## 3. Ownership Table

| Holder | Creation | Release condition |
|---|---|---|
| Tracker Owner | `Tracker.Track` | BroadcastAck releases ordinary messages immediately or broadcast messages after Coordinator Ack. |
| Dispatch Retained | `Owner.Clone` | RecoveryStorage releases after synchronous manager dispatch. |
| VChannel dispatch Retained | Manager clones for each routed VChannel | Manager releases after synchronous VChannel observation. |
| Segment Retained | Segment exposes concrete async work | Object/lifecycle work succeeds after recovery metadata is installed and dirty. |
| WALSummary | Copies records without retaining a handle | `LastAcked()` independently bounds checkpoint publication. |
| L0Materializer | Observes window boundaries only, with no handle | Reads Summary lazily; durable VChannel cursor controls its GC release. |
| QueryRuntime event | Plain immutable copy | QueryRuntime queue lifecycle; outside RecoveryStorage Ack. |

## 4. Typical Messages

### TimeTick

No persistence consumer retains the message. The ordinary Owner releases after
dispatch and Tracker can advance immediately.

### CreateCollection And CreatePartition

The VChannel snapshot records collection, partition, and schema state. If the
component snapshot already covers the message TimeTick, replay is a no-op.

### CreateSegment

The target SegmentView records assignment state and retains a handle while
ensure-growing work is outstanding. Its `checkpoint_time_tick` advances only
after the required lifecycle state is recoverable.

### Insert

Every affected SegmentView retains one handle with its pending L1 pack. A
Segment flush may batch multiple WAL messages from the same segment. All
covered handles release only after the shared object write succeeds and the
resulting recovery metadata is installed.

### Delete

WALSummary copies the Delete record and advances `LastAcked()` only through its
recoverable prefix. RecoveryStorage caps checkpoint publication at that frontier.
L0Materializer advances its observed window boundary. The revised admission
policy accumulates Deletes to capacity, or waits for an explicit completion or
Summary backlog request; observation alone does not force output. It reads
Summary independently without retaining the message.

### Flush-Style Messages

Flush, ManualFlush, FlushAll, DropCollection, DropPartition,
TruncateCollection, schema-changing AlterCollection, and AlterWAL may create
work in multiple SegmentViews and L0Materializers. Each asynchronous Segment
consumer owns an independent clone; L0Materializer only merges the relevant
boundary into its requested window, without storing a BarrierEntry. Under the
revised batching policy, operations requiring completed L0 output also record
explicit completion intent and wait for related L1 flush/final-commit completion.
Barrier classification alone does not establish that requirement.

### Txn

A committed Txn is one WAL message. Each affected SegmentView retains an
independent reference to the whole outer Txn. Summary copies records;
L0Materializer classifies the Txn and records its outer TimeTick when applicable.
Children returned by `RangeOver` do not receive independent Tracker entries.

One Txn may contain several inserts for the same segment. That SegmentView owns
one Txn handle and applies all of its assignments before completing its local
effect. A Txn spanning multiple segments completes only after every affected
SegmentView releases its handle.

## 5. RecoveryBarrier

RecoveryBarrier passes through the same Observe flow. Summary records its
readable coverage even though it has no payload; each affected L0Materializer
advances its requested boundary. This exposes pre-checkpoint Summary backlog
without needing new Deletes. Under the revised batching policy, Summary backlog
or another admission reason requests actual materialization; RecoveryBarrier
does not itself force the historical window into L0. Tracker accounts for any retained component work.

The recovery controller separately observes its TimeTick to announce that the
startup scanner caught up. The barrier does not change component behavior and
does not create a second checkpoint.

## 6. Checkpoint Batch

```text
candidate = min_by_TimeTick(Tracker.CompletedPoint(), WALSummary.LastAcked())
freeze candidate
  -> consume stable component snapshots
  -> persist snapshots
  -> persist candidate checkpoint last
  -> MarkPersisted
  -> truncate WAL
```

The global checkpoint may not pass an unfinished message. A component snapshot
may be newer than the global checkpoint and uses its own
`checkpoint_time_tick` to make replay safe.

## 7. Invariants

1. Every observed message has exactly one Tracker Owner.
2. Startup and live messages use the same complete Observe flow.
3. Every asynchronous Segment consumer clones before dispatch returns;
   Summary copies records and L0Materializer records only window boundaries.
4. Txn children never have independent recovery ownership.
5. QueryRuntime does not retain RecoveryStorage handles.
6. Metadata/Segment filtering uses TimeTick and `checkpoint_time_tick`; L0
   window observation uses its own materialized/requested positions.
7. RecoveryBarrier is a catch-up event, not an Observe-mode transition.
8. Poisoned release frees memory but blocks the successful prefix and broadcast Ack.
