# Message Workflow

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

This document describes how one WAL observation stream routes messages into
VChannel-owned persistence components. Handle lifetime and checkpoint gating
are defined by [WAL Message Ack Design](message_ack.md).

The shared observation path and capacity/API/Summary-backlog admission policy
are implemented in [L0Materializer](l0_materializer.md).

**Current runtime:** [WAL L0 Materializer](l0_materializer.md) retains Delete
handles for legacy query recovery. The [Summary consumer](summary_l0_materializer.md)
is retained for future QueryView wiring; the two implementations are not run together.

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
       -> after Segment/L1 updates, WALMaterializer retains Delete and explicit completion handles
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
different Segments may complete independently. WALMaterializer separately
tracks materialized and observed positions; a VChannel metadata no-op does
not skip its observation.

## 3. Ownership Table

| Holder | Creation | Release condition |
|---|---|---|
| Tracker Owner | `Tracker.Track` | BroadcastAck releases ordinary messages immediately or broadcast messages after Coordinator Ack. |
| Dispatch Retained | `Owner.Clone` | RecoveryStorage releases after synchronous manager dispatch. |
| VChannel dispatch Retained | Manager clones for each routed VChannel | Manager releases after synchronous VChannel observation. |
| Segment Retained | Segment exposes concrete async work | Object/lifecycle work succeeds after recovery metadata is installed and dirty. |
| WALSummary | Copies records without retaining a handle | `LastAcked()` independently bounds checkpoint publication. |
| WALMaterializer | Clones Delete/Txn and explicit Flush/lifecycle requests | Covered L0 output succeeds and recovery metadata is installed and dirty. Durable VChannel cursor separately controls GC. |
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
WALMaterializer retains the outer Delete/Txn message until L0 output and
DataCoord registration succeed and dirty materialization metadata is installed.
Size, age, explicit completion and recovery-tail requests drive output; only
initial L1 registration is a prerequisite, not final L1 flush.

### Flush-Style Messages

Flush, ManualFlush, FlushAll, DropCollection, DropPartition,
TruncateCollection, schema-changing AlterCollection, and AlterWAL may create
work in multiple SegmentViews and L0Materializers. Each asynchronous Segment
consumer owns an independent clone. L0Materializer merges the relevant boundary
without storing a BarrierEntry. ManualFlush, FlushAll, DropCollection,
DropPartition, TruncateCollection and AlterWAL additionally retain one handle
per affected VChannel until the requested L0 boundary completes. L1 and L0 tasks run independently
and their handles join before message completion. Dirty materialization metadata must be installed before these handles
release. This pins checkpoint and broadcast completion; unfinished requests
are rebuilt from WAL replay. Ordinary Segment Flush and generic DDL barriers
do not create explicit L0 requests.

### Txn

A committed Txn is one WAL message. Each affected SegmentView retains an
independent reference to the whole outer Txn. Summary copies records;
WALMaterializer retains the whole Txn when it contains Delete and commits
all Delete children together at the outer TimeTick.
Children returned by `RangeOver` do not receive independent Tracker entries.

One Txn may contain several inserts for the same segment. That SegmentView owns
one Txn handle and applies all of its assignments before completing its local
effect. A Txn spanning multiple segments completes only after every affected
SegmentView releases its handle.

## 5. RecoveryBarrier

RecoveryBarrier passes through the same Observe flow. Summary advances readable
coverage. The current WAL materializer does not force output or retain the
barrier; any earlier pending Delete already holds its own handle. The retained
Summary consumer uses barriers to expose pre-checkpoint history when reconnected.

The recovery controller separately observes its TimeTick to announce that the
startup scanner caught up. The barrier does not change component behavior and
does not create a second checkpoint.

## 6. Checkpoint Batch

```text
candidate = Tracker.CheckpointThrough(WALSummary.LastAcked())
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
3. Every asynchronous Segment consumer and explicit L0 completion consumer
   clones before dispatch returns. The current L0 consumer also clones ordinary
   Delete/Txn messages. Summary only copies records.
4. Txn children never have independent recovery ownership.
5. QueryRuntime does not retain RecoveryStorage handles.
6. Metadata/Segment filtering uses TimeTick and `checkpoint_time_tick`; L0
   window observation uses its own materialized/requested positions.
7. RecoveryBarrier is a catch-up event, not an Observe-mode transition.
8. Poisoned release frees memory but blocks the successful prefix and broadcast Ack.
