# Message Workflow

This document describes how one WAL observation stream routes messages into
VChannel-owned persistence components. Handle lifetime and checkpoint gating
are defined by [WAL Message Ack Design](message_ack.md).

## 1. Common Flow

Observation is serialized in PChannel WAL order:

```text
raw message M
  -> Owner O = Tracker.Track(M)
  -> dispatch Retained D = O.Clone()
  -> PChannelRecoveryManager.ObserveMessage(D)
       -> route to every affected VChannel
       -> each actual async consumer clones its own handle
       -> QueryRuntime receives a plain immutable copy when needed
  -> D.Release()
  -> BroadcastAck.Accept(O)
```

Every message follows this flow during both startup replay and live
consumption. There is no metadata-only scan and no observation mode.

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

For a component with outstanding earlier work, later completed work waits in a
component-local completed queue. Its published `checkpoint_time_tick` moves
only when the component's relevant prefix is continuous.

## 3. Ownership Table

| Holder | Creation | Release condition |
|---|---|---|
| Tracker Owner | `Tracker.Track` | BroadcastAck releases ordinary messages immediately or broadcast messages after Coordinator Ack. |
| Dispatch Retained | `Owner.Clone` | RecoveryStorage releases after synchronous manager dispatch. |
| VChannel dispatch Retained | Manager clones for each routed VChannel | Manager releases after synchronous VChannel observation. |
| Segment Retained | Segment exposes concrete async work | Object/lifecycle work succeeds after recovery metadata is installed and dirty. |
| TransformLog Retained | Delete or barrier requires chunk work | Covering chunk is durable and TransformLog metadata is installed and dirty. |
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

TransformLog retains the message until the chunk containing the Delete is
durable and represented by dirty TransformLog metadata. L0 materialization is a
separate downstream operation and does not delay message completion.

### Flush-Style Messages

Flush, ManualFlush, FlushAll, DropCollection, DropPartition,
TruncateCollection, schema-changing AlterCollection, and AlterWAL may create
work in multiple SegmentViews and TransformLogs. Every concrete consumer owns
an independent clone.

### Txn

A committed Txn is one WAL message. Each affected SegmentView and TransformLog
retains an independent reference to the whole outer Txn. Children returned by
`RangeOver` do not receive independent Tracker entries.

One Txn may contain several inserts for the same segment. That SegmentView owns
one Txn handle and applies all of its assignments before completing its local
effect. A Txn spanning multiple segments completes only after every affected
SegmentView releases its handle.

## 5. RecoveryBarrier

RecoveryBarrier passes through the same Observe flow. Components process its
normal synchronization effects and Tracker accounts for any retained work.

The recovery controller separately observes its TimeTick to announce that the
startup scanner caught up. The barrier does not change component behavior and
does not create a second checkpoint.

## 6. Checkpoint Batch

```text
candidate = Tracker continuous completed point
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
3. Every asynchronous consumer clones before synchronous dispatch returns.
4. Txn children never have independent recovery ownership.
5. QueryRuntime does not retain RecoveryStorage handles.
6. Component filtering uses only TimeTick and `checkpoint_time_tick`.
7. RecoveryBarrier is a catch-up event, not an Observe-mode transition.
