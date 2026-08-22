# VChannel Recovery Module

`VChannelRecoveryModule` owns all recovery state for one VChannel and is
indexed by `PChannelRecoveryManager`.

Message completion is defined by
[WAL Message Ack Design](message_ack.md).

## 1. Ownership

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       +-- VChannelView
       +-- SegmentView*
       +-- TransformLog
       +-- DataView recovery state
       +-- QueryRuntime bridge
```

The module owns:

- collection, partition, schema, lifecycle, and tombstone state;
- one continuous VChannel metadata `checkpoint_time_tick`;
- SegmentView creation, lookup, routing, and snapshot aggregation;
- the VChannel TransformLog and stream registration;
- DataView recovery state and segment DataVersion summaries;
- QueryRuntime snapshot construction and live-event forwarding.

It does not own the PChannel global checkpoint, AckTracker, Coordinator
broadcast acknowledgement, or QueryView state transitions.

## 2. One Observe Path

```text
ObserveMessage(Retained)
  -> apply VChannel metadata when not already covered
  -> route the same retained message to affected SegmentViews
  -> route it to TransformLog
  -> forward a plain live event to QueryRuntime when present
  -> mark changed recovery components dirty
```

There is no mode argument. The message TimeTick and each component's loaded
`checkpoint_time_tick` are sufficient to choose apply versus no-op.

For a PChannel-scoped message, the manager gives every affected VChannel an
independent dispatch clone. Every SegmentView or TransformLog that exposes
asynchronous work clones again before VChannel observation returns.

## 3. VChannel Metadata State

VChannel metadata messages are observed serially in PChannel order. The view
keeps:

- mutable live state used by the running node;
- a stable recoverable snapshot state;
- `checkpoint_time_tick` for the stable state;
- persisted checkpoint used only by dirty-snapshot bookkeeping.

Metadata-only operations normally commit synchronously into stable state. If a
metadata transition depends on asynchronous work, it joins a component-local
pending queue and cannot advance `checkpoint_time_tick` across a gap.

Rules include:

- CreateCollection/CreatePartition add identity, membership, and schema state;
- DropCollection/DropPartition persist logical tombstones before cleanup;
- TruncateCollection records the new lifecycle boundary and routes data work;
- schema-changing AlterCollection appends schema history before segment routing;
- AlterWAL state belongs to the PChannel control snapshot, not the global
  checkpoint.

## 4. Dirty Snapshots

`ConsumeDirtySnapshots` aggregates immutable snapshots from:

- VChannelView;
- dirty SegmentViews;
- TransformLog;
- owned DataView recovery state.

Every snapshot has one `checkpoint_time_tick` and an exact `MarkPersisted`
callback. The callback advances only through the captured snapshot and cannot
clear later mutations.

The owning RecoveryStorage writes these component snapshots before the one
global checkpoint.

## 5. Segment And TransformLog Join

One message may have independent effects:

```text
Txn Owner
  +-- Segment A handle
  +-- Segment B handle
  +-- TransformLog handle
  +-- BroadcastAck root
```

The reference graph joins these effects without a VChannel-level Meta/Data
state machine. Each component advances its own durable state and releases its
own handle. Tracker completion occurs only when the entire graph reaches zero.

The VChannel module also computes the L0 materialization safety bound across
its SegmentViews. An L1 Segment blocks TransformLog materialization after its
creation TimeTick until its final commit completes. This is scheduling
coordination only; it does not merge Segment and TransformLog persistence or
source-message ownership.

## 6. Recovery

`PChannelRecoveryManager` creates VChannel modules from the union of persisted
VChannel, Segment, TransformLog, and DataView records. This allows tombstoned
base state to coexist with retained child state.

After construction:

1. load each component's stable snapshot and `checkpoint_time_tick`;
2. start the single PChannel replay from the global checkpoint;
3. route every replayed message through the normal Observe path;
4. let each component independently skip already-covered effects;
5. announce startup catch-up when the scanner reaches RecoveryBarrier;
6. independently resolve recovered lifecycle work needed for QueryRuntime view
   capture.

There is no module mode transition before or after the barrier.

## 7. QueryRuntime Boundary

QueryRuntime receives ordinary immutable events and owns no RecoveryStorage
handles. VChannel WAL-view capture and live observer installation use the same
VChannel lock so messages appear either in the captured state or in the live
event queue, never in neither.

QueryRuntime readiness may wait for component-specific conditions such as a
flushed Segment whose `l1_commit_done` marker is absent. It does not create or
wait for a second global recovery checkpoint.

## 8. Cleanup

Cleanup is logical before physical:

1. persist a VChannel or child tombstone;
2. retain state while serving, recovery, or DataVersion rules require it;
3. persist removal from recovery metadata;
4. remove object data asynchronously afterward.

Cleanup progress is component snapshot state. It never advances the global
checkpoint by itself.

## 9. Invariants

1. One VChannel module owns every recovery component for that VChannel.
2. Every message uses the same Observe API during replay and live processing.
3. Component filtering uses only TimeTick and one `checkpoint_time_tick`.
4. SegmentView and TransformLog are VChannel-owned, not top-level modules.
5. QueryRuntime observation never delays Message Ack.
6. Dirty snapshots are stable and precede global checkpoint publication.
7. VChannel cleanup cannot delete child state still required for recovery.
