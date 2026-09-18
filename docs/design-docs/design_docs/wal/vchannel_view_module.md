# VChannel Recovery Module

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

`VChannelRecoveryModule` owns all recovery state for one VChannel and is
indexed by `PChannelRecoveryManager`.

RecoveryStorage constructs and dispatches through these modules during bounded
recovery and live observation. It separately owns [WALSummary](summary.md),
restores the Summary index, and caps checkpoint publication at `LastAcked`.
RecoveryStorage also restores idempotency windows from retained Summary history
and startup replay before accepting writes. QueryRuntime wiring remains follow-up work.

The current [WAL L0 materializer](l0_materializer.md) retains Delete handles.
The [Summary consumer](summary_l0_materializer.md) is retained for QueryView;
its future scheduling and recovery integration is outlined in sections 5–6.

## 1. Ownership

```text
PChannelRecoveryManager
  -> VChannelRecoveryModule
       +-- VChannelView
       +-- SegmentView*
       +-- L0Materializer
```

The module owns:

- collection, partition, schema, lifecycle, and tombstone state;
- one continuous VChannel metadata `checkpoint_time_tick`;
- SegmentView creation, lookup, routing, and snapshot aggregation;
- the VChannel WALMaterializer and its growing-registration prerequisite;
- DataView recovery state and QueryRuntime live-event forwarding (design
  intent, pending the qviews feature — not yet wired in the current code).

It does not own the PChannel global checkpoint, AckTracker, Coordinator
broadcast acknowledgement, or QueryView state transitions.

## 2. One Observe Path

```text
ObserveMessage(Retained)
  -> apply VChannel metadata when not already covered
  -> route the same retained message to affected SegmentViews
  -> let WALMaterializer retain Delete/Flush messages and schedule batches
  -> forward a plain live event to QueryRuntime when present
  -> mark changed recovery components dirty
```

RecoveryStorage first installs the message's Summary records and readable
coverage, then dispatches to this module. L0Materializer observation follows
Segment state changes, so it knows all earlier L1 registration dependencies.

There is no mode argument. Metadata and Segment effects use their loaded
`checkpoint_time_tick` to choose apply versus no-op. L0Materializer uses its own
materialized/observed positions; a metadata no-op must not suppress its Delete
observation.

For a PChannel-scoped message, the manager gives every affected VChannel an
independent dispatch clone. Every SegmentView exposing asynchronous work clones
again before VChannel observation returns. WALMaterializer clones Delete/Txn
and explicit Flush/lifecycle messages until L0 completion and installation of
dirty metadata.

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
- AlterWAL state belongs to the PChannel control fields embedded in the global
  WALCheckpoint, not a VChannel snapshot.

## 4. Dirty Snapshots

`ConsumeDirtySnapshots` aggregates immutable snapshots from:

- VChannelView (its snapshot carries `transform_materialized_time_tick`, so
  the L0 materialization frontier persists with it);
- dirty SegmentViews.

L0Materializer has no independent snapshot: its materialization frontier is
carried by VChannelMeta. Delete and explicit completion requests retain WAL handles until
L0 succeeds and the owner installs dirty M. Checkpoint cannot skip an unfinished
request, so replay reconstructs F without another metadata field. Restored
Segment state supplies the L1 dependencies.
After either a full or a base-only VChannel snapshot is durable, report its
captured frontier to Summary;
a newer in-memory value cannot authorize GC.

Every snapshot has one `checkpoint_time_tick` and an exact `MarkPersisted`
callback. The callback advances only through the captured snapshot and cannot
clear later mutations.

The owning RecoveryStorage writes these component snapshots before the one
global checkpoint.

## 5. Segment Completion And Summary L0 Scheduling (Future Wiring)

One message may have independent effects:

```text
Txn Owner
  +-- Segment A handle
  +-- Segment B handle
  +-- BroadcastAck root
```

The reference graph joins these effects without a VChannel-level Meta/Data
state machine. Each component advances its own durable state and releases its
own handle. Successful Tracker completion requires the entire graph to reach
zero without poison. Summary copies Txn records separately; its `LastAcked`
additionally bounds checkpoint publication. The retained Summary materializer reads those records without joining this
reference graph. The currently wired WAL consumer retains Delete handles and
uses only the growing-registration prerequisite described in
[WAL L0 Materializer](l0_materializer.md).

When the Summary consumer is reconnected, the VChannel module computes the L0 materialization safety bound across
its SegmentViews. An L1 Segment blocks L0 materialization after its
creation TimeTick until its final commit completes. This is scheduling
coordination only; it does not merge Segment and L0 persistence or
source-message ownership. A raised bound independently re-evaluates pending
requests, including when no new message arrives; it does not force undersized
output. Explicit API materialization waits for related L1 flushes and final
commits, in addition to respecting the VChannel safety bound. DropPartition
therefore flushes all Segments in the VChannel created before its boundary,
including Segments belonging to other partitions; its logical tombstone still
applies only to the target partition.

## 6. Summary L0 Recovery (Future Wiring)

`PChannelRecoveryManager` creates VChannel modules from persisted VChannel and
Segment metadata. L0Materializer restores M from VChannelMeta and initializes
its requested boundary W = M, without loading Delete payloads. The owner derives
the L1 bound from restored Segments before scheduling work. There is no separate
materializer catalog record. Tombstoned base state can coexist with retained
child state.

After construction:

1. load each component's stable snapshot and `checkpoint_time_tick`;
2. start the single PChannel replay from the global checkpoint;
3. route every replayed message through the normal Observe path;
4. let each component independently skip already-covered effects;
5. route RecoveryBarrier to every VChannel still requiring materialization,
   advancing W so pre-checkpoint Summary backlog can be read lazily when the
   batching policy admits work; the barrier itself does not force output;
   then announce startup catch-up;
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
2. retain state while serving, recovery, or QueryView rules require it; a
   VChannel tombstone also remains until Summary has durably retired its
   retained Delete history. Its persisted materialization frontier authorizes
   Summary GC before catalog removal, including after restart;
3. persist removal from recovery metadata;
4. remove object data asynchronously afterward.

Cleanup progress is component snapshot state. It never advances the global
checkpoint by itself.

## 9. Invariants

1. One VChannel module owns every recovery component for that VChannel.
2. Every message uses the same Observe API during replay and live processing.
3. Metadata/Segment filtering uses TimeTick and `checkpoint_time_tick`; L0
   window observation independently uses materialized/requested positions.
4. SegmentView and L0Materializer are VChannel-owned, not top-level modules.
5. QueryRuntime observation never delays Message Ack.
6. Dirty snapshots are stable and precede global checkpoint publication.
7. VChannel cleanup cannot delete child state still required for recovery.

## 10. Temporary legacy query checkpoint reporting

Until QueryView is enabled, the [WAL L0 materializer](l0_materializer.md)
retains Delete handles through output/registration and dirty cursor installation.
SegmentView retains Insert handles through growing publication. cp_updater can
therefore report the published global checkpoint's original MessageID and
TimeTick directly. There is no per-VChannel candidate queue, progress minimum,
or completed-Flush fallback. RPC failures retry a published point next tick.
This bridge is marked `TODO: Remove after enabling queryview.`

This change does not yet adjust WAL truncation. Backends that physically remove
history independently of this conservative query position still need the
separate truncation/retention integration before relying on older query seeks.
