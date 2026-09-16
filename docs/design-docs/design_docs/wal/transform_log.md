# TransformLog Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

TransformLog is the VChannel-level transform **consumer**: it turns the
transform records of the pchannel-scoped WALSummary into DataCoord-managed L0
segments. Delete is the initial transform payload. QueryNode and StreamingNode
query resources consume the L0 output to advance transform visibility.

The module is implemented, but its production recovery wiring is pending.
Its WALSummary interactions are reads when needed, recovery of its consumer
window, and reporting a GC position. This document specifies those consumer
contracts and L0 materialization. The WALSummary storage protocol is defined
in [WALSummary](summary.md).

## 1. Ownership

```text
RecoveryStorage (pchannel)
  -> PChannelRecoveryManager
       +-- VChannelRecoveryModule A
       |     +-- summaryView (per-vchannel view of the walsummary.Manager)
       |     +-- TransformLog A   (materialize-only consumer)
       +-- VChannelRecoveryModule B
       |     +-- summaryView (per-vchannel view of the walsummary.Manager)
       |     +-- TransformLog B
       +-- walsummary.Manager (pchannel-scoped summary read interface)
             +-- views per vchannel
```

TransformLog owns:

- the in-memory materialization window (`pending`): the transform records of
  its vchannel after the committed frontier, fed by direct observation (and
  once, by recovery);
- the committed materialization frontier `materialized_time_tick`, carried by
  `VChannelMeta.transform_materialized_time_tick`;
- the L1 upper bound derived from uncommitted L0 segments;
- L0 materialization (batching, ordering, retry).

## 2. WALSummary Consumer Interface

TransformLog consumes ordered entries for its vchannel through the summary read
interface, currently
`Manager.ReadTransformEntries(vchannel, materializedTimeTick, +inf)`. Recovery
uses this interface to obtain the outstanding transform records and seed
`PendingEntries`. The current implementation loads that window once during
recovery; additional reads, if needed, use the same consumer interface.

Live messages feed the materialization window through `ObserveMessage`.
TransformLog reports the position through which its records can be released
only after the corresponding materialization metadata is durable (§8).

## 3. Message Classification

| Kind | WAL messages | Effect |
|---|---|---|
| Payload | Delete, committed Txn containing Delete | `TransformLog.ObserveMessage` appends one ordered Delete record to its materialization window. |
| Barrier | RecoveryBarrier, Flush, ManualFlush, FlushAll, DropPartition, DropCollection, TruncateCollection, CreateCollection, schema-changing AlterCollection, AlterWAL | VChannel handlers coordinate lifecycle work. A barrier of this VChannel also enters the TransformLog window without a delete payload, allowing its materialization frontier to reach the boundary. Pchannel-level broadcasts do not add such a boundary. |
| None | Insert and other messages | No transform effect. |

A committed Txn creates one record at the outer Txn TimeTick and stores Delete
blocks for all Delete children.

## 4. Observe And Materialization Trigger

There is one Observe path for recovery and live messages:

1. classify the message;
2. return for `None`;
3. `TransformLog.ObserveMessage`: build the transform record, skip it when its
   timetick is at or below the committed frontier or the recovery-loaded
   window coverage, append it to `pending` otherwise, and schedule a
   materialize task for the current window frontier (at most one task per
   observation moment; the cap-batch continuation keeps the chain going).

No external request exists: the transform consumer materializes whatever its
window holds, as soon as the L1 upper bound allows, at its own pace.

## 5. The Materialization Window

The window (`pending`) is the transform records of the vchannel after the
committed frontier, in ascending timetick order:

- recovery seeds its head once: the durable records after
  `materialized_time_tick`, loaded from the summary store
  (`ReadTransformEntries`); the coverage of that load is remembered as
  `loadedThrough`;
- live observation appends the tail: delete records past `loadedThrough` (and
  past the committed frontier);
- committed batches trim the head.

Replay deduplication: after a restart, WAL replay re-observes the records the
recovered window already holds. Observation skips records at or below
`loadedThrough`, so the window never duplicates the recovered backlog.
If a later read is needed, merge its results using the same coverage and
materialized-position checks.

## 6. L0 Materialization

Materialization converts the windowed Delete entries into DataCoord-managed L0
deltalogs. It is triggered autonomously: observation (or an L1 upper bound
advance) schedules a task whenever the window holds materializable records.

Materialization:

- consumes the materialization window populated by recovery reads and live
  observation;
- does not retain source WAL messages;
- does not delay BroadcastAck;
- does not gate the global recovery checkpoint;
- does not pass the earliest uncommitted L1 Segment's creation TimeTick;
- commits `materialized_time_tick` into `VChannelMeta`, marking the vchannel
  snapshot dirty for the next RecoveryStorage checkpoint;
- may be retried idempotently at the logical level.

`VChannelRecoveryModule` derives one inclusive materialization upper bound from
its SegmentViews:

```text
upper_bound = min(create_segment_time_tick of every Segment with l1_commit_done = false)
```

When there is no such Segment, the bound is unbounded. The creation TimeTick is
safe to include because rows assigned to that Segment have later TimeTicks.
This guarantees that an L0 Segment never covers a transform range whose L1
data has not completed its final commit.

The target of one batch is `min(window_frontier, upper_bound)`. Every
completed L1 final commit makes the owning VChannel recompute the bound, which
schedules the next batch without requiring another WAL trigger. Batches are
capped by rows/bytes; a capped batch schedules a continuation task whose
predecessor is the current one, keeping batches strictly sequential.

Physical duplicate L0 output after a crash is outside the WAL checkpoint
protocol and requires lifecycle idempotency or reconciliation.

## 7. Recovery

The caller supplies a recovered WALSummary read interface and the VChannel's
durable materialization metadata. TransformLog treats summary recovery as a
precondition; it does not inspect object keys or reconstruct the summary index.

1. restore `materialized_time_tick` from
   `VChannelMeta.transform_materialized_time_tick`;
2. load the initial materialization window:
   `summaryManager.ReadTransformEntries(vchannel, materializedTimeTick, +inf)`
   and seed the pending records after that frontier;
3. set `loadedThrough` to the loaded window's coverage, using at least the
   materialized frontier when the window is empty;
4. replay WAL through the normal `ObserveMessage` path, skipping records already
   materialized or included in the recovered window;
5. continue live observation and materialization through the same interfaces.

Recovery relies on the summary read contract to provide its retained records
and on WAL replay for subsequent records. The consumer's GC position preserves
the history it still needs. The materialized frontier claims only work whose L0
output is durable; observation does not reintroduce records already covered by
that frontier or the recovered window.

## 8. GC Position

TransformLog supplies the position through which it no longer requires transform
records. The integration reports it to WALSummary through
`Manager.AdvanceGCTimeTick`; recovery seeds it from durable VChannel metadata.

L0 materialization first produces durable output and updates the in-memory
`materialized_time_tick`, marking the VChannel snapshot dirty. That update alone
does not authorize GC. Only after the corresponding VChannel snapshot is durable
may its position be reported to WALSummary. A publication callback reports the
position captured in that snapshot, not a newer in-memory position. Durable
lifecycle cleanup may supply an equivalent release position when applicable.

## 9. Invariants

1. TransformLog is VChannel-owned and consumes WALSummary through its read and
   GC-position interfaces.
2. All entry positions use source WAL TimeTick.
3. The materialized frontier covers only durable L0 output; its reported GC
   position additionally requires durable VChannel metadata.
4. Records are copied at observation; TransformLog retains no source message
   handle, and L0 materialization does not gate source-message Ack.
5. VChannel barriers may advance the consumer window without a payload; their
   arrival alone does not advance the durable GC position.
6. Recovery reads and WAL replay must not duplicate records in the pending
   materialization window.
