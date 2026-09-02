# TransformLog Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

TransformLog is the VChannel-level transform **consumer**: it turns the
transform records of the pchannel-scoped WALSummary into DataCoord-managed L0
segments. Delete is the initial transform payload. QueryNode and StreamingNode
query resources consume the L0 output to advance transform visibility.

Per-message ownership is defined by
[WAL Message Ack Design](../message_ack.md).

## 1. Ownership

```text
RecoveryStorage (pchannel)
  -> PChannelRecoveryManager
       +-- VChannelRecoveryModule A
       |     +-- TransformLog A   (materialize-only consumer)
       +-- VChannelRecoveryModule B
       |     +-- TransformLog B
  -> walsummary.Manager (pchannel-scoped summary store, owns persistence)
```

Persistence lives exclusively in the [WALSummary](../summary.md) of the
pchannel; the TransformLog owns **no** buffer, no chunk objects, and no catalog
metadata.

TransformLog owns:

- the in-memory materialization window (`pending`): the transform records of
  its vchannel after the committed frontier, fed by direct observation (and
  once, by recovery);
- the committed materialization frontier `materialized_time_tick`, carried by
  `VChannelMeta.transform_materialized_time_tick`;
- the L1 upper bound derived from uncommitted L0 segments;
- L0 materialization (batching, ordering, retry).

## 2. Summary Interaction

[WALSummary](../summary.md) owns its persistence workflow. TransformLog reads
summary records for recovery and provides the materialization frontier for GC.
It observes live messages into its own materialization window without retaining
WAL handles. RecoveryStorage separately caps checkpoint publication through
`WALSummary.LastAcked()`; L0 materialization may proceed independently.

## 3. Message Classification

| Kind | WAL messages | Effect |
|---|---|---|
| Payload | Delete, committed Txn containing Delete | `WALSummary.ObserveMessage` appends one ordered Delete record to the summary staging; `TransformLog.ObserveMessage` appends the same record to its own materialization window. |
| Barrier | RecoveryBarrier, Flush, ManualFlush, FlushAll, DropPartition, DropCollection, TruncateCollection, CreateCollection, schema-changing AlterCollection, AlterWAL | VChannel-level handlers only (segment flush etc.). No transform effect: neither the summary nor the TransformLog reacts to barriers. |
| None | Insert and other messages | No transform effect. |

A committed Txn creates one record at the outer Txn TimeTick and stores Delete
blocks for all Delete children.

## 4. Observe And Materialization Trigger

There is one Observe path for recovery and live messages:

1. classify the message;
2. return for `None`;
3. `WALSummary.ObserveMessage`: build the transform record (a standalone
   proto), copy it into summary staging without retaining a message handle;
4. `TransformLog.ObserveMessage`: build the same record, skip it when its
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
`loadedThrough`, so the window never duplicates the recovered backlog; the
summary manager independently skips records at or below its restored durable
frontier, so the same records are never rewritten into new chunks.

## 6. L0 Materialization

Materialization converts the windowed Delete entries into DataCoord-managed L0
deltalogs. It is triggered autonomously: observation (or an L1 upper bound
advance) schedules a task whenever the window holds materializable records.

Materialization:

- consumes the **observed window** directly (never the summary staging or
  store), so it does not wait for persistence;
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

1. the summary recovers its manifest (and fences the term via the catalog
   meta);
2. for every vchannel, recovery loads the initial materialization window once:
   `summaryManager.ReadTransformEntries(vchannel, materializedTimeTick, +inf)`
   — the only read of the summary store in the whole consumer path; the
   coverage of the load becomes `loadedThrough`;
3. the module restores `materialized_time_tick` from
   `VChannelMeta.transform_materialized_time_tick` and seeds the window;
4. the summary view's durable frontier is restored from the manifest
   (`Manager.DurableTimeTick` / `SetDurableTimeTick`), so replay does not
   re-stage already-durable records;
5. live operation continues from the restored frontier.

Consistency argument (no ordering between materialization and persistence):

- materialization commits a frontier only after its L0 output is in object
  storage, so `materialized_time_tick` never claims un-materialized records;
- un-materialized records are, by construction, still covered by a retained
  summary chunk (the summary never releases a chunk above a vchannel's
  materialization frontier), so recovery step 3 rebuilds them even when the
  WAL checkpoint has already advanced past them;
- a materialization ahead of the durable frontier is safe: the L0 output
  survives independently, and replay observation skips what the window
  already holds.

## 8. GC

The summary releases chunk objects by retention budget, bounded below by the
per-vchannel materialization frontiers mirrored via
`Manager.SetMaterializedTimeTick`. A chunk fully covered by the materialization
frontier is guaranteed to have been materialized (its L0 output is durable), so
releasing it cannot lose transform data.

## 9. Invariants

1. TransformLog is VChannel-owned; persistence is pchannel-owned (WALSummary).
2. All entry positions use source WAL TimeTick.
3. `WAL checkpoint <= durable summary frontier` (handle lifecycle, summary
   owns the handles); the materialization frontier has **no** ordering
   relation with either.
4. A Delete handle releases only after the chunk and manifest are durable
   (summary-owned).
5. Barriers have no effect on the summary or the TransformLog.
6. L0 materialization does not gate source-message Ack.
7. The transform consumer never triggers persistence and never reads the
   summary store at runtime (except the one-time recovery window load).
