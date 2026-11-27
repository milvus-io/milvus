# Design Document: Online Shard Split on QueryView

> Revised protocol: [r5 design](20261113-shard_split_on_queryview_r5.md). The atomic-all-segments adoption limit and unbounded replay cache described below are initial implementation constraints, superseded by the r5 implementation work. CDC multigeneration remains unresolved and gated.

**Date**: November 2026 (revision 4.1)
**Related Issue**: [#50463](https://github.com/milvus-io/milvus/issues/50463)
**Supersedes**: `20260610-shard_split.md`, and revisions 1–3 of this document

---

**Status.** An initial six-layer implementation is available on master base
`30c7552ed8`, with automatic splitting disabled. Unit and component tests cover
the mechanisms below; no real cluster or CDC failover run has been completed.
Statements about the master architecture cite the code they were read from.
§12 records the first-cut limits and remaining verification obligations.

Revisions 2 and 3 are withdrawn. Revision 2 rewrote the shard before the
switch; revision 3 shared segments between shards behind a per-row filter.
Both fail the constraints in §2: the first makes the switch depend on physical
state a secondary cluster does not share, the second needs a row filter in
segcore that was judged too heavy. §13 records them.

## 1. Summary

A shard (vchannel) `S` is split into two new shards `T1` and `T2` by **one**
WAL message, `SplitShard`.

- **Writes switch at the message.** `S` stops accepting DML at the message's
  time tick `T_switch`. `T1` and `T2` are created by the same message and take
  all later DML, routed by `hash(pk) mod modulus`.
- **Reads keep `S`.** The serving list becomes `[S, T1, T2]`. `S` keeps every
  row written before `T_switch`. `T1` and `T2` serve only what was written to
  them.
- **Whoever holds a row of `S` subscribes to the delete streams of `T1` and
  `T2`.** A delete of an old row is written to `T1` or `T2`, because that is
  where its primary key routes now. QueryNodes holding `S`'s sealed segments,
  and the StreamingNode holding `S`'s unflushed segments, subscribe to both
  TransformLog streams from `T_switch`.
- **One read timestamp for the three shards.** During the window a query reads
  `S`, `T1` and `T2` at one common time point `B`, obtained through the
  existing barrier path.
- **Rewrite in the background, adopt at the end.** `S`'s segments are
  rewritten by residue class into segments that each belong to one target.
  When none is left, one DataView change moves them to `T1`/`T2` and `S`
  leaves the serving list. This step is local to each cluster.

The rule the design obeys:

> **A row is served by exactly one shard. Its deletes reach it either because
> they are written to that shard, or because the holder of the row subscribes
> to the shard they are written to.**

## 2. Constraints

1. **Message first.** The first step is a WAL message. Every logical state
   change (writability, routing, the set of shards) is the effect of consuming
   that message, so a secondary cluster and a recovering node reach the same
   state.
2. **One message.** A second message whose handling assumes the first one's
   background work has finished is the same mistake as rewriting before the
   message: the secondary's background work runs on its own schedule.
3. **No per-row filter in the query engine.**
4. **The write stall does not depend on the amount of data.**
5. **Reads and writes stay correct during the whole window**, at every
   consistency level, including upsert and delete of rows written before the
   split.

## 3. What the design builds on

All paths are relative to the repository root.

| Fact on master | Where |
|---|---|
| A QueryView is per (replica, vchannel). A QueryNode binds one TransformLog buffer per view, keyed by the view's own vchannel | `internal/querynodev2/transformlogbuffer/buffer.go` `Bind` |
| A segment registers on the buffer of its own vchannel, from its transform cursor; a cursor below the buffer's retention start is rejected | same file, `RegisterSegment`, `registerSegment` |
| The TransformLog stream of a vchannel carries delete entries and payload-free progress (`SyncUp`), and can be subscribed remotely | `pkg/proto/streaming.proto` `SubscribeTransform`; `internal/streamingnode/server/wal/walsummary/dispatch_stream.go` |
| The stream advances only on delete-class events, not on time ticks | `dispatch_stream.go` `dispatchTransformMessageLocked`; `pkg/streaming/util/message/messageutil/query_mvcc.go` |
| Per vchannel the StreamingNode keeps a growing frontier and a transform frontier, and per pchannel a complete published prefix `through` | `internal/streamingnode/server/wal/interceptors/timetick/mvcc/query_mvcc.go` |
| Sealed segments are read at the transform frontier; growing segments at the growing frontier | `internal/querynodev2/viewquery/search_request.go`; `internal/views/queryclient/shard_client.go` `legacyMVCCForNode` |
| A query can fix a time point `B`; the StreamingNode returns frontiers equivalent to `B` once `through >= B`, otherwise it waits for its allocator to pass `B` and forces a sync | `internal/streamingnode/server/wal/adaptor/query_plan.go` `resolveBarrierMVCC`; `timetick/inspector/barrier.go`; `internal/util/idalloc/allocator.go` `BarrierUntil` |
| A published barrier reaches remote TransformLog subscribers of that vchannel as progress | `wal/recovery/query_barrier.go`; `wal/walsummary/query_barrier.go` |
| The proxy can hand a StreamingNode a ready-made MVCC pair, which is used as is | `pkg/proto/view.proto` `GetQueryPlanRequest.query_plan_mvcc` |
| A StreamingNode picks the Up view whose schema version equals the request's exactly | `internal/streamingnode/server/wal/snview/shard_view.go` `tryAcquireUpViewForSchema` |
| A view can be taken out of service without failing in-flight plans | `QueryViewMeta.control.preempt_serving_lease` (#55213) |
| A segment has one location in a DataView; a second location is a DataIntegrity error. A change group adds and removes segments in one version | `internal/dataview/manager.go` `addSegments`; `internal/dataview/publish_change.go` `PublishChange` |
| L0 compaction applies an L0 segment only to segments of the same insert channel | `internal/datacoord` compaction task `l0.go` |
| A message can be marked unreplicable. Without a replicate header it is admitted on a secondary as well; `ManualFlush` is sent this way today | `pkg/streaming/util/message/builder.go` `WithUnreplicable`; `wal/interceptors/replicate/replicates/impl.go`; `internal/datacoord/services.go` `broadcastManualFlush` |
| A StreamingNode builds a view for a schema version only if that version was observed on the vchannel's own WAL | `wal/vchannel/schema.go` `GetSchemaVersion`; `view.go` `ObserveSchemaChangeMessageV2`, `ObserveAlterCollectionMessageV2` |
| A schema change is a coordinator-driven handoff: views of every shard are prepared at the target version, then the version is published | `internal/views/coord/resource/schema.go` `PrepareSchema`; `coordview/shard_view_registry.go` `PublishCollectionSchema` |
| The proxy already asks the coordinator for cluster-local load state, with a version | `GetQueryViewLoadInfo` in `query_coord.proto`; `queryclient/load_check.go` |
| The vchannel state enum reserves `VCHANNEL_STATE_SPLITTED = 4` for shard split | `pkg/proto/streaming.proto` |
| The proxy resolves the vchannel list once per request and fans out, each shard running both phases on its own | `internal/views/queryclient/client_impl.go`, `shard_client.go` |

## 4. Model

### 4.1 Shards and states

| Shard | After `SplitShard` | Writable | In DDL broadcasts | Served |
|---|---|---|---|---|
| `S` | split: fenced, lineage `{T1, T2, T_switch}` recorded. Written once and never changed again | no | no | yes, until adoption on this cluster |
| `T1`, `T2` | `Normal`, with residue and modulus | yes | yes | yes |

`S` receives no replicated message after `T_switch`, and no DML at all. The
only later appends on `S` are local, unreplicable ones: a forwarded schema
(§10.1) and its retirement (§5.5). RootCoord's record of `S` is a lineage
record, identical on every cluster; whether `S` is still served is known only
to the cluster's own DataView.

A vchannel never changes its key range. A split replaces one vchannel by two
new names.

### 4.2 Serving generations

The set of shards a query fans out to is a **serving generation**. It is
derived state, owned by each cluster, and it follows the DataView: the
DataView carries a shard-set epoch that increases only when a shard is
declared or removed. `S` stays a shard of the DataView until adoption, so the
DataView, not RootCoord, is the source of the desired shard set for the view
coordinator.

| Generation | List | Published when |
|---|---|---|
| g0 | `[S]` | before the split |
| g1 | `[S, T1, T2]`, `S` with lineage | views of `T1`, `T2` and the lineage views of `S` are Up on every replica |
| g2 | `[T1, T2]` | adoption (§5.5) |

A view is built from one DataView version and so carries its epoch. The view
coordinator publishes an epoch once every shard of it is Up, next to the
published schema version (`ShardViewRegistry`), and persists it in its own
catalog. The proxy reads the list, the epoch and the lineage from the
coordinator through `GetQueryViewLoadInfo`, instead of taking the vchannel
list from its collection cache. A request carries the epoch the proxy
resolved its list from. A StreamingNode serves a request
only with a view of exactly that generation, the way it matches schema
versions today. A query therefore sees one generation on every shard or is
replanned. No invalidation broadcast is needed: a proxy with a stale epoch is
refused, refreshes and replans through the existing retry path.

### 4.3 Time points

All ticks come from one TSO and are comparable.

| Symbol | Meaning |
|---|---|
| `T_switch` | tick of `SplitShard` on `S`; `S`'s last event |
| `g1`, `g2` | genesis ticks of `T1`, `T2`; both greater than `T_switch` because the copy on `S` is persisted first |
| `through_i` | complete published prefix of the pchannel of `T_i` |
| `X_i` | transform frontier of `T_i` |
| `B` | common read point of one query |

### 4.4 Invariants

1. Every row and every delete in `S`'s own stream is at or below `T_switch`.
   Every event of `T1`, `T2` is above it.
2. During the window every holder of a row of `S` has applied, before it
   answers a query at `B`, every delete of `T1` and `T2` at or below `B`.
3. `S`, `T1` and `T2` are read at the same `B`. An upsert's delete and insert
   carry one timestamp, so both are visible or neither is.
4. `B <= through_i` for both targets when the query executes. No event at or
   below `B` can still appear on either target.
5. Delete history of `T1` and `T2` from `T_switch` on is retained until
   adoption has completed and the history has been folded into the adopted
   segments.

## 5. The split, in order

### 5.1 Before the message

Trigger and planning pick `S`, allocate the names of `T1`/`T2` and their
pchannels, and may issue a `ManualFlush` on `S` to shrink what is unflushed at
the fence. `ManualFlush` is unreplicable, so this only helps the cluster that
issues it; a secondary simply has a larger residual, which §6.2 handles.
Nothing else happens before `SplitShard`.

### 5.2 `SplitShard`

Broadcast to `S`, `T1`, `T2` and the control channel. The copy on `S` is
persisted first.

| Consumer | Action |
|---|---|
| StreamingNode of `S` | Seals every growing segment of `S`, rejects DML on `S` from now on with a retriable error, records `T_switch` and the lineage in `VChannelMeta`. The sealed-but-unflushed segments (the *residual*) flush through the normal path and stay segments of `S` |
| StreamingNodes of `T1`, `T2` | Genesis: create the vchannel module, the query MVCC frontier at the genesis tick, and the retention pin at `T_switch`. Today only `CreateCollection` and `RestoreSnapshot` do this (`wal/vchannel/manager.go`, `timetick/mvcc/query_mvcc.go` `applyLocked`) |
| RootCoord (ack callback) | `S` is recorded as split with its lineage, in its final form; `T1`, `T2` are added as `Normal` with residue and modulus; `S` leaves the DML routing table and the DDL broadcast set. One meta write, then proxy cache invalidation |
| DataCoord (ack callback) | Declares `T1`, `T2` in the DataView **first**, so that a flush on a target that is replayed early finds its shard. Records `T_switch`. Pins the transform frontier of `T1`, `T2` at `T_switch`. Suspends L0 compaction on `T1`, `T2` (§7.3) |
| QueryCoord / balancer | Builds views of `T1`, `T2`, and rebuilds the views of `S` with lineage streams, all for generation g1 |

On the primary, `T1` and `T2` do not admit DML until g1 is published
(§8.2). The callback holds the collection's broadcast resource key until the
residual has flushed, which serialises other DDL behind the split.

### 5.3 The window

- DML goes to `T1`, `T2`.
- Queries fan out to `[S, T1, T2]` and read at a common `B` (§6).
- QueryNodes holding segments of `S` subscribe to the TransformLog streams of
  `T1` and `T2` from `T_switch` (§6.1).
- The StreamingNode of `S` does the same for the residual until it has
  flushed (§6.2).

### 5.4 Rewrite

Runs in DataCoord's compaction framework, on each cluster independently.

1. `S`'s own L0 segments are folded into `S`'s segments by ordinary L0
   compaction. `S` is frozen, so this is a finite job; afterwards every
   segment of `S` has its cursor on `S`'s stream at `T_switch`.
2. A rewrite task takes a batch of segments of `S` and writes, per partition,
   one output per residue class. Outputs are segments **of `S`**, labelled
   with their target. The commit is an ordinary in-shard change group.
3. Outputs do not fold deletes of `T1`/`T2`. Like every segment of `S` they
   are subscribed to both target streams from `T_switch`.

The window ends when the residual has flushed and every segment of `S`
carries a label.

### 5.5 Adoption

Local to each cluster. No replicated message.

1. One DataView change moves every segment of `S` to the shard on its label,
   with its cursor on the target's stream at `T_switch`, and removes `S`.
   This is the only event that moves a segment between shards.
2. The balancer builds g2 views of `T1`, `T2`, seeded from the placement of
   `S`. Segments are shared by id on a QueryNode, so nothing is reloaded.
3. When every replica is Up at g2, g2 is published.
4. The g1 views are taken out of service with `preempt_serving_lease`.
5. L0 compaction on `T1`, `T2` resumes and folds the L0 segments accumulated
   since `T_switch` into all segments, adopted ones included. Cursors advance
   and the retention pin releases.
6. `S` is retired on this cluster by an unreplicable message on `S`'s WAL.
   The StreamingNode moves the vchannel to the reserved `SPLITTED` state and
   releases its module. DataCoord's split task records that this cluster has
   finished. RootCoord is not written.

## 6. Reads during the window

### 6.1 Sealed segments of `S` on a QueryNode

A g1 view of `S` carries its lineage: `{T1, T2, T_switch}`.

- `Bind` creates or references a buffer for `S` and for each lineage stream.
  The stream to the owner of each pchannel is opened as today
  (`getOrCreateStreamLocked`, `AcquireTransformLogStream`).
- Every segment of the view has one registration per stream: on `S` from its
  cursor, on `T1` and on `T2` from `T_switch`. A delete entry is applied
  through the same `ApplyTransform`; entries of the other class miss the
  segment's primary-key filter.
- Before executing, the QueryNode waits per registration for the tick of
  that registration's stream, and reads the segment at `B`.

The request therefore carries, in addition to today's pair, the read point
`B` and a transform tick per lineage vchannel.

### 6.2 The residual on the StreamingNode of `S`

The growing runtime today takes deletes only from its own WAL
(`growingruntime/live.go`, `dispatch.go`). For the residual it subscribes to
the two target streams from `T_switch` with the same client a QueryNode uses,
feeds entries into the existing `applyTransformLogEntry`, and keeps an applied
position per stream. `WaitMVCCVisible` additionally waits for those positions.
The subscription ends when the residual has flushed.

A gate on writes instead of this subscription is not correct on a secondary:
replicated DML cannot be refused there (§9).

### 6.3 The common read point

For a collection whose serving generation contains a shard with lineage, the
proxy does this per query:

1. Ask the StreamingNodes of `T1` and `T2` for `through` and, for
   Strong/Session, for the Strong minimum prefix (`QueryMVCCManager.Minimum`).
2. `B = max(lower bound, min(through_1, through_2))`.
3. Phase 1 on `T1` and `T2` with `barrier_mvcc = B`. Each waits until its
   `through >= B` and returns its frontiers capped at `B`.
4. Phase 1 on `S` with a proxy-provided pair `(T_switch, T_switch)`.
5. Phase 2 on `S` carries `B` and `{T1: X_1@B, T2: X_2@B}`.

| Consistency | Lower bound | Waits |
|---|---|---|
| Strong, Session | larger of the two Strong minimum prefixes | the lagging side syncs once |
| Bounded | guarantee timestamp from the proxy | normally none |
| Eventually | none | none |
| Customized | the user's guarantee timestamp | only if newer than both `through` |
| Iterator, statistics, delete by expression | already a fixed point; used as `B` | as today |

Why this is sufficient:

- Events at or below `B` are complete on both targets: `through_i >= B`, and
  `BarrierUntil(B)` guarantees the node allocates nothing at or below `B`
  afterwards.
- Events above `B` may already be applied to a segment of `S`; the engine
  filters them by timestamp.
- A read may see writes that completed after it arrived and before `B`. That
  is the existing Strong semantics: `getQueryPlanMVCC` returns the frontier
  at the time the wait ends.

Worked example. `T_switch = 100`, upsert of key `k` on `T1` at 110 (old row in
`S`), insert on `T2` at 120, delete of key `m` on `T2` at 130 (row in `S`). A
Strong query arrives with `through_1 = 125`, `through_2 = 135`, nothing
pending.

| Step | Value |
|---|---|
| `B` | 135; `T1` syncs until `through_1 >= 135` |
| `T1` at `B` | growing 110, transform 110 |
| `T2` at `B` | growing 130, transform 130 |
| `S` | 100, 100 |
| Segments of `S` | wait `S` to 100, `T1` to 110, `T2` to 130; read at 135 |

`k` is returned once, the insert at 120 is visible, `m` is gone. A delete on
`T1` allocated at 127 but unacknowledged when the query arrived holds
`through_1` below 127 until it is published, so it is included. A delete
issued after the query arrived is allocated above 135 and is excluded
everywhere.

### 6.4 After adoption

Adopted segments belong to one target and have one stream. Queries go back to
the ordinary per-shard path.

## 7. Data management

### 7.1 DataView

| Event | Change |
|---|---|
| Declare targets | new: add empty shards outside `OnCreateCollection` |
| Flush of the residual, rewrite commits, `S`'s L0 compaction | unchanged, all inside `S` |
| Adoption | new: move segments between shards and remove a shard in one version |

### 7.2 Segment metadata

One new field on a segment: the split target label. The cursor of a segment of
`S` on a lineage stream is the constant `T_switch`, so no per-stream cursor is
stored.

### 7.3 Delete history of the targets

Segments of `S` are not targets of `T1`/`T2`'s L0 compaction, because their
insert channel is `S`. If that compaction ran during the window and dropped
its L0 segments, the adopted segments could never be folded and would replay
from `T_switch` for ever. Therefore L0 compaction on `T1`/`T2` is suspended
from `T_switch` to adoption, their L0 segments accumulate, and the
TransformLog of both is retained from `T_switch`.

Cost: every QueryNode holding segments of `S` buffers the target delete
entries since `T_switch` in memory (`vchannelBuffer.entries`), and a restarted
QueryNode replays them. The cost is linear in the deletes written during the
window. A later optimisation is an L0 compaction that selects targets by
lineage and a per-stream cursor on segments of `S`.

## 8. Writes

### 8.1 Routing

After the RootCoord callback, a proxy routes each row by residue to `T1` or
`T2`. A proxy with a stale cache sends to `S`, is refused with a retriable
error, refreshes and retries **the refused rows only**.

### 8.2 Admission on the targets (primary only)

A Strong read must see a write that has completed. A write to `T1` is visible
only to a query of generation g1. So `T1` and `T2` refuse DML, retriably,
until g1 is published and the g0 views are out of service. From then on a
proxy still at g0 cannot read at all until it refreshes. This is admission control
on the primary, not a logical state; replicated appends on a secondary are
not subject to it.

### 8.3 Write stall

From the fence on `S` to the opening of the targets: the broadcast, one
RootCoord write, building views over no new data (`T1`, `T2` are empty and
`S`'s segments are already loaded), and proxy invalidation. No flush, no
load, no compaction.

## 9. Replication

| Aspect | Behaviour |
|---|---|
| What is replicated | `SplitShard`, and the DML and DDL that follow on `T1`/`T2` |
| On the secondary | The same consumers run: fence, genesis, RootCoord and DataCoord callbacks |
| Rewrite, adoption, retirement of `S` | Derived per cluster, like segments and placement. The retirement message is unreplicable |
| RootCoord metadata | Changed only by replicated messages. `S`'s record is written once by `SplitShard` |
| Schema change inside a cluster's window | The `AlterCollection` callback runs on both clusters and forwards the schema to that cluster's `S` locally (§10.1) |
| Serving generation | Per cluster. A secondary may still serve g1 while the primary serves g2; both return the same rows |
| Reads between the append-side source fence and g1 | The initial implementation refuses new plans against a source view without lineage on either cluster, until g1 is ready. Previously acquired plans may finish at their fixed snapshot at or below `T_switch`. This trades temporary read availability for preserving Strong/guarantee lower bounds; g0 alone cannot satisfy a newer bound |
| Replicated DML to a target before g1 | Accepted. New source plans without lineage are refused from the append-side fence, even before its consumer catches up; the residual subscribes to target deletes |
| Switchover inside the window | The new primary continues its own window. Nothing refers to the other cluster's physical state |

Time ticks, segment ids and `T_switch` used in any state change on a cluster
are those of the message in that cluster's own WAL.

## 10. Recovery and failures

| Failure | Behaviour |
|---|---|
| Coordinator restart inside the callback | The callback is re-entered; each step is idempotent (declare, record, pin) |
| Coordinator restart inside the window | The window is reconstructed from `S`'s lineage record, the DataView and DataCoord's split task; rewrite tasks recover as compaction tasks |
| StreamingNode of `S` restarts before the residual flushed | Growing segments are rebuilt from the WAL; the runtime resubscribes to both target streams from `T_switch` |
| StreamingNode of a target restarts | Streams break; waits on QueryNodes block until resubscription, as for a single stream today |
| QueryNode restarts | Segments of `S` are reloaded and register on three streams; the target history is replayed from `T_switch` (invariant 5) |
| Rewrite task fails | Retried; inputs stay in place |
| Failure during adoption | The DataView change is one version. Before it, g1 keeps serving. After it, g2 views are built and published on recovery |
| Abort | None after `SplitShard` is appended. The split only moves forward |

### 10.1 DDL during the window

`S` is not in the DDL broadcast set. A secondary cannot refuse a replicated
DDL, so "refuse until adoption" is only available for requests a cluster
originates itself.

| DDL | Handling |
|---|---|
| Schema change | The `AlterCollection` ack callback, before `PrepareSchema`, appends an unreplicable `SchemaChange` message carrying the new schema to the WAL of every `S` this cluster has not retired. `S` then observes the version and can build its view; `PrepareSchema` waits for `S` like for any shard. The message is a single-vchannel append, not a broadcast, so no callback runs twice. On a fenced `S` its interceptor finds no growing segment to flush |
| Drop collection | Coordinators drop everything, `S` included |
| Drop partition, truncate | Broadcast to the targets; DataCoord removes the affected segments of `S` in the collection-level callback. `S` holds only sealed data once the residual has flushed, and the split callback keeps the collection's broadcast key until then, so no DDL interleaves with the residual |
| Create partition, flush | Targets only |
| Import, snapshot, a second split of `T1`/`T2` | Refused on the primary until its local retirement (including the g2 handover and retained-delete fold); out of scope for a secondary still in its window |

## 11. What changes on master

| # | Change | Where |
|---|---|---|
| 1 | Shard state, residue, modulus, lineage in collection metadata and the published view; residue routing | `internal/rootcoord`, `internal/util/routing`, `internal/proxy/dml` |
| 2 | `SplitShard` message; fence on `S`; genesis of a vchannel outside `CreateCollection`; refused-row retry | `wal/interceptors/shard`, `wal/vchannel/manager.go`, `timetick/mvcc`, proxy |
| 3 | Serving generation: shard-set epoch in the DataView; desired shards from the DataView; published epoch in the view registry; `GetQueryViewLoadInfo` returns list, epoch and lineage; exact match on the StreamingNode | `internal/dataview`, `internal/views/coord`, `query_coord.proto`, `snview/shard_view.go`, `queryclient/resolver` |
| 4 | Lineage on view meta; multi-stream `Bind`; one registration per stream | `view.proto`, `transformlogbuffer/buffer.go`, `qnview` |
| 5 | Read point and per-vchannel transform ticks in the plan MVCC; per-registration wait; read at `B` | `view.proto` `QueryPlanMVCC`, `qnview/query_task_provider.go`, `segment_resource_handle.go`, `viewquery` |
| 6 | `GetMVCCTimestamp` returns `through` and the Strong minimum; lineage-group orchestration in the proxy | `adaptor/query_plan.go`, `queryclient/client_impl.go`, `shard_client.go` |
| 7 | Residual subscription and extra applied positions in the growing runtime | `wal/vchannel/growingruntime` |
| 8 | DataView: declare shards; adoption event | `internal/dataview` |
| 9 | Rewrite compaction with a target label; suspend target L0 compaction; pin target transform frontier | `internal/datacoord`, `internal/datanode/compactor` |
| 10 | Target admission gate; unreplicable retirement of `S` and the `SPLITTED` vchannel state; local schema forward to `S` | `wal/interceptors/shard`, `wal/vchannel`, `internal/rootcoord` alter callback, DataCoord split task |
| 11 | Trigger, configuration, metrics | DataCoord |

No change to the query engine. No milvus-proto change: `ShardState`,
`MsgType_SplitShard` and the routing fields are in the pinned version.

## 12. Not verified, and open decisions

Implementation limits of the first cut:

- The configuration defaults to disabled, and the minimum capable node version
  remains unset. A release must establish that version before automatic splits
  can be admitted, even if the configuration switch is enabled.
- New source queries wait from the append-side fence until a lineage view can
  serve them on both roles; they do not silently read an older g0 snapshot
  when a newer Strong/guarantee bound is required.
- Adoption is one metadata transaction. A move exceeding the catalog's atomic
  operation limit or the implementation's 1 MiB payload budget is refused
  before metadata changes, leaving g1 serving. Large adoptions need a future
  atomic snapshot protocol; they are not split into partially visible batches.
- A conservative capacity check runs under the collection resource key before
  the broadcast. The default 1 TiB size trigger exceeds this first-cut capacity;
  smaller test limits do not establish support for large production shards.
  TEXT and namespace collections are refused. Import remains refused for a
  collection with residue routing, including after adoption.
- Replication admission remains disabled by default. Experimental override does
  not guarantee a secondary's independent segment layout fits atomic adoption;
  such a secondary retains g1 and its deletion-history protection.
- Before recording target L0 resumption, the data plane appends a local,
  unreplicable ManualFlush to each target. The persisted materialization
  boundary and absence of outstanding L0 data certify when segment cursors and
  the DataView split pin can advance, including a target with no deletes.
  Releasing this pin does not change master's conservative WAL query-retention
  policy, which still retains history from collection creation.
- A cluster-local lifecycle guard serializes schema handoff with adoption and
  retirement. After recovery, an ALTER callback first finishes any already
  durable adoption at the existing DataView schema before preparing its new
  schema, regardless of whether it or the background driver acquires the
  guard first. g1 publication and write admission do not take this guard.

Original open questions (some now have unit coverage; real deployment and
native-engine verification remain separate obligations):

- Genesis of a vchannel by a message other than `CreateCollection`.
- That suspending L0 compaction per vchannel fits the existing trigger.
- That an idle secondary publishes barriers the same way a primary does; no
  special case was found in `timetick/`.
- That a sealed segment whose rows are newer than the last delete is covered
  because `Flush` advances the transform frontier (inferred from
  `AdvancesQueryTransformMVCC`, not checked in the engine).
- Memory held by the target buffers on QueryNodes over a long window.

- That a `SchemaChange` appended to a fenced vchannel has no effect beyond
  recording the schema (`handleSchemaChange` flushes and fences growing
  segments, of which `S` has none), and that a retried callback appending it
  twice is harmless.
- That the read path on the StreamingNode of `S` takes its partition set from
  load state and not from `S`'s own WAL; `growingruntime` keeps a
  `loadedPartitions` set, its source was not traced to the end.
- That the proxy's write path and other users of the collection's vchannel
  list are unaffected when the read list moves to `GetQueryViewLoadInfo`.

Decided in this revision, with the reasoning in §4.2, §5.5 and §10.1:

1. Schema change during the window is forwarded to `S` locally.
2. The serving generation is the DataView's shard-set epoch, published by the
   view coordinator.
3. RootCoord keeps a permanent lineage record of `S`; retirement is local and
   does not touch RootCoord.

## 13. Alternatives considered

| Alternative | Why not |
|---|---|
| Rewrite in place first, then switch (revision 2) | A compaction sits in the stall; a hot shard may not converge; as two messages it breaks constraint 2 |
| Shared segments behind a per-row class filter (revision 3) | Constraint 3 |
| Shared segments without a filter | Counts and aggregations are wrong |
| Class-aware writes before the switch, then relabel | Needs two messages; the secondary may not have finished when the second arrives |
| `S` consumes the insert streams of the targets and stays the only serving shard | Double ownership of growing data, StreamingNode-to-StreamingNode MVCC, `S`'s node becomes the bottleneck |
| Dual write to `S` and the targets | Two tick coordinates for one row, a cross-WAL transaction, duplicated replication |
| Gate target writes until the residual is loaded, instead of §6.2 | Cannot gate replicated DML on a secondary |
| Per-stream read timestamps instead of a common `B` | The engine has one read timestamp per segment |
| `B` computed by the holder of `S` as the minimum of its stream positions | The stream does not advance without deletes, holders differ, and the targets must read at the same point |
