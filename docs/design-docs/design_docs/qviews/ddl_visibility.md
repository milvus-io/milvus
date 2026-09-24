# TODO: Truncate and Partition Drop Query Visibility

Status: agreed design TODO for issue #40451, recorded on 2026-09-24.
**The MVCC-based design and its distributed delivery are outside the current SN
query extraction PR.** This document does not change the current DDL protocol.

## 1. Two Visibility Models

| Model | How removed data becomes invisible | Query handoff requirement |
|---|---|---|
| Ideal: DDL in TransformLog | SN and QN evaluate segment eligibility using the DDL boundary and the request's Transform MVCC | No new QueryView is required solely to hide affected data, once the required Transform prefix is applied |
| Transitional: view-based DDL | A replacement QueryView excludes the removed data | DDL completion must wait for the corresponding QueryView handoff; the legacy equivalent is QueryCoord target update |

The lack of immediate MVCC filtering is not, by itself, a defect in the
transitional model. Its correctness must be checked across the complete
flush/publication, DataView update and query-view handoff chain.

## 2. Ideal Design: Typed DDL Transform Entries

Introduce explicit TransformLogEntry variants for TruncateCollection and
DropPartition. Each carries its source WAL TimeTick and sufficient collection,
partition and lifecycle identity to determine which data it invalidates.
A payload-free progress barrier cannot substitute for either entry.

Deliver the entries reliably and in WAL order to **all SNs and QNs serving the
affected VChannels and replicas**. This is logical broadcast through the
TransformLog delivery/replay contract, not best-effort notification to currently
connected processes. A node loading data later, reconnecting or recovering must
reconstruct the same effects before serving the corresponding MVCC. Unrelated
nodes need not consume these entries.

For a DDL entry at TimeTick T on a VChannel and a request with Transform MVCC M:

- M < T: that DDL does not exclude the segment; normal QueryView membership,
  DataVersion handoff and row visibility rules still apply.
- M >= T: exclude segments invalidated by that DDL, even when executing through
  a retained older QueryView.

Truncate invalidates the collection data before its boundary, while subsequent
writes remain eligible. DropPartition identifies the dropped partition by its
stable ID/lifecycle; another partition with the same name is not the same target.
The check applies consistently to planning/candidate probing and execution on
both SN and QN. A consumer may advertise Transform coverage through T only after
it has applied, or proven irrelevant, every required Delete and DDL effect in
that prefix. Merely observing a WAL TimeTick does not prove query visibility.

### Segment boundary prerequisite

Whole-segment filtering requires affected segments to contain only data on one
side of the DDL boundary. Truncate must establish a write/segment rotation
boundary; compaction and other rewrites must preserve that separation and its
provenance. The creation time of a replacement physical segment alone is not
sufficient to identify the age of its contents. If an operation can mix retained
and invalidated rows, it needs finer-grained visibility or must prohibit that
mixing before this segment-level design can be enabled.

### Completion and resource lifetime

This model removes query visibility's dependency on loading a replacement view.
It does not remove WAL durability, metadata, or DDL acknowledgement requirements.
The exact completion fence and behavior of lagging consumers remain part of the
follow-up protocol: requests must wait for their required Transform prefix or
fail/retry, never serve with an unapplied DDL hidden behind an advanced frontier.

Logical exclusion does not immediately free physical segments. Older MVCC plans
and acquired handles retain the resources required by the existing Up lease and
view-lifetime contracts. QueryView replacement and DataView reclamation can
continue separately. BM25 remains one current local aggregate, not a set of
historical MVCC aggregates; its DDL/refresh reconciliation needs separate tests.

## 3. Transitional Design: Wait for QueryView Handoff

Until all participating SN/QN consumers support the ideal protocol, use the
view-based contract for TruncateCollection and DropPartition:

1. Establish the DDL boundary and finish the required flush/publication work.
2. Update Coordinator membership so the new DataView excludes invalidated data.
3. Prepare the corresponding QueryView and complete its handoff to serving Up
   across the affected shards/replicas, so fresh plans use the replacement view.
4. Complete the DDL's query-visibility fence. Existing plans and resource cleanup
   follow their view/lease rules; handoff does not require immediate physical GC.

Creating a DataView version or reaching Preparing/Ready alone is insufficient:
the replacement must actually be selected for subsequent queries. The legacy
implementation expresses the analogous coordination through QueryCoord target
updates. This contract must not be silently replaced by advancing an SN-local
Transform frontier while QNs continue to use old membership.

## 4. Current Branch Evidence and Follow-Up Scope

Currently, streaming.proto's TransformLogEntry has only a Delete variant.
TruncateCollection and DropPartition classify as Transform barriers, and the SN
GrowingRuntime does not implement their time-based visibility effects. The
local Summary-backed adaptor supports SN Delete bootstrap; remote QN delivery
is still outside this extraction.

The legacy Truncate callback explicitly invokes ManualUpdateCurrentTarget,
which waits for QueryCoord's current target. DropPartition instead notifies
DataCoord, changes metadata and expires Proxy caches; that callback alone is
not evidence that the new QueryView handoff fence already exists. The table
above records the agreed transitional correctness contract, not a claim that
this PR has wired or validated the full new Coord/Proxy workflow for both DDLs.

Follow-up work must cover typed entries and retained replay, ordered SN/QN
application, MVCC selection, segment/compaction boundaries, DDL completion,
recovery and rollout compatibility. Mixed deployments must retain the view-based
fence until every serving consumer understands the new entries; an older node
must not silently skip them and still claim Transform coverage.

Validation must include pre/post-DDL queries through old and new views, lagging
and restarted nodes, later segment loads, duplicate delivery, Truncate followed
by new writes, partition recreation, compaction across the boundary, and Up
leases spanning a handoff. No proto, runtime, transport or acknowledgement
implementation is included in this TODO update.

## References

- [QueryView state machine](query_view_state_machine.md)
- [Serving lease](query_view_lease.md)
- [DataView](data_view.md)
- [TransformLog adaptor](../wal/transform_log.md)
- [Transform start-after coverage](transform_start_after_timetick.md)

Source checkpoints: pkg/proto/streaming.proto;
pkg/streaming/util/message/messageutil/transform_log.go;
internal/streamingnode/server/wal/vchannel/growingruntime/{live,dispatch,segment}.go;
internal/rootcoord/ddl_callbacks_{truncate_collection,drop_partition}.go;
internal/querycoordv2/services.go (ManualUpdateCurrentTarget).
