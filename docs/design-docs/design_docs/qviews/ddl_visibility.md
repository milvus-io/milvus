# TODO: Truncate and Partition Drop Query Visibility

Status: agreed design TODO for issue #40451, recorded on 2026-09-24.
**The MVCC-based design and its distributed delivery are outside the current SN
query extraction PR.** The transitional QueryView fence below is implemented for TruncateCollection.

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

Truncate's callback calls `resource.Resource().Refresh` directly. The package-level
accessor in `internal/views/coord/resource` exposes the Coordinator's shared
QueryView infrastructure without forwarding through QueryCoord RPCs. DataCoord
synchronously reconciles committed SegmentMeta into a DataView and returns its
DataVersion; the shared QueryView readiness service waits for every expected VChannel × configured replica
to report an Up view covering that version and the current LoadInfoVersion.
Preparing/Ready and an older Up view cannot satisfy this fence. An unloaded
collection has no serving obligation, and release ends an existing obligation.
Fresh loads consume the committed DataView. Cancellation and publication errors
propagate to the DDL callback for retry.

DropPartition still needs a separate audit of its completion fence; this change
does not claim to implement typed DDL Transform entries or its handoff wait.

### Refresh load and newly created partitions

`LoadCollection(refresh=true)` and `LoadPartitions(refresh=true)` use the same
synchronous DataView publication and QueryView handoff path. Refresh requires a
loaded QueryView configuration. QueryCoord records the greatest requested data
fence in memory and reports refresh progress against that fence and the current
LoadInfoVersion, separately from ordinary loading progress. Release clears the
fence. Concurrent refreshes cannot lower it. These are request-local completion
requirements, not a new durable configuration or DataView field.

CreatePartition first commits partition metadata in its WAL acknowledgement
callback, then applies the union of the new partition and the existing loaded
partition set through the same application path as AlterLoadConfig. It preserves
fields and replica placement, and does not load an unloaded collection. The
CreatePartition WAL operation supplies serialization and replay: the callback
must not recursively acquire a collection broadcast lock. Coordinator waits for
all expected Up views to carry the resulting current LoadInfoVersion, then
expires caches and completes CreatePartition; Proxy performs no extra wait.
Duplicate callback delivery must neither duplicate partitions nor skip the
QueryView readiness check.

Follow-up work must cover typed entries and retained replay, ordered SN/QN
application, MVCC selection, segment/compaction boundaries, DDL completion,
recovery and rollout compatibility. Mixed deployments must retain the view-based
fence until every serving consumer understands the new entries; an older node
must not silently skip them and still claim Transform coverage.

Validation must include pre/post-DDL queries through old and new views, lagging
and restarted nodes, later segment loads, duplicate delivery, Truncate followed
by new writes, partition recreation, compaction across the boundary, and Up
leases spanning a handoff. The typed DDL Transform protocol remains deferred; the implementation above
uses the transitional view-based protocol.

### External collection refresh completion

RefreshExternalCollection broadcasts to the target collection VChannels with
AckSyncUp, advancing their Transform MVCC. Its callback only persists the job
and per-channel fences, so it does not recursively broadcast schema changes.
Background work explores and applies the snapshot, stamps changed Segment bases
with those fences, and persists `segments_applied` while keeping the job
InProgress. A crash before that marker may replay the idempotent manifest update.
After the marker, retries must not reapply Segment results or invalidate indexes.

The worker then publishes the external source/spec through RootCoord, reconciles
the committed DataView, and uses Coordinator's shared QueryView readiness fence.
Readiness also checks each Segment's required Manifest version; Reopen must
load refreshed deltalogs before acknowledging that version.
Only after current serving views cover that publication (and optional index
waiting completes) may the job report Finished/100%. Transient publication
failures remain pending; remote waits happen outside the job lock. An unloaded
collection has no handoff obligation. Legacy jobs without business-channel
fences retain their original completion path for recovery compatibility.

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
internal/views/coord/resource/resource.go; internal/views/coord/loadstatus/wait.go.

## Import completion and serving readiness

CommitImport completes the storage publication, not the query-node load. Its full
ACK callback persists Committing, applies each business channel's commit TimeTick
to the imported segments, synchronously publishes their DataView, then persists
Completed. The existing collection broadcast resource lock serializes these
publications. Persistence failures retry in Committing; no QueryView Up wait is
performed while holding this lock.

A subsequent initial Load starts from the already published DataView. On an
already loaded collection, clients use refresh-load to wait for serving views
that cover the committed DataVersion. Import Completed alone is not a statement
that an existing serving view has already handed off. This matches the existing
separation between import completion and Load/Refresh readiness.

Pending imports without a CommitImport position freeze their channel's accepted
W; they do not block publication of another committed import. See
[Transform start frontier](transform_start_after_timetick.md) for the constraint
handoff from pending jobs to committed segment bases.
