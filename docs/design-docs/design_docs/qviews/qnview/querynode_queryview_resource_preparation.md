# QueryNode QueryView Resource Preparation

## 1. Scope

This document describes the QueryNode work required to turn a
`QueryViewOfQueryNode` assignment into query-ready sealed-segment resources.
It covers collection runtime pinning, revisioned segment metadata, detached
physical segment loading, TransformLog catch-up, sharing across view versions,
and release.

It deliberately does not define QueryView placement, QueryCoord balancing,
StreamingNode growing-resource preparation, two-phase query execution, or the
wire implementation of QueryCoord and WAL adapters. Those components consume
the contracts defined here but have independent lifecycles.

## 2. Readiness Definition

A QueryNode may report an assigned segment ready only after all of the
following conditions hold:

1. The collection runtime is pinned for the lifetime of the QueryView.
2. An exact, immutable collection load-config snapshot has been resolved.
3. A revisioned `SegmentLoadInfo` snapshot has been accepted for the segment.
4. The sealed segment data, delta logs, PK candidate, and required indexes are
   loaded into a detached physical segment.
5. The physical segment is registered with the view's TransformLog buffer.
6. The registration has consumed transforms through the buffer's catch-up
   frontier.

Physical load completion alone is not readiness. Reporting Ready before step 6
can expose a sealed segment that is missing deletes between its persisted delta
position and the QueryView transform frontier.

## 3. Resource Pipeline

```text
SyncQueryView(Preparing)
  -> acquire TransformLog view guard
  -> acquire versioned collection runtime guard
  -> acquire view-scoped physical segment references
       -> subscribe SegmentLoadInfo(collection, segment, after_revision)
       -> reserve loading memory/disk
       -> create detached sealed segment
       -> load data + delta logs + PK candidate + indexes
       -> apply later metadata revisions by reopen/index update
  -> register each segment with TransformLog
  -> wait for catch-up
  -> report segment Ready
```

Dropping executes the inverse lifetime:

```text
cancel collection/load/update work
  -> unregister TransformLog segments
  -> release TransformLog view guard
  -> release view-scoped physical references
  -> release collection runtime guard
  -> report Dropped
```

Callbacks are asynchronous. `Acquire` must eventually report Ready progress or
Unrecoverable; `Release` must report Dropped exactly once.

## 4. Collection Runtime and Load-Config Version

`QueryViewMeta.load_info_version` identifies an immutable, collection-level
load-config snapshot. The metadata provider must return exactly the requested
version. Returning the current snapshot while ignoring the requested version is
invalid: a view could otherwise combine an old placement with new load fields,
partitions, or index metadata.

The runtime manager validates both `collection_id` and `load_info_version`
before mutating local collection state. A mismatch is a system error and is
retryable; it is not an input error because the QueryView request is valid and
the provider failed to honor its internal version contract.

After validation, the runtime manager calls `CollectionManager.PutOrRef` and
holds the resulting reference until Dropped. Index metadata refreshes use the
same collection schema-transition epoch as schema changes, preventing native
collection metadata from observing a partially ordered update.

## 5. Segment Metadata Stream

Segment load metadata changes independently of QueryView membership. Index
completion, binlog refresh, manifest replacement, and other content changes do
not require a new QueryView version. Each segment therefore has a separate,
strictly increasing `SegmentLoadInfoRevision`.

The QueryNode subscription protocol is:

- subscribe by `(collection_id, segment_id, after_revision)`;
- accept only matching collection and segment IDs;
- ignore empty, duplicate, and older revisions;
- acknowledge a revision only after the local handler accepts it;
- on a retryable disconnect, reconnect from the last accepted revision;
- close the subscription when the final view reference is removed.

The revision is an ordering token owned by the metadata source. It is not a
segment loader version and must not be forwarded as one.

The core uses `SegmentLoadInfoEventSource` and `SegmentLoadInfoEventReader` as a
transport-neutral boundary. The QueryCoord RPC adapter is intentionally thin:
it opens the stream, maps wire snapshots to the core type, and classifies
transport errors. Reconnect, resume, monotonic filtering, and handler
acknowledgement remain inside QueryNode.

## 6. Physical Segment Ownership

`ViewScopedPhysicalSegmentManager` owns at most one physical instance for a
segment ID. Every QueryView that references the segment acquires its own
manager reference, including views that reuse an already loaded or currently
catching-up segment. This is required so dropping an older view cannot release
a segment still used by a newer view.

Physical segments are detached from the legacy global `SegmentManager` map.
They reuse the existing loader primitives for resource reservation, data load,
delta load, Bloom-filter/PK-candidate load, reopen, and index replacement.
Loading resource reservations are explicit and idempotently released on all
success and failure paths.

For a newer metadata revision:

1. an unloaded segment starts a load from that snapshot;
2. a loading or updating segment retains only the highest pending revision;
3. a loaded segment performs reopen and index replacement;
4. a lower or equal revision is ignored.

Scheduler task cancellation is awaited before final physical release so an
in-flight load or update cannot access a released segment.

## 7. TransformLog Boundary

The preparation core depends on two independent interfaces:

- `TransformLogGuard` pins the buffer range required by one QueryView;
- `TransformRegistration` pins and catches up one physical segment.

The WAL layer owns transform-entry decoding. The QueryNode physical segment
offers the transport-neutral `TransformDeleteApplier` surface: apply a decoded
delete to a partition and advance the applied timetick only after the mutation
succeeds. This keeps generated WAL proto dependencies out of the resource core
until the WAL TransformLog API is available on master.

Registration failure or catch-up failure makes every waiting view that uses the
segment Unrecoverable. The physical manager is reset for that segment so a
future view cannot reuse a partially prepared resource.

## 8. Sharing and Failure Semantics

Collection, physical segment, TransformLog registration, and readiness state
have distinct reference domains:

| Resource | Sharing key | Released when |
|---|---|---|
| Collection runtime | collection | final view collection guard drops |
| Physical segment | segment ID | final physical view reference drops |
| Transform registration | segment ID | final readiness reference drops |
| Transform buffer guard | QueryView key | that QueryView drops |

Expected resource pressure is retried by the injected QueryNode scheduler.
Invalid or incomplete internal metadata, physical load failure, and TransformLog
continuity failure are terminal for the affected preparation attempt. Internal
and transient failures remain system errors so retry policy is not accidentally
disabled by input-error classification.

## 9. Integration Boundary

The extracted core compiles independently of the handler, node-scheduler,
QueryCoord metadata RPC, and WAL TransformLog implementations that are still
developed on separate branches. Integration requires only adapters for:

1. the QueryNode scheduler (`TaskScheduler`);
2. QueryCoord collection/load metadata (`QueryViewLoadMetadataProvider`);
3. QueryCoord segment metadata streaming (`SegmentLoadInfoEventSource`);
4. WAL TransformLog buffering (`TransformLogBuffer`);
5. the QueryView handler's Preparing and Dropping callbacks.

No adapter may weaken the exact load-config version check, revision monotonicity,
or TransformLog catch-up readiness gate described above.
