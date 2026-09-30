# TransformLog Subscription Adaptor Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** Remote transport and resumable clients are implemented in the
view-resource preparation workspace. The local Summary subscription implementation
belongs to `40451-sn_query_extract`; it is not duplicated here. The two workspaces
integrate through the interfaces in `internal/streamingnode/server/wal/transform_log.go`.

TransformLog is a read-only subscription adaptor over
[WALSummary](summary.md#54-transform-read-contract). It owns no record storage,
has no `ObserveMessage`, and does not materialize L0. L0 materialization and
TransformLog subscriptions are independent consumers of the same Summary.

## 1. Ownership

```text
WALSummary (one store per PChannel)
  +-- L0Materializer per VChannel       [implemented]
  +-- TransformLog subscription adaptor [future integration]
        +-- local / remote PChannel streams
              +-- VChannel subscriptions
```

TransformLog owns stream and subscription lifetimes, delivery cursors,
historical catch-up, live delivery, and subscription errors. WALSummary owns
payloads, indexes, bounded reads, storage caches, readable coverage, and GC.
The adaptor keeps only bounded delivery batches; it does not mirror the full
Summary backlog or maintain independent chunks, manifests, or catalog keys.

The qv branch's stream protocol and consumers are the reference for external
behavior. Its independent VChannel storage and retained-message write path are
not part of this design.

## 2. Subscription Interface

The qv interface shape is retained:

```text
AcquireStream(PChannel)
  -> Subscribe(VChannel, StartAfterTimeTick, optional EndTimeTick, Handler)
       -> DeleteEntry / SyncUp / FastForward / Error
```

One stream may carry several VChannel subscriptions. A subscription reads
strictly after its start cursor. An unset end means continuous delivery; a set
end means bounded replay through that position. Stream closure releases all
subscriptions; closing one subscription does not close a shared stream.

QueryNode uses continuous subscriptions to catch loaded sealed Segments up and
then apply live Deletes. StreamingNode uses bounded subscriptions when preparing
growing resources from a captured WAL view; subsequent resource events arrive
through the VChannel's ordered live event path. Both consumers use the same
Summary-backed read semantics.

## 3. Entry And SyncUp Semantics

DeleteEntry carries Delete payloads at the source WAL TimeTick. A committed Txn
uses its outer TimeTick and delivers its Delete children as one ordered entry.
Pure Inserts do not produce transform entries. Other ordered messages may
establish progress without producing payload records.

`SyncUp(T)` means every Delete in the subscription's requested interval through
T has been delivered successfully. It can advance through an empty interval,
which lets query consumers advance visibility even when no Delete exists at T.
It does not prove Summary persistence, completion of other WAL effects, or L0
materialization. SyncUp and payload-free Barriers are not stored as records.

The adaptor derives progress from Summary's complete readable coverage, not the
largest Delete TimeTick or a requested end position. Subscription delivery is
independent of the L1 safety bound used by the separate materializer.

## 4. Catch-Up And Live Delivery

For each catch-up round:

1. capture a readable target and a change token from Summary;
2. cap the target by EndTimeTick when set;
3. read and deliver bounded batches after the cursor through that target;
4. emit SyncUp only through the range actually covered and delivered;
5. recheck Summary progress before waiting for a change.

Snapshot capture and change registration must avoid lost wakeups. Moving data
from pending to sealed to durable storage must not create subscription gaps or
duplicates. A fixed catch-up target prevents a busy writer from postponing the
initial SyncUp indefinitely.

A bounded subscription completes only after coverage reaches its EndTimeTick.
If Summary has not reached that position, the subscription waits or reports an
explicit failure; an empty read is not successful completion. In particular,
StreamingNode preparation must not complete with an incomplete Delete replay.

Use bounded work and delivery buffers. A slow subscriber must not block WAL
observation or create an unbounded adaptor backlog. A stream that cannot keep up
may be closed and resumed from its accepted cursor. Sharing reads for live
subscriptions to the same VChannel avoids decoding the same records repeatedly.

## 5. Resume And Failure

Local and remote transports expose the same contract. After transport failure,
the client reacquires the PChannel stream and resubscribes exclusively after the
last position its handler successfully accepted. Entry, SyncUp and an explicitly accepted FastForward advance
that cursor; failed handler calls do not.

No durable consumer ACK or cross-process exactly-once guarantee is introduced.
A caller recovering its own state must select a cursor consistent with that
state. If the recovered server has not yet reconstructed a previously delivered
position, it must not manufacture coverage from the resume request.

Invalid options and an unavailable VChannel are explicit semantic errors.
When Summary returns `FastForwardTimeTick`, the adaptor must expose the skipped
interval before delivering later entries. The caller must reconcile that skip
with its own base state; it is not SyncUp or proof that retired Deletes were
delivered. A consumer requiring complete replay rejects the skip. Missing or
corrupt retained objects fail the read; they are never converted into empty
history, fast-forward, or SyncUp.

## 6. Retention Prerequisite

Before subscriptions are enabled, QueryView/DataView integration must protect
the historical start points needed for future Segment loads, reconnects, and
local bounded replays. Already delivered data can still be needed by a retained
view; subscription delivery is not permission to delete it.

The owner of those view requirements reports retention constraints to Summary.
TransformLog does not own object deletion or infer view lifetime from a stream's
cursor. Unknown requirements during recovery are not equivalent to no readers.
New requirements must be installed before GC can remove the requested history.
See [Summary retention](summary.md#4-retention-gc) for the shared-store contract
and [WAL input view](streamingnode_vchannel_wal_view.md) for snapshot handoff.

## 7. Invariants

1. TransformLog has no WAL observation or storage-write path.
2. Every subscription reads the same WALSummary record store.
3. Payload delivery is ordered by source WAL TimeTick with an exclusive cursor.
4. SyncUp claims only complete, successfully delivered coverage.
5. Historical/live handoff and storage transitions lose no records.
6. Subscription cursors do not advance L0 materialization or authorize GC.
7. L0 materialization does not depend on this adaptor or on external subscribers.

## 8. Workspace Integration Boundary

The shared `TransformLogStreamManager`, `TransformLogStream`, subscription options,
handler and sentinel errors match `40451-sn_query_extract` at `be4b5bdaa0`.
The local workspace exposes its manager from a WAL through `wal.TransformLogProvider`:

```go
type TransformLogProvider interface {
    TransformLog() TransformLogAccesser
}
```

The remote workspace implements `StreamingNodeHandlerService.SubscribeTransform`,
assignment-aware remote stream creation, typed subscription-error transport and
resumption from the last handler-accepted Entry/SyncUp. It exposes
`streaming.TransformLogStreamManager()` for the QueryNode resource-preparation
consumer. Even co-located clients use this remote transport; there is no second
local reader in this workspace.

The local workspace owns the provider implementation, Summary reads, bounded
replay, coverage proofs, VChannel validation, WAL shutdown, recovery and retention.
Each acquired stream has its own lifetime: closing it must not close the SN's
shared bootstrap stream or block WAL observation. Subscription IDs must identify
independent subscriptions within that stream. Caller cancellation must unblock
pending reads and close their handlers. A WAL without the provider fails stream
creation explicitly, before the remote client considers the assignment usable.

The remote service adds a readiness header after acquiring the local stream.
Subscription errors preserve invalid-option, truncated-history and unavailable-
VChannel reasons. Disconnects resume subscriptions; these terminal semantic errors
are surfaced to the consumer. An empty bounded interval is permitted by the shared
interface; the provider still owns proof that its end is readable.

Remote/QN retention remains a local-provider prerequisite. The agreed first policy
is conservative retention of active VChannel history across disconnections and
recovery, until QueryView/DataView requirements are wired. SN retained-segment
`SetQueryRetention` alone does not prove that remote/QN history is safe to release.
This remote-only change does not modify Summary GC or install retention pins.
Tests mock the local interfaces and run the production gRPC service, client,
resumption and QN buffer; they do not claim local-provider or retention validation.
