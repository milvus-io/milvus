# TransformLog Subscription Adaptor Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** The local SN bootstrap adaptor is implemented in
`internal/streamingnode/server/wal/walsummary/stream.go` and wired through
`vchannel.PChannelRecoveryManager` into GrowingRuntime. It performs bounded Delete replay for
QueryRuntime preparation. Remote transport and QN continuous-subscription
integration remain planned; the general subscription contract below includes
those future consumers.

TransformLog is a read-only subscription adaptor over
[WALSummary](summary.md#54-transform-read-contract). It owns no record storage,
has no `ObserveMessage`, and does not materialize L0. The current WAL L0
materializer retains WAL handles independently; the retained Summary-based L0
consumer is not enabled by this adaptor. See [L0 Materializer](l0_materializer.md).
The former `vchannel/transformlog` storage package has been removed.

## 1. Ownership

```text
WALSummary (one store per PChannel)
  +-- Summary L0 consumer               [retained, not wired]
  +-- TransformLog subscription adaptor
        +-- local SN bounded replay     [implemented]
        +-- remote / QN subscriptions   [planned]
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

The planned QueryNode integration uses continuous subscriptions to catch loaded
sealed Segments up and then apply live Deletes. StreamingNode uses bounded subscriptions when preparing
growing resources from a captured WAL view; subsequent resource events arrive
through the VChannel's ordered live event path. Both consumers are specified to use the same Summary-backed read semantics.
The current SN adaptor rejects a skipped historical interval with
`ErrTransformLogStartPointTruncated`; it does not expose successful fast-forward
to GrowingRuntime.

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

QueryView/DataView integration must protect
the historical start points needed for future Segment loads, reconnects, and
local bounded replays. Already delivered data can still be needed by a retained
view; subscription delivery is not permission to delete it.

The owner of those view requirements reports retention constraints to Summary.
TransformLog does not own object deletion or infer view lifetime from a stream's
cursor. Unknown requirements during recovery are not equivalent to no readers.
New requirements must be installed before GC can remove the requested history.
For local SN preparation, `VChannelRecoveryModule.refreshQueryRetentionLocked`
publishes the retained segments' earliest Delete replay requirement through
`WALSummary.SetQueryRetention`. Remote/QN consumers require their own integration.
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

## 8. TODO: DDL Visibility Entries (Outside This PR)

The agreed [Truncate and Partition Drop visibility TODO](../qviews/ddl_visibility.md)
extends TransformLogEntry with explicit TruncateCollection and DropPartition
payloads. All affected SN/QN consumers would use their ordered application and
request MVCC to exclude invalidated segments. This requires extending the
current Delete-only delivery/SyncUp coverage contract; payload-free barriers
are insufficient. Until the distributed protocol is implemented, retain the
QueryView handoff visibility fence described there. No entry, transport or
runtime implementation is added by this design note.

## 9. TODO: Bound SN Bootstrap Consumer Memory (Deferred)

The current local subscription bounds the replay interval and the adaptor's
delivery batches, but not GrowingRuntime's total bootstrap replay buffer.
`growingruntime.drainDeleteReplay` collects every entry in the requested interval
before `Runtime.Prepare` applies them. Its additional retained payload therefore
grows with the entire replay interval, despite WALSummary's paged reads and the
handler's bounded event channel.

This optimization is explicitly deferred from the current PR. A follow-up should
apply entries incrementally to the unpublished runtime, preserving partition
scope, transaction commit timestamps and ordered application. Only complete
replay through the target SyncUp may publish the preparation frontiers. A read or
apply failure must discard the partial runtime and use the existing fresh-build
retry path; cancellation must unblock delivery before waiting for subscription
shutdown.

The intended bound covers the current read batch, bounded queued entries and one
active entry. A single oversized entry or transaction can exceed the batch soft
limits. This does not bound WALSummary's own retained data or the delete state
that segcore must retain. No new disk cache or per-version resources are planned.
Validation should cover multi-page replay, midstream failures, cancellation with
a full queue and peak extra replay memory. The current implementation remains
unchanged by this TODO.
