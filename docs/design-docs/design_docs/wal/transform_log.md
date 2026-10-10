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
Summary's resident chunk index owns a shared, byte-budgeted LRU of encoded
objects, populated by both successful reads and writes. Pagination and different
VChannel subscriptions reuse the same chunk buffer while resident. Cache eviction
does not remove index entries or change delivery cursors; live subscriptions may
read object storage again after eviction. See [chunk cache](summary.md#541-resident-index-and-chunk-cache).

The qv branch's stream protocol and consumers are the reference for external
behavior. Its independent VChannel storage and retained-message write path are
not part of this design.

## 2. Subscription Interface

The qv interface shape is retained:

```text
AcquireStream(PChannel)
  -> Subscribe(VChannel, StartAfterTimeTick, optional EndTimeTick, Handler)
       -> DeleteEntry / SyncUp / Error
```

One stream may carry several VChannel subscriptions. A subscription reads
strictly after its start cursor. An unset end means continuous delivery; a set
end means bounded replay through that position. Stream closure releases all
subscriptions; closing one subscription does not close a shared stream.

The planned QueryNode integration uses continuous subscriptions to catch loaded
sealed Segments up and then apply live Deletes. StreamingNode uses bounded subscriptions when preparing
growing resources from a captured WAL view; subsequent resource events arrive
through the VChannel's ordered live event path. Both consumers are specified to use the same Summary-backed read semantics.
The current adaptor clamps an older start to Summary's lower bound and delivers
the retained suffix. It does not emit a separate FastForward event or a truncated
start error. A bounded interval wholly at or below the lower bound completes
with SyncUp at its requested end and no entries. This is an explicit compatibility
policy, not proof that skipped Deletes were applied; reliable retention remains
[TODO](summary.md#todo-reliable-retention-for-query-recovery).

## 3. Entry And SyncUp Semantics

DeleteEntry carries Delete payloads at the source WAL TimeTick. A committed Txn
uses its outer TimeTick and delivers its Delete children as one ordered entry.
Pure Inserts do not produce transform entries. Other ordered messages may
establish progress without producing payload records.

`SyncUp(T)` means every retained Delete after the adjusted start through T has
been delivered successfully. It makes no claim about history below Summary's
lower bound. It has two responsibilities:

1. **Advance through intervals without Deletes.** Relevant DDL/publication
   events, insert-only transaction commits and WAL recovery barriers can advance
   query Transform MVCC without producing a Delete Entry. SyncUp supplies the
   complete-prefix evidence needed to advance the consumer through those empty
   intervals. Applicable PChannel-wide barriers align all affected subscriptions;
   unrelated VChannel traffic does not require a broadcast.
2. **Signal catch-up for view readiness.** An unbounded subscription reports
   when delivery has reached the latest readable frontier sampled by its read.
   After applying the preceding entries, a view can use this signal to establish
   that it has caught up, rather than enter service with historical TransformLog
   backlog and make its first queries pay that replay cost. QN readiness wiring
   remains planned in this extraction.

Delivery and application are distinct. A successful Handler call may only enqueue
work. Consumers must apply the complete ordered prefix before advancing their
applied MVCC or accepting SyncUp as a readiness signal. A whole Delete Entry at T,
including every Delete child of a transaction, can itself advance applied MVCC
to T after all preceding entries have been applied. Continuous Deletes therefore
do not need an extra SyncUp after each Entry to make progress. SyncUp supplies
empty-interval progress and catch-up evidence beyond that per-Entry progress.

For example, applying Delete(200) in order already advances Delete visibility
through 200; an earlier SyncUp(100) adds no later MVCC coverage. Conversely, if
the last Delete is at 100 and a relevant barrier advances Transform MVCC to 200,
SyncUp(200) supplies the missing coverage proof without inventing a Delete.

SyncUp does not prove Summary persistence, completion of other WAL effects, or
L0 materialization. In particular, it does not implement the deferred DDL
visibility effects. SyncUp and payload-free Barriers are not stored as records.

The adaptor derives progress from Summary's complete readable coverage, not the
largest Delete TimeTick or a requested end position. Subscription delivery is
independent of the L1 safety bound used by the separate materializer.

## 4. Catch-Up And Live Delivery

Each read captures readable coverage and a change token atomically, then returns
a bounded batch through that coverage, capped by EndTimeTick when set. Deliver
all entries in the batch before advancing the delivery cursor or sending SyncUp.

| Subscription | SyncUp condition after delivering the batch | Next action |
|---|---|---|
| Bounded | CoveredThrough reaches EndTimeTick | Send SyncUp and complete the subscription |
| Unbounded | CoveredThrough reaches that read's sampled ReadableThrough | Send SyncUp and wait on the captured change token |
| Either, with a page limit before the required boundary | No SyncUp for this page | Read the next page immediately |

Snapshot capture and change registration must avoid lost wakeups. Moving data
from pending to sealed to durable storage must not create subscription gaps or
duplicates. A relevant change during I/O or delivery invalidates the captured token so
the next wait can resume immediately; it does not invalidate the coverage just
delivered. New writes after the read's snapshot therefore do not prevent that
batch from sending SyncUp for its sampled frontier.

The unbounded adaptor samples current coverage on each page. There is no
requirement to freeze an older target across pages merely to force an early
SyncUp. If sustained Delete backlog prevents the subscriber from reaching each
sampled frontier, SyncUp may be delayed while ordered Entries continue advancing
applied MVCC. That distinction is intentional: completing an arbitrary older
prefix is not the same signal as catching up to the sampled tail.

A SyncUp is a point-in-time catch-up observation, not a promise of zero lag at
every later instant. ReadableThrough belongs to Summary's observed complete
prefix, which can lag the write-side query MVCC. The consumer's readiness
protocol must relate its applied frontier to the required query/recovery
boundary; it must not equate receipt alone with being current. New writes, slow
application and the interval between Ready and Up can still require per-query
MVCC waiting. Catch-up avoids exposing historical replay backlog, not every
possible future wait.

A bounded subscription completes only after coverage reaches its EndTimeTick.
If the end is beyond Summary's lower bound and readable coverage has not reached
it, the subscription waits or reports an explicit failure; an empty retained read
is not successful completion. The lower-bound compatibility rule only skips the
unavailable prefix, never the retained interval still required through the end.

### Unbounded subscriptions for QueryNode

The local adaptor's unbounded mode uses WALSummary's VChannel-scoped transform
notifications. This prepares the server-side behavior for QueryNode; remote
transport and QN integration remain outside this extraction. Bounded SN bootstrap
keeps its existing global-coverage notification behavior.

The QN contract is to wait for the VChannel `transforming_timetick` selected by
the query plan. Ordinary messages on B do not advance A's query Transform MVCC,
so they need not wake A or produce a new A SyncUp merely because the global
Summary coverage increased. Initial historical reads still use complete global
coverage; after catch-up, A wakes for its own query-transform changes or an
applicable global barrier. It reads and delivers the required Delete prefix
before reporting SyncUp. QN must finish applying that prefix before advancing
its local visibility; receipt of a notification is not query readiness.

The planned QN view preparation uses this applied catch-up signal, together with
segment loading and the required MVCC boundary, before reporting Ready. Loading
the base segments or receiving a SyncUp while its preceding Deletes remain
queued is insufficient. This consumer-side readiness integration is not yet
implemented by the local adaptor.

Notifications cover payload-free changes too: insert-only transaction commits,
flush/import publication, relevant DDL and schema changes. Their classification
is shared with query-plan MVCC advancement. PChannel-wide FlushAll/AlterWAL and
recovery baselines remain broadcasts. GC truncation wakes readers to advance
to the retained boundary; terminal failures wake them to fail explicitly rather
than wait indefinitely.

An unbounded subscription acquires one scoped-notifier reference and releases
it on cancellation, closure or failure. Multiple subscribers share notification
state, but still have independent delivery cursors and reads; this change does
not introduce shared payload decoding or eliminate subscription goroutines.
The optimization promises progress to VChannel query Transform targets, not
continuous delivery of every unrelated global TimeTick. A future consumer that
requires an arbitrary PChannel target must use global notifications or add an
explicit progress request protocol.

Use bounded work and delivery buffers. A slow subscriber must not block WAL
observation or create an unbounded adaptor backlog. A stream that cannot keep up
may be closed and resumed from its accepted cursor. Sharing reads for live
subscriptions to the same VChannel avoids decoding the same records repeatedly.

## 5. Resume And Failure

Local and remote transports expose the same contract. After transport failure,
the client reacquires the PChannel stream and resubscribes exclusively after the
last position its handler successfully accepted. Entry and SyncUp advance that
cursor; failed handler calls do not. The server applies its lower-bound policy
again on reconnect.

No durable consumer ACK or cross-process exactly-once guarantee is introduced.
A caller recovering its own state must select a cursor consistent with that
state. If the recovered server has not yet reconstructed a previously delivered
position, it must not manufacture coverage from the resume request.

Invalid options and an unavailable VChannel are explicit semantic errors.
When Summary returns `FastForwardTimeTick`, the adaptor accepts that boundary
and delivers the returned suffix without rereading it. Cursor advancement is
capped by EndTimeTick for bounded subscriptions, including an entirely skipped
interval. The same policy applies when GC overtakes a live cursor. Missing or
corrupt retained objects still fail the read; they are never converted into empty
history, fast-forward, or SyncUp. Correct replay of all query-required effects
depends on the reliable-retention TODO, not on accepting an older cursor.

### Local read recovery

The SN adaptor retries temporary and unclassified `ReadTransform` failures in
the same subscription, including ordinary `ErrIoFailed` and operation-local
timeouts. It uses exponential backoff with a 100 ms initial interval, a 10 s
maximum base interval and jitter, without a retry-count or elapsed-time limit.
A successful read resets backoff; successful reads allocate no retry timer.
Retries emit rate-limited warnings with the unchanged read cursor.

Each failed read returns no deliverable batch. The adaptor keeps its previous
cursor, delivers no partial entries and sends no SyncUp for that attempt. It
retries only the read, not handler delivery or Segment application. After success,
it delivers complete entries before advancing coverage, following the same
bounded/unbounded rules as a read that never failed.

The retry loop sits outside `ReadTransform`, releasing its snapshot lock and
local GC pin before waiting. Chunk-cache and Store reads retain their single
attempt semantics. Subscription cancellation and stream closure cancel both
backoff and context-aware storage reads. Only the subscription context ending
means cancellation; a timeout or cancellation of a lower-level attempt while
that context is active remains retryable.

Known store corruption (`ErrStoreCorrupted`), data-integrity failures and a
missing object still referenced by the retained index terminate the read
explicitly. Summary's terminal state also returns `ErrStoreCorrupted`; it must
not enter an indefinite retry loop. These errors and consumer-handler failures
keep the existing terminal notification path. Unknown read failures are not
classified as permanent merely because the generic error lacks a retryable flag.

Remote Provider integration must preserve this distinction: owner shutdown,
migration and PChannel unavailability end the physical stream for PChannel-level
reconnection; only explicit logical terminal failures become SubscriptionError.
That transport wiring remains outside the local SN implementation. Segment
application failure handling is unchanged by read recovery.

This retry policy does not alter the lower-bound compatibility rule above or
implement reliable retention. It preserves delivery across transient failures
within retained history; it cannot reconstruct Deletes already retired by GC.

### Deferred: NodeScheduler task priorities

TODO(#40451, explicitly deferred from this PR): introduce task priorities in
NodeScheduler to protect persistence and recovery progress from long-running
query-resource preparation. Bootstrap currently waits synchronously for bounded
TransformLog replay inside a shared NodeScheduler task. Although the subscription
reads in its own goroutine, the preparation task retains its worker until replay
finishes or is cancelled. When enough VChannels need uncached historical chunks
and those reads keep failing, their preparation tasks can occupy all workers and
delay otherwise runnable Summary chunk/manifest and other recovery tasks.

This is an availability/performance limitation under sustained read failures,
not a violation of Delete delivery, cursor ordering or SyncUp coverage. Reads can
resume when storage recovers; owner cancellation stops the retry. Keep the
current retry behavior in this PR.

The future priority design must address workers already occupied by waiting or
retrying tasks, through cooperative yielding or execution-capacity reservation;
prioritizing queued tasks alone cannot free an occupied worker. Preserve the
same subscription and its accepted replay progress, cancellation and resource
cleanup semantics. Do not implement priority by turning temporary read failures
into terminal subscription errors or repeatedly rebuilding the whole runtime.
Validate that persistence still progresses while enough bootstrap reads fail to
saturate the pool, followed by recovery without missing Deletes or premature
SyncUp and prompt cancellation during backoff.

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
4. SyncUp claims complete delivery only after the adjusted start; it does not
   certify history skipped below the retained lower bound.
5. Historical/live handoff and storage transitions lose no retained records.
6. Subscription cursors do not advance L0 materialization or authorize GC.
7. L0 materialization does not depend on this adaptor or on external subscribers.
8. Applied MVCC and view readiness require ordered consumer application;
   delivery progress alone is insufficient. Delete Entries can advance applied
   MVCC before catch-up, while SyncUp additionally supplies empty-range progress
   and the subscription's completion/catch-up signal.

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
