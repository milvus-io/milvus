# TransformLog Subscription Adaptor Design

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** The local SN bootstrap adaptor is implemented in
`internal/streamingnode/server/wal/walsummary/stream.go` and wired through
`vchannel.PChannelRecoveryManager` into GrowingRuntime. It performs bounded Delete replay for
QueryRuntime preparation. Remote transport and QN continuous subscriptions use the same reader through
`PChannelRecoveryManager.AcquireStream` and `SubscribeTransform`. Each acquired
stream owns its subscriptions and closes on client cancellation or PChannel
shutdown.

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
        +-- remote / QN subscriptions   [implemented]
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

The qv stream protocol and consumers share this adaptor. The former independent
VChannel storage and retained-message write path are not used.

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

QueryNode integration uses continuous subscriptions to catch loaded
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
   events and WAL recovery barriers can advance query Transform MVCC without
   producing a Delete Entry. SyncUp supplies the
   complete-prefix evidence needed to advance the consumer through those empty
   intervals. Applicable PChannel-wide barriers align all affected subscriptions;
   unrelated VChannel traffic does not require a broadcast.
2. **Signal catch-up for view readiness.** An unbounded subscription reports
   when delivery has reached the latest readable frontier sampled by its read.
   After applying the preceding entries, a view can use this signal to establish
   that it has caught up, rather than enter service with historical TransformLog
   backlog and make its first queries pay that replay cost. QN readiness wiring
   is implemented by the qv segment readiness manager.

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
notifications. The qv branch exposes this through SubscribeTransform and the QN
VChannel buffer. Bounded SN bootstrap
keeps its existing global-coverage notification behavior.

The QN contract is to wait for the VChannel `transforming_timetick` selected by
the query plan. Ordinary messages on B do not advance A's query Transform MVCC,
so they need not wake A or produce a new A SyncUp merely because the global
Summary coverage increased. Initial historical reads still use complete global
coverage; after catch-up, A wakes for its own query-transform changes or an
applicable global barrier. It reads and delivers the required Delete prefix
before reporting SyncUp. QN must finish applying that prefix before advancing
its local visibility; receipt of a notification is not query readiness.

QN view preparation waits for base segment loading and application through the
buffer's catch-up frontier before reporting Ready. Loading base segments or
receiving SyncUp while preceding Deletes remain queued is insufficient. The
readiness manager and TransformLogBuffer implement this consumer-side gate;
per-query checks separately wait for the query plan's required MVCC boundary.

Notifications cover payload-free changes too: flush/import publication, relevant
DDL and schema changes. Pure Insert transactions advance only GrowingMVCC and
do not notify Transform subscribers. Their classification is shared with
query-plan MVCC advancement. PChannel-wide FlushAll/AlterWAL and
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

### VChannel Query Barriers

Primary WAL first checks its confirmed Growing and Transform MVCC. If both
cover B, query planning bypasses Inspector. Otherwise
`TimeTickInspector.EnsureBarrier(pchannel, vchannel, B)` queues a scoped request
and actively triggers Sync. Inspector coalesces pending targets, remembers sent
frontiers per WAL owner and VChannel, and completes callers once the confirmation
has been appended and published to WAB. It never waits for RecoveryStorage or
query consumers. Its existing heartbeat retries transient failures or incomplete
ACK prefixes; callers introduce no polling. Ownership termination cancels waiters.
TODO: reclaim sent scopes of dropped VChannels under the unified GC policy;
currently unregistering the WAL owner releases the complete Inspector state.

Sync drives the allocator past the requested target and obtains an ACK-confirmed
prefix C. With scoped requests it emits a distinct `MVCCBarrier` confirmation
whose common `_vc` property remains empty (PChannel-level transport scope).
Its dedicated Header lists the affected `vchannels`; the message TimeTick is C.
Every listed VChannel receives the same complete-prefix frontier C; query plans
keep the requested snapshot B. The Body is empty, and no per-VChannel timestamp
is encoded. The common `_vc` encoding and single-channel APIs remain unchanged.
The message is persisted when the ACK batch contains persisted business messages, or a scheduled durable sync is
due; otherwise it uses the existing NotPersisted transport. Published target scopes
are carried on subsequent sync confirmations so idle coalescing and durable
catchup cannot lose a previously established scoped frontier. This state belongs
to Inspector, not WAB. WAB retains generic ordering and latest-confirmation
semantics; scanners drain preceding payloads and transactions before delivering
the new confirmation. Ordinary TimeTick generation, filtering and MVCC behavior
are unchanged, including filtering redundant non-persisted TimeTicks in a data
batch. Ordinary volatile TimeTicks do not establish query frontiers.

Keeping `_vc` unchanged preserves the existing scope encoding; it does not add
new-message support to older binaries. Producers and consumers that encounter
`MVCCBarrier` must understand this message type. New readers retain support for
existing TimeTick and RecoveryBarrier records. Mixed-version consumption by
binaries without MVCCBarrier support is not established by this protocol change.

RecoveryStorage passively observes ordered `MVCCBarrier` messages. It exposes
scoped Summary coverage and sends `QueryBarrierEvent` through the VChannel's
ordered resource events. Persisted MVCCBarrier confirmations retain full-prefix
resource effects. The Header scopes the extra advertised query-plan frontiers
and targeted query notifications. Ordinary TimeTick messages update PChannel
AckTracker and Summary coverage, then stop before VChannel dispatch; they do not
advance query execution frontiers. Pure Insert commits require only GrowingMVCC
progress and never rely on a later TimeTick to complete a Transform wait.
Empty transactions cannot be newly committed. Historical empty transactions
produce no Transform entry or notification and are filtered before VChannel
dispatch while their PChannel recovery progress is retained.
Volatile confirmations use only the scoped event path and never dirty checkpoints
or Summary manifests.
Neither form adds a payload-free TransformLog record. GrowingRuntime advances
both applied frontiers after preceding events; runtime rebuild and partition
refresh preserve the progress through snapshot capture and event replay.

Summary reads combine their normal coverage with the requested VChannel's
volatile floor. Existing paged delivery drains preceding Deletes before emitting
SyncUp. Only the target VChannel's continuous subscriptions are notified; bounded
bootstrap readers may also wake to recheck coverage. The floor remains available
to late subscriptions and is removed during VChannel cleanup. It is not a durable
acknowledgement, materializer boundary or permission to trim history.

The Iterator plan keeps Growing=B and Transform=B even when actual consumption
advances further. SN/QN must finish applying the prefix before executing B.
After WAL recovery, volatile barriers are re-established from the new owner's
ordered coverage; a resume cursor alone never proves coverage. This does not
extend historical data retention or add arbitrary time-travel guarantees.

## 5. Resume And Failure

Local and remote transports expose the same contract. After transport failure,
the client reacquires the PChannel stream and resubscribes exclusively after the
last position delivered to its handler. Entry and SyncUp advance that
cursor after Handle returns; Handle has no error return. The server applies its lower-bound policy
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

Temporary chunk reads retry inside WALSummary's shared load, so SN bootstrap,
remote subscriptions and idempotency readers waiting for the same chunk share
one retry sequence. The adaptor does not start a separate retry loop for each
subscription. Backoff uses a 100 ms initial interval, a 10 s maximum base interval
and jitter, without a retry-count or elapsed-time limit. Successful loads
allocate no retry timer. Generic I/O, unclassified failures and operation-local
timeouts remain retryable while the shared load is active.

A waiting read delivers no partial batch or SyncUp. Successful earlier chunks
in that batch do not have to be downloaded and decoded again when a later chunk
retries. After success, the adaptor delivers complete entries before advancing
coverage, following the same bounded/unbounded rules as a read that never failed.
Handler delivery and Segment application are never retried by this mechanism.

Each caller owns only its wait, not the shared load context. Subscription
cancellation or stream closure releases that subscriber without interrupting
other callers or resetting backoff. Once the final waiter leaves, the shared
context cancels both backoff and context-aware storage I/O. The last waiter joins
the task before releasing its local GC snapshot pin. Unlike the previous
subscription-level retries, the snapshot stays pinned during shared backoff;
sustained failures can defer this Manager's physical garbage collection until
reads recover or all waiters cancel. Download slots are released during backoff.

Known corruption (`ErrStoreCorrupted`), data-integrity failures, raw
`io.ErrUnexpectedEOF`, and permanent storage errors such as missing objects or
permission failures terminate the shared load and fail every remaining waiter.
Summary terminal-state errors and decoding failures also end the underlying
stream and publish its Done/Error; they are not handler events. Remote storage maps an interrupted
object download to retryable `merr.ErrIoUnexpectEOF`; that transient transfer
failure remains retryable. Bytes returned successfully still undergo framing,
checksum and index validation, so a damaged object is a permanent failure.
No error becomes an empty history or a successful SyncUp.

The resumable layer treats every underlying read, subscription or transport
failure as the end of one physical attempt and recreates all subscriptions from
their individual delivered cursors. It does not classify runtime failures or
pass them to business handlers. A persistent corruption therefore remains
observable in retry logs and blocks progress until repaired or the owner closes
the logical stream; it never produces fabricated data or coverage. Local callers
that use a Summary stream directly observe its Done/Error without resuming.
Segment application failure handling is outside this transport contract.

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

## 10. Remote Transport And Ordered MVCCBarriers

`WALAccesser.TransformLogStreamManager` opens a logical PChannel stream.
`HandlerClient.AcquireTransformLogStream` resolves its current assignment and
selects a local WAL accessor or a bidirectional `SubscribeTransform` RPC.
Creation metadata carries the PChannel name and term. After validating that
owner and acquiring its Summary stream, the server sends a stream-level
`CreateTransformStreamResponse` without waiting for a VChannel request. The
remote factory waits for this confirmation before returning success, like the
WAL consumer factory. A rejected owner/term therefore returns through the shared
HandlerClient assignment-error reporting and assignment-watch/backoff path.
Failure to send the confirmation closes the acquired Summary stream.

Subscription Create acknowledgements still precede all subscription events.
Multiple VChannels share one physical stream; each retains its own exclusive
cursor and optional end timestamp. Owner loss after creation is handled by the
existing resumable layer; its next factory call validates the owner again.
The handshake does not change logical subscription lifetime or accepted cursors.

The logical subscription filters events before Handle: entries at or below its
exclusive cursor and SyncUp below that cursor are discarded. SyncUp at an already
delivered SyncUp watermark is also discarded across physical attempts. Entry(T)
does not suppress the first SyncUp(T), including bounded completion; the first
SyncUp at the initial cursor (including zero) is still delivered. Each logical
subscription owns its own filtering state.
The resumable layer checkpoints Entries/SyncUp after Handle returns. On any
underlying failure it closes that attempt and resubscribes from those cursors,
aligning with WAL Consumer. Explicit Close ends the logical lifetime; canceling
one Subscribe call only removes that subscription. Factory cancellation or a
closed HandlerClient also ends resuming. Other factory errors, including
Unimplemented, use retry backoff. A Send EOF yields to Recv's final gRPC status
for diagnostics. Invalid local Subscribe options are rejected synchronously.

All client sends and CloseSend are serialized. Explicit stream close sends a
CloseStream request and waits for acknowledgement, bounded by five seconds.
Receive completion cancels only the physical RPC, without waiting for its send
lock. For an explicit subscription close the server disables event delivery
before canceling/joining its Summary worker, then acknowledges closure; this
internal cancellation must not escape as a SubscriptionError. Failed create
acknowledgements also release the event gate before joining the worker.

`GetQueryPlanRequest.barrier_mvcc` requests a fixed read timestamp on the primary
RW WAL. The TimeTick inspector coalesces per-VChannel requests and publishes an
MVCCBarrier after the acknowledged prefix reaches the requested timestamp.
The barrier carries target VChannels and traverses the same ordered WAB/scanner
path as confirmations. Durable barriers confirm the full WAL prefix; volatile
barriers advance only scoped query visibility and Summary read coverage.
WAB queues each scoped barrier, including volatile barriers, as an ordinary
ordered pending record; later TimeTicks cannot replace it. RecoveryStorage
observes preceding records before publishing barrier progress.
It does not use volatile barriers to advance durable checkpoints, seal chunks,
acknowledge handles or authorize GC. Scoped Summary notifications wake readers,
which deliver retained Entries before SyncUp. GrowingRuntime consumes the same
ordered query barrier to advance its observed query frontier.

After a confirmation has been observed by Summary and dispatched to the ordered
VChannel resource path, RecoveryStorage reports its consumed-through TimeTick
to WAB. That prefix becomes eligible for eviction; consumption alone does not
release cached records. Capacity overflow or keepalive expiry must also apply.
WAB retains the latest persisted confirmation separately for ordinary WAL
catchup handoff. Its suffix is not pinned: consumed volatile barriers follow the
cache policy, while all unconsumed records remain protected regardless of capacity/TTL. The
inspector therefore only needs pending scoped requests, not continuous replay
of all previously published scopes. Multi-scope barriers use their existing
batched header; no additional per-VChannel retention map is maintained in WAB.

WAB pressure is configured under `streaming.walRateLimit.writeAheadBuffer`:
`slowdownRatio` S (default 1.5, minimum 1) gives threshold C = instance capacity * S,
`slowdownRate` R (32MB/s), and `rejectRatio` Q (2). For queued bytes B, admission
is normal through C; rate is `max(1, floor(R*(Q-B/C)/(Q-1)))` between C and Q*C;
new DML is rejected at Q*C. B includes all retained cache bytes.
Append, consumption and live config updates all
re-evaluate the same curve immediately, even if the queue has no traffic.
There is no time-based ramp or additional control goroutine. The existing
multi-source WAL limiter combines WAB pressure with independent persistence-tail
pressure. Capacity never blocks WAB publication or evicts unconsumed barriers.

This guarantee is local to the WAL owner. A volatile barrier is not persisted;
owner recovery reconstructs progress through RecoveryBarrier. The consumed
boundary is neither an AckTracker completion point nor a durable checkpoint.
Startup retains the live suffix while recovery reads the durable prefix, then
the same scanner tails WAB. Other readers that lag behind a released prefix
can still use durable catchup, then join WAB at its retained durable confirmation
and receive retained messages and latest volatile progress without waiting for a
new WAL write. Readers already at or beyond the latest durable confirmation can
also rejoin after their offset expires, skipping only consumed volatile
confirmations. This does not replay every expired scoped barrier to ordinary
readers; the delivery guarantee is to RecoveryStorage and its ordered observers.

A query waits for the requested timestamp, not a later publication timestamp.
Before registering a scoped sync, planning waits for the TSO allocator to reach
the requested timestamp under the caller and WAL-owner contexts. An unallocated
future timestamp therefore cannot occupy the PChannel's shared sync operation
or prevent ordinary and durable confirmations. The request keeps its exact
timestamp; cancellation during this allocator wait does not enqueue the target.
Owner shutdown cancels pending planning and permits replanning on the new owner;
caller cancellation remains caller cancellation. Barrier state belongs to one
WAL registration and is rebuilt after owner replacement.

This transport does not implement QN TransformLogBuffer, DataView/QueryView
replay-frontier or retention wiring, read-proof/CAS, or a query-path switch.
Existing Delete/L0 behavior and the retained-history lower-bound contract stay
in force. Remote subscription progress alone is not a retention guarantee.

A closing physical stream reports a typed shutdown so new subscriptions can
reacquire an owner. SubscriptionError responses end the physical attempt, just
like an RPC failure; errors are never delivered through Handle. The resume loop
closes failed attempts outside receive callbacks. Reconnect backoff resets only
after an attempt remains available for at least ten seconds after its initial
subscriptions are created. Delivery by a healthy sibling, including repeated
SyncUp, does not reset the backoff of a repeatedly failing shared stream. New
subscriptions wait for an ongoing failure backoff; explicit Close cancels it.
The client acquires or resumes a physical stream only while logical subscriptions
exist. Closing the last subscription keeps an established stream available for
reuse for one minute; the background resume loop then retires it and sleeps
until a new subscription arrives. Local and remote streams use this same policy.
An acquisition still waiting for an owner or creation ACK is canceled as soon as
its final subscriber leaves. A subscription waiting for data is active and is
never retired just because no events arrive. Explicit stream Close ends the
logical lifetime and releases the current attempt immediately.
The server's execution loop owns sends and handles their failures. Summary
workers deliver data through a cancellable channel, and read failures wake the
execution loop through the Summary stream's Done/Error, even with no incoming
client requests. Explicit subscription Close releases delivery before joining
the worker and does not fail sibling subscriptions.
Subscription creation has a separate result from normal stream shutdown. Closing
before creation completes returns a shutdown error to Subscribe, while Close
itself can succeed. A successfully created subscription may subsequently close
without retroactively changing its creation result.

Handlers must independently abort blocked delivery before their caller waits
for subscription/stream Close; Close on a handler only signals completion.
A blocked remote handler delays all subscriptions sharing that receive loop.
Never synchronously close a subscription from its own Handle callback.
EndTimeTick SyncUp signals bounded completion, but the caller must still Close
the subscription. Only physical stream Close has the five-second timeout;
per-subscription Close joins delivery without a peer timeout.
