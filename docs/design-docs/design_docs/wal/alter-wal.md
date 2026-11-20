# AlterWAL without logical WAL recovery

## Contract and ownership

`autowalimpls` composes physical WAL implementations into one logical PChannel
log. A normal AlterWAL neither closes the logical WAL nor rebuilds its
RecoveryStorage, WAB, query runtime or scanner. Actual writer loss still requires
normal startup recovery.

AlterWAL is a low-frequency maintenance operation. Deployment procedures enforce
the upgrade and legacy-history preconditions below, so the implementation does
not retain compatibility paths for the old switch behavior. These operational
constraints do not relax durability, ordering, or failure-recovery guarantees.

Query availability during the maintenance window is an accepted tradeoff.
QueryMVCC publication precedes target preparation and old-boundary persistence.
If target opening or persistence stalls, queries selecting the published switch
frontier, including Eventually reads, may wait or time out until consumption
catches up. Existing reads bound to an earlier snapshot need not wait for the
switch. Keep the publication-before-consumption ordering; this low-frequency
operation does not add snapshot fallback or a publication gate on WAL consumers
to preserve read availability during a stalled switch.

For an AlterWAL timestamp T, business messages before T belong to the old WAL;
business messages after T belong to the target WAL. The old AlterWAL record is
both a durable prefix confirmation and a link to the target. Data persistence
is triggered asynchronously; it is not a prerequisite for admitting new writes.

The existing PChannel exclusive append lock supplies business-write exclusion.
The TimeTick layer supplies synchronization with periodic Sync and publishes the
old acknowledged batch. The wrapper owns physical handles, append routing,
scanner handoff and truncation routing. Auto is constructed from the checkpoint
backend before recovery and owns all physical handles throughout its lifetime.
RecoveryStorage owns startup traversal and explicitly selects each recovery writer
through auto before writing that leg's RecoveryBarrier.

Auto has one current RW handle for current reads and writes, and a collection
of historical reader handles. A historical reader may be RO or a retired RW
handle. Selecting a new writer moves the old current handle into that collection
without closing or reopening it; switching back reuses a retained RW handle.
There is one historical handle per backend, and current is excluded from that
map. RO handles are never used for append: when a backend previously opened for
historical reads becomes the writer, acquire an RW handle, replace the cached RO
handle and close it. Physical RO handles do not own shared opener resources or
scanner connections; existing scanners continue independently and close their own
resources on handoff or subscription shutdown. A failed RW open leaves the RO
handle in place. A prepared target is owned by the map until selected as current.
Other historical handles remain cached for reuse until auto closes, which closes
them and current exactly once. There is no idle eviction or reference counting.
A reader-only auto has an RO current handle and does not admit writes.

## Durable link and target position

Each hot-switch AlterWAL carries `AlterWALMessageHeader.target_start_message_id`
(field 3, `common.MessageID`): the target's durable start anchor S. Its WALName
must match the header's target. The local WAL fills this field before persisting
its own PChannel's boundary, preserving the broadcast header's target/config.
The header is re-encoded through `OverwriteHeader`, so the anchor survives WAL
serialization. Readers decode through BasicMessage properties so scanner
wrappers preserve the same interpretation. The additive optional field preserves
existing field numbers and native ID encodings.

Before appending the old-end record, prepare a target-local persisted TimeTick(T)
with self LastConfirmed and record its ID as S. This anchor is not a recovery
operation or business event. A scanner following the link starts *after* S;
a target-local LastConfirmed reader may start *at* S. Thus the linked stream
delivers AlterWAL(T) once, without a second confirmation at T. Reusing a backend
after an intervening switch starts at the new S, never at its historical earliest
or its moving latest position. An unreferenced prepared anchor is not a switch.

Before the first hot switch, RecoveryStorage publishes recovery magic 4 through
its normal owner-fenced checkpoint publisher, without advancing the data point.
Older binaries reject this magic rather than interpreting a linked record as a
legacy checkpoint-reset operation. A failed publication prevents the switch.

Upgrade operations prohibit AlterWAL during the upgrade and enable hot switching
only after all relevant nodes understand the linked format. Operations also
ensure that post-upgrade subscriptions do not depend on backends retired by a
legacy switch or traverse legacy switch records. These are deployment
preconditions, not runtime compatibility detection: no backend-membership list
is persisted or consulted. From/After subscriptions select their physical
backend directly and follow the target anchors in the retained linked history.

`AlterWALState` and its FLUSHING / ADVANCE_CHECKPOINT stages are deprecated.
Their protobuf types and field numbers are retained for decoding old metadata;
the runtime neither creates nor resumes that state machine, resets checkpoints
to a target's earliest position, nor reopens the logical WAL for a switch.
Deployments must finish legacy switches before upgrade and must not require
replay across a legacy switch. Startup follows durable links directly. Every
reader must understand those links before enabling hot switches; rollback also
requires a compatible reader.

## Append and synchronization

1. Serialize AlterWAL with the entire ordinary Sync operation (sampling,
   physical append and WAB publication), then acquire the PChannel exclusive
   lock. Never wait under this lock for a Sync blocked on the same lock.
2. Drain preceding appends, allocate T, fence old segment allocation, and publish
   the old acknowledged prefix's query targets before the boundary is visible.
3. Prepare S, persist AlterWAL(T,S) in the old WAL, then select the new writer.
   The control link remains one complete physical record even when business-message
   chunking is enabled. `autowalimpls` retries target opening, target anchor
   append and old-end append independently; completed stages are not repeated.
   Each append retry retains its backend, TimeTick and chunk identity. Once the
   old boundary is durable, failure cannot reopen the old prefix for business writes.
4. End unfinished transactions in the exclusive scope. Publish the old ACK batch
   followed by AlterWAL(T) into the existing WAB. Do not discard acknowledged
   data when draining the manager.
5. Reset the AckManager's pending state and the Sync operator's published batch,
   retain the timestamp watermark T and set the new LastConfirmed baseline to S.
   Invalidate the adaptor's cached chunk budget while this exclusive lock is
   still held, before a waiting append can use the newly selected backend.
   Release exclusion and admit new appends.

```mermaid
sequenceDiagram
    participant A as WAL append / TimeTick
    participant O as Old backend
    participant N as Target backend
    participant R as RecoveryStorage
    Note over A,R: First switch only: publish reader capability magic 4
    Note over A,N: Sync exclusion + PChannel exclusive append lock
    A->>N: Persist anchor S at T
    A->>O: Persist AlterWAL(T, S)
    A->>A: Select target writer; publish old ACK batch + boundary
    A->>A: Reset native LastConfirmed to S; release locks
    A->>N: Business writes with TimeTick > T
    O-->>R: Ordered AlterWAL observation
    R->>R: Request asynchronous Summary / segment / L0 flush
    Note over R: Publish safe checkpoint only after completion
```

The adaptor records trace around its call into `autowalimpls`. One span covers
the complete internal retry sequence. A successful return is never changed into
an injected append error: Forward Recovery requires ordinary physical errors to
remain inside retry-until-success while the writer is alive. Lost-acknowledgement
unit tests inject failures at the mocked physical backend after recording the
message. `autowalimpls` owns backoff, retry metrics, failure logs and
writer-switch logs, and receives raw physical implementations with no observation
wrapper. A physical backend's lost acknowledgement is retried on that same
backend. Only fencing or cancellation of the WAL-owned context ends the retry
loop; an individual backend attempt timeout is retriable. Recovery probes use
the same auto append path; completing recovery does not reopen or transfer writers.

The old AlterWAL keeps its old native MessageID and LastConfirmed. Subsequent
messages have target-native IDs and target-native LastConfirmed. No Ack heap or
serialized `_lc` mixes backend encodings. Clearing the timestamp heap alone is
insufficient: pending transaction IDs, acknowledged details, operator details
and the retained LastConfirmed scalar all participate in the handoff.

## Scanners

The physical scanner forwards a complete AlterWAL, closes that physical reader
and continues after S. Outer chunk assembly, ordering and transaction processing
remain one continuous logical stream. AlterWAL confirms the prefix through T,
ends uncommitted transactions and remains an internal control event. Simply
treating it as a TimeTick while bypassing its exclusive effect is incorrect.

After forwarding a cross-backend boundary, auto retries target backend opening
and reader creation with exponential backoff until success or scanner cancellation.
Every attempt uses StartAfter(S); a successfully opened backend is reused. The
same logical scanner stays alive without replaying the old prefix or boundary.
The previous physical reader is closed when the target reader is ready, or when
the logical scanner closes. Invalid links and non-advancing boundaries still fail
validation before handoff. Reconnection within an established physical reader
remains the responsibility of its backend implementation.

The scanner adaptor first enqueues AlterWAL in the reorder buffer, then confirms
through its TimeTick. The boundary is delivered with its ordered prefix without
waiting for another TimeTick. The reorder buffer also rejects non-TimeTick retry
copies at its confirmed TimeTick after popping the original records and removing
their deduplication entries. Ordinary TimeTick and old-format records retain
their existing same-TimeTick behavior; the adaptor keeps no switch-specific
deduplication state.

A confirmation covers the entire PChannel even when the cluster broadcast copy
carries a PChannel or control-channel name in its VChannel property. Collection-
scoped readers therefore retain it for ordering and transaction processing;
ordinary records still require a matching VChannel. Public delivery filters
apply after the confirmation is projected as described below.

Only the internal recovery stream delivers the complete AlterWAL to persistence
and broadcast ACK consumers. Public WAL subscriptions receive a TimeTick at the
same T with the original native MessageID and LastConfirmedMessageID. This is an
output projection, not another WAL append; it does not mutate the shared WAB
record or expose the target/config/broadcast header. Query pipelines retain their
confirmation progress, while CDC treats the projection as a self-controlled tick.

Ordering, physical deduplication and exclusive transaction effects finish before
one delivery stage applies that projection and subscription filters. GT/GTE
therefore apply to the public confirmation as usual; message-type filters retain
ordinary system TimeTicks. Raw transaction input still uses its existing commit-
timestamp and body-type filtering semantics. The implementation does not move
that raw filter unchanged onto assembled transactions. Pure confirmation records
are appended once after processing the ordered prefix; exclusive controls already
arrive through the reorder buffer. No switch-specific DeleteFunc or delivery
watermark is needed. Physical catchup and WAB tailing share this path.

From/After options select the historical backend using their position. In
particular, StartAfter(old AlterWAL) must inspect the excluded boundary for routing
before excluding it from delivery. Reader creation does not acquire a new writer
on a historical backend. Physical Open is serialized separately from the mutex
used to select the current writer: slow historical Open does not block ordinary
Append or WALName. Waiting for open admission is cancellable. Concurrent opens
must not create duplicate RW handles that fence each other. A handle returned
after Close is closed instead of installed.

`All` starts at the earliest retained record of the physical backend selected
when the read is opened. For autowalimpls this is the current backend. It follows
any subsequent linked boundaries but does not discover earlier backends or
promise all historical records of the logical PChannel. Recovery and historical
subscriptions that must span a prior switch use From/After with a checkpoint's
MessageID. No historical backend catalog is maintained for `All`.

Latest delivery is deprecated and rejected synchronously by the consumer and
WAL read entry points. It has no production caller in this repository; the old
backend test harness now verifies rejection. Use All or an explicit From/After
position. Auto therefore needs no Latest-specific generation, completion channel
or reader reinitialization loop.

WAB eviction may fall back through an old LastConfirmed
and follow the same chain. Missing retained history is an error, not permission
to jump to the current writer. Truncation uses the checkpoint's backend and never
invalidates an uncompleted recovery prefix merely because writes switched.

## Startup recovery

The data checkpoint selects the first physical writer, which is immediately
owned by autowalimpls. RecoveryStorage uses this same auto during recovery and
live operation. There is no tail-query API or separate chain-discovery scan.

1. Acquire/fence the checkpoint backend's writer and append a RecoveryBarrier.
2. Replay with the ordinary scanner's decoding, chunk assembly, ordering and
   transaction semantics. At a cross-backend AlterWAL, pause physical input
   before reading anything beyond that marker. Observe and dispatch the old
   prefix through the boundary, including transaction abortion and async flush.
3. RecoveryStorage calls the builder's SwitchWAL, which selects the linked target
   through `auto.SwitchWriterForRecovery`. Auto opens/fences or reuses the writer
   without writing an anchor or another AlterWAL. The builder then appends a new
   local RecoveryBarrier through auto and rebuilds the recovery reader after S,
   with the new exact barrier as its startup pause point. Reallocate
   the barrier timestamp after acquiring that writer, and use target-local
   LastConfirmed. Each leg's expected barrier is identified by its exact native
   ID, WAL name and timestamp. The old leg stops at AlterWAL and does not consume
   its later probe barrier. If the chain returns to that backend, the old probe
   may appear after the return anchor; it is ordinary historical confirmation,
   never the new leg's completion marker.
4. Repeat until the current leg reaches its own RecoveryBarrier. At an AlterWAL
   boundary all old transactions and incomplete chunks have ended, so a physical
   leg can release its scanner; the RecoveryStorage component state and ACK
   handles remain continuous. A same-backend AlterWAL creates no new leg.
5. After bounded replay and the snapshot succeed, auto already has the final
   writer selected. Initialize WAB, MVCC and append interceptors from the final
   barrier. The final scanner stays paused, retaining its transaction buffer.
   Bind its live WAB, then release startup input and write admission. The same
   auto stays in place; no handle transfer, data replay or writer reopening occurs.

`SwitchWriterForRecovery` is a startup-only operation, called before business
append admission. Reading historical AlterWAL messages never changes the shared
writer; RecoveryStorage explicitly selects it after ordered observation. Runtime
switching still occurs only through Append(AlterWAL). Auto owns the initial and
subsequent physical handles, reuses writers on round trips, and closes them on
failure or shutdown. The builder keeps no separate writer cache or ownership flag.
The safe data checkpoint may still belong to an older WAL; recovery never rewrites
it to the target's earliest ID.
RO opening does not run this recovery protocol; it constructs a reader-capable
autowalimpls directly and follows historical read options and durable links.

## Persistence and failure handling

RecoveryStorage observes AlterWAL through its existing ordered path. It requests
Summary coverage through T, flushes older segments and materializes L0 data.
Existing retained handles and Summary.LastAcked gate the safe published
checkpoint. A new writer may be active while the checkpoint remains old. A failed
upload or metadata publication must not advance that checkpoint.

| Failure point | Recovery |
| --- | --- |
| Target anchor only | No committed link; old prefix remains authoritative |
| Old AlterWAL durable, writer not exchanged | Replay follows S and completes the same switch |
| New writes acknowledged, old data not flushed | Replay old suffix and new suffix from the original checkpoint |
| Ordinary Sync races cutover | Serialization finishes it before the boundary; no stale batch enters the new generation |

Validation covers empty/nonempty ACK state, unfinished transactions, concurrent
Sync, zero subsequent writes, chunked messages, From/After at the boundary, WAB
fallback, repeated/round-trip switching, all durable-boundary crash windows and
failed persistence. Broadcast ACK completion continues to follow retained handle
completion; changing its public completion contract is outside this refactor.
