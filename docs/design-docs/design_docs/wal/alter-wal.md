# AlterWAL without logical WAL recovery

## Contract and ownership

`autowalimpls` composes physical WAL implementations into one logical PChannel
log. A normal AlterWAL neither closes the logical WAL nor rebuilds its
RecoveryStorage, WAB, query runtime or scanner. Actual writer loss still requires
normal startup recovery.

For an AlterWAL timestamp T, business messages before T belong to the old WAL;
business messages after T belong to the target WAL. The old AlterWAL record is
both a durable prefix confirmation and a link to the target. Data persistence
is triggered asynchronously; it is not a prerequisite for admitting new writes.

The existing PChannel exclusive append lock supplies business-write exclusion.
The TimeTick layer supplies synchronization with periodic Sync and publishes the
old acknowledged batch. The wrapper owns physical handles, append routing,
scanner handoff and truncation routing. RecoveryStorage owns startup traversal
of physical WALs; autowalimpls is constructed only after bounded recovery succeeds.

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

Old messages without `target_start_message_id` retain the legacy switch/recovery interpretation.
Readers must understand the new link before hot switches are enabled throughout
a deployment. Protobuf compatibility alone does not make an old binary safe to
take over a channel containing hot-switch links; rollback requires compatible
readers and a drained migration boundary.

## Append and synchronization

1. Serialize AlterWAL with the entire ordinary Sync operation (sampling,
   physical append and WAB publication), then acquire the PChannel exclusive
   lock. Never wait under this lock for a Sync blocked on the same lock.
2. Drain preceding appends, allocate T, fence old segment allocation, and publish
   the old acknowledged prefix's query targets before the boundary is visible.
3. Prepare S, persist AlterWAL(T,S) in the old WAL, then select the new writer.
   The control link remains one complete physical record even when business-message
   chunking is enabled. Backend retries retain the same switch and target anchor. Once the old
   boundary is durable, failure cannot reopen the old prefix for business writes.
4. End unfinished transactions in the exclusive scope. Publish the old ACK batch
   followed by AlterWAL(T) into the existing WAB. Do not discard acknowledged
   data when draining the manager.
5. Reset the AckManager's pending state and the Sync operator's published batch,
   retain the timestamp watermark T and set the new LastConfirmed baseline to S.
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

The old AlterWAL keeps its old native MessageID and LastConfirmed. Subsequent
messages have target-native IDs and target-native LastConfirmed. No Ack heap or
serialized `_lc` mixes backend encodings. Clearing the timestamp heap alone is
insufficient: pending transaction IDs, acknowledged details, operator details
and the retained LastConfirmed scalar all participate in the handoff.

## Scanners

The physical scanner forwards a complete AlterWAL, closes that physical reader
and continues after S. Outer chunk assembly, ordering and transaction processing
remain one continuous logical stream. AlterWAL confirms the prefix through T,
ends uncommitted transactions and remains an observable control event. Simply
treating it as a TimeTick while bypassing its exclusive effect is incorrect.

From/After options select the historical backend using their position. In
particular, StartAfter(old AlterWAL) must inspect the excluded boundary for routing
before excluding it from delivery. Reader creation does not acquire a new writer
on a historical backend. WAB eviction may fall back through an old LastConfirmed
and follow the same chain. Missing retained history is an error, not permission
to jump to the current writer. Truncation uses the checkpoint's backend and never
invalidates an uncompleted recovery prefix merely because writes switched.

## Startup recovery

The data checkpoint selects the first physical WAL. RecoveryStorage recovers
through physical WAL implementations directly, before constructing autowalimpls.
There is no tail-query API and no separate chain-discovery scan.

1. Acquire/fence the checkpoint backend's writer and append a RecoveryBarrier.
2. Replay with the ordinary scanner's decoding, chunk assembly, ordering and
   transaction semantics. At a cross-backend AlterWAL, pause physical input
   before reading anything beyond that marker. Observe and dispatch the old
   prefix through the boundary, including transaction abortion and async flush.
3. RecoveryStorage opens/fences the linked target writer, appends a new local
   RecoveryBarrier, and continues after the durable target anchor S. Reallocate
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
5. After the bounded replay and snapshot succeed, construct autowalimpls by
   transferring the already-open writers, using the final physical writer as
   current. Initialize WAB, MVCC and append interceptors from the final barrier.
   The final scanner stays paused, retaining its transaction buffer. Bind its
   live WAB and autowalimpls fallback source, then release startup input and write
   admission. No second data replay or writer reopening occurs at this handoff.

Historical handles have one owner throughout startup and are transferred on
success; failures close newly opened handles as well as the original writer.
The safe data checkpoint may still belong to an older WAL. Neither finishing
recovery nor constructing autowalimpls rewrites it to the target's earliest ID.
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
