# QueryView Transform Subscriptions

QueryNode subscribes to Delete transforms through a read-only adaptor over the
PChannel's WALSummary. The authoritative storage and recovery contracts are
[WALSummary](../wal/summary.md), [TransformLog Subscription Adaptor](../wal/transform_log.md),
and [WAL Recovery Architecture](../wal/wal-recovery-architecture.md).

The earlier design of an independent VChannel TransformLog store is superseded.
There is no second chunk store, TransformLog catalog checkpoint, retained-message
write path, or subscription acknowledgement protocol.

## Ownership and transport

```text
RecoveryStorage ordered PChannel observation
  -> WALSummary publication + live Transform dispatch
       -> DispatchStream (shared replay worker + callback worker per stream)
          -> SubscribeTransform RPC
             -> QN VChannel TransformLogBuffer
                -> bounded catchup pages on the shared worker pool
                -> synchronous live dispatch to caught-up physical segments

SN Growing bootstrap: WALSummary -> bounded Stream -> QueryRuntime live events
```

`PChannelRecoveryManager.AcquireStream` creates an independently closeable
stream over the same summary reader used for SN preparation. Closing a client
stream releases its subscriptions, without closing the manager's SN bootstrap
stream or WALSummary. PChannel shutdown cancels acquired streams.

The server gates subscription callbacks until the Create response is sent. If
that send fails, it closes the callback handler before closing the subscription.
Subscription Close joins in-flight delivery; a callback must not wait
for a Create response that will never succeed. Terminal errors end the physical
stream through Done/Error rather than appearing as transform events. The failed creation emits no
subscription events, and RPC cleanup also closes the acquired stream.

The handler client resolves the current PChannel assignment. Local consumers
use the WAL adaptor; remote consumers use `SubscribeTransform`. Assignment
changes invalidate the old stream and the client resumes against the new owner.
The protocol contains transforms and progress, not WAL message IDs or scanner
internals. QN keeps one shared transform buffer per VChannel; individual sealed
segments register with that buffer rather than opening their own subscriptions.

The remote client serializes request sends and `CloseSend` with one send lock.
Only the explicit close path half-closes the transport; receive completion
cancels the RPC's own context without taking that lock, so a flow-controlled
send can exit. Requests recheck termination after acquiring the send lock.
Explicit close retains the `CloseStream` handshake, with a five-second deadline
covering both the closing send and response. Expiry terminates the stream with
`context.DeadlineExceeded`. RPC cancellation does not cancel the parent
resumable stream, and subscription cleanup still runs once.

An underlying read, subscription or transport failure ends the physical attempt
through the stream's Done/Error, never a business-handler event. The resumable
client closes that attempt outside delivery callbacks and recreates subscriptions
from each handler's last accepted Entry/SyncUp cursor. Runtime failures are not
classified or forwarded to the shared QN buffer; persistent failures remain
observable in retry logs and block progress without fabricating coverage.
Invalid subscription options are rejected before starting an attempt. Explicit
consumer Close terminates the resumable stream rather than reconnecting. See
[Resume and Failure](../wal/transform_log.md#5-resume-and-failure).

## Cursor and visibility contracts

- `StartAfterTimeTick` is exclusive. A nonzero `EndTimeTick` bounds replay;
  zero requests continuous delivery.
- Delete entries use the outer committed transaction TimeTick when applicable.
  Plain inserts do not produce Delete entries.
- `SyncUp(T)` certifies delivery of the retained suffix through T, including
  empty intervals after the adjusted start. It does not certify persistence,
  L0 materialization, segment application, or delivery below Summary's lower bound.
- The adaptor accepts Summary's `FastForwardTimeTick` and resumes at that
  retained lower bound. A bounded interval entirely below the bound completes
  without entries. This compatibility behavior does not establish that skipped
  Deletes were applied; complete query recovery still requires the retention
  guarantees below. See [the adaptor contract](../wal/transform_log.md).
- The summary reader provides bounded pages and scoped change notifications.
  No independent subscriber backlog is persisted on SN.
- The QN buffer applies entries to each registered segment and advances that
  segment's visible TimeTick only after application. Query execution waits on
  the requested transform visibility before reading the segment.

SN uses bounded replay while preparing a captured WAL input view. Its subsequent
live resource events come through the VChannel's ordered dispatch path. QN uses
continuous subscriptions for sealed segments. Both use the same summary read
and truncation semantics.

## Catchup-to-dispatch continuity

SN captures readable frontier H and registers its live observer while holding
WALSummary's observation lock. RecoveryStorage is the single ordered producer:
`observeModulesMessage -> Summary.ObserveMessage` publishes ordinary records;
`observeQueryBarriers -> Summary.ObserveQueryBarrier` publishes scoped barriers.
A Delete at or before H belongs to retained historical replay. A later Delete
belongs to the registered suffix, then live dispatch. Registration cannot leave
a gap between the two. The stream producer lock orders the final suffix,
initial SyncUp and subsequent live events. Slow callbacks never run on the
RecoveryStorage producer; only the bounded stream queue is updated there.

QN appends received entries and selects live registrations under the VChannel
buffer lock. A pending registration replays bounded pages without occupying a
worker while waiting for SyncUp. The final empty-history check and move from
pending to live share that same lock. An arriving Delete is therefore either
included in pending replay or dispatched to the live registration. Application
advances the instance's watermark only after the complete entry succeeds.
View retention and pending replay pins protect the required history during this
transition; catchup does not authorize trimming the shared View range.

The catchup task's scheduled bit covers queued and executing work. Parking
clears it under the producer lock; an arrival either belongs to that pass or
queues its successor. Each page yields to the shared worker queue. No waiting
worker is required for an idle pending channel, and caught-up segments consume
only the existing buffer dispatch. The SegmentResource's temporary catchup
wait ends at the finite readiness frontier; later failure is delivered by an
installed callback (including failures preceding installation), not one waiting
goroutine per Segment.

These guarantees cover retained history and the subsequent live suffix. The
existing FastForward policy for already GC'd history is unchanged and is not a
proof of delivery of that discarded prefix. Stream overflow is a terminal
attempt error, never successful progress over a dropped queue item; the client
resumes from its last accepted cursor using the same retained-history contract.

## Persistence and retention

WALSummary owns chunks, manifests, readable coverage, cache and GC. Its durable
confirmation bounds the single RecoveryStorage checkpoint. Query subscriptions
do not retain RecoveryStorage acknowledgement handles or advance that checkpoint.

The active WAL L0 materializer independently retains Delete/Flush handles for
legacy query recovery. The summary-based L0 consumer remains an alternative,
unwired implementation; enabling QueryView subscriptions does not switch L0
materialization to it.

`transform_start_after_timetick` is the shard's exclusive replay frontier in
QueryView metadata. QueryView recovers incremental history through TransformLog
only; it must not load or forward L0 Segments to fill missing history. A recovery
checkpoint is not by itself a safe replay start for every Segment.

The DataCoord producer persists W (SN's conditional final-publication proof)
and A (minimum cursor of all selected Segment bases) with each DataVersion.
It computes `F = min(W, A)` for QueryView; the DataView proto stores only W/A.
Recovery uses the same version's inputs without consulting checkpoints.
Per-Segment cursors remain tied to their base revisions; the shared buffer
retains the whole View's range, not merely its locally assigned Segments. See
[Transform Start-After TimeTick](transform_start_after_timetick.md) for generation,
materialization prerequisites, asynchronous publication fences, and recovery.

Storage GC must protect the suffix required by the latest reloadable View and
all still-protected older Views, together with SN local recovery requirements.
L0 materialization alone cannot release this history. A conservative initial
implementation may retain history from the VChannel creation point; advancing
retention requires recovered View references and coordinated publication.

## Key packages

- `internal/streamingnode/server/wal/walsummary/dispatch_stream.go`: remote replay and PChannel live dispatch.
- `internal/streamingnode/server/wal/walsummary/stream.go`: bounded bootstrap and reader-only adaptor.
- `internal/streamingnode/server/wal/vchannel/manager.go`: stream ownership.
- `internal/streamingnode/server/service/handler/transformlog`: remote server.
- `internal/streamingnode/client/handler/transformlog`: remote stream client.
- `internal/querynodev2/transformlogbuffer`: shared QN buffers and segment application.
- `internal/querynodev2/qnview`: resource preparation, readiness and query leases.
