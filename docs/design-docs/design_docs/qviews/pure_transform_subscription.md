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
RecoveryStorage -> WALSummary
                       |
                 walsummary.Stream
                  /           \
     SN bounded bootstrap     SubscribeTransform RPC
                                     |
                            QN VChannel transform buffer
                                     |
                            sealed-segment Delete application
```

`PChannelRecoveryManager.AcquireStream` creates an independently closeable
stream over the same summary reader used for SN preparation. Closing a client
stream releases its subscriptions, without closing the manager's SN bootstrap
stream or WALSummary. PChannel shutdown cancels acquired streams.

The handler client resolves the current PChannel assignment. Local consumers
use the WAL adaptor; remote consumers use `SubscribeTransform`. Assignment
changes invalidate the old stream and the client resumes against the new owner.
The protocol contains transforms and progress, not WAL message IDs or scanner
internals. QN keeps one shared transform buffer per VChannel; individual sealed
segments register with that buffer rather than opening their own subscriptions.

## Cursor and visibility contracts

- `StartAfterTimeTick` is exclusive. A nonzero `EndTimeTick` bounds replay;
  zero requests continuous delivery.
- Delete entries use the outer committed transaction TimeTick when applicable.
  Plain inserts do not produce Delete entries.
- `SyncUp(T)` certifies successful delivery through T, including empty intervals.
  It does not certify persistence, L0 materialization, or segment application.
- The adaptor rejects missing history with `ErrTransformLogStartPointTruncated`.
  It never reports a successful catch-up across a truncated interval.
- The summary reader provides bounded pages and scoped change notifications.
  No independent subscriber backlog is persisted on SN.
- The QN buffer applies entries to each registered segment and advances that
  segment's visible TimeTick only after application. Query execution waits on
  the requested transform visibility before reading the segment.

SN uses bounded replay while preparing a captured WAL input view. Its subsequent
live resource events come through the VChannel's ordered dispatch path. QN uses
continuous subscriptions for sealed segments. Both use the same summary read
and truncation semantics.

## Persistence and retention

WALSummary owns chunks, manifests, readable coverage, cache and GC. Its durable
confirmation bounds the single RecoveryStorage checkpoint. Query subscriptions
do not retain RecoveryStorage acknowledgement handles or advance that checkpoint.

The active WAL L0 materializer independently retains Delete/Flush handles for
legacy query recovery. The summary-based L0 consumer remains an alternative,
unwired implementation; enabling QueryView subscriptions does not switch L0
materialization to it.

`transform_start_after_timetick` remains an exclusive replay frontier in
QueryView metadata. Producing a safe advancing frontier from DataCoord requires
the shard flush protocol described in
[Transform Start-After TimeTick](transform_start_after_timetick.md). The current
DataView manager does not yet produce that advancing frontier.

## Key packages

- `internal/streamingnode/server/wal/walsummary/stream.go`: bounded and live reads.
- `internal/streamingnode/server/wal/vchannel/manager.go`: stream ownership.
- `internal/streamingnode/server/service/handler/transformlog`: remote server.
- `internal/streamingnode/client/handler/transformlog`: remote stream client.
- `internal/querynodev2/transformlogbuffer`: shared QN buffers and segment application.
- `internal/querynodev2/qnview`: resource preparation, readiness and query leases.
