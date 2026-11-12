# Recovery Tail Controller

- Feature DRI: @chyezh
- Primary Approver: @czs007
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

The Recovery Tail Controller keeps the replayable WAL suffix within a target
size. It does not decide how SegmentView or L0Materializer batches data and it
does not solve object fragmentation.

**Current runtime:** [WAL L0 Materializer](l0_materializer.md) retains Delete
handles for legacy query recovery. The [Summary consumer](summary_l0_materializer.md)
is retained for future QueryView wiring; the two implementations are not run together.

WAB cache occupancy is a separate pressure source, including unconsumed records
and the retained consumed cache. Its live slowdownRatio (minimum 1) scales the
WAB instance capacity; rate and rejection-ratio settings are configured under `streaming.walRateLimit.writeAheadBuffer`, with a rate determined
directly by the current excess percentage (see [WAB delivery](../../../agent_guides/streaming-system/wal/timetick_and_txn.md#wab-delivery-and-producer-backpressure)).
It does not use this controller's timed slowdown/recovery ramp or its
`recoveryStorage.adaptiveRateLimit` settings; those settings still control the
observed-to-published recovery tail. The unused `flusher.adaptiveRateLimit`
settings and scanner catchup/tailing rate-control wiring have been removed. Both sources
publish through the existing multi-source WAL limiter; clearing one source
cannot clear the other's pressure. WAB's consumed-through boundary makes records eligible for capacity/TTL
eviction, while retaining the durable handoff anchor separately from the queue; it authorizes in-memory release only and never advances any durable frontier below.

## 1. Byte Frontiers

RecoveryStorage tracks three runtime byte offsets:

```text
publishedOffset  -- catalog-published global checkpoint
completedOffset  -- AckTracker continuous completed point
observedOffset   -- latest observed WAL message
```

Derived pressure values are:

```text
recoveryTailBytes = observedOffset - publishedOffset
blockingBytes     = observedOffset - completedOffset
publishLagBytes   = completedOffset - publishedOffset
recoveryTailMessages = observedMessageCount - publishedMessageCount
```

The recovery tail uses `publishedOffset`, not `completedOffset`, because a
completed but unpublished message is replayed after a crash.

## 2. Byte Accounting

Each tracked WAL point records the message's estimated logical encoded bytes
and a monotonic cumulative end offset. The offset is runtime bookkeeping, not
part of the durable checkpoint format.

Each tracked point also carries a cumulative message count. It counts persisted
messages that advance the observed prefix, including TimeTicks; an assembled
committed transaction counts once, matching the existing logical-byte units.
Completed ACKs do not remove this count. Summary-gated checkpoint selection
freezes its byte offset and count from the same entry, and only successful
catalog publication advances both published frontiers. Retries reuse the frozen
pair, so newer observations cannot be accidentally subtracted from the tail.

Idle time is local elapsed time since the last newly observed persisted
non-TimeTick message. TimeTicks and volatile heartbeats do not refresh it, and
checkpoint publication does not restart the timer. Replay reconstructs the
relative counters and starts a new local idle clock; no clock or count is added
to the durable checkpoint format.

After restart, replay begins with offset zero at the persisted checkpoint and
reconstructs the relative tail size while scanning. Thresholds include headroom
for backend framing, compression, and conservative `LastConfirmedMessageID`
delivery semantics.

## 3. Stall Detection

AckTracker retains lightweight ordered entries after message payload release.
For incomplete entries it also retains enough classification to report:

- message TimeTick;
- affected VChannels;
- first-observed time;
- explicit blocking categories are a follow-up diagnostic, not current Tracker state.

On each control cycle, the Tracker finds incomplete messages older than the
stall timeout. For each affected VChannel, it returns the largest stalled
TimeTick, not the first stalled TimeTick.

Example:

```text
Tracker: TT=100, TT=200, TT=201 affect three different segments
```

The request for the VChannel containing TT=100 targets 100. If that Segment
also holds 101 and 102, SegmentView may batch 100, 101, and 102 itself. A later
request for 101 becomes a no-op because the component `checkpoint_time_tick`
already covers it. RecoveryStorage never widens the request to the PChannel's
latest TimeTick.

## 4. Component Trigger Interface

AckTracker calls the VChannel-scoped trigger directly:

```go
type VChannelPersistRequester interface {
    RequestPersistThrough(vchannel string, targetTimeTick uint64)
}
```

The interface means “make progress sufficient to durably cover this VChannel
through at least this target where applicable.” It does not mean one object per
request and it does not carry a WAL message.

Components implement idempotency:

- a covered target is a no-op;
- an existing task is widened or reused when safe;
- SegmentView batches only its own segment data;
- Summary is not a recipient of Tracker stall requests. It seals its own
  PChannel staging buffer on size or independent tail-pressure checks, so
  summary-only backlog cannot be hidden by completed Ack;
- the currently wired [WAL L0 materializer](l0_materializer.md) handles the
  same RequestPersistThrough contract as SegmentView and holds source Delete
  handles until output/registration and dirty metadata installation;
- non-persistence blockers such as BroadcastAck rely on their own retry paths.
  Explicit blocker classification is not implemented; a VChannel persist
  request may still be issued without resolving an Ack or poisoned-message stall.

## 5. Independent Persistence And Append Admission

Persistence triggers and append rate limiting use independent configurations.
They share the observed-to-published tail counters. Once append pressure is
active, persistence must continue until that pressure can enter recovery.

### Persistence triggers

The initial per-PChannel defaults are:

```text
streaming.walRecovery.flush.sizeThreshold = 6 GiB
streaming.walRecovery.flush.messageThreshold = 6144
streaming.walRecovery.flush.idleMinSize = 512 MiB
streaming.walRecovery.flush.idleTimeout = 10 minutes
```

```text
recoveryTailBytes >= flush.sizeThreshold
  OR recoveryTailMessages >= flush.messageThreshold
  OR (recoveryTailBytes > flush.idleMinSize AND idle time >= flush.idleTimeout)
  -> request progress for the VChannel owning the oldest incomplete prefix
  -> let Summary's independent worker seal its pending span
```

Append Slowdown or Reject also keeps this periodic persistence loop active until
the tail reaches `tail.lowWatermark` and the controller sends
`EnterRecoveryMode`. For example, draining 8 GiB to 5 GiB must not stop
persistence just because it passed the 6 GiB flush threshold: admission still
needs a tail of at most 4 GiB to recover. This also covers rejection reached by
the adaptive limiter's delay rather than the immediate 16 GiB threshold.

These thresholds are captured when a WAL opens. Nonpositive size or message
thresholds disable the respective trigger; either nonpositive idle setting
disables the size-and-idle trigger. The periodic Tracker and Summary workers
check the predicate once per second, even without new WAL messages or incomplete
ACKs. Summary has no separate idle/count policy. Requests enqueue asynchronous
work; they neither close every growing Segment nor establish a full-tail drain
barrier. Existing age-based Segment/L0 persistence remains independent.

`ShouldFlush` is a read-only predicate. Reaching a persistence threshold does
not enter slowdown, arm a rejection timer, or change the limiter's recovery
condition. Disabling persistence triggers does not disable byte-based append
admission or its required persistence-to-recovery loop. The thresholds need not
be ordered against the limiter watermarks. When no recovery-tail pressure is
active, including after requesting Recovery, only the independent flush triggers
apply; starting persistence at 6 GiB does not itself put the limiter into
Slowdown or Recovery.

### Append admission watermarks

Existing configuration keys and defaults remain:

```text
streaming.walRecovery.tail.lowWatermark  = 4 GiB
streaming.walRecovery.tail.softWatermark = 8 GiB
streaming.walRecovery.tail.highWatermark = 16 GiB
```

```text
recoveryTailBytes >= softWatermark -> enter adaptive append slowdown
recoveryTailBytes >= highWatermark -> reject new DML append on this PChannel
recoveryTailBytes <= lowWatermark  -> enter adaptive recovery
```

Only bytes affect this state machine and its slowdown checker. At the low
watermark, rejection is released through `EnterRecoveryMode`, even if the count
or idle persistence condition still holds. There is no Reject-to-Slowdown request.
The existing adaptive limiter can also escalate sustained slowdown to rejection
after its configured delay; 16 GiB is the immediate byte rejection threshold,
not the only route to rejection. Persistence requests never start this delay.

Without append backpressure, object storage, catalog, or Coordinator Ack failure
can grow the tail without bound. A background persistence trigger alone is
best-effort. Increasing publisher frequency while completed progress is
unpublished remains independent of these two predicates.

## 6. RTO Configuration

The configured tail target follows measured recovery throughput:

```text
maxTailBytes <= targetRecoveryDuration * replayBytesPerSecond
```

At 64 MiB/s, replaying 16 GiB has a raw lower bound of 256 seconds, or 4 minutes
16 seconds. Production thresholds must additionally account for decode,
object-store writes, catalog publication, and retry overhead.

## 7. Metrics

The initial controller exports these PChannel metrics:

- published, completed, and observed TimeTicks;
- recovery-tail, blocking, and publish-lag bytes;
- the existing WAL adaptive-rate-limit state.

Follow-up diagnostic metrics should add:

- oldest incomplete message age;
- stalled VChannel count;
- requested and completed RequestPersistThrough targets;
- blocker category: Segment data, Summary persistence, lifecycle RPC, BroadcastAck,
  object storage, or catalog;
- append-backpressure duration.

## 8. Defrag Boundary

RequestPersistThrough may generate small objects in high-cardinality Partition Key
workloads. RecoveryStorage accepts that tradeoff to meet the recovery-tail
target. Future log Defrag owns object coalescing, reference replacement, and old
object reclamation. Defrag must not move the global WAL checkpoint or alter
message Ack semantics.
