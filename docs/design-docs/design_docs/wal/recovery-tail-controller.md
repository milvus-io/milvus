# Recovery Tail Controller

The Recovery Tail Controller keeps the replayable WAL suffix within a target
size. It does not decide how SegmentView or TransformLog batches data and it
does not solve object fragmentation.

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
```

The recovery tail uses `publishedOffset`, not `completedOffset`, because a
completed but unpublished message is replayed after a crash.

## 2. Byte Accounting

Each tracked WAL point records the message's logical encoded bytes and a
monotonic cumulative end offset. The offset is runtime bookkeeping, not part of
the durable checkpoint format.

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
- current blocking category when known.

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
- TransformLog flushes only its own open chunk range;
- non-persistence blockers such as BroadcastAck are reported, not converted
  into fake flush requests.

## 5. Watermarks

Soft pressure starts persistence requests before the target is exceeded.

```text
recoveryTailBytes >= softWatermark
  -> request progress for stalled VChannels
  -> increase publisher frequency while completed progress is unpublished
```

A strict bound needs append admission control:

```text
recoveryTailBytes >= highWatermark
  -> apply PChannel write backpressure

recoveryTailBytes <= lowWatermark
  -> release backpressure
```

Without append backpressure, object storage, catalog, or Coordinator Ack failure
can grow the tail without bound. A background trigger alone is best-effort.

## 6. RTO Configuration

The configured tail target follows measured recovery throughput:

```text
maxTailBytes <= targetRecoveryDuration * replayBytesPerSecond
```

At 64 MiB/s, replaying 16 GiB has a raw lower bound of 256 seconds, or 4 minutes
16 seconds. Production thresholds must additionally account for decode,
object-store writes, catalog publication, and retry overhead.

## 7. Metrics

Required PChannel metrics include:

- published, completed, and observed TimeTicks;
- recovery-tail, blocking, and publish-lag bytes;
- oldest incomplete message age;
- stalled VChannel count;
- requested and completed RequestPersistThrough targets;
- blocker category: Segment data, TransformLog, lifecycle RPC, BroadcastAck,
  object storage, or catalog;
- append-backpressure duration.

## 8. Defrag Boundary

RequestPersistThrough may generate small objects in high-cardinality Partition Key
workloads. RecoveryStorage accepts that tradeoff to meet the recovery-tail
target. Future log Defrag owns object coalescing, reference replacement, and old
object reclamation. Defrag must not move the global WAL checkpoint or alter
message Ack semantics.
