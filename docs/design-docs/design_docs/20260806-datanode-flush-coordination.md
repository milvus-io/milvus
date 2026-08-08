# DataNode Flush Coordination

Status: implemented
Scope: `internal/flushcommon/writebuffer`, `internal/flushcommon/syncmgr`, `internal/datanode/importv2`

## Problem

The DataNode flush path grew two data sources — payload buffered locally, and rows
still pinned in a segcore growing segment — and each grew its own scheduling,
retry, completion and shutdown machinery. The two were near-copies that had
already drifted apart, and the segment state machine they shared had a transition
that could be issued optimistically and then undone, which no reader could reason
about locally.

This document states the model the two paths now share.

## Segment state machine

The state machine lives in the DataNode's metacache. DataCoord has its own,
smaller one that shares the `commonpb.SegmentState` enum but not its meaning —
nothing writes `Flushing` on the DataCoord side, and the checks that read it are
vestigial.

```
Growing ──seal──> Sealed ──claim──> Flushing ──commit──> Flushed ──> removed
   │                 │                  │
   └─────────────────┴──────────────────┴──> Dropped   (collection/partition drop)
```

| Transition | Trigger | Executed by | Driven by |
| --- | --- | --- | --- |
| → `Growing` | first insert, or recovery | `CreateNewGrowingSegment` | WAL |
| `Growing → Sealed` | in-band WAL FlushMessage | `ddNode` → `sealSegments` | WAL |
| `Sealed → Flushing` | timetick / memory eviction / resync | `getSyncTask` | local |
| `Flushing → Flushed` | task commit succeeded | `SyncTask.Commit` | local |
| `* → Dropped` | collection / partition drop | drop message | WAL |

The first two transitions are decided by WAL position; the last two by local
scheduling. **What data belongs to a segment is decided by the WAL; when it is
flushed is decided by the DataNode.**

### Why `Sealed` and `Flushing` are separate

They answer different questions:

- `Sealed` — does this segment still accept writes? **No.**
- `Flushing` — is this flush claimed, with its content fixed? **Yes.**

Collapsing them loses two properties: policy idempotency (every timetick would
re-issue a task for the same segment) and the ability to retry *the same flush*
rather than re-deciding what to flush.

### The claim is one-way

`getSyncTask` claims `Sealed → Flushing` after `yieldBuffer` has taken the
segment's buffered content. This is the point where the flush's content is fixed,
and it is fixed for good:

- The seal arrived **in band** on the same single-threaded flowgraph
  (`dmStreamNode → ddNode → writeNode`), so every row of a `Sealed` segment is
  already buffered, and no further row can be assigned to it. Whatever the task
  takes is the segment's tail.
- A task that fails to build, or fails to commit, leaves the segment in
  `Flushing`. `GetSealedSegmentsPolicy` selects `Flushing` segments ahead of
  `Sealed` ones, so the retry resumes **that** flush.

There is therefore no `Flushing → Sealed` rollback. Selection performs no state
change at all: it cannot know whether the segment's source can produce the flush
yet, so claiming there would mean claiming optimistically and undoing on failure —
and an undoable claim is one another path can observe half-done.

## Two flush sources, one coordination model

`metacache.FlushSourceMode` records which subsystem owns a segment's payload; the
decision is sticky for the segment's lifetime.

| | `FlushSourceWriteBuffer` | `FlushSourceGrowing` |
| --- | --- | --- |
| Data lives in | the DataNode's insert/delta buffers | a segcore growing segment |
| Task type | `SyncTask` | `GrowingSourceSyncTask` |
| Row ownership | the task **owns** the yielded payload | rows stay pinned until `CommitGrowingFlush` |
| Cost of a failed attempt | the task must be retained — the rows exist nowhere else | one round trip |
| Per-segment state | `writeBufferSyncQueue` | `growingSourceProgress` |

That ownership difference is the only reason the two paths differ in structure.
Everything below is shared, and lives in `flush_coordination.go`.

### `flushIntent` — one debt, two triggers

A segment that wanted a flush and did not get one carries a debt:

```go
type flushIntent struct {
    owes  bool
    since time.Time // zero: not rate-limited
}
```

- `want()` — admission refused the task (queue full, a task in flight). What
  unblocks it is an event, so no delay is added.
- `backoff(now)` — the attempt could not be made or it failed. Time must pass
  first. Only the transition into a rate-limited debt stamps `since`, so repeated
  failures in one round do not keep pushing the next attempt out.
- `due(now, interval)` — the interval is applied at **drive** time, not stored, so
  a changed `dataNode.flushRetryInterval` takes effect on debts already
  outstanding.
- `attempted(now)` — a re-drive just happened: the debt stays (only a completed
  task settles it) but the rate limit starts over. Every driver must call this;
  without it `since` keeps the FIRST failure's timestamp, `backoff` refuses to
  move it because the debt is already outstanding, and `due()` is true on every
  timetick from then on — one attempt per interval degrades into a retry storm.

Two triggers read the same debt: a task completing, and a timetick. Both go
through `due()`, so a completion cannot jump a retry interval that a failure just
imposed.

`growingSourceProgress.owesFlush` is a **different** bit: sticky, set at seal,
cleared only by a successful flush task. A segment can owe a flush while having no
outstanding attempt to make, and vice versa.

### Retry is driven by timeticks

There is no timer. `BufferData` runs on every msgpack, including pure-timetick
ones, and calls `driveRetries` before handling new data — so a segment's queue is
always replayed from its oldest task. The only place that survives without
timeticks, Drop, drives its own retries inside `waitSyncsSettled`.

### Terminal release and shutdown

- `releaseTerminalSync` is the single definition of "this task is done for good":
  discard its metacache syncing counters, release its payload and any prepared
  storage handle. Both terminal paths call it.
- `waitSyncsSettled` waits for write-buffer entries **and** growing-source
  progress together, under one `growingFlushCancelGrace`. The growing side
  signals completion through the `growingSettled` generation channel rather than
  being polled; `ackGrowingSyncLocked` / `failGrowingSyncLocked` are the only ways
  to leave the in-flight set, so the broadcast cannot be forgotten.

The wait is bounded after cancellation. An already-started native write takes no
cancellation token, so waiting past the grace turns the caller's timeout into a
hang — and a `DropChannel` that never returns is strictly worse than one that
reports failure and lets WAL replay redo the work. Giving up the wait is not
giving up the work: the task keeps its segment pin and finishes in the background.

## The rule every failure path follows

Two rules, both learned from defects this change had to fix:

**A task returns what it reserved, and that return is derived from the task.**
`releaseTerminalSync` (write-buffer) and `settleFailedGrowingTaskLocked` (growing)
take only the task: its metacache syncing rows and its checkpoint candidate. They
run unconditionally, never under a lookup of state a concurrent teardown may have
already removed. The growing failure path used to sit inside
`if progress, exists := ...growingSourceProgress[id]`, so a callback landing after
`abortDrop` skipped both — leaving the segment's `syncingRows` inflated forever
and the channel checkpoint pinned behind a candidate nobody would remove.

**Cleanup hangs off the point state disappears, not off a path that happens to
reach it.** The L0 partition→segment mapping was cleared only in the `triggerSync`
loop inside `BufferData`, while the segment leaves the metacache in
`finishWriteBufferSync`. A segment flushed through any other path — the memory
watchdog's `EvictBuffer` — left a dead mapping behind: the next delete recreated a
buffer for an ID that no longer existed, every sync attempt died on
segment-not-found, and that buffer and the checkpoint it pins never moved again.
`forgetL0SegmentLocked` now hangs off `RemoveSegments`.

The same rule explains what `abortDrop` must NOT do. It declares its data
un-committed and promises the checkpoint stays pinned for WAL replay — so it may
not clear `buffers` or `growingSourceProgress`, which are two of the three
candidate sources `GetCheckpoint` pins on. With all three empty the checkpoint
falls back to the latest CONSUMED position, past data that was never written.

A retry must also see a quiet queue. `needsRetryLocked` requires that no entry is
still submitted: a re-drive replays the whole queue from its oldest task, and a
second submission against an already-aborted dispatcher key finishes
synchronously — so the terminal branch would `Abandon` a task whose first
`Prepare` is still writing, pulling the payload out from under the writer and
orphaning the native handle it assigns afterwards.

## The flush range is a pair of WAL positions

Two representations, one per side, deliberately asymmetric:

- **DataNode, DataCoord and recovery keep the full `MsgPosition`.** The MsgID
  is the only thing a WAL can seek by, so it is what gets persisted
  (`SaveBinlogPaths CheckPoints[].Position` → the segment's DML position) and
  what recovery resumes from.
- **The source side consumes only the position's timestamp projection.** The
  flush range handed to segcore is `(startTs, endTs]`, resolved inside segcore
  against the segment's own rows via `get_active_count(ts)` — the same
  `upper_bound` the query path uses for MVCC visibility, bounded by the
  acknowledged insert prefix.

The projection is sound only within one vchannel: there the TimeTick order is
monotonic and every message's timestamp is unique, so a position and its
timestamp select the same boundary. Timestamps from different physical
channels are NOT comparable, and nothing in this design compares them — every
fence, watermark (tsafe) and checkpoint named here lives on the one channel
the write buffer owns.

The two fences:

- the lower fence is the position the segment was last flushed through
  (`metacache.lastFlushPosition`, restored from the DML position on recovery);
- the upper fence is the newest pack recorded for the segment, and the task
  publishes exactly that position — the full MsgPosition, not merely its
  timestamp — as its checkpoint, unchanged.

No row count crosses the boundary in either direction. Row offsets exist only
inside segcore, and they share no origin with anything the DataNode can keep: a
restart rebuilds the segment from a WAL replay and its offsets start over at
zero. The previous protocol — the DataNode accumulating a `targetOffset` row
count and reconciling it against segcore's `AckedRowCount` — required two
independently-maintained counters to converge; a divergence (a dropped row, a
replay with a different origin) had no way to self-correct and stalled the
segment silently.

The range is `(startTs, endTs]`. `upper_bound` semantics make the boundary
exact: a whole insert request shares one timestamp, so a fence can never split
a request; a pack's rows are all `<=` its end position's timestamp and the next
pack's are strictly greater, so a fence can never split a pack. Consecutive
flushes therefore partition the rows — every row written once, none skipped
(asserted directly by `FlushGrowingSegmentPartitionsRowsAcrossAdjacentFences`).

Readiness is a raw watermark read, never a wait. The source exposes `TSafe()`
— the position its pipeline has fully consumed and applied — and the task
refuses to run while `TSafe() < endTs`, surfacing a retryable error that the
write buffer re-drives on a later timetick. It must not block: the flusher and
the source consume the same channel through the message dispatcher, whose sends
are sequential, so waiting on the source's progress can stall the very
consumption being waited for. And it must not use the delegator's `waitTSafe`,
whose external-table and `DowngradeTsafe` escape hatches report success without
the watermark having advanced — acceptable for serving a slightly stale read,
data loss for a flush. Behind is a normal outcome; nothing is lost while it
lasts, because unflushed packs keep `firstUncommittedPosition` pinned and the
channel checkpoint cannot advance past them.

A retry of a flush whose data landed but whose metadata ack did not
(`pendingCommitted`) replays the FROZEN attempt — its manifest, checkpoint, row
count and finalization flags — rather than re-deriving any of them from live
state, which by retry time may already include newer packs or a concurrent
seal.

Drop does not paper over an unavailable source: `syncDropSegment` retries on
`errGrowingSourceUnavailable` until its context expires, then fails loudly and
leaves the checkpoint pinned.

## Error classification

All classification is in `ClassifySyncError`; every layer below returns its error
unchanged.

| Decision | Meaning | Effect |
| --- | --- | --- |
| `SyncRetry` | default — throttling, coordinator not ready | keep payload, counters and queue position; arm the segment's intent |
| `SyncTerminal` | no attempt can change the outcome | `releaseTerminalSync`; checkpoint stays pinned for WAL replay |
| `SyncCanceled` | the caller went away | not a task failure, and never escalated |

`ErrSegmentNotFound` / `ErrChannelNotFound` are terminal. DataCoord saying the
target is gone cannot be fixed by trying again, and the meta writer already stops
its own retry loop for them — but the default is `SyncRetry`, so without an
explicit case the task would be re-driven on every timetick forever, pinning the
channel checkpoint behind a segment that no longer exists.

`SyncCanceled` must be excluded from fatal escalation: `DataNode.Stop()` closes
the sync manager **before** the flowgraphs, so during a graceful stop the
dispatcher aborts in-flight tasks with `context.Canceled` while the write buffer
is still open. Escalating that would make every drain-with-traffic panic.

## Storage-v3 manifest commit

The flush path commits with `packed.CommitManifestUpdates`
(`LOON_TRANSACTION_RESOLVE_OVERWRITE`).

`OverwriteResolver` applies the staged updates to a **deep copy of the manifest
that was read**, ignoring any newer one, and commits the result as `latest+1`. So
a version that appeared since — this handle's own lost answer, or a crashed
incarnation whose DataCoord ack never landed — is discarded rather than rebased
onto, and its files become orphans for object-storage GC. A retried commit
re-stages the same updates onto the same pristine base and never stacks them.

The pinning alternative (`RESOLVE_FAIL` on the read version) cannot express that:
it can only refuse. Refusing wedges the channel permanently, because the base
version comes from etcd, which the lost acknowledgement never advanced — every WAL
replay would reproduce the same refusal.

Single-writer-per-segment during flush is what makes discarding safe: the
dispatcher serializes commits per segment, and the stats tasks that share a
manifest only run once the segment is flushed.

## What is not accounted

BM25 stats are deliberately outside the write buffer's memory budget. They used
to be counted: folded into `insertBuffer.size`, handed to the task as payload,
and released by `Prepare` — while `SyncPack.ReleaseData` deliberately keeps them
alive until `Commit`. The accounting therefore claimed the memory was gone during
exactly the window it was still held, and `Abandon` did not clear them either.

They grow with distinct terms rather than with rows, so they are small next to
the row payload. Counting them consistently nowhere beats counting them wrong;
the alternative — a third accounting bucket released at Commit and Abandon — buys
precision the flush path does not need.

## Task ownership outside the write buffer

`SyncTask.Abandon` is the only way to release a task's payload and prepared
storage handle, and the write buffer calls it for tasks it owns. Import has no
write buffer, so it settles its own tasks (`importv2.ReleaseSyncTasks`) on every
exit path — after awaiting them with `conc.BlockOnAll`, since releasing under a
live writer is worse than leaking. `conc.AwaitAll` is the wrong primitive here: it
returns at the FIRST failed future and would leave later tasks inside
Prepare/Commit while their payload is pulled away.

Import also has no single owner deciding a segment's physical layout. Concurrent
files can target the same segment, and each derives its column groups from its own
batch's statistics, so every task reads the CAS winner back from the metacache
rather than freezing what it computed. Writing files under two layouts and
publishing one is an unrecoverable column-group mismatch.

## Follow-ups

- DataCoord's `Flushing` state is dead: nothing writes it, and
  `handleFlushingSegments` can never find anything. Cleaning it up is independent
  of this change.
- `HandleLoonFFIResult` flattens every FFI failure into `ErrLoonTransient`,
  discarding the segcore code it already carries. Until that is fixed,
  `isLayoutMismatch` has to match on message text, and conflict cannot be
  distinguished from a transient error at the manifest layer.
