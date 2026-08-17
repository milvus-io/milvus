# DataNode Flush Coordination

Status: implemented
Scope: `internal/flushcommon/writebuffer`, `internal/flushcommon/syncmgr`, `internal/datanode/importv2`

## Problem

The DataNode flush path grew two data sources — payload buffered locally, and
rows still pinned in a segcore growing segment — and each grew its own
scheduling, retry, completion and shutdown machinery: near-copies that had
already drifted apart. This document states the unified model: one
coordinator, one payload interface with two implementations, and only the
axis that genuinely differs — where the bytes live — expressed as a
difference.

## Segment state machine

The state machine lives in the DataNode's metacache. DataCoord's smaller one
shares the `commonpb.SegmentState` enum but not its meaning — nothing writes
`Flushing` on the DataCoord side.

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
| `Flushing → Flushed` | task commit succeeded | task `Commit` | local |
| `* → Dropped` | collection / partition drop | drop message | WAL |

The first two transitions are decided by WAL position; the last two by local
scheduling. **What data belongs to a segment is decided by the WAL; when it
is flushed is decided by the DataNode.** `Sealed` and `Flushing` answer
different questions — "does this segment still accept writes?" (no) vs "is
this flush claimed, with its content fixed?" (yes); collapsing them loses
policy idempotency (every timetick would re-issue a task) and the ability to
retry *the same flush* rather than re-deciding what to flush.

### The claim is one-way

`getSyncTask` claims `Sealed → Flushing` and immediately calls
`payload.Snapshot`, both under the same `writeBufferBase.mut` critical
section. The seal arrived **in band** on the same single-threaded flowgraph
(`dmStreamNode → ddNode → writeNode`), so every row of a `Sealed` segment is
already recorded: whatever the snapshot takes IS the segment's tail. A task
that fails to build or to commit leaves the segment in `Flushing`;
`GetSealedSegmentsPolicy` selects `Flushing` ahead of `Sealed`, so the retry
resumes **that** flush. There is no `Flushing → Sealed` rollback, and
selection performs no state change at all: it cannot know whether the
segment's source can produce the flush yet (`Snapshot` may fail for a ref
payload), so claiming there would mean claiming optimistically and undoing on
failure — and an undoable claim is one another path can observe half-done.

## One payload interface, two implementations

`segmentPayload` (`segment_payload.go`) is a segment's unflushed insert data.
The coordinator does not know where the bytes live; it knows only debt,
positions, and flush attempts:

- `Buffer` — absorb one insert pack (owned: copy rows in; ref: record batch
  positions/rows only).
- `Snapshot(ctx, throughTs)` — fix the next attempt's content, named by a
  `snapshotID`. Owned: move the rows out (ownership to the task), record
  their start position as a **floor**; cannot fail. Ref: freeze the fence
  `(lastFlushedTs, throughTs]` over the ledger and pin the source; **may
  fail** (source Unavailable/Pending) — a first-class retryable debt, never
  terminal, nothing reserved on the failure path.
- `CommitFlush(snapshotID)` — the **only** settlement. Owned: drop exactly
  that snapshot's floor. Ref: advance `lastFlushedTs`, trim the acked
  batches (`CommitGrowingFlush` on the source stays inside the task, after
  metadata durability).
- `AbortFlush(snapshotID)` — return what Snapshot reserved WITHOUT settling:
  floor and debt stay. An owned terminal failure keeps the floor pinned —
  the rows exist nowhere but the WAL; ref just unpins an untaken lease.
- `EarliestPosition` — the replay origin of everything not yet
  CommitFlush'd, **in-flight snapshots included**; absorbs the old per-entry
  checkpoint registry. Accounting (`UnflushedRows/Bytes/MinTimestamp/
  IsFull`) covers only buffered, not-yet-snapshotted content.

| | `ownedPayload` (`payload_owned.go`) | `refPayload` (`payload_ref.go`) |
| --- | --- | --- |
| Data lives in | this process (`InsertBuffer`) | a segcore growing segment |
| `metacache.FlushSourceMode` | `FlushSourceWriteBuffer` | `FlushSourceGrowing` |
| Task type | `SyncTask` | `GrowingSourceSyncTask` |
| Cost of a failed attempt | task retained — rows exist nowhere else | one round trip |
| Pin while in flight | floor list `[(startPos, snapshotID)]` | ledger batches, trimmed only by ack |
| `UnflushedBytes` | real resident bytes (evictable) | 0 — nothing evictable |

The mode decision is unchanged: sticky, at first insert
(`decideGrowingFlushSource`; `Pending` counts as ref — the birth race is the
normal case; fallback to owned when v3 is off, admission is fenced, or the
source is unavailable). It now selects the payload **implementation** at
buffer creation; from there both modes take the identical path — `getSyncTask`
takes ONE snapshot, and the presence of `input.growing` selects the task
builder. The delta buffer stays owned Go memory on every path (deletes go
exclusively to L0 segments, always owned); the coordinator yields it next to
the snapshot and widens the owned floor to the combined task start
(`widenFloor`), so delta-only L0 tasks pin their replay origin too.

The ownership difference decides exactly one downstream behavior, the queue
entry's lifetime (`writeBufferSyncEntry`): an owned entry — and the payload
its task holds — survives a failed attempt parked in the queue, re-submitted
as the SAME task object from the oldest pending one; a ref entry leaves the
queue on ANY completion, and a retry builds a FRESH task with a fresh source
lease. One completion function, `finishWriteBufferSync`, owns both: entry
identity, `ClassifySyncError`, payload settlement (success → `CommitFlush`;
failure that will not be resubmitted → `AbortFlush`), admission handling,
resync drive, observer/fatal callbacks.

### Debt is derived, not stored

A segment's flush debt is three observable facts, not parallel flags: the
metacache state — `Sealed`/`Flushing` owes a flush, `Dropped` owes a drop,
until the task carrying that flag commits (the old `owesFlush`/`owesDrop`
bits, deleted); a non-empty payload or per-segment queue; and the queue's
`flushIntent` — the retry debt, exactly two verbs. `want()` records the debt
without stamping `since`: a fresh debt is due immediately, one already
rate-limited stays rate-limited — used when the owner/reorder gate defers a
new task, where completion, not time, unblocks it. `attempted(now)` records
that an attempt was just made (a re-drive or a failure — the failed attempt IS
an attempt): the debt stays, the rate limit restarts. Every driver must call
it, or `since` keeps the FIRST failure's timestamp and `due()` fires on every
timetick — a retry storm; failure paths are `want()` + `attempted(now)`, and
unconditional restamping costs at most one extra interval on one retry.
`due(now, interval)` applies the interval at **drive** time, not stored, so a
changed `dataNode.flushRetryInterval` covers outstanding debts.

One intent per queue for BOTH modes: an owned retry replays the queue as a
unit; a ref retry has no parked entry, so the queue outlives its entries while
`intent.owes` (`removeWriteBufferSyncLocked`). Every driver reads the same
debt through `due()`, so no path can jump an interval a failure just imposed.

### One selection path

Policy selection (`getSegmentsToSync`) is the ONE trigger path for both
modes; the old dedicated growing selection loop dissolved into the standard
policy inputs (parity mapped in `TestRefPayloadSelectionTriggerParity`,
`write_buffer_test.go`):

| old growing trigger | unified carrier |
| --- | --- |
| `owesFlush` / `owesDrop` | metacache `Sealed`/`Flushing`/`Dropped` via `GetSealedSegmentsPolicy` / `GetDroppedSegmentPolicy`, admitted by `refPayloadSyncableLocked` |
| ledger reached a flush budget | `refPayload.IsFull` via `GetFullBufferPolicy` |
| stale progress | `refPayload.MinTimestamp` via `GetSyncStaleBufferPolicy` |
| committed-manifest replay pending | `refPayloadSyncableLocked` admits `pendingCommitted` regardless of batches |
| non-retryable failure parked | `refPayloadSyncableLocked` refuses; escalated loudly at failure time |

The selection filter also honors the queue intent's outstanding rate limit —
selection runs on every timetick, so without it a segment whose source just
came back `Pending` would be re-probed immediately, ignoring the retry
interval. The staleness policy adds a 0–10% random jitter of
`dataNode.syncPeriod` (`GetSyncStaleBufferPolicy`): ref segments buffer
nothing locally, so whole channels' worth cross the staleness boundary
together, and the jitter decorrelates that stampede.

### Checkpoint: exactly two derived pin classes

`GetCheckpoint` takes the minimum over two candidate classes, both DERIVED on
every call — the old `syncCheckpoint` registry (`checkpointCandidates`
Add/AddUnique/RemoveUnique keys) is deleted; nothing is registered, so no path
can forget to release:

1. **`payload.EarliestPosition()` per segment buffer** — buffered AND
   in-flight data, both modes. Owned: the insert buffer's start plus every
   in-flight snapshot's floor. Ref: the first uncommitted batch start; once
   all data is durable but a terminal metadata-only task is still owed or in
   flight, the last durable position (`lastFlushedPosition`) — deliberately
   older than the control pack that created the debt, so the checkpoint
   cannot pass the fence before the terminal action is durable. Buffers
   persist in `wb.buffers` across flush rounds precisely so these floors stay
   reachable; empty buffers are invisible to shape-based policies instead.
2. **The metacache seal pin** — `SetPendingFlushCheckpointIfNil`, installed by
   `sealActionLocked` in the same action that writes `Sealed`, read back for
   every `Sealed`/`Flushing` segment. It covers the metadata-only terminal
   flush from the seal instant — before any task exists, when the buffer may
   be empty — and vanishes when the segment leaves the metacache on commit.
   Set-if-nil under the Growing filter: only the actual `Growing → Sealed`
   transition pins, so a re-seal cannot move it.

A metadata-only DROP task pins **nothing**, deliberately (`Dropped` is
excluded from `pendingFlushStates`): drop authority is coordinator meta, which
converges with zero DataNode drop-ack even if the task never lands —
RootCoord's FastAck'd DropPartition callback retries `NotifyDropPartition`
until DataCoord's `meta.DropSegmentsOfPartition` marks every partition segment
Dropped, and a channel drop lands in `meta.UpdateDropChannelSegmentInfo` (via
the DropVirtualChannel ack-callback AND the recovery storage's
`dropAllVirtualChannel`), which drops every channel segment regardless of the
request payload. The residual cost of a crashed drop task is minutes of
Dropped-state latency, not correctness. Full trace: the pinning comment in
`getWriteBufferSyncTask` (write_buffer_sync_coordinator.go) and
`TestMetadataOnlyDropTaskDoesNotPinCheckpoint` (write_buffer_test.go).

Invariant, one sentence: **the channel checkpoint never advances past the
replay origin of anything not yet durable — data floors via (1), seal fences
via (2).**

### Node-wide payload admission

Per-segment reorder windows do not bound a node with many active segments, so
the sync manager keeps a node-wide task admission capacity of `maxParallel *
2` (minimum four). The write buffer reserves one slot **before** it snapshots
payload: reserve outside `writeBufferBase.mut` (admission may block, and
completion callbacks need that lock to settle older entries and release
slots) → lock and recheck lifecycle → claim `Sealed → Flushing` when needed →
`Snapshot` and register the `writeBufferSyncEntry` → unlock and submit.

An owned entry owns its slot for the materialized task's whole lifetime; a
retry reuses the same lease, and only success, terminal abandonment, or close
returns it — otherwise quick storage failures could park one payload per
segment while recycling the slot to materialize more, recreating the
unbounded-memory bug behind a different queue. Ref tasks own no yielded
payload, so their reservation is per attempt, returned by the completion
callback. The wait is a plain blocking call bounded by the graceful-stop
timeout, so a Drop queued on the same flowgraph goroutine cannot wait
forever; on timeout no payload has been snapshotted and no slot can leak (a
successful acquire racing cancellation releases the semaphore itself), and
the segment stays eligible for a later round. This is a task-count bound,
matching the previous dispatcher semaphore, not a byte quota; per-task size
remains governed by the existing flush-size and import-memory limits.

### Metric boundaries

The split dispatcher exposes phase metrics — `PrepareQueueDuration`,
`PrepareDuration`, `CommitWaitDuration`, `CommitDuration` — and two
compatibility aggregates: `QueueDuration = PrepareQueueDuration +
CommitWaitDuration`, `ExecuteDuration = Prepare + Commit + completion
callbacks`. The aggregate names are retained for dashboards, but their
baseline is not directly comparable with the old single-phase dispatcher.
Legacy write metrics keep one ownership rule across both payload modes: rows,
bytes and save latency are published when a **new physical write** finishes in
`Prepare` (replaying a committed manifest does not publish them again);
flush/auto-flush operation success is published after `Commit`, and a canceled
attempt is lifecycle rather than a failure.

### Retry drive cannot depend on a blocked flowgraph

Retries are dual-driven, over ONE structure — the per-segment sync queues. The
fast path is the channel timetick: `BufferData` runs on every msgpack,
including pure-timetick ones, and calls `driveRetries` before handling new
data. The backstop is a single `bufferManager` ticker (period from
`dataNode.flushRetryInterval`, clamped to [100ms, 1s]; the per-segment
interval is still applied at drive time) that sweeps every registered buffer
and drives its due retries under that buffer's own locking, bound to the
buffer's own `syncCtx`, never to any caller's wait. Running on its own
goroutine, it keeps working when the flowgraph goroutine is parked — on
node-wide admission or flush backpressure — or the WAL is idle, so a full
admission budget made of parked retry entries cannot self-deadlock and no
wait has to pump retries in-line.

One drive, two queue shapes: a queue with entries is the owned shape
(re-submit the same parked tasks, oldest first — the tasks behind it were
built against state its commit was supposed to publish); a queue whose entries
are gone but whose intent still owes is the ref shape (`dueRefRebuildsLocked`
rebuilds a fresh task when `refPayloadSyncableLocked` admits it, and
self-heals by deleting an orphan queue whose ref buffer is gone).

The one exception is `waitSyncsSettled`: `DropChannel`/`RemoveChannel` do
`GetAndRemove` **before** `Close`, so a dropping buffer has already left the
manager map, out of the ticker's reach — the drop wait keeps its own retry arm
(write-buffer queues only; ref progress during Drop is re-driven by
`syncDropSegment`'s own loop, the path that knows how to wait for a source).

### Terminal release and shutdown

`releaseTerminalSync` is the single definition of "this owned task is done for
good": discard its metacache syncing counters, release its payload and any
prepared storage handle. Both terminal paths — the failure branch of
`finishWriteBufferSync` and the parked-entry sweep on Close/abortDrop — call
it; the sweep also runs `AbortFlush` under the lock so the abandoned
snapshot's floor stays pinned.

`waitSyncsSettled` waits for write-buffer entries **and** every in-flight ref
sync together, under one `growingFlushCancelGrace`. The ref side signals
through the `growingSettled` generation channel rather than being polled;
`failGrowingSyncLocked` and `refPayload.CommitFlush`/`AbortFlush` are the
only ways to leave the in-flight state, and all broadcast, so the wake-up
cannot be forgotten. The wait is bounded after cancellation — and carries a
hard ceiling from the START (graceful-stop timeout), because the meta
writer's retry loop is driven by `wb.syncCtx` and a merely-unresponsive
coordinator never errors; an already-started native write takes no
cancellation token, so waiting past the grace turns the caller's timeout into
a hang. Giving up the wait is not giving up the work — the task keeps its
segment pin and finishes in the background — and the hard-bound return is an
explicit error, never a nil that would report a successful drop for data
still in flight.

## The rule every failure path follows

Two rules, both learned from defects this change had to fix:

**A task returns what it reserved, and that return is derived from the
task.** `releaseTerminalSync` (owned) and `settleFailedGrowingTaskLocked`
(ref) take only the task and its metacache syncing rows; the checkpoint needs
no per-task settlement, because the payload floors/batches ARE the pin and
settle through `CommitFlush`/`AbortFlush` on the entry's own snapshot. They
run unconditionally, never under a lookup of state a concurrent teardown may
have already removed — the ref failure path used to sit inside
`if progress, exists := ...`, so a callback landing after `abortDrop` skipped
it, leaving `syncingRows` inflated forever. A callback whose payload is no
longer the buffer's live payload settles ONLY its own snapshot record, so the
source lease and in-flight state cannot leak either way.

**Cleanup hangs off the point state disappears, not off a path that happens to
reach it.** The L0 partition→segment mapping was cleared only in the
`triggerSync` loop inside `BufferData`, missing every other route to a flush:
a segment flushed by the memory watchdog's `EvictBuffer` left a dead mapping,
the next delete recreated a buffer for an ID that no longer existed, every
sync died on segment-not-found, and that buffer — and the checkpoint it pins —
never moved again. `rotateL0SegmentLocked` therefore retires the mapping at
task construction, before the snapshot is taken, so later deletes cannot join
a segment whose payload has already moved into a task.

The same rule explains what `abortDrop` must NOT do. It declares its data
un-committed and promises the checkpoint stays pinned for WAL replay — so it
may not clear `buffers` (owned and ref payloads alike), the candidate class
whose floors and ledgers `GetCheckpoint` pins on; with them gone the
checkpoint falls back to the latest CONSUMED position, past data that was
never written. Nothing leaks by keeping them: the buffer is unreachable after
abortDrop and dies with its maps.

Derived debt also self-heals where stored debt used to stall or lie: a drop
debt whose segment is missing from the metacache is refused rather than
re-driven (`owesDropLocked` reads the metacache, so `syncDropSegment`'s loop
terminates, and `dueRefRebuildsLocked` deletes an entry-less queue whose ref
buffer is gone) — while the inverse, a ref ledger with recorded batches
outliving its metacache segment, is a loud `Fatal` (`getSyncTasksLocked`):
silently discarding checkpointed data is the one thing the debt model must
never do.

A retry must also see a quiet queue: `needsRetryLocked` requires that no entry
is still submitted, because a re-drive replays the whole queue from its oldest
task, and a second submission against an already-aborted dispatcher key
finishes synchronously — the terminal branch would `Abandon` a task whose
first `Prepare` is still writing, pulling the payload out from under the
writer and orphaning the native handle it assigns afterwards.

## The flush range is a pair of WAL positions

Two representations, one per side, deliberately asymmetric. **DataNode,
DataCoord and recovery keep the full `MsgPosition`** — the MsgID is the only
thing a WAL can seek by, so it is what gets persisted (`SaveBinlogPaths
CheckPoints[].Position` → the segment's DML position) and what recovery
resumes from. **The source side consumes only the timestamp projection** — the
range handed to segcore is `(startTs, endTs]`, resolved inside segcore against
the segment's own rows via `get_active_count(ts)`, the same `upper_bound` the
query path uses for MVCC visibility, bounded by the acknowledged insert
prefix. The projection is sound only within one vchannel, where TimeTick order
is monotonic and message timestamps are unique; timestamps from different
physical channels are NOT comparable, and nothing in this design compares
them — every fence, watermark (tsafe) and checkpoint named here lives on the
one channel the write buffer owns.

The two fences: the lower fence is the position the segment was last flushed
through (`refPayload.lastFlushedTs`, seeded from the metacache
`LastFlushPosition`, which recovery restores from the DML position); the upper
fence is the newest pack recorded at or before the snapshot's `throughTs`, and
the task publishes exactly that position — the full MsgPosition, not merely
its timestamp — as its checkpoint, unchanged (`flushTargetThrough`).

No row count crosses the boundary in either direction: row offsets exist only
inside segcore, and a restart rebuilds the segment from WAL replay with
offsets starting over at zero. The previous protocol — a DataNode
`targetOffset` reconciled against segcore's `AckedRowCount` — required two
independently-maintained counters to converge; a divergence had no way to
self-correct and stalled the segment silently.

`upper_bound` semantics make the boundary exact: a whole insert request shares
one timestamp, so a fence can never split a request; a pack's rows are all
`<=` its end position's timestamp and the next pack's are strictly greater, so
a fence can never split a pack. Consecutive flushes therefore partition the
rows — every row written once, none skipped (asserted by pk values in
`FlushFenceSemanticsWithGroupedTimestamps`,
`internal/core/src/segcore/flush_growing_segment_test.cpp`).

Readiness is a raw watermark read, never a wait: the task refuses to run while
the source's `TSafe() < endTs`, surfacing a retryable error re-driven on the
retry cadence. It must not block — the flusher and the source are independent
readers of the same WAL with no backpressure edge, so "wait for the source" is
an unbounded wait. And it must not use the delegator's `waitTSafe`, whose
external-table and `DowngradeTsafe` escape hatches report success without the
watermark having advanced — acceptable for a slightly stale read, data loss
for a flush. Behind is a normal outcome; nothing is lost while it lasts,
because unflushed batches keep `firstUncommittedPosition` pinned.

A retry of a flush whose data landed but whose metadata ack did not
(`pendingCommitted`) replays the FROZEN attempt — its manifest, checkpoint,
row count and finalization flags — never re-deriving them from live state,
which by retry time may already include newer packs or a concurrent seal or
Drop. In particular, a Drop arriving after a periodic T1 manifest was written
is not ORed into that replay: that would publish the T1-only manifest as a
drop and discard a later T2 tail. Drop is a monotonic debt read off the
metacache: the replay settles exactly T1, and its completion sees the
`Dropped` state (or a surviving batch tail) and re-drives a new drop task
from the remaining live progress, covering T2 before removing the segment.
The same monotonicity covers a flush overtaken by a channel drop mid-flight:
a ref segment mid-flush when `Close(drop)` runs is marked `Dropped` before
its final task is built (`submitDropSegment`), and drop supersedes flush on
every completion path — a committed flush task neither marks the segment
`Flushed` nor removes it while the metacache says `Dropped`
(`finishWriteBufferSync`; the task's own Commit preserves an existing
`Dropped` state), and the drop debt drives the separate drop task after the
frozen attempt settles, the ledger pinning the checkpoint at each unsettled
range until both commit. Drop does not paper over an unavailable source:
`syncDropSegment` retries on `errGrowingSourceUnavailable` until its context
expires, then fails loudly and leaves the checkpoint pinned.

A final task can release the real growing source before a concurrent Drop's
metadata-only follow-up is constructed. `SourceFinalized` is therefore set
only when a non-nil source actually receives `CommitGrowingFlush`; the
refPayload keeps an explicit proof bit (timestamp zero must not read as
"notified") plus the highest such fence in `sourceFinalizedThroughTs`. A
zero-row terminal attempt may skip reacquiring the source only when that proof
already covers its checkpoint (`Snapshot`'s `sourceSettlementSatisfied`);
otherwise source unavailability stays a retryable, checkpoint-pinning failure.

### Release waits the flush out instead of retaining its source

Nothing keeps a growing segment alive past its release so an in-flight flush
can finish; the ordering runs the other way — the release side must not drop
the segment until no flush still needs it. In growing-source mode the segment
is the only copy of the unflushed rows, so a flush whose source is dropped
mid-flight can never be completed by anyone: every rebuild fails with
`errGrowingSourceUnavailable` and the ref ledger, trimmed only by a commit's
ack, pins the channel checkpoint forever.

The release-manual-flush prepare therefore fences admission, appends a
`ManualFlush` (whose timetick is the fence), and blocks in
`WaitGrowingFlushDrained` until no segment on the channel still owes a
growing-source flush; only then does the querynode drop its growing segments.
It deliberately does not wait for the write buffer to consume up to the fence
first: the drain's predicate keeps reporting a growing-source segment as
owing until it is actually Flushed, so the drain waits the ManualFlush out
regardless, and a pre-wait only delayed releases on channels that owed
nothing.

The wait alone is not airtight: the WAL keeps accepting inserts, so a segment
created around the release could still be admitted to growing-source mode and
lose its only data copy. The release therefore fences growing-source
**admission** — raised BEFORE the `ManualFlush` is appended, which is the
whole correctness argument: everything admitted before the fence was created
by an insert already in the WAL, and since WAL timestamps are monotonic the
later ManualFlush's fence timestamp is above all of them and seals every one —
they flush, and the drain converges; everything admitted after the fence is
refused growing-source mode and buffers its rows in the write buffer, where
they survive the release without the delegator. Fencing any later — even
between the append returning and the next statement — leaves a window for a
segment that is growing-source AND unsealed by that ManualFlush; it reads as
owing a flush forever, and the release blocks to its deadline. Admission and
the fence check share `writeBufferBase.mut`, so a segment admitted before the
fence always has its ref buffer by the time the drain reads the map; the drain
re-scans every growing-backed buffer rather than the caller's list. The fence
records the newest provider registration token and reopens only when a newer
token registers — i.e. the channel was re-subscribed locally. An abandoned
release therefore leaves the channel in write-buffer mode until
re-subscription: safe, and the cost of not having a rollback to get wrong.

The provider takes **no part** in the release: no permission state to
publish, validate, clear or roll back, no fence of its own to wait out (a
flush running ahead of the delegator fails its own `TSafe` check and is
re-driven — waiting here only converted that retry into a block), and no
lease count of its own (a flush holds its segment through `PinIfNotReleased`,
and `LocalSegment.Release` already blocks on that refcount). The delegator
source's `CommitGrowingFlush` is a no-op — it retains nothing past the pin —
and the `SourceFinalized` proof lives entirely on the DataNode side, in the
refPayload. The drain accordingly asks only whether a segment still needs its
growing SOURCE, not whether it still owes work: once a terminal task has
committed and notified the source (`sourceFinalized`, set at exactly one site
on that task's success path), whatever remains is a metadata-only replay that
`Snapshot` builds without reacquiring the source.

There is deliberately no fast-fail for "something owes a flush but no provider
is registered". Reaching it needs a delegator torn down while its own release
is mid-prepare, and `UnsubDmChannel` closes the delegator after the prepare
returns. An earlier version signalled it with `ErrChannelNotAvailable`, which
`UnsubDmChannel` classifies as structural — the exact opposite of what the
signal meant. That state now degrades the honest way: the drain blocks, the
release times out and the coordinator retries.

Two invariants to preserve when touching any of this: **release safety is the
drain plus the admission fence, and nothing else** — bookkeeping that merely
describes what those two already guarantee is state that can disagree with
reality, not a second line of defence. And **a guard must close a hole, not
narrow it**: the drain alone is narrowing (new debt can appear after it) and
the fence alone is narrowing (old debt survives it); only their combination
closes, and only because WAL timestamp monotonicity makes their boundaries
meet exactly. Anything that merely shrinks a window — an "am I still active?"
flag ahead of a pin, a pre-check ahead of an unsynchronised action — belongs
nowhere in this path.

`UnsubDmChannel` distinguishes why a prepare failed. Structural unavailability
— no streaming node or preparer in this process, the channel served by another
node, the WAL shutting down — means no local write buffer can be left owing a
growing-source flush (the feature is process-local, and a closing WAL's buffer
dies with it while the unadvanced checkpoint replays), so the unsubscribe
proceeds without the drain. A merely transient failure (service unavailable,
client closed, read-only WAL) on a channel with a registered growing-source
provider fails the unsubscribe instead: the local write buffer may be alive
with such a flush in flight, and the coordinator's retry performs the drain
this attempt could not. The transient guard keys off the **provider
registration alone**, never off the node's local growing-segment snapshot:
`GetGrowingFlushSource` answers `Pending` for a segment the QueryNode has not
materialised yet, and `Pending` is a sticky "choose growing source" — so the
write buffer can already own a ref ledger for a segment the snapshot does not
see, and conditioning the guard on a non-empty snapshot would drop exactly
that segment. The snapshot is a log field, not a predicate.

### Two teardown paths, two different protections

`UnsubDmChannel` is **not** the only way a growing segment is dropped.
`ReleaseSegments` with `DataScope_Streaming` (or `_All`) drops growing
segments of a channel that stays subscribed — issued by `ReleasePartitions` or
a target update, through the delegator and the direct worker call. It is
reachable in normal operation: a position fence flushes the `(start, end]`
prefix while the segment keeps taking rows, so "persisted prefix + live
growing tail" is the steady state, and dropping the tail strands the debt
exactly as an unguarded unsubscribe would.

| path | channel after | mechanism |
| --- | --- | --- |
| `UnsubDmChannel` | gone | fence admission, append `ManualFlush`, **block** on the drain; fail the RPC if it does not converge |
| `ReleaseSegments(Streaming)` | still subscribed and ingesting | check the debt of **those segment IDs**, nudge it with a `ManualFlush`, and return a **retryable error without removing anything**; never block |

The partial release must not reuse the channel-release mechanism: no
`FenceGrowingSourceAdmission` — the fence is channel-wide and reopens only on
re-subscription, so closing it to release one partition would degrade every
surviving partition of a live channel to write-buffer mode; no
`WaitGrowingFlushDrained` — the RPC must return, so the caller retries
instead, and by the retry the nudged flush has normally settled the debt.

The nudge is a plain `ManualFlush`, collection-scoped even though the check
is segment-scoped: a caller cannot scope a `ManualFlush` to segment IDs
(`ManualFlushMessageHeader.SegmentIds` is the shard interceptor's *output*,
and the one segment-scoped seal message, `FlushMessageV2`, is the segment
flush worker's message, not an external API). The extra segments the nudge
seals are simply flushed early — what a user `Flush()` does anyway.

**The nudge is rate-limited per (collection, vchannel); the refusal is not.**
The retry loop is the segment checker re-deriving the dist/target diff every
`queryCoord.checkSegmentInterval` (3s), so a stuck debt would otherwise append
one collection-wide fence every 3s — exactly when storage is already
unhealthy. One nudge per `10 × dataNode.flushRetryInterval` (30s by default)
is enough: the first ManualFlush seals the segments, and from there the flush
path re-drives itself on `flushRetryInterval`; the repeat still exists because
a segment created after the previous ManualFlush is not sealed by it. The
nudge is best-effort by design (`release_segments_nudge_limiter.go`): the
grant is consumed before the append, with no compensation on append failure —
harmless, because the debt is still reported on every check and the release
stays refused, so a suppressed or lost nudge never turns into allowing the
release. The limiter's map prunes entries older than one interval, so it stays
bounded by recently nudged channels.

The refusal itself is retried, not dropped: `ReleaseSegments` for
`DataScope_Streaming` is always issued with `NeedTransfer=true`
(`task/executor.go`), so it runs the delegator path, and the guard refuses
before `RemoveDistributions` — leaving the segment in the leader view, from
which `SegmentChecker.getGrowingSegmentDiff` regenerates the identical task
on the next checker round; the checker is the retry loop. The check must run
**before** `RemoveDistributions` and `AddExcludedSegments`, not just before
the worker call: excluding a segment stops it from ingesting further rows
while the write buffer still expects to pull them from it — worse than the
removal it was meant to precede. Structural unavailability short-circuits both
paths to the pre-existing behaviour: remove immediately, no error.

### Why the write buffer is the baseline, not the fallback

Growing-source flush is available only while a delegator for the vchannel is
serving **in the same process** as the WAL owner's flusher — the source
registry is a process singleton. Whether the optimisation is usable is decided
by LOAD state, which the write path does not control: an unloaded collection
(no delegator, no provider, every segment `FlushSourceWriteBuffer`) is a
steady state carrying live traffic, not a degraded one. `querycoord` prefers
to place the delegator on the WAL owner
(`assignChannelToWALLocatedFirstForNodeInfo`) but nothing enforces it, and
release-collection, balance, RG reshuffle, manual transfer and
repeated-channel eviction all unsubscribe a channel while its WAL — and its
write buffer — stays put. Writes stop for none of them, and fencing appends
for the duration of the drain would mean blocking ingestion on every balance.
So the release faces two populations, each needing its own mechanism:

| population | mechanism | why not the other one |
| --- | --- | --- |
| already `FlushSourceGrowing` | **drain** — flush them out before the source goes | they cannot fall back: the mode is sticky and their earlier rows exist only in the growing segment |
| not yet seen / created after the fence | **admission fence** — make them choose the write buffer | they have no history, so their rows can simply be buffered instead |

The fence is not redundant with "the provider disappears": at fence time the
delegator is still alive and serving — `UnsubDmChannel` runs the whole prepare
BEFORE `delegator.Close()` — so without it a new segment would resolve
`Usable` and start owing a flush the drain would wait for, forever. This table
describes the CHANNEL release only; a partial `ReleaseSegments` has no second
population to handle and uses no fence at all.

### What keeps a growing-source flush correct across a restart

A growing segment is rebuilt by WAL replay after any restart, and its row
offsets restart at zero. Nothing about the segment itself is ordered across
restarts. What holds instead is a chain: (1) a recorded-but-unflushed batch
pins the channel checkpoint at its start position
(`refPayload.EarliestPosition`), so the published checkpoint is never above
the `startTs` of a pending range; (2) both consumers resume from that same
persisted checkpoint, inclusively — the flusher through
`GetChannelRecoveryInfo`, the delegator through the querycoord target, both
sourced from `meta.GetChannelCheckpoint`; (3) both suppress the same
already-flushed prefix, keyed on the segment's `DmlPosition`, which also
seeds `lastFlushedTs`; (4) the range is resolved inside the segment by
**timestamp**, never by offset, so a rebuilt segment resolves the same rows;
(5) a row-count cross-check refuses to publish metadata if the two sides ever
disagree.

The head of a pending range can therefore never be missing: that would require
the persisted checkpoint to have advanced past it, which the pin forbids. Only
the TAIL can be missing — the delegator has not caught up yet — and that is
what `Pending` and the `TSafe < flushThroughTs` refusal exist for; both leave
the checkpoint unadvanced and retry. `Pending` means "behind but still
consuming", and both halves are load-bearing: a provider that has not started
or was deactivated will never catch up, and the caller turns the answer into a
sticky, irreversible decision.

## Error classification

All classification is in `ClassifySyncError`; every layer below returns its
error unchanged.

| Decision | Meaning | Effect |
| --- | --- | --- |
| `SyncRetry` | default — throttling, coordinator not ready | keep payload, counters and queue position; arm the segment's intent |
| `SyncTerminal` | no attempt can change the outcome | `releaseTerminalSync` + `AbortFlush`; checkpoint stays pinned for WAL replay |
| `SyncCanceled` | the caller went away | not a task failure, and never escalated |

`ErrSegmentNotFound` / `ErrChannelNotFound` are terminal: DataCoord saying the
target is gone cannot be fixed by trying again, and without an explicit case
the `SyncRetry` default would re-drive the task on every timetick forever,
pinning the checkpoint behind a segment that no longer exists. A ref terminal
failure is additionally escalated to the fatal handler after
`markNonRetryableFailure` parks the segment: a permanently parked ref segment
is an unbounded, alert-less stall — batches never trimmed, checkpoint pinned —
strictly worse than a crash, and the rows are still recoverable from the WAL.

`SyncCanceled` must be excluded from fatal escalation: `DataNode.Stop()`
closes the sync manager **before** the flowgraphs, so during a graceful stop
the dispatcher aborts in-flight tasks with `context.Canceled` while the write
buffer is still open; escalating that would make every drain-with-traffic
panic. On the ref side cancellation also keeps the failure streak untouched
and arms no retry that shutdown would never drive.

## Storage-v3 manifest commit

The flush path commits with `packed.CommitManifestUpdates`
(`LOON_TRANSACTION_RESOLVE_OVERWRITE`). `OverwriteResolver` applies the staged
updates to a **deep copy of the manifest that was read**, ignoring any newer
one, and commits the result as `latest+1`: a version that appeared since —
this handle's own lost answer, or a crashed incarnation whose DataCoord ack
never landed — is discarded rather than rebased onto, its files becoming
orphans for object-storage GC, and a retried commit re-stages the same updates
onto the same pristine base. The pinning alternative (`RESOLVE_FAIL`) can only
refuse, and refusing wedges the channel permanently: the base version comes
from etcd, which the lost acknowledgement never advanced, so every WAL replay
reproduces the same refusal. Single-writer-per-segment during flush makes
discarding safe: the dispatcher serializes commits per segment, and the stats
tasks that share a manifest only run once the segment is flushed.

## What is not accounted

BM25 stats are deliberately outside the write buffer's memory budget. They
used to be counted: folded into `insertBuffer.size`, handed to the task as
payload, and released by `Prepare` — while `SyncPack.ReleaseData` deliberately
keeps them alive until `Commit`, so the accounting claimed the memory was gone
during exactly the window it was still held. They grow with distinct terms
rather than with rows, so they are small next to the row payload; counting
them consistently nowhere beats counting them wrong.

## Task ownership outside the write buffer

`SyncTask.Abandon` is the only way to release a task's payload and prepared
storage handle, and the write buffer calls it for tasks it owns. Import has no
write buffer, so every accepted import task installs `importv2.releaseOnDone`
as its dispatcher completion callback (`Abandon` only after the task stopped
running), while `importFile`/`importL0` keep `conc.BlockOnAll` as a deferred
completion fence on every exit path: the callback prevents
payload/native-handle retention, the fence prevents the file worker from
returning its memory budget or publishing a terminal import state with a sync
still inside Prepare/Commit. `conc.AwaitAll` is the wrong primitive — it
returns at the first failed future.

Import also has no single owner deciding a segment's physical layout, and a
lazy Get-then-Add would let a second initializer replace the first segment's
manifest, statistics, row count, and layout. `importv2.initImportSegments`
therefore creates every request segment before file workers start, and
`NewSyncTask` refuses a missing segment instead of initializing one. Each task
derives column groups from its own batch, uses the metacache compare-and-set
to agree on one winner, and freezes the winner on the task: writing files
under two layouts and publishing one is an unrecoverable column-group
mismatch.

## Invariants

The audit list. Each holds by a mechanism, not by convention:

1. **`(startTs, endTs]` partitions rows across adjacent fences** — segcore
   resolves both ends by `upper_bound` over timestamps.
2. **The checkpoint never passes an unfulfilled replay origin** — derived
   from the payload floors/ledgers (kept in flight by `AbortFlush`) and the
   seal pin installed atomically with `Sealed`.
3. **`CommitGrowingFlush` runs only after metadata is durable** — the task's
   `Commit` orders meta-writer publication before source notification.
4. **The admission fence precedes the release `ManualFlush` append** — the
   preparer's call order plus WAL timestamp monotonicity.
5. **A drain error fails the release** — `WaitGrowingFlushDrained` surfaces
   its context error; `UnsubDmChannel` fails the RPC.
6. **Pending debt blocks unsubscribe by provider registration, never by the
   local segment snapshot** — the guard keys off `ProviderCount`.
7. **Retries survive a parked flowgraph** — the bufferManager ticker, on its
   own goroutine, bound to the buffer's `syncCtx`.
8. **The claim is one-way; an empty-buffer seal produces exactly one
   metadata-only flush** — resync terminates once a `WithFlush` task commits.
9. **Node-wide admission is bounded; owned leases survive retries** — one
   slot per materialized payload for its lifetime; a reserve timeout returns
   before any snapshot is taken.
10. **A partial release refuses without removing** — the delegator-layer
    guard runs before `RemoveDistributions`; a suppressed nudge still
    refuses.
11. **A failed ref Snapshot keeps debt and pin** — nothing is reserved on
    its failure path; ledger and metacache state carry the debt, re-armed on
    the shared cadence.
12. **Drop settles through derived state** — a late callback settles only
    its own snapshot record; drop debt is `Dropped` plus a surviving ref
    buffer, re-driven until the drop commit deletes the buffer, refused when
    the segment is gone from the metacache.

## Follow-ups

- DataCoord's `Flushing` state is dead: nothing writes it, and
  `handleFlushingSegments` can never find anything. Cleaning it up is
  independent of this change.
- `HandleLoonFFIResult` flattens every FFI failure into `ErrLoonTransient`,
  discarding the segcore code it already carries. Until that is fixed,
  `isLayoutMismatch` has to match on message text, and conflict cannot be
  distinguished from a transient error at the manifest layer.
