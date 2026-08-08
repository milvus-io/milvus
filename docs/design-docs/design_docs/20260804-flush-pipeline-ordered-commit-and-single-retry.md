# Flush pipeline: ordered commit and a single retry owner

Status: implemented
Date: 2026-08-04

## Problem

Two problems in the DataNode flush path, with one shared root cause.

**1. A flush task could not fail.** `getSyncTask` yields rows *out* of the write
buffer into the task; the buffer keeps no copy. So a discarded task means the
rows are gone from memory while the channel checkpoint still points before them.
Nothing owned that data any more, and nothing could re-run the task. The only
correct response left was to crash and let WAL replay redo the work — which is
literally what the code did:

```go
// options.go, before
// errorHandler terminates the process. It is the last resort ...
errorHandler: func(err error) { panic(err) }
```

Because reaching the top meant a panic, every layer that could fail grew its own
"keep retrying". Five layers accumulated: the blob writer, the V2 insert writer,
the V3 manifest commit, the meta writer, and the write buffer's growing-source
timer. None could be removed without risking a crash.

Worse, `retry.Do` short-circuits `InputError`, so the flush path passed
`retry.RetryErr(func(error) bool { return true })` — retry *everything*,
including deterministic failures. A layout mismatch would then spin forever with
the checkpoint pinned, which is why `growingSourceSyncFatal` had to be invented
later to carve the exceptions back out. The classifier existed only to undo the
blanket retry, which existed only because failure meant panic.

**2. One segment could only have one flush in flight.** Storage writes dominate
flush latency, but they were serialized per segment because the metadata they
publish must be ordered.

## Design

### Two phases per task

`Task` has exactly one shape, with no optional methods:

```go
type Task interface {
    ...
    Prepare(context.Context) error   // storage work; runs out of order, in parallel
    Commit(context.Context) error    // metadata publication; strict FIFO per segment
    HandleError(error)
    UncommittedBytes() int64
}
```

`Prepare` is slow and independent. `Commit` advances segment counters and
checkpoints the next task reads, so it is ordered. Both `SyncTask` and
`GrowingSourceSyncTask` implement it; there is no adapter and no fallback path,
so a task missing a method fails to compile rather than silently degrading.

### The dispatcher is only an executor

`reorderDispatcher` runs `Prepare` concurrently across the node and publishes
`Commit` — and every completion callback — in submission order per segment. It
does not retry, throttle, or bound anything. `Submit` never blocks.

A task's callbacks run *before* it gives up its place in the queue: the next
`Commit` depends on the checkpoint and segment state those callbacks publish.

When a task fails, the whole suffix of its segment is aborted with it. The tasks
behind it were built against state its `Commit` was supposed to publish, so they
can only be replayed together, in order.

### The write buffer owns the queue and the retry

One owner, one queue: `ordinarySyncQueues`. `segmentReorderWindow` (5) lives
there too — the sync manager just executes what it is handed.

A failed task keeps **everything the retry depends on**: its payload, its place
in the queue, its metacache syncing counters, and its prepared storage handle
(see "Who may release a task"). "Failed" became a representable, recoverable
state, which is what makes retry a choice instead of an obligation.

`driveRetries` runs at the head of `BufferData`, so **timeticks are the retry
clock** — no timer anywhere in the flush path. It re-submits from the *oldest*
pending task of the segment, gated by `dataNode.flushRetryInterval` (3s) so a
dense timetick stream cannot become a retry storm. Drop is the one place that
survives without timeticks, so it drives the same primitive itself.

### Retry, in exactly two places

| layer | scope | budget |
|---|---|---|
| writer / meta writer | one PUT, one RPC | `ioRetryOptions(attempts)`, default **3**, ≤1s |
| pipeline | the whole task | timetick-driven, `dataNode.flushRetryInterval` |

The inner budget is short on purpose. An unbounded retry there would hide an
outage from the layer that owns ordering and memory backpressure — the outage
would never produce the backpressure it is supposed to produce.

**How long the inner retry should be depends on how expensive the next layer's
retry is.** Flush re-drives a failed task from its own queue: cheap, so 3
attempts. Import has no such queue — a failure fails the whole `ImportTask` and
DataCoord re-reads and re-parses the entire file — so it raises the budget via
`dataNode.import.maxWriteRetryAttempts` (0 = unlimited). Same mechanism, same
classifier, different budget.

### One physical layout per segment

Column groups are derived from a batch's own column statistics, so two tasks
that both observe an empty `currentSplit` compute *different* layouts. Under
parallel Prepare that is a real race, and its symptom — files written under one
layout, a manifest published under another — surfaces as an unrecoverable
column-group mismatch far from its cause.

Deferring the metacache write to Commit does NOT fix it: the divergence happens
at read time, when both tasks see nothing. The layout has to be agreed on
*before any file is written*. Two mechanisms, and the second is what makes it
safe rather than merely conventional:

1. The write buffer resolves the layout while task construction is still
   serialized, publishes it with `SetCurrentSplitIfNil`, and freezes it on the
   task (`WithFrozenColumnGroups`). Prepare then only consumes it.
2. A task that arrives unfrozen — import, and any future caller — resolves
   through `resolveSharedColumnGroups`, which compare-and-sets its own layout
   into the metacache and then **reads the winner back**. Whoever loses the CAS
   writes its files under the winner's layout.

So correctness does not depend on every caller remembering to freeze. Freezing
is an optimisation that decides once; the CAS is the invariant.

### Committing the manifest exactly once (V3)

Retrying a task means its `Commit` can run again after an attempt whose outcome
we never learned. The manifest commit is the one step where that is dangerous,
because a loon transaction stages **additive** operations —
`add_column_group`, `append_files`, `add_delta_log` — plus `update_stat`, which
is the only replace-by-key one.

The strategy the flush path used to pass, `LOON_TRANSACTION_RESOLVE_OVERWRITE`,
resolves a conflict by *re-reading the latest version and re-applying the staged
changes on top of it* (documented on `getRetryLimit`). That is correct for the
stats tasks it was written for — replaying `update_stat` is idempotent — and
wrong for a flush, where replaying registers the same files a second time.

Nothing else writes a segment's manifest while it is being flushed: commits are
serialized per segment, and the stats tasks that share a manifest only run once
the segment is flushed. So a version beyond the one we based on can only be our
own previous attempt. `preparedV3Write.Commit` therefore checks before it
stages anything:

| storage's latest | meaning | action |
|---|---|---|
| `== base` | nothing landed since we read | commit (strict, no auto-resolve) |
| `== base + 1` | our previous attempt landed, unacknowledged | **adopt it, commit nothing** |
| anything else | the single-writer premise is broken | terminal error, report loudly |

Adopting is right because that version's content already *is* the intended
result. Writing another one on top of it would duplicate; writing one that
supersedes it is not expressible — the transaction API has no operation that
replaces a manifest wholesale.

The third branch is what keeps the other assumptions honest: if versions do not
advance by exactly one, or another writer does appear, this fails loudly instead
of quietly producing wrong data.

### Who may release a task

A retryable failure keeps the task alive, so releasing anything it owns on that
path destroys what the next attempt needs. Ownership is therefore explicit:

- `HandleError` only **reports** — failure callback and metrics.
- `Abandon` **releases** — payload and prepared storage handle — and only the
  write buffer calls it, only on a terminal outcome.

There is deliberately no narrower "release just the payload" helper. One existed
(`ReleasePayload`), it also destroyed the prepared manifest handle, and its name
made that invisible.

`SyncTask.Commit` asserts the invariant rather than trusting it: a V3 task that
arrives with neither a prepared handle nor a manifest path fails loudly, instead
of falling through and reporting an empty manifest as success.

### One classifier

`ClassifySyncError` returns `SyncRetry` / `SyncTerminal` / `SyncCanceled`, and
replaces `growingSourceSyncFatal`, per-task `Retriable`, and the scattered `merr`
checks. Storage-layer permanence is deliberately not consulted: every loon FFI
failure is currently wrapped as `ErrLoonTransient` regardless of cause, so asking
would misclassify with confidence. Retrying is the safe side of that ignorance —
rows stay pinned and the checkpoint stays put. When storage classification
lands, it goes here, and only here.

### Backpressure

Admission-style bounding is gone from the dispatcher. What remains:

- per-segment: the reorder window plus `ordinarySyncBlockedLocked`;
- per-channel: `waitFlushCapacity` slows ingestion while a segment holds more
  than one flush's worth of data, counting buffered *and* retained payload —
  retained payload only grows while flushes fail, which is exactly when
  ingestion must slow down;
- node-wide: the `bufferManager` memory watchdog force-syncs and evicts.

`waitFlushCapacity` is bounded. The flowgraph delivers `BufferData` and an
eventual `DropChannel` on one goroutine, so an unbounded wait deadlocks a drop:
the `DropChannel` that would cancel the wait is queued behind the call that is
waiting. After the bound it proceeds; the memory watchdog still covers the
excess, and a wedged flowgraph would take the channel down with it.

## Code map

| file | role |
|---|---|
| `syncmgr/task_interface.go` | the single `Task` lifecycle |
| `syncmgr/sync_error.go` | `ClassifySyncError`, `ioRetryOptions` |
| `storagev2/packed/manifest_commit.go` | `CommitManifestUpdatesStrict` (no auto-resolve) |
| `syncmgr/reorder_dispatcher.go` | parallel Prepare, ordered Commit, abort-suffix |
| `writebuffer/ordinary_sync_coordinator.go` | queue ownership, retry drive |
| `writebuffer/growing_flush_coordinator.go` | growing-segment flush path |

## Configuration

| key | default | meaning |
|---|---|---|
| `dataNode.flushRetryInterval` | 3000ms | minimum gap between two flush attempts for one segment |
| `dataNode.import.maxWriteRetryAttempts` | 0 (unlimited) | import's writer retry budget |

## Not covered

- Storage-layer error classification (`ErrLoonTransient` collapses every loon FFI
  failure); until it lands, transient and permanent storage errors are both
  retried.
- A crash between the manifest commit and the DataCoord write leaves a committed
  version the replayed task cannot recognise: replay writes *new* files, so the
  version check lands in the third branch and stalls the segment loudly. Making
  that case recover on its own needs an idempotency key (the flushed WAL
  position) recorded in the manifest, which needs a stats key reserved for it.
- Two questions for the storage team, neither blocking: what `resolve_id = 1`
  is, and whether manifest versions are guaranteed to advance by exactly one.
  If they are not, the third branch turns it into a loud, immediate failure
  rather than silent corruption.
- Fault-injection coverage of each failure mode end to end; current verification
  is unit tests plus tracing by hand.
