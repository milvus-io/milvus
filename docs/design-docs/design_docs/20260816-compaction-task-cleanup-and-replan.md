# Compaction Task Cleanup and Replan Under a Fresh Plan ID

## Document Information

- Date: 2026-08-16
- Status: Proposed (PR #52277)
- Language: English (canonical)
- Components: DataCoord / DataNode
- Primary scope:
  - `internal/datacoord/compaction_inspector.go`
  - `internal/datacoord/compaction_task{,_mix,_l0,_clustering,_bump_schema_version}.go`
  - `internal/datacoord/compaction_task_meta.go`
  - `internal/datacoord/task/global_scheduler.go`
  - `internal/datacoord/garbage_collector.go`
  - `internal/datanode/compactor/executor.go`
  - `pkg/proto/data_coord.proto`
  - `pkg/util/paramtable/component_param.go`
  - `internal/datacoord/copy_segment_{task,inspector,checker}.go` (§3.8, §3.10)
  - `internal/datanode/importv2/task_manager.go`
  - `internal/datanode/index/task_manager.go`
  - `internal/datanode/task_sweeper.go`
  - `internal/datacoord/import_{checker,task_import,task_preimport}.go` (§3.9)
  - `internal/datacoord/{external_collection_refresh_manager,external_collection_refresh_checker}.go` (§3.10)
  - `internal/datacoord/task_refresh_external_collection.go` (§3.10)
  - `internal/datanode/external/manager.go` (§3.10)
  - `internal/datanode/importv2/task_copy_segment.go` (§3.8)
- Related:
  - Issue: [#52332](https://github.com/milvus-io/milvus/issues/52332)
  - [Collection level autocompaction switch](./20230511-collection_level_autocompaction_switch.md)

---

## 1. Summary

A compaction task owes its input segments exactly one thing when it terminates:
the **segment lock** (`SegmentInfo.isCompacting`). `canTriggerSortCompaction`
requires `!isCompacting`, and a flushed segment is published as
`Flushed + IsInvisible` whose only exit is a sorted replacement being committed.
So an input whose cleanup never runs is never re-sorted, and a segment that is
never re-sorted never leaves the growing query path.

This design does two things:

1. Makes cleanup reach **every** terminated task, by giving a task's state a
   single owner and taking the cleanup decision from an immutable snapshot.
2. Replaces every retry path with **one** mechanism: an attempt that ends
   without succeeding is rebuilt at cleanup under a **fresh plan ID and a fresh
   pre-allocated output segment range**, keeping the trigger ID.

The second requires a new persisted state, `CompactionTaskState.retrying`, so
that "this attempt is over but the work is still owed" is a fact on disk rather
than something re-derived later.

---

## 2. Background

### 2.1 Two owners of one task

A compaction task was driven by two independent components:

- **`compactionInspector`** — business logic: `Process()` advances the state
  machine, and cleanup settles what a terminated task owes.
- **`task.GlobalScheduler`** — dispatch: `CreateTaskOnWorker`,
  `QueryTaskOnWorker`, `DropTaskOnWorker`, all of which mutate the same task
  state from worker callbacks.

Both mutated the state with no handover. The callbacks took a *read* lock, which
never serialized two callbacks for the same task. Consequences observed:

- The cleanup decision was re-read after `Process` returned, with the lock
  released in between. A worker callback that fails to probe its node rewrites a
  terminal state back to `pipelining`; a task caught in that window left
  `executingTasks` without ever entering `cleaningTasks` — never cleaned, its
  inputs locked until DataCoord restarted.
- Recovery dropped a task it could not admit. Admission rejects a task whose
  inputs a snapshot protects — exactly the state that terminates a sort task — so
  the record vanished while its inputs stayed locked.
- `resetSegmentCompacting` ran twice for mix and L0, which can unlock segments a
  different compaction has legitimately re-acquired.

### 2.2 Retry was unsafe

`dataCoord.requestTimeoutSeconds` was 600s. Every call it bounds — `createTask`,
`queryTask`, `dropTask`, `QuerySlot` — is a control-plane operation a healthy
node answers in microseconds, and the scheduler waits on all of them per round,
so one black-holed DataNode stalled dispatch for ten minutes.

Shortening it makes failure common, and the pre-existing retry re-dispatched the
task **under the same plan ID and the same pre-allocated output segment IDs**.
That is not survivable when the worker may still be running the plan:

- clustering keys its partition-stats object by plan ID;
- a text index lands on fixed names under the output segment's base path.

A second execution overwrites artifacts the adopted result references, which
neither manifest versioning nor GC can undo.

---

## 3. Design

### 3.1 Single ownership

`task.GlobalScheduler` becomes the sole owner of a task's state.

- Worker callbacks hold the scheduler's per-task lock **exclusively**.
- `Finalize(taskID, fn)` removes the task from dispatch first — so no further
  plan can be handed to a worker — then runs `fn` under that same lock, waiting
  for any in-flight callback to drain. Cleanup runs inside `Finalize`.
- A **terminal-state worker drop is sent after the lock is released**, never
  inside it (§3.6). It is the one worker RPC that is not the callback's own
  business, and `Finalize` is the thing that would queue behind it.
- `Update(taskID, fn)` lets `Process` borrow the lock. It waits for any
  in-flight callback to finish, bounded by the callback's own work (normally
  one worker RPC, `dataCoord.requestTimeoutSeconds`).
- Worker callbacks bail out on any terminal state at entry, before any RPC or
  segment mutation.
- The cleanup decision comes from immutable snapshots taken around `Process`,
  for every compaction type.

Cleanup itself runs **off** the scheduling loop with a bounded fan-out —
membership in the in-flight set (guarded by the same lock as `cleaningTasks`)
is the cleanup slot, so one check deduplicates dispatch and caps concurrency —
while
`cleaningTasks` keeps excluding the task's channel and label from scheduling.
Anything slow that is not cleanup — the worker-side plan drop, the analyze abort
— runs on its own goroutine after cleanup has released its slot. Together with
the rule that the scheduler's own drops are sent outside the per-task lock
(§3.6), this is what keeps an unresponsive DataNode from pinning a cleanup slot
or a channel exclusion: no path holds either behind a worker RPC.

### 3.2 States

```
pipelining → executing → meta_saved → completed → cleaned
                      ↘ retrying / failed / timeout → cleaned
```

`retrying` (proto value 11) means: **this attempt ended without succeeding and
the work is owed a rebuild under a fresh plan ID.** It is terminal for the
attempt, not for the trigger.

`setAttemptEnded()` is the single place that ends an attempt. It decides there
and then:

- `retrying` while `dataCoord.compaction.maxAttempts` still leaves a rebuild;
- `failed` once it does not.

Deciding at the moment of failure rather than re-deriving it later is what keeps
the answer stable: `maxAttempts` is refreshable, so a cap lowered mid-flight must
not retroactively turn a task already queued for a rebuild into a settled
failure. Every path that ends an attempt also records a `fail_reason` — a
worker's timeout, an unanswered round, an unsupported state, or the worker's own
reason — so a task that keeps failing leaves something to diagnose beyond "it
failed".

Two predicates read the state and nothing else:

- `isRetrying(task)` — `state == retrying`, and nothing else.
  Used by cleanup **and** by `summaryCompactionState`, so the two cannot
  disagree. It reads no configuration: the cap was already spent by
  `setAttemptEnded` when it wrote the state, and re-deriving the decision from a
  refreshable parameter is exactly what this state exists to avoid.
- `needsCleanup(state)` — terminal and not yet `cleaned`.

`GetCompactionState` counts a task owing a rebuild as running. Before this, a
trigger reported `Completed` the moment its task terminated — before cleanup,
before the replan — while the caller's trigger ID was about to be reused by the
rebuild.

For the global scheduler, `retrying` maps to `taskcommon.Failed`: the scheduler
owes the task nothing further. The inspector, not the scheduler, does the
rebuild.

### 3.3 Replan under a fresh plan ID

At cleanup, a task owing a rebuild is rebuilt as a new record that:

- keeps `TriggerID` (what `ManualCompaction` handed the client and what
  `GetCompactionState` looks up);
- takes a **fresh `PlanID`** and a **fresh, disjoint `PreAllocatedSegmentIDs`
  range of the same size** — the count is the only thing that matters, and the
  old range already carries it, so no per-type sizing rule is reproduced;
- clears `ResultSegments` / `TmpSegments` — results belong to the attempt that
  produced them;
- increments `RetryTimes`;
- for clustering, allocates a **new analyze task ID** and resets
  `AnalyzeVersion`, so the analysis re-runs.

Whatever a zombie worker still writes under the old IDs is unreferenced and
reclaimed by GC.

Carrying the old analyze task over would save a k-means pass, but it would make
the old record's cleanup conditional on the rebuild succeeding — cleanup would
have to be told to leave the analyze record alone *before* the replacement is
known to exist, and every way the rebuild can fail would then strand that record
and its files. Re-running the analysis costs a rare clustering retry some time;
getting the ownership handoff wrong leaks forever, on the paths hardest to test.

### 3.4 The debt must be durable at every instant

`Clean()` overwrites the state with `cleaned`, which is the old record's last
claim that the work is owed. Everything that can fail therefore happens **before**
the clean:

| Step | Where | Can fail |
|---|---|---|
| `AllocID`, `AllocN`, analyze `AllocID` | `buildReplacement` | leaks IDs only |
| `AllocTimestamp` | `buildReplacement` | leaks IDs only |
| **write the replacement record** | `buildReplacement` | nothing persisted |
| `Clean()` the old record | `Finalize` | both records remain, round retried |
| admit inputs (`CheckAndSetSegmentsCompacting`) | reconciler | in-memory |
| push onto the queue | reconciler | in-memory |

So the debt is recorded at every instant: first on the old record, then on both,
then only on the new one. After the clean nothing touches the metastore, so
nothing there can lose it.

The replan path deliberately does **not** route through `enqueueCompaction`:
that would re-persist the record, i.e. add a metastore write on the one path
where a failure has nowhere to put the debt back.

**Normal producer handoff.** The persist→enqueue gap — a durable `pipelining`
record with no runtime owner — is closed by making the queue unable to refuse.
`CompactionQueue.Enqueue` always accepts; the limit is consulted once, by
`enqueueCompaction` before it claims inputs and persists anything, and never
enforced afterwards. A failure before persistence releases the input claim; after
persistence, publishing cannot fail.

The limit is therefore **advisory**. Concurrent producers can all pass the check
and push the queue a few items past it, and the scheduler's own put-backs (a task
popped for an exclusion decision that turns out to be excluded) can too. That
overshoot is bounded by the number of producers plus one round's exclusions —
single digits — and it is the deliberate trade. The alternative is an exact
limit, which requires refusing a task at `Enqueue`, which means refusing a task
that is already durable. There is nothing good to do with such a task: its inputs
are marked compacting, nothing in memory drives it, and it waits for a restart.
Being a few over a 100000-item limit costs nothing; being exact costs stranded
work.

An earlier iteration achieved exactness with capacity reservations claimed before
persisting and converted afterwards. It worked, but every producer, every
scheduler pop, and every put-back had to participate in the protocol, and
recovery grew a branch that **erased** a durable task when the queue happened to
be full at startup — process-local, transient state destroying persisted work.
Removing the reservations removed that branch with them: `loadMeta` now takes
every durable task unconditionally.

**Driving the replacement: a stateless reconciler.** After the clean, the
replacement is a persisted `pipelining` record that no queue holds. One sweep on
the schedule loop (`reconcileReplacements`, every `scheduleInterval`) drives
every such record to one of its two terminal ownership states — queued, or
erased — re-deriving everything from persisted meta each round. There is no
in-memory ownership state: a record the round cannot finish is simply found
again on the next one, and after a restart the same sweep resumes from the same
persisted facts. Steady-state retry and crash recovery are one code path.

The sweep's scope is exact without any new persisted field:
`state == pipelining` and not present in the queue, `executingTasks`, or
`cleaningTasks`. `enqueueCompaction` claims, persists, and queues in one call;
the writers that leave a pipelining record unowned are `buildReplacement` (a
replan, persisted before its predecessor is cleaned) and `loadMeta` deferring
a blocked admission. A reconciler racing an in-flight `enqueueCompaction` is
turned back by the claim conflict and retries once the record is queued.

Each candidate is decided by predicates over the same scan, in order:

1. **Inputs overlap a record that still owes cleanup** (terminal, not
   `cleaned`): the predecessor still holds them — its clean releases them last —
   so admission can only conflict. Wait.
2. **Inputs overlap any other live record**: wait. Overlap proves only
   contention, not equivalent work. For example, clustering `{1,2}` is not
   satisfied by mix `{1,3}` merely because both contain segment 1. Erasing on
   overlap would discard segment 2 and the clustering semantics while making
   the original trigger report completion.
3. **Queue full**: wait, without claiming and releasing the inputs.
4. **Admit and queue.** An admission conflict is transient: it is either the
   predecessor's clean between its durable cleaned-write and its in-memory flag
   release, or another task's current claim. Snapshot protection is transient
   too; it pauses compaction but does not satisfy it. Both verdicts retain the
   durable replacement for a later round. Only a decisive verdict such as all
   inputs being gone erases the record.
5. **Queue push lost the last-slot race**: release the claim; the record
   retries next round.

Two consequences of deriving from meta rather than remembering:

- **`cleaned` and absent are the same fact.** Cleaned records are GC'd on a
  timer measured from `StartTime`, so any predicate keying on "the predecessor
  record exists" would flip when GC runs. Rules 1–2 therefore key only on
  states that still hold inputs (`needs-cleanup`) or still claim work
  (non-terminal); a `cleaned` or missing predecessor reads identically as
  "released".
- **A clean that fails leaves both records, and the next cleanup round must
  reuse the same replacement.** `replacementForCleanup` accepts only a direct
  semantic successor: same trigger, type, collection, partition, channel and
  complete input set, with `RetryTimes == predecessor.RetryTimes + 1`. Any-input
  overlap is not an ownership edge; using it can steal another trigger's or
  compaction type's replacement. Erasing and rebuilding instead has an
  unrecoverable double-failure window: if the erase fails, the next round
  persists a second replacement over the first.

**Recovery rule.** A crash between the write and the clean leaves both records.
Admitting the replacement then would let the old attempt's cleanup release inputs
the new one is already compacting, and a third task could be planned on top of
it. `loadMeta` therefore **defers** a non-terminal record whose inputs another
record still owes a cleanup for: the record is left exactly as it is, neither
submitted nor claiming inputs — the predecessor's reconstructed claim covers
them — and the runtime pair (cleanup reuses it, the reconciler admits it) picks
it up as if the crash had not happened. The pair has exactly one cause:
admission makes inputs exclusive, and a terminated task keeps holding its inputs
until cleanup hands them back — every `doClean` writes `cleaned` before
`resetSegmentCompacting`, so a record that is still cleanup-bound still holds
its inputs.

Recovery treats every admission failure the same way, whatever the record's
`RetryTimes`: a pipelining record is deferred to the reconciler, an executing
one ends its attempt so cleanup rebuilds it under a fresh plan ID. Recovery
therefore never erases durable work over transient admission conditions -- a
full queue, a snapshot, or a reservation -- and a trigger cannot settle while
its record still exists.

Cost. A round first collects the plan IDs some runtime structure already owns —
one walk each over the queue, `executingTasks` and `cleaningTasks` — then makes
one clone-free pass over persisted meta (`compactionTaskMeta.Range`) looking for
candidates. In the steady state, where no unowned pipelining record exists, that is where
it stops: nothing is cloned and no admission is attempted. Only a round that
actually finds a candidate pays for the second meta pass that derives which
inputs are held, and for cloning the candidates it will drive.

### 3.5 When to stop

- **Attempt cap**: `dataCoord.compaction.maxAttempts` (default 10, refreshable).
  Enforced where the attempt ends (`setAttemptEnded`), not where it is rebuilt.
  Past it the work is left to the periodic trigger.
- **Giving up on a worker**: one rule for every RPC round, identical to the
  create path — **a round that ends without an answer ends the attempt**. The
  query already spent `dataCoord.requestTimeoutSeconds` (30s) on an operation a
  healthy worker answers in microseconds; abandoning is cheap by design (the
  replan cannot collide with whatever the worker is still doing) and the
  attempt cap bounds the churn. No counter, no clock, no per-type variation.
  `ErrNodeNotFound` is the same outcome with a better log line: a DataNode
  deregisters before tearing down its running compactions, so waiting learns
  nothing.
- The assignment is **persisted before** the create RPC; only
  `ErrDuplicatedCompactionTask` — proof the first attempt was accepted — keeps
  it.

### 3.6 Worker-side plan entries

The DataNode executor holds a finished plan's entry — its result binlog lists and
its compactor's plan — until something reclaims it. Two layers:

- **Best-effort release**, never retried and never special-cased: the plan's
  executor entry, plus (for a clustering attempt that failed while analyzing)
  its analyze job, which would otherwise keep a worker slot with nothing left to
  claim its result. Recovery sends none — re-sending would be a retry.

  Cleanup is not the only sender. The global scheduler also drops the plan when
  a task reaches a terminal state under its own dispatch — `schedule()` on a
  create that terminated immediately, `check()` on a query that ended the
  attempt — which for a replanned attempt is the ordinary path and runs before
  cleanup. A plan can therefore be dropped more than once, and that is harmless
  by construction: `RemoveTask` is idempotent, a drop for an executing plan is
  remembered and applied at completion, and a drop for an already-reclaimed
  entry is a no-op. Deduplicating them would mean tracking cross-component state
  to save an RPC that costs nothing when it lands twice.

  **Every one of these sends happens outside the per-task lock.** The drop is
  bounded only by `dataCoord.requestTimeoutSeconds`, so sending it inside the
  scheduler's critical section would let a node that stopped answering hold the
  per-task lock for a second full timeout on top of the one its query already
  spent — and `Finalize` waits on exactly that lock, so the stall would land on
  cleanup, holding its slot and channel exclusion for the duration.
  `TryAbortAndRemoveTask`, `schedule()` and `check()` all relinquish ownership
  under the lock and send afterwards.
- **One DataNode-wide expiration sweep** reclaims worker entries 24 hours after
  their start. DataNode records a `StartedAt` value when each task is admitted
  and one hourly loop applies the same cutoff to import/pre-import/copy-segment,
  index/analyze/stats, compaction, and external-refresh tasks. Individual
  managers own no ticker; they only expose synchronous `RemoveExpiredTasks`
  methods. This bounds every path a drop can miss without adding one cleanup
  goroutine per task family. Expired runnable work is canceled as its entry is
  removed. Compaction is the exception in mechanics, not policy: `Stop` waits
  for `Compact` to exit, so its entry is marked for deferred removal and kept
  until `completeTask` finishes with it. Measuring from start deliberately sets
  an upper bound on execution as well as result retention.
- A drop that arrives while the plan is still executing is remembered and applied
  at completion, instead of being reported as success and ignored.

### 3.7 Reclaiming partition-stats objects

`recycleUnusedPartitionStatsFiles` is added to GC. It is the only reclaim path
these objects have: the binlog orphan scan resolves a segment ID out of the path
and this layout (`{root}/part_stats/{collectionID}/{partitionID}/{vchannel}/{version}`)
carries none, and `cleanPartitionStats` only walks versions that reached meta —
a `PartitionStatsInfo` is persisted on completion only, so an abandoned attempt
leaves an object no metadata ever referenced.

A version is garbage when none of its durable owners names it — the clustering
task record that may still publish it (a terminal attempt keeps ownership until
it reaches `cleaned`), a `PartitionStatsInfo` record, or any segment's
`PartitionStatsVersion` — and the object is past `missingTolerance`. All three
sources are required: a segment can outlive the record that produced it, and the
delegator prunes with what the segment says.

### 3.8 The same rule for copy-segment

Copy-segment (the data mover behind snapshot restore) takes §3.5's "a round that
ends without an answer ends the attempt" rule, and takes §4's fresh-identity
rule with it. A Create RPC that does not return success, or a query that produces
no answer, replans the task as a **new record**: a fresh task ID, and a fresh
target segment ID for every mapping, carrying the source IDs and the planned row
counts and positions over unchanged. As with compaction, the worker assignment
is persisted before Create; this closes the accepted-RPC→failed-state-write
window, while a non-successful Create still abandons that identity immediately.
The assignment write is a compare-and-set on `Pending`: a concurrent failure
path may have marked the task `Failed` under a terminal job, and dispatching it
anyway would resurrect work the job has already settled.

Why the target IDs and not just the task ID: a copy target object key is a pure
function of the target segment ID (`{logType}/{collection}/{partition}/{segment}
/{field}/{logID}`, with only the first three substituted). Re-dispatching under
the same target IDs points two live attempts at identical keys, which is
precisely what nothing downstream can then tell apart. Index output already
rotated per attempt — a fresh buildID on every dispatch — so this closes the
half of the rule binlog output was missing.

- **Ordering.** A `Pending` replacement and all of its fresh target segments are
  published by one composite catalog update, with the task record first and
  `predecessor_task_id` naming the exact task whose work it takes over. The
  operation is atomic when it fits the backend transaction limit. Its ordered
  over-limit fallback writes the task first, so an interrupted write leaves a
  durable recovery owner rather than ownerless `Importing` segments. Both
  in-memory caches are updated only after the full operation succeeds; only then
  are the predecessor targets marked `Dropped` and its record retired. An
  ambiguous result never triggers an abort that could drop a durably published
  replacement target.
- **Recovery.** `reconcileReplannedTasks` follows the persisted predecessor edge,
  idempotently fills any targets missing from an interrupted owner-first write,
  and keeps the pair out of scheduling until publication and predecessor cleanup
  finish. A successor is unique by construction: before minting an identity,
  the replan adopts one already persisted for that predecessor -- its composite
  write may have landed while the response was lost -- so recovery never has to
  elect among several. It never infers ownership from one overlapping source
  segment: unrelated tasks may share a source while representing different
  work, and overlap is not an atomic handoff record.
- **Job completion is guarded too.** The checker finishing a job re-uses the
  task snapshot it already verified as all-Completed (a replan can swap the
  task set between the check and the flush), and both terminal transitions are
  a compare-and-set on `CopySegmentJobExecuting`: a concurrent failure or
  replan that moved the job on wins, so a `Failed` job is never resurrected as
  `Completed` and a replacement's unfinished work never flushes.
- **No worker drop from the query path.** It runs under the scheduler's per-task
  lock, and the drop is an RPC bounded only by `dataCoord.requestTimeoutSeconds`
  against the node that just failed to answer. Once the replacement is durable,
  an in-process fence reports the superseded attempt as terminal to the
  scheduler while its persisted record is retired rather than marked failed,
  so the job checker never sees a failure from a handoff that merely needs
  retrying; the scheduler therefore sends the drop after the unlock (§3.6).
- **A retired predecessor's worker drop is best-effort.** The live replan path
  retires the predecessor record and relies on the scheduler's terminal release
  to send the drop. If that single RPC fails, no retry exists — the record that
  named the node is gone, so the GC drop-retry cannot see it — and the DataNode
  entry then persists only until the shared 24-hour expiration sweep reclaims it
  (memory only, and bounded by 24 hours from the attempt's start; slots are
  freed when the attempt finishes). The restart path is covered: the reconciler
  reclaims a still-recorded predecessor's worker attempt before retiring the
  record. Sending the drop from inside the query callback is forbidden (§3.6),
  and a stranded-drop registry is not worth it for a few bytes of in-memory
  state lasting at most one retention window.
- **No object cleanup on the worker.** The DataNode deliberately never deletes
  the objects an attempt wrote:
  before this change those keys were shared with the attempt that replaced it,
  so "cleaning up its own files" could delete a live restore, and DataCoord's GC
  already reclaims them — a failed task drops its targets, and the binlog orphan
  scan removes whatever is left under a segment meta no longer names. Its task
  record and terminal result are normally reclaimed when DataCoord sends
  `DropCopySegment` (the coordinator ACK), with the DataNode-wide 24-hour sweep
  as the bounded fallback when that drop is lost.
- **Cap.** `dataCoord.copySegmentMaxAttempts` (default 10) bounds the churn;
  past it the job is settled as failed. The count rides on the existing
  `CopySegmentTask.task_version`. The proto additions are the exact predecessor
  edge and the worker-reported `CopySegmentTaskRetry` state (§3.10).

One prerequisite had to be fixed first: a StorageV3 segment abandoned before it
published anything has an empty `ManifestPath`, and `removeDroppedSegmentFiles`
treated that as a parse failure — returning before `DropSegment`, so the meta
entry survived forever, and while it survived the orphan scan kept every object
under it too. Rotating identities turns that from a rare restore-failure case
into a routine one, so it is now handled as what it is: a segment with no
manifest to remove.

### 3.9 Import terminal edges

**Import rotates its output segments on retry.** Import was the one task type
that did not satisfy §4's rule. Its segment IDs live in the persisted plan and
were reused across attempts; only the log IDs rotated. Under StorageV1 that was
enough -- an object key is a function of the log ID, so a straggler write from an
abandoned attempt landed on a key nothing referenced and the orphan scan
reclaimed it. StorageV3 broke the assumption: a segment's authority is its
manifest chain, addressed by (collection, partition, segment) alone. Two attempts
on one segment ID share that chain, and loon commits **rebase-merge on conflict
rather than reject**, so a straggler commit is absorbed into the very version the
replacement reports -- the segment carries both attempts' fragments while meta
counts one, surfacing later as a sort-compaction row-count mismatch. `DropImport`
cancels the old attempt but does not wait for in-flight writes, so the window is
real however promptly the drop lands.

`rotateImportTaskSegments` closes it by giving the retry an identity the
abandoned attempt cannot produce, exactly as compaction, copy-segment and
refresh already do:

- **Ordering.** Allocate and register the replacement origin IDs (no sorted
  targets are preallocated any more — the sort stage allocates its own output
  per origin); adopt them into the live fields together with the Pending reset
  in one write; retire the old segments.
- **Recovery.** A crash between registering the new segments and the adopt
  leaves ownerless Importing segments carrying no data and invisible to query;
  they are left for segment GC and are not worth a recovery scan. A crash after
  the adopt leaves the old segments to be dropped by a later round, which is
  harmless -- nothing references them any more. An **ambiguous adopt** — the
  write landed but the error was observed — is resolved by re-reading the task
  record: if it already references the new segments and is Pending, the adopt
  is durable and only the old segments are retired; dropping the new ones then
  would strand the task on its own Dropped output, hanging every later request
  assembly until the job times out.
- **The task ID does not rotate.** Unlike copy-segment, only the *output* is
  rebuilt, so there is no predecessor edge, no reconciler pairing, and no job
  bookkeeping: the swap is one task-record write. Pre-import has no output
  segments and is unchanged.
- **What this replaced.** The previous mitigation reset `NumOfRows` on the
  inherited segments before re-dispatch (`ResetImportingSegmentRows`, now
  deleted). It papered over the reuse rather than removing it: it could not stop
  a straggler manifest commit, only the stale row count. Nothing is reused now,
  so nothing needs resetting.
- **Straggler writes.** A straggler still writes to the retired chain until it
  exits. That chain is unreferenced -- meta points at the new segments and the
  old ones are Dropped -- so it is reclaimed by segment GC. The commit path also
  declines to *initiate* a commit once its context is canceled, which keeps the
  retired chain from growing for the common case.

Every drop of a segment that was created importing — import rotation, a failed
import's terminal cleanup, a copy-segment replan's abandoned targets, a failed
copy task's targets — clears `is_importing` in the same write. Segment GC skips
`is_importing` segments to protect the in-flight commit marker, so a dropped
segment left with the flag would never be reclaimed. The one exception is the
zero-row sort-skip marker itself: it is already `Dropped` when written and is
deliberately left `is_importing` until `HandleCommitVchannel` clears it.

Import planning assigns an origin per channel, and every normal import is
sort-planned: the decision rides on the durable job options (L0 imports, which
carry deletes only, never sort). No sorted target is preallocated — the sort
stage allocates its own output per origin, and the checker discovers it through
the segment's durable `CompactionFrom`/compactionTo edge written when the sort
commits. The estimate can be non-empty while the actual hash distribution sends
every row to another channel. Sorting records that branch by marking the
zero-row importing origin `Dropped` and deliberately creates no output.
Index-target validation treats only that exact marker (`Dropped`, `NumRows ==
0`, still importing, and within the job's collection/partition/channel) as a
completed skip. Any other origin without an output remains durable plan
corruption and fails the job. A job whose every origin is explicitly skipped is
a valid empty result and may advance to `Uncommitted`.

On DataNode, import, pre-import, and copy-segment use the same worker-entry
retention rule as every other dispatched task: explicit Drop is the normal
acknowledgement, while the shared hourly loop removes an entry once its recorded
start time is at least 24 hours old. If it is still runnable, the sweep
cancels it and removes its ownership entry. Consequently, a coordinator outage
or execution that outlives the retention window can turn a later query into
`TaskNotFound`; DataCoord must then apply that task family's normal retry/replan
policy. This is the deliberate tradeoff for bounding every worker attempt with
one mechanism.

### 3.10 What may be retried, and who decides

The three task paths above answer "may this attempt be tried again?" from the
same evidence — an RPC round that produced no answer — because that evidence is
the only thing DataCoord has. External-collection refresh is different: its
worker *does* answer, and the answer names a failure. So the question becomes
one DataCoord cannot decide alone, and the only component that can is the one
holding the error.

**One classifier, not a second taxonomy.** merr already splits every error into
`InputError` (the request author's fault) and `SystemError` (Milvus's fault, and
the default). That is exactly the retriability question, so it is the only
classifier used:

> **InputError ⇒ permanent.** No worker, and no later attempt, does better with
> the same request. End it now.
> **SystemError ⇒ retriable.** The failure is a condition of this process, this
> node, or the object store. Spend an attempt.

`merr.GetErrorType` answers `SystemError` for anything it cannot classify, which
is the safe direction on both paths: the worst case is a spent attempt, never a
job wrongly failed. Applying the blame test (§ *Input vs System* in
[error_handling_guide.md](../../dev/error_handling_guide.md)) at each site is
the whole of the design; there is deliberately **no** bespoke "non-retriable"
error type alongside merr's, because a second taxonomy is one more thing that
can disagree with the first.

**Three places make the call, in one direction each.**

- **DataNode, per refresh task** (`externalRefreshFailureState`). The task
  function's error decides the reported state: `JobStateFailed` for an
  InputError — a source with zero total rows, a function field the schema does
  not have — and `JobStateRetry` for everything else. A recovered panic stays
  `Failed` deliberately: it is deterministic on the same input, so retrying it
  reproduces it. A worker with no entry for the queried task reports `Retry`,
  never `Failed` — a lost entry says nothing about the request. Before this
  change every failure was reported as `Failed` and DataCoord ended the job on
  the first report; there was no retry machinery to classify for.
- **DataNode, per copy-segment task** (`copySegmentFailureState`, §3.8). The
  same rule, expressed in that path's vocabulary: an InputError keeps
  `ImportTaskStateV2_Failed`, everything else becomes `ImportTaskStateV2_Retry`.
  The distinction has to survive the wire, so `CopySegmentTaskState` gains
  `CopySegmentTaskRetry`; collapsing `Retry` into `Failed` at the response
  boundary — which the state mapping used to do — destroys the classification
  the task had already made and turns one throttled object-storage read into a
  permanently failed restore. The new value is worker-reported only and never
  persisted; DataCoord turns it into a replan (§3.8) on the spot.
- **DataCoord, per refresh job** (`ensureTasksForInitJob`). Planning failures
  are classified the same way. An InputError — the named source has no files,
  its schema or manifest cannot be read as the requested format — transitions
  the job to `Failed` immediately. Anything else leaves it in `Init` for the
  checker tick to re-plan.

**Honoring the worker's call.** A task-level failure that arrives as `Failed`
is terminal on the first report; one that arrives as `Retry` spends one of
`dataCoord.externalCollectionMaxRetryTimes` (new in this change, default 10).
Both go through the same meta mutation, which takes the cap as a parameter —
the permanent path simply passes `1`. The budget exists for the `Retry` class
only; spending it on a permanent input error would leave the job in
`RefreshInProgress` for N attempts when the answer was available on the first.

**Sentinels that cannot classify themselves.** `packed.ErrLoonTransient` marks
*any* failure surfaced by the loon FFI layer. Its own contract says to treat all
of them as retryable and rely on a bounded retry budget, precisely because
milvus-storage can lose the structured detail and fall back to a generic code —
a throttled read and a missing bucket arrive as the same sentinel. It therefore
must not appear in a terminal set: a boundary that calls it permanent inverts
transient and permanent for every error that lost its detail. What bounds it is
`dataCoord.externalCollectionJobTimeout`, not a classification the sentinel
cannot support.

**Keeping the cause when the answer is "wait".** Leaving a job in `Init` used to
discard why: the only thing a caller ever saw for a source that never came back
was the checker's bare `timeout`, with the actual error (a bad bucket, a denied
credential) in DataNode logs. A retriable planning failure now records its cause
on the job while leaving the state alone, and the timeout path reports
`timeout, last failure: …` instead. Retriable is not a reason to be silent.

**Recovery.** Nothing here introduces a new recovery mechanism; it decides which
of the two existing ones applies.

- A **retriable task** failure is replaced under a fresh task ID, the
  fresh-identity rule of §4 applied to this path. The swap is one composite
  catalog update — the replacement is added and the job's `task_ids` entry is
  repointed at it — taken under the job and task locks, and run inside the
  scheduler's `Finalize` so the predecessor is already out of dispatch and no
  worker callback can interleave. Scheduling only ever follows `task_ids`, so
  the predecessor stops being schedulable the instant that update lands; the two
  are never both live. The accumulated failure count moves to the replacement,
  so the cap counts attempts at the *work*, not at whichever identity is
  currently carrying it.
- A **retriable job** failure leaves the job in `Init`, and the checker tick
  re-runs planning. Every attempt explores into its own attempt-scoped manifest
  directory, so a retried plan never reads a partial one left by its
  predecessor.
- Both are bounded: the task path by
  `dataCoord.externalCollectionMaxRetryTimes`, the job path by
  `dataCoord.externalCollectionJobTimeout`. Neither can spin forever, which is
  what makes "when in doubt, retry" safe to adopt as the default.

**A durable replan reserves its inputs.** The window between a replan's
persistence and its admission used to leave its inputs claimable by unrelated
tasks: another compaction could take an input, finish, and get it GC'd, leaving
the replan nothing to admit, and the manual trigger then reported Completed for
work that was not done. Admission now treats every pipelining record with
`RetryTimes > 0` as a reservation: an unrelated task whose inputs overlap one
is refused exactly like a segment-lock conflict, and the replan itself skips
the check. The reservation is the durable record itself, so it covers recovery
too -- after a crash, an ordinary record overlapping a pending replan is
deferred rather than erased: the reconciler admits the replan (which skips the
check) and the ordinary record waits on the reservation until the replan's
work removes its inputs, at which point the reconciler erases it decisively.
Deterministic whatever the map order. No release step exists: the reservation
disappears when the replan is admitted or erased.

**Ambiguous metadata writes fail-stop.** A fresh-identity swap cannot safely
interpret a catalog error as "not committed": an etcd transaction may have
committed before its response was lost, while the DataCoord cache still holds
the predecessor. Continuing in that process could mint a second replacement
and leave the first unreachable from job-driven GC. The two fresh-identity swap
call sites therefore abort on a write error: copy-segment's composite
publication (`PublishReplan`) and external refresh's `ReplaceRetryTask`. Each
emits one Fatal log and terminates the process, guarded so that a caller that
has already timed out or a process that is already shutting down does not crash
on a write failure that merely reflects that cancellation. A non-transport
error is unreachable at both sites -- every action type they send is
implemented and the payloads are marshal-safe protos -- so every realistic
failure is a storage write whose outcome is ambiguous. Every other
catalog write is idempotent and returns its error to the caller, which retries
or degrades. Restart then reloads the authoritative transaction outcome: either
the committed replacement is present, or the predecessor remains eligible for a
clean retry.

---

## 4. Invariants

**The rule these exist to serve, stated once.** *A retry never writes where its
predecessor could still be writing.* An attempt that is abandoned is not
necessarily an attempt that has stopped, so the only safe way to retry is to
give the new attempt an identity — task ID and every output name derived from it
— that the old one cannot possibly produce. Then two attempts cannot collide no
matter what the abandoned worker is still doing, and whatever it writes is
unreferenced and reclaimable. Invariant 4 below is this rule for compaction.

The rule is not compaction-specific and holds for every task type that can be
re-dispatched: compaction satisfies it with a fresh plan ID and a fresh output
segment range, external collection refresh with a fresh task ID, copy-segment
with a fresh task ID and a fresh set of target segment IDs (§3.8), and import
with a fresh set of output segment IDs (§3.9) -- the last of these was the one
violator until this change, and only StorageV3 made the violation observable.

1. `isCompacting` is released exactly once per task, last, inside `doClean`.
2. A task that is terminal and not `cleaned` still holds its input segments.
3. Two live tasks never share an input segment (admission enforces it).
4. A rebuild never reuses a plan ID or an output segment range, so an attempt
   that is still running somewhere cannot collide with the attempt that
   replaced it.
5. At every instant, at least one persisted record claims work that is owed.
6. `RetryTimes` is written only by the replan.
7. A trigger reports `Completed` only when no record under it owes a rebuild.
8. `retrying` never crosses the wire.
9. A persisted record with `state == pipelining` that no runtime structure
   holds is an unowned attempt awaiting the reconciler — a replan written by
   `buildReplacement`, or an original attempt `loadMeta` deferred because its
   admission was blocked. `enqueueCompaction` claims, persists, and queues in
   one call, and a reconciler racing it is turned back by the claim conflict.
10. Compaction input overlap alone never settles or transfers retry debt.
11. A copy-segment replacement is not visible to in-memory scheduling until its
    composite publication has registered every target segment; recovery repairs
    an interrupted owner-first fallback before scheduling it.
12. Copy-segment predecessor cleanup follows `predecessor_task_id`, never a
    partial source-set overlap.
13. A DataNode terminal import result survives until the coordinator explicitly
    drops it.
14. An import retry never writes into the output segments of the attempt it
    replaces; the replacement is adopted in one write and the old segments are
    retired, so an abandoned attempt's output is never left referenced.
15. Retriability is decided by merr's Input-vs-System classification and by
    nothing else; an unclassified error is retriable. No path carries a second,
    private notion of "non-retriable" (§3.10).
16. Every retriable path is bounded — by an attempt cap or by a job timeout —
    so defaulting to retry can never spin forever.
17. A worker's permanent verdict is honored on the first report: a failure the
    worker classified as permanent never spends a retry attempt.
18. A dropped segment is dropped reclaimable: every drop path that can touch an
    importing segment clears `is_importing` in the same write — except the
    zero-row sort-skip marker, which stays `is_importing` until vchannel
    commit, protected by segment GC's `is_importing` exemption.
19. A copy-segment job reaches a terminal state only via a compare-and-set on
    its current state: no stale snapshot can resurrect a `Failed` job as
    `Completed`, and no dispatch happens after a task was already settled.

---

## 5. Configuration

| Key | Before | After | Notes |
|---|---|---|---|
| `dataCoord.compaction.maxAttempts` | — | `10` | new, refreshable |
| `dataCoord.requestTimeoutSeconds` | `600` | `30` | not exported in `milvus.yaml`, so no existing configuration is affected |
| `dataCoord.externalCollectionMaxRetryTimes` | — | `10` | new: bounds *retriable* refresh task failures; a permanent one settles on the first report. At the merge-base there was no task retry at all -- every worker failure ended the job on the first report (§3.10) |
| `dataCoord.copySegmentMaxAttempts` | — | `10` | new: bounds how many times a copy-segment task is rebuilt under a fresh identity before its job fails (§3.8) |
| `dataCoord.externalCollectionJobTimeout` | — | unchanged | the only bound on a refresh job whose planning keeps failing transiently, including every `ErrLoonTransient` (§3.10) |

`dataCoord.enableCompaction` now gates only the **producers** of new work: the
triggers are not started and `ManualCompaction` is rejected while it is off. The
inspector itself always runs, because whatever reaches its queue is either
inherited debt (recovered tasks and their replans) or work correctness requires
regardless of the switch — an import's sort compaction, without which imported
segments never leave `IsInvisible`. Gating execution froze those tasks in the
queue holding their segment locks until restart.

Sort triggering no longer depends on `dataCoord.compaction.enableAutoCompaction`
for the same reason. The merge half of `singleCompactionPolicy` still honors it.

Sort compaction is always on: the `dataCoord.sortCompaction.enable` switch is
gone. Every producer creates sort debt unconditionally — flush publishes
flushed segments as `Flushed+IsInvisible` (`flushFlushingSegment`, the
segment-creation RPC), and every normal import is sort-planned by its durable
job options, with origins published invisible until their sorted output exists
— and the sweep sorts every unsorted segment, visible or invisible, whenever
the policy runs. The only remaining gate is the global `dataCoord.enableCompaction`
switch, which stops the producers (and the trigger manager) while the inspector
keeps draining segments already owed a sort.

---

## 6. Compatibility

- **Upgrade.** Nothing writes `timeout` any more — every worker-reported
  timeout goes through `setAttemptEnded` — so a persisted `timeout` can only
  predate this change. It is treated as settled: cleanup still runs and still
  releases its inputs, and the periodic trigger picks the work up again. That is
  what the old code did with the same record, so the upgrade neither regresses
  it nor retroactively improves it.
- **Mixed version.** Unaffected. `CompactionTaskState` crosses the wire only in
  `CompactionPlanResult.state`, which DataNode fills and never sets to
  `retrying`.
- **Downgrade is not handled.** An older DataCoord's `Process` has no branch for
  `retrying`, so a record left in that state across a downgrade would sit there
  holding its inputs. The exposure is the seconds between an attempt ending and
  cleanup writing `cleaned`. The copy-segment analog: a `predecessor_task_id`
  persisted mid-replan is meaningless to an older coordinator, which schedules
  the replacement as an ordinary Pending task with no handoff — the pair may
  both run, colliding only on already-Dropped predecessor targets. Consistent
  with the no-downgrade-guarantee stance; stated for symmetry.
- **Mixed version, copy-segment Retry.** A new DataNode's `Retry` report crosses
  the wire as a `taskcommon` property string, and an old coordinator's
  `ToCopySegmentState` maps `Retry` to `Failed` — it fails the job on the first
  transient fault, exactly the pre-change behavior. Degraded but familiar, and
  bounded; no upgrade ordering is required.
- **Upgrade, import sort plan.** Every normal import is now sort-planned by its
  durable job options, while an older binary planned sort conditionally on
  `dataCoord.enableCompaction`. A legacy job that finished writing without a
  sort plan (origins healthy, visible, `is_importing`, no sorted output) is
  recognized by that visible shape at `IndexBuilding` time and its origins are
  treated as the final imported segments instead of failing the job and
  dropping the data. A sort-planned origin is always invisible under this
  change, so the two shapes cannot be confused.
- **Upgrade, legacy sort switch.** Clusters that disabled sort through the
  legacy `dataCoord.statsTask.enable=false` key (the removed
  `sortCompaction.enable`'s fallback) silently re-enable sort on upgrade:
  newly flushed non-L0 segments are published `Flushed+IsInvisible` and must
  await their sorted replacement before becoming queryable. The change is
  deliberate (sort is always on), but operators see extra compaction load and
  query-visibility latency with no config change and no warning emitted.
- **EnableCompaction on→off across a restart.** A segment flushed while the
  switch is on is published invisible and handed to the in-memory stats
  channel before its sort task is persisted; a crash-and-restart with the
  switch off leaves no path to recreate that task (the trigger scan is gated
  off, `loadMeta` recovers only persisted tasks), so the segment stays
  invisible until compaction is re-enabled. A narrow window (crash + config
  flip + unpersisted task), documented rather than fixed with a startup scan.
- **Recovery defers every blocked admission.** A pipelining record whose
  admission fails at `loadMeta` -- snapshot protection, a reservation, or a
  decisive "inputs gone" -- is left for the reconciler, which retries the
  transient cases every round and erases the decisive ones; an executing
  record ends its attempt and is rebuilt under a fresh plan ID. Recovery
  therefore never erases durable work over transient conditions, and a
  `ManualCompaction` trigger keeps reporting InProgress while its record
  waits, instead of a false `Completed`.

---

## 7. Alternatives considered

- **Re-dispatch under the same plan ID.** Rejected: collides with a worker that
  may still be running the plan, on artifacts named after the plan ID and the
  output segment range (§2.2).
- **Keep clustering's in-place retry loop.** Rejected: it was a second retry
  mechanism with different rules, and it reset `RetryTimes` on every forward
  state transition, so no cap could read the field.
- **Derive "owes a rebuild" instead of persisting it.** Rejected: `maxAttempts`
  is refreshable, so the derived answer can change under a task that cleanup has
  already committed to rebuilding.
- **Hand the input claim over to the replacement instead of releasing and
  re-claiming it.** Rejected: it requires the replacement to be live before the
  old attempt is cleaned, and an in-flight callback of the old attempt could
  then mutate the inputs while the new attempt is compacting them.
- **Applying the attempt cap to legacy `timeout` records at read time**, so
  that a `timeout` with retries left is also rebuilt. Rejected: it puts a
  refreshable parameter back in the predicate that cleanup and the trigger
  summary both read, which is the instability `retrying` exists to remove, and
  the two could then disagree across a configuration refresh. A legacy record is
  settled instead — the same reading the code that wrote it had.
- **A time budget, and then a consecutive-unanswered-rounds counter, for giving
  up on a worker.** Both rejected for the same reason, the counter last: each is
  a second give-up mechanism layered on rounds that are already time-bounded
  (30s each) and attempt-capped, buying only tolerance for a worker that is
  reachable enough to hold a session yet silent for a full RPC round — a state
  with no legitimate cause. One rule remains: an unanswered round ends the
  attempt, on the query path exactly as on the create path.
- **An in-memory ownership state machine for replacements** — a runtime owner
  object per durable replan record, with awaiting-cleanup / ready / dropping
  states, an in-flight latch, and a predecessor index, handed from the cleanup
  goroutine to a retry loop. Rejected after being implemented: every state
  duplicated a fact already derivable from persisted meta (awaiting-cleanup ≡
  inputs overlap a record owing cleanup; ready ≡ they do not; dropping ≡ re-run
  the admission and get the same answer), the handoff invariants — exactly one
  owner per record, owner-identity checks on every transition — had to hold
  across three goroutines, and restart recovery and steady-state retry were two
  code paths maintaining the same guarantee. The reconciler re-derives all
  three states per round, is idempotent by construction, and is the recovery
  path.
- **Relying on `loadMeta` to resubmit a replan that lost the last queue slot.**
  Rejected: it strands the record — and the trigger a `ManualCompaction` caller
  is polling — until a restart happens to occur. Moot now that the queue cannot
  refuse a persisted task at all, but the reasoning is why it cannot.
- **An exact queue limit, enforced at `Enqueue`.** Rejected: see §3.4. Exactness
  requires refusing a durable task, and every mechanism that makes the refusal
  safe (capacity reservations) has to be threaded through every producer and
  every scheduler put-back. An advisory limit overshot by single digits gives up
  nothing the limit was for.
- **Erasing a replan on input overlap or admission conflict.** Rejected: an
  overlap can be a different trigger, compaction type, complete input set, or
  semantic operation; it proves contention, not completion. The claim can also
  be invisible in meta (the predecessor's cleaned-write→flag-release window, or
  an `enqueueCompaction` between its claim and persist) and later disappear.
  Either case waits rather than discarding durable work.
- **Queueing the replacement directly from the cleanup goroutine.** Rejected:
  a second driver racing the reconciler for the same records buys one schedule
  round of latency on a path that is already a failure retry, at the price of
  every cross-goroutine dedup concern the ownership machine existed to manage.

- **Mutating copy-segment's task record in place instead of replacing it** —
  keeping the task ID and rewriting only its target segment IDs. Rejected: it
  leaves the DataNode's owner fence, which keys on task ID, unable to tell the
  attempts apart, so a re-dispatch landing on the node that still holds the old
  record is silently cancelled; and it makes an ordinary late response from the
  abandoned attempt indistinguishable from a corrupt one, since the result
  validation also fences by target segment ID. Replacing the record removes both
  problems rather than compensating for them (§3.8).
- **Letting the copy-segment worker delete the objects an abandoned attempt
  wrote.** Rejected: before §3.8 those keys were shared with the attempt that
  replaced it, so the cleanup could delete a live restore; and DataCoord's GC
  already reclaims them once the abandoned targets are dropped. The worker keeps
  the inventory for diagnostics and deletes nothing.

---

## 8. Test plan

- Cleanup decision from snapshots, for every compaction type; `Finalize` /
  `Update` ownership under the scheduler lock; terminal-state entry guards;
  single release of `isCompacting`.
- Lock discipline: the terminal-state worker drop is sent only after the
  per-task lock is released, on both dispatch paths (create-terminated and
  query-terminated). The test asserts the lock is already free from inside
  `DropTaskOnWorker`, so putting the RPC back under the lock fails it.
- Recovery: deferring cleanup off the readiness path; releasing inputs it cannot
  queue while retaining a durable replan; failing startup on an unwritable
  metastore; deferring a replacement whose predecessor is not cleaned yet and
  reusing only its exact semantic successor afterwards.
- Queue admission: two producers that both saw room both succeed and the queue
  overshoots its limit, rather than one being refused after persisting;
  pre-persistence failures release the input claim; a task the scheduler popped
  for an exclusion decision always gets back in; recovery queues every durable
  task even when the queue is already over its limit.
- Replan: fresh-ID invariants (same input set, disjoint output range, preserved
  trigger ID) on every failure class; the record is durable before the clean; a
  clean that fails reuses the same durable replacement on the next round;
  partial overlap with different work and a transient snapshot block both keep
  the record.
- Input reservation (§3.10): while a replan awaits admission, an unrelated task
  whose inputs overlap it is refused admission exactly like a segment-lock
  conflict and the replan itself is exempt; recovery defers an ordinary record
  overlapping a pending replan -- the reconciler admits the replan and the
  ordinary record waits on the reservation, deterministically, whatever the
  map order.
- The cap: a settled failure is not rebuilt; a trigger keeps reporting executing
  while a rebuild is owed.
- Single-unanswered-round give-up; duplicate-create keeping the assignment.
- Drop reclaimability: every drop of an importing segment (import rotation,
  terminal import failure, copy replan targets, failed copy targets) clears
  `is_importing` in the same write, while the zero-row sort-skip marker keeps
  it until vchannel commit; segment GC's `is_importing` exemption protects the
  marker but never a retired segment.
- Copy-segment terminal transitions are CAS-guarded: a stale checker snapshot
  cannot flip a job that a concurrent replan/failure moved on, and a task
  already settled as `Failed` is never dispatched again.
- DataNode index/analyze/stats registration: a duplicate or a post-registration
  failure (plugin context, scheduling) releases both the task context and the
  registered entry, so no phantom InProgress task and no ctx hooked into
  node.ctx until shutdown.
- DataNode task expiration: one hourly owner and one 24-hour start-time cutoff
  across import/pre-import/copy-segment, index/analyze/stats, compaction, and
  external refresh; each manager removes old terminal entries, cancels old
  runnable entries, and retains fresh entries. Compaction cancellation keeps
  its entry until the completion callback applies deferred removal.
- GC: both halves of the partition-stats reference check, the grace period, and
  the path parser's rejections; a StorageV3 segment with no manifest still
  retires, so neither its meta nor its objects outlive it.
- Copy-segment replan: assignment is persisted before Create and an unknown
  Create result gets fresh task/target IDs; a replan that already has a durable
  successor adopts it instead of minting another, so recovery never elects among
  several; a composite task-plus-target publication is atomic when possible and
  owner-first when chunked, and resumes after restart before scheduling; cleanup
  follows the exact predecessor edge and ignores unrelated source overlap;
  abandoned targets end up Dropped and the job stays Executing. The attempt cap
  settles the job instead of rebuilding again.
- Import segment rotation: a retry gets output segments disjoint from the
  abandoned attempt's and retires the old ones; a restart mid-swap leaves the
  live output untouched; a task with no output yet resets to Pending without
  allocating anything.
- Import terminal edges: an explicitly dropped zero-row origin counts as a
  completed skip, and any other origin without a sorted output fails. Terminal
  DataNode task results remain queryable before the common expiration cutoff
  and are removed by either coordinator Drop or the first hourly sweep after
  that cutoff.
- Retriability classification (§3.10), asserted in both directions at every
  site that decides it, because a one-directional test passes just as well when
  everything is classified the same way:
  - DataNode refresh and copy-segment: an InputError (and an InputError wrapped
    by the copy path) reports permanent; a system error and an unclassified
    error both report retriable.
  - DataCoord refresh planning: an InputError fails the job at once; a system
    error leaves it in `Init` **and** records the cause, so the timeout does not
    degrade to a bare `timeout`. An `ErrLoonTransient` explore failure takes the
    retriable branch — the test that used to assert the opposite now pins the
    contract it inverted.
  - Refresh task query: a worker-reported `Failed` is terminal on the first
    report and does not spend an attempt; a worker-reported `Retry` spends one
    and stays retriable.
  - Copy-segment query: `CopySegmentTaskRetry` replans under a fresh identity
    and leaves the job Executing; `CopySegmentTaskFailed` still fails it.
  - `errors.Is` still reaches `errMilvusTableRefreshSchemaInvalid` through the
    classification marker, so marking never breaks a sentinel chain.
- Metrics: every bucket of `datacoord_compaction_task_num` returns to where it
  started once a task's lifecycle completes, including a task that never reaches
  a worker.
