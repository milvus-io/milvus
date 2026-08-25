# External-Collection Refresh: Index Wait

- Status: implemented, off by default (`dataCoord.externalCollection.refreshWaitForIndex`)
- Related issue: [#52714](https://github.com/milvus-io/milvus/issues/52714)
- Components: DataCoord (`internal/datacoord/external_collection_refresh_*`)

## 1. Problem

A **refresh** re-scans an external collection's source and re-points the
collection at what it finds. Its terminal transition is one atomic catalog
write:

```
tasks all Finished
  └─[one write] apply segments into collection meta + State=Finished + Progress=100
      → publish the refreshed external_source/external_spec (AlterCollection)
```

Index building is asynchronous — the index inspector notices the new segments
and schedules builds afterwards. So a client that polls `DescribeRefresh` and
queries the moment the job reports Finished meets segments with no index yet.
The results are correct (segcore brute-force scans an unindexed segment) but the
latency is a full scan of the refreshed data.

Deployments that keep a warm replica loaded never notice: the replica has been
serving and picks the new segments up through target updates, and by the time
real traffic arrives the indexes are usually there. Deployments that **load the
collection on demand per query** have nothing to hide the window behind — the
refresh-completion signal *is* their readiness signal.

## 2. Goal

Make "refresh finished" mean "finished **and** queryable at indexed speed", for
deployments that ask for it, without changing the default behavior.

Non-goals: changing how indexes are built or scheduled; hiding unindexed
segments from queries (they are not hidden — see §3); making this the default.

## 3. Design

One config flag and one extra phase in the job's life:

```
tasks all Finished
  ├─ off → [one write] apply + Finished          ← native, untouched
  └─ on  → [one write] apply + IndexWaitStartedTime   (job stays InProgress)
             → publish external_source/spec            (as always, right after)
             → each tick: unindexed? nudge build channel, report 90-99, wait
             → all indexed → Finished
             → outran the job timeout → Failed         (as any refresh does)
```

**What waits is the job's completion signal, not the data.** The segments are
applied and the refreshed source/spec published at exactly the same point the
native path does it — a loaded replica sees the new data either way. The only
thing deferred is the moment the job reports Finished.

### 3.1 The apply stays welded to a state write

`BeginIndexWait` is `UpdateJobStateWithPreApply` with a different target: it
applies the results and stamps `index_wait_started_time` in **one** catalog
write, under the collection job lock, with the same terminal re-read and the
same "pre-apply failed → persist Failed" behavior.

This is the load-bearing decision. Prying the apply loose from a state write —
committing segments first and deciding the job's fate later — creates a state
the rest of DataCoord has never had to reason about: segments committed and
served while the job is neither finished nor failed and the schema is
unpublished. Everything downstream (timeout, restart recovery, index-build
eligibility, schema publication, GC) would need to learn about it. Keeping them
welded means `index_wait_started_time` is simultaneously the sub-state marker
and the apply-once guard: a job carrying it has already committed, so the
Finished transition at the end of the wait runs with **no** pre-apply.

Two consequences of that marker are load-bearing in their own right, because
this is the one transition through `updateJobStateWithPreApply` that does *not*
write a terminal state — so the terminal re-read, which serializes every other
caller for free, cannot serialize this one:

- **The guard is evaluated under the job lock**, as a `skip` predicate, not in
  the caller. The eager task path and the periodic tick run on different
  goroutines, and two tasks of one job can finish at once; both can read the
  job before either writes the marker. Only a read ordered against the write —
  i.e. under the same lock — makes the apply once.
- **The wait phase is entered on the marker, not on the parameter.** A job that
  has applied belongs to the wait path for the rest of its life, because that
  path is the only exit that does not re-run the apply. Turning the parameter
  off mid-wait therefore *releases* such a job — it finishes at once, without
  waiting — rather than sending it down the generic transition, whose pre-apply
  would replay the apply.

A replayed apply is not a harmless no-op: `applyExternalRefreshPatch` clears
`TextStatsLogs` and `JsonKeyStats` unconditionally, so a second write discards a
text index or JSON key stats built since the first one and orphans their files.
Newly created segments have always short-circuited on the manifest they would
install; patched baseline segments now do the same
(`externalRefreshManifestAlreadyApplied`), which makes the apply idempotent at
its source rather than only at the callers this feature happens to add — and is
the only defense against a replay by an older binary after a downgrade.

### 3.2 The debt is the whole collection

Each tick asks `GetUnindexedSegments(collectionID, <every flushed segment of the
collection>)`. Not a snapshot of what this refresh produced — a refresh
*defines* the collection's contents (it keeps, patches, adds and drops until
what remains is exactly the external source), so once its apply has landed
"this refresh's segments" and "the collection's segments" are the same set.

That is why this needs no per-job segment list, no result-store re-reads, and no
in-memory bookkeeping, where import (which adds only part of a collection) has
to carry `SegmentIDs` on each task.

Unindexed segments are pushed into the index-build acceleration channel each
tick, exactly as `importChecker.checkIndexBuildingJob` does.

### 3.3 Progress

The job reports `90 + 10 * indexed/total`, capped at 99 — 100 is reserved for
done, which pollers key on. `DescribeRefresh` / `ListRefreshJobs` re-derive
progress from tasks, which are all Finished during the wait (a flat 100 that
says nothing about it), so `normalizeRefreshJobProgress` prefers the job's
persisted value while `index_wait_started_time` is set. Progress writes carry a
terminal-state guard so a racing tick cannot pin a Finished job below 100.

### 3.4 Timeout

None of its own. The wait happens while the job is `InProgress`, so the ordinary
`tryTimeoutJob` applies: `dataCoord.externalCollectionJobTimeout` measured from
the job's start, exceeded → `Failed`.

Deliberately the *job's* clock, not a fresh one for the wait. Giving the wait
its own budget would mean a job can live for up to twice the parameter, and a
parameter that no longer bounds what it says it bounds is worse than a wait that
sometimes runs out. The consequence is real and worth stating: a long ingest
leaves the wait less room, and an operator who wants the wait to survive a large
refresh raises `externalCollectionJobTimeout`.

A second consequence follows from the debt oracle. `GetUnindexedSegments`
counts a segment as indexed only when every one of its indexes is `Finished`, so
a build that ended `Failed` reads as unindexed forever — nothing in DataCoord
resets that state, and `createIndexesForSegment` will not re-create an index
that already has a `SegIndex` record. A refresh that hits one therefore waits
out the whole budget and then fails, rather than giving up early. That is the
accepted trade: detecting "this can never finish" means a second debt oracle
that splits pending from terminally-failed, and the failure mode it buys is an
hour of waiting before an outcome that is already bounded and already Failed.

A failed refresh **does not roll back**. Its segments were applied when the wait
began and are the collection's contents; the job reporting Failed only says the
index wait did not complete in budget. Re-run the refresh to try again. This is
the same shape as the native path when the apply succeeds and the state write
fails, and it is stated in the parameter documentation.

### 3.5 What the wait costs while it holds

The wait can last as long as an index build, so everything it does per tick is
sized for that rather than for a single pass.

- **Task results are released at wait entry**, not at the end. They are dead
  weight the moment the apply lands — the marker guarantees no replay, and the
  state aggregate reads task *states*, never results — and they are not small:
  a `SegmentInfo` per produced segment, inline in the task's catalog record plus
  a blob in the result store. Holding them for an index build, and for the
  retention period on top when a job times out mid-wait, is a cost the ungated
  path never pays, because there the apply and Finished are the same write and
  the clear follows immediately.
- **Progress is persisted only when it changes.** `UpdateJobProgress` skips a
  write that changes nothing, under the job lock, so a held job writes at most
  ten times across the whole wait instead of once per tick.
- **The debt is logged only when the band moves**, with a bounded sample of ids
  rather than the whole list — the debt can be the entire collection, and one
  line per tick per waiting job says nothing new in between.
- **The per-tick query is `O(segments of this collection)`**: `WithCollection`
  narrows through the `coll2Segments` secondary index, and `GetUnindexedSegments`
  is map-based throughout. `getSegmentsIndexStates` no longer allocates a map
  for a segment that has no index records at all, which matters once the caller
  is scanning a whole collection on a timer.
- **A waiting job is counted separately in the checker's stats log.** It is
  `InProgress` like any other, so the state histogram alone cannot tell "still
  ingesting" from "waiting for indexes" — the distinction an operator looking at
  a long-lived `InProgress` count actually needs.

## 4. What this deliberately does not do

**It does not close the apply→publish window.** Between the apply and the
`AlterCollection` reaching DataCoord's collection cache, an index build
dispatched for a refreshed segment takes the *pre-refresh* source/spec from the
collection schema. For a refresh that moves bucket or rotates credentials that
build fails terminally and is never retried.

That window exists on the **native path too** — apply is atomic with Finished,
publication follows it — and is not made materially wider here, because the wait
begins *after* the publication is issued. It is a real pre-existing defect and
deserves its own change (the guard belongs at the three points a build can
start: `createIndexesForSegment`, `indexBuildTask.CreateTaskOnWorker`, and
`statsTask.CreateTaskOnWorker`), not a rider on this one.

**It does not change admission.** Only one refresh per collection may be active,
and a waiting job stays active — so a second `RefreshExternalCollection` for
that collection is rejected as a duplicate for the length of the wait, where the
native path rejects it for one checker tick. Documented in the parameter doc.

## 5. Compatibility

- Default `false`. Off, the phase is not reached and the transition is the
  native one, asserted by a test.
- One added proto field: `ExternalCollectionRefreshJob.index_wait_started_time`
  (tag 13). Additive and written only when the flag is on, so a downgrade reads
  jobs that simply do not carry it.
- No RPC or field change. `DescribeRefresh` / `ListRefreshJobs` report the same
  fields; only the progress a *waiting* job reports is new.
- A waiting job is still an *active* job, so `SubmitRefreshJob` refuses a new
  refresh of that collection until the wait ends — the same rule that already
  applies during an ingest, over a longer window. A caller refreshing on a fixed
  schedule shorter than its index builds will start seeing
  `refresh job already in progress`.
- Downgrade: an older binary does not know the marker, so it would see an
  applied job as merely in progress and apply again. The manifest short-circuit
  in §3.1 is what makes that replay a no-op rather than a loss of text-index and
  JSON-key-stats metadata.

## 6. Key packages

- `internal/datacoord/external_collection_refresh_checker.go` — the wait:
  `beginIndexWait`, `indexWaitDone`, `finishAfterIndexWait`.
- `internal/datacoord/external_collection_refresh_meta.go` — `BeginIndexWait`,
  the one-write apply-and-mark.
- `internal/datacoord/import_checker.go` — `checkIndexBuildingJob`, the shape
  this follows.
- `internal/datacoord/index_meta.go` — `GetUnindexedSegments`, the debt oracle.
