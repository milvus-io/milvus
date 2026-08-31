# DataCoord compaction: generic bounded retry and failure surfacing

- Status: Implemented (this PR) — scoped to retry generalization, metrics, and failure
  history. Segment-level cross-task cooldown (issue proposal #2) is explicitly deferred;
  see [6. Out of scope](#6-out-of-scope--follow-ups).
- Date: 2026-08-21
- Scope: `internal/datacoord/compaction_task_retry.go` (new),
  `internal/datacoord/compaction_task_mix.go`,
  `internal/datacoord/compaction_task_bump_schema_version.go`,
  `internal/datacoord/compaction_task_l0.go`,
  `internal/datacoord/compaction_task_meta.go`,
  `internal/datacoord/compaction_inspector.go`,
  `internal/datacoord/server.go`,
  `pkg/metrics/metrics.go`,
  `pkg/util/metricsinfo/metric_request.go`,
  `pkg/util/paramtable/component_param.go`
- Related: #52708

## 1. Problem

DataCoord compaction has four task types (`mixCompactionTask`, `bumpSchemaVersionTask`,
`l0CompactionTask`, `clusteringCompactionTask`). Only `clusteringCompactionTask` implements
bounded retry (`merr.IsRetryableErr(err) && RetryTimes < 3`); the other three transition
straight to `failed` on the first error, with no distinction between a transient failure
(worth retrying) and a deterministic one.

Separately, failure visibility is poor:
- `fail_reason` is persisted on the task, but `compactionInspector.cleanCompactionTaskMeta`
  deletes the whole task record (including the reason) once it has been `cleaned` for
  longer than `CompactionDropToleranceInSeconds` — so the reason does not survive
  diagnosis after the fact.
- `metrics.DataCoordCompactionTaskNum` only has `pending`/`executing`/`done` labels;
  `checkCompaction()` incremented `done` for every finished task regardless of state, so a
  compaction that fails every time looks identical to a healthy one on a dashboard.
- `GetCompactionState`'s `FailedPlanNo` is scoped by `triggerID`, which only a manual
  compaction caller ever queries back — background triggers (bump/sort/clustering
  auto-triggers) generate their own internal trigger IDs nobody polls, so this RPC is
  structurally blind to background failures.

Full analysis: #52708.

## 2. Design

### 2.1 Shared retry classification

`classifyFailure()` (new file, `compaction_task_retry.go`) generalizes clustering's
existing pattern into one function every task type calls:

```go
func classifyFailure(ctx context.Context, label string, taskProto *datapb.CompactionTask,
    err error, updateAndSaveTaskMeta func(opts ...compactionTaskOpt) error) bool
```

A retryable error (`merr.IsRetryableErr`) under the configured budget
(`dataCoord.compaction.maxRetryTimes`, default `3`) increments `retry_times` and leaves
state untouched, so the task is reattempted on its own next scheduling cycle. Anything
else — non-retryable, or retryable but out of budget — transitions the task to `failed`
with `fail_reason` set.

Wired into every call site that was previously an *unconditional* `setState(failed)` in
`compaction_task_mix.go`, `compaction_task_bump_schema_version.go`, and
`compaction_task_l0.go`.

**Deliberately not touched**, to avoid changing behavior for cases this PR did not
analyze deeply enough to be confident about:
- Node-connectivity retry loops (`setState(pipelining), setNodeID(NullNodeID)` on
  `CreateCompaction`/`QueryCompaction` RPC failure) — already an existing, working,
  unbounded retry; out of scope here.
- The opaque "compaction failed in datanode" branches — `CompactionPlanResult` carries no
  error detail from the DataNode side today (only a state enum), so there is nothing to
  classify without fabricating an error.
- One `saveSegmentMeta` failure path per task type (mix/bump/L0) that today only
  terminates on `merr.ErrIllegalCompactionPlan` and otherwise implicitly retries forever
  via a bare `return` (next poll re-tries `QueryCompaction` → `saveSegmentMeta`).
  `merr.IsRetryableErr` defaults to `false` for any error that isn't an explicit merr
  sentinel (e.g. a raw KV/etcd error), so routing this specific path through
  `classifyFailure` unmodified would flip "retry forever on a transient storage error"
  into "fail permanently on the first transient storage error" — a regression, not an
  improvement. Left as-is; revisiting it needs an audit of what errors
  `CompleteCompactionMutation`'s catalog writes actually raise.

`clusteringCompactionTask` itself is untouched: it already has working retry coverage,
and rewriting it risked scope creep. One latent gap was found during review — in
`QueryTaskOnWorker`, an explicit `setState(failed)` call at the segment-validation
failure site leaves `err` still holding the original error for the deferred
`t.retryOnError(err)` to see, so a "retryable" classification there only bumps
`retry_times` on a task whose state was already forced to `failed` two lines above,
i.e. it doesn't actually retry. Documented as a known follow-up, not fixed here.

### 2.2 Metrics

Added `Failed`/`Timeout` labels to `pkg/metrics/metrics.go`. `checkCompaction()`
(`compaction_inspector.go`) now switches on the task's terminal state instead of always
incrementing `done`. This changes the *meaning* of the existing `done` label (it no
longer double-counts failures) without adding a new label name — treated as compatible
per this repo's metric-compatibility convention (label-set stable, `done` becomes
strictly more accurate). Failure/timeout finishes also log at `Warn` instead of `Info`.

### 2.3 Terminal failure history

`compactionTaskMeta` gets a second bounded LRU, `failureHistory` (256 entries, 24h TTL —
deliberately longer than the existing `taskStats` LRU's 15 minutes, since this exists
specifically to outlive `cleanCompactionTaskMeta`'s deletion of the task's own record),
with `RecordTerminalFailure()` / `FailureHistoryJSON()`. Recorded in `checkCompaction()`
at the same point the metric label is chosen — the one place every task type's terminal
transition (classified retry-exhaustion, opaque DataNode failure, or timeout) is
observed uniformly, regardless of which internal path produced it.

Exposed through the existing `GetMetrics` debug-request mechanism via a new
`metricsinfo.CompactionTaskFailureKey` (`"compaction_task_failures"`), registered in
`server.go` next to the existing `compaction_tasks` key — no new RPC.

### 2.4 Fixes from code review

Two issues surfaced in review, both addressed:

- **`RecordTerminalFailure` recorded a blank `EndTime` (Medium).** `updateAndSaveTaskMeta`
  only stamps `EndTime` when the task's state was *already* terminal before that save —
  but the save that transitions a task *into* `failed`/`timeout` (via `classifyFailure`,
  or the timeout branches in `QueryTaskOnWorker`) runs while the prior state was still
  `executing`/`pipelining`. The stamp only lands on the *next* save after that (`Clean()`'s
  `failed/timeout -> cleaned` transition), which runs later and asynchronously — after
  `RecordTerminalFailure`'s snapshot is already taken. So every history entry's `EndTime`
  was silently empty. Fixed by backfilling `EndTime` with the current time in
  `RecordTerminalFailure` whenever it's still zero (on a clone, not mutating the caller's
  task). The original regression test couldn't catch this because it hand-set `EndTime` on
  the test fixture; `TestRecordTerminalFailureBackfillsEndTime` was added to drive the real
  zero-`EndTime` path, plus `TestRecordTerminalFailurePreservesExistingEndTime` to confirm
  the backfill doesn't clobber a task that legitimately already has one.
- **`DataCoordCompactionTaskNum` mixes gauge and counter semantics (Low).**
  `pending`/`executing` are true gauges (paired Inc/Dec); `done`/`failed`/`timeout` only
  ever increase, which is counter behavior sharing a `GaugeVec` — meaning the count
  resets to 0 on every DataCoord restart with no record that a restart happened, and
  `rate()`/`increase()` (the standard way to watch for "failures spiking") isn't designed
  for that. Two fixes were on the table: split the terminal-outcome labels into a proper
  `CounterVec` (correct, but a new public metric name existing dashboards would need to
  adopt), or clarify the Help text so the mixed semantics are at least visible to a reader
  (fully backward compatible, doesn't fix the `rate()` problem). Went with the minimal
  fix for this PR — `DataCoordCompactionTaskNum`'s Help text now explicitly states which
  labels are live counts and which are cumulative-since-restart. The `CounterVec` split
  remains a valid follow-up if the restart-reset behavior needs to be queryable correctly.

## 3. Config

`dataCoord.compaction.maxRetryTimes` (`DataCoordCfg.CompactionMaxRetryTimes`,
refreshable, default `3`) — replaces clustering's previously-hardcoded `maxRetryTimes: 3`
literal as the shared budget for all task types using `classifyFailure`.
(`clusteringCompactionTask`'s own hardcoded constant is untouched per §2.1.)

## 4. Testing

- `compaction_task_retry_test.go` (new): `classifyFailure` in isolation — retry-under-budget,
  retry-at-budget terminates, non-retryable terminates immediately, budget is configurable,
  a raw (non-merr) error is treated as non-retryable.
- `compaction_task_mix_test.go`, `compaction_task_bump_schema_version_test.go`,
  `compaction_task_l0_test.go`: end-to-end wiring tests through each type's real
  `QueryTaskOnWorker`/`CreateTaskOnWorker`/`updateAndSaveTaskMeta`, proving retry_times
  persists through each type's actual save path (bump/L0 additionally exercised against a
  real catalog-backed meta, not just a mock).
- `compaction_task_meta_test.go`: failure history — empty state, recording, and the core
  regression case (`TestFailureHistorySurvivesTaskMetaDrop`) proving a failure reason
  remains queryable after the normal task meta is dropped. Plus (§2.4)
  `TestRecordTerminalFailureBackfillsEndTime` and
  `TestRecordTerminalFailurePreservesExistingEndTime` for the `EndTime` backfill.
- `compaction_inspector_test.go`: `checkCompaction()` no longer folds failed/timeout into
  `done`, and failure history is populated before cleanup. `SetupTest()` also stubs
  `MockCompactionMeta.GetCompactionTaskMeta()` (`.Maybe()`, returning a real
  `compactionTaskMeta`) since `checkCompaction()` now calls it for every finished task —
  see §5, this was the one gap hand-review missed and CI caught.

## 5. Verification note

The dev environment used to write this PR could not run `go test` for
`internal/datacoord` — the C++ core (`milvus_core.pc`) was never built there, which
blocks *any* Go test in that package tree independent of this change. The pure-Go
pieces (`pkg/util/paramtable`, `pkg/metrics`, `pkg/util/metricsinfo`) were confirmed to
`go build` cleanly there; the `internal/datacoord` changes were instead verified by
hand — every new getter/wrapper/mock call site checked against its actual generated
signature.

That hand-review had exactly one gap, and CI (which does have the core built) caught
it: `checkCompaction()`'s new `c.meta.GetCompactionTaskMeta().RecordTerminalFailure(...)`
call is a dependency none of the five pre-existing `TestCompactionPlanHandlerSuite`
tests that reach a terminal task state had stubbed on `MockCompactionMeta` — hand-review
only added the stub to the *new* test, not to the shared `SetupTest()` every other test
in the suite also runs through. Fixed once, in `SetupTest()` (§4). All five failures
traced back to this single root cause; everything else in the diff passed CI as
hand-verified. Lesson for next time: a new call added to a widely-shared code path needs
its mock stub added at the suite's shared setup, not just the test that motivated it.

## 6. Out of scope / follow-ups

**Cross-task cooldown (issue proposal #2).** `retry_times` is scoped to one
`CompactionTask` instance; every trigger cycle allocates a fresh `PlanID` and a new task
struct (see `compaction_trigger_v2.go`), so `retry_times` cannot express "this segment
has failed compaction repeatedly across many trigger cycles." Stopping a trigger policy
from re-selecting a segment that keeps failing needs new state that outlives a task
instance, keyed by segment. Two shapes were considered and neither was implemented here:
a field on `SegmentInfo`/its proto (persists across restarts, small diff, but a proto
change and a per-policy selection-filter change across five policy files), or a separate
keyed store in `compactionTaskMeta` (more isolated, more plumbing). Needs a decision
before implementation.

**Clustering's `retryOnError` bug** (§2.1) — not fixed, to keep this change's blast
radius to the three previously-unprotected task types.

**`saveSegmentMeta`'s implicit-infinite-retry paths** (§2.1) — left as pre-existing
behavior; would need auditing what errors the underlying catalog/KV writes actually
raise before converting safely.
