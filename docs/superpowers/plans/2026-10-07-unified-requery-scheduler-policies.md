# Unified Requery Scheduler Policies Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make one Milvus image support `fifo`, `requery-edf`, and a simple fixed-credit `requery-priority` read scheduling policy, while keeping scheduler diagnostics entirely policy-neutral and allowing the requery credit to take effect at runtime.

**Architecture:** Keep policy selection as a startup-time factory decision, so changing `scheduleReadPolicy.name` requires a QueryNode restart but never a new image. Extract only the shared regular/requery lane mechanics used by EDF and the new credit policy. The credit policy is work-conserving, uses one positive `int64` credit setting, refreshes that setting on scheduler decisions, and replenishes credit only after an actual regular-task handoff. Diagnostics stay in the generic scheduler lifecycle and receive the policy name only as an opaque metric label.

**Tech Stack:** Go, Milvus `paramtable`, Prometheus metrics, existing QueryNode scheduler unit tests and benchmarks.

**Spec:** [PR #53876](https://github.com/milvus-io/milvus/pull/53876), plus the approved constraints from this task on 2026-10-07.

## Global Constraints

- The implementation worktree is `/Users/zilliz/.codex/worktrees/requery-policy-unification/milvus` at PR #53876 head. Do not implement this plan in `/Users/zilliz/Documents/project/milvus`.
- One image must expose all three policies. Policy changes may require a Pod restart; hot policy replacement is out of scope.
- Keep the existing key `queryNode.scheduler.requeryPriorityBaseCredit`. It is a positive `int64`, defaults to `3`, and is hot-applied by code rather than merely marked refreshable.
- Do not port the adaptive level/window/hysteresis controller from PR #52190. Reuse only its small “actual handoff” observation mechanism.
- Diagnostics must not import, type-assert, branch on, or receive callbacks from a concrete policy. A policy string may remain as an opaque label.
- Preserve work-conserving behavior: if only one lane has runnable work, it can use all available execution slots.
- Run Go tests with `-tags dynamic,test -gcflags="all=-N -l"` as required by this repository.

## Industry-Convention Boundary

- **Adopted:** the standard work-conserving rule: an idle lane does not reserve an execution slot while another lane has runnable work. This matches the scheduling terminology in [RFC 7806](https://www.rfc-editor.org/rfc/rfc7806.html).
- **Adapted:** the fixed credit is a two-lane, weighted-round-robin-like burst quota: while both lanes are backlogged, serve at most `credit` requery tasks before one regular task. Unlike byte-based DRR in [RFC 8290](https://www.rfc-editor.org/rfc/rfc8290.html), one task consumes one credit because this design intentionally does not estimate variable task cost.
- **Retained separately:** `requery-edf` continues to select the earlier absolute deadline. EDF is a mature policy, but this Milvus implementation does not claim the admission or bandwidth guarantees of Linux `SCHED_DEADLINE`/CBS described in the [Linux scheduler documentation](https://docs.kernel.org/scheduler/sched-deadline.html).
- **Rejected for this enhancement:** task-cost prediction, DRR-style cost deficits, adaptive credit levels, time-window feedback, and policy hot replacement. Those mechanisms add estimation/state complexity not required for the controlled comparison.

---

### Task 1: Remove diagnostics coupling to EDF

**Files:**

- Modify: `internal/util/searchutil/scheduler/tasks.go`
- Modify: `internal/util/searchutil/scheduler/concurrent_safe_scheduler.go`
- Modify: `internal/util/searchutil/scheduler/requery_edf_policy.go`
- Modify: `internal/util/searchutil/scheduler/diagnostics.go`
- Modify: `internal/util/searchutil/scheduler/diagnostics_test.go`
- Modify: `internal/util/searchutil/scheduler/diagnostics_benchmark_test.go`
- Modify: `pkg/metrics/scheduler_diagnostics.go`

- [x] **Step 1: Write policy-neutral diagnostics tests**

Replace the EDF-specific diagnostics assertions with tests that exercise diagnostics through the generic scheduler surface:

```go
func TestDiagnosticsSeriesArePolicyNeutral(t *testing.T) {
    policies := []string{
        schedulePolicyNameFIFO,
        schedulePolicyNameRequeryEDF,
    }
    // Enable diagnostics for each scheduler, collect metric families, and assert
    // that the same metric names and series counts exist for every policy.
}

func TestDiagnosticsTracksGenericRequerySelectionStreak(t *testing.T) {
    // Feed selected queuedTask values to generic diagnostics and assert that
    // regular/requery transitions update the streak without naming EDF.
}
```

Delete or rewrite tests that inspect `requeryEDFPolicy.diagnostics`, `edfChoice*`, or EDF head-gap buckets. Keep the cardinality budget assertion, but make it compare policies instead of special-casing EDF.

- [x] **Step 2: Run the diagnostics tests and confirm they fail**

Run from the implementation worktree:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/searchutil/scheduler \
  -run 'TestDiagnostic|TestDiagnostics'
```

Expected: failures because scheduler construction still discovers EDF through a concrete type switch and EDF still owns diagnostics callbacks and metrics.

- [x] **Step 3: Pass the policy name explicitly into the generic scheduler**

Change the private constructor from:

```go
func newScheduler(policy schedulePolicy) Scheduler
```

to:

```go
func newScheduler(policyName string, policy schedulePolicy) Scheduler
```

Make `NewScheduler` pass the matching constant from each factory branch. Update direct test constructors to provide an explicit label. Remove the concrete-policy type switch from `newScheduler`; the string must be stored and used only as a diagnostics label.

- [x] **Step 4: Remove policy-owned diagnostics**

Remove the diagnostics field and `recordChoice` calls from `requeryEDFPolicy`. Remove EDF-only choice reasons, head-gap histograms, and `QueryNodeSchedulerDiagnosticChoice` metric declarations/registration.

Keep the requery selection streak only if it is computed in generic diagnostics from the selected task kind. Initialize it for every policy and rename help text from “EDF streak” to “requery selection streak.” Do not branch on the policy label during metric allocation, collection, or logging.

- [x] **Step 5: Run focused tests and the existing diagnostics benchmark**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/searchutil/scheduler \
  -run 'TestDiagnostic|TestDiagnostics'

go test -tags dynamic,test -gcflags="all=-N -l" -run '^$' \
  -bench 'BenchmarkSchedulerDiagnostics' -benchmem \
  ./internal/util/searchutil/scheduler
```

Expected: tests pass; disabled diagnostics stay allocation-free on the scheduling path; no new material regression relative to the pre-change benchmark.

- [x] **Step 6: Commit the policy-neutral diagnostics change**

```bash
git add internal/util/searchutil/scheduler pkg/metrics
git commit -s -m "enhance: decouple scheduler diagnostics from policy"
```

---

### Task 2: Add the fixed-credit configuration

**Files:**

- Modify: `pkg/util/paramtable/component_param.go`
- Modify: `pkg/util/paramtable/component_param_test.go`
- Modify: `configs/milvus.yaml`

- [x] **Step 1: Add failing configuration tests**

Cover the default, validation, full `int64` range, and runtime refresh behavior:

```go
func TestRequeryPriorityBaseCredit(t *testing.T) {
    params := &ComponentParam{}
    params.Init(NewBaseTable(SkipRemote(true), SkipEnv(true)))
    assert.Equal(t, int64(3), params.QueryNodeCfg.RequeryPriorityBaseCredit.GetAsInt64())

    params.Save("queryNode.scheduler.requeryPriorityBaseCredit", "6")
    assert.Equal(t, int64(6), params.QueryNodeCfg.RequeryPriorityBaseCredit.GetAsInt64())

    params.Save("queryNode.scheduler.requeryPriorityBaseCredit", "9223372036854775807")
    assert.Equal(t, int64(9223372036854775807), params.QueryNodeCfg.RequeryPriorityBaseCredit.GetAsInt64())
}
```

Add table cases for `0`, negative values, malformed values, and overflow; each must fall back to `3`.

- [x] **Step 2: Run the focused test and confirm failure**

```bash
(cd pkg && go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./util/paramtable -run 'TestRequeryPriorityBaseCredit')
```

- [x] **Step 3: Define the positive `int64` config**

Add to `QueryNodeConfig`:

```go
RequeryPriorityBaseCredit ParamItem `refreshable:"true"`
```

Initialize it with key `queryNode.scheduler.requeryPriorityBaseCredit`, default `3`, `Export: true`, and a formatter based on `strconv.ParseInt(value, 10, 64)` that accepts only values greater than zero. The documentation must say that `requery-priority` rereads it at scheduler decision points, so a saved value can take effect without recreating the scheduler.

Add to `configs/milvus.yaml`:

```yaml
requeryPriorityBaseCredit: 3
```

Update the policy-name comment to list `fifo`, `requery-edf`, and `requery-priority`. Update `requeryUnsolvedQueueSize` documentation to state that it applies to both requery-lane policies.

- [x] **Step 4: Run the focused test and commit**

```bash
(cd pkg && go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./util/paramtable -run 'TestRequeryPriorityBaseCredit')

git add pkg/util/paramtable/component_param.go \
  pkg/util/paramtable/component_param_test.go \
  configs/milvus.yaml
git commit -s -m "enhance: add requery priority credit config"
```

---

### Task 3: Implement shared requery lanes and fixed-credit scheduling

**Files:**

- Create: `internal/util/searchutil/scheduler/requery_lanes.go`
- Modify: `internal/util/searchutil/scheduler/tasks.go`
- Modify: `internal/util/searchutil/scheduler/requery_edf_policy.go`
- Create: `internal/util/searchutil/scheduler/requery_priority_policy.go`
- Create: `internal/util/searchutil/scheduler/requery_priority_policy_test.go`
- Modify: `internal/util/searchutil/scheduler/requery_edf_test.go`
- Modify: `internal/util/searchutil/scheduler/concurrent_safe_scheduler.go`
- Modify: `internal/util/searchutil/scheduler/concurrent_safe_scheduler_test.go`
- Modify: `internal/util/searchutil/scheduler/policy_test.go`

- [x] **Step 1: Add a failing factory test**

Add a scheduler test that requests `requery-priority` and expects construction to succeed:

```go
func TestNewSchedulerSupportsRequeryPriority(t *testing.T) {
    require.NotPanics(t, func() {
        require.NotNil(t, NewScheduler(schedulePolicyNameRequeryPriority))
    })
    require.Panics(t, func() { NewScheduler("unknown") })
}
```

- [x] **Step 2: Add exact credit-sequence tests**

Use regular tasks `R` and requery tasks `Q`. With both lanes continuously non-empty and credit `2`, assert the selected sequence is:

```text
Q, Q, R, Q, Q, R
```

Also assert:

- only `Q` queued: `Q` drains without consuming or blocking on credit;
- only `R` queued: `R` drains immediately;
- a canceled or expired task is skipped before lane selection;
- the requery capacity limit remains independent from the regular queue;
- `Cleanup`, `Remove`, and `Len` cover both lanes exactly once.

- [x] **Step 3: Add hot-apply tests**

Test the credit-update rule while the scheduler object remains alive:

```go
// Preserve already consumed credit rather than grant a fresh burst.
// oldLimit=3, remaining=1 => used=2
// newLimit=5 => newRemaining=3
// newLimit=1 => newRemaining=0
```

Save the new paramtable value, call the next `Pop`, and assert the new limit affects that decision. This proves code-level hot apply; the `refreshable` tag alone is not sufficient evidence.

- [x] **Step 4: Add actual-handoff replenishment tests**

Add a small test-only execution channel and verify:

- popping a regular task does not replenish credit by itself;
- dropping, expiring, removing, or clearing a regular task does not replenish credit;
- only a successful scheduler-to-executor channel send replenishes credit;
- all three successful handoff paths call the observer: the normal loop, `produceExecChan`, and shutdown draining.

- [x] **Step 5: Run the new tests and confirm failure**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/searchutil/scheduler \
  -run 'TestNewSchedulerSupportsRequeryPriority|TestRequeryPriority|TestTaskServedObserver|TestRequeryLane'
```

- [x] **Step 6: Extract the shared two-lane mechanics**

Create a private helper used by EDF and credit scheduling:

```go
type requeryLanes struct {
    regular         *fifoPolicy
    requery         *mergeTaskQueue
    requeryCapacity int64
}
```

Move only lane-neutral behavior into it: admission, task classification, push, cleanup, remove, and length. Embed it in both policies so their selection logic remains small and independent. EDF must continue to set and compare scheduling deadlines exactly as before.

- [x] **Step 7: Register and implement the fixed-credit policy**

Add the startup-selectable name and factory branch:

```go
const schedulePolicyNameRequeryPriority = "requery-priority"
```

`NewScheduler(schedulePolicyNameRequeryPriority)` constructs `newRequeryPriorityPolicy()`. Keep the existing panic for unknown names.

Use this minimal state:

```go
type requeryPriorityPolicy struct {
    *requeryLanes
    configuredCredit int64
    remainingCredit  int64
}
```

On every `Pop`, read `RequeryPriorityBaseCredit.GetAsInt64()`. When it changes, preserve the already consumed portion:

```go
used := max(int64(0), p.configuredCredit-p.remainingCredit)
p.configuredCredit = next
p.remainingCredit = max(int64(0), next-used)
```

Selection rules:

1. Discard canceled/expired lane heads using the same cleanup semantics as EDF.
2. If one lane is empty, pop the other immediately.
3. If both lanes are non-empty and `remainingCredit > 0`, pop requery and decrement credit.
4. Otherwise pop regular.
5. Do not replenish credit during `Pop`.

This is a bounded preference, not a resource-cost estimator and not the adaptive controller from PR #52190.

- [x] **Step 8: Add the actual-handoff observer**

Add a private optional interface:

```go
type taskServedObserver interface {
    onTaskServed(*queuedTask)
}
```

After each successful send to `execChan`, have the generic scheduler notify the policy if it implements this interface. The credit policy resets `configuredCredit` and `remainingCredit` to the latest configured value only when the handed-off task is regular. No diagnostics code may know about this interface.

- [x] **Step 9: Run scheduler tests and commit**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/searchutil/scheduler

git add internal/util/searchutil/scheduler
git commit -s -m "enhance: implement fixed-credit requery scheduling"
```

---

### Task 4: Prove cross-policy behavior, diagnostics neutrality, and low overhead

**Files:**

- Modify: `internal/util/searchutil/scheduler/diagnostics_test.go`
- Modify: `internal/util/searchutil/scheduler/diagnostics_benchmark_test.go`
- Modify: `internal/util/searchutil/scheduler/requery_priority_policy_test.go`

- [x] **Step 1: Extend the cross-policy matrix to all three policies**

Run the same generic diagnostics assertions for:

```go
[]string{
    schedulePolicyNameFIFO,
    schedulePolicyNameRequeryEDF,
    schedulePolicyNameRequeryPriority,
}
```

Assert that enabling diagnostics does not change the task selection order for any policy, and that all policies expose the same diagnostics metric names and series budget. A different policy label value is expected; a different metric schema is not.

- [x] **Step 2: Add credit-policy benchmarks**

Benchmark the credit policy with diagnostics disabled and enabled under:

- a regular-only queue;
- a requery-only queue;
- sustained lane contention;
- a full requery queue near `requeryUnsolvedQueueSize`.

Report `ns/op` and `allocs/op`. Do not claim production performance from microbenchmarks; use them only to reject an obvious scheduler hot-path regression.

- [x] **Step 3: Run the full scheduler verification**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/searchutil/scheduler

go test -race -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/util/searchutil/scheduler

go test -tags dynamic,test -gcflags="all=-N -l" -run '^$' \
  -bench 'Benchmark.*(Diagnostics|RequeryPriority)' -benchmem \
  ./internal/util/searchutil/scheduler
```

- [ ] **Step 4: Verify the QueryNode startup integration**

Blocked locally: the worktree has no matching `milvus_core`, `milvus-storage`, `rdkafka`, or `rocksdb` native build. Reusing the main checkout's native output reaches compilation but fails on a `milvus-storage` C ABI mismatch, so this suite must run in CI or after building native libraries from this exact worktree revision.

The current QueryNode reads `scheduleReadPolicy.name` once when constructing the scheduler. Run its existing suite to ensure the factory signature change does not break startup and confirm by inspection that no runtime policy replacement is introduced:

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 \
  ./internal/querynodev2/...
```

- [x] **Step 5: Run static decoupling checks**

These searches must produce no matches in generic diagnostics/scheduler construction:

```bash
rg -n 'requeryEDFPolicy|requeryPriorityPolicy|edfChoice|QueryNodeSchedulerDiagnosticChoice|policy == schedulePolicyName' \
  internal/util/searchutil/scheduler/diagnostics.go \
  internal/util/searchutil/scheduler/concurrent_safe_scheduler.go \
  pkg/metrics/scheduler_diagnostics.go
```

Confirm that the old adaptive controller was not reintroduced:

```bash
rg -n 'priorityLevel|hysteresis|successWindow|adaptive' \
  internal/util/searchutil/scheduler/requery_priority_policy.go
```

Expected: no matches.

- [x] **Step 6: Run repository hygiene checks and review the diff**

```bash
gofmt -w internal/util/searchutil/scheduler/*.go \
  pkg/util/paramtable/component_param.go \
  pkg/util/paramtable/component_param_test.go

git diff --check
git status --short
git diff --stat
```

Perform an adversarial review focused on: replenishment happening before a real handoff, credit updates granting an accidental fresh burst, diagnostics depending on a policy type/name, and work-conserving behavior when one lane is empty.

- [x] **Step 7: Commit final tests and verification updates**

```bash
git add internal/util/searchutil/scheduler
git commit -s -m "test: verify unified query scheduler policies"
```

## Acceptance Criteria

- The same binary/image accepts `fifo`, `requery-edf`, or `requery-priority`; changing the policy requires only config plus QueryNode restart.
- `queryNode.scheduler.requeryPriorityBaseCredit` is a positive `int64`, defaults to `3`, and an updated value affects the live credit policy on its next scheduling decision.
- The credit policy is fixed and work-conserving; it contains no adaptive level, time window, or resource-cost model.
- Credit is replenished only after a regular task is actually handed to an executor.
- FIFO and EDF selection semantics remain unchanged except for the behavior-preserving shared lane extraction.
- Diagnostics are collected exclusively by generic scheduler lifecycle code. Their metric schema and code path are identical across all three policies, apart from the opaque policy label value.
- Scheduler tests and race tests pass with the repository-required build tags and flags; benchmarks show no obvious hot-path allocation or latency regression.
