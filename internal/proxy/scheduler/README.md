# Scheduler Package

The `scheduler` package owns the proxy's task queues and the scheduling loops
that drive task execution. It was extracted verbatim from the proxy root
package's `task_scheduler.go` (issue #44761) and now depends only on the shared
task model (`taskmodel`) plus `pkg/v3`.

## Overview

The proxy exposes five task queues:

- **DdQueue** (definition) — DDL tasks such as create/drop collection, alias,
  index, database, resource-group operations.
- **DmQueue** (manipulation) — DML tasks: insert/delete/upsert.
- **DqQueue** (query) — search and query tasks, with a separate queue for
  requery and lexical highlight tasks. Each queue has its own dispatch loop
  and worker pool so a Search waiting on a subtask can finish even when the
  main pool is full.
- **DcQueue** (control) — data-control operations such as flush.
- **MSearchQueue** — MultiSearch batches, with 64 waiting slots and eight workers
  independent of DQL. Workers pop only when free, so a saturated worker pool
  leaves all waiting batches inside the admission bound.

RPC handlers and Search tasks enqueue work. The dispatcher runs each task's
`PreExecute -> Execute -> PostExecute` lifecycle under a bounded worker pool.

## Responsibilities

1. **Queues** (`DdTaskQueue`, `DmTaskQueue`, `DqTaskQueue`, `MSearchTaskQueue`) —
   `DcQueue` uses `DdTaskQueue`. Each maintains an unissued list and an active map, plus the enqueue/dequeue
   and task-lookup primitives.
2. **`TaskScheduler`** — owns the queues and their loops (`definitionLoop`,
   `controlLoop`, `manipulationLoop`, and one `queryLoop` for each DQL queue),
   plus the MSearch workers, and exposes `Start`/`Close`.
3. **TSO + ID allocation** — `Enqueue` allocates a timestamp (or an ID from the
   meta cache for tasks that skip timestamp allocation) before a task is
   unissued.
4. **Metrics** — `GetMetrics` reports pending/executing task counts and
   timing, consumed by the proxy's quota/system-info metrics. Both DQL queues
   contribute to the `dql` metrics.

## Architecture

```
┌───────────────────────────────────────────────────────────────┐
│                       TaskScheduler                            │
│                                                               │
│   DdQueue ──► definitionLoop ──► processTask(Pre/Exec/Post)   │
│   DmQueue ──► manipulationLoop ──► processTask                 │
│   DqQueue ──► queryLoop ──► processTask                        │
│   DcQueue ──► controlLoop ──► processTask                      │
│   MSearchQueue ──► multiSearchLoop workers ──► processTask     │
│                                                               │
│   queues hold: unissued list · active map · TSO allocator      │
└───────────────────────────────────────────────────────────────┘
```

### Key types

```go
func NewTaskScheduler(ctx context.Context, tsoAllocator taskmodel.TsoAllocator,
    opts ...SchedOpt) (*TaskScheduler, error)

func (s *TaskScheduler) Start() error
func (s *TaskScheduler) Close()
func (s *TaskScheduler) GetMetrics() []metricsinfo.TaskQueueMetrics
func (s *TaskScheduler) ClearDQLQueue(taskType string, reason string) ClearTaskQueueResult
```

The `TaskScheduler` exposes its queues as fields (`DdQueue`, `DmQueue`,
`DqQueue`, `DcQueue`, `MSearchQueue`) so the proxy's RPC handlers can call
`Enqueue` directly.

DQL and MSearch share `BaseTaskQueue.enqueueWithContext`: `TaskCondition`
removes a still-queued task when its waiter returns on cancellation or expiry.
MultiSearch derives its execution context from both the caller and
`TaskScheduler.Context()`. `Close` cancels that context and waits for the MSearch
workers, including searches still unwinding after a timeout response. Batch
result finalization belongs to the concrete task; the scheduler continues to
use the standard success-only `PreExecute -> Execute -> PostExecute` sequence.

## Dependency rule

`scheduler` imports `taskmodel` (for `Task`/`DMLTask`/`TsoAllocator` and the
channel/timestamp types) and `pkg/v3` (`conc`, `metrics`, `metricsinfo`, `merr`,
`paramtable`, `tsoutil`, `typeutil`). It has no `internal/*` imports and never
imports the proxy root package, so the one-way `proxy -> scheduler -> taskmodel`
edge stays acyclic.

## Related components

- **taskmodel** (`internal/proxy/taskmodel/`): the interfaces this package
  schedules against.
- **proxy root** (`internal/proxy/`): constructs the scheduler in `Proxy.Init`
  and enqueues concrete tasks from `impl.go` / `snapshot_impl.go`; consumes
  `GetMetrics` from `metrics_info.go`.
