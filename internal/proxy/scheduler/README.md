# Scheduler Package

The `scheduler` package owns the proxy's task queues and the scheduling loops
that drive task execution. It was extracted verbatim from the proxy root
package's `task_scheduler.go` (issue #44761) and now depends only on the shared
task model (`taskmodel`) plus `pkg/v3`.

## Overview

The proxy exposes four task queues:

- **DdQueue** (definition) — DDL tasks such as create/drop collection, alias,
  index, database, resource-group operations.
- **DmQueue** (manipulation) — DML tasks: insert/delete/upsert.
- **DqQueue** (query) — search and query tasks, with a separate queue for
  requery and lexical highlight tasks. Each queue has its own dispatch loop
  and worker pool so a Search waiting on a subtask can finish even when the
  main pool is full.
- **DcQueue** (control) — data-control operations such as flush.

RPC handlers and Search tasks enqueue work. The dispatcher runs each task's
`PreExecute -> Execute -> PostExecute` lifecycle under a bounded worker pool.

## Responsibilities

1. **Queues** (`DdTaskQueue`, `DmTaskQueue`, `DqTaskQueue`, `DcTaskQueue`) —
   each maintains an unissued list and an active map, plus the enqueue/dequeue
   and task-lookup primitives.
2. **`TaskScheduler`** — owns the queues and their loops (`definitionLoop`,
   `controlLoop`, `manipulationLoop`, and one `queryLoop` for each DQL queue),
   and exposes `Start`/`Close`.
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
`DqQueue`, `DcQueue`) so the proxy's RPC handlers can call `Enqueue` directly.

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
