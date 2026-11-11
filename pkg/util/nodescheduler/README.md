# NodeScheduler

NodeScheduler shares a bounded worker pool and an unbounded, non-blocking task
queue. `New(concurrency)` creates an executor; `Get()` returns the process-wide
executor with dynamically configured concurrency. Existing `Scheduler`, `Task`
and `TaskHandle` interfaces are unchanged.

## Priority and ordering

`Submit(task, WithPriority(int32Value))` assigns an absolute priority. The default
is 0; larger signed values are preferred, and negative values remain runnable.
Values must be between `MinPriority` (-20) and `MaxPriority` (20), inclusive.
`WithPriority` and the built-in handle's `SetPriority` panic on invalid values.
Multiple priority options use the last supplied value.

Handles returned by the built-in scheduler implement `PriorityTaskHandle`:

```go
handle := scheduler.Submit(task, nodescheduler.WithPriority(10))
if priorityHandle, ok := handle.(nodescheduler.PriorityTaskHandle); ok {
    priorityHandle.SetPriority(20)
}
```

`SetPriority` replaces the value and returns false after the task finishes. It is
safe to call concurrently with cancellation, completion and shutdown. Concurrent
updates are serialized by the scheduler lock; callers needing domain-specific
update ordering must serialize their own updates.

- Ready tasks are reordered without changing their ready age.
- Running tasks are never interrupted, canceled or restarted by priority changes.
  The updated value applies only if the task subsequently retries.
- Delayed tasks retain their exact deadline and backoff state. Priority does not
  make them eligible before that deadline.

A worker claims a task under the scheduler lock. Once claimed, it is running for
scheduling purposes, even if its `Execute` call has not started yet. Equal
priorities use FIFO **dispatch** order by the current ready-queue admission.
Concurrent workers may begin or finish executions in a different order.
Retry admission gets a new ready age; reprioritization preserves it. All-default
submissions therefore retain FIFO dispatch behavior.

## Fairness

Normally the scheduler chooses the highest priority. After at most 8 consecutive
priority dispatches, it dispatches the oldest eligible ready task once, then repeats.
The counter is shared by all workers and resets when the ready queue empties.
Reprioritization cannot reset a task's age or this counter.

The value 8 is an internal policy: under sustained contention it reserves one
out of nine dispatch opportunities for the oldest task, while allowing priority
to choose the other eight. A task with N older ready tasks is selected within at
most `9 * (N + 1)` further dispatches, provided it remains eligible under the worker
limits below and workers keep completing attempts.
New submissions and retries join behind it in age order. This bounds dispatch
opportunities, not elapsed time, CPU share or completion latency. Tasks must not
synchronously wait for other tasks on the same saturated executor.

Fairness is per task, not per collection, View or PChannel. A producer submitting
many tasks can still consume more worker capacity. The policy does not define
business weights, per-producer quotas, cooperative yielding or dependency waiting.

## Worker limits

Priority bands impose cumulative limits on running attempts:

| Priority | Maximum running tasks at this priority or below | Current use |
| --- | --- | --- |
| -20 through 0 | 50% of workers | Segment flush and other data tasks (0) |
| 1 through 9 | 75% of workers | QueryView build/ready (5) |
| 10 through 19 | 90% of workers | QueryView release (10) |
| 20 | 100% of workers | Broadcast ACK (20) |

Each limit is rounded down, with a minimum of one worker. With 20 workers,
the cumulative limits are 10, 15, 18 and 20. Higher-priority tasks can consume
unused capacity; lower-priority tasks cannot borrow reserved capacity even when
no higher-priority work is pending. A workload containing only default-priority
tasks therefore uses at most 50% of workers (subject to rounding).
Small pools can have identical limits in adjacent bands; a one-worker pool has
no reserved capacity. Fairness never bypasses a worker limit.

An attempt is charged to its band when dispatched, until it returns. Changing
its priority while running does not move that charge or preempt it; the next
retry uses the new priority. Shrinking concurrency can temporarily leave more
running attempts than allowed. Those attempts finish normally, and new attempts
wait until their cumulative limits permit admission.

## Retry and lifecycle

`Execute` returning `ErrDelay` uses the submission's existing exponential backoff
(or `WithBackoff`); `DelayUntil(t)` requests retry no earlier than t. Retries keep
the same handle and latest priority. An expired explicit deadline rejoins the
ready queue immediately. Errors other than `ErrDelay` remain terminal.

Cancellation removes queued/delayed work immediately. Running tasks retain their
execution context until they return; `Cancel` and `Close` cancel that context as
before. `Wait` waits for completion and returns only errors from its own wait
context, not the task's execution error. Close drains queued/delayed tasks and
waits for running workers. Terminal handles detach cancellation and priority
callbacks so retaining a handle does not retain its task or scheduler.

The ready queue uses an indexed priority heap and four indexed age heaps, one
per band. Fairness examines at most four age-heap roots, without scanning blocked
backlogs. Submit,
reprioritization, dispatch and removal cost O(log N), with O(N) pending storage.
Delayed retries keep their separate indexed deadline heap and single timer.
Priority adds no worker, per-task goroutine or timer.

The WAL recovery scoped scheduler forwards priority options and dynamic handle
updates while retaining owner tracking, cancellation and Close behavior. A legacy
inner handle without `PriorityTaskHandle` rejects dynamic updates with false.
The wrapper does not close the shared executor. Broadcast ACK uses a 100 ms
RPC deadline and returns `ErrDelay` on failure, releasing its worker while
waiting for retry. Its preceding legacy import completion RPC has no additional
timeout, but remains cancelable; that transitional path can occupy a worker for
longer than 100 ms. In-memory QueryView callbacks run in separate goroutines.
QueryView scoring, wire fields and resource-priority aggregation are separate work.
