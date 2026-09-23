# Specifications

TLA+ models of parts of Milvus whose correctness rests on interleaving rather
than on any single path through the code. They are checked with TLC.

Running one needs the TLA+ tools jar and a JVM:

```
curl -fsSLO https://github.com/tlaplus/tlaplus/releases/latest/download/tla2tools.jar
java -cp tla2tools.jar tlc2.TLC -config Isolated.cfg -workers 8 -deadlock MergedGroupCancel
```

`-deadlock` is needed because every behaviour ends with every request answered,
which TLC would otherwise report as a deadlock.

## MergedGroupCancel

Models how a QueryNode search group survives the cancellation of one of the
requests merged into it, for
[the running-request list and cancel design](../20260916-running-request-list-and-cancel.md)
and [issue 53508](https://github.com/milvus-io/milvus/issues/53508).

A QueryNode folds compatible searches waiting in its scheduler queue into one
group and runs them as a single segcore call. A cancellation can land at four
moments: before the request is enqueued, while the group waits in the queue,
between the group leaving the queue and reaching the executor, and while the
group is executing. A waiting group can also leave the queue through the expiry
sweep that runs when the queue is full. Tests can sample these one at a time;
the model enumerates every interleaving of them.

What is checked:

| Property | Meaning |
| --- | --- |
| `AnsweredAtMostOnce`, `AnsweredWhenDone` | every caller is answered exactly once |
| `CanceledLearnTheirOwnFate` | a canceled request is told it was canceled |
| `NobodyElseIsCanceled` | a request nobody canceled is never told it was |
| `CountersNeverNegative`, `CountersClearWhenIdle`, `CountersCoverTheQueue` | the scheduler's waiting counters are conserved across pruning |
| `Termination` | every request eventually gets its answer |

Three constants turn the model back into code this design replaces, so the
properties can be shown to fail on that rather than only to hold on this:

| Configuration | Result |
| --- | --- |
| `Isolated.cfg` | four requests, 145,243 distinct states, no error |
| `BeforeTheFix.cfg` (`Isolation = FALSE`) | fails: a request nobody canceled is told it was, and a canceled member whose owner is alive is told the group's result instead of its own cancellation |
| `CounterAccounting.cfg` (`RememberNQ = FALSE`) | fails: debiting the pruned size rather than the size the counters were credited with leaves the waiting counter above zero forever |
| `ExpiryOwnerOnly.cfg` (`IsolateExpiry = FALSE`) | fails: the expiry sweep, deciding on the owner's context alone, tells a request nobody canceled that it was. The waiting counters stay conserved on this path |

Five requests also passes, at 3,916,092 distinct states in about three minutes.

The expiry sweep was not in the first version of the model: it was left out as
unrelated to cancellation, and review found that it broke the rule. The
`ExpiryOwnerOnly` configuration is the model with the sweep added but not
isolated, and it reports the case review described, step for step.

What the model still leaves out: which searches may merge, time itself, and
what a search computes. A deadline that has passed, or is close enough that
the sweep treats it as passed, is modelled as the request being canceled. The
sweep only runs when the queue is full; the model lets it run at any time,
which admits strictly more behaviours.

The model is not the code. It follows `Merge`, `PruneCanceled`,
`useGroupContext`, `Done`, `ExpiryReady` and `FinishExpired` in
`internal/querynodev2/tasks/search_task.go`; `setupExecListener`, `schedule`,
`exec` and `cleanupExpiredTasks` in
`internal/util/searchutil/scheduler/concurrent_safe_scheduler.go`; and
`cleanupReady` in `internal/util/searchutil/scheduler/tasks.go`. Changing
either of those without revisiting the model leaves the model describing
something that no longer exists.
