# Latency weighted replica selection with hedged reads

This implements an opt-in policy for the 2.6 delegator read path, based on
[xiaofan-luan's prototype PR #55588](https://github.com/milvus-io/milvus/pull/55588).
Related issue: [#55587](https://github.com/milvus-io/milvus/issues/55587).

`proxy.replicaSelectionPolicy` (`round_robin`, `look_aside`, or `latency_weighted`;
default `look_aside`) is refreshable (`refreshable: true`): live changes apply to
new channel requests, while in-flight requests retain the policy and accounting
captured at entry. `proxy.hedging` (`enabled: false`, `minDelay: 200ms`) is orthogonal to `proxy.replicaSelectionPolicy` and
applies to Search/Query regardless of which selection policy is active, sending
at most one hedge per channel request across all retry rounds (orthogonal to
`proxy.retryTimesOnReplica`). No wire protocol, storage format, persistent
metadata, or CDC state changes are involved.

## Problem and scope

A request already sent to a replica that stops answering cannot benefit from
ordinary failover until its RPC returns an error or reaches its deadline.
Fast errors also must not make a failing replica look attractive to a
latency-based selection policy.

This policy combines outcome-based replica selection with one optional hedge
per channel request. It applies to the existing proxy-to-delegator path. It does
not solve a shared WAL availability problem, guarantee zero failures, or bound
additional cluster load under every workload. Current master's QueryView path
needs a separate implementation.

## Selection and observations

Replica observations are keyed by channel, request class, and delegator node.
A request class is the request kind and its effective consistency level, such
as `search/Strong` or `query/Bounded`. Each proxy learns independently.

The weight is `1 / (effective latency * (in-flight attempts + 1))`. Effective latency is the larger of a time-decayed successful latency
mean and a decaying failure penalty. A candidate without history uses the mean
of the other candidates, or 100 microseconds when no history exists.

Every eligible replica keeps a minimum selection share, by default 0.1%, for
recovery probes. Existing serviceability and unreachable-node checks constrain
weighted selection; minimum share does not override those checks. The existing
serviceable preferred-node fast path for requery remains in effect, including
its bypass of balancer reachability checks and workload accounting.

A replica receives a penalty after three consecutive failed attempts without
a successful observation between them. The penalty uses the attempt's
remaining timeout at dispatch, defaults to 60 seconds without a deadline,
and is capped at one hour. It decays with a default time constant of 10 seconds.
Successful observations reset the consecutive failure counter; the existing
penalty continues to decay rather than disappearing immediately.

| Outcome | Observation |
|---|---|
| Success | Actual elapsed latency; reset consecutive failures |
| Late successful result discarded by `CommitAttempt` | Same successful observation |
| Input error | Ignore for replica health |
| Caller context canceled | Ignore for replica health |
| Other error, including deadline expiry | One failure |
| Primary still pending when the hedge succeeds | One failure |

An attempt can contribute at most one failure. Its completion and overtake
feedback are serialized: a later error does not double-count an earlier
overtake, and a completion already recorded is not treated as still pending
just because its result has not been consumed by the result loop. A late
success still updates the successful latency and resets consecutive failures.

## Hedge execution

Search and Query, including requery and query iterators, opt into hedging.
Other load-balancer callers receive weighted selection without hedging.

A hedge becomes eligible after the larger of the request class's p99 and
`proxy.hedging.minDelay`, which defaults to 200 milliseconds. The p99 uses successful
retry-round completion latency until the winning result, not the losing RPC's
latency or end-to-end latency across retries. Samples come from the latest
10 seconds, falling back to the latest 100 samples when fewer are recent,
with a ring capacity of 1024. Without history, the minimum delay applies.
The sample window is copied under the balancer lock and sorted outside it.

The main result loop never performs a blocking coordinator lookup:

1. On the hedge timer, check recent hedge outcomes and start an asynchronous
   candidate lookup using the shard cache first.
2. The lookup returns a candidate snapshot only. It does not select a node,
   reserve workload, or read the main loop's mutable exclusion set.
3. The main loop filters the returned candidates using its current exclusions
   and always excludes the primary. It selects and reserves a hedge only when
   dispatching it. A single candidate does not force a coordinator refresh.
4. If alternatives exist but selection fails, a fresh lookup may run
   asynchronously. Lookup failure skips the hedge and leaves the primary
   running. An ordinary primary failure returns to the existing retry loop.
5. Completion of the round cancels the lookup. A late lookup result cannot
   dispatch a hedge in the next round or leak an in-flight reservation.

Primary and hedge share a `CommitAttempt` condition. Exactly one successful
attempt can publish a channel result and its corresponding node information;
the other returns `ErrAttemptSuperseded` and contributes only a successful
latency observation.

Dispatched attempts retain the outer request context. The loser is not
canceled immediately when one shard obtains a winner: older QueryNodes may
merge Search tasks under the leading task's context. Earlier cancellation
requires isolation on every receiving QueryNode, including during upgrades. A caller-canceled RPC does not invalidate the proxy's shard cache;
deadline expiry and transport failures with an active caller retain the
existing invalidation behavior.

Recent hedge outcomes suppress speculative traffic when it does not help.
Among the latest 128 outcomes, at least 10 within 10 seconds are required to
use the win rate. Below the default minimum win rate of 30%, only 10% of
eligible hedges are dispatched as probes. With insufficient recent outcomes,
hedging remains enabled. This is feedback control, not a hard concurrency or
request-rate budget.

## Configuration and accounting

These parameters are refreshable and apply to new channel requests.

| Key | Default | Meaning |
|---|---|---|
| `proxy.replicaSelectionPolicy` | `look_aside` | Selection policy |
| `proxy.latencyWeighted.decay` | `10s` | Latency and penalty decay time constant |
| `proxy.latencyWeighted.minShare` | `0.001` | Minimum candidate share |
| `proxy.hedging.enabled` | `false` | Enable hedged Search and Query attempts |
| `proxy.hedging.minDelay` | `200ms` | Minimum hedge delay |

The hedge win-rate threshold is the internal constant
`defaultHedgeMinWinRate` (`0.3`).

The accounting owner is captured at `ExecuteWithRetry` entry. Changing
`proxy.replicaSelectionPolicy` affects later calls; in-flight calls keep their original selection,
release, and retry behavior. Weighted reservations are released when the
attempt completes, including a losing attempt. Look-aside workload is released
through the exact underlying balancer that reserved it.

Weighted retries start with a 10 millisecond pause and are capped at
20 milliseconds by the existing retry utility. No generic retry defaults are
changed.

The counter `milvus_proxy_replica_hedge_total{node_id,decision}` records `sent`,
`won`, `skipped_low_win_rate`, and `skipped_no_replica`. The node label identifies
the proxy, not every candidate; channel or collection labels are not added.

## Verification and remaining limits

Focused tests cover candidate lookup blocked while the primary succeeds or
fails, cancellation of the lookup at round end, late lookup delivery, failure
feedback ordering, hot configuration switches, late loser publication, and
caller cancellation versus deadline cache invalidation. Pure statistics tests
cover decay, minimum shares, quantiles, hedge outcomes, and cleanup.

The original prototype's fault-injection measurements are available in
PR #55588. They are historical evidence for the approach, not measurements of
this implementation. Multi-proxy fault recovery, rolling upgrades, production
workload distributions, sudden overload, and coordinator refresh amplification
still need deployment-level measurement.

Heavy and light reads in one request class share a hedge threshold and win
rate. Per-proxy learning can delay convergence at low request rates. A minimum
share is probabilistic rather than a bound on recovery time. The existing
lookup cache does not coalesce concurrent refreshes. No new refresh-coalescing
or hedge-concurrency budget is introduced without measurements justifying it.

This 2.6 path does not carry a request-level resource group routing scope;
it selects from all eligible advertised replicas for the channel. A port to
a branch with scoped routing must preserve that scope for primary, hedge,
retry, and pooled hedge statistics. Such a port must also reconcile an existing
cross-request channel blacklist with the policy's penalty and probe mechanism.

## Attempt errors and heartbeat health

An InputError prevents a new retry round but does not discard an already
running sibling. If either dispatched attempt succeeds, its result wins.
If both fail, the input error is retained even when the other error arrives
later. With only one dispatched attempt, an input error still returns
immediately. Result-size errors can vary with the serving node's limit or
snapshot; their input classification alone does not prove both attempts fail.

Weighted selection creates the heartbeat health record for each candidate.
If heartbeat cleanup removed a record, reinitialization preserves an
unreachable mark from the retained consecutive heartbeat failure count.
Healthy heartbeats or successful cost updates restore eligibility through
the existing health transitions. Minimum share does not bypass this check.

An overload refusal (`ErrServiceTooManyRequests`) is also ignored in replica
latency/failure accounting: the replica has more work than it gets done, not
that it is failing.

## Overload

Under load above what the replicas get done, requests wait in the QueryNode
read scheduler queue (`queryNode.scheduler.unsolvedQueueSize`, 1024), turning
excess traffic into long queue latency. The following behavior bounds the wait of
answered requests and fail the excess fast:

**QueryNode queue wait limit.** `queryNode.scheduler.maxQueueWaitRatio`
(default `0`, off; refreshable) limits how long a read task may wait in the
queue:

```
limit = min(maxQueueWait, maxQueueWaitRatio * time left until the deadline)
```

`queryNode.scheduler.maxQueueWait` defaults to `1s` (refreshable) and is also
the limit for a task without a deadline; the intended ratio is `0.1`. A new
task is refused immediately with `ErrServiceTooManyRequests` when the task that
leaves the queue next (`head`) has already waited for more than half of the new
task's limit. A task whose deadline has passed fails as expired rather than
overloaded, and a task that waited longer than its limit by the time it leaves
the queue is refused then. `milvus_querynode_read_task_queue_duration{outcome="overloaded"}`
counts refused tasks.

**One extra attempt, then the refusal goes to the client.** A `Hedgeable` read
gets at most one extra attempt on another replica across hedging and overload
failover combined. Either the first replica refuses it as overloaded and it
moves to another replica at once (bypassing policy retry backoff and hedge
delay), or the hedge delay fires and uses that slot. A second overload refusal
ends the request immediately with `ErrServiceTooManyRequests` without trying a
third replica or refreshing shard leaders. Non-overload replica failures (down
node, broken connection) do not consume the extra-attempt slot. This applies
under all replica selection policies (`round_robin`, `look_aside`,
`latency_weighted`).

**No resending inside the RPC client.** `grpcclient.WithFailFastOnOverload`
prevents Proxy and Delegator gRPC clients from retrying `ErrServiceTooManyRequests`
against the same node, returning the overload status immediately so the caller
can failover or abort.

**Dispatched shards retain the caller context.** `LBPolicyImpl.Execute`
waits for sibling channels using their outer request context. This also keeps
unfinished hedge attempts alive when the channel fan-out completes. During a
rolling upgrade, an older QueryNode can share a merged Search's cancellation
with unrelated callers. Earlier cross-channel cancellation requires every
receiving QueryNode to provide merged-request cancellation isolation, even
when hedging and queue protection are disabled.

Regression tests exercise real Search/Query task callbacks and status
conversion, both error arrival orders, winner metadata, and no new retry after
all attempts fail. Heartbeat lifecycle tests exercise registration, the failure
threshold, cleanup, rediscovery, and recovery without manually seeding health
records. RPC outcomes are controlled in tests; no distributed configuration
rollout or snapshot-divergence reproduction is claimed.

Requery channels skipped locally are filtered before replica selection. They
create no replica observations or hedge-delay samples. A request that expires
while acquiring a client also releases selection accounting without recording
a replica failure. System errors already observed from an attempt take
precedence over a later sibling cancellation; InputError keeps its precedence.

Queue wait is checked again when a worker starts, before `PreExecute` and
`Execute`, including time blocked handing off to the worker pool. The
`scheduled` queue-duration observation is recorded at that point. Rejection
occurs when the worker becomes available; queue budgets do not install timers.
Searches with a positive queue budget merge only when their absolute queue
expiry times match. This preserves the expiry when cancellation promotes a
surviving member, at the cost of fewer merge opportunities under queue limits.

Unknown replica latency uses the candidate fallback before applying the current
failure penalty. A recent penalty still suppresses a replica with no successes;
a small decayed penalty cannot make an unknown replica appear faster than the
fallback. Hedge-delay samples continue to be collected while hedging is disabled
so live enabling can use the observed workload latency.
