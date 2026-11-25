# Latency-weighted replica selection and hedged reads on 3.0

Related issue: [#55587](https://github.com/milvus-io/milvus/issues/55587).
This design adapts the prototype in
[#55588](https://github.com/milvus-io/milvus/pull/55588), authored by
xiaofan-luan, to the 3.0 delegator execution path.

## Scope and configuration

Replica selection policy is configured via `proxy.replicaSelectionPolicy`
(`round_robin`, `look_aside`, or `latency_weighted`; default `look_aside`).
`proxy.replicaSelectionPolicy` is refreshable (`refreshable: true`): live changes
apply to new channel requests, while an in-flight channel request retains the
policy and accounting owner captured at entry to `ExecuteWithRetry`.

Hedged reads are configured orthogonally under `proxy.hedging` (default
`enabled: false`) and apply to Search and Query (including Search requery and
query iterator reads) regardless of which `proxy.replicaSelectionPolicy` is
active. Each channel request sends at most one hedge across all retry rounds,
keeping speculative hedging orthogonal to `proxy.retryTimesOnReplica` error
retries. Other shardclient users use the configured selection policy without
hedging. No QueryNode protocol, persisted metadata, or WAL messages change.
Routing statistics are local to each Proxy and are not replicated between
clusters.

| Parameter | Default | Meaning |
| --- | --- | --- |
| `proxy.replicaSelectionPolicy` | `look_aside` | Replica selection policy (`round_robin`, `look_aside`, `latency_weighted`); refreshable |
| `proxy.latencyWeighted.decay` | `10s` | Time constant for successful latency samples and penalties |
| `proxy.latencyWeighted.minShare` | `0.001` | Minimum selection probability among eligible replicas |
| `proxy.hedging.enabled` | `false` | Enable hedged Search/Query attempts across replicas, independent of `proxy.replicaSelectionPolicy` |
| `proxy.hedging.minDelay` | `200ms` | Minimum wait before looking for a hedge target |

Internally, `hedgeOutcomes` retains a `0.3` win-rate threshold (`defaultHedgeMinWinRate`) to
throttle ineffective hedges to a 10% probe rate.

## Selection and failure feedback

Replica observations are keyed by channel, request kind/consistency class, and
delegator node ID. The effective latency is the greater of the successful
time-decayed mean and the decaying failure penalty. Selection uses effective
latency (`effectiveLatency`) and the number of dispatched attempts still in
flight (`inFlight`):

```text
weight = 1 / (effectiveLatency × (inFlight + 1))
```

Candidates without recent observations use the mean of known candidates.
Every eligible candidate retains the configured minimum share. Topology,
resource-group scope and serviceability preference constrain candidates.
Existing unreachable-node marks constrain weighted selection. The existing
serviceable preferred-node fast path bypasses weighted selection and remains
unchanged.

Three consecutive failed attempts without an intervening success establish a
penalty equal to the attempt's available timeout, capped at one hour. Without
a caller deadline the assumed timeout is one minute. Both an execution error
and being overtaken by a successful hedge can provide failure evidence, but
one attempt contributes at most one failure. A late successful loser still
records its actual latency and resets the consecutive-failure count.

`attemptFeedback` serializes completion with overtake feedback. It records
whether an attempt has actually completed, rather than treating an unread
result-channel message as proof that the attempt still runs.

Input errors are ignored by replica learning and terminate retry. An actual
caller `context.Canceled` is also ignored. Deadline expiry and other system
errors remain failure observations. `ErrAttemptSuperseded` means that a
successful attempt lost result publication; it is observed as success and
consumed inside shardclient rather than returned as a wire error.

The weighted retry path uses request-local exclusions and decaying penalties.
It neither reads nor writes the legacy channel blacklist, so that blacklist
does not suppress recovery probes. Existing `look_aside` and `round_robin`
requests preserve the existing blacklist logic. Retrying starts after 10ms;
the shared retry options cap subsequent pauses at 20ms. The unscoped leader
count remains the retry budget, preserving 3.0's bounded polling behavior for
a resource group whose replicas are still becoming ready.

## Resource-group scope

`ExecuteWithRetry` applies the existing request-context scope before entering
either policy. `CollectionWorkLoad.ForChannel` carries the request class and
hedging capability alongside that scope, so namespace single-channel paths
and ordinary fan-out use the same selection inputs.

Primary selection, retries, and hedge target selection all filter candidates
by the request's resource group. A named group with no leader retains the
existing retriable `ErrCollectionNotFullyLoaded` result. A hedge cannot clear
exclusions or select the primary again. If only one eligible replica exists
inside the group, replicas in other groups do not make hedging possible.

Pooled request latency and hedge win-rate histories include the request scope
in their key. The unscoped request population has its own history. A slow or
overloaded group cannot thereby raise another group's hedge threshold or
suppress its hedges. Per-node successful latency, failure, and in-flight
observations remain shared when requests use the same channel/class/node.

## Hedged attempt lifecycle

The delay is the greater of the minimum delay and the recent p99 of the first
successful result in an attempt round. This is neither full client latency
nor the duration of slow losing attempts. The bounded sample ring holds up
to 1024 observations. It uses observations within ten seconds, retaining at
least the latest 100 when traffic is sparse. Samples are copied while holding
the balancer mutex and sorted after releasing it.

When the delay expires, the main result loop starts an asynchronous candidate
lookup. The lookup may use the cache or wait for coordinator refresh, but it
never reserves workload and never reads the mutable exclusion set. The main
loop remains able to receive the primary result during this wait. On a
successful lookup, the main loop applies the current exclusions and scope,
selects the target, reserves in-flight workload, and dispatches at most one
hedge. If selection of an alternate cached candidate fails, one fresh lookup
is attempted. A single-replica snapshot does not force a refresh just to hedge.

The lookup has a round-local child context. Returning from the round cancels
the lookup, including when the primary fails fast and the normal retry loop
will start another round. A buffered result channel and cancellation-aware
send prevent a late lookup result from blocking or creating a late hedge.

Dispatched primary and hedge attempts keep the outer request context. The
winner does not immediately cancel the loser: older QueryNodes can merge
Search tasks under a leading task's context. Earlier cancellation requires
cancellation isolation on every receiving QueryNode, including during upgrades. A losing attempt can therefore keep
running while other shards or Search requery finish.

Search and Query call `CommitAttempt` before writing any successful result.
Only the winner publishes its result and associated metadata. This includes
Search's serving-node map and Query's `actualChannelsMvcc` map; a late success
cannot overwrite the snapshot used by later processing. The existing input
MVCC and requery semantics are unchanged.

A canceled QueryNode RPC does not invalidate the shard leader cache when its
caller context is actually canceled. Active-context transport errors,
deadlines, NotShardLeader, and collection-not-loaded failures retain their
existing invalidation behavior.

## Hedge admission and observability

The latest 128 hedge outcomes supply a ten-second win-rate window. With fewer
than ten recent outcomes, hedges are allowed. Below the minimum win rate,
10% of potential hedges remain as probes. This is feedback-based suppression,
not a hard concurrency limit or a guarantee against added load during a burst.

The new counter `milvus_proxy_replica_hedge_total` has Proxy node ID and
`decision` labels: `sent`, `won`, `skipped_low_win_rate`,
`skipped_no_replica`, and `skipped_inflight_limit`. It does not add channel,
collection, or resource-group labels.

## Verification and limits

Focused tests cover:

- Weighted selection, decay, probing, failure classification and recovery.
- Completion-before-overtake and overtake-before-late-success/error orderings.
- A blocked coordinator lookup while the primary succeeds or fails fast,
  including lookup cancellation and absence of late workload reservations.
- Enable-switch changes in both directions, stable release accounting and
  retention of legacy blacklist behavior while the weighted policy is off.
- Resource-group candidate filtering and independent pooled hedge histories.
- Actual Search/Query result publication, winning node/MVCC metadata, and
  cache invalidation for caller cancellation versus deadline/transport errors.
- QueryNode status classification surviving serialization and actual task wrapping.

The failure-path audit traces QueryNode `Search`/`Query` through
`searchChannel`/`queryChannel`, delegator readiness and `waitTSafe`,
`merr.Status`, the distributed QueryNode client, and the task callbacks.
Collection-not-loaded, channel-not-found/not-available and tSafe-stalled errors
are system errors; BM25 metric validation produces an input error.
`merr.Status` carries the input flag and `merr.Error` reconstructs it. The
distributed client converts retriable statuses to errors even with one attempt;
this preserves their system classification before the task observes them.
Caller cancellation is checked against the original attempt context, rather
than inferred from a remote gRPC cancellation code. Native execution errors
keep the existing classification; this change does not claim to repair every
upstream error-construction site.

The prototype's fault-injection numbers are not measurements of this port.
Production throughput, fault recovery, burst overload, multiple Proxies, and
more than two replicas require separate deployment measurements. Heavy and
light requests within one class/scope still share a hedge threshold. Proxies
learn independently, and a shared WAL/consistency bottleneck cannot be bypassed
merely by choosing a different QueryNode replica.

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
latency/failure accounting and does not enter `lb.blacklist`: the replica has
more work than it gets done, not that it is failing.

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

**One extra attempt, then the refusal goes to the client.** A `Hedgeable` or `OverloadRetryable` read
gets at most one extra attempt on another replica across hedging and overload
failover combined. Either the first replica refuses it as overloaded and it
moves to another replica from the request's captured leader snapshot at once
(bypassing policy retry backoff and hedge delay), or the hedge delay fires and uses that slot. A second overload refusal
ends the request immediately with `ErrServiceTooManyRequests` without trying a
third replica or refreshing shard leaders. Non-overload replica failures (down
node, broken connection) do not consume the extra-attempt slot. This applies
under all replica selection policies (`round_robin`, `look_aside`,
`latency_weighted`), respecting resource group scoping when present.

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
a replica failure. `ClientBase` marks local context failures only when it has
not entered any RPC callback, including expiry after the shard client was
acquired. This marker preserves the original context error and releases selection
accounting without recording replica failure penalties. Search/Query callbacks
also avoid invalidating the shard cache for this local failure.
After callback entry, a deadline failure remains replica feedback because the
client cannot establish whether the request was dispatched. System errors already observed from an attempt take
precedence over a later sibling cancellation; InputError keeps its precedence.

Queue wait is checked again when a worker starts, before `PreExecute` and
`Execute`, including time blocked handing off to the worker pool. The
`scheduled` queue-duration observation is recorded at that point. Rejection
occurs when the worker becomes available; queue budgets do not install timers.
Compatible legacy Searches may merge with different queue budgets. Each original
request retains its enqueue time and captured queue limit. At dequeue and worker
start, cancellation, deadline expiry, and queue-budget expiry remove only the
affected members; surviving members retain their own budgets after owner
promotion. Existing NQ, request compatibility, and deadline-gap merge rules still
apply. Queue-duration observations remain per scheduler entry: a partially
pruned group does not add a separate observation for every rejected member.

Unknown replica latency uses the candidate fallback before applying the current
failure penalty. A recent penalty still suppresses a replica with no successes;
a small decayed penalty cannot make an unknown replica appear faster than the
fallback. Hedge-delay samples continue to be collected while hedging is disabled
so live enabling can use the observed workload latency.

## Default behavior and retry capability

With `maxQueueWaitRatio=0` and `proxy.hedging.enabled=false`, the new queue wait
limit and concurrent hedging are off. Overload fail-fast and one serial alternate
attempt are still active, including existing queue-capacity refusals. Compared
with the old retry loop, this can return a refusal sooner and avoids repeated
work against overloaded replicas. It can also reject a request that might have
succeeded after a longer wait; disabling hedging does not restore the old overload
retry behavior. No leader-cache lookup or coordinator refresh is performed for
this extra attempt, even if another request invalidated the cache.

`Hedgeable` permits concurrent attempts and requires isolated publication through
`CommitAttempt`. `OverloadRetryable` permits only one serial failover after a
refusal and requires the callback to have published no results or writes on that
failure. Search and Query already meet the stronger hedging contract; GetStatistics, Highlight, and RunAnalyzer
opt into serial failover without concurrent execution. This capability does not
enroll an RPC in the QueryNode read scheduler; RunAnalyzer currently calls the
analyzer directly. Delete-by-expression does
not opt in: its Query stream can already have produced DeleteTasks before a later
receive fails, so restarting the whole callback could repeat writes. Requests
without either capability propagate an overload refusal immediately.

Round-robin hedges use an independent cursor. A hedge selecting the sole
alternative no longer advances the primary cursor and biases subsequent serial
requests toward the slow replica. This does not impose deterministic iteration
order on the candidate map or change LookAside workload accounting.
