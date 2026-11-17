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
| `proxy.hedging.maxInflightRatio` | `0.1` | Proxy-level cap on concurrent in-flight hedged attempts relative to concurrent in-flight hedgeable primary attempts |

`maxInflightRatio` defaults to `0.1` (10%), referencing gRPC's `retryThrottling`
(`tokenRatio = 0.1`, gRFC A6), to bound worst-case speculative fan-out under
cluster-wide tail latency spikes without requiring per-cluster tuning. A
built-in floor of `2` concurrent hedges preserves hedging at low QPS, and
`inflightHedges` is held until the hedged RPC goroutine actually returns (so
un-canceled lingering losers count against the budget). Internally,
`hedgeOutcomes` retains a `0.3` win-rate threshold (`defaultHedgeMinWinRate`) to
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
winner does not immediately cancel the loser: QueryNode can merge Search
tasks under a leading task's context, so earlier cancellation requires a
separate cancellation-isolation change. A losing attempt can therefore keep
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

Regression tests exercise real Search/Query task callbacks and status
conversion, both error arrival orders, winner metadata, and no new retry after
all attempts fail. Heartbeat lifecycle tests exercise registration, the failure
threshold, cleanup, rediscovery, and recovery without manually seeding health
records. RPC outcomes are controlled in tests; no distributed configuration
rollout or snapshot-divergence reproduction is claimed.
