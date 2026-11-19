# Latency-weighted QueryView replica selection and hedged reads

Related issue: [#55587](https://github.com/milvus-io/milvus/issues/55587).
The time-decayed latency, failure accrual, p99 trigger, and hedge win-rate
algorithms originate in [#55588](https://github.com/milvus-io/milvus/pull/55588)
by @xiaofan-luan. This implementation applies them to master's QueryView client
with live-refreshable policy selection, orthogonal hedging configuration, and
concurrent speculative load bounding.

## Problem and scope

The QueryView client retries unavailable views and workers, but a selected
QueryNode that does not return can hold up a shard despite another ready
replica. The existing replica directory already supplies eligible replica IDs
and versions; it does not rank them by observed request latency or race a slow
attempt against another replica.

Replica selection (`proxy.replicaSelectionPolicy`) and hedged execution
(`proxy.hedging.*`) are configured orthogonally and cover shard Search and Query
in both QueryView client entry points. Statistics and requery bound to retained
Search plans keep their existing execution paths. Defaults remain unchanged
(`proxy.replicaSelectionPolicy: look_aside`, which preserves QueryView's
round-robin router, and `proxy.hedging.enabled: false`). Both can be toggled
live for new shard requests without restarting the proxy. This is a local proxy
routing choice, with no durable metadata, WAL message, or cross-cluster
replication change.

## Selection and feedback

When `proxy.replicaSelectionPolicy` is `latency_weighted`, a proxy records
observations separately for each channel, operation, and consistency level. Each
replica has a time-decayed successful latency mean, consecutive negative
observations, a decaying penalty, and an in-flight count. Selection weight is
`1 / (latency * (in-flight count + 1))`. Unknown or stale replicas inherit the
mean of known eligible replicas. A probability floor preserves exploration.
Inactivity alone does not count as failure or create a penalty.

Three consecutive negative observations apply a penalty. Only failures from
QueryNode execution, or a hedge overtaking an attempt with currently outstanding
QueryNode execution, are attributed to a replica. Input errors and caller
cancellation do not provide failure evidence. A shared WAL-owner planning or
growing-side stall alone does not penalize any replica. A single attempt cannot
record both an overtake and a late error as two negative observations. A late
success still records its measured latency and resets consecutive failures.

Note that `retryableQueryError` intentionally retains QueryView's existing retry
classification (view/session changes and transport unavailability) rather than
retrying worker business errors such as `CodeMemAllocateFailed` (OOM) across
replicas inside `shard_client`, avoiding cascading OOM amplification while still
recording negative replica feedback for subsequent routing.

Selections retain the configuration sampled at shard-request entry. Live
configuration changes cannot leave unmatched accounting for dispatched work.
Idle replica entries are pruned even when their channel remains active. Idle
channel/class entries are also reclaimed. Cleanup is lazy, at most once per
minute; no additional background goroutine is required.

## Hedged execution and speculative load budget

Hedging can be enabled under any `proxy.replicaSelectionPolicy` (using weighted
selection under `latency_weighted` and QueryView round-robin otherwise). After
the greater of `proxy.hedging.minDelay` and the recent successful request p99,
the client may dispatch one speculative attempt to another replica from the
current subscribed directory snapshot. The lookup performs no coordinator RPC
and never waits for a refreshed directory. An invalid snapshot or absence of
another eligible replica skips that hedge.

To prevent retry-times-hedge multiplication and overload cascades, speculative
work is bounded by two mechanisms:

1. **Single hedge per shard request**: At most one hedge attempt is dispatched
   across the entire lifecycle of a shard request (across all failure-retry
   rounds), keeping `proxy.hedging.*` orthogonal to sequential error retries.
2. **Win-rate guard**: Recent hedge outcomes cut unproductive hedges
   (win rate below 30% over at least 10 samples in 10s) to 10% recovery probes.

Every attempt owns its GetQueryPlan request payload, plan, response buffers,
and two-stage Search state. All workers must succeed before an attempt can
publish. The first complete successful attempt adds its responses to the shard
reducer and returns that same attempt's complete ShardPlan, including version,
MVCC, work nodes, segment selections, and execution summary. If reducer
publication fails, the shard's partial publication is reset and the request
fails. A loser never reaches the request reducer or replaces the retained plan.

A failed attempt does not abort an already dispatched sibling: that sibling
can still publish a successful result even when the first error is not
retryable. Once all dispatched attempts fail, any non-retryable error ends the
request; otherwise the existing retry rules apply. InputError also waits for
already dispatched siblings: result-size limits can depend on the serving
node or snapshot. If no attempt succeeds, an input error takes precedence
over other attempt errors and prevents another round. Caller cancellation
still ends the request immediately.

A supplied MVCC or BarrierMVCC is preserved by every attempt. Ordinary requests
continue sampling consistency on the primary WAL owner as before. Internal
requery uses only the retained winning plan; a retired view returns an error
rather than selecting a new replica or snapshot. Empty Query results still
retain their successful plan.

Selecting a winner does not cancel already dispatched loser RPCs. Existing
caller cancellation and per-attempt error cancellation remain in force. This
avoids introducing an earlier cancellation that could interact with QueryNode
SearchTask merging. Consequently, speculative work can continue after a winner
is known until it completes or the request context ends.

All replicas share the primary WAL owner for planning and growing data.
Hedging therefore cannot bypass that common dependency; the per-shard
single-hedge limit bounds amplification to at most one extra attempt per shard
request, and the win-rate guard suppresses unproductive hedging after 10
outcomes via completion-based feedback (rather than a global concurrency cap).

## Configuration and observability

All settings are live-refreshable (`refreshable: true`) and apply to new shard
requests:

| Setting | Default | Meaning |
| --- | --- | --- |
| `proxy.replicaSelectionPolicy` | `look_aside` | Replica selection policy (`round_robin`, `look_aside`, `refined_look_aside`, or `latency_weighted`); QueryView uses round-robin unless `latency_weighted` is selected. |
| `proxy.latencyWeighted.decay` | `10s` | Exponential decay time constant for replica latency samples and failure penalties. |
| `proxy.latencyWeighted.minShare` | `0.001` | Minimum selection share per eligible replica, capped by replica count. |
| `proxy.hedging.enabled` | `false` | Enable hedged Search/Query attempts across replicas, independent of `replicaSelectionPolicy`. |
| `proxy.hedging.minDelay` | `200ms` | Lower bound of the p99-based hedge trigger delay. |

`milvus_proxy_replica_hedge_total` uses only proxy node ID and decision labels:
`sent`, `won`, `skipped_low_win_rate`, and
`skipped_no_replica`. It does not create per-channel or per-replica metric
series. Master currently exposes no resource group selection in this QueryView
request contract; the policy uses only the replicas offered by its existing
directory.

## Verification

Deterministic tests hold the first QueryNode dispatch open to demonstrate the
baseline wait and the enabled hedge's complete winner, both with
`latency_weighted` and with decoupled round-robin hedging. Additional tests verify single-hedge-per-shard enforcement across retry rounds, fixed MVCC,
exact retained selections, empty Query, isolated late loser results, two-stage
Search, retained requery failure, local target selection, feedback attribution,
and idle replica cleanup. Search and Query regression tests cover wire-encoded
worker OOM followed by sibling success, both failure orders, retry exhaustion,
InputError followed by sibling success, all-attempt failure, single-attempt
InputError, and caller cancellation while the sibling is pending.
