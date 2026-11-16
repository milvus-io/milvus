# Latency-weighted QueryView replica selection

Related issue: [#55587](https://github.com/milvus-io/milvus/issues/55587).
The time-decayed latency, failure accrual, p99 trigger, and hedge win-rate
algorithms originate in [#55588](https://github.com/milvus-io/milvus/pull/55588)
by @xiaofan-luan. This implementation applies them to master's QueryView client.

## Problem and scope

The QueryView client retries unavailable views and workers, but a selected
QueryNode that does not return can hold up a shard despite another ready
replica. The existing replica directory already supplies eligible replica IDs
and versions; it does not rank them by observed request latency or race a slow
attempt against another replica.

The opt-in policy covers shard Search and Query in both QueryView client entry
points. Statistics and requery bound to retained Search plans keep their existing
execution paths. The default proxy policy and existing QueryView round-robin
routing remain unchanged. This is a local proxy routing choice, with no durable
metadata, WAL message, or cross-cluster replication change.

## Selection and feedback

A proxy records observations separately for each channel, operation, and
consistency level. Each replica has a time-decayed successful latency mean,
consecutive negative observations, a decaying penalty, and an in-flight count.
Selection weight is `1 / (latency * (in-flight count + 1))`. Unknown or stale replicas inherit the mean of known
eligible replicas. A probability floor preserves exploration. Inactivity alone
does not count as failure or create a penalty.

Three consecutive negative observations apply a penalty. Only failures from
QueryNode execution, or a hedge overtaking an attempt with currently outstanding
QueryNode execution, are attributed to a replica. Input errors and caller
cancellation do not provide failure evidence. A shared WAL-owner planning or
growing-side stall alone does not penalize any replica. A single attempt cannot
record both an overtake and a late error as two negative observations. A late
success still records its measured latency and resets consecutive failures.

Selections retain the configuration sampled at shard-request entry. Live
configuration changes cannot leave unmatched accounting for dispatched work.
Idle replica entries are pruned even when their channel remains active. Idle
channel/class entries are also reclaimed. Cleanup is lazy, at most once per
minute; no additional background goroutine is required.

## Hedged execution

The first attempt uses weighted selection. After the greater of the configured
minimum delay and recent successful request p99, the client may dispatch one
additional replica from the current subscribed directory snapshot. The lookup
performs no coordinator RPC and never waits for a refreshed directory. An
invalid snapshot or absence of another eligible replica skips that hedge. Normal
retry and subscription handling continue to handle view/session changes.

Recent hedge outcomes suppress speculative work when its win rate is low, while
ten percent of eligible opportunities remain probes. Quantile sorting operates
on a copied sample ring outside the policy mutex. Retry rounds retain the
existing attempt budget/full-pass rules, with at most one hedge per round.

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
Hedging therefore cannot bypass that common dependency and can add load to it.
The implementation makes no cluster-wide throughput or recovery-time claim;
those require workload and fault benchmarks.

## Configuration and observability

Set `proxy.replicaSelectionPolicy: latency_weighted` before starting the proxy.
The policy name is restart-only. Its live settings are:

| Setting under `proxy.latencyWeighted` | Default | Meaning |
| --- | --- | --- |
| `enabled` | `true` | New shard requests use this policy; false restores existing QueryView routing. |
| `decay` | `10s` | Latency and penalty decay time constant. |
| `minShare` | `0.001` | Minimum share per eligible replica, capped by replica count. |
| `hedge.enabled` | `true` | Permit one extra attempt per retry round. |
| `hedge.minDelay` | `200ms` | Lower limit of the p99-based trigger. |
| `hedge.minWinRate` | `0.3` | Below this recent win rate, retain only ten-percent probes. |

`milvus_proxy_replica_hedge_total` uses only proxy node ID and decision labels:
`sent`, `won`, `skipped_low_win_rate`, and `skipped_no_replica`. It does not create
per-channel or per-replica metric series. Master currently exposes no resource
group selection in this QueryView request contract; the policy uses only the
replicas offered by its existing directory.

## Verification

Deterministic tests hold the first QueryNode dispatch open to demonstrate the
baseline wait and the enabled hedge's complete winner. Other tests cover fixed
MVCC, exact retained selections, empty Query, isolated late loser results,
two-stage Search, retained requery failure, local target selection, feedback
attribution, and idle replica cleanup. Existing router/session/retry tests cover
the disabled path, whose executeShard implementation is unchanged.
Search and Query regression tests also cover wire-encoded worker OOM followed
by sibling success, both failure orders, retry exhaustion, InputError followed
by sibling success, all-attempt failure, single-attempt InputError, and caller
cancellation while the sibling is pending.

Distributed fault injection, workload throughput measurements, and cancellation
isolation across merged QueryNode tasks remain follow-up validation before
considering default enablement or active loser cancellation.
