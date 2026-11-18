# Latency weighted replica selection with hedged reads

- Status: prototype on 2.6, measured on a fault-injection test cluster. Not
  ported to master.
- Policy name: `proxy.replicaSelectionPolicy: latency_weighted`. Opt-in;
  `look_aside` stays the default.

## Problem

With several replicas, a proxy should stop sending reads to a replica that has
failed and move them to the healthy ones within seconds. With `look_aside` it
does not:

- A replica that fails fast (connection refused, not ready, channel not
  available) looks fast to a latency based score, so it keeps its share.
- A replica on a powered off node never answers. Nothing is learned until the
  request times out, and the cost metrics that steer `look_aside` expire after
  one second.
- Requests already sent to the failed replica wait for their deadline.

On a two replica collection, after one query node lost power, the successful
searches per 5s fell from about 400 to 19 / 7 / 9 / 10 until the coordinator
removed the node after its session TTL.

## Goals

- A replica fault is invisible to Bounded/Eventually reads of a multi-replica
  collection: no failures, a short latency spike.
- Do not make overload worse.
- A heavy request, or a single error, must not move traffic off a healthy
  replica.
- No change for single replica collections, and none unless the policy is
  selected.

## The policy in three rules

1. **Share by latency.** The attempts of a channel and request class are
   spread over the replicas in inverse proportion to their recent latency and
   in-flight attempts. Every replica keeps at least 0.1%.
2. **Three failures in a row.** A replica on which three attempts in a row
   failed, with no success in between, is given the timeout as its latency. A
   failure is an error, a timeout, or being overtaken by a hedge.
3. **Hedge after the p99.** An attempt that has not returned after the p99
   latency of its class (at least 200ms) gets a second attempt on another
   replica; the first result wins. When fewer than 30% of the recent hedges
   won, only 10% of the hedges are sent.

The rest of this document details them.

## Statistics

Statistics are kept per **channel x request class x replica**. The request
class is the request kind and the consistency level the request is served at
(`search/Strong`, `query/Bounded`, ...). Outcomes of different classes say
different things about a replica: while the WAL lags, Strong reads fail on
every replica, and Bounded reads on the same replicas succeed.

Per replica (`replica_stats.go`):

| Item | How |
|---|---|
| Success latency | time-decayed mean, time constant `decay` (10s) |
| Penalty | set by rule 2, decays with the same time constant |
| In-flight attempts | a count |
| Failures in a row | a count, reset by any success |

The latency of a replica is the larger of its mean and its penalty. How long
an in-flight attempt has been running does not count: a heavy request (a LIKE
over many rows, a large limit) running long on a healthy replica must not push
traffic away from it.

Per channel and request class, over all replicas:

| Item | How |
|---|---|
| Request latency (`latency_samples.go`) | the latest 1024 samples, one per successful channel request: the time until its first result |
| Hedge outcomes (`hedge_outcomes.go`) | whether each of the latest 128 hedges won |

## Rule 1: share by latency

Weight = 1 / (latency x (in-flight + 1)), and

share = `minShare` + (1 - n x `minShare`) x weight / sum of weights

for n replicas. A replica without statistics gets the mean latency of the
others, so a new replica starts with an even share. Each attempt picks a
replica at random by share. Replicas the look-aside health check marked
unreachable are skipped.

| Replica A | Replica B | In flight (A / B) | Share of A | Share of B |
|---|---|---|---|---|
| 10ms | 10ms | 0 / 0 | 50% | 50% |
| 10ms | 12ms | 0 / 0 | 54.5% | 45.5% |
| 10ms | 30ms | 0 / 0 | 75% | 25% |
| 10ms | 10ms | 0 / 3 | 80% | 20% |
| 10ms | 5s (penalty) | 0 / 0 | 99.7% | 0.3% |

The minimum share keeps probing a failed replica. A probe that hangs is
covered by a hedge, so probing costs latency to nobody.

## Rule 2: three failures in a row

How an attempt is recorded (`attempt_outcome.go`):

| Outcome | Recorded as |
|---|---|
| success, or `ErrAttemptSuperseded` (it succeeded after the other attempt had published) | its elapsed time, into the mean; resets the failures in a row |
| input error (`merr.InputError`) | nothing: the request is at fault |
| context canceled (the caller stopped waiting, or the request returned with another attempt's result) | nothing |
| refused as overloaded (`ErrServiceTooManyRequests`, see Overload) | nothing: the replica has more work than it gets done, it is not failing |
| any other error, a timeout of the request included | a failure |
| still running when a hedge on another replica succeeded | a failure |

Once 3 attempts in a row failed on a replica it gets a penalty of the last
attempt's timeout: the request deadline minus the attempt start, or 60s for a
request without deadline (the gRPC entry of the proxy sets no default
deadline; the REST entry does, `proxy.http.requestTimeoutMs`, 30s). A failed
attempt counts as if it had taken as long as the request was willing to wait,
so a replica that fails fast does not look fast. The share comes back after
about decay x ln(penalty / normal latency): 62s for a 5s timeout against 10ms
latency, 87s without deadline.

Why in a row, as in consecutive-failure outlier detection (Envoy
`consecutive_5xx`, Finagle failure accrual): a single failure is weak
evidence. A healthy replica returns an occasional "channel not available" or
"segment not loaded" during a balance or a segment handoff; a heavy request
may time out on it; one slow request may be overtaken by a hedge. In each
case it keeps answering other requests, which resets the count, and nothing
changes. A replica that is really down fails every attempt:

- fast errors: each is retried on another replica after 10ms, so three take
  tens of milliseconds;
- no answer at all: its attempts are overtaken by hedges, three of which take
  a few hundred milliseconds at a moderate request rate. An overtaken attempt
  is canceled with its request and not heard of again, which is why being
  overtaken itself counts.

A request that is heavy on every replica is not overtaken quickly, so no
replica is blamed for it.

Errors are deliberately not told apart further. `ErrChannelTSafeStalled` (505)
is returned both when the WAL lags (every replica) and when one replica's
consumer is stuck (that replica only); the code cannot tell the cases apart.
With statistics per class, an error every replica returns raises every
replica alike and leaves the shares unchanged, while the same error from one
replica moves the traffic away from it.

## Rule 3: hedged attempts

`hedge.go`, for search and query tasks:

- **When.** An attempt that has not returned after max(p99, `hedge.minDelay`)
  gets a second attempt on another replica, if there is one (checked on the
  cached shard leaders, so a single replica collection never refreshes them
  for this). At most one hedge per attempt; an attempt that fails fast goes
  back to the retry loop instead.
- **The p99** is taken over the request latency of the class within the last
  10s, or, when these are fewer than 100 samples, over the latest 100 however
  old (all of them while fewer were seen). Without any sample the delay is
  `hedge.minDelay`, so a rare class is hedged from its first request. It is
  request latency, not attempt latency: per attempt, the slow losers of
  hedges would raise the p99, and with it the delay, during a fault.
- **One result.** The search and query tasks call `CommitAttempt` right
  before inserting into their result buffers; the late attempt returns
  `ErrAttemptSuperseded`.
- **The request's context.** Both attempts run on it: they keep its deadline,
  and the loser is canceled when the request returns. A hedge never outlives
  its request, and a heavy request is not left running twice.
- **Held back when it does not pay off.** The win rate is taken over the
  hedge outcomes of the last 10s among the latest 128. Below
  `hedge.minWinRate` only 10% of the hedges are sent, as samples that let the
  win rate recover; with fewer than 10 outcomes in the last 10s a hedge is
  always sent. A hedge won when it returned the result; it lost when the first
  attempt did, when both failed, or when the request deadline came first.
  Under overall overload every replica is slow and hedges stop winning (0 to
  2% measured), after a replica fault they win (96% and more).

With the policy on, a failed attempt is retried on the next replica after
10ms instead of the default backoff: the failed replica is excluded for the
request, there is nothing to wait for.

## Overload

The three rules route around a replica that is slow or down. They do nothing
for a request that already waits in a QueryNode read queue, and under load
above what the replicas get done every request waits there: the queue is
limited by task count (`queryNode.scheduler.unsolvedQueueSize`, 1024), which
says nothing about time, so the whole excess becomes queue time and every
request gets slow instead of some failing fast. Four changes bound the
latency of the requests that are answered and make the others fail fast.

**QueryNode queue wait limit.** `queryNode.scheduler.maxQueueWaitRatio`
(default `0`, off) limits how long a read task may wait in the queue:

```
limit = min(maxQueueWait, maxQueueWaitRatio * time left until the deadline)
```

`queryNode.scheduler.maxQueueWait` defaults to `1s` and is also the limit of a
task without a deadline; the intended ratio is `0.1`. A new task is judged by
how long the task that leaves the queue next has waited so far: when that is
more than half of the new task's limit, the new task is refused at once with
`ErrServiceTooManyRequests`, as a full queue refuses it. This is a measured
time. An estimate from the queue length and the pace of the workers was tried
first and dropped: both numbers are noisy (canceled tasks still count, search
tasks are merged, workers run in bursts under a CPU quota), their errors
multiply, and a pace that was once judged too slow let no second task into
the queue to correct it. Half of the limit rather than all of it: with the
whole limit the head of the queue would wait for about the limit all the
time, and nearly every task would be let in only to be refused when it
leaves the queue. A task whose deadline has passed fails as expired, not as
refused. A task that
waited longer than its limit after all is refused when it leaves the queue,
so the wait of an executed task is bounded. Merged search tasks are judged by
the first task of the group. Refusing starts only once tasks have waited: the
tasks let in during the first moments of a burst are covered by the check on
leaving the queue.

**One extra attempt, then the refusal goes to the client.** A search or query
gets one extra attempt on another replica, and one only, whatever the number
of replicas. It is used by whichever comes first:

- the first replica refuses the attempt as overloaded: the extra attempt
  takes its place at once, not after the retry interval of the policy (200ms
  and growing by default, which also gives up when less than that is left of
  the request), and without waiting for the hedge delay;
- the hedge delay passes (rule 3): the extra attempt is the hedge.

When the extra attempt is refused as overloaded too, the request ends with
`ErrServiceTooManyRequests`. It is not tried on a third replica, and the shard
leaders are not refreshed to go around again: that would add to the queues
that refused it. The server does not retry; the client may. A refused hedge
is dropped while the first attempt is still running, and the request ends
only when that one is refused as well. With a single replica the first
refusal ends the request.

A refusal is not recorded as a failure of the replica (see the table in Rule
2). A replica that fails otherwise, because it is down or the connection
breaks, does not use up the extra attempt: the request moves on to the other
replicas as before. This holds under every replica selection policy and at
every consistency level. A read at Strong consistency waits on each replica
until the replica has consumed the WAL up to the request's timestamp, so an
extra attempt on another replica returns the same data; it is only later
there when that replica reads the WAL over the network.

With a 2s request timeout and `maxQueueWaitRatio` 0.1, a request waits about
200ms in a queue at most, so it is answered or refused after about 200ms plus
the execution on the first replica, and after about 400ms on the second.

**No resending inside the RPC client.** `ErrServiceTooManyRequests` is a
retriable error, and the gRPC client of a component sends a request again on
a retriable status, up to 10 times with backoff. For a refused read that
keeps the request in the client until its deadline, while the work done for
its other shards is finished for nothing. `grpcclient.WithFailFastOnOverload`
makes a call return the refusal instead; the Proxy sets it on every read, the
delegator on reads of its workers.

**A failed shard stops the others.** A request over several shards fails with
its first failed shard. `LBPolicyImpl.Execute` now cancels the attempts on
the other shards then; they were finished before, for a result nobody got.
This applies to every replica selection policy.

The client gets `ErrServiceTooManyRequests` (code 4). pymilvus does not retry
it by itself; it retries the rate limit code (8) only.

## Configuration

All under `proxy.latencyWeighted.`, all refreshable.

| Key | Default | Meaning |
|---|---|---|
| `enabled` | true | switch; off behaves as `look_aside` |
| `decay` | 10s | time constant of the latency mean and of the penalty |
| `minShare` | 0.001 | minimum share of every replica |
| `hedge.enabled` | true | hedged attempts |
| `hedge.minDelay` | 200ms | shortest hedge delay |
| `hedge.minWinRate` | 0.3 | when fewer than this share of the recent hedges won, only 10% of the hedges are sent |

Constants in the code, to be made configurable when a need shows: failures in
a row before the penalty (3), timeout assumed without deadline (60s), hedge
quantile (p99) with its window (10s) and samples (100 of at most 1024), hedge
outcomes for the win rate (128, at least 10 within 10s), share of hedges still
sent below the minimum win rate (10%), retry interval (10ms).

Metric: `milvus_proxy_replica_hedge_total{decision}` with `sent`, `won`,
`skipped_low_win_rate`, `skipped_no_replica`.

For overload, on the QueryNode, both refreshable:

| Key | Default | Meaning |
|---|---|---|
| `queryNode.scheduler.maxQueueWaitRatio` | 0 | share of the time a read task has left that it may wait in the queue; 0 turns the limit off |
| `queryNode.scheduler.maxQueueWait` | 1s | longest wait whatever the deadline, and the wait of a task without a deadline |

Metric: `milvus_querynode_read_task_queue_duration{outcome="overloaded"}`
counts the refused tasks.

## Measurements

Test cluster: one container per node (2 streaming nodes, 4 query nodes limited
to 2 CPUs each, 1 proxy), collection with 2 replicas and 2 shards, 180k rows.
"Power loss" drops all packets of a node's container and freezes it; nothing
is really powered off. Each point was run once or twice: indications, not
statistics. Unless noted, the numbers are of this version of the code.

Query node power loss, Bounded reads of the two replica collection:

| | Failures | Longest search | Searches over 150ms in the first 15s | Searches per 5s after the fault (before) |
|---|---|---|---|---|
| run 1 | 0 | 0.23s | 2.1% | 363 / 382 / 366 (398) |
| run 2 (cluster already slower: 3.2% over 150ms before) | 0 | 0.58s | 7.8% | 212 / 223 / 219 (221) |

Queries, at 4 requests/s: no failures, longest 0.23s and 0.30s, about 12%
over 150ms in the first 15s. At a low rate three requests must be overtaken
before the replica is penalized.

Streaming node power loss (the node also hosts one replica's shard leaders):
Bounded searches and queries 0 failures, longest 0.24s and 0.23s, searches
per 5s 373 / 382 / 399 against 399. Strong reads and writes fail for 17 to
20s until the WAL is taken over; that is outside replica selection.

Overload (open loop, capacity about 80 requests/s):

| Requests/s | p50 / p99 | Hedges sent / held back / won |
|---|---|---|
| 80 | 81ms / 190ms and 65ms / 179ms | 3 / 0 / 0 and 2 / 0 / 0 |
| 120 | 5.5s / 9.1s | 210 / 1614 / 1 |

For reference, from earlier versions of this code under the same conditions:

- hedging off at 80 and 120 requests/s: 55ms / 179ms and 5.3s / 10.1s, so
  hedging does not add to an overload;
- query node power loss with hedging off: no failures, longest search 4.5s,
  searches per 5s 48 / 368 / 294 / 356;
- `look_aside`, without CPU limits: searches per 5s 19 / 7 / 9 / 10;
- a heavy LIKE of about 0.6s every 2s among light reads of its class: the
  replicas kept their shares whether the heavy requests completed or timed
  out. With a query node power loss on top: no failures, longest light request
  about 1s, because the heavy requests raise the p99 the hedge delay comes
  from.

## Known issues, limits and what was not tested

Known issue, to fix before this is used:

- `searchShard` and `queryShard` invalidate the shard leader cache of the
  collection on any error of the query node call, a canceled one included. The
  loser of a hedge is canceled when its request returns, so every hedge that
  wins invalidates the cache, and the next request refetches the shard
  leaders from the coordinator. Found by reading the code, not yet measured.

Limits:

- The queue wait limit covers the wait in the scheduler queue only. A query
  node that runs more read tasks at once than it has CPUs for
  (`maxReadConcurrentRatio` above 1, or a CPU limit it does not see) does
  not queue them: they wait while they run, where nothing bounds it.
- The queue wait limit follows the deadline, so on one query node requests
  with a long timeout outlast requests with a short one: under overload the
  short ones are refused first. `maxQueueWait` bounds how far this goes.
- A request that needs several query nodes or shards is admitted by each of
  them on its own. One refusal ends the attempt on that replica; the tasks
  already queued for it elsewhere are canceled, but one that has started is
  finished for nothing.
- The handling of a refusal changes what a full queue
  (`unsolvedQueueSize`) does under the default policy too: the read moves to
  the next replica instead of being sent again to the same one by the RPC
  client.
- A refusal leaves no trace in the statistics of the replica. A replica that
  refuses most attempts returns fast and has few attempts in flight, so by
  rule 1 it looks idle and draws first attempts, each of which is refused and
  moved. Not measured; recording a refusal as a slow sample would close it.
- A replica that is down keeps a share that grows with the latency of the
  others: its penalty is the attempt timeout, and replicas that queue come
  closer to it. Measured with two replicas under overload: 15 to 30% of the
  first attempts still went to the replica that was down, and waited for the
  request deadline when the hedge was held back.
- With hedging off, or held back by the win rate, a replica that does not
  answer is only noticed at the request deadline.
- At a low request rate about a tenth of the requests wait for a hedge
  (200ms) in the seconds after a fault, until three were overtaken.
- A replica that stalls briefly may be overtaken three times in a row and
  then stays near the minimum share for about a minute.
- Heavy and light requests of one class share a p99 and a win rate: the heavy
  ones raise the hedge delay of the light ones, and are themselves hedged
  (their second attempt is canceled when the request returns). A class made
  of heavy requests only has no light successes to reset its failures.
- Only search and query tasks (query iterators and the requery of a search
  included) are hedged and carry a request class. The other users of the load
  balancer (delete by expression, statistics, the `ExecuteOneChannel`
  callers) get the latency weighted selection without hedging, with
  statistics keyed by channel only.
- Each proxy learns on its own.

Not tested: more than two replicas; several proxies; production workloads;
errors during rolling upgrade and balance, which the three-in-a-row rule is
meant for; heavy requests of several seconds timing out (no such query could
be built on the test data; LIKE patterns with eight or more wildcards failed
with a segcore error instead); the unit tests under the race detector.

Master: since the QueryView rewrite (#52977) every replica's leader of a
channel is the WAL owner streaming node, so a streaming node fault is not
covered by switching replicas there. 3.0 and master also have the 30s replica
blacklist (#46091, with #55051 not blacklisting on cancel or timeout); the
two mechanisms must be reconciled when porting.
