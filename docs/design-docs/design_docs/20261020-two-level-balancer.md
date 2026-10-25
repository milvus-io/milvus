# QueryCoord Two-Level Balancer

## Overview

The two-level balancer replaces the score-based segment balancer of QueryCoord
with a planner built on a single potential function. Memory fill per QueryNode
comes first, then each collection's share of segments and bytes across the
nodes of its replica, and every move must lower the potential strictly, so
balance converges and then stops. The same share logic drives placement of new
segments, and a load pacer, adaptive task timeouts and a force-load escape
hatch keep loading from stalling under memory pressure.

It is selected with `queryCoord.balancer=TwoLevelBalancer`; placement follows
the balancer automatically. Everything else is unchanged.

## Motivation

Seven days of production QueryCoord data (October 2026) and a replay simulator
showed four recurring problems with the score-based balancer:

- Churn. The score has no notion of "good enough": it keeps moving segments as
  long as any node scores lower, and the move it makes is often undone by the
  next (ping-pong). One five-node cluster averaged 2,349 moves per hour.
- Tables piled on a few nodes. The score is per node, so a collection can sit
  entirely on one or two nodes while the nodes are perfectly even. On an
  eight-node cluster with 1,580 collections, 23% of them had a node holding
  more than twice their fair share.
- Memory left unbalanced when segments are large. A move that would make the
  receiver fuller than the giver is refused outright, so two nodes holding one
  and two giant segments never trade toward the best split.
- Loads stall. Under memory pressure every node refuses loads just under its
  overload threshold, and the scheduler retries the same node until the task
  times out; a slow node is sent the same concurrency as a fast one.

The priority the balancer serves is fixed: node memory fill, then each table's
segment count per node, then each table's bytes per node, then as few moves as
possible.

## Design

### The potential

The cluster's potential is a tuple compared lexicographically:

1. Node term: for every node, how far its memory fill is outside the band
   around its resource group's mean fill, in bands. The band is
   `max(nodeTolerance × mean, 0.1% of capacity)`, with `nodeTolerance` 0.10,
   tightened to 0.05 (`highWaterTolerance`) once the mean is above
   `highWater` 0.8. A node's term is `max(0, |fill − mean| − band/2) / band`.
2. Table count term: for every (collection, replica), the segments each node
   holds outside the fair share ± `max(1, ceil(0.15 × fair))`.
3. Table bytes term: likewise for bytes, with a tolerance of
   `max(0.15 × fair bytes, the table's largest segment)`.

A change of less than a quantum in a term (1% of a band, half a segment, 1% of
a byte tolerance) does not count, so a move must close a visible part of a
band or a tolerance to be worth a task.

Actions are a move of one segment, an exchange within a table (its larger
segment out, a smaller one in: bytes move, counts stay) or a swap across
tables. Among actions that lower the potential, the one that lowers it most
wins; ties go to fewer bytes moved. Because every applied action lowers one
well-ordered potential, no action undoes another: in a static cluster balance
converges and stops.

### A round

Each round plans against a private snapshot of the cluster view (the global
state lock is held only to build the view and to record the plans at the end)
and applies each plan to that view before choosing the next, so one round can
plan many moves consistently.

1. Node leveling, per resource group. While some node is outside its band,
   the four fullest nodes above the mean offer segments (the largest
   `levelCandidates`, default 1000) to the four emptiest below it. The best
   move or swap by potential is applied, repeatedly, until none lowers it.
   Candidates are ranked first by what they do to tables (bring a table to its
   share, leave every table at its share, push a table off its share) and only
   then by potential, so leveling helps tables when it can. A node outside its
   band may hurt tables: memory comes first.
2. Table balance. Collections are visited round-robin (`tablesPerRound`),
   Loaded ones only. For each (collection, replica) off its share, the best
   move, exchange or swap is applied. A table action may not raise the node
   term by any amount, not even within the quantum: a quantum of leak per move
   would let tables walk a node out of its band one small segment at a time
   for leveling to walk it back. A swap's partner table must stay as good as
   it was.
3. Room for blocked nodes. A table action rejected because it would push a
   node out of its band marks that node as blocked: parked at the edge, it
   blocks every table on it, and leveling will not touch it because it is
   inside the band. The round then moves each blocked node back into the
   inner half of its band (`band/4`), with moves that keep every node inside
   its band and every table at its share. The inner band is a second, lower
   potential term, used only here; a move must lower it by a quantum, so nodes
   already inside the inner band are left alone.

Leveling and table balance see one and the same node term. An earlier version
gave leveling a tighter band than tables; with two bands there were two
potentials, and they cycled (a 4GB-segment case produced 1,540 ping-pong moves
a day, the production balancer 13). Two things removed that: one band for
both, and the no-leak rule above.

### Hard constraints on every action

- The target must have room in its load budget (the pacer's bytes and load
  slots, see below), so a move starts at once and recovery and new tables keep
  bandwidth. Recovery itself is never limited.
- The target's peak memory, counting what it is still receiving, may not pass
  the QueryNode's reject line (`queryNode.overloadedMemoryThresholdPercentage`).
- A segment moved within `cooldownSeconds` (600) is not moved again; one a
  task is acting on is not touched. The ledger of recent moves survives a
  QueryCoord restart only as far as the task scheduler does: fresh loads do
  not cool, moves do.
- Only Loaded collections are balanced. Collections still loading neither
  count toward shares nor move.
- Small tables, with fewer than `smallTableSegmentsPerNode` (2) segments per
  node of their replica on average, are not table-balanced, and moving their
  segments never counts as hurting a table. Placement remains table-aware for
  them. This keeps a million-table cluster at 7–12 seconds per round (4 GB of
  heap) with no loss on the table metrics.

### Placement

New segments (a load, a recovery, a compaction result) are placed by the same
share logic. Segments are sorted largest first and assigned in tiers: first to
nodes with a free load slot (in-flight loads below the load budget), ranked by
fill, then to the node whose queue, measured in rounds of its own budget, is
shortest. Fill ties within 0.5% of capacity are broken by segment count. When
the node a table is shortest on has its budget in flight, the table waits for
it rather than filling a node that is nearly at its share.

### Loading: budget, pacer, timeouts, force load

- Pacer (`loadPacingEnabled`, on by default): per node, admission control on
  the bytes of segment data in flight. A QueryNode loads at a bandwidth shared
  by whatever is in flight, so the pacer keeps the bytes in flight to what the
  node drains within a quarter of the task timeout at its believed bandwidth,
  and the loads in flight to one per core. The bandwidth starts at
  `loadBandwidthPerCoreMBps` (32 MiB/s per core: a 16-core node 512 MiB/s,
  7.5 GiB in flight under the 60 s timeout) and adapts: a window of loads
  that took more than `loadPacingSlowFraction` (0.5) of their timeout on a
  saturated node halves it, a window of fast ones adds one core's worth,
  between an eighth and eight times the start. An idle node always takes one
  load. Loads beyond the budget wait in QueryCoord instead of queueing on the
  node and are dispatched the moment a slot frees, so a slow node gets fewer
  bytes and a fast one more, and canceled loads under the production
  balancer drop from tens of thousands a day to tens.
- Task timeouts: a segment task's first attempt has `segmentTaskTimeout`
  (1 min, down from 5: with the pacer a load does not queue on the node, so
  one that does not finish in a minute is unusually slow). A load that ends on
  its deadline is retried with `segmentTaskRetryTimeout` (5 min) until it
  succeeds, which resets the segment. Channel watches follow the same ladder
  (`channelTaskTimeout` 1 min, `channelTaskRetryTimeout` 5 min, per channel
  and node).
- Load budget: the pacer's budget (bytes and load slots) is also the node's
  load budget for placement and balance. Placement prefers nodes the pacer
  would admit the load to now; balance only sends a move to such a node, so a
  move starts at once rather than queueing behind other loads, and the moves
  waiting on a node are bounded by its budget. Loads for new tables and
  recovery are not limited: they queue in the pacer.
- Force load: after `queryCoord.forceLoadAfterRejections` (3) consecutive
  memory refusals of a segment, the next load request carries `force_load`,
  and the QueryNode admits it up to
  `queryNode.forceLoadMemoryThresholdPercentage` (95) instead of the overload
  threshold. Balance then moves memory off that node. This breaks the stall
  where every node sits just under the threshold and every load is refused.

### Metrics

QueryCoord exposes, per round: planned moves by reason (`level`, `level_swap`,
`level_room`, `table_move`, `table_exchange`, `table_swap`), rejected actions
by cause, planning latency, each node's deviation from its group mean in bands,
segments and in-flight loads per node, tables off their share, the pacer's cap
and waits, memory refusals and force loads, and placements by pass.

### Configuration

Balancer keys are under `queryCoord.twoLevel.`, defaults in parentheses:
`nodeTolerance` (0.10), `highWater` (0.8), `highWaterTolerance` (0.05),
`tableSegmentCountTolerance` (0.15), `tableByteTolerance` (0.15),
`smallTableSegmentsPerNode` (2, 0 = off), `levelCandidates` (1000, 0 = all),
`ceilingRatio` (0.5), `swapCandidates` (32), `tablesPerRound` (20000),
`cooldownSeconds` (600), `ledgerTTLSeconds` (600), `viewTTLMillis` (500).
Loading keys are under `queryCoord.`: `loadPacingEnabled` (true),
`loadBandwidthPerCoreMBps` (32), `loadPacingSlowFraction` (0.5),
`segmentTaskTimeout` (60000 ms), `segmentTaskRetryTimeout` (300000 ms),
`channelTaskTimeout` (60000 ms), `channelTaskRetryTimeout` (300000 ms),
`forceLoadAfterRejections` (3); and `queryNode.forceLoadMemoryThresholdPercentage`
(95).

### Compatibility

The balancer is opt-in by `queryCoord.balancer`. `SegmentAssignPlan` gains a
`Reason` string used for task reasons and metrics. `LoadSegmentsRequest` gains
`force_load` (field 15); an old QueryNode ignores it. The pacer and the force
load path are independent of the balancer choice: the pacer is on by default
under either balancer, force load triggers only after repeated refusals.

## Alternatives considered

- Overshoot rule (the receiver may not end fuller than the giver). Removed:
  strict potential decrease already prevents oscillation, and the rule
  blocked the giant-segment cases (a 1.61 max/mean cluster went to 1.03
  without it).
- Leveling to an inner band. Produced the two-potential cycle described above.
- Two-step chains for big segments (move y off target B so that x fits next
  round). Implemented and measured on eight cases: triggered 0–15 times a day,
  no measurable gain. Large uniform segments are a quantization floor (an 8GB
  segment on a 32GB node is 25% of it; no planner can do better than ±1
  segment), and the lever is the segment size itself, not the balancer.
  Removed.
- Room moves sized to the blocked action (the smallest sufficient segment) or
  to the largest segment that does not overshoot the mean. The first starved
  table balance (segment p90 1.48 against 1.29), the second moved giant
  segments 1,487 times a day for nothing. The inner-band term gives the same
  room as the removed inner-band leveling without its cycle.

## Validation

The simulator in `internal/querycoordv2/simulation` runs the real QueryCoord
against simulated QueryNodes (see its README; node load speed is fitted to
production, October 2026, and shared dynamically among the loads in flight
on a node). 46 cases were run under the production balancer and the
two-level balancer, both with the load pacer and the 60 s / 5 min task
timeouts of the companion PRs: 17 synthetic days (48-node clusters, 5,000
small tables, giant segments, mixed node sizes, two resource groups, multiple
replicas, hot ingest, high memory, streaming mode, large segments, a cold
start of 6,000 small segments), 26 replays of production clusters including
three serverless ones with 2,000–3,800 collections, and three variants of a
cold load replayed from an upgrade incident (two tables of ~300 segments
onto six empty nodes). Every run passes the simulator's invariants (a
segment never has more than two live copies in a replica; the initial load
settles). Over the 46 cases:

| | production balancer | two-level |
|---|---|---|
| moves per day, all cases | 129,395 | 111,898 |
| ping-pong moves | 30,365 | 11,661 |
| canceled loads | 41 | 24 |
| cases with table segment p90 > 1.5 × fair | 19 | 7 |
| cases with table bytes p90 > 1.5 × fair | 15 | 15 |

On the 26 production replays the two-level balancer moves about a quarter as
much (10,668 against 2,592 moves a day in total) and its ping-pong count is
near zero (1,808 against 123; the serverless replay 604dfc45: 1,349 moves and
124 ping-pongs a day against 45 and 5). Drains for scale-in and rolling
upgrades finish sooner (the typical SaaS day: 26 s against 68 s; the 48-node
day: 45 s against 69 s; skewed tables: 9 s against 25 s). Recovery after a
node failure or a QueryCoord restart settles in under a second on most days
for both balancers; on the two largest days the two-level balancer is slower
to settle (9 s against 4 s on the 48-node day, 6 s against 1 s on the SaaS
day), because it fills the recovering table's share on the nodes it picks
rather than scattering the segments. Table bytes p90 is the one metric not
better: the byte tolerance admits the table's largest segment, so a table of
few large segments is called balanced at 1.5 × fair.

On the incident replay (the old scheduler dispatched ~300 segments at once
onto six 8-core nodes and most loads were cut at the 5-minute timeout), the
pacer alone brings the full copy from 6–13 minutes with 77–354 canceled
loads down to 2.5–3 minutes with at most 4, and the two-level placement to
2–2.3 minutes with 4 GiB less peak memory per node; the result does not
depend on how badly the node's throughput collapses under concurrency,
because the pacer never lets it get there.

Unit tests cover the planner (`balance/two_level_balancer_test.go`,
`assign/placement_policy_test.go`), scale (65,536 and 1,000,000 tables in
`balance/two_level_scale_test.go`) and the checker wiring; the `balance` and
`assign` packages pass with the race detector.

## Future work

- Band floor by segment size: report a node within half the largest segment
  of the mean as balanced, so leveling does not keep trying at the
  quantization floor.
- Cap compaction output relative to QueryNode memory in DataCoord, which is
  where the large-segment cases come from.
- Tiered storage: the fill the balancer reads is loaded bytes; with eviction
  the reject line and the budget should come from the node's headroom instead.
