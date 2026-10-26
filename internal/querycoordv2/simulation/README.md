# QueryCoord balance simulator

This package runs the **real** QueryCoord placement machinery (checkers, task
scheduler and executor, distribution pulls, target/collection/replica/resource
observers, balancers and assign policies) against **simulated** QueryNodes and
an in-memory DataCoord, inside one test process.

- `broker.go`: stateful DataCoord/RootCoord (`meta.Broker`). Collections,
  vchannels and flushed segments. `Flush` models handoff; `Compact` merges the
  smallest segments of a channel, never past `MaxSegmentRows` (the case's
  `max_segment_rows`, default 2,097,152 rows = 1GB at 512 bytes a row, as
  Milvus's max segment size; `-1` lifts the cap). Without the cap a replayed
  day merges its segments into a few 8–21GB ones that no node can admit.
- `fleet.go`: simulated QueryNodes (`session.Cluster`). Each node has a load
  pool (five loads per core), load latency fitted to production (October
  2026, 2.6.24+ nodes with local NVMe: a load gets min(400 MiB/s, 45 MiB/s x
  cores / loads in flight), re-evaluated every 250 ms as loads start and
  finish so the node's bandwidth is shared dynamically, with an optional
  concurrency penalty that shrinks the aggregate as loads pile up past the
  cores, plus a 0.3 s floor and log-normal jitter 0.45 on the bytes; the
  older per-slot model and a reference-latency model remain available), memory
  capacity (loads past the overload threshold are refused, as a QueryNode
  does; `force_load` admits up to the force threshold) and delegators with
  leader views. Nodes can be killed, restarted or slowed down. `FleetStats`
  counts ground-truth work: loads, releases, watches, loads to stopping nodes,
  refusals and peak concurrent loads.
- `streaming.go`: the streaming-mode fleet (StreamingNodes own the delegators).
- `harness.go`: wires one QueryCoord incarnation over a persistent KV and adds,
  kills or drains nodes. It also restarts QueryCoord, loads and releases
  collections, creates resource groups and applies `MILVUS_SIM_PARAMS`
  (`key=value,key=value`) to the paramtable before QueryCoord starts.
- `movement.go`: ingest and compaction drivers (rows per second per channel,
  flush period, ingest-rate changes over the day).
- `metrics.go`: samples QueryNode ground truth on a period. It reports missing
  segments, leaderless channels, copies per replica, node fill spread, table
  spread (each table's largest node share over its fair share, in segments and
  bytes), scheduler backlog and load counters, and writes JSON report lines.
- `trace.go`: with `MILVUS_SIM_TRACE=<file>` every task, node sample, event
  and keyframe is written for the tools below.

Time is real but scaled: `ScaledParams` shortens QueryCoord intervals about
tenfold, and simulated loads take milliseconds. Scenarios that take minutes in
production finish in seconds; a replayed day takes 20–40 minutes. Results are
statistical rather than bit-reproducible: compare totals over many cases, or
averages over several runs, not single cells.

## Running

The package is built with `-tags dynamic,test` and needs the C++ core of the
same checkout (`make build-cpp`). Build it with `-gcflags="all=-N -l"`: the
tests patch functions with mockey, which needs inlining off. The smoke test
always runs. Everything else is opt-in because each run takes tens of seconds
to tens of minutes.

Scenario tests (`scenarios_*_test.go`):

```bash
MILVUS_SIM=1 MILVUS_SIM_BRANCH=master MILVUS_SIM_REPORT=/tmp/master.jsonl \
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 -timeout 60m \
  ./internal/querycoordv2/simulation/ -run 'TestSimP0'
```

Cases (`testdata/cases/*.json`, run by `TestSimCases` in `case_runner_test.go`):

```bash
MILVUS_SIM=1 MILVUS_SIM_BRANCH=v1 MILVUS_SIM_CASE=day_13_typical_saas \
MILVUS_SIM_PARAMS="queryCoord.balancer=TwoLevelBalancer" \
MILVUS_SIM_REPORT=/tmp/v1.jsonl MILVUS_SIM_TRACE=/tmp/v1_day_13.json \
  go test -tags dynamic,test -gcflags="all=-N -l" -count=1 -timeout 60m \
  ./internal/querycoordv2/simulation/ -run 'TestSimCases$'
```

`MILVUS_SIM_CASES=a,b,c` runs several; `MILVUS_SIM_SCALE=4` multiplies the
cluster sizes of the scenario tests (nodes, collections, segments);
`MILVUS_SIM_DEBUG_RECOVERY=1` logs what is missing during a recovery.

To compare two report files (repeated runs are averaged):

```bash
MILVUS_SIM_COMPARE=/tmp/master.jsonl,/tmp/pr.jsonl \
  go test -tags dynamic,test ./internal/querycoordv2/simulation/ -run TestSimCompareReports -v
```

To compare balancers, run the same binary twice with different
`MILVUS_SIM_PARAMS` (the balancer is a config key), never two binaries: the
harness and the metrics must be identical on both sides.

## Cases

A case is one cluster shape living through a timeline. Fields: `qns`, `sns`,
`qn_cores`, `qn_memory_gb`, `streaming`, `resource_groups`, the `load` model
(pool concurrency, node bandwidth, single-load bandwidth, base latency,
jitter, and `concurrency_penalty`: how much the node's aggregate throughput
collapses as loads pile up past its cores, 0 = constant), the `search` model,
`collections` (count, channels, segments per channel, rows median and sigma,
replicas, resource group, ingest rate) and `events`, each at a simulated time
with one op: `kill_qn`, `restart_qn`, `add_qn`, `stop_qn`, `slow_qn`,
`rolling_replace`, `restart_qc`, `ingest_rate`, `load_tables`,
`release_tables`, `reload_tables`, `kill_sn`, `add_sn`, `pin_rg` (fix a
resource group's requested node count so a growing group takes only joining
nodes) and `transfer_rg` (move the first table's replicas to resource group
`rg`, as a transfer-replica request does: the old nodes serve read-only until
the new ones hold the segments). A blue-green upgrade is `pin_rg`, `add_qn`
with `rg`, `transfer_rg`, then `stop_qn` on the old nodes. `time_scale`
compresses the timeline (6: a day replays in about 24 minutes).

- `day_*`: synthetic days generated by `testdata/gen_days.py`. One shape each
  (a 48-node cluster with many tables, 5,000 small tables, skewed tables, few
  big tables, giant segments, coarse and fine segments, mixed node sizes, two
  resource groups, multiple replicas, hot ingest, high memory pressure,
  streaming mode, a typical SaaS mix, 65k and 1M small tables, and three
  large-segment shapes at 4GB, mixed and 8GB), all through the same 24 hours:
  a nightly bulk insert, a crash, a scale-out for the daytime peak, new
  tables, releases and reloads, a rolling upgrade, a QueryCoord restart and a
  scale-in at night.
- `prod_6_modash_*`: a 2.6 to 3.0 upgrade replayed from an incident (aws-eu,
  2026-10-22): the new QueryCoord cold-loads two tables (~300 segments,
  ~90 GiB) onto six empty nodes while the old nodes keep serving; the old
  scheduler dispatched every segment at once and most loads were cut at the
  5-minute timeout. Two load-model variants (concurrency penalty 1.5 and
  2.5).
- `prod_*`: production clusters replayed from their metrics (node count and
  memory, collection and segment shapes, flush and compaction rates),
  including serverless clusters with 2,000–3,800 collections. `source` says
  where the numbers came from.
- `legacy_*`: the same clusters under the pre-2.6 layout, to measure an
  upgrade.

## Scenario tests

| file | what |
|---|---|
| `scenarios_p0_test.go` | the P0 list: many collections, one huge collection, balance under ingestion, ingestion and compaction, single and multi-node crash, QueryCoord restart during moves and during recovery, crash and drain during balance, scale out, scale in, rolling upgrade, resource-group isolation |
| `scenarios_master_test.go` | S1–S10: streaming load, WAL migration, StreamingNode crash and scale-out, slow-load scale-out (streaming and classic), straggler recovery, skewed segments, ingestion during scale-out, restart during slow moves |
| `scenarios_queue_test.go` | S11–S13: recovery backlog and timeouts, steady-state churn, an oversized segment that never loads |
| `scenarios_memory_test.go` | S14a–c, S15: wide rows, capacity shortfall, fragmentation, tiny collections beside a big one |
| `scenarios_evenness_test.go` | S16a–g: channel groups, uneven channel data, bytes-per-row variance, few large segments, mixed node sizes, many small tables |
| `scenarios_tables_test.go` | TB1–TB8: table spread, including a legacy layout upgraded to the new balancer, two resource groups and a 29-node, 27-table replay |
| `scenarios_robust_test.go` | RB1–RB5: restart mid-balance, load failures, node flapping, sequential drains, near-full nodes |
| `scenarios_repro_test.go` | reproductions of production incidents |
| `scenarios_v2_test.go` | V1–V2: replacing a node and scaling out with small tables only |

## Invariants (hard failures)

- At most two live read-write copies of a segment within one replica (two
  means a Move is in progress). Copies on a draining node still count for
  availability but not for this rule.
- No load is sent to a stopping node.
- Every scenario settles: nothing missing, every channel has a leader, and the
  scheduler is idle.

## Metrics

| metric | meaning |
|---|---|
| `load_seconds`, `initial_load_seconds` | wall time until every collection is Loaded |
| `recovery_seconds` | time from the failure until no segment is missing and every channel has a leader |
| `recovery_steady_seconds_*`, `worst_recovery_steady_seconds` | time until missing segments and leaderless channels are back to the level of the minute before the disruption (the absolute zero is never reached under ingestion, so the raw recovery time is a metric artifact) |
| `settle_seconds` | time until settled, held for at least one second |
| `balance_seconds` | time until row CV ≤ 0.15 and settled |
| `drain_seconds` | time for a stopping node to be emptied |
| `missing_segment_samples` | sum over samples of (replica, segment) pairs with no live copy, i.e. unavailability |
| `leaderless_channel_samples` | sum over samples of (replica, channel) pairs without a watched delegator |
| `loads_succeeded`, `releases`, `delta_loads_*` | ground-truth data movement in the measured phase: succeeded, refused, cancelled, to stopping nodes, queue waits |
| `loads_between_old_nodes` | after scale-out, loads that did not target the new node (needless shuffling) |
| `max_node_loading` | peak concurrent loads on one node |
| `final_row_cv`, `final_segment_cv`, node fill min/max over mean | imbalance at the end of the run |
| `table_max_count_over_fair_p90`, `tables_unbalanced_frac`, `tables_on_1_node` | table spread: a table's largest node share over its fair share (p90 over tables), the share of tables off their share, tables sitting on one node |

Ping-pong, reported by the tools from a trace: a segment copy that balance
moved off a node and later loaded back onto that node. A user's release and
reload, or a node restart bringing its segments back, does not count.

## Tools (`tools/`)

- `simviz.py`: one static HTML page per trace set, with a shared time axis,
  per-node fill lanes, task and event streams and a keyframe slider.
- `simgallery.py`: a gallery over traces named `<label>_<case>.json`, one row
  per case with the three metric groups in the order the balancer is judged:
  memory (node fill min/max over mean), table distribution (p90 and worst
  share), churn (moves, ping-pong). Labels starting with `m` are the
  production balancer, with `v` the two-level one.
- `simstory.py`: a storyline page per case, production balancer and two-level
  side by side as the day unfolds (fill band, tables off share, moves per bin
  with ping-pong in red, under the event band).
- `simchurn.py`: where a trace's churn comes from: moves by phase and reason,
  reversals by pair of reasons, ping-pong by the reason that caused the load.

## Fidelity notes

- A delegator reports its distribution only once it is serviceable; before
  that the node looks empty to QueryCoord, as in production.
- Ingest never exceeds the load bandwidth of the case, or every balancer
  drowns in loads and the comparison measures the bandwidth.
- Compaction is capped by `max_segment_rows` (see above). Replays run before
  the cap exist in old reports; do not compare across it.
- Timing metrics (initial load, recovery) depend on the host's load: run the
  sides of a comparison on the same host at the same time.
