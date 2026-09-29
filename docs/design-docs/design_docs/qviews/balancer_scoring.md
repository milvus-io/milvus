# Incremental Score-Based Balancing

**Status: selected design direction; not implemented.** The implementation
baseline is documented in [Balancer Design](balancer_design.md). This document
defines the next scoring, target-accounting, and candidate-acceptance policy.
Existing configuration values still control the baseline implementation; new
weights, tolerances, and budgets require calibration before defaults are chosen.

Related contracts: [Balancer Cache](balancer_cache.md),
[Replica Placement](replica_placement.md),
[Shard View Management](shard_view_management.md),
[QueryViewHandler](query_view_handler.md), and [DataView](data_view.md).

## 1. Scope and Objectives

Retain weighted scoring, greedy candidate selection, Must-before-optional batch
planning, shared projected loads, immutable cache reads, and QueryView lifecycle
execution. Improve three distinct placement objectives:

1. Balance intended node rows within each ResourceGroup (RG).
2. Keep small shard replicas on few nodes.
3. Spread large shard replicas, and distribute a large collection replica even
   when it consists of many small shards.

RowNum remains the load metric and nodes are assumed homogeneous. Balanced rows
do not imply balanced bytes, memory capacity, CPU, or query traffic. RG membership
is owned upstream; the policy does not move data across an RG boundary to fix
skew. A node reassignment invalidates both affected RGs.

Preserve collection/RG replica node quotas, stable target ownership, eventual
physical isolation, shortage suspension, and last-serving-cover protection.
QueryNode replicas remain the scope. StreamingNode replica placement, production
runtime wiring, new RPCs, AnchorHash, and persistent Segment Groups are excluded.

## 2. Why the Baseline Changes

The baseline rebuilds each shard from an empty candidate and scores nodes with
bounded stickiness, node-load, and fanout components. Its equal default weights
can prevent a large reusable segment from moving even to an empty new node.
FanoutBudget permits spreading but does not measure shard row concentration.
Optional assignment inequality is the only final emission test; it does not
measure total improvement or bound movement. Up/Pending accounting also mixes
the old serving placement with the next placement during preparation.

The replacement scores the **change in a placement objective**, starts from
existing legal assignments, and separates intended load from resource residency.

## 3. Placement Objective

All penalty terms below have units of rows; their weights are dimensionless.
They are not independently clipped to [0, 1]. Compute before/after values using
the same pinned configuration, demand, topology, and normalization parameters.
Earlier accepted candidates update the batch's projected placement.

### 3.1 ResourceGroup Balance

For an RG with N eligible nodes:

```text
D     = total desired rows of active replicas in this RG
mu    = D / N
delta = max(absoluteToleranceRows, relativeTolerance * mu)
lo    = max(0, mu - delta)
hi    = mu + delta

distance(x) = max(lo - x, 0, x - hi)
G(P) = sum_n distance(L[n])^2 / (2 * mu)
```

L[n] is intended steady-state logical load, not current physical occupancy.
Demand includes required but unplaced data. Failed preparation must not erase
desired demand. Released or suspended replicas add no new desired demand;
views retained solely for serving handover remain resource/serving facts.
For identical replica requirements in one collection/RG, the active count is
min(desired replicas, eligible nodes), following the replica-layout contract.

If N=0, required nonempty placement waits/retries. If D=0, skip the RG term;
release and zero-row placement lifecycle still apply. Demand and its aggregates
must cover the entire RG, not just the selected collections.

An in-band node has zero penalty. The absolute tolerance is essential at low
volume: relative variance alone can favor splitting one tiny shard over many
empty nodes. A tolerance is a balance target, not a capacity admission check.

### 3.2 Shard and Collection Concentration

For each nonempty shard replica s, let Q be the desired concentration scale in
rows, R_s its total rows, and A_s its eligible replica target node set:

```text
K_raw = min(len(A_s), SegmentCount_s, max(1, ceil(R_s / Q)))
U_s   = (1 + shardTolerance) * R_s / K_s
H_s(P) = sum_n max(0, ShardRows[s,n] - U_s)^2 / (2 * U_s)
```

K_s is the retained preferred fanout derived from K_raw. Use distinct growth
and shrink thresholds around the row boundaries. Clamp K_s immediately when
node or segment counts shrink; only data-size-driven changes use hysteresis.
Evaluate candidates with one fixed K_s. The exact hysteresis margins require
calibration. Empty membership uses K_s=0; a nonempty zero-row shard still needs
legal assignments but skips the zero-denominator concentration term.

For a small shard K_s=1, so this term does not reward spreading it. For a large
shard it penalizes excessive rows on one node. K_s is a preference, not a demand
to open exactly K_s nodes, and indivisible segments may prevent the target.

For collection replica c with R_c rows and N_c eligible target nodes:

```text
U_c   = (1 + collectionTolerance) * max(Q, R_c / N_c)
H_c(P) = sum_n max(0, CollectionRows[c,n] - U_c)^2 / (2 * U_c)
```

Skip this term when N_c=0. The floor Q lets a tiny collection remain compact.
The collection term prevents many individually small shards of a large
collection from all accumulating on a few nodes. Replica constraints can make
some concentration unavoidable; do not widen every node's tolerance to the
largest segment in the cluster merely to hide such violations.

### 3.3 Fanout

```text
F(P) = Q * sum_s max(0, UsedNodes[s] - K_s)
```

Count nodes with assigned segments, including zero-row segments. Charge the
net change in fanout: opening an excess node increases the penalty; closing one
reduces it. An entire small shard moved to another node has unchanged fanout.
This is a soft placement cost, not a hard node-count cap.

### 3.4 Migration Cost and Acceptance

```text
E(P) = wG * G(P) + wS * sum_s H_s(P) + wC * sum_c H_c(P) + wF * F(P)

Cost(P -> P') = movePrice * MovedRows + loadPrice * AdditionalLoadRows
Score(P -> P') = E(P) - E(P') - Cost(P -> P')
```

MovedRows counts final placement changes relative to the candidate's baseline.
AdditionalLoadRows counts missing compatible destination resources, deduplicated
by destination and materialization identity for that candidate. These are row
estimates, not measured I/O. A reusable destination can avoid loading cost while
still incurring movement cost. Do not count temporary search intermediates as
executed moves. Unknown compatibility receives no guaranteed reuse discount.

Staying has score zero. Optional candidates require Score greater than a
positive minimum-gain threshold; unchanged assignments are no-ops. Recompute
the complete candidate's delta before accepting it. Do not sum the baseline's
per-segment node scores, which repeatedly charge load and fanout effects.

Apply an additional RG guard to Optional work: G(P') must not exceed G(P), apart
from numerical comparison tolerance. Consequently, local optimization cannot
take an in-band RG out of its band or worsen aggregate out-of-band violation.
Inside the band, local distribution, fanout, and reuse determine the tradeoff.

Must work bypasses the positive-gain test and optional RG guard: new placement,
failure repair, RG legality, replica isolation, and required metadata advancement
cannot depend on economic benefit. Choose the best legal required placement;
avoid including unrelated optional moves that failed their own acceptance test.
An unchanged assignment can still require a new QueryView for version changes.

Weights and cost prices are finite and nonnegative, Q is positive, and the
selected profile must enable the required objectives. New parameter names,
defaults, and migration from the baseline configuration remain implementation
and calibration work. Do not silently reinterpret existing keys or install the
previously discussed 1/20/10 weight tuple as part of this documentation change.

### 3.5 Quantitative Example

Consider node loads 3M/3M/0, demand 6M, mu=2M, and delta=0.2M. Each segment has
1M rows. For illustration only, take wG=1 and combined migration cost 0.1M per
move, and omit the other objective terms:

| Placement | G(P), in row units | Next move's RG gain | Net gain |
|---|---:|---:|---:|
| 3M / 3M / 0 | 1.13M | 0.81M | 0.71M |
| 2M / 3M / 1M | 0.32M | 0.32M | 0.22M |
| 2M / 2M / 2M | 0 | No further beneficial move | Stay |

Unlike saturated stickiness, this model allows a 1M-row segment to move when
its actual benefit exceeds its cost. With zero tolerance, the RG gain for moving
x rows from a to b is x*(L[a]-L[b]-x)/mu. A price gamma*x then gives the explicit
threshold L[a]-L[b] > x + gamma*mu. With a band, evaluate the two affected node
penalties directly. Positive migration cost can stop improvement near a band
boundary; this rule does not guarantee reaching the exact band. These numbers
are analytical examples, not validated defaults or execution-test results.

## 4. Incremental Candidate Construction

Start from the selected intended assignment, preserving existing legal
placements. Remove retired membership and place new, missing, or illegal
segments. A DataVersion change requires fresh view metadata but does not by
itself require relocating every surviving SegmentID. Placement retention and
materialization compatibility are separate decisions.

For optional work, search bounded candidates:

- Move one segment between eligible nodes.
- Move a whole small shard while preserving its fanout.
- Consolidate a shard's assignments from one node onto other eligible nodes.

Evaluate whole-shard/consolidation candidates as complete changes within one
shard view, so a temporary search prefix's fanout does not block a beneficial
final candidate. Preserve deterministic tie-breaking, preferring fewer moved
rows, fewer additional loaded rows, and stable IDs for equivalent gains.

Keep Must before Optional, descending shard rows within the selected classes,
and shared projected loads. Merge accepted changes into one final QueryView per
changed shard. Plan the complete bounded batch before applying it. Speculative
candidates never update cache facts. Healthy accepted Preparing targets remain
stable while loading, unless topology or intent requires replacement.

Cross-shard/collection exchanges may escape local optima, but are follow-up
work: multiple QueryViews do not apply atomically, and an exchange needs explicit
partial-application tracking. Replica target ownership changes likewise require
a separate quota-preserving layout decision. The first implementation must report
these constrained stalls rather than claim global optimality or infeasibility.

## 5. Target Load, Residency, and Reuse

These are distinct derived views with different lifetimes:

| Information | Purpose | Removal/replacement |
|---|---|---|
| Intended placement and row contributions | Steady-state balance scoring | Accepted replacement, failure, release, or eligibility/intent invalidation |
| Resident/loading resources and references | Execution occupancy and load throttling | Actual resource/reference lifecycle; failure alone is not release |
| Confirmed compatible ready resources | Reuse discount | Loss of readiness, node loss, incompatibility, or loss of protected references |

Select a valid accepted Preparing target when present, otherwise the applicable
Up placement. Treat missing/invalid required placements as unplaced demand, not
zero desired demand. Publication must provide per-view contributions: the current
merged segment statistics cannot reconstruct this distinction reliably.

Replacing a selected target replaces its contribution; it does not add a second
full copy on top of Up. AddPreparing acceptance publishes this change
synchronously before returning. Failed acceptance reserves nothing. Failure,
preemption, release, and node/intent changes invalidate or replace contributions
through publication hooks, with a surviving valid Up placement used when
applicable. Count physical overlap separately until cleanup.

The cache remains derived state, not the lifecycle owner. The implementation
must define one target-selection publisher that combines accepted per-view
facts with LoadConfig, node eligibility, and the retained layout's active/release
decisions. Layout-derived selectors must be identifiable and refreshed on layout
changes; they are distinct from upstream actual-resource facts. Publishing a
layout selector must not reserve rows for an unaccepted candidate. Suspension
does not write back to LoadConfigStore. Recovery rebuilds derived target state
before enabling Optional planning.

Maintain immutable node totals and matching shard contributions, plus
collection-replica/node totals, shard/node totals, and RG demand aggregates.
Keep the existing object-local read and synchronous publication contracts. An
object's total and subtracted contribution must come from the same version.
No global consistent snapshot is required; changing inputs and rejected/failed
application lead to replanning.

### 5.1 Failure Does Not Erase Partial Success

Suppose Preparing A assigns S1/S2/S3 to node 1. S1 and S2 finish, but S3 fails.
The next Preparing B can keep S1/S2 on node 1 and assign S3 to node 2. A's failed
target no longer contributes as a valid plan; S1/S2 remain reusable if their
resource identity, node liveness, and protected references remain valid.

Required sequence:

1. Publish A's failure, invalidate its target contribution, and retain confirmed
   per-segment readiness and existing resource references.
2. Score B using these retained compatible resources. Do not award reuse credit
   to failed or merely requested/loading segments without a stronger contract.
3. Accept B and publish B's target contribution synchronously.
4. On each overlapping node, acquire B's references before releasing A's.
5. Release resources unique to A through ordinary teardown. Shared resources
   remain held by B. If no replacement is needed, release A normally.

The resource manager must establish references before Acquire returns, or offer
an equivalent ordered handover guarantee before old Release can unload them.
Asynchronous loading and callbacks are allowed; deferring reference acquisition
past Release is not safe merely because Acquire was called first. A stale reuse
estimate must still fall back to correct loading. Lost nodes, incompatible
materializations, and resources already being released receive no guaranteed
reuse credit. Whole-view failure must not poison every successful segment.

### 5.2 Current Implementation Gap

The existing QN state machine preserves ready segment IDs when reporting
Unrecoverable and does not immediately request Release. Coord defers Dropping
until replacement or release, and QN ApplyViews processes new Preparing before
old teardown within the batch. These are useful lifecycle foundations.

However, ShardStats.Resources currently indexes only whole Up/Ready views.
Although merged segment statistics can retain Ready segments from an
Unrecoverable view, allocate.reusableResources rebuilds positive reuse evidence
from that narrower index. Partial success from failed Preparing is therefore
not fully credited unless another qualifying view holds the resource.

Extend the index to confirmed individual ready resources from relevant
Preparing/Unrecoverable views while references remain protected. Retain exact
compatibility checks; matching SegmentID alone across DataVersions is
insufficient evidence. The current branch only defines the injected QV
SegmentManager interface, so concrete resource sharing and handover still need
integration verification. This design does not claim that they are already wired.

## 6. Partial Scopes, Budgets, and Scenarios

A partial reconcile reads RG node-level aggregates and selected collection
details. Unselected contributions remain background load. Candidate deltas need
only changed nodes and local summaries; they do not require all collections'
segment membership. Maintain demand on publication and derive active demand from
replica/topology summaries, without scanning all segments. Publish topology
changes and account for the old/new RG before comparing affected candidates.

Fairly rotate candidate discovery among relevant collections with retained
continuations. Top-K-only selection must not starve smaller useful candidates.
An explicitly restricted collection batch cannot fix a problem requiring moves
outside that scope. Periodic coverage supplies opportunities, not proof that a
weighted greedy policy reaches a globally optimal placement.

Bound Optional candidate evaluations, moved rows, additional loading rows,
changed shards, and concurrent destination loading. An indivisible item larger
than the per-pass budget requires accumulated credit or an explicit oversized
item allowance; otherwise it can starve forever. Required recovery has separate
work budgets but still obeys execution resource constraints. Keep unfinished
work scheduled rather than marking budget exhaustion as convergence.

Use bounded batches to reduce recovery latency. Must-first order is not
preemption of a batch already running. Candidate search bounds do not remove
the O(S) cost of materializing a complete changed QueryView; a hard latency bound
for enormous shards would require separate resumable/materialization work.

| Scenario | Policy behavior |
|---|---|
| Scale out | Include new eligible nodes, repair replica quotas, then make bounded positive-gain moves |
| Scale in/drain | Exclude departing nodes from new placement; replace affected assignments while retaining serving coverage |
| Failure | Repair missing/invalid placements first, retaining healthy placements and confirmed reusable partial resources |
| Replica load/release | Update demand and target constraints; restore or release through normal lifecycle |
| Node changes RG | Refresh both groups, repair legality and replica targets, then optimize within each group |
| Rolling replacement | Use explicit upstream maintenance/replacement intent if available; a new or larger NodeID alone cannot identify a replacement |

Node loss cannot wait indefinitely for a presumed rolling replacement. Existing
NodeInfo has no maintenance/replacement identity; such scenario-specific control
requires a separate upstream contract. Preserve the current shortage suspension
and last-serving-cover behavior in all cases.

## 7. Guarantees, Delivery, and Verification

With fixed inputs and successful application, a positive accepted net gain and
nonnegative migration cost strictly reduce E for the evaluated Optional target
transition. This is not a guarantee about instantaneous physical occupancy,
globally atomic mixed-time reads, partially applied batches, or global optimality.
Changed demand, topology, configuration, and hysteresis targets change the
objective; invalidated/failed work must be evaluated again. Disjoint replica
quotas can impose unavoidable skew, such as two full replicas on three nodes
with a 2+1 split.

Implement in stages:

1. Per-view target accounting, protected per-segment reuse evidence, objective
   deltas, incremental single-shard candidates, and final gain acceptance.
2. Fair bounded discovery, continuations, and migration/loading budgets.
3. Calibration using reproducible event sequences; add multi-view exchanges or
   layout-owner exchanges only if constrained stalls justify their complexity.

Required checks include:

- Objective arithmetic, net opening/closing fanout, deterministic ties, zero
  demand/rows, large row counts, and unchanged-plan rejection.
- Tiny shard on many empty nodes, large shard spread, and a large collection
  composed of tiny shards; growth/shrink around fanout thresholds.
- Initial load, scale out/in, node 1 loss followed by node 4 addition, replica
  changes, RG reassignment, and indivisible segments/unequal replica quotas.
- DataView/compaction changes preserving unaffected placements; no unsupported
  cross-version reuse inference.
- S1/S2 ready followed by S3 failure, failure on another node, replacement
  reference handover, cleanup without replacement, and node loss during handover.
- AddPreparing rejection, later failure, preemption, release, delayed loading,
  partial batch application, restart, and no leaked target contributions.
- Partial scopes with 10,000+ collections, fair continuation, oversized work,
  bounded candidate search, and explicit final-view materialization costs.

Measure RG/local skew, fanout, moved and newly loaded rows, convergence passes,
recovery latency, and planning CPU/allocations. No new default weights or strict
convergence-to-band claim may be inferred from the illustrative example.

Key implementation packages: `internal/views/coord/balancer/` (policy/scoring,
planning, layout, and controller), its `cache/` and `api/` subpackages,
`internal/views/coord/coordview/` (per-view facts and lifecycle publication),
`internal/querynodev2/qnview/` (handler/reference interface), and
`internal/dataview/` (immutable membership and matching row footprints).
