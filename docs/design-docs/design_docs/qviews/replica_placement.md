# QueryNode Replica Placement Algorithm

Node balance and stability take priority over loading cost. The implementation
integrates with the [Balancer](balancer_design.md) and
[Cache](balancer_cache.md). The current interfaces do not expose node capacity,
so the first version assumes homogeneous QueryNodes and identical resource
requirements for replicas of the same collection.

## 1. Planning Domain and Invariants

Each `(CollectionID, ResourceGroup)` is an independent planning domain. Let N
be the number of Alive, non-Stopping QueryNodes in that resource group, and R
the desired number of replicas for the collection in that resource group.
Different collections may share nodes; there is no global exclusive ownership.

For R > 0, the following quota rules apply, including when N < R:

- Each node has exactly one target replica owner within the domain. All
  vchannels of a replica use the same target node set.
- Each replica receives floor(N/R) or ceil(N/R) nodes, and all N nodes belong
  to a target set. Assignment does not require loading segments onto every node.
- Given existing unique ownership, first minimize ownership changes for nodes
  that remain alive and within the domain, then heuristically reduce the number
  of rows that must be loaded.
- Targets constrain new views. Existing views may remain outside their targets
  until safe replacement and cleanup complete.

When N < R, the scheduler temporarily suspends excess replicas instead of
maintaining all replicas through permanent node sharing. Since floor(N/R)=0,
exactly N replicas receive one node each; the remaining R-N replicas receive a
zero quota. Desired LoadConfig, ReplicaID, and resource group membership remain
unchanged. Replicas are restored automatically when nodes recover. Active means
that a replica has a nonempty target node set, not that it is already Serving.
Externally reported counts must distinguish Desired, Active, and Serving.

Replicas that already own healthy nodes are preferred, using the gain-based
quota algorithm in Section 4 rather than truncating by ReplicaID. When retention
gain and previous quota stability are equal and targets have not yet been
frozen, prefer fully serving replicas, then use a stable hash to break ties.
Ordinary view progress does not trigger reelection of the Active set. During a
resource shortage, do not rotate replicas for fairness: that would repeatedly
suspend healthy replicas.

A zero quota is a target state and does not require immediate destruction of
existing views. A replica moves through Draining to Suspended: first ensure
that a retained replica can serve the corresponding shard, then release the
suspended replica through the existing view lifecycle. Retain an existing
healthy view temporarily if it is the only serving coverage for a shard;
shrinking the Active set must not cause an outage. Views that cannot serve do
not need to wait for this barrier. Temporary node sharing is allowed during
this handover.

When N = 0, no valid new placement exists for nonempty data. Preserve lifecycle
state and wait for node events or retries without generating invalid targets.
When R = 0, discard targets and clean up residual views.

Feasibility still requires target nodes to load the data. The current interfaces
provide no capacity admission control, so node count alone cannot guarantee
sufficient memory. In particular, when N < R, each Active replica has only one
target node. If a complete replica does not fit, min(N,R) is only the upper
bound implied by node count, not a guarantee that all selected replicas will
serve. The algorithm does not further reduce the Active count automatically
in response to memory failures; that requires a separate capacity model.
Loading failures must neither destabilize targets nor cause premature removal
of an existing serving Up view.

## 2. State and Interfaces

DefaultBalancePolicy owns LayoutManager under its mutex and retains targets
across reconciles. The Balancer's reconcileMu serializes planning and execution.
The cache stores facts only.

```go
type CollectionLayout struct {
    // Fingerprint contains only replica/RG intent and eligible topology,
    // excluding ordinary DataVersion, row count, and Ready/Up progress changes.
    Fingerprint LayoutFingerprint
    Owner map[NodeID]ReplicaID
    NodesByReplica map[ReplicaID][]NodeID // An empty set targets Suspended.
}
```

The conceptual interface is
`EnsureLayout(collection, eligibleNodes, previous, facts) -> (layout, changedNodes, affectedReplicas)`.
Construct the complete layout before replacing the previous target. A partial
apply failure does not roll back the target. Retain the target before issuing
this batch's AddPreparing calls so subsequent reconciles can reuse it. The
target is not a loaded-state fact; only synchronous runtime hooks update actual
state in the cache.

The cache needs fine-grained facts: each shard's Up, Preparing, and retained
reference node sets; aggregated node-to-replica references; and a deduplicated
node-to-compatible-resource index. Resource reuse requires evidence of
compatibility, not just a matching SegmentID. SegmentDataView exposes only ID,
PartitionID, and RowNum. Merged ShardStats alone cannot fully describe resource
compatibility and Dropping references, so the internal interfaces must provide
these facts or conservatively treat the resource as non-reusable.

Each reconcile uses Get to read immutable objects without requiring a global
snapshot. Subsequent events repair concurrent changes, and the next reconcile
compares the layout fingerprint. Resource group eligibility loss must notify
the group's desired collections, not only shards whose existing views use the
affected node.

## 3. Retaining Valid Targets

If the fingerprint is unchanged and target invariants hold, reuse the layout
without scanning segments.

When the fingerprint changes, retain owners whose nodes remain eligible and
whose replicas remain in the domain. New nodes, nodes owned by removed replicas,
and nodes entering from another resource group become Unowned. Remove nodes
that leave the domain. For targets that already have Preparing views, keep the
previous target as the baseline; an older Up view must not pull ownership back.

On initial startup or recovery without retained targets:

1. For nodes without conflicting replica references, seed ownership from
   actual Up views. If only healthy Preparing views exist, use those as seeds.
2. For conflicting nodes, select a provisional owner by lexicographically
   maximizing retained serving shard-node relationships, then Preparing
   relationships. Break ties with a stable collection/replica/node hash.
3. Nodes without references are Unowned. References from retired replicas do
   not establish new ownership.
4. The quota repair below produces a balanced target and freezes it within the
   recovered process.

Seeding during recovery with conflicts is heuristic. It does not guarantee
globally minimal movement relative to every historical view. The minimum
movement guarantee applies to existing unique target ownership. Existing views
remain protected by their lifecycle and are not revoked directly by seed
selection.

## 4. Quotas: Determine the Minimum Number of Node Transfers

Let `b = N / R` and `k = N % R`. Exactly k replicas must receive b+1 nodes; all
others receive b.

Let `c[r]` be the number of nodes currently owned by replica r after removing
invalid owners. Unowned nodes do not contribute to c. For a candidate quota:

```
Retained(quota) = sum_r min(c[r], quota[r])
Moved(quota)    = sum_r max(c[r] - quota[r], 0)
```

The retention gain from assigning r one extra slot is exactly:

```
gain[r] = min(c[r], b+1) - min(c[r], b) // 0 or 1
```

Select k replicas by `(gain descending, previously held an extra slot first,
fully serving first, stable hash)`. The first criterion exactly maximizes
Retained and minimizes Moved; the second stabilizes remainder allocation.
Loading cost optimization cannot override these higher-priority decisions.
There is no need to enumerate all quota combinations. When N < R, this rule
naturally selects Active replicas with nodes and assigns quota=0 to the others,
which remain in desired config.

Then compute:

```
excess[r]  = max(c[r] - quota[r], 0)
deficit[r] = max(quota[r] - c[r], 0)
M          = sum(excess)
```

First assign all Unowned nodes, then transfer exactly M nodes from replicas
with excess to replicas with deficits. Each previously owned node moves at
most once. Every node in the domain is eligible for every replica in that
domain, so candidate constraints cannot prevent filling the deficits.

Only `sum(c)` of the N nodes have valid owners, hence
`sum(deficit) = len(Unowned) + M`. This both establishes feasibility and proves
minimal movement: each donor must release at least its excess nodes, and the
algorithm transfers exactly that many.

## 5. Loading Cost: Deterministic Greedy Selection Within a Budget

Node balance and the minimum transfer count are already fixed. Compare loading
costs only among valid Unowned assignments and donor-to-receiver transfers.
Do not swap additional stable nodes to reduce loading cost.

### 5.1 Estimating Resource Coverage

Identify resources by compatible materialization identity, using raw RowNum
values. Shared, compatible loaded resources are reusable regardless of their
view or replica owner. Resources being released, resources with unknown
compatibility, and failed resources cannot receive guaranteed reuse credit.
In-flight loading can count as a single load only if request coalescing is
guaranteed.

Build sparse coverage counts for the tentative targets:

```
cover[r,s] = number of target nodes of r holding compatible resource s
Missing(T) = sum_active_r sum_desired_s rows(s) * [cover[r,s] == 0]
```

Do not construct an `N * R * S` matrix. Replicas of the same collection
currently have identical requirements, so coverage can be built by traversing
the deduplicated sparse node-resource index.

The immediate marginal estimate for transferring node n from a to b is:

```
loss(n,a) = sum_{s on n} rows(s) * [cover[a,s] == 1]
gain(n,b) = sum_{s on n} rows(s) * [cover[b,s] == 0]
delta    = loss(n,a) - gain(n,b)
```

For an Unowned node, loss is zero. This estimates the change in missing resource
rows across target sets, not the node's total rows or exact eventual I/O.
Balancing within a target set may still require additional loading.

### 5.2 Selection Procedure

The first version uses two greedy passes with fixed scores to avoid repeated
resource traversal from dynamically invalidated heap entries:

1. Build candidate edges from Unowned nodes to replicas with deficits. Compute
   `-gain` using current coverage, sort by `(cost, stable hash, IDs)`, and accept
   edges whose node remains unassigned and whose target still has capacity.
   Assign every Unowned node.
2. Refresh coverage once. Build candidate edges from donor nodes to replicas
   with remaining deficits, compute `loss-gain`, and sort. Accept edges while
   the node has not moved, the donor still has excess, and the receiver still
   has a deficit, until exactly M transfers have been selected.
3. Do not re-sort all edges after accepting one. Fixed scores may become stale,
   so loading cost remains heuristic; Missing is not guaranteed to be globally
   optimal. Structural constraints ensure that all deficits are filled,
   independently of cost ordering.
4. Build final coverage and Missing. An optional bounded improvement pass may
   swap the destination replicas of two transferred nodes, or replace a
   transferred node with a retained node from the same donor. Accept only
   changes that preserve quotas, do not increase ownership changes relative
   to original owners, and strictly reduce exact Missing. Leave stable layouts
   with no required transfers untouched.
5. Put hard limits on both candidate count and resource accesses for the
   improvement pass. Evaluate each swap using coverage-count deltas for the
   affected resources before applying it. Stop at the budget limit; the layout
   remains complete. This optional pass may be disabled in the first version
   without affecting correctness.

If estimated candidate edges or resource accesses exceed the cost optimization
budget, assign Unowned nodes in stable order, then select donor nodes to fill
deficits. Do not produce partial targets or delay required transfers. The
budget affects only loading cost, the lowest-priority objective; balance,
minimum movement, and availability are preserved. Start with internal fixed
budgets and tune them with benchmarks without adding user-facing configuration.

Basic quota repair costs `O(N + R log R)` if a stable sorted node index can be
reused; otherwise add `N log N`. Full candidate scoring costs
`O(F*R + N*R*log(N*R))` with `O(F + N*R)` temporary memory, where F is the number
of deduplicated reusable node-resource relationships for this collection in
this resource group. Candidate generation and resource access budgets bound
this additional work. Other collections are not scanned. Building resource
coverage itself consumes the budget and is skipped if the budget is insufficient.

## 6. Integration with Batch Shard Planning

```
WaitForReady()
takePending()
pin object-local planning inputs
for affected collection/RG:
    layout = EnsureLayout(...)
    add shards affected by changed layout
classify shards against layout
Must first, descending rows, shared projectedRows
allocate each shard within NodesByReplica[replica]
Apply batch through existing lifecycle
```

Plan all shard candidates for the entire batch before Apply. Do not switch to
immediate application after planning each collection.

When targets change, use per-shard footprints to identify shards referencing
transferred nodes and classify them as Must. Newly eligible nodes affect
optional optimization for all shards of the corresponding replicas. Reconsider
previously unallocatable shards and shards without views as well; existing
reference indexes alone are insufficient. Ordinary progress events only
recompute dirty shards and reuse the collection layout.

Refine the `hasPreparing -> None` rule: do not duplicate a healthy Preparing
view that can finish, but allow replacement of Preparing views referencing
lost or ineligible nodes. A healthy Preparing view outside its new target may
finish first, then converge through a subsequent Must migration. An early
return must not indefinitely prevent convergence.

Classification handles quota=0 before the rule "desired exists but no Up ->
Must." With no existing views, emit NoOp; otherwise RequestRelease subject to
the serving handover barrier. Suspension is not an allocation failure and must
not enter the normal failure retry queue. The next reconcile must not issue
AddPreparing merely because LoadConfig still contains the replica. The
Balancer's internal target node sets represent suspension; do not write it
back to LoadConfigStore or notify CollectionLoadManager. When the quota becomes
positive again, trigger all desired shards for that replica and restore service
through the normal Preparing/Up lifecycle.

The Balancer emits only view Prepare/Release operations and necessary retries.
It does not maintain or publish a separate discovery set. The QueryView
lifecycle should reflect actual serving state. Production discovery
subscriptions and publication wiring remain deferred and are not prerequisites
for this Balance execution. Existing lifecycle mechanisms continue to protect
in-flight requests and enforce view leases.

Use the node/resource index for reuse across replicas. Lifecycle and assignment
equality remain scoped by ShardID. Shared projectedRows retains the current
logical row accounting: do not deduplicate actual physical resources while
still subtracting each shard's full contribution. This version gives reuse
credit only to ready resources with exactly matching versions and does not
assume in-flight loads can be coalesced. New targets are disjoint across
replicas, and a segment belongs to only one vchannel, so this batch's new
placements for the same collection do not request the same node/resource
across shards.

## 7. Application and Convergence

- AddPreparing publishes to the cache synchronously before asynchronous loading
  and synchronization. If the target is unchanged, the next reconcile uses the
  same node set.
- Retain the old Up view until the new view reaches Up. Other replicas' old
  views may still reference target nodes. Do not wait for those references to
  be released before acquiring resources; without spare nodes, such waiting
  could deadlock migrations.
- After the new Up view is ready, old views exit through the existing
  Down/Dropping lifecycle. Track convergence of serving placement separately
  from cleanup of physical references. Dropping views may still hold resources;
  PendingRows=0 does not prove that physical isolation is complete.
- Ordinary Load/DataView resource changes do not reelect owners. Only
  replica, resource group, or topology changes repair targets. Fluctuating row
  counts on target nodes do not cause ownership churn.
- When a Suspended replica becomes Active, establish its target first, then
  load each shard and restore service after Up. Loading failures preserve
  existing serving views and retry the same target rather than repeatedly
  switching layouts.
- Convergence assumes that topology and intent eventually stabilize, target
  resources can be loaded, and retries and cleanup eventually progress. Under
  these assumptions, fixed targets, Must migrations, and old-reference cleanup
  provide eventual physical isolation.

## 8. Validation Matrix

**Quotas and stability:** floor/ceil quotas for arbitrary N/R; unique ownership
of every node; the minimum transfer bound; exactly one transfer for six nodes
from 4/2 to 3/3; unchanged 3/2 allocation for five nodes; assigning a sixth node
to the smaller replica without moving existing nodes; scale-down; replica
addition/removal; resource group changes; and conflicting replica references
during recovery.

**Cost:** reuse across replicas; counting multiple copies of a resource as a
single covered resource; fixed-score error and bounded improvement when moving
the last two copies; rejecting false compatibility across different manifests
or loading requirements; completing targets with a zero optimization budget;
and preserving large RowNum differences without saturation normalization.

**Lifecycle:** Active=min(N,R) with excess replicas Suspended and desired state
unchanged; no rotation of healthy Active replicas; no repeated Prepare/Retry
for suspended replicas; protection of the last serving cover; no suspension
updates back to load configuration management; automatic restoration of the
original ReplicaID when nodes recover; migrations without spare nodes;
partial Apply; healthy Preparing views outside targets and Preparing views on
lost nodes; crash recovery; delayed Dropping cleanup; new nodes triggering
relevant collections even without existing views; and ordinary progress not
scanning the full resource set.

**Performance:** benchmark the layout fast path, quota-only repair, and resource
cost estimation budget limits separately from the existing shard allocator's
complexity. This design does not claim to eliminate the existing CurrentRows
scan over all nodes or the per-shard copy of projectedRows.

Implementation choices: readiness and an exact match of
`(PartitionID, SegmentID, DataVersion, LoadInfoVersion)` are sufficient for reuse
credit. Compatible resources across DataVersions do not receive credit yet;
this conservative estimate avoids incorrect reuse without materialization
identity evidence. The first version disables the optional local improvement
pass. The default policy's dedicated LayoutManager retains target layouts and
serializes Plan; the custom policy interface remains unchanged.

The view and cache component implementation introduces neither new RPCs nor
replica node lists. Targets are retained as immutable layout objects without
additional persisted generations or discovery revisions.
