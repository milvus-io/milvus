# MEP: Resource-Group Balance Epoch

- **Created:** 2026-07-12
- **Last Updated:** 2026-07-15
- **Author(s):** @xiaofan-luan
- **Status:** Draft
- **Component:** Coordinator
- **Related Issues:** [milvus-io/milvus#51244](https://github.com/milvus-io/milvus/issues/51244)
- **Related Pull Requests:** [milvus-io/milvus#49861](https://github.com/milvus-io/milvus/pull/49861), [milvus-io/milvus#50774](https://github.com/milvus-io/milvus/pull/50774)
- **Implementation Baseline:** `milvus-io/milvus@4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d`
- **Implementation Branch:** `xiaofan-luan/milvus:feature/resource-group-balance-epoch` at `8c7b55f32cdf031ead70e99afdace281434761bb`
- **Implementation Pull Request:** [milvus-io/milvus#51431](https://github.com/milvus-io/milvus/pull/51431)
- **Released:** Not released

## Summary

This MEP introduces a resource-group-scoped balance epoch for QueryCoord.

Today, QueryCoord periodically generates balance plans collection by collection and submits them directly to the task scheduler. A later checker iteration can plan again while an earlier set of Grow and Reduce actions is still being executed or has only partially appeared in QueryNode distribution. Scheduler task deltas reduce this problem, but they do not provide a consistent planning snapshot, a bounded planning wave, or a clear boundary between acting and observing.

The implemented control loop is:

```text
Observe -> Plan -> Admit -> Execute -> Reconcile -> Observe again
```

Each Resource Group (RG) is an independent balance group and may have at most one current normal-balance planning generation. An epoch:

1. captures a versioned placement snapshot for the RG;
2. generates a bounded wave of channel and segment plans;
3. admits tasks while revalidating their preconditions;
4. observes the authoritative QueryNode distribution and classifies task outcomes;
5. reconciles completed work and carries unresolved safe-settle work forward as locked pending work;
6. ends as completed, degraded, superseded, or timed out; and
7. allows the next epoch to plan unrelated objects from a newly reconciled snapshot.

Different RGs can run epochs concurrently. Stopping balance and failure recovery retain higher priority and stay on legacy paths in the MVP. The stopping path inside `BalanceChecker` prevents a new normal epoch in the same tick. Independent recovery checkers are coordinated conservatively through pending snapshots, scheduler deduplication, stale admission, and later reconciliation rather than a common preemption signal.

This MEP changes the orchestration and correctness boundary of balancing. It does not define a new resource-aware scoring function, shard placement policy, or migration-cost model. Those policies can be implemented separately on top of the snapshot and epoch interfaces defined here.

## Implementation Status

The MVP described by this MEP is implemented and reviewed on the Milvus feature branch. Relative to baseline `4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d`, final implementation commit `8c7b55f32cdf031ead70e99afdace281434761bb` contains 22 commits and changes 39 files, including 38 Go files. Its exact tree is `bf66602b87ebde3dd1332cce9fa956d1a08864e9`. Active rollout remains disabled by default, and the implementation PR has not yet been opened because delivery follows the design-first sequence described below.

| Layer | Status | Milvus commits | Main files |
|---|---|---|---|
| Task completion and distribution-aware pending effects | Implemented and reviewed | `89232b82ad` | `task/task.go`, `task/scheduler.go` |
| Typed and generation-aware held admission | Implemented and reviewed | `6301167abb`, `090cd11a34` | `task/balance_admission.go`, `task/scheduler.go` |
| Atomic distribution publication and immutable RG snapshots | Implemented and reviewed | `18bf3d3850` through `9e586bcf37` | `meta/dist_manager.go`, `dist/dist_handler.go`, `balance/epoch_types.go`, `balance/epoch_snapshot.go` |
| Hard wave budgets, immutable reservations, and projected placement | Implemented and reviewed | `81ed05fa0d`, `4e0ca8b074` | `balance/epoch_wave.go` |
| Snapshot-only ScoreBased and ChannelLevel policy | Implemented and reviewed | `00285d5882` | `balance/epoch_score_policy.go`, `balance/balancer_factory.go` |
| Tick-driven per-RG epoch state machine | Implemented and reviewed | `cf4cc96145` through `9e8358791e` | `balance/epoch_manager.go` |
| `BalanceChecker` integration | Implemented and reviewed | `c8c1b18d04` | `checkers/balance_checker.go` |
| Dynamic rollout configuration | Implemented and reviewed | `c052fdbd66` | `configs/milvus.yaml`, `pkg/util/paramtable/component_param.go` |
| Metrics, shadow mode, and retained-state observation | Implemented and reviewed | `ba3b96816b`, `13f81066a6` | `pkg/metrics/querycoord_metrics.go`, checker and manager files |
| #51244 convergence and failure fixtures | Implemented and reviewed | `b454240dc6` | `balance/epoch_manager_test.go` |
| Ambiguous balance RPC outcome preservation | Implemented and reviewed | `5db539d20c` | `session/rpc_outcome.go`, `session/cluster.go`, `task/execution_outcome.go`, `task/executor.go`, `balance/epoch_manager.go` |
| Dist controller start-loop synchronization | Test-only validation stabilization | `6b3a211127` | `dist/dist_controller_test.go` |
| Typed balance-epoch error classification | Review fix; no new control-loop architecture | `0db483db33` | `balance/epoch_manager.go`, `epoch_snapshot.go`, `epoch_wave.go`, and focused tests |
| Unused server snapshot-builder wiring removal | Review cleanup; manager-owned builder unchanged | `8c7b55f32c` | `server.go`, `server_test.go` |

Commit `6b3a211127` synchronizes only the dist controller test start loops so the validation fixture waits for both goroutines; it does not change production distribution behavior. Commit `0db483db33` replaces 21 branch-originated raw production errors with typed `merr` origins: 17 invariant/protocol failures use the `ErrServiceInternal` family, while four transient cases use retriable `ErrServiceUnavailable`; the unresolved-ambiguity path preserves the prior cause as an `errors.Join` sibling. Commit `8c7b55f32c` removes the unused server-owned snapshot-builder field, construction, and assertion while leaving the manager-owned builder unchanged. These review fixes refine diagnostics and construction ownership without changing the epoch protocol described by this MEP.

The final focused, race, and complete `internal/querycoordv2/...` behavioral runs are green on the approved `mini` macOS development host with the repository native-library RPATH and isolated temporary etcd. The final re-review reported zero Critical, Important, or Minor findings. Branch-added raw-constructor scans found zero production origins, and the scoped balance-package ruleguard run reported `rawmerrerror: 0`. Repository-wide `make static-check` is not claimed green: it stops during core-package typechecking, before ruleguard, because this checkout lacks the unchanged generated package `cmd/tools/migration/legacy/legacypb`.

## Motivation

### Current behavior

The implementation was rebased from Milvus master commit `4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d`:

- `BalanceChecker` periodically selects collections and replicas, invokes the configured balancer, converts plans to tasks, and calls `Scheduler.Add` directly.
- Stopping balance has priority over normal balance.
- The default normal policy is `ChannelLevelScoreBalancer`.
- Channel planning is performed before segment planning. If channel plans are generated, segment planning is deferred to a later checker iteration.
- A segment move is represented as ordered `Grow(target)` and `Reduce(source)` actions.
- RPC completion is not authoritative task completion. QueryCoord waits for QueryNode distribution to contain the target before Reduce, and waits for the source copy to disappear before completing the task.
- Pending segment-task deltas are distribution-aware after PR #49861, preventing Grow and Reduce effects that are already reflected in distribution from being counted twice.

These mechanisms are necessary but do not form a complete closed-loop balance protocol.

### Problems

#### Planning may overlap execution feedback

QueryNode distribution is pulled asynchronously. A checker iteration can observe a mixture of:

- actions that have not started;
- Grow RPCs that returned but are not visible in distribution;
- target copies that are visible while source copies still exist;
- actions with RPC errors, RPC deadlines, or external cancellation;
- node or shard-leader changes; and
- target or replica metadata from a different logical point in time.

Task deltas project some in-flight effects, but a new planning round still has no explicit dependency on the completion or reconciliation of the previous planning round.

#### Independently valid plans may conflict as a batch

Plans generated for different collections or replicas in the same RG compete for the same QueryNode capacity. If they are generated independently from the same pre-move state, multiple plans may select the same apparently underloaded node. Their combined result may provide little or no net redistribution even when each individual decision appeared beneficial.

#### Generated work is not the same as admitted work

The current checker counts generated task objects before scheduler admission. `Scheduler.Add` may reject a task because of deduplication, a stale source, a missing leader, or another state change. Admission errors are currently ignored by `BalanceChecker`, so control-loop accounting may advance even when no work was accepted.

#### Existing batch limits are not hard wave boundaries

Batch limits are checked before processing a collection. One collection with multiple replicas, shards, or outbound nodes can generate more tasks than the nominal checker-wide limit. There is no RG-wide movement or task reservation shared by all plans in the round.

#### Failure handling lacks an epoch-level reconciliation boundary

A failed Grow is normally safe because the source remains. A failed Reduce leaves a safe but redundant copy. A node failure, RG membership change, target change, or shard-leader change can invalidate the assumptions of many plans at once. These cases require different handling, but the current periodic checker has no explicit epoch state describing whether it is still executing, reconciling, or invalidated.

### Goals

This MEP has the following goals:

1. Establish one current closed-loop normal-balance planning generation per RG.
2. Prevent a new normal planning wave from ignoring unresolved results from the previous wave.
3. Build each wave from one immutable, versioned placement snapshot.
4. Count and reserve only successfully admitted tasks.
5. Enforce hard per-epoch task limits and provide an extension point for byte/resource budgets.
6. Define deterministic epoch behavior for task failure, ambiguous RPC outcomes, node failure, target changes, and RG topology changes.
7. Preserve the existing Grow-before-Reduce availability invariant.
8. Allow different RGs to balance independently and concurrently.
9. Preserve stopping-balance and recovery priority while keeping their admission path outside the MVP epoch controller.
10. Prevent one permanently slow or bad object from blocking unrelated objects in the same RG.
11. Provide metrics that distinguish planning, admission, execution, distribution waiting, reconciliation, and convergence.
12. Preserve legacy behavior when the epoch feature is disabled or the configured balancer has no snapshot-backed epoch policy.

### Non-goals

This MEP does not:

- replace `ChannelLevelScoreBalancer` or define a new score;
- add per-segment memory or disk estimation;
- define small-cluster versus shard-local placement;
- optimize migration byte cost;
- make a balance wave transactional or roll back all successful moves after one task fails;
- persist an in-progress epoch across QueryCoord restart;
- serialize unrelated RGs behind a cluster-global balance lock;
- add scheduler action retries;
- add reverse-move cooldown;
- add byte-level migration admission in the MVP; or
- route stopping/recovery work through the typed epoch admission gateway in the MVP.

Stopping balance and recovery remain on their existing higher-priority paths in the first implementation. Stopping work admitted by `BalanceChecker` prevents new normal work in that tick. Independently scheduled recovery may race with normal admission; pending-snapshot validation, scheduler deduplication, and reconciliation preserve safety. Common recovery admission classes and epoch-scoped preemption are follow-up work.

## Terminology

### Balance group

A balance group is one Resource Group. QueryNode capacity and replica placement are isolated by RG, so normal placement decisions in different RGs can proceed independently.

### Placement snapshot

An immutable, versioned view of the RG state used to produce one epoch plan. It contains actual distribution, desired target state, topology, pending tasks, and node eligibility.

### Balance epoch

A versioned planning and admission generation for one RG. An epoch is not a transaction and is not an all-task completion barrier. Tasks from an older epoch may continue to a safe point after that epoch is superseded, but their objects and resource reservations remain visible and locked in later snapshots.

### Wave

The set of channel and segment move tasks admitted by one epoch. Plans are simulated together before admission and share one task/resource budget.

### Reconciliation

The process of reading authoritative distribution and scheduler state after task execution, failure, timeout, or invalidation. Reconciliation classifies each object as resolved or carried forward so the next epoch can plan without reusing uncertain capacity or moving the same object again.

## Design Details

### Architecture overview

```text
                         +----------------------+
                         | Balance Trigger      |
                         | periodic / manual    |
                         +----------+-----------+
                                    |
                                    v
                    +---------------+----------------+
                    | BalanceEpochManager             |
                    | active epoch keyed by RG        |
                    +---------------+----------------+
                                    |
              +---------------------+---------------------+
              |                                           |
              v                                           v
  +-----------+------------+                 +------------+-----------+
  | PlacementSnapshotBuilder|                 | Epoch Event Handler    |
  | dist/target/topology/    |                 | node/target/RG changes |
  | pending task versions    |                 | task outcomes/deadline|
  +-----------+-------------+                 +------------+-----------+
              |                                           |
              v                                           |
  +-----------+-------------+                             |
  | BalancePlanner          |                             |
  | channel/segment plans   |                             |
  | simulated as one wave   |                             |
  +-----------+-------------+                             |
              |                                           |
              v                                           |
  +-----------+-------------+                             |
  | Epoch Admission         |<----------------------------+
  | revalidate + reserve    |
  +-----------+-------------+
              |
              v
  +-----------+-------------+
  | Existing Task Scheduler |
  | Grow -> dist -> Reduce   |
  +-----------+-------------+
              |
              v
  +-----------+-------------+
  | Distribution Feedback   |
  +-------------------------+
```

### Epoch scope and concurrency

The balance-group key is the RG name. Each RG has at most one current normal planning generation:

```text
RG-A -> epoch 101, Executing; epoch 100 has one safe-settle task
RG-B -> epoch 38, Reconciling
RG-C -> no active epoch, converged
```

The RG boundary is chosen because all replicas assigned to an RG compete for the same QueryNodes. Planning only per collection cannot reserve shared destination capacity. A cluster-wide epoch is unnecessarily coarse because independent RGs do not share QueryNodes.

A collection can have replicas in different RGs. These replicas participate in the epoch of their assigned RG and do not force epochs in different RGs to serialize.

Moving a node between RGs or moving a replica to another RG is a topology operation, not a normal segment-balance action. It invalidates epochs in the affected RGs. Those RGs reconcile independently after the topology operation.

### Epoch identity

Task attribution uses the implemented `task.BalanceEpochMeta` value:

```go
type BalanceEpochMeta struct {
    ResourceGroup string
    LeaderTerm    uint64
    Sequence      uint64
}
```

`LeaderTerm` is a process-boot identifier in the MVP, derived once when `BalanceEpochManager` is created. Tests may inject a fixed value. It prevents an old in-process completion from being attributed to a new manager instance. The manager keeps a monotonic next-sequence counter per RG for its full process lifetime, even after an idle runtime is removed, so `(ResourceGroup, LeaderTerm, Sequence)` is not reused. Neither field replaces distribution, target, or topology versions.

### Placement snapshot

The planner consumes one immutable, value-only `PlacementSnapshot`. The implemented token separates the identity of topology and desired state from the ordinary distribution revisions that the epoch itself is expected to advance:

```go
type SnapshotToken struct {
    ResourceGroup         string
    RGHash                uint64
    ReplicaHash           uint64
    NodeHash              uint64
    LeaderHash            uint64
    PlacementHash         uint64
    SegmentRevision       int64 // global diagnostic only
    ChannelRevision       int64 // global diagnostic only
    PendingTaskRevision   uint64 // RG-scoped absolute revision
    PendingGlobalRevision uint64 // diagnostic only
    CurrentTargetVersion  map[int64]int64
    NextTargetVersion     map[int64]int64
}

type AdmissionToken struct {
    Snapshot           SnapshotToken
    Epoch              task.BalanceEpochMeta
    CollectionID       int64
    ReplicaID          int64
    ExpectedSourceNode int64
    Segment            *SegmentObjectKey
    Channel            *ChannelObjectKey
}

type PlacementSnapshot struct {
    Token             SnapshotToken
    CapturedAt        time.Time
    Nodes             map[int64]NodeSnapshot
    Replicas          map[int64]ReplicaSnapshot
    Segments          map[SegmentObjectKey][]SegmentPlacement
    Channels          map[ChannelObjectKey][]ChannelPlacement
    CollectionTargets map[int64]CollectionTargetSnapshot
    PendingWork       PendingWorkSnapshot
    EligibleReplicas  map[int64]struct{}
}

type SegmentObjectKey struct {
    ReplicaID int64
    SegmentID int64
    Scope     querypb.DataScope
}

type ChannelObjectKey struct {
    ReplicaID int64
    Channel   string
}
```

`NodeSnapshot`, `ReplicaSnapshot`, segment/channel placement records, and target records contain primitives only. The snapshot copies RG physical nodes, replica RW/RO/RWSQ/ROSQ nodes, complete channel-to-RW-node mappings, node state/capacity/penalty, scoped segment and channel distribution, leader serviceability and target version, and both Current and Next targets. It never exposes manager-owned protobufs, maps, slices, `Segment`, `DmChannel`, or leader-view objects.

`SnapshotToken.Equal` compares Resource Group, RG/replica/node/leader/placement hashes, the RG-scoped pending revision, and Current/Next target-version maps. It intentionally ignores the global segment/channel and pending diagnostic revisions. `AdmissionToken.Epoch` binds validation to the generation that owns the plan. `SnapshotToken` privately retains the captured per-epoch pending revisions; `SnapshotToken.PendingRevision(epoch)` returns the RG absolute revision together with the selected epoch revision so generation-aware admission can compare their effective difference. `WithPendingRevision` returns a clone and never mutates the token stored in the planned wave.

The scope contains all workload placed on RG physical nodes or replica outgoing RO/ROSQ nodes, including collections that are not eligible to generate moves in this tick. Workload from unrelated RGs is excluded. In a collection with multiple replicas, an object is associated only with the replica that owns its physical or outgoing node; it is not duplicated across every replica of the collection.

One QueryNode distribution response is published atomically:

```go
func (dm *DistributionManager) PublishNodeDistribution(
    nodeID int64,
    segments []*Segment,
    channels []*DmChannel,
) []*DmChannel

func (dm *DistributionManager) RemoveNodeDistribution(nodeID int64)
func (dm *DistributionManager) Capture() DistributionSnapshot
```

Segment and channel managers share one publication RW lock. The lock order is always publication lock followed by the manager's internal lock. `distHandler` constructs both halves of a QueryNode response before publishing them, and node removal uses one atomic removal call. A capture therefore observes either the complete old response or the complete new response, never a mixed segment/channel pair.

The snapshot builder has the implemented interface:

```go
func NewPlacementSnapshotBuilder(
    metadata *meta.Meta,
    distribution *meta.DistributionManager,
    target meta.TargetManagerInterface,
    nodes *session.NodeManager,
    inspector task.BalanceTaskInspector,
    opts ...PlacementSnapshotBuilderOption,
) *PlacementSnapshotBuilder

func (b *PlacementSnapshotBuilder) Build(
    ctx context.Context,
    resourceGroup string,
    eligibleReplicaIDs []int64,
    carryOver []task.PendingBalanceTaskSnapshot,
) (*PlacementSnapshot, error)

func (b *PlacementSnapshotBuilder) Validate(
    token AdmissionToken,
) task.BalanceAdmissionReason
```

`Build` performs at most three optimistic attempts:

1. atomically capture segment/channel distribution;
2. capture a stable scheduler pending-task snapshot and revision;
3. copy RG, replica, node, leader, and Current/Next target primitives and compute deterministic FNV digests;
4. recapture distribution, pending revision, and topology/target digests; and
5. accept only when the complete before/after token is equal.

Pending-task revision is part of the same snapshot boundary because scheduler work can change capacity and object availability without an immediate distribution change. Capturing distribution and topology without the pending generation would allow the planner to observe old scheduler effects and then commit into a newer pending set. The before/after pending check makes snapshot construction stable, and the held effective-revision comparison closes the remaining capture-to-admission window.

Current and Next target content is generation-consistent inside one attempt. For each collection and target scope, the builder reads version-before, copies sealed segments and channels, then reads version-after. A version change retries the attempt. This prevents old segments, new channels, and a new version from being accepted as one target snapshot.

Workload on an RG physical node remains in node capacity even when replica metadata no longer has an owner. Such released/dirty placement is represented as capacity-only unowned work using `meta.NilReplica` (`ReplicaID=-1`) and is never offered as a normal move candidate. Nil-replica cleanup actions are scoped from action-node labels, ResourceManager physical membership, and outgoing RO/ROSQ ownership rather than an empty task Resource Group string.

Segment and channel revisions remain separate for capture diagnostics. `PlacementSnapshotBuilder.Validate` deliberately does not reject an executing epoch merely because those revisions advanced: the epoch's own Grow and Reduce actions are expected to change distribution. It revalidates RG, node, replica, target, leader, and expected source presence. The scheduler gateway separately performs the atomic RG-scoped pending-generation check at commit time.

#### Revision scope and RG independence

RG independence means more than separate runtime mutexes. Unrelated RG activity must not repeatedly invalidate admission for this RG.

The implementation uses an RG-filtered placement digest and per-RG pending revisions. Global segment/channel and pending revisions remain diagnostic fields only. Admission is wave-aware: successful commits from the current `BalanceEpochMeta` increase both absolute and epoch revisions, so the effective revision remains stable; external same-RG mutations change the effective revision and reject the remaining suffix. Unrelated RG distribution or scheduler churn does not retry or invalidate this RG.

Ordinary task mutations advance only the affected RG revisions. A task with `ReplicaID == meta.NilReplica.GetID()` and an empty Resource Group is different: its RG scope is inferred from action nodes and topology, and that mapping may change between scope resolution and commit. Every such mutation therefore also advances a topology-independent `UnscopedRevision`. `RevisionFor(rg, epoch)` adds this fence to each RG revision, so RG epochs are independent from ordinary work elsewhere but are deliberately not independent from unscoped cleanup churn that could have entered their node scope.

Revision equality is used to validate snapshot capture, not as a rule that aborts an executing epoch whenever distribution changes. The epoch's own Grow and Reduce actions are expected to advance distribution. Runtime invalidation is driven by the scoped event matrix below.

### Pending work

The snapshot includes primitive copies of relevant scheduler tasks plus epoch carry-over tasks. `PendingWork.Tasks` deliberately retains every copied action, including actions whose effect is already reflected in the captured distribution. Only the aggregate `SegmentWorkloadByNode` and `ChannelWorkloadByNode` projections filter reflected actions. Keeping the raw actions is necessary because the policy later reconstructs per-collection pending effects; it must call the same reflection rule again instead of assuming every retained action still contributes workload.

Pending effects remain distribution-aware:

- a Grow effect is pending until the target distribution contains the resource;
- a Reduce effect is pending until the source distribution no longer contains the resource; and
- an ambiguous RPC remains pending until reconciliation resolves the actual distribution.

Ambiguous carry-over is converted back into synthetic Grow(target) and Reduce(source) actions and projected with the same distribution-reflection rule as scheduler tasks. Independently, `ReservationAmbiguousCapacity` locks the replica-scoped object and charges both positive endpoints against the later generation's per-node budget. The workload score and the admission-capacity guard are therefore explicit, separate mechanisms.

Segment and channel task deltas now both store immutable action records and filter them against observed distribution. A channel Grow stops contributing `+1` once the target channel is visible; a Reduce stops contributing `-1` once the source channel disappears. The legacy `Scheduler.GetChannelTaskDelta` API remains available as a distribution-aware compatibility wrapper.

The scheduler also exposes an optional primitive inspector without widening the public `Scheduler` interface:

```go
type BalancePendingRevision struct {
    ResourceGroup string
    Epoch         BalanceEpochMeta
    Revision      uint64
    EpochRevision uint64
}

func (r BalancePendingRevision) EffectiveRevision() uint64

type PendingBalanceSnapshot struct {
    Revision               uint64
    UnscopedRevision       uint64
    ResourceGroupRevisions map[string]uint64
    EpochRevisions         map[BalanceEpochMeta]uint64
    Tasks                  []PendingBalanceTaskSnapshot
}

type BalanceTaskInspector interface {
    GetPendingBalanceTasks() PendingBalanceSnapshot
}

func (snapshot PendingBalanceSnapshot) RevisionFor(
    resourceGroup string,
    epoch BalanceEpochMeta,
) BalancePendingRevision
```

The concrete scheduler publishes task actions, epoch attribution, status, and object identity as defensive primitive copies. `GetPendingBalanceTasks().RevisionFor(rg, epoch)` is the initial token later supplied to the first wave admission. Its absolute revision is `ResourceGroupRevisions[rg] + UnscopedRevision`; `EffectiveRevision()` subtracts successful commits attributed to the same active epoch. A pending-view RW lock covers task-index and delta mutation as one critical section. Per-RG revisions change after a complete add, replacement, or removal affecting that RG, so the inspector cannot return a mixed view assembled from different scheduler states. NilReplica tasks derive affected RGs from action nodes, ResourceManager physical membership, and outgoing RO/ROSQ ownership, while the independent unscoped fence closes the topology-change window.

Pending/index/delta/revision replacement bookkeeping is atomic under the `scheduleMu` then `pendingMu` lock order. After that mutation, `pendingMu` is released first. `applyTaskRemovalPenalty` and replacement cancellation still run while `scheduleMu` excludes dispatch; cancellation may invoke task callbacks, so it must not run under `pendingMu`. After `scheduleMu` is released, the later removal finalizer performs target refresh, task-error caching, latency accounting, and removal logging. A slow finalizer therefore cannot block unrelated admission or pending-snapshot inspection.

Pending work is not required to belong to the current epoch. A task carried over from an older generation keeps:

- an epoch-owned object lock keyed by `SegmentObjectKey` or `ChannelObjectKey`;
- its destination-capacity reservation until distribution resolves the Grow outcome; and
- its source/target projected effects in later snapshots.

This allows the current planning generation to work on unrelated objects without pretending that old work disappeared.

Scheduler inspection and epoch carry-over use the concrete merge implemented by `mergePendingTasks`:

1. each task with a nonzero ID uses `task/<TaskID>` as its merge key;
2. a synthetic carry record with no task ID uses `anonymous/<CollectionID>/<ReplicaID>/<Actions>`;
3. scheduler records are inserted first, so a carry record with the same key is ignored rather than unioned with it;
4. a task whose Resource Group matches the snapshot is copied in full; an unscoped NilReplica task is included only with actions whose nodes fall inside the RG snapshot scope; and
5. the merged tasks are sorted by task ID, collection ID, and replica ID before workload projection.

There is no action union and there are no retry, quarantine, ambiguity, or reservation fields inside `PendingBalanceTaskSnapshot`. Those concerns are separate manager-owned `EpochPlanningConstraints`. If a scheduler task has disappeared but the manager still owns an ambiguous object, `pendingCarryOver` synthesizes its Grow and Reduce actions, allowing the anonymous or task-ID record to remain visible to the next snapshot. Object locks and node charges remain keyed by replica-scoped `BalanceObjectKey`, so later planning cannot reuse that uncertain object.

The object lock and reservation are owned by `BalanceEpochManager`, not by the current scheduler task record. A raw error returned after RPC dispatch can make the scheduler fail and remove a task even though the remote side effect is still unknown; removal also drops the scheduler dedup index and task delta. Epoch safety therefore cannot depend on that scheduler entry surviving. The executor preserves this case as a typed `AmbiguousExecutionError`, and the epoch-owned record remains until distribution resolves the outcome even after the scheduler task disappears.

In the MVP, a capacity reservation consists of scheduler task slots plus the row/count or channel-count workload effects understood by the configured current policy. This MEP does not claim exact byte-level admission. A future resource-aware planner may extend the same structure with memory, disk, GPU, and loading-byte reservations.

### Planning one wave

The planner receives the placement snapshot and one RG-wide budget. Balance policy evaluation must use the immutable snapshot view; it must not re-read live distribution managers while planning. Existing policy behavior can be preserved through snapshot-backed adapters, but the existing `BalanceReplica(ctx, replica)` interface is insufficient as the epoch planner contract because its implementations read mutable global managers internally.

The epoch-facing planning interface is:

```go
type EpochBalancePolicy interface {
    Plan(
        snapshot *PlacementSnapshot,
        budget BalanceWaveBudget,
        constraints EpochPlanningConstraints,
        config EpochPolicyConfig,
    ) BalanceWave
    Evaluate(
        snapshot *PlacementSnapshot,
        projected *ProjectedPlacement,
        kind PlanKind,
        config EpochPolicyConfig,
    ) ScorePotential
}

type EpochPolicyConfig struct {
    GlobalRowCountFactor          float64
    DelegatorMemoryOverloadFactor float64
    CollectionChannelCountFactor  float64
    AutoBalanceChannel            bool
    StreamingServiceEnabled       bool
}

type EpochObjectConstraint struct {
    Object          BalanceObjectKey
    CollectionID    int64
    From            int64
    To              int64
    Class           ReservationClass
    ChargedNodes    []int64
    QuarantineUntil time.Time
}

type ReservationClass int

const (
    ReservationQuarantineOnly ReservationClass = iota + 1
    ReservationActiveTask
    ReservationAmbiguousCapacity
)

type EpochPlanningConstraints struct {
    Objects map[BalanceObjectKey]EpochObjectConstraint
}

type BalanceWaveBudget struct {
    MaxSegmentTasks       int
    MaxChannelTasks       int
    MaxTasksPerNode       int
    MaxTasksPerCollection int
}

type PlanKind int

const (
    PlanKindSegment PlanKind = iota + 1
    PlanKindChannel
)

type EpochPlan struct {
    Kind         PlanKind
    CollectionID int64
    ReplicaID    int64
    Shard        string
    SegmentID    int64
    Channel      string
    Scope        querypb.DataScope
    RowCount     int64
    From         int64
    To           int64
    Token        AdmissionToken
}

type ScorePotential struct {
    Value float64
}

type BalanceWave struct {
    Kind      PlanKind
    Plans     []EpochPlan
    PrefixAfter []ScorePotential
    Decisions []BalancePlanDecision
    Before    ScorePotential
    After     ScorePotential
    Converged bool
}

type BalancePlanDecision struct {
    Plan        EpochPlan
    Before      ScorePotential
    After       ScorePotential
    Result      string
    Explanation string
}

type BalanceObjectKey struct {
    Kind      BalanceObjectKind
    ReplicaID int64
    SegmentID int64
    Channel   string
    Scope     querypb.DataScope
}

type BalanceObjectKind int

const (
    BalanceObjectSegment BalanceObjectKind = iota + 1
    BalanceObjectChannel
)

func (p EpochPlan) ObjectKey() BalanceObjectKey
func (p EpochPlan) NodeActions() []int64

func NewWaveLedger(
    budget BalanceWaveBudget,
    constraints EpochPlanningConstraints,
) *WaveLedger

func (l *WaveLedger) TryReserve(plan EpochPlan) bool
func (l *WaveLedger) Release(plan EpochPlan)

func (p *ProjectedPlacement) Apply(plan EpochPlan) error
func (p *ProjectedPlacement) Undo(plan EpochPlan)
```

The first implementation provides snapshot-backed adapters only for the configured names `ScoreBasedBalancer` and `ChannelLevelScoreBalancer`. Other configured balancers continue on the legacy path. `EpochPolicyConfig` is captured once when the checker tick creates the request and is frozen with a running generation; policy code does not reread dynamic global configuration during planning. All accepted candidates are applied to one `ProjectedPlacement`, so later candidates see the effects of earlier candidates from the same RG wave.

Streaming Service support is intentionally narrower than non-streaming support. `ChannelLevelScoreBalancer` always falls back to legacy while Streaming Service is enabled. `ScoreBasedBalancer` also falls back when Streaming Service is enabled and `autoBalanceChannel=true`; it remains epoch-capable for segment-only planning when Streaming Service is enabled and `autoBalanceChannel=false`. These boundaries prevent active mode from claiming equivalence with live-manager streaming branches that the snapshot policy does not implement.

The budget is a hard maximum. Plan generation stops or trims deterministically when the remaining budget is exhausted. A future MEP may add `MaxLoadBytes`, `MaxMemoryReservation`, and `MaxDiskReservation` without changing epoch lifecycle semantics.

`MaxTasksPerNode` counts placement actions by the node whose placement changes: a move consumes one Grow slot on the target and one Reduce slot on the source. It does not count the shard leader used as the RPC routing hop. Existing executor concurrency remains a separate runtime limit.

Distribution-aware scheduler pending work affects the projected row/channel score. Manager-owned ambiguous carry-over additionally locks its replica-scoped object and charges its source/target nodes against `MaxTasksPerNode`; quarantine locks the object without charging a node. Ordinary external pending tasks continue to rely on the scheduler's existing deduplication and generation fence rather than being represented as new-wave ledger reservations. Within a wave:

1. later plans see the projected effects of earlier accepted plans;
2. the same replica-scoped segment or channel object cannot appear in multiple plans;
3. destination eligibility is checked against projected state and reservations;
4. plans have deterministic ordering; and
5. the planner records why each plan was selected or skipped.

`WaveLedger` stores kind counters, per-node action counters, per-collection counters, and a set of `BalanceObjectKey` locks. `TryReserve(plan)` first validates that plan fields and `AdmissionToken` identity agree exactly: collection, replica, source, kind, segment/channel key, and scope must match. Reservation is all-or-nothing across kind, collection, both node endpoints, and the replica-scoped object lock. A move consumes one segment/channel task slot but one node-action slot on both source and target. Zero or negative configured limits admit no new work. The ledger stores a private immutable `planMutationIdentity`; `Release` compares the supplied plan with that identity and reverses the stored collection, endpoints, kind, counters, and lock exactly once. Mutating a caller-owned plan cannot redirect release bookkeeping.

The manager materializes `EpochPlanningConstraints` from ambiguous carry-over and active quarantine history. The policy never consults mutable manager state directly. `ReservationActiveTask` is supported by the ledger contract for callers that supply an active-task constraint, while the current manager represents scheduler-active effects in `PendingWork` and emits the other two classes. Charging is explicit:

| Reservation class | Object lock | New-wave kind/collection cap | Node capacity reservation | Workload projection |
|---|---:|---:|---:|---|
| `ReservationQuarantineOnly` | Yes | No | No | Observed distribution only |
| `ReservationActiveTask` | Yes | No | Action endpoints still pending | Distribution-aware pending effects |
| `ReservationAmbiguousCapacity` | Yes | No | Conservative source/target endpoints | Keep observed source workload and reserve possible target workload |
| New plan reserved by `WaveLedger` | Yes | Yes | Source and target | Apply projected move |

New-wave task and per-collection caps limit admissions created by this generation; unresolved older work does not consume those kind/collection caps. Carry-over remains bounded by object locks and per-node charges, so one ambiguous channel does not block unrelated channel work when the involved nodes still have budget. Planning uses an internal ledger to construct the wave. Admission creates a fresh ledger from the same frozen constraints and reserves each plan again; a rejection releases that plan and stops the suffix, while accepted plans stay reserved until the generation reconciles.

`ProjectedPlacement.Apply(plan)` applies the same plan/token identity validation, then verifies source presence and target absence before removing exactly one source placement and adding one target placement without reading live managers. Source missing, target already present, duplicate object application, or partial mutation returns an error and leaves projection unchanged. The apply record stores both the immutable mutation identity and the exact pre-mutation placement slice. `Undo` requires the same identity, reverses the stored mutation rather than trusting mutable plan fields, is valid exactly once, and restores the byte-for-byte prior projection. If reservation succeeds but projection or score validation fails, the planner undoes the projection and releases the reservation before considering another candidate.

The MVP preserves channel-first policy semantics: if an RG wave contains any legal improving channel move, it does not also add segment moves. Both plan types still share the same epoch and hard budget. Repeated channel waves therefore cannot cause an unrelated, overlapping normal epoch to start before reconciliation.

### Convergence criterion and wave-level benefit

Per-object benefit checks are not sufficient for an epoch. Multiple moves can each look beneficial against their local source and target while the complete wave over-corrects or produces little RG-level improvement.

Every snapshot-backed policy adapter exposes one `ScorePotential`. `ScorePotential.Improves` implements the single strict comparison `candidate.Value + 1e-9 < current.Value`. The same comparison is used when simulating an individual candidate, accepting the complete wave, and deciding convergence.

The planner evaluates:

```text
before = policy.Evaluate(observed snapshot, observed projection, kind, frozen config)
after  = policy.Evaluate(observed snapshot, projected placement, kind, frozen config)
```

A normal wave is eligible for admission only when `after.Value + 1e-9 < before.Value`. Planning, admission gating, and the `Converged` decision use this same strict-decrease rule; the MVP does not define a second policy-specific improvement threshold.

An RG is converged for the current normal policy when:

1. there is no recovery or hard-placement violation in the RG;
2. no legal wave within the configured hard budget passes the strict potential-decrease comparison; and
3. there is no ambiguous carry-over still awaiting reconciliation; quarantined known-failure objects may remain excluded and observable until their backoff expires.

The first implementation derives this objective from the existing ScoreBased/ChannelLevelScore workload and domain semantics, but active epoch mode intentionally does not reuse the legacy per-candidate `scoreUnbalanceTolerationFactor`, `reverseUnBalanceTolerationFactor`, or balance-cost tolerance gates. Those local gates are replaced by the one strict Lyapunov-style potential-decrease rule above. Legacy fallback continues to use the legacy balancer unchanged.

The snapshot's `MemoryCapacity` metadata is used only to allocate each eligible node's assigned-score quota when every target node reports a positive capacity. The current score is still derived from row and channel workload; the MVP does not observe or score current resident-memory usage, disk usage, loading bytes, or per-segment byte size. Memory/disk/GPU/schema-aware cost and migration byte cost remain follow-up policy dimensions built on the same epoch boundary.

For each eligible domain and node, the segment score reproduces the existing policy from snapshot primitives:

```text
globalRows     = historical rows + growing rows + pending/carry-over row effects
collectionRows = rows for this domain's collection + pending/carry-over effects
currentScore   = float64(collectionRows
                 + int(float64(globalRows) * GlobalRowCountFactor))

assignedScore  = capacity-weighted quota when every node has positive MemCapacity
                 otherwise equal share

currentScore  += assignedScore
                 * DelegatorMemoryOverloadFactor
                 * delegatorCount(collection, node)

domainPotential = sum((currentScore - assignedScore)^2)
```

Channel score preserves the existing split semantics. Resident channels belonging to the current collection use `max(1, CollectionChannelCountFactor)` and other resident channels use weight `1`. Pending channel actions are then added as `globalPendingDelta + currentCollectionPendingDelta`; they are not multiplied by `CollectionChannelCountFactor`.

Physical workload is deduplicated by collection/object/node before scoring, so replica aliases do not multiply historical rows, growing rows, or resident channel counts. If the same physical move candidate is associated with more than one replica in the captured snapshot, the MVP excludes that candidate conservatively instead of risking duplicate movement under ambiguous replica ownership.

`ChannelLevelScoreBalancer` creates one exclusive domain per `(replica, channel)` when every Current-target channel has channel RW-node assignments; outgoing RO placements remain valid sources. Otherwise it falls back to one replica-wide ScoreBased domain. In the MVP, `ChannelLevelScoreBalancer` active epoch mode is unsupported while Streaming Service is enabled because the legacy implementation first performs ScoreBased channel balancing and then uses different exclusive-loop rules. That combination falls back to legacy until a snapshot-backed streaming branch is specified and tested.

Domains are sorted by collection, replica, and channel. Candidate objects are sorted by source overload and then stable object ID. Targets are sorted by projected deficit and then node ID. These rules make the same snapshot, budget, constraints, and frozen policy configuration produce the same wave.

Each candidate is reserved, applied, and evaluated against the current projection. A candidate that does not strictly reduce potential is undone immediately. After all candidates, the complete wave is evaluated again against the observed snapshot; a non-improving aggregate wave is discarded even when individual moves had looked useful. Under static topology, target, and workload, every admitted prefix therefore strictly decreases a non-negative scalar potential. The sequence cannot admit an infinite static zero-net cycle and reaches a fixed point when no legal improving plan remains; the final potential need not be mathematically zero.

Cross-epoch reverse cooldown is not required by this design. If the reconciled snapshot and the shared objective show that a reverse move is beneficial, the move may be legitimate after topology or workload changes. The #51244 convergence fixture records the move sequence and rejects static zero-net reversals; a dedicated reverse-move metric or cooldown is follow-up work only if non-improving reversals remain after wave-level gating.

### Policy boundary and resource-aware follow-up

The epoch solves when a decision is allowed to act and how its result is reconciled. It does not, by itself, make row count a sufficient resource model.

The current ScoreBased adapters preserve the existing row/channel workload dimensions and placement domains while replacing their local candidate tolerances with the epoch-wide potential gate. A follow-up resource-aware policy should consume the same immutable snapshot and add:

- observed or estimated resident memory, disk footprint, and loading bytes per segment;
- schema/index-aware segment cost rather than assuming equal cost per row;
- collection-level fairness so one collection is not concentrated while the RG appears globally balanced;
- small-cluster spread objectives and large-cluster shard-aware placement domains;
- hard node resource limits and soft collection/segment-count fairness; and
- explicit migration-byte cost in the objective and admission budget.

That policy must still obey the epoch invariants: one RG snapshot, one projected placement, one hard reservation ledger, strict whole-wave improvement, and distribution-based reconciliation. Keeping policy and orchestration separate prevents a new scoring model from hiding control-loop races.

### Admission

Planning does not change scheduler state. A task belongs to the epoch only after scheduler admission succeeds.

For every plan, admission revalidates at least:

- the active epoch ID for the RG;
- RG membership of source and target nodes;
- replica membership and version;
- target version;
- segment or channel identity and expected source;
- shard/domain eligibility required by the configured policy;
- source presence for a move;
- target node state and resource-exhaustion penalty;
- scheduler deduplication; and
- remaining epoch budget.

These checks belong to four explicit boundaries:

| Boundary | Responsibility |
|---|---|
| Snapshot-only policy | Domain legality, source/target choice, deterministic ordering, and objective improvement. |
| RG epoch manager | Current epoch identity, object lock, hard budget, and reservation before calling the scheduler. |
| `PlacementSnapshotBuilder.Validate` | RG/node/replica/target/leader token and expected source presence. |
| Scheduler held admission | Atomic RG-scoped pending-generation check, collection-lock ordering, resource deduplication, existing leader/source checks, ID allocation, replacement, indexes, deltas, task statistics, metrics, and queue visibility. |

The scheduler gateway does not own the RG wave ledger. The manager reserves before admission and releases the reservation on any rejected result; only accepted tasks retain persistent budget and object locks.

All epoch-generated normal-balance tasks use one typed admission gateway. Stopping and recovery tasks remain on the legacy path in the MVP; integrating them with a common admission class is explicitly deferred.

The base held gateway does not change the public `Scheduler` interface. A generation-aware optional gateway extends it while preserving the compatibility interface:

```go
type BalanceAdmissionResult struct {
    TaskID          int64
    Reason          BalanceAdmissionReason
    Err             error
    PendingRevision BalancePendingRevision
}

type BalanceAdmissionValidator func() BalanceAdmissionReason

type BalanceTaskAdmitter interface {
    AdmitBalanceTask(task Task, validate BalanceAdmissionValidator) BalanceAdmissionResult
}

type BalanceTaskGenerationAdmitter interface {
    AdmitBalanceTaskAtPendingRevision(
        task Task,
        expected BalancePendingRevision,
        validate BalanceAdmissionValidator,
    ) BalanceAdmissionResult
}
```

Active epoch mode requires `BalanceTaskGenerationAdmitter`; the base `BalanceTaskAdmitter` remains for compatibility and tests. Generation-aware admission acquires the existing collection lock, runs the caller's topology/source validator, performs typed scheduler validation and deduplication, and runs the caller validator a second time. It then resolves affected RGs, acquires `scheduleMu`, determines any lower-priority replacement, and acquires `pendingMu` in that order. Under `pendingMu`, it compares `expected.EffectiveRevision()` with the current RG effective revision, removes the replacement from pending indexes, registers the new task, increments affected RG and epoch revisions, and returns the next `PendingRevision`.

Replacement bookkeeping is committed before slow finalization. `pendingMu` is released, then `applyTaskRemovalPenalty` and `cancelReplacementLocked` execute while `scheduleMu` still excludes dispatch. `scheduleMu` is released before `finishTaskAddition` publishes task statistics/metrics/logging and before `finishReplacement` runs target refresh, task-error cache updates, latency accounting, and removal logging. This preserves one atomic pending-generation boundary while keeping callback-capable cancellation out of `pendingMu` and the later finalizers out of `scheduleMu`.

The manager replaces the remaining wave token with `result.PendingRevision` after every accepted task. Same-epoch successful commits increase both absolute RG and epoch revisions, leaving the effective revision stable for the wave. Legacy adds, failures, removals, and other same-RG mutations increase only the effective revision and reject the next admission. Epoch revision entries are pruned after the epoch's final active task is removed; the absolute RG revision still keeps old tokens stale. The task receives no ID and is absent from dispatcher queues, indexes, deltas, task statistics, and metrics before all final checks succeed.

Stable reasons are `accepted`, `duplicate`, `source_gone`, `leader_missing`, `replica_changed`, `resource_group_changed`, `target_changed`, `node_ineligible`, `budget_exhausted`, `stale_epoch`, and `internal_error`. During admission, leader, replica, RG, target, node-eligibility, stale-epoch, and internal failures stop the suffix as superseding conditions; duplicate, source-gone, and budget rejection stop the suffix as degraded/local conditions. In every case the accepted prefix remains admitted and the unattempted suffix is reported separately.

No QueryNode action can be dispatched between the final token validation and admission commit. A topology change after commit is ordered after admission and is handled by epoch invalidation and distribution-based reconciliation.

Admission errors must be typed. Epoch state transitions must not parse error strings to distinguish a duplicate from a stale source or changed topology.

The epoch tracks separate collections of plans and tasks:

```text
planned  = planner output
admitted = AdmissionReason == AdmissionAccepted
rejected = any other typed AdmissionReason
```

Only admitted tasks consume persistent new-wave budget and appear in epoch completion accounting. Carry-over work is accounted separately before the budget is calculated.

Admission processes plans in the same deterministic order used during projected simulation. The first rejection stops the rest of the wave. The already accepted prefix remains valid because every accepted candidate strictly improved the projection produced by the preceding accepted prefix. Continuing with a later plan would be unsafe: that plan was scored against a projection containing the rejected move. Rejected and unattempted plans are released and reconsidered from a fresh snapshot in the next generation.

`BalanceWave.PrefixAfter[i]` is the potential after plans `0..i`. If admission accepts `k` plans before stopping, `EpochAdvanceResult.ObjectiveAfter`, objective metrics, and #51244 assertions use `Before` when `k=0` or `PrefixAfter[k-1]` otherwise. They never report the uncommitted full-wave `After` value after a partial admission.

Tasks admitted by an epoch carry metadata identifying the RG and epoch ID. This metadata is diagnostic and does not replace the scheduler's existing resource deduplication keys.

### Execution invariants

The epoch layer does not execute QueryNode RPCs directly. Admitted tasks continue through the existing scheduler and executor.

The following invariants must be preserved:

1. A move performs Grow before Reduce.
2. Reduce is not dispatched until authoritative distribution confirms the target copy.
3. For a channel move, the Grow-to-Reduce transition additionally waits until the new target is observed as the replica's shard leader/delegator.
4. RPC success alone does not complete an action.
5. An action with an ambiguous RPC outcome remains locked until distribution reconciliation.
6. The epoch planner never mutates distribution or manufactures pending-task deltas.

The implementation adds typed outcome markers at the existing session and executor boundaries without coupling the epoch manager to the mutable executor action step.

`session.QueryCluster.send` is the boundary that knows whether the QueryNode client callback was invoked. For the four balance RPCs—`LoadSegments`, `ReleaseSegments`, `WatchDmChannels`, and `UnsubDmChannel`—a failure returned before callback invocation is wrapped with `session.NewRPCNotSentError`. The wrapper preserves the original cause through `Unwrap`; because the request was not dispatched, this is a definitive local failure.

After dispatch, `task.checkBalanceRPCCall` applies one rule to Segment Grow/Reduce and Channel Grow/Reduce:

- an `RPCNotSentError` stays definitive;
- a raw Go/gRPC error after dispatch is wrapped with `task.NewAmbiguousExecutionError`, because QueryNode may have applied the operation before the response was lost; and
- an explicit non-OK protobuf status returned without a raw transport error is definitive, because QueryNode returned an application response.

The epoch manager still observes only `Task.Done()`, `Task.Status()`, `Task.Err()`, and authoritative Segment/Channel distribution. `Task.Fail` stores the plain task error before closing `Done`. Reconciliation first observes the closed channel and only then calls `Task.Err()`, so the channel close establishes the happens-before edge for that error read. A `Failed` status visible before `Done` closes is treated as ambiguous instead of reading an unsynchronized error or declaring a definitive Grow/Reduce failure.

Classification is deliberately conservative:

- target present and ready while source is absent is completed regardless of the task label;
- `Started` or `Canceled` with any other placement is ambiguous carry-over;
- `Failed` before `Done` closes is ambiguous carry-over;
- a completed `Failed` task with `AmbiguousExecutionError` is ambiguous carry-over for every non-desired placement;
- a completed definitive `Failed` task with source present and target absent is a known Grow failure;
- a completed definitive `Failed` task with both copies present is a known Reduce failure;
- a completed definitive `Failed` task with neither copy present is lost placement and therefore degraded; and
- `Succeeded` without the desired authoritative placement is still ambiguous.

For a channel, target readiness additionally requires a serviceable target record, a nonzero leader equal to the target node, and a leader target version at least as new as the frozen Current-target version. Task terminality or RPC success never substitutes for placement readiness.

### Epoch state machine

The state machine is tick-driven. It does not create a long-lived epoch goroutine. `BalanceChecker.Check()` performs at most one active-generation advancement per RG in a tick through the interface below. An inactive RG with retained retry history may first receive an observation-only call and later a new active or shadow planning call in the same tick; no two calls can own concurrent generations because `HasActive`, the tick-start set, and the per-RG `TryLock` enforce ownership.

```go
type EpochRequest struct {
    ResourceGroup      string
    EligibleReplicaIDs []int64
    Balancer           string
    Budget             BalanceWaveBudget
    PolicyConfig       EpochPolicyConfig
    AllowNew           bool
    Shadow             bool
    Deadline           time.Duration
    NoProgressDeadline time.Duration
    SegmentTaskTimeout time.Duration
    ChannelTaskTimeout time.Duration
    MaxObjectRetries   int
    QuarantineBackoff  time.Duration
}

type BalanceEpochController interface {
    Advance(context.Context, EpochRequest) EpochAdvanceResult
    HasActive(resourceGroup string) bool
    ActiveResourceGroups() []string
    ResourceGroupsToObserve() []string
}

type EpochAdvanceResult struct {
    ResourceGroup    string
    Epoch            task.BalanceEpochMeta
    State            EpochState
    Planned          int
    Admitted         int
    Rejected         map[task.BalanceAdmissionReason]int
    Started          bool
    Terminal         bool
    Converged        bool
    ObjectiveBefore  float64
    ObjectiveAfter   float64
    Err              error
}

type EpochState int

const (
    EpochIdle EpochState = iota
    EpochPlanning
    EpochAdmitting
    EpochExecuting
    EpochReconciling
    EpochCompleted
    EpochDegraded
    EpochSuperseded
    EpochTimedOut
)
```

The manager uses a small global map lock only to obtain the per-RG runtime. Each RG runtime has its own mutex, sequence, active epoch, reservations, object locks, retry history, and carry-over work. `Advance` uses `TryLock`; a concurrent same-RG call returns the last published result instead of blocking or creating a second generation. A blocked RG-A therefore does not prevent RG-B from observing, planning, admitting, or reconciling.

`HasActive(resourceGroup)` and `ActiveResourceGroups()` report only the current generation while its active flag is retained. A terminal generation is cleared at the start of the next `Advance`; the checker remembers which RGs were active at tick start and therefore does not start another generation for that RG in the same tick. Ambiguous carry-over is consumed as a planning constraint by a later generation. Retry/quarantine history is exposed separately through `ResourceGroupsToObserve()` so it can be pruned or dynamically disabled without suppressing legacy balance when no generation is active.

The constructor makes mutable boundaries explicit and testable:

```go
func NewBalanceEpochManager(
    metadata *meta.Meta,
    dist *meta.DistributionManager,
    targetMgr meta.TargetManagerInterface,
    nodeMgr *session.NodeManager,
    scheduler task.Scheduler,
    admitter task.BalanceTaskGenerationAdmitter,
    inspector task.BalanceTaskInspector,
    source task.Source,
    policyProvider func(string, EpochPolicyConfig) (EpochBalancePolicy, bool),
    opts ...EpochManagerOption,
) *BalanceEpochManager

func WithEpochClock(now func() time.Time) EpochManagerOption
func WithLeaderTerm(term uint64) EpochManagerOption
func WithEpochTaskFactory(factory EpochTaskFactory) EpochManagerOption

type PlacementSnapshotSource interface {
    Build(context.Context, string, []int64, []task.PendingBalanceTaskSnapshot) (*PlacementSnapshot, error)
    Validate(AdmissionToken) task.BalanceAdmissionReason
}

func WithPlacementSnapshotSource(source PlacementSnapshotSource) EpochManagerOption
```

`Planning` and `Admitting` are synchronous transient phases inside the start call. The call may capture, plan, reserve, and admit a bounded wave, but it never waits for QueryNode task completion or sleeps for progress. `Executing` and `Reconciling` persist across checker ticks.

One checker tick performs this bounded advancement:

| Current condition | Tick action |
|---|---|
| No active epoch and `AllowNew=false` | Return `Idle`. |
| No active epoch and `Shadow=true` | Snapshot, plan, emit comparison data, and return without runtime state, locks, reservations, or admission. |
| No active epoch and `AllowNew=true` | Enter transient `Planning`/`Admitting`, then persist `Executing` or finish `Completed/Converged`. |
| `Executing` | Inspect `Task.Done`, task status, deadlines, and authoritative placement. |
| Quiescent, invalidated, or deadline reached | Enter `Reconciling`. |
| `Reconciling` | Classify all accepted objects, rebuild carry-over, then publish a terminal result. |
| Tick after a terminal result | Clear the old generation. The checker does not restart an RG that was active at tick start; a later tick may create a fresh generation. |

The manager checks the overall deadline before and after snapshot, planning, and each admission. If the deadline expires during a transient phase, it stops generating/admitting and reconciles the accepted prefix. Already admitted work is not assumed cancelled. Repeated checker triggers cannot overlap these phases because the per-RG runtime mutex covers the complete `Advance` call.

Balancer name, budget, policy factors, deadlines, task timeouts, retry count, and quarantine backoff are captured when a new generation starts and remain immutable for that generation. Dynamic configuration changes apply to the next generation; they do not change the objective or retry policy midway through an admitted wave. When no generation is active, an observation-only request may immediately clear retained retry/quarantine state if retries or backoff are dynamically disabled.

```text
Idle
  |
  v
Planning
  | snapshot accepted and plans generated
  v
Admitting
  | at least one task admitted
  v
Executing
  | wave reaches quiescence, no-progress deadline,
  | or scoped invalidation occurs
  v
Reconciling
  | resolved work is committed to observed state;
  | unresolved safe-settle work is carried forward
  +---------------------+----------------------+-------------------+
  |                     |                      |                   |
  v                     v                      v                   v
Completed             Degraded             Superseded           TimedOut
```

If planning produces no improving plan and the snapshot remains valid, the epoch may transition directly from `Planning` to `Completed` with the reason `Converged`.

Planning and admission edge cases are explicit:

- First admission is `Duplicate` or another local rejection: stop the wave, reconcile existing pending work, and replan from a fresh snapshot.
- A stale-snapshot rejection before any admission: transition to `Superseded` through reconciliation.
- Any rejection after partial admission: retain the validated accepted prefix, discard the remaining suffix, and reconcile before publishing the next generation.
- Deadline during `Planning` or `Admitting`: stop generation/admission and transition through reconciliation to `TimedOut`.
- During admission, RG/node/replica/target/leader generation invalidation or `StaleEpoch` stops the suffix and transitions toward `Superseded`; already admitted actions follow reconciliation rules.
- During execution, `StaleEpoch` is not a superseding signal because normal completion/removal of the epoch's own tasks advances the RG pending revision and may prune its epoch revision. Execution is reconciled from task state and authoritative placement.
- `LeaderHash` remains a strict admission fence, but leader publication is also non-superseding after admission: a successful channel Grow necessarily publishes a new leader, and delegators may republish while admitted work executes. RG, node, replica, and target changes remain superseding.

Terminal state definitions:

| State | Meaning |
|---|---|
| `Completed` | The wave reached its desired observed state. Every admitted object is either complete or observed satisfied. |
| `Degraded` | Some objects failed or were quarantined, but resolved placement and carried-forward work are known, so unrelated work may be replanned. |
| `Superseded` | A newer generation is required because a planning assumption changed. Unstarted plans are discarded and unresolved running objects are carried forward. |
| `TimedOut` | The no-progress or epoch deadline expired. Unresolved objects are carried forward or quarantined before the next generation. |

All terminal states permit a later epoch. None permits the next epoch to reuse the old projected placement.

### Quiescence and generation handoff

An empty scheduler queue is not sufficient to declare an epoch complete. The manager waits until every admitted or carried object is quiescent: either the desired authoritative placement is visible, the task status is no longer `Started`, or `Task.Done()` is closed. It then classifies all objects from one captured distribution, rebuilds the carry map, records known failures, and publishes a terminal result. An ambiguous action retains its object lock and conservative source/target node charges; a later snapshot receives synthesized pending actions for it.

The result determines `Completed`, `Degraded`, `Superseded`, or `TimedOut`. A stuck object does not block the whole RG indefinitely, but it cannot be moved again or have its reserved capacity reused while its outcome is uncertain.

Reconciliation applies this ordered placement/status matrix:

| Precedence | Task observation | Target/source presence | Classification and outcome |
|---:|---|---|---|
| 1 | Any status or error | Ready target present, source absent | Completed; desired authoritative placement wins, retry history is cleared, and the object constraint is released. |
| 2 | `Started`, `Canceled`, or `Failed` before `Done` is observed closed | Any other placement | Ambiguous carry-over; keep the object lock and charge both positive source/target endpoints. |
| 3 | Completed `Failed` task with `AmbiguousExecutionError` | Any non-desired placement | Ambiguous carry-over; a post-dispatch raw error is not proof of the remote side effect. |
| 4 | Completed definitive `Failed` task | Target absent, source present | Known Grow failure; retain source, record retry history, finish degraded. |
| 5 | Completed definitive `Failed` task | Target present, source present | Known Reduce failure/redundant copy; record retry history, finish degraded, and let existing cleanup logic resolve the extra copy. |
| 6 | Completed definitive `Failed` task | Target absent, source absent | Lost placement; record retry history and force `Degraded`, even if the prior intent was timeout or supersession. |
| 7 | Completed definitive `Failed` task | Target present, source absent but target not ready | Ambiguous until target readiness becomes authoritative. |
| 8 | `Succeeded` or any other status | Desired placement not observed | Ambiguous carry-over; task success alone is not authoritative. |

Only accepted scheduler admissions enter this table. Rejected plans release their provisional wave reservation immediately and do not increment admitted-task accounting.

### Epoch deadline

The epoch owns an explicit control-loop deadline. `EpochRequest` also carries the existing segment and channel task timeout settings into `NewSegmentTask` and `NewChannelTask`, but those constructors currently ignore their `timeout` argument: `newBaseTask` derives only `context.WithCancel`, not `context.WithTimeout`. The epoch deadline is therefore the implemented bound on planning/admission/no-progress orchestration, independent of per-task constructor timeout behavior.

When the deadline expires:

1. stop admitting new plans;
2. do not assume that any already admitted or dispatched RPC was cancelled;
3. reconcile every ambiguous Grow or Reduce against distribution;
4. release or rebuild reservations;
5. carry unresolved safe-settle tasks and their locks into the next snapshot; and
6. normally finish as `TimedOut`.

Terminal precedence is based on final observed state. If a deadline intent exists but every object reaches desired placement with no known failure or carry, reconciliation upgrades the result to `Completed`. Lost placement always forces `Degraded`. Otherwise unresolved carry retains `TimedOut`, while a superseding topology/target intent remains `Superseded`.

An in-flight Grow is not followed by Reduce unless target presence is confirmed. Only a definitive terminal Reduce failure can be classified as a redundant copy. A cancelled task or raw post-dispatch deadline, unavailable, or lost-response Reduce remains ambiguous and carries its lock/reservation until later distribution proves the result. A pre-dispatch `RPCNotSentError` or explicit non-OK response remains definitive. The next epoch may plan unrelated objects while this object remains locked.

### Scoped invalidation matrix

Runtime changes do not all invalidate the same scope:

| Event | Handling |
|---|---|
| RG node add/remove, RW-to-RO transition, or RG capacity/quota change | Hard-supersede the current RG generation because shared capacity assumptions changed. |
| Replica moved into or out of the RG | Supersede the affected RG generations. |
| Target version changed for one collection | MVP conservatively stops the current RG's remaining admission and reconciles admitted objects. A fresh generation may immediately continue unrelated collections. |
| Channel-exclusive mapping changed | MVP conservatively stops remaining RG admission and reconciles; finer replica/shard continuation within the same generation is follow-up work. |
| Expected distribution change caused by this epoch's task | Do not invalidate; reconcile it as expected feedback. |
| Stopping work is admitted by `BalanceChecker` | Do not start a new normal epoch in that tick; active epochs continue observation and reconciliation. |
| Independent recovery task changes pending work | During admission, the effective pending revision rejects the remaining suffix. During execution, `StaleEpoch` is ignored and authoritative placement/task state owns reconciliation. The MVP has no same-tick recovery preemption signal. |
| Unrelated or same-RG placement publication without topology/target change | Do not automatically abort execution; ordinary `PlacementHash` changes are feedback, not supersession. |
| QueryNode resource exhausted | Stop remaining admission, reconcile accepted work, and rebuild the next generation with updated node eligibility/penalty. |
| Raw post-dispatch RPC timeout or temporary unavailable | Keep the object locked and reconcile within its bounded policy; do not automatically invalidate unrelated plans. |
| Pre-dispatch `RPCNotSentError` or explicit non-OK QueryNode status | Treat as a definitive task failure and apply the placement-specific Grow/Reduce classification. |
| Source resource disappeared | Mark the plan stale and reconcile that object. |
| `LeaderHash` changed before admission | Reject admission as `leader_missing` and stop the suffix as superseded. |
| Leader publication after admission | Do not supersede execution, including publication for an unrelated collection; channel handoff itself changes leader state. Reconcile using target readiness and task outcome. |
| QueryCoord restarts | The in-memory manager and its process-boot leader term are replaced; recovered distribution is replanned rather than replaying the old epoch. |

In particular, the implementation must not abort an epoch merely because a cluster-global distribution version changed. The epoch's own tasks and unrelated RGs both advance those versions.

### Failure handling

Epochs are not transactions. A successfully completed move is not rolled back because another move failed.

#### Grow/load failure

If Grow has a definitive failure and the target is absent from distribution:

- keep the source copy;
- mark the task failed;
- release its destination reservation;
- retain the task error and increment epoch-level object retry history; existing scheduler/node resource-exhaustion penalties remain unchanged; and
- allow independent tasks in the epoch to continue.

Definitive failures include an `RPCNotSentError` from before dispatch and an explicit non-OK QueryNode application status. The next epoch may choose a different target. Repeated failures trigger the existing node resource-exhaustion penalty when applicable and the quarantine policy below.

#### No-progress handling and quarantine

The epoch layer must prevent one permanently failing object from monopolizing the RG control loop.

- The first implementation does not add in-place scheduler action retries. Both definitive and ambiguous RPC errors terminate the scheduler task according to current behavior; typed outcome plus authoritative placement determines whether the epoch releases or carries the object.
- Epoch reconciliation classifies the result and a later generation may admit a newly planned task if the preconditions still make sense.
- Each object has a bounded number of consecutive known epoch-level failures before quarantine; attempts below the threshold are retained as retry history but are not delayed by a separate per-attempt backoff.
- The no-progress deadline sets a timeout intent and forces reconciliation. It does not by itself quarantine an ambiguous object.
- An object with an ambiguous in-flight RPC remains locked and reserved rather than retried.
- Quarantine/retry history is cleared when its backoff expires, when the frozen RG/replica/node/leader/target topology changes, or when retry/quarantine is dynamically disabled.
- Quarantined objects are reported as degraded placement, but they do not block unrelated collections, replicas, or shards.

Quarantine is a control-loop decision, not a declaration that the segment may be dropped. Existing availability and target checkers remain responsible for required copies.

#### Ambiguous Grow outcome

If a dispatched Grow RPC returns a raw timeout, `Unavailable`, or another transport error, the executor wraps it as `AmbiguousExecutionError`. The task must not immediately retry or replan the segment. Reconciliation checks target distribution:

- ready target present and source absent: desired placement wins even if the task carries an ambiguous error;
- target present with source retained: desired placement is not reached, so keep the handoff locked as ambiguous carry-over until authoritative source disappearance completes it;
- Grow has a definitive terminal failure, target absent, source present: treat Grow as failed and release the destination reservation; and
- task not terminal, `Failed` not yet synchronized through `Done`, or completed Grow outcome is typed ambiguous with target absent: keep the object lock and conservative reservation as ambiguous carry-over. Absence at one observation point is not proof that an already-dispatched Grow cannot still become visible.

#### Reduce failure

If target presence was confirmed and Reduce has a definitive terminal failure, the segment may exist on both source and target. This is availability-safe but consumes extra resources. A non-terminal or cancelled Reduce remains ambiguous. A raw post-dispatch timeout, `Unavailable`, or lost-response error is typed ambiguous even after the task fails; it retains the lock and both node charges until authoritative source disappearance completes the move. An `RPCNotSentError` or explicit non-OK QueryNode status remains definitive.

The epoch records a partial completion and ends as `Degraded` after reconciliation. The MVP does not add a standalone cleanup plan type: existing target/availability checkers decide which copy to remove. Normal epoch policy keeps the object locked for the current handoff and must not generate a reverse move merely because Reduce failed.

#### Node failure

A node failure invalidates the RG snapshot and immediately stops normal admission:

1. stop admission and move the current generation to `Reconciling`;
2. stop new normal admission;
3. leave admitted actions to distribution-based reconciliation;
4. run or allow the existing higher-priority recovery path for missing channels and segments;
5. rebuild authoritative RG state; and
6. end the normal epoch as `Superseded`.

Recovery does not wait for the normal epoch to reach its original deadline.

#### Shard leader publication

The admission token freezes `LeaderHash`, so a leader change before commit rejects the suffix. After admission, leader publication is mutable execution-plane state rather than an epoch-generation fence: a successful channel Grow must publish the target as leader, and any delegator may republish while Segment work is running. Execution-time `leader_missing` is therefore ignored for supersession. Channel reconciliation still requires the target record to be serviceable, led by the target node, and at least at the frozen target version; scheduler safety checks continue to protect action dispatch.

#### Target, replica, or RG membership change

A target version change, replica topology change, or node-eligibility change stops the current RG's remaining admission and supersedes execution. A pre-admission leader change also stops the suffix; a post-admission leader publication does not. An RG membership or capacity change supersedes the whole generation. Unadmitted suffix plans are discarded; admitted tasks are reconciled from actual placement and task status. A fresh generation can continue unrelated work from a new snapshot. Recovery and target-consistency checkers retain higher priority.

### Normal versus recovery priority

Normal balance is opportunistic. Stopping balance and recovery protect availability and topology correctness and remain higher priority.

The MVP deliberately does not introduce a recovery admission class or route stopping/recovery task producers through `BalanceEpochManager`. `BalanceChecker` first runs the existing stopping-balance path. If stopping work is admitted, it does not create a new normal epoch in that tick. Existing recovery and availability checkers continue to use their current scheduler paths.

An active normal epoch is never abandoned merely because new normal work is disabled. Checker deactivation, `autoBalance=false`, feature disablement, unsupported policy selection, or stopping-balance precedence set `AllowNew=false`; the manager still observes and reconciles active tasks and carry-over locks.

Topology and distribution changes caused by recovery are observed through the snapshot token and authoritative distribution. They may supersede the normal generation, but the MVP does not promise atomic normal-versus-recovery reservation replacement or epoch-scoped cancellation.

A later recovery-integration MEP may add:

- explicit recovery and normal admission classes;
- object-scoped preemption of not-started normal tasks;
- shared RG capacity reservation across recovery and normal work; and
- finer-grained shard/replica invalidation events.

### QueryCoord restart

Balance epochs are in-memory control-loop state and are not persisted.

After QueryCoord restart:

1. recover target, replica, RG, and QueryNode distribution using existing recovery paths;
2. establish a new leader term and ignore delayed in-process events carrying an older term;
3. discard old scheduler-task, epoch, object-lock, reservation, and projected-placement state because it was not persisted;
4. treat any redundant or missing copies visible in recovered distribution as fresh repair input; and
5. create new epochs only after initial distribution recovery completes.

This avoids treating an old projected plan as durable desired state. Target metadata and observed QueryNode distribution remain the sources of truth.

An old leader's already-dispatched RPC may still take effect after the new leader starts; `LeaderTerm` cannot prevent that external side effect. A later distribution pull observes it, and the new leader's recovery/redundancy logic reconciles the resulting actual placement.

### Implemented components

#### BalanceEpochManager

Responsibilities:

- owns the current planning generation and carry-over work map keyed by RG;
- serializes planning and admission within one RG;
- permits epochs in different RGs to proceed concurrently;
- advances through checker ticks rather than a dedicated goroutine;
- polls task completion and authoritative distribution;
- manages deadlines, supersession, quarantine, and terminal transitions; and
- exposes epoch status and metrics.

#### PlacementSnapshotBuilder

Responsibilities:

- captures immutable distribution, target, replica, RG, node, and pending-task state;
- validates the version tuple before publishing a snapshot; and
- retries or reports a transient failure when a consistent snapshot cannot be obtained.

#### EpochBalancePolicy and WaveLedger

Responsibilities:

- consumes only immutable snapshot data;
- invokes a snapshot-backed adapter for the configured channel/segment policy;
- simulates candidates in one shared projected placement;
- enforces the hard wave budget; and
- gates candidates, the whole wave, and convergence with one score potential.

#### Reconciliation inside BalanceEpochManager

Responsibilities:

- classifies task outcomes using scheduler and distribution state;
- resolves ambiguous RPC outcomes;
- rebuilds reservations;
- determines the terminal epoch state; and
- produces the authoritative input boundary for the next epoch.

#### Balance RPC outcome markers

Responsibilities:

- marks failures known to occur before QueryNode RPC dispatch as `RPCNotSentError` at the session boundary;
- marks raw post-dispatch transport/gRPC errors as `AmbiguousExecutionError` at the balance executor boundary;
- keeps explicit non-OK QueryNode application statuses definitive; and
- preserves original causes through `Unwrap` for diagnostics and existing error handling.

### Delivery map

| MEP component | Milvus package/file |
|---|---|
| Epoch/task identity, typed admission, pending generations, task completion | `internal/querycoordv2/task/balance_admission.go`, `task.go`, `scheduler.go` |
| Balance RPC dispatch and execution outcome typing | `internal/querycoordv2/session/rpc_outcome.go`, `cluster.go`, `internal/querycoordv2/task/execution_outcome.go`, `executor.go` |
| Atomic QueryNode segment/channel publication and capture | `internal/querycoordv2/meta/dist_manager.go`, `segment_dist_manager.go`, `channel_dist_manager.go`, `internal/querycoordv2/dist/dist_handler.go` |
| Immutable placement snapshot and admission validation | `internal/querycoordv2/balance/epoch_types.go`, `epoch_snapshot.go` |
| Hard wave budget, object locks, projected apply/undo | `internal/querycoordv2/balance/epoch_wave.go` |
| Snapshot-only ScoreBased/ChannelLevel policy and strict potential | `internal/querycoordv2/balance/epoch_score_policy.go`, `balancer_factory.go` |
| Per-RG tick-driven state machine and reconciliation | `internal/querycoordv2/balance/epoch_manager.go` |
| Checker orchestration, drain, shadow, legacy fallback, fair collection cursor | `internal/querycoordv2/checkers/balance_checker.go` |
| Runtime construction | `internal/querycoordv2/server.go` |
| Dynamic rollout configuration | `configs/milvus.yaml`, `pkg/util/paramtable/component_param.go` |
| Prometheus collectors | `pkg/metrics/querycoord_metrics.go` |
| Deterministic unit, state-machine, checker, and #51244 fixtures | matching `_test.go` files in the packages above |

### Integration with BalanceChecker

`BalanceChecker` remains a trigger and eligibility component. In active epoch mode with a supported policy, it no longer submits normal balance tasks directly. Disabled mode, shadow mode, unsupported policies, or missing optional scheduler capabilities retain the legacy normal-balance path.

The implemented flow is:

```text
BalanceChecker tick/manual trigger
    -> run legacy stopping balance
    -> observe every active or retained-state RG with AllowNew=false
    -> determine whether a new normal epoch is allowed
    -> group eligible replicas by RG deterministically
    -> BalanceEpochManager.Advance(request)
```

If an RG already has a current normal planning generation, repeated periodic triggers are coalesced. They do not create another concurrent planner for the RG. A trigger received during reconciliation may request the next generation after resolved and carry-over state has been published.

Stopping balance continues on its existing path and retains precedence. Node removal or another RG-wide capacity change is observed as a token change and supersedes the current normal generation through reconciliation.

`NewBalanceChecker` keeps its public signature. It type-asserts the concrete scheduler to both `task.BalanceTaskGenerationAdmitter` and `task.BalanceTaskInspector`; active and shadow epoch planning are unavailable unless both capabilities exist.

Each tick follows this order:

1. read dynamic configuration;
2. run legacy stopping balance;
3. call `Advance(...AllowNew=false)` once for every RG returned by `ResourceGroupsToObserve()`, including active generations and retained retry/quarantine history, regardless of feature, checker, or auto-balance enablement;
4. if stopping work was admitted, do not start a new normal epoch;
5. evaluate checker activation, auto-balance, interval, active/shadow mode, and policy support;
6. collect eligible collections/replicas, group them by RG, and sort collection IDs, replica IDs, and RG names; and
7. call `Advance(...AllowNew=true)` for permitted RGs, or use the legacy normal path when no active epoch needs draining.

When `BalanceCheckCollectionMaxCount` truncates the eligible collection set, the checker uses a persisted round-robin cursor over the sorted collection IDs, wraps at the end, and advances only after producing a normal request. This preserves deterministic ordering without permanently starving collections outside a fixed sorted prefix.

| Mode/condition | Existing active epochs | New normal work | Legacy normal balance |
|---|---|---|---|
| `enabled=true`, supported policy, capabilities available | Reconcile/advance | Start RG epoch | Suppressed |
| `enabled=false`, `shadowMode=true`, no active generation | Observe retained retry history, if any | Stateless shadow snapshot/plan | Continues in the same eligible tick |
| `enabled=false`, `shadowMode=true`, active generation exists | Reconcile/advance | No shadow until active generations drain | Suppressed until no active generation remains |
| Feature disabled, checker active, `autoBalance=true` | Reconcile/advance | Forbidden | Resumes after active generations drain |
| Checker inactive or `autoBalance=false` | Reconcile/advance | Forbidden | Forbidden |
| Unsupported policy or missing optional capability with normal balance enabled | Reconcile/advance if owned state exists | Forbidden | Used after drain |
| Stopping work admitted this tick | Reconcile/advance | Forbidden | No new normal work this tick |

Rows are evaluated in this priority order: accepted stopping work and active-generation drain; checker/auto-balance/interval gate; active epoch mode; shadow mode; legacy fallback. If both `enabled` and `shadowMode` are true, active mode wins and no shadow/legacy plan is produced. Retained retry history without an active generation is still observed, but it does not by itself suppress shadow or legacy normal balance.

Shadow planning is stateless but not silent. If it computes a wave and the deadline or context becomes terminal immediately afterward, the returned result and `shadow=true` log retain the planned count and observed/projected objectives, and the shadow plan/objective metrics are still published. Shadow never calls generation admission or creates an active runtime.

Legacy `submitTasks` must count only successful `Scheduler.Add` calls and log every admission error. Generated task slice length is not accepted-work accounting.

### Observability

The MVP registers these Prometheus collectors:

| Metric | Collector and label contract |
|---|---|
| `milvus_querycoord_balance_epoch_active{resource_group,state}` | `GaugeVec`; state is `planning`, `admitting`, `executing`, or `reconciling`. A transition deletes the previous state label before setting the new one; terminal publication deletes the last active-state label. |
| `milvus_querycoord_balance_epoch_total{resource_group,result}` | `CounterVec`; active-generation result is `converged`, `completed`, `degraded`, `superseded`, `timed_out`, or `snapshot_error`, published exactly once. Stateless shadow runs do not increment it. |
| `milvus_querycoord_balance_epoch_plans_total{resource_group,kind,result}` | `CounterVec`; kind is `segment` or `channel`; result is `planned`, `reserved`, `rejected`, `unattempted`, or `shadow`. |
| `milvus_querycoord_balance_epoch_admission_total{resource_group,reason}` | `CounterVec`; reason uses the stable `BalanceAdmissionReason.String()` values. Shadow does not admit and therefore does not increment it. |
| `milvus_querycoord_balance_epoch_snapshot_retries_total{resource_group}` | `CounterVec`; increment once at the start of every optimistic attempt after the first. |
| `milvus_querycoord_balance_epoch_objective{resource_group,phase}` | `GaugeVec`; phase is `observed`, `projected`, `committed`, or `shadow`. Values are replaced with the latest applicable generation/run; an accepted prefix updates `committed`. |
| `milvus_querycoord_balance_epoch_carry_over{resource_group,kind}` | `GaugeVec`; kind is `segment` or `channel`. It counts active admitted objects, ambiguous carry-over, and unexpired quarantine. A zero count deletes that label; retained below-threshold retry history remains observable without appearing as carry. |
| `milvus_querycoord_balance_epoch_duration_seconds{resource_group,result}` | `HistogramVec` using `prometheus.DefBuckets`; one observation is published with each active terminal counter result. |

The MVP logs planned, admitted, rejected, terminal, timed-out, and carry-over outcomes with RG, epoch term/sequence, collection, replica, shard, segment/channel, source, target, admission reason, and objective values. More detailed redundant-copy age, ambiguous-RPC age, and recovery-preemption metrics are follow-up observability.

## Public Interfaces

This implementation does not change the Milvus user-facing API.

Internal interfaces are added or extended to support:

- atomic primitive distribution capture;
- immutable placement snapshots and admission tokens;
- RG and epoch metadata on balance tasks;
- `Task.Done()` terminal notification;
- optional distribution-aware channel delta and pending-task inspection;
- typed held admission on the concrete scheduler;
- snapshot-only planning and hard RG wave reservations; and
- a tick-driven `BalanceEpochController` used by `BalanceChecker`.

Configuration is dynamic and defaults to a non-disruptive rollout:

| Key | Default | Meaning |
|---|---:|---|
| `queryCoord.balanceEpoch.enabled` | `false` | Route supported normal policies through active epochs. |
| `queryCoord.balanceEpoch.shadowMode` | `false` | Snapshot and plan without typed admission; legacy normal balance still runs. |
| `queryCoord.balanceEpoch.deadline` | `120000` ms | Overall epoch deadline. |
| `queryCoord.balanceEpoch.noProgressDeadline` | `30000` ms | Reconcile when relevant placement makes no progress. |
| `queryCoord.balanceEpoch.maxSegmentTasks` | `5` | Hard segment-task cap; fallback order is `queryCoord.balanceSegmentBatchSize`, then deprecated `queryCoord.collectionBalanceSegmentBatchSize`. |
| `queryCoord.balanceEpoch.maxChannelTasks` | `1` | Hard channel-task cap; fallback order is `queryCoord.balanceChannelBatchSize`, then deprecated `queryCoord.collectionBalanceChannelBatchSize`. |
| `queryCoord.balanceEpoch.maxTasksPerNode` | `5` | Hard placement-action cap per source or target node. |
| `queryCoord.balanceEpoch.maxTasksPerCollection` | `5` | Hard per-collection cap within one RG wave. |
| `queryCoord.balanceEpoch.maxObjectRetries` | `3` | Consecutive known failures before quarantine. |
| `queryCoord.balanceEpoch.quarantineBackoff` | `60000` ms | Backoff before a quarantined object becomes eligible again. |

All ten keys are refreshable. An epoch-specific batch value overrides both fallback keys; resetting it restores the current key, then the deprecated key, then the documented default. Zero or negative task limits allow no new work for that dimension; they are not interpreted as unlimited.

## Compatibility, Deprecation, and Migration Plan

The feature is internal and does not change collection, replica, or search APIs.

The minimum deliverable is deliberately limited to the normal-balance closed loop:

1. RG-scoped immutable snapshot capture;
2. snapshot-backed adaptation of the existing configured policy;
3. one current normal planning generation per RG;
4. typed, atomic admission with hard task budgets;
5. epoch-owned replica-scoped object locks and pending reservations;
6. distribution-based reconciliation, deadline, and generation handoff; and
7. planned-versus-admitted observability.

Recovery admission classes, epoch-scoped cancellation, and fine-grained recovery integration are follow-up designs, not hidden requirements of this MVP. Resource vectors, migration byte budgets, and new placement objectives remain separate MEPs.

Active epoch mode is supported only for `ScoreBasedBalancer` and `ChannelLevelScoreBalancer`, subject to the Streaming Service boundaries above. Round-robin, row-count, multi-target, and any future balancer without a snapshot policy use the legacy path. They must never claim snapshot-backed epoch semantics while reading live managers.

Rollout is divided into explicit stages:

| Stage | Status in this change | Behavior |
|---|---|---|
| Instrumentation | Implemented | Epoch attribution, planned/admitted accounting, lifecycle metrics, and snapshot retry observation are present while active mode remains off by default. |
| Shadow planning | Implemented, opt-in | With active mode disabled and shadow enabled, build RG snapshots/waves without admission or runtime state, then run legacy normal balance in the same eligible tick. |
| Normal-balance epoch | Implemented, opt-in | Route supported normal policies through one epoch per RG. Active mode takes precedence over shadow. |
| Recovery integration | Follow-up | Add explicit recovery admission classes, object-scoped preemption, and common capacity reservation. Existing stopping/recovery paths retain priority in the MVP. |
| Default enablement | Follow-up | Consider changing the default only after upgrade, failure-injection, scale, and production-shadow evidence shows no convergence or availability regression. |

During rollout, disabling `queryCoord.balanceEpoch.enabled` stops creation of new epochs. QueryCoord continues one observation `Advance` per active RG and restores legacy normal balance only after `ActiveResourceGroups()` is empty and the checker/`autoBalance` gates permit it. A terminal RG is not restarted in the same tick. Retained retry/quarantine state continues observation but does not suppress legacy fallback once no generation is active. In-flight scheduler tasks continue to use existing safety and deduplication semantics. Epoch task metadata remains inside QueryCoord and does not require a QueryNode protocol change.

Mixed-version QueryNode deployments are supported because the first version of the epoch protocol relies on existing distribution fields. Future resource-vector extensions require their own compatibility design.

### Verified implementation evidence

Verification targets final implementation commit `8c7b55f32cdf031ead70e99afdace281434761bb`, tree `bf66602b87ebde3dd1332cce9fa956d1a08864e9`, relative to baseline `4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d`. The range contains 22 commits, changes 39 files including 38 Go files, and passes the 22/22 final-trailer DCO audit. NUL-safe `gofmt -d` over the changed Go files emitted no diff, and `git diff --check` exited `0`.

Added-line scans over branch-modified non-test Go code and direct scans of `epoch_manager.go`, `epoch_snapshot.go`, and `epoch_wave.go` found zero production origins using `fmt.Errorf`, `errors.New`, `errors.Newf`, or `errors.Errorf`. The repository-configured scoped lint command reached the affected balance package and produced:

```text
FINAL8C7_SCOPED_LINT_EXIT=1
FINAL8C7_RAWMERRERROR_COUNT=0
14 issues:
* depguard: 2
* gci: 1
* staticcheck: 11
```

The nonzero scoped exit therefore is not a clean lint result; its narrower evidence is that ruleguard reported `rawmerrerror: 0`. Repository-wide `make static-check` is also not claimed passing. It exited `2` during core-package typechecking, before ruleguard, because the checkout lacks the unchanged generated migration package:

```text
cmd/tools/migration/meta/210_to_220.go:12:2: could not import
github.com/milvus-io/milvus/cmd/tools/migration/legacy/legacypb

no required module provides package
github.com/milvus-io/milvus/cmd/tools/migration/legacy/legacypb

1 issues:
* typecheck: 1
make: *** [static-check] Error 1
```

Because the repository-wide target did not reach ruleguard, it supplies no `rawmerrerror` evidence. The scoped command is the direct evidence for that rule. The same missing generated-package blocker existed before and after the final review-fix commits.

Behavioral tests were run on the approved `mini` macOS development host. Every accepted run sourced `scripts/setenv.sh`, supplied the repository native-library RPATH, used a fresh isolated no-auth etcd/local-storage directory, preserved the direct Go test exit code, and verified process, port, and data-directory cleanup.

The final focused matrix contained nine commands:

```bash
go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/session \
  -run 'TestClusterSuite/TestBalanceRPCPreDispatchErrorsAreMarkedNotSent' -count=1

go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/task \
  -run 'TestTask/(TestExecutorRawLoadSegmentsErrorIsAmbiguous|TestExecutorRawReleaseSegmentsErrorIsAmbiguous|TestExecutorRawWatchDmChannelsErrorIsAmbiguous|TestExecutorRawUnsubDmChannelErrorIsAmbiguous|TestExecutorRPCNotSentReleaseErrorIsDefinitive|TestExecutorNonOKReleaseStatusIsDefinitive)' -count=1

go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/balance \
  -run 'Test(EpochManagerAmbiguousRPCFailureRetainsReservationsUntilAuthoritativePlacementSettles|EpochManagerFailedStatusBeforeDoneIsAmbiguous)' -count=1

go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/task \
  -run 'TestTask/(TestChannelTaskDeltaSnapshot|TestAdmitBalanceTask|TestTaskDone)' -count=1

go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/balance \
  -run 'Test(PlacementSnapshot|WaveLedger|ProjectedPlacement|ScoreEpochPolicy|ChannelLevelEpochPolicy|EpochManager|BalanceEpoch)' -count=1

go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/checkers \
  -run 'TestBalanceChecker|TestCheckControllerSuite' -count=1

go test -timeout 120s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/dist \
  -run 'TestDistControllerSuite/TestStart' -count=1

cd pkg
go test ./util/paramtable -run 'TestComponentParam_BalanceEpoch' -count=1
go test ./metrics -run 'TestQueryCoordBalanceEpochMetrics' -count=1
```

The exact accepted package results were:

```text
ok  github.com/milvus-io/milvus/internal/querycoordv2/session   0.733s
FINAL8C7_FOCUS_SESSION_EXIT=0
ok  github.com/milvus-io/milvus/internal/querycoordv2/task      0.827s
FINAL8C7_FOCUS_AMBIG_TASK_EXIT=0
ok  github.com/milvus-io/milvus/internal/querycoordv2/balance   0.753s
FINAL8C7_FOCUS_AMBIG_BALANCE_EXIT=0
ok  github.com/milvus-io/milvus/internal/querycoordv2/task      0.836s
FINAL8C7_FOCUS_TASK_EXIT=0
ok  github.com/milvus-io/milvus/internal/querycoordv2/balance   0.856s
FINAL8C7_FOCUS_BALANCE_EXIT=0
ok  github.com/milvus-io/milvus/internal/querycoordv2/checkers  1.312s
FINAL8C7_FOCUS_CHECKERS_EXIT=0
ok  github.com/milvus-io/milvus/internal/querycoordv2/dist      5.794s
FINAL8C7_FOCUS_DIST_EXIT=0
ok  github.com/milvus-io/milvus/pkg/v3/util/paramtable          0.321s
FINAL8C7_FOCUS_PARAMTABLE_EXIT=0
ok  github.com/milvus-io/milvus/pkg/v3/metrics                  0.272s
FINAL8C7_FOCUS_METRICS_EXIT=0
focused remote wrapper exit=0
FINAL8C7_FOCUS_PROCESS_CLEAR=1
FINAL8C7_FOCUS_PORTS_CLEAR=1
FINAL8C7_FOCUS_DATA_CLEAR=1
```

All nine focused commands exited `0`. The matrix covers RPC outcome typing, task admission/delta/completion, snapshot/wave/policy/manager behavior, checker integration, the synchronized dist `TestStart` fixture, all ten refreshable configuration keys, and all eight metric collectors.

The prescribed balance race selector also passed:

```bash
go test -race -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/balance \
  -run 'Test(EpochManagerAmbiguousRPCFailureRetainsReservationsUntilAuthoritativePlacementSettles|EpochManagerFailedStatusBeforeDoneIsAmbiguous)' \
  -count=1
```

```text
ok  github.com/milvus-io/milvus/internal/querycoordv2/balance  1.931s
FINAL8C7_RACE_BALANCE_EXIT=0
race remote wrapper exit=0
FINAL8C7_RACE_PROCESS_CLEAR=1
FINAL8C7_RACE_PORTS_CLEAR=1
FINAL8C7_RACE_DATA_CLEAR=1
```

No race report was emitted.

The final complete QueryCoord command was:

```bash
source scripts/setenv.sh
go test -timeout 300s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" \
  -tags dynamic,test ./internal/querycoordv2/... -count=1
```

The exact accepted output was:

```text
ok   github.com/milvus-io/milvus/internal/querycoordv2            109.183s
ok   github.com/milvus-io/milvus/internal/querycoordv2/assign       1.358s
ok   github.com/milvus-io/milvus/internal/querycoordv2/balance      6.212s
ok   github.com/milvus-io/milvus/internal/querycoordv2/checkers     5.969s
ok   github.com/milvus-io/milvus/internal/querycoordv2/dist        19.951s
ok   github.com/milvus-io/milvus/internal/querycoordv2/job          5.701s
ok   github.com/milvus-io/milvus/internal/querycoordv2/meta        13.371s
?    github.com/milvus-io/milvus/internal/querycoordv2/mocks       [no test files]
ok   github.com/milvus-io/milvus/internal/querycoordv2/observers   28.001s
?    github.com/milvus-io/milvus/internal/querycoordv2/params      [no test files]
ok   github.com/milvus-io/milvus/internal/querycoordv2/session      3.209s
ok   github.com/milvus-io/milvus/internal/querycoordv2/task        22.413s
ok   github.com/milvus-io/milvus/internal/querycoordv2/utils        4.016s
FINAL8C7_FULL_QUERYCOORD_EXIT=0
full remote wrapper exit=0
FINAL8C7_FULL_PROCESS_CLEAR=1
FINAL8C7_FULL_PORTS_CLEAR=1
FINAL8C7_FULL_DATA_CLEAR=1
```

Only the known non-fatal Darwin linker warnings about deprecated `-bind_at_load` and ignored duplicate native libraries were emitted. A separate follow-up after all wrappers exited again confirmed no matching processes, listeners, or run-specific temporary paths remained. The final re-review of the two review-fix commits reported zero Critical, Important, or Minor findings.

### #51244 static-target convergence fixture

`TestBalanceEpochIssue51244StaticTargetConverges` uses the production `scoreEpochPolicy`, `BalanceEpochManager`, `PlacementSnapshotBuilder`, and authoritative `DistributionManager`, with a deterministic generation-aware admission seam at the external scheduler boundary. It does not script successive waves. The retained sequence is:

```text
epoch 1: potential 68550 -> 48750, 2 planned / 2 admitted
epoch 2: potential 48750 -> 16550, 2 planned / 2 admitted
epoch 3: potential 16550 ->  5550, 1 planned / 1 admitted
epoch 4: potential  5550 ->  5550, 0 planned / 0 admitted, Converged

moves: segment 110 1->3, 111 1->3, 112 1->3, 113 1->3, 114 1->3
```

The fixture proves every committed-prefix potential is non-increasing across observations and strictly decreasing when work is admitted, every planned task is admitted with no rejected suffix, all wave budgets hold, no replica-scoped object is concurrent, no object moves `A -> B -> A`, and admitted move count reaches zero at the fixed point. The nonzero final potential is expected: convergence means no legal strict improvement, not zero mathematical variance.

Five companion fixtures prove delayed Grow locking, successful-prefix retention across failed Grow/Reduce, unrelated-collection progress around ambiguous carry, per-RG manager-lock isolation, and hard segment/channel/collection/node budgets across two collections and replicas. The channel fixture also found and fixed the execution-time `LeaderHash` self-supersession defect described above.

## Test Plan

### Unit tests

- Snapshot construction retries when any member of the version tuple changes.
- Unrelated RG distribution and pending-task churn does not retry or invalidate this RG; relevant same-RG changes still do.
- Two independent tasks from one wave can advance the RG pending token and both be admitted; a same-RG external mutation between admissions rejects the suffix.
- Advancing target state between sealed-segment and channel reads forces a retry; accepted target content and version come from one generation.
- Unowned segment/channel placement on an RG physical node and NilReplica cleanup effects are counted exactly once until distribution removes them.
- Blocking one collection's target refresh does not block another collection's task add or pending snapshot inspection.
- Every balance-epoch config key has the documented default and refreshes through `Save`/`Reset`; active generations retain frozen creation-time values.
- All eight collectors register once; active-state transitions delete the previous state label, terminal counters/durations publish exactly once, objective phases retain the latest observation, and carry labels are removed when their count reaches zero.
- Returned snapshot maps and slices are isolated from distribution-manager mutation.
- One RG cannot run two planning/admission generations concurrently.
- Different RG `Advance` calls use independent runtime locks; a blocked RG-A call does not block RG-B from converging or starting work.
- Old-generation safe-settle tasks remain locked and reserved while the current generation plans unrelated objects.
- Hard task limits cannot be exceeded by multiple collections, replicas, shards, or outbound nodes.
- Only successfully admitted tasks count toward epoch completion and budget.
- A mid-wave rejection stops the suffix; `ObjectiveAfter` and metrics use the accepted-prefix potential, not the full planned wave.
- Deterministic plan ordering produces the same wave for the same snapshot.
- A wave whose aggregate objective does not pass `ScorePotential.Improves` is rejected even when its individual plans pass local benefit checks.
- `Converged` uses the same strict potential comparison as wave admission.
- Duplicate and stale plans are rejected without corrupting reservations.
- Epoch deadline transitions through reconciliation before `TimedOut`.
- Pre-dispatch failures for all four balance RPCs are marked `RPCNotSentError`, preserve their original causes, and remain definitive.
- Raw post-dispatch errors for Segment Grow/Reduce and Channel Grow/Reduce are marked `AmbiguousExecutionError`; explicit non-OK QueryNode statuses and RPC-not-sent errors remain definitive.
- `Task.Err()` is read only after `Done` is observed closed; a `Failed` status observed before that synchronization point remains ambiguous.
- Real scheduler/executor task completion preserves the typed error through the `Done`/`Err` surface consumed by epoch reconciliation.

### State-machine tests

- No-plan snapshot finishes as `Completed/Converged`.
- All tasks succeed and distribution matches: `Completed`.
- Grow fails before target presence: source remains and epoch becomes `Degraded`.
- A raw post-dispatch Grow RPC times out but target later appears: reconciliation recognizes success.
- Reduce fails after target presence: redundant copy remains and epoch becomes `Degraded`.
- Cancelled or raw post-dispatch timed-out, unavailable, or lost-response Reduce with source+target both present remains locked; a delayed source disappearance later reconciles as completed.
- Ambiguous Reduce with both source and target absent remains locked for normal balance while availability repair proceeds; it is not misclassified as a definitive missing-placement quarantine.
- A completed failed task with `AmbiguousExecutionError` remains locked and keeps both charged-node reservations for Segment/Channel Grow/Reduce until authoritative placement settles.
- Desired target-only placement completes even when the scheduler task retains an ambiguous post-dispatch error.
- Node failure and RG membership change supersede the RG generation.
- A collection target, replica, node-eligibility, or pre-admission leader change stops the remaining suffix and transitions through reconciliation.
- Execution tolerates `StaleEpoch` and post-admission leader publication while RG/node/replica/target invalidation still supersedes.
- Stopping balance prevents creation of a new normal epoch while active epochs continue observation and reconciliation.
- The epoch's own distribution updates do not self-invalidate the generation.
- Unrelated RG distribution updates do not invalidate the snapshot scope.

### Integration tests

- Multiple collections in one RG select destinations under one shared wave budget.
- Multiple replicas and channels cannot exceed RG-wide task limits.
- A slow load does not allow repeated normal checker ticks to generate overlapping waves.
- A permanently stuck segment does not prevent unrelated collections in the same RG from progressing after the no-progress deadline.
- Scheduler admission rejection is visible in epoch accounting and does not suppress future planning indefinitely.
- Distribution pull delay between Grow and Reduce does not create a reverse move for the same segment.
- One failed task does not roll back unrelated successful moves.
- One RG's stuck or failed epoch does not block another RG.
- Ambiguous post-dispatch failures cannot admit a duplicate or reverse move for the same replica-scoped object; delayed authoritative Grow/Reduce publication releases the carry only after target-only placement is observed.
- Shadow mode produces snapshot/plan/objective metrics, performs zero typed admissions, installs no locks/reservations/runtime state, and leaves legacy normal balance enabled when no active generation is draining.

The issue-#51244 static-target fixture records every `(replica, object, from, to)` admission across repeated checker ticks and asserts:

1. score potential never increases at an admitted wave boundary;
2. new admissions never exceed the wave's kind/collection caps, and pending plus carry-over node reservations never exceed the per-node capacity cap;
3. the same replica-scoped object is never admitted concurrently;
4. successful moves remain successful when another task fails;
5. admitted move count eventually reaches zero under static topology and targets; and
6. no zero-net `A -> B -> A` cycle is admitted under the static snapshot sequence.

### Follow-up validation not claimed by this change

- Repeated QueryNode restarts and shard-leader changes while normal balance is active.
- RG scale-out and scale-in near balance trigger boundaries.
- Object-storage load failures and resource-exhaustion responses.
- A production-scale distribution modeled after issue #51244, beyond the deterministic retained fixture, verifying bounded in-flight work and the absence of repeated reverse moves under a static target.
- QueryCoord process restart during active execution, verifying that recovered distribution—not in-memory epoch state—drives the next plan.

### Final validation commands

Focused task, balance, checker, dist, configuration, and metric tests ran first with the nine selectors shown in the evidence section. The final local/static and `mini` behavioral commands were:

```bash
git diff --name-only -z \
  4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d...HEAD -- '*.go' |
  xargs -0 gofmt -d

git diff --check \
  4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d...HEAD

# No branch-added production raw constructors are expected.
! git diff -U0 \
  4dbaba0042d3952fa75bbb5d2fb9606c6b67f44d...HEAD -- \
  '*.go' ':(exclude)**/*_test.go' |
  rg '^\+[^+].*(fmt\.Errorf|errors\.(New|Newf|Errorf))'

! rg -n 'fmt\.Errorf|errors\.(New|Newf|Errorf)' \
  internal/querycoordv2/balance/epoch_manager.go \
  internal/querycoordv2/balance/epoch_snapshot.go \
  internal/querycoordv2/balance/epoch_wave.go

source scripts/setenv.sh
GO111MODULE=on GOFLAGS=-buildvcs=false bin/golangci-lint run \
  --build-tags dynamic,test --timeout=30m --config .golangci.yml \
  ./internal/querycoordv2/balance/...

make static-check

source scripts/setenv.sh
go test -timeout 300s -gcflags="all=-N -l" -ldflags="-r ${RPATH}" -tags dynamic,test \
  ./internal/querycoordv2/... -count=1

go test -race -ldflags="-r ${RPATH}" -tags dynamic,test ./internal/querycoordv2/balance \
  -run 'Test(EpochManagerAmbiguousRPCFailureRetainsReservationsUntilAuthoritativePlacementSettles|EpochManagerFailedStatusBeforeDoneIsAmbiguous)' -count=1
```

The scoped lint command exits nonzero because of the 14 listed non-raw findings, while directly establishing `rawmerrerror: 0`. Repository-wide `make static-check` exits `2` at the unchanged missing `cmd/tools/migration/legacy/legacypb` typecheck dependency before ruleguard; it is recorded as a blocker, not as a passing verifier gate. The implementation PR will record the same host, final commit, focused selectors, full command, exact exit codes and package timings, isolated-etcd cleanup evidence, and this static-check limitation.

## Rejected Alternatives

### Cluster-global epoch

A single epoch for the entire QueryCoord would provide one global barrier but would couple unrelated RGs. One slow or failed load could block normal balancing for all tenants and resource pools. RG-level epochs preserve the required shared-capacity boundary without introducing cluster-wide head-of-line blocking.

### Per-collection epoch

Collections in the same RG compete for the same destination nodes. Independent collection epochs can reserve the same apparent headroom and reproduce conflicting plans. The balance group therefore must include all collections and replicas assigned to the RG.

### Per-replica or per-shard epoch

This granularity increases parallelism but does not provide shared RG capacity reservation. It may be added later as internal wave scheduling under one RG epoch, provided all sub-waves use the same snapshot and reservation ledger.

### Continue using task deltas without an epoch

Distribution-aware task deltas correctly project individual in-flight actions, but they do not define a consistent snapshot, hard wave boundary, admission accounting, failure reconciliation, or topology invalidation protocol.

### Transactional rollback

Rolling back every successful move after one task fails would create additional loads and releases, increasing cost and availability risk. Grow-before-Reduce makes most partial outcomes safe. The design keeps successful moves, reconciles actual placement, and replans from reality.

### Persist in-progress epochs

Persisting projected placement and action state introduces recovery complexity and can conflict with the authoritative QueryNode distribution after failover. The first version rebuilds from target, topology, scheduler state, and recovered distribution instead.

## Delivery Sequence

This feature used a design-first two-commit cross-link flow:

1. Open the design-doc PR as [milvus-io/milvus#51430](https://github.com/milvus-io/milvus/pull/51430), with the implementation PR still pending.
2. Open the draft implementation PR as [milvus-io/milvus#51431](https://github.com/milvus-io/milvus/pull/51431). Its body links the design-doc PR, states that active rollout defaults to disabled, and includes the verified focused/full test evidence and repository-wide static-check blocker.
3. Add this follow-up commit to the design-doc branch, replacing the pending implementation-PR prose with the implementation PR URL.
4. Verify that both PRs cross-link before review handoff.

## References

- [Issue #51244: ScoreBasedBalancer never converges](https://github.com/milvus-io/milvus/issues/51244)
- [PR #49861: make balance workload delta dist aware](https://github.com/milvus-io/milvus/pull/49861)
- [PR #50774: 2.6 backport](https://github.com/milvus-io/milvus/pull/50774)
- [PR #51430: Resource-Group Balance Epoch MEP](https://github.com/milvus-io/milvus/pull/51430)
- [PR #51431: Resource-Group Balance Epoch implementation](https://github.com/milvus-io/milvus/pull/51431)
- `internal/querycoordv2/checkers/balance_checker.go`
- `internal/querycoordv2/balance/channel_level_score_balancer.go`
- `internal/querycoordv2/task/scheduler.go`
- `internal/querycoordv2/task/executor.go`
- `internal/querycoordv2/dist/dist_handler.go`
- `internal/querycoordv2/meta/segment_dist_manager.go`
- `internal/querycoordv2/meta/channel_dist_manager.go`
