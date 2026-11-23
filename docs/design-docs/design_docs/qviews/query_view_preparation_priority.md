# QueryView Preparation Priority and Cooperative Resource Scheduling

Related requirement: [milvus-io/milvus#55284](https://github.com/milvus-io/milvus/issues/55284).

Status: the first implementation of priority ordering for pending tasks was connected on 2026-11-11. This document distinguishes the implemented queue scheduling from the finer-grained cooperative scheduling proposed for later work. [ViewFeedback](view_feedback.md) defines access-statistics collection and aggregation.

## Current Implementation Scope (2026-11-11)

The first version explicitly changes only the order of tasks that have not started; it does not preempt running tasks. Implemented behavior:

- `QueryViewControl.preparation_priority` is `sint32` field 2, with a range of [-20, 20]. The default is 0, and larger values have higher priority. It is immutable after creation. The builder copies Control, and Coord lease control strengthens only the lease field. For retransmissions of the same identity, SN retains the preparation priority first received, including when an older message omits the field. Coord/SN recovery uses the persisted value.
- Coord computes the score when creating a new View through normal planning, published-schema recovery planning, or manual migration. Existing Views keep their score. Before planning, the balancer also uses this rule to rank queued shard IDs. A repeated submission updates their queue priority. An operation retains immutable feedback snapshots for replicas in the current configuration and aggregates rates from the last 6 windows by RG; numerator and denominator use the same snapshots.
- `nodeScheduler.WithPriority(int32)` and `PriorityTaskHandle.SetPriority(int32)` use the same executor. Dispatch normally follows priority, with FIFO among equal scores; after every 8 priority dispatches, the oldest runnable task gets an opportunity. SetPriority preserves queue age, does not advance retry deadlines, and does not cancel or restart Execute. A new effective value for a running task applies only to subsequent retries.
- QC dirty flush tasks choose ready shards by their merged effects' priority when claiming a batch. See [keyed concurrent batch flush](shard_view_management.md#44-keyed-concurrent-batch-flush) for fairness, eligibility and batching.
- QN retains the task handle for each SegmentResource and takes the maximum priority among bindings whose dependencies are resolved, which are not ready or failed, and which are not waiting for retry or catch-up. If all scores are negative, the result remains negative. Dependency addition, release, and failure reorder pending tasks. Within a resource, reusable reads are published first, then eligible bindings are selected by priority. Equal scores preserve cursor order; after every 8 priority selections, the round-robin starting point gets an opportunity. Each reconcile performs at most one physical preparation before re-enqueuing, retaining the existing retry timer and cleanup behavior.
- SN shared builds take the maximum priority of unfinished dependencies; removing a dependency can lower the score. Priority does not change the build snapshot selected for the oldest View. Each per-View ready task uses its fixed value. WAL scoped handles forward SetPriority without changing task tracking or shutdown semantics.

The shared scheduler from PR #55398 (b36d566118) is now fully integrated, replacing the previous local implementation. In addition to priority dequeueing, it applies cumulative worker caps of 50% / 75% / 100% to running tasks in the <=-10 / <=-1 / <=20 bands, rounded down with a minimum of 1. Lower bands cannot borrow capacity reserved for higher bands, and fairness dispatch cannot exceed these caps. A score change therefore affects both dequeue order and the capacity band for subsequent admission; a running attempt remains charged to its original band.

ACK has a fixed priority of 20, release has 5, and data defaults to -5. Pending data tasks within the recovery-tail pressure target can be raised to 5. QueryView build/ready tasks replace the default 0 with the View score or the maximum of shared dependencies. Views that reach 20 compete in the ACK band, so reserved capacity is no longer exclusive to ACK.

The current policy selects a scenario based on user demand, then adds a scenario-specific traffic score and configuration offset. It replaces the earlier LOW=-10 rule, separate activity-time bonus, and linear rate bonus.

| Scenario, evaluated in order | Base score | Traffic-score cap | Range including configuration |
| --- | --- | --- | --- |
| Recent access, no live UpView | +10 | 8 | [10,20] |
| Initial AutoLoad (shard has no evidence of ever reaching Up) | +10 | 0 | [10,12] |
| Recent access, live UpView exists | 0 | 2 | [0,4] |
| No recent access, live UpView exists (cold handoff) | -10 | 0 | [-10,-8] |
| Known long-term inactivity after initial AutoLoad or on a manual load, no live UpView (cold recovery) | -5 | 0 | [-5,-3] |
| No access history, no live UpView, after initial AutoLoad or on a manual load | 0 | 0 | [0,2] |

- Configuration offset: HIGH, including the proto default, adds +2; LOW and unknown enum values add 0. The enum's underlying numeric value is not used.
- Recent access: LastAccessTime is nonzero and no more than 2 minutes before this planning operation's time. Future timestamps count as recent. LastAccessTime selects the scenario and does not add a separate activity score.
- Traffic mapping: `share=clamp(replica.Last6Windows / same_RG_total,0,1)`, `heat=log1p(100*share)/log1p(100)`, and `traffic=round(limit*heat)`. A denominator <=0 gives a traffic score of 0.
- The total is `clamp(base + traffic + config, -20,20)`. The current policy actually produces [-10,20]; the wire/scheduler range remains [-20,20] and need not be fully used.
- In the most urgent band, shares of 1%/5%/10%/50%/100% receive traffic scores of 1/3/4/7/8. AutoLoad and recovery with recent access but no UpView share a +10 base. With the same configuration, zero-traffic recovery ties with AutoLoad, while recovery can receive an additional traffic bonus. Different +2 configuration offsets allow these scenarios to interleave at low traffic.

ViewFeedback removes windows older than 2 minutes but retains the replica's last activity time until the replica is removed. Zero window rates do not imply cold data: the scenario base remains while the access is still within 2 minutes. The last 6 windows are not assumed to mean exactly 6 seconds. Missing statistics do not establish long-term inactivity; a View without an UpView or access history uses the initial/manual-load base without inventing access records.

**Finer-grained proposals not yet implemented:** the separate Yield/Wait/Event interfaces in Sections 6 and 12, fairness across collection groups, and priority handling within the separate Transform catch-up drain pool, IDF permits, and live-event dispatcher. The first version retains their existing execution boundaries and budgets. Once Execute starts, its I/O, native, and module stages are neither interrupted nor readmitted when a higher score arrives. The current implementation therefore establishes queue priority for the main preparation tasks; it does not imply that every downstream queue follows View priority or guarantee wall-clock recovery latency.

## 1. Goals and Established Constraints

Under resource contention, Coord can assign preparation priority to a new QueryView based on user configuration and collection access activity. QN/SN propagate that value to actual resource tasks so cooperative scheduling can advance more urgent preparation first, with the aim of improving resource allocation during failure recovery.

Core constraints:

1. Priority is a signed integer in `[Pmin, Pmax]`, where `Pmin < 0 < Pmax`. The default baseline is 0; negative values are lower and positive values are higher.
2. It is an absolute score, not an increment command. Receiving `+10` always means a value of 10.
3. **Priority is immutable for a given QueryView identity.** It is computed at creation and retained through preparation, retries, retransmissions, and recovery.
4. Coord uses the target replica's LastAccessTime and rate over the last 6 windows, computing its share against the total replica rate across all collections in the same RG. SN collects the inputs through GetQueryPlan, and ReplicaFeedbackStore provides replica snapshots with lifetimes independent of Views. Balancer does not scan shards/windows or restore the old Contribution/Registry path; QN adds no access-statistics collection.
5. Policy is scoped to an RG, without introducing a single cross-RG recovery target or serialization barrier. Work proceeds in parallel when resources do not actually contend.
6. Priority does not change placement, readiness, schema/MVCC, leases, reference counts, or WAL ordering.

The issue's original proposal to raise or lower priority while an existing View is preparing is outside this design. Statistics keep changing, but are consumed only when normal processing creates a new View. Views are not replaced merely to rescore them.

## 2. Original Integration Checklist (See the Opening Scope for Implementation Status)

| Location | Original behavior | Required work |
| --- | --- | --- |
| `pkg/proto/view.proto` | QueryViewControl contains `preempt_serving_lease` | Add an immutable preparation-priority field, defaulting to 0 for old data |
| `internal/views/coord/balancer/` | Plans Views/placement without using replica Priority to order preparation | Read scoped statistics and user configuration when creating a View, and compute a fixed value |
| `internal/views/coord/coordview/` | Persists Views before synchronizing them to nodes | Preserve priority consistency across creation, persistence, retransmission, and recovery |
| `pkg/util/nodescheduler/` | Runnable FIFO; delayed tasks ordered by nextRun | Numeric priority, fair selection, and cooperative continuation/wait semantics |
| `internal/querynodev2/qnview/` | Segment resource tasks and round-robin bindings within resources | Propagate dependent View priorities into selection between and within resources |
| `internal/querynodev2/transformlogbuffer/` | Separate catch-up drain queue | Let registrations inherit dependency priorities while preserving batch yields and ordering |
| `internal/streamingnode/server/wal/vchannel/queryresource/` | Shared runtime builds and per-View readiness | Shared-task inheritance, resumable stages, and yielding while waiting for dependencies |
| SN IDF/event-application admission | Separate permits/dispatchers | Apply the same scheduling inputs at actual contention points while preserving each budget |

In the original integration baseline, `LoadReplicaConfig.priority` was stored in load configuration and projected into management data, but did not affect QueryView preparation scheduling. DataCoord's `query_view_segment_load_info.go` supplied a fixed HIGH for QV Segment and index loading. This design makes replica configuration **an input to Coord's new priority calculation**, rather than reusing an existing QV ordering policy.

## 3. Priority Representation, Persistence, and Compatibility

### 3.1 Control Field

The proposed field layout is shown below; verify proto field numbers before implementation:

```protobuf
message QueryViewControl {
    bool preempt_serving_lease = 1;
    sint32 preparation_priority = 2;
}
```

The business range need not use all of sint32. Larger values compare as higher priority; a score difference does not automatically imply a multiple of resource share. Negative values still represent executable tasks, not suspension or rejection.

Control fields have distinct semantics: lease control can be strengthened under existing rules, while preparation priority is fixed. Paths that replace the entire Control must preserve priority. Control comments must no longer describe every field as the same kind of monotonic control.

### 3.2 Creation Flow

```text
Balancer plans a new QueryView through normal processing
  -> Read the target replica's statistics snapshot, same-RG total rate, and replica configuration
  -> Compute and fix preparation_priority
  -> Persist the View
  -> Send the same View through SyncQueryView
  -> QN/SN retain the fixed priority on first Acquire
  -> Node resource managers compute effective priorities for actual tasks
```

Priority is determined before publishing the View. Persist-before-sync follows the existing dirty-flush rules. Retrying a message for an already submitted View does not rerun scoring; duplicate applies of the same identity do not overwrite the priority held by the resource registration.

QN/SN retain the fixed value alongside the current acquisition. Reordering must not replace the requirement's Meta pointer: in particular, SN `prepareViewRef` uses Meta identity and runtime identity to identify registration generations. It must not reacquire/release resources, reopen subscriptions, or discard preparation progress.

### 3.3 Immutability and Upgrades

- Do not add UpdatePriority, priority control revisions, applied-priority ACKs, or retransmissions dedicated to priority updates.
- User-configuration and statistics changes affect only subsequently created Views. When normal failure or version progression requires a new View, compute its score from the inputs available then.
- Coord and SN use the persisted value when recovering the same View. SN's local UpRecovering does not add a temporary View priority.
- Old data/messages without the field default to 0; older nodes ignore the new field. Scheduling effects may be incomplete during mixed-version operation, but the existing lifecycle must continue.
- A nonzero value for an identity is not silently overwritten by the default 0 in an old-format retransmission. Normal creation/recovery paths must provide a consistent value; there is no last-message-wins priority protocol.

Under this constraint, an existing ordinary Preparing View is not upgraded in place after a failure, and an existing high-score View is not automatically downgraded when conditions change. This is an accepted simplification, not something to bypass by creating extra Views.

## 4. How Coord Computes the Score

### 4.1 Statistics and Policy Scope

Statistics come from ReplicaFeedbackStore's immutable replica snapshots:

- Last activity time: the target replica's `LastAccessUnixNanos`.
- Numerator: the target replica's `RequestRates.Last6Windows`.
- Denominator: the sum of `Last6Windows` for every replica of every collection in the same RG under the current configuration. Replicas without snapshots contribute 0. This is neither the collection-local total nor the Store's cluster-wide `GetGlobalRequestRates()`.

The Store already averages shard-window rates over the collection's total shard count to obtain a replica rate, treating unreported shards as 0. Balancer consumes this rate directly without converting it back into a request count. The Store's 2-minute window policy handles validity; no additional report timeout is introduced.

ReplicaFeedbackStore incrementally maintains RG total rates on feedback publication, expiry, and topology changes. A planning/manual operation acquires an immutable PlanningSnapshot when scoring is first needed. It contains both replica feedback and RG aggregates; reading the version root neither scans collections/replicas nor allocates memory. Subsequent scoring within the operation uses that fixed version, so concurrent feedback cannot change its numerator or denominator. Index lookups use replica ID and RG name; health checks still traverse the UpView's QN list. Normal planning scores a candidate only after deciding that a new View is required. Unchanged optional candidates, failed allocations, and releases do not read feedback.

The Store tracks each replica's RG through the LoadConfig listener. Migration subtracts its contribution from the old group and adds it to the new one; release removes the contribution. Published versions use persistent radix indexes, copying only changed paths rather than cloning the entire replica map. If the RG in the planning operation's pinned LoadConfig differs from membership in the feedback version, that replica's feedback is treated as unavailable to avoid pairing a mismatched numerator and denominator. Load priority, AutoLoad, and node health still come from the operation's existing inputs.

A new View does not need its own query history. Views created for the same replica within an operation share statistics inputs; different replicas of the same collection use their own access activity. Without a snapshot, the scenario is selected from load demand and UpView state. A zero denominator only removes the traffic bonus.

Existing QN placement is planned by `(CollectionID, ResourceGroup)`. Replica target-node sets within one collection can be disjoint, while different collections can share nodes. RG is therefore the policy scope, while actual priority selection occurs in contending node/resource queues. Independent execution resources must not sit idle merely because another View in the RG is unfinished.

### 4.2 Scenario Scoring

```text
recent = LastAccessTime != 0 && now - LastAccessTime <= 2min
(base, limit) = SelectScenario(recent, healthyUp, LoadConfig.AutoLoad && !maybeUped, knownHistory)
share = clamp(replica.Last6Windows / same_RG_total, 0, 1)
traffic = round(limit * log1p(100 * share) / log1p(100))
P(view) = clamp(base + traffic + ConfigOffset, -20, 20)
```

The opening table defines the scenarios and fixed parameters. When the total rate is <=0, traffic=0. A missing snapshot or zero LastAccessTime means no access history, which is different from known long-term inactivity. No additional NQ, execution-cost, or preparation-progress statistics are collected.

Shards of the same replica share access feedback, but UpView health is evaluated separately for each `(ReplicaID,VChannel)`, so they can receive different scores. An UpView on another replica in the same RG does not reduce the target shard's recovery urgency. Configuration provides only a +2 offset. AutoLoad and recovery with recent access but no UpView share a base score and are ordered by their final scores.

### 4.3 UpView and Node Health

Reuse Balancer's `healthyUp`: ShardStats must have an UpVersion, and every QN in UpNodes must still exist in the node snapshot and be Alive. If a QN has gone offline but its Up metadata has not yet been removed, the UpView is still considered unhealthy. Stopping only disallows new placement; it does not invalidate an UpView whose nodes remain alive. Normal planning and manual migration both capture node snapshots, preventing missing node data on the manual path from being misinterpreted as a node failure.

This consumes existing state and topology rather than recreating serviceability through ViewFeedback. SN failures are handled by existing PChannel assignment/recovery paths; SNs are not mixed into the QN node table. Recovering an SN UpView with the same identity retains its persisted priority instead of rescoring after failure. This follows from immutability. SN failure recovery and the View lifecycle are unchanged.

### 4.4 Persisting the AutoLoad Origin

AutoLoad enters through QueryCoord `EnsureCollectionReady`. When a load configuration must be created, it calls the shared internal `loadCollection(ctx, req, true)`; the public `LoadCollection` entry point passes false. An explicit internal parameter carries the origin without changing the `LoadCollectionRequest` RPC. Health checks, resource admission, errors, and metrics continue to use the shared load flow.

`AlterLoadConfigMessageHeader.auto_load` is persisted and replayed with the broadcast, then projected into versioned `CollectionLoadConfig.auto_load` / `loadmgr.LoadConfig.AutoLoad`. Balancer reads it directly from the existing immutable LoadConfig snapshot, without maintaining AutoLoadDemand, waiter counts, extra locks, or injected interfaces. Coord restores the flag directly from LoadConfig after restart rather than waiting for query retries to reconstruct demand.

Scoring evaluates scenarios in order. Recent access without a healthy UpView receives a +10 base and up to +8 traffic points. Otherwise, initial AutoLoad receives a +10 base until the shard first reaches Up. Coord sets the in-memory `maybeUped` flag on that transition, including recovery fast-forward. A first preparation failure before Up remains eligible for the initial boost while Coord is running. A serving QN failure, a schema change, or temporary suspension of a desired replica preserves the history. Removing the replica or fully releasing the collection ends its load cycle.

On Coord restart, any surviving Up, Down, or Unrecoverable record implies `maybeUped`. An Unrecoverable record can also come from a failed first preparation, so this inference can suppress the boost in that case. If only Preparing records survive, or earlier records have all been deleted, the shard is eligible for the initial boost again. Once `maybeUped` is true, the ordinary access and health rules apply. HIGH retains its +2 offset.

The flag has the same lifetime as the current load configuration. Readiness completion, cancellation of an individual query, and request timeouts do not clear it. Partition creation/partial release, replica adjustments, and derived Schema/Index versions retain it. A new configuration from an explicit manual LoadCollection/LoadPartitions writes false. Load's refresh mode only refreshes data and does not change the origin. A full Release removes the configuration and flag; the next load determines the origin again. If only the origin changes, update the scheduling metadata in the current persisted record under the existing collection update lock, retaining LoadInfoVersion. After persistence succeeds, publish a new read-only configuration object instead of mutating a published object. Configuration snapshots and listener notifications still advance, so subsequently created Views use the new origin. Only resource-requirement changes produce a new LoadInfoVersion.

An origin-only change does not replace an Up View, regress readiness, or add another round of low-score Views after a Preparing View completes. Concurrent readiness validation uses the resource-requirement version; replacing the configuration pointer alone must not make it unready. Actual resource changes and Release/reload remain subject to version checks.

A query waiting on an existing manual LoadConfig no longer registers a temporary AutoLoad bonus. Query arrival or an origin change does not rescore an accepted Preparing View. Only subsequently created Views use the configuration, feedback, and node state available at their creation.

## 5. Fixed View Priority and Shared-Task Priority

Resource managers map View requirements to actual resource work. For a shared task R that can advance a set of requirements:

```text
effective_priority(R) = max(P(V) | V still needs R's current work stage)
```

Each View's P remains fixed, but dependencies change as they are added, satisfied, failed, or released. A shared task's effective priority can therefore change. This is node-local aggregation, not a View control update.

For example, a resource initially serves only View A with P=-10. When View B with P=20 requires the same work, the task becomes priority 20. After B's requirement is satisfied or removed, the task recomputes its score from the remaining dependencies. If all dependencies are negative, the result must remain negative; do not initialize the maximum to 0.

Aggregation must respect physical-resource compatibility and work stages:

- QN must not raise unrelated work with incompatible manifest/schema/read requirements merely because SegmentID matches.
- An SN runtime for a VChannel can be shared by Views from multiple replicas/RGs. Shared preparation runs once to satisfy multiple dependencies. Independent per-View work, such as load scope, uses its own requirements.
- Ready/Up references still protect resource lifetimes, but satisfied preparation requirements must not permanently boost unrelated preparation tasks.
- Remove a preparation task when no preparation dependencies remain. Any remaining serving/release responsibilities belong to their existing owners; referenced resources must not be closed arbitrarily.

A queued task must be repositioned when its effective value changes. Having a heap comparator read a mutable atomic value does not maintain heap order. Resource managers update dependencies under their own locks and submit the computed value to the scheduler. The scheduler must not call back into resource managers while holding its lock.

## 6. Cooperative Scheduling Contract

### 6.1 Execution Unit

QN/SN share scheduling semantics and common capabilities, while implementing their own resource tasks. This is not a global cross-node queue, nor does it require combining CPU, I/O, Transform, and live-event budgets into one pool.

Each execution advances a bounded amount of work and returns one of the following conceptual results. The Go interfaces and error/result representation are determined during implementation:

| Result | Semantics |
| --- | --- |
| Done | Complete the task, release the execution slot, and notify dependents |
| Yield | Preserve progress, release the slot, and requeue at the current effective priority without increasing error backoff |
| Wait | Register a dependency notification, leave the runnable queue, release the slot, and wake when the dependency is satisfied |
| RetryAt | Delay a transient failure according to the existing backoff/deadline; participate in ordering only when due |
| Terminal / Cancel | Follow existing failure or owner-shutdown handling, terminate work, and clean up owned references/callbacks |

A task runs at most once at any instant; wakeups and duplicate submissions are coalesced. Dependency checks and wait registration require a no-lost-wakeup contract so execution can continue whether the dependency completes before or after registration.

Yield is neither successful completion nor a failed retry. Wait does not mean blocking a worker on a dependency future. Cancel+Submit must not be used to reorder tasks while resetting backoff, references, or replay cursors.

### 6.2 Preserving Progress and Waiting for Dependencies

Resumable steps store cursors, completed stages, and unpublished intermediate resources in the task/resource owner. Scheduling occurs between completing one step and starting the next. An ongoing native/load call that cannot yet be split may finish before yielding. Arbitrary-instruction preemption and responses within a fixed number of milliseconds are not promised.

Waiting for an I/O permit, shared load result, WAL coverage, or barrier should remove the task from the general runnable queue. Actual I/O retains its own bounded execution capacity. Unbounded goroutines must not bypass concurrency and memory limits, and a task must not synchronously wait for a child task in the same exhausted executor pool.

Resources retained across Yield/Wait still count against memory budgets; pausing is not release. An I/O operation that has started keeps its permit until completion, and priority selection applies at the next admission. Release/cancellation must wake waiters and finish cleanup under existing ownership rules.

Cooperative yielding preserves valid intermediate state; real execution failures still follow the existing retry contract. For example, an SN build failure may require discarding part of the runtime and capturing a fresh snapshot. Error recovery must not be confused with a normal Yield.

### 6.3 Fairness

Priority defines order rather than directly acting as a weight. The proposed initial rule combines priority selection with a bounded run of priority dispatches:

1. Normally select the runnable task with the highest effective priority.
2. Round-robin between logical demand groups at the same priority, maintaining stable order within each group. A View with many Segments must not receive unlimited extra share.
3. After K consecutive priority dispatches, give the longest-waiting runnable group an opportunity so ordinary/negative-priority work continues to make progress.

K and the within-group execution quantum are parameters to determine. Resource managers provide stable fairness keys. Shared work occupies one task slot and must not be enqueued once per dependent View. Groups can use collection/vchannel identity; choose the exact key alongside existing owner structures.

This guarantees dispatch opportunities, not wall-clock latency or CPU shares. Long indivisible calls still limit responsiveness. Tasks in Wait/RetryAt do not bypass dependencies or backoff because of their age.

SN WAL progression, persistence, and release also need execution opportunities. The [TransformLog design](../wal/transform_log.md) documents the risk of bootstrap waits occupying all NodeScheduler workers. Cooperative waiting is necessary. With multiple workers, preparation concurrency can also be capped; queue ordering alone is insufficient. Even with one worker, a task must not synchronously wait on another task in the same pool. Reserved capacity must not be implemented by setting the preparation quota to zero.

## 7. QueryNode Integration

See [QN resource preparation](qnview/querynode_queryview_resource_preparation.md).

| Scheduling point | Implementation requirement |
| --- | --- |
| `SegmentResourceManager.Acquire` | Read the fixed priority when registering View requirements, preserving synchronous registration/reference protection |
| `segmentResource` runnable task | Aggregate dependencies this resource can advance, select the next node-local work item, and update the queued effective value when the shared set changes |
| `nextBindingLocked` | Publish existing compatible ready reads first, then select eligible bindings by priority and fairness; a blocked high-score requirement must not block other executable work |
| Segment load/read/index operations | Preserve preparation results and yield between safe resumable stages; progressively split coarse calls |
| Transform buffer drain queue | Propagate actual registration dependency priorities into bounded batch selection; preserve Delete/SyncUp application order within a registration |
| Collection/schema/metadata prerequisites | Wait as dependencies without occupying general workers; propagate demand priority into any actual admission queue |

Selection rules are required both within and between resources. Changing only the node task queue while retaining arbitrary binding selection can give a high-score View a slot that still advances unrelated low-score work.

Underlying native/object-storage executors may not support the new numeric priorities. The first step controls admission of new work and natural cooperative boundaries. Passing Control does not establish preemption of running CGO calls or already submitted lower-level I/O. The new signed score need not be forced into the old HIGH/LOW enum.

## 8. StreamingNode Integration

See [SN resource management](snview/streamingnode_resource_manager.md), [Growing Runtime](snview/growing_segment_runtime.md), and [IDF Runtime](snview/idf_oracle_runtime.md).

### 8.1 Shared Runtimes and Per-View Tasks

`PChannelRecoveryManager` routes by VChannel to resource modules; one runtime serves multiple Views. A shared build uses the maximum fixed priority of its dependent Views. Per-View readiness uses that View's requirements and inheritance by shared subtasks.

Priority does not change the oldest-requirement baseline, DataVersion admission, or historical retention conditions for a shared build. A newer, highest-score View must not directly replace the build snapshot and cause older dependencies to lose required resources.

### 8.2 Cooperative Boundaries

- Runtime initialization advances through resumable module/resource stages. Historical replay may yield after bounded batches while retaining its receive cursor and unpublished resources.
- Per-View readiness work for partition stats, load scope, schema/IDF/index preparation must identify shared waits so it does not occupy a worker while waiting on other tasks.
- IDF file loading considers dependency priority when acquiring a permit. A running low-score task must not acquire permits for all subsequent files without another selection. Preserve the existing memory constraint in which permits cover loading, decoding, and merging.
- If a live-event dispatcher applies barriers needed for preparation, it should reselect runtimes between bounded event batches. Events remain serial within each VChannel: do not reorder Delete/DDL/barrier events or starve runtimes already serving queries.
- Local UpRecovering reads the persisted View's fixed value without recomputing or adding a temporary score.

SN still shares physical executors; it does not duplicate runtimes by RG or add RG quotas. Business statistics remain independent between RGs, while preparing one shared runtime can help multiple RGs at once.

`recovery/scopedTaskScheduler` wraps tasks. Priority/wait handles must pass through that wrapper while preserving owner cancellation, tracking, and Close semantics. Adding interfaces only to inner tasks is insufficient if wrapping hides them.

## 9. Implementation Sequence

1. Complete the separate [SN access statistics and Coord aggregation](view_feedback.md), validating sampling and fallback for unknown statistics.
2. Add the fixed priority field and creation-time scoring entry points, connecting persistence, recovery, and first Acquire. Preserve the field across lease Control updates.
3. Extend common scheduling and the necessary wait/continuation mechanisms, first validating deduplication, cancellation, retry, fairness, and shared inheritance.
4. Integrate QN resource tasks, bindings, Transform catch-up, SN build/readiness, and critical permits/dispatchers.
5. Record actual limitations of coarse native calls and downstream queues, and use failure experiments to determine which contention points are controlled.

Implementation may be split across commits, but changing only send order or sorting the main queue must not be described as complete cooperative preparation priority. The opening section defines the implemented scope and limitations; validation evidence belongs in docs/plans.

## 10. Validation Requirements

### 10.1 Functionality and Concurrency

- Default 0, positive/negative boundaries, component clamping, and overflow; identical inputs produce identical scores.
- Statistics/configuration changes after creation do not modify existing Views. Retransmission, retry, and Coord/SN recovery retain the original value; lease updates preserve priority.
- Correct inheritance/removal when shared dependencies are added, satisfied, failed, or released, including all-negative dependencies, incompatible requirements, and no remaining waiters.
- High-score tasks get opportunities at cooperative boundaries; low-score tasks retain guaranteed progress. Equal scores rotate, large Views do not monopolize execution, and blocked high-score work does not block eligible low-score work.
- Yield/Wait do not complete handles, lose progress, or reset backoff. No lost wakeups, duplicate execution, or state corruption from late callbacks after cancellation.
- Single-worker execution, dynamic concurrency changes, and owner Close. WAL/persistence/release keep progressing while multiple SN bootstrap tasks wait.
- QN ordered Transform and SN ordered live-event/retention/schema/readiness contracts remain unchanged.
- Defaults for old/new messages and old nodes do not block the lifecycle when priority is unsupported.

### 10.2 Failure-Recovery Benefits

Compare preparation priority disabled/enabled under the same placement, data size, concurrency, and cold/warm cache conditions:

- Active and cold collections in one RG require preparation together. Measure recovery waiting time and time to sustained query success for active collections.
- Different RGs recover independently, without artificial cross-RG serialization. Independent QN resources within one RG can also proceed concurrently.
- Multiple Views of one VChannel share an SN runtime. Verify that shared preparation runs once and inherits fixed scores correctly.
- QN loss, SN/WAL owner replacement, slow storage, Transform backlog, large Segments, and BM25 file contention. Observe ordinary work and WAL progress as well.

Measure benefit as time from failure to sustained query success, separating detection, resource queueing, actual preparation, and existing routing recovery costs. Earlier dequeueing, earlier Ready callbacks, or a successful build are not proof of recovery benefit. Record the limitations caused by fixed scores on existing Views and indivisible calls.

Follow the repository build/cluster/test/feature-verify skills for subsequent validation. New/modified production functions require at least 90% statement coverage. This document reports no execution results or performance gains.

## 11. Settled Parameters and Further Discussion

- The initial scope, +2 configuration offset, demand scenarios, 2-minute recency threshold, logarithmic mapping of same-RG rate share over the last 6 windows, and unknown-statistics policy are settled; see Section 4 and the opening table.
- The current scheduler performs an age-based fairness dispatch after every 8 priority dispatches. Collection fairness keys and cooperative quanta for finer resource stages remain future work.
- Further details include the interface names and compatibility adapters in Section 12, and resumable boundaries for individual native/I/O stages.

These parameters and implementation interfaces remain open to review. The fixed architectural boundaries are SN's two access statistics, Coord replica feedback and same-RG rate shares, numeric values fixed at creation, cooperative node execution, and shared-dependency inheritance.

## 12. Resource-Preparation Scheduler Interfaces and Implementation Plan

This section specifies interface contracts that can guide implementation breakdown. The symbols are proposed APIs. Extend the same executor in `pkg/util/nodescheduler` rather than adding an unrelated preparation worker pool.

### 12.1 Submission and Task Handles

Retain `Scheduler.Submit(Task, ...Option) TaskHandle` and `Task.Execute(context.Context) error`. Add explicit cooperative interfaces so new methods are not discovered only through type assertions that the WAL scoped wrapper can hide:

```go
type Priority int32
type FairnessKey string // Resource owners provide stable groups; the scheduler does not interpret View/RG.

type CooperativeScheduler interface {
    Scheduler
    // Nonblocking admission; reject when closed without taking task ownership.
    SubmitCooperative(task CooperativeTask, opts ...Option) (CooperativeHandle, error)
}

type CooperativeHandle interface {
    TaskHandle // Cancel(), Wait(context.Context) error

    // Update only this resource task's effective value, not View Control.
    // Callable while queued, waiting, or running; return false after termination.
    SetPriority(priority Priority) bool
}

func WithPriority(priority Priority) Option
func WithFairnessKey(key FairnessKey) Option
func GetCooperative() CooperativeScheduler
```

`GetCooperative()` and the existing `Get()` return the same nodeScheduler instance, sharing concurrency configuration and workers. Existing tasks default to priority=0, and WithBackoff remains effective. Preparation resource owners retain handles from successful submissions. Discarding handles, as some current Submit callers do, would prevent effective-priority updates and correct cancellation of shared tasks.

Each resource owner serializes SetPriority calls, for example by computing and submitting under the resource lock. The scheduler accepts only the numeric value and does not call the owner under its own lock. A handle from an old acquisition cannot modify a later registration's task. For a retry not yet due or a task waiting on dependencies, SetPriority updates only the value; it does not make the task runnable early.

On successful SubmitCooperative, the scheduler takes execution ownership of the task. Rejection does not call OnFinished; the submitter rolls back the admission/references. Existing Submit signatures and closed-scheduler return behavior remain, using an internal adapter into the same execution state machine.

### 12.2 Cooperative Steps and Completion Callbacks

```go
type StepState uint8
const (
    StepDone StepState = iota
    StepYield
    StepWait
    StepRetry
    StepFailed
)

type StepResult struct {
    State      StepState
    Dependency WaitToken // StepWait
    NextRun    time.Time // StepRetry; zero uses the existing exponential backoff.
    Err        error    // StepRetry / StepFailed
}

type CooperativeTask interface {
    RunStep(ctx context.Context) StepResult

    // Called exactly once for a successfully submitted task, even if canceled before its first execution.
    // Runs outside scheduler/event locks to publish results and notify the owner.
    OnFinished(err error)
}
```

Done/Yield/Await/Retry/Fail constructors may prevent callers from creating contradictory results. RunStep's context belongs to the entire task. Yield/Wait neither cancel it nor create a new task identity.

| Result | Scheduler behavior | Handle completion |
| --- | --- | --- |
| Done | Mark terminal; OnFinished(nil) | After the callback returns |
| Yield | Keep the same entry and rejoin the fair queue without advancing backoff | No |
| Wait | Install one wait subscription and leave runnable state | No |
| Retry | Retain entry/backoff and enter the deadline heap | No |
| Failed | OnFinished(Err), without implicit retry | After the callback returns |
| Cancel | Cancel task ctx and revoke the wait; call OnFinished(ctx.Err()) after a running step exits | After the callback returns |

OnFinished performs only short result publication/in-memory notification. It does not perform I/O, wait for other tasks in the same pool, or traverse and release large resource sets. The owner's separate drain/release flow handles complex cleanup. The callback does not run concurrently with RunStep for the same task.

TaskHandle.Wait retains its existing meaning of waiting for task termination and returning errors from the caller's wait; it does not acquire new semantics for business execution errors. OnFinished publishes business results to the resource owner. A resourceBuildTask that needs Result stores it there and closes its own completion notification.

### 12.3 Dependency Interface Without Lost Wakeups

```go
// Event belongs to the dependency owner and stores only a change generation and wait subscriptions.
type Event struct { /* private */ }
type WaitToken struct { /* event identity + observed generation */ }

func NewEvent() *Event
func (e *Event) Observe() WaitToken
func (e *Event) Signal()
```

A typical step observes a token before checking the dependency:

```go
token := dependency.Changed.Observe()
value, ready, err := dependency.TryResult() // Nonblocking.
if !ready {
    return StepResult{State: StepWait, Dependency: token}
}
if err != nil {
    return StepResult{State: StepFailed, Err: err}
}
return task.advanceOneStep(value)
```

The dependency owner calls Signal after changing state. Observe must precede the condition check; reversing the order can miss a change. Notification does not guarantee dependency success. RunStep rechecks after waking, and failures follow existing business classifications.

When the scheduler registers a wait, Event compares generations under its short lock. If the generation has already changed, it reports that the task can retry immediately; otherwise, it registers the wakeup function. Registration does not invoke the callback inline. Signal invokes detached callbacks outside the Event lock. This avoids both a goroutine per waiter and polling.

Each entry carries a wait generation. A wakeup enqueues it once only if it is still Waiting and the generation matches. Old callbacks arriving after cancellation or another wait have no effect. Cancellation removes subscriptions; Events, tasks, and handles must not retain one another after termination. Resource owners can combine multiple prerequisites through one change Event; the first version does not add a general DAG scheduler.

### 12.4 Internal States, Queues, and Fair Selection

```text
Queued -> Running -> Done / Failed -> Completing -> Finished
                 -> Yield -> Queued
                 -> Wait -> Waiting -> signal -> Queued
                 -> Retry -> Delayed -> deadline -> Queued

Cancel: Queued / Waiting / Delayed -> Completing
        Running -> canceled context -> step returns -> Completing
```

Each successfully submitted entry stores its task/context, handle, effective priority, fairness key, state, ready sequence, queue indexes, retry backoff/deadline, and wait token/generation/unsubscribe handle. Continuations do not copy the task into a separate entry.

Suggested data structures:

- `groups[(priority, fairnessKey)]`: runnable entries ordered by ready sequence. An indexed min-heap can preserve waiting order after priority changes.
- A max-heap of nonempty priority buckets, with round-robin groups inside each bucket.
- An age min-heap of each group's oldest runnable entry, allowing selection of the oldest group after every K priority dispatches.
- A deadline min-heap and one timer, following the existing delayed-retry design.
- A Waiting table and Event subscriptions. These entries occupy neither runnable/deadline queues nor workers.

Normal dispatch selects the next group in the highest-priority bucket and then its earliest ready entry. Fairness dispatch selects the oldest group from the age heap. Dequeue and SetPriority maintain all affected indexes under the scheduler lock without scanning the entire queue.

SetPriority removes/reinserts a Queued entry while preserving the start of its current wait, so reordering does not erase queue age. Running/Waiting/Delayed entries only record the new value and use it when they next become ready. Yield means an execution opportunity has been consumed and assigns a new ready sequence on requeue. An old runnable entry repeatedly displaced by new work retains its waiting age.

Each resource submits only one active task. Resource owners may use stable collection/vchannel groups as FairnessKey. Adding shared dependencies does not increase task count or duplicate scheduling share. Other legacy tasks without explicit keys use a common default group with stable internal order.

Enqueueing, effective-value updates, and deadline transitions are typically O(log N); selection does not scan all Views. Measure the actual constant costs of individual heaps/lists during implementation. Dynamic concurrency changes retain worker resizing; shrinking takes effect only at step boundaries and does not lose Queued/Waiting/Delayed entries.

### 12.5 Completion, Cancellation, and Locking Rules

- Workers execute RunStep and OnFinished outside the scheduler lock.
- Cancel is nonblocking: it cancels the context, removes runnable/delayed/wait registrations, and schedules terminal handling. It must not synchronously invoke OnFinished under the resource lock held by its caller.
- Terminal entries enter an internal completion queue. Workers invoke OnFinished once outside locks. Close must drain this queue rather than closing queued-task handles without result notification. Handles close only after callbacks finish.
- Canceling a running entry does not release its task early. Terminate after the current RunStep returns and reject its Yield/Wait/Retry result.
- Wait registration does not invoke callbacks inline. Event.Signal invokes the short scheduler wakeup outside its lock; the scheduler does not call back into the dependency's business methods.
- Resource owners do not call handle.Wait while holding resource locks that completion callbacks may need.

Task-handle completion establishes only that RunStep and OnFinished are no longer running. If a task started bounded asynchronous I/O, its owner must also cancel and join those operations and wait for operation references to reach zero before releasing underlying resources. Wait does not imply that the entire resource lifecycle has ended.

After the global scheduler closes, owners use their existing shutdown flows for subsequent complex cleanup rather than relying on submitting release tasks to the closed pool. The rule against synchronously waiting for a task in the same pool also applies with a single worker.

### 12.6 Compatibility Adapters and Resource Tasks

**Legacy adapter.** Map Execute returning nil to Done, ErrDelay/DelayUntil to Retry, and other errors to Failed. Preserve existing retry deadlines and default backoff. Legacy tasks remain nonpreemptible until Execute returns; retain that limitation in validation reports.

**SN scoped wrapper.** `scopedTaskScheduler` also implements CooperativeScheduler and uses the same global executor. A new trackedCooperativeTask forwards RunStep and completes its scoped entry only in OnFinished; it also forwards CooperativeHandle.SetPriority. Unlike the old trackedTask.Execute path, it must not treat Yield/Wait as completion merely because they are not ErrDelay. Owner shutdown cancels its handles and waits for completion without closing the global scheduler.

**Resource-priority aggregation.** QN/SN resource managers retain immutable View priorities, the dependency set, and one resource task handle. After dependencies join, complete, or leave, compute the maximum and call SetPriority under the resource lock. Cancel when no preparation dependencies remain. Ordinary exclusive tasks set WithPriority only on submission. There is no View-level UpdatePriority API.

**QN Segment task.** Split reconcile into resumable states: select an eligible binding, obtain metadata/prerequisites, perform one physical preparation stage, then publish a compatible read or arrange catch-up. Short selection/publication runs under the resource lock; I/O/native stages run outside it. Await if the stage cannot progress, or Yield after a portion completes if work remains. Publish already satisfied compatible bindings first without reloading them. Binding selection uses the same fixed priorities and fairness ordering.

**QN catch-up.** Registrations retain unique execution and batch progression semantics. Their existing drain pool can reuse the same priority queue and effective-value source, rather than becoming another unbudgeted global task stream. Yield/requeue after a page; park while waiting for coverage and wake on buffer arrival. Moving into the common executor must preserve the existing catch-up concurrency budget, not increase parallelism merely by moving the queue.

**SN build/ready task.** Retain the stage, current build snapshot, unpublished runtime, replay cursor, and completed module results in the task. Yield between stages and Await unfinished shared futures/barriers. Real build failures follow the existing fresh-build retry contract; Yield does not discard the runtime. Ready still requires every existing condition to be satisfied.

**I/O and permits.** First attempt budgeted admission. If a permit is unavailable, Await a capacity Event. Once admitted, start one bounded operation, wait for its completion Event, then advance the stage. The asynchronous operation itself holds the permit and operation reference, with completion/cancellation releasing them exactly once. Moving blocking calls into unbounded goroutines is insufficient. For native calls that cannot yet be split, acknowledge the coarse RunStep and yield after it returns.

**SN live events.** Preserve serial application within each runtime and make each drain a bounded batch. Remaining work returns to the queue for selection. Preparation dependencies can raise that runtime's effective priority, but runtimes already serving queries retain fair progress. Internal event order remains unchanged.

### 12.7 Required Interface-Level Validation

In addition to Section 10, verify: rejected submissions do not transfer ownership; cancellation before execution still invokes OnFinished exactly once; Wait does not return early after Yield/Wait/Retry; notifications before the dependency check, after the check, during wait installation, and after cancellation neither lose wakeups nor duplicate execution; SetPriority behaves correctly in the four nonterminal states and terminal state; priority changes preserve queue age; dependencies and completion notifications progress with one worker; scoped wrappers do not remove tasks early; Close drains the completion queue; and existing Execute/ErrDelay callers remain compatible.

Implementation submissions must separately measure and validate step continuation state, cancellation, and resource draining. These interfaces do not imply that adding a priority heap alone completes the feature.
