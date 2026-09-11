# Phase 0: Schema Install Gate

Status: Baseline implemented; rollout and fault-injection validation pending
Parent design: [Online Schema Evolution](../20260715-online-schema-evolution.md)

Deployment contract for this delivery phase:

- coordinators run only as one in-process MixCoord;
- an old coordinator is never paired with a new QueryNode;
- schema-changing DDL is admitted only when every currently registered Milvus
  service session reports released version `3.0.1` or later;
- absent roles are allowed, but an unreadable, missing, malformed, old, or
  pre-release session version fails closed with retriable `ServiceNotReady`;
- the repository development version may remain `3.0.0-beta`; the contract is
  enabled for production only by the eventual `3.0.1` release or a later
  release.

## 1. Purpose

Phase 0 provides the first production safety boundary for online schema
evolution. It serializes a collection schema installation with query-side
topology mutations that could otherwise install a stale collection schema,
schema barrier, function runtime, index view, or distribution state on a
QueryNode.

The gate protects the short schema-installation window. It is not held for the
entire Write Only or Data Build stage. Once the target full schema and barrier
have been installed safely, load and balance resume using the target schema
epoch while user reads may continue to observe the previous Read view.

The central safety property is:

```text
No query-topology operation captured under an epoch older than the target
schema epoch may commit after the target phase-1 cutover fence.
```

## 2. Motivation

Schema broadcast and query-side load/balance are currently independent enough
to form the following race:

```text
T0  QueryCoord reads collection schema N and plans a segment/channel move.
T1  RootCoord starts installing schema N+1.
T2  QueryNode consumes schema N+1.
T3  The operation planned at T0 sends or commits a payload based on schema N.
T4  QueryNode or QueryCoord distribution state is changed by the stale payload.
```

Waiting until every QueryNode reports schema version N+1 does not close this
race. A stale payload may arrive after the readiness check. This is a
time-of-check/time-of-use problem, so readiness must be combined with
admission control, in-flight operation tracking, and receiver-side fencing.

The gate distinguishes two concepts:

- A **legacy segment** is a sealed segment whose own schema version is N. It is
  valid and may continue to be loaded and queried during Write Only.
- A **legacy topology operation** is a load, balance, transfer, watch, reopen,
  or distribution update planned using collection schema epoch N. It must not
  commit after the N+1 cutover fence.

Phase 0 forbids the second case, not the first.

## 3. Relationship to the F1-style protocol

During the stable Write Only stage:

| Plane | Required schema semantics |
| --- | --- |
| User reads | Previous user-visible field set remains serviceable |
| Proxy writes | Target Write schema N+1 |
| StreamingNode | Target Full/Write schema N+1 |
| QueryNode internal state | Target Full schema N+1 |
| QueryNode user planning | Read view derived from N+1, with pending fields hidden |
| Historical sealed segments | May retain segment schema N |
| New growing segments | Use segment schema N+1 |
| New load/balance operations | Must be planned and committed under target epoch N+1 |

The install gate covers only the transition into this state:

```text
Stable schema N
    -> close topology admission
    -> drain or fence operations planned under N
    -> broadcast and install schema N+1
    -> verify target schema/barrier and topology epoch
    -> reopen topology admission under N+1
    -> continue Write Only and Data Build
```

Holding the gate until backfill, index, or IDF completion would violate the
online design goal. Those conditions belong to the later publish-readiness
gate.

## 4. Scope

### 4.1 Operations gated for the collection

The rule is semantic: an operation is gated if it can install schema-dependent
state or change the node that serves a segment or channel.

The initial implementation must cover:

- `LoadCollection` and `LoadPartitions`;
- collection/partition reload and schema-dependent refresh;
- manual `LoadBalance`;
- `TransferSegment` and `TransferChannel`;
- automatic segment and channel balance;
- stopping-node segment and channel balance;
- node-failure recovery that assigns a segment/channel to another node;
- segment reopen and reload paths;
- channel watch/subscription;
- `LoadSegments`, `WatchDmChannels`, and schema-dependent
  `SyncDistribution`/set-distribution actions.

The current primary entry points include
[internal/querycoordv2/services.go](../../../../internal/querycoordv2/services.go),
[internal/querycoordv2/ops_services.go](../../../../internal/querycoordv2/ops_services.go),
and the checker/balancer/task packages below `internal/querycoordv2`.

### 4.2 Operations not gated by Phase 0

Phase 0 does not stop the collection as a whole. It should not directly block:

- ordinary Search/Query on the existing Read view;
- Insert/Upsert that passes the target write-schema/version checks;
- Flush;
- compaction and index build computation that does not publish query topology;
- Describe and other metadata reads;
- operations on unrelated collections.

If a background operation eventually triggers a QueryNode reload/reopen, its
publication or topology-mutation step is gated even when its computation is
not.

## 5. Three distinct gates

The implementation must not represent all schema-evolution coordination with a
single Boolean.

| Mechanism | Lifetime | Responsibility |
| --- | --- | --- |
| DDL admission lease | Protocol phase 1 through phase 2 | Serialize schema DDL and backfill publication for one collection |
| Schema install gate | Before phase-1 broadcast through target-schema installation | Prevent stale query-topology mutations from committing |
| Publish readiness gate | Data Build through phase-2 publish | Decide when the new Read view and data view may be published |

This document specifies only the schema install gate. A minimal durable install
record is still required for crash safety, but it is not the complete persisted
schema-evolution task designed for later phases.

## 6. Safety and liveness invariants

### 6.1 Safety

1. No topology operation with `captured_epoch < target_epoch` commits after the
   target cutover fence.
2. No stale `LoadSegments`, `WatchDmChannels`, reopen, or distribution request
   produces a visible side effect after the target schema/barrier is installed.
3. Every topology operation admitted after gate release is planned from the
   target Full schema and carries the target epoch/barrier.
4. QueryNode never lets an older load payload overwrite a newer collection or
   Delegator schema snapshot.
5. A segment remains bound to exactly one segment schema version. Loading an old
   segment under a target collection schema does not rewrite the segment's own
   version.
6. A post-cut failure never performs an unsafe schema rollback from N+1 to N.

### 6.2 Liveness

1. Failures before a durable phase-1 cut can abort and reopen the gate under N.
2. Failures after the durable cut enter recovery and are retried idempotently.
3. A permanently failed participant can be removed from the relevant loaded
   topology only through a fenced target-epoch recovery path.
4. Once every required participant is ready and no stale commit can succeed,
   load/balance admission reopens under N+1.
5. Backfill/index/IDF delays do not keep the install gate closed.

## 7. Ownership and durable state

### 7.1 RootCoord ownership

RootCoord owns the schema DDL and the durable installation intent. Before asking
QueryCoord to quiesce, it creates a minimal install record:

```protobuf
message SchemaInstallRecord {
  int64 collection_id = 1;
  string operation_id = 2;
  int32 from_schema_version = 3;
  int32 target_schema_version = 4;
  SchemaInstallState state = 5;
  uint64 topology_epoch = 6;
  repeated ChannelBarrier target_barriers = 7;
  string last_error = 8;
  int32 retry_count = 9;
}
```

The exact storage format is implementation-dependent. The durability contract
is not: after a coordinator restart, the system must know whether the operation
is pre-cut and abortable or post-cut and recoverable only by moving forward.

### 7.2 QueryCoord ownership

QueryCoord owns topology admission, in-flight topology leases, task fencing,
distribution convergence, and QueryNode readiness aggregation.

The gate manager must be collection-scoped and shared by API jobs, manual
operations, checkers, automatic balancers, stopping balancers, and recovery
schedulers. Separate per-RPC flags would leave uncovered paths.

### 7.3 QueryNode ownership

QueryNode owns the final receiver fence. It validates the request epoch/barrier
before worker loading or distribution mutation and coordinates the validation
with concurrent schema updates.

Receiver fencing is mandatory even when QueryCoord drains tasks correctly. It
protects against RPC delay, retries, coordinator failover, and bugs in an
admission path.

## 8. Gate state machine

```text
Open
  |
  v
Quiescing
  |  reject new external/background topology operations
  v
DrainingLegacy
  |  wait, cancel, or fence operations captured under the old epoch
  v
ReadyToBroadcast
  |  no old-epoch operation can commit past the future cut
  v
Broadcasting
  |  append/install phase-1 schema and obtain per-channel barriers
  v
AwaitingApply
  |  wait for Streaming/Data/Query control-plane readiness
  v
Releasing
  |  establish target-epoch topology baseline and reopen admission
  v
Open(target epoch)
```

Failure states:

```text
PreCutAbort
  The phase-1 schema is not durable. Abort, reopen under the old epoch, and
  return a retriable system error.

PostCutRecovery
  The phase-1 schema may be durable on at least one required WAL/control path.
  Preserve N+1, keep topology fenced, and recover forward.
```

### 8.1 Transition requirements

| Transition | Required proof |
| --- | --- |
| Open → Quiescing | Durable install intent and unique operation ID exist |
| Quiescing → DrainingLegacy | All topology admission sources observe the closed gate |
| DrainingLegacy → ReadyToBroadcast | Old-epoch task leases are zero or every remaining task is guaranteed to fail its commit fence |
| ReadyToBroadcast → Broadcasting | RootCoord still owns the operation and the collection schema is still at `from_schema_version` |
| Broadcasting → AwaitingApply | The phase-1 broadcast outcome is durably resolved and target barriers are known |
| AwaitingApply → Releasing | All required install-readiness conditions pass |
| Releasing → Open | New tasks can only capture the target epoch/full schema/barrier |

## 9. Topology operation lease and fencing

### 9.1 Lease acquisition

Every topology operation must acquire a collection operation lease before it
reads schema-dependent planning state. The lease captures:

```text
collection_id
operation_id
topology_epoch
collection_schema_version
schema_barrier(s)
task_source
```

Lease acquisition and gate closure must be atomic relative to each other:

- if lease acquisition wins, the gate observes and drains/fences the operation;
- if gate closure wins, the new operation is rejected or delayed.

A lease is held through the last visible topology commit, not merely through
plan generation.

### 9.2 Task lifecycle handling

When the gate enters `DrainingLegacy`:

| Existing task state | Required action |
| --- | --- |
| Queued, no payload built | Cancel or mark stale; do not execute under the old epoch |
| Planning | Stop at the next fence point and re-plan after gate release |
| Payload built, RPC not sent | Discard the payload; never retry it unchanged |
| RPC in flight | Let the receiver fence decide; wait for a terminal response |
| Worker load completed, distribution not committed | Revalidate epoch before commit; discard/release loaded resources if stale |
| Distribution committed before the cut | Include the result in the pre-cut baseline and then install the target schema |

Initial Phase 0 should prefer drain/cancel over transparent multi-version
execution. A later optimization may allow a bounded old operation to complete
only if its lease and final commit are ordered before the cutover fence.

### 9.3 QueryCoord commit fence

Before mutating QueryCoord distribution or marking a task successful:

```text
task.topology_epoch == gate.current_epoch
AND task.schema_version >= gate.required_schema_version
AND task.barrier is not older than gate.required_barrier
```

Failure marks the task stale. It must be re-planned from current metadata rather
than retried with the original request.

### 9.4 QueryNode receiver fence

`LoadSegments`, `WatchDmChannels`, reopen, and distribution requests must carry
the captured epoch/schema/barrier. QueryNode validates them before side effects.

Schema installation and topology requests need local ordering. A recommended
model is:

- topology requests take a shared collection-schema guard, validate, execute,
  and publish their local result under that guard or a commit token;
- schema update takes the exclusive guard, advances the schema/barrier, and
  invalidates older request tokens;
- if long worker loading cannot hold the guard, perform validation both before
  loading and immediately before publication, cleaning up a stale partial load.

The existing logical schema version and `SchemaBarrierTs` checks should be
reused, but the gate also needs an explicit topology/evolution epoch so that
same-version visibility updates and delayed operations are not conflated.

## 10. Proposed control flow

The exact transport can be an internal RPC, callback, or persisted observer.
The semantic protocol is:

```text
RootCoord                         QueryCoord                    QueryNode/Streaming/Data
    |                                  |                                  |
    | persist install intent           |                                  |
    | PrepareSchemaInstall(op, N, N+1) |                                  |
    |--------------------------------->| close admission                   |
    |                                  | drain/fence old leases            |
    |<---------------------------------| ready-to-broadcast(epoch)         |
    |                                  |                                  |
    | broadcast phase-1 schema N+1 -------------------------------------->|
    | resolve durable barriers         |                                  |
    | CommitInstallBarrier(op, barriers)                                  |
    |--------------------------------->| observe/update target readiness   |
    |                                  |--------------------------------->|
    |                                  |<---------------------------------|
    |<---------------------------------| installed(target epoch/barriers)  |
    | FinalizeSchemaInstall(op)         |                                  |
    |--------------------------------->| reopen topology admission         |
    |                                  |                                  |
```

Candidate internal operations:

```go
PrepareSchemaInstall(collectionID, operationID, fromVersion, targetVersion)
    -> topologyEpoch

CommitSchemaInstallBarrier(operationID, topologyEpoch, channelBarriers)

GetSchemaInstallReadiness(operationID, topologyEpoch)
    -> per-replica/per-channel readiness and blockers

FinalizeSchemaInstall(operationID, topologyEpoch)

AbortSchemaInstall(operationID, topologyEpoch) // pre-cut only
```

The API names are illustrative. Idempotency by `(collectionID, operationID)` and
epoch validation are required regardless of the final transport.

## 11. Install readiness

The gate can enter `Releasing` only when:

```text
all required phase-1 WAL/control updates are durable
AND old_epoch_topology_leases == 0
AND no stale topology commit can succeed
AND DataCoord can serve target-schema load metadata
AND all relevant QueryNode/Delegator instances applied target schema/barriers
AND QueryCoord distribution metadata is fenced at the target epoch
```

### 11.1 Relevant participants

Readiness does not mean every physical QueryNode in the cluster. It covers:

- loaded replicas for the collection;
- their current shard leaders and workers;
- relevant vchannels/pchannels;
- nodes participating in the distribution baseline captured by the closed gate;
- target-epoch repair/recovery nodes explicitly admitted by the gate owner.

Because TimeTick is comparable only within a PChannel, barriers are tracked per
channel and then aggregated at collection level.

### 11.2 Conditions excluded from Phase 0 readiness

The following block phase-2 publication, not install-gate release:

- historical backfill completion;
- new-field index completion;
- BM25 IDF readiness;
- external refresh completion, unless the refresh is required to produce valid
  target-schema load metadata for QueryCoord;
- final drop drain and physical cleanup.

## 12. Component responsibilities and failure handling

### 12.1 RootCoord

Responsibilities:

- serialize schema installation for a collection;
- allocate an operation ID and persist the minimal install record;
- ask QueryCoord to quiesce before broadcasting;
- resolve ambiguous broadcaster outcomes instead of assuming timeout means
  failure;
- persist whether the operation is pre-cut or post-cut;
- drive retry/recovery and finalize the gate.

Failures:

| Failure | Handling |
| --- | --- |
| Install-record persistence or version allocation fails | Do not close/broadcast; return retriable SystemError |
| QueryCoord cannot quiesce or drain before timeout | Abort pre-cut; reopen under N; no schema mutation |
| Broadcast fails before any durable append | Mark `PreCutAbort`, release gate, retain N |
| Broadcast result is ambiguous or partially durable | Enter `PostCutRecovery`; never roll back blindly |
| ACK callback/DataCoord refresh fails after durable append | Keep N+1 pending, retry idempotently, keep topology fenced |
| RootCoord restarts | Recover the install record before accepting conflicting DDL; reconcile broadcaster and QueryCoord state |

If the user RPC times out after the durable cut, the response must describe a
pending/retriable system operation rather than claim rollback. Reissuing the
same DDL must be idempotent by operation identity or recognized target state.

### 12.2 QueryCoord

Responsibilities:

- own the collection gate manager and topology epoch;
- atomically close admission and track active leases;
- cover API jobs, checkers, all balancers, transfers, and recovery scheduling;
- reject old-epoch distribution commits;
- aggregate per-node/channel install readiness;
- re-plan stale tasks from target metadata after release.

Failures:

| Failure | Handling |
| --- | --- |
| A scheduler creates a task after gate closure | Reject/skip it and report an invariant violation metric |
| A queued/planning task holds the old epoch | Cancel or mark stale; release its lease |
| An old RPC is in flight | Await terminal result; rely on QueryNode fence; do not mark success from transport alone |
| Old-epoch distribution commit is attempted | Reject, clean partial state, and re-plan under target epoch |
| QueryNode is unreachable | Keep participant not ready; retry or safely remove it through target-epoch recovery |
| QueryCoord restarts | Restore gates before starting checkers/balancers; fail closed for collections with unresolved install records |
| Readiness times out | Return blockers to RootCoord; do not release the gate |

Automatic and stopping balance should skip a gated collection and retry later,
not classify the collection as permanently unbalanceable.

### 12.3 QueryNode

Responsibilities:

- apply the target Full schema and per-channel barrier;
- update collection, Delegator, function runtime, and local schema snapshots
  consistently;
- fence stale load/watch/reopen/distribution requests before visible effects;
- expose readiness only after all required local state is installed.

Failures:

| Failure | Handling |
| --- | --- |
| Stale topology request arrives | Return a retriable schema/barrier mismatch; do not publish local/distribution state |
| Schema update partially fails | Do not report ready; retain pending target and retry idempotently |
| Worker load succeeds but final epoch check fails | Release/discard the partial load and return stale-operation error |
| QueryNode restarts during install | Recover/load target schema and barrier before joining readiness |
| Segment load fails for storage/index/resource reasons | Return classified SystemError; retry or move to another node; do not roll back schema |

An expected stale request must not blacklist a healthy QueryNode.

### 12.4 StreamingNode

Responsibilities:

- use the phase-1 `AlterCollectionMessage` TimeTick as the write-schema cut;
- flush/fence old growing segments;
- install target Full/Write schema;
- stamp new segments with target schema version;
- reject stale writes that cannot preserve the single-schema segment invariant.

Failures:

| Failure | Handling |
| --- | --- |
| Flush/fence fails before local apply | Do not acknowledge apply; retry or surface post-cut recovery depending on WAL durability |
| WAL is durable but live apply fails | Recover forward through WAL replay; do not allocate mismatched segments |
| Stale insert arrives after barrier | Reject with schema mismatch; do not allocate or mix segment versions |
| StreamingNode restarts | Reconstruct schema history, phase-1 barrier, and segment schema versions before accepting writes |

Streaming readiness is established by the durable broadcast/apply contract; a
partial or ambiguous channel result prevents install finalization.

### 12.5 DataCoord and DataNode

Phase 0 does not require historical backfill, but QueryCoord must not reopen
load admission until DataCoord can provide target-schema metadata for new load
payloads.

Failures:

| Failure | Handling |
| --- | --- |
| Altered-collection schema refresh fails | Retry; keep install not ready because future loads could use stale metadata |
| Segment metadata has an old segment schema version | Allowed when it accurately describes a legacy segment |
| Target collection schema metadata is missing/inconsistent | Block release and report retriable not-ready |
| Backfill/index task fails | Record publish-readiness failure; do not keep Phase 0 gate closed after installation is otherwise safe |
| DataNode fails | Retry its data task independently unless it prevents DataCoord from serving valid target load metadata |

### 12.6 Proxy

Proxy is not a query-topology owner, but its cache and request behavior determine
the user-visible transition.

Failures:

| Failure | Handling |
| --- | --- |
| Schema cache expiration fails | Do not silently claim a global cut; stale writes are fenced downstream and may return retriable mismatch |
| Schema refresh fails | Return retriable service error rather than planning with known-invalid metadata |
| Stale write reaches StreamingNode | Refresh/retry if safe, otherwise return schema mismatch |
| Stale read plan references an invisible/dropped field | Return field-not-visible/InputError without blacklisting QueryNode |

For additive changes, Proxy cache failure alone should not hold the topology
install gate forever. Drop finalization has a separate cache and in-flight-plan
drain requirement.

### 12.7 Segcore and error projection

- User references to absent/invisible fields are InputErrors.
- Schema install not-ready, stale topology epochs, unreachable nodes, storage
  failures, and resource failures are SystemErrors, retriable where applicable.
- `FieldIDInvalid` must preserve its input-error projection.
- Stale load/watch requests must not be collapsed into permanent user errors.

## 13. Pre-cut versus post-cut recovery

### 13.1 Pre-cut failure

A failure is pre-cut only when RootCoord can prove that no required phase-1
schema update became durable.

Recovery:

1. Mark the install record aborted.
2. Cancel/finalize QueryCoord drain state.
3. Reopen topology admission under the original epoch N.
4. Return a retriable system error to the DDL caller.

### 13.2 Post-cut failure

If any required WAL/control path may contain the target schema, the operation is
post-cut. Recovery moves forward:

1. Preserve target schema N+1 and operation identity.
2. Keep old-epoch topology operations fenced.
3. Resolve broadcast state and replay missing updates.
4. Refresh DataCoord target metadata.
5. Retry QueryNode/Delegator schema application.
6. Recompute readiness and release only when all invariants pass.

Do not write schema N back as a compensating action. Writes or segments may
already have crossed the N+1 barrier.

## 14. Error behavior

Recommended error categories:

| Situation | Error category |
| --- | --- |
| New load/balance/transfer while gate is closed | Retriable schema-install-in-progress / ServiceNotReady |
| Old topology request rejected by epoch/barrier fence | Retriable stale-schema-operation |
| Relevant node unavailable during apply | Retriable Unavailable |
| DDL fails before durable cut | Retriable system failure; operation aborted |
| DDL times out after durable cut | Operation pending/recovery required; never report successful rollback |
| User references an invisible or removed field | InputError |

The request content does not cause install-gate blocking, so these gate errors
must never be classified as InputError.

## 15. Observability

### 15.1 Metrics

- active schema install gates by state;
- time spent in quiescing, draining, broadcasting, awaiting apply, and recovery;
- active old-epoch topology leases;
- stale tasks canceled or re-planned;
- stale requests rejected at QueryCoord and QueryNode;
- participant readiness counts and timeout totals;
- pre-cut aborts and post-cut recoveries;
- gate reopen latency.

Use bounded labels such as component, state, operation type, and error category.
Do not use collection ID or operation ID as metric labels.

### 15.2 Logs and status

Structured logs and an admin/debug status should include:

- collection ID and operation ID;
- from/target schema versions and topology epoch;
- per-channel barriers;
- gate state and state-enter timestamp;
- active leases and task sources;
- not-ready participants and reasons;
- last error and retry count.

## 16. Testing strategy

### 16.1 Unit tests

- gate state transitions and idempotent duplicate requests;
- atomic lease acquisition versus gate closure;
- old-epoch task cancellation and target-epoch re-plan;
- QueryCoord distribution commit fencing;
- QueryNode pre-load and pre-publish epoch/barrier checks;
- pre-cut abort versus post-cut recovery classification;
- per-channel readiness aggregation;
- coordinator restart recovery before scheduler startup.

### 16.2 Concurrency and integration tests

1. Start LoadCollection while phase-1 schema installation begins.
2. Run manual and automatic segment balance concurrently with schema change.
3. Run channel transfer and stopping-node balance concurrently with schema
   change.
4. Delay an old `LoadSegments` RPC until after every QueryNode reports N+1; verify
   the receiver still rejects it and readiness remains valid.
5. Complete worker loading under N, advance to N+1 before distribution commit,
   and verify cleanup plus re-plan.
6. Restart RootCoord in each gate state.
7. Restart QueryCoord with active old-epoch leases and verify fail-closed
   reconstruction.
8. Restart a QueryNode during `AwaitingApply` and verify it cannot report ready
   before target recovery.
9. Inject DataCoord schema-refresh failure after durable broadcast.
10. Inject partial/ambiguous broadcast completion across channels.
11. Verify legacy schema-N segments remain loadable after gate release using
    target Full schema N+1.
12. Verify backfill/index/IDF delays do not keep the install gate closed.

### 16.3 Required assertions

- No old-epoch topology mutation commits after the target cut.
- No stale request changes QueryNode or QueryCoord visible distribution state.
- New post-release tasks always carry target epoch/schema/barrier.
- Normal Search/Query remains available on the valid Read view.
- Gate failures are retriable SystemErrors.
- Post-cut failure never rolls schema back to N.
- A healthy QueryNode is not blacklisted for rejecting an expected stale request.

## 17. Rollout plan

1. Add epoch/barrier fields and receiver validation behind a feature flag.
2. Add QueryCoord operation leases and cover all topology task sources.
3. Add the RootCoord-to-QueryCoord quiesce/install/finalize protocol.
4. Add minimal durable install records and restart recovery.
5. Enable fail-closed audit mode that records uncovered task sources without
   blocking production traffic.
6. Enable enforcement for test and canary clusters.
7. Enable collection-level enforcement by default after concurrency and restart
   fault-injection passes.

Mixed-version admission is fail closed. RootCoord checks every registered
service role before closing the collection gate or creating the durable
broadcast task. QueryCoord repeats the same check before opening the gate after
apply/recovery. Therefore a rolling upgrade may continue serving ordinary
traffic, but schema-changing DDL remains unavailable until all registered nodes
have reached released version `3.0.1` or later.

## 18. Implementation status (2026-08-12)

The Phase 0 baseline is implemented for the in-process MixCoord deployment.
The implementation establishes the install boundary and receiver fencing, but
it does not yet implement the full durable protocol proposed in Sections 7-10.
In particular, rollout must not treat this baseline as the later phase-1/phase-2
schema-evolution state machine.

### 18.1 Implemented control flow

The current control flow is:

```text
RootCoord holds the collection broadcaster resource lock
    -> QueryCoord closes collection topology admission
    -> QueryCoord drains admitted jobs, tasks, and direct-operation leases
    -> broadcaster registers/persists the AlterCollectionV2 task
    -> StreamingNode/WAL paths apply and acknowledge the target schema
    -> RootCoord ACK callback updates RootCoord metadata
    -> DataCoord synchronously refreshes its cached collection schema
    -> RootCoord expires Proxy caches and completes other ACK work
    -> QueryCoord installs schema + barrier on current distribution holders
    -> QueryNode fans the install out to delegators and workers
    -> QueryCoord reopens topology admission
```

The implementation is located primarily in:

- `internal/schemaevolution/install_gate.go`;
- `internal/rootcoord/schema_install_gate.go` and the AlterCollection ACK
  callback;
- `internal/querycoordv2/schema_install_gate.go`, the task/job schedulers,
  target observer, services, and task executor;
- `internal/querynodev2/services.go`, `delegator/`, and `segments/`;
- `internal/streamingcoord/server/broadcaster/` for durable pending-task
  recovery.

### 18.2 Current gate representation

QueryCoord uses a collection-scoped in-memory gate with an active-operation
count. `Close` and `Acquire` share one mutex, so a topology operation is either
counted in the drain or rejected as `ServiceNotReady`. The gate itself is not
persisted.

Crash recovery currently derives unresolved installs from durable, non-
tombstoned `AlterCollectionV2` broadcaster tasks. In MixCoord startup,
RootCoord restores those collection gates before QueryCoord starts its task
scheduler, checker controller, job scheduler, and topology observers. An ACK
callback also closes the gate idempotently before continuing post-cut recovery.

QueryCoord must also recover the initial distribution from every registered
QueryNode before it becomes Healthy or starts schedulers/observers. A failed
distribution pull aborts QueryCoord startup. This prevents a recovered pending
schema ACK from observing an empty or partial distribution and incorrectly
opening the gate with zero participants.

This is intentionally smaller than the proposed `SchemaInstallRecord`. It does
not persist an operation ID, from/target versions, an explicit topology epoch,
per-participant readiness, retry count, or last error.

### 18.3 Lock-order rule for load intent

Normal `LoadCollection` and `LoadPartitions` do not hold a topology lease while
waiting for the collection broadcaster resource lock. Holding it would create
this deadlock:

```text
load:       topology lease -> collection broadcaster resource lock
schema DDL: collection broadcaster resource lock -> close/drain topology gate
```

Instead, normal load performs an admission check before broadcasting. The
shared collection resource lock orders the load intent against schema DDL, and
the delayed load ACK callback acquires the topology lease around the actual
replica/target metadata mutation. A load intent that was ordered before the
schema DDL but whose ACK is delayed is rejected while the gate is closed and is
retried by the broadcaster after gate release.

Refresh mode directly mutates target state without a broadcaster intent, so it
holds a topology lease for the complete refresh operation.

### 18.4 Covered QueryCoord mutation sources

The baseline covers:

- normal and refresh load paths, including delayed load ACK mutation;
- manual load balance and transfer helpers;
- replica/load-config updates;
- task-scheduler segment, channel, and leader tasks for their full lifetime;
- queued/running job draining;
- target-observer current-target and direct `SyncDistribution` paths;
- checker, automatic balance, stopping-node balance, and recovery work that
  reaches the shared task scheduler;
- schema/barrier propagation on every QueryCoord-produced `LoadSegments`,
  `WatchDmChannels`, and schema-dependent `SyncDistribution` request.

The task scheduler releases its topology lease only at terminal task removal.
Expected stale-schema rejection is not recorded in the long-lived failed-load
cache and does not apply a resource-exhaustion penalty to a healthy QueryNode.

### 18.5 QueryNode receiver boundary

QueryNode serializes coordinator schema installation with `LoadSegments`,
`WatchDmChannels`, and `SyncDistribution` at collection scope. Receiver checks
compare the request collection schema version and `SchemaBarrierTs` with the
local collection snapshot.

Full segment load now has a staged commit boundary: loaded segments are not
published to `SegmentManager` until a final schema/barrier check succeeds. If
the request becomes stale, staged local segments are released. Delegator remote
loads perform the same post-worker check and issue a best-effort
`ReleaseSegments` cleanup before returning the retriable stale-schema error.
Legacy L0 ownership is preserved: normal loader publication still leaves L0
segments with the Delegator delete buffer.

Channel watch starts its pipeline/delegator only under the final receiver
fence, and its existing deferred cleanup removes partially constructed local
state on rejection. Distribution metadata and distribution-delta markers are
published only after the fenced operation succeeds.

Coordinator-driven `UpdateSchema` is explicitly identified by
`MsgType_AlterCollectionSchema`. It propagates to all local delegators, and
each delegator propagates to its current workers. Equal-version/barrier retries
still reach workers, allowing recovery after a prior partial fanout. A
process-local context marker prevents a local worker callback from recursively
acquiring the same non-reentrant topology lock; remote worker RPCs retain their
own receiver lock.

### 18.6 Failure and recovery behavior

The implemented cut classification is:

- before gate close, RootCoord checks the cluster version contract; session
  lookup failure or any registered node below released `3.0.1` rejects the DDL,
  creates no broadcaster task, and leaves the gate open;
- if gate preparation fails before `Broadcast`, RootCoord aborts and reopens;
- if `Broadcast` returns `ErrBroadcastTaskNotCreated`, the broadcaster task was
  not registered, so RootCoord treats it as an unambiguous pre-cut abort;
- any other broadcast timeout/error is treated as ambiguous post-cut state and
  leaves the gate closed;
- once the broadcaster task is registered, its recovery record is persisted
  before WAL append and the broadcaster retries append/ACK work forward;
- RootCoord ACK, DataCoord cache refresh, Proxy cache expiration, bound-index
  apply, QueryCoord completion, and QueryNode fanout failures keep the task
  pending and are retried idempotently;
- QueryNode unavailability or partial schema fanout keeps the QueryCoord gate
  closed; successful participants are not rolled back;
- a node joining with an old version after the durable cut keeps completion
  pending because QueryCoord rechecks the version contract before fanout/open;
- QueryCoord restart cannot reach Healthy when initial distribution recovery
  fails, so pending ACK recovery cannot release the gate from an uninitialized
  participant snapshot;
- collection deletion is terminal: if the collection no longer exists,
  QueryCoord completes the gate without trying to reinstall query state;
- stale topology RPCs return retriable
  `ErrCollectionSchemaVersionNotReady` and must be re-planned from fresh
  QueryCoord metadata.

No implemented post-cut path writes the old schema back.

### 18.7 Release and drop policy

Removal-only paths are intentionally outside the install admission gate:

- `ReleaseCollection`, full `DropLoadConfig`, QueryNode segment release, and
  channel unsubscribe may continue because they remove serving state and
  cannot install an old schema snapshot;
- a newly submitted release intent is still ordered against schema DDL by the
  same collection broadcaster resource lock;
- an `AlterLoadConfig` ACK, including partial partition release that rewrites
  collection/target metadata, uses the topology lease and waits until gate
  release;
- a delayed full-release ACK may remove a participant while the gate is closed;
  completion recomputes current distribution holders, so removal reduces the
  readiness set instead of introducing stale state.

If a future release path starts publishing schema-dependent state rather than
only removing it, it must become a gated participant.

### 18.8 Verification completed

Focused tests cover:

- minimum-version admission for every registered service role, rejection before
  cut, and completion-time rejection that keeps the gate closed;
- atomic gate close/acquire and lease drain;
- normal-load lock ordering, closed-gate rejection, refresh lease lifetime,
  and delayed load ACK rejection;
- job/task/target-observer participation;
- pre-cut abort and ambiguous post-cut classification;
- broadcaster recovery enumeration;
- RootCoord ACK failure keeping the gate pending;
- QueryCoord participant discovery, including leader-view workers;
- QueryNode coordinator fanout, partial failure, equal retry, and local-worker
  reentrancy;
- stale pre-load/pre-publish rejection, staged-load cleanup, distribution-delta
  suppression, and legacy L0 ownership;
- schema/barrier propagation for all current production request producers;
- stale-schema rejection exclusion from failed-load penalties.

The focused distribution-controller tests also cover propagation of initial
QueryNode distribution recovery failures. Full MixCoord restart fault injection
with a pending schema task is still pending.

Targeted tests pass for QueryCoord, QueryNode, RootCoord, broadcaster,
coordinator wiring, and `internal/schemaevolution`. Full recursive RootCoord and
QueryNode suites still require the repository's etcd-backed test environment;
restart and multi-component fault-injection scenarios from Section 16.2 have
not all been executed.

### 18.9 Known limitations and rollout blockers

1. Phase 0 intentionally supports only the permanent in-process MixCoord
   deployment. A separate RootCoord-to-QueryCoord RPC and independently
   deployed coordinator protocol are non-goals under this contract.
2. There is no explicit topology epoch. The baseline uses
   `(schema.Version, SchemaBarrierTs)` plus the broadcaster resource lock as the
   freshness fence. A later phase still needs an explicit operation/evolution
   identity for richer state transitions and diagnostics.
3. QueryCoord readiness is synchronous fanout to nodes currently found in
   segment/channel distribution and leader views. It does not persist a closed-
   gate baseline or per-replica/per-channel readiness record.
4. DataCoord readiness currently means its collection cache refresh RPC
   succeeded. Broader DataCoord/DataNode readiness is not aggregated.
5. StreamingNode readiness relies on the existing durable WAL apply/ACK and
   replay contract; Phase 0 adds no separate StreamingNode readiness RPC.
6. Remote stale-load cleanup is best effort. Cleanup RPC failure can leave
   unreferenced worker resources until later release/reconciliation or restart,
   although the stale load is not published in leader distribution.
7. Gate metrics, an admin/debug status surface, explicit operation IDs, and a
   complete persisted install record are not implemented.
8. Version admission is implemented as an etcd session snapshot checked before
   cut and before completion. It does not reserve cluster membership across the
   entire operation; a joining old node blocks completion on the second check.
   Operational rollout must prevent old-node admission after the cluster has
   been declared fully upgraded.
9. The complete restart, delayed-RPC, unavailable-participant, partial-
   broadcast, and cleanup-failure fault-injection matrix remains required
   before declaring Phase 0 production-complete.

## 19. Open decisions

1. Whether the minimal install record lives in RootCoord catalog metadata or a
   dedicated schema-install catalog.
2. Whether old tasks are always drained/canceled in the first rollout, or some
   can complete before the cut using bounded leases.
3. The exact topology epoch representation and its relationship to schema
   version and `SchemaBarrierTs`.
4. How QueryCoord safely removes an unreachable participant while the gate is
   closed without reopening general balance.
5. The DDL response shape for a post-cut timeout or pending recovery.
6. Whether external-refresh metadata is required for install readiness for each
   external collection mode.
7. The exact set of `SyncDistribution` actions that carry schema-dependent state
   and therefore require the fence.
