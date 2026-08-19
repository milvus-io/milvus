# WAL Replica Meta Design

> This document defines the metadata model for multiple WAL replicas on one
> PChannel. It focuses on StreamingCoord-owned assignment metadata and the
> recovery key boundary. Runtime replay, QueryView planning, and concrete RPC
> implementation are covered by their own designs.
>
> References:
> [WAL Recovery Architecture](wal-recovery-architecture.md),
> [StreamingNode VChannel WAL Input View](streamingnode_vchannel_wal_view.md),
> [Distributed Query View Design](../qviews/README.md),
> [Query Client Design](../qviews/query/query_client.md).

## 1. Goal

QueryView multi-replica support needs multiple read-serving WAL replicas for the
same PChannel:

- one `AccessModeRW` WAL replica owns writes for the PChannel. This is the
  Primary WAL replica;
- zero or more `AccessModeRO` WAL replicas replay the same WAL and serve
  QueryView resources only. These are Secondary WAL replicas;
- QueryView replicas bind to stable WAL replicas;
- StreamingCoord remains responsible for assigning each WAL replica to a
  StreamingNode.

In existing single-replica code this is described as PChannel-to-StreamingNode
assignment. After this design the StreamingCoord metadata key remains
PChannel-scoped, but the value contains a replica set. Each replica entry has a
stable `walReplicaID` so QueryView bindings and assignment discovery can refer
to one serving replica of the PChannel.

The current `PChannelMeta` model already has the right PChannel-scoped
ownership boundary for write term and assignment publication. Its value is too
narrow because it can describe only one assignment for the PChannel. The target
design is to extend `PChannelMeta` to contain multiple WAL replica assignment
entries without a broad code rename away from PChannel terminology.

## 2. Decision Summary

The metadata split is:

```text
WAL backend log key:            pchannel
authoritative WAL recovery key: pchannel
StreamingCoord meta key:        pchannel
WAL replica identity in meta:   pchannel + walReplicaID
QueryView binding:              queryReplicaID + vchannel -> walReplicaID
```

The backend WAL log is shared by PChannel. The authoritative WAL recovery
projection is also shared by PChannel because VChannel state, Segment state, and
TransformLog state are deterministic projections of that same log. WAL replicas
are assignment and serving identities over this shared WAL state, not
independent copies of the WAL recovery state.

`AccessMode` is not part of the StreamingCoord meta key or the WAL replica
identity. A WAL replica can switch between READWRITE and READONLY without
changing the identity used by QueryView bindings or assignment history.

## 3. Concepts

### 3.1 WALReplicaKey

`WALReplicaKey` is the stable logical identity of one WAL serving replica:

```go
type WALReplicaKey struct {
    PChannel  string
    ReplicaID int64
}
```

`PChannel` names the shared WAL stream. `ReplicaID` distinguishes serving
replicas of that stream for assignment, QueryView binding, access-mode
management, and assignment discovery.

`WALReplicaKey` is not the StreamingCoord persistence key. StreamingCoord
persists one `PChannelMeta` key per PChannel, and `WALReplicaKey` identifies one
replica entry inside that value.

The initial `AccessModeRW` replica can use a reserved stable replica ID, for
example `0`, or a StreamingCoord-allocated stable ID. The exact allocation
policy is an implementation detail, but the ID must be stable across
access-mode changes and StreamingNode reassignment.

### 3.2 AccessMode

`PChannelAccessMode` describes the current authority of the replica and should
be the single source for Primary and Secondary behavior:

```text
AccessModeRW -> writable, Primary WAL replica
AccessModeRO -> read-only, Secondary WAL replica
```

There should not be a separate WAL replica role enum. A separate role would
duplicate `AccessMode` and create invalid state combinations such as
`Primary + READONLY` or `Secondary + READWRITE`.

### 3.3 PChannelMeta With Replica Set

The existing `PChannelMeta` key remains PChannel-scoped. The value is extended
from one assignment to a replica set:

```go
type PChannelMeta struct {
    PChannel string

    // PChannel write-link epoch, not replica-scoped.
    Term int64

    PrimaryReplicaID int64
    NextReplicaID    int64

    Replicas []WALReplicaAssignment
}

type WALReplicaAssignment struct {
    ReplicaID int64

    AccessMode PChannelAccessMode

    // Replica-scoped control-plane generation, not a WAL write term.
    AssignmentEpoch int64

    ResourceGroup string

    ActiveNode StreamingNodeInfo
    TargetNode StreamingNodeInfo
    State WALReplicaState

    Histories []PChannelAssignmentLog
}
```

Each `WALReplicaAssignment` entry keeps a replica-scoped assignment state. It
can reuse the existing `PChannelMetaState` wire enum shape or introduce an
equivalent per-replica enum, but the persisted states should stay limited to
stable control-plane states:

```text
UNINITIALIZED -> ASSIGNING -> ASSIGNED -> UNAVAILABLE -> ASSIGNING
ASSIGNED -> DROPPING
UNAVAILABLE -> DROPPING
```

The state meanings are:

```text
UNINITIALIZED:
  replica entry exists, but no owner has been selected yet.

ASSIGNING:
  a node assignment or access-mode transition is in progress. TargetNode is the
  pending owner when a different StreamingNode is being prepared. ActiveNode may
  still be serviceable for healthy AccessModeRO make-before-break migration.

ASSIGNED:
  ActiveNode has confirmed the replica runtime for the current AccessMode.
  TargetNode must be empty.

UNAVAILABLE:
  there is no serviceable owner for this replica. ActiveNode may keep the last
  failed owner for cleanup and diagnostics, but routing must not use it.

DROPPING:
  the replica is being removed. ActiveNode and TargetNode, if present, are
  cleanup targets. New query bindings and routing must not use the replica.
```

`ResourceGroup` is the hard placement affinity inherited from the QueryView
replica or from a WAL replica policy. StreamingCoord must assign the WAL
replica only to a StreamingNode in that resource group when the field is set.

`PrimaryReplicaID` must point to the single `AccessModeRW` replica. All other
assigned serviceable replicas for the same PChannel must be `AccessModeRO`.

`ActiveNode` is the last confirmed owner. It is serviceable in `ASSIGNED`. It
can also remain serviceable during `ASSIGNING` only for healthy
`AccessModeRO` make-before-break migration, where the old readable runtime
continues serving while the new target warms up.

`TargetNode` is persisted whenever StreamingCoord has asked another
StreamingNode to prepare this replica. It is not an in-memory hint. Persisting
it lets a recovered StreamingCoord find and release a half-prepared runtime
instead of leaking resources. Query and write routing must never use
`TargetNode`; it becomes serviceable only after a metadata CAS moves it to
`ActiveNode` and clears `TargetNode`.

`AssignmentEpoch` filters stale control-plane events for one WAL replica, such
as delayed `AssignDone`, old StreamingNode error reports, or recovery results
from a previous assignment. It is not written into WAL messages and is not used
for WAL backend writer fencing.

### 3.4 PChannel Write Term

`Term` remains a PChannel write-link epoch. It is not a WAL replica attribute.

The term is advanced only when the PChannel write chain is opened as
`AccessModeRW` under a new writer:

```text
AccessModeRW WAL open
AccessModeRW failover / promotion
AccessModeRW write-chain reopen after fencing
```

The term is not advanced by:

```text
AccessModeRO WAL replica creation
AccessModeRO replay restart
AccessModeRO assignment to another StreamingNode
QueryView binding to a WAL replica
```

Only the current `AccessModeRW` replica writes with the current PChannel term.
`AccessModeRO` replicas observe the term carried by WAL messages or assignment
discovery, but they do not own or advance it.

Read-only replicas do not need a WAL term for correctness. Their freshness and
ownership checks use the replica-scoped `AssignmentEpoch`, while `Term` remains
reserved for the PChannel write chain.

### 3.5 Naming And Implementation Scope

This design introduces WAL replica as a concept, but implementation should not
rename existing PChannel-oriented code just for terminology alignment.

Keep names such as `PChannelMeta`, `PChannelInfo`, `PChannelInfoAssigned`,
`ChannelManager`, and existing PChannel catalog helpers when they still describe
the physical WAL channel or the existing assignment subsystem. Extend their
payload, in-memory selectors, and discovery projections with replica identity
where needed; do not change the StreamingCoord catalog key away from PChannel
scope.

Use new WAL-replica names only for new concepts that cannot be expressed by the
old PChannel-only identity, for example a query-facing binding field or a helper
that explicitly selects among multiple replicas of the same PChannel.

## 4. Ownership Boundaries

There are three independent decisions:

```text
QV balancer:
  QueryReplicaID + VChannel -> WALReplicaKey
  QueryReplicaID + VChannel -> QueryNode segment placement

StreamingCoord channel balancer:
  WALReplicaKey -> StreamingNode

WAL runtime:
  pchannel -> shared authoritative WAL recovery projection
  WALReplicaKey -> local serving runtime and query resources
```

QV balancer does not directly choose StreamingNode. It binds a QueryView replica
to a stable WAL replica. StreamingCoord then decides which StreamingNode owns
that WAL replica in the current assignment snapshot.

StreamingCoord does not decide `QueryReplicaID -> WALReplicaKey`. It only
publishes enough assignment information for clients and work nodes to resolve
that binding consistently.

## 5. Persistence Model

### 5.1 StreamingCoord Meta

StreamingCoord persists one metadata record per PChannel:

```text
streamingcoord/pchannel/{pchannel}
```

The value is `PChannelMeta` extended with a WAL replica set, or a compatible
protobuf representation. All WAL replica assignment entries for the PChannel
are updated under this one key.

The old single-replica value under the same key:

```text
streamingcoord/pchannel/{pchannel}
```

maps to a replica set with one initial entry, usually `walReplicaID = 0`. That
entry becomes the initial `AccessModeRW` replica unless the cluster is in a
special read-only replication mode.

Keeping one StreamingCoord key per PChannel makes cross-replica invariants
single-record invariants: `Term`, `PrimaryReplicaID`, `NextReplicaID`, and the
RW/RO access-mode switch can be persisted atomically with the affected replica
assignments.

### 5.2 WAL Recovery Projection

The authoritative WAL recovery projection remains PChannel-scoped:

```text
streamingnode/recovery/{pchannel}/...
```

All WAL-recovered module metadata stays below the PChannel recovery root:

```text
streamingnode/recovery/{pchannel}/checkpoint
streamingnode/recovery/{pchannel}/vchannel/{vchannel}
streamingnode/recovery/{pchannel}/segment/{...}
streamingnode/recovery/{pchannel}/transformlog/{vchannel}
```

The exact child key shape follows the existing module catalog layout. It must
not be multiplied by `walReplicaID`.

Only the `AccessModeRW` replica advances authoritative WAL recovery checkpoints
and durable module projections. `AccessModeRO` replicas rebuild local serving
runtimes from the shared recovery projection plus WAL replay. A read-only
replica may keep non-authoritative local runtime cursors or caches for faster
restart, but those must not replace or regress the PChannel-level recovery
checkpoint.

### 5.3 QueryView Recovery Info

StreamingNode QueryView recovery information is not WAL module recovery state.
It is scoped by QueryView identity and must record the WAL replica binding:

```text
streamingnode/queryview/{pchannel}/{walReplicaID}/{queryReplicaID}/{vchannel}
```

This lets the StreamingNode that currently hosts a WAL replica recover the
QueryViews bound to that replica. It does not imply that VChannel, Segment, or
TransformLog recovery metadata is per WAL replica.

## 6. Recovery Semantics

The PChannel WAL data and authoritative recovered projection are shared:

```text
pchannel log
  -> shared WAL recovery projection
       - checkpoint
       - VChannel metadata and schema history
       - Segment assignment and persisted data state
       - TransformLog checkpoint, chunks, materialization, truncation state
```

Each WAL replica has its own assignment state and local serving runtime, but the
runtime is recovered from the shared projection. QueryView Up recovery info is
per QueryView binding because QueryViews are bound to WAL replicas.

### 6.1 READWRITE Recovery

`AccessModeRW` recovery keeps the current write-owner semantics:

```text
load PChannel recovery checkpoint
open READWRITE WAL
append RecoveryBarrier
replay checkpoint -> RecoveryBarrier
establish query-resource baseline
serve writes and bound QueryViews
```

The successful `RecoveryBarrier` append proves the replica owns the writable WAL
for the current PChannel write term. On backends with writer fencing, it also
fences stale writers.

### 6.2 READONLY Recovery

`AccessModeRO` recovery is read-only:

```text
load PChannel recovery checkpoint
open READONLY WAL
choose readable WAL frontier
replay checkpoint -> readable frontier
establish read-only query-resource baseline
serve bound QueryViews only
```

An `AccessModeRO` replica must not append `RecoveryBarrier` while it remains
read-only. It does not prove write ownership. It only rebuilds a local serving
runtime up to a readable frontier of the shared WAL.

The concrete read-only baseline marker is a runtime design detail. It must not
advance the authoritative PChannel recovery checkpoint unless the replica has
been promoted to `AccessModeRW`.

### 6.3 READONLY Migration

Healthy `AccessModeRO` reassignment should be make-before-break. A read-only
replica has no WAL writer fencing requirement, so StreamingCoord can keep the
old active owner serviceable while a new target owner warms up:

```text
ASSIGNED:
  ActiveNode = SN-B
  TargetNode = nil

ASSIGNING:
  ActiveNode = SN-B      // still serviceable
  TargetNode = SN-C      // persisted pending owner, warming up
  AssignmentEpoch++

SWITCH:
  ActiveNode = SN-C
  TargetNode = nil
  State = ASSIGNED
  Histories keep SN-B until cleanup succeeds

CLEANUP:
  stop/release old runtime on SN-B
  clear Histories only after the release succeeds
```

Only `ActiveNode` is published as the serviceable owner for
`WALReplicaKey{pchannel, walReplicaID}`. During this healthy `ASSIGNING`
period, the old `ActiveNode` remains the published read owner, so QueryView
state does not need to become Down merely because a new StreamingNode is being
prepared. `TargetNode` receives an internal prepare assignment and builds local
runtime, but it must not serve QueryView traffic until StreamingCoord
atomically switches it to `ActiveNode`.

`TargetNode` must be persisted before the prepare request is sent. If
StreamingCoord crashes during warmup, recovery can observe the pending target
and either continue waiting for the matching `AssignmentEpoch` or ask that node
to release the half-prepared runtime.

If the old `ActiveNode` is already unavailable, the same read-only replica uses
normal failover reassignment instead. In that path there is no serviceable
owner while the target is preparing:

```text
UNAVAILABLE:
  ActiveNode = SN-B      // last failed owner, not serviceable
  TargetNode = nil

ASSIGNING:
  ActiveNode = SN-B      // retained only for cleanup/history
  TargetNode = SN-C      // persisted pending owner
  AssignmentEpoch++

ASSIGNED:
  ActiveNode = SN-C
  TargetNode = nil
  Histories keep SN-B until cleanup succeeds
```

In both healthy migration and failover, the PChannel write term is unchanged.
The old owner cleanup target must remain persisted after the new owner becomes
`ASSIGNED`; otherwise a StreamingCoord restart or failed cleanup RPC can lose
the only recoverable reference to the old runtime.

## 7. AccessMode Switch

Access-mode switch is controlled by StreamingCoord by changing which WAL replica
is `AccessModeRW`. Only opening the new `AccessModeRW` write chain advances the
PChannel write term.

The normal write-owner movement is a PChannel-level switchover, not an isolated
demotion followed later by an isolated promotion. StreamingCoord should persist
the `PrimaryReplicaID`, `Term`, and affected replica access-mode changes in one
`PChannelMeta` CAS.

### 7.1 Planned Switchover

Planned switchover moves write ownership from the current `AccessModeRW`
replica to a serviceable `AccessModeRO` replica:

```text
before:
  Term = 10
  PrimaryReplicaID = 0
  replica 0: READWRITE, ActiveNode = SN-A
  replica 1: READONLY,  ActiveNode = SN-B

after metadata switch:
  Term = 11
  PrimaryReplicaID = 1
  replica 0: READONLY,  State = ASSIGNED,  ActiveNode = SN-A
  replica 1: READWRITE, State = ASSIGNING, ActiveNode = SN-B

after new writer recovery done:
  Term = 11
  PrimaryReplicaID = 1
  replica 0: READONLY,  State = ASSIGNED, ActiveNode = SN-A
  replica 1: READWRITE, State = ASSIGNED, ActiveNode = SN-B
```

Preconditions:

```text
target replica is AccessModeRO and serviceable readable
target ActiveNode is healthy
target has settings-aligned Up QueryViews for shards that need primary serving
```

If the target is missing a required Up QueryView, StreamingCoord asks QV
balancer to create or advance the target replica's QueryView first. The WAL
state machine does not mutate QueryView bindings itself.

After the single-key metadata switch:

```text
old ActiveNode:
  stop accepting new writes for old term
  drain or fail in-flight writes
  close RW writer
  continue or reopen as READONLY if still serving readable QueryViews

new ActiveNode:
  open READWRITE WAL with the new term
  append RecoveryBarrier
  replay checkpoint -> RecoveryBarrier
  become serviceable READWRITE
```

The target replica can be stored as `ASSIGNING` after the metadata switch until
its StreamingNode confirms the READWRITE open and recovery barrier. During that
window `PrimaryReplicaID` already points to the target, but primary serving is
not considered available yet. Query routing derives primary QueryView status
from the serviceable WAL runtime, so an Up QueryView bound to the target becomes
primary only after the target WAL replica is serviceable READWRITE.

If the new writer fails to open, append `RecoveryBarrier`, or replay through the
barrier, the PChannel should not roll back to the previous term. The failed
attempt remains a fenced write epoch, and StreamingCoord should retry promotion
with a newer term on the same or another target replica.

### 7.2 READWRITE To READONLY

Demotion changes authority but keeps identity:

```text
WALReplicaKey{pchannel, replicaID}
  AccessMode: READWRITE -> READONLY
```

Flow:

1. StreamingCoord publishes the replica with READONLY access mode.
2. The old `AccessModeRW` owner stops accepting writes for the previous
   PChannel write term.
3. In-flight appends drain or fail.
4. The local RW writer is closed.
5. The same WAL replica continues or reopens as READONLY.
6. QueryView resources bound to the replica can continue after local recovery
   catches up.

Standalone demotion is only valid for special drain/read-only modes where the
PChannel is allowed to have no serviceable writer. In the normal serving path,
READWRITE to READONLY should be part of planned switchover.

### 7.3 READONLY To READWRITE

Promotion also keeps identity:

```text
WALReplicaKey{pchannel, replicaID}
  AccessMode: READONLY -> READWRITE
```

Flow:

1. Before promotion, the coordinator checks the target WAL replica's
   primary-serving readiness for the loaded shards on this PChannel.
2. For each shard that needs primary serving, the target WAL replica must
   already have an Up QueryView whose settings match the current load settings.
3. If such an Up QueryView is missing, QV balancer creates or advances the
   target replica's QueryView normally and waits until it becomes Up while the
   WAL replica is still READONLY.
4. After the target WAL replica has the required Up QueryViews, StreamingCoord
   publishes the selected replica with READWRITE access mode.
5. The selected replica opens the WAL as READWRITE.
6. Opening the `AccessModeRW` write chain advances the PChannel write term.
7. It appends `RecoveryBarrier`.
8. If the append succeeds, it replays through the barrier and becomes the
   active writer.
9. If the append fails because it is fenced or not writable, promotion fails and
   the replica must not serve writes.

The readiness check is intentionally narrow. It checks that primary serving will
exist on the target WAL replica after promotion; it does not require the target
QueryViews to have the same DataVersion, QueryVersion, segment placement, or
TransformLog start point as the old primary. Those are QueryView-layer
correctness properties already covered by the QueryView Up transition.

`RecoveryBarrier` is therefore an `AccessModeRW` recovery and promotion
primitive, not an `AccessModeRO` recovery primitive.

## 8. QueryView Binding

Every QueryView has a stable binding to one WAL replica:

```text
QueryViewOfShard.Meta:
  replica_id
  vchannel

QueryViewOfStreamingNode:
  wal_replica_id
```

The binding means:

- Phase 1 query planning is served by the StreamingNode currently hosting that
  WAL replica.
- StreamingNode growing-side Phase 2 execution uses that WAL replica local
  projection.
- TransformLog production for QueryNodes in this QueryView uses that WAL
  replica local TransformLog stream.
- If the WAL replica moves to another StreamingNode, the QueryView identity does
  not change.

The QueryView replica ID and WAL replica ID are different concepts. They may be
one-to-one in the first implementation, but the metadata should store the
binding explicitly instead of assuming equality.

QueryView metadata does not declare whether a QueryReplica is primary. Primary
status is derived at serving time from the WAL binding: a QueryView is a primary
QueryView only when its `wal_replica_id` equals the current
`PChannelMeta.PrimaryReplicaID` for the vchannel's PChannel and that WAL replica
is serviceable READWRITE. If the same Up QueryView remains bound to a replica
that is later demoted to READONLY, it is still an Up readable QueryView, but it
is no longer a primary QueryView.

## 9. Replica Removal

Only non-primary `AccessModeRO` WAL replicas can be removed directly. The
current `PrimaryReplicaID` cannot be deleted; it must first be switched over to
another WAL replica.

Removal preconditions:

```text
replicaID != PrimaryReplicaID
replica.AccessMode == READONLY
replica.State is ASSIGNED or UNAVAILABLE
replica.TargetNode is empty
QV balancer reports no active non-Dropped QueryView depends on replicaID
no pending QueryView sync or teardown depends on replicaID
```

The removal flow is:

```text
ASSIGNED or UNAVAILABLE:
  mark replica DROPPING
  AssignmentEpoch++

cleanup targets:
  ActiveNode if present
  TargetNode if present
  Histories entries if present

healthy cleanup target:
  send stop/release local RO runtime
  wait DropDone(replicaID, AssignmentEpoch)
  remove replica entry from PChannelMeta

missing or unavailable cleanup target:
  remove replica entry directly after dependency checks
```

Removing a read-only WAL replica does not change `Term`, `PrimaryReplicaID`, the
backend WAL log, or the PChannel-scoped authoritative recovery projection.
StreamingNode QueryView recovery keys remain owned by the QueryView state
machine; WAL replica removal should not delete QueryView recovery metadata that
may still be controlled by QueryView Dropping/Dropped transitions.

## 10. Assignment Discovery

Assignment discovery must publish WAL replica identity, not only PChannel name
and access mode.

Conceptually:

```proto
message WALReplicaInfo {
    string pchannel = 1;
    int64 wal_replica_id = 2;
    PChannelAccessMode access_mode = 3;
    string resource_group = 4;
    int64 pchannel_write_term = 5;
    WALReplicaState state = 6;
}

message StreamingNodeAssignment {
    StreamingNodeInfo node = 1;
    repeated WALReplicaInfo wal_replicas = 2;
    ShardAssignmentInfo shard_assignment = 3;
}

message ShardAssignmentEntry {
    int64 collection_id = 1;
    int32 shard_index = 2;
    int64 query_replica_id = 3;
    int64 wal_replica_id = 4;
}
```

Existing `channels` and `secondary_channels` can be retained for compatibility:

- `channels` contains WAL replicas whose access mode is READWRITE, projected as
  `PChannelInfo` with READWRITE access mode.
- `secondary_channels` contains WAL replicas whose access mode is READONLY,
  projected as `PChannelInfo` with READONLY access mode.

New clients must consume the explicit WAL replica identity because multiple
secondary replicas can share the same PChannel and READONLY access mode.

Assignment discovery publishes serviceable `ActiveNode` ownership only. It must
not expose `TargetNode` as a routable owner. For healthy `AccessModeRO`
make-before-break migration, the old `ActiveNode` can remain visible while the
replica entry is `ASSIGNING`; for `AccessModeRW` promotion, the target should
not be considered serviceable READWRITE until the WAL open and
`RecoveryBarrier` recovery complete.

## 11. Resource Group And Balance

Resource group placement is split across QueryView planning and WAL assignment:

```text
QV balancer:
  choose QueryNodes inside the replica resource group
  bind QueryReplica + VChannel to a compatible WALReplicaKey
  request/create/release AccessModeRO WAL replicas when QueryView demand changes

StreamingCoord:
  assign WALReplicaKey to a StreamingNode inside the WAL replica resource group
  move WAL replicas across StreamingNodes without changing QueryView bindings
```

`QueryReplica.ResourceGroup` and `WALReplicaAssignment.ResourceGroup` are
related but not the same field:

- `QueryReplica.ResourceGroup` constrains QueryNode sealed segment placement.
- `WALReplicaAssignment.ResourceGroup` constrains StreamingNode WAL runtime
  placement.

For `AccessModeRO` replicas, QV balancer should normally bind a QueryReplica to
a WAL replica in the same resource group. If no such WAL replica exists but the
resource group has eligible StreamingNodes, QV balancer can ask StreamingCoord
to create or keep an `AccessModeRO` WAL replica for that group. If no compatible
StreamingNode exists for the required WAL replica, dependent QueryViews remain
reconcilable but not serviceable; the system must not silently place the WAL
replica outside its resource group.

`AccessModeRW` is different because it is unique at PChannel scope. A PChannel
can host vchannels from multiple collections, and those collections may have
QueryReplicas in different resource groups. Therefore a per-QueryReplica
resource-group request cannot independently choose the PChannel Primary WAL
placement. The Primary WAL resource group is selected by WAL policy, such as
the existing global `streaming.primaryResourceGroup` hint or a future
per-PChannel policy.

Primary QueryView status is derived from the selected WAL Primary:

```text
Up QueryView is primary-serving
  iff its wal_replica_id == PChannelMeta.PrimaryReplicaID
  and that WAL replica is serviceable AccessModeRW
```

If a QueryReplica's resource group is not compatible with the current
`AccessModeRW` WAL replica, that QueryReplica can still bind to an
`AccessModeRO` WAL replica in its own resource group and serve as a secondary
readable replica, but it cannot be primary-serving. If a collection requires a
primary-serving QueryView in a particular resource group, load-config admission
or QV balancer reconciliation must ensure that the PChannel Primary WAL is
placed in a compatible resource group. Otherwise the collection should remain in
an explicit not-ready state instead of falling back across resource groups.

The two balancers must not overwrite each other's decisions:

```text
QV balancer owns:
  QueryReplica + VChannel -> WALReplicaKey
  QueryNode segment placement
  demand for AccessModeRO WAL replicas
  primary-serving readiness before WAL switchover

StreamingCoord owns:
  WALReplicaKey -> ActiveNode / TargetNode
  AccessModeRW / AccessModeRO switch
  Term and PrimaryReplicaID
  StreamingNode balance inside each WAL replica resource group
```

StreamingCoord balance should treat resource group as a hard bucket. It may
rebalance `AccessModeRO` replicas inside their bucket using make-before-break.
It must not move an `AccessModeRW` replica with ordinary RO-style rebalance; RW
movement is a planned switchover and must satisfy the primary-serving
preconditions described above.

## 12. Invariants

1. StreamingCoord persists one `PChannelMeta` key per PChannel.
2. `WALReplicaKey` is `(pchannel, walReplicaID)` and identifies one replica
   entry inside `PChannelMeta`.
3. The backend WAL log remains keyed only by `pchannel`.
4. Authoritative WAL recovery and module projection state remains keyed by
   `pchannel`, not by `WALReplicaKey`.
5. Optional per-replica runtime cursors or caches are non-authoritative and must
   not regress the PChannel recovery checkpoint.
6. StreamingNode QueryView recovery info is keyed by QueryView identity and WAL
   replica binding.
7. `AccessMode` is not part of any recovery key.
8. For each PChannel, exactly one WAL replica may have `AccessModeRW`, and
   `PrimaryReplicaID` must point to that replica. It may be `ASSIGNING` during
   a switchover and is not serviceable READWRITE until recovery confirms it.
9. For each PChannel, zero or more WAL replicas may be `AccessModeRO`.
10. Primary and Secondary are explanatory names derived from `AccessMode`, not
   persisted as a separate role.
11. `Term` is the PChannel write term, not a WAL replica term.
12. The PChannel write term advances only when the `AccessModeRW` write chain is
    opened or reopened.
13. `AccessModeRO` WAL replica creation, replay restart, and StreamingNode
    reassignment do not advance the PChannel write term.
14. `AssignmentEpoch` is replica-scoped control-plane state. It is not a WAL
    term and must not be used for writer fencing.
15. `ActiveNode` is the only field that can be published for serving. It is
    serviceable in `ASSIGNED`, and may remain serviceable during `ASSIGNING`
    only for healthy `AccessModeRO` make-before-break migration.
16. `TargetNode` is persisted whenever a pending owner is being prepared, and
    is a recovery and cleanup target after StreamingCoord restart.
17. `TargetNode` must not be used by query or write routing until it becomes
    `ActiveNode`.
18. A QueryView binds to one WAL replica and keeps that binding across
    StreamingNode reassignment.
19. QueryView primary status is derived from WAL runtime state, not from a
    QueryView or QueryReplica primary flag.
20. An Up QueryView is a primary QueryView only when its bound WAL replica is
    the current serviceable `AccessModeRW` replica for the PChannel.
21. Before promoting an `AccessModeRO` replica to `AccessModeRW`, the target WAL
    replica must already have settings-aligned Up QueryViews for shards that
    need primary serving on the PChannel.
22. Planned switchover updates `Term`, `PrimaryReplicaID`, old replica
    READWRITE-to-READONLY, and target replica READONLY-to-READWRITE in one
    `PChannelMeta` CAS.
23. A failed promotion attempt must not roll back the PChannel write term.
24. The current `PrimaryReplicaID` cannot be removed directly.
25. A non-primary `AccessModeRO` replica can be removed only after QV balancer
    reports no QueryView dependency on that replica.
26. Removing an `AccessModeRO` replica does not delete PChannel-scoped WAL
    recovery projection state.
27. QV balancer owns `QueryReplica -> WALReplica` binding.
28. StreamingCoord owns `WALReplica -> StreamingNode` assignment.
29. `AccessModeRO` WAL replicas do not append WAL messages while they remain
    read-only.
30. Promotion to `AccessModeRW` is valid only after opening READWRITE WAL and
    successfully appending a `RecoveryBarrier`.
31. `QueryReplica.ResourceGroup` constrains QueryNode placement.
    `WALReplicaAssignment.ResourceGroup` constrains StreamingNode placement.
32. Resource group fallback is not allowed for WAL replica placement. If no
    compatible StreamingNode exists, the WAL replica remains not serviceable.
33. `AccessModeRW` placement is PChannel-scoped and unique. Multiple
    QueryReplicas in different resource groups cannot independently require
    different Primary WAL placements for the same PChannel.
34. StreamingCoord may rebalance `AccessModeRO` replicas inside their resource
    group bucket, but `AccessModeRW` movement is a planned switchover, not an
    ordinary balance move.

## 13. Out Of Scope

This document does not define:

- the exact protobuf migration plan;
- WAL backend support for multiple read-only scanners;
- the read-only recovery baseline implementation;
- query-client replica selection policy;
- exact QV balancer scoring for choosing WAL replicas;
- exact StreamingCoord scoring for balancing WAL replicas inside a resource
  group;
- TransformLog stream multiplexing internals;
- operational metrics and alert names.

Those should be specified after the meta identity and key boundaries are
accepted.
