# MEP: In-Tree Extension Mechanism for Distributions

- **Created:** 2026-08-31
- **Author(s):** @xiaocai2333
- **Status:** In progress
- **Component:** pkg/extension | cmd/milvus | hookutil | Proxy | Coordinator
- **Related Issues:** #52979
- **Implemented by:** #52981

## Summary

A distribution that compiles its own behavior into the milvus binary needs two
things from milvus: a way to install a request hook without a `.so`, and a
callback that starts its control-plane engine when the coordinator becomes
active and stops it on shutdown. `pkg/extension` is those two setters, plus
the one context mark the proxy reads. Everything else such a distribution
does is either the hook's own reach, a coordinator RPC, or a configuration
item. A stock binary installs nothing and behaves exactly as before.

## Motivation

The managed-cloud form of milvus had been a fork carrying a handful of
behavior changes in the proxy and the coordinators, re-applied by hand on
every rebase. The first attempt to replace the fork was a table of eight
typed capabilities consulted from eighteen places in the tree. Reviewing it
against the fork's actual needs showed that most entries were not
capabilities: four were constants per deployment (configuration), two
duplicated what the request hook already does on every proxy RPC, one
duplicated coordinator RPCs, and one was an answer with no RPC that belonged
on the wire. What remains is small enough to state on one page.

## Public interfaces

```go
package extension

func SetHook(h hook.Hook)                     // hookutil prefers it over proxy.soPath
func InstalledHook() hook.Hook
func FormInstalled() bool                     // InstalledHook() != nil; read by the coordinators too

type Coordinator interface {                  // the coordinator as its own clients see it
	rootcoordpb.RootCoordClient
	querypb.QueryCoordClient
	datapb.DataCoordClient
}
type CoordinatorEngine interface {
	Start(ctx context.Context, coord Coordinator) error
	Stop() error
}
func SetCoordinatorEngine(e CoordinatorEngine)
func InstalledCoordinatorEngine() CoordinatorEngine

func WithQueryResourceGroup(ctx, rg) context.Context   // set by a hook's Before
func QueryResourceGroupFromContext(ctx) string
```

A distribution calls the two setters, then `cmd/milvus.Main(os.Args)`.

### The request hook

`hook.Hook` (milvus-proto) is consulted by the proxy's unary interceptor for
every RPC on both the gRPC and the REST surface, and by `CreateReplicateStream`,
which is a stream and so consults it by hand - the same way, `Mock`, `Before`
and `After` in order, with an empty `ReplicateRequest` as the request and the
stream run under the context `Before` returned. A compiled-in hook that also
implements `hook.Extension` is stored as the extension, so `Report` and
`ReportAction` reach it as they reach a plug-in's `MilvusExtension`.
`Mock` answers without forwarding, `Before` may rewrite the request in place,
block, and return the context the handler runs under, `After` sees the result.
`VerifyAPIKey` answers the API key. Whether the external listener also accepts
a username and password is a policy the hook itself enforces from `Before`,
which sees the same metadata the authentication interceptor does; a refusal
from `Before` always reaches the client as the interceptor's own
`InvalidArgument`, which is in pymilvus's non-retried set.

One addition to hookutil: a compiled-in hook (`SetHook`) is used in preference
to `proxy.soPath`, and a deployment that configures both is refused at
start-up. A compiled-in hook is otherwise treated exactly as a plug-in is: it
gets the same `Init` call with the `hook.*` configuration before it is
installed, a failure to initialize keeps the proxy from starting, and it is
registered with the same watcher, so editing a `hook.*` key re-initializes it
with the new configuration without a restart.

### The coordinator engine

`mixcoord` starts the installed engine once the replica is ACTIVE (a standby
never starts it), on the coordinator client it uses itself, and stops it on
shutdown. A start failure on activation is fatal: a coordinator serving without
its engine would accept work nothing accounts for. Start is called at most
once and Stop at most once, only after Start - a standby that shuts down never
has its engine stopped - and a shutdown does not wait for a slow Start: Stop
may overlap it, and must make it return. The engine reaches the
coordinator through nothing but `Coordinator`, so what it can do is exactly
what a proxy can do - including seeding its own accounts through
`CreateCredential` / `OperateUserRole`, and reading per-resource-group load
progress through `ShowLoadCollections` with `resource_group` set.

### Context marks

`WithQueryResourceGroup` pins a query to one resource group. The shard client
routes it to the leaders whose replica lives in that group
(`ShardLeadersList.resource_groups`) and the proxy attributes its latency to
that group. Nothing in a stock binary sets it.

### Hook-gated behaviors

The hook is also the mark of an installed form: `extension.FormInstalled()`
answers true once `SetHook` has been called, and three behaviors in the
coordinators are switched on by that answer alone. They exist for the
deployment shape a distribution runs - a resource group whose only compute is
a streaming node, one collection loaded into several resource groups
independently, every role rolled from one image - and a stock binary, which
answers false, keeps master's behavior exactly. Because the query coordinator
and the data coordinator read the mark too, a distribution must install its
hook in every role it runs, not only in the proxy.

- **Streaming-node admission and placement** (`utils.AssignReplica`, the
  segment checker). With a form installed, a resource group's streaming query
  nodes count as replica capacity, and a replica with no regular query node
  has its sealed segments placed on the streaming node's embedded query node.
  Stock: a load into a resource group with no regular node is refused with
  `ErrResourceGroupNodeNotEnough`, and sealed segments never land on a
  streaming node - the balancers only walk regular nodes, so a segment placed
  there would stay for good.

- **Resource-group-scoped load placement** (`completePlacementForOutOfScopeResourceGroups`).
  With a form installed, a load request naming resource groups only ever
  changes the placement in those groups: the groups it does not name keep the
  replicas they hold; the querycoord load job recognizes a request that only
  adds groups to a Loaded collection as a pure expansion (the fast path in
  `job_load.go`) and keeps the collection serving instead of resetting it to
  Loading; and a scoped `LoadPartitions` on a Loaded collection is judged on
  the groups it names - a named group that holds no replica yet is an
  addition, not a replica-number change, while changing the count of a named
  group that already holds replicas is refused as before. Stock, all three
  are master's: every request states the whole placement, so a second
  `LoadCollection(resource_groups=[rg_1])` on a collection loaded in `rg_0`
  moves the replica to `rg_1` and the request's `replica_number` is the
  total; every load of an already-loaded collection - a two-group
  `LoadCollection` included - writes it back to Loading/0 and the
  collection-wide observer walks it up again; and `LoadPartitions` refuses any
  change of the total replica count.

  The scope comes from the request as the caller wrote it. A request naming
  no group at all states the whole placement even for a form, which is what a
  plain `load_collection` or `load_partitions` has always meant, so nothing is
  carried over for it; a request that names `__default_resource_group`
  explicitly is a scoped request like any other. A cluster-level load
  override states the whole placement too, whatever the request named.

  The expansion path is replay-safe: the spawn is persisted before the rest
  of it runs, and a message replayed after a failure finds the same replica
  set already stored. An identical replica set on a Loaded collection is
  therefore read as a replayed expansion (a request that changes nothing is
  never broadcast), the added group's observer task is registered before the
  two writes that can fail, and every step is idempotent.

  A resource group added this way is watched by its own load task, which
  judges the group on its own load percentage rather than on the collection's:

  - The task's clock restarts whenever that percentage *changes*, in either
    direction. A load can go backwards - a delegator restarts, or a freshly
    flushed segment enters the next target - and only a percentage that does
    not move at all for the load timeout counts as stalled and has its
    replicas released. The consequence is deliberate: a load that keeps
    moving is never declared stalled, so a percentage oscillating below 100
    (50, 49, 50) refreshes forever. Keeping a loaded-count watermark of our
    own would catch that, but it would also bring back the false regression
    this rule removes, on a figure that legitimately moves down while the
    collection is ingesting; a group that is still moving is left alone.
  - A percentage is only acted on when it is *evidence*. A serving group reads
    0 in several ordinary situations, so all of these count as "unknown"
    rather than as a number: the read failed; the collection's target is not
    known (the current target is persisted only on a graceful stop and the
    next one has to be pulled from datacoord, so an ungraceful restart has no
    target for a while); or some replica of the group owns no node that has
    reported a channel of the collection (the group's figure is a minimum
    across its replicas, so one pod still pending drags a loaded group to 0).
    Unknown *pauses* the clock: the last known figure is kept and the timeout
    is only ever measured over ticks that learned something. A task whose
    group never becomes readable simply stays paused until the collection is
    released or dropped, which removes it.
  - Before the timeout tears anything down, the group is asked the question
    the proxy asks of it: can its shard leaders serve every shard of the
    collection right now (`utils.ShardLeaderReadinessByResourceGroup`,
    measured against the *current* target). A Ready group is never released,
    however long its percentage has sat still: the percentage is measured
    against the *next* target and integer-truncated, so a large collection
    under continuous flush legitimately sits at 99 for as long as the ingest
    lasts while the group serves every query. The task is kept and asked
    again a load timeout later; it finishes only on 100.
  - The teardown itself has a hard limit: it may shrink an expansion that
    never came up, and it may abandon a load that never completed, but it may
    never take the last replicas of a *Loaded* collection or delete its load
    meta. If that is what releasing the group would do, the task is dropped
    and the collection keeps serving. The collection-wide path has always had
    this property, because its timeout only runs while a collection is
    Loading; a scoped task sits on a Loaded collection, so the rule is
    written out. Between the three - only evidence starts the clock, a ready
    group is never released, and the teardown can never unload a serving
    collection - a reading that is wrong in the pessimistic direction costs
    nothing.
  - The task finishes only once the group carries every target *and* the
    collection's current target has been promoted, because until then the
    group cannot serve: shard leader readiness is measured against the current
    target.
  - The tasks live only in memory, so a querycoord restart rebuilds them: for
    every loaded collection, every resource group holding a replica gets its
    task back. Nothing extra is persisted for this, and no attempt is made to
    guess which groups need one: the constructor runs before any QueryNode has
    reported, so a group that has been serving for weeks and a group that
    never came up look identical there. The task is what tells them apart
    afterwards, on the first tick that carries evidence - a group that turns
    out to be loaded finishes then and there.

- **Index engine version with no QueryNode session** (datacoord's version
  manager). With a form installed, datacoord answers its own compiled-in
  index engine version (knowhere's for vectors, the constant for scalars)
  rather than zero, which knowhere would otherwise read as "only DISKANN
  loads off disk" and misroute other disk indexes onto the in-memory path.
  This assumes a QueryNode started later runs the same image as the
  coordinator - true for a distribution that rolls every role from one image
  - and the same assumption sets the upper bound with no session: an
  operator's `dataCoord.targetVecIndexVersion` (or the scalar one) above what
  this image can load is clamped down to it rather than written into index
  builds unchecked, with a rate-limited warning. A QueryNode that does
  register replaces both figures with the cluster-wide ones. Stock: 0 and no
  upper bound, as on master - the current version is the minimum over every
  QueryNode's, a datacoord that comes up first in a rolling upgrade must not
  build indexes an older QueryNode cannot load, and an override is written
  through.

## Configuration this mechanism relies on

Both items are code defaults, not `milvus.yaml` entries: a distribution sets
them through `user.yaml` or the environment.

| Key | Default | What a distribution sets it for |
|---|---|---|
| `dataCoord.externalCollection.refreshWaitForIndex` | false | hold a refresh until its segments are indexed |
| `builtinRoles.*` | - | the roles the distribution's accounts bind to |

## Compatibility

- `ShowCollectionsRequest.resource_group` and `ShardLeadersList.resource_groups`
  are appended proto fields; an old peer leaves them empty and both sides read
  empty as "no scope".
- Every configuration item defaults to the stock behavior.
- `hook.Hook` is milvus-proto's and unchanged.
- The default configuration file is `milvus.yaml`; a distribution that ships
  its configuration under another name places it as `user.yaml` in
  `MILVUSCONF`, which the existing file list already reads last.

## Test plan

- `pkg/extension`: the setters and getters, and the context mark.
- hookutil: a compiled-in hook is used, refused beside a plug-in, absent by
  default, initialized with the `hook.*` configuration, and re-initialized
  when that configuration changes.
- proxy: a hook-pinned resource group reaches the search task; per-resource-
  group latency series exist only for pinned requests.
- mixcoord: the engine starts on activation only, receives the coordinator
  client, and is stopped once.
- querycoord / datacoord: each hook-gated behavior with and without a form
  installed - the stock cases assert master's answers (a load refused for a
  group with no regular node, a scoped load that moves the replica, version 0
  with no session) - plus a request naming no resource group, a load
  percentage that regresses, the three ways a serving group reads as an
  unreliable 0 (failed read, no target, a replica that has not reported), a
  ready group whose percentage sits at 99 for the whole timeout, a load
  timeout that may not unload a serving collection, a replayed expansion,
  the rebuild of scoped load tasks after a restart, and an index engine
  version override clamped with no QueryNode registered.

## Rejected alternatives

- **A capability table with a `Provider`, `Requires()` and typed-nil checks.**
  Shipped first as #52981. Eight capabilities, nine cross-boundary interfaces,
  eighteen read points; most entries were constants or duplicates of existing
  mechanisms. Replaced by this document.
- **A link-time primary configuration file name.** An entrypoint that renames
  the file solves it with no milvus change.
- **Per-callback interfaces for the proxy request path.** Every proxy RPC
  already passes the hook's `Mock`, `Before` and `After`, and `Before` returns
  the handler's context.
