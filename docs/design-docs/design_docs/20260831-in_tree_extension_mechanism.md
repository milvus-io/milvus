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
every RPC on both the gRPC and the REST surface, and by `CreateReplicateStream`.
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
its engine would accept work nothing accounts for. The engine reaches the
coordinator through nothing but `Coordinator`, so what it can do is exactly
what a proxy can do - including seeding its own accounts through
`CreateCredential` / `OperateUserRole`, and reading per-resource-group load
progress through `ShowLoadCollections` with `resource_group` set.

### Context marks

`WithQueryResourceGroup` pins a query to one resource group. The shard client
routes it to the leaders whose replica lives in that group
(`ShardLeadersList.resource_groups`) and the proxy attributes its latency to
that group. Nothing in a stock binary sets it.

## Configuration this mechanism relies on

| Key | Default | What a distribution sets it for |
|---|---|---|
| `dataCoord.externalCollection.refreshWaitForIndex` | false | hold a refresh until its segments are indexed |
| `builtinRoles.*` | - | the roles the distribution's accounts bind to |

## General behavior

Two things a distribution used to configure are unconditional milvus behavior
now, not part of this mechanism:

- A load request naming resource groups only ever changes the placement in
  those groups: the groups it does not name keep the replicas they hold, and
  the querycoord load job recognizes a request that only adds groups as a
  pure expansion and keeps the collection serving instead of resetting it to
  Loading.

  The scope comes from the request as the caller wrote it. A request naming
  no group at all states the whole placement, which is what a plain
  `load_collection` or `load_partitions` has always meant, so nothing is
  carried over for it; a request that names `__default_resource_group`
  explicitly is a scoped request like any other. A cluster-level load
  override states the whole placement too, whatever the request named.

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
  - The teardown itself has a hard limit: it may shrink an expansion that
    never came up, and it may abandon a load that never completed, but it may
    never take the last replicas of a *Loaded* collection or delete its load
    meta. If that is what releasing the group would do, the task is dropped
    and the collection keeps serving. The collection-wide path has always had
    this property, because its timeout only runs while a collection is
    Loading; a scoped task sits on a Loaded collection, so the rule is
    written out. Between the two - only evidence starts the clock, and the
    teardown can never unload a serving collection - a reading that is wrong
    in the pessimistic direction costs nothing.
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

- With no QueryNode session registered, datacoord answers its own compiled-in
  index engine version (knowhere's for vectors, the constant for scalars)
  rather than zero, which knowhere would otherwise read as "only DISKANN
  loads off disk" and misroute other disk indexes onto the in-memory path.
  This assumes a QueryNode started later runs the same image as the
  coordinator, and the same assumption sets the upper bound with no session:
  an operator's `dataCoord.targetVecIndexVersion` (or the scalar one) above
  what this image can load is clamped down to it rather than written into
  index builds unchecked. This changes what a *forced* override does in that
  window: with no QueryNode registered, a target version above the
  coordinator's own is silently clamped, with only a rate-limited warning in
  the log, where before it was written through. A QueryNode that does register
  replaces both figures with the cluster-wide ones.

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
- querycoord / datacoord: the remaining configuration items off (stock) and
  on; the two now-default behaviors above - including a request naming no
  resource group, a load percentage that regresses, the three ways a serving
  group reads as an unreliable 0 (failed read, no target, a replica that has
  not reported), a load timeout that may not unload a serving collection, the
  rebuild of scoped load tasks after a restart, and an index engine version
  override clamped with no QueryNode registered.

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
