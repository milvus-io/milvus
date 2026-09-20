# MEP: In-Tree Extension Mechanism for Distributions

- **Created:** 2026-08-31
- **Author(s):** @xiaocai2333
- **Status:** In progress
- **Component:** pkg/extension | cmd/milvus | hookutil | Proxy | Coordinator
- **Related Issues:** #52979
- **Implemented by:** #52981

## Summary

A distribution that compiles its own behavior into the milvus binary needs
little from milvus: a way to install a request hook, the QueryNode's tuning
hook and the cipher without a `.so`, and a callback that starts its
control-plane engine when the coordinator becomes active and stops it on
shutdown. `pkg/extension` is those setters, plus the one context mark the
proxy reads. Everything else such a distribution does is either the hook's
own reach, a coordinator RPC, or a configuration item. A stock binary installs
nothing and behaves exactly as before.

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

type QueryHook interface { ... }              // the queryNode.soPath plug-in's method set
func SetQueryHook(h QueryHook)                // the QueryNode prefers it over queryNode.soPath
func InstalledQueryHook() QueryHook

func SetCipher(c hook.Cipher)                 // hookutil prefers it over cipherPlugin.soPathGo
func InstalledCipher() hook.Cipher

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

A distribution calls the setters it needs, then `cmd/milvus.Main(os.Args)`.

### The request hook

`hook.Hook` (milvus-proto) is consulted by the proxy's unary interceptor for
every RPC on both the gRPC and the REST surface, and by the service's two
streams, `CreateReplicateStream` and `DumpMessages`, which consult it by hand -
the same way, `Mock`, `Before` and `After` in order, with the stream's request
(`DumpMessagesRequest`, or an empty `ReplicateRequest` for the replicate
stream, which carries none of its own) and the stream run under the context
`Before` returned. A test fails as soon as the service declares a stream that
does not consult it. A compiled-in hook that also
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
with the new configuration without a restart. It differs in one respect:
`common.panicWhenPluginFail`, which lets an operator run on without a plug-in
that failed to load, does not reach it. A compiled-in hook that cannot
initialize, or is configured beside a plug-in, stops the proxy whatever the
setting says, because the distribution that compiled it in has switched the
coordinators' behaviors on too, and a proxy serving through the default hook
beside them would run half of that distribution.

### The query hook

The QueryNode's search-parameter tuning hook - the `QueryNodePlugin` symbol a
`queryNode.soPath` plug-in exports - can be compiled in the same way
(`SetQueryHook`). A compiled-in query hook is used in preference to
`queryNode.soPath`, and a deployment that configures both is refused at
start-up: both would tune every search, and only one can. The refusal is
reported the way a plug-in's load failure is: the QueryNode treats it as
fatal when `autoIndex.enable` is on, and with tuning off it starts without
either hook, as it always has. It is otherwise treated exactly as the plug-in
is: it gets the same two `Init` calls with the
`autoIndex.params.search` and `autoIndex.params.tuning` configuration before it
is installed, and the same watchers re-initialize it when those keys change.
`autoIndex.enable` still decides whether tuning happens at all, whichever way
the hook got there. `optimizers.QueryHook` is now an alias of
`extension.QueryHook`, so nothing in the tree changes.

### The cipher

The Go half of the cipher plug-in pair - the `CipherPlugin` symbol a
`cipherPlugin.soPathGo` plug-in exports - can be compiled in the same way
(`SetCipher`). A compiled-in cipher is used in preference to
`cipherPlugin.soPathGo`, and a deployment that configures both is refused at
start-up: both would answer for the encryption keys, and only one can. Only
that half changes hands: the C++ half is still loaded by the core from
`cipherPlugin.soPathCpp`, and that path stays the deployment's declaration
that encryption is on. A compiled-in cipher in a deployment without
`soPathCpp` is left idle and `IsClusterEncryptionEnabled` stays false,
exactly as with no plug-in. When it is used it is treated exactly as the
plug-in is: it gets the same `Init` with the `cipherPlugin.*` configuration
before it is installed, and the same reload callbacks re-initialize it when
those keys change.

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
deployment shape a distribution runs - one streaming node kept for DDL and the
write ahead log while queries are served from resource groups of regular query
nodes, one collection loaded into several of those groups independently,
every role rolled from one image - and a stock binary, which
answers false, keeps master's behavior exactly. Because the query coordinator
and the data coordinator read the mark too, a distribution must install its
hook in every role it runs, not only in the proxy.

- **Delegator placement** (`streamingutil.UseStreamingQueryNodeAsDelegator`).
  With the streaming service on, the query coordinator places shard delegators
  on streaming query nodes only, gives every replica a streaming query node of
  its own, and refuses a collection more replicas than there are streaming
  nodes. With a form installed, delegators go onto the replica's regular RW
  query nodes instead, which watch the channel and read the WAL remotely, and
  the streaming node takes no part in serving: a form runs one, and loads the
  same collection into several resource groups. The replica bound in
  `utils.AssignReplica`, the channel and leader checkers, the assign policies'
  node filter, the channel balance helpers and the manual channel transfer ask
  this one question; each already carried both placements. Stock: exactly the
  streaming gate's answer, as before.

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
  never broadcast), and every step is idempotent.

  An expansion registers no load task of its own. The collection stays
  Loaded, so there is no status to write back and no aggregate to complete;
  how far a group has loaded and whether it can serve are answered from the
  live target and distribution (`utils.LoadPercentageByResourceGroup` and
  `utils.ShardLeaderReadinessByResourceGroup`), and the checkers load the
  added replicas on their own schedule. This is what upstream's
  `UpdateLoadConfig` already does when it adds a replica to a loaded
  collection: spawn, write the count, pull the target, and leave the loading
  to the checkers. A group whose replicas never load is the caller's to
  release - the deployment that asked for the group is the one that knows it
  no longer wants it - exactly as an added replica that never loads is
  upstream.
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
- `hook.Cipher` is milvus-proto's and unchanged; `cipherPlugin.soPathCpp`
  keeps its meaning.
- `optimizers.QueryHook` is an alias of `extension.QueryHook` with the same
  method set; a `QueryNodePlugin` built against either loads unchanged.
- The default configuration file is `milvus.yaml`; a distribution that ships
  its configuration under another name places it as `user.yaml` in
  `MILVUSCONF`, which the existing file list already reads last.

## Test plan

- `pkg/extension`: the setters and getters, and the context mark.
- hookutil: a compiled-in hook is used, refused beside a plug-in, absent by
  default, initialized with the `hook.*` configuration, re-initialized when
  that configuration changes, and fatal when it cannot initialize even with
  `common.panicWhenPluginFail` off.
- hookutil: a compiled-in cipher is used when `cipherPlugin.soPathCpp` is
  set, refused beside `cipherPlugin.soPathGo`, idle without `soPathCpp`,
  absent by default, not installed when it cannot initialize, and
  re-initialized on a `cipherPlugin.*` edit.
- querynode: a compiled-in query hook is used, refused beside a plug-in,
  absent by default, initialized with the `autoIndex.params.*` configuration,
  and not installed when it cannot initialize.
- proxy: a hook-pinned resource group reaches the search task; per-resource-
  group latency series exist only for pinned requests; both streams consult
  the hook, and every stream the service declares is one of them.
- mixcoord: the engine starts on activation only, receives the coordinator
  client, and is stopped once.
- querycoord / datacoord: each hook-gated behavior with and without a form
  installed - the stock cases assert master's answers (a second replica
  refused for want of a streaming node, a delegator placed on the streaming
  query node, a scoped load that moves the replica, version 0
  with no session) - plus a request naming no resource group, a load
  percentage that regresses, the three ways a serving group reads as an
  unreliable 0 (failed read, no target, a replica that has not reported), a
  ready group whose percentage sits at 99 for the whole timeout, a group that
  stays unknown past the load timeout and one that loses its last replica
  (neither may keep pushing the checkers), a load
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
