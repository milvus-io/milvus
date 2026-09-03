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
start-up.

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
- With no QueryNode session registered, datacoord answers its own compiled-in
  index engine version (knowhere's for vectors, the constant for scalars)
  rather than zero, which knowhere would otherwise read as "only DISKANN
  loads off disk" and misroute other disk indexes onto the in-memory path.
  This assumes a QueryNode started later runs the same image as the
  coordinator.

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
  default.
- proxy: a hook-pinned resource group reaches the search task; per-resource-
  group latency series exist only for pinned requests.
- mixcoord: the engine starts on activation only, receives the coordinator
  client, and is stopped once.
- querycoord / datacoord: the remaining configuration items off (stock) and
  on; the two now-default behaviors above.

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
