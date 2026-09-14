# Resource-group scoping for the replica load-config compliance endpoint

- Status: Implemented
- Date: 2026-09-11
- Scope: `internal/coordinator/restful_replica.go`, `internal/querycoordv2/server.go`
- Related: builds on the per-resource-group readiness work in `20260824-per-resource-group-load-and-shard-leader-readiness.md`

## 1. Problem

`GET /management/replica/loadconfig/compliance` answers one cluster-wide question: has every loaded collection converged to the cluster-level replica load config (`queryCoord.clusterLevelLoadReplicaNumber` / `clusterLevelLoadResourceGroups` / `clusterLevelLoadForceOverrideUserReplicaMode`). Ops tooling polls it and proceeds — e.g. terminates querynodes — only once `state == "Ready"`.

The per-collection step checks four things: replica count, RG distribution, per-replica serviceability (a live shard-leader check), per-replica query-visibility, and leaked resources on non-replica nodes. All four are **collection-wide**. When a rollout only touches nodes of specific resource groups — e.g. restarting the querynodes of `rg-b` while `rg-a` keeps serving — the caller only needs serving readiness for the groups it is about to disrupt. A still-converging replica in an unrelated group holds the whole endpoint at `NotReady`, blocking a rollout that is actually safe to proceed.

## 2. Design

Add an optional query parameter:

```
GET /management/replica/loadconfig/compliance?resourceGroups=rg-a,rg-b
```

Semantics: **only the two per-replica liveness checks are scoped** — serviceability and query-visibility consider just the replicas whose own resource group is in the list. Everything else stays cluster-wide:

| Check | Under `?resourceGroups=` | Why |
|---|---|---|
| WAL primary-RG placement | unchanged (cluster-wide) | Independent of per-collection replica layout; a WAL misplacement affects every collection. |
| Replica count vs `clusterLevelLoadReplicaNumber` | unchanged | Layout is a property of the whole collection; a partial count answer would mask a failed scale-up in another group. |
| RG distribution vs `clusterLevelLoadResourceGroups` | unchanged | Same reason. |
| Per-replica serviceability (`CheckAllReplicasServiceable`) | **scoped to listed groups** | This is the rolling-restart question: can the groups I am about to touch serve every shard right now. |
| Per-replica query-visibility | **scoped to listed groups** | Same question from the routing side: a not-yet-promoted replica in a group I am not touching must not hold me. |
| Leaked resources (`GetLeakedResourcesByCollection`) | unchanged (cluster-wide) | Fail-safe: a leak anywhere means some node still holds state; the reason string names the collection. Deliberately not scoped — a leaked node may already have been unassigned from its group, so group attribution of leaks is best-effort at exactly the moment the leak matters. |

Parameter handling details:

- **Absent or empty parameter → byte-identical current behavior.** No compatibility break; the endpoint previously accepted no parameters.
- **Unknown group name → `400 {"msg": "unknown resource group: <name>"}`.** A typo matches no replica and would silently pass both scoped checks — a vacuous `Ready` is the worst possible answer for an ops gate. Validated via `ResourceManager.ContainResourceGroup` before any check runs, matching the input-error treatment `ShardLeaderReadinessByResourceGroup` gives the same mistake.
- Duplicates are deduplicated; entries are whitespace-trimmed.

### 2.1 QueryCoord additions

Two methods on `*querycoordv2.Server` (`internal/querycoordv2/server.go`); the handler already holds the concrete server, so no interface or mock regeneration is involved:

- `CheckReplicasServiceableInRGs(ctx, collectionID, rgNames)` — the scoped form of `CheckAllReplicasServiceable`, reusing the same private `checkReplicaServiceable` per replica (leader-view data-ready + `IsServiceable`), so strictness is identical. **A collection with no replica in the given groups returns nil**, unlike the unscoped form's "no replica found" error: whether any replica *should* live there is the count/distribution check's question, and those still run unscoped. An existing-but-uninvolved group is therefore a vacuous pass — only a nonexistent group (a typo) is an error.
- `ContainResourceGroup(ctx, rgName)` — thin delegate to `meta.Meta.ContainResourceGroup` for the 400 validation.

`utils.ShardLeaderReadinessByResourceGroup` was deliberately **not** reused: it answers per-shard coverage by *any* visible replica of a group and skips the `CheckDelegatorDataReady` leader-view check, i.e. a weaker question than this endpoint's per-replica convergence semantics.

### 2.2 Response contract

Unchanged: `200 {"state": "Ready"|"NotReady", "reason": ...}` for poll results, `{"msg": ...}` for 400/405/500. The filter is echoed in the handler's info log (`resourceGroupsFilter`) and scoped failure reasons carry the replica's group as before.

## 3. Testing

- `internal/coordinator/restful_replica_test.go`: unknown group → 400; filter scopes serviceable+visibility (invisible replica in an unlisted group still yields `Ready`, and the unscoped serviceable method is proven not to run); count check still enforced under a filter; scoped serviceable failure → `NotReady`; leaks still cluster-wide under a filter.
- `internal/querycoordv2/server_test.go`: `CheckReplicasServiceableInRGs` — empty scope → nil; non-serviceable replica inside the filter → error; non-serviceable replica outside the filter → ignored.
