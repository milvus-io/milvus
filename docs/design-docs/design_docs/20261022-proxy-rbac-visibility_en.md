# Proxy-local RBAC Visibility and Snapshot Consistency Fix

- Date: 2026-10-22
- Status: PR1 implemented with local mode disabled by default; PR2 remains a proposal
- Analysis baseline: `2158f40a2e`; PR1 branch base: `0cd0677567`; incident comparison version: `v3.0.3`
- Scope: RBAC, DescribeCollection visibility, and Proxy RBAC snapshot synchronization. RLS, password authentication caches, and migration of authorization for all other APIs are excluded.

## 1. Problem and objective

The Proxy Casbin check answers whether a caller may invoke DescribeCollection. RootCoord (RC) separately checks whether the caller may see the target collection. The public role receives a global DescribeCollection permission, so the existing Casbin decision cannot replace collection visibility authorization.

The cached provider calls RC for visibility even on a metadata cache hit. RC reads authorization metadata through `getCurrentUserVisibleCollections → SelectGrant → Catalog.ListGrant`. Checking ownership of an applicable legacy 16-character grantee ID can call `findOtherGranteeWithID`, which scans the entire `grantee-privileges/` prefix. Ordinary ListGrant does not reuse that full scan across legacy grants.

The objective is complete user authorization in Proxy: no visibility recheck RPC on a hot Describe cache, and no etcd reads for steady-state visibility decisions, while retaining object isolation and revocation protection.

The incident's approximately 16,191 keys / 2.3 MB per scan comes from the supplied incident context, not an independent production query. The code mechanism has been traced; the deployed commit, legacy IDs, and actual trigger counts still require production verification.

## 2. Meaning of equivalence

1. **Rule equivalence:** given identical RBAC state, user, and resolved resource, Proxy and the original RC logic produce the same allow/deny decision.
2. **Activation contract:** after a permission mutation returns success, subsequent requests cannot use revoked permissions. Requests that already completed authorization may finish.
3. **Resource identity:** authorize the resolved database, canonical collection name, and collection ID, rather than an alias or caller-supplied database hint.

A local cache without a remote barrier cannot unconditionally observe RC's latest state on every request. The target is the completion contract above, not identical observations at every instant while a mutation is in progress.

## 3. Target structure

```text
RC: raw grants + groups + user-role bindings
    → compile visibility rules and action policies
    → distribute revisioned snapshots / updates

Proxy: authenticated user
    → Casbin method permission check
    → resolve collection identity (shared metadata cache / internal RC lookup)
    → local CanSeeCollection(snapshot, user, resolvedResource)
    → return response
```

Reuse RootCoord, CChannel, the Proxy privilege cache, and Casbin. Do not introduce a separate authorization service, change public SDK APIs, or require an etcd RBAC storage-format change.

## 4. PR1: Equivalent Proxy CanSeeCollection (M)

### 4.1 Rules and data

Extract the existing RC visibility rules into reusable, I/O-free compilation and lookup functions. Preserve:

- authorization-disabled, rootShouldBindRole, and admin behavior;
- exclusion of public from collection visibility contributions;
- applicable database scopes, role union, Global All, and Collection grants;
- built-in/custom privilege groups and existing wildcard matching;
- cases where raw grants and expanded action policies differ, including empty groups.

Do not reconstruct raw visibility from expanded Casbin policies. RC should emit a minimal projection:

```text
RoleVisibility {
    role
    rules: [{ database, collection }]
}
```

`*` represents visibility over the corresponding scope. Compile this projection using existing RC semantics; do not silently replace existing string matching with Casbin glob matching.

Extend internal `ListPolicyResponse` with `visibility_format_version` and the visibility projection. Keep the additions compatible with PR2's revision fields. Distinguish unsupported/missing data from a valid empty rule set. Regenerate protocol code using repository tooling; do not edit generated files manually.

### 4.2 Proxy integration

- Store visibility data in the privilege cache and expose a separate `CanSeeCollection`. Do not store a user's allow/deny in the shared metadata cache.
- Cached provider: resolve identity, authorize locally, and remove the extra per-response visibility RPC in local mode.
- Remote provider: fetch metadata through a trusted internal request, then authorize in Proxy before returning to the user.
- A remote task can retain the requested alias in CollectionName. Use the resolved schema name, actual database, and ID for authorization.
- Cover cached/remote, gRPC, HTTP, and console Describe paths; preserve the verified management administrator's operator view.
- With an older RC or missing required resource identity, use the original user-forwarded RC check or fail closed. Never allow by default.

Initially retain RC checks for requests carrying user identity. New Proxies use the existing trusted internal metadata path in local mode. Do not accept a client-controlled field or header as proof of completed authorization.

### 4.3 Mutation integration

Connect visibility replacement/invalidation to existing notifications for grants, revokes, user-role changes, user/role drops, groups, restore, and collection drop/rename/database moves. A raw grant mutation must trigger a visibility update when needed even if expanded action policies do not change.

Prefer complete visibility replacements for affected roles. Simple add/remove operations can mishandle multiple grants supporting the same visible resource.

PR1 proves rule equivalence only. It does not claim to fix lost notifications, stale refresh overwrites, or bootstrap synchronization gaps. It can merge independently, with local mode disabled by default until PR2 is complete.

### 4.4 PR1 implementation choices

- `proxy.enableLocalCollectionVisibility` defaults to `false`. PR1 does not enable the new path for existing deployments.
- Internal `CollectionVisibilitySnapshot` version 1 distinguishes unsupported data from supported empty rules. `ListPolicyResponse` carries the full projection; grant/revoke notifications carry complete replacements for affected roles across all databases.
- A raw grant change with no expanded action delta uses the existing full-refresh operation so older Proxies remain compatible. Group membership changes preserve raw visibility and carry an explicit supported empty partial update.
- Legacy grant/revoke deltas without a projection invalidate local visibility support until the next complete refresh. They do not infer visibility from action policies.
- Cached responses authorize the resolved cache entry locally. The remote task has a provider-selected internal-metadata mode and the provider checks its resolved result before release. Console ordinary-user requests use the same local decision and retain RC fallback; verified management administrators retain their operator view.
- RC retains user-forwarded visibility checks, and no client-controlled bypass field is added. Missing actual database identity fails closed.
- The PR does not implement revision ordering, bootstrap catch-up, resource-generation fencing, or a cluster-wide revocation completion protocol. Those remain PR2 work. Local mode must remain disabled for the default rollout until that work is complete.

### 4.5 Estimated size

- 10–16 source/test files, excluding generated files.
- Approximately 500–900 production lines and 600–1,000 test lines.
- Approximately 5–8 engineer-days for one engineer; a rough estimate, not a schedule commitment.
- Main areas: RC visibility/ListPolicy, internal protobufs, Proxy privilege cache, providers, DDL Describe task, HTTP/console, and regression tests.

## 5. PR2: Proxy RBAC snapshot consistency (L)

### 5.1 Versioning and publication

- Add revisions to full snapshots and deltas; define RC commit and publication ordering.
- Ignore duplicate/stale updates. Resynchronize on a revision gap.
- Produce a coherent RC snapshot instead of combining grants, groups, and role bindings from different logical states.
- Build roles, Casbin, and visibility state before publishing them together. Do not ACK successful installation after a build failure.
- Pin one published snapshot per request and bind decision-cache entries to that snapshot's version.
- A revision is not the existing local result-cache version. It must not be assumed to equal a WAL TimeTick without defining concurrent callback publication and restart identity.

### 5.2 Lifecycle and failures

- Close the gap between a Proxy's initial snapshot fetch and registration for updates. Local authorization cannot serve before catch-up completes.
- Define resynchronization after RC failover, reconnects, and missed updates.
- Include collection drop, rename, database moves, and restore in the authorization version boundary.
- If a relevant generation changes during identity resolution/authorization, resolve again or return a retriable error. Caching permissions without resource-binding invalidation does not preserve the current protections.
- Successful revocation requires every still-serving Proxy to install the state or be prevented from accepting requests under old authorization through an enforceable serving gate. Routing removal alone does not cover direct node access.
- A failed notification must not permit indefinite stale authorization. Define retries, serving gates, and recovery together.

### 5.3 Estimated size

- 12–20 source/test files. Some overlap PR1, so counts are not additive unique-file totals.
- Approximately 800–1,500 production lines and 800–1,500 test lines.
- Approximately 7–12 engineer-days for one engineer. Main uncertainties are revocation completion, bootstrap catch-up, and RC failover.
- RBAC snapshots only: no password-cache, RLS, or general metadata-cache rewrite.

## 6. Verification and rollout

### Rule and identity matrix

Compare the original RC rules and new functions on identical inputs. Cover public/admin/root, custom/built-in groups, empty groups, overlapping permissions and revokes, multiple roles, DB/collection wildcards, default database, aliases, ID-only lookups, incorrect/missing database hints, same-name collections across databases, management administrators, and denied responses without schema/ID/alias leakage.

### Concurrency and fault matrix

Cover an old full snapshot arriving after a revoke, duplicate/out-of-order/missing notifications, failed snapshot installation, revocation during Proxy bootstrap, RC restart/failover, a disconnected Proxy, same-name recreation, rename/database moves, and mixed versions.

### Performance acceptance

- Hot Describe in local mode: zero extra coordinator visibility RPCs.
- Steady-state local visibility decisions: zero etcd reads.
- Cold metadata lookups are allowed; they must not reintroduce per-grant full-tree scans.
- Validate request/RPC/Range counts, CPU, and latency, not successful responses alone.

### Rollout order

1. PR1: add equivalent rules, capability detection, and fallback. Keep RC final checks as the default.
2. PR2: complete versioning, revocation, and resource-invalidation contracts.
3. Validate mixed versions, then enable local mode incrementally per instance. Fall back when capability/readiness requirements are not met.

Go tests must use `-tags dynamic,test -gcflags="all=-N -l" -count=1`.

PR1 validation on macOS arm64 with the local native libraries:

- Full package tests passed for `pkg/util/rbacutil` and `pkg/util/paramtable`.
- Targeted RBAC/privilege/Describe regressions passed in `internal/proxy/privilege`, `internal/rootcoord`, `internal/proxy`, `internal/proxy/ddl`, and `internal/distributed/proxy/httpserver`. RC write-side callback tests used a temporary local etcd.
- The hot-cache test has no coordinator Describe expectation and checks repeated successful local decisions followed by denial after a role projection replacement.
- Tests cover supported-empty versus unsupported snapshots, legacy delta fallback, role binding removal/addition, empty custom groups, exact/wildcard matching, resolved DB/ID, and internal RPC identity stripping without mutating the parent context.
- The deployment integration matrix now covers cached/remote providers with local mode on/off. It was compiled, not executed against rebuilt Milvus component processes.
- Protobufs were regenerated through `make generated-proto-without-cpp`; generated files were not hand-edited.

No complete repository test run, deployment E2E run, failure-injection consistency test, or production benchmark is claimed. PR2 guarantees remain unverified and local mode stays disabled by default.

## 7. Quick mitigation: reuse legacy-ID check data within ListGrant (S)

### Minimal patch

Ordinary `Catalog.ListGrant` currently enters the per-grant path without a loaded-grantees provider. Create one lazy provider for **each ListGrant invocation** and reuse its full-prefix data for all legacy ownership checks in that invocation. Existing `newLazyLoadedGranteesProvider` and `listGrantWithLoadedGranteeProvider` provide the implementation foundation.

Load the full tree only when an applicable legacy ID requires ownership checking. Do not add a full scan to new-ID paths. Keep the provider invocation-local: no cross-request cache, TTL, or invalidation protocol.

Preserve an important compatibility detail: current `findOtherGranteeWithID` treats a full-prefix `ErrIoKeyNotFound` as no other owner, whereas the existing lazy provider propagates it. Do not blindly make a one-line substitution. Preserve the original missing-prefix, empty-result, error, candidate-lookup, and shared-legacy-ID rejection behavior.

### Expected benefit and limits

Let Q be request rate, R the roles actually read per request, L the total legacy grants checked, and S the full-tree response size:

```text
Before: approximately Q × L × S in full-tree responses
After:  approximately Q × R_legacy × S
R_legacy = ListGrant invocations encountering at least one checked legacy grant
```

This caps full scans at one per role lookup, not one per Describe. If each request checks only one legacy grant, savings can be small. At 7 QPS and one remaining 2.3 MB scan per request, estimated responses still total about 16.1 MB/s. The patch cannot guarantee that a 2-core etcd stops saturating.

Reusing arrays retains per-ID linear CPU scans. A separate invocation-local `legacyID → owners/ambiguous` index can reduce that cost if necessary.

### Acceptance and size

- Multiple unique legacy IDs: one full-prefix load per ListGrant, with equivalent returned grants.
- New IDs only: no full-prefix load for ownership checks.
- Shared legacy IDs: ambiguous authorization is still rejected.
- Check missing prefixes, empty results, storage failures, cancellation, legacy/new candidates, database filtering, and independent calls that must not reuse previous data.
- About one production file and one test file: 10–40 production lines, 100–250 test lines, and 1–2 engineer-days.

### Operational relief without a patch

Retain the already-applied 2-core limit and inspect post-change throttling, Range latency, and traffic. Clients should reuse connections and collection/schema metadata, reducing repeated Describe calls under a refresh policy appropriate for their schema changes. Connection reuse alone does not eliminate application loops issuing Describe.

Do not disable authorization, grant root/admin, bypass role checks, or revert object-visibility protection. Do not rewrite production legacy IDs as a quick mitigation; migration needs its own collision, recovery, and rollback review.

## 8. Source and historical references

- `internal/proxy/service_provider.go`: shared metadata and per-request RC visibility checks.
- `internal/proxy/ddl/task.go`, `describe_util.go`: remote Describe and identity contexts.
- `internal/rootcoord/describe_collection_task.go`, `root_coord.go`: resolved-resource visibility.
- `internal/metastore/kv/rootcoord/kv_catalog.go`: ListGrant, legacy ownership checks, lazy provider.
- `internal/proxy/privilege/cache.go`, `result_cache.go`: current cache updates and local version.
- `internal/rootcoord/ddl_callbacks_rbac_*.go`, `rbac_task.go`: write-side notifications and group updates.
- `pkg/proto/internal.proto`, `pkg/proto/proxy.proto`: snapshot/update protocols.
- [RBAC WAL semantics](../../agent_guides/streaming-system/message/message-semantic-rbac.md): CChannel and ExclusivePrivilege.
- [PR #41798](https://github.com/milvus-io/milvus/pull/41798): RC Describe visibility added in 2025.
- [Issue #53723](https://github.com/milvus-io/milvus/issues/53723), [PR #53724](https://github.com/milvus-io/milvus/pull/53724): cached/ID-only bypasses, actual database identity, and per-response RC RPC tradeoff.
- [PR #51404](https://github.com/milvus-io/milvus/pull/51404): scan reuse in ListPolicy does not cover ordinary ListGrant.
