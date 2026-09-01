# Milvus Row-Level Security (RLS) Design

- **Feature DRI:** TBD
- **Primary Approver:** TBD
- **Independent Approver:** TBD
- **Design Review:** TBD

## Overview

Row-Level Security (RLS) restricts row-bearing operations on a collection by
combining user requests with collection-scoped row policies. RLS is enabled per
collection through the `rls.enabled` collection property. The optional
`rls.force` collection property prevents request-scoped bypass when RLS is
enabled.

The runtime caller supplies an `rls_principal` string on RLS-protected
operations. The principal is an application-level identity and is deliberately
not derived from the authenticated Milvus username. Policies can reference that
principal and its collection-scoped tags to build a query/search/delete
predicate or to validate inserted/upserted rows. A caller that omits the
principal must request `skip_rls=true` and pass the corresponding privilege
check; otherwise the operation is denied.

## Collection Switch

RLS is disabled by default. Enable it when creating the collection:

```python
client.create_collection(
    collection_name="docs",
    schema=schema,
    properties={"rls.enabled": "true", "rls.force": "true"},
)
```

The initial release treats `rls.enabled` as immutable after collection creation.
`alter_collection_properties` rejects setting, changing, or deleting it. RLS
policy and principal management APIs are available only when the target
collection was created with `rls.enabled=true`.

A future implementation may support disabling RLS dynamically. That transition
must delete all policy and principal metadata for the collection and invalidate
all Proxy RLS cache entries before the disabled state is reported as complete.
If dynamic enabling is added later, it must synchronously load the collection's
complete RLS metadata from MixCoord before the enabled state becomes visible to
requests.

When `rls.enabled=false`, row-bearing operations bypass RLS. When
`rls.enabled=true`, the request must either provide `rls_principal` or set
`skip_rls=true` with permission to bypass RLS.

When `rls.force=true`, `skip_rls=true` is rejected and every row-bearing request
must pass RLS. When `rls.force=false`, `skip_rls=true` bypasses RLS only when
authorization is disabled or the current authenticated Milvus user has the
`SkipRLS` privilege on the target collection.

## Principal Tags

Principal tags are collection-scoped metadata stored by RootCoord and synced to
Proxy caches.

```python
client.set_rls_principal_tags(
    collection_name="docs",
    principal_name="alice",
    tags={
        "tenant": "acme",
        "department": "engineering",
    },
)
```

Supported principal tag APIs:

| API | Behavior |
| --- | --- |
| `set_rls_principal_tags` | Incrementally upsert a non-empty tag map for one principal. Supplied keys are added or overwritten, while unspecified tags are preserved; an empty map is rejected. |
| `get_rls_principal_tags` | Return the tag map for one principal. |
| `list_rls_principals` | List principals with tags on one collection. |
| `delete_rls_principal_tags` | Delete selected tag keys. If no keys remain, delete the principal tag record. |

Passing no tag keys deletes the complete principal tag record. A non-empty key
list deletes only those tags, and the principal record is also deleted when no
tags remain afterward. Deleting an already-missing principal succeeds as an
idempotent retry. Repeated tag keys are deduplicated, and the number of distinct
keys in one delete request is bounded
by `proxy.rls.maxTagsPerPrincipal` before RootCoord broadcasts the mutation.
The raw list is also subject to a separate fixed transport/work bound before
deduplication.

Policy expressions may reference:

| Variable | Meaning |
| --- | --- |
| `$current_principal` | The request `rls_principal` value. |
| `$current_principal_tags['key']` | The tag value for `key` on the current principal. |

Tag keys cannot contain a single quote because policy tag references use the
single-quoted `$current_principal_tags['key']` syntax and do not support key
escaping.

If an enabled RLS policy references a missing principal tag at runtime, that
policy predicate evaluates to false for the current request.

## Row Policies

Each policy belongs to one collection and has a unique `policy_name` in that
collection. `CreateRowPolicy` rejects any existing policy with the same name,
even when the requested definition is identical. `UpdateRowPolicy` updates an
existing policy by name and preserves its internal `policy_id`.

```python
client.create_row_policy(
    collection_name="docs",
    policy_name="tenant_isolation",
    policy_type="permissive",
    actions=["query", "search", "delete", "insert", "upsert"],
    using_expr="tenant == $current_principal_tags['tenant']",
    check_expr="tenant == $current_principal_tags['tenant']",
    description="Tenant scoped access",
)
```

Supported policy APIs:

| API | Behavior |
| --- | --- |
| `create_row_policy` | Create a new named policy. Any existing policy with the same name causes the request to fail. |
| `update_row_policy` | Replace an existing named policy while preserving its `policy_id`. |
| `drop_row_policy` | Drop a named policy. Dropping a missing policy succeeds as an idempotent retry. |
| `list_row_policies` | List policies on one collection. |

Supported actions:

| Action | Uses `using_expr` | Uses `check_expr` |
| --- | --- | --- |
| `query` | Yes | No |
| `query_iterator` | Yes | No |
| `search` | Yes | No |
| `search_iterator` | Yes | No |
| `hybrid_search` | Yes | No |
| `delete` | Yes | No |
| `insert` | No | Yes |
| `upsert` | Yes, for existing rows | Yes, for written rows |

`Get` is a client-side convenience over `Query`; it is not a separate RLS
action.

## Policy Evaluation

RLS is deny-by-default when enabled. If an operation has no applicable policy
for its action and required expression kind, the operation fails with a
privilege error.

Policies are combined by policy type:

```text
(permissive_policy_1 OR permissive_policy_2 OR ...)
AND
(restrictive_policy_1 AND restrictive_policy_2 AND ...)
```

At least one applicable permissive policy is required. If only restrictive
policies match an action, the final predicate is false.

`CreateRowPolicy` evaluates the prospective complete policy set and rejects it
when any action's combined `using_expr` or `check_expr` exceeds the current
`proxy.rls.maxCombinedExpressionLength`. `UpdateRowPolicy` does not apply this
creation guard because an update may reduce an already oversized policy set.
Proxy checks the complete expression whenever it compiles a runtime predicate,
so an update or a refreshable limit reduction that leaves the set oversized
causes later row-bearing requests to fail with a quota error until the policies
or configuration are adjusted.

For query, search, and delete, the final `using_expr` predicate is merged into
the request plan with logical AND. For insert, the final `check_expr` predicate
is evaluated against each input row in Proxy. For upsert, existing rows must pass
the `using_expr` check, and the final row written by the upsert must pass
`check_expr`.

Local insert and upsert checks use SQL three-valued logic, matching Segcore
filter evaluation. Comparisons involving NULL produce UNKNOWN, boolean
operators preserve UNKNOWN according to SQL semantics, and only a final TRUE
result admits a row.

## Expression Support

RLS expressions intentionally use a restricted subset of Milvus boolean
expressions so Proxy can both merge predicates into plans and locally evaluate
write checks.

Supported expression forms:

- `true` / `false`
- equality comparisons between a top-level scalar field and a literal or
  supported template value
- `in` with literal value lists
- `array_contains`, `array_contains_all`, and `array_contains_any` on primitive
  array fields
- `$current_principal` and `$current_principal_tags['key']` as string template
  values

Each individual `using_expr` or `check_expr` must contain exactly one simple
predicate. Inline `and`, `or`, and boolean `not` are rejected; policy authors
compose predicates through multiple permissive or restrictive policies.

RLS pseudo variables follow the normal Milvus expression-template syntax: only
unquoted variable tokens are converted to template variables. Identical text in
normal or raw string literals remains literal data.

Unsupported forms include vector fields, JSON fields, nested/element-level
fields, system fields, inline boolean composition, ordered comparisons,
field-to-field comparisons, and dynamic functions such as `now()`.

## Metadata And Sync

RootCoord owns RLS metadata. Policies and principal tags are persisted in etcd as
separate records and cached in RootCoord collection metadata for collection
cleanup. Persistent RLS records are addressed only by the globally unique
collection ID; database ID and collection name are descriptive metadata and do
not participate in identity.

Policy and principal-tag mutations use the same broadcast task mechanism and
the same `SharedDBName + ExclusiveCollectionName` resource keys as collection
DDL. After validation under that collection-scoped resource, RootCoord appends
a CChannel-only message containing either the complete normalized post-image or
the stable identity to drop. The ACK callback persists the mutation and updates
the RootCoord collection cache before the resource is released. This serializes
RLS validation and commit with CreateCollection, DropCollection, and schema
changes, so collection lifecycle or schema dependencies cannot cross an RLS
mutation.

RLS catalog I/O does not hold RootCoord's global DDL or RBAC locks. The ACK
callback performs catalog I/O first and holds the global collection metadata
lock only briefly while replacing the cached policy or principal entry. The
post-image and drop callbacks are idempotent, so broadcaster recovery can retry
them safely after a coordinator restart.

Proxy maintains an in-memory RLS manager cache. RootCoord exposes an internal,
collection-scoped `GetRLSMetadata` RPC that returns the collection identity,
all row policies, and all principals with their complete tag maps in one
response. Proxy startup only configures the RLS manager dependencies; it does
not enumerate collections or preload metadata. Only a request that actually
enforces RLS can establish or refresh a collection's RLS state.

Each RLS WAL message carries the affected collection's Proxy cache expiration.
After applying the metadata post-image or drop, the ACK callback synchronously
invalidates that collection's complete RLS state on every active Proxy, using
the same callback and retry model as AlterCollection schema invalidation. A
real Proxy notification failure keeps the callback pending; the broadcaster
retries it with exponential backoff while the collection resource remains
serialized. The Proxy invalidation is intentionally lightweight and does not
fetch metadata inside the callback.

The next RLS-enforced request observes the missing state and loads both policy
and principal snapshots through one `GetRLSMetadata` call. There is no
background reconciliation loop. On every RLS-enforced use, Proxy treats a
snapshot whose last successful load is older than
`proxy.rls.metaRefreshInterval` as stale and refreshes it synchronously. Thus a
missed invalidation converges on the first request after the freshness window,
while collections that are not accessed produce no metadata RPCs.

As with Proxy collection-schema cache fills, a per-collection read/write
barrier orders RLS refreshes against invalidation. A refresh holds the read
side across its singleflight lifecycle; invalidation takes the write side,
waits for earlier refreshes to finish, and then deletes the state. A refresh
also writes only to the collection-state incarnation captured before its RPC,
so a detached result cannot recreate or overwrite a later incarnation.
Snapshot version tokens continue to suppress out-of-order refreshes within one
live incarnation; they are not treated as authoritative RootCoord metadata
revisions.

Every RLS-enforced request checks both policy and principal-tag snapshot
freshness before evaluating a predicate. A missing or expired snapshot is
refreshed synchronously through `GetRLSMetadata`. If the refresh fails, the
request fails closed and the expired snapshot is not used for authorization.
Concurrent request-path refreshes for the same collection are coalesced.

The manager-level lock protects only the collection-state map and dependency
configuration. A recyclable keyed read/write lock provides the
per-collection refresh/invalidation barrier without serializing unrelated
collections. Invalidation deletes the current state pointer; refreshes write
only to the incarnation captured before their RPC. Each collection state has
its own read/write lock for snapshot
versions, refresh timestamps, policies, principal tags, and compiled predicate
state. MixCoord RPCs are always executed without the manager or state lock
held; only the target collection's refresh read guard spans the RPC, so
metadata work for one collection cannot serialize another collection.
On a compiled-cache hit, predicate evaluation holds the collection read lock
only long enough to capture the immutable compiled expression and principal-tag
snapshot. Expression cloning, template instantiation, and rewriting happen after
the lock is released. A cache miss compiles under that collection's write lock.

RLS policy and principal-tag broadcast messages are eligible for the generic
CDC path and are not marked unreplicable. On a secondary, the replicated
CChannel message rebuilds a replicated broadcast task and invokes the same
idempotent ACK callback used on the primary, applying the complete metadata
post-image or stable drop identity. This initial release has no pre-existing RLS
metadata to bootstrap. Dedicated end-to-end RLS CDC compatibility and recovery
validation remains follow-up work; the current implementation relies on the
generic replicated-broadcast contract.

## Configuration

| Config | Meaning |
| --- | --- |
| `proxy.rls.maxPoliciesPerCollection` | Maximum policies on one collection. |
| `proxy.rls.maxPrincipalsPerCollection` | Maximum principals on one collection. Existing principals remain updatable if the limit is lowered. |
| `proxy.rls.maxTagsPerPrincipal` | Maximum tags on one collection-scoped principal. |
| `proxy.rls.maxExpressionLength` | Maximum length of one `using_expr` or `check_expr`. |
| `proxy.rls.maxCombinedExpressionLength` | Maximum length of the final combined expression. |
| `proxy.rls.maxPolicyNameLength` | Maximum policy name length. |
| `proxy.rls.maxPolicyDescriptionLength` | Maximum policy description length in bytes. |
| `proxy.rls.maxPrincipalNameLength` | Maximum principal name length. |
| `proxy.rls.maxTagKeyLength` | Maximum principal tag key length. |
| `proxy.rls.maxTagValueLength` | Maximum principal tag value length. |
| `proxy.rls.maxArrayLiteralElements` | Maximum literal elements in `in` and `array_contains*` expressions. |
| `proxy.rls.metaRefreshInterval` | Maximum age in seconds of cached policy and principal metadata before an RLS-enforced request refreshes it. |
