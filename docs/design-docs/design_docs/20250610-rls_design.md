# Milvus Row-Level Security (RLS) Design

## Overview

Row-Level Security (RLS) restricts row-bearing operations on a collection by
combining user requests with collection-scoped row policies. RLS is enabled per
collection through the `rls.enabled` collection property. The optional
`rls.force` collection property prevents request-scoped bypass when RLS is
enabled.

The runtime caller supplies an `rls_principal` string on RLS-protected
operations. The principal is an application-level identity and is deliberately
not derived from the authenticated Milvus username. Policies can reference that
principal and its principal-scoped tags to build a query/search/delete
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

The initial release does not support dynamically enabling RLS. A collection
created without `rls.enabled=true`, or one whose RLS setting was later disabled,
cannot be enabled through `alter_collection_properties`. Dynamically disabling
RLS remains supported by setting `rls.enabled=false` or deleting the property.
Future dynamic-enable support must synchronously load the collection's complete
RLS metadata from MixCoord before the enabled state becomes visible to requests.

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
        "access_level": 3,
        "risk_score": 0.25,
    },
)
```

Supported principal tag APIs:

| API | Behavior |
| --- | --- |
| `set_rls_principal_tags` | Set the full, non-empty tag map for one principal. Existing tags are overwritten; an empty map is rejected. |
| `get_rls_principal_tags` | Return the tag map for one principal. |
| `list_rls_principals` | List principals with tags on one collection. |
| `delete_rls_principal_tags` | Delete selected tag keys. If no keys remain, delete the principal tag record. |

Passing no tag keys deletes the complete principal tag record. Deleting an
already-missing principal succeeds as an idempotent retry. Repeated tag keys are
deduplicated, and the number of distinct keys in one delete request is bounded
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

Principal tag values support `string`, `int64`, and `double`. The value keeps
its declared type through the public API, RootCoord metadata, WAL replay, and
Proxy snapshots. A tag template is applicable only when its type matches the
referenced field exactly by family: string tags match string fields, int64 tags
match integer fields, and double tags match floating-point fields. A mismatch
evaluates that policy predicate to false; Milvus does not coerce between
strings, integers, and floating-point values for RLS tags.

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

`CreateRowPolicy` and `UpdateRowPolicy` evaluate the prospective complete policy
set and reject it when any action's combined `using_expr` or `check_expr` exceeds
the current `proxy.rls.maxCombinedExpressionLength`. Proxy repeats the check when
compiling a runtime predicate. The configuration remains refreshable: lowering
the limit below an already stored policy set may cause later requests to fail
with a quota error until the policies or configuration are adjusted.

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
- `$current_principal` as a string template value and
  `$current_principal_tags['key']` as a string, int64, or double template value

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

Proxy maintains collection-scoped policy state and principal-scoped tag state.
RootCoord exposes a collection-scoped `GetRLSMetadata` RPC for the collection
identity and all row policies. Principal tags are loaded through
`GetRLSPrincipalTags` only for the principal on the current RLS-enforced
request. Proxy startup only configures the RLS manager dependencies; it does
not enumerate collections, policies, or principals.

Policy WAL messages carry a collection policy-cache expiration. Principal-tag
updates and drops carry the affected principal name and invalidate only the
`(collectionID, principal)` tag entry on every active Proxy. Creating a new
principal does not notify Proxy: missing principals are never negative-cached,
so the first request after creation reads RootCoord normally. A real Proxy
notification failure keeps the callback pending; the broadcaster retries it
with exponential backoff while the collection resource remains serialized.

The next RLS-enforced request observes missing policy or principal state and
loads only the missing object. There is no background reconciliation loop. On
every RLS-enforced use, Proxy treats the collection policy entry and the current
principal tag entry independently: either entry older than
`proxy.rls.metaRefreshInterval` is refreshed synchronously. Thus a missed
invalidation converges on the first request after the freshness window, while
unused principals produce no metadata RPCs.

As with Proxy collection-schema cache fills, a per-collection read/write
barrier orders policy refreshes and collection drops against invalidation.
Principal tags additionally use a `(collectionID, principal)` read/write
barrier, so updating or deleting one principal waits only for that principal's
in-flight load. Refreshes hold the read side through singleflight and
invalidation takes the write side before deleting the entry. Policy snapshot
version tokens suppress out-of-order policy writes but are not authoritative
RootCoord metadata revisions; principal ordering is guaranteed by its keyed
read/write barrier.

Every RLS-enforced request checks the collection policy entry and current
principal tag entry before evaluating a predicate. A missing or expired entry is
refreshed synchronously through `GetRLSMetadata` for policies or
`GetRLSPrincipalTags` for the current principal. If the refresh fails, the
request fails closed and the expired entry is not used for authorization.
Concurrent refreshes are coalesced independently per collection policy entry
and per principal tag entry.

The manager-level lock protects the collection-policy map, principal-tag map,
and dependency configuration. Recyclable keyed read/write locks provide the
per-collection and per-principal refresh/invalidation barriers without
serializing unrelated objects. Each collection state has its own lock for
policy versions, policy refresh timestamps, collection identity, and compiled
predicates. MixCoord RPCs run without the manager or collection-state lock;
only the relevant keyed refresh guard spans the RPC. Predicate evaluation
captures the immutable compiled expression and cloned principal tags, then
instantiates the expression after releasing cache locks.

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
