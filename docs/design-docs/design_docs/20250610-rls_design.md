# Milvus Row-Level Security (RLS) Design

- **Feature DRI:** @aoiasd
- **Primary Approver:** @zhengbuqian
- **Independent Approver:** TBD
- **Design Review:** TBD

## Overview

Row-Level Security (RLS) restricts row-bearing operations with
collection-scoped policies. It is disabled by default and controlled by the
`rls.enabled` collection property.

The caller supplies an application-level `rls_principal`. It is intentionally
independent of the authenticated Milvus username because a Milvus account
commonly represents an application serving many end users. A trusted
application derives the principal from its own authentication context and
passes it to Milvus. Policies may reference the principal and its tags.

RLS fails closed when required metadata, a required tag, or an applicable
permissive policy is unavailable. Sub-search principal overrides, atomic tag
upsert-and-delete in one request, bulk import enforcement, and external
collection refresh enforcement are outside the initial scope.

## Collection Switch

`rls.enabled` can be changed through collection properties. Disabled
collections bypass RLS. Enabling RLS makes the collection deny-by-default and
must not become observable until the RLS metadata state is ready and every
serving Proxy will refresh the collection state before handling later
requests. Disabling RLS removes all policies and tag bindings for the
collection and invalidates Proxy RLS state. These synchronization steps are
performed by Milvus as part of the property transition; users do not manage
Proxy caches directly.

Every row-bearing request on an enabled collection must provide a non-blank
top-level `rls_principal` or request `skip_rls=true`. Sub-searches inherit the
top-level decision. A skip is allowed only when authorization is disabled or
the authenticated Milvus user has `SkipRLS` on the collection.

`rls.force=true` rejects `skip_rls=true` and is meaningful only while RLS is
enabled.

## Principal Tag Bindings

A principal is an application-provided identifier, not a Milvus metadata
entity. Milvus therefore does not create or delete principals. It stores only
optional, collection-scoped tag bindings keyed by
`(collectionID, principalName)`. A principal with no tags remains valid and can
still be used by policies referencing `$current_principal`.

Supported tag APIs:

| API | Behavior |
| --- | --- |
| `set_rls_principal_tags` | Incrementally upsert a non-empty tag map. Supplied keys are added or overwritten; unspecified tags are preserved. |
| `get_rls_principal_tags` | Return the stored tags for one principal identifier. |
| `list_rls_principals` | List principal identifiers that currently have stored tags. |
| `delete_rls_principal_tags` | Delete selected tag keys. No keys means delete all tags for that identifier. |

When deletion leaves no tags, Milvus removes the empty storage record. This is
tag-binding cleanup, not principal deletion. Deleting missing tags succeeds;
repeated keys are deduplicated and limits are enforced.

One request currently performs either incremental tag upserts or deletions,
not both. Applications that need both changes should revoke tags before
granting replacements so partial completion fails closed. A future atomic
patch API may carry both operations.

Policy expressions may reference:

| Variable | Meaning |
| --- | --- |
| `$current_principal` | The request `rls_principal` value. |
| `$current_principal_tags['key']` | The current principal's tag value for `key`. |

Principal names and string tag values are bound as template values rather than
interpolated into expression text, so they do not require an ASCII-only
whitelist. Tag keys cannot contain a single quote because the
`$current_principal_tags['key']` syntax does not define key escaping. Names and
keys must otherwise be non-blank and satisfy their configured byte limits.

If a policy references a missing tag, that policy predicate evaluates to
false.

### JSON Number Semantics

Tag payloads are JSON objects whose values are strings or numbers. Milvus maps
an integral token such as `3` to `int64` when it is in range, and a token with
a decimal point or exponent such as `3.0` or `3e0` to IEEE-754 binary64
(`double`). An integral token outside the int64 range is represented as a
double when it is finite and representable. When Milvus serializes tags again,
it preserves the numeric kind, including emitting an integral double with a
decimal point.

String tags match only string fields. Integer and double tags may match either
numeric field family when conversion preserves the value exactly. An
incompatible, overflowing, or lossy conversion evaluates the predicate to
false; Milvus never coerces between strings and numbers.

The usable boundaries differ: int64 covers `[-2^63, 2^63-1]`, while double has
a wider magnitude range but cannot exactly represent every large integer.
Applications should avoid using extreme JSON numeric values as authorization
sentinels and prefer strings when exact cross-language identity is required.

## Row Policies

Each policy belongs to one collection and has a unique `policy_name` in that
collection. `CreateRowPolicy` rejects an existing name. `UpdateRowPolicy`
replaces the named definition while preserving its internal `policy_id`.
Dropping a missing policy succeeds as an idempotent retry.

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

`Get` is a client-side convenience over `Query`; it is not a separate action.

## Policy Evaluation

RLS is deny-by-default when enabled. At least one applicable permissive policy
is required. Policies are combined as:

```text
(permissive_policy_1 OR permissive_policy_2 OR ...)
AND
(restrictive_policy_1 AND restrictive_policy_2 AND ...)
```

If only restrictive policies apply, the final predicate is false. A field
referenced by a policy cannot be dropped or changed incompatibly until the
policy is removed or updated.

`CreateRowPolicy` rejects a prospective policy set whose combined expression
exceeds `proxy.rls.maxCombinedExpressionLength`. `UpdateRowPolicy` does not use
this admission guard: an update may repair a set that became oversized after a
configuration decrease, and it may also make a previously valid set
oversized. Proxy always checks the complete expression when compiling it, so
row-bearing requests fail with a quota error until the policies or limit are
corrected.

For query, search, and delete, Proxy merges the final `using_expr` into the
request plan with logical AND. For insert and the written side of upsert, Proxy
compiles the restricted `check_expr` into the same plan expression nodes and
evaluates those nodes directly against each input row's `FieldData`; this is a
small RLS evaluator, not a second general SQL engine. Existing rows selected by
upsert must also pass `using_expr`.

Local checks use SQL three-valued logic consistent with Segcore filtering.
Comparisons involving NULL produce UNKNOWN, and only a final TRUE admits a row.

## Expression Support

RLS accepts a deliberately restricted expression subset:

- `true` and `false`;
- equality between a top-level scalar field and a literal or supported
  template value;
- `in` with literal value lists;
- `array_contains`, `array_contains_all`, and `array_contains_any` on primitive
  array fields;
- `$current_principal` as a string template value;
- `$current_principal_tags['key']` as a string, int64, or double template value.

Each `using_expr` or `check_expr` contains one simple predicate. Policy authors
compose predicates through multiple permissive or restrictive policies rather
than inline `and`, `or`, or boolean `not`.

RLS variables follow normal Milvus template syntax: only unquoted variable
tokens become template variables; identical text inside normal or raw string
literals remains literal data.

Unsupported forms include vector and JSON fields, nested or element-level
fields, system fields, ordered and field-to-field comparisons, and dynamic
functions such as `now()`.

## Metadata And Synchronization

RootCoord owns policies and principal tag bindings. Records use globally unique
collection IDs as identity; database and collection names are descriptive.
RootCoord keeps complete policies in a name-keyed collection map, including
their internal IDs.

The initial design assumes policy and tag mutations are low-frequency
control-plane operations. Each mutation uses a CChannel broadcast with the same
`SharedDBName + ExclusiveCollectionName` resources as collection DDL. The
message carries a complete post-image or stable drop identity. Its ACK callback
persists metadata, updates RootCoord state, and invalidates the relevant Proxy
cache; callback failures are retried. This orders mutations with collection
drop and schema changes.

CChannel load is determined by policy and tag update rate, not by the number of
principals used in data requests. Applications should not use tag APIs as a
per-request data path. Supporting high-frequency tag churn and scaling
RootCoord storage or recovery for very large numbers of tag bindings remain
separate work.

Proxy caches policies per collection and tags per
`(collectionID, principalName)`. It does not preload Proxy RLS state. An
RLS-enforced request loads missing state through `GetRLSMetadata`; refresh
failure denies the request. Policy freshness is checked on use. Principal tag
entries expire through a periodic scanner and reload on their next use.

RLS messages are eligible for generic CDC replication and replay the same
idempotent ACK callbacks on a secondary. Dedicated RLS CDC compatibility and
recovery validation remains follow-up work.

## Configuration

| Config | Meaning |
| --- | --- |
| `proxy.rls.maxPoliciesPerCollection` | Maximum policies on one collection. |
| `proxy.rls.maxPrincipalsPerCollection` | Maximum principal identifiers with stored tags on one collection. |
| `proxy.rls.maxTagsPerPrincipal` | Maximum stored tags for one collection-scoped principal identifier. |
| `proxy.rls.maxExpressionLength` | Maximum bytes in one policy expression. |
| `proxy.rls.maxCombinedExpressionLength` | Maximum bytes in one combined expression. |
| `proxy.rls.maxPolicyNameLength` | Maximum policy-name length in bytes. |
| `proxy.rls.maxPolicyDescriptionLength` | Maximum policy-description length in bytes. |
| `proxy.rls.maxPrincipalNameLength` | Maximum principal-name length in bytes. |
| `proxy.rls.maxTagKeyLength` | Maximum tag-key length in bytes. |
| `proxy.rls.maxTagValueLength` | Maximum string tag-value length in bytes. |
| `proxy.rls.maxArrayLiteralElements` | Maximum literal elements in supported array expressions. |
| `proxy.rls.metaRefreshInterval` | Policy freshness interval and principal-tag cache lifetime. |

## Compatibility And Rollout

There is no previously released RLS metadata to migrate. RLS may be enabled
only after all serving Proxy and RootCoord instances understand its API, WAL
messages, dynamic property transition, and cache invalidation contract. A
cluster with enabled collections must not roll back to a version that cannot
enforce RLS.

## Observability And Verification

RLS uses existing request errors, component logs, and broadcaster/WAL
diagnostics. It adds no dedicated metrics initially.

Verification covers policy combination; numeric kind and boundary round trips;
exact and rejected numeric conversions; all enforced operations; bypass
authorization; dynamic enable and disable; restart recovery; cache refresh and
invalidation; collection/schema DDL ordering; metadata cleanup; and concurrent
mutations. Dedicated CDC E2E validation remains follow-up work.

## Alternatives And Follow-ups

RLS uses an explicit application principal rather than the Milvus username,
lazy principal-tag loading rather than collection-wide Proxy snapshots, and
the existing broadcast/ACK path rather than direct catalog mutation.

Follow-ups include an atomic tag patch API, high-frequency tag mutation,
RootCoord tag-storage and lookup scaling, and dedicated CDC validation.
