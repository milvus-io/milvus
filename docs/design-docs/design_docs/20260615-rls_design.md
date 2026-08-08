# Milvus Row-Level Security (RLS) Design Document

| Field | Value |
| --- | --- |
| Title | Row-Level Security (RLS) |
| Status | Draft |
| Author | James Luan (@xiaofan-luan) Buqian Zheng (@zhengbuqian) |
| Initial Draft | 2025-04-10 |
| Consolidated | 2026-06-15 |

## 1. Summary
Row-Level Security (RLS) provides fine-grained row-level access control for Milvus collections. After RLS is enabled on a collection, Milvus automatically limits which rows a request can `query`, `search`, `hybrid_search`, `delete`, `insert`, or `upsert` based on the request's RLS principal, RLS principal tags, and administrator-defined row policies.
RLS moves user-owned data isolation and department-scoped data isolation from application code into the database layer. This avoids data leaks caused by application paths that forget to add ownership or department filters.
RLS is enforced in Proxy:

- For read-like paths (`query`, `search`, `hybrid_search`, `delete`), Proxy injects additional filter expressions before request execution.
- For write-like paths (`insert`, `upsert`), Proxy validates incoming rows against RLS expressions before data is written.
- DDL, admin, metadata, bulk/indirect writes, aggregate/statistics, and collection-level operations are outside the RLS enforcement scope. They remain controlled by Milvus RBAC and existing Milvus request validation.

Core principles:

1. RLS is disabled by default and can be enabled per collection through collection properties.
2. When enabled, RLS is fail-closed for row-bearing operations: no applicable policy means denied access.
3. Policies support PostgreSQL-style `PERMISSIVE` and `RESTRICTIVE` combination semantics.
4. Each policy has an `expr` and a list of `actions`, the principal may perform the `actions` only on rows admitted by `expr`.
5. Principals may include tags.
6. RLS management operations require `CollectionReadWrite` or `CollectionAdmin` privilege on the target collection, but do not require an RLS principal.

> #### RBAC and RLS
> Milvus RBAC and RLS solve different authorization problems:

| Layer | Identity | Scope | Example question |
| --- | --- | --- | --- |
| Milvus RBAC | Milvus connection credential, roles, and grants | Cluster, database, collection, partition, index, resource, RBAC, and other Milvus API privileges | Can this Milvus client call `CreateIndex` on this collection? |
| RLS | Application end-user RLS principal and RLS tags | Rows inside one RLS-enabled collection for row-bearing operations | Which rows can end user `alice` search, query, delete, insert, or upsert? |
>
> RBAC is evaluated before the request is allowed to reach the operation. RLS is evaluated only after RBAC allows a row-bearing operation on an RLS-enabled collection.
>
> This separation lets an application connect to Milvus with a service credential while passing an RLS principal such as `alice`, `bob`, or `support_admin` for row-level enforcement. The service credential is responsible for Milvus API privileges. The RLS principal is responsible for row visibility and write validation.

## 2. Main User Interface
RLS exposes a small user-facing surface:

1. Enable RLS on a collection.
2. Create collection-scoped RLS principals.
3. Create row policies on the collection.
4. Send row-bearing requests with an explicit RLS principal.

Enable RLS:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "false",
    },
)
```

Create collection-scoped RLS principals with tags:
```python
client.set_rls_principal_tags(
    collection_name="documents",
    principal_name="alice",
    tags={"department": "engineering"},
)
```

Create a row policy:
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_own_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    expr="owner == $current_principal",
)
```

Run a row-bearing request with an RLS principal:
```python
client.query(
    collection_name="documents",
    filter="title like 'RLS%'",
    rls_principal="alice",
)
```

RLS applies only to row-bearing operations on an RLS-enabled collection:

| Operation category | Examples | RLS behavior |
| --- | --- | --- |
| Read existing rows | `query`, `search`, `hybrid_search` | Evaluate `expr` against existing rows |
| Delete existing rows | `delete` | Evaluate `expr` against existing rows |
| Write incoming rows | `insert`, `upsert` | Evaluate `expr` against incoming rows |

DDL, admin, metadata, bulk/indirect writes, aggregate/statistics, and collection-level operations are outside the RLS enforcement scope and remain controlled by Milvus RBAC and existing Milvus request validation.

## 3. Detailed API Definition
This section describes the public configuration, SDK options, and user-visible API semantics. Internal RPC, protobuf, and storage details are covered in the architecture section.

### Authorization, RBAC, and RLS Identity

#### Authorization Requirement
RLS depends on Milvus authorization, but RLS identity is separate from Milvus connection identity. A cluster must enable authorization before RLS can take effect.
```yaml
common:
  security:
    authorizationEnabled: true # Required before any collection can enable RLS.

proxy:
  rls:
    auditEnabled: true              # Emit audit logs for RLS management and enforcement decisions.
    cacheExpirationSeconds: 3600    # TTL for cached RLS policy and principal metadata in Proxy.
    maxCacheEntries: 10000          # Maximum number of cached RLS metadata entries per Proxy.
    maxPoliciesPerCollection: 100   # Maximum number of row policies allowed on one collection.
    maxTagsPerPrincipal: 50         # Maximum number of tags allowed on one collection-scoped RLS principal.
    maxExpressionLength: 4096       # Maximum length of policy expr.
```

Semantics:

- `common.security.authorizationEnabled=false` means RLS cannot be enabled for any collection.
- Milvus authorization authenticates and authorizes the Milvus connection credential.
- The RLS principal is an application end-user identity supplied by a trusted Milvus client or service.
- Enabling authorization does not enable RLS for every collection automatically.
- RLS is enabled explicitly at collection level.
- RLS management APIs are protected by collection-scoped Milvus RBAC privileges. Runtime RLS policy evaluation uses the RLS principal, not the Milvus connection user.

#### RLS Principal
An RLS principal is a collection-scoped application end-user identity used for RLS policy evaluation.

An RLS principal has:

1. A principal name, exposed to expressions as `$current_principal`.
2. Zero or more collection-scoped RLS tags, exposed to expressions as `$current_principal_tags['key']`.
3. An optional collection-scoped RLS admin marker.

The principal name, tags, and admin marker belong to one collection's RLS metadata. Application membership and similar application concepts should be represented as collection-scoped tags.

RLS admin principals are an RLS concept, not a synonym for Milvus `root` or the Milvus `admin` RBAC role. RLS admin status is scoped to one collection. When `rls.force=false`, an RLS admin principal bypasses RLS policies for row-bearing operations on that collection. When `rls.force=true`, even an RLS admin principal must satisfy applicable RLS policies.

#### Collection-Level RLS Properties
RLS is controlled by collection properties, not by separate enable/disable APIs. A Milvus RBAC user with `CollectionReadWrite` or `CollectionAdmin` privilege enables, disables, or forces RLS by updating collection properties.
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "false",
    },
)
```

Collection-level properties:

| Property | Default | Meaning |
| --- | --- | --- |
| `rls.enabled` | `false` | Whether RLS is enabled for this collection |
| `rls.force` | `false` | Whether RLS admin principals must also follow RLS policies |

Enable RLS for normal RLS principals while RLS admin principals still bypass RLS:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "false",
    },
)
```

Enable FORCE RLS so every RLS principal, including RLS admin principals, must satisfy applicable RLS policies:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "true",
    },
)
```

Disable RLS for the collection:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "false",
    },
)
```

Semantics:

- RLS is disabled by default for every collection.
- RLS configuration is scoped to one collection.
- Policies may exist on a collection before RLS is enabled, but they do not affect requests until `rls.enabled=true`.
- When `rls.enabled=true`, row-bearing operations must include an explicit RLS principal.
- If the RLS principal is an RLS admin principal and `rls.force=false`, policies are bypassed for that collection.
- Non-admin RLS principals must be admitted by applicable policies for the requested action.
- If no applicable policy expression exists for the action, access is denied.
- RLS admin principals bypass RLS by default when `rls.force=false`.
- When `rls.force=true`, RLS admin principals must also be admitted by applicable policies for the requested action.
- When `rls.enabled=false`, RLS does not change `query`, `search`, `hybrid_search`, `delete`, `insert`, or `upsert` behavior for that collection, regardless of `rls.force`.

#### Enforcement Scope
RLS applies only to row-bearing operations on an RLS-enabled collection:

| Operation category | Examples | RLS behavior |
| --- | --- | --- |
| Read existing rows | `query`, `search`, `hybrid_search` | Evaluate `expr` against existing rows |
| Delete existing rows | `delete` | Evaluate `expr` against existing rows |
| Write incoming rows | `insert`, `upsert` | Evaluate `expr` against incoming rows |

The following operations are outside RLS enforcement scope and do not require an RLS principal:

| Operation category | Examples | Authorization model |
| --- | --- | --- |
| DDL and admin operations | `create_collection`, `drop_collection`, `add_collection_field`, `alter_collection_schema`, `create_index`, `drop_index`, `create_partition`, `drop_partition`, alias operations, load/release, compaction, resource group operations | Milvus RBAC and existing Milvus checks |
| Bulk or indirect writes | `import`, `ImportV2`, external collection refresh | Milvus RBAC and existing Milvus checks |
| Aggregate and statistics operations | `get_statistics`, `get_collection_statistics`, `get_partition_statistics`, load/index progress APIs | Milvus RBAC and existing Milvus checks |
| Collection-level destructive operations | `truncate_collection`, `drop_collection`, `drop_partition` | Milvus RBAC and existing Milvus checks |

These out-of-scope operations may affect the collection as a whole, but they are not row-level policy decisions. Applications that expose those operations to end users should protect them at the application layer or through Milvus RBAC.

### Row Policy API

#### Policy Fields
The public row policy API exposes the following fields:

| Field | Description |
| --- | --- |
| `collection_name` | Target collection. A row policy belongs to exactly one collection |
| `policy_name` | Human-readable policy name, unique within a collection |
| `policy_type` | `PERMISSIVE` or `RESTRICTIVE` |
| `actions` | Operations this policy applies to |
| `expr` | Row policy expression evaluated for all listed actions |
| `description` | Optional description |

#### Policy Applicability

Example policy that applies to the current action and filters rows by the current principal:
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_shared_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    expr="array_contains(shared_users, $current_principal)",
)
```

Example policy that uses a tag to grant department-based access:
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_department_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    expr="department == $current_principal_tags['department']",
)
```

If an RLS principal does not have a `department` tag, the second policy evaluates to false for that principal. If no applicable policy expression admits rows for the action, RLS denies access.

#### Supported Actions
RLS supports the following actions:
```plaintext
query
search
hybrid_search
delete
insert
upsert
```

For `query`, `search`, `hybrid_search`, and `delete`, `expr` is evaluated against existing rows.
For `insert` and `upsert`, `expr` is evaluated against incoming rows.

#### Policy Types

##### PERMISSIVE
Multiple permissive policies are combined with `OR`:
```plaintext
department == 'engineering'
OR
visibility == 'public'
```

A row passes the permissive part if it satisfies at least one permissive policy.

##### RESTRICTIVE
Multiple restrictive policies are combined with `AND`:
```plaintext
status != 'archived'
AND
region in ['us', 'eu']
```

A row passes the restrictive part only if it satisfies all restrictive policies.

##### Combined Semantics
The final RLS expression is:
```plaintext
(permissive_1 OR permissive_2 OR ...)
AND
(restrictive_1)
AND
(restrictive_2)
...
```

If the user request also contains a filter, the final execution expression is:
```plaintext
(user_filter) AND (rls_filter)
```

If no applicable policy exists, the RLS expression is `false`, which denies all rows.

### Expression Semantics
Row policies expose one public `expr` field. The requested action decides which row version the expression is evaluated against.

For existing-row actions, `expr` limits which stored rows an RLS principal can read or delete:
```plaintext
query
search
hybrid_search
delete
```

Example:
```plaintext
department == $current_principal_tags['department']
```

If the user request filter is:
```plaintext
category == 'report'
```

and the current RLS principal's `department` tag is `engineering`, Proxy injects:
```plaintext
(category == 'report') AND (department == 'engineering')
```

`delete` evaluates existing rows because it chooses which stored rows can be deleted. It does not validate newly inserted row values.

For `hybrid_search`, Proxy applies the RLS filter to each sub-search request.

For incoming-row actions, `expr` validates each proposed row before it is written:
```plaintext
insert
upsert
```

Example:
```plaintext
department == $current_principal_tags['department']
```

If the current RLS principal's tags are:
```json
{
  "department": "engineering"
}
```

then every inserted or upserted row must satisfy:
```plaintext
department == 'engineering'
```

If read and write actions need different semantics, define separate policies with different `actions` and `expr` values.

### Variables
RLS expressions support variable substitution.

#### `$current_principal`
`$current_principal` represents the current RLS principal name, not the Milvus connection username.
Example:
```plaintext
owner == $current_principal
```

If the current RLS principal is `alice`, the expression becomes:
```plaintext
owner == 'alice'
```

#### `$current_principal_tags['key']`
`$current_principal_tags['key']` represents a tag value attached to the current RLS principal.
Example:
```plaintext
department == $current_principal_tags['department']
```

If the current RLS principal's tags are:
```json
{
  "department": "engineering"
}
```

then the expression becomes:
```plaintext
department == 'engineering'
```

#### Missing Tags
If an expression references a missing tag, expression construction must fail closed.
Example expression:
```plaintext
department == $current_principal_tags['department']
```

If the current RLS principal has no `department` tag, the expression becomes:
```plaintext
false
```

### Supported Operators
The first version supports only the following expression operators:

| Operator | Field type | Operand | Meaning |
| --- | --- | --- | --- |
| `==` | Scalar field | Single literal or variable | Field equals operand |
| `in` | Scalar field | Array literal | Field value is in the literal list |
| `array_contains` | Array field | Single literal or variable | Array field contains the operand |
| `array_contains_all` | Array field | Array literal | Array field contains every literal in the operand |
| `array_contains_any` | Array field | Array literal | Array field contains at least one literal in the operand |

### Public Management APIs
RLS management APIs are collection-scoped Milvus management operations. They require the Milvus connection user to have `CollectionReadWrite` or `CollectionAdmin` privilege on the target collection, and they do not require an RLS principal in the request.

Row-bearing runtime APIs on an RLS-enabled collection must carry an explicit RLS principal. The exact SDK transport can be request options, client context, or request metadata, but the request must provide enough information for Proxy to resolve the current RLS principal name, tags, and admin marker.

#### Create Policy
Go client option:
```go
client.NewCreateRowPolicyOption(
    "documents",
    "department_access",
    []entity.RowPolicyAction{
        entity.RowPolicyActionQuery,
        entity.RowPolicyActionSearch,
        entity.RowPolicyActionDelete,
        entity.RowPolicyActionInsert,
        entity.RowPolicyActionUpsert,
    },
    "department == $current_principal_tags['department']",
).WithDescription(
    "department access policy",
)
```

#### Drop Policy
```go
client.DropRowPolicy(
    ctx,
    client.NewDropRowPolicyOption("documents", "department_access"),
)
```

#### List Policies
```go
policies, err := client.ListRowPolicies(
    ctx,
    client.NewListRowPoliciesOption("documents"),
)
```

#### Manage RLS Principal Tags
RLS principal tag APIs manage metadata for one RLS principal in one collection. The `alice` argument below is an RLS principal name, not necessarily a Milvus RBAC username.
```go
client.SetRLSPrincipalTags(
    ctx,
    client.NewSetRLSPrincipalTagsOption(
        "documents",
        "alice",
        map[string]string{
            "department": "engineering",
        },
    ),
)

tags, err := client.GetRLSPrincipalTags(
    ctx,
    client.NewGetRLSPrincipalTagsOption("documents", "alice"),
)

client.DeleteRLSPrincipalTags(
    ctx,
    client.NewDeleteRLSPrincipalTagsOption("documents", "alice", "department"),
)
```

#### Manage RLS Principal Admin Status
RLS admin status is collection-scoped RLS principal metadata. An RLS admin principal bypasses row policies for that collection when `rls.force=false`; it is still subject to row policies when `rls.force=true`.

The API should provide a collection-scoped management operation to mark or unmark an RLS principal as an RLS admin principal. This does not grant any Milvus RBAC privilege.

Conceptual API:
```go
client.SetRLSPrincipalAdmin(
    ctx,
    client.NewSetRLSPrincipalAdminOption("documents", "support_admin", true),
)
```

#### Manage Collection RLS Properties
```go
client.AlterCollectionProperties(
    ctx,
    client.NewAlterCollectionPropertiesOption(
        "documents",
        map[string]string{
            "rls.enabled": "true",
            "rls.force": "false",
        },
    ),
)
```

#### Runtime Request Principal
Runtime row-bearing requests must provide an RLS principal when RLS is enabled for the target collection:
```go
client.Search(
    ctx,
    client.NewSearchOption("documents", limit, vectors).
        WithRLSPrincipal("alice"),
)
```

If the collection has `rls.enabled=true` and the request does not include an RLS principal, Proxy rejects the request before execution.
SDKs should use explicit names such as `rls_principal` and `WithRLSPrincipal` instead of generic `principal`, so the application end-user identity is not confused with the Milvus connection credential.

### End-to-End Example: Personal Documents and Sharing
This quick start shows how to use RLS to build a document collection where:

1. Each user can read their own documents.
2. Each user can insert, update, and delete only documents they own.
3. A user can share their documents with specific users.
4. A user can share their documents with an entire department.
5. Shared users or departments can read shared documents, but cannot modify or delete them.

#### Create a Document Collection
Assume a collection named `documents` with the following fields:
```plaintext
id: int64 primary key
department: varchar
owner: varchar
shared_users: array<varchar>
shared_departments: array<varchar>
title: varchar
content: varchar
embedding: float_vector
```

Example document owned by Alice and shared with Bob:
```json
{
  "id": 1,
  "owner": "alice",
  "department": "engineering",
  "shared_users": ["bob"],
  "shared_departments": [],
  "title": "RLS Design Notes",
  "content": "...",
  "embedding": []
}
```

Example document owned by Alice and shared with the Product department:
```json
{
  "id": 2,
  "owner": "alice",
  "department": "engineering",
  "shared_users": [],
  "shared_departments": ["product"],
  "title": "Product Plan",
  "content": "...",
  "embedding": []
}
```

#### Assign RLS Principal Tags
RLS can use collection-scoped RLS principal tags to express dynamic policies.
```python
client.set_rls_principal_tags(
    collection_name="documents",
    principal_name="alice",
    tags={"department": "engineering"},
)

client.set_rls_principal_tags(
    collection_name="documents",
    principal_name="bob",
    tags={"department": "engineering"},
)

client.set_rls_principal_tags(
    collection_name="documents",
    principal_name="carol",
    tags={"department": "product"},
)
```

#### Enable RLS on the Collection
RLS is controlled by collection properties. A Milvus user with `CollectionReadWrite` or `CollectionAdmin` privilege enables RLS by updating the target collection:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "false",
    },
)
```

#### Allow Users to Read Their Own Documents
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_own_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    expr="owner == $current_principal",
)
```

If Alice runs:
```python
client.query(
    collection_name="documents",
    filter="title like 'RLS%'",
    rls_principal="alice",
)
```

Milvus executes:
```plaintext
(title like 'RLS%') AND (owner == 'alice')
```

Alice only sees documents she owns.

#### Allow Users to Read Documents Shared Directly With Them
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_user_shared_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    expr="array_contains(shared_users, $current_principal)",
)
```

Because `read_own_documents` and `read_user_shared_documents` are both `PERMISSIVE`, they are combined with `OR`.
For Bob, the effective read expression becomes:
```plaintext
(owner == 'bob') OR array_contains(shared_users, 'bob')
```

Bob can read documents he owns and documents explicitly shared with him.

#### Allow Users to Read Documents Shared With Their Department
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_department_shared_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    expr="array_contains(shared_departments, $current_principal_tags['department'])",
)
```

For Carol, whose department tag is `product`, the effective read expression becomes:
```plaintext
(owner == 'carol')
OR
array_contains(shared_users, 'carol')
OR
array_contains(shared_departments, 'product')
```

Carol can read documents she owns, documents shared directly with her, and documents shared with the Product department.

#### Allow Users to Insert Only Their Own Documents
Reading shared documents should not imply write permission. For `insert`, use a write policy whose `expr` ensures every inserted row is owned by the current RLS principal.
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="insert_own_documents",
    policy_type="PERMISSIVE",
    actions=["insert"],
    expr="owner == $current_principal",
)
```

Alice can insert:
```python
client.insert(
    collection_name="documents",
    rls_principal="alice",
    data=[
        {
            "id": 100,
            "owner": "alice",
            "department": "engineering",
            "shared_users": [],
            "shared_departments": [],
            "title": "Alice Private Doc",
            "content": "...",
            "embedding": [],
        }
    ],
)
```

Alice cannot insert a document owned by Bob:
```python
client.insert(
    collection_name="documents",
    rls_principal="alice",
    data=[
        {
            "id": 101,
            "owner": "bob",
            "department": "engineering",
            "shared_users": [],
            "shared_departments": [],
            "title": "Fake Bob Doc",
            "content": "...",
            "embedding": [],
        }
    ],
)
```

The request is rejected because the row violates:
```plaintext
owner == 'alice'
```

Example error:
```plaintext
row 0 violates RLS expression: field 'owner' value 'bob' not allowed by policy
```

#### Allow Users to Upsert Only Their Own Documents
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="upsert_own_documents",
    policy_type="PERMISSIVE",
    actions=["upsert"],
    expr="owner == $current_principal",
)
```

This allows Alice to update her own document, including its sharing fields:
```python
client.upsert(
    collection_name="documents",
    rls_principal="alice",
    data=[
        {
            "id": 100,
            "owner": "alice",
            "department": "engineering",
            "shared_users": ["bob"],
            "shared_departments": [],
            "title": "Alice Shared Doc",
            "content": "...",
            "embedding": [],
        }
    ],
)
```

After this, Bob can read document `100` because:
```plaintext
array_contains(shared_users, 'bob')
```

Alice can also share the same document with an entire department:
```python
client.upsert(
    collection_name="documents",
    rls_principal="alice",
    data=[
        {
            "id": 100,
            "owner": "alice",
            "department": "engineering",
            "shared_users": ["bob"],
            "shared_departments": ["product"],
            "title": "Alice Shared Doc",
            "content": "...",
            "embedding": [],
        }
    ],
)
```

After this, all users whose `department` tag is `product` can read the document.
For the first version, RLS validates the incoming row for `upsert`. If an application allows user-controlled primary keys, it should also ensure one user cannot upsert over another user's existing primary key. A common approach is to use server-generated document IDs or include ownership in the application's ID allocation logic. Future versions may add stronger existing-row validation for upsert.

#### Allow Users to Delete Only Their Own Documents
Delete should not use the same expression as read sharing. Otherwise, users could delete documents merely shared with them.
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="delete_own_documents",
    policy_type="PERMISSIVE",
    actions=["delete"],
    expr="owner == $current_principal",
)
```

Alice can delete her own documents:
```python
client.delete(
    collection_name="documents",
    filter="id == 100",
    rls_principal="alice",
)
```

Milvus executes:
```plaintext
(id == 100) AND (owner == 'alice')
```

Bob cannot delete Alice's document even if Alice shared it with Bob, because delete does not use the sharing policies.

#### Final Effective Behavior
For `query` and `search`, the effective RLS expression is:
```plaintext
owner == $current_principal
OR
array_contains(shared_users, $current_principal)
OR
array_contains(shared_departments, $current_principal_tags['department'])
```

For `insert` and `upsert`, the effective RLS expression is:
```plaintext
owner == $current_principal
```

For `delete`, the effective RLS expression is:
```plaintext
owner == $current_principal
```


| Operation | Allowed rows |
| --- | --- |
| `query` | Own documents, directly shared documents, and department-shared documents |
| `search` | Own documents, directly shared documents, and department-shared documents |
| `insert` | Only incoming rows where `owner == current RLS principal` |
| `upsert` | Only incoming rows where `owner == current RLS principal` |
| `delete` | Only existing rows where `owner == current RLS principal` |

If shared users or shared departments must also write documents, the schema should separate read ACLs from write ACLs, for example `shared_read_users`, `shared_write_users`, `shared_read_departments`, and `shared_write_departments`. The read and write policies should then be defined separately. The first version's upsert validation only checks the incoming row, so secure shared-write semantics over existing documents need additional existing-row validation or application-level ID ownership guarantees.

## 4. Architecture

### Component Architecture

#### High-Level Components
```plaintext
Client
  |
  v
Proxy
  |
  +-- RLS RPC handlers
  |
  +-- RLSManager
       |
       +-- RLSCache
       +-- RLSCacheWithLoader
       +-- RLSContextProvider
       +-- RLSExpressionBuilder
       +-- RLSQueryInterceptor
       +-- RLSSearchInterceptor
       +-- RLSDeleteInterceptor
       +-- RLSInsertInterceptor
       +-- RLSUpsertInterceptor
       +-- RLSWriteExpressionValidator
  |
  v
Task Layer
  |
  +-- query task
  +-- search task
  +-- hybrid search task
  +-- delete task
  +-- insert task
  +-- upsert task
  |
  v
QueryNode / DataNode

RootCoord
  |
  +-- RLS DDL tasks
  +-- policy ID allocation
  +-- metastore persistence
  +-- cache refresh broadcast

MetaStore
  |
  +-- collection meta, including collection-level RLS config
  +-- rls/policy/{dbID}/{collectionID}/{policyID}
  +-- rls/principal/{dbID}/{collectionID}/{principalName}
```

#### Why Enforce in Proxy
RLS is enforced in Proxy because:

1. Proxy is the user request entry point and can access the RLS principal context.
2. Query/search/hybrid_search/delete requests still have original filters at the Proxy layer.
3. Insert/upsert row data is available at the Proxy layer for schema validation and per-row validation.
4. QueryNode and DataNode execution engines do not need to be changed.
5. RLS enforcement stays close to existing authorization checks.

Tradeoffs:

1. QueryNode and DataNode do not independently know RLS policy state.
2. Every relevant operation must go through Proxy hooks.
3. Proxy cache must fail closed and must not bypass RLS when RootCoord is temporarily unavailable.

### Request Flow

#### Create Policy Flow
```plaintext
Client
  -> Proxy.CreateRowPolicy
  -> require CollectionReadWrite or CollectionAdmin privilege on the target collection
  -> RootCoord.CreateRowPolicy
  -> RootCoord DDL task
  -> allocate policyID
  -> validate policy
  -> persist to metastore
  -> broadcast RLS cache refresh
  -> return success
```

#### Query Flow
```plaintext
Proxy query task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> resolve RLS principal
  -> check RLS admin principal bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure RLS principal tags loaded
  -> RLSQueryInterceptor.InterceptQuery
  -> RLSExpressionBuilder.BuildExpression(action=query)
  -> merge user filter and RLS filter
  -> continue query plan generation
```

#### Search Flow
```plaintext
Proxy search task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> resolve RLS principal
  -> check RLS admin principal bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure RLS principal tags loaded
  -> RLSSearchInterceptor.InterceptSearch
  -> RLSExpressionBuilder.BuildExpression(action=search)
  -> merge search filter and RLS filter
```

Hybrid search applies the RLS filter to each sub-request independently.

#### Delete Flow
```plaintext
Proxy delete task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> resolve RLS principal
  -> check RLS admin principal bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure RLS principal tags loaded
  -> RLSDeleteInterceptor.InterceptDelete
  -> RLSExpressionBuilder.BuildExpression(action=delete)
  -> merge delete filter and RLS filter
  -> continue delete execution
```

#### Insert Flow
Insert has two validation stages.
Stage 1: collection-level and policy gate:
```plaintext
insert task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> resolve RLS principal
  -> check RLS admin principal bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure RLS principal tags loaded
  -> RLSInsertInterceptor.InterceptInsert
  -> build action=insert RLS expression
  -> if no applicable policy expression exists: reject request
  -> save write expression
```

Stage 2: per-row expression validation:
```plaintext
insert task
  -> normal schema/data validation
  -> validateInsertAgainstRLSExpression(expr, fieldsData)
  -> parse RLS expression
  -> find target field
  -> validate each row
  -> any violation rejects whole batch
```

#### Upsert Flow
Upsert follows the same structure as insert:
```plaintext
upsert task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> resolve RLS principal
  -> check RLS admin principal bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure RLS principal tags loaded
  -> build action=upsert RLS expression
  -> save write expression
  -> validate incoming rows before execution
```

The first version validates the incoming row. It does not validate the previous stored row that may be overwritten by the upsert.

### Expression Builder

#### Applicable Policy Selection
A policy participates in expression construction only when both conditions are true:

1. The policy `actions` contains the current action.
2. The policy has a non-empty expression for that action.

#### No Policy Semantics
If a collection has no policies:
```plaintext
false
```

If a collection has policies but none are applicable to the current action:
```plaintext
false
```

This means deny all.

#### Expression Merge
If the user request filter is empty:
```plaintext
rls_expr
```

If the RLS expression is empty or cannot be generated:
```plaintext
false
```

If both user filter and RLS expression exist:
```plaintext
(user_filter) AND (rls_expr)
```

#### PostgreSQL-Style Policy Combination
```plaintext
Final RLS Filter = (Permissive_1 OR Permissive_2 OR ...)
                   AND
                   (Restrictive_1)
                   AND
                   (Restrictive_2)
                   AND ...
```

Then merge with user filter:
```plaintext
Final Execution Filter = (User Filter) AND (Final RLS Filter)
```

### Incoming-Row Expression Validator

#### Supported Incoming-Row Expressions
The first version supports simple per-row expressions:
```plaintext
field == 'string'
field == 123
field in ['a', 'b', 'c']
field in [1, 2, 3]
array_contains(array_field, 'a')
array_contains_all(array_field, ['a', 'b'])
array_contains_any(array_field, ['a', 'b'])
```

#### Unsupported Incoming-Row Expressions
The first version does not support these expressions for per-row insert/upsert validation:
```plaintext
department == 'engineering' AND region == 'us'
department == 'engineering' OR visibility == 'public'
score > 10
field != 'x'
nested function calls
subqueries
```

Unsupported expressions must return clear errors and must not bypass validation.
These expressions are not supported in v1 because read-path filters can be delegated to the existing Milvus filter execution engine, but write-path validation runs in Proxy before rows are written. Proxy needs a row evaluator that can parse the expression, bind one incoming row at a time, and evaluate the expression safely. The first version intentionally supports only `==`, `in`, and `array_contains*` so insert/upsert validation can be implemented safely and fail closed without building a full expression interpreter in Proxy.

#### Validation Behavior
Validation flow:
```plaintext
parse RLS expression
  -> extract field name
  -> extract operator
  -> extract allowed values
  -> locate field data by field name
  -> validate all row values
  -> first violation returns error
```

If the expression field does not exist:
```plaintext
RLS expression field 'department' not found in insert data
```

If the field type is incompatible with the operator:
```plaintext
RLS expression field 'department' is not compatible with the operator
```

If a row does not satisfy the expression:
```plaintext
row 2 violates RLS expression: field 'department' value 'sales' not allowed by policy
```

### Cache Design

#### Cache Types
Proxy has two RLS cache layers:

1. `RLSCache`

  - Stores collection policies.
  - Stores collection meta RLS config.
  - Stores RLS principal metadata, including tags and admin marker.

2. `RLSCacheWithLoader`

  - Loads data from RootCoord on cache miss.
  - Uses TTL for expiration.
  - Uses double-checked locking to avoid thundering herd.

#### Policy Load
When a request enters Proxy, Proxy calls:
```plaintext
EnsurePoliciesLoaded
```

If the cache does not contain policies for the current collection, Proxy loads policies through RootCoord:
```plaintext
ListRowPolicies
```

#### RLS Principal Metadata Load
When a request enters Proxy, Proxy calls:
```plaintext
EnsureRLSPrincipalLoaded
```

If the cache does not contain metadata for the current RLS principal, Proxy loads principal metadata through RootCoord:
```plaintext
GetRLSPrincipal(collection, principal)
```

#### Load Failure
RLS is a security feature. Policy load failure must not bypass enforcement.
Current design:
```plaintext
cache miss
  -> load from RootCoord
  -> retry
  -> still failed
  -> panic
  -> proxy restart
```

If Proxy cannot load policies for a collection with RLS enabled, continuing to serve requests may cause unauthorized access. Failing closed is required.

#### Cache Refresh
RootCoord broadcasts cache refresh events when any of the following changes:

1. Policy created.
2. Policy dropped.
3. RLS principal tags updated.
4. RLS principal tags deleted.
5. Collection RLS properties changed.
6. RLS principal admin status changed.

### Storage Design

#### Policy Storage
Policy storage key:
```plaintext
rls/policy/{dbID}/{collectionID}/{policyID}
```

The value is protobuf-serialized `RLSPolicy`.
Use `policyID` instead of `policy_name` in the storage key because:

1. It is consistent with other Milvus metadata objects.
2. It supports future `ALTER POLICY RENAME`.
3. It avoids key migration when a policy name changes.
4. Policy name uniqueness is only required within a collection.

#### RLS Principal Storage
RLS principal storage key:
```plaintext
rls/principal/{dbID}/{collectionID}/{principalName}
```

The value is protobuf-serialized RLS principal metadata, including tags and admin marker.

#### Collection-Level RLS Config Storage
Collection-level RLS config is stored together with collection metadata, not as a separate RLS metastore key.
Collection meta should contain fields similar to:
```plaintext
rls_enabled: bool
force_rls: bool
```

Semantics:

- `rls_enabled=false` means RLS is disabled for this collection.
- `rls_enabled=true` means row-bearing requests are enforced by policies on this collection.
- `force_rls=false` means RLS admin principals bypass RLS by default.
- `force_rls=true` means RLS admin principals are also enforced by RLS.

Storing the switch in collection meta keeps RLS enable/disable consistent with other collection-level properties and avoids maintaining a separate collection config object.

## 5. Remaining Details

### Security Model

#### Collection-Scoped Management
The following operations require the Milvus connection user to have `CollectionReadWrite` or `CollectionAdmin` privilege on the target collection. They are RLS management operations, not row-bearing runtime operations, so they do not require an RLS principal:
```plaintext
CreateRowPolicy
DropRowPolicy
ListRowPolicies
SetRLSPrincipalTags
GetRLSPrincipalTags
DeleteRLSPrincipalTags
SetRLSPrincipalAdmin
AlterCollectionProperties for rls.enabled / rls.force
```

#### Runtime Enforcement
RLS principals are enforced by RLS for these row-bearing operations when `rls.enabled=true`:
```plaintext
query
search
hybrid_search
delete
insert
upsert
```

DDL/admin operations, bulk/indirect writes, aggregate/statistics APIs, and collection-level operations are outside RLS enforcement scope.

#### Bypass and FORCE RLS
RLS bypass is based on RLS principal metadata, not Milvus RBAC identity.

An RLS admin principal bypasses RLS by default when `rls.force=false`.

FORCE RLS is a collection-level switch. When `rls.force=true`, RLS admin principals no longer bypass RLS for that collection and must be admitted by applicable policies for the requested action.

#### Fail-Closed Rules
When RLS is enabled for a row-bearing operation, all of the following deny access:

| Scenario | Behavior |
| --- | --- |
| No RLS principal context | Deny |
| No policy | Deny |
| No applicable policy | Deny |
| Expression construction fails | Deny |
| Referenced RLS principal tag is missing | Deny |
| Expression operator is unsupported | Deny |
| Referenced expression field is missing | Deny |
| Policy load fails | Panic/restart after retries |
| RootCoord unavailable and cache miss occurs | Panic/restart after retries |

Query/search/hybrid_search/delete paths deny by returning a `false` filter or an error.
Insert/upsert paths deny by returning an error.
Cache load paths deny by panicking after retry exhaustion so the proxy restarts instead of serving without enforcement.

#### Injection Prevention
Variable substitution must not allow policy expression injection.
Rules:

- RLS principal tag keys and values are validated.
- String values are escaped before substitution.
- Missing RLS principal tags become `false` rather than empty strings.
- RLS expressions are parsed and validated before use.
- RLS management APIs require collection-scoped Milvus RBAC management privileges.

### Error Handling

#### User-Facing Errors
No applicable insert policy:
```plaintext
insert operation denied by RLS: no applicable policies
```

RLS expression violation:
```plaintext
row 1 violates RLS expression: field 'department' value 'sales' not allowed by policy
```

Missing expression field:
```plaintext
RLS expression field 'department' not found in insert data
```

Unsupported expression:
```plaintext
compound RLS expressions are not supported for per-row insert validation
```

Policy load failure:
```plaintext
RLS policy load failed after retries for collection db/collection.
Proxy cannot serve requests without RLS enforcement.
```

#### Internal Error Principle
RLS errors must not be swallowed.
Read paths:
```plaintext
error -> return error or false expression
```

Write paths:
```plaintext
error -> reject insert/upsert
```

Cache load path:
```plaintext
error after retry -> panic
```

### Observability

#### Logs
Log the following events:

1. Policy created or dropped.
2. RLS principal metadata updated or deleted.
3. RLS filter injection failure.
4. RLS write expression validation failure.
5. Cache load failure.
6. RLS admin principal bypass.
7. Cache refresh.
8. FORCE RLS enforcement for RLS admin principals.

Logs should include:
```plaintext
rls_principal
db
collection
collectionID
action
policy names
error
requestID / traceID
```

#### Metrics
Recommended metrics:
```plaintext
rls_cache_hit_total
rls_cache_miss_total
rls_policy_load_failure_total
rls_active_policy_count
rls_expression_build_latency
rls_write_expression_validation_failure_total
rls_denied_total
rls_bypass_total
rls_force_enforced_total
```

### Limitations
First version limitations:

1. RLS expressions only support `==`, `in`, `array_contains`, `array_contains_all`, and `array_contains_any`.
2. RLS expressions do not support `AND` / `OR`.
3. `ALTER POLICY` is not supported; use drop and create.
4. Principal grouping is modeled with tags; there is no separate principal-group API.
5. Policy simulator/explain API is not supported.
6. Cross-collection policies are not supported.
7. RLS cache has a short eventual-consistency window before cache refresh reaches every proxy.
8. Upsert validation checks the incoming row but does not validate the previous existing row that may be overwritten.

### Future Work

#### Complex Expression Evaluator
Support expressions such as:
```plaintext
department == 'engineering' AND region == 'us'
visibility == 'public' OR owner == $current_principal
score >= 10
```

Implementation approach:

1. Reuse the Milvus filter parser.
2. Parse RLS expressions into an AST.
3. Build a row evaluator for insert/upsert rows.
4. Evaluate the AST for each incoming row.

#### Existing-Row Validation for Upsert
Strengthen upsert semantics by validating both:

1. The incoming row satisfies the write expression.
2. The existing row being overwritten is visible or writable under the applicable RLS policy.

This is required for stronger shared-write semantics when users can update documents they do not own.

#### ALTER POLICY
Add:
```plaintext
AlterRowPolicy
```

Support:

1. Modify actions.
2. Modify `expr`.
3. Modify description.
4. Rename policy.

#### Policy Explain
Provide a debugging API:
```plaintext
ExplainRowPolicy(principal, collection, action)
```

Example response:
```json
{
  "principal": "alice",
  "tags": {"department": "engineering"},
  "applicable_policies": ["department_access"],
  "rls_expr": "department == 'engineering'",
  "final_expr": "(category == 'report') AND (department == 'engineering')"
}
```

#### SDK Support
Complete SDK support for:

1. Python SDK.
2. Java SDK.
3. Go SDK.
4. Node SDK.
5. REST API documentation.

### Testing Plan

#### Unit Tests
Cover:

1. Policy validation.
2. RLS principal tag validation.
3. Collection property validation for `rls.enabled` and `rls.force`.
4. Expression builder.
5. Variable substitution.
6. Missing tag fail-closed behavior.
7. Permissive policy OR combination.
8. Restrictive policy AND combination.
9. Permissive plus restrictive combination.
10. Query filter merge.
11. Search filter merge.
12. Hybrid search filter merge.
13. Delete filter merge.
14. Insert expression parse.
15. Insert expression row validation.
16. Upsert expression row validation.
17. Cache update and invalidation.
18. Policy loader retry failure.
19. RLS admin principal bypass when `rls.force=false`.
20. RLS admin principal enforcement when `rls.force=true`.

#### Integration Tests
Cover:

1. Create policy and verify query isolation.
2. Different users with different tags see different rows.
3. Delete only deletes allowed rows.
4. Insert cannot write another user's data.
5. Upsert cannot write another user's data in incoming rows.
6. No policy means deny.
7. RLS admin principal bypass when `rls.force=false`.
8. RLS admin principal enforced when `rls.force=true`.
9. Policy deletion refreshes cache.
10. RLS principal metadata update refreshes cache.
11. Collection property update refreshes cache.
12. RLS disabled collection ignores existing policies.
13. RLS enabled collection enforces existing policies.

#### Required Go Test Command
```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/... -run TestRLS
```
