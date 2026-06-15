# Milvus Row-Level Security (RLS) Design Document

| Field | Value |
| --- | --- |
| Title | Row-Level Security (RLS) |
| Status | Draft |
| Author | James Luan (@xiaofan-luan) Buqian Zheng (@zhengbuqian) |
| Initial Draft | 2025-04-10 |
| Consolidated | 2026-05-25 |
| Related Branch | [rls-feature](https://github.com/milvus-io/milvus/pull/48448) |

## 1. Summary
Row-Level Security (RLS) provides fine-grained row-level access control for Milvus collections. After RLS is enabled on a collection, Milvus automatically limits which rows a user can `query`, `search`, `hybrid_search`, `delete`, `insert`, or `upsert` based on the current user, user roles, user tags, and administrator-defined row policies.
RLS moves multi-tenant isolation, user-owned data isolation, and department-scoped data isolation from application code into the database layer. This avoids data leaks caused by application paths that forget to add tenant or ownership filters.
RLS is enforced in Proxy:

- For read-like paths (`query`, `search`, `delete`), Proxy injects additional filter expressions before request execution.
- For write-like paths (`insert`, `upsert`), Proxy validates incoming rows against `CHECK` expressions before data is written.

Core principles:

1. RLS is disabled by default.
2. RLS is enabled per collection through collection properties.
3. When enabled, RLS is fail-closed.
4. No matching policy means deny access.
5. Policies support PostgreSQL-style `PERMISSIVE` and `RESTRICTIVE` combination semantics.
6. Policies separate `USING` expressions for existing rows and `CHECK` expressions for incoming rows.
7. Policies support dynamic expressions through user tags, such as tenant isolation.
8. Root/admin users bypass RLS by default, unless `rls.force=true` is set on the collection.
9. RLS management operations require admin privileges.

## 2. Quick Start: Personal Documents and Sharing
This quick start shows how to use RLS to build a document collection where:

1. Each user can read their own documents.
2. Each user can insert, update, and delete only documents they own.
3. A user can share their documents with specific users.
4. A user can share their documents with an entire department.
5. Shared users or departments can read shared documents, but cannot modify or delete them.

### 2.1 Create a Document Collection
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

Field meanings:

| Field | Meaning |
| --- | --- |
| `owner` | The user who owns the document |
| `department` | The owner's department |
| `shared_users` | Additional users who can read this document |
| `shared_departments` | Departments whose members can read this document |

`shared_users` does not need to include the owner. The owner is represented separately by `owner`, and owner access is granted by owner-specific policies.

### 2.2 Assign User Tags
RLS can use user tags to express dynamic policies.
```python
client.set_user_tags(
    user_name="alice",
    tags={"department": "engineering"},
)

client.set_user_tags(
    user_name="bob",
    tags={"department": "engineering"},
)

client.set_user_tags(
    user_name="carol",
    tags={"department": "product"},
)
```

RLS expressions can reference:
```plaintext
$current_user_name
$current_user_tags['department']
```

### 2.3 Enable RLS on the Collection
RLS is controlled by collection properties. An admin enables RLS by updating the target collection:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "false",
    },
)
```

### 2.4 Allow Users to Read Their Own Documents
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_own_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    roles=["PUBLIC"],
    using_expr="owner == $current_user_name",
)
```

If Alice runs:
```python
alice_client = MilvusClient()
alice_client.set_current_user("alice")
alice_client.query(
    collection_name="documents",
    filter="title like 'RLS%'",
)
alice_client.search
alice_client.insert
```

Milvus executes:
```plaintext
(title like 'RLS%') AND (owner == 'alice')
```

Alice only sees documents she owns.

### 2.5 Allow Users to Read Documents Shared Directly With Them
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_user_shared_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    roles=["PUBLIC"],
    using_expr="$current_user_name in shared_users",
)
```

Because `read_own_documents` and `read_user_shared_documents` are both `PERMISSIVE`, they are combined with `OR`.
For Bob, the effective read expression becomes:
```plaintext
(owner == 'bob') OR ('bob' in shared_users)
```

Bob can read documents he owns and documents explicitly shared with him.

### 2.6 Allow Users to Read Documents Shared With Their Department
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_department_shared_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    roles=["PUBLIC"],
    using_expr="$current_user_tags['department'] in shared_departments",
)
doc0: [a b c d...]

a: [doc0]
b: [doc0]
shared_departments: [dev, qa]
```

For Carol, whose department tag is `product`, the effective read expression becomes:
```plaintext
(owner == 'carol')
OR
('carol' in shared_users)
OR
('product' in shared_departments)
```

Carol can read documents she owns, documents shared directly with her, and documents shared with the Product department.

### 2.7 Allow Users to Insert Only Their Own Documents
Reading shared documents should not imply write permission. For `insert`, use `CHECK` to ensure every inserted row is owned by the current user.
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="insert_own_documents",
    policy_type="PERMISSIVE",
    actions=["insert"],
    roles=["PUBLIC"],
    using_expr="owner == $current_user_name",
    check_expr="owner == $current_user_name",
)
```

Alice can insert:
```python
alice_client.insert(
    collection_name="documents",
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
alice_client.insert(
    collection_name="documents",
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
row 0 violates RLS CHECK: field 'owner' value 'bob' not allowed by policy
```

### 2.8 Allow Users to Upsert Only Their Own Documents
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="upsert_own_documents",
    policy_type="PERMISSIVE",
    actions=["upsert"],
    roles=["PUBLIC"],
    using_expr="owner == $current_user_name",
    check_expr="owner == $current_user_name",
)
```

This allows Alice to update her own document, including its sharing fields:
```python
alice_client.upsert(
    collection_name="documents",
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
'bob' in shared_users
```

Alice can also share the same document with an entire department:
```python
alice_client.upsert(
    collection_name="documents",
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
For the first version, RLS `CHECK` validates the incoming row. If an application allows user-controlled primary keys, it should also ensure one user cannot upsert over another user's existing primary key. A common approach is to use server-generated document IDs or include ownership in the application's ID allocation logic. Future versions may add stronger existing-row validation for upsert.

### 2.9 Allow Users to Delete Only Their Own Documents
Delete should not use the same expression as read sharing. Otherwise, users could delete documents merely shared with them.
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="delete_own_documents",
    policy_type="PERMISSIVE",
    actions=["delete"],
    roles=["PUBLIC"],
    using_expr="owner == $current_user_name",
)
```

Alice can delete her own documents:
```python
alice_client.delete(
    collection_name="documents",
    filter="id == 100",
)
```

Milvus executes:
```plaintext
(id == 100) AND (owner == 'alice')
```

Bob cannot delete Alice's document even if Alice shared it with Bob, because delete does not use the sharing policies.

### 2.10 Final Effective Behavior
For `query` and `search`, the effective RLS expression is:
```plaintext
owner == $current_user_name
OR
$current_user_name in shared_users
OR
$current_user_tags['department'] in shared_departments
```

For `insert` and `upsert`, the effective `CHECK` expression is:
```plaintext
owner == $current_user_name
```

For `delete`, the effective RLS expression is:
```plaintext
owner == $current_user_name
```


| Operation | Allowed rows |
| --- | --- |
| `query` | Own documents, directly shared documents, and department-shared documents |
| `search` | Own documents, directly shared documents, and department-shared documents |
| `insert` | Only incoming rows where `owner == current user` |
| `upsert` | Only incoming rows where `owner == current user` |
| `delete` | Only existing rows where `owner == current user` |

If shared users or shared departments must also write documents, the schema should separate read ACLs from write ACLs, for example `shared_read_users`, `shared_write_users`, `shared_read_departments`, and `shared_write_departments`. The read and write policies should then be defined separately. The first version's upsert validation only checks the incoming row, so secure shared-write semantics over existing documents need additional existing-row validation or application-level ID ownership guarantees.

## 3. User-Facing Semantics

### 3.1 Authorization Requirement
RLS depends on Milvus authorization. A cluster must enable authorization before RLS can take effect.
```yaml
common:
  authorizationEnabled: true

proxy:
  rls:
    auditEnabled: true
    cacheExpirationSeconds: 3600
    maxCacheEntries: 10000
    maxPoliciesPerCollection: 100
    maxUserTags: 50
    maxExpressionLength: 4096
```

Semantics:

- `authorizationEnabled=false` means RLS cannot be enabled for any collection.
- Authorization provides authenticated users and roles for RLS evaluation.
- Enabling authorization does not enable RLS for every collection automatically.
- RLS is enabled explicitly at collection level.

### 3.2 Collection-Level RLS Properties
RLS is controlled by collection properties, not by separate enable/disable APIs. An admin enables, disables, or forces RLS by updating collection properties.
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
| `rls.force` | `false` | Whether root/admin users must also follow RLS policies |

Enable RLS for normal users while root/admin users still bypass RLS:
```python
client.alter_collection_properties(
    collection_name="documents",
    properties={
        "rls.enabled": "true",
        "rls.force": "false",
    },
)
```

Enable FORCE RLS so every user, including root/admin users, must satisfy applicable RLS policies:
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
- When `rls.enabled=true`, non-root/admin users must match an applicable policy for the requested action.
- If no applicable policy exists for the action and user, access is denied.
- Root/admin users bypass RLS by default when `rls.force=false`.
- When `rls.force=true`, root/admin users must also match an applicable policy for the requested action.
- When `rls.enabled=false`, RLS does not change `query`, `search`, `delete`, `insert`, or `upsert` behavior for that collection, regardless of `rls.force`.

## 4. Policy Model

### 4.1 Policy Fields
An RLS policy contains the following fields:
```plaintext
policy_name
db_id
collection_id
policy_type
actions
roles
using_expr
check_expr
description
created_at
policy_id
```


| Field | Description |
| --- | --- |
| `policy_name` | Human-readable policy name, unique within a collection |
| `db_id` | Database ID |
| `collection_id` | Collection ID |
| `policy_type` | `PERMISSIVE` or `RESTRICTIVE` |
| `actions` | Operations this policy applies to |
| `roles` | Roles this policy applies to; supports `PUBLIC` |
| `using_expr` | Filter expression for `query`, `search`, and `delete` |
| `check_expr` | Validation expression for `insert` and `upsert` |
| `description` | Optional description |
| `created_at` | Creation timestamp |
| `policy_id` | Globally unique ID allocated by RootCoord |

Go model validation is implemented by `RLSPolicy.Validate()` in `internal/metastore/model/rls_policy.go`.

### 4.2 Protobuf Model
```protobuf
message RLSPolicy {
    string policy_name = 1;
    string collection_name = 2;
    int64 collection_id = 3;
    RLSPolicyType policy_type = 4;
    repeated string actions = 5;
    repeated string roles = 6;
    string using_expr = 7;
    string check_expr = 8;
    string description = 9;
    int64 created_at = 10;
    int64 policy_id = 11;
    int64 db_id = 12;
}
```

`policy_id` is allocated by RootCoord and used in the storage key. `policy_name` is still unique within a collection and is used by user-facing APIs such as `DropRowPolicy`.

### 4.3 Collection and Policy Association
A collection uses the row policies whose `db_id` and `collection_id` match the collection.
There is no separate policy binding table. The association is encoded directly in each policy object:
```plaintext
policy.db_id == collection.db_id
AND
policy.collection_id == collection.collection_id
```

When a request reaches Proxy, Proxy resolves the target collection to `db_id` and `collection_id`, then loads all policies under that collection:
```plaintext
rls/policy/{dbID}/{collectionID}/{policyID}
```

Only policies from that collection can participate in RLS expression construction. Policies from other collections are ignored, even if they have the same policy name, role, action, or expression.
A collection-level RLS switch controls whether those associated policies are enforced:

| Collection property | Effect |
| --- | --- |
| `rls.enabled=false` | Associated policies exist but are not enforced |
| `rls.enabled=true` | Associated policies are enforced for non-root/admin users |
| `rls.force=true` | Associated policies are also enforced for root/admin users |

Policy creation and policy enforcement are separate steps:

1. Create one or more policies on a collection.
2. Set `rls.enabled=true` on that collection.
3. Proxy loads the collection's policies and applies policies matching the current action and user roles.

### 4.4 Roles
`roles` controls which users a policy applies to. It does not grant Milvus privileges by itself; normal authorization still decides whether the user can issue `query`, `search`, `insert`, `delete`, or `upsert`. RLS `roles` only decides whether this policy participates in row-level filtering for that request.
A policy applies to a user when either:

1. The policy contains one of the user's roles.
2. The policy contains `PUBLIC`.

`PUBLIC` means the policy applies to all authenticated users.
Example policy that applies to every authenticated user:
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="read_shared_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    roles=["PUBLIC"],
    using_expr="$current_user_name in shared_users",
)
```

Example policy that applies only to users with the `auditor` role:
```python
client.create_row_policy(
    collection_name="documents",
    policy_name="auditor_read_published_documents",
    policy_type="PERMISSIVE",
    actions=["query", "search"],
    roles=["auditor"],
    using_expr="status == 'published'",
)
```

If a user does not have the `auditor` role, the second policy is ignored for that user. If no other applicable policy matches the user and action, RLS denies access.

### 4.5 Supported Actions
RLS supports the following actions:
```plaintext
query
search
delete
insert
upsert
```

`query`, `search`, and `delete` use `USING` expressions because they operate on existing rows.
`insert` and `upsert` use `CHECK` expressions because they validate incoming rows.

### 4.6 Policy Types

#### PERMISSIVE
Multiple permissive policies are combined with `OR`:
```plaintext
tenant_id == 'acme'
OR
visibility == 'public'
```

A row passes the permissive part if it satisfies at least one permissive policy.

#### RESTRICTIVE
Multiple restrictive policies are combined with `AND`:
```plaintext
status != 'archived'
AND
region in ['us', 'eu']
```

A row passes the restrictive part only if it satisfies all restrictive policies.

#### Combined Semantics
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

## 5. USING and CHECK

### 5.1 USING
`USING` limits which existing rows a user can read or delete.
Applicable actions:
```plaintext
query
search
delete
```

Example:
```plaintext
tenant_id == $current_user_tags['tenant']
```

If the user request filter is:
```plaintext
category == 'report'
```

and the current user's `tenant` tag is `acme`, Proxy injects:
```plaintext
(category == 'report') AND (tenant_id == 'acme')
```

`delete` uses `USING`, not `CHECK`, because delete chooses which existing rows can be deleted. It does not validate newly inserted row values.

### 5.2 CHECK
`CHECK` limits which rows a user can write.
Applicable actions:
```plaintext
insert
upsert
```

Example:
```plaintext
tenant_id == $current_user_tags['tenant']
```

If the current user's tags are:
```json
{
  "tenant": "acme"
}
```

then every inserted or upserted row must satisfy:
```plaintext
tenant_id == 'acme'
```

### 5.3 USING to CHECK Fallback
If a policy contains `insert` or `upsert` in `actions` but does not explicitly configure `check_expr`, the system automatically uses `using_expr` as `check_expr`.
This allows a single policy to protect both read and write paths:
```python
client.create_row_policy(
    collection_name="docs",
    policy_name="tenant_isolation",
    policy_type="PERMISSIVE",
    actions=["query", "search", "delete", "insert", "upsert"],
    roles=["PUBLIC"],
    using_expr="tenant_id == $current_user_tags['tenant']",
)
```

## 6. Variables
RLS expressions support variable substitution.

### 6.1 `$current_user_name`
`$current_user_name` represents the authenticated username.
Example:
```plaintext
owner == $current_user_name
```

If the current user is `alice`, the expression becomes:
```plaintext
owner == 'alice'
```

### 6.2 `$current_user_tags['key']`
`$current_user_tags['key']` represents a tag value attached to the current user.
Example:
```plaintext
tenant_id == $current_user_tags['tenant']
```

If the current user's tags are:
```json
{
  "tenant": "acme"
}
```

then the expression becomes:
```plaintext
tenant_id == 'acme'
```

### 6.3 Missing Tags
If an expression references a missing tag, expression construction must fail closed.
Example expression:
```plaintext
tenant_id == $current_user_tags['tenant']
```

If the current user has no `tenant` tag, the expression becomes:
```plaintext
false
```

## 7. Management APIs

### 7.1 Create Policy
Go client option:
```go
client.NewCreateRowPolicyOption(
    "documents",
    "tenant_isolation",
    []entity.RowPolicyAction{
        entity.RowPolicyActionQuery,
        entity.RowPolicyActionSearch,
        entity.RowPolicyActionDelete,
        entity.RowPolicyActionInsert,
        entity.RowPolicyActionUpsert,
    },
    []string{"PUBLIC"},
    "tenant_id == $current_user_tags['tenant']",
).WithCheckExpr(
    "tenant_id == $current_user_tags['tenant']",
).WithDescription(
    "tenant isolation policy",
)
```

### 7.2 Drop Policy
```go
client.DropRowPolicy(
    ctx,
    client.NewDropRowPolicyOption("documents", "tenant_isolation"),
)
```

### 7.3 List Policies
```go
policies, err := client.ListRowPolicies(
    ctx,
    client.NewListRowPoliciesOption("documents"),
)
```

### 7.4 Manage User Tags
```go
client.SetUserTags(
    ctx,
    client.NewSetUserTagsOption("alice", map[string]string{
        "tenant": "acme",
    }),
)

tags, err := client.GetUserTags(
    ctx,
    client.NewGetUserTagsOption("alice"),
)

client.DeleteUserTags(
    ctx,
    client.NewDeleteUserTagsOption("alice", "tenant"),
)
```

### 7.5 Manage Collection RLS Properties
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

## 8. Architecture

### 8.1 High-Level Components
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
       +-- RLSCheckValidator
  |
  v
Task Layer
  |
  +-- query task
  +-- search task
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
  +-- rls/user_tags/{userName}
```

### 8.2 Why Enforce in Proxy
RLS is enforced in Proxy because:

1. Proxy is the user request entry point and can access user context.
2. Query/search/delete requests still have original filters at the Proxy layer.
3. Insert/upsert row data is available at the Proxy layer for schema validation and per-row validation.
4. QueryNode and DataNode execution engines do not need to be changed.
5. RLS enforcement stays close to existing authorization checks.

Tradeoffs:

1. QueryNode and DataNode do not independently know RLS policy state.
2. Every relevant operation must go through Proxy hooks.
3. Proxy cache must fail closed and must not bypass RLS when RootCoord is temporarily unavailable.

## 9. Request Flow

### 9.1 Create Policy Flow
```plaintext
Client
  -> Proxy.CreateRowPolicy
  -> require admin privilege
  -> RootCoord.CreateRowPolicy
  -> RootCoord DDL task
  -> allocate policyID
  -> validate policy
  -> persist to metastore
  -> broadcast RLS cache refresh
  -> return success
```

### 9.2 Query Flow
```plaintext
Proxy query task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> check root/admin bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure user tags loaded
  -> RLSQueryInterceptor.InterceptQuery
  -> RLSExpressionBuilder.BuildExpression(action=query)
  -> merge user filter and RLS filter
  -> continue query plan generation
```

### 9.3 Search Flow
```plaintext
Proxy search task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> check root/admin bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure user tags loaded
  -> RLSSearchInterceptor.InterceptSearch
  -> RLSExpressionBuilder.BuildExpression(action=search)
  -> merge search filter and RLS filter
```

Hybrid search applies the RLS filter to each sub-request independently.

### 9.4 Delete Flow
```plaintext
Proxy delete task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> check root/admin bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure user tags loaded
  -> RLSDeleteInterceptor.InterceptDelete
  -> RLSExpressionBuilder.BuildExpression(action=delete)
  -> merge delete filter and RLS filter
  -> continue delete execution
```

### 9.5 Insert Flow
Insert has two validation stages.
Stage 1: collection-level and policy gate:
```plaintext
insert task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> check root/admin bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure user tags loaded
  -> RLSInsertInterceptor.InterceptInsert
  -> build action=insert CHECK expression
  -> if no matching policy: reject request
  -> save check expression
```

Stage 2: per-row CHECK validation:
```plaintext
insert task
  -> normal schema/data validation
  -> validateInsertAgainstRLSCheck(checkExpr, fieldsData)
  -> parse CHECK expression
  -> find target field
  -> validate each row
  -> any violation rejects whole batch
```

### 9.6 Upsert Flow
Upsert follows the same structure as insert:
```plaintext
upsert task
  -> resolve dbID / collectionID
  -> check collection RLS config
  -> check root/admin bypass unless rls.force=true
  -> ensure policies loaded
  -> ensure user tags loaded
  -> build action=upsert CHECK expression
  -> save check expression
  -> validate incoming rows before execution
```

The first version validates the incoming row. It does not validate the previous stored row that may be overwritten by the upsert.

## 10. Expression Builder

### 10.1 Applicable Policy Selection
A policy participates in expression construction only when both conditions are true:

1. The policy `actions` contains the current action.
2. The policy `roles` contains one of the current user's roles or contains `PUBLIC`.

### 10.2 No Policy Semantics
If a collection has no policies:
```plaintext
false
```

If a collection has policies but none are applicable to the current action and user:
```plaintext
false
```

This means deny all.

### 10.3 Expression Merge
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

### 10.4 PostgreSQL-Style Policy Combination
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

## 11. CHECK Expression Validator

### 11.1 Supported CHECK Expressions
The first version supports simple per-row CHECK expressions:
```plaintext
field == 'string'
field == 123
field in ['a', 'b', 'c']
field in [1, 2, 3]
```

### 11.2 Unsupported CHECK Expressions
The first version does not support these CHECK expressions for per-row insert/upsert validation:
```plaintext
tenant_id == 'acme' AND region == 'us'
tenant_id == 'acme' OR visibility == 'public'
score > 10
field != 'x'
nested function calls
subqueries
```

Unsupported CHECK expressions must return clear errors and must not bypass validation.
These expressions are not supported in v1 because read-path filters can be delegated to the existing Milvus filter execution engine, but write-path CHECK validation runs in Proxy before rows are written. Proxy needs a row evaluator that can parse the expression, bind one incoming row at a time, and evaluate the expression safely. The first version intentionally supports only `==` and `IN` so insert/upsert validation can be implemented safely and fail closed without building a full expression interpreter in Proxy.

### 11.3 Validation Behavior
Validation flow:
```plaintext
parse CHECK expression
  -> extract field name
  -> extract operator
  -> extract allowed values
  -> locate field data by field name
  -> validate all row values
  -> first violation returns error
```

If the CHECK field does not exist:
```plaintext
RLS CHECK field 'tenant_id' not found in insert data
```

If the field is not scalar:
```plaintext
RLS CHECK field 'tenant_id' is not a scalar field
```

If a row does not satisfy the CHECK:
```plaintext
row 2 violates RLS CHECK: field 'tenant_id' value 'globex' not allowed by policy
```

## 12. Cache Design

### 12.1 Cache Types
Proxy has two RLS cache layers:

1. `RLSCache`

  - Stores collection policies.
  - Stores collection meta RLS config.
  - Stores user tags.

2. `RLSCacheWithLoader`

  - Loads data from RootCoord on cache miss.
  - Uses TTL for expiration.
  - Uses double-checked locking to avoid thundering herd.

### 12.2 Policy Load
When a request enters Proxy, Proxy calls:
```plaintext
EnsurePoliciesLoaded
```

If the cache does not contain policies for the current collection, Proxy loads policies through RootCoord:
```plaintext
ListRowPolicies
```

### 12.3 User Tags Load
When a request enters Proxy, Proxy calls:
```plaintext
EnsureUserTagsLoaded
```

If the cache does not contain tags for the current user, Proxy loads tags through RootCoord:
```plaintext
GetUserTags
```

### 12.4 Load Failure
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

### 12.5 Cache Refresh
RootCoord broadcasts cache refresh events when any of the following changes:

1. Policy created.
2. Policy dropped.
3. User tags updated.
4. User tags deleted.
5. Collection RLS properties changed.

## 13. Storage Design

### 13.1 Policy Storage
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

### 13.2 User Tags Storage
User tags storage key:
```plaintext
rls/user_tags/{userName}
```

The value is a protobuf-serialized user tags map.

### 13.3 Collection-Level RLS Config Storage
Collection-level RLS config is stored together with collection metadata, not as a separate RLS metastore key.
Collection meta should contain fields similar to:
```plaintext
rls_enabled: bool
force_rls: bool
```

Semantics:

- `rls_enabled=false` means RLS is disabled for this collection.
- `rls_enabled=true` means non-root/admin users are enforced by policies on this collection.
- `force_rls=false` means root/admin users bypass RLS by default.
- `force_rls=true` means root/admin users are also enforced by RLS.

Storing the switch in collection meta keeps RLS enable/disable consistent with other collection-level properties and avoids maintaining a separate collection config object.

## 14. Security Model

### 14.1 Admin-Only Management
The following operations require admin privileges:
```plaintext
CreateRowPolicy
DropRowPolicy
ListRowPolicies
SetUserTags
GetUserTags
DeleteUserTags
AlterCollectionProperties for rls.enabled / rls.force
```

### 14.2 Runtime Enforcement
Normal users are enforced by RLS for these operations when `rls.enabled=true`:
```plaintext
query
search
delete
insert
upsert
```

### 14.3 Bypass and FORCE RLS
The following users bypass RLS by default:

1. Root user.
2. Users with admin role.

FORCE RLS is a collection-level switch. When `rls.force=true`, root/admin users no longer bypass RLS for that collection and must match applicable policies for the requested action.

### 14.4 Fail-Closed Rules
When RLS is enabled, all of the following deny access:

| Scenario | Behavior |
| --- | --- |
| No user context | Deny |
| No policy | Deny |
| No applicable policy | Deny |
| Expression construction fails | Deny |
| Referenced user tag is missing | Deny |
| CHECK expression is unsupported | Deny |
| CHECK field is missing | Deny |
| Policy load fails | Panic/restart after retries |
| RootCoord unavailable and cache miss occurs | Panic/restart after retries |

Query/search/delete paths deny by returning a `false` filter or an error.
Insert/upsert paths deny by returning an error.
Cache load paths deny by panicking after retry exhaustion so the proxy restarts instead of serving without enforcement.

### 14.5 Injection Prevention
Variable substitution must not allow policy expression injection.
Rules:

- User tag keys and values are validated.
- String values are escaped before substitution.
- Missing user tags become `false` rather than empty strings.
- RLS expressions are parsed and validated before use.
- RLS management APIs require admin privileges.

## 15. Error Handling

### 15.1 User-Facing Errors
No matching insert policy:
```plaintext
insert operation denied by RLS: no matching policies
```

RLS CHECK violation:
```plaintext
row 1 violates RLS CHECK: field 'tenant_id' value 'globex' not allowed by policy
```

Missing CHECK field:
```plaintext
RLS CHECK field 'tenant_id' not found in insert data
```

Unsupported CHECK expression:
```plaintext
compound CHECK expressions are not supported for per-row insert validation
```

Policy load failure:
```plaintext
RLS policy load failed after retries for collection db/collection.
Proxy cannot serve requests without RLS enforcement.
```

### 15.2 Internal Error Principle
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

## 16. Observability

### 16.1 Logs
Log the following events:

1. Policy created or dropped.
2. User tags updated or deleted.
3. RLS filter injection failure.
4. RLS CHECK validation failure.
5. Cache load failure.
6. Root/admin bypass.
7. Cache refresh.
8. FORCE RLS enforcement for root/admin users.

Logs should include:
```plaintext
user
db
collection
collectionID
action
policy names
error
requestID / traceID
```

### 16.2 Metrics
Recommended metrics:
```plaintext
rls_cache_hit_total
rls_cache_miss_total
rls_policy_load_failure_total
rls_active_policy_count
rls_expression_build_latency
rls_check_validation_failure_total
rls_denied_total
rls_bypass_total
rls_force_enforced_total
```

## 17. Limitations
First version limitations:

1. CHECK expressions only support `==` and `IN`.
2. CHECK expressions do not support `AND` / `OR`.
3. `ALTER POLICY` is not supported; use drop and create.
4. Role inheritance is not supported.
5. `$current_role` is not supported.
6. Policy simulator/explain API is not supported.
7. Cross-collection policies are not supported.
8. RLS cache has a short eventual-consistency window before cache refresh reaches every proxy.
9. Upsert validation checks the incoming row but does not validate the previous existing row that may be overwritten.

## 18. Future Work

### 18.1 Complex CHECK Evaluator
Support expressions such as:
```plaintext
tenant_id == 'acme' AND region == 'us'
visibility == 'public' OR owner == $current_user_name
score >= 10
```

Implementation approach:

1. Reuse the Milvus filter parser.
2. Parse CHECK expressions into an AST.
3. Build a row evaluator for insert/upsert rows.
4. Evaluate the AST for each incoming row.

### 18.2 Existing-Row Validation for Upsert
Strengthen upsert semantics by validating both:

1. The incoming row satisfies `CHECK`.
2. The existing row being overwritten is visible or writable under the applicable RLS policy.

This is required for stronger shared-write semantics when users can update documents they do not own.

### 18.3 ALTER POLICY
Add:
```plaintext
AlterRowPolicy
```

Support:

1. Modify actions.
2. Modify roles.
3. Modify `using_expr`.
4. Modify `check_expr`.
5. Modify description.
6. Rename policy.

### 18.4 Policy Explain
Provide a debugging API:
```plaintext
ExplainRowPolicy(user, collection, action)
```

Example response:
```json
{
  "user": "alice",
  "roles": ["analyst"],
  "tags": {"tenant": "acme"},
  "matched_policies": ["tenant_isolation"],
  "rls_expr": "tenant_id == 'acme'",
  "final_expr": "(category == 'report') AND (tenant_id == 'acme')"
}
```

### 18.5 SDK Support
Complete SDK support for:

1. Python SDK.
2. Java SDK.
3. Go SDK.
4. Node SDK.
5. REST API documentation.

## 19. Testing Plan

### 19.1 Unit Tests
Cover:

1. Policy validation.
2. User tags validation.
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
14. Insert CHECK parse.
15. Insert CHECK row validation.
16. Upsert CHECK row validation.
17. Cache update and invalidation.
18. Policy loader retry failure.
19. Root/admin bypass when `rls.force=false`.
20. Root/admin enforcement when `rls.force=true`.

### 19.2 Integration Tests
Cover:

1. Create policy and verify query isolation.
2. Different users with different tags see different rows.
3. Delete only deletes allowed rows.
4. Insert cannot write another tenant's data.
5. Upsert cannot write another tenant's data in incoming rows.
6. No policy means deny.
7. No matching role means deny.
8. Root/admin bypass when `rls.force=false`.
9. Root/admin enforced when `rls.force=true`.
10. Policy deletion refreshes cache.
11. User tag update refreshes cache.
12. Collection property update refreshes cache.
13. RLS disabled collection ignores existing policies.
14. RLS enabled collection enforces existing policies.

### 19.3 Required Go Test Command
```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/... -run TestRLS
```
