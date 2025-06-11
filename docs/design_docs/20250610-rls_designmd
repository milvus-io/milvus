# Milvus Row Level Security (RLS) Design Document

## ✨ Overview

Row Level Security (RLS) provides fine-grained access control at the row level for collections in Milvus. By enabling RLS and defining policies based on user identity, roles, or dynamic tags, administrators can enforce data access restrictions without modifying application logic or data structures.

---

## ⚙️ Core Capabilities

| Feature               | Description                                                  |
|-----------------------|--------------------------------------------------------------|
| Enable/Disable RLS    | Toggle RLS at the collection level with runtime control      |
| Enforce RLS           | Enforce RLS even for superusers and administrators          |
| Policy Definition     | Define policies based on user ID, roles, field values, or tags |
| Multi-policy Support  | Support for multiple policies per action/role combination    |
| User Tag Mechanism    | Use dynamic user metadata for flexible access filtering      |
| Expression Language   | Rich expression syntax for complex access control rules      |

---

## 🔖 User Tag Mechanism

RLS leverages runtime user context including `$current_user_name` and `$current_user_tags` to evaluate access policies dynamically.

### ✅ Setting User Tags

```python
client.set_user_tags(
    user="user_abc",
    tags={
        "department": "engineering",
        "region": "us-west-1",
        "tenant": "customer_a",
        "security_level": "confidential"
    }
)
```

### ✅ Tag Management APIs

| API                          | Description                                    |
| ---------------------------- | ---------------------------------------------- |
| `set_user_tags(user, tags)`  | Set or update user tags (overwrites existing) |
| `delete_user_tag(user, key)` | Delete a specific tag key for a user          |
| `get_user_tags(user)`        | Fetch all user tag information                 |
| `list_users_with_tag(key, value)` | Find users with specific tag values           |

Tags can be referenced in policy expressions using the following syntax:

```python
using_expr="region == $current_user_tags['region']"
check_expr="security_level >= $current_user_tags['clearance']"
```

---

## 🛠️ API Design

### 1. Enable or Disable RLS

```python
# Enable RLS for a collection
client.alter_collection_properties(
    collection="my_collection",
    properties={"rls.enabled": True}
)

# Disable RLS for a collection
client.alter_collection_properties(
    collection="my_collection", 
    properties={"rls.enabled": False}
)
```

### 2. Enforce RLS (even for superusers)

```python
client.alter_collection_properties(
    collection="my_collection",
    properties={
        "rls.enabled": True, 
        "rls.force": True  # Applies to all users including superusers
    }
)
```

### 3. Create an RLS Policy

```python
client.create_row_policy(
    collection="user_documents",
    policy_name="limit_to_user",
    actions=["query", "insert", "delete", "update"],
    roles=["$current_user", "user_role"],
    using_expr="user_id == $current_user_name",
    check_expr="user_id == $current_user_name",
    description="Restrict users to their own documents"
)
```

**Policy Parameters:**
- `collection`: Target collection name
- `policy_name`: Unique identifier for the policy
- `actions`: List of operations this policy applies to (`query`, `insert`, `delete`, `update`)
- `roles`: List of roles this policy applies to (`$current_user`, `admin`, custom roles)
- `using_expr`: Expression for filtering data during queries
- `check_expr`: Expression for validating data during mutations
- `description`: Optional human-readable description

### 4. Delete an RLS Policy

```python
client.drop_row_policy(
    collection="user_documents",
    policy_name="limit_to_user"
)
```

### 5. List All RLS Policies

```python
policies = client.list_row_policies(collection="user_documents")
# Example response:
# [
#   {
#     "policy_name": "limit_to_user",
#     "using_expr": "user_id == $current_user_name",
#     "check_expr": "user_id == $current_user_name", 
#     "roles": ["$current_user"],
#     "actions": ["query", "insert", "delete"],
#     "description": "Restrict users to their own documents",
#     "created_at": "2024-01-15T10:30:00Z"
#   }
# ]
```

### 6. Get Collection RLS Status

```python
status = client.get_collection_properties(
    collection="user_documents",
    properties=["rls.enabled", "rls.force"]
)
# Returns: {"rls.enabled": True, "rls.force": False}
```

---

## ✅ Usage Examples

### Example 1: Users Can Only Access Their Own Data

**Scenario:** A document management system where users should only see and modify their own documents.

**Collection Schema:**
```python
# Collection includes a user_id field
{
    "user_id": "string",
    "document_name": "string", 
    "content": "string",
    "created_at": "timestamp"
}
```

**RLS Policy:**
```python
client.create_row_policy(
    collection="user_documents",
    policy_name="user_own_data",
    actions=["query", "insert", "delete", "update"],
    roles=["$current_user"],
    using_expr="user_id == $current_user_name",
    check_expr="user_id == $current_user_name",
    description="Users can only access their own documents"
)
```

---

### Example 2: Role-Based Access Control

**Scenario:** Admins have full access, managers see department data, users see only their own data.

**User Policy (restricted):**
```python
client.create_row_policy(
    collection="employee_records",
    policy_name="user_scope",
    actions=["query", "insert", "delete", "update"],
    roles=["$current_user"],
    using_expr="employee_id == $current_user_name",
    check_expr="employee_id == $current_user_name"
)
```

**Manager Policy (department scope):**
```python
client.create_row_policy(
    collection="employee_records", 
    policy_name="manager_scope",
    actions=["query", "insert", "update"],
    roles=["manager"],
    using_expr="department == $current_user_tags['department']",
    check_expr="department == $current_user_tags['department']"
)
```

**Admin Policy (full access):**
```python
client.create_row_policy(
    collection="employee_records",
    policy_name="admin_full_access",
    actions=["query", "insert", "delete", "update"],
    roles=["admin"],
    using_expr="true",
    check_expr="true"
)
```

---

### Example 3: Multi-Tenant Data Isolation

**Scenario:** SaaS application with tenant-based data isolation using user tags.

**Policy:**
```python
client.create_row_policy(
    collection="customer_data",
    policy_name="tenant_isolation",
    actions=["query", "insert", "delete", "update"],
    roles=["$current_user"],
    using_expr="tenant_id == $current_user_tags['tenant']",
    check_expr="tenant_id == $current_user_tags['tenant']"
)
```

**User Tag Setup:**
```python
client.set_user_tags(
    user="user_123",
    tags={"tenant": "acme_corp", "role": "analyst"}
)
```

---

### Example 4: Time-Based Access Control

**Scenario:** Documents are only accessible during business hours for non-admin users.

**Policy:**
```python
client.create_row_policy(
    collection="sensitive_documents",
    policy_name="business_hours_access",
    actions=["query"],
    roles=["$current_user"],
    using_expr="(hour(now()) >= 9 AND hour(now()) <= 17) OR $current_user_tags['role'] == 'admin'",
    check_expr="true"
)
```

---

## 🔒 Security Model Notes

### Policy Evaluation
- **OR Logic**: All policies for a user are OR-combined - if any policy grants access, the operation is allowed
- **Action-Specific**: Policies are evaluated based on the specific action being performed
- **Role Matching**: Users must have at least one role that matches the policy's role list

### Access Control Levels
- **Default Behavior**: RLS applies only to non-superusers
- **Force Mode**: With `rls.force=True`, RLS applies to everyone including superusers and administrators
- **Bypass Options**: Superusers can temporarily bypass RLS for maintenance operations

### Expression Language
- **Field References**: Use field names directly in expressions
- **Variables**: `$current_user_name`, `$current_user_tags`, `$current_roles`
- **Functions**: Support for common functions like `now()`, `hour()`, `date()`
- **Operators**: Standard comparison and logical operators

### Performance Considerations
- **Index Usage**: RLS expressions should leverage indexed fields for optimal performance
- **Expression Complexity**: Complex expressions may impact query performance
- **Policy Count**: Large numbers of policies per collection may affect evaluation speed

### Best Practices
- **Principle of Least Privilege**: Start with restrictive policies and gradually expand access
- **Regular Auditing**: Periodically review and test RLS policies
- **Documentation**: Maintain clear documentation of policy purposes and effects
- **Testing**: Test policies with various user roles and scenarios before production deployment


