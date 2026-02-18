# RLS Operations Guide

This guide provides practical instructions for using Row Level Security (RLS) in Milvus.

## Table of Contents

1. [Policy Creation](#policy-creation)
2. [User Tags Management](#user-tags-management)
3. [Expression Syntax](#expression-syntax)
4. [Common Patterns](#common-patterns)
5. [Troubleshooting](#troubleshooting)

---

## Policy Creation

### Basic Policy Structure

```go
policy := &RLSPolicy{
    PolicyName:   "policy_name",
    CollectionID: collectionID,
    DBID:         dbID,
    PolicyType:   RLSPolicyTypePermissive,  // or RLSPolicyTypeRestrictive
    Actions:      []string{"query", "search"},
    Roles:        []string{"PUBLIC"},       // or specific role names
    UsingExpr:    "owner_id == $current_user_name",  // filter expression
    CheckExpr:    "owner_id == $current_user_name",  // for INSERT/UPSERT
    Description:  "User can see own data",
}
```

### Policy Types

#### Permissive Policy
- **Default behavior**: DENY (no access)
- **With policy**: Allow if condition matches
- **Multiple policies**: Combined with OR
- **Use case**: "Allow if..."

Example:
```
User can see their own data OR public data
Policy 1: owner_id == $current_user_name
Policy 2: public == true
Result: (owner_id == 'alice' OR public == true) = ALLOW
```

#### Restrictive Policy
- **Default behavior**: ALLOW (with permissive)
- **With policy**: Deny if condition fails
- **Multiple policies**: Combined with AND
- **Use case**: "Deny if..."

Example:
```
Never show archived data AND never show confidential
Policy 1: archived == false
Policy 2: confidential != 'HIGH'
Result: (archived == false AND confidential != 'HIGH') = DENY if fails
```

### Actions Supported

- `query`: Traditional query operations
- `search`: Vector search operations
- `insert`: Insert new records
- `delete`: Delete records
- `upsert`: Insert or update records

### Role Types

- `PUBLIC`: Applies to all users
- Named roles: `employee`, `manager`, `admin`, etc.

---

## User Tags Management

### Setting User Tags

User tags are dynamic metadata attached to users, used in RLS expressions.

```go
tags := map[string]string{
    "department": "engineering",
    "region":     "us-west",
    "level":      "senior",
}

// API call
SetUserTags(userName="alice", tags=tags)
```

### Using Tags in Expressions

Reference tags with bracket notation:

```
department == $current_user_tags['department']
region IN ['us-west', 'us-east']
level >= $current_user_tags['level']
```

### Tag Update Scenarios

**Scenario 1: User Promotion**
```
Old tags: department=engineer, level=junior
New tags: department=engineer, level=senior
→ Policy evaluating on level now grants more access
```

**Scenario 2: User Transfer**
```
Old tags: department=sales, region=north
New tags: department=marketing, region=south
→ Can access different data based on new tags
```

---

## Expression Syntax

### Supported Operators

- Comparison: `==`, `!=`, `<`, `>`, `<=`, `>=`
- Logical: `AND`, `OR`, `NOT`
- Membership: `IN`, `EXISTS`
- Field access: `field_name`

### Variable Substitution

#### Current User Name
```
owner_id == $current_user_name
→ owner_id == 'alice'
```

#### Current User Tags
```
department == $current_user_tags['department']
region == $current_user_tags['region']
→ department == 'engineering'
→ region == 'us-west'
```

#### Escaping
Single quotes in values are escaped:
```
User: alice's-data
Expr: owner == $current_user_name
→ owner == 'alice\'s-data'
```

### Expression Validation

- Non-empty expression required
- Balanced parentheses checked
- Basic format validation applied

### Complex Expressions

```
// Multi-condition AND
department == $current_user_tags['dept'] AND
region == $current_user_tags['region'] AND
status == 'active'

// Grouped OR
(owner == $current_user_name OR manager == $current_user_name) AND
archived == false

// NOT operator
NOT confidential_level > 3 AND
NOT restricted == true
```

---

## Common Patterns

### Pattern 1: User Data Isolation

**Policy**: Each user sees only their own data

```
Name: user_own_data
Type: Permissive
Actions: [query, search, delete]
Roles: [PUBLIC]
Expression: owner_id == $current_user_name
```

### Pattern 2: Department-Based Access

**Policies**:
```
// Permissive: Allow access to own department
Name: department_access
Type: Permissive
Expression: department == $current_user_tags['department']

// Restrictive: Never show confidential
Name: no_confidential
Type: Restrictive
Expression: confidential_level <= 2
```

### Pattern 3: Role-Based Hierarchy

**Policies**:
```
// Employee: Own data + public
Name: employee_policy
Type: Permissive
Roles: [employee]
Expression: owner == $current_user_name OR public == true

// Manager: Team data + public
Name: manager_policy
Type: Permissive
Roles: [manager]
Expression: team_id == $current_user_tags['team'] OR public == true

// Admin: All data
Name: admin_policy
Type: Permissive
Roles: [admin]
Expression: true
```

### Pattern 4: Temporal Access Control

**Policy**: Access only recent data

```
Name: recent_only
Type: Restrictive
Expression:
  created_at >= CURRENT_DATE - INTERVAL 30 DAY
```

### Pattern 5: Geographic Restrictions

**Policy**: Users access only their region

```
Name: geo_restriction
Type: Restrictive
Expression:
  region == $current_user_tags['region'] OR public == true
```

### Pattern 6: Hybrid: Public + Private

**Policies**:
```
// Permissive: User's own OR public
Name: hybrid_access
Type: Permissive
Expression:
  owner == $current_user_name OR visibility == 'public'

// Restrictive: Exclude archived
Name: exclude_archived
Type: Restrictive
Expression: archived == false
```

---

## Troubleshooting

### Issue: Data Not Visible

**Causes**:
1. No matching permissive policy
2. Restrictive policy denies access
3. Role mismatch

**Solution**:
```
1. Verify policy exists:
   ListRowPolicies(dbName, collectionName)

2. Check user role:
   Ensure user has role in policy.Roles

3. Verify expression evaluation:
   Test expression with current values:
   - $current_user_name = alice
   - $current_user_tags = {dept: eng}

4. Check for restrictive policies:
   Ensure restrictive condition is not false
```

### Issue: User Can See All Data

**Causes**:
1. Policy with `true` expression
2. No policy created
3. Admin role with unrestricted policy

**Solution**:
```
1. Verify policies:
   SELECT * FROM rls_policies
   WHERE collection_id = ?

2. Check policy expressions:
   Ensure expressions are not "true" or empty

3. Verify user role:
   User should not have unrestricted admin role
```

### Issue: Insert Rejected

**Causes**:
1. CHECK expression evaluation failed
2. User lacks permission for action
3. Invalid data violates constraint

**Solution**:
```
1. Check INSERT policy:
   Verify policy has 'insert' in actions

2. Check CHECK expression:
   Ensure data satisfies:
   CHECK_EXPR with substituted values

3. Test locally:
   Evaluate expression with actual data values
   owner_id='alice' → should match if user is alice
```

### Issue: Cache Not Updated

**Causes**:
1. Proxy not receiving refresh signal
2. Cache invalidation failed
3. Network issue to proxy

**Solution**:
```
1. Check RootCoord logs:
   Verify RefreshRLSCacheRequest sent

2. Check Proxy logs:
   Verify cache refresh handler invoked

3. Restart proxy if needed:
   Force cache reload

4. Verify etcd:
   Check policy actually stored in etcd
```

### Performance Issues

**Symptoms**:
1. Slow query response
2. High CPU usage
3. Memory issues

**Solutions**:
```
1. Expression complexity:
   - Simplify boolean logic
   - Reduce nested conditions
   - Avoid expensive operations

2. Tag substitution:
   - Cache user tags
   - Avoid frequent tag lookups
   - Use indexed fields in expressions

3. Policy count:
   - Keep permissive count low (< 10)
   - Combine related restrictive policies
   - Archive unused policies

4. Monitor cache:
   - Check L1 hit rate
   - Monitor L2 cache size
   - Tune cache parameters
```

---

## Best Practices

### Do's ✓

- ✓ Use specific role names (avoid broad PUBLIC when possible)
- ✓ Test policies before production deployment
- ✓ Document policy intent in description field
- ✓ Keep expressions simple and readable
- ✓ Use user tags for dynamic attributes
- ✓ Monitor cache performance
- ✓ Regular audit of policies
- ✓ Version control policy definitions

### Don'ts ✗

- ✗ Don't use complex expressions (> 5 conditions)
- ✗ Don't change policies without testing
- ✗ Don't leave unnecessary policies enabled
- ✗ Don't mix unrelated conditions
- ✗ Don't rely on default values
- ✗ Don't forget to handle edge cases
- ✗ Don't expose policy expressions to users
- ✗ Don't skip security validation

---

## Examples

### Example 1: SaaS Multi-Tenant

```
Scenario: Each tenant sees only their data

Policies:
1. Permissive:
   Name: tenant_isolation
   Expression: tenant_id == $current_user_tags['tenant_id']

User alice (tenant=123):
- SetUserTags(alice, {tenant_id: 123})
- Query sees only tenant_id=123 data

User bob (tenant=456):
- SetUserTags(bob, {tenant_id: 456})
- Query sees only tenant_id=456 data
```

### Example 2: HR System with Confidentiality Levels

```
Scenario: HR sees all, managers see team, employees see own

Policies:
1. Employee Policy:
   Permissive: owner == $current_user_name
   Restrictive: confidential_level <= 1

2. Manager Policy:
   Permissive: department == $current_user_tags['dept']
   Restrictive: confidential_level <= 2

3. HR Policy:
   Permissive: true
   (no restrictive)
```

### Example 3: Time-Sensitive Data Access

```
Scenario: Recent data is more accessible, old data restricted

Policies:
1. Restrictive:
   name: archive_recent
   expression:
     (created_at >= CURRENT_DATE - 90 AND confidential != 'HIGH')
     OR created_by == $current_user_name
```

---

## References

- [RLS Implementation Plan](./2026-01-20-rls-implementation.md)
- [E2E Test Scenarios](./rls_e2e_test_scenarios.md)
- RLS Policy Model: `internal/metastore/model/rls_policy.go`
- Expression Builder: `internal/proxy/rls_expr_builder.go`
