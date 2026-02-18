# RLS End-to-End Test Scenarios

This document describes comprehensive end-to-end test scenarios for Row Level Security (RLS) in Milvus.

## Overview

These E2E scenarios test the complete RLS workflow from policy creation through data access enforcement across RootCoord, Proxy, and data nodes.

## Scenario 1: Basic User Isolation

**Objective:** Verify that users can only see their own data

**Setup:**
- Create collection "users_data"
- Insert 3 documents: user1_doc, user2_doc, user3_doc (owner_id field)
- Create RLS policy: `owner_id == $current_user_name`

**Workflow:**
1. RootCoord receives CreateRowPolicy request
2. Policy stored in etcd via metastore
3. Proxy loads policy into cache (L1)
4. Query request from user1 triggers RLS evaluation
5. Expression builder merges: (user_filter) AND (owner_id == 'user1')
6. QueryNode applies merged filter
7. Only user1_doc is returned

**Verification:**
- [ ] RootCoord successfully stores policy
- [ ] Proxy cache is updated with policy
- [ ] User1 sees only user1_doc
- [ ] User2 sees only user2_doc
- [ ] Admin with no RLS policy sees all docs

---

## Scenario 2: Department-Based Access with User Tags

**Objective:** Verify tag-based access control

**Setup:**
- Create collection "sales_data"
- Insert documents with dept field: {name, dept}
- Create users with tags: alice(dept=eng), bob(dept=sales)
- Create policy: `dept == $current_user_tags['dept']`

**Workflow:**
1. RootCoord receives SetUserTags request for alice
   - SetUserTagsTask stores {dept: eng} in etcd
2. Proxy receives query from alice
   - Cache loads user tags
   - RLS substitutes $current_user_tags['dept'] → 'eng'
3. Expression becomes: (filter) AND (dept == 'eng')
4. QueryNode applies filter
5. Only engineering records returned

**Verification:**
- [ ] User tags stored in metastore
- [ ] Proxy cache reflects user tags
- [ ] Tag substitution works correctly
- [ ] Alice sees only eng dept records
- [ ] Bob sees only sales dept records

---

## Scenario 3: Mixed Permissive and Restrictive Policies

**Objective:** Verify correct AND/OR semantics

**Setup:**
- Create collection "products"
- Insert: {id, owner, archived, category}
- Create two policies:
  1. Permissive: `owner == $current_user_name OR category == 'public'`
  2. Restrictive: `archived == false`

**Workflow:**
1. RootCoord creates both policies
2. Policies cached in Proxy
3. User query triggers expression building:
   - Permissive policies ORed: (owner == 'alice' OR category == 'public')
   - Restrictive policies ANDed: (archived == false)
   - Final: (owner == 'alice' OR category == 'public') AND (archived == false)
4. Only active records from user's own or public items shown

**Verification:**
- [ ] Both policies created successfully
- [ ] Expression merging is correct
- [ ] Alice sees: own records + public records (non-archived)
- [ ] Archive enforcement works
- [ ] OR/AND precedence is correct

---

## Scenario 4: Policy Update and Cache Refresh

**Objective:** Verify cache coherence during policy updates

**Setup:**
- Initial policy: `owner == 'admin'` (only admin sees data)
- Create documents as admin

**Workflow:**
1. Admin queries - sees all data
2. RootCoord updates policy: change to `owner == $current_user_name`
3. RootCoord broadcasts RefreshRLSCacheRequest to all proxies
4. Proxy receives refresh notification
5. Cache refresh handler invalidates collection cache
6. Next query triggers cache reload
7. New expression: `owner == 'alice'`

**Verification:**
- [ ] Policy updated in etcd
- [ ] Cache invalidation triggered
- [ ] Admin sees data with new policy
- [ ] Regular users see data based on new policy
- [ ] No stale cache serving

---

## Scenario 5: Insert and Upsert Validation

**Objective:** Verify INSERT/UPSERT operations respect RLS CHECK expressions

**Setup:**
- Collection "user_profiles"
- Policy with CHECK expr: `user_role == 'admin' OR created_by == $current_user_name`

**Workflow 1: Valid Insert**
1. Admin inserts record with user_role='admin'
   - Insert interceptor validates
   - CHECK expression: (user_role == 'admin') evaluates true
   - Insert allowed ✓

2. Regular user inserts record with created_by='alice'
   - INSERT interceptor validates
   - CHECK expression: (created_by == 'alice') evaluates true
   - Insert allowed ✓

**Workflow 2: Invalid Insert**
1. Regular user inserts with user_role='admin', created_by='bob'
   - CHECK expression: (admin OR bob) evaluates false for alice
   - Insert denied ✗

**Verification:**
- [ ] Admin inserts succeed
- [ ] User inserts succeed for own created records
- [ ] User inserts fail for privileged records
- [ ] CHECK expression enforcement works

---

## Scenario 6: Delete with Role-Based Policies

**Objective:** Verify DELETE respects both user filters and RLS

**Setup:**
- Collection "logs"
- Two policies:
  1. Permissive (employee): `owner == $current_user_name`
  2. Restrictive: `log_level != 'CRITICAL'`

**Workflow:**
1. Employee deletes with filter: `timestamp < '2024-01-01'`
2. Delete interceptor merges:
   - User filter: (timestamp < '2024-01-01')
   - Permissive: (owner == 'alice')
   - Restrictive: (log_level != 'CRITICAL')
   - Final: (timestamp < '2024-01-01') AND (owner == 'alice') AND (log_level != 'CRITICAL')
3. Only matching logs deleted

**Verification:**
- [ ] User filter preserved
- [ ] RLS filter applied
- [ ] CRITICAL logs protected
- [ ] Owner enforcement works
- [ ] Correct records deleted

---

## Scenario 7: Search with Vector Filters

**Objective:** Verify RLS works with vector search

**Setup:**
- Collection "embeddings" with embedding field
- Policy: `department == $current_user_tags['dept']`
- User alice with dept=engineering

**Workflow:**
1. Vector search for similar embeddings (score > 0.8)
2. Search interceptor applies RLS:
   - Search filter: (score > 0.8)
   - RLS filter: (department == 'engineering')
   - Merged: (score > 0.8) AND (department == 'engineering')
3. QueryNode applies compound filter on search results
4. Only engineering embeddings with high score returned

**Verification:**
- [ ] Vector search works with RLS
- [ ] Score threshold respected
- [ ] Department filtering applied
- [ ] Results combine both constraints

---

## Scenario 8: Hybrid Search with RLS

**Objective:** Verify RLS applied to all hybrid search sub-queries

**Setup:**
- Collection with multiple search fields
- Policy: `category == 'public' OR owner == $current_user_name`

**Workflow:**
1. Hybrid search with 2 sub-queries:
   - Sub1: (vector_score > 0.8)
   - Sub2: (text_similarity > 0.7)
2. Search interceptor applies RLS to each:
   - Sub1: (vector_score > 0.8) AND (category == 'public' OR owner == 'alice')
   - Sub2: (text_similarity > 0.7) AND (category == 'public' OR owner == 'alice')
3. Both sub-searches executed with RLS
4. Results merged respecting RLS constraints

**Verification:**
- [ ] All sub-queries get RLS applied
- [ ] Consistent filtering across searches
- [ ] Results respect RLS for all sub-queries

---

## Scenario 9: Admin Bypass (if configured)

**Objective:** Verify admin users can bypass or have permissive RLS

**Setup:**
- Restrictive policy: `archived == false`
- Two roles: "admin" and "user"

**Workflow:**
1. Admin queries with specific admin_bypass policy
2. No RLS filter applied (or apply "true" policy)
3. All records visible including archived
4. Regular user sees only non-archived records

**Verification:**
- [ ] Admin sees all records
- [ ] User sees filtered records
- [ ] Role-based behavior works
- [ ] Policies properly applied per role

---

## Scenario 10: Concurrent Operations

**Objective:** Verify RLS handles concurrent access safely

**Setup:**
- Collection with multiple RLS policies
- 5 concurrent users: alice, bob, charlie, david, eve

**Workflow:**
1. All 5 users simultaneously:
   - Query collection
   - Insert own records
   - Search with vector
   - Delete own records
2. Each operation applies correct RLS
3. No interference between users

**Verification:**
- [ ] All operations complete successfully
- [ ] Each user sees only their data
- [ ] No cross-user data leakage
- [ ] Insert/delete operations respect RLS
- [ ] Cache handles concurrent loads

---

## Scenario 11: Policy Enforcement Error Handling

**Objective:** Verify graceful handling of policy evaluation errors

**Setup:**
- Collection with complex policy
- Inject policy with invalid expression

**Workflow:**
1. Invalid policy created (if validation allows)
2. Proxy attempts to build expression
3. Expression builder detects error
4. Request denied with error message
5. Operation fails safely without exposing data

**Verification:**
- [ ] Errors caught at expression building
- [ ] No data leak on error
- [ ] User gets clear error message
- [ ] System remains stable

---

## Scenario 12: Cache Invalidation on Policy Update

**Objective:** Verify cache stays in sync when policies change

**Setup:**
- Initial policy P1 in cache
- 10 concurrent queries

**Workflow:**
1. RootCoord updates policy to P2
2. Broadcasts cache refresh to all proxies
3. Some proxies already executing with P1
   - Queries complete with P1 (acceptable)
4. New queries load P2
5. Cache rebuild completes

**Verification:**
- [ ] Old queries complete with old policy
- [ ] New queries use new policy
- [ ] No data inconsistency
- [ ] Cache refresh completes quickly

---

## Test Execution Checklist

### Unit Tests
- [ ] RLS policy validation
- [ ] Expression building (permissive, restrictive, mixed)
- [ ] Variable substitution
- [ ] Cache operations
- [ ] Interceptor logic

### Integration Tests
- [ ] Policy storage and retrieval
- [ ] Cache synchronization
- [ ] Multi-interceptor coordination
- [ ] Error handling

### E2E Tests
- [ ] Complete workflows (scenarios above)
- [ ] Multi-user scenarios
- [ ] Policy lifecycle
- [ ] Cache refresh
- [ ] Concurrent operations

### Performance Tests
- [ ] Expression building latency
- [ ] Cache hit rate
- [ ] Query throughput with RLS
- [ ] Memory usage under load

### Security Tests
- [ ] SQL injection in expressions
- [ ] Privilege escalation
- [ ] Data leakage scenarios
- [ ] Expression bypass attempts

---

## Expected Outcomes

1. **Functional Correctness**: RLS correctly filters data based on policies
2. **Data Isolation**: Users cannot access unauthorized data
3. **Performance**: RLS adds minimal overhead (< 5% latency increase)
4. **Reliability**: Graceful error handling, no data leaks on failure
5. **Scalability**: Works with thousands of policies and concurrent users

---

## References

- RLS Implementation Plan: `/Users/xiaofanluan/code/agent/milvus/docs/plans/2026-01-20-rls-implementation.md`
- RLS Policy Model: `internal/metastore/model/rls_policy.go`
- Expression Builder: `internal/proxy/rls_expr_builder.go`
- Interceptors: `internal/proxy/rls_*_interceptor.go`
