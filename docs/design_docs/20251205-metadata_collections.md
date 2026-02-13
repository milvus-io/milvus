# MEP: Simple Joins for Vector Search

**Current state**: Under Discussion
**ISSUE**: https://github.com/milvus-io/milvus/issues/46153
**Keywords**: join, normalization, nested loop join
**Released**: TBD

---

## Executive Summary

**What**: Introduce simple JOIN syntax for combining ANN search results with data from other collections.

**Why**: Enable normalized data modeling. Users currently must denormalize metadata across vector records, causing inefficient updates and storage waste.

**How**: Implement JOIN as a post-search operation that fetches and merges data from other collections using nested loop joins with bounded result sets.

**Impact**: Zero breaking changes to existing collections - purely additive feature

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Collection Validation** | Allow zero or more vector fields | Natural extension - collection without vectors is just a collection |
| **No Special Types** | Single `Collection` type | If collection has no vector field, search() returns clear error |
| **Join Implementation** | Post-search nested loop join | KNN result set bounds the loop size, predictable performance |
| **Join Strategy** | Nested loop join only (Phase 1) | Simple, safe, predictable - no complex query planning needed |
| **Join Size Limit** | Error if joined collection side is too large | Prevents OOM and unpredictable latency |
| **Backward Compatibility** | Existing collections work exactly as before | Validation relaxation + JOIN are purely additive |

---

## For Reviewers

**Main Questions for Review:**
1. Is the JOIN syntax after ANN search acceptable?
2. Are nested loop joins with size limits the right initial constraint?
3. Are there concerns with the implementation approach?

**Quick Review Path:**
- Read: [Executive Summary](#executive-summary) + [Key Design Decisions](#key-design-decisions) (2 min)
- Read: [Public Interfaces](#public-interfaces) - see the API design (5 min)
- Skim: [Design Details](#design-details) - implementation approach (5 min)
- Check: [Implementation Checklist](#implementation-checklist) - rollout plan (2 min)

---

## Table of Contents

1. [Summary](#summary)
2. [Motivation](#motivation) - Real-world use cases and pain points
3. [Public Interfaces](#public-interfaces) - API design and SDK examples
4. [Design Details](#design-details) - Implementation architecture
5. [Compatibility](#compatibility-deprecation-and-migration-plan) - Zero breaking changes
6. [Test Plan](#test-plan) - Comprehensive testing strategy
7. [Implementation Checklist](#implementation-checklist) - Rollout plan

---

## Summary

Two complementary changes:

**1. Allow Collections Without Vector Fields**
- Relax validation to allow zero or more vector fields
- No special type - it's just a `Collection` that happens to have no vectors
- `search()` on such collections returns clear error: "search() requires vector fields"
- All other operations work: `insert()`, `upsert()`, `delete()`, `query()`

**2. JOIN Syntax After Search**
- New `join` parameter in search API
- Syntax: `collection.search(..., join="JOIN other_collection ON key = key WHERE condition")`
- Uses nested loop join - safe because KNN topK bounds the outer loop size
- Returns error if joined collection side exceeds size limit (prevents OOM)

**Backward Compatibility**
- Existing collections: 100% unchanged behavior
- Existing APIs: Continue to work as-is

## Motivation

### Current Problem

Milvus currently requires users to denormalize metadata into vector collections, leading to significant challenges:

**Use Case Example**: E-commerce Recommendation Platform

**Entity Relationships**:
- **Shop** → Items (1:many)
- **Merchant** → Items (1:many)
- Items have embeddings for similarity search and recommendations

**Frequently Changing Attributes**:
- **Shop metadata**: status (active/inactive), rating, region, featured_flag, promotion_tier
- **Merchant metadata**: verification_status, reputation_score, response_time, fulfillment_rate

**Current Forced Solution**: Denormalize all metadata into item records
```python
# Current denormalized schema
items_collection = {
    "item_id": int64,
    "item_embedding": float_vector,  # Required for search

    # Shop metadata (denormalized)
    "shop_id": int64,
    "shop_status": varchar,
    "shop_rating": float,
    "shop_region": varchar,

    # Merchant metadata (denormalized)
    "merchant_id": int64,
    "merchant_verification": varchar,
    "merchant_reputation": float,
}
```

**Pain Points**:
1. **Update Inefficiency**:
   - Changing a shop's status requires updating many item records
   - Batch updates (e.g., region-wide shop promotions) cascade to many items

2. **Storage Overhead**:
   - Redundant metadata duplicated across many items
   - Each item stores multiple entities' worth of denormalized attributes

3. **Consistency Issues**:
   - Risk of stale/inconsistent data during bulk updates
   - Temporal inconsistency: Shop status changed but half the items still show old status

4. **Maintenance Burden**:
   - Complex application logic to manage denormalized updates
   - Background jobs to sync metadata changes across collections
   - Difficult to debug data quality issues

**Example Impact**:
- **Shop updates**: Shop with many items → Changing shop status requires many upsert operations
- **Merchant updates**: Merchant with many items → Reputation score update requires many upserts
- **Storage overhead**: Significant redundant storage for denormalized fields vs normalized design

### Proposed Solution

**JOIN Syntax**: Add explicit JOIN clause that executes after ANN search, using nested loop joins with bounded result sets.

This approach uses nested loop joins specifically because the KNN result set bounds the outer loop size, making performance predictable.

**Normalized Schema**:
```python
# Items collection - ONLY item-specific data + embeddings
items_collection = {
    "item_id": int64,
    "item_name": varchar,
    "item_embedding": float_vector,  # For similarity search
    "shop_id": int64,       # Reference only
    "merchant_id": int64,   # Reference only
    "price": float,
    "category": varchar
}

# Shop collection - no vector fields needed
shop_collection = {
    "shop_id": int64,       # Primary key
    "shop_name": varchar,
    "shop_status": varchar,
    "shop_rating": float,
    "shop_region": varchar,
    "featured_flag": bool,
    "promotion_tier": int64
    # No vector field - this is fine now
}

# Merchant collection - no vector fields needed
merchant_collection = {
    "merchant_id": int64,           # Primary key
    "merchant_name": varchar,
    "verification_status": varchar,
    "reputation_score": float,
    "response_time_hours": int64,
    "fulfillment_rate": float
    # No vector field - this is fine now
}
```

**Benefits**:
1. **Normalized Data Model**: Store shop/merchant metadata once in dedicated collections
2. **Efficient Updates**:
   - Update shop status: 1 operation (not thousands)
   - Update merchant reputation: 1 operation (not thousands)
3. **Reduced Storage**: Eliminate redundant denormalized data
4. **Explicit JOIN Syntax**: Clear, SQL-like semantics that developers understand
5. **Predictable Performance**: Nested loop join bounded by KNN topK - no surprise costs
6. **Safe by Design**: Error returned if joined collection side too large

### Real-World Use Cases

1. **E-commerce Recommendation Systems** (primary motivating example):
   - Item embeddings for similarity search
   - Shop/merchant metadata (status, ratings, verification) - changes frequently
   - **Impact**: Significantly faster metadata updates and storage reduction

2. **Multi-tenant SaaS**:
   - Application data with embeddings for semantic search
   - Tenant configurations (billing tier, feature flags, quotas)
   - **Impact**: Efficient tenant metadata management

3. **Content Platforms (social media, streaming)**:
   - Content embeddings for recommendations
   - Creator metadata (verification status, subscriber count, content rating)
   - **Impact**: Real-time metadata updates without touching millions of content records

4. **IoT and Smart Devices**:
   - Sensor data embeddings for anomaly detection
   - Device metadata (location, model, firmware version, status) - updated frequently
   - **Impact**: Efficient device fleet management with metadata updates

## Public Interfaces

### Collections Without Vector Fields

Collections can now be created without vector fields. No special type - just a regular `Collection`:

```python
from pymilvus import CollectionSchema, FieldSchema, DataType, Collection

# Collection without vector fields - now allowed
shop_schema = CollectionSchema([
    FieldSchema("shop_id", DataType.INT64, is_primary=True),
    FieldSchema("shop_name", DataType.VARCHAR, max_length=256),
    FieldSchema("shop_status", DataType.VARCHAR, max_length=50),
    FieldSchema("shop_rating", DataType.FLOAT),
], description="Shop data")

# Same Collection class - validation now allows zero vector fields
shop_collection = Collection("shop_collection", schema=shop_schema)

# All operations work
shop_collection.insert([[1, 2, 3],
                        ["Shop A", "Shop B", "Shop C"],
                        ["active", "active", "inactive"],
                        [4.5, 4.8, 3.2]])

results = shop_collection.query(
    expr="shop_status == 'active' AND shop_rating > 4.0",
    output_fields=["shop_id", "shop_name"]
)

# search() returns clear error
# shop_collection.search(...)  # Error: "search() requires a collection with vector fields"
```

### JOIN Syntax After ANN Search

The JOIN clause executes after the ANN search completes, using a nested loop join strategy. This is safe because the KNN result set (topK) bounds the outer loop size.

```python
# Current Milvus search API (today):
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},
    expr="price >= 10.0 AND price <= 100.0",      # Filter on same collection
    output_fields=["item_id", "price", "shop_id"]  # Fields from same collection
)

# NEW: Search with JOIN clause (proposed)
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},
    expr="price >= 10.0 AND price <= 100.0",

    # NEW: JOIN clause - executes AFTER ANN search completes
    join="JOIN shop_collection ON shop_id = shop_collection.shop_id WHERE shop_status == 'active' AND shop_rating > 4.0",

    # RETURN fields from both collections
    output_fields=["item_id", "price", "shop_collection.shop_name", "shop_collection.shop_rating"]
)

# Results include data from both collections
for hit in results[0]:
    print(f"Item: {hit.entity['item_id']}, Price: {hit.entity['price']}")
    print(f"Shop: {hit.entity['shop_collection.shop_name']} (rating: {hit.entity['shop_collection.shop_rating']})")
```

**JOIN Grammar**:

```
JOIN <collection_name> ON <join_key> = <collection_name>.<join_key> WHERE <filter_condition>
```

**Examples:**
```python
# Simple join: Get shop info for search results
join="JOIN shop_collection ON shop_id = shop_collection.shop_id"

# Join with filter: Only active shops with high rating
join="JOIN shop_collection ON shop_id = shop_collection.shop_id WHERE shop_status == 'active' AND shop_rating > 4.0"

# Multiple joins: Get both shop and merchant info
join="""JOIN shop_collection ON shop_id = shop_collection.shop_id WHERE shop_status == 'active'
        JOIN merchant_collection ON merchant_id = merchant_collection.merchant_id WHERE verification_status == 'verified'"""
```

**How It Works - Nested Loop Join After ANN Search**:

```python
# Execution flow:
# 1. Execute ANN search → Get topK results (e.g., 100 items)
# 2. Extract join keys from results (e.g., shop_ids: [101, 205, 387, ...])
# 3. Query joined collection with extracted keys + WHERE filter
# 4. Nested loop join: For each search result, find matching joined data
# 5. Return combined results

# Why nested loop is safe:
# - Outer loop: bounded by topK (e.g., 100)
# - Inner loop: bounded by matching records
# - Total operations: O(topK * avg_matches) - predictable and bounded
```

**Size Limit Protection**:

To prevent OOM and unpredictable latency, the join returns an error if the joined collection side is too large:

```python
# Configuration parameter
proxy:
  maxJoinResultSize: 10000  # Default limit for joined result size

# Error example:
# "JOIN failed: query on 'shop_collection' returned 50,000 matching records,
#  exceeding limit of 10,000. Use a more selective WHERE clause or increase limit."
```

**Benefits of JOIN Syntax**:
- **Explicit semantics**: Clear SQL-like JOIN syntax familiar to developers
- **Predictable performance**: Nested loop bounded by KNN topK
- **Safe by design**: Error returned if joined side too large
- **Post-search execution**: JOIN runs after ANN, doesn't affect vector search performance
- **Single API call**: No multiple round trips from application to Milvus

**Update Efficiency Example**:
```python
# Scenario: Shop updates their status

# OLD WAY (denormalized): Update all item records for this shop
# If shop has 10,000 items, need 10,000 upsert operations
old_way_updates = 10000

# NEW WAY (normalized with JOIN): Single update
shop_collection.upsert([{
    "shop_id": 123,
    "shop_status": "inactive"
}])
new_way_updates = 1  # Much more efficient!

# Next search with JOIN automatically uses latest shop data
# No sync lag, no stale data, no complex update logic
```

### Configuration

```yaml
proxy:
  # Maximum number of records returned from joined collection
  # Returns error if exceeded - prevents OOM and unpredictable latency
  maxJoinResultSize: 10000
```

## Design Details

### Architecture Summary

The JOIN feature is implemented as a **post-search operation** that queries the joined collection and merges results.

**Current Flow (Manual Two-Pass)**:
```
Application does:
1. Search vector collection → Get topK results
2. Extract IDs from results
3. Query other collection with those IDs
4. Manually merge results in application

Requires 2+ round trips from application to Milvus
```

**New Flow (JOIN in Single Call)**:
```
Application sends: Search with join="JOIN other_collection ON key = key WHERE condition"

Milvus executes:
1. Execute ANN search → Get topK results
2. Extract join keys from results
3. Query joined collection with keys + WHERE filter
4. Nested loop merge results
5. Return combined results

Single round trip from application to Milvus
```

**Why Nested Loop is Safe**:
- Results bounded by topK from search
- Outer loop: bounded by topK (e.g., 100)
- Inner loop: bounded by matching records
- Size limit: error if joined side exceeds threshold

### 1. Relax Collection Validation

**File**: `internal/proxy/task.go`

Remove the vector field requirement:

```go
func (t *createCollectionTask) validateSchema() error {
    vectorFields := len(typeutil.GetVectorFieldSchemas(t.schema))

    if vectorFields > Params.ProxyCfg.MaxVectorFieldNum.GetAsInt() {
        return fmt.Errorf("maximum vector field's number should be limited to %d",
            Params.ProxyCfg.MaxVectorFieldNum.GetAsInt())
    }

    // REMOVED: Vector field requirement
    // if vectorFields == 0 {
    //     return merr.WrapErrParameterInvalidMsg("schema does not contain vector field")
    // }

    return nil
}
```

**File**: `internal/proxy/task_search.go`

Add runtime check to reject search on collections without vectors:

```go
func (t *searchTask) PreExecute(ctx context.Context) error {
    // ... existing validation ...

    // NEW: Check if collection has vector fields
    vectorFields := typeutil.GetVectorFieldSchemas(t.schema)
    if len(vectorFields) == 0 {
        return merr.WrapErrParameterInvalidMsg(
            "search() requires a collection with vector fields. " +
            "Use query() instead.")
    }

    // ... rest of existing PreExecute ...
}
```

### 2. Protobuf Changes

**File**: `pkg/proto/milvuspb/milvus.proto`

**Add join parameter to SearchRequest**:
```protobuf
message SearchRequest {
  string collection_name = 2;
  string dsl = 5;                      // Filter expression (unchanged)
  repeated string output_fields = 8;   // Extended to support qualified names like "collection.field"
  repeated KeyValuePair search_params = 9;
  bytes placeholder_group = 10;
  string join_clause = 15;             // NEW: JOIN clause for post-search joins
  // ... other fields
}
```

### 3. JOIN Execution in Search Task

**File**: `internal/proxy/task_search.go`

The JOIN is executed in the PostExecute phase after search results are available:

```go
func (t *searchTask) PostExecute(ctx context.Context) error {
    // ... existing post-execute logic ...

    // NEW: Execute JOIN if specified
    if t.request.GetJoinClause() != "" {
        if err := t.executeJoin(ctx); err != nil {
            return err
        }
    }

    // ... rest of existing post-execute ...
}

func (t *searchTask) executeJoin(ctx context.Context) error {
    // Parse JOIN clause
    joinSpec, err := parseJoinClause(t.request.GetJoinClause())
    if err != nil {
        return err
    }

    // Extract join keys from search results
    joinKeys := t.extractJoinKeys(joinSpec.LocalKeyField)
    if len(joinKeys) == 0 {
        return nil  // No results to join
    }

    // Build query for joined collection
    queryExpr := fmt.Sprintf("%s in %v", joinSpec.ForeignKeyField, joinKeys)
    if joinSpec.WhereExpr != "" {
        queryExpr = fmt.Sprintf("(%s) AND (%s)", queryExpr, joinSpec.WhereExpr)
    }

    // Execute query on joined collection
    queryReq := &milvuspb.QueryRequest{
        CollectionName: joinSpec.CollectionName,
        Expr:           queryExpr,
        OutputFields:   joinSpec.OutputFields,
    }
    queryResults, err := t.node.(*Proxy).Query(ctx, queryReq)
    if err != nil {
        return err
    }

    // Check size limit
    resultCount := getResultCount(queryResults)
    if resultCount > Params.ProxyCfg.MaxJoinResultSize.GetAsInt64() {
        return fmt.Errorf(
            "JOIN failed: query on '%s' returned %d records, "+
            "exceeding limit of %d. Use a more selective WHERE clause.",
            joinSpec.CollectionName, resultCount, Params.ProxyCfg.MaxJoinResultSize.GetAsInt64())
    }

    // Nested loop join: merge results
    t.mergeJoinResults(queryResults, joinSpec)
    return nil
}

func (t *searchTask) mergeJoinResults(queryResults *milvuspb.QueryResults, joinSpec *JoinSpec) {
    // Build lookup map for O(1) access
    lookupMap := buildLookupMap(queryResults, joinSpec.ForeignKeyField)

    // For each search result, find and merge matching joined data
    for i, hit := range t.result.Results.Hits {
        localKey := getFieldValue(hit, joinSpec.LocalKeyField)
        if joinedData, ok := lookupMap[localKey]; ok {
            // Add joined fields with qualified names (e.g., "shop_collection.shop_name")
            for fieldName, value := range joinedData {
                qualifiedName := fmt.Sprintf("%s.%s", joinSpec.CollectionName, fieldName)
                t.result.Results.Hits[i].Fields[qualifiedName] = value
            }
        }
    }
}
```

### 4. JOIN Clause Parser

**File**: `internal/proxy/join_parser.go`

```go
type JoinSpec struct {
    CollectionName  string
    LocalKeyField   string
    ForeignKeyField string
    WhereExpr       string
    OutputFields    []string
}

func parseJoinClause(joinClause string) (*JoinSpec, error) {
    // Parse: JOIN <collection> ON <local_key> = <collection>.<foreign_key> WHERE <condition>
    // Returns structured JoinSpec
    // ...
}
```

### 5. Multiple JOINs Support

For multiple JOINs, execute them sequentially:

```go
func (t *searchTask) executeJoins(ctx context.Context) error {
    joinSpecs, err := parseJoinClauses(t.request.GetJoinClause())
    if err != nil {
        return err
    }

    for _, joinSpec := range joinSpecs {
        if err := t.executeSingleJoin(ctx, joinSpec); err != nil {
            return err
        }
    }
    return nil
}
```

**Limitations (Phase 1)**:
- Nested loop join only (no hash join, sort-merge join)
- Error if joined collection side exceeds size limit
- No aggregations - just field lookups
- Inner join semantics only

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility

**Impact**: None. This is a purely additive feature.

- Existing collections: No changes, work exactly as before
- Existing APIs: Fully compatible, no breaking changes
- New `join` parameter: Optional, existing searches work unchanged

### Migration Path

No migration needed. Users can:
1. Continue using denormalized patterns (existing behavior)
2. Create separate collections for normalized data
3. Use JOIN syntax to combine search results with other collections

### Configuration Rollout

```yaml
proxy:
  # Maximum size of results from joined collection
  # Returns error if exceeded - prevents OOM and unpredictable latency
  maxJoinResultSize: 10000  # Default
```

## Test Plan

### Unit Tests

**Collections Without Vector Fields**:
1. **Test**: Create collection with zero vector fields
   - **Expected**: Success (validation relaxed)

2. **Test**: search() on collection without vectors
   - **Expected**: Clear error: "search() requires a collection with vector fields"

3. **Test**: query() on collection without vectors
   - **Expected**: Success - returns filtered results

4. **Test**: insert/upsert/delete on collection without vectors
   - **Expected**: All operations succeed

**JOIN Tests**:
5. **Test**: Parse JOIN clause with various formats
   - **Expected**: Correctly extracts collection name, keys, and WHERE clause

6. **Test**: Execute JOIN with valid collections
   - **Expected**: Returns merged results

7. **Test**: JOIN with non-existent collection
   - **Expected**: Clear error message

8. **Test**: JOIN exceeding size limit
   - **Expected**: Error with clear message about size limit

9. **Test**: Multiple JOINs
   - **Expected**: All joins executed and merged correctly

### Integration Tests

**File**: `tests/python_client/testcases/test_collection_no_vector.py`

```python
class TestCollectionWithoutVector:
    def test_create_collection_no_vector(self):
        """Test creating collection without vector fields"""
        schema = CollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True),
            FieldSchema("name", DataType.VARCHAR, max_length=100),
            FieldSchema("status", DataType.VARCHAR, max_length=50),
        ])
        coll = Collection("no_vector_test", schema=schema)
        assert coll is not None

    def test_crud_no_vector(self):
        """Test CRUD operations on collection without vectors"""
        # Insert
        coll.insert([[1, 2, 3], ["a", "b", "c"], ["active", "active", "inactive"]])

        # Query
        results = coll.query(expr="status == 'active'", output_fields=["id", "name"])
        assert len(results) == 2

        # Upsert
        coll.upsert([[1], ["updated"], ["inactive"]])

        # Delete
        coll.delete("id == 3")

    def test_search_returns_error(self):
        """Test that search() returns clear error"""
        with pytest.raises(MilvusException) as exc:
            coll.search(data=[[0.1]*128], anns_field="vector", limit=10)
        assert "requires a collection with vector fields" in str(exc.value)
```

**File**: `tests/python_client/testcases/test_join.py`

```python
class TestJoin:
    def test_simple_join(self):
        """Test basic JOIN syntax"""
        results = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=100,
            output_fields=["item_id", "shop_id", "shop_collection.shop_name"],
            join="JOIN shop_collection ON shop_id = shop_collection.shop_id"
        )

        # Verify join worked - results should have joined fields
        for hit in results[0]:
            assert "shop_collection.shop_name" in hit.entity

    def test_join_with_filter(self):
        """Test JOIN with WHERE clause"""
        results = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=100,
            output_fields=["item_id", "shop_collection.shop_name", "shop_collection.shop_rating"],
            join="JOIN shop_collection ON shop_id = shop_collection.shop_id WHERE shop_status == 'active' AND shop_rating > 4.0"
        )

        # Verify filter was applied
        for hit in results[0]:
            assert hit.entity.get("shop_collection.shop_rating", 0) > 4.0

    def test_multiple_joins(self):
        """Test joining with multiple collections"""
        results = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=50,
            output_fields=["item_id", "shop_collection.shop_name", "merchant_collection.verification_status"],
            join="""JOIN shop_collection ON shop_id = shop_collection.shop_id WHERE shop_status == 'active'
                    JOIN merchant_collection ON merchant_id = merchant_collection.merchant_id WHERE verification_status == 'verified'"""
        )

        # Verify both joins worked
        for hit in results[0]:
            assert "shop_collection.shop_name" in hit.entity
            assert "merchant_collection.verification_status" in hit.entity

    def test_join_size_limit_error(self):
        """Test that JOIN returns error when result too large"""
        with pytest.raises(MilvusException) as exc:
            item_collection.search(
                data=[embedding],
                anns_field="embedding",
                limit=100,
                join="JOIN large_collection ON key = large_collection.key"  # No WHERE filter
            )
        assert "exceeding limit" in str(exc.value)

    def test_update_efficiency(self):
        """Test that updating joined collection is O(1) operation"""
        import time

        # Measure update time (should be fast - single record)
        start = time.time()
        shop_collection.upsert([{"shop_id": 123, "shop_status": "inactive"}])
        update_time = time.time() - start

        # Should complete quickly
        assert update_time < 0.1, f"Update took {update_time}s, expected to be fast"
```

### E2E Tests

| Test Case | Expected Behavior |
|:----------|:-----------------|
| Create collection without vector fields | Success |
| query() on collection without vectors | Returns correct results |
| search() on collection without vectors | Returns clear error message |
| Simple JOIN | Returns combined data correctly |
| JOIN with WHERE clause | Filter applied correctly |
| Multiple JOINs | All collections joined correctly |
| JOIN exceeds size limit | Returns clear error with limit info |
| JOIN with non-existent collection | Returns appropriate error |
| JOIN with invalid key field | Returns appropriate error |
| Concurrent searches with JOIN | No conflicts, consistent results |

### Performance Tests

**Benchmark Scenarios**:

1. **Update Efficiency**: Compare denormalized vs normalized patterns
   - **Denormalized**: Update many item records when shop status changes
   - **Normalized with JOIN**: Update 1 shop record
   - **Expected**: Significantly faster with normalized approach

2. **JOIN Performance**: Measure JOIN latency
   - **Nested loop join overhead**: Measure time added by JOIN clause
   - **Multiple JOINs**: Measure cumulative overhead
   - **Expected**: O(topK * matches) - bounded and predictable

3. **Size Limit Validation**:
   - Test with varying joined collection sizes
   - Verify error is returned promptly when limit exceeded
   - **Expected**: Fast fail, no OOM

## Rejected Alternatives

### Alternative 1: General-Purpose SQL JOIN Support

**Idea**: Implement full SQL-style JOIN operations (LEFT, RIGHT, OUTER, nested joins, aggregations across joins, etc.)

**Rejected Because**:
- Too complex for a vector database - conflicts with Milvus's vector-first design
- Massive architectural change to query planner and execution engine
- Distributed join complexity (shuffle, broadcast, network overhead across multiple nodes)
- Performance unpredictable and difficult to optimize in distributed settings
- Would require query optimizer, cost-based planning, join reordering, etc.

**What We're Doing Instead**: Simple nested loop joins after ANN search
- Limited to simple equality joins (inner join only)
- Nested loop bounded by KNN topK - predictable performance
- Error if joined side too large - prevents OOM
- Keeps implementation simple and maintainable

### Alternative 2: JOIN as Pipeline Operator (Requery Pattern)

**Idea**: Implement JOIN as a pipeline operator in the search pipeline (`search_pipeline.go`), following the existing `requeryOperator` pattern.

Pipeline would be: `[search] → [reduce] → [requery] → [join] → [organize] → [end]`

**Rejected Because**:
- Adds unnecessary complexity by coupling JOIN with the pipeline framework
- Requery fetches fields from the SAME collection; JOIN fetches from OTHER collections - different semantics
- Pipeline operators have specific contracts and state management that aren't needed for JOIN
- Simpler to implement JOIN directly in PostExecute phase of search task

**What We're Doing Instead**: Direct JOIN execution in search task PostExecute
- Cleaner separation of concerns
- Simpler implementation without pipeline overhead
- Easier to understand and maintain

### Alternative 3: Application-Level Joins Only

**Idea**: Require users to perform joins in application code (multiple queries + manual join logic).

**Rejected Because**:
- Poor developer experience - requires complex application logic
- Multiple round trips to Milvus (higher network overhead)
- Error-prone manual join implementation

**What We're Doing Instead**: JOIN syntax inside Milvus
- Single query API with JOIN clause - cleaner developer experience
- Nested loop join executed in proxy - safe and predictable
- Lower latency due to reduced network round trips

### Alternative 4: External Database for Metadata

**Idea**: Use PostgreSQL/MySQL for metadata, only use Milvus for vectors.

**Rejected Because**:
- Additional infrastructure complexity
- More network round-trips
- Data consistency challenges across two systems
- Milvus already has excellent scalar query capabilities
- Users want unified data management

### Alternative 5: Always Denormalize

**Idea**: Accept denormalization as the Milvus way, optimize bulk updates instead.

**Rejected Because**:
- Doesn't solve fundamental data modeling mismatch
- Update efficiency limits still exist
- Storage overhead remains problematic at scale
- Users already requesting normalized patterns
- Limits Milvus applicability for relational use cases

## References

1. **Current Search API Architecture**:
   - External: `github.com/milvus-io/milvus-proto` - Client-facing SearchRequest protobuf definition
   - Internal: `pkg/proto/internal.proto:105-125` - Internal SearchRequest protobuf
   - `internal/proxy/task_search.go` - Search task implementation, PreExecute/Execute/PostExecute flow
   - `internal/parser/planparserv2/plan_parser_v2.go` - Expression parsing (CreateSearchPlan)

2. **Query vs Search Architecture**:
   - `docs/design_docs/20221221-retrieve_entity.md`
   - `internal/proxy/task_query.go` - Scalar-only queries
   - `internal/proxy/task_search.go` - Vector searches

3. **Expression Grammar**:
   - `docs/design_docs/20220105-query_boolean_expr.md` - Boolean expression grammar
   - `internal/parser/planparserv2/` - Expression parser implementation

---

## Implementation Checklist

### Validation Changes
- [ ] Modify `internal/proxy/task.go` to allow zero vector fields in schema
- [ ] Add runtime check in `internal/proxy/task_search.go` to reject search on collections without vectors

### Protobuf Changes
- [ ] Add `join_clause` field to SearchRequest in `milvus.proto`

### Parser Changes (`internal/proxy/`)
- [ ] Create `join_parser.go` with JOIN clause parsing functions
- [ ] Implement `parseJoinClause()` to extract JOIN clauses
- [ ] Support syntax: `JOIN <collection> ON <key> = <collection>.<key> WHERE <condition>`
- [ ] Add validation for join collections (existence, field validity)

### Search Task Changes (`internal/proxy/task_search.go`)
- [ ] Add `joinClauses` field to `searchTask` struct
- [ ] Extend `PreExecute()` to parse JOIN clause and validate
- [ ] Implement `executeJoin()` in `PostExecute()` phase
- [ ] Implement `mergeJoinResults()` for result merging
- [ ] Add size limit check with clear error message

### Configuration
- [ ] Add `maxJoinResultSize` configuration parameter (default: 10,000)

### SDK Changes
- [ ] Python SDK: Add `join` parameter to `search()` method
- [ ] Python SDK: Documentation for JOIN grammar
- [ ] Go SDK: Add `Join` parameter to search request
- [ ] Java SDK: Add join parameter
- [ ] Node.js SDK: Add join parameter
- [ ] RESTful API: Add `join` field to search endpoint

### Testing
**Collections Without Vector Fields**:
- [ ] Unit test: Create collection with zero vector fields succeeds
- [ ] Unit test: search() on collection without vectors returns clear error
- [ ] Unit test: query/insert/upsert/delete work on collections without vectors
- [ ] Integration test: Full CRUD operations on collection without vectors

**JOIN Tests**:
- [ ] Unit tests for JOIN clause parsing
- [ ] Unit tests for nested loop join execution
- [ ] Unit tests for size limit enforcement
- [ ] Integration tests for single JOIN
- [ ] Integration tests for multiple JOINs
- [ ] Integration tests for JOIN with WHERE clause
- [ ] Integration tests for JOIN exceeding size limit (error case)
- [ ] E2E tests for e-commerce scenario with JOINs
- [ ] Performance tests for JOIN overhead
- [ ] Backward compatibility tests (existing searches unaffected)

### Documentation
- [ ] User guide: JOIN syntax after ANN search
- [ ] User guide: Grammar specification
- [ ] User guide: Size limits and error handling
- [ ] API reference: Updated Collection.search() with JOIN examples
- [ ] Tutorial: E-commerce multi-entity search with JOINs
- [ ] Best practices: Writing efficient WHERE clauses
- [ ] Best practices: Indexing join keys
- [ ] Performance guide: JOIN overhead and optimization tips

### Release
- [ ] Release notes entry
- [ ] Community announcement
- [ ] Blog post with e-commerce example demonstrating JOINs

---

## Summary for Reviewers

**What's being proposed:**
1. **Allow collections without vector fields**
   - Relax validation to allow zero vector fields
   - No special type - just a regular `Collection`
   - search() returns clear error on such collections

2. **JOIN as post-search operation**
   - Executed in `task_search.go` PostExecute phase
   - Syntax: `join="JOIN <collection> ON <key> = <key> WHERE <condition>"`
   - Nested loop join - safe because bounded by KNN topK
   - Error if joined collection side exceeds size limit

**What's NOT changing:**
- Existing collections with vectors: 100% unchanged behavior
- Existing search API: Continue to work as-is
- Backward compatibility: Zero breaking changes

**Key design decisions:**
- Single `Collection` type (no special "scalar collection" or "metadata collection" type)
- JOIN executed directly in search task PostExecute (not pipeline operator)
- Nested loop join only (simple, predictable)
- Size limit on joined side (prevents OOM)
- Single round trip from application

**Questions? Please comment on:** https://github.com/milvus-io/milvus/issues/46153
