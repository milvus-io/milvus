# MEP: Metadata Collections and Simple Joins

**Current state**: Under Discussion
**ISSUE**: https://github.com/milvus-io/milvus/issues/46153
**Keywords**: metadata collection, join, normalization, nested loop join
**Released**: TBD

---

## Executive Summary

**What**: Allow collections without vector fields and introduce simple JOIN syntax for combining ANN search results with metadata collections.

**Why**: Enable normalized data modeling. Users currently must denormalize metadata across vector records, causing inefficient updates and storage waste.

**How**: Relax the existing collection validation to allow zero vector fields. Implement JOIN as a **pipeline operator** in the existing hybrid search framework (`search_pipeline.go`), executing after the requery stage using nested loop joins with bounded result sets.

**Impact**: Zero breaking changes to existing collections - purely additive feature

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Type System** | Single `Collection` type (allow zero vector fields) | Simpler design - if collection has no vector field, it's a metadata collection |
| **Validation** | Relax existing validation to allow zero vectors | Natural extension, no new types needed |
| **Search Safety** | Runtime error when calling `search()` on collection without vectors | Clear error message guides users |
| **Join Implementation** | Pipeline operator (like `requeryOperator`) | Reuses existing infrastructure, composable with rerank/requery |
| **Join Strategy** | Nested loop join only (Phase 1) | KNN result set bounds the loop size, predictable performance |
| **Join Size Limit** | Error if metadata collection side is too large | Prevents OOM and unpredictable latency |
| **Backward Compatibility** | Existing collections with vectors work exactly as before | Validation relaxation is purely additive |

---

## For Reviewers

**Main Questions for Review:**
1. Do you agree with allowing collections without vector fields (vs. separate type)?
2. Is the JOIN syntax after ANN search acceptable?
3. Are nested loop joins with size limits the right initial constraint?
4. Are there concerns with the implementation approach?

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
7. [Implementation Checklist](#implementation-checklist) - Phase-by-phase rollout

---

## Summary

Allow collections to be created **without vector fields** (metadata collections) by relaxing the existing validation. Introduce **JOIN syntax** implemented as a **pipeline operator** in the existing hybrid search framework, executing after the requery stage. The join uses a **nested loop** strategy, which is safe because the ANN result set (topK) bounds the loop size.

**Building on Existing Capabilities**: This proposal (1) relaxes vector field validation to allow zero vectors, and (2) adds a `joinOperator` to the search pipeline (similar to existing `requeryOperator`) that operates on bounded KNN result sets. We're extending existing infrastructure rather than introducing fundamentally new concepts.

**Key Features**:

**Feature 1: Collections Without Vector Fields** (Metadata Collections)
- Relax existing validation to allow collections with zero vector fields
- If a collection has no vector field, it's a metadata collection
- Same `Collection` type - no new types needed
- `search()` returns a clear error on collections without vectors

**Feature 2: JOIN as Pipeline Operator**
- New `joinOperator` in search pipeline (`internal/proxy/search_pipeline.go`)
- Follows same pattern as existing `requeryOperator` - queries collection, merges results
- Pipeline: `[search] → [reduce] → [requery] → [join] → [organize] → [end]`
- Syntax: `collection.search(..., join="JOIN metadata ON key = key WHERE condition")`
- Uses nested loop join - safe because KNN topK bounds the outer loop size
- Returns error if metadata collection side exceeds size limit (prevents OOM)
- Composable with existing rerank and requery operators

**Backward Compatibility**
- Existing collections with vectors: 100% unchanged behavior
- Existing validation: Now allows zero vectors (relaxation, not restriction)
- Existing APIs: Continue to work as-is

## Motivation

### Current Problem

Milvus currently requires every collection to have at least one vector field. This constraint forces users into denormalized data models when dealing with relational patterns, leading to significant challenges:

**Use Case Example**: E-commerce Recommendation Platform

**Entity Relationships**:
- **Shop** → Items (1:many)
- **Merchant** → Items (1:many)
- **Consumer Profile** → User-Item Interactions (1:many)
- Items have embeddings for similarity search and recommendations

**Frequently Changing Attributes**:
- **Shop metadata**: status (active/inactive), rating, region, featured_flag, promotion_tier
- **Merchant metadata**: verification_status, reputation_score, response_time, fulfillment_rate
- **Consumer preferences**: preferred_categories, price_range, style_preferences, dietary_restrictions, size_preferences

**Current Forced Solution**: Denormalize all metadata into item records
```python
# Current denormalized schema (forced by vector field requirement)
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

    # NO way to store consumer preferences without vectors!
    # Must create separate interaction records or denormalize into user profiles
}

# Consumer preferences require awkward workarounds:
# Option 1: User embeddings (forced to create unnecessary embeddings)
# Option 2: Store in external database (lose Milvus query capabilities)
# Option 3: Denormalize into every user-item interaction (massive duplication)
```

**Pain Points**:
1. **Update Inefficiency**:
   - Changing a shop's status requires updating many item records
   - Updating consumer preferences requires touching all their interaction records
   - Batch updates (e.g., region-wide shop promotions) cascade to many items

2. **Storage Overhead**:
   - Redundant metadata duplicated across many items
   - Each item stores multiple entities' worth of denormalized attributes
   - Consumer preferences copied into every interaction record

3. **Consistency Issues**:
   - Risk of stale/inconsistent data during bulk updates
   - Temporal inconsistency: Shop status changed but half the items still show old status
   - Consumer preference updates may not reflect immediately in recommendations

4. **Maintenance Burden**:
   - Complex application logic to manage denormalized updates
   - Background jobs to sync metadata changes across collections
   - Difficult to debug data quality issues

5. **Forced Vector Creation**:
   - Consumer preferences don't naturally have embeddings
   - Forced to create dummy embeddings or store externally
   - Loses Milvus's powerful scalar query capabilities for pure metadata

**Example Impact**:
- **Shop updates**: Shop with many items → Changing shop status requires many upsert operations
- **Merchant updates**: Merchant with many items → Reputation score update requires many upserts
- **Consumer updates**: Active user with many interactions → Preference change requires many upserts
- **Batch operations**: Database with many shops, merchants, and users → Significant unnecessary updates for routine metadata changes
- **Storage overhead**: Significant redundant storage for denormalized fields vs normalized design
- **Query complexity**: Cannot efficiently filter "show items from verified merchants with rating > 4.5 preferred by users with preference X" without multiple denormalized fields

### Proposed Solution

Two complementary features that enable normalized data modeling:

1. **Metadata Collections**: Allow collections without vector fields to store pure metadata (relax existing validation)
2. **JOIN Syntax**: Add explicit JOIN clause that executes after ANN search, using nested loop joins with bounded result sets

This approach uses nested loop joins specifically because the KNN result set bounds the outer loop size, making performance predictable.

**Normalized Schema with Metadata Collections**:
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

# Shop metadata collection - NO vector field needed!
shop_metadata = {
    "shop_id": int64,       # Primary key
    "shop_name": varchar,
    "shop_status": varchar,
    "shop_rating": float,
    "shop_region": varchar,
    "featured_flag": bool,
    "promotion_tier": int64
}

# Merchant metadata collection - NO vector field needed!
merchant_metadata = {
    "merchant_id": int64,           # Primary key
    "merchant_name": varchar,
    "verification_status": varchar,
    "reputation_score": float,
    "response_time_hours": int64,
    "fulfillment_rate": float
}

# Consumer profile metadata collection - NO vector field needed!
consumer_profile = {
    "user_id": int64,               # Primary key
    "user_name": varchar,
    "preferred_categories": varchar,  # JSON array or comma-separated
    "price_range_min": float,
    "price_range_max": float,
    "style_preferences": varchar,
    "dietary_restrictions": varchar,
    "size_preferences": varchar,
    "last_updated": int64
}

# User-item interactions (optional, if tracking views/purchases)
user_item_interactions = {
    "interaction_id": int64,
    "user_id": int64,       # Reference to consumer_profile
    "item_id": int64,       # Reference to items
    "interaction_type": varchar,  # viewed, purchased, favorited
    "timestamp": int64
}
```

**Benefits**:
1. **Normalized Data Model**: Store shop/merchant/consumer metadata once in dedicated metadata collections
2. **Efficient Updates**:
   - Update shop status: 1 operation (not thousands)
   - Update consumer preferences: 1 operation (not hundreds)
   - Update merchant reputation: 1 operation (not thousands)
3. **Reduced Storage**: Eliminate redundant denormalized data
4. **Explicit JOIN Syntax**: Clear, SQL-like semantics that developers understand
5. **Predictable Performance**: Nested loop join bounded by KNN topK - no surprise costs
6. **Safe by Design**: Error returned if metadata collection side too large
7. **Natural Data Modeling**: Store entities that don't have embeddings without forcing dummy vectors
8. **Consistent Updates**: Single source of truth for metadata ensures consistency
9. **Simple Implementation**: Nested loop join is straightforward to implement and reason about

### Real-World Use Cases

1. **E-commerce Recommendation Systems** (primary motivating example):
   - Item embeddings for similarity search
   - Shop/merchant metadata (status, ratings, verification) - changes frequently
   - Consumer profiles and preferences - updated by users regularly
   - Enable personalized recommendations with business rules filtering
   - **Impact**: Significantly faster metadata updates and storage reduction

2. **Multi-tenant SaaS**:
   - Application data with embeddings for semantic search
   - Tenant configurations (billing tier, feature flags, quotas) - no natural embeddings
   - User preferences and settings per tenant
   - **Impact**: Efficient tenant metadata management without dummy vectors

3. **Content Platforms (social media, streaming)**:
   - Content embeddings for recommendations
   - User profiles (age, location, interests, privacy settings) - frequently updated
   - Creator metadata (verification status, subscriber count, content rating)
   - **Impact**: Real-time preference updates without touching millions of content records

4. **IoT and Smart Devices**:
   - Sensor data embeddings for anomaly detection
   - Device metadata (location, model, firmware version, status) - updated frequently
   - User device preferences and configurations
   - **Impact**: Efficient device fleet management with metadata updates

5. **Lookup Tables and Reference Data**:
   - Categories, tags, taxonomies
   - Configuration tables
   - Localization and translation tables
   - **Impact**: Store reference data naturally without forced vector fields

## Public Interfaces

### Type System Changes

**CRITICAL DECISION**: Use single `Collection` type with relaxed validation - if a collection has no vector field, it's a metadata collection.

**Important Context**: Milvus already supports **nullable vectors** (vectors that can be null in records), which demonstrates that the system can handle collections where not every record has vector data. Allowing zero vector fields entirely is a natural extension of this capability.

**Validation Logic**:
- `Collection` creation: Allow zero or more vector fields (relaxed from current "at least one" requirement)
- Collections with vectors: Full functionality including `search()`
- Collections without vectors (metadata collections): `search()` returns clear error message
- No new types needed - same `Collection` type handles both cases

### API Behavior

**Collection with vectors (existing, unchanged)**:
- `CreateCollection()` - Works with one or more vector fields (no change)
- `Insert()`, `Upsert()`, `Delete()`, `Query()`, `Search()` - Full support (no change)
- `CreateIndex()` - Supports both vector and scalar indexes (no change)

**Collection without vectors (metadata collection)**:
- `CreateCollection()` - Now accepts schemas with zero vector fields
- `Insert()`, `Upsert()`, `Delete()`, `Query()` - Full support
- `CreateIndex()` - Supports scalar indexes only (STL_SORT, Inverted, Trie)
- `Search()` - Returns error: "search() requires a collection with vector fields"

### SDK Changes

**Python SDK Example** (Metadata Collection):
```python
from pymilvus import CollectionSchema, FieldSchema, DataType, Collection

# Metadata collection - same Collection class, just no vector fields
shop_schema = CollectionSchema([
    FieldSchema("shop_id", DataType.INT64, is_primary=True),
    FieldSchema("shop_name", DataType.VARCHAR, max_length=256),
    FieldSchema("shop_status", DataType.VARCHAR, max_length=50),
    FieldSchema("shop_region", DataType.VARCHAR, max_length=100),
    FieldSchema("merchant_id", DataType.INT64),
    FieldSchema("rating", DataType.FLOAT),
    FieldSchema("is_verified", DataType.BOOL),
], description="Shop metadata")

# Same Collection class - validation now allows zero vector fields
shop_metadata = Collection("shop_metadata", schema=shop_schema)

# Supported operations
shop_metadata.insert([[1, 2, 3],
                      ["Shop A", "Shop B", "Shop C"],
                      ["active", "active", "inactive"],
                      ["US-West", "US-East", "EU"],
                      [101, 102, 103],
                      [4.5, 4.8, 3.2],
                      [True, True, False]])

results = shop_metadata.query(
    expr="shop_status == 'active' AND rating > 4.0",
    output_fields=["shop_id", "shop_name", "merchant_id"]
)

# search() returns clear error on metadata collections
# shop_metadata.search(...)  # Error: "search() requires a collection with vector fields"
```

**JOIN Syntax After ANN Search** (New Feature):

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
    join="JOIN shop_metadata ON shop_id = shop_metadata.shop_id WHERE shop_status == 'active' AND shop_rating > 4.0",

    # RETURN fields from both collections
    output_fields=["item_id", "price", "shop_metadata.shop_name", "shop_metadata.shop_rating"]
)

# Results include data from both collections
for hit in results[0]:
    print(f"Item: {hit.entity['item_id']}, Price: {hit.entity['price']}")
    print(f"Shop: {hit.entity['shop_metadata.shop_name']} (rating: {hit.entity['shop_metadata.shop_rating']})")
```

**JOIN Grammar**:

```
JOIN <collection_name> ON <join_key> = <collection_name>.<join_key> WHERE <filter_condition>
```

**Examples:**
```python
# Simple join: Get shop info for search results
join="JOIN shop_metadata ON shop_id = shop_metadata.shop_id"

# Join with filter: Only active shops with high rating
join="JOIN shop_metadata ON shop_id = shop_metadata.shop_id WHERE shop_status == 'active' AND shop_rating > 4.0"

# Multiple joins: Get both shop and merchant info
join="""JOIN shop_metadata ON shop_id = shop_metadata.shop_id WHERE shop_status == 'active'
        JOIN merchant_metadata ON merchant_id = merchant_metadata.merchant_id WHERE verification_status == 'verified'"""
```

**How It Works - Nested Loop Join After ANN Search**:

```python
# Execution flow:
# 1. Execute ANN search → Get topK results (e.g., 100 items)
# 2. Extract join keys from results (e.g., shop_ids: [101, 205, 387, ...])
# 3. Query metadata collection with extracted keys
# 4. Apply WHERE filter on metadata results
# 5. Nested loop join: For each search result, find matching metadata
# 6. Return combined results

# Why nested loop is safe:
# - Outer loop: bounded by topK (e.g., 100)
# - Inner loop: bounded by matching metadata records
# - Total operations: O(topK * avg_matches) - predictable and bounded
```

**Size Limit Protection**:

To prevent OOM and unpredictable latency, the join returns an error if the metadata collection side is too large:

```python
# Configuration parameter
proxy:
  maxJoinMetadataSize: 10000  # Default limit for metadata collection size in join

# Error example:
# "JOIN failed: metadata collection 'shop_metadata' has 50,000 matching records,
#  exceeding limit of 10,000. Use a more selective WHERE clause or increase limit."
```

**Benefits of JOIN Syntax**:
- **Explicit semantics**: Clear SQL-like JOIN syntax familiar to developers
- **Predictable performance**: Nested loop bounded by KNN topK
- **Safe by design**: Error returned if metadata side too large
- **Post-search execution**: JOIN runs after ANN, doesn't affect vector search performance
- **Single API call**: No multiple round trips from application to Milvus

**Update Efficiency Example**:
```python
# Scenario: Consumer updates their dietary preferences

# OLD WAY (denormalized): Update all user-item interaction records
# If user has 500 interactions, need 500 upsert operations
old_way_updates = 500

# NEW WAY (metadata collection): Single update
consumer_profile.upsert([{
    "user_id": 12345,
    "dietary_restrictions": "vegan, gluten-free"
}])
new_way_updates = 1  # More efficient!

# Next recommendation query automatically uses latest preferences via JOIN
# No sync lag, no stale data, no complex update logic
```

### Configuration

```yaml
proxy:
  # Maximum size of metadata collection allowed in JOIN operations
  # Returns error if exceeded - prevents OOM and unpredictable latency
  maxJoinMetadataSize: 10000
```

## Design Details

### Design Rationale

This proposal builds on three key insights from the Milvus maintainers:

1. **Nullable Vectors Precedent**: Milvus already supports nullable vectors, allowing records to exist without vector data. This demonstrates that the core system can handle collections where not all records have vectors. Allowing zero vector fields is a natural extension - if a collection has no vector field, it's simply a metadata collection.

2. **Existing Requery Infrastructure**: Milvus already has a `requeryOperator` in the search pipeline (`internal/proxy/search_pipeline.go`) that fetches additional fields after search. This operator:
   - Takes primary keys from search results
   - Queries the collection to fetch full field data
   - Returns merged results

   The JOIN operator follows the same pattern but queries OTHER collections instead of the same collection.

3. **Nested Loop Join on Bounded Result Sets**: The JOIN executes as a pipeline stage AFTER the reduce stage, so the outer loop is bounded by topK. This makes nested loop joins safe and predictable:
   - ANN search returns topK results (e.g., 100)
   - JOIN iterates over these bounded results
   - Performance is O(topK * metadata_matches) - predictable and bounded

These insights guide the implementation: simple validation relaxation + JOIN as a pipeline operator after requery.

### Architecture Summary

The JOIN feature is implemented as a **pipeline operator** in the existing hybrid search framework, executing after the requery stage.

**Key Insight**: Milvus already has a `requeryOperator` in the search pipeline that fetches additional fields after search. The JOIN operator extends this pattern to fetch data from OTHER collections.

**Existing Search Pipeline** (from `internal/proxy/search_pipeline.go`):
```
[search] → [reduce] → [requery] → [organize] → [end]
                          ↑
                  Fetches additional fields
                  from SAME collection by PK
```

**Proposed Pipeline with JOIN**:
```
[search] → [reduce] → [requery] → [join] → [organize] → [end]
                                     ↑
                             Fetches fields from
                             OTHER collections by join key
                             (nested loop, bounded by topK)
```

**Why This Fits the Framework**:
1. **Operator Pattern**: JOIN is just another composable operator
2. **Bounded Execution**: Result set already limited by topK at this point
3. **Existing Infrastructure**: Can leverage existing `QueryRequest` and field data fetching from `requeryOperator`
4. **Pipeline Selection**: Existing `newBuiltInPipeline()` logic can select appropriate pipeline when JOIN is present

**Current Flow (Manual Two-Pass)**:
```
Application does:
1. Search vector collection → Get topK results
2. Extract IDs from results
3. Query metadata collection with those IDs
4. Manually merge results in application

Requires 2+ round trips from application to Milvus
```

**New Flow (JOIN as Pipeline Operator)**:
```
Application sends: Search with join="JOIN metadata ON key = key WHERE condition"

Search Pipeline executes:
1. [search]   Execute ANN search → Get topK results
2. [reduce]   Merge results from shards
3. [requery]  Fetch additional fields from same collection (existing)
4. [join]     NEW: Query metadata collection, nested loop merge
5. [organize] Reorder results
6. [end]      Filter output fields

Single round trip from application to Milvus
```

**Why Nested Loop is Safe at This Pipeline Stage**:
- Results already bounded by topK from [reduce] stage
- Outer loop: bounded by topK (e.g., 100)
- Inner loop: bounded by matching metadata records
- Size limit: error if metadata side exceeds threshold
- Composable with existing rerank and requery operators

### 1. Relax Collection Validation

**File**: `internal/proxy/task.go`

**CHANGE**: Remove the vector field requirement from collection validation:

```go
// createCollectionTask - now allows zero vector fields
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

    // Log whether this is a metadata collection (no vectors)
    if vectorFields == 0 {
        log.Info("creating metadata collection (no vector fields)",
            zap.String("collection", t.schema.Name),
            zap.Int("scalar_fields", len(t.schema.Fields)-1))
    }

    return nil
}
```

### 2. Search Validation for Metadata Collections

**File**: `internal/proxy/task_search.go`

Add runtime check in search to reject metadata collections:

```go
func (t *searchTask) PreExecute(ctx context.Context) error {
    // ... existing validation ...

    // NEW: Check if collection has vector fields
    vectorFields := typeutil.GetVectorFieldSchemas(t.schema)
    if len(vectorFields) == 0 {
        return merr.WrapErrParameterInvalidMsg(
            "search() requires a collection with vector fields. " +
            "This collection has no vector fields (metadata collection). " +
            "Use query() instead, or add vector fields to the schema.")
    }

    // ... rest of existing PreExecute ...
}
```

### 3. Collection Metadata

No protobuf changes needed. The existing schema already stores field information, which is sufficient to determine if a collection has vector fields at runtime.

### 4. Implement JOIN as Pipeline Operator

**Building on Hybrid Search Framework**:

The JOIN is implemented as a new pipeline operator in `internal/proxy/search_pipeline.go`, following the same pattern as the existing `requeryOperator`.

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

**File**: `internal/proxy/search_pipeline.go`

**Add JOIN operator constant and registration**:
```go
const (
    // ... existing operators
    searchReduceOp = "search_reduce"
    requeryOp      = "requery"
    joinOp         = "join"  // NEW
    // ...
)

func init() {
    // ... existing registrations
    opFactory[joinOp] = newJoinOperator  // NEW
}
```

**Implement joinOperator** (following requeryOperator pattern):
```go
// joinOperator - fetches data from OTHER collections and merges with search results
type joinOperator struct {
    // Configuration from JOIN clause
    joinCollectionName string
    localKeyField      string
    foreignKeyField    string
    whereExpr          string
    outputFields       []string

    // Context (similar to requeryOperator)
    node              types.ProxyComponent
    collectionID      int64
    maxJoinSize       int64
    consistencyLevel  commonpb.ConsistencyLevel
    // ...
}

func newJoinOperator(node types.ProxyComponent, params map[string]any) *joinOperator {
    return &joinOperator{
        joinCollectionName: params["collectionName"].(string),
        localKeyField:      params["localKey"].(string),
        foreignKeyField:    params["foreignKey"].(string),
        whereExpr:          params["whereExpr"].(string),
        outputFields:       params["outputFields"].([]string),
        node:               node,
        maxJoinSize:        Params.ProxyCfg.MaxJoinMetadataSize.GetAsInt64(),
    }
}

func (op *joinOperator) run(ctx context.Context, span trace.Span, inputs ...any) ([]any, error) {
    // Input: field data and IDs from previous pipeline stage
    fieldsData := inputs[0].([]*schemapb.FieldData)
    storageCost := inputs[1].(segcore.StorageCost)

    // Step 1: Extract join keys from search results
    joinKeys := extractFieldValues(fieldsData, op.localKeyField)
    if len(joinKeys) == 0 {
        return []any{fieldsData, storageCost}, nil  // No results to join
    }

    // Step 2: Build query expression for joined collection
    queryExpr := fmt.Sprintf("%s in %v", op.foreignKeyField, joinKeys)
    if op.whereExpr != "" {
        queryExpr = fmt.Sprintf("(%s) AND (%s)", queryExpr, op.whereExpr)
    }

    // Step 3: Query joined collection (reuse requeryOperator's query infrastructure)
    joinedData, joinStorageCost, err := op.queryJoinedCollection(ctx, span, queryExpr)
    if err != nil {
        return nil, err
    }

    // Step 4: Check metadata result size limit
    joinedCount := getResultCount(joinedData)
    if joinedCount > op.maxJoinSize {
        return nil, fmt.Errorf(
            "JOIN failed: metadata collection '%s' has %d matching records, "+
            "exceeding limit of %d. Use a more selective WHERE clause.",
            op.joinCollectionName, joinedCount, op.maxJoinSize)
    }

    // Step 5: Nested loop join - merge joined data into results
    // Build lookup map for O(1) access
    lookupMap := buildLookupMap(joinedData, op.foreignKeyField)

    // Merge fields (add qualified names like "shop_metadata.shop_name")
    mergedFields := mergeJoinedFields(fieldsData, lookupMap, op.localKeyField, op.joinCollectionName)

    totalCost := storageCost.Add(joinStorageCost)
    return []any{mergedFields, totalCost}, nil
}

// queryJoinedCollection - similar to requeryOperator.requery() but for different collection
func (op *joinOperator) queryJoinedCollection(ctx context.Context, span trace.Span, expr string) (*milvuspb.QueryResults, segcore.StorageCost, error) {
    // Create QueryRequest for joined collection (following requeryOperator pattern)
    queryReq := &milvuspb.QueryRequest{
        CollectionName: op.joinCollectionName,
        Expr:           expr,
        OutputFields:   op.outputFields,
        // ... consistency level, etc.
    }

    // Execute query via proxy
    return op.node.(*Proxy).query(ctx, queryReq)
}
```

**Define new pipeline with JOIN**:
```go
// searchWithJoinPipe - search pipeline with JOIN operator
var searchWithJoinPipe = &pipelineDef{
    name: "searchWithJoin",
    nodes: []*nodeDef{
        {
            name:    "reduce",
            opName:  searchReduceOp,
            inputs:  []string{pipelineInput, pipelineStorageCost},
            outputs: []string{"reduced", "metrics"},
        },
        {
            name:    "merge",
            opName:  lambdaOp,
            inputs:  []string{"reduced"},
            outputs: []string{"unique_ids"},
            params:  map[string]any{"func": mergeIDsFunc},
        },
        {
            name:    "requery",
            opName:  requeryOp,
            inputs:  []string{"unique_ids", pipelineStorageCost},
            outputs: []string{"fields", pipelineStorageCost},
        },
        {
            name:    "join",  // NEW: JOIN operator
            opName:  joinOp,
            inputs:  []string{"fields", pipelineStorageCost},
            outputs: []string{"joined_fields", pipelineStorageCost},
            // params populated from parsed JOIN clause
        },
        {
            name:    "organize",
            opName:  organizeOp,
            inputs:  []string{"joined_fields", "reduced"},
            outputs: []string{"organized"},
        },
        // ... end node
    },
}
```

**Update pipeline selection logic**:
```go
func newBuiltInPipeline(t *searchTask) *pipeline {
    needRequery := t.needRequery()
    needJoin := len(t.joinClauses) > 0  // NEW

    if needJoin {
        // Select pipeline with JOIN operator
        return buildPipeline(searchWithJoinPipe, t.joinParams())
    }
    if needRequery {
        return buildPipeline(searchWithRequeryPipe, t.requeryParams())
    }
    // ... existing logic
}
```

**Why Pipeline Approach is Better**:

```go
// Benefits of implementing JOIN as pipeline operator:
// 1. Composable: Can combine with rerank, requery in any order
// 2. Reusable: Leverages existing QueryRequest infrastructure
// 3. Consistent: Same error handling, storage cost tracking as other operators
// 4. Testable: Each operator can be unit tested independently
// 5. Extensible: Easy to add variations (multi-join, conditional join, etc.)
```

**Limitations (Phase 1)**:
- Nested loop join only (no hash join, sort-merge join)
- Error if metadata collection side exceeds size limit
- No aggregations - just field lookups
- No nested joins - only flat joins from search results

### 5. Index Creation

**File**: `internal/proxy/task_index.go`

For metadata collections (no vector fields), only scalar index types are allowed:

```go
func (t *createIndexTask) PreExecute(ctx context.Context) error {
    // ... existing validation ...

    // Check if this is a metadata collection
    vectorFields := typeutil.GetVectorFieldSchemas(t.schema)
    if len(vectorFields) == 0 {
        // Metadata collection - only allow scalar indexes
        indexType := t.req.GetIndexParams()["index_type"]
        if !isScalarIndexType(indexType) {
            return merr.WrapErrParameterInvalidMsg(
                "Metadata collections only support scalar indexes (STL_SORT, Trie, Inverted)")
        }
    }

    // ... rest of existing validation ...
}

// Helper function
func isScalarIndexType(indexType string) bool {
    scalarIndexTypes := []string{"STL_SORT", "Trie", "Inverted", "AUTOINDEX"}
    for _, t := range scalarIndexTypes {
        if indexType == t {
            return true
        }
    }
    return false
}
```

### 6. Query Optimization

**File**: `internal/querynodev2/segments/segment_loader.go`

Metadata collection segments skip vector-specific loading:

```go
func (loader *segmentLoader) loadSegment(ctx context.Context, segment *Segment) error {
    // Check if collection has vector fields
    vectorFields := typeutil.GetVectorFieldSchemas(segment.Schema)

    if len(vectorFields) == 0 {
        // Metadata collection: Only load scalar data, skip vector data entirely
        if err := loader.loadScalarData(ctx, segment); err != nil {
            return err
        }
    } else {
        // Collection with vectors: Load both vector and scalar data (existing behavior)
        if err := loader.loadVectorData(ctx, segment); err != nil {
            return err
        }
        if err := loader.loadScalarData(ctx, segment); err != nil {
            return err
        }
    }

    // Rest of existing code...
}
```

### 7. Storage Optimization

Metadata collections have simpler storage requirements:
- No vector index files needed
- Only scalar field data and scalar indexes
- Reduced segment size and memory footprint
- Faster load times

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility

**Impact**: None. This is a purely additive feature.

- Existing collections with vector fields: No changes, work exactly as before
- Existing validation: Relaxed to allow zero vector fields
- Existing APIs: Fully compatible, no breaking changes
- New `join` parameter: Optional, existing searches work unchanged

### Migration Path

No migration needed. Users can:
1. Continue using denormalized patterns (existing behavior)
2. Create new metadata collections for normalized patterns
3. Gradually refactor to use JOIN syntax if desired

### Configuration Rollout

```yaml
proxy:
  # Maximum size of metadata collection allowed in JOIN operations
  # Returns error if exceeded - prevents OOM and unpredictable latency
  maxJoinMetadataSize: 10000  # Default
```

## Test Plan

### Unit Tests

**File**: `internal/proxy/task_test.go`

1. **Test**: Create collection with zero vector fields
   - **Expected**: Success (validation relaxed)

2. **Test**: Create collection with vector fields (existing)
   - **Expected**: Success (unchanged behavior)

3. **Test**: Insert/Upsert/Delete on metadata collection
   - **Expected**: All operations succeed

4. **Test**: Query on metadata collection
   - **Expected**: Returns filtered results correctly

5. **Test**: Search on metadata collection
   - **Expected**: Clear error: "search() requires a collection with vector fields"

6. **Test**: Create vector index on metadata collection
   - **Expected**: Error rejecting vector index types

7. **Test**: Create scalar index on metadata collection
   - **Expected**: Success for scalar index types (STL_SORT, Inverted, etc.)

8. **Test**: Parse JOIN clause
   - **Expected**: Correctly parses collection name, keys, and WHERE clause

9. **Test**: JOIN with metadata collection exceeding size limit
   - **Expected**: Error with clear message about size limit

### Integration Tests

**File**: `tests/python_client/testcases/test_metadata_collection.py`

```python
from pymilvus import Collection, CollectionSchema, FieldSchema, DataType

class TestMetadataCollection:
    def test_create_metadata_collection(self):
        """Test creating collection without vector fields"""
        schema = CollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True),
            FieldSchema("name", DataType.VARCHAR, max_length=100),
            FieldSchema("status", DataType.VARCHAR, max_length=50),
        ])
        # Same Collection class - now allows zero vector fields
        metadata_coll = Collection("metadata_test", schema=schema)
        assert metadata_coll is not None

    def test_insert_query_metadata(self):
        """Test insert and query operations on metadata collection"""
        # Insert data
        metadata_coll.insert([[1, 2, 3],
                             ["shop1", "shop2", "shop3"],
                             ["active", "active", "inactive"]])

        # Query with filter
        results = metadata_coll.query(
            expr="status == 'active'",
            output_fields=["id", "name"]
        )
        assert len(results) == 2

    def test_search_returns_error(self):
        """Test that search() returns clear error on metadata collection"""
        with pytest.raises(MilvusException) as exc:
            metadata_coll.search(data=[[0.1]*128], anns_field="vector", limit=10)
        assert "requires a collection with vector fields" in str(exc.value)

    def test_join_with_metadata_collection(self):
        """Test JOIN syntax with metadata collection"""
        # Vector search with JOIN
        results = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=100,
            output_fields=["item_id", "shop_id", "shop_metadata.shop_name", "shop_metadata.shop_rating"],
            join="JOIN shop_metadata ON shop_id = shop_metadata.shop_id WHERE shop_status == 'active'"
        )

        # Verify join worked - results should have metadata fields
        for hit in results[0]:
            assert "shop_metadata.shop_name" in hit.entity
            assert "shop_metadata.shop_rating" in hit.entity

    def test_multi_collection_join(self):
        """Test joining with multiple metadata collections"""
        results = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=50,
            output_fields=["item_id", "shop_metadata.shop_name", "merchant_metadata.verification_status"],
            join="""JOIN shop_metadata ON shop_id = shop_metadata.shop_id WHERE shop_status == 'active'
                    JOIN merchant_metadata ON merchant_id = merchant_metadata.merchant_id WHERE verification_status == 'verified'"""
        )

        # Verify both joins worked
        for hit in results[0]:
            assert "shop_metadata.shop_name" in hit.entity
            assert "merchant_metadata.verification_status" in hit.entity

    def test_join_size_limit_error(self):
        """Test that JOIN returns error when metadata side too large"""
        # Create large metadata collection
        # ... insert 50,000 records ...

        with pytest.raises(MilvusException) as exc:
            item_collection.search(
                data=[embedding],
                anns_field="embedding",
                limit=100,
                join="JOIN large_metadata ON key = large_metadata.key"  # No WHERE filter
            )
        assert "exceeding limit" in str(exc.value)

    def test_metadata_update_efficiency(self):
        """Test that updating metadata is O(1) operation"""
        import time

        # Measure update time (should be fast - single record)
        start = time.time()
        shop_metadata.upsert([{"shop_id": 123, "shop_status": "inactive"}])
        update_time = time.time() - start

        # Should complete quickly
        assert update_time < 0.1, f"Update took {update_time}s, expected to be fast"
```

### E2E Tests

| Test Case | Expected Behavior |
|:----------|:-----------------|
| Create metadata collection (no vectors) | Success with proper logging |
| Create collection with vectors (existing) | Works exactly as before (backward compatible) |
| Query metadata collection with complex filter | Returns correct results |
| Search on metadata collection | Returns clear error message |
| Insert 1M records into metadata collection | Completes successfully with good performance |
| Create scalar index on metadata collection | Index created successfully |
| Attempt vector index on metadata collection | Returns appropriate error |
| JOIN with single metadata collection | Returns combined data correctly |
| JOIN with multiple metadata collections | All metadata correctly joined |
| JOIN exceeds size limit | Returns clear error with limit info |
| Metadata update (single record) | Updates quickly |
| Partition key on metadata collection | Works correctly for multi-tenancy |
| Clustering key on metadata collection | Improves query performance |
| Concurrent updates to metadata | No conflicts, consistent reads |

### Performance Tests

**Benchmark Scenarios**:

1. **Update Efficiency**: Compare denormalized vs normalized patterns
   - **Shop update**:
     - Denormalized: Update many item records when shop status changes
     - Normalized: Update 1 shop record in metadata collection
     - **Expected**: Significantly faster with normalized approach

   - **Merchant update**:
     - Denormalized: Update many item records when merchant reputation changes
     - Normalized: Update 1 merchant record in metadata collection
     - **Expected**: Significantly faster

2. **Storage Efficiency**: Measure storage overhead
   - **Denormalized approach**: Significant redundant metadata across all items
   - **Normalized approach**: Minimal metadata storage in dedicated collections
   - **Expected**: Significant storage reduction for metadata

3. **JOIN Performance**: Measure JOIN latency
   - **Nested loop join overhead**: Measure time added by JOIN clause
   - **Multiple JOINs**: Measure cumulative overhead
   - **Expected**: O(topK * metadata_matches) - bounded and predictable

4. **E-commerce Recommendation End-to-End**:
   - **Workflow**: Single search with JOIN to shop/merchant collections
   - **Measurement**:
     - Query latency with JOINs
     - Throughput for recommendations
     - Concurrent user support
   - **Expected**: JOINs add minimal overhead with proper indexing

5. **Size Limit Validation**:
   - Test with varying metadata collection sizes
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

**What We're Doing Instead**: Nested loop joins after ANN search
- Limited to simple equality joins (inner join only)
- Nested loop bounded by KNN topK - predictable performance
- Error if metadata side too large - prevents OOM
- Keeps implementation simple and maintainable

### Alternative 2: Mandatory Dummy Vector Fields

**Idea**: Require users to add dummy/zero vector fields to meet current validation.

**Rejected Because**:
- Wastes storage and memory
- Confuses users ("why do I need a vector field?")
- Index creation overhead for unused vectors
- Architectural smell - working around limitation instead of fixing it

### Alternative 3: Application-Level Joins Only

**Idea**: Support metadata collections but require users to perform joins in application code (multiple queries + manual join logic).

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

1. **Current Validation Code**:
   - `internal/proxy/task.go:391-393` - Vector field requirement
   - `internal/distributed/proxy/httpserver/utils.go:826-827` - HTTP validation

2. **Collection Creation Flow**:
   - `docs/design_docs/20211217-milvus_create_collection.md`
   - `internal/rootcoord/create_collection_task.go`

3. **Current Search API Architecture**:
   - External: `github.com/milvus-io/milvus-proto` - Client-facing SearchRequest protobuf definition
   - Internal: `pkg/proto/internal.proto:105-125` - Internal SearchRequest protobuf
   - `internal/proxy/task_search.go` - Search task implementation, PreExecute/Execute/PostExecute flow
   - `internal/parser/planparserv2/plan_parser_v2.go` - Expression parsing (CreateSearchPlan)
   - `pkg/proto/plan.proto` - PlanNode structure with VectorANNS and predicates
   - Current parameters: `dsl` (filter expression), `output_fields` (field list)

4. **Query vs Search Architecture**:
   - `docs/design_docs/20221221-retrieve_entity.md`
   - `internal/proxy/task_query.go` - Scalar-only queries
   - `internal/proxy/task_search.go` - Vector searches

5. **Search Pipeline Framework** (Key Infrastructure for JOIN):
   - `internal/proxy/search_pipeline.go` - Pipeline operators and definitions
   - **Existing operators**: `search_reduce`, `hybrid_search_reduce`, `rerank`, `requery`, `organize`, `lambda`, `end`, `highlight`
   - **`requeryOperator`** (lines 322-437): Fetches additional fields after search - **model for `joinOperator`**
   - **Pipeline definitions** (lines 720-1102): `searchPipe`, `searchWithRequeryPipe`, `hybridSearchPipe`, etc.
   - **`newBuiltInPipeline()`** (lines 1104-1133): Selects appropriate pipeline based on search requirements
   - JOIN will be implemented as new `joinOperator` following `requeryOperator` pattern

6. **Expression Grammar**:
   - `docs/design_docs/20220105-query_boolean_expr.md` - Boolean expression grammar
   - `internal/parser/planparserv2/` - Expression parser implementation
   - Proposed: New JOIN clause parser for post-search joins

7. **Related Features and Precedents**:
   - **Nullable vectors** (Key Precedent): Milvus already supports nullable vectors, demonstrating the system can handle records without vector data. This validates that collections without vectors should not be a big issue.
   - Partition keys: Multi-tenancy support
   - Clustering compaction: Scalar field optimization
   - Dynamic fields: Flexible schema

---

## Implementation Checklist

### Phased Rollout Strategy

**Phase 1: Metadata Collections (Core Feature)**
- Relax collection validation to allow zero vector fields
- Add runtime error for search() on metadata collections
- Enables application-level joins immediately

**Phase 2: JOIN Syntax (Enhancement)**
- Add `join` parameter to search API
- Implement JOIN clause parser
- Execute nested loop joins in PostExecute
- Enforce size limits on metadata collection side

---

### Phase 1: Metadata Collections Implementation

**Validation Changes**
- [ ] Modify `internal/proxy/task.go` to allow zero vector fields in schema
- [ ] Add runtime check in `internal/proxy/task_search.go` to reject search on metadata collections
- [ ] Update index creation to only allow scalar indexes for metadata collections
- [ ] Update segment loader to skip vector data for metadata collections

**No Protobuf Changes Required**:
- Existing schema already stores field information
- Collection type determined by checking if vector fields exist

### Phase 2: JOIN as Pipeline Operator

**Protobuf Changes**:
- [ ] Add `join_clause` field to SearchRequest in `milvus.proto`

**Parser Changes** (`internal/parser/planparserv2/`):
- [ ] Create `join_parser.go` with JOIN clause parsing functions
- [ ] Implement `parseJoinClauses()` to extract JOIN clauses
- [ ] Support syntax: `JOIN <collection> ON <key> = <collection>.<key> WHERE <condition>`
- [ ] Add validation for join collections (existence, field validity)

**Pipeline Operator** (`internal/proxy/search_pipeline.go`):
- [ ] Add `joinOp` constant to operator registry
- [ ] Implement `joinOperator` struct (following `requeryOperator` pattern)
- [ ] Implement `newJoinOperator()` factory function
- [ ] Implement `joinOperator.run()` with nested loop join logic
- [ ] Implement `queryJoinedCollection()` (reusing query infrastructure)
- [ ] Add size limit check with clear error message
- [ ] Implement `mergeJoinedFields()` for result merging

**Pipeline Definition**:
- [ ] Define `searchWithJoinPipe` pipeline (reduce → requery → join → organize)
- [ ] Define `searchWithJoinRerankPipe` for join + rerank combination
- [ ] Update `newBuiltInPipeline()` to select appropriate pipeline when JOIN present

**Search Task Changes** (`internal/proxy/task_search.go`):
- [ ] Add `joinClauses []JoinClause` field to `searchTask` struct
- [ ] Extend `PreExecute()` to parse JOIN clause and validate
- [ ] Pass join params to pipeline via `joinParams()` method

**Configuration**:
- [ ] Add `maxJoinMetadataSize` configuration parameter (default: 10,000)

### SDK Changes

**Metadata Collection Support (all SDKs)**:
- [ ] Python SDK: Update documentation showing collections without vectors
- [ ] Python SDK: Add examples for metadata collections
- [ ] Go SDK: Update documentation and examples
- [ ] Java SDK: Update documentation and examples
- [ ] Node.js SDK: Update documentation and examples

**JOIN Syntax (Collection search API)**:
- [ ] Python SDK: Add `join` parameter to `search()` method
- [ ] Python SDK: Update examples to show JOIN syntax
- [ ] Python SDK: Documentation for JOIN grammar
- [ ] Go SDK: Add `Join` parameter to search request
- [ ] Java SDK: Add join parameter
- [ ] Node.js SDK: Add join parameter
- [ ] RESTful API: Add `join` field to search endpoint

### Testing

**Metadata Collection Tests**:
- [ ] Unit tests: Create collection with zero vector fields succeeds
- [ ] Unit tests: Create collection with vector fields still succeeds (unchanged)
- [ ] Unit tests: Search on metadata collection returns clear error
- [ ] Integration tests for metadata collection CRUD operations
- [ ] Integration tests for metadata collection index creation (scalar indexes only)
- [ ] E2E tests for metadata collection load/release/drop operations

**JOIN Tests**:
- [ ] Unit tests for JOIN clause parsing
- [ ] Unit tests for nested loop join execution
- [ ] Unit tests for size limit enforcement
- [ ] Integration tests for single JOIN
- [ ] Integration tests for multiple JOINs
- [ ] Integration tests for JOIN with WHERE clause
- [ ] Integration tests for JOIN exceeding size limit (error case)
- [ ] E2E tests for e-commerce recommendation scenario with JOINs
- [ ] Performance tests for JOIN overhead

**General Tests**:
- [ ] Performance benchmarks (update efficiency, storage, join overhead)
- [ ] Backward compatibility tests (existing collections unaffected)

### Documentation

**Metadata Collection Documentation**:
- [ ] User guide: Collections without vectors (metadata collections)
- [ ] User guide: When to use metadata collections
- [ ] Tutorial: Creating your first metadata collection
- [ ] Best practices: Normalized data modeling with metadata collections
- [ ] Migration guide: Refactoring from denormalized patterns

**JOIN Documentation**:
- [ ] User guide: JOIN syntax after ANN search
- [ ] User guide: Grammar specification - `JOIN <collection> ON <key> = <key> WHERE <condition>`
- [ ] User guide: Understanding nested loop join behavior
- [ ] User guide: Size limits and error handling
- [ ] API reference: Updated Collection.search() with JOIN examples
- [ ] Tutorial: E-commerce multi-entity recommendation with JOINs
- [ ] Best practices: Writing efficient WHERE clauses
- [ ] Best practices: Indexing join keys
- [ ] Performance guide: JOIN overhead and optimization tips

### Release
- [ ] Release notes entry for Phase 1 (Metadata collections)
- [ ] Release notes entry for Phase 2 (JOIN syntax)
- [ ] Community announcement
- [ ] Blog post with e-commerce example demonstrating JOINs

---

## Summary for Reviewers

**What's being proposed:**
1. **Phase 1**: Allow collections without vector fields (metadata collections)
   - Relax existing validation - no new types needed
   - search() returns clear error on metadata collections
2. **Phase 2**: JOIN as pipeline operator
   - New `joinOperator` in `search_pipeline.go` (follows `requeryOperator` pattern)
   - Pipeline: `[search] → [reduce] → [requery] → [join] → [organize] → [end]`
   - Syntax: `join="JOIN <collection> ON <key> = <key> WHERE <condition>"`
   - Nested loop join - safe because bounded by KNN topK at [reduce] stage
   - Error if metadata collection side exceeds size limit

**What's NOT changing:**
- Existing collections with vectors: 100% unchanged behavior
- Existing search API: Continue to work as-is
- Existing pipeline operators: Unchanged, JOIN is additive
- Backward compatibility: Zero breaking changes

**Key design decisions:**
- Single `Collection` type with relaxed validation (not separate type)
- JOIN as pipeline operator (reuses existing `requeryOperator` infrastructure)
- JOIN syntax instead of query subexpressions (clearer semantics)
- Nested loop join only (KNN bounds the outer loop)
- Size limit on metadata side (prevents OOM)
- Composable with existing rerank/requery operators

**What happens next:**
1. Get maintainer approval on approach
2. Implement Phase 1 (Metadata collections)
3. Implement Phase 2 (JOIN pipeline operator)

**Questions? Please comment on:** https://github.com/milvus-io/milvus/issues/46153
