# MEP: Scalar Collections (Collections Without Vector Fields)

**Current state**: Under Discussion
**ISSUE**: https://github.com/milvus-io/milvus/issues/46153
**Keywords**: scalar collection, metadata, relational, join, normalization
**Released**: TBD

---

## Executive Summary

**What**: Introduce `ScalarCollection` - a new first-class collection type (separate from `Collection`) for storing scalar-only data without vectors.

**Why**: Enable normalized data modeling. Users currently must denormalize metadata across vector records, causing inefficient updates and storage waste.

**How**: Create a distinct `ScalarCollection` type with compile-time type safety. No changes to existing `Collection` type.

**Impact**: Zero breaking changes - purely additive feature

---

## Key Design Decisions

| Decision | Choice | Rationale |
|----------|--------|-----------|
| **Type System** | Separate `ScalarCollection` type (not single type with runtime detection) | Compile-time safety, better IDE support, clearer API |
| **Validation** | `Collection`: requires vectors (unchanged)<br>`ScalarCollection`: requires zero vectors | Type system enforces distinction, no runtime flags needed |
| **Search Safety** | `ScalarCollection.search()` method does not exist | Compile-time error prevention (better than runtime error) |
| **Configuration** | No config flags needed | Users explicitly choose type at creation time |
| **Backward Compatibility** | Existing `Collection` behavior 100% unchanged | New type is purely additive |

---

## For Reviewers

**Main Questions for Review:**
1. Do you agree with separate `ScalarCollection` type vs runtime detection?
2. Is the protobuf design (CollectionType enum) acceptable?
3. Should join grammar be in Phase 1 or deferred to Phase 2?
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

Introduce **ScalarCollection** as a new first-class collection type (distinct from `Collection`) that stores only scalar fields without requiring vector fields. Extend Milvus's **search filter expression** to support **query subexpressions** that reference other collections. The **proxy layer** automates what applications currently do manually in two-pass search: execute scalar collection queries first, then use results in vector search filters. This provides a more convenient API while maintaining the same execution pattern applications already use.

**Building on Existing Capabilities**: This proposal (1) extends nullable vector support to allow zero vector fields (ScalarCollection), and (2) automates the two-pass search pattern that applications already use. We're not introducing new execution models - just making existing patterns more convenient.

**Key Features**:

**Feature 1: ScalarCollection Type** (Distinct from Collection)
- New type separate from `Collection` for storing pure metadata without vector fields
- Compile-time type safety - `search()` method doesn't exist on ScalarCollection
- Clear API distinction between vector and scalar collections
- Better IDE support and developer experience

**Feature 2: Query Subexpressions in Filter** (On Collection only, not ScalarCollection)
- Extend the `expr` parameter to support query subexpressions: `"field in (query <collection> where <condition> select <field>)"`
- Proxy does exactly what applications do manually in two-pass search:
  1. Execute query on scalar collection
  2. Get the field values (e.g., list of shop_ids)
  3. Translate to IN clause for vector search (e.g., `shop_id in [101, 205, ...]`)
  4. Execute vector search with the translated filter
- Output fields support cross-collection qualified names: `"collection.field"`
- If cross-collection fields requested, proxy queries those collections after search (same as application would)

**Backward Compatibility**
- Existing `Collection` type: 100% unchanged
- Existing `Collection` validation: Still requires vectors (no change)
- `ScalarCollection`: Purely additive feature

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

1. **Scalar Collections**: Allow collections without vector fields to store pure metadata
2. **Automated Two-Pass Search**: Proxy automatically executes queries on scalar collections and uses results in vector search filters - same as what applications do manually, but more convenient

This approach builds on existing patterns that applications already use, avoiding the complexity of general-purpose SQL joins.

**Normalized Schema with Scalar Collections**:
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

# Shop scalar collection - NO vector field needed!
shop_metadata = {
    "shop_id": int64,       # Primary key
    "shop_name": varchar,
    "shop_status": varchar,
    "shop_rating": float,
    "shop_region": varchar,
    "featured_flag": bool,
    "promotion_tier": int64
}

# Merchant scalar collection - NO vector field needed!
merchant_metadata = {
    "merchant_id": int64,           # Primary key
    "merchant_name": varchar,
    "verification_status": varchar,
    "reputation_score": float,
    "response_time_hours": int64,
    "fulfillment_rate": float
}

# Consumer profile scalar collection - NO vector field needed!
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
1. **Normalized Data Model**: Store shop/merchant/consumer metadata once in dedicated scalar collections
2. **Efficient Updates**:
   - Update shop status: 1 operation (not thousands)
   - Update consumer preferences: 1 operation (not hundreds)
   - Update merchant reputation: 1 operation (not thousands)
3. **Reduced Storage**: Eliminate redundant denormalized data
4. **Convenient Two-Pass Search**: Single API call instead of manual query→search steps
5. **Same Execution Pattern**: Proxy does exactly what applications already do - no new execution model
6. **Cleaner Application Code**: No manual join logic, no multiple round trips
7. **Natural Data Modeling**: Store entities that don't have embeddings without forcing dummy vectors
8. **Consistent Updates**: Single source of truth for metadata ensures consistency
9. **Simple Implementation**: Just automation, no complex join engine needed

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

**CRITICAL DECISION**: Introduce **ScalarCollection** as a new first-class type, distinct from `Collection`.

**Important Context**: Milvus already supports **nullable vectors** (vectors that can be null in records), which demonstrates that the system can handle collections where not every record has vector data. ScalarCollection is a natural extension of this capability—instead of nullable vectors, we have zero vector fields with a dedicated type for better safety and clarity.

**Type Design**:
```go
// UNCHANGED: Existing Collection type
type Collection struct {
    name   string
    schema *CollectionSchema
    // ... vector search methods (all existing behavior unchanged)
}

// NEW: ScalarCollection type - separate from Collection
type ScalarCollection struct {
    name   string
    schema *ScalarCollectionSchema  // Schema without vector fields
    // ... only scalar operations, NO search() method
}
```

**Validation Logic**:
- `Collection` creation: Still requires at least one vector field (existing validation unchanged)
- `ScalarCollection` creation: Requires zero vector fields (new validation)
- Type safety enforced at compile time, not runtime

### API Behavior

**Collection (existing, unchanged)**:
- `CreateCollection()` - Requires at least one vector field (no change)
- `Insert()`, `Upsert()`, `Delete()`, `Query()`, `Search()` - Full support (no change)
- `CreateIndex()` - Supports both vector and scalar indexes (no change)

**ScalarCollection (new)**:
- `CreateScalarCollection()` - Accepts schemas with zero vector fields
- `Insert()`, `Upsert()`, `Delete()`, `Query()` - Full support
- `CreateIndex()` - Supports scalar indexes only (STL_SORT, Inverted, Trie)
- `Search()` - Method does not exist (compile-time type safety)

### SDK Changes

**Python SDK Example**:
```python
from pymilvus import ScalarCollectionSchema, FieldSchema, DataType, ScalarCollection

# NEW: ScalarCollection type - distinct from Collection
shop_schema = ScalarCollectionSchema([
    FieldSchema("shop_id", DataType.INT64, is_primary=True),
    FieldSchema("shop_name", DataType.VARCHAR, max_length=256),
    FieldSchema("shop_status", DataType.VARCHAR, max_length=50),
    FieldSchema("shop_region", DataType.VARCHAR, max_length=100),
    FieldSchema("merchant_id", DataType.INT64),
    FieldSchema("rating", DataType.FLOAT),
    FieldSchema("is_verified", DataType.BOOL),
], description="Shop metadata")

# ScalarCollection - separate class from Collection
shop_metadata = ScalarCollection("shop_metadata", schema=shop_schema)

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

# Compile-time type safety: search() method doesn't exist
# shop_metadata.search(...)  # AttributeError: 'ScalarCollection' object has no attribute 'search'
```

**Go SDK Example**:
```go
// NEW: ScalarCollection type - distinct from Collection
type ScalarCollection struct {
    name   string
    schema *ScalarCollectionSchema
}

// Only scalar operations - NO Search() method
func (sc *ScalarCollection) Query(expr string) ([]map[string]interface{}, error)
func (sc *ScalarCollection) Insert(data []interface{}) error
func (sc *ScalarCollection) Upsert(data []interface{}) error
func (sc *ScalarCollection) Delete(expr string) error
// Search() method doesn't exist - type-safe at compile time
```

**Query Subexpressions in Filter** (New Feature):

Based on maintainer feedback, we extend the existing `expr` filter parameter to support **query subexpressions** that reference other collections. The **proxy layer** handles the query-search reform automatically:

```python
# Current Milvus search API (today):
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},
    expr="price >= 10.0 AND price <= 100.0",      # Filter on same collection
    output_fields=["item_id", "price", "shop_id"]  # Fields from same collection
)

# NEW: Search with query subexpressions in filter (proposed)
# Extend existing expr parameter to support cross-collection queries
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},

    # expr now supports query subexpressions to filter via other collections
    expr="""
        shop_id in (query shop_metadata
                    where shop_status == 'active' and shop_rating > 4.0
                    select shop_id)
        and price >= 10.0 and price <= 100.0
    """,

    # output_fields extended to support cross-collection fields with qualified names
    output_fields=["item_id", "price", "shop_metadata.shop_name", "shop_metadata.shop_rating"]
)

# Results include data from both collections - no application-level join needed!
for hit in results[0]:
    print(f"Item: {hit.entity['item_id']}, Price: {hit.entity['price']}")
    print(f"Shop: {hit.entity['shop_metadata.shop_name']} (rating: {hit.entity['shop_metadata.shop_rating']})")
```

**Query Subexpression Grammar**:

The query subexpression syntax:
```
query <collection_name> where <filter_condition> select <field_name>
```

**Examples:**
```python
# Simple query: Get shop_ids where shop is active
expr="shop_id in (query shop_metadata where shop_status == 'active' select shop_id)"

# Query with complex filter: Active shops with high rating
expr="shop_id in (query shop_metadata where shop_status == 'active' and shop_rating > 4.0 select shop_id)"

# Combined with local filters: Price range AND active shops
expr="price >= 10.0 and price <= 100.0 and shop_id in (query shop_metadata where shop_status == 'active' select shop_id)"

# Multiple query subexpressions: Filter by both shop and merchant
expr="""shop_id in (query shop_metadata where shop_status == 'active' select shop_id)
        and merchant_id in (query merchant_metadata where verification_status == 'verified' select merchant_id)"""
```

**How It Works - Proxy Does Two-Pass Search Automatically**:

The proxy does exactly what applications currently do manually:

```python
# What applications do today (manual two-pass):
# Step 1: Query scalar collection
active_shop_ids = shop_metadata.query(
    expr="shop_status == 'active' and shop_rating > 4.0",
    output_fields=["shop_id"]
)  # Returns: [101, 205, 387, ..., 9823]

# Step 2: Use results in vector search filter
results = items.search(
    data=[query_embedding],
    anns_field="item_embedding",
    expr=f"shop_id in {active_shop_ids} and price >= 10.0",  # Use the IDs from step 1
    limit=100
)

# With query subexpressions, proxy does steps 1-2 automatically:
results = items.search(
    data=[query_embedding],
    anns_field="item_embedding",
    expr="shop_id in (query shop_metadata where shop_status == 'active' and shop_rating > 4.0 select shop_id) and price >= 10.0",
    limit=100
)

# Proxy internal flow (same as manual two-pass):
# 1. Detect "query shop_metadata..." pattern in expr
# 2. Execute: shop_metadata.query(expr="shop_status == 'active' and shop_rating > 4.0")
# 3. Get shop_ids: [101, 205, 387, ..., 9823]
# 4. Translate expr to: "shop_id in [101, 205, 387, ..., 9823] and price >= 10.0"
# 5. Execute vector search with translated filter
```

**Benefits of Query Subexpressions**:
- **Same as manual two-pass, but automatic**: Proxy does what applications already do, just more conveniently
- **Single API call**: No multiple round trips from application to Milvus
- **SQL-like syntax**: Natural `IN (query ... where ... select ...)` pattern familiar to developers
- **Reuses existing API**: Extends the familiar `expr` parameter instead of adding new parameters
- **Simple implementation**: Proxy just does: parse → query → translate → search (same steps as applications)
- **Backward compatible**: Existing expr filters continue to work unchanged

**Update Efficiency Example**:
```python
# Scenario: Consumer updates their dietary preferences

# OLD WAY (denormalized): Update all user-item interaction records
# If user has 500 interactions, need 500 upsert operations
old_way_updates = 500

# NEW WAY (metadata collection): Single update
consumer_profile_collection.upsert([{
    "user_id": 12345,
    "dietary_restrictions": "vegan, gluten-free"
}])
new_way_updates = 1  # More efficient!

# Next recommendation query automatically uses latest preferences
# No sync lag, no stale data, no complex update logic
```

### Configuration

No configuration parameters needed. Users explicitly choose between `Collection` and `ScalarCollection` types at creation time. The type system enforces the distinction.

## Design Details

### Design Rationale

This proposal builds on two key insights from the Milvus maintainers:

1. **Nullable Vectors Precedent**: Milvus already supports nullable vectors, allowing records to exist without vector data. This demonstrates that the core system can handle collections where not all records have vectors. Scalar-only collections are a natural extension—instead of nullable vectors within a collection, we have zero vector fields entirely. This existing capability means introducing scalar collections should not be a big issue.

2. **Proxy Does Two-Pass Search**: The query subexpression feature is simply automating what applications already do manually in two-pass search. The proxy:
   - Detects query patterns in the filter expression
   - Executes queries on scalar collections first
   - Translates query results into IN clauses
   - Executes vector search with the translated filter
   - This is exactly what applications do, but more convenient (single API call instead of multiple round trips)

These insights guide the implementation approach: leverage what already exists and automate existing patterns rather than introducing fundamentally new concepts.

### Architecture Summary

The query subexpression feature automates two-pass search in the proxy layer.

**Key Insight**: Applications already do two-pass search manually (query scalar collection → use results in vector search filter). The proxy just automates this pattern.

**Current Flow (Manual Two-Pass)**:
```
Application does:
1. Query scalar collection → Get IDs
2. Search vector collection with "field in [IDs]" filter

Requires 2 round trips from application to Milvus
```

**New Flow (Automatic Two-Pass)**:
```
Application sends: Search with expr="field in (query collection where ... select ...)"

Proxy does automatically:
1. Parse expr → Detect query subexpression
2. Execute query on scalar collection → Get IDs
3. Translate expr: "field in (query...)" → "field in [101, 205, ...]"
4. Execute vector search with translated filter
5. (Optional) If output_fields has cross-collection fields, query those collections and merge

Single round trip from application to Milvus
```

**Implementation is Simple**:
- Extend expression parser to detect query patterns
- Proxy executes queries before search (same as application would)
- Proxy translates query results into IN clauses (simple string replacement)
- Rest of search pipeline works unchanged

### 1. Core Type Implementation

**Create ScalarCollection Type** (NEW)

**File**: `internal/core/src/scalar_collection.go` (new file)

```go
package core

// ScalarCollection - distinct type for collections without vector fields
type ScalarCollection struct {
    name         string
    schema       *ScalarCollectionSchema
    collectionID int64
    // ... collection management fields
}

// ScalarCollectionSchema - schema that validates zero vector fields
type ScalarCollectionSchema struct {
    Name        string
    Description string
    Fields      []*FieldSchema
}

// Validation: Ensure zero vector fields
func (s *ScalarCollectionSchema) Validate() error {
    vectorFields := typeutil.GetVectorFieldSchemas(s.Fields)
    if len(vectorFields) > 0 {
        return errors.New("ScalarCollection cannot contain vector fields")
    }
    if len(s.Fields) < 2 { // at least primary key + one scalar field
        return errors.New("ScalarCollection must have at least one scalar field")
    }
    return nil
}

// Only scalar operations - NO Search() method
func (sc *ScalarCollection) Insert(data []interface{}) error { /* ... */ }
func (sc *ScalarCollection) Upsert(data []interface{}) error { /* ... */ }
func (sc *ScalarCollection) Delete(expr string) error { /* ... */ }
func (sc *ScalarCollection) Query(expr string) ([]map[string]interface{}, error) { /* ... */ }
func (sc *ScalarCollection) CreateIndex(fieldName string, indexType string) error { /* ... */ }
func (sc *ScalarCollection) Load() error { /* ... */ }
func (sc *ScalarCollection) Release() error { /* ... */ }
func (sc *ScalarCollection) Drop() error { /* ... */ }
// Search() does NOT exist - compile-time type safety
```

**File**: `internal/proxy/task.go`

**Existing Collection Validation - UNCHANGED**:
```go
// createCollectionTask - for regular Collection (with vectors)
func (t *createCollectionTask) validateSchema() error {
    vectorFields := len(typeutil.GetVectorFieldSchemas(t.schema))

    if vectorFields > Params.ProxyCfg.MaxVectorFieldNum.GetAsInt() {
        return fmt.Errorf("maximum vector field's number should be limited to %d",
            Params.ProxyCfg.MaxVectorFieldNum.GetAsInt())
    }

    // Collection still requires vector fields - NO CHANGE
    if vectorFields == 0 {
        return merr.WrapErrParameterInvalidMsg("schema does not contain vector field")
    }

    return nil
}
```

**NEW ScalarCollection Validation**:
```go
// createScalarCollectionTask - for ScalarCollection (without vectors)
type createScalarCollectionTask struct {
    baseTask
    schema *ScalarCollectionSchema
}

func (t *createScalarCollectionTask) validateSchema() error {
    // Ensure zero vector fields
    vectorFields := len(typeutil.GetVectorFieldSchemas(t.schema.Fields))
    if vectorFields > 0 {
        return merr.WrapErrParameterInvalidMsg(
            "ScalarCollection cannot contain vector fields")
    }

    // Ensure at least one scalar field
    if len(t.schema.Fields) < 2 {
        return merr.WrapErrParameterInvalidMsg(
            "ScalarCollection must have at least one scalar field")
    }

    log.Info("creating scalar collection",
        zap.String("collection", t.schema.Name),
        zap.Int("scalar_fields", len(t.schema.Fields)-1))

    return nil
}
```

### 2. Type Safety (No Runtime Checks Needed)

With separate `Collection` and `ScalarCollection` types:

- **Collection** type: Has `Search()` method, requires vector fields
- **ScalarCollection** type: NO `Search()` method - compile-time type safety

**No runtime validation needed** - the type system prevents calling search on ScalarCollection:

```go
// Collection - has Search() method
func (c *Collection) Search(...) error {
    // Existing search logic, unchanged
}

// ScalarCollection - Search() method does NOT exist
// Attempting sc.Search() results in compile error:
// "ScalarCollection has no method Search"
```

This is superior to runtime checks because errors are caught at compile time, not runtime.

### 3. Collection Metadata

**File**: `internal/rootcoord/create_scalar_collection_task.go` (NEW)

Add separate task for ScalarCollection creation:
```go
// NEW task for ScalarCollection creation
type createScalarCollectionTask struct {
    baseTask
    req    *milvuspb.CreateScalarCollectionRequest
    schema *ScalarCollectionSchema
}

func (t *createScalarCollectionTask) Execute(ctx context.Context) error {
    // Create collection metadata with CollectionType flag
    meta := &collectionMeta{
        CollectionID:   uniqueID,
        Name:           t.schema.Name,
        Schema:         t.schema,
        CollectionType: ScalarCollectionType,  // NEW: Distinguishes from VectorCollectionType
        CreateTime:     now,
    }

    // Store metadata in meta store
    // Rest of collection creation logic (similar to regular collection)
}
```

**File**: `pkg/proto/schema.proto`

Add collection type enum:
```protobuf
enum CollectionType {
    VectorCollection = 0;   // Default, existing collections
    ScalarCollection = 1;   // NEW: Collections without vectors
}

message CollectionMeta {
    int64 collection_id = 1;
    string name = 2;
    CollectionType type = 3;  // NEW field
    // ... other fields
}
```

### 4. Extend Search API with Query Subexpressions

**Building on Current Architecture**:

Based on `internal/proxy/task_search.go` and `milvus-proto/proto/milvus.proto`, we extend the existing filter expression parser to support query subexpressions.

**File**: `pkg/proto/milvuspb/milvus.proto`

**Current SearchRequest** (no changes to protobuf - only parser extension):
```protobuf
message SearchRequest {
  string collection_name = 2;
  string dsl = 5;                      // Filter expression (extended to support query subexpressions)
  repeated string output_fields = 8;   // Extended to support qualified names like "collection.field"
  repeated KeyValuePair search_params = 9;
  bytes placeholder_group = 10;
  // ... other fields
}
```

**NO Protobuf Changes Required**:
- The `dsl` field (filter expression) is extended to support the new `query ... where ... select ...` syntax
- The `output_fields` array is extended to accept qualified names like `"shop_metadata.shop_name"`
- Parser detects and handles query subexpressions automatically
- 100% backward compatible - existing filter expressions continue to work unchanged

**File**: `internal/proxy/task_search.go`

**Extend searchTask structure**:
```go
type searchTask struct {
    // Existing fields...
    ctx context.Context
    *internalpb.SearchRequest
    request *milvuspb.SearchRequest

    // NEW: Query subexpression fields
    querySubexpressions []QuerySubexpression  // Detected query subexpressions from expr
    translatedExpr      string                // Filter expression after query translation
    crossCollectionOutputFields map[string][]string  // output_fields grouped by collection
}

// Query subexpression detected in filter
type QuerySubexpression struct {
    CollectionName string      // Target collection to query
    FilterExpr     string      // Filter condition for the query
    SelectField    string      // Field to select (for IN clause)
    TargetField    string      // Field in primary collection to match
}
```

**Parse Query Subexpressions in searchTask.PreExecute**:

Extend the existing `PreExecute` method to detect and handle query subexpressions:

```go
func (t *searchTask) PreExecute(ctx context.Context) error {
    // Existing code: validate collection, partitions, etc.

    // NEW: Detect and parse query subexpressions in expr
    originalExpr := t.request.GetDsl()
    if originalExpr != "" {
        // Check if expression contains query subexpressions
        if containsQuerySubexpression(originalExpr) {
            // Parse query subexpressions
            querySubexprs, err := parseQuerySubexpressions(originalExpr)
            if err != nil {
                return errors.Wrap(err, "failed to parse query subexpressions")
            }
            t.querySubexpressions = querySubexprs

            // Execute queries and translate expression
            translatedExpr, err := t.executeAndTranslateQueries(ctx, originalExpr)
            if err != nil {
                return errors.Wrap(err, "failed to execute query subexpressions")
            }
            t.translatedExpr = translatedExpr
        } else {
            // No query subexpressions - use original expr
            t.translatedExpr = originalExpr
        }
    }

    // Parse output_fields to detect cross-collection field requests
    t.crossCollectionOutputFields = parseOutputFields(t.request.GetOutputFields())

    // Continue with existing plan generation using translated expression
    return t.generateSearchPlan(ctx)
}
```

**Query Subexpression Parser**:

**File**: `internal/parser/planparserv2/query_subexpr_parser.go` (new)

```go
// Detect if expression contains query subexpressions
// Example: "shop_id in (query shop_metadata where shop_status == 'active' select shop_id)"
func containsQuerySubexpression(expr string) bool {
    return strings.Contains(expr, " in (query ")
}

// Parse query subexpressions from filter expression
// Input: "shop_id in (query shop_metadata where shop_status == 'active' and shop_rating > 4.0 select shop_id) and price >= 10.0"
// Output: []QuerySubexpression
func parseQuerySubexpressions(expr string) ([]QuerySubexpression, error) {
    subexprs := make([]QuerySubexpression, 0)

    // Regular expression to match: field in (query collection where condition select field)
    // Pattern: <target_field> in \(query <collection> where <condition> select <field>\)
    pattern := regexp.MustCompile(`(\w+)\s+in\s+\(query\s+(\w+)\s+where\s+(.+?)\s+select\s+(\w+)\)`)

    matches := pattern.FindAllStringSubmatch(expr, -1)
    for _, match := range matches {
        if len(match) != 5 {
            return nil, fmt.Errorf("invalid query subexpression format: %s", match[0])
        }

        subexprs = append(subexprs, QuerySubexpression{
            TargetField:    match[1],  // e.g., "shop_id"
            CollectionName: match[2],  // e.g., "shop_metadata"
            FilterExpr:     match[3],  // e.g., "shop_status == 'active' and shop_rating > 4.0"
            SelectField:    match[4],  // e.g., "shop_id"
        })
    }

    return subexprs, nil
}
```

**Proxy Implementation (Simple Translation)**:

```go
// Execute query subexpressions and translate to filter predicate
// This is exactly what applications do manually - just automated in proxy
func (t *searchTask) executeAndTranslateQueries(ctx context.Context, originalExpr string) (string, error) {
    translatedExpr := originalExpr

    // For each query subexpression detected
    for _, querySubexpr := range t.querySubexpressions {
        // Step 1: Execute query on scalar collection (same as application would)
        fieldValues, err := t.queryScalarCollection(ctx,
            querySubexpr.CollectionName,
            querySubexpr.FilterExpr,
            querySubexpr.SelectField)
        if err != nil {
            return "", err
        }

        // Step 2: Check result size limit (same limit applications face)
        if len(fieldValues) > maxResultSetSize {
            return "", fmt.Errorf("query returned %d results, exceeding limit. Use more selective filter.", len(fieldValues))
        }

        // Step 3: Simple string replacement (query... → [IDs])
        // Replace: "shop_id in (query shop_metadata where status == 'active' select shop_id)"
        // With:    "shop_id in [101, 205, 387, 9823]"
        translatedExpr = strings.Replace(translatedExpr,
            querySubexpr.OriginalText,
            fmt.Sprintf("%s in %v", querySubexpr.TargetField, fieldValues),
            1)
    }

    return translatedExpr, nil
}
```

**That's it!** The proxy just does what applications do:
1. Query scalar collection
2. Get the field values
3. Use them in the vector search filter

**Handling Large Result Sets**:

The query-to-filter translation can produce very large ID arrays:

```python
# Example: Most shops are active (40,000 out of 50,000)
expr = "shop_id in (query shop_metadata where shop_status == 'active' select shop_id)"

# Query returns: 40,000 shop IDs
# Translation produces: "shop_id in [40,000 IDs]"  # Potentially too large
```

**Solution: Return error if result set exceeds threshold**

If the query subexpression returns more IDs than a configurable limit (default: 10,000), return an error:

```python
# Configuration parameter
proxy:
  maxQuerySubexprResultSize: 10000  # Default limit for query subexpression result size

# Error message example:
# "Query subexpression on 'shop_metadata' returned 40,000 results, exceeding limit of 10,000.
#  Please write a more selective filter."
```

Users should write selective filters in query subexpressions, just like they would in application-side two-pass search.

**Cross-Collection Output Fields (Optional)**:

If `output_fields` contains fields from other collections (e.g., `"shop_metadata.shop_name"`), proxy fetches them after the search:

```go
func (t *searchTask) PostExecute(ctx context.Context) error {
    // Existing code: reduce results from shards

    // NEW: If output_fields has cross-collection fields, fetch them
    if hasCrossCollectionFields(t.request.GetOutputFields()) {
        if err := t.fetchCrossCollectionFields(ctx); err != nil {
            return err
        }
    }

    // Continue with existing code: format results
}

func (t *searchTask) fetchCrossCollectionFields(ctx context.Context) error {
    // This is exactly what applications do after search
    // 1. Extract IDs from search results (e.g., shop_ids)
    // 2. Query the other collection with those IDs
    // 3. Merge the results

    // Example: If output_fields includes "shop_metadata.shop_name"
    shopIDs := extractUniqueValues(t.results, "shop_id")  // [101, 205, 387]

    shopData := query(shop_metadata, expr=f"shop_id in {shopIDs}", output_fields=["shop_id", "shop_name"])

    mergeIntoResults(t.results, shopData, joinKey="shop_id")

    return nil
}
```

**That's it!** Same as what applications do manually - just automated.

**Limitations (Keep It Simple)**:
- Query subexpressions only in filter (not nested)
- Result size limit (same limit applications face with IN clauses)
- No aggregations or complex joins - just simple ID matching

### 5. Index Creation

**File**: `internal/proxy/task_index.go`

ScalarCollection has its own index creation method that only accepts scalar index types:

```go
// ScalarCollection.CreateIndex - only accepts scalar index types
func (sc *ScalarCollection) CreateIndex(fieldName string, indexParams map[string]string) error {
    indexType := indexParams["index_type"]

    // Validate index type - only scalar indexes allowed
    if !isScalarIndexType(indexType) {
        return merr.WrapErrParameterInvalidMsg(
            "ScalarCollection only supports scalar indexes (STL_SORT, Trie, Inverted)")
    }

    // Create scalar index
    // ... existing scalar index creation logic
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

No changes needed to `Collection.CreateIndex()` - it continues to support both vector and scalar indexes.

### 6. Query Optimization

**File**: `internal/querynodev2/segments/segment_loader.go`

ScalarCollection segments skip vector-specific loading based on CollectionType:

```go
func (loader *segmentLoader) loadSegment(ctx context.Context, segment *Segment) error {
    // Check collection type from metadata
    collectionMeta := loader.getCollectionMeta(segment.CollectionID)

    if collectionMeta.CollectionType == ScalarCollectionType {
        // ScalarCollection: Only load scalar data, skip vector data entirely
        if err := loader.loadScalarData(ctx, segment); err != nil {
            return err
        }
    } else {
        // VectorCollection: Load both vector and scalar data (existing behavior)
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

Scalar collections have simpler storage requirements:
- No vector index files needed
- Only scalar field data and scalar indexes
- Reduced segment size and memory footprint
- Faster load times

## Compatibility, Deprecation, and Migration Plan

### Backward Compatibility

**Impact**: None. This is a purely additive feature.

- Existing collections with vector fields: No changes, work exactly as before
- Existing validation: Relaxed only if configuration enables metadata collections
- Existing APIs: Fully compatible, no breaking changes

### Migration Path

No migration needed. Users can:
1. Continue using denormalized patterns (existing behavior)
2. Opt-in to metadata collections for new use cases
3. Gradually refactor to normalized patterns if desired

### Configuration Rollout

No configuration rollout needed. ScalarCollection is a new additive type that doesn't affect existing Collection behavior. Users can start using ScalarCollection immediately when the feature is released.

## Test Plan

### Unit Tests

**File**: `internal/core/scalar_collection_test.go`

1. **Test**: Create ScalarCollection with zero vector fields
   - **Expected**: Success

2. **Test**: Create ScalarCollection with vector fields
   - **Expected**: Validation error - ScalarCollection cannot contain vector fields

3. **Test**: Create Collection with zero vector fields
   - **Expected**: Validation error - Collection requires at least one vector field (existing behavior unchanged)

4. **Test**: Insert/Upsert/Delete on ScalarCollection
   - **Expected**: All operations succeed

5. **Test**: Query on ScalarCollection
   - **Expected**: Returns filtered results correctly

6. **Test**: ScalarCollection has no search() method
   - **Expected**: Compile-time error (method does not exist)

7. **Test**: Create vector index on ScalarCollection
   - **Expected**: Error rejecting vector index types

8. **Test**: Create scalar index on ScalarCollection
   - **Expected**: Success for scalar index types (STL_SORT, Inverted, etc.)

### Integration Tests

**File**: `tests/python_client/testcases/test_scalar_collection.py`

```python
from pymilvus import ScalarCollection, ScalarCollectionSchema, FieldSchema, DataType

class TestScalarCollection:
    def test_create_scalar_collection(self):
        """Test creating ScalarCollection without vector fields"""
        schema = ScalarCollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True),
            FieldSchema("name", DataType.VARCHAR, max_length=100),
            FieldSchema("status", DataType.VARCHAR, max_length=50),
        ])
        scalar_coll = ScalarCollection("scalar_test", schema=schema)
        assert scalar_coll is not None

    def test_insert_query_metadata(self):
        """Test insert and query operations on ScalarCollection"""
        # Insert data
        scalar_coll.insert([[1, 2, 3],
                           ["shop1", "shop2", "shop3"],
                           ["active", "active", "inactive"]])

        # Query with filter
        results = scalar_coll.query(
            expr="status == 'active'",
            output_fields=["id", "name"]
        )
        assert len(results) == 2

    def test_search_method_not_available(self):
        """Test that ScalarCollection has no search() method"""
        with pytest.raises(AttributeError) as exc:
            scalar_coll.search(...)
        assert "'ScalarCollection' object has no attribute 'search'" in str(exc.value)

    def test_application_level_join(self):
        """Test joining Collection (vector) with ScalarCollection"""
        # Vector search on Collection
        items = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=100,
            output_fields=["shop_id"]
        )

        # Metadata query on ScalarCollection
        shop_ids = [hit.entity.get("shop_id") for hit in items[0]]
        shops = shop_scalar_collection.query(
            expr=f'id in {shop_ids}',
            output_fields=["id", "status"]
        )

        # Verify join works
        assert len(shops) > 0

    def test_multi_entity_join(self):
        """Test joining with multiple scalar collections (shop, merchant, consumer)"""
        # Step 1: Get consumer preferences
        consumer = consumer_profile_collection.query(
            expr="user_id == 123",
            output_fields=["price_range_min", "price_range_max", "preferred_categories"]
        )[0]

        # Step 2: Vector search with consumer price filter
        items = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=50,
            expr=f'price >= {consumer["price_range_min"]} AND price <= {consumer["price_range_max"]}',
            output_fields=["item_id", "shop_id", "merchant_id", "price"]
        )

        # Step 3: Batch query metadata
        shop_ids = list(set([hit.entity.get("shop_id") for hit in items[0]]))
        merchant_ids = list(set([hit.entity.get("merchant_id") for hit in items[0]]))

        shops = shop_metadata_collection.query(
            expr=f'shop_id in {shop_ids}',
            output_fields=["shop_id", "shop_status"]
        )

        merchants = merchant_metadata_collection.query(
            expr=f'merchant_id in {merchant_ids}',
            output_fields=["merchant_id", "verification_status"]
        )

        # Verify all joins successful
        assert len(items[0]) > 0
        assert len(shops) > 0
        assert len(merchants) > 0

    def test_consumer_preference_update_efficiency(self):
        """Test that updating consumer preferences is O(1) operation"""
        import time

        # Create consumer profile
        user_id = 999
        consumer_profile_collection.insert([[user_id], ["John"], ["electronics"]])

        # Measure update time (should be fast - single record)
        start = time.time()
        consumer_profile_collection.upsert([{
            "user_id": user_id,
            "preferred_categories": "electronics,clothing,home"
        }])
        update_time = time.time() - start

        # Should complete quickly
        assert update_time < 0.1, f"Update took {update_time}s, expected to be fast"
```

### E2E Tests

| Test Case | Expected Behavior |
|:----------|:-----------------|
| Create scalar collection | Success with proper logging |
| Create collection with vectors (existing) | Works exactly as before (backward compatible) |
| Query scalar collection with complex filter | Returns correct results |
| Search on scalar collection | Returns clear error message |
| Insert 1M records into scalar collection | Completes successfully with good performance |
| Create scalar index on scalar collection | Index created successfully |
| Attempt vector index on scalar collection | Returns appropriate error |
| Requery-based join (2 collections) | Single query successfully joins and returns combined data |
| Multi-entity join (items + shop + merchant + consumer) | All metadata correctly joined via requery |
| Consumer preference update (single record) | Updates quickly, subsequent queries see new values |
| Shop metadata update efficiency | Update 1 shop record instead of many items |
| Merchant batch update | Updates complete faster than updating all item records |
| Partition key on scalar collection | Works correctly for multi-tenancy |
| Clustering key on scalar collection | Improves query performance |
| Consumer profile scalar collection with many users | Query by user_id returns quickly |
| Concurrent updates to shop/merchant/consumer scalar data | No conflicts, consistent reads |

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

   - **Consumer preference update**:
     - Denormalized: Update multiple user-item interaction records
     - Normalized: Update 1 consumer profile record
     - **Expected**: Faster with normalized approach

2. **Storage Efficiency**: Measure storage overhead
   - **Denormalized approach**:
     - Significant redundant metadata across all items

   - **Normalized approach**:
     - Minimal metadata storage in dedicated collections

   - **Expected**: Significant storage reduction for metadata

3. **Query Performance**: Measure query latency with requery-based joins
   - **Denormalized (single collection)**: 1 vector search with all metadata inline
   - **Normalized (requery joins)**: 1 vector search + requery to multiple scalar collections
   - **Expected**: Requery adds minimal overhead (uses batch queries like applications would)
   - **Trade-off analysis**: Minimal latency increase for significantly faster updates and storage reduction

4. **E-commerce Recommendation End-to-End**:
   - **Workflow**: Single search query with requery joins to shop/merchant/consumer collections
   - **Measurement**:
     - Query latency with requery joins
     - Requery overhead for multiple collection joins
     - Throughput for recommendations
     - Concurrent user support
   - **Expected**: Requery-based joins perform comparably to denormalized queries with proper indexing

5. **Batch Update Scenarios**:
   - **Scenario**: Update all shops in a region
   - Denormalized: Many upsert operations across item records
   - Normalized: Minimal upsert operations on shop records
   - **Expected**: Complete significantly faster

6. **Metadata Query Performance**:
   - Query consumer profiles by user_id with scalar index
   - **Expected**: Fast point queries
   - Query shops by region with inverted index
   - **Expected**: Fast filtered queries

## Rejected Alternatives

### Alternative 1: General-Purpose SQL JOIN Support

**Idea**: Implement full SQL-style JOIN operations (LEFT, RIGHT, OUTER, nested joins, aggregations across joins, etc.)

**Rejected Because**:
- Too complex for a vector database - conflicts with Milvus's vector-first design
- Massive architectural change to query planner and execution engine
- Distributed join complexity (shuffle, broadcast, network overhead across multiple nodes)
- Performance unpredictable and difficult to optimize in distributed settings
- Would require query optimizer, cost-based planning, join reordering, etc.

**What We're Doing Instead**: Lightweight requery-based joins
- Limited to simple equality joins (inner join only)
- Builds on existing requery infrastructure
- Predictable performance via batching
- Keeps implementation simple and maintainable

### Alternative 2: Mandatory Dummy Vector Fields

**Idea**: Require users to add dummy/zero vector fields to meet current validation.

**Rejected Because**:
- Wastes storage and memory
- Confuses users ("why do I need a vector field?")
- Index creation overhead for unused vectors
- Architectural smell - working around limitation instead of fixing it

### Alternative 3: Application-Level Joins Only

**Idea**: Support scalar collections but require users to perform joins in application code (multiple queries + manual join logic).

**Rejected Because**:
- Poor developer experience - requires complex application logic
- Multiple round trips to Milvus (higher network overhead)
- Error-prone manual join implementation

**What We're Doing Instead**: Requery-based joins inside Milvus
- Single query API - cleaner developer experience
- Same execution as application-side (batching, parallelization), but inside Milvus
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

5. **Existing Requery Stage** (Key Infrastructure):
   - `internal/proxy/search_pipeline.go:requeryOperator` - Existing requery operator implementation
   - `internal/proxy/search_pipeline.go:115` - `requeryOp` constant and operator factory
   - `internal/proxy/task_search.go:51-55` - `requeryThreshold` and requery decision logic
   - **Current capability**: Retrieves additional output fields from same collection after search
   - **Proposed extension**: Cross-collection data retrieval for joins (extends existing mechanism)

6. **Expression Grammar**:
   - `docs/design_docs/20220105-query_boolean_expr.md` - Boolean expression grammar
   - `internal/parser/planparserv2/` - Expression parser implementation
   - Proposed extension: filterExpr/joinExpr/outputExpr three-parameter grammar

7. **Related Features and Precedents**:
   - **Nullable vectors** (Key Precedent): Milvus already supports nullable vectors, demonstrating the system can handle records without vector data. This existing capability validates that scalar-only collections should not be a big issue—we're extending an existing concept.
   - Partition keys: Multi-tenancy support
   - Clustering compaction: Scalar field optimization
   - Dynamic fields: Flexible schema

8. **Maintainer and Community Feedback** (Issue #46153):
   - **Key maintainer insight 1**: Milvus already supports nullable vectors, so introducing scalar-only collections should not be a big issue
   - **Key maintainer insight 2**: Milvus already has a requery stage that retrieves additional fields from the same collection; extend it to support retrieving data from other collections as well
   - Pain point validated: Denormalization challenges clearly articulated
   - Three-parameter join grammar preferred (filterExpr, joinExpr, outputExpr)
   - Requery-based joins preferred over general SQL joins
   - Keep joins simple: Equality joins only, no complex join types
   - Build on existing search API infrastructure

---

## Implementation Checklist

### Phased Rollout Strategy

**Phase 1: ScalarCollection Type (Core Feature)**
- Minimum viable product: Users can create ScalarCollection and perform CRUD operations
- Enables application-level joins immediately
- No query subexpressions yet - that's Phase 2

**Phase 2: Query Subexpressions in Filter (Enhancement)**
- Extend filter expression parser to support query subexpressions
- Proxy handles query-search reform (executes queries, translates to IN clauses)
- Extend output_fields to support cross-collection qualified names
- Extend requery stage for cross-collection data retrieval
- Enables single-query joins inside Milvus via natural SQL-like syntax

---

### Phase 1: ScalarCollection Type Implementation

**Core Type System**
- [ ] Create `ScalarCollection` type in `internal/core/src/scalar_collection.go` (separate from Collection)
- [ ] Create `ScalarCollectionSchema` type with validation (zero vector fields required)
- [ ] Add `CollectionType` enum to `pkg/proto/schema.proto` (VectorCollection, ScalarCollection)
- [ ] Create `createScalarCollectionTask` in `internal/rootcoord/create_scalar_collection_task.go`
- [ ] Implement ScalarCollection methods: Insert(), Upsert(), Delete(), Query(), CreateIndex(), Load(), Release(), Drop()
- [ ] Ensure Collection validation remains unchanged (still requires vector fields)
- [ ] Update collection metadata to track CollectionType field

**Protobuf and API**
- [ ] Add `CreateScalarCollectionRequest` to `milvus.proto`
- [ ] Add `CollectionType` field to `CollectionMeta` message
- [ ] Update RootCoord API to handle ScalarCollection creation
- [ ] Update Proxy to route ScalarCollection operations correctly

### Phase 2: Query Subexpressions in Filter Implementation

**NO Protobuf Changes Required**:
- Existing `dsl` field is extended to support query subexpressions
- Existing `output_fields` is extended to support qualified names
- 100% backward compatible

**Parser Changes** (`internal/parser/planparserv2/`):
- [ ] Create `query_subexpr_parser.go` with query subexpression parsing functions
- [ ] Implement `containsQuerySubexpression()` to detect query patterns
- [ ] Implement `parseQuerySubexpressions()` to extract query subexpressions from expr
- [ ] Support syntax: `field in (query <collection> where <condition> select <field>)`
- [ ] Add validation for query subexpressions (collection existence, field validity)
- [ ] Add validation for result set size limits

**Search Task Changes** (`internal/proxy/task_search.go`):
- [ ] Add `querySubexpressions []QuerySubexpression` field to `searchTask` struct
- [ ] Add `translatedExpr string` field for the filter after query translation
- [ ] Add `crossCollectionOutputFields map[string][]string` for output_fields parsing
- [ ] Create `QuerySubexpression` struct (CollectionName, FilterExpr, SelectField, TargetField)
- [ ] Extend `PreExecute()` to detect and parse query subexpressions in expr
- [ ] Implement `executeAndTranslateQueries()` to execute queries and translate expr
- [ ] Implement `executeScalarCollectionQuery()` to query scalar collections
- [ ] Extend `PostExecute()` to handle cross-collection output fields
- [ ] Implement `executeCrossCollectionRequery()` for fetching additional fields
- [ ] Implement `mergeCrossCollectionData()` to merge data into results

**Query Execution**:
- [ ] Implement query subexpression detection in proxy
- [ ] Execute scalar collection queries in PreExecute phase
- [ ] Translate query results to IN clauses for vector search
- [ ] Support multiple query subexpressions in single expr
- [ ] Handle result set size limits (maxQuerySubexprResultSize configuration)
- [ ] Support cross-collection field fetching via requery stage
- [ ] Implement qualified field names in results (collection.field)

### SDK Changes

**ScalarCollection Type (all SDKs)**:
- [ ] Python SDK: Create `ScalarCollection` class (separate from `Collection`)
- [ ] Python SDK: Create `ScalarCollectionSchema` class
- [ ] Python SDK: Implement ScalarCollection methods (no `search()` method)
- [ ] Go SDK: Create `ScalarCollection` struct (separate from `Collection`)
- [ ] Go SDK: Create `ScalarCollectionSchema` struct
- [ ] Go SDK: Implement ScalarCollection methods (no `Search()` method)
- [ ] Java SDK: Create `ScalarCollection` class
- [ ] Node.js SDK: Create `ScalarCollection` class
- [ ] RESTful API: Add `/scalar_collection/create` endpoint

**Query Subexpressions in Filter (Collection search API)**:
- [ ] Python SDK: Update examples to show query subexpression syntax in `expr` parameter
- [ ] Python SDK: Update examples to show qualified field names in `output_fields`
- [ ] Python SDK: Documentation for query subexpression grammar
- [ ] Go SDK: Update examples and documentation for query subexpressions
- [ ] Java SDK: Update examples and documentation
- [ ] Node.js SDK: Update examples and documentation
- [ ] RESTful API: Update documentation (no code changes - parser handles it)

### Testing

**ScalarCollection Type Tests**:
- [ ] Unit tests for ScalarCollection type creation and validation
- [ ] Unit tests: ScalarCollection rejects schemas with vector fields
- [ ] Unit tests: Collection still requires vector fields (unchanged behavior)
- [ ] Integration tests for ScalarCollection CRUD operations (Insert, Upsert, Delete, Query)
- [ ] Integration tests: Verify ScalarCollection has no search() method (compile-time)
- [ ] Integration tests for ScalarCollection index creation (scalar indexes only)
- [ ] Integration tests: ScalarCollection rejects vector index creation
- [ ] E2E tests for ScalarCollection load/release/drop operations

**Query Subexpression Tests**:
- [ ] Unit tests for query subexpression parsing
- [ ] Unit tests for query execution and translation to IN clauses
- [ ] Unit tests for result set size limit enforcement
- [ ] Integration tests for single query subexpression in filter
- [ ] Integration tests for multiple query subexpressions in single filter
- [ ] Integration tests for cross-collection output fields (requery)
- [ ] E2E tests for e-commerce recommendation scenario with query subexpressions
- [ ] Performance tests for query translation overhead and requery batching

**General Tests**:
- [ ] Performance benchmarks (update efficiency, storage, join overhead)
- [ ] Backward compatibility tests (existing Collection type unaffected)

### Documentation

**ScalarCollection Type Documentation**:
- [ ] User guide: ScalarCollection vs Collection - when to use each type
- [ ] User guide: Creating and managing ScalarCollection
- [ ] API reference: ScalarCollection class/struct reference (all SDKs)
- [ ] API reference: ScalarCollectionSchema reference
- [ ] Tutorial: Creating your first ScalarCollection
- [ ] Tutorial: Application-level joins with ScalarCollection
- [ ] Best practices: When to use ScalarCollection vs Collection
- [ ] Migration guide: Refactoring from denormalized to normalized pattern with ScalarCollection

**Query Subexpression Documentation**:
- [ ] User guide: Query subexpression syntax in filter expressions
- [ ] User guide: Grammar specification - `field in (query <collection> where <condition> select <field>)`
- [ ] User guide: Cross-collection output fields with qualified names
- [ ] API reference: Updated Collection.search() with query subexpression examples
- [ ] API reference: Query subexpression specification and complete examples
- [ ] Tutorial: E-commerce multi-entity recommendation with query subexpressions
- [ ] Tutorial: Step-by-step query subexpression examples (simple to complex)
- [ ] Best practices: Writing selective filters in query subexpressions
- [ ] Best practices: Indexing fields used in query subexpressions
- [ ] Best practices: Understanding result set size limits
- [ ] Performance guide: Query translation overhead and optimization tips
- [ ] Migration guide: Converting application-level joins to query subexpressions

### Release
- [ ] Release notes entry for Phase 1 (ScalarCollection type)
- [ ] Release notes entry for Phase 2 (Query subexpressions in filter) - if included
- [ ] Community announcement
- [ ] Blog post with e-commerce example demonstrating query subexpressions

---

## Summary for Reviewers

**What's being proposed:**
1. **Phase 1**: New `ScalarCollection` type (separate from `Collection`) - core feature
2. **Phase 2**: Query subexpressions in filter expressions - enhancement
   - Extend existing `expr` parameter to support `field in (query <collection> where <condition> select <field>)` syntax
   - Proxy-based query-search reform: execute queries first, translate to IN clauses
   - Extend `output_fields` to support cross-collection qualified names
   - No protobuf changes required - 100% backward compatible

**What's NOT changing:**
- Existing `Collection` type behavior: 100% unchanged
- Existing `Collection` validation: Still requires vectors
- Existing search API parameters: Continue to work as-is
- Backward compatibility: Zero breaking changes

**Key decision for review:**
- Separate `ScalarCollection` type vs single type with runtime detection
  - Recommendation: Separate type for compile-time safety
- Query subexpressions in filter vs three-parameter join grammar
  - Recommendation: Query subexpressions (per maintainer feedback) - simpler, more SQL-like, reuses existing API

**What happens next:**
1. Get maintainer approval on query-subexpression approach
2. Implement Phase 1 (ScalarCollection type)
3. Implement Phase 2 (Query subexpressions) based on community feedback

**Questions? Please comment on:** https://github.com/milvus-io/milvus/issues/46153
