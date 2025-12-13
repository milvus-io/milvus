# MEP: Scalar Collections (Collections Without Vector Fields)

Current state: Under Discussion

ISSUE: https://github.com/milvus-io/milvus/issues/46153 (community feedback incorporated)

Keywords: scalar collection, scalar-only, metadata, relational, join, normalization

Released: TBD

## Summary

Enable creation of **scalar collections** (collections without vector fields) and extend Milvus's **search API** with a **three-parameter join grammar** to support lightweight joins between collections. This allows normalized data modeling with efficient, single-query joins inside Milvus.

**Building on Existing Capabilities**: This proposal leverages two existing Milvus features: (1) nullable vector support demonstrates the system already handles records without vector data, and (2) the existing requery stage that we extend for cross-collection data retrieval. We're enhancing existing infrastructure, not building from scratch.

**Key Features**:
1. **Scalar Collections**: Collections without vector fields for storing pure metadata
2. **Three-Parameter Join Grammar**: Adds NEW parameters to search API:
   - `filterExpr` - Filter on primary and scalar collections (NEW, works alongside existing `expr`/`dsl`)
   - `joinExpr` - Join clauses: `"join <collection> where <condition>"` (NEW)
   - `outputExpr` - Output fields from all collections: `"collection.field, collection.field"` (NEW)
3. **Requery-Based Joins**: Executes joins inside Milvus via **extended requery stage** (existing mechanism)
4. **Fully Backward Compatible**: All existing parameters (`dsl`, `expr`, `output_fields`) continue to work unchanged

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
2. **Requery-Based Joins**: Extend Milvus's existing requery stage to retrieve data from other collections via simple join expressions

This approach keeps joins lightweight and builds on existing infrastructure, avoiding the complexity of general-purpose SQL joins.

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
4. **Single-Query Joins**: Requery-based joins execute inside Milvus - no application-level join logic needed
5. **Better Performance**: Minimal join overhead vs multiple round trips
6. **Cleaner Architecture**: Separate concerns between vector collections and scalar collections
7. **Flexibility**: Support various relational patterns (lookup tables, configuration, user profiles)
8. **Natural Data Modeling**: Store entities that don't have embeddings without forcing dummy vectors
9. **Consistent Updates**: Single source of truth for metadata ensures consistency
10. **Lightweight Joins**: Simple equality joins only - avoids complexity of general SQL joins

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

### Schema Changes

Remove the hard requirement that collections must contain at least one vector field.

**Important Context**: Milvus already supports **nullable vectors** (vectors that can be null in records), which demonstrates that the system can handle collections where not every record has vector data. Scalar-only collections are a natural extension of this capability—instead of nullable vectors, we simply have zero vector fields. This makes the implementation straightforward since the infrastructure already handles the concept of "no vector data for some records."

**Before** (current validation in `internal/proxy/task.go:391-393`):
```go
if vectorFields == 0 {
    return merr.WrapErrParameterInvalidMsg("schema does not contain vector field")
}
```

**After** (proposed):
```go
// Allow collections without vector fields (scalar collections)
// They can only be queried via Query API, not Search API
if vectorFields == 0 {
    log.Info("Creating scalar collection without vector fields",
        zap.String("collection", t.schema.Name))
}
```

### API Behavior

**Supported Operations on Scalar Collections**:
- `CreateCollection()` - Allow schemas with zero vector fields (creates a scalar collection)
- `Insert()`, `Upsert()`, `Delete()` - Full support
- `Query()` - Full support for scalar filtering and retrieval
- `CreateIndex()` - Support scalar indexes only (STL_SORT, Inverted, Trie)

**Unsupported Operations**:
- `Search()` - Return clear error: "Search operation not supported on scalar collections (no vector fields)"
- `CreateIndex()` with vector index types - Return error for vector index requests

### SDK Changes

**Python SDK Example**:
```python
from pymilvus import CollectionSchema, FieldSchema, DataType, Collection

# Scalar collection - NO vector fields
# Same Collection API, but schema has no vector fields
shop_schema = CollectionSchema([
    FieldSchema("shop_id", DataType.INT64, is_primary=True),
    FieldSchema("shop_name", DataType.VARCHAR, max_length=256),
    FieldSchema("shop_status", DataType.VARCHAR, max_length=50),
    FieldSchema("shop_region", DataType.VARCHAR, max_length=100),
    FieldSchema("merchant_id", DataType.INT64),
    FieldSchema("rating", DataType.FLOAT),
    FieldSchema("is_verified", DataType.BOOL),
], description="Shop scalar collection")

# Creates a scalar collection (detected automatically from schema)
shop_metadata = Collection("shop_metadata", schema=shop_schema)

# Supported: Query operation
results = shop_metadata.query(
    expr="shop_status == 'active' AND rating > 4.0",
    output_fields=["shop_id", "shop_name", "merchant_id"]
)

# Unsupported: Search operation (will raise clear error)
# shop_metadata.search(...)  # Error: Search not supported on scalar collections
```

**Requery-Based Join Example** (New Feature):

Based on maintainer feedback and the existing Milvus search architecture, we introduce a **clean three-parameter join grammar** that extends the current search API:

```python
# Current Milvus search API (today):
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},
    expr="price >= 10.0 AND price <= 100.0",      # Single filter expression
    output_fields=["item_id", "price", "shop_id"]  # Fields from same collection
)

# NEW: Search with join grammar (proposed)
# Extends existing API with three new expression parameters
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},

    # Three-part join grammar:
    filterExpr="items.price >= 10.0 AND items.price <= 100.0",

    joinExpr="""
        join shop_metadata where items.shop_id = shop_metadata.shop_id
        join merchant_metadata where items.merchant_id = merchant_metadata.merchant_id
    """,

    outputExpr="""
        items.item_id,
        items.price,
        shop_metadata.shop_name,
        shop_metadata.shop_rating,
        merchant_metadata.merchant_name
    """
)

# Results already include joined data - no application-level join needed!
for hit in results[0]:
    print(f"Item: {hit.entity['items.item_id']}")
    print(f"Shop: {hit.entity['shop_metadata.shop_name']} (rating: {hit.entity['shop_metadata.shop_rating']})")
    print(f"Merchant: {hit.entity['merchant_metadata.merchant_name']}")
```

**Join Grammar Design Rationale**:

Building on top of Milvus's existing search architecture (`internal/proxy/task_search.go`):

1. **filterExpr** (replaces `expr`): Filter on primary and scalar collections
   - Uses existing expression parser (`planparserv2.CreateSearchPlan`)
   - Same syntax as current `dsl` field
   - Can filter on both primary collection and joined scalar collections
   - Example: `"items.price >= 10.0"`

2. **joinExpr** (new): Join clauses to other collections
   - Extends plan parser to support `join <collection> where <condition>` syntax
   - Multiple joins supported (one per line)
   - Only equality joins: `collectionA.field = collectionB.field`
   - Example: `"join shop_metadata where items.shop_id = shop_metadata.shop_id"`

3. **outputExpr** (extends `output_fields`): Fields from any collection
   - Replaces array-based `output_fields` with expression-based field selection
   - Qualified names: `collection.field`
   - Example: `"items.item_id, shop_metadata.shop_name"`

**Alternative: Single Combined Expression** (if three parameters seem too verbose):
```python
# Alternative: Combine all three parts into one expr parameter with keywords
results = items_collection.search(
    data=[query_embedding],
    anns_field="item_embedding",
    param={"metric_type": "L2", "topk": 100},

    # Single expr with FILTER/JOIN/OUTPUT keywords
    expr="""
        FILTER items.price >= 10.0 AND items.price <= 100.0
        JOIN shop_metadata ON items.shop_id = shop_metadata.shop_id
        JOIN merchant_metadata ON items.merchant_id = merchant_metadata.merchant_id
        OUTPUT items.item_id, items.price,
               shop_metadata.shop_name, shop_metadata.shop_rating,
               merchant_metadata.merchant_name
    """
)
```

**Benefits of Requery-Based Joins**:
- ✅ **Single query**: No multiple round trips to Milvus
- ✅ **Automatic optimization**: Milvus can batch requery operations internally
- ✅ **Cleaner API**: No manual join logic in application code
- ✅ **Performance**: Lower overhead vs application-level joins
- ✅ **Simpler than SQL**: Limited to simple equality joins (no complex join types)

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

Add optional configuration parameter:
```yaml
# milvus.yaml
proxy:
  allowScalarCollections: true  # Default: true (enable the feature)
```

## Design Details

### Design Rationale

This proposal builds on two key insights from the Milvus maintainers:

1. **Nullable Vectors Precedent**: Milvus already supports nullable vectors, allowing records to exist without vector data. This demonstrates that the core system can handle collections where not all records have vectors. Scalar-only collections are a natural extension—instead of nullable vectors within a collection, we have zero vector fields entirely. This existing capability means introducing scalar collections should not be a big issue.

2. **Existing Requery Infrastructure**: Milvus already has a **requery stage** (`internal/proxy/search_pipeline.go:requeryOperator`) that retrieves additional output fields after the initial vector search completes. Currently, requery only fetches data from the same collection. The join feature simply **extends this existing mechanism** to support retrieving data from other collections. We're not building a new join engine—we're enhancing an existing post-processing stage.

These insights guide the implementation approach: leverage what already exists (nullable vector handling, requery pipeline) rather than introducing fundamentally new concepts.

### Architecture Summary

The join feature extends Milvus's existing search infrastructure with minimal changes.

**Key Insight**: Milvus **already has a requery stage** (`internal/proxy/search_pipeline.go:requeryOperator`) that retrieves additional fields after the initial search completes. Currently, this requery stage only supports retrieving fields from the **same collection**. Our proposal simply **extends this existing mechanism** to support retrieving data from **other collections** as well. This makes the implementation straightforward—we're enhancing an existing feature, not building a new one from scratch.

**Current Flow**:
```
User → Search API (dsl, output_fields) → Plan Parser → Search → Requery (same collection) → Results
```

**New Flow with Joins**:
```
User → Search API (filterExpr, joinExpr, outputExpr) → Plan Parser → Search → Requery (cross-collection) → Join Merge → Results
```

**Key Extension Points**:
1. **API Layer** (external `milvus-proto` + internal `pkg/proto/internal.proto`): Add 3 new fields to SearchRequest
2. **Parser Layer** (`planparserv2/`): Parse join grammar into JoinSpec structures
3. **Execution Layer** (`task_search.go`): Extend PostExecute to fetch joined data via requery
4. **Requery Operator** (`search_pipeline.go`): Extend existing `requeryOperator` to support cross-collection queries
5. **Result Layer**: Merge joined fields into search results with qualified names

This approach leverages existing infrastructure (expression parser, query API, requery stage, result formatting) and only adds cross-collection requery logic.

### 1. Validation Changes

**File**: `internal/proxy/task.go`

**Current Code** (lines 386-393):
```go
vectorFields := len(typeutil.GetVectorFieldSchemas(t.schema))
if vectorFields > Params.ProxyCfg.MaxVectorFieldNum.GetAsInt() {
    return fmt.Errorf("maximum vector field's number should be limited to %d",
        Params.ProxyCfg.MaxVectorFieldNum.GetAsInt())
}

if vectorFields == 0 {
    return merr.WrapErrParameterInvalidMsg("schema does not contain vector field")
}
```

**Proposed Change**:
```go
vectorFields := len(typeutil.GetVectorFieldSchemas(t.schema))
if vectorFields > Params.ProxyCfg.MaxVectorFieldNum.GetAsInt() {
    return fmt.Errorf("maximum vector field's number should be limited to %d",
        Params.ProxyCfg.MaxVectorFieldNum.GetAsInt())
}

// Allow scalar collections (collections without vector fields)
if vectorFields == 0 {
    if !Params.ProxyCfg.AllowScalarCollections.GetAsBool() {
        return merr.WrapErrParameterInvalidMsg(
            "scalar collections disabled: schema does not contain vector field")
    }
    log.Info("creating scalar collection without vector fields",
        zap.String("collection", t.schema.Name),
        zap.Int("scalar_fields", len(t.schema.Fields)-1)) // exclude primary key
}
```

**File**: `internal/distributed/proxy/httpserver/utils.go`

**Current Code** (lines 826-827):
```go
if len(nameDims) == 0 && len(sch.Functions) == 0 && !partialUpdate {
    return nil, fmt.Errorf("collection: %s has no vector field or functions")
}
```

**Proposed Change**:
```go
if len(nameDims) == 0 && len(sch.Functions) == 0 && !partialUpdate {
    if !Params.ProxyCfg.AllowScalarCollections.GetAsBool() {
        return nil, fmt.Errorf("collection: %s has no vector field or functions")
    }
    log.Info("HTTP API: creating scalar collection", zap.String("collection", collectionName))
}
```

### 2. Search API Protection

**File**: `internal/proxy/task_search.go`

Add validation at the beginning of search operations:
```go
func (t *searchTask) PreExecute(ctx context.Context) error {
    // Existing validations...

    // Prevent search on scalar collections
    vectorFields := typeutil.GetVectorFieldSchemas(t.schema)
    if len(vectorFields) == 0 {
        return merr.WrapErrParameterInvalidMsg(
            "search operation not supported on scalar collections (no vector fields)")
    }

    // Rest of existing code...
}
```

### 3. Collection Metadata

**File**: `internal/rootcoord/create_collection_task.go`

Add collection property to track scalar-only collections:
```go
type collectionMeta struct {
    // Existing fields...
    IsScalarCollection bool `json:"is_scalar_collection"`
}

func (t *createCollectionTask) Execute(ctx context.Context) error {
    // Existing code...

    vectorFields := typeutil.GetVectorFieldSchemas(t.schema)
    isScalarCollection := len(vectorFields) == 0

    // Store scalar collection property
    // This can be used for query optimization and validation

    // Rest of existing code...
}
```

### 4. Extend Search API with Join Grammar

**Building on Current Architecture**:

Based on `internal/proxy/task_search.go` and `milvus-proto/proto/milvus.proto`, extend the SearchRequest protobuf:

**File**: `pkg/proto/milvuspb/milvus.proto`

**Current SearchRequest** (relevant fields):
```protobuf
message SearchRequest {
  string collection_name = 2;
  string dsl = 5;                      // Current: Single filter expression
  repeated string output_fields = 8;   // Current: Fields from same collection
  repeated KeyValuePair search_params = 9;
  bytes placeholder_group = 10;
  // ... other fields
}
```

**Proposed Extension** (adds three new fields):
```protobuf
message SearchRequest {
  string collection_name = 2;

  // Legacy field (deprecated in favor of filter_expr)
  string dsl = 5;

  // NEW: Three-part join grammar
  string filter_expr = 25;   // Filter on primary collection (replaces dsl)
  string join_expr = 26;     // Join clauses: "join <coll> where <cond>"
  string output_expr = 27;   // Output fields from all collections

  repeated string output_fields = 8;   // Legacy (for backward compatibility)
  repeated KeyValuePair search_params = 9;
  bytes placeholder_group = 10;
  // ... other fields
}
```

**Backward Compatibility**:
- If `dsl` is provided and `filter_expr` is empty: Use `dsl` (legacy behavior)
- If `filter_expr` is provided: Use new three-parameter grammar
- If `output_fields` is provided and `output_expr` is empty: Use `output_fields` (legacy)
- If `output_expr` is provided: Use `output_expr` with cross-collection fields

**File**: `internal/proxy/task_search.go`

**Extend searchTask structure**:
```go
type searchTask struct {
    // Existing fields...
    ctx context.Context
    *internalpb.SearchRequest
    request *milvuspb.SearchRequest

    // NEW: Join-related fields
    filterExpr string           // Parsed from request.FilterExpr
    joinSpecs  []JoinSpec       // Parsed from request.JoinExpr
    outputExpr string           // Parsed from request.OutputExpr
}

// Join specification parsed from joinExpr
type JoinSpec struct {
    CollectionName string      // Target collection to join
    LeftField      string      // Join key from primary collection
    RightField     string      // Join key from target collection
    JoinCondition  string      // Full join condition
}
```

**Parse Join Grammar in searchTask.PreExecute**:

Extend the existing `PreExecute` method to parse the three new parameters:

```go
func (t *searchTask) PreExecute(ctx context.Context) error {
    // Existing code: validate collection, partitions, etc.

    // NEW: Parse join grammar
    if t.request.GetFilterExpr() != "" {
        // Use new three-parameter grammar
        t.filterExpr = t.request.GetFilterExpr()

        // Parse join expressions
        if t.request.GetJoinExpr() != "" {
            joinSpecs, err := parseJoinExpr(t.request.GetJoinExpr())
            if err != nil {
                return errors.Wrap(err, "failed to parse join expression")
            }
            t.joinSpecs = joinSpecs
        }

        // Parse output expression
        if t.request.GetOutputExpr() != "" {
            t.outputExpr = t.request.GetOutputExpr()
            // Translate output expression to field IDs across collections
        }
    } else if t.request.GetDsl() != "" {
        // Backward compatibility: use legacy dsl field
        t.filterExpr = t.request.GetDsl()
    }

    // Continue with existing plan generation...
    return t.generateSearchPlan(ctx)
}
```

**Join Expression Parser**:

**File**: `internal/parser/planparserv2/join_parser.go` (new)

```go
// Parse joinExpr string into JoinSpec structures
// Input: "join shop_metadata where items.shop_id = shop_metadata.shop_id\njoin merchant_metadata where items.merchant_id = merchant_metadata.merchant_id"
// Output: []JoinSpec
func parseJoinExpr(joinExpr string) ([]JoinSpec, error) {
    lines := strings.Split(strings.TrimSpace(joinExpr), "\n")
    joins := make([]JoinSpec, 0, len(lines))

    for _, line := range lines {
        line = strings.TrimSpace(line)
        if line == "" {
            continue
        }

        // Expected format: "join <collection> where <left.field> = <right.field>"
        if !strings.HasPrefix(strings.ToLower(line), "join ") {
            return nil, fmt.Errorf("invalid join syntax: %s", line)
        }

        // Parse: "join shop_metadata where items.shop_id = shop_metadata.shop_id"
        parts := strings.SplitN(line[5:], " where ", 2)
        if len(parts) != 2 {
            return nil, fmt.Errorf("join clause missing 'where' condition: %s", line)
        }

        collectionName := strings.TrimSpace(parts[0])
        condition := strings.TrimSpace(parts[1])

        // Parse condition: "items.shop_id = shop_metadata.shop_id"
        eqParts := strings.Split(condition, "=")
        if len(eqParts) != 2 {
            return nil, fmt.Errorf("join condition must be equality: %s", condition)
        }

        left := strings.TrimSpace(eqParts[0])
        right := strings.TrimSpace(eqParts[1])

        // Parse "collection.field" format
        leftParts := strings.Split(left, ".")
        rightParts := strings.Split(right, ".")
        if len(leftParts) != 2 || len(rightParts) != 2 {
            return nil, fmt.Errorf("join fields must use collection.field format: %s", condition)
        }

        joins = append(joins, JoinSpec{
            CollectionName: collectionName,
            LeftField:      leftParts[1],   // e.g., "shop_id" from "items.shop_id"
            RightField:     rightParts[1],  // e.g., "shop_id" from "shop_metadata.shop_id"
            JoinCondition:  condition,
        })
    }

    return joins, nil
}
```

**Output Expression Parser**:

```go
// Parse outputExpr string into qualified field names
// Input: "items.item_id, items.price, shop_metadata.shop_name, shop_metadata.shop_rating"
// Output: map[collection][]field
func parseOutputExpr(outputExpr string) (map[string][]string, error) {
    outputFields := make(map[string][]string)

    fields := strings.Split(outputExpr, ",")
    for _, field := range fields {
        field = strings.TrimSpace(field)

        // Parse "collection.field" format
        parts := strings.Split(field, ".")
        if len(parts) != 2 {
            return nil, fmt.Errorf("output field must use collection.field format: %s", field)
        }

        collection := parts[0]
        fieldName := parts[1]

        outputFields[collection] = append(outputFields[collection], fieldName)
    }

    return outputFields, nil
}
```

**Requery Execution Flow**:

**Note**: We're extending the **existing** requery mechanism (`internal/proxy/search_pipeline.go:requeryOperator`) which already handles retrieving additional fields from the same collection. The implementation below shows how to extend it to support cross-collection data fetching:

```go
func (t *searchTask) PostExecute(ctx context.Context) error {
    // Existing code: reduce results from shards

    // NEW: Execute join requery if joinSpecs present
    if len(t.joinSpecs) > 0 {
        if err := t.executeJoinRequery(ctx); err != nil {
            return err
        }
    }

    // Continue with existing code: format results
}

func (t *searchTask) executeJoinRequery(ctx context.Context) error {
    // For each join specification
    for _, join := range t.joinSpecs {
        // Step 1: Extract join key values from primary results
        joinKeyValues := t.extractJoinKeys(join.LeftField)

        // Step 2: Build query expression: "shop_id IN [1, 2, 3, ...]"
        queryExpr := fmt.Sprintf("%s in %v", join.RightField, joinKeyValues)

        // Step 3: Query the joined collection
        joinedData, err := t.queryJoinedCollection(ctx, join.CollectionName, queryExpr)
        if err != nil {
            return errors.Wrapf(err, "failed to query joined collection %s", join.CollectionName)
        }

        // Step 4: Merge joined data back into primary results by join key
        t.mergeJoinedData(join, joinedData)
    }

    return nil
}
```

**Requery Optimization**:
1. **Batch Queries**: Collect all join keys, query once with `IN` clause
2. **Parallel Joins**: Execute multiple join requeries in parallel
3. **Caching**: Cache frequently accessed metadata (e.g., shop info)
4. **Lazy Loading**: Only execute joins if output fields from joined collection are requested

**Limitations (Keep Joins Simple)**:
- ✅ Equality joins only (`=`), no inequality joins
- ✅ Inner joins only (no LEFT/RIGHT/OUTER joins initially)
- ✅ Single field joins (no composite keys initially)
- ❌ No nested joins (joining result of a join to another collection)
- ❌ No aggregations across joins
- ❌ No cross-collection sorting/ranking

### 5. Index Creation

**File**: `internal/proxy/task_index.go`

Validate index types for scalar collections:
```go
func (t *createIndexTask) PreExecute(ctx context.Context) error {
    // Existing validations...

    vectorFields := typeutil.GetVectorFieldSchemas(t.schema)
    isScalarCollection := len(vectorFields) == 0

    if isScalarCollection {
        // Only allow scalar index types
        indexType := t.req.GetIndexParams()["index_type"]
        if !isScalarIndexType(indexType) {
            return merr.WrapErrParameterInvalidMsg(
                "scalar collections only support scalar indexes (STL_SORT, Trie, Inverted)")
        }
    }

    // Rest of existing code...
}
```

### 6. Query Optimization

**File**: `internal/querynodev2/segments/segment_loader.go`

Scalar collections can skip vector-specific loading:
```go
func (loader *segmentLoader) loadSegment(ctx context.Context, segment *Segment) error {
    // Existing code...

    if !segment.IsScalarCollection() {
        // Load vector data only for vector collections
        if err := loader.loadVectorData(ctx, segment); err != nil {
            return err
        }
    }

    // Load scalar data (always required)
    if err := loader.loadScalarData(ctx, segment); err != nil {
        return err
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

**Phase 1** (v2.X.0): Feature disabled by default
```yaml
proxy:
  allowScalarCollections: false  # Conservative default
```

**Phase 2** (v2.X+1): Feature enabled by default after validation
```yaml
proxy:
  allowScalarCollections: true  # Enable after testing
```

## Test Plan

### Unit Tests

**File**: `internal/proxy/task_test.go`

1. **Test**: Create collection with zero vector fields
   - **Expected**: Success when `allowScalarCollections=true`
   - **Expected**: Error when `allowScalarCollections=false`

2. **Test**: Insert/Upsert/Delete on scalar collection
   - **Expected**: All operations succeed

3. **Test**: Query on scalar collection
   - **Expected**: Returns filtered results correctly

4. **Test**: Search on scalar collection
   - **Expected**: Clear error message about unsupported operation

5. **Test**: Create vector index on scalar collection
   - **Expected**: Error rejecting vector index types

6. **Test**: Create scalar index on scalar collection
   - **Expected**: Success for scalar index types (STL_SORT, Inverted, etc.)

### Integration Tests

**File**: `tests/python_client/testcases/test_scalar_collection.py`

```python
class TestScalarCollection:
    def test_create_scalar_collection(self):
        """Test creating collection without vector fields"""
        schema = CollectionSchema([
            FieldSchema("id", DataType.INT64, is_primary=True),
            FieldSchema("name", DataType.VARCHAR, max_length=100),
            FieldSchema("status", DataType.VARCHAR, max_length=50),
        ])
        collection = Collection("scalar_test", schema=schema)
        assert collection is not None

    def test_insert_query_metadata(self):
        """Test insert and query operations"""
        # Insert data
        collection.insert([[1, 2, 3],
                          ["shop1", "shop2", "shop3"],
                          ["active", "active", "inactive"]])

        # Query with filter
        results = collection.query(
            expr="status == 'active'",
            output_fields=["id", "name"]
        )
        assert len(results) == 2

    def test_search_fails_on_scalar_collection(self):
        """Test that search properly fails"""
        with pytest.raises(Exception) as exc:
            collection.search(...)
        assert "not supported on scalar collections" in str(exc.value)

    def test_application_level_join(self):
        """Test joining vector and scalar collections"""
        # Vector search
        items = item_collection.search(
            data=[embedding],
            anns_field="embedding",
            limit=100,
            output_fields=["shop_id"]
        )

        # Metadata query
        shop_ids = [hit.entity.get("shop_id") for hit in items[0]]
        shops = shop_metadata.query(
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
   - **Expected**: Requery adds minimal overhead, optimized via batching
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
- Multiple round trips to Milvus (significant overhead)
- Difficult to optimize - application can't see full query intent
- Error-prone manual join implementation
- Doesn't leverage Milvus's internal capabilities

**What We're Doing Instead**: Requery-based joins inside Milvus
- Single query API - cleaner developer experience
- Milvus can optimize joins internally (batching, parallelization)
- Lower overhead vs application joins

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

### Core Changes - Scalar Collections
- [ ] Modify `internal/proxy/task.go` validation (allow scalar collections without vector field requirement)
- [ ] Update `internal/distributed/proxy/httpserver/utils.go` HTTP validation
- [ ] Add `allowScalarCollections` configuration parameter
- [ ] Add search operation protection for scalar collections
- [ ] Update collection metadata to track `IsScalarCollection` flag

### Core Changes - Three-Parameter Join Grammar

**Protobuf Changes** (`milvus-proto`):
- [ ] Add `filter_expr` (string) field to `SearchRequest` in `milvus.proto`
- [ ] Add `join_expr` (string) field to `SearchRequest` in `milvus.proto`
- [ ] Add `output_expr` (string) field to `SearchRequest` in `milvus.proto`
- [ ] Maintain backward compatibility with `dsl` and `output_fields`

**Parser Changes** (`internal/parser/planparserv2/`):
- [ ] Create `join_parser.go` with `parseJoinExpr()` function
- [ ] Implement join expression parser: "join <collection> where <condition>"
- [ ] Create `output_parser.go` with `parseOutputExpr()` function
- [ ] Implement output expression parser: "collection.field, collection.field, ..."
- [ ] Add validation for join conditions (equality only, field type matching)
- [ ] Add validation for collection existence

**Search Task Changes** (`internal/proxy/task_search.go`):
- [ ] Add `filterExpr`, `joinSpecs`, `outputExpr` fields to `searchTask` struct
- [ ] Create `JoinSpec` struct (CollectionName, LeftField, RightField, JoinCondition)
- [ ] Extend `PreExecute()` to parse filterExpr/joinExpr/outputExpr
- [ ] Extend `PostExecute()` to execute join requery
- [ ] Implement `executeJoinRequery()` method
- [ ] Implement `extractJoinKeys()` to get join key values from results
- [ ] Implement `queryJoinedCollection()` to fetch data from other collections
- [ ] Implement `mergeJoinedData()` to merge joined fields into results

**Join Execution**:
- [ ] Add join optimizations (batching, parallel execution)
- [ ] Support multiple joins in single query
- [ ] Handle missing join keys gracefully
- [ ] Implement qualified field names in results (collection.field)

### SDK Changes
- [ ] Python SDK: Add `filterExpr`, `joinExpr`, `outputExpr` parameters to `search()`
- [ ] Python SDK: Update examples to show three-parameter join grammar
- [ ] Python SDK: Documentation for join grammar syntax
- [ ] Go SDK: Add FilterExpr, JoinExpr, OutputExpr fields to SearchRequest
- [ ] Go SDK: Documentation and examples
- [ ] Java SDK: Add filterExpr, joinExpr, outputExpr parameters
- [ ] Node.js SDK: Add filterExpr, joinExpr, outputExpr parameters
- [ ] RESTful API: Support new parameters in search endpoint

### Testing
- [ ] Unit tests for scalar collection validation changes
- [ ] Unit tests for join expression parsing
- [ ] Unit tests for join key extraction and merging logic
- [ ] Integration tests for CRUD operations on scalar collections
- [ ] Integration tests for single-collection joins (requery)
- [ ] Integration tests for multi-collection joins (2-3 collections)
- [ ] E2E tests for e-commerce recommendation scenario with joins
- [ ] Performance benchmarks (update efficiency, storage, join overhead)
- [ ] Performance tests for requery join batching and parallelization
- [ ] Backward compatibility tests (existing collections unaffected)

### Documentation
- [ ] User guide: Scalar collections concept (what, when, why)
- [ ] User guide: Three-parameter join grammar (filterExpr, joinExpr, outputExpr)
- [ ] User guide: Join expression syntax - "join <collection> where <condition>"
- [ ] User guide: Output expression syntax - "collection.field, collection.field"
- [ ] User guide: Migration from denormalized to normalized data models
- [ ] API reference: Updated search API with filterExpr/joinExpr/outputExpr parameters
- [ ] API reference: Join grammar specification and examples
- [ ] API reference: Backward compatibility with dsl and output_fields
- [ ] Tutorial: E-commerce multi-entity recommendation with joins
- [ ] Tutorial: Step-by-step join query examples
- [ ] Tutorial: Migrating from denormalized to normalized pattern
- [ ] Best practices: When to use scalar collections vs vector collections
- [ ] Best practices: Join performance optimization (indexing join keys)
- [ ] Best practices: Designing schemas for efficient joins
- [ ] Performance guide: Requery join overhead and optimization strategies

### Release
- [ ] Feature flag for gradual rollout
- [ ] Release notes entry
- [ ] Migration guide (if needed)
- [ ] Community announcement
