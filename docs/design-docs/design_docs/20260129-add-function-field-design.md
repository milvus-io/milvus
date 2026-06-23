# Design Document: Add Function Field Feature

**Commit**: 513c92d7f2 feat: support add function field (#44444)
**Author**: MrPresent-Han
**Date**: September 2025
**Scope**: 156 files, +10,129/-3,475 lines

---

## 1. Overview

### 1.1 Motivation

Function fields enable users to dynamically add computed/derived fields to existing collections without requiring data re-ingestion. The primary use case is adding BM25 (sparse vector) fields to collections that were originally created with only dense vector fields, enabling hybrid search capabilities post-creation.

### 1.2 Key Requirements

1. **Non-disruptive schema evolution**: Add function fields without collection recreation
2. **Backward compatibility**: Existing segments must remain queryable during and after the transition
3. **Consistency guarantees**: All components must have a unified view of schema changes
4. **Performance**: Minimize impact on ongoing read/write operations
5. **Backfill support**: Optionally compute function outputs for existing data

### 1.3 Design Principles

- **Schema versioning**: Every schema change increments a version number for tracking
- **Lazy evaluation**: Function outputs can be computed on-demand rather than requiring physical backfill
- **Write-ahead semantics**: Schema changes are durably logged before in-memory state updates
- **Graceful degradation**: Queries handle missing function field data without crashing

---

## 2. Architecture Overview

### 2.1 High-Level Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           AlterCollectionSchema Request                      │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                   PROXY                                      │
│  • Validate request (field types, names, schema version consistency)         │
│  • Check all segments have aligned schema versions                           │
│  • Create alterCollectionSchemaTask and enqueue to DDL queue                 │
│  • Optionally create indexes for new fields                                  │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                                ROOTCOORD                                     │
│  • Validate function schema (input/output fields, uniqueness)                │
│  • Assign field IDs and function IDs                                         │
│  • Increment schema version                                                  │
│  • Broadcast AlterCollectionMessage to WAL + all virtual channels            │
└─────────────────────────────────────────────────────────────────────────────┘
                                        │
                    ┌───────────────────┼───────────────────┐
                    ▼                   ▼                   ▼
┌──────────────────────────┐ ┌──────────────────────┐ ┌──────────────────────┐
│      STREAMING/WAL       │ │       DATACOORD      │ │      QUERYNODE       │
│ • Flush existing segments│ │ • Track segment      │ │ • SyncSchema to      │
│ • Log schema change      │ │   schema versions    │ │   segments           │
│ • Update in-memory schema│ │ • Trigger backfill   │ │ • Update IDF oracle  │
│ • Validate insert schema │ │   compaction if      │ │ • Rebuild function   │
│   versions               │ │   enabled            │ │   runners            │
└──────────────────────────┘ └──────────────────────┘ └──────────────────────┘
                                        │
                                        ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                             DATANODE (Backfill)                              │
│  • Execute backfill compaction for segments with outdated schema             │
│  • Compute function outputs (e.g., BM25 sparse vectors)                      │
│  • Write new binlogs with function field data                                │
│  • Update BM25 statistics                                                    │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 2.2 Component Responsibilities

| Component | Responsibility |
|-----------|----------------|
| **Proxy** | API gateway, request validation, schema consistency checks, task orchestration |
| **RootCoord** | Schema management, ID assignment, metadata persistence, broadcast coordination |
| **Streaming/WAL** | Durable schema change logging, write consistency, version mismatch detection |
| **DataCoord** | Segment metadata tracking, backfill compaction policy, schema version monitoring |
| **DataNode** | Backfill compaction execution, function output computation, binlog writing |
| **QueryNode** | Schema synchronization, function runner management, IDF oracle updates |
| **Segcore (C++)** | Low-level schema sync, field accessibility checks, data storage |

---

## 3. API Design

### 3.1 New RPC Endpoints

#### RootCoord: AlterCollectionSchema

```protobuf
rpc AlterCollectionSchema(AlterCollectionSchemaRequest) returns (AlterCollectionSchemaResponse) {}
```

**Request Structure**:
- `db_name`: Database name
- `collection_name`: Collection name
- `action`: Schema alteration action (currently only ADD supported)
- `add_request`: Contains field infos and function schemas to add
- `do_physical_backfill`: Whether to backfill existing data

**Constraints**:
- Only one function field can be added per request (current limitation)
- All segments must have consistent schema versions before alteration

#### QueryNode: UpdateIndex

```protobuf
rpc UpdateIndex(UpdateIndexRequest) returns (common.Status) {}

message UpdateIndexRequest {
    common.MsgBase base = 1;
    int64 collectionID = 2;
    oneof Action {
        AddIndex add_index_request = 3;
        DropIndex drop_index_request = 4;
    }
}
```

### 3.2 Proto Message Changes

#### Schema Versioning

Multiple message types now include `schema_version` for tracking:

```protobuf
// data_coord.proto
message AllocSegmentRequest {
    int32 schema_version = 7;
}

message SegmentInfo {
    int32 schema_version = 33;
}

// messages.proto
message InsertMessageHeader {
    int32 schema_version = 3;
}

message CreateSegmentMessageHeader {
    int32 schema_version = 8;
}
```

#### Backfill Compaction

```protobuf
// data_coord.proto
enum CompactionType {
    BackfillCompaction = 12;
}

message CompactionPlan {
    repeated schema.FunctionSchema functions = 30;
}

message CompactionTask {
    repeated schema.FunctionSchema diff_functions = 29;
}
```

#### Index Versioning

```protobuf
// index_coord.proto
message FieldIndex {
    int32 min_schema_version = 4;
}
```

#### Streaming Error Handling

```protobuf
// streaming.proto
enum StreamingCode {
    STREAMING_CODE_SCHEMA_VERSION_MISMATCH = 14;
}
```

---

## 4. Component Design Details

### 4.1 Proxy Layer

**File**: `internal/proxy/impl.go`, `internal/proxy/task.go`

#### AlterCollectionSchema Flow

```
1. Health Check
      │
      ▼
2. DescribeCollection (get current schema)
      │
      ▼
3. Schema Version Consistency Check
   • GetCollectionStatistics
   • Verify SchemaVersionConsistencyProportion == 100%
      │
      ▼
4. Create alterCollectionSchemaTask
      │
      ▼
5. Enqueue to DDL Queue
      │
      ▼
6. PreExecute: Validate fields and function schema
   • Check max field count
   • Check no duplicate names
   • Validate data types
   • Check not system field
      │
      ▼
7. Execute: Call RootCoord.AlterCollectionSchema
      │
      ▼
8. Post-Execute: Create indexes if configured
```

#### Schema Version Consistency Check

Before allowing schema changes, the proxy validates that all segments have aligned schema versions:

```go
stats, _ := s.GetCollectionStatistics(ctx, collectionID)
proportion := stats[common.SchemaVersionConsistencyProportionKey]
if proportion != "100" {
    return errors.New("segments have inconsistent schema versions")
}
```

### 4.2 RootCoord Layer

**File**: `internal/rootcoord/ddl_callbacks_alter_collection_schema.go`

#### Schema Change Processing

```
1. Acquire broadcast lock on collection
      │
      ▼
2. Retrieve current collection metadata
      │
      ▼
3. Validate request:
   • Exactly one function schema
   • Valid field schemas
   • No duplicate field names
   • Function name doesn't exist
      │
      ▼
4. Assign IDs:
   • Field IDs from nextFieldID(coll)
   • Function ID from nextFunctionID(coll)
   • Resolve field names → field IDs for function I/O
      │
      ▼
5. Construct new schema:
   • Copy existing fields and functions
   • Append new fields and function
   • Increment schema version
   • Set DoPhysicalBackfill flag
      │
      ▼
6. Broadcast AlterCollectionMessage:
   • Send to control channel
   • Send to all virtual channels
```

#### ID Assignment

```go
// Field ID assignment
func nextFieldID(coll *model.Collection) int64 {
    maxFieldID := findMaxFieldID(coll.Fields, coll.StructArraySubFields)
    return maxFieldID + 1
}

// Function ID assignment
func nextFunctionID(coll *model.Collection) int64 {
    maxFunctionID := common.StartOfUserFunctionID
    for _, fn := range coll.Functions {
        if fn.ID > maxFunctionID {
            maxFunctionID = fn.ID
        }
    }
    return maxFunctionID + 1
}
```

### 4.3 Streaming/WAL Layer

**Files**: `internal/streamingnode/server/wal/interceptors/shard/`

#### Schema Version Validation

The shard interceptor validates schema versions at write time:

```go
func handleInsertMessage(ctx context.Context, msg InsertMessage) error {
    schemaVersion := msg.Header.GetSchemaVersion()
    correctVersion, err := shardManager.CheckIfCollectionSchemaVersionMatch(
        msg.Header.GetCollectionId(),
        schemaVersion,
    )
    if err != nil {
        return status.NewSchemaVersionMismatch(
            "schema version mismatch, input: %d, collection: %d",
            schemaVersion, correctVersion,
        )
    }
    // Process insert...
}
```

#### Schema Change Ordering (Critical)

Schema changes follow strict ordering to maintain consistency:

```go
func handleAlterCollection(ctx context.Context, msg AlterCollectionMessage) error {
    // 1. FLUSH existing segments FIRST (creates checkpoint)
    if messageutil.IsSchemaChange(header) {
        segmentIDs, _ := flushSegments(ctx, collectionID)
        header.FlushedSegmentIds = segmentIDs
    }

    // 2. APPEND to WAL (durable record)
    msgID, err := appendOp(ctx, msg)
    if err != nil {
        return err
    }

    // 3. UPDATE in-memory state LAST (after WAL success)
    alterCollectionMsg := message.AsImmutableAlterCollectionMessageV2(msg)
    if err := shardManager.AlterCollection(alterCollectionMsg); err != nil {
        panic("failed to alter collection after WAL append")
    }
}
```

**Why `panic()` is Used (Critical Design Decision)**:

The panic at step 3 is intentional and represents an unrecoverable state where:

1. **WAL-Memory Inconsistency**: The schema change has been durably written to WAL but failed to apply to in-memory state. This creates a dangerous inconsistency where:
   - The WAL contains the new schema version
   - In-memory state still has the old schema version
   - Subsequent writes would be validated against the wrong schema

2. **Why Alternatives Don't Work**:
   - **Retry**: Cannot retry because WAL append succeeded—retrying would create duplicate schema change entries
   - **Rollback**: Cannot rollback WAL append (write-ahead log is append-only)
   - **Ignore**: Would allow writes with mismatched schema versions, causing data corruption
   - **Flag for Manual Intervention**: Would leave the node in a zombie state serving stale schema

3. **Recovery Process After Panic**:
   - Node crashes and restarts
   - On restart, node replays WAL from last checkpoint
   - Replayed `AlterCollectionMessage` updates in-memory state correctly
   - Node reaches consistent state (WAL and memory both have new schema)
   - Service resumes with correct schema version

4. **Consistency Guarantees**:
   - Crash-recovery ensures WAL is the source of truth
   - Other nodes will also replay WAL and converge to same schema
   - No data corruption occurs (all flushed segments have old schema)
   - New segments will be created with new schema after recovery

This ordering ensures:
- All old-schema data is flushed before schema change
- Schema change is durably recorded before being visible
- System can recover to consistent state after crash
- **Panic prevents silent schema inconsistencies that would corrupt data**

### 4.4 DataCoord Layer

**Files**: `internal/datacoord/meta.go`, `internal/datacoord/compaction_policy_backfill.go`

#### Segment Schema Version Tracking

Each segment tracks its schema version:

```go
type SegmentInfo struct {
    // ... other fields
    SchemaVersion int32
}
```

The DataCoord calculates schema consistency metrics:

```go
func GetCollectionStatistics(ctx context.Context, req Request) Response {
    collectionSchemaVersion := collection.Schema.GetVersion()
    segments := meta.SelectSegments(ctx, WithCollection(req.CollectionID))

    consistentCount := 0
    for _, segment := range segments {
        if segment.GetSchemaVersion() == collectionSchemaVersion {
            consistentCount++
        }
    }

    proportion := float64(consistentCount) / float64(len(segments)) * 100.0
    return Response{
        Stats: map[string]string{
            SchemaVersionConsistencyProportionKey: fmt.Sprintf("%.2f", proportion),
        },
    }
}
```

#### Backfill Compaction Policy

**Trigger Conditions**:
1. Collection has `DoPhysicalBackfill = true`
2. Segment's `SchemaVersion < Collection.SchemaVersion`
3. Segment is healthy, flushed, not compacting, not importing, visible

**Policy Flow**:

```go
func (p *backfillCompactionPolicy) Trigger(ctx context.Context) ([]CompactionView, error) {
    for _, collection := range collections {
        segments := getEligibleSegments(collection.ID)

        for _, segment := range segments {
            if segment.SchemaVersion < collection.SchemaVersion {
                if collection.DoPhysicalBackfill {
                    // Get schema diff to identify new functions
                    oldSchema := getSchemaByVersion(segment.SchemaVersion)
                    funcDiff := util.SchemaDiff(oldSchema, collection.Schema)

                    // Create backfill compaction view
                    views = append(views, BackfillSegmentsView{
                        segmentID: segment.ID,
                        funcDiff:  funcDiff,
                    })
                } else {
                    // Just update metadata (no physical backfill)
                    segment.SchemaVersion = collection.SchemaVersion
                }
            }
        }
    }
    return views, nil
}
```

### 4.5 DataNode Layer (Backfill Compactor)

**File**: `internal/datanode/compactor/backfill_compactor.go`

#### Backfill Execution Pipeline

```
┌─────────────────────────────────────────────────────────────────┐
│                    Backfill Compaction Pipeline                  │
├─────────────────────────────────────────────────────────────────┤
│                                                                  │
│  1. Pre-Validation                                               │
│     • Exactly one segment in plan                                │
│     • Field binlogs present                                      │
│     • Exactly one backfill function                              │
│     • FunctionRunner validates successfully                      │
│                                                                  │
│  2. Read Input Data                                              │
│     • Read input field binlogs (e.g., varchar for BM25)          │
│     • Decompress and parse via BinlogRecordReader                │
│     • Build input data array                                     │
│                                                                  │
│  3. Execute Function                                             │
│     • Run FunctionRunner.BatchRun() on input data                │
│     • For BM25: Compute sparse float vectors                     │
│     • Build InsertData with function outputs                     │
│                                                                  │
│  4. Write Output                                                 │
│     • Create PackedWriter for new field binlogs                  │
│     • Allocate new log IDs                                       │
│     • Write records to object storage                            │
│                                                                  │
│  5. Update Statistics (BM25)                                     │
│     • Serialize BM25 stats (term frequencies)                    │
│     • Write to dedicated BM25 stats log files                    │
│                                                                  │
│  6. Merge Logs                                                   │
│     • Combine new function field binlogs with original binlogs   │
│     • Create FieldBinlog entries with sizes                      │
│                                                                  │
│  7. Return Result                                                │
│     • CompactionPlanResult with merged logs                      │
│     • Segment ID, row count, BM25 logs                           │
│                                                                  │
└─────────────────────────────────────────────────────────────────┘
```

#### Performance Tracking

```go
type backfillMetrics struct {
    getInputDataDuration time.Duration
    executeBM25Duration  time.Duration
    writeRecordDuration  time.Duration
    updateStatsDuration  time.Duration
}
```

### 4.6 QueryNode Layer

**Files**: `internal/querynodev2/delegator/delegator.go`, `internal/querynodev2/pipeline/embedding_node.go`

#### Schema Update Flow

```go
func (sd *shardDelegator) UpdateSchema(ctx context.Context, schema *schemapb.CollectionSchema) error {
    // Update collection manager
    sd.collection.UpdateSchema(schema)

    // Update BM25 function runners
    sd.updateBM25Functions(schema, ctx)

    // Propagate to all segments
    return sd.propagateSchemaToSegments(ctx, schema)
}
```

#### BM25 Function Detection

```go
func (sd *shardDelegator) updateBM25Functions(schema *schemapb.CollectionSchema, ctx context.Context) {
    // Get current BM25 output field IDs
    currentOutputFields := getCurrentBM25OutputFields(sd.schema)

    // Get new BM25 output field IDs
    newOutputFields := getBM25OutputFields(schema)

    // Find only NEW functions (not in current set)
    for fieldID := range newOutputFields {
        if _, exists := currentOutputFields[fieldID]; !exists {
            // Create function runner for new BM25 function
            runner := createFunctionRunner(schema, fieldID)
            sd.functionRunners[fieldID] = runner
            sd.analyzerRunners[inputFieldID] = runner
            sd.isBM25Field[fieldID] = true
        }
    }

    // Update or create IDF Oracle
    if sd.idfOracle == nil {
        sd.idfOracle = NewIDFOracle(schema.Functions)
    } else {
        sd.idfOracle.UpdateCurrent(schema.Functions)
    }
}
```

#### Embedding Node Dynamic Schema Handling

The embedding node dynamically adapts to schema changes:

```go
type embeddingNode struct {
    curSchema       *schemapb.CollectionSchema
    functionRunners map[int64]function.FunctionRunner  // keyed by function ID
}

func (en *embeddingNode) Operate(msgs []flowgraph.Msg) []flowgraph.Msg {
    for _, msg := range msgs {
        insertMsg := msg.(*insertNodeMsg)

        // Check for schema update
        if insertMsg.schema != nil && insertMsg.schema != en.curSchema {
            en.curSchema = insertMsg.schema
            en.setupFunctionRunners()  // Rebuild runners for new schema
        }

        // Process with current function runners
        if len(en.functionRunners) > 0 {
            en.processWithFunctions(insertMsg)
        }
    }
}
```

### 4.7 Segcore (C++) Layer

**Files**: `internal/core/src/common/Schema.h`, `internal/core/src/segcore/SegmentInterface.h`

#### Schema Synchronization

New `SyncSchema()` operation allows runtime schema updates:

```cpp
class SegmentInternalInterface {
public:
    void SyncSchema(SchemaPtr new_schema) {
        std::unique_lock<std::shared_mutex> lock(sch_mutex_);
        if (new_schema->get_schema_version() > schema_->get_schema_version()) {
            schema_ = new_schema;
        }
    }

protected:
    SchemaPtr schema_;
    mutable std::shared_mutex sch_mutex_;  // Thread-safe schema access
};
```

#### Field Accessibility Checks

New method to check if a field is accessible (either has data or index):

```cpp
bool FieldAccessible(FieldId field_id) const {
    return HasFieldData(field_id) || HasIndex(field_id);
}
```

#### Safe Search Handling

Vector search operations now gracefully handle missing function fields:

```cpp
std::unique_ptr<SearchResult> AsyncSearch(SearchInfo& search_info) {
    FieldId target_field = search_info.GetFieldId();

    // Check if function field is accessible
    if (!segment->FieldAccessible(target_field)) {
        // Return empty result instead of crashing
        return std::make_unique<SearchResult>(
            make_empty_search_result(search_info)
        );
    }

    // Proceed with normal search
    return DoSearch(search_info);
}
```

---

## 5. Schema Diff Utility

**File**: `internal/util/schema_util.go`

### 5.1 Data Structures

```go
type FieldDiff struct {
    Added []*schemapb.FieldSchema  // Fields in new but not in old
}

type FuncDiff struct {
    Added []*schemapb.FunctionSchema  // Functions in new but not in old
}
```

### 5.2 Comparison Logic

```go
func SchemaDiff(oldSchema, newSchema *schemapb.CollectionSchema) (*FieldDiff, *FuncDiff, error) {
    if oldSchema == nil || newSchema == nil {
        return nil, nil, errors.New("schema cannot be nil")
    }

    fieldDiff := compareFields(oldSchema.Fields, newSchema.Fields)
    funcDiff := compareFunctions(oldSchema.Functions, newSchema.Functions)

    return fieldDiff, funcDiff, nil
}

func compareFunctions(oldFuncs, newFuncs []*schemapb.FunctionSchema) *FuncDiff {
    // Build map of old function IDs for O(1) lookup
    oldMap := make(map[int64]bool)
    for _, fn := range oldFuncs {
        if fn != nil {
            oldMap[fn.Id] = true
        }
    }

    // Find functions in new but not in old
    var added []*schemapb.FunctionSchema
    for _, fn := range newFuncs {
        if fn != nil && !oldMap[fn.Id] {
            added = append(added, fn)
        }
    }

    return &FuncDiff{Added: added}
}
```

---

## 6. IDF Oracle Updates

**File**: `internal/querynodev2/delegator/idf_oracle.go`

### 6.1 UpdateCurrent Method

New method to handle function field additions:

```go
func (oracle *IDFOracle) UpdateCurrent(functions []*schemapb.FunctionSchema) {
    oracle.mu.Lock()
    defer oracle.mu.Unlock()

    for _, fn := range functions {
        if fn.Type == schemapb.FunctionType_BM25 {
            outputFieldID := fn.OutputFieldIds[0]

            // Initialize stats for new BM25 fields
            if _, exists := oracle.currentStats[outputFieldID]; !exists {
                oracle.currentStats[outputFieldID] = NewBM25Stats()
            }
        }
    }
}
```

### 6.2 Stats Merging

Enhanced merging for backfilled segments:

```go
func (seg *segmentStats) MergeStats(newStats bm25Stats) bool {
    seg.mu.Lock()
    defer seg.mu.Unlock()

    // Load from disk if needed
    if seg.stats == nil && seg.statsPath != "" {
        seg.stats = loadStatsFromLocalNoLock(seg.statsPath)
    }

    // Merge stats
    for fieldID, newFieldStats := range newStats {
        if oldStats, exists := seg.stats[fieldID]; exists {
            oldStats.Merge(newFieldStats)
        } else {
            seg.stats[fieldID] = newFieldStats.Clone()
        }
    }

    return seg.activated  // Return whether to update current stats
}
```

---

## 7. Index Service Changes

**File**: `internal/datacoord/index_service.go`

### 7.1 MinSchemaVersion Tracking

Indexes now track the minimum schema version required:

```go
func (s *Server) CreateIndex(ctx context.Context, req *indexpb.CreateIndexRequest) error {
    // Get latest schema
    schema, _ := s.broker.DescribeCollectionInternal(ctx, collectionID, typeutil.MaxTimestamp)

    index := &model.Index{
        // ... other fields
        MinSchemaVersion: schema.GetVersion(),  // NEW: Track schema version
    }

    // Broadcast to all channels including control channel
    channels := append([]string{streaming.WAL().ControlChannel()}, vchannels...)

    return s.saveAndBroadcastIndex(ctx, index, channels)
}
```

---

## 8. Error Handling

### 8.1 Schema Version Mismatch

New streaming error code for version conflicts:

```go
const STREAMING_CODE_SCHEMA_VERSION_MISMATCH = 14

func (e *StreamingError) IsSchemaVersionMismatch() bool {
    return e.Code == streamingpb.StreamingCode_STREAMING_CODE_SCHEMA_VERSION_MISMATCH
}

func (e *StreamingError) IsUnrecoverable() bool {
    return e.Code == STREAMING_CODE_UNRECOVERABLE ||
           e.IsReplicateViolation() ||
           e.IsTxnUnavailable() ||
           e.IsSchemaVersionMismatch()  // Schema mismatches are unrecoverable
}
```

### 8.2 Graceful Degradation

Segments without function field data return empty results rather than failing:

```cpp
// In VectorSearchNode
if (!segment->FieldAccessible(target_vector_field_id)) {
    return make_empty_search_result(num_queries, topK);
}
```

---

## 9. Configuration

### 9.1 New Parameters

```go
// component_param.go
type BackfillConfig struct {
    // Whether backfill compaction is enabled
    Enabled bool

    // Maximum concurrent backfill tasks
    MaxConcurrentTasks int

    // Backfill batch size
    BatchSize int
}
```

---

## 10. Sequence Diagrams

### 10.1 Add Function Field Flow

```
Client              Proxy           RootCoord        Streaming         DataCoord        QueryNode
  │                   │                 │                │                 │                │
  │ AlterCollectionSchema               │                │                 │                │
  ├──────────────────►│                 │                │                 │                │
  │                   │ DescribeCollection              │                 │                │
  │                   ├────────────────►│                │                 │                │
  │                   │◄────────────────┤                │                 │                │
  │                   │                 │                │                 │                │
  │                   │ GetCollectionStatistics         │                 │                │
  │                   ├─────────────────────────────────────────────────►│                │
  │                   │◄─────────────────────────────────────────────────┤                │
  │                   │ (check schema version consistency = 100%)         │                │
  │                   │                 │                │                 │                │
  │                   │ AlterCollectionSchema            │                 │                │
  │                   ├────────────────►│                │                 │                │
  │                   │                 │                │                 │                │
  │                   │                 │ Broadcast AlterCollectionMessage │                │
  │                   │                 ├───────────────►│                 │                │
  │                   │                 │                │                 │                │
  │                   │                 │       (handleAlterCollection)   │                │
  │                   │                 │       • Flush existing segments │                │
  │                   │                 │       • Append to WAL           │                │
  │                   │                 │       • Update in-memory state  │                │
  │                   │                 │                │                 │                │
  │                   │                 │                │ Forward to      │                │
  │                   │                 │                ├────────────────►│                │
  │                   │                 │                │  DataCoord      │                │
  │                   │                 │                │                 │                │
  │                   │                 │                │ UpdateSchema    │                │
  │                   │                 │                ├────────────────────────────────►│
  │                   │                 │                │                 │                │
  │                   │◄────────────────┤                │                 │                │
  │◄──────────────────┤                 │                │                 │                │
  │                   │                 │                │                 │                │
```

**Key Points**:
1. RootCoord broadcasts a single `AlterCollectionMessage` to the Streaming node
2. The Streaming node's `handleAlterCollection` function internally performs three steps in strict order:
   - **Step 1**: Flush existing segments (creates checkpoint with old schema)
   - **Step 2**: Append schema change to WAL (durability)
   - **Step 3**: Update in-memory state (visibility)
3. This ordering (described in Section 4.3) ensures crash consistency and prevents mixed-schema segments
4. The message is then forwarded to DataCoord and QueryNode for metadata updates

### 10.2 Backfill Compaction Flow

```
DataCoord                 DataNode                  ObjectStorage
    │                         │                          │
    │ (Backfill policy detects                           │
    │  segment with old schema)                          │
    │                         │                          │
    │ SubmitBackfillCompaction│                          │
    ├────────────────────────►│                          │
    │                         │                          │
    │                         │ Read input field binlogs │
    │                         ├─────────────────────────►│
    │                         │◄─────────────────────────┤
    │                         │                          │
    │                         │ Execute BM25 function    │
    │                         │ (compute sparse vectors) │
    │                         │                          │
    │                         │ Write output binlogs     │
    │                         ├─────────────────────────►│
    │                         │◄─────────────────────────┤
    │                         │                          │
    │                         │ Write BM25 stats         │
    │                         ├─────────────────────────►│
    │                         │◄─────────────────────────┤
    │                         │                          │
    │ CompactionPlanResult    │                          │
    │◄────────────────────────┤                          │
    │                         │                          │
    │ Update segment metadata │                          │
    │ (schemaVersion = new)   │                          │
    │                         │                          │
```

---

## 11. Key Design Decisions

### 11.1 Schema Versioning Strategy

**Decision**: Use monotonically increasing integer version numbers.

**Rationale**:
- Simple comparison (`<`, `>`, `==`)
- No timestamp synchronization issues
- Easy to track in all components
- Supports partial ordering of schema changes

### 11.2 Physical vs Logical Backfill

**Decision**: Support both modes via `DoPhysicalBackfill` flag.

**Physical Backfill** (`DoPhysicalBackfill = true`):
- Computes and stores function outputs
- Higher storage cost
- Better query performance
- Required for complex functions

**Logical Backfill** (`DoPhysicalBackfill = false`):
- Only updates metadata
- Function outputs computed on-demand
- Lower storage cost
- Higher query latency

### 11.3 Single Function Per Request

**Decision**: Limit to one function field addition per request.

**Rationale**:
- Simplifies validation and rollback
- Easier to track progress
- Reduces complexity of partial failures
- Can be relaxed in future versions

### 11.4 Write-Ahead Schema Changes

**Decision**: Flush segments before schema changes, log to WAL before updating in-memory state.

**Rationale**:
- Ensures no mixed-schema segments
- Provides durability guarantees
- Enables crash recovery
- Maintains consistency across components

### 11.5 Graceful Search Degradation

**Decision**: Return empty results for inaccessible function fields instead of failing.

**Rationale**:
- Maintains availability during transitions
- Allows gradual backfill
- Better user experience
- Consistent with eventual consistency model

---

## 12. Testing Strategy

### 12.1 Unit Tests

| Component | Test File | Coverage |
|-----------|-----------|----------|
| Schema Util | `internal/util/schema_util_test.go` | Field diff, function diff, nil handling |
| Backfill Policy | `internal/datacoord/compaction_policy_backfill_test.go` | Trigger conditions, segment selection |
| Backfill Task | `internal/datacoord/compaction_task_backfill_test.go` | State machine, progress tracking |
| Backfill Compactor | `internal/datanode/compactor/backfill_compactor_test.go` | Execution pipeline, error handling |

### 12.2 Integration Tests

- End-to-end function field addition
- Hybrid search with backfilled BM25 fields
- Schema version consistency during concurrent operations
- Recovery after crash during schema change

---

## 13. Future Enhancements

### 13.1 Multi-Function Addition

Support adding multiple function fields in a single request for efficiency.

### 13.2 Function Field Modification

Support modifying function parameters without full re-computation.

### 13.3 Function Field Deletion

Support removing function fields with proper cleanup of binlogs and indexes.

### 13.4 Incremental Backfill

Support pausing and resuming backfill operations for large collections.

### 13.5 Custom Function Types

Extend beyond BM25 to support user-defined function types.

---

## 14. References

- Milvus Architecture: [docs/architecture.md](../architecture.md)
- Segcore Pipeline: [docs/segcore-pipeline.md](../segcore-pipeline.md)
- Reduce Mechanism: [docs/reduce-mechanism.md](../reduce-mechanism.md)
- BM25 Algorithm: [pkg/util/bm25/bm25.go](../../pkg/util/bm25/bm25.go)
