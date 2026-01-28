# Upsert Message Implementation Summary

## Overview
Implemented Upsert message support in the Milvus streaming system to optimize Upsert operations by combining Insert and Delete into a single message type, reducing overhead and maintaining semantic consistency with the original Txn-wrapped Insert+Delete approach.

## Implementation Details

### 1. Protocol Buffer Definitions
**File**: `pkg/proto/messages.proto`
- Added `Upsert = 42` to MessageType enum
- Created `UpsertMessageHeader` with:
  - `collection_id`: Collection identifier
  - `partitions`: List of partition segment assignments
  - `delete_rows`: Number of delete rows
- Created `UpsertMessageBody` with:
  - `insert_request`: InsertRequest for insert part
  - `delete_request`: DeleteRequest for delete part

### 2. Message Code Generation
**File**: `pkg/streaming/util/message/codegen/reflect_info.json`
- Registered Upsert message type mapping for automatic code generation

### 3. Proxy Layer (Packing & Splitting)
**Files**:
- `internal/proxy/task_upsert_streaming.go`
- `internal/proxy/task_upsert_repack.go` (NEW)

**Changes**:
- Modified `Execute()` to use `packUpsertMessage()` instead of separate insert/delete packing
- Implemented `repackUpsertDataForStreamingService()`: Hash-based splitting for both Insert and Delete parts
- Implemented `repackUpsertDataWithPartitionKeyForStreamingService()`: Partition key mode support
- **Semantic Consistency**: Groups Insert rows and Delete PKs by the same channel hash, ensuring they stay together in the same Upsert message per channel

### 4. WAL Adaptor (V0 to V1 Conversion)
**File**: `internal/streamingnode/server/wal/adaptor/old_version_message.go`
- Added `MsgType_Upsert` case in `newOldVersionImmutableMessage()`
- Implemented `newV1UpsertMsgFromV0()` to convert V0 UpsertMsg to V1 format
- Builds UpsertMessageBody with both InsertRequest and DeleteRequest
- Builds UpsertMessageHeader with partitions and delete_rows
- **Semantic Consistency**: Preserves all fields from both Insert and Delete parts

### 5. WAL Recovery
**File**: `internal/streamingnode/server/wal/recovery/recovery_storage_impl.go`
- Added `MessageTypeUpsert` case in `handleMessage()`
- Implemented `handleUpsert()` for segment recovery
- Processes insert part for segment observation (similar to handleInsert)
- **Semantic Consistency**: Treats Upsert insert part exactly like standalone Insert for recovery

### 6. Message Adaptor (Header Recovery)
**File**: `pkg/streaming/util/message/adaptor/message.go`
- Added `MessageTypeUpsert` case in `recoverMessageFromHeader()`
- Implemented `recoverUpsertMsgFromHeader()` to recover both insert and delete parts
- Recovers insert part with segment assignment, timestamps, and shard name
- Recovers delete part with timestamps and shard name
- **Semantic Consistency**: Applies same header recovery logic as separate Insert/Delete messages

### 7. TsMsg Interface Implementation
**File**: `pkg/mq/msgstream/msg.go`
- Added complete TsMsg interface implementation for `UpsertMsg`
- Implemented all 17 required methods: TraceCtx, SetTraceCtx, ID, SetID, BeginTs, EndTs, Type, VChannel, CollID, SourceID, HashKeys, Marshal, Unmarshal, Position, SetPosition, SetTs, Size
- Methods delegate to InsertMsg or DeleteMsg as appropriate
- **Semantic Consistency**: UpsertMsg behaves as a composite message with properties from both parts

### 8. WAL Interceptors
**Files**:
- `internal/streamingnode/server/wal/interceptors/shard/shard_interceptor.go`
- `internal/streamingnode/server/wal/interceptors/redo/redo_interceptor.go`

**Shard Interceptor Changes**:
- Added `MessageTypeUpsert` to ops map
- Implemented `handleUpsertMessage()` combining:
  - Segment assignment for insert part (via AssignSegmentByPartitions)
  - Delete metrics tracking (via ApplyDelete)
- **Semantic Consistency**: Applies both insert segment assignment and delete metrics, just like processing them separately

**Redo Interceptor Changes**:
- Modified `waitUntilGrowingSegmentReady()` to handle `MessageTypeUpsert`
- Waits for growing segment readiness for upsert's insert part
- **Semantic Consistency**: Ensures growing segment is ready before processing upsert, same as insert

### 9. QueryNode Pipeline (Filter & Message)
**Files**:
- `internal/querynodev2/pipeline/filter_node.go`
- `internal/querynodev2/pipeline/message.go`

**Filter Node Changes**:
- Added `MsgType_Upsert` case in `filtrate()` method
- Applied insert policies to `upsertMsg.InsertMsg`
- Verified excluded segments for insert part
- Applied delete policies to `upsertMsg.DeleteMsg`
- Updated metrics: `QueryNodeConsumeCounter` with `UpsertLabel`
- **Semantic Consistency**: Applies both insert and delete policies, maintains all filter semantics

**Message Node Changes**:
- Added `MsgType_Upsert` case in `insertNodeMsg.append()` method
- Splits UpsertMsg into separate InsertMsg and DeleteMsg
- Appends insert part to `msg.insertMsgs`
- Appends delete part to `msg.deleteMsgs`
- Updates metrics: `UpsertConsumeThroughput`
- **Semantic Consistency**: Both parts go into the same message batch, processed in sequence with same timestamp

### 10. Delegator (Data Processing)
**File**: `internal/querynodev2/delegator/delegator_data.go`
- Implemented `ProcessUpsert()` method (for potential future atomic processing)
- Combines insert processing (segment creation, bloom filter updates, registration)
- Combines delete processing (buffer insertion, streaming deletion forwarding)
- Records metrics with `UpsertLabel`
- **Note**: Currently not used as UpsertMsg is split into separate messages in the pipeline

### 11. Metrics
**File**: `pkg/util/metricsinfo/quota_metric.go`
- Added `UpsertConsumeThroughput` metric label to track Upsert consumption rate

## Semantic Consistency Verification

### Key Guarantees:
1. **Hash-based Channel Distribution**: Both Insert and Delete parts of an Upsert use the same primary key hash for channel selection, ensuring they go to the same channel and maintain locality

2. **Batch Processing**: When UpsertMsg is split in the pipeline (message.go), both InsertMsg and DeleteMsg are added to the same `insertNodeMsg` batch, ensuring they're processed together

3. **Sequential Processing**: Insert part is processed first (via ProcessInsert), then Delete part (via ProcessDelete), maintaining the logical order

4. **Timestamp Consistency**: Both parts use the same `timeRange.timestampMax` for processing, ensuring MVCC consistency

5. **Policy Application**: All insert policies (InsertNotAligned, InsertEmpty, InsertOutOfTarget) and delete policies (DeleteNotAligned, DeleteEmpty, DeleteOutOfTarget) are applied to their respective parts

6. **Segment Verification**: Excluded segment verification is performed for the insert part, preventing writes to released segments

7. **Metrics Tracking**: Separate metrics for Upsert operations while maintaining visibility into insert and delete components

### Comparison with Txn-wrapped Insert+Delete:
- **Original**: Insert and Delete wrapped in a Txn message, processed together
- **New**: Insert and Delete in a single Upsert message, split at pipeline but processed in same batch
- **Result**: Same semantic guarantees - atomicity within a batch, same timestamp, sequential processing

## Files Modified:
1. `pkg/proto/messages.proto`
2. `pkg/streaming/util/message/codegen/reflect_info.json`
3. `internal/proxy/task_upsert_streaming.go`
4. `internal/proxy/task_upsert_repack.go` (NEW)
5. `internal/streamingnode/server/wal/adaptor/old_version_message.go`
6. `internal/streamingnode/server/wal/recovery/recovery_storage_impl.go`
7. `pkg/streaming/util/message/adaptor/message.go`
8. `pkg/mq/msgstream/msg.go`
9. `internal/streamingnode/server/wal/interceptors/shard/shard_interceptor.go`
10. `internal/streamingnode/server/wal/interceptors/redo/redo_interceptor.go`
11. `internal/querynodev2/pipeline/filter_node.go`
12. `internal/querynodev2/pipeline/message.go`
13. `internal/querynodev2/delegator/delegator_data.go`
14. `pkg/util/metricsinfo/quota_metric.go`

## Testing Recommendations:
1. **Unit Tests**: Test Upsert message packing/unpacking with various partition key modes
2. **Integration Tests**: Verify end-to-end Upsert flow from proxy to querynode
3. **Semantic Tests**: Confirm Upsert behaves identically to Txn-wrapped Insert+Delete
4. **Performance Tests**: Measure latency and throughput improvements
5. **Edge Cases**: Test large Upserts, multi-partition Upserts, concurrent Upserts

## Next Steps:
1. Run `make build-go` to verify compilation
2. Run unit tests: `make test-proxy test-querynode`
3. Run integration tests for streaming
4. Use `/milvus-cluster-manage` skill to test in full cluster mode
