# Truncate Collection Design Document

## Overview

This document describes the design and implementation of the `TruncateCollection` feature in Milvus.

**Feature**: Clear all data within a collection without dropping the collection itself.

**GitHub Issue**: https://github.com/milvus-io/milvus/issues/26280

## Goals

- Add `TruncateCollection` API to efficiently remove all data from a collection
- Maintain collection schema, indexes, and configuration
- Ensure data consistency across distributed components

## Basic Approach

`TruncateCollection` is treated as a DDL (Data Definition Language) operation with three main steps:

1. **Streaming Node**: Flush all growing segments
2. **Data Coordinator**: Drop all segments with update timestamp earlier than the truncate message
3. **Query Coordinator**: Trigger view update on `currentTarget` to ensure query invisibility

## Constraints & Requirements

- Must not block Root Coordinator or other components
- Must not block read/write operations on other collections or the current collection
- During truncate, DDL operations on the target collection are not allowed

## Architecture

### Component Interaction Flow

```
┌──────┐    ┌───────┐    ┌────────────┐    ┌───────────┐    ┌────────────┐    ┌─────────────┐
│ User │    │ Proxy │    │ Root Coord │    │ Streaming │    │ Data Coord │    │ Query Coord │
└──┬───┘    └───┬───┘    └─────┬──────┘    │   Node    │    └─────┬──────┘    └──────┬──────┘
   │            │              │           └─────┬─────┘          │                  │
   │ truncate   │              │                 │                │                  │
   │ collection │              │                 │                │                  │
   │───────────>│              │                 │                │                  │
   │            │  truncate    │                 │                │                  │
   │            │  collection  │                 │                │                  │
   │            │─────────────>│                 │                │                  │
   │            │              │   append wal    │                │                  │
   │            │              │────────────────>│                │                  │
   │            │              │                 │ flush segments │                  │
   │            │              │                 │───────────────>│                  │
   │            │              │    fast ack     │                │                  │
   │            │              │<────────────────│                │                  │
   │            │              │                 │                │                  │
   │            │              │   drop segment  │                │                  │
   │            │              │─────────────────────────────────>│                  │
   │            │              │                 │                │ filter segment   │
   │            │              │                 │                │ to drop          │
   │            │              │                 │                │                  │
   │            │              │  wait for view to update         │                  │
   │            │              │─────────────────────────────────────────────────────>│
   │            │              │                 │                │                  │
   │            │              │                 │                │  update          │
   │            │              │                 │                │  currentTarget   │
   │            │              │                 │                │                  │
```

## Protocol Buffer Definitions

### New TruncateCollection Message

```protobuf
// TruncateCollectionMessageHeader is the header of truncate collection message.
message TruncateCollectionMessageHeader {
    int64 db_id = 1;
    string db_name = 2;
    int64 collection_id = 3;
    string collection_name = 4;
    repeated int64 segment_ids = 5;
}

// TruncateCollectionMessageBody is the body of truncate collection message.
message TruncateCollectionMessageBody {
}
```

## Component Implementation

### Proxy

New interface:

```go
func (node *Proxy) TruncateCollection(ctx context.Context, request *milvuspb.TruncateCollectionRequest) (*commonpb.Status, error)
```

### Streaming Node

New interfaces:

```go
// Called when writing to WAL, invokes FlushAndFenceSegmentAllocUntil to mark all growing segments
// as flush and return segmentIDs to update the message header
func (impl *shardInterceptor) handleTruncateCollectionMessage(ctx context.Context, msg message.MutableMessage, appendOp interceptors.Append) (message.MessageID, error)

// Called when WAL flusher consumes messages, executes sealSegments and flushChannel to flush growing segments
func (impl *msgHandlerImpl) HandleTruncateCollection(flushMsg message.ImmutableTruncateCollectionMessageV2) error
```

### Root Coordinator

New interfaces:

```go
// Entry point
func (c *Core) TruncateCollection(ctx context.Context, in *milvuspb.TruncateCollectionRequest) (*commonpb.Status, error)

// Construct message and write to WAL
func (c *Core) broadcastTruncateCollection(ctx context.Context, req *milvuspb.TruncateCollectionRequest) error

// Callback after WAL write, notify DC to drop segments, wait for QC to update query view
func (c *DDLCallback) truncateCollectionV2AckCallback(ctx context.Context, result message.BroadcastResultTruncateCollectionMessageV2) error
```

### Data Coordinator

New interface:

```go
// Wait for flush to complete, filter segments with update time less than truncate request timestamp, then drop them
func (s *Server) DropSegmentsByTime(ctx context.Context, req *datapb.DropSegmentsByTimeRequest) (*commonpb.Status, error)
```

### Query Coordinator

New interface:

```go
// Call WaitCurrentTargetUpdated to trigger view update
func (s *Server) ManualUpdateCurrentTarget(ctx context.Context, req *querypb.ManualUpdateCurrentTargetRequest) (*commonpb.Status, error)
```

## Design Details

### DDL Request Isolation

Under the existing DDL framework, DDL requests acquire resource locks before writing to WAL. Locks are categorized by level (database, collection) and type (shared, exclusive). Different DDL operations are isolated at this step:

```go
func (*Core) startBroadcastWithCollectionLock(ctx context.Context, dbName string, collectionName string) (broadcaster.BroadcastAPI, error) {
    broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx,
        message.NewSharedDBNameResourceKey(dbName),
        message.NewExclusiveCollectionNameResourceKey(dbName, collectionName),
    )
    if err != nil {
        return nil, errors.Wrap(err, "failed to start broadcast with collection lock")
    }
    return broadcaster, nil
}
```

### Segment Metadata Update Steps

1. **Streaming Node**: Update in-memory segment state, set flush timestamp

```go
type SegmentInfo struct {
    segmentID        int64
    partitionID      int64
    state            commonpb.SegmentState
    startPosition    *msgpb.MsgPosition
    checkpoint       *msgpb.MsgPosition
    startPosRecorded bool
    flushedRows      int64
    bufferRows       int64
    syncingRows      int64
    bfs              pkoracle.PkStat
    bm25stats        *SegmentBM25Stats
    level            datapb.SegmentLevel
    syncingTasks     int32
    storageVersion   int64
    binlogs          []*datapb.FieldBinlog
    statslogs        []*datapb.FieldBinlog
    deltalogs        []*datapb.FieldBinlog
    bm25logs         []*datapb.FieldBinlog
    currentSplit     []storagecommon.ColumnGroup
    manifestPath     string
}
```

2. **Streaming Node**: WriteNode performs flush, notifies DC to update state after completion

```go
type SegmentInfo struct {
    *datapb.SegmentInfo
    allocations     []*Allocation
    lastFlushTime   time.Time
    isCompacting    bool
    // a cache to avoid calculate twice
    size            atomic.Int64
    deltaRowcount   atomic.Int64
    earliestTs      atomic.Uint64
    lastWrittenTime time.Time
}
```

3. **Data Coordinator**: Drop relevant segments

4. **Query Coordinator**: Trigger view update, pull latest segment information from DC

### Determining Which Segments to Drop

- When the truncate message is written to WAL, it is assigned a timestamp
- When data is inserted into a segment, the `DmlPosition` is updated, which contains a timestamp representing the latest data position consumed by that segment

```go
type MsgPosition struct {
    state         protoimpl.MessageState
    sizeCache     protoimpl.SizeCache
    unknownFields protoimpl.UnknownFields

    ChannelName string
    MsgID       []byte
    MsgGroup    string
    Timestamp   uint64
}
```

- During compaction, the new segment's `DmlPosition` takes the minimum value from the old segments

**Drop Condition**: Segment is non-empty AND `DmlPosition.Timestamp < msg.Timestamp`

### Preventing Compaction Impact on Old and New Data

A new property `CollectionOnTruncatingKey` is added to collection properties to indicate whether the collection is being truncated:

- Set to `1` after the truncate message is written to WAL
- Set back to `0` after truncate completes

In the compaction validation flow, add validation for `CollectionOnTruncatingKey`. If truncation is in progress, reject the compaction request.

## Summary

The `TruncateCollection` operation provides an efficient way to clear all data from a collection while preserving its structure. By leveraging the existing DDL framework and coordinating across multiple components (Root Coordinator, Streaming Node, Data Coordinator, Query Coordinator), the implementation ensures:

- **Consistency**: All segments are properly flushed before being dropped
- **Isolation**: Other operations are not blocked during truncation
- **Visibility**: Query results are updated to reflect the truncated state
- **Safety**: Compaction is blocked during truncation to prevent data inconsistencies
