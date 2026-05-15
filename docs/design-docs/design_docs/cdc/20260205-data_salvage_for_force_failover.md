# Data Salvage for Force Failover

- **Created:** 2026-02-05
- **Author(s):** @bigsheeper
- **Status:** Draft
- **Component:** Proxy | StreamingCoord | StreamingNode
- **Related Issues:** #47598
- **Related Design:** [Force Promote for Primary-Secondary Failover](./20260202-force_promote_failover.md)

## Summary

This document describes the data salvage mechanism for recovering unreplicated data after a force failover. It introduces a salvage checkpoint that captures the last synced position from the old primary during force promote, and a `DumpMessages` API that allows external tools to retrieve WAL messages for data recovery.

## Motivation

When a force promote occurs, the secondary cluster immediately becomes a standalone primary. However, there may be data on the old primary cluster that was never replicated to the secondary:

```
Timeline:
  Primary:     [msg1] [msg2] [msg3] [msg4] [msg5] [msg6] ---> (becomes unreachable)
                                     ^
                                     └── Last synced to secondary

  Secondary:   [msg1] [msg2] [msg3] [msg4] ---> (force promotes to primary)
                                     ^
                                     └── Salvage checkpoint captures this position

  Lost data:   [msg5] [msg6] (need recovery mechanism)
```

After the old primary becomes available again, operators need a way to:

1. **Identify the data gap** — Know exactly which messages were not replicated
2. **Retrieve lost messages** — Dump WAL messages from the old primary starting from the salvage checkpoint
3. **Replay data** — Re-insert the recovered data into the new primary

## Public Interfaces

### New API: GetReplicateInfo

Extends the existing `GetReplicateInfo` API response with salvage checkpoint:

```protobuf
message GetReplicateInfoResponse {
  common.Status status = 1;
  common.ReplicateCheckpoint checkpoint = 2;
  common.ReplicateCheckpoint salvage_checkpoint = 3;  // NEW: Last synced position before force promote
}
```

### New API: DumpMessages

A streaming RPC that dumps WAL messages from a specified starting position:

```protobuf
// Dumps messages from a WAL channel for data recovery
rpc DumpMessages(DumpMessagesRequest) returns (stream DumpMessagesResponse) {}

message DumpMessagesRequest {
  string pchannel = 1;                    // Physical channel to dump from
  common.MessageID start_message_id = 2;  // Starting position (from salvage checkpoint)
  uint64 start_timetick = 3;              // Optional: filter messages after this timetick
  uint64 end_timetick = 4;                // Optional: stop dumping after this timetick (0 = no limit)
  bool start_position_exclusive = 5;      // If true, skip the message at start_message_id
}

message DumpMessagesResponse {
  oneof response {
    common.Status status = 1;             // Error status (only on failure)
    common.ImmutableMessage message = 2;  // Dumped message (success path)
  }
}
```

### Message Types Dumped

Only replicable data messages are dumped. System messages are filtered out:

| Dumped (Replicable) | Filtered (System) |
|---------------------|-------------------|
| Insert | TimeTick |
| Delete | CreateSegment |
| CreateCollection | Flush (system flush) |
| DropCollection | RollbackTxn |
| CreatePartition | |
| DropPartition | |
| BeginTxn | |
| CommitTxn | |
| Txn | |
| Import | |
| ManualFlush | |

## Design Details

### Salvage Checkpoint Capture

During force promote, the salvage checkpoint is captured from the current replicate checkpoint:

```
Force Promote Flow (with salvage checkpoint):

1. UpdateReplicateConfiguration(config, force_promote=true)
       │
       ▼
2. StreamingCoord validates force promote constraints
       │
       ▼
3. For each pchannel:
   ┌─────────────────────────────────────────────────────────┐
   │  salvageCheckpoints[pchannel] = currentReplicateCheckpoint │
   └─────────────────────────────────────────────────────────┘
       │
       ▼
4. Broadcast AlterReplicateConfigMessage with force_promote=true
       │
       ▼
5. Persist ReplicateConfigurationMeta with:
   - force_promoted = true
   - force_promote_timestamp = current timestamp
   - salvage_checkpoints = map[pchannel]checkpoint
```

### Salvage Checkpoint Storage

Salvage checkpoints are stored in the replicate configuration metadata:

```protobuf
// In streaming.proto
message ReplicateConfigurationMeta {
  common.ReplicateConfiguration replicate_configuration = 1;
  bool force_promoted = 2;
  uint64 force_promote_timestamp = 3;
  map<string, common.ReplicateCheckpoint> salvage_checkpoints = 4;  // NEW
}
```

### DumpMessages Implementation

The `DumpMessages` API uses the WAL scanner to stream messages:

```go
func (node *Proxy) DumpMessages(req *DumpMessagesRequest, stream DumpMessagesServer) error {
    // 1. Validate request
    if req.Pchannel == "" || req.StartMessageId == nil {
        return status.Error(codes.InvalidArgument, "pchannel and start_message_id required")
    }

    // 2. Create WAL scanner from start position
    scanner := streaming.WAL().Read(ctx, ReadOption{
        VChannel:     req.Pchannel,  // Uses pchannel as vchannel for raw access
        DeliverPolicy: StartFrom(req.StartMessageId, req.StartPositionExclusive),
    })
    defer scanner.Close()

    // 3. Stream messages until end condition
    for {
        msg, err := scanner.Next()
        if err == io.EOF {
            return nil
        }

        // Filter system messages
        if !shouldDumpMessage(msg.MessageType()) {
            continue
        }

        // Check end timetick
        if req.EndTimetick > 0 && msg.TimeTick() > req.EndTimetick {
            return nil
        }

        // Filter by start timetick
        if req.StartTimetick > 0 && msg.TimeTick() < req.StartTimetick {
            continue
        }

        // Send message
        stream.Send(&DumpMessagesResponse{
            Response: &DumpMessagesResponse_Message{
                Message: convertToImmutableMessage(msg),
            },
        })
    }
}
```

### Data Recovery Workflow

After force failover, operators can recover data using these steps:

```
Data Recovery Workflow:

1. Get salvage checkpoint from new primary (force-promoted cluster):

   resp = new_primary.GetReplicateInfo(pchannel)
   salvage_checkpoint = resp.salvage_checkpoint

2. Get current checkpoint from old primary (when it becomes available):

   resp = old_primary.GetReplicateInfo(pchannel)
   current_checkpoint = resp.checkpoint

3. Dump messages from old primary starting at salvage checkpoint:

   stream = old_primary.DumpMessages(
       pchannel = pchannel,
       start_message_id = salvage_checkpoint.message_id,
       end_timetick = current_checkpoint.timetick,  // Optional bound
   )

   for msg in stream:
       # Process message (Insert, Delete, DDL, etc.)
       replay_message_to_new_primary(msg)

4. Verify data consistency between clusters
```

### Security Considerations

The `DumpMessages` API exposes raw WAL data, which may contain sensitive information:

1. **Authentication** — Standard Milvus authentication applies
2. **Authorization** — Requires admin/root privileges (to be enforced)
3. **Audit logging** — All DumpMessages calls should be logged for compliance
4. **Network isolation** — Recommend using internal network for data recovery operations

## Files Modified

### Proto Changes
- `proto/milvus.proto` — Add `DumpMessages` RPC, `DumpMessagesRequest`, `DumpMessagesResponse`
- `proto/common.proto` — Add `salvage_checkpoint` to `GetReplicateInfoResponse` (if not already added)

### Core Implementation
- `internal/streamingcoord/server/service/assignment.go` — Capture salvage checkpoints during force promote
- `internal/streamingcoord/server/balancer/channel/manager.go` — Store/retrieve salvage checkpoints
- `internal/proxy/impl.go` — Implement `DumpMessages` handler
- `internal/distributed/proxy/service.go` — Add gRPC service method
- `internal/distributed/streaming/replicate_service.go` — Add `GetSalvageCheckpoint` method

### Tests
- `internal/proxy/dump_messages_test.go` — Unit tests for message filtering
- `tests/integration/replication/data_salvage_test.go` — Integration tests for DumpMessages and salvage checkpoint

## Test Plan

1. **Unit Tests**
   - Test `shouldDumpMessage` correctly filters system messages
   - Test salvage checkpoint capture during force promote

2. **Integration Tests**
   - Test `GetReplicateInfo` returns salvage checkpoint after force promote
   - Test `DumpMessages` streams messages from specified position
   - Test `DumpMessages` respects timetick filters
   - Test `DumpMessages` handles missing pchannel/message_id

3. **E2E Recovery Test**
   - Set up primary-secondary replication
   - Insert data on primary
   - Simulate primary failure and force promote secondary
   - Recover old primary
   - Dump messages from old primary using salvage checkpoint
   - Verify data can be replayed to new primary

## Rejected Alternatives

### 1. Automatic data sync after old primary recovery
Rejected: Automatic sync could cause conflicts if the new primary has diverged. Manual recovery gives operators control over the reconciliation process.

### 2. Store salvage data in object storage during force promote
Rejected: Would require significant additional infrastructure and increase force promote latency. The WAL already contains all needed data.

### 3. Extend CDC to handle salvage recovery
Rejected: CDC is designed for continuous replication, not point-in-time recovery. A dedicated API provides clearer semantics and better debugging.

## References

- [Force Promote Design Document](./20260202-force_promote_failover.md)
- [Milvus Issue #47598](https://github.com/milvus-io/milvus/issues/47598)
- [Implementation PR #47599](https://github.com/milvus-io/milvus/pull/47599)
