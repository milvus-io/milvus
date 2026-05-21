# Force Promote for Primary-Secondary Failover

**Date:** 2026-02-02

## Overview

Add a `force_promote` flag to the `UpdateReplicateConfiguration` API that allows a secondary cluster to immediately become a standalone primary when the original primary is unavailable. This enables active-passive failover for Milvus cross-cluster replication.

## Motivation

In Milvus cross-cluster replication, a secondary cluster applies configuration changes by waiting for the primary cluster to broadcast the `AlterReplicateConfigMessage` via CDC. If the primary becomes unreachable, the secondary blocks indefinitely because it can never receive the replicated message.

Operators need a mechanism to:
- Promote a secondary cluster to primary during disaster recovery
- Resume service without waiting for the unreachable primary
- Handle incomplete transactions and broadcasts left in an inconsistent state after failover

## Current Replication Flow

**Primary Cluster:**
1. Receives `UpdateReplicateConfiguration` request
2. Broadcasts `AlterReplicateConfigMessage` to all pchannels
3. Returns after broadcast completes
4. Configuration persisted via broadcast callback

**Secondary Cluster:**
1. Receives `UpdateReplicateConfiguration` request
2. Attempts broadcast but fails with `ErrNotPrimary`
3. Waits for CDC to replicate the `AlterReplicateConfigMessage` from primary
4. Returns only when configuration matches

**Problem:** Secondary clusters block indefinitely if the primary is unreachable.

## Design

### API Change

Add `force_promote` field to the existing `UpdateReplicateConfigurationRequest`:

```protobuf
// In milvus.proto
message UpdateReplicateConfigurationRequest {
  common.ReplicateConfiguration replicate_configuration = 1;
  bool force_promote = 2;  // Immediately promote secondary to standalone primary
}
```

Add `force_promote` and `ignore` fields to the internal message header:

```protobuf
// In messages.proto
message AlterReplicateConfigMessageHeader {
  common.ReplicateConfiguration replicate_configuration = 1;
  bool force_promote = 2;
  bool ignore = 3;  // Skip processing of this message (used for incomplete broadcasts)
}
```

Add metadata field to track force-promoted configurations:

```protobuf
// In streaming.proto
message ReplicateConfigurationMeta {
  common.ReplicateConfiguration replicate_configuration = 1;
  bool force_promoted = 2;
}
```

### Force Promote Constraints

For safety, force promote requires an empty configuration and auto-constructs the standalone primary config from existing meta:

| Constraint | Validation | Rationale |
|---|---|---|
| Secondary cluster only | Use `WithSecondaryClusterResourceKey()` API; returns error if primary | Only secondary clusters need emergency promotion |
| Empty clusters field | `len(config.Clusters) == 0` | Config is auto-constructed from existing meta |
| Empty topology field | `len(config.CrossClusterTopology) == 0` | Config is auto-constructed from existing meta |

The auto-constructed configuration:
- Contains a single cluster entry for the current cluster
- Uses existing pchannels from the cluster's meta
- Has no cross-cluster topology (standalone primary)

### Force Promote Flow

```
Client SDK
    │  UpdateReplicateConfiguration(config={}, force_promote=true)
    ▼
Proxy
    │  Forward to StreamingCoord
    ▼
StreamingCoord (Assignment Service)
    │  1. Validate empty cluster/topology fields
    │  2. Use WithSecondaryClusterResourceKey() to acquire lock and verify secondary
    │  3. Auto-construct standalone primary config from existing meta
    │  4. Build message with AckSyncUp=true (disable fast DDL ack)
    │  5. Broadcast AlterReplicateConfigMessage with ForcePromote=true
    ▼
StreamingNode (TxnBuffer)
    │  6. Detect ForcePromote && !Ignore in message
    │  7. Roll back all uncommitted transactions via RollbackAllUncommittedTxn()
    ▼
StreamingNode (Replicate Interceptor)
    │  8. Detect ForcePromote && !Ignore in message header
    │  9. Switch replication mode to primary
    ▼
StreamingCoord (Broadcast Callback)
    │  10. Skip if Ignore=true (incomplete old message)
    │  11. Fix incomplete broadcasts: mark with Ignore=true, supplement to remaining vchannels
    │  12. Persist config with ForcePromoted=true flag
    ▼
Done — cluster is now standalone primary
```

### Handling Incomplete Messages

When force promote executes, incomplete messages from the old topology must be handled:

#### Transaction Rollback (via TxnBuffer)

When TxnBuffer processes the forced `AlterReplicateConfigMessage`:

1. TxnBuffer detects `ForcePromote == true && Ignore == false` in the message header
2. Calls `RollbackAllUncommittedTxn()` to clean up all pending transactions
3. All buffered transaction messages are discarded
4. Rollback happens before the message is passed to downstream consumers

```go
// TxnBuffer method
func (b *TxnBuffer) RollbackAllUncommittedTxn() {
    for txnID := range b.builders {
        b.rollbackTxn(txnID)
    }
    b.logger.Info("Rolled back all uncommitted transactions in TxnBuffer due to force promote")
}
```

No remote detection or coordinator intervention is needed — each vchannel's TxnBuffer handles its own transactions.

#### Incomplete Broadcast Fixing (In Callback on StreamingCoord)

During force promote, incomplete broadcasts from previous operations (e.g., failed switchover) must be handled to prevent their callbacks from overwriting the force promote configuration.

In the `alterReplicateConfiguration()` callback:

1. Skip processing if `Ignore == true` (this is an old incomplete message)
2. For force promote messages, call `FixIncompleteBroadcastsForForcePromote()`
3. Mark incomplete `AlterReplicateConfigMessage` broadcasts with `Ignore=true`
4. Supplement marked messages to their remaining vchannels
5. This ensures old callbacks don't overwrite the new force promote config

```go
// Broadcaster internal method
func (bm *broadcastTaskManager) FixIncompleteBroadcastsForForcePromote(ctx context.Context) error {
    // 1. Find incomplete AlterReplicateConfig broadcasts
    // 2. Update task messages with Ignore=true
    // 3. Persist updated tasks to catalog
    // 4. Supplement to remaining vchannels
}
```

#### The `ignore` Field

The `ignore` field in `AlterReplicateConfigMessageHeader` prevents processing of messages that were broadcast before force promote but completed after:

| Location | Behavior when `Ignore=true` |
|----------|----------------------------|
| TxnBuffer | Skip transaction rollback |
| Replicate Interceptor | Skip replication mode switch |
| DDL ACK Callback | Skip config update and DDL fixing |
| CDC Channel Replicator | Skip replication removal check |
| CDC Stream Client | Skip message handling |
| Replicate Service | Skip message overwrite |
| Recovery Storage | Skip checkpoint and config update |

## Files Modified

### Proto Changes
- `pkg/proto/messages.proto` — Add `force_promote` and `ignore` fields to `AlterReplicateConfigMessageHeader`
- `pkg/proto/streaming.proto` — Add `force_promoted` to `ReplicateConfigurationMeta`

### Core Implementation
- `internal/streamingcoord/server/service/assignment.go` — Add `handleForcePromote()`, ignore field checks in ACK callback
- `internal/streamingcoord/server/balancer/channel/manager.go` — Persist force promote flag in configuration meta
- `internal/streamingcoord/server/broadcaster/broadcast_manager.go` — Add `WithSecondaryClusterResourceKey()`, `FixIncompleteBroadcastsForForcePromote()`
- `internal/streamingcoord/server/broadcaster/broadcaster.go` — Add methods to `Broadcaster` interface
- `internal/streamingnode/server/wal/utility/txn_buffer.go` — Add `RollbackAllUncommittedTxn()`, force promote detection in `HandleImmutableMessages()`
- `internal/streamingnode/server/wal/interceptors/replicate/replicate_interceptor.go` — Add ignore field check
- `internal/streamingnode/server/wal/recovery/recovery_storage_impl.go` — Add ignore field check

### CDC Integration
- `internal/cdc/replication/replicatemanager/channel_replicator.go` — Add ignore field check
- `internal/cdc/replication/replicatestream/replicate_stream_client_impl.go` — Add ignore field check
- `internal/cdc/util/util.go` — Add ignore field check in `IsReplicationRemovedByAlterReplicateConfigMessage()`

### Client & Proxy
- `internal/proxy/impl.go` — Pass through `force_promote` flag
- `client/milvusclient/replicate_builder.go` — Add `WithForcePromote()` builder method
- `internal/distributed/streaming/replicate_service.go` — Accept request object, add ignore field check
- `internal/distributed/streaming/streaming.go` — Update `ReplicateService` interface

### Tests
- `internal/streamingcoord/server/service/assignment_test.go` — Force promote validation, ignore field, and DDL fixing tests
- `internal/streamingnode/server/wal/utility/txn_buffer_test.go` — TxnBuffer rollback and force promote tests
- `tests/integration/replication/force_promote_test.go` — Integration tests

## Edge Cases

1. **Primary cluster rejection** — Force promote rejected via `WithSecondaryClusterResourceKey()` returning `ErrNotSecondary`
2. **Non-empty config rejection** — Force promote requires empty clusters/topology fields; non-empty configs are rejected
3. **Concurrent force promotes** — `WithSecondaryClusterResourceKey()` acquires exclusive cluster-level lock
4. **Idempotency** — `proto.Equal()` check skips duplicate updates
5. **Incomplete switchover messages** — Marked with `ignore=true` before supplementing, preventing config overwrite
6. **Empty pending broadcasts** — DDL fixing is a no-op when no incomplete broadcasts exist
7. **Ignored messages** — All 7 locations check `ignore` field and skip processing

## Alternatives Considered

### 1. Append RollbackTxn messages to WAL for each transaction
Rejected: Requires enumerating all in-flight transactions at the coordinator level and appending individual rollback messages. The `TxnBuffer.RollbackAllUncommittedTxn()` approach is simpler and handles transactions locally in each vchannel's buffer.

### 2. Handle transaction rollback during WAL recovery
Rejected: Force promote is not a WAL recovery event. The `AlterReplicateConfigMessage` propagates naturally through the WAL, and TxnBuffer's message handling is the correct place to trigger rollback.

### 3. Separate API endpoint for force promote
Rejected: Force promote is a specialized mode of `UpdateReplicateConfiguration`. Adding a separate endpoint would duplicate validation logic and complicate the client SDK.

### 4. User-specified config for force promote
Rejected: Allowing user-specified clusters/topology creates opportunities for configuration mismatches. Auto-constructing the config from existing meta ensures consistency and simplifies the API.

### 5. Timestamp-based detection of incomplete messages
Rejected: Using a `force_promote_timestamp` field to detect stale messages is fragile and requires clock synchronization. The `ignore` field approach is explicit and doesn't depend on timing.

## Related Issues

- https://github.com/milvus-io/milvus/issues/47351
- https://github.com/milvus-io/milvus/pull/47352
