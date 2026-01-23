# Milvus Failover Mechanism Implementation Plan

## Overview

This plan implements a comprehensive failover mechanism for Milvus streaming system, addressing two critical jobs:

**Job 1**: Force update replicate configuration without waiting for CDC replication
- **IMPORTANT CONSTRAINTS**:
  - Must be executed on a **secondary cluster only** (not primary)
  - Configuration must contain **only the current cluster** (single cluster)
  - Configuration must have **no topology** (no replication edges)
  - Use case: Emergency promotion of secondary to become sole primary cluster

**Job 2**: Handle incomplete messages (TxnMessages and BroadcastMessages) after failover
- Roll back uncommitted transaction
- Supplement incomplete broadcast messages to missing vchannels

## Architecture Context

### Current Replication Flow

**Primary Cluster**:
1. Receives UpdateReplicateConfiguration request
2. Broadcasts AlterReplicateConfigMessage to all pchannels
3. Returns after broadcast completes
4. Configuration persisted via broadcast callback

**Non-Primary (Secondary) Cluster**:
1. Receives UpdateReplicateConfiguration request
2. Attempts broadcast but fails with `ErrNotPrimary`
3. **Waits** for CDC to replicate the AlterReplicateConfigMessage from primary
4. Returns only when `proto.Equal(config, currentConfig)` succeeds

**Problem**: Non-primary clusters block indefinitely if primary is unreachable.

### Failover Scenarios

When a StreamingNode fails:
1. StreamingCoord detects node failure (session lost)
2. Channels reassigned to healthy nodes
3. New node opens WAL → triggers recovery
4. **Issue**: Incomplete messages left in inconsistent state

---

## Job 1: Force Update Replicate Configuration

### Objective
When `force_update=true`, apply configuration immediately without waiting for CDC replication.

### Design Approach

**Bypass-Wait-But-Keep-Broadcast Strategy**:
- Still broadcast the message (for eventual consistency)
- Don't wait for CDC replication on non-primary clusters
- Immediately persist configuration with "force" marker
- Prevent CDC from overwriting forced configs using version stamping

### Force Update Constraints

For safety and correctness, force update is restricted to failover scenarios only. The following constraints MUST be enforced:

#### Constraint 1: Secondary Cluster Only
- **Requirement**: Force update can ONLY be executed on secondary (non-primary) clusters
- **Rationale**: Force update is designed for failover when a secondary cluster loses connectivity to primary
- **Validation**: Attempt to acquire cluster-level broadcast lock; if successful (we are primary), reject the request
- **Error**: Return `InvalidArgument` error: "force_update can only be used on secondary clusters"

#### Constraint 2: Single Primary Cluster Configuration Only
The forced configuration must create a global cluster with only one primary cluster (no replication):

**2a. Single Cluster Only**:
- **Requirement**: Configuration must contain exactly ONE cluster
- **Validation**: `len(config.Clusters) == 1`
- **Error**: Return `InvalidArgument` error: "force_update requires configuration with exactly one cluster, got N clusters"

**2b. Current Cluster Only**:
- **Requirement**: The single cluster must be the current cluster (not a different cluster)
- **Validation**: `config.Clusters[0].ClusterId == currentClusterID`
- **Error**: Return `InvalidArgument` error: "force_update requires configuration with only current cluster ID"

**2c. No Topology**:
- **Requirement**: Configuration must have NO topology edges (no replication relationships)
- **Validation**: `config.Topology == nil || len(config.Topology.Edges) == 0`
- **Error**: Return `InvalidArgument` error: "force_update requires configuration with no topology, got N edges"

**Use Case**: These constraints ensure force update is used correctly - to promote a secondary cluster to become the sole primary cluster during emergency failover, not for arbitrary configuration changes.

### Implementation Steps

#### 1. Add Force Update Handler in Assignment Service

**File**: `internal/streamingcoord/server/service/assignment.go`

**Modifications** (lines 66-112):
```go
func UpdateReplicateConfiguration(...) {
    // ... existing validation ...

    // NEW: Force update path
    if forceUpdate {
        return s.handleForceUpdate(ctx, config)
    }

    // Original flow continues for normal updates...
}

// NEW METHOD
func handleForceUpdate(ctx context.Context, config *commonpb.ReplicateConfiguration) error {
    log.Ctx(ctx).Warn("Force update replicate configuration requested")

    // VALIDATION 1: Must be a secondary cluster (not primary)
    balancer, err := balance.GetWithContext(ctx)
    if err != nil {
        return err
    }

    latestAssignment, err := balancer.GetLatestChannelAssignment()
    if err != nil {
        return err
    }

    currentClusterID := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
    currentConfig := latestAssignment.ReplicateConfiguration

    // Check if current cluster is primary by attempting to acquire cluster lock
    // Only non-primary clusters should use force update
    broadcaster, err := broadcast.StartBroadcastWithResourceKeys(ctx, message.NewExclusiveClusterResourceKey())
    if err == nil {
        // Successfully acquired lock = we ARE primary cluster
        broadcaster.Close()
        return status.NewInvalidArgument("force_update can only be used on secondary clusters, current cluster is primary")
    }
    if !errors.Is(err, broadcast.ErrNotPrimary) {
        // Some other error, not the expected ErrNotPrimary
        return err
    }
    // We got ErrNotPrimary, which is what we expect for secondary cluster - continue

    // VALIDATION 2: New config must contain ONLY current cluster and no topology
    if err := validateForceUpdateConfiguration(config, currentClusterID); err != nil {
        return err
    }

    // Double check if configuration is same
    if proto.Equal(config, currentConfig) {
        log.Ctx(ctx).Info("configuration is same in force update, ignored")
        return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
    }

    // Validate the configuration using existing validator
    controlChannel := streaming.WAL().ControlChannel()
    pchannels := lo.MapToSlice(latestAssignment.PChannelView.Channels, func(_ channel.ChannelID, channel *channel.PChannelMeta) string {
        return channel.Name()
    })

    validator := replicateutil.NewReplicateConfigValidator(config, currentConfig, currentClusterID, pchannels)
    if err := validator.Validate(); err != nil {
        log.Ctx(ctx).Warn("Force UpdateReplicateConfiguration validation failed", zap.Error(err))
        return err
    }

    if _, err := replicateutil.NewConfigHelper(currentClusterID, config); err != nil {
        return err
    }

    // Create a synthetic broadcast result for immediate application
    broadcastPChannels := lo.Map(pchannels, func(pchannel string, _ int) string {
        if funcutil.IsOnPhysicalChannel(controlChannel, pchannel) {
            return controlChannel
        }
        return pchannel
    })

    msg := message.NewAlterReplicateConfigMessageBuilderV2().
        WithHeader(&message.AlterReplicateConfigMessageHeader{
            ReplicateConfiguration: config,
            ForceUpdate:           true,  // NEW: Mark as force update
        }).
        WithBody(&message.AlterReplicateConfigMessageBody{}).
        WithBroadcast(broadcastPChannels).
        MustBuildBroadcast()

    // Get WAL accessor and append the forced message directly
    // This bypasses the broadcaster's cluster resource lock (which would fail on secondary)
    wal := streaming.WAL()
    splitMessages := msg.Split()
    appendResultsMap := wal.AppendMessages(ctx, splitMessages...)

    // Check for append errors
    for vchannel, appendResult := range appendResultsMap {
        if appendResult.Err() != nil {
            log.Ctx(ctx).Error("Failed to append forced AlterReplicateConfigMessage",
                zap.String("vchannel", vchannel),
                zap.Error(appendResult.Err()))
            return nil, appendResult.Err()
        }
    }

    // Convert append results to expected format
    appendResults := lo.MapValues(appendResultsMap, func(result message.AppendResult, _ string) *message.AppendResult {
        return &message.AppendResult{
            LastConfirmedMessageID: result.MessageID(),
            TimeTick:              result.TimeTick(),
        }
    })

    result := message.BroadcastResultAlterReplicateConfigMessageV2{
        Message: msg,
        Results: appendResults,
    }

    // Apply the configuration immediately with force flag
    if err := balancer.ForceUpdateReplicateConfiguration(ctx, result); err != nil {
        return err
    }

    log.Ctx(ctx).Info("Force update replicate configuration completed successfully")
    return &streamingpb.UpdateReplicateConfigurationResponse{}, nil
}

// validateForceUpdateConfiguration validates that the forced configuration is safe.
// Requirements:
// 1. Must contain ONLY the current cluster
// 2. Must have NO topology (no replication relationships)
func validateForceUpdateConfiguration(config *commonpb.ReplicateConfiguration, currentClusterID string) error {
    if config == nil {
        return status.NewInvalidArgument("configuration cannot be nil")
    }

    // Check that configuration contains exactly one cluster: the current cluster
    if len(config.Clusters) != 1 {
        return status.NewInvalidArgument(
            "force_update requires configuration with exactly one cluster (current cluster only), got %d clusters",
            len(config.Clusters))
    }

    // Check that the single cluster is the current cluster
    if config.Clusters[0].ClusterId != currentClusterID {
        return status.NewInvalidArgument(
            "force_update requires configuration with only current cluster %s, got cluster %s",
            currentClusterID,
            config.Clusters[0].ClusterId)
    }

    // Check that there is NO topology (no replication)
    if config.Topology != nil && len(config.Topology.Edges) > 0 {
        return status.NewInvalidArgument(
            "force_update requires configuration with no topology (standalone cluster), got %d topology edges",
            len(config.Topology.Edges))
    }

    return nil
}
```

#### 2. Add Force Update Interface and Implementation

**File**: `internal/streamingcoord/server/balancer/balancer.go`
- Add `ForceUpdateReplicateConfiguration` method to interface

**File**: `internal/streamingcoord/server/balancer/balancer_impl.go`
- Implement method that delegates to `channelMetaManager.ForceUpdateReplicateConfiguration()`

#### 3. Implement Core Force Update in ChannelManager

**File**: `internal/streamingcoord/server/balancer/channel/manager.go`

**New Method** (after line 524):
```go
func ForceUpdateReplicateConfiguration(ctx, result) error {
    cm.cond.L.Lock()
    defer cm.cond.L.Unlock()

    // Check idempotency
    if proto.Equal(config, cm.replicateConfig) {
        return nil
    }

    // Create new CDC tasks with nil checkpoints
    newIncomingCDCTasks := cm.getNewIncomingTask(config, appendResults)

    // Save with force flag
    configMeta := &streamingpb.ReplicateConfigurationMeta{
        ReplicateConfiguration: config,
        ForceUpdated:          true,   // NEW FIELD
        ForceUpdateTimestamp:  time.Now().UnixMilli(),  // NEW FIELD
    }

    if err := catalog.SaveReplicateConfiguration(ctx, configMeta, newIncomingCDCTasks); err != nil {
        return err
    }

    // Update in-memory state
    cm.replicateConfig = config
    cm.version.Local++
    cm.cond.UnsafeBroadcast()

    return nil
}
```

**Modify Existing** `UpdateReplicateConfiguration` (lines 496-524):
```go
func UpdateReplicateConfiguration(ctx, result) error {
    cm.cond.L.Lock()
    defer cm.cond.L.Unlock()

    // NEW: Check if current config is force-updated
    currentMeta, _ := catalog.GetReplicateConfiguration(ctx)
    if currentMeta != nil && currentMeta.GetForceUpdated() {
        // Skip CDC updates that conflict with force updates
        log.Warn("Skipping CDC update because config is force-updated")
        return nil
    }

    // ... rest of existing logic ...
}
```

#### 4. Extend Proto Definitions

**File**: `pkg/proto/messages.proto`

Add `force_update` field to `AlterReplicateConfigMessageHeader` (around line 228-231):
```protobuf
message AlterReplicateConfigMessageHeader {
    common.ReplicateConfiguration replicate_configuration = 1;
    bool force_update = 2;  // NEW: indicates this is a forced configuration update
}
```

**File**: `pkg/proto/streaming.proto`

Find `ReplicateConfigurationMeta` message (around line 720-730) and add:
```protobuf
message ReplicateConfigurationMeta {
    common.ReplicateConfiguration replicate_configuration = 1;
    bool force_updated = 2;           // NEW: marks if this was force-updated
    int64 force_update_timestamp = 3; // NEW: timestamp of force update
}
```

**After modifying**: Run `make proto-gen` to regenerate Go code.

### Edge Cases Handled

1. **Primary cluster rejection**: Force update explicitly rejected on primary clusters (only secondary clusters allowed)
2. **Configuration validation**: Enforces that forced config contains ONLY current cluster with NO topology
3. **Multi-cluster config rejection**: Rejects configurations with multiple clusters
4. **Topology present rejection**: Rejects configurations with any replication topology edges
5. **Wrong cluster rejection**: Rejects configurations that don't contain the current cluster
6. **Concurrent force updates**: Catalog atomic saves prevent corruption
7. **CDC overwrite prevention**: `ForceUpdated` flag blocks CDC updates
8. **Invalid configuration**: Rejected before any persistence
9. **Idempotency**: `proto.Equal()` check skips duplicate updates

---

## Job 2: Handling Incomplete Messages

### Overview

When force update completes (secondary cluster promoted to sole primary cluster), incomplete messages from the old topology must be handled:

- **Transaction rollback**: Happens automatically when each StreamingNode processes the forced `AlterReplicateConfigMessage` in the replicate interceptor
- **Broadcast supplementation**: Happens in the broadcast callback `alterReplicateConfiguration()`

Neither happens during WAL recovery.

**How incomplete message handling works:**
- **Transaction rollback**: Each StreamingNode automatically handles this when processing the AlterReplicateConfigMessage (in replicate interceptor)
  - Detects force update by checking `ForceUpdate` field in message header
  - Calls TxnManager to rollback all in-flight transactions
  - No coordinator intervention needed
- **Broadcast supplementation**: Handled in the broadcast callback `alterReplicateConfiguration()` on StreamingCoord
  - Callback is invoked when the config update completes
  - Checks if config was force-updated (reads `ForceUpdated` flag from catalog)
  - Supplements incomplete broadcasts to missing vchannels
  - Ensures DDL operations complete

### Background: Transaction Rollback Mechanism

Milvus has an existing transaction rollback infrastructure:

**Existing RollbackTxn Message (Type 902)** - for explicit rollback:
- Client appends RollbackTxn message to WAL
- TxnBuffer processes it and discards the transaction builder
- Used for explicit transaction abort

**TxnSession.RequestRollback()** - for programmatic rollback:
- Called directly on TxnSession object
- Transitions session to `TxnStateOnRollback`
- Waits for in-flight messages to complete, then cleans up
- Used by TxnManager for timeout or forced cleanup

**For force update, we use the programmatic approach** - calling `TxnSession.RequestRollback()` directly when detecting the force update, rather than appending RollbackTxn messages to WAL. This avoids remote detection complexity.

---

### Part A: TxnMessage Rollback

#### Objective
Roll back all uncommitted transactions when force update promotes secondary to sole primary cluster.

#### Design Decision
**Automatic rollback when AlterReplicateConfigMessage is processed** - both writer side (TxnManager) and reader side (TxnBuffer) detect the forced config change and rollback their transactions locally. No remote detection or RollbackTxn message appending needed.

#### How It Works

When the forced `AlterReplicateConfigMessage` flows through the WAL to each StreamingNode:

**Detection Logic**:
- Check `AlterReplicateConfigMessage.Header().ForceUpdate` field
- If `ForceUpdate == true` → rollback all incomplete transactions

**Writer Side (TxnManager)** handles the rollback. Reader Side (TxnBuffer) relies on existing auto-expiration.

#### Implementation Steps

##### 1. Writer Side - TxnManager Rollback

**File**: `internal/streamingnode/server/wal/interceptors/txn/txn_manager.go`

Add method to rollback all in-flight transactions:

```go
// RollbackAllInFlightTransactions rolls back all active transaction sessions.
// Called when force update transitions to single primary cluster.
func (m *txnManagerImpl) RollbackAllInFlightTransactions() {
    m.mu.Lock()
    defer m.mu.Unlock()

    log.Info("Rolling back all in-flight transactions due to force update",
        zap.Int("sessionCount", len(m.sessions)))

    for txnID, session := range m.sessions {
        log.Info("Requesting rollback for in-flight transaction",
            zap.Int64("txnID", int64(txnID)))
        session.RequestRollback()
    }
}
```

**File**: `internal/streamingnode/server/wal/interceptors/replicate/replicate_interceptor.go`

In the replicate interceptor where `AlterReplicateConfigMessage` is handled:

```go
if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
    alterReplicateConfig := message.MustAsMutableAlterReplicateConfigMessageV2(msg)

    // Check if this is a force update
    isForceUpdate := alterReplicateConfig.Header().ForceUpdate

    if isForceUpdate {
        log.Ctx(ctx).Info("Detected force update, rolling back transactions")

        // Rollback all in-flight transactions in TxnManager
        if impl.txnManager != nil {
            impl.txnManager.RollbackAllInFlightTransactions()
        }
    }

    // Continue with normal replication mode switch
    if err := impl.replicateManager.SwitchReplicateMode(ctx, alterReplicateConfig); err != nil {
        return nil, err
    }
    return appendOp(ctx, msg)
}
```

##### 2. Reader Side - TxnBuffer Rollback (Optional)

**File**: `internal/streamingnode/server/wal/utility/txn_buffer.go`

Add method to clear all uncommitted transactions:

```go
// ClearAllUncommittedTransactions clears all uncommitted transaction builders.
// Called when force update transitions to single primary cluster.
func (b *TxnBuffer) ClearAllUncommittedTransactions() {
    b.logger.Info("Clearing all uncommitted transactions due to force update",
        zap.Int("uncommittedCount", len(b.builders)))

    for txnID, builder := range b.builders {
        b.logger.Info("Rolling back uncommitted transaction",
            zap.Int64("txnID", int64(txnID)),
            zap.Int("bodyMessageCount", builder.BodyMessageCount()))

        b.bytes -= builder.EstimateSize()
        delete(b.builders, txnID)
        b.metrics.ObserveTxn(message.TxnStateRollbacked)
    }
}
```

Add detection in `HandleImmutableMessages`:

```go
func (b *TxnBuffer) HandleImmutableMessages(msgs []message.ImmutableMessage, ts uint64) []message.ImmutableMessage {
    result := make([]message.ImmutableMessage, 0, len(msgs))

    for _, msg := range msgs {
        // NEW: Detect force update in AlterReplicateConfigMessage
        if msg.MessageType() == message.MessageTypeAlterReplicateConfig {
            alterMsg, _ := message.AsImmutableAlterReplicateConfigMessageV2(msg)
            if alterMsg != nil {
                // Check if this is force update by detecting topology change
                // Need to access old config - this requires passing it in or storing it
                // For simplicity, we can detect based on message properties or rely on TxnManager
                // Alternative: Add a flag to the message header indicating force update
            }
        }

        // Not a txn message, can be consumed right now.
        if msg.TxnContext() == nil {
            b.metrics.ObserveAutoCommitTxn()
            result = append(result, msg)
            continue
        }

        // ... rest of existing logic ...
    }

    b.clearExpiredTxn(ts)
    return result
}
```

**Note**: For TxnBuffer, it's cleaner to handle this at a higher level where we have access to both old and new configs. The TxnManager approach (writer side) is more straightforward.

##### 3. Simplified Approach - Writer Side Only

**Recommended**: Only implement rollback in **TxnManager (writer side)**, because:
- TxnManager has access to the interceptor chain where config changes are processed
- Force update is explicitly marked in the message header (`ForceUpdate` field)
- Reader side (TxnBuffer) will naturally clear transactions when they expire or when new messages arrive
- Force update isolates the cluster, so no new transaction messages will come from replication

**Therefore**: Focus on implementing step 1 above (TxnManager rollback). TxnBuffer can rely on existing auto-expiration mechanism.

#### Edge Cases Handled

1. **Automatic rollback**: No remote detection needed, each node handles its own transactions
2. **Natural propagation**: AlterReplicateConfigMessage flows to all StreamingNodes automatically
3. **Writer-side coverage**: TxnManager rollback covers active writing transactions
4. **Reader-side coverage**: Existing auto-expiration handles uncommitted buffered transactions
5. **No extra messages**: Don't need to append RollbackTxn messages to WAL

---

### Part B: BroadcastMessage Supplementation

#### Objective
Complete partially-broadcasted DDL messages by re-sending to missing vchannels.

#### Implementation Steps

##### 1. Update Broadcast Callback

**File**: `internal/streamingcoord/server/service/assignment.go`

**Modify** `alterReplicateConfiguration()` (around line 189):

```go
// alterReplicateConfiguration puts the replicate configuration into the balancer.
// It's a callback function of the broadcast service.
func (s *assignmentServiceImpl) alterReplicateConfiguration(ctx context.Context, result message.BroadcastResultAlterReplicateConfigMessageV2) error {
    balancer, err := balance.GetWithContext(ctx)
    if err != nil {
        return err
    }

    // Update the configuration first
    if err := balancer.UpdateReplicateConfiguration(ctx, result); err != nil {
        return err
    }

    // Check if this is a force update by reading the config meta
    configMeta, err := resource.Resource().StreamingCatalog().GetReplicateConfiguration(ctx)
    isForceUpdate := err == nil && configMeta != nil && configMeta.GetForceUpdated()

    // NEW: If this is a force update, supplement incomplete broadcasts
    // Note: Transaction rollback is handled automatically when each StreamingNode
    // processes the AlterReplicateConfigMessage (in TxnManager interceptor)
    if isForceUpdate {
        log.Ctx(ctx).Info("Force update completed, supplementing incomplete broadcasts")

        if err := s.supplementIncompleteBroadcasts(ctx); err != nil {
            log.Ctx(ctx).Error("Failed to supplement incomplete broadcasts", zap.Error(err))
            // Don't fail the config update, but log the error
        }
    }

    return nil
}
```

##### 2. Implement Broadcast Supplementation

**File**: `internal/streamingcoord/server/service/assignment.go`

```go
// supplementIncompleteBroadcasts supplements any incomplete broadcast messages
// by re-sending to vchannels that haven't received them.
func (s *assignmentServiceImpl) supplementIncompleteBroadcasts(ctx context.Context) error {
    log.Ctx(ctx).Info("Supplementing incomplete broadcasts after force update")

    // Get broadcaster to access pending tasks
    broadcaster := broadcast.GetBroadcaster()
    if broadcaster == nil {
        return errors.New("broadcaster not available")
    }

    // Get all pending broadcast tasks
    pendingTasks := broadcaster.GetPendingTasks()

    supplementCount := 0
    for _, task := range pendingTasks {
        // Check if task has missing vchannels
        pendingMessages := task.PendingBroadcastMessages()
        if len(pendingMessages) == 0 {
            continue
        }

        log.Ctx(ctx).Info("Supplementing incomplete broadcast",
            zap.Uint64("broadcastID", task.Header().BroadcastID),
            zap.String("messageType", task.MessageType().String()),
            zap.Int("pendingVChannels", len(pendingMessages)))

        // Get WAL accessor
        wal := streaming.WAL()

        // Append pending messages to their respective vchannels
        appendResults := wal.AppendMessages(ctx, pendingMessages...)

        // Check for errors
        for vchannel, result := range appendResults {
            if result.Err() != nil {
                log.Ctx(ctx).Warn("Failed to supplement vchannel",
                    zap.String("vchannel", vchannel),
                    zap.Error(result.Err()))
                continue
            }
            supplementCount++
        }
    }

    log.Ctx(ctx).Info("Completed supplementing incomplete broadcasts",
        zap.Int("supplementCount", supplementCount))
    return nil
}
```

##### 2. Add Broadcaster Query Method

**File**: `internal/streamingcoord/server/broadcaster/broadcaster.go`

```go
// GetPendingTasks returns all broadcast tasks that have pending vchannels.
func (b *broadcasterImpl) GetPendingTasks() []*broadcastTask {
    b.mu.RLock()
    defer b.mu.RUnlock()

    var pendingTasks []*broadcastTask
    for _, task := range b.tasks {
        if task.State() == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_PENDING ||
           task.State() == streamingpb.BroadcastTaskState_BROADCAST_TASK_STATE_REPLICATED {
            if len(task.PendingBroadcastMessages()) > 0 {
                pendingTasks = append(pendingTasks, task)
            }
        }
    }
    return pendingTasks
}
```

#### Edge Cases Handled

1. **Partial broadcast detection**: Use existing `PendingBroadcastMessages()` method
2. **Broadcast ID consistency**: Keep original broadcast_id for deduplication
3. **Idempotency**: Duplicate broadcasts are safe due to broadcast_id tracking
4. **Resource locks**: Supplementation helps complete tasks and release locks
5. **Timing**: Happens in callback, after config is updated

---

## Critical Files to Modify

### Job 1: Force Update
1. `internal/streamingcoord/server/service/assignment.go` - Add `handleForceUpdate()` and `validateForceUpdateConfiguration()` methods
2. `internal/streamingcoord/server/balancer/balancer.go` - Add `ForceUpdateReplicateConfiguration` interface method
3. `internal/streamingcoord/server/balancer/balancer_impl.go` - Implement force update delegation
4. `internal/streamingcoord/server/balancer/channel/manager.go` - Core force update logic, CDC conflict prevention
5. `pkg/proto/streaming.proto` - Extend `ReplicateConfigurationMeta` with `force_updated` and `force_update_timestamp` fields

### Job 2: Incomplete Message Handling

**Part A: Transaction Rollback (automatic in StreamingNode)**
1. `pkg/proto/messages.proto` - Add `force_update` field to `AlterReplicateConfigMessageHeader`
2. `internal/streamingnode/server/wal/interceptors/txn/txn_manager.go` - Add `RollbackAllInFlightTransactions()` method
3. `internal/streamingnode/server/wal/interceptors/replicate/replicate_interceptor.go` - Check `ForceUpdate` field and trigger TxnManager rollback

**Part B: Broadcast Supplementation (in callback)**
1. `internal/streamingcoord/server/service/assignment.go` - Modify `alterReplicateConfiguration()` callback to supplement broadcasts
2. `internal/streamingcoord/server/service/assignment.go` - Add `supplementIncompleteBroadcasts()` method
3. `internal/streamingcoord/server/broadcaster/broadcaster.go` - Add `GetPendingTasks()` query method

---

## Verification Plan

### Unit Tests

**Job 1**:
- `internal/streamingcoord/server/service/assignment_test.go`
  - Test force update on secondary cluster succeeds immediately
  - Test force update REJECTED on primary cluster (constraint 1)
  - Test force update REJECTED with multiple clusters in config (constraint 2a)
  - Test force update REJECTED with wrong cluster ID (constraint 2b)
  - Test force update REJECTED with topology present (constraint 2c)
  - Test force update with valid single-primary config succeeds
  - Test force update idempotency
  - Test CDC doesn't overwrite force-updated config

**Job 2A: Transaction Rollback**:
- `internal/streamingnode/server/wal/interceptors/txn/txn_manager_test.go`
  - Test `RollbackAllInFlightTransactions` rolls back all active sessions
  - Test `RollbackAllInFlightTransactions` handles empty session list
- `internal/streamingnode/server/wal/interceptors/replicate/replicate_interceptor_test.go`
  - Test interceptor detects `ForceUpdate=true` and calls TxnManager.RollbackAllInFlightTransactions
  - Test interceptor skips rollback when `ForceUpdate=false`
  - Test interceptor handles AlterReplicateConfigMessage without force_update field (backwards compatibility)

**Job 2B: Broadcast Supplementation**:
- `internal/streamingcoord/server/service/assignment_test.go`
  - Test `alterReplicateConfiguration` detects force update and calls supplementIncompleteBroadcasts
  - Test `supplementIncompleteBroadcasts` called only on force update
  - Test normal (non-force) config update skips broadcast supplementation
- `internal/streamingcoord/server/broadcaster/broadcaster_test.go`
  - Test `GetPendingTasks` returns only tasks with pending vchannels
  - Test `GetPendingTasks` excludes completed tasks

### Integration Tests

**Job 1**:
1. Set up A (primary) → B (secondary) replication topology
2. Verify force update rejected on A (primary cluster)
3. Make A unreachable
4. Force update B's config to single primary (single cluster, no topology)
5. Verify B updates immediately without waiting for CDC
6. Verify B is now sole primary cluster (no replication)
7. Restore A, verify CDC doesn't overwrite B's forced config
8. Verify force update rejected with invalid configs (multi-cluster, with topology)

**Job 2** (Combined - incomplete message handling):
1. Set up A (primary) → B (secondary) topology
2. On cluster B:
   - Start transaction without committing (leaves in-flight txn in TxnManager)
   - Start CreateCollection but inject failure (leaves incomplete broadcast)
3. Make A unreachable
4. Force update B's config to single primary cluster (no replication)
5. **Verify automatic transaction rollback** (happens when AlterReplicateConfigMessage is processed):
   - Check StreamingNode logs for "Detected force update to single primary, rolling back transactions"
   - Check logs for "Rolling back all in-flight transactions"
   - Verify incomplete transaction is not visible in queries
6. **Verify broadcast supplementation** (happens in callback):
   - Check StreamingCoord logs for "Force update completed, supplementing incomplete broadcasts"
   - Check logs for "Supplementing incomplete broadcast task"
   - Verify CreateCollection completes successfully
7. Verify no resource lock leaks

### Manual Testing Checklist

**Force Update (Job 1)**:
- [ ] Force update REJECTED on primary cluster with clear error message
- [ ] Force update REJECTED with multi-cluster configuration
- [ ] Force update REJECTED with wrong cluster ID in config
- [ ] Force update REJECTED when topology edges present
- [ ] Force update SUCCEEDS on secondary cluster with valid single-primary config
- [ ] Force update completes immediately without waiting for CDC
- [ ] Force-updated config persists through restart
- [ ] CDC messages don't overwrite forced config
- [ ] Logs clearly indicate force update path and constraints checked

**Incomplete Messages (Job 2)**:
- [ ] In-flight transactions automatically rolled back when TxnManager processes forced AlterReplicateConfigMessage
- [ ] StreamingNode logs show "Detected force update" and transaction rollback
- [ ] Partial broadcasts supplemented in callback on StreamingCoord
- [ ] StreamingCoord logs show "Supplementing incomplete broadcasts"
- [ ] Metrics and logs clearly show failover handling
- [ ] No resource lock leaks after incomplete broadcasts
- [ ] Performance acceptable (failover is rare event)

---

## Implementation Sequence

### Phase 1: Force Update Infrastructure (Job 1)
**Priority**: High
**Complexity**: Medium
**Risk**: Low (well-isolated change)

**Steps**:
1. Extend proto definition:
   - Add `force_updated` and `force_update_timestamp` fields to `ReplicateConfigurationMeta`
   - Run `make proto-gen` to regenerate Go code
2. Implement force update logic:
   - Add `validateForceUpdateConfiguration()` validation function
   - Implement `handleForceUpdate()` in assignment service
   - Implement `ForceUpdateReplicateConfiguration()` in balancer and channel manager
3. Add CDC conflict prevention:
   - Modify `UpdateReplicateConfiguration()` to check `ForceUpdated` flag
4. Unit tests for all constraint validations
5. Integration testing with primary/secondary topology

### Phase 2: Incomplete Message Handling (Job 2)
**Priority**: High (depends on Phase 1)
**Complexity**: Medium
**Risk**: Medium (integrated with force update)

**Steps**:

**Part A: Automatic Transaction Rollback (StreamingNode)**
1. Extend message proto:
   - Add `force_update` field to `AlterReplicateConfigMessageHeader` in `messages.proto`
   - Run `make proto-gen`
2. Implement TxnManager rollback:
   - Add `RollbackAllInFlightTransactions()` method to TxnManager
3. Integrate into replicate interceptor:
   - Modify replicate interceptor to check `ForceUpdate` field and call TxnManager.RollbackAllInFlightTransactions()
4. Unit tests for transaction rollback
5. Integration tests verifying automatic rollback

**Part B: Broadcast Supplementation (StreamingCoord)**
1. Modify broadcast callback:
   - Update `alterReplicateConfiguration()` to detect force update and call supplementIncompleteBroadcasts()
2. Implement broadcast supplementation:
   - Add `supplementIncompleteBroadcasts()` method in assignment service
   - Add `GetPendingTasks()` to broadcaster interface
3. Add comprehensive logging and metrics
4. Unit tests for callback integration
5. Integration tests with force update + incomplete broadcasts
6. Load testing

**Critical**: Phase 2 must be implemented together with Phase 1 because incomplete message handling is triggered by force update.

---

## Rollback & Monitoring

### Rollback Plan
- Add feature flag to enable/disable force update
- Force update failures fall back to normal flow
- Incomplete message handling can be disabled via config

### Metrics to Add

**Force Update**:
```
milvus_streaming_force_update_total{status="success|failure"}
milvus_streaming_force_update_duration_seconds
```

**TxnMessage Rollback**:
```
milvus_streaming_txn_rollback_after_failover_total{pchannel}
```

**BroadcastMessage Supplementation**:
```
milvus_streaming_broadcast_supplementation_total{message_type, status}
milvus_streaming_broadcast_supplementation_vchannels{message_type}
```

### Logging
- All force updates logged with `forceUpdate=true` field
- Force update detection in replicate interceptor logged with "Detected force update to single primary"
- TxnManager rollbacks logged with session count: "Rolling back all in-flight transactions"
- Individual transaction rollbacks logged with txnID
- Broadcast supplementations logged with broadcast_id and vchannel count
- CDC conflicts logged when skipping updates

---

## Summary

This plan implements a robust failover mechanism for promoting a secondary cluster to become the sole primary cluster:

### Job 1: Force Update with Strict Constraints
**Enables force updates** to bypass CDC wait, with safety constraints:
- **Only on secondary clusters** (primary clusters rejected)
- **Only single-primary configs** (single cluster, no topology)
- **Prevents CDC overwrites** using `force_updated` flag

### Job 2: Incomplete Message Cleanup
**Handles incomplete messages** when force update completes:
- **Transaction rollback**: Automatic when TxnManager processes AlterReplicateConfigMessage (detects force update and rolls back in-flight transactions)
- **Broadcast supplementation**: Re-sends messages to missing vchannels (triggered in callback)
- **Distributed handling**: Each component handles its own cleanup when seeing the forced config change

### Key Design Principles
1. **Leverage existing infrastructure**: TxnManager sessions, BroadcastTask tracking, catalog persistence
2. **Automatic propagation**: AlterReplicateConfigMessage flows naturally to all nodes, triggering local cleanup
3. **No remote detection**: Each StreamingNode handles its own transactions, no coordinator intervention needed
4. **Safety-first**: Multiple constraint validations prevent misuse
5. **Minimal risk**: Well-isolated changes with comprehensive testing
