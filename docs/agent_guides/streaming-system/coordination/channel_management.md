# Channel Management

Singletons running inside StreamingCoord that manage [PChannel/VChannel/CChannel](../channel/channel.md) assignment and metadata.

## Functionality

- **PChannel assignment**: Assigns each PChannel to exactly one StreamingNode via a pluggable balance policy (default: `vchannelfair`). Rebalancing is triggered by node join/leave, VChannel count change, periodic timer, or manual `Trigger()`.
- **Node health monitoring**: Watches StreamingNode status. Unhealthy nodes have their PChannels marked UNAVAILABLE and reassigned.
- **VChannel allocation**: `AllocVirtualChannels()` assigns new VChannels to the least-loaded `AvailableInReplication` PChannels.
- **CChannel management**: Persists which PChannel hosts the singleton CChannel. Assigned once at initialization, never changes.
- **Assignment publishing**: Exposes gRPC `AssignmentDiscover` endpoint for StreamingClient. Versioned by `(Global, Local)` pair.
- **Replication config**: Persists `ReplicateConfiguration` and maintains the per-PChannel `AvailableInReplication` flag. When false, the PChannel is excluded from VChannel allocation and DDL broadcasts. `UpdateReplicateConfiguration()` recomputes this flag for all PChannels and creates CDC tasks for newly added target clusters. `AvailableInReplication` works as below:
  - **No replication config** or **not joined**: All PChannels are available.
  - **Joined replication**: Only PChannels listed in the current cluster's `ReplicateConfiguration.pchannels` are available.

## PChannel State Machine

```
UNINITIALIZED → ASSIGNING → ASSIGNED → UNAVAILABLE → ASSIGNING → ...
```

- **UNINITIALIZED**: Initial state for new PChannels (from config or dynamic `AddPChannels()`).
- **ASSIGNING**: Term incremented, persisted to catalog. Waiting for StreamingNode to confirm WAL open.
- **ASSIGNED**: StreamingNode confirmed. Operational.
- **UNAVAILABLE**: Node failure or Term mismatch. Queued for reassignment.

Assignment is two-phase: `AssignPChannels` (persist + increment Term) → `AssignPChannelsDone` (StreamingNode confirms). If the node fails during ASSIGNING, the PChannel stays in ASSIGNING and is re-assigned on the next rebalance cycle.

## Key Packages

- `internal/streamingcoord/server/balancer/` — `Balancer`, `ChannelManager`, `PChannelMeta`, balance policy
- `internal/streamingcoord/server/service/` — gRPC `AssignmentDiscover` server
- `internal/streamingcoord/client/` — Client-side assignment watcher
