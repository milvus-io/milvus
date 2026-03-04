# Cluster Messages

Messages operating at cluster scope — global barriers and cluster-wide configuration.

All broadcast messages implicitly carry **SharedCluster** via the Broadcaster.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| FlushAll | Broadcast: all PChannels | Yes (all PChannels) | ExclusiveCluster |
| AlterReplicateConfig | Broadcast: all PChannels | Yes (all PChannels) | ExclusiveCluster |
| AlterWAL | Broadcast: all PChannels | Yes (all PChannels) | ExclusiveCluster |
| AlterResourceGroup | Broadcast: CChannel | No | ExclusiveCluster |
| DropResourceGroup | Broadcast: CChannel | No | ExclusiveCluster |

- **FlushAll**: Flushes ALL growing segments across ALL collections on ALL PChannels.
- **AlterReplicateConfig**: Changes replication topology — enabling, disabling, or switching PRIMARY/SECONDARY roles.
- **AlterWAL**: Switches WAL backend implementation across the entire cluster.
- **AlterResourceGroup**: Creates, updates, or transfers nodes between query resource groups.
- **DropResourceGroup**: Removes a resource group after verifying no replicas are loaded in it.

## Key Invariants

- FlushAll, AlterReplicateConfig, AlterWAL act as **global barriers**: no other messages can be in flight on any PChannel.
- AlterResourceGroup, DropResourceGroup are serialized via ExclusiveCluster but are CChannel-only (no PChannel-level exclusive lock).
