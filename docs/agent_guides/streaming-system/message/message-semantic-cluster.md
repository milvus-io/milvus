# Cluster Messages

Messages operating at cluster scope — global barriers and cluster-wide configuration.

All broadcast messages implicitly carry **SharedCluster** via the Broadcaster.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| FlushAll | Broadcast: all PChannels, Unreplicable (`_ur`) | Yes (all PChannels) | ExclusiveCluster |
| AlterReplicateConfig | Broadcast: all PChannels | Yes (all PChannels) | ExclusiveCluster |
| AlterWAL | Broadcast: all PChannels | Yes (all PChannels) | ExclusiveCluster |
| AlterResourceGroup | Broadcast: CChannel, Unreplicable (`_ur`) | No | ExclusiveCluster |
| DropResourceGroup | Broadcast: CChannel, Unreplicable (`_ur`) | No | ExclusiveCluster |
| AddFileResource | Broadcast: CChannel | No | ExclusiveCluster |
| RemoveFileResource | Broadcast: CChannel | No | ExclusiveCluster |

- **FlushAll**: Issued through the unreplicable broadcast entrance on either replication role, affecting only the receiving cluster. Aborts incomplete transactions in each PChannel after the local WAL append succeeds (see [Local Flush Control](../wal/local-flush.md)). Flushes ALL growing segments and required L0 Delete output across ALL collections on ALL PChannels. Uses AckSyncUp; the RPC waits for consuming-side completion on every PChannel. Follow-up GetFlushAllState returns true without polling channel checkpoints. Returned ImmutableMessage copies omit `_ur` so Backup can explicitly replicate them for secondary position alignment; the persisted WAL records retain the marker.
- **AlterReplicateConfig**: Changes replication topology — enabling, disabling, or switching PRIMARY/SECONDARY roles.
- **AlterWAL**: Switches WAL backend implementation across the entire cluster.
- **AlterResourceGroup**: Creates, updates, or transfers nodes between query resource groups.
- **DropResourceGroup**: Removes a resource group after verifying no replicas are loaded in it.
- **AddFileResource**: Persists the source-allocated resource ID, name and object path, then notifies the file resource observer. The source API retains its synchronous node-sync attempt; node downloads retry independently of callback completion.
- **RemoveFileResource**: Removes the exact resource ID and name after request-side reference validation, then notifies the observer. Replay is idempotent and cannot remove a newer registration under the same name.

## Key Invariants

- FlushAll, AlterReplicateConfig, AlterWAL act as **global barriers**: no other messages can be in flight on any PChannel.
- AlterResourceGroup, DropResourceGroup are serialized via ExclusiveCluster but are CChannel-only (no PChannel-level exclusive lock). Resource groups describe the nodes of one cluster, so these messages are issued through `StartUnreplicableBroadcastWithResourceKeys` on any replicate role, written to the local WAL, and never replicated.
- AddFileResource and RemoveFileResource use the ordinary primary-only broadcast API and are eligible for CDC. ExclusiveCluster orders their metadata callbacks against schema callbacks. It does not guarantee resource download readiness on other PChannels.
