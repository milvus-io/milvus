# Replication & CDC

Milvus supports multi-cluster WAL replication via a star topology: one PRIMARY cluster (origin of all writes) and one or more SECONDARY clusters (replicas receiving WAL messages). Replication operates per-PChannel.

## ReplicateConfig

`ReplicateConfiguration` (protobuf), stored in the [WALCheckpoint](../wal/recovery-storage.md) and updated atomically via `AlterReplicateConfig` broadcast message (see [Cluster Messages](../message/message-semantic-cluster.md)), contains a **Clusters** list (`ClusterID`, `PChannels` ordered list, `ConnectionParam`) and a **CrossClusterTopology** edge list (`SourceClusterID → TargetClusterID`). Only **star topology** is supported: one PRIMARY center node (out-degree=N-1, in-degree=0) and N-1 SECONDARY leaf nodes (in-degree=1, out-degree=0). All clusters must have the same number of PChannels; cross-cluster PChannel mapping is **by index position**: `Source.PChannels[i] → Target.PChannels[i]`.

## Roles

- **PRIMARY**: Accepts client writes (DML/DDL/DCL). The Replicate Interceptor **rejects** any message carrying a replicate header.
- **SECONDARY**: Only accepts replicated messages forwarded from the primary. The Replicate Interceptor **rejects** any message without a replicate header (except WAL self-controlled messages like TimeTick/CreateSegment/Flush, which bypass the interceptor entirely since they are locally generated regardless of role).

## Data Flow

1. **Primary WAL** → **CDC ChannelReplicator** (per-PChannel, runs on primary StreamingNode): reads messages from the primary WAL starting at the secondary's `ReplicateCheckpoint`. Self-controlled messages (TimeTick, CreateSegment, Flush) are skipped.
2. **ChannelReplicator** → **Secondary Proxy** via `CreateReplicateStream` gRPC bidirectional stream: sends each message with its original `MessageID`, `Properties`, and `Payload`, along with the `SourceClusterID`.
3. **Secondary Proxy** → **Secondary WAL**: the Proxy remaps VChannel names and appends to the local WAL. The **Replicate Interceptor** validates the incoming message (cluster ID match, TimeTick deduplication) and tracks checkpoint.

## Checkpoint & Consistency

The secondary maintains a `ReplicateCheckpoint` per PChannel: `{ClusterID, PChannel, MessageID, TimeTick}`.

- **Non-transactional messages**: checkpoint advances immediately after successful append.
- **Transactional messages**: checkpoint advances only on **CommitTxn** — not on BeginTxn or body messages. This ensures that on recovery, uncommitted transactions can be re-replicated without data loss.
- **Deduplication**: messages with `TimeTick ≤ checkpoint.TimeTick` are ignored (for txn body messages, `< checkpoint.TimeTick` is used since all messages within a transaction share the same TimeTick).

The checkpoint is persisted in the [WALCheckpoint](../wal/recovery-storage.md) and can be queried by the primary via `GetReplicateInfo` to resume replication from the correct position after restart.

## Recovery

On WAL open, `RecoverReplicateManager` loads the `ReplicateConfig` and `ReplicateCheckpoint` from the [RecoveryStorage](../wal/recovery-storage.md) snapshot. For SECONDARY clusters, it also recovers in-progress transaction state from the `TxnBuffer` (uncommitted replicated transactions), so that the secondary can continue receiving body/commit messages for the interrupted transaction.

## Topology Changes

All topology changes are triggered by `AlterReplicateConfig` broadcast messages, which require **ExclusiveCluster** [resource lock](../coordination/broadcaster.md) — acting as a global barrier across all PChannels.

- **AddNewMember**: Add a new cluster and topology edge. Replication starts from the current WAL position of new incoming `AlterReplicateConfig` message. Existing cluster attributes are immutable.
- **AddNewPChannel**: Not supported via config change — all clusters must have equal PChannel count set at initial configuration.
- **SwitchOver**: Update topology edges to reverse roles (e.g., PRIMARY A → SECONDARY B becomes PRIMARY B → SECONDARY A). On the old primary, `SwitchReplicateMode` drops the secondary state. On the new primary, it creates a new secondary state pointing to the new source.
- **FailOver**: Remove the failed primary from topology edges and designate a secondary as the new primary by updating the topology. The CDC ChannelReplicator on the old primary stops when it detects its topology edge is removed.
- **RemoveMember**: Remove topology edges pointing to the target cluster. The CDC ChannelReplicator detects the edge removal via `AlterReplicateConfig` message and cleans up the replicate PChannel metadata from etcd.

## Key Packages

- `pkg/util/replicateutil/` — `ConfigHelper`, `ConfigValidator`, role definitions
- `internal/streamingcoord/server/balancer/` — `ChannelManager` replication config persistence, `AvailableInReplication`, CDC task creation
- `internal/streamingnode/server/wal/interceptors/replicate/` — Replicate interceptor, `ReplicateManager`, secondary state
- `internal/cdc/replication/` — CDC `ChannelReplicator`, `ReplicateStreamClient`
