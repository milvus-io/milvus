# Replication & CDC

Milvus supports multi-cluster WAL replication via a star topology: one PRIMARY cluster (origin of all writes) and one or more SECONDARY clusters (replicas receiving WAL messages). Replication operates per-PChannel.

## ReplicateConfig

`ReplicateConfiguration` (protobuf), stored in the [WALCheckpoint](../wal/recovery-storage.md) and updated atomically via `AlterReplicateConfig` broadcast message (see [Cluster Messages](../message/message-semantic-cluster.md)), contains a **Clusters** list (`ClusterID`, `PChannels` ordered list, `ConnectionParam`) and a **CrossClusterTopology** edge list (`SourceClusterID → TargetClusterID`). Only **star topology** is supported: one PRIMARY center node (out-degree=N-1, in-degree=0) and N-1 SECONDARY leaf nodes (in-degree=1, out-degree=0). All clusters must have the same number of PChannels; cross-cluster PChannel mapping is **by index position**: `Source.PChannels[i] → Target.PChannels[i]`.

## Roles

- **PRIMARY**: Accepts client writes (DML/DDL/DCL). The Replicate Interceptor **rejects** any message carrying a replicate header.
- **SECONDARY**: Only accepts replicated messages forwarded from the primary. The Replicate Interceptor **rejects** any message without a replicate header, with two exceptions: WAL self-controlled messages like TimeTick/CreateSegment/Flush, which bypass the interceptor entirely since they are locally generated regardless of role, and messages carrying the `Unreplicable` (`_ur`) property, which are local to the cluster: the secondary WAL appends them and CDC never forwards them. The broadcaster issues them through `StartUnreplicableBroadcastWithResourceKeys`, which skips the primary check (used for resource group DDL, see [Cluster Messages](../message/message-semantic-cluster.md)).

## Data Flow

1. **Primary WAL** → **CDC ChannelReplicator** (per-PChannel, runs on primary StreamingNode): reads messages from the primary WAL starting at the secondary's `ReplicateCheckpoint`. Self-controlled messages (TimeTick, CreateSegment, Flush) and messages carrying the `Unreplicable` (`_ur`) property are skipped.
2. **ChannelReplicator** → **Secondary Proxy** via `CreateReplicateStream` gRPC bidirectional stream: sends each message with its original `MessageID`, `Properties`, and `Payload`, along with the `SourceClusterID`.
3. **Secondary Proxy** → **Secondary WAL**: the Proxy remaps VChannel names and appends to the local WAL. The **Replicate Interceptor** validates the incoming message (cluster ID match, TimeTick deduplication) and tracks checkpoint.

## Message-Level Replication Skip

Some DDL/control messages cannot be safely replayed on a SECONDARY until their replay contract is deterministic across clusters. Producers mark those concrete WAL messages with the `Unreplicable` (`_ur`) message property. The CDC sender treats them like ignored messages and advances replication progress without sending them. The SECONDARY replicate interceptor also ignores replicated messages that carry `_ur`, which protects mixed-version or already-forwarded traffic.

This is a **message property**, not a static `MessageType` rule. Future support for one of these DDLs should stop setting `_ur` on newly generated messages; old WAL messages that already carry `_ur` remain skipped for rolling-upgrade compatibility.

## Checkpoint & Consistency

The secondary maintains a `ReplicateCheckpoint` per PChannel: `{ClusterID, PChannel, MessageID, TimeTick}`.

- **Non-transactional messages**: checkpoint advances immediately after successful append.
- **Transactional messages**: checkpoint advances only on **CommitTxn** — not on BeginTxn or body messages. This ensures that on recovery, uncommitted transactions can be re-replicated without data loss.
- **Deduplication**: messages with `TimeTick ≤ checkpoint.TimeTick` are ignored. Txn body messages for the current in-flight transaction keep the equality case for the txn helper to deduplicate by message ID, since all messages within a transaction share the same TimeTick.

The checkpoint is persisted in the [WALCheckpoint](../wal/recovery-storage.md) and can be queried by the primary via `GetReplicateInfo` to resume replication from the correct position after restart.

### Rejected transaction bodies

An unrecoverable transaction body append error, such as DML for a missing collection, fails the replication stream without confirming the rejected message. The replicate interceptor appends a local `Unreplicable` RollbackTxn before releasing the transaction. Consumers and recovery discard its persisted bodies; the source checkpoint stays unchanged.

If rollback fails, the transaction continues to block unrelated messages. Its local session rejects body/commit writes but allows rollback retries on replay. Transient body append errors keep the transaction and its message-ID deduplication state for retry.

Once rollback is durable, other messages may use the PChannel. Replay must start from BeginTxn: body/commit messages above the checkpoint are rejected when no transaction is active. Senders must preserve source ordering. Bootstrap tooling must restore required metadata before replay and keep the checkpoint below data still awaiting restoration; advancing it makes older messages ineligible for replay.

After sending queued confirmations, the Proxy returns the receive-loop error. Streaming append errors retain their code and cause in gRPC status details, allowing clients to distinguish permanent rejection, retryable errors, and normal EOF.

## Recovery

On WAL open, `RecoverReplicateManager` loads the `ReplicateConfig` and `ReplicateCheckpoint` from the [RecoveryStorage](../wal/recovery-storage.md) snapshot. For SECONDARY clusters, it restores the in-progress transaction identity from the `TxnBuffer` but marks it as requiring replay from `BeginTxn`. This prevents an interrupted transaction from committing after restart; replaying `BeginTxn` resets the buffered bodies before new body/commit messages are accepted.

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
