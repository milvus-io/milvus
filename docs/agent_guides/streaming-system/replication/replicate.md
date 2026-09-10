# Replication & CDC

Milvus supports multi-cluster WAL replication via a star topology: one PRIMARY cluster (origin of all writes) and one or more SECONDARY clusters (replicas receiving WAL messages). Replication operates per-PChannel.

## ReplicateConfig

`ReplicateConfiguration` (protobuf), stored in the [WALCheckpoint](../wal/recovery-storage.md) and updated atomically via `AlterReplicateConfig` broadcast message (see [Cluster Messages](../message/message-semantic-cluster.md)), contains a **Clusters** list (`ClusterID`, `PChannels` ordered list, `ConnectionParam`) and a **CrossClusterTopology** edge list (`SourceClusterID → TargetClusterID`). Only **star topology** is supported: one PRIMARY center node (out-degree=N-1, in-degree=0) and N-1 SECONDARY leaf nodes (in-degree=1, out-degree=0). All clusters must have the same number of PChannels; cross-cluster PChannel mapping is **by index position**: `Source.PChannels[i] → Target.PChannels[i]`.

## Roles

- **PRIMARY**: Accepts client writes (DML/DDL/DCL). The Replicate Interceptor **rejects** any message carrying a replicate header.
- **SECONDARY**: Only accepts replicated messages forwarded from the primary. The Replicate Interceptor **rejects** any message without a replicate header (except WAL self-controlled messages like TimeTick/CreateSegment/Flush, which bypass the interceptor entirely since they are locally generated regardless of role).

## Data Flow

1. **Primary WAL** → **CDC ChannelReplicator** (per-PChannel, runs on primary StreamingNode): reads messages from the primary WAL starting at the secondary's `ReplicateCheckpoint`. Self-controlled messages (TimeTick, CreateSegment, Flush) and messages carrying the `Unreplicable` (`_ur`) property are skipped.
2. **ChannelReplicator** → **Secondary Proxy** via `CreateReplicateStream` gRPC bidirectional stream: sends each message with its original `MessageID`, `Properties`, and `Payload`, along with the `SourceClusterID`.
3. **Secondary Proxy** → **Secondary WAL**: the Proxy remaps VChannel names, waits on the append gate if the message needs it (see below), and appends to the local WAL. The **Replicate Interceptor** validates the incoming message (cluster ID match, TimeTick deduplication) and tracks checkpoint.

## Name Remap

`replicateService.overwriteReplicateMessage` rewrites every channel name a replicated message carries into the local namespace: PChannels correspond **by index position**, and a VChannel name is the source name with its PChannel prefix replaced. The message's own VChannel and its `BroadcastHeader.VChannels` are remapped for every message; `BroadcastHeader.append_first_vchannels` is remapped through that same list, position by position, so the two can never disagree (a name that is not in the broadcast's own list fails the message).

Message types whose **body** also names channels need a case of their own:

- **CreateCollection** — the body's virtual/physical channel lists.
- **SplitShard** — the header's `source_vchannels` and `targets[].vchannel`, and the body's routing post-image and genesis channel lists. The post-image's per-shard `vchannel_name` is remapped too, not just the two name lists: the routing table refuses a shard info whose name disagrees with the VChannel at its position, so a name left in the source namespace would make the whole table unreadable rather than merely stale.
- **AlterCollection with the `shard_split_routing` mask** — the same name lists and shard infos in its updates. Every other AlterCollection is left alone; the routing mask is the only one whose updates carry channel names.

Collection ids, partition ids, the split task id, residues and the routing modulus are deliberately **not** remapped — they are the same facts in both clusters, and rewriting them would break the correspondence replication exists to keep.

## Append Gate (`append_first_vchannels`)

A broadcast whose header names append-first VChannels is ordered on the primary by the [Broadcaster](../coordination/broadcaster.md): it appends **and persists** that group before it appends any other replica. Replication carries the replicas as independent per-PChannel streams, which restores no order between them, so the secondary reproduces it on the receiving side — the only side that can, since the sender cannot observe another cluster's TimeTicks.

**Rule**: in `replicateService.Append`, after the remap and before the append, a replica whose VChannel is not in `append_first_vchannels` blocks on `StreamingCoordBroadcastService.WaitVChannelsAcked(broadcast_id, vchannels)` until every append-first replica of that broadcast has been acked **in this cluster**. The wait also covers the broadcast task's creation, because a secondary learns of a broadcast only from whichever replica arrives first.

Today only a shard split uses this: its source VChannels are the append-first group, so on the secondary a target's genesis can never be appended before its source's fence, and a delete cannot be resequenced ahead of the insert it removes.

**Why it cannot wedge replication.** "A source never waits, so the wait graph is acyclic" is *not* the argument — a gated replica blocks its entire PChannel stream, so a parked target also blocks any append-first replica queued behind it. Progress rests on three facts: (a) every append-first replica's TimeTick is strictly below every other replica of the same broadcast, because the primary persists that group before starting on the rest; (b) TimeTicks are totally ordered across PChannels, coming from one TSO; (c) each replicate stream delivers its PChannel in TimeTick order. The minimum-TimeTick message among all stream heads is therefore either ungated, or gated on replicas that are already appended here or queued behind a smaller head — a contradiction. Fact (a) lives in `pendingBroadcastTask.Execute`, not here: appending the rest concurrently with the append-first group would keep every primary-side test green and wedge a secondary.

The wait is observable, since head-of-line blocking is otherwise indistinguishable from wedged replication: a Warn after 30s naming the broadcast id and the VChannels waited on, and a gauge of currently gated appends. `broadcaster.Close()` releases the waiters instead of holding shutdown open.

## Message-Level Replication Skip

Some DDL/control messages cannot be safely replayed on a SECONDARY until their replay contract is deterministic across clusters. Producers mark those concrete WAL messages with the `Unreplicable` (`_ur`) message property. The CDC sender treats them like ignored messages and advances replication progress without sending them. The SECONDARY replicate interceptor also ignores replicated messages that carry `_ur`, which protects mixed-version or already-forwarded traffic.

This is a **message property**, not a static `MessageType` rule. Future support for one of these DDLs should stop setting `_ur` on newly generated messages; old WAL messages that already carry `_ur` remain skipped for rolling-upgrade compatibility.

## Shard Split

A shard split replicates: `SplitShard` and the `AlterCollection` that adopts its targets both travel down the replicate streams, and a secondary ends up with the same shard topology, task id, residues and modulus as the primary. The two mechanisms that make it safe are the name remap and the append gate above.

Everything a cluster needs to reproduce the topology is in the message; every coordinator action happens in the **ack callback**, so both clusters run the same code. On a secondary the `SplitShard` callback finds no split task and creates one outright, recording the **local** `T_switch` (the tick its own fence landed on) and the **local** target genesis checkpoints — the primary's values are never read. The adoption callback additionally asks the local DataCoord whether that split has drained *here*, and refuses until it has; the broadcaster retries with backoff while holding the broadcast's resource keys, which queues the same collection's later DDL callbacks behind it — and, because `appendSharedClusterRK` puts `SharedCluster` on every broadcast and the locker keys on domain+name only, the cluster-exclusive ones (`FlushAll`, resource-group DDL, `UpdateReplicateConfiguration`, a forced promotion's own ack callback) as well. See [Collection Messages](../message/message-semantic-collection.md) and `docs/design-docs/design_docs/20260610-shard_split.md` §6.5.

Three operator rules follow, none enforced by code. Replicate `SplitShard` and `AlterCollection` together or not at all (skipping one leaves a secondary with targets created but never routed, or the reverse). Do not perform a **graceful switchover while a split is in flight** — nothing reconciles a half-replicated split across a planned role swap; a *forced* promotion is handled **while the adoption has not yet replicated**, in that `fixIncompleteBroadcastsForForcePromote` strips the replicate header from the incomplete task's pending replicas and re-drives them through the normal broadcast path, which reproduces the two-phase order. And, for the same reason the cluster key is held above, do not **force-promote a secondary between an adoption replicating to it and that secondary draining**: the broadcast fix still runs, but the promotion's own ack callback waits for the drain, which is the split manager moving data and can take hours. Not holding the cluster key across that wait is a follow-up (design doc §11).

## Checkpoint & Consistency

The secondary maintains a `ReplicateCheckpoint` per PChannel: `{ClusterID, PChannel, MessageID, TimeTick}`.

- **Non-transactional messages**: checkpoint advances immediately after successful append.
- **Transactional messages**: checkpoint advances only on **CommitTxn** — not on BeginTxn or body messages. This ensures that on recovery, uncommitted transactions can be re-replicated without data loss.
- **Deduplication**: messages with `TimeTick ≤ checkpoint.TimeTick` are ignored. Txn body messages for the current in-flight transaction keep the equality case for the txn helper to deduplicate by message ID, since all messages within a transaction share the same TimeTick.

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
- `internal/distributed/streaming/replicate_service.go` — secondary-side name remap and the append gate
