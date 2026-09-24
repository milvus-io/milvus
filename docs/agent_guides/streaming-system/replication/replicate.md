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
3. **Secondary Proxy** → **Secondary WAL**: the Proxy remaps VChannel names, waits on the append gate if the message needs it (see below), and appends to the local WAL. The **Replicate Interceptor** validates the incoming message (cluster ID match, TimeTick deduplication) and tracks checkpoint.

## Name Remap

`replicateService.overwriteReplicateMessage` rewrites every channel name a replicated message carries into the local namespace: PChannels correspond **by index position**, and a VChannel name is the source name with its PChannel prefix replaced. The message's own VChannel and its `BroadcastHeader.VChannels` are remapped for every message; `BroadcastHeader.append_first_vchannels` is remapped through that same list, position by position, so the two can never disagree. The remap re-checks every invariant the builder enforces by panicking and fails the message, once, with a `ReplicateViolation`: an append-first name that is not in the broadcast's own list, `AckSyncUp` combined with append-first VChannels, more than one append-first VChannel, the control channel named as append-first. A forced promotion re-drives a replicated task through the broadcaster's own append path, where those invariants are asserted with panics again, so they must be refused here.

Message types whose **body** also names channels need a case of their own:

- **CreateCollection** — the body's virtual/physical channel lists.
- **SplitShard** — the header's `source_vchannel` and `target_vchannels`, and the body's routing post-image and genesis channel lists. The post-image's per-shard `vchannel_name` is remapped too, not just the two name lists: the routing table refuses a shard info whose name disagrees with the VChannel at its position, so a name left in the source namespace would make the whole table unreadable rather than merely stale. The header's `flushed_segment_ids` is cleared: the ids are the primary's sealed segments, a same-task re-fence appends the header as received, and no consumer reads them off the record (the flusher seals every growing segment of the VChannel).
- **AlterCollection with the `shard_split_routing` mask** — the same name lists and shard infos in its updates. Every other AlterCollection is left alone; the routing mask is the only one whose updates carry channel names.

Collection ids, partition ids, the split task id, and the post-image's residues and routing modulus (the header carries neither) are deliberately **not** remapped — they are the same facts in both clusters, and rewriting them would break the correspondence replication exists to keep.

## Append Gate (`append_first_vchannels`)

A broadcast whose header names append-first VChannels is ordered on the primary by the [Broadcaster](../coordination/broadcaster.md): it appends **and persists** that group before it appends any other replica. Replication carries the replicas as independent per-PChannel streams, which restores no order between them, so the secondary reproduces it on the receiving side — the only side that can, since the sender cannot observe another cluster's TimeTicks.

**Rule**: in `replicateService.Append`, after the remap and before the append, a replica whose VChannel is not in `append_first_vchannels` blocks on `StreamingCoordBroadcastService.WaitVChannelsAcked(broadcast_id, vchannels)` until every append-first replica of that broadcast has been acked **in this cluster**. The wait also covers the broadcast task's creation, because a secondary learns of a broadcast only from whichever replica arrives first. A `ReplicateViolation` from that wait (the header names an append-first VChannel the local task does not carry) is logged by the gate as a refusal that will not clear; the stream has no terminal state and still retries from its checkpoint.

Before waiting, the gate short-circuits a replica this cluster has already appended: the PChannel's replicate checkpoint (`GetReplicateCheckpoint`) is from the same source cluster, and the replica's replicate TimeTick is ≤ that checkpoint's. A redelivered replica whose broadcast task was already tombstoned and collected here would otherwise wait for a task that is never recreated. The short-circuit does not cover a second copy of a replica that the primary's broadcaster wrote by retrying an append that had landed (it carries a later replicate TimeTick); for that one the coord's wait opens on the durable record that the append-first replica landed here (see `WaitVChannelsAcked` in the broadcaster guide), which is also why the progress argument below, about replicas still in flight, does not need to cover it.

Today only a shard split uses this: its single source VChannel is the append-first replica, so on the secondary a target's genesis can never be appended before its source's fence, and a delete cannot be resequenced ahead of the insert it removes.

The CChannel replica is **never gated** (`funcutil.IsControlChannel`). A gated replica blocks its whole PChannel stream, and the CChannel's PChannel stream carries the CChannel replica of every collection's DDL, so gating it would stall replicated DDL for **every** collection behind one split's source PChannel, for nothing: the replica has no shard, flusher or recovery effect, and the ack callback still waits for every replica. The consequence is that the routing commit's stamp (`UpdateTimestamp`, the CChannel replica's tick) is local to the CChannel's PChannel on a secondary and may be below the local `T_switch`; no reader compares it with a data PChannel's tick (`T_switch` and the drain gate come from the source replica's result), and `CreateCollection` / `CreatePartition` / `DropPartition` stamp the same kind of tick. A CChannel is never append-first, so the exemption cannot skip a legitimate wait.

**Why it cannot wedge replication.** "A source never waits, so the wait graph is acyclic" is *not* the argument — a gated replica blocks its entire PChannel stream, so a parked target also blocks any append-first replica queued behind it. Progress rests on three facts: (a) every append-first replica's TimeTick is strictly below every other replica of the same broadcast, because the primary persists that group before starting on the rest; (b) TimeTicks are totally ordered across PChannels, coming from one TSO; (c) each replicate stream delivers its PChannel in TimeTick order. The minimum-TimeTick message among all stream heads is therefore either ungated, or gated on replicas that are already appended here or queued behind a smaller head — a contradiction. Fact (a) lives in `pendingBroadcastTask.Execute`, not here: appending the rest concurrently with the append-first group would keep every primary-side test green and wedge a secondary.

The wait is observable, since head-of-line blocking is otherwise indistinguishable from wedged replication: a Warn after 30s naming the broadcast id and the VChannels waited on, and the gauge `milvus_streaming_replicate_gated_appends` (per PChannel) of currently gated appends. `broadcaster.Close()` releases the waiters instead of holding shutdown open.

## Message-Level Replication Skip

Some DDL/control messages cannot be safely replayed on a SECONDARY until their replay contract is deterministic across clusters. Producers mark those concrete WAL messages with the `Unreplicable` (`_ur`) message property. The CDC sender treats them like ignored messages and advances replication progress without sending them. The SECONDARY replicate interceptor also ignores replicated messages that carry `_ur`, which protects mixed-version or already-forwarded traffic.

This is a **message property**, not a static `MessageType` rule. Future support for one of these DDLs should stop setting `_ur` on newly generated messages; old WAL messages that already carry `_ur` remain skipped for rolling-upgrade compatibility.

## Shard Split

A shard split replicates: `SplitShard` and the `AlterCollection` that adopts its targets both travel down the replicate streams, and a secondary ends up with the same shard topology, task id, residues and modulus as the primary. The two mechanisms that make it safe are the name remap and the append gate above.

Everything a cluster needs to reproduce the topology is in the message; every coordinator action happens in the **ack callback**, so both clusters run the same code. On a secondary the `SplitShard` callback finds no split task and creates one outright, recording the **local** `T_switch` (the tick its own fence landed on) and the **local** target genesis checkpoints — the primary's values are never read. **Ordering across a rename.** The two routing commits, `SplitShard` and the adoption, must apply in that order on the secondary too, and the broadcaster does not provide it there: a `REPLICATED` task holds no lock from issue, resource keys are collection names, and a rename between the split and its adoption gives the two different name keys, so a later routing commit's callback can run while an earlier one's is still retrying (see [Broadcaster](../coordination/broadcaster.md)). The routing commit judge (`routing.JudgeCommit`) turns that into a retry. Each commit may move the collection only by its own delta -- the split fences its source and creates its targets; the adoption retires its task's source and adopts its task's targets, as this cluster's DataCoord record of the task names them (`CheckShardSplitDrained` describes the record) -- and a post-image that differs from the meta by more than that in the forward direction (a split not yet applied here, another split's source retired, a task not recorded here) is refused as a retriable `ServiceUnavailable`: never skipped, never applied on the earlier commit's behalf, so the earlier commit's own gates are never bypassed. A `SplitShard` refused this way records no task. No new resource key is involved.

The adoption callback additionally asks the local DataCoord whether that split has drained *here*, and refuses with a retriable `ServiceUnavailable` until it has; the broadcaster retries with backoff while holding the broadcast's resource keys. In practice this wait is seen on a secondary only: the primary's issuer (not on this branch) sends the adoption once its own drain holds. While it lasts, the ack callbacks of every replicated DDL with a conflicting key are deferred: the same collection's (index, import, snapshot, load-config, partition, AlterCollection, Truncate, Drop), the same database's `ExclusiveDBName` DDL (rename, alias, database) — and, because `appendSharedClusterRK` puts `SharedCluster` on every broadcast and the locker keys on domain+name only, the cluster-exclusive ones (`FlushAll`, resource-group DDL, `UpdateReplicateConfiguration`, a forced promotion's own ack callback) as well. See [Collection Messages](../message/message-semantic-collection.md) and `docs/design-docs/design_docs/20260610-shard_split.md` §6.5.

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
