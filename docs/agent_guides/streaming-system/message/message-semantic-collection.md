# Collection Messages

Messages operating on collections, partitions, segments, indexes, snapshots, imports, and DML.

All broadcast messages implicitly carry **SharedCluster** via the Broadcaster.

| Message | Dispatch | ExclusiveRequired | ResourceKey |
|---------|----------|-------------------|-------------|
| CreateCollection | Broadcast: VChannels + CChannel | Yes (VChannel) | SharedDBName + ExclusiveCollectionName |
| DropCollection | Broadcast: VChannels + CChannel | Yes (VChannel) | SharedDBName + ExclusiveCollectionName |
| AlterCollection | Broadcast: VChannels + CChannel | Yes (VChannel) | SharedDBName + ExclusiveCollectionName |
| TruncateCollection | Broadcast: VChannels + CChannel (AckSyncUp) | Yes (VChannel) | SharedDBName + ExclusiveCollectionName |
| CreatePartition | Broadcast: VChannels + CChannel | Yes (VChannel) | SharedDBName + ExclusiveCollectionName |
| DropPartition | Broadcast: VChannels + CChannel | Yes (VChannel) | SharedDBName + ExclusiveCollectionName |
| CreateIndex | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| AlterIndex | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| DropIndex | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| CreateSnapshot | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName + ExclusiveSnapshotName |
| DropSnapshot | Broadcast: CChannel | No | ExclusiveSnapshotName |
| RestoreSnapshot | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName + ExclusiveSnapshotName |
| DropSnapshotsByCollection | Broadcast: CChannel | No | SharedDBName + SharedCollectionName |
| Import | Broadcast: VChannels (no CChannel) | No | SharedDBName + ExclusiveCollectionName |
| Insert | Single VChannel | No | — |
| Delete | Single VChannel | No | — |
| CreateSegment *(SelfControlled)* | Single VChannel | No | — |
| Flush *(SelfControlled)* | Single VChannel | No | — |
| ManualFlush | Single VChannel | Yes (VChannel) | — |
| SplitShard | Broadcast: every VChannel of the collection + target vchannel(s) + CChannel, sources appended first | Yes (VChannel-exclusive per data replica) | SharedDBName + ExclusiveCollectionName (held by the broadcast) |
| AlterLoadConfig | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| DropLoadConfig | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName (or ExclusiveCluster) |
| AlterRLSMetadata | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| DropRLSMetadata | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| BatchUpdateManifest | Broadcast: CChannel | No | SharedDBName + SharedCollectionName |
| RefreshExternalCollection | Broadcast: CChannel | No | — |

## Message Descriptions

- **CreateCollection**: Creates a new collection with its partitions and VChannels.
- **DropCollection**: Drops a collection and all its data, indexes, and load config. Implicitly flushes all growing segments.
- **AlterCollection**: Alters collection properties, description, consistency level, or schema. Schema changes implicitly flush growing segments. When used for **RenameCollection**, the ResourceKey changes to `ExclusiveDBName(srcDB) + ExclusiveDBName(dstDB)` (deduplicated if same DB), blocking all collection DDL in both databases. When its update mask carries `shard_split_routing` and the routing post-image no longer names the VChannel a given replica landed on (`messageutil.RetiresVChannel`), that replica **retires** the VChannel instead of applying a collection-wide update. Retire is not teardown: the split's fence already tore the shard-manager registration down, so this replica only marks the recovery meta `retired` (it stays SPLITTED) and is not forwarded to the data sync service — which closes itself when its own checkpoint passes the fence, since on a secondary this replica can arrive before the fenced segments are flushed. Such a commit is also the split's **adoption**, so its ack callback first asks the local DataCoord whether that `split_task_id` has drained here, and refuses to apply until it has.
- **TruncateCollection**: Logically truncates by sealing and dropping all segments before the truncation timestamp. Implicitly flushes all growing segments. Uses AckSyncUp.
- **CreatePartition** / **DropPartition**: Creates or drops a partition. DropPartition implicitly flushes the partition's growing segments.
- **CreateIndex** / **AlterIndex** / **DropIndex**: Manages indexes on a collection's field. CChannel-only.
- **CreateSnapshot** / **DropSnapshot** / **RestoreSnapshot** / **DropSnapshotsByCollection**: Manages collection snapshots. CChannel-only.
- **Import**: Initiates a bulk import job for a collection.
- **Insert** / **Delete**: DML on a single VChannel. CipherEnabled.
- **CreateSegment** / **Flush**: WAL-generated (SelfControlled). Allocates or seals a growing segment.
- **ManualFlush**: Seals all growing segments for a collection on a VChannel.
- **SplitShard**: One broadcast carrying every replica of a shard split, dispatched to **every VChannel the collection has**, to the target VChannel(s) the split creates, and to the CChannel. `BroadcastHeader.append_first_vchannels` names the sources; the broadcaster appends and persists them (via a partial ack) before any other replica, and the type property `FreshTimeTick` makes every other replica take a freshly fetched TSO batch, so a target's TimeTick is always greater than the sources' `T_switch`. The body carries the target genesis schema and the **routing post-image** the ack callback commits; the header carries the collection/task ids, sources, targets with residues, modulus and partition snapshot. Each replica is read by its role (`message.SplitShardRoleOf`):
  - **source** — the write fence. Its TimeTick is `T_switch`, and after it the VChannel never accepts DML again (`STREAMING_CODE_SHARD_FENCED`); the shard interceptor seals every growing segment as of `T_switch`, embeds their ids in the header — this message is the only seal record, there is no separate ManualFlush — and tears the registration down leaving a fence tombstone, and the flusher's `HandleSplitShard` hands those ids to the write buffer, which is then forwarded to the data sync service like any other message. A re-fence by the same split task appends again and succeeds, raising the recorded fence tick to the later record's (every fence record of one task seals the same data, since no DML ran in between); a fence attempted by another task — including one whose task id reads zero — is refused with `SHARD_FENCED` carrying the recorded tick and task id.
  - **target** — the genesis of the VChannel: the body's `genesis` (schema) and the header's partition ids register it exactly as CreateCollection would; this replica is never forwarded to the data sync service, which instead spawns the target's data sync service directly from it.
  - **bystander** — a VChannel of the **same collection** the split neither fences nor creates. The broadcast covers the whole collection, so this replica is expected: it lands and passes through the shard interceptor, the recovery storage and the flusher without effect, and is not forwarded to the data sync service.
  - **CChannel** — a no-op in every consumer; it exists only to give the ack callback a TimeTick to order against.
  - anything else — a replica on a VChannel of a *different* collection is a misroute: refused on the append path, reported as an inconsistency on the consume path.
- **AlterLoadConfig**: Modifies load configuration — partition set, replica count, load fields, etc. CChannel-only, consumed by QueryCoord.
- **DropLoadConfig**: Removes load configuration, unloading/releasing from query nodes. Uses ExclusiveCluster when part of DropCollection flow.
- **AlterRLSMetadata**: Persists a complete row-policy or principal-tag post-image in the ACK callback. CChannel-only and serialized with collection/schema DDL.
- **DropRLSMetadata**: Drops a row policy or principal-tag record by stable logical identity in the ACK callback. A policy name resolves through RootCoord's collection metadata to its internal policy ID, allowing the callback to remove the single ID-keyed etcd record without a prefix scan. CChannel-only and serialized with collection/schema DDL.
- **BatchUpdateManifest**: Updates segment manifest versions in batch. Used after compaction or index building. CChannel-only.
- **RefreshExternalCollection**: Submits an external collection refresh job using a pre-allocated job ID from the WAL message. CChannel-only.

## Replication Compatibility

Current producers explicitly mark these collection-scoped broadcast messages with `Unreplicable` (`_ur`): CreateSnapshot, DropSnapshot, RestoreSnapshot, BatchUpdateManifest, and RefreshExternalCollection. Replication skips the concrete marked messages instead of classifying the whole message type as permanently unsupported, so newly generated messages can become replicable later by no longer setting `_ur`.

`SplitShard` does not set `_ur` either: a shard split replicates, together with the `AlterCollection(shard_split_routing)` that adopts its targets. The secondary rewrites every channel name both messages carry — the header's sources and targets, the body's routing post-image (its shard infos included) and genesis lists — and holds every non-append-first replica until this cluster has acked the append-first ones, which is what restores the fence-before-genesis order that replication itself does not carry. All coordinator work is in the ack callbacks, so both clusters run the same code on their own `T_switch` and their own drain state. See [Replication](../replication/replicate.md).

`AlterRLSMetadata` and `DropRLSMetadata` do not set `_ur`. They are eligible for the generic CDC path: the secondary rebuilds the replicated broadcast task and invokes the same idempotent ACK callback to apply the complete post-image or stable drop identity. Dedicated end-to-end RLS CDC validation is tracked separately.

## Data Lifecycle Ordering Invariants

All TimeTick comparisons are within the same PChannel.

### Collection Lifecycle

```
CreateCollection → [CreatePartition | DropPartition]* → DropCollection
```

- **CreateCollection** must precede all other messages targeting this collection.
- **DropCollection** must be the last message. No messages may follow.
- **CreatePartition** must precede Insert/Delete targeting that partition.
- **DropPartition** terminates a partition. No Insert/Delete may target it afterward.
- **AlterLoadConfig** must come after CreateCollection. Referenced partitions must already exist.
- **DropLoadConfig** must come before DropCollection. DropPartition should be preceded by AlterLoadConfig removing the partition from the load set.

### Segment Lifecycle

```
CreateSegment → Insert* → (Flush | ManualFlush | DropPartition | DropCollection | TruncateCollection | FlushAll)
```

- **CreateSegment** must precede any Insert referencing that segment.
- Any message with flush semantics (Flush, ManualFlush, DropPartition, DropCollection, TruncateCollection, FlushAll) seals the segment. No Insert may reference it afterward.

### Shard Split VChannel Lifecycle

```
SplitShard(target genesis) → [Insert | Delete | CreateSegment | Flush]* → …
SplitShard(source fence) fences the source
AlterCollection(shard_split_routing) that delists the source retires it
```

All TimeTick comparisons here are within one cluster: `T_switch` is the tick the fence landed on locally, and a secondary's is a different number.

A target VChannel and the source VChannel(s) it replaces are born and die on opposite ends of the same broadcast timeline: the target's genesis and the source's fence are two replicas of the SAME SplitShard message, and the source's later retirement is a replica of the AlterCollection that commits the shrunken routing.

- A VChannel enters the WAL either through **CreateCollection** or through the **target genesis replica** of a **SplitShard** broadcast; both are exempt from the recovery storage's "vchannel not found" check because they create the VChannel they name.
- **SplitShard**'s **source replica** moves the VChannel to `VCHANNEL_STATE_SPLITTED` and records `T_switch` in `VChannelMeta.split_time_tick`. The state is persisted: the fence must still hold after a restart. A re-fence by the same split task appends again and succeeds, raising the recorded tick to the later record's, while a fence from another task is refused with `SHARD_FENCED`.
- The **retire replica** — the **AlterCollection** replica whose `shard_split_routing` commit no longer names the VChannel it landed on — leaves it **SPLITTED** and sets `retired`; the recovery storage collects the meta only once the flusher checkpoint has passed `split_time_tick`, and DataCoord's `DropVirtualChannel` is never called for it (see [RecoveryStorage](../wal/recovery-storage.md)). Because the commit that delists the VChannel from `collection.VirtualChannelNames` and the retirement of that VChannel are the same broadcast — which is why the broadcast reaches the current VChannel list as well as the post-image's — the two can never observe each other's absence: a VChannel removed from the list first, by some other path, could never be reached by any later message.
- **One VChannel per collection per PChannel.** The shard manager's registration map is keyed by collection id, but a split source frees its slot at the fence rather than at retirement, so a target may be placed on that PChannel as soon as the fence lands. What must never happen is a target landing on a PChannel that still holds a **live** VChannel of the same collection; the append path enforces that (`CheckIfVChannelCanBeCreated` → `ErrVChannelConflict`) rather than trusting the coordinator, because the failure it prevents is silent and permanent: the newcomer would skip its own registration and inherit the incumbent's state, fence included.
- **Target, bystander and retire replicas are never forwarded to the data sync service; the CChannel replica is a no-op in every consumer.** The flusher and the shard interceptor read each replica's role (`message.SplitShardRoleOf`, `messageutil.RetiresVChannel`) directly off the WAL: a target replica spawns the new VChannel's data sync service in place of forwarding, a bystander replica does nothing at all, a retire replica does not close anything — the source's data sync service closes itself once its own checkpoint passes the fence tick recorded when the source replica was dispatched — and the CChannel replica is skipped outright. Only the source replica falls through to the data sync service, whose flow graph seals the fenced segments and sets the flush timestamp.

### Exclusive Lock Rule

DDL messages (CreateCollection, DropCollection, CreatePartition, DropPartition, TruncateCollection, ManualFlush, FlushAll, SplitShard) acquire exclusive locks. While held:
- No DML (Insert/Delete) can append to locked VChannels.
- In-flight transactions on locked VChannels are failed.

DML (Insert/Delete) acquires **shared** locks — concurrent with each other, blocked by any exclusive lock.

### Example Timeline (single VChannel)

```
CreateCollection(p0)@tt=1                    (creates collection with default partition p0)
  → CreateSegment(seg=100)@tt=3              (WAL-generated)
    → Insert(p0, seg=100)@tt=5
    → Insert(p0, seg=100)@tt=6               (concurrent with tt=5)
    → Delete(p0)@tt=7                        (concurrent with Insert)
  → Flush(seg=100)@tt=8                      (WAL-generated, seg=100 sealed)
  → CreatePartition(p1)@tt=10                (add new partition)
    → CreateSegment(seg=101)@tt=11           (WAL-generated)
      → Insert(p1, seg=101)@tt=13
  → DropPartition(p1)@tt=15                  (exclusive, flushes p1 segments, no more p1 DML)
  → ManualFlush@tt=18                        (exclusive, seals remaining segments)
→ DropCollection@tt=20                       (exclusive, flushes all segments, collection terminated)
```
