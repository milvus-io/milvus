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
| SplitShard | Broadcast: source VChannel + target VChannels + CChannel, source appended first | Yes (VChannel-exclusive on the source and target replicas) | SharedDBName + ExclusiveCollectionName (the issuer's obligation; no issuer exists on this branch) |
| AlterLoadConfig | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| DropLoadConfig | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName (or ExclusiveCluster) |
| AlterRLSMetadata | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| DropRLSMetadata | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| BatchUpdateManifest | Broadcast: CChannel | No | SharedDBName + SharedCollectionName |
| RefreshExternalCollection | Broadcast: CChannel | No | — |

## Message Descriptions

- **CreateCollection**: Creates a new collection with its partitions and VChannels.
- **DropCollection**: Drops a collection and all its data, indexes, and load config. Implicitly flushes all growing segments.
- **AlterCollection**: Alters collection properties, description, consistency level, or schema. Schema changes implicitly flush growing segments. When used for **RenameCollection**, the ResourceKey changes to `ExclusiveDBName(srcDB) + ExclusiveDBName(dstDB)` (deduplicated if same DB), blocking all collection DDL in both databases. When its update mask carries `shard_split_routing` and the routing post-image no longer names the VChannel a given replica landed on (`messageutil.RetiresVChannel`), that replica **retires** the VChannel instead of applying a collection-wide update. Retire is not teardown: the split's fence already tore the shard-manager registration down, so this replica only marks the recovery meta `retired` (it stays SPLITTED) and is not forwarded to the data sync service. That service is closed on the flusher dispatch goroutine once its acked checkpoint passes the fence (`closeDrainedFencedSources`), because on a secondary this replica can arrive before the fenced segments are flushed. Such a commit is also the split's **adoption**. Its ack callback goes through the single routing apply path (`MetaTable.ApplyShardSplitRouting`). It judges the post-image against the meta (`routing.JudgeCommit`) with the adoption's own delta -- the source it may retire and the targets it may adopt, which the local DataCoord recorded for that `split_task_id` (`CheckShardSplitDrained` describes the record as well as the drain) -- before it acts on whether the split has drained here, and refuses to apply until it has, with a retriable `ServiceUnavailable`. A post-image whose delta the collection already carries is a no-op. One that differs from the meta by more than the adoption's own delta in the forward direction -- a VChannel the collection does not carry yet, another split's source retired, a task this cluster has no record of -- belongs to a routing commit this cluster has not applied, and is also refused with a retriable `ServiceUnavailable`. Every other refusal names an incoherent post-image, including one that lists a shard as `Dropped`, since a shard reaches `Dropped` only by being delisted. Nothing in the resource keys orders this callback behind its split's own callback on a secondary across a rename; the judge is what keeps the two in order.
- **TruncateCollection**: Logically truncates by sealing and dropping all segments before the truncation timestamp. Implicitly flushes all growing segments. Uses AckSyncUp.
- **A collection-keyed DDL message addressed to a VChannel this PChannel does not hold appends without effect; a segment message is refused.** The shard interceptor checks this **once**, in `DoAppend`, with `CheckIfVChannelCanBeWritten` on the message's own VChannel, because the handlers act by *collection id*. Acting anyway would flush, re-schema or re-partition whichever VChannel does hold the entry. A shard split creates both ways this happens: the fenced source, which stays on the collection's VChannel list until adoption retires it, and a target that has taken the PChannel's single slot for that collection.

  | Message | VChannel not held here |
  |---|---|
  | CreatePartition, DropPartition, SchemaChange, AlterCollection, TruncateCollection | appended with no shard-state effect |
  | DropCollection | appended; only the addressed VChannel's function-runner key is released |
  | ManualFlush | fenced source: appended with an empty segment list and `ManualFlushExtraResponse{[]}`, so `Flush` succeeds; never held: refused (unrecoverable), as before |
  | Insert, Delete | not gated here; their own admission returns `SHARD_FENCED` for a fenced VChannel and an unrecoverable error otherwise |
  | CreateSegment, Flush | refused: `SHARD_FENCED` on a fenced source, unrecoverable on a VChannel never held; a Flush from the old architecture is exempt |
  | CreateCollection, SplitShard, FlushAll, Import | not gated: genesis/fence, PChannel-level, or no handler |

  A broadcast replica is appended rather than refused, because an unrecoverable refusal would wedge the collection's DDL behind the broadcaster's forever-retry while it holds the exclusive key. CreateSegment and Flush are not broadcast replicas. This node's own segment workers append them, and the workers stop on either error. Their handlers look a partition up by (collection, partition), which a split target of the same collection on the same PChannel shares, so a source worker still retrying after the fence would otherwise act on the target. Dropping a partition manager also cancels its segment-alloc worker. On the consume side, [RecoveryStorage](../wal/recovery-storage.md) skips a CreateSegment it observes on a SPLITTED VChannel, since only a replay can deliver one; it reports an inconsistency only when the CreateSegment is ticked after the fence gate, a replay of the pre-fence history on a restart being expected.
- **CreatePartition** / **DropPartition**: Creates or drops a partition. DropPartition implicitly flushes the partition's growing segments.
- **CreateIndex** / **AlterIndex** / **DropIndex**: Manages indexes on a collection's field. CChannel-only.
- **CreateSnapshot** / **DropSnapshot** / **RestoreSnapshot** / **DropSnapshotsByCollection**: Manages collection snapshots. CChannel-only.
- **Import**: Initiates a bulk import job for a collection. The shard interceptor takes no action on it on any VChannel, so an Import replica on a fenced split source appends with no shard effect.
- **Insert** / **Delete**: DML on a single VChannel. CipherEnabled.
- **CreateSegment** / **Flush**: WAL-generated (SelfControlled). Allocates or seals a growing segment.
- **ManualFlush**: Seals all growing segments for a collection on a VChannel.
- **SplitShard**: One broadcast carrying every replica of a shard split, dispatched to the **source** VChannel it fences, to the target VChannels it creates, and to the CChannel — and to nothing else: a shard the split leaves alone gets no replica. `BroadcastHeader.append_first_vchannels` names the source; the broadcaster appends and persists it (via a partial ack) before any other replica, and the type property `FreshTimeTick` makes every other replica take a freshly fetched TSO batch, so a target's TimeTick is always greater than the source's `T_switch`. The body carries the target genesis schema and the **routing post-image** the ack callback commits — the only copy of the targets' residues and the routing modulus; the header carries only the collection/task ids, the `source_vchannel`, the `target_vchannels` names and the partition snapshot. Each replica is read by its role (`message.SplitShardRoleOf`):
  - **source** — the write fence. Its TimeTick is `T_switch`, and after it the VChannel never accepts DML again (`STREAMING_CODE_SHARD_FENCED`); the shard interceptor seals every growing segment as of `T_switch`, embeds their ids in the header — this message is the only seal record, there is no separate ManualFlush — and tears the registration down leaving a fence tombstone. The fence is installed **before** the record is appended and kept whatever the append returns, because an append error does not prove the record is absent. If the append did fail, DML stays refused with `SHARD_FENCED` until the broadcaster's re-drive lands; a node restart before that loses the unrecorded fence, and the re-drive is a genuine first fence. The flusher records this record's own tick as the close gate of the VChannel's data sync service and forwards the replica to it. The service's `dd_node` then calls `msgHandler.HandleSplitShard` (interface in `internal/flushcommon/util`), which seals **every** growing segment of the VChannel in the write buffer, not only the header's ids: a re-driven record carries none, and a CreateSegment whose append errored after persisting is missing from them. That call is the only seal, so a failure there leaves the source checkpoint short of `T_switch` and the split's drain waiting forever. The source replica's append result carries `SplitShardExtraResponse{split_time_tick}` — the task's first fence tick — and the same value is stamped on the record, so a consumer-side ack reports it too; the ack callback records `T_switch` from it and refuses (retriable System error) a source result without it. A re-fence by the same split task appends again and succeeds but moves nothing: the tombstone, `split_time_tick` and the flusher's close gate do not move, and the re-fence reports the first fence's tick. Every fence record of one task seals the same data, since no DML ran in between. After a failed first append the re-drive is the first record in the WAL, so its own tick is later than the `T_switch` it reports, and the close gate is that later tick. A source replica reaching a VChannel another task fenced (a recorded task id of zero counts as another task) is refused with `SHARD_FENCED` carrying the recorded tick and task id, and reports no extra response. A source replica whose own `split_task_id` is zero is refused (unrecoverable) before anything else, on a first fence and a re-fence alike; `SplitShardParam.Validate` already refuses it before the broadcast.
  - **target** — the genesis of the VChannel: the body's `genesis` (schema) and the header's partition ids register it exactly as CreateCollection would; this replica is never forwarded to the data sync service, which instead spawns the target's data sync service directly from it. RecoveryStorage records the replica's position (last confirmed message id and time tick) in `VChannelMeta.split_genesis_checkpoint`. A restarted flusher recovers the target from that position when DataCoord has none for it yet, because the ack callback has not seeded one. It never recovers the target from a position before the genesis, including DataCoord's fallback to the collection's creation position.
  - **CChannel** — a no-op in every consumer; it exists only to give the ack callback a TimeTick to order against.
  - anything else — a replica on any other VChannel, another shard of the same collection included, is a misroute: refused on the append path, reported as an inconsistency on the consume path.
- **AlterLoadConfig**: Modifies load configuration — partition set, replica count, load fields, etc. CChannel-only, consumed by QueryCoord.
- **DropLoadConfig**: Removes load configuration, unloading/releasing from query nodes. Uses ExclusiveCluster when part of DropCollection flow.
- **AlterRLSMetadata**: Persists a complete row-policy or principal-tag post-image in the ACK callback. Policy mutations invalidate the collection policy cache. Principal-tag mutations invalidate only that principal, including creation so an in-flight lookup cannot publish a pre-create miss. CChannel-only and serialized with collection/schema DDL; cache invalidation failures are retried by the broadcaster callback.
- **DropRLSMetadata**: Drops a row policy or principal-tag record by stable logical identity in the ACK callback. A policy name resolves through RootCoord's collection metadata to its internal policy ID, allowing the callback to remove the single ID-keyed etcd record without a prefix scan. Policy drops invalidate the collection policy cache, while principal drops invalidate only that principal. CChannel-only and serialized with collection/schema DDL; cache invalidation failures are retried by the broadcaster callback.
- **BatchUpdateManifest**: Updates segment manifest versions in batch. Used after compaction or index building. CChannel-only.
- **RefreshExternalCollection**: Submits an external collection refresh job using a pre-allocated job ID from the WAL message. CChannel-only.

RLS cache invalidation does not fetch metadata in the ACK callback. Policy
metadata remains collection-scoped. Principal tags are cached by
`(collectionID, principal)` and loaded lazily only when that principal sends an
RLS-enforced request. There is no background RLS reconciliation loop and no
negative principal cache.

## Replication Compatibility

Current producers explicitly mark these collection-scoped broadcast messages with `Unreplicable` (`_ur`): CreateSnapshot, DropSnapshot, RestoreSnapshot, BatchUpdateManifest, and RefreshExternalCollection. Replication skips the concrete marked messages instead of classifying the whole message type as permanently unsupported, so newly generated messages can become replicable later by no longer setting `_ur`.

`SplitShard` does not set `_ur` either: a shard split replicates, together with the `AlterCollection(shard_split_routing)` that adopts its targets. The secondary rewrites every channel name both messages carry — the header's source and target names, the body's routing post-image (its shard infos included) and genesis lists — and holds every non-append-first replica until this cluster has acked the append-first ones, which is what restores the fence-before-genesis order that replication itself does not carry. All coordinator work is in the ack callbacks, so both clusters run the same code on their own `T_switch` and their own drain state. See [Replication](../replication/replicate.md).

`AlterRLSMetadata` and `DropRLSMetadata` do not set `_ur`. They are eligible for the generic CDC path: the secondary rebuilds the replicated broadcast task and invokes the same idempotent ACK callback to apply the complete post-image or stable drop identity and invalidate local Proxy RLS caches. Dedicated end-to-end RLS CDC validation is tracked separately.

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

A target VChannel and the source VChannel it replaces are born and die on opposite ends of the same broadcast timeline. The target's genesis and the source's fence are two replicas of the SAME SplitShard message, and the source's later retirement is a replica of the AlterCollection whose routing commit delists it.

- A VChannel enters the WAL either through **CreateCollection** or through the **target genesis replica** of a **SplitShard** broadcast; both are exempt from the recovery storage's "vchannel not found" check because they create the VChannel they name.
- **SplitShard**'s **source replica** moves the VChannel to `VCHANNEL_STATE_SPLITTED` and records `T_switch` in `VChannelMeta.split_time_tick`, read from the `SplitShardExtraResponse` stamped on the record (`_ae`). The state is persisted: the fence must still hold after a restart. A re-fence by the same split task appends again and succeeds without moving `split_time_tick`, while a fence from another task is refused with `SHARD_FENCED`.
- The **retire replica** — the **AlterCollection** replica whose `shard_split_routing` commit no longer names the VChannel it landed on — leaves it **SPLITTED** and sets `retired`; the recovery storage collects the meta only once the VChannel's own flusher checkpoint has reached its fence gate (`DrainedPastFence`, `recovery.SplitFenceGate`): the tick of the seal record the data sync service consumed, which is `split_time_tick` except after a re-driven fence, where it is later, and is reseeded on restart as `max(split_time_tick, checkpoint tick)`. DataCoord's `DropVirtualChannel` is never called for it (see [RecoveryStorage](../wal/recovery-storage.md)). Because the commit that delists the VChannel from `collection.VirtualChannelNames` and the retirement of that VChannel are the same broadcast — which is why the broadcast reaches the current VChannel list as well as the post-image's, and why the adoption callback refuses a retiring commit that was not broadcast to the retired VChannel — the two can never observe each other's absence: a VChannel removed from the list first, by some other path, could never be reached by any later message.
- A genuine **DropCollection** of a split collection is a drop, not a retirement. The source's replica moves its meta to DROPPED with `retired` cleared, so `DropVirtualChannel` is called for it.
- **One VChannel per collection per PChannel.** The shard manager's registration map is keyed by collection id, but a split source frees its slot at the fence rather than at retirement, so a target may be placed on that PChannel as soon as the fence lands. What must never happen is a target landing on a PChannel that still holds a **live** VChannel of the same collection; the append path enforces that (`CheckIfVChannelCanBeCreated` → `ErrVChannelConflict`) rather than trusting the coordinator, because the failure it prevents is silent and permanent: the newcomer would skip its own registration and inherit the incumbent's state, fence included.
- **Target and retire replicas are never forwarded to the data sync service; the CChannel replica is a no-op in every consumer.** The flusher and the shard interceptor read each replica's role (`message.SplitShardRoleOf`, `messageutil.RetiresVChannel`) directly off the WAL: a target replica spawns the new VChannel's data sync service in place of forwarding, a retire replica does not close anything — the source's data sync service is closed on the flusher dispatch goroutine (`closeDrainedFencedSources`) once its acked checkpoint passes the close gate: the own tick of the first source record dispatched to it, reseeded on restart from `max(split_time_tick, checkpoint tick)` — and the CChannel replica is skipped outright. Only the source replica falls through to the data sync service, whose flow graph seals the fenced segments and sets the flush timestamp.

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
