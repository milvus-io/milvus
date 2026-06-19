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
| Import | Broadcast: VChannels (no CChannel) | No | — |
| Insert | Single VChannel | No | — |
| Delete | Single VChannel | No | — |
| CreateSegment *(SelfControlled)* | Single VChannel | No | — |
| Flush *(SelfControlled)* | Single VChannel | No | — |
| ManualFlush | Single VChannel | Yes (VChannel) | — |
| AlterLoadConfig | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName |
| DropLoadConfig | Broadcast: CChannel | No | SharedDBName + ExclusiveCollectionName (or ExclusiveCluster) |
| BatchUpdateManifest | Broadcast: CChannel | No | SharedDBName + SharedCollectionName |

## Message Descriptions

- **CreateCollection**: Creates a new collection with its partitions and VChannels.
- **DropCollection**: Drops a collection and all its data, indexes, and load config. Implicitly flushes all growing segments.
- **AlterCollection**: Alters collection properties, description, consistency level, or schema. Schema changes implicitly flush growing segments. When used for **RenameCollection**, the ResourceKey changes to `ExclusiveDBName(srcDB) + ExclusiveDBName(dstDB)` (deduplicated if same DB), blocking all collection DDL in both databases.
- **TruncateCollection**: Logically truncates by sealing and dropping all segments before the truncation timestamp. Implicitly flushes all growing segments. Uses AckSyncUp.
- **CreatePartition** / **DropPartition**: Creates or drops a partition. DropPartition implicitly flushes the partition's growing segments.
- **CreateIndex** / **AlterIndex** / **DropIndex**: Manages indexes on a collection's field. CChannel-only.
- **CreateSnapshot** / **DropSnapshot** / **RestoreSnapshot**: Manages collection snapshots. CChannel-only.
- **Import**: Initiates a bulk import job for a collection.
- **Insert** / **Delete**: DML on a single VChannel. CipherEnabled.
- **CreateSegment** / **Flush**: WAL-generated (SelfControlled). Allocates or seals a growing segment.
- **ManualFlush**: Seals all growing segments for a collection on a VChannel.
- **AlterLoadConfig**: Modifies load configuration — partition set, replica count, load fields, etc. CChannel-only, consumed by QueryCoord.
- **DropLoadConfig**: Removes load configuration, unloading/releasing from query nodes. Uses ExclusiveCluster when part of DropCollection flow.
- **BatchUpdateManifest**: Updates segment manifest versions in batch. Used after compaction or index building. CChannel-only.

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

### Exclusive Lock Rule

DDL messages (CreateCollection, DropCollection, CreatePartition, DropPartition, TruncateCollection, ManualFlush, FlushAll) acquire exclusive locks. While held:
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
