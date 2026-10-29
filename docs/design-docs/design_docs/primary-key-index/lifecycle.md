# MEP: Lifecycle of the primary key index

- **Created:** 2026-10-28
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** DataCoord, StreamingNode
- **Related Issues:** #52305

## Summary

This document describes what happens to the primary key index of a collection
after its create ([ddl.md](ddl.md)): the operations that are rejected, the
truncate of the collection, and the removal of the objects of a dropped index.

- An import, a snapshot create and a snapshot restore add or copy rows without
  the inserts of the WAL. DataCoord rejects them on a collection with a
  primary key index.
- A truncate resets the index of each vchannel in place, at the WAL position of
  the truncate. The next handover commits an empty table set to the manifest.
- The object keys and the manifest pointer keys hold the index ID. The GC
  removes the objects of an index by its index ID, after the tolerances of the
  binlog GC.

## Principle

The index is correct when its keys are a superset of the primary keys of all
live rows.

- An operation that **adds** rows without the write path breaks the rule. It
  must be rejected: import and snapshot restore.
- An operation that **removes** rows leaves stale keys in the index. A stale key
  can only cause one extra companion delete for a key that has no live row.
  The result stays correct, so the operation is allowed: DropPartition and TTL
  expiry. The space of the stale keys is a separate problem. A truncate resets
  the index in place. DropPartition and TTL keep their stale keys until a later
  phase.

## Rejected operations

A request that asks for an unsupported operation gets an Input error
(`ParameterInvalid`). A collection "has a primary key index" when it has one
that is not deleted. A Failed index counts until its DropIndex, because it can
still be enabled on some vchannels.

| Operation | Where | When it is checked |
|---|---|---|
| Import (bulk insert) | `broadcastImport`, `ddl_callbacks_import.go` | Under the collection lock of the import broadcast. A CreateIndex takes the same lock. |
| Snapshot create | `CreateSnapshot`, `services.go` | Under the collection lock of the snapshot broadcast. A restore of the snapshot would copy rows without the WAL. |
| Snapshot restore of a snapshot that holds a primary key index | `snapshot_manager.go` | Before the restore creates the collection. The snapshot is immutable, so no lock is needed. |
| Snapshot restore into a collection that got a primary key index after its creation | `snapshot_manager.go` | Under the lock of the restore message. The restore rolls back the new collection. |

The last row closes a race. A restore creates a new, empty collection, and
then broadcasts the restore message. A CreateIndex can take the collection lock
between the two, and its precheck sees an empty collection. So the restore
checks again under the lock of its own message. After the restore message, the
copy job exists, and the precheck of a CreateIndex rejects the create while the
job runs.

The callback of a replicated import is not checked. A callback must not fail,
and the primary accepted the import.

Not rejected:

- A user drop of the primary key index.
- The DropIndex that the callback of a DropCollection calls inside DataCoord.

## Object layout and GC by index ID

### Layout

The index ID is a level in every key:

| Store | Key |
|---|---|
| Meta kv | `<metaRoot>/pkindex-meta/manifest/<collectionID>/<indexID>/<vchannel>` |
| Object storage | `<rootPath>/pk_index/<collectionID>/<indexID>/<vchannel>/manifest/<version>-<uuid>.pb` |
| Object storage | `<rootPath>/pk_index/<collectionID>/<indexID>/<vchannel>/sst/<id>.sst` |
| Local disk of the streaming node | `<root>/<vchannel>/<indexID>/` |

- `internal/pkindex/objpath.go` builds the object keys, and
  `internal/datacoord/pkindex/manifest/metakey.go` builds the pointer keys.
- The streaming node takes the index ID from `pk_index.index_id` of the
  vchannel meta and sends it in `CommitFlushRequest.index_id`. The first flush
  records it in `Manifest.index_id`.
- A reader of a manifest uses its recorded `sst_base_path`. It does not build
  the path.
- No compatibility with the old layout: no release has the old layout.

Preconditions, checked in the code:

- DataCoord allocates the index ID with the global ID allocator and never
  reuses it.
- The index ID goes to a CDC secondary in the body of the CreateIndex, so both
  clusters use the same keys.
- A snapshot restore creates a new collection with a new collection ID, so its
  keys can not collide. The restore of a primary key index is also rejected.

So a dropped index and a new index of the same collection never share a key.
A new index does not wait for the cleanup of the old one.

### GC rules

Each round of the GC (`internal/datacoord/pkindex/gc`) also handles whole
indexes (`runIndexes`, `index_gc.go`). It finds the indexes from the manifest
pointers and from one recursive walk of `pk_index/`. It does not use a
non-recursive listing: MinIO and GCS report a zero modification time for a
directory, Azure reports the current time, and the local chunk manager reports
the directory itself.

The age of an index is the time since the newest modification of its objects.
An index without objects has an infinite age.

| The index in the DataCoord meta | Action |
|---|---|
| Live (not deleted) | Keep |
| Deleted (a user drop or the compensation) | Remove when the age is above `dataCoord.gc.dropTolerance` |
| Missing | Remove when the age is above `dataCoord.gc.missingTolerance`. This keeps an index whose CreateIndex callback did not write the meta yet. |
| The collection does not exist (`HasCollection` returns false) | Remove when the age is above `dataCoord.gc.dropTolerance` |

- A `HasCollection` error skips the round before anything is removed.
- A collection with a paused GC (`collectionGCPaused`, used by a backup) is
  skipped.
- The meta GC removes a deleted index from the DataCoord meta on its next cycle
  (`recycleUnusedIndexes`), and the meta has no delete time. So a dropped index
  is usually "missing" when the GC of the primary key index sees it.
- Order: the pointers first, through `Manager.DropIndex(collectionID, indexID,
  vchannels)`, which removes them under the lock of each vchannel and forgets
  their cache. Then the objects under `pk_index/<collectionID>/<indexID>/`.
  Each step is idempotent. A crash between them leaves objects without a
  pointer, and the next round removes them.
- No tombstone. A late flush can write a pointer again under a dropped index
  ID. The index is then still deleted or missing, so a later round removes it
  again. A new index has a new ID and is not affected.

## Truncate: reset the index in place

A truncate has a time tick T on each vchannel. Every row with a time tick below
T is gone, so the index must forget their keys. The TruncateCollection is an
exclusive message, and it takes the same exclusive collection key as the
CreateIndex.

### Observe

The observe of the WAL recovery handles a TruncateCollection on a data vchannel
with an open Index (`PChannelIndexManager.ResetIndex`, `index/reset.go`):

- It keeps the Index and its Decider. The observe runs after the append, so the
  in-flight set can hold writes after T that the append path decided. A new
  Decider would lose them.
- `Engine.Reset` empties the engine in one step under the engine locks. It
  opens a new empty active generation and retires every other generation. A
  lookup sees the whole old state or an empty engine.
- The Index forgets the recovered manifest and the installed tables, drops a
  prepared handover, records `resetTT = T`, and becomes dirty. The dirty Index
  goes into the `LastAcked` heap of the pchannel.
- It does nothing when `T <= LastCommittedTimeTick`, because a committed
  manifest covers T and the flush that committed it carried the reset or came
  after it. It also does nothing when T is before the enable time tick of the
  Index.
- The vchannel module sets `ever_written` back to NEVER. `pk_index` stays.

### Persistence

The next handover commits the reset:

- It waits until the observed time tick passes T.
- It sends a flush with `reset_time_tick = T`, also when no table is new. The
  SSTs of the flush replace the SST list of the manifest, so a reset with no
  SST empties the list.
- A handover that was prepared before the reset sends nothing. If its flush is
  in flight, it installs nothing, retires nothing and does not move
  `LastCommittedTimeTick`. A reset epoch under the install lock detects it.

### Manifest service

`CommitFlushRequest.reset_time_tick` is set by the streaming node. When it is
set, the SSTs of the flush replace the SST list. An SST that the old list holds
can come again. An SST that is twice in the flush is `ErrDuplicateSST`. The
retry of an applied reset is an applied retry, as for every flush. The service
rejects a reset time tick above the checkpoint as an internal error. The
existing mark-sweep GC removes the replaced SSTs.

### Recovery

- A crash before the commit of the reset: the manifest checkpoint is below T.
  The replay sees the truncate again and resets again.
- A replay of a truncate whose reset is committed: `T <= LastCommittedTimeTick`,
  so it does nothing.
- An install of a recovered manifest runs after the barrier, in a task. It
  does not install a manifest whose checkpoint is below the last reset, so it
  does not bring back the tables before T.

### WAL truncation

Before the reset is committed, `LastCommittedTimeTick < T`. The Index is dirty,
so it is in the `LastAcked` heap and holds the WAL truncation at its last
commit until the reset commits.

### CDC

The truncate is replicated. The streaming nodes of the secondary reset at the
same position. The coordinator does nothing.

### Why not a new index ID

TiDB gives a truncated table a new table ID and removes the old range (not
verified). This change keeps the index ID. A truncate does not go through the
index meta of DataCoord, so a new ID would need a new coordinator path. The
reset in place needs only the streaming node and the manifest.

### Writes between the truncate append and its observe

The append path decides a write after the truncate before the observe reaches
the truncate. The engine still holds the keys before T at that time. So such a
write can get a companion delete for a key that the truncate removed. The delete
masks no live row. It costs one extra delete message.

## Allowed and not handled

- **DropPartition.** A companion delete uses `common.AllPartitionsID`. A stale
  key of a dropped partition gives one extra delete when it is written again.
- **Collection TTL expiry.** The same as DropPartition.

The keys of both stay in the index until a later phase adds a drop marker and a
purge.

## Comparison with similar systems

| System | What it does | Source | This design |
|---|---|---|---|
| Kafka KIP-516 topic IDs | A topic known only by name mixes old data into a topic of the same name that is created again after a delete. The old fix required a full delete before a new create, so one down broker blocked the name for a long time. The KIP gives each topic a unique ID. | KIP-516 (motivation verified) | The keys hold the index ID, so a new index does not wait for the cleanup of the old one. |
| TiDB | Keys start with `t{tableID}_i{indexID}`. DROP TABLE or DROP INDEX records the range to delete and its time. A GC removes the range after the GC safe point. | TiDB GC overview (range delete verified, `gc_delete_range` details not verified) | The GC reads the drop from the meta and waits a tolerance. |
| CockroachDB | Keys are `/Table/<tableID>/<indexID>/...`. After a drop, a GC job clears the span when `gc.ttlseconds` expires. | Product docs (key layout not verified in the official docs) | The same. |
| Milvus binlog GC | `dataCoord.gc.missingTolerance` keeps a file that the meta does not know for a time, so a file that is written but not yet registered survives. `dropTolerance` keeps the files of a dropped segment for a time. `collectionGCPaused` lets milvus-backup pause the GC of a collection. | `configs/milvus.yaml`, `datacoord/garbage_collector.go` | The same three rules. |

## Not done

- **The truncate reset and the GC by index ID have unit tests only.** The
  integration suite does not run them (see [ddl.md](ddl.md#not-done)).
- **DropPartition and TTL.** Their stale keys stay in the index.
- **Delete time of an index.** `FieldIndex` has no delete time, so the GC uses
  the newest object time for both tolerances.
- **Snapshot protection of the SSTs.** A snapshot of a collection with a
  primary key index is rejected, so no snapshot holds them yet.

## Test plan

Unit tests cover these behaviors.

Rejection (`internal/datacoord`):

- An import and a snapshot create of a collection with a primary key index are
  rejected. A restore of a snapshot that holds a primary key index is rejected
  before the collection is created. A restore whose new collection got a
  primary key index is rejected and rolls back.

Layout and GC (`internal/pkindex`, `internal/datacoord/pkindex/...`):

- The object keys and the pointer keys hold the index ID. Two indexes of one
  vchannel have separate manifests. `DropIndex` removes the pointers.
- The GC keeps a live index, removes a missing index after the missing
  tolerance, and a deleted index or one of a dropped collection after the drop
  tolerance. It skips the round on a collection check error and skips a paused
  collection. It removes what a failed round left, the pointer of a late flush,
  and objects without a pointer. It ignores keys of another layout. It rejects
  a negative tolerance.

Truncate (`internal/pkindex/engine`, `pkindex`, `pkindex/recovery`,
`pkindex/manifest`):

- `Engine.Reset` empties the engine, and a reset of a closed engine fails.
- The reset forgets the keys before the truncate and keeps the in-flight writes
  after it. A reset at or before the committed checkpoint does nothing. A
  handover commits a reset without new writes. A reset during a commit drops
  its result. A reset before the commit of a prepared handover sends nothing.
  An install after a reset installs nothing.
- The replay of a truncate resets the index, and the replay of a committed
  truncate does nothing.
- A flush with a reset replaces the SST list.

Not run: a truncate with a kill of the streaming node on a cluster, and a
truncate concurrent with a compaction. The truncate with a kill needs a
kill -9 in the integration suite; the concurrent case needs the compaction of
the primary key index.
