# MEP: Create and drop the primary key index

- **Created:** 2026-10-28
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** Proxy, DataCoord, StreamingCoord, StreamingNode
- **Related Issues:** #52305

## Summary

A user enables the primary key index of a collection with
`create_index(field=<pk>, index_type="PRIMARY_KEY")` and removes it with
`drop_index`. This document describes how the create reaches every vchannel,
where each vchannel decides whether it can enable the index, how the decisions
come back to DataCoord, and how DataCoord removes an index that failed.

The first phase has no backfill. So the index can start on a vchannel only at a
position where the vchannel holds no row. The CreateIndex goes to every
vchannel of the collection. Each streaming node decides "empty" at the WAL
position of the CreateIndex, from a one-way write history in the vchannel meta.
It sends the decision back with the broadcast ack. The ack callback in
DataCoord writes the index state once: Finished when every vchannel enabled the
index, Failed otherwise. The index inspector broadcasts a DropIndex for a
Failed index. The create RPC returns the reason of the failure.

The lifecycle after the create (truncate, import and restore rejection, the
removal of the objects of a dropped index) is in [lifecycle.md](lifecycle.md).

## Motivation

The index of a vchannel must hold the primary key of every live row of the
vchannel. A key that the index does not hold gives no companion delete, and the
duplicate row stays (see [write path](write-path.md)). Without a backfill, the
only safe start is an empty vchannel.

Three facts make the create hard:

1. A check in DataCoord before the broadcast is a check-then-act. An insert can
   reach the WAL after the check and before the CreateIndex. Only the vchannel
   itself can decide at the exact position of the CreateIndex.
2. A broadcast that is persisted can not be rejected. Each vchannel handles
   it at a different time, so the vchannels can come to different decisions.
3. The user sees one index state. It must be "all vchannels" or "none".

## Correctness

The proxy routes an insert, an upsert and a delete by primary key with
`typeutil.HashPK2Channels` (`proxy/dml/util_dml.go`,
`task_upsert_streaming.go`, `task_delete.go`). So one primary key always goes
to the same vchannel. "No duplicate primary key in the collection" is the same
as "no duplicate primary key in each vchannel".

So an index that starts on an empty vchannel is correct for that vchannel,
whatever the other vchannels decide. The "all or none" rule is a product
requirement, not a correctness requirement. This is why the design needs no
two-phase commit.

**Constraint.** The mapping from a primary key to its vchannel must not
change. A future resharding, or any other change of this mapping, must handle
the primary key index first.

## Product semantics

- API: `create_index(field=<pk>, index_type="PRIMARY_KEY")`. There is no
  `unique` param.
- Limits:
  - The field must be the primary key, of type Int64 or VarChar, without
    autoID.
  - No index param other than the index type. `mmap.enabled=true` is
    rejected.
  - A field has at most one index that is not deleted. So the primary key index
    blocks every other index on the primary key field, and an index on that
    field (for example STL_SORT) blocks the primary key index.
  - An external collection is rejected: its rows come from a refresh job, not
    from the WAL.
  - `streaming.pkindex.enabled` must be true on the proxy and on DataCoord.
  - Every streaming node must have a session version in the supported range.
- The collection must be empty. A truncate makes a collection empty again.
- The result is one of two:
  - Success: every vchannel deduplicates.
  - Failure: the RPC returns the reason. The system removes the index without
    user action, and at the end no vchannel deduplicates. The user can create
    the index again later.
- Transition window. It occurs only in a race: the precheck passes, and a
  concurrent write reaches a vchannel before the CreateIndex. Between the
  create and the end of the cleanup (usually seconds), some vchannels may
  deduplicate. A duplicate key that one of them overwrote in that window stays
  overwritten. PostgreSQL documents the same kind of window for an INVALID
  index.
- A user can drop the primary key index.

## Flow

```
proxy:      check the params (indexparamcheck PRIMARY_KEY checker)
               │
DataCoord:  under the collection lock of the broadcast
              switch, field, external collection      → Input error
              a streaming node outside the version range → ServiceNotReady
              precheck: a segment that is not dropped,
                        or a running snapshot restore  → Input error, no broadcast
            broadcast CreateIndex(header.index_type=PRIMARY_KEY)
              to every vchannel (the broadcaster adds the CChannel)
              ExclusiveRequired, AckSyncUp
               │
               ▼ on each data vchannel
StreamingNode append (interceptor):
              write history read before the append is NEVER → open the Index
              otherwise → writes wait until the observe decides
StreamingNode observe (WAL recovery, at the time tick X of the CreateIndex):
              vchannel module: ever_written == NEVER → pk_index = {index_id, field_id, X}
              primary key index: pk_index names this index at X → open or keep the Index
                                 otherwise → drop the Index that the append opened
              broadcast ack: CreateIndexAckResult{ENABLED | NOT_EMPTY | DISABLED}
               │
               ▼ after every vchannel acked
DataCoord ack callback (the only writer of the index meta):
              all ENABLED → state Finished
              otherwise   → state Failed, fail_reason names the vchannels
                            notify the index inspector
               │
               ▼ Broadcast returns
DataCoord:  state Failed → the RPC returns fail_reason as an Input error
               │
               ▼ only for Failed, only on the primary cluster
index inspector: broadcast DropIndex(header.index_type=PRIMARY_KEY) to every vchannel
              interceptor: forget the target after the append
              observe: clear pk_index, close the Index, remove its local directory
              ack callback: MarkIndexAsDeleted
              GC: remove the objects by index ID (lifecycle.md)
```

### Why the compensation is not in the ack callback

The broadcaster releases the resource keys of a broadcast only at
`MarkAckCallbackDone` (`broadcast_task.go`). The ack callback scheduler holds
its lock while the callback runs (`ack_callback_scheduler.go`). The CreateIndex
holds the exclusive key of the collection. A callback that broadcasts a
DropIndex with the same key and waits for it would deadlock.

The existing cascade drops in a callback (`ddl_callbacks_drop_collection.go`,
`ddl_callbacks_alter_collection_properties.go`) call the DropIndex callback
directly and change only the meta. They write no WAL message. The primary key
index needs a message at a WAL position on each vchannel, so this pattern does
not apply.

## Messages and meta

### Messages

- `CreateIndexMessageHeader` and `DropIndexMessageHeader` get `index_type`
  (`pkg/proto/messages.proto`). A streaming node finds a primary key index
  message from the header and does not read the body. The CreateIndex header
  carries the type for every index type.
- The DropIndex header carries `PRIMARY_KEY` when `index_ids` holds the primary
  key index, or when `index_ids` is empty (drop all) and the collection has a
  primary key index. The streaming node acts only on the ID of its own index.
- `MessageTypeCreateIndex` and `MessageTypeDropIndex` are `ExclusiveRequired`
  (`message_type.go`). The copy on the control channel keeps the shared lock.
  So only the copies on the data vchannels take the exclusive vchannel lock,
  and only a primary key index message goes to the data vchannels. Another
  index type still goes to the control channel only.
- A primary key index CreateIndex uses
  `WithBroadcast(vchannels, OptBuildBroadcastAckSyncUp())`. A DropIndex of the
  primary key index uses `WithBroadcast(vchannels)`. The broadcaster adds the
  CChannel to both.
- No new message type. A message stands for a DDL operation, and the header
  tells the index type. The index meta and the callbacks of DataCoord are
  shared by all index types.

### The ack result

The broadcast ack can carry a result to the ack callback. This is a generic
feature of the streaming layer:

- `BroadcastAckRequest.extra` and `AckedCheckpoint.extra` are
  `google.protobuf.Any` (`pkg/proto/streaming.proto`).
- `streaming.WAL().Broadcast().Ack`, the coordinator client and the
  broadcaster take the extra. The broadcast task stores it in the acked
  checkpoint of the vchannel and persists it with the task. The first ack of a
  vchannel decides its extra. A repeated ack does not change it.
- `message.AppendResult.Extra` gives it to the ack callback, also after a
  coordinator restart.
- FastAck and LegacyAck attach no extra.

The streaming node attaches `pkindexpb.CreateIndexAckResult` only to a
primary key index CreateIndex on a data vchannel
(`broadcastAckExtraLocked`, `wal/recovery/broadcast_ack_extra.go`). It runs
right after the recovery modules observed the message, under the lock of the
recovery storage. The broadcast ack module keeps the extra with the ack task,
so a retry of the ack sends the same extra.

| Outcome | Condition on the streaming node |
|---|---|
| `ENABLED` | `pk_index` of the vchannel names this index ID with the time tick of this message |
| `NOT_EMPTY` | Otherwise: rows were written before the CreateIndex, the write history is UNKNOWN, or no vchannel state exists |
| `DISABLED` | `streaming.pkindex.enabled` is false on the node |

The callback (`pkIndexStateOf`, `internal/datacoord/pkindex_ddl.go`) skips the
control channel. A data vchannel with no extra, an extra of another type, or an
unknown outcome counts as "streaming node gave no result". Every result other
than `ENABLED` makes the index Failed.

### The vchannel meta

`CollectionInfoOfVChannel` (`pkg/proto/streaming.proto`) gets two fields:

```proto
enum VChannelWriteHistory {
  VCHANNEL_WRITE_HISTORY_UNKNOWN = 0; // vchannel created before this field existed; treated as written.
  VCHANNEL_WRITE_HISTORY_NEVER = 1;
  VCHANNEL_WRITE_HISTORY_WRITTEN = 2; // stays WRITTEN even if all rows are deleted later.
}
message PKIndexOfVChannel {
  int64 index_id = 1;
  int64 field_id = 2;
  uint64 enable_time_tick = 3;
}
// CollectionInfoOfVChannel
VChannelWriteHistory ever_written = 4;
PKIndexOfVChannel pk_index = 5; // unset means not enabled
```

| Observed message | `ever_written` | `pk_index` | Checkpoint of the meta |
|---|---|---|---|
| CreateCollection | NEVER | unset | moves |
| Insert, Txn, Import, CommitImport | WRITTEN (one way) | no change | no change |
| TruncateCollection at T | NEVER | no change, the Index resets in place ([lifecycle.md](lifecycle.md)) | moves |
| CreateIndex(PRIMARY_KEY) at X | no change | `{index_id, field_id, X}` when NEVER, else no change | moves |
| DropIndex(PRIMARY_KEY) that names the index or names no index | no change | unset | moves |

- An enum and not a bool: a proto3 bool reads as false when the field is
  absent. A vchannel meta from before the field would read as "never written".
  That is the unsafe direction.
- A write marks the meta dirty only when the history changes. It does not move
  the checkpoint, because the replay of a write is idempotent.
- A CreateIndex or a DropIndex moves the checkpoint, so its replay does not
  decide again.
- The name follows the "has or once had" one-way flags of PostgreSQL
  (`pg_class.relhassubclass`).

Why not derive "empty" from the state that exists:

- `SegmentAssignmentMeta` goes away after a segment becomes TOMBSTONED and the
  state is persisted (`segment/view.go`).
- `segment_data_version_summary` is the oldest version that can be rebuilt
  after cleanup. A growing segment does not move it.
- DataCoord makes the segments of an import. The streaming node has no segment
  meta for them, so a derived check would say "empty" after an import.

### The index meta

The index reuses `FieldIndex` (`index_coord.proto`). Before this change, the
state of an index was not persisted: `completeIndexInfo` computed it from the
segments at each describe. The primary key index has no build per segment, so
`model.Index` gets `State` and `FailReason`, persisted in the existing
`IndexInfo.state` and `index_state_fail_reason` fields.

| Case | state | deleted |
|---|---|---|
| Every vchannel enabled the index | Finished | false |
| A vchannel did not enable it, the drop did not run yet | Failed, with the reason | false |
| The DropIndex completed (a user drop or the compensation) | no change | true |

The ack callback writes the index once, as for every other index type. Between
the broadcast and the callback, the meta has no index. The create RPC blocks
until the callback is done (`BlockUntilDone`), and the exclusive collection key
blocks another create of the same collection.

## Streaming node

### Append side

The interceptor (`wal/interceptors/pkindex`) handles a CreateIndex of type
`PRIMARY_KEY` on a data vchannel that is not replicated:

1. It reads the vchannel state (write history, `pk_index`, schema) before the
   append. The CreateIndex holds the exclusive lock of the vchannel, so no
   write of the vchannel is appended until the call returns.
2. If the append fails after it got a time tick, the message can be in the
   WAL. The interceptor marks the enable as pending at that time tick.
3. If the write history is NEVER, the interceptor opens the Index and makes the
   vchannel a target. A failed open after a successful append calls `OnFatal`:
   the WAL becomes unavailable, and its reopen enables the index through the
   observe.
4. If the write history is not NEVER, the interceptor marks the enable as
   pending.

The observe can lag the append by about one tick. So the write history that
the append reads can be wrong in two directions:

- It can say NEVER for a vchannel that has a write in the lag. The observe
  decides again at the CreateIndex position and drops the wrong Index.
- It can say UNKNOWN when the observe has not built the vchannel state yet,
  for example right after the CreateCollection. This is why "not NEVER" leaves
  the decision to the observe and does not pass the writes through.

While the enable is pending, a write of the vchannel returns a retriable error
(`errEnablePending`). The pending state ends in one of two ways:

- The observe opens the Index. The next write adopts it.
- The observe passes the time tick of the CreateIndex without an Index. The
  write passes through.

A replicated CreateIndex passes through the interceptor. The observe opens its
Index, and the next write adopts it.

A DropIndex that names the open index, or names no index, and a DropCollection
make the interceptor forget the target after the append. The Index stays open
until the observe reaches the drop, because a write before the drop in WAL
order reaches the observe after the append of the drop. No write adopts that
Index again.

### Observe side

The vchannel module (`wal/vchannel`) keeps `ever_written` and `pk_index` as the
table above says. The primary key index observe
(`pkindex/recovery/observe.go`) runs after it on the same message, so it reads
the state at that message:

- CreateIndex of the primary key index: if `pk_index` names this index at this
  time tick, it opens the Index, or keeps the Index that the append or the
  recovery opened. An Index that the observe opens is ready at once: an index
  enabled after the checkpoint of the pchannel has no manifest. Otherwise it
  drops an Index of this index ID.
- A CreateIndex or DropIndex at or before the enable time tick of the open
  Index is the replay of an earlier index. It does nothing.
- DropIndex that names the index, or names no index: it drops the Index.
- DropCollection: it drops the Index with index ID 0, which matches any Index
  and removes the local directory of the vchannel.

A drop detaches the Index from the open set at once, so a later CreateIndex of
the vchannel can open a new Index. The observe returns the close and the
removal of the local directory to the WAL recovery storage, which runs them
after it releases its lock, because the close waits for a running handover.
The local directory of an Index is `<root>/<vchannel>/<indexID>`, so the close
of an old index never touches the files of a new index.

### Recovery

At the WAL open, the recovery opens an Index only for a vchannel whose meta has
`pk_index`, on the primary key field that `pk_index` names. It does not derive
the index from the schema. See [recovery](recovery.md).

## Coordinator and proxy

- **Proxy.** The `PRIMARY_KEY` checker in `indexparamcheck` accepts only the
  primary key field without autoID, of type Int64 or VarChar, and no index param
  other than the index type and the mmap switch. The proxy rejects the type
  when `streaming.pkindex.enabled` is off, and keeps it out of the scalar
  AUTOINDEX rewrite.
- **DataCoord checks** (`checkPKIndexCreate`), under the collection lock of the
  broadcast:
  - the switch, the field and the external collection: Input errors.
  - the version gate: every streaming node session must be in
    `pkIndexStreamingNodeVersions` (now `>=3.0.0-beta`). Otherwise the create
    returns a retriable ServiceNotReady error.
  - the precheck: no segment that is not dropped (growing and importing
    segments count, row counts do not), and no snapshot restore job that still
    copies segments into the collection. Otherwise an Input error.
- **Create on a Failed index.** A create on the field or the name of a Failed
  primary key index that is not deleted returns a retriable ServiceNotReady
  error. The compensation ends without user action.
- **Callback and result.** See the flow. The RPC reads the state that the
  callback wrote and returns `fail_reason` as an Input error. The decision
  point is the CreateIndex position, so a "not empty" result is not a race: the
  user wrote rows before the create, and a retry fails the same way.
- **Compensation.** The index inspector checks every Failed primary key index
  that is not deleted, on each tick and when the callback notifies it. It
  broadcasts the DropIndex under the collection lock and reads the index again
  under the lock, so a repeated drop does nothing. Only the primary cluster
  drops.
- **"Built per segment".** The primary key index has no build task and no
  index files per segment. The predicate `builtPerSegment` reads the index type.
  The segment index code uses it and does not test for `PRIMARY_KEY` in each
  place: the index inspector creates no segment index, the unindexed and
  indexed segment queries, `HasIndex`, the segment readiness, the clustering
  compaction wait, the copied segment targets and the metrics skip it, and the
  QueryCoord index checker does not wait for a segment to load it.
  `DescribeIndex`, `GetIndexState` and `GetSegmentIndexState` return the
  persisted state and report every row as indexed.

### Error classes

| Where | Condition | Error | Class |
|---|---|---|---|
| Proxy, DataCoord | not the primary key, autoID, mmap, extra param, external collection | `ParameterInvalid` | Input |
| Proxy, DataCoord | `streaming.pkindex.enabled` is false | `ParameterInvalid` | Input |
| DataCoord | a streaming node outside the version range | `ServiceNotReady` | System, retriable |
| DataCoord precheck | a segment that is not dropped, or a running restore | `ParameterInvalid` | Input |
| DataCoord after the callback | a vchannel did not enable the index | `ParameterInvalid` with `fail_reason` | Input |
| DataCoord `CanCreateIndex` | a Failed primary key index is not deleted yet | `ServiceNotReady` | System, retriable |
| StreamingNode interceptor | a write while the enable is pending | `status.NewInner` | streaming, retriable |
| StreamingNode interceptor | the open fails after a successful append | `OnFatal` | the WAL becomes unavailable |

## CDC

The design replicates the create and the drop through their WAL messages:

- The secondary cluster has the same WAL. Its streaming nodes decide at the
  same positions, and its ack callback writes the state from its own acks.
- DataCoord of the primary allocates the index ID. The ID goes to the secondary
  in the `FieldIndex` of the body.
- Only the primary compensates. A secondary does not broadcast
  (`broadcast_manager.go`). Its index inspector logs each Failed index once and
  waits for the replicated DropIndex.

The secondary computes `ever_written` from its own vchannel meta. If the two
clusters have different write histories for a vchannel, they can reach
different states. See [Not done](#not-done).

## Rejected alternatives

| Alternative | Why it was rejected |
|---|---|
| Declare the index in the schema at collection creation | A schema declaration and a later `create_index` would be two permanent entries. A "create collection with the index" path does not exist: every client sends CreateCollection and CreateIndex as two RPCs. It needs new proto fields, a pre-allocated ID, a callback that writes the index meta, and an SDK change. Too large. |
| Two phases: a pending state, writes wait, then a commit or abort message after the callback | This is 2PC. Success takes two messages, plus a pending state and a write wait. Correctness does not need it. |
| Partial success: report a failure and keep the dedup on the empty vchannels | The partial dedup would stay until a backfill exists, and it depends on the hash of each key. A user can not reason about it. |
| Ask the user to drop the failed index | The system made the failure, so the system removes it. |
| Enable the index only in the callback | The writes between the CreateIndex and the callback are not deduplicated. A duplicate from that window stays in the data. A later delete by primary key removes every version of the key before the delete, so it can not repair the duplicate alone. |
| Return the decision in a property of the ack message and keep a per-vchannel state in DataCoord | A property that never goes into the WAL misuses the message. DataCoord would store a second copy of each vchannel decision. |

## Not done

- **End-to-end coverage is partial.** The integration suite
  `tests/integration/pkindex` creates the `PRIMARY_KEY` index on an empty
  collection, deduplicates inserts, commits a handover at flush, and keeps the
  deduplication across a graceful restart of the streaming node. It does not
  cover a kill -9, a measured recovery time, or a real S3 throttle.
- **CDC divergence.** The secondary computes `ever_written` from its own
  vchannel meta. For example, if the streaming nodes of the secondary were
  older when the collection was created there (UNKNOWN), and the primary has
  NEVER, the primary enables the index and the secondary fails it. Only the
  primary compensates, so the secondary keeps a Failed index until the primary
  drops its own index. The version gate checks only the local cluster. Not
  decided: the secondary takes the decision of the primary from the replicated
  message, or the risk is accepted.
- **AlterIndex does not reject params on the primary key index.** An
  `alter_index` that sets or deletes a configurable param (for example
  `mmap.enabled` or a warmup param) on the primary key index is accepted and
  changes the index meta. The proxy accepts only configurable keys, so the
  index type can not change. The new params have no effect on the streaming
  node.
- **The version gate value.** `pkIndexStreamingNodeVersions` is
  `>=3.0.0-beta`. It rejects only the 2.x streaming nodes. A 3.0 streaming node
  without the feature passes the gate, acks with no result, and the create
  fails and is compensated. The lower bound must go up to the first release
  that has the primary key index, when that release gets a version.
- **Backfill on a non-empty collection.** The create fails with "collection is
  not empty". A later phase adds a backfill. Then "not empty" stops being a
  failure reason.
- **DropPartition and TTL.** They remove rows but leave their keys in the
  index. See [lifecycle.md](lifecycle.md#allowed-and-not-handled).

## Test plan

Unit tests cover these behaviors.

Proxy and the checker (`indexparamcheck`, `proxy/ddl`):

- The checker accepts an Int64 or VarChar primary key without autoID, and
  rejects another field, an autoID primary key, `mmap.enabled=true` and an
  extra param. The proxy rejects the type when the switch is off and keeps it
  out of the scalar AUTOINDEX rewrite.

Messages and the broadcaster (`pkg/streaming/util/message`,
`streamingcoord`, `distributed/streaming`):

- CreateIndex and DropIndex are exclusive, and their headers carry the index
  type.
- Each vchannel keeps its own extra. The extra survives a coordinator recovery
  and reaches the ack callback. FastAck carries no extra. The client and the
  service pass the extra.

Streaming node (`wal/vchannel`, `wal/recovery`, `wal/interceptors/pkindex`,
`pkindex`, `pkindex/recovery`):

- The vchannel state follows the table of the vchannel meta. An UNKNOWN history
  never enables the index. A persisted CreateIndex is not decided again at a
  replay, and a write before a persisted truncate is skipped. The state getter
  returns a copy.
- The ack result is ENABLED on an empty vchannel, NOT_EMPTY after a write or
  when another CreateIndex enabled the index, DISABLED when the switch is off,
  and absent for other messages. The broadcast ack module sends the same extra
  on a retry.
- The interceptor enables the index on an empty vchannel. On a written or
  UNKNOWN vchannel the writes wait, and adopt the Index that the observe opens
  or pass through after the observe passes the time tick. An append with an
  unknown result works the same way. An append failure without a time tick
  leaves no pending state. A failed open marks the WAL unavailable. Other index
  types, the control channel and a replicated message pass through. A DropIndex
  forgets the target. A CreateIndex after a DropIndex builds a new Index.
- The observe opens an Index only when the vchannel state names it, drops the
  Index of the append on a vchannel that is not empty, closes only the named
  index at a DropIndex, and keeps a later index at the replay of an earlier one.
  The close of a dropped index does not close the Index of a later CreateIndex.
  The recovery opens only the vchannels with `pk_index`.
- Two indexes of one vchannel use separate directories. `Apply` skips the
  writes before the enable time tick. An open of another index ID fails.

DataCoord (`internal/datacoord`):

- Each rejection of the create: the switch, an old streaming node, another
  field, an external collection, a collection that is not empty, a running
  restore.
- An empty collection gives a Finished index and a broadcast to every
  vchannel. A vchannel that is not empty gives a Failed index, the reason in
  the RPC, and a notice to the compensation. A retry while the index is Failed
  returns ServiceNotReady. A node in the version range that acks with no result
  gives a Failed index and a notice to the compensation.
- Another index type goes to the control channel only.
- The compensation drops a Failed index on the primary, waits on a secondary,
  and keeps a Finished index. A user drop of the primary key index, by name or
  by a drop of all indexes, goes to every vchannel.
- The segment index code skips the primary key index, and `DescribeIndex`
  reports its persisted state. The QueryCoord index checker skips it.

The integration suite `tests/integration/pkindex` runs the CreateIndex on an
empty collection end to end. Not run: a CreateIndex on a non-empty
collection, a DropIndex, and a truncate on a cluster.
