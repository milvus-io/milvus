# MEP: Primary key index recovery at the WAL open

- **Created:** 2026-10-16
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** StreamingNode, `internal/streamingnode/server/pkindex/recovery`
- **Related Issues:** #52305
- **Related work:** [handover](snapshot.md), [write path](write-path.md), [core library](core-library.md), [manifest service](manifest.md)

## Summary

When a WAL opens, its recovery storage recovers the primary key index of
the pchannel. It opens the index of every indexed collection and reads the
manifests of the vchannels. The index is a resident
module of the recovery storage: the observe of the storage applies every
persisted WAL message to the index, from the checkpoint of the pchannel to
the recovery barrier and then live, through one code path. The interceptor
gets the index from the recovery snapshot and decides the writes; it does
not write the engine. The committed table sets are installed in the
background while the index serves writes degraded. The oldest write that an index did not commit holds the
WAL checkpoint back, so WAL truncation never drops a write the index does
not have. Two handover triggers keep that hold bounded: a stalled checkpoint
and an explicit flush.

## Motivation

The WAL recovery replays one stream from the global checkpoint of the
pchannel to the recovery barrier and continues the same stream live. Each
component skips the messages that it persisted already and applies the
rest. The index of a vchannel knows its own checkpoint from its manifest
(`checkpoint_time_tick`, see [Coverage](snapshot.md#coverage)). Three gaps
stay without this change:

- Without the replay, the index misses the writes between its manifest
  checkpoint and the barrier. Those writes were in the engine before the
  restart, and the open removes the engine.
- Without a link from the index to the WAL checkpoint, the WAL could drop
  writes that the index did not commit. A later replay then can not bring
  them back.
- With the interceptor as the writer of the engine, the replay and the live
  path are two implementations of the same apply, and a replicated message is
  never applied, so the index of a secondary cluster is empty. The
  [handover](snapshot.md) change moved the engine write to an observe that
  did not exist yet; this change adds it.

One pchannel after a restart:

```
global checkpoint of the pchannel     80
recovery barrier                     150
vchannel A: manifest checkpoint      100   committed tables cover tt <= 100
vchannel B: no manifest                    a collection that never handed over

the observe delivers tt 80..150, then the live messages
  A skips tt <= 100, applies (100, 150] and on      the committed set covers the rest
  B applies (80, 150] and on                        its engine starts empty
```

Without the replay, A misses (100, 150] and B misses (80, 150]. Without the
truncation link, the checkpoint of the pchannel could have moved to 140
before the restart while A was still at 100. The replay would then start at
140, and the writes of A in (100, 140] would be lost from the index.

## Where the recovery runs

The WAL open of a pchannel (`openRWWAL` in `wal/adaptor/opener.go`) runs the
steps below in this order:

```
openRWWAL
  buildInterceptorParams        appends the RecoveryBarrier, keeps it as LastTimeTickMessage
  RecoverRecoveryStorage
    recoverRecoveryInfoFromMeta reads the checkpoint, the VChannelMetas and the SegmentMetas
      initRecoveryModules       pkrecovery.Recover: opens the indexes, reads the manifests
    runBoundedRecovery          replays the stream from the checkpoint
      observeModulesMessage     Recovery.ObserveMessage for every persisted message
      at the barrier            Recovery.Run: submits the installs, starts the handover driver
    buildInitialRecoverySnapshot RecoverySnapshot.PKIndex = the index of the pchannel
  adaptImplsToRWWAL             builds the interceptors; the pkindex interceptor reads the snapshot
  close(liveReady)              the same scanner continues live: observeMessage, Recovery.ObserveMessage

background
  consumeDirtySnapshot          summaryThrough = min(summary.LastAcked, Recovery.LastAcked)
  handover driver               the handovers of snapshot.md, plus triggers 4 and 5
  install task per Index        reconcile committed/, download, install, MarkReady
  closeRecoveryResources        Recovery.Close, on every exit of the storage
```

Ownership:

- The recovery storage (`recoveryStorageImpl`) owns the `Recovery`, like
  the WAL summary and the vchannel modules. It creates it in
  `initRecoveryModules`, feeds it every persisted message in
  `observeModulesMessage` (the bounded replay and the live scanner call the
  same function), calls `Run` at the barrier in `runBoundedRecovery`, reads
  its `LastAcked` in `consumeDirtySnapshot`, and closes it in
  `closeRecoveryResources`. Every exit path of the storage runs
  `closeRecoveryResources`: a failed open, a cancelled open, the AlterWAL
  path and a normal close.
- The recovery snapshot carries the index. `buildInitialRecoverySnapshot`
  sets `RecoverySnapshot.PKIndex`, as it sets the idempotency windows, and
  the pkindex interceptor reads it in `Build`. A snapshot without an index
  gives the passthrough interceptor. `Close` of the interceptor drops the
  reference only.
- The `Manager` is the node-level registry of the open pchannels. `New`
  registers a `PChannelIndexManager` without its driver, and
  `PChannelIndexManager.Close` removes it. Nothing looks an index up by
  pchannel name.

Lock order: the recovery storage holds `r.mu` when it calls `ObserveMessage`
and `LastAcked`, and those take the locks of the `PChannelIndexManager`, of
the `Index` and of the `Authority`. So the order is `r.mu`, then the index
locks. The WAL tail goes to the index with each `ObserveTimeTick`: the
observe reads it from the tail controller and the ack tracker, which have
their own locks, while it holds `r.mu`, and never from `Metrics`. No code
of the index calls back into the storage.

## Package layout

```
wal/recovery  ──▶  pkindex/recovery  ──▶  pkindex (root), pkindex/index  ──▶  pkindex/authority
                   (Recovery, Param,      (Manager, PChannelIndexManager,      (Applied, RestorePending,
                    install task)          ManifestReader; Index, Write)        WantsTimeTick)
```

- `internal/streamingnode/server/pkindex/recovery` is the pchannel level of
  the recovery: `Recover`, `ObserveMessage`, `Run`, `Close`, `LastAcked`,
  `Manager`, the `Param` struct and the install task. It uses only the
  exported API of the root package and of `index`. It translates WAL
  messages into `index.Write` values, so `index` reads no messages.
- The index level lives in `index/recovery.go`: `SetCommitted` records the
  manifest view of the vchannel, `Install` brings the committed set into the
  engine. These change the private state of the `Index` (the engine,
  `lastCommittedTimeTick`, the installed version, the readiness), so they
  are methods of it. `Index.Apply`, which writes one `Write` to the engine,
  and the observed time tick of the pchannel (`ObserveTimeTick`,
  `LastObserved`) are part of the [handover](snapshot.md) change.
- The root package adds `ManifestReader.ReadManifest`, `PKsOf` (the conversion of
  `primarykey.Keys` to `authority.PK`, shared with the interceptor), the
  accessors the recovery needs (`Manager.Committer`, `Scheduler`, `Params`,
  `Root`, `PChannelIndexManager.Info`, `Closed`), and a `LastAcked` that
  stays out of the truncation on a node without a `Committer`. `Manager.Get`
  goes away: nothing looks an index up by pchannel name.
- The authority gets the entries of the in-flight set of the write path
  that the recovery calls: `Applied` retires the keys of an applied write,
  `RestorePending` records a body of an open transaction, and
  `WantsTimeTick` says whether the vchannel needs the TimeTick (it is
  degraded until a tick, or holds a write with an unknown outcome).

The `Recovery` lives as long as the WAL. It holds the targets by vchannel
(the `Index`, the primary key field ID and its data type), the indexes with
a manifest that wait for their install, the first error of the replay
before `Run`, the `finished`, `stopped` and `closed` flags, the vchannels to
wipe at the close, and the handles of the submitted install tasks. `Run` is
called once; a second call does nothing. `Close` releases everything that
`Recover` opened and is idempotent.

## Recovery of one pchannel

| Step | Where | What | Sync | Fails the open | Retries until the open ends |
|---|---|---|---|---|---|
| R1 open the indexes | `Recover` | `Manager.New` registers the `PChannelIndexManager` without its driver, with the WAL checkpoint as the first observed time tick. For every `VChannelMeta` in state NORMAL whose primary key is not autoID and is Int64 or VarChar, `CreateCollection` opens the `Index`, one vchannel after the other. The last schema of the meta names the primary key field. | Sync | A stopped `Manager`, an unsupported primary key type, a local disk error | — |
| R2 manifests | `Recover` | `GetRecoveryManifests` in batches of `MaxRecoveryManifests` (10000) refs. `ReadManifest` of every found body, at most `ManifestReadConcurrency` at a time. `SetCommitted(view)` on each `Index` with a manifest. `MarkReady` on each `Index` without one. The pchannel is not claimed: the manifest service fences a flush through the pchannel meta of the streaming coordinator. | Sync | `ErrRejected`, `pkindex.ErrCorrupted`, a wrong answer count, a body whose version differs from the pointer | Every other error of the lookup and the body read |
| R3 observe | `ObserveMessage` | See [Observe](#observe). Before and after the barrier. | Sync, on the observe of the storage | The first error before `Run`, returned by `Run` | — |
| R4 install | `Run` | One install task per `Index` with a manifest on the node scheduler. Then `Start` of the handover driver. | Async | A missing or corrupted object, through `OnFatal` | Every other error, with backoff, until the task is cancelled or the `Index` closes |
| R5 the interceptor | `Build` | Reads `RecoverySnapshot.PKIndex`. Rebuilds the pending writes of the open transactions from `RecoverySnapshot.TxnBuffer`. | — | A body of an open transaction that can not be read, through `OnFatal` | — |

R2 is synchronous. The observe skips by the checkpoint of each vchannel, so
the checkpoints must be known before the first observed message, and a
handover can follow the barrier at once.
With `MaxRecoveryManifests` refs per lookup, the part of the open that grows
with the number of vchannels is the body reads. The body reads run in
parallel, and the first failed read cancels the others. A fatal error of any
read wins over the cancel error of a sibling.

Without a `Committer`, R2 looks nothing up and marks nothing ready. Nothing
is ever committed, so no `Index` holds the WAL checkpoint back
([Truncation](#truncation)), and the writes before the checkpoint are gone
at a reopen: the engine starts empty and the replay only brings back the
writes above the checkpoint. So every vchannel that exists at the open stays
degraded for the life of the WAL: every key counts as present, and every
insert gets a companion delete. Only a collection created later, in this WAL
lifetime, is ready: its writes are all above the checkpoint. The same holds
for a collection that existed before the index was turned on. The index
without a `Committer` deduplicates by a cheaper probe only for those
collections.

R5: a transaction that was open at the barrier can still commit after the
open. Its bodies went through the index before the restart. The interceptor
reads them from the transaction buffer of the snapshot (an independent copy
of the open transactions at the barrier), extracts their keys and records
them with `RestorePending`, so the commit decides its companion deletes over
all of the bodies. The bodies are in the WAL already: they need no
in-flight entry, and the observe applies them with the commit.

## Readiness

[Readiness](snapshot.md#readiness) defines ready: the committed table set of
the vchannel is installed. Who calls `MarkReady`:

- `Recover`, right after the lookup, for an `Index` without a manifest.
- The install task, at the end of a successful `Install`, for an `Index`
  with a manifest.
- `ObserveMessage`, for a collection that an observed CreateCollection
  creates. A collection created in this term has no manifest. A
  CreateCollection of a vchannel whose index is open already does nothing,
  so the interceptor may open the index first and the observe finds it.
  The enabling message of the index is a CreateIndex of type PRIMARY_KEY,
  opened after a successful append; that message carries no index type
  yet and goes to the control channel only, so the index is opened at the
  CreateCollection until then (see [Not done](#not-done-in-this-change)).
- `CreateCollection` after `Start`, for a new collection, as before.

Until then the `Index` serves writes degraded: `Authority` does not probe
the engine and treats every key as present. The handover driver starts only
at `Run`, so no handover runs during the replay, and an `Index` that is
not ready is never a handover candidate.

## Observe

`ObserveMessage` receives the messages that the recovery storage delivers:
a transaction as one assembled message, an autocommit write, a DDL message,
a persisted TimeTick, in time tick order, before and after the barrier. The
storage holds its lock and calls the modules on one goroutine, so the apply
is serial. What each message becomes:

| Message | Action |
|---|---|
| TimeTick, RecoveryBarrier | `ObserveTimeTick(tt, tail)` moves the observed time tick of the pchannel to tt and evaluates the handover triggers with the WAL tail of the storage ([Coverage](snapshot.md#coverage), [Triggers](snapshot.md#triggers)). The writes before the tick are applied by then, on the same goroutine. Then the `Authority` of every vchannel that wants the tick (`WantsTimeTick`: degraded until a tick, or holding a write with an unknown outcome) gets `ObserveTimeTick(tt)`; an idle vchannel does not, so a tick does not fan out to every vchannel. |
| ManualFlush, FlushAll | `RequestHandover(vchannel, tt)`, `RequestHandoverAll(tt)` (trigger 5 of the handover change). |
| CreateCollection (not the control channel) | `CreateCollection` with the schema of the body and `MarkReady`. A vchannel that is a target already is ignored. |
| DropCollection (not the control channel) | Forgets the vchannel and closes its index in a task on the node scheduler, outside the lock of the storage, because the close waits for a running handover. The interceptor only forgets the vchannel at the append of the drop: a write before the drop in WAL order reaches the observe after that append returned and must still find the engine open. A failed drop is logged and does not fail the open. |
| Insert (autocommit) | `KeysOfInsert(body, pkFieldID, pkDataType)`, then `Write{Puts: {key, segment}..., TimeTick}`. The segment is `Partitions[0].SegmentAssignment.SegmentId`, as on the append path. |
| Delete | `KeysOfDelete(body)`, then `Write{Deletes, TimeTick}`. A delete with an expression plan panics, as on the append path, which does not accept it. |
| Transaction | One `Write` with the deletes of every body and the puts of every body, at the commit time tick. A companion delete of the append path is such a transaction. |
| A message of the control channel | Skipped. |
| A message with a replicate header | Applied like any other. The index of a secondary cluster follows its WAL. |
| Every other type | Ignored. |

The observe applies through `PChannelIndexManager.Apply`, never through
the `Index` alone, so the dirty tracking below sees every write.
`Index.Apply` skips a write whose `TimeTick` is at or below
`LastCommittedTimeTick`: the committed tables hold it already. A transaction
carries the commit time tick on every body, so it is skipped or applied as
a whole. `Apply` does not decide. It writes the whole `Write` in one engine
batch with the deletes before the puts, so a put of the same key at the
same time tick survives: the body order of a transaction that the index
built for an insert hit is the insert and then its companion delete, and
the insert must win. A delete
writes a tombstone for every key, whether the index holds it or not. This
is what keeps a deleted record of the committed tables deleted: before the
install the engine has no committed layer, so a probe would miss every key,
and the install would bring the deleted records back. The tombstone of a
key the index never held costs space until a compaction drops it.

After the batch, `Apply` calls `Authority.Applied(keys, tt)` once, which
raises the observed time tick of the vchannel and retires the in-flight
entries of the keys at or below tt. The engine write comes first and the
whole transaction is one batch, so no entry of a transaction leaves the set
before every key of the transaction is in the engine, and a writer that
misses a key in the in-flight set finds it in the engine
([write path](write-path.md)). The interceptor does not write the engine:
the observe is the only writer, for the replay and the live messages alike.

`PChannelIndexManager.Apply` adds the `Index` to the `LastAcked` heap, with
its manifest checkpoint as the key, when the write made it dirty.

## Warm start

`index.Open` removes the engine directory from before this open and keeps
`committed/`. The generations in the removed engine directory could hold
three kinds of writes: writes at or below the manifest checkpoint, which the
committed tables hold already; writes above the global checkpoint, which the
observe writes again; and writes between the two, which do not exist because
the truncation never passes `LastAcked`. So nothing in the engine directory
is needed, and the engine starts empty. Without a `Committer` the second kind
does exist, and the vchannel stays degraded for that reason.

`Install` runs after the barrier, as a task on the node scheduler:

1. `reconcileCommittedDir` removes every file in `committed/` that the
   manifest does not list. That covers the tables that a compaction
   replaced and the temporary files of downloads that did not finish.
2. `installIfNewer` resolves a local file for each table. `resolveLocal`
   reuses `committed/<id>.sst` when its size equals the `Size` of the
   manifest, removes it and downloads again when the size differs, and
   downloads when it is missing.
3. `MarkReady`.

Every step is idempotent, so the task retries `Install` with backoff on a
transient error (`BackoffBase` to `MaxBackoff`). A missing or corrupted
object is fatal: the task calls `OnFatal`, and the WAL becomes unavailable.
The task ends without an error when its context ends or the `Index` closes.

The warm start saves the download of the committed tables. The replay is
bounded by the WAL tail, so its cost stays small next to the committed set.

After a corruption error of the observe, the local data of the vchannel is
not trusted: `Close` removes its directory, engine and `committed/`, and the
next open downloads the committed set from the manifest.

## Truncation

`consumeDirtySnapshot` computes the point through which the WAL checkpoint
may move:

```
summaryThrough := min(summaryManager.LastAcked(), pkindex.LastAcked())
completedPoint := ackTracker.CheckpointThrough(summaryThrough)
```

[LastAcked](snapshot.md#lastacked) is the minimum of the observed time tick
of the pchannel and the `lastCommittedTimeTick` of every dirty `Index`. An
idle vchannel does not hold it back. On a node without a `Committer`,
`LastAcked` returns `MaxUint64` before it reads the heap: nothing ever
commits, so the index never holds the truncation back.

The observed time tick ([lastObserved](snapshot.md#lastobserved)) is moved
by the observe alone. The observe applies the writes in time tick order on one goroutine
and moves the tick on a persisted TimeTick, after the writes before the tick
are in the engine. So every write with a time tick at or below the observed
tick is in the engine, with no count of writers in flight. A handover reads
the tick before it rotates as the time tick it covers; a write above it that
lands in the frozen generation is harmless, because the replay after a
restart applies it again and the apply is idempotent.

`LastAcked` never returns less than it returned before (`ackedMax`). The
clamp never reports too much. Every value returned before is at most the
observed tick at that time. When the observed tick is V, every write with a
time tick at or below V is applied, and the apply adds its `Index` to the
heap. So every such write either is in the heap already or is committed,
and every write applied later has a time tick above V. An `Index` that joins
the heap with a key below `ackedMax` therefore has only writes above
`ackedMax`.

The WAL checkpoint is more than the truncation point. `PChannelCheckpointUpdater` reports it to DataCoord as the channel
checkpoint of every vchannel, and the flushing stage of an AlterWAL waits
until it reaches the time tick of the AlterWAL. A flush of a collection
returns on the acknowledgement of its ManualFlush broadcast and does not
wait for the checkpoint. An `Index` with uncommitted writes holds all of
these back, and the size triggers of [Triggers](snapshot.md#triggers) do not
fire on a small tail. So the index must hand over in bounded time.

## Triggers

Triggers 4 (stalled checkpoint) and 5 (explicit flush) of
[Triggers](snapshot.md#triggers) exist for the hold above. They are defined
and evaluated in the handover change; this change delivers their inputs:

- The tail of the WAL comes with every `ObserveTimeTick`: the recovery
  tail bytes of the tail controller and the completed point of the ack
  tracker. The index holds the checkpoint back while its `LastAcked`
  without the clamp is below that completed point, and trigger 4 measures
  the hold in observed time, between the ticks the observe delivers.
- Trigger 5: when the observe sees a ManualFlush, it calls
  `RequestHandover(vchannel, tt)` with the time tick of the message; for a
  FlushAll, `RequestHandoverAll(tt)`. The request sits at the position of
  the flush in the WAL, and the driver picks the `Index` once the observed
  time tick reached it, so the handover covers the flush.

## Failure handling

| Case | Handling |
|---|---|
| A local disk error when an `Index` opens in R1 | The open fails with `pkindex.ErrIO`. The WAL is assigned again. |
| An unsupported primary key type in a `VChannelMeta` or an observed CreateCollection | The open fails. |
| A stopped `Manager` | `Recover` returns an error marked `pkindex.ErrUnavailable`. The open fails. |
| `ErrRejected` from the lookup | The open fails. A lookup reads no term, so it is never fenced: only a flush is. |
| A transient error of the lookup or a manifest body read | `retry.Do` without an attempt limit, `BackoffBase` to `MaxBackoff`, until the context of the open ends. |
| A manifest body whose version differs from the pointer, an answer count that differs from the ref count | `ErrRejected`. The open fails. |
| A manifest body that does not exist or does not decode | `pkindex.ErrCorrupted` from `ReadManifest`. The open fails. |
| An observe error before `Run` | The first error is kept. `Run` returns it, and the open fails. |
| A closed index or a canceled context in the observe after `Run` | The WAL is closing. The observe stops: it applies nothing more and does not move the observed time tick, so no later handover covers the write it skipped. A dropped collection does not reach this case: the observe skips its writes after the drop and closes its index itself. |
| Every other engine error in the observe after `Run` | The observe stops and calls `OnFatal`, which marks the WAL unavailable; the WAL reopens from the manifest and the WAL. The log line names the pchannel, the vchannel and the error class, and `pkindex_fatal_total` counts it by class. A corruption error also marks the vchannel for the cold start of [Warm start](#warm-start). The alternative, a vchannel that serves degraded forever, was rejected: its uncommitted writes are gone, so it would have to hold `LastAcked` forever and the truncation of the pchannel with it. |
| A persisted body that does not unmarshal, or whose primary keys can not be read | Panic, the rule of every module of the recovery storage, which read a persisted body with `MustBody`. The append path rejects such a body and reads the same keys before it is written, so a persisted one is storage corruption, and a reopened WAL would read it again. |
| A transient body decode error in the observe (a cipher whose key service is away) | Retry with backoff on the observe goroutine, under the lock of the storage, until the context ends. The body cache does not retry: a failed decode is not cached and every call decodes again. The whole pchannel waits with it. |
| A transient install error | Retry with backoff. The `Index` stays degraded. The first failure logs at warn level, the next ones at debug level. |
| A missing or corrupted object in the install | `OnFatal` through the fatal handler of the storage, which marks the WAL unavailable, and one count in `pkindex_fatal_total` by class. The handler ignores the error once the storage closes. |
| A body of an open transaction that can not be read at `Build` | `OnFatal`: the commit of the transaction would be decided without its bodies. |
| The open is cancelled during the replay, or the opener fails after `Run` (the replicate manager, the AlterWAL path) | `closeRecoveryResources` calls `Recovery.Close`: it cancels the install tasks, waits for them and closes the index. |

## Configuration

| Key | Default | Refreshable | Meaning |
|---|---|---|---|
| `streaming.pkindex.recovery.manifestReadConcurrency` | `64` | No; the value is read at each WAL open | The number of manifest bodies that the recovery reads in parallel. |
| `dataNode.segment.syncPeriod` | `600` (s) | Yes, at the next observed TimeTick | Existing key. The stall timeout of trigger 4 of the handover change, the same value as the stall timeout of the WAL ack tracker. The observe of this change delivers the ticks it is measured in. |

The retry of the lookup, of the install and of a body decode reads
`handover.maxBackoff` as in [Configuration](snapshot.md#configuration).

The engine write runs on the observe goroutine of the storage. A stall of
the memtable (Pebble stops writers when the queued memtables reach
`MemTableStopWritesThreshold × MemTableSize`) stalls every module of the
pchannel. A burst replay of 512 MiB into one vchannel on a local NVMe
stalled with the default `streaming.pkindex.memTableSize` of 4 MiB (up to
182 ms per write) and did not stall from 16 MiB on. The default is not
changed in this change; the measurement is one vchannel on one disk.

## Comparison with similar systems

| System | Replay start and skip rule | Serves during the replay | Local file reuse | What bounds the log truncation | Time or stall trigger |
|---|---|---|---|---|---|
| Pebble v1.1.5 (`open.go`, `compaction.go`) | Replays only the WALs with `FileNum >= minUnflushedLogNum` into the memtable, flushes them to L0 inside `Open` and moves `MinUnflushedLogNum` to a new log | No: the replay completes inside `Open` under `d.mu` | Existing SSTs stay in place; `checkConsistency` checks the file sizes against the MANIFEST | After a flush, `MinUnflushedLogNum` is the log of the oldest unflushed memtable, then `deleteObsoleteFiles` | `maybeScheduleDelayedFlush`: a timer marks the memtable `flushForced` |
| RocksDB (`db_impl_open.cc`) | `min_wal_number = max(MinLogNumberToKeep, MinLogNumberWithUnflushedData)`; `WriteBatch::InsertInto` filters each record by the `log_number` of its column family | No: `Recover` holds `mutex_`, one thread until the client has the handle | SSTs stay in place | With `avoid_flush_during_recovery`, the replayed memtables are not flushed and the WALs wait in `alive_wal_files_` for `FindObsoleteFiles` | `max_total_wal_size` forces a flush of the column family that holds the oldest WAL (not verified against the source) |
| SlateDB (`db.rs`, `wal_replay.rs`) | Replays the WAL SSTs after `replay_after_wal_id` of the manifest, skips rows with `seq <= last_l0_seq`; the result is a memtable, no L0 is written during the replay | No: `replay_wal` is an async step that `open` waits for (call site in `builder.rs`, not verified against the source) | `preload_disk_cache_on_startup` warms the local cache from the manifest; `CachedObjectStore` reads through | WAL GC reads `replay_after_wal_id` of the manifest; a `WalTruncated` during the replay refreshes the manifest, a larger `writer_epoch` is `Fenced` | — |
| Neon (RFC 025) | The pageserver finds `index_part.json` by generation and loads only indexes at or below its own generation | Reads download layers on demand (not verified against the source) | Local content is removed at a new attach; the RFC proposes secondary locations that keep local data for a faster failover | `remote_consistent_lsn` moves only after the index upload with a verified generation; the safekeeper drops WAL by it | — |
| TiKV raftstore (`peer_storage.rs`) | Applies the raft log after `apply_state.applied_index` of the KV engine; `truncated_state` is the truncation point | No: no service during `SnapState::Applying` | The KV engine stays in place | `clear_meta_in_kv_and_raft` syncs the KV write before it clears the raft log; a comment says the raft log GC needs a flush first | `raft_log_gc` by size and count (not verified against the source) |
| CockroachDB (`raft_log_queue.go`) | — | — | — | A truncated entry must be "already durably applied to the state machine"; with loose coupling it waits for a durable `RaftAppliedIndex` | `RaftLogQueueStaleThreshold` (100 entries) and `RaftLogQueueStaleSize` (64 KiB) allow a truncation; a slow follower forces one by size |
| Milvus WAL recovery framework (`wal/recovery`) | One scanner replays from the global checkpoint to the barrier and continues live; each component skips by its own `checkpoint_time_tick` | No: no append before the barrier | The segment views come from the catalog | `CheckpointThrough(min(summary.LastAcked))` | The ack tracker sends a persist request to a stalled vchannel after `ackStallTimeout` (`dataNode.segment.syncPeriod`); an explicit flush persists the segments at once |

The apply on one goroutine, which this change adds, compared the same way:

| System | Apply goroutine and batch | Where the applied position lives | Engine write failure | Back pressure |
|---|---|---|---|---|
| TiKV raftstore (`components/raftstore/src/store/fsm/apply.rs`, v8.5.0) | An apply worker applies the entries of a region in order into one write batch (`handle_put`, `handle_delete`), committed by `ApplyContext::commit` and `finish_for` | `apply_state` goes into the same write batch as the data | `write_to_db` panics; a comment says that for local errors "just panic is ok" | `apply_yield_write_size` and `apply_yield_duration` yield the worker |
| CockroachDB (`pkg/kv/kvserver/replica_app_batch.go`, v24.3.0) | `Stage` under `raftMu` into one `replicaAppBatch`, one commit in `ApplyToStateMachine`, no sync | `RaftAppliedIndex` goes into the same batch: if the batch is not durable, the entries are applied again at startup | The commit error is wrapped and returned; the caller ends the replica | None at the apply |
| FoundationDB storage server (`fdbserver/storageserver.actor.cpp`, 7.3.43) | One `update()` actor applies the mutations in version order to `versionedData`, then sets `data->version` | `updateStorage()` makes versions durable later and forgets the old ones with `oldestVersion` | `update()` logs `SSUpdateError` and rethrows; a rollback throws `please_reboot` | Above `STORAGE_HARD_LIMIT_BYTES` the actor sets `behind` and waits for durability: "we need to block updates or we could run out of memory" |
| Pebble (v1.1.5) | The writer commits its own batch; a full memtable rotates | — | — | `makeRoomForWrite` blocks the writer when the queued memtables reach `MemTableStopWritesThreshold × MemTableSize` |

What the index follows:

- Every system replays inside the open and serves nothing until the replay
  is done. The index replays inside `runBoundedRecovery`, so the index is at
  the barrier when the first append arrives.
- Every system applies on one goroutine in log order. The index applies in
  the observe of the storage, and the observed time tick is the applied
  position. TiKV and CockroachDB put the applied position into the same
  batch as the data; the position of the index is the manifest checkpoint
  on the coordinator, so the index holds the truncation at `LastAcked` and
  applies an uncommitted write again after a restart instead.
- The replayed writes stay in memory, nothing flushes them early (RocksDB
  `avoid_flush_during_recovery`, SlateDB). The observe writes into the
  active generation, and the normal handover publishes it.
- Each component skips by its own durable position (RocksDB by the
  `log_number` of the column family, SlateDB by `last_l0_seq`). The index
  skips by the `checkpoint_time_tick` of the manifest of the vchannel.
- Local files are reused in place and checked against the metadata (Pebble
  `checkConsistency` checks the sizes). The index reuses `committed/` and
  checks the `Size` of the manifest.
- The truncation waits for the durable progress of the state machine. The
  index gives `LastAcked`.
- Every system bounds how long the log can wait (the forced flush timer of
  Pebble, the 100 entries or 64 KiB of CockroachDB, the stall request and
  the explicit flush of the WAL framework). The index adds triggers 4 and 5.
- An engine failure restarts the instance (TiKV, FoundationDB). The index
  makes the WAL unavailable through `OnFatal` instead of ending the
  process, so the other pchannels of the node keep serving.

## Not done in this change

- **The adapter over the manifest service.** It implements
  `ManifestReader.ReadManifest` with the manifest proto and the `Committer`. Until
  it is injected, the recovery runs without a `Committer`: no lookup, every
  `Index` that exists at the open stays degraded, the WAL checkpoint does not
  wait for the index (see [Recovery of one pchannel](#recovery-of-one-pchannel)).
- **The enabling by a `CreateIndex` of type `PRIMARY_KEY`.** The index of a
  collection is still opened at its `CreateCollection`. The enabling message
  has to be exclusive on the data vchannels (`ExclusiveRequired` of the
  message type, and a broadcast to the vchannels of the collection), so that
  no write runs between the append of the message and the open of the index.
- **The degraded write while a vchannel waits for an event the index can not
  see in its in-flight set**, the promotion of a secondary cluster and the
  recovery from an engine failure.
- **The default of `streaming.pkindex.memTableSize`.**
- **A metric of the degraded vchannels**, and the log lines at the entry
  and the exit of the degraded state with the wait time.
- **End-to-end tests with a restart.** A cluster with the index on, a kill
  of the streaming node, duplicate inserts after the restart, a flush that
  returns, and the recovery time by the number of vchannels and by the table
  size.
- **Lazy recovery.** A read-through of the committed tables from object
  storage instead of the download before ready.
- **A `checkpoint_time_tick` in the manifest pointer.** It would let R2 skip
  the body reads and move them into the install.
- **The view refresh after a compaction.**
- **A flush request is lost at a restart.** The replay does not re-create
  the request of a ManualFlush, FlushAll or AlterWAL that was appended
  before the crash; after a restart the checkpoint waits for the stall
  timeout or a size trigger. The flushing stage of AlterWAL polls the
  checkpoint, so it waits the same way.

## Test plan

Unit tests cover these behaviors.

`pkindex/recovery`:

- `Recover` opens the index of every indexed vchannel, marks an `Index`
  without a manifest ready and leaves one with a manifest not ready with its
  checkpoint set. A dropped vchannel in the catalog is skipped. An
  unsupported primary key type fails the open.
- A fenced lookup fails the open and leaves nothing open. A transient lookup
  failure is retried. A stopped `Manager` fails the open. Without a
  `Committer`, an `Index` that exists at the open stays degraded, a
  collection created after the open is ready, and a key written before a
  reopen gets a companion delete when it is inserted again.
- The manifests are looked up in batches. A body whose version differs
  from the pointer is rejected. A fatal body read cancels the other reads and
  wins over their cancel error.
- `Run` starts the driver and submits the installs. `Close` cancels
  them and removes the pchannel from the `Manager`.
- The observe of inserts, deletes, transactions and companion deletes gives
  the same engine content as the append path, for Int64 and VarChar keys.
  Writes that the manifest covers are skipped. The control channel is
  skipped and a replicated message is applied. An observed CreateCollection
  creates a ready `Index`, one of an opened vchannel is ignored, and a
  DropCollection closes it from a task of the observe, after the writes
  before the drop, while the observe goes on. The first error before `Run`
  is kept and `Run` returns it.
- The replay and the live path are one: a message after `Run` is applied by
  the same observe. The delete of a transaction is applied before its put,
  each insert keeps its segment, and a transaction is one engine batch whose
  in-flight entries leave the set only after the batch. The in-flight
  entries of a write leave the set after its engine write, and only those at
  or below the applied time tick. A write is in the engine before the
  TimeTick after it moves the observed time tick, a write alone does not
  move it, and a handover covers the tick it read before the rotate. A
  delete writes a tombstone for a key the index does not hold. A ManualFlush
  requests a handover of its vchannel, a FlushAll of every vchannel. A
  TimeTick reaches the `Authority` of a degraded vchannel and of one with
  an uncertain write, not an idle one.
- A closed index after `Run` stops the observe without a fatal and keeps the
  observed time tick. An engine error after `Run` goes to `OnFatal` with its
  class, counts in the metric, and stops the observe. A corruption error
  makes `Close` remove the local directory of the vchannel. A persisted body
  that does not unmarshal panics.
- The install task retries a failed download and marks the `Index` ready,
  reports a missing object through `OnFatal`, and stops when the `Recovery`
  closes and when the `Index` closes.

`pkindex` (index, authority and driver):

- An open removes the engine directory, also when only it has files, and
  keeps `committed/`. `resolveLocal` reuses a table with the right size and
  downloads one with the wrong size again.
- `Apply` skips the writes the manifest covers, writes the deletes before
  the puts, writes a tombstone for an unknown key, and a delete before the
  install writes a tombstone for every key.
- `Install` removes the files the manifest does not list, only marks ready
  without a manifest, and retries after a failed download.
- `Applied` removes the entries of the keys at or below the time tick and
  no others. `RestorePending` feeds the commit decision and adds no in-flight
  entry. `WantsTimeTick` is true while the vchannel is degraded until a tick
  or holds a write with an unknown outcome, and false otherwise.
- `LastAcked` without a `Committer` stays out of the truncation. `PKsOf`
  keeps the order and the type of the keys. The tests of `ObserveTimeTick`,
  `RequestHandover` and the stall trigger are in the handover change.

`wal/recovery`:

- `pkindexTailLocked` reads the tail controller and the ack tracker. The
  `LastAcked` of the index clamps the checkpoint. The index is closed when the open fails. A `Run`
  error fails the open. The recovery snapshot carries the index. The
  messages after the barrier reach the index through the same observe.

The interceptor:

- `Build` uses the index of the snapshot and passes through without one. It
  rebuilds the pending writes of a transaction that was open at the barrier,
  so the commit of the transaction gets a companion delete for a key the
  index holds. The append of a DropCollection leaves the index open, and a
  write that reaches the observe after the drop is skipped.

The fake `Committer` of the tests serves the manifest bodies that it
committed. Not run for this change: the end-to-end tests with a restart.
