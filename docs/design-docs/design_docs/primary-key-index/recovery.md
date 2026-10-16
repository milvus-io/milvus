# MEP: Primary key index recovery at the WAL open

- **Created:** 2026-10-16
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** StreamingNode, `internal/streamingnode/server/pkindex/recovery`
- **Related Issues:** #52305
- **Related work:** [handover](snapshot.md), [write path](write-path.md), [core library](core-library.md), [manifest service](manifest.md)

## Summary

When a WAL opens, its recovery storage recovers the primary key index of
the pchannel. It opens the index of every indexed collection, claims the
pchannel and reads the manifests of the vchannels. It replays the WAL from
the checkpoint of the pchannel through the write authority of each index, so
the replay writes the same records as the append path. It installs the
committed table sets in the background while the index serves writes
degraded. The oldest write that an index did not commit holds the WAL
checkpoint back, so WAL truncation never drops a write the index does not
have. Two new handover triggers keep that hold bounded: a stalled checkpoint
and an explicit flush.

## Motivation

The WAL recovery replays one stream from the global checkpoint of the
pchannel to the recovery barrier. Each component skips the messages that it
persisted already and applies the rest. The index of a vchannel knows its own
checkpoint from its manifest (`checkpoint_time_tick`, see
[Coverage watermarks](snapshot.md#coverage-watermarks)). Two gaps stay
without this change:

- Without the replay, the index misses the writes between its manifest
  checkpoint and the barrier. Those writes were in the engine before the
  restart, and the open removes the engine.
- Without a link from the index to the WAL checkpoint, the WAL could drop
  writes that the index did not commit. A later replay then can not bring
  them back.

One pchannel after a restart:

```
global checkpoint of the pchannel     80
recovery barrier                     150
vchannel A: manifest checkpoint      100   committed tables cover tt <= 100
vchannel B: no manifest                    a collection that never handed over

replay delivers tt 80..150 to the index
  A skips tt <= 100, applies (100, 150]       the committed set covers the rest
  B applies (80, 150]                         its engine starts empty
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
      initRecoveryModules       pkrecovery.Recover: opens the indexes, claims, reads the manifests
    runBoundedRecovery          replays the stream from the checkpoint
      observeModulesMessage     Recovery.Observe for every persisted message
      at the barrier            Recovery.Finish: submits the installs, starts the handover driver
  adaptImplsToRWWAL             builds the interceptors; the pkindex interceptor borrows the index
  close(liveReady)              the scanner continues live; appends are accepted

background
  consumeDirtySnapshot          summaryThrough = min(summary.LastAcked, Recovery.LastAcked)
  handover driver               the handovers of snapshot.md, plus triggers 4 and 5
  install task per Index        reconcile committed/, download, install, MarkReady
  closeRecoveryResources        Recovery.Close, on every exit of the storage
```

Ownership:

- The recovery storage (`recoveryStorageImpl`) owns the `Recovery`. It
  creates it in `initRecoveryModules`, feeds it every replayed message in
  `observeModulesMessage`, finishes it at the barrier in
  `runBoundedRecovery`, reads its `LastAcked` in `consumeDirtySnapshot`, and
  closes it in `closeRecoveryResources`. Every exit path of the storage runs
  `closeRecoveryResources`: a failed open, a cancelled open, the AlterWAL
  path and a normal close. `RecoverySnapshot` gets no field for the index.
- The pkindex interceptor borrows the index. `Build` takes it from
  `Manager.Get(pchannel)` and passes the messages through when there is
  none. `Close` of the interceptor drops the reference only.
- The `Manager` is a registry of the open pchannels, nothing more. `New`
  registers a `PChannelIndexManager` without its driver, `Get` finds it, and
  `PChannelIndexManager.Close` removes it.

Lock order: the recovery storage holds `r.mu` when it calls `Observe` and
`LastAcked`, and those take the locks of the `PChannelIndexManager` and of
the `Index`. So the order is `r.mu`, then the index locks. The handover
driver calls `pkindexTail` (which takes `r.mu` through `Metrics`) outside
every index lock. No code of the index calls back into the storage while it
holds an index lock.

## Package layout

```
wal/recovery  ──▶  pkindex/recovery  ──▶  pkindex (root), pkindex/index
                   (Recovery, Param,      (Manager, PChannelIndexManager,
                    install task)          Claimer; Index, ReplayWrite)
```

- `internal/streamingnode/server/pkindex/recovery` is the pchannel level of
  the recovery: `Recover`, `Observe`, `Finish`, `Close`, `LastAcked`, the
  `Param` struct and the install task. It uses only the exported API of the
  root package and of `index`. It translates WAL messages into
  `index.ReplayWrite` values, so `index` reads no messages.
- The index level lives in `index/recovery.go`: `SetCommitted` records the
  manifest view of the vchannel, `Replay` applies one write, `Install` brings
  the committed set into the engine. These change the private state of the
  `Index` (the engine, `lastCommittedTimeTick`, the installed version, the
  readiness), so they are methods of it.
- The root package adds `Claimer.ReadManifest`, `PKsOf` (the conversion of
  `primarykey.Keys` to `authority.PK`, shared with the interceptor),
  `RequestHandover` and `RequestHandoverAll`, trigger 4 and
  `Params.StallTimeout`.

The `Recovery` holds only the state of the replay period: the targets by
vchannel (the `Index`, the primary key field ID and its data type), the
indexes with a manifest that wait for their install, the first error of the
replay, the `finished` and `closed` flags, and the handles of the submitted
install tasks. `Observe` works only before `Finish`. `Finish` runs once.
`Close` releases everything that `Recover` opened and is idempotent.

## Recovery of one pchannel

| Step | Where | What | Sync | Fails the open | Retries until the open ends |
|---|---|---|---|---|---|
| R1 open the indexes | `Recover` | `Manager.New` registers the `PChannelIndexManager` without its driver, with the WAL checkpoint as the watermark seed. For every `VChannelMeta` in state NORMAL whose primary key is not autoID and is Int64 or VarChar, `CreateCollection` opens the `Index`. The last schema of the meta names the primary key field. | Sync | A stopped `Manager`, an unsupported primary key type, a local disk error | — |
| R2 claim and manifests | `Recover` | `ClaimPChannel(pchannel, term)`. `GetRecoveryManifests` in batches of `MaxRecoveryManifests` (10000) refs. `ReadManifest` of every found body, at most `ManifestReadConcurrency` at a time. `SetCommitted(view)` on each `Index` with a manifest. `MarkReady(false)` on each `Index` without one. | Sync | `ErrFenced`, `ErrRejected`, a wrong answer count, a body whose version differs from the pointer | Every other error of the claim, the lookup and the body read |
| R3 replay | `Observe` | See [Replay](#replay). | Sync, with the framework replay | The first error of the replay, returned by `Finish` | — |
| R4 install | `Finish` | One install task per `Index` with a manifest on the node scheduler. Then `Start` of the handover driver. | Async | A missing or corrupted object, through `OnFatal` | Every other error, with backoff, until the task is cancelled or the `Index` closes |
| R5 the interceptor | `Build` | `Manager.Get(pchannel)` borrows the index. | — | — | — |

R2 is synchronous. The replay skips by the checkpoint of each vchannel, so
the checkpoints must be known before the first replayed message. The claim
must come before any commit, and a handover can follow the replay at once.
With `MaxRecoveryManifests` refs per lookup, the part of the open that grows
with the number of vchannels is the body reads. The body reads run in
parallel, and the first failed read cancels the others. A fatal error of any
read wins over the cancel error of a sibling.

Without a `Committer`, R2 does not claim and does not look anything up:
every `Index` is ready at once, the replay starts at the global checkpoint
and skips nothing, and `LastAcked` stays out of the truncation (see
[Truncation](#truncation)).

## Readiness

[Readiness](snapshot.md#readiness) defines ready: the committed table set of
the vchannel is installed. Who calls `MarkReady`:

- `Recover`, right after the lookup, for an `Index` without a manifest.
- The install task, at the end of a successful `Install`, for an `Index`
  with a manifest.
- `Observe`, for a collection that a replayed CreateCollection creates. A
  collection created in this term has no manifest. A replayed
  CreateCollection of a vchannel that `Recover` opened from the catalog does
  nothing.
- `CreateCollection` after `Start`, for a new collection, as before.

Until then the `Index` serves writes degraded: `Authority` does not probe
the engine and treats every key as present. The handover driver starts only
at `Finish`, so no handover runs during the replay, and an `Index` that is
not ready is never a handover candidate.

## Replay

`Observe` receives the messages that the recovery framework delivers: a
transaction as one assembled message, an autocommit write, a DDL message, a
persisted TimeTick, in time tick order up to the barrier. What each becomes:

| Message | Action |
|---|---|
| TimeTick, RecoveryBarrier | `ObserveTimeTick(tt)`. The replay is sequential, so no writer is in flight and the watermark moves to tt at once. At the barrier it equals the barrier tt. |
| CreateCollection (not the control channel) | `CreateCollection` with the schema of the body and `MarkReady(false)`. A vchannel that is a target already is ignored. |
| DropCollection (not the control channel) | `DropCollection`. A failure is logged and does not fail the open. |
| Insert (autocommit) | `KeysOfInsert(body, pkFieldID, pkDataType)`, then `ReplayWrite{Kind: Insert, PKs, SegmentID, TimeTick}`. The segment is `Partitions[0].SegmentAssignment.SegmentId`, as on the append path. |
| Delete | `KeysOfDelete(body)`, then `ReplayWrite{Kind: Delete, PKs, TimeTick}`. A delete with an expression plan panics, as on the append path, which does not accept it. |
| Transaction | One `ReplayWrite` per body with the `TxnID` and the commit time tick, then `ReplayWrite{Kind: CommitTxn, TxnID, TimeTick: commit tt}`. A companion delete of the append path is such a transaction. |
| A message with a replicate header, a message of the control channel | Skipped, as on the append path. |
| Every other type | Ignored. |

`Index.Replay` skips a write whose `TimeTick` is at or below
`LastCommittedTimeTick`: the committed tables hold it already. A transaction
carries the commit time tick on every body, so it is skipped or replayed as
a whole. Every other write goes through `Authority.Decide` and
`WriteGuard.Apply`, the same code as the append path. So the replay and the
live write produce the same records, and the CommitTxn order of the engine
(deletes before puts) has one implementation. The `CompanionDeletePKs` of a
decision are not used by the replay: the companion delete of a live insert
is in the WAL as its own message, and the replay sees it again.

Before the install, the `Authority` of an `Index` with a manifest is
degraded, and a replayed delete writes a tombstone for every key. This is
required: the engine has no committed layer yet, so a probe would miss every
key, and the install would bring the deleted records back.

A replayed write goes through the dirty tracking of the append path, so it
adds the `Index` to the `LastAcked` heap with its manifest checkpoint as the
key. The body of a message is decoded with the context of the open. The
first error of the replay is kept, the messages after it are ignored, and
`Finish` returns it, which fails the open. An engine write that fails panics
in `WriteGuard`, as on the append path.

## Warm start

`index.Open` removes the engine directory from before this open and keeps
`committed/`. The generations in the removed engine directory could hold
three kinds of writes: writes at or below the manifest checkpoint, which the
committed tables hold already; writes above the global checkpoint, which the
replay writes again; and writes between the two, which do not exist because
the truncation never passes `LastAcked`. So nothing in the engine directory
is needed, and the engine starts empty.

`Install` runs after the replay, as a task on the node scheduler:

1. `reconcileCommittedDir` removes every file in `committed/` that the
   manifest does not list. That covers the tables that a compaction
   replaced and the temporary files of downloads that did not finish.
2. `installIfNewer` resolves a local file for each table. `resolveLocal`
   reuses `committed/<id>.sst` when its size equals the `Size` of the
   manifest, removes it and downloads again when the size differs, and
   downloads when it is missing.
3. `MarkReady(true)`.

Every step is idempotent, so the task retries `Install` with backoff on a
transient error (`BackoffBase` to `MaxBackoff`). A missing or corrupted
object is fatal: the task calls `OnFatal`, and the WAL becomes unavailable.
The task ends without an error when its context ends or the `Index` closes.

The warm start saves the download of the committed tables. The replay is
bounded by the WAL tail, so its cost stays small next to the committed set.

## Truncation

`consumeDirtySnapshot` computes the point through which the WAL checkpoint
may move:

```
summaryThrough := min(summaryManager.LastAcked(), pkindex.LastAcked())
completedPoint := ackTracker.CheckpointThrough(summaryThrough)
```

[LastAcked](snapshot.md#lastacked) is the minimum of the watermark of the
pchannel and the `lastCommittedTimeTick` of every dirty `Index`. An idle
vchannel does not hold it back. On a node without a `Committer`, `LastAcked`
returns `MaxUint64` before it reads the heap: nothing ever commits, so the
index never holds the truncation back.

`LastAcked` never returns less than it returned before (`ackedMax`). The
clamp never reports too much. Every value returned before is at most the
watermark at that time. When the watermark is V, every writer that entered
before tick V has ended, and the write adds its `Index` to the heap before
the writer ends. So every write with a time tick at or below V either is in
the heap already or is committed, and a writer that enters after tick V gets
a time tick above V. An `Index` that joins the heap with a key below
`ackedMax` therefore has only writes above `ackedMax`.

The WAL checkpoint is more than the truncation point. `PChannelCheckpointUpdater` reports it to DataCoord as the channel
checkpoint of every vchannel, and the flushing stage of an AlterWAL waits
until it reaches the time tick of the AlterWAL. A flush of a collection
returns on the acknowledgement of its ManualFlush broadcast and does not
wait for the checkpoint. An `Index` with uncommitted writes holds all of
these back, and the size triggers of [Triggers](snapshot.md#triggers) do not
fire on a small tail. So the index must hand over in bounded time.

## Triggers

Two triggers join the three of [Triggers](snapshot.md#triggers):

| # | Trigger | Condition | Pick |
|---|---|---|---|
| 4 | Stalled checkpoint | `LastAcked` without the clamp is below the recovery time tick of the WAL for `Params.StallTimeout` | The dirty `Index` with the smallest `lastCommittedTimeTick` |
| 5 | Explicit flush | A ManualFlush or FlushAll was appended with time tick T, and the watermark reached T | The dirty `Index` of the flushed vchannel, or every dirty `Index` of the pchannel for a FlushAll, while its `lastCommittedTimeTick` is below T |

Trigger 4 is the same idea as the stall request of the WAL ack tracker.
`Params.StallTimeout` is `dataNode.sync.period` (600 s by default). The
stall clock starts when the index first holds the truncation back and stops
when it does not. It keeps running while the oldest `Index` waits in its
backoff. The recovery time tick of the WAL is the completed point of the ack
tracker, which the `LastAcked` of the index does not clamp, so the condition
stays meaningful after the index joined the checkpoint.

Trigger 5: after the append of a ManualFlush, the interceptor calls
`RequestHandover(vchannel, tt)` with the time tick of the message. After a
FlushAll, it calls `RequestHandoverAll(tt)`. The request raises the
`requestedTT` of the `Index` and is never reset. The driver step picks the
`Index` once the watermark of the pchannel reached `requestedTT`, at every
step until a handover commits a checkpoint at or above it. The watermark
only moves forward and `Prepare` reads it after the pick, so the handover
covers the flush. A clean or not ready `Index` waits. So the checkpoint
moves past the flush after one handover, not after the stall timeout. The
cost is a small table per flush.

## Failure handling

| Case | Handling |
|---|---|
| A local disk error when an `Index` opens in R1 | The open fails with `pkindex.ErrIO`. The WAL is assigned again. |
| An unsupported primary key type in a `VChannelMeta` or a replayed CreateCollection | The open fails. |
| A stopped `Manager` | `Recover` returns an error marked `pkindex.ErrUnavailable`. The open fails. |
| `ErrFenced` or `ErrRejected` from the claim or the lookup | The open fails, as a fenced claim of the WAL checkpoint does. |
| A transient error of the claim, the lookup or a body read | `retry.Do` without an attempt limit, `BackoffBase` to `MaxBackoff`, until the context of the open ends. |
| A body whose version differs from the pointer, an answer count that differs from the ref count | `ErrRejected`. The open fails. |
| A body that does not exist or does not decode | `pkindex.ErrCorrupted` from `ReadManifest`. The open fails. |
| A read error of the replay, a decision on a closed engine | The first error is kept. `Finish` returns it, and the open fails. |
| An engine write that fails in the replay | Panic in `WriteGuard`, as on the append path. |
| A transient install error | Retry with backoff. The `Index` stays degraded. The first failure logs at warn level, the next ones at debug level. |
| A missing or corrupted object in the install | `OnFatal` through the fatal handler of the storage, which marks the WAL unavailable. The handler ignores the error once the storage closes. |
| The open is cancelled during the replay, or the opener fails after `Finish` (the replicate manager, the AlterWAL path) | `closeRecoveryResources` calls `Recovery.Close`: it cancels the install tasks, waits for them and closes the index. `Manager.Get` returns nil. |

## Configuration

| Key | Default | Refreshable | Meaning |
|---|---|---|---|
| `streaming.pkindex.recovery.manifestReadConcurrency` | `64` | Read at each WAL open | The number of manifest bodies that the recovery reads in parallel. |
| `dataNode.sync.period` | `600` (s) | Yes, at the next driver step | Existing key. The stall timeout of trigger 4, the same value as the stall timeout of the WAL ack tracker. |

The retry of the claim and of the install reads `handover.maxBackoff` as in
[Configuration](snapshot.md#configuration).

## Comparison with similar systems

| System | Replay start and skip rule | Serves during the replay | Local file reuse | What bounds the log truncation | Time or stall trigger |
|---|---|---|---|---|---|
| Pebble v1.1.5 (`open.go`, `compaction.go`) | Replays only the WALs with `FileNum >= minUnflushedLogNum` into the memtable, flushes them to L0 inside `Open` and moves `MinUnflushedLogNum` to a new log | No: the replay completes inside `Open` under `d.mu` | Existing SSTs stay in place; `checkConsistency` checks the file sizes against the MANIFEST | After a flush, `MinUnflushedLogNum` is the log of the oldest unflushed memtable, then `deleteObsoleteFiles` | `maybeScheduleDelayedFlush`: a timer marks the memtable `flushForced` |
| RocksDB (`db_impl_open.cc`) | `min_wal_number = max(MinLogNumberToKeep, MinLogNumberWithUnflushedData)`; `WriteBatch::InsertInto` filters each record by the `log_number` of its column family | No: `Recover` holds `mutex_`, one thread until the client has the handle | SSTs stay in place | With `avoid_flush_during_recovery`, the replayed memtables are not flushed and the WALs wait in `alive_wal_files_` for `FindObsoleteFiles` | `max_total_wal_size` forces a flush of the column family that holds the oldest WAL (not verified against the source) |
| SlateDB (`db.rs`, `wal_replay.rs`) | Replays the WAL SSTs after `replay_after_wal_id` of the manifest, skips rows with `seq <= last_l0_seq`; the result is a memtable, no L0 is written during the replay | No: `replay_wal` is an async step that `open` waits for (call site in `builder.rs`, not verified against the source) | `preload_disk_cache_on_startup` warms the local cache from the manifest; `CachedObjectStore` reads through | WAL GC reads `replay_after_wal_id` of the manifest; a `WalTruncated` during the replay refreshes the manifest, a larger `writer_epoch` is `Fenced` | — |
| Neon (RFC 025) | The pageserver finds `index_part.json` by generation and loads only indexes at or below its own generation | Reads download layers on demand (not verified against the source) | Local content is removed at a new attach; the RFC proposes secondary locations that keep local data for a faster failover | `remote_consistent_lsn` moves only after the index upload with a verified generation; the safekeeper drops WAL by it | — |
| TiKV raftstore (`peer_storage.rs`) | Applies the raft log after `apply_state.applied_index` of the KV engine; `truncated_state` is the truncation point | No: no service during `SnapState::Applying` | The KV engine stays in place | `clear_meta_in_kv_and_raft` syncs the KV write before it clears the raft log; a comment says the raft log GC needs a flush first | `raft_log_gc` by size and count (not verified against the source) |
| CockroachDB (`raft_log_queue.go`) | — | — | — | A truncated entry must be "already durably applied to the state machine"; with loose coupling it waits for a durable `RaftAppliedIndex` | `RaftLogQueueStaleThreshold` (100 entries) and `RaftLogQueueStaleSize` (64 KiB) allow a truncation; a slow follower forces one by size |
| Milvus WAL recovery framework (`wal/recovery`) | One scanner replays from the global checkpoint to the barrier; each component skips by its own `checkpoint_time_tick` | No: no append before the barrier | The segment views come from the catalog | `CheckpointThrough(min(summary.LastAcked))` | The ack tracker sends a persist request to a stalled vchannel after `ackStallTimeout` (`dataNode.sync.period`); an explicit flush persists the segments at once |

What the index follows:

- Every system replays inside the open and serves nothing until the replay
  is done. The index replays inside `runBoundedRecovery`, so the index is at
  the barrier when the first append arrives, and there is no write during
  the replay.
- The replayed writes stay in memory, nothing flushes them early (RocksDB
  `avoid_flush_during_recovery`, SlateDB). The replay writes into the active
  generation, and the normal handover publishes it.
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

## Not done in this change

- **The adapter over the manifest service.** It implements
  `Claimer.ReadManifest` with the manifest proto and the `Committer`. Until
  it is injected, the recovery runs without a `Committer`: no claim, every
  `Index` ready at once, `LastAcked` out of the truncation. The adapter must
  merge after this change. Otherwise a manifest could be committed while the
  truncation does not wait for the index, and after a restart the writes
  between the manifest checkpoint and the global checkpoint would be gone.
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

## Test plan

Unit tests cover these behaviors.

`pkindex/recovery`:

- `Recover` opens the index of every indexed vchannel, claims the pchannel,
  marks an `Index` without a manifest ready and leaves one with a manifest
  not ready with its checkpoint set. A dropped vchannel in the catalog is
  skipped. An unsupported primary key type fails the open.
- A fenced claim fails the open and leaves nothing open. A transient claim
  failure is retried. A stopped `Manager` fails the open. Without a
  `Committer`, every `Index` is ready at once.
- The manifests are looked up in batches. A body whose version differs
  from the pointer is rejected. A fatal body read cancels the other reads and
  wins over their cancel error.
- `Finish` starts the driver and submits the installs. `Close` cancels
  them and makes `Manager.Get` return nil.
- The replay of inserts, deletes, transactions and companion deletes gives
  the same engine content as the append path, for Int64 and VarChar keys.
  Writes that the manifest covers are skipped. Replicated messages and the
  control channel are skipped. A replayed CreateCollection creates a ready
  `Index`, one of an opened vchannel is ignored, and a DropCollection drops
  it. The first error is kept and `Finish` returns it. `Observe` after
  `Finish` does nothing.
- The install task retries a failed download and marks the `Index` ready,
  reports a missing object through `OnFatal`, and stops when the `Index`
  closes.

`pkindex` (index and driver):

- An open removes the engine directory, also when only it has files, and
  keeps `committed/`. `resolveLocal` reuses a table with the right size and
  downloads one with the wrong size again.
- `Replay` skips the writes the manifest covers, applies a transaction as a
  whole, and a delete before the install writes a tombstone for every key.
- `Install` removes the files the manifest does not list, only marks ready
  without a manifest, and retries after a failed download.
- `RequestHandover` waits for the watermark, repeats until a commit covers
  it, keeps the highest tick, and waits on a clean or not ready `Index`.
- A stalled checkpoint hands over the oldest `Index`. The stall clock runs
  through a backoff. A zero stall timeout never triggers.
- `LastAcked` without a `Committer` stays out of the truncation. `PKsOf`
  keeps the order and the type of the keys.

`wal/recovery`:

- `pkindexTail` reads the recovery metrics. The `LastAcked` of the index
  clamps the checkpoint. The index is closed when the open fails. A `Finish`
  error fails the open.

The interceptor:

- An appended ManualFlush requests a handover of its vchannel that covers
  the time tick of the message. An appended FlushAll requests one of every
  vchannel of the pchannel.

The fake `Committer` of the tests serves the manifest bodies that it
committed. Not run for this change: the end-to-end tests with a restart.
