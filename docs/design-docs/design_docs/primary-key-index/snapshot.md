# MEP: Primary key index handover to object storage

- **Created:** 2026-10-12
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** StreamingNode, `internal/streamingnode/server/pkindex`
- **Related Issues:** #52305
- **Related work:** [write path](write-path.md), [core library](core-library.md), [manifest service](manifest.md)

## Summary

The streaming node moves the primary key index writes of each vchannel from
the engine generations on local disk to a committed table set in object
storage. This step is the handover. A handover freezes the active generation,
uploads its tables, commits them to the manifest service, and installs the
result. Each vchannel then knows up to which time tick (tt) its index is
committed. The pchannel reports the minimum of these values as `LastAcked`, a
future bound for WAL truncation.

This document covers the handover, the coverage of a handover, `LastAcked`,
the triggers and the failure handling. It also describes the structure
(`Manager`, `PChannelIndexManager`, `Index`, `Decider`) that runs them.

## Motivation

The [write path](write-path.md) writes each index change into the engine of
the vchannel. The engine keeps the writes in generations on one node (see
[Handover cycle](core-library.md#handover-cycle)). Without a handover, two
problems stay:

- The generations grow without a bound. Nothing moves them off the node.
- The index can not give the WAL a truncation bound. Recovery must replay the
  WAL from a point that the index has fully committed. Without a committed
  checkpoint, that point does not exist.

## Components

```
Manager                     one per streaming node: shared engine caches, config
 └─ PChannelIndexManager    one per pchannel: the member table, the handover driver,
     │                      lastObserved, the triggers, LastAcked
     └─ Index               one per vchannel: engine, Decider, lastCommittedTimeTick
          │  └─ Decider     the write path decision over the engine (reads only)
          ├─ Apply ◀── the observe of the WAL recovery, the only writer of the engine
          └─ handover ── upload ──▶ object storage   (TableStore)
                      ── commit ──▶ manifest service (Committer)
                      ── install ─▶ engine           (InstallCommitted)

packages:  server/pkindex  ──▶  server/pkindex/index  ──▶  pkindex/dedup  ──▶  pkindex/{codec, engine}
           (Manager, PChannelIndexManager)   (Index, handover)      (Decider, WriteIntent)
```

- `internal/streamingnode/server/pkindex` is the pchannel level and above:
  `Manager` opens a `PChannelIndexManager` per pchannel. The
  `PChannelIndexManager` keeps the member table of its vchannels
  (`CreateCollection`, `DropCollection`, `Decider(vchannel)`), takes the
  writes and the TimeTicks from the observe of the WAL recovery (`Apply`,
  `ObserveTimeTick`), evaluates the handover triggers at each TimeTick, drives
  the handovers once the recovery started it (`Start`), and keeps
  `lastObserved` and `LastAcked` (`lastacked.go`). The root package defines
  `ManifestReader`, the part of the manifest service that the recovery of a
  pchannel uses: `GetRecoveryManifests`.
- `internal/streamingnode/server/pkindex/index` is the vchannel level: the
  `Index` owns the engine of a vchannel and its `Decider`, and the handover
  is a behavior of it: its two phases, Prepare and Commit, are methods of the
  `Index`, and the pending request is a field of it. The package defines the
  interfaces and types that the handover exchanges with object storage and
  the manifest service: `TableStore`, `Committer` (only `CommitFlush`),
  `ManifestView`, `FlushArgs`, `ErrFenced`, `ErrRejected`. The `Index` does
  not import the pchannel level: `index.Config` carries what it needs from
  it, and two callbacks (`LastObserved`, `OnRefresh`) that the
  `PChannelIndexManager` points at its `lastObserved` and its `LastAcked`
  heap. `Index.Apply` writes the change of one WAL message (`index.Write`:
  the deletes, the puts each with its segment, and the time tick) into the
  engine; it is the only engine write. Its order is the engine write, then
  `Decider.Observe`, then the dirty mark. Neither package depends on the
  WAL.
- `internal/pkindex/dedup` holds the write path decision (`Decider`,
  `WriteIntent`, the in-flight set that is also the key lock, and the pending
  writes of transactions) over the `engine.RW` of a vchannel. It reads the
  engine for its probes and never writes it. `Index.Apply` reports the
  writes of the observe to it (`Observe`), which retires the entries of the
  in-flight set, and `ObserveTimeTick` reports every observed TimeTick to
  every index, also an idle one (`Observe` without keys), which removes the
  in-doubt entries up to it. The `Index` owns one `Decider` and the
  interceptor decides every write through `PChannelIndexManager.Decider(vchannel)`.
  The package knows neither the `Index` nor the WAL.
- `TableStore` has two methods, `UploadFile` and `DownloadFile`. The
  handover defines this interface for its own use. It does not use the
  object store interface of the manifest service.
- The manifest service is used through two interfaces that the users define,
  `ManifestReader` (the recovery of a pchannel) and `Committer` (the handover of a
  vchannel); the root package's `Committer` is the union of the two, which the
  one adapter of the streaming node implements. Neither side imports the manifest service, its
  client or its proto. See [Committer](#committer).

### Committer

The index owns the types that it exchanges with the manifest service:

| Type | Fields |
|---|---|
| `ManifestView` | `Version`, `SSTBasePath`, `CheckpointTimeTick`, `Tables []TableRef` |
| `TableRef` | the table info of the core library (`sst.Info`): `ID`, `TableFormat`, `Size`, `MinKey`, `MaxKey`, `NumEntries` |
| `VChannelRef` | `CollectionID`, `VChannel` |
| `RecoveryManifest` | `Found bool`, `Version`, `BodyKey` (the object key of the manifest body), `Err` (the failure of this item) |
| `FlushArgs` | `CollectionID`, `VChannel`, `PChannelTerm`, `PKDataType`, `AddTables`, `CheckpointTimeTick` |

```go
// package pkindex (pchannel level)
type ManifestReader interface {
    GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
}

// package index (vchannel level)
type Committer interface {
    CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error)
}
```

- `GetRecoveryManifests` returns one answer for each ref, in the same order.
  An item that fails has its own `Err`. A call has at most
  `MaxRecoveryManifests` (10000) refs. The handover only uses `Found`.
  `Version` and `BodyKey` are for recovery, which reads the manifest body.
- The first `CommitFlush` of a vchannel creates its manifest at version 1
  with `PKDataType`.

The streaming node does not claim a pchannel. The manifest service fences a
flush through the pchannel meta of the streaming coordinator: a
`CommitFlush` whose `PChannelTerm` is below the term of that meta is fenced,
one above it is rejected, and the pointer write checks that the meta did not
change. A lookup carries no term, so it is never fenced.

A `Committer` returns an error that matches one of two sentinels of the
handover when a retry can not help:

- `index.ErrFenced`: the pchannel is assigned to a higher term.
- `index.ErrRejected`: the manifest service rejected the request for
  good. Examples are a bug of the caller, an invalid request, and a
  corrupted stored manifest.

Every other error is retryable. The handover code does not look at merr
codes. The adapter over the manifest service client translates them, in a
later change (see [Not done in this change](#not-done-in-this-change)):

| Manifest service code | Handover error |
|---|---|
| `ChannelNotAvailable` | `ErrFenced` |
| `ServiceInternal`, `ParameterInvalid`, `DataIntegrity` | `ErrRejected` |
| `ServiceUnavailable`, `ServiceNotReady`, a timeout, a cancel | retryable, unchanged |

The streaming node computes the SST base path of a vchannel itself, with
`pkindex.SSTBasePath(rootPath, collectionID, vchannel)`:
`<rootPath>/pk_index/<collectionID>/<vchannel>/sst`, where `rootPath` is
`ChunkManager.RootPath()`. The handover uploads under the computed path. The
manifest records its own `SSTBasePath`, and the install downloads with it.
When the two differ, the commit is `ErrRejected`.

`pkindex.SSTKey` builds the object key of a table:
`<sst_base_path>/<id>.sst`. The file `internal/pkindex/objpath.go` builds
and parses all object keys of the primary key index. The manifest service and
its garbage collection use the same functions, so both sides use one key
format. `objpath.go` and the constant `common.PKIndexPath` are byte-identical to
the copies in the manifest service change. The change that merges second
drops its copy at the rebase.

`NewManager` panics without a `Committer`. An `Index` that can not commit is
never recovered: the WAL checkpoint would not wait for it, and a reopen of the
WAL would lose the writes before the checkpoint. This change does not inject a
`Committer` (see
[Wiring into the streaming node](#wiring-into-the-streaming-node)).

### Wiring into the WAL

The pkindex interceptor is a thin shell. It translates WAL events into calls on
the primary key index:

- Before the downstream append, the interceptor calls
  `PChannelIndexManager.Decider(vchannel).Decide`. `Decide` returns a
  `WriteIntent`. A vchannel without an index (it closed between the lookup of
  the target and the call) gives the on-shutdown error of the streaming node.
- After the append, the interceptor calls `WriteIntent.Appended`,
  `WriteIntent.NotAppended` or `WriteIntent.InDoubt`. None of them marks the
  `Index` dirty: the observe does, when it applies the message.
- The observe of the WAL recovery storage applies every persisted message to
  the index: it translates the message into an `index.Write` and calls
  `PChannelIndexManager.Apply(vchannel, write)`, in time tick order, one
  message at a time, for the replay and for the live stream alike. At every
  persisted TimeTick it calls `ObserveTimeTick(tt, tail)` with the WAL tail
  of the recovery storage. When it sees a ManualFlush or a FlushAll, it calls
  `ObserveFlush` or `ObserveFlushAll` with the time tick of the flush; the
  `Index` then commits a checkpoint at or above that time tick soon, once
  `lastObserved` reaches it. The flush is evaluated with the other triggers
  at the next TimeTick. A
  write that finds its `Index` closed because a DropCollection detached it
  after the lookup is ignored, so `lastObserved` keeps moving. This wiring
  is part of the recovery change.
- A `Decide` that fails with `pkindex.ErrCorrupted` makes the interceptor call
  `OnFatal`: the index of the vchannel is wrong, and the WAL becomes
  unavailable.
- The interceptor build param carries an `OnFatal` callback. The WAL opener
  fills it with the function that marks the WAL unavailable.
- The index never calls the recovery storage: the tail comes with each
  `ObserveTimeTick`.
- A `Decide` on a closed index returns the on-shutdown error of the streaming
  node.
- The WAL recovery owns the index of a pchannel. It creates it with
  `Manager.New` (the pchannel, the seed of `lastObserved`, `OnFatal`),
  creates the index of every existing collection with `CreateCollection`,
  recovers them (manifest lookups, install, replay), calls `Start`, and
  closes it with the WAL (see [Readiness](#readiness)).
- The interceptor only uses the index: `Build` reads it from the recovery
  snapshot (`RecoverySnapshot.PKIndex`), and passes the messages through when
  there is none.
  It registers the translation of every existing collection and calls
  `CreateCollection` for it, which finds the index that the recovery opened.
  An open that fails on the local disk goes to `OnFatal`: the WAL rejects all
  appends, and its next open tries again. A schema that the index rejects is
  a broken invariant for an existing collection, and it panics. `Close` of the
  interceptor does not close the index.
- The rollback of a failed CreateCollection calls
  `PChannelIndexManager.DropCollection`. It closes the index and removes `<root>/<vchannel>/`. A
  vchannel name embeds the collection ID, so no later open of the vchannel
  would remove the files. At a DropCollection the interceptor only forgets
  the vchannel; the observe of the WAL recovery closes the index when it
  reaches the drop, after the writes before it (see
  [recovery](recovery.md#observe)). A failed removal is only logged.

### Wiring into the streaming node

When `streaming.pkindex.enabled` is true, the streaming node builds the
`Manager` at start. The `Manager` gets the shared engine caches, the local data
directory, the chunk manager as the `TableStore`, the node ID allocator and
the node scheduler. The WAL interceptors get the `Manager` as a streaming
node resource.

The `Manager` gets no `Committer` in this change. A later change builds the
adapter over the manifest service client and injects it, after the manifest
service is merged. Until then, a streaming node with
`streaming.pkindex.enabled` set to true panics at start, and the code marks
the place with a TODO. The switch is false by default, and the product entry
for the index does not exist yet, so production does not see the difference.

The interceptor does not read `streaming.pkindex.enabled`. It uses the index
only when the streaming node built the `Manager`. Without a `Manager`, it
passes every message through. The param can not change at runtime (see
[Configuration](#configuration)), so the `Manager` exists exactly when the
param was true at start.

On stop, the `Manager` and the caches close after the WAL manager.
The WAL interceptors close their channels first.

At start, the node warns when the soft open-file limit is below the table
cache size plus a reserve of 10000. The number of indexed vchannels is unknown
at start, so the node does not refuse to start.

Every build now has the engine. The note in the [write path](write-path.md)
design that the engine exists only in test builds no longer applies, and the
integration suite of the write path runs without a special binary.

## One handover

A handover has two phases. Only one handover of a vchannel runs at a time.

```
Prepare (only when the vchannel has no pending request):
  covered := lastObserved                     read it first, see Coverage
  if covered <= lastCommittedTimeTick: stop   nothing new is observed
  RotateIncrement                             freeze the active generation
  for each frozen generation, newest first:   includes leftovers of failed handovers
      StageFrozen                             same tables, same IDs on every call
      hard-link each table to <dir>/committed/<id>.sst
  request := CommitFlushRequest{pk_data_type, add_ssts, checkpoint_time_tick = covered}
  record it as the pending request at once    no upload yet
                                              (request is nil when there are no tables)

Commit (a retry sends the same pending request):
  request is nil:  DropFrozen the generations, lastCommittedTimeTick = covered
  otherwise:       if no upload finished, or the upload is older than grace/2:
                       upload each table to <sst_base_path>/<id>
                   before := installed version
                   CommitFlush(request) → complete manifest
                   manifest.version < before or
                   manifest.checkpoint < covered:  ErrRejected, keep the generations
                   installIfNewer(manifest, retire the generations)
                   lastCommittedTimeTick = covered, clear the pending request
```

`Prepare` does not upload. The `Index` records its result as pending before
`Commit` starts. So a failed upload keeps the pending request, and the retry
does not rotate again. If `Prepare` uploaded and a failed upload left no
pending request, each retry would rotate once more. During a long object
storage outage, a retry each minute would make about 1440 frozen generations a
day. Each one is a separate engine database on the read path.

`Prepare` itself can still fail after the rotate, when `StageFrozen` or the
hard link fails. Then no pending request exists, and the retry rotates again.
The cause is a local disk error. A disk that keeps failing adds one frozen
generation at each retry.

Upload and commit run outside every engine lock (see
[Locking](core-library.md#locking)).

### A retry sends the same request

A retry does not read `lastObserved` again and does not rotate again. It sends
the pending request without a change. A counter-example shows why:

```
attempt 1: CommitFlush(ssts={7,8}, checkpoint=100)
           the manifest service commits it, the response is lost
attempt 2 if it prepared again:
           lastObserved is now 120, a new rotate gives table 9
           CommitFlush(ssts={9,7,8}, checkpoint=120)
           checkpoint moves forward → duplicate check → 7 and 8 are listed
           → ErrDuplicateSST, not retryable. The vchannel never hands over again.
attempt 2 with the same request:
           CommitFlush(ssts={7,8}, checkpoint=100)
           same term, checkpoint does not move forward → no write
           → the service returns the committed manifest
```

The [flush rule](manifest.md#flush) of the manifest service treats a same-term
flush whose checkpoint does not move forward as a retry of an applied flush.
The service keeps `ErrDuplicateSST` strict, so it finds caller bugs. SlateDB
and Neon use the same approach: a retry keeps the same SST ID or the same
upload.

Cost: while a request is pending, no new handover starts. This also applies
while its upload fails. During a long object
storage outage, the active generation keeps growing on local disk. The engine
memtable flushes to local tables when it is full, so memory stays bounded.

### The returned manifest must have the request in effect

`Commit` installs the returned manifest and retires the frozen generations
only when the manifest has the request in effect: its version is not below
the installed version from before the commit, and its checkpoint is not below
the covered time tick of this handover. Otherwise `Commit` returns
`ErrRejected`, which marks the WAL unavailable, and the generations stay.
Without this check, a flush that the manifest service takes as an applied
retry by mistake would make the generations retire while their tables are
not committed.

### Upload again before a late retry

Coordinator garbage collection keeps an uploaded table that no manifest lists
only for the [grace window](manifest.md#grace-window) (24 hours by default). A
retry can come after that window, for example after a long coordinator outage.
Then GC has deleted the tables, the retry commits, and the manifest lists
objects that do not exist.

So the pending request records its upload time. `Commit` uploads the tables
before it sends the request for the first time. When more than grace/2 passed
since that upload, `Commit` uploads the same tables under the same keys again
before it sends the request. The upload is idempotent and refreshes the object
modification time. The request does not change.

### Prepare does nothing while lastObserved did not move

When `lastObserved` is not above `lastCommittedTimeTick`, `Prepare`
returns no handover and does not rotate. A flush of the same term with that
checkpoint looks like a retry to the manifest service, and the service would
not add its tables. The writes stay in the engine, and a later handover
covers them.

### Local copies of committed tables

`StageFrozen` puts the tables in a staging directory of the engine. That
directory goes away when its generation retires. The handover hard-links each
table to `<root>/<vchannel>/committed/<id>.sst` and installs that path. The
engine lives in `<root>/<vchannel>/engine/`. `committed/` is outside it,
because `engine.Destroy` removes the whole engine directory. An open that
finds files from before in the engine directory or in `committed/` removes
both directories (see
[Readiness](#readiness)). So the links are not reused after a restart.

### Install order

A handover and, later, compaction both install a complete committed set. Two
installs that interleave could let an old set replace a new one. The engine
does not know manifest versions, so `Index` serializes the installs:

- `installIfNewer` installs a manifest only when its version is above the
  installed version.
- When a newer version is installed already, it only retires the covered
  generations. The newer version still covers what the older one covers.
- The set to install is the complete manifest that `CommitFlush` returns. The
  handover does not build the set itself.

Before the install, `Index` finds a local file for each table, in this order:

1. the links of this handover,
2. the tables that the `Index` installed before,
3. `committed/<id>.sst` (a link or a download of an earlier attempt of this
   open),
4. a download from object storage to `committed/<id>.sst`.

The download writes a temporary file and renames it. It syncs the file
before the rename and the directory after it. A temporary file that a crash
left in `committed/` is a file from before, so the next open removes both
local directories. The download runs outside the install lock, and the
version check runs again under the lock.

### Readiness

An `Index` is ready when the committed table set of its vchannel is
installed. `Index.Ready` reports it, and `Index.MarkReady` sets it once.
Until then the writes of the vchannel wait for it and the `Index` does not
hand over.

Who marks an `Index` ready:

- The recovery of the pchannel, for an `Index` of an existing collection. It
  looks up the manifests of the existing vchannels (`GetRecoveryManifests`, in calls of at most 10000
  items), and then marks an `Index` without a manifest ready right away, and
  an `Index` with a manifest after it downloaded and installed the committed
  set. It replays the WAL from the checkpoints and calls
  `PChannelIndexManager.Start` when it is done. The recovery is a later
  change; this change has no caller of `MarkReady` outside the tests.
- The `PChannelIndexManager`, for a collection created after `Start`. Its
  vchannel name holds the new collection ID, so no manifest can exist for it,
  and `CreateCollection` marks it ready at once. A collection created before
  `Start` waits for the recovery.

`Start` also starts the handover driver. No handover runs before it, and no
`Index` that is not ready is a candidate: a handover of such an `Index` would
commit a checkpoint that covers writes it does not have.

Why the index before its install can not hand over:

- **Restart.** Without a replay, the index after a restart does not have the
  writes that were only in a memtable before the restart. But `lastObserved`
  starts from the global checkpoint and moves with the TimeTicks. A handover would write "covers up to tt"
  into the durable manifest. When recovery later replays from that
  checkpoint, the missing writes never come back.
- **Move.** When a pchannel moves to another node and later comes back in a
  new term, the generations that stayed on local disk can be older than the
  data that the other node committed since. A handover of them would hide the
  newer state.
- **Crash after a commit.** The pending request lives only in memory. A crash
  after a successful commit and before the install loses it. The vchannel
  then has a manifest, and the install brings its tables back.

So an open removes the local files from before (the engine directory and
`committed/`, a file in only one of them counts too), and the `Index` starts
empty; the recovery brings the committed set back.

While an `Index` is not ready, its engine misses the committed records, so
its `Decider` can not decide exactly. `dedup.Config.Ready` is the ready
channel of the `Index`: until `MarkReady` closes it, `Decide` returns a
`*dedup.WaitError`, and the redo interceptor waits for the channel outside the
WAL lock. The observe still applies the WAL to the engine meanwhile, so the
`Index` is complete after the install. The alternative, a degraded decision
that treats every key as present, would give every insert a companion delete
for every key while the install downloads the tables.

A dirty `Index` that is not ready holds `LastAcked` back, like any dirty
`Index`: its writes are not committed.

## Coverage

Three stages of a WAL message give three positions:

| Stage | Meaning | Position |
|---|---|---|
| appended | The message is in the WAL. | tt of the last TimeTick message |
| observed | The index change of the message is in the engine. | `lastObserved` (per pchannel) |
| committed | The tables with the change are uploaded and listed by a committed manifest. | `lastCommittedTimeTick` (per vchannel) = manifest `checkpoint_time_tick` |

committed ≤ observed ≤ appended. Each position is a contiguous prefix: every
change with tt ≤ the value is done.

### lastObserved

The observe of the WAL recovery storage is the only writer of the engines. It
runs on one goroutine, delivers the persisted messages in time tick order, an
assembled transaction at the time tick of its commit, and `Index.Apply` writes
the change of a message before the observe moves on. So when the observe sees
a persisted TimeTick T, every change with tt ≤ T is in the engine.

`lastObserved` is that T: an atomic value of the `PChannelIndexManager` that
only `ObserveTimeTick` moves, at a persisted TimeTick or the recovery barrier.
Its seed is the time tick of the global checkpoint of the pchannel, where the
observe starts. The order matters: the observe writes the engine first, then
moves `lastObserved`.

No writer count and no epoch is needed. An earlier design of this change
counted the writers in flight of the interceptor, because the interceptor
applied its own write after the append returned, and writers of different
keys applied out of order. The interceptor no longer writes the engine, so
that algorithm went away.

### The rotate reads lastObserved first

```
covered := lastObserved
RotateIncrement
```

Every change with tt ≤ covered was written before the read, into the active
generation of that moment, and the rotate freezes it. So the frozen
generations hold every change up to covered, and the manifest checkpoint of
the handover is covered.

The observe can write more changes between the read and the swap. They land
in the frozen generation with a tt above covered. That is harmless: the
handover commits them, and after a restart the replay from covered applies
them again, with the same result (the same key gets the same value or the
same tombstone). The batch of one message never splits across two
generations: `engine.Write` holds the active generation for the whole call.

Why the rotate does not run in the observe, where covered would be exact:
`RotateIncrement` takes the cycle lock of the engine, which `StageFrozen`
(writes a whole generation to a table) and `InstallCommitted` (opens and
preloads tables) hold during their disk IO. An observe that waits for it
stops every module of the pchannel: the checkpoint, the segments, the WAL
summary. Pebble and RocksDB rotate in the writer thread because their rotate
only swaps pointers and the flush runs on another thread under another lock.
This change keeps the lock model of the engine and trades the exact bound for
a rotate outside the observe.

### Per-pchannel position

TimeTick messages are per pchannel, so `PChannelIndexManager` keeps one
`lastObserved` for all its vchannels. A per-vchannel value would need a walk
over all vchannels on every TimeTick. The shared value is never above the
true value of any vchannel, so it is only more conservative.
`lastCommittedTimeTick` stays per vchannel.

## LastAcked

```
LastAcked(pchannel) = min(lastObserved,
                          lastCommittedTimeTick of every dirty Index)
                      = +inf (math.MaxUint64) while the pchannel never had an Index
                      = the last returned value after Close
```

An `Index` is dirty when it has writes since its last rotate, frozen
generations, or a pending request.

- An idle vchannel does not hold the value back. It follows `lastObserved`:
  every write the observe applied to it is committed. WAL summary uses the
  same rule.
- A min-heap keyed by `lastCommittedTimeTick` holds the dirty indexes.
  `PChannelIndexManager.Apply` adds an `Index` when `Index.Apply` reports its
  first write since a rotate. The end of each handover and `MarkReady` update
  or remove it. A pchannel with many vchannels does not compute a full minimum on
  each call.
- After `Close`, the value is the last one returned before: the indexes are
  detached, but their uncommitted writes are not committed by that. Only a
  pchannel that never had an `Index` reports +inf.
- The value is monotonic. `PChannelIndexManager` keeps the largest value that
  it returned and never returns less. The clamp never reports too much: every
  returned value is at most the `lastObserved` of that time, and the observe
  adds an `Index` to the heap inside `Apply`, before it moves `lastObserved`
  past the write. So every write with a tt at or below `lastObserved` either
  put its `Index` in the heap already or is committed, and an `Index` that
  joins the heap later with a key below the clamp has only writes above it.
  The tail triggers read the value without the clamp, because the clamp can
  hide the `Index` that holds the checkpoint back.

The WAL recovery storage takes the minimum of this value and the `LastAcked`
of the WAL summary before it moves the WAL checkpoint. That wiring is part
of the recovery change.

## Triggers

The observe of the WAL recovery is the clock of the handovers. At every
persisted TimeTick, `ObserveTimeTick` evaluates the triggers below over the
dirty indexes and wakes the driver goroutine of the pchannel when one is met.
The driver then picks the vchannels to hand over and submits each handover as
a task to the node scheduler (`nodescheduler`). The driver has no ticker: it
wakes on that signal, and on the backoff of a failed handover. An idle
vchannel has no goroutine and no timer, and an idle pchannel costs no step.

| # | Trigger | Purpose | Condition | Default |
|---|---|---|---|---|
| — | Pending request | Finish a started handover | The `Index` has a pending request and its backoff is over. An observed TimeTick does not wake the driver for an `Index` in backoff: the backoff timer of the driver does | — |
| 1 | Size | Bound the unpublished data of one vchannel, batch the uploads | `ActiveBytes` of the engine ≥ `handover.activeBytes` | 64 MiB |
| 2 | Early tail | Hand over before the WAL throttles appends | WAL recovery tail ≥ `handover.tailBytes` and the index holds the checkpoint back: the dirty vchannel with the smallest `lastCommittedTimeTick` hands over | 2 GiB |
| 3 | Soft pressure | Fallback for trigger 2 | WAL recovery tail ≥ `walRecovery.tail.softWatermark` and the index holds the checkpoint back: same pick as trigger 2 | 8 GiB |
| 4 | Stall | Let the WAL checkpoint move when the tail is small: AlterWAL and the truncation wait for it | The index held the checkpoint back for `dataNode.segment.syncPeriod`, measured in observed time: same pick as trigger 2 | 600 s |
| 5 | Explicit flush | Let the checkpoint catch up with a flush within one upload and one RPC | The observe saw a ManualFlush of the vchannel (or a FlushAll of the pchannel) at tt, and `lastObserved` reached tt: the `Index` hands over until a handover commits a checkpoint at or above tt | — |

Only a ready `Index` hands over (see [Readiness](#readiness)). Triggers 2 to
4 look for the dirty `Index` with the oldest commit over every dirty `Index`,
also one that runs a handover, is in backoff or is not ready. When that
`Index` can not run now, they pick nothing: a younger `Index` would not move
`LastAcked`. A stall pick resets the stall clock, so the next stall pick
comes a stall timeout later, not at the next TimeTick while the handover
runs.

**The index holds the checkpoint back** when `LastAcked` without the clamp
is below the completed point of the ack tracker of the WAL recovery
(`CompletedTimeTick` in the tail that comes with `ObserveTimeTick`). The
completed point is the checkpoint before the modules bound it, so the
`LastAcked` of the index never bounds it. When `LastAcked` is at or above it,
another component holds the tail, and a handover does not shorten it. It
only makes a small table.

**Observed time, not wall clock time.** The stall of trigger 4 is measured
between two time ticks: the `lastObserved` at which the index started to hold
the checkpoint back, and the current `lastObserved`, by their physical parts.
The persisted TimeTicks of the WAL arrive at least every forced persist
interval, also on an idle pchannel, so no timer is needed, and a replay after
a restart does not count as a stall: its time ticks are in the past.

**Explicit flush.** The flush of a collection does not wait for the WAL
checkpoint, but AlterWAL and the truncation do. Without trigger 5 a flush on
a quiet collection would wait for trigger 4. The request is raised when the
observe sees the flush message, so it sits at the position of the flush in
the WAL, and the handover is picked only once `lastObserved` reached that
position, so it covers the flush. The cost is a small table.

`handover.tailBytes` is below `streaming.walRecovery.tail.lowWatermark`
(4 GiB). With the defaults, trigger 3 fires only when an operator sets
`tailBytes` above the soft watermark.

**Why trigger 2.** The index size does not follow the WAL size. A row with a
768-dim float vector takes about 3 KB in the WAL and about 25 B in the index.
When the WAL tail reaches the soft watermark (8 GB), the index holds about
65 MB. So the size trigger and the soft watermark arrive at about the same
time, and with larger vectors the soft watermark comes first. From the soft
watermark the WAL slows appends, and from 16 GB it rejects DML. Without
trigger 2, every pchannel would hit the throttle before each handover. A
trigger on replay distance is the same idea as Neon `checkpoint_distance` and
the replay distance trigger of TiKV.

**Why not by time alone.** WAL summary never triggers by time. When the WAL
gets no writes, its tail does not grow, so a small idle vchannel costs
nothing. Trigger 4 counts only while the index holds the checkpoint back.

**Why 64 MiB and not smaller.** The value does not control memory. The engine
memtable size controls memory, and a full memtable flushes to a local table.
`ActiveBytes` counts those tables too. Triggers 2 to 4 bound the WAL length.
A smaller value only makes more small tables. Until compaction exists,
nothing merges them, so each extra table adds manifest entries, coordinator
memory, open files and lookups. With many vchannels, most vchannels never
reach 64 MiB and hand over through triggers 2 and 4.

## Failure handling

| Case | Handling |
|---|---|
| Retryable upload, download or commit error (any error without a fatal sentinel: object storage, network, manifest service not ready, timeout, cancel) | Exponential backoff from 1 s to `handover.maxBackoff` (60 s), no limit on retries. A manifest lookup uses `retry.Do` and read `handover.maxBackoff` when they start. A handover uses `typeutil.BackoffWithInstant`: it reads `handover.maxBackoff` when the `Index` opens and after a successful handover that follows a failure, and its wait has a jitter of ±50 %. The pending request keeps the same table IDs. |
| Fenced (`index.ErrFenced`) on a commit | The `Index` stops its handovers. `OnFatal` marks the WAL unavailable: a newer owner has the pchannel. |
| Corrupted (`pkindex.ErrCorrupted`) in a handover | The `Index` stops its handovers. `OnFatal` marks the WAL unavailable. The WAL is assigned again and recovers on another node. |
| Corrupted (`pkindex.ErrCorrupted`) in a lookup on the write path | `Decide` returns the error and calls `OnFatal`, which marks the WAL unavailable. |
| Rejected commit (`index.ErrRejected`: duplicate table, schema mismatch, regressed checkpoint, a term that is not the assigned term, an invalid request, a corrupted stored manifest), a returned manifest that does not have the flush in effect, or a manifest with another SST base path | `OnFatal` marks the WAL unavailable. There is no retry: the same request fails the same way again. A retry without a limit would only hold `LastAcked` back until the WAL rejects DML. |
| Missing object on download | `DownloadFile` marks it as corrupted. A manifest lists the table, so a retry can not clear it. |
| Local disk error when the index of an existing collection opens at WAL open | `OnFatal` marks the WAL unavailable. The streaming node and its other WALs keep running. |
| Concurrency | At most one handover per vchannel at a time. The node scheduler limits the handovers of the node. |

- `isFatal` checks only `ErrFenced`, `ErrRejected` and
  `pkindex.ErrCorrupted`. The adapter of the later change maps the merr codes
  of the manifest service to the sentinels (see [Committer](#committer)).
  The mapping relies on the manifest service to return
  `ServiceUnavailable` for every transient failure. A transient failure that
  arrives as `ServiceInternal` would end the WAL instance.
- The table store on the streaming node classifies its errors:

  | Failure | Error |
  |---|---|
  | Remote object storage (`PutObject`, a reader, the network read of a download) | `pkindex.ErrUnavailable`, retried |
  | Local disk (open, stat, temporary file, rename, sync, a write of the local chunk manager) | `pkindex.ErrIO`, retried |
  | Missing object on download | `pkindex.ErrCorrupted`, fatal |

- The backoff state and the failure count belong to the `Index`. The task returns no error to the
  scheduler. The scheduler only has a fixed 100 ms requeue for a delayed task.
- After more than 5 failures in a row, the failure log goes from info to
  warn level.
- `OnFatal` stops all vchannels of the pchannel, not only the failed one. WAL
  summary handles its terminal errors the same way.
- A handover that ends because the `Index` or the scheduler closes is not a
  failure.

Metrics:

- `pkindex_handover_total`: finished handovers, with the empty ones.
- `pkindex_handover_failures_total`: failed handovers that will retry.

## Configuration

| Key | Default | Refreshable | Meaning |
|---|---|---|---|
| `streaming.pkindex.enabled` | `false` | No, forbidden at runtime | Experimental. Turns the index on. |
| `streaming.pkindex.dataDir` | empty | No, forbidden at runtime | Local data directory. Empty means `<localStorage.path>/pkindex`. |
| `streaming.pkindex.blockCacheSize` | `512m` | No, forbidden at runtime | Block cache that all indexes on one streaming node share. |
| `streaming.pkindex.maxOpenFiles` | `4096` | No, forbidden at runtime | Table cache size that all indexes on one streaming node share. |
| `streaming.pkindex.handover.activeBytes` | `64m` | Yes, at the next observed TimeTick | Size trigger. |
| `streaming.pkindex.handover.tailBytes` | `2g` | Yes, at the next observed TimeTick | Early tail trigger. Keep it below `streaming.walRecovery.tail.lowWatermark`. |
| `streaming.pkindex.handover.maxBackoff` | `60s` | Yes: a handover when the `Index` opens or after its next success | Longest wait between two attempts of a failed handover or manifest lookup. A new value does not change the wait of a handover that keeps failing. |
| `streaming.pkindex.memTableSize` | `4m` | No, read when an `Index` opens | Memtable size of the engine of one vchannel. |
| `streaming.walRecovery.tail.softWatermark` | `8g` | Yes, at the next observed TimeTick | Existing key. Soft pressure trigger. |
| `dataNode.segment.syncPeriod` | `600` (s) | Yes, at the next observed TimeTick | Existing key. Stall trigger, in observed time. |
| `dataCoord.pkindex.gc.grace` | `86400` (s) | No, read when the params are read | Key of the manifest service, defined here byte-identical to its definition there. `ReuploadAfter` of the primary key index is half of it: a retry uploads the tables again after that age, so that the garbage collection of the manifest service does not remove them before the commit. |

The first backoff wait (1 s) is a constant.

The config system does not apply a change of the four keys that are
forbidden at runtime. A read returns the value from start. The
streaming node builds the `Manager` only when `enabled` is true, and the
interceptor uses the index only when the `Manager` exists.

## Comparison with similar systems

| System | How the covered position is set | Isolation of frozen data from writes in flight | Trigger | Failure handling | Concurrency per shard |
|---|---|---|---|---|---|
| RocksDB | Structural bound: `SetNextLogNumber` at `SwitchMemtable`, flush writes `SetLogNumber` (`db_impl_write.cc`, `flush_job.cc`) | `WaitForPendingWrites` before the switch (`unordered_write` waits for `pending_memtable_writes_==0`). Install commits only the oldest contiguous prefix (`memtable_list.cc`) | `write_buffer_size`, `max_total_wal_size`, `WriteBufferManager` | Roll back to not flushed, report a background error | Several, installed in order |
| Pebble | `MinUnflushedLogNum` (`compaction.go`, `flush1`) | Reserve the memtable under the same lock that assigns the seqnum. Flush only at `writerRefs==0` (`mem_table.go`) | Full memtable, threshold, forced, delayed forced | No backoff (a TODO in the source) | 1 |
| SlateDB | Conservative `replay_after_wal_id` plus an exact `last_l0_seq` filter (`memtable_flusher/`) | Single writer. Publish only the oldest contiguous prefix | `l0_sst_size_bytes` (64 MiB), `max_wal_flushes_before_l0_flush`, `max_unflushed_bytes` | Retry without a limit, keep the same SST ID | 4 uploads, single manifest writer |
| Neon | `last_record_lsn+1` at freeze. `remote_consistent_lsn` moves only after the index upload (`timeline.rs`) | Single ingest writer (`write_lock`) | `checkpoint_distance` (LSN distance), `checkpoint_timeout`, global memory | Exponential backoff, retry without a limit | 1, node-level semaphore |
| TiKV v2 / unistore | Applied index at freeze (`apply_trace.rs`, `engine/flush.go`) | Single apply thread | Size, replay distance (4096000) | — | — |
| Milvus WAL summary | `lastObserved` that the recovery scanner sees in tt order (`walsummary/manager.go`) | Observed in order | Size, tail pressure, never by time (`walsummary/async.go`) | `ErrDelay`, retry without a limit | Chunks in parallel, single manifest writer |

Where the rotate runs and where the triggers are evaluated:

| System | Rotate | Table write | Triggers | Source |
|---|---|---|---|---|
| Pebble 1.1.5 | Writer thread under `d.mu`; only swaps the memtable (`makeRoomForWrite`, `rotateMemtable`) | A flush goroutine | Size at the rotate, a timer for the delayed flush | `db.go`, `compaction.go` |
| RocksDB 9.7 | The write thread leader under `mutex_` (`SwitchMemtable`); only creates a memtable | A background thread of the env (`BGWorkFlush`), the mutex released during the table write | In the write thread: write buffer size, write buffer manager, total WAL size | `db_impl_write.cc`, `db_impl_compaction_flush.cc` |
| TiKV 8.5 raftstore-v2 | RocksDB seals the memtable; `on_memtable_sealed` records the applied index under the engine lock and must stay light | RocksDB background | `should_flush` on the distance between the modifications and the flushed index | `engine_traits/src/flush.rs`, `apply_trace.rs` |
| CockroachDB 24.3 | — | — | The log truncation runs in its own task, woken by the flush-completed callback of the engine, not by a timer | `raft_log_truncator.go` |
| Milvus WAL summary | The seal only moves records in memory, in the observe | Chunk write on the node scheduler | Size in the observe; tail pressure on a one-second loop | `walsummary/manager.go`, `async.go` |

Common patterns:

- A system freezes only at a point where all writes below the cut are in the
  frozen memtable, and records a structural bound. No system takes the
  maximum position of entries that apply concurrently. With one writer, the
  observe, the bound is `lastObserved`.
- Every system rotates in the writer or apply thread, and every rotate only
  swaps pointers; the table write runs on another thread. The engine of the
  index holds one lock across the rotate and the table write, so the rotate
  runs outside the observe (see [Coverage](#coverage)).
- Size triggers are evaluated by the writer or apply thread; time triggers
  need a clock. The index uses the observed TimeTicks as that clock, and the
  driver wakes on a signal, like the truncation task of CockroachDB.
- Publish commits only the oldest contiguous prefix, with a monotonic check.
  The log can be truncated only after the remote manifest commit.
- Systems on object storage retry without a limit and keep the same file ID.
- Neon and TiKV trigger on replay distance, not on local data size. Trigger 2
  uses the same idea.

## Not done in this change

- **The adapter over the manifest service client and its wiring.** A later
  change, after the manifest service and this change merge, adds the
  adapter that implements `Committer`. It translates the proto
  types and the merr codes (see [Committer](#committer)), with a test for
  each error class, and the streaming node injects it into the `Manager`.
  Until then no `Index` hands over.
- **End-to-end tests of the handover.** They run in the wiring change: a
  handover with object storage, an object storage outage, sustained writes
  with large vectors, and a check in the integration suite that a handover
  ran.

- **Recovery of a pchannel.** The manifest lookups, the install of
  the committed sets, the WAL replay and `Start`, and the `Manager.New` call
  that creates the index of a pchannel at WAL open. Nothing creates one in
  this change, so the interceptor passes the messages through. Until the
  recovery exists, an index after a restart starts empty and is never marked
  ready (see [Readiness](#readiness)).
- **`LastAcked` for WAL truncation.** `PChannelIndexManager.LastAcked` exists, but the WAL
  recovery storage does not use it. It goes in with recovery, because the
  bound only matters with replay.
- **Compaction and the view sync after it.** Nothing merges committed tables.
  Files in `committed/` that leave the committed set are not deleted.
- **A sweep of orphan local directories.** When the removal at DropCollection
  fails, or the node stops between the append and the removal, the directory
  of the dropped vchannel stays on disk. A sweep at WAL open can remove the
  directories of the vchannels that the WAL does not open.
- **A trigger on node memory pressure.** It needs a node-level memory budget.
- **A handover before a vchannel closes.** Recovery replays from the global
  checkpoint of the pchannel anyway. The need depends on the measured recovery
  speed.
- **The observe wiring.** The calls of `Apply`, `ObserveTimeTick`,
  `ObserveFlush` and `ObserveFlushAll` from the observe of the WAL
  recovery storage, and the translation of a message into an `index.Write`,
  come with the recovery change. In this change nothing calls them outside
  the tests, and the triggers never fire.
- **A forced handover on snapshot.** A snapshot persists segment data. The
  index is a performance index, and WAL replay covers it; a collection
  restored from a snapshot has no index and needs a rebuild.
- **GC that knows which tables a node still uses.** GC of the manifest service
  decides from the manifests only.

## Test plan

Unit tests cover these cases:

- Apply: a write at or below the committed checkpoint is skipped; the
  deletes of a message are written before its puts, so a key in both is
  present; a delete writes a tombstone for a key the index never saw; the
  first write since a rotate reports dirty and `PChannelIndexManager.Apply`
  puts the `Index` into the `LastAcked` heap.
- Coverage: the observe never rotates, only the driver does; the covered
  time tick of a rotate is the `lastObserved` read before it, and a write
  the observe applied after that TimeTick sits in the frozen generation and
  applies again without a change; a `StageFrozen` of a large generation does
  not block `Apply` and `ObserveTimeTick`.
- Handover: injected upload and commit failures. The request is pending
  before the upload, so a failed upload keeps it and the retry does not
  rotate again. A retry keeps the table IDs and sends the same request, also
  after a lost response. A retry after grace/2 uploads again. An empty
  handover retires its generations and moves `lastCommittedTimeTick`. An
  older manifest does not replace a newer one. A returned manifest below the
  installed version, with a checkpoint below the covered time tick, or with
  another SST base path, is rejected, and the generations stay.
- Install: a missing table is downloaded, and a missing object is fatal.
- Readiness: an `Index` created before `Start` is not ready, makes its
  writes wait, holds `LastAcked` back while dirty, and does not hand over; `MarkReady`
  makes it a candidate; a collection created after `Start` is ready at once.
  A ready `Index` uploads under the SST base path that it computes and
  creates the manifest at version 1 with its first handover. An open with
  files from before, in the engine directory or in `committed/` only,
  removes them and starts empty.
- No `Committer`: `NewManager` panics.
- Fatal errors: `ErrFenced`, `ErrRejected` and `ErrCorrupted` are fatal.
  Other errors are retryable, also a merr code that no adapter translated.
- Object key: `pkindex.SSTKey` gives `<sst_base_path>/<id>.sst`.
- Duplicate CreateCollection: a `CreateCollection` of an `Index` that exists
  reports that it created nothing, so the rollback after a failed append does not
  remove the `Index` in use.
- Drop: DropCollection and the rollback of a failed CreateCollection remove
  the local directory, and a new create of the vchannel sees none of the old
  keys. A drop on a closed `PChannelIndexManager` fails and keeps the files.
- WAL open: a local disk error goes to `OnFatal` and does not panic. An
  existing collection with an unsupported primary key type panics.
- Table store: remote failures are `ErrUnavailable`, local disk failures
  (also a write of the local chunk manager) are `ErrIO`, and a missing object
  is `ErrCorrupted`.
- Triggers: every trigger starts from an observed TimeTick; the driver runs
  no step without one and wakes for the backoff of a failed handover. Size,
  early tail (picks the smallest `lastCommittedTimeTick`), soft pressure,
  stall measured in observed time, and an explicit flush that is picked once
  `lastObserved` reaches the flush. The tail triggers do not fire when
  `LastAcked` is at or above the completed point of the WAL recovery. They
  use `LastAcked` without the clamp. They pick nothing while the `Index`
  with the oldest commit is in backoff or not ready. A TimeTick does not
  wake the driver for a pending `Index` in backoff. A stall pick restarts
  the stall clock.
- `LastAcked`: idle vchannels do not hold it back, the heap minimum equals a
  full minimum, and a closed `PChannelIndexManager` keeps its last value.
- `Apply` on a dropped `Index`: a write that finds the `Index` closed after a
  DropCollection is ignored; one on a closed `Index` that is still attached
  reports the closed engine.
- Failures: a fenced commit stops the handovers, a commit that the manifest
  service rejects is fatal, a retryable commit error backs off, and a lookup
  that finds corrupted data marks the WAL unavailable.
- Interceptor: without a `Manager`, it passes every message through. With a
  `Manager`, it uses the index.

The fake `Committer` of the tests follows the lookup and flush rules of the
manifest service. The integration suite of the write path is skipped: a
streaming node with the index on does not start without a `Committer`.

Not run for this change: the end-to-end tests of the handover. They run in
the wiring change. Restart consistency needs recovery.

An end-to-end run with a crash and a large WAL tail is not done. It waits for
recovery.
