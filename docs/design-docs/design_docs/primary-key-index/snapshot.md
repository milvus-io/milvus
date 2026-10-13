# MEP: Primary key index handover to object storage

- **Created:** 2026-10-12
- **Author(s):** @bigsheeper
- **Status:** Under Review
- **Component:** StreamingNode, `internal/pkindex/index`, `internal/pkindex/snapshot`
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

This document covers the handover, the coverage watermarks, `LastAcked`, the
triggers and the failure handling. It also describes the index layer
(`Manager`, `Channel`, `Index`) that runs them.

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
index.Manager            one per streaming node: shared engine caches, config
 └─ index.Channel        one per pchannel: the claim, the manifest lookups,
     │                   the handover driver, lastAppliedTimeTick, LastAcked
     └─ index.Index      one per vchannel: engine, decider, lastCommittedTimeTick
          └─ handover ── upload ──▶ object storage   (snapshot.TableStore)
                      ── commit ──▶ manifest service (snapshot.Committer)
                      ── install ─▶ engine           (InstallCommitted)
```

- `internal/pkindex/snapshot` holds the two handover phases (`Prepare`,
  `Commit`), the retry state (`Backoff`) and the `TableStore` interface. It
  does not know the streaming node.
- `internal/pkindex/index` holds the index layer. `Index` implements
  `snapshot.Target`.
- `snapshot.TableStore` has two methods, `UploadFile` and `DownloadFile`. The
  handover defines this interface for its own use. It does not use the
  object store interface of the manifest service.
- `snapshot.Committer` has three methods, `ClaimPChannel`,
  `GetRecoveryManifests` and `CommitFlush`. The handover defines it with its
  own types, so the handover code does not
  import the manifest service, its client or its proto. See
  [Committer](#committer).

### Committer

The handover owns the types that it exchanges with the manifest service:

| Type | Fields |
|---|---|
| `ManifestView` | `Version`, `SSTBasePath`, `CheckpointTimeTick`, `Tables []TableRef` |
| `TableRef` | the table info of the core library (`sst.Info`): `ID`, `TableFormat`, `Size`, `MinKey`, `MaxKey`, `NumEntries` |
| `VChannelRef` | `CollectionID`, `VChannel` |
| `RecoveryManifest` | `Found bool`, `Version`, `BodyKey` (the object key of the manifest body), `Err` (the failure of this item) |
| `FlushArgs` | `CollectionID`, `VChannel`, `PChannelTerm`, `PKDataType`, `AddTables`, `CheckpointTimeTick` |

```go
type Committer interface {
    ClaimPChannel(ctx context.Context, pchannel string, term int64) error
    GetRecoveryManifests(ctx context.Context, refs []VChannelRef) ([]RecoveryManifest, error)
    CommitFlush(ctx context.Context, args FlushArgs) (ManifestView, error)
}
```

- `ClaimPChannel` claims the pchannel for the term of the WAL instance. A
  claim with the claimed term again succeeds.
- `GetRecoveryManifests` returns one answer for each ref, in the same order.
  An item that fails has its own `Err`. A call has at most
  `MaxRecoveryManifests` (10000) refs. The handover only uses `Found`.
  `Version` and `BodyKey` are for recovery, which reads the manifest body.
- The first `CommitFlush` of a vchannel creates its manifest at version 1
  with `PKDataType`.

A `Committer` returns an error that matches one of two sentinels of the
handover when a retry can not help:

- `snapshot.ErrFenced`: a newer term owns the pchannel.
- `snapshot.ErrRejected`: the manifest service rejected the request for
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

The streaming node computes the SST base path of a vchannel itself, with the
shared helper `metautil.BuildPKIndexSSTBasePath(rootPath, collectionID,
vchannel)`: `<rootPath>/pk_index/<collectionID>/<vchannel>/sst`, where
`rootPath` is `ChunkManager.RootPath()`. This follows how the streaming node
builds binlog paths with `metautil.BuildInsertLogPath`. The handover uploads
under the computed path. The manifest records its own `SSTBasePath`, and the
install downloads with it. When the two differ, the commit is `ErrRejected`.
The helper and the constant `common.PKIndexPath` are byte-identical to the
copies in the manifest service change. The change that merges second drops
its copy at the rebase.

`snapshot.SSTKey` builds the object key of a table:
`<sst_base_path>/<id>.sst`. The manifest service and its garbage collection
use the same format. A test fixes the format, and the adapter change checks
it against the key function of the manifest service.

When the `Manager` has no `Committer`, no `Channel` claims, and no `Index`
hands over. The indexes still deduplicate the writes, and they stay out of
the `LastAcked` heap. This change does not inject a `Committer` (see
[Wiring into the streaming node](#wiring-into-the-streaming-node)).

### Wiring into the WAL

The pkindex interceptor is a thin shell. It translates WAL events into calls on
the index layer:

- Before the downstream append, the interceptor calls `Index.Decide`. `Decide`
  registers the writer with the watermark of the pchannel and returns an
  `Intent`.
- After the append, the interceptor calls `Intent.Apply` or `Intent.Discard`.
  Both end the writer in the watermark.
- After each TimeTick message returns from the append, the interceptor calls
  `Channel.Tick(tt)`.
- The interceptor build param carries an `OnFatal` callback. The WAL opener
  fills it with the function that marks the WAL unavailable.
- The pchannel reads the WAL recovery tail size from the recovery storage
  metrics (`RecoveryTailBytes`). The tail triggers use it.
- A `Decide` on a closed index returns the on-shutdown error of the streaming
  node.
- At WAL open, the interceptor opens the index of every existing collection.
  An open that fails on the local disk goes to `OnFatal`: the WAL rejects all
  appends, and its next open tries again. A schema that the index rejects is
  a broken invariant for an existing collection, and it panics.
- DropCollection and the rollback of a failed CreateCollection call
  `Channel.Drop`. It closes the index and removes `<root>/<vchannel>/`. A
  vchannel name embeds the collection ID, so no later open of the vchannel
  would remove the files. A failed removal is only logged, because the
  append is done.

### Wiring into the streaming node

When `streaming.pkindex.enabled` is true, the streaming node builds the
`Manager` at start. The `Manager` gets the shared engine caches, the local data
directory, the chunk manager as the `TableStore`, the node ID allocator and
the node scheduler. The WAL interceptors get the `Manager` as a streaming
node resource.

The `Manager` gets no `Committer` in this change. A later change builds the
adapter over the manifest service client and injects it, after the manifest
service is merged. Until then, the indexes only deduplicate the writes.
`streaming.pkindex.enabled` is false by default, and the product entry for
the index does not exist yet, so production does not see the difference.

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
  covered := lastAppliedTimeTick              read the watermark first
  if covered <= lastCommittedTimeTick: stop   nothing new is applied
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

A retry does not read the watermark again and does not rotate again. It sends
the pending request without a change. A counter-example shows why:

```
attempt 1: CommitFlush(ssts={7,8}, checkpoint=100)
           the manifest service commits it, the response is lost
attempt 2 if it prepared again:
           watermark is now 120, a new rotate gives table 9
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

### Prepare does nothing while the watermark did not move

When `lastAppliedTimeTick` is not above `lastCommittedTimeTick`, `Prepare`
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
[Only a fresh index hands over](#only-a-fresh-index-hands-over)). So the links
are not reused after a restart.

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

### Claim

The WAL open first adds the `Index` of each existing collection, and then
calls `Channel.StartClaim`. When the `Manager` has a `Committer`, the
`Channel` claims its pchannel once, in the background (see
[the manifest service](manifest.md)). The number of vchannels does not change
the number of claims. A failed claim retries with backoff. A fenced claim and
a rejected claim (see [Failure handling](#failure-handling)) mark the WAL
unavailable.

After the claim, the `Channel` looks up the manifests of the indexes of the
existing collections with `GetRecoveryManifests`, in calls of at most 10000
items. It retries only the items that failed, with backoff. A fenced or
rejected item, or a fenced or rejected call, marks the WAL unavailable. The
answer decides whether the `Index` is fresh.

An `Index` that a CreateCollection adds does not look up. Its collection is
new, and the vchannel name holds the new collection ID, so no manifest can
exist for it. The check for local files from before still applies. No `Index`
hands over before the claim and the lookups are done.

### Only a fresh index hands over

An `Index` is fresh when both of these are true:

- When it opens, its engine directory and its `committed/` directory hold no
  files from before this open. A file in only one of them counts too, for
  example the rest of a removal that did not finish.
- Its vchannel has no manifest.

When an open finds files from before, it removes both local directories, and
the `Index` starts empty and not fresh. When the vchannel has a manifest of
any version and content, the `Index` keeps the writes from after the open,
but it is not fresh either.

A manifest means that an owner committed for the vchannel. That owner can
have writes that it did not hand over, and that this `Index` neither has nor
replays. A handover of this `Index` would commit a checkpoint that covers
them, and the WAL replay would skip them.

The rule is conservative. `engine.Open` always creates the first generation,
so any reopen of a vchannel on the same node finds files from before and does
not hand over until recovery exists. It never writes a wrong manifest.

Only a fresh `Index` hands over. An `Index` that is not fresh:

- serves writes as usual,
- never hands over,
- does not join the `LastAcked` heap, so it does not hold `LastAcked` back.

Recovery does not exist yet. When it replays the WAL into the index, it will
clear this state.

Why the data from before can not hand over:

- **Restart.** Without a replay, the index after a restart does not have the
  writes that were only in a memtable before the restart. But the watermark
  starts from the current TimeTick. A handover would write "covers up to tt"
  into the durable manifest. When recovery later replays from that
  checkpoint, the missing writes never come back.
- **Move.** When a pchannel moves to another node and later comes back in a
  new term, the generations that stayed on local disk can be older than the
  data that the other node committed since. A handover of them would hide the
  newer state.
- **Crash after a commit.** The pending request lives only in memory. A crash
  after a successful commit and before the install loses it. The vchannel
  then has a manifest, so the `Index` is not fresh and does not send the
  tables again.

An alternative was to hand over as usual and let recovery mark the older
checkpoints as not trusted by version. It was not chosen, because the wrong
data would first go into storage.

## Coverage watermarks

Three stages of a WAL message give three watermarks:

| Stage | Meaning | Watermark |
|---|---|---|
| appended | The message is in the WAL. | tt of the last TimeTick message |
| applied | The index change of the message is in the engine. | `lastAppliedTimeTick` (per pchannel) |
| committed | The tables with the change are uploaded and listed by a committed manifest. | `lastCommittedTimeTick` (per vchannel) = manifest `checkpoint_time_tick` |

committed ≤ applied ≤ appended. Each watermark is a contiguous prefix: every
change with tt ≤ the value is done. It is not the tt of the newest applied
message.

### Why the newest applied tt is wrong

A writer applies its change after its append returns. Writers on different
lock stripes apply in any order. A TimeTick acknowledgement means that the
append returned, not that the index applied the change. A counter-example:

```
writer A: enter pkindex → tt=100 → append returns ……… (not applied yet)
writer B: enter pkindex → tt=101 → append returns → apply into gen1
handover: rotate (gen1 frozen, "covers 101") → upload → commit checkpoint=101
writer A:                                             apply into gen2
crash → replay starts after tt=101 → change of A (tt=100) is lost
      → a later duplicate of A's key is not detected
```

### The epoch algorithm

TimeTick messages split the writers into epochs. The watermark counts the
writers in flight per epoch. It never blocks a writer. It only delays the
reported value.

```
writer enters pkindex (before Decide):          inflight[cur]++
writer ends (engine write done, or Discard):    inflight[its epoch]--
TimeTick T returns from the append (Tick):      close epoch cur with T, cur++
an epoch and all older epochs have no writers:  lastAppliedTimeTick = T of that epoch
```

The same counter-example with epochs:

```
A enters (epoch 0)    B enters (epoch 0)          inflight[0] = 2
A gets tt=100, B gets tt=101, both appends return
B applies, ends                                    inflight[0] = 1
Tick(101): epoch 0 closes with 101, cur = 1        A is in flight: watermark stays
handover: covered = old watermark (< 100)
A applies, ends                                    inflight[0] = 0 → watermark = 101
```

The proof uses one fact: the pkindex interceptor is above the timetick
interceptor. So a writer enters the watermark before it gets a tt. A TimeTick
message T is appended only after every message with tt ≤ T is appended. So
every writer with tt ≤ T entered before `Tick(T)` and counts in an older or the
same epoch. A writer that enters before `Tick(T)` and gets a tt > T only delays
the watermark.

Messages that do not change the index do not enter: TimeTick, flush, writes to
a collection without an index, and discarded writes release at once. The
engine changes of a client transaction happen at its CommitTxn. Replay also
delivers an assembled transaction at the tt of its CommitTxn, so both sides
agree.

The handover reads `lastAppliedTimeTick` before it rotates. All changes with
tt ≤ that value are then in the frozen generations or older ones. A frozen
generation can also hold changes with a larger tt. That is harmless: replay
applies them again in WAL order with the same result.

### Per-pchannel watermark

TimeTick messages are per pchannel, so `Channel` keeps one
`lastAppliedTimeTick` for all its vchannels. A per-vchannel watermark would
need a walk over all vchannels on every TimeTick. The shared value is never
above the true value of any vchannel, so it is only more conservative.
`lastCommittedTimeTick` stays per vchannel.

## LastAcked

```
LastAcked(pchannel) = min(lastAppliedTimeTick,
                          lastCommittedTimeTick of every dirty Index)
                      = +inf (math.MaxUint64) when the pchannel has no Index
```

An `Index` is dirty when it has writes since its last rotate, frozen
generations, or a pending request.

- An idle vchannel does not hold the value back. It follows
  `lastAppliedTimeTick`. WAL summary uses the same rule.
- A min-heap keyed by `lastCommittedTimeTick` holds the dirty indexes. The
  first write after a clean state adds an `Index`. The end of each handover
  and of the manifest lookup updates or removes it. An `Index` that is not fresh never
  joins the heap. A pchannel with many vchannels
  does not compute a full minimum on each call.
- The value is monotonic. `Channel` keeps the largest value that it returned
  and never returns less. This clamp can report too much: an `Index` that
  joins the heap after a larger value was returned has an older
  `lastCommittedTimeTick`. Recovery must handle it before WAL truncation uses
  `LastAcked`. The tail triggers read the value without the clamp.

WAL truncation does not use `LastAcked` yet. See "Not done in this change".

## Triggers

A driver goroutine per pchannel runs a step each second. A step picks the
vchannels to hand over and submits each handover as a task to the node
scheduler (`nodescheduler`). An idle vchannel has no goroutine and no timer.

| # | Trigger | Purpose | Condition | Default |
|---|---|---|---|---|
| — | Pending request | Finish a started handover | The `Index` has a pending request and its backoff is over | — |
| 1 | Size | Bound the unpublished data of one vchannel, batch the uploads | `ActiveBytes` of the engine ≥ `handover.activeBytes` | 64 MiB |
| 2 | Early tail | Hand over before the WAL throttles appends | WAL recovery tail ≥ `handover.tailBytes` and `LastAcked` < the recovery time tick of the WAL: the dirty vchannel with the smallest `lastCommittedTimeTick` hands over | 2 GiB |
| 3 | Soft pressure | Fallback for trigger 2 | WAL recovery tail ≥ `walRecovery.tail.softWatermark` and `LastAcked` < the recovery time tick of the WAL: same pick as trigger 2 | 8 GiB |

Only a fresh `Index` whose claim and manifest lookup are done can hand over.

The condition on `LastAcked` makes sure that the index holds the WAL tail
back. The driver reads the recovery time tick (`RecoveryTimeTick` in the
recovery storage metrics) and the tail size from the same metrics. The
recovery time tick is the time tick of the WAL recovery checkpoint. When
`LastAcked` is at or above it, another component holds the tail. A handover
then does not shorten the tail. It only makes a small table each second.

The condition uses `LastAcked` without the clamp to the largest value that it
returned (see [LastAcked](#lastacked)). The clamp can hide an `Index` that
joined the heap later with an older `lastCommittedTimeTick`, and that `Index`
holds the tail back.

The condition works only while `LastAcked` is not part of the WAL recovery
checkpoint. When recovery adds it, the recovery time tick is never above
`LastAcked`, and triggers 2 and 3 never fire. Recovery must then compare with
the checkpoint of the other components, without the index.

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

**Why not by time.** WAL summary also never triggers by time. When the WAL
gets no writes, its tail does not grow, so a small idle vchannel costs
nothing. When it gets writes, trigger 2 covers it.

**Why 64 MiB and not smaller.** The value does not control memory. The engine
memtable size controls memory, and a full memtable flushes to a local table.
`ActiveBytes` counts those tables too. Triggers 2 and 3 bound the WAL length.
A smaller value only makes more small tables. Until compaction exists,
nothing merges them, so each extra table adds manifest entries, coordinator
memory, open files and lookups. With many vchannels, most vchannels never
reach 64 MiB and hand over through trigger 2.

## Failure handling

| Case | Handling |
|---|---|
| Retryable upload, download, claim, lookup or commit error (any error without a fatal sentinel: object storage, network, manifest service not ready, timeout, cancel) | Exponential backoff from 1 s to `handover.maxBackoff` (60 s), no limit on retries. Each failure reads `handover.maxBackoff` again. The pending request keeps the same table IDs. |
| Fenced (`snapshot.ErrFenced`) on the claim, a lookup or a commit | The `Index` stops its handovers. `OnFatal` marks the WAL unavailable: a newer owner has the pchannel. |
| Corrupted (`pkerr.ErrCorrupted`) in a handover | The `Index` stops its handovers. `OnFatal` marks the WAL unavailable. The WAL is assigned again and recovers on another node. |
| Corrupted (`pkerr.ErrCorrupted`) in a lookup on the write path | `Decide` returns the error and calls `OnFatal`, which marks the WAL unavailable. |
| Rejected claim, lookup item or commit (`snapshot.ErrRejected`: duplicate table, schema mismatch, regressed checkpoint, not claimed, an invalid request, a corrupted stored manifest), a returned manifest that does not have the flush in effect, or a manifest with another SST base path | `OnFatal` marks the WAL unavailable. There is no retry: the same request fails the same way again. A retry without a limit would only hold `LastAcked` back until the WAL rejects DML. |
| Missing object on download | `DownloadFile` marks it as corrupted. A manifest lists the table, so a retry can not clear it. |
| Local disk error when the index of an existing collection opens at WAL open | `OnFatal` marks the WAL unavailable. The streaming node and its other WALs keep running. |
| Concurrency | At most one handover per vchannel at a time. The node scheduler limits the handovers of the node. |

- `isFatal` checks only `snapshot.ErrFenced`, `snapshot.ErrRejected` and
  `pkerr.ErrCorrupted`. The adapter of the later change maps the merr codes
  of the manifest service to the sentinels (see [Committer](#committer)).
  The mapping relies on the manifest service to return
  `ServiceUnavailable` for every transient failure. A transient failure that
  arrives as `ServiceInternal` would end the WAL instance.
- The table store on the streaming node classifies its errors:

  | Failure | Error |
  |---|---|
  | Remote object storage (`PutObject`, a reader, the network read of a download) | `pkerr.ErrUnavailable`, retried |
  | Local disk (open, stat, temporary file, rename, sync, a write of the local chunk manager) | `pkerr.ErrIO`, retried |
  | Missing object on download | `pkerr.ErrCorrupted`, fatal |

- The backoff state belongs to the `Index`. The task returns no error to the
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
| `streaming.pkindex.handover.activeBytes` | `64m` | Yes, at the next driver step | Size trigger. |
| `streaming.pkindex.handover.tailBytes` | `2g` | Yes, at the next driver step | Early tail trigger. Keep it below `streaming.walRecovery.tail.lowWatermark`. |
| `streaming.pkindex.handover.maxBackoff` | `60s` | Yes, at the next failure | Longest wait between two attempts of a failed handover, claim or manifest lookup. Each failure reads it again. |
| `streaming.pkindex.memTableSize` | `4m` | No, read when an `Index` opens | Memtable size of the engine of one vchannel. |
| `streaming.walRecovery.tail.softWatermark` | `8g` | Yes, at the next driver step | Existing key. Soft pressure trigger. |
| `dataCoord.pkindex.gc.grace` | `86400` (s) | — | Key of the manifest service. The change that injects the `Committer` sets `ReuploadAfter` of the index layer to half of it. A retry uploads again after that age. |

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

Common patterns:

- A system freezes only at a point where all writes below the cut are in the
  frozen memtable, and records a structural bound. No system takes the
  maximum position of entries that apply concurrently. The epoch watermark
  follows this rule.
- Publish commits only the oldest contiguous prefix, with a monotonic check.
  The log can be truncated only after the remote manifest commit.
- Systems on object storage retry without a limit and keep the same file ID.
- Neon and TiKV trigger on replay distance, not on local data size. Trigger 2
  uses the same idea.

## Not done in this change

- **The adapter over the manifest service client and its wiring.** A later
  change, after the manifest service and this change merge, adds the
  adapter that implements `snapshot.Committer`. It translates the proto
  types and the merr codes (see [Committer](#committer)), with a test for
  each error class, and the streaming node injects it into the `Manager`. It
  also sets `ReuploadAfter`. Until then no `Channel` claims and no `Index`
  hands over.
- **End-to-end tests of the handover.** They run in the wiring change: a
  handover with object storage, an object storage outage, sustained writes
  with large vectors, and a check in the integration suite that a handover
  ran.

- **Recovery and replay after a WAL open.** On a WAL open, the index removes
  its local data from before and starts empty. It does not replay the WAL.
  The index has no restart durability yet.
- **A handover after a restart or a move.** A vchannel whose index is not
  fresh does not hand over until recovery exists (see
  [Only a fresh index hands over](#only-a-fresh-index-hands-over)).
- **`LastAcked` for WAL truncation.** `Channel.LastAcked` exists, but the WAL
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
- **A forced handover on flush or snapshot.** Flush and snapshot persist
  segment data. The index is a performance index, and WAL replay covers it. A
  forced handover would make the flush latency depend on uploads and RPCs of
  every vchannel.
- **GC that knows which tables a node still uses.** GC of the manifest service
  decides from the manifests only.

## Test plan

Unit tests cover these cases:

- Watermark: a writer with a small tt that applies late holds the watermark
  back (the counter-example above). A discard and a failed `Decide` release
  their writer.
- Handover: injected upload and commit failures. The request is pending
  before the upload, so a failed upload keeps it and the retry does not
  rotate again. A retry keeps the table IDs and sends the same request, also
  after a lost response. A retry after grace/2 uploads again. An empty
  handover retires its generations and moves `lastCommittedTimeTick`. An
  older manifest does not replace a newer one. A returned manifest below the
  installed version, with a checkpoint below the covered time tick, or with
  another SST base path, is rejected, and the generations stay.
- Install: a missing table is downloaded, and a missing object is fatal.
- Fresh only: an open with engine data from before removes the local data,
  starts empty and does not hand over. A file from before in `committed/`
  only, such as the temporary file of a download, has the same result. No
  manifest and no leftover is fresh, uploads under the SST base path that it
  computes, and creates the manifest at version 1 with its first handover. A
  manifest of any version, also version 1 with no tables, gives an `Index`
  that is not fresh and never calls `CommitFlush`. An `Index` that is not fresh is not in
  the `LastAcked` heap.
- Claim and lookup: one `ClaimPChannel` call for a `Channel` with many
  vchannels, and one `GetRecoveryManifests` call for the indexes of the
  existing collections. More than the batch size splits into several calls.
  A retry sends only the failed items. An `Index` of a new collection does
  not look up, also when it opens while the claim runs, and it waits for
  the claim. No handover before the claim. A retryable claim or lookup
  error retries. A fenced claim, a rejected claim, a rejected call and a
  rejected item are fatal.
- No `Committer`: the `Channel` does not claim, the `Index` does not hand over, stays out
  of the `LastAcked` heap, and still deduplicates the writes.
- Fatal errors: `ErrFenced`, `ErrRejected` and `ErrCorrupted` are fatal.
  Other errors are retryable, also a merr code that no adapter translated.
- Object key: `SSTKey` gives `<sst_base_path>/<id>.sst`.
- Duplicate CreateCollection: an `Add` of an `Index` that exists reports
  that it created nothing, so the rollback after a failed append does not
  remove the `Index` in use.
- Drop: DropCollection and the rollback of a failed CreateCollection remove
  the local directory, and a new create of the vchannel sees none of the old
  keys. A drop on a closed `Channel` fails and keeps the files.
- WAL open: a local disk error goes to `OnFatal` and does not panic. An
  existing collection with an unsupported primary key type panics.
- Table store: remote failures are `ErrUnavailable`, local disk failures
  (also a write of the local chunk manager) are `ErrIO`, and a missing object
  is `ErrCorrupted`.
- Triggers: size, early tail (picks the smallest `lastCommittedTimeTick`) and
  soft pressure. The tail triggers do not fire when `LastAcked` is at or
  above the recovery time tick of the WAL. They use `LastAcked` without the
  clamp.
- `LastAcked`: idle vchannels do not hold it back, and the heap minimum
  equals a full minimum.
- Failures: a fenced commit stops the handovers, a commit that the manifest
  service rejects is fatal, a retryable commit error backs off, and a lookup
  that finds corrupted data marks the WAL unavailable.
- Interceptor: without a `Manager`, it passes every message through. With a
  `Manager`, it uses the index.

The fake `Committer` of the tests follows the claim, lookup and flush rules of the
manifest service. The integration suite of the write path runs with the
index on, but no handover runs in it without a `Committer`.

Not run for this change: the end-to-end tests of the handover. They run in
the wiring change. Restart consistency needs recovery.

An end-to-end run with a crash and a large WAL tail is not done. It waits for
recovery.
