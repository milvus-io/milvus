# WALSummary Design

- Feature DRI: @tinszwy
- Primary Approver: @chyezh
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** The write, recovery and GC contracts below are the agreed target
architecture. The current implementation has not yet been migrated to them;
see §7. The existing object-key encoding is retained; §8 describes forward
generation-prefix discovery and its recovery cost.
The cross-owner GC protocol is not yet designed; see the TODO in §9.

## 1. Core Purpose

WALSummary is the WAL consumer-side summary of a physical WAL channel: it
centrally stores the brief fields of the WAL that downstream features need
(for example primary keys, idempotency, TimeTick, and TransformLog entries).
It exists for two reasons:

1. **Log compression.** Keeping only these brief fields instead of the whole
   raw WAL history shrinks the log size and lets the WAL checkpoint advance;
   features no longer need to replay a large amount of WAL, which would make
   fault recovery slow.
2. **VChannel-level lazy loading.** The summary is stored centrally at
   VChannel granularity, so any VChannel-level component can be recovered
   lazily from the retained window on demand, instead of from the raw WAL.

## 2. Organization

### 2.1 Scope And Dependencies

```text
future async RecoveryStorage -> walsummary
vchannel (VChannelRecoveryModule) -> walsummary
walsummary                  -> (no dependency on vchannel / transformlog)
```

The summary is organized per pchannel and internally groups its records by
vchannel:

```text
walsummary.Manager (one per pchannel)
  +-- pending: ordered records not yet sealed
  +-- upload state: sealed chunks and their independent upload completions
  +-- continuous durable frontier: the prefix with no missing chunk
  +-- manifest: retained chunk/section index and data coverage boundaries
  +-- manifest dirty state: changes awaiting normal publication
  +-- durableFrontiers: per-vchannel replay filtering positions
  +-- gcFrontiers: consumer progress used to decide retention
  +-- lastAcked: the continuous, recoverable summary confirmation position
```

Upload completion, dirty bookkeeping and GC work belong to runtime state.
They are not serialized into the manifest.

### 2.2 Objects (object storage)

There are two object types, under a root scoped by pchannel:

- immutable chunks, identified by writer term and sequence;
- one complete manifest per term, overwritten by subsequent publications
  within that term.

A new term's manifest contains the complete inherited index plus its adopted
and newly published data. It references previous-term chunks directly; neither
copying chunk objects nor traversing a manifest chain is required.

The design retains the current paths and fixed-width, zero-padded 20-digit
decimal numbers:

```text
<root>/walsummary/<pchannel>/chunks/<generation>_<term>
<root>/walsummary/<pchannel>/manifest/<term>
```

Recovery lists a small manifest directory and probes successive generation
prefixes after the manifest's published boundary. No reverse encoding, new
directory layout or StartAfter support is required; see §8.

### 2.3 Term arbitration

WALSummary owns no catalog (etcd) key. The integration obtains the current
assignment term and fences recovery-checkpoint advancement before restoring
summary state. If discovery finds a manifest from a newer assignment than the
caller's, the caller must not proceed as the writer.

Chunk keys include their writer term. Assignment handoff must also prevent a
superseded owner's GC from deleting objects still needed by the successor;
the unresolved protocol is tracked separately in §9. A same-term reopen must not
reuse an object key for different content, including objects beyond a recovered
sequence gap. A new writer term or an explicit immutable-key reuse protocol is
required. The implementation permits same-term recovery for reading, but
refuses to seal new chunks after such a reopen. Writers must receive a fresh
assignment term on every reopen, including attempts that left no manifest.

### 2.4 Manifest And Sections

The manifest describes the retained data organization and accelerates access:

- chunk identities, sizes and TimeTick ranges;
- per-vchannel section locations and record counts;
- the monotonically advancing published sequence boundary and corresponding
  covered WAL position, retained even when the chunk set becomes empty.

The coverage boundary distinguishes unpublished tail objects from objects
already removed by retention but not yet physically deleted. It describes
which WAL prefix has been summarized; it is not a GC task or retry state.
Exact proto names for this coverage metadata remain an implementation detail.
The manifest contains no `pending_gc` queue or DDL invalidation markers.

Each `VChannelSummaryChunkIndex` has independent `idempotency`, `inserts`, and
`transform` section references. The first two sections are paired by position;
transform records are independently ordered by WAL TimeTick. DDL does not
invalidate the history of executed requests.

`transform_end_timetick` bounds transform retention without waiting for later
inserts in the same chunk. Physical deletion remains subject to the consumer's
durable materialization or cleanup frontier.

## 3. Lifecycle And Persistence

```text
ObserveMessage(immutable), in WAL TimeTick order
  -> copy keyed insert and optional delete transform records
  -> seal ordered spans and assign chunk sequences before upload
  -> upload chunks concurrently; completion may be out of order
  -> extend the continuous durable prefix only across completed uploads
  -> mark eligible manifest changes dirty and submit to NodeScheduler
NodeScheduler manifest publication task
  -> overwrite this term's manifest with a complete index snapshot
  -> maintain LastAcked within the prefix that recovery can reconstruct
```

### 3.1 Ordered Publication After Concurrent Uploads

`FlushMaxBytes` and `RequestFlushThrough` trigger sealing and scheduling.
Observation retains no source WAL handle and performs no object-storage I/O.
The caller owns the scheduler lifetime. The existing convention that a zero
`FlushMaxBytes` disables size-triggered sealing is unchanged.

Chunk sequence order follows the ordered input stream, not upload completion
order. An upload completing at sequence N makes N eligible for the manifest
only when every predecessor after the published boundary has completed too.
Every successful chunk upload checks whether it extends that prefix. For
example, if 101, 103 and 104 have reached S3 but 102 has not, a manifest
previously ending at 100 may advance only to 101. When 102 completes, the
continuous frontier can advance through 104 and one manifest update can
include all three newly eligible chunks. An out-of-order completion that does
not extend the continuous prefix creates no manifest change by itself.

Each eligible chunk is immutable and fully accessible before its reference is
published. Same-term manifest publications must be serialized, or protected by
conditional writes, so an older snapshot cannot overwrite a newer one.
Transient failures retry the same immutable chunk content. A failed predecessor
pins the continuous frontier; later completed uploads cannot bypass it.

### 3.2 Confirmation And The First Publication Of A Term

`LastAcked` exposes a continuous position that the specified recovery algorithm
can reconstruct. A copied/released source message or an isolated completed
upload does not establish this property. Non-record messages can extend the
confirmed span once preceding record-bearing messages are safely covered.

Restore does not perform a manifest PUT inline or wait for publication. Once
its in-memory state is initialized, a dirty manifest is submitted directly to
NodeScheduler as described in §3.3. The first publication under the current
term must contain the full inherited and adopted index, together with any
eligible new chunks.

Until this first publication succeeds, external checkpoint advancement must
not depend on new chunks written under this term: recovery discovers the term
through its manifest and probes only that manifest's term. Chunks under a term
with no manifest are left out by that recovery algorithm and must remain
replayable from WAL. Restore does not wait for this scheduled write.

After a term has a discoverable manifest, any confirmation beyond its published
boundary must still be reconstructible as a continuous tail under §6. Neither
a sequence gap nor work under an undiscoverable term can advance confirmation.

DDL does not erase idempotency records or create manifest persistence work.
A delayed retry with the same key must not execute again merely because
truncate or drop removed the original data. New logical writes use new keys.

### 3.3 Dirty Manifest Scheduling And Coalescing

Marking the manifest dirty directly ensures that a manifest publication task
is submitted to NodeScheduler. This applies after recovery has installed its
state, when a chunk upload extends the continuous frontier, and when retention
changes the retained index. It does not depend on a new chunk arriving, another
flush request, or a periodic external check. Restore enqueues the task without
performing or waiting for its object-storage write; task execution may proceed
asynchronously once the restored state is ready.

Only one manifest publisher per manager may execute at a time. Further dirty
updates join an already queued or running task instead of creating concurrent
PUTs. The publisher captures the latest eligible complete index and its runtime
revision; several chunk completions may therefore share one manifest PUT.
Snapshot capture and same-term publication ordering must also serialize GC
edits so a stale snapshot cannot reintroduce a deleted reference.

A successful PUT acknowledges only the captured revision. If another update
arrived during I/O, the task publishes the remaining dirty state before it
finishes. Task completion and the decision to submit a successor are coordinated
with dirty-state updates so no wakeup is lost. A transient failure preserves
dirty state and retries through NodeScheduler even if no more chunks arrive.
Pending-work reporting includes dirty manifests and their tasks. Runtime task
and revision bookkeeping is never serialized into the manifest.

Recovery applies the same continuous-prefix rule as upload completion. Finding
later chunks, even the final generation of a scanned prefix, does not authorize
skipping a missing predecessor. Only the validated continuous tail is included
in the restored dirty manifest submitted to the scheduler.

## 4. Retention GC

This section states the agreed retention and deletion-ordering contracts.
Cross-owner GC coordination remains TODO in §9.

Retention removes the oldest eligible chunks from the retained index. Existing
byte/count budgets and transform-consumer frontiers determine eligibility.
Unmaterialized transform records cannot be discarded merely to meet a budget;
missing consumer metadata does not prove cleanup.

GC has no persistent work queue inside the manifest:

1. compute a retained index without the objects to release;
2. publish that manifest successfully before deleting any released object;
3. protect readers still using a previous manifest snapshot;
4. asynchronously delete objects that are no longer needed.

A crash after step 2 leaves garbage, not a missing referenced object. A later
background sweep may rediscover garbage from the object set and authoritative
retained index. The sweep must distinguish obsolete objects from in-flight or
recoverable unpublished tails, respect reader lifetimes, and obey term
ownership. It must not delete every unreferenced object indiscriminately.
Its scan cost is outside the Restore path; the concrete sweep and reader
protection mechanisms remain implementation work.

The manifest's published coverage boundary never moves backward when retention
removes chunks, including the last chunk. Recovery only adopts objects after
that boundary, so an old object awaiting deletion cannot re-enter the index.
An existing empty manifest is authoritative and must not cause fallback to an
older manifest that still references retired objects.

Consumer GC positions advance only after their corresponding VChannel metadata
is durable. Transform materialization and summary persistence remain independent.

After term T's complete manifest is successfully published, manifests with
terms strictly less than T can be deleted asynchronously. Keep T's manifest:
it is the recovery root, including when its retained chunk set is empty. This
cleanup does not require copying or deleting older-term chunks that T still
references. It runs after normal publication and through background retries,
never during Restore, and does not wait for those chunks to expire. The manifest
directory should normally contain only a handful of objects, ideally one after
cleanup. Failed cleanup or repeated interrupted handoffs can temporarily leave
more objects; discovery must still consider all of them.

## 5. Consumers: TransformLog

Idempotency readers use the insert and idempotency sections.
[TransformLog](transform_log.md) consumes the transform section. Its interface
with WALSummary consists of reading transform entries when needed, rebuilding
its materialization window from summary data during recovery, and supplying a
GC position after its materialization metadata is durable.

The existing read interface is
`Manager.ReadTransformEntries(vchannel, materializedTimeTick, +inf)`. The
committed consumer frontier is carried by
`VChannelMeta.transform_materialized_time_tick`. Summary persistence, manifest
publication and LastAcked are owned by WALSummary and its recovery integration;
TransformLog does not define or drive that protocol. The next async
RecoveryStorage PR wires the read/recovery/GC interactions between the modules.

## 6. Recovery And Term Takeover

`Restore` reconstructs state through reads, without inline manifest PUTs, object
deletions or catalog writes. After installing that state it submits a dirty
manifest to NodeScheduler, without waiting for publication. The caller
establishes assignment ownership and checkpoint fencing beforehand.

1. **Discover the manifest.** Scan the manifest namespace and choose the largest
   term. Read and validate that complete manifest. An existing empty manifest
   is a valid result; a corrupt newest manifest is an error, not permission to
   fall back to an older index.
2. **Discover its unpublished tail.** Starting at the generation after the
   manifest's published coverage boundary, list successive numeric generation
   prefixes as specified in §8. Filter objects to the selected manifest's term
   and generations beyond that boundary. Validate the candidate chunk indexes and
   incorporate only the continuous prefix extending that boundary. Encountering
   a higher-sequence object does not prove its predecessors exist. An absent or
   corrupt predecessor prevents adopting later objects; transient I/O errors
   must not be treated as proof of absence.
3. **Build runtime state.** Combine the manifest and recoverable tail into an
   in-memory manifest. Record the continuous summary coverage and per-vchannel
   frontiers. Mark the manifest dirty when adoption changes its content or the
   current writer term needs its first publication; submit the publication task
   directly to NodeScheduler (§3.3). The greatest discovered TimeTick beyond a
   gap is not a safe continuation position.
4. **Resume observation.** Feed WAL messages logically after the reconstructed
   summary TimeTick through `ObserveMessage`. The caller must start the physical
   WAL read at a safe MessageID/LastConfirmedMessageID, preserve transaction
   assembly, and filter by TimeTick. Other recovery modules may require earlier
   replay; the summary frontier does not replace their replay positions.

If no manifest exists, there is no manifest-based recovery root. Chunks under
unpublished terms do not independently authorize checkpoint advancement; rebuild
from the safe WAL recovery position. If that WAL is unavailable, fail recovery
rather than treating the state as a successfully recovered empty history.

The source manifest's term and the current writer term can differ. Merely
restoring under a newer assignment does not synchronously create a new manifest
object. The first scheduled publication establishes that term's complete index,
as in §3.2.

## 7. Implementation And Integration Status

WALSummary now implements the standalone workflow described above: independent
concurrent chunk tasks, continuous-prefix confirmation, versioned dirty-manifest
publication on NodeScheduler, and recovery through the newest manifest plus
100-generation prefix scans. Coverage metadata survives retention of an empty
chunk set. Restore performs no inline writes; it enqueues publication when the
recovered state is dirty or the new term needs its first manifest.

Manifest protos contain data indexes and coverage, with no `pending_gc` queue.
Local GC waits for reference-removal publication and active reader snapshots,
then deletes asynchronously and rediscovers failed deletions on retry. Older
manifests are removed after the current term's publication. The cross-owner GC
protocol remains TODO in §9; local locking is not distributed exclusion.

Legacy RecoveryStorage has no WALSummary wiring. The later async integration
must provide ordered observation and scheduler lifetime, combine AckTracker
completion with summary `LastAcked`, restore idempotency and transform consumer
windows, and advance consumer GC positions only after their metadata is durable.

## 8. Object Listing And Recovery Cost

### 8.1 Manifest Discovery

List the manifest directory and select the largest term. Prompt cleanup after
successful publication (§4) keeps this directory small in normal operation;
reverse term encoding is unnecessary. Follow listing pagination when needed:
cleanup backlog must not make discovery miss the newest manifest.

### 8.2 Forward Generation-Prefix Discovery

Keep `chunks/<generation>_<term>` unchanged. Divide the generation space into
fixed batches of 100 values by dropping the last two decimal digits from
the zero-padded generation string. With the existing 20-digit encoding, the
LIST prefix is the chunk directory plus the first 18 digits, without an
underscore or term suffix.

The stride of 100 controls the generation range of each prefix scan, not the
number of objects returned per request. It reduces the usual metadata volume
per prefix scan compared with a stride of 1,000. Keep the storage client's LIST
page-size setting unchanged; do not set MaxKeys to 100 for this strategy.
Concurrent writes from different terms can create multiple objects at the same
generation. Follow pagination for the whole prefix and retain only chunks whose
term matches the selected latest manifest's term. A larger term suffix on a
chunk does not change the recovery term selected through manifest discovery.

Let G be the manifest's published generation boundary, retained independently
of the live chunk set. Start with the batch containing G + 1, or generation 0
if no generation has been covered yet. For each batch:

1. List that prefix, following all pages needed to establish its candidate set.
2. Parse keys, keep only the selected manifest's term and generations at or
   after the next expected generation, and order them numerically.
3. Read and validate chunks in consecutive generation order. Stop adoption at
   the first missing or corrupt predecessor; later uploads cannot bridge it.
   Listing failures are errors, not evidence of a missing generation.
4. Only if the continuous recovered prefix reaches this batch's last generation
   proceed to the next numeric prefix. Seeing the last object alone is
   insufficient when an earlier generation is missing. Stop at the numeric
   type's maximum rather than wrapping around.

For example, with generation 1001 and term 42, the actual encoding is
`%020d_%020d`. All paths below are relative to
`<root>/walsummary/<pchannel>/`:

```text
manifest/00000000000000000042
  published generation: 00000000000000001001
  last covered chunk: chunks/00000000000000001001_00000000000000000042

LIST prefix: chunks/000000000000000010
  generation range: 00000000000000001000 .. 00000000000000001099
  filter term: 00000000000000000042
  first expected chunk: chunks/00000000000000001002_00000000000000000042
  continue only after recovering consecutively through:
    chunks/00000000000000001099_00000000000000000042

next LIST prefix: chunks/000000000000000011
  first expected chunk: chunks/00000000000000001100_00000000000000000042
```

For example, the first prefix may return both
`chunks/00000000000000001002_00000000000000000042` and
`chunks/00000000000000001002_00000000000000000043`. With the selected manifest
at term `00000000000000000042`, only the first object is a recovery candidate.
An object in another term cannot fill a missing generation in the selected term.

If the published generation is already `00000000000000001099`, start directly
at prefix `chunks/000000000000000011`. If generation `00000000000000001003`
is missing in the selected term while `00000000000000001004` and
`00000000000000001099` exist, stop at `00000000000000001002` and rebuild the
rest from the safe WAL replay position. No backward scan or full-history LIST
is needed. The same-term immutable-key rule in §2.3 still applies to later
objects left beyond such a gap.

### 8.3 Cost And Bounds

A prefix spans at most 100 **generation values**, not necessarily 100
objects: the term suffix allows multiple objects at a generation after
interrupted handoffs. Filtering by the selected term is mandatory, and the
listing must support multiple pages. S3's per-request key limit is separate
from this numeric batch size; see the
[S3 ListObjectsV2 API](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html).

Normally there are few manifests and a short unpublished tail, so recovery
needs only a small number of prefix scans independent of the total historical
chunk count. If the continuous tail contains U generations, it touches at most
`ceil(U / 100) + 1` batches, including the batch where it stops. Prefix scans
return object metadata; chunk content reads are needed only for tail candidates,
not for already covered generations in the first batch.

WAL owns backpressure, including limiting accumulation when uploads or manifest
publication stall. WALSummary does not introduce a separate backpressure policy
or admission-control interface. Its progress and pending-work state participate
in that later WAL integration. The integration must account for the whole
unpublished backlog, not just actively uploading tasks.

Discovery cost still depends on manifest cleanup backlog, old-term objects
sharing a batch, unpublished tail length and loaded index size. Normal
publication, background cleanup and WAL-level backpressure determine those
bounds. The numeric prefix stride alone is not a fixed bound on recovery work.

Chunk footer range reads may reduce tail transfer cost without changing key
encoding. Full manifest loading, consumer-window loading and WAL catch-up still
depend on their data sizes; the prefix discovery strategy alone does not make
all of those operations constant-time.

## 9. GC Design: Cross-Owner Coordination (TODO)

**TODO:** Define the GC protocol across term handoff. The retention rules in §4
do not by themselves prevent an old owner from deleting objects referenced by
its successor. No cross-owner protection mechanism has been selected yet.

### Race To Resolve

1. The new owner reads manifest T, which still references chunk C.
2. The old owner publishes its own retention update removing C, then deletes C.
3. The new owner publishes manifest T+1 from its earlier snapshot, retaining
   the reference to C. Its manifest now points to a missing object.

The same deletion can also race the successor's recovery reads before it
publishes a manifest.

### Existing Mechanisms And Their Limits

The idempotency PR publishes `pending_gc` before deleting exact
`{generation, term}` objects and serializes manifest updates within a manager.
Checkpoint term fencing prevents stale checkpoint advancement, but does not
fence S3 deletions or protect readers on another node.

Its argument that an old owner's retention boundary is more conservative does
not establish that the successor has already removed those references. An
object being eligible for release is different from its removal being reflected
in every manifest or recovery snapshot still using it.

### Design Work Remaining

- Define when the old owner must stop GC and how outstanding deletions are
  accounted for during handoff.
- Protect the successor's manifest discovery, consumer reads and inherited
  manifest publication from concurrent deletion.
- Specify crash and retry behavior for that protocol. A term check immediately
  before deletion alone leaves a check/delete race.

The solution must preserve the agreed manifest contract: it describes data
organization and coverage, without a persisted GC work queue. This TODO does
not select a new catalog key, locking mechanism or deletion delay.
