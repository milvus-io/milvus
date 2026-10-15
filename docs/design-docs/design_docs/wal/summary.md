# WALSummary Design

- Feature DRI: @tinszwy
- Primary Approver: @chyezh
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

**Status:** The asynchronous write, recovery and local GC workflow is implemented
and wired into RecoveryStorage, including independent backlog checks and
checkpoint gating and startup idempotency-window restoration (§7).
The existing object-key encoding is retained; §8 describes forward
generation-prefix discovery and its recovery cost.
The cross-owner GC protocol is not yet designed; see the TODO in §9.
The shared bounded-read contract (§5.4) and [L0Materializer](l0_materializer.md)
implementation is retained for future wiring, including range statistics and Summary-owned
materialization-backlog requests described in
[L0Materializer §5](l0_materializer.md#5-read-and-materialize). TransformLog
subscriptions (§5.5) are a separate future integration.

The protocol added by this feature is still under development. Intermediate
branch versions are not compatibility targets: removed draft messages and
fields are deleted without reservations, and Transform indexes must provide
the current section boundaries and statistics.

**Current runtime:** [WAL L0 Materializer](l0_materializer.md) retains Delete
handles for legacy query recovery. The [Summary consumer](summary_l0_materializer.md)
is retained for future QueryView wiring; the two implementations are not run together.

## 1. Core Purpose

WALSummary is the WAL consumer-side summary of a physical WAL channel: it
centrally stores the brief fields of the WAL that downstream features need
(for example primary keys, idempotency, TimeTick, and Delete transform records).
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
RecoveryStorage             -> walsummary (sole record observation/storage)
RecoveryStorage             -> vchannel (owns SegmentViews and L0Materializer)
vchannel/l0materializer     -> walsummary read interface
TransformLog adaptor        -> walsummary read interface (future)
walsummary                  -> no dependency on its consumers
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
  +-- readable coverage / change version: complete readable prefix (§5.4)
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
  covered WAL TimeTick interval, retained even when the chunk set becomes empty.

The manifest's `coverage: SummaryCoverage` stores the last continuously covered
`generation` and `term` together with `[start_time_tick, end_time_tick]` of the
complete summarized WAL interval. It is progress metadata, not an object
reference. Removing even every retained chunk leaves coverage intact. An empty
manifest is authoritative; it must never resurrect an older retained set.

All persisted Summary ranges use inclusive endpoints. Chunk footer and chunk
index `start_time_tick/end_timetick` describe complete WAL coverage `[start,end]`,
including keyless inserts and payload-free barriers. A new chunk starts at the
previous covered end plus one, so consecutive chunks satisfy
`next.start == previous.end + 1`. The initial start is the recovery checkpoint
TimeTick plus one, or 1 when starting from the beginning. TimeTick zero is not a
valid coverage start; a range containing one TimeTick has `start == end`.
No successor exists after the maximum TimeTick.

Per-VChannel and Transform indexes also use closed ranges, but their endpoints
are the actual first and last stored record TimeTicks. Coverage starts must not
be narrowed to the first stored record: messages without Summary payload still
belong to the covered WAL interval. Read APIs retain the exclusive cursor
contract `(after, through]`, allowing the next page to start after the previous
`CoveredThrough` without arithmetic on the cursor.

There is no additional covered position or physical message ID in Summary.
`LastAcked()` returns only a TimeTick; RecoveryStorage's
Tracker selects a completed WAL checkpoint at or below it. Only WALCheckpoint
persists the physical replay position.

`transform_fast_forward_time_tick[vchannel]` records the last retired Delete
boundary. GC advances it with each removed Transform index's end TimeTick and
publishes it atomically with reference removal, before physical deletion.
Materialization alone does not advance it. Readers explicitly report a
fast-forward when a cursor precedes the boundary; skipped history is never
reported as a proven empty interval. L0 must reject a fast-forward beyond its
durable materialized position as inconsistent recovery state.

Each VChannel index has paired idempotency/inserts sections and a separate
`VChannelSummaryTransformIndex { ref, start_time_tick, end_time_tick, total_size }`.
The Transform range contains actual Delete records; total_size counts logical
entry bytes. No per-entry statistics are persisted or retained in a separate
runtime index. Full sections provide exact size; boundary sections provide
bounds and are read asynchronously when exact capacity admission is needed.
DDL does not invalidate executed-request history.

### 2.5 Chunk Format And Read Validation

The format is shared by all consumers; the idempotency API contract remains in
[Idempotent Write](../20260604-idempotent_write.md). A chunk has this layout:

```text
16-byte header: "PSCCH001" | version | header size
per-vchannel payload sections:
  idempotency: {key, row_offsets}[]                    (optional)
  inserts:    {message_id, timetick, last_confirmed, ids}[]
  transform:  {timetick, delete blocks}[]              (optional)
protobuf footer: pchannel, generation, term, complete WAL TimeTick span,
  per-vchannel section indexes
SHA-256 of the exact footer bytes
4-byte footer length | "PSCFT001"
```

Each section reference records its offset, length and record count. The manifest
copies the stored footer's indexes and object size, so readers can select a
vchannel and section without a separate index lookup. Idempotency annotations
and insert facts are paired by position and sorted together; when an idempotency
section exists, its record count must match the insert section. Transform records
have their own ordering and do not participate in that pairing. Separating the
sections avoids storing primary keys twice and permits an insert-only consumer
to ignore idempotency annotations.

Payload sections have no self-describing boundaries or separate checksums.
Readers validate the header/version, footer framing and checksum, section
bounds, protobuf decoding and record counts. The checksum covers the stored
footer bytes, never a re-marshaled proto. Manifest objects use a separate
`PSMF0001` frame with version, payload length, protobuf payload and SHA-256 of
that payload. Corruption is reported rather than converted into an empty view.

The layout supports ranged reads, but the current store reads a whole chunk
and decodes the requested sections. `ReadIdempotencySectionsOfChunk` shares that
read across requested vchannels. True section-only object reads remain an
optimization; an index entry alone does not make transfer cost constant.

Retries of an immutable chunk key accept identical bytes or equivalent decoded
records and coverage. If encodings differ but content is equivalent, the store
returns the existing object's footer and size: new offsets must never be paired
with old object bytes. Different content at the same key is corruption. This
retry check is not an atomic create-if-absent protocol and does not permit two
writers to share an assignment term; the fresh-term rule in §2.3 still applies.

## 3. Lifecycle And Persistence

```text
ObserveMessage(immutable), in WAL TimeTick order
  -> copy keyed insert and delete transform records
  -> seal ordered spans and assign chunk sequences before upload
  -> upload chunks concurrently; completion may be out of order
  -> extend the continuous durable prefix only across completed uploads
  -> mark eligible manifest changes dirty and submit to NodeScheduler
NodeScheduler manifest publication task
  -> overwrite this term's manifest with a complete index snapshot
  -> maintain LastAcked within the prefix that recovery can reconstruct
```

### 3.1 Ordered Publication After Concurrent Uploads

`FlushMaxBytes` triggers sealing when staged bytes reach the threshold.
`Manager.Run` independently checks WAL recovery-tail pressure once per second
and requests a flush through the observed frontier under soft pressure. This
works even when AckTracker has no incomplete entries or no new messages arrive.
AckTracker stall requests target only VChannel consumers and never seal Summary.
Neither `dataNode.segment.syncPeriod` nor the checkpoint persistence interval
is a Summary sealing trigger. Small low-traffic batches may remain in memory,
backed by WAL, and pin the global checkpoint until size or tail pressure requires
persistence. This does not weaken the Summary confirmation bound.

Sealed chunks and dirty manifests continue on their own scheduler retry paths;
periodic pressure checks do not manufacture completion or bypass the first-manifest
requirement. Empty backlog produces no new chunk.
Observation retains no source WAL handle and performs no object-storage I/O.
In the target read integration, Summary installs records and their complete
readable coverage before VChannel observation can advance L0Materializer's
requested window. This ordering does not wait for upload or publication.
The caller owns the scheduler lifetime. The existing convention that a zero
`FlushMaxBytes` disables size-triggered sealing is unchanged.

For the future Summary consumer, Summary also governs **materialization backlog**: Deletes not yet consumed into L0, whether pending, sealed or already
persisted. An upload does not discharge this work. Backlog governance may issue
a coalesced, bounded progress request to the VChannel owner even with an empty
pending buffer and no new WAL traffic. It must account for recovered retained
history and distinguish work needing L0 output from output awaiting durable
VChannel metadata. The latter needs metadata publication, not duplicate output.
An ordinary Summary seal/upload is not automatically an L0 flush request.
L0Materializer introduces no age/idle timer; long-standing unmaterialized data
is handled through this Summary-owned mechanism. `Manager.Run` performs both
persistence and consumption checks. Consumption age uses the original WAL
physical time of the earliest outstanding Delete and the worker's existing
materialization age budget, separate from Summary sealing. The current WAL
consumer does not wire this callback or age budget. Retention pressure requests
only the oldest blocking chunk.
`ReportMaterialized` suppresses redundant consumption before metadata is saved;
only `AdvanceGCTimeTick` from durable metadata authorizes release.
See [L0Materializer §5](l0_materializer.md#5-read-and-materialize).

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

A terminal Summary error pins confirmation and stops further observation and
readable-coverage advancement. All terminal transitions, including sealing
invariant failures, notify the owning RecoveryStorage once outside Summary
locks. The owner marks the WAL unavailable through its existing fatal handler;
it does not synchronously close the WAL from the failing task. Ordinary storage
outages remain retryable and normal shutdown does not report a fatal failure.

### 3.2 Confirmation And The First Publication Of A Term

`LastAcked` exposes a continuous TimeTick that the specified recovery algorithm
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

### 3.4 Configuration And Checkpoint Integration

`ManagerConfig` supplies `FlushMaxBytes`, `RetentionMaxBytes`,
`MaxRetainedChunks`, and the scheduler/runtime. WALSummary and Delete transform
recording are always active; neither has an enable switch. A zero flush
threshold disables size-triggered sealing; a zero retention bound disables that
bound. `RequestFlushThrough` can still request progress independently of size.
RecoveryStorage supplies `FlushL0MaxSize` as the staging threshold and
`SummaryMaxBytesPerPChannel` as the retained-byte budget. It does not yet pass `MaxRetainedChunks`, so the count bound is disabled
in production wiring even though the standalone manager supports it.
The old idempotency-specific retained-byte and chunk-count settings are removed;
retention of this shared store is controlled by `streaming.summary.maxBytesPerPChannel`.

Publication is scheduled independently of the RecoveryStorage checkpoint tick.
The integration must combine its own completed frontier with `LastAcked()`;
it must not publish or truncate beyond the summary's recoverable TimeTick.
Different TimeTicks can have the same safe LastConfirmedMessageID, so comparing
only message IDs does not establish this bound. A physical replay position and
logical coverage TimeTick serve different purposes.

An object-storage failure pins confirmation until retry succeeds; it does not
require synchronous object writes inside a checkpoint transaction. WAL-level
backpressure handles accumulation (§8.3). Persistence and GC observability must
track this asynchronous progress. The old `idempotency_persist_total` and
`idempotency_pending_gc_chunks` descriptions from the synchronous design are
not implemented metrics of the standalone manager and do not define its API.

## 4. Retention GC

This section states the agreed retention and deletion-ordering contracts.
Cross-owner GC coordination remains TODO in §9.

Retention removes the oldest eligible chunks from the retained index. Existing
byte/count budgets and transform-consumer frontiers determine eligibility.
Bytes bound storage volume; chunk count bounds index entries and per-object
read overhead that can grow even with many tiny chunks. Budgets apply to whole
pchannel objects, not per-vchannel slices. A GC-eligible chunk remains retained
while the configured retention budgets are not exceeded. This preserves readable
history after materialization without promising a duration: there is no TTL or
minimum retention time.
Either budget can request release, but neither overrides a transform consumer
that still needs the oldest chunk.
Unmaterialized transform records cannot be discarded merely to meet a budget;
missing consumer metadata does not prove cleanup. Restored metadata always
uses its persisted materialization frontier for GC. New VChannel tombstones
are published only after L0 completes through Drop, with that frontier captured
in the snapshot; the state flag does not replace the frontier. A cleaned-up VChannel retains its
durable tombstone until the recovery-authoritative manifest no longer retains
its Delete history. `CanCleanupVChannel` also requires confirmed observation
through its cleanup boundary and completed manifest publication; an in-memory
retirement is insufficient. This keeps the materialization/GC frontier
recoverable across restart without adding a manifest GC work queue. Physical
object deletion can finish after the tombstone is removed.

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
Its scan cost is outside the Restore path. The local implementation pins reader
snapshots with `readMu` and captures a fully published retained index and coverage
under `publishMu`. Acquiring the exclusive reader lock proves that readers of
older snapshots have finished. Scheduler tasks yield for retry rather than wait
for either lock. GC releases both locks before object listing and deletion,
using the frozen index and coverage throughout the sweep: retired references
cannot reappear in this manager, previous-term references are inherited only
during Restore, and new current-term generations beyond the captured coverage
are excluded even if uploaded or published during the sweep. New readers
therefore cannot reference candidates, while new publication proceeds during
slow deletion. Garbage is rediscovered in bounded deletion rounds. These rules
protect only one manager; cross-owner deletion safety remains the TODO in §9.

The manifest's published coverage boundary never moves backward when retention
removes chunks, including the last chunk. Recovery only adopts objects after
that boundary, so an old object awaiting deletion cannot re-enter the index.
An existing empty manifest is authoritative and must not cause fallback to an
older manifest that still references retired objects.

L0Materializer's GC position advances only after its corresponding VChannel
metadata is durable. L0 materialization and Summary persistence remain
independent. Before future subscriptions are enabled, the integration must also
supply the minimum historical start point required by retained QueryViews,
DataViews, and protected local replays. The effective release position is the
minimum of those requirements and the durable materialization/cleanup position;
subscription delivery cursors are not retention acknowledgements. Unknown
requirements during recovery keep history pinned until they are reconstructed.
Summary accepts storage retention constraints, not QueryView-specific types.

The read contract also requires a durable fast-forward boundary (§5.4).
Reference removal and that boundary must be published consistently, so restart
cannot report removed history as a successfully read empty interval. Local read
pins protect in-progress I/O; view-level requirements protect future reads.

After term T's complete manifest is successfully published, manifests with
terms strictly less than T can be deleted asynchronously. Keep T's manifest:
it is the recovery root, including when its retained chunk set is empty. This
cleanup does not require copying or deleting older-term chunks that T still
references. It runs after normal publication and through background retries,
never during Restore, and does not wait for those chunks to expire. The manifest
directory should normally contain only a handful of objects, ideally one after
cleanup. Failed cleanup or repeated interrupted handoffs can temporarily leave
more objects; discovery must still consider all of them.

## 5. Consumers

### 5.1 Idempotency

[Idempotent Write](../20260604-idempotent_write.md) owns key derivation, duplicate
responses and per-vchannel window eviction. WALSummary stores the committed
facts needed to reconstruct those responses: the key and original row offsets
from the annotation, plus primary keys and original WAL positions from the
paired insert record. A committed transaction contributes its committed write
unit, not independent dedup entries for its uncommitted bodies.

`ReadIdempotencyEntries` and `ReadIdempotencyEntriesOfVChannels` select entries
by vchannel and TimeTick across retained chunks and in-memory records. The
consumer rebuilds its window from these records and applies its own memory
budget. WALSummary does not persist window membership or a per-key eviction
cursor. It does keep runtime consumer frontiers where retention safety requires
them, as for L0Materializer. DDL does not invalidate executed-request history;
replicated writes do not contribute foreign keys to the local dedup view.

The current observer stages insert/idempotency pairs for keyed writes. A
keyless insert does not by itself populate a general primary-key history; a
future insert-only consumer would need to provide that observation policy.
A primary-key index requiring full history would also need a retention contract
beyond the bounded idempotency tail.

### 5.2 Summary L0 Consumer (Future Runtime Wiring)

[L0Materializer](l0_materializer.md) consumes the transform section directly.
It observes WAL messages only to merge a requested materialization boundary;
it keeps no copied record window. Work requires a capacity trigger, an explicit
completion request after related L1 flushes, or a Summary backlog request;
the L1 safety bound alone is insufficient. Once admitted, it reads a bounded
range from Summary, writes/registers L0 output, and updates
`VChannelMeta.transform_materialized_time_tick` through the VChannel owner.
Recovery restores that cursor and rebuilds the requested boundary through
ordered replay, including RecoveryBarrier; it does not preload Delete history.

The transform section contains Delete payloads only. Payload-free Barriers
advance readable coverage and the materializer's requested boundary, but are
not staged or written as records. Pure Inserts produce no transform entry;
general Summary coverage may still pass their positions.

Summary owns persistence, manifest publication and LastAcked. The materializer
reports its release position only after the corresponding VChannel snapshot is
durable. Both full and base-only snapshots must participate in that callback.
This release position is only one input to shared-store retention.

### 5.3 Consumer Lifecycle

WALSummary is a permanent PChannel component. It records Delete transforms
regardless of request-level idempotency, and records local keyed writes when
an explicit IK is present. There are no global, collection or transform enable
switches. Keyless inserts do not create idempotency records or clear existing
request history.

The standalone `RemoveAllObjects` helper is destructive maintenance, not a
feature-toggle or corruption-recovery workflow. Repair must preserve the
history required by every consumer.

### 5.4 Transform Read Contract

This is the shared storage contract required by L0Materializer now and the
future TransformLog adaptor. Exact Go interface names remain an implementation
choice; the semantic result is:

```text
ReadTransform(vchannel, after, through, row/byte limit)
    -> Entries, CoveredThrough, ReadableThrough, FastForwardTimeTick, Changed
ReadableProgress() -> coverage and change token
WaitForChange(token)
```

The reader merges retained durable chunks, sealed records awaiting publication,
and the pending tail into one ordered VChannel view. Memory-backed records are
already backed by WAL; reading them does not authorize WAL truncation or require
waiting for Summary uploads. `LastAcked` retains its separate durability meaning.

The contract is:

1. Return Delete entries strictly in `(max(after, FastForwardTimeTick), CoveredThrough]`, ordered by
   source WAL TimeTick, with `CoveredThrough <= through`.
2. CoveredThrough proves every Delete in that interval has been included.
   Page limits stop at a complete Entry boundary; a Txn uses its outer TimeTick
   and all its Delete children. One oversized Entry may exceed a soft limit.
3. A proven empty interval may advance CoveredThrough. An empty result without
   coverage progress does not prove catch-up or completion of the requested
   range. Never infer coverage from the last payload or the requested end.
4. Capture disk indexes and in-memory records consistently with readable
   progress. A concurrent pending/sealed/durable transition cannot leave a
   record in neither half or return it twice.
5. A cursor before retained history is explicitly fast-forwarded. Return
   `FastForwardTimeTick` with the last retired Delete boundary, and read only
   after that boundary. `CoveredThrough` remains capped by the requested and
   readable ends; it describes coverage after accounting for this explicit
   skip, not proof that retired history was empty. Persist the per-VChannel
   `transform_fast_forward_time_tick` with reference removal, even when the
   last chunk is removed. L0 rejects any fast-forward beyond its materialized
   cursor. Future subscription adaptors must expose the skip to their caller.
6. Missing or corrupt referenced objects fail the read; an absent VChannel
   section means an empty interval only within known complete retained coverage.
7. Pin a read's required objects against local deletion. Pins have bounded read
   lifetimes, not the lifetime of an external stream. Cross-owner GC still
   requires the protocol in §9.

Readable coverage advances only after the ordered input prefix has been fully
accounted for in Summary. This includes applicable payload-free PChannel
messages such as RecoveryBarrier: skipping record creation must not skip their
coverage effect. Non-persisted heartbeats do not establish new coverage.
Recovered coverage comes from validated continuous stored coverage plus ordered
WAL replay; client cursors and requested endpoints never create coverage.

A change token is captured consistently with progress. Notifications wake
consumers to recheck state, avoiding a missed update between reading and waiting;
they do not carry record ownership or subscription delivery guarantees. The
revised materializer re-evaluates admission after observation, L1 completion,
and Summary backlog requests. Notifications alone do not force L0 output. Future subscriptions use progress notifications to follow
the tail without adding another WAL observer.

Summary owns any decoded cache and shared object-fetch coordination. Cache
memory must be bounded independently of total retained history. PChannel
objects can contain many VChannels; reuse reads where possible instead of
fetching the same object for each subscriber. Section indexes do not imply
section-only I/O: the current Store downloads the whole chunk (§2.5).

### 5.5 Future TransformLog Adaptor

[TransformLog](transform_log.md) wraps §5.4 to provide local and remote
subscriptions. It has no ObserveMessage, independent storage, or L0 execution.
Entry/SyncUp delivery, resume cursors, stream backpressure, and QueryView
consumer integration are outside this PR. The storage interfaces must not
require those components to exist for L0 materialization to run.

Before enabling subscriptions, wire the additional history retention constraints
in §4 and preserve the [WAL-view handoff](streamingnode_vchannel_wal_view.md).
L0 completion alone is insufficient to release history required by those readers.

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
   a higher-sequence object does not prove its predecessors exist. An absent
   predecessor stops adoption; corrupt chunk data fails recovery.
   Neither allows adopting later objects, and transient I/O errors must not be
   treated as proof of absence.
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

When bootstrapping without manifest coverage, InitLastAcked also seeds the
record-deduplication floor from the published global checkpoint. Physical replay
may start earlier at LastConfirmedMessageID; records at or below this floor
must not enter a chunk whose coverage starts after it. This floor never creates
stored history or a manifest coverage range. Transform reads expose this initial
boundary via FastForwardTimeTick; after publication the coverage start preserves
it across restarts.

If no manifest exists, there is no manifest-based recovery root. Chunks under
unpublished terms do not independently authorize checkpoint advancement; rebuild
from the safe WAL recovery position. If that WAL is unavailable, fail recovery
rather than treating the state as a successfully recovered empty history.

The source manifest's term and the current writer term can differ. Merely
restoring under a newer assignment does not synchronously create a new manifest
object. The first scheduled publication establishes that term's complete index,
as in §3.2.

### 6.1 Crash And Failure Cases

| Failure or interruption | Required behavior |
| --- | --- |
| Chunk uploaded before the term has a manifest | Do not advance confirmation based on it; recover from WAL if the term never becomes discoverable. |
| Chunk uploaded beyond the published boundary | Adopt only the selected term's continuous tail, then schedule the amended manifest. |
| Gap before a later completed upload | Stop at the gap; later generations do not advance the recovered position. |
| Manifest PUT interrupted | Readers must see a complete old or new object; retry dirty publication, never a partial index. |
| Manifest ahead of the external checkpoint | Replay may overlap; restored coverage suppresses staging already summarized records. |
| Transient LIST, GET or PUT failure | Return or retry the error; do not infer absent data or permit checkpoint advancement past undurable records. |
| Corrupt newest manifest or corrupt tail candidate | Fail recovery; do not fall back to an older manifest or silently skip corrupt data. |
| Missing/corrupt retained chunk during consumer loading | Fail the read and consumer recovery; never fabricate an empty history. Restore validates the index without eagerly reading every retained chunk. |
| Reference removal published before physical deletion | Garbage may remain; a later background sweep rediscovers it. |
| GC interrupted after some deletes | Retry with the authoritative index; absent objects need no further work. |
| Manifest from a newer assignment | Reject the stale writer. |

A same-term reopen may read recovered data but cannot seal new chunks (§2.3).
Neither corruption nor missing WAL is repaired by silently discarding summary
history. Operational repair must account for every consumer, not just turn off
idempotency and remove the shared prefix.

### 6.2 Checkpoint Ownership And Truncation

WALSummary creates no etcd key. Its caller owns `WALCheckpoint` and must claim
it with the new assignment term before reading summary storage, leaving its
position unchanged. Otherwise an old publisher could advance the checkpoint
between the new owner's probe and claim, covering records the new owner never
adopted and making their WAL unavailable for replay.

The catalog rejects older terms and guards an existing checkpoint with a value
CAS over its serialized value. First creation uses a version-zero CAS; read-back
verification detects a rejected or ambiguously completed guarded write. Every
later publication carries the owner's term. This is a checkpoint-publication
fence, not an object-store deletion fence.

Checkpoint publication satisfies the Summary confirmation bound in §3.4. Summary storage
stalls can therefore pin WAL truncation even while the append side still makes
progress. Backend retention behavior and WAL backpressure remain outside
WALSummary; the replay interval must remain available until safely summarized.

### 6.3 Storage Lifetime And Failure Boundaries

- Object identity is scoped by pchannel and assignment term, without a separate
  cluster-incarnation identifier. Resetting etcd while retaining the bucket is
  not a supported reset procedure. A higher-term manifest causes rejection,
  rather than being adopted by a lower-term writer; that check alone cannot
  distinguish every reuse of a previous incarnation's names and terms.
- Checkpoint CAS does not fence every component write in a large recovery
  snapshot. The catalog's multi-batch fallback can write component metadata
  before the guarded final commit; full cross-owner component fencing belongs
  to the metastore/recovery integration.
- Losing a guarded publication does not guarantee one uniform public
  superseded-owner error across backends. The catalog verifies the stored value;
  callers must handle a failed or ambiguous publication without advancing their
  own persisted frontier. The old assertion that every lost CAS necessarily
  retries until context timeout no longer describes the catalog implementation.
- Background garbage scans and old-manifest cleanup may lag or fail. Their
  cost is not part of the 100-generation discovery stride, and cross-owner GC
  safety remains unresolved (§9). Elapsed time or retention crossing a term is
  not proof that a previous owner has stopped deleting objects.

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

RecoveryStorage supplies ordered observation and scheduler lifetime, combines
AckTracker completion with `LastAcked`, and runs Summary backlog checks
independently of Tracker stalls and catalog retries.
At the startup RecoveryBarrier, RecoveryStorage populates
`RecoverySnapshot.SummarySnapshots` from all retained idempotency sections and
records staged or sealed during WAL replay, without waiting for their uploads.
It enumerates VChannels from Summary, including history absent from the current
write path, and reads the whole retained range rather than filtering by the
WAL checkpoint. A single multi-VChannel read avoids fetching each chunk once
per VChannel. The interceptor rebuilds its windows and applies its byte cap
before the WAL accepts appends. A read or decode failure fails WAL open rather
than admitting writes with an incomplete deduplication window.

`ReadTransform` captures durable indexes, sealed records, pending records and
readable coverage under the same lock. It returns caller-owned whole entries,
`CoveredThrough`, `ReadableThrough`, and a change channel. Row/byte limits are
soft for one oversized Entry; decoding holds one chunk section at a time.
Local read pins protect the captured objects against physical GC.
`ReadTransformEntries` remains an uncapped convenience wrapper; production L0
consumption uses bounded reads exclusively.

RecoveryStorage observes Summary before VChannel modules. The current WAL L0
consumer holds Delete handles and rebuilds its buffer from WAL replay. It
reports in-memory materialization completion and durable GC positions as before;
both full and base-only VChannel snapshot commits use their captured frontier.
The manifest persists transform fast-forward TimeTicks, even after the last
chunk is removed.

The retained Summary consumer implements capacity/API/backlog admission, bounded
reads, and L1-final-commit safety as described in its separate design. The
current RecoveryStorage does not wire Summary's materialization-request callback;
Summary persistence, confirmation and GC still run independently.

`TransformStats(vchannel, after, through)` returns lower/upper logical-byte
bounds from section totals spanning hot, sealed and durable records. The
manifest stores one Transform index per VChannel section, with its actual
Delete range and total size. Pending records use one aggregate per VChannel;
sealing transfers that aggregate without maintaining per-entry metadata.
Fully included sections contribute exact totals; partial sections contribute
only to the upper bound. L0 resolves uncertain admission by bounded async reads.
Summary backlog similarly resolves a partial section's oldest remaining Delete
with a bounded read in its existing worker. Object I/O never enters Observe.

Count-budget wiring remains absent (§3.4). Future subscription retention and
cross-owner GC fencing remain separate follow-up work.

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
   the first missing predecessor and fail on corrupt data; later uploads cannot
   bridge either condition.
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
or admission-control interface. Its confirmation frontier constrains checkpoint
publication, and its independent backlog worker observes WAL tail pressure.
Staged records participate even when there are no active uploads or Tracker
entries. Tail accounting currently uses observed logical bytes; it does not
include unobserved scanner lag or directly bound the retained transform window.

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

The earlier idempotency design published `pending_gc` before deleting exact
`{generation, term}` objects and serialized manifest updates within a manager.
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

## 10. Validation And Source Map

The standalone tests cover chunk/manifest framing and checksums, section
alignment and bounds, immutable retry equivalence, concurrent upload ordering,
coalesced manifest publication and retries, restore without inline writes,
continuous tail probing across numeric-prefix boundaries and mixed terms,
empty-manifest coverage, same-term writer rejection, byte/count retention,
transform GC frontiers, reader pins, failed-deletion rediscovery and retention
across restart. Backlog tests cover source Ack followed by silence,
pressure-triggered sealing, absence of age-triggered sealing, and cancellation.
Recovery tests cover retained
idempotency history before the checkpoint, staged/sealed replay at the barrier,
and WAL-open failure on unreadable history; interceptor-builder tests verify
that recovered keys return the original append result without another append.
These tests do not establish distributed GC safety.

Key source files, relative to the repository root:

- `internal/streamingnode/server/wal/walsummary/{manager,async}.go`: observation,
  sealing, scheduling, confirmation and consumer reads.
- `internal/streamingnode/server/wal/walsummary/{store,recover,gc}.go`: object
  format, manifest discovery, recovery and local garbage collection.
- Tests in that package, especially `store_test.go`, `recover_test.go`,
  `async_test.go`, `workflow_test.go` and `transform_test.go`.
- `pkg/proto/streaming.proto`: authoritative manifest, coverage and section fields.
- `internal/metastore/kv/streamingnode/update.go` and `checkpoint_cas_test.go`:
  checkpoint CAS, term validation and ambiguous-write verification.
- [Idempotent Write](../20260604-idempotent_write.md#test-coverage): consumer/API
  tests and integration gaps.
