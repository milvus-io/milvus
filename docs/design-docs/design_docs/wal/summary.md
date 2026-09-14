# WALSummary Design

- Feature DRI: @tinszwy
- Primary Approver: @chyezh
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

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
recovery (RecoveryStorage)  -> walsummary
vchannel (VChannelRecoveryModule) -> walsummary
walsummary                  -> (no dependency on vchannel / transformlog)
```

The summary is organized per pchannel and internally groups its records by
vchannel:

```text
walsummary.Manager (one per pchannel)
  +-- pending: staged records of the current unsealed chunk span
  +-- pendingSealed: sealed chunks waiting for their object + manifest write
  +-- manifest: the chunk index of the current term
  +-- durableFrontiers: newest durable record timetick per vchannel
  +-- gcFrontiers: retention GC position per vchannel
  +-- pendingInvalidations: idempotency DDL tombstones awaiting publication
```

### 2.2 Objects (object storage)

Object keys are fixed-width, zero-padded decimals (`%020d`): lexicographic
order equals numeric order, so a prefix list returns chunks in generation
order without parsing. The width covers the full uint64 range (and every
non-negative int64); a wider value would silently break ordering, so the
width must never shrink. Chunks and manifests live in separate directories
(`chunks/`, `manifest/`) and carry no extension — the object types are
distinguished by directory and by the magic inside each object.

Chunk: one flush of one or more vchannels.

```text
<root>/walsummary/<pchannel>/chunks/<generation>_<term>
```

The key carries the term, so a fenced owner can never collide with the
successor's chunks.

Manifest: the chunk index of the current term.

```text
<root>/walsummary/<pchannel>/manifest/<term>
```

### 2.3 Term arbitration

The summary store owns no catalog (etcd) record of its own. Term arbitration
is split between two other mechanisms:

- the object keys are term-scoped (`<generation>_<term>`, `manifest/<term>`),
  so a superseded owner can never collide with the successor's chunks;
- the recovery layer claims the checkpoint term with compare-and-swap before
  restoring the summary. Later checkpoint writes carry that term, preventing
  a superseded publisher from advancing the checkpoint past unadopted chunks.

### 2.4 Protos

`PChannelSummaryManifest` indexes chunks and pending GC objects and carries
idempotency invalidation timeticks. Each `VChannelSummaryChunkIndex` has
independent `idempotency` (field 4), `inserts` (field 5), and `transform`
(field 6) section references. The first two sections are paired by position;
transform records are independently ordered by WAL timetick.

`transform_end_timetick` (field 7) bounds transform retention without waiting
for later inserts in the same chunk. Readers fall back to the vchannel span's
end for older transform-only chunks that do not carry this field.

Legacy per-vchannel formats (`VChannelTransformLogMeta`,
`TransformLogChunk`) are deprecated: the proto definitions are retained with
`Deprecated` markers, but no reader or migration code remains on this branch.

## 3. Lifecycle And Persistence

```text
Manager.ObserveMessage(immutable)
  -> build idempotency/insert records for keyed inserts
  -> with EnableTransform: also build delete transform records
  -> stage records beyond the durable frontier; retain no WAL message handle
RecoveryStorage.persistDirtySnapshot
  -> Manager.Persist(ctx): seal pending, write chunk, publish manifest
  -> save the recovery snapshot and fenced consume checkpoint
```

`Persist` is synchronous. It writes all staged records and DDL invalidations
before the checkpoint that covers them is committed. A persistence failure
is retried by recovery and prevents checkpoint advancement. Observation does
not schedule a background flush, and there is no separate summary `LastAcked`
frontier or `RequestFlushThrough` API in this implementation.

The active recovery path enables only the idempotency consumer. The
TransformLog/VChannel modules are available for the later recovery integration;
that caller must opt into `ManagerConfig.EnableTransform`, restore transform
GC frontiers, and feed the module's recovery window. This keeps the current
idempotency path from accumulating unconsumed deletes. Asynchronous recovery
persistence remains future work.

DDL invalidations affect idempotency records only. Staged transform records,
including transforms sharing a transaction with an insert, remain until they
are persisted and their materialization or cleanup frontier is durable.

## 4. Retention GC

`Manager.GCOnce` releases the oldest chunks above `RetentionMaxBytes` or
`MaxRetainedChunks`. The active idempotency recovery path supplies
`streaming.idempotency.maxRetainedBytes` and
`streaming.idempotency.maxRetainedChunks`. Each zero value disables that bound.

A chunk with a transform section stays pinned until every transform-bearing
vchannel has a GC position at or above its `transform_end_timetick`. Vchannels
with only inserts do not pin transform retention. These bounds are soft:
retention cannot discard unmaterialized deletes to satisfy the budget.

`RestoreTransformGCTimeTicks` initializes GC positions from durable VChannel
metadata when `PChannelRecoveryManager` is constructed. Missing metadata does
not prove cleanup and leaves records pinned. `AdvanceGCTimeTick` advances the
frontier only after the corresponding snapshot is durable; dropped/tombstoned
channels use `DroppedVChannelTimeTick` (`math.MaxUint64`).

Released objects first move into the manifest's `pending_gc` queue and are
deleted only after publication. The target branch's retired-term sweep also
collects objects left by superseded writers when retention retires their term.

## 5. Consumers: TransformLog

The idempotency consumer is wired into RecoveryStorage and reads the insert
and idempotency sections. [TransformLog](transform_log.md) is the additional
consumer module, with production recovery wiring still pending. It is decoupled
from the summary:

- on the write path it only materializes the vchannel's transform records into
  DataCoord-managed L0 segments — it owns no persistent buffer, no chunk
  objects, and no catalog metadata;
- its persistence and recovery rely entirely on the WAL plus the summary: the
  committed materialization frontier rides in
  `VChannelMeta.transform_materialized_time_tick` (persisted with the vchannel
  catalog snapshot), and its in-memory window is rebuilt once on recovery via
  `Manager.ReadTransformEntries(vchannel, materializedTimeTick, +inf)` — the
  only read of the summary store in the consumer path;
- it never triggers persistence and never waits for it: L0 materialization and
  WALSummary persistence are **not** ordered against each other, and the
  summary never delivers flush events. Neither position loses data on crash —
  materialization commits only after its L0 output is in object storage, and
  un-materialized records are rebuilt from the retained chunks
  (see [TransformLog](transform_log.md) §7 Recovery).

## 6. Recovery And Term Takeover

The recovery layer fences the consume checkpoint **before** summary recovery.
`Manager.Restore(ctx)` changes no catalog records:

1. read the current term's manifest and probe its unrecorded chunk tail;
2. if necessary, list prior manifest terms and inherit the most recent
   non-empty term, including its probed tail;
3. publish the inherited index under the current term and restore per-vchannel
   durable frontiers;
4. continue chunk generations after the inherited set.

A term publishes a manifest before its first chunk so successors can discover
it even if it crashes before publishing the chunk index. Probing stops at a
hole or corrupt tail. The transform module's caller separately restores its
GC frontiers from VChannel metadata and reads its durable backlog once.

Chunk keys are term-scoped, so different owners write different objects. A
same-key rewrite succeeds only when its contents match; otherwise it reports
corruption. If the encoding differs but the contents match, the stored footer
and size are returned so manifest offsets continue to describe the stored bytes.
