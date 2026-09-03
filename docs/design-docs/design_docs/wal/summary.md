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
  +-- pending: staged transform records of the current unsealed chunk span
  +-- pendingSealed: sealed chunks waiting for their object + manifest write
  +-- manifest: the chunk index of the current term
  +-- durableFrontiers: newest durable record timetick per vchannel
  +-- gcFrontiers: retention GC position per vchannel
  +-- lastAcked: the summary's own confirmation frontier
```

### 2.2 Objects (object storage)

Chunk: one flush of one or more vchannels.

```text
<root>/streamingnode/summary/<pchannel>/chunks/chunk.<gen>.term<term>.psc
```

The key carries the term, so a fenced owner can never collide with the
successor's chunks.

Manifest: the chunk index of the current term.

```text
<root>/streamingnode/.../<pchannel>.manifest.<term>
```

### 2.3 Term arbitration

The summary store owns no catalog (etcd) record of its own. Term arbitration
is split between two other mechanisms:

- the object keys are term-scoped (`chunk.<gen>.term<term>`, `<pchannel>.manifest.<term>`),
  so a superseded owner can never collide with the successor's chunks;
- the consume-checkpoint advancement is NOT yet fenced on this branch: the
  checkpoint is the last-write commit point of the snapshot, and a superseded
  older-term publisher can still overwrite it. The intended compare-and-swap
  (fenced-commit) design lands together with the recovery async refactor.

### 2.4 Protos

```proto
message PChannelSummaryManifest { repeated PChannelSummaryChunkIndexEntry chunks = 1; }
message PChannelSummaryChunkIndexEntry {
    uint64 generation = 1;
    uint64 start_timetick = 2;
    uint64 end_timetick = 3;
    uint64 object_size = 4;
    repeated VChannelSummaryChunkIndex vchannels = 5; // per-vchannel section offsets
}
message PChannelSummaryChunkFooter { int64 term = 1; ... }
message VChannelSummaryTransformRecord { uint64 time_tick = 1; TransformDeleteEntry delete = 2; }
```

Legacy per-vchannel formats (`VChannelTransformLogMeta`,
`TransformLogChunk`) are deprecated: the proto definitions are retained with
`Deprecated` markers, but no reader or migration code remains on this branch.

## 3. Lifecycle And Persistence

```text
Manager.ObserveMessage(retained)
  -> classify the message; only delete-carrying messages produce a record
  -> build TransformLogEntry (standalone proto, message NOT retained)
  -> if record timetick > durable frontier of its vchannel: stage into pending
  -> if pendingBytes >= FlushMaxBytes: requestSeal
write task (summary-owned decision)
  -> seal pending into a chunk (generation)
  -> write chunk object + publish manifest
  -> advance durableFrontiers and lastAcked   (WAL checkpoint may now advance)
```

The summary alone decides when persistence happens:

| Trigger | Path |
|---|---|
| size threshold | `FlushMaxBytes` (staging binary size, configured as `FlushL0MaxSize`) |
| forced persist | `Manager.RequestFlushThrough(tt)` (tracker stall / pressure) |

There is no barrier trigger: external write APIs (flush / flush-all /
manual-flush / drop / truncate) never force a flush. Their semantics still
hold — the checkpoint cannot advance past a delete until the covering chunk is
durable, so a flush request waits on the tracker path
(`RequestFlushThrough`) for whatever the summary has not persisted yet.

The summary retains **no WAL message handle**: it copies the record into the
pending buffer and keeps only its WAL position. It advances its own
confirmation frontier (`lastAcked`) strictly after the chunk object AND the
manifest record are durable, and the recovery storage merges this frontier
with the ack tracker's completed point when persisting a snapshot, so the
global WAL checkpoint can never outrun an un-durable delete record:

```text
WAL checkpoint <= durable summary frontier
```

## 4. Retention GC

`Manager.GCOnce` releases chunks above `RetentionMaxBytes`
(`streaming.summary.maxBytesPerPChannel`, default 4 GB), bounded below by the
per-vchannel GC positions (`gcFrontiers`): records not yet consumed by a
dependent component must never be released. A chunk is releasable only when
every vchannel it covers has a GC position at or above the chunk's end
timetick, so a chunk that still holds a not-yet-consumed record is never
released whatever the budget pressure.

The GC positions are restored by `Manager.Restore` from the vchannel metas
(the persisted materialization frontier, or `DroppedVChannelTimeTick` for a
dropped/tombstoned vchannel) and advanced at runtime by
`Manager.AdvanceGCTimeTick` — with `walsummary.DroppedVChannelTimeTick`
(= `math.MaxUint64`) when a vchannel's cleanup snapshot is durable, releasing
everything of that vchannel regardless of consumption.

## 5. Consumers: TransformLog

[TransformLog](transform_log.md) is the **first** VChannel-level consumer of
the summary, and today the only one. It is deliberately decoupled from the
summary:

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

`Manager.Restore` is read-only with respect to the catalog:

1. read the manifest of the own term; if absent, probe forward for chunks of
   the own term and seal them into a fresh manifest;
2. on a term handoff, walk back past empty intermediate terms (a term can be
   assigned and die before ever sealing a manifest) and inherit the most
   recent non-empty earlier term's index, so un-consumed records of the
   superseded owner stay reachable; seal the union into the own manifest;
3. restore the per-vchannel durable frontiers from the manifest and the GC
   positions from the vchannel metas;
4. the manifest is the durable chunk index for the live flush path.

The takeover of the checkpoint happens in the recovery layer, after
`Manager.Restore` seals the inherited manifest. The consume checkpoint is
currently a plain last-write commit point, **not** a compare-and-swap: a
superseded older-term publisher can still overwrite it. The intended
fenced-commit design lands together with the recovery async refactor.

Write arbitration across terms: an `Exist -> Write` of a chunk key is not
atomic; on a byte mismatch the footer is decoded — a footer term greater than
the own term fences the writer, a smaller term is overwritten, an equal term
with identical content is an idempotent retry, and an equal term with
different content is corruption.
