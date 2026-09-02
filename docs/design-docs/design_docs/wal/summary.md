# WALSummary Design

- Feature DRI: @tinszwy
- Primary Approver: @chyezh
- Independent Approver: @weiliu1031
- Design Review: 2026-07-29

WALSummary is the **pchannel-scoped** summary of the transform records (Delete
and Txn-Delete) observed on a physical WAL channel. It is the single owner of
transform-record persistence and plays the role of a pchannel-level
"SegmentView" for transform data: records are staged in memory, persisted to
object storage under a summary store, retained within a budget, and released
through the same MessageRef lifecycle as every other recovery component.

## 1. Scope And Dependencies

```text
recovery (RecoveryStorage)  -> walsummary
vchannel (VChannelRecoveryModule) -> walsummary
walsummary                  -> (no dependency on vchannel / transformlog)
```

The transform consumer of each vchannel (see
[TransformLog](transformlog/transform_log.md)) is deliberately decoupled from
the summary: it observes the vchannel's messages directly and materializes at
its own pace, while the summary persists at its own pace. Neither drives the
other — in particular, no barrier or external write API forces a summary flush
or a materialization. There is no ordering constraint between L0
materialization and WALSummary persistence; recovery reconciles them (below).

## 2. Object Model

### 2.1 Objects (object storage)

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

### 2.2 Term arbitration

The summary store owns no catalog (etcd) record of its own. Term arbitration
is split between two other mechanisms:

- the object keys are term-scoped (`chunk.<gen>.term<term>`, `<pchannel>.manifest.<term>`),
  so a superseded owner can never collide with the successor's chunks;
- the consume-checkpoint advancement is NOT yet fenced: the checkpoint is the
  last-write commit point of the snapshot, and a superseded older-term
  publisher can still overwrite it (see checkpoint-persistence.md §5.1 for the
  TODO and the intended compare-and-swap design).

### 2.3 Protos

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
ObserveMessage(retained)
  -> build TransformLogEntry (standalone proto)
  -> if record timetick > durable frontier: append to view staging (retained.Clone)
  -> if stagingBytes >= FlushMaxBytes: requestFlush
flush task (summary-owned decision)
  -> collectStaging of every view
  -> write chunk (generation)
  -> publish manifest
  -> release handles            (WAL checkpoint may now advance)
```

The summary alone decides when persistence happens:

| Trigger | Path |
|---|---|
| size threshold | `FlushMaxBytes` (staging binary size, configured as `FlushL0MaxSize`) |
| forced persist | `SummaryView.RequestPersistThrough(tt)` (tracker stall / pressure) |

There is no barrier trigger: external write APIs (flush / flush-all /
manual-flush / drop / truncate) never force a flush. Their semantics still
hold — the checkpoint cannot advance past a delete until the covering chunk is
durable, so a flush request waits on the tracker path
(`RequestPersistThrough`) for whatever the summary has not persisted yet.

Handle release happens only after the chunk object AND the manifest record are
durable, so the global WAL checkpoint — which advances past a message only
when every retained handle is released — can never outrun the summary:

```text
WAL checkpoint <= durable frontier (of the summary)
```

L0 materialization is **not** ordered against the durable frontier: the
transform consumer may be ahead of (records observed but not yet persisted) or
behind (records persisted but not yet materialized) the summary at any moment.
Neither position loses data on crash: materialization commits only after its
L0 output is in object storage, and un-materialized records are rebuilt by
recovery from the retained chunks (see
[TransformLog](transformlog/transform_log.md)).

## 4. Decoupling From The Transform Consumer

The summary delivers no flush events: there is no `FlushListener` /
`FlushedBatch`. The transform consumer of a vchannel observes the same message
stream through the vchannel module and keeps its own materialization window.
The only interaction between the two components is the materialization
frontier mirror (`Manager.SetMaterializedTimeTick`), which bounds the summary
retention from below, and the recovery-time restore
(`Manager.SetDurableTimeTick`), which tells a fresh summary view where the
manifest's coverage starts so replay does not re-stage already-durable
records.

## 5. Recovery And Term Takeover

`Manager.Recover` is read-only with respect to the catalog:

1. read the manifest of the own term; if absent, probe forward for chunks of
   the own term and seal them into a fresh manifest;
2. on a term handoff, walk back past empty intermediate terms (a term can be
   assigned and die before ever sealing a manifest) and inherit the most
   recent non-empty earlier term's index, so un-materialized records of the
   superseded owner stay reachable; seal the union into the own manifest;
3. the manifest is the durable chunk index for the live flush path.

The takeover of the checkpoint happens in the recovery layer, after
`Manager.Recover` seals the inherited manifest. The consume checkpoint is
currently a plain last-write commit point, **not** a compare-and-swap: a
superseded older-term publisher can still overwrite it. See
checkpoint-persistence.md §5.1 for the TODO and the intended fenced-commit
design.

After recovery the recovery path restores each vchannel's durable frontier
(`Manager.DurableTimeTick`, the largest per-vchannel chunk index end in the
manifest) into its summary view via `Manager.SetDurableTimeTick`, before the
vchannel modules build their views. WAL replay then re-observes records the
manifest already covers; the view skips them, so the same records are never
staged and rewritten into new chunks.

Write arbitration across terms: an `Exist -> Write` of a chunk key is not
atomic; on a byte mismatch the footer is decoded — a footer term greater than
the own term fences the writer, a smaller term is overwritten, an equal term
with identical content is an idempotent retry, and an equal term with
different content is corruption.

## 6. Retention GC

`Manager.GCOnce` releases chunks above `RetentionMaxBytes`
(`streaming.summary.maxBytesPerPChannel`, default 4 GB), bounded below by the
per-vchannel materialization frontiers (`SetMaterializedTimeTick`): records
not yet materialized must never be released. The materialization frontier
mirrored from the transform consumer is a hard lower bound of the retention.
