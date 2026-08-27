# DataNode task placement on CPU and memory (3.0)

Date: 2026-08-27
Branch: `enhance/datanode-cpu-memory-slots-3.0` (base `upstream/3.0` @ `b4870e2a34`)
Supersedes the approach in PR #52561.

## Goal

Extend the single scalar "slot" DataCoord uses to place tasks on DataNodes into
two dimensions, CPU and memory, so a memory-heavy task is never placed on a
worker that cannot hold it and compute-heavy tasks are spread instead of packed.

Non-goals:

- DataNode does not estimate anything and does not gain any admission logic.
  Its execution limits (compaction pool, `buildParallel`, import pool) are
  unchanged.
- The existing scalar slot path is not removed. It stays as the compatibility
  tier for workers or coordinators that predate this change.
- No rollback switch: the two-dimensional path only engages when both sides
  speak it, and the scalar path is always there underneath.

## Architecture

```
DataCoord                                          DataNode
task.GetTaskResource() -> {cpu, memory}  --(each request carries cpu+memory)-->  ledger.Accept(id, cpu, mem)
        |                                                                          ...task runs under the existing pools...
        v                                                                          ledger.Release(id)
pickNode (2-D)  <--(QuerySlot: total/available cpu+memory)--                       available = total - sum(accepted)
```

- Estimation lives only in DataCoord: every task family implements
  `GetTaskResource() taskcommon.Resource` next to the existing `GetTaskSlot()`.
- DataNode only does bookkeeping: whatever cpu/memory the request carries is
  added on accept and subtracted on completion, at exactly the points where the
  scalar `usingSlots` is added and subtracted today. A request without the
  fields (old coordinator) books zero.
- The old scalar chain (`GetTaskSlot`, `slot_usage`/`task_slot`,
  `available_slots`, the max-heap `pickNode`) is untouched.

## Wire changes (fixed fields, appended at the end of each message)

| message | new fields |
|---|---|
| `datapb.QuerySlotResponse` | `int64 total_cpu`, `int64 available_cpu`, `int64 total_memory`, `int64 available_memory` (memory in bytes) |
| `datapb.CompactionPlan` | `int64 cpu`, `int64 memory` |
| `workerpb.CreateJobRequest` (index), `workerpb.AnalyzeRequest`, `workerpb.CreateStatsRequest` | `int64 cpu`, `int64 memory` |
| `datapb.PreImportRequest`, `datapb.ImportRequest`, `datapb.CopySegmentRequest` | `int64 cpu`, `int64 memory` |

`RefreshExternalCollectionTaskRequest` is not extended: the DataNode does not
count that task in its slot report today (QuerySlot sums index, compaction and
import only) and that stays as is. DataCoord still charges it 1 CPU / 64MB
within a scheduling round.

## DataCoord estimation

`taskcommon.Resource{CPU int64; Memory int64}` — CPU in whole cores, memory in
bytes.

| task | CPU | memory |
|---|---|---|
| vector index | 8 | fieldSize x 2 |
| scalar index | 1 | fieldSize x 2 |
| stats (TextIndex / BM25 / JsonKeyIndex) | 1 | segmentSize x 2 |
| sort compaction | 1 | segmentSize x 2 |
| mix compaction / bump schema version | 1 | `dataCoord.segment.maxSize` (1024MB) |
| L0 compaction | 1 | sum(deltalog) x 2, floor 64MB |
| clustering compaction | 8 | 32GB |
| analyze | 8 | rows x dim x elemSize x 2 |
| import / preimport | 1 | existing `taskBufferSize` (base x vchannels x partitions; L0 import uses deleteBufferSize; preimport uses base) |
| copy segment / refresh external collection | 1 | 64MB |

Every memory estimate is clamped to at least `minTaskMemory` (64MB) so that a
task never reports zero and gets treated as free to place anywhere.

### fieldSize / segmentSize fallback

V3 segments do not persist per-field binlog KVs (`kv_catalog.go`: "V3 segments
persist paths via the LOON manifest"), so after a DataCoord restart
`segment.getFieldBinlogSize(fieldID)` returns 0, and external-collection
segments also lack `Stats`. Only when the binlog-derived size is 0 (warn log):

- vector field: `rows x dim x elemSize` (closed form, valid on every storage
  version)
- scalar field: `segmentSize x (field share of typeutil.EstimateSizePerRecord(schema))`
- `segmentSize` itself is 0: `rows x EstimateSizePerRecord(schema)`

### Configuration (`dataCoord.taskResource.*`, refreshable, in milvus.yaml)

| key | default |
|---|---|
| `vectorIndexCPU` | 8 |
| `analyzeCPU` | 8 |
| `clusteringCompactionCPU` | 8 |
| `defaultCPU` | 1 |
| `indexMemoryFactor` | 2 |
| `statsMemoryFactor` | 2 |
| `l0CompactionMemoryFactor` | 2 |
| `analyzeMemoryFactor` | 2 |
| `clusteringCompactionMemory` | 32GB |
| `minTaskMemory` | 64MB |

The estimate is cached once on the task object (same pattern as
`slotUsage.Load()` today) and the request builder ships the same value the
scheduler saw: cached on the task for the nine families that walk meta, a pure
function of three job fields for import/preimport. Either way, what was placed
and what was shipped are the same number.

A family that cannot resolve its inputs — nil schema, missing segment, empty or
invalid index type — returns the floor and is **not** cached, so the next
scheduling round retries instead of freezing a placeholder for the task's
lifetime. A field that is genuinely absent from a schema we DO have is a real
answer, not a miss: it is priced at the whole segment (conservative) and cached.

## DataNode ledger and report

- No separate `internal/datanode/resource` package. Each executor keeps its own
  `taskcommon.Resource` counter right beside the `usingSlots` it already keeps,
  with the identical lifecycle: booked where `usingSlots` is added, released
  where it is subtracted, and the release subtracts exactly what was booked
  rather than a re-derived value. That is the compaction executor
  (`usingResource`), the index task queue (`usingCPU`/`usingMemory`) and the
  import scheduler (summed over pending + in-progress tasks). One shared ledger
  keyed by task ID would have duplicated three lifecycles that already exist.
- The external-collection refresh task is not booked by any ledger: it never
  entered the scalar slot report either (`QuerySlot` sums index, compaction and
  import only), and it runs through its own manager rather than one of the three
  executors. DataCoord still prices it, so it is charged within a scheduling
  round.
- `QuerySlot`: `total_cpu = hardware.GetCPUNum()`,
  `total_memory = hardware.GetMemoryCount()`; in standalone both are multiplied
  by `dataNode.standaloneSlotFactor` (the DataNode shares the process with a
  QueryNode). `available = max(total - sum(accepted), 0)`. `available_slots`
  is reported exactly as before.
- The memory total is the full cgroup limit (standalone: ×
  `standaloneSlotFactor`). No headroom ratio is reserved in this version: this
  was an explicit decision (no new config); the unchanged scalar slot gate still
  binds first, so nothing is admitted that the pre-existing path would have
  refused. A `dataNode.taskResource.memoryRatio`-style headroom is a follow-up
  if the memory filter turns out to refuse too little.
- New gauge `milvus_datanode_task_resource{node_id, type=cpu|memory, state=total|available}`
  (`metrics.DataNodeTaskResource`).

## DataCoord picker (`internal/datacoord/task/node_picker.go`)

Same rules as PR #52561, implemented thinner:

- A worker that reports `total_memory > 0` is placed on the two dimensions;
  any other worker is placed by the existing max-heap, which is unchanged.
- Two-dimensional placement: skip a worker whose `available_slots <= 0` or
  whose `available_memory < req.Memory` (memory is the only hard filter).
  Rank the rest by `0.6 x memFrac + 0.25 x cpuFrac + 0.15 x (1 - |memFrac - cpuFrac|)`
  where each fraction is what remains after the task, as a fraction of the
  worker's total. Take the highest; charge cpu, memory and slot on the picked
  worker so later picks in the round see it.
- Nothing fits: fall through to the scalar heap first ("no dimensioned home →
  scalar heap" outranks the oversized rule). Only when the scalar heap has no
  room either does the oversized rule apply: if `req.Memory` exceeds the largest
  `total_memory` of any worker, dispatch to the dimensioned worker with the most
  `available_memory` (waiting never helps such a task); otherwise return
  `NullNodeID`.
- A task with a zero requirement (family that does not estimate) is placed on
  the dimensioned tier when one exists — the memory filter passes trivially —
  and only reaches the scalar heap when no dimensioned worker has a slot.
  Sending it "straight to the scalar heap" would starve it in an
  all-dimensioned cluster. Unreachable in practice, since every family floors
  its memory at `minTaskMemory`.
- `NullNodeID` is per-task, not per-round. `schedule()` ends the round only when
  `nodePicker.exhausted()` — no dimensioned worker with a free slot and no free
  scalar slot. A task that alone does not fit gives way exactly like a task in
  failure backoff: it is set aside and re-queued after the round, so one
  oversized task at the head of the queue (ordered by task ID, i.e. oldest, not
  biggest) cannot stall every smaller task behind it.

  Trade-off, stated: ending the round used to reserve the cluster for that task
  implicitly. Without the reservation a steady stream of small tasks can keep
  delaying it, and under memory pressure with slots still free each round
  examines more of the queue instead of stopping at the first miss. An explicit
  reservation or aging mechanism is a follow-up.

  The set-aside scan is capped at `maxDelayedPerRound = 64` tasks per round
  (shared with the failure-backoff branch, which uses the same slice). The cap
  bounds both the work of a round and the window in which a set-aside task is
  in neither queue and therefore invisible to `GetPendingTaskCount` and
  `AbortAndRemoveTask`. It is a round-trip budget, not a fairness guarantee: a
  task beyond the cap is simply looked at in a later round.

## Compatibility

- New DataCoord + old DataNode: no new fields in the report, so the scalar
  heap is used; extra request fields are ignored by the old worker.
- Old DataCoord + new DataNode: requests carry no cpu/memory, the ledger books
  zero, `available == total`; the old coordinator never reads the new fields.
- Scheduling semantics do change for an all-dimensioned cluster: the old
  "one unplaceable task ends the round" behaviour is gone (see the `NullNodeID`
  is per-task rule above). A cluster with no free slot anywhere still ends the
  round at the first refusal, as before.
- During a rolling upgrade, dimensioned workers are preferred until their slots
  are full, then the scalar heap serves the rest; aggregate throughput is
  preserved, fill order changes.

## Testing

- Estimation: table-driven test per task family, including the
  `fieldSize == 0` fallbacks and a V3 fixture with empty `Binlogs` and
  non-empty `Stats` (the shape PR #52561's review missed).
- Ledger: Accept/Release/Snapshot, negative clamp, standalone discount
  (mockey on `hardware.GetCPUNum` / `hardware.GetMemoryCount`).
- Picker: memory filter, score ordering, in-round charging, oversized to the
  emptiest worker, all-old-workers fall back to the heap, mixed cluster.
- Executors: ledger and `usingSlots` move together on enqueue and completion.
- Coverage target >= 90% on touched code; run with
  `-tags dynamic,test -gcflags="all=-N -l"`.

## Commits

1. `enhance: add cpu/memory resource fields to task and QuerySlot protos`
2. `enhance: add dataCoord.taskResource config params`
3. `enhance: estimate cpu/memory for every DataCoord task type`
4. `enhance: DataNode reports cpu/memory ledger in QuerySlot`
5. `enhance: place tasks on cpu/memory when workers report them`
