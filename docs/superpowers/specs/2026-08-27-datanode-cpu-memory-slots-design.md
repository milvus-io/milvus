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
`slotUsage.Load()` today) and the request builder ships the cached value, so
what was placed and what was shipped are the same number.

## DataNode ledger and report

- New package `internal/datanode/resource` with one ledger:
  `Accept(taskID, cpu, memory)`, `Release(taskID)`, `Snapshot()`.
- Compaction executor, index task queue and import scheduler call `Accept` at
  the point where they add to `usingSlots` and `Release` where they subtract,
  so the two lifecycles are identical.
- `QuerySlot`: `total_cpu = hardware.GetCPUNum()`,
  `total_memory = hardware.GetMemoryCount()`; in standalone both are multiplied
  by `dataNode.standaloneSlotFactor` (the DataNode shares the process with a
  QueryNode). `available = max(total - sum(accepted), 0)`. `available_slots`
  is reported exactly as before.
- New gauge `DataNodeResource{nodeID, type=cpu|memory, state=total|available}`.

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
- Nothing fits: if `req.Memory` exceeds the largest `total_memory` of any
  worker (oversized), dispatch to the worker with the most `available_memory`;
  otherwise return `NullNodeID` for this round, exactly what happens today when
  slots are exhausted.
- A task with a zero requirement (family that does not estimate) goes straight
  to the scalar heap.

## Compatibility

- New DataCoord + old DataNode: no new fields in the report, so the scalar
  heap is used; extra request fields are ignored by the old worker.
- Old DataCoord + new DataNode: requests carry no cpu/memory, the ledger books
  zero, `available == total`; the old coordinator never reads the new fields.

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
