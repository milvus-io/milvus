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
task.GetTaskResource() -> {cpu, memory}  --(CreateTask properties: task_cpu/task_memory)-->  ledger.Accept(id, cpu, mem)
        |                                                                          ...task runs under the existing pools...
        v                                                                          ledger.Release(id)
pickNode (2-D)  <--(QuerySlot: total/available cpu+memory)--                       available = total - sum(accepted)
```

- Estimation lives only in DataCoord: every task family implements
  `GetTaskResource() taskcommon.Resource` next to the existing `GetTaskSlot()`.
- DataNode only does bookkeeping: whatever cpu/memory the CreateTask properties
  carry is added on accept and subtracted on completion, at exactly the points
  where the scalar `usingSlots` is added and subtracted today. The worker never
  recomputes an estimate, and never reads one off the request payload — the
  handler resolves it once and passes it down as a parameter. A request without
  the properties (old coordinator) books zero.
- The old scalar chain (`GetTaskSlot`, `slot_usage`/`task_slot`,
  `available_slots`, the max-heap `pickNode`) is untouched.

## Wire changes (fixed fields, appended at the end of each message)

| message | new fields |
|---|---|
| `datapb.QuerySlotResponse` | `int64 total_cpu`, `int64 available_cpu`, `int64 total_memory`, `int64 available_memory` (memory in bytes) |

That is the only proto change. The worker's report needs fixed fields because
it is a response the coordinator reads directly; a per-task estimate does not.

Every task request carries its estimate as
`workerpb.CreateTaskRequest.properties["task_cpu"]` and `["task_memory"]` —
the same `taskcommon.Properties` map that already carries `task_slot`,
`task_type` and `collection_id`. `session.Cluster.Create*` sets both next to
its existing `AppendTaskSlot`, from the `taskcommon.Resource` its caller passes
in; the worker's `CreateTask` handler reads them once with
`Properties.GetTaskResource()` and threads the value to the executor that books
it. The request payload protos — `CompactionPlan`, `CreateJobRequest`,
`AnalyzeRequest`, `CreateStatsRequest`, `PreImportRequest`, `ImportRequest`,
`CopySegmentRequest` — are unchanged.

One consequence worth naming: because DataCoord reads its own estimate once per
dispatch and hands that same value to `Create*`, "what the scheduler placed the
task on" and "what the worker books" are the same variable by construction,
not two independently computed numbers that have to be kept in step.

`RefreshExternalCollectionTaskRequest` gets no estimate at all: the DataNode
does not count that task in its slot report today (QuerySlot sums index,
compaction and import only) and that stays as is. DataCoord still charges it
1 CPU / 64MB within a scheduling round.

## DataCoord estimation

`taskcommon.Resource{CPU int64; Memory int64}` — CPU in whole cores, memory in
bytes. This is the estimate DataCoord places on. The DataNode that accepts the
task refines it (see "DataNode correction" below) and books the refined value,
so the next round places on corrected availability.

Every formula mirrors what the worker holds for that family and errs high
where the worker's behavior depends on data or on a machine DataCoord does not
see.

| task | CPU | memory | mirrors |
|---|---|---|---|
| vector index | 8 | fieldSize x 2 | `index/task_index.go` loads the whole field through cgo and builds beside it |
| scalar index | 1 | fieldSize x 2 | same |
| stats (TextIndex / BM25 / JsonKeyIndex) | 1 | sum(size of the fields the sub job indexes) x 2; whole segment when the schema is not cached | `index/task_stats.go` loops over the `enable_match` fields / the JSON fields / the BM25 output fields only |
| sort compaction | 1 | segmentSize x 2 | `storage.Sort` retains every input record |
| mix compaction / bump schema version | 1 | min(sum(input segments), `dataCoord.segment.maxSize`) | `MultiSegmentWriter` streams: never more than the input, never more than one output segment |
| L0 compaction | 1 | sum(deltalog) x 2 | `l0_compactor.go` loads every delta log |
| clustering compaction | 8 | sum(input segments) | buckets are flushed at a share of the machine, applied on the DataNode |
| analyze | 8 | rows x dim x elemSize x 2 | the train-set cap is a share of the machine, applied on the DataNode |
| import | 1 | files x (base x vchannels x partitions) x `importMemoryFactor` | every file is submitted at once with one read buffer; the allocator limit is applied on the DataNode |
| preimport | 1 | files x base buffer | every file is read in parallel with one base buffer |
| copy segment / refresh external collection | 1 | 64MB | stream between buckets |

The import per-file buffer is deliberately not capped at the task's largest
file: `importv2.ImportTask.GetBufferSize` reads that cap from the task's own
`ImportTaskV2.FileStats`, which the worker never fills for an import task, so
the cap never fires there.

Every memory estimate is clamped to at least `minTaskMemory` (64MB). A task
whose inputs cannot be resolved yet is priced at the floor and not cached.

### fieldSize

Every place that sizes one field of a segment uses the same rule
(`taskcommon.EstimateFieldSize`): the field is the **smaller of two upper
bounds**.

- the schema's: `rows x width` for a fixed-width type (numbers, bool,
  timestamps, dense vectors), `rows x (max_length + 4)` for a varchar (the
  proxy rejects a value longer than `max_length` bytes; 4 is the Arrow
  offset), plus the validity bitmap when nullable. json, text, array,
  geometry, sparse and array-of-vector fields have no schema bound;
- the container's: the memory size of the binlogs holding the field. In
  storage v2/v3 one binlog holds a whole column group (the system group, or
  the group of all remaining short fields), so this is the group, not the
  field.

Neither bound alone is the field: the schema cannot see that a varchar is
short, and a column group cannot tell its fields apart.

V3 segments do not persist the column-group binlogs (`kv_catalog.go`), so after
a DataCoord restart the container is unknown. A fixed-width field is exact
anyway; a variable-width field is then bounded by the segment's insert size
minus every fixed-width field and the system fields (16 bytes a row). With no
bound at all (an unbounded type in a segment without size statistics) the
schema's per-row estimate is used, then the whole segment. An unknown field is
charged its group, else the whole segment.

The **scalar task slot** of an index task is derived from this same field size
(`calculateIndexTaskSlot`, at creation and at reload); without a cached schema
it falls back to the binlog size it used before. The **memory** of index and
stats tasks is this field size times the expansion factor.

## DataNode correction

`internal/datanode/taskresource` refines the estimate on the DataNode that
accepts the task, in `CreateTask`, before the task is constructed. The
corrected value is what the task carries, so it is exactly what the ledger
books at acceptance and releases at completion; `QuerySlot` reports
`available = total - sum(corrected)`. A difference from the estimate is logged
("task resource corrected on accept").

Contract, identical for every family:

- a zero estimate (coordinator that predates estimates) is left at zero
- CPU is DataCoord's; it only ranks
- when the request carries nothing better than DataCoord had, the estimate
  stands (a V3 scalar field after a DataCoord restart has no binlog sizes on
  either side, so its estimate is not replaced by a guess)
- the corrected memory is floored at `minTaskMemory`

What the worker knows better, per family:

| task | corrected memory |
|---|---|
| index | input: min(schema bound, column group in the request's binlogs) for the indexed field and the optional scalar fields the build loads; without binlogs only an exact fixed-width size is used, otherwise the estimate stands; expansion: the build model of the index type, below |
| stats | per target field, sized the same way: text `raw + min(tantivy budget, 2 x raw)`, json key `2 x raw + json_key_stats_tantivy_memory`, bm25 `2 x raw`; any target field that cannot be sized keeps the estimate |
| analyze | `min(raw, machine x max_train_size_ratio) x analyzeMemoryFactor` |
| sort compaction | `insert + rows x 8 + binlogMaxSize + 2 x deltas` |
| mix / bump | `min(insert, plan.max_size) + 2 x deltas` |
| L0 compaction | `L0 deltas x l0CompactionMemoryFactor + target segments' statslogs` (the bloom filters it loads) |
| clustering | `min(insert, machine x memoryBufferRatio) + 2 x deltas` |
| import | `min(files x importv2.CalculateImportBufferSize, importv2.ImportMemoryLimit) x importMemoryFactor` |
| preimport | `files x base buffer` |

Index build models (knowhere exposes a load-time estimate only, so these are
derived from each index's layout; build parameters are the request's merged
with this node's knowhere build defaults):

| index types | build memory |
|---|---|
| FLAT, BIN_FLAT, GPU brute force, SVS_FLAT | `2 x raw` |
| IVF_FLAT family, SVS_IVF | `2 x raw + rows x 8 + nlist x bytesPerRow` |
| IVF_SQ8, IVF_SQ_CC | `raw + rows x code(sq_type) + IVF lists` |
| IVF_PQ family | `raw + rows x ceil(m x nbits / 8) + IVF lists + 2^nbits x bytesPerRow` |
| IVF_RABITQ | `raw + rows x (ceil(dim/8) + 8) + IVF lists (+ raw with refine)` |
| SCANN | `raw + rows x ceil(dim/2) + IVF lists (+ raw with with_raw_data)` |
| HNSW family | `2 x raw + rows x ((3M + 2) x 4 + 56)` (links, label, pointer, lock) |
| DISKANN, AISAQ, SVS_VAMANA | `raw + raw x pq_code_budget_gb_ratio + rows x (max_degree x 4 x 1.3 + 16)` |
| sparse, MINHASH_LSH, Trie, RTREE | `2 x raw` |
| STL_SORT | `2 x raw + rows x 4 + rows / 8` |
| INVERTED, NGRAM | `raw + min(500MB tantivy budget, 2 x raw)` |
| BITMAP | `raw + (rows / 8) x bitmap_cardinality_limit` |
| HYBRID | max(BITMAP, INVERTED) |
| anything else | `raw x indexMemoryFactor` |

These models are structural, not measured: they have not been calibrated
against real builds yet.

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
| `importMemoryFactor` | 2 |
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
  is reported exactly as before; it is consumed only by coordinators/pickers on
  the scalar tier.
- The memory total is the full cgroup limit (standalone: ×
  `standaloneSlotFactor`). No headroom ratio is reserved in this version: this
  was an explicit decision (no new config). The scalar slot does not bind first
  on a dimensioned worker — the picker never gates on it — so the memory filter
  is that worker's only admission gate. A `dataNode.taskResource.memoryRatio`-style
  headroom is a follow-up if the memory filter turns out to refuse too little.
- New gauge `milvus_datanode_task_resource{node_id, type=cpu|memory, state=total|available}`
  (`metrics.DataNodeTaskResource`).

## DataCoord picker (`internal/datacoord/task/node_picker.go`)

Same rules as PR #52561, implemented thinner:

- A worker that reports `total_memory > 0` is placed on the two dimensions;
  any other worker is placed by the existing max-heap, which is unchanged.
- Two-dimensional placement: skip a worker whose
  `available_memory < req.Memory`; memory is the only hard filter — the scalar
  slot is a compatibility currency for workers that do not report cpu/memory and
  never gates a dimensioned worker (otherwise a worker whose slot budget is spent
  but whose memory is free would sit idle, which is exactly what this design
  exists to prevent).
  Rank the rest by `0.6 x memFrac + 0.25 x cpuFrac + 0.15 x (1 - |memFrac - cpuFrac|)`
  where each fraction is what remains after the task, as a fraction of the
  worker's total. Take the highest; charge cpu and memory on the picked worker so
  later picks in the round see it.
- Nothing fits: fall through to the scalar heap first ("no dimensioned home →
  scalar heap" outranks the oversized rule). Only when the scalar heap has no
  room either does the oversized rule apply: if `req.Memory` exceeds the largest
  `total_memory` of any worker, dispatch to the dimensioned worker with the most
  `available_memory` (waiting never helps such a task); otherwise return
  `NullNodeID`. The scalar slot is not consulted here either.
- A task with a zero requirement (family that does not estimate) is placed on
  the dimensioned tier when one exists — the memory filter passes trivially —
  and only reaches the scalar heap when no dimensioned worker has free memory.
  Sending it "straight to the scalar heap" would starve it in an
  all-dimensioned cluster. Unreachable in practice, since every family floors
  its memory at `minTaskMemory`.
- `NullNodeID` is per-task, not per-round. `schedule()` ends the round only when
  `nodePicker.exhausted()` — no dimensioned worker with any free memory and no
  free scalar slot. A task that alone does not fit gives way exactly like a task in
  failure backoff: it is set aside and re-queued after the round, so one
  oversized task at the head of the queue (ordered by task ID, i.e. oldest, not
  biggest) cannot stall every smaller task behind it.

  Trade-off, stated: ending the round used to reserve the cluster for that task
  implicitly. Without the reservation a steady stream of small tasks can keep
  delaying it, and under memory pressure (some memory free on the workers, but
  not enough for the tasks at hand) each round examines more of the queue
  instead of stopping at the first miss. An explicit
  reservation or aging mechanism is a follow-up.

  The set-aside scan is capped at `maxDelayedPerRound = 64` tasks per round
  (shared with the failure-backoff branch, which uses the same slice). The cap
  bounds both the work of a round and the window in which a set-aside task is
  in neither queue and therefore invisible to `GetPendingTaskCount` and
  `AbortAndRemoveTask`. It is a round-trip budget, not a fairness guarantee: a
  task beyond the cap is simply looked at in a later round.

## Compatibility

- New DataCoord + old DataNode: no new fields in the report, so the scalar
  heap is used; the extra `task_cpu` / `task_memory` properties are unknown
  keys the old worker simply ignores.
- Old DataCoord + new DataNode: the properties carry neither key, so
  `GetTaskResource` returns the zero resource with no error, the ledger books
  zero and `available == total`. Absence is the compatibility case, not a
  protocol violation, so it must not be reported as one — unlike `task_slot`,
  a missing `task_cpu` never fails the task. A key that is present but
  unparsable is a real error and does fail the `CreateTask` call.
- The pre-`CreateTask` RPCs (`PreImport`, `ImportV2`, `CompactionV2`,
  `CreateJob`, `CreateJobV2`) carry no properties at all; a coordinator
  reaching them books zero for the same reason.
- Scheduling semantics do change for an all-dimensioned cluster: the old
  "one unplaceable task ends the round" behaviour is gone (see the `NullNodeID`
  is per-task rule above). A cluster with no room anywhere — no free memory on
  any dimensioned worker and no free scalar slot — still ends the round at the
  first refusal, as before.
- During a rolling upgrade, dimensioned workers are preferred until their memory
  is full, then the scalar heap serves the rest; aggregate throughput is
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
6. `enhance: carry task cpu/memory in CreateTask properties, not proto fields`
   — supersedes the per-request proto fields added in (1); only
   `QuerySlotResponse` keeps its new fields.
