# DataNode task placement on CPU and memory

Date: 2026-08-27
Issue: #52180. Master PR #52561; the 3.0 PR #52982 is its cherry-pick.
This replaces the DataNode-side admission design #52561 started from.

## Goal

Extend the single scalar "slot" DataCoord uses to place tasks on DataNodes into
two dimensions, CPU and memory, so estimated memory constrains placement and
local index execution, and CPU demand spreads compute-heavy tasks.

Scope:

- DataNode does not estimate task sizes. Its index scheduler replaces scalar
  slot admission with memory reservations when node memory capacity is known;
  missing legacy estimates are converted from their slot share. The vector
  task cap (`dataNode.index.maxVecIndexBuildConcurrency`, default 4), backend
  build reservations, compaction pool and import pools remain. There is no
  new CPU admission limit and no scalar-index task-count limit.
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
pickNode (2-D)  <--(QuerySlot: total/available cpu+memory)--                       available = min(total - sum(accepted), free)
```

- Estimation lives only in DataCoord: every task family implements
  `GetTaskResource() taskcommon.Resource` next to the existing `GetTaskSlot()`.
- DataNode books whatever cpu/memory the CreateTask properties carry on accept
  and releases it after execution cleanup. Local index admission reserves the
  same accepted memory separately before starting work. The worker never
  recomputes an estimate, and never reads one off the request payload — the
  handler resolves it once and passes it down as a parameter. A request without
  the properties (old coordinator) books the share of the node its scalar
  `task_slot` stands for; see Compatibility.
- The old scalar chain (`GetTaskSlot`, `slot_usage`/`task_slot`,
  `available_slots`, the max-heap `pickNode`) stays as the compatibility path.

## Wire changes (fixed fields, appended at the end of each message)

| message | new fields |
|---|---|
| `datapb.QuerySlotResponse` | `int64 total_cpu`, `int64 available_cpu`, `int64 total_memory`, `int64 available_memory`, `int64 machine_memory` (memory in bytes), tags 6-10; tags 4 and 5 are master's `copy_segment_shared_indexes` and `segment_rooted_indexes` |

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

The scheduler reads the estimate for placement, and `CreateTaskOnWorker` reads
it again to pass to `Create*`. An exact answer is cached, so both reads use the
same value. An inexact answer is not cached and can change if its inputs resolve
between placement and request construction. The worker books the value in the
request; its next `QuerySlot` report corrects the scheduler's view, but cannot
undo placements already made in that round. Passing the placement estimate into
request construction is a follow-up. The per-node import bound is also computed
on both sides, as described below.

`RefreshExternalCollectionTaskRequest` carries its estimate the same way, and
the DataNode's external collection manager books it like the four executors
do. Its scalar slot is still not counted in the slot report (QuerySlot sums
index, compaction and import slots only), which stays as is.

## DataCoord estimation

`taskcommon.Resource{CPU int64; Memory int64}` — CPU in whole cores, memory in
bytes. This is the estimate DataCoord places on, and the worker books exactly
this value. A DataNode-side correction from the data it actually reads is a
follow-up and is not part of this change.

Every formula mirrors what the worker holds for that family and errs high
where the worker's behavior depends on data or on a machine DataCoord does not
see.

Known gap, a follow-up: clustering compaction and analyze are bounded only by
their input, not by the machine share the worker actually caps them at
(`memoryBufferRatio`, `maxTrainSizeRatio`). If the resulting estimate exceeds
every worker's available memory, dispatch waits, including on empty workers.
As with any task that does not fit, this can hold up tasks behind it.

| task | CPU | memory | mirrors |
|---|---|---|---|
| vector index | 8 | fieldSize x 2 | `index/task_index.go` loads the whole field through cgo and builds beside it |
| scalar index | 1 | fieldSize x 2 | same |
| stats (TextIndex / BM25 / JsonKeyIndex) | 1 | sum(size of the fields the sub job indexes) x 2; whole segment when the schema is not cached | `index/task_stats.go` loops over the `enable_match` fields / the JSON fields / the BM25 output fields only |
| sort compaction | 1 | segmentSize x 2 | `storage.Sort` retains every input record |
| mix compaction / bump schema version | 1 | min(sum(inputs), `dataCoord.segment.maxSize`) + merge-sort reader windows + delete maps | The writer streams, but `storage.MergeSort` opens every input reader before output. For V1, sum each field's largest encoded + decoded binlog chunk, then sum across inputs. Missing chunk sizes, packed storage and manifests conservatively use twice the whole input per reader. mergeSplit keeps its existing streaming budget. Delete maps sum on merge-sort; only the largest is held on mergeSplit. |
| L0 compaction | 1 | sum(deltalog) x 2 + sum(target statslog) | `l0_compactor.go` loads every delta log, then loads the target L1/L2 bloom filters in batches through `loadBF`; summing all targets' stats logs is conservative across batches |
| clustering compaction | 8 | sum(input segments) | buckets are flushed at `memoryBufferRatio` of the machine; that cap is not applied, so a large input is over-priced |
| analyze | 8 | Σ clustering-key field size (the field-size rule below) x 2 | the train set is down-sampled to `maxTrainSizeRatio` of the machine; that cap is not applied. Going through the field-size rule gives sparse and array-of-vector keys an answer, where rows x dim had none |
| import | 1 | sum over files of min(base x vchannels x partitions, file size) x `importMemoryFactor`; at placement, at most `dataNode.import.memoryLimitPercentage` x `importMemoryFactor` of the node's total memory (L0 import excluded) | every file is submitted at once with one read buffer, and a buffer never fills beyond the file it reads; every reader reserves from the import allocator, which caps all import buffers at that share of the machine |
| preimport | 1 | sum over files of min(base buffer, file size) | every file is read in parallel with one base buffer |
| import / preimport from a snapshot source | 1 | files x (per-file buffer + `dataNode.import.deleteBufferSize`), plus one more delete budget when the sources share one L0 set; import applies `importMemoryFactor` to the row buffers only; at placement, at most `dataNode.import.memoryLimitPercentage` of the node's total memory, times `importMemoryFactor` for an import (a pre-import syncs nothing) | each snapshot reader reserves its row buffer and a delete budget from the import allocator (`reserveSnapshotRead` / `AllocateSnapshotTask`); the delete budget is a fixed-size reservation, so the factor that covers an in-flight batch does not apply to it. The slot is sized on the same formula (`snapshotImportBufferBytes`) |
| reshard (import v3) | 1 | `reshardmem.Model.WorkingSet` of the job | the same working set the slot charge is derived from and the worker budgets with; it already carries the expansion factor, so none is applied here |
| import v3 merge | 1 | `reshardmem.Model.ImportWorkingSet` for the plan's fan-in | same model, same reasoning |
| preimport v3 | 1 | files x base buffer | count-only, every file at once: a JSON/CSV file is scanned through one base read buffer; a parquet or numpy file reads only its metadata and a backup job only sizes objects, so those are over-priced, which keeps the price from resolving the files |
| copy segment | 1 | 64MB | stream between buckets |
| refresh external collection | 1 | `externalRefreshMemory` (1GB) | explores the source and reads file metadata and the current segments; depends on the source, so a fixed budget |

The import per-file buffer is not capped at the task's *largest* file:
`importv2.ImportTask.GetBufferSize` reads that cap from the task's own
`ImportTaskV2.FileStats`, which the worker never fills for an import task, so
the cap never fires there. Each file is still bounded by *its own* size, which
is all the buffer reading it can hold, so a task of many small files is priced
at their bytes rather than at one whole buffer each. The worker still reserves a
whole buffer per running reader from its import memory allocator, and a reader
such as Parquet preallocates its batches by buffer capacity, so the import
scheduler never reports less memory than that allocator holds. The allocator
caps it at `dataNode.import.memoryLimitPercentage` of the machine.

That cap also bounds the price. A task whose readers all reserve from the
allocator -- every non-L0 import, and a pre-import from a snapshot source --
never holds more read buffers than that share of the node it runs on, however
many files it has. The allocator does not cover everything an import holds:
each batch read is hashed into a copy (`HashData`) that is handed to an
asynchronous sync while the next batch is read, and the task waits for its
syncs only at the end of the file. Those copies are bounded only by the sync
dispatcher's node-wide pending-task semaphore (2 x CPU x
`maxParallelSyncMgrTasksPerCPUCore` tasks, shared with flush), which cannot be
attributed to one task. So an import's bound keeps the factor its estimate
applies to the buffers: `memoryLimitPercentage x importMemoryFactor` of the
node; a snapshot pre-import, which syncs nothing, is bounded at the allocator's
share alone. Such a task implements `NodeMemoryBounded`, and the picker charges
it `min(estimate, share x node machine_memory)` on each node, so a snapshot
restore of many small segments, priced at tens of GB, is not held at the head
of the queue until some node has that much free. The worker books the same
bound (`boundByImportMemoryLimit`, over the machine memory the allocator itself
is sized on).

The bound is an estimate, not a hard limit: a slow object store can leave more
hashed copies waiting than the factor allows. As for every estimate here, the
worker corrects it at run time: its import memory is never reported below the
allocator's reservations, and while anything is booked its available memory is
bounded by what the machine has free, which includes the resident copies. The
correction reaches DataCoord with the next report, so placements made in the
same round are not undone.

The share is taken of `machine_memory`, the machine the worker reports
(`hardware.GetMemoryCount`, the base the allocator and
`boundByImportMemoryLimit` use), not of `total_memory`. In standalone the total
carries the standalone discount and the allocator does not: on an 8GB machine
the total is 2GB, the worker books an import at 8GB x 10% x 2 = 1.6GB, and a
share of the total would charge 0.4GB, so one round could place five imports
the worker then books at 8GB. Charged on the machine, one round charges what
the next report shows used. DataCoord does not check whether it runs in
standalone: whether a worker's total is discounted is the worker's own role,
and a DataNode bound from a separate pool to a standalone Milvus is not
discounted. A worker that does not report `machine_memory` is bounded on its
`total_memory`. A plain pre-import and
an L0 import do not go through the allocator and keep their estimate.

The import scheduler stores each task's booking with its file futures when it
submits them, and counts every entry whose futures have not all returned,
whatever the task's state, as the import v3 scheduler does. A task that fails
when one file fails, or that is dropped (`DropImport` after its job failed, or
a user abort) while its other readers still run, therefore keeps its whole
booking -- read buffers and in-flight sync alike -- until they return.

Every memory estimate is clamped to at least `minTaskMemory` (64MB).
The scheduler uses inexact answers immediately, subject to memory admission.
A task that cannot be priced exactly answers `(resource, false)`, and the `false` only
stops the answer from being cached. Two cases are priced differently:

- **inputs still resolving** (a collection schema not cached yet after a
  DataCoord restart, index params not read back yet): an upper bound on the
  task, the whole segment. An FM-index whose schema is missing is bounded with
  its own peak model on the field's size without a schema (its recorded size,
  or the whole segment), because its peak is several times the field and the
  generic factor on the whole segment can fall below it; the index params do
  not need the schema. An over-estimate only makes a worker take less work;
  the floor would let a worker accept a task of unknown size and then book its
  real size once the request is built.
- **inputs gone for good** (a segment dropped by compaction, drop-collection or
  drop-partition; a job no longer in meta): the floor. Such a task never
  resolves, and `CreateTaskOnWorker` is the only code that retires it, so it
  must be placed to reach that code. Waiting for an exact price instead would
  re-queue it forever, and since a task that does not fit ends the round, it
  would stop every task behind it as well.
- **an L0 compaction with a Sealed target**: if the target is still Sealed at
  dispatch, `SelectFlushedSegment` fails the task. The price counts what is
  there now and is not cached. That matters for a task that waits for
  capacity while the target flushes: the round that dispatches it prices the
  flushed target, including the stats logs of its final flush.

### fieldSize

Every place that prices one field of a segment uses the same rule
(`taskresource.EstimateFieldSize` over `taskcommon.EstimateFieldSize`; the
recorded size comes from `meta.SegmentInfo.RecordedFieldSize`), in this order:

1. **The binlog size recorded in meta** for the field: its own FieldBinlog in
   storage v1, its column group in storage v2/v3 (the system group, or the
   group of all remaining short fields). It is what the writer measured, so it
   answers whenever it is positive. The one exception is a fixed-width field
   (numbers, bool, timestamps, dense vectors): its schema bound is its exact
   size, so the price is `min(recorded, exact width)` where the worker
   decodes only that column. Without it an int64 sharing a group with a wide
   varchar would be priced on the varchar. A reader that opens the segment
   through its manifest projects the one column. A storage v2 segment without
   a manifest is read through its insert files, and that reader
   (`GetFieldDatasFromStorageV2`) decodes every column of the group in
   windows of up to 128MB (`DEFAULT_FIELD_MAX_MEMORY_LIMIT`) before it takes
   the column out. For such a segment the price is
   `min(recorded, exact width + 128MB)`. `recorded` is the group's decoded
   size, so a group smaller than a window is priced on what it holds. A row
   group larger than the window is still read whole and is not modeled.
   Passing a projected schema to that reader would remove the window; that is
   a follow-up.
2. **The schema's upper bound**, only when meta has no positive binlog size for
   the field -- the storage v3 case after a DataCoord restart, whose per-field
   binlogs live in the manifest and are not persisted (`kv_catalog.go`).
   `rows x width` for a fixed-width type (numbers, bool, timestamps, dense
   vectors), `rows x (max_length + 4)` for a varchar (the proxy rejects a value
   longer than `max_length` bytes; 4 is the Arrow offset), plus the validity
   bitmap when nullable. A JSON value is bounded by `common.JSONMaxLength` and
   an array by `max_capacity` x element width, both enforced on insert. Text
   and geometry have no such limit (`checkTextFieldData` skips the length
   check, geometry is only checked for WKB convertibility), so they, sparse and
   array-of-vector fields have no schema bound.
3. **The whole segment**, when neither of the above answers: no schema, a field
   the schema does not know, or a type with no schema bound.

Steps 2 and 3 are capped at the segment size: one field never holds more than
its segment. The cap matters because a schema bound is `rows x` the per-row
limit, which for a `varchar(65535)` or a JSON field (64KB per value) is orders
of magnitude above the data. Uncapped, such a task can exceed every worker and
block dispatch until the estimate or worker capacity changes.

When meta has neither positive size statistics nor binlogs, that cap itself is
an estimate: `EstimateSegmentSize` deliberately uses rows x
`typeutil.EstimateSizePerRecord(schema)`. As discussed in the earlier review,
the task memory formulas are already conservative, with expansion factors and
a memory floor, so this fallback keeps average per-record sizes rather than
schema maxima that can prevent dispatch by exceeding every worker's memory.
This is an accepted estimation trade-off, not a strict upper-bound guarantee for every
variable-width value. `EstimateMaxSizePerRecord` also still uses heuristics for
text/JSON/array/geometry and sparse vectors. Recording actual field sizes
remains a follow-up; this change keeps the existing fallback.

The rule is the same for internal and external collections; they differ only in
what meta holds for their segments, and that is what the three steps key on.

The **scalar task slot** of an index task keeps master's own field size
(`IndexInspector.estimateIndexFieldSize`: the binlog size, or the projected
column for a manifest-backed V3 segment); this change does not re-derive it.
The **memory** of index and
stats tasks is this field size times the expansion factor, except an FM-index
build: its peak holds the text, its suffix array and the sampling structures at
once, several times the field, so it is priced on `estimateFMIndexBuildPeakBytes`
-- the same peak its scalar slot already comes from.

### Configuration (`dataCoord.taskResource.*`, refreshable, not exported to milvus.yaml)

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
| `externalRefreshMemory` | 1GB |

An exact estimate is cached on the task object (same pattern as
`slotUsage.Load()` today). The request builder reuses that cached value. Inexact
answers are recomputed, including between placement and request construction;
see the wire section for the resulting same-round accounting gap.

A family that cannot resolve its inputs is **not** cached, so the next
scheduling round retries instead of freezing a placeholder for the task's
lifetime. A nil schema or an empty or invalid index type answers the
whole-segment upper bound; a missing segment answers the floor (see above). A field that is genuinely absent from a schema we DO have is a real
answer, not a miss: it is priced at the whole segment (conservative) and cached.

## DataNode ledger and report

- No separate `internal/datanode/resource` package. Each executor keeps its own
  `taskcommon.Resource` counter right beside the `usingSlots` it already keeps,
  with the identical lifecycle: booked where `usingSlots` is added, released
  where it is subtracted, and the release subtracts exactly what was booked
  rather than a re-derived value. That is the compaction executor
  (`usingResource`), the index task queue (`usingCPU`/`usingMemory`), the
  import scheduler (summed over pending + in-progress tasks, plus a failed
  task until all its file futures have returned: a failed file marks the task
  Failed at once while its sibling readers still hold their buffers) and the import v3
  scheduler (summed over queued + running runs, the same scope its
  `Slots()+QueuedSlots()` covers). One shared ledger keyed by task ID would
  have duplicated four lifecycles that already exist.
- The index queue books each task's CPU/memory on acceptance and releases it
  after execution cleanup. This report includes waiting tasks, so DataCoord
  stops placing when accepted work has spoken for the node. Bookings are
  captured before cleanup can reset task fields. Active tasks, reservations
  and releases use execution identity: a canceled native build that keeps
  running can overlap a retry with the same name without releasing its budget.
- Local index admission has a separate execution ledger. Before preparation
  and download it atomically reserves accepted memory against node capacity
  minus index execution reservations and the other executors' accepted memory.
  Its own waiting tasks are excluded from this test, so queued bookings cannot
  prevent their own execution. The waiting queue is separately bounded to one
  node's memory and 1024 tasks. An empty queue accepts one oversized estimate;
  an otherwise idle node executes such an estimate alone so it can progress.
  This does not claim that an oversized estimate fits physical memory.
  Reservations remain held through backend admission waits, upload and input
  cleanup, including canceled native calls until they actually return. Vector
  tasks retain FIFO build admission and their full-lifecycle concurrency cap.
  A blocked vector can be bypassed by a non-vector task that fits. If memory
  capacity cannot be read, queue and execution admission use legacy slots.
- The external-collection refresh task runs through its own manager rather
  than one of the four executors. The manager books the estimate when it
  accepts the task (queued in its pool included, a duplicate dispatch books
  nothing) and releases it when the task function returns, or at once if the
  pool refuses the task. `QuerySlot` adds that ledger to the other four. Its
  scalar slot still never entered the slot report.
- `QuerySlot`: `total_cpu = hardware.GetCPUNum()`,
  `total_memory = hardware.GetMemoryCount()`; in standalone both are multiplied
  by `dataNode.standaloneSlotFactor` (the DataNode shares the process with a
  QueryNode). `machine_memory = hardware.GetMemoryCount()` without the
  discount: the base of the per-node bound of a `NodeMemoryBounded` task. `available_cpu = max(total_cpu - sum(accepted cpu), 0)`.
  `available_memory = min(max(total_memory - sum(accepted memory), 0),
  system free memory)`, where system free memory on Linux is the cgroup-aware
  limit minus this process's resident memory excluding shared pages (the definition
  `hardware.GetFreeMemoryCount` uses, computed without its uint64 wrap-around).
  On Darwin and Windows the helper instead subtracts machine-wide used memory.
  A failed limit reading leaves the ledger's figure alone rather than reporting
  nothing free. The system term applies only while the ledger has something
  booked: a node with nothing booked reports `available_memory = total_memory`,
  the same progress rule as local index admission on an otherwise idle node. DataCoord ends a round at a task that does not fit, so
  without this a task that fits `total_memory` but never the system free
  memory (resident memory held by the other roles or an allocator) would stop
  every task behind it for good. An idle node therefore accepts work up to its
  whole capacity in a round, as slot-only placement does. `available_slots` is reported exactly
  as before.
- The DataNode still does no estimation: its ledger is exactly what DataCoord
  priced. The system term only bounds the report from above, so memory the
  ledger does not know about -- estimates that came out low, flowgraph buffers,
  the write path, and in standalone the other roles in the process -- is never
  offered. It never widens the report beyond the ledger.
- The memory total is the full cgroup limit (standalone: ×
  `standaloneSlotFactor`). No headroom ratio is reserved; the system term above
  is what keeps the report honest. Dimensioned workers are admitted on
  memory; scalar slots remain a compatibility report (see the picker).
- New gauge `milvus_datanode_task_resource{node_id, type=cpu|memory, state=total|available}`
  (`metrics.DataNodeTaskResource`).

## DataCoord picker (`internal/datacoord/task/node_picker.go`)

- A priced task on a worker reporting `total_memory > 0` uses the two
  dimensions. Skip only when `available_memory < req.Memory`. CPU never
  refuses; scalar slots neither gate nor reorder this tier.
- Rank fitting workers by
  `0.6 x memFrac + 0.25 x cpuFrac + 0.15 x (1 - |memFrac - cpuFrac|)`,
  where each fraction is what remains after the task relative to the total.
  Charge CPU and memory immediately so later picks in the round see the
  reservation. Scalar slots are still charged for compatibility tasks in the
  same round, but exhausting them does not stop priced work.
- A task without a resource price retains scalar slot admission and ranking,
  including on upgraded workers. Every current DataCoord task family floors
  its memory at `minTaskMemory`, so this is a compatibility fallback.
  After an unpriced placement, reserve the worker's remaining offered memory
  for this round: its actual slot-derived booking is unknown until the next
  report, and must not be offered again to priced tasks. Further unpriced
  tasks retain slot placement. A fresh report restores actual available
  memory for priced placement, regardless of scalar slot exhaustion.
- Workers without a resource report use the existing slot max-heap. If no
  reporting worker fits, try that compatibility tier before returning
  `NullNodeID`.
- A task exceeding every reporting worker's total memory waits too. There is
  no oversized dispatch exception: memory admission may not be bypassed.
  Such a task needs its estimate or worker capacity to change before it fits.

- A `NullNodeID` ends the round, exactly as the slot heap always has: the task
  goes back on the queue and nothing behind it is placed this round. The queue
  is ordered by task ID, so the oldest waiting task keeps its place at the head,
  and the capacity running tasks release is held for it instead of being taken
  by a stream of smaller tasks behind it. The cost, the same as before this
  change, is head-of-line blocking: while the head does not fit, smaller tasks
  that would fit wait too.

  A task in failure backoff is set aside without limit and the round goes on,
  as before this change. Backoff is checked before pricing and costs no meta
  walk, and during a retry storm the oldest tasks are the ones failing over and
  over: if setting them aside could end the round, a healthy task behind them
  would never be reached.

## Compatibility

- New DataCoord + old DataNode: no new fields in the report, so the scalar
  heap is used; the extra `task_cpu` / `task_memory` properties are unknown
  keys the old worker simply ignores.
- Old DataCoord using `CreateTask` + new DataNode: the properties carry neither key, so
  `GetTaskResource` returns the zero resource with no error. `CreateTask` then
  books the share of the node the task's `task_slot` stands for:
  `slot / CalculateNodeSlots()` of the node's capacity. Both totals describe
  the same machine under the same standalone discount, so the ratio holds
  inside one node. Booking zero instead would make the node report memory it
  does not have, and a worker pool shared by several Milvus versions runs such
  tasks for as long as the pool exists, not only during an upgrade. Absence is the compatibility case, not a
  protocol violation, so it must not be reported as one — unlike `task_slot`,
  a missing `task_cpu` never fails the task. A key that is present but
  unparsable is a real error and does fail the `CreateTask` call.
- Direct index RPCs (`CreateJob`, `CreateJobV2`) carry no resource properties.
  The index queue converts missing memory from the task's scalar slot, or uses
  the memory floor if that slot is absent too. Other direct pre-`CreateTask`
  RPCs (`PreImport`, `ImportV2`, `CompactionV2`, `PreImportV3`) still book zero
  CPU/memory and do not reach `CreateTask`'s slot-share fallback. Accounting for
  those historical callers remains outside this change: a shared pool does
  not include those direct tasks in its CPU/memory ledger. The compatibility
  guarantee does not cover every historical RPC.
- Scheduling semantics are those of the slot heap: a task that does not fit
  ends the round (see the `NullNodeID` rule above). What changes is when a
  task fits: on a dimensioned worker that is memory; old workers keep slots.
- A worker pool that implements `QuerySlot` itself (a managed or proxied
  DataNode pool, for instance) and fills only `available_slots` reports no
  `total_memory`. DataCoord then places the whole pool on the scalar heap, as
  it does an old DataNode, so this change has no effect there until that
  implementation reports the new fields and admits index work by memory.
  Roll the pool side out first, then the coordinator: a coordinator ahead of
  the pool only keeps the slot behavior.
- During a rolling upgrade, dimensioned workers are preferred until their memory
  is full, then the scalar heap serves the rest. Throughput changes require
  measurement; the tiers preserve compatibility, not a performance guarantee.

## Code layout

- `internal/datacoord/taskresource`: the shared pricing -- the per-family
  formulas, the field- and segment-size rules, the stats target fields and the
  `Cache` every task memoizes its price in. It depends only on `meta` (and on
  `storage` for the storage version), so each task package can import it.
- Each task package keeps the part that needs its own types:
  `compaction/task` (`compactionInputSize` for clustering,
  `mixCompactionInputs` for mix and schema-bump compaction), `bulkimport`
  (`importFileBufferSize`), `index` (the FM-index peak from
  `EstimateFMIndexBuildPeakBytes`), and every task's `GetTaskResource`.

## Testing

- Estimation: table-driven test per task family, including the
  `fieldSize == 0` fallbacks and a V3 fixture with empty `Binlogs` and
  non-empty `Stats` (the shape PR #52561's review missed).
- Ledger: Accept/Release/Snapshot, negative clamp, standalone discount
  (mockey on `hardware.GetCPUNum` / `hardware.GetMemoryCount`).
- Picker: memory filter, CPU/slot exhaustion with spare memory, score ordering,
  in-round charging, oversized requests waiting, unpriced slot compatibility,
  all-old-workers falling back to the heap, and mixed clusters.
- Executors: separate acceptance and execution ledgers; exhausted old slots
  with spare memory, external executor usage, oversized tasks, cancellation
  through cleanup, same-name retries, and the retained vector concurrency cap.
- Merge-sort compaction: a V1 fan-in whose resident reader windows exceed one
  output segment, chunk-size fallbacks, and namespace sort-path selection.
- Coverage target >= 90% on touched code; run with
  `-tags dynamic,test -gcflags="all=-N -l"`.

## Commits

One commit: `enhance: place DataNode tasks on cpu and memory instead of a scalar
slot` -- the wire fields, the pricing, the ledgers, the picker and the import
v3 booking. An earlier revision also bounded scalar index/stats/analyze builds
with a worker-side semaphore. The final index execution admission uses memory
instead of the scalar budget while retaining the vector-task concurrency cap
and backend build reservations.
