# DataNode task placement on CPU and memory

Date: 2026-08-27; revised 2026-11-17.
Issue: #52180. Master PR #52561; 3.0 PR #52982.

## Goal and responsibilities

DataCoord estimates task CPU and memory and selects a worker. Memory constrains
placement; CPU ranks suitable workers. A blocked head task retains its priority.
If no worker has enough available memory, an idle worker may execute that one
task exclusively, even if its estimate exceeds the worker's total capacity.
This guarantees scheduling progress; it does not promise the task fits physical RAM.

DataNode executes the price supplied by CreateTask. Index/stats/analyze and
import V3 each apply memory admission within their own scheduler. They do not
share an execution budget or acquire locks in other schedulers. Overall task
placement belongs to DataCoord. Existing vector build concurrency, backend
build reservations, compaction pools and import pools remain.

## Request and response contract

Every task implements `GetTaskResource() taskcommon.Resource`. Exactness is an
internal concern of `taskresource.Cache`; it is not part of the Task interface.
A cached estimate avoids repeated metadata walks; unresolved estimates are
recomputed on subsequent scheduling rounds.

The picker returns both the selected node and its charged Resource. The global
scheduler passes that same value to `CreateTaskOnWorker(nodeID, cluster, resource)`;
the request builder does not reprice it. Session Create* methods encode it in
CreateTask properties `task_cpu` and `task_memory`, alongside `task_slot`,
`task_type` and `collection_id`. DataNode parses the properties once and passes
them to the executor. It neither estimates the task nor reapplies import caps.

QuerySlotResponse adds:

| Tag | Field | Meaning |
|---|---|---|
| 6 | total_cpu | Worker task capacity in whole cores |
| 7 | available_cpu | Capacity minus booked task CPU, negative when overcommitted |
| 8 | total_memory | Worker task capacity in bytes |
| 9 | available_memory | Worker task capacity minus booked task memory, floored at zero |
| 10 | idle | No accepted background-task CPU or memory is booked |

Master uses tag 4 for shared-index copies and reserves the retired tag 5.
3.0 reserves both tags for wire compatibility. Both branches use
the same new field numbers. An absent idle report is not evidence of idleness.

In cluster mode total memory is the worker's cgroup-aware hardware capacity.
Standalone discounts CPU and memory with `dataNode.standaloneSlotFactor`,
clamped to [0,1]. Both dimensions have a positive floor. Scheduling assumes a worker dedicated
to DataCoord tasks. QuerySlot subtracts accepted task bookings from this
budget, including tasks that are still queued.

Old workers that do not report total_memory still use the scalar heap. The
active task-creation path is CreateTask; direct historical CreateJob/CreateJobV2
RPCs are not a reason to retain scalar-to-memory conversion in local schedulers.
For an old coordinator using CreateTask without resource properties, its positive
task_slot is converted once at that actual RPC boundary to a proportional share
of node capacity. Explicit coordinator prices pass through unchanged.
Malformed or negative resource properties are rejected at the request boundary.

## Placement

A picker is built from QuerySlot once per round and charged after every choice.
Among workers with sufficient available memory, it combines free-memory score
(weight 0.6), remaining CPU fraction (0.25), and balanced remaining proportions (0.15).
The CPU fraction can be negative: booking an extra half of a worker's CPU capacity
reduces its score by 0.125. The balance term treats exhausted CPU as zero.
CPU or scalar slot exhaustion does not reject an otherwise memory-fitting task.
Legacy workers can serve through the existing scalar-slot heap.

If ordinary placement fails, the picker chooses an explicitly idle worker,
charges the selected estimate and marks the worker exclusive for the remainder
of the round. A worker becomes non-idle after any placement in that round.
If no placement is possible,
worker dispatch stops at that task. Later tasks that can finish on the coordinator
may still complete locally. Tasks in failure backoff give way.

Import tasks backed by the shared import allocator implement NodeMemoryBounded.
Their selected price is min(estimate, machine memory x task share). Cluster
workers expose the full machine capacity; standalone derives the machine size
locally because coordinator and worker share a process. Plain preimport and L0
import are not allocator-bounded. The bound includes importMemoryFactor for
import's buffers and asynchronous copies; it remains an estimate.

## Coordinator-only completion

Index and stats tasks can identify obsolete or no-work cases from cached
metadata. Classification in schedule performs no remote lookup or persistence.
Actual local completion runs asynchronously on the execution pool with a request
timeout and without UpdateTaskVersion or worker placement. Local tasks remain
owned until completion, cancellation or retry; a metadata failure requeues with
normal failure backoff. If rechecking instead finds worker work, the task returns
to placement without adding a failure backoff and is classified again using the
current metadata and threshold. Nullable-vector row counts
use the same rule during classification and preparation, and failed schema lookups
retry rather than continuing with an unchecked row count.
This work is not included in schedule's wait for worker
dispatches. A full shared execution pool retains its existing backpressure;
this change does not create a second pool or unbounded dispatch goroutines.
Normal worker preparation checks target liveness at dispatch.

## Worker execution and reporting

Each executor owns its existing lifecycle and reports its own Resource. QuerySlot
sums those reports; there is no new shared execution-budget component.

The index queue records accepted resources, including waiting tasks. Execution
admission separately sums admitted task records under its scheduler lock and
compares memory to its capacity. Queue memory is bounded independently and an
empty queue can accept one oversized task. Execution state contains the booked
resource rather than maintaining parallel slot, memory and release maps. Native
execution retains its booking through completion and cleanup; task identity
keeps same-name retries separate. The existing vector concurrency limit remains.

Import V3 captures the supplied Resource at Submit. Under its own lock it sums
running resources and admits pending work by memory. One task can run when its
scheduler is idle even if the estimate exceeds capacity. Queued work remains in
the report, but is not execution memory. Failure cancels sibling preimport reads;
reporting the error does not perform an additional unbounded join.

Import V2 cancels a task when it fails or is removed. Its normal task/future
lifecycle and allocator usage supply the resource report; it does not maintain
an additional submitting/detached reservation layer. Cancellation is cooperative,
so reports are scheduling estimates rather than a measurement of every live
allocation during cleanup. External refresh books at registration, including time spent waiting for its pool,
and releases the booking when execution exits or the pool rejects submission.

## Estimation

Every normal task price has positive CPU and a configured memory floor.
Missing inputs that can still resolve use a conservative estimate without
caching. Obsolete index/stats work can complete on the coordinator. When recorded
segment size is absent, the existing average-row schema estimate is used;
variable-width distributions are not a strict bound.

| Task | Memory model |
|---|---|
| Vector/scalar index | Estimated field size x indexMemoryFactor; FM index uses its existing peak-memory model |
| Text/JSON/BM25 stats | Distinct input column groups/fields x statsMemoryFactor |
| Sort compaction | Whole segment x statsMemoryFactor; sorting retains its input |
| Mix / schema-bump compaction | Streaming writer budget + concurrently live reader budgets + delete maps |
| L0 compaction | Deltalog bytes x l0CompactionMemoryFactor + target bloom-filter stats |
| Clustering / analyze | Input sizes, with analyzeMemoryFactor for analyze |
| Import / preimport | File reader buffers and snapshot delete budgets; import includes importMemoryFactor |
| Import V3 reshard / merge | Existing reshardmem working-set model |
| Preimport V3 | Number of files x base buffer |
| Copy segment | Memory floor for streaming copies |
| External refresh | externalRefreshMemory |

For a StorageV3 NGRAM build on Milvus-written TEXT, DataCoord adds the indexed
field's manifest LOB payload bytes to its field-size estimate before applying
`indexMemoryFactor`. The column group can store references while the index
reader materializes their text. The task caches the price for that manifest
revision and retries a failed manifest read before worker placement. Counting
a whole reused LOB file can overestimate the text referenced by one segment.

Field-width helpers live under internal/datacoord/fieldsize. Metadata size
estimation reuses FixedFieldWidth, but legacy scalar-slot estimates remain in the
index/stats inspectors and use meta.EstimateFieldsReadSize with
meta.SupportsFieldProjection. That projection check recognizes StorageV3 with a
manifest; task memory estimation also treats StorageV2 with a manifest as
projecting. The paths share width helpers, not a single field-size policy.

For task memory estimates, VarChar uses the smaller of the recorded field/group
size and its schema bound: rows x (max_length + 4) plus the nullable bitmap.
Without a valid schema bound, the recorded size is retained. A projecting
reader can cap non-nullable fixed-width fields at their schema width. For a
nullable fixed-width field, the estimator keeps the recorded size when available.
The fallback schema bound for a nullable fixed-dimension vector includes its
validity bitmap and `4 * (rows + 1)` bytes for Arrow offsets. V2 file readers
keep the whole-group estimate for fixed-width fields. The VarChar cap estimates
field input, not the peak memory of decoding a whole group. Stats deduplicates
shared column groups as logical input; parallel field readers and index-building
allocations are additional overhead represented by the stats factor, not a
proven upper bound on RSS.

Coordinator and worker share CanMergeSort, including namespace sort order and
fan-in limits. The worker uses parameters serialized into the plan by DataCoord.
Merge sort keeps all input readers and delete maps alive; mergeSplit keeps only
the current input's delete map. V1 reader budgets use the largest encoded plus
decoded binlog chunk per field. Packed readers use the existing nominal read-buffer working-set estimate,
bounded by the recorded input size. They no longer charge twice every entire
segment. Backend row-group/chunk sizes are not a hard byte bound.
Oversized row groups, retained FFI backing chunks, skewed variable-width rows
and TEXT LOBs can exceed this estimate and require measurement-based tuning.

## Configuration and validation

The existing refreshable `dataCoord.taskResource.*` settings remain: vectorIndexCPU
8, analyzeCPU 8, clusteringCompactionCPU 8, defaultCPU 1; indexMemoryFactor,
statsMemoryFactor, l0CompactionMemoryFactor, analyzeMemoryFactor and
importMemoryFactor each 2; minTaskMemory 64MB; externalRefreshMemory 1GB.

Regression tests cover exact selected-price propagation, memory-only admission,
task bookings across scheduling rounds, exclusive progress and FIFO blocking, local
completion during slow persistence, cancellation, cache refresh, column-group
deduplication and packed-reader estimates. Go tests use `-tags dynamic,test`
and `-gcflags="all=-N -l"`. Protocols and generated mocks are regenerated, and
the same behavioral changes are maintained on master and 3.0.
