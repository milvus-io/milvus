# Async Storage V3 Field-Data Loading

- **Created:** 2026-08-11
- **Author(s):** @sparknack
- **Status:** Under Review
- **Component:** QueryNode, Segcore, Storage, Caching Layer
- **Related Issues:** [milvus-io/milvus#51245](https://github.com/milvus-io/milvus/issues/51245)
- **Implementation:** [milvus-io/milvus#51246](https://github.com/milvus-io/milvus/pull/51246)

## Summary

This design adds an opt-in asynchronous load path for field data backed by the
Storage V3 manifest and column-group reader. The existing caching-layer
`Translator` interface remains synchronous, but the work behind
`ManifestGroupTranslator::get_cells()` is decomposed into coroutine-based read
windows that can overlap remote reads without dedicating one load worker to
each blocking storage request.

The design has five main properties:

1. Requested cache cells are sorted by their physical row-group position and
   grouped into byte-bounded, contiguous read windows.
2. Every window acquires a process-wide transient-memory budget lease before
   its storage read is submitted.
3. Remote reads run through the native asynchronous `ChunkReader` API on a
   priority-aware executor.
4. In-memory finalization stays on the load executor, while mmap finalization
   may move to a dedicated local-file executor so blocking file writes do not
   occupy remote-load workers.
5. Caller cancellation and the first sibling failure cancel pending windows;
   failures are published before a window releases its budget lease.

The path is guarded by the temporary
`queryNode.segcore.storageV2.enableAsyncLoad` switch, which defaults to
`false`. The legacy `LoadCellBatchAsync` path remains available during rollout.

This PR covers Storage V3 field data and JSON key-stat column groups. It does
not make scalar-index loading asynchronous.

## Terminology

This document follows the names used by the current Segcore implementation:

| Term | Meaning |
|---|---|
| cache cell | The caching-layer unit returned by a translator. One cell contains one or more adjacent row groups. |
| row group / chunk | A physical reader unit addressed by `ChunkReader`; `chunk_indices` are row-group indices in this path. |
| read window | One async storage request containing one or more adjacent cache cells and all row groups belonging to them. |
| loaded bytes | The estimated final decoded size used to split read windows. |
| transient bytes | The estimated peak temporary memory held from admission through finalization. This is what the global budget charges. |
| finalization | Converting Arrow record batches into Milvus `GroupChunk` data, including local mmap-file materialization when enabled. |

This feature is part of the Storage V3 load path. Some existing implementation
identifiers still use the historical `storageV2` name, including the
`storagev2translator` namespace and the
`queryNode.segcore.storageV2.*` configuration keys. Those identifiers are kept
for compatibility and do not define the feature's public terminology.

## Motivation

### Existing behavior

The existing manifest translator loads cells through `LoadCellBatchAsync`:

```text
cache miss
  -> build CellSpec entries
  -> group adjacent cells into batches
  -> submit batch to the load thread pool
  -> call synchronous ChunkReader::get_chunks()
  -> materialize Arrow tables into GroupChunk
  -> return futures to the synchronous translator
```

This path already merges adjacent row groups and applies load admission, but a
worker executing a batch remains tied to the synchronous storage call. For a
wide schema, many row groups, or high object-storage latency, useful parallelism
therefore depends on occupying more load threads.

### Desired behavior

The new path should allow one request to have multiple admitted remote reads in
flight while preserving the existing cache-cell contract and controlling peak
temporary memory. It must also retain load priority, cancellation, mmap
semantics, and error categories across coroutine and executor boundaries.

## Goals

- Use the native `ChunkReader::get_chunks_async()` path for Storage V3
  field-data reads.
- Overlap independent read windows without blocking a worker for each remote
  I/O wait.
- Bound admitted transient data across concurrent load calls with the existing
  process-wide load budget.
- Preserve high- and low-priority load scheduling in both budget admission and
  executor queues.
- Preserve the requested cache-cell result order even though physical reads are
  sorted and may complete out of order.
- Keep memory-mode and mmap-mode finalization behavior compatible with the
  existing translator.
- Cancel pending sibling windows after the first failure and propagate the
  original typed error.
- Allow a disabled-by-default rollout with the legacy path as fallback.
- Reuse the same async pipeline for normal manifest field data and JSON
  key-stat column groups.

## Non-Goals

- Async scalar-index loading. That is a follow-up under issue #51245.
- Removing `LoadCellBatchAsync` or the legacy translator path in this PR.
- Changing the public caching-layer `Translator` interface to return a future
  or coroutine.
- Making `ChunkReader` creation asynchronous. Reader preparation remains
  synchronous before the translator is installed.
- Changing cache-cell sizing or cache eviction semantics.
- Allowing a zero-byte read window. A read-window target must be positive.
- Guaranteeing immediate cancellation of a storage request that has already
  been started by the storage library. The pipeline prevents further useful
  work and waits for started children to finish safely.
- Adding dedicated per-window metrics in this PR.

## Architecture

### Integration boundary

`ManifestGroupTranslator` continues to implement the synchronous caching-layer
interface:

```cpp
std::vector<CellResult>
ManifestGroupTranslator::get_cells(OpContext* ctx,
                                   const std::vector<cid_t>& cids);
```

The rollout mode is captured when a translator is constructed. A cache miss
then selects one of two implementations:

```text
ManifestGroupTranslator::get_cells()
  |
  +-- enable_async_load_ == false
  |     -> get_cells_legacy()
  |     -> LoadCellBatchAsync()
  |
  +-- enable_async_load_ == true
        -> get_cells_via_async_pipeline()
        -> blockingWait(LoadCellsAsync())
```

The outer call is still synchronous because the caching-layer contract is
unchanged. The asynchronous benefit is inside `LoadCellsAsync`: storage waits
and sibling windows compose as coroutines on dedicated executors rather than as
one blocking storage operation per load task.

The switch is passed into translators created for:

- eager and lazy Storage V3 sealed-segment column groups; and
- eager and lazy JSON key-stat column groups.

### End-to-end flow

```text
requested cache cell IDs
        |
        v
build CellSpec for each cell
  - row-group start/count
  - estimated loaded bytes
  - estimated transient overhead
        |
        v
BuildAsyncReadWindows
  - stable sort by (file_idx, local_rg_offset)
  - split on file boundary, row-group gap, or byte threshold
        |
        v
for each window, in physical order
  await transient-budget admission
        |
        v
schedule admitted window on priority load executor
        |
        v
ChunkReader::get_chunks_async(chunk_indices, parallelism=1)
        |
        +------------------------------+
        | memory finalization          | mmap finalization
        v                              v
load executor / read continuation   LocalFileIOPool when enabled
        |                              |
        +---------------+--------------+
                        v
             release Arrow batches and budget lease
                        |
                        v
             join all window coroutines
                        |
                        v
             restore original cell request order
```

## Cell and Window Planning

### Cell metadata

The translator maps each requested cache cell to a `CellSpec`:

```cpp
struct CellSpec {
    int64_t cid;
    size_t file_idx;
    int64_t local_rg_offset;
    int64_t rg_count;
    int64_t memory_size;
    int64_t loading_overhead_size;
};
```

For the manifest translator in this PR, all cells refer to one logical
`ChunkReader`, so `file_idx` is `0`. The generic planner retains `file_idx` so a
window can never cross a physical-file boundary if other callers use it later.

`memory_size` is derived from the translator's row-group size estimates. The
translator prefers projected-column estimates, then uses the existing sampled,
aggregate, or last-resort fallbacks. This matters for lazy projected columns:
the window target should reflect the columns that will actually be decoded,
not unrelated columns in the same physical column group.

`loading_overhead_size` is the admission charge. It equals `memory_size` for
most field types and is conservatively doubled for array fields, whose Arrow
normalization can retain additional temporary buffers.

### Contiguous-window invariant

`BuildAsyncReadWindows` stable-sorts cells by
`(file_idx, local_rg_offset)`. A new window starts if any of these conditions is
true:

1. the next cell belongs to another file;
2. the next cell does not start exactly at the current window's row-group end;
3. adding the cell would exceed the configured loaded-byte target.

Therefore every current read window contains a contiguous row-group sequence.
The storage API itself accepts arbitrary chunk-index vectors, so continuity is
a pipeline planning decision rather than an API requirement. It preserves
merged sequential reads and avoids silently combining sparse cache misses into
one large logical window.

For example, assume the requested cells become the following physical ranges
after sorting:

```text
cell 0: row groups [0, 2), estimated 4 MiB
cell 1: row groups [2, 4), estimated 4 MiB
cell 2: row groups [4, 6), estimated 4 MiB
cell 3: row groups [8, 9), estimated 1 MiB
```

With an 8 MiB target, the planner creates:

```text
window 0: row groups [0, 4)  -> cells 0, 1
window 1: row groups [4, 6)  -> cell 2
window 2: row groups [8, 9)  -> cell 3, split because [6, 8) is a gap
```

The threshold is a target, not a hard maximum. A cell is never split by this
planner, so one oversized cell forms an oversized single-cell window. This is
required for forward progress and keeps cache-cell finalization atomic.

### Loaded-byte target versus transient budget

Two byte counts intentionally serve different purposes:

| Value | Used for | Reason |
|---|---|---|
| sum of `memory_size` | deciding where to split a read window | approximates the useful decoded result size and I/O granularity |
| sum of `loading_overhead_size` | transient-budget lease | approximates peak temporary memory through Arrow decoding and finalization |

Using the loaded size for grouping prevents a type-specific overhead factor
from unexpectedly changing I/O granularity. Using the overhead size for
admission prevents the same factor from being ignored by memory control.

## Async Scheduling

### Lazy coroutine entry

`LoadCellsAsync` returns a lazy `folly::coro::Task`. No admission or storage
work starts until the task is awaited. The function captures the executor
keep-alive token and the `OpContext` cancellation token before returning, so a
deferred task cannot outlive its executor or accidentally read a later context
state.

### Load executor

The default executor is a process-wide `folly::CPUThreadPoolExecutor` with:

- `max(1, CPU_NUM)` workers;
- two priority queues;
- thread name prefix `MILVUS_ASYNC_LOAD_`.

Milvus `LoadPriority::HIGH` maps to Folly high priority and
`LoadPriority::LOW` maps to Folly low priority. The continuation after an
asynchronous storage future is also rebound to this executor and priority.

The pipeline can accept a custom executor for tests or future integration. A
custom executor must defer submitted work, support Folly keep-alive semantics
or outlive the task, and implement priority submission if it advertises more
than one priority.

### Window submission

The parent coroutine walks windows in physical order. For each window it first
awaits budget admission, then adds the admitted window to an `AsyncScope`.
This ordering has two effects:

- a window cannot enter the executor queue before its transient memory is
  reserved; and
- the number of submitted-but-not-finished windows is bounded by budget
  capacity when a non-zero capacity is configured.

All children are joined before the parent returns or throws. Each child writes
only its own per-window optional slot; the parent assembles the final ordered
result vector after the join.

### Storage read

Each window calls:

```cpp
chunk_reader->get_chunks_async(window.chunk_indices, /*parallelism=*/1)
```

Window-level concurrency is owned by the Milvus pipeline. The per-call
parallelism is set to one to avoid multiplying concurrency inside each window.
The storage reader returns record batches in the same order as the requested
chunk indices; the pipeline validates the returned count before finalization.

The `ChunkReader` interface has a synchronous default implementation of
`get_chunks_async()`, so true non-blocking behavior depends on the selected
storage format providing the native async override. This PR pins a
milvus-storage revision containing that implementation and optionally exposes
the CRT-backed S3 build path through `WITH_CRT`.

## Transient-Memory Admission

The pipeline uses the process-wide
`storage::TransientMemoryBudget::GetLoadTransientBudget()` instance. The same
budget is shared with other load-time streaming paths, including scalar-index
V3 entry streaming, so concurrent subsystems are controlled by one transient
memory ceiling.

For each window:

1. Sum `loading_overhead_size` across its cells.
2. Await `AcquireAsync(bytes, priority, cancellation_token)`.
3. Move the returned RAII lease into the window coroutine.
4. Hold it across remote read and finalization.
5. Release it when temporary Arrow data and the finalization frame unwind.

Admission has the following semantics:

- capacity `0` means unlimited;
- high-priority waiters are considered before low-priority waiters;
- FIFO order is preserved within one priority queue;
- a request larger than total capacity may run only when no other bytes are in
  flight, preventing permanent starvation;
- cancellation removes a pending waiter and immediately re-evaluates the queue;
- a runtime capacity increase or disabling the limit wakes eligible waiters.

The async feature can technically run with capacity `0`, but production
rollout should configure `common.loadTransientBudgetBytes` to a positive value.
Otherwise the read-window size limits one storage operation but does not bound
the number of simultaneously admitted windows across segments.

## Finalization and Local File I/O

### Memory-backed fields

For non-mmap fields, Arrow-to-`GroupChunk` conversion runs in the storage-read
continuation on the load executor. This avoids an unnecessary extra scheduling
hop after the remote future completes.

### Mmap-backed fields

Mmap finalization creates local files and performs blocking write syscalls. If
`common.diskWriteNumThreads` is positive, the translator asks
`LocalFileIOPool` for a keep-alive token only after the remote read succeeds and
then schedules the complete finalization task there.

`LocalFileIOPool` is a priority-aware `CPUThreadPoolExecutor` because the work
is blocking file I/O, not EventBase-driven async I/O. It uses the existing disk
writer thread-count configuration and maps load priority onto its high/low
queues.

The keep-alive is intentionally acquired after remote I/O. Reconfiguring or
disabling the local-file pool therefore does not wait for unrelated remote
reads. Once finalization has been queued, pool shutdown drains that work before
retiring the executor.

`FileWriter` remains synchronous. A global `WritePermit` limits concurrent
blocking writes to the configured local-file worker count across `FileWriter`
callers, and the existing disk write rate limiter and load priority remain in
effect.

If `common.diskWriteNumThreads` is `0`, no dedicated local-file executor exists
and mmap finalization falls back to the async load executor. This preserves the
existing default configuration while allowing deployments to isolate blocking
local writes during async-load rollout.

## Cancellation and Failure Propagation

### Cancellation sources

The parent coroutine merges:

- the `OpContext` cancellation token captured at `LoadCellsAsync` call time;
- the cancellation token of the coroutine awaiting the task; and
- an internal sibling-cancellation token owned by `WindowFailureState`.

Cancellation is checked before admission work, after storage read, between cell
finalizations, and before completion.

### First-failure protocol

All window layers share a `WindowFailureState` containing the first exception
and a cancellation source. A read or finalization failure performs this order:

```text
record the first exception
  -> request sibling cancellation
  -> unwind the current coroutine
  -> release the current window's budget lease
```

Publishing failure before releasing the lease is important. If the lease were
released first, the budget queue could admit the next window and start another
storage read before sibling cancellation became visible.

The internal catch points cover the read coroutine, optional local-I/O
finalization coroutine, and result-storage coroutine. This keeps the ordering
valid even when execution crosses to another executor.

Pending budget admission is cancellable. A sibling that already started a
storage operation may still complete inside the storage library, but it checks
the merged token before finalization. The parent joins every child before
returning, which prevents references to parent-owned result slots from escaping.

After the join:

1. caller/context cancellation is surfaced if requested;
2. otherwise the first recorded failure is rethrown;
3. only a failure-free request assembles results.

Arrow and storage statuses are translated through
`milvus_storage::ToSegcoreError`, preserving typed storage error categories at
the Segcore boundary.

## Result Ordering and Ownership

Physical planning is independent of caller order. Every sorted cell carries
its original `request_index`. Window finalization returns
`(request_index, cell_result)` pairs, and the parent fills an ordered optional
slot for every requested cell.

Before returning, the pipeline verifies that:

- every window produced a result;
- every request index is in range; and
- every original request slot was populated.

The caller therefore sees the same ordering contract as the legacy translator,
even if windows were reordered for I/O or completed out of order.

The transient budget lease lives through finalization. This is deliberate:
remote record batches are not considered consumed until the final
`GroupChunk` has been built and the temporary Arrow ownership can unwind.

## Configuration and Rollout

| Parameter | Default | Refresh behavior | Purpose |
|---|---:|---|---|
| `queryNode.segcore.storageV2.enableAsyncLoad` | `false` | watched dynamically; mode is captured by newly constructed translators | temporary rollout and rollback switch |
| `queryNode.segcore.storageV2.asyncLoadReadWindowSizeBytes` | `16777216` (16 MiB) | watched dynamically; non-positive values fall back to 16 MiB | target loaded bytes per contiguous read window |
| `common.loadTransientBudgetBytes` | `0` (unlimited) | refreshable | process-wide admitted transient bytes across load paths |
| `common.diskWriteNumThreads` | `0` | applied through disk-writer configuration | optional local mmap-finalization executor and write concurrency limit |

The async switch is intentionally not exported in the generated public config
surface. It is a temporary operational control, not a long-term user-facing
feature contract.

The switch is captured at translator construction. Changing it does not mutate
translators that already exist:

- enabling it affects newly created column-group translators;
- disabling it stops new translators from selecting the async path;
- existing async translators keep using the selected mode until their segment
  state is replaced or released.

The read-window target is looked up when the async task runs, so an updated
positive value affects subsequent loads performed by existing async
translators. The transient budget is process-wide and also applies immediately
to subsequent admissions.

Recommended rollout sequence:

1. Configure a positive `common.loadTransientBudgetBytes` appropriate for the
   QueryNode memory envelope.
2. Optionally configure `common.diskWriteNumThreads` when mmap finalization
   should be isolated from the async load executor.
3. Enable `queryNode.segcore.storageV2.enableAsyncLoad` on a limited set of
   QueryNodes.
4. Reload or replace test segments so their translators capture the new mode.
5. Compare load latency, peak memory, object-storage errors, cancellation, and
   query availability with the legacy cohort.
6. Disable the switch for new translators if rollback is needed.

## Compatibility

- The caching-layer translator API and returned `GroupChunk` representation do
  not change.
- Cache keys, cell IDs, warmup policy, and eviction support are unchanged.
- The legacy path remains the default.
- Reader construction remains synchronous, limiting the initial scope to chunk
  reads and finalization.
- The async mode is captured per translator, avoiding a mid-load mode switch.
- Memory and mmap finalization use the same `load_group_chunk()` implementation
  as the legacy path.
- JSON key stats use the same translator and async pipeline instead of a
  separate scheduler.
- Invalid read-window configuration is normalized to 16 MiB; the pipeline does
  not implement a special zero-window mode.

## Alternatives Considered

### Keep using synchronous reads on a larger load pool

This increases object-storage concurrency by adding blocked workers. It couples
I/O latency to thread count and increases scheduling and stack overhead under
many segments.

### Submit one async request per cache cell

This maximizes task count and can turn adjacent row groups into many small
storage calls. Contiguous windows retain I/O merging while allowing bounded
overlap.

### Put every requested cell into one async request

This reduces task count but makes one slow or large request retain all
transient data and weakens memory admission granularity. Sparse cache misses
would also be combined into one logical window.

### Build sparse, non-contiguous windows

`get_chunks_async()` supports arbitrary indices, so this is possible. The
current design splits on gaps to keep each window physically contiguous and
its byte estimate easy to reason about. A future storage-aware planner may
choose sparse batching if measurements show a benefit.

### Release budget immediately after remote read

Arrow buffers and normalization temporaries remain live through finalization.
Releasing at read completion would under-account peak transient memory.

### Run mmap finalization on the remote-load executor only

Blocking local writes can occupy the same workers that should resume remote
read continuations. The optional local-file executor provides isolation while
retaining a fallback for the default zero-thread configuration.

### Make reader creation asynchronous in the first rollout

Reader preparation has different metadata and lifetime failure modes. Keeping
it synchronous reduces the first rollout's scope and makes the async boundary
start at a fully constructed shared `ChunkReader`.

## Testing Strategy

The implementation adds focused tests for the following contracts:

- contiguous window construction, gap splitting, oversized cells, positive
  read-window validation, and original-order restoration;
- lazy task behavior and execution off the caller thread;
- async storage continuation binding to the configured executor and priority;
- budget-before-submit ordering, high-priority admission, FIFO behavior,
  oversized admission, dynamic capacity, and cancellation races;
- budget release on storage, finalization, scheduling, and cancellation errors;
- first-failure publication before budget release;
- cancellation while waiting for budget, after storage read, and between cell
  finalizations;
- in-memory versus mmap finalization executor selection;
- local-file pool reconfiguration, draining, concurrency limiting, and write
  error preservation;
- typed storage error preservation;
- eager and lazy manifest translators, including projected estimates and JSON
  key-stat integration;
- disabled-switch compatibility with the legacy synchronous reader path.

The PR is verified with the C++ unit-test build, targeted async-load and sealed
segment tests, and `make verifiers`.

## Follow-Up Work

- Add the async scalar-index path described by issue #51245.
- Add dedicated window/admission/read/finalization latency and in-flight byte
  metrics before broad rollout.
- Validate production budget and read-window defaults with object-storage and
  mmap workloads.
- Decide whether native async storage support must become a hard requirement
  instead of allowing the synchronous `get_chunks_async()` fallback.
- Consider a storage-aware sparse-window planner only if contiguous windows
  leave measurable throughput on the table.
- Enable the path by default after rollout validation, then remove the
  temporary switch and legacy-only concepts in a separate cleanup.

## Key Source Files

| Area | Files |
|---|---|
| coroutine pipeline and window planner | `internal/core/src/segcore/storagev2translator/AsyncLoadPipeline.{h,cpp}` |
| manifest translator integration | `internal/core/src/segcore/storagev2translator/ManifestGroupTranslator.{h,cpp}` |
| per-process async configuration | `internal/core/src/segcore/storagev2translator/StorageV2Config.h` |
| cell metadata and size planning | `internal/core/src/segcore/storagev2translator/GroupCTMeta.h` |
| transient-memory admission | `internal/core/src/storage/EntryStreamUtils.h` |
| mmap finalization executor | `internal/core/src/storage/LocalFileIOPool.{h,cpp}` |
| synchronous local file writer and permits | `internal/core/src/storage/FileWriter.{h,cpp}` |
| sealed segment translator construction | `internal/core/src/segcore/ChunkedSegmentSealedImpl.cpp` |
| JSON key-stat integration | `internal/core/src/index/json_stats/JsonKeyStats.cpp` |
| C/Go configuration bridge | `internal/core/src/common/init_c.{h,cpp}`, `internal/util/initcore/` |
| parameter definitions | `pkg/util/paramtable/component_param.go`, `configs/milvus.yaml` |
| storage dependency/build option | `internal/core/thirdparty/milvus-storage/CMakeLists.txt`, `scripts/core_build.sh`, `Makefile` |
