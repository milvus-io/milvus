# Async Scalar Index V3 Loading

- **Created:** 2026-08-19
- **Status:** Draft; implementation is under review
- **Components:** QueryNode, Segcore, Scalar Index V3, Milvus Storage
- **Related issue:** [milvus-io/milvus#51245](https://github.com/milvus-io/milvus/issues/51245)
- **Parent design:** [Async Storage V3 Field-Data Loading](20260811-async-storage-v3-field-data-loading.md)

## Summary

This design adds an opt-in asynchronous load path for plaintext entries in a
Scalar Index V3 packed file. The public `ScalarIndex::LoadUnified()` boundary
remains synchronous, but entry materialization is split into four phases:

```text
PlanLoad
  -> bounded Slice materialization
  -> EntryFinalize
  -> FinalizeLoad
```

The split lets an index decide every entry's memory or file target before
remote reads start. Independent slices can then complete out of order and
write into non-overlapping caller-owned regions. If the storage backend
supports native caller-owned reads, bytes land in the target directly. Other
`RemoteInputStream` implementations use Arrow `ReadAsync()` plus one bounded
copy into the same target; they no longer force the whole index back to the
synchronous loader.

The implementation has six main properties:

1. `IndexEntryReader::Open()` synchronously inspects the V3 footer, directory,
   and metadata and exposes an immutable `IndexEntryCatalog`.
2. Each participating index builds a complete `IndexLoadPlan`, including all
   entry targets and Milvus-side slices, before materialization starts.
3. Plaintext slices use one async read-into-target interface. Native storage
   implementations write into the target directly; the generic Arrow path
   reads into a temporary Arrow buffer and copies into the target.
4. One admission loop walks entries round-robin, awaits at most one budget
   request at a time, and also limits the number of in-flight slice tasks.
5. Per-slice CRCs are combined in logical order at EntryFinalize; the final
   index is constructed only after all required entries are ready and all
   issued operations have drained.
6. Field-data and scalar-index loading share one physical priority load
   executor. Blocking local-file preparation uses the existing
   `LocalFileIOPool`; no scalar-index-specific executor is introduced.

The path is guarded by the temporary
`queryNode.segcore.storageV2.enableAsyncLoad` switch, which defaults to
`false`. Unsupported index layouts, encrypted entries, and non-
`RemoteInputStream` readers continue through the synchronous `LoadEntries()`
implementation. Native direct-into-target capability is an optimization, not
an eligibility requirement.

The transient-budget charge used by this path is deliberately conservative.
It controls Milvus admission and range concurrency but is not a measurement or
hard bound of AWS CRT's internal request memory.

## Terminology

| Term | Meaning |
|---|---|
| packed file | The single Scalar Index V3 object containing entry payloads, `__meta__`, the entry directory, and the footer. |
| catalog | Immutable metadata produced while opening the packed file: entry names, plaintext sizes, CRCs, source ranges, encryption layout, and index metadata. |
| entry | One named logical payload in the packed file, such as Sort data, an FM-index blob, or one Tantivy file. |
| slice | A Milvus-planned range of one plaintext entry. One slice maps one remote range to one non-overlapping target range. |
| entry target | Preallocated heap memory or a region of a pre-sized writable mmap into which slices are ultimately placed. |
| admission bytes | The conservative byte charge held for one slice from admission through read, CRC, and placement. It is not CRT's actual allocation. |
| EntryFinalize | Ordered CRC combination and readiness transition for one entry after its last slice completes. |
| IndexFinalize | Index-specific construction after every required entry is ready and every slice task has drained. |
| artifact | The materialized entries, index-specific finalization context, and transactional staging-file ownership passed to `FinalizeLoad()`. |

This feature uses the existing Storage V3 rollout and admission foundation.
Some implementation identifiers retain the historical `storageV2` name,
including the rollout switch and `storagev2translator` namespace. Those names
are kept for compatibility.

## Motivation

### Existing behavior

The packed-index reader exposes pull-oriented entry APIs. An index's
`LoadEntries()` method decides what to read and where to put it while loading
is already in progress:

```text
ScalarIndex::LoadUnified
  -> IndexEntryReader::Open
  -> index-specific LoadEntries
       -> ReadEntry / ReadEntryToFile / stream consumer
       -> copy or pwrite into local representation
       -> deserialize or open index
```

This works for synchronous loading, but the common storage layer cannot plan
concurrency across entries because target allocation and read order remain
inside each index implementation. Mmap-backed indexes may also pass through a
Milvus slice buffer and a local write even when the storage backend can read
directly into caller-owned memory.

Making only `ReadEntry()` asynchronous does not solve that ownership problem.
The caller would still decide the next entry and its destination incrementally,
so independent entries could not be admitted fairly or safely materialized out
of order by a common pipeline.

### Implemented behavior

The new path separates index policy from common byte movement:

- the index owns entry selection, target layout, metadata validation, and final
  index construction;
- the common materializer owns bounded admission, asynchronous reads, target
  placement, CRC verification, cancellation, draining, and staging cleanup.

Because every target exists before the first slice starts, a completion never
depends on another slice's completion order.

## Goals

- Use asynchronous reads into planned caller-owned targets for plaintext
  Scalar Index V3 entries, with native direct placement as the fast path.
- Allow slices from different entries to overlap without allowing one large
  entry to monopolize admission order.
- Avoid a Milvus-owned download buffer and subsequent copy or `pwrite` when
  the storage backend can place persisted bytes directly into their target.
- Keep the same planned pipeline available when storage must return an Arrow
  buffer before copying into the target.
- Preserve existing memory-mode, mmap-mode, index-format, and query semantics.
- Preserve high- and low-priority scheduling in both budget admission and the
  shared load executor.
- Validate the complete plan before creating staging files or issuing remote
  reads.
- Verify entry CRCs even when slices complete out of order.
- Cancel pending siblings after the first failure, drain already-issued work,
  and preserve typed storage errors.
- Keep staging-file publication transactional with index construction.
- Retain a disabled-by-default rollout and synchronous fallback.

## Non-Goals

- Making footer, entry-directory, or `__meta__` inspection asynchronous.
- Supporting planned materialization of encrypted entries in the first
  version.
- Proving a hard byte bound for AWS CRT internal buffers or treating CRT's
  `downloadMemoryUsageWindow` as a process-wide Milvus lease.
- Eliminating all final heap allocations. Validity bitmaps, null offsets,
  wrapper metadata, and index-specific derived structures remain final memory.
- Changing the Scalar Index V3 on-disk format.
- Converting Bitmap's normal Roaring representation directly into its final
  frozen mmap representation.
- Removing `LoadEntries()` or the synchronous V3 entry-stream APIs during
  rollout.
- Changing the synchronous `IndexBase::Load` or caching-layer interfaces.
- Adding a scalar-index-specific materialization or disk executor.
- Guaranteeing immediate cancellation of a remote request already started by
  the storage library. Issued work is drained before state is destroyed.

## Architecture

### Integration boundary

`ScalarIndex::LoadUnified()` remains the synchronous entry point:

```cpp
void
ScalarIndex<T>::LoadUnified(const Config& config, OpContext* op_ctx);
```

Opening the packed file and constructing `IndexEntryReader` remain
synchronous. After inspection, the implementation selects the planned async
path only when all of the following are true:

1. `queryNode.segcore.storageV2.enableAsyncLoad` is enabled;
2. the concrete index returns `true` from `SupportsPlannedLoad()`;
3. the reader returns `true` from `SupportsAsyncPlainSliceRead()`, which
   currently means it wraps a `RemoteInputStream`;
4. every catalog entry is plaintext.

Otherwise it calls the existing `LoadEntries()` implementation.

```text
ScalarIndex::LoadUnified
  |
  +-- open stream and synchronously inspect V3 metadata
  |
  +-- planned-path preconditions are false
  |     -> LoadEntries(reader, config)
  |
  +-- planned-path preconditions are true
        -> bind to the HIGH/LOW view of the shared LoadExecutor
        -> blockingWait(LoadUnifiedAsync())
             -> PlanLoad(catalog, config)
             -> MaterializeIndexAsync(reader, plan, token)
             -> FinalizeLoad(artifact, config)
             -> CommitTargets()
```

The only `blockingWait()` in the new path is at this outer synchronous
boundary. `LoadUnifiedAsync()` and the common materializer compose with
`co_await` and do not block a load worker while waiting for remote I/O or
budget admission.

### End-to-end flow

```text
MIDDLE / Search caller
        |
        v
ScalarIndex::LoadUnified
        |
        v
synchronous IndexEntryReader::Open
  - footer and entry directory
  - __meta__ JSON
  - immutable IndexEntryCatalog
        |
        v
index-specific PlanLoad
  - validate index metadata and entry sizes
  - allocate final heap targets
  - describe writable mmap targets
  - split entries into Milvus slices
        |
        v
common plan validation
  - unique entry names
  - plaintext source, size, and CRC match
  - contiguous slice coverage
  - in-bounds, non-overlapping target ranges
        |
        v
prepare mmap targets on LocalFileIOPool when configured
  - create parent directory
  - open and truncate file
  - writable MAP_SHARED mapping
        |
        v
single round-robin admission loop
  - at most one pending AcquireAsync
  - max_inflight_slices bound
        |
        v
SliceTask on the priority LoadExecutor
  - ReadAtAsyncInto target range
      - native direct-into-target when supported
      - otherwise Arrow ReadAsync + copy into target
  - verify returned byte count
  - scan target range for CRC-32C
  - save slice CRC by sequence
  - release lease when task frame unwinds
        |
        v
EntryFinalize on the last successful slice
  - ordered CRC combine
  - compare expected entry CRC
  - mark entry READY
        |
        v
drain issued SliceTasks and join AsyncScope
  - rethrow first failure, or
  - assert all required entries READY
  - close every writable mmap
        |
        v
index-specific FinalizeLoad on the LoadExecutor
  - read-only mmap / deserialize / open directory
  - install complete index state
        |
        v
commit retained staging targets
```

## Catalog Inspection

### Synchronous inspection

`IndexEntryReader::Open()` reads the V3 footer and directory and parses index
metadata before the async task starts. It builds an `IndexEntryCatalog` next to
the reader's existing lookup structures.

For a plaintext entry, the catalog records:

```cpp
struct PlainEntrySource {
    uint64_t remote_offset;
    size_t remote_bytes;
};

struct IndexEntryCatalogEntry {
    std::string name;
    size_t plaintext_size;
    uint32_t expected_crc;
    std::variant<PlainEntrySource, EncryptedEntrySource> source;
};
```

The source offset is absolute within the packed remote object, including the
V3 magic prefix. The catalog also exposes immutable access to parsed `__meta__`
values so `PlanLoad()` does not perform remote I/O.

Encrypted source metadata is represented in the catalog for inspection and
future planning, but the current materializer accepts plaintext sources only.
The outer gate sends any packed file containing encrypted entries to
`LoadEntries()`.

### Why inspection remains synchronous

Reader construction already has established format validation and error
handling for the footer, directory, metadata, and encryption descriptors.
Keeping it synchronous limits the coroutine boundary to materialization and
avoids mixing metadata lifetime changes into the first rollout.

## Index Planning

### Two-phase index interface

Indexes that support planned plaintext loading implement:

```cpp
virtual IndexLoadPlan
PlanLoad(const IndexEntryCatalog& catalog, const Config& config);

virtual void
FinalizeLoad(IndexLoadArtifact&& artifact, const Config& config);
```

`PlanLoad()` may validate index-specific metadata and allocate final targets,
but it does not perform remote reads. `FinalizeLoad()` consumes fully verified
targets and constructs index state without remote I/O.

Index-specific state needed between these methods is stored in
`IndexLoadPlan::finalize_context`. The common materializer carries this
`std::any` into `IndexLoadArtifact` without interpreting it.

### Target model

The implemented common target types are:

```cpp
struct MemoryEntryTarget {
    std::shared_ptr<void> owner;
    uint8_t* data;
    size_t bytes;
};

struct MmapFileTarget {
    std::string path;
    size_t file_size;
    bool retain_on_success;
    std::shared_ptr<WritableMmapFile> file;
};

struct MmapEntryTarget {
    std::shared_ptr<MmapFileTarget> staging;
    size_t offset;
    size_t bytes;
};

using EntryTarget = std::variant<MemoryEntryTarget, MmapEntryTarget>;
```

The owner in a memory target keeps the allocation alive across every slice and
through IndexFinalize. Multiple entries may share one `MmapFileTarget` and use
different non-overlapping regions, as the persisted Marisa CSR entries do.

`retain_on_success` distinguishes files that remain part of the final mmap or
directory from temporary staging files used only while a memory-backed index
is opened or deserialized.

### Slice plan

`MakePlainEntryLoadPlan()` divides one entry into fixed-size ranges based on
the existing default V3 entry-stream slice size:

```cpp
struct SlicePlan {
    size_t seq;
    uint64_t entry_offset;
    size_t remote_bytes;
    size_t target_offset;
    size_t target_bytes;
    size_t admission_bytes;
};
```

`entry_offset` is relative to the logical entry. `IndexEntryReader` resolves it
against the catalog's absolute source offset. The current plaintext helper maps
the same logical range to the same target offset and charges the range size as
`admission_bytes`.

An empty entry has no slices. It is finalized by computing and validating the
CRC of an empty byte sequence.

### Common plan validation

The materializer validates the entire plan before preparing any mmap target or
issuing any read. It enforces:

- unique planned entry names;
- a matching plaintext catalog entry, size, and expected CRC;
- a target at least as large as the entry;
- contiguous slice sequence numbers and logical coverage with no holes;
- equal remote and target byte counts for plaintext slices;
- positive slice and admission sizes;
- in-bounds target regions; and
- no overlapping write ranges between memory targets or between regions of
  the same mmap file.

Index-specific planners additionally validate persisted element sizes,
metadata consistency, file-name safety, duplicate file names, row counts, and
integer overflow before returning a plan.

## Async Plain-Slice Reads

### Native and buffered implementations

The materializer calls one reader API regardless of whether the storage
backend supports native direct placement:

```cpp
bool
SupportsAsyncPlainSliceRead() const noexcept;

folly::coro::Task<void>
ReadPlainSliceIntoAsync(std::string_view entry,
                        uint64_t entry_offset,
                        uint8_t* destination,
                        size_t destination_bytes,
                        folly::CancellationToken token);
```

`ReadPlainSliceIntoAsync()` delegates to
`RemoteInputStream::ReadAtAsyncInto()`. That method has two implementations:

1. If the remote file implements `AsyncReadAtFile`, call its native
   `ReadAtAsyncInto()` and let storage write directly into the caller-owned
   target. `SupportsNativePlainSliceRead()` reports this fast-path capability.
2. Otherwise call Arrow `RandomAccessFile::ReadAsync()`, receive an allocating
   `arrow::Buffer`, validate its length, and copy it into the caller-owned
   target.

Neither branch performs a synchronous remote read on the LoadExecutor. The
buffered branch does allocate storage-owned temporary memory and performs one
copy, but it keeps the common planned materializer, cross-entry scheduling,
CRC, cancellation, and transactional staging behavior.

`IndexEntryReader::ReadPlainSliceIntoAsync()` performs logical and integer
bounds checks, resolves the remote offset, bridges the Arrow future into a
Folly coroutine future, preserves translated storage errors, checks
cancellation, and rejects a short read.

### Why Milvus still creates slices

AWS CRT may internally split one range request, but its chunks, allocation
lifetime, and completion events are not visible to the Milvus admission loop.
CRT chunking therefore does not replace Milvus slices.

The Milvus slice remains the unit for:

1. process-wide budget admission;
2. cross-entry round-robin scheduling;
3. per-index in-flight request pressure;
4. cancellation and drain ownership; and
5. ordered entry CRC combination.

Each slice still maps to one asynchronous range read. The native path allocates
no Milvus or Arrow download vector for the range. The generic path may have one
Arrow-owned range buffer until the copy into the target completes.

## Async Scheduling

### Shared load executor

Field-data and scalar-index async loading use one process-wide
`PriorityThreadPoolExecutor` with:

- `max(1, CPU_NUM)` workers;
- two priority queues;
- thread name prefix `MILVUS_ASYNC_LOAD_`.

`LoadPriority::HIGH` maps to Folly high priority and `LoadPriority::LOW` maps
to Folly low priority. Priority-specific keep-alive views bind coroutine work
and continuations to the same physical pool.

The previous `segcore/async_load/AsyncLoadExecutor` implementation is removed.
The field-data `AsyncLoadPipeline` now uses `storage::GetLoadExecutor()` by
default, and `ThreadPools::GetLoadExecutorWorkers()` reports the shared pool's
worker count for memory-planning policy.

No `AsyncLoadMaterializeExecutor` or `AsyncLoadDiskExecutor` is added.

### Target preparation

Before slices start, the materializer collects unique mmap descriptors. For
each unprepared target it creates parent directories, opens and truncates the
file to its final size, and establishes a writable `MAP_SHARED` mapping.

This work runs on `LocalFileIOPool` when that pool is configured because file
creation and truncation can block. If the pool is disabled, preparation falls
back to the priority load-executor view. Pure memory targets need no executor
switch.

Both slice paths ultimately write through the mapping and therefore do not
issue a second `pwrite`. Native read-into-target writes there directly; the
generic path copies from its Arrow buffer into the mapped range.

### Round-robin admission

One parent coroutine owns admission for the complete index. It selects slices
in entry round-robin order:

```text
Entry A Slice 0
Entry B Slice 0
Entry C Slice 0
Entry A Slice 1
Entry B Slice 1
...
```

For each selected slice, the parent awaits
`TransientMemoryBudget::AcquireAsync()` before adding a child task to its
`AsyncScope`. Because the loop itself awaits admission, there is at most one
pending acquisition for one index.

The plan also carries `max_inflight_slices`. A value of zero selects the
shared load executor's worker count. This bound remains effective when the
transient budget is configured as unlimited, preventing a large multi-file
index from creating all requests and coroutine frames at once.

When the in-flight limit is reached, the parent waits on a completion queue
before selecting more work. Every child enqueues one completion after success
or failure, so the parent can drain issued tasks without blocking a load
worker.

### Lease lifetime

The RAII admission lease moves into the slice coroutine and remains live
through:

```text
admission
  -> asynchronous remote read
  -> optional Arrow-buffer copy into target
  -> returned-byte validation
  -> target-region CRC scan
  -> per-entry completion bookkeeping
  -> coroutine-frame unwind
```

There is no extra copy on the native path. The buffered path copies once before
the Arrow future completes. In both cases the lease remains non-zero because
it is a conservative range-concurrency charge; it is not ownership of, or a
hard bound on, storage-library temporary memory.

## Entry and Index Finalization

### EntryFinalize

Every entry owns a `RangeCrc` slot for each slice and an atomic count of
remaining slices. A successful slice stores its CRC in the slot indexed by
`seq` and decrements the count.

The last successful slice combines CRCs in logical sequence order:

```text
slice_crcs[0]
  -> Crc32cCombine(slice_crcs[1])
  -> ...
  -> compare with catalog expected_crc
  -> mark entry READY
```

EntryFinalize does not call a generic `Deserialize()`. Index construction may
depend on multiple entries or a complete file directory and remains an
index-level operation.

In the current implementation, writable mmap targets are closed after all
required entries are ready rather than by one entry's finalizer. This supports
files containing multiple planned entry regions and guarantees that no target
mapping is closed while another slice can still reference it.

### Writable mmap lifecycle

`WritableMmapFile` owns the descriptor, writable mapping, path, and commit
state. `Finish()` unmaps the writable region and closes the descriptor while
retaining ownership of the staging path.

The materializer calls `Finish()` for every prepared mmap after all issued
slices have drained and all required entries are ready. `FinalizeLoad()` can
then establish a read-only mapping or open a Tantivy/RTree directory without
overlapping the writable phase.

The implementation does not call `msync()` or `fsync()` merely for visibility
inside the same process. `MAP_SHARED` changes are visible through the backing
file after unmap. Crash durability is not a requirement of this load path.

### IndexFinalize and transactional publication

After successful materialization, `IndexLoadArtifact` contains:

- one ready materialized entry for each plan entry;
- the index-specific `finalize_context`; and
- shared ownership of every uncommitted staging target.

`FinalizeLoad()` runs on the priority load executor. Depending on the index, it
may create a read-only mmap view, deserialize a memory buffer, open a Tantivy
directory, load RTree files, unpack validity or null bits, or rebuild optional
derived metadata.

`LoadUnifiedAsync()` calls `artifact.CommitTargets()` only after
`FinalizeLoad()` succeeds. Files marked `retain_on_success` then remain part of
the completed index. Temporary targets remain uncommitted and are removed when
the artifact unwinds after their contents have been consumed.

If materialization or `FinalizeLoad()` throws, artifact and cleanup guards
destroy every uncommitted `WritableMmapFile`, which unmaps, closes, and unlinks
its staging path. No partially constructed index is intentionally published.

## Cancellation and Failure Propagation

### Cancellation sources

The materializer merges:

- the `OpContext` cancellation token passed from `LoadUnified()`;
- the cancellation token of the coroutine awaiting the task; and
- an internal cancellation source triggered by the first slice failure.

Cancellation is checked before plan validation, during target preparation,
while waiting for budget, before slice finalization, and before successful
completion.

### First-failure protocol

All slice tasks share a `FailureState` containing the first exception and the
internal cancellation source. The first failing task records its exception and
requests sibling cancellation before returning its lease and completion event.

The effects are:

1. pending `AcquireAsync()` observes the merged cancellation token;
2. the admission loop stops creating useful new slice tasks;
3. already-issued remote operations are allowed to complete or cancel;
4. every issued child reports completion;
5. the parent drains the completion queue and joins the `AsyncScope` with
   cancellation disabled; and
6. the first recorded exception is rethrown after drain.

Entry state, target owners, the reader, and staging mappings remain alive until
the join completes. This prevents a late storage callback from writing into
freed memory.

Arrow storage failures pass through `milvus_storage::ToSegcoreError`; the
coroutine bridge does not stringify or replace the typed storage error.

## Memory Accounting

### Final memory versus admission charge

The design separates final index ownership from slice admission:

| Memory category | Examples | Accounting meaning |
|---|---|---|
| final heap memory | memory-mode entry vectors, validity/null bitmaps, offsets, FM derived state | Reserved by the upper load-memory planning path and owned by the final index. |
| final mmap/file data | Sort data, Marisa files, FM blob, Tantivy/RTree files | Planned file/mmap footprint and OS page-cache residency. |
| Milvus download buffer | an explicit temporary vector owned by the scalar materializer | Not allocated by either path. The generic path instead receives an Arrow-owned buffer. |
| Arrow fallback buffer | one `ReadAsync()` result for a non-native slice | Storage-owned temporary memory released after the copy; not precisely charged by the Milvus lease. |
| admission bytes | currently the plaintext slice range size | Conservative proxy that limits submitted range volume and participates in shared priority admission. |
| CRT internal memory | request state, internal chunks, transport buffers | Opaque to the current Milvus lease and not precisely accounted here. |
| kernel memory | dirty mmap pages and page cache | Accounted by the OS/cgroup rather than the slice lease. |

The entry target itself is not covered by the transient lease. Charging the
complete destination through the transient budget would prevent a large index
from loading under a smaller streaming budget even though its final memory was
already admitted separately.

### CRT limitation

`admission_bytes` equals the plaintext range size today, but that does not mean
CRT allocates the same number of bytes. Milvus cannot observe when CRT allocates
or releases its internal buffers, so the lease cannot prove a CRT peak-memory
bound.

CRT's internal request splitting also does not expose Milvus-compatible
acquire/release events. A request-level option such as
`downloadMemoryUsageWindow` is not treated as a process-wide value shared by
all entries or all concurrent index loads.

Consequently:

- native direct materialization removes both the Milvus slice buffer and the
  copy;
- generic async materialization removes the Milvus-owned buffer but may use an
  Arrow-owned slice buffer and one copy;
- `common.loadTransientBudgetBytes=0` still means unlimited Milvus admission;
- `max_inflight_slices` still limits operation count in that configuration;
- neither a native target nor a zero-valued budget makes CRT/Arrow temporary
  memory zero; and
- the current implementation does not claim a hard CRT memory ceiling.

## Index Coverage

### Planned implementations and V3 Entry layout

The Entry count below excludes the packed-file `__meta__`, directory, and
footer. “N” means the number of files emitted by the underlying file-based
index. These are current Scalar Index V3 layouts; no pre-V3 format is involved.

| Index | V3 Entries | Memory target | Mmap/file target | IndexFinalize |
|---|---|---|---|---|
| `ScalarIndexSort<T>` (including Bool) | 3: `index_data`, `idx_to_offsets`, `valid_bitset` | All three can use final heap storage. | `index_data` and offsets use retained mmap files; validity remains heap-backed. | Installs typed data, offsets, and validity. |
| `StringIndexSort` | 3: packed `index_data`, `valid_bitset`, `idx_to_offsets` | Packed data and validity are loaded; offsets are reconstructed from packed data. | Packed data and offsets use retained mmap files; validity remains heap-backed. | Parses the packed header and creates the memory or mmap implementation. |
| `StringIndexMarisa` | 4: trie, string IDs, CSR index, CSR offsets | Trie uses a temporary staging file; IDs and CSR arrays use heap targets. | Trie and IDs use retained files; both CSR Entries occupy non-overlapping regions of one retained file. | Opens the trie and installs IDs/CSR. |
| `FMIndex` | 1, or 2 when nullable: FM blob plus packed null bitmap | Blob and optional null bitmap use heap targets. | Blob uses a padded retained mmap file; optional null bitmap remains heap-backed. | Calls `Deserialize()` or `LoadView()`, validates row count, and restores null state. |
| `BitmapIndex<T>` | 1, or 2 when validity is persisted: normal Roaring data plus packed valid bitset | Raw Roaring bytes and optional validity use heap staging. | Raw Roaring bytes use a temporary writable mmap; validity remains heap-backed. | Memory mode deserializes the raw representation. Mmap mode converts the complete raw Entry into the final frozen-bitmap file, then discards raw staging. |
| `InvertedIndexTantivy<T>` | N Tantivy files, plus 1 `null_offset` Entry when needed | Tantivy files use temporary staging files; null offsets use heap memory. | Tantivy files are retained in the local directory; null offsets remain heap-backed. | Opens Tantivy only after every file is ready. `TextMatch` and JsonFlat variants inherit this layout when they add no Entry. |
| `NgramInvertedIndex` | Tantivy N (+ optional null offsets) + 1 `avg_row_size` | Uses the Tantivy targets plus one heap scalar. | Same Tantivy directory behavior plus one heap scalar. | Finalizes Tantivy, then restores `avg_row_size`. |
| `RTreeIndex<T>` | N RTree files, plus 1 `index_null_offset` Entry when needed | File Entries use temporary staging files; null offsets use heap memory. | The staged RTree files are the local persisted representation; null offsets remain heap-backed. | Opens `RTreeIndexWrapper` only after all files are ready. |
| `HybridScalarIndex<T>` | Exactly the selected internal index's Entries; `index_type` is metadata, not an Entry | Delegates to Bitmap, Sort, or the selected internal planner. | Delegates to the selected internal planner. | Resolves the internal type from catalog metadata before planning and delegates finalization. |
| `JsonScalarIndexWrapper<T, Base>` | Base Entries + optional 1 `non_exist_offsets` Entry | Delegates Base targets; wrapper offsets use heap memory. | Delegates Base targets; wrapper offsets remain heap-backed. | Finalizes Base, restores path-missing offsets, and rebuilds the Exists bitmap. |
| `JsonHybridScalarIndex<T>` | Selected Hybrid Entries + optional 1 `non_exist_offsets` Entry | Hybrid targets plus heap wrapper offsets. | Hybrid targets plus heap wrapper offsets. | Finalizes Hybrid and rebuilds wrapper existence state. |

Every row above implements `SupportsPlannedLoad()`. Whether a Slice uses native
direct placement or Arrow buffer-and-copy is decided by
`RemoteInputStream::ReadAtAsyncInto()` and is independent of the index type.

### Whole-file synchronous fallbacks

| Condition | Reason for fallback |
|---|---|
| any encrypted packed file | The current planned materializer accepts plaintext sources only; encrypted slice planning and decryption targets are follow-up work. |
| reader is not backed by `RemoteInputStream` | The reader cannot provide the async plain-Slice interface. |
| an index without an explicit planner | `SupportsPlannedLoad()` defaults to `false`; this includes specialized classes that do not use the Scalar Index V3 `WriteEntries()` contract. |
| rollout switch disabled | `LoadEntries()` remains the default behavior. |

The fallback is selected before `PlanLoad()` starts. A single packed file is
not partially materialized by the planned path and then resumed by the
synchronous loader.

## Configuration and Rollout

| Parameter | Default | Scalar-index behavior |
|---|---:|---|
| `queryNode.segcore.storageV2.enableAsyncLoad` | `false` | Read when `ScalarIndex::LoadUnified()` starts; selects the planned path only when all other preconditions also pass. |
| `common.loadTransientBudgetBytes` | `0` (unlimited) | Process-wide priority admission shared with other V3 load paths. Plaintext slices charge their range size as a conservative proxy, for both native and buffered reads. |
| internal V3 entry slice size | `16 MiB` | Current planners use `DEFAULT_INDEX_FILE_SLICE_SIZE`; this first implementation does not add a refreshable scalar-index slice setting. |
| `common.diskWriteNumThreads` | `0` | When positive, `LocalFileIOPool` prepares writable mmap targets away from the load executor. |

No new scalar-index-specific user configuration is introduced.
`max_inflight_slices` is an internal plan field; current planners leave it at
zero so the materializer uses the shared load executor's worker count.

Recommended rollout sequence:

1. Use the native async storage implementation, including CRT support for the
   target object store, when direct-into-target performance is required. The
   generic Arrow async path remains functional without that capability.
2. Configure a positive `common.loadTransientBudgetBytes` if Milvus admission
   should be bounded across concurrent load operations.
3. Optionally configure `common.diskWriteNumThreads` to isolate blocking local
   target preparation.
4. Enable `queryNode.segcore.storageV2.enableAsyncLoad` on a limited set of
   QueryNodes.
5. Reload supported plaintext Scalar Index V3 objects and compare latency,
   request pressure, peak process/cgroup memory, cancellation, and errors with
   the synchronous cohort.
6. Disable the switch to return subsequent index loads to `LoadEntries()` if
   rollback is needed.

## Compatibility

- The packed V3 file format and entry CRC format do not change.
- `IndexBase::Load`, `ScalarIndex::LoadUnified`, and query interfaces remain
  synchronous and source-compatible.
- Catalog inspection uses the existing footer, directory, metadata, and
  encryption validation.
- Memory-backed and mmap-backed indexes expose the same query representation
  as their synchronous loaders.
- The planned path is all-or-fallback for one packed file.
- The rollout switch defaults to the synchronous path.
- High/low load priority is preserved in the shared executor and transient
  admission queues.
- Staging files are committed only after index-specific construction succeeds.

## Alternatives Considered

### Make `ReadEntry()` asynchronous without changing the index interface

This preserves pull-oriented loading. The index would still allocate targets
and select entries incrementally, so the common layer could not safely
round-robin across entries or know every final write location in advance.

### Materialize one complete entry before starting the next

This simplifies entry lifetime but allows one large entry to monopolize the
index load and delays independent files required by Tantivy or RTree. The
round-robin planner provides cross-entry progress while retaining per-entry CRC
semantics.

### Submit one asynchronous read for each complete entry

CRT may split that request internally, but Milvus would lose admission and
cancellation granularity and could submit an unbounded number of large files.
Milvus slices retain control over range and operation pressure even though CRT
memory remains opaque.

### Require native direct-into-target support

This would avoid every temporary Arrow buffer, but would make the whole index
fall back to synchronous `LoadEntries()` on otherwise async-capable storage.
The implementation instead keeps native placement as a fast path and uses one
Arrow buffer-and-copy per Slice when it is unavailable.

### Add dedicated scalar materialization and disk executors

Another CPU pool would duplicate scheduling and make field-data and index load
priority compete indirectly. The implementation shares one priority load pool
and reuses `LocalFileIOPool` only for blocking file operations.

### Charge zero admission bytes for native direct reads

The Milvus download vector is gone, but request count and opaque CRT buffers
still consume resources. A zero charge would make the process-wide budget
irrelevant to native slices. The current range-size proxy is conservative and
explicitly documented as not being CRT accounting.

### Make catalog inspection asynchronous in the first version

Footer, directory, and metadata inspection have separate request and format
failure modes. Keeping the established synchronous reader construction reduces
the first rollout's scope and produces a complete catalog before index policy
runs.

## Testing Strategy

The implementation adds focused tests for the following contracts:

- catalog construction for plaintext entries and metadata lookup;
- exactly one native caller-owned read per Slice when native capability exists;
- Arrow `ReadAsync()` buffer-and-copy fallback when native capability is
  absent;
- short-read rejection and typed storage error preservation on both paths;
- cross-entry round-robin admission and out-of-order slice completion;
- default and explicit `max_inflight_slices` behavior;
- plan rejection before read or target preparation for overlapping memory and
  mmap regions;
- ordered CRC combination and failure cleanup;
- cancellation while admission is pending and draining of already-issued
  reads;
- writable mmap closure before IndexFinalize and unlink of uncommitted files;
- one shared field-data/scalar priority load executor and HIGH/LOW mapping;
- planned memory and mmap loading for Sort, Marisa, FM, Bitmap, Tantivy,
  Ngram, RTree, Hybrid, and JSON wrappers;
- Bitmap raw staging followed by normal-to-frozen mmap conversion;
- Hybrid delegation and JSON/Ngram wrapper-owned Entry restoration; and
- synchronous fallback for encrypted files and indexes without planners.

The change is verified with the C++ unit-test build, the focused async-load
suite, materializer contract tests, and synchronous index regressions.

## Follow-Up Work

- Make footer, directory, and metadata inspection asynchronous if profiling
  shows that synchronous reader construction is material.
- Add encrypted-entry planning once decryption can target final memory
  with explicit ciphertext/plaintext lifetime accounting.
- Add request, admission, first-byte, completion, CRC, and finalization metrics
  before broad rollout.
- Work with milvus-storage to expose observable CRT allocation or request-lease
  semantics if a real CRT memory hard bound is required.
- Validate production slice size and in-flight defaults against object-store
  request limits, cgroup memory, dirty-page behavior, and load latency.
- Decide whether retained local index files require crash durability; add
  `msync`/`fsync` only if that becomes an explicit cache contract.
- Enable the path by default after rollout validation, then remove the
  temporary switch and obsolete synchronous-only code in a separate cleanup.

## Key Source Files

| Area | Files |
|---|---|
| scalar-index integration and rollout gate | `internal/core/src/index/ScalarIndex.{h,cpp}` |
| immutable packed-entry catalog | `internal/core/src/storage/IndexEntryCatalog.h`, `IndexEntryReader.{h,cpp}` |
| planning and artifact ownership | `internal/core/src/storage/IndexLoadPlan.h` |
| common coroutine materializer | `internal/core/src/storage/IndexMaterializer.h` |
| native and buffered async remote reads | `internal/core/src/storage/RemoteInputStream.{h,cpp}` |
| shared priority load executor | `internal/core/src/storage/LoadExecutor.{h,cpp}` |
| transient admission and cancellation helpers | `internal/core/src/storage/EntryStreamUtils.h` |
| writable staging mmap ownership | `internal/core/src/storage/WritableMmapFile.h` |
| blocking local-file executor | `internal/core/src/storage/LocalFileIOPool.{h,cpp}` |
| field-data executor integration | `internal/core/src/segcore/storagev2translator/AsyncLoadPipeline.{h,cpp}` |
| scalar-index planners/finalizers | `internal/core/src/index/ScalarIndexSort.{h,cpp}`, `StringIndexSort.{h,cpp}`, `StringIndexMarisa.{h,cpp}`, `FMIndex.{h,cpp}`, `BitmapIndex.{h,cpp}`, `InvertedIndexTantivy.{h,cpp}`, `NgramInvertedIndex.{h,cpp}`, `RTreeIndex.{h,cpp}`, `HybridScalarIndex.{h,cpp}`, `JsonScalarIndexWrapper.h`, `JsonHybridScalarIndex.h` |
| rollout and budget configuration | `internal/core/src/segcore/storagev2translator/StorageV2Config.h`, `pkg/util/paramtable/component_param.go`, `configs/milvus.yaml` |
| materializer and index contract tests | `internal/core/src/storage/IndexEntryWriterTest.cpp`, `internal/core/src/index/*Test.cpp` |
