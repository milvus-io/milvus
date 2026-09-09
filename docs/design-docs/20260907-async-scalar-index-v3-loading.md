# Async scalar-index V3 loading

## Scope and selection

This change is based on `origin/master` at `5f7ddf23d2`, which includes
`milvus-async-load-ready` through [#51246](https://github.com/milvus-io/milvus/pull/51246).
The merged foundation includes native asynchronous chunk-reader opens,
cancellation/error fixes, local-file finalization, configuration, and
milvus-storage `a18d007`.

The child shares the parent's `AsyncLoadExecutor` and `LoadAdmissionController`.
Field-data loading retains the parent's `AsyncLoadPipeline`.
The child adds independent V3 scalar reader/materialization and scalar index
integration with resource estimates. Tests accompany their implementation layer.
The parent's field-data translator and its creation sites are retained unchanged.

`queryNode.segcore.storageV2.enableAsyncLoad` is a process-wide rollout switch,
defaulting to false. The parent QueryNode configuration watcher applies startup
values and runtime updates together with the admission limits. Each scalar V3
load entry reads `StorageV2AsyncLoadEnabled()` directly. There is no per-index
override, native-reader eligibility switch, or encrypted fallback to a legacy
executor.

Runtime-update limitation: the scalar translator selects its resource estimate
and overhead group at construction, while execution reads the rollout switch
again at load entry. A switch update between these points can make their modes
differ. Aligning these decisions remains necessary before relying on scalar
rollout updates during a translator's lifetime.

When disabled, scalar loading uses `IndexEntryReader` with its existing HIGH/LOW
pool scheduling. When enabled, `AsyncIndexEntryReader` owns the scalar pipeline.
Legacy storage formats, vector index loading, index building and uploads retain
their existing APIs.

## Shared code and independent scheduling

The shared executor lives in `storage/AsyncLoadExecutor.{h,cpp}` under the
`milvus::storage` namespace, alongside load admission and `LocalFileIOPool`.
`AsyncIndexEntryReader` and `IndexMaterializer` also live in `storage/`.
Field-data reader preparation and cell loading stay in
`segcore/storagev2translator/AsyncChunkReader` and `AsyncLoadPipeline`; they
consume the shared storage executor. Scalar materialization uses the same
executor directly, without depending on those field-data components.

Async CPU work shares one `folly::CPUThreadPoolExecutor`, configured through
`queryNode.segcore.storageV2.asyncLoadThreadPoolSize` and defaulting to
`max(1, min(CPU_NUM, 16))`. HIGH and LOW map to priority views of this executor.
The parent resizes that executor in place; legacy pool resizing is independent.
The child reuses its priority resolver and `GetAsyncLoadThreadPoolSize()`.
Each scalar materialization captures its slice limit once. Resource estimation
reads the same getter independently, so increasing the worker limit between
estimation and materialization can exceed the earlier request-local estimate.
That estimate/lifetime alignment remains unverified after this rebase.

The implementations share mechanisms that have the same semantics:

- `IndexEntryFormat` reads and parses the V3 footer/directory. The legacy reader
  retains its original entry-download methods and state. The new reader converts
  the parsed directory to an immutable catalog and discards the temporary parse.
- Index `PlanLoad`/`FinalizeLoad` reuse existing representation parsers, null
  handling, and index constructors. JSON sidecar planning and ownership transfer
  are shared by the two JSON wrappers. Hybrid type recovery shares the existing
  persisted-type/filename/metadata compatibility rule with legacy loading.
- `LoadAdmissionController` jointly reserves transient bytes and one slot for
  each scalar slice or field-data window. The child uses the parent's leases,
  priority/FIFO queues, cancellation, scheduling optimizations, and metrics.
  Legacy and async loads share the same process-wide resource limits.

## Packed scalar-index pipeline

`ScalarIndex::LoadUnified` selects once at entry. The async branch schedules:

```
Open input and directory -> PlanLoad -> MaterializeIndexAsync
                        -> FinalizeLoad -> CommitTargets
```

The scalar pipeline reuses the parent's `LocalFileIOPool` for blocking file
operations. Executor selection follows the actual targets in the plan, since
Tantivy and Marisa also use staging files when the final index is loaded into
memory.

| Phase | Executor |
| --- | --- |
| Catalog, planning, slice scheduling, decryption, CRC | Shared async-load executor |
| Create directories, preallocate and map writable targets | `LocalFileIOPool` |
| Unmap and close writable targets after reads drain | `LocalFileIOPool` |
| Finalize an artifact containing file targets; commit and release its staging resources | `LocalFileIOPool` |
| Finalize a memory-only artifact | Shared async-load executor |
| Clean up failed materialization with file targets, including its directory-lease context | `LocalFileIOPool` |

Each local-file phase obtains its executor token immediately before scheduling
and releases it before returning to the caller. No such token spans remote I/O,
so disabling the pool does not wait for a pending remote read. HIGH/LOW remain
priority views of the selected executor. If the local-file pool is disabled,
the parent's priority resolver falls back to the shared async-load executor.
Cross-executor waits use `co_await`; they do not block a worker on a nested task.

Cancellation is checked again when queued file preparation or finalization
starts. Cleanup is awaited with cancellation disabled so it completes before
the original exception is rethrown. Finalization owns its artifact in the
scheduled coroutine body, ensuring destructor cleanup runs there on both
success and failure. This routing covers materialization and finalization;
planning that throws before returning a plan and later index destruction keep
their existing lifetimes.

All index-specific plans allocate final heap destinations or describe staging
mmap files. The materializer validates full, non-overlapping entry coverage
before preparing targets, interleaves entries in round-robin order, and issues
at most one executor-worker-count of slices per load. It limits both request
count and transient bytes, including when the byte limit is disabled.

Plain native readers use `NonBlockingRandomAccessFile::ReadAtAsyncInto` to write into
the caller-owned destination. Other Arrow files use `ReadAsync` and copy the
returned buffer into that destination. A non-Arrow InputStream performs its
synchronous read on the async worker. Encrypted files use persisted ciphertext
slice boundaries; decryption writes to the same planned plaintext destinations.
These backend differences do not change the selected Milvus executor.

Each slice computes CRC-32C after its write. Entry CRCs are combined in logical
slice order so out-of-order completions need no second full-entry scan. A failed
entry never becomes ready. Only a complete successful artifact is finalized.

Writable files reserve blocks before mapping, reporting allocation failure
before a writer can fault on an unbacked page. The materializer keeps mappings
alive until every issued read completes, unmaps writable targets before final
index construction, and retains files only after successful finalization.
Bitmap's derived frozen file also has failure cleanup. Existing final index
objects continue to own their read-only mappings and files.

## Parent field-data loading

`ManifestGroupTranslator` retains the parent's `get_cells_legacy` and
`get_cells_via_async_pipeline` selection. Sealed V3 field-loading and JSON
key-stat creation sites use the existing translator constructor. Field-data
loading retains native chunk-reader opens, `LoadCellsAsync`, and
`LocalFileIOPool` mmap finalization, as described in the
[parent design](design_docs/20260811-async-storage-v3-field-data-loading.md).
Read-window updates retain the parent's behavior and apply to subsequent loads
through an existing translator. Field-data resource-group bindings are unchanged.

## Admission, ownership, and resource estimates

Admission jointly caps outstanding bytes and slots across legacy and async
loads. When async loading is enabled and no override is supplied, the parent
uses a 2 GiB transient budget and twice the initialized CPU count for slots.
When disabled, absent settings resolve to zero; explicit overrides apply in
either mode. Both settings are refreshable, and zero disables the respective
limit. Waiters hold neither resource. Each scalar slice, including metadata, reserves one slot until its
read and processing finish. Scalar materializers also retain their per-load
in-flight slice limit.

Admission is FIFO within each priority, with HIGH preceding queued LOW
requests. Cancellation removes pending admission in constant time. A request
larger than the byte ceiling runs without other byte reservations but still
requires a slot. Promise completion and cancellation callbacks run outside the
controller mutex. Legacy blocking admission retains the parent's behavior.

A slice/window lease lasts through read completion, integrity checks, and
finalization. First failure is published before releasing its lease, cancelling
pending admission before another task can consume the released bytes. Once an
I/O request owns a destination, cancellation stops new work and waits for issued
work to drain; it does not free the destination early.

Plain slice charge is its byte count. Encrypted slice charge is twice the
ciphertext bytes plus plaintext bytes, covering an Arrow buffer, ciphertext,
and decrypt output. Final destinations are separately accounted as index
resident or staging memory. These estimates bound Milvus admission, not every
allocation internal to a remote SDK or format decoder.

Async scalar loads and field-data loads share the memory-overhead group from
`LoadMemoryOverheadController::GetInstance()`. Scalar bindings use the same
`GetOrCreate(ThreadPools::GetLoadExecutorWorkers())` initialization as the
parent's bindings. The byte-budget policy, fallback policy, and runtime updates
are provided by the existing controller; this child adds no separate controller
or accounting group. Scalar staging files retain request-local reservations.

The parent's fallback when the byte budget is disabled still depends on HIGH/LOW
worker counts. Removing that dependency belongs to the shared accounting
follow-up. Async execution itself continues to use the shared async executor.
Admission slots continue to limit outstanding work through the existing
`LoadAdmissionController`, independently of the memory-accounting policy.

Scalar estimates reuse representation-cost calculations. The read peak is
bounded by actual catalog slices and executor concurrency. Full-entry buffers
that survive individual leases, including nullable Tantivy sidecars, Bitmap
conversion inputs and packed validity metadata, stay in request-local peak
reservations. JSON non-existence offsets transfer their vector ownership into
the final index, avoiding a second full sidecar copy. Only overhead entirely
controlled by runtime slice leases is folded into the shared resource group.

## Compatibility and failures

No packed format changes are introduced. Plain and encrypted V3 directories use
the same parser as legacy loading. Sort files with missing persisted auxiliary
entries rebuild their metadata. Marisa requires a complete persisted CSR set if
any CSR fields exist; otherwise it rebuilds CSR. HYBRID files lacking
`index_type` follow the existing standalone physical-file recovery rule.
Tantivy finalization builds sealed validity state as current master does;
JSON wrappers then construct the existence bitmap.

The materializer forwards the first exception after draining submitted work.
The reader retains Arrow statuses through retry classification and converts
terminal statuses with `milvus_storage::ToSegcoreError`. Short reads, CRC
mismatches, invalid plans, cancelled admission, file preparation and finalizer
failures cannot publish a successful artifact.

Storage remains responsible for field-data backend errors. In the current
milvus-storage dependency, `ChunkReaderImpl::get_chunks_async` converts a thrown
backend exception to Arrow IOError during fan-in. This change preserves the
status it receives and does not claim to recover categories already erased by
that dependency. Direct scalar native-read retry tests and typed-error tests
must not be interpreted as end-to-end S3 fault injection or throughput results.

## Validation

Revalidated on 2026-09-08 against the complete `ac66424968` parent and
milvus-storage `a18d007`, using GCC 12, Release CMake/Ninja, and at most 16 build
jobs. The `all_tests` incremental build passed with scalar local-file phases
routed through the parent's local-file executor.

- All 754 distinct C++ cases passed in the combined parent regression and scalar
  child suites, including joint admission, slot updates,
  priority/FIFO/cancellation, metrics, native reader opens, local-file finalization,
  entry streaming, sealed field-data loading, and scalar async loading.
  The parent's 12 disabled tests remain disabled and are excluded from the count.
- Five new cases cover file/memory executor selection for both priorities,
  disabled-pool fallback, actual Sort mmap loading with one local worker, queued
  cancellation, CRC/finalizer failures, and pool shutdown during pending I/O.
  Failure cases verify the cleanup executor and preservation of typed errors.
- The controlled scalar-reader case verifies shared slot consumption, live
  shrinking, disabling the slot bound, and returning all reservations.
- Parent manifest tests verify read-window updates through an existing
  translator and memory/mmap async loading behavior.

Earlier validation on the same parent also passed:

- Seven cancellation, admission-race, out-of-order-read, and dynamic-slot cases,
  with 20 repetitions each.
- Go configuration tests: three paramtable subtests for async loading,
  read-window normalization, and common byte/slot limits, plus five initcore
  tests for serialized watcher catch-up, read-window update/delete, and the
  read-window/slot C setters. Both packages were run with
  `-tags dynamic,test -gcflags='all=-N -l' -count=1` and `GOFLAGS=-p=8`.

Failure tracing and controlled tests establish the following boundaries:

| Trigger | Verified result |
| --- | --- |
| Global switch off/on | Legacy/async scalar path selection; encrypted and buffered reads retain the async executor when enabled. |
| Cancel pending metadata admission | Budget cancellation becomes `SegcoreError(FollyCancel)`. |
| Cancel while native I/O is pending | Issued reads drain before destination cleanup; cancellation stops subsequent retries. |
| Typed transient native-read error | Arrow status detail controls retry; terminal errors retain the storage translator's category. |
| Short read, CRC mismatch, or overlapping targets | Load fails; incomplete staging files are removed and no successful artifact is returned. |
| Async lease release racing with cancellation | Admission resolves once and returns its byte/slot reservation. |
| Slot limit changes during scalar materialization | Other loads share the limit; shrinking waits for existing reads, and disabling it wakes pending work. |
| Scalar artifact contains file targets | Finalization and artifact cleanup run on the configured local-file executor, including when final mmap is disabled; memory-only artifacts stay on the async executor. |
| Cancel queued file preparation or an issued scalar read | Queued preparation skips file creation; issued reads drain before local-file cleanup, preserving `FollyCancel`. |
| Disable local-file pool while a remote read is pending | Pool shutdown completes independently; subsequent file phases use the parent's async-executor fallback. |

The pre-rebase rollout value was traced from paramtable through QueryNode
initialization and the C setter to load selection; that revision had no
production rollout watcher. The current parent restores runtime updates, with
the scalar limitations described above. The pre-rebase static comparison
also confirmed that legacy entry-download bodies and shared GroupChunk
construction are unchanged, and the parent native-open, pipeline, and local-I/O
implementations are retained. The parent admission fast path, FIFO queues,
cancellation, and metrics are also unchanged. Those historical runs used the
earlier separate async accounting group. The current revision restores the
parent's controllers and binds scalar loads to its existing shared memory group;
the historical results do not validate this accounting change.

Full-server E2E, ASAN, real S3 fault injection, OOM/ENOSPC injection, RSS, and
throughput benchmarks were not run. File-preallocation and writable-mapping
finish failures were traced through cleanup ownership, without filesystem
fault injection.

The rebase to `origin/master` (`5f7ddf23d2`) retained the executor configuration,
QueryNode admission initialization, and stream-download lease-lifetime fix.
The historical results above cover `ac66424968`.

For the shared-accounting correction on 2026-09-09, the GCC 12 Release
`all_tests` build passed on the `5f7ddf23d2` master base. All 52 focused C++ tests
passed: the two Tantivy async load cases, four scalar routing/mmap cases, four
existing scalar overhead-bound cases, 40 admission cases, and two thread-pool
cases. The Tantivy tests now assert that eligible scalar memory and mmap loads
bind to the same memory group returned by the parent's `GetInstance()`.
The full historical 754-case set was not rerun for this correction.

For the executor relocation on 2026-09-09, the GCC 12 Release `all_tests` build
passed. All 88 focused cases passed: two executor tests, 59 field-data pipeline
tests (including reader preparation), 21 packed-index reader/materializer tests,
two Tantivy async load tests, and four scalar routing/mmap tests. The executor
implementation and its relocated test bodies are unchanged apart from namespace,
include paths, and formatting.
