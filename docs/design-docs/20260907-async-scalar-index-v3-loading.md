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

The scalar translator reserves the larger final and temporary costs of the two
load paths. It binds the shared memory-overhead group only when both paths'
overhead is fully covered by slice leases. Execution still reads the live
rollout switch at load entry, so a cache reload can change paths without
invalidating its original resource estimate.

When disabled, scalar loading uses `IndexEntryReader` with its existing HIGH/LOW
pool scheduling. When enabled, `AsyncIndexEntryReader` owns the scalar pipeline.
The subsequent [legacy scalar and Knowhere streaming migration](20260909-streaming-legacy-index-loading.md)
extends the shared infrastructure to legacy scalar and vector memory loads and
documents each phase's executor. Index building and uploads retain their existing APIs.

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
The child reuses its priority resolver. Packed and legacy async index loads
share a fixed window of eight slices and 128 MiB of transient charges per load.
An indivisible encrypted/encoded unit above that byte window runs alone.
Worker-count and global-admission updates do not enlarge this local window.

The implementations share mechanisms that have the same semantics:

- `IndexEntryFormat` shares pure footer validation and directory parsing. The
  legacy reader retains synchronous directory reads. The new reader reads magic,
  footer and directory through its asynchronous range reader under admission,
  converts the directory to an immutable catalog and discards the temporary parse.
  Directory admission includes the serialized bytes and estimated parsing scratch.
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

The scalar pipeline reuses the parent's `LocalFileIOPool` for staging-file
operations. Engine opening, read-only mapping, deserialization, and query-state
restoration run on the shared async worker, including file-backed indexes.
Tantivy and Marisa may use staging files even when the final index is loaded
into memory. The existing `FinalizeLoad` contract is an awaited coroutine so
Bitmap can suspend for derived frozen-file writes without moving conversion
or representation restoration onto local-file workers.

| Phase | Executor |
| --- | --- |
| Catalog, planning, slice scheduling, decryption, CRC | Shared async-load executor |
| Create directories, preallocate and map writable targets | `LocalFileIOPool` |
| Unmap and close writable targets after reads drain | `LocalFileIOPool` |
| Open engines/read-only mappings; deserialize and restore query state for every artifact | Shared async-load executor |
| Convert Bitmap postings into bounded frozen-output batches | Shared async-load executor |
| Create/write/flush/close Bitmap's derived frozen file | `LocalFileIOPool` |
| Commit and release staging resources of an artifact containing file targets | `LocalFileIOPool` |
| Release a memory-only artifact | Shared async-load executor |
| Clean up failed materialization with file targets, including its directory-lease context | `LocalFileIOPool` |

Each local-file phase obtains its executor token immediately before scheduling
and releases it before returning to the caller. No such token spans remote I/O,
so disabling the pool does not wait for a pending remote read. HIGH/LOW remain
priority views of the selected executor. If the local-file pool is disabled,
the parent's priority resolver falls back to the shared async-load executor.
Cross-executor waits use `co_await`; they do not block a worker on a nested task.

Cancellation is checked before finalization and after synchronous engine/state
restoration, and between Bitmap conversion/write batches. Engine calls are
not interrupted; they return before borrowed inputs or staging resources are
released. Cleanup is awaited with cancellation disabled before rethrowing the
original failure. The outer coroutine owns the artifact while
`FinalizeLoad(IndexLoadArtifact&)` borrows it, then moves it into the local-file operation that commits retained
targets and releases staging resources. Directory retention follows committed
targets, so a failed or cancelled finalizer does not retain its directory.
Tantivy heap-mode removal and pending Marisa/StringSort/Bitmap file guards are
released in that cleanup phase. No local-file executor token spans engine
opening or deserialization. Synchronous engine reads and read-only mapping
occupy the async worker until the engine returns.

This routing covers materialization and finalization. Planning that throws
before returning a plan and later index destruction/eviction keep their
existing lifetimes; it does not relocate destruction of a completed index.

Index plans contain entry names and final heap or staging-file destinations.
The materializer derives offsets, slice sizes and expected CRCs from the catalog,
validates destination capacity and overlap, then interleaves entries in
round-robin order within the fixed slice/byte window. Successful return of the
complete artifact is the readiness boundary; plans have no per-entry ready state.
Tantivy and RTree share directory-name validation, file-target planning and
lease/cleanup ownership. Their engine-specific finalizers remain separate.

Plain native readers use `NonBlockingRandomAccessFile::ReadAtAsyncInto` to write into
the caller-owned destination. Other Arrow files use `ReadAsync` and copy the
returned buffer into that destination. A non-Arrow InputStream performs its
synchronous read on the async worker. Encrypted files use persisted ciphertext
slice boundaries; decryption writes to the same planned plaintext destinations.
These backend differences do not change the selected Milvus executor.

Each slice computes CRC-32C after its write. Entry CRCs are combined in logical
slice order so out-of-order completions need no second full-entry scan. Only a
complete successful artifact is finalized.

Writable files reserve blocks before mapping, reporting allocation failure
before a writer can fault on an unbacked page. The materializer keeps mappings
alive until every issued read completes, unmaps writable targets before final
index construction, and retains files only after successful finalization.
Bitmap's derived frozen file also has failure cleanup. Frozen conversion reuses
a 64 KiB batch buffer, allowing at most one additional large bitmap in a batch;
writes are awaited before reusing the buffer. Resource estimates include the
batch prefix, decoded/frozen bitmap scratch, temporary output-buffer growth,
and the refreshable FileWriter buffer bound. Existing final index objects
continue to own their read-only mappings and files.

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
`LoadMemoryOverheadController::GetInstance().GetOrCreate()`. Group creation does
not inspect or initialize an executor, and the handle survives configuration
changes. Scalar staging files retain request-local reservations.

`LoadAdmissionController` supplies the shared accounting limits for both load
modes. With a nonzero byte budget, memory uses the existing Budget policy,
including its oversized-unit allowance. Without that budget, memory uses the
admission slot capacity multiplied by the largest bound runtime unit. Field-data
file overhead uses the same slot bound. The existing multiplicative Executor
policy implements this bound; its count is admission slots, not CPU workers.
Suspended async I/O retains slots even when it releases a worker. A zero slot
capacity uses Passthrough instead of assuming a concurrency bound; memory still
uses Budget when its byte budget is enabled. Counts beyond the policy's signed
range conservatively use Passthrough as well. With both limits disabled, this
can reserve more overhead than the old worker-count heuristic.

Byte and slot updates share one configuration mutex. Expansion updates the
applicable overhead groups before allowing more admitted work; a rejected update
keeps the admission limit unchanged. Group reservations retain the existing
cache reconciliation behavior on reserve/release. Tightening restricts admission
first. Slot-based accounting retains at least the already admitted slot count
until it drains to the new limit, then applies that
limit on release. This also covers a second resize before draining completes.
Promise completion stays outside the configuration lock.
HIGH/LOW and async executor resizes no longer update resource accounting.

Scalar estimates reuse representation-cost calculations. The read peak is
bounded by actual catalog slices and the fixed per-load window; the compatibility
path contributes its conservative full-stream estimate. Full-entry buffers
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
| Scalar artifact contains file targets | Engine/state finalization runs on the async worker; file writes and artifact cleanup use the local-file executor, including when final mmap is disabled. |
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

### Scalar finalizer executor validation (2026-09-11)

The executor split rebuilt `all_tests` and `json_stats_test` with GCC 12 in the
cached Release build, with outer parallelism capped at 16 and independent
third-party builders capped at one.

All 44 focused scalar tests passed. The new restoration gate covers numeric
Sort, StringSort, Marisa and high-cardinality Bitmap across legacy/V3,
heap/mmap, and success/cancellation/injected restoration failure. While actual
query-state restoration is blocked on the single async worker, the single
local-file worker remains available and slice admission can be acquired.
Bitmap uses 5,000 distinct keys to cross frozen-write batches, checks both ends
of the result, and verifies derived-file removal on failure/cancellation.
Common V3 routing tests separately check finalization and artifact-cleanup
thread names with the local-file pool enabled and disabled.

Another 655 regression cases passed, covering synchronous scalar mmap/nullable
loads, resource estimates, storage/admission, Knowhere and BSON. The separate
JSON stats binary passed all 84 cases. These are 783 distinct selected cases,
with no failures or skips. This increment does not measure real object-storage
throughput or process-wide peak memory.


### Design review follow-up validation (2026-09-11)

Both cached Release targets, `all_tests` and `json_stats_test`, built with at
most 16 concurrent compiler jobs. The final targeted run passed 43 cases;
the related regression run passed 736 and the JSON stats binary passed 84.
These cover 838 distinct selected cases, with no failures or skips.

The added checks exercise directory admission before any read, cancellation
while a large native directory read is pending, the fixed materialization
window with one async worker and unlimited global admission, and estimates
across rollout/worker changes. They also check legacy envelope reuse in both
file managers and BSON translator reloads, plus synchronous/async Ngram heap
cleanup. Regression includes the existing plain/encrypted V3 reader's large
directory and metadata cases, scalar mmap/nullable paths, Knowhere, resource
estimates, admission and finalizer failures. This is local correctness and
routing validation, not an object-storage throughput or peak-RSS measurement.

### Admission overhead validation (2026-09-11)

Rebuilt `all_tests` and `json_stats_test` in the cached GCC 12 Release tree with
at most 16 build jobs. The focused run passed 57 cases, the loading regression
run passed 249, and JSON stats passed 84: 390 distinct executed cases, with no
failures or skips. The pool-map-lock check also passed in a fresh process.
The added cases inspect actual cache reservations across byte/slot policy
changes, executor and rollout changes, unlimited capacities, and slot reductions
with admitted work still in flight. An incompatible binding verifies that a
rejected expansion keeps admission bounded and a subsequent retry can succeed.
No remote-cluster or throughput validation was run for this accounting change.

### Independent TextMatch entry point (2026-09-11)

Independent TextMatch packed files reuse `LoadUnifiedAsync`, the Tantivy plan
and finalizer, and the shared file-aware resource estimator. The translator
now forwards cancellation and planning opens the packed object under its
text-log prefix. Legacy TextMatch uses the existing Tantivy legacy coroutine.
See [TextMatch loading](20260909-streaming-legacy-index-loading.md#independent-textmatch-loading)
for routing, executor ownership and estimates.
