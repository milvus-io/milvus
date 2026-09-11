# Streaming legacy scalar, Knowhere and BSON shared-key index loads

## Scope

This extends [async scalar-index V3 loading](20260907-async-scalar-index-v3-loading.md)
to legacy scalar formats, Knowhere memory, mmap, and disk staging loads, and
BSON shared-key indexes. It shares
`storage::AsyncLoadExecutor`, `LoadAdmissionController::GetInstance()`, and
`LocalFileIOPool`. It adds no controller singleton or per-index executor.

The migration is incremental:

1. Legacy numeric Sort and shared admitted transport.
2. Remaining legacy scalar consumers, metadata, and file destinations.
3. Knowhere memory loading through `VectorMemIndex`.
4. Knowhere mmap/disk loading, including its stream/lazy-load contracts.
5. Bounded concurrent slice reads and a final routing audit.

Steps 1–5 are implemented at the Milvus load boundary. Internal remote reads
in stream-capable Knowhere implementations retain
their own admission contract, as detailed below. Index building,
uploads, and independent text-match/JSON-key stats entries are outside these
five steps. The subsequent BSON shared-key increment is described below;
other JSON stats entry points retain their existing loaders. Packed scalar V3
keeps its existing reader and materializer.

The context-aware sealed-index `Load(trace, config, OpContext*)` reads
`StorageV2AsyncLoadEnabled()` directly. Enabled legacy scalar and vector loads
enter the shared executor. Disabled loads call their existing synchronous
loader. The two-argument compatibility entry remains synchronous.

## Executor ownership by phase

HIGH and LOW on the new path are priorities on the shared async executor; they
do not select the old HIGH/LOW worker pools.

| Phase | Executor / thread | Waiting behavior |
| --- | --- | --- |
| File-aware resource estimate | Shared async executor when enabled; planning caller when disabled | Envelope reads acquire async admission; the outer planning caller uses `blockingWait` |
| Sealed-load dispatch | Existing synchronous cache-load caller | One outer `blockingWait` schedules the complete load coroutine |
| Open object, obtain size, parse envelope and slice metadata | Shared async executor | `OpenInputFile` / ChunkManager `Size` can still block this worker; this migration does not make object opening asynchronous |
| Wait for slice admission | Shared async executor | Coroutine suspends without occupying a worker |
| Dispatch concurrent slices and join their completion | Shared async executor | Child coroutines run on the caller's executor; no nested `blockingWait` |
| Native `ReadAtAsyncInto` | Storage backend owns I/O and completion | Coroutine suspends and resumes on the shared async executor |
| Buffered Arrow `ReadAsync` fallback | Arrow I/O context / backend; its completion callback copies the returned buffer | Coroutine resumes on the shared async executor after that copy |
| ChunkManager-only or rooted-local source read | Shared async executor | Existing synchronous read; no submission to HIGH/LOW |
| Decode/decrypt a legacy envelope; copy into its final BinarySet entry | Shared async executor | CPU work runs directly; consumer completes before its slice lease is released |
| Wait for the preceding file consumer | Shared async executor | Coroutine suspends while retaining admission; acquires no local-file executor token |
| Restore vector nullable/empty-list sidecars and call `LoadWithoutAssemble` | Shared async executor | Synchronous CPU phase, with no slice admission held |
| Call memory Knowhere `Deserialize` | Same shared async worker | One synchronous call; it occupies this worker until Knowhere returns |
| Create/write/flush/close mmap files or disk slices | `LocalFileIOPool` | Coroutine awaits the operation; a read/decode lease survives each write |
| Restore file-backed nullable/empty-list metadata; invoke `DeserializeFromFile` or disk `Deserialize` | Shared async executor | Files are closed first; the synchronous call occupies this worker, with no slice admission or local-file executor held |
| Open BSON shared-key Tantivy reader, including heap loading or mapping | Shared async executor | One synchronous engine call after all staged files close and slice admission releases |
| Failure cleanup of files created by the load | `LocalFileIOPool` | Issued I/O and synchronous finalization drain before cleanup |
| Parallel work inside `Deserialize` | Whatever workers / parallel runtime Knowhere selects | Milvus does not create deserialization tasks or impose an additional parallelism limit |
| Publish a completed index | Existing cache-load caller after successful return | Cancellation or failure prevents publishing that result |

Knowhere is responsible for its internal deserialization parallelism. A
deserializer that executes serially continues on the calling async worker. The
integration does not imply that every deserializer uses a Knowhere thread pool.
It also does not make Knowhere deserialization incremental: the complete
`BinarySet` is available before the call starts.

Memory, mmap, and disk Knowhere finalization all run on the shared async worker.
For these loads, `LocalFileIOPool` handles Milvus file creation, writes,
flush/close, and cleanup. After awaiting file closure, the coroutine resumes on
the shared worker to restore sidecars and invoke the engine. Local file reads,
mapping, and any synchronous I/O inside Knowhere therefore occupy that async
worker until the engine returns. Other loads can continue using the local-file
workers while deserialization runs.

The inspected FLAT, IVF, HNSW, sparse, and embedding-list memory deserializers
consume memory readers / BinarySet entries. Some sparse implementations retain
shared ownership of input buffers. They keep that ownership contract. GPU and
remote-cluster execution have not been validated by this increment.

Legacy scalar consumers use the same transport phases. All query-representation
restoration runs on the shared async worker:

| Consumer phase | Executor |
| --- | --- |
| Pure memory representation finalization | Shared async executor |
| Hybrid child selection and child coroutine | Shared async executor; directly `co_await` the child |
| Numeric/String Sort mapping, parsing, offsets and validity restoration | Shared async executor |
| Marisa trie read/mmap and string-ID/CSR restoration | Shared async executor |
| Bitmap decode, frozen conversion, read-only mapping and offset-cache restoration | Shared async executor |
| Tantivy/RTree engine opening and state restoration | Shared async executor |
| Staging-file preparation, writes, flush/close and removal | `LocalFileIOPool` |
| JSON wrapper bitmap restoration | Resumes on the shared async executor after the base coroutine |

Local-file work uses the existing disabled-pool fallback to the shared async
executor. For Milvus staging, a local-file executor token does not span remote
I/O. Cross-executor operations are awaited; an async worker never calls a child's
blocking load wrapper. Existing disk-index construction still prepares its generated directory
on the constructing caller; the table describes the subsequent sealed `Load`.

The existing `FinishLegacyLoadAsync` hook is overridden only by BinarySet
consumers that need awaited file preparation: numeric Sort, StringSort, Marisa
and Bitmap. Compatibility entry points reuse the extracted parsers and keep
their synchronous scheduling. Tantivy/RTree open directly after disk staging
returns. Bitmap shares its frozen conversion and mapping helpers between
compatibility and async loading; async conversion groups output into 64 KiB
batches (plus at most one large bitmap), then awaits writes before buffer reuse.
No complete additional frozen index is retained in heap memory.

Cancellation is checked before/after restoration and between Bitmap batches.
An in-progress engine call completes before cancellation cleanup; pending
writers close and temporary-file guards release on the local-file executor.
Read-only engine mappings and later index destruction retain their normal
ownership. The matching V3 phase split is described in the
[packed scalar design](20260907-async-scalar-index-v3-loading.md#packed-scalar-index-pipeline).

## Slice ownership and memory estimates

The translator inspects each immutable legacy object once at construction and
passes the envelope snapshot through `FileManagerContext`. Resource estimates,
`MemFileManagerImpl` and `DiskFileManagerImpl` reuse it by exact object path;
RTree basenames are resolved before inspection. Snapshots retain sizes and
encoding information, not remote readers. Direct load callers without a snapshot
still inspect normally. There is no global cache or refresh/invalidation state.
This reuse also applies to memory/mmap Knowhere and BSON shared-key loads.

`MemFileManagerImpl::StreamIndexEntriesAsync` validates slice counts and aggregate lengths, prepares one destination per logical
entry, then streams slices into its awaited consumer. `LoadIndexBinarySetAsync`
uses that reader to allocate and fill each required BinarySet entry. It reuses
`LegacyIndexLoader` rather than retaining a map of every decoded slice. Unsliced
entries remain separate entries; sliced entries use their persisted slice metadata.

Each slice follows this lifetime:

```text
admit temporary bytes + one slot
  -> read -> decode if needed -> copy/write into destination
  -> destroy temporary buffers -> release admission
```

Raw unencrypted payloads use bounded ranges. Parquet and encrypted envelopes can
require a whole persisted object to decode. Admission charges that indivisible
unit's estimated peak. An oversized request may run alone, so the configured
byte capacity is not a hard cap below that unit's memory requirement.

For vector memory loads, file-aware planning retains the existing Knowhere
resource estimate and reserves at least final estimated memory plus assembled
payload bytes plus the bounded concurrent decode scratch described below. This covers
the overlap between the BinarySet and the constructed index. These are resource
estimates, not allocator-enforced limits inside Knowhere or codec libraries.
Input retained by Knowhere uses its existing shared ownership.

The BinarySet is request-owned and is not charged to a short-lived slice lease
or a shared overhead group. `SegmentLoadInfo` defers memory/mmap-vector estimates
until a file context is available. Mmap estimates include retained nullable and
empty-list payloads, parsed slice metadata, bounded decode scratch, and up to
three `FileWriter::MAX_BUFFER_SIZE` buffers (main, embedding metadata, raw index).
Knowhere reads the embedding metadata file into heap before restoring its
strategy; that payload also belongs in the estimate. Compatibility sidecar
assembly can overlap codecs and contiguous output, so these retained sidecar
bytes are conservatively counted twice. The file-aware estimate covers both
rollout modes because the switch can change before a subsequent cache load.

Disk loading retains Knowhere's coarse resource estimate and the existing
download allowance, plus one maximum-sized writer buffer. It does not inspect
all disk-index objects for planning: stream-capable backends can use a different
format and avoid caching most files. Consequently this coarse estimate does not
prove a bound on unusually large encoded disk objects, retained disk sidecars,
or backend-internal loading scratch. Actual Milvus slice reads still acquire the
shared admission controller using their inspected decode requirements.

## Concurrent slice window

`StreamLegacyIndexFilesAsync` processes the ordered files of one logical entry
or one local disk file. It splits raw payloads into the existing 16 MiB ranges;
each encoded/encrypted object remains one decode unit. Files are opened lazily
as the window advances. Envelope inspection and logical-entry preparation stay
sequential; small unsliced entries reuse the single-unit path.

Each window contains at most eight outstanding units and 128 MiB of estimated
scratch. These bounds reuse `DEFAULT_FIELD_MAX_MEMORY_LIMIT` and its ratio to
`DEFAULT_INDEX_FILE_SLICE_SIZE`. A decode unit exceeding 128 MiB runs alone in
that load. Every unit also acquires the shared controller's byte and slot lease;
global capacity can further restrict progress, including to a single slot.
With one shared worker, multiple native reads can still be outstanding. Purely
synchronous source reads occupy that worker, so their parallelism depends on
the executor's workers.

The producer acquires leases in destination order before dispatching child
coroutines. Raw ranges are flattened across file boundaries for this purpose:
an earlier file never needs another admission while a later file holds a lease
waiting for it. Memory consumers place completed ranges directly at disjoint
offsets. File consumers await the preceding consumer's completion before
calling the existing sequential `FileWriter` on `LocalFileIOPool`. This retains
its unaligned-slice, buffering, direct-I/O, and write-limiter behavior.

The window advances from its oldest completed unit. A slow first unit can limit
refill, while completed memory ranges release their leases immediately and
other loads can use that capacity. The loader retains only bounded completion
signals, not a separate copied-result queue. Its first error cancels sibling
work; all issued reads and consumers join before buffers, writers, or index
destinations can be destroyed. Waiting and joining suspend coroutines rather
than blocking async workers.

For largest inspected unit scratch `s`, the shared planning helper reserves
`max(s, min(128 MiB, 8 * s))`, using saturating arithmetic. Executor worker count
and refreshable global byte/slot capacities do not reduce this estimate, so
expanding them after translator construction cannot invalidate the legacy
window estimate. Parsed slice metadata remains request-owned alongside that
window in scalar and vector estimates. Writer buffers and retained BinarySet
or sidecar data remain separate. Disk's coarse allowance covers the ordinary
128 MiB window; the oversized-codec/backend limitations above still apply.

Packed scalar V3 uses its separate materializer with the same fixed window
bounds. Its resource estimate covers both rollout modes; shared overhead follows
the admission limits described in the [V3 design](20260907-async-scalar-index-v3-loading.md).

## File layout and Knowhere I/O boundary

Mmap uses the same logical-entry ordering as memory assembly. Sliced entries
follow metadata order and numeric slice order; remaining unsliced entries follow
basename order. Ordinary entries append to the main file. Embedding-list metadata
and MUVERA raw-index entries go to their configured sidecar files. Nullable and
empty-list payloads remain in memory until the shared mmap finalizer restores
them. The compatibility loader reuses this finalizer and retains its transport.

`VectorDiskAnnIndex` reuses `DiskFileManagerImpl::CacheIndexToDiskAsync` for the
selected legacy disk slices. Files use the existing basename/numeric-suffix
layout. The shared disk finalizer restores nullable/empty-list state and calls
Knowhere with the same configuration as the compatibility loader.

`LoadIndexWithStream()` selection is preserved: Milvus stages nullable and
empty-list metadata files and lets Knowhere load the other objects itself.
Empty-list metadata is now included alongside nullable metadata in the shared
selection helper, so metadata-only loads do not call the backend deserializer.
The checked-in Knowhere source has only the default `false` implementation. Its DiskANN
`Deserialize` calls `FileManager::LoadFile` (a no-op in `DiskFileManagerImpl`),
then reads the staged local files through `LinuxAlignedFileReader` and DiskANN's
load/cache code. AISAQ follows the same local-reader pattern. MinHash LSH calls
its local `FileReader` and optionally maps its local index file. Finalization
may therefore perform blocking local I/O on the shared async worker after
staging finishes. Knowhere owns its internal parallelism and subsequent
query-time reads.

Other Knowhere distributions may override `LoadIndexWithStream()`. Their
`FileManagerImpl::OpenInputStream` opens a `RemoteInputStream` directly, outside
`LegacyIndexLoader`; those backend-internal reads are **not newly admitted by
this change**. Eagerly caching every object would change lazy-load behavior, so
that route is retained. Its executor, cancellation, and admission guarantees
need validation in that backend before claiming that all of its reads obey the
Milvus streaming budget. A backend that performs synchronous remote reads inside
`Deserialize` occupies its calling async worker until the call returns. It holds
no `LocalFileIOPool` executor token during that call; cancellation still waits
for the engine to return before releasing its inputs and cleaning up staging.

## Final routing audit

The production call-site audit covers `LoadIndexToMemory`, `CacheIndexToDisk`
and its text/ngram/stats variants, `GetObjectData`, index metadata loaders,
`OpenInputStream`, and explicit HIGH/LOW pool retrievals.

| Reachable load path | Enabled route / remaining boundary |
| --- | --- |
| `SealedIndexTranslator` legacy scalar dispatch | Context-aware `ScalarIndex::Load` invokes `LoadLegacyAsync` on the shared executor |
| Numeric/string Sort, Bitmap, Marisa, Hybrid | Shared BinarySet streamer; Hybrid awaits its child coroutine |
| Tantivy/Ngram and RTree | Shared metadata streamer, concurrent disk-file streamer, engine opening on the async worker |
| JSON scalar wrappers | Await the base coroutine and restore existing missing/null sidecars |
| Knowhere memory and mmap | Shared logical-entry streamer; memory copies by offset and mmap consumers write in order |
| Knowhere disk | Concurrent staging for the files selected by `LoadIndexWithStream`; backend-owned remote reads retain their existing contract |
| Legacy Hybrid/Bitmap resource metadata | Admitted inspection and metadata assembly, scheduled on the shared executor when enabled |
| Packed V3 and FMIndex | Existing `LoadUnifiedAsync` reader/materializer; not routed through the legacy decoder |
| BSON shared-key `BsonInvertedIndexTranslator` | Context-aware `LoadIndex` uses the concurrent disk streamer, then opens Tantivy on the shared async worker |

The remaining synchronous calls in those scalar/vector implementations belong
to their two-argument compatibility loaders and associated metadata overloads.
Index building uses `CacheRawDataToMemory`, `CacheRawDataToDisk`,
`CacheOptFieldToDisk`, and field-data `GetObjectData` consumers. Independent
`TextMatchIndex::Load` and JSON stats metadata retain
their existing loaders. JSON shredding data already has its own async branch,
but that does not migrate every stats file. These entry points do not pass
through the sealed-index dispatch covered by this migration.

HIGH/LOW pools therefore still exist for compatibility and independent loaders.
Shared-overhead accounting uses admission bytes and slots, with no worker-count
lookup or pool construction. Enabled legacy payload tasks submit to the shared
async executor.

## BSON shared-key loading

`BsonInvertedIndexTranslator` passes its `OpContext` to the four-argument
`BsonInvertedIndex::LoadIndex`. This entry reads `StorageV2AsyncLoadEnabled()`
directly. When disabled it calls the existing three-argument synchronous loader;
that compatibility entry remains synchronous even when the global switch is on.
When enabled, only the outer cache caller uses `blockingWait`.

The coroutine reuses `DiskFileManagerImpl::CacheIndexToDiskAsync` with the
manager's generated JSON shared-index directory. It restores the existing
basename/numeric-suffix layout, including multi-slice files, using the same
legacy decoder and bounded concurrent window as other disk loads. HIGH/LOW
select priority on the shared executor and admission controller. They do not
submit payload work to the old HIGH/LOW pools.

Creation, writes, flush/close and load-time removal run on `LocalFileIOPool`.
Tantivy opening runs synchronously on the resumed async worker with no slice
lease or local-file executor held. Tantivy's internal local reads or mapping
occupy that worker until opening completes. Heap loads remove staged files
after opening; mmap loads retain them under the existing index ownership.
Normal cache eviction/destruction keeps its existing cleanup behavior.

Cancellation before admission prevents new reads. Issued reads and writes
drain before the coroutine exits. Cancellation during Tantivy opening is
observed after it returns. A failed or cancelled load releases the reader on
the async worker, then awaits cleanup of its own generated directory before
returning the original exception. Cleanup does not remove another load's
generated directory. There is no new retry classification: the existing Rust
Tantivy binding returns engine errors as strings and the C++ wrapper reports
them through its existing assertion error. Local directory creation can still
propagate the existing Boost filesystem exception; this increment preserves
that exception rather than assigning a new error category.

The translator inspects persisted envelopes once at construction and reuses that
snapshot on async loads and reloads. Its final
size estimate is at least the decoded file total and retains a larger supplied
JSON stats size estimate. Heap loads reserve that memory plus temporary disk;
mmap loads reserve final disk. Temporary memory additionally covers
`LegacyIndexMaxTransientBytes(largest_unit_scratch)` plus one
`FileWriter::MAX_BUFFER_SIZE`, while retaining a larger compatibility download
allowance for mmap. The estimate covers both switch settings and does not
shrink with worker count, admission capacity, or the current writer-buffer
setting. It is an estimate of engine residency, not a hard bound on allocations
inside Tantivy. Shared-key loading does not introduce a BinarySet copy of all
staged files.

## Cancellation and failures

Cancellation before admission prevents new reads. An issued async read drains
before releasing its destination or lease. Cancellation between materialization
and finalization skips Knowhere. During the synchronous finalizer, cancellation
is reported after it returns: the BinarySet, mapped files, and index remain
alive throughout. File writes likewise drain before their borrowed read buffer
or admission lease is released. Failed mmap loads remove the targets prepared by
that call. Configured mmap filenames replace stale targets, matching the
compatibility path and allowing cache reloads; unrelated files are preserved.
Failed disk loads remove the file manager's generated staging directory.
Successful loads retain the existing file ownership and unmapping behavior.

Existing typed storage failures propagate through the new boundary. Escaping
`std::bad_alloc` and Folly cancellation become `MemAllocateFailed` and
`FollyCancel`. The existing Knowhere finalizer still translates failed Knowhere
statuses to its existing `UnexpectedError`; Knowhere can catch exceptions
internally before they reach Milvus. This increment does not establish new
end-to-end retry classification for Knowhere failures.

Thread-local active trace scopes are not held across coroutine suspension.
Read and engine spans keep their existing names; the synchronous engine phase
can safely use an active scope.

## Verification scope

`VectorMemIndexAsyncLoadTest` covers sliced/unsliced memory loads, enabled and
disabled routing, nullable IDs, empty embedding lists, representative dense,
binary and sparse indexes, native async completion, cancellation while waiting
or finalizing, read/finalizer failures, and input/scratch resource estimates.
It uses one async worker and one admission slot, and checks that native reads
suspend that worker and that finalization starts after admission is released.
File-backed coverage adds real mmap load/query parity, nullable and empty-list
metadata, TokenANN/MUVERA sidecars, executor selection, failure cleanup,
cancellation during finalization, and local-pool shutdown during remote reads.
Controlled mmap/disk finalizers also check that the single local-file worker
can execute another task while Knowhere deserialization is paused on the async
worker, and that failure cleanup happens after deserialization drains.
The cached build has `WITH_DISKANN=OFF`: controlled Knowhere nodes test the disk
staging/finalization boundary and stream-selection behavior. A real MinHash LSH
round trip also exercises the disk adapter with nullable IDs and both heap and
mapped hash-code storage. These checks do not validate DiskANN query results or
a proprietary stream implementation.

The shared legacy-loader suite covers raw/Parquet/encrypted decoding, malformed
envelopes, short reads, decoder failures and draining reads on cancellation.
Concurrent cases control native read completion to exercise a slow first range,
reversed completion across raw/encoded file boundaries, offset placement and
ordered writes, encrypted decoding with multiple workers, and byte-for-byte
memory/disk assembly. They also cover the eight-unit and byte windows with
unlimited global admission, indivisible oversized units, shrinking global
limits, HIGH/LOW contention, and cancellation/consumer failure while sibling
reads remain outstanding. The vector estimate test compares rollout modes and
worker/budget settings against the same stable peak estimate.
Throughput benchmarks and remote-cluster tests are not part of this verification.

On 2026-09-10 the GCC 12 Release `all_tests` target rebuilt successfully with
up to 16 concurrent build jobs. After moving Knowhere file deserialization to
the shared async worker, the focused run passed all 22 vector-loading cases.
The broader run passed all 456 selected tests from 14 suites, with no failures
or skips. It covers legacy scalar and vector loads, storage codecs, shared async
infrastructure, admission, segment-resource estimates, and `FileWriter`
failures. The vector cases exercise sparse engine versions 6 and 8 and MUVERA
sidecars at version 11. DiskANN remains disabled in this build; its backend I/O
boundary was inspected statically.

On 2026-09-11 the BSON increment rebuilt `all_tests` and `json_stats_test` with
GCC 12 Release, at most 16 build jobs and nested builders capped at one job.
All eight `BsonInvertedIndexAsyncLoadTest` cases passed. They exercise real
Tantivy build/upload/load/query in heap and mmap modes, HIGH/LOW priority,
rollout changes between reloads, the synchronous compatibility entry, numeric
multi-slice assembly, native read suspension and cancellation with one worker
and one admission slot, cancellation while waiting for admission, read and
envelope failures, invalid Tantivy metadata, failed destination creation,
directory isolation, and estimates across configuration changes. The
synchronous engine-open cancellation boundary was traced statically; no test
hook pauses Tantivy internally.

The broader run passed all 464 selected cases from 15 suites, including those
eight BSON cases and the existing scalar/vector/storage/admission regression
set. The separate JSON stats binary passed all 84 cases from 13 suites. Neither
run had failures or skips. No remote-cluster or throughput test was run for this
increment.

### Scalar finalizer executor follow-up (2026-09-11)

The finalizer split rebuilt both C++ test targets and passed 44 focused scalar
cases, 655 related regression cases, and 84 JSON stats cases (783 distinct
cases, no failures or skips). The focused suite now observes actual
`ComputeByteSize` restoration on async workers for legacy consumers, including
Marisa, and probes the single local-file worker while Sort/StringSort/Marisa/
Bitmap/Tantivy/RTree restoration is blocked. The four BinarySet consumers also
exercise V3/legacy, memory/mmap, cancellation and injected restoration failure.
The regression set includes typed synchronous Bitmap mmap/array/nullable cases
and existing writer, streamer, admission, vector and BSON checks. See the
[packed scalar validation](20260907-async-scalar-index-v3-loading.md#scalar-finalizer-executor-validation-2026-09-11)
for the coverage boundary.


### Design review follow-up (2026-09-11)

Immutable envelope snapshots now reach both file managers through the loading
context. Tests verify that actual loads reuse the inspection and BSON reloads
do not reread descriptor prefixes; whole encoded-object payload reads still
start at offset zero. Ngram's synchronous heap path again removes its staging
directory after engine restoration. The shared fixed streaming window also
bounds packed V3 loads independently of worker-count changes.

Both test targets built successfully, and 838 distinct selected cases passed
without failures or skips. See the
[review validation](20260907-async-scalar-index-v3-loading.md#design-review-follow-up-validation-2026-09-11)
for the executed coverage and its limits.
