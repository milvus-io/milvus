# Streaming legacy scalar, Knowhere, BSON and TextMatch index loads

> Rebased onto packed scalar PR #53483 (`041cf55efa`) on 2026-09-20.
> Validation entries dated before this rebase describe the old branch, not this base.

## Scope

This extends [async scalar-index V3 loading](design_docs/20260907-async-packed-scalar-index-loading.md)
to legacy scalar formats, Knowhere memory, mmap, and disk staging loads, and
BSON shared-key and independent TextMatch indexes. It shares
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
uploads, and independent TextMatch/JSON-key stats entries were outside those
five steps. The subsequent BSON shared-key and TextMatch increments are
described below. JSON stats `meta.json` and Parquet schema/footer planning now
also use admitted async reads. Packed scalar V3 uses the entry reader and load plan from PR #53483.

The context-aware sealed-index `Load(trace, config, OpContext*)` uses the
mode pinned in `FileManagerContext`, falling back to
`StorageV2AsyncLoadEnabled()` for direct callers. Enabled legacy scalar and vector loads
enter the shared executor. Disabled loads call their existing synchronous
loader. The two-argument compatibility entry remains synchronous.

## Executor ownership by phase

HIGH and LOW on the new path are priorities on the shared async executor; they
do not select the old HIGH/LOW worker pools.

| Phase | Executor / thread | Waiting behavior |
| --- | --- | --- |
| File-aware resource estimate | Shared async executor when enabled; planning caller when disabled | Envelope reads acquire async admission; the outer planning caller uses `blockingWait` |
| Sealed-load dispatch | Existing synchronous cache-load caller | One outer `blockingWait` schedules the complete load coroutine |
| Open object, obtain size, parse envelope and slice metadata | Shared async executor | Remote objects use `RemoteInputStream::OpenAsync`; ChunkManager-only size/open remains synchronous |
| Wait for slice admission | Shared async executor | Coroutine suspends without occupying a worker |
| Dispatch concurrent slices and join their completion | Shared async executor | Child coroutines run on the caller's executor; no nested `blockingWait` |
| Native `ReadAtAsyncInto` | Storage backend owns I/O and completion | Coroutine suspends and resumes on the shared async executor |
| Buffered Arrow `ReadAsync` fallback | Arrow I/O context / backend | The existing stream resumes on the shared async executor and copies the returned buffer there |
| ChunkManager-only or rooted-local source read | Shared async executor | Existing synchronous read; no submission to HIGH/LOW |
| Decode/decrypt a legacy envelope; copy into its final BinarySet entry | Shared async executor | CPU work runs directly; consumer completes before its slice lease is released |
| Restore vector nullable/empty-list sidecars and call `LoadWithoutAssemble` | Shared async executor | Synchronous CPU phase, with no slice admission held |
| Call memory Knowhere `Deserialize` | Same shared async worker | One synchronous call; it occupies this worker until Knowhere returns |
| Create/write/flush/close mmap files or disk slices | `LocalFileIOPool` | Coroutine awaits the operation; a read/decode lease survives each write |
| Restore file-backed nullable/empty-list metadata; invoke `DeserializeFromFile` or disk `Deserialize` | Shared async executor | Files are closed first; the synchronous call occupies this worker, with no slice admission or local-file executor held |
| Read and parse JSON stats `meta.json` | Shared async executor | One admission lease covers the whole metadata input and parser scratch; range reads are awaited sequentially, without local staging |
| Plan JSON stats Parquet files | Shared async executor | Probe size/trailer under a small lease, release it, then admit footer reads, schema conversion and legacy layout restoration; at most 16 files per group run concurrently |
| Open BSON shared-key or TextMatch Tantivy reader, including heap loading or mapping | Shared async executor | One synchronous engine call after all staged files close and slice admission releases |
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
compatibility and async loading; async conversion groups output into 16 MiB
batches (plus at most one large bitmap), then awaits writes before buffer reuse.
No complete additional frozen index is retained in heap memory.

Cancellation is checked before/after restoration and between Bitmap batches.
An in-progress engine call completes before cancellation cleanup; pending
writers close and temporary-file guards release on the local-file executor.
Read-only engine mappings and later index destruction retain their normal
ownership. The matching V3 phase split is described in the
[packed scalar design](design_docs/20260907-async-packed-scalar-index-loading.md#reading-the-pipeline).

## Slice ownership and memory estimates

The translator inspects each immutable legacy object once at construction and
passes the envelope snapshot through `FileManagerContext`. Resource estimates,
`MemFileManagerImpl` and `DiskFileManagerImpl` reuse it by exact object path;
RTree basenames are resolved before inspection. Snapshots retain sizes and
encoding information, not remote readers. Direct load callers without a snapshot
still inspect normally. There is no global cache or refresh/invalidation state.
This reuse also applies to memory/mmap Knowhere and BSON shared-key loads.

`MemFileManagerImpl::InspectIndexEntriesAsync` validates slice counts and
aggregate lengths and groups source objects by logical entry. Destinations use
PR #53483's `MemoryEntryTarget` and `FileEntryTarget`; `ReadIndexEntriesAsync`
fills those targets through the legacy envelope reader. `LoadIndexBinarySetAsync`
allocates final BinarySet entries as memory targets. No consumer factory or
separate destination hierarchy is needed.

Remote open, size, retries and direct/buffered async reads use the existing
`RemoteInputStream`. Packed and legacy exact-range reads share the same drained
`InputStream::ReadAtAsync` boundary. Legacy parsing remains separate because
packed directory/CRC/encryption layouts do not describe old multi-object files.

BSON, Tantivy, RTree and Knowhere retain an `IndexLoadPlan` through engine
finalization. `IndexFileTarget` owns prepare/write/finish/commit/cleanup; plans
are released on `LocalFileIOPool`. Directory leases and collision checks remain
with the file manager. Raw JSON and Parquet metadata reuse streams/admission,
without constructing artificial index entries.

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
three `FileWriter::MAX_BUFFER_SIZE` buffers for the compatibility path (main,
embedding metadata, raw index). Async positioned writes hold an aligned copy
only during the write; the slice admission includes it and tail padding.
Knowhere reads the embedding metadata file into heap before restoring its
strategy; that payload also belongs in the estimate. Compatibility sidecar
assembly can overlap codecs and contiguous output, so these retained sidecar
bytes are conservatively counted twice. The file-aware estimate uses the loading
mode captured by the cache translator, so cached reloads keep the same path.

Disk loading retains Knowhere's coarse resource estimate and inspects persisted
objects to account for staging scratch, retained sidecars and writer buffers.
This remains conservative for stream-capable backends that avoid staging some
files, and does not bound backend-internal loading allocations. Actual Milvus
slice reads acquire the shared admission controller using their inspected decode
requirements.

## Concurrent slices and admission

`StreamLegacyIndexFilesAsync` processes the ordered files of one logical entry
or one local disk file. It splits raw payloads into the existing 16 MiB ranges;
each encoded/encrypted object remains one decode unit. Files are opened as
submission advances. Envelope inspection and logical-entry preparation stay
sequential; small unsliced entries reuse the single-unit path.

Every unit acquires the global controller's byte and slot lease before dispatch.
There is no additional per-load task count or byte limit. A unit larger than
the global byte budget follows the controller's oversized-unit rule. With one
shared worker, multiple native reads can still be outstanding. Synchronous
source reads occupy that worker, so their parallelism depends on worker count.

Raw ranges are flattened across file boundaries. All consumers place disjoint
ranges by offset; file consumers await `PositionedFileWriter::WriteAt` on
`LocalFileIOPool`. There is no preceding-slice wait. `WriteAt` and `Finish` share
the existing global write permits with `FileWriter`; permits cover blocking
local I/O and never span remote reads or coroutine waits.

Read/decode buffers are destroyed and admission released after consumption.
The release wakes pending admissions directly, without a per-load completion
queue. A slow first unit does not prevent other completed units from releasing
capacity. The first error cancels sibling work; all issued reads and consumers
join before buffers, writers, or index destinations can be destroyed. Waiting
and joining suspend coroutines rather than blocking async workers.

Async legacy scalar, Knowhere (including disk staging), and BSON estimates sum
the inspected objects' possible scratch buffers. For raw objects this includes all
16 MiB read ranges and aligned write copies, with padding only for the tail;
encoded objects contribute their whole-object decode charge. Parsed slice
metadata, writer buffers and retained BinarySet or sidecar data remain separate.
These estimates remain valid across changes to workers and admission limits,
including unlimited admission. They are conservative: request-local overhead
cannot currently combine retained buffers with partially shared scratch
accounting. For those paths, the scratch estimate can exceed the scratch
permitted by the current global budget, especially for large file-backed loads.

Packed scalar V3 uses its separate entry reader and load plan and the same global admission.
Its estimate covers both rollout modes; eligible shared overhead follows the
live admission limits described in the [V3 design](design_docs/20260907-async-packed-scalar-index-loading.md).
The disabled HIGH/LOW loading paths retain their existing batching limits and
a 128 MiB download allowance. Their batch contains
`128 MiB / common.indexSliceSize` whole objects (normally eight). Planning uses
the larger of 128 MiB and the inspected scratch required by that batch; oversized
objects and encoded-object decoding can require more. BinarySet retention,
metadata and writer buffers are additional costs, not part of the 128 MiB.

Sealed-index, TextMatch and BSON cache translators capture the loading mode in
`FileManagerContext` when created. Their estimates and all reloads use that mode,
matching the existing manifest-reader lifetime. A global switch update applies
to new cache entries; it cannot make an old entry run async with a legacy budget.
Direct loads without a cache-owned estimate still read the live switch.

## File layout and Knowhere I/O boundary

Mmap prepares entries in the same logical order as memory assembly, but slices
within an entry may write concurrently. Knowhere serializes one main index entry;
embedding-list metadata and MUVERA raw-index entries each use a separate file.
Async preparation learns each entry's exact length and opens its positioned
writer on `LocalFileIOPool` before issuing reads. A second entry targeting the
same file fails an assertion, so entry-relative offsets remain file-relative.
Configured cache targets are cleared before streaming, including unused targets
on all-null reloads. Nullable and
empty-list payloads remain in memory until the shared mmap finalizer restores
them. The compatibility loader reuses this finalizer and retains its transport.

Production non-tail slices are whole MiB (`common.indexSliceSize`, default
16 MiB); raw read ranges are capped at 16 MiB. Compile-time and configuration
assertions enforce valid, 4 KiB-aligned slice sizes.
The positioned writer copies
unaligned addresses into aligned allocations, pads only the file tail, and
truncates to the exact length after all writes drain. No extra buffered fallback
is selected for these consumers; the existing write-mode/priority policy remains.
Raw admission charges the read buffer plus the 4 KiB-rounded write copy.

Async download histograms observe each successful read/decode unit before its
consumer starts. Legacy file-target write histograms observe each actual local write
and each file close, excluding executor queueing. These are per-operation
durations, not an index load's wall time; overlapping write durations are never
subtracted from total load duration. Synchronous metric sampling is unchanged.

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
| Knowhere memory and mmap | Shared logical-entry streamer; memory copies and mmap writes place disjoint ranges by offset |
| Knowhere disk | Concurrent staging for the files selected by `LoadIndexWithStream`; backend-owned remote reads retain their existing contract |
| Legacy Hybrid/Bitmap resource metadata | Admitted inspection and metadata assembly, scheduled on the shared executor when enabled |
| Packed V3 and FMIndex | Existing `LoadUnifiedAsync` entry reader and load plan; not routed through the legacy decoder |
| BSON shared-key `BsonInvertedIndexTranslator` | Context-aware `LoadIndex` uses the concurrent disk streamer, then opens Tantivy on the shared async worker |
| Independent `TextMatchIndexTranslator` | Context-aware `TextMatchIndex::Load` selects inherited Tantivy legacy loading or the packed V3 entry reader and load plan |
| JSON stats `meta.json` | Context-aware `JsonKeyStats::Load` reads and parses raw JSON under admission on the shared executor |
| JSON shredding Parquet schema/footer planning | Admitted `ParquetFormatReader::open_async()` preparation; first-file schema is reused for field mapping and column creation |

The remaining synchronous calls in those scalar/vector implementations belong
to their two-argument compatibility loaders and associated metadata overloads.
Index building uses `CacheRawDataToMemory`, `CacheRawDataToDisk`,
`CacheOptFieldToDisk`, and field-data `GetObjectData` consumers. JSON shredding
data has its own async branch; the separate Parquet planning phase now also
uses async reads and admission. HIGH-pool metadata tasks remain only in the
switch-disabled JSON stats path. Provider file-open and generic size fallback
calls can still block their invoking worker.

HIGH/LOW pools therefore still exist for compatibility and independent loaders.
Shared-overhead accounting uses admission bytes and slots, with no worker-count
lookup or pool construction. Enabled legacy payload tasks submit to the shared
async executor.

## Independent TextMatch loading

`TextMatchIndexTranslator` passes its `OpContext` into `TextMatchIndex::Load`.
With async loading enabled, legacy relative filenames are resolved against
`stats_base_path` and passed to the inherited Tantivy `LoadLegacyAsync`.
Its existing `is_index_file_` flag selects the text-log directory and cleanup.
TextMatch reuses the existing streamer, load plan, and finalizer.
With the switch disabled, the legacy loader keeps its HIGH/LOW scheduling.

A single `.v3` file already used `LoadUnified`; that branch now forwards the
cancellation context. The shared V3 path reads from the text-log prefix and
uses the existing generated index staging directory. Both async formats open
Tantivy and build validity on the shared async worker after file writes close.
`LocalFileIOPool` handles creation, writes, flush/close, and cleanup. Analyzer
registration stays on the cache-load caller after loading. A final cancellation
check prevents the translator from publishing a cancelled result.

Resource planning reuses the ordinary Tantivy file-aware estimator, selecting
the text-log prefix for packed files. Legacy planning resolves the same remote
paths and saves the immutable envelope snapshot in `FileManagerContext`; both
file managers reuse it during loading. Inspection uses the same admitted
coroutine in both modes: enabled schedules it on the shared async executor;
disabled drives it on the planning caller, without HIGH/LOW dispatch. That
planning choice is separate from compatibility payload loading. Estimates
cover validity, retained null-offset/slice metadata, bounded transient reads
and temporary files for the mode retained by the cache entry. Shared overhead
is used only when the existing estimator can prove that admission leases cover
it completely.

## JSON stats metadata loading

`BuildJsonKeyStatsIndex` forwards its `OpContext` into the context-aware
`JsonKeyStats::Load` overload. When enabled, the raw `meta.json` object is opened
with the existing exact-path helper and read directly into its final string
buffer. It is not a legacy envelope or a packed scalar index. The coroutine
uses the existing range reader and `JsonStatsMeta::DeserializeToKeyFieldMap`;
it does not write a staging file or create another translator.

Opening, reading and parsing run on the shared async executor. Native range
reads suspend it; Arrow fallback retains the existing buffer-copy behavior,
and ChunkManager-only/rooted-local reads execute synchronously on that worker.
One admission lease covers the complete metadata input and parsing, using the
same conservative `32 * file_bytes + 4096` scratch estimate as packed-index
catalogs. Ranges are bounded by the existing default stream slice size and
read sequentially under that lease. An oversized metadata object follows the
controller's existing exclusive-admission rule, without a nested acquisition.
The existing parser still materializes the complete JSON document and key map.
Its durable key map becomes part of `JsonKeyStats` after successful parsing.

Cancellation is checked before reading, between ranges, before/after parsing,
and at subsequent load-stage boundaries. An issued native read drains before
its destination and admission lease are released. The same context reaches
BSON shared-key cache creation and synchronous warmup; background warmup
retains its slot-owned cancellation. Synchronous parsers remain cooperative
boundaries: cancellation waits for them to return.

With the switch disabled, `CacheJsonStatsMetaToDisk` and `LoadMetaFile` retain
their existing synchronous ChunkManager/local-file flow on the calling thread.
Files without `meta.json` retain the Parquet metadata fallback. The subsequent
Parquet planning increment below migrates that fallback as well as schema and
row-count reads.

Validation passed all 85 JSON stats tests and 19 related BSON/JSON query tests.
The metadata check uses one async worker and one admission slot, covering two
bounded native ranges, cancellation during admission and an issued read, read
errors, short reads, malformed JSON, and absence of local staging. Compatibility
loading and missing-metadata fallback are checked in both rollout modes. These
checks use local files and controlled native reads; real object-store validation
remains a follow-up.

## JSON stats Parquet planning

Enabled loads prepare Parquet metadata on the shared async executor. Each file
first acquires 4096 bytes and one slot, opens the existing Arrow file source,
resolves its size through the native async API when available, and reads only
the eight-byte trailer with the shared range reader. The trailer must describe
a nonempty plaintext footer wholly inside the file. JSON stats' packed writer
uses default, unencrypted Parquet writer properties.

The probe lease is released before acquiring `32 * footer_bytes + 64 KiB` and
one slot. The factor is a conservative parser/scratch estimate, not measured
allocator accounting. The extra 64 KiB covers Arrow 17's native tail-read
fallback when a hinted footer cannot be parsed. Oversized units use the
existing exclusive-admission rule. There is no nested acquisition.

`ParquetFormatReader::open_async()` receives the probed file and footer sizes;
it owns footer reads, schema conversion and row-group interpretation. The
first file's schema restores field mappings and, when needed, the legacy JSON
layout map while the same lease is held. Files are immutable build outputs;
preparation retains one schema per group and one row count per file, without
retaining decoded footer objects or duplicate schemas. A successful file uses
one small trailer read plus the reader's footer read; the format reader reopens
the source with known size so the second open can avoid another size request.
No local staging or `LocalFileIOPool` operation is involved in planning.

Within each group, `collectAllWindowed(..., 16)` permits bounded file concurrency
on the shared executor and preserves file-ID order. Groups are prepared in
order. Failure cancels queued sibling work and drains issued size/footer
requests before returning; external cancellation takes precedence. The callback
that restores the group's schema runs only for its first file, so mutable field
maps are not written by competing file tasks. Native I/O suspends the worker;
provider `OpenInputFile` and generic `GetSize` can still block it. Arrow's generic
range fallback retains its existing I/O executor behavior.

All planning leases end before the common column creation path opens projected
data readers. That path reuses the prepared schema/row counts and forwards the
load context to existing `OpenChunkReadersAsync` calls for eager and lazy loads.
Cache warmup and data materialization retain their existing executors. Closing
the switch retains synchronous schema reads and HIGH-pool tasks for additional
files; shared schema restoration and column creation contain no new I/O path.

Validation passed 86 JSON stats tests and 41 related BSON, JSON query and async
index-reader tests. The new check uses real consecutive Parquet files with
controlled native I/O: one worker/one slot, concurrent and out-of-order footer
completion, cancellation during native size and footer requests, cancellation
while decode admission waits, read failure, invalid trailer/length, and corrupt
metadata exercising Arrow's 64 KiB fallback. It verifies drained requests and
released admission, then reads the actual lazy columns in file-ID order.
Missing metadata and historical footer-embedded layout maps are checked with
the switch both off and on. These are local/controlled-I/O checks; production
object-store behavior and the scratch multiplier still need workload validation.

## BSON shared-key loading

`BsonInvertedIndexTranslator` passes its `OpContext` to the four-argument
`BsonInvertedIndex::LoadIndex`. This entry reads `StorageV2AsyncLoadEnabled()`
directly. When disabled it calls the existing three-argument synchronous loader;
that compatibility entry remains synchronous even when the global switch is on.
When enabled, only the outer cache caller uses `blockingWait`.

The coroutine reuses `DiskFileManagerImpl::CacheIndexToDiskAsync` with the
manager's generated JSON shared-index directory. It restores the existing
basename/numeric-suffix layout, including multi-slice files, using the same
legacy decoder and global admission as other disk loads. HIGH/LOW
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
the mode-specific scratch estimate described above plus one
`FileWriter::MAX_BUFFER_SIZE`, while retaining a larger compatibility allowance
for mmap. The translator keeps the mode used for its estimate throughout its
lifetime. Its estimate does not shrink with worker count, admission capacity,
or the current writer-buffer setting. It is an estimate of engine residency, not a hard bound on allocations
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
memory/disk assembly. They also cover dispatch beyond eight units with unlimited global admission,
byte limits controlled by global admission, indivisible oversized units, shrinking global
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
All eight `BsonInvertedIndexTest` cases passed. They exercise real
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
[packed scalar validation](design_docs/20260907-async-packed-scalar-index-loading.md#validation)
for the coverage boundary.


### Design review follow-up (2026-09-11)

Immutable envelope snapshots now reach both file managers through the loading
context. Tests verify that actual loads reuse the inspection and BSON reloads
do not reread descriptor prefixes; whole encoded-object payload reads still
start at offset zero. Ngram's synchronous heap path again removes its staging
directory after engine restoration. At that revision, a shared fixed streaming
window also bounded packed V3 loads independently of worker-count changes.

Both test targets built successfully, and 838 distinct selected cases passed
without failures or skips. See the
[review validation](design_docs/20260907-async-packed-scalar-index-loading.md#validation)
for the executed coverage and its limits.

### TextMatch validation (2026-09-11)

Both C++ test targets built with GCC 12 Release, at most 16 build jobs and
nested builders capped at one job. Six focused TextMatch cases passed,
including the three new async cases. The broader scalar/vector/BSON/TextMatch
run passed 104 cases, and JSON stats passed 84. These are 191 distinct cases,
with no failures or skips.

TextMatch coverage includes legacy/V3, heap/mmap, relative text-log paths,
sliced Tantivy files and null-offset metadata, query/null parity, and estimates
across rollout changes. With one async worker and one admission slot, a native
read can remain pending while another async task runs; cancellation waits for
that read to drain and prevents cell publication. Cancellation at finalization
also removes the staged directory for both formats and storage modes. These
are local and controlled-backend tests, without remote-cluster throughput
measurements.
