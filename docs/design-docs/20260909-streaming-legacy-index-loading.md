# Streaming legacy scalar and Knowhere index loads

## Scope

This extends [async scalar-index V3 loading](20260907-async-scalar-index-v3-loading.md)
to legacy scalar formats and Knowhere memory, mmap, and disk staging loads. It shares
`storage::AsyncLoadExecutor`, `LoadAdmissionController::GetInstance()`, and
`LocalFileIOPool`. It adds no controller singleton or per-index executor.

The migration is incremental:

1. Legacy numeric Sort and shared admitted transport.
2. Remaining legacy scalar consumers, metadata, and file destinations.
3. Knowhere memory loading through `VectorMemIndex`.
4. Knowhere mmap/disk loading, including its stream/lazy-load contracts.
5. Bounded concurrent slice reads and a final routing audit.

Steps 1–4 are implemented at the Milvus load boundary. Step 5 remains follow-up
work; internal remote reads in stream-capable Knowhere implementations retain
their own admission contract, as detailed below. Index building,
uploads, and independent text-match/JSON-key stats entries are outside these
steps. Packed scalar V3 keeps its existing reader and materializer.

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
| Native `ReadAtAsyncInto` | Storage backend owns I/O and completion | Coroutine suspends and resumes on the shared async executor |
| Buffered Arrow `ReadAsync` fallback | Arrow I/O context / backend; its completion callback copies the returned buffer | Coroutine resumes on the shared async executor after that copy |
| ChunkManager-only or rooted-local source read | Shared async executor | Existing synchronous read; no submission to HIGH/LOW |
| Decode/decrypt a legacy envelope; copy into its final BinarySet entry | Shared async executor | CPU work runs directly; consumer completes before its slice lease is released |
| Restore vector nullable/empty-list sidecars and call `LoadWithoutAssemble` | Shared async executor | Synchronous CPU phase, with no slice admission held |
| Call memory Knowhere `Deserialize` | Same shared async worker | One synchronous call; it occupies this worker until Knowhere returns |
| Create/write/flush/close mmap files or disk slices | `LocalFileIOPool` | Coroutine awaits the operation; a read/decode lease survives each write |
| Restore file-backed nullable/empty-list metadata; invoke `DeserializeFromFile` or disk `Deserialize` | `LocalFileIOPool` | Files are closed first; one synchronous call, with no slice admission held |
| Failure cleanup of files created by the load | `LocalFileIOPool` | Issued I/O and synchronous finalization drain before cleanup |
| Parallel work inside `Deserialize` | Whatever workers / parallel runtime Knowhere selects | Milvus does not create deserialization tasks or impose an additional parallelism limit |
| Publish a completed index | Existing cache-load caller after successful return | Cancellation or failure prevents publishing that result |

Knowhere is responsible for its internal deserialization parallelism. A
deserializer that executes serially continues on the calling async worker. The
integration does not imply that every deserializer uses a Knowhere thread pool.
It also does not make Knowhere deserialization incremental: the complete
`BinarySet` is available before the call starts.

The inspected FLAT, IVF, HNSW, sparse, and embedding-list memory deserializers
consume memory readers / BinarySet entries. Some sparse implementations retain
shared ownership of input buffers. They keep that ownership contract. GPU and
remote-cluster execution have not been validated by this increment.

Legacy scalar consumers use the same transport phases. Their final phase varies:

| Consumer phase | Executor |
| --- | --- |
| Pure memory representation finalization | Shared async executor |
| Hybrid child selection and child coroutine | Shared async executor; directly `co_await` the child |
| Marisa temporary-file loading, scalar mmap finalization | `LocalFileIOPool` |
| Tantivy/RTree directory preparation, writes, finish/open, failure cleanup | `LocalFileIOPool` |
| JSON wrapper bitmap restoration | Resumes on the shared async executor after the base coroutine |

Local-file work uses the existing disabled-pool fallback to the shared async
executor. For Milvus staging, a local-file executor token does not span remote
I/O. Cross-executor operations are awaited; an async worker never calls a child's blocking load
wrapper. Existing disk-index construction still prepares its generated directory
on the constructing caller; the table describes the subsequent sealed `Load`.

## Slice ownership and memory estimates

`MemFileManagerImpl::StreamIndexEntriesAsync` inspects persisted envelopes,
validates slice counts and aggregate lengths, prepares one destination per logical
entry, then streams slices into its awaited consumer. `LoadIndexBinarySetAsync`
uses that reader to allocate and fill each required BinarySet entry. It reuses
`LegacyIndexLoader` rather than retaining a map of every decoded slice. Unsliced entries remain separate entries;
sliced entries are reconstructed using their persisted slice metadata.

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
payload bytes plus the largest estimated decode/metadata scratch. This covers
the overlap between the BinarySet and the constructed index. These are resource
estimates, not allocator-enforced limits inside Knowhere or codec libraries.
Input retained by Knowhere uses its existing shared ownership.

The BinarySet is request-owned and is not charged to a short-lived slice lease
or a shared overhead group. `SegmentLoadInfo` defers memory/mmap-vector estimates
until a file context is available. Mmap estimates include retained nullable and
empty-list payloads, parsed slice metadata, the largest decode scratch, and up to
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

Steps 1–4 process one slice at a time within a load; different loads can progress
concurrently. Step 5 will add bounded intra-load concurrency.

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
its local `FileReader` and optionally maps its local index file. Milvus's local
finalization call may therefore do blocking local I/O; Knowhere owns any internal
parallelism and subsequent query-time reads.

Other Knowhere distributions may override `LoadIndexWithStream()`. Their
`FileManagerImpl::OpenInputStream` opens a `RemoteInputStream` directly, outside
`LegacyIndexLoader`; those backend-internal reads are **not newly admitted by
this change**. Eagerly caching every object would change lazy-load behavior, so
that route is retained. Its executor, cancellation, and admission guarantees
need validation in that backend before claiming that all of its reads obey the
Milvus streaming budget. A backend that performs synchronous remote reads inside
`Deserialize` occupies its calling local-file worker and retains that phase's
executor token until the call returns; the staging shutdown guarantee does not
extend to those opaque reads.

## Cancellation and failures

Cancellation before admission prevents new reads. An issued async read drains
before releasing its destination or lease. Cancellation between materialization
and finalization skips Knowhere. During the synchronous finalizer, cancellation
is reported after it returns: the BinarySet, mapped files, and index remain
alive throughout. File writes likewise drain before their borrowed read buffer
or admission lease is released. Failed mmap loads remove the targets prepared by
that call. Configured mmap filenames replace stale targets, matching the
compatibility path and allowing cache reloads; unrelated files are preserved.
Failed disk loads remove the file manager's generated staging directory. Successful loads retain
the existing file ownership and unmapping behavior.

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
The cached build has `WITH_DISKANN=OFF`: controlled Knowhere nodes test the disk
staging/finalization boundary and stream-selection behavior. A real MinHash LSH
round trip also exercises the disk adapter with nullable IDs and both heap and
mapped hash-code storage. These checks do not validate DiskANN query results or
a proprietary stream implementation.

The shared legacy-loader suite covers raw/Parquet/encrypted decoding, malformed
envelopes, short reads, decoder failures and draining reads on cancellation.
Throughput benchmarks and remote-cluster tests are not part of this verification.

On 2026-09-10 the GCC 12 Release `all_tests` target rebuilt successfully with
up to 16 concurrent build jobs. All 447 selected tests from 14 suites passed,
including 22 vector-loading cases, existing vector load/query cases, scalar
streaming, storage codecs, shared async infrastructure, admission,
segment-resource estimates, and `FileWriter` failures. The vector cases exercise
sparse engine versions 6 and 8 and MUVERA sidecars at version 11. DiskANN remains
disabled in this build; its backend I/O boundary was inspected statically.
