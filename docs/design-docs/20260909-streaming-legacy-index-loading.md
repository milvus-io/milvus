# Streaming legacy scalar and Knowhere index loads

## Scope

This extends [async scalar-index V3 loading](20260907-async-scalar-index-v3-loading.md)
to legacy scalar formats and Knowhere memory loads. It shares
`storage::AsyncLoadExecutor`, `LoadAdmissionController::GetInstance()`, and
`LocalFileIOPool`. It adds no controller singleton or per-index executor.

The migration is incremental:

1. Legacy numeric Sort and shared admitted transport.
2. Remaining legacy scalar consumers, metadata, and file destinations.
3. Knowhere memory loading through `VectorMemIndex`.
4. Knowhere mmap/disk loading, including its stream/lazy-load contracts.
5. Bounded concurrent slice reads and a final routing audit.

Steps 1–3 are implemented. Steps 4–5 remain follow-up work. Index building,
uploads, and independent text-match/JSON-key stats entries are outside these
steps. Packed scalar V3 keeps its existing reader and materializer.

The context-aware sealed-index `Load(trace, config, OpContext*)` reads
`StorageV2AsyncLoadEnabled()` directly. Enabled legacy scalar and vector memory
loads enter the shared executor. Disabled loads call their existing synchronous
loader. The two-argument compatibility entry remains synchronous. Vector calls
with `MMAP_FILE_PATH` and disk-index classes await step 4.

## Executor ownership by phase

HIGH and LOW on the new path are priorities on the shared async executor; they
do not select the old HIGH/LOW worker pools.

| Phase | Executor / thread | Waiting behavior |
| --- | --- | --- |
| File-aware resource estimate | Shared async executor when enabled; planning caller when disabled | Envelope reads acquire async admission; the outer planning caller uses `blockingWait` |
| Sealed-load dispatch | Existing synchronous cache-load caller | One outer `blockingWait` schedules the complete memory-load coroutine |
| Open object, obtain size, parse envelope and slice metadata | Shared async executor | `OpenInputFile` / ChunkManager `Size` can still block this worker; this migration does not make object opening asynchronous |
| Wait for slice admission | Shared async executor | Coroutine suspends without occupying a worker |
| Native `ReadAtAsyncInto` | Storage backend owns I/O and completion | Coroutine suspends and resumes on the shared async executor |
| Buffered Arrow `ReadAsync` fallback | Arrow I/O context / backend; its completion callback copies the returned buffer | Coroutine resumes on the shared async executor after that copy |
| ChunkManager-only or rooted-local source read | Shared async executor | Existing synchronous read; no submission to HIGH/LOW |
| Decode/decrypt a legacy envelope; copy into its final BinarySet entry | Shared async executor | CPU work runs directly; consumer completes before its slice lease is released |
| Restore vector nullable/empty-list sidecars and call `LoadWithoutAssemble` | Shared async executor | Synchronous CPU phase, with no slice admission held |
| Call Knowhere `Deserialize` | Same shared async worker | One synchronous call; it occupies this worker until Knowhere returns |
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
executor. A local-file executor token does not span remote I/O. Cross-executor
operations are awaited; an async worker never calls a child's blocking load
wrapper. Knowhere mmap/disk executor routing is a separate step.

## Slice ownership and memory estimates

`MemFileManagerImpl::LoadIndexBinarySetAsync` inspects persisted envelopes,
validates slice counts and aggregate lengths, allocates each final destination,
then streams slices directly into it. It reuses `LegacyIndexLoader` rather than
retaining a map of every decoded slice. Unsliced entries remain separate entries;
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
or a shared overhead group. `SegmentLoadInfo` defers memory-vector estimates
until a file context is available. The file-aware estimate covers both rollout
modes because the switch can change before a subsequent cache load. Steps 1–3
process one slice at a time within a load; different loads can progress
concurrently. Step 5 will add bounded intra-load concurrency.

## Cancellation and failures

Cancellation before admission prevents new reads. An issued async read drains
before releasing its destination or lease. Cancellation between materialization
and finalization skips Knowhere. During the synchronous finalizer, cancellation
is reported after it returns: the BinarySet and index remain alive throughout.

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

The shared legacy-loader suite covers raw/Parquet/encrypted decoding, malformed
envelopes, short reads, decoder failures and draining reads on cancellation.
Throughput benchmarks and remote-cluster tests are not part of this verification.

On 2026-09-09 the GCC 12 Release `all_tests` target rebuilt successfully. All
373 selected tests passed, including the 11 new vector-loading cases, the
existing vector load/query cases, scalar streaming, storage codecs, shared
async infrastructure, admission, and segment-resource tests. The new cases
exercise sparse engine versions 6 and 8 and MUVERA sidecars at version 11.
