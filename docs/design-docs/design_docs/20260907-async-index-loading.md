# MEP: Async Index Loading

- **Created:** 2026-09-07
- **Author(s):** @sparknack
- **Status:** Under Review
- **Component:** QueryNode, Storage, Index
- **Related Issues:** [#51245](https://github.com/milvus-io/milvus/issues/51245)
- **Implementation:** [#52453](https://github.com/milvus-io/milvus/pull/52453)

## Summary

Extend the shared async field-data loading infrastructure to packed scalar V3,
legacy scalar, Knowhere, independent TextMatch, BSON shared-key indexes, and JSON
stats metadata. Loads overlap admitted reads, place slices directly into their
destinations, and restore query state after the inputs are complete.

## Motivation and Scope

Blocking downloads occupy load workers while waiting for storage. Reading every
slice before assembling an index also retains unnecessary temporary buffers.
Coroutine-based reads allow more requests to make progress with fewer workers;
shared admission and a bounded per-load window control temporary memory.

This design covers loading existing index artifacts and planning JSON stats.
Index building, uploads, on-disk formats, and query semantics remain compatible.
The existing [field-data pipeline](20260811-async-storage-v3-field-data-loading.md)
continues to load field data; index loading reuses its executor and controllers.
Knowhere owns its internal deserialization parallelism and backend-specific lazy
reads. Those reads are outside the admission boundary described here.

## Public Interfaces and Selection

`queryNode.segcore.storageV2.enableAsyncLoad` is the process-wide switch and
is false by default. Context-aware load entries read
`StorageV2AsyncLoadEnabled()` at entry and propagate `OpContext` cancellation.

| Setting / entry | Behavior |
| --- | --- |
| Switch enabled | Covered loads use the shared async executor, including encrypted and generic-reader inputs. HIGH/LOW select its priorities. |
| Switch disabled | Existing synchronous payload loaders retain their scheduling, including HIGH/LOW pools. |
| Explicit compatibility overloads | Scalar/vector two-argument and BSON three-argument loaders remain synchronous. Production translators use context-aware entries. |
| `queryNode.segcore.storageV2.asyncLoadThreadPoolSize` | Resizes the shared CPU executor; default is `max(1, min(CPU_NUM, 16))`. |
| Shared admission bytes and slots | Refreshable process-wide limits, shared with synchronous loads and field data. Zero disables the respective limit. |

Without explicit overrides, admission uses 2 GiB and twice the initialized CPU
count while async loading is enabled; absent limits resolve to zero when it is
disabled. Configuration updates do not move an already started load to another
executor. A later cache reload can select a different path, so resource estimates
must cover both settings.

## Design Details

### Shared components and executors

![Shared loading components and executor boundaries](../assets/graphs/async-index-loading/overview.svg)

`storage::AsyncLoadExecutor` provides one CPU pool. `LoadAdmissionController::GetInstance()`
provides the shared bytes/slots budget. `AsyncIndexEntryReader` and
`IndexMaterializer` implement packed entry loading; `LegacyIndexLoader` implements
legacy object decoding and streaming. They live in `storage/` and are independent
of `segcore/storagev2translator/AsyncLoadPipeline`.

| Phase | Executor / owner |
| --- | --- |
| Synchronous cache-load boundary | Existing caller schedules the coroutine and uses `blockingWait`. |
| Catalog/envelope parsing, planning, admission, slice dispatch, decode, CRC and memory placement | Shared async executor. Waiting for admission or native I/O suspends the coroutine. |
| Native range reads and completion | Storage backend. Processing resumes on the async executor. |
| Generic Arrow `ReadAsync` | Arrow/backend I/O context; completion copies the returned buffer before resuming the caller. |
| Provider opens, generic size queries, non-Arrow reads | May block the invoking async worker. They do not fall back to HIGH/LOW. |
| Directory creation, writable-target preparation, positioned writes, flush/close and load-time file cleanup | `LocalFileIOPool`, awaited from the async coroutine. |
| Engine opening, `Deserialize`, `DeserializeFromFile`, read-only mapping and query-state restoration | Shared async worker. Synchronous engine work occupies it until the engine returns. |
| TextMatch analyzer registration and cache publication | Cache-load caller after successful loading and cancellation checks. |

Children use `co_await`, including when they use the same executor as their
parent. There is no nested blocking wait on an async worker. A local-file executor
token covers only its immediate file operation, never remote I/O or engine
restoration. If that pool is disabled, its existing resolver uses the async
executor. Normal index destruction and cache eviction retain their existing
lifetimes.

File-aware legacy estimation reuses admitted inspection in both switch settings:
enabled inspection is scheduled on the async executor; disabled inspection runs
on the planning caller. Sharing that coroutine does not merge the two payload
loaders. Immutable envelope snapshots contain sizes/encoding, not open readers,
and pass through `FileManagerContext` for exact-path reuse during loads/reloads.

### Slice concurrency and positioned writes

Packed and legacy loading each keep at most **eight outstanding units and
128 MiB of estimated transient memory per load**. Every unit also needs a global
bytes/slot lease. An indivisible encoded unit above the byte window runs alone;
the same oversized-unit exception applies to global byte admission.

| Input | Unit and destination |
| --- | --- |
| Packed V3 | Catalog-defined entry slices; entries are interleaved in round-robin order. |
| Raw legacy payload | Ranges of at most **16 MiB**, flattened across the objects of one logical entry/file. |
| Encoded/encrypted legacy payload | A persisted object may need to be decoded as one whole unit. |
| Memory consumer | Copy into its preallocated entry at the supplied logical offset. |
| Legacy disk or Knowhere mmap consumer | Await `PositionedFileWriter::WriteAt(offset, ...)` on `LocalFileIOPool`. |

The window refills when **any** unit completes. A slow first slice does not block
refill or later disjoint writes. Legacy entry preparation and file traversal
remain sequential; concurrency is within the current entry/file. Small unsliced
entries use the single-unit path.

A unit owns its read/decode buffers and admission through the awaited consumer.
After those buffers are destroyed, it releases the lease and publishes completion.
There is no additional queue of copied slice results. Synchronous engine
restoration starts after all its input units drain and their leases are released.

The 16 MiB limit is a raw read size, not the admission charge. Raw legacy units
reserve the read buffer plus a 4 KiB-rounded aligned write copy; the 128 MiB
window may therefore admit fewer than eight units. `WriteAt` copies unaligned
addresses into aligned allocations, pads only the final file tail, and `Finish`
truncates to the exact logical length after writes drain. Slice-size assertions
enforce positive, 4 KiB-aligned production slices and a nonzero dispatch window.
`WriteAt` and `Finish` share existing global write permits with `FileWriter`;
those permits cover blocking local I/O only.

### Packed scalar V3

A packed object contains a directory of entries, and each entry describes its
slices. `AsyncIndexEntryReader` reads the directory under admission;
`PlanLoad` selects heap or file destinations without introducing another index
format. `IndexMaterializer` validates sizes/overlap, reads into those destinations,
and combines per-slice CRCs in logical order.

File targets are preallocated before writable mapping. Mappings survive issued
reads and are closed before engine restoration. `FinalizeLoad` borrows the complete
artifact and reuses the index's existing parsers. Successful finalization commits
retained files; failure releases the artifact and removes temporary targets.

For numeric Sort, persisted `index_data` and `idx_to_offsets` can become read-only
mappings, while validity remains in heap. Older artifacts can rebuild auxiliary
state; newly built Sort indexes at scalar engine version >= 3 always persist
these auxiliaries. Bitmap's
frozen-file conversion restores state on the async worker and awaits bounded
64 KiB output batches, with at most one additional large bitmap per batch.

![Packed scalar V3 architecture](../assets/graphs/async-index-loading/packed-architecture.svg)

![Packed scalar V3 mmap sequence](../assets/graphs/async-index-loading/packed-sequence.svg)

### Legacy scalar

Legacy `A_0`, `A_1`, and `SLICE_META` are separate objects. The file manager validates
slice counts and aggregate lengths, prepares the logical entry `A` once, and
streams decoded ranges into their final offsets. It does not retain a second map
of every decoded slice.

Sort, StringSort, Bitmap, Marisa and Hybrid consume the assembled BinarySet;
Hybrid awaits its selected child's coroutine. Tantivy/Ngram and RTree share disk
staging and restore their engines after file closure. JSON scalar wrappers reuse
the base loader and restore missing/null sidecars. Numeric Sort's legacy mmap
path first assembles its input, then writes its file and restores mappings and
auxiliary state; it is distinct from V3 direct-to-target materialization.

![Legacy scalar Sort architecture](../assets/graphs/async-index-loading/legacy-architecture.svg)

![Legacy scalar Sort mmap sequence](../assets/graphs/async-index-loading/legacy-sequence.svg)

### Knowhere

All three Milvus loading modes reuse the legacy transport and existing sidecar
parsers. Knowhere deserialization remains a synchronous engine call; its own
implementation determines any internal parallelism.

| Mode | Inputs prepared by Milvus | Engine phase on async worker |
| --- | --- | --- |
| Memory | Complete BinarySet plus nullable/empty-list metadata | `LoadWithoutAssemble` / `Deserialize` |
| mmap | Positioned main-index file, separate embedding metadata/raw-index files, heap validity sidecars | `FinalizeMmapLoad` / `DeserializeFromFile` |
| Disk | Files selected by the existing disk-load policy | `FinalizeDiskLoad` / `Deserialize` |

Mmap learns each entry's exact size before opening its writer. One main entry
maps to one file; embedding metadata and optional raw-index entries each have
their own file. A second entry for one writer is rejected. This keeps entry-relative
offsets file-relative, including tails. Configured cache targets are cleared even
for all-null reloads.

`LoadIndexWithStream()` keeps its file-selection contract: Milvus stages nullable
and empty-list sidecars while a stream-capable engine owns other reads. Such
backend-internal reads are not automatically admitted by `LegacyIndexLoader`.
Staging every object would defeat lazy loading. Blocking local or remote reads
inside an engine still occupy its async worker, and cancellation waits for the
engine call to return.

![Knowhere input and engine architecture](../assets/graphs/async-index-loading/knowhere-architecture.svg)

![Knowhere mmap sequence](../assets/graphs/async-index-loading/knowhere-sequence.svg)

### Independent TextMatch

`TextMatchIndexTranslator` forwards `OpContext`. A single `.v3` text-log object uses
the packed materializer; legacy relative paths are resolved against
`stats_base_path` and use the inherited Tantivy legacy loader. Both reuse Tantivy
planning/restoration and sealed validity, including null-offset sidecars.

The async worker opens Tantivy after staging finishes. Heap loading awaits
staging cleanup; mmap retains files under index ownership. The cache caller
registers the analyzer after `Load` returns. No TextMatch-specific streamer or
executor is introduced.

![TextMatch format selection and shared restoration](../assets/graphs/async-index-loading/textmatch-architecture.svg)

![TextMatch legacy heap sequence](../assets/graphs/async-index-loading/textmatch-sequence.svg)

### BSON shared-key

`BsonInvertedIndexTranslator` uses the context-aware `BsonInvertedIndex::LoadIndex`.
The `shared_key_index/*` artifacts, including `meta.json_0`, are legacy envelopes;
this entry does **not** use packed scalar V3. `DiskFileManagerImpl` stages them
with the shared positioned-write consumer in a directory owned by this load.

After all files close, `FinishLegacyLoad` opens Tantivy on the async worker.
Heap mode removes staging before returning; mmap retains it. A failed load
releases its reader before cleaning its own directory. It does not materialize
all staged files into a BinarySet.

![BSON shared-key architecture](../assets/graphs/async-index-loading/bson-architecture.svg)

![BSON shared-key heap sequence](../assets/graphs/async-index-loading/bson-sequence.svg)

### JSON stats

JSON stats separates metadata planning from later column loading. Its root
`meta.json` is raw JSON, distinct from BSON's legacy `meta.json_0`.

| Phase | Admission and retained result |
| --- | --- |
| Read root `meta.json` | One lease for `32 * file_bytes + 4096` bytes and one slot spans bounded sequential reads and parsing; retain the key/field map. |
| Probe a Parquet file | 4096 bytes and one slot cover size discovery and the eight-byte trailer; release before footer admission. |
| Open the footer | `32 * (serialized_footer_bytes + 8) + 64 KiB` and one slot cover reads, schema conversion and layout restoration. The extra 64 KiB covers Arrow's tail-read fallback. |
| Collect a group | At most 16 file tasks at once, preserving file-ID order; retain one schema per group and a row count per file. Groups are planned sequentially. |
| Prepare projected readers | All metadata leases have ended. Existing `OpenChunkReadersAsync` prepares readers for column caches. |

These parser multipliers are conservative estimates, not measured allocator
bounds. Metadata is read and parsed on the async path without local staging.
Missing root metadata uses the existing first-Parquet-schema layout fallback.
Only the group's first-file task restores shared field mappings.

Eager column setup projects all fields in a group; lazy setup creates per-column
projections. Each projected reader may open its own group's underlying files;
the 16-file metadata-planning window does not bound that reader's internal opens.
Preparing readers does not materialize cold columns. Warmup/cache loading still
uses `ManifestGroupTranslator` and the field-data async pipeline. BSON slots are
created separately after column preparation.

![JSON stats planning and cache architecture](../assets/graphs/async-index-loading/json-architecture.svg)

![JSON stats metadata and reader preparation sequence](../assets/graphs/async-index-loading/json-sequence.svg)

### Resource accounting

Admission limits transient work, while cache reservations account for final
indexes and data retained beyond a slice. FIFO applies within HIGH/LOW priorities;
HIGH waiters precede queued LOW waiters. Waiting requests hold neither bytes nor
slots. An oversized unit may run exclusively, so a configured byte limit cannot
be treated as a hard cap below that unit's requirement.

| Memory / disk lifetime | Accounting |
| --- | --- |
| Read/decode/write scratch, including aligned copies | Slice admission plus the fixed per-load window |
| Complete BinarySet, retained sidecars, parsed slice metadata, Bitmap conversion input/output | Request-local peak reservation |
| Final index and retained mappings/files | Existing representation estimates and cache ownership |
| Overhead fully covered by runtime leases in both load modes | Shared group from `LoadMemoryOverheadController::GetInstance().GetOrCreate()` |

Knowhere memory estimates include the overlap between the assembled BinarySet
and the constructed index. Mmap estimates include heap sidecars, embedding
metadata, decode scratch and compatibility writer buffers. BSON retains the
larger supplied JSON-stats estimate when appropriate. Disk loading keeps a coarse
Knowhere/download allowance; it does not prove a bound on oversized encoded disk
objects or backend-internal scratch.

For a largest legacy unit charge `s`, the bounded read peak is
`max(s, min(128 MiB, 8 * s))`, calculated with saturating arithmetic. Estimates
cover both rollout paths and do not shrink with CPU worker count or current
admission settings. Suspended reads keep their slots after releasing workers.

Shared accounting follows byte/slot admission limits: a nonzero byte budget uses
the existing Budget policy; otherwise finite slots multiply the largest bound
unit. Unlimited slots use Passthrough rather than assuming a worker-count bound.
Limit expansion updates accounting before admitting more work; rejection keeps
the old limit. Tightening restricts admission first and retains accounting for
already admitted slots until they drain. Executor resizing does not resize these
resource reservations.

### Cancellation and failure

![Cancellation drains issued work before cleanup](../assets/graphs/async-index-loading/cancellation.svg)

The first failure stops new work and cancels sibling admission. Issued reads and
awaited writes drain before their buffers, mappings or writers can be released.
Cancellation is checked around synchronous parsers/engine calls and between
Bitmap output batches; it cannot interrupt an engine call in progress.

Packed artifacts commit only after successful finalization. Legacy file loads
close their writers before opening engines and remove failed staging after
issued work drains. Cleanup is awaited with cancellation disabled where needed,
and the original load failure is retained. No partial result is published.

Short reads, malformed metadata, CRC mismatches, invalid destination plans and
file-write failures terminate loading. Existing typed storage statuses are kept
where the backend supplies them; this design does not recover categories already
erased by a backend or replace Tantivy's string-based error contract. Backend-owned
retry behavior and actual S3 retries must be checked at the network boundary.

## Compatibility, Deprecation, and Migration Plan

The switch supports gradual rollout and rollback without rebuilding indexes.
Packed readers share format parsing with synchronous readers. Legacy assembly
preserves slice metadata and file naming; Sort can reconstruct missing auxiliary
state, Marisa reconstructs absent CSR but rejects partial CSR, and Hybrid retains
its persisted-type/filename/metadata recovery rule.

Both paths preserve null/missing-value handling and memory/mmap query results.
Independent TextMatch supports packed and legacy artifacts; BSON shared-key remains
legacy. HIGH/LOW pools continue serving compatibility paths. This change adds no
new public index format, per-index async switch, or controller singleton.

## Test Plan

The implementation keeps deterministic C++ checks at resource/executor boundaries
and Go integration tests for actual loading and object-store traffic. The matrix
specifies coverage; it is not a record of which revision ran each suite.

| Layer | Scenarios and required assertions |
| --- | --- |
| Reader/materializer C++ UT | Plain/encrypted input, metadata admission, short/corrupt reads, CRC and destination validation; incomplete targets are never committed. |
| Streaming/writer C++ UT | Out-of-order reads/writes, slow first slice, byte/slot windows, aligned tails, exact file length and write failure; all buffers/permits return after drain. |
| Executor/lifetime C++ UT | One async worker and one slot, priority and disabled-local-pool routing, engine restoration, cancellation and pool shutdown; no nested worker wait or early input cleanup. |
| Scalar/vector/TextMatch/BSON C++ UT | Legacy/V3 where supported, memory/mmap, nullable and missing sidecars, reloads, query parity, finalizer failure and generated-directory isolation. |
| JSON stats C++ UT | Raw metadata, absent metadata fallback, Parquet probe/footer admission, ordered rows/schema, cancelled size/footer requests and lazy-column reads. |
| Accounting C++ UT | Rollout/worker changes, live budget/slot shrink and expansion, oversized/unlimited units, retained BinarySet/sidecars and rejected reservation expansion. |
| Go loading integration | Build legacy scalar and JSON stats artifacts, load/query/release/reload with the switch off and on. |
| Go object-storage fault integration | An HTTP proxy in front of MinIO injects two `503 SlowDown` responses or pauses a selected GET; verify retry/query success and server-side release followed by reload. |

The paused-GET test exercises server-side teardown with `ReleaseCollection`, not
client RPC cancellation. Exact cancellation observation and lease lifetimes are
checked in C++ UT. Real DiskANN/backend-specific streaming, GPU/remote deployments,
ASAN, production RSS and throughput measurements remain separate validation work;
local correctness tests do not establish those results.

## Rejected Alternatives

- A separate async admission singleton would split the process budget; use the
  existing `GetInstance()` controller and overhead group.
- Routing enabled loads back to HIGH/LOW based on index format or reader capability
  would make the global switch inconsistent; generic reads stay on the async path.
- Running engine deserialization on `LocalFileIOPool` would occupy file workers
  with CPU work and synchronous engine reads; only staging operations use it.
- Waiting for preceding file slices would serialize consumers and delay window
  refill; disjoint positioned writes allow completion in any order.
- Rewriting every index parser or eagerly caching backend-owned lazy files would
  duplicate semantics or change loading behavior; reuse the existing contracts.

## References

- [Shared async field-data design](20260811-async-storage-v3-field-data-loading.md)
- [Implementation PR](https://github.com/milvus-io/milvus/pull/52453)
- Shared transport: `storage/AsyncIndexEntryReader`, `IndexMaterializer`,
  `LegacyIndexLoader`, `MemFileManagerImpl`, `DiskFileManagerImpl`, `FileWriter`.
- Index integration: `index/ScalarIndexAsync.cpp`, `VectorMemIndex.cpp`,
  `VectorDiskIndex.cpp`, `TextMatchIndex.cpp`, `json_stats/bson_inverted.cpp`,
  `json_stats/JsonKeyStats.cpp` (paths relative to `internal/core/src/`).
