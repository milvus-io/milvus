# Index loader lifecycle and asynchronous loading

## Scope

Scalar V3 keeps its packed `IndexEntryReader` / `IndexEntryWriter` path.
Legacy scalar V1/V2 and vector artifacts retain their existing FileSource /
FileSink formats. The pinned `use_async_load` setting selects the transport for
an entire cache cell, including metadata inspection and resource estimation.
This change does not add an asynchronous writer or change the persisted format.
BSON shared-key indexes use the legacy FileSource transport with the same pinned
mode, file-aware estimates, request cancellation and priority. Their complete
remote keys are preserved, and each load owns a private DiskFileManager directory
until its mmap reader is released (RAM loads remove it after opening Tantivy).
Legacy TextMatch uses the same loader entry and pinned mode as packed TextMatch.
JSON stats preparation also follows the pinned mode for `meta.json`, Parquet
footer inspection and projected-reader opening; see the
[field-loading design](design_docs/20260811-async-storage-v3-field-data-loading.md#json-stats-preparation).
Old storage V1/V2 raw-field loading and native Knowhere internal reads remain
outside this rollout.

Translator construction borrows the loading request's `OpContext` for metadata
inspection and file-aware estimates, including cancellation and runtime priority.
Already-issued I/O drains before cancellation is reported; synchronous I/O cannot
be interrupted while in flight. Construction retains no request context, and each
subsequent cache-cell load binds its own context.

## Read path

`LoaderEntry::Load` opens the logical legacy directory with
`V1RemoteSource::OpenAsync`, including asynchronous slice-metadata loading.
HYBRID's selector is read asynchronously before choosing the concrete loader.
The load pipeline invokes `LoaderEntry::create` with an `OpenedIndexSource` and
fixed options. `Create(source, options)` retains the source and parsed metadata
in an internal loader; `Load(ctx)` creates the reader and its payload targets.
Both synchronous and asynchronous transport use this lifecycle. Bitmap and
sorted metadata are parsed once during Create; payload allocation and file
staging remain in Load.
There is no intermediate V3 file or preload-all adapter.

`storage/LegacyIndexFileIO` provides functions to validate each immutable
physical object's envelope. Raw
payloads stream in bounded ranges. Parquet and encrypted envelopes retain one
complete decoding unit because their existing decoders require it. Each issued
payload range/unit acquires global admission asynchronously and holds the lease
through read, decoding and consumption. FileSource assembles physical slices in their
existing order into logical entries, memory targets or positioned file targets.

Synchronous unsliced entry reads validate the envelope in the downloaded buffer
and allocate the result from its decoded size, without separate remote probes.
Explicit size queries and multi-slice entries without a known total still inspect
envelopes before allocating. Async open/size and envelope-inspection phases
reserve one process-wide load slot each, without charging small metadata bytes. Descriptors up to 64 KiB
remain outside byte accounting in both modes. Synchronous metadata reads retain
their existing scheduling. Larger descriptors reserve their estimated
wire/parse scratch through the existing admission controller before allocation,
and hold that lease through inspection, including failure draining. Async
inspection releases its ordinary slot before acquiring descriptor bytes and a
slot together, so a one-slot limit permits progress.
This threshold changes resource handling, not the accepted storage format.
Async inspection retains its per-batch window and drains issued I/O on failure.
All batches now also share `LoadAdmissionController` slots with payload loading;
`common.loadAdmissionSlots=0` retains unlimited slot admission. Packed V3 stream
opening and directory inspection participate too; directory inspection releases
its slot before `__meta__` loading acquires its existing slice reservations.
No separate metadata controller or byte budget is introduced. Slot waits preserve
request priority and cancellation; issued I/O drains before returning its slot.

Synchronous file materialization decodes bounded batches directly into the target
staging file, including concatenated entries and directory outputs. It does not
keep an intermediate disk copy of each entry; staging publication and rollback
retain the existing target ownership rules. The staging writer is `FileWriter`,
so load priority, configured buffered/direct mode and global write permits also
apply to these synchronous writes.

Remote reads and admission waits suspend. Legacy loader orchestration runs on
the shared async-load executor; families dispatch blocking local size/read/write
and native initialization phases to LocalFileIOPool. When the local pool is
disabled, the configured async executor remains the existing fallback.
The cache's synchronous interface waits once at the coroutine boundary.

## Module boundaries

`IndexLoader` exposes only `Load(ctx)`. It does not expose storage format,
entry directories, metadata accessors or an opening-finalization hook.

`IndexLoadInput` describes unopened paths and opened sources. Packed sources own
an EntryReader and expose its directory and metadata; legacy sources own a
FileSource and a transport choice. Vector loaders retain only a legacy source,
and FM loaders retain only a packed source. Other families retain the explicit
source variant they support.

`LoaderEntry.cpp` implements the complete load operation: open the storage,
construct the concrete family's metadata state, detach the source context on
both success and failure, then materialize the reader. `OpenIndexSource`,
`CreateIndexLoader` and `LoadIndexAsync` are private implementation steps.
`LoaderEntry::Load` is the single format-independent entry point used by the
cache boundary.

`PackedIndexLoad` and `LegacyIndexLoad` execute format-specific loading and
cleanup with per-call context. Concrete family loaders own their fixed options
and parsed metadata, allocate their own reader targets and initialize readers.
The registry only selects the factory and capability inspector.

## Loader lifetime

For legacy async loading, SealedIndexTranslator shares one source across family
selection, resource estimation and the first cache-cell load. It detaches the
construction context after inspection and consumes the source at the start of
the first load attempt, including cancellation or failure. Later loads open a
fresh source. This retains directory/envelope metadata, not payload buffers;
when the effective warmup policy is disabled, inspection releases the prepared
source before construction returns and the first demand load reopens it. Warmup
loads retain the source until their first attempt; its memory is included in the
load's transient estimate, without a separate idle resident cache charge.
Packed and synchronous loads still open within each cache-cell request.

The internal loader owns its source and does not retain the creation operation
context or cancellation token after Create completes. Each Load binds its own
context and owns its targets; sequential repeated loads are supported, but
concurrent loads on the same loader are not. Borrowed-source adapters require
the caller to keep the source alive.

The synchronous bridge blocks once and runs both phases on the calling thread.
Using coroutine return types does not itself add an executor hop. Scalar V3
continues to use IndexEntryReader directly, without a FileSource adapter.

## BSON shared-key loading

The JSON stats BSON index retains its own `BsonInvertedIndexTranslator`; it is
not a registered scalar reader family. Its translator opens a legacy `FileSource`
with disk-file slice grouping and passes that source to `BsonInvertedIndex`.
Both transport modes materialize files directly into the loading manager's
private generation directory. Async remote reads and admission use the shared
legacy pipeline. Orchestration runs on the shared async-load executor; manager
creation, Tantivy opening, RAM-mode directory removal and failed-load cleanup
are separate local-file phases. Remote waits do not retain the local executor,
so disabling that pool need not wait for remote I/O. Sync loading stays on the
calling thread; a disabled local-file pool uses the existing async executor
fallback.

The translator pins the mode at construction. Async admission uses the existing
Tantivy file-aware estimate, including envelope/decode scratch and staging disk.
The inspected source is detached from the construction context and consumed on
the first load attempt, even when that attempt is cancelled. Reloads reopen it
with the new request's cancellation and runtime priority. When effective warmup
is disabled, the constructor releases the source and demand loading reopens it.
JSON stats preparation forwards its request context to BSON construction.
The context is optional for direct callers; cache-cell loads bind their own
request context independently.

Failed loads drain issued I/O before the index and its private directory are
destroyed. A loaded mmap index retains that directory until destruction; a RAM
index removes it after Tantivy has copied the files. No persisted keys or formats
change.

## Legacy TextMatch loading

`TextMatchIndexTranslator` passes legacy text files through `LoaderEntry::Load`
with the TextLog namespace and disk-file grouping. The construction request's
context covers metadata inspection and file-aware Tantivy resource estimation.
Async preparation retains one detached source for the first load attempt;
cancellation consumes it too, and later loads reopen with their own context.
When effective warmup is disabled, preparation releases the source before the
constructor returns and the first demand load reopens it.
The pinned mode applies to preparation, first load and reloads regardless of
subsequent changes to the global setting. Packed TextMatch retains its existing
loader, and construction now receives the same request context.

Both legacy modes stage directly into a private load directory. mmap readers
retain that directory, while RAM readers remove it after Tantivy initialization.
The async TextMatch loader dispatches local file operations, native opening and
cleanup before reader handoff to LocalFileIOPool. Engine and null-sidecar remote
waits do not retain that executor. Failure cleanup releases the engine before its
directory; a disabled local pool uses the existing async executor fallback.
The synchronous fallback estimate charges this single staging copy; async
estimates additionally account for file-specific transport/decode peaks and
nullable-row metadata. Existing text querying and null semantics are unchanged.

## JSON stats preparation

The outer `JsonStatsTranslator` pins the mode at construction and supplies it to
`JsonKeyStats`, projected column translators and the BSON translator. Preparation
borrows the request's cancellation token and runtime priority; it retains neither
after returning. Direct `JsonKeyStats::Load` callers pin the supplied context mode
or the current global setting on their first load. A missing filesystem in their
`FileManagerContext` falls back to the default filesystem, as before.

`meta.json` is read directly into its parsing buffer, without a local staging
copy. Async mode uses the remote stream API; sync mode retains ChunkManager
reads. Parquet schema and row counts share one footer read per file during preparation.
When no typed-key map is available, the compatibility path additionally reads
the first raw footer for legacy layout metadata, using the same transport mode.
This preserves KV entries omitted from Arrow's stored schema and does not
eliminate footer reads needed by subsequent projected-reader opening. The preparation batch limits async
footer opens to 16. Each async `meta.json` read, footer inspection, legacy-layout
footer read and projected-reader open also reserves one shared load slot without
charging footer bytes. Preparation releases its slots before nested BSON or
column-data loading. Issued opens drain before returning failure or cancellation.
A projected-reader open can still fan out to multiple files inside storage; the
shared slot limit bounds unfinished preparation operations, not individual
network requests or total footer memory. The detailed flow and remaining
synchronous interfaces are documented in the field-loading design linked above.

## Vector backends

Memory vector loading asynchronously fills BinarySet entries or the existing
combined mmap file. Embedding-list metadata/raw files remain separate. Validity
is restored before native deserialization; all-null and empty embedding-list
artifacts retain their metadata-only handling.

Disk vector loading probes `LoadIndexWithStream` before preparing engine files.
A native stream backend prepares only validity/empty-list sidecars; neither the
resource inspector nor FileSource eagerly inspects its engine objects. Other
backends stream engine files into their existing local directory. Native
Knowhere deserialization and its internal remote reads remain synchronous;
this change does not make Knowhere's internals coroutine-based.

## Lifetime and failure

Input buffers, targets and admission leases outlive every issued operation.
Cancellation stops new work but drains issued reads/writes before freeing their
storage. The load pipeline merges the operation token with coroutine
cancellation.
The loader checks cancellation again before returning a reader.

File output uses same-directory staging and the existing multi-file publication
rollback. Cancellation is checked inside the queued publication task. Successful
file preparation is still owned by the loading generation; initialization
failure destroys native readers before their backing directories.

Unaligned concatenation explicitly requests BUFFERED positioned writes, with
no padding between legacy payloads. Other writes retain the existing priority
policy, including HIGH using BUFFERED. Error carriers are preserved across
executor hops; corrupt envelopes produce DataFormatBroken and admission
cancellation produces FollyCancel. No Go error mapping or retry policy changes
are part of this implementation.

## Resources and validation

Legacy async estimates inspect decoded envelope sizes and count read/decode
scratch independently of refreshable admission limits. Family-specific resident
state includes bitmap expansion, rebuilt Marisa CSR and retained null-offset
sidecars. Native disk-stream engine files remain covered by Knowhere's existing
resource estimate. Request-owned buffers are not assigned to shared scratch
reservations.

SegmentLoadInfo uses generic estimates to derive raw-data capabilities only.
It leaves the load resource override unset so the translator's file-aware
estimate reaches cache admission. Explicit caller-provided overrides retain
their existing precedence.

Regression tests cover raw/Parquet/encrypted envelopes, exact slice assembly,
real legacy scalar/vector loading, mmap, NULL and empty-list state, typed I/O
and corruption errors, admission/read cancellation and local executor placement.
Local validation on 2026-09-22: `index_tests` passed all 12,711 tests from
53 suites. The two-stage regression cases cover metadata reuse, fresh operation
contexts, cancellation followed by another Load, deferred mmap targets and
readers outliving their loader. Failed-Open tests cancel the former opening
context and directly reuse the borrowed source, verifying exception-path
context cleanup for legacy and packed input. Registry coverage verifies that the synchronous
bridge runs Open and Load on the calling thread. Existing cancellation tests
also cover cancellation while legacy initialization waits for its executor.
Changed C++ files passed clang-format 15; the segcore error-boundary guard and
`git diff --check` passed. The build used at most 16 outer jobs and one job per
nested dependency builder.

BSON and legacy TextMatch regressions cover pinned transport modes, cancellation,
reloads and reader ownership. JSON stats preparation regressions cover both
transport modes with and without `meta.json`, legacy footer KV outside the stored
Arrow schema, missing-filesystem fallback, corruption followed by retry, and
draining issued metadata I/O on cancellation.

The tests exercise injected native asynchronous reads over real serialized
legacy objects. They do not establish real-S3 behavior, complete DISKANN native
deserialization, or end-to-end Go retry behavior. The disk test checks backend
selection and resource inspection; it is not a full disk-index load test.
