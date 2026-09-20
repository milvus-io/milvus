# Index loader lifecycle and asynchronous loading

## Scope

Scalar V3 keeps its packed `IndexEntryReader` / `IndexEntryWriter` path.
Legacy scalar V1/V2 and vector artifacts retain their existing FileSource /
FileSink formats. The pinned `use_async_load` setting selects the transport for
an entire cache cell, including metadata inspection and resource estimation.
This change does not add an asynchronous writer or change the persisted format.
BSON/JSON stats and the legacy TextMatch translator are outside this rollout.

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

`LegacyIndexLoader` validates each immutable physical object's envelope. Raw
payloads stream in bounded ranges. Parquet and encrypted envelopes retain one
complete decoding unit because their existing decoders require it. Each issued
range/unit acquires global admission asynchronously and holds the lease through
read, decoding and consumption. FileSource assembles physical slices in their
existing order into logical entries, memory targets or positioned file targets.

Remote reads and admission waits suspend. Local size/read/write calls run on
LocalFileIOPool. The family coroutine also runs there so local staging, mmap,
native initialization and failure cleanup retain their existing synchronous
semantics without blocking the remote read executor. When the local pool is
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

`IndexLoad.cpp` implements the complete load operation: open the storage,
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

The current translator opens and loads within each cache-cell request. The
internal loader owns its source and does not retain the creation operation
context or cancellation token after Create completes. Each Load binds its own
context and owns its targets; sequential repeated loads are supported, but
concurrent loads on the same loader are not. Borrowed-source adapters require
the caller to keep the source alive.

The synchronous bridge blocks once and runs both phases on the calling thread.
Using coroutine return types does not itself add an executor hop. Scalar V3
continues to use IndexEntryReader directly, without a FileSource adapter.

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

The tests exercise injected native asynchronous reads over real serialized
legacy objects. They do not establish real-S3 behavior, complete DISKANN native
deserialization, or end-to-end Go retry behavior. The disk test checks backend
selection and resource inspection; it is not a full disk-index load test.
