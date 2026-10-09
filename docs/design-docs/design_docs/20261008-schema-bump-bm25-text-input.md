# Schema-bump BM25 and MinHash materialization from TEXT LOB inputs

Status: Implementation in progress on 2026-10-08. Includes MinHash TEXT support
and removal of redundant TEXT gates. The validation evidence below distinguishes
executed tests from pending and environment-blocked checks.

## 1. Goal and invariants

Backfill missing BM25 sparse and MinHash binary-vector outputs from historical
StorageV3 TEXT columns. Use existing runners and the BM25 statistics pipeline.
MinHash does not produce BM25 statistics. Do not change TEXT
persistence merely because TEXT is a function input.

- BM25 and MinHash materializers consume `*array.String`, never LOB references or readers.
- Additive reconciliation appends columns without changing existing TEXT column
  groups, reference bytes, LOB payload files or their manifest metadata.
- Same-partition-base full rewrite preserves reference bytes for surviving rows
  and reuses LOB files through the existing metadata-accounting path.
- Cross-partition-base rewriting remains the existing exception required by
  `docs/agent_guides/storage/path_contract.md`: preserve text values and source
  data, but create references valid under the destination namespace.
- Select rows once. Decoded inputs and function outputs use that selected batch's
  order. No second sequential reader, source-row cursor, join or alignment pass.
- Decode only deduplicated TEXT inputs of missing BM25/MinHash functions. Existing
  outputs, unrelated TEXT fields, and VarChar-only materialization need no new
  decoder work. Existing migration-related decoding is independent.
- LOB formats, coordinator scheduling, general LOB-rewrite policy, and the
  BM25/MinHash algorithms are outside this enhancement.

## 2. Conventions: adopted, adapted, rejected

**Adopt:** separate stored large-value references from values expanded for
computation. PostgreSQL documents both detoasting before consuming values and
preserving unchanged out-of-line fields during updates:
[TOAST](https://www.postgresql.org/docs/18/storage-toast.html).
This is strong production precedent for the principle, not for copying its
row layout or lifecycle into Milvus.

**Adopt:** immutable shared Arrow buffers with explicit ownership and release
at the language boundary:
[Arrow C data interface](https://arrow.apache.org/docs/format/CDataInterface.html).
The interface permits sharing; LOB I/O and decoding still allocate memory.

**Adapt:** one selected write record plus a temporary logical-input overlay.
This is a Milvus-specific design, not an industry-standard interface. A single
decoded reader is sufficient when only logical values are needed, but cannot
by itself provide both decoded values and unchanged references under one field ID.

**Reject:** forcing REWRITE_ALL for computation; casting encoded references to
strings; reimplementing the LOB codec in Go; introducing two scans whose batch
boundaries need synchronization; retaining plaintext for an entire segment.

## 3. Current implementation and the exact gap

| Code | Current behavior | Design consequence |
| --- | --- | --- |
| `compactor_common.go: compactionReadSchema` | Reads target-schema fields present in storage and reader-filled ordinary fields; excludes absent function outputs | TEXT inputs already participate in the existing batch and row ordering |
| `bump_schema_version_compactor.go: runFullSchemaRewrite` | Reads one batch, derives selection, materializes, writes | Keep this single-batch structure |
| `record_reader.go: newManifestReader` | Internal TEXT uses physical Binary without decode configs | Ordinary REUSE_ALL batches contain references, not plaintext |
| `bump_schema_version_compactor.go: WithTextRefsAsBinary` | Preserves encoded references at the writer | Do not replace TEXT in the write base with decoded input |
| `record_materializer.go: WrapWithSelection` | Functions and output wrapper currently share one base record | Separate logical computation input from write base |
| `record_materializer.go: stringInputsFromRecord` | Accepts String, rejects Binary | Keep execution string-only |
| `packed/segment_reader_ffi.go: FFISegmentReader` | Exposes a decoded stream, replacing configured TEXT arrays | Not an existing arbitrary-batch decoder |
| `proxy/task.go: validateAddFunctionInputNotText` | Rejects BM25 and MinHash TEXT backfill | Remove this temporary gate after both worker routes support TEXT |
| `validator.CheckFunctionInputField` / `function.ValidateMinHashFunction` | Maintain separate, inconsistent MinHash input-type rules | Keep one schema-level compatibility rule; retain algorithm parameter checks |

The repository pins milvus-storage at `15ab3d7` in
`internal/core/thirdparty/milvus-storage/CMakeLists.txt`. Inspect that revision,
not the possibly different HEAD of a cached build checkout.

At this pinned revision, `lob_column::LobColumnReader::ReadArrowArray` already
accepts a BinaryArray of references, groups payload reads by file, and maps the
results back to the original input positions, preserving nulls. Its output is
a BinaryArray of **decoded payload bytes**. The TEXT-specific boundary must
expose those decoded bytes as a valid String array; this is not a cast of refs.

The C API exposes SegmentReader streams and Take, but not arbitrary reference-
array decoding. `SegmentReaderImpl::ResolveLobColumns` is private. The Milvus
Go package has no binding for `LobColumnReader::ReadArrowArray`.

Therefore the recommended same-batch design needs a **small internal binding**
to an existing C++ capability. It does not need a new `storage.RecordReader`,
a generic storage-read framework, a new LOB format or a milvus-storage public
API/dependency update. Existing Milvus core C shims in `storage/loon_ffi` and
`packed/packed_reader_ffi.go` establish this integration pattern.

### Alternatives considered

1. **Recommended: bind existing reference-array decoding.** One source scan,
   explicit input/output separation, and decoding after row selection. Cost:
   a narrow Milvus-internal Go/C++ boundary with ownership tests.
2. **Bind SegmentReader Take.** No new upstream C API, but rereads reference
   columns and needs physical row indices. Useful for genuinely random-access
   consumers, not necessary for this already-read batch.
3. **Use two existing sequential readers.** Avoids new FFI functions but adds
   duplicate reads, buffering and alignment invariants. Not selected.
   A decoded reader alone remains viable for additive-only work, but does not
   solve reference preservation for ordinary full rewrite.

## 4. Recommended components

### A. Internal TEXT decoder binding

Proposed files:

- `internal/core/src/storage/loon_ffi/text_lob_decoder_c.h`
- `internal/core/src/storage/loon_ffi/text_lob_decoder_c.cpp`
- Register the source in that directory's `CMakeLists.txt`.
- `internal/storagev2/packed/text_lob_decoder.go`

Proposed Go contract, intentionally per-field and independent of function type:

```go
func NewTextLOBDecoder(
    fieldID int64, lobBasePath string, storageConfig *indexpb.StorageConfig,
) (*TextLOBDecoder, error)

func (d *TextLOBDecoder) Decode(
    ctx context.Context, refs *array.Binary,
) (*array.String, error)

func (d *TextLOBDecoder) Close() error
```

Create a C++ LobColumnReader with the existing filesystem/property helpers and
source LOB path. Reuse its handle across batches; do not use it concurrently.
Decode imports the reference array synchronously, calls ReadArrowArray, and
exports logical UTF8 through the Arrow C data interface. Input refs remain
unchanged and caller-owned; returned strings are caller-owned and remain valid
after the decoder closes. Keep any exported input buffers alive until C++ has
released its import, including on failure. Close must be idempotent.

The source LOB base is derived using the same convention as
`LOBCompactionContext.GetSourceTextColumnConfigs`: source manifest partition
base plus `lobs/<fieldID>`. It must not come from the new output segment's root
or require changing `DecodeTextFromSource` or a LOB strategy. Preserve the
existing complete-key/local-root contract.

Guard empty non-null references, invalid tags/lengths and invalid offsets in
the native boundary using the library's reference constants/helpers, before
passing unsafe input to the decoder. Preserve null separately from valid empty
text. Validate decoded TEXT as UTF8 and preserve array length/null positions.
Do not implement the reference format in Go.

Keep the existing manifest reader and its encryption/plugin context intact.
The decoder reuses the current LOB filesystem/properties path; it does not
introduce new encrypted-LOB format support. Test supported encrypted-reference
configurations before claiming CMEK end-to-end compatibility.

### B. Compactor-local logical input preparation

Add `internal/datanode/compactor/function_input_view.go`.

Construct the required TEXT input set from the existing missing-function
decision; deduplicate shared input fields. Reuse VarChar and multi-analyzer
selector columns from the selected write base.

For each required TEXT input:

- String: already decoded, borrow it without I/O.
- Binary with physical source data: lazily open/use the field decoder.
- Physically absent nullable TEXT: create a same-length null String array,
  without reading LOBs; preserve its reader-filled write representation.
- Unexpected representation: return a typed internal/function-contract error.
  Do not infer reference validity by treating arbitrary bytes as text.

Build a lightweight input overlay: Column returns a decoded replacement when
present, otherwise the selected base column. It has exactly the base's row
count and order. Cleanup releases owned decoded arrays, not the borrowed base.
No selection or source-row-offset mapping occurs in this overlay.

### C. Generic materializer input/output separation

Add this string/record-oriented method to `RecordMaterializer`:

```go
func (m *RecordMaterializer) WrapWithInputs(
    writeBase storage.Record, logicalInputs storage.Record,
) (storage.Record, error)
```

It borrows both inputs, checks equal lengths, computes using logicalInputs, and
returns `materializedRecord{base: writeBase, computed: outputs}`. It neither
knows why an input was transformed nor retains the logical view after the call.

On error, release partial computed outputs only; the caller owns selected-base
cleanup. Existing Wrap/WrapWithSelection delegate with one record used as both
arguments and preserve their current external ownership contract. A successful
result's existing cleanup path releases computed arrays and derived selection
arrays, but never releases the reader-owned source record.

Remove the BM25/MinHash materializer constructors' VarChar-only gates; do not
replace them with another schema whitelist. Keep runtime inputs String-only.
Replace LOB-specific runtime error wording with the logical string-input
contract. Schema admission owns type compatibility; field/output consistency
and execution-shape checks remain in their existing boundaries.

### D. Selection preserves actual TEXT representation

`buildSelectedColumn` currently calls `storage.NewRecordBuilder`, which always
constructs a Binary builder for TEXT. Existing namespace migration can supply
String TEXT instead. Do not assume that selection preserves both forms today.

For TEXT only, select/slice/concatenate using the actual Arrow array type, so
both reference Binary and decoded String remain unchanged in representation.
Keep other type handling intact. Test nulls, non-contiguous ranges and
non-zero-offset slices. Do not change the generic storage builder globally.

## 5. End-to-end batch flow

```text
existing reader.Next()
        |
existing delete/TTL selection (full rewrite only)
        |
selected writeBase --------------------------------------+
        |                                               |
prepare logicalInputs (decode only when still refs)      |
        |                                               |
BM25 / MinHash -> computed columns ----------------------+
                                                        |
                                      attach to writeBase
                                                        |
                              existing writer / stats / publication
```

**Additive:** no delete/TTL filtering and no row-count changes. Use the existing
projected physical reader plus the same input preparation. The writer projects
only appended fields; existing TEXT inputs never get rewritten. It is not
necessary to carry original input refs into any newly persisted result.

**Same-base full rewrite:** apply selection once to the read record, decode
only needed TEXT from that selected record, and attach computed output to the
same selected write base. Writer options remain REUSE_ALL/WithTextRefsAsBinary.
An all-filtered batch skips decoding and materialization entirely.

**Existing namespace migration:** retain its decoded reader and REWRITE_ALL
writer. Already-decoded TEXT is a no-op for input preparation. New nullable
TEXT may still need logical null synthesis. Preserve that route's source-path
handling and apply representation-preserving selection.

Do not reinterpret full-schema rewrite as LOB REWRITE_ALL: they are different
decisions. Existing readRows/writtenRows, reference merge, timestamp overwrite,
statistics increments versus absolute stats, and manifest-result modes remain.

Keep the existing ordering exactly: reader.Next, delete/TTL selection, function
input preparation/materialization, timestamp overwrite, writer.Write. An
already-decoded migration reader still decodes during Next; preparation is a
pass-through for its String columns, not a second decode.

General LOB REWRITE_ALL is broader than namespace migration. Non-forced mix
compaction may choose it when the hole ratio reaches the threshold. In the
same namespace its writer consumes refs with RewriteMode=true and performs
decode/re-encode internally. Cross-namespace compatibility instead sets
DecodeTextFromSource=true and RewriteMode=false. Schema-bump currently forces
REUSE_ALL and skips hole-ratio selection, overridden only for a partition-base
mismatch. This enhancement changes none of those decisions.

## 6. Failure, lifetime and resource boundaries

- Decode/runner/write/commit errors must not produce a successful compaction
  result or mutate the source. Retain existing writer cleanup and publication.
- Translate C++ failures through existing typed status facilities; add Go
  context with merr.Wrap/Wrapf. Do not stringify away codes or claim correct
  retry classification based only on the boundary.
- Check cancellation before native decoding and before publication; do not
  claim a Go context can interrupt an already-running native I/O operation
  unless the implementation actually wires such cancellation.
- Release logical input arrays after synchronous materialization. Release
  output/selection arrays after writing. Finish all borrowed access before the
  next reader.Next/Close; retained results must obey existing Retain/Release.
- Keep plaintext per batch, not per segment. ReadArrowArray still allocates
  payload buffers; a reference-byte or row limit is not a decoded-byte bound.
  Measure large-text amplification and Arrow String offset limits. If batches
  need subdivision, use contiguous slices of the same selected batch, never
  a second stream. Bound decoder file-handle caching using its existing cache
  cleanup capability when evaluating large multi-file segments.
- Audit malformed refs, out-of-range offsets and truncated payloads in the
  pinned dependency: source inspection confirms normal ordering, not complete
  corruption detection. The pinned implementation maps Vortex results back to
  input positions without an explicit per-file result-count check. Fault tests
  are a release gate; do not turn absent payloads into empty successful function
  output. Any necessary dependency repair must be identified explicitly.
- MinHash currently copies input strings into a contiguous C buffer. Include
  that extra allocation and its per-text int32 lengths in long-TEXT resource
  verification; do not change hashing/tokenization to support TEXT.

## 7. Admission, rollout and acceptance

This enhancement removes capability-related restrictions, not all validation:

- Delete validateAddFunctionInputNotText and its Proxy call.
- Delete the schema-bump VarChar-only input gate and its calls, preserving
  field existence, partial-output and persisted-output consistency checks.
- Delete the BM25/MinHash constructor input-type gates. Materializers consume
  logical String arrays and never determine LOB support from schema types.
- CheckFunctionInputField owns the schema input-type rule: public BM25 and
  MinHash inputs accept VarChar/Text. Remove the second type whitelist from
  ValidateMinHashFunction. Preserve its parameter, dimension/overflow, and
  algorithm-specific checks, including when runtime checks are disabled.
- Preserve analyzer, selector, output type/nullability and StorageV3 gates.

Complete worker support before changing public admission. Cover create and add
function paths, Proxy and RootCoord's shared validator, fresh inserts, historical
rows, and MinHash word/char tokenization. Do not broaden support to unrelated
function types or expose deprecated DataType_String as a new public capability.

Deploy capable workers before enabling this new admission path. This proposal
does not establish a mixed-version scheduling protocol; verify the supported
upgrade boundary before claiming rolling-upgrade compatibility.

Required evidence:

1. Decoder: inline, out-of-line, mixed, null, empty, threshold boundaries,
   duplicate refs, non-monotonic offsets, sliced arrays and multiple LOB files.
   Assert actual strings, row order, null bitmap, immutable refs and releases.
2. Materializer: both runners see strings; output retains Binary refs exactly;
   one TEXT shared by BM25 and MinHash decodes once per batch; VarChar and
   multi-analyzer regressions pass.
3. Additive: real LOB input produces exact sparse vectors and BM25 stats;
   existing column groups, refs, LOB hashes and metadata remain unchanged in
   both manifest-delta and complete-manifest result modes.
4. Full rewrite: drop plus pending BM25, deletes and TTL; compare retained
   PK -> TEXT -> sparse output and ref bytes, not only row counts. Include
   all-filtered batches, unrelated TEXT, and existing sparse outputs.
5. Migration: pending BM25 with decoded input, deletes/TTL, String-preserving
   selection and successful retrieval under the new namespace.
6. API: insert/flush old TEXT rows, add BM25/sparse index or MinHash/binary
   index, await readiness, search old/new rows, release/load and verify search
   plus TEXT. Cover create-time MinHash TEXT and compare exact binary outputs
   to the existing runner oracle in compactor tests.
7. Failures: malformed/missing/truncated LOB, transient I/O, cancellation,
   runner/write/commit failures; no successful publication or source mutation.
   Apply AGENTS.md G1–G4 before claiming error/retry behavior.
8. Repeat execution uses physical completeness and avoids redundant decoding.
   Measure memory, I/O and elapsed time; report rather than assume improvements.

## 8. Implementation sequence

1. Native decoder binding and real-array ownership/ordering tests.
2. Generic materializer input separation and type-preserving TEXT selection.
3. Deduplicated BM25/MinHash input preparation with lazy decoder ownership.
4. Additive/full-rewrite integration and worker capability-gate removal.
5. Schema-validation consolidation and public BM25/MinHash TEXT admission.
6. Real-LOB preservation, failure injection and resource verification.
7. API E2E for historical/fresh rows, create/add, index/search/reload.

The companion plan is
`docs/superpowers/plans/2026-10-08-schema-bump-bm25-text-input.md`.
Both documents supersede the earlier dual-reader/cursor and BM25-only proposals.
The user authorized implementation in the current conversation. Deployment and
mixed-version admission remain outside that authorization.

## 9. Implementation and verification evidence (2026-10-08)

The Milvus core C shim now calls the pinned `LobColumnReader::ReadArrowArray`
through a read-only Go binding. It validates encoded references before calling
the pinned batch reader and exposes separately owned Arrow String values. The
compactor keeps a physical write record and borrows a temporary logical input
view only for missing BM25/MinHash outputs. In additive mode no original TEXT
column is written; in same-base full rewrite the original Binary references
remain in the write record. Existing partition-base migration still uses its
decoded reader and rewrite writer. One input preparer deduplicates TEXT field
IDs and caches one decoder handle per field across batches.

Executed and passed with `-tags dynamic,test -gcflags="all=-N -l"` and a rebuilt
native core library on macOS arm64:

- `./internal/storagev2/packed/... -run TestTextLOBDecoder`: inline/null/empty,
  unchanged references, closed-handle and canceled-context checks, malformed
  tag/length/offset rejection. The first implementation exposed an uncaught
  native exception on malformed input; expected failures now return `CStatus`.
- `./internal/datanode/compactor -run 'TestBumpUT(AdditiveBM25AndMinHashFromTextLOB|FullRewriteBM25FromTextLOB|TextLOBDecoderRejectsOutOfRangeRowWithoutChangingSource)|TestFunctionInputPreparer|TestRecordMaterializer(SelectionPreserves|WrapWithInputs)'`:
  real out-of-line LOB backfill and reference-preservation checks passed.
- `./internal/datanode/compactor -run 'Test(Bump|RecordMaterializer|RM|FunctionInputPreparer)'`:
  broader schema-bump and materializer regressions passed.
- The full `./internal/datanode/compactor` package passed (175.312s). A separate
  preparer fault-injection test confirmed transient decoder failures retain a
  retryable code. The real-LOB failure test covers both an out-of-range row and
  a reference to a missing LOB file, leaving the source unchanged.
- `./internal/util/function/... -run 'TestValidateFunction|TestValidateMinHashFunction|TestCheckFunctionInputField'`,
  `./internal/proxy -run 'TestFunctionTask|TestValidateFunctionInputField|TestAlterCollectionSchemaTask'`,
  and `./internal/rootcoord -run Test_createCollectionTask_prepareSchema_validatesFunctions` passed.
- `./internal/util/function -run 'TestMinHashFunctionRunnerAcceptsTextInput|TestValidateMinHashFunctionAcceptsTextInput'`
  passed, exercising create-time TEXT admission and actual MinHash output.

The standalone C++ bridge test passed. The production-mode native core was
rebuilt and installed after that test; a final focused run across packed,
compactor, function validation, Proxy and RootCoord passed against that library.
Python API tests passed `py_compile` but
have not executed: pytest collection in the available Conda environment is
blocked by missing `bm25s`; a controlled StorageV3 Milvus test instance is also
not yet available. No memory/performance, mixed-version rollout, or live API
search claim is made. The initial all-tests C++ build failed in unrelated
existing test translation units (`RemoteInputStream.h` and `FileManager.h`
`override` errors). An unfiltered `internal/util/function/...` run reached an
existing invalid-analyzer test that aborts in `canalyzer`; the focused
validation regressions passed.
