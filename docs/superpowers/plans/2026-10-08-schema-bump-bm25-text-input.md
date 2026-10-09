# Schema-bump BM25 / MinHash TEXT Input Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. Do not start sub-agents unless the user explicitly chooses delegated execution.

**Goal:** Backfill BM25 sparse and MinHash binary-vector outputs from historical TEXT, preserve existing LOB storage according to current compaction policy, and remove redundant TEXT capability gates.

**Architecture:** Keep one existing source reader and the existing read → selection → materialization → timestamp overwrite → write order. A narrow Milvus-local binding exposes the existing native LobColumnReader; compactor prepares a temporary String input view while retaining the selected physical write base. Both function materializers remain storage-agnostic.

**Tech Stack:** Go, Arrow Go v17 / Arrow C data interface, Milvus core C++, milvus-storage pinned at `e6e1ab0`, StorageV3, existing BM25 / MinHash runners.

**Spec:** `docs/design-docs/design_docs/20261008-schema-bump-bm25-text-input.md`.

**Status:** Implementation in progress, updated 2026-10-08. The worker path, validation changes and regression cases from Tasks 1–6 are implemented; the native standalone test and full compactor package have passed. Some planned native edge-case, performance and fault-injection checks remain unverified. Task 7's Python cases are added but API execution awaits a controlled StorageV3 instance and test dependencies. No commit or publication has been performed. Original BM25-only filenames are retained to keep existing links valid.

## Global Constraints

- BM25 and MinHash materializers consume `*array.String`, never LOB references or readers.
- Reading TEXT for computation does not change its persistence strategy.
- Additive reconciliation appends columns without changing existing TEXT column groups, reference bytes, LOB payload files or their manifest metadata.
- Same-partition-base full rewrite preserves reference bytes for surviving rows and reuses LOB files through the existing metadata-accounting path.
- Cross-partition-base rewriting remains governed by `docs/agent_guides/storage/path_contract.md`.
- Select rows once. No second sequential reader, source-row cursor, join or alignment pass.
- Decode only deduplicated TEXT inputs of missing BM25/MinHash functions.
- Already-decoded String input passes through without another decode. An absent nullable TEXT input becomes a null String array without LOB I/O.
- Schema-bump remains forced REUSE_ALL except for existing partition-base mismatch handling. Hole-ratio policy and writer-side LOB rewrite behavior do not change.
- LOB formats, coordinator scheduling, general LOB-rewrite policy, and the BM25/MinHash algorithms are outside this enhancement.
- No new `storage.RecordReader` interface, public storage API, or dependency update is planned.
- Go tests use `-tags dynamic,test -gcflags="all=-N -l" -count=1`.
- Read `docs/dev/error_handling_guide.md` and `docs/dev/error_handling_casebook.md` before coding errors. Use existing typed status facilities and `merr.Wrap/Wrapf`; apply AGENTS.md G1–G4 before behavioral claims.
- Preserve unrelated changes; no service restart, deployment, commit or push is implied by this plan. If commits are authorized, use scoped staging and `git commit -s`.

## Source anchors and responsibilities

Read the spec, storage path contract, and these implementation anchors before execution:

| Existing source | Responsibility to preserve |
| --- | --- |
| `internal/datanode/compactor/bump_schema_version_compactor.go` | Physical completeness decision; additive/full rewrite; filtering; stats; publication |
| `internal/datanode/compactor/record_materializer.go` | Selection and generic function output ownership |
| `internal/datanode/compactor/compactor_common.go` | Physically present input projection and BM25 selector inclusion |
| `internal/storage/record_reader.go`; `internal/storage/rw.go` | Physical Binary vs decoded String reader contracts; absent-field fill |
| `internal/compaction/lob_compaction.go` | LOB strategies, source paths and reference metadata accounting |
| `internal/storagev2/packed/packed_reader_ffi.go`; `internal/core/src/storage/loon_ffi/ffi_reader_c.cpp` | Existing Milvus Go/C++ bridge pattern and storage properties |
| `internal/util/function/validator/validator.go`; `internal/util/function/minhash_function.go` | Shared schema admission vs MinHash parameter validation |

The build cache can have a different revision from the pin. Inspect the dependency with:

```bash
git -C cmake_build/thirdparty/milvus-storage/milvus-storage-src show e6e1ab0:cpp/include/milvus-storage/lob_column/lob_column_reader.h
git -C cmake_build/thirdparty/milvus-storage/milvus-storage-src show e6e1ab0:cpp/src/lob_column/lob_column_reader.cpp
git -C cmake_build/thirdparty/milvus-storage/milvus-storage-src show e6e1ab0:cpp/src/segment/segment_writer.cpp
```

New production files have three narrowly scoped responsibilities:

1. `internal/core/src/storage/loon_ffi/text_lob_decoder_c.{h,cpp}`: native handle, safe Arrow import/export, invoke existing decoder.
2. `internal/storagev2/packed/text_lob_decoder.go`: Go lifecycle, cancellation checks and typed errors for that handle.
3. `internal/datanode/compactor/function_input_view.go`: required input discovery, per-field lazy decoders, logical input overlay and single-batch integration helper.

Execution dependencies: Task 1 + Task 2 → Task 3 → Task 4 → Task 5. Task 6 verifies the complete worker path; Task 7 verifies public behavior after all earlier gates pass.

## Task 1: Bind existing native TEXT reference-array decoding

**Files**

- Create `internal/core/src/storage/loon_ffi/text_lob_decoder_c.h` and `text_lob_decoder_c.cpp`.
- Modify `internal/core/src/storage/loon_ffi/CMakeLists.txt`.
- Create `internal/core/unittest/test_text_lob_decoder.cpp`; register in `internal/core/unittest/CMakeLists.txt`.
- Create `internal/storagev2/packed/text_lob_decoder.go` and `text_lob_decoder_test.go`.

**Interfaces**

Consumes existing `CreateLobColumnReader`, `ReadArrowArray`, `MakeInternalPropertiesFromStorageConfig` and Arrow C data import/export. Produces:

```go
func NewTextLOBDecoder(
    fieldID int64, lobBasePath string, storageConfig *indexpb.StorageConfig,
) (*TextLOBDecoder, error)

func (d *TextLOBDecoder) Decode(
    ctx context.Context, refs *array.Binary,
) (*array.String, error)

func (d *TextLOBDecoder) Close() error
```

The native entry points are:

```c
typedef void* CTextLOBDecoder;
CStatus NewTextLOBDecoder(int64_t field_id, const char* lob_base_path,
                         CStorageConfig config, CTextLOBDecoder* out);
CStatus DecodeTextLOB(CTextLOBDecoder decoder,
                     struct ArrowArray* refs, struct ArrowArray* strings);
CStatus CloseTextLOBDecoder(CTextLOBDecoder decoder);
```

- [ ] **1.1 Write failing native tests.** Use `lob_column::EncodeInlineText` for inline refs and `CreateLobColumnWriter` / `WriteArrowArray` / `Close` to produce real out-of-line refs. Register the test in the unit-test target before building. Cover inline, out-of-line, mixed, null, valid empty text, duplicate refs, reversed offsets, slices and multiple files. Use this concrete inline fixture:

```cpp
arrow::BinaryBuilder builder;
auto ref = milvus_storage::lob_column::EncodeInlineText("alpha");
ASSERT_TRUE(builder.Append(ref.data(), ref.size()).ok());
ASSERT_TRUE(builder.AppendNull().ok());
ref = milvus_storage::lob_column::EncodeInlineText("");
ASSERT_TRUE(builder.Append(ref.data(), ref.size()).ok());
std::shared_ptr<arrow::Array> refs;
ASSERT_TRUE(builder.Finish(&refs).ok());
// Export refs through Arrow C data, invoke DecodeTextLOB, import as utf8.
// Assert values ["alpha", null, ""], length 3 and unchanged input buffers.
```

- [ ] **1.2 Establish RED.** Build/run the newly registered test; before implementation it must fail on the missing bridge or behavior, not an unrelated unavailable native dependency. Record environmental build blockers separately.

- [ ] **1.3 Implement the bridge.** Use an opaque handle owning one `unique_ptr<LobColumnReader>`. Construct the filesystem through existing loon property/factory conventions, keeping complete local paths rooted at `/`. Import Binary refs, validate their representation with the pinned codec's constants/helpers, call the existing decoder, expose decoded payload buffers as UTF8, run UTF8 validation and export. The core operation remains:

```cpp
struct TextLOBDecoder {
    std::unique_ptr<milvus_storage::lob_column::LobColumnReader> reader;
};
// After safe Binary import and reference validation:
auto decoded = decoder->reader->ReadArrowArray(refs);
```

Do not implement a Go codec, convert encoded refs to strings, instantiate a stream reader, or modify a manifest. Check zero-length non-null refs before reading a tag; reject unknown tags, wrong out-of-line length and negative offsets. Null refs skip byte inspection. Persistent corruption is not an invalid client request.

- [ ] **1.4 Implement Go ownership and failure behavior.** Export a temporary input lease; C++ consumes that lease, not the Go array. Release any unconsumed lease on failure; zero-initialize output and release partial exports. Use existing CStatus conversion and preserve causes. The returned String owns its buffers independently of the decoder; Close clears the Go handle and is idempotent. Decode rejects closed handles and checks `ctx.Err()` before native I/O; no claim of interrupting in-flight native I/O.

- [ ] **1.5 Add binding lifecycle tests and establish GREEN.** Test valid results after decoder Close, retained inputs after Decode, repeated Close, canceled context, malformed input and failure cleanup. Run with native memory diagnostics where available; Arrow Go allocators alone cannot prove native allocations are released.

**Verification**

```bash
make test-cpp
./cmake_build/output/unittest/all_tests --gtest_filter='TextLOBDecoder.*'
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/storagev2/packed/... -run TextLOBDecoder
```

Use an isolated supported build environment; check existing build processes, disk space and build parallelism first. Rebuild native libraries before Go tests. Review/stage only this task's files if a commit is authorized.

## Task 2: Separate logical function inputs from the physical write base

**Files**

- Modify `internal/datanode/compactor/record_materializer.go`.
- Extend `internal/datanode/compactor/record_materializer_test.go` and `record_materializer_ut_test.go`.

**Interfaces**

Produces:

```go
func (m *RecordMaterializer) WrapWithInputs(
    writeBase storage.Record, logicalInputs storage.Record,
) (storage.Record, error)
```

Borrows both records; equal lengths are required. Computation reads `logicalInputs`. The result uses `writeBase` and owns computed outputs. On failure, release partial computed arrays only; the caller owns selected-base cleanup.

- [ ] **2.1 Add this failing input/output separation test**, using existing test types and helpers:

```go
func TestRecordMaterializerWrapWithInputsPreservesBase(t *testing.T) {
    refs := newBinaryArray(t, [][]byte{[]byte("opaque-ref")})
    defer refs.Release()
    text := newStringArray(t, []string{"alpha"})
    defer text.Release()
    output := newBinaryArray(t, [][]byte{[]byte("computed")})
    probe := &inputViewMaterializer{
        inputFieldID: 100, outputFieldID: 101, output: output,
    }
    m := &RecordMaterializer{materializers: []FunctionMaterializer{probe}}
    base := &materializerTestRecord{
        len: 1, columns: map[storage.FieldID]arrow.Array{100: refs},
    }
    inputs := &materializerTestRecord{
        len: 1, columns: map[storage.FieldID]arrow.Array{100: text},
    }
    got, err := m.WrapWithInputs(base, inputs)
    require.NoError(t, err)
    require.Same(t, text, probe.input)
    require.Same(t, refs, got.Column(100))
    require.Same(t, output, got.Column(101))
    cleanupMaterializedRecord(got)
    require.Zero(t, base.releaseCount)
    require.Zero(t, inputs.releaseCount)
}
```

- [ ] **2.2 Establish RED**, then extract the existing function loop into WrapWithInputs. Preserve the no-pending-functions fast path. Keep Wrap/WrapWithSelection as compatible entry points: create selection once, delegate with the same base as both arguments, and clean their derived selection on failure.

```go
if writeBase.Len() != logicalInputs.Len() {
    return nil, merr.WrapErrFunctionFailedMsg("function input row count mismatch")
}
if !m.hasMaterialization() { return writeBase, nil }
outputs := make(map[int64]arrow.Array)
for _, fm := range m.materializers {
    arrays, err := fm.Materialize(logicalInputs)
    if err != nil {
        releaseArrowArrays(outputs)
        return nil, err
    }
    for id, values := range arrays { outputs[id] = values }
}
if len(outputs) == 0 { return writeBase, nil }
return &materializedRecord{base: writeBase, computed: outputs}, nil
```

- [ ] **2.3 Remove storage capability checks from both materializer constructors.** Delete their VarChar-only input checks instead of adding `|| Text`. Keep field existence, output structure/type/nullability and row-count checks. Keep `stringInputsFromRecord` String-only; use function-neutral error wording that describes the logical input contract, not LOB support.

- [ ] **2.4 Add failing selection tests for both TEXT representations.** Use rows ["a", null, "c", "d"], non-contiguous ranges [0,1) and [2,4), and non-zero-offset source slices. Assert result is the same Arrow type, values/nulls are exact, and only selected rows reach the runner. In `buildSelectedColumn`, use actual-type Arrow slices/concatenation only for TEXT; release temporary slices on all paths. Do not modify `storage.NewRecordBuilder` globally.

- [ ] **2.5 Establish GREEN for ownership and regressions.** Include unequal lengths, no pending outputs, two functions where the second fails, computed output cleanup, retained results across reader advance, and normal VarChar BM25/MinHash behavior.

**Verification**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datanode/compactor/... -run 'Materializer|StringInputsFromRecord|Selected'
```

## Task 3: Build a compactor-local, deduplicated function input view

**Files**

- Create `internal/datanode/compactor/function_input_view.go` and `function_input_view_test.go`.

**Interfaces**

Consumes Tasks 1–2. The constructor receives only the functions already identified as missing, plus physical-field presence:

```go
func newFunctionInputPreparer(
    schema *schemapb.CollectionSchema,
    missingFunctions []*schemapb.FunctionSchema,
    existingFields map[int64]struct{},
    sourceManifest string,
    storageConfig *indexpb.StorageConfig,
) (*functionInputPreparer, error)

func (p *functionInputPreparer) Prepare(
    ctx context.Context, writeBase storage.Record,
) (logicalInputs storage.Record, cleanup func(), err error)

func (p *functionInputPreparer) Close() error

type textLOBDecoder interface {
    Decode(context.Context, *array.Binary) (*array.String, error)
    Close() error
}
```

The private interface is only a test seam for the packed decoder, not another storage API. The preparer has a lazy per-field decoder map and a constructor factory matching `NewTextLOBDecoder` through a small adapter returning `textLOBDecoder`.

- [ ] **3.1 Add failing unit tests with a counting fake decoder.** Define the fake in the new test file:

```go
type countingTextLOBDecoder struct {
    values *array.String
    calls, closes int
}
func (d *countingTextLOBDecoder) Decode(
    _ context.Context, _ *array.Binary,
) (*array.String, error) {
    d.calls++
    d.values.Retain()
    return d.values, nil
}
func (d *countingTextLOBDecoder) Close() error {
    d.closes++
    return nil
}
```

Construct one TEXT input referenced by a missing BM25 and a missing MinHash function. After one Prepare, assert `calls == 1`, both functions resolve the same String column, the view length equals the base, and base.Column still returns the exact Binary object. Prepare a second batch and assert the decoder constructor ran once but Decode ran twice.

- [ ] **3.2 Establish RED**, then discover deduplicated TEXT IDs from both function types. Skip functions whose outputs are already present using the caller's existing missing-function decision; do not invent another completeness rule. No source-path parsing or decoder creation is needed when this set is empty.

- [ ] **3.3 Implement the representation dispatch** and a lightweight overlay. The dispatch contract is:

```text
required field present as String       -> borrow, no allocation or I/O
required field physically absent       -> null String of writeBase.Len(), no I/O
required physical field present Binary -> lazily decode via the field handle
other representation                   -> typed execution-contract error
unrelated field                        -> delegate Column to writeBase
```

For absent fields retain existing null/default semantics; do not read LOB files. Use the source manifest's partition base plus `lobs/<fieldID>`, not the output root. Keep decoding outside LOBCompactionContext; do not set DecodeTextFromSource or change strategies.

The new view's lookup has no row mapping or I/O:

```go
type functionInputView struct {
    base storage.Record
    columns map[int64]arrow.Array
}
func (v *functionInputView) Column(id storage.FieldID) arrow.Array {
    if col, ok := v.columns[id]; ok { return col }
    return v.base.Column(id)
}
func (v *functionInputView) Len() int { return v.base.Len() }
```

- [ ] **3.4 Implement explicit ownership.** Prepare returns an idempotent cleanup for decoded/null arrays only. Partial preparation failure releases already-created arrays. The view borrows the base and is synchronous-call scoped. If implementing Record.Retain/Release, mirror base/array leases symmetrically; cleanup must not substitute Release on a borrowed base. Close all opened decoders, even when one Close fails, and preserve the first error.

- [ ] **3.5 Establish GREEN.** Include String pass-through, nullable missing input, null vs empty text, no required TEXT, unrelated TEXT, multiple fields, partial decode failure, canceled context and retained overlay leases. Assert no decoder creation for String/null/no-work cases.

**Verification**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datanode/compactor/... -run FunctionInputPreparer
```

## Task 4: Integrate both compactor routes without altering LOB policy

**Files**

- Modify `internal/datanode/compactor/bump_schema_version_compactor.go` and `function_input_view.go`.
- Extend `bump_schema_version_ut_test.go` and `bump_schema_version_compactor_test.go` in that package.

**Interfaces**

Consumes `newFunctionInputPreparer` and `WrapWithInputs`. Produces this private shared helper:

```go
func materializeWithPreparedInputs(
    ctx context.Context, rec storage.Record, selection *recordSelection,
    m *RecordMaterializer, p *functionInputPreparer,
) (storage.Record, error) {
    base := rec
    if selection != nil {
        selected, err := newSelectedRecord(rec, m.schema, m.pendingOutputs, selection)
        if err != nil { return nil, err }
        base = selected
    }
    inputs, cleanup, err := p.Prepare(ctx, base)
    if err != nil {
        cleanupMaterializedRecord(base)
        return nil, err
    }
    defer cleanup()
    out, err := m.WrapWithInputs(base, inputs)
    if err != nil { cleanupMaterializedRecord(base) }
    return out, err
}
```

- [ ] **4.1 Add failing real-LOB BM25/MinHash cases for additive and full rewrite.** Reuse `buildBumpFixture`, `withSourceFields`, `withTextLOBSource`, `withTargetFunctions` and the existing golden helpers. A concrete BM25 fixture is:

```go
const textID, outputID = int64(105), int64(106)
fn := &schemapb.FunctionSchema{
    Name: "bm25_text", Id: 1000, Type: schemapb.FunctionType_BM25,
    InputFieldNames: []string{"big_text"}, InputFieldIds: []int64{textID},
    OutputFieldNames: []string{"sparse"}, OutputFieldIds: []int64{outputID},
}
fix := buildBumpFixture(t, withRows(6),
    withSourceFields(&schemapb.FieldSchema{
        FieldID: textID, Name: "big_text", DataType: schemapb.DataType_Text,
        TypeParams: []*commonpb.KeyValuePair{{Key: "enable_analyzer", Value: "true"}},
    }),
    withFillValue(func(i int, _ uint64, values map[int64]any) {
        values[textID] = bumpFxLobText(i)
    }),
    withTextLOBSource(textID),
    withTargetAddedField(&schemapb.FieldSchema{
        FieldID: outputID, Name: "sparse",
        DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true,
    }),
    withTargetFunctions(fn),
)
beforeRefs := readTextRefs(t, fix, fix.sourceManifest, textID)
beforeFiles := listLobFiles(t, fix)
_, manifest := runAdditiveCompact(t, fix)
texts := make([]string, len(fix.rows))
for i := range texts { texts[i] = bumpFxLobText(i) }
expected := expectedBM25SparseRows(t, fix.targetSchema, fn, texts)
requireSparseRows(t, fix, manifest, outputID, len(texts), expected)
require.Equal(t, beforeRefs, readTextRefs(t, fix, manifest, textID))
require.Equal(t, beforeFiles, listLobFiles(t, fix))
```

For MinHash use BinaryVector output with `dim=512`, `num_hashes=16` and `shingle_size=3`, then compare each output row to `expectedMinHashRows`. Both cases call `setupBumpUTEnv(t)` before building fixtures. Add a shared-TEXT case with both outputs and assert one decode per field/batch.

- [ ] **4.2 Establish RED** on current preflight/materializer rejection. Wire one preparer per task execution and Close it on every exit. Use the existing physical completeness decision to supply missing functions. Keep the additive reader projection and BM25 by_field selector inclusion.

- [ ] **4.3 Replace the additive Wrap call** with `materializeWithPreparedInputs(ctx, record, nil, materializer, preparer)`. Do not filter rows. Keep output-only writer projection, incremental stats, row counts, manifest-delta/full-manifest result modes, and publication unchanged.

- [ ] **4.4 Replace full rewrite's WrapWithSelection call** with the helper and the existing selection. Preserve all-filtered early-continue before preparation; do not call WrapWithSelection again. Keep timestamp overwrite after materialization and write before derived-array cleanup.

- [ ] **4.5 Preserve both existing LOB routes.** Same-base: physical reader, WithTextRefsAsBinary, unchanged surviving refs. Partition-base mismatch: existing decoded reader and rewrite writer; preparation sees String and does nothing. Add a high-hole-ratio schema-bump test proving it still reuses refs; no new hole-ratio decision. Test migration with deletion/TTL to exercise String-preserving selection.

- [ ] **4.6 Delete `validateMaterializationInputField` and its calls** after both worker paths are wired. Keep source-field existence, duplicate/partial outputs, persisted output type and zero-row checks. Establish GREEN for both functions, shared/mixed inputs, BM25 multi-analyzer, nulls, unrelated TEXT, multiple batches, all-filtered batches and no-op reruns.

**Verification**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datanode/compactor/... -run 'Bump.*(Text|LOB|BM25|MinHash|Materialization)'
```

## Task 5: Consolidate validation and open public TEXT admission

**Files**

- Modify `internal/proxy/task.go`; tests `function_task_test.go`, `task_test.go` and `util_test.go`.
- Modify `internal/util/function/validator/validator.go` and `validator_test.go`.
- Modify `internal/util/function/minhash_function.go` and `minhash_function_test.go`.

**Interfaces**

No new schema/transport interfaces. `CheckFunctionInputField` remains the owner of schema input compatibility. `ValidateMinHashFunction` retains parameter/dimension checks but no independent input-type whitelist. Both materializers consume only logical String arrays.

- [ ] **5.1 Add failing shared-validator tests** for TEXT with runtime checks disabled, matching RootCoord usage:

```go
func TestValidateFunctionMinHashTextWithRuntimeCheckDisabled(t *testing.T) {
    schema := minHashCollectionSchema("2")
    schema.Fields[0].DataType = schemapb.DataType_Text
    require.NoError(t, ValidateFunction(schema, "minhash", true))

    schema = minHashCollectionSchema("3")
    schema.Fields[0].DataType = schemapb.DataType_Text
    require.ErrorContains(t, ValidateFunction(schema, "minhash", true),
        "does not match expected dim")
}
```

Also cover VarChar success, non-string rejection, wrong input count, disabled BM25 analyzer, invalid output type/nullability, invalid token_level/shingle_size/num_hashes and overflow.

- [ ] **5.2 Establish RED**, then change only the shared MinHash input compatibility rule to accept VarChar/Text and remove its stale TEXT-rejection comment. Delete the duplicate input-type condition in ValidateMinHashFunction. Keep all other parameter/shape guarantees; do not newly expose deprecated DataType_String at public admission. Audit production call sites of ValidateMinHashFunction before removal; current source calls it from the shared validator.

Replace the MinHash branch's input condition with a count-safe check (an invalid
count must not dereference fields[0]):

```go
if len(fields) != 1 {
    return merr.WrapErrParameterInvalidMsg("MinHash requires one input field")
}
if fields[0].DataType != schemapb.DataType_VarChar &&
    fields[0].DataType != schemapb.DataType_Text {
    return merr.WrapErrParameterInvalidMsg(
        "MinHash function input field must be VARCHAR/TEXT, got %s",
        fields[0].DataType.String())
}
```

- [ ] **5.3 Delete `validateAddFunctionInputNotText` and its call** from Proxy; do not retain a MinHash-only version. Replace helper-only rejection tests with create/add pre-execution success tests for both TEXT functions. Update the shared-validator assertion in util_test.go from TEXT rejection to acceptance.

- [ ] **5.4 Keep genuine prerequisites.** StorageV3, schema-bump enabled, storage-version upgrade enabled, valid output schema and analyzer/selector rules still reject invalid requests. Verify create/add use the same shared input rule; RootCoord's `disableRuntimeCheck=true` must still validate MinHash parameters.

- [ ] **5.5 Establish GREEN** through validator, function, Proxy and RootCoord regressions. Grep removed names and obsolete "TEXT requires LOB decoding" capability rejections; only logical runtime type checks may remain. Do not delete structural/data-integrity checks under the heading of validation cleanup.

**Verification**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/... ./internal/proxy/... ./internal/rootcoord/...
rg -n 'validateAddFunctionInputNotText|validateMaterializationInputField|text input requires LOB decoding' internal
```

The final rg should have no stale production rejections. Review any remaining explanatory/test strings rather than silently ignoring them.

## Task 6: Prove preservation, failure behavior and resource bounds

**Files**

- Extend `internal/core/unittest/test_text_lob_decoder.cpp` and `internal/storagev2/packed/text_lob_decoder_test.go`.
- Extend `internal/datanode/compactor/function_input_view_test.go` and `bump_schema_version_ut_test.go`.
- Record executed evidence and limitations in the linked design document.

**Interfaces**

No new product interface. Use existing `readTextRefs`, `listLobFiles`, `mustManifestLobFiles`, `verifySourceIntact`, `expectedBM25SparseRows` and `expectedMinHashRows` test helpers.

- [ ] **6.1 Extend exact preservation assertions.** Additive compares original column-group descriptors and LOB metadata as well as refs/file hashes in both result modes. Full rewrite compares only surviving PKs, with exact original refs and function outputs; source data remains intact. Migration compares logical text and source integrity, permits destination refs to change, and resolves them through the destination reader. Assert no MinHash BM25-stat entries.

```go
sourceRefs := readTextRefs(t, fix, fix.sourceManifest, textID)
segment := runCompact(t, fix)
outputRefs := readTextRefs(t, fix, segment.GetManifest(), textID)
for _, row := range fix.keptRows() {
    pk := row.pk.(int64)
    require.Equal(t, sourceRefs[pk], outputRefs[pk])
}
```

Here `fix` is the full-rewrite fixture from Task 4, with dropped field and real deltalog/TTL inputs. Compare per-PK function output using its kept plaintext; never use row count alone as the oracle.

- [ ] **6.2 Add failure injection cases with explicit assertions.** Test malformed/zero-length/tag/offset refs, missing LOB, truncated payload, transient read error, canceled context, later-function failure, writer failure and commit failure. Each must return an error, not a successful compaction result; source refs/payloads stay unchanged and all temporary arrays/handles close. Check actual publication state: additive append and full rewrite output must not become a successful logical result on failure. Do not equate leftover unreferenced output files with source mutation or claim GC cleanup this feature does not provide.

- [ ] **6.3 Audit producer-to-consumer errors under G1/G2.** Trace native file read/Vortex Take → ReadArrowArray → new C bridge → Go error → compactor/task consumer. Inspect construction, catch-all and stringify sites. The pinned Vortex `take()` validates requested row indices before scanning; keep an out-of-range test to verify the resulting corruption error crosses the bridge without altering source references. Do not silently bump the dependency or invent successful empty vectors.

- [ ] **6.4 Measure batch-local resources.** Exercise long TEXT for both functions, MinHash word/char modes, shared inputs, many source files and repeated task execution. Record decoded bytes, peak RSS, elapsed time and handles/cache growth. Include MinHash's contiguous C-buffer copy and int32 input lengths; Arrow String uses 32-bit offsets. No performance claim without measurements. If a bound fails, report it before adding new tuning knobs; any proposed sub-batching must slice the same selected record, not reopen a stream.

- [ ] **6.5 Run broad regressions and G4 adversarial review.** Include storage, packed, compaction, function, Proxy and RootCoord. Check the generic materializer's existing callers, because Wrap/WrapWithSelection remain shared APIs. If a global error wire mapping is changed, run merr guards and full `make test-go`; do not change such mappings merely to complete this feature.

**Verification**

```bash
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/storage/... ./internal/storagev2/packed/... ./internal/compaction/... ./internal/datanode/compactor/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/function/... ./internal/proxy/... ./internal/rootcoord/...
```

## Task 7: Verify public create/add, backfill, insert and search

**Files**

- Extend `tests/python_client/milvus_client/test_add_function_field_feature.py`.
- Extend `tests/python_client/milvus_client/test_milvus_client_minhash.py`.
- Update evidence/status in the linked design document.

**Interfaces**

Use existing test clients and `wait_for_index_ready` / `wait_for_search_hit` helpers. No new client API. Configure TEXT without a VARCHAR max_length. Do not change the function algorithms or query wire format.

- [ ] **7.1 Add failing API tests named with `text_lob`.** Create historical TEXT rows containing an actual out-of-line value, flush/seal, then add BM25 or MinHash output and the corresponding index. Reuse existing add-function/index helpers in these files; include short, empty and nullable text alongside long text. Use the existing MinHash parameter/backfill case as the call-pattern reference:

```python
schema.add_field("doc", DataType.TEXT, enable_analyzer=True)
old_text = "historical alpha document " * 4096
assert len(old_text.encode("utf-8")) > 65536

mh_field = FieldSchema(name="mh", dtype=DataType.BINARY_VECTOR, dim=512)
mh_function = Function(
    name="minhash_text_lob",
    function_type=FunctionType.MINHASH,
    input_field_names=["doc"],
    output_field_names=["mh"],
    params={"num_hashes": 16, "shingle_size": 3, "token_level": "word"},
)
```

Use BM25 with SparseFloatVector output and sparse index in its case. Index readiness alone is insufficient: await an old-row search hit.

- [ ] **7.2 Verify old and new data separately.** Historical rows must acquire missing outputs; rows inserted after function admission must compute outputs through normal insertion. Query original TEXT values, search old/new rows, release/load and repeat. For MinHash also cover create-time TEXT function and word/char parameterization. Verify each long TEXT row actually used a LOB in the local integration suite; API text length alone is not proof under a changed inline threshold.

- [ ] **7.3 Establish GREEN on a controlled fresh build.** Use an isolated test instance with StorageV3/backfill prerequisites. Do not restart a shared user instance. Collect tests first; no matching tests is not success:

```bash
python3 -m pytest tests/python_client/milvus_client/test_add_function_field_feature.py --collect-only -q -k text_lob
python3 -m pytest tests/python_client/milvus_client/test_add_function_field_feature.py -k text_lob
python3 -m pytest tests/python_client/milvus_client/test_milvus_client_minhash.py -k text_lob
```

- [ ] **7.4 Record rollout/verification limits.** Capable workers must precede enabling new admission. This plan adds no mixed-version scheduler capability handshake; report mixed-version support only if verified. Report exact tests/build/platform exercised, untested failures and native memory limits. Mark only executed steps complete.

## Completion and handoff checklist

- [ ] Both functions compute exact outputs for real historical TEXT LOB inputs.
- [ ] Additive preserves existing TEXT artifacts; ordinary full rewrite preserves surviving refs and payload files.
- [ ] Existing namespace compatibility remains working, including selection of decoded String columns.
- [ ] Shared inputs decode once per batch; already-decoded/missing/no-work inputs perform no LOB I/O.
- [ ] Schema rules are consolidated; capability-only gates are removed; real prerequisites remain.
- [ ] G1–G4 failure/ownership review and public create/add/insert/index/search/reload evidence are recorded.
- [ ] Neither test discovery failure nor compilation-only success is reported as behavioral verification.
- [ ] If a PR is later requested, include the feature issue and this repository design doc, use DCO, and state only verified outcomes.
