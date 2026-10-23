# Design: STL_SORT indexes for ordinary ARRAY fields

- Date: 2026-10-21
- Status: Implemented on `feat/array-stl-sort` with targeted validation complete. Local SDK validation is covered below; real mixed-binary upgrade and performance validation remain outstanding.
- Related PR: [milvus-io/milvus#53972](https://github.com/milvus-io/milvus/pull/53972)
- Review record: [ARRAY STL_SORT design review](https://zilliverse.feishu.link/app/app_17gfpkhg788)
- Analysis baseline: PR snapshot `66b9f0b86d77`; source baseline `5c4b3520c1f0`.

## 1. Goals and scope

Support STL_SORT scalar indexes on ordinary schema ARRAY fields through two independently verifiable paths:

1. Explicit `index_type=STL_SORT`: create, build, load, and execute ARRAY contains queries.
2. HYBRID: select SORT for high-cardinality ordinary ARRAY data when the negotiated capability version permits it, while retaining BITMAP for low cardinality.

The index accelerates element membership without changing array values, order, length, duplicate counts, or query semantics. Underlying SORT range operations must remain correct, but this design adds no ARRAY range-query syntax and does not interpret element range matches as whole-array comparisons.

This design excludes the global NaN total order, Tantivy floating-key normalization, non-finite scalar ingestion, REST representations, ORDER BY/aggregation semantics, and legacy floating-index raw fallback changes in PR #53972. It also does not extend schema or ingestion support for JSON cast arrays, multiple Struct nesting levels, or nullable array members.

**Type boundary:** Public support covers BOOL, INT8/INT16/INT32/INT64 and VARCHAR ARRAY elements. Ordinary FLOAT/DOUBLE ARRAY SORT is deferred; HYBRID keeps BITMAP/INVERTED for those elements at both v5 and v6. Existing Struct floating sub-field support is unchanged. Lower-level STRING/TEXT/TIMESTAMPTZ representations do not extend public schema capabilities.

## 2. Baseline implementation and gaps

The source baseline already has the scalar index interface refactor. This design uses Builder → Artifact → Loader → Reader rather than restoring the PR snapshot's `ScalarIndexSort`, `StringIndexSort`, or `IndexFactory`.

| Layer | Source baseline | Required change |
| --- | --- | --- |
| Proxy | `STLSORTChecker` accepts ARRAY only for Struct sub-fields | Accept supported ordinary ARRAY element types |
| C++ type routing | `IndexTypeAdapter` accepts only HYBRID/BITMAP/INVERTED for ordinary ARRAY | Add explicit STL_SORT routing |
| Build | `SortedArrayIndexBuilder` already accepts `ScalarBuildInput<ArrayView>` | Reuse it and verify its behavior; do not transplant entire legacy implementations |
| Load/query | Sorted readers already distinguish Row/Element domains and disable ordinary ARRAY value lookup | Verify agreement between metadata caps, loaded caps, and production consumers |
| HYBRID | Nested fields can select SORT after a version gate; high-cardinality ordinary ARRAY still selects INVERTED | Add an independent ordinary ARRAY capability gate |
| Version | Scalar engine current/maximum is 5 | Reserve named capability constants and allocate the version at merge time |

Update HYBRID's obsolete comment that the sort family cannot handle ordinary ARRAY. An existing ArrayView builder alone does not mean the production entry points are open.

## 3. Data and coordinate contracts

### 3.1 Parent-row postings

Each ordinary ARRAY element indexes its parent row ID. Nested Struct sub-fields continue to index flattened element offsets. Do not reuse the nested flag to enable ordinary ARRAY, or project Row-domain results from elements to parents a second time.

For example:

```text
row 0 = [1, 1, 100]
row 1 = []
row 2 = NULL
row 3 = [2, 3]
row 4 = [1, 4]

logical postings: 1→{0,4}, 2→{3}, 3→{3}, 4→{4}, 100→{0}
validity:         [1,1,0,1,1]
Count():          5
CoordDomain():    Row
```

Physical numeric pairs may retain duplicate elements, while string postings may deduplicate them. Logical results are always sets of parent rows. Deduplication is not a correctness prerequisite and never changes raw data. Posting counts may exceed row counts; do not derive `Count()` from pair counts, distinct counts, or the largest matching ID.

### 3.2 NULL, empty arrays, and input ownership

- Never access a NULL row's ArrayView payload. It emits no postings and has false validity.
- A non-NULL empty array emits no postings but has true validity. Postings cannot reconstruct that validity.
- Parent-row coordinates increase continuously across batches. NULL rows and empty arrays each consume a row coordinate.
- An empty validity view means all rows are valid. Values and variable-length backing storage remain stable throughout synchronous Build.
- Neither Builder nor Artifact retains borrowed input. An Artifact must remain serializable after the input is destroyed.
- Zero-row input follows the existing build contract. Rows containing only empty arrays or NULL must work and must not be mistaken for zero rows.

This feature adds no member-level NULL capability. Future member validity requires a separate contains-semantics contract. Tests with unsupported or invalid payloads are not evidence that ingestion supports them.

## 4. Build, persistence, and Reader

### 4.1 Build and registration

`IndexTypeAdapter` normalizes explicit ARRAY STL_SORT into:

```text
family = sort
field_type = ARRAY
array_element_type / value_type = schema element type
nested = false
scalar_index_engine_version = negotiated target
```

Select the existing builder through `BuilderRegistry<ScalarBuildInput<ArrayView>>`. Numeric inputs produce sorted `(value, parent_row)` pairs; strings produce an ordered dictionary and parent postings. No preliminary element flattening, additional full-field copy, or replay protocol is needed. Supported element types must agree at the Proxy, type adapter, builder, and loader boundaries rather than depending on a more permissive template dispatch at one layer.

### 4.2 Persistence

Reuse the existing SORT Artifacts, named entries, and containers without adding storage paths or object namespaces:

- `index_data` stores numeric pairs or string dictionary/postings.
- `num_rows` is the parent-row count. `index_length` retains its numeric/string format-specific meaning.
- `valid_bitset` explicitly stores validity for every parent row. New ordinary ARRAY artifacts must not omit it and infer validity from postings later.
- Preserve compatibility of the existing offsets entry. Offsets do not provide ordinary ARRAY value reconstruction.
- Set `nested=false`. Obtain field/element types from trusted schema and normalized runtime parameters, and cross-check persisted metadata.

V3 synchronous, asynchronous, and mmap loading use the existing Plan/Finish flow. Readers retain files, mappings, and bitmaps for their full lifetime. Preserve existing legacy-reader capabilities; the feature does not require enabling an additional legacy writer. If a format cannot express these contracts, revise the design and format version instead of silently omitting validity.

### 4.3 Capabilities and raw-data retention

An ordinary ARRAY Reader reports `CoordDomain=Row`, `Count=parent rows`, `predicate=true`, `nested=false`, `value_lookup=false`, and `cheap_value_lookup=false`. Supported membership queries are exact. Metadata-only capabilities must match the opened `Reader::Caps()`.

Keep the raw ARRAY column for projection, positional access, length, whole-array comparisons, and other operations that require the original structure. Storing element values does not allow `Lookup/Gather` to reconstruct the array; returning an arbitrary element is invalid. The old `HasRawData` contract corresponds to capabilities and raw-column loading decisions in the new architecture.

## 5. Query semantics

Let `V` be parent-row validity and `P(x)` the set of parent rows containing element x. Ordinary ARRAY contains reuses `IScalarPredicateReader<T>::In` without depending on the concrete family:

| Operation | Data matches; apply final validity separately |
| --- | --- |
| contains(x) | P(x) |
| contains_any(S) | Union of postings for elements in S |
| contains_all(S) | Intersection of postings for elements in S |
| NOT contains(x) | Complement of P(x) within V |
| IS NULL / IS NOT NULL | Complement of V / V |

Repeated query terms do not change results. For an empty query set, any is false and all is vacuously true for valid rows. NULL remains UNKNOWN, and an outer NOT must not turn it into a match. Empty arrays return false for nonempty any/all and true for empty all. In the example, `contains_all([1,4])` matches only row 4. The empty array in row 1 and NULL in row 2 must behave differently under empty queries and negation.

Underlying `NotIn` means “contains none of the supplied values,” not “has an element unequal to a supplied value.” A positive range query means “has an element within the range.” Row 0 must match [1,4] because it contains 1, even though it also contains 100. When multiple elements share a parent, do not optimize positive ranges by clearing parents of out-of-range elements. Retain and verify the forward-matching branch used with `value_lookup=false`.

Membership postings cannot directly answer whole-array equality, positional predicates, or ARRAY length. Correlated Struct predicates must still combine in the Element domain before projection; this change must not move their fold point.

## 6. HYBRID policy and version compatibility

Let L be the distinct valid-element cardinality threshold and A the ordinary ARRAY SORT capability version:

| Field shape | cardinality < L | cardinality ≥ L |
| --- | --- | --- |
| Ordinary ARRAY, target < A | BITMAP | INVERTED |
| Ordinary ARRAY, target ≥ A, supported element type | BITMAP | SORT |
| Element type not open to SORT | Existing policy | INVERTED |
| Nested Struct sub-field | Existing policy | Existing nested SORT version gate |

Cardinality probing ignores NULL rows; empty arrays contribute no values. Repeating a value across rows does not increase cardinality. Probing may stop at the threshold, but the selected builder receives the same complete, stable input. Preserve normalized field shape, nested state, version, staging, and other parameters during delegation. ARRAY still ignores the configurable primitive low/high family overrides; configuration semantics do not expand here.

Introduce `MinScalarIndexVersionForArrayStlSort` and C++ `kArrayStlSortMinVersion` for explicit creation and HYBRID. This implementation allocates version 6; advance both together if that version is occupied before merge. Do not couple this capability to a NaN version constant or mistake an unchanged container format for an old reader's ability to interpret multiple parent-row postings.

Explicit STL_SORT returns a retriable service-not-ready error when the negotiated version is too old. It must not silently switch to INVERTED. Cover regular CreateIndex and snapshot-restore/direct-DDL entry points using a shared gate analogous to FMINDEX, and verify schema/element metadata is available at each entry. Proxy validation alone does not protect restore paths. HYBRID retains the old-version INVERTED fallback shown above.

Artifacts record the actual target version. New builds, background rebuilds, and compaction use negotiation, never the local maximum. Background dispatch repeats the explicit ARRAY SORT gate, and the build boundary rejects old targets. Preserve existing scalar/nested SORT loading, do not disable old floating indexes wholesale, and do not require existing ARRAY INVERTED/BITMAP indexes to rebuild automatically. Old HYBRID children still load through their selector; only new builds may select SORT.

Existing negotiation primarily protects QueryNode readers. Validate the upgrade order and scheduling constraints for DataNode/IndexNode builders. If old builders can still receive new tasks, the gate is incomplete: add worker capability constraints or require build workers to upgrade first, and validate a real upgrade. After writing new ARRAY SORT artifacts, downgrade requires rebuilding a compatible index first; direct rollback readability is not guaranteed.

## 7. Floating-point compatibility boundary

Ordinary FLOAT/DOUBLE ARRAY SORT is deferred. Native comparison does not provide a suitable order for NaN, and older gRPC writes could already store non-finite floating ARRAY values. Rejecting only new writes cannot make those stored values safe: changing HYBRID selection would make index rebuilds fail, a new reader-based import check would reject binlog restores, and validation after partial-update merging could reject untouched legacy fields.

This feature therefore does not add Proxy ARRAY non-finite checks, centralized DataNode import checks, or SORT finite-value guards. Existing parser checks and Struct floating sub-field behavior remain unchanged. Floating ARRAY HYBRID retains its original BITMAP/INVERTED selection. Inf is ordered by native comparison; future work must distinguish Inf from NaN and define legacy-data, restore and query compatibility. JSON STRING_TO_DOUBLE and the broader #53971 work remain separate.

## 8. Resources and failure behavior

Memory/file estimates must distinguish parent-row count N, element-posting count E, distinct-string count U, and string bytes. Do not use N to estimate E numeric pairs or E to infer parent validity/offset lengths. Relate actual Loader MemoryUsage/FileUsage to metadata estimates, including mmap auxiliary data and peak load staging. Claim no memory or performance improvement without measurements.

| Failure | Expected handling |
| --- | --- |
| User selects an unsupported ARRAY element type | Return the existing typed parameter error at the public boundary |
| New capability unavailable during rolling upgrade | System/retriable; never mark as InputError |
| Contradictory normalized parameters or wrong builder input shape | Internal contract failure; do not blame the current user request |
| Out-of-range persisted postings, missing/corrupt validity, truncation, or CRC failure | Data-integrity failure; do not disguise it as a missing index or silently swallow it and fall back |
| Storage throttling, timeout, cancellation, or insufficient resources | Preserve existing typed causes and retry classification through the existing load/scheduling mechanisms |

Add no wire codes or retry policies. Audit error construction, wrapping, cgo propagation, and consumers according to the error-handling guide. Add context with `merr.Wrap/Wrapf`; do not collapse system failures into parameter errors.

## 9. Validation plan and merge gates

The following is the required validation matrix, not a statement that every check is complete.

| Layer | Required coverage |
| --- | --- |
| Builder/Reader contracts | Destroy input after registry build, then serialize/load independently; Row domain and Count; metadata/loaded caps agreement; no ARRAY value lookup |
| Data matrix | BOOL, integer widths/boundaries, VARCHAR empty/Unicode/NUL strings; duplicates, batch boundaries, NULL, empty arrays, all-empty/all-NULL input, validity-word boundaries; rejection of ordinary floating SORT and preservation of floating HYBRID routing |
| Queries | contains/any/all, repeated/empty terms, NOT, NULL three-valued logic; Range with matching and nonmatching elements in one row; raw paths for positional/whole-array operations |
| Persistence | Current writer format, V3 synchronous/asynchronous, heap/mmap; supported legacy-reader regressions; corrupt metadata, out-of-range postings, missing validity |
| Production path | Explicit create → build → upload → load → query; verify the actual HYBRID selector and SORT use rather than raw/INVERTED |
| Upgrade/restore | Thresholds L−1/L/L+1 and target A−1/A; CreateIndex, snapshot restore, rebuild/compaction; mixed worker/reader versions and downgrade restrictions |
| Resources/failures | N≪E, long strings, all-empty/all-NULL input; estimates versus allocations; typed propagation and cleanup for storage faults, cancellation/timeout, and OOM |

For every supported production predicate, compare raw evaluation with BITMAP/INVERTED/SORT and assert complete data and validity bitmaps. Derive expected results from manually inspectable data or an independent oracle so raw and indexed paths cannot agree through a shared broken helper. For failures, perform the G1 repository-wide construction/rewrite audit and G2 end-to-end tracing or injection. Check for uncovered entry points and limit PR claims to proven behavior.

Use current `index_tests` contracts and expression/segment integration tests for native validation. Run Go tests for affected packages, for example:

```sh
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/util/indexparamcheck/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/ddl/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datacoord/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/proxy/fieldvalidator/...
go test -tags dynamic,test -gcflags="all=-N -l" -count=1 ./internal/datanode/importv2/...
```

Public API/SDK integration and real version-upgrade tests are also required. Helpers, compilation, or happy-path E2E alone do not satisfy the full design. Performance comparisons should report build time, artifact size, peak load memory, and contains latency, grouped by cardinality, array length, duplicate ratio, and nullable ratio. Do not assume SORT always outperforms INVERTED.

## 10. Implementation breakdown

| Task | Scope | Dependencies / completion criteria |
| --- | --- | --- |
| A1: Lower-level contracts and loading | Audit/reuse the existing ArrayView builder, Artifact, Loader, and Reader; close validity, range, and resource gaps | Prove parent-row/NULL/load behavior under section 9; do not duplicate legacy classes |
| A2: Explicit entry points and compatibility gates | Proxy, IndexTypeAdapter, supported-type lists, DataCoord create/restore checks, and version constants | Depends on A1; deliver a usable public API |
| A3: HYBRID selection | Ordinary ARRAY version/type gates, cardinality policy, parameter delegation, and configuration notes | Depends on A1 and version support; may follow A2 |
| A4: Integration and release notes | Complete persistence/upgrade/failure matrices and measurements; document floating boundaries and rollback requirements | Ship corresponding validation with each feature PR rather than treating it as an ungated follow-up |

A1/A2/A3 each include their tests. Keep global NaN, non-finite REST/import support, JSON typed-array routing, and unrelated nested fixes separate. Preserve the floating compatibility boundary in section 7. Only fixes actually required by this feature become explicit prerequisite PRs; do not cherry-pick entire files.

## 11. Alternatives

- **Open SORT only inside HYBRID:** Does not satisfy explicit ARRAY STL_SORT creation.
- **Pretend ordinary ARRAY is nested:** Confuses Row/Element coordinates and folding semantics.
- **Continue using INVERTED for every ARRAY:** Simpler compatibility; retain it for old versions and unsupported types, but it does not deliver the new capability.
- **Combine global NaN changes in the same PR:** Expands upgrade, protocol, and query-semantics risk beyond the approved split.

## 12. Code and specification references

Paths are relative to this design document. Baseline descriptions refer to the source revision identified above.

- [Scalar index architecture](../../../internal/core/src/index/scalar/README.md)
- [Interfaces and coordinate/NULL contracts](../../../internal/core/src/index/contracts/README.md), [contract tests](../../../internal/core/src/index/contracts/TESTING.md)
- [ARRAY SORT builder](../../../internal/core/src/index/scalar/sort/SortedIndexBuilder.cpp), [Artifact](../../../internal/core/src/index/scalar/sort/SortedIndexArtifact.cpp), [Loader](../../../internal/core/src/index/scalar/sort/SortedIndexLoader.cpp), [Reader](../../../internal/core/src/index/scalar/sort/SortedIndexReader.cpp)
- [HYBRID builder](../../../internal/core/src/index/scalar/hybrid/HybridIndexBuilder.cpp)
- [C++ type routing](../../../internal/core/src/index/IndexTypeAdapter.cpp), [contains execution](../../../internal/core/src/exec/expression/JsonContainsExpr.cpp)
- [Proxy checker](../../../internal/util/indexparamcheck/stl_sort_checker.go), [DDL](../../../internal/proxy/ddl/task_index.go)
- [DataCoord CreateIndex](../../../internal/datacoord/index_service.go), [snapshot restore](../../../internal/datacoord/snapshot_manager.go)
- [Go version definitions](../../../pkg/common/common.go), [C++ version definitions](../../../internal/core/src/common/Consts.h)
- [Error-handling guide](../../dev/error_handling_guide.md), [casebook](../../dev/error_handling_casebook.md), [storage-path contract](../../agent_guides/storage/path_contract.md)

## 13. Initial implementation and validation record

Date: 2026-10-21. These are historical results from before the floating extension.

Explicit ordinary ARRAY STL_SORT was enabled for BOOL/integer/VARCHAR, with a v6 gate for high-cardinality HYBRID selection. An intermediate floating extension was tested in section 14 and subsequently withdrawn after review; section 16 defines the current scope. CreateIndex, snapshot restore, and background dispatch share the gate. If negotiation falls below the gate at dispatch time, the task waits rather than sending incompatible work. The C++ boundary independently rejects explicit ARRAY SORT targets below v6.

The load audit found and fixed a pre-existing lower-level builder gap: numeric legacy Artifacts omitted validity, turning empty arrays into NULL after reload. Only ordinary ARRAY numeric Artifacts now write this sidecar, and the legacy loader reads and validates it. Both ordinary ARRAY legacy and V3 loaders reject artifacts missing validity. Old scalar legacy Artifacts without the sidecar retain their reconstruction behavior. V3 reuses existing entries without changing storage paths.

| Validation | Result and scope |
| --- | --- |
| Targeted SORT/Artifact/HYBRID native tests | 32/32 passed, covering parent postings, empty/NULL arrays, heap/mmap, synchronous/asynchronous reload, missing validity, version boundaries, and the then-current floating-selection restriction |
| Full `index_tests` | 13,245 tests: 13,232 passed, 11 skipped, 2 failed because this machine has no DISKANN registration: `LegacyIndexLoadTest.DiskEstimateSelectsNativeBackendBeforeReadingEngineObjects` and `ArtifactBuilderInputTest.PreparedFilesRejectMissingAndEmptyPaths`. This is not an all-green result |
| Build/expression integration | 27/27 passed using a temporary binary linked from two compiled integration objects and the `index_tests` link configuration. Integer/VARCHAR explicit SORT and HYBRID verified the actual SORT selector through binlog → materializer → build → publish → load. Production `ExecuteQueryExpr` results were compared with raw evaluation and independent expectations |
| Go parameter checks | Targeted STL_SORT/scalar parameter tests passed. The full package is affected by missing local DISKANN: the direct DiskANN checker and `Test_CheckAutoIndex/normal_case,_diskann` fail. Their tests and production logic were not changed |
| Go Proxy DDL | Index/checkTrain regressions passed; ordinary ARRAY's old rejection assertion was updated, with assertions retaining floating rejection at that stage |
| Go DataCoord | New create/restore/dispatch gates and version-manager regressions passed, along with existing CreateIndex, RestoreIndexes, and index-task suite regressions |
| Go `pkg/common` | Full package passed, including scalar engine version-clamp regressions |
| Static checks | `git diff --check`, gofmt, clang-format on changed C++, and local design-document links checked |

Go tests used `-tags dynamic,test -gcflags="all=-N -l" -count=1`. Local pkg-config and rpath settings selected source headers and the newly built core because the installed headers/core were stale. C++ `plan.pb.*` also lagged the existing `plan.proto` and was regenerated with the repository's protoc; this feature did not change proto sources. These were local test preparations, not extra deployment requirements.

The full `all_tests` target did not build: untouched `LoadAdmissionControllerTest.cpp` uses `std::jthread/stop_token`, unavailable in this macOS standard library. Successfully compiled ARRAY integration objects were linked separately. A pre-existing batch-test helper also has a cross-DSO RTTI issue: it returns `ColumnVector`, but executable-side `dynamic_pointer_cast` fails. Neither the helper nor production RTTI was changed. New query tests use the production entry point to verify final row matches, including NULL exclusion under NOT; Reader contract tests independently check validity. These results do not establish that the full expression suite passes.

Real mixed-binary upgrade/downgrade, SDK/distributed E2E, storage fault injection, and performance measurements were not run. Those release gates remain outstanding, including the build-workers-first prerequisite. No wire error codes changed. Create/dispatch not-ready errors remain System/retriable, and corrupt artifacts remain `DataFormatBroken`. The existing path carrying build-boundary `Unsupported` through cgo to scheduler failure classification was traced in code.

## 14. Superseded finite floating ARRAY extension validation

Date: 2026-10-21.

Historical validation of an earlier scope, superseded by the review follow-up in section 16. These results do not describe the current floating ARRAY policy.

- The complete Proxy fieldvalidator package passed. New full-`Validate` entry tests reject NaN/±Inf for FLOAT/DOUBLE and accept finite values/±0.
- Targeted DataNode import tests passed for ordinary and recursive ARRAY floating leaves. Actual `importFile` and pre-import `readFileStat` reject invalid input before syncing imported data.
- Targeted Go STL_SORT checker, DataCoord type/version gate, and Proxy `parseIndexParams` tests passed.
- SORT/HYBRID native regressions passed 19/19. Explicit SORT and HYBRID→SORT reject non-finite values; finite floating data queries correctly after legacy and V3 synchronous/asynchronous reload.
- Binlog build integration passed 2/2. Finite FLOAT/DOUBLE preserve parent postings. NaN/±Inf fail binlog → materializer → SORT builds and leave sessions unpublishable, through both index entry points.
- `make lint-fix` completed with zero issues in the root, pkg, and client modules; `internal/core/run_clang_format.sh` and `git diff --check` completed successfully.
- Real multi-node SDK E2E was not run. Direct snapshot file copying does not scan values; new SORT builds still validate stored data.


## 15. CI target versions and SDK compatibility coverage

Date: 2026-10-22.

The Go SDK CI failure on `887e12cd58` had two causes: SDK tests still expected
ARRAY STL_SORT to be unsupported, and the Helm values forced scalar target v5
with `forceRebuildScalarSegmentIndex=true`. The new binary correctly rejected
explicit ARRAY SORT because its resolved build target was below v6. This was
not evidence of an old QueryNode or a SORT build/query failure.

Regular current-binary E2E/nightly values now force target v6. The supported
scalar-index test includes ordinary ARRAY fields, and SORT is removed from the
unsupported ARRAY index list. A dedicated nonparallel SDK test temporarily
selects v5 and v6 through the management API and restores both configuration
settings afterward. It asserts rejection without index metadata at v5,
explicit SORT success at v6, and matching membership/any/all/NOT/empty-query
results for HYBRID at both targets, with row counts above the indexing threshold.
Low- and high-cardinality fields are covered through the public AUTOINDEX
entry, which selects internal HYBRID. Native selector tests verify the
physical BITMAP/INVERTED/SORT choices; the public HYBRID description cannot
prove its concrete backend.

The runtime matrix requires a writable EtcdSource-backed management API. Local
validation uses the branch binary, local FS, Woodpecker local, and an independent
Docker etcd. The earlier embedded-etcd setup can serve SDK requests but lacks a
writable remote configuration source because parameters initialize before the
embedded server. No production workaround for that environment issue is added.

Both artifact target versions retain the existing floating ARRAY ingestion behavior. An explicit low-version SORT request still returns the existing
retriable ServiceNotReady error. Distinguishing a fixed administrator override
from a transient rolling-upgrade condition remains a separate follow-up.
This target-version matrix does not establish mixed-binary upgrade safety.


Validation for the CI follow-up passed on the local native branch binary:
`TestCreateSortedScalarIndex`, `TestCreateUnsupportedIndexArrayField`, and
`TestArraySTLSORTVersionCompatibility` (all four v5/v6 SORT/HYBRID subcases).
Each successful matrix build indexed 1,200 rows and verified membership,
any/all, NOT, and empty-all results. The v5 rejection leaves no index metadata;
the management API confirms restoration to the original target and force flag.
Both native selector regressions passed, and all 18 modified Helm values parsed
with scalar target v6. This is local targeted SDK coverage, not the complete
CI SDK suite or a real mixed-binary upgrade.

## 16. Review follow-up: compatibility and build complexity

Ordinary ARRAY SORT is limited to BOOL, integer and VARCHAR elements. Explicit floating ARRAY SORT is rejected in Go admission and the native build adapter; high-cardinality floating HYBRID remains INVERTED at v6. The proposed finite-value ingress and builder guards are removed, preserving compatibility with legacy floating payloads and existing Struct behavior. Section 14 records superseded experiments only.

Numeric ARRAY construction no longer reserves `size + row_length` per row. Vector geometric growth avoids repeatedly copying all previously accumulated postings. Reviewer timings are not local validation results.

The existing narrow-integer contains overflow bug is independent of ARRAY SORT and will be handled separately, with raw/index and NULL/NOT conformance coverage.


Targeted validation on 2026-10-23:

- SORT/HYBRID native regressions: 20/20 passed, including FLOAT/DOUBLE selection at v5/v6, both cardinality branches, explicit floating ARRAY SORT rejection, and persistence/query checks.
- Binlog/build boundary regressions: 4/4 passed in a temporary executable linked from the updated ScalarBuildSessionTest object and the index_tests link configuration. This covers parent-row postings, old-target rejection, floating ARRAY admission, and existing Struct routing.
- The synthetic numeric ARRAY test builds 40,000/80,000/160,000 rows with 10 increasing unique INT64 values per row. Build-only times on this machine were 5/8/11 ms, excluding input preparation, serialization and loading. It also reopens each artifact and verifies selected parent-row postings. This is not a before/after comparison or an end-to-end production benchmark.
- Proxy fieldvalidator full package, targeted STL_SORT checker/DataCoord gate/Proxy DDL tests, SDK testcase compilation, targeted checker/fieldvalidator lint (0 issues), changed-file formatting and git diff checks passed.
- The full SDK suite, real partial-update/binlog-restore E2E, distributed testing and mixed-binary upgrades were not rerun for this follow-up.
