# 共享标量契约框架独立静态审查

Date: 2026-09-15

Scope: `test_utils/ScalarTestData.h`, `ScalarDataSets.cpp`,
`ScalarReaderFactory.h/.cpp`, `ScalarReaderBackends.cpp`,
`ReaderTestDriver.h`, `FilterTestDriver.h`, `TestArtifactIO.h/.cpp`, and
`ArtifactTestUtils.h`. Review followed `~/.claude/CODE_REVIEW_GUIDE.md` and the
frozen lifecycle/specialized inventories. No build, test listing, or test was
run.

No actionable framework finding remains in the reviewed snapshot.

## 注册与选择

- `DataCatalog` stores only typed descriptor functions. The only `make_data()`
  calls under the framework/contracts tree are inside actual GTest runners;
  the large cardinality, long string, random byte, WKB, JSON, Text, and ARRAY
  values are not generated during static registration.
- `ScalarDataSets()` is the single catalog and invokes the Text/candidate and
  JSON registration functions exactly once. Dataset keys include input C++
  type, and duplicate type/name pairs fail at registration.
- `BackendCatalog` is the single profile catalog. Source construction accounts
  for 434 unique profiles: 136 primitive row, 64 nested element, 128 ARRAY row,
  24 Text, 18 Ngram, 4 Spatial, 8 JsonFlat, and 52 projected JSON.
- Selection checks C++ input type, input shape, coordinate domain, nullability,
  logical value type, and production-derived capability before case-local
  selectors. Ordinary `For<T>` remains Scalar/Row-only, so WKB, JSON,
  ArrayView, projected Ngram, and nested data cannot enter primitive predicate
  cases. All-valid datasets intentionally select nullable and non-nullable
  profiles; null-bearing datasets select nullable profiles only.
- Both filter and observation drivers create one `FilterParam` per resolved
  backend and reject empty or duplicate expansions. Profile selection happens
  without generating dataset rows.

## 输入与所有权

- `ScalarTestData` owns string bytes, ARRAY bytes/offsets, projected strings,
  validity and batches. `ScalarTestInput` binds views only after owners reach
  their final location, owns `vector<bool>` replacement storage, preserves
  packed validity subview offsets across zero/nonzero batches, and validates
  absent-validity and total batch length.
- Both ordinary ARRAY-row generators use `{0, 2, 0, 2, 0}` batch sizes. The
  total remains four rows while exercising leading, internal, adjacent-boundary,
  and trailing zero batches without changing values or validity.
- `ReaderObservationCases` generates independent deterministic expectation and
  build owners. It destroys the input adapter and build data before observing
  the Reader. `ReaderBackend::Create` also destroys Artifact, V3 persisted maps,
  sink, and source before returning, so each observation includes input and
  persistence lifetime checks.
- All registered generators were checked as deterministic across the two
  calls. Random-byte data uses a fixed local seed. Dataset metadata and row
  counts are regenerated rather than shared.

## 构建/打开与能力声明

- Backend fields provide normalized build/load field type, value type,
  ARRAY element type, nested/domain state, nullability, and per-data completion.
  Text consume, V3 serialize, Hybrid resolved-family, JSON projection wrapping,
  Spatial row count, local staging, and mmap-request modes remain in this one
  profile representation.
- Projected and Ngram load params carry the production `INDEX_TYPE`. JSON path
  and `non_exist_offsets` are completed from dataset metadata; the wrapper
  retains row count and projection completeness separately from scalar
  validity.
- Serialize open resolves the actual family from the persisted artifact,
  verifies it belongs to the profile's declared loader set, annotates JSON
  completeness from the actual source, derives caps from that concrete loader,
  opens it, and checks all ten caps plus domain and logical value type before
  returning. Consume mode performs the same validation against its one declared
  loader.
- Hybrid capability selection is conservative across every possible delegate;
  query policy remains a separate per-operation contract so optimization
  decline does not suppress direct result testing.

## 测试产物 IO

- The V3 sink owns copied entry bytes and typed metadata. The V3 source keeps a
  const artifact owner reference only for the duration of loader open; loaders
  must retain their own state before the source and maps are destroyed.
- Single-file writes use same-directory staging and rename. Multi-entry file
  reads validate all entries before staging. Directory reads validate every
  name/entry before writing, stage every target, and publish with backup/restore
  rollback. RAII removes unpublished staging, while successful publication
  removes backups.
- `ArtifactTestUtils` applies the same actual-family resolution, JSON
  completeness annotation, ten-field caps comparison, domain, and logical
  value checks to focused generation/corruption tests.

Static clang-format dry-run and `git diff --check` pass for the reviewed shared
files. Runtime registry order, every profile's concrete builder/loader
availability, and filesystem failure behavior remain subject to the centralized
validation gate.
