# 剩余生命周期和标量族契约覆盖

日期：2026-09-15

源码快照：本地实现 `af90d32547`、契约 stack
`b7cfc84da8` / `f672fd5967`、固定 master
`a876f471053edb2f68a06a9894afee9810ea7906`。这仅是源码计数。
集中式 configure/build/list/run 门禁保持关闭。

## 中央框架和 profile 矩阵

`ScalarReaderFactory`、`ScalarTestData`、`ReaderTestDriver` 和
`TestArtifactIO` 现为 build 输入、Artifact、V3 持久化或直接消费、具体 loader 选择和读取器观察提供一条有类型的惰性路径。观察运行器从独立所有者构建，在查询前销毁该所有者，`ReaderBackend::Open` 将全部十个 caps 字段与从实际持久化 Artifact 选择的具体 loader 比较。Hybrid 选择不缓存在共享描述符状态中。

The single central catalog declares 434 profiles:

| 组 | Profile | 形状和模式 |
|---|---:|---|
| Primitive row scalar | 136 | 8 types x Bitmap/Sorted/Inverted/Hybrid x nullable/non-nullable x heap/mmap, plus Marisa and FM VARCHAR |
| Flattened nested elements | 64 | 8 types x Bitmap/Sorted/Inverted/Hybrid x heap/mmap; non-null Element domain |
| Ordinary ARRAY rows | 128 | 8 element types x Bitmap/Sorted/Inverted/Hybrid x nullable/non-nullable x heap/mmap |
| Text | 24 | VARCHAR/STRING/TEXT, V5/V7, standard/Jieba, persisted or V7 RAM consume, nullable/non-nullable |
| Ngram | 18 | scalar VARCHAR/STRING/TEXT min-gram variants plus two projected JSON VARCHAR profiles |
| Spatial | 4 | RTree heap/mmap-requested, nullable/non-nullable |
| JSON flat | 8 | V5/V7 x heap/mmap x nullable/non-nullable |
| JSON projected | 52 | supported scalar and ARRAY cast/family pairs x heap/mmap x nullable/non-nullable |

逻辑 field/value/array-element 类型、输入形状、row/element 域、可空性和打开模式均显式指定。投影 load params 携带生产 `INDEX_TYPE`；JSON path、投影存在性 offset、RTree 行计数、本地 staging 和 mmap 根目录按调用惰性完成。既有标量 `For<T>` 选择仍仅适用于 Scalar/Row。

## Task 5 源码测试

| 契约源码/组 | 参数展开后的 GTest | 覆盖 |
|---|---:|---|
| `RegistryTest.cpp` | 442 | 8 protocol/routing cases plus one registration case for every 434 profile |
| `ArtifactBuilderTest.cpp` | 262 | 70 scalar lifecycle configurations and 192 ordinary ARRAY configurations |
| `ReaderConvertibleTest.cpp` | 7 | null/unsupported/success/error/null-result ownership plus real scalar/Text paths; every outcome is a separate GTest |
| Bitmap/Sorted/Inverted/Hybrid/Text/RTree artifact tests | 59 | legacy/V3 round trips, selectors, required parts, public relation and payload corruption |
| Marisa/FM artifact tests | 32 | legacy behavior, heap/mmap ownership and current-format required-part/relation/payload checks |
| NamedBuffer/LocalDirectory tests | 37 | sink 9, source 16, local directory/guards 12 |
| **Task 5 总计** | **839** | 包含两个既有 Hybrid 能力守卫；净新增计数为 837 |

实际注册和结果计数等待门禁开启后的 GTest 列举。

### Builder、ARRAY、嵌套和 selector 行为

- Real scalar builders end before their returned Artifact is serialized. The
  input adapter, values, strings, validity, and batches are destroyed first;
  the opened reader is then queried after Artifact/source/persisted buffers are
  gone. Every scalar `InputSpec` has no side input.
- Ordinary ARRAY covers bool/int8/int16/int32/int64/float/double/VARCHAR across
  Bitmap, Sorted, Inverted, and Hybrid, nullable/non-nullable and heap/mmap.
  Each case includes a null row, valid empty array, duplicate elements and
  multiple batches. `NullReader` distinguishes null from valid empty, while In
  and Range verify row-domain any-element postings and per-row de-duplication.
  int8/int16 use the production-required int32 physical ArrayView storage.
- Nested profile 接受已扁平化且无 validity 的有类型 element，报告 Element 域和 nested caps，不虚构 row offset 或 Segment 投影。
- Hybrid tests cover below, exactly-at, and above threshold; null payloads,
  duplicate values, and batch boundaries; numeric bitmap/sort, VARCHAR
  bitmap/inverted, and ARRAY/nested bitmap/inverted delegates. Both V1/V2
  selector entry and V3 typed metadata resolve through the production loader.
  Each threshold/type/shape configuration and each constructor outcome is an
  independent parameter; a focused case also confirms the Hybrid envelope is
  not reader-convertible.

### Artifact generation 和无效输入

- V1/V2 round trips cover Bitmap numeric/string, Sorted numeric/string,
  Inverted directory, Marisa CSR reconstruction, Hybrid low/high envelopes,
  and persisted Text validity. FM asserts its exact legacy serialization
  rejection.
- V3 broad build/open is exercised by every query observation. Focused family
  tests additionally inspect each public file/meta inventory and independently
  reject a missing required part, an inconsistent public metadata relation,
  and invalid payload bytes for each distinct local persisted shape.
- Bitmap explicitly rejects truncated packed validity and row/element domain
  disagreement. Sorted separately rejects bad numeric count, incomplete and
  invalid reverse-offset state, unsupported string version and invalid string
  offsets. Inverted separately rejects empty/reserved/missing file inventory,
  null-sidecar relation and out-of-range null offsets.
- Text RAM is the real successful `ReaderConvertible`; ordinary scalar
  Artifacts reject consumption without serializing. Tracking artifacts verify
  shell and dependency ownership on every conversion outcome.
- RTree checks its unique `.bgi` archive, null sidecar relation, missing
  declared archive, invalid archive, completed `num_rows`, and reader lifetime.
  `SpatialRTreeMmapRequested` is deliberately named as a request: the current
  loader remains heap-backed and the spatial query suite asserts zero file
  bytes, so it is not counted as a physical mmap implementation.
- JSON 投影成功路由由 JSON 查询套件和生产注册矩阵在每个中央声明 profile 上执行。Registry 测试另行保留两个 master 拒绝边界：Bitmap/DOUBLE 和 Sorted/BOOL 投影 cast 返回精确 `DataTypeInvalid`。

### L1 内存 IO 和清理

- NamedBuffer sink tests cover V1/V2 generation, binary/empty/local-file
  ownership, stats, Finish/Data/Take state, unsupported raw/meta operations,
  duplicate/null/missing inputs, and failed-state behavior.
- NamedBuffer source tests cover independent manual three-slice assembly as
  well as sink-produced slices, logical reads, exact concatenation order,
  single/directory publication, pre-publication failure preservation, basename
  collision, empty requests, and same-directory staging cleanup.
- LocalDirectory and guards cover unique owned children, configured-parent
  preservation, last-owner cleanup, canonical descendant and symlink escape
  boundaries, local-entry move/release, mapping move/release, and checked fd
  close. All paths live under `std::filesystem::temp_directory_path()` for the
  final isolated-TMPDIR audit.
- Extracted `TestArtifactSource` validates all requested entries before staged
  multi-file publication, atomically replaces single targets, restores existing
  destinations if publication fails, and removes its staging/backup files.

## 固定 master 处置

- `WriterMemoryBudgets` is a private Tantivy tuning constant rather than a
  Reader/Artifact contract. The current Text builder uses the current binding
  constant; no test freezes a historical 500 MiB policy value.
- Raw sealed finalization maps to the one-shot Text builder lifecycle: Build
  finalizes owned engine/null state, then serialization and query succeed after
  input destruction. Persisted Text V1/V2 round trip covers nullable validity;
  multi-batch nullable offsets are also exercised by query observations.
- RTree build/load, invalid WKB, empty geometry, committed Count, filenames and
  local serialization map to the spatial datasets plus its focused artifact
  tests. Public `num_rows` completion is centralized in the backend profile.
- Text/RTree upload path naming, remote/local mixed paths, V1 slice translator
  accounting, sliced remote null-offset objects, V1/V3 FileManager transport,
  and cancellation require #67 FileManager/ChunkManager/translator services.
  They are outside the approved local in-memory Artifact scope.
- RTree injected write/close failure hooks, allocator/OOM, concurrent writer
  starvation, process termination and historical rollback matrices require
  dedicated fault/concurrency machinery and are not ordinary lifecycle cases.
- The missing-physical-slice corrupt input reaches an unchecked legacy
  `Assemble` dereference. A process-isolated assertion was blocked twice by the
  execution tool and, per root direction, was not retried or bypassed. Valid
  manual slices and malformed metadata remain covered; source evidence is in
  `25-storage-artifact-issues.md`.
- Segment, executor/refinement, materializer, remote build service, vector,
  growing publication, disk-engine handles and compatibility wrappers remain
  in #66/#67 or deleted-API scope.

## 静态验收状态

Owned shared/lifecycle sources have been formatted with clang-format 12 and
`git diff --check` is clean. The Task 1 shared framework passed independent
review by the pattern contract owner. The Task 5 lifecycle/CMake sources passed
independent review by the scalar contract owner, including the final bounded
concurrent-registry handshake. Storage files and their independent manual
three-slice fixture also passed source review. No compile or runtime claim is
made before the centralized gate opens.
