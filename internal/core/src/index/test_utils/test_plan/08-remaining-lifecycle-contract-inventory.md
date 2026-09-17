# 剩余 #64/#65 生命周期和标量系列契约清单

日期：2026-09-15

这是只读源码清单。准备期间未修改源码、CMake、构建缓存、二进制或测试进程。

## 固定的实现范围

- PR #64，`enhance: [Segcore 2] index contracts and L1 artifact layer`，在 `b7cfc84da8ad03ca3b33daca4242a73cacfa614c` 处为 open/non-draft，基于 `4efc2b3b6ee779452b28fe0defd109ba90f58a5d`。
- PR #65，`enhance: [Segcore 3] scalar index family implementations`，在 `f672fd596774bb25f47a138a5ce59983932954d3` 处为 open/non-draft，基于 PR #64。
- 两个远程 head 分支均存在，并通过 GitHub branches API 报告 `protected=false`。本地 `SegcoreRefactor/6-tests-index` 分支没有 upstream 或 remote branch，位于 `af90d325473284279ae680b14ddde1f9b9c82464`。
- 对本清单文件而言，本地 scalar-family tree 与 PR #65 相同。PR #64 与本地 HEAD 之间，仅 `Meta.h` 和 `ParamUtils.h` 在检查的 #64 index/artifact 范围内变更；下述 contract、Registry、storage/artifact API 未变。
- 工作树已包含未提交 reader-contract 工作和 CMake/runner 变更，必须保留。其已验证基线为 7,960 个测试、7,816 个通过和 144 个保留生产失败。

PR 描述与已提交的 contract README 一致确认层边界：#64 负责 contracts、typed registry 和 L1 artifact IO；#65 负责 sealed scalar builders/artifacts/loaders/readers。#66 vector/growing engines，以及 #67 materialization、Segment、execution、build service、remote publication 和 consumer selection 均排除。

## 现有覆盖与剩余缺口

现有 7,960 个测试覆盖 V3 `Build -> Artifact::Serialize -> loader open -> Reader`：原始行域 bitmap/sort/inverted/hybrid 矩阵、Marisa 和 FM，包含 heap/mmap-request 和 nullable/non-nullable 配置。它们全面覆盖 scalar predicates 和 pattern queries、小型 int64 Lookup 集、系列能力守卫、Open 后 source/artifact 销毁及正常路径临时文件清理。

它们未隔离以下普通契约：

1. Artifact serialization 前结束的 builder input 和 builder 生命周期；
2. `IReaderConvertible::FromArtifact` 所有权和 failure 行为；
3. Registry 协议和 typed-table 隔离；
4. NamedBuffer sink/source 状态、slicing、本地 materialization 原子性和 LocalDirectory 所有权；
5. V1/V2 scalar artifact serialization/load 分支；
6. 行域 `ScalarBuildInput<ArrayView>` 和 typed flattened nested-element builders；
7. 作为 artifact contract 的 Hybrid threshold/selector 行为；
8. 每个普通 scalar loader 的 required-entry/typed-metadata 损坏。

## #64 文件到测试映射

| 生产接口 | 计划普通测试 | 处置 |
|---|---|---|
| `IArtifactBuilder.h`、`ScalarBuildInput.h` | 默认空 `InputSpec`；多个和零大小 batches；缺失/存在 validity；Build 消费 builder，但返回 Artifact 不拥有借用 numeric/string/ArrayView input | 使用真实 scalar builders 添加 `contracts/build/ArtifactBuilderTest.cpp` |
| `IReaderConvertible.h` | null Artifact -> 精确 `UnexpectedError`；缺失 capability -> 精确 `Unsupported` 且零次 Serialize 调用；成功保留 Reader dependency 至 Reader 销毁；converter throw 保留精确 `SegcoreError`；null Reader -> 精确 `UnexpectedError`；shell 恰销毁一次 | 以小型 tracking types 添加 `contracts/build/ReaderConvertibleTest.cpp`；通过同一 helper 添加代表性真实普通 scalar rejection 和 Text RAM success |
| `contracts/Registry.h`、`Registry.cpp` | unknown Lookup/Create；typed builder tables 不 alias；params 原样传递；保留 factory exception；empty/duplicate factory rejection 保持原 entry 完整；loader derive/open dispatch 独立；repeat-safe 和 concurrent 独立 registration/lookups | 添加 `contracts/RegistryTest.cpp`；使用 test-only family keys，不使用 reset API |
| 生产 scalar registrations | 每个中心声明的生产配置一个参数，验证其 builder entry 和 resolved loader entry；Hybrid 有 builder 且有意没有 Hybrid loader；没有测试 self-registers 生产 family | 向 `RegistryTest.cpp` 添加 parameterized 生产 registration smoke；专用负责人贡献其中心配置 |
| `Artifact.h`、`ArtifactStats.h` | 系列实现时的两种 serialization mode；Finish stats 拥有 file names/sizes 并报告 serialized bytes | 由真实 family artifact tests 和 NamedBufferSink tests 覆盖；不做仅 getter 的 mirror test |
| `FileSink.h/.cpp` NamedBuffer implementation | binary 和 empty entries、借用 local file 保留、Finish/Take/Data/stats、before-Finish 和 after-Finish state、Unsupported meta/raw operations、failed-state behavior | 添加 `storage/artifact/FileSinkTest.cpp` |
| `FileSource.h/.cpp` NamedBuffer implementation | names/HasEntry/EntrySize/ReadEntry；小型手工 sliced-entry assembly 和 malformed/missing slices；精确 concatenation order；missing entry；single-file 和 multi-directory 同目录 staging、保留 existing destinations、collision rejection、staging cleanup；unsupported DiskEngine open 和 absent V3 meta | 添加 `storage/artifact/FileSourceTest.cpp` |
| `LocalDirectory.h`、相关 `LocalFileUtils.h` guards | 唯一拥有 child、保留 configured parent、`Owns` 接受 descendants 并拒绝 self/parent/sibling-prefix/symlink escape、最后 owner cleanup、在不做 fault injection 时可观察的 move-only local entry/mapping/descriptor guard checks | 添加 `storage/artifact/LocalDirectoryTest.cpp`；主要通过 FileSource 执行 file publication helpers |
| `FileSourceUtils.h`、`ParamUtils.h`、`ResourceUsageUtils.h`、`LoadOptions.h`、`NamedBuffer.h` | typed required metadata、entry-name validation、alias/type parsing、saturating arithmetic、option preservation、仅在真实 loader/IO test 使用时的 buffer ownership | 仅在计划 family/IO path 使用时添加聚焦用例；避免仅重复 inline getters 的测试 |
| `V1DiskSink`、`V3PackedSink`、`V1RemoteSource`、`V3PackedSource` | remote naming/upload、packed container 和 cancellation | 暂缓：需要 #67 FileManager/ChunkManager/service transport 或 cancellation infrastructure，不属于请求的小型 in-memory L1 path |
| `DiskEngineFileHandle` | disk-vector backing lifetime | 暂缓至 #66 vector scope |
| `IGrowingIndex.h`、query contracts | growing publication 和 query methods | 归其他 inventories 所有或排除的 #66；不在此重复 |

本地 test-only `TestArtifactSource` 必须继续遵守生产 FileSource atomic-publication 承诺。其已实现的 staging/rollback 逻辑将被提取，不得弱化。

## #65 普通系列矩阵

### 通用 builder/artifact 生命周期

为每个不同 builder template/family 和证明 input independence 所需的 nullability 添加一个 parameterized 生命周期用例：

- bitmap、sort、inverted 和 Hybrid：bool、int8、int16、int32、int64、float、double 和 string_view；
- Marisa 和 FM：string_view；
- 包含 Text RAM/persisted 生命周期，因为 Text 是唯一的 scalar `ReaderConvertible`；Text query 语义仍归专用负责人。

测试从惰性安全小数据集构建，销毁 `ScalarTestInput`、拥有的 values/string storage、validity、batches 和 builder，然后 serialize 并 open。销毁 Artifact、sink/source 和 caller buffers 后，检查 Reader metadata 及一个小型 contract query。它避开已知 NUL 和 signed-zero 生产缺陷，以便诊断生命周期；现有 red tests 继续保留这些正确性失败。

每个 scalar builder 的 `InputSpec().side_inputs` 为空；仅 #66 vector builders 覆盖它。通过中心声明的 profiles 验证 scalar 承诺，不添加 scalar-only production base class。

### Generation 与加载器边界

对 V1/V2 使用生产 `NamedBufferSink/Source`，对逻辑 V3 使用提取的 `TestArtifactSink/Source`。覆盖每种不同持久化形状：

| 系列/产物形状 | V1/V2 | V3 | 基础无效产物用例 |
|---|---:|---:|---|
| Bitmap numeric 和 string | 往返 | 已较广；检查 typed meta | 缺失 meta/data；错误 count/validity；nested 不一致 |
| Sorted numeric | 往返/重建 aux | 已较广 | 缺失/无效 data 或 count；不完整/无效 V3 aux |
| Sorted string packed dictionary/postings | 往返/重建 offsets | 已较广 | 缺失 entry；损坏 version/posting/offset data |
| Inverted Tantivy directory + null sidecar | 往返 | 已较广 | 空/缺失/无效文件列表、has-null 不匹配、无效 engine bytes/null offsets |
| Marisa trie + IDs（+ V3 CSR） | 往返/重建 CSR | 已较广 | 缺失 trie/IDs、畸形 IDs、不完整/错误 CSR metadata/bytes |
| FM blob + nullable meta | 明确 V1/V2 `UnexpectedError` | 已较广 | 缺失/错误 row/nullable meta、缺失/损坏 blob、null-bitmap 不匹配 |
| Hybrid envelope | selector entry 加 selected-family 往返 | selector meta 已查询；检查 selector | 通过聚焦产物测试的 null/unsupported inner constructor；selector-to-family load 解析 |
| Text | 中心声明的持久化 V1/V2/V3 和 RAM consume | query owner 提供配置 | RAM Serialize 拒绝、持久化/consume 读取器生命周期 |

每个错误测试仅捕获被测 loader/serialization 调用，检查当前构造点产生的精确 `SegcoreError` code。Build failure 不能满足 loader-error 期望。仅为创建指定无效公开 entry/meta 关系而修改 layout bytes；测试不复制私有 codec 作为 oracle。

### ARRAY 与 nested 输入

这些是普通 #65 builder 形状，仍在范围内。

- 行域 ARRAY 对 bitmap、sort、inverted、Hybrid 使用 `ScalarBuildInput<ArrayView>`。一个小型拥有的 adapter 创建 bool storage、int8/int16 的物理 int32 storage、int32/int64/float/double storage 和显式长度 string storage。矩阵覆盖 null row、有效 empty row、单行重复 element、多个 batch 和每个支持 element type。打开的 Reader 具有 element `ValueType`、`Domain::Row`、等于 array rows 的 Count，并且对多值行坐标没有 `ScalarValueReader`。一个 In/Range 结果验证行 posting 语义和逐行去重。
- Nested ARRAY 使用有类型平坦化 `ScalarBuildInput<T>`，逻辑 `field_type=ARRAY`、`array_element_type/value_type=T`、`nested=true`。值已是 materialized element coordinates，validity 缺失/全 true。Bitmap、sort、inverted 和 Hybrid 后端配置覆盖全部支持 primitive element type。读取器报告 `Domain::Element`、element Count、`caps.nested=true`、`caps.exact=false`；不引入 row projection 或 offsets。int8/int16 明确覆盖行路径的 ArrayView physical stride。
- Hybrid ARRAY 当前对 row ArrayView 和 typed nested inputs 都在 threshold 以下选择 bitmap，在 threshold 处或以上选择 inverted。较新的仅 master 的 version-dependent nested-sort compatibility policy 不属于 PR #65，不作断言。

### Hybrid selector

在 caps 之外扩展现有 `HybridIndexBuilderTest.cpp`：

- threshold 以下选择声明的 low family；
- 恰在 threshold 和高于 threshold 时选择声明的 high family；
- null 不增加 distinct values，duplicates 不增加 distinct values，且跨 batch boundaries 累积 distinct values；
- 覆盖 numeric low bitmap/high sort、string low bitmap/high inverted、普通/nested ARRAY low bitmap/high inverted；
- 将 `INDEX_TYPE` 检查为一字节 V1/V2 entry 和 typed V3 metadata，解析对应具体 loader，并运行安全 result query；
- 普通 scalar/Hybrid artifacts 保持不可转换，不获得 forwarding Reader/Loader。

Master cases tied to the retired runtime-forwarding `HybridScalarIndex`, #67
factory/Segment behavior, inference of old standalone files, or rollback
compatibility are explicitly not applicable to this PR #65 builder contract.

## Pinned-master mapping

Applicable behavior retained from master
`a876f471053edb2f68a06a9894afee9810ea7906`:

- Bitmap/Bool/Sorted/Marisa/Inverted/FM scalar query tables are already covered
  by the 7,942 parameterized predicate/pattern tests and are not recopied.
- `BitmapIndexArrayTest`: direct element-domain build/load metadata, nullable
  rows before valid elements, int8/int16 ArrayView stride, sorted nested query,
  row ARRAY not exposing raw scalar lookup, and Hybrid low/high selection map to
  the ARRAY/nested and Hybrid tasks above.
- `InvertedIndexArrayTest`: nested element-domain all-valid null semantics maps
  to the nested/Null contract; executor ArrayNotEqual/refinement does not.
- `StringIndexSortTest`, `StringIndexTest`, `FMIndexTest`, `InvertedIndexTest`:
  serialization/load, mmap ownership, nullable sidecars, and public corruption
  checks map to the generation/loader tasks. Query tables, pattern routing and
  random-byte semantics are already covered.

Not applicable or deferred with reason:

- old `IndexFactory`, `ScalarIndex<T>`, in-place mutable index, codec wrapper,
  `InApply*`, runtime Hybrid forwarding, and private layout APIs were deleted by
  #65; only their surviving public Reader/Artifact behavior is carried forward;
- Collection/Schema/Segment factories, ArrayOffsetsSealed, LoadResource,
  expression fallback/refinement, cached execution, and unified remote
  upload/load are #67;
- vector, growing append/commit/pin, old-pin/deferred-iterator and Add/Search
  concurrency are #66;
- benchmarks, randomized fuzz/performance machinery, allocator/OOM injection,
  process-termination cleanup, remote cancellation, corrupt historical-version
  compatibility matrices, and cross-version rollback are specialized/custom
  suites. Deterministic small corrupt current-format inputs remain in scope.

## Frozen shared API proposal

There remains one backend catalog. No contract owns a parallel backend/profile
registry.

```cpp
enum class BackendInputShape {
    Scalar,
    ArrayRows,
    NestedElements,
    SpatialWkb,
    JsonDocument,
    JsonProjected,
};

enum class BackendOpenMode {
    Serialize,
    Consume,
};

struct BackendCaseMetadata {
    size_t row_count{0};
    Config values = Config::object();
};
```

`BackendSpec` keeps the current fields and adds:

```cpp
BackendInputShape input_shape{BackendInputShape::Scalar};
DataType field_type{DataType::NONE};       // inferred for primitive Scalar
DataType value_type{DataType::NONE};       // inferred for primitive Scalar
DataType array_element_type{DataType::NONE};
Domain expected_domain{Domain::Row};
BackendOpenMode open_mode{BackendOpenMode::Serialize};
std::function<void(Config&, const BackendCaseMetadata&)>
    complete_build_params;
std::function<void(Config&, const BackendCaseMetadata&)>
    complete_load_params;
std::function<storage::ArtifactPtr(storage::ArtifactPtr,
                                   const BackendCaseMetadata&)>
    wrap_artifact;
```

`BackendCatalog::Add<InputT>(BackendSpec)` stores the C++ build-input
`std::type_index`. Primitive Scalar profiles infer field/value type from
`InputT`; nonprimitive/logically different profiles must state them. Selection
APIs are:

```cpp
template <typename T>
std::vector<ReaderBackend>
For(bool ReaderCaps::*capability, bool requires_nullable = false) const;

template <typename T>
std::vector<ReaderBackend>
All(bool requires_nullable, Domain domain = Domain::Row) const;

template <typename InputT>
std::vector<ReaderBackend>
ForInput(BackendInputShape shape,
         bool ReaderCaps::*capability,
         bool requires_nullable,
         std::optional<DataType> logical_value_type = std::nullopt,
         std::optional<Domain> domain = std::nullopt) const;
```

Existing `For<T>` additionally requires input type `T`, shape `Scalar`, logical
type `T`, and Row domain, so JSON-projected predicate backends and typed nested
profiles cannot enter the existing exact row filter cases. Specialized cases
request their strong shape explicitly. This is data eligibility, not a duplicate
capability table; capability values still come from production LoaderRegistry.
Text and scalar Ngram use the ordinary `Scalar` shape because their physical
input is the same values/validity stream; production caps keep them out of
predicate/pattern suites while Null/Base tests can reuse scalar datasets. JSON
Ngram is distinguished by `InputT=JsonProjectedString`; logical STRING/VARCHAR/
TEXT aliases are additionally matched through explicit field/value metadata.

`ReaderBackend` exposes:

```cpp
BackendInputShape InputShape() const;
Domain ExpectedDomain() const;
DataType ExpectedValueType() const;
BackendOpenMode OpenMode() const;

template <typename InputT>
storage::ArtifactPtr
Build(const ScalarBuildInput<InputT>& input,
      BackendCaseMetadata metadata = {}) const;

IndexReaderBasePtr
Open(storage::ArtifactPtr artifact,
     BackendCaseMetadata metadata = {}) const;

template <typename InputT>
IndexReaderBasePtr
Create(const ScalarBuildInput<InputT>& input,
       BackendCaseMetadata metadata = {}) const;
```

`Create` fills `row_count` from batches when zero, then composes `Build` and
`Open`. Build checks the stored input type, completes build params, invokes the
production typed registry, and applies the optional wrapper. Open either uses
`IReaderConvertible::FromArtifact` or serializes through V3 test IO and opens the resolved
production loader; it completes load params first. Mmap roots and family local
staging parents are chosen lazily from `std::filesystem::temp_directory_path()`
so isolated `TMPDIR` verification remains meaningful.

Generic non-filter cases reuse `FilterParam` at the GTest boundary:

```cpp
template <typename T>
struct ReaderObservationCase {
    std::string name;
    std::string dataset;
    BackendInputShape input_shape{BackendInputShape::Scalar};
    Domain domain{Domain::Row};
    bool ReaderCaps::*capability{nullptr};
    std::optional<DataType> logical_value_type;
    std::vector<std::string> backends;
    std::function<void(const ReaderBackend&,
                       const ScalarTestData<T>&,
                       const IndexReaderBase&)>
        observe;
};

class ReaderObservationCases {
 public:
    template <typename T>
    void Add(ReaderObservationCase<T>);
    const std::vector<FilterParam>& All() const;
};
```

`Add` resolves but does not generate its dataset, selects `All<T>` when the
capability pointer is null or the matching capability-aware overload otherwise,
and emits one lazy `FilterParam` per backend. The runner creates two independent
deterministic datasets: an `expected` copy retained for the callback and a real
`input_data` copy (including distinct string storage). It builds inside an inner
scope, destroys `ScalarTestInput` and `input_data`, then checks the returned
Reader and invokes `observe`. It centrally checks non-null Reader, Count,
ExpectedDomain, ExpectedValueType, and every ReaderCaps field against
`DeriveCaps`. This makes input/source/Artifact lifetime part of all new base,
Null, Value and specialized observation tests without a stateful fixture.

Extract the current private V3 factory implementation, without semantic change,
to `test_utils/TestArtifactIO.h/.cpp`:

```cpp
struct TestArtifactData {
    std::map<std::string, std::vector<uint8_t>> entries;
    std::map<std::string, nlohmann::json> metadata;
};

class TestArtifactSink final : public storage::FileSink {
 public:
    explicit TestArtifactSink(TestArtifactData&);
    // Gen() == V3; existing entry/meta validation and stats behavior.
};

class TestArtifactSource final : public storage::FileSource {
 public:
    explicit TestArtifactSource(const TestArtifactData&);
    // Gen() == V3; existing checked reads and atomic local publication.
};
```

The maps are intentionally mutable test data so a loader test can remove or
corrupt one declared logical entry or metadata value. V1/V2 tests continue to
use the production `NamedBufferSink/Source`.

ARRAY owners remain local to the shared builder-contract test unless a second
test actually reuses them. Candidate-set logic, JSON truth, text analysis and
spatial geometry stay in their contract tests and do not enter this factory.

## Proposed bounded implementation batches

1. Extract TestArtifactIO; generalize the single BackendCatalog and add central
   profiles/datasets requested by the core/specialized owners. Add all new
   source paths to `INDEX_TEST_FILES`/support and remove them from all_tests.
2. Implement `ReaderConvertibleTest`, `RegistryTest`, and L1
   FileSink/FileSource/LocalDirectory tests.
3. Implement real `ArtifactBuilderTest` lifecycle matrix and ARRAY/nested input
   matrix.
4. Extend existing Hybrid builder test and add only family Artifact/Loader test
   files that exercise the generation/corruption behavior listed above.
5. Integrate Text conversion lifecycle after the specialized Text profiles
   exist. Perform static ownership review and formatting/diff checks.
6. Keep the build gate closed until every planned test source from all owners is
   complete. After root opens it, perform the one centralized initial configure/
   build/list/full-run phase; iteratively fix only test/framework/CMake defects,
   retain and document production failures, then repeat the final isolated
   TMPDIR cleanup run.

## Bounded choices for implementation

- Add one public required-entry, typed-metadata relation, and payload-corruption
  example per distinct storage shape. Do not multiply the same malformed bytes
  across every primitive instantiation or reconstruct a private codec in tests.
- Cover V1/V2 once per distinct serialized layout: bitmap numeric and string,
  sorted numeric and string, inverted, Marisa, and Hybrid low/high selectors;
  FM has only the explicit V1/V2 rejection. The existing V3 matrix already
  exercises every primitive template.
- The first compilation remains intentionally unavailable for API questions;
  resolve them through headers/source and record any residual issue for the
  centralized build phase.
