# 索引单元测试实施计划

> **后续范围更新：** ScalarPredicateReaderTest 和 PatternMatchReaderTest 的完整普通用例补充，按 [reader contract completion plan](02-2026-09-14-reader-contract-completion-plan.md) 执行。本文中的 demo 用例数量和配置矩阵是此前设计阶段的记录；当前覆盖清单以该后续计划链接的验收材料为准。继续不编译、不运行。

> **执行者：** 使用 `superpowers:executing-plans` 按任务执行。完整计划尚未完成；本轮只编写测试框架与小型设计 demo，未验证编译或运行。

> **2026-09-14 本轮评估范围：** 测试框架、Bitmap/Sorted 声明式后端配置、声明式数据集目录、bitset validity、一个 In 算子适配及公共精确过滤 driver。四个 int64 In case 展示共享 oracle、手写命中和手写空结果，使用四行与十万行数据；一个 varchar In case 展示动态生成字符串的所有权及跨类型复用。Lookup 保留复用四行 int64 数据的示例。每个组合独立注册为 GTest 参数实例。用户要求不编译、不运行；下面的 configure/build/test 命令暂不执行。若发现生产实现问题，记录到本目录临时且不提交的 `2026-09-14-index-unit-test-issues.md`，不自动修改生产实现。

> **跨读取器契约 demo：** 新增 PatternMatchReader 的 PrefixMatch 命中与无命中用例，复用 In 的字符串数据集、既有后端表和 FilterTestDriver，不增加后端配置或公共 helper。

**目标：** 追加一个覆盖实现 PR #64、#65 的测试 PR，建立独立的 `index_tests` 二进制文件，以公共契约用例验证真实索引实现，并覆盖公共机制和系列特有行为。

**架构：** 测试源码与生产代码相邻。公共查询用例通过小型工厂取得真实读取器，只调用契约；后端用 `Add<T>(BackendSpec{...})` 集中声明，工厂复用索引层 BuilderRegistry 和 LoaderRegistry 完成 Builder、Artifact、Loader 流程。同一个测试 PR 内分阶段覆盖 #64 的公共机制和 #65 的一致性用例及标量系列，按任务追加小提交。

**技术栈：** C++20、GoogleTest、现有 CMake/Conan、实际标量引擎、内存条目与本地临时目录。

## 1. 已确定的边界

1. 所有测试从最新实现状态追加新 branch/commit，不 amend、rebase 或重写已有实现 PR。本轮 #64、#65 合用一个 test PR，不要求为了原实现层边界拆开共享测试。
2. 本轮只规划 #64、#65。#66 的具体 growing/vector 引擎、#67 的构建编排和消费者测试留给各自后续 test PR。
3. 不在 `internal/core/unittest/` 新增测试 `.cpp`；在那里只调整构建入口。
4. 不直接引用 `unittest/test_utils/DataGen.h`、`indexbuilder_test_utils.h`、`AssertUtils.h` 等聚合旧接口的头文件，不复用 `init_gtest.cpp`。
5. 测试输入是 values、validity、batches、必要的本地文件和索引参数。测试不创建 Collection、Segment、FieldSchema、执行计划，也不调用 #67 的 materializer、BuildSession、IndexBuildService、C ABI 或 Go。
6. 使用现有 contract 中必要的 `DataType`、`ValidityView`、`TargetBitmap` 等类型。“少引入 Milvus 概念”约束测试代码和 helper，不要求本轮消除现有基础头文件的所有传递依赖。
7. `index_tests` 独立收集、构建和运行。允许按当前构建组织链接 `milvus_core`，不把生产库的大规模拆分作为前置条件；这不等于声明整个构建依赖图已经独立。
8. 使用真实索引验证其语义。Fake 只用于测试公共机制的回调、所有权、发布和故障边界，不用 fake Reader 证明查询算法正确。
9. 不加入新旧 binary/历史格式兼容矩阵。若当前实现通过 `Generation` 或引擎版本选择不同路径，覆盖当前代码的这些路径；不将逻辑 FileSource 测试称为远端格式兼容验证。
10. 测试发现生产缺陷时，保留失败用例，将输入、期望、实际结果、定位证据和复现方式记录到临时且不提交的 Markdown，交由用户评估；不自动修复生产实现。不能修改期望值、跳过后端或扩大 fixture 生命周期来掩盖问题。

## 2. 基线与进入代码的顺序

计划编写时的本地基线：

- 分支：`SegcoreRefactor/5-consumers`。
- HEAD：`af90d325473284279ae680b14ddde1f9b9c82464`。
- 原实现层：#64 contracts/L1 artifact；#65 sealed scalar families；#66 vector/growing；#67 consumers/build integration。
- 当前 `cmake_build` 是 Release、CPU、`BUILD_UNIT_TEST=OFF`、`USE_ASAN=OFF`。执行时重新核对，不能把本文快照当作当前配置。

执行前阅读：

1. `internal/core/src/index/contracts/README.md`、`internal/core/src/index/scalar/README.md`。
2. 当前任务对应的 contract 头文件，以及实际 Builder/Artifact/Loader/Reader。
3. `internal/core/unittest/CMakeLists.txt`、`internal/core/src/index/CMakeLists.txt`、`internal/core/cmake/Utils.cmake`。
4. 涉及新错误构造、包装或分类时，先读 `docs/dev/error_handling_guide.md` 与 `docs/dev/error_handling_casebook.md`。
5. 做 code review 前先读 `~/.claude/CODE_REVIEW_GUIDE.md`；处理 stack 前读 `~/.claude/PR_STACK.md`。

文档与实现若出现语义冲突，先记录并与用户确认；不能把观察到的输出直接当成期望值。

当前需要利用或处理的事实：

- `unittest/CMakeLists.txt` 递归收集 `src/**/*Test.cpp` 到 `all_tests`，新测试需要显式移出该列表。
- `add_source_at_current_directory_recursively()` 只按测试文件后缀排除；普通 test helper `.cpp` 仍可能进入生产 target。
- `Registry.cpp` 使用全局 singleton 表，重复注册报错；BuilderRegistry 仅为生产支持的输入类型显式实例化。
- `NamedBufferSink/Source` 已提供实际内存 round-trip，但只表示 V1/V2 named entries；sink 拒绝 `PutMeta` 和 raw-file 发布。
- `index_runtime_acceptance_test.cpp` 已有 Bitmap/Sorted nullable round-trip 示例，可参考小段输入构造和断言；它还包含 #66/#67 内容，不整体迁入新 binary。
- FM 提供 Pattern/Null，并不提供 ScalarPredicateReader；Hybrid 没有独立 Reader/Loader；JSON resolved view 不拥有父 Reader。

## 3. 分支、commit 和 PR 划分

```text
执行时的最新实现 HEAD
  └─ SegcoreRefactor/6-tests-index
       test: [SegcoreUT 1] cover index contracts and scalar families
       同时对应实现 #64、#65；包含公共机制、共享契约和 family 测试的多个 commit
```

- 新分支从执行时确认的最新实现 HEAD 创建，所有任务都在这条测试分支上追加提交。
- 当前基线只是定位依据。若有新实现提交，使用新的最新状态，不回退到本文 SHA。
- 一个 test PR 可以包含多个小 commit；所有 commit 使用 `git commit -s`，不添加 AI co-author。
- 本文的任务结束点是可构建、可运行的本地测试提交。推送和创建远端 PR 在明确授权后执行；PR 标题使用上面的 stack 前缀，body 链接对应实现 PR 和实际验证结果。
- 将当前实现 stack 的现有成员保留原状，只追加测试层。创建分支时检查同名分支是否已存在，不使用 `switch -C` 覆盖。

## 4. 测试组织与共享方式

### 4.1 公共用例验证真实实现

查询测试只知道输入和 contract。后端工厂负责 family 参数、Build、Serialize、Open；不得在期望值计算和断言中分支判断 family。

后端配置只在 `ScalarReaderBackends.cpp` 定义一次，使用 `catalog.Add<T>({.name, .family, .nullable, .build_params, .load_params})`。不为每个类型声明后端类，不逐 family 手写工厂函数，不复制索引到接口的能力表。公共工厂用生产 BuilderRegistry 创建 builder，LoaderRegistry 提供 DeriveCaps 和 Open。`Add<T>` 统一注入构建和加载所需的类型、nullable、row-domain 元数据，参数 bag 不重复声明这些字段。

公共套件用 `ScalarReaderBackends().For<T>(&ReaderCaps::predicate)` 按类型及能力筛选配置，派生 caps 与 Open 使用同一份加载参数。各 family 自身通过不筛选能力的 `Get<T>(name)` 保留关键能力承诺测试，防止生产 caps 与接口一同退化后公共用例消失。缺少注册、重复名称或参数声明冲突均失败，不当作不支持跳过。

当前 demo 声明 Bitmap/Sorted 的 nullable int64 和 varchar、row-domain、named-buffer、heap 配置。四个 int64 In case 引用两个数据集，一个 varchar In case 引用字符串数据集，Lookup 复用小整数数据；不加入 mmap 和其他后端参数组合。后续增加 nullable/坐标/传输模式时，需要同时明确用例的输入形态筛选和 IO 支持，不能只追加一个未被处理的开关。

```text
普通 C++ 数据
  → ScalarBuildInput<T>
  → BuilderRegistry<ScalarBuildInput<T>>::Create
  → concrete Builder
  → Artifact
  → 本地 FileSink / FileSource
  → LoaderRegistry::Lookup(family).open
  → owning IndexReaderBase
  → 借用 ScalarPredicateReader<T> 等接口执行同一套用例
```

公共 conformance 工厂复用索引层 Registry，避免重复生产 factory 选择；这不涉及 #67 的上层选择和服务编排。Registry 本身的选择与注册测试仍单独编写；family 特有测试需要控制具体实现步骤时可以直接创建 builder。

工厂返回 owning Reader，不返回悬空 mixin 指针。默认工厂在返回前释放输入之外的构建中间对象与 load source；需要刻意控制每一步生命周期的测试直接在 family 测试文件中展开这些步骤。

### 4.2 公共源码位置

```text
internal/core/src/index/
  contracts/
    RegistryTest.cpp
    build/
      ReaderConvertibleTest.cpp
      ArtifactBuilderTest.cpp              # #65：真实 family 的构建契约
    growing/
      GrowingIndexTest.cpp
    query/
      IndexReaderTest.cpp
      ScalarPredicateReaderTest.cpp
      ScalarValueReaderTest.cpp
      NullReaderTest.cpp
      PatternMatchReaderTest.cpp
      TextMatchReaderTest.cpp
      NgramReaderTest.cpp
      SpatialReaderTest.cpp
      JsonIndexReaderTest.cpp
  scalar/<family>/
    <ProductionClass>Test.cpp              # family 特有行为，见各任务文件表
  test_utils/
    ScalarTestData.h                       # 数据集目录类型、owning 数据与借用输入适配
    ScalarDataSets.cpp                     # 集中 Add<T> 声明惰性数据集
    FilterTestDriver.h                    # case 描述、手写预期与单组合精确过滤 driver
    BitmapAssertions.h/.cpp                # 位图/集合断言
    ScalarReaderBackends.cpp               # 集中 Add<T> 声明后端配置
    ScalarReaderFactory.h/.cpp             # 配置筛选；通过生产 Registry 创建 Reader
    TestArtifactIO.h/.cpp                  # 首个需要逻辑文件/meta 注入的用例引入

internal/core/src/storage/artifact/
  FileSinkTest.cpp
  FileSourceTest.cpp
  LocalDirectoryTest.cpp
```

只有出现实际复用时才增加 helper。局部 tracking Reader、callback counters、故障 source 留在使用它们的测试 `.cpp`。不新增所有测试都必须继承的有状态 fixture 基类，不做插件式测试后端注册框架。

输入 owner 必须持有 string storage、string_view 数组、batch 数组和 validity。全部存储稳定后再构造 span；bool 输入使用连续 bool 存储，不把 `vector<bool>` 当作 `bool*`。默认用明确小数据；随机数据必须固定种子并输出失败输入。

### 4.3 接口适用矩阵

| 用例组 | 纳入的实现/模式 | 断言边界 |
|---|---|---|
| ScalarPredicate，普通行 | Bitmap、Sorted、Inverted；Marisa 的字符串类型 | 同一个类型的 In/NotIn/Range 结果与独立 oracle 一致 |
| Null | 实际提供 NullReader 的系列 | Count-sized bitmap；依据该 Reader 的域和 null 定义建组 |
| Value | 实际提供 ScalarValueReader 的系列 | Lookup 的 owning result；Gather 按回调 i 还原请求位置 |
| Pattern | Bitmap/Sorted/Inverted/Marisa/FM 支持的 op/literal | 支持范围显式建表；检查 ShouldUseForOp，精确匹配时比较精确结果 |
| Text | Text | 固定 analyzer 的 Match/Phrase/Fuzzy 语义 |
| Ngram | 标量 Ngram、JSON projected Ngram | CanHandle；Candidates 与原候选做 AND；无 false negative |
| Spatial | RTree | MBR 候选覆盖真实命中；不要求候选等于精确几何结果 |
| JSON | JsonFlat、projected wrapper | path/cast 选择、存在性、三态与借用生命周期 |
| Array / nested | 实际支持该输入形状的系列 | 行与 element 分组；不在测试中引入 Segment 投影 |

上表说明用例语义，不作为另一份手写配置能力表。配置的适用性复用生产能力描述；没有对应 caps 位的接口在取得 Reader 后检查。关键能力承诺由 family 自身测试验证，缺少应有能力时失败，不能自动 `GTEST_SKIP()`。不要仅凭一个 `caps.exact` 标志推断所有操作的语义；尤其元素 postings 与整数组相等语义要分开。

### 4.4 执行时生成数据与跨 contract 复用

- `ScalarDataSets()` 返回 DataCatalog，通过 `Add<T>({.name, .make_data})` 集中定义数据集。`ScalarDataSet<T>` 是小描述，`ScalarTestData<T>` 是执行时生成的可变数据；头文件不导出逐数据集常量。
- 数据包含 values 和等长 validity bitset，行数从 values.size() 得到，构造时 validity 默认全 1。ScalarTestInput 直接把 bitset 借用为 packed ValidityView，不额外复制 expanded bool 数组。
- 类型参数使用 contract 的类型：varchar 为 `std::string_view`，但数据和查询 keys 通过 `ScalarTestValue<T>` 持有 `std::string`。Build 和 In 调用前才生成临时 views，GTest 参数和数据对象不缓存 views。数据集名称按类型区分，int64 和 varchar 可以都叫 RepeatedNullable。
- 每个算子在对应 contract 测试中定义参数类型、所需能力、实际调用以及可选的默认 Oracle。`In<T>::Oracle` 是一份独立逐行扫描，供多个 In case 共用。
- `InCases()` 使用统一的 FilterCases 表，通过 `Add<In<int64_t>>`、`Add<In<std::string_view>>` 声明名称、数据集名称、keys 和可选的 expected 回调。Add 保留参数、数据集与算子的类型检查。不填 expected 时调用算子 Oracle；`ManualHits({0, 3})` 与 `ManualHits({})` 都是明确的手写模式。复杂算子可以没有默认 Oracle，缺失 expected 时失败。大预期结果可以在 expected 回调中生成，避免捕获提前分配的大数组。
- RunFilterCase 只处理传入的一个组合：生成数据、绑定输入、生成 ground truth、构建 Reader、检查能力/接口/行域/尺寸、调用算子、通过 bitset 的 operator== 比较位图。当前 driver 只覆盖精确行过滤；candidate、nested、特殊生命周期不强行使用它。
- FilterCases::Add 在注册阶段筛选同类型且支持对应能力的后端，展开为 FilterParam（名称 + std::function<void()>）。回调按值持有后端配置与有类型的 case 小描述，只在执行时调用 RunFilterCase。统一的 ScalarPredicateReaderTest 使用一次 ValuesIn 注册所有类型，测试体只调用 GetParam().run()；禁止在一个测试中循环后端、数据集或查询参数组合。每个组合有包含后端、数据集和 case 名称的独立 GTest 参数名。
- 小数据 `[10, NULL, 30, 10]` 在 In 和 Lookup 间复用生成器，每次调用得到独立数据。十万行数据和 ground truth 只在对应测试执行时生成，结束后释放；不缓存共享可变数据或 Reader。
- PatternMatchReaderTest.cpp 使用同一 FilterCases::Add<PrefixMatch> 入口。PrefixMatch 仅声明 string_view 类型、pattern_match 能力、拥有字符串的 prefix 参数、实际调用和 validity + starts_with oracle；调用前断言 ShouldUseForOp。命中与无命中 case 复用 In 的 RepeatedNullable 字符串数据集，每个 case 自动覆盖所有匹配后端。
- 当前注册结构为两个 int64 后端 × 四个 In case、两个 varchar 后端 × 一个 In case、两个 varchar 后端 × 两个 PrefixMatch case，以及两个 int64 后端 × 一个 Lookup case。所有 In case 在一张表中声明，复用 In<T> 的 oracle 和公共 driver，动态字符串数据的命中行为与小整数数据一致。新增类型无需增加 fixture 或 INSTANTIATE_TEST_SUITE_P。两个 family 的不筛选能力断言覆盖 int64 的 predicate/value_lookup 和 varchar 的 predicate/pattern_match。未运行 GTest，数量仅由静态注册结构推导。

## 5. 执行与验证约定

每个任务采用同一执行步骤：

1. 读取该任务列出的接口和实现，写出输入、预期、适用实现和生命周期边界。
2. 添加一个真实测试，再扩展表中的用例。获准运行后，新测试验证已有正确行为时首次运行可以通过，不制造无意义的失败；若发现缺陷，保留最小失败用例并写入临时问题记录。
3. 编译 `index_tests`，用该任务的 GTest filter 运行；确认实际执行数量大于零。
4. 检查失败诊断包含 family、类型、模式、输入或差异 offset。共享套件采用 GoogleTest 参数化/类型化设施，后端差异只进入工厂或明确的语义分组。
5. `git diff --check`，只 stage 本任务文件，以 `git commit -s` 追加提交。

以下命令以仓库根目录为工作目录，使用已存在且与当前工作树一致的 `cmake_build`。首次打开单元测试会改变 `WITHOUT_GO_LOGGING` 等编译选项，允许重编受影响的生产对象。

```bash
rtk proxy cmake -S internal/core -B cmake_build -DBUILD_UNIT_TEST=ON -DCMAKE_EXPORT_COMPILE_COMMANDS=ON
rtk proxy cmake --build cmake_build --target index_tests --parallel 8
rtk proxy ./scripts/run_index_unittest.sh --gtest_list_tests
rtk proxy ./scripts/run_index_unittest.sh --gtest_filter='ReaderConvertibleTest.*'
```

必须核对 `CMAKE_HOME_DIRECTORY` 和工具链路径属于本工作树。新 worktree 先按仓库构建流程建立自己的 Conan/CMake 配置；不能复制旧工作树的绝对路径 CMakeCache。worktree 放在 `/home/zilliz/...` 或 `/tmp/kilo/...`。

## 6. 公共机制测试阶段（覆盖 #64）

### Task 1 — 建立 binary 与运行入口，加入第一个真实用例

**修改：**

- `internal/core/unittest/CMakeLists.txt`
- `scripts/run_cpp_unittest.sh`

**新增：**

- `scripts/run_index_unittest.sh`
- `internal/core/src/index/contracts/build/ReaderConvertibleTest.cpp`

**步骤：**

1. 创建 `SegcoreRefactor/6-tests-index`，记录其起点 SHA。
2. 在 ReaderConvertibleTest 中先加入 null artifact 的错误码测试；调用生产 `IReaderConvertible::FromArtifact(nullptr)`，验证捕获到 `SegcoreError` 且 code 为该入口实际约定的 `UnexpectedError`。
3. CMake 定义绝对路径的 `INDEX_TEST_FILES` 显式列表。用同一列表从 `MILVUS_TEST_FILES` 移除文件，再创建 `index_tests`，避免两个 binary 重复收集。
4. 使用 `GTest::gtest_main`，按现有编译依赖链接 `milvus_core` 和必要依赖，不复制 plan parser、Segment YAML、远端存储初始化。不复用 `init_gtest.cpp`。
5. 为 target 设置只需要的 include/compile/link 属性，安装到既有 `unittest` 目录。既有目录级 flag 有继承时明确记录，不声称完成了链接依赖隔离。
6. 独立脚本默认执行本工作树 `cmake_build/unittest/index_tests`，允许 `INDEX_TEST_BINARY` 显式覆盖为安装后的路径；缺失 binary 返回非零，原样转发所有 GTest 参数。依照已有脚本设置必要 library path 和现有 LSan suppression，不初始化服务。
7. 现有 `run_cpp_unittest.sh` 的默认完整运行分支调用新 binary；既有专门针对 all_tests 的 filter 分支保持范围明确。独立运行 index_tests 不要求 all_tests 已构建或通过。
8. 执行上节 configure/build/list/filter 命令；用 `bash -n` 检查修改的两个脚本。预期 list 中包含真实测试，运行至少 1 个且退出 0。

**提交：** `test: [SegcoreUT 1] add standalone index test target`

### Task 2 — 消费式转换和 JSON resolved handle 的所有权

**新增/修改：**

- `internal/core/src/index/contracts/build/ReaderConvertibleTest.cpp`
- `internal/core/src/index/contracts/query/JsonIndexReaderTest.cpp`
- `internal/core/unittest/CMakeLists.txt`

**读取：** `IReaderConvertible.h`、`IJsonIndexReader.h`。

**用例：**

- 消费成功：Artifact shell 恰好析构一次，返回的 Reader 和其依赖仍存活，Reader 释放后依赖才释放。
- 缺少转换能力：返回 `Unsupported`，Artifact 仍销毁；Serialize/IO 调用计数为零。
- IntoReader 抛错：原异常/code 不被替换，Artifact 恰好销毁一次。
- IntoReader 返回 null：按公共入口的约定报错，清理完整。
- Owned JsonResolvedReader 的 move construction、move assignment、自移动和释放；移出对象为空，旧 owned view 按时释放。
- Borrowed JsonResolvedReader 的移动/销毁不析构被借用 Reader；空 handle 移动仍为空。
- 用 `static_assert` 验证 move-only 等编译期约束。不在销毁父 Reader 后解引用 view 来测试未定义行为。

Tracking 类型只实现计数/所有权需要的最小接口；被测的是生产 helper/handle 的行为。

**运行：** `--gtest_filter='ReaderConvertibleTest.*:JsonResolvedReaderTest.*'`。

**提交：** `test: [SegcoreUT 1] cover consuming artifact and resolved reader ownership`

### Task 3 — Registry 公共机制

**新增：** `internal/core/src/index/contracts/RegistryTest.cpp`。

**读取：** `index/contracts/Registry.h`、`index/contracts/Registry.cpp`。

**用例：**

- 未知 family 的 Lookup 返回空 entry，未知/未注册输入类型组合的 Create 返回 nullptr。
- 使用已显式实例化的 `ScalarBuildInput<int64_t>` 和 `ScalarBuildInput<double>`，验证同一 family 在不同 Input registry 中互不串用。
- 参数原样交给 factory；返回对象来自指定 factory；factory 抛出的异常原样传播。
- 空 factory 拒绝注册；重复注册失败且原有效 factory 仍可使用。
- 静态 loader provider 的 DeriveCaps/Open 分派正确；派生 caps 不调用 Open；Open 的故障不会变成“未注册”。
- 使用明确的测试专用 family key。正常注册通过进程内 `std::call_once` 等一次性初始化完成；`SetUpTestSuite` 在 repeat 中可能再次执行，不能只依赖它避免重复注册。重复注册测试检查已经注册的 key，不依赖测试运行次序；并发注册用例也为每轮使用独立 key。
- 适量并发注册/查找互不冲突的 key，使用 barrier/latch 同步，不靠 sleep。

不为测试新增 registry reset/clear API，不伪造未显式实例化的生产输入类型，也不在此测试全部 family 的查询算法。

**运行：** `--gtest_filter='IndexRegistryTest.*'`，再对本组执行 repeat/shuffle。

**提交：** `test: [SegcoreUT 1] cover typed index registry behavior`

### Task 4 — Growing 公共发布与 pin

**新增：** `internal/core/src/index/contracts/growing/GrowingIndexTest.cpp`。

**读取：** `IGrowingIndex.h`；参考 acceptance 文件中的 tracking 思路，不引入 GrowingIndexSet。

**用例：**

- 空 pin、已发布的空 Reader 是两种状态；空 pin 的 CoveredRowEnd 为 0，Reader 访问报错。
- pin 的 copy/move、移出对象为空，Reader 与 coverage 成对固定。
- 发布新的记录后旧 pin 继续读旧记录；owner 析构后已取得的 pin 继续有效；最后一个 pin 释放后 Reader 恰好析构一次。
- Count 与 CoveredRowEnd 使用不同数值，防止把元素数误当行覆盖。
- null Reader、负 coverage、coverage 回退被拒绝，当前发布记录保持不变；相同 coverage 可更换合法记录。
- 发布与 pin 并发时，每个 pin 的 Reader 标识和 coverage 必须来自同一记录。
- 使用析构探针在旧 Reader 析构时调用 PinSnapshot，验证旧记录在发布锁外释放；安排有界运行超时，避免死锁测试无限挂起。

最小派生 publisher 仅暴露受保护 PublishSnapshot。不用它证明真实引擎的 Append、Flush、提交策略或 Add/Search 并发安全；这些属于 #66。

**运行：** `--gtest_filter='GrowingIndexContractTest.*'`。

**提交：** `test: [SegcoreUT 1] cover snapshot publication and pin lifetime`

### Task 5 — 本地 artifact primitives

**新增：**

- `internal/core/src/storage/artifact/FileSinkTest.cpp`
- `internal/core/src/storage/artifact/FileSourceTest.cpp`
- `internal/core/src/storage/artifact/LocalDirectoryTest.cpp`

**读取：** 同目录 FileSink/FileSource/LocalDirectory/LocalFileUtils 的生产实现。

**用例：**

- 真实 NamedBufferSink/Source：写入、Finish、Take、条目名字/长度/内容、空条目、二进制零字节。
- Finish 前取 Data、Finish 后继续写入、失败后再次写入等状态约束；断言实际约定的 code，不锁定完整错误字符串。
- NamedBufferSink 的 PutMeta/raw-file 能力拒绝；不能把该实现当作通用 V3 metadata 容器。
- 从借用文件写入后，原文件不被删除；source 拼接多个条目时严格保持请求顺序。
- 缺少条目、截断 slice/错误 slice metadata；测试实际重组后的结果或明确拒绝，不复制生产解码器作 oracle。
- ReadEntryToLocalFile/ReadEntriesToLocalDir 中途遇到确定的缺失条目时，原目标内容保留，临时 staging 清理；目录目标名冲突被拒绝。
- LocalDirectory 创建独占子目录、识别拥有的子路径、拒绝相邻前缀/父路径/指向外部的 symlink；最后一个 owner 释放时只删除自身子目录。

临时根由测试内小 RAII 持有，不使用全局 TestLocalPath。失败构造采用“路径中某级是普通文件”等确定条件，避免 root 身份下权限测试失效。

该任务不测试远端 ChunkManager、V3 packed storage、上传事务。小型 injected FileSource 的行为不能充当这些生产适配器的验证证据。

**运行：** `--gtest_filter='NamedBufferSinkTest.*:NamedBufferSourceTest.*:LocalDirectoryTest.*'`。

**提交：** `test: [SegcoreUT 1] cover local artifact IO and cleanup`

### Task 6 — 验证公共机制阶段

1. 全量运行当前 index_tests，执行一次固定 seed 的 repeat/shuffle，核对所有组均非空。
2. 确认 target 列表没有把这些测试再次编入 all_tests 或生产库；没有依赖旧 test_utils 聚合头。
3. 完成文末失败/生命周期检查表中本阶段适用的条目，记录实际执行的组、命令、配置和未覆盖项，供最终 test PR 说明使用。
4. 当前可运行版本追加提交后，在同一分支继续 Task 7；这是中间检查点，不代表整个 test PR 完成。

## 7. 共享契约与真实索引测试阶段（覆盖 #65）

### Task 7 — 建立最小真实 Reader 工厂与第一套共享谓词用例

**新增：**

- `internal/core/src/index/test_utils/ScalarTestData.h`
- `internal/core/src/index/test_utils/BitmapAssertions.h`
- `internal/core/src/index/test_utils/BitmapAssertions.cpp`
- `internal/core/src/index/test_utils/ScalarReaderFactory.h`
- `internal/core/src/index/test_utils/ScalarReaderFactory.cpp`
- `internal/core/src/index/contracts/query/ScalarPredicateReaderTest.cpp`

**修改：** `internal/core/src/index/CMakeLists.txt`、`internal/core/unittest/CMakeLists.txt`。

**步骤：**

1. 在 index 的生产 SOURCE_FILES 收集后、创建 target 前，显式排除相对路径 `^test_utils/`；helper `.cpp` 只加入 `INDEX_TEST_SUPPORT_FILES`。测试源仍使用 `*Test.cpp` 后缀。
2. 第一批只用 int64 + Bitmap/Sorted 跑通真实接口用例，再按 Task 8 扩展矩阵；这一步结束仍只是 test PR 的中间 commit。
3. 工厂不 include 各 family 头，通过生产 BuilderRegistry/LoaderRegistry 选择实现；配置集中到 ScalarReaderBackends.cpp。使用生产 `NamedBufferSink/Source`，不为已有 round-trip 另写替代实现。
4. 建立数据 `[10, NULL, 30, 10]`，第一、第二 batch 的划分与最终逻辑位置无关。
5. 同一用例实例化到各工厂，打印清楚 family/type/mode；断言只访问 ScalarPredicateReader。

共享用例的目标形状如下，`reader_` 由 fixture 的参数工厂创建，`predicate_` 在持有 reader_ 时借用：

```cpp
TEST_P(ScalarPredicateInt64Test, InMatchesEveryOccurrence) {
    const std::array<int64_t, 1> keys{10};
    ExpectBits(predicate_->In(keys.size(), keys.data()), 4, {0, 3});
}

TEST_P(ScalarPredicateInt64Test, EmptySetHasNoHits) {
    ExpectBits(predicate_->In(0, nullptr), 4, {});
}

TEST_P(ScalarPredicateInt64Test, NotInExcludesNullRows) {
    const std::array<int64_t, 1> keys{10};
    ExpectBits(predicate_->NotIn(keys.size(), keys.data()), 4, {2});
}
```

使用 GoogleTest 的现成参数化/类型化设施；为更多值类型共享本文件内的模板用例或断言函数，不复制一套后端专属查询用例，不创建运行期测试注册系统。

**运行：** filter `'*ScalarPredicate*'`；输出必须明确包含 Bitmap 和 Sorted 两个真实后端。

**提交：** `test: [SegcoreUT 1] share scalar predicate tests across real readers`

### Task 8 — 扩展公共 Reader 与 Builder 契约矩阵

**新增：**

- `internal/core/src/index/contracts/query/IndexReaderTest.cpp`
- `internal/core/src/index/contracts/query/NullReaderTest.cpp`
- `internal/core/src/index/contracts/query/ScalarValueReaderTest.cpp`
- `internal/core/src/index/contracts/build/ArtifactBuilderTest.cpp`

**修改：** Task 7 的工厂、输入 helper、谓词测试和 CMake 列表。

**实现步骤：** 每个下面的用例组分别添加、运行、提交；不要一次提交未经运行的整个矩阵。

| 用例组 | 具体断言 |
|---|---|
| In/NotIn | 空集合、不存在值、重复 key、重复数据、多 key 顺序变化、全部命中、全部不命中；NotIn = valid AND complement(In) |
| Range | 单值六种 CompareOp、双端点四种开闭组合、相等端点、反向区间、类型边界；使用独立比较生成结果 |
| 输入边界 | 空输入、全 NULL、空 batch、跨 batch 重复值、不规则 batch 划分；不支持的输入按明确约定拒绝 |
| 类型 | 所有实际支持的 bool、整数、浮点、string_view 模板实例；字符串覆盖空值、UTF-8、嵌入零字节；非有限浮点先确定契约再设期望 |
| Null | 无 validity 表示全有效；显式有效/无效混合；位图长度等于 Count；不把 row-null 与 JSON path nonexist 合并 |
| Value | Lookup 存在/NULL；字符串返回独立拥有数据；Gather 按 i 写回、保持重复请求、valid=false 不解引用值指针、不要求回调按顺序发生 |
| Metadata | Count、CoordDomain、ValueType；DeriveCaps 与真实 Reader::Caps 逐字段一致；声明的能力可以取得实际接口 |
| Build 生命周期 | Build 返回后销毁 Builder 和全部输入存储，再 Serialize/Open/Query；特别覆盖 string_view、batch 容器和变长后备数据 |
| Open 生命周期 | Open 返回后释放 Artifact、sink/source、调用者 buffer，Reader 仍可查询；必要的 mmap/directory owner 应由 Reader 自己保留 |

将 Inverted、Marisa 加入支持的共享用例。后端内存/文件模式通过工厂参数表达；公共 oracle 不能调用生产查询工具或旧索引作为唯一参照。

一般查询测试不要求任何特定私有布局。Bitmap 等需要特定布局数据才能触发的分支由 Task 9 补充，继续复用本任务的结果断言。

**运行：** 每组对应 filter，完成后运行 `'*ScalarPredicate*:*ScalarValue*:*NullReader*:*IndexReader*:*ArtifactBuilder*'`。

**提交：** 按接口分小 commit，统一前缀 `test: [SegcoreUT 1] ...`。

### Task 9 — Bitmap / Sorted / Inverted / Marisa 的实现路径

**按实际用例新增：**

- `internal/core/src/index/scalar/bitmap/BitmapIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/bitmap/BitmapIndexReaderTest.cpp`
- `internal/core/src/index/scalar/bitmap/BitmapIndexLoaderTest.cpp`
- `internal/core/src/index/scalar/sort/SortedIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/sort/SortedIndexReaderTest.cpp`
- `internal/core/src/index/scalar/sort/SortedIndexLoaderTest.cpp`
- `internal/core/src/index/scalar/inverted/InvertedIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/inverted/InvertedIndexLoaderTest.cpp`
- `internal/core/src/index/scalar/marisa/MarisaIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/marisa/MarisaIndexReaderTest.cpp`
- `internal/core/src/index/scalar/marisa/MarisaIndexLoaderTest.cpp`

每个文件只在有对应行为用例时创建，不生成空 fixture。

**场景：**

- Bitmap：定位当前 posting 存储分支，构造能实际触发分支的数据；覆盖位图边界、稀疏/密集分布、重载后的等价结果。
- Sorted：数值与字符串不同存储布局、重复值区间、lookup/Gather 的字符串所有权与 mmap 后备生命周期。
- Inverted：真实 Tantivy writer 的多 batch 构建、当前支持的引擎 generation 分支、目录/sidecar 完整性、stream-load 与 mmap。
- Marisa：重复 key 的全部位置、字典重建返回值所有权、加载文件完整性；不复制生产 trie 算法作 oracle。
- 对实际支持 ARRAY 的 Bitmap/Sorted/Inverted，直接准备 ArrayView 的拥有数据：区分 null/empty、同一数组的重复元素、row postings 去重。nested 用预先准备好的元素输入，断言 element Count/Domain 和 offset，不构造 Segment 或执行投影。
- 对每个 loader 至少覆盖一条正常加载、一条缺少必需条目、一条损坏元数据/载荷、一条实际 IO 失败；错误从故障入口追踪到 Open 调用者。

允许在这些测试中调用 In，但目的必须是验证这里的存储/加载/输入形状分支，共用 bitmap oracle。

**运行：** 每次一个 family filter，最后运行本任务全部新 suites。

**提交：** 每个 family 一个或多个 `test: [SegcoreUT 1] ...` commit。

### Task 10 — Pattern、FM、Text、Ngram 与 RTree

**新增：**

- `internal/core/src/index/contracts/query/PatternMatchReaderTest.cpp`
- `internal/core/src/index/contracts/query/TextMatchReaderTest.cpp`
- `internal/core/src/index/contracts/query/NgramReaderTest.cpp`
- `internal/core/src/index/contracts/query/SpatialReaderTest.cpp`
- `internal/core/src/index/scalar/fmindex/FmIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/fmindex/FmIndexReaderTest.cpp`
- `internal/core/src/index/scalar/text/TextIndexArtifactTest.cpp`
- `internal/core/src/index/scalar/text/TextIndexLoaderTest.cpp`
- `internal/core/src/index/scalar/ngram/NgramIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/ngram/NgramIndexLoaderTest.cpp`
- `internal/core/src/index/scalar/spatial/RTreeIndexArtifactTest.cpp`
- `internal/core/src/index/scalar/spatial/RTreeIndexLoaderTest.cpp`
- 需要时：`internal/core/src/index/test_utils/TestArtifactIO.h` 和 `internal/core/src/index/test_utils/TestArtifactIO.cpp`。

**按组执行：**

1. Pattern：明确 LIKE/Regex 的语法与 Prefix/Postfix/Inner 的 literal 区别；覆盖 `%`、`_`、空串和转义。先验证 op/literal 的支持约定，再对支持组合执行共享精确结果用例。FM 的路由拒绝单独测试。
2. Text：固定 analyzer 和小语料，覆盖 min_should_match、phrase slop、fuzzy edit distance。分别测试文件模式 Serialize/Open 和 RAM 模式 IReaderConvertible::FromArtifact；RAM 不能 Serialize 的能力边界要有负例。
3. Text 生命周期：转换后销毁 Artifact shell，Reader 保留 engine/backing；失败转换不保留多余 owner。普通 scalar 的 Artifact 不允许因缺少转换能力而隐式走 IO fallback。
4. Ngram：验证 CanHandle；从全候选及预过滤候选开始查询，结果只能收缩且包含原候选范围内所有真实匹配；用普通字符串操作求真值。另测 false positive 示例，不能把候选当最终精确结果。
5. RTree：用明确几何构造验证 MBR 候选和 NullReader；DWithin 的输入是调用者已准备好的扩展查询形状，不拉入 executor。覆盖 Artifact 的各实际支持序列化模式及对应重载。
6. 每组补一个必需文件缺失/损坏路径，并验证故障后的目录清理。Tantivy wrapper 的实际创建/释放通过这些真实 family 路径执行；不能据此声称整个 Rust binding 已获得单元覆盖。

TestArtifactIO 仅在 NamedBuffer transport 无法表达需要的 raw entry 或 typed metadata 时增加：提供 `name → bytes/file`、`key → JSON value`、可选定点失败。实现放 `.cpp`，明确保持 bool/integer/string/array 类型。它表示注入的逻辑 IO 边界，不实现远端命名、切片、加密、上传或 packed container。

**运行：** 每组 filter，完成后运行 `'*Pattern*:*FmIndex*:*Text*:*Ngram*:*Spatial*:*RTree*'`。

**提交：** 按 family/contract 组追加 `test: [SegcoreUT 1] ...`。

### Task 11 — Hybrid 和 JSON

**新增：**

- `internal/core/src/index/scalar/hybrid/HybridIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/hybrid/HybridIndexArtifactTest.cpp`
- `internal/core/src/index/scalar/json/JsonFlatIndexBuilderTest.cpp`
- `internal/core/src/index/scalar/json/JsonFlatIndexLoaderTest.cpp`
- `internal/core/src/index/scalar/json/JsonProjectedIndexArtifactTest.cpp`
- `internal/core/src/index/scalar/json/JsonPathIndexReaderTest.cpp`

**修改：** `contracts/query/JsonIndexReaderTest.cpp`、必要的工厂和 CMake 列表。

**Hybrid：**

- 构造阈值下方、正好阈值、阈值上方的完整输入，断言选择；同时覆盖 NULL、重复值和跨 batch 的基数。
- 验证 delegate params 进入选定 factory，同一完整输入交给实际选定 family，结果满足公共查询契约。
- 断言产物 selector 与 delegate 对应。测试按已知预期选择 concrete Loader 打开内部结果，不借用 #67 IndexTypeAdapter 来替自己判断预期。
- 不创建不存在的 Hybrid Reader/Loader，也不期待 Hybrid 转发 ReaderConvertible。

**JSON：**

- JsonFlat 直接输入测试持有的 JSON 字符串；projected wrapper/JSON Ngram 直接构造已投影值和状态，不调用 #67 JsonBuildMaterializer。
- 区分 field null、missing path、present value；typed cast 失败与 nonexist 按 family 契约处理。
- 支持但全不存在的 path：CastTypesOf 非空且 Exists 为全零；不支持的 shape：CastTypesOf 为空，不调用前置条件不满足的 Exists。
- Resolve 获得接口后复用匹配的 scalar 查询用例。父 Reader 在使用 resolved view 期间保持存活；view 的 move 行为仍由 #64 的公共 handle 测试覆盖。
- 数字覆盖 int64/double 精度边界，避免把两者都先转 double 算期望。
- ARRAY cast 与 nullable/empty/nonexist 分开构造，nested 结果留在元素域。
- 缺失/损坏 projection sidecar 的失败及 wrapper 释放时的 owner 行为。

**运行：** `'*Hybrid*:*Json*'`，然后运行全量 index_tests。

**提交：** 分 Hybrid、JSON 追加 `test: [SegcoreUT 1] ...`。

### Task 12 — 验证生产注册和完整测试入口

**修改：** `contracts/RegistryTest.cpp`、CMake 列表、必要的独立运行脚本。

1. 在 #64 的 registry 机制用例之外，为 #65 的真实 factory/loader 注册增加小型 smoke 矩阵；只验证生产注册、输入类型匹配和配置解析，不再重跑全部查询组合。
2. 用集中定义的后端配置与各 family 的关键能力承诺测试对照实际测试列表；构造失败、缺少已承诺能力必须报错，不能自动把组合删掉。
3. 确认 static registration 对象实际在 binary 依赖中。当前共享库方式保持注册；若执行时改变 archive 组织，验证链接保留情况，不能用测试自注册真实 family 掩盖生产漏注册。
4. 所有 `INDEX_TEST_FILES` 都从 all_tests 收集中移除；所有 helper `.cpp` 都只编入 index_tests。
5. 运行下一节的最终 gate，补齐 #64、#65 的实际覆盖矩阵和未覆盖项，再结束这个合并的 test PR。

## 8. 最终验收

### 8.1 构建、运行和重复执行

```bash
rtk proxy cmake --build cmake_build --target index_tests --parallel 8
rtk proxy ./scripts/run_index_unittest.sh --gtest_list_tests
rtk proxy ./scripts/run_index_unittest.sh --gtest_output=xml:cmake_build/index_tests.xml
rtk proxy ./scripts/run_index_unittest.sh --gtest_shuffle --gtest_random_seed=914 --gtest_repeat=3
rtk git diff --check
```

验收：退出码为零；预期 suites 和参数组均实际执行；XML 没有意外 skipped/disabled；repeat 不因 singleton 注册或临时目录污染失败。记录具体用例数和配置，不以“编译了测试 binary”代替运行结果。

生命周期组再用 ASan 配置执行；核对目标生产 C++ 对象与 index_tests 的编译命令确实包含 sanitizer，不能只有测试入口带 ASan：

```bash
rtk proxy cmake -S internal/core -B cmake_build -DBUILD_UNIT_TEST=ON -DUSE_ASAN=ON
rtk proxy cmake --build cmake_build --target index_tests --parallel 8
rtk proxy ./scripts/run_index_unittest.sh --gtest_filter='ReaderConvertibleTest.*:JsonResolvedReaderTest.*:GrowingIndexContractTest.*:*ArtifactBuilder*:*Lifetime*'
```

这会改变当前构建配置，执行前记录；报告中说明最终配置。若使用独立 sanitizer build directory，先按仓库流程配置工具链并通过 `INDEX_TEST_BINARY` 指定 binary。不扩大既有 LSan suppression 来掩盖新泄漏，不宣称未插桩的第三方代码得到相同覆盖。

### 8.2 失败与生命周期检查表

每个已声称覆盖的行为都要填写实际测试名称与结果：

| 起点 | 必须追踪到的结果 |
|---|---|
| registry 未注册/重复注册/factory 抛错 | 区分空能力和失败；原注册/原异常保持 |
| Artifact 不支持转换/转换抛错/null 返回 | 正确错误；shell 恰好释放；无隐藏 IO |
| 发布非法 coverage/无 Reader | 旧记录不变；旧 pin 继续有效 |
| Build 借用输入失效 | 已完成 Artifact/Reader 不再借用该输入 |
| FileSource 缺失/损坏/read 失败 | 到 Loader 调用者的真实错误；部分产物和 staging 清理 |
| FileSink 写入失败 | 失败状态不被 Finish 掩盖；借用输入文件不被删除 |
| Loader 成功返回后 source/Artifact 释放 | Reader 的真实查询仍正确；backing owner 按约定存活 |
| JSON resolved view 移动/销毁 | owned 与 borrowed 的析构责任正确 |
| 原候选已过滤后的 Ngram 查询 | 只收缩；不恢复被过滤位置；真实命中不丢失 |

错误码断言从构造点追到调用者，不只写 `EXPECT_ANY_THROW`。若发现生产构造/转换丢失错误码，遵循仓库 G1/G2，不能只改测试边界。

### 8.3 范围检查

- 测试源码全部位于相应生产文件旁；没有新增 `unittest/*.cpp`。
- 新 helper 小且用途明确；没有 include 旧 test_utils 大头文件或 #67 的编排/消费者头文件。
- 共享 In/Range 等用例确实调用了多个真实实现；family-specific 测试只补特有场景。
- 普通 exact、candidate、ARRAY、element、JSON path 语义没有强行合并。
- 不依赖运行中的 Milvus、etcd、MinIO 或外部网络；文件型引擎使用本地临时目录。
- 旧 acceptance、旧 all_tests、#66/#67 的构建/查询集成不被这些结果替代；未运行的内容明确列出。
- 新测试引入的生产修复以新 commit 保留；原实现 stack 未被改写。

## 9. 实施时的完成记录

这个 test PR 的说明至少包含：对应实现 #64、#65、最终基线 SHA、覆盖的公共机制/真实 family、实际命令与结果、配置、未完成项。只有 Task 1–12 的实际适用矩阵和失败路径全部完成后才能称 test PR 完成；公共机制阶段或第一个 Bitmap/Sorted 用例通过都只是中间里程碑。

本轮评估改动位于 `SegcoreRefactor/6-tests-index`，从本文基线创建，尚未提交。已添加 index_tests 的 CMake/CTest/脚本入口；后端在 ScalarReaderBackends.cpp 使用 Add<T> 声明，公共工厂复用生产 Registry 和 DeriveCaps。

ScalarDataSets.cpp 提供集中声明的数据集目录，ScalarTestData.h 定义 bitset validity、字符串所有权与借用输入适配。ScalarPredicateReaderTest.cpp 声明 In 算子、四个 int64 case（共享 oracle 的小/大输入、手写命中、手写空结果）和一个动态字符串 varchar case；FilterTestDriver.h 每次执行一个独立的 GTest 参数组合，通过 operator== 比较位图。PatternMatchReaderTest.cpp 新增两个 PrefixMatch case，复用同一 driver、字符串数据和后端配置。ScalarValueReaderTest.cpp 仍用独立 Lookup 断言复用四行 int64 数据集。BitmapIndexReaderTest.cpp、SortedIndexReaderTest.cpp 各保留不经过筛选的 int64 predicate/value_lookup 和 varchar predicate/pattern_match 能力承诺测试。后端表包含 BitmapVarchar、SortedVarchar，没有增加 mmap 或其他后端参数组合。

收到“不编译运行”指令前，初始构建命令曾触发 CMake 重新生成，随后已中止；新增测试及其构建配置没有执行编译或测试。后续是否扩展用例、编译、运行和提交，等待本轮评估。
