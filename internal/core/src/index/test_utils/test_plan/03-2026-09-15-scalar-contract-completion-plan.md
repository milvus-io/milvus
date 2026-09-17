# 剩余标量契约测试实施计划

> 执行方式：Astra 负责范围、设计、任务编排和验收；Sol 5.6 xhigh 子代理负责源码盘点、实现与验证。用户已授权计划完成后直接执行，无需再次确认。
>
> 验证顺序：计划内的测试代码全部完成并经过静态检查后，才开启集中编译和运行。集中验证发现测试、框架或构建配置错误时继续修正并重新验证。禁止修改生产实现、修复生产 bug、弱化正确预期或跳过失败后端。

**目标：** 在现有独立 `index_tests` 中补完 #64、#65 标量索引和标量过滤相关、能用当前声明式测试框架表达的剩余普通契约测试。

**架构：** 保留统一后端配置表、惰性数据集、契约本地用例和独立 GTest 参数实例。按实际接口需要补充取值、候选集、JSON 路由及构建生命周期的测试适配，不引入 Collection、Segment、FieldSchema、执行计划或 #67 服务。真实索引用于查询语义；小型局部伪对象仅用于独立所有权或接口协议检查。

**技术栈：** C++20、GoogleTest、现有 CMake/Conan、真实标量引擎、内存 Artifact IO、隔离临时目录。

## 1. 基线与边界

- 工作分支：`SegcoreRefactor/6-tests-index`；实现 HEAD：`af90d325473284279ae680b14ddde1f9b9c82464`。
- #64：`b7cfc84da8ad03ca3b33daca4242a73cacfa614c`，contracts/L1 artifact；#65：`f672fd596774bb25f47a138a5ce59983932954d3`，sealed scalar families。已通过远端 PR 元数据核对，本地最新实现包含两层。
- master 测试基线：`a876f471053edb2f68a06a9894afee9810ea7906`，与上一阶段保持一致。
- 当前测试与框架均为本地未提交工作；保留它们并继续追加，不 amend、重写 stack、创建远端消息或修改生产实现。
- 现有验证基线：7,960 个测试，7,816 通过、144 个生产行为失败，0 跳过、0 崩溃。144 个失败为 Marisa NUL 74、Inverted NUL 18、Inverted 正负零 52。
- 上阶段已经完成 ScalarPredicateReader 和 PatternMatchReader 的普通 row-domain 测试。仅为本轮新增输入形状、后端或能力补充相关用例，不重写已有 oracle。
- #66 的具体 vector/growing 引擎、#67 的构建服务和执行消费者不在本轮。索引不执行元素到行投影、原值精确复核或 Segment 级能力聚合。
- “能放到当前框架”包括必要的小型通用扩展；不能因为 demo 暂无类型或结果形式而直接排除普通测试。真正需要独立资源故障注入、并发发布、执行计划、远端存储或兼容性设施的用例明确列为暂缓。

## 2. 盘点与覆盖台账

三个 inventory 必须将接口方法、master 用例、生产支持路径、计划用例、预期失败与暂缓理由对应起来：

- `06-remaining-core-contract-inventory.md`：Null、Value、Reader 元数据。
- `07-remaining-specialized-contract-inventory.md`：Text、Ngram、Spatial、JSON。
- `08-remaining-lifecycle-contract-inventory.md`：#64/#65 文件范围、Registry、Builder、Artifact 和普通 family 路径。

三个 inventory 已完成并与下列任务对应，实施范围和 API 已冻结。不能仅以接口声明存在或测试数量增加作为覆盖证据。

## 3. 共享框架约束

1. 后端只在统一 catalog 中声明，复用生产 registry/caps；不得在各 contract 测试复制后端列表或引擎选择实现。
2. 数据集在单个测试执行时生成。注册参数仅保存数据集/后端描述和查询参数，不物化所有数据、Reader 或期望位图。
3. 每个后端、数据集、算子参数、读取/构建模式的有效组合是独立 GTest 实例。允许单次 Gather 的多个回调和一次生命周期操作中的多项断言，不允许一个测试循环运行所有配置组合。
4. 输入 owner 保留字符串、JSON、数组和 batch 的实际后备数据直到 Build 返回；测试有意验证 Build 后释放输入、Open 后释放中间产物的能力。
5. 精确过滤继续使用完整位图相等比较。候选集使用明确的包含/收缩约束，必要时提供手写候选位图；不能把候选超集伪装成精确结果。
6. 查询出错的预期仅包围查询；构建、加载和接口取得失败不能替代预期的查询错误。
7. 空接口、能力拒绝、合法空结果、NULL、missing path、cast 不支持必须分别表达。
8. 只增加被实际测试使用的 helper。测试源码继续放在对应生产文件旁边，`unittest/` 仅修改 CMake 收集。

## 4. 实施任务

### 任务 1 — 共享适配和后端/数据目录

**Owner:** framework_support。

**Files:** `index/test_utils/ScalarTestData.h`、`ScalarDataSets.cpp`、`ScalarReaderFactory.h/.cpp`、`ScalarReaderBackends.cpp`、`FilterTestDriver.h`；确有重复用途时才拆出新 helper。

- 为剩余 scalar family 增加实际支持的配置和输入适配。
- 支持非过滤输出、候选集检查、必要的 JSON path/typed input 及生命周期观察。
- 扩展现有 `BackendSpec`，显式保存 logical field/value/array-element type、expected domain、Serialize/Consume 打开模式、必要的按 case 补全参数和 Artifact 包装回调。所有后端仍只在 `ScalarReaderBackends.cpp` 登记。
- 输入形状用于数据适配：普通 Scalar（包含 Text/Ngram 的普通字符串输入）、ArrayRows、NestedElements、SpatialWkb、JsonDocument、JsonProjected。C++ 类型和输入形状共同筛选，避免 `string_view` 的 WKB、JSON 和普通字符串互相误用；不复制一份 family/capability 表。
- `ReaderBackend` 提供 Build/Open/Create 三个入口；Create 仍组合前两者。公开 ExpectedDomain/ExpectedValueType/OpenMode，新增 `All<T>` 和显式输入形状选择，保留现有 predicate/pattern 默认行为。
- 完整 caps 比较使用 Open 实际选择的 concrete LoaderEntry 和实际加载元数据，尤其是 Hybrid；不能用候选 family 的能力交集代替，不能在后端 descriptor 中保存可变的上一次选型。
- 将已有内存 Artifact IO 原样提取到 `test_utils/TestArtifactIO.h/.cpp`，公开 Data/Sink/Source；保留当前局部文件原子发布和清理行为，避免重写 IO 设施。
- 通用 `ReaderObservationCase<T>` / `ReaderObservationCases::Add` 复用 `FilterParam`，记录 name、dataset、shape、domain、可选 capability/logical type/backend-name 筛选和 typed observe 回调。每次执行生成相互独立的 expected 与 input 数据；input 和借用视图销毁后再调用 observe，以验证输入所有权。
- 最终 observe 签名为 `void(const ReaderBackend&, const ScalarTestData<T>&, IndexReaderBasePtr&)`；普通查询借用其 const 接口，所有权用例可 reset Reader 后检查拥有的 Lookup 结果。共同元数据检查在回调之前完成，之后不再解引用可能已释放的 owner。
- 后端公开只读 `BuildParams()`；case 的可选 `select_backend` 在基础类型/shape/domain/caps 匹配后进一步筛选 analyzer/gram 配置。默认运行全部匹配后端，不解析 backend 名字或复制配置表，最终匹配为空仍报错。
- NullReader 没有独立 caps 位，不在测试侧发明与生产继承重复的能力表；其选择规则在 inventory 阶段明确。
- 支持 row/element 元数据的普通输入，不引入列 offsets 或投影执行器。
- 先约定调用 API 和文件所有权，供 contract 作者使用；实现期间仅做静态验证。

### 任务 2 — Null、Value 与读取器自描述

**Owner:** scalar_contract。

**Files:** `contracts/query/NullReaderTest.cpp`、`ScalarValueReaderTest.cpp`、`IndexReaderTest.cpp`。

- Null：IsNull/IsNotNull，全部有效、无 validity、混合、全部 NULL、单行、跨 bitset 边界、多 batch 和 element-domain。
- Lookup：全部支持类型、正常值/NULL、重复值、字符串拥有数据；保留结果跨后续查询和 Reader 生命周期的合法拥有语义。
- Gather：空请求、单个/多个/乱序/重复 offset、NULL、全部有效和多 batch。按回调 `i` 写回，检查每个请求恰好回调一次；不能要求回调顺序。`valid=false` 不解引用指针，字符串在回调内复制。
- Reader：Count、ValueType、CoordDomain、逐字段 caps 与 DeriveCaps 一致，能力对应实际接口，candidate 的 exact=false。Text 不“隐含” NullReader 并非禁止实现该接口；实际 TextIndexReader 提供 NullReader，按真实公开接口测试，不虚构接口缺失断言。资源字段仅断言公开契约，不能将 sentinel 当成实测零或设私有实现字节常量。

### 任务 3 — Text、Ngram 与 Spatial

**Owner:** pattern_contract，必要时在完成后顺序接受下一组任务。

**Files:** `contracts/query/TextMatchReaderTest.cpp`、`NgramReaderTest.cpp`、`SpatialReaderTest.cpp`，以及有实际专属断言的对应 family `*Test.cpp`。

- Text：固定 analyzer 和小型/生成语料；MatchQuery/min_should_match、PhraseMatchQuery/slop、FuzzyMatchQuery/edit distance。复杂语义允许手写结果，不复制全文检索算法。
- Text 的文件模式和 RAM 消费模式按真实支持能力分别登记；转换/持久化的生命周期断言归 Task 5，查询用例复用。
- Ngram：CanHandle 的算子/长度边界；从全候选、预过滤候选和空候选开始，结果不得增加原候选位，必须保留原候选内手写真值。公共 contract 不要求保留或排除特定 false-positive；少量当前 Phase1 精确候选回归放在 NgramIndexReaderTest，使用中心 catalog 中已命名的该 family profile。
- Spatial：明确几何和手写真实命中，覆盖全部 SpatialOp、NULL、边界、相离、相交和调用方已扩展的 DWithin 查询形状。公共 contract 检查 Count 与不漏必需候选；当前 RTree 的精确 MBR 集合和 invalid-query fallback 放入 RTreeIndexReaderTest。不能把当前实现的 false-positive/fallback 特点强加给全部 backend，不拉入 executor 或重写几何算法。
- 每个 contract 使用自己的 dataset 子集、参数目录和声明式 case；不复制后端配置。

### 任务 4 — JSON 路由及普通投影输入

**Owner:** scalar_contract，在 Task 2 的源码完成后继续实现；共享输入/后端仍归 framework_support。

**Files:** `contracts/query/JsonIndexReaderTest.cpp`，必要的 `scalar/json/JsonPathIndexReaderTest.cpp`、`JsonFlatIndexBuilderTest.cpp` 或对应已有生产文件旁的测试。

- JsonFlat 直接使用拥有的 JSON 输入；投影索引使用测试预先定义的值和状态，不调用 materializer。
- CastTypesOf、Exists、Resolve 分别覆盖支持/不支持路径、支持但全缺失、field-null、path-missing、present value、类型选择。
- Resolve 后复用普通谓词/Null/Pattern 语义，保持父 Reader 存活；数字预期保留 int64 与 double 精度差异。
- 检查 advertised cast 与可 Resolve 类型的契约一致性。当前实现如返回与声明不一致的 marker，记录并保留失败测试，不把该值硬编码为正确预期。
- JsonResolvedReader 的 owned/borrowed/empty/move 所有权用小型局部对象验证；不构造 Segment/cache pin。
- 普通 ARRAY/nested/projection 状态的可适配范围由 inventory 明确；仅缺少 demo 适配不是暂缓理由。
- ARRAY row 输入覆盖所有支持的元素类型、null row、有效空数组、重复元素与多 batch。int8/int16 使用生产要求的 int32 物理存储；nested 使用预先平坦化的元素和 absent validity，不做行投影。

### 任务 5 — 标量构建、Artifact 与 Registry

**Owner:** framework_support 负责 Registry、Builder、Consume、Bitmap/Sorted/Inverted/Hybrid/Text/RTree 生命周期及全部 CMake；scalar_contract 在 Task 3 独立检查后负责 MarisaIndexArtifactTest、FmIndexArtifactTest；pattern_contract 在 Task 3 源码和 Task 2 独立静态检查完成后负责 FileSinkTest、FileSourceTest、LocalDirectoryTest。

**Files:** `contracts/build/ArtifactBuilderTest.cpp`、`ReaderConvertibleTest.cpp`、`contracts/RegistryTest.cpp`，以及包含实际被测机制的 `storage/artifact/*Test.cpp` 和 scalar family `*Test.cpp`。

- Build 返回后释放 builder 和输入，产物仍可 Serialize；Open 后释放 Artifact/IO/source，Reader 仍可使用。
- 只检查公开的元数据/条目/selector 与可见结果，不绑定私有布局。
- Text ReaderConvertible 支持路径、普通 scalar 不支持转换的路径、消费前后唯一所有权；不做隐藏的 serialize/load fallback。
- 生产 registry 的 scalar 支持类型和 loader/caps 注册；局部 test-only registry key 用于必要的协议检查，不能补注册真实 family 来掩盖生产缺失。
- Hybrid 阈值下/等于/超过阈值、NULL/重复值/跨 batch 基数与实际 selector/delegate。
- 现有内存 Artifact IO 能表达的普通元数据、序列化/加载、输入形状和失败边界纳入；需要另建复杂故障平台的部分明确暂缓。
- 新增 `storage/artifact/FileSinkTest.cpp`、`FileSourceTest.cpp`、`LocalDirectoryTest.cpp`，覆盖 NamedBuffer 状态/二进制条目/小型分片、原子 materialization、路径边界和最后 owner 清理。远端 V1/V3 transport、packed container、磁盘向量 handle 和强杀/故障平台暂缓。
- V1/V2 按独立存储布局各覆盖一次：Bitmap numeric/string、Sorted numeric/string、Inverted、Marisa、Hybrid low/high；FM 验证明确拒绝。V3 继续覆盖全部 primitive 模板，不把版本再乘进全部查询用例。
- 每种独立存储形状补一例缺失必需条目、一例损坏的公开元数据关系和一例无效 payload；只变造具体输入，不复制私有 codec。转换协议使用小型局部 tracking 类型。

### 任务 6 — 静态交叉验收与集中编译运行

**Owner:** Astra 验收、各代理交叉检查；framework_support 独占构建树和测试运行。

1. 所有计划内测试源文件和共享扩展全部完成，inventory 无未归类接口或 master 用例。
2. 独立检查规格覆盖，再检查代码质量；修正测试问题。确认生产文件无行为修改、旧 144 个失败预期未被弱化。
3. 检查 CMake：所有新增测试仅进入 `index_tests`，测试 helper 不进入生产库；不新增 `unittest/*.cpp`。
4. 通过适用 formatter、`git diff --check`、注册名/类型/惰性生成的静态检查后，由 Astra 明确打开首次构建门禁。
5. 集中构建：`rtk proxy cmake --build cmake_build --target index_tests --parallel 6`（并行度按资源复核）。
6. 列举 GTest 并核对每个预期组合；全量运行生成独立日志/XML，确认未遗漏或以 skip 掩盖失败。
7. 测试/框架/构建错误允许修正并重新验证；生产错误只记录到临时未提交 Markdown，保留正确红测。
8. 最终用隔离 TMPDIR 验证索引临时文件清理，保留日志/XML/构建产物。报告实际耗时、通过/失败/跳过/崩溃、相对旧 144 个失败的变化及明确未覆盖范围。

## 5. 实施与验收记录

本节在 inventory 收敛、任务分发、静态验收和集中运行时更新。任何新增范围先落入任务/覆盖表，再实现；不得将编译问题或生产 bug 转成未说明的范围缩减。

### 2026-09-15 源码阶段启动

- 三份 inventory、单一后端 catalog API、候选集断言规则、JSON 契约冲突处理和文件所有权均已确认。
- framework_support：共享适配/数据入口/CMake + Task 5。
- scalar_contract：Task 2，完成后 Task 4；仅修改自己负责的 contract 测试和分配的数据定义文件。
- pattern_contract：Task 3；仅修改自己负责的 contract/family 测试和分配的数据定义文件。
- 多个数据定义文件可以通过 `ScalarDataSets()` 的唯一入口注册，以避免并发编辑同一个源文件；后端定义始终只在统一后端表。不得在 contract 测试中各建一套配置表。
- 首次 configure/build/test/list 门禁：**关闭**。全部源码和交叉静态验收完成后才开启。

### 源码里程碑

- Task 1：共享 API、observation driver、输入适配、Artifact IO 和中心后端源码已落地；静态检查待完成。后端表声明 434 个配置，实际注册与运行数待集中验证。
- Task 2：NullReaderTest、ScalarValueReaderTest、IndexReaderTest 源码完成，作者静态格式/差异检查及独立规格/质量检查通过。静态预计 161 个 descriptor / 1,738 个参数；实际参数数目待集中列举核对。全十字段 Caps 校验由共享 Open 使用实际加载器及元数据执行，外层不复制推导。
- Task 3：Text/Ngram/Spatial 和 19 个惰性数据集源码完成，补齐独立检查发现的 Jieba Phrase 和 UTF-8 长度边界后，声明 157 个 case，静态预计展开 1,355 个 GTest 参数；独立检查已通过。
- Task 4：JsonIndexReaderTest 和 JsonDataSets 源码完成，已补齐 BOOL 六比较、四种区间边界及独立检查发现的 LIKE、拒绝协议和 resolved child 元数据；静态预计 133 个 descriptor / 980 个参数，加 1 个所有权测试。审查修正待独立复核关闭。CastTypesOf/Resolve 不一致的正确失败预期保留，问题记入临时文档。
- Task 5：Registry/Builder/Consume 与多数 family 生命周期由 framework_support 实现；Marisa/FM 两文件已独立分给 scalar_contract，三个 storage/artifact 文件由 pattern_contract 实现。缺失分片的崩溃风险原计划用子进程断言隔离，但该代理两次被工具内容安全检查中止；已停止重试此动作，将这一需要进程隔离的用例单列为工具阻塞，临时记录静态问题。其余普通 IO/生命周期用例继续。尚未进入编译/运行阶段。

### 工具阻塞的单例

- `NamedBufferSource` 的 slice metadata 引用缺失分片时，静态发现旧 `Assemble` 路径空指针解引用。直接放进普通同进程用例会中断整轮。
- 原拟使用 GTest 子进程断言保留失败；负责代理连续两次被工具内容安全检查中止（提示 possible cybersecurity risk）。停止重试该动作，也不换工具绕过。此单例暂缓并在临时问题表记录；不是通过、预期成功或静默 skip。
- 集中验证仍等待其他全部计划内源码及交叉检查完成。最终结果须说明这个明确缺口。

### 全部普通源码已齐备，最终静态验收中

- Task 1：434 个统一后端配置；pattern_contract 最终独立检查共享框架。
- Task 5：framework_support 负责的源码已冻结，scalar_contract 检查 Registry/Builder/Consume/各 family/CMake；Marisa/FM 各 16 个用例由 pattern_contract 复核。
- Storage：补独立手工合法三分片后共 37 个用例，framework_support 独立检查已通过。
- 首次 configure/build/list/run 闸门仍关闭，等待上述静态检查及修正全部关闭。

### 候选 contract 的预期边界修正

- 最终按 INgramReader.h / ISpatialReader.h 复核发现，初版 inventory 将当前 Phase1/MBR 输出误当成所有实现的共同保证。公共接口承诺的是候选超集，允许合法实现更精确或有不同的误报集合。
- Ngram 公共 case 移除固定 candidate 位图相等，保留手写真值不漏、Count、AND 收缩与 CanHandle。Spatial 公共 case 使用每个 op 的手写真值；未承诺的 invalid fallback 不作为共同要求。
- 代表性的实现特性回归放在两个 colocated family 测试文件，继续复用当前 driver、lazy dataset 和中心 catalog 已命名 profile，不复制后端配置。两个文件及原公共断言的变更由 scalar_contract 独立复核，完成前仍不构建。

### 首次集中验证门禁已开启

- UTC: 2026-09-14T20:16:58.496851+00:00。全部普通测试源码和交叉静态验收已完成；明确受工具阻塞的单例仍单列为未覆盖。
- root 复核 126 个生产源码哈希均未改变；CMake 30 个测试源条目均存在、无重复；共享 helper 及独立 binary 的源码隔离已核对。
- 预期总数：旧 7,960 减去被全类型 Value suite 替代的 6 个 Lookup，保留 7,954；加 Task2 1,738、Task3 1,362、JSON 981、Task5 净新增 837，合计 12,872。实际列举必须核对，不能以静态预测替代执行结果。
- framework_support 独占首次 configure/build/list/run；源码修正、日志、XML、基线比对和 TMPDIR 清理由其统一协调。生产代码 bug 不修，正确失败预期保留。

### 集中编译与注册核对

- 全部源码齐备后开始集中编译；已修复测试侧 move-only bitmap 复制、非 const 左值的整体比较、括号语法、完整 JSON 类型包含、私有构造调用、直接链接依赖，以及 Ngram 后端缺失加载参数的问题。未修改生产代码。
- `index_tests` 已编译链接成功，重新列举注册成功：12,880 个唯一 GTest 名称，无重复。比旧 7,960 个测试净增 4,920。
- 旧测试精确保留 7,954 个；6 个旧 int64 Lookup 参数由新的全类型 `ScalarValueReaderTest` 矩阵替代，最终验证报告须列出映射。
- 静态预测少算的 8 个来自 `Utf8ChinesePostfix`：实际适配三个逻辑字符串类型，各含 heap/mmap 与 nullable/non-null 共 12 个，原台账只计 VARCHAR 的 4 个。正确注册保留，Task 3 实际为 1,370 个。
- 正在进行隔离 TMPDIR 的全量运行。注册成功不代表测试通过，运行结论以最终 XML 为准。

### 第一轮运行与测试侧诊断

- 第一轮实际执行 12,880 个测试：12,011 通过、869 失败、0 disabled、0 errors，无崩溃。GTest 218.610 秒，进程 wall 219.50 秒。
- 旧 144 个失败逐项保持，无旧测试新增失败；本轮新增失败 725 个。分布为 JSON 708、ScalarValue 8、Ngram 6、Hybrid 1、InvertedArtifact 1、TextArtifact 1。
- JSON 中 704 个尚未通过构建/加载，不能将它们直接归为生产实现错误，也不能据此声称内部接口已得到验证。作者根据 contract 和 master 核对配置后交由统一构建验证。
- Hybrid 输入长度与 batch 划分不匹配已识别为测试错误；其他失败仍按实际实现路径和契约逐项分类，禁止改预期以迎合生产行为。
- 隔离 TMPDIR 前后均为空，未产生 core。完整日志与 XML 位于 `/tmp/segcore-index-test-run-20260915-041720/`。

### 测试侧修正与最终执行结果

- 修正 JsonFlat 被无条件传入 typed-projection annotation 的框架错误；保留实际 loader 的全部 capability 校验。移除生产明确不支持、master 也未使用的 4 个 JsonFlat V5 正向配置，保留 V7 heap/mmap 与 nullable/non-null 的全部逻辑 case。
- 修正 Hybrid 输入与 batch 划分；Ngram 两字符 LIKE 按 min2 的支持与 min3 的拒绝分别注册，组合总数不减少。全部测试侧修正经过独立静态复核，再集中编译与运行。
- 最终编译成功；统一 catalog 430 个配置，实际注册 12,524 个唯一 GTest，相对旧阶段净增 4,564。旧 7,954 个名称均保留，6 个 Lookup 的语义替代映射见 `18-lookup-case-mapping.md`。
- 不加 filter 的运行在 JsonFlat `RegexPattern` 崩溃，无法完成整轮。四个 V7 组合均单独复现 SIGABRT/exit 134，源码用例继续启用，未写 skip/disabled 或改变正则预期。
- 仅将这四个已独立执行的崩溃名称排除后，其余 12,520 个测试完成：12,306 通过、214 断言失败、0 skip、0 disabled、0 errors；GTest 174.849 秒，进程 wall 175.73 秒。
- root 独立对比最终 listing、remaining XML 和四份 crash log/status：两组名称不相交，合计精确覆盖全部 12,524 个注册名，无遗漏、重复或额外名称。四个崩溃独立计入，不计为通过或跳过。
- 旧 144 个失败逐名保留，旧保留用例无新增失败。新增 70 个断言失败与 4 个崩溃按生产路径记入临时问题文档，正确期望不改；生产源码 126 份哈希复核无变化。
- 正常完成的 12,520 个测试自动清理了隔离 TMPDIR；两个 mmap 崩溃进程各留 1 目录、10 文件，共 3,051 字节，记录 manifest 后由 runner 人工清理。不得将崩溃后的人工清理描述为析构正确。最终已知 `/tmp` 前缀、cwd core、`/var/crash` 相对基线无新增残留。
- 最终验证报告：`37-2026-09-15-scalar-contract-run-summary.md`；清理报告：同目录 `38-2026-09-15-scalar-contract-cleanup-summary.md`。完整 XML、日志、退出状态和构建产物保留供复查。
- 工具内容安全检查阻塞的缺失物理 slice 子进程测试仍是明确未实现、未执行的单例；不包含在 12,524 个已注册测试中。
