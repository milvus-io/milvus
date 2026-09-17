# 剩余查询/核心标量契约清单

清单日期：2026-09-15。当前实现 HEAD：`af90d325473284279ae680b14ddde1f9b9c82464`。契约提交 #64：`b7cfc84da8ad03ca3b33daca4242a73cacfa614c`（`#2-index-contracts`）。标量系列提交 #65：`f672fd596774bb25f47a138a5ce59983932954d3`（`#3-scalar-families`）。Master 参考固定为 `a876f471053edb2f68a06a9894afee9810ea7906`。

这是仅基于源码的清单。未运行 configure、build、测试二进制或测试列表命令。现有运行时基线仍为 7,960 个已注册/执行、7,816 个通过和 144 个已知生产失败（Marisa 嵌入 NUL 74 个、Inverted 嵌入 NUL 18 个、Inverted 正负零 52 个）。

## 公开方法与不变量

### `IndexReaderBase`

源码：`contracts/query/IIndexReaderBase.h:43-85`、`contracts/README.md:15-22,48-79`。

- `Caps()` 必须等于唯一已打开清单条目由元数据派生的 caps。不得将无关条目的 caps 作 OR。
- 普通标量索引的 `CoordDomain()` 为 `Row`，已平坦化嵌套 ARRAY 索引为 `Element`。
- `Count()` 是该域的基数。谓词和 null 位图恰有 `Count()` 位。
- `ValueType()` 是索引的原始类型或 VARCHAR 值类型；嵌套读取器仍报告元素值类型。
- `MemoryUsage()` 报告拥有的堆字节。`CellByteSize()` 区分拥有的内存/文件字节；原生统计不可用时允许文档化的全零 sentinel。通用测试可要求值非负且稳定。由于契约允许文档化的旧版加载后缓存估计，不能一律将内存部分等同于 `MemoryUsage()`；也不能要求 mmap 文件字节为正或绑定私有字节常量。
- `IndexReaderBasePtr` 是唯一所有权。构建输入、构建器、Artifact、sink/source 及用于打开它的持久化内存缓冲区离开作用域后，已加载读取器仍必须可用。

### `ReaderCaps`

源码：`contracts/query/ReaderCaps.h:19-58`。

精确字段为 `predicate`、`pattern_match`、`text_match`、`ngram_candidates`、`spatial`、`nested`、`value_lookup`、`cheap_value_lookup`、`json_paths` 和 `exact`。`cheap_value_lookup` 蕴含 `value_lookup`。`nested` 表示元素坐标。Ngram/spatial 候选声明 `exact=false`；普通行标量读取器声明精确结果。`NullReader` 有意没有 caps 位。

对于普通标量清单，正接口检查为 predicate、仅字符串模式、声明时的 value lookup 和 NullReader。负检查为 TextMatchReader、NgramReader、SpatialReader 和 JsonIndexReader。具体 TextIndexReader 当前也实现 NullReader；头文件中独立文本产物“不蕴含” NullReader 的措辞不禁止具体读取器提供它。Text 的正/null 行为属于 Text 套件。

### `ScalarValueReader<T>`

源码：`contracts/query/IScalarValueReader.h:33-68`。

- `Lookup(offset)` 返回 `optional<owned_t<T>>`；`owned_t<string_view>` 为 `string`。成功结果拥有其字节，后续查询和读取器销毁后仍有效。null 坐标返回 `nullopt`。
- `Gather(offsets,count,callback)` 可按任意顺序访问请求结果位置。回调 `i` 标识结果位置，未必是输入行或回调顺序。包括重复 offset 在内，每个请求位置必须恰产生一次。`valid=false` 是权威状态，值指针不得解引用。有效借用字符串在回调内复制，因为其 view 仅在该回调中有效。
- `(nullptr,0,callback)` 是现有实现覆盖的定义空请求形状。负 count、非零 count 配 null offsets 和越界 offsets 是实现断言，但公开契约未规定，故不增加跨系列错误码期望。

当前实现者：Bitmap 和 Sorted 覆盖每个支持的原始/VARCHAR 类型；Marisa 覆盖 VARCHAR；numeric/bool Hybrid 总解析为 Bitmap 或 Sorted，适用于通用值能力选择。标量 VARCHAR Hybrid 可解析为 Inverted，已由 `Supports(value_lookup)` 正确排除。Inverted 和 FM 未实现 value lookup。

### `NullReader`

源码：`contracts/query/INullReader.h:21-37`。

`IsNull()` 和 `IsNotNull()` 返回读取器域中 `Count()` 位的独立拥有位图。二者是精确补集。存在的全有效 validity 位图和缺失的 validity view 逻辑结果相同但构建输入形状不同。对平坦化嵌套输入，父级 null/空行已移除，故每个提供的元素坐标均非 null；`IsNull()` 为空，`IsNotNull()` 覆盖全部元素。

共享普通清单中的当前实现者为 Bitmap、Sorted、Inverted、Hybrid 所选 delegate、Marisa 和 FM。候选实现 Ngram 和 RTree 也提供 NullReader，TextIndexReader 当前亦提供；但其配置/数据属于专用查询套件，而非此处复制的后端表。

## 现有测试覆盖与缺口

`contracts/query/ScalarValueReaderTest.cpp` 仅有一个针对 `RepeatedNullable` 的 int64 `Lookup` 测试，展开为 6 个具 value 能力的行后端配置。它未覆盖 bool/int8/int16/int32/float/double/string、Gather、缺失 validity、全 null、重复/全等值、批次、高基数、元素坐标、回调协议、输入生命周期、借用字符串复制或拥有的 Lookup 生命周期。

不存在 `contracts/query/NullReaderTest.cpp` 或 `IndexReaderTest.cpp`。过滤测试附带断言行 `Count()`/domain 和所选 op caps，系列守卫断言少数正能力。它们未提供完整的十字段 caps 比较、通用负接口检查、资源不变量、输入/source 生命周期、NullReader 覆盖或元素域元数据。

现有共享行清单有 136 个后端配置：

- Bitmap、Sorted、Inverted、Hybrid：8 类型 x nullable/non-nullable x heap/mmap = 128。
- Marisa 和 FM：VARCHAR x nullable/non-nullable x heap/mmap = 8。

所有当前后端配置均为行域。当前 `BackendCatalog::For` 需要 ReaderCaps 成员，故不能选择 NullReader。`FilterTestDriver` 硬编码 `Domain::Row`。若不做域筛选而添加元素后端，会错误地将现有行数据集注册到嵌套 ARRAY 元数据。

## Master 源码到契约映射

- `ScalarIndexTest.cpp`：为 int8/int16/int32/int64/float/double 实例化的 `TypedScalarIndexTest.Count` 和 `Reverse` 映射到基础 Count/ValueType 加所有行的有类型 Lookup。`Codec` 映射到共享工厂使用的强制 build/serialize/open 路径。
- `StringIndexTest.cpp`：`StringIndexMarisaTest.Count` 和 `Reverse` 映射到 VARCHAR base/Lookup。`IsNull`、`IsNullHasNull`、`IsNotNull` 和 `IsNotNullHasNull` 映射到 all-valid/mixed/all-null Null 用例。
- `StringIndexSortTest.cpp`：`ReverseLookupMemory`、`ReverseLookupMmap` 及 `SerializeDeserializeMemory` 后的 reverse 检查映射到两个后端配置的有类型 Lookup。`NullHandlingMemory`、`NullHandlingMmap` 和独立 nullable/load 用例映射到 Null 用例。其 invalid-offset `nullopt` 行为不迁移，因为新接口未规定 invalid offset 行为，当前实现改为断言。
- `BitmapIndexTest.cpp`：每个 V1-V6 `CountFuncTest`、`IsNullFuncTest` 和 `IsNotNullFuncTest` 映射到 base 和 Null 结果。Missing-binlog/default 行合成是上游 materializer/build 契约，已 materialized 的 all-valid/null reader 结果在此覆盖。
- `HybridScalarIndexTest.cpp`：V1/V2/Nullable/V3/V4 Count/IsNull/IsNotNull 映射到低/高 delegate 上的 base 和 Null 行为。Selector 阈值和 missing-row/default 合成仍是 Hybrid/build 生命周期测试。
- `InvertedIndexTest.cpp`：nullable/non-nullable IsNull/IsNotNull 检查和 `SealedAllValidDoesNotRetainValidityBitmap` 映射到逻辑 all-valid/mixed/all-null 用例。私有 validity-allocation 字节检查仍是系列/资源测试。
- `FMIndexTest.cpp`：`SerializeLoadRoundTripNoMmap` null 结果和 `NullVsEmptyStringDistinctAfterReload` 要求显式数据集：一个有效行和一个 null 行有相等的空 payload 字节。heap/mmap 往返来自共享工厂后端配置。
- `BitmapIndexArrayTest.cpp`：`BuildAndLoadElementLevelBitmap`、`NullableNullsBeforeValidElementBitmap`、`NullableNullsBeforeValidUnifiedLoad` 和 nested sort 测试建立 `Domain::Element`、元素 Count、全有效元素 null mask 和元素 reverse lookup。父行 validity/offset 平坦化发生在 `ScalarBuildInput<T>` 前，不在此查询测试重建。
- `InvertedIndexArrayTest.cpp`：`NestedSealedValidityUsesMaterializedElementDomain` 建立全有效元素域 Null 结果和元素 Count。ARRAY equality/contains 投影和 executor 精炼不属于这些 base/Null/Value 方法。
- `NgramInvertedIndexTest.cpp`、`RTreeIndexTest.cpp` 和 `TextMatchIndexTest.cpp` 包含 Count/null 检查，但其 candidate/analyzer/geometry 输入和声明 caps 归专用 Text/Ngram/Spatial 契约套件所有。应在其中复用基础断言，而非注册重复后端定义。

公开契约要求但 master 之外的缺口：bool value lookup；每个支持类型在所有适用系列下的行为；跨越第 63/64 位的 packed validity；空/间插批次；Gather 空/单个/置换/重复/null 协议；输出位置回调语义；结果独立性；输入销毁；字符串借用/拥有生命周期；明确的无关接口负检查；精确的全 caps 比较。

## 共享框架冻结的 API/数据请求

使用一个类型安全的非过滤观察运行器，与 `FilterCases` 并列，而非按类型测试夹具：

- 注册仅保存用例名、数据集名、后端描述符和回调。数据生成/构建/打开在 GTest 主体中进行。
- 选择支持所有有类型后端或一个 ReaderCaps 成员，并始终按数据集 nullability 和坐标域筛选。零个合格后端是错误。
- 运行器分别生成期望数据副本和构建输入副本，从后者构建，销毁 `ScalarTestInput` 及其 owner，并只在 `ReaderBackend::Create` 亦销毁 builder/Artifact/sink/source/持久化临时缓冲区后调用回调。
- 回调接收 `const ReaderBackend&`、`const ScalarTestData<T>& expected` 和 `const IndexReaderBase&`。契约文件自行执行转换/断言。

所需清单形状：

- 向 `BackendSpec`、`ReaderBackend` 和 `ScalarDataSet` 添加 `Domain`（或等价的 `nested` 加派生域）。
- 现有 `For<T>(cap,requires_nullable)` 保持行域默认行为。增加坐标感知 overload 和 `All<T>(requires_nullable,domain)`；不增加 Null 能力表。
- 嵌套元数据为 `field_type=ARRAY`、`array_element_type=value_type=T`、`nested=true`、`nullable=false`；嵌套数据集使用缺失 validity。
- 为 8 个支持值类型的 Bitmap、Sorted、Inverted、Hybrid 添加 non-nullable heap/mmap 嵌套后端配置：8 类型 x 4 系列 x 2 load mode = 64 元素后端配置。其 loader caps 为 predicate/nested、支持时的字符串模式、`exact=false`；仅 Bitmap 和 Sorted 始终具 value 能力。Hybrid ARRAY 可选择 Bitmap 或 Inverted，故通用 Hybrid 值选择仍排除。
- 更新 `RunFilterCase`，断言后端/数据集期望域而非硬编码 Row；现有行数据集注册不得选择元素后端配置。

所需惰性数据集：

- 复用 `PredicateEdges`、`PredicateAllValid`（缺失 validity）、`PredicateAllNull`、`PredicateSingleRow`（存在的全有效）、`PredicateAllEqual`、`PredicateEdgesMultiBatch`、`PredicateEdgesWithEmptyBatches`、`PredicateFloatInfinities` 和 `TenThousandHighCardinality`。
- 添加 `NullVsEmptyString`：`{"", "", "x"}`，仅第二个空字符串为 null，存在 validity，行域。
- 添加 `BitBoundaryNullable<int64_t>`：至少 70 个确定值，存在的 validity 在 63/64 边界两侧包含 null，并有非字节对齐的多批切分。
- 为全部 8 类型添加 `NestedElements<T>`：确定的平坦化元素值、缺失 validity、元素域。它不含行 offsets，因为读取器契约有意不拥有投影。

CMake 只需将 `contracts/query/NullReaderTest.cpp` 和 `contracts/query/IndexReaderTest.cpp` 添加到 `INDEX_TEST_FILES`；`ScalarValueReaderTest.cpp` 已存在。没有测试源码属于 `unittest/`，helper 仍排除在 `milvus_index` 外。

## 计划的契约矩阵

### NullReader

每个描述符调用两个方法，检查精确期望位和 `Count()` 大小，验证互补性，修改一个返回位图后再查询，以证明返回位图独立性。

- 所有 8 类型：在未按 caps 位选择的全部普通行后端上使用 `PredicateEdges`、`PredicateAllValid`、`PredicateAllNull`、`PredicateSingleRow`。
- 代表输入形状：字符串 `PredicateEdgesMultiBatch`；int64 `PredicateEdgesWithEmptyBatches`；跨 packed-bit 和批边界的 int64 `BitBoundaryNullable`。
- VARCHAR `NullVsEmptyString` 覆盖 Bitmap/Sorted/Inverted/Hybrid/Marisa/FM 的 nullable heap+mmap 后端配置。
- 所有 8 类型 `NestedElements` 覆盖全部 64 个元素后端配置，期望零 null 和全部元素非 null。

使用所需的 136 行 + 64 元素清单，这为 512 个独立 GTest 参数：68 mixed + 136 absent-all-valid + 68 all-null + 136 present-all-valid-single + 12 string multi-batch + 8 empty-batch + 8 bit-boundary + 12 null-vs-empty + 64 nested。

### ScalarValueReader

Lookup 描述符：

- 所有 8 类型：mixed `PredicateEdges`、absent `PredicateAllValid`、all-null `PredicateAllNull`、repeated `PredicateAllEqual`。
- int8/int16/int32/int64/float/double/VARCHAR 的高基数边界 offsets。
- 通过 `PredicateFloatInfinities` 覆盖 float/double 无穷值。
- 所有 8 类型 `NestedElements` 覆盖具 value 能力的 Bitmap/Sorted 元素后端配置。
- VARCHAR owning-lifetime 用例保留一个 80 字节 Lookup 结果，执行另一次查询，销毁读取器，再比较保留字符串。

Gather 描述符：

- 所有 8 类型：独立的空、single-offset、permuted/duplicate/mixed-null、absent-validity、all-null 请求。
- 代表性的 int64 和 VARCHAR 多批请求、int64 间插空批次及全部 8 个元素类型。
- 回调按 `i` 存储，接受任意回调顺序，对 `i` 做边界检查，每个结果位置递增一个访问计数器，立即复制有效值，`valid=false` 时绝不解引用指针，最后要求每个请求结果位置恰一次。

全部比较使用逻辑有类型值。Float 相等性遵循旧 reverse 测试使用的公开标量值相等性；不为正负零创造未文档化的位模式要求。嵌入 NUL 保持长度感知。针对 `PredicateEdges`/`PredicateAllValid` 的 Marisa Value 用例预计暴露已有文档的生产截断缺陷，必须保持启用。

### IndexReaderBase 与能力声明

- 每个现有 136 后端配置一个行域 `PredicateSingleRow` 用例。
- 每个 64 元素后端配置一个 `NestedElements` 用例。
- 对照 loader 派生 caps 断言 Count、ValueType、期望 CoordDomain、每个 ReaderCaps 字段、`cheap_value_lookup => value_lookup`、`nested == (domain==Element)` 和期望精确性。
- 断言声明的 predicate/pattern/value 能力具有对应 mixin，缺失的普通 caps 不具有。独立于 caps 断言每个普通标量后端配置均有 NullReader。
- 明确断言普通标量后端配置没有 TextMatchReader、NgramReader、SpatialReader 或 JsonIndexReader，且相应 caps 为 false。正候选/text/json caps 及候选 `exact=false` 由其所属专用套件使用同一基础断言 helper 检查。
- 断言 MemoryUsage 和 CellByteSize 非负且稳定。不得一律等同其内存值、要求 fallback 后端配置的物理 mmap，或绑定精确私有字节总数。

## 实际排除项与归属边界

- 无效 Lookup/Gather offsets、负 Gather count 和非零/null offset pointer：公开契约层未定义；当前系列断言细节未标准化。
- 零行 Reader：普通 builder 拒绝空输入；这是 builder 负契约，不是查询 Reader 用例。空 Gather 在非空读取器上覆盖。
- 父 ARRAY 行 offsets、null/空行平坦化、行投影、ARRAY equality/contains 语义和多层嵌套投影：上游 materializer/executor 工作。读取器接收已平坦化元素，直接测试元素域契约。
- Ngram 精炼、spatial 精确几何、Text 分词器/查询语义和 JSON path/cast 路由：独立专用契约文件。适用时必须复用 base/caps 检查，不复制后端表。
- missing-binlog/default 合成、load slicing、损坏、远程 IO、转换、selector 阈值机制、并发和精确私有资源统计：构建/产物/registry 或系列生命周期测试。
- 无需 fake Reader 测试 `std::unique_ptr` 机制。真实测试通过在输入和打开中间物销毁后查询，以及在 Reader 销毁后保留拥有的字符串 Lookup 结果证明所有权。

## 后续运行的已知语义风险

- Marisa 截断嵌入 NUL 输入和返回值；正确 Value 期望会向已有文档缺陷增加红测。不得跳过 Marisa 或规范化期望字符串。
- 必须对实际解析的 selector 比较 Hybrid 元数据。低基数行/元素基础 fixture 解析为 Bitmap；高基数能力测试仍是 selector/lifecycle 覆盖。
- 共享内存 source 在 `ReaderBackend::Create` 内销毁，因此返回后的任何失败是真实读取器所有权问题，而非 source 生命周期 fixture 假象。
