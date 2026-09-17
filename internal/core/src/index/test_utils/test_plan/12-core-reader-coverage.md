# 剩余查询/核心契约覆盖

源码快照：集成 HEAD `af90d32547`，契约 stack `b7cfc84da8` / `f672fd5967`，master 参考 `a876f471053edb2f68a06a9894afee9810ea7906`。这仅是静态源码计数。源码阶段未运行 configure、编译、测试列举或测试执行。

## 已实现文件和公开方法

| 文件 | 已实现的描述符组 | 所执行的契约 |
|---|---|---|
| `contracts/query/NullReaderTest.cpp` | `MixedValidity`, `AbsentValidityIsAllValid`, `AllNull`, `PresentAllValidSingleRow`, `NestedElementsAreAllValid`, `AcrossBatches`, `AcrossEmptyBatches`, `AcrossPackedBitBoundary`, `NullIsDistinctFromEmptyString` | `IsNull`, `IsNotNull`, Count-sized masks, exact complement, independent returned bitmap, row and element coordinates |
| `contracts/query/ScalarValueReaderTest.cpp` | `LookupMixedValidity`, `LookupAbsentValidity`, `LookupAllNull`, `LookupRepeatedValues`, `LookupNestedElements`, `LookupHighCardinalityBoundaries`, `LookupInfinities`, `LookupOwnsStringAfterLaterQueryAndReaderDestruction` | typed `Lookup` for bool/int8/int16/int32/int64/float/double/VARCHAR, nullable and absent validity, owned string result lifetime |
| `contracts/query/ScalarValueReaderTest.cpp` | `GatherEmptyRequest`, `GatherSingleOffset`, `GatherPermutedDuplicateAndNullOffsets`, `GatherAbsentValidity`, `GatherAllNull`, `GatherNestedElements`, `GatherAcrossBatches`, `GatherAcrossEmptyBatches` | `Gather(nullptr,0)`, callback `i` as result position, arbitrary callback order, duplicate offsets, exactly one callback per requested position, invalid pointer not dereferenced, callback-borrowed string copied immediately |
| `contracts/query/IndexReaderTest.cpp` | `RowMetadataAndInterfaces`, `ElementMetadataAndInterfaces` for all eight types | `Count`, `ValueType`, `CoordDomain`, actual-loader-validated ten-field caps, caps-to-mixin correlation, independent `NullReader`, nested/cheap/exact invariants, stable nonnegative resource reports |

共享 `ReaderObservationCases` 为构建输入创建第二个独立数据集，在调用观察回调前关闭输入所有者、builder、Artifact、sink/source 和序列化 buffer，并将打开的 caps 与从产物选择的具体 loader 比较。每个展开的后端/数据集/描述符均是一个 `FilterParam`，因此也是一个 GTest 参数。

## 数据集和后端矩阵

| 逻辑输入 | 数据集 | 适用 profile |
|---|---|---|
| Row scalar, all eight types | `PredicateEdges`, `PredicateAllValid`, `PredicateAllNull`, `PredicateSingleRow`, `PredicateAllEqual` | Bitmap/Sorted/Inverted/Hybrid, nullable/non-nullable as allowed, heap/mmap |
| Row VARCHAR | same plus `NullVsEmptyString`, multi-batch | primitive families plus Marisa, FM, Text, and Ngram for Null/base; only actual `value_lookup` profiles for Value |
| Element scalar, all eight types | `NestedElements` | Bitmap/Sorted/Inverted/Hybrid nested heap/mmap; Value descriptors select only advertised value lookup |
| Focused shapes | `BitBoundaryNullable<int64_t>`, `PredicateEdgesWithEmptyBatches`, `TenThousandHighCardinality`, `PredicateFloatInfinities` | nullability/capability-selected profiles, never a copied family table |

当前源码推导的展开：

| 套件 | 展开前的描述符 | GTest 参数 |
|---|---:|---:|
| `NullReaderTest` | 44 | 672 |
| `ScalarValueReaderTest` | 101 | 826 |
| `IndexReaderTest` | 16 | 240 |
| Total | 161 | 1,738 |

Null 计数包含当前集中声明的 Text 和 Ngram 标量 profile，因为它们公开实现 `NullReader`；它不推断 caps 位。基础计数包含它们并动态关联其实际接口。Value 计数仍由能力选择：非字符串行值使用 Bitmap/Sorted 和安全的 Hybrid delegate，VARCHAR 使用 Bitmap/Sorted/Marisa，嵌套 profile 使用 Bitmap/Sorted。

## 固定 master 可追溯性

| master 源码/测试组 | 新契约用例 |
|---|---|
| `ScalarIndexTest.cpp`: `TypedScalarIndexTest.Count`, `Reverse` for int8/int16/int32/int64/float/double | `RowMetadataAndInterfaces`; all typed `Lookup*` groups, including complete-row and high-cardinality boundary lookup |
| `ScalarIndexTest.cpp`: `Codec` | every observation uses the central build -> serialize/consume -> open path before its query |
| `StringIndexTest.cpp`: `StringIndexMarisaTest.Count`, `Reverse` | VARCHAR `RowMetadataAndInterfaces`; `LookupMixedValidity`, `LookupAbsentValidity`, `LookupAllNull`, `LookupRepeatedValues`, owning long-string lookup |
| `StringIndexTest.cpp`: `IsNull`, `IsNullHasNull`, `IsNotNull`, `IsNotNullHasNull` | VARCHAR core Null descriptors plus `NullIsDistinctFromEmptyString` |
| `StringIndexSortTest.cpp`: `ReverseLookupMemory`, `ReverseLookupMmap`, reverse after `SerializeDeserializeMemory` | all VARCHAR Lookup descriptors across both load modes; `LookupOwnsStringAfterLaterQueryAndReaderDestruction` |
| `StringIndexSortTest.cpp`: `NullHandlingMemory`, `NullHandlingMmap`, nullable/load variants | VARCHAR mixed/all-null/all-valid/single/multi-batch Null descriptors |
| `BitmapIndexTest.cpp`: V1-V6 `CountFuncTest`, `IsNullFuncTest`, `IsNotNullFuncTest` | typed row metadata and all logical Null distributions over bitmap heap/mmap layouts |
| `HybridScalarIndexTest.cpp`: V1/V2/Nullable/V3/V4 Count/Null groups | typed row metadata and Null groups over low/high resolved delegates; concrete loaded caps are checked after selector resolution |
| `InvertedIndexTest.cpp`: nullable/non-nullable Null groups and sealed all-valid result | mixed/all-null/absent-all-valid/present-all-valid Null descriptors |
| `FMIndexTest.cpp`: `SerializeLoadRoundTripNoMmap`, `NullVsEmptyStringDistinctAfterReload` | VARCHAR base and Null groups; explicit `NullVsEmptyString` payload collision case |
| `BitmapIndexArrayTest.cpp`: `BuildAndLoadElementLevelBitmap`, `NullableNullsBeforeValidElementBitmap`, `NullableNullsBeforeValidUnifiedLoad`, nested sort cases | `ElementMetadataAndInterfaces`, `NestedElementsAreAllValid`, `LookupNestedElements`, `GatherNestedElements` |
| `InvertedIndexArrayTest.cpp`: `NestedSealedValidityUsesMaterializedElementDomain` | element Count/domain/caps and all-valid element Null masks |
| Ngram/RTree/Text Count and Null assertions | reused `ExpectReaderBase` plus actual interface checks in their specialized suites; generic Null/base cases also include centrally eligible scalar-shaped Text/Ngram profiles |

## 已关闭的超出 master 的额外契约缺口

- 每个支持的有类型值实现中的 bool 反向 Lookup 和 Gather；
- 缺失 validity 与存在的全有效输入；
- 全空和重复/全相等值；
- 跨非字节对齐批次的打包 validity offset 62、63、64 和 65；
- 与非空批交错的零长度批；
- 不假设回调顺序的空/单个/置换/重复/混合空值 Gather 请求；
- 全部构建/打开输入销毁后的读取器使用、Gather 内借用字符串复制，以及读取器销毁后的自有 Lookup 字符串使用；
- 每种 primitive/VARCHAR 类型的 Element 坐标 Count/type/caps/null/value 行为；
- 无重复后端能力表的 scalar、pattern、text、ngram、spatial、value、JSON 和独立 Null 接口的 caps 到接口一致性；
- 不将旧估算等同于 heap 所有权的非负且可重复公开资源观察。

## 已定义排除项

- 无效 Lookup/Gather offset、负 Gather 计数及非零计数配合空指针未由 `ScalarValueReader` 指定；当前实现断言细节未标准化。
- 普通 builder 拒绝零行读取器，它们归属 builder 负向测试。有效非空读取器上覆盖空 Gather。
- 父 ARRAY 行 offset、父空值/空值展开、多层投影和 executor 行折叠发生在这些查询接口之前/之后。这些测试覆盖已定义的扁平 Element 域。
- selector 阈值、畸形持久化、切片文件、远程 IO、注册表构造、转换/物化和私有分配总量归属 build/artifact/family 套件。
- Text analyzer 语义、Ngram 候选细化、spatial 几何和 JSON 路由由其专用契约套件覆盖，不在此复制。

## 静态验证和预期运行时风险

三个自有 Task 2 源均通过 `clang-format --dry-run --Werror` 和 `git diff --check`。集中式构建门禁前不允许编译或运行时命令。嵌入 NUL 的 VARCHAR Lookup 仍感知长度，因此 Marisa profile 可能增加归因于已记录生产截断缺陷的失败；测试不规范化或跳过它。
